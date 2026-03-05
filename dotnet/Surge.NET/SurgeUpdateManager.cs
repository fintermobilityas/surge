using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace Surge
{
    /// <summary>
    /// Manages checking for and applying application updates via the native Surge library.
    /// </summary>
    public sealed class SurgeUpdateManager : IDisposable
    {
        private IntPtr _nativeCtx;
        private IntPtr _nativeMgr;
        private bool _disposed;
        private string _channel = "stable";
        private string _currentVersion = "0.0.0";

        /// <summary>
        /// Maximum number of old releases to retain on disk after updating.
        /// </summary>
        public int ReleaseRetentionLimit { get; set; } = 1;

        /// <summary>
        /// Create a new update manager. Requires that <see cref="SurgeApp.Current"/>
        /// returns valid application info (i.e., the app is running inside a Surge-managed installation).
        /// </summary>
        /// <exception cref="InvalidOperationException">Thrown when no current app info is available.</exception>
        public SurgeUpdateManager()
        {
            var appInfo = SurgeApp.Current;
            if (appInfo == null)
                throw new InvalidOperationException(
                    "Cannot create SurgeUpdateManager: no Surge app info available. " +
                    "Ensure the application is running inside a Surge-managed installation.");

            _nativeCtx = NativeMethods.ContextCreate();
            if (_nativeCtx == IntPtr.Zero)
                throw new InvalidOperationException("Failed to create native Surge context.");

            int provider = ParseStorageProvider(appInfo.StorageProvider);
            int storageResult = NativeMethods.ConfigSetStorage(
                _nativeCtx,
                provider,
                appInfo.StorageBucket,
                NullIfEmpty(appInfo.StorageRegion),
                null,
                null,
                NullIfEmpty(appInfo.StorageEndpoint));
            if (storageResult != 0)
            {
                string? error = GetContextLastError(_nativeCtx);
                NativeMethods.ContextDestroy(_nativeCtx);
                _nativeCtx = IntPtr.Zero;
                throw new InvalidOperationException(
                    "Failed to configure native storage." + (string.IsNullOrWhiteSpace(error) ? "" : $" {error}"));
            }

            _nativeMgr = NativeMethods.UpdateManagerCreate(
                _nativeCtx,
                appInfo.Id,
                appInfo.Version,
                appInfo.Channel,
                appInfo.InstallDirectory);

            if (_nativeMgr == IntPtr.Zero)
            {
                NativeMethods.ContextDestroy(_nativeCtx);
                _nativeCtx = IntPtr.Zero;
                throw new InvalidOperationException("Failed to create native update manager.");
            }

            _channel = string.IsNullOrWhiteSpace(appInfo.Channel) ? "stable" : appInfo.Channel;
            _currentVersion = string.IsNullOrWhiteSpace(appInfo.Version) ? "0.0.0" : appInfo.Version;
        }

        /// <summary>
        /// The currently selected update channel for this manager.
        /// </summary>
        public string Channel => _channel;

        /// <summary>
        /// The current installed version baseline used for update checks.
        /// </summary>
        public string CurrentVersion => _currentVersion;

        /// <summary>
        /// Switch update channel at runtime (for example, from production to test).
        /// </summary>
        /// <param name="channel">Target channel name.</param>
        public void SetChannel(string channel)
        {
            ThrowIfDisposed();
            if (string.IsNullOrWhiteSpace(channel))
                throw new ArgumentException("Channel cannot be empty.", nameof(channel));

            var normalizedChannel = channel.Trim();
            int result = NativeMethods.UpdateManagerSetChannel(_nativeMgr, normalizedChannel);
            if (result != 0)
            {
                var errorMsg = GetLastError();
                throw new SurgeException(result, errorMsg ?? "Failed to set update channel.");
            }

            try
            {
                SurgeApp.PersistCurrentChannel(normalizedChannel);
            }
            catch (Exception ex)
            {
                int rollbackResult = NativeMethods.UpdateManagerSetChannel(_nativeMgr, _channel);
                if (rollbackResult != 0)
                {
                    var rollbackError = GetLastError();
                    throw new InvalidOperationException(
                        $"Failed to persist selected update channel '{normalizedChannel}', and rollback to '{_channel}' also failed." +
                        (string.IsNullOrWhiteSpace(rollbackError) ? "" : $" {rollbackError}"),
                        ex);
                }

                throw new InvalidOperationException(
                    $"Failed to persist selected update channel '{normalizedChannel}'.",
                    ex);
            }

            _channel = normalizedChannel;
        }

        /// <summary>
        /// Update the current version baseline used for future update checks.
        /// </summary>
        /// <param name="version">Installed version string.</param>
        public void SetCurrentVersion(string version)
        {
            ThrowIfDisposed();
            SetCurrentVersionInternal(version);
        }

        /// <summary>
        /// Check for updates and optionally apply the latest release.
        /// </summary>
        /// <param name="progressSource">Optional progress source for receiving update progress.</param>
        /// <param name="onUpdatesAvailable">Called when updates are found, before applying.</param>
        /// <param name="onBeforeApplyUpdate">Called before applying a specific release.</param>
        /// <param name="onAfterApplyUpdate">Called after successfully applying a release.</param>
        /// <param name="onApplyUpdateException">Called when applying a release fails.</param>
        /// <param name="cancellationToken">Token to cancel the operation.</param>
        /// <returns>
        /// The <see cref="SurgeAppInfo"/> for the newly installed version,
        /// or null if no updates were available or the operation was cancelled.
        /// </returns>
        public Task<SurgeAppInfo?> UpdateToLatestReleaseAsync(
            ISurgeProgressSource? progressSource = null,
            Action<ISurgeChannelReleases>? onUpdatesAvailable = null,
            Action<SurgeRelease>? onBeforeApplyUpdate = null,
            Action<SurgeRelease>? onAfterApplyUpdate = null,
            Action<SurgeRelease, Exception>? onApplyUpdateException = null,
            CancellationToken cancellationToken = default)
        {
            ThrowIfDisposed();

            return Task.Run(() =>
            {
                // Register cancellation
                CancellationTokenRegistration registration = default;
                IntPtr ctx = _nativeCtx;
                if (cancellationToken.CanBeCanceled)
                {
                    registration = cancellationToken.Register(() =>
                    {
                        if (ctx != IntPtr.Zero)
                            _ = NativeMethods.Cancel(ctx);
                    });
                }

                try
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    // Check for updates
                    int checkResult = NativeMethods.UpdateCheck(_nativeMgr, out IntPtr releasesInfoPtr);

                    if (checkResult == -3) // SURGE_NOT_FOUND
                        return null;

                    if (checkResult != 0)
                    {
                        var errorMsg = GetLastError();
                        throw new SurgeException(checkResult, errorMsg ?? "Update check failed.");
                    }

                    try
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        // Build releases list
                        int count = NativeMethods.ReleasesCount(releasesInfoPtr);
                        var releases = new List<SurgeRelease>(count);

                        for (int i = 0; i < count; i++)
                        {
                            var versionPtr = NativeMethods.ReleaseVersion(releasesInfoPtr, i);
                            var channelPtr = NativeMethods.ReleaseChannel(releasesInfoPtr, i);
                            var fullSize = NativeMethods.ReleaseFullSize(releasesInfoPtr, i);
                            var isGenesis = NativeMethods.ReleaseIsGenesis(releasesInfoPtr, i) != 0;

                            releases.Add(new SurgeRelease
                            {
                                Version = MarshalUtf8(versionPtr),
                                Channel = MarshalUtf8(channelPtr),
                                FullSize = fullSize,
                                IsGenesis = isGenesis
                            });
                        }

                        if (releases.Count == 0)
                            return null;

                        // Notify about available updates
                        if (onUpdatesAvailable != null)
                        {
                            var channelReleases = new SurgeChannelReleases(
                                releases[0].Channel, releases);
                            onUpdatesAvailable(channelReleases);
                        }

                        cancellationToken.ThrowIfCancellationRequested();

                        // Native releases are ordered oldest -> newest.
                        var latestRelease = releases[releases.Count - 1];

                        // Before apply callback
                        onBeforeApplyUpdate?.Invoke(latestRelease);

                        // Set up progress callback
                        SurgeProgressCallbackDelegate? nativeProgressCb = null;
                        if (progressSource != null)
                        {
                            nativeProgressCb = (progressPtr, _) =>
                            {
                                var native = Marshal.PtrToStructure<SurgeProgressNative>(progressPtr);
                                var progress = new SurgeProgress
                                {
                                    Phase = (SurgeProgressPhase)native.Phase,
                                    PhasePercent = native.PhasePercent,
                                    TotalPercent = native.TotalPercent,
                                    BytesDone = native.BytesDone,
                                    BytesTotal = native.BytesTotal,
                                    ItemsDone = native.ItemsDone,
                                    ItemsTotal = native.ItemsTotal,
                                    SpeedBytesPerSec = native.SpeedBytesPerSec
                                };

                                DispatchProgress(progressSource, progress);
                            };
                        }

                        // Download and apply
                        try
                        {
                            int applyResult = NativeMethods.UpdateDownloadAndApply(
                                _nativeMgr,
                                releasesInfoPtr,
                                nativeProgressCb,
                                IntPtr.Zero);

                            if (applyResult == -2) // SURGE_CANCELLED
                            {
                                cancellationToken.ThrowIfCancellationRequested();
                                return null;
                            }

                            if (applyResult != 0)
                            {
                                var errorMsg = GetLastError();
                                var ex = new SurgeException(applyResult, errorMsg ?? "Update apply failed.");
                                onApplyUpdateException?.Invoke(latestRelease, ex);
                                throw ex;
                            }

                            onAfterApplyUpdate?.Invoke(latestRelease);
                            SetCurrentVersionInternal(latestRelease.Version);

                            return new SurgeAppInfo
                            {
                                Id = SurgeApp.Current?.Id ?? "",
                                Version = latestRelease.Version,
                                Channel = latestRelease.Channel,
                                InstallDirectory = SurgeApp.Current?.InstallDirectory ?? "",
                                SupervisorId = SurgeApp.Current?.SupervisorId ?? ""
                            };
                        }
                        catch (SurgeException)
                        {
                            throw;
                        }
                        catch (OperationCanceledException)
                        {
                            throw;
                        }
                        catch (Exception ex)
                        {
                            onApplyUpdateException?.Invoke(latestRelease, ex);
                            throw;
                        }
                    }
                    finally
                    {
                        NativeMethods.ReleasesDestroy(releasesInfoPtr);
                    }
                }
                finally
                {
                    registration.Dispose();
                }
            }, cancellationToken);
        }

        private static int ParseStorageProvider(string provider)
        {
            switch ((provider ?? "").Trim().ToLowerInvariant())
            {
                case "s3":
                    return 0;
                case "azure":
                case "azureblob":
                case "azure_blob":
                    return 1;
                case "gcs":
                    return 2;
                case "filesystem":
                case "":
                    return 3;
                case "github":
                case "github_releases":
                case "github-releases":
                case "githubreleases":
                    return 4;
                default:
                    throw new InvalidOperationException($"Unsupported storage provider: {provider}");
            }
        }

        private static string? NullIfEmpty(string value)
        {
            return string.IsNullOrWhiteSpace(value) ? null : value;
        }

        private void SetCurrentVersionInternal(string version)
        {
            if (string.IsNullOrWhiteSpace(version))
                throw new ArgumentException("Version cannot be empty.", nameof(version));

            int result = NativeMethods.UpdateManagerSetCurrentVersion(_nativeMgr, version);
            if (result != 0)
            {
                var errorMsg = GetLastError();
                throw new SurgeException(result, errorMsg ?? "Failed to set current version.");
            }

            _currentVersion = version;
        }

        private static string? GetContextLastError(IntPtr nativeCtx)
        {
            if (nativeCtx == IntPtr.Zero)
                return null;

            IntPtr errorPtr = NativeMethods.ContextLastError(nativeCtx);
            if (errorPtr == IntPtr.Zero)
                return null;

            var error = Marshal.PtrToStructure<SurgeErrorNative>(errorPtr);
            if (error.Message == IntPtr.Zero)
                return null;

            return MarshalUtf8(error.Message);
        }

        private static void DispatchProgress(ISurgeProgressSource source, SurgeProgress progress)
        {
            source.TotalProgress?.Invoke(progress);

            switch (progress.Phase)
            {
                case SurgeProgressPhase.Download:
                    source.DownloadProgress?.Invoke(progress);
                    break;
                case SurgeProgressPhase.Verify:
                    source.VerifyProgress?.Invoke(progress);
                    break;
                case SurgeProgressPhase.Extract:
                    source.ExtractProgress?.Invoke(progress);
                    break;
                case SurgeProgressPhase.ApplyDelta:
                    source.ApplyDeltaProgress?.Invoke(progress);
                    break;
            }
        }

        private string? GetLastError()
        {
            if (_nativeCtx == IntPtr.Zero)
                return null;

            IntPtr errorPtr = NativeMethods.ContextLastError(_nativeCtx);
            if (errorPtr == IntPtr.Zero)
                return null;

            var error = Marshal.PtrToStructure<SurgeErrorNative>(errorPtr);
            if (error.Message == IntPtr.Zero)
                return null;

            return MarshalUtf8(error.Message);
        }

        private void ThrowIfDisposed()
        {
#if NET10_0_OR_GREATER
            ObjectDisposedException.ThrowIf(_disposed, this);
#else
            if (_disposed)
                throw new ObjectDisposedException(nameof(SurgeUpdateManager));
#endif
        }

        /// <summary>
        /// Release native resources held by this update manager.
        /// </summary>
        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;

            if (_nativeMgr != IntPtr.Zero)
            {
                NativeMethods.UpdateManagerDestroy(_nativeMgr);
                _nativeMgr = IntPtr.Zero;
            }

            if (_nativeCtx != IntPtr.Zero)
            {
                NativeMethods.ContextDestroy(_nativeCtx);
                _nativeCtx = IntPtr.Zero;
            }
        }

        private static string MarshalUtf8(IntPtr ptr)
        {
#if NETSTANDARD2_0
            return MarshalHelper.PtrToStringUTF8(ptr) ?? "";
#else
            return Marshal.PtrToStringUTF8(ptr) ?? "";
#endif
        }
    }

    /// <summary>
    /// Exception thrown when a Surge native operation fails.
    /// </summary>
    public class SurgeException : Exception
    {
        /// <summary>
        /// The native error code returned by the Surge C API.
        /// </summary>
        public int NativeErrorCode { get; }

        public SurgeException(int nativeErrorCode, string message)
            : base(message)
        {
            NativeErrorCode = nativeErrorCode;
        }
    }
}

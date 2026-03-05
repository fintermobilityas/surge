using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;

namespace Surge
{
    /// <summary>
    /// Static entry point for Surge application lifecycle management.
    /// Provides access to current app info, lifecycle events, and supervisor control.
    /// </summary>
    public static class SurgeApp
    {
        private enum LifecycleAction
        {
            None,
            FirstRun,
            Installed,
            Updated
        }

        private static SurgeAppInfo? _current;
        private static readonly object _lock = new object();

        /// <summary>
        /// Information about the currently installed application, or null if not
        /// running inside a Surge-managed installation.
        /// </summary>
        public static SurgeAppInfo? Current
        {
            get
            {
                if (_current == null)
                {
                    lock (_lock)
                    {
                        if (_current == null)
                        {
                            _current = LoadCurrentApp();
                        }
                    }
                }
                return _current;
            }
        }

        /// <summary>
        /// The working directory for this application (parent of the assembly location).
        /// </summary>
        public static string WorkingDirectory => GetWorkingDirectory();

        /// <summary>
        /// The Surge library version.
        /// </summary>
        public static string Version => "0.1.0";

        /// <summary>
        /// Process application lifecycle events. Should be called early in the
        /// application startup to handle first-run, installed, and updated hooks.
        /// </summary>
        /// <param name="args">Command line arguments from Main().</param>
        /// <param name="onFirstRun">Called on the very first run with the current version.</param>
        /// <param name="onInstalled">Called after a fresh install with the installed version.</param>
        /// <param name="onUpdated">Called after an update with the new version.</param>
        /// <returns>True if a lifecycle event was handled.</returns>
        public static bool ProcessEvents(
            string[] args,
            Action<string>? onFirstRun = null,
            Action<string>? onInstalled = null,
            Action<string>? onUpdated = null)
        {
            var lifecycleAction = DetermineLifecycleAction(args);
            var argPtrs = new IntPtr[args.Length];
            var pinnedHandles = new GCHandle[args.Length];

            try
            {
                for (int i = 0; i < args.Length; i++)
                {
                    var bytes = System.Text.Encoding.UTF8.GetBytes(args[i] + '\0');
                    pinnedHandles[i] = GCHandle.Alloc(bytes, GCHandleType.Pinned);
                    argPtrs[i] = pinnedHandles[i].AddrOfPinnedObject();
                }

                var argvHandle = GCHandle.Alloc(argPtrs, GCHandleType.Pinned);

                try
                {
                    SurgeEventCallbackDelegate? firstRunCb = null;
                    SurgeEventCallbackDelegate? installedCb = null;
                    SurgeEventCallbackDelegate? updatedCb = null;

                    if (onFirstRun != null)
                    {
                        firstRunCb = (versionPtr, _) =>
                        {
                            var version = MarshalUtf8(versionPtr);
                            onFirstRun(version);
                        };
                    }

                    if (onInstalled != null)
                    {
                        installedCb = (versionPtr, _) =>
                        {
                            var version = MarshalUtf8(versionPtr);
                            onInstalled(version);
                        };
                    }

                    if (onUpdated != null)
                    {
                        updatedCb = (versionPtr, _) =>
                        {
                            var version = MarshalUtf8(versionPtr);
                            onUpdated(version);
                        };
                    }

                    int result = NativeMethods.ProcessEvents(
                        args.Length,
                        argvHandle.AddrOfPinnedObject(),
                        firstRunCb,
                        installedCb,
                        updatedCb,
                        IntPtr.Zero);

                    if (result != 0 || lifecycleAction == LifecycleAction.None)
                    {
                        return false;
                    }

                    return lifecycleAction is LifecycleAction.Installed or LifecycleAction.Updated;
                }
                finally
                {
                    argvHandle.Free();
                }
            }
            finally
            {
                for (int i = 0; i < pinnedHandles.Length; i++)
                {
                    if (pinnedHandles[i].IsAllocated)
                        pinnedHandles[i].Free();
                }
            }
        }

        private static LifecycleAction DetermineLifecycleAction(string[] args)
        {
            bool sawFirstRun = false;

            foreach (var arg in args)
            {
                if (arg == "--surge-installed")
                {
                    return LifecycleAction.Installed;
                }

                if (arg == "--surge-updated" || arg.StartsWith("--surge-updated=", StringComparison.Ordinal))
                {
                    return LifecycleAction.Updated;
                }

                if (arg == "--surge-first-run")
                {
                    sawFirstRun = true;
                }
            }

            return sawFirstRun ? LifecycleAction.FirstRun : LifecycleAction.None;
        }

        /// <summary>
        /// Start the supervisor process that monitors and restarts this application.
        /// </summary>
        /// <param name="restartArguments">Arguments to pass when restarting the application.</param>
        /// <param name="environment">Additional environment variables for the supervised process.</param>
        /// <returns>True if the supervisor was started successfully.</returns>
        public static bool StartSupervisor(
            string[]? restartArguments = null,
            IDictionary<string, string>? environment = null)
        {
            var current = Current;
            if (current == null)
                return false;

            _ = StopSupervisor();
            _ = environment;

            var exePath = GetCurrentExePath();
            var installDir = current.InstallDirectory;
            var supervisorId = ResolveSupervisorId(current);
            if (string.IsNullOrWhiteSpace(installDir) || string.IsNullOrWhiteSpace(supervisorId))
                return false;

            var actualArgs = restartArguments ?? Array.Empty<string>();
            var argPtrs = new IntPtr[actualArgs.Length];
            var pinnedHandles = new GCHandle[actualArgs.Length];

            try
            {
                for (int i = 0; i < actualArgs.Length; i++)
                {
                    var bytes = System.Text.Encoding.UTF8.GetBytes(actualArgs[i] + '\0');
                    pinnedHandles[i] = GCHandle.Alloc(bytes, GCHandleType.Pinned);
                    argPtrs[i] = pinnedHandles[i].AddrOfPinnedObject();
                }

                IntPtr argvPtr = IntPtr.Zero;
                GCHandle argvHandle = default;

                if (argPtrs.Length > 0)
                {
                    argvHandle = GCHandle.Alloc(argPtrs, GCHandleType.Pinned);
                    argvPtr = argvHandle.AddrOfPinnedObject();
                }

                try
                {
                    int result = NativeMethods.SupervisorStart(
                        exePath,
                        installDir,
                        supervisorId,
                        actualArgs.Length,
                        argvPtr);

                    if (result == 0)
                    {
                        current.IsSupervisorRunning = true;
                        return true;
                    }

                    return false;
                }
                finally
                {
                    if (argvHandle.IsAllocated)
                        argvHandle.Free();
                }
            }
            finally
            {
                for (int i = 0; i < pinnedHandles.Length; i++)
                {
                    if (pinnedHandles[i].IsAllocated)
                        pinnedHandles[i].Free();
                }
            }
        }

        /// <summary>
        /// Stop the supervisor process if it is running.
        /// </summary>
        /// <returns>True if a stop signal was sent successfully.</returns>
        public static bool StopSupervisor()
        {
            var current = Current;
            if (current == null)
                return false;

            var supervisorId = ResolveSupervisorId(current);
            if (string.IsNullOrWhiteSpace(current.InstallDirectory) || string.IsNullOrWhiteSpace(supervisorId))
                return false;

            int result = NativeMethods.SupervisorStop(current.InstallDirectory, supervisorId);
            if (result != 0)
                return false;

            current.IsSupervisorRunning = false;
            return true;
        }

        private static SurgeAppInfo? LoadCurrentApp()
        {
            var assemblyDir = GetWorkingDirectory();
            var (runtimeManifestPath, legacyManifestPath) = GetRuntimeManifestPaths(assemblyDir);

            return TryLoadCurrentAppFromManifest(runtimeManifestPath, assemblyDir)
                ?? TryLoadCurrentAppFromManifest(legacyManifestPath, assemblyDir);
        }

        internal static void PersistCurrentChannel(string channel)
        {
            var current = Current;
            if (current == null)
                throw new InvalidOperationException("Cannot persist Surge channel: no current app info is available.");

            var normalizedChannel = channel.Trim();
            if (string.IsNullOrWhiteSpace(normalizedChannel))
                throw new ArgumentException("Channel cannot be empty.", nameof(channel));

            var assemblyDir = GetWorkingDirectory();
            var (runtimeManifestPath, legacyManifestPath) = GetRuntimeManifestPaths(assemblyDir);
            string? updatedManifest = null;
            var updatedAnyManifest = false;

            foreach (var manifestPath in new[] { runtimeManifestPath, legacyManifestPath })
            {
                if (!File.Exists(manifestPath))
                    continue;

                var manifest = File.ReadAllText(manifestPath);
                var updated = UpsertChannelInManifest(manifest, normalizedChannel);
                File.WriteAllText(manifestPath, updated);
                updatedManifest ??= updated;
                updatedAnyManifest = true;
            }

            if (!updatedAnyManifest || updatedManifest == null)
                throw new FileNotFoundException("Unable to persist Surge channel because no runtime manifest was found.");

            if (!File.Exists(runtimeManifestPath))
                File.WriteAllText(runtimeManifestPath, updatedManifest);

            if (!File.Exists(legacyManifestPath))
                File.WriteAllText(legacyManifestPath, updatedManifest);

            lock (_lock)
            {
                _current = CloneWithChannel(current, normalizedChannel);
            }
        }

        private static SurgeAppInfo? TryLoadCurrentAppFromManifest(string manifestPath, string assemblyDir)
        {
            if (!File.Exists(manifestPath))
                return null;

            try
            {
                string? appId = null;
                string? version = null;
                string? channel = null;
                string? installDir = null;
                string? supervisorId = null;
                string? storageProvider = null;
                string? storageBucket = null;
                string? storageRegion = null;
                string? storageEndpoint = null;

                foreach (var line in File.ReadLines(manifestPath))
                {
                    // Strip leading whitespace and YAML list marker (- )
                    var trimmed = line.Trim();
                    if (trimmed.StartsWith("- ", StringComparison.Ordinal))
                        trimmed = trimmed.Substring(2).Trim();

                    if (trimmed.StartsWith("id:", StringComparison.Ordinal))
                        appId = trimmed.Substring(3).Trim().Trim('"');
                    else if (trimmed.StartsWith("version:", StringComparison.Ordinal))
                        version = trimmed.Substring(8).Trim().Trim('"');
                    else if (trimmed.StartsWith("channel:", StringComparison.Ordinal))
                        channel = trimmed.Substring(8).Trim().Trim('"');
                    else if (trimmed.StartsWith("installDirectory:", StringComparison.Ordinal))
                        installDir = trimmed.Substring(17).Trim().Trim('"');
                    else if (trimmed.StartsWith("supervisorId:", StringComparison.Ordinal))
                        supervisorId = trimmed.Substring(13).Trim().Trim('"');
                    else if (trimmed.StartsWith("supervisorid:", StringComparison.OrdinalIgnoreCase))
                        supervisorId = trimmed.Substring(13).Trim().Trim('"');
                    else if (trimmed.StartsWith("supervisor_id:", StringComparison.Ordinal))
                        supervisorId = trimmed.Substring(14).Trim().Trim('"');
                    else if (trimmed.StartsWith("provider:", StringComparison.Ordinal))
                        storageProvider = trimmed.Substring(9).Trim().Trim('"');
                    else if (trimmed.StartsWith("bucket:", StringComparison.Ordinal))
                        storageBucket = trimmed.Substring(7).Trim().Trim('"');
                    else if (trimmed.StartsWith("region:", StringComparison.Ordinal))
                        storageRegion = trimmed.Substring(7).Trim().Trim('"');
                    else if (trimmed.StartsWith("endpoint:", StringComparison.Ordinal))
                        storageEndpoint = trimmed.Substring(9).Trim().Trim('"');
                }

                if (string.IsNullOrWhiteSpace(appId))
                    return null;

                var resolvedAppId = appId!.Trim();
                return new SurgeAppInfo
                {
                    Id = resolvedAppId,
                    Version = version ?? "0.0.0",
                    Channel = channel ?? "stable",
                    InstallDirectory = ResolveInstallDirectory(resolvedAppId, installDir, assemblyDir),
                    SupervisorId = supervisorId ?? "",
                    StorageProvider = storageProvider ?? "filesystem",
                    StorageBucket = storageBucket ?? "",
                    StorageRegion = storageRegion ?? "",
                    StorageEndpoint = storageEndpoint ?? ""
                };
            }
            catch
            {
                return null;
            }
        }

        private static string GetWorkingDirectory()
        {
            return AppContext.BaseDirectory;
        }

        internal static string ResolveCurrentExePath(string? processPath, string? mainModulePath, string[] commandLineArgs)
        {
            if (!string.IsNullOrWhiteSpace(processPath))
                return processPath!;

            if (!string.IsNullOrWhiteSpace(mainModulePath))
                return mainModulePath!;

            return commandLineArgs.Length == 0 ? "" : commandLineArgs[0];
        }

        private static string GetCurrentExePath()
        {
#if NET10_0_OR_GREATER
            var processPath = Environment.ProcessPath;
#else
            string? processPath = null;
#endif

            string? mainModulePath = null;
            try
            {
                using var process = Process.GetCurrentProcess();
                mainModulePath = process.MainModule?.FileName;
            }
            catch
            {
                // Ignore platform/process inspection failures and fall back to argv[0].
            }

            return ResolveCurrentExePath(processPath, mainModulePath, Environment.GetCommandLineArgs());
        }

        internal static string UpsertChannelInManifest(string manifest, string channel)
        {
            var normalizedChannel = channel.Trim();
            if (string.IsNullOrWhiteSpace(normalizedChannel))
                throw new ArgumentException("Channel cannot be empty.", nameof(channel));

            var lines = manifest.Replace("\r\n", "\n").Replace('\r', '\n').Split('\n');
            var foundChannel = false;

            for (int i = 0; i < lines.Length; i++)
            {
                var line = lines[i];
                var trimmed = line.TrimStart();
                if (!trimmed.StartsWith("channel:", StringComparison.Ordinal))
                    continue;

                var prefixLength = line.Length - trimmed.Length;
#if NETSTANDARD2_0
                lines[i] = line.Substring(0, prefixLength) + "channel: " + normalizedChannel;
#else
                lines[i] = string.Concat(line.AsSpan(0, prefixLength), "channel: ", normalizedChannel);
#endif
                foundChannel = true;
                break;
            }

            var updatedLines = new List<string>(lines.Length + (foundChannel ? 0 : 1));
            foreach (var line in lines)
            {
                if (updatedLines.Count == lines.Length - 1 && line.Length == 0)
                    continue;

                updatedLines.Add(line);
            }

            if (!foundChannel)
                updatedLines.Add("channel: " + normalizedChannel);

            return string.Join(Environment.NewLine, updatedLines) + Environment.NewLine;
        }

        private static string ResolveSupervisorId(SurgeAppInfo appInfo)
        {
            if (!string.IsNullOrWhiteSpace(appInfo.SupervisorId))
                return appInfo.SupervisorId;

            return string.IsNullOrWhiteSpace(appInfo.Id) ? "" : appInfo.Id + "-supervisor";
        }

        private static string ResolveInstallDirectory(string appId, string? installDirectoryName, string fallbackDirectory)
        {
            if (string.IsNullOrWhiteSpace(appId))
                return fallbackDirectory;

            var localAppData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
            var targetDirectoryName = string.IsNullOrWhiteSpace(installDirectoryName)
                ? appId
                : installDirectoryName!;

            if (Path.IsPathRooted(targetDirectoryName))
                return targetDirectoryName;

            if (string.IsNullOrWhiteSpace(localAppData))
                return Path.Combine(fallbackDirectory, targetDirectoryName);

            return Path.Combine(localAppData, targetDirectoryName);
        }

        private static (string runtimeManifestPath, string legacyManifestPath) GetRuntimeManifestPaths(string assemblyDir)
        {
            var surgeDir = Path.Combine(assemblyDir, ".surge");
            return (
                Path.Combine(surgeDir, "runtime.yml"),
                Path.Combine(surgeDir, "surge.yml"));
        }

        private static SurgeAppInfo CloneWithChannel(SurgeAppInfo current, string channel)
        {
            return new SurgeAppInfo
            {
                Id = current.Id,
                Version = current.Version,
                Channel = channel,
                InstallDirectory = current.InstallDirectory,
                SupervisorId = current.SupervisorId,
                StorageProvider = current.StorageProvider,
                StorageBucket = current.StorageBucket,
                StorageRegion = current.StorageRegion,
                StorageEndpoint = current.StorageEndpoint,
                IsSupervisorRunning = current.IsSupervisorRunning
            };
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
}

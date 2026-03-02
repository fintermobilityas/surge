using System;
using System.Collections.Generic;
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
            var argPtrs = new IntPtr[args.Length];
            var pinnedHandles = new GCHandle[args.Length];
            bool eventHandled = false;

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
                            eventHandled = true;
                        };
                    }

                    if (onInstalled != null)
                    {
                        installedCb = (versionPtr, _) =>
                        {
                            var version = MarshalUtf8(versionPtr);
                            onInstalled(version);
                            eventHandled = true;
                        };
                    }

                    if (onUpdated != null)
                    {
                        updatedCb = (versionPtr, _) =>
                        {
                            var version = MarshalUtf8(versionPtr);
                            onUpdated(version);
                            eventHandled = true;
                        };
                    }

                    int result = NativeMethods.ProcessEvents(
                        args.Length,
                        argvHandle.AddrOfPinnedObject(),
                        firstRunCb,
                        installedCb,
                        updatedCb,
                        IntPtr.Zero);

                    return result == 0 && eventHandled;
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

            var exePath = GetCurrentExePath();
            var workingDir = WorkingDirectory;
            var supervisorId = current.Id + "-supervisor";

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
                        workingDir,
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
            if (current == null || !current.IsSupervisorRunning)
                return false;

            current.IsSupervisorRunning = false;
            return true;
        }

        private static SurgeAppInfo? LoadCurrentApp()
        {
            var assemblyDir = GetWorkingDirectory();
            var surgeDir = Path.Combine(assemblyDir, ".surge");
            var manifestPath = Path.Combine(surgeDir, "surge.yml");

            if (!File.Exists(manifestPath))
                return null;

            try
            {
                string? appId = null;
                string? version = null;
                string? channel = null;
                string? installDir = null;
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
                    else if (trimmed.StartsWith("provider:", StringComparison.Ordinal))
                        storageProvider = trimmed.Substring(9).Trim().Trim('"');
                    else if (trimmed.StartsWith("bucket:", StringComparison.Ordinal))
                        storageBucket = trimmed.Substring(7).Trim().Trim('"');
                    else if (trimmed.StartsWith("region:", StringComparison.Ordinal))
                        storageRegion = trimmed.Substring(7).Trim().Trim('"');
                    else if (trimmed.StartsWith("endpoint:", StringComparison.Ordinal))
                        storageEndpoint = trimmed.Substring(9).Trim().Trim('"');
                }

                if (appId == null)
                    return null;

                return new SurgeAppInfo
                {
                    Id = appId,
                    Version = version ?? "0.0.0",
                    Channel = channel ?? "stable",
                    InstallDirectory = installDir ?? assemblyDir,
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

        private static string GetCurrentExePath()
        {
            return Environment.GetCommandLineArgs()[0];
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

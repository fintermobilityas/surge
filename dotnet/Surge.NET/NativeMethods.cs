using System;
using System.Runtime.InteropServices;

namespace Surge
{
    /// <summary>
    /// Delegate for progress callbacks from native code.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void SurgeProgressCallbackDelegate(IntPtr progress, IntPtr userData);

    /// <summary>
    /// Delegate for lifecycle event callbacks from native code.
    /// </summary>
    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    internal delegate void SurgeEventCallbackDelegate(IntPtr version, IntPtr userData);

    /// <summary>
    /// Native interop structure matching surge_progress from surge_api.h.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    internal struct SurgeProgressNative
    {
        public int Phase;
        public int PhasePercent;
        public int TotalPercent;
        public long BytesDone;
        public long BytesTotal;
        public long ItemsDone;
        public long ItemsTotal;
        public double SpeedBytesPerSec;
    }

    /// <summary>
    /// Native interop structure matching surge_resource_budget from surge_api.h.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    internal struct SurgeResourceBudgetNative
    {
        public long MaxMemoryBytes;
        public int MaxThreads;
        public int MaxConcurrentDownloads;
        public long MaxDownloadSpeedBps;
        public int ZstdCompressionLevel;
    }

    /// <summary>
    /// Native interop structure matching surge_error from surge_api.h.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    internal struct SurgeErrorNative
    {
        public int Code;
        public IntPtr Message;
    }

    internal static partial class NativeMethods
    {
        private const string LibName = "surge";

#if NET10_0_OR_GREATER
        // --- Lifecycle ---

        [LibraryImport(LibName, EntryPoint = "surge_context_create")]
        internal static partial IntPtr ContextCreate();

        [LibraryImport(LibName, EntryPoint = "surge_context_destroy")]
        internal static partial void ContextDestroy(IntPtr ctx);

        [LibraryImport(LibName, EntryPoint = "surge_context_last_error")]
        internal static partial IntPtr ContextLastError(IntPtr ctx);

        // --- Configuration ---

        [LibraryImport(LibName, EntryPoint = "surge_config_set_storage", StringMarshalling = StringMarshalling.Utf8)]
        internal static partial int ConfigSetStorage(
            IntPtr ctx,
            int provider,
            string bucket,
            string? region,
            string? accessKey,
            string? secretKey,
            string? endpoint);

        [LibraryImport(LibName, EntryPoint = "surge_config_set_lock_server", StringMarshalling = StringMarshalling.Utf8)]
        internal static partial int ConfigSetLockServer(IntPtr ctx, string url);

        [LibraryImport(LibName, EntryPoint = "surge_config_set_resource_budget")]
        internal static partial int ConfigSetResourceBudget(IntPtr ctx, ref SurgeResourceBudgetNative budget);

        // --- Update manager ---

        [LibraryImport(LibName, EntryPoint = "surge_update_manager_create", StringMarshalling = StringMarshalling.Utf8)]
        internal static partial IntPtr UpdateManagerCreate(
            IntPtr ctx,
            string appId,
            string currentVersion,
            string channel,
            string installDir);

        [LibraryImport(LibName, EntryPoint = "surge_update_manager_destroy")]
        internal static partial void UpdateManagerDestroy(IntPtr mgr);

        [LibraryImport(LibName, EntryPoint = "surge_update_manager_set_channel", StringMarshalling = StringMarshalling.Utf8)]
        internal static partial int UpdateManagerSetChannel(IntPtr mgr, string channel);

        [LibraryImport(LibName, EntryPoint = "surge_update_manager_set_current_version", StringMarshalling = StringMarshalling.Utf8)]
        internal static partial int UpdateManagerSetCurrentVersion(IntPtr mgr, string currentVersion);

        [LibraryImport(LibName, EntryPoint = "surge_update_check")]
        internal static partial int UpdateCheck(IntPtr mgr, out IntPtr info);

        [LibraryImport(LibName, EntryPoint = "surge_update_download_and_apply")]
        internal static partial int UpdateDownloadAndApply(
            IntPtr mgr,
            IntPtr info,
            SurgeProgressCallbackDelegate? progressCb,
            IntPtr userData);

        // --- Releases info accessors ---

        [LibraryImport(LibName, EntryPoint = "surge_releases_count")]
        internal static partial int ReleasesCount(IntPtr info);

        [LibraryImport(LibName, EntryPoint = "surge_releases_destroy")]
        internal static partial void ReleasesDestroy(IntPtr info);

        [LibraryImport(LibName, EntryPoint = "surge_release_version")]
        internal static partial IntPtr ReleaseVersion(IntPtr info, int index);

        [LibraryImport(LibName, EntryPoint = "surge_release_channel")]
        internal static partial IntPtr ReleaseChannel(IntPtr info, int index);

        [LibraryImport(LibName, EntryPoint = "surge_release_full_size")]
        internal static partial long ReleaseFullSize(IntPtr info, int index);

        [LibraryImport(LibName, EntryPoint = "surge_release_is_genesis")]
        internal static partial int ReleaseIsGenesis(IntPtr info, int index);

        // --- Binary diff/patch ---

        [LibraryImport(LibName, EntryPoint = "surge_bsdiff")]
        internal static partial int Bsdiff(IntPtr ctx);

        [LibraryImport(LibName, EntryPoint = "surge_bspatch")]
        internal static partial int Bspatch(IntPtr ctx);

        [LibraryImport(LibName, EntryPoint = "surge_bsdiff_free")]
        internal static partial void BsdiffFree(IntPtr ctx);

        [LibraryImport(LibName, EntryPoint = "surge_bspatch_free")]
        internal static partial void BspatchFree(IntPtr ctx);

        // --- Pack ---

        [LibraryImport(LibName, EntryPoint = "surge_pack_create", StringMarshalling = StringMarshalling.Utf8)]
        internal static partial IntPtr PackCreate(
            IntPtr ctx,
            string manifestPath,
            string appId,
            string rid,
            string version,
            string artifactsDir);

        [LibraryImport(LibName, EntryPoint = "surge_pack_build")]
        internal static partial int PackBuild(
            IntPtr packCtx,
            SurgeProgressCallbackDelegate? progressCb,
            IntPtr userData);

        [LibraryImport(LibName, EntryPoint = "surge_pack_push", StringMarshalling = StringMarshalling.Utf8)]
        internal static partial int PackPush(
            IntPtr packCtx,
            string channel,
            SurgeProgressCallbackDelegate? progressCb,
            IntPtr userData);

        [LibraryImport(LibName, EntryPoint = "surge_pack_destroy")]
        internal static partial void PackDestroy(IntPtr packCtx);

        // --- Lock ---

        [LibraryImport(LibName, EntryPoint = "surge_lock_acquire", StringMarshalling = StringMarshalling.Utf8)]
        internal static partial int LockAcquire(IntPtr ctx, string name, int timeoutSeconds, out IntPtr challengeOut);

        [LibraryImport(LibName, EntryPoint = "surge_lock_release", StringMarshalling = StringMarshalling.Utf8)]
        internal static partial int LockRelease(IntPtr ctx, string name, string challenge);

        // --- Supervisor ---

        [LibraryImport(LibName, EntryPoint = "surge_supervisor_start", StringMarshalling = StringMarshalling.Utf8)]
        internal static partial int SupervisorStart(
            string exePath,
            string workingDir,
            string supervisorId,
            int argc,
            IntPtr argv);

        // --- Lifecycle events ---

        [LibraryImport(LibName, EntryPoint = "surge_process_events")]
        internal static partial int ProcessEvents(
            int argc,
            IntPtr argv,
            SurgeEventCallbackDelegate? onFirstRun,
            SurgeEventCallbackDelegate? onInstalled,
            SurgeEventCallbackDelegate? onUpdated,
            IntPtr userData);

        // --- Cancellation ---

        [LibraryImport(LibName, EntryPoint = "surge_cancel")]
        internal static partial int Cancel(IntPtr ctx);
#else
        // netstandard2.0: CharSet.Ansi maps to UTF-8 on Linux/.NET Core.
        // CA2101 is suppressed because there is no StringMarshalling.Utf8 in netstandard2.0.
#pragma warning disable CA2101
        // --- Lifecycle ---

        [DllImport(LibName, EntryPoint = "surge_context_create", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ContextCreate();

        [DllImport(LibName, EntryPoint = "surge_context_destroy", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void ContextDestroy(IntPtr ctx);

        [DllImport(LibName, EntryPoint = "surge_context_last_error", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ContextLastError(IntPtr ctx);

        // --- Configuration ---

        [DllImport(LibName, EntryPoint = "surge_config_set_storage", CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi)]
        internal static extern int ConfigSetStorage(
            IntPtr ctx,
            int provider,
            string bucket,
            string? region,
            string? accessKey,
            string? secretKey,
            string? endpoint);

        [DllImport(LibName, EntryPoint = "surge_config_set_lock_server", CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi)]
        internal static extern int ConfigSetLockServer(IntPtr ctx, string url);

        [DllImport(LibName, EntryPoint = "surge_config_set_resource_budget", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int ConfigSetResourceBudget(IntPtr ctx, ref SurgeResourceBudgetNative budget);

        // --- Update manager ---

        [DllImport(LibName, EntryPoint = "surge_update_manager_create", CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi)]
        internal static extern IntPtr UpdateManagerCreate(
            IntPtr ctx,
            string appId,
            string currentVersion,
            string channel,
            string installDir);

        [DllImport(LibName, EntryPoint = "surge_update_manager_destroy", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void UpdateManagerDestroy(IntPtr mgr);

        [DllImport(LibName, EntryPoint = "surge_update_manager_set_channel", CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi)]
        internal static extern int UpdateManagerSetChannel(IntPtr mgr, string channel);

        [DllImport(LibName, EntryPoint = "surge_update_manager_set_current_version", CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi)]
        internal static extern int UpdateManagerSetCurrentVersion(IntPtr mgr, string currentVersion);

        [DllImport(LibName, EntryPoint = "surge_update_check", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int UpdateCheck(IntPtr mgr, out IntPtr info);

        [DllImport(LibName, EntryPoint = "surge_update_download_and_apply", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int UpdateDownloadAndApply(
            IntPtr mgr,
            IntPtr info,
            SurgeProgressCallbackDelegate? progressCb,
            IntPtr userData);

        // --- Releases info accessors ---

        [DllImport(LibName, EntryPoint = "surge_releases_count", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int ReleasesCount(IntPtr info);

        [DllImport(LibName, EntryPoint = "surge_releases_destroy", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void ReleasesDestroy(IntPtr info);

        [DllImport(LibName, EntryPoint = "surge_release_version", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ReleaseVersion(IntPtr info, int index);

        [DllImport(LibName, EntryPoint = "surge_release_channel", CallingConvention = CallingConvention.Cdecl)]
        internal static extern IntPtr ReleaseChannel(IntPtr info, int index);

        [DllImport(LibName, EntryPoint = "surge_release_full_size", CallingConvention = CallingConvention.Cdecl)]
        internal static extern long ReleaseFullSize(IntPtr info, int index);

        [DllImport(LibName, EntryPoint = "surge_release_is_genesis", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int ReleaseIsGenesis(IntPtr info, int index);

        // --- Binary diff/patch ---

        [DllImport(LibName, EntryPoint = "surge_bsdiff", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int Bsdiff(IntPtr ctx);

        [DllImport(LibName, EntryPoint = "surge_bspatch", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int Bspatch(IntPtr ctx);

        [DllImport(LibName, EntryPoint = "surge_bsdiff_free", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void BsdiffFree(IntPtr ctx);

        [DllImport(LibName, EntryPoint = "surge_bspatch_free", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void BspatchFree(IntPtr ctx);

        // --- Pack ---

        [DllImport(LibName, EntryPoint = "surge_pack_create", CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi)]
        internal static extern IntPtr PackCreate(
            IntPtr ctx,
            string manifestPath,
            string appId,
            string rid,
            string version,
            string artifactsDir);

        [DllImport(LibName, EntryPoint = "surge_pack_build", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int PackBuild(
            IntPtr packCtx,
            SurgeProgressCallbackDelegate? progressCb,
            IntPtr userData);

        [DllImport(LibName, EntryPoint = "surge_pack_push", CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi)]
        internal static extern int PackPush(
            IntPtr packCtx,
            string channel,
            SurgeProgressCallbackDelegate? progressCb,
            IntPtr userData);

        [DllImport(LibName, EntryPoint = "surge_pack_destroy", CallingConvention = CallingConvention.Cdecl)]
        internal static extern void PackDestroy(IntPtr packCtx);

        // --- Lock ---

        [DllImport(LibName, EntryPoint = "surge_lock_acquire", CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi)]
        internal static extern int LockAcquire(IntPtr ctx, string name, int timeoutSeconds, out IntPtr challengeOut);

        [DllImport(LibName, EntryPoint = "surge_lock_release", CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi)]
        internal static extern int LockRelease(IntPtr ctx, string name, string challenge);

        // --- Supervisor ---

        [DllImport(LibName, EntryPoint = "surge_supervisor_start", CallingConvention = CallingConvention.Cdecl, CharSet = CharSet.Ansi)]
        internal static extern int SupervisorStart(
            string exePath,
            string workingDir,
            string supervisorId,
            int argc,
            IntPtr argv);

        // --- Lifecycle events ---

        [DllImport(LibName, EntryPoint = "surge_process_events", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int ProcessEvents(
            int argc,
            IntPtr argv,
            SurgeEventCallbackDelegate? onFirstRun,
            SurgeEventCallbackDelegate? onInstalled,
            SurgeEventCallbackDelegate? onUpdated,
            IntPtr userData);

        // --- Cancellation ---

        [DllImport(LibName, EntryPoint = "surge_cancel", CallingConvention = CallingConvention.Cdecl)]
        internal static extern int Cancel(IntPtr ctx);
#pragma warning restore CA2101
#endif
    }
}

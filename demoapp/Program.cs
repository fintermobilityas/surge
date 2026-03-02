using System;
using System.Threading;
using System.Threading.Tasks;
using Surge;

namespace DemoApp;

internal static class Program
{
    static async Task<int> Main(string[] args)
    {
        Console.WriteLine($"Surge Demo App v{SurgeApp.Version}");
        Console.WriteLine($"Working Directory: {SurgeApp.WorkingDirectory}");

        var appInfo = SurgeApp.Current;
        if (appInfo != null)
        {
            Console.WriteLine($"App ID: {appInfo.Id}");
            Console.WriteLine($"Installed Version: {appInfo.Version}");
            Console.WriteLine($"Channel: {appInfo.Channel}");
            Console.WriteLine($"Install Directory: {appInfo.InstallDirectory}");
        }
        else
        {
            Console.WriteLine("Not running inside a Surge-managed installation.");
            Console.WriteLine("Place a .surge/surge.yml manifest next to the executable to enable Surge features.");
            return 0;
        }

        // Process lifecycle events
        SurgeApp.ProcessEvents(args,
            onFirstRun: version => Console.WriteLine($"[Event] First run! Version: {version}"),
            onInstalled: version => Console.WriteLine($"[Event] Installed! Version: {version}"),
            onUpdated: version => Console.WriteLine($"[Event] Updated to version: {version}"));

        // Start supervisor for automatic restarts
        if (SurgeApp.StartSupervisor())
        {
            Console.WriteLine("[Supervisor] Started successfully.");
        }

        // Check for updates
        Console.WriteLine("\n[Update] Checking for updates...");

        using var cts = new CancellationTokenSource();

        // Allow Ctrl+C to cancel the update
        Console.CancelKeyPress += (_, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
            Console.WriteLine("\n[Update] Cancellation requested...");
        };

        try
        {
            using var updateManager = new SurgeUpdateManager();

            var progressSource = new SurgeProgressSource
            {
                DownloadProgress = p =>
                    Console.Write($"\r[Download] {p.PhasePercent}% ({FormatBytes(p.BytesDone)}/{FormatBytes(p.BytesTotal)}) @ {FormatSpeed(p.SpeedBytesPerSec)}    "),
                VerifyProgress = p =>
                    Console.Write($"\r[Verify] {p.PhasePercent}% ({p.ItemsDone}/{p.ItemsTotal} files)    "),
                ExtractProgress = p =>
                    Console.Write($"\r[Extract] {p.PhasePercent}% ({p.ItemsDone}/{p.ItemsTotal} files)    "),
                ApplyDeltaProgress = p =>
                    Console.Write($"\r[Delta] {p.PhasePercent}% ({p.ItemsDone}/{p.ItemsTotal} files)    "),
                TotalProgress = p =>
                    Console.Title = $"Surge Demo - Update {p.TotalPercent}%"
            };

            var result = await updateManager.UpdateToLatestReleaseAsync(
                progressSource: progressSource,
                onUpdatesAvailable: releases =>
                {
                    Console.WriteLine($"\n[Update] {releases.Count} update(s) available on channel '{releases.Channel}'.");
                    if (releases.Latest != null)
                    {
                        Console.WriteLine($"[Update] Latest: v{releases.Latest.Version} " +
                                          $"({FormatBytes(releases.Latest.FullSize)})");
                        if (!string.IsNullOrEmpty(releases.Latest.ReleaseNotes))
                            Console.WriteLine($"[Update] Notes: {releases.Latest.ReleaseNotes}");
                    }
                },
                onBeforeApplyUpdate: release =>
                    Console.WriteLine($"\n[Update] Applying v{release.Version}..."),
                onAfterApplyUpdate: release =>
                    Console.WriteLine($"[Update] Successfully applied v{release.Version}."),
                onApplyUpdateException: (release, ex) =>
                    Console.Error.WriteLine($"[Update] Failed to apply v{release.Version}: {ex.Message}"),
                cancellationToken: cts.Token);

            Console.WriteLine();

            if (result != null)
            {
                Console.WriteLine($"[Update] Updated to v{result.Version}.");
                Console.WriteLine("[Update] Restart the application to use the new version.");
            }
            else
            {
                Console.WriteLine("[Update] No updates available. You are running the latest version.");
            }
        }
        catch (InvalidOperationException ex)
        {
            Console.Error.WriteLine($"[Update] Cannot check for updates: {ex.Message}");
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("[Update] Update check was cancelled.");
        }
        catch (SurgeException ex)
        {
            Console.Error.WriteLine($"[Update] Error (code {ex.NativeErrorCode}): {ex.Message}");
            return 1;
        }

        return 0;
    }

    private static string FormatBytes(long bytes)
    {
        if (bytes < 1024) return $"{bytes} B";
        if (bytes < 1024 * 1024) return $"{bytes / 1024.0:F1} KB";
        if (bytes < 1024 * 1024 * 1024) return $"{bytes / (1024.0 * 1024.0):F1} MB";
        return $"{bytes / (1024.0 * 1024.0 * 1024.0):F2} GB";
    }

    private static string FormatSpeed(double bytesPerSec)
    {
        if (bytesPerSec < 1024) return $"{bytesPerSec:F0} B/s";
        if (bytesPerSec < 1024 * 1024) return $"{bytesPerSec / 1024.0:F1} KB/s";
        return $"{bytesPerSec / (1024.0 * 1024.0):F1} MB/s";
    }
}

using System;

namespace Surge
{
    /// <summary>
    /// Phases of an update operation reported through progress callbacks.
    /// </summary>
    public enum SurgeProgressPhase
    {
        Check = 0,
        Download = 1,
        Verify = 2,
        Extract = 3,
        ApplyDelta = 4,
        Finalize = 5
    }

    /// <summary>
    /// Snapshot of update progress at a point in time.
    /// </summary>
    public readonly struct SurgeProgress
    {
        /// <summary>Current phase of the update operation.</summary>
        public SurgeProgressPhase Phase { get; init; }

        /// <summary>Percent complete within the current phase (0-100).</summary>
        public int PhasePercent { get; init; }

        /// <summary>Overall percent complete across all phases (0-100).</summary>
        public int TotalPercent { get; init; }

        /// <summary>Bytes processed so far in the current operation.</summary>
        public long BytesDone { get; init; }

        /// <summary>Total bytes expected in the current operation.</summary>
        public long BytesTotal { get; init; }

        /// <summary>Items (files) processed so far.</summary>
        public long ItemsDone { get; init; }

        /// <summary>Total items (files) expected.</summary>
        public long ItemsTotal { get; init; }

        /// <summary>Current transfer speed in bytes per second.</summary>
        public double SpeedBytesPerSec { get; init; }
    }

    /// <summary>
    /// Interface for receiving progress notifications during update operations.
    /// </summary>
    public interface ISurgeProgressSource
    {
        Action<SurgeProgress>? DownloadProgress { get; set; }
        Action<SurgeProgress>? VerifyProgress { get; set; }
        Action<SurgeProgress>? ExtractProgress { get; set; }
        Action<SurgeProgress>? ApplyDeltaProgress { get; set; }
        Action<SurgeProgress>? TotalProgress { get; set; }
    }

    /// <summary>
    /// Default implementation of <see cref="ISurgeProgressSource"/> with settable callbacks.
    /// </summary>
    public sealed class SurgeProgressSource : ISurgeProgressSource
    {
        public Action<SurgeProgress>? DownloadProgress { get; set; }
        public Action<SurgeProgress>? VerifyProgress { get; set; }
        public Action<SurgeProgress>? ExtractProgress { get; set; }
        public Action<SurgeProgress>? ApplyDeltaProgress { get; set; }
        public Action<SurgeProgress>? TotalProgress { get; set; }
    }
}

namespace Surge
{
    /// <summary>
    /// Configuration for resource usage limits during update operations.
    /// Maps to the native surge_resource_budget struct.
    /// </summary>
    public sealed class SurgeResourceBudget
    {
        /// <summary>
        /// Maximum memory usage in bytes. 0 means unlimited.
        /// </summary>
        public long MaxMemoryBytes { get; set; }

        /// <summary>
        /// Maximum number of threads for parallel operations. 0 means auto-detect.
        /// </summary>
        public int MaxThreads { get; set; }

        /// <summary>
        /// Maximum number of concurrent download connections.
        /// </summary>
        public int MaxConcurrentDownloads { get; set; } = 4;

        /// <summary>
        /// Maximum download speed in bytes per second. 0 means unlimited.
        /// </summary>
        public long MaxDownloadSpeedBps { get; set; }

        /// <summary>
        /// Zstandard compression level (1-22). Higher values use more CPU but produce smaller packages.
        /// </summary>
        public int ZstdCompressionLevel { get; set; } = 9;

        internal SurgeResourceBudgetNative ToNative()
        {
            return new SurgeResourceBudgetNative
            {
                MaxMemoryBytes = MaxMemoryBytes,
                MaxThreads = MaxThreads,
                MaxConcurrentDownloads = MaxConcurrentDownloads,
                MaxDownloadSpeedBps = MaxDownloadSpeedBps,
                ZstdCompressionLevel = ZstdCompressionLevel
            };
        }
    }
}

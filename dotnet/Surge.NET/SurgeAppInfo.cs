namespace Surge
{
    /// <summary>
    /// Information about the currently installed application managed by Surge.
    /// </summary>
    public sealed class SurgeAppInfo
    {
        /// <summary>
        /// Application identifier as defined in the surge.yml manifest.
        /// </summary>
        public string Id { get; init; } = "";

        /// <summary>
        /// Currently installed version string (semver).
        /// </summary>
        public string Version { get; init; } = "";

        /// <summary>
        /// Release channel this installation is tracking.
        /// </summary>
        public string Channel { get; init; } = "";

        /// <summary>
        /// Root installation directory for this application.
        /// </summary>
        public string InstallDirectory { get; init; } = "";

        /// <summary>
        /// Storage provider used for updates (filesystem, s3, azure, gcs).
        /// </summary>
        public string StorageProvider { get; init; } = "";

        /// <summary>
        /// Storage bucket, container, or root path.
        /// </summary>
        public string StorageBucket { get; init; } = "";

        /// <summary>
        /// Optional storage region.
        /// </summary>
        public string StorageRegion { get; init; } = "";

        /// <summary>
        /// Optional storage endpoint.
        /// </summary>
        public string StorageEndpoint { get; init; } = "";

        /// <summary>
        /// Whether the supervisor process is currently running for this application.
        /// </summary>
        public bool IsSupervisorRunning { get; internal set; }
    }
}

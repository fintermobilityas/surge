namespace Surge
{
    /// <summary>
    /// Information about a single release available for installation or update.
    /// </summary>
    public sealed class SurgeRelease
    {
        /// <summary>
        /// Version string (semver) of this release.
        /// </summary>
        public string Version { get; init; } = "";

        /// <summary>
        /// Release channel this release belongs to.
        /// </summary>
        public string Channel { get; init; } = "";

        /// <summary>
        /// Size in bytes of the full release package.
        /// </summary>
        public long FullSize { get; init; }

        /// <summary>
        /// Size in bytes of the delta package from the previous version,
        /// or 0 if no delta is available.
        /// </summary>
        public long DeltaSize { get; init; }

        /// <summary>
        /// Release notes text, if available.
        /// </summary>
        public string ReleaseNotes { get; init; } = "";

        /// <summary>
        /// Whether this is a genesis (initial) release with no prior version.
        /// </summary>
        public bool IsGenesis { get; init; }
    }
}

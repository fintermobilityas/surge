using System.Collections.Generic;

namespace Surge
{
    /// <summary>
    /// Provides access to the list of available releases for a channel.
    /// Passed to the onUpdatesAvailable callback during update checks.
    /// </summary>
    public interface ISurgeChannelReleases
    {
        /// <summary>
        /// The channel name these releases belong to.
        /// </summary>
        string Channel { get; }

        /// <summary>
        /// All available releases in this channel, ordered oldest to newest.
        /// </summary>
        IReadOnlyList<SurgeRelease> Releases { get; }

        /// <summary>
        /// The latest available release, or null if no releases exist.
        /// </summary>
        SurgeRelease? Latest { get; }

        /// <summary>
        /// Number of available releases.
        /// </summary>
        int Count { get; }
    }

    /// <summary>
    /// Default implementation of <see cref="ISurgeChannelReleases"/> backed by a native releases handle.
    /// </summary>
    internal sealed class SurgeChannelReleases : ISurgeChannelReleases
    {
        private readonly List<SurgeRelease> _releases;

        public string Channel { get; }
        public IReadOnlyList<SurgeRelease> Releases => _releases;
        public SurgeRelease? Latest => _releases.Count > 0 ? _releases[_releases.Count - 1] : null;
        public int Count => _releases.Count;

        internal SurgeChannelReleases(string channel, List<SurgeRelease> releases)
        {
            Channel = channel;
            _releases = releases;
        }
    }
}

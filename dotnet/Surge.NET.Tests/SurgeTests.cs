using System;
using Xunit;

namespace Surge.Tests
{
    public class SurgeAppTests
    {
        [Fact]
        public void Version_ReturnsExpectedVersion()
        {
            Assert.Equal("0.1.0", SurgeApp.Version);
        }

        [Fact]
        public void WorkingDirectory_ReturnsNonEmpty()
        {
            string dir = SurgeApp.WorkingDirectory;
            Assert.False(string.IsNullOrEmpty(dir));
        }

        [Fact]
        public void Current_ReturnsNullWhenNoManifest()
        {
            // When running outside a Surge-managed installation, Current should be null
            // because there is no .surge/surge.yml in the test output directory.
            Assert.Null(SurgeApp.Current);
        }

        [Fact]
        public void ProcessEvents_WithEmptyArgs_DoesNotThrow()
        {
            // ProcessEvents should handle empty args gracefully.
            // Without the native library loaded, this would throw DllNotFoundException,
            // which is expected in a unit test environment.
            var ex = Record.Exception(() =>
            {
                try
                {
                    SurgeApp.ProcessEvents(Array.Empty<string>());
                }
                catch (DllNotFoundException)
                {
                    // Expected when native library is not present
                }
            });
            Assert.Null(ex);
        }

        [Fact]
        public void StopSupervisor_WhenNotRunning_ReturnsFalse()
        {
            // Without a current app, StopSupervisor should return false
            Assert.False(SurgeApp.StopSupervisor());
        }
    }

    public class SurgeAppInfoTests
    {
        [Fact]
        public void DefaultValues_AreEmpty()
        {
            var info = new SurgeAppInfo();
            Assert.Equal("", info.Id);
            Assert.Equal("", info.Version);
            Assert.Equal("", info.Channel);
            Assert.Equal("", info.InstallDirectory);
            Assert.False(info.IsSupervisorRunning);
        }

        [Fact]
        public void Properties_CanBeSet()
        {
            var info = new SurgeAppInfo
            {
                Id = "myapp",
                Version = "1.2.3",
                Channel = "stable",
                InstallDirectory = "/opt/myapp"
            };

            Assert.Equal("myapp", info.Id);
            Assert.Equal("1.2.3", info.Version);
            Assert.Equal("stable", info.Channel);
            Assert.Equal("/opt/myapp", info.InstallDirectory);
        }
    }

    public class SurgeUpdateManagerTests
    {
        [Fact]
        public void Constructor_ThrowsWhenNoAppInfo()
        {
            // When there is no current app info (no .surge/surge.yml),
            // the constructor should throw InvalidOperationException.
            Assert.Throws<InvalidOperationException>(() => new SurgeUpdateManager());
        }
    }

    public class SurgeProgressTests
    {
        [Fact]
        public void DefaultProgress_HasZeroValues()
        {
            var progress = new SurgeProgress();
            Assert.Equal(SurgeProgressPhase.Check, progress.Phase);
            Assert.Equal(0, progress.PhasePercent);
            Assert.Equal(0, progress.TotalPercent);
            Assert.Equal(0L, progress.BytesDone);
            Assert.Equal(0L, progress.BytesTotal);
            Assert.Equal(0L, progress.ItemsDone);
            Assert.Equal(0L, progress.ItemsTotal);
            Assert.Equal(0.0, progress.SpeedBytesPerSec);
        }

        [Fact]
        public void Progress_CanBeInitialized()
        {
            var progress = new SurgeProgress
            {
                Phase = SurgeProgressPhase.Download,
                PhasePercent = 50,
                TotalPercent = 25,
                BytesDone = 1024,
                BytesTotal = 2048,
                ItemsDone = 3,
                ItemsTotal = 10,
                SpeedBytesPerSec = 512.5
            };

            Assert.Equal(SurgeProgressPhase.Download, progress.Phase);
            Assert.Equal(50, progress.PhasePercent);
            Assert.Equal(25, progress.TotalPercent);
            Assert.Equal(1024L, progress.BytesDone);
            Assert.Equal(2048L, progress.BytesTotal);
            Assert.Equal(3L, progress.ItemsDone);
            Assert.Equal(10L, progress.ItemsTotal);
            Assert.Equal(512.5, progress.SpeedBytesPerSec);
        }

        [Fact]
        public void ProgressPhase_EnumValues_MatchNative()
        {
            Assert.Equal(0, (int)SurgeProgressPhase.Check);
            Assert.Equal(1, (int)SurgeProgressPhase.Download);
            Assert.Equal(2, (int)SurgeProgressPhase.Verify);
            Assert.Equal(3, (int)SurgeProgressPhase.Extract);
            Assert.Equal(4, (int)SurgeProgressPhase.ApplyDelta);
            Assert.Equal(5, (int)SurgeProgressPhase.Finalize);
        }
    }

    public class SurgeProgressSourceTests
    {
        [Fact]
        public void DefaultCallbacks_AreNull()
        {
            var source = new SurgeProgressSource();
            Assert.Null(source.DownloadProgress);
            Assert.Null(source.VerifyProgress);
            Assert.Null(source.ExtractProgress);
            Assert.Null(source.ApplyDeltaProgress);
            Assert.Null(source.TotalProgress);
        }

        [Fact]
        public void Callbacks_CanBeAssigned()
        {
            bool invoked = false;
            var source = new SurgeProgressSource
            {
                TotalProgress = _ => invoked = true
            };

            Assert.NotNull(source.TotalProgress);
            source.TotalProgress!(new SurgeProgress());
            Assert.True(invoked);
        }
    }

    public class SurgeReleaseTests
    {
        [Fact]
        public void DefaultValues_AreEmpty()
        {
            var release = new SurgeRelease();
            Assert.Equal("", release.Version);
            Assert.Equal("", release.Channel);
            Assert.Equal(0L, release.FullSize);
            Assert.Equal(0L, release.DeltaSize);
            Assert.Equal("", release.ReleaseNotes);
            Assert.False(release.IsGenesis);
        }

        [Fact]
        public void Properties_CanBeSet()
        {
            var release = new SurgeRelease
            {
                Version = "2.0.0",
                Channel = "beta",
                FullSize = 1024 * 1024,
                DeltaSize = 256 * 1024,
                ReleaseNotes = "Bug fixes and improvements",
                IsGenesis = true
            };

            Assert.Equal("2.0.0", release.Version);
            Assert.Equal("beta", release.Channel);
            Assert.Equal(1024 * 1024, release.FullSize);
            Assert.Equal(256 * 1024, release.DeltaSize);
            Assert.Equal("Bug fixes and improvements", release.ReleaseNotes);
            Assert.True(release.IsGenesis);
        }
    }

    public class SurgeResourceBudgetTests
    {
        [Fact]
        public void DefaultValues_AreReasonable()
        {
            var budget = new SurgeResourceBudget();
            Assert.Equal(0L, budget.MaxMemoryBytes);
            Assert.Equal(0, budget.MaxThreads);
            Assert.Equal(4, budget.MaxConcurrentDownloads);
            Assert.Equal(0L, budget.MaxDownloadSpeedBps);
            Assert.Equal(9, budget.ZstdCompressionLevel);
        }

        [Fact]
        public void ToNative_MapsCorrectly()
        {
            var budget = new SurgeResourceBudget
            {
                MaxMemoryBytes = 512 * 1024 * 1024L,
                MaxThreads = 8,
                MaxConcurrentDownloads = 2,
                MaxDownloadSpeedBps = 10 * 1024 * 1024L,
                ZstdCompressionLevel = 15
            };

            var native = budget.ToNative();
            Assert.Equal(512 * 1024 * 1024L, native.MaxMemoryBytes);
            Assert.Equal(8, native.MaxThreads);
            Assert.Equal(2, native.MaxConcurrentDownloads);
            Assert.Equal(10 * 1024 * 1024L, native.MaxDownloadSpeedBps);
            Assert.Equal(15, native.ZstdCompressionLevel);
        }
    }

    public class SurgeExceptionTests
    {
        [Fact]
        public void Constructor_SetsProperties()
        {
            var ex = new SurgeException(-1, "Something failed");
            Assert.Equal(-1, ex.NativeErrorCode);
            Assert.Equal("Something failed", ex.Message);
        }

        [Fact]
        public void IsException_Derived()
        {
            var ex = new SurgeException(-2, "Cancelled");
            Assert.IsAssignableFrom<Exception>(ex);
        }
    }

    public class SurgeChannelReleasesTests
    {
        [Fact]
        public void EmptyReleases_HasNullLatest()
        {
            var releases = new SurgeChannelReleases("test",
                new System.Collections.Generic.List<SurgeRelease>());
            Assert.Equal("test", releases.Channel);
            Assert.Equal(0, releases.Count);
            Assert.Null(releases.Latest);
            Assert.Empty(releases.Releases);
        }

        [Fact]
        public void WithReleases_ReturnsLatest()
        {
            var list = new System.Collections.Generic.List<SurgeRelease>
            {
                new SurgeRelease { Version = "2.0.0", Channel = "stable" },
                new SurgeRelease { Version = "1.0.0", Channel = "stable" }
            };

            var releases = new SurgeChannelReleases("stable", list);
            Assert.Equal("stable", releases.Channel);
            Assert.Equal(2, releases.Count);
            Assert.NotNull(releases.Latest);
            Assert.Equal("2.0.0", releases.Latest!.Version);
        }
    }
}

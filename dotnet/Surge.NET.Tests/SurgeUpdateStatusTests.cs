using Xunit;

namespace Surge.Tests
{
    public class SurgeUpdateStatusTests
    {
        [Fact]
        public void Parse_ConvergedRecord_RoundTrip()
        {
            const string json = """
                {
                  "state": "converged",
                  "installed_version": "9999.0.0",
                  "target_version": "9999.0.0",
                  "channel": "stable",
                  "app_id": "demo-app",
                  "supervisor_restart_confirmed": true,
                  "attempted_at_utc": "2026-05-11T14:00:00Z",
                  "completed_at_utc": "2026-05-11T14:05:00Z"
                }
                """;

            var status = SurgeUpdateStatus.Parse(json);
            Assert.NotNull(status);
            Assert.Equal(SurgeUpdateConvergenceState.Converged, status!.State);
            Assert.Equal("9999.0.0", status.InstalledVersion);
            Assert.Equal("9999.0.0", status.TargetVersion);
            Assert.Equal("stable", status.Channel);
            Assert.Equal("demo-app", status.AppId);
            Assert.True(status.SupervisorRestartConfirmed);
            Assert.Equal("2026-05-11T14:00:00Z", status.AttemptedAtUtc);
            Assert.Equal("2026-05-11T14:05:00Z", status.CompletedAtUtc);
            Assert.Null(status.Reason);
        }

        [Fact]
        public void Parse_PendingRestart_PreservesReason()
        {
            const string json = """
                {
                  "state": "pending_restart",
                  "installed_version": "9999.0.0",
                  "target_version": "9999.0.0",
                  "channel": "stable",
                  "app_id": "demo-app",
                  "supervisor_restart_confirmed": false,
                  "attempted_at_utc": "2026-05-11T14:00:00Z",
                  "completed_at_utc": "2026-05-11T14:05:00Z",
                  "reason": "supervisor pid file did not appear within 5000ms after restart"
                }
                """;

            var status = SurgeUpdateStatus.Parse(json);
            Assert.NotNull(status);
            Assert.Equal(SurgeUpdateConvergenceState.PendingRestart, status!.State);
            Assert.False(status.SupervisorRestartConfirmed);
            Assert.Contains("supervisor pid file did not appear", status.Reason);
        }

        [Fact]
        public void Parse_FailedRecord_PreservesPreAttemptVersion()
        {
            const string json = """
                {
                  "state": "failed",
                  "installed_version": "9998.0.0",
                  "target_version": "9999.0.0",
                  "channel": "stable",
                  "app_id": "demo-app",
                  "supervisor_restart_confirmed": false,
                  "attempted_at_utc": "2026-05-11T14:00:00Z",
                  "reason": "storage backend returned 503"
                }
                """;

            var status = SurgeUpdateStatus.Parse(json);
            Assert.NotNull(status);
            Assert.Equal(SurgeUpdateConvergenceState.Failed, status!.State);
            Assert.Equal("9998.0.0", status.InstalledVersion);
            Assert.Equal("9999.0.0", status.TargetVersion);
            Assert.Null(status.CompletedAtUtc);
            Assert.Equal("storage backend returned 503", status.Reason);
        }

        [Fact]
        public void Parse_UnknownState_DoesNotThrow()
        {
            const string json = """
                {
                  "state": "something_brand_new",
                  "installed_version": "1.0.0",
                  "target_version": "1.0.0",
                  "channel": "stable",
                  "app_id": "demo-app",
                  "supervisor_restart_confirmed": false
                }
                """;

            var status = SurgeUpdateStatus.Parse(json);
            Assert.NotNull(status);
            Assert.Equal(SurgeUpdateConvergenceState.Unknown, status!.State);
        }

        [Fact]
        public void Parse_DecodesEscapeSequencesInReason()
        {
            const string json = """
                {
                  "state": "failed",
                  "installed_version": "1.0.0",
                  "target_version": "1.0.0",
                  "channel": "stable",
                  "app_id": "demo-app",
                  "supervisor_restart_confirmed": false,
                  "reason": "broke at \"phase 5\"\nretry pending"
                }
                """;

            var status = SurgeUpdateStatus.Parse(json);
            Assert.NotNull(status);
            Assert.Equal("broke at \"phase 5\"\nretry pending", status!.Reason);
        }

        [Fact]
        public void Parse_RejectsMalformedJson()
        {
            Assert.Null(SurgeUpdateStatus.Parse("{not json"));
            Assert.Null(SurgeUpdateStatus.Parse(""));
            Assert.Null(SurgeUpdateStatus.Parse("[]"));
        }
    }
}

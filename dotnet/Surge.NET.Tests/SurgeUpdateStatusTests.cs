using System;
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
                  "reason": "supervisor handoff accepted; waiting for previous child pid 1234 to exit",
                  "failure_phase": "restart handoff waiting for old child"
                }
                """;

            var status = SurgeUpdateStatus.Parse(json);
            Assert.NotNull(status);
            Assert.Equal(SurgeUpdateConvergenceState.PendingRestart, status!.State);
            Assert.False(status.SupervisorRestartConfirmed);
            Assert.Contains("waiting for previous child", status.Reason);
            Assert.Equal("restart handoff waiting for old child", status.FailurePhase);
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

        [Fact]
        public void Parse_FailedRecord_ReadsRetryScheduleFields()
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
                  "reason": "storage backend returned 503",
                  "retry_safe": true,
                  "next_retry_at_utc": "2026-05-11T14:05:00Z",
                  "retry_count": 2
                }
                """;

            var status = SurgeUpdateStatus.Parse(json);
            Assert.NotNull(status);
            Assert.True(status!.RetrySafe);
            Assert.Equal("2026-05-11T14:05:00Z", status.NextRetryAtUtc);
            Assert.Equal(2, status.RetryCount);
        }

        [Fact]
        public void Parse_LegacyFailedRecord_LacksRetryScheduleFields()
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
                  "reason": "storage backend returned 503",
                  "retry_safe": true
                }
                """;

            var status = SurgeUpdateStatus.Parse(json);
            Assert.NotNull(status);
            Assert.True(status!.RetrySafe);
            Assert.Null(status.NextRetryAtUtc);
            Assert.Null(status.RetryCount);
        }

        [Fact]
        public void ShouldDeferUpdate_DeferOnlyRetrySafeFailuresInsideTheirWindow()
        {
            var now = new DateTimeOffset(2026, 5, 11, 14, 0, 0, TimeSpan.Zero);

            // No record at all: never defer.
            Assert.False(SurgeUpdateStatus.ShouldDeferUpdate(null, now));

            SurgeUpdateStatus? Status(string state, bool? retrySafe, string? nextRetryAtUtc)
                => new SurgeUpdateStatus
                {
                    State = state switch
                    {
                        "failed" => SurgeUpdateConvergenceState.Failed,
                        "in_progress" => SurgeUpdateConvergenceState.InProgress,
                        _ => SurgeUpdateConvergenceState.Converged
                    },
                    RetrySafe = retrySafe,
                    NextRetryAtUtc = nextRetryAtUtc,
                };

            // Failed + retry-safe + window in the future: defer.
            Assert.True(SurgeUpdateStatus.ShouldDeferUpdate(Status("failed", true, "2026-05-11T14:05:00Z"), now));
            // Window exactly at now: not deferred (the window has elapsed).
            Assert.False(SurgeUpdateStatus.ShouldDeferUpdate(Status("failed", true, "2026-05-11T14:00:00Z"), now));
            // Window in the past: not deferred.
            Assert.False(SurgeUpdateStatus.ShouldDeferUpdate(Status("failed", true, "2026-05-11T13:55:00Z"), now));
            // No scheduled retry: never defer.
            Assert.False(SurgeUpdateStatus.ShouldDeferUpdate(Status("failed", true, null), now));
            Assert.False(SurgeUpdateStatus.ShouldDeferUpdate(Status("failed", null, "2026-05-11T14:05:00Z"), now));
            // Not retry-safe: never defer.
            Assert.False(SurgeUpdateStatus.ShouldDeferUpdate(Status("failed", false, "2026-05-11T14:05:00Z"), now));
            // Non-failed states: never defer.
            Assert.False(SurgeUpdateStatus.ShouldDeferUpdate(Status("in_progress", true, "2026-05-11T14:05:00Z"), now));
            Assert.False(SurgeUpdateStatus.ShouldDeferUpdate(Status("converged", true, "2026-05-11T14:05:00Z"), now));
            // Unparseable timestamp: fail open (do not defer).
            Assert.False(SurgeUpdateStatus.ShouldDeferUpdate(Status("failed", true, "not-a-timestamp"), now));
            // Offset form parses and compares against UTC.
            Assert.True(SurgeUpdateStatus.ShouldDeferUpdate(Status("failed", true, "2026-05-11T16:05:00+02:00"), now));
        }
    }
}

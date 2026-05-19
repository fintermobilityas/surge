using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;

namespace Surge
{
    /// <summary>
    /// Convergence state recorded by <see cref="SurgeUpdateManager"/> after every
    /// update attempt. Use this to distinguish "update in progress", "applied but
    /// pending supervisor restart", "fully converged", and "failed" without
    /// having to infer the state from version drift between the installed
    /// manifest and the running process.
    /// </summary>
    public enum SurgeUpdateConvergenceState
    {
        /// <summary>State could not be classified (record predates this enum).</summary>
        Unknown,
        /// <summary>Install completed but no update has been attempted yet.</summary>
        Idle,
        /// <summary>An update is currently being applied.</summary>
        InProgress,
        /// <summary>
        /// Latest update applied to disk and the supervisor handoff proved a
        /// replacement runtime is active.
        /// </summary>
        Converged,
        /// <summary>
        /// Latest update applied to disk but the supervisor restart could not be
        /// confirmed within the post-update window. The runtime process may still
        /// be running an older binary even though
        /// <see cref="SurgeUpdateStatus.InstalledVersion"/> already reflects the
        /// new release.
        /// </summary>
        PendingRestart,
        /// <summary>
        /// Most recent attempt failed before the install swap could complete.
        /// <see cref="SurgeUpdateStatus.InstalledVersion"/> reflects the
        /// pre-attempt state.
        /// </summary>
        Failed,
    }

    /// <summary>
    /// A point-in-time snapshot of the install's convergence to a channel
    /// release. Read via <see cref="SurgeUpdateStatus.Read(string)"/> or
    /// <see cref="SurgeUpdateManager.GetCurrentStatus"/>.
    /// </summary>
    public sealed class SurgeUpdateStatus
    {
        /// <summary>Convergence state at the time the record was written.</summary>
        public SurgeUpdateConvergenceState State { get; init; }

        /// <summary>Application identifier the record was written for.</summary>
        public string AppId { get; init; } = "";

        /// <summary>
        /// Version present in the active app directory at the time the record
        /// was written. For <see cref="SurgeUpdateConvergenceState.Failed"/>
        /// records this is the pre-attempt version; for
        /// <see cref="SurgeUpdateConvergenceState.PendingRestart"/> records this
        /// is the new release even though the runtime process may not be.
        /// </summary>
        public string InstalledVersion { get; init; } = "";

        /// <summary>
        /// Version that the most recent update attempt targeted. Equal to
        /// <see cref="InstalledVersion"/> for converged records.
        /// </summary>
        public string TargetVersion { get; init; } = "";

        /// <summary>Channel the install is tracking.</summary>
        public string Channel { get; init; } = "";

        /// <summary>
        /// True when a supervisor was configured for this release and the
        /// post-update handoff proved a target-version child is active. When no
        /// supervisor was configured this is false and carries no signal — read
        /// <see cref="State"/> for convergence.
        /// </summary>
        public bool SupervisorRestartConfirmed { get; init; }

        /// <summary>UTC timestamp (RFC 3339) the attempt began, if known.</summary>
        public string? AttemptedAtUtc { get; init; }

        /// <summary>UTC timestamp (RFC 3339) the attempt completed, if known.</summary>
        public string? CompletedAtUtc { get; init; }

        /// <summary>
        /// Human-readable reason for <see cref="SurgeUpdateConvergenceState.Failed"/>
        /// and <see cref="SurgeUpdateConvergenceState.PendingRestart"/> records.
        /// </summary>
        public string? Reason { get; init; }

        /// <summary>
        /// Phase active when a terminal failure or pending restart state was
        /// recorded. For restart handoff records this distinguishes cases such
        /// as waiting for the previous child from target child exit.
        /// </summary>
        public string? FailurePhase { get; init; }

        /// <summary>
        /// Read the persisted update convergence record from <paramref name="installDirectory"/>.
        /// Returns <c>null</c> when no record has been written yet (e.g. clean
        /// install that has never run an update).
        /// </summary>
        public static SurgeUpdateStatus? Read(string installDirectory)
        {
            if (string.IsNullOrWhiteSpace(installDirectory))
                throw new ArgumentException("Install directory must be provided.", nameof(installDirectory));

            int rc = NativeMethods.UpdateStatusReadJson(installDirectory, out IntPtr jsonPtr);
            try
            {
                if (rc != 0 || jsonPtr == IntPtr.Zero)
                    return null;

                string? json = MarshalUtf8(jsonPtr);
                if (string.IsNullOrWhiteSpace(json))
                    return null;
                return Parse(json!);
            }
            finally
            {
                if (jsonPtr != IntPtr.Zero)
                    NativeMethods.FreeCString(jsonPtr);
            }
        }

        /// <summary>
        /// Parse a JSON record written by surge-core's <c>update::status</c>
        /// module. Exposed for tests; production code should use
        /// <see cref="Read(string)"/>.
        /// </summary>
        internal static SurgeUpdateStatus? Parse(string json)
        {
            var fields = ParseFlatJsonObject(json);
            if (fields == null)
                return null;

            return new SurgeUpdateStatus
            {
                State = ParseState(GetString(fields, "state")),
                AppId = GetString(fields, "app_id") ?? "",
                InstalledVersion = GetString(fields, "installed_version") ?? "",
                TargetVersion = GetString(fields, "target_version") ?? "",
                Channel = GetString(fields, "channel") ?? "",
                SupervisorRestartConfirmed = GetBool(fields, "supervisor_restart_confirmed"),
                AttemptedAtUtc = NullIfEmpty(GetString(fields, "attempted_at_utc")),
                CompletedAtUtc = NullIfEmpty(GetString(fields, "completed_at_utc")),
                Reason = NullIfEmpty(GetString(fields, "reason")),
                FailurePhase = NullIfEmpty(GetString(fields, "failure_phase")),
            };
        }

        private static SurgeUpdateConvergenceState ParseState(string? raw)
        {
            return (raw ?? "").Trim() switch
            {
                "idle" => SurgeUpdateConvergenceState.Idle,
                "in_progress" => SurgeUpdateConvergenceState.InProgress,
                "converged" => SurgeUpdateConvergenceState.Converged,
                "pending_restart" => SurgeUpdateConvergenceState.PendingRestart,
                "failed" => SurgeUpdateConvergenceState.Failed,
                _ => SurgeUpdateConvergenceState.Unknown,
            };
        }

        private static string? NullIfEmpty(string? value) => string.IsNullOrWhiteSpace(value) ? null : value;

        private static string? GetString(Dictionary<string, JsonValue> fields, string key)
        {
            return fields.TryGetValue(key, out var value) && value.Kind == JsonValueKind.String ? value.StringValue : null;
        }

        private static bool GetBool(Dictionary<string, JsonValue> fields, string key)
        {
            return fields.TryGetValue(key, out var value) && value.Kind == JsonValueKind.Bool && value.BoolValue;
        }

        private static string? MarshalUtf8(IntPtr ptr)
        {
#if NETSTANDARD2_0
            return MarshalHelper.PtrToStringUTF8(ptr);
#else
            return Marshal.PtrToStringUTF8(ptr);
#endif
        }

        // ----------------------------------------------------------------------
        // Tiny hand-written JSON parser for one shallow object with string/bool
        // values. Keeps the .NET wrapper free of System.Text.Json package and
        // AOT/trim warnings while matching the schema written by surge-core's
        // update::status module exactly.
        // ----------------------------------------------------------------------

        private enum JsonValueKind { Null, String, Bool }

        private readonly struct JsonValue
        {
            public JsonValueKind Kind { get; }
            public string? StringValue { get; }
            public bool BoolValue { get; }

            public static readonly JsonValue Null = new JsonValue(JsonValueKind.Null, null, false);
            public static JsonValue OfString(string s) => new JsonValue(JsonValueKind.String, s, false);
            public static JsonValue OfBool(bool b) => new JsonValue(JsonValueKind.Bool, null, b);

            private JsonValue(JsonValueKind kind, string? stringValue, bool boolValue)
            {
                Kind = kind;
                StringValue = stringValue;
                BoolValue = boolValue;
            }
        }

        private static Dictionary<string, JsonValue>? ParseFlatJsonObject(string json)
        {
            var dict = new Dictionary<string, JsonValue>(StringComparer.Ordinal);
            int i = 0;
            if (!SkipWhitespace(json, ref i) || i >= json.Length || json[i] != '{')
                return null;
            i++;

            while (true)
            {
                if (!SkipWhitespace(json, ref i))
                    return null;
                if (i >= json.Length)
                    return null;
                if (json[i] == '}') { i++; break; }

                if (!TryReadString(json, ref i, out string key))
                    return null;
                if (!SkipWhitespace(json, ref i) || i >= json.Length || json[i] != ':')
                    return null;
                i++;
                if (!SkipWhitespace(json, ref i) || i >= json.Length)
                    return null;

                JsonValue value;
                if (json[i] == '"')
                {
                    if (!TryReadString(json, ref i, out string sv))
                        return null;
                    value = JsonValue.OfString(sv);
                }
                else if (TryReadKeyword(json, ref i, "true"))
                    value = JsonValue.OfBool(true);
                else if (TryReadKeyword(json, ref i, "false"))
                    value = JsonValue.OfBool(false);
                else if (TryReadKeyword(json, ref i, "null"))
                    value = JsonValue.Null;
                else
                    return null;

                dict[key] = value;

                if (!SkipWhitespace(json, ref i) || i >= json.Length)
                    return null;
                if (json[i] == ',') { i++; continue; }
                if (json[i] == '}') { i++; break; }
                return null;
            }

            return dict;
        }

        private static bool SkipWhitespace(string s, ref int i)
        {
            while (i < s.Length)
            {
                char c = s[i];
                if (c == ' ' || c == '\t' || c == '\r' || c == '\n')
                    i++;
                else
                    return true;
            }
            return true;
        }

        private static bool TryReadKeyword(string s, ref int i, string keyword)
        {
            if (i + keyword.Length > s.Length)
                return false;
            for (int k = 0; k < keyword.Length; k++)
            {
                if (s[i + k] != keyword[k])
                    return false;
            }
            i += keyword.Length;
            return true;
        }

        private static bool TryReadString(string s, ref int i, out string value)
        {
            value = "";
            if (i >= s.Length || s[i] != '"')
                return false;
            i++;

            var sb = new StringBuilder();
            while (i < s.Length)
            {
                char c = s[i++];
                if (c == '"')
                {
                    value = sb.ToString();
                    return true;
                }
                if (c != '\\')
                {
                    sb.Append(c);
                    continue;
                }
                if (i >= s.Length)
                    return false;
                char esc = s[i++];
                switch (esc)
                {
                    case '"': sb.Append('"'); break;
                    case '\\': sb.Append('\\'); break;
                    case '/': sb.Append('/'); break;
                    case 'b': sb.Append('\b'); break;
                    case 'f': sb.Append('\f'); break;
                    case 'n': sb.Append('\n'); break;
                    case 'r': sb.Append('\r'); break;
                    case 't': sb.Append('\t'); break;
                    case 'u':
                        if (i + 4 > s.Length)
                            return false;
#if NETSTANDARD2_0
                        if (!ushort.TryParse(s.Substring(i, 4), System.Globalization.NumberStyles.HexNumber,
                            System.Globalization.CultureInfo.InvariantCulture, out ushort code))
                            return false;
#else
                        if (!ushort.TryParse(s.AsSpan(i, 4), System.Globalization.NumberStyles.HexNumber,
                            System.Globalization.CultureInfo.InvariantCulture, out ushort code))
                            return false;
#endif
                        sb.Append((char)code);
                        i += 4;
                        break;
                    default:
                        return false;
                }
            }
            return false;
        }
    }
}

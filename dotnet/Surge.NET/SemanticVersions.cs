using System;
using Semver;

namespace Surge
{
    internal static class SemanticVersions
    {
        internal static readonly SemVersion Zero = new SemVersion(0, 0, 0);
        internal static readonly SemVersion LibraryVersion = ParseArgument("0.1.0", "libraryVersion");

        internal static SemVersion ParseArgument(string value, string paramName)
        {
            if (value == null)
                throw new ArgumentNullException(paramName);

            if (!SemVersion.TryParse(value, SemVersionStyles.Strict, out var version))
                throw new ArgumentException(
                    $"Version must be a valid Semantic Versioning 2.0 value: '{value}'.",
                    paramName);

            return version;
        }

        internal static SemVersion ParseRuntimeValue(string value, string source)
        {
            if (string.IsNullOrWhiteSpace(value))
                throw new InvalidOperationException($"{source} is missing.");

            if (!SemVersion.TryParse(value, SemVersionStyles.Strict, out var version))
                throw new InvalidOperationException(
                    $"{source} is not a valid Semantic Versioning 2.0 value: '{value}'.");

            return version;
        }

        internal static bool TryParseRuntimeValue(string value, out SemVersion version)
        {
            version = Zero;
            return !string.IsNullOrWhiteSpace(value)
                && SemVersion.TryParse(value, SemVersionStyles.Strict, out version);
        }
    }
}

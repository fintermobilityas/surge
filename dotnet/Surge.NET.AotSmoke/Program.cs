using System;
using Surge;

var release = new SurgeRelease
{
    Version = "1.0.0",
    Channel = "stable",
    FullSize = 42,
    IsGenesis = true
};

var budget = new SurgeResourceBudget { MaxThreads = 2 };
var retention = SurgeArtifactRetentionPolicy.LatestFull(2);
var lifecycleCallbackVersion = "";
var lifecycleHandled = SurgeApp.ProcessEvents(
    Array.Empty<string>(),
    onFirstRun: version => lifecycleCallbackVersion = version,
    onInstalled: version => lifecycleCallbackVersion = version,
    onUpdated: version => lifecycleCallbackVersion = version);

if (release.Version != "1.0.0"
    || release.Channel != "stable"
    || budget.MaxThreads != 2
    || retention.Mode != SurgeArtifactRetentionMode.LatestFull
    || lifecycleHandled
    || lifecycleCallbackVersion.Length != 0)
{
    return 1;
}

Console.WriteLine($"{SurgeApp.Version}:{release.Version}:{retention.KeepFullCount}");
return 0;

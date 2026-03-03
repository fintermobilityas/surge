# Surge

**Automatic updates for any application.** Surge.NET is the .NET wrapper for the [Surge](https://github.com/fintermobilityas/surge) native update framework.

## Target Frameworks

| Target | Binding | Compatibility |
|---|---|---|
| `netstandard2.0` | `[DllImport]` | .NET Framework 4.6.1+, .NET Core, Mono, Xamarin |
| `net10.0` | `[LibraryImport]` | Full AOT and trimming support |

## Quick Start

```csharp
using var mgr = new SurgeUpdateManager();
await mgr.UpdateToLatestReleaseAsync(
    onUpdatesAvailable: releases =>
        Console.WriteLine($"{releases.Count} update(s), latest: {releases.Latest?.Version}"),
    onAfterApplyUpdate: release =>
        Console.WriteLine($"Updated to {release.Version}")
);
```

One call that checks for updates, downloads the smallest available patch, verifies integrity, extracts, and applies — with progress callbacks and cancellation support.

## Key Features

- **Zero dependencies** — no external NuGet packages required
- **Delta patches** — binary diffs (bsdiff + zstd) mean users download only what changed, typically 5–20% of the full package
- **Release channels** — ship to `beta` first, then promote the exact same build to `stable`
- **Persistent assets** — config files, databases, and user content survive across updates
- **Cross-platform** — Linux, Windows, and macOS with native shortcuts and platform-correct install directories
- **Progress & cancellation** — per-phase progress callbacks and `CancellationToken` support

## Storage Backends

Releases are stored directly in cloud storage — no update server required.

- Amazon S3 (and compatible: MinIO, Cloudflare R2, DigitalOcean Spaces)
- Azure Blob Storage
- Google Cloud Storage
- GitHub Releases
- Local filesystem

## Links

- [GitHub Repository](https://github.com/fintermobilityas/surge)
- [Full Documentation](https://github.com/fintermobilityas/surge#readme)
- [License (MIT)](https://github.com/fintermobilityas/surge/blob/main/LICENSE)

<p align="center">
  <img src="assets/logo.svg" alt="Surge" width="128" />
</p>

<h1 align="center">Surge</h1>

<p align="center">
  Automatic updates for any application. Built in Rust. Ships in 5 minutes.
</p>

<p align="center">
  <a href="#why-surge">Why Surge</a> &bull;
  <a href="#5-minute-setup">5-Minute Setup</a> &bull;
  <a href="#cicd-integration">CI/CD</a> &bull;
  <a href="#how-it-works">How It Works</a> &bull;
  <a href="#features">Features</a> &bull;
  <a href="#integration">Integration</a> &bull;
  <a href="#reference">Reference</a> &bull;
  <a href="#building-from-source">Building</a>
</p>

---

## Why Surge

Your users should always be on the latest version. Chrome, VS Code, and Slack do this transparently &mdash; the app checks for updates, downloads a small patch, and applies it. The user never thinks about it.

Building that yourself means solving a dozen hard problems: hosting an update server, generating delta patches, handling partial downloads, supporting multiple platforms, managing release channels, coordinating deployments across servers, preserving user data across updates, creating installers, setting up shortcuts. Most teams either skip it entirely or ship a half-baked updater that breaks silently.

**Surge gives you Chrome-style automatic updates for any application, on any platform, in about 5 minutes.**

- **No update server to run.** Releases are stored directly in S3, Azure Blob, GCS, GitHub Releases, or a plain directory. You already have one of these.
- **No framework lock-in.** Surge is a native shared library with a stable C ABI. Call it from Rust, C, C++, .NET, Go, Python &mdash; anything that can load a `.so` or `.dll`.
- **Small downloads.** Binary delta patches (bsdiff + zstd) mean users download only what changed between versions. Typically 5-20% of the full package.
- **Release channels.** Ship to `beta` first, then `promote` the exact same build to `stable` when you're confident. No rebuild, no re-upload.
- **User data survives updates.** Mark config files, databases, and user content as persistent assets &mdash; Surge preserves them across every version.
- **Fits your CI pipeline.** `surge pack` and `surge push` are plain CLI commands. Add them to GitHub Actions, GitLab CI, or Jenkins &mdash; works in any matrix build across OS, architecture, and build variants.
- **Cross-platform from day one.** Linux, Windows, and macOS. Native shortcuts (.desktop files, .lnk files, .app bundles), platform-correct install directories, and architecture detection built in.

## 5-Minute Setup

You need two things: somewhere to store your releases and the `surge` CLI.

### 1. Initialize your project

```bash
surge init --wizard
```

The wizard walks you through storage provider, app name, and target platform. Or do it non-interactively:

```bash
surge init \
  --app-id my-app \
  --name "My App" \
  --provider s3 \
  --bucket my-app-releases
```

The result is a `surge.yml` manifest:

```yaml
schema: 1
storage:
  provider: s3
  bucket: my-app-releases
  region: us-east-1
apps:
  - id: my-app
    name: My App
    main: my-app
    target:
      rid: linux-x64
```

Credentials are never stored in the manifest. Surge reads them from environment variables (`AWS_ACCESS_KEY_ID`, `GITHUB_TOKEN`, etc.) or IAM roles.

### 2. Pack a release

Point Surge at your build output:

```bash
surge pack \
  --app-id my-app \
  --rid linux-x64 \
  --version 1.0.0
```

By default, `surge pack` reads artifacts from `.surge/artifacts/<app-id>/<rid>/<version>`, writes packages to
`.surge/packages`, and writes installers to `.surge/installers/<app-id>/<rid>`. Use `--artifacts-dir`/`--output-dir`
to override.

Surge compresses everything into a `tar.zst` package. If a previous version exists in storage, it also generates a
binary delta patch automatically.

### 3. Push to storage

```bash
surge push \
  --app-id my-app \
  --rid linux-x64 \
  --version 1.0.0 \
  --channel stable
```

Done. Your release is live. Clients on the `stable` channel will pick it up on their next update check.

### Optional: install via Tailscale

If your devices are on a tailnet, Surge can pick a matching package for a remote node and transfer it directly:

```bash
surge tailscale install \
  --node my-node \
  --ssh-user operator \
  --channel stable
```

This command:
- probes remote OS/architecture and checks for NVIDIA GPU support,
- resolves the newest matching release on the selected channel,
- downloads it locally and sends it with `tailscale file cp`.

Use `--plan-only` to preview selection without transfer, or `--rid` to force a specific RID. If your tailnet
requires explicit SSH identity, pass `--ssh-user <account>` (or set `--node <account>@<node>` directly).

### 4. Add update checking to your app

**.NET**
```csharp
using var mgr = new SurgeUpdateManager();
await mgr.UpdateToLatestReleaseAsync(
    onUpdatesAvailable: releases =>
        Console.WriteLine($"{releases.Count} update(s), latest: {releases.Latest?.Version}"),
    onAfterApplyUpdate: release =>
        Console.WriteLine($"Updated to {release.Version}")
);
```

**Rust**
```rust
let mut mgr = UpdateManager::new(ctx, "my-app", "1.0.0", "stable", install_dir)?;
if let Some(info) = mgr.check_for_updates().await? {
    mgr.download_and_apply(&info, None::<fn(_)>).await?;
}
```

**C / C++ / anything else**
```c
surge_update_manager* mgr = surge_update_manager_create(ctx, "my-app", "1.0.0", "stable", dir);
surge_releases_info* info = NULL;
if (surge_update_check(mgr, &info) == SURGE_OK)
    surge_update_download_and_apply(mgr, info, progress_cb, NULL);
```

## CI/CD Integration

Surge is built for automated pipelines. The CLI does all the heavy lifting &mdash; your CI just calls `surge pack` and `surge push` after each build. GitHub Actions is the most common setup.

### Single-platform example

```yaml
# .github/workflows/release.yml
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v6
      - run: cargo build --release

      - run: surge pack --version ${{ env.VERSION }}
      - run: surge push --version ${{ env.VERSION }} --channel stable
```

### Multi-platform matrix

Real applications target multiple OS and architecture combinations. Use a matrix strategy to build each variant in parallel, then pack and push each one:

```yaml
jobs:
  build:
    strategy:
      matrix:
        include:
          - os: ubuntu-latest
            rid: linux-x64
          - os: windows-latest
            rid: win-x64
          - os: macos-latest
            rid: osx-arm64
    runs-on: ${{ matrix.os }}
    steps:
      - uses: actions/checkout@v6
      - run: dotnet publish -c Release -r ${{ matrix.rid }}

      - run: surge pack --rid ${{ matrix.rid }} --version ${{ env.VERSION }}
      - run: surge push --rid ${{ matrix.rid }} --version ${{ env.VERSION }} --channel stable
```

Each matrix entry produces its own platform-specific package and delta patch. Clients only download the package matching their OS and architecture.

### Staged rollouts

Combine matrix builds with channel promotion for safe deployments:

```yaml
jobs:
  deploy-beta:
    needs: [build]
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/develop'
    steps:
      - run: surge push --version ${{ env.VERSION }} --channel beta

  promote-stable:
    needs: [build]
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'
    steps:
      - run: surge promote --version ${{ env.VERSION }} --from beta --to stable
```

Push to `develop` ships to beta testers. Merge to `main` promotes the exact same build to stable &mdash; no rebuild, no re-upload, no risk of a different binary reaching production.

### Distributed lock for safe concurrent pushes

When multiple matrix jobs push to the same storage backend, use the distributed lock to prevent race conditions on the release index:

```yaml
    steps:
      - run: surge lock acquire --name "${{ matrix.rid }}-deploy"
      - run: surge push --version ${{ env.VERSION }} --rid ${{ matrix.rid }} --channel stable
      - run: surge lock release --name "${{ matrix.rid }}-deploy"
```

## How It Works

```
  You (developer)                          Your Users
  ──────────────                           ──────────

  cargo build / dotnet publish
         │
         ▼
  surge pack ──► tar.zst full package
                 + bsdiff delta patch
         │
         ▼
  surge push ──► S3 / Azure / GCS / GitHub Releases / filesystem
                        │
                        │  release index (compressed YAML)
                        │  + package files
                        │
                        ▼
                 ┌──────────────┐
                 │ Cloud Storage │
                 └──────┬───────┘
                        │
            ┌───────────┼───────────┐
            ▼           ▼           ▼
         Linux       Windows      macOS
         app           app         app
            │           │           │
            └───────────┴───────────┘
                        │
               check_for_updates()
               download_and_apply()
                        │
                        ▼
                 Update applied.
                 User never noticed.
```

### The update pipeline

When a client calls `download_and_apply`, Surge runs a 6-phase pipeline:

1. **Check** &mdash; validate update info and prepare staging directory
2. **Download** &mdash; fetch delta patch (or full package as fallback) from storage
3. **Verify** &mdash; SHA-256 hash check of every downloaded file
4. **Extract** &mdash; decompress the tar.zst archive
5. **Apply delta** &mdash; apply bsdiff patches if using delta updates
6. **Finalize** &mdash; atomic move into place, clean up staging, preserve persistent assets

Progress callbacks fire at each phase with percentage, bytes transferred, and speed.

## Features

### Release channels

Channels are labels on releases. A single version can be on multiple channels simultaneously.

```bash
# Ship to beta testers first
surge push --version 2.1.0 --channel beta

# A week later, promote the exact same build to stable (no re-upload)
surge promote --version 2.1.0 --from beta --to stable

# Something wrong? Pull it back
surge demote --version 2.1.0 --channel stable
```

Clients specify which channel they follow. Switching channels at runtime is a single API call &mdash; useful for opt-in beta programs.

### Persistent assets

Files and directories that should survive across updates:

```yaml
apps:
  - id: my-app
    persistentAssets:
      - config.json
      - user-data/
      - settings.ini
```

During updates, Surge copies these from the old version directory to the new one before removing the old version.

### Platform-native shortcuts

```yaml
apps:
  - id: my-app
    icon: icon.png
    shortcuts:
      - desktop
      - start_menu
      - startup
```

Surge creates real platform shortcuts:
- **Linux** &mdash; `.desktop` files in `~/.local/share/applications` and `~/.config/autostart` (XDG freedesktop spec)
- **Windows** &mdash; `.lnk` shortcuts on Desktop, Start Menu, and Startup via `WScript.Shell`
- **macOS** &mdash; `.app` bundles with `Info.plist` in `~/Applications`, LaunchAgent for startup

### Process supervisor

The supervisor binary monitors your application, restarts on crash, and coordinates version handoffs:

```bash
surge-supervisor --supervisor-id <uuid> --install-dir /opt/my-app --exe-path /opt/my-app/my-app
```

Or from code:
```csharp
SurgeApp.StartSupervisor();
```

It handles graceful shutdown on SIGTERM/SIGINT (Unix) and Ctrl+C (Windows).

### Lifecycle events

Hook into first-run, post-install, and post-update events:

```csharp
SurgeApp.ProcessEvents(args,
    onFirstRun: v => ShowWelcomeScreen(),
    onInstalled: v => RunMigrations(),
    onUpdated: v => ShowChangelogFor(v));
```

### Installer generation

Surge can produce installer bundles in two modes:

```yaml
target:
  rid: win-x64
  installers:
    - web       # Small bootstrap, downloads app on first run
    - offline   # Self-contained, includes full package
```

### Resource budgets

Throttle resource usage for constrained environments:

```csharp
var budget = new SurgeResourceBudget {
    MaxMemoryBytes = 256 * 1024 * 1024,   // 256 MB
    MaxConcurrentDownloads = 2,
    MaxDownloadSpeedBps = 1_000_000,       // 1 MB/s
    ZstdCompressionLevel = 6               // faster compression
};
```

### Distributed locking

For server-side deployments where multiple CI runners might push releases concurrently, Surge provides a distributed mutex via [snapx.dev](https://snapx.dev):

```bash
surge lock acquire --name "my-app-deploy" --timeout 300
# ... push release ...
surge lock release --name "my-app-deploy"
```

### Backend migration

Move all your releases from one storage provider to another without downtime:

```bash
surge migrate --dest-manifest new-backend.yml
```

## Storage Backends

Use whatever you already have.

| Provider | Config value | Notes |
|---|---|---|
| Amazon S3 | `s3` | Any S3-compatible API (MinIO, Cloudflare R2, DigitalOcean Spaces). Auth via `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` or IAM roles |
| Azure Blob Storage | `azure_blob` | Auth via `AZURE_STORAGE_ACCOUNT_NAME`/`AZURE_STORAGE_ACCOUNT_KEY` |
| Google Cloud Storage | `gcs` | Auth via `GOOGLE_APPLICATION_CREDENTIALS` or application default credentials |
| GitHub Releases | `github_releases` | Free for public repos. `bucket` = `owner/repo`. Auth via `GITHUB_TOKEN` |
| Local filesystem | `filesystem` | For testing or air-gapped environments. `bucket` = root directory path |

## Integration

Surge is a native shared library (`libsurge.so` / `surge.dll` / `libsurge.dylib`) with a C ABI. You don't need Rust in your project.

### .NET

The [`Surge.NET`](dotnet/Surge.NET/) NuGet package provides the full API:

- **netstandard2.0** &mdash; `[DllImport]` for .NET Framework 4.6.1+, .NET Core, Mono, Xamarin
- **net10.0** &mdash; `[LibraryImport]` with full AOT and trimming support
- Zero external dependencies
- `SurgeUpdateManager.UpdateToLatestReleaseAsync()` &mdash; one call that checks, downloads, verifies, extracts, and applies
- Per-phase progress callbacks, cancellation tokens, pre/post-update hooks

### C / C++

Include [`surge_api.h`](include/surge/surge_api.h) and link against the shared library. 31 functions, all following the same pattern: opaque handles, `surge_result` return codes, thread-safe cancellation.

### Rust

Use `surge-core` as a Cargo dependency for direct access to the async API without the FFI overhead.

## Reference

### CLI commands

```
surge init          Create a surge.yml manifest (--wizard for interactive)
surge pack          Build full and delta packages from artifacts
surge push          Upload packages and update the release index
surge list          List releases on a channel
surge promote       Promote a release to another channel
surge demote        Remove a release from a channel
surge migrate       Copy releases between storage backends
surge restore       Restore artifacts from backup
surge lock          Acquire/release distributed locks
surge tailscale     Resolve and transfer packages to a Tailscale node
```

If the manifest has one app, `--app-id` is optional. If the app has one target, `--rid` is optional.

`surge restore` also supports installer-only generation (snapx-style `restore -i`) from existing full packages:

```bash
surge restore -i
```

By default this resolves the latest release for the manifest app/target and default channel, restores missing full
packages from storage into `.surge/packages`, and builds installers using artifacts from
`.surge/artifacts/<app-id>/<rid>/<version>`. The generated installers are written to
`.surge/installers/<app-id>/<rid>`.

Explicit override example:

```bash
surge restore -i \
  --version 1.2.3 \
  --artifacts-dir ./publish \
  --packages-dir .surge/packages
```

### C API function groups

| Group | Functions |
|---|---|
| Lifecycle | `surge_context_create`, `surge_context_destroy`, `surge_context_last_error` |
| Configuration | `surge_config_set_storage`, `surge_config_set_lock_server`, `surge_config_set_resource_budget` |
| Update Manager | `surge_update_manager_create`, `surge_update_manager_destroy`, `surge_update_manager_set_channel`, `surge_update_manager_set_current_version`, `surge_update_check`, `surge_update_download_and_apply` |
| Release Info | `surge_releases_count`, `surge_releases_destroy`, `surge_release_version`, `surge_release_channel`, `surge_release_full_size`, `surge_release_is_genesis` |
| Binary Diff | `surge_bsdiff`, `surge_bspatch`, `surge_bsdiff_free`, `surge_bspatch_free` |
| Pack Builder | `surge_pack_create`, `surge_pack_build`, `surge_pack_push`, `surge_pack_destroy` |
| Distributed Lock | `surge_lock_acquire`, `surge_lock_release` |
| Supervisor | `surge_supervisor_start` |
| Events | `surge_process_events` |
| Cancellation | `surge_cancel` |

### Manifest reference

```yaml
schema: 1
storage:
  provider: s3                    # s3 | azure_blob | gcs | github_releases | filesystem
  bucket: my-bucket               # bucket, container, owner/repo, or directory
  region: us-east-1               # cloud region (or release tag for github_releases)
  endpoint: ""                    # custom endpoint (MinIO, R2, etc.)
  prefix: ""                      # path prefix within bucket

lock:
  url: https://snapx.dev         # distributed lock server (optional)

apps:
  - id: my-app                    # unique identifier
    name: My App                  # display name
    main: my-app                  # main executable (defaults to id)
    installDirectory: my-app      # install dir name (defaults to id)
    icon: icon.png                # application icon
    channels: [stable, beta]      # supported channels
    shortcuts: [desktop, start_menu, startup]
    persistentAssets: [config.json, user-data/]
    installers: [web, offline]
    environment:
      MY_VAR: value
    target:
      rid: linux-x64             # linux-x64, win-x64, win-arm64, osx-x64, osx-arm64
```

Target-level settings override app-level defaults for `icon`, `shortcuts`, `persistentAssets`, `installers`, and `environment`.

### Architecture

```
┌──────────────────────────────────────────────────────────┐
│                     Your Application                     │
│               (.NET / C / C++ / any FFI)                 │
└─────────────────────────┬────────────────────────────────┘
                          │  P/Invoke or C calls
┌─────────────────────────▼────────────────────────────────┐
│                 surge-ffi  (cdylib)                       │
│          31 exported functions · surge_api.h              │
└─────────────────────────┬────────────────────────────────┘
                          │
┌─────────────────────────▼────────────────────────────────┐
│                     surge-core                           │
│  config · crypto · storage · archive · diff · releases   │
│  update · pack · supervisor · platform · download        │
└──────────────────────────────────────────────────────────┘
```

| Crate | Description |
|---|---|
| `surge-core` | Core library &mdash; config, crypto, storage backends, archive (tar+zstd), bsdiff, release index, update manager, pack builder, supervisor, platform detection |
| `surge-ffi` | C API shared library exporting 31 functions through `surge_api.h` |
| `surge-cli` | Command-line tool for packing, pushing, and managing releases |
| `surge-supervisor` | Standalone process supervisor binary |

## Building from Source

```bash
git clone --recurse-submodules https://github.com/fintermobilityas/surge.git
cd surge
```

If you already cloned without `--recurse-submodules`:

```bash
git submodule update --init
```

### Requirements

- **Rust 1.85+** (Edition 2024) &mdash; install via [rustup](https://rustup.rs/)
- **.NET 10 SDK** (optional, for the .NET wrapper and demo app)

### Build and test

```bash
cargo build --release
cargo test
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all
```

```bash
cd dotnet
dotnet build --configuration Release
dotnet test --configuration Release
```

## License

[MIT](LICENSE) &copy; 2026 Finter As

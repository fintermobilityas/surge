<p align="center">
  <img src="assets/logo.svg" alt="Surge" width="128" />
</p>

<h1 align="center">Surge</h1>

<p align="center">
  A fast, cross-platform application update framework built in Rust.
</p>

<p align="center">
  <a href="#features">Features</a> &bull;
  <a href="#architecture">Architecture</a> &bull;
  <a href="#quick-start">Quick Start</a> &bull;
  <a href="#c-api">C API</a> &bull;
  <a href="#net-wrapper">.NET Wrapper</a> &bull;
  <a href="#cli">CLI</a> &bull;
  <a href="#building">Building</a> &bull;
  <a href="#license">License</a>
</p>

---

## Features

- **Direct cloud storage** &mdash; ships updates from S3, Azure Blob, GCS, or a plain filesystem. No package feed server required.
- **Delta updates** &mdash; binary diffs via bsdiff minimize download sizes between versions.
- **Native performance** &mdash; core written in Rust; single shared library (`libsurge.so` / `surge.dll` / `libsurge.dylib`) with a stable C ABI.
- **Cross-platform** &mdash; Linux, Windows, and macOS from one codebase.
- **Process supervisor** &mdash; optional supervisor binary monitors the application and restarts on crash.
- **Distributed locking** &mdash; coordinate deployments across instances with an HTTP lock server.
- **.NET wrapper** &mdash; zero-dependency P/Invoke bindings targeting `netstandard2.0` and `net10.0` (AOT compatible).
- **Resource budgets** &mdash; cap memory, threads, concurrent downloads, bandwidth, and compression level.
- **Cancellation** &mdash; thread-safe cancellation token propagated through every async operation.

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                     Your Application                     │
│               (.NET / C / C++ / any FFI)                 │
└─────────────────────────┬────────────────────────────────┘
                          │  P/Invoke or C calls
┌─────────────────────────▼────────────────────────────────┐
│                 surge-ffi  (cdylib)                       │
│          29 exported functions · surge_api.h              │
└─────────────────────────┬────────────────────────────────┘
                          │
┌─────────────────────────▼────────────────────────────────┐
│                     surge-core                           │
│  config · crypto · storage · archive · diff · releases   │
│  update · pack · supervisor · platform · download        │
└──────────────────────────────────────────────────────────┘
         │              │              │
    ┌────▼───┐    ┌────▼───┐    ┌────▼────┐
    │  S3    │    │ Azure  │    │  GCS /  │
    │        │    │  Blob  │    │  Local  │
    └────────┘    └────────┘    └─────────┘
```

### Crates

| Crate | Description |
|---|---|
| `surge-core` | Core library &mdash; config, crypto, storage backends, archive (tar+zstd), bsdiff, release index, update manager, pack builder, supervisor, platform detection |
| `surge-ffi` | C API shared library exporting 29 functions through `surge_api.h` |
| `surge-cli` | Command-line tool for packing, pushing, and managing releases |
| `surge-supervisor` | Standalone process supervisor binary |

## Quick Start

### Check for updates (C)

```c
#include <surge/surge_api.h>

surge_context* ctx = surge_context_create();
surge_config_set_storage(ctx, SURGE_STORAGE_S3, "my-bucket", "us-east-1",
                         access_key, secret_key, NULL);

surge_update_manager* mgr = surge_update_manager_create(
    ctx, "my-app", "1.0.0", "stable", "/opt/my-app");

surge_releases_info* info = NULL;
surge_result rc = surge_update_check(mgr, &info);

if (rc == SURGE_OK) {
    int count = surge_releases_count(info);
    printf("Found %d update(s), latest: %s\n",
           count, surge_release_version(info, count - 1));

    surge_update_download_and_apply(mgr, info, my_progress_cb, NULL);
    surge_releases_destroy(info);
}

surge_update_manager_destroy(mgr);
surge_context_destroy(ctx);
```

### Check for updates (.NET)

```csharp
using Surge;

var app = SurgeApp.Current;
var mgr = new SurgeUpdateManager(app);

var releases = await mgr.CheckForUpdatesAsync();
if (releases != null)
{
    Console.WriteLine($"Update available: {releases[0].Version}");
    await mgr.DownloadAndApplyAsync(releases, progress =>
        Console.WriteLine($"Phase {progress.Phase}: {progress.TotalPercent}%"));
}
```

### Pack and publish a release (CLI)

```bash
# Build packages (full + delta)
surge pack build --manifest surge.yml \
                 --app-id my-app \
                 --rid linux-x64 \
                 --version 1.2.0 \
                 --artifacts ./publish

# Push to storage
surge pack push --channel stable
```

## C API

The public API is defined in [`include/surge/surge_api.h`](include/surge/surge_api.h). All 29 functions follow a consistent pattern:

- Opaque pointer handles (`surge_context*`, `surge_update_manager*`, etc.)
- Return `surge_result` (`0` = success, negative = error)
- Progress callbacks with user-data pointers
- Thread-safe cancellation via `surge_cancel()`
- Errors retrievable with `surge_context_last_error()`

### Function groups

| Group | Functions |
|---|---|
| Lifecycle | `surge_context_create`, `surge_context_destroy`, `surge_context_last_error` |
| Configuration | `surge_config_set_storage`, `surge_config_set_lock_server`, `surge_config_set_resource_budget` |
| Update Manager | `surge_update_manager_create`, `surge_update_manager_destroy`, `surge_update_check`, `surge_update_download_and_apply` |
| Release Info | `surge_releases_count`, `surge_releases_destroy`, `surge_release_version`, `surge_release_channel`, `surge_release_full_size`, `surge_release_is_genesis` |
| Binary Diff | `surge_bsdiff`, `surge_bspatch`, `surge_bsdiff_free`, `surge_bspatch_free` |
| Pack Builder | `surge_pack_create`, `surge_pack_build`, `surge_pack_push`, `surge_pack_destroy` |
| Distributed Lock | `surge_lock_acquire`, `surge_lock_release` |
| Supervisor | `surge_supervisor_start` |
| Events | `surge_process_events` |
| Cancellation | `surge_cancel` |

## .NET Wrapper

The `Surge.NET` library in [`dotnet/Surge.NET/`](dotnet/Surge.NET/) provides idiomatic C# bindings with zero external dependencies:

- **netstandard2.0** &mdash; `[DllImport]` for broad compatibility
- **net10.0** &mdash; `[LibraryImport]` for AOT / trimming support
- All strings marshalled as UTF-8
- Nullable annotations enabled throughout

## CLI

The `surge` CLI (`crates/surge-cli/`) provides commands for release management:

```
surge pack build    Build full and delta packages from artifacts
surge pack push     Upload packages and update the release index
```

## Building

### Requirements

- **Rust 1.85+** (Edition 2024) &mdash; install via [rustup](https://rustup.rs/)
- **.NET 10 SDK** (optional, for the .NET wrapper and demo app)

### Rust

```bash
cargo build --release          # Build all crates
cargo test                     # Run all tests
cargo clippy --all-targets --all-features -- -D warnings  # Lint
cargo fmt --all                # Format
```

### .NET

```bash
cd dotnet
dotnet build --configuration Release
dotnet test --configuration Release
```

### Verify exported symbols

```bash
cargo build --release -p surge-ffi
nm -D target/release/libsurge.so | grep ' T surge_'   # Linux
```

## Storage Backends

| Provider | Enum Value | Notes |
|---|---|---|
| Amazon S3 | `SURGE_STORAGE_S3` | Supports custom endpoints (MinIO, R2, etc.) |
| Azure Blob Storage | `SURGE_STORAGE_AZURE_BLOB` | Uses account key auth |
| Google Cloud Storage | `SURGE_STORAGE_GCS` | Service account or default credentials |
| Local Filesystem | `SURGE_STORAGE_FILESYSTEM` | Bucket = root directory path |

## License

[MIT](LICENSE) &copy; 2026 fintermobilityas

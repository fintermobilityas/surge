# Surge - Development Instructions

## Project Overview

Surge is a Rust application update framework. It uses direct cloud storage (S3, Azure Blob, GCS, filesystem) instead of NuGet, produces native binaries, and provides a .NET wrapper via P/Invoke.

## Repository Structure

- `include/surge/` - Public C API header (`surge_api.h`)
- `crates/surge-core/` - Core library: config, crypto, storage, archive, diff, releases, update, pack, supervisor, platform
- `crates/surge-ffi/` - C API cdylib (`libsurge.so` / `surge.dll` / `libsurge.dylib`)
- `crates/surge-cli/` - CLI tool (`surge` binary)
- `crates/surge-supervisor/` - Process supervisor binary
- `vendor/bsdiff/` - Vendored bsdiff library (do not modify)
- `dotnet/Surge.NET/` - .NET wrapper (netstandard2.0 + net10.0)
- `dotnet/Surge.NET.Tests/` - .NET xUnit tests
- `demoapp/` - .NET demo application

## Build System

- **Rust**: Cargo workspace with 4 crates, Edition 2024
- **.NET**: Multi-target netstandard2.0 + net10.0

### Building Rust

```bash
cargo build --release
cargo test --release
```

### Building .NET

```bash
cd dotnet && dotnet build --configuration Release
dotnet test --configuration Release
```

## Code Quality

### Warnings

Both Rust and .NET builds treat warnings as errors. **Do not suppress warnings** - fix the underlying issue.

- Rust: clippy `all` + `pedantic` at warn level (see `Cargo.toml` workspace lints)
- .NET: `<TreatWarningsAsErrors>true</TreatWarningsAsErrors>` in `Directory.Build.props`

### rustfmt

All Rust code must conform to `rustfmt.toml`. Run before committing:

```bash
cargo fmt --all
```

### clippy

Static analysis via clippy:

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

## Coding Conventions

### Rust

- Edition 2024 (Rust 1.85+)
- Workspace organized into 4 crates: `surge-core`, `surge-ffi`, `surge-cli`, `surge-supervisor`
- Use `std::path` for all path operations
- Use `tracing` for logging
- Use `thiserror` for error types
- Use `tokio` for async runtime
- Use `serde` for serialization (YAML manifests, JSON APIs)
- Use `reqwest` with `rustls-tls` for HTTP (no OpenSSL dependency)
- Vendored bsdiff compiled via `cc` crate in `build.rs`
- All strings are UTF-8

### C API (`surge_api.h`)

- All functions use `SURGE_API` and `SURGE_CALL` macros
- Return `int32_t` (0 = success, negative = error)
- Opaque pointer handles for all objects
- `#[unsafe(no_mangle)]` + `std::panic::catch_unwind` at every FFI boundary

### .NET

- Zero external dependencies
- `[LibraryImport]` for net10.0+ (AOT compatible), `[DllImport]` for netstandard2.0
- All P/Invoke strings marshalled as UTF-8
- Nullable annotations enabled

## Git Workflow

- All work done on `develop` branch
- PR to `main` for releases
- Commit messages: imperative mood, concise
- CI must pass before merge

## Testing

- Rust tests: `cargo test` (unit tests in each module)
- .NET tests: xUnit in `dotnet/Surge.NET.Tests/`
- All tests must pass locally before pushing

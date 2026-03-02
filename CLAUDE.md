# Surge - Development Instructions

## Project Overview

Surge is a C++20 application update framework that replaces Snapx. It uses direct cloud storage (S3, Azure Blob, GCS, filesystem) instead of NuGet, produces native binaries, and provides a .NET wrapper via P/Invoke.

## Repository Structure

- `include/surge/` - Public C API header (`surge_api.h`)
- `src/` - C++ library source (core, config, crypto, storage, archive, diff, releases, update, pack, lock, supervisor, platform, async, api, cli)
- `vendor/bsdiff/` - Vendored bsdiff library (do not modify)
- `tests/` - GoogleTest test files
- `dotnet/Surge.NET/` - .NET wrapper (netstandard2.0 + net8.0)
- `dotnet/Surge.NET.Tests/` - .NET xUnit tests
- `demoapp/` - .NET demo application

## Build System

- **C++**: CMake 3.21+ with vcpkg for dependency management
- **Standard**: C++20, no extensions
- **.NET**: Multi-target netstandard2.0 + net8.0

### Building C++

```bash
cmake --preset linux-x64-debug
cmake --build --preset linux-x64-debug
ctest --preset linux-x64-debug
```

### Building .NET

```bash
cd dotnet && dotnet build --configuration Release
dotnet test --configuration Release
```

## Code Quality

### Warnings as Errors

Both C++ and .NET builds treat warnings as errors. **Do not suppress warnings** - fix the underlying issue.

- C++: `-Wall -Wextra -Wpedantic -Werror` (GCC/Clang), `/W4 /WX` (MSVC)
- .NET: `<TreatWarningsAsErrors>true</TreatWarningsAsErrors>` in `Directory.Build.props`

### clang-format

All C++ code must conform to `.clang-format`. Run before committing:

```bash
find src include tests -name '*.cpp' -o -name '*.hpp' -o -name '*.h' | xargs clang-format -i
```

### clang-tidy

Static analysis is available via CMake:

```bash
cmake --preset linux-x64-debug -DSURGE_ENABLE_CLANG_TIDY=ON
cmake --build --preset linux-x64-debug
```

## Coding Conventions

### C++

- Use `#pragma once` for include guards
- Namespace: `surge::` with sub-namespaces (`surge::storage`, `surge::archive`, etc.)
- Use `std::filesystem` for all path operations
- Use `spdlog` for logging, `fmt` for formatting
- Use `std::span` for non-owning buffer references
- Use pimpl pattern for public-facing classes to maintain ABI stability
- Headers are the source of truth - implementations must match exactly
- All strings are UTF-8

### C API (`surge_api.h`)

- All functions use `SURGE_API` and `SURGE_CALL` macros
- Return `int32_t` (0 = success, negative = error)
- Opaque pointer handles for all objects
- Catch all C++ exceptions at the C boundary

### .NET

- Zero external dependencies
- `[LibraryImport]` for net8.0+ (AOT compatible), `[DllImport]` for netstandard2.0
- All P/Invoke strings marshalled as UTF-8
- Nullable annotations enabled

## Git Workflow

- All work done on `develop` branch
- PR to `main` for releases
- Commit messages: imperative mood, concise
- CI must pass before merge

## Testing

- C++ tests: GoogleTest in `tests/`
- .NET tests: xUnit in `dotnet/Surge.NET.Tests/`
- All tests must pass locally before pushing

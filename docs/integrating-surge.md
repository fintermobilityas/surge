# Integrating Surge Into An App

This guide is for both humans and agents wiring Surge into an application repo.

The goal is to make integrations repeatable:

- the app repo should have one clear publishing path
- the app repo should have one clear smoke path
- unreleased Surge fixes should have one clear override path

## 1. Pick The Integration Shape First

There are two valid integration shapes. Decide this before editing code.

### Publisher-only integration

Use this when the app only needs Surge to:

- pack installers
- publish releases
- manage channels

The app itself does not link `surge-core` or the FFI at runtime.

Choose this by default if you do not need an in-app update prompt or in-app apply flow.

### Managed runtime integration

Use this when the app also needs Surge at runtime to:

- detect whether the app was installed by Surge
- check for updates in-app
- prompt the user or apply updates

In Rust, this usually means linking `surge-core`. In other stacks, use the C ABI or `.NET` wrapper.

If you choose this path, the app repo must treat Surge as both:

- a publishing dependency
- a runtime dependency

Do not mix these two shapes accidentally.

## 2. What Every App Repo Should Contain

Every app repo that integrates Surge should carry these pieces explicitly.

### Required repo contract

- one committed manifest, usually `.surge/surge.yml`
- one artifact staging script that maps app build output into Surge's expected layout
- one local filesystem smoke wrapper for macOS/Linux/local Windows
- one Azure Windows smoke wrapper if the app ships a Windows installer
- release docs that say exactly which commands are authoritative

The app repo should not rely on contributors remembering raw `surge pack` and `surge push` invocations.

### Wrapper script expectations

The wrapper scripts should encode these decisions so agents do not need to make them:

- default app id and RID selection
- default Surge version/tag
- artifact, package, and installer output directories
- local smoke versions like `0.2.0-smoke.1` and `0.2.0-smoke.2`
- install root for each platform
- success checks after install and after update

## 3. Default Integration Rules

These are the defaults agents should follow unless the app repo says otherwise.

### Use released Surge by default

Use the latest released Surge tag by default.

- for Rust runtime integrations, depend on a released `surge-core` tag
- for publishing/toolchain work in CI, prefer the Surge release bundle or a staged toolchain artifact built from a released tag

Do not point normal app development at an open Surge PR unless you are explicitly validating an unreleased Surge fix.

### Fix Surge upstream first

If the failure is inside Surge itself:

- fix it in the Surge repo first
- merge it
- cut a new prerelease or release
- update the app repo to that released version

Do not carry long-lived app-local patches for Surge behavior that should live upstream.

### Scripts are the source of truth

The app repo should expose copy-paste entrypoints like:

```bash
./scripts/run-surge-filesystem-smoke.sh
./scripts/run-surge-azure-smoke.sh
```

Humans and agents should use those first. Only fall back to raw `surge` commands while debugging the wrappers themselves.

## 4. Runtime Integration Rules

If the app links Surge at runtime, keep the app policy explicit and centralized.

### Update policy

The app should decide in one place:

- which channels are eligible for update UX
- which install providers are eligible for update UX
- whether the app prompts, auto-downloads, or auto-applies

Do not spread that policy across multiple UI components.

### Hosted release URLs

If the app opens a hosted installer URL, derive that URL from install or release metadata.

Do not hardcode a production repo name into runtime update code if the app ever needs staging or smoke validation against another repo.

## 5. Pre-merge Surge Validation

Sometimes the app needs to validate an unreleased Surge fix before that fix is merged and tagged.

That path should be explicit and temporary.

### Preferred override workflow

Use one of these:

- local checkout override for local smoke
- pinned commit override for CI or Azure smoke

After the fix is released, remove the override and go back to the released tag.

### Rust override rule

When a Rust app temporarily overrides `surge-core`, do not patch to the raw crate directory if the crate depends on workspace-inherited settings.

Prefer a local `file://` Git source pinned to the exact Surge commit, for example:

```toml
[patch."https://github.com/fintermobilityas/surge.git"]
surge-core = { git = "file:///abs/path/to/surge", rev = "<sha>", package = "surge-core" }
```

This keeps Cargo workspace semantics intact during validation.

Avoid:

```toml
[patch."https://github.com/fintermobilityas/surge.git"]
surge-core = { path = "../surge/crates/surge-core" }
```

That raw crate-path style can break `workspace = true` dependency resolution in clean or cross-platform smoke runs.

## 6. Windows Smoke Guidance

Windows is usually the strictest smoke path because it surfaces fresh-checkout and installer assumptions early.

If the app ships Windows installers, the app repo should provide a repeatable Azure smoke path.

### Recommended defaults

- keep one warm Azure Windows VM and reuse it during iteration
- install Visual Studio Build Tools once and reuse them
- start the smoke from an interactive scheduled task, not the Startup folder alone
- if the app uses Git LFS assets, run `git lfs pull` on the guest after clone

Do not destroy the VM between every iteration unless the point of the test is first-boot provisioning.

## 7. Minimum Smoke Acceptance Criteria

The app repo should define success in the same way every time.

At minimum, a green smoke should prove:

- the installer is built
- the installer installs successfully
- the installed app writes its Surge runtime metadata
- a newer build on `beta` stays hidden from a `stable` install
- promoting that exact build to `stable` exposes the update
- the update applies successfully
- the app relaunches successfully after update

If the app only implements prompt-first UX instead of `download_and_apply()`, the repo should still have one helper that exercises the lower-level apply path during smoke.

## 8. Common Failure Signatures

These failures are worth documenting because they usually mean the same root cause.

### Missing vendored C files

Symptoms:

- missing `vendor/3rdparty/bzip2/*.c`
- build failures inside `surge-core` on clean Windows builds

Meaning:

- the crate is not self-contained
- published or tagged consumers are missing required vendored sources

Fix:

- commit the exact vendored sources inside the crate
- do not rely on submodules or symlinks at consumer build time

### Missing `workspace.dependencies.*`

Symptoms:

- Cargo fails with an error like `dependency.async-trait was not found in workspace.dependencies`

Meaning:

- the app patched `surge-core` as an isolated crate path instead of preserving the Surge workspace context

Fix:

- use a Git-based override for the exact Surge checkout or commit

## 9. What To Put In AGENTS.md

The app repo `AGENTS.md` should stay short and enforce the defaults.

Recommended rules:

- use released Surge by default
- use app wrapper scripts, not ad hoc `surge` commands
- use overrides only for pre-merge Surge validation
- if overriding `surge-core`, use a local `file://` Git source instead of a raw crate path
- if Surge itself is broken, fix Surge upstream first

This document provides the human-readable context. The app repo `AGENTS.md` should point back to it and restate only the hard rules.

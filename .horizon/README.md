# Cloud worker

The `development` profile uses the pinned Horizon Ubuntu 24.04 worker base,
4 vCPU and 16 GB RAM. It enables Claude, Codex and the desktop for the egui
installer. There are no browser tests or GPU requirements in the selected CI.
The provider is selected in Horizon's New cloud dialog.

## Tool sources

| Tool | Pin | Repository or upstream source |
| --- | --- | --- |
| Rust, Cargo, rustfmt, Clippy | 1.95.0 | `rust-toolchain.toml`; CI `msrv` job |
| rustup bootstrap | 1.29.1 | Official rustup release metadata and SHA256 |
| .NET SDK | 11.0.100-rc.1.26425.128 | Both setup-dotnet steps in `ci.yml`; Microsoft release metadata SHA512 |
| GCC / G++ | 13 | Ubuntu 24.04 native compiler packages |
| Clang for Native AOT | 18 | Ubuntu 24.04 native compiler package |
| localci | 0.1.0 | Latest public release at setup; musl x64 archive checked against SHA256SUMS and a fixed digest |
| Bash, Git, Python, awk, diff, coreutils | Ubuntu 24.04 packages | Hosted-runner tools used by repository scripts; Git and Python also supplied by the base |
| X11, Wayland, EGL, GL, fonts | Ubuntu 24.04 packages | egui features in `Cargo.toml`; native dependency list in `build-linux-arm64-ubuntu18.sh` |

The README's .NET 10 requirement predates the current CI SDK pin and net11.0
project targets. The worker follows CI. .NET dependency versions remain in
`dotnet/Directory.Packages.props`; Rust dependencies remain in `Cargo.lock`.
The .NET build version flows from `Cargo.toml` through the `version-sync` job
output. Native AOT uses the explicit linux-x64/net11.0 CI flags.

Tool paths are applied to SSH sessions by
`/etc/horizon-worker/session-env.d/20-surge.env`. Cargo's registry cache and
NuGet packages use the persistent session home. Toolchain installation runs as
root; rustup's toolchain directory is writable by worker UID 10001 so CI's
component and MSRV installation steps can run without sudo.

The localci launcher uses the base image's Tini child subreaper. Proof containers
override the normal init entrypoint with Bash; this launcher ensures detached
installer processes are reaped when their parent exits, matching a hosted
Linux runner's process lifecycle. The Rust tests remain unchanged and enabled.

`TMPDIR=/workspace/home` keeps temporary test fixtures on the same volume as
the built executables: in-process updater tests hard-link their current binary,
which cannot cross from the workspace volume to the container's `/tmp` layer.
Horizon supplies this directory for sessions; the localci launcher also ensures
it exists. Rust tests run
with `RUST_TEST_THREADS=1` to reduce concurrent process churn around `/proc`
lifecycle probes; every test still runs, while Cargo builds use four jobs.

## Image proof

Horizon runs these `ci.yml` targets for the push event:

```sh
localci run -w ci.yml --event push --skip 'Verify surge-core vendor snapshot' \
  version-sync vendor-sync msrv maintainability \
  clippy clippy-strict clippy-pedantic rust-test#1 rust-artifacts#1 \
  dotnet dotnet-prerelease
```

The same selection is rerun with `--fast`. localci skips `uses:` actions:
Horizon supplies the checkout, the SDK is already installed, local caches replace
cache actions, and artifact upload is omitted. The current proof harness does not
initialize the private `vendor/bsdiff` submodule. Consequently the proof skips
`Verify surge-core vendor snapshot`, which compares that submodule against the
committed publishable snapshot. Rust builds use `crates/surge-core/vendor`
directly, so restore, compilation and tests remain covered. No credentials or
submodule sources are copied into the image. Run the comparison separately in a
checkout with initialized submodules. No selected run step discards package caches.

Not covered: the PR-only rustfmt job on this push event; Windows/macOS legs;
the ARM64 Ubuntu 18.04 artifact build requiring nested Docker; release,
registry publication, signing and trusted-publishing authentication. The
pedantic job retains CI's advisory behavior.

## Desktop preview

After CI leaves the release binaries in `/workspace/checkout/target/release`,
Horizon starts this command from `/workspace/checkout` on its virtual desktop:

```sh
/usr/local/bin/surge-worker-preview
```

It follows the README's manifest and `surge pack` workflow to package a tiny
synthetic offline GUI installer entirely under `/tmp`, then opens the installer.
It uses the CI-built CLI and GUI launcher, performs no compilation or upload,
and requires no credentials. It does not use `--simulator`, which the source
restricts to debug builds. Mesa software rendering supports the CPU desktop.

Review and commit the setup, then return to **Cloud > New cloud** and reload
`.horizon/cloud.yml`. Builds and proof execution belong to Horizon.

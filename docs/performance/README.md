# Performance Notes

This directory is the persistent memory for pack, delta, installer, and update-path benchmarking.

Use these files when:

- changing pack defaults
- changing delta strategy or compression policy
- changing `surge tune pack`
- changing `surge-bench`
- changing `.github/workflows/benchmark.yml`

Files:

- [benchmark-profiles.md](/home/peters/github/surge/docs/performance/benchmark-profiles.md): anonymized profiles, scenarios, and reproducible commands
- [pack-policy.md](/home/peters/github/surge/docs/performance/pack-policy.md): current defaults, why they were chosen, and what tune is allowed to write
- [update-chains.md](/home/peters/github/surge/docs/performance/update-chains.md): what is known about long delta chains and what remains unresolved

Maintenance rules:

- Keep payload descriptions anonymized and generic.
- When pack defaults change, update `pack-policy.md`, `README.md`, and `.github/workflows/benchmark.yml` together.
- When benchmark scenarios or scales change, update `benchmark-profiles.md` and `.github/workflows/benchmark.yml` together.
- When new update-chain measurements materially change conclusions, update `update-chains.md` and the workflow if the tracked CI scenarios should also change.

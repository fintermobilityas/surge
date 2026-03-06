# Update Chains

## What Is Known

The native updater path is now benchmarked end to end through the real `UpdateManager` flow.

That means the benchmark now measures:

- release-index lookup
- delta-chain selection
- artifact download size
- restore-and-apply behavior
- final installed payload verification

## Current Findings

### Localized long chains are acceptable

Large anonymized profile, `sdk_only`, `100` deltas:

- client download stayed around `15.6 MiB`
- client apply time was about `18s`

Meaning:

- repeated localized SDK changes are not the catastrophe case on the client side

### Broad churn still produces bad transfer economics

Large anonymized profile, `full_release`, `10` deltas:

- client download was about `248 MiB`
- apply time remained moderate at about `5.2s`

Meaning:

- the real problem is not always patch application time
- the real problem is often transfer size and publisher/storage cost

### Publisher cost remains important

Localized `100`-delta chain:

- publishing the `101`-release chain took about `337s`

Meaning:

- even when the client path is acceptable, history retention and checkpoint policy still matter

## What Is Not Solved Yet

- retained full checkpoints are not enforced yet
- max chain length is not enforced yet
- broad-churn chains still trend toward large transfers
- file-level or content-addressed deltas are not implemented

## Recommended Direction

Short term:

- keep the current chunked archive-delta path
- keep pack defaults aligned with the measured recommendation
- implement checkpoint retention and chain caps

Long term:

- move to file-level or content-addressed deltas
- stop relying on linear archive-level history for large broad-churn release sets

## When To Rerun

Rerun the long-chain benchmarks when:

- delta strategy changes
- pack defaults change
- restore logic changes
- update planning changes
- retention or chain-cap logic is implemented

# Reservation Runtime Seam Evaluation

## Purpose

This document closes `M11-T05` by evaluating the shared-runtime boundary after the third engine
proof:

- `allocdb-core`
- `quota-core`
- `reservation-core`

The question is no longer whether there is an engine family here. That is now proven strongly
enough. The question is whether the repository has crossed the line from "family of engines" to
"extract a reusable runtime now."

## Decision

Do not extract a broad shared runtime crate yet.

Do prepare the first tiny internal extraction slice now.

The right outcome after the third engine is:

- still no `dsm-runtime` or public database-building library on `main`
- still keep command surfaces, snapshot schemas, recovery entry points, and state-machine logic
  engine-local
- now treat `retire_queue` as an immediate extraction candidate
- keep `wal`, `wal_file`, and `snapshot_file` as the next likely candidates only after the first
  micro-extraction lands cleanly

So the third engine changed the answer from "defer everything" to "defer the big runtime, but start
one tiny internal extraction."

## What The Third Engine Proved

The engine thesis is now materially stronger than it was after `quota-core`.

All three engines now demonstrate the same trusted-core discipline:

- bounded in-memory hot-path structures
- WAL-backed durable ordering
- snapshot plus WAL replay through the live apply path
- logical `request_slot`
- bounded retry and retirement state
- fail-closed recovery on corruption or monotonicity violations

That is enough to say `allocdb` contains a real deterministic-engine family, not just one lease
allocator plus one adjacent experiment.

## What Is Now Mechanically Shared

### `retire_queue`

`retire_queue` is now the strongest extraction candidate.

It is byte-identical across:

- `crates/allocdb-core/src/retire_queue.rs`
- `crates/quota-core/src/retire_queue.rs`
- `crates/reservation-core/src/retire_queue.rs`

This is the first module that is no longer just "similar." It is the same substrate in all three
engines.

### `wal`

`wal.rs` is byte-identical between:

- `crates/quota-core/src/wal.rs`
- `crates/reservation-core/src/wal.rs`

That is a stronger seam than `M10` had, but it is still not universal across all three engines
because `allocdb-core` carries a richer record surface and recovery/reporting contract.

### `wal_file`

`wal_file.rs` is now extremely close between `quota-core` and `reservation-core`.

The remaining delta is small and concrete:

- `reservation-core` refreshes the append handle after truncation
- the test fixtures differ only in engine-local payloads

This is close to extractable, but still wants one more deliberate pass rather than a speculative
generic crate.

### `snapshot_file`

The runtime discipline is effectively the same between `quota-core` and `reservation-core`.

Most visible diffs are test-fixture and domain-shape differences. The remaining constructor/path
surface still differs from `allocdb-core`, so this is a later extraction candidate, not the first
one.

## What Stayed Engine-Specific

The third engine did not make these safer to extract.

### Snapshot schema

Do not extract the snapshot schema layer.

`reservation-core` widened the divergence:

- active-hold rebuild only for `held` records
- deadline-based expiry semantics
- hold/pool state that has nothing to do with quota buckets or lease reservations

The persistence discipline is shared. The schema is not.

### Recovery orchestration

Do not extract the top-level recovery API yet.

The replay skeleton is recognizably similar, but the third engine made the semantic hooks more
obvious, not less:

- overdue-hold expiry on later request slots
- held-only rebuild behavior on restore
- torn-tail proof around expiry boundaries
- engine-specific replay and mutation contracts

There may be helper seams later, but the public recovery entry points are still engine-local.

### State-machine substrate above collections

Do not extract state-machine traits or generic apply plumbing.

The shared truth is still at the discipline level:

- bounded state
- deterministic apply
- logical time
- retry retirement

The actual mutation logic diverged further:

- `allocdb-core` is identity-heavy and fence-heavy
- `quota-core` is arithmetic-heavy and refill-heavy
- `reservation-core` is lifecycle-heavy and expiry-heavy

That divergence is healthy. It means the engines are real.

## Why A Broad Runtime Is Still Premature

A broad extraction would still create cost too early:

- more crate boundaries
- more generic traits and type plumbing
- more internal APIs to stabilize
- more coordination every time an engine evolves

The third engine improved the evidence, but not enough for a full runtime crate:

- one module is now fully mechanical across all three
- two to three more modules are close only within the smaller quota/reservation pair
- snapshot, recovery, config, command, and state-machine layers still diverge in important ways

So the right move is smaller than "extract the runtime."

## Recommended Next Step

Close `M11` after this readout.

Then start one narrowly scoped extraction slice:

1. extract `retire_queue` into one tiny internal shared crate or module
2. prove that no engine behavior changes
3. only after that, reassess `wal`, `wal_file`, and `snapshot_file`

Do not start with:

- snapshot schemas
- recovery entry points
- command codecs
- generic state-machine traits
- a public library story

## Library Thesis Status

The third engine strengthened the thesis, but it did not yet produce a reusable "database-building
library."

The honest status is:

- there is now enough common substrate to justify the first tiny internal extraction
- there is still not enough stable shared shape to claim a general DB-construction framework

That is progress, but it is not the same thing as "the library is ready."

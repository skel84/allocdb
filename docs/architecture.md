# AllocDB Architecture

## Scope

This document defines the execution model, logical-time handling, boundedness rules, and
trusted-core system shape for the approved lease-centric follow-on surface.

The current implementation anchor is still reservation-centric in places. The architecture here
describes the contract the implementation is expected to converge toward as `M9` lands.

## Current Target

The implementation target remains intentionally narrow:

- single process
- single shard
- single writer and executor thread
- deterministic expiration through logged events

## Design Constraints

1. The WAL is the source of truth.
2. Only the executor mutates allocation state.
3. Every state transition must be replayable from persisted input.
4. The state machine must not read wall-clock time, random numbers, or thread interleavings.
5. All hot-path queues, maps, tables, buffers, and retention windows must have explicit bounds.
6. Expected operating failures return deterministic result codes. Assertions are for programmer
   error, data corruption, and broken invariants.
7. Correctness beats reclaim latency. A resource may become reusable late, but never early.
8. The trusted core targets allocation-free steady-state execution after startup.
9. Liveness observation stays outside the trusted core; the core consumes explicit ownership
   transitions such as `revoke` and `reclaim`.

## Core Components

- API ingress
- bounded submission queue
- sequencer and WAL writer
- executor
- expiration scheduler
- snapshot writer

Ingress or networking code may be async if needed. The trusted core boundary is synchronous and
explicit: once a command enters the sequencer and executor path, no async suspension or lock-based
interleaving is allowed in the core state machine.

## Write Path

```text
client
  -> ingress validation
  -> submission queue
  -> sequencer assigns lsn and request_slot
  -> append WAL record
  -> fsync or group commit
  -> executor applies state transition
  -> publish result
```

Rules:

- the executor is single-threaded for one shard
- live execution and replay use the same apply logic
- publish never rewrites the result after the command is applied
- bundle visibility is all-or-nothing at the executor boundary
- holder-authorized mutations validate the current `(lease_id, lease_epoch)` before state changes

Current implementation anchor:

- `allocdb_node::SingleNodeEngine` in `crates/allocdb-node/src/engine.rs`
- `allocdb_node::api` in `crates/allocdb-node/src/api.rs` for the current transport-neutral alpha
  request and response boundary

## Read Path

```text
client
  -> read request
  -> check applied_lsn >= required_lsn
  -> answer from in-memory state or return fence_not_applied
```

For the trusted core, this is enough for strict reads.

The approved public read shapes are:

- `get_resource(resource_id)`
- `get_lease(lease_id)`

If the live engine halts after a WAL-path ambiguity, reads must fail closed until recovery
reconstructs memory from durable state.

## Expiration Path

```text
slot ticker
  -> inspect due reserved leases
  -> enqueue internal expire commands
  -> append to WAL
  -> executor applies expire
```

Rules:

- the scheduler never mutates allocation state directly
- at most `MAX_EXPIRATIONS_PER_TICK` expirations are enqueued
- only `reserved` leases are eligible for expiration
- `active` or `revoking` leases are never freed by timer
- lag must be observable as an explicit metric outside the trusted core

## Revoke And Reclaim Path

```text
external observer
  -> decides holder authority should be withdrawn
  -> submit revoke(lease_id)
  -> executor moves lease to revoking and bumps lease_epoch
  -> later submit reclaim(lease_id) when reuse is safe
  -> executor returns member resources to available
```

Rules:

- external systems observe heartbeats, node state, pod state, or other liveness signals
- the trusted core does not inspect those signals directly
- revoke may happen before reuse is safe
- reclaim is the explicit point where reuse becomes allowed

## Time and TTL Model

The state machine never reads the system clock directly.

Required configuration:

```text
slot_duration_ms                 : u64
max_ttl_slots                    : u64
max_client_retry_window_slots    : u64
lease_history_window_slots       : u64
max_expiration_bucket_len        : u32
max_bundle_size                  : u32
```

Rules:

- external APIs may accept `ttl_ms`, but the WAL and executor operate only on slots
- `max_ttl_slots * slot_duration_ms <= 3_600_000`
- `lease_history_window_slots <= max_ttl_slots`
- TTL applies to `reserved` leases only

Crossing a deadline does not instantly free resources. Resources become reusable only after the
corresponding `expire` or `reclaim` command is committed and applied.

## Retention and Capacity Model

The design uses:

- one fixed-capacity lease table for active and recently terminal leases
- one fixed-capacity lease-member table for bundle membership

Rules:

- active and revoking leases occupy entries until they terminate
- terminal leases keep their entry until `retire_after_slot`
- member records retire with the parent lease
- retirement frees table slots for reuse
- retirement also advances a bounded retired-lookup watermark so later lease lookups stay distinct
  from `not_found` after the full record is dropped

This keeps history bounded and prevents the product-level history policy from silently making the
core unbounded.

## Backpressure and Bounds

At minimum define:

- `MAX_SUBMISSION_QUEUE`
- `MAX_BATCH_SIZE`
- `MAX_COMMAND_BYTES`
- `MAX_RESOURCES`
- `MAX_LEASES`
- `MAX_LEASE_MEMBERS`
- `MAX_BUNDLE_SIZE`
- `MAX_OPERATION_RECORDS`
- `MAX_TTL_SLOTS`
- `LEASE_HISTORY_WINDOW_SLOTS`
- `MAX_EXPIRATION_BUCKET_LEN`
- `MAX_EXPIRATIONS_PER_TICK`

Expected behavior under pressure:

- new writes fail fast with `overloaded` or a more specific capacity error
- reads remain available where possible
- expirations may lag, but lag must be observable
- revoke or reclaim may be delayed externally, but the kernel must not guess

Required operational signals:

- `logical_slot_lag = max(0, current_wall_clock_slot - last_request_slot)`
- expiration backlog, for example the number of due expirations not yet applied
- `operation_table_utilization`
- `lease_table_utilization`
- `lease_member_table_utilization`
- recovery and checkpoint status, including:
  - how the current process started (`fresh_start`, `wal_only`, `snapshot_only`, or
    `snapshot_and_wal`)
  - which snapshot LSN was loaded at startup, if any
  - how many WAL frames were replayed at startup
  - what snapshot LSN is currently the active durable anchor

Delayed expiration and delayed reclaim are acceptable. Premature reuse is not.

## Expiration Index

The expiration index is a fixed-capacity timing wheel keyed by `deadline_slot`.

Rules:

- each slot holds a bounded list of reserved lease references
- the wheel size is derived from `MAX_TTL_SLOTS`
- if a slot bucket reaches `MAX_EXPIRATION_BUCKET_LEN`, new reserve commands fail fast with
  `expiration_index_full`

This is a fundamental design decision, not an open question.

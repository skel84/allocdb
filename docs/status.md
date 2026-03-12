# AllocDB Status

## Current State

- Phase: single-node v1 foundation
- Planning IDs:
  - tasks use `M#-T#`
  - spikes use `M#-S#`
- Current milestone status:
  - `M0` semantics freeze: complete enough for core work
  - `M1` pure state machine: implemented
  - `M1H` constant-time core hardening: complete
  - `M2` durability and recovery: implemented
  - `M3` submission pipeline: implemented
  - `M4` simulation: in progress
  - `M5` single-node alpha surface: in progress
  - `M6` replication design: not started
- Latest completed implementation chunks:
  - `4156a80` `Bootstrap AllocDB core and docs`
  - `f84a641` `Add WAL file and snapshot recovery primitives`
  - `d87c9a7` `Add repo guardrails and status tracking`
  - `79ae34f` `Add snapshot persistence and replay recovery`
  - `1583d67` `Use fixed-capacity maps in allocator core`
  - `3d6ff0f` `Fail closed on WAL corruption`
  - `39f103b` `Defer conditional confirm and add health metrics`
  - `82cb8d8` `Add single-node submission engine crate`
  - current validated chunk: keep retired reservation lookups distinct from `not_found` across
    later writes and snapshot restore using bounded retired-watermark metadata, with core/API
    regression coverage and docs alignment

## What Exists

- Trusted-core crate: `crates/allocdb-core`
- Single-node wrapper crate: `crates/allocdb-node`
- Benchmark harness crate: `crates/allocdb-bench`
- In-memory deterministic allocator:
  - deterministic fixed-capacity open-addressed resource, reservation, and operation tables
  - bounded reservation and operation retirement queues
  - bounded timing-wheel expiration index
  - `create_resource`, `reserve`, `confirm`, `release`, `expire`
  - bounded health snapshot with logical slot lag, expiration backlog, and operation-table
    utilization
- In-process submission engine:
  - typed and encoded request validation before commit
  - bounded submission queue with deterministic overload behavior
  - LSN assignment, WAL append, sync, and live apply
  - pre-sequencing duplicate lookup for applied and already-queued `operation_id`
  - strict-read fence by applied LSN
  - restart path from snapshot plus WAL
  - explicit definite-vs-indefinite submission error categorization
  - explicit restart-and-retry handling for ambiguous WAL failures within the dedupe window
  - node-level metrics for queue pressure, write acceptance, startup recovery status, and active
    snapshot anchor
- Deterministic benchmark harness:
  - CLI entrypoint at `cargo run -p allocdb-bench -- --scenario all`
  - one-resource-many-contenders scenario for hot-spot reserve contention
  - high-retry-pressure scenario for duplicate replay, conflict replay, full dedupe table
    rejection, and post-window recovery
  - scenario reports include elapsed time, throughput, metrics snapshots, and WAL byte counts
- Alpha API surface:
  - transport-neutral request and response types in `crates/allocdb-node::api`
  - binary request and response codec with fixed-width little-endian encoding
  - explicit wire-level mapping for definite vs indefinite submission failures
  - strict-read fence responses plus halt-safe read rejection for resource and reservation queries
  - retired reservation lookups remain distinct from `not_found` across later writes and snapshot
    restore through bounded retired-watermark metadata
  - bounded `tick_expirations` maintenance request for live TTL enforcement
  - metrics exposure through the same API boundary
- Durability primitives:
  - WAL frame codec and recovery scan
  - file-backed WAL append, sync, recovery, and torn-tail truncation
  - fail-closed recovery on middle-of-log corruption
  - snapshot encode, decode, capture, restore
  - file-backed snapshot write and load
  - explicit WAL command payload encoding and live-path replay recovery
  - checkpoint path that writes the new snapshot first, then rewrites retained WAL history
  - one-checkpoint WAL overlap and `snapshot_marker` retention for safe checkpoint replacement
- Deterministic simulation support:
  - reusable simulation harness in `crates/allocdb-node/src/simulation.rs`
  - explicit simulated slot advancement under test control, with no wall-clock reads in the
    exercised engine path
  - seeded same-slot ready-set scheduling with reproducible transcripts
  - checkpoint, restart, and injected persist-failure helpers over the real `SingleNodeEngine`
- Validation:
  - `cargo test -p allocdb-core retired_reservation_lookup_survives_unrelated_later_write`
  - `cargo test -p allocdb-core snapshot_restores_retired_lookup_watermark`
  - `cargo test -p allocdb-node api_reservation_reports_retired_history`
  - `cargo run -p allocdb-bench -- --scenario all`
  - `scripts/preflight.sh`

## Current Focus

- `M4-T02`: inject reproducible crash points around WAL, apply, snapshot, and recovery boundaries
  on top of the promoted simulation harness
- follow with `M4-T03` storage-fault coverage using the same deterministic driver
- resume `M5-T04` operator guidance after the simulation evidence set is broader

## How To Check Progress

- implementation status: [work-breakdown.md](./work-breakdown.md)
- milestone sequencing: [roadmap.md](./roadmap.md)
- current snapshot: this file
- reviewable history: `git log --oneline`

## Update Rule

Update this file whenever a task or milestone materially changes:

- milestone completion state
- implementation coverage
- recommended next step
- required validation commands

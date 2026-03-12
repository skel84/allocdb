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
  - `M4` simulation: not started
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
  - current validated chunk: transport-neutral alpha API surface with submit, strict/halt-safe
    reads, bounded expiration ticks, recovery startup reporting, and metrics request/response
    handling

## What Exists

- Trusted-core crate: `crates/allocdb-core`
- Single-node wrapper crate: `crates/allocdb-node`
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
- Alpha API surface:
  - transport-neutral request and response types in `crates/allocdb-node::api`
  - binary request and response codec with fixed-width little-endian encoding
  - explicit wire-level mapping for definite vs indefinite submission failures
  - strict-read fence responses plus halt-safe read rejection for resource and reservation queries
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
- Validation:
  - `scripts/preflight.sh`

## Current Focus

- `M5-T03`: add the benchmark harness for contention and boundedness behavior
- open the next tracked issue for `M5-T03`
- follow with `M5-T04` operator runbook once benchmark and fault-run evidence is available

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

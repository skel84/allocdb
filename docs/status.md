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
  - `M2` durability primitives: partially implemented
  - `M3` submission pipeline: in progress
  - `M4+` simulation, alpha hardening, replication design: not started
- Latest completed implementation chunks:
  - `4156a80` `Bootstrap AllocDB core and docs`
  - `f84a641` `Add WAL file and snapshot recovery primitives`
  - `d87c9a7` `Add repo guardrails and status tracking`
  - `79ae34f` `Add snapshot persistence and replay recovery`
  - `1583d67` `Use fixed-capacity maps in allocator core`
  - `3d6ff0f` `Fail closed on WAL corruption`
  - `39f103b` `Defer conditional confirm and add health metrics`
  - `82cb8d8` `Add single-node submission engine crate`
  - current validated chunk: explicit submission error categories and dedupe-window utilization
    signal

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
- Durability primitives:
  - WAL frame codec and recovery scan
  - file-backed WAL append, sync, recovery, and torn-tail truncation
  - fail-closed recovery on middle-of-log corruption
  - snapshot encode, decode, capture, restore
  - file-backed snapshot write and load
  - explicit WAL command payload encoding and live-path replay recovery
- Validation:
  - `cargo fmt --all`
  - `cargo clippy --all-targets --all-features -- -D warnings`
  - `cargo test`
  - `scripts/check_repo.sh`

## Current Focus

- `M2-T08`: tighten WAL/snapshot checkpoint coordination on top of the current recovery path
- `M2-T08`: implement a safe truncation rule that preserves overlap through the previous
  checkpoint anchor
- `M3-T06`: finish the remaining submission semantics around indefinite outcomes after write/sync
  failure
- `M5-T02`: expose recovery status alongside the current queue-pressure and core-health signals

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

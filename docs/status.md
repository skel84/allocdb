# AllocDB Status

## Current State

- Phase: single-node v1 foundation
- Current milestone status:
  - `M0` semantics freeze: complete enough for core work
  - `M1` pure state machine: implemented
  - `M1H` constant-time core hardening: planned, not started
  - `M2` durability primitives: partially implemented
  - `M3+` submission pipeline, simulation, alpha hardening, replication design: not started
- Latest completed implementation chunks:
  - `4156a80` `Bootstrap AllocDB core and docs`
  - `f84a641` `Add WAL file and snapshot recovery primitives`
  - `d87c9a7` `Add repo guardrails and status tracking`
  - current chunk: snapshot file IO, explicit WAL payload encoding, and replay recovery

## What Exists

- Trusted-core crate: `crates/allocdb-core`
- In-memory deterministic allocator:
  - fixed-capacity resource, reservation, and operation stores
  - bounded timing-wheel expiration index
  - `create_resource`, `reserve`, `confirm`, `release`, `expire`
- Durability primitives:
  - WAL frame codec and recovery scan
  - file-backed WAL append, sync, recovery, truncate-to-valid-prefix
  - snapshot encode, decode, capture, restore
  - file-backed snapshot write and load
  - explicit WAL command payload encoding and live-path replay recovery
- Validation:
  - `cargo fmt --all`
  - `cargo clippy --all-targets --all-features -- -D warnings`
  - `cargo test`
  - `scripts/check_repo.sh`

## Current Focus

- replace sorted `Vec` lookups with deterministic fixed-capacity open-addressed tables
- separate lookup tables from retirement order so retirement work is proportional to expired items
- harden WAL recovery to distinguish EOF torn tails from middle-of-log corruption
- decide whether version-guarded `conditional_confirm` belongs in v1 or is deferred
- define `logical_slot_lag` and expiration backlog as first-class operational signals

## How To Check Progress

- implementation status: [work-breakdown.md](./work-breakdown.md)
- milestone sequencing: [roadmap.md](./roadmap.md)
- current snapshot: this file
- reviewable history: `git log --oneline`

## Update Rule

Update this file whenever a unit of work materially changes:

- milestone completion state
- implementation coverage
- recommended next step
- required validation commands

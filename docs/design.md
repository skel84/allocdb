# AllocDB Engineering Design v0

## Status

Draft. This file is the top-level engineering overview for the v1 design.

For the detailed design docs, read:

- [Semantics and API](./semantics.md)
- [Architecture](./architecture.md)
- [Fault Model](./fault-model.md)
- [Replication Notes](./replication.md)
- [Roadmap](./roadmap.md)
- [Work Breakdown](./work-breakdown.md)
- [Spikes](./spikes.md)
- [Storage and Recovery](./storage.md)
- [Implementation Rules](./implementation.md)
- [Testing Strategy](./testing.md)
- [AllocDB Principles](./principles.md)

## v1 Target

The first implementation target is intentionally narrow:

- single process
- single shard
- single writer and executor thread
- WAL plus snapshot recovery
- deterministic TTL expiration through logged events

## Design Rules

1. The WAL is the source of truth.
2. Only the executor mutates allocation state.
3. Every state transition must be replayable from persisted input.
4. The state machine must not read wall-clock time, randomness, or thread interleavings.
5. Every hot-path structure has an explicit bound.
6. The trusted core targets allocation-free steady-state execution after startup.

Current implementation anchor:

- `crates/allocdb-core` contains the trusted-core allocator and durability logic
- `crates/allocdb-node` contains the first in-process single-node submission wrapper

## Document Map

- [semantics.md](./semantics.md): domain model, identifiers, invariants, commands, retention
- [architecture.md](./architecture.md): single-node pipeline, logical time, bounds, scheduler
- [fault-model.md](./fault-model.md): crash, clock, storage, and bounded-overload assumptions
- [replication.md](./replication.md): deferred distributed design areas and boundaries
- [roadmap.md](./roadmap.md): milestone plan and exit criteria
- [work-breakdown.md](./work-breakdown.md): concrete units of work for the first implementation
- [spikes.md](./spikes.md): bounded throwaway experiments for implementation uncertainty
- [storage.md](./storage.md): WAL, snapshots, recovery
- [implementation.md](./implementation.md): Rust memory policy, dependency policy, assertions
- [testing.md](./testing.md): simulation, replay, property tests, Jepsen gate for replication

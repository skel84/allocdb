# Product Requirements Document: AllocDB

## Status

Draft. This file is the top-level product overview for AllocDB.

For the detailed product docs, read:

- [Product Scope](./product.md)
- [Semantics and API](./semantics.md)
- [AllocDB Principles](./principles.md)

## Purpose

AllocDB is a deterministic database for allocating scarce resources under contention.

It exists to make "one resource, one winner" a first-class primitive for tickets, seats,
inventory, rooms, and compute slots.

## Product Constraints

These are product requirements, not implementation preferences:

1. Determinism first.
2. Boundedness first.
3. Assertions for programmer errors, result codes for operating conditions.
4. Allocation-free steady-state execution is the target for the Rust trusted core.
5. Single-node correctness comes before replication.

## v1 Shape

v1 is intentionally narrow:

- single-node, single-shard execution
- logical-slot TTLs
- bounded idempotency window
- bounded reservation-history lookup
- no trusted-core metadata
- `holder_id` required for `confirm` and `release`

## Document Map

- [product.md](./product.md): problem statement, goals, non-goals, milestones, success criteria
- [semantics.md](./semantics.md): resource model, reservation lifecycle, API contract, consistency
- [principles.md](./principles.md): TigerStyle adapted to AllocDB and Rust

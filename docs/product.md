# AllocDB Product Scope

## Purpose

AllocDB is a deterministic database for allocating scarce resources under contention.

It exists to make "one resource, one winner" a first-class primitive for workloads such as:

- ticketing
- seats
- inventory
- rooms and tables
- GPUs and other compute slots

The system is inspired by TigerBeetle's architectural discipline, but specialized for
reservations rather than money movement.

## Product Discipline

AllocDB is defined by hard constraints as well as features:

1. Determinism first. The same snapshot and WAL must always produce the same state.
2. Boundedness first. Every queue, table, batch, payload, and retention window must have an
   explicit limit.
3. Assertions for programmer mistakes, result codes for operating conditions.
4. Rust trusted-core code should target allocation-free steady-state execution after startup.
5. A rock-solid single-node trusted core is the foundation for replication.

## Problem Statement

Modern systems repeatedly rebuild reservation logic on top of generic databases, caches, queues,
and cleanup workers.

Typical stacks rely on:

- PostgreSQL row locking
- Redis TTL keys
- ad hoc queues
- background cleanup workers

These systems frequently fail by:

- double allocation
- ghost reservations
- race conditions
- retry storms
- inconsistent failover behavior
- fragile crash recovery

AllocDB exists to remove those failure modes from application code.

## Goals

### Primary Goals

1. Never allocate one resource to two owners at the same logical point in time.
2. Behave deterministically under replay and later under replication.
3. Keep every hot-path operation strictly bounded.
4. Deliver predictable latency for small state transitions under contention.
5. Preserve correctness during crash recovery.
6. Keep the trusted core operationally and conceptually small.

### Secondary Goals

- high throughput for small commands
- low operational overhead
- simple, auditable failure behavior

### Core Engineering Target

AllocDB proves correctness and boundedness on its trusted core before scaling out through replication.

## Non-Goals

AllocDB is not intended to be:

- a general SQL database
- a document store
- a queue system
- an analytics engine
- a streaming platform
- a workflow engine

It is specialized infrastructure for resource allocation.

## Failure Model

The first version must tolerate:

- process crash
- power loss
- torn WAL tail
- restart from snapshot plus WAL replay

Network partitions, failover, and replication lag matter later, but they are not allowed to
distort the trusted core design.

## Success Criteria

AllocDB succeeds when the following are true:

- no double allocation is ever observed
- resources may become reusable late, but never early
- replay after crash yields the same state and command outcomes
- all hot-path limits are explicit and testable
- command outcomes are deterministic under contention
- replication provides high availability without rewriting the trusted core allocator semantics

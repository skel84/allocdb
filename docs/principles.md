# AllocDB Principles

## Status

Draft. This document adapts TigerStyle to AllocDB, Rust, and a deterministic resource-allocation
database.

These rules apply most strongly to the trusted core:

- WAL and snapshots
- deterministic executor
- reservation state machine
- recovery logic

## 1. Priorities

AllocDB inherits TigerBeetle's ordering of values:

1. safety
2. performance
3. developer convenience

For this project, "safety" means:

- no double allocation
- no premature resource reuse
- deterministic replay
- bounded failure behavior

## 2. Determinism

The core rule is simple:

```text
same snapshot + same WAL == same state + same results
```

That has concrete consequences:

- the state machine does not read wall-clock time
- the state machine does not use randomness
- the state machine does not depend on thread scheduling
- IDs derived inside the system come from log order, not random generators
- iteration order used for snapshots, replay, or externally visible behavior must be explicit
- live execution and replay share the same apply logic

Logical time is represented with slots, not timestamps, in the trusted core.

## 3. Boundedness

Every hot-path structure must have an explicit limit.

This includes:

- queues
- batches
- WAL frame size
- payload size
- TTL range
- dedupe retention
- reservation-history retention
- work done per scheduler tick

If a loop is intended to run forever, the design must still bound the amount of work done in each
iteration and make overload visible.

AllocDB does not accept "infinite history" or "unbounded retries" as hidden assumptions in the
core.

## 4. Assertions and Errors

Assertions are for broken code, broken assumptions, and broken persisted data.

Expected operating conditions are not assertion failures. They are deterministic results such as:

- `resource_busy`
- `resource_not_found`
- `ttl_out_of_range`
- `overloaded`

The assertion rules are:

- assert preconditions and postconditions inside important state transitions
- assert persisted-data invariants when encoding, decoding, snapshotting, and replaying
- pair assertions across boundaries where possible
- assert both the positive space and the negative space when that boundary is where bugs hide
- prefer compile-time assertions for layout and constant relationships

Crash on violated internal invariants. Do not try to continue after corruption.

## 5. Rust Memory and Allocation Policy

TigerStyle says "no dynamic allocation after initialization." In Rust, AllocDB should aim for the
same operational outcome in the trusted core:

- allocate capacity at startup from explicit configuration
- avoid heap growth in the steady-state executor path
- reuse buffers instead of allocating scratch memory per command
- make capacity failures explicit and testable

Practical Rust guidance:

- use explicit capacities with `Vec::with_capacity()` or equivalent startup allocation
- if a container may reallocate, treat that as a design problem unless the growth is intentionally
  bounded and documented
- avoid pointer-rich ownership graphs and hidden allocator traffic in the core
- prefer index-based storage, fixed-capacity pools, and deterministic tables
- avoid randomized hash tables in the trusted core

For early prototypes, a temporary exception is acceptable only if all three are true:

1. the memory growth is still bounded by configuration
2. the exception is called out in the design
3. it is not on the intended production hot path

## 6. Dependency Policy

The trusted core should have as few dependencies as possible.

Default rule:

- prefer `std`
- admit a tiny crate only when it is small, audited, deterministic, and materially simpler than a
  local implementation

Not acceptable in the core by default:

- async runtimes
- networking stacks
- generic serialization frameworks
- macro-heavy abstraction crates
- libraries with hidden allocation or hidden concurrency

Control plane, networking, TLS, CLI, and observability are separate concerns and may use more
libraries outside the trusted core boundary.

## 7. Naming and API Semantics

Names must match the domain model precisely.

Rules:

- separate `resource` state from `reservation` state
- use nouns and verbs consistently: `reserve`, `confirm`, `release`, `expire`
- put units in names: `ttl_slots`, `slot_duration_ms`, `latency_ms_p99`
- avoid overloaded words such as `state` when a more specific name is possible
- avoid semantic shortcuts that hide authority or retention rules

API semantics must also be explicit:

- exactly-once means "within a configured dedupe window"
- TTL means logical-slot expiration, not wall-clock magic inside the state machine
- strict reads mean "up to a specified applied LSN"

## 8. Single-Node First

Replication is important, but it is not the first design center.

AllocDB must first prove:

- deterministic single-node execution
- crash recovery correctness
- bounded queues and memory
- invariant-heavy tests under contention

Only after those semantics are fixed should the project specify:

- replication
- failover
- sharding

Future distributed features are not allowed to complicate or weaken the single-node core before it
is correct.

## 9. Simplicity

The design should stay narrow until there is a demonstrated need to widen it.

That means:

- do not add speculative states only because they might be useful later
- do not admit metadata to the trusted core without a correctness reason
- do not hide policy decisions inside helper abstractions
- do not choose a flexible abstraction where a concrete bounded state machine is clearer

AllocDB is foundational infrastructure. Small, explicit, and replayable is better than clever.

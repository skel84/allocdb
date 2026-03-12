# AllocDB Implementation Rules

## Scope

This document defines the Rust trusted-core implementation discipline for v1.

## Memory Policy

The trusted core should target allocation-free steady-state execution after startup.

Current trusted-core boundary:

- `crates/allocdb-core`

Everything else should be treated as outside the trusted core unless explicitly promoted into it.

Current non-core crates:

- `crates/allocdb-node`
- `crates/allocdb-bench`

Design target:

- fixed-capacity resource storage
- fixed-capacity reservation storage
- fixed-capacity operation dedupe storage
- fixed-capacity expiration index
- reusable IO buffers

Practical Rust rules:

- allocate capacity at startup from configuration
- if `Vec` is used, reserve once and assert that capacity never grows afterward
- prefer indexes and handles over pointer-rich object graphs
- avoid `HashMap` with randomized seeds in the trusted core
- if hashing is needed, use a fixed-seed deterministic table with an explicit probe bound
- avoid per-command heap allocation in the steady-state hot path

Current implementation:

- deterministic fixed-capacity open-addressed tables for resources, reservations, and operations
- preallocated timing-wheel buckets with explicit per-bucket capacity
- bounded retirement queues so reservation and operation retirement drains expired fronts instead
  of scanning whole tables

Current observability surface:

- keep `confirm` keyed by `reservation_id`; defer any version-guarded confirm API until the
  product needs read-version preconditions beyond reservation identity
- classify torn EOF tails separately from durable-log corruption in every recovery path
- expose logical slot lag and expiration backlog through a bounded health snapshot
- expose operation-table utilization so dedupe-window pressure is visible before hard rejection
- expose queue depth, write-acceptance state, startup recovery status, and the active snapshot
  anchor through the single-node engine wrapper
- expose a transport-neutral alpha API with explicit submission failure categories, strict-read
  fence responses, and binary request/response codecs outside the trusted core
- provide one deterministic benchmark harness for hot-spot contention and retry-window pressure
  outside the trusted core

Current durability shape before alpha:

- the WAL is one append-only file on the live path
- checkpoints rewrite retained WAL history through a temp-file and rename path
- retained WAL keeps one-checkpoint overlap and appends a `snapshot_marker` at the active snapshot
  anchor

Next hardening steps before alpha are:

- write the operator runbook for startup, recovery, overload, and corruption handling

## Dependency Policy

The trusted core should stay close to `std`.

Allowed by default in the core:

- `std`
- small checksum or hash crates such as `crc32c` or `blake3`, if pinned and justified

Not allowed in the core by default:

- async runtimes
- networking stacks
- macro-heavy frameworks
- generic serialization frameworks
- hidden-allocation helper crates
- ORM-style abstractions

Rust-specific guidance:

- the executor path should avoid `Arc<Mutex<_>>`, `Rc<RefCell<_>>`, and trait-object-heavy
  indirection
- use explicit-width integer newtypes for important IDs if it improves clarity
- separate the trusted core crate from ingress, CLI, observability, and networking code

## Assertion Policy

The implementation is not credible without invariant-heavy assertions.

Rules:

- use `assert!` for impossible states, corrupted persisted data, broken invariants, and violated
  internal contracts
- use `debug_assert!` only for checks that are too expensive for production but still valuable
- return deterministic result codes for expected operating conditions such as `resource_busy`
- pair assertions across boundaries when possible, for example before WAL write and after WAL read
- prefer compile-time assertions for type sizes, field offsets, and format constraints

## Required Testing

- state-machine unit tests for every transition
- WAL replay equivalence tests
- crash recovery tests with torn WAL tails
- idempotency tests with duplicate `operation_id`
- contention tests where many clients race on one resource
- TTL tests where confirm, release, and expire interleave in different orders
- property tests asserting "at most one active owner per resource"
- capacity tests proving failure behavior at every configured bound

## Repository Guardrails

The repository should enforce basic layout discipline automatically.

Required guardrails:

- `scripts/check_repo.sh` must pass before a chunk is considered complete
- trusted-core Rust source files stay small enough to review in one pass
- the core crate dependency set stays explicitly allow-listed
- the core must not grow async runtimes, generic serializers, randomizing maps, or shared mutable
  ownership primitives without a documented design change

Current guardrail targets:

- `crates/allocdb-core/src/**/*.rs`
- [`docs/status.md`](./status.md) as the single-file progress snapshot

## Follow-Up Docs

The next useful detailed docs are:

1. `replication.md` only after the single-node semantics are fixed
2. `roadmap.md` and `work-breakdown.md` for execution planning
3. operator-facing runbook details once the alpha surface is complete

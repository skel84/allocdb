# Quota Engine Plan

## Purpose

This document turns the `quotadbfork.txt` note into a structured engineering plan.

The canonical v1 semantic boundary lives in [`quota-semantics.md`](./quota-semantics.md).

The goal is not to rebrand AllocDB into a framework immediately. The goal is to test the engine
thesis directly:

- can the current durability, replay, dedupe, boundedness, and logical-time model support a second
  deterministic database engine
- can that second engine live in the same workspace without weakening AllocDB's current
  correctness discipline
- can two real engines reveal which seams are truly reusable and which are still domain-specific

This is a research and architecture plan, not a commitment to productize a general framework yet.

## Decision

The quota experiment should start inside the existing `allocdb` workspace.

The initial shape is:

- keep the repository identity as `allocdb`
- add a sibling crate such as `crates/quota-core`
- do not create a new repo
- do not extract a shared runtime crate first
- do not introduce a plugin or framework surface first

The sequence is:

1. fork semantics now
2. prove a second engine
3. extract shared runtime pieces later, only if the duplication stabilizes

## Why The Experiment Stays In-Repo

The in-repo experiment is the most direct test of the thesis.

Benefits:

- it keeps runtime and semantic comparison cheap
- it avoids premature framework packaging and naming work
- it keeps the quota engine under the same implementation constraints as AllocDB
- it makes later extraction easier because the copied code and the original remain easy to diff

Risks:

- the workspace becomes broader
- correctness work now exists in two domains
- it becomes easier to widen the repo without proving the need first

Mitigations:

- keep the new work in a sibling crate, not inside `allocdb-core`
- keep the current docs explicit that AllocDB remains the primary engine and the quota work is an
  engine experiment
- keep all domain semantics separate
- do not generalize public docs or public identity yet

## Success Criteria

The experiment is successful if all of the following become true:

- `quota-core` has one deterministic command surface with replay-equivalent results
- the quota engine uses the same core execution discipline:
  - WAL as durable source of truth
  - replay through the same apply path
  - logical `request_slot`
  - bounded queues and bounded dedupe state
  - fail-closed recovery
- at least one copied runtime slice becomes obviously reusable across both engines
- the shared-vs-domain boundary becomes clearer after the second engine, not blurrier

The experiment is unsuccessful if any of the following happen:

- quota semantics need a materially different runtime contract
- most copied runtime pieces diverge quickly and stay divergent
- the experiment forces premature generic abstractions into `allocdb-core`
- the second engine cannot preserve the same recovery, dedupe, and replay discipline without
  awkward special cases

## Non-Goals

The first quota experiment should not try to do any of the following:

- publish a generic framework
- split the repo into multiple public identities
- build a generic plugin system
- extract a shared `dsm-runtime` crate before the second engine proves the seams
- add async runtimes, ORM-style abstractions, generic serializers, or unbounded containers
- solve tenant policy, queueing, UI, or billing concerns

## Initial Repository Shape

The expected first layout is:

- `crates/allocdb-core`
- `crates/allocdb-node`
- `crates/quota-core`

Possible later layout, only after validation:

- `crates/dsm-runtime`
- `crates/allocdb-core`
- `crates/quota-core`
- maybe later `crates/dsm-node`

## Phase 0: Freeze The Experiment Boundary

Before code, freeze the experiment boundary in docs.

Deliverables:

- one design note for quota semantics and non-goals
- one explicit statement that the experiment stays in the current workspace
- one statement that runtime extraction is deferred until after two engines exist

Exit criteria:

- the quota experiment has a narrow v1 scope
- the repo does not promise a generic framework yet

## Phase A: Add `quota-core` As A Sibling Engine

Start with the smallest viable deterministic quota model.

### First domain model

- `BucketId`
- `BucketRecord`
- `OperationRecord`

### First commands

- `CreateBucket`
- `Debit`
- optional admin-only `Credit` for tests or repair

### First result codes

- `Ok`
- `AlreadyExists`
- `BucketTableFull`
- `BucketNotFound`
- `InsufficientFunds`
- `OperationConflict`
- `OperationTableFull`
- `SlotOverflow`

### Rules

- do not start with refill
- do not start with hierarchy, tenancy, or policy
- do not start with rich read APIs

Exit criteria:

- one bucket can be created
- one debit can succeed or fail deterministically
- duplicate retry returns the cached outcome
- conflicting `operation_id` reuse returns conflict

## Phase B: Copy Runtime Pieces Locally, Without Extraction

Copy the runtime and persistence pieces needed by quota.

Candidate copied pieces:

- WAL frame codec and scan discipline
- snapshot write/load discipline
- recovery monotonicity checks
- bounded queue helpers
- retire queue helpers
- command context carrying `lsn` and `request_slot`
- operation dedupe storage and retry retirement mechanics

Rules:

- copy first, do not extract first
- keep naming close enough that later diffing is easy
- allow the copies to diverge while quota semantics settle

Exit criteria:

- `quota-core` can run its own live apply and replay loop with copied runtime pieces
- no forced abstraction is introduced back into `allocdb-core`

## Phase C: Prove Debit, Dedupe, And Replay Before Refill

This phase is the real engine test.

Required invariants:

- `0 <= balance <= limit`
- a successful `Debit(op, amount)` reduces balance exactly once
- same `operation_id` plus same payload returns the stored outcome
- same `operation_id` plus different payload returns `OperationConflict`
- replay from snapshot plus WAL produces the same balance and outcomes as the live path

Required tests:

- duplicate retry after success
- duplicate retry after failure
- conflicting retry with the same `operation_id`
- full operation-table deterministic rejection
- replay equivalence across snapshot and WAL restore
- crash/restart around debit boundaries

Exit criteria:

- debit semantics are boring and deterministic
- replay uses the same apply logic as the live path

## Phase D: Add Deterministic Refill With Logical Time Only

Only after debit semantics are stable should refill be added.

### Refill rules

Refill must be a pure function of:

- persisted bucket state
- refill policy
- `request_slot`

Refill must not read:

- wall-clock time
- system time
- external timers

### Bucket expansion

The bucket record likely gains:

- `limit`
- `balance`
- `last_refill_slot`
- `refill_rate_per_slot` or an equivalent checked representation

### Required tests

- large slot gaps
- slot arithmetic near overflow
- cap-at-limit behavior
- replay equivalence across refill boundaries
- retry behavior near a refill boundary

Exit criteria:

- refill stays deterministic under replay
- no wall-clock dependency enters the state machine

## Phase E: Durability And Recovery Proof

Quota must inherit the same recovery rigor as AllocDB.

Required properties:

- reject non-monotonic LSN
- reject rewound `request_slot`
- replay through the same live apply logic
- truncate only torn EOF tails
- fail closed on mid-log corruption

Business claim to prove:

- crash recovery does not invent or lose debits

Exit criteria:

- quota recovery semantics are as strict as AllocDB's current recovery semantics

## Phase F: Batch And Group Commit Only After Core Correctness

Batching is later work.

Required properties:

- ordered batch apply is equivalent to serial apply for the same ordered input
- per-command outcomes remain individually visible
- overload bounds remain explicit
- retry-hit behavior remains bounded and reviewable

Exit criteria:

- batch correctness is proven before batch performance is marketed

## Phase G: Compare Engines And Decide On Extraction

Only after both engines stabilize should extraction be considered.

### Extraction rule

Extract shared runtime code only when all of the following are true:

1. both engines use the seam unchanged
2. the duplication is causing real maintenance pain
3. the abstraction needs no `if allocdb` / `if quota` branching
4. replay, dedupe, and failure semantics are truly the same across both

### Likely extraction order

#### 1. Low-risk runtime primitives

- `CommandContext`
- bounded queues
- retire queues
- fixed-capacity storage helpers

#### 2. Durability substrate

- WAL codec and scanner
- WAL file handling
- snapshot write/load discipline
- recovery driver
- monotonicity checks
- torn-tail vs corruption classification

#### 3. Dedupe and retry substrate

- operation fingerprinting
- same-op cached outcome
- conflicting-op rejection
- retire-after-slot mechanics
- bounded operation-table pressure behavior

#### 4. Outer engine shell, if the shape is still shared

- submission queue
- sequencer
- WAL writer
- apply loop
- publish/read fence behavior
- fail-closed ambiguous write handling

### What must not be extracted early

- bucket state
- lease state
- refill logic
- expiration logic
- domain command enums
- domain result codes
- public API semantics

Those are product semantics, not runtime substrate.

## Stop Conditions

Stop the experiment and reassess if:

- runtime copies diverge immediately for quota-specific reasons
- the second engine requires weaker guarantees than AllocDB
- extraction pressure appears before quota semantics are stable
- the experiment starts forcing generic naming into unrelated AllocDB docs or APIs

## Recommended First Execution Prompt

The first implementation prompt should be:

> Add `quota-core` as a sibling crate. Copy the persistence and runtime pieces locally. Implement
> `CreateBucket` and `Debit` with request-slot-based determinism, operation-id dedupe, replay
> equivalence, and bounded failure modes. Do not extract shared runtime code yet.

That is the narrowest move that still tests the engine thesis honestly.

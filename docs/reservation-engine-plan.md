# Reservation Engine Plan

## Purpose

This document defines the next engine experiment after `quota-core`.

The goal is not to chase the biggest market first. The goal is to apply stronger pressure to the
 engine thesis:

- can the current bounded, deterministic, replay-safe runtime discipline support a third engine
  with a different lifecycle shape
- can that third engine expose cleaner shared-runtime seams than `quota-core` did
- if the experiment succeeds, does building quota/credits on top of a later extracted runtime look
  materially easier

The canonical v1 semantic boundary lives in
[`reservation-semantics.md`](./reservation-semantics.md).

## Decision

The next engine experiment should be `reservation-core` in the existing workspace.

The initial shape is:

- keep the repository identity as `allocdb`
- add a sibling crate such as `crates/reservation-core`
- do not extract a shared runtime crate first
- do not rebrand the repo as a framework yet
- use this engine to test extraction pressure more aggressively than `quota-core` did

## Why Reservation-Core Is The Right Next Experiment

`quota-core` proved that a second deterministic engine can live in-repo. It did not prove that the
 shared runtime is ready to extract.

`reservation-core` is the better next pressure test because it changes the semantic shape while
 still reusing the same core discipline:

- active objects with terminal retirement
- idempotent retry
- deterministic expiry
- snapshot plus WAL recovery
- replay through the same apply path
- bounded live state

Compared with quota semantics, reservation semantics should stress:

- expiry queues more directly
- state transitions across multiple terminal outcomes
- hold lifecycle invariants instead of balance arithmetic
- recovery around confirm/release/expire transitions

If the engine thesis is real, this experiment should make the shared-vs-domain boundary clearer.

## Hypothesis

If `reservation-core` lands cleanly, then a later extracted runtime should make quota/credits
 materially easier to build on top of it.

What "easier" means here:

- bounded storage, WAL, snapshot, replay, retirement, and recovery should already exist as shared
  substrate
- quota should mainly reintroduce domain semantics such as balances, refill, and quota-specific
  result codes
- the remaining hard work should be semantic, not runtime duplication

If that does not happen, the library thesis is weaker than it currently appears.

## Success Criteria

The experiment is successful if all of the following become true:

- `reservation-core` has one deterministic command surface with replay-equivalent results
- the engine uses the same runtime discipline as the existing engines:
  - WAL as durable source of truth
  - snapshot plus WAL recovery
  - logical `request_slot`
  - bounded maps, queues, and operation dedupe state
  - fail-closed recovery
- at least two runtime slices become obviously reusable across all three engines
- the shared-vs-domain boundary becomes narrower and more defensible after the third engine

The experiment is unsuccessful if any of the following happen:

- reservation semantics require materially different durability or recovery contracts
- copied runtime pieces diverge again and stay divergent
- extraction pressure still points at only one tiny helper rather than a coherent substrate
- the engine forces awkward generic abstractions into domain logic

## Non-Goals

The first reservation experiment should not try to do any of the following:

- build a booking or commerce product
- add payment, carts, pricing, or customer workflow logic
- add background daemons or wall-clock timers
- build cross-pool or multi-object transactions
- extract a framework before the third-engine readout
- promise a public database-building library yet

## Initial Repository Shape

The expected next layout is:

- `crates/allocdb-core`
- `crates/quota-core`
- `crates/reservation-core`
- `crates/allocdb-node`

Possible later layout, only after the third-engine readout:

- `crates/dsm-runtime`
- `crates/allocdb-core`
- `crates/quota-core`
- `crates/reservation-core`

## Phase 0: Freeze The Reservation Boundary

Before code, freeze the experiment boundary in docs.

Deliverables:

- one design note for reservation semantics and non-goals
- one explicit statement that the experiment stays in the current workspace
- one explicit extraction hypothesis tied to a later quota/credits build

Exit criteria:

- the reservation experiment has a narrow v1 scope
- the repo still does not promise a generic framework yet

## Phase A: Add Reservation-Core As A Sibling Engine

Start with the smallest viable scarce-capacity hold model.

### First domain model

- `PoolId`
- `PoolRecord`
- `HoldId`
- `HoldRecord`
- `OperationRecord`

### First commands

- `CreatePool`
- `PlaceHold`
- `ConfirmHold`
- `ReleaseHold`
- `ExpireHold`

### First result codes

- `Ok`
- `AlreadyExists`
- `PoolTableFull`
- `PoolNotFound`
- `HoldTableFull`
- `HoldNotFound`
- `InsufficientCapacity`
- `HoldExpired`
- `InvalidState`
- `OperationConflict`
- `OperationTableFull`
- `SlotOverflow`

### Rules

- no wall-clock time reads
- no background sweeper inside v1
- no multi-pool reservations
- no partial confirms
- no cross-command convenience APIs before the lifecycle is proven

Exit criteria:

- one pool can be created
- one hold can be placed, confirmed, released, or expired deterministically
- duplicate retry returns the cached outcome
- conflicting `operation_id` reuse returns conflict

## Phase B: Copy Runtime Pieces Locally, Without Extraction

Copy the minimum runtime and persistence pieces needed by `reservation-core`.

Candidate copied pieces:

- WAL frame codec and scan discipline
- snapshot write/load discipline
- recovery monotonicity checks
- bounded queue helpers
- retire queue helpers
- command context carrying `lsn` and `request_slot`
- operation dedupe storage and retirement mechanics

Rules:

- copy first, do not extract first
- keep naming close enough for later diffing
- allow the copies to diverge while reservation semantics settle

Exit criteria:

- `reservation-core` can run its own live apply and replay loop with copied runtime pieces
- no shared crate is introduced yet

## Phase C: Prove Hold Lifecycle, Dedupe, And Replay

This phase is the real third-engine test.

Required invariants:

- `0 <= held + consumed <= total_capacity`
- a successful `PlaceHold(op, qty)` reserves capacity exactly once
- `ConfirmHold` moves held capacity to consumed exactly once
- `ReleaseHold` returns held capacity exactly once
- same `operation_id` plus same payload returns the stored outcome
- same `operation_id` plus different payload returns `OperationConflict`
- replay from snapshot plus WAL produces the same pool and hold state as the live path

Required tests:

- duplicate retry after successful hold placement
- duplicate retry after failed hold placement
- duplicate confirm and duplicate release
- conflicting retry with the same `operation_id`
- deterministic invalid-state rejection
- replay equivalence across snapshot and WAL restore
- crash/restart around hold lifecycle boundaries

Exit criteria:

- lifecycle semantics are boring and deterministic
- replay uses the same apply logic as the live path

## Phase D: Add Deterministic Expiry With Logical Time Only

Only after lifecycle semantics are stable should expiry be added.

### Expiry rules

Expiry must be a pure function of:

- persisted hold state
- persisted deadline slot
- `request_slot`

Expiry must not read:

- wall-clock time
- process time
- external timers

### Required tests

- due-at-boundary expiry
- large slot gaps
- retry behavior near expiry boundaries
- replay equivalence across expiry boundaries
- crash/restart with overdue holds still pending expiry application

Exit criteria:

- expiry stays deterministic under replay
- no wall-clock dependency enters the state machine

## Phase E: Durability And Recovery Proof

Reservation-core must inherit the same recovery rigor as the existing engines.

Required proof points:

- snapshot plus WAL recovery preserves hold states exactly
- torn-tail WAL truncation never fabricates or drops confirmed state silently
- rewound `request_slot` progress is rejected fail-closed
- dedupe cache outcomes survive checkpoint and replay
- expiry decisions after recovery match the live path

Exit criteria:

- recovery correctness is demonstrated, not inferred
- there is no alternate apply path for restore

## Phase F: Third-Engine Seam Evaluation

After `reservation-core` is stable, perform a new seam readout.

Questions to answer:

- which modules are now copied nearly unchanged across all three engines
- which modules still only look similar but differ in important ways
- is there a small shared runtime worth extracting now
- would building quota/credits on top of that shared runtime be materially simpler

Required output:

- one short seam-evaluation document
- one clear decision: extract now, defer again, or abandon the library thesis

## Recommended Issue Shape

- `M11`: third-engine proof with `reservation-core`
- `M11-T01`: freeze reservation boundary and v1 scope
- `M11-T02`: scaffold `reservation-core` sibling crate with copied substrate
- `M11-T03`: implement `CreatePool`, `PlaceHold`, `ConfirmHold`, and `ReleaseHold`
- `M11-T04`: add logical-slot expiry and durability/recovery proof
- `M11-T05`: evaluate shared runtime seams after the third engine

## Stop Conditions

Stop and reassess if any of the following happen:

- `reservation-core` needs cross-object transactions to feel coherent
- expiry cannot stay deterministic without introducing wall-clock coupling
- the copied runtime diverges in ways that make future extraction less plausible
- the experiment starts pulling the repo toward product workflow instead of engine truth

## Current Recommendation

Do this next if the goal is architecture truth and library extraction pressure.

Do not do it next if the goal is immediate market pull or fastest path to a product wedge.

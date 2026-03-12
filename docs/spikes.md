# AllocDB Spikes

## Status

Draft. This document defines when throwaway experiments are appropriate and which spikes are
currently justified.

## Principle

Spikes are for implementation uncertainty, not semantic uncertainty.

They exist to answer questions like:

- which fixed-capacity table shape is simplest and safest in Rust?
- what timing-wheel structure is easiest to keep bounded and readable?
- what WAL framing shape makes torn-tail recovery simplest?

They do not exist to answer questions like:

- what `reserve` means
- whether `holder_id` is required
- whether retention is bounded
- whether indefinite outcomes exist

Those are design decisions already captured elsewhere.

## Spike Rules

Every spike must be:

- time-boxed
- narrowly scoped
- disposable by default
- documented with the decision it informs

Every spike must end in one of three outcomes:

1. choose an implementation direction
2. reject an implementation direction
3. identify a genuine design gap that must be resolved in docs before coding continues

If a spike starts changing semantics, stop and move the issue back into the design docs.

## Code Handling

Spike code should:

- live in an obviously non-production location such as `scratch/` or `experiments/`
- be deleted once the decision is made, unless a piece is directly promoted into production code
- never quietly become trusted-core code without review and tests

## Approved Spike Areas

### M1-S01: Fixed-Capacity Tables

Question:

- what table shape best supports bounded resource, reservation, and operation storage in Rust?

Why a spike is justified:

- this is an implementation-shape question with strong effects on safety, clarity, and allocation

Current chosen direction:

- the first implementation slice used sorted fixed-capacity `Vec` stores to keep the prototype
  small and deterministic
- the production direction is now deterministic fixed-capacity open-addressed tables in the
  trusted core

### M1-S02: Timing Wheel

Question:

- what timing-wheel bucket layout makes expiration, overflow, and retirement simplest to reason
  about?

Why a spike is justified:

- the design decision is fixed, but the implementation shape is still uncertain

Current chosen direction:

- preallocated timing-wheel buckets
- explicit `MAX_EXPIRATION_BUCKET_LEN` per slot
- deterministic sorted bucket contents

### M2-S01: WAL Framing

Question:

- what binary frame layout makes corruption detection and torn-tail recovery simplest?

Why a spike is justified:

- a short experiment can eliminate format complexity before the real codec is written

Current chosen direction:

- explicit little-endian binary frame layout
- per-frame CRC32C checksum
- recovery scan stops at the last valid frame boundary

### M4-S01: Simulation Harness

Question:

- what simulator shape can drive the real trusted core with seeded slot advancement and crash
  injection?

Why a spike is justified:

- this is a harness-design question and should be proven early

Current chosen direction:

- a scripted single-node harness around the real `SingleNodeEngine`
- simulated slot lives in the harness and advances only when the test driver says so
- seeded choice is used only to order ready ingress at one logical slot; state-machine and
  recovery semantics remain the production implementations
- crash, restart, checkpoint, and persist-failure events stay explicit driver actions rather than
  hidden behind fake clock or storage traits

Evidence gathered:

- `crates/allocdb-node/src/simulation.rs` and
  `crates/allocdb-node/src/simulation_tests.rs` now carry the promoted harness shape selected by
  the spike: the real engine, seeded same-slot ordering, and explicit slot advancement
- the spike proves one restart path with checkpoint, logged expiration, injected WAL ambiguity, and
  recovery from snapshot plus WAL

Reuse for `M4-T01` through `M4-T04`:

- the external-driver shape
- explicit simulated-slot state owned by the harness
- seeded ready-set ordering for same-slot ingress
- temp WAL/snapshot lifecycle and restart helpers

Discard after the spike:

- the ad hoc test-only API names and one-off helper layout
- the exact PRNG choice used only to prove reproducibility
- any expectation that all future scenarios fit one linear script helper without refinement

Next step:

- add crash-point and storage-fault coverage on top of the promoted simulation support during
  `M4-T02` and `M4-T03`

## Non-Approved Spike Areas

Do not spike:

- command semantics
- result-code meanings
- retention rules
- fault-model rules
- whether replication changes single-node guarantees

Those issues belong in the docs and review process, not in throwaway code.

## Related Docs

- [roadmap.md](./roadmap.md)
- [work-breakdown.md](./work-breakdown.md)
- [semantics.md](./semantics.md)
- [implementation.md](./implementation.md)

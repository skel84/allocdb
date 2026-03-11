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

### SPIKE-101: Fixed-Capacity Tables

Question:

- what table shape best supports bounded resource, reservation, and operation storage in Rust?

Why a spike is justified:

- this is an implementation-shape question with strong effects on safety, clarity, and allocation

Current chosen direction:

- sorted fixed-capacity `Vec` stores with deterministic binary search and insertion
- no hash tables in the trusted core for the first implementation slice

### SPIKE-102: Timing Wheel

Question:

- what timing-wheel bucket layout makes expiration, overflow, and retirement simplest to reason
  about?

Why a spike is justified:

- the design decision is fixed, but the implementation shape is still uncertain

Current chosen direction:

- preallocated timing-wheel buckets
- explicit `MAX_EXPIRATION_BUCKET_LEN` per slot
- deterministic sorted bucket contents

### SPIKE-201: WAL Framing

Question:

- what binary frame layout makes corruption detection and torn-tail recovery simplest?

Why a spike is justified:

- a short experiment can eliminate format complexity before the real codec is written

Current chosen direction:

- explicit little-endian binary frame layout
- per-frame CRC32C checksum
- recovery scan stops at the last valid frame boundary

### SPIKE-401: Simulation Harness

Question:

- what simulator shape can drive the real trusted core with seeded slot advancement and crash
  injection?

Why a spike is justified:

- this is a harness-design question and should be proven early

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

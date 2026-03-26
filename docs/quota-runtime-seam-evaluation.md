# Quota Runtime Seam Evaluation

## Purpose

This document closes `M10-T05` by evaluating whether `allocdb-core` and `quota-core` now justify a
shared runtime extraction.

The answer is based on the code as merged in `allocdb#107`, not on a framework-first plan.

## Decision

Do not extract a shared runtime crate yet.

The second-engine proof succeeded, but the overlap is not yet stable enough to justify a
`dsm-runtime` or similar crate on `main`.

The correct outcome at this point is:

- keep `allocdb-core` and `quota-core` as sibling engines in the same repository
- keep copied runtime pieces local to each engine for now
- record which seams look real
- defer extraction until repeated maintenance pressure proves it is worth the churn

## What The Second Engine Proved

The engine thesis is now materially stronger than it was before `quota-core` existed.

Both engines now demonstrate the same execution discipline:

- bounded in-memory hot-path structures
- WAL-backed durable ordering
- snapshot plus WAL replay through the live apply path
- logical `request_slot`
- operation dedupe with bounded retirement
- fail-closed recovery on corruption or monotonicity violations

That is enough to say there is a real engine family here, not just one special-case lease kernel.

## What Is Actually Shared

### Clearly shared discipline

The following ideas are genuinely common across both engines:

- ordered frame append and replay
- bounded probe-table storage
- bounded retirement queues
- snapshot plus WAL recovery orchestration
- monotonic LSN and request-slot enforcement
- deterministic retry semantics

### Closest to mechanical extraction

These modules are the closest to being extractable later with low semantic risk:

- `retire_queue`
- parts of `fixed_map`
- parts of `wal`
- parts of `wal_file`

`retire_queue` is the strongest example. It is effectively the same data structure in both
engines, differing only in the surrounding key types and local tests.

`fixed_map`, `wal`, and `wal_file` do the same job in both engines, but they already diverge in
details that matter:

- `fixed_map` in `allocdb-core` carries richer trace logging and more key types
- `wal` and `wal_file` are similar in shape, but their error surfaces and tests already differ
- the extraction point would need to preserve boundedness and fail-closed behavior without
  introducing generic abstraction noise

## What Is Not Shared Enough

The following should remain engine-local.

### Command and result surfaces

Do not extract:

- command enums
- command codecs
- result codes and domain outcomes
- config types

These are runtime-adjacent, but they are still domain contracts, not generic substrate.

### Snapshot schema

Do not extract snapshot encoding logic yet.

The persistence discipline is shared, but the actual on-disk schema is not:

- `allocdb-core` carries a richer allocator-specific snapshot layout
- `quota-core` carries a much smaller bucket/operation layout
- forcing a generic snapshot schema would either add indirection or erase useful domain structure

The most that could be extracted later is helper machinery, not the schema itself.

### Recovery API surface

Do not extract recovery orchestration yet.

The top-level recovery flow is recognizably similar, but the differences are already meaningful:

- `allocdb-core` has richer replay error variants and slot-overflow reporting
- `allocdb-core` has more operational logging
- the restore path and replay details are still closely tied to engine-specific command decoding and
  state-machine APIs

There may be a later helper seam here, but not a good generic crate boundary today.

### State machine logic

Do not extract any state-machine layer.

The commonality is only at the discipline level:

- deterministic apply
- bounded state
- retry cache
- logical time

The actual state transitions, invariants, and read models are completely different and should stay
separate.

## Why Extraction Is Premature Now

Extraction would create cost immediately:

- more crate boundaries
- more generic traits and type plumbing
- more public internal APIs to stabilize
- more coordination every time one engine evolves

But the benefit is still limited:

- only one module is basically mechanical today
- most other overlap is still “same shape, different details”
- there is not yet repeated maintenance pain from fixing the same bug in both engines over time

So the code is similar enough to reveal seams, but not similar enough to deserve a shared runtime
crate yet.

## Extraction Triggers Later

Revisit extraction only when one of these becomes true:

- the same runtime bug or improvement lands independently in both engines more than once
- `fixed_map`, `wal`, or `wal_file` stay structurally stable across several follow-on slices
- a third engine appears and wants the same substrate
- the repo starts paying obvious maintenance cost for duplicated runtime fixes

Until then, duplication is cheaper than premature abstraction.

## Recommended Next Step

Treat `M10` as complete.

The next step is not more framework work. The next step is either:

- stop here and keep both engines local while they stabilize, or
- start a new domain/engine experiment only if there is a strong reason to test a third point in
  the design space

If extraction is revisited later, start with the smallest possible mechanical move:

1. `retire_queue`
2. selected `fixed_map` helpers
3. selected `wal` / `wal_file` helpers

Do not start with snapshot schemas, command codecs, or state-machine traits.

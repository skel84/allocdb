# Fourth-Engine Proof Reassessment

## Purpose

This document closes `M13-T02` by answering a narrower question than the roadmap originally posed:

- does `M14` still matter?
- if it does, does it still need a full fourth-engine proof?

The answer is based on the repository state after:

- three real engines on `main`
- the first shared runtime extractions
- the engine authoring boundary
- the focused runtime-vs-engine contract

## Decision

`M14` is still required.

A full fourth engine is no longer required by default.

The preferred proof shape is now:

- one substantial new engine slice built against the extracted substrate, with clearly reduced
  copy-paste and no correctness regression

A fourth engine remains acceptable, but it is no longer the default bar for the next step.

## Why `M14` Still Matters

The repository now has:

- multiple real engines
- multiple real runtime extractions
- an explicit shared-vs-local authoring boundary

That is enough to justify the internal library thesis as plausible and increasingly concrete.

It is not yet enough to prove the extracted substrate actually lowers authoring cost in live
implementation work.

The missing evidence is still practical:

- can an author add real new behavior with less copy-paste than before
- does the shared runtime help without pulling semantics upward into the wrong layer

That is what `M14` still needs to prove.

## Why A Full Fourth Engine Is No Longer Required

Earlier in the roadmap, a fourth engine was the cleanest imaginable proof because the shared
runtime boundary did not yet exist.

That changed during `M12` and `M13`:

- `retire_queue`, `wal`, and `wal_file` are now actually shared
- `snapshot_file` has already been evaluated and explicitly deferred
- the semantic line is now documented instead of implicit
- future extraction rules are constrained by the contract rather than intuition

Because that groundwork is done, the next proof does not need to pay the cost of inventing an
entire fourth engine just to answer whether reduced-copy authoring is real.

A smaller proof can now answer the real question more directly.

## Preferred Next Proof

The next proof should be:

- one substantial new engine slice against the extracted substrate

That slice should be large enough to make the outcome obvious:

- it should consume the shared runtime directly
- it should avoid re-copying runtime substrate already extracted
- it should keep domain semantics local
- it should make the reduction in duplicated runtime code easy to inspect

Good examples:

- a meaningful new runtime-adjacent engine slice inside an existing sibling engine
- a new engine kernel whose first slice already relies on the extracted substrate instead of copied
  queue/frame/file code

Bad examples:

- trivial docs-only proof
- tiny cosmetic edits that do not exercise the shared runtime boundary
- a fourth engine that mostly re-copies domain-local semantics and still does not show reduced-copy
  authoring clearly

## Resulting Plan Change

`M14` remains necessary, but its preferred proof shape narrows:

1. first try a substantial reduced-copy engine or engine-slice proof
2. only reach for a full fourth engine if that smaller proof fails to answer the question cleanly

That is a better use of effort now that the repository already has both:

- extracted substrate
- explicit authoring rules

## Recommended Next Step

Treat this reassessment as complete.

The next implementation step is:

1. start `M14-T01`
2. pick one engine or engine slice that can consume the extracted substrate directly
3. use that result to judge whether the internal DB-building library claim is becoming real or is
   still premature

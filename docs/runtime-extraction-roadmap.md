# Runtime Extraction Roadmap

## Purpose

This document defines the path from the current engine family to something that can honestly be
called a general internal DB-building library.

The current state is:

- `allocdb-core`, `quota-core`, and `reservation-core` all exist on `main`
- the engine thesis is proven strongly enough
- a broad shared runtime is still premature
- `retire_queue` is the first justified micro-extraction candidate

The goal is not to market a framework early. The goal is to extract only the runtime substrate that
has actually stabilized under multiple engines.

## End State

We should only call this a general internal DB-building library when all of the following are true:

- more than one runtime module is shared cleanly across engines
- the shared-vs-domain boundary is explicit and stable
- a new engine or engine slice can be built with materially less copy-paste
- extraction reduces maintenance cost more than it adds abstraction cost

Until then, the honest description remains:

- multiple deterministic engines
- emerging shared runtime

## Milestone Shape

### M12: First Internal Runtime Extractions

Goal:

- extract the smallest runtime pieces that are already mechanically shared

Scope:

- `retire_queue`
- `wal`
- `wal_file`
- evaluate `snapshot_file`, but extract it only if the file-level discipline stays separable from
  snapshot schemas across the full engine family

Non-goals:

- no public framework story
- no snapshot schema extraction
- no recovery API extraction
- no state-machine trait layer

Exit criteria:

- extracted modules are used by all applicable engines
- behavior is unchanged
- tests stay green without new abstraction leaks

### M13: Internal Engine Authoring Contract

Goal:

- define the stable boundary between shared runtime and engine-local semantics

Scope:

- one internal runtime contract note
- explicit ownership of:
  - bounded collections
  - durable frame/file helpers
  - snapshot-file discipline
  - recovery helper seams, if any
- explicit non-ownership of:
  - command schemas
  - result surfaces
  - snapshot schemas
  - state-machine semantics

Exit criteria:

- the contract is clear enough that another engine authoring pass is constrained by it

### M14: Fourth-Engine Or Reduced-Copy Proof

Goal:

- prove that the extracted substrate lowers authoring cost rather than only moving code around

Acceptable proof shapes:

- preferred: retrofit one substantial new engine slice against the extracted substrate with clearly
  reduced copy-paste and no correctness regression, or
- build a fourth engine against the extracted substrate if the smaller proof cannot answer the
  question cleanly

Exit criteria:

- one new engine or engine slice uses the extracted substrate directly
- the reduction in duplicated runtime code is obvious
- the authoring contract survives contact with real implementation work

## Recommended Issue Shape

### M12

- `M12`: Extract the first internal shared runtime substrate from the three-engine family
- `M12-T01`: Extract shared `retire_queue`
- `M12-T02`: Extract shared `wal`
- `M12-T03`: Extract shared `wal_file`
- `M12-T04`: Evaluate and, if still clean, extract shared `snapshot_file`

### M13

- `M13`: Define the internal engine authoring boundary after the first extractions
- `M13-T01`: Write the internal runtime-vs-engine contract
- `M13-T02`: Reassess whether a fourth-engine proof is still required or whether the extracted
  substrate already lowered authoring cost enough

### M14

- `M14`: Prove the extracted substrate lowers engine-authoring cost
- `M14-T01`: Build one new engine or engine slice against the extracted substrate
- `M14-T02`: Re-evaluate whether the repository can now honestly claim an internal DB-building
  library

## Execution Rules

- extract smallest-first
- after each micro-extraction, stop and verify before continuing
- if one extraction introduces awkward generic plumbing, stop and reassess rather than force the
  sequence
- keep domain logic local even if runtime discipline is shared

## Current Recommendation

Do this next:

1. `M12-T01` shared `retire_queue`
2. `M12-T02` shared `wal`
3. `M12-T03` shared `wal_file`
4. only then decide whether `snapshot_file` is still clean enough to extract

Result:

- `retire_queue`, `wal`, and `wal_file` were extracted successfully
- `snapshot_file` was evaluated and deferred because the seam is still only clean inside the
  `quota-core` / `reservation-core` pair, not across all three engines
- `M13` is the next correct move, followed by a reassessment of whether `M14` still needs a full
  fourth-engine proof or can prefer a smaller reduced-copy slice

Do not do this next:

- public framework branding
- generic state-machine APIs
- generic snapshot schemas
- extracting recovery entry points before the lower layers stabilize

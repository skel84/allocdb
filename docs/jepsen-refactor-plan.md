# Jepsen Refactor Plan

Issue: [#70](https://github.com/skel84/allocdb/issues/70)

## Goal

Reduce the size and maintenance cost of [`allocdb-jepsen.rs`](../crates/allocdb-node/src/bin/allocdb-jepsen.rs)
without changing validated behavior, command surfaces, or release-gate semantics.

## Constraints

- Keep the current `allocdb-jepsen` binary behavior stable.
- Do not introduce a new crate in this task.
- Preserve the already-proven KubeVirt/QEMU validation path.
- Keep the repo buildable after each refactor slice.
- Revalidate with both local test coverage and representative live KubeVirt runs.

## Non-Goals

- New Jepsen workloads or backend features are out of scope.
- Hetzner backend work is deferred for this task.
- Analyzer/history model redesign in [`jepsen.rs`](../crates/allocdb-node/src/jepsen.rs)
  unless a minimal extraction requires a purely mechanical move.
- A dedicated `allocdb-validation` crate is still deferred.

## Target Shape

The long-term binary layout should become:

- [`allocdb-jepsen.rs`](../crates/allocdb-node/src/bin/allocdb-jepsen.rs): `main()`, high-level
  dispatch, and minimal glue only
- `allocdb-jepsen/args.rs`: subcommand parsing and usage text
- `allocdb-jepsen/watch.rs`: run status loading, fleet watching, and terminal rendering
- `allocdb-jepsen/artifacts.rs`: run tracker, status/event persistence, archive/bundle helpers
- `allocdb-jepsen/runtime.rs`: external backend execution, remote helpers, and scenario runners
- `allocdb-jepsen/tests.rs`: binary-specific regression tests

This keeps the binary implementation in one place under `src/bin/` while establishing seams that
can later support a dedicated validation crate if it still makes sense.

## Execution Plan

### Slice 1: Thin CLI Front End

- Extract `ParsedCommand`, argument parsing helpers, and `usage()` into `args.rs`.
- Leave the binary root with:
  - imports
  - `main()`
  - top-level dispatch
- Validation:
  - `cargo test -p allocdb-node --bin allocdb-jepsen -- --nocapture`
  - `./scripts/preflight.sh`

### Slice 2: Watch and Status Modules

- Move watcher-specific code into focused modules:
  - run status snapshot encoding/decoding
  - event log parsing
  - terminal rendering helpers
  - single-lane and fleet watch orchestration
- Keep command behavior and output contract unchanged.
- Validation:
  - binary tests
  - one watcher smoke check against an existing KubeVirt workspace if available
  - `./scripts/preflight.sh`

### Slice 3: Artifact and Run Tracker Modules

- Move:
  - `RunTracker`
  - `RunExecutionContext`
  - status/event file helpers
  - archive/bundle glue
- Keep artifact file names and text formats stable.
- Validation:
  - binary tests for status/event round-trips
  - `./scripts/preflight.sh`

### Slice 4: Runtime and Backend Execution Modules

- Move:
  - external backend trait/glue
  - QEMU/KubeVirt remote command helpers
  - scenario execution helpers
  - failover/rejoin staging helpers
- Keep the validated control surfaces unchanged:
  - `verify-qemu-surface`
  - `verify-kubevirt-surface`
  - `run-qemu`
  - `run-kubevirt`
  - `archive-qemu`
  - `archive-kubevirt`
- Validation:
  - binary tests
  - `./scripts/preflight.sh`

### Slice 5: Test Relocation and Final Reduction

- Relocate binary tests next to the new modules or into one bin-local `tests.rs`.
- Make the root binary materially smaller and mostly dispatch.
- Remove now-dead local helpers and duplicate wiring.
- Validation:
  - `./scripts/preflight.sh`
  - one representative live KubeVirt control run
  - one representative live KubeVirt faulted run

## Acceptance Criteria

- [`allocdb-jepsen.rs`](../crates/allocdb-node/src/bin/allocdb-jepsen.rs) is materially smaller
  and primarily CLI wiring.
- Planning, analysis invocation, watching/rendering, artifact handling, and backend execution live
  in focused internal modules.
- `./scripts/preflight.sh` passes after the refactor.
- One representative live KubeVirt control run and one faulted run still pass after the refactor.

## Decision After This Task

Once the internal seams are stable, re-evaluate whether the next step should be:

- keep the modules in-place under `src/bin/allocdb-jepsen/`, or
- extract them into a dedicated validation crate for future backends such as Hetzner.

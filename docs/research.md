# AllocDB Research Lineage

## Scope

This document records the main external ideas influencing AllocDB's design.

## Primary Influences

### TigerStyle and TigerBeetle

Why it matters:

- explicit bounds
- deterministic execution
- strong assertion culture
- minimal dependencies
- small trusted core

Sources:

- https://tigerbeetle.com/
- https://docs.tigerbeetle.com/concepts/safety/

### Viewstamped Replication Revisited

Why it matters:

- replicated state machine model
- deterministic log application
- view change and recovery structure

Source:

- https://www.cs.princeton.edu/courses/archive/fall19/cos418/papers/vr-revisited.pdf

Status in AllocDB:

- conceptually relevant
- deferred until [`replication.md`](./replication.md)

### Flexible Paxos

Why it matters:

- quorum-shape flexibility
- better framing for future replication tradeoffs

Source:

- https://arxiv.org/abs/1608.06696

Status in AllocDB:

- deferred until replication work begins
- tracked in [`replication.md`](./replication.md)

### Protocol-Aware Recovery for Consensus-Based Storage

Why it matters:

- storage recovery should use protocol knowledge
- corruption handling must be explicit

Source:

- https://pages.cs.wisc.edu/~aws/papers/fast18.pdf

Status in AllocDB:

- informs `fault-model.md` and `storage.md`

### Can Applications Recover from fsync Failures?

Why it matters:

- local durability assumptions are weaker than many systems assume
- fsync failure must be part of the design, not an afterthought

Source:

- https://www.usenix.org/conference/atc20/presentation/rebello

Status in AllocDB:

- informs fail-closed storage handling

### FoundationDB Deterministic Simulation

Why it matters:

- real code under deterministic simulation
- seeded, reproducible fault injection

Source:

- https://apple.github.io/foundationdb/testing.html

Status in AllocDB:

- informs `testing.md`

### Dropbox Nucleus Testing Work

Why it matters:

- single control thread
- design away invalid states
- random fault testing

Source:

- https://dropbox.tech/infrastructure/-testing-our-new-sync-engine

Status in AllocDB:

- informs `principles.md`, `semantics.md`, and `testing.md`

### Jepsen on TigerBeetle 0.16.11

Why it matters:

- exposes client-visible ambiguity under fault
- pushes the design to distinguish definite and indefinite outcomes
- reinforces the need for explicit retry semantics instead of vague "exactly once" claims

Source:

- https://jepsen.io/analyses/tigerbeetle-0.16.11

Status in AllocDB:

- informs `semantics.md`, `fault-model.md`, and `testing.md`

## Usage Rule

These references are design inputs, not cargo cult requirements.

AllocDB should adopt the underlying discipline:

- deterministic state machines
- explicit fault models
- boundedness
- replayable recovery
- testability under injected failure

without copying distributed features into v1 before the single-node allocator is correct.

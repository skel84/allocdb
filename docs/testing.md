# AllocDB Testing Strategy

## Scope

This document defines the v1 testing model and the additional testing gate required before any
replicated release.

## Principle

Unit tests are necessary but not sufficient.

AllocDB should follow the TigerBeetle, FoundationDB, and Dropbox line of thinking:

- deterministic execution is a design property
- simulation should run the real code
- failures should be injected systematically, not only reproduced after the fact

## v1 Testing Layers

### State-Machine Tests

Required coverage:

- every legal transition
- every illegal transition
- resource and reservation state agreement
- terminal-state behavior

### Replay and Recovery Tests

Required coverage:

- WAL replay equivalence
- crash during WAL append
- torn WAL tails
- snapshot plus WAL recovery
- corruption detection and fail-closed behavior

### Idempotency and Submission Tests

Required coverage:

- duplicate `operation_id`
- `operation_id` reuse with different payload
- indefinite outcomes resolved by retry with the same `operation_id`
- behavior at dedupe-window expiry

### Capacity Tests

Required coverage:

- full submission queue
- full reservation table
- full expiration bucket
- maximum TTL and retention settings

### Contention Tests

Required coverage:

- many contenders for one resource
- simultaneous expirations and confirms
- retry storms with reused `operation_id`

## Deterministic Simulation

v1 should add a deterministic simulator around the trusted core as early as practical.

The simulator should control:

- slot advancement
- ingress scheduling order
- WAL write and fsync outcomes
- crash points
- restart timing

Properties:

- seeded and reproducible
- runs real state-machine and recovery code
- supports shrinking failures to minimal cases when possible

### M4-S01 Harness Direction

The `M4-S01` spike selected one external scripted driver as the starting point for `M4-T01`
through `M4-T04`.

The selected shape is:

- wrap the real `allocdb_node::SingleNodeEngine` instead of adding simulator-only execution paths
- keep one explicit simulated current slot in the harness and pass it into real engine calls
- model ingress, tick, checkpoint, crash, restart, and injected persistence faults as explicit
  driver events
- use a seed only to choose ordering among ready actions at the same logical slot

The promoted `M4-T01` harness now lives in `crates/allocdb-node/src/simulation.rs`, with
regression coverage in `crates/allocdb-node/src/simulation_tests.rs`. The current evidence shows:

- the same seed reproduces the same same-slot action order and LSN transcript
- the same seed plus enabled crash-point set reproduces the same one-shot crash selection,
  independent of slice order
- advancing the simulated slot without ticking produces deterministic expiration backlog
- a checkpoint plus WAL-backed restart path still works when an expiration commit halts the live
  engine after append and before sync
- seeded crash plans now interrupt the real engine at client submit/apply, checkpoint, and
  recovery boundaries, with restart tests covering post-sync submit replay, snapshot-written
  before WAL rewrite, and replay-interrupted recovery
- seeded schedule actions can now resolve one labeled ingress or tick action into one candidate
  slot window, replay the same resolved schedule from seed, and record one transcript that captures
  both chosen slots and outcomes
- seeded schedule exploration now covers ingress contention order, same-deadline expiration
  selection while preserving earliest-deadline priority under bounded tick throughput, and retry
  timing across the dedupe window with replay from the same seed
- one-shot storage-fault helpers now cover append-failure halts, sync-failure ambiguity,
  checksum-mismatch fail-closed recovery, and torn-tail truncation against the real WAL and
  restart path

What to reuse in follow-up tasks:

- the external-driver architecture
- explicit slot advancement under test control
- seeded scheduling for same-slot ready work
- labeled schedule actions with candidate slot windows and replayable transcripts
- seeded due-expiration selection over the real internal-expire path while preserving
  earliest-deadline priority
- seeded one-shot crash plans over real engine and recovery boundaries
- one-shot storage-fault helpers over live WAL writes and post-crash WAL mutation
- restart helpers that reopen from snapshot plus WAL on disk

What not to promote directly:

- the original spike's ad hoc helper surface
- any scheduler choice that is not covered by deterministic transcript tests
- opaque randomized loops that do not record the resolved schedule and seed
- crash toggles that are not selected from a seed and named boundary set
- one-off layouts that hide the reusable harness from follow-on simulation tasks

This direction keeps trusted-core churn low because the real engine already exposes the slot,
checkpoint, recovery, and failure-injection seams the simulator needs. It also avoids trait-heavy
virtual clock or fake storage abstractions inside the core before the project has proven they are
necessary.

## External Validation

Before any replicated release, AllocDB should add a Jepsen-style external validation stage.

That stage should verify:

- linearizable command behavior
- behavior under network partition and process crash
- indefinite client outcomes and retry semantics
- failover and recovery semantics

Jepsen is not a substitute for simulation. It is the outer validation layer after deterministic
simulation and fault injection already exist.

See [replication.md](./replication.md) for the deferred replicated-validation scope.

## Research Influence

This testing strategy is informed by:

- TigerBeetle's safety and simulation emphasis
- FoundationDB's deterministic simulation approach
- Dropbox Nucleus' single-control-thread and random fault-testing work
- Jepsen's analysis of real client-visible ambiguity and fault handling

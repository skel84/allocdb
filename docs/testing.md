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

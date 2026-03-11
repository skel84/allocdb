# AllocDB Roadmap

## Status

Draft. This document turns the current product and engineering docs into a dependency-driven
implementation roadmap.

It is intentionally organized around correctness gates, not feature count.

## Planning Principles

The roadmap follows these rules:

1. Freeze semantics before implementation invents behavior.
2. Build the pure deterministic allocator before adding IO and transport.
3. Prove recovery and boundedness before polishing APIs.
4. Add simulation before distributed features.
5. Start replication design only after the single-node core is credible.

These principles follow:

- [semantics.md](./semantics.md)
- [architecture.md](./architecture.md)
- [storage.md](./storage.md)
- [fault-model.md](./fault-model.md)
- [testing.md](./testing.md)

## Workstreams

These workstreams can overlap, but they should not violate milestone dependencies:

- semantics and API surface
- core engine
- durability and recovery
- submission pipeline
- simulation and fault injection
- observability and operational packaging
- documentation
- bounded implementation spikes

## Spike Policy

Spikes are allowed only for implementation uncertainty, not for reopening settled semantics.

Use a spike when:

- multiple implementation shapes are plausible
- the choice materially affects boundedness, determinism, or simplicity
- a short experiment can retire meaningful technical risk

Do not use a spike when the issue is already a product or semantics decision.

Spike outputs must be:

- time-boxed
- disposable by default
- documented with the decision they support

See [spikes.md](./spikes.md).

## Milestones

### M0: Freeze v1 Semantics

Goal:

- stabilize the v1 rules so the codebase does not invent semantics during implementation

Deliverables:

- stable command and result-code surface
- stable retention and capacity knobs
- stable trusted-core crate boundary
- stable slot and TTL rules

Exit criteria:

- no open semantic questions affecting core state transitions
- no ambiguity in indefinite-outcome handling
- no ambiguity in bounded retention behavior
- candidate spike list is approved so experimentation does not drift into product design

Primary docs:

- [semantics.md](./semantics.md)
- [architecture.md](./architecture.md)
- [fault-model.md](./fault-model.md)

### M1: Pure State Machine

Goal:

- implement the allocator as an in-memory deterministic state machine with fixed-capacity structures

Deliverables:

- resource, reservation, and operation models
- fixed-capacity tables
- deterministic expiration index
- command apply functions
- invariant checks
- decisions from the table and timing-wheel spikes folded into the implementation

Exit criteria:

- state-machine tests cover all transitions
- property tests prove no double allocation
- capacity tests prove fail-fast behavior at bounds

### M2: Durability and Recovery

Goal:

- make the state machine durable and replayable through WAL and snapshots

Deliverables:

- WAL frame format and codec
- append and recovery scanner
- snapshot format
- replay path using the same apply logic
- corruption and torn-tail handling
- decisions from the WAL spike folded into the implementation

Exit criteria:

- live apply and replay produce identical results
- crash-recovery tests pass
- corrupted or torn local state fails closed

### M3: Submission Pipeline

Goal:

- add a bounded single-node submission path with idempotent retries and strict reads

Deliverables:

- command envelope validation
- bounded submission queue
- LSN and request-slot assignment
- result publication and retry lookup
- strict-read fence by applied LSN

Exit criteria:

- duplicate `operation_id` tests pass
- indefinite-outcome retry tests pass
- overload behavior is deterministic and bounded

### M4: Deterministic Simulation

Goal:

- exercise the real core under seeded, reproducible fault injection

Deliverables:

- simulated slot driver
- injected crash points
- injected WAL and fsync failures
- reproducible seeded execution harness
- decisions from the simulation spike folded into the implementation

Exit criteria:

- failures are reproducible from seed
- simulated crash/restart scenarios cover recovery logic
- the simulator runs the real core logic, not a mock semantics layer

### M5: Single-Node Alpha

Goal:

- package the single-node allocator into a usable alpha release

Deliverables:

- minimal API surface
- metrics and health signals
- benchmark harness
- operator runbook

Exit criteria:

- benchmark and fault runs are documented
- operational limits are visible
- single-node alpha is usable without semantic caveats

### M6: Replication Design Gate

Goal:

- begin replicated-system design only after the single-node core has earned it

Deliverables:

- expanded [replication.md](./replication.md)
- protocol choice and rationale
- recovery and failover semantics
- replicated simulation plan
- Jepsen validation plan

Exit criteria:

- replication design does not rewrite single-node semantics
- replication work has explicit validation gates
- quorum and failover choices are justified against the research inputs

## Sequencing

The minimum dependency chain is:

```text
M0 -> M1 -> M2 -> M3 -> M4 -> M5 -> M6
```

Allowed overlap:

- docs can advance continuously
- approved spikes can run ahead of their consuming milestone if they stay time-boxed and disposable
- some M2 storage scaffolding can start during late M1
- some M5 API and metrics work can start during late M3

Not allowed:

- networking or async polish before M1 semantics are proven
- replication design pressure changing M1-M5 semantics

## Review Rhythm

Suggested review points:

1. end of M0: semantic freeze review
2. end of M1: invariant and state-machine review
3. end of M2: durability and corruption-handling review
4. end of M4: simulation credibility review
5. end of M5: alpha readiness review

## Planning Output

The milestone plan here is paired with:

- [work-breakdown.md](./work-breakdown.md)

That document lists the concrete units of work that can be tracked in issue or ticket form.

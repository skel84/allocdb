# AllocDB Work Breakdown

## Status

Draft. This document breaks the roadmap into concrete milestone-scoped tasks sized for
implementation and review.

## Task Rules

Each task should:

- produce one reviewable artifact
- be small enough for roughly 1 to 3 days of focused work
- have explicit dependencies
- have acceptance criteria
- end with concrete test evidence

Tracking template:

- `Goal`
- `Inputs`
- `Changes`
- `Blocked By`
- `Acceptance Criteria`
- `Test Evidence`
- `Non-Goals`

## Naming

- tasks use `M#-T#` identifiers such as `M2-T08`
- spikes use `M#-S#` identifiers such as `M1-S01`
- GitHub issues and PRs should use the same identifiers when they map to planned work

## M0: Freeze v1 Semantics

### M0-T01: Approve spike list and guardrails

Goal:

- decide which implementation uncertainties justify spikes and which do not

Blocked by:

- [spikes.md](./spikes.md)

Acceptance criteria:

- planned spikes are listed and time-boxed
- semantics questions are explicitly excluded from spike scope

Test evidence:

- docs review only

### M0-T02: Define result codes

Goal:

- finalize deterministic result and error codes for v1 commands

Blocked by:

- [semantics.md](./semantics.md)

Acceptance criteria:

- every command has success and failure outcomes documented
- indefinite outcomes are distinguished from state-machine results

Test evidence:

- docs review only

### M0-T03: Define config knobs and bounds

Goal:

- finalize the required configuration surface for bounds and retention

Acceptance criteria:

- `MAX_*` and slot-window knobs are listed in one place
- no bound affecting correctness remains implicit

Test evidence:

- docs review only

### M0-T04: Define trusted-core crate boundaries

Goal:

- specify which modules belong to the trusted core and which do not

Acceptance criteria:

- core, ingress, tooling, and observability boundaries are documented
- dependency policy can be checked against module boundaries

Test evidence:

- docs review only

### M0-T05: Decide version-guarded command semantics

Goal:

- decide whether v1 needs `conditional_confirm` or another version-guarded write surface

Blocked by:

- [semantics.md](./semantics.md)

Acceptance criteria:

- docs explain whether `reservation_id` already prevents the relevant ABA cases
- if a version guard is adopted, the exact command shape and failure semantics are explicit
- if deferred, the deferral rationale is written down explicitly

Test evidence:

- docs review only

## M1: Pure State Machine

### M1-S01: Fixed-capacity table experiment

Goal:

- compare candidate implementations for resource, reservation, and operation storage

Blocked by:

- M0-T01
- M0-T02
- M0-T03

Acceptance criteria:

- at least two plausible table shapes are compared
- the experiment ends with one chosen approach and a short rationale
- spike code is either deleted or clearly marked as non-production

Test evidence:

- benchmark notes or focused experiment output

### M1-S02: Timing-wheel experiment

Goal:

- validate bucket shape, overflow behavior, and retirement interaction for the expiration index

Blocked by:

- M0-T01
- M0-T03

Acceptance criteria:

- overflow behavior is explicit
- retirement and slot advancement are tested in the spike
- one concrete timing-wheel shape is selected

Test evidence:

- focused experiment output

### M1-T01: Implement ID newtypes and core records

Goal:

- add Rust types for resource, reservation, operation, LSN, and slot identifiers

Blocked by:

- M0-T02
- M0-T03

Acceptance criteria:

- no persistent-format field uses implicit-width integer types
- compile-time layout checks exist where useful

Test evidence:

- unit tests for parsing, construction, and size assertions

### M1-T02: Implement fixed-capacity resource table

Goal:

- create the in-memory resource store with explicit capacity

Blocked by:

- M1-T01
- M1-S01

Acceptance criteria:

- insert, lookup, and update semantics are deterministic
- capacity exhaustion fails explicitly

Test evidence:

- unit tests and capacity-bound tests

### M1-T03: Implement fixed-capacity reservation table

Goal:

- create the shared active-plus-terminal reservation store

Blocked by:

- M1-T01
- M0-T03
- M1-S01

Acceptance criteria:

- supports active and terminal reservation retention
- retirement frees slots for reuse

Test evidence:

- unit tests for insert, retire, reuse

### M1-T04: Implement fixed-capacity operation table

Goal:

- add bounded idempotency storage keyed by `operation_id`

Blocked by:

- M1-T01
- M1-S01

Acceptance criteria:

- duplicate same-payload lookup returns the original result
- mismatched payload returns `operation_conflict`

Test evidence:

- unit tests for duplicates and conflict cases

### M1-T05: Implement timing-wheel expiration index

Goal:

- create the fixed-capacity expiration structure keyed by `deadline_slot`

Blocked by:

- M1-T01
- M1-T03
- M1-S02

Acceptance criteria:

- bounded bucket behavior is explicit
- overflow returns `expiration_index_full`

Test evidence:

- unit tests for scheduling, draining, and overflow

### M1-T06: Implement `create_resource`

Goal:

- add the pure apply logic for resource creation

Blocked by:

- M1-T02
- M1-T04

Acceptance criteria:

- deterministic success and `already_exists` behavior

Test evidence:

- state-machine unit tests

### M1-T07: Implement `reserve`

Goal:

- add the pure apply logic for reservation creation

Blocked by:

- M1-T02
- M1-T03
- M1-T04
- M1-T05

Acceptance criteria:

- only one active reservation may exist for a resource
- deadline and reservation ID derivation are deterministic

Test evidence:

- state-machine tests and contention property tests

### M1-T08: Implement `confirm`

Goal:

- add confirm transition logic

Blocked by:

- M1-T03
- M1-T07

Acceptance criteria:

- `holder_id` is checked
- invalid-state and retired-reservation behavior are explicit

Test evidence:

- transition tests and negative-path tests

### M1-T09: Implement `release`

Goal:

- add release transition logic

Blocked by:

- M1-T03
- M1-T07

Acceptance criteria:

- resource returns to `available`
- terminal-state retention fields are set correctly

Test evidence:

- transition tests and negative-path tests

### M1-T10: Implement `expire`

Goal:

- add internal expiration command logic

Blocked by:

- M1-T03
- M1-T05
- M1-T07

Acceptance criteria:

- no-op behavior is deterministic for raced cases
- no early reuse is possible

Test evidence:

- interleaving tests with confirm and release

### M1-T11: Add invariant assertion layer

Goal:

- centralize internal checks for table consistency and state agreement

Blocked by:

- M1-T02 through M1-T10

Acceptance criteria:

- resource and reservation state agreement is asserted
- corruption paths fail closed

Test evidence:

- invariant tests and negative injected-state tests

## M1H: Constant-Time Core Hardening

### M1H-S01: Fixed-capacity open-addressed table experiment

Goal:

- compare deterministic open-addressing shapes for the trusted-core lookup tables

Blocked by:

- M1 exit criteria

Acceptance criteria:

- probe strategy, tombstone policy, and resize-free behavior are compared explicitly
- the experiment ends with one deterministic table design and a short rationale
- spike output includes expected full-capacity failure behavior

Test evidence:

- focused experiment output or benchmark notes

### M1H-T01: Implement deterministic fixed-capacity hash-table primitive

Goal:

- add the shared open-addressed table primitive for the trusted core

Blocked by:

- M1H-S01

Acceptance criteria:

- no randomized seeds are used
- probe bounds are explicit and testable
- full-capacity behavior fails explicitly

Test evidence:

- primitive unit tests for insert, lookup, replace, delete, and full-capacity behavior

### M1H-T02: Replace resource lookup with constant-time table access

Goal:

- remove binary-search and shifting-insert costs from resource lookup and update paths

Blocked by:

- M1H-T01

Acceptance criteria:

- resource lookup is constant-time in the intended design
- resource updates do not require shifting unrelated records

Test evidence:

- state-machine regression tests and table-focused unit tests

### M1H-T03: Replace reservation and operation lookup with constant-time table access

Goal:

- remove binary-search and shifting-insert costs from reservation and operation lookup paths

Blocked by:

- M1H-T01

Acceptance criteria:

- reservation and operation lookup are constant-time in the intended design
- duplicate-operation lookup no longer depends on sorted `Vec` order

Test evidence:

- state-machine regression tests and table-focused unit tests

### M1H-T04: Replace full-table retirement scans with ordered retirement draining

Goal:

- retire reservations and operation records in expiration order instead of using full-table
  `retain` scans on every apply

Blocked by:

- M1H-T03

Acceptance criteria:

- retirement work is proportional to expired entries
- reservation retirement and operation dedupe retirement do not scan the whole lookup table
- the ordering structure is bounded and deterministic

Test evidence:

- retirement regression tests and capacity-bound tests

### M1H-T05: Decide and implement version-guarded confirm handling

Goal:

- either add `conditional_confirm` or explicitly document why `confirm` remains keyed only by
  `reservation_id`

Blocked by:

- M0-T05
- M1-T08

Acceptance criteria:

- if implemented, version mismatch behavior is deterministic and tested
- if deferred, the docs explain which stale-read races are already prevented by `reservation_id`
  and which are intentionally unsupported in v1

Test evidence:

- docs review only, or state-machine tests if the command is added

## M2: Durability and Recovery

### M2-S01: WAL framing and torn-tail experiment

Goal:

- validate the binary framing shape and recovery boundary logic before committing to the real codec

Blocked by:

- M0-T01
- M1-T01

Acceptance criteria:

- corrupted-frame and torn-tail cases are exercised
- one framing shape is selected and documented

Test evidence:

- focused experiment output

### M2-T01: Define WAL frame codec

Goal:

- implement binary WAL frame encode and decode with checksum verification

Blocked by:

- M1-T01
- M2-S01

Acceptance criteria:

- framing is explicit and versioned
- invalid checksum is detected

Test evidence:

- codec unit tests with corrupted frames

### M2-T02: Implement append-only WAL writer

Goal:

- append validated frames to the live WAL file

Blocked by:

- M2-T01

Acceptance criteria:

- write path honors `MAX_COMMAND_BYTES`
- fsync failures are surfaced explicitly

Test evidence:

- file-backed tests with induced write failures

### M2-T03: Implement recovery scanner

Goal:

- scan WAL history and stop at the last valid boundary

Blocked by:

- M2-T01
- M2-T02

Acceptance criteria:

- torn tails are truncated
- invalid frames do not leak into replay

Test evidence:

- torn-tail recovery tests

### M2-T04: Define snapshot format

Goal:

- define a snapshot representation for core tables and indexes

Blocked by:

- M1-T02 through M1-T05

Acceptance criteria:

- snapshot contains exactly the state required for replayable recovery
- no implicit-layout serializer is used

Test evidence:

- round-trip snapshot tests

### M2-T05: Implement snapshot writer and loader

Goal:

- persist and load snapshots safely

Blocked by:

- M2-T04

Acceptance criteria:

- temp-file, fsync, rename flow is implemented
- corrupted snapshot is rejected

Test evidence:

- crash and corruption tests around snapshot load

### M2-T06: Implement replay using the live apply path

Goal:

- recover allocator state by replaying WAL into the real state machine

Blocked by:

- M2-T02
- M2-T03
- M2-T05
- M1-T11

Acceptance criteria:

- replay shares the same apply logic as live execution
- live and recovered states match exactly

Test evidence:

- replay-equivalence tests

### M2-T07: Distinguish torn tails from durable-log corruption

Goal:

- classify WAL recovery stop reasons so only EOF torn tails are auto-truncated

Blocked by:

- M2-T03
- M2-T06

Acceptance criteria:

- incomplete EOF tails are classified as expected crash artifacts
- checksum or framing failures in the middle of durable history fail closed
- recovery surfaces the failure kind and byte offset clearly

Test evidence:

- torn-tail tests and mid-log corruption tests

### M2-T08: Add safe checkpoint coordination and WAL truncation rules

Goal:

- coordinate snapshot replacement and WAL retention so recovery always has overlapping durable
  history

Blocked by:

- M2-T05
- M2-T06
- M2-T07

Acceptance criteria:

- checkpoint metadata makes the active snapshot anchor explicit
- any WAL truncation preserves overlap through the previously successful snapshot anchor
- crash during snapshot replacement or WAL rewrite remains recoverable
- the implementation either rewrites WAL prefixes safely or introduces segmented retention; it does
  not pretend suffix truncation solves prefix-retention needs

Test evidence:

- checkpoint crash/restart tests and retained-history recovery tests

## M3: Submission Pipeline

### M3-T01: Implement command envelope validation

Goal:

- validate command envelope and payload before sequencing

Blocked by:

- M0-T02
- M2-T06

Acceptance criteria:

- malformed commands fail before commit
- validation does not mutate state

Test evidence:

- request validation tests

### M3-T02: Implement bounded submission queue

Goal:

- add the ingress queue with explicit overflow behavior

Blocked by:

- M0-T03

Acceptance criteria:

- overflow returns deterministic overload behavior
- queue growth is bounded

Test evidence:

- overload and backpressure tests

### M3-T03: Implement sequencer

Goal:

- assign LSN and `request_slot` deterministically

Blocked by:

- M2-T02
- M3-T01
- M3-T02

Acceptance criteria:

- sequencing order is explicit
- request-slot assignment is visible in WAL frames

Test evidence:

- sequencing tests

### M3-T04: Implement result publication and retry lookup

Goal:

- publish command results and resolve duplicate `operation_id` lookups

Blocked by:

- M1-T04
- M3-T03

Acceptance criteria:

- same `operation_id` returns original result
- mismatch returns `operation_conflict`

Test evidence:

- duplicate and conflict-path tests

### M3-T05: Implement strict-read fence

Goal:

- serve strict reads only after required LSN has been applied

Blocked by:

- M2-T06
- M3-T03

Acceptance criteria:

- strict reads are tied to `applied_lsn`

Test evidence:

- read-fence tests

### M3-T06: Implement indefinite-outcome retry behavior

Goal:

- document and test retry with the same `operation_id` after timeout or reply loss

Blocked by:

- M3-T04

Acceptance criteria:

- no duplicate execution occurs for the same `operation_id`
- ambiguity is resolved within retention only
- submission failures distinguish definite pre-commit rejection from indefinite post-write
  ambiguity

Test evidence:

- retry and retention-expiry tests

## M4: Deterministic Simulation

### M4-S01: Simulation harness experiment

Goal:

- prove the trusted core can run under a seeded simulated driver without forking semantics

Blocked by:

- M0-T01
- M3-T03

Acceptance criteria:

- real state-machine code runs under simulated slot advancement
- at least one crash/restart scenario is exercised by the spike
- one simulator shape is selected

Test evidence:

- focused experiment output

### M4-T01: Build simulated slot driver

Goal:

- run the core against a deterministic simulated clock

Blocked by:

- M3-T03
- M4-S01

Acceptance criteria:

- slot advancement is deterministic and seedable

Test evidence:

- simulator tests with reproducible seeds

### M4-T02: Inject crash points

Goal:

- allow seeded crashes around WAL, apply, and recovery boundaries

Blocked by:

- M2-T06
- M4-T01

Acceptance criteria:

- crash points are reproducible
- recovery resumes with correct replay behavior

Test evidence:

- crash-seed regression tests

### M4-T03: Inject storage faults

Goal:

- simulate torn writes, checksum mismatch, and fsync failures

Blocked by:

- M2-T03
- M2-T05
- M4-T01

Acceptance criteria:

- fail-closed storage behavior is exercised in simulation

Test evidence:

- storage fault-injection tests

### M4-T04: Add seeded schedule exploration

Goal:

- vary ingress order, expiration order, and retry timing under a reproducible seed

Blocked by:

- M4-T01

Acceptance criteria:

- failures can be reproduced from seed

Test evidence:

- schedule-seed regression cases

## M5: Single-Node Alpha

### M5-T01: Add minimal API surface

Goal:

- expose the single-node allocator through a stable alpha API

Blocked by:

- M3-T06

Acceptance criteria:

- API matches documented command semantics
- indefinite-outcome behavior is documented for clients

Test evidence:

- API integration tests

### M5-T02: Add metrics and health signals

Goal:

- expose core operational signals for lag, overload, and recovery

Blocked by:

- M3-T02
- M4-T03

Acceptance criteria:

- `logical_slot_lag = current_wall_clock_slot - last_request_slot` is visible outside the trusted
  core
- expiration backlog and recovery status are visible
- queue pressure is visible
- operation-table utilization is visible before `operation_table_full`

Test evidence:

- metrics integration tests

### M5-T03: Add benchmark harness

Goal:

- measure hot-spot contention and boundedness behavior

Blocked by:

- M5-T01

Acceptance criteria:

- benchmark scenarios include one-resource-many-contenders and high retry pressure

Test evidence:

- documented benchmark runs

### M5-T04: Write operator runbook

Goal:

- document startup, recovery, overload, and corruption-handling behavior

Blocked by:

- M5-T02

Acceptance criteria:

- runbook matches actual system behavior

Test evidence:

- docs review only

## M6: Replication Design Gate

### M6-T01: Expand replication protocol notes

Goal:

- turn [replication.md](./replication.md) into a real protocol-design draft

Blocked by:

- M1 through M5 exit criteria

Acceptance criteria:

- protocol family and invariants are explicit
- replication does not rewrite single-node semantics

Test evidence:

- design review only

### M6-T02: Define replicated simulation plan

Goal:

- specify how deterministic simulation extends to replicated execution

Blocked by:

- M6-T01

Acceptance criteria:

- partitions, primary crash, and rejoin are part of the simulation plan

Test evidence:

- design review only

### M6-T03: Define Jepsen validation plan

Goal:

- specify the Jepsen workloads and properties required before any replicated release

Blocked by:

- M6-T02

Acceptance criteria:

- covers client ambiguity, failover, recovery, and linearizable behavior
- workloads and invariants are explicit and tied to replicated semantics

Test evidence:

- design review only

## M7: Replicated Core Prototype

### M7-T01: Add replicated node state and durable protocol metadata

Goal:

- build the replica wrapper state and durable protocol metadata required by the first replicated
  prototype

Blocked by:

- M6-T03

Acceptance criteria:

- replicated node startup has explicit durable protocol metadata
- local validation and faulted-state entry are defined in code and tests
- no simulator-only replica execution path is introduced
- docs stay aligned if the persisted metadata contract changes

Test evidence:

- `./scripts/preflight.sh`

### M7-T02: Build deterministic 3-replica cluster harness

Goal:

- build the deterministic in-process cluster harness for the replicated prototype

Blocked by:

- M7-T01

Acceptance criteria:

- the harness runs three real replicas without mock state-machine semantics
- message delivery, partitioning, crash, and restart are explicit driver actions
- the same seed and starting state reproduce the same transcript
- docs and harness APIs stay aligned with [testing.md](./testing.md)

Test evidence:

- `./scripts/preflight.sh`

### M7-T03: Implement quorum write path with one configured primary

Goal:

- implement the first majority-backed write path with one configured primary and primary-only reads

Blocked by:

- M7-T01
- M7-T02

Acceptance criteria:

- committed write results are published only after majority durable append
- backups do not apply entries before they are committed
- the existing allocator executor remains the only apply path
- tests cover normal quorum write and primary-only read behavior

Test evidence:

- `./scripts/preflight.sh`

### M7-T04: Add view change and fail-closed reads on quorum loss

Goal:

- implement higher-view takeover plus fail-closed read behavior under quorum ambiguity

Blocked by:

- M7-T03

Acceptance criteria:

- a quorum-lost primary fails closed for reads and writes
- a new primary reconstructs the latest safe committed prefix before normal mode
- committed entries survive failover unchanged
- tests cover primary loss, higher-view takeover, and stale-node read rejection

Test evidence:

- `./scripts/preflight.sh`

### M7-T05: Add suffix catch-up, snapshot transfer, and rejoin

Goal:

- implement stale-replica catch-up and safe rejoin

Blocked by:

- M7-T04

Acceptance criteria:

- stale replicas can rejoin without rewriting committed history
- divergent uncommitted suffix is discarded safely during catch-up
- corrupted replicas do not vote, lead, or serve before repair
- tests cover suffix-only catch-up, snapshot transfer, and faulted rejoin rejection

Test evidence:

- `./scripts/preflight.sh`

### M7-T06: Promote replicated simulation scenarios into executable tests

Goal:

- turn the replicated simulation plan into executable regression coverage

Blocked by:

- M7-T05

Acceptance criteria:

- partition, primary-crash, and rejoin scenarios execute in the real replicated harness
- the same seed and starting state reproduce the same replicated transcript
- promoted tests check the invariants documented in [testing.md](./testing.md)
- failing schedules can be replayed directly from recorded transcript data

Test evidence:

- `./scripts/preflight.sh`

## M8: External Cluster Validation

### M8-T01: Build multi-process local replicated cluster runner

Goal:

- build the first multi-process local cluster runner for replicated AllocDB

Blocked by:

- M7-T05

Acceptance criteria:

- a local operator can start and stop a three-replica cluster from one command surface
- node identities and durable workspaces remain stable across restart
- logs and control hooks are sufficient for follow-on fault injection
- docs stay aligned with the local cluster runner usage

Test evidence:

- `./scripts/preflight.sh`

### M8-T02: Add local fault-control harness for process and network disruption

Goal:

- add a local fault-control harness for process crash, restart, and network isolation

Blocked by:

- M8-T01

Acceptance criteria:

- process and network disruption are controllable through one local harness
- the harness can isolate stale primaries and later heal the cluster
- cluster timelines are recorded well enough for later checker and debug use
- follow-on validation work can reuse the same fault-control surface

Test evidence:

- `./scripts/preflight.sh`

### M8-T03: Build local QEMU replicated testbed

Goal:

- build the local QEMU-backed replicated cluster testbed

Blocked by:

- M8-T02

Acceptance criteria:

- the QEMU environment boots a three-replica cluster plus one control node locally
- control hooks can isolate networks, reboot guests, and collect logs
- the environment is repeatable enough for scripted validation runs
- docs describe how the QEMU testbed maps to the first Jepsen gate

Test evidence:

- `./scripts/preflight.sh`

### M8-T04: Implement Jepsen harness against QEMU-backed cluster

Goal:

- implement the Jepsen harness and release-blocking validation runs against the QEMU-backed cluster

Blocked by:

- M7-T06
- M8-T03

Acceptance criteria:

- the Jepsen harness runs the documented workload families against the QEMU-backed cluster
- ambiguous outcomes are retried and interpreted with stable `operation_id` semantics
- the documented release blockers are enforced automatically by the analysis step
- the external validation docs stay aligned with the implemented harness

Test evidence:

- `./scripts/preflight.sh`

## M9: Lease Kernel Follow-On

### M9-T01: Freeze generic lease-kernel scope and non-goals

Goal:

- define the minimal generic AllocDB lease-kernel surface needed by downstream consumers without
  moving scheduler or product policy into the trusted core

Blocked by:

- [lease-kernel-follow-on.md](./lease-kernel-follow-on.md)

Sequencing note:

- planning overlap with late `M8-T04` is allowed
- code-bearing `M9` tasks still remain downstream of the external-validation gate

Acceptance criteria:

- bundle ownership, fencing, revoke, and liveness-boundary work are explicitly in or out of scope
- Kubernetes, queueing, topology, routing, and chargeback concerns stay explicitly outside AllocDB
- the follow-on is documented as a generic lease-kernel extension, not a product-specific fork

Test evidence:

- docs review only

### M9-T02: Decide bundle lease data model and invariants

Goal:

- choose the exact ownership model for all-or-nothing multi-resource commits

Blocked by:

- M9-T01
- [semantics.md](./semantics.md)

Acceptance criteria:

- the bundle identifier strategy is explicit
- per-resource and bundle-level invariants are explicit
- partial visibility and partial success are ruled out explicitly
- retained-history rules for bundle members are documented

Test evidence:

- docs review only

### M9-T03: Decide fencing-token and stale-holder semantics

Goal:

- define how external holders prove current authority and how stale actors are rejected

Blocked by:

- M9-T02

Acceptance criteria:

- token shape and monotonicity rules are explicit
- stale-holder rejection outcomes are documented
- replay and failover preserve the same fencing behavior

Test evidence:

- docs review only

### M9-T04: Decide revoke and liveness-boundary semantics

Goal:

- define explicit revoke behavior and the boundary between external liveness observation and
  trusted-core ownership transitions

Blocked by:

- M9-T01
- M9-T03

Acceptance criteria:

- revoke is documented as an explicit logged transition
- the trusted core does not read wall-clock time or external heartbeats directly
- reclaim semantics still preserve the late-not-early reuse rule

Test evidence:

- docs review only

### M9-T05: Extend semantics, API, and fault-model docs for the approved lease surface

Goal:

- fold the approved follow-on decisions into the authoritative product and engineering docs

Blocked by:

- M9-T02
- M9-T03
- M9-T04

Acceptance criteria:

- [semantics.md](./semantics.md), [api.md](./api.md), [architecture.md](./architecture.md), and
  [fault-model.md](./fault-model.md) reflect the approved command and result-code surface
- the docs make clear which previous single-resource commands remain unchanged, which are
  generalized, and which are superseded
- retention and idempotency rules for the new primitives are explicit

Test evidence:

- docs review only

### M9-T06: Implement atomic bundle commit in the trusted core

Goal:

- add the real all-or-nothing multi-resource ownership transition in `allocdb-core`

Blocked by:

- M9-T05

Acceptance criteria:

- a successful bundle commit acquires all target resources atomically
- failure leaves no partial ownership behind
- deterministic capacity and conflict failures remain explicit and bounded
- trusted-core invariants cover bundle membership and per-resource exclusivity

Test evidence:

- `./scripts/preflight.sh`

### M9-T07: Implement fencing tokens and stale-holder rejection

Goal:

- add the approved authority token model to live execution, replay, and read surfaces

Blocked by:

- M9-T06

Acceptance criteria:

- holder-facing operations can reject stale authority deterministically
- duplicate requests remain idempotent under the new token rules
- the read surface exposes the authority information needed by external controllers

Test evidence:

- `./scripts/preflight.sh`

### M9-T08: Implement explicit revoke and safe reuse rules

Goal:

- add revoke behavior without weakening the late-not-early reuse guarantee

Blocked by:

- M9-T07

Acceptance criteria:

- revoke transitions are logged and replayable
- duplicate revoke requests are deterministic
- revoked ownership cannot be reused early under crash, retry, or failover

Test evidence:

- `./scripts/preflight.sh`

### M9-T09: Extend WAL, snapshot, recovery, and transport surfaces for lease primitives

Goal:

- preserve the new lease semantics through persistence, restart, and external API boundaries

Blocked by:

- M9-T08

Acceptance criteria:

- WAL and snapshot formats can encode the new primitives
- live apply and replay share the same path and produce the same results
- wire-level request and response surfaces expose the approved fields without ambiguity

Test evidence:

- `./scripts/preflight.sh`

### M9-T10: Preserve lease primitives across replication and failover

Goal:

- extend the replicated path so bundle ownership, fencing, and revoke semantics survive quorum
  replication and view change unchanged

Blocked by:

- M9-T09

Acceptance criteria:

- replication does not invent a second apply path for the new primitives
- failover preserves committed bundle ownership and fencing outcomes
- stale primaries and stale holders still fail closed

Test evidence:

- `./scripts/preflight.sh`

### M9-T11: Add simulation and regression coverage for bundle, revoke, and stale-holder faults

Goal:

- promote the new lease-kernel safety rules into reproducible regression coverage

Blocked by:

- M9-T10

Acceptance criteria:

- tests cover duplicate bundle retries, partial-failure avoidance, revoke races, stale-holder
  rejection, crash recovery, and replicated failover
- the new scenarios run through the same deterministic simulation and replicated-validation
  surfaces already used elsewhere in the repo

Test evidence:

- `./scripts/preflight.sh`

## Suggested First Slice

If implementation starts immediately, the highest-value first slice is:

1. M0-T01
2. M0-T02
3. M0-T03
4. M1-T01
5. M1-S01
6. M1-S02
7. M1-T02
8. M1-T03
9. M1-T04
10. M1-T05
11. M1-T07
12. M1-T08
13. M1-T09
14. M1-T10
15. M1-T11

That sequence yields the pure allocator core before any durability or API work.

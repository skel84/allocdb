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

## Replicated Deterministic Simulation

`M6-T02` extends the single-node simulation approach to replicated execution without introducing a
mock semantics layer.

The rule stays the same:

- run the real allocator and recovery code
- keep time, message delivery, crash, and restart under explicit test control
- make every schedule decision reproducible from seed plus transcript

### Design Goal

The replicated simulator should answer one question before Jepsen exists:

```text
can the chosen replication protocol preserve the single-node invariants under deterministic fault schedules?
```

That means the simulator is not a toy cluster model. It is a deterministic cluster driver around
real replica state, real durable state transitions, and real retry semantics.

### Cluster Harness Shape

The first replicated harness should model one fixed-membership shard with `3` real replicas.

The harness should:

- wrap one real replicated node per replica, each with its own WAL and snapshot workspace
- keep one explicit simulated slot counter shared by the cluster driver
- treat protocol messages as explicit driver-visible events
- treat timeout firing, view change, crash, restart, and rejoin as explicit driver-visible events
- use a seed only to choose among already-ready actions at the same simulated slot
- record one transcript containing seed, chosen actions, delivered messages, dropped messages, and
  resulting view and `lsn` observations

What must not happen:

- no simulator-only apply path
- no fake replica state that bypasses the real durable log and recovery path
- no hidden random network loop that cannot be replayed from transcript

### Driver Actions

The replicated harness should choose among explicit labeled actions such as:

- `client_submit`
- `deliver_protocol_message`
- `drop_protocol_message`
- `advance_slot`
- `fire_timeout`
- `crash_replica`
- `restart_replica`
- `allow_rejoin`
- `complete_snapshot_transfer`

As with the current single-node schedule exploration, the harness should resolve one ready action
into one recorded transcript step. The recorded step should include enough metadata to replay the
same cluster schedule exactly.

### Network And Failure Model In Simulation

The deterministic cluster driver should model:

- connectivity as an explicit replica-to-replica and client-to-replica delivery matrix
- partitions as rule changes in that matrix, not as implicit timing guesses
- process crash as loss of volatile state with durable WAL and snapshot files left on disk
- restart as reopening the replica from its own durable state plus replicated catch-up
- rejoin as restoring connectivity and allowing suffix catch-up or snapshot-plus-suffix catch-up

The simulator does not need arbitrary packet corruption in the first replicated design pass.
Message loss, partition, delay by non-delivery, crash, restart, and rejoin are the required first
faults.

### Required Scenario Families

The first replicated simulation plan must cover these families explicitly.

#### Partition Scenarios

- isolate the primary from one backup but keep quorum, so writes still commit and the minority
  replica later catches up
- isolate the primary from the majority, so the old primary stops serving and a new primary must
  win the higher view
- split the cluster into non-quorum minorities, so no side commits and reads fail closed
- heal the partition and verify that all healthy replicas converge on one committed prefix

Key checks:

- no split-brain commit
- no read served from a view-uncertain or quorum-lost replica
- retries with the same `operation_id` resolve ambiguity without duplicate execution

#### Primary Crash Scenarios

- crash the primary before quorum append, so the write remains uncommitted and clients see only an
  indefinite outcome
- crash the primary after quorum append but before client reply, so retry must recover the
  committed result
- crash the primary after reply and force later reads and retries through the new primary
- crash during expiration leadership, so overdue work may be delayed but never applied early

Key checks:

- committed entries survive failover unchanged
- uncommitted suffix does not become visible as committed history
- expiration remains log-driven and may be late but not early

#### Rejoin And Recovery Scenarios

- restart a stale backup and catch up by replicated suffix only
- restart a replica whose local state requires snapshot-plus-suffix transfer
- rejoin a replica that holds an uncommitted divergent suffix and verify that the suffix is
  discarded safely
- restart a replica with invalid local durable state and verify that it stays faulted instead of
  voting or serving

Key checks:

- rejoined replicas recover the committed prefix already accepted by the healthy quorum
- committed history is never rewritten during catch-up
- corrupted replicas fail closed until repaired

### Required Invariants In Simulation

Every promoted replicated simulation test should check some subset of these invariants:

- same seed plus same starting durable state yields the same transcript
- no two different payloads commit at the same `lsn`
- replicas that apply the same committed prefix reach the same allocator state and outputs
- a client-visible success is published only for a quorum-committed entry
- retry with the same `operation_id` never creates a second successful execution
- reads succeed only on the current primary after the requested `required_lsn` is locally applied
- resource reuse after expiration is never earlier than the single-node rules allow
- rejoin never lets a stale or corrupted replica serve, vote, or lead before validation completes

### Promotion Path

The recommended implementation sequence is:

1. add a deterministic cluster driver that can host `3` real replicas, one message queue, and one
   explicit connectivity map
2. add seeded message-delivery and timeout scheduling with replayable transcripts
3. add partition scenarios that prove fail-closed reads and no split-brain commit
4. add primary-crash scenarios that exercise retry semantics around quorum commit boundaries
5. add rejoin scenarios for suffix catch-up, snapshot transfer, and divergent-suffix truncation
6. add replicated storage-fault combinations only after the basic cluster schedule is already
   replayable

This keeps the first replicated simulator narrow enough to validate protocol behavior before
expanding into broader randomized search.

## Jepsen Validation Gate

`M6-T03` defines the external validation required before any replicated release.

### Design Goal

Jepsen should answer a different question than deterministic simulation:

```text
does the deployed replicated system preserve the client contract under real network faults and client-visible ambiguity?
```

Deterministic simulation proves protocol behavior against explicit schedules. Jepsen validates the
same contract through the real client surface, real routing mistakes, real timeout behavior, and
real failover and recovery timing.

### Minimum Testbed

The first Jepsen gate should target the same narrow release shape as the replication draft:

- one shard
- `3` replicas
- fixed membership
- the real external API planned for the replicated release
- clients that preserve `operation_id` across retries
- a pre-created bounded resource set large enough to force contention and expiration
- logical-slot advancement and `tick_expirations` driven through the public or operator-facing
  surface, not simulator internals

`5`-replica clusters, online reconfiguration, and multi-shard behavior are follow-on work, not
part of the first Jepsen gate.

### Required Workload Families

Every release candidate should run these workload families.

#### Reservation Contention

- many clients reserve, confirm, and release from a hot resource set
- duplicate retries and conflicting `operation_id` reuse are injected deliberately
- reads sample resource and reservation state during load

Checks:

- successful operations remain linearizable
- no resource is committed to two holders at once
- one `operation_id` never produces two successful executions

#### Ambiguous Write Retry

- crash or isolate the primary around quorum-commit boundaries for `reserve`, `confirm`, and
  `release`
- let clients observe timeouts or indefinite write outcomes, then force retries with the same
  `operation_id`

Checks:

- an ambiguous write resolves to at most one committed result
- retry returns the original committed result after failover when the first attempt already
  committed
- unresolved ambiguity after cleanup fails the gate

#### Failover And Read Fences

- mix writes with `required_lsn` reads while primaries fail, elections run, and some clients route
  to stale nodes
- keep both read-only traffic and read-after-write traffic active during failover

Checks:

- successful reads come only from the current primary after the requested `required_lsn` is
  locally applied
- stale or quorum-lost replicas fail closed instead of serving stale success
- committed state remains linearizable across primary change

#### Expiration And Recovery

- create expiring reservations, advance logical time, call `tick_expirations`, and combine that
  load with crash, partition, restart, and rejoin
- force rejoin after stale state, suffix catch-up, and snapshot-plus-suffix recovery paths

Checks:

- expiration may be delayed by failover but never frees a resource early
- restarted and rejoined replicas converge on committed history before serving or voting
- recovery preserves the same client-visible result for retried operations

### Nemesis Families

The first Jepsen gate should explicitly cover:

- primary crash and restart
- majority-loss and minority-loss partitions plus heal
- stale-primary isolation with client misrouting to the old primary
- backup crash during catch-up and later rejoin
- mixed crash-plus-partition schedules around ambiguous writes

Clock skew, disk corruption, membership change, and multi-shard faults are not part of the first
Jepsen gate. Those are either already covered by deterministic simulation and local durability
testing or deferred until later replicated milestones.

### History Interpretation

Jepsen must interpret AllocDB histories using the product's retry contract, not a generic
"exactly once" assumption.

Rules:

- every mutating client operation carries a stable `operation_id`
- definite successes and definite failures enter the history directly
- timeouts, transport loss, and other indefinite outcomes are recorded as ambiguous client events,
  not as silent success or failure
- the checker folds an ambiguous event and all later retries with the same `operation_id` into one
  logical command
- cleanup retries every ambiguous command within the dedupe window; any ambiguity that remains
  unresolved after cleanup fails the gate
- fail-closed read rejection from stale or quorum-lost replicas is an allowed outcome; stale
  successful read is not

### Required Checkers

The Jepsen analysis should include at least:

- a linearizability checker over successful writes and successful reads
- an `operation_id` uniqueness checker that rejects duplicate committed execution
- a resource-safety checker that rejects double allocation
- a strict-read fence checker for successful `required_lsn` reads
- an expiration-safety checker that rejects early reuse

### Release Gate

Jepsen is not a substitute for simulation. The deterministic replicated-simulation gate must pass
before Jepsen begins.

The minimum release gate for the first replicated version is:

- one control run for each workload family with no nemesis
- one crash-restart run for each workload family
- one partition-heal run for each workload family
- one mixed failover run for the ambiguity, failover, and expiration workloads
- every faulted run lasts at least `30` minutes after the first injected fault
- every run archives Jepsen history, client logs, replica logs, and a cluster-timeline summary

Any of these outcomes blocks release:

- linearizability violation
- duplicate committed execution for one `operation_id`
- double allocation
- stale successful read from a non-primary or under-applied replica
- early resource reuse after expiration
- unresolved ambiguous client outcome after retry cleanup

See [replication.md](./replication.md) for the protocol draft and the replicated-simulation
section above for the deterministic pre-Jepsen gate.

## Research Influence

This testing strategy is informed by:

- TigerBeetle's safety and simulation emphasis
- FoundationDB's deterministic simulation approach
- Dropbox Nucleus' single-control-thread and random fault-testing work
- Jepsen's analysis of real client-visible ambiguity and fault handling

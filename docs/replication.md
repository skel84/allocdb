# AllocDB Replication Protocol

## Status

This document chooses the protocol family, states the safety invariants replication must
preserve, and narrows the replicated release. The replicated simulation plan and Jepsen gate
now live in [testing.md](./testing.md).

## Scope

The first replicated design target is intentionally narrow:

- one shard
- one fixed membership replica group
- one primary at a time
- one replicated log order per shard
- one deterministic allocator executor per replica

Sharding, reconfiguration, follower reads, and flexible quorum rules are deferred.

## Design Goal

Replication exists to add availability and failover without rewriting the single-node allocator
semantics already fixed in `M0` through `M5`.

The replication layer is allowed to:

- replicate the log
- choose a primary
- recover and rejoin replicas
- delay visibility during failover

It is not allowed to:

- change command meanings
- change result-code meanings
- invent a second execution path that bypasses the trusted-core state machine
- make resources reusable earlier than the single-node rules permit

## Single-Node Semantics That Must Stay Fixed

Replication must preserve these rules exactly:

- command application stays deterministic
- reservation IDs stay derived from committed log position
- `operation_id` retries remain the way clients resolve indefinite outcomes
- strict reads stay defined as "up to a specified applied LSN"
- TTL stays logical-slot based
- expiration may free a resource late, but never early
- bounded retention and bounded retired-history semantics stay part of the product contract

The current authoritative definitions remain:

- [semantics.md](./semantics.md)
- [architecture.md](./architecture.md)
- [fault-model.md](./fault-model.md)
- [storage.md](./storage.md)
- [operator-runbook.md](./operator-runbook.md)

## Chosen Protocol Family

The first replicated AllocDB design is a viewstamped-replication-style primary/backup protocol
with majority quorums.

This draft uses the VSR vocabulary:

- `view`
- `primary`
- `backup`
- `prepare`
- `commit`
- `view change`

Why this is the right fit for AllocDB:

- the current single-node engine already has one explicit sequencer and one explicit apply path
- the core safety boundary is already "one ordered WAL, one executor, one result per LSN"
- the product already treats log position as a first-class identifier because `reservation_id`
  derives from committed `lsn`
- view change and protocol-aware recovery matter more to AllocDB than follower-read ergonomics or
  configuration churn in the first replicated release

This is intentionally not a Flexible Paxos design and not a reconfiguration-heavy Raft variant in
the first replicated version. Majority quorums and fixed membership are the simpler bounded choice.

## Replica Group Shape

The first replicated release assumes:

- fixed odd-sized replica groups
- majority quorum for normal replication
- majority quorum for view change
- no witness nodes
- no learner-only replicas in the core protocol
- no online membership change

Recommended first deployment sizes:

- `3` replicas for the minimum fault-tolerant production shape
- `5` replicas only when the higher write quorum latency is acceptable

## Replica Roles

Each replica is always in one of these protocol states:

- `primary`: accepts writes for the current view
- `backup`: durably appends and later applies committed log entries
- `recovering`: rebuilding local state from validated durable state plus cluster catch-up
- `faulted`: excluded from voting and catch-up because local durable state failed validation

Only the primary may:

- assign new log positions
- publish committed write results to clients
- synthesize internal `expire` commands
- advance the replicated commit point

Backups never mutate allocator state from local timers, local wall clock, or speculative client
execution.

## Persistent Replica State

Every replica must durably persist at least:

- `replica_id`
- `shard_id`
- `current_view`
- durable log entries keyed by `lsn`
- `commit_lsn`
- local snapshot anchor metadata
- enough election/view-change state to guarantee at most one durable vote per view

The exact persisted election record may look like `voted_for`, `last_normal_view`, or an
equivalent durable view-participation marker. The implementation detail can vary; the safety
obligation cannot.

The first `M7-T01` implementation persists this wrapper state in one dedicated local metadata file
alongside the replica workspace. The current persisted fields are:

- `replica_id`
- `shard_id`
- `current_view`
- `role`
- `commit_lsn`
- `active_snapshot_lsn`
- `last_normal_view`
- optional durable vote record `(view, voted_for)`

Replica startup validates identity, vote/view ordering, commit-versus-snapshot consistency, and
that the local applied LSN plus snapshot anchor exactly match this metadata before a replica can
join. Decode failure, metadata inconsistency, unreadable or permission-denied sidecars, applied
state that is ahead of or behind the persisted `commit_lsn`, or an explicitly persisted `faulted`
role leaves the replica in `faulted` state until repaired.

## Replicated Log Contents

The replicated log contains allocator-relevant entries only:

- client commands
- internal `expire` commands

Each replicated client entry carries:

- `view`
- `lsn`
- `operation_id`
- `client_id`
- `request_slot`
- encoded command payload

Each replicated internal expiration entry carries:

- `view`
- `lsn`
- `reservation_id`
- `deadline_slot`
- `request_slot`

Local checkpoint markers, snapshot rewrite metadata, and other storage housekeeping remain local
durability details. They must not become separate replicated state-machine commands.

## Normal Write Path

The normal replicated write path is:

```text
client
  -> current primary
  -> deterministic ingress validation
  -> assign next lsn in current view
  -> append locally
  -> send prepare(view, lsn, prev_lsn, commit_lsn, entry) to backups
  -> majority durable append
  -> mark committed
  -> apply through allocator executor
  -> reply to client
```

Rules:

- the primary must not publish a committed result before a majority has durably appended the entry
- backups must not apply an entry before it is known committed
- committed entries are applied in `lsn` order only
- live execution and replay still share the same allocator apply logic
- internal `expire` commands use the same replicated path as client commands

This keeps the single-node rule intact: one committed command, one log position, one replay result.

## Read Path

The first replicated release serves reads only from the current primary.

Follower reads are deliberately out of scope because they would force extra semantics around leases,
read-index confirmation, or stale-read modes before the basic protocol has been validated.

Read rules:

- the primary serves a strict-read only from locally applied committed state
- the `required_lsn` fence keeps the same meaning as in single-node mode
- a replica in `backup`, `recovering`, `faulted`, or view-uncertain state does not serve API
  reads
- during view change or quorum ambiguity, reads fail closed instead of guessing

## View Change And Failover

AllocDB needs explicit failover rules because client-visible ambiguity is already part of the
single-node design.

The protocol rules are:

- there is at most one primary in normal mode for any given view
- a replica that observes a higher view immediately stops acting as primary in the older view
- a primary that loses quorum stops accepting writes and stops serving reads
- a new primary must gather enough state from a majority to reconstruct the latest safe log prefix
  before entering normal mode
- committed entries survive the view change unchanged
- uncommitted suffix entries from the old primary may be discarded

The current replicated prototype realizes those rules with one explicit `view_uncertain` role and
durable higher-view vote metadata:

- a replica that loses quorum or votes for a higher view leaves normal mode and stops serving reads
  or accepting fresh client writes
- the new primary records one durable vote from a reachable majority before it re-enters normal
  mode
- view change reconstructs the latest committed prefix on the new primary, discards stale
  uncommitted suffix state, and drops old-view protocol messages instead of trying to finish them

Client impact:

- if the old primary fails before replying, the client still has an indefinite outcome
- the client resolves that ambiguity by retrying the same `operation_id`
- the new primary must return the already committed result if the command committed in an earlier
  view
- the protocol must never create a second fresh execution for the same committed `operation_id`

## Recovery And Rejoin

Replication does not weaken the existing local durability rules.

A restarting replica first validates its own local durable state using the same fail-closed rules
already required in single-node mode:

- malformed snapshot input is rejected
- invalid WAL framing or checksum failure is rejected or truncated only at valid tail boundaries
- semantically invalid recovered allocator state is rejected

After local validation:

- a valid but stale replica catches up from the primary by log suffix or snapshot-plus-suffix
- a replica with divergent uncommitted suffix may truncate that suffix during catch-up
- a replica must not discard committed history unless a validated snapshot replaces the same
  committed prefix
- a replica with irreconcilable corruption enters `faulted` state and must not vote or serve until
  repaired

Protocol-aware recovery rule:

- the primary may use knowledge of committed `lsn` and snapshot anchor to decide whether suffix
  catch-up is sufficient or snapshot transfer is required
- suffix-only catch-up is allowed only when the stale replica already retains one committed durable
  prefix recent enough for the primary's current retained WAL window
- when the target is older than that retained-WAL floor, the primary must transfer one validated
  snapshot plus its retained WAL suffix instead of assuming the missing prefix still exists locally
- rejoin clears any prepared-but-uncommitted suffix before the replica returns to backup mode, and
  it also drops stale protocol messages that still reference the rejoined replica's old state
- rejoin must not move one replica backward in durable view knowledge; a target that has already
  observed or voted in a higher view than the current primary stays out of service until a
  compatible higher-view recovery path is available
- a replica already in `faulted` state is not auto-repaired by rejoin; it stays out of service
  until an operator repairs or replaces it
- recovery must preserve the same committed prefix seen by healthy replicas

## Expiration And Logical Time Under Replication

Replication must preserve the single-node TTL rule: late reuse is acceptable; early reuse is not.

That means:

- only the primary may inspect external logical time and decide which reservations are due
- the primary logs internal `expire` commands as ordinary replicated entries
- backups never expire reservations directly from local time
- a view change may delay expiration work, but it must not allow premature reuse
- a newly elected primary must inspect overdue reservations after it is in normal mode and append
  any needed `expire` commands through the replicated log

Logical time therefore remains an input to the primary-controlled scheduler, not a follower-side
state-machine dependency.

## Safety Invariants

Replication must satisfy all single-node invariants plus these protocol invariants:

1. At most one log entry can become committed at a given `(view, lsn)` position.
2. A committed `lsn` never changes payload across views.
3. Any two majorities intersect, so two different primaries cannot both commit divergent entries at
   the same `lsn`.
4. Every replica that applies the same committed log prefix reaches the same allocator state and
   command outcomes.
5. `reservation_id = (shard_id << 64) | lsn` remains valid because committed `lsn` order is global
   within a shard.
6. A client-visible write result is published only after the corresponding log entry is committed.
7. Retrying the same `operation_id` after failover returns the original committed result or an
   indefinite outcome; it never creates a silent second execution.
8. Reads are served only from replicas that know they are in the current view and have locally
   applied at least the requested `lsn`.
9. Expiration remains log-driven. No replica may free a resource from local wall-clock observation
   alone.
10. A replica with unvalidated or corrupted local durable state must not vote, lead, or serve until
    repaired.

## What The First Replicated Release Deliberately Does Not Do

This draft intentionally leaves these areas out of the first replicated implementation:

- follower reads
- lease-read optimization
- flexible or asymmetric quorums
- online membership change
- cross-shard transactions or cross-shard reservations
- leaderless write paths
- background speculative execution on backups

Those may be revisited only if the simpler majority-primary design proves insufficient.

## Follow-On Work

This draft now pairs with the replicated simulation plan and Jepsen gate in
[testing.md](./testing.md). The `M6` replication design gate is complete; further work should
reopen as tracked implementation tasks rather than silent protocol drift.

## Research Anchors

This draft is shaped primarily by:

- [Viewstamped Replication Revisited](https://www.cs.princeton.edu/courses/archive/fall19/cos418/papers/vr-revisited.pdf)
- [Flexible Paxos](https://arxiv.org/abs/1608.06696)
- [Protocol-Aware Recovery for Consensus-Based Storage](https://pages.cs.wisc.edu/~aws/papers/fast18.pdf)
- [Can Applications Recover from fsync Failures?](https://www.usenix.org/conference/atc20/presentation/rebello)

The rule is still the same as elsewhere in the repository: research informs the design, but
boundedness, determinism, and the existing single-node semantics stay authoritative.

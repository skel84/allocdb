# AllocDB Replication Notes

## Status

Deferred. This document records the distributed-system topics that AllocDB expects to design later,
without pretending those details are already settled.

The single-node allocator semantics are the prerequisite. Replication is allowed to extend them,
not rewrite them.

## Why This Exists

The project now references replication-related research and testing work in several places.

This file exists to make those deferred areas visible and indexed:

- view change and failover
- quorum design
- replication recovery
- partitions and rejoin
- Jepsen validation for replicated releases

## What Is Already Fixed Before Replication

Replication must preserve these v1 properties:

- deterministic command application
- WAL-driven replay
- bounded submission and retention rules
- logical-slot TTL semantics
- explicit indefinite-outcome handling via `operation_id`
- no early resource reuse

Those rules are already specified in:

- [semantics.md](./semantics.md)
- [architecture.md](./architecture.md)
- [fault-model.md](./fault-model.md)
- [storage.md](./storage.md)

## Main Distributed Design Areas

### Replicated Log Protocol

Open questions:

- whether the protocol is closest to VSR, Raft-like structure, or a more custom design
- how leaders are chosen and replaced
- what state is persisted for elections and recovery

Research inputs:

- [Viewstamped Replication Revisited](https://www.cs.princeton.edu/courses/archive/fall19/cos418/papers/vr-revisited.pdf)

### Quorum Design

Open questions:

- quorum sizes for normal operation
- quorum sizes for leader change
- whether asymmetric quorum rules are worth the complexity

Research inputs:

- [Flexible Paxos](https://arxiv.org/abs/1608.06696)

### Replication Recovery

Open questions:

- how a replica verifies local storage before rejoining
- what recovery path exists after checksum mismatch or partial local corruption
- how much protocol knowledge is used to recover versus forcing snapshot transfer

Research inputs:

- [Protocol-Aware Recovery for Consensus-Based Storage](https://pages.cs.wisc.edu/~aws/papers/fast18.pdf)
- [Can Applications Recover from fsync Failures?](https://www.usenix.org/conference/atc20/presentation/rebello)

### Partition and Rejoin Semantics

Open questions:

- client behavior during leader ambiguity
- behavior under minority partition
- replica catch-up and rejoin rules
- whether read semantics during failover are supported at all in early replicated versions

### External Validation

Before any replicated release, the system should have:

- deterministic simulation of replicated execution
- fault injection for partitions, crashes, and storage faults
- Jepsen-style external validation

The Jepsen gate should check:

- linearizable command behavior
- indefinite client outcomes and retry semantics
- failover behavior
- replica recovery and rejoin

## Boundaries

This document deliberately does not define:

- final quorum numbers
- a leader election algorithm
- read semantics for followers
- reconfiguration protocol
- cross-shard coordination

If one of those becomes necessary to answer a single-node design question, that is a sign the
single-node design is still too coupled to future replication.

## Next Trigger

This document should be expanded only after:

1. single-node semantics are stable
2. recovery behavior is tested under crash and corruption
3. deterministic simulation exists for the trusted core

At that point, `replication.md` can split into:

- protocol
- recovery
- failover
- validation

# AllocDB Fault Model

## Scope

This document defines the faults the system is designed to tolerate, the faults it detects and halts on,
and the faults intentionally deferred until replication.

## Principle

AllocDB prefers fail-closed behavior over ambiguous continuation.

If the system detects broken invariants or storage corruption, it should halt rather than continue
serving potentially incorrect allocation decisions.

## Fault Classes

### Process Faults

The system is designed to tolerate:

- process crash
- process restart
- abrupt termination during WAL append
- temporary pause or scheduler delay

Expected behavior:

- crash recovery replays snapshot plus WAL
- pauses may delay expiration, but must never cause early reuse

### Clock and Time Faults

The trusted core does not consult wall-clock time.

Rules:

- TTL is represented in logical slots
- slot delay may make a reservation expire late
- no clock jump may make a reservation reusable early

This makes clock skew a liveness issue, not a correctness issue, for v1.

### Storage Faults

The system explicitly considers:

- torn WAL tail
- checksum mismatch in WAL frame
- invalid or corrupted snapshot
- fsync failure
- missing latest WAL segment after crash

Required behavior:

- detect invalid WAL frames by checksum and framing
- truncate recovery at the last valid WAL boundary
- reject corrupted snapshot state rather than trusting it blindly
- fail closed on storage corruption that cannot be reconciled from a known-good snapshot plus WAL

The system must not continue after discovering corrupted allocator state.

### Capacity and Overload Faults

The system assumes overload happens and treats it as a normal operating condition.

Required behavior:

- bounded queues and tables fail fast
- capacity exhaustion returns deterministic errors
- expiration lag is visible
- overload must not silently turn into unbounded memory growth

### Transport Faults

Even in single-node mode, clients can experience:

- timeout
- disconnect
- reply loss after commit

These produce indefinite outcomes from the client perspective.

Required behavior:

- clients retry with the same `operation_id`
- the server returns the original result if the command already committed within retention
- the server never executes the same `operation_id` twice

## Faults Deferred Until Replication

These matter later, but should not distort the core:

- primary crash
- follower lag
- quorum loss
- network partition
- rejoin after divergence
- reconfiguration

Those belong in `replication.md` after the single-node semantics are fixed.

See [replication.md](./replication.md) for the deferred replication topics and boundaries.

## Design Consequences

The fault model implies:

- WAL is the source of truth
- replay and live execution share the same apply logic
- corruption handling is explicit, not "best effort"
- transport ambiguity is handled by idempotent submission, not by pretending ambiguity cannot occur

# Lease Kernel Design Decisions

## Status

Draft. This document freezes the current local design direction for `M9` so follow-on semantics
work can proceed from explicit choices instead of open-ended scope language.

It is still a planning document. It does not yet change the authoritative API contract in
[semantics.md](./semantics.md) or [api.md](./api.md).

## Purpose

The follow-on scope in [lease-kernel-follow-on.md](./lease-kernel-follow-on.md) identifies the
generic ownership primitives AllocDB should add next:

- atomic multi-resource ownership
- stale-holder fencing
- explicit revoke
- an explicit external liveness boundary

This document makes the first concrete design choices for those areas.

## Decision Summary

The current local design direction is:

1. introduce a first-class `lease_id` as the authority object for ownership
2. represent bundle ownership as one lease plus bounded member records, not as independent
   peer-level reservations
3. treat the current single-resource reservation model as the semantic special case of bundle size
   `1`
4. add a lease-scoped fencing token `lease_epoch` and require it on holder-authorized mutations
5. model revoke as a two-stage flow: `active -> revoking -> revoked`
6. keep heartbeat, pod, node, or other liveness observation outside the trusted core; the kernel
   consumes only explicit commands

These choices are meant to preserve AllocDB's existing strengths:

- one ordered log
- one authoritative ownership object per committed claim
- deterministic replay
- bounded history
- late reuse is acceptable, early reuse is not

## Decision 1: Introduce A First-Class Lease Object

### Choice

`M9` should introduce a first-class `lease_id`.

The new ownership authority should not be modeled as a loose grouping of existing reservation
records.

### Why

Bundle ownership, fencing, revoke, and retention all want one authoritative object:

- one identifier for the whole committed claim
- one holder identity
- one lifecycle
- one fencing token
- one retained-history record

Trying to express those semantics as only peer-level reservation records would force the system to
reconstruct one logical authority from several records after replay and failover. That is the wrong
direction for a deterministic kernel.

### Identifier Strategy

The starting design should preserve the existing log-derived identity model:

```text
lease_id = (shard_id << 64) | lsn
```

That keeps one important property unchanged:

- one committed ownership decision gets one durable log position and one stable identifier

## Decision 2: Represent Bundles As One Lease Plus Member Records

### Choice

A bundle should be represented as:

- one lease record
- one bounded list or table of lease-member records
- per-resource pointers to the current owning `lease_id`

### Why

This keeps the authority model simple:

- the lease record answers who owns the claim
- the member records answer which resources are in the claim
- each resource still points at at most one live owner

### Resource Rule

The existing invariant remains unchanged at the resource level:

- a resource has at most one active owner at a time

Bundle ownership does not weaken that invariant. It only lets one committed ownership object cover
several resources atomically.

### Atomic Visibility Rule

Bundle visibility must be all-or-nothing.

On success:

- all member resources point to the new `lease_id`

On failure:

- no member resource points to the new `lease_id`
- no partial lease remains live

### Single-Resource Compatibility

The single-resource path should become the semantic special case of bundle size `1`.

That does not require immediate removal of specialized single-resource commands or fast paths.

The rule is:

- one-resource ownership and multi-resource ownership must share one invariant model

Compatibility strategy:

- preserve the current single-resource surface while the authoritative semantics are generalized
- only remove or rename compatibility commands after the generalized model is proven

## Decision 3: Use A Lease-Scoped Fencing Token

### Choice

Each live lease should carry one monotonic fencing token:

```text
lease_epoch : u64
```

The holder-authority token is the pair:

```text
(lease_id, lease_epoch)
```

### Why

The token exists to answer one question:

is this actor still the authority for this lease, or is it acting on stale knowledge?

That is a generic ownership problem. It shows up anywhere an external actor may continue acting
after timeout, failover, retry ambiguity, or explicit revoke.

### Initial Value

The starting rule should be:

- a newly committed live lease begins with `lease_epoch = 1`

### When It Must Change

`lease_epoch` must increase on any transition that invalidates the previous holder's authority.

For the first follow-on, that means at least:

- `revoke`
- terminal release or expiry transitions that end holder authority

If later work adds holder transfer, rebind, or reissue semantics, those transitions must also
increment `lease_epoch`.

### Command Rule

Any holder-authorized mutation must carry the current `(lease_id, lease_epoch)` pair.

For the first follow-on, that means at least:

- activate or confirm-equivalent transition into live use
- holder-initiated release

Control-plane or operator-authorized commands such as `revoke` or `reclaim` should not require the
holder token.

### Read Rule

The eventual lease read surface must expose `lease_epoch`.

That is the minimum information downstream systems need to reject stale actors deterministically.

## Decision 4: Revoke Is A Two-Stage Flow

### Choice

Revoke should not free resources immediately.

The revoke flow should be:

```text
active -> revoking -> revoked
```

### Why

Immediate reuse after revoke would be unsafe whenever the old holder may still be acting outside
AllocDB.

AllocDB's safety rule already prefers late reuse over early reuse. Revoke has to preserve that.

### Revoke

`revoke(lease_id)` should:

- move the lease from `active` to `revoking`
- increment `lease_epoch`
- invalidate further holder-authorized commands that present the old token
- keep the resources unavailable for reuse

### Reclaim

A separate explicit command should return the resources to reuse:

```text
reclaim(lease_id)
```

`reclaim` should:

- move the lease from `revoking` to terminal `revoked`
- clear the member resources back to `available`
- preserve retained history as a revoked outcome

This is the core liveness-boundary decision:

- the trusted core does not decide from wall-clock or heartbeat signals when reclaim is safe
- an external observer decides when it has enough evidence to reclaim, then submits an explicit
  command

## Decision 5: Use A Minimal Lease Lifecycle

### Choice

The current starting lifecycle should be:

```text
reserved
active
revoking
released
expired
revoked
```

### Meaning

- `reserved`: ownership committed but not yet activated for holder use
- `active`: holder authority is live
- `revoking`: holder authority has been withdrawn, but resources are not yet reusable
- `released`: holder ended the lease normally and resources are reusable
- `expired`: a time-bounded pre-active lease aged out through the explicit expiration path
- `revoked`: reclaim completed after revoke and resources are reusable

### Why This State Set

This is the smallest lifecycle that makes the new safety boundaries explicit:

- `active` replaces the old conceptual role of `confirmed`
- `revoking` exists because revoke and reuse cannot be collapsed safely
- separate terminal outcomes preserve history and audit meaning

The lifecycle deliberately does not include policy states such as scheduling, queue, warm pool,
placement, or serving state.

## Decision 6: Keep Liveness Observation Outside The Core

### Choice

The trusted core should not ingest heartbeats, pod state, node state, or wall-clock-derived lease
timeouts directly.

### Why

That would blur the existing boundary that keeps time observation and external orchestration outside
the deterministic state machine.

The correct split is:

- external systems observe liveness
- external systems decide when to request revoke or reclaim
- AllocDB records the resulting ownership transition durably and deterministically

### Consequence

`expire` remains a bounded explicit path for time-bounded reservations.

Long-running live leases should use explicit revoke and reclaim, not implicit timer-driven reuse
inside the kernel.

## Resulting Data-Model Direction

The current local direction is:

### Lease

```text
lease_id           : u128
holder_id          : u128
state              : enum { reserved, active, revoking, released, expired, revoked }
lease_epoch        : u64
created_lsn        : u64
deadline_slot      : u64 | 0
released_lsn       : u64 | 0
retire_after_slot  : u64 | 0
member_count       : u32
```

### Lease Member

```text
lease_id           : u128
resource_id        : u128
member_index       : u32
```

### Resource

The resource record should eventually point at:

```text
current_lease_id   : u128 | 0
current_state      : enum { available, reserved, active, revoking }
```

The exact field migration from today's reservation-centric naming can happen later. The important
decision now is the authority model, not the final struct spelling.

## History Direction

History should become lease-centric.

Rules:

- retained-history lookup should anchor on `lease_id`
- member records retire with their parent lease
- first follow-on reads do not need a public per-member historical identifier

This keeps bundle history bounded and avoids inventing a second public identity layer for bundle
members.

## Deferred Items

These decisions are still intentionally deferred:

- exact wire encoding changes
- whether the public command verb stays `confirm` or is renamed to `activate`
- detailed capacity bounds for maximum bundle size and member table growth
- shared-resource or fractional-resource semantics
- holder transfer between different holders
- policy reasons, tenant metadata, or topology metadata inside the trusted core

Those should be handled by the next doc pass that updates the authoritative semantics and API
documents.

## Required Next Doc Pass

If this design direction is accepted, the next local planning step should update:

- [semantics.md](./semantics.md)
- [api.md](./api.md)
- [architecture.md](./architecture.md)
- [fault-model.md](./fault-model.md)

That pass should turn these design choices into one coherent command, state, and result-code
surface.

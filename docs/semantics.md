# AllocDB Semantics and API

## Scope

This document defines the approved follow-on data model, command semantics, consistency model, and
product-level API rules for AllocDB's generic lease kernel.

For the transport-neutral request and response surface, see [api.md](./api.md).

The current implementation still exposes a reservation-centric alpha compatibility path. That path
is expected to converge toward the lease-centric model defined here as `M9` lands.

Related planning docs:

- [lease-kernel-follow-on.md](./lease-kernel-follow-on.md)
- [lease-kernel-design.md](./lease-kernel-design.md)

## Core Concepts

### Resource

A resource is the allocatable object identified by `resource_id`.

Examples:

- `seat_21A`
- `gpu_node_14`
- `hotel_room_204`
- `inventory_unit_8932`

In the trusted core, a resource has only allocatable state. Arbitrary metadata is out of scope.

Suggested record:

```text
resource_id              : u128
current_state            : enum { available, reserved, active, revoking }
current_lease_id         : u128 | 0
version                  : u64
```

`version` increments on every resource state transition. It is a read-side observation field, not
a write precondition.

### Lease

A lease is the authoritative ownership object for one scarce-resource claim by one holder.

A lease may cover:

- one resource
- or one all-or-nothing bundle of multiple resources

Suggested record:

```text
lease_id                 : u128
holder_id                : u128
state                    : enum { reserved, active, revoking, released, expired, revoked }
lease_epoch              : u64
created_lsn              : u64
deadline_slot            : u64 | 0
released_lsn             : u64 | 0
retire_after_slot        : u64 | 0
member_count             : u32
```

Rules:

- lease state is separate from resource state
- `released`, `expired`, and `revoked` are terminal lease-history states, not live resource states
- `revoking` withdraws holder authority but does not yet make the resources reusable
- terminal lease history is retained only until `retire_after_slot`

### Lease Member

A lease member ties one `resource_id` to one parent `lease_id`.

Suggested record:

```text
lease_id                 : u128
resource_id              : u128
member_index             : u32
```

Rules:

- member records exist only to describe the resource set owned by one lease
- a member has no independent holder, lifecycle, or public identity
- member history retires with the parent lease

### Operation

Every client-visible write carries an `operation_id`.

Suggested record:

```text
operation_id             : u128
command_hash             : u128
result_code              : enum
result_lease_id          : u128 | 0
result_deadline_slot     : u64 | 0
applied_lsn              : u64
retire_after_slot        : u64
```

The operation table provides bounded idempotency, not permanent dedupe.

The trusted core stores operation outcomes so duplicate `operation_id` retries can return the
original result while the dedupe window is still open.

## Identifier Strategy

### Resource IDs

`resource_id` is externally supplied and stable.

### Lease IDs

`lease_id` is derived from committed log position:

```text
lease_id = (shard_id << 64) | lsn
```

For the single-node deployment, `shard_id` is always `0`.

### Operation IDs

`operation_id` is client supplied and must be unique within the configured dedupe window.

## Hard Invariants

1. A resource has at most one active lease authority at a time.
2. A successful bundle commit becomes visible atomically or not at all.
3. Every active lease member belongs to exactly one active lease.
4. `active` and `revoking` leases cannot be expired.
5. A terminal lease never becomes active again.
6. A successful reserve command creates exactly one lease object.
7. Every holder-authorized mutation is checked against the current `(lease_id, lease_epoch)`.
8. Revoke may withdraw authority immediately, but reuse may happen only after explicit safe
   reclaim.
9. Every committed command has exactly one log position and one replay result.
10. Replaying the same snapshot plus WAL yields the same state and the same command outcomes.
11. Expiration or reclaim can make a resource reusable late, but never early.
12. Reads that claim strict consistency must observe a specific applied LSN.
13. Bounded retention rules must be sufficient to preserve the advertised API semantics.

## Logical Time

TTL is represented in logical slots, not wall-clock timestamps, inside the trusted core.

External APIs may accept duration-style input, but the WAL and state machine operate on:

- `ttl_slots`
- `deadline_slot`

TTL policy:

- product-level maximum reserve TTL is `1` hour
- deployments may configure a lower maximum
- TTL applies to the `reserved` pre-activation state
- longer-lived ownership should use `active` leases plus explicit revoke and reclaim, not
  ever-longer timer-driven reuse inside the state machine

## Command Model

All client-visible state-changing commands use the same outer envelope.

### Client Command Envelope

```text
operation_id   : u128
client_id      : u128
request_slot   : u64
command_kind   : enum
payload         ...
```

The command signatures below show payload fields only.

## Result Codes

The approved follow-on result-code set is:

- `ok`
- `noop`
- `already_exists`
- `resource_table_full`
- `resource_not_found`
- `resource_busy`
- `bundle_too_large`
- `ttl_out_of_range`
- `lease_table_full`
- `lease_member_table_full`
- `lease_not_found`
- `lease_retired`
- `expiration_index_full`
- `operation_table_full`
- `operation_conflict`
- `invalid_state`
- `holder_mismatch`
- `stale_epoch`
- `slot_overflow`

`operation_table_full` is a pre-commit failure: the allocator cannot accept a new deduped client
command because it has no bounded space to remember the outcome.

`slot_overflow` is the trusted-core guard result when a derived deadline or retirement slot would
exceed `u64::MAX`. The single-node engine rejects the same condition before WAL commit as a
definite submission failure.

## Commands

### Create Resource

```text
create_resource(resource_id)
```

Success effect:

- inserts a new resource in `available`

Failure cases:

- `already_exists`
- `resource_table_full`

### Reserve Bundle

```text
reserve_bundle(resource_ids[], holder_id, ttl_slots)
```

Preconditions:

- `resource_ids` is non-empty
- `resource_ids.len()` is within the configured maximum bundle size
- every `resource_id` is unique within the command
- every resource exists
- every resource state is `available`
- `ttl_slots` is within configured bounds

Success effect:

- derive `lease_id` from the committed `lsn`
- compute `deadline_slot = request_slot + ttl_slots`
- create a lease with `state = reserved` and `lease_epoch = 1`
- create one bounded member record for each resource
- attach all member resources to the new lease atomically

Failure cases:

- `resource_not_found`
- `resource_busy`
- `bundle_too_large`
- `ttl_out_of_range`
- `lease_table_full`
- `lease_member_table_full`
- `expiration_index_full`

Compatibility rule:

- the current single-resource `reserve(resource_id, holder_id, ttl_slots)` command is the
  compatibility form of `reserve_bundle([resource_id], holder_id, ttl_slots)`

### Activate

```text
activate(lease_id, holder_id, lease_epoch)
```

Preconditions:

- lease exists
- lease state is `reserved`
- `holder_id` matches
- `lease_epoch` matches the current live lease record

Success effect:

- lease state becomes `active`
- every member resource state becomes `active`

Failure cases:

- `lease_not_found`
- `lease_retired`
- `invalid_state`
- `holder_mismatch`
- `stale_epoch`

Compatibility rule:

- the current single-resource `confirm(reservation_id, holder_id)` command is a compatibility form
  of `activate(lease_id, holder_id, lease_epoch)` for a bundle of size `1`

### Release

```text
release(lease_id, holder_id, lease_epoch)
```

Preconditions:

- lease exists
- lease state is `reserved` or `active`
- `holder_id` matches
- `lease_epoch` matches the current live lease record

Success effect:

- lease state becomes `released`
- `lease_epoch` increments to invalidate further holder authority
- all member resources return to `available`

Failure cases:

- `lease_not_found`
- `lease_retired`
- `invalid_state`
- `holder_mismatch`
- `stale_epoch`

Compatibility rule:

- the current single-resource `release(reservation_id, holder_id)` command is a compatibility form
  of `release(lease_id, holder_id, lease_epoch)` for a bundle of size `1`

### Revoke

```text
revoke(lease_id)
```

Preconditions:

- lease exists
- lease state is `active`

Success effect:

- lease state becomes `revoking`
- `lease_epoch` increments
- member resources remain unavailable for reuse

Failure cases:

- `lease_not_found`
- `lease_retired`
- `invalid_state`

### Reclaim

```text
reclaim(lease_id)
```

Preconditions:

- lease exists
- lease state is `revoking`

Success effect:

- lease state becomes `revoked`
- all member resources return to `available`

Failure cases:

- `lease_not_found`
- `lease_retired`
- `invalid_state`

### Expire

```text
expire(lease_id, deadline_slot)
```

This is an internal command written through the same WAL path as external commands.

Preconditions:

- lease exists
- lease state is `reserved`
- `deadline_slot` matches the lease record

Success effect:

- lease state becomes `expired`
- `lease_epoch` increments to invalidate any future stale holder use
- all member resources return to `available`

Deterministic no-op cases:

- lease already `active`
- lease already `revoking`
- lease already `released`
- lease already `expired`
- lease already `revoked`
- lease does not match the expected deadline

### Query State

```text
get_resource(resource_id)
get_lease(lease_id)
```

Rules:

- strict reads are tied to an applied LSN
- active and revoking leases are always queryable
- terminal leases remain queryable until `retire_after_slot`
- after the live record retires, reads return `lease_retired` via bounded retained metadata
- that retained metadata is conservative: once full history is dropped, older shard-local
  `lease_id` values at or below the retired watermark also read as `lease_retired`

Compatibility rule:

- the current `get_reservation(reservation_id)` read is superseded by `get_lease(lease_id)` and
  remains only as a compatibility surface during the implementation transition

## Compatibility Surface

The approved lease model supersedes the older reservation-centric naming.

During the implementation transition:

- `reserve(resource_id, holder_id, ttl_slots)` remains as the compatibility form of one-member
  `reserve_bundle`
- `confirm(reservation_id, holder_id)` remains as the compatibility form of one-member `activate`
- `release(reservation_id, holder_id)` remains as the compatibility form of one-member `release`
- `get_reservation(reservation_id)` remains as the compatibility form of `get_lease(lease_id)`

The semantic rule is that these compatibility commands must not diverge from the lease model.

## Consistency and Idempotency

### Core Execution

The current version targets strict serializable behavior on a single shard with one executor.

All state changes are applied in one deterministic order.

### Replay Rule

```text
same snapshot + same WAL -> same state + same command results
```

### Idempotency Rule

"Exactly once" in AllocDB means:

- the same `operation_id` with the same command returns the original result
- the same `operation_id` with different command contents returns `operation_conflict`
- the guarantee holds within a configured retention window `W`

Infinite dedupe is not a goal because it conflicts with bounded storage.

If the operation table itself is full, the allocator returns `operation_table_full` and does not
accept the command into the deduped execution path.

### Submission Outcomes

The transport-level outcome of a write is not always the same as the state-machine outcome.

Clients must distinguish:

- definite success: the command committed and the result is known
- definite failure: the command was rejected before commit and did not take effect
- indefinite outcome: the client cannot tell whether the command committed

Indefinite outcomes are expected under timeout, disconnect, process crash, or reply loss.

The rule is:

- clients resolve indefinite outcomes by retrying the same `operation_id` within the dedupe window
- the server returns the original result if that operation already committed
- the server never invents a fresh second execution for the same `operation_id`
- the server does not promise to resolve ambiguity after dedupe retention has expired

Current single-node engine rule:

- malformed request, payload-too-large, overload, and slot-overflow errors are definite
  pre-commit failures
- `lsn_exhausted` is a definite write rejection once the engine has committed `u64::MAX` and no
  further LSN can be assigned
- WAL write failure halts the live engine and is an indefinite submission failure
- after a WAL-path failure, the live engine refuses further writes with `engine_halted`
- clients resolve that ambiguity by recovering the node, then retrying the same `operation_id`
- if the failed attempt reached durable WAL, the retry returns the original stored result
- if the failed attempt did not reach durable WAL, the retry executes once as a fresh command
- `engine_halted` remains an indefinite submission failure because the halted node refuses to claim
  whether a prior ambiguous write committed
- while the engine is halted, live reads also fail closed because in-memory state may lag durable
  WAL until recovery rebuilds the node
- the retry contract only holds while the dedupe retention window is still open
- the future wire protocol must preserve that distinction explicitly instead of flattening all
  submission errors into one generic failure class

This is the practical meaning of reliable submission. It keeps command handling bounded without
pretending that transport failures do not exist.

### Architectural Decisions

The following rules are fixed:

1. holder-authorized commands carry both `holder_id` and the current `lease_epoch`
2. lease authority is keyed by `lease_id`, not `resource.version`
3. the resource `version` field is observable but is not a write guard
4. lease lookup history is bounded and returns `lease_retired` after retirement
5. resource metadata is excluded from the trusted core
6. indefinite write outcomes are resolved by client retry with the same `operation_id`
7. liveness observation stays outside the trusted core; the core consumes explicit `revoke` and
   `reclaim` commands instead of wall-clock-derived lease timeouts

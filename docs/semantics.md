# AllocDB Semantics and API

## Scope

This document defines the v1 data model, command semantics, consistency model, and product-level
API rules.

For the current transport-neutral alpha request/response surface, see [api.md](./api.md).

## Core Concepts

### Resource

A resource is the allocatable object identified by `resource_id`.

Examples:

- `seat_21A`
- `gpu_node_14`
- `hotel_room_204`
- `inventory_unit_8932`

In the trusted core, a resource has only allocatable state. Arbitrary metadata is out of scope for
v1.

Suggested record:

```text
resource_id              : u128
current_state            : enum { available, reserved, confirmed }
current_reservation_id   : u128 | 0
version                  : u64
```

`version` increments on every resource state transition. In v1 it is a read-side observation field,
not a write precondition.

### Reservation

A reservation is a historical claim on one resource by one holder.

Suggested record:

```text
reservation_id           : u128
resource_id              : u128
holder_id                : u128
state                    : enum { reserved, confirmed, released, expired }
created_lsn              : u64
deadline_slot            : u64
released_lsn             : u64 | 0
retire_after_slot        : u64 | 0
```

Rules:

- reservation state is separate from resource state
- `released` and `expired` are reservation-history states, not live resource states
- terminal reservation history is retained only until `retire_after_slot`

### Operation

Every client-visible write carries an `operation_id`.

Suggested record:

```text
operation_id             : u128
command_hash             : u128
result_code              : enum
result_reservation_id    : u128 | 0
result_deadline_slot     : u64 | 0
applied_lsn              : u64
retire_after_slot        : u64
```

The operation table provides bounded idempotency, not permanent dedupe.

The first implementation stores operation outcomes in the trusted core so duplicate
`operation_id` retries can return the original result while the dedupe window is still open.

## Identifier Strategy

### Resource IDs

`resource_id` is externally supplied and stable.

### Reservation IDs

`reservation_id` is derived from committed log position:

```text
reservation_id = (shard_id << 64) | lsn
```

For the single-node prototype, `shard_id` is always `0`.

### Operation IDs

`operation_id` is client supplied and must be unique within the configured dedupe window.

## Hard Invariants

1. A resource has at most one active reservation at a time.
2. An active reservation belongs to exactly one resource.
3. `confirmed` reservations cannot be expired.
4. A terminal reservation never becomes active again.
5. A successful `reserve` creates exactly one reservation object.
6. Every committed command has exactly one log position and one replay result.
7. Replaying the same snapshot plus WAL yields the same state and the same command outcomes.
8. Expiration can make a resource reusable late, but never early.
9. Reads that claim strict consistency must observe a specific applied LSN.
10. Bounded retention rules must be sufficient to preserve the advertised API semantics.

## Logical Time

TTL is represented in logical slots, not wall-clock timestamps, inside the trusted core.

External APIs may accept duration-style input, but the WAL and state machine operate on:

- `ttl_slots`
- `deadline_slot`

v1 TTL policy:

- product-level maximum reservation TTL is 1 hour
- deployments may configure a lower maximum
- longer-lived ownership should use `confirmed`, not ever-longer reservation TTLs

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

The current v1 result-code set is:

- `ok`
- `noop`
- `already_exists`
- `resource_table_full`
- `resource_not_found`
- `resource_busy`
- `ttl_out_of_range`
- `reservation_table_full`
- `reservation_not_found`
- `reservation_retired`
- `expiration_index_full`
- `operation_table_full`
- `operation_conflict`
- `invalid_state`
- `holder_mismatch`
- `slot_overflow`

`operation_table_full` is a pre-commit failure: the allocator cannot accept a new deduped client
command because it has no bounded space to remember the outcome.

`slot_overflow` is the trusted-core guard result when a derived deadline or retirement slot would
exceed `u64::MAX`. The single-node engine rejects the same condition before WAL commit as a
definite submission failure.

### Create Resource

```text
create_resource(resource_id)
```

Success effect:

- inserts a new resource in `available`

Failure cases:

- `already_exists`
- `resource_table_full`

### Reserve

```text
reserve(resource_id, holder_id, ttl_slots)
```

Preconditions:

- resource exists
- resource state is `available`
- `ttl_slots` is within configured bounds

Success effect:

- derive `reservation_id` from the committed `lsn`
- compute `deadline_slot = request_slot + ttl_slots`
- create reservation with `state = reserved`
- attach the reservation to the resource

Failure cases:

- `resource_not_found`
- `resource_busy`
- `ttl_out_of_range`
- `reservation_table_full`
- `expiration_index_full`

### Confirm

```text
confirm(reservation_id, holder_id)
```

Preconditions:

- reservation exists
- reservation state is `reserved`
- `holder_id` matches

Success effect:

- reservation state becomes `confirmed`
- resource state becomes `confirmed`

Failure cases:

- `reservation_not_found`
- `reservation_retired`
- `invalid_state`
- `holder_mismatch`

v1 intentionally does not add `conditional_confirm(expected_version)`.

Rationale:

- `reservation_id` is derived from committed log position and is never reused
- stale confirms on an old reservation cannot mutate a newer reservation for the same resource
- the stale client sees `invalid_state`, `reservation_not_found`, or `reservation_retired` instead

If the product later needs read-modify-write protection keyed to a resource read version, that can
be added as a separate command without changing the single-node v1 safety model.

### Release

```text
release(reservation_id, holder_id)
```

Preconditions:

- reservation exists
- reservation state is `reserved` or `confirmed`
- `holder_id` matches

Success effect:

- reservation state becomes `released`
- resource returns to `available`

Failure cases:

- `reservation_not_found`
- `reservation_retired`
- `invalid_state`
- `holder_mismatch`

### Expire

```text
expire(reservation_id, deadline_slot)
```

This is an internal command written through the same WAL path as external commands.

Preconditions:

- reservation exists
- reservation state is `reserved`
- `deadline_slot` matches the reservation record

Success effect:

- reservation state becomes `expired`
- resource returns to `available`

Deterministic no-op cases:

- reservation already `confirmed`
- reservation already `released`
- reservation already `expired`
- reservation does not match the expected deadline

### Query State

```text
get_resource(resource_id)
get_reservation(reservation_id)
```

Rules:

- strict reads are tied to an applied LSN
- active reservations are always queryable
- terminal reservations remain queryable until `retire_after_slot`
- after the live record retires, reads return `reservation_retired` via bounded retained metadata
- that retained metadata is conservative: once full history is dropped, older shard-local
  `reservation_id` values at or below the retired watermark also read as `reservation_retired`

## Consistency and Idempotency

### Single-Node v1

The first version targets strict serializable behavior on a single shard with one executor.

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

The v1 rule is:

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

This is the practical meaning of reliable submission in v1. It keeps command handling bounded
without pretending that transport failures do not exist.

### v1 Decisions

The following rules are fixed for v1:

1. `confirm` and `release` require `holder_id`.
2. `confirm` remains keyed by `reservation_id`, not `resource.version`.
3. The resource `version` field is observable but is not a v1 write guard.
4. Reservation lookup history is bounded and returns `reservation_retired` after retirement.
5. Resource metadata is excluded from the trusted core.
6. Indefinite write outcomes are resolved by client retry with the same `operation_id`.

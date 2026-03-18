# AllocDB API

## Scope

This document defines the transport-neutral API exposed by `crates/allocdb-node::api`.

It is intentionally not an HTTP or RPC spec. It first fixes:

- request and response shapes
- binary wire encoding
- definite vs indefinite submission failures
- strict-read behavior for resource and lease queries
- metrics exposure

Network transport, authentication, and multi-node routing are out of scope for this stage.

The approved follow-on lease-centric surface is defined here. The current implementation still
exposes a reservation-centric alpha compatibility path while `M9` is being implemented.

## Current Rust Entry Points

The API is available through:

- `allocdb_node::SingleNodeEngine::handle_api_request`
- `allocdb_node::SingleNodeEngine::handle_api_bytes`
- `allocdb_node::encode_request`
- `allocdb_node::decode_request`
- `allocdb_node::encode_response`
- `allocdb_node::decode_response`

The public request encoder is fallible. It returns `length_too_large` if a variable-length field
cannot fit in the binary wire length prefix.

`handle_api_bytes` is the transport-facing path:

```text
encoded request -> decode -> engine -> encode -> encoded response
```

## Request Types

The approved request set is:

- `submit`
- `get_resource`
- `get_lease`
- `get_metrics`
- `tick_expirations`

Compatibility note:

- the current alpha `get_reservation` request is the compatibility precursor to `get_lease`

### Submit

```text
submit {
  request_slot : u64
  payload      : bytes
}
```

`payload` is the encoded core `ClientRequest` payload:

```text
operation_id : u128
client_id    : u128
command      : ...
```

The API deliberately wraps that payload instead of redefining command encoding a second time.

The approved command set inside `payload` is:

- `create_resource`
- `reserve_bundle`
- `activate`
- `release`
- `revoke`
- `reclaim`

Compatibility note:

- the current single-resource `reserve`, `confirm`, and reservation-centric `release` commands
  remain only as compatibility aliases while the implementation transitions to the lease model

### Get Resource

```text
get_resource {
  required_lsn : u64 | none
  resource_id  : u128
}
```

If `required_lsn` is present, the node enforces the strict-read fence before serving the read.

### Get Lease

```text
get_lease {
  required_lsn : u64 | none
  current_slot : u64
  lease_id     : u128
}
```

`current_slot` is required because retained-history retirement is defined in logical slots.

### Get Metrics

```text
get_metrics {
  current_wall_clock_slot : u64
}
```

The caller provides wall-clock progress so the engine can derive `logical_slot_lag` and
`expiration_backlog` deterministically.

### Tick Expirations

```text
tick_expirations {
  current_wall_clock_slot : u64
}
```

This request drives the bounded expiration scheduler for one maintenance tick. The node inspects
due reserved leases, commits up to `MAX_EXPIRATIONS_PER_TICK` internal `expire` commands through
the WAL, and applies them through the normal executor path.

## Submission Responses

Submission responses are split into two classes:

- `committed`
- `rejected`

### Committed

```text
committed {
  applied_lsn       : u64
  result_code       : enum
  lease_id          : u128 | none
  lease_epoch       : u64 | none
  deadline_slot     : u64 | none
  from_retry_cache  : bool
}
```

This is the committed state-machine result. A committed command may still return domain failures
such as `resource_busy`, `holder_mismatch`, or `stale_epoch`; those are allocator outcomes, not
submission-layer failures.

Compatibility note:

- the current alpha response field `reservation_id` is superseded by `lease_id`

### Rejected

```text
rejected {
  category : enum { definite_failure, indefinite }
  code     : enum
}
```

Current submission failure codes are:

- `invalid_request(buffer_too_short | invalid_command_tag(tag) | invalid_layout)`
- `slot_overflow(kind, request_slot, delta)`
- `command_too_large(encoded_len, max_command_bytes)`
- `lsn_exhausted(last_applied_lsn)`
- `overloaded(queue_depth, queue_capacity)`
- `engine_halted`
- `storage_failure`

The required category mapping is:

- definite failure:
  - `invalid_request`
  - `slot_overflow`
  - `command_too_large`
  - `lsn_exhausted`
  - `overloaded`
- indefinite:
  - `engine_halted`
  - `storage_failure`

This distinction is mandatory. The API must not flatten indefinite write outcomes into ordinary
hard failures.

## Read Responses

### Resource Query

`get_resource` returns one of:

- `found(resource_view)`
- `not_found`
- `engine_halted`
- `fence_not_applied(required_lsn, last_applied_lsn)`

Approved `resource_view` fields are:

```text
resource_id        : u128
state              : enum { available, reserved, active, revoking }
current_lease_id   : u128 | none
version            : u64
```

### Lease Query

`get_lease` returns one of:

- `found(lease_view)`
- `not_found`
- `retired`
- `engine_halted`
- `fence_not_applied(required_lsn, last_applied_lsn)`

Approved `lease_view` fields are:

```text
lease_id             : u128
holder_id            : u128
state                : enum { reserved, active, revoking, released, expired, revoked }
lease_epoch          : u64
created_lsn          : u64
deadline_slot        : u64 | none
released_lsn         : u64 | none
retire_after_slot    : u64 | none
member_resource_ids  : [u128]
```

`retired` is distinct from `not_found` because bounded history is part of the product contract.
The live lease record remains queryable until `retire_after_slot`; after that, the engine may drop
the full record but must keep returning `retired` for shard-local `lease_id` values at or below
its retired watermark. This watermark is conservative: once full history is gone, the API no
longer distinguishes an aged-out lease from an older shard-local `lease_id` below that watermark.

If the live engine has halted after a WAL-path ambiguity, reads must fail closed with
`engine_halted` until recovery rebuilds memory from durable state.

Compatibility note:

- the current alpha `get_reservation` and `reservation_view` are superseded by `get_lease` and
  `lease_view`

## Expiration Tick Responses

`tick_expirations` returns one of:

- `applied(processed_count, last_applied_lsn)`
- `rejected(category, code)`

```text
applied {
  processed_count   : u32
  last_applied_lsn  : u64 | none
}
```

`processed_count` is the number of internal `expire` commands committed in this maintenance tick.
`last_applied_lsn` is absent when no due expiration was processed.

`rejected(category, code)` reuses the same failure envelope as `submit`. The current tick path can
return the same indefinite failures as `submit`, and it can also return definite failures such as
`slot_overflow` if the derived expiration retirement slot would exceed `u64::MAX`.

## Metrics Response

`get_metrics` returns:

```text
metrics {
  queue_depth
  queue_capacity
  accepting_writes
  recovery {
    startup_kind
    loaded_snapshot_lsn
    replayed_wal_frame_count
    replayed_wal_last_lsn
    active_snapshot_lsn
  }
  core {
    last_applied_lsn
    last_request_slot
    logical_slot_lag
    expiration_backlog
    operation_table_used
    operation_table_capacity
    operation_table_utilization_pct
    lease_table_used
    lease_table_capacity
    lease_member_table_used
    lease_member_table_capacity
  }
}
```

`loaded_snapshot_lsn` remains optional. An empty durable snapshot reports `startup_kind =
snapshot_only` with `loaded_snapshot_lsn = none`.

## Wire Discipline

The binary encoding remains:

- little-endian
- fixed-width integers
- explicit request and response tags
- length-prefixed arrays or byte payloads where variable length is required
- no generic serializer dependency

Malformed outer API frames are codec failures. Malformed `submit.payload` values are submission
failures with category `definite_failure` and code `invalid_request(...)`.

## Non-Goals

The current API does not yet specify:

- HTTP or gRPC transport
- authentication or authorization
- batch submission
- streaming reads
- replication-aware routing
- client SDK ergonomics

# AllocDB Alpha API

## Scope

This document defines the current transport-neutral alpha API exposed by
`crates/allocdb-node::api`.

It is intentionally not an HTTP or RPC spec yet. v1 first fixes:

- request and response shapes
- binary wire encoding
- definite vs indefinite submission failures
- strict-read behavior for resource and reservation queries
- metrics exposure

Network transport, authentication, and multi-node routing are out of scope for this stage.

## Current Rust Entry Points

The alpha API is available through:

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

The current request set is:

- `submit`
- `get_resource`
- `get_reservation`
- `get_metrics`

### Submit

```text
submit {
  request_slot : u64
  payload      : bytes
}
```

`payload` is the existing encoded core `ClientRequest` payload:

```text
operation_id : u128
client_id    : u128
command      : ...
```

The node API deliberately wraps that payload instead of redefining command encoding a second time.

### Get Resource

```text
get_resource {
  required_lsn : u64 | none
  resource_id  : u128
}
```

If `required_lsn` is present, the node enforces the strict-read fence before serving the read.

### Get Reservation

```text
get_reservation {
  required_lsn   : u64 | none
  current_slot   : u64
  reservation_id : u128
}
```

`current_slot` is required because reservation retirement is defined in logical slots.

### Get Metrics

```text
get_metrics {
  current_wall_clock_slot : u64
}
```

The caller provides wall-clock progress so the engine can derive `logical_slot_lag` and
`expiration_backlog` deterministically.

## Submission Responses

Submission responses are split into two classes:

- `committed`
- `rejected`

### Committed

```text
committed {
  applied_lsn       : u64
  result_code       : enum
  reservation_id    : u128 | none
  deadline_slot     : u64 | none
  from_retry_cache  : bool
}
```

This is the committed state-machine result. A committed command may still return domain failures
such as `resource_busy` or `holder_mismatch`; those are allocator outcomes, not submission-layer
failures.

### Rejected

```text
rejected {
  category : enum { definite_failure, indefinite }
  code     : enum
}
```

Current submission failure codes are:

- `invalid_request(buffer_too_short | invalid_command_tag(tag) | invalid_layout)`
- `command_too_large(encoded_len, max_command_bytes)`
- `overloaded(queue_depth, queue_capacity)`
- `engine_halted`
- `storage_failure`

The required category mapping is:

- definite failure:
  - `invalid_request`
  - `command_too_large`
  - `overloaded`
- indefinite:
  - `engine_halted`
  - `storage_failure`

This distinction is mandatory. The API must not flatten indefinite write outcomes into ordinary hard
failures.

## Read Responses

### Resource Query

`get_resource` returns one of:

- `found(resource_view)`
- `not_found`
- `fence_not_applied(required_lsn, last_applied_lsn)`

Current `resource_view` fields are:

```text
resource_id             : u128
state                   : enum { available, reserved, confirmed }
current_reservation_id  : u128 | none
version                 : u64
```

### Reservation Query

`get_reservation` returns one of:

- `found(reservation_view)`
- `not_found`
- `retired`
- `fence_not_applied(required_lsn, last_applied_lsn)`

Current `reservation_view` fields are:

```text
reservation_id    : u128
resource_id       : u128
holder_id         : u128
state             : enum { reserved, confirmed, released, expired }
created_lsn       : u64
deadline_slot     : u64
released_lsn      : u64 | none
retire_after_slot : u64 | none
```

`retired` is distinct from `not_found` because bounded history is part of the product contract.

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
  }
}
```

## Wire Discipline

The current binary encoding is:

- little-endian
- fixed-width integers
- explicit request and response tags
- length-prefixed byte payloads where variable length is required
- no generic serializer dependency

Malformed outer API frames are codec failures. Malformed `submit.payload` values are submission
failures with category `definite_failure` and code `invalid_request(...)`.

## Non-Goals

The current alpha API does not yet specify:

- HTTP or gRPC transport
- authentication or authorization
- batch submission
- streaming reads
- replication-aware routing
- client SDK ergonomics

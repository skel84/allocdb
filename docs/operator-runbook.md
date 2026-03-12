# AllocDB Operator Runbook

## Scope

This runbook covers the current single-node alpha implemented by `crates/allocdb-node`.

It is intentionally limited to behavior that exists on current `main`:

- one process
- one WAL file
- one optional snapshot file
- one in-memory executor
- one transport-neutral API surface

Current operational constraints:

- there is no standalone node daemon in this repository yet
- there is no built-in background expiration worker
- there is no built-in background checkpoint loop
- the host process must choose WAL and snapshot paths, expose the API, set up logging, and drive
  `tick_expirations`

Related docs:

- [Alpha API](./api.md)
- [Storage and Recovery](./storage.md)
- [Testing Strategy](./testing.md)
- [Benchmark Harness](./benchmark-harness.md)
- [Architecture](./architecture.md)

## Durable Files And Startup Path

The single-node alpha persists state in:

- one append-only WAL file on the live path
- one snapshot file written only when the host calls `checkpoint`

Recommended startup path:

1. provide the same core and engine bounds that were used to create the durable state
2. call `SingleNodeEngine::recover(core_config, engine_config, snapshot_path, wal_path)`
3. inspect `get_metrics` or `metrics(current_wall_clock_slot)` before admitting traffic

Why `recover` is the preferred restart path:

- it tolerates a missing snapshot file
- it tolerates an empty or missing WAL file
- it reports which startup path actually happened through recovery metrics

Use `SingleNodeEngine::open(...)` only for an intentional fresh start when prior durable state is
being discarded on purpose.

## Startup States

The engine reports startup status through `metrics.recovery`.

### `fresh_start`

Meaning:

- no snapshot was loaded
- no WAL frames were replayed

Expected cases:

- `open(...)`
- `recover(...)` against absent or empty durable state

### `wal_only`

Meaning:

- no snapshot was loaded
- one or more WAL frames were replayed

Expected case:

- restart from a live WAL without a snapshot

### `snapshot_only`

Meaning:

- a snapshot file was loaded
- no later WAL frames were replayed

Current detail:

- an empty durable snapshot still counts as `snapshot_only`
- in that case `loaded_snapshot_lsn` remains `none`

### `snapshot_and_wal`

Meaning:

- a snapshot was loaded
- one or more later WAL frames were replayed

Expected case:

- restart after at least one successful checkpoint and at least one later committed write

### Startup Checklist

After every process start or restart, verify:

- `accepting_writes = true`
- `startup_kind` matches the expected durable state
- `loaded_snapshot_lsn`, `replayed_wal_frame_count`, and `replayed_wal_last_lsn` look plausible
- `active_snapshot_lsn` matches the last successful checkpoint, if one was expected

If `accepting_writes = false`, do not admit traffic. The engine is in fail-closed mode and must be
recovered first.

## Normal Restart And Planned Checkpoint

For a normal planned restart:

1. stop new ingress in the host process
2. drain any queued work if the host uses `enqueue_*` plus `process_next`
3. call `checkpoint(snapshot_path)` if you want a fresh restart anchor and a shorter retained WAL
4. verify that `metrics.recovery.active_snapshot_lsn` advanced
5. stop the process
6. restart with `recover(...)`

Current checkpoint behavior:

- checkpoint requires `accepting_writes = true`
- checkpoint rejects a non-empty submission queue
- checkpoint writes the new snapshot first
- checkpoint then rewrites the WAL with one-checkpoint overlap
- checkpoint appends a `snapshot_marker` at the active snapshot anchor in the retained WAL

Treat these checkpoint failures as operationally significant:

- `EngineHalted`
- `QueueNotEmpty`
- `WalNotClean`
- `WalStateMismatch`
- snapshot or WAL file errors

Current checkpoint nuance:

- snapshot write failure does not halt the live engine
- WAL cleanliness/state failures and WAL rewrite failures can leave `accepting_writes = false`,
  which then requires recovery before traffic resumes

Do not claim a clean checkpoint if any of those occur.

## Expiration Maintenance

The current alpha does not expire reservations in the background. The host process must call
`tick_expirations(current_wall_clock_slot)` or send the matching API request.

Current tick behavior:

1. drain already-queued client submissions first
2. scan for overdue reserved reservations
3. sort them deterministically
4. truncate to `max_expirations_per_tick`
5. append internal `expire` commands to the WAL
6. sync the WAL
7. apply the expire commands through the normal executor path

Operational implications:

- passing a reservation deadline does not free the resource by itself
- the resource becomes reusable only after the internal `expire` command commits and applies
- delayed expiration is acceptable; premature reuse is not
- sustained non-zero `expiration_backlog` means the host is not ticking often enough or the
  current `max_expirations_per_tick` bound is too low for current load

Watch these metrics together:

- `core.logical_slot_lag`
- `core.expiration_backlog`
- `recovery.active_snapshot_lsn`

## Read Behavior

Strict reads are fail-fast in the current alpha.

If the caller supplies `required_lsn`, the engine does not wait. It checks the local in-memory
state immediately and returns:

- normal data when `last_applied_lsn >= required_lsn`
- `fence_not_applied(required_lsn, last_applied_lsn)` when the fence has not been met yet
- `engine_halted` when the node has entered fail-closed mode

Reservation lookups also distinguish:

- `found`
- `not_found`
- `retired`

`retired` means the reservation existed but has already aged out of the bounded live-history
window.

## Overload And Bounded Failure Modes

The operator-facing distinction to preserve is:

- submission-layer rejection before commit
- committed allocator outcome after durable sequencing

### Submission-Layer Rejections

`submit` and `tick_expirations` can return `rejected(category, code)`.

Current definite failures:

- `invalid_request`
- `command_too_large`
- `overloaded`

Current indefinite failures:

- `engine_halted`
- `storage_failure`

What `overloaded` means:

- the bounded submission queue is full
- the request was not sequenced
- no WAL append happened for that request
- the engine may still be otherwise healthy

What `engine_halted` or `storage_failure` means:

- the node hit a WAL append or sync ambiguity
- the write outcome is not safe to classify as definite
- the host should stop admitting traffic and recover from durable state

### Committed Capacity Outcomes

A committed response can still contain a bounded rejection in `result_code`. Current examples:

- `resource_busy`
- `resource_table_full`
- `reservation_table_full`
- `expiration_index_full`
- `operation_table_full`
- `operation_conflict`

These are not transport or storage failures. They are committed allocator outcomes and should be
treated as definite business results.

### Metrics To Watch Under Pressure

- `queue_depth` and `queue_capacity` for ingress pressure
- `core.operation_table_utilization_pct` for retry-window pressure before
  `operation_table_full`
- `core.logical_slot_lag` and `core.expiration_backlog` for expiration maintenance lag

## WAL Ambiguity And Fail-Closed Behavior

If the live engine hits a WAL append or sync failure while committing either a client command or an
internal expiration command, it immediately enters fail-closed mode.

Current fail-closed behavior:

- `accepting_writes` flips to `false`
- later writes return `engine_halted`
- strict reads also return `engine_halted`
- the process must rebuild from durable state before it can serve traffic again

The code logs these events at `error` level with these exact signatures:

- `halting engine on WAL error, accepting_writes set to false: operation_id={} request_slot={} applied_lsn={} phase={} error={error:?}`
- `halting engine on internal WAL error, accepting_writes set to false: request_slot={} applied_lsn={} phase={} error={error:?}`

### Ambiguous Write Retry Rule

Client retries after an indefinite write outcome must:

- reuse the same `operation_id`
- reuse the same payload
- happen within the configured retry window

Current recovery behavior within the dedupe window:

- if the failure happened before WAL append, the retry executes once after recovery
- if the failure happened after WAL append but before sync returned, recovery may restore the
  committed write and the retry returns the cached result

Current limitation:

- once the dedupe window has expired, the same `operation_id` is no longer guaranteed to resolve
  the earlier ambiguity

## Recovery And Corruption Handling

`recover(...)` is the only supported way to clear fail-closed mode.

### What Recovery Repairs Automatically

Recovery automatically truncates only one case:

- an incomplete EOF torn tail

That means:

- recovery scans the WAL to the last valid frame boundary
- if the stop reason is `torn_tail`, it truncates the file to the valid prefix
- it then replays the valid frames

### What Recovery Treats As Fatal

Recovery fails closed and returns an error for:

- invalid runtime configuration for the persisted state
- snapshot file read or decode failure
- semantically invalid decoded snapshot contents
- middle-of-log WAL corruption such as checksum failure before EOF
- checksum-valid WAL with non-monotonic `lsn`
- checksum-valid WAL with rewound `request_slot`
- WAL payload decode failure during replay

Current operational rule:

- do not truncate or rewrite the WAL manually for any of those fatal cases
- preserve the WAL file, snapshot file, config, and logs for offline investigation

### Recovery Logs

Successful recovery logs one `info` line:

- `recovery complete: loaded_snapshot=... loaded_snapshot_lsn=... replayed_wal_frame_count=... replayed_wal_last_lsn=...`

Failure logs use `error` level and distinguish:

- `recovery aborted due to invalid configuration`
- `recovery failed while loading snapshot`
- `recovery rejected semantically invalid snapshot`
- `recovery failed while scanning wal`
- `recovery rejected wal payload`
- `recovery rejected wal replay ordering`

## Recommended Validation And Inspection Commands

Validate the repository path used by CI:

```sh
./scripts/preflight.sh
```

Re-run the core recovery coverage directly:

```sh
cargo test -p allocdb-core recovery -- --nocapture
```

Re-run node restart and fail-closed coverage directly:

```sh
cargo test -p allocdb-node recover_restores_state_and_retry_cache
cargo test -p allocdb-node halted_engine_rejects_reads_until_recovery
cargo test -p allocdb-node expiration_tick_post_append_failure_requires_recovery_for_expired_state
cargo test -p allocdb-node recovery_metrics_report_snapshot_and_wal_replay
```

Run the deterministic benchmark harness:

```sh
cargo run -p allocdb-bench -- --scenario all
```

Inspect the current recovery and halt log signatures in source:

```sh
rg -n "recovery complete|recovery failed while loading snapshot|recovery rejected semantically invalid snapshot|recovery rejected wal replay ordering|halting engine on WAL error|halting engine on internal WAL error" \
  crates/allocdb-core/src/recovery.rs \
  crates/allocdb-node/src/engine.rs
```

Inspect durable files on disk:

```sh
ls -lh /path/to/node.wal /path/to/node.snapshot
stat /path/to/node.wal /path/to/node.snapshot
```

## Minimum Operator Checklist

- start with `recover(...)`, not `open(...)`, for every normal restart
- do not admit traffic until `accepting_writes = true`
- drive `tick_expirations` from the host process
- watch queue pressure, retry-window utilization, logical slot lag, and expiration backlog
- treat `engine_halted` and `storage_failure` as indefinite outcomes
- recover from durable state after any WAL-path ambiguity
- allow automatic EOF torn-tail truncation, but fail closed on all other corruption classes

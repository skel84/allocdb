# AllocDB Operator Runbook

## Scope

This runbook covers the current operator surfaces implemented by `crates/allocdb-node`.

It is intentionally limited to behavior that exists on the current branch:

- the single-node alpha engine
- the first local multi-process replicated cluster runner
- the first local QEMU replicated testbed

Current operational constraints for the local replicated runner:

- the cluster runner binds one loopback `control`, `client`, and `protocol` address per replica
- only the `control` listener is implemented today
- `client` and `protocol` listeners are reserved and logged but return `not implemented`
- there is a built-in local process fault-control harness and a QEMU control-node script, but
  there is still no replicated client transport on either boundary
- there is no built-in background expiration worker or checkpoint loop

Current operational constraints for the single-node alpha remain:

- there is no standalone general-purpose node daemon beyond the local runner
- the host process must choose WAL and snapshot paths, expose the API, set up logging, and drive
  `tick_expirations`

Related docs:

- [Alpha API](./api.md)
- [Storage and Recovery](./storage.md)
- [Testing Strategy](./testing.md)
- [Benchmark Harness](./benchmark-harness.md)
- [Architecture](./architecture.md)

## Local Replicated Cluster Runner

Command surface:

- `cargo run -p allocdb-node --bin allocdb-local-cluster -- start --workspace <path>`
- `cargo run -p allocdb-node --bin allocdb-local-cluster -- status --workspace <path>`
- `cargo run -p allocdb-node --bin allocdb-local-cluster -- stop --workspace <path>`
- `cargo run -p allocdb-node --bin allocdb-local-cluster -- crash --workspace <path> --replica-id <id>`
- `cargo run -p allocdb-node --bin allocdb-local-cluster -- restart --workspace <path> --replica-id <id>`
- `cargo run -p allocdb-node --bin allocdb-local-cluster -- isolate --workspace <path> --replica-id <id>`
- `cargo run -p allocdb-node --bin allocdb-local-cluster -- heal --workspace <path> --replica-id <id>`

What `start` does:

- creates or reuses one stable workspace rooted at `<path>`
- persists one `cluster-layout.txt` file with the chosen local bounds, replica identities,
  addresses, and file paths
- launches `3` external replica processes from one command surface
- recovers each replica through `ReplicaNode::recover(...)`
- bootstraps a fresh workspace into view `1` with replica `1` as `primary` and replicas `2` and
  `3` as `backup`

What restart preserves:

- replica IDs
- the durable workspace layout under `<path>/replica-{1,2,3}/`
- the persisted engine and core bounds stored in `cluster-layout.txt`
- the chosen loopback `control`, `client`, and `protocol` addresses

Current workspace layout:

- `<path>/cluster-layout.txt`
- `<path>/cluster-faults.txt`
- `<path>/cluster-timeline.log`
- `<path>/logs/replica-{1,2,3}.log`
- `<path>/run/replica-{1,2,3}.pid`
- `<path>/replica-{1,2,3}/replica.metadata`
- `<path>/replica-{1,2,3}/replica.metadata.prepare`
- `<path>/replica-{1,2,3}/state.snapshot`
- `<path>/replica-{1,2,3}/state.wal`

What `status` shows:

- process ID
- replica state, role, and current view
- local network fault state for each replica
- committed and active-snapshot `lsn`
- startup recovery kind and write-acceptance flag when the wrapped engine is active
- control, client, and protocol addresses
- log, pid, metadata, prepare-log, snapshot, and WAL paths
- fault-state and timeline file paths

What the fault-control commands do:

- `crash` sends an abrupt local process kill to one replica and waits for the process to exit
- `restart` spawns one replica back through the normal `ReplicaNode::recover(...)` path
- `isolate` marks one replica isolated from external `client` and `protocol` traffic while leaving
  `control` reachable
- `heal` restores that replica's external `client` and `protocol` connectivity
- every command appends one ordered event to `cluster-timeline.log`

Current control hooks:

- `status`
- `stop`

Current limits:

- the runner still reserves `client` and `protocol` listeners for follow-on replicated transport
  work; they now reject traffic either as `not implemented` or `network isolated by local harness`
- the runner only auto-configures normal-mode roles on a fresh workspace when recovered metadata is
  still in `recovering`; after that it preserves recovered durable metadata instead of resetting
  views or roles
- there is still no background expiration or checkpoint worker inside the replica daemons

## Local QEMU Testbed

Command surface:

- `cargo run -p allocdb-node --bin allocdb-qemu-testbed -- prepare --workspace <path> --base-image-path <cloudimg.qcow2> --local-cluster-bin <allocdb-local-cluster>`
- `cargo run -p allocdb-node --bin allocdb-qemu-testbed -- start --workspace <path>`
- `cargo run -p allocdb-node --bin allocdb-qemu-testbed -- status --workspace <path>`
- `cargo run -p allocdb-node --bin allocdb-qemu-testbed -- stop --workspace <path>`
- `cargo run -p allocdb-node --bin allocdb-qemu-testbed -- ssh-control --workspace <path>`
- `cargo run -p allocdb-node --bin allocdb-qemu-testbed -- control --workspace <path> -- <status|isolate|heal|crash|restart|reboot|export-replica|import-replica|collect-logs> ...`
- `cargo run -p allocdb-node --bin allocdb-jepsen -- plan`
- `cargo run -p allocdb-node --bin allocdb-jepsen -- analyze --history-file <history.txt>`
- `cargo run -p allocdb-node --bin allocdb-jepsen -- verify-qemu-surface --workspace <path>`
- `cargo run -p allocdb-node --bin allocdb-jepsen -- run-qemu --workspace <path> --run-id <run-id> --output-root <artifacts>`
- `cargo run -p allocdb-node --bin allocdb-jepsen -- archive-qemu --workspace <path> --run-id <run-id> --history-file <history.txt> --output-root <artifacts>`

What `prepare` does:

- persists one `qemu-testbed-layout.txt` file rooted at `<path>`
- creates one copy-on-write overlay and one generated NoCloud seed image for the control guest and
  each replica guest
- copies one per-guest firmware-vars file from the local QEMU firmware templates
- writes one shared SSH keypair used by the host and control guest for replica orchestration
- embeds the existing `allocdb-local-cluster` binary and one static local-cluster layout into the
  guest seed data

Host overrides and prerequisites:

- `QEMU_SHARE_DIR` can point at a non-default QEMU firmware directory when the host does not keep
  firmware templates under the standard search paths
- `QEMU_ACCEL` can override the accelerator embedded into the rendered QEMU command; the default is
  `hvf` on macOS and `kvm` on Linux
- `prepare` hard-fails unless the host has the arch-specific `qemu-system-*` binary, `qemu-img`,
  `ssh`, and `ssh-keygen` on `PATH`
- `prepare` uses `hdiutil` on macOS and `mkisofs` or `genisoimage` on Linux-class hosts when it
  builds NoCloud seed images, and it hard-fails if the platform-appropriate ISO builder is absent
- `prepare` hard-fails unless the configured `allocdb-local-cluster` binary already exists on the
  host
- if `--base-image-path` does not already exist, `prepare` hard-fails unless `curl` is on `PATH`
  and the base-image download succeeds
- `prepare` hard-fails if the QEMU firmware templates are not reachable either through the default
  search paths or `QEMU_SHARE_DIR`

What `start` does:

- boots `3` replica guests and `1` control guest under QEMU
- places replica `control` on one management network and replica `client`/`protocol` on one
  separate cluster network
- forwards host SSH only to the control guest; all replica orchestration then flows through the
  generated control-node script

Current workspace layout:

- `<path>/qemu-testbed-layout.txt`
- `<path>/qemu/images/control-overlay.qcow2`
- `<path>/qemu/images/replica-{1,2,3}-overlay.qcow2`
- `<path>/qemu/seed/control-seed.iso`
- `<path>/qemu/seed/replica-{1,2,3}-seed.iso`
- `<path>/qemu/seed/allocdb-control/{meta-data,network-config,user-data}`
- `<path>/qemu/seed/replica-{1,2,3}/{meta-data,network-config,user-data}`
- `<path>/qemu/firmware/{control,replica-{1,2,3}}-vars.fd`
- `<path>/qemu/run/{control,replica-{1,2,3}}.pid`
- `<path>/qemu/logs/{control,replica-{1,2,3}}-console.log`
- `<path>/qemu/ssh/id_ed25519`
- `<path>/qemu/ssh/id_ed25519.pub`

Current control hooks:

- `status` queries each replica control socket from the control guest
- `isolate`, `heal`, `crash`, and `restart` SSH into the target replica guest and reuse the
  existing local fault-control commands there
- `reboot` reboots one target replica guest through the control node
- `export-replica` and `import-replica` stream one replica workspace through the control guest so
  host-side tools can stage failover and rejoin rewrites without inventing a second recovery path
- `collect-logs` gathers replica `journalctl`, `cluster-faults.txt`, `cluster-timeline.log`, and
  control-status snapshots under the control guest

Current limits:

- the first QEMU testbed still depends on one supported cloud image already being available or
  downloadable on the host
- `verify-qemu-surface` still proves only one basic metrics plus primary submit/read round trip
- `run-qemu` now executes the documented Jepsen control and nemesis runs, but it still drives one
  scripted gate scenario at a time rather than one long-running soak with independent clients
- failover and rejoin now use host-side staged workspace rewrites, so the operator still needs one
  prepared QEMU testbed and enough local disk for fetched/pushed replica archives during those runs

## Single-Node Engine

### Durable Files And Startup Path

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
rg -n "recovery complete|recovery aborted due to invalid configuration|recovery failed while loading snapshot|recovery rejected semantically invalid snapshot|recovery failed while scanning wal|recovery rejected wal payload|recovery rejected wal replay ordering|halting engine on WAL error|halting engine on internal WAL error" \
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

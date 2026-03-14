# AllocDB Status

## Current State

- Phase: replicated implementation
- Planning IDs: tasks use `M#-T#`; spikes use `M#-S#`
- Current milestone status:
  - `M0` semantics freeze: complete enough for core work
  - `M1` pure state machine: implemented
  - `M1H` constant-time core hardening: complete
  - `M2` durability and recovery: implemented
  - `M3` submission pipeline: implemented
  - `M4` simulation: implemented
  - `M5` single-node alpha surface: implemented
  - `M6` replication design: implemented
  - `M7` replicated core prototype: in progress
  - `M8` external cluster validation: in progress
- Latest completed implementation chunks:
  - `4156a80` `Bootstrap AllocDB core and docs`
  - `f84a641` `Add WAL file and snapshot recovery primitives`
  - `d87c9a7` `Add repo guardrails and status tracking`
  - `79ae34f` `Add snapshot persistence and replay recovery`
  - `1583d67` `Use fixed-capacity maps in allocator core`
  - `3d6ff0f` `Fail closed on WAL corruption`
  - `39f103b` `Defer conditional confirm and add health metrics`
  - `82cb8d8` `Add single-node submission engine crate`
  - current validated chunk: explicit seeded crash-point injection across submit, checkpoint, and
    recovery boundaries plus checked slot and LSN arithmetic across trusted-core and single-node
    sequencing paths, including deterministic pre-commit overflow rejection for request slots,
    fail-closed replay rejection for overflowed WAL commands, restart coverage for post-sync
    submit replay and replay-interrupted recovery, explicit `next_lsn` exhaustion handling after
    `u64::MAX`, deterministic storage-fault injection for append failures, sync-failure
    ambiguity, checksum-mismatch fail-closed recovery, and torn-tail truncation over the real WAL
    restart path, plus replayable seeded schedule exploration over ingress contention,
    due-expiration selection with earliest-deadline priority, retry timing, a replicated-node
    wrapper with durable replica metadata bootstrap, restart validation, and explicit faulted-state
    entry on invalid local protocol metadata, and a deterministic three-replica harness with one
    explicit message queue, one connectivity matrix, and replayable transcripts for queued,
    delivered, dropped, crashed, and restarted replica actions, plus the first majority-backed
    quorum write path with one configured primary, durable prepared-entry buffering, real
    `prepare`/`prepare_ack`/`commit` queue semantics, commit publication only after majority
    durable append, primary-only read enforcement on locally applied committed state, explicit
    quorum-loss demotion into `view_uncertain`, durable higher-view vote recording, higher-view
    takeover that reconstructs the latest committed prefix before normal mode, stale-message
    discard across view change, fail-closed stale-primary read and write rejection, plus
    checkpoint-aware stale-replica rejoin that chooses suffix-only catch-up when the target still
    holds a recent enough committed durable prefix, falls back to snapshot transfer when the
    primary has already pruned older history, discards divergent uncommitted prepared suffix
    during rejoin, rejects faulted replicas instead of auto-repairing them, and fails closed if a
    target already knows a higher durable view than the current primary, plus promoted
    deterministic partition and primary-crash scenarios that prove minority-partition catch-up,
    full-split fail-closed behavior, pre-quorum retry replay, majority-appended failover
    reconstruction, prepared-suffix recovery from another voter during takeover, and post-reply
    retry/read preservation on the new primary, and the first multi-process local cluster runner
    with one persisted `cluster-layout.txt`, stable three-replica workspaces, per-replica pid and
    log files, reserved loopback addresses, and a smoke-tested `start`/`status`/`stop` command
    surface that preserves replica identity and on-disk layout across restart, plus the first
    local fault-control harness with per-replica `crash`/`restart`/`isolate`/`heal` commands, one
    persisted `cluster-faults.txt` network-isolation state, one replayable `cluster-timeline.log`,
    and externally visible isolation rejection on the reserved `client` and `protocol` listeners, plus one host-side QEMU testbed CLI that materializes per-guest overlays, NoCloud seed images, firmware vars, one static in-guest cluster layout, and one control-node orchestration script around the existing replica daemon and local fault-control commands

## What Exists

- Trusted-core crate: `crates/allocdb-core`
- Single-node wrapper crate: `crates/allocdb-node`
- Benchmark harness crate: `crates/allocdb-bench`
- In-memory deterministic allocator:
  - deterministic fixed-capacity open-addressed resource, reservation, and operation tables
  - bounded reservation and operation retirement queues
  - bounded timing-wheel expiration index
  - `create_resource`, `reserve`, `confirm`, `release`, `expire`
  - bounded health snapshot with logical slot lag, expiration backlog, and operation-table
    utilization
- In-process submission engine:
  - typed and encoded request validation before commit
  - bounded submission queue with deterministic overload behavior
  - LSN assignment, WAL append, sync, and live apply
  - definite pre-commit rejection for request slots whose derived deadline, history, or dedupe
    windows would overflow `u64`
  - pre-sequencing duplicate lookup for applied and already-queued `operation_id`
  - strict-read fence by applied LSN
  - restart path from snapshot plus WAL
  - explicit definite-vs-indefinite submission error categorization
  - explicit restart-and-retry handling for ambiguous WAL failures within the dedupe window
  - explicit `lsn_exhausted` write rejection after the engine commits the last representable LSN
  - node-level metrics for queue pressure, write acceptance, startup recovery status, and active
    snapshot anchor
- Deterministic benchmark harness:
  - CLI entrypoint at `cargo run -p allocdb-bench -- --scenario all`
  - one-resource-many-contenders scenario for hot-spot reserve contention
  - high-retry-pressure scenario for duplicate replay, conflict replay, full dedupe table
    rejection, and post-window recovery
  - scenario reports include elapsed time, throughput, metrics snapshots, and WAL byte counts
- Alpha API surface:
  - transport-neutral request and response types in `crates/allocdb-node::api`
  - binary request and response codec with fixed-width little-endian encoding
  - explicit wire-level mapping for definite vs indefinite submission failures
  - strict-read fence responses plus halt-safe read rejection for resource and reservation queries
  - retired reservation lookups remain distinct from `not_found` across later writes and snapshot
    restore through bounded retired-watermark metadata
  - bounded `tick_expirations` maintenance request for live TTL enforcement
  - metrics exposure through the same API boundary
- Operator documentation:
  - operator-facing runbook for the single-node alpha, local replicated cluster runner, and local QEMU testbed, including workspace layout plus current control-hook limits
- Replication design draft:
  - VSR-style primary/backup replicated log with fixed membership and majority quorums
  - primary-only reads in the first replicated release
  - protocol invariants that preserve single-node idempotency, strict-read, TTL, and
    reservation-ID semantics across failover
- Replicated validation planning:
  - deterministic cluster-simulation plan that extends seeded simulation to partitions, primary
    crash, and rejoin without a mock semantics layer
  - Jepsen gate with explicit contention, ambiguity, failover, and expiration workloads
  - retry-aware history interpretation and release-blocking invariants for duplicate execution,
    stale successful reads, double allocation, and early reuse
- Host-side Jepsen harness slice:
  - one release-gate matrix planner, one retry-aware history codec/analyzer, one host-side artifact bundler for duplicate-execution, double-allocation, stale-read, early-expiration, unresolved-ambiguity, and fetched external-cluster log checks, plus explicit `verify-qemu-surface` and `verify-kubevirt-surface` probes that exercise one real metrics round trip on every replica and one real primary submit/read round trip through the live replicated protocol surface
  - one real `run-qemu` and one real `run-kubevirt` executor for the full documented release-gate matrix, with persisted histories and artifact bundles for control, crash-restart, partition-heal, and mixed-failover runs, plus host-side failover/rejoin orchestration built from replica workspace export/import and staged `ReplicaNode::recover(...)` rewrites
  - one `capture-kubevirt-layout` helper that records the live KubeVirt VM IPs, namespace, helper-pod settings, and SSH key path needed to drive the matrix from the host
- Replicated node scaffolding:
  - dedicated replica metadata file with temp-write, rename, and directory-sync durability
  - persisted replica identity, role, view, commit point, snapshot anchor, last-normal view, and
    optional durable vote metadata
  - startup bootstrap for missing metadata on both fresh-open and recover paths
  - fail-closed `faulted` state when metadata bytes are corrupt, identity is mismatched, or local
    applied/snapshot state contradicts the persisted replicated metadata
  - configurable normal-mode `primary` and `backup` roles for one current view
  - explicit `view_uncertain` role plus durable higher-view voting for replicas that lost quorum
    or are participating in failover
  - durable prepared-entry sidecar for pre-commit replicated client commands
  - prepare append, commit-through, and strict primary-read guards built around the existing
    single-node executor rather than a second apply path
- Local multi-process cluster runner:
  - CLI entrypoint at `cargo run -p allocdb-node --bin allocdb-local-cluster -- <start|stop|status|crash|restart|isolate|heal> ...` with one persisted `cluster-layout.txt`
  - stable replica identities, local bounds, and three external replica processes from one command surface
  - per-replica loopback `control`, `client`, and `protocol` listeners with `status` and `stop` hooks on `control`
  - per-replica pid, log, WAL, snapshot, metadata, and prepared-log paths exposed through `status`, with restart through the real `ReplicaNode::recover` path and stable durable workspace reuse
  - one persisted `cluster-faults.txt` file that marks whole-replica client/protocol isolation without affecting control reachability, plus one append-only `cluster-timeline.log` for later checker/debug reuse
  - reserved `client` and `protocol` listeners now fail with explicit isolation errors when the local fault harness marks that replica isolated
  - real primary-side client/protocol transport for external `submit`, `get_resource`, `get_reservation`, `get_metrics`, and replicated `tick_expirations`, with majority append before publish and backup reads still failing closed as `not primary`
- Durability primitives:
  - WAL frame codec and recovery scan
  - file-backed WAL append, sync, recovery, and torn-tail truncation
  - fail-closed recovery on middle-of-log corruption
  - fail-closed recovery on non-monotonic WAL replay metadata and malformed decoded snapshot
    semantics
  - fail-closed recovery on replayed commands whose derived slot windows overflow configured
    bounds
  - snapshot encode, decode, capture, restore
  - file-backed snapshot write and load
  - explicit WAL command payload encoding and live-path replay recovery
  - checkpoint path that writes the new snapshot first, then rewrites retained WAL history
  - one-checkpoint WAL overlap and `snapshot_marker` retention for safe checkpoint replacement
- Deterministic simulation support:
  - reusable simulation harness in `crates/allocdb-node/src/simulation.rs`
  - explicit simulated slot advancement under test control, with no wall-clock reads in the
    exercised engine path
  - seeded same-slot ready-set scheduling with reproducible transcripts
  - seeded labeled schedule actions that resolve candidate slot windows into replayable
    submit/tick transcripts
  - seeded due-expiration selection over the real internal-expire path, bounded by the production
    per-tick expiration limit
  - seeded one-shot crash plans over named client-submit, internal-apply, checkpoint, and
    recovery boundaries
  - one-shot storage fault helpers over append failure, sync failure, checksum mismatch, and
    torn-tail WAL mutation against real on-disk recovery
  - checkpoint, restart, and live write-fault helpers over the real `SingleNodeEngine`
  - regression coverage for crash-selected post-sync submit replay, crash-after-snapshot-write
    checkpoint recovery, replay-interrupted recovery restart, sync-failure retry recovery,
    checksum-corruption fail-closed restart, torn-tail truncation retry, ingress contention winner
    order, same-deadline expiration order, mixed-deadline earliest-first expiration priority, and
    retry timing across the dedupe window
  - reusable replicated cluster harness in `crates/allocdb-node/src/replicated_simulation.rs`
  - three real `ReplicaNode`s with independent WAL, snapshot, and metadata workspaces
  - explicit replica-to-replica and client-to-replica connectivity matrix under test control
  - explicit protocol-message queue plus replayable transcripts for queue, deliver, drop, crash,
    and restart actions
  - real `prepare`, `prepare_ack`, and `commit` protocol payload delivery on that queue
  - configured-primary client submit flow with result publication only after majority durable
    append
  - retry-aware client submit helper that returns one cached committed result on the current
    primary instead of assigning a fresh replicated LSN
  - backup replicas that durably append prepares but do not apply allocator state until commit
  - primary-only resource reads guarded by the existing strict-read fence after local commit
  - automatic quorum-loss detection that demotes a stranded primary out of service
  - explicit higher-view takeover that records durable votes from a reachable majority,
    reconstructs the safe committed prefix on the new primary, discards stale uncommitted suffix,
    and drops old-view protocol messages
  - replica crash as loss of volatile state with restart through real `ReplicaNode::recover`
  - checkpoint-assisted rejoin that rewrites one stale replica from suffix-only WAL catch-up or
    snapshot transfer, then restarts through the real recovery path before returning the replica
    to backup mode
  - regression coverage for quorum-loss fail-closed reads and writes, higher-view takeover with
    stale-primary read rejection, prepared-suffix recovery from another voter during takeover,
    isolated-backup partition heal and catch-up, non-quorum split fail-closed behavior with later
    rejoin convergence, primary crash before quorum append, primary crash after majority append,
    primary crash after reply, suffix-only rejoin, snapshot-transfer rejoin, and faulted rejoin
    rejection
- Validation:
  - core durability: `cargo test -p allocdb-core wal -- --nocapture`, `cargo test -p allocdb-core snapshot -- --nocapture`, `cargo test -p allocdb-core recovery -- --nocapture`, `cargo test -p allocdb-core snapshot_restores_retired_lookup_watermark`
  - node runtime: `cargo test -p allocdb-node api_reservation_reports_retired_history`, `cargo test -p allocdb-node engine -- --nocapture`, `cargo test -p allocdb-node replica -- --nocapture`
  - simulation: `cargo test -p allocdb-node simulation -- --nocapture`, `cargo test -p allocdb-node replicated_simulation -- --nocapture`
  - local cluster, qemu assets, Jepsen harness, and benchmarks: `cargo test -p allocdb-node local_cluster -- --nocapture`, `cargo test -p allocdb-node qemu_testbed -- --nocapture`, `cargo test -p allocdb-node jepsen -- --nocapture`, `cargo test -p allocdb-node --bin allocdb-jepsen -- --nocapture`, `cargo run -p allocdb-node --bin allocdb-jepsen -- plan`, `cargo run -p allocdb-bench -- --scenario all`
  - repo gate: `scripts/preflight.sh`
## Current Focus
- `M8-T04` now has one real external Jepsen executor for the documented release-gate matrix across both QEMU and KubeVirt: the live runtime surface covers replicated `submit`, strict reads, and `tick_expirations`, while `allocdb-jepsen` can now capture one KubeVirt layout, verify the KubeVirt surface, execute real KubeVirt control runs with archived histories and host-side failover/rejoin cutovers, and expose one terminal watcher for live phase/replica progress during the run
- Jepsen run isolation is now stronger on persistent clusters: each `allocdb-jepsen` invocation uses one distinct client/slot namespace instead of reusing the same request identity across separate runs
- the next honest step is still operational, not architectural: start `watch-kubevirt` in one terminal, run the full KubeVirt matrix in another, capture artifacts for every run, and then decide whether the roadmap should open a post-M8 hardening milestone or declare the current queue complete
## How To Check Progress
- implementation status: [work-breakdown.md](./work-breakdown.md)
- milestone sequencing: [roadmap.md](./roadmap.md)
- reviewable history: `git log --oneline`
## Update Rule
Update this file whenever a task or milestone materially changes:
- milestone completion state
- implementation coverage
- recommended next step
- required validation commands

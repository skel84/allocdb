# AllocDB Status

## Current State
- Phase: replicated implementation with external Jepsen gate closed and M9 core follow-on active
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
  - `M9` generic lease-kernel follow-on: `T06` in progress on issue branch
- Latest completed implementation chunks:
  - `4156a80` `Bootstrap AllocDB core and docs`
  - `f84a641` `Add WAL file and snapshot recovery primitives`
  - `d87c9a7` `Add repo guardrails and status tracking`
  - `79ae34f` `Add snapshot persistence and replay recovery`
  - `1583d67` `Use fixed-capacity maps in allocator core`
  - `3d6ff0f` `Fail closed on WAL corruption`
  - `39f103b` `Defer conditional confirm and add health metrics`
  - `82cb8d8` `Add single-node submission engine crate`
  - current validated chunk: seeded crash-point and WAL-fault coverage across submit, checkpoint,
    and recovery boundaries; checked slot and LSN overflow handling; deterministic simulation over
    contention, retry timing, and due-expiration ordering; replicated metadata bootstrap and
    fail-closed faulted-state entry; majority-backed quorum writes with primary-only reads,
    quorum-loss demotion, and higher-view takeover; suffix and snapshot-based stale-replica rejoin
    with divergent prepared-suffix discard; promoted partition and primary-crash scenarios that
    preserve fail-closed behavior and retry/read continuity after failover; the local
    three-replica cluster runner, fault-control harness, and QEMU testbed around the real replica
    daemon; and the first trusted-core bundle-commit slice with bundle membership, bundle-aware
    confirm/release/expire, and bundle regression coverage

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
- Follow-on planning:
  - one draft lease-kernel follow-on plan that narrows the next trusted-core additions to bundle
    ownership, fencing, revoke, and an explicit liveness boundary, framed as generic
    scarce-resource semantics rather than product-specific behavior
  - one draft lease-kernel design-decision document that chooses a first-class lease authority
    object, bundle size `1` as the single-resource semantic special case, a lease-scoped fencing
    token, and a two-stage `revoke -> reclaim` safety model
  - one active authoritative-docs pass under issue `#80` that is rewriting semantics, API,
    architecture, and fault-model docs to the approved lease-centric contract while keeping the
    current reservation-centric implementation explicitly marked as compatibility surface
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
  - structured daemon-side logging for successful prepare quorum formation, commit-broadcast acknowledgements, accepted protocol prepare/commit traffic, expiration batch planning, and applied expiration commands
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
- PR `#82` merged the `#70` maintainability follow-up, and the closing evidence included the live
  KubeVirt `reservation_contention-control` and full `1800s`
  `reservation_contention-crash-restart` reruns on `allocdb-a` with `blockers=0`
- `M9-T01` through `M9-T05` are merged on `main` via PR `#81`, and the planning issues are closed
  on the `AllocDB` project
- issue `#83` / `M9-T06` is the active implementation slice on the current branch: the trusted
  core now has atomic bundle reservation, explicit bundle membership records, bundle-aware
  confirm/release/expire, and bundle-aware snapshot/codec coverage while preserving the existing
  reservation compatibility surface
- validation for the `#83` branch currently includes `cargo test -p allocdb-core -- --nocapture`
  plus full workspace test compilation via `cargo test --workspace --no-run`
- the next planned slices after `#83` remain `M9-T07` fencing, `M9-T08` revoke/safe reuse,
  `M9-T09` persistence and transport extension, `M9-T10` replication preservation, and
  `M9-T11` broader regression coverage

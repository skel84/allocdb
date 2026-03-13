use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use allocdb_core::command::{ClientRequest, Command};
use allocdb_core::config::Config;
use allocdb_core::ids::{ClientId, HolderId, Lsn, OperationId, ResourceId, Slot};

use crate::engine::{EngineConfig, SingleNodeEngine};
use crate::replica::{
    DurableVote, ReplicaFaultReason, ReplicaId, ReplicaIdentity, ReplicaMetadata,
    ReplicaMetadataDecodeError, ReplicaMetadataFile, ReplicaMetadataFileError,
    ReplicaMetadataLoadError, ReplicaNode, ReplicaNodeStatus, ReplicaPaths, ReplicaRole,
    ReplicaStartupValidationError,
};

fn test_path(name: &str, extension: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("allocdb-replica-{name}-{nanos}.{extension}"))
}

fn metadata_path(name: &str) -> PathBuf {
    test_path(name, "replica")
}

fn snapshot_path(name: &str) -> PathBuf {
    test_path(name, "snapshot")
}

fn wal_path(name: &str) -> PathBuf {
    test_path(name, "wal")
}

fn core_config() -> Config {
    Config {
        shard_id: 0,
        max_resources: 8,
        max_reservations: 8,
        max_operations: 16,
        max_ttl_slots: 16,
        max_client_retry_window_slots: 8,
        reservation_history_window_slots: 4,
        max_expiration_bucket_len: 8,
    }
}

fn engine_config() -> EngineConfig {
    EngineConfig {
        max_submission_queue: 2,
        max_command_bytes: 512,
        max_expirations_per_tick: 1,
    }
}

fn identity() -> ReplicaIdentity {
    ReplicaIdentity {
        replica_id: ReplicaId(1),
        shard_id: 0,
    }
}

fn create(resource_id: u128, operation_id: u128) -> ClientRequest {
    ClientRequest {
        operation_id: OperationId(operation_id),
        client_id: ClientId(7),
        command: Command::CreateResource {
            resource_id: ResourceId(resource_id),
        },
    }
}

fn reserve(
    resource_id: u128,
    operation_id: u128,
    holder_id: u128,
    ttl_slots: u64,
) -> ClientRequest {
    ClientRequest {
        operation_id: OperationId(operation_id),
        client_id: ClientId(7),
        command: Command::Reserve {
            resource_id: ResourceId(resource_id),
            holder_id: HolderId(holder_id),
            ttl_slots,
        },
    }
}

fn remove_if_exists(path: &PathBuf) {
    match fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => panic!("failed to remove {}: {error}", path.display()),
    }
}

fn cleanup(paths: &ReplicaPaths) {
    remove_if_exists(&paths.metadata_path);
    remove_if_exists(&paths.snapshot_path);
    remove_if_exists(&paths.wal_path);
}

fn base_metadata() -> ReplicaMetadata {
    ReplicaMetadata {
        identity: identity(),
        current_view: 0,
        role: ReplicaRole::Backup,
        commit_lsn: None,
        active_snapshot_lsn: None,
        last_normal_view: None,
        durable_vote: None,
    }
}

fn assert_faulted(node: &ReplicaNode, expected_reason: ReplicaFaultReason) {
    assert_eq!(
        node.status(),
        ReplicaNodeStatus::Faulted(crate::replica::ReplicaFault {
            reason: expected_reason,
        })
    );
    assert!(node.engine().is_none());
}

#[test]
fn replica_metadata_file_round_trips() {
    let path = metadata_path("metadata-roundtrip");
    let file = ReplicaMetadataFile::new(&path);
    let expected = ReplicaMetadata {
        identity: identity(),
        current_view: 4,
        role: ReplicaRole::Backup,
        commit_lsn: Some(Lsn(17)),
        active_snapshot_lsn: Some(Lsn(12)),
        last_normal_view: Some(3),
        durable_vote: Some(DurableVote {
            view: 5,
            voted_for: ReplicaId(2),
        }),
    };

    file.write_metadata(&expected).unwrap();
    let loaded = file.load_metadata().unwrap().unwrap();

    assert_eq!(loaded, expected);

    remove_if_exists(&path);
}

#[test]
fn replica_open_bootstraps_missing_metadata() {
    let paths = ReplicaPaths::new(
        metadata_path("bootstrap-open"),
        snapshot_path("bootstrap-open"),
        wal_path("bootstrap-open"),
    );

    let node =
        ReplicaNode::open(core_config(), engine_config(), identity(), paths.clone()).unwrap();

    assert_eq!(node.status(), ReplicaNodeStatus::Active);
    assert!(node.engine().is_some());
    assert_eq!(
        node.metadata(),
        &ReplicaMetadata::bootstrap(identity(), None, None)
    );
    assert!(
        ReplicaMetadataFile::new(node.metadata_path())
            .load_metadata()
            .unwrap()
            .is_some()
    );

    cleanup(&paths);
}

#[test]
fn replica_recover_bootstraps_metadata_from_local_durable_state() {
    let paths = ReplicaPaths::new(
        metadata_path("bootstrap-recover"),
        snapshot_path("bootstrap-recover"),
        wal_path("bootstrap-recover"),
    );
    let mut engine =
        SingleNodeEngine::open(core_config(), engine_config(), &paths.wal_path).unwrap();
    engine.submit(Slot(1), create(11, 1)).unwrap();
    engine.submit(Slot(2), reserve(11, 2, 5, 3)).unwrap();
    let checkpoint = engine.checkpoint(&paths.snapshot_path).unwrap();
    assert_eq!(checkpoint.snapshot_lsn, Some(Lsn(2)));
    drop(engine);

    let node =
        ReplicaNode::recover(core_config(), engine_config(), identity(), paths.clone()).unwrap();

    assert_eq!(node.status(), ReplicaNodeStatus::Active);
    assert_eq!(node.metadata().commit_lsn, Some(Lsn(2)));
    assert_eq!(node.metadata().active_snapshot_lsn, Some(Lsn(2)));
    assert!(node.engine().is_some());

    cleanup(&paths);
}

#[test]
fn replica_open_faults_on_corrupt_metadata_bytes() {
    let paths = ReplicaPaths::new(
        metadata_path("corrupt-metadata"),
        snapshot_path("corrupt-metadata"),
        wal_path("corrupt-metadata"),
    );
    fs::write(&paths.metadata_path, [0x01, 0x02, 0x03]).unwrap();

    let node =
        ReplicaNode::open(core_config(), engine_config(), identity(), paths.clone()).unwrap();

    assert_eq!(
        node.status(),
        ReplicaNodeStatus::Faulted(crate::replica::ReplicaFault {
            reason: ReplicaFaultReason::MetadataLoad(ReplicaMetadataLoadError::Decode(
                ReplicaMetadataDecodeError::BufferTooShort,
            )),
        })
    );
    assert!(node.engine().is_none());
    assert_eq!(node.metadata().role, ReplicaRole::Faulted);

    cleanup(&paths);
}

#[test]
fn replica_open_faults_on_metadata_identity_mismatch() {
    let paths = ReplicaPaths::new(
        metadata_path("identity-mismatch"),
        snapshot_path("identity-mismatch"),
        wal_path("identity-mismatch"),
    );
    ReplicaMetadataFile::new(&paths.metadata_path)
        .write_metadata(&ReplicaMetadata {
            identity: ReplicaIdentity {
                replica_id: ReplicaId(2),
                shard_id: identity().shard_id,
            },
            current_view: 1,
            role: ReplicaRole::Backup,
            commit_lsn: None,
            active_snapshot_lsn: None,
            last_normal_view: None,
            durable_vote: None,
        })
        .unwrap();

    let node =
        ReplicaNode::open(core_config(), engine_config(), identity(), paths.clone()).unwrap();

    assert_eq!(
        node.status(),
        ReplicaNodeStatus::Faulted(crate::replica::ReplicaFault {
            reason: ReplicaFaultReason::Validation(
                ReplicaStartupValidationError::ReplicaIdMismatch {
                    expected: ReplicaId(1),
                    found: ReplicaId(2),
                }
            ),
        })
    );
    assert!(node.engine().is_none());

    cleanup(&paths);
}

#[test]
fn replica_open_faults_on_metadata_shard_mismatch() {
    let paths = ReplicaPaths::new(
        metadata_path("shard-mismatch"),
        snapshot_path("shard-mismatch"),
        wal_path("shard-mismatch"),
    );
    let mut metadata = base_metadata();
    metadata.identity.shard_id = 99;
    ReplicaMetadataFile::new(&paths.metadata_path)
        .write_metadata(&metadata)
        .unwrap();

    let node =
        ReplicaNode::open(core_config(), engine_config(), identity(), paths.clone()).unwrap();

    assert_faulted(
        &node,
        ReplicaFaultReason::Validation(ReplicaStartupValidationError::ShardIdMismatch {
            expected: identity().shard_id,
            found: 99,
        }),
    );

    cleanup(&paths);
}

#[test]
fn replica_open_faults_when_snapshot_has_no_commit_lsn() {
    let paths = ReplicaPaths::new(
        metadata_path("snapshot-without-commit"),
        snapshot_path("snapshot-without-commit"),
        wal_path("snapshot-without-commit"),
    );
    let mut metadata = base_metadata();
    metadata.active_snapshot_lsn = Some(Lsn(3));
    ReplicaMetadataFile::new(&paths.metadata_path)
        .write_metadata(&metadata)
        .unwrap();

    let node =
        ReplicaNode::open(core_config(), engine_config(), identity(), paths.clone()).unwrap();

    assert_faulted(
        &node,
        ReplicaFaultReason::Validation(ReplicaStartupValidationError::SnapshotWithoutCommit {
            active_snapshot_lsn: Lsn(3),
        }),
    );

    cleanup(&paths);
}

#[test]
fn replica_open_faults_when_snapshot_exceeds_commit_lsn() {
    let paths = ReplicaPaths::new(
        metadata_path("snapshot-beyond-commit"),
        snapshot_path("snapshot-beyond-commit"),
        wal_path("snapshot-beyond-commit"),
    );
    let mut metadata = base_metadata();
    metadata.commit_lsn = Some(Lsn(2));
    metadata.active_snapshot_lsn = Some(Lsn(3));
    ReplicaMetadataFile::new(&paths.metadata_path)
        .write_metadata(&metadata)
        .unwrap();

    let node =
        ReplicaNode::open(core_config(), engine_config(), identity(), paths.clone()).unwrap();

    assert_faulted(
        &node,
        ReplicaFaultReason::Validation(ReplicaStartupValidationError::SnapshotBeyondCommit {
            active_snapshot_lsn: Lsn(3),
            commit_lsn: Lsn(2),
        }),
    );

    cleanup(&paths);
}

#[test]
fn replica_open_faults_when_last_normal_view_exceeds_current_view() {
    let paths = ReplicaPaths::new(
        metadata_path("last-normal-view-ahead"),
        snapshot_path("last-normal-view-ahead"),
        wal_path("last-normal-view-ahead"),
    );
    let mut metadata = base_metadata();
    metadata.current_view = 4;
    metadata.last_normal_view = Some(5);
    ReplicaMetadataFile::new(&paths.metadata_path)
        .write_metadata(&metadata)
        .unwrap();

    let node =
        ReplicaNode::open(core_config(), engine_config(), identity(), paths.clone()).unwrap();

    assert_faulted(
        &node,
        ReplicaFaultReason::Validation(ReplicaStartupValidationError::LastNormalViewAhead {
            current_view: 4,
            last_normal_view: 5,
        }),
    );

    cleanup(&paths);
}

#[test]
fn replica_open_faults_when_vote_view_precedes_current_view() {
    let paths = ReplicaPaths::new(
        metadata_path("vote-below-current-view"),
        snapshot_path("vote-below-current-view"),
        wal_path("vote-below-current-view"),
    );
    let mut metadata = base_metadata();
    metadata.current_view = 7;
    metadata.durable_vote = Some(DurableVote {
        view: 6,
        voted_for: ReplicaId(2),
    });
    ReplicaMetadataFile::new(&paths.metadata_path)
        .write_metadata(&metadata)
        .unwrap();

    let node =
        ReplicaNode::open(core_config(), engine_config(), identity(), paths.clone()).unwrap();

    assert_faulted(
        &node,
        ReplicaFaultReason::Validation(
            ReplicaStartupValidationError::DurableVoteBelowCurrentView {
                current_view: 7,
                voted_view: 6,
            },
        ),
    );

    cleanup(&paths);
}

#[test]
fn replica_open_faults_when_vote_view_precedes_last_normal_view() {
    let paths = ReplicaPaths::new(
        metadata_path("vote-below-last-normal"),
        snapshot_path("vote-below-last-normal"),
        wal_path("vote-below-last-normal"),
    );
    let mut metadata = base_metadata();
    metadata.current_view = 7;
    metadata.last_normal_view = Some(6);
    metadata.durable_vote = Some(DurableVote {
        view: 5,
        voted_for: ReplicaId(2),
    });
    ReplicaMetadataFile::new(&paths.metadata_path)
        .write_metadata(&metadata)
        .unwrap();

    let node =
        ReplicaNode::open(core_config(), engine_config(), identity(), paths.clone()).unwrap();

    assert_faulted(
        &node,
        ReplicaFaultReason::Validation(
            ReplicaStartupValidationError::DurableVoteBelowLastNormalView {
                last_normal_view: 6,
                voted_view: 5,
            },
        ),
    );

    cleanup(&paths);
}

#[test]
fn replica_open_faults_when_snapshot_anchor_is_not_local() {
    let paths = ReplicaPaths::new(
        metadata_path("snapshot-anchor-mismatch"),
        snapshot_path("snapshot-anchor-mismatch"),
        wal_path("snapshot-anchor-mismatch"),
    );
    let mut metadata = base_metadata();
    metadata.commit_lsn = Some(Lsn(3));
    metadata.active_snapshot_lsn = Some(Lsn(3));
    ReplicaMetadataFile::new(&paths.metadata_path)
        .write_metadata(&metadata)
        .unwrap();

    let node =
        ReplicaNode::open(core_config(), engine_config(), identity(), paths.clone()).unwrap();

    assert_faulted(
        &node,
        ReplicaFaultReason::Validation(ReplicaStartupValidationError::ActiveSnapshotMismatch {
            metadata_snapshot_lsn: Some(Lsn(3)),
            local_snapshot_lsn: None,
        }),
    );

    cleanup(&paths);
}

#[test]
fn replica_recover_faults_when_local_apply_exceeds_commit_lsn() {
    let paths = ReplicaPaths::new(
        metadata_path("commit-mismatch"),
        snapshot_path("commit-mismatch"),
        wal_path("commit-mismatch"),
    );
    let mut engine =
        SingleNodeEngine::open(core_config(), engine_config(), &paths.wal_path).unwrap();
    engine.submit(Slot(1), create(11, 1)).unwrap();
    engine.submit(Slot(2), reserve(11, 2, 5, 3)).unwrap();
    drop(engine);

    ReplicaMetadataFile::new(&paths.metadata_path)
        .write_metadata(&ReplicaMetadata {
            identity: identity(),
            current_view: 2,
            role: ReplicaRole::Backup,
            commit_lsn: Some(Lsn(1)),
            active_snapshot_lsn: None,
            last_normal_view: Some(2),
            durable_vote: None,
        })
        .unwrap();

    let node =
        ReplicaNode::recover(core_config(), engine_config(), identity(), paths.clone()).unwrap();

    assert_eq!(
        node.status(),
        ReplicaNodeStatus::Faulted(crate::replica::ReplicaFault {
            reason: ReplicaFaultReason::Validation(
                ReplicaStartupValidationError::AppliedLsnExceedsCommitLsn {
                    last_applied_lsn: Lsn(2),
                    commit_lsn: Some(Lsn(1)),
                }
            ),
        })
    );
    assert!(node.engine().is_none());

    cleanup(&paths);
}

#[test]
fn replica_open_faults_when_commit_lsn_is_ahead_of_local_state() {
    let paths = ReplicaPaths::new(
        metadata_path("commit-ahead-of-open-state"),
        snapshot_path("commit-ahead-of-open-state"),
        wal_path("commit-ahead-of-open-state"),
    );
    let mut metadata = base_metadata();
    metadata.commit_lsn = Some(Lsn(1));
    ReplicaMetadataFile::new(&paths.metadata_path)
        .write_metadata(&metadata)
        .unwrap();

    let node =
        ReplicaNode::open(core_config(), engine_config(), identity(), paths.clone()).unwrap();

    assert_faulted(
        &node,
        ReplicaFaultReason::Validation(ReplicaStartupValidationError::AppliedLsnBehindCommitLsn {
            last_applied_lsn: None,
            commit_lsn: Lsn(1),
        }),
    );

    cleanup(&paths);
}

#[test]
fn replica_recover_faults_when_local_apply_lags_commit_lsn() {
    let paths = ReplicaPaths::new(
        metadata_path("commit-ahead-of-recover-state"),
        snapshot_path("commit-ahead-of-recover-state"),
        wal_path("commit-ahead-of-recover-state"),
    );
    let mut engine =
        SingleNodeEngine::open(core_config(), engine_config(), &paths.wal_path).unwrap();
    engine.submit(Slot(1), create(11, 1)).unwrap();
    drop(engine);

    let mut metadata = base_metadata();
    metadata.current_view = 2;
    metadata.commit_lsn = Some(Lsn(2));
    metadata.last_normal_view = Some(2);
    ReplicaMetadataFile::new(&paths.metadata_path)
        .write_metadata(&metadata)
        .unwrap();

    let node =
        ReplicaNode::recover(core_config(), engine_config(), identity(), paths.clone()).unwrap();

    assert_faulted(
        &node,
        ReplicaFaultReason::Validation(ReplicaStartupValidationError::AppliedLsnBehindCommitLsn {
            last_applied_lsn: Some(Lsn(1)),
            commit_lsn: Lsn(2),
        }),
    );

    cleanup(&paths);
}

#[test]
fn replica_metadata_file_rejects_oversized_sidecar() {
    let path = metadata_path("oversized");
    let file = ReplicaMetadataFile::new(&path);
    let oversized =
        usize::try_from(super::MAX_REPLICA_METADATA_BYTES + 1).expect("oversized test fits usize");
    fs::write(&path, vec![0x5a; oversized]).unwrap();

    let error = file.load_metadata().unwrap_err();
    assert!(matches!(
        error,
        ReplicaMetadataFileError::TooLarge {
            file_len,
            max_bytes,
        } if file_len == super::MAX_REPLICA_METADATA_BYTES + 1
            && max_bytes == super::MAX_REPLICA_METADATA_BYTES
    ));

    remove_if_exists(&path);
}

#[test]
fn replica_metadata_file_overwrite_replaces_prior_contents() {
    let paths = ReplicaPaths::new(
        metadata_path("metadata-overwrite"),
        snapshot_path("metadata-overwrite"),
        wal_path("metadata-overwrite"),
    );
    let file = ReplicaMetadataFile::new(&paths.metadata_path);
    let mut valid = base_metadata();
    valid.current_view = 2;
    valid.commit_lsn = Some(Lsn(2));
    let mut invalid = valid;
    invalid.current_view = 4;
    invalid.last_normal_view = Some(5);

    file.write_metadata(&valid).unwrap();
    file.write_metadata(&invalid).unwrap();

    let node =
        ReplicaNode::open(core_config(), engine_config(), identity(), paths.clone()).unwrap();

    assert_faulted(
        &node,
        ReplicaFaultReason::Validation(ReplicaStartupValidationError::LastNormalViewAhead {
            current_view: 4,
            last_normal_view: 5,
        }),
    );

    cleanup(&paths);
}

#[test]
fn replica_recover_honors_persisted_faulted_role() {
    let paths = ReplicaPaths::new(
        metadata_path("persisted-faulted"),
        snapshot_path("persisted-faulted"),
        wal_path("persisted-faulted"),
    );
    ReplicaMetadataFile::new(&paths.metadata_path)
        .write_metadata(&ReplicaMetadata {
            identity: identity(),
            current_view: 3,
            role: ReplicaRole::Faulted,
            commit_lsn: Some(Lsn(4)),
            active_snapshot_lsn: Some(Lsn(4)),
            last_normal_view: Some(3),
            durable_vote: None,
        })
        .unwrap();

    let node =
        ReplicaNode::recover(core_config(), engine_config(), identity(), paths.clone()).unwrap();

    assert_eq!(
        node.status(),
        ReplicaNodeStatus::Faulted(crate::replica::ReplicaFault {
            reason: ReplicaFaultReason::PersistedFaultState,
        })
    );
    assert!(node.engine().is_none());

    cleanup(&paths);
}

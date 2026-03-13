use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use allocdb_core::command::{ClientRequest, Command};
use allocdb_core::config::Config;
use allocdb_core::ids::{ClientId, HolderId, Lsn, OperationId, ResourceId, Slot};

use crate::engine::{EngineConfig, SingleNodeEngine};
use crate::replica::{
    DurableVote, ReplicaFaultReason, ReplicaId, ReplicaIdentity, ReplicaMetadata,
    ReplicaMetadataDecodeError, ReplicaMetadataFile, ReplicaMetadataLoadError, ReplicaNode,
    ReplicaNodeStatus, ReplicaPaths, ReplicaRole, ReplicaStartupValidationError,
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

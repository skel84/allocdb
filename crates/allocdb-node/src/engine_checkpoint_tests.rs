use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use allocdb_core::ResourceState;
use allocdb_core::command::{ClientRequest, Command};
use allocdb_core::config::Config;
use allocdb_core::ids::{ClientId, HolderId, Lsn, OperationId, ReservationId, ResourceId, Slot};
use allocdb_core::snapshot_file::SnapshotFile;
use allocdb_core::wal::{RecordType, ScanStopReason};
use allocdb_core::wal_file::WalFile;

use crate::engine::{CheckpointError, EngineConfig, RecoveryStartupKind, SingleNodeEngine};

fn test_path(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("allocdb-checkpoint-{name}-{nanos}.wal"))
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

fn create(resource_id: u128, operation_id: u128) -> ClientRequest {
    ClientRequest {
        operation_id: OperationId(operation_id),
        client_id: ClientId(7),
        command: Command::CreateResource {
            resource_id: ResourceId(resource_id),
        },
    }
}

fn reserve(resource_id: u128, operation_id: u128, holder_id: u128) -> ClientRequest {
    ClientRequest {
        operation_id: OperationId(operation_id),
        client_id: ClientId(7),
        command: Command::Reserve {
            resource_id: ResourceId(resource_id),
            holder_id: HolderId(holder_id),
            ttl_slots: 3,
        },
    }
}

fn confirm(reservation_id: u128, operation_id: u128, holder_id: u128) -> ClientRequest {
    ClientRequest {
        operation_id: OperationId(operation_id),
        client_id: ClientId(7),
        command: Command::Confirm {
            reservation_id: ReservationId(reservation_id),
            holder_id: HolderId(holder_id),
        },
    }
}

#[test]
fn checkpoint_rejects_queued_submissions() {
    let wal_path = test_path("queue-not-empty");
    let snapshot_path = wal_path.with_extension("snapshot");
    let mut engine = SingleNodeEngine::open(core_config(), engine_config(), &wal_path).unwrap();

    engine.enqueue_client(Slot(1), create(11, 1)).unwrap();
    let error = engine.checkpoint(&snapshot_path).unwrap_err();

    assert!(matches!(
        error,
        CheckpointError::QueueNotEmpty { queue_depth: 1 }
    ));

    let _ = fs::remove_file(wal_path);
    let _ = fs::remove_file(snapshot_path);
}

#[test]
fn checkpoint_rewrites_wal_with_one_checkpoint_overlap() {
    let wal_path = test_path("overlap");
    let snapshot_path = wal_path.with_extension("snapshot");
    let mut engine = SingleNodeEngine::open(core_config(), engine_config(), &wal_path).unwrap();

    engine.submit(Slot(1), create(11, 1)).unwrap();
    let first = engine.checkpoint(&snapshot_path).unwrap();
    assert_eq!(first.snapshot_lsn, Some(Lsn(1)));
    assert_eq!(first.previous_snapshot_lsn, None);
    let first_metrics = engine.metrics(Slot(1));
    assert_eq!(
        first_metrics.recovery.startup_kind,
        RecoveryStartupKind::FreshStart
    );
    assert_eq!(first_metrics.recovery.active_snapshot_lsn, Some(Lsn(1)));

    engine.submit(Slot(2), reserve(11, 2, 5)).unwrap();
    engine.submit(Slot(3), confirm(2, 3, 5)).unwrap();
    let second = engine.checkpoint(&snapshot_path).unwrap();

    assert_eq!(second.snapshot_lsn, Some(Lsn(3)));
    assert_eq!(second.previous_snapshot_lsn, Some(Lsn(1)));
    let second_metrics = engine.metrics(Slot(3));
    assert_eq!(second_metrics.recovery.active_snapshot_lsn, Some(Lsn(3)));

    let wal = WalFile::open(&wal_path, engine_config().max_command_bytes).unwrap();
    let recovered_wal = wal.recover().unwrap();
    let retained: Vec<_> = recovered_wal
        .scan_result
        .frames
        .iter()
        .map(|frame| (frame.lsn, frame.record_type))
        .collect();

    assert_eq!(
        recovered_wal.scan_result.stop_reason,
        ScanStopReason::CleanEof
    );
    assert_eq!(
        retained,
        vec![
            (Lsn(2), RecordType::ClientCommand),
            (Lsn(3), RecordType::ClientCommand),
            (Lsn(3), RecordType::SnapshotMarker),
        ]
    );

    let recovered_engine =
        SingleNodeEngine::recover(core_config(), engine_config(), &snapshot_path, &wal_path)
            .unwrap();
    assert_eq!(
        recovered_engine
            .db()
            .resource(ResourceId(11))
            .unwrap()
            .current_state,
        ResourceState::Confirmed
    );

    fs::remove_file(wal_path).unwrap();
    fs::remove_file(snapshot_path).unwrap();
}

#[test]
fn recovery_survives_new_snapshot_before_wal_rewrite() {
    let wal_path = test_path("snapshot-before-rewrite");
    let snapshot_path = wal_path.with_extension("snapshot");
    let mut engine = SingleNodeEngine::open(core_config(), engine_config(), &wal_path).unwrap();

    engine.submit(Slot(1), create(11, 1)).unwrap();
    engine.checkpoint(&snapshot_path).unwrap();
    engine.submit(Slot(2), reserve(11, 2, 5)).unwrap();
    engine.submit(Slot(3), confirm(2, 3, 5)).unwrap();

    let snapshot_file = SnapshotFile::new(&snapshot_path);
    snapshot_file
        .write_snapshot(&engine.db().snapshot())
        .unwrap();
    drop(engine);

    let recovered =
        SingleNodeEngine::recover(core_config(), engine_config(), &snapshot_path, &wal_path)
            .unwrap();

    assert_eq!(
        recovered
            .db()
            .resource(ResourceId(11))
            .unwrap()
            .current_state,
        ResourceState::Confirmed
    );

    fs::remove_file(wal_path).unwrap();
    fs::remove_file(snapshot_path).unwrap();
}

use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use allocdb_core::command::{ClientRequest, Command};
use allocdb_core::config::Config;
use allocdb_core::ids::{ClientId, HolderId, Lsn, OperationId, ReservationId, ResourceId, Slot};
use allocdb_core::snapshot::Snapshot;
use allocdb_core::snapshot_file::SnapshotFile;
use allocdb_core::{ReservationState, SlotOverflowKind};

use crate::engine::{
    EngineConfig, EngineOpenError, RecoverEngineError, SingleNodeEngine, SubmissionError,
    SubmissionErrorCategory,
};

fn wal_path(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("allocdb-engine-{name}-{nanos}.wal"))
}

fn snapshot_path(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("allocdb-engine-{name}-{nanos}.snapshot"))
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

fn write_snapshot(
    path: &PathBuf,
    last_applied_lsn: Option<Lsn>,
    last_request_slot: Option<Slot>,
    config: &Config,
) {
    SnapshotFile::new(path)
        .write_snapshot(&Snapshot {
            last_applied_lsn,
            last_request_slot,
            max_retired_reservation_id: None,
            resources: Vec::new(),
            reservations: Vec::new(),
            operations: Vec::new(),
            wheel: vec![Vec::new(); config.wheel_len()],
        })
        .unwrap();
}

#[test]
fn submit_rejects_deadline_overflow_before_wal_append() {
    let config = core_config();
    let request_slot = u64::MAX - config.max_ttl_slots + 1;
    let wal_path = wal_path("deadline-overflow");
    let mut engine = SingleNodeEngine::open(config, engine_config(), &wal_path).unwrap();
    engine.submit(Slot(1), create(11, 1)).unwrap();
    let wal_len = fs::metadata(&wal_path).unwrap().len();

    let error = engine
        .submit(
            Slot(request_slot),
            reserve(11, 2, 5, core_config().max_ttl_slots),
        )
        .unwrap_err();

    assert_eq!(error.category(), SubmissionErrorCategory::DefiniteFailure);
    assert!(matches!(
        error,
        SubmissionError::SlotOverflow(error)
            if error.kind == SlotOverflowKind::Deadline
                && error.request_slot == Slot(request_slot)
                && error.delta == core_config().max_ttl_slots
    ));
    assert_eq!(fs::metadata(&wal_path).unwrap().len(), wal_len);
    assert!(
        engine
            .db()
            .reservation(ReservationId(2), Slot(request_slot))
            .is_err()
    );

    fs::remove_file(wal_path).unwrap();
}

#[test]
fn tick_expirations_rejects_history_overflow_before_internal_commit() {
    let config = core_config();
    let current_wall_clock_slot = u64::MAX - config.reservation_history_window_slots + 1;
    let wal_path = wal_path("history-overflow");
    let mut engine = SingleNodeEngine::open(config, engine_config(), &wal_path).unwrap();
    engine.submit(Slot(1), create(11, 1)).unwrap();
    engine.submit(Slot(2), reserve(11, 2, 5, 3)).unwrap();
    let wal_len = fs::metadata(&wal_path).unwrap().len();

    let error = engine
        .tick_expirations(Slot(current_wall_clock_slot))
        .unwrap_err();

    assert_eq!(error.category(), SubmissionErrorCategory::DefiniteFailure);
    assert!(matches!(
        error,
        SubmissionError::SlotOverflow(error)
            if error.kind == SlotOverflowKind::ReservationHistory
                && error.request_slot == Slot(current_wall_clock_slot)
                && error.delta == core_config().reservation_history_window_slots
    ));
    assert_eq!(fs::metadata(&wal_path).unwrap().len(), wal_len);
    assert_eq!(
        engine
            .db()
            .reservation(ReservationId(2), Slot(current_wall_clock_slot))
            .unwrap()
            .state,
        ReservationState::Reserved
    );

    fs::remove_file(wal_path).unwrap();
}

#[test]
fn recover_rejects_exhausted_next_lsn() {
    let config = core_config();
    let wal_path = wal_path("recover-next-lsn");
    let snapshot_path = snapshot_path("recover-next-lsn");
    write_snapshot(&snapshot_path, Some(Lsn(u64::MAX)), Some(Slot(1)), &config);

    let error =
        SingleNodeEngine::recover(config, engine_config(), &snapshot_path, &wal_path).unwrap_err();

    assert!(matches!(
        error,
        RecoverEngineError::EngineOpen(EngineOpenError::NextLsnExhausted {
            last_applied_lsn: Lsn(u64::MAX),
        })
    ));

    fs::remove_file(wal_path).unwrap();
    fs::remove_file(snapshot_path).unwrap();
}

#[test]
fn engine_returns_lsn_exhausted_after_last_representable_commit() {
    let config = core_config();
    let wal_path = wal_path("lsn-exhausted");
    let snapshot_path = snapshot_path("lsn-exhausted");
    write_snapshot(
        &snapshot_path,
        Some(Lsn(u64::MAX - 1)),
        Some(Slot(1)),
        &config,
    );

    let mut engine =
        SingleNodeEngine::recover(config, engine_config(), &snapshot_path, &wal_path).unwrap();

    let first = engine.submit(Slot(1), create(11, 1)).unwrap();
    let retry = engine.submit(Slot(1), create(11, 1)).unwrap();
    let second = engine.submit(Slot(1), create(12, 2)).unwrap_err();

    assert_eq!(first.applied_lsn, Lsn(u64::MAX));
    assert_eq!(retry.applied_lsn, Lsn(u64::MAX));
    assert!(retry.from_retry_cache);
    assert!(matches!(
        second,
        SubmissionError::LsnExhausted {
            last_applied_lsn: Lsn(u64::MAX),
        }
    ));
    assert!(!engine.metrics(Slot(1)).accepting_writes);

    fs::remove_file(wal_path).unwrap();
    fs::remove_file(snapshot_path).unwrap();
}

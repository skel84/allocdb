use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::command::{ClientRequest, Command};
use crate::command_codec::encode_client_request;
use crate::config::Config;
use crate::ids::{ClientId, HolderId, Lsn, OperationId, ResourceId, Slot};
use crate::snapshot::{Snapshot, SnapshotError};
use crate::snapshot_file::SnapshotFile;
use crate::state_machine::{ResourceRecord, ResourceState};
use crate::wal::{Frame, RecordType};
use crate::wal_file::WalFile;

use super::{RecoveryError, ReplayError, recover_allocdb};

fn test_path(name: &str, extension: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("allocdb-{name}-{nanos}.{extension}"))
}

fn config() -> Config {
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

fn client_frame(lsn: u64, request_slot: u64, request: ClientRequest) -> Frame {
    Frame {
        lsn: Lsn(lsn),
        request_slot: Slot(request_slot),
        record_type: RecordType::ClientCommand,
        payload: encode_client_request(request),
    }
}

#[test]
fn recover_allocdb_rejects_non_monotonic_lsn() {
    let wal_path = test_path("recover-non-monotonic-lsn", "wal");
    let snapshot_path = test_path("recover-non-monotonic-lsn", "snapshot");
    let mut wal = WalFile::open(&wal_path, 512).unwrap();

    wal.append_frame(&client_frame(
        2,
        1,
        ClientRequest {
            operation_id: OperationId(1),
            client_id: ClientId(7),
            command: Command::CreateResource {
                resource_id: ResourceId(11),
            },
        },
    ))
    .unwrap();
    wal.append_frame(&client_frame(
        1,
        2,
        ClientRequest {
            operation_id: OperationId(2),
            client_id: ClientId(7),
            command: Command::Reserve {
                resource_id: ResourceId(11),
                holder_id: HolderId(5),
                ttl_slots: 3,
            },
        },
    ))
    .unwrap();
    wal.sync().unwrap();

    let error = recover_allocdb(config(), &SnapshotFile::new(&snapshot_path), &wal).unwrap_err();
    assert!(matches!(
        error,
        RecoveryError::Replay(ReplayError::NonMonotonicLsn {
            previous_lsn: Lsn(2),
            next_lsn: Lsn(1),
        })
    ));

    drop(wal);
    fs::remove_file(wal_path).unwrap();
}

#[test]
fn recover_allocdb_rejects_rewound_request_slot() {
    let wal_path = test_path("recover-rewound-slot", "wal");
    let snapshot_path = test_path("recover-rewound-slot", "snapshot");
    let mut wal = WalFile::open(&wal_path, 512).unwrap();

    wal.append_frame(&client_frame(
        1,
        5,
        ClientRequest {
            operation_id: OperationId(1),
            client_id: ClientId(7),
            command: Command::CreateResource {
                resource_id: ResourceId(11),
            },
        },
    ))
    .unwrap();
    wal.append_frame(&client_frame(
        2,
        4,
        ClientRequest {
            operation_id: OperationId(2),
            client_id: ClientId(7),
            command: Command::Reserve {
                resource_id: ResourceId(11),
                holder_id: HolderId(5),
                ttl_slots: 3,
            },
        },
    ))
    .unwrap();
    wal.sync().unwrap();

    let error = recover_allocdb(config(), &SnapshotFile::new(&snapshot_path), &wal).unwrap_err();
    assert!(matches!(
        error,
        RecoveryError::Replay(ReplayError::RewoundRequestSlot {
            previous_request_slot: Slot(5),
            next_request_slot: Slot(4),
        })
    ));

    drop(wal);
    fs::remove_file(wal_path).unwrap();
}

#[test]
fn recover_allocdb_rejects_semantically_invalid_snapshot() {
    let wal_path = test_path("recover-invalid-snapshot", "wal");
    let snapshot_path = test_path("recover-invalid-snapshot", "snapshot");
    let wal = WalFile::open(&wal_path, 512).unwrap();
    let snapshot_file = SnapshotFile::new(&snapshot_path);

    snapshot_file
        .write_snapshot(&Snapshot {
            last_applied_lsn: None,
            last_request_slot: None,
            max_retired_reservation_id: None,
            resources: vec![
                ResourceRecord {
                    resource_id: ResourceId(11),
                    current_state: ResourceState::Available,
                    current_reservation_id: None,
                    version: 0,
                },
                ResourceRecord {
                    resource_id: ResourceId(11),
                    current_state: ResourceState::Available,
                    current_reservation_id: None,
                    version: 1,
                },
            ],
            reservations: Vec::new(),
            operations: Vec::new(),
            wheel: vec![Vec::new(); config().wheel_len()],
        })
        .unwrap();

    let error = recover_allocdb(config(), &snapshot_file, &wal).unwrap_err();
    assert!(matches!(
        error,
        RecoveryError::Snapshot(SnapshotError::DuplicateResourceId(ResourceId(11)))
    ));

    drop(wal);
    fs::remove_file(wal_path).unwrap();
    fs::remove_file(snapshot_path).unwrap();
}

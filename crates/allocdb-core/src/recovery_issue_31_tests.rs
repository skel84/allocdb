use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::SlotOverflowKind;
use crate::command::{ClientRequest, Command};
use crate::command_codec::encode_client_request;
use crate::config::Config;
use crate::ids::{ClientId, Lsn, OperationId, ResourceId, Slot};
use crate::snapshot_file::SnapshotFile;
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
fn recover_allocdb_rejects_client_slot_overflow_in_replayed_wal() {
    let config = config();
    let request_slot = u64::MAX - config.operation_window_slots() + 1;
    let operation_window_slots = config.operation_window_slots();
    let wal_path = test_path("recover-slot-overflow", "wal");
    let snapshot_path = test_path("recover-slot-overflow", "snapshot");
    let mut wal = WalFile::open(&wal_path, 512).unwrap();

    wal.append_frame(&client_frame(
        1,
        request_slot,
        ClientRequest {
            operation_id: OperationId(1),
            client_id: ClientId(7),
            command: Command::CreateResource {
                resource_id: ResourceId(11),
            },
        },
    ))
    .unwrap();
    wal.sync().unwrap();

    let error = recover_allocdb(config, &SnapshotFile::new(&snapshot_path), &wal).unwrap_err();
    assert!(matches!(
        error,
        RecoveryError::Replay(ReplayError::SlotOverflow {
            lsn: Lsn(1),
            kind: SlotOverflowKind::OperationWindow,
            request_slot: slot,
            delta,
        }) if slot == Slot(request_slot) && delta == operation_window_slots
    ));

    drop(wal);
    fs::remove_file(wal_path).unwrap();
}

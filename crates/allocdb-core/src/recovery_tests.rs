use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::command::{ClientRequest, Command, CommandContext};
use crate::command_codec::{encode_client_request, encode_internal_command};
use crate::config::Config;
use crate::ids::{ClientId, HolderId, Lsn, OperationId, ReservationId, ResourceId, Slot};
use crate::snapshot_file::SnapshotFile;
use crate::state_machine::{AllocDb, ResourceState};
use crate::wal::{DecodeError, Frame, RecordType};
use crate::wal_file::{WalFile, WalFileError};

use super::{RecoveryError, recover_allocdb};

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
        max_bundle_size: 1,
        max_operations: 16,
        max_ttl_slots: 16,
        max_client_retry_window_slots: 8,
        reservation_history_window_slots: 4,
        max_expiration_bucket_len: 8,
    }
}

fn context(lsn: u64, request_slot: u64) -> CommandContext {
    CommandContext {
        lsn: Lsn(lsn),
        request_slot: Slot(request_slot),
    }
}

fn client_frame(lsn: u64, request_slot: u64, request: &ClientRequest) -> Frame {
    Frame {
        lsn: Lsn(lsn),
        request_slot: Slot(request_slot),
        record_type: RecordType::ClientCommand,
        payload: encode_client_request(request),
    }
}

fn internal_frame(lsn: u64, request_slot: u64, command: &Command) -> Frame {
    Frame {
        lsn: Lsn(lsn),
        request_slot: Slot(request_slot),
        record_type: RecordType::InternalCommand,
        payload: encode_internal_command(command),
    }
}

#[test]
fn recover_allocdb_replays_wal_without_snapshot() {
    let wal_path = test_path("recover-no-snapshot", "wal");
    let snapshot_path = test_path("recover-no-snapshot", "snapshot");

    let mut live = AllocDb::new(config()).unwrap();
    let mut wal = WalFile::open(&wal_path, 512).unwrap();
    let create = ClientRequest {
        operation_id: OperationId(1),
        client_id: ClientId(7),
        command: Command::CreateResource {
            resource_id: ResourceId(11),
        },
    };
    let reserve = ClientRequest {
        operation_id: OperationId(2),
        client_id: ClientId(7),
        command: Command::Reserve {
            resource_id: ResourceId(11),
            holder_id: HolderId(5),
            ttl_slots: 3,
        },
    };

    live.apply_client(context(1, 1), create.clone());
    live.apply_client(context(2, 2), reserve.clone());
    wal.append_frame(&client_frame(1, 1, &create)).unwrap();
    wal.append_frame(&client_frame(2, 2, &reserve)).unwrap();
    wal.sync().unwrap();

    let snapshot_file = SnapshotFile::new(&snapshot_path);
    let recovered = recover_allocdb(config(), &snapshot_file, &wal).unwrap();

    assert!(!recovered.loaded_snapshot);
    assert_eq!(recovered.loaded_snapshot_lsn, None);
    assert_eq!(recovered.replayed_wal_frame_count, 2);
    assert_eq!(recovered.replayed_wal_last_lsn, Some(Lsn(2)));
    assert_eq!(recovered.db.snapshot(), live.snapshot());

    drop(wal);
    fs::remove_file(wal_path).unwrap();
}

#[test]
fn recover_allocdb_skips_frames_covered_by_snapshot() {
    let wal_path = test_path("recover-with-snapshot", "wal");
    let snapshot_path = test_path("recover-with-snapshot", "snapshot");
    let mut wal = WalFile::open(&wal_path, 512).unwrap();
    let snapshot_file = SnapshotFile::new(&snapshot_path);

    let create = ClientRequest {
        operation_id: OperationId(1),
        client_id: ClientId(7),
        command: Command::CreateResource {
            resource_id: ResourceId(11),
        },
    };
    let reserve = ClientRequest {
        operation_id: OperationId(2),
        client_id: ClientId(7),
        command: Command::Reserve {
            resource_id: ResourceId(11),
            holder_id: HolderId(5),
            ttl_slots: 3,
        },
    };
    let confirm = ClientRequest {
        operation_id: OperationId(3),
        client_id: ClientId(7),
        command: Command::Confirm {
            reservation_id: ReservationId(2),
            holder_id: HolderId(5),
            lease_epoch: 1,
        },
    };

    let mut live = AllocDb::new(config()).unwrap();
    live.apply_client(context(1, 1), create.clone());
    snapshot_file.write_snapshot(&live.snapshot()).unwrap();
    live.apply_client(context(2, 2), reserve.clone());
    live.apply_client(context(3, 2), confirm.clone());

    wal.append_frame(&client_frame(1, 1, &create)).unwrap();
    wal.append_frame(&client_frame(2, 2, &reserve)).unwrap();
    wal.append_frame(&client_frame(3, 2, &confirm)).unwrap();
    wal.sync().unwrap();

    let recovered = recover_allocdb(config(), &snapshot_file, &wal).unwrap();

    assert!(recovered.loaded_snapshot);
    assert_eq!(recovered.loaded_snapshot_lsn, Some(Lsn(1)));
    assert_eq!(recovered.replayed_wal_frame_count, 2);
    assert_eq!(recovered.replayed_wal_last_lsn, Some(Lsn(3)));
    assert_eq!(recovered.db.snapshot(), live.snapshot());

    drop(wal);
    fs::remove_file(wal_path).unwrap();
    fs::remove_file(snapshot_path).unwrap();
}

#[test]
fn recover_allocdb_truncates_torn_tail() {
    let wal_path = test_path("recover-torn-tail", "wal");
    let snapshot_path = test_path("recover-torn-tail", "snapshot");
    let mut wal = WalFile::open(&wal_path, 512).unwrap();

    let create = ClientRequest {
        operation_id: OperationId(1),
        client_id: ClientId(7),
        command: Command::CreateResource {
            resource_id: ResourceId(11),
        },
    };
    let reserve = ClientRequest {
        operation_id: OperationId(2),
        client_id: ClientId(7),
        command: Command::Reserve {
            resource_id: ResourceId(11),
            holder_id: HolderId(5),
            ttl_slots: 3,
        },
    };

    wal.append_frame(&client_frame(1, 1, &create)).unwrap();
    let torn = client_frame(2, 2, &reserve).encode();
    fs::OpenOptions::new()
        .append(true)
        .open(&wal_path)
        .unwrap()
        .write_all(&torn[..torn.len() - 2])
        .unwrap();

    let recovered = recover_allocdb(config(), &SnapshotFile::new(&snapshot_path), &wal).unwrap();

    assert!(!recovered.loaded_snapshot);
    assert_eq!(recovered.recovered_wal.scan_result.frames.len(), 1);
    assert_eq!(recovered.replayed_wal_frame_count, 1);
    assert_eq!(recovered.replayed_wal_last_lsn, Some(Lsn(1)));
    assert_eq!(
        recovered.db.resource(ResourceId(11)).unwrap().current_state,
        ResourceState::Available
    );

    drop(wal);
    fs::remove_file(wal_path).unwrap();
}

#[test]
fn recover_allocdb_marks_empty_snapshot_as_loaded() {
    let wal_path = test_path("recover-empty-snapshot", "wal");
    let snapshot_path = test_path("recover-empty-snapshot", "snapshot");
    let wal = WalFile::open(&wal_path, 512).unwrap();
    let snapshot_file = SnapshotFile::new(&snapshot_path);
    snapshot_file
        .write_snapshot(&AllocDb::new(config()).unwrap().snapshot())
        .unwrap();

    let recovered = recover_allocdb(config(), &snapshot_file, &wal).unwrap();

    assert!(recovered.loaded_snapshot);
    assert_eq!(recovered.loaded_snapshot_lsn, None);
    assert_eq!(recovered.replayed_wal_frame_count, 0);
    assert_eq!(recovered.replayed_wal_last_lsn, None);

    drop(wal);
    fs::remove_file(wal_path).unwrap();
    fs::remove_file(snapshot_path).unwrap();
}

#[test]
fn recover_allocdb_replays_internal_commands() {
    let wal_path = test_path("recover-internal", "wal");
    let snapshot_path = test_path("recover-internal", "snapshot");
    let mut wal = WalFile::open(&wal_path, 512).unwrap();

    let create = ClientRequest {
        operation_id: OperationId(1),
        client_id: ClientId(7),
        command: Command::CreateResource {
            resource_id: ResourceId(11),
        },
    };
    let reserve = ClientRequest {
        operation_id: OperationId(2),
        client_id: ClientId(7),
        command: Command::Reserve {
            resource_id: ResourceId(11),
            holder_id: HolderId(5),
            ttl_slots: 3,
        },
    };
    let expire = Command::Expire {
        reservation_id: ReservationId(2),
        deadline_slot: Slot(5),
    };

    wal.append_frame(&client_frame(1, 1, &create)).unwrap();
    wal.append_frame(&client_frame(2, 2, &reserve)).unwrap();
    wal.append_frame(&internal_frame(3, 5, &expire)).unwrap();
    wal.sync().unwrap();

    let recovered = recover_allocdb(config(), &SnapshotFile::new(&snapshot_path), &wal).unwrap();

    assert_eq!(recovered.replayed_wal_frame_count, 3);
    assert_eq!(recovered.replayed_wal_last_lsn, Some(Lsn(3)));
    assert_eq!(
        recovered.db.resource(ResourceId(11)).unwrap().current_state,
        ResourceState::Available
    );

    drop(wal);
    fs::remove_file(wal_path).unwrap();
}

#[test]
fn recover_allocdb_fails_closed_on_mid_log_corruption() {
    let wal_path = test_path("recover-mid-log-corruption", "wal");
    let snapshot_path = test_path("recover-mid-log-corruption", "snapshot");
    let mut wal = WalFile::open(&wal_path, 512).unwrap();

    let create = ClientRequest {
        operation_id: OperationId(1),
        client_id: ClientId(7),
        command: Command::CreateResource {
            resource_id: ResourceId(11),
        },
    };
    let reserve = ClientRequest {
        operation_id: OperationId(2),
        client_id: ClientId(7),
        command: Command::Reserve {
            resource_id: ResourceId(11),
            holder_id: HolderId(5),
            ttl_slots: 3,
        },
    };

    wal.append_frame(&client_frame(1, 1, &create)).unwrap();
    wal.append_frame(&client_frame(2, 2, &reserve)).unwrap();
    wal.sync().unwrap();

    let mut bytes = fs::read(&wal_path).unwrap();
    let first_len = client_frame(1, 1, &create).encode().len();
    let last_index = bytes.len() - 1;
    bytes[last_index] ^= 0xff;
    fs::write(&wal_path, bytes).unwrap();

    let error = recover_allocdb(config(), &SnapshotFile::new(&snapshot_path), &wal).unwrap_err();

    assert!(matches!(
        error,
        RecoveryError::WalFile(WalFileError::Corruption {
            offset,
            error: DecodeError::InvalidChecksum,
        }) if offset == first_len
    ));

    drop(wal);
    fs::remove_file(wal_path).unwrap();
}

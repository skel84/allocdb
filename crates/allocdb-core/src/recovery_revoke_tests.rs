use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::command::ClientRequest;
use crate::command_codec::encode_client_request;
use crate::config::Config;
use crate::ids::{ClientId, HolderId, Lsn, OperationId, ReservationId, ResourceId, Slot};
use crate::snapshot_file::SnapshotFile;
use crate::state_machine::{ReservationState, ResourceState};
use crate::wal::{Frame, RecordType};
use crate::wal_file::WalFile;

use super::recover_allocdb;

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

fn client_frame(lsn: u64, request_slot: u64, request: &ClientRequest) -> Frame {
    Frame {
        lsn: Lsn(lsn),
        request_slot: Slot(request_slot),
        record_type: RecordType::ClientCommand,
        payload: encode_client_request(request),
    }
}

#[test]
fn recover_allocdb_preserves_revoking_state() {
    let wal_path = test_path("recover-revoking", "wal");
    let snapshot_path = test_path("recover-revoking", "snapshot");
    let mut wal = WalFile::open(&wal_path, 512).unwrap();

    let create = ClientRequest {
        operation_id: OperationId(1),
        client_id: ClientId(7),
        command: crate::command::Command::CreateResource {
            resource_id: ResourceId(11),
        },
    };
    let reserve = ClientRequest {
        operation_id: OperationId(2),
        client_id: ClientId(7),
        command: crate::command::Command::Reserve {
            resource_id: ResourceId(11),
            holder_id: HolderId(5),
            ttl_slots: 3,
        },
    };
    let confirm = ClientRequest {
        operation_id: OperationId(3),
        client_id: ClientId(7),
        command: crate::command::Command::Confirm {
            reservation_id: ReservationId(2),
            holder_id: HolderId(5),
            lease_epoch: 1,
        },
    };
    let revoke = ClientRequest {
        operation_id: OperationId(4),
        client_id: ClientId(7),
        command: crate::command::Command::Revoke {
            reservation_id: ReservationId(2),
        },
    };

    wal.append_frame(&client_frame(1, 1, &create)).unwrap();
    wal.append_frame(&client_frame(2, 2, &reserve)).unwrap();
    wal.append_frame(&client_frame(3, 2, &confirm)).unwrap();
    wal.append_frame(&client_frame(4, 2, &revoke)).unwrap();
    wal.sync().unwrap();

    let recovered =
        recover_allocdb(config(), &SnapshotFile::new(&snapshot_path), &mut wal).unwrap();
    let reservation = recovered.db.reservation(ReservationId(2), Slot(2)).unwrap();
    let resource = recovered.db.resource(ResourceId(11)).unwrap();

    assert_eq!(reservation.state, ReservationState::Revoking);
    assert_eq!(reservation.lease_epoch, 2);
    assert_eq!(reservation.released_lsn, None);
    assert_eq!(resource.current_state, ResourceState::Revoking);
    assert_eq!(resource.current_reservation_id, Some(ReservationId(2)));

    drop(wal);
    fs::remove_file(wal_path).unwrap();
}

#[test]
fn recover_allocdb_preserves_revoked_state() {
    let wal_path = test_path("recover-revoked", "wal");
    let snapshot_path = test_path("recover-revoked", "snapshot");
    let mut wal = WalFile::open(&wal_path, 512).unwrap();

    let create = ClientRequest {
        operation_id: OperationId(1),
        client_id: ClientId(7),
        command: crate::command::Command::CreateResource {
            resource_id: ResourceId(11),
        },
    };
    let reserve = ClientRequest {
        operation_id: OperationId(2),
        client_id: ClientId(7),
        command: crate::command::Command::Reserve {
            resource_id: ResourceId(11),
            holder_id: HolderId(5),
            ttl_slots: 3,
        },
    };
    let confirm = ClientRequest {
        operation_id: OperationId(3),
        client_id: ClientId(7),
        command: crate::command::Command::Confirm {
            reservation_id: ReservationId(2),
            holder_id: HolderId(5),
            lease_epoch: 1,
        },
    };
    let revoke = ClientRequest {
        operation_id: OperationId(4),
        client_id: ClientId(7),
        command: crate::command::Command::Revoke {
            reservation_id: ReservationId(2),
        },
    };
    let reclaim = ClientRequest {
        operation_id: OperationId(5),
        client_id: ClientId(7),
        command: crate::command::Command::Reclaim {
            reservation_id: ReservationId(2),
        },
    };

    wal.append_frame(&client_frame(1, 1, &create)).unwrap();
    wal.append_frame(&client_frame(2, 2, &reserve)).unwrap();
    wal.append_frame(&client_frame(3, 2, &confirm)).unwrap();
    wal.append_frame(&client_frame(4, 2, &revoke)).unwrap();
    wal.append_frame(&client_frame(5, 3, &reclaim)).unwrap();
    wal.sync().unwrap();

    let recovered =
        recover_allocdb(config(), &SnapshotFile::new(&snapshot_path), &mut wal).unwrap();
    let reservation = recovered.db.reservation(ReservationId(2), Slot(5)).unwrap();
    let resource = recovered.db.resource(ResourceId(11)).unwrap();

    assert_eq!(reservation.state, ReservationState::Revoked);
    assert_eq!(reservation.lease_epoch, 2);
    assert_eq!(reservation.released_lsn, Some(Lsn(5)));
    assert_eq!(reservation.retire_after_slot, Some(Slot(7)));
    assert_eq!(resource.current_state, ResourceState::Available);
    assert_eq!(resource.current_reservation_id, None);

    drop(wal);
    fs::remove_file(wal_path).unwrap();
}

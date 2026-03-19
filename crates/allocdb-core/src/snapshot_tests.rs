use crate::command::{ClientRequest, Command, CommandContext};
use crate::config::Config;
use crate::ids::{ClientId, HolderId, Lsn, OperationId, ReservationId, ResourceId, Slot};
use crate::result::ResultCode;
use crate::snapshot::{Snapshot, SnapshotError};
use crate::state_machine::{AllocDb, OperationRecord, ReservationLookupError};

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

fn empty_snapshot() -> Snapshot {
    Snapshot {
        last_applied_lsn: None,
        last_request_slot: None,
        max_retired_reservation_id: None,
        resources: Vec::new(),
        reservations: Vec::new(),
        reservation_members: Vec::new(),
        operations: Vec::new(),
        wheel: vec![Vec::new(); config().wheel_len()],
    }
}

#[test]
fn snapshot_round_trips_allocator_state() {
    let mut db = AllocDb::new(config()).unwrap();
    db.apply_client(
        context(1, 1),
        ClientRequest {
            operation_id: OperationId(1),
            client_id: ClientId(7),
            command: Command::CreateResource {
                resource_id: ResourceId(11),
            },
        },
    );
    db.apply_client(
        context(2, 2),
        ClientRequest {
            operation_id: OperationId(2),
            client_id: ClientId(7),
            command: Command::Reserve {
                resource_id: ResourceId(11),
                holder_id: HolderId(5),
                ttl_slots: 3,
            },
        },
    );

    let snapshot = db.snapshot();
    let encoded = snapshot.encode();
    let decoded = Snapshot::decode(&encoded).unwrap();
    let restored = AllocDb::from_snapshot(config(), decoded).unwrap();

    assert_eq!(restored.snapshot(), snapshot);
}

#[test]
fn snapshot_round_trips_bundle_state() {
    let mut config = config();
    config.max_bundle_size = 2;
    let mut db = AllocDb::new(config.clone()).unwrap();
    db.apply_client(
        context(1, 1),
        ClientRequest {
            operation_id: OperationId(1),
            client_id: ClientId(7),
            command: Command::CreateResource {
                resource_id: ResourceId(11),
            },
        },
    );
    db.apply_client(
        context(2, 1),
        ClientRequest {
            operation_id: OperationId(2),
            client_id: ClientId(7),
            command: Command::CreateResource {
                resource_id: ResourceId(12),
            },
        },
    );
    db.apply_client(
        context(3, 2),
        ClientRequest {
            operation_id: OperationId(3),
            client_id: ClientId(7),
            command: Command::ReserveBundle {
                resource_ids: vec![ResourceId(11), ResourceId(12)],
                holder_id: HolderId(5),
                ttl_slots: 3,
            },
        },
    );

    let snapshot = db.snapshot();
    let encoded = snapshot.encode();
    let decoded = Snapshot::decode(&encoded).unwrap();
    let restored = AllocDb::from_snapshot(config, decoded).unwrap();

    assert_eq!(restored.snapshot(), snapshot);
}

#[test]
fn snapshot_decode_rejects_corruption() {
    let mut bytes = empty_snapshot().encode();
    bytes[0] = 0;

    assert!(matches!(
        Snapshot::decode(&bytes),
        Err(SnapshotError::InvalidMagic(_))
    ));
}

#[test]
fn from_snapshot_rejects_wheel_size_mismatch() {
    let snapshot = Snapshot {
        wheel: vec![Vec::new(); 1],
        ..empty_snapshot()
    };

    let restored = AllocDb::from_snapshot(config(), snapshot);
    assert!(matches!(restored, Err(SnapshotError::InvalidLayout)));
}

#[test]
fn snapshot_restores_retired_lookup_watermark() {
    let mut db = AllocDb::new(config()).unwrap();
    db.apply_client(
        context(1, 1),
        ClientRequest {
            operation_id: OperationId(1),
            client_id: ClientId(7),
            command: Command::CreateResource {
                resource_id: ResourceId(11),
            },
        },
    );
    db.apply_client(
        context(2, 2),
        ClientRequest {
            operation_id: OperationId(2),
            client_id: ClientId(7),
            command: Command::Reserve {
                resource_id: ResourceId(11),
                holder_id: HolderId(5),
                ttl_slots: 3,
            },
        },
    );
    db.apply_client(
        context(3, 3),
        ClientRequest {
            operation_id: OperationId(3),
            client_id: ClientId(7),
            command: Command::Release {
                reservation_id: ReservationId(2),
                holder_id: HolderId(5),
            },
        },
    );
    db.apply_client(
        context(4, 8),
        ClientRequest {
            operation_id: OperationId(4),
            client_id: ClientId(7),
            command: Command::CreateResource {
                resource_id: ResourceId(12),
            },
        },
    );

    assert_eq!(
        db.reservation(ReservationId(2), Slot(8)),
        Err(ReservationLookupError::Retired)
    );

    let restored =
        AllocDb::from_snapshot(config(), Snapshot::decode(&db.snapshot().encode()).unwrap())
            .unwrap();

    assert_eq!(
        restored.reservation(ReservationId(2), Slot(8)),
        Err(ReservationLookupError::Retired)
    );
}

#[test]
fn snapshot_decode_accepts_legacy_v1_layout() {
    let snapshots = [
        Snapshot { ..empty_snapshot() },
        Snapshot {
            last_applied_lsn: Some(Lsn(7)),
            ..empty_snapshot()
        },
        Snapshot {
            last_request_slot: Some(Slot(11)),
            ..empty_snapshot()
        },
        Snapshot {
            last_applied_lsn: Some(Lsn(7)),
            last_request_slot: Some(Slot(11)),
            ..empty_snapshot()
        },
    ];

    for snapshot in snapshots {
        let mut bytes = snapshot.encode();
        bytes[4..6].copy_from_slice(&1_u16.to_le_bytes());
        let removal_index = 8
            + encoded_optional_u64_len(snapshot.last_applied_lsn.map(Lsn::get))
            + encoded_optional_u64_len(snapshot.last_request_slot.map(Slot::get));
        bytes.remove(removal_index);
        let reservation_member_count_index = removal_index + 8;
        bytes.drain(reservation_member_count_index..reservation_member_count_index + 4);

        assert_eq!(Snapshot::decode(&bytes).unwrap(), snapshot);
    }
}

fn encoded_optional_u64_len(value: Option<u64>) -> usize {
    if value.is_some() { 9 } else { 1 }
}

#[test]
fn snapshot_round_trips_slot_overflow_operation_result() {
    let snapshot = Snapshot {
        last_applied_lsn: Some(Lsn(7)),
        last_request_slot: Some(Slot(9)),
        operations: vec![OperationRecord {
            operation_id: OperationId(1),
            command_fingerprint: 42,
            result_code: ResultCode::SlotOverflow,
            result_reservation_id: None,
            result_deadline_slot: None,
            applied_lsn: Lsn(7),
            retire_after_slot: Slot(12),
        }],
        ..empty_snapshot()
    };

    let encoded = snapshot.encode();
    let decoded = Snapshot::decode(&encoded).unwrap();

    assert_eq!(decoded, snapshot);
}

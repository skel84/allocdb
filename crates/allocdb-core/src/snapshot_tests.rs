use crate::command::{ClientRequest, Command, CommandContext};
use crate::config::Config;
use crate::ids::{ClientId, HolderId, Lsn, OperationId, ReservationId, ResourceId, Slot};
use crate::snapshot::{Snapshot, SnapshotError};
use crate::state_machine::{
    AllocDb, AllocDbInvariantError, OperationRecord, ReservationRecord, ReservationState,
    ResourceRecord, ResourceState,
};

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

fn context(lsn: u64, request_slot: u64) -> CommandContext {
    CommandContext {
        lsn: Lsn(lsn),
        request_slot: Slot(request_slot),
    }
}

fn available_resource(resource_id: u128) -> ResourceRecord {
    ResourceRecord {
        resource_id: ResourceId(resource_id),
        current_state: ResourceState::Available,
        current_reservation_id: None,
        version: 0,
    }
}

fn expired_reservation(reservation_id: u128) -> ReservationRecord {
    let reservation_lsn = u64::try_from(reservation_id).expect("test reservation ids fit in u64");

    ReservationRecord {
        reservation_id: ReservationId(reservation_id),
        resource_id: ResourceId(11),
        holder_id: HolderId(5),
        state: ReservationState::Expired,
        created_lsn: Lsn(reservation_lsn),
        deadline_slot: Slot(5),
        released_lsn: Some(Lsn(reservation_lsn + 1)),
        retire_after_slot: Some(Slot(7)),
    }
}

fn operation_record(operation_id: u128) -> OperationRecord {
    let operation_lsn = u64::try_from(operation_id).expect("test operation ids fit in u64");

    OperationRecord {
        operation_id: OperationId(operation_id),
        command_fingerprint: operation_id + 10,
        result_code: crate::result::ResultCode::Ok,
        result_reservation_id: None,
        result_deadline_slot: None,
        applied_lsn: Lsn(operation_lsn),
        retire_after_slot: Slot(operation_lsn + 8),
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
fn snapshot_decode_rejects_corruption() {
    let mut bytes = Snapshot {
        last_applied_lsn: None,
        last_request_slot: None,
        resources: Vec::new(),
        reservations: Vec::new(),
        operations: Vec::new(),
        wheel: vec![Vec::new(); config().wheel_len()],
    }
    .encode();
    bytes[0] = 0;

    assert!(matches!(
        Snapshot::decode(&bytes),
        Err(SnapshotError::InvalidMagic(_))
    ));
}

#[test]
fn from_snapshot_rejects_wheel_size_mismatch() {
    let snapshot = Snapshot {
        last_applied_lsn: None,
        last_request_slot: None,
        resources: Vec::new(),
        reservations: Vec::new(),
        operations: Vec::new(),
        wheel: vec![Vec::new(); 1],
    };

    let restored = AllocDb::from_snapshot(config(), snapshot);
    assert!(matches!(restored, Err(SnapshotError::InvalidLayout)));
}

#[test]
fn from_snapshot_rejects_duplicate_resource_ids() {
    let snapshot = Snapshot {
        last_applied_lsn: None,
        last_request_slot: None,
        resources: vec![
            available_resource(11),
            ResourceRecord {
                version: 1,
                ..available_resource(11)
            },
        ],
        reservations: Vec::new(),
        operations: Vec::new(),
        wheel: vec![Vec::new(); config().wheel_len()],
    };

    let restored = AllocDb::from_snapshot(config(), snapshot);
    assert!(matches!(
        restored,
        Err(SnapshotError::DuplicateResourceId(ResourceId(11)))
    ));
}

#[test]
fn from_snapshot_rejects_resource_table_over_capacity() {
    let snapshot = Snapshot {
        last_applied_lsn: None,
        last_request_slot: None,
        resources: (0..=config().max_resources)
            .map(|offset| available_resource(u128::from(offset) + 1))
            .collect(),
        reservations: Vec::new(),
        operations: Vec::new(),
        wheel: vec![Vec::new(); config().wheel_len()],
    };

    let restored = AllocDb::from_snapshot(config(), snapshot);
    assert!(matches!(
        restored,
        Err(SnapshotError::ResourceTableOverCapacity { count: 9, max: 8 })
    ));
}

#[test]
fn from_snapshot_rejects_missing_active_reservation_reference() {
    let snapshot = Snapshot {
        last_applied_lsn: None,
        last_request_slot: None,
        resources: vec![ResourceRecord {
            resource_id: ResourceId(11),
            current_state: ResourceState::Reserved,
            current_reservation_id: Some(ReservationId(77)),
            version: 1,
        }],
        reservations: Vec::new(),
        operations: Vec::new(),
        wheel: vec![Vec::new(); config().wheel_len()],
    };

    let restored = AllocDb::from_snapshot(config(), snapshot);
    assert!(matches!(
        restored,
        Err(SnapshotError::Invariant(
            AllocDbInvariantError::ActiveResourceMissingReservation {
                resource_id: ResourceId(11),
                reservation_id: ReservationId(77),
            }
        ))
    ));
}

#[test]
fn from_snapshot_rejects_terminal_reservation_without_retirement() {
    let snapshot = Snapshot {
        last_applied_lsn: None,
        last_request_slot: None,
        resources: vec![ResourceRecord {
            version: 2,
            ..available_resource(11)
        }],
        reservations: vec![ReservationRecord {
            retire_after_slot: None,
            ..expired_reservation(2)
        }],
        operations: Vec::new(),
        wheel: vec![Vec::new(); config().wheel_len()],
    };

    let restored = AllocDb::from_snapshot(config(), snapshot);
    assert!(matches!(
        restored,
        Err(SnapshotError::Invariant(
            AllocDbInvariantError::TerminalReservationMissingRetireAfterSlot {
                reservation_id: ReservationId(2),
                reservation_state: ReservationState::Expired,
            }
        ))
    ));
}

#[test]
fn from_snapshot_rejects_duplicate_reservation_ids() {
    let snapshot = Snapshot {
        last_applied_lsn: None,
        last_request_slot: None,
        resources: vec![available_resource(11)],
        reservations: vec![
            expired_reservation(2),
            ReservationRecord {
                holder_id: HolderId(8),
                ..expired_reservation(2)
            },
        ],
        operations: Vec::new(),
        wheel: vec![Vec::new(); config().wheel_len()],
    };

    let restored = AllocDb::from_snapshot(config(), snapshot);
    assert!(matches!(
        restored,
        Err(SnapshotError::DuplicateReservationId(ReservationId(2)))
    ));
}

#[test]
fn from_snapshot_rejects_reservation_table_over_capacity() {
    let snapshot = Snapshot {
        last_applied_lsn: None,
        last_request_slot: None,
        resources: vec![available_resource(11)],
        reservations: (0..=config().max_reservations)
            .map(|offset| expired_reservation(u128::from(offset) + 1))
            .collect(),
        operations: Vec::new(),
        wheel: vec![Vec::new(); config().wheel_len()],
    };

    let restored = AllocDb::from_snapshot(config(), snapshot);
    assert!(matches!(
        restored,
        Err(SnapshotError::ReservationTableOverCapacity { count: 9, max: 8 })
    ));
}

#[test]
fn from_snapshot_rejects_duplicate_operation_ids() {
    let snapshot = Snapshot {
        last_applied_lsn: None,
        last_request_slot: None,
        resources: Vec::new(),
        reservations: Vec::new(),
        operations: vec![
            operation_record(4),
            OperationRecord {
                command_fingerprint: 99,
                ..operation_record(4)
            },
        ],
        wheel: vec![Vec::new(); config().wheel_len()],
    };

    let restored = AllocDb::from_snapshot(config(), snapshot);
    assert!(matches!(
        restored,
        Err(SnapshotError::DuplicateOperationId(OperationId(4)))
    ));
}

#[test]
fn from_snapshot_rejects_operation_table_over_capacity() {
    let snapshot = Snapshot {
        last_applied_lsn: None,
        last_request_slot: None,
        resources: Vec::new(),
        reservations: Vec::new(),
        operations: (0..=config().max_operations)
            .map(|offset| operation_record(u128::from(offset) + 1))
            .collect(),
        wheel: vec![Vec::new(); config().wheel_len()],
    };

    let restored = AllocDb::from_snapshot(config(), snapshot);
    assert!(matches!(
        restored,
        Err(SnapshotError::OperationTableOverCapacity { count: 17, max: 16 })
    ));
}

#[test]
fn from_snapshot_rejects_inconsistent_progress_watermarks() {
    let snapshot = Snapshot {
        last_applied_lsn: Some(Lsn(3)),
        last_request_slot: None,
        resources: Vec::new(),
        reservations: Vec::new(),
        operations: Vec::new(),
        wheel: vec![Vec::new(); config().wheel_len()],
    };

    let restored = AllocDb::from_snapshot(config(), snapshot);
    assert!(matches!(
        restored,
        Err(SnapshotError::InconsistentWatermarks {
            last_applied_lsn: Some(Lsn(3)),
            last_request_slot: None,
        })
    ));
}

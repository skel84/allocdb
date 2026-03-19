use crate::config::Config;
use crate::ids::{HolderId, Lsn, OperationId, ReservationId, ResourceId, Slot};
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
        max_bundle_size: 1,
        max_operations: 16,
        max_ttl_slots: 16,
        max_client_retry_window_slots: 8,
        reservation_history_window_slots: 4,
        max_expiration_bucket_len: 8,
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
        lease_epoch: 2,
        state: ReservationState::Expired,
        created_lsn: Lsn(reservation_lsn),
        deadline_slot: Slot(5),
        released_lsn: Some(Lsn(reservation_lsn + 1)),
        retire_after_slot: Some(Slot(7)),
        member_count: 1,
    }
}

fn operation_record(operation_id: u128) -> OperationRecord {
    let operation_lsn = u64::try_from(operation_id).expect("test operation ids fit in u64");

    OperationRecord {
        operation_id: OperationId(operation_id),
        command_fingerprint: operation_id + 10,
        result_code: crate::result::ResultCode::Ok,
        result_reservation_id: None,
        result_lease_epoch: None,
        result_deadline_slot: None,
        applied_lsn: Lsn(operation_lsn),
        retire_after_slot: Slot(operation_lsn + 8),
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
fn from_snapshot_rejects_duplicate_resource_ids() {
    let snapshot = Snapshot {
        resources: vec![
            available_resource(11),
            ResourceRecord {
                version: 1,
                ..available_resource(11)
            },
        ],
        ..empty_snapshot()
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
        resources: (0..=config().max_resources)
            .map(|offset| available_resource(u128::from(offset) + 1))
            .collect(),
        ..empty_snapshot()
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
        resources: vec![ResourceRecord {
            resource_id: ResourceId(11),
            current_state: ResourceState::Reserved,
            current_reservation_id: Some(ReservationId(77)),
            version: 1,
        }],
        ..empty_snapshot()
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
        resources: vec![ResourceRecord {
            version: 2,
            ..available_resource(11)
        }],
        reservations: vec![ReservationRecord {
            retire_after_slot: None,
            ..expired_reservation(2)
        }],
        ..empty_snapshot()
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
        resources: vec![available_resource(11)],
        reservations: vec![
            expired_reservation(2),
            ReservationRecord {
                holder_id: HolderId(8),
                ..expired_reservation(2)
            },
        ],
        ..empty_snapshot()
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
        resources: vec![available_resource(11)],
        reservations: (0..=config().max_reservations)
            .map(|offset| expired_reservation(u128::from(offset) + 1))
            .collect(),
        ..empty_snapshot()
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
        operations: vec![
            operation_record(4),
            OperationRecord {
                command_fingerprint: 99,
                ..operation_record(4)
            },
        ],
        ..empty_snapshot()
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
        operations: (0..=config().max_operations)
            .map(|offset| operation_record(u128::from(offset) + 1))
            .collect(),
        ..empty_snapshot()
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
        ..empty_snapshot()
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

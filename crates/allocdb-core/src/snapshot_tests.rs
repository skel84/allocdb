use crate::command::{ClientRequest, Command, CommandContext};
use crate::config::Config;
use crate::ids::{ClientId, HolderId, Lsn, OperationId, ReservationId, ResourceId, Slot};
use crate::snapshot::{Snapshot, SnapshotError};
use crate::state_machine::{
    AllocDb, AllocDbInvariantError, ReservationState, ResourceRecord, ResourceState,
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
            .map(|offset| ResourceRecord {
                resource_id: ResourceId(u128::from(offset) + 1),
                current_state: ResourceState::Available,
                current_reservation_id: None,
                version: 0,
            })
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
            resource_id: ResourceId(11),
            current_state: ResourceState::Available,
            current_reservation_id: None,
            version: 2,
        }],
        reservations: vec![crate::state_machine::ReservationRecord {
            reservation_id: ReservationId(2),
            resource_id: ResourceId(11),
            holder_id: HolderId(5),
            state: ReservationState::Expired,
            created_lsn: Lsn(2),
            deadline_slot: Slot(5),
            released_lsn: Some(Lsn(3)),
            retire_after_slot: None,
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

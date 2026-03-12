use crate::command::{ClientRequest, Command, CommandContext};
use crate::config::Config;
use crate::ids::{ClientId, HolderId, Lsn, OperationId, ReservationId, ResourceId, Slot};
use crate::state_machine::{AllocDb, ReservationLookupError, ReservationState};

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

fn create(resource_id: u128) -> ClientRequest {
    ClientRequest {
        operation_id: OperationId(resource_id),
        client_id: ClientId(7),
        command: Command::CreateResource {
            resource_id: ResourceId(resource_id),
        },
    }
}

#[test]
fn retired_reservation_lookup_survives_unrelated_later_write() {
    let mut db = AllocDb::new(config()).unwrap();
    db.apply_client(context(1, 1), create(11));
    db.apply_client(
        context(2, 2),
        ClientRequest {
            operation_id: OperationId(2),
            client_id: ClientId(7),
            command: Command::Reserve {
                resource_id: ResourceId(11),
                holder_id: HolderId(1),
                ttl_slots: 3,
            },
        },
    );
    db.apply_client(
        context(3, 2),
        ClientRequest {
            operation_id: OperationId(3),
            client_id: ClientId(7),
            command: Command::Release {
                reservation_id: ReservationId(2),
                holder_id: HolderId(1),
            },
        },
    );

    assert_eq!(
        db.reservation(ReservationId(2), Slot(6)).unwrap().state,
        ReservationState::Released
    );

    db.apply_client(context(4, 7), create(12));

    assert_eq!(
        db.reservation(ReservationId(2), Slot(7)),
        Err(ReservationLookupError::Retired)
    );
}

#[test]
fn reservation_lookup_keeps_unknown_future_ids_not_found() {
    let mut db = AllocDb::new(config()).unwrap();
    db.apply_client(context(1, 1), create(11));
    db.apply_client(
        context(2, 2),
        ClientRequest {
            operation_id: OperationId(2),
            client_id: ClientId(7),
            command: Command::Reserve {
                resource_id: ResourceId(11),
                holder_id: HolderId(1),
                ttl_slots: 3,
            },
        },
    );
    db.apply_client(
        context(3, 2),
        ClientRequest {
            operation_id: OperationId(3),
            client_id: ClientId(7),
            command: Command::Release {
                reservation_id: ReservationId(2),
                holder_id: HolderId(1),
            },
        },
    );
    db.apply_client(context(4, 7), create(12));

    assert_eq!(
        db.reservation(ReservationId(99), Slot(7)),
        Err(ReservationLookupError::NotFound)
    );
}

#[test]
fn reservation_lookup_conservatively_marks_older_ids_retired() {
    // IDs below the retired watermark are conservatively reported as Retired,
    // even if they were never actually created. This bounds memory usage while
    // preserving the "never flips to NotFound after Retired" guarantee.
    let mut db = AllocDb::new(config()).unwrap();
    db.apply_client(context(1, 1), create(11));
    db.apply_client(
        context(2, 2),
        ClientRequest {
            operation_id: OperationId(2),
            client_id: ClientId(7),
            command: Command::Reserve {
                resource_id: ResourceId(11),
                holder_id: HolderId(1),
                ttl_slots: 3,
            },
        },
    );
    db.apply_client(
        context(3, 2),
        ClientRequest {
            operation_id: OperationId(3),
            client_id: ClientId(7),
            command: Command::Release {
                reservation_id: ReservationId(2),
                holder_id: HolderId(1),
            },
        },
    );
    db.apply_client(context(4, 7), create(12));

    assert_eq!(
        db.reservation(ReservationId(1), Slot(7)),
        Err(ReservationLookupError::Retired)
    );
}

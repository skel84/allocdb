use crate::command::{ClientRequest, Command, CommandContext};
use crate::config::{Config, ConfigError};
use crate::ids::{ClientId, HolderId, Lsn, OperationId, ReservationId, ResourceId, Slot};
use crate::result::ResultCode;

use crate::state_machine::{AllocDb, ReservationState, ResourceState};

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
fn config_validation_rejects_invalid_history_window() {
    let mut invalid = config();
    invalid.reservation_history_window_slots = invalid.max_ttl_slots + 1;
    assert_eq!(invalid.validate(), Err(ConfigError::HistoryWindowTooLarge));
}

#[test]
fn create_resource_is_idempotent_with_same_operation_id() {
    let mut db = AllocDb::new(config()).unwrap();

    let first = db.apply_client(context(1, 1), create(11));
    let second = db.apply_client(context(2, 1), create(11));

    assert_eq!(first.result_code, ResultCode::Ok);
    assert_eq!(second.result_code, ResultCode::Ok);
    assert_eq!(db.resources.len(), 1);
}

#[test]
fn create_resource_rejects_conflicting_reuse_of_operation_id() {
    let mut db = AllocDb::new(config()).unwrap();

    let first = db.apply_client(context(1, 1), create(11));
    let conflicting = db.apply_client(
        context(2, 1),
        ClientRequest {
            operation_id: OperationId(11),
            client_id: ClientId(7),
            command: Command::CreateResource {
                resource_id: ResourceId(22),
            },
        },
    );

    assert_eq!(first.result_code, ResultCode::Ok);
    assert_eq!(conflicting.result_code, ResultCode::OperationConflict);
    assert!(db.resource(ResourceId(22)).is_none());
}

#[test]
fn reserve_assigns_deterministic_reservation_id_and_deadline() {
    let mut db = AllocDb::new(config()).unwrap();
    db.apply_client(context(1, 1), create(11));

    let outcome = db.apply_client(
        context(2, 10),
        ClientRequest {
            operation_id: OperationId(2),
            client_id: ClientId(7),
            command: Command::Reserve {
                resource_id: ResourceId(11),
                holder_id: HolderId(99),
                ttl_slots: 3,
            },
        },
    );

    assert_eq!(outcome.result_code, ResultCode::Ok);
    assert_eq!(outcome.reservation_id, Some(ReservationId(2)));
    assert_eq!(outcome.deadline_slot, Some(Slot(13)));

    let resource = db.resource(ResourceId(11)).unwrap();
    assert_eq!(resource.current_state, ResourceState::Reserved);
    assert_eq!(resource.current_reservation_id, Some(ReservationId(2)));
}

#[test]
fn reserve_rejects_busy_resource() {
    let mut db = AllocDb::new(config()).unwrap();
    db.apply_client(context(1, 1), create(11));
    let first = db.apply_client(
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
    let second = db.apply_client(
        context(3, 2),
        ClientRequest {
            operation_id: OperationId(3),
            client_id: ClientId(8),
            command: Command::Reserve {
                resource_id: ResourceId(11),
                holder_id: HolderId(2),
                ttl_slots: 3,
            },
        },
    );

    assert_eq!(first.result_code, ResultCode::Ok);
    assert_eq!(second.result_code, ResultCode::ResourceBusy);
}

#[test]
fn confirm_requires_matching_holder() {
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

    let wrong_holder = db.apply_client(
        context(3, 2),
        ClientRequest {
            operation_id: OperationId(3),
            client_id: ClientId(7),
            command: Command::Confirm {
                reservation_id: ReservationId(2),
                holder_id: HolderId(2),
            },
        },
    );
    let correct_holder = db.apply_client(
        context(4, 2),
        ClientRequest {
            operation_id: OperationId(4),
            client_id: ClientId(7),
            command: Command::Confirm {
                reservation_id: ReservationId(2),
                holder_id: HolderId(1),
            },
        },
    );

    assert_eq!(wrong_holder.result_code, ResultCode::HolderMismatch);
    assert_eq!(correct_holder.result_code, ResultCode::Ok);
    assert_eq!(
        db.resource(ResourceId(11)).unwrap().current_state,
        ResourceState::Confirmed
    );
}

#[test]
fn release_returns_resource_to_available_and_retains_history() {
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

    let outcome = db.apply_client(
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

    assert_eq!(outcome.result_code, ResultCode::Ok);
    assert_eq!(
        db.resource(ResourceId(11)).unwrap().current_state,
        ResourceState::Available
    );
    assert_eq!(
        db.reservation(ReservationId(2), Slot(5)).unwrap().state,
        ReservationState::Released
    );
}

#[test]
fn expire_is_noop_after_confirm() {
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
            command: Command::Confirm {
                reservation_id: ReservationId(2),
                holder_id: HolderId(1),
            },
        },
    );

    let outcome = db.apply_internal(
        context(4, 5),
        Command::Expire {
            reservation_id: ReservationId(2),
            deadline_slot: Slot(5),
        },
    );

    assert_eq!(outcome.result_code, ResultCode::Noop);
    assert_eq!(
        db.resource(ResourceId(11)).unwrap().current_state,
        ResourceState::Confirmed
    );
}

#[test]
fn expire_releases_reserved_resource() {
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

    let outcome = db.apply_internal(
        context(3, 5),
        Command::Expire {
            reservation_id: ReservationId(2),
            deadline_slot: Slot(5),
        },
    );

    assert_eq!(outcome.result_code, ResultCode::Ok);
    assert_eq!(
        db.resource(ResourceId(11)).unwrap().current_state,
        ResourceState::Available
    );
    assert_eq!(
        db.reservation(ReservationId(2), Slot(5)).unwrap().state,
        ReservationState::Expired
    );
}

#[test]
fn operation_dedupe_expires_after_window() {
    let mut db = AllocDb::new(config()).unwrap();

    let first = db.apply_client(context(1, 1), create(11));
    assert_eq!(first.result_code, ResultCode::Ok);
    assert!(db.operation(OperationId(11), Slot(1)).is_some());

    db.apply_client(context(2, 30), create(12));

    assert!(db.operation(OperationId(11), Slot(30)).is_none());
}

#[test]
fn resource_table_capacity_fails_fast() {
    let mut config = config();
    config.max_resources = 1;
    let mut db = AllocDb::new(config).unwrap();

    let first = db.apply_client(context(1, 1), create(11));
    let second = db.apply_client(context(2, 1), create(12));

    assert_eq!(first.result_code, ResultCode::Ok);
    assert_eq!(second.result_code, ResultCode::ResourceTableFull);
}

#[test]
fn expiration_bucket_capacity_fails_fast() {
    let mut config = config();
    config.max_expiration_bucket_len = 1;
    let mut db = AllocDb::new(config).unwrap();
    db.apply_client(context(1, 1), create(11));
    db.apply_client(context(2, 1), create(12));

    let first = db.apply_client(
        context(3, 2),
        ClientRequest {
            operation_id: OperationId(3),
            client_id: ClientId(7),
            command: Command::Reserve {
                resource_id: ResourceId(11),
                holder_id: HolderId(1),
                ttl_slots: 3,
            },
        },
    );
    let second = db.apply_client(
        context(4, 2),
        ClientRequest {
            operation_id: OperationId(4),
            client_id: ClientId(7),
            command: Command::Reserve {
                resource_id: ResourceId(12),
                holder_id: HolderId(2),
                ttl_slots: 3,
            },
        },
    );

    assert_eq!(first.result_code, ResultCode::Ok);
    assert_eq!(second.result_code, ResultCode::ExpirationIndexFull);
}

#[test]
fn due_reservations_are_bucketed_by_deadline_slot() {
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

    assert_eq!(db.due_reservations(Slot(5)), &[ReservationId(2)]);
}

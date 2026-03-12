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

fn create(resource_id: u128, operation_id: u128) -> ClientRequest {
    ClientRequest {
        operation_id: OperationId(operation_id),
        client_id: ClientId(7),
        command: Command::CreateResource {
            resource_id: ResourceId(resource_id),
        },
    }
}

#[test]
fn config_validation_rejects_operation_window_overflow() {
    let mut invalid = config();
    invalid.max_ttl_slots = u64::MAX - 1;
    invalid.max_client_retry_window_slots = 2;

    assert_eq!(
        invalid.validate(),
        Err(ConfigError::OperationWindowTooLarge)
    );
}

#[test]
fn create_rejects_operation_window_overflow_without_advancing_progress() {
    let config = config();
    let request_slot = u64::MAX - config.operation_window_slots() + 1;
    let mut db = AllocDb::new(config).unwrap();

    let outcome = db.apply_client(context(1, request_slot), create(11, 1));

    assert_eq!(outcome.result_code, ResultCode::SlotOverflow);
    assert!(db.resource(ResourceId(11)).is_none());
    assert!(db.operation(OperationId(1), Slot(request_slot)).is_none());
    assert_eq!(db.last_applied_lsn(), None);
    assert_eq!(db.last_request_slot(), None);
}

#[test]
fn reserve_rejects_large_request_slot_without_mutating_resource() {
    let config = config();
    let request_slot = u64::MAX - config.max_ttl_slots + 1;
    let mut db = AllocDb::new(config).unwrap();
    db.apply_client(context(1, 1), create(11, 1));

    let outcome = db.apply_client(
        context(2, request_slot),
        ClientRequest {
            operation_id: OperationId(2),
            client_id: ClientId(7),
            command: Command::Reserve {
                resource_id: ResourceId(11),
                holder_id: HolderId(5),
                ttl_slots: 16,
            },
        },
    );

    assert_eq!(outcome.result_code, ResultCode::SlotOverflow);
    assert_eq!(
        db.resource(ResourceId(11)).unwrap().current_state,
        ResourceState::Available
    );
    assert!(
        db.reservation(ReservationId(2), Slot(request_slot))
            .is_err()
    );
    assert_eq!(db.last_applied_lsn(), Some(Lsn(1)));
    assert_eq!(db.last_request_slot(), Some(Slot(1)));
}

#[test]
fn internal_expire_rejects_history_window_overflow_without_mutating_state() {
    let config = config();
    let request_slot = u64::MAX - config.reservation_history_window_slots + 1;
    let mut db = AllocDb::new(config).unwrap();
    db.apply_client(context(1, 1), create(11, 1));
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

    let outcome = db.apply_internal(
        context(3, request_slot),
        Command::Expire {
            reservation_id: ReservationId(2),
            deadline_slot: Slot(5),
        },
    );

    assert_eq!(outcome.result_code, ResultCode::SlotOverflow);
    assert_eq!(
        db.resource(ResourceId(11)).unwrap().current_state,
        ResourceState::Reserved
    );
    assert_eq!(
        db.reservation(ReservationId(2), Slot(5)).unwrap().state,
        ReservationState::Reserved
    );
    assert_eq!(db.last_applied_lsn(), Some(Lsn(2)));
    assert_eq!(db.last_request_slot(), Some(Slot(2)));
}

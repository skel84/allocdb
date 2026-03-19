use crate::command::{ClientRequest, Command, CommandContext};
use crate::config::Config;
use crate::ids::{ClientId, HolderId, Lsn, OperationId, ReservationId, ResourceId, Slot};
use crate::result::ResultCode;
use crate::state_machine::{AllocDb, ResourceState};

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
fn stale_confirm_cannot_confirm_a_newer_reservation() {
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
                ttl_slots: 1,
            },
        },
    );
    db.apply_internal(
        context(3, 3),
        Command::Expire {
            reservation_id: ReservationId(2),
            deadline_slot: Slot(3),
        },
    );
    db.apply_client(
        context(4, 4),
        ClientRequest {
            operation_id: OperationId(4),
            client_id: ClientId(8),
            command: Command::Reserve {
                resource_id: ResourceId(11),
                holder_id: HolderId(2),
                ttl_slots: 3,
            },
        },
    );

    let stale_confirm = db.apply_client(
        context(5, 4),
        ClientRequest {
            operation_id: OperationId(5),
            client_id: ClientId(7),
            command: Command::Confirm {
                reservation_id: ReservationId(2),
                holder_id: HolderId(1),
                lease_epoch: 1,
            },
        },
    );

    assert!(matches!(
        stale_confirm.result_code,
        ResultCode::StaleEpoch | ResultCode::ReservationRetired
    ));
    assert_eq!(
        db.resource(ResourceId(11)).unwrap().current_reservation_id,
        Some(ReservationId(4))
    );
    assert_eq!(
        db.resource(ResourceId(11)).unwrap().current_state,
        ResourceState::Reserved
    );
}

#[test]
fn logical_slot_lag_saturates_at_zero() {
    let mut db = AllocDb::new(config()).unwrap();

    assert_eq!(db.logical_slot_lag(Slot(3)), 0);

    db.apply_client(context(1, 5), create(11));

    assert_eq!(db.logical_slot_lag(Slot(3)), 0);
    assert_eq!(db.logical_slot_lag(Slot(9)), 4);
}

#[test]
fn health_metrics_report_due_expiration_backlog() {
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

    let due = db.health_metrics(Slot(5));
    assert_eq!(due.last_applied_lsn, Some(Lsn(2)));
    assert_eq!(due.last_request_slot, Some(Slot(2)));
    assert_eq!(due.logical_slot_lag, 3);
    assert_eq!(due.expiration_backlog, 1);
    assert_eq!(due.operation_table_used, 2);
    assert_eq!(due.operation_table_capacity, 16);
    assert_eq!(due.operation_table_utilization_pct, 12);

    db.apply_internal(
        context(3, 5),
        Command::Expire {
            reservation_id: ReservationId(2),
            deadline_slot: Slot(5),
        },
    );

    let cleared = db.health_metrics(Slot(5));
    assert_eq!(cleared.logical_slot_lag, 0);
    assert_eq!(cleared.expiration_backlog, 0);
    assert_eq!(cleared.operation_table_used, 2);
    assert_eq!(cleared.operation_table_capacity, 16);
    assert_eq!(cleared.operation_table_utilization_pct, 12);
}

#[test]
fn operation_table_utilization_drops_after_retry_window_retirement() {
    let mut db = AllocDb::new(Config {
        shard_id: 0,
        max_resources: 136,
        max_reservations: 1,
        max_bundle_size: 1,
        max_operations: 128,
        max_ttl_slots: 4,
        max_client_retry_window_slots: 4,
        reservation_history_window_slots: 2,
        max_expiration_bucket_len: 1,
    })
    .unwrap();

    for value in 1..=128_u128 {
        db.apply_client(
            context(u64::try_from(value).unwrap(), 1),
            ClientRequest {
                operation_id: OperationId(value),
                client_id: ClientId(7),
                command: Command::CreateResource {
                    resource_id: ResourceId(value),
                },
            },
        );
    }

    let filled = db.health_metrics(Slot(1));
    assert_eq!(filled.operation_table_used, 128);

    let recovered = db.apply_client(
        context(129, 10),
        ClientRequest {
            operation_id: OperationId(129),
            client_id: ClientId(7),
            command: Command::CreateResource {
                resource_id: ResourceId(129),
            },
        },
    );
    assert_eq!(recovered.result_code, ResultCode::Ok);

    let retired = db.health_metrics(Slot(10));
    assert_eq!(retired.operation_table_used, 1);
    assert_eq!(retired.operation_table_capacity, 128);
}

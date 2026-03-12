use crate::command::{ClientRequest, Command, CommandContext};
use crate::config::Config;
use crate::ids::{ClientId, Lsn, OperationId, ResourceId, Slot};
use crate::result::ResultCode;
use crate::state_machine::AllocDb;

fn config() -> Config {
    Config {
        shard_id: 0,
        max_resources: 8,
        max_reservations: 8,
        max_operations: 4,
        max_ttl_slots: 2,
        max_client_retry_window_slots: 0,
        reservation_history_window_slots: 1,
        max_expiration_bucket_len: 8,
    }
}

fn context(lsn: u64, request_slot: u64) -> CommandContext {
    CommandContext {
        lsn: Lsn(lsn),
        request_slot: Slot(request_slot),
    }
}

fn create_with_operation(operation_id: u128, resource_id: u128) -> ClientRequest {
    ClientRequest {
        operation_id: OperationId(operation_id),
        client_id: ClientId(7),
        command: Command::CreateResource {
            resource_id: ResourceId(resource_id),
        },
    }
}

#[test]
fn operation_reuse_conflict_survives_probe_chain_retirement_gap() {
    let mut db = AllocDb::new(config()).unwrap();

    let first = db.apply_client(context(1, 1), create_with_operation(22, 1));
    let second = db.apply_client(context(2, 2), create_with_operation(4, 1));
    let third = db.apply_client(context(3, 3), create_with_operation(26, 1));

    assert_eq!(first.result_code, ResultCode::Ok);
    assert_eq!(second.result_code, ResultCode::AlreadyExists);
    assert_eq!(third.result_code, ResultCode::AlreadyExists);

    let unrelated = db.apply_client(context(4, 4), create_with_operation(1, 2));
    assert_eq!(unrelated.result_code, ResultCode::Ok);

    assert!(db.operation(OperationId(22), Slot(4)).is_none());
    assert_eq!(
        db.operation(OperationId(4), Slot(4))
            .map(|record| record.result_code),
        Some(ResultCode::AlreadyExists)
    );
    assert_eq!(
        db.operation(OperationId(26), Slot(4))
            .map(|record| record.result_code),
        Some(ResultCode::AlreadyExists)
    );

    let conflicting = db.apply_client(context(5, 4), create_with_operation(26, 3));

    assert_eq!(conflicting.result_code, ResultCode::OperationConflict);
    assert!(db.resource(ResourceId(3)).is_none());
}

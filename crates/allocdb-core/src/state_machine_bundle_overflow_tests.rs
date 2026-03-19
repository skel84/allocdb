use crate::command::{ClientRequest, Command, CommandContext};
use crate::config::Config;
use crate::ids::{ClientId, HolderId, Lsn, OperationId, ResourceId, Slot};
use crate::result::ResultCode;
use crate::state_machine::{AllocDb, ResourceState};

fn config() -> Config {
    Config {
        shard_id: 0,
        max_resources: 8,
        max_reservations: 8,
        max_bundle_size: 2,
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
fn reserve_bundle_rejects_deadline_overflow_without_mutating_state() {
    let config = config();
    let request_slot = u64::MAX - config.max_ttl_slots + 1;
    let mut db = AllocDb::new(config).unwrap();
    db.apply_client(context(1, 1), create(11));
    db.apply_client(context(2, 1), create(12));

    let outcome = db.apply_client(
        context(3, request_slot),
        ClientRequest {
            operation_id: OperationId(3),
            client_id: ClientId(7),
            command: Command::ReserveBundle {
                resource_ids: vec![ResourceId(11), ResourceId(12)],
                holder_id: HolderId(5),
                ttl_slots: 16,
            },
        },
    );

    assert_eq!(outcome.result_code, ResultCode::SlotOverflow);
    assert_eq!(db.reservations.len(), 0);
    assert_eq!(db.reservation_members.len(), 0);
    for resource_id in [ResourceId(11), ResourceId(12)] {
        let resource = db.resource(resource_id).unwrap();
        assert_eq!(resource.current_state, ResourceState::Available);
        assert_eq!(resource.current_reservation_id, None);
    }
}

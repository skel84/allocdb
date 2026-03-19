use crate::command::{ClientRequest, Command, CommandContext};
use crate::config::Config;
use crate::ids::{ClientId, HolderId, Lsn, OperationId, ReservationId, ResourceId, Slot};
use crate::result::ResultCode;
use crate::state_machine::{AllocDb, ReservationState, ResourceState};

fn bundle_config() -> Config {
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
fn bundle_confirm_rejects_stale_epoch_without_mutating_members() {
    let mut config = bundle_config();
    config.max_bundle_size = 2;
    let mut db = AllocDb::new(config).unwrap();
    db.apply_client(context(1, 1), create(11));
    db.apply_client(context(2, 1), create(12));
    db.apply_client(
        context(3, 2),
        ClientRequest {
            operation_id: OperationId(3),
            client_id: ClientId(7),
            command: Command::ReserveBundle {
                resource_ids: vec![ResourceId(11), ResourceId(12)],
                holder_id: HolderId(1),
                ttl_slots: 3,
            },
        },
    );

    let stale = db.apply_client(
        context(4, 2),
        ClientRequest {
            operation_id: OperationId(4),
            client_id: ClientId(7),
            command: Command::Confirm {
                reservation_id: ReservationId(3),
                holder_id: HolderId(1),
                lease_epoch: 2,
            },
        },
    );

    assert_eq!(stale.result_code, ResultCode::StaleEpoch);
    assert_eq!(
        db.reservation(ReservationId(3), Slot(5)).unwrap().state,
        ReservationState::Reserved
    );
    assert_eq!(
        db.reservation(ReservationId(3), Slot(5))
            .unwrap()
            .lease_epoch,
        1
    );
    for resource_id in [ResourceId(11), ResourceId(12)] {
        let resource = db.resource(resource_id).unwrap();
        assert_eq!(resource.current_state, ResourceState::Reserved);
        assert_eq!(resource.current_reservation_id, Some(ReservationId(3)));
    }
}

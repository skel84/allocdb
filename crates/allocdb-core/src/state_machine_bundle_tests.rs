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
fn reserve_bundle_acquires_all_resources_atomically() {
    let mut config = bundle_config();
    config.max_bundle_size = 3;
    let mut db = AllocDb::new(config).unwrap();
    db.apply_client(context(1, 1), create(11));
    db.apply_client(context(2, 1), create(12));

    let outcome = db.apply_client(
        context(3, 5),
        ClientRequest {
            operation_id: OperationId(3),
            client_id: ClientId(7),
            command: Command::ReserveBundle {
                resource_ids: vec![ResourceId(11), ResourceId(12)],
                holder_id: HolderId(9),
                ttl_slots: 4,
            },
        },
    );

    assert_eq!(outcome.result_code, ResultCode::Ok);
    assert_eq!(outcome.reservation_id, Some(ReservationId(3)));
    assert_eq!(outcome.deadline_slot, Some(Slot(9)));
    assert_eq!(db.reservations.len(), 1);
    assert_eq!(db.reservation_members.len(), 2);

    let reservation = db.reservation(ReservationId(3), Slot(5)).unwrap();
    assert_eq!(reservation.member_count, 2);
    assert_eq!(reservation.resource_id, ResourceId(11));
    assert_eq!(
        db.resource(ResourceId(11)).unwrap().current_reservation_id,
        Some(ReservationId(3))
    );
    assert_eq!(
        db.resource(ResourceId(12)).unwrap().current_reservation_id,
        Some(ReservationId(3))
    );
}

#[test]
fn reserve_bundle_leaves_no_partial_state_on_conflict() {
    let mut config = bundle_config();
    config.max_bundle_size = 3;
    let mut db = AllocDb::new(config).unwrap();
    db.apply_client(context(1, 1), create(11));
    db.apply_client(context(2, 1), create(12));
    db.apply_client(
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

    let outcome = db.apply_client(
        context(4, 2),
        ClientRequest {
            operation_id: OperationId(4),
            client_id: ClientId(8),
            command: Command::ReserveBundle {
                resource_ids: vec![ResourceId(11), ResourceId(12)],
                holder_id: HolderId(2),
                ttl_slots: 3,
            },
        },
    );

    assert_eq!(outcome.result_code, ResultCode::ResourceBusy);
    assert_eq!(db.reservations.len(), 1);
    assert_eq!(db.reservation_members.len(), 1);
    assert_eq!(
        db.resource(ResourceId(11)).unwrap().current_reservation_id,
        Some(ReservationId(3))
    );
    assert_eq!(
        db.resource(ResourceId(12)).unwrap().current_state,
        ResourceState::Available
    );
    assert_eq!(
        db.resource(ResourceId(12)).unwrap().current_reservation_id,
        None
    );
}

#[test]
fn reserve_bundle_rejects_requests_beyond_configured_limit() {
    let mut config = bundle_config();
    config.max_bundle_size = 2;
    let mut db = AllocDb::new(config).unwrap();
    db.apply_client(context(1, 1), create(11));
    db.apply_client(context(2, 1), create(12));
    db.apply_client(context(3, 1), create(13));

    let outcome = db.apply_client(
        context(4, 2),
        ClientRequest {
            operation_id: OperationId(4),
            client_id: ClientId(7),
            command: Command::ReserveBundle {
                resource_ids: vec![ResourceId(11), ResourceId(12), ResourceId(13)],
                holder_id: HolderId(5),
                ttl_slots: 3,
            },
        },
    );

    assert_eq!(outcome.result_code, ResultCode::BundleTooLarge);
    assert_eq!(db.reservations.len(), 0);
    assert_eq!(db.reservation_members.len(), 0);
}

#[test]
fn confirm_and_release_update_every_bundle_member() {
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

    let confirmed = db.apply_client(
        context(4, 2),
        ClientRequest {
            operation_id: OperationId(4),
            client_id: ClientId(7),
            command: Command::Confirm {
                reservation_id: ReservationId(3),
                holder_id: HolderId(1),
            },
        },
    );
    assert_eq!(confirmed.result_code, ResultCode::Ok);
    assert_eq!(
        db.resource(ResourceId(11)).unwrap().current_state,
        ResourceState::Confirmed
    );
    assert_eq!(
        db.resource(ResourceId(12)).unwrap().current_state,
        ResourceState::Confirmed
    );

    let released = db.apply_client(
        context(5, 2),
        ClientRequest {
            operation_id: OperationId(5),
            client_id: ClientId(7),
            command: Command::Release {
                reservation_id: ReservationId(3),
                holder_id: HolderId(1),
            },
        },
    );
    assert_eq!(released.result_code, ResultCode::Ok);
    assert_eq!(
        db.resource(ResourceId(11)).unwrap().current_state,
        ResourceState::Available
    );
    assert_eq!(
        db.resource(ResourceId(12)).unwrap().current_state,
        ResourceState::Available
    );
    assert_eq!(
        db.reservation(ReservationId(3), Slot(5)).unwrap().state,
        ReservationState::Released
    );
}

#[test]
fn expire_releases_every_bundle_member() {
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

    let outcome = db.apply_internal(
        context(4, 5),
        Command::Expire {
            reservation_id: ReservationId(3),
            deadline_slot: Slot(5),
        },
    );

    assert_eq!(outcome.result_code, ResultCode::Ok);
    assert_eq!(
        db.resource(ResourceId(11)).unwrap().current_state,
        ResourceState::Available
    );
    assert_eq!(
        db.resource(ResourceId(12)).unwrap().current_state,
        ResourceState::Available
    );
    assert_eq!(
        db.reservation(ReservationId(3), Slot(5)).unwrap().state,
        ReservationState::Expired
    );
}

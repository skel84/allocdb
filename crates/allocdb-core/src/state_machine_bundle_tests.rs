use crate::command::{ClientRequest, Command, CommandContext};
use crate::config::Config;
use crate::ids::{ClientId, HolderId, Lsn, OperationId, ReservationId, ResourceId, Slot};
use crate::result::ResultCode;
use crate::state_machine::{AllocDb, ReservationMemberRecord, ReservationState, ResourceState};

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
    assert_eq!(
        db.reservation_member(ReservationId(3), 0).unwrap(),
        ReservationMemberRecord {
            reservation_id: ReservationId(3),
            resource_id: ResourceId(11),
            member_index: 0,
        }
    );
    assert_eq!(
        db.reservation_member(ReservationId(3), 1).unwrap(),
        ReservationMemberRecord {
            reservation_id: ReservationId(3),
            resource_id: ResourceId(12),
            member_index: 1,
        }
    );

    let reservation = db.reservation(ReservationId(3), Slot(5)).unwrap();
    assert_eq!(reservation.member_count, 2);
    assert_eq!(reservation.resource_id, ResourceId(11));
    for resource_id in [ResourceId(11), ResourceId(12)] {
        let resource = db.resource(resource_id).unwrap();
        assert_eq!(resource.current_state, ResourceState::Reserved);
        assert_eq!(resource.current_reservation_id, Some(ReservationId(3)));
    }
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
                resource_ids: vec![ResourceId(12), ResourceId(11)],
                holder_id: HolderId(2),
                ttl_slots: 3,
            },
        },
    );

    assert_eq!(outcome.result_code, ResultCode::ResourceBusy);
    assert_eq!(db.reservations.len(), 1);
    assert_eq!(db.reservation_members.len(), 1);
    assert_eq!(
        db.reservation_member(ReservationId(3), 0).unwrap(),
        ReservationMemberRecord {
            reservation_id: ReservationId(3),
            resource_id: ResourceId(11),
            member_index: 0,
        }
    );
    assert!(db.reservation_member(ReservationId(4), 0).is_none());
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
fn reserve_bundle_rejects_empty_resource_sets() {
    let mut config = bundle_config();
    config.max_bundle_size = 2;
    let mut db = AllocDb::new(config).unwrap();

    let outcome = db.apply_client(
        context(1, 2),
        ClientRequest {
            operation_id: OperationId(1),
            client_id: ClientId(7),
            command: Command::ReserveBundle {
                resource_ids: Vec::new(),
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
fn single_resource_bundle_matches_plain_reserve() {
    let mut config = bundle_config();
    config.max_bundle_size = 2;

    let mut plain = AllocDb::new(config.clone()).unwrap();
    plain.apply_client(context(1, 1), create(11));

    let plain_outcome = plain.apply_client(
        context(2, 5),
        ClientRequest {
            operation_id: OperationId(2),
            client_id: ClientId(7),
            command: Command::Reserve {
                resource_id: ResourceId(11),
                holder_id: HolderId(9),
                ttl_slots: 4,
            },
        },
    );

    let mut bundled = AllocDb::new(config).unwrap();
    bundled.apply_client(context(1, 1), create(11));

    let bundled_outcome = bundled.apply_client(
        context(2, 5),
        ClientRequest {
            operation_id: OperationId(2),
            client_id: ClientId(7),
            command: Command::ReserveBundle {
                resource_ids: vec![ResourceId(11)],
                holder_id: HolderId(9),
                ttl_slots: 4,
            },
        },
    );

    assert_eq!(bundled_outcome, plain_outcome);
    assert_eq!(
        bundled.reservation(ReservationId(2), Slot(5)).unwrap(),
        plain.reservation(ReservationId(2), Slot(5)).unwrap()
    );
    assert_eq!(
        bundled.resource(ResourceId(11)).unwrap(),
        plain.resource(ResourceId(11)).unwrap()
    );
    assert_eq!(
        bundled.reservation_members.len(),
        plain.reservation_members.len()
    );
    assert_eq!(
        bundled.reservation_member(ReservationId(2), 0).unwrap(),
        plain.reservation_member(ReservationId(2), 0).unwrap()
    );
}

#[test]
fn reserve_bundle_rejects_duplicate_resource_ids() {
    let mut config = bundle_config();
    config.max_bundle_size = 2;
    let mut db = AllocDb::new(config).unwrap();
    db.apply_client(context(1, 1), create(11));

    let outcome = db.apply_client(
        context(2, 2),
        ClientRequest {
            operation_id: OperationId(2),
            client_id: ClientId(7),
            command: Command::ReserveBundle {
                resource_ids: vec![ResourceId(11), ResourceId(11)],
                holder_id: HolderId(9),
                ttl_slots: 3,
            },
        },
    );

    assert_eq!(outcome.result_code, ResultCode::ResourceBusy);
    assert_eq!(db.reservations.len(), 0);
    assert_eq!(db.reservation_members.len(), 0);
    assert_eq!(
        db.resource(ResourceId(11)).unwrap().current_state,
        ResourceState::Available
    );
    assert_eq!(
        db.resource(ResourceId(11)).unwrap().current_reservation_id,
        None
    );
}

#[test]
fn reserve_bundle_rejects_mixed_existing_and_missing_resources() {
    let mut config = bundle_config();
    config.max_bundle_size = 2;
    let mut db = AllocDb::new(config).unwrap();
    db.apply_client(context(1, 1), create(11));

    let outcome = db.apply_client(
        context(2, 2),
        ClientRequest {
            operation_id: OperationId(2),
            client_id: ClientId(7),
            command: Command::ReserveBundle {
                resource_ids: vec![ResourceId(11), ResourceId(99)],
                holder_id: HolderId(9),
                ttl_slots: 3,
            },
        },
    );

    assert_eq!(outcome.result_code, ResultCode::ResourceNotFound);
    assert_eq!(db.reservations.len(), 0);
    assert_eq!(db.reservation_members.len(), 0);
    assert_eq!(
        db.resource(ResourceId(11)).unwrap().current_state,
        ResourceState::Available
    );
    assert_eq!(
        db.resource(ResourceId(11)).unwrap().current_reservation_id,
        None
    );
    assert!(db.resource(ResourceId(99)).is_none());
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
    let reservation = db.reservation(ReservationId(3), Slot(5)).unwrap();
    assert_eq!(reservation.state, ReservationState::Confirmed);
    assert_eq!(reservation.holder_id, HolderId(1));
    assert_eq!(reservation.member_count, 2);
    for resource_id in [ResourceId(11), ResourceId(12)] {
        let resource = db.resource(resource_id).unwrap();
        assert_eq!(resource.current_state, ResourceState::Confirmed);
        assert_eq!(resource.current_reservation_id, Some(ReservationId(3)));
    }

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
    for resource_id in [ResourceId(11), ResourceId(12)] {
        let resource = db.resource(resource_id).unwrap();
        assert_eq!(resource.current_state, ResourceState::Available);
        assert_eq!(resource.current_reservation_id, None);
    }
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
    for resource_id in [ResourceId(11), ResourceId(12)] {
        let resource = db.resource(resource_id).unwrap();
        assert_eq!(resource.current_state, ResourceState::Available);
        assert_eq!(resource.current_reservation_id, None);
    }
    assert_eq!(
        db.reservation(ReservationId(3), Slot(5)).unwrap().state,
        ReservationState::Expired
    );
}

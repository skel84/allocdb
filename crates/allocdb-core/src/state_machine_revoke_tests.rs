use crate::command::{ClientRequest, Command, CommandContext};
use crate::config::Config;
use crate::ids::{ClientId, HolderId, Lsn, OperationId, ReservationId, ResourceId, Slot};
use crate::result::ResultCode;
use crate::state_machine::{AllocDb, ReservationState, ResourceState};

fn config() -> Config {
    Config {
        shard_id: 0,
        max_resources: 8,
        max_reservations: 8,
        max_bundle_size: 4,
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

fn reserve_bundle(
    operation_id: u128,
    resource_ids: &[u128],
    holder_id: u128,
    ttl_slots: u64,
) -> ClientRequest {
    ClientRequest {
        operation_id: OperationId(operation_id),
        client_id: ClientId(7),
        command: Command::ReserveBundle {
            resource_ids: resource_ids.iter().copied().map(ResourceId).collect(),
            holder_id: HolderId(holder_id),
            ttl_slots,
        },
    }
}

#[test]
fn revoke_moves_confirmed_bundle_to_revoking_and_bumps_epoch() {
    let mut db = AllocDb::new(config()).unwrap();
    for resource_id in [11, 12] {
        db.apply_client(
            context(resource_id - 10, 1),
            create(u128::from(resource_id)),
        );
    }

    let reserve = db.apply_client(context(3, 2), reserve_bundle(3, &[11, 12], 21, 4));
    assert_eq!(reserve.result_code, ResultCode::Ok);

    let confirm = db.apply_client(
        context(4, 2),
        ClientRequest {
            operation_id: OperationId(4),
            client_id: ClientId(7),
            command: Command::Confirm {
                reservation_id: ReservationId(3),
                holder_id: HolderId(21),
                lease_epoch: 1,
            },
        },
    );
    assert_eq!(confirm.result_code, ResultCode::Ok);

    let revoke = db.apply_client(
        context(5, 2),
        ClientRequest {
            operation_id: OperationId(5),
            client_id: ClientId(7),
            command: Command::Revoke {
                reservation_id: ReservationId(3),
            },
        },
    );
    assert_eq!(revoke.result_code, ResultCode::Ok);

    let reservation = db.reservation(ReservationId(3), Slot(2)).unwrap();
    assert_eq!(reservation.state, ReservationState::Revoking);
    assert_eq!(reservation.lease_epoch, 2);
    assert_eq!(reservation.released_lsn, None);
    assert_eq!(reservation.retire_after_slot, None);

    for resource_id in [11, 12] {
        let resource = db.resource(ResourceId(resource_id)).unwrap();
        assert_eq!(resource.current_state, ResourceState::Revoking);
        assert_eq!(resource.current_reservation_id, Some(ReservationId(3)));
    }
}

#[test]
fn stale_holder_commands_fail_after_revoke_epoch_bump() {
    let mut db = AllocDb::new(config()).unwrap();
    db.apply_client(context(1, 1), create(11));
    db.apply_client(context(2, 2), reserve_bundle(2, &[11], 31, 4));
    db.apply_client(
        context(3, 2),
        ClientRequest {
            operation_id: OperationId(3),
            client_id: ClientId(7),
            command: Command::Confirm {
                reservation_id: ReservationId(2),
                holder_id: HolderId(31),
                lease_epoch: 1,
            },
        },
    );
    db.apply_client(
        context(4, 2),
        ClientRequest {
            operation_id: OperationId(4),
            client_id: ClientId(7),
            command: Command::Revoke {
                reservation_id: ReservationId(2),
            },
        },
    );

    let stale_confirm = db.apply_client(
        context(5, 2),
        ClientRequest {
            operation_id: OperationId(5),
            client_id: ClientId(7),
            command: Command::Confirm {
                reservation_id: ReservationId(2),
                holder_id: HolderId(31),
                lease_epoch: 1,
            },
        },
    );
    assert_eq!(stale_confirm.result_code, ResultCode::StaleEpoch);

    let stale_release = db.apply_client(
        context(6, 2),
        ClientRequest {
            operation_id: OperationId(6),
            client_id: ClientId(7),
            command: Command::Release {
                reservation_id: ReservationId(2),
                holder_id: HolderId(31),
                lease_epoch: 1,
            },
        },
    );
    assert_eq!(stale_release.result_code, ResultCode::StaleEpoch);
}

#[test]
fn reclaim_is_the_only_transition_that_frees_revoked_resources() {
    let mut db = AllocDb::new(config()).unwrap();
    for resource_id in [11, 12] {
        db.apply_client(
            context(resource_id - 10, 1),
            create(u128::from(resource_id)),
        );
    }
    db.apply_client(context(3, 2), reserve_bundle(3, &[11, 12], 41, 4));
    db.apply_client(
        context(4, 2),
        ClientRequest {
            operation_id: OperationId(4),
            client_id: ClientId(7),
            command: Command::Confirm {
                reservation_id: ReservationId(3),
                holder_id: HolderId(41),
                lease_epoch: 1,
            },
        },
    );
    db.apply_client(
        context(5, 2),
        ClientRequest {
            operation_id: OperationId(5),
            client_id: ClientId(7),
            command: Command::Revoke {
                reservation_id: ReservationId(3),
            },
        },
    );

    let early_reuse = db.apply_client(
        context(6, 3),
        ClientRequest {
            operation_id: OperationId(6),
            client_id: ClientId(8),
            command: Command::Reserve {
                resource_id: ResourceId(11),
                holder_id: HolderId(99),
                ttl_slots: 2,
            },
        },
    );
    assert_eq!(early_reuse.result_code, ResultCode::ResourceBusy);

    let reclaim = db.apply_client(
        context(7, 3),
        ClientRequest {
            operation_id: OperationId(7),
            client_id: ClientId(7),
            command: Command::Reclaim {
                reservation_id: ReservationId(3),
            },
        },
    );
    assert_eq!(reclaim.result_code, ResultCode::Ok);

    let reservation = db.reservation(ReservationId(3), Slot(7)).unwrap();
    assert_eq!(reservation.state, ReservationState::Revoked);
    assert_eq!(reservation.lease_epoch, 2);
    assert_eq!(reservation.released_lsn, Some(Lsn(7)));
    assert_eq!(reservation.retire_after_slot, Some(Slot(7)));

    for resource_id in [11, 12] {
        let resource = db.resource(ResourceId(resource_id)).unwrap();
        assert_eq!(resource.current_state, ResourceState::Available);
        assert_eq!(resource.current_reservation_id, None);
    }
}

#[test]
fn reclaim_before_revoke_is_invalid_state() {
    let mut db = AllocDb::new(config()).unwrap();
    db.apply_client(context(1, 1), create(11));
    db.apply_client(context(2, 2), reserve_bundle(2, &[11], 51, 4));
    db.apply_client(
        context(3, 2),
        ClientRequest {
            operation_id: OperationId(3),
            client_id: ClientId(7),
            command: Command::Confirm {
                reservation_id: ReservationId(2),
                holder_id: HolderId(51),
                lease_epoch: 1,
            },
        },
    );

    let reclaim = db.apply_client(
        context(4, 2),
        ClientRequest {
            operation_id: OperationId(4),
            client_id: ClientId(7),
            command: Command::Reclaim {
                reservation_id: ReservationId(2),
            },
        },
    );
    assert_eq!(reclaim.result_code, ResultCode::InvalidState);
}

#[test]
fn duplicate_revoke_and_reclaim_requests_return_cached_results() {
    let mut db = AllocDb::new(config()).unwrap();
    db.apply_client(context(1, 1), create(11));
    db.apply_client(context(2, 2), reserve_bundle(2, &[11], 61, 4));
    db.apply_client(
        context(3, 2),
        ClientRequest {
            operation_id: OperationId(3),
            client_id: ClientId(7),
            command: Command::Confirm {
                reservation_id: ReservationId(2),
                holder_id: HolderId(61),
                lease_epoch: 1,
            },
        },
    );

    let revoke_request = ClientRequest {
        operation_id: OperationId(4),
        client_id: ClientId(7),
        command: Command::Revoke {
            reservation_id: ReservationId(2),
        },
    };
    let first_revoke = db.apply_client(context(4, 2), revoke_request.clone());
    let duplicate_revoke = db.apply_client(context(5, 2), revoke_request);
    assert_eq!(first_revoke, duplicate_revoke);

    let reclaim_request = ClientRequest {
        operation_id: OperationId(5),
        client_id: ClientId(7),
        command: Command::Reclaim {
            reservation_id: ReservationId(2),
        },
    };
    let first_reclaim = db.apply_client(context(6, 3), reclaim_request.clone());
    let duplicate_reclaim = db.apply_client(context(7, 3), reclaim_request);
    assert_eq!(first_reclaim, duplicate_reclaim);
}

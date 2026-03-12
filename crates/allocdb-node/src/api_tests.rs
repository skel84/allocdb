use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use allocdb_core::command::{ClientRequest, Command};
use allocdb_core::config::Config;
use allocdb_core::ids::{ClientId, HolderId, Lsn, OperationId, ReservationId, ResourceId, Slot};
use allocdb_core::result::ResultCode;
use allocdb_core::{ReservationState, ResourceState};

use super::{
    ApiCodecError, ApiRequest, ApiResponse, InvalidRequestReason, MetricsRequest, MetricsResponse,
    ReservationRequest, ReservationResponse, ResourceRequest, ResourceResponse,
    SubmissionFailureCode, SubmitRequest, SubmitResponse, TickExpirationsApplied,
    TickExpirationsRequest, TickExpirationsResponse, decode_request, decode_response,
    encode_request, encode_response,
};
use crate::engine::{
    EngineConfig, PersistFailurePhase, RecoveryStartupKind, SingleNodeEngine,
    SubmissionErrorCategory,
};

fn test_path(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("allocdb-api-{name}-{nanos}.wal"))
}

fn test_snapshot_path(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("allocdb-api-{name}-{nanos}.snapshot"))
}

fn core_config() -> Config {
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

fn engine_config() -> EngineConfig {
    EngineConfig {
        max_submission_queue: 2,
        max_command_bytes: 512,
        max_expirations_per_tick: 1,
    }
}

fn create_request(resource_id: u128, operation_id: u128) -> ClientRequest {
    ClientRequest {
        operation_id: OperationId(operation_id),
        client_id: ClientId(7),
        command: Command::CreateResource {
            resource_id: ResourceId(resource_id),
        },
    }
}

fn reserve_request(resource_id: u128, operation_id: u128, holder_id: u128) -> ClientRequest {
    ClientRequest {
        operation_id: OperationId(operation_id),
        client_id: ClientId(7),
        command: Command::Reserve {
            resource_id: ResourceId(resource_id),
            holder_id: HolderId(holder_id),
            ttl_slots: 3,
        },
    }
}

fn release_request(reservation_id: u128, operation_id: u128, holder_id: u128) -> ClientRequest {
    ClientRequest {
        operation_id: OperationId(operation_id),
        client_id: ClientId(7),
        command: Command::Release {
            reservation_id: ReservationId(reservation_id),
            holder_id: HolderId(holder_id),
        },
    }
}

#[test]
fn submit_request_round_trips_through_wire_codec() {
    let request = ApiRequest::Submit(SubmitRequest::from_client_request(
        Slot(9),
        reserve_request(11, 1, 22),
    ));

    let decoded = super::decode_request(&encode_request(&request).unwrap()).unwrap();

    assert_eq!(decoded, request);
}

#[test]
fn request_codec_round_trips_all_variants() {
    let requests = vec![
        ApiRequest::Submit(SubmitRequest::from_client_request(
            Slot(9),
            reserve_request(11, 1, 22),
        )),
        ApiRequest::GetResource(ResourceRequest {
            resource_id: ResourceId(12),
            required_lsn: Some(Lsn(7)),
        }),
        ApiRequest::GetReservation(ReservationRequest {
            reservation_id: ReservationId(13),
            current_slot: Slot(14),
            required_lsn: None,
        }),
        ApiRequest::GetMetrics(MetricsRequest {
            current_wall_clock_slot: Slot(15),
        }),
        ApiRequest::TickExpirations(TickExpirationsRequest {
            current_wall_clock_slot: Slot(16),
        }),
    ];

    for request in requests {
        let encoded = encode_request(&request).unwrap();
        assert_eq!(decode_request(&encoded).unwrap(), request);
    }
}

#[test]
fn response_codec_round_trips_all_variants() {
    let responses = vec![
        ApiResponse::Submit(SubmitResponse::Committed(super::SubmissionCommitted {
            applied_lsn: Lsn(1),
            outcome: allocdb_core::result::CommandOutcome::new(ResultCode::Ok),
            from_retry_cache: false,
        })),
        ApiResponse::Submit(SubmitResponse::Rejected(super::SubmissionFailure {
            category: SubmissionErrorCategory::DefiniteFailure,
            code: SubmissionFailureCode::InvalidRequest(InvalidRequestReason::InvalidLayout),
        })),
        ApiResponse::GetResource(ResourceResponse::Found(super::ResourceView {
            resource_id: ResourceId(21),
            state: ResourceState::Confirmed,
            current_reservation_id: Some(ReservationId(22)),
            version: 5,
        })),
        ApiResponse::GetResource(ResourceResponse::NotFound),
        ApiResponse::GetResource(ResourceResponse::EngineHalted),
        ApiResponse::GetResource(ResourceResponse::FenceNotApplied {
            required_lsn: Lsn(9),
            last_applied_lsn: Some(Lsn(8)),
        }),
        ApiResponse::GetReservation(ReservationResponse::Found(super::ReservationView {
            reservation_id: ReservationId(31),
            resource_id: ResourceId(32),
            holder_id: HolderId(33),
            state: ReservationState::Released,
            created_lsn: Lsn(2),
            deadline_slot: Slot(7),
            released_lsn: Some(Lsn(3)),
            retire_after_slot: Some(Slot(9)),
        })),
        ApiResponse::GetReservation(ReservationResponse::NotFound),
        ApiResponse::GetReservation(ReservationResponse::Retired),
        ApiResponse::GetReservation(ReservationResponse::EngineHalted),
        ApiResponse::GetReservation(ReservationResponse::FenceNotApplied {
            required_lsn: Lsn(11),
            last_applied_lsn: None,
        }),
        ApiResponse::GetMetrics(MetricsResponse {
            metrics: crate::engine::EngineMetrics {
                queue_depth: 1,
                queue_capacity: 2,
                accepting_writes: false,
                recovery: crate::engine::RecoveryStatus {
                    startup_kind: RecoveryStartupKind::SnapshotOnly,
                    loaded_snapshot_lsn: Some(Lsn(4)),
                    replayed_wal_frame_count: 0,
                    replayed_wal_last_lsn: None,
                    active_snapshot_lsn: Some(Lsn(4)),
                },
                core: allocdb_core::HealthMetrics {
                    last_applied_lsn: Some(Lsn(4)),
                    last_request_slot: Some(Slot(6)),
                    logical_slot_lag: 3,
                    expiration_backlog: 2,
                    operation_table_used: 5,
                    operation_table_capacity: 16,
                    operation_table_utilization_pct: 31,
                },
            },
        }),
        ApiResponse::TickExpirations(TickExpirationsResponse::Applied(TickExpirationsApplied {
            processed_count: 1,
            last_applied_lsn: Some(Lsn(12)),
        })),
        ApiResponse::TickExpirations(TickExpirationsResponse::Rejected(
            super::SubmissionFailure {
                category: SubmissionErrorCategory::Indefinite,
                code: SubmissionFailureCode::EngineHalted,
            },
        )),
    ];

    for response in responses {
        let encoded = encode_response(&response);
        assert_eq!(decode_response(&encoded).unwrap(), response);
    }
}

#[test]
fn api_codec_rejects_truncated_and_trailing_frames() {
    let request = ApiRequest::GetMetrics(MetricsRequest {
        current_wall_clock_slot: Slot(5),
    });
    let mut encoded_request = encode_request(&request).unwrap();
    encoded_request.push(0);
    assert_eq!(
        decode_request(&encoded_request),
        Err(ApiCodecError::InvalidLayout)
    );

    let response = ApiResponse::GetResource(ResourceResponse::NotFound);
    let mut encoded_response = encode_response(&response);
    encoded_response.push(0);
    assert_eq!(
        decode_response(&encoded_response),
        Err(ApiCodecError::InvalidLayout)
    );

    let mut truncated_request = encode_request(&request).unwrap();
    truncated_request.pop();
    assert_eq!(
        decode_request(&truncated_request),
        Err(ApiCodecError::BufferTooShort)
    );

    let mut truncated_response = encode_response(&response);
    truncated_response.clear();
    assert_eq!(
        decode_response(&truncated_response),
        Err(ApiCodecError::BufferTooShort)
    );
}

#[test]
fn api_submit_commits_and_exposes_retry_cache() {
    let wal_path = test_path("submit-retry");
    let mut engine = SingleNodeEngine::open(core_config(), engine_config(), &wal_path).unwrap();
    let request = SubmitRequest::from_client_request(Slot(1), create_request(11, 1));

    let first = engine.handle_api_request(ApiRequest::Submit(request.clone()));
    let second = engine.handle_api_request(ApiRequest::Submit(request));

    assert_eq!(
        first,
        ApiResponse::Submit(SubmitResponse::Committed(super::SubmissionCommitted {
            applied_lsn: Lsn(1),
            outcome: allocdb_core::result::CommandOutcome::new(ResultCode::Ok),
            from_retry_cache: false,
        }))
    );
    assert_eq!(
        second,
        ApiResponse::Submit(SubmitResponse::Committed(super::SubmissionCommitted {
            applied_lsn: Lsn(1),
            outcome: allocdb_core::result::CommandOutcome::new(ResultCode::Ok),
            from_retry_cache: true,
        }))
    );

    drop(engine);
    fs::remove_file(&wal_path).unwrap();
}

#[test]
fn api_submit_maps_invalid_payload_to_definite_failure() {
    let wal_path = test_path("invalid-submit");
    let mut engine = SingleNodeEngine::open(core_config(), engine_config(), &wal_path).unwrap();

    let response = engine.handle_api_request(ApiRequest::Submit(SubmitRequest {
        request_slot: Slot(1),
        payload: vec![1, 2, 3],
    }));

    assert_eq!(
        response,
        ApiResponse::Submit(SubmitResponse::Rejected(super::SubmissionFailure {
            category: SubmissionErrorCategory::DefiniteFailure,
            code: SubmissionFailureCode::InvalidRequest(InvalidRequestReason::BufferTooShort),
        }))
    );

    drop(engine);
    fs::remove_file(&wal_path).unwrap();
}

#[test]
fn api_reads_enforce_fence_and_return_views() {
    let wal_path = test_path("reads");
    let mut engine = SingleNodeEngine::open(core_config(), engine_config(), &wal_path).unwrap();

    let create = engine.handle_api_request(ApiRequest::Submit(SubmitRequest::from_client_request(
        Slot(1),
        create_request(11, 1),
    )));
    let reserve = engine.handle_api_request(ApiRequest::Submit(
        SubmitRequest::from_client_request(Slot(2), reserve_request(11, 2, 9)),
    ));

    assert!(matches!(
        create,
        ApiResponse::Submit(SubmitResponse::Committed(_))
    ));
    assert!(matches!(
        reserve,
        ApiResponse::Submit(SubmitResponse::Committed(_))
    ));

    let blocked_resource = engine.handle_api_request(ApiRequest::GetResource(ResourceRequest {
        resource_id: ResourceId(11),
        required_lsn: Some(Lsn(3)),
    }));
    assert_eq!(
        blocked_resource,
        ApiResponse::GetResource(ResourceResponse::FenceNotApplied {
            required_lsn: Lsn(3),
            last_applied_lsn: Some(Lsn(2)),
        })
    );

    let resource = engine.handle_api_request(ApiRequest::GetResource(ResourceRequest {
        resource_id: ResourceId(11),
        required_lsn: Some(Lsn(2)),
    }));
    assert_eq!(
        resource,
        ApiResponse::GetResource(ResourceResponse::Found(super::ResourceView {
            resource_id: ResourceId(11),
            state: ResourceState::Reserved,
            current_reservation_id: Some(ReservationId(2)),
            version: 1,
        }))
    );

    let reservation = engine.handle_api_request(ApiRequest::GetReservation(ReservationRequest {
        reservation_id: ReservationId(2),
        current_slot: Slot(2),
        required_lsn: Some(Lsn(2)),
    }));
    assert_eq!(
        reservation,
        ApiResponse::GetReservation(ReservationResponse::Found(super::ReservationView {
            reservation_id: ReservationId(2),
            resource_id: ResourceId(11),
            holder_id: HolderId(9),
            state: ReservationState::Reserved,
            created_lsn: Lsn(2),
            deadline_slot: Slot(5),
            released_lsn: None,
            retire_after_slot: None,
        }))
    );

    drop(engine);
    fs::remove_file(&wal_path).unwrap();
}

#[test]
fn api_reservation_reports_retired_history() {
    let wal_path = test_path("retired");
    let mut engine = SingleNodeEngine::open(core_config(), engine_config(), &wal_path).unwrap();

    let _ = engine.handle_api_request(ApiRequest::Submit(SubmitRequest::from_client_request(
        Slot(1),
        create_request(11, 1),
    )));
    let _ = engine.handle_api_request(ApiRequest::Submit(SubmitRequest::from_client_request(
        Slot(2),
        reserve_request(11, 2, 9),
    )));
    let _ = engine.handle_api_request(ApiRequest::Submit(SubmitRequest::from_client_request(
        Slot(3),
        release_request(2, 3, 9),
    )));

    let response = engine.handle_api_request(ApiRequest::GetReservation(ReservationRequest {
        reservation_id: ReservationId(2),
        current_slot: Slot(8),
        required_lsn: Some(Lsn(3)),
    }));

    assert_eq!(
        response,
        ApiResponse::GetReservation(ReservationResponse::Retired)
    );

    drop(engine);
    fs::remove_file(&wal_path).unwrap();
}

#[test]
fn api_tick_expirations_commits_due_internal_expire() {
    let wal_path = test_path("tick-expirations");
    let mut engine = SingleNodeEngine::open(core_config(), engine_config(), &wal_path).unwrap();

    let _ = engine.handle_api_request(ApiRequest::Submit(SubmitRequest::from_client_request(
        Slot(1),
        create_request(11, 1),
    )));
    let _ = engine.handle_api_request(ApiRequest::Submit(SubmitRequest::from_client_request(
        Slot(2),
        reserve_request(11, 2, 9),
    )));

    let tick = engine.handle_api_request(ApiRequest::TickExpirations(TickExpirationsRequest {
        current_wall_clock_slot: Slot(20),
    }));
    assert_eq!(
        tick,
        ApiResponse::TickExpirations(TickExpirationsResponse::Applied(TickExpirationsApplied {
            processed_count: 1,
            last_applied_lsn: Some(Lsn(3)),
        },))
    );

    let resource = engine.handle_api_request(ApiRequest::GetResource(ResourceRequest {
        resource_id: ResourceId(11),
        required_lsn: Some(Lsn(3)),
    }));
    assert_eq!(
        resource,
        ApiResponse::GetResource(ResourceResponse::Found(super::ResourceView {
            resource_id: ResourceId(11),
            state: ResourceState::Available,
            current_reservation_id: None,
            version: 2,
        }))
    );

    let reservation = engine.handle_api_request(ApiRequest::GetReservation(ReservationRequest {
        reservation_id: ReservationId(2),
        current_slot: Slot(20),
        required_lsn: Some(Lsn(3)),
    }));
    assert_eq!(
        reservation,
        ApiResponse::GetReservation(ReservationResponse::Found(super::ReservationView {
            reservation_id: ReservationId(2),
            resource_id: ResourceId(11),
            holder_id: HolderId(9),
            state: ReservationState::Expired,
            created_lsn: Lsn(2),
            deadline_slot: Slot(5),
            released_lsn: Some(Lsn(3)),
            retire_after_slot: Some(Slot(24)),
        }))
    );

    drop(engine);
    fs::remove_file(&wal_path).unwrap();
}

#[test]
fn api_reads_reject_when_engine_is_halted() {
    let wal_path = test_path("halted-read");
    let snapshot_path = test_snapshot_path("halted-read");
    let mut live = SingleNodeEngine::open(core_config(), engine_config(), &wal_path).unwrap();
    live.inject_next_persist_failure(PersistFailurePhase::AfterAppend);

    let submit = live.handle_api_request(ApiRequest::Submit(SubmitRequest::from_client_request(
        Slot(1),
        create_request(11, 1),
    )));
    assert_eq!(
        submit,
        ApiResponse::Submit(SubmitResponse::Rejected(super::SubmissionFailure {
            category: SubmissionErrorCategory::Indefinite,
            code: SubmissionFailureCode::StorageFailure,
        }))
    );
    assert_eq!(
        live.handle_api_request(ApiRequest::GetResource(ResourceRequest {
            resource_id: ResourceId(11),
            required_lsn: None,
        })),
        ApiResponse::GetResource(ResourceResponse::EngineHalted)
    );
    assert_eq!(
        live.handle_api_request(ApiRequest::GetReservation(ReservationRequest {
            reservation_id: ReservationId(11),
            current_slot: Slot(1),
            required_lsn: None,
        })),
        ApiResponse::GetReservation(ReservationResponse::EngineHalted)
    );
    drop(live);

    let mut recovered =
        SingleNodeEngine::recover(core_config(), engine_config(), &snapshot_path, &wal_path)
            .unwrap();
    assert_eq!(
        recovered.handle_api_request(ApiRequest::GetResource(ResourceRequest {
            resource_id: ResourceId(11),
            required_lsn: Some(Lsn(1)),
        })),
        ApiResponse::GetResource(ResourceResponse::Found(super::ResourceView {
            resource_id: ResourceId(11),
            state: ResourceState::Available,
            current_reservation_id: None,
            version: 0,
        }))
    );

    drop(recovered);
    let _ = fs::remove_file(&snapshot_path);
    fs::remove_file(&wal_path).unwrap();
}

#[test]
fn api_bytes_round_trip_metrics_response() {
    let wal_path = test_path("metrics");
    let mut engine = SingleNodeEngine::open(core_config(), engine_config(), &wal_path).unwrap();

    let encoded = encode_request(&ApiRequest::GetMetrics(MetricsRequest {
        current_wall_clock_slot: Slot(5),
    }))
    .unwrap();
    let response = decode_response(&engine.handle_api_bytes(&encoded).unwrap()).unwrap();

    assert_eq!(
        response,
        ApiResponse::GetMetrics(MetricsResponse {
            metrics: crate::engine::EngineMetrics {
                queue_depth: 0,
                queue_capacity: 2,
                accepting_writes: true,
                recovery: crate::engine::RecoveryStatus {
                    startup_kind: RecoveryStartupKind::FreshStart,
                    loaded_snapshot_lsn: None,
                    replayed_wal_frame_count: 0,
                    replayed_wal_last_lsn: None,
                    active_snapshot_lsn: None,
                },
                core: allocdb_core::HealthMetrics {
                    last_applied_lsn: None,
                    last_request_slot: None,
                    logical_slot_lag: 0,
                    expiration_backlog: 0,
                    operation_table_used: 0,
                    operation_table_capacity: 16,
                    operation_table_utilization_pct: 0,
                },
            },
        })
    );

    drop(engine);
    fs::remove_file(&wal_path).unwrap();
}

#[test]
fn api_bytes_recovery_preserves_state_and_retry_cache() {
    let wal_path = test_path("bytes-recovery");
    let snapshot_path = test_snapshot_path("bytes-recovery");
    let mut engine = SingleNodeEngine::open(core_config(), engine_config(), &wal_path).unwrap();
    let submit_bytes = encode_request(&ApiRequest::Submit(SubmitRequest::from_client_request(
        Slot(1),
        create_request(11, 1),
    )))
    .unwrap();

    let first = decode_response(&engine.handle_api_bytes(&submit_bytes).unwrap()).unwrap();
    assert_eq!(
        first,
        ApiResponse::Submit(SubmitResponse::Committed(super::SubmissionCommitted {
            applied_lsn: Lsn(1),
            outcome: allocdb_core::result::CommandOutcome::new(ResultCode::Ok),
            from_retry_cache: false,
        }))
    );

    drop(engine);

    let mut recovered =
        SingleNodeEngine::recover(core_config(), engine_config(), &snapshot_path, &wal_path)
            .unwrap();

    let retry = decode_response(&recovered.handle_api_bytes(&submit_bytes).unwrap()).unwrap();
    assert_eq!(
        retry,
        ApiResponse::Submit(SubmitResponse::Committed(super::SubmissionCommitted {
            applied_lsn: Lsn(1),
            outcome: allocdb_core::result::CommandOutcome::new(ResultCode::Ok),
            from_retry_cache: true,
        }))
    );

    let read_bytes = encode_request(&ApiRequest::GetResource(ResourceRequest {
        resource_id: ResourceId(11),
        required_lsn: Some(Lsn(1)),
    }))
    .unwrap();
    let read = decode_response(&recovered.handle_api_bytes(&read_bytes).unwrap()).unwrap();
    assert_eq!(
        read,
        ApiResponse::GetResource(ResourceResponse::Found(super::ResourceView {
            resource_id: ResourceId(11),
            state: ResourceState::Available,
            current_reservation_id: None,
            version: 0,
        }))
    );

    drop(recovered);
    let _ = fs::remove_file(&snapshot_path);
    fs::remove_file(&wal_path).unwrap();
}

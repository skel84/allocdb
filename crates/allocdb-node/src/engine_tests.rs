use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use allocdb_core::command::{ClientRequest, Command};
use allocdb_core::command_codec::{CommandCodecError, encode_client_request};
use allocdb_core::config::Config;
use allocdb_core::ids::{ClientId, HolderId, Lsn, OperationId, ReservationId, ResourceId, Slot};
use allocdb_core::result::ResultCode;
use allocdb_core::snapshot_file::SnapshotFile;
use allocdb_core::state_machine::AllocDb;
use allocdb_core::wal::RecordType;
use allocdb_core::wal_file::{WalFile, WalFileError};
use allocdb_core::{ReservationState, ResourceState};

use super::PersistFailurePhase;
use crate::engine::{
    EngineConfig, EngineConfigError, EnqueueResult, ReadError, RecoveryStartupKind,
    SingleNodeEngine, SubmissionError, SubmissionErrorCategory,
};

fn test_path(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("allocdb-engine-{name}-{nanos}.wal"))
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
fn engine_config_rejects_zero_bounds() {
    assert_eq!(
        (EngineConfig {
            max_submission_queue: 0,
            max_command_bytes: 1,
            max_expirations_per_tick: 1,
        })
        .validate(),
        Err(EngineConfigError::ZeroCapacity("max_submission_queue"))
    );
    assert_eq!(
        (EngineConfig {
            max_submission_queue: 1,
            max_command_bytes: 0,
            max_expirations_per_tick: 1,
        })
        .validate(),
        Err(EngineConfigError::ZeroCapacity("max_command_bytes"))
    );
    assert_eq!(
        (EngineConfig {
            max_submission_queue: 1,
            max_command_bytes: 1,
            max_expirations_per_tick: 0,
        })
        .validate(),
        Err(EngineConfigError::ZeroCapacity("max_expirations_per_tick"))
    );
}

#[test]
fn encoded_submission_rejects_malformed_payload_before_commit() {
    let wal_path = test_path("malformed");
    let mut engine = SingleNodeEngine::open(core_config(), engine_config(), &wal_path).unwrap();

    let error = engine.submit_encoded(Slot(1), &[1, 2, 3]).unwrap_err();
    assert_eq!(error.category(), SubmissionErrorCategory::DefiniteFailure);
    assert!(matches!(
        error,
        SubmissionError::InvalidRequest(CommandCodecError::BufferTooShort)
    ));
    assert!(engine.db().resource(ResourceId(11)).is_none());
    assert_eq!(fs::metadata(&wal_path).unwrap().len(), 0);

    fs::remove_file(wal_path).unwrap();
}

#[test]
fn oversized_submission_is_rejected_before_commit() {
    let wal_path = test_path("too-large");
    let mut engine = SingleNodeEngine::open(
        core_config(),
        EngineConfig {
            max_submission_queue: 2,
            max_command_bytes: 8,
            max_expirations_per_tick: 1,
        },
        &wal_path,
    )
    .unwrap();

    let request = create(11, 1);
    let error = engine.submit(Slot(1), request).unwrap_err();
    assert_eq!(error.category(), SubmissionErrorCategory::DefiniteFailure);
    assert!(matches!(
        error,
        SubmissionError::CommandTooLarge {
            encoded_len,
            max_command_bytes: 8,
        } if encoded_len > 8
    ));
    assert_eq!(fs::metadata(&wal_path).unwrap().len(), 0);

    fs::remove_file(wal_path).unwrap();
}

#[test]
fn enqueue_respects_bounded_queue_capacity() {
    let wal_path = test_path("queue-capacity");
    let mut engine = SingleNodeEngine::open(
        core_config(),
        EngineConfig {
            max_submission_queue: 1,
            max_command_bytes: 512,
            max_expirations_per_tick: 1,
        },
        &wal_path,
    )
    .unwrap();

    let first = engine.enqueue_client(Slot(1), create(11, 1)).unwrap();
    let second = engine.enqueue_client(Slot(1), create(12, 2)).unwrap_err();

    assert_eq!(first, EnqueueResult::Queued);
    assert_eq!(second.category(), SubmissionErrorCategory::DefiniteFailure);
    assert!(matches!(
        second,
        SubmissionError::Overloaded {
            queue_depth: 1,
            queue_capacity: 1,
        }
    ));

    fs::remove_file(wal_path).unwrap();
}

#[test]
fn submit_sequences_wal_and_applies_state() {
    let wal_path = test_path("sequence");
    let mut engine = SingleNodeEngine::open(core_config(), engine_config(), &wal_path).unwrap();

    let created = engine.submit(Slot(1), create(11, 1)).unwrap();
    let reserved = engine
        .submit(
            Slot(2),
            ClientRequest {
                operation_id: OperationId(2),
                client_id: ClientId(7),
                command: Command::Reserve {
                    resource_id: ResourceId(11),
                    holder_id: HolderId(9),
                    ttl_slots: 3,
                },
            },
        )
        .unwrap();

    assert_eq!(created.applied_lsn.get(), 1);
    assert_eq!(created.outcome.result_code, ResultCode::Ok);
    assert_eq!(reserved.applied_lsn.get(), 2);
    assert_eq!(reserved.outcome.result_code, ResultCode::Ok);
    assert_eq!(
        engine
            .db()
            .resource(ResourceId(11))
            .unwrap()
            .current_reservation_id,
        Some(ReservationId(2))
    );

    fs::remove_file(wal_path).unwrap();
}

#[test]
fn duplicate_retry_returns_cached_result_without_wal_growth() {
    let wal_path = test_path("retry-cache");
    let mut engine = SingleNodeEngine::open(core_config(), engine_config(), &wal_path).unwrap();
    let request = create(11, 1);

    let first = engine.submit(Slot(1), request).unwrap();
    let wal_len = fs::metadata(&wal_path).unwrap().len();
    let second = engine.submit(Slot(2), request).unwrap();

    assert!(!first.from_retry_cache);
    assert!(second.from_retry_cache);
    assert_eq!(second.applied_lsn, first.applied_lsn);
    assert_eq!(second.outcome, first.outcome);
    assert_eq!(fs::metadata(&wal_path).unwrap().len(), wal_len);

    fs::remove_file(wal_path).unwrap();
}

#[test]
fn queued_duplicate_retry_reuses_original_submission() {
    let wal_path = test_path("queued-retry-cache");
    let mut engine = SingleNodeEngine::open(core_config(), engine_config(), &wal_path).unwrap();
    let request = create(11, 1);
    let expected_wal_len = u64::try_from(31 + encode_client_request(request).len()).unwrap();

    let queued = engine.enqueue_client(Slot(1), request).unwrap();
    assert_eq!(queued, EnqueueResult::Queued);

    let retry = engine.submit(Slot(2), request).unwrap();

    assert!(retry.from_retry_cache);
    assert_eq!(retry.applied_lsn, Lsn(1));
    assert_eq!(retry.outcome.result_code, ResultCode::Ok);
    assert_eq!(fs::metadata(&wal_path).unwrap().len(), expected_wal_len);

    fs::remove_file(wal_path).unwrap();
}

#[test]
fn conflicting_retry_returns_conflict_without_wal_growth() {
    let wal_path = test_path("retry-conflict");
    let mut engine = SingleNodeEngine::open(core_config(), engine_config(), &wal_path).unwrap();

    let first = engine.submit(Slot(1), create(11, 1)).unwrap();
    let wal_len = fs::metadata(&wal_path).unwrap().len();
    let conflict = engine.submit(Slot(2), create(12, 1)).unwrap();

    assert_eq!(first.outcome.result_code, ResultCode::Ok);
    assert!(conflict.from_retry_cache);
    assert_eq!(conflict.applied_lsn, first.applied_lsn);
    assert_eq!(conflict.outcome.result_code, ResultCode::OperationConflict);
    assert!(engine.db().resource(ResourceId(12)).is_none());
    assert_eq!(fs::metadata(&wal_path).unwrap().len(), wal_len);

    fs::remove_file(wal_path).unwrap();
}

#[test]
fn queued_conflicting_retry_returns_conflict_without_second_entry() {
    let wal_path = test_path("queued-retry-conflict");
    let mut engine = SingleNodeEngine::open(core_config(), engine_config(), &wal_path).unwrap();
    let expected_wal_len = u64::try_from(31 + encode_client_request(create(11, 1)).len()).unwrap();

    let queued = engine.enqueue_client(Slot(1), create(11, 1)).unwrap();
    assert_eq!(queued, EnqueueResult::Queued);

    let conflict = engine.submit(Slot(2), create(12, 1)).unwrap();

    assert!(conflict.from_retry_cache);
    assert_eq!(conflict.applied_lsn, Lsn(1));
    assert_eq!(conflict.outcome.result_code, ResultCode::OperationConflict);
    assert!(engine.db().resource(ResourceId(12)).is_none());
    assert_eq!(fs::metadata(&wal_path).unwrap().len(), expected_wal_len);

    fs::remove_file(wal_path).unwrap();
}

#[test]
fn metrics_include_queue_depth_and_core_health() {
    let wal_path = test_path("metrics");
    let mut engine = SingleNodeEngine::open(core_config(), engine_config(), &wal_path).unwrap();
    engine.enqueue_client(Slot(2), create(11, 1)).unwrap();
    engine
        .enqueue_client(
            Slot(5),
            ClientRequest {
                operation_id: OperationId(2),
                client_id: ClientId(7),
                command: Command::CreateResource {
                    resource_id: ResourceId(12),
                },
            },
        )
        .unwrap();

    let metrics = engine.metrics(Slot(9));
    assert_eq!(metrics.queue_depth, 2);
    assert_eq!(metrics.queue_capacity, 2);
    assert!(metrics.accepting_writes);
    assert_eq!(
        metrics.recovery.startup_kind,
        RecoveryStartupKind::FreshStart
    );
    assert_eq!(metrics.recovery.loaded_snapshot_lsn, None);
    assert_eq!(metrics.recovery.replayed_wal_frame_count, 0);
    assert_eq!(metrics.recovery.replayed_wal_last_lsn, None);
    assert_eq!(metrics.recovery.active_snapshot_lsn, None);
    assert_eq!(metrics.core.logical_slot_lag, 0);
    assert_eq!(metrics.core.expiration_backlog, 0);

    fs::remove_file(wal_path).unwrap();
}

#[test]
fn encoded_submission_round_trips_same_as_typed_submit() {
    let wal_path = test_path("encoded-submit");
    let mut engine = SingleNodeEngine::open(core_config(), engine_config(), &wal_path).unwrap();
    let request = create(11, 1);
    let encoded = encode_client_request(request);

    let result = engine.submit_encoded(Slot(1), &encoded).unwrap();

    assert_eq!(result.outcome.result_code, ResultCode::Ok);
    assert_eq!(
        engine.db().resource(ResourceId(11)).unwrap().current_state,
        ResourceState::Available
    );

    fs::remove_file(wal_path).unwrap();
}

#[test]
fn expiration_tick_commits_internal_expire_and_frees_overdue_resource() {
    let wal_path = test_path("expiration-tick");
    let mut engine = SingleNodeEngine::open(core_config(), engine_config(), &wal_path).unwrap();

    engine.submit(Slot(1), create(11, 1)).unwrap();
    let reserved = engine
        .submit(
            Slot(2),
            ClientRequest {
                operation_id: OperationId(2),
                client_id: ClientId(7),
                command: Command::Reserve {
                    resource_id: ResourceId(11),
                    holder_id: HolderId(9),
                    ttl_slots: 3,
                },
            },
        )
        .unwrap();

    let tick = engine.tick_expirations(Slot(20)).unwrap();

    assert_eq!(tick.processed_count, 1);
    assert_eq!(tick.last_applied_lsn, Some(Lsn(3)));
    assert_eq!(
        engine.db().resource(ResourceId(11)).unwrap().current_state,
        ResourceState::Available
    );
    assert_eq!(
        engine
            .db()
            .reservation(
                reserved
                    .outcome
                    .reservation_id
                    .expect("reserve must return reservation id"),
                Slot(20),
            )
            .unwrap()
            .state,
        ReservationState::Expired
    );

    let recovered_wal = WalFile::open(&wal_path, engine_config().max_command_bytes)
        .unwrap()
        .recover()
        .unwrap();
    assert_eq!(
        recovered_wal
            .scan_result
            .frames
            .iter()
            .map(|frame| frame.record_type)
            .collect::<Vec<_>>(),
        vec![
            RecordType::ClientCommand,
            RecordType::ClientCommand,
            RecordType::InternalCommand,
        ]
    );

    fs::remove_file(wal_path).unwrap();
}

#[test]
fn strict_read_fence_requires_applied_lsn() {
    let wal_path = test_path("read-fence");
    let mut engine = SingleNodeEngine::open(core_config(), engine_config(), &wal_path).unwrap();

    assert_eq!(
        engine.enforce_read_fence(Lsn(1)),
        Err(ReadError::RequiredLsnNotApplied {
            required_lsn: Lsn(1),
            last_applied_lsn: None,
        })
    );

    let create = engine.submit(Slot(1), create(11, 1)).unwrap();

    assert_eq!(engine.enforce_read_fence(Lsn(0)), Ok(()));
    assert_eq!(engine.enforce_read_fence(create.applied_lsn), Ok(()));
    assert_eq!(
        engine.enforce_read_fence(Lsn(2)),
        Err(ReadError::RequiredLsnNotApplied {
            required_lsn: Lsn(2),
            last_applied_lsn: Some(Lsn(1)),
        })
    );

    fs::remove_file(wal_path).unwrap();
}

#[test]
fn halted_engine_rejects_reads_until_recovery() {
    let wal_path = test_path("halted-read");
    let snapshot_path = wal_path.with_extension("snapshot");

    {
        let mut live = SingleNodeEngine::open(core_config(), engine_config(), &wal_path).unwrap();
        live.inject_next_persist_failure(PersistFailurePhase::AfterAppend);

        let error = live.submit(Slot(1), create(11, 1)).unwrap_err();
        assert!(matches!(error, SubmissionError::WalFile(_)));
        assert_eq!(
            live.enforce_read_fence(Lsn(0)),
            Err(ReadError::EngineHalted)
        );
        assert_eq!(
            live.enforce_read_fence(Lsn(1)),
            Err(ReadError::EngineHalted)
        );
    }

    let recovered =
        SingleNodeEngine::recover(core_config(), engine_config(), &snapshot_path, &wal_path)
            .unwrap();
    assert_eq!(recovered.enforce_read_fence(Lsn(1)), Ok(()));
    assert!(recovered.db().resource(ResourceId(11)).is_some());

    fs::remove_file(wal_path).unwrap();
    let _ = fs::remove_file(snapshot_path);
}

#[test]
fn recover_restores_state_and_retry_cache() {
    let wal_path = test_path("recover-engine");
    let snapshot_path = wal_path.with_extension("snapshot");

    {
        let mut live = SingleNodeEngine::open(core_config(), engine_config(), &wal_path).unwrap();
        let result = live.submit(Slot(1), create(11, 1)).unwrap();
        assert_eq!(result.applied_lsn, Lsn(1));
    }

    let wal_len = fs::metadata(&wal_path).unwrap().len();
    let mut recovered =
        SingleNodeEngine::recover(core_config(), engine_config(), &snapshot_path, &wal_path)
            .unwrap();

    assert!(recovered.db().resource(ResourceId(11)).is_some());
    let metrics = recovered.metrics(Slot(2));
    assert_eq!(metrics.recovery.startup_kind, RecoveryStartupKind::WalOnly);
    assert_eq!(metrics.recovery.loaded_snapshot_lsn, None);
    assert_eq!(metrics.recovery.replayed_wal_frame_count, 1);
    assert_eq!(metrics.recovery.replayed_wal_last_lsn, Some(Lsn(1)));
    assert_eq!(metrics.recovery.active_snapshot_lsn, None);

    let retry = recovered.submit(Slot(2), create(11, 1)).unwrap();
    assert!(retry.from_retry_cache);
    assert_eq!(retry.applied_lsn, Lsn(1));
    assert_eq!(fs::metadata(&wal_path).unwrap().len(), wal_len);

    fs::remove_file(wal_path).unwrap();
    let _ = fs::remove_file(snapshot_path);
}

#[test]
fn recovery_metrics_report_snapshot_and_wal_replay() {
    let wal_path = test_path("recover-metrics");
    let snapshot_path = wal_path.with_extension("snapshot");

    {
        let mut live = SingleNodeEngine::open(core_config(), engine_config(), &wal_path).unwrap();
        live.submit(Slot(1), create(11, 1)).unwrap();
        live.checkpoint(&snapshot_path).unwrap();
        live.submit(Slot(2), create(12, 2)).unwrap();
    }

    let recovered =
        SingleNodeEngine::recover(core_config(), engine_config(), &snapshot_path, &wal_path)
            .unwrap();
    let metrics = recovered.metrics(Slot(4));

    assert_eq!(
        metrics.recovery.startup_kind,
        RecoveryStartupKind::SnapshotAndWal
    );
    assert_eq!(metrics.recovery.loaded_snapshot_lsn, Some(Lsn(1)));
    assert_eq!(metrics.recovery.replayed_wal_frame_count, 1);
    assert_eq!(metrics.recovery.replayed_wal_last_lsn, Some(Lsn(2)));
    assert_eq!(metrics.recovery.active_snapshot_lsn, Some(Lsn(1)));

    fs::remove_file(wal_path).unwrap();
    fs::remove_file(snapshot_path).unwrap();
}

#[test]
fn recovery_metrics_treat_loaded_empty_snapshot_as_snapshot_startup() {
    let wal_path = test_path("empty-snapshot-recovery");
    let snapshot_path = wal_path.with_extension("snapshot");
    SnapshotFile::new(&snapshot_path)
        .write_snapshot(&AllocDb::new(core_config()).unwrap().snapshot())
        .unwrap();

    let recovered =
        SingleNodeEngine::recover(core_config(), engine_config(), &snapshot_path, &wal_path)
            .unwrap();
    let metrics = recovered.metrics(Slot(1));

    assert_eq!(
        metrics.recovery.startup_kind,
        RecoveryStartupKind::SnapshotOnly
    );
    assert_eq!(metrics.recovery.loaded_snapshot_lsn, None);
    assert_eq!(metrics.recovery.replayed_wal_frame_count, 0);
    assert_eq!(metrics.recovery.replayed_wal_last_lsn, None);
    assert_eq!(metrics.recovery.active_snapshot_lsn, None);

    fs::remove_file(snapshot_path).unwrap();
    fs::remove_file(wal_path).unwrap();
}

#[test]
fn submission_errors_have_explicit_indefinite_category() {
    assert_eq!(
        SubmissionError::EngineHalted.category(),
        SubmissionErrorCategory::Indefinite
    );
    assert_eq!(
        SubmissionError::WalFile(WalFileError::Io(std::io::Error::other("boom"))).category(),
        SubmissionErrorCategory::Indefinite
    );
    assert_eq!(
        SubmissionError::Overloaded {
            queue_depth: 1,
            queue_capacity: 2,
        }
        .category(),
        SubmissionErrorCategory::DefiniteFailure
    );
}

#[test]
fn retry_after_failed_pre_append_attempt_executes_once_after_recovery() {
    let wal_path = test_path("retry-after-pre-append-failure");
    let snapshot_path = wal_path.with_extension("snapshot");

    {
        let mut live = SingleNodeEngine::open(core_config(), engine_config(), &wal_path).unwrap();
        live.inject_next_persist_failure(PersistFailurePhase::BeforeAppend);

        let error = live.submit(Slot(1), create(11, 1)).unwrap_err();
        assert_eq!(error.category(), SubmissionErrorCategory::Indefinite);
        assert!(matches!(error, SubmissionError::WalFile(_)));
        assert!(!live.metrics(Slot(1)).accepting_writes);
        assert!(matches!(
            live.submit(Slot(2), create(11, 1)),
            Err(SubmissionError::EngineHalted)
        ));
        assert_eq!(fs::metadata(&wal_path).unwrap().len(), 0);
    }

    let mut recovered =
        SingleNodeEngine::recover(core_config(), engine_config(), &snapshot_path, &wal_path)
            .unwrap();
    let retry = recovered.submit(Slot(2), create(11, 1)).unwrap();

    assert!(!retry.from_retry_cache);
    assert_eq!(retry.applied_lsn, Lsn(1));
    assert_eq!(retry.outcome.result_code, ResultCode::Ok);
    assert!(recovered.db().resource(ResourceId(11)).is_some());
    assert!(fs::metadata(&wal_path).unwrap().len() > 0);

    fs::remove_file(wal_path).unwrap();
    let _ = fs::remove_file(snapshot_path);
}

#[test]
fn retry_after_failed_post_append_attempt_returns_cached_result_after_recovery() {
    let wal_path = test_path("retry-after-post-append-failure");
    let snapshot_path = wal_path.with_extension("snapshot");

    let persisted_wal_len = {
        let mut live = SingleNodeEngine::open(core_config(), engine_config(), &wal_path).unwrap();
        live.inject_next_persist_failure(PersistFailurePhase::AfterAppend);

        let error = live.submit(Slot(1), create(11, 1)).unwrap_err();
        assert_eq!(error.category(), SubmissionErrorCategory::Indefinite);
        assert!(matches!(error, SubmissionError::WalFile(_)));
        assert!(!live.metrics(Slot(1)).accepting_writes);
        fs::metadata(&wal_path).unwrap().len()
    };

    assert!(persisted_wal_len > 0);

    let mut recovered =
        SingleNodeEngine::recover(core_config(), engine_config(), &snapshot_path, &wal_path)
            .unwrap();
    assert!(recovered.db().resource(ResourceId(11)).is_some());

    let retry = recovered.submit(Slot(2), create(11, 1)).unwrap();

    assert!(retry.from_retry_cache);
    assert_eq!(retry.applied_lsn, Lsn(1));
    assert_eq!(retry.outcome.result_code, ResultCode::Ok);
    assert_eq!(fs::metadata(&wal_path).unwrap().len(), persisted_wal_len);

    fs::remove_file(wal_path).unwrap();
    let _ = fs::remove_file(snapshot_path);
}

#[test]
fn retry_resolution_is_only_guaranteed_within_the_dedupe_window() {
    let wal_path = test_path("retry-window-expiry");
    let mut engine = SingleNodeEngine::open(core_config(), engine_config(), &wal_path).unwrap();

    let first = engine.submit(Slot(1), create(11, 1)).unwrap();
    let wal_len = fs::metadata(&wal_path).unwrap().len();
    let retried_after_expiry = engine.submit(Slot(30), create(12, 1)).unwrap();

    assert_eq!(first.outcome.result_code, ResultCode::Ok);
    assert_eq!(retried_after_expiry.outcome.result_code, ResultCode::Ok);
    assert!(!retried_after_expiry.from_retry_cache);
    assert_eq!(retried_after_expiry.applied_lsn, Lsn(2));
    assert!(engine.db().resource(ResourceId(12)).is_some());
    assert!(fs::metadata(&wal_path).unwrap().len() > wal_len);

    fs::remove_file(wal_path).unwrap();
}

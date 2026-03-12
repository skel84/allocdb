use allocdb_core::command::{ClientRequest, Command};
use allocdb_core::config::Config;
use allocdb_core::ids::{ClientId, HolderId, OperationId, ResourceId, Slot};
use allocdb_core::{ReservationState, ResourceState};

use crate::engine::{
    CheckpointError, CrashPlan, CrashPoint, EngineConfig, PersistFailurePhase, RecoverEngineError,
    RecoveryStartupKind, SubmissionError,
};
use crate::simulation::SimulationHarness;

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
        max_submission_queue: 4,
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

fn reserve(
    resource_id: u128,
    holder_id: u128,
    operation_id: u128,
    ttl_slots: u64,
) -> ClientRequest {
    ClientRequest {
        operation_id: OperationId(operation_id),
        client_id: ClientId(7),
        command: Command::Reserve {
            resource_id: ResourceId(resource_id),
            holder_id: HolderId(holder_id),
            ttl_slots,
        },
    }
}

fn seed_for_point(point: CrashPoint, enabled_points: &[CrashPoint]) -> u64 {
    (0_u64..256)
        .find(|seed| CrashPlan::from_seed(*seed, enabled_points).point == point)
        .expect("test crash point must be reachable from one seed")
}

#[test]
fn seeded_ready_batch_transcript_is_reproducible() {
    let requests = [
        create(11, 1),
        create(12, 2),
        create(13, 3),
        create(14, 4),
        create(15, 5),
    ];

    let mut first =
        SimulationHarness::new("reproducible-first", 0x5eed, core_config(), engine_config())
            .unwrap();
    let first_transcript = first.submit_ready_batch(Slot(7), &requests).unwrap();

    let mut second = SimulationHarness::new(
        "reproducible-second",
        0x5eed,
        core_config(),
        engine_config(),
    )
    .unwrap();
    let second_transcript = second.submit_ready_batch(Slot(7), &requests).unwrap();

    let mut different = SimulationHarness::new(
        "reproducible-different",
        0x5eee,
        core_config(),
        engine_config(),
    )
    .unwrap();
    let different_transcript = different.submit_ready_batch(Slot(7), &requests).unwrap();

    assert_eq!(first_transcript, second_transcript);
    assert_ne!(first_transcript, different_transcript);
    assert_eq!(
        first_transcript
            .iter()
            .map(|entry| entry.slot)
            .collect::<Vec<_>>(),
        vec![Slot(7), Slot(7), Slot(7), Slot(7), Slot(7)]
    );
    assert_eq!(
        first_transcript
            .iter()
            .map(|entry| entry.applied_lsn.get())
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5]
    );
    assert_eq!(first.metrics().core.last_request_slot, Some(Slot(7)));
    assert!(first.engine().db().resource(ResourceId(11)).is_some());
    assert!(first.engine().db().resource(ResourceId(12)).is_some());
    assert!(first.engine().db().resource(ResourceId(13)).is_some());
    assert!(first.engine().db().resource(ResourceId(14)).is_some());
    assert!(first.engine().db().resource(ResourceId(15)).is_some());
}

#[test]
fn explicit_slot_advancement_controls_lag_and_backlog() {
    let mut harness =
        SimulationHarness::new("lag-backlog", 0x44, core_config(), engine_config()).unwrap();

    harness.advance_to(Slot(1));
    let created = harness.submit(create(11, 1)).unwrap();
    assert_eq!(created.applied_lsn.get(), 1);

    harness.advance_to(Slot(2));
    let reserved = harness.submit(reserve(11, 9, 2, 3)).unwrap();
    let reservation_id = reserved
        .outcome
        .reservation_id
        .expect("reserve must return reservation id");
    assert_eq!(reserved.applied_lsn.get(), 2);
    assert_eq!(reserved.outcome.deadline_slot, Some(Slot(5)));

    harness.advance_to(Slot(6));
    let metrics = harness.metrics();
    assert_eq!(harness.current_slot(), Slot(6));
    assert_eq!(metrics.core.last_request_slot, Some(Slot(2)));
    assert_eq!(metrics.core.logical_slot_lag, 4);
    assert_eq!(metrics.core.expiration_backlog, 1);
    assert_eq!(
        harness
            .engine()
            .db()
            .resource(ResourceId(11))
            .unwrap()
            .current_state,
        ResourceState::Reserved
    );
    assert_eq!(
        harness
            .engine()
            .db()
            .reservation(reservation_id, Slot(6))
            .unwrap()
            .state,
        ReservationState::Reserved
    );
}

#[test]
fn simulated_slot_driver_handles_expiration_restart_path() {
    let mut harness =
        SimulationHarness::new("expiration-restart", 0x15, core_config(), engine_config()).unwrap();

    harness.advance_to(Slot(1));
    let created = harness.submit(create(11, 1)).unwrap();
    assert_eq!(created.applied_lsn.get(), 1);

    harness.advance_to(Slot(2));
    let reserved = harness.submit(reserve(11, 9, 2, 3)).unwrap();
    let reservation_id = reserved
        .outcome
        .reservation_id
        .expect("reserve must return reservation id");
    assert_eq!(reserved.applied_lsn.get(), 2);
    assert_eq!(reserved.outcome.deadline_slot, Some(Slot(5)));

    let checkpoint = harness.checkpoint().unwrap();
    assert_eq!(checkpoint.snapshot_lsn.map(allocdb_core::Lsn::get), Some(2));
    assert_eq!(checkpoint.retained_frame_count, 3);

    harness.advance_to(Slot(6));
    let before_tick = harness.metrics();
    assert_eq!(before_tick.core.logical_slot_lag, 4);
    assert_eq!(before_tick.core.expiration_backlog, 1);
    assert_eq!(
        harness
            .engine()
            .db()
            .resource(ResourceId(11))
            .unwrap()
            .current_state,
        ResourceState::Reserved
    );
    assert_eq!(
        harness
            .engine()
            .db()
            .reservation(reservation_id, Slot(6))
            .unwrap()
            .state,
        ReservationState::Reserved
    );

    harness.inject_next_persist_failure(PersistFailurePhase::AfterAppend);
    let error = harness.tick_expirations().unwrap_err();
    assert!(matches!(error, SubmissionError::WalFile(_)));
    assert!(!harness.metrics().accepting_writes);

    let recovered = harness.restart().unwrap();
    assert_eq!(
        recovered.recovery.startup_kind,
        RecoveryStartupKind::SnapshotAndWal
    );
    assert_eq!(
        recovered
            .recovery
            .loaded_snapshot_lsn
            .map(allocdb_core::Lsn::get),
        Some(2)
    );
    assert_eq!(recovered.recovery.replayed_wal_frame_count, 1);
    assert_eq!(
        recovered
            .recovery
            .replayed_wal_last_lsn
            .map(allocdb_core::Lsn::get),
        Some(3)
    );
    assert_eq!(
        harness
            .engine()
            .db()
            .resource(ResourceId(11))
            .unwrap()
            .current_state,
        ResourceState::Available
    );
    assert_eq!(
        harness
            .engine()
            .db()
            .reservation(reservation_id, Slot(6))
            .unwrap()
            .state,
        ReservationState::Expired
    );
}

#[test]
fn harness_submit_propagates_persist_failure_for_negative_path_tests() {
    let mut harness =
        SimulationHarness::new("persist-failure", 0x99, core_config(), engine_config()).unwrap();

    harness.advance_to(Slot(1));
    harness.inject_next_persist_failure(PersistFailurePhase::BeforeAppend);

    let error = harness.submit(create(11, 1)).unwrap_err();

    assert!(matches!(error, SubmissionError::WalFile(_)));
    assert!(!harness.metrics().accepting_writes);
}

#[test]
fn crash_plan_seed_is_reproducible_and_order_independent() {
    let enabled_points = [
        CrashPoint::ClientBeforeWalAppend,
        CrashPoint::ClientAfterWalSync,
        CrashPoint::ClientAfterApply,
    ];
    let reordered_points = [
        CrashPoint::ClientAfterApply,
        CrashPoint::ClientBeforeWalAppend,
        CrashPoint::ClientAfterWalSync,
        CrashPoint::ClientAfterApply,
    ];

    let planned = CrashPlan::from_seed(0x5eed, &enabled_points);
    let reordered = CrashPlan::from_seed(0x5eed, &reordered_points);
    let observed: std::collections::BTreeSet<_> = (0_u64..16)
        .map(|seed| CrashPlan::from_seed(seed, &enabled_points).point)
        .collect();

    assert_eq!(planned, reordered);
    assert!(observed.len() > 1);
}

#[test]
fn seeded_client_post_sync_crash_recovers_via_real_engine() {
    let runtime_points = [
        CrashPoint::ClientBeforeWalAppend,
        CrashPoint::ClientAfterWalSync,
        CrashPoint::ClientAfterApply,
    ];
    let seed = seed_for_point(CrashPoint::ClientAfterWalSync, &runtime_points);

    let mut harness = SimulationHarness::new(
        "client-post-sync-crash",
        seed,
        core_config(),
        engine_config(),
    )
    .unwrap();
    harness.advance_to(Slot(1));
    let planned = harness.arm_next_engine_crash(seed, &runtime_points);
    assert_eq!(planned.point, CrashPoint::ClientAfterWalSync);

    let error = harness.submit(create(11, 1)).unwrap_err();

    assert!(matches!(
        error,
        SubmissionError::CrashInjected(plan) if plan == planned
    ));
    assert!(!harness.metrics().accepting_writes);

    let recovered = harness.restart().unwrap();
    assert_eq!(
        recovered.recovery.startup_kind,
        RecoveryStartupKind::WalOnly
    );
    assert_eq!(recovered.recovery.replayed_wal_frame_count, 1);
    assert_eq!(
        recovered
            .recovery
            .replayed_wal_last_lsn
            .map(allocdb_core::Lsn::get),
        Some(1)
    );
    assert!(harness.engine().db().resource(ResourceId(11)).is_some());

    let retry = harness.submit(create(11, 1)).unwrap();
    assert!(retry.from_retry_cache);
    assert_eq!(retry.applied_lsn.get(), 1);
}

#[test]
fn seeded_checkpoint_crash_after_snapshot_write_is_recoverable() {
    let checkpoint_points = [
        CrashPoint::CheckpointAfterSnapshotWrite,
        CrashPoint::CheckpointAfterWalRewrite,
    ];
    let seed = seed_for_point(CrashPoint::CheckpointAfterSnapshotWrite, &checkpoint_points);
    let mut harness =
        SimulationHarness::new("checkpoint-crash", seed, core_config(), engine_config()).unwrap();

    harness.advance_to(Slot(1));
    harness.submit(create(11, 1)).unwrap();
    harness.checkpoint().unwrap();

    harness.advance_to(Slot(2));
    harness.submit(create(12, 2)).unwrap();

    let planned = harness.arm_next_engine_crash(seed, &checkpoint_points);
    let error = harness.checkpoint().unwrap_err();

    assert!(matches!(
        error,
        CheckpointError::CrashInjected(plan) if plan == planned
    ));
    assert_eq!(planned.point, CrashPoint::CheckpointAfterSnapshotWrite);
    assert!(!harness.metrics().accepting_writes);

    let recovered = harness.restart().unwrap();
    assert_eq!(
        recovered.recovery.startup_kind,
        RecoveryStartupKind::SnapshotOnly
    );
    assert_eq!(
        recovered
            .recovery
            .loaded_snapshot_lsn
            .map(allocdb_core::Lsn::get),
        Some(2)
    );
    assert_eq!(recovered.recovery.replayed_wal_frame_count, 0);
    assert!(harness.engine().db().resource(ResourceId(11)).is_some());
    assert!(harness.engine().db().resource(ResourceId(12)).is_some());
}

#[test]
fn seeded_recovery_crash_is_reproducible_and_resumable() {
    let recovery_points = [
        CrashPoint::RecoveryAfterSnapshotLoad,
        CrashPoint::RecoveryAfterWalTruncate,
        CrashPoint::RecoveryAfterReplayFrame,
    ];
    let seed = seed_for_point(CrashPoint::RecoveryAfterReplayFrame, &recovery_points);

    let mut first =
        SimulationHarness::new("recovery-crash-first", seed, core_config(), engine_config())
            .unwrap();
    first.advance_to(Slot(1));
    first.submit(create(11, 1)).unwrap();
    first.checkpoint().unwrap();
    first.advance_to(Slot(2));
    first.submit(create(12, 2)).unwrap();

    let mut second = SimulationHarness::new(
        "recovery-crash-second",
        seed,
        core_config(),
        engine_config(),
    )
    .unwrap();
    second.advance_to(Slot(1));
    second.submit(create(11, 1)).unwrap();
    second.checkpoint().unwrap();
    second.advance_to(Slot(2));
    second.submit(create(12, 2)).unwrap();

    let first_plan = first.arm_next_recovery_crash(seed, &recovery_points);
    let second_plan = second.arm_next_recovery_crash(seed, &recovery_points);
    assert_eq!(first_plan, second_plan);
    assert_eq!(first_plan.point, CrashPoint::RecoveryAfterReplayFrame);

    let first_error = first.restart().unwrap_err();
    let second_error = second.restart().unwrap_err();

    assert!(matches!(
        first_error,
        RecoverEngineError::CrashInjected(plan) if plan == first_plan
    ));
    assert!(matches!(
        second_error,
        RecoverEngineError::CrashInjected(plan) if plan == second_plan
    ));

    let recovered = first.restart().unwrap();
    assert_eq!(
        recovered.recovery.startup_kind,
        RecoveryStartupKind::SnapshotAndWal
    );
    assert_eq!(
        recovered
            .recovery
            .loaded_snapshot_lsn
            .map(allocdb_core::Lsn::get),
        Some(1)
    );
    assert_eq!(recovered.recovery.replayed_wal_frame_count, 1);
    assert_eq!(
        recovered
            .recovery
            .replayed_wal_last_lsn
            .map(allocdb_core::Lsn::get),
        Some(2)
    );
    assert!(first.engine().db().resource(ResourceId(11)).is_some());
    assert!(first.engine().db().resource(ResourceId(12)).is_some());
}

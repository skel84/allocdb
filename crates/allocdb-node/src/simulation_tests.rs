use allocdb_core::command::{ClientRequest, Command};
use allocdb_core::config::Config;
use allocdb_core::ids::{ClientId, HolderId, OperationId, ResourceId, Slot};
use allocdb_core::recovery::RecoveryError;
use allocdb_core::result::ResultCode;
use allocdb_core::wal::{DecodeError, ScanStopReason};
use allocdb_core::wal_file::{WalFile, WalFileError};
use allocdb_core::{ReservationState, ResourceState};

use crate::engine::{
    CheckpointError, CrashPlan, CrashPoint, EngineConfig, RecoverEngineError, RecoveryStartupKind,
    SubmissionError,
};
use crate::simulation::{
    ScheduleAction, ScheduleActionKind, ScheduleObservation, ScheduleObservationKind,
    SimulationHarness, StorageFault,
};

fn core_config() -> Config {
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

fn schedule_submit(
    label: &'static str,
    candidate_slots: &[u64],
    request: ClientRequest,
) -> ScheduleAction {
    ScheduleAction {
        label,
        candidate_slots: candidate_slots.iter().copied().map(Slot).collect(),
        action: ScheduleActionKind::Submit(request),
    }
}

fn schedule_tick(label: &'static str, candidate_slots: &[u64]) -> ScheduleAction {
    ScheduleAction {
        label,
        candidate_slots: candidate_slots.iter().copied().map(Slot).collect(),
        action: ScheduleActionKind::TickExpirations,
    }
}

fn run_schedule(
    name: &str,
    seed: u64,
    actions: &[ScheduleAction],
) -> (SimulationHarness, Vec<ScheduleObservation>) {
    let mut harness = SimulationHarness::new(name, seed, core_config(), engine_config()).unwrap();
    let transcript = harness.explore_schedule(actions).unwrap();
    (harness, transcript)
}

fn seed_for_schedule<F>(name: &str, actions: &[ScheduleAction], predicate: F) -> u64
where
    F: Fn(&[ScheduleObservation]) -> bool,
{
    (0_u64..1024)
        .find(|seed| {
            let scenario_name = format!("{name}-{seed}");
            let (_, transcript) = run_schedule(&scenario_name, *seed, actions);
            predicate(&transcript)
        })
        .expect("test schedule condition must be reachable from one seed")
}

fn submit_observation(
    transcript: &[ScheduleObservation],
    label: &'static str,
) -> crate::simulation::SimulationObservation {
    match &transcript
        .iter()
        .find(|entry| entry.label == label)
        .expect("schedule transcript must contain the requested submit label")
        .outcome
    {
        ScheduleObservationKind::Submit(observation) => *observation,
        ScheduleObservationKind::Tick(_) => panic!("requested label must point to one submit"),
    }
}

fn tick_expiration_ids(transcript: &[ScheduleObservation], label: &'static str) -> Vec<u128> {
    match &transcript
        .iter()
        .find(|entry| entry.label == label)
        .expect("schedule transcript must contain the requested tick label")
        .outcome
    {
        ScheduleObservationKind::Submit(_) => panic!("requested label must point to one tick"),
        ScheduleObservationKind::Tick(observation) => observation
            .expirations
            .iter()
            .map(|entry| entry.reservation_id.get())
            .collect(),
    }
}

fn tick_expiration_deadlines(transcript: &[ScheduleObservation], label: &'static str) -> Vec<u64> {
    match &transcript
        .iter()
        .find(|entry| entry.label == label)
        .expect("schedule transcript must contain the requested tick label")
        .outcome
    {
        ScheduleObservationKind::Submit(_) => panic!("requested label must point to one tick"),
        ScheduleObservationKind::Tick(observation) => observation
            .expirations
            .iter()
            .map(|entry| entry.deadline_slot.get())
            .collect(),
    }
}

fn seed_for_point(point: CrashPoint, enabled_points: &[CrashPoint]) -> u64 {
    (0_u64..256)
        .find(|seed| CrashPlan::from_seed(*seed, enabled_points).point == point)
        .expect("test crash point must be reachable from one seed")
}

fn seed_for_recovery_replay_ordinal(
    target_ordinal: u32,
    replayable_frame_count: u32,
    enabled_points: &[CrashPoint],
) -> u64 {
    (0_u64..1024)
        .find(|seed| {
            let plan = CrashPlan::from_seed(*seed, enabled_points);
            plan.point == CrashPoint::RecoveryAfterReplayFrame
                && plan.selected_recovery_replay_ordinal(replayable_frame_count) == target_ordinal
        })
        .expect("test replay ordinal must be reachable from one seed")
}

fn setup_recovery_harness(
    name: &str,
    seed: u64,
    replay_requests: &[(Slot, ClientRequest)],
) -> SimulationHarness {
    let mut harness = SimulationHarness::new(name, seed, core_config(), engine_config()).unwrap();
    harness.advance_to(Slot(1));
    harness.submit(create(11, 1)).unwrap();
    harness.checkpoint().unwrap();
    for (slot, request) in replay_requests {
        harness.advance_to(*slot);
        harness.submit(request.clone()).unwrap();
    }
    harness
}

fn assert_snapshot_and_wal_recovery(
    harness: &mut SimulationHarness,
    expected_replayed_wal_frame_count: u32,
    expected_replayed_wal_last_lsn: u64,
    expected_resource_ids: &[u128],
) {
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
        Some(1)
    );
    assert_eq!(
        recovered.recovery.replayed_wal_frame_count,
        expected_replayed_wal_frame_count
    );
    assert_eq!(
        recovered
            .recovery
            .replayed_wal_last_lsn
            .map(allocdb_core::Lsn::get),
        Some(expected_replayed_wal_last_lsn)
    );
    for resource_id in expected_resource_ids {
        assert!(
            harness
                .engine()
                .db()
                .resource(ResourceId(*resource_id))
                .is_some()
        );
    }
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
fn seeded_schedule_explores_ingress_contention_reproducibly() {
    let actions = [
        schedule_submit("create", &[1], create(11, 1)),
        schedule_submit("reserve-holder-9", &[2], reserve(11, 9, 2, 3)),
        schedule_submit("reserve-holder-10", &[2], reserve(11, 10, 3, 3)),
    ];
    let holder_nine_seed =
        seed_for_schedule("schedule-ingress-holder-nine", &actions, |transcript| {
            submit_observation(transcript, "reserve-holder-9").result_code == ResultCode::Ok
        });
    let holder_ten_seed =
        seed_for_schedule("schedule-ingress-holder-ten", &actions, |transcript| {
            submit_observation(transcript, "reserve-holder-10").result_code == ResultCode::Ok
        });

    let (first_harness, first_transcript) =
        run_schedule("schedule-ingress-first", holder_nine_seed, &actions);
    let (_, second_transcript) =
        run_schedule("schedule-ingress-second", holder_nine_seed, &actions);
    let (_, different_transcript) =
        run_schedule("schedule-ingress-different", holder_ten_seed, &actions);

    assert_eq!(first_transcript, second_transcript);
    assert_ne!(first_transcript, different_transcript);
    assert_eq!(
        submit_observation(&first_transcript, "reserve-holder-9").result_code,
        ResultCode::Ok
    );
    assert_eq!(
        submit_observation(&first_transcript, "reserve-holder-10").result_code,
        ResultCode::ResourceBusy
    );
    assert_eq!(
        submit_observation(&different_transcript, "reserve-holder-9").result_code,
        ResultCode::ResourceBusy
    );
    assert_eq!(
        submit_observation(&different_transcript, "reserve-holder-10").result_code,
        ResultCode::Ok
    );
    assert_eq!(
        first_harness
            .engine()
            .db()
            .resource(ResourceId(11))
            .unwrap()
            .current_state,
        ResourceState::Reserved
    );
}

#[test]
fn seeded_schedule_explores_due_expiration_order_reproducibly() {
    let actions = [
        schedule_submit("create-11", &[1], create(11, 1)),
        schedule_submit("create-12", &[1], create(12, 2)),
        schedule_submit("reserve-11", &[2], reserve(11, 9, 3, 2)),
        schedule_submit("reserve-12", &[2], reserve(12, 10, 4, 2)),
        schedule_tick("tick-first", &[4]),
        schedule_tick("tick-second", &[5]),
    ];
    let reserve_eleven_first_seed =
        seed_for_schedule("schedule-expiration-eleven-first", &actions, |transcript| {
            tick_expiration_ids(transcript, "tick-first") == vec![3]
        });
    let reserve_twelve_first_seed =
        seed_for_schedule("schedule-expiration-twelve-first", &actions, |transcript| {
            tick_expiration_ids(transcript, "tick-first") == vec![4]
        });

    let (_, first_transcript) = run_schedule(
        "schedule-expiration-first",
        reserve_eleven_first_seed,
        &actions,
    );
    let (_, second_transcript) = run_schedule(
        "schedule-expiration-second",
        reserve_eleven_first_seed,
        &actions,
    );
    let (_, different_transcript) = run_schedule(
        "schedule-expiration-different",
        reserve_twelve_first_seed,
        &actions,
    );

    assert_eq!(first_transcript, second_transcript);
    assert_ne!(first_transcript, different_transcript);
    assert_eq!(
        tick_expiration_ids(&first_transcript, "tick-first"),
        vec![3]
    );
    assert_eq!(
        tick_expiration_ids(&first_transcript, "tick-second"),
        vec![4]
    );
    assert_eq!(
        tick_expiration_ids(&different_transcript, "tick-first"),
        vec![4]
    );
    assert_eq!(
        tick_expiration_ids(&different_transcript, "tick-second"),
        vec![3]
    );
}

#[test]
fn seeded_schedule_preserves_earliest_deadline_priority_under_bounded_ticks() {
    let actions = [
        schedule_submit("create-11", &[1], create(11, 1)),
        schedule_submit("create-12", &[1], create(12, 2)),
        schedule_submit("reserve-11", &[2], reserve(11, 9, 3, 1)),
        schedule_submit("reserve-12", &[2], reserve(12, 10, 4, 3)),
        schedule_tick("tick-first", &[5]),
        schedule_tick("tick-second", &[6]),
    ];

    for seed in 0_u64..32 {
        let scenario_name = format!("schedule-earliest-deadline-{seed}");
        let (_, transcript) = run_schedule(&scenario_name, seed, &actions);

        assert_eq!(
            tick_expiration_deadlines(&transcript, "tick-first"),
            vec![3],
            "seed {seed} must expire the earliest deadline first"
        );
        assert_eq!(
            tick_expiration_deadlines(&transcript, "tick-second"),
            vec![5],
            "seed {seed} must leave the later deadline for the next tick"
        );
    }
}

#[test]
fn seeded_schedule_explores_retry_timing_reproducibly() {
    let actions = [
        schedule_submit("create", &[1], create(11, 1)),
        schedule_submit("reserve-original", &[2], reserve(11, 9, 2, 2)),
        schedule_tick("expire-original", &[4]),
        schedule_submit("retry-original", &[3, 27], reserve(11, 9, 2, 2)),
    ];
    let cached_retry_seed = seed_for_schedule("schedule-retry-cached", &actions, |transcript| {
        submit_observation(transcript, "retry-original").from_retry_cache
    });
    let replayed_retry_seed =
        seed_for_schedule("schedule-retry-replayed", &actions, |transcript| {
            !submit_observation(transcript, "retry-original").from_retry_cache
        });

    let (cached_harness, cached_transcript) =
        run_schedule("schedule-retry-cached", cached_retry_seed, &actions);
    let (_, cached_replay_transcript) =
        run_schedule("schedule-retry-cached-replay", cached_retry_seed, &actions);
    let (replayed_harness, replayed_transcript) =
        run_schedule("schedule-retry-replayed", replayed_retry_seed, &actions);

    assert_eq!(cached_transcript, cached_replay_transcript);
    assert_ne!(cached_transcript, replayed_transcript);
    assert!(submit_observation(&cached_transcript, "retry-original").from_retry_cache);
    assert_eq!(
        submit_observation(&cached_transcript, "retry-original")
            .applied_lsn
            .get(),
        2
    );
    assert!(!submit_observation(&replayed_transcript, "retry-original").from_retry_cache);
    assert_eq!(
        submit_observation(&replayed_transcript, "retry-original")
            .applied_lsn
            .get(),
        4
    );
    assert_eq!(
        cached_harness
            .engine()
            .db()
            .resource(ResourceId(11))
            .unwrap()
            .current_state,
        ResourceState::Available
    );
    assert_eq!(
        replayed_harness
            .engine()
            .db()
            .resource(ResourceId(11))
            .unwrap()
            .current_state,
        ResourceState::Reserved
    );
}

#[test]
#[should_panic(expected = "schedule actions must have unique labels")]
fn schedule_exploration_rejects_duplicate_labels() {
    let actions = [
        schedule_submit("duplicate", &[1], create(11, 1)),
        schedule_submit("duplicate", &[2], create(12, 2)),
    ];

    let _ = run_schedule("schedule-duplicate-labels", 0xabc, &actions);
}

#[test]
#[should_panic(expected = "schedule action requires at least one candidate slot")]
fn schedule_exploration_rejects_empty_candidate_slots() {
    let actions = [ScheduleAction {
        label: "empty-slot-window",
        candidate_slots: Vec::new(),
        action: ScheduleActionKind::Submit(create(11, 1)),
    }];

    let _ = run_schedule("schedule-empty-candidates", 0xdef, &actions);
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

    harness.inject_storage_fault(StorageFault::SyncFailure);
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
fn harness_storage_fault_append_failure_halts_engine_for_negative_path_tests() {
    let mut harness =
        SimulationHarness::new("persist-failure", 0x99, core_config(), engine_config()).unwrap();

    harness.advance_to(Slot(1));
    harness.inject_storage_fault(StorageFault::AppendFailure);

    let error = harness.submit(create(11, 1)).unwrap_err();

    assert!(matches!(error, SubmissionError::WalFile(_)));
    assert!(!harness.metrics().accepting_writes);
}

#[test]
fn simulated_sync_failure_recovers_retryable_write_from_real_wal() {
    let mut harness = SimulationHarness::new(
        "sync-failure-recovery",
        0x155,
        core_config(),
        engine_config(),
    )
    .unwrap();

    harness.advance_to(Slot(1));
    harness.inject_storage_fault(StorageFault::SyncFailure);

    let error = harness.submit(create(11, 1)).unwrap_err();

    assert!(matches!(error, SubmissionError::WalFile(_)));
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
fn simulated_torn_tail_recovers_from_snapshot_and_retries_once() {
    let mut harness =
        SimulationHarness::new("torn-tail-recovery", 0x166, core_config(), engine_config())
            .unwrap();

    harness.advance_to(Slot(1));
    harness.submit(create(11, 1)).unwrap();
    harness.checkpoint().unwrap();

    harness.advance_to(Slot(2));
    harness.submit(create(12, 2)).unwrap();
    harness.inject_storage_fault(StorageFault::TornLastFrameTail { truncate_bytes: 2 });

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
        Some(1)
    );
    assert_eq!(recovered.recovery.replayed_wal_frame_count, 0);
    assert!(harness.engine().db().resource(ResourceId(11)).is_some());
    assert!(harness.engine().db().resource(ResourceId(12)).is_none());
    let wal = WalFile::open(
        harness.engine().wal_path(),
        engine_config().max_command_bytes,
    )
    .unwrap();
    let recovered_wal = wal.recover().unwrap();
    assert_eq!(
        recovered_wal.scan_result.stop_reason,
        ScanStopReason::CleanEof
    );

    let retried = harness.submit(create(12, 2)).unwrap();
    assert!(!retried.from_retry_cache);
    assert_eq!(retried.applied_lsn.get(), 2);
    assert!(harness.engine().db().resource(ResourceId(12)).is_some());
}

#[test]
fn simulated_checksum_corruption_fails_closed_during_restart() {
    let mut harness =
        SimulationHarness::new("checksum-corruption", 0x177, core_config(), engine_config())
            .unwrap();

    harness.advance_to(Slot(1));
    harness.submit(create(11, 1)).unwrap();
    harness.checkpoint().unwrap();

    harness.advance_to(Slot(2));
    harness.submit(create(12, 2)).unwrap();
    harness.inject_storage_fault(StorageFault::CorruptLastFrameChecksum);

    let error = harness.restart().unwrap_err();

    assert!(matches!(
        error,
        RecoverEngineError::Recovery(RecoveryError::WalFile(WalFileError::Corruption {
            error: DecodeError::InvalidChecksum,
            ..
        }))
    ));
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
fn seeded_checkpoint_crash_after_wal_rewrite_is_recoverable() {
    let checkpoint_points = [
        CrashPoint::CheckpointAfterSnapshotWrite,
        CrashPoint::CheckpointAfterWalRewrite,
    ];
    let seed = seed_for_point(CrashPoint::CheckpointAfterWalRewrite, &checkpoint_points);
    let mut harness = SimulationHarness::new(
        "checkpoint-crash-wal-rewrite",
        seed,
        core_config(),
        engine_config(),
    )
    .unwrap();

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
    assert_eq!(planned.point, CrashPoint::CheckpointAfterWalRewrite);
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
fn seeded_recovery_boundary_crashes_are_reproducible_and_resumable() {
    let recovery_points = [
        CrashPoint::RecoveryAfterSnapshotLoad,
        CrashPoint::RecoveryAfterWalTruncate,
        CrashPoint::RecoveryAfterReplayFrame,
    ];
    let replay_requests = [(Slot(2), create(12, 2))];

    for point in recovery_points {
        let seed = seed_for_point(point, &recovery_points);
        let mut first = setup_recovery_harness(
            &format!("recovery-crash-first-{point:?}"),
            seed,
            &replay_requests,
        );
        let mut second = setup_recovery_harness(
            &format!("recovery-crash-second-{point:?}"),
            seed,
            &replay_requests,
        );

        let first_plan = first.arm_next_recovery_crash(seed, &recovery_points);
        let second_plan = second.arm_next_recovery_crash(seed, &recovery_points);
        assert_eq!(first_plan, second_plan);
        assert_eq!(first_plan.point, point);

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

        assert_snapshot_and_wal_recovery(&mut first, 1, 2, &[11, 12]);
    }
}

#[test]
fn seeded_recovery_replay_crash_can_target_later_replayed_frame() {
    let recovery_points = [
        CrashPoint::RecoveryAfterSnapshotLoad,
        CrashPoint::RecoveryAfterWalTruncate,
        CrashPoint::RecoveryAfterReplayFrame,
    ];
    let replay_requests = [(Slot(2), create(12, 2)), (Slot(3), create(13, 3))];
    let seed = seed_for_recovery_replay_ordinal(2, 2, &recovery_points);

    let mut harness = setup_recovery_harness("recovery-crash-late-replay", seed, &replay_requests);
    let planned = harness.arm_next_recovery_crash(seed, &recovery_points);
    assert_eq!(planned.point, CrashPoint::RecoveryAfterReplayFrame);
    assert_eq!(planned.selected_recovery_replay_ordinal(2), 2);

    let error = harness.restart().unwrap_err();

    assert!(matches!(
        error,
        RecoverEngineError::CrashInjected(plan) if plan == planned
    ));

    assert_snapshot_and_wal_recovery(&mut harness, 2, 3, &[11, 12, 13]);
}

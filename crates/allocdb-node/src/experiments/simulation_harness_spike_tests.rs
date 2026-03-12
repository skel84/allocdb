use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use allocdb_core::command::{ClientRequest, Command};
use allocdb_core::config::Config;
use allocdb_core::ids::{ClientId, HolderId, OperationId, ResourceId, Slot};
use allocdb_core::result::ResultCode;
use allocdb_core::{ReservationState, ResourceState};

use super::checkpoint::CheckpointResult;
use super::observe::RecoveryStartupKind;
use super::{
    EngineConfig, EngineMetrics, ExpirationTickResult, PersistFailurePhase, SingleNodeEngine,
    SubmissionError, SubmissionResult,
};

fn test_path(name: &str, seed: u64) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("allocdb-sim-spike-{name}-seed-{seed}-{nanos}.wal"))
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SimAction {
    Submit(ClientRequest),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SimObservation {
    slot: Slot,
    operation_id: OperationId,
    applied_lsn: u64,
    result_code: ResultCode,
}

#[derive(Debug)]
struct SeededChooser {
    state: u64,
}

impl SeededChooser {
    const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_index(&mut self, len: usize) -> usize {
        assert!(len > 0, "chooser requires at least one candidate");
        self.state = self
            .state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let upper = self.state >> 32;
        let upper = usize::try_from(upper).expect("upper bits must fit usize");
        upper % len
    }
}

struct SimulationHarness {
    current_slot: Slot,
    chooser: SeededChooser,
    core_config: Config,
    engine_config: EngineConfig,
    wal_path: PathBuf,
    snapshot_path: PathBuf,
    engine: Option<SingleNodeEngine>,
}

impl SimulationHarness {
    fn new(seed: u64) -> Self {
        let wal_path = test_path("driver", seed);
        let snapshot_path = wal_path.with_extension("snapshot");
        let core_config = core_config();
        let engine_config = engine_config();
        let engine = SingleNodeEngine::open(core_config.clone(), engine_config, &wal_path).unwrap();

        Self {
            current_slot: Slot(0),
            chooser: SeededChooser::new(seed),
            core_config,
            engine_config,
            wal_path,
            snapshot_path,
            engine: Some(engine),
        }
    }

    fn advance_to(&mut self, slot: Slot) {
        assert!(
            slot.get() >= self.current_slot.get(),
            "simulated slot must not move backwards"
        );
        self.current_slot = slot;
    }

    fn run_batch(&mut self, slot: Slot, actions: &[SimAction]) -> Vec<SimObservation> {
        self.advance_to(slot);
        let mut pending = actions.to_vec();
        let mut observations = Vec::with_capacity(actions.len());

        while !pending.is_empty() {
            let index = self.chooser.next_index(pending.len());
            let action = pending.remove(index);
            observations.push(self.execute(action).unwrap());
        }

        observations
    }

    fn submit(&mut self, request: ClientRequest) -> Result<SubmissionResult, SubmissionError> {
        let current_slot = self.current_slot;
        self.engine_mut().submit(current_slot, request)
    }

    fn tick_expirations(&mut self) -> Result<ExpirationTickResult, SubmissionError> {
        let current_slot = self.current_slot;
        self.engine_mut().tick_expirations(current_slot)
    }

    fn checkpoint(&mut self) -> CheckpointResult {
        let snapshot_path = self.snapshot_path.clone();
        self.engine_mut().checkpoint(snapshot_path).unwrap()
    }

    fn metrics(&self) -> EngineMetrics {
        self.engine_ref().metrics(self.current_slot)
    }

    fn restart(&mut self) -> EngineMetrics {
        drop(self.engine.take());
        let recovered = SingleNodeEngine::recover(
            self.core_config.clone(),
            self.engine_config,
            &self.snapshot_path,
            &self.wal_path,
        )
        .unwrap();
        let metrics = recovered.metrics(self.current_slot);
        self.engine = Some(recovered);
        metrics
    }

    fn inject_next_persist_failure(&mut self, phase: PersistFailurePhase) {
        self.engine_mut().inject_next_persist_failure(phase);
    }

    fn engine_ref(&self) -> &SingleNodeEngine {
        self.engine
            .as_ref()
            .expect("simulation harness must have one live engine")
    }

    fn engine_mut(&mut self) -> &mut SingleNodeEngine {
        self.engine
            .as_mut()
            .expect("simulation harness must have one live engine")
    }

    fn execute(&mut self, action: SimAction) -> Result<SimObservation, SubmissionError> {
        match action {
            SimAction::Submit(request) => {
                let result = self.submit(request)?;
                Ok(SimObservation {
                    slot: self.current_slot,
                    operation_id: request.operation_id,
                    applied_lsn: result.applied_lsn.get(),
                    result_code: result.outcome.result_code,
                })
            }
        }
    }
}

impl Drop for SimulationHarness {
    fn drop(&mut self) {
        drop(self.engine.take());
        let _ = fs::remove_file(&self.wal_path);
        let _ = fs::remove_file(&self.snapshot_path);
    }
}

#[test]
fn seeded_batch_order_is_reproducible() {
    let actions = [
        SimAction::Submit(create(11, 1)),
        SimAction::Submit(create(12, 2)),
        SimAction::Submit(create(13, 3)),
    ];

    let mut first = SimulationHarness::new(0x5eed);
    let first_transcript = first.run_batch(Slot(7), &actions);

    let mut second = SimulationHarness::new(0x5eed);
    let second_transcript = second.run_batch(Slot(7), &actions);

    assert_eq!(first_transcript, second_transcript);
    assert_eq!(
        first_transcript
            .iter()
            .map(|entry| entry.operation_id.get())
            .collect::<Vec<_>>(),
        vec![1, 3, 2]
    );
    assert_eq!(
        first_transcript
            .iter()
            .map(|entry| entry.applied_lsn)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert!(first.engine_ref().db().resource(ResourceId(11)).is_some());
    assert!(first.engine_ref().db().resource(ResourceId(12)).is_some());
    assert!(first.engine_ref().db().resource(ResourceId(13)).is_some());
    assert_eq!(first.metrics().core.last_request_slot, Some(Slot(7)));
}

#[test]
fn simulated_slot_driver_handles_expiration_restart_path() {
    let mut harness = SimulationHarness::new(0x15);

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

    let checkpoint = harness.checkpoint();
    assert_eq!(checkpoint.snapshot_lsn.map(allocdb_core::Lsn::get), Some(2));
    assert_eq!(checkpoint.retained_frame_count, 3);

    harness.advance_to(Slot(6));
    let before_tick = harness.metrics();
    assert_eq!(before_tick.core.logical_slot_lag, 4);
    assert_eq!(before_tick.core.expiration_backlog, 1);
    assert_eq!(
        harness
            .engine_ref()
            .db()
            .resource(ResourceId(11))
            .unwrap()
            .current_state,
        ResourceState::Reserved
    );
    assert_eq!(
        harness
            .engine_ref()
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

    let recovered = harness.restart();
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
            .engine_ref()
            .db()
            .resource(ResourceId(11))
            .unwrap()
            .current_state,
        ResourceState::Available
    );
    assert_eq!(
        harness
            .engine_ref()
            .db()
            .reservation(reservation_id, Slot(6))
            .unwrap()
            .state,
        ReservationState::Expired
    );
}

#[test]
fn harness_submit_propagates_wal_failure_for_negative_path_tests() {
    let mut harness = SimulationHarness::new(0x99);

    harness.advance_to(Slot(1));
    harness.inject_next_persist_failure(PersistFailurePhase::BeforeAppend);

    let error = harness.submit(create(11, 1)).unwrap_err();

    assert!(matches!(error, SubmissionError::WalFile(_)));
    assert!(!harness.metrics().accepting_writes);
}

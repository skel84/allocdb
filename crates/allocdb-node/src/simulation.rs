use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use allocdb_core::command::ClientRequest;
use allocdb_core::config::Config;
use allocdb_core::ids::{Lsn, OperationId, ReservationId, Slot};
use allocdb_core::result::ResultCode;

use crate::engine::{
    CheckpointError, CheckpointResult, CrashPlan, CrashPoint, EngineConfig, EngineMetrics,
    EngineOpenError, ExpirationTickResult, PersistFailurePhase, RecoverEngineError,
    SingleNodeEngine, SubmissionError, SubmissionResult,
};

#[cfg(test)]
#[path = "simulation_tests.rs"]
mod tests;

static NEXT_SIMULATION_WORKSPACE_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SimulationObservation {
    pub slot: Slot,
    pub operation_id: OperationId,
    pub applied_lsn: Lsn,
    pub result_code: ResultCode,
    pub from_retry_cache: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ExpirationObservation {
    pub reservation_id: ReservationId,
    pub deadline_slot: Slot,
    pub applied_lsn: Lsn,
    pub result_code: ResultCode,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TickObservation {
    pub processed_count: u32,
    pub last_applied_lsn: Option<Lsn>,
    pub expirations: Vec<ExpirationObservation>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScheduleActionKind {
    Submit(ClientRequest),
    TickExpirations,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ScheduleAction {
    pub label: &'static str,
    pub candidate_slots: Vec<Slot>,
    pub action: ScheduleActionKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ScheduleObservationKind {
    Submit(SimulationObservation),
    Tick(TickObservation),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ScheduleObservation {
    pub label: &'static str,
    pub slot: Slot,
    pub outcome: ScheduleObservationKind,
}

#[derive(Debug)]
struct ReadySetScheduler {
    state: u64,
}

impl ReadySetScheduler {
    const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_index(&mut self, len: usize) -> usize {
        assert!(len > 0, "scheduler requires at least one ready action");

        let next = self.next_u64();
        let len = u64::try_from(len).expect("ready-set length must fit u64");
        usize::try_from(next % len).expect("ready-set index must fit usize")
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut mixed = self.state;
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        mixed ^ (mixed >> 31)
    }
}

#[derive(Debug)]
pub(crate) struct SimulatedSlotDriver {
    current_slot: Slot,
    scheduler: ReadySetScheduler,
}

impl SimulatedSlotDriver {
    pub(crate) const fn new(seed: u64) -> Self {
        Self {
            current_slot: Slot(0),
            scheduler: ReadySetScheduler::new(seed),
        }
    }

    pub(crate) const fn current_slot(&self) -> Slot {
        self.current_slot
    }

    pub(crate) fn advance_to(&mut self, slot: Slot) {
        assert!(
            slot.get() >= self.current_slot.get(),
            "simulated slot must not move backwards"
        );
        self.current_slot = slot;
    }

    fn next_ready_index(&mut self, len: usize) -> usize {
        self.scheduler.next_index(len)
    }

    fn choose_index(&mut self, len: usize) -> usize {
        assert!(len > 0, "scheduler requires at least one candidate");
        if len == 1 {
            0
        } else {
            self.next_ready_index(len)
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ResolvedScheduleAction {
    label: &'static str,
    slot: Slot,
    action: ScheduleActionKind,
}

#[derive(Debug)]
pub(crate) struct SimulationHarness {
    slot_driver: SimulatedSlotDriver,
    core_config: Config,
    engine_config: EngineConfig,
    wal_path: PathBuf,
    snapshot_path: PathBuf,
    engine: Option<SingleNodeEngine>,
    pending_recovery_crash: Option<CrashPlan>,
}

impl SimulationHarness {
    pub(crate) fn new(
        name: &str,
        seed: u64,
        core_config: Config,
        engine_config: EngineConfig,
    ) -> Result<Self, EngineOpenError> {
        let wal_path = simulation_workspace_path(name, seed).with_extension("wal");
        let snapshot_path = wal_path.with_extension("snapshot");
        let engine = SingleNodeEngine::open(core_config.clone(), engine_config, &wal_path)?;

        Ok(Self {
            slot_driver: SimulatedSlotDriver::new(seed),
            core_config,
            engine_config,
            wal_path,
            snapshot_path,
            engine: Some(engine),
            pending_recovery_crash: None,
        })
    }

    pub(crate) const fn current_slot(&self) -> Slot {
        self.slot_driver.current_slot()
    }

    pub(crate) fn advance_to(&mut self, slot: Slot) {
        self.slot_driver.advance_to(slot);
    }

    pub(crate) fn submit_ready_batch(
        &mut self,
        slot: Slot,
        requests: &[ClientRequest],
    ) -> Result<Vec<SimulationObservation>, SubmissionError> {
        self.advance_to(slot);
        let mut pending = requests.to_vec();
        let mut transcript = Vec::with_capacity(pending.len());

        while !pending.is_empty() {
            let index = self.slot_driver.choose_index(pending.len());
            let request = pending.remove(index);
            let result = self.submit(request)?;
            transcript.push(SimulationObservation {
                slot: self.current_slot(),
                operation_id: request.operation_id,
                applied_lsn: result.applied_lsn,
                result_code: result.outcome.result_code,
                from_retry_cache: result.from_retry_cache,
            });
        }

        Ok(transcript)
    }

    pub(crate) fn explore_schedule(
        &mut self,
        actions: &[ScheduleAction],
    ) -> Result<Vec<ScheduleObservation>, SubmissionError> {
        let mut seen_labels = std::collections::BTreeSet::new();
        let current_slot = self.current_slot();
        let mut pending = Vec::with_capacity(actions.len());

        for action in actions {
            assert!(
                seen_labels.insert(action.label),
                "schedule actions must have unique labels"
            );
            let slot = self.resolve_schedule_slot(&action.candidate_slots);
            assert!(
                slot >= current_slot,
                "schedule actions must not resolve before the current simulated slot"
            );
            pending.push(ResolvedScheduleAction {
                label: action.label,
                slot,
                action: action.action,
            });
        }

        pending.sort_unstable_by_key(|action| (action.slot, action.label));
        let mut transcript = Vec::with_capacity(pending.len());

        while let Some(next_slot) = pending.first().map(|action| action.slot) {
            self.advance_to(next_slot);
            let ready_len = pending
                .iter()
                .take_while(|action| action.slot == next_slot)
                .count();
            let index = self.slot_driver.choose_index(ready_len);
            let action = pending.remove(index);
            transcript.push(self.execute_schedule_action(action)?);
        }

        Ok(transcript)
    }

    pub(crate) fn submit(
        &mut self,
        request: ClientRequest,
    ) -> Result<SubmissionResult, SubmissionError> {
        let current_slot = self.current_slot();
        self.engine_mut().submit(current_slot, request)
    }

    pub(crate) fn tick_expirations(&mut self) -> Result<ExpirationTickResult, SubmissionError> {
        let current_slot = self.current_slot();
        self.engine_mut().tick_expirations(current_slot)
    }

    pub(crate) fn checkpoint(&mut self) -> Result<CheckpointResult, CheckpointError> {
        let snapshot_path = self.snapshot_path.clone();
        self.engine_mut().checkpoint(snapshot_path)
    }

    pub(crate) fn metrics(&self) -> EngineMetrics {
        self.engine_ref().metrics(self.current_slot())
    }

    pub(crate) fn restart(&mut self) -> Result<EngineMetrics, RecoverEngineError> {
        drop(self.engine.take());
        let recovered = SingleNodeEngine::recover_with_crash_plan(
            self.core_config.clone(),
            self.engine_config,
            &self.snapshot_path,
            &self.wal_path,
            self.pending_recovery_crash.take(),
        )?;
        let metrics = recovered.metrics(self.current_slot());
        self.engine = Some(recovered);
        Ok(metrics)
    }

    pub(crate) fn inject_next_persist_failure(&mut self, phase: PersistFailurePhase) {
        self.engine_mut().inject_next_persist_failure(phase);
    }

    pub(crate) fn arm_next_engine_crash(
        &mut self,
        seed: u64,
        enabled_points: &[CrashPoint],
    ) -> CrashPlan {
        let plan = CrashPlan::from_seed(seed, enabled_points);
        self.engine_mut().arm_next_crash(plan);
        plan
    }

    pub(crate) fn arm_next_recovery_crash(
        &mut self,
        seed: u64,
        enabled_points: &[CrashPoint],
    ) -> CrashPlan {
        let plan = CrashPlan::from_seed(seed, enabled_points);
        assert!(
            plan.point.is_recovery_boundary(),
            "recovery crash plan must target one recovery boundary"
        );
        self.pending_recovery_crash = Some(plan);
        plan
    }

    pub(crate) fn engine(&self) -> &SingleNodeEngine {
        self.engine_ref()
    }

    fn execute_schedule_action(
        &mut self,
        action: ResolvedScheduleAction,
    ) -> Result<ScheduleObservation, SubmissionError> {
        let outcome = match action.action {
            ScheduleActionKind::Submit(request) => {
                let result = self.submit(request)?;
                ScheduleObservationKind::Submit(SimulationObservation {
                    slot: self.current_slot(),
                    operation_id: request.operation_id,
                    applied_lsn: result.applied_lsn,
                    result_code: result.outcome.result_code,
                    from_retry_cache: result.from_retry_cache,
                })
            }
            ScheduleActionKind::TickExpirations => {
                ScheduleObservationKind::Tick(self.tick_expirations_seeded()?)
            }
        };

        Ok(ScheduleObservation {
            label: action.label,
            slot: self.current_slot(),
            outcome,
        })
    }

    fn tick_expirations_seeded(&mut self) -> Result<TickObservation, SubmissionError> {
        while self.engine_mut().process_next()?.is_some() {}

        let current_slot = self.current_slot();
        let (request_slot, limit, mut due) = {
            let engine = self.engine_ref();
            (
                engine.expiration_request_slot(current_slot),
                usize::try_from(engine.max_expirations_per_tick())
                    .expect("validated max_expirations_per_tick must fit usize"),
                engine.collect_due_expirations(current_slot),
            )
        };
        let mut expirations = Vec::with_capacity(due.len().min(limit));

        while expirations.len() < limit && !due.is_empty() {
            let index = self.slot_driver.choose_index(due.len());
            let target = due.remove(index);
            let result = self
                .engine_mut()
                .apply_due_expiration(request_slot, target)?;
            expirations.push(ExpirationObservation {
                reservation_id: target.reservation_id,
                deadline_slot: target.deadline_slot,
                applied_lsn: result.applied_lsn,
                result_code: result.outcome.result_code,
            });
        }

        Ok(TickObservation {
            processed_count: u32::try_from(expirations.len())
                .expect("expiration count must fit the engine tick result"),
            last_applied_lsn: expirations.last().map(|entry| entry.applied_lsn),
            expirations,
        })
    }

    fn resolve_schedule_slot(&mut self, candidate_slots: &[Slot]) -> Slot {
        assert!(
            !candidate_slots.is_empty(),
            "schedule action requires at least one candidate slot"
        );

        let mut resolved = candidate_slots.to_vec();
        resolved.sort_unstable();
        resolved.dedup();
        let index = self.slot_driver.choose_index(resolved.len());
        resolved[index]
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
}

impl Drop for SimulationHarness {
    fn drop(&mut self) {
        drop(self.engine.take());
        let _ = fs::remove_file(&self.wal_path);
        let _ = fs::remove_file(&self.snapshot_path);
    }
}

fn simulation_workspace_path(name: &str, seed: u64) -> PathBuf {
    let workspace_id = NEXT_SIMULATION_WORKSPACE_ID.fetch_add(1, Ordering::Relaxed);
    let process_id = std::process::id();
    std::env::temp_dir().join(format!(
        "allocdb-sim-{name}-seed-{seed}-pid-{process_id}-run-{workspace_id}"
    ))
}

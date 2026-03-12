use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use allocdb_core::command::ClientRequest;
use allocdb_core::config::Config;
use allocdb_core::ids::{Lsn, OperationId, Slot};
use allocdb_core::result::ResultCode;

use crate::engine::{
    CheckpointError, CheckpointResult, EngineConfig, EngineMetrics, EngineOpenError,
    ExpirationTickResult, PersistFailurePhase, RecoverEngineError, SingleNodeEngine,
    SubmissionError, SubmissionResult,
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
}

#[derive(Debug)]
pub(crate) struct SimulationHarness {
    slot_driver: SimulatedSlotDriver,
    core_config: Config,
    engine_config: EngineConfig,
    wal_path: PathBuf,
    snapshot_path: PathBuf,
    engine: Option<SingleNodeEngine>,
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
            let index = self.slot_driver.next_ready_index(pending.len());
            let request = pending.remove(index);
            let result = self.submit(request)?;
            transcript.push(SimulationObservation {
                slot: self.current_slot(),
                operation_id: request.operation_id,
                applied_lsn: result.applied_lsn,
                result_code: result.outcome.result_code,
            });
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
        let recovered = SingleNodeEngine::recover(
            self.core_config.clone(),
            self.engine_config,
            &self.snapshot_path,
            &self.wal_path,
        )?;
        let metrics = recovered.metrics(self.current_slot());
        self.engine = Some(recovered);
        Ok(metrics)
    }

    pub(crate) fn inject_next_persist_failure(&mut self, phase: PersistFailurePhase) {
        self.engine_mut().inject_next_persist_failure(phase);
    }

    pub(crate) fn engine(&self) -> &SingleNodeEngine {
        self.engine_ref()
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

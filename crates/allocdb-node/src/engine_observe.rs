use allocdb_core::HealthMetrics;
use allocdb_core::ids::{Lsn, Slot};

use super::SingleNodeEngine;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EngineMetrics {
    pub queue_depth: u32,
    pub queue_capacity: u32,
    pub accepting_writes: bool,
    pub core: HealthMetrics,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadError {
    RequiredLsnNotApplied {
        required_lsn: Lsn,
        last_applied_lsn: Option<Lsn>,
    },
}

impl SingleNodeEngine {
    #[must_use]
    /// Returns the current engine-level health snapshot including queue pressure and core metrics.
    ///
    /// # Panics
    ///
    /// Panics only if internal queue state has already violated the configured queue bound.
    pub fn metrics(&self, current_wall_clock_slot: Slot) -> EngineMetrics {
        EngineMetrics {
            queue_depth: u32::try_from(self.queue.len()).expect("queue depth must fit u32"),
            queue_capacity: u32::try_from(self.queue.capacity())
                .expect("queue capacity must fit u32"),
            accepting_writes: self.accepting_writes,
            core: self.db.health_metrics(current_wall_clock_slot),
        }
    }

    /// Enforces the single-node strict-read fence before serving a read from in-memory state.
    ///
    /// # Errors
    ///
    /// Returns [`ReadError`] if the required LSN has not yet been applied locally.
    pub fn enforce_read_fence(&self, required_lsn: Lsn) -> Result<(), ReadError> {
        if required_lsn.get() == 0 {
            return Ok(());
        }

        let last_applied_lsn = self.db.last_applied_lsn();
        if last_applied_lsn.is_some_and(|applied| applied.get() >= required_lsn.get()) {
            Ok(())
        } else {
            Err(ReadError::RequiredLsnNotApplied {
                required_lsn,
                last_applied_lsn,
            })
        }
    }
}

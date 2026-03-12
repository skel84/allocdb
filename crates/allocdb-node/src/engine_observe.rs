use allocdb_core::HealthMetrics;
use allocdb_core::ids::{Lsn, Slot};

use super::SingleNodeEngine;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryStartupKind {
    FreshStart,
    WalOnly,
    SnapshotOnly,
    SnapshotAndWal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecoveryStatus {
    pub startup_kind: RecoveryStartupKind,
    pub loaded_snapshot_lsn: Option<Lsn>,
    pub replayed_wal_frame_count: u32,
    pub replayed_wal_last_lsn: Option<Lsn>,
    pub active_snapshot_lsn: Option<Lsn>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EngineMetrics {
    pub queue_depth: u32,
    pub queue_capacity: u32,
    pub accepting_writes: bool,
    pub recovery: RecoveryStatus,
    pub core: HealthMetrics,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadError {
    EngineHalted,
    RequiredLsnNotApplied {
        required_lsn: Lsn,
        last_applied_lsn: Option<Lsn>,
    },
}

impl SingleNodeEngine {
    #[must_use]
    /// Returns the current engine-level health snapshot including queue pressure, recovery
    /// metadata, and core metrics.
    ///
    /// # Panics
    ///
    /// Panics only if internal queue state has already violated the configured queue bound.
    pub fn metrics(&self, current_wall_clock_slot: Slot) -> EngineMetrics {
        EngineMetrics {
            queue_depth: u32::try_from(self.queue.len()).expect("queue depth must fit u32"),
            queue_capacity: u32::try_from(self.queue.capacity())
                .expect("queue capacity must fit u32"),
            accepting_writes: self.writes_available(),
            recovery: RecoveryStatus {
                startup_kind: self.recovery_startup_kind(),
                loaded_snapshot_lsn: self.startup_recovery.loaded_snapshot_lsn,
                replayed_wal_frame_count: self.startup_recovery.replayed_wal_frame_count,
                replayed_wal_last_lsn: self.startup_recovery.replayed_wal_last_lsn,
                active_snapshot_lsn: self.active_snapshot_lsn,
            },
            core: self.db.health_metrics(current_wall_clock_slot),
        }
    }

    /// Enforces the single-node strict-read fence before serving a read from in-memory state.
    ///
    /// # Errors
    ///
    /// Returns [`ReadError`] if the required LSN has not yet been applied locally.
    pub fn enforce_read_fence(&self, required_lsn: Lsn) -> Result<(), ReadError> {
        if !self.accepting_writes {
            return Err(ReadError::EngineHalted);
        }

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

    fn recovery_startup_kind(&self) -> RecoveryStartupKind {
        match (
            self.startup_recovery.loaded_snapshot,
            self.startup_recovery.replayed_wal_frame_count,
        ) {
            (false, 0) => RecoveryStartupKind::FreshStart,
            (false, _) => RecoveryStartupKind::WalOnly,
            (true, 0) => RecoveryStartupKind::SnapshotOnly,
            (true, _) => RecoveryStartupKind::SnapshotAndWal,
        }
    }
}

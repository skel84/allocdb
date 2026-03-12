use std::path::Path;

use allocdb_core::ids::{Lsn, Slot};
use allocdb_core::snapshot_file::{SnapshotFile, SnapshotFileError};
use allocdb_core::wal::{Frame, RecordType, ScanStopReason};
use allocdb_core::wal_file::{RecoveredWal, WalFileError};

use crate::engine::SingleNodeEngine;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CheckpointResult {
    pub snapshot_lsn: Option<Lsn>,
    pub previous_snapshot_lsn: Option<Lsn>,
    pub retained_frame_count: usize,
}

#[derive(Debug)]
pub enum CheckpointError {
    EngineHalted,
    QueueNotEmpty {
        queue_depth: u32,
    },
    SnapshotFile(SnapshotFileError),
    WalFile(WalFileError),
    WalNotClean(ScanStopReason),
    WalStateMismatch {
        wal_last_lsn: Option<Lsn>,
        snapshot_lsn: Option<Lsn>,
    },
}

impl From<SnapshotFileError> for CheckpointError {
    fn from(error: SnapshotFileError) -> Self {
        Self::SnapshotFile(error)
    }
}

impl From<WalFileError> for CheckpointError {
    fn from(error: WalFileError) -> Self {
        Self::WalFile(error)
    }
}

impl SingleNodeEngine {
    /// Persists one new snapshot and rewrites the WAL with one-checkpoint overlap.
    ///
    /// The new snapshot becomes the active anchor, but retained WAL history still starts after the
    /// previously successful snapshot anchor so recovery remains safe across checkpoint replacement.
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointError`] if the engine is halted, queued writes still exist, the WAL is
    /// not clean, or snapshot/WAL persistence fails.
    ///
    /// # Panics
    ///
    /// Panics only if validated queue bounds no longer fit into `u32`.
    pub fn checkpoint(
        &mut self,
        snapshot_path: impl AsRef<Path>,
    ) -> Result<CheckpointResult, CheckpointError> {
        if !self.accepting_writes {
            return Err(CheckpointError::EngineHalted);
        }

        if self.queue.len() != 0 {
            return Err(CheckpointError::QueueNotEmpty {
                queue_depth: u32::try_from(self.queue.len()).expect("queue depth must fit u32"),
            });
        }

        let snapshot = self.db.snapshot();
        let recovered_wal = self.require_clean_wal().inspect_err(|_| {
            self.accepting_writes = false;
        })?;
        let wal_last_lsn = recovered_wal
            .scan_result
            .frames
            .last()
            .map(|frame| frame.lsn);
        if wal_last_lsn != snapshot.last_applied_lsn {
            self.accepting_writes = false;
            return Err(CheckpointError::WalStateMismatch {
                wal_last_lsn,
                snapshot_lsn: snapshot.last_applied_lsn,
            });
        }

        let previous_snapshot_lsn = self.active_snapshot_lsn;
        let snapshot_file = SnapshotFile::new(snapshot_path);
        snapshot_file.write_snapshot(&snapshot)?;

        let retained_frames = retained_frames(
            &recovered_wal,
            previous_snapshot_lsn,
            snapshot.last_applied_lsn,
            snapshot.last_request_slot,
        );
        if let Err(error) = self.wal.replace_with_frames(&retained_frames) {
            self.accepting_writes = false;
            self.active_snapshot_lsn = snapshot.last_applied_lsn;
            return Err(CheckpointError::WalFile(error));
        }
        self.active_snapshot_lsn = snapshot.last_applied_lsn;

        Ok(CheckpointResult {
            snapshot_lsn: snapshot.last_applied_lsn,
            previous_snapshot_lsn,
            retained_frame_count: retained_frames.len(),
        })
    }

    fn require_clean_wal(&self) -> Result<RecoveredWal, CheckpointError> {
        let recovered = self.wal.recover()?;
        match recovered.scan_result.stop_reason {
            ScanStopReason::CleanEof => Ok(recovered),
            stop_reason => Err(CheckpointError::WalNotClean(stop_reason)),
        }
    }
}

fn retained_frames(
    recovered_wal: &RecoveredWal,
    previous_snapshot_lsn: Option<Lsn>,
    snapshot_lsn: Option<Lsn>,
    snapshot_request_slot: Option<Slot>,
) -> Vec<Frame> {
    let retention_floor = previous_snapshot_lsn.map_or(0, Lsn::get);
    let mut retained: Vec<_> = recovered_wal
        .scan_result
        .frames
        .iter()
        .filter(|frame| frame.lsn.get() > retention_floor)
        .cloned()
        .collect();

    if let Some(snapshot_lsn) = snapshot_lsn {
        retained.push(Frame {
            lsn: snapshot_lsn,
            request_slot: snapshot_request_slot.unwrap_or(Slot(0)),
            record_type: RecordType::SnapshotMarker,
            payload: Vec::new(),
        });
    }

    retained
}

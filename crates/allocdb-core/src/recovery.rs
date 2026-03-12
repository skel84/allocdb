use crate::command::CommandContext;
use crate::command_codec::{CommandCodecError, decode_client_request, decode_internal_command};
use crate::config::{Config, ConfigError};
use crate::ids::{Lsn, Slot};
use crate::snapshot::SnapshotError;
use crate::snapshot_file::{SnapshotFile, SnapshotFileError};
use crate::state_machine::AllocDb;
use crate::wal::RecordType;
use crate::wal_file::{RecoveredWal, WalFile, WalFileError};
use log::{error, info};

#[derive(Debug)]
pub struct RecoveryResult {
    pub db: AllocDb,
    pub recovered_wal: RecoveredWal,
    pub loaded_snapshot: bool,
    pub loaded_snapshot_lsn: Option<Lsn>,
    pub replayed_wal_frame_count: u32,
    pub replayed_wal_last_lsn: Option<Lsn>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplayError {
    NonMonotonicLsn {
        previous_lsn: Lsn,
        next_lsn: Lsn,
    },
    RewoundRequestSlot {
        previous_request_slot: Slot,
        next_request_slot: Slot,
    },
}

#[derive(Debug)]
pub enum RecoveryError {
    Config(ConfigError),
    SnapshotFile(SnapshotFileError),
    Snapshot(SnapshotError),
    WalFile(WalFileError),
    CommandCodec(CommandCodecError),
    Replay(ReplayError),
}

impl From<ConfigError> for RecoveryError {
    fn from(error: ConfigError) -> Self {
        Self::Config(error)
    }
}

impl From<SnapshotFileError> for RecoveryError {
    fn from(error: SnapshotFileError) -> Self {
        Self::SnapshotFile(error)
    }
}

impl From<SnapshotError> for RecoveryError {
    fn from(error: SnapshotError) -> Self {
        Self::Snapshot(error)
    }
}

impl From<WalFileError> for RecoveryError {
    fn from(error: WalFileError) -> Self {
        Self::WalFile(error)
    }
}

impl From<CommandCodecError> for RecoveryError {
    fn from(error: CommandCodecError) -> Self {
        Self::CommandCodec(error)
    }
}

impl From<ReplayError> for RecoveryError {
    fn from(error: ReplayError) -> Self {
        Self::Replay(error)
    }
}

/// Recovers one allocator by loading a snapshot, truncating the WAL to the last valid prefix,
/// and replaying later frames through the live apply path.
///
/// # Errors
///
/// Returns [`RecoveryError`] if snapshot loading or restore, WAL recovery, replay-order
/// validation, or payload decoding fails.
pub fn recover_allocdb(
    config: Config,
    snapshot_file: &SnapshotFile,
    wal_file: &WalFile,
) -> Result<RecoveryResult, RecoveryError> {
    let result = recover_allocdb_impl(config, snapshot_file, wal_file);

    match &result {
        Ok(result) => {
            info!(
                "recovery complete: loaded_snapshot={} loaded_snapshot_lsn={:?} replayed_wal_frame_count={} replayed_wal_last_lsn={:?}",
                result.loaded_snapshot,
                result.loaded_snapshot_lsn,
                result.replayed_wal_frame_count,
                result.replayed_wal_last_lsn,
            );
        }
        Err(error) => log_recovery_failure(error, snapshot_file, wal_file),
    }

    result
}

fn recover_allocdb_impl(
    config: Config,
    snapshot_file: &SnapshotFile,
    wal_file: &WalFile,
) -> Result<RecoveryResult, RecoveryError> {
    let snapshot = snapshot_file.load_snapshot()?;
    let loaded_snapshot = snapshot.is_some();
    let loaded_snapshot_lsn = snapshot.as_ref().and_then(|value| value.last_applied_lsn);
    let mut db = match snapshot {
        Some(snapshot) => AllocDb::from_snapshot(config, snapshot)?,
        None => AllocDb::new(config)?,
    };

    let recovered_wal = wal_file.truncate_to_valid_prefix()?;
    let mut replayed_wal_frame_count = 0_u32;
    let mut replayed_wal_last_lsn = None;
    let mut replay_last_lsn = db.last_applied_lsn();
    let mut replay_last_request_slot = db.last_request_slot();
    for frame in &recovered_wal.scan_result.frames {
        if loaded_snapshot_lsn.is_some_and(|snapshot_lsn| frame.lsn.get() <= snapshot_lsn.get()) {
            continue;
        }

        validate_replay_order(
            replay_last_lsn,
            replay_last_request_slot,
            frame.lsn,
            frame.request_slot,
        )?;

        let context = CommandContext {
            lsn: frame.lsn,
            request_slot: frame.request_slot,
        };

        match frame.record_type {
            RecordType::ClientCommand => {
                let request = decode_client_request(&frame.payload)?;
                db.apply_client(context, request);
                replayed_wal_frame_count = replayed_wal_frame_count.saturating_add(1);
                replayed_wal_last_lsn = Some(frame.lsn);
            }
            RecordType::InternalCommand => {
                let command = decode_internal_command(&frame.payload)?;
                db.apply_internal(context, command);
                replayed_wal_frame_count = replayed_wal_frame_count.saturating_add(1);
                replayed_wal_last_lsn = Some(frame.lsn);
            }
            RecordType::SnapshotMarker => {}
        }

        replay_last_lsn = Some(frame.lsn);
        replay_last_request_slot = Some(frame.request_slot);
    }

    Ok(RecoveryResult {
        db,
        recovered_wal,
        loaded_snapshot,
        loaded_snapshot_lsn,
        replayed_wal_frame_count,
        replayed_wal_last_lsn,
    })
}

fn validate_replay_order(
    previous_lsn: Option<Lsn>,
    previous_request_slot: Option<Slot>,
    next_lsn: Lsn,
    next_request_slot: Slot,
) -> Result<(), ReplayError> {
    if let Some(previous_lsn) = previous_lsn {
        if next_lsn.get() <= previous_lsn.get() {
            return Err(ReplayError::NonMonotonicLsn {
                previous_lsn,
                next_lsn,
            });
        }
    }

    if let Some(previous_request_slot) = previous_request_slot {
        if next_request_slot.get() < previous_request_slot.get() {
            return Err(ReplayError::RewoundRequestSlot {
                previous_request_slot,
                next_request_slot,
            });
        }
    }

    Ok(())
}

fn log_recovery_failure(error: &RecoveryError, snapshot_file: &SnapshotFile, wal_file: &WalFile) {
    let snapshot_path = snapshot_file.path().display();
    let wal_path = wal_file.path().display();

    match error {
        RecoveryError::Config(config_error) => {
            error!(
                "recovery aborted due to invalid configuration snapshot_path={snapshot_path} wal_path={wal_path} error={config_error:?}"
            );
        }
        RecoveryError::SnapshotFile(snapshot_error) => {
            error!(
                "recovery failed while loading snapshot snapshot_path={snapshot_path} wal_path={wal_path} error={snapshot_error:?}"
            );
        }
        RecoveryError::Snapshot(snapshot_error) => {
            error!(
                "recovery rejected semantically invalid snapshot snapshot_path={snapshot_path} wal_path={wal_path} error={snapshot_error:?}"
            );
        }
        RecoveryError::WalFile(wal_error) => {
            error!(
                "recovery failed while scanning wal wal_path={wal_path} snapshot_path={snapshot_path} error={wal_error:?}"
            );
        }
        RecoveryError::CommandCodec(codec_error) => {
            error!(
                "recovery rejected wal payload wal_path={wal_path} snapshot_path={snapshot_path} error={codec_error:?}"
            );
        }
        RecoveryError::Replay(replay_error) => {
            error!(
                "recovery rejected wal replay ordering wal_path={wal_path} snapshot_path={snapshot_path} error={replay_error:?}"
            );
        }
    }
}

#[cfg(test)]
#[path = "recovery_issue_30_tests.rs"]
mod issue_30_tests;
#[cfg(test)]
#[path = "recovery_tests.rs"]
mod tests;

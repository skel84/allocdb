use crate::command::CommandContext;
use crate::command_codec::{CommandCodecError, decode_client_request, decode_internal_command};
use crate::config::{Config, ConfigError};
use crate::ids::Lsn;
use crate::snapshot_file::{SnapshotFile, SnapshotFileError};
use crate::state_machine::AllocDb;
use crate::wal::RecordType;
use crate::wal_file::{RecoveredWal, WalFile, WalFileError};
use log::info;

#[derive(Debug)]
pub struct RecoveryResult {
    pub db: AllocDb,
    pub recovered_wal: RecoveredWal,
    pub loaded_snapshot: bool,
    pub loaded_snapshot_lsn: Option<Lsn>,
    pub replayed_wal_frame_count: u32,
    pub replayed_wal_last_lsn: Option<Lsn>,
}

#[derive(Debug)]
pub enum RecoveryError {
    Config(ConfigError),
    SnapshotFile(SnapshotFileError),
    WalFile(WalFileError),
    CommandCodec(CommandCodecError),
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

/// Recovers one allocator by loading a snapshot, truncating the WAL to the last valid prefix,
/// and replaying later frames through the live apply path.
///
/// # Errors
///
/// Returns [`RecoveryError`] if snapshot loading, WAL recovery, or payload decoding fails.
pub fn recover_allocdb(
    config: Config,
    snapshot_file: &SnapshotFile,
    wal_file: &WalFile,
) -> Result<RecoveryResult, RecoveryError> {
    let snapshot = snapshot_file.load_snapshot()?;
    let loaded_snapshot = snapshot.is_some();
    let loaded_snapshot_lsn = snapshot.as_ref().and_then(|value| value.last_applied_lsn);
    let mut db = match snapshot {
        Some(snapshot) => AllocDb::from_snapshot(config, snapshot)
            .map_err(|error| RecoveryError::SnapshotFile(SnapshotFileError::Decode(error)))?,
        None => AllocDb::new(config)?,
    };

    let recovered_wal = wal_file.truncate_to_valid_prefix()?;
    let mut replayed_wal_frame_count = 0_u32;
    let mut replayed_wal_last_lsn = None;
    for frame in &recovered_wal.scan_result.frames {
        if loaded_snapshot_lsn.is_some_and(|snapshot_lsn| frame.lsn.get() <= snapshot_lsn.get()) {
            continue;
        }

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
    }

    Ok(RecoveryResult {
        db,
        recovered_wal,
        loaded_snapshot,
        loaded_snapshot_lsn,
        replayed_wal_frame_count,
        replayed_wal_last_lsn,
    })
    .inspect(|result| {
        info!(
            "recovery complete: loaded_snapshot_lsn={:?} replayed_wal_frame_count={} replayed_wal_last_lsn={:?}",
            result.loaded_snapshot_lsn,
            result.replayed_wal_frame_count,
            result.replayed_wal_last_lsn,
        );
    })
}

#[cfg(test)]
#[path = "recovery_tests.rs"]
mod tests;

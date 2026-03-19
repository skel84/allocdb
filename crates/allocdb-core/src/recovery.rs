use std::convert::Infallible;

use crate::command::CommandContext;
use crate::command_codec::{CommandCodecError, decode_client_request, decode_internal_command};
use crate::config::{Config, ConfigError};
use crate::ids::{Lsn, Slot};
use crate::snapshot::SnapshotError;
use crate::snapshot_file::{SnapshotFile, SnapshotFileError};
use crate::state_machine::{AllocDb, SlotOverflowError, SlotOverflowKind};
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
pub enum RecoveryBoundary {
    AfterSnapshotLoad,
    AfterWalTruncate,
    AfterReplayFrame {
        lsn: Lsn,
        record_type: RecordType,
        replay_ordinal: u32,
        replayable_frame_count: u32,
    },
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
    SlotOverflow {
        lsn: Lsn,
        kind: SlotOverflowKind,
        request_slot: Slot,
        delta: u64,
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

#[derive(Debug)]
pub enum RecoveryObserverError<E> {
    Recovery(RecoveryError),
    Observer(E),
}

impl<E> From<ConfigError> for RecoveryObserverError<E> {
    fn from(error: ConfigError) -> Self {
        Self::Recovery(RecoveryError::from(error))
    }
}

impl<E> From<SnapshotFileError> for RecoveryObserverError<E> {
    fn from(error: SnapshotFileError) -> Self {
        Self::Recovery(RecoveryError::from(error))
    }
}

impl<E> From<SnapshotError> for RecoveryObserverError<E> {
    fn from(error: SnapshotError) -> Self {
        Self::Recovery(RecoveryError::from(error))
    }
}

impl<E> From<WalFileError> for RecoveryObserverError<E> {
    fn from(error: WalFileError) -> Self {
        Self::Recovery(RecoveryError::from(error))
    }
}

impl<E> From<CommandCodecError> for RecoveryObserverError<E> {
    fn from(error: CommandCodecError) -> Self {
        Self::Recovery(RecoveryError::from(error))
    }
}

impl<E> From<ReplayError> for RecoveryObserverError<E> {
    fn from(error: ReplayError) -> Self {
        Self::Recovery(RecoveryError::from(error))
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
    recover_allocdb_with_observer(
        config,
        snapshot_file,
        wal_file,
        |_| Ok::<(), Infallible>(()),
    )
    .map_err(|error| match error {
        RecoveryObserverError::Recovery(error) => error,
        RecoveryObserverError::Observer(never) => match never {},
    })
}

/// Recovers one allocator while notifying one observer at named recovery boundaries.
///
/// # Errors
///
/// Returns [`RecoveryObserverError::Recovery`] if snapshot loading, WAL scanning, replay-order
/// validation, or payload decoding fails. Returns [`RecoveryObserverError::Observer`] if the
/// caller-provided boundary observer aborts recovery.
pub fn recover_allocdb_with_observer<E, F>(
    config: Config,
    snapshot_file: &SnapshotFile,
    wal_file: &WalFile,
    mut observer: F,
) -> Result<RecoveryResult, RecoveryObserverError<E>>
where
    F: FnMut(RecoveryBoundary) -> Result<(), E>,
{
    let result = recover_allocdb_impl(config, snapshot_file, wal_file, &mut observer);

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
        Err(RecoveryObserverError::Recovery(error)) => {
            log_recovery_failure(error, snapshot_file, wal_file);
        }
        Err(RecoveryObserverError::Observer(_)) => {}
    }

    result
}

fn recover_allocdb_impl<E, F>(
    config: Config,
    snapshot_file: &SnapshotFile,
    wal_file: &WalFile,
    observer: &mut F,
) -> Result<RecoveryResult, RecoveryObserverError<E>>
where
    F: FnMut(RecoveryBoundary) -> Result<(), E>,
{
    let snapshot = snapshot_file.load_snapshot()?;
    let loaded_snapshot = snapshot.is_some();
    let loaded_snapshot_lsn = snapshot.as_ref().and_then(|value| value.last_applied_lsn);
    let mut db = match snapshot {
        Some(snapshot) => AllocDb::from_snapshot(config, snapshot)?,
        None => AllocDb::new(config)?,
    };
    observer(RecoveryBoundary::AfterSnapshotLoad).map_err(RecoveryObserverError::Observer)?;

    let recovered_wal = wal_file.truncate_to_valid_prefix()?;
    observer(RecoveryBoundary::AfterWalTruncate).map_err(RecoveryObserverError::Observer)?;
    let replayable_frame_count = u32::try_from(
        recovered_wal
            .scan_result
            .frames
            .iter()
            .filter(|frame| {
                loaded_snapshot_lsn.is_none_or(|snapshot_lsn| frame.lsn.get() > snapshot_lsn.get())
                    && !matches!(frame.record_type, RecordType::SnapshotMarker)
            })
            .count(),
    )
    .expect("replayable frame count must fit u32");
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
                validate_replay_slot_math(
                    db.validate_client_request_slot(frame.request_slot, &request.command),
                    frame.lsn,
                )?;
                db.apply_client(context, request);
                replayed_wal_frame_count = replayed_wal_frame_count.saturating_add(1);
                replayed_wal_last_lsn = Some(frame.lsn);
                observer(RecoveryBoundary::AfterReplayFrame {
                    lsn: frame.lsn,
                    record_type: frame.record_type,
                    replay_ordinal: replayed_wal_frame_count,
                    replayable_frame_count,
                })
                .map_err(RecoveryObserverError::Observer)?;
            }
            RecordType::InternalCommand => {
                let command = decode_internal_command(&frame.payload)?;
                validate_replay_slot_math(
                    db.validate_internal_request_slot(frame.request_slot, &command),
                    frame.lsn,
                )?;
                db.apply_internal(context, command);
                replayed_wal_frame_count = replayed_wal_frame_count.saturating_add(1);
                replayed_wal_last_lsn = Some(frame.lsn);
                observer(RecoveryBoundary::AfterReplayFrame {
                    lsn: frame.lsn,
                    record_type: frame.record_type,
                    replay_ordinal: replayed_wal_frame_count,
                    replayable_frame_count,
                })
                .map_err(RecoveryObserverError::Observer)?;
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

fn validate_replay_slot_math(
    result: Result<(), SlotOverflowError>,
    lsn: Lsn,
) -> Result<(), ReplayError> {
    result.map_err(|error| ReplayError::SlotOverflow {
        lsn,
        kind: error.kind,
        request_slot: error.request_slot,
        delta: error.delta,
    })
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
#[path = "recovery_issue_31_tests.rs"]
mod issue_31_tests;
#[cfg(test)]
#[path = "recovery_revoke_tests.rs"]
mod revoke_tests;
#[cfg(test)]
#[path = "recovery_tests.rs"]
mod tests;

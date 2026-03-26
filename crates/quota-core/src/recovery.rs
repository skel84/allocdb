use std::convert::Infallible;

use crate::command::CommandContext;
use crate::command_codec::{CommandCodecError, decode_client_request, decode_internal_command};
use crate::config::{Config, ConfigError};
use crate::ids::{Lsn, Slot};
use crate::snapshot::SnapshotError;
use crate::snapshot_file::{SnapshotFile, SnapshotFileError};
use crate::state_machine::QuotaDb;
use crate::wal::RecordType;
use crate::wal_file::{RecoveredWal, WalFile, WalFileError};

#[derive(Debug)]
pub struct RecoveryResult {
    pub db: QuotaDb,
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

pub fn recover_quota(
    config: Config,
    snapshot_file: &SnapshotFile,
    wal_file: &mut WalFile,
) -> Result<RecoveryResult, RecoveryError> {
    recover_quota_with_observer(
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

pub fn recover_quota_with_observer<E, F>(
    config: Config,
    snapshot_file: &SnapshotFile,
    wal_file: &mut WalFile,
    mut observer: F,
) -> Result<RecoveryResult, RecoveryObserverError<E>>
where
    F: FnMut(RecoveryBoundary) -> Result<(), E>,
{
    let snapshot = snapshot_file.load_snapshot()?;
    let loaded_snapshot = snapshot.is_some();
    let loaded_snapshot_lsn = snapshot.as_ref().and_then(|value| value.last_applied_lsn);
    let mut db = match snapshot {
        Some(snapshot) => QuotaDb::from_snapshot(config, snapshot)?,
        None => QuotaDb::new(config)?,
    };
    observer(RecoveryBoundary::AfterSnapshotLoad).map_err(RecoveryObserverError::Observer)?;

    let recovered_wal = wal_file.truncate_to_valid_prefix()?;
    observer(RecoveryBoundary::AfterWalTruncate).map_err(RecoveryObserverError::Observer)?;

    let replayable_frames: Vec<_> = recovered_wal
        .scan_result
        .frames
        .iter()
        .filter(|frame| {
            loaded_snapshot_lsn.is_none_or(|snapshot_lsn| frame.lsn.get() > snapshot_lsn.get())
        })
        .collect();
    let replayable_frame_count =
        u32::try_from(replayable_frames.len()).expect("replayable frame count must fit u32");

    let mut replayed_wal_frame_count = 0_u32;
    let mut replayed_wal_last_lsn = None;
    let mut replay_last_lsn = db.last_applied_lsn();
    let mut replay_last_request_slot = db.last_request_slot();

    for frame in replayable_frames {
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
                let _ = db.apply_client(context, request);
            }
            RecordType::InternalCommand => {
                let command = decode_internal_command(&frame.payload)?;
                let _ = db.apply_internal(context, command);
            }
            RecordType::SnapshotMarker => {}
        }
        replayed_wal_frame_count += 1;
        replayed_wal_last_lsn = Some(frame.lsn);
        replay_last_lsn = Some(frame.lsn);
        replay_last_request_slot = Some(frame.request_slot);
        observer(RecoveryBoundary::AfterReplayFrame {
            lsn: frame.lsn,
            record_type: frame.record_type,
            replay_ordinal: replayed_wal_frame_count,
            replayable_frame_count,
        })
        .map_err(RecoveryObserverError::Observer)?;
    }

    db.set_progress(
        replayed_wal_last_lsn.or(db.last_applied_lsn()),
        replay_last_request_slot,
    );
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

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Write;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::{
        Config,
        command::{ClientRequest, Command, CommandContext},
        command_codec::encode_client_request,
        ids::{BucketId, ClientId, Lsn, OperationId, Slot},
        snapshot_file::SnapshotFile,
        state_machine::QuotaDb,
        wal::{Frame, RecordType},
        wal_file::WalFile,
    };

    use super::{RecoveryError, recover_quota};

    fn test_path(name: &str, extension: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("quota-core-{name}-{nanos}.{extension}"))
    }

    fn config() -> Config {
        Config {
            max_buckets: 8,
            max_operations: 16,
            max_batch_len: 8,
            max_client_retry_window_slots: 8,
            max_wal_payload_bytes: 1024,
            max_snapshot_bytes: 4096,
        }
    }

    #[test]
    fn recovery_replays_frames_after_snapshot_boundary() {
        let snapshot_path = test_path("recovery-snapshot", "snapshot");
        let wal_path = test_path("recovery-wal", "wal");
        let snapshot_file = SnapshotFile::new(&snapshot_path, config().max_snapshot_bytes);
        let mut live = QuotaDb::new(config()).unwrap();
        let create = ClientRequest {
            operation_id: OperationId(1),
            client_id: ClientId(1),
            command: Command::CreateBucket {
                bucket_id: BucketId(11),
                limit: 8,
                initial_balance: 2,
                refill_rate_per_slot: 2,
            },
        };
        let debit = ClientRequest {
            operation_id: OperationId(2),
            client_id: ClientId(1),
            command: Command::Debit {
                bucket_id: BucketId(11),
                amount: 2,
            },
        };
        let second_debit = ClientRequest {
            operation_id: OperationId(3),
            client_id: ClientId(1),
            command: Command::Debit {
                bucket_id: BucketId(11),
                amount: 1,
            },
        };

        let _ = live.apply_client(
            CommandContext {
                lsn: Lsn(1),
                request_slot: Slot(1),
            },
            create,
        );
        snapshot_file.write_snapshot(&live.snapshot()).unwrap();
        let _ = live.apply_client(
            CommandContext {
                lsn: Lsn(2),
                request_slot: Slot(3),
            },
            debit,
        );
        let _ = live.apply_client(
            CommandContext {
                lsn: Lsn(3),
                request_slot: Slot(5),
            },
            second_debit,
        );

        let mut wal = WalFile::open(&wal_path, 64).unwrap();
        wal.append_frame(&Frame {
            lsn: Lsn(2),
            request_slot: Slot(3),
            record_type: RecordType::ClientCommand,
            payload: encode_client_request(debit),
        })
        .unwrap();
        wal.append_frame(&Frame {
            lsn: Lsn(3),
            request_slot: Slot(5),
            record_type: RecordType::ClientCommand,
            payload: encode_client_request(second_debit),
        })
        .unwrap();
        wal.sync().unwrap();

        let recovered = recover_quota(config(), &snapshot_file, &mut wal).unwrap();
        assert_eq!(recovered.replayed_wal_frame_count, 2);
        assert_eq!(recovered.replayed_wal_last_lsn, Some(Lsn(3)));
        assert_eq!(recovered.db.snapshot(), live.snapshot());

        fs::remove_file(snapshot_path).unwrap();
        fs::remove_file(wal_path).unwrap();
    }

    #[test]
    fn recovery_replays_from_empty_snapshot() {
        let snapshot_path = test_path("recovery-rewound-snapshot", "snapshot");
        let wal_path = test_path("recovery-rewound-wal", "wal");
        let snapshot_file = SnapshotFile::new(&snapshot_path, config().max_snapshot_bytes);
        snapshot_file
            .write_snapshot(&QuotaDb::new(config()).unwrap().snapshot())
            .unwrap();

        let config = config();
        let mut wal = WalFile::open(&wal_path, config.max_wal_payload_bytes).unwrap();
        wal.append_frame(&Frame {
            lsn: Lsn(1),
            request_slot: Slot(1),
            record_type: RecordType::ClientCommand,
            payload: encode_client_request(ClientRequest {
                operation_id: OperationId(1),
                client_id: ClientId(1),
                command: Command::CreateBucket {
                    bucket_id: BucketId(11),
                    limit: 8,
                    initial_balance: 4,
                    refill_rate_per_slot: 1,
                },
            }),
        })
        .unwrap();
        wal.sync().unwrap();

        let recovered = recover_quota(config, &snapshot_file, &mut wal).unwrap();
        assert_eq!(recovered.replayed_wal_frame_count, 1);
        assert_eq!(recovered.db.snapshot().buckets.len(), 1);

        fs::remove_file(snapshot_path).unwrap();
        fs::remove_file(wal_path).unwrap();
    }

    #[test]
    fn recovery_rejects_rewound_request_slot_against_snapshot_progress() {
        let snapshot_path = test_path("recovery-rewound-snapshot-progress", "snapshot");
        let wal_path = test_path("recovery-rewound-wal-progress", "wal");
        let snapshot_file = SnapshotFile::new(&snapshot_path, config().max_snapshot_bytes);
        let mut db = QuotaDb::new(config()).unwrap();
        let _ = db.apply_client(
            CommandContext {
                lsn: Lsn(2),
                request_slot: Slot(2),
            },
            ClientRequest {
                operation_id: OperationId(1),
                client_id: ClientId(1),
                command: Command::CreateBucket {
                    bucket_id: BucketId(11),
                    limit: 8,
                    initial_balance: 4,
                    refill_rate_per_slot: 0,
                },
            },
        );
        snapshot_file.write_snapshot(&db.snapshot()).unwrap();

        let config = config();
        let mut wal = WalFile::open(&wal_path, config.max_wal_payload_bytes).unwrap();
        wal.append_frame(&Frame {
            lsn: Lsn(3),
            request_slot: Slot(1),
            record_type: RecordType::ClientCommand,
            payload: encode_client_request(ClientRequest {
                operation_id: OperationId(2),
                client_id: ClientId(1),
                command: Command::Debit {
                    bucket_id: BucketId(11),
                    amount: 1,
                },
            }),
        })
        .unwrap();
        wal.sync().unwrap();

        let error = recover_quota(config, &snapshot_file, &mut wal).unwrap_err();
        assert!(matches!(error, RecoveryError::Replay(_)));

        fs::remove_file(snapshot_path).unwrap();
        fs::remove_file(wal_path).unwrap();
    }

    #[test]
    fn recovery_truncates_torn_tail_without_inventing_refilled_debit() {
        let snapshot_path = test_path("recovery-torn-tail", "snapshot");
        let wal_path = test_path("recovery-torn-tail", "wal");
        let snapshot_file = SnapshotFile::new(&snapshot_path, config().max_snapshot_bytes);
        let mut wal = WalFile::open(&wal_path, config().max_wal_payload_bytes).unwrap();

        let create = ClientRequest {
            operation_id: OperationId(1),
            client_id: ClientId(1),
            command: Command::CreateBucket {
                bucket_id: BucketId(11),
                limit: 8,
                initial_balance: 2,
                refill_rate_per_slot: 2,
            },
        };
        let debit = ClientRequest {
            operation_id: OperationId(2),
            client_id: ClientId(1),
            command: Command::Debit {
                bucket_id: BucketId(11),
                amount: 3,
            },
        };

        wal.append_frame(&Frame {
            lsn: Lsn(1),
            request_slot: Slot(1),
            record_type: RecordType::ClientCommand,
            payload: encode_client_request(create),
        })
        .unwrap();

        let torn = Frame {
            lsn: Lsn(2),
            request_slot: Slot(4),
            record_type: RecordType::ClientCommand,
            payload: encode_client_request(debit),
        }
        .encode();
        fs::OpenOptions::new()
            .append(true)
            .open(&wal_path)
            .unwrap()
            .write_all(&torn[..torn.len() - 3])
            .unwrap();

        let recovered = recover_quota(config(), &snapshot_file, &mut wal).unwrap();

        assert!(!recovered.loaded_snapshot);
        assert_eq!(recovered.recovered_wal.scan_result.frames.len(), 1);
        assert_eq!(recovered.replayed_wal_frame_count, 1);
        assert_eq!(recovered.replayed_wal_last_lsn, Some(Lsn(1)));
        let bucket = recovered.db.snapshot().buckets[0];
        assert_eq!(bucket.balance, 2);
        assert_eq!(bucket.last_refill_slot, Slot(1));

        fs::remove_file(wal_path).unwrap();
    }
}

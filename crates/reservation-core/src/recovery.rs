use std::convert::Infallible;

use crate::command::CommandContext;
use crate::command_codec::{CommandCodecError, decode_client_request, decode_internal_command};
use crate::config::{Config, ConfigError};
use crate::ids::{Lsn, Slot};
use crate::snapshot::SnapshotError;
use crate::snapshot_file::{SnapshotFile, SnapshotFileError};
use crate::state_machine::ReservationDb;
use crate::wal::RecordType;
use crate::wal_file::{RecoveredWal, WalFile, WalFileError};

#[derive(Debug)]
pub struct RecoveryResult {
    pub db: ReservationDb,
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

pub fn recover_reservation(
    config: Config,
    snapshot_file: &SnapshotFile,
    wal_file: &mut WalFile,
) -> Result<RecoveryResult, RecoveryError> {
    recover_reservation_with_observer(
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

pub fn recover_reservation_with_observer<E, F>(
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
        Some(snapshot) => ReservationDb::from_snapshot(config, snapshot)?,
        None => ReservationDb::new(config)?,
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
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::{
        command::{ClientRequest, Command, CommandContext},
        command_codec::encode_client_request,
        config::Config,
        ids::{ClientId, HoldId, Lsn, OperationId, PoolId, Slot},
        snapshot_file::SnapshotFile,
        state_machine::{HoldState, PoolRecord, ReservationDb},
        wal::{Frame, RecordType},
        wal_file::WalFile,
    };

    use super::{RecoveryError, ReplayError, recover_reservation};

    fn temp_path(name: &str, extension: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("reservation-core-{name}-{nanos}.{extension}"))
    }

    fn config() -> Config {
        Config {
            max_pools: 8,
            max_holds: 8,
            max_operations: 8,
            max_batch_len: 8,
            max_client_retry_window_slots: 8,
            max_wal_payload_bytes: 1024,
            max_snapshot_bytes: 4096,
        }
    }

    fn create_pool_request() -> ClientRequest {
        ClientRequest {
            operation_id: OperationId(1),
            client_id: ClientId(1),
            command: Command::CreatePool {
                pool_id: PoolId(11),
                total_capacity: 5,
            },
        }
    }

    fn place_hold_request(deadline_slot: Slot) -> ClientRequest {
        ClientRequest {
            operation_id: OperationId(2),
            client_id: ClientId(1),
            command: Command::PlaceHold {
                pool_id: PoolId(11),
                hold_id: HoldId(21),
                quantity: 5,
                deadline_slot,
            },
        }
    }

    fn extend_hold_request(deadline_slot: Slot) -> ClientRequest {
        ClientRequest {
            operation_id: OperationId(3),
            client_id: ClientId(1),
            command: Command::ExtendHold {
                hold_id: HoldId(21),
                deadline_slot,
            },
        }
    }

    #[test]
    fn recovery_replays_from_empty_snapshot() {
        let snapshot_path = temp_path("snapshot-empty", "snapshot");
        let wal_path = temp_path("wal-empty", "wal");
        let snapshot_file = SnapshotFile::new(&snapshot_path, 4096);
        let mut wal_file = WalFile::open(&wal_path, 1024).unwrap();
        let request = ClientRequest {
            operation_id: OperationId(1),
            client_id: ClientId(2),
            command: Command::CreatePool {
                pool_id: PoolId(3),
                total_capacity: 5,
            },
        };
        wal_file
            .append_frame(&Frame {
                lsn: Lsn(1),
                request_slot: Slot(2),
                record_type: RecordType::ClientCommand,
                payload: encode_client_request(request),
            })
            .unwrap();
        wal_file.sync().unwrap();

        let recovered = recover_reservation(config(), &snapshot_file, &mut wal_file).unwrap();
        assert_eq!(recovered.replayed_wal_frame_count, 1);
        assert_eq!(recovered.db.last_applied_lsn(), Some(Lsn(1)));
        assert_eq!(recovered.db.last_request_slot(), Some(Slot(2)));

        let _ = fs::remove_file(snapshot_path);
        let _ = fs::remove_file(wal_path);
    }

    #[test]
    fn recovery_rejects_rewound_request_slot_against_snapshot_progress() {
        let snapshot_path = temp_path("snapshot-rewound", "snapshot");
        let wal_path = temp_path("wal-rewound", "wal");
        let snapshot_file = SnapshotFile::new(&snapshot_path, 4096);
        let mut wal_file = WalFile::open(&wal_path, 1024).unwrap();

        let mut db = ReservationDb::new(config()).unwrap();
        db.restore_pool(PoolRecord {
            pool_id: PoolId(7),
            total_capacity: 10,
            held_capacity: 0,
            consumed_capacity: 0,
        })
        .unwrap();
        db.set_progress(Some(Lsn(5)), Some(Slot(8)));
        snapshot_file.write_snapshot(&db.snapshot()).unwrap();

        wal_file
            .append_frame(&Frame {
                lsn: Lsn(6),
                request_slot: Slot(7),
                record_type: RecordType::SnapshotMarker,
                payload: Vec::new(),
            })
            .unwrap();
        wal_file.sync().unwrap();

        let error = recover_reservation(config(), &snapshot_file, &mut wal_file).unwrap_err();
        assert!(matches!(
            error,
            RecoveryError::Replay(ReplayError::RewoundRequestSlot { .. })
        ));

        let _ = fs::remove_file(snapshot_path);
        let _ = fs::remove_file(wal_path);
    }

    #[test]
    fn recovery_matches_live_path_when_next_request_expires_overdue_hold() {
        let snapshot_path = temp_path("snapshot-expiry-live-match", "snapshot");
        let wal_path = temp_path("wal-expiry-live-match", "wal");
        let snapshot_file = SnapshotFile::new(&snapshot_path, 4096);
        let mut wal_file = WalFile::open(&wal_path, 1024).unwrap();

        let prefix = [
            Frame {
                lsn: Lsn(1),
                request_slot: Slot(1),
                record_type: RecordType::ClientCommand,
                payload: encode_client_request(ClientRequest {
                    operation_id: OperationId(1),
                    client_id: ClientId(1),
                    command: Command::CreatePool {
                        pool_id: PoolId(11),
                        total_capacity: 5,
                    },
                }),
            },
            Frame {
                lsn: Lsn(2),
                request_slot: Slot(2),
                record_type: RecordType::ClientCommand,
                payload: encode_client_request(ClientRequest {
                    operation_id: OperationId(2),
                    client_id: ClientId(1),
                    command: Command::PlaceHold {
                        pool_id: PoolId(11),
                        hold_id: HoldId(21),
                        quantity: 5,
                        deadline_slot: Slot(5),
                    },
                }),
            },
        ];
        for frame in prefix {
            wal_file.append_frame(&frame).unwrap();
        }
        wal_file.sync().unwrap();

        let mut live = ReservationDb::new(config()).unwrap();
        let _ = live.apply_client(
            CommandContext {
                lsn: Lsn(1),
                request_slot: Slot(1),
            },
            ClientRequest {
                operation_id: OperationId(1),
                client_id: ClientId(1),
                command: Command::CreatePool {
                    pool_id: PoolId(11),
                    total_capacity: 5,
                },
            },
        );
        let _ = live.apply_client(
            CommandContext {
                lsn: Lsn(2),
                request_slot: Slot(2),
            },
            ClientRequest {
                operation_id: OperationId(2),
                client_id: ClientId(1),
                command: Command::PlaceHold {
                    pool_id: PoolId(11),
                    hold_id: HoldId(21),
                    quantity: 5,
                    deadline_slot: Slot(5),
                },
            },
        );

        let mut recovered = recover_reservation(config(), &snapshot_file, &mut wal_file).unwrap();
        let request = ClientRequest {
            operation_id: OperationId(3),
            client_id: ClientId(1),
            command: Command::CreatePool {
                pool_id: PoolId(12),
                total_capacity: 1,
            },
        };
        let context = CommandContext {
            lsn: Lsn(3),
            request_slot: Slot(20),
        };

        let live_outcome = live.apply_client(context, request);
        let recovered_outcome = recovered.db.apply_client(context, request);

        assert_eq!(recovered_outcome, live_outcome);
        assert_eq!(recovered.db.snapshot(), live.snapshot());
        assert_eq!(
            recovered
                .db
                .snapshot()
                .holds
                .iter()
                .find(|record| record.hold_id == HoldId(21))
                .unwrap()
                .state,
            HoldState::Expired
        );

        let _ = fs::remove_file(snapshot_path);
        let _ = fs::remove_file(wal_path);
    }

    #[test]
    fn recovery_preserves_extended_deadline_before_later_request() {
        let snapshot_path = temp_path("snapshot-extend-live-match", "snapshot");
        let wal_path = temp_path("wal-extend-live-match", "wal");
        let snapshot_file = SnapshotFile::new(&snapshot_path, 4096);
        let mut wal_file = WalFile::open(&wal_path, 1024).unwrap();

        let prefix = [
            Frame {
                lsn: Lsn(1),
                request_slot: Slot(1),
                record_type: RecordType::ClientCommand,
                payload: encode_client_request(create_pool_request()),
            },
            Frame {
                lsn: Lsn(2),
                request_slot: Slot(2),
                record_type: RecordType::ClientCommand,
                payload: encode_client_request(place_hold_request(Slot(5))),
            },
            Frame {
                lsn: Lsn(3),
                request_slot: Slot(3),
                record_type: RecordType::ClientCommand,
                payload: encode_client_request(extend_hold_request(Slot(10))),
            },
        ];
        for frame in prefix {
            wal_file.append_frame(&frame).unwrap();
        }
        wal_file.sync().unwrap();

        let mut live = ReservationDb::new(config()).unwrap();
        let _ = live.apply_client(
            CommandContext {
                lsn: Lsn(1),
                request_slot: Slot(1),
            },
            create_pool_request(),
        );
        let _ = live.apply_client(
            CommandContext {
                lsn: Lsn(2),
                request_slot: Slot(2),
            },
            place_hold_request(Slot(5)),
        );
        let _ = live.apply_client(
            CommandContext {
                lsn: Lsn(3),
                request_slot: Slot(3),
            },
            extend_hold_request(Slot(10)),
        );

        let mut recovered = recover_reservation(config(), &snapshot_file, &mut wal_file).unwrap();
        let request = ClientRequest {
            operation_id: OperationId(4),
            client_id: ClientId(1),
            command: Command::CreatePool {
                pool_id: PoolId(12),
                total_capacity: 1,
            },
        };
        let context = CommandContext {
            lsn: Lsn(4),
            request_slot: Slot(8),
        };

        let live_outcome = live.apply_client(context, request);
        let recovered_outcome = recovered.db.apply_client(context, request);

        assert_eq!(recovered_outcome, live_outcome);
        assert_eq!(recovered.db.snapshot(), live.snapshot());
        assert_eq!(
            recovered
                .db
                .snapshot()
                .holds
                .iter()
                .find(|record| record.hold_id == HoldId(21))
                .unwrap()
                .state,
            HoldState::Held
        );
        assert_eq!(
            recovered
                .db
                .snapshot()
                .holds
                .iter()
                .find(|record| record.hold_id == HoldId(21))
                .unwrap()
                .deadline_slot,
            Slot(10)
        );

        let _ = fs::remove_file(snapshot_path);
        let _ = fs::remove_file(wal_path);
    }

    #[test]
    fn recovery_truncates_torn_expire_frame_without_fabricating_expired_state() {
        let snapshot_path = temp_path("snapshot-torn-expire", "snapshot");
        let wal_path = temp_path("wal-torn-expire", "wal");
        let snapshot_file = SnapshotFile::new(&snapshot_path, 4096);
        let mut wal_file = WalFile::open(&wal_path, 1024).unwrap();

        let create_pool = ClientRequest {
            operation_id: OperationId(1),
            client_id: ClientId(1),
            command: Command::CreatePool {
                pool_id: PoolId(11),
                total_capacity: 5,
            },
        };
        let place_hold = ClientRequest {
            operation_id: OperationId(2),
            client_id: ClientId(1),
            command: Command::PlaceHold {
                pool_id: PoolId(11),
                hold_id: HoldId(21),
                quantity: 5,
                deadline_slot: Slot(5),
            },
        };
        wal_file
            .append_frame(&Frame {
                lsn: Lsn(1),
                request_slot: Slot(1),
                record_type: RecordType::ClientCommand,
                payload: encode_client_request(create_pool),
            })
            .unwrap();
        wal_file
            .append_frame(&Frame {
                lsn: Lsn(2),
                request_slot: Slot(2),
                record_type: RecordType::ClientCommand,
                payload: encode_client_request(place_hold),
            })
            .unwrap();
        wal_file.sync().unwrap();

        let torn_expire = Frame {
            lsn: Lsn(3),
            request_slot: Slot(6),
            record_type: RecordType::InternalCommand,
            payload: vec![3, 21],
        }
        .encode();
        let mut raw = OpenOptions::new().append(true).open(&wal_path).unwrap();
        raw.write_all(&torn_expire[..torn_expire.len() - 2])
            .unwrap();
        raw.sync_all().unwrap();

        let mut recovered = recover_reservation(config(), &snapshot_file, &mut wal_file).unwrap();
        assert_eq!(
            recovered
                .db
                .snapshot()
                .holds
                .iter()
                .find(|record| record.hold_id == HoldId(21))
                .unwrap()
                .state,
            HoldState::Held
        );

        let outcome = recovered.db.apply_client(
            CommandContext {
                lsn: Lsn(3),
                request_slot: Slot(6),
            },
            ClientRequest {
                operation_id: OperationId(3),
                client_id: ClientId(1),
                command: Command::CreatePool {
                    pool_id: PoolId(12),
                    total_capacity: 1,
                },
            },
        );

        assert_eq!(outcome.result_code, crate::result::ResultCode::Ok);
        assert_eq!(
            recovered
                .db
                .snapshot()
                .holds
                .iter()
                .find(|record| record.hold_id == HoldId(21))
                .unwrap()
                .state,
            HoldState::Expired
        );

        let _ = fs::remove_file(snapshot_path);
        let _ = fs::remove_file(wal_path);
    }
}

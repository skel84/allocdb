use crate::command::CommandContext;
use crate::command_codec::{CommandCodecError, decode_client_request, decode_internal_command};
use crate::config::{Config, ConfigError};
use crate::ids::Lsn;
use crate::snapshot_file::{SnapshotFile, SnapshotFileError};
use crate::state_machine::AllocDb;
use crate::wal::RecordType;
use crate::wal_file::{RecoveredWal, WalFile, WalFileError};

#[derive(Debug)]
pub struct RecoveryResult {
    pub db: AllocDb,
    pub recovered_wal: RecoveredWal,
    pub loaded_snapshot_lsn: Option<Lsn>,
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
    let loaded_snapshot_lsn = snapshot.as_ref().and_then(|value| value.last_applied_lsn);
    let mut db = match snapshot {
        Some(snapshot) => AllocDb::from_snapshot(config, snapshot)
            .map_err(|error| RecoveryError::SnapshotFile(SnapshotFileError::Decode(error)))?,
        None => AllocDb::new(config)?,
    };

    let recovered_wal = wal_file.truncate_to_valid_prefix()?;
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
            }
            RecordType::InternalCommand => {
                let command = decode_internal_command(&frame.payload)?;
                db.apply_internal(context, command);
            }
            RecordType::SnapshotMarker => {}
        }
    }

    Ok(RecoveryResult {
        db,
        recovered_wal,
        loaded_snapshot_lsn,
    })
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Write;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::command::{ClientRequest, Command, CommandContext};
    use crate::command_codec::{encode_client_request, encode_internal_command};
    use crate::config::Config;
    use crate::ids::{ClientId, HolderId, Lsn, OperationId, ReservationId, ResourceId, Slot};
    use crate::snapshot_file::SnapshotFile;
    use crate::state_machine::{AllocDb, ResourceState};
    use crate::wal::{Frame, RecordType};
    use crate::wal_file::WalFile;

    use super::recover_allocdb;

    fn test_path(name: &str, extension: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("allocdb-{name}-{nanos}.{extension}"))
    }

    fn config() -> Config {
        Config {
            shard_id: 0,
            max_resources: 8,
            max_reservations: 8,
            max_operations: 16,
            max_ttl_slots: 16,
            max_client_retry_window_slots: 8,
            reservation_history_window_slots: 4,
            max_expiration_bucket_len: 8,
        }
    }

    fn context(lsn: u64, request_slot: u64) -> CommandContext {
        CommandContext {
            lsn: Lsn(lsn),
            request_slot: Slot(request_slot),
        }
    }

    fn client_frame(lsn: u64, request_slot: u64, request: ClientRequest) -> Frame {
        Frame {
            lsn: Lsn(lsn),
            request_slot: Slot(request_slot),
            record_type: RecordType::ClientCommand,
            payload: encode_client_request(request),
        }
    }

    fn internal_frame(lsn: u64, request_slot: u64, command: Command) -> Frame {
        Frame {
            lsn: Lsn(lsn),
            request_slot: Slot(request_slot),
            record_type: RecordType::InternalCommand,
            payload: encode_internal_command(command),
        }
    }

    #[test]
    fn recover_allocdb_replays_wal_without_snapshot() {
        let wal_path = test_path("recover-no-snapshot", "wal");
        let snapshot_path = test_path("recover-no-snapshot", "snapshot");

        let mut live = AllocDb::new(config()).unwrap();
        let mut wal = WalFile::open(&wal_path, 512).unwrap();
        let create = ClientRequest {
            operation_id: OperationId(1),
            client_id: ClientId(7),
            command: Command::CreateResource {
                resource_id: ResourceId(11),
            },
        };
        let reserve = ClientRequest {
            operation_id: OperationId(2),
            client_id: ClientId(7),
            command: Command::Reserve {
                resource_id: ResourceId(11),
                holder_id: HolderId(5),
                ttl_slots: 3,
            },
        };

        live.apply_client(context(1, 1), create);
        live.apply_client(context(2, 2), reserve);
        wal.append_frame(&client_frame(1, 1, create)).unwrap();
        wal.append_frame(&client_frame(2, 2, reserve)).unwrap();
        wal.sync().unwrap();

        let snapshot_file = SnapshotFile::new(&snapshot_path);
        let recovered = recover_allocdb(config(), &snapshot_file, &wal).unwrap();

        assert_eq!(recovered.db.snapshot(), live.snapshot());

        fs::remove_file(wal_path).unwrap();
    }

    #[test]
    fn recover_allocdb_skips_frames_covered_by_snapshot() {
        let wal_path = test_path("recover-with-snapshot", "wal");
        let snapshot_path = test_path("recover-with-snapshot", "snapshot");
        let mut wal = WalFile::open(&wal_path, 512).unwrap();
        let snapshot_file = SnapshotFile::new(&snapshot_path);

        let create = ClientRequest {
            operation_id: OperationId(1),
            client_id: ClientId(7),
            command: Command::CreateResource {
                resource_id: ResourceId(11),
            },
        };
        let reserve = ClientRequest {
            operation_id: OperationId(2),
            client_id: ClientId(7),
            command: Command::Reserve {
                resource_id: ResourceId(11),
                holder_id: HolderId(5),
                ttl_slots: 3,
            },
        };
        let confirm = ClientRequest {
            operation_id: OperationId(3),
            client_id: ClientId(7),
            command: Command::Confirm {
                reservation_id: ReservationId(2),
                holder_id: HolderId(5),
            },
        };

        let mut live = AllocDb::new(config()).unwrap();
        live.apply_client(context(1, 1), create);
        snapshot_file.write_snapshot(&live.snapshot()).unwrap();
        live.apply_client(context(2, 2), reserve);
        live.apply_client(context(3, 2), confirm);

        wal.append_frame(&client_frame(1, 1, create)).unwrap();
        wal.append_frame(&client_frame(2, 2, reserve)).unwrap();
        wal.append_frame(&client_frame(3, 2, confirm)).unwrap();
        wal.sync().unwrap();

        let recovered = recover_allocdb(config(), &snapshot_file, &wal).unwrap();

        assert_eq!(recovered.loaded_snapshot_lsn, Some(Lsn(1)));
        assert_eq!(recovered.db.snapshot(), live.snapshot());

        fs::remove_file(wal_path).unwrap();
        fs::remove_file(snapshot_path).unwrap();
    }

    #[test]
    fn recover_allocdb_truncates_torn_tail() {
        let wal_path = test_path("recover-torn-tail", "wal");
        let snapshot_path = test_path("recover-torn-tail", "snapshot");
        let mut wal = WalFile::open(&wal_path, 512).unwrap();

        let create = ClientRequest {
            operation_id: OperationId(1),
            client_id: ClientId(7),
            command: Command::CreateResource {
                resource_id: ResourceId(11),
            },
        };
        let reserve = ClientRequest {
            operation_id: OperationId(2),
            client_id: ClientId(7),
            command: Command::Reserve {
                resource_id: ResourceId(11),
                holder_id: HolderId(5),
                ttl_slots: 3,
            },
        };

        wal.append_frame(&client_frame(1, 1, create)).unwrap();
        let torn = client_frame(2, 2, reserve).encode();
        fs::OpenOptions::new()
            .append(true)
            .open(&wal_path)
            .unwrap()
            .write_all(&torn[..torn.len() - 2])
            .unwrap();

        let recovered =
            recover_allocdb(config(), &SnapshotFile::new(&snapshot_path), &wal).unwrap();

        assert_eq!(recovered.recovered_wal.scan_result.frames.len(), 1);
        assert_eq!(
            recovered.db.resource(ResourceId(11)).unwrap().current_state,
            ResourceState::Available
        );

        fs::remove_file(wal_path).unwrap();
    }

    #[test]
    fn recover_allocdb_replays_internal_commands() {
        let wal_path = test_path("recover-internal", "wal");
        let snapshot_path = test_path("recover-internal", "snapshot");
        let mut wal = WalFile::open(&wal_path, 512).unwrap();

        let create = ClientRequest {
            operation_id: OperationId(1),
            client_id: ClientId(7),
            command: Command::CreateResource {
                resource_id: ResourceId(11),
            },
        };
        let reserve = ClientRequest {
            operation_id: OperationId(2),
            client_id: ClientId(7),
            command: Command::Reserve {
                resource_id: ResourceId(11),
                holder_id: HolderId(5),
                ttl_slots: 3,
            },
        };
        let expire = Command::Expire {
            reservation_id: ReservationId(2),
            deadline_slot: Slot(5),
        };

        wal.append_frame(&client_frame(1, 1, create)).unwrap();
        wal.append_frame(&client_frame(2, 2, reserve)).unwrap();
        wal.append_frame(&internal_frame(3, 5, expire)).unwrap();
        wal.sync().unwrap();

        let recovered =
            recover_allocdb(config(), &SnapshotFile::new(&snapshot_path), &wal).unwrap();

        assert_eq!(
            recovered.db.resource(ResourceId(11)).unwrap().current_state,
            ResourceState::Available
        );

        fs::remove_file(wal_path).unwrap();
    }
}

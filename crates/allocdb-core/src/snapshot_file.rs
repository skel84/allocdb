use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::snapshot::{Snapshot, SnapshotError};

#[derive(Debug)]
pub struct SnapshotFile {
    path: PathBuf,
}

#[derive(Debug)]
pub enum SnapshotFileError {
    Io(std::io::Error),
    Decode(SnapshotError),
}

impl From<std::io::Error> for SnapshotFileError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<SnapshotError> for SnapshotFileError {
    fn from(error: SnapshotError) -> Self {
        Self::Decode(error)
    }
}

impl SnapshotFile {
    #[must_use]
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Writes one snapshot using temp-file, fsync, rename, and directory-sync discipline.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotFileError`] if serialization, write, sync, or rename fails.
    pub fn write_snapshot(&self, snapshot: &Snapshot) -> Result<(), SnapshotFileError> {
        let bytes = snapshot.encode();
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }

        let temp_path = self.temp_path();
        let mut file = File::create(&temp_path)?;
        file.write_all(&bytes)?;
        file.sync_data()?;
        drop(file);

        fs::rename(&temp_path, &self.path)?;
        sync_parent_dir(&self.path)?;
        Ok(())
    }

    /// Loads one snapshot file if present.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotFileError`] if the file cannot be read or if decoding fails.
    pub fn load_snapshot(&self) -> Result<Option<Snapshot>, SnapshotFileError> {
        let mut file = match File::open(&self.path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(SnapshotFileError::Io(error)),
        };

        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        Ok(Some(Snapshot::decode(&bytes)?))
    }

    fn temp_path(&self) -> PathBuf {
        let mut temp_path = self.path.clone();
        let extension = temp_path
            .extension()
            .and_then(|value| value.to_str())
            .map_or_else(|| "tmp".to_owned(), |value| format!("{value}.tmp"));
        temp_path.set_extension(extension);
        temp_path
    }
}

#[cfg(unix)]
fn sync_parent_dir(path: &Path) -> Result<(), std::io::Error> {
    if let Some(parent) = path.parent() {
        OpenOptions::new().read(true).open(parent)?.sync_all()?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn sync_parent_dir(_path: &Path) -> Result<(), std::io::Error> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::command::{ClientRequest, Command, CommandContext};
    use crate::config::Config;
    use crate::ids::{ClientId, HolderId, Lsn, OperationId, ResourceId, Slot};
    use crate::snapshot::Snapshot;
    use crate::state_machine::AllocDb;

    use super::{SnapshotFile, SnapshotFileError};

    fn test_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("allocdb-{name}-{nanos}.snapshot"))
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

    fn seeded_snapshot() -> Snapshot {
        let mut db = AllocDb::new(config()).unwrap();
        db.apply_client(
            context(1, 1),
            ClientRequest {
                operation_id: OperationId(1),
                client_id: ClientId(7),
                command: Command::CreateResource {
                    resource_id: ResourceId(11),
                },
            },
        );
        db.apply_client(
            context(2, 2),
            ClientRequest {
                operation_id: OperationId(2),
                client_id: ClientId(7),
                command: Command::Reserve {
                    resource_id: ResourceId(11),
                    holder_id: HolderId(5),
                    ttl_slots: 3,
                },
            },
        );
        db.snapshot()
    }

    #[test]
    fn snapshot_file_round_trips() {
        let path = test_path("roundtrip");
        let snapshot_file = SnapshotFile::new(&path);
        let expected = seeded_snapshot();

        snapshot_file.write_snapshot(&expected).unwrap();
        let loaded = snapshot_file.load_snapshot().unwrap().unwrap();

        assert_eq!(loaded, expected);

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn snapshot_file_returns_none_when_missing() {
        let path = test_path("missing");
        let snapshot_file = SnapshotFile::new(&path);

        assert!(snapshot_file.load_snapshot().unwrap().is_none());
    }

    #[test]
    fn snapshot_file_rejects_corruption() {
        let path = test_path("corrupt");
        let snapshot_file = SnapshotFile::new(&path);
        snapshot_file.write_snapshot(&seeded_snapshot()).unwrap();

        let mut bytes = fs::read(&path).unwrap();
        bytes[0] = 0;
        fs::write(&path, bytes).unwrap();

        let error = snapshot_file.load_snapshot().unwrap_err();
        assert!(matches!(error, SnapshotFileError::Decode(_)));

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn snapshot_file_replaces_existing_contents() {
        let path = test_path("replace");
        let snapshot_file = SnapshotFile::new(&path);

        let first = Snapshot {
            last_applied_lsn: None,
            last_request_slot: None,
            max_retired_reservation_id: None,
            resources: Vec::new(),
            reservations: Vec::new(),
            operations: Vec::new(),
            wheel: vec![Vec::new(); config().wheel_len()],
        };
        let second = seeded_snapshot();

        snapshot_file.write_snapshot(&first).unwrap();
        snapshot_file.write_snapshot(&second).unwrap();

        let loaded = snapshot_file.load_snapshot().unwrap().unwrap();
        assert_eq!(loaded, second);

        fs::remove_file(path).unwrap();
    }
}

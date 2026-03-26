use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::snapshot::{Snapshot, SnapshotError};

const FOOTER_MAGIC: u32 = 0x5154_4246;
const FOOTER_LEN: usize = 16;

#[derive(Debug)]
pub struct SnapshotFile {
    path: PathBuf,
    max_snapshot_bytes: usize,
}

#[derive(Debug)]
pub enum SnapshotFileError {
    Io(std::io::Error),
    Decode(SnapshotError),
    SnapshotTooLarge { actual_bytes: u64, max_bytes: usize },
    InvalidFooterMagic(u32),
    LengthMismatch { file_bytes: u64, payload_bytes: u64 },
    InvalidChecksum { stored: u32, computed: u32 },
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
    pub fn new(path: impl AsRef<Path>, max_snapshot_bytes: usize) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            max_snapshot_bytes,
        }
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn write_snapshot(&self, snapshot: &Snapshot) -> Result<(), SnapshotFileError> {
        let bytes = snapshot.encode();
        if bytes.len() > self.max_snapshot_bytes {
            return Err(SnapshotFileError::SnapshotTooLarge {
                actual_bytes: u64::try_from(bytes.len()).expect("snapshot length must fit u64"),
                max_bytes: self.max_snapshot_bytes,
            });
        }
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }

        let temp_path = self.temp_path();
        let mut file = File::create(&temp_path)?;
        file.write_all(&bytes)?;
        let checksum = crc32c::crc32c(&bytes);
        file.write_all(&FOOTER_MAGIC.to_le_bytes())?;
        file.write_all(
            &u64::try_from(bytes.len())
                .expect("snapshot length must fit u64")
                .to_le_bytes(),
        )?;
        file.write_all(&checksum.to_le_bytes())?;
        file.sync_data()?;
        drop(file);

        fs::rename(&temp_path, &self.path)?;
        sync_parent_dir(&self.path)?;
        Ok(())
    }

    pub fn load_snapshot(&self) -> Result<Option<Snapshot>, SnapshotFileError> {
        let mut file = match File::open(&self.path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(SnapshotFileError::Io(error)),
        };

        let file_bytes = file.metadata()?.len();
        let max_file_bytes = u64::try_from(self.max_snapshot_bytes + FOOTER_LEN)
            .expect("max snapshot file length must fit u64");
        if file_bytes > max_file_bytes {
            return Err(SnapshotFileError::SnapshotTooLarge {
                actual_bytes: file_bytes,
                max_bytes: self.max_snapshot_bytes,
            });
        }
        if file_bytes < u64::try_from(FOOTER_LEN).expect("footer length must fit u64") {
            return Err(SnapshotFileError::Decode(SnapshotError::BufferTooShort));
        }

        file.seek(SeekFrom::End(
            -i64::try_from(FOOTER_LEN).expect("footer length must fit i64"),
        ))?;
        let mut footer = [0_u8; FOOTER_LEN];
        file.read_exact(&mut footer)?;

        let footer_magic =
            u32::from_le_bytes(footer[0..4].try_into().expect("slice has exact size"));
        if footer_magic != FOOTER_MAGIC {
            return Err(SnapshotFileError::InvalidFooterMagic(footer_magic));
        }

        let payload_bytes =
            u64::from_le_bytes(footer[4..12].try_into().expect("slice has exact size"));
        if payload_bytes
            > u64::try_from(self.max_snapshot_bytes).expect("max snapshot bytes must fit u64")
        {
            return Err(SnapshotFileError::SnapshotTooLarge {
                actual_bytes: payload_bytes,
                max_bytes: self.max_snapshot_bytes,
            });
        }

        let expected_file_bytes = payload_bytes
            .checked_add(u64::try_from(FOOTER_LEN).expect("footer length must fit u64"))
            .expect("snapshot file length must not overflow");
        if expected_file_bytes != file_bytes {
            return Err(SnapshotFileError::LengthMismatch {
                file_bytes,
                payload_bytes,
            });
        }

        file.seek(SeekFrom::Start(0))?;
        let mut bytes = vec![
            0_u8;
            usize::try_from(payload_bytes)
                .expect("validated payload length must fit usize")
        ];
        file.read_exact(&mut bytes)?;

        let stored_checksum =
            u32::from_le_bytes(footer[12..16].try_into().expect("slice has exact size"));
        let computed_checksum = crc32c::crc32c(&bytes);
        if stored_checksum != computed_checksum {
            return Err(SnapshotFileError::InvalidChecksum {
                stored: stored_checksum,
                computed: computed_checksum,
            });
        }

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
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::{
        command::Command,
        ids::{HoldId, Lsn, OperationId, PoolId, Slot},
        result::ResultCode,
        snapshot::Snapshot,
        state_machine::{HoldRecord, HoldState, OperationRecord, PoolRecord},
    };

    use super::{SnapshotFile, SnapshotFileError};

    fn test_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("reservation-core-{name}-{nanos}.snapshot"))
    }

    fn seeded_snapshot() -> Snapshot {
        Snapshot {
            last_applied_lsn: Some(Lsn(2)),
            last_request_slot: Some(Slot(2)),
            pools: vec![PoolRecord {
                pool_id: PoolId(11),
                total_capacity: 8,
                held_capacity: 2,
                consumed_capacity: 1,
            }],
            holds: vec![HoldRecord {
                hold_id: HoldId(21),
                pool_id: PoolId(11),
                quantity: 2,
                deadline_slot: Slot(5),
                state: HoldState::Held,
            }],
            operations: vec![OperationRecord {
                operation_id: OperationId(1),
                command: Command::PlaceHold {
                    pool_id: PoolId(11),
                    hold_id: HoldId(21),
                    quantity: 2,
                    deadline_slot: Slot(5),
                },
                result_code: ResultCode::Ok,
                result_pool_id: Some(PoolId(11)),
                result_hold_id: Some(HoldId(21)),
                applied_lsn: Lsn(2),
                retire_after_slot: Slot(9),
            }],
        }
    }

    fn snapshot_file(path: &Path) -> SnapshotFile {
        SnapshotFile::new(path, 4096)
    }

    #[test]
    fn snapshot_file_round_trips() {
        let path = test_path("roundtrip");
        let snapshot_file = snapshot_file(&path);
        let expected = seeded_snapshot();

        snapshot_file.write_snapshot(&expected).unwrap();
        let loaded = snapshot_file.load_snapshot().unwrap().unwrap();

        assert_eq!(loaded, expected);

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn snapshot_file_returns_none_when_missing() {
        let path = test_path("missing");
        let snapshot_file = snapshot_file(&path);

        assert!(snapshot_file.load_snapshot().unwrap().is_none());
    }

    #[test]
    fn snapshot_file_rejects_corruption() {
        let path = test_path("corrupt");
        let snapshot_file = snapshot_file(&path);
        snapshot_file.write_snapshot(&seeded_snapshot()).unwrap();

        let mut bytes = fs::read(&path).unwrap();
        bytes[0] = 0;
        fs::write(&path, bytes).unwrap();

        let error = snapshot_file.load_snapshot().unwrap_err();
        assert!(matches!(error, SnapshotFileError::InvalidChecksum { .. }));

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn snapshot_file_rejects_oversized_payloads() {
        let path = test_path("oversized");
        let snapshot_file = SnapshotFile::new(&path, 32);

        let error = snapshot_file
            .write_snapshot(&seeded_snapshot())
            .unwrap_err();
        assert!(matches!(error, SnapshotFileError::SnapshotTooLarge { .. }));
    }
}

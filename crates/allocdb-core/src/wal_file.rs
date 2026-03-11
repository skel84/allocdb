use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::wal::{Frame, ScanResult, scan_frames};

#[derive(Debug)]
pub struct WalFile {
    path: PathBuf,
    file: File,
    max_payload_bytes: usize,
}

#[derive(Debug)]
pub struct RecoveredWal {
    pub scan_result: ScanResult,
    pub file_len: u64,
}

#[derive(Debug)]
pub enum WalFileError {
    Io(std::io::Error),
    PayloadTooLarge {
        payload_len: usize,
        max_payload_bytes: usize,
    },
}

impl From<std::io::Error> for WalFileError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl WalFile {
    /// Opens or creates one WAL segment file.
    ///
    /// # Errors
    ///
    /// Returns [`WalFileError`] if the file cannot be opened or created.
    pub fn open(path: impl AsRef<Path>, max_payload_bytes: usize) -> Result<Self, WalFileError> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;

        Ok(Self {
            path,
            file,
            max_payload_bytes,
        })
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Appends one encoded frame to the WAL file.
    ///
    /// # Errors
    ///
    /// Returns [`WalFileError::PayloadTooLarge`] if the frame payload exceeds the configured bound,
    /// or [`WalFileError::Io`] if the append fails.
    pub fn append_frame(&mut self, frame: &Frame) -> Result<(), WalFileError> {
        if frame.payload.len() > self.max_payload_bytes {
            return Err(WalFileError::PayloadTooLarge {
                payload_len: frame.payload.len(),
                max_payload_bytes: self.max_payload_bytes,
            });
        }

        let encoded = frame.encode();
        self.file.write_all(&encoded)?;
        Ok(())
    }

    /// Forces appended WAL bytes to durable storage.
    ///
    /// # Errors
    ///
    /// Returns [`WalFileError::Io`] if the sync fails.
    pub fn sync(&self) -> Result<(), WalFileError> {
        self.file.sync_data()?;
        Ok(())
    }

    /// Reads and scans the file, stopping at the last valid frame boundary.
    ///
    /// # Errors
    ///
    /// Returns [`WalFileError::Io`] if the file cannot be read.
    pub fn recover(&self) -> Result<RecoveredWal, WalFileError> {
        recover_path(&self.path)
    }

    /// Truncates the file to the last valid frame boundary discovered by recovery scanning.
    ///
    /// # Errors
    ///
    /// Returns [`WalFileError::Io`] if the file cannot be reopened, truncated, or synced.
    ///
    /// # Panics
    ///
    /// Panics only if the discovered valid prefix cannot fit into `u64`.
    pub fn truncate_to_valid_prefix(&self) -> Result<RecoveredWal, WalFileError> {
        let recovered = recover_path(&self.path)?;
        if recovered.file_len
            > u64::try_from(recovered.scan_result.valid_up_to)
                .expect("valid WAL prefix must fit into u64")
        {
            let mut file = OpenOptions::new().write(true).open(&self.path)?;
            file.set_len(
                u64::try_from(recovered.scan_result.valid_up_to)
                    .expect("valid WAL prefix must fit into u64"),
            )?;
            file.seek(SeekFrom::Start(
                u64::try_from(recovered.scan_result.valid_up_to)
                    .expect("valid WAL prefix must fit into u64"),
            ))?;
            file.sync_data()?;
        }

        Ok(recovered)
    }
}

fn recover_path(path: &Path) -> Result<RecoveredWal, WalFileError> {
    let mut file = File::open(path)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    let scan_result = scan_frames(&bytes);
    Ok(RecoveredWal {
        scan_result,
        file_len: u64::try_from(bytes.len()).expect("file length must fit into u64"),
    })
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::ids::{Lsn, Slot};
    use crate::wal::{Frame, RecordType};

    use super::{WalFile, WalFileError};

    fn test_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("allocdb-{name}-{nanos}.wal"))
    }

    fn frame(lsn: u64, slot: u64, payload: &[u8]) -> Frame {
        Frame {
            lsn: Lsn(lsn),
            request_slot: Slot(slot),
            record_type: RecordType::ClientCommand,
            payload: payload.to_vec(),
        }
    }

    #[test]
    fn wal_file_round_trips_frames() {
        let path = test_path("roundtrip");
        let mut wal = WalFile::open(&path, 64).unwrap();
        wal.append_frame(&frame(1, 1, b"one")).unwrap();
        wal.append_frame(&frame(2, 2, b"two")).unwrap();
        wal.sync().unwrap();

        let recovered = wal.recover().unwrap();
        assert_eq!(recovered.scan_result.frames.len(), 2);
        assert_eq!(recovered.scan_result.frames[0].lsn, Lsn(1));
        assert_eq!(recovered.scan_result.frames[1].lsn, Lsn(2));

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn wal_file_rejects_large_payloads() {
        let path = test_path("payload-limit");
        let mut wal = WalFile::open(&path, 2).unwrap();

        let error = wal.append_frame(&frame(1, 1, b"abc")).unwrap_err();
        assert!(matches!(
            error,
            WalFileError::PayloadTooLarge {
                payload_len: 3,
                max_payload_bytes: 2
            }
        ));

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn wal_file_recovery_ignores_torn_tail() {
        let path = test_path("torn-tail");
        let mut wal = WalFile::open(&path, 64).unwrap();
        wal.append_frame(&frame(1, 1, b"one")).unwrap();
        wal.append_frame(&frame(2, 2, b"two")).unwrap();
        wal.sync().unwrap();

        let metadata = fs::metadata(&path).unwrap();
        fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(metadata.len() - 2)
            .unwrap();

        let recovered = wal.recover().unwrap();
        assert_eq!(recovered.scan_result.frames.len(), 1);
        assert_eq!(recovered.scan_result.frames[0].lsn, Lsn(1));

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn wal_file_truncates_to_valid_prefix() {
        let path = test_path("truncate");
        let mut wal = WalFile::open(&path, 64).unwrap();
        wal.append_frame(&frame(1, 1, b"one")).unwrap();
        wal.append_frame(&frame(2, 2, b"two")).unwrap();
        wal.sync().unwrap();

        let metadata = fs::metadata(&path).unwrap();
        fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(metadata.len() - 3)
            .unwrap();

        let recovered = wal.truncate_to_valid_prefix().unwrap();
        let new_len = fs::metadata(&path).unwrap().len();

        assert_eq!(recovered.scan_result.frames.len(), 1);
        assert_eq!(
            new_len,
            u64::try_from(recovered.scan_result.valid_up_to).unwrap()
        );

        fs::remove_file(path).unwrap();
    }
}

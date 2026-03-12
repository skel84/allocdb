use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::wal::{DecodeError, Frame, ScanResult, ScanStopReason, scan_frames};

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
    Corruption {
        offset: usize,
        error: DecodeError,
    },
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
        let file = open_append_file(&path)?;

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
        self.validate_payload_len(frame)?;

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
    /// Returns [`WalFileError::Corruption`] if recovery finds a middle-of-log corruption, or
    /// [`WalFileError::Io`] if the file cannot be reopened, truncated, or synced.
    ///
    /// # Panics
    ///
    /// Panics only if the discovered valid prefix cannot fit into `u64`.
    pub fn truncate_to_valid_prefix(&self) -> Result<RecoveredWal, WalFileError> {
        let recovered = recover_path(&self.path)?;
        let valid_prefix =
            u64::try_from(recovered.scan_result.valid_up_to).expect("valid WAL prefix must fit");

        match recovered.scan_result.stop_reason {
            ScanStopReason::CleanEof => {}
            ScanStopReason::TornTail { .. } => {
                if recovered.file_len > valid_prefix {
                    let mut file = OpenOptions::new().write(true).open(&self.path)?;
                    file.set_len(valid_prefix)?;
                    file.seek(SeekFrom::Start(valid_prefix))?;
                    file.sync_data()?;
                }
            }
            ScanStopReason::Corruption { offset, error } => {
                return Err(WalFileError::Corruption { offset, error });
            }
        }

        Ok(recovered)
    }

    /// Replaces the on-disk WAL contents with one new ordered frame set.
    ///
    /// # Errors
    ///
    /// Returns [`WalFileError::PayloadTooLarge`] if any frame exceeds the configured payload
    /// bound, or [`WalFileError::Io`] if the temp-file rewrite, rename, or reopen fails.
    pub fn replace_with_frames(&mut self, frames: &[Frame]) -> Result<(), WalFileError> {
        for frame in frames {
            self.validate_payload_len(frame)?;
        }

        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }

        let temp_path = self.temp_path();
        {
            let mut temp_file = File::create(&temp_path)?;
            for frame in frames {
                temp_file.write_all(&frame.encode())?;
            }
            temp_file.sync_data()?;
        }

        fs::rename(&temp_path, &self.path)?;
        sync_parent_dir(&self.path)?;
        self.file = open_append_file(&self.path)?;
        Ok(())
    }

    fn validate_payload_len(&self, frame: &Frame) -> Result<(), WalFileError> {
        if frame.payload.len() > self.max_payload_bytes {
            return Err(WalFileError::PayloadTooLarge {
                payload_len: frame.payload.len(),
                max_payload_bytes: self.max_payload_bytes,
            });
        }

        Ok(())
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

fn open_append_file(path: &Path) -> Result<File, std::io::Error> {
    OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(path)
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

    use crate::ids::{Lsn, Slot};
    use crate::wal::{DecodeError, Frame, RecordType, ScanStopReason};

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
    fn wal_file_recovery_reports_torn_tail() {
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
        assert_eq!(
            recovered.scan_result.stop_reason,
            ScanStopReason::TornTail {
                offset: recovered.scan_result.valid_up_to,
            }
        );

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
        assert_eq!(
            recovered.scan_result.stop_reason,
            ScanStopReason::TornTail {
                offset: recovered.scan_result.valid_up_to,
            }
        );

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn wal_file_truncate_rejects_mid_log_corruption() {
        let path = test_path("mid-log-corruption");
        let mut wal = WalFile::open(&path, 64).unwrap();
        wal.append_frame(&frame(1, 1, b"one")).unwrap();
        wal.append_frame(&frame(2, 2, b"two")).unwrap();
        wal.sync().unwrap();

        let mut bytes = fs::read(&path).unwrap();
        let first_len = frame(1, 1, b"one").encode().len();
        let last_index = bytes.len() - 1;
        assert!(last_index >= first_len);
        bytes[last_index] ^= 0xff;
        fs::write(&path, bytes).unwrap();

        let original_len = fs::metadata(&path).unwrap().len();
        let error = wal.truncate_to_valid_prefix().unwrap_err();

        assert!(matches!(
            error,
            WalFileError::Corruption {
                offset,
                error: DecodeError::InvalidChecksum,
            } if offset == first_len
        ));
        assert_eq!(fs::metadata(&path).unwrap().len(), original_len);

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn wal_file_replace_with_frames_rewrites_contents() {
        let path = test_path("rewrite");
        let mut wal = WalFile::open(&path, 64).unwrap();
        wal.append_frame(&frame(1, 1, b"one")).unwrap();
        wal.append_frame(&frame(2, 2, b"two")).unwrap();
        wal.sync().unwrap();

        let rewritten = vec![frame(2, 2, b"two"), frame(3, 3, b"three")];
        wal.replace_with_frames(&rewritten).unwrap();

        let recovered = wal.recover().unwrap();
        assert_eq!(recovered.scan_result.frames, rewritten);
        assert_eq!(recovered.scan_result.stop_reason, ScanStopReason::CleanEof);

        fs::remove_file(path).unwrap();
    }
}

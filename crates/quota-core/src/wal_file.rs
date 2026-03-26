use std::path::Path;

use crate::wal::{DecodeError, Frame, ScanResult, ScanStopReason, scan_frames};
use allocdb_wal_file::AppendWalFile;

#[derive(Debug)]
pub struct WalFile {
    file: AppendWalFile,
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
    pub fn open(path: impl AsRef<Path>, max_payload_bytes: usize) -> Result<Self, WalFileError> {
        let file = AppendWalFile::open(path)?;

        Ok(Self {
            file,
            max_payload_bytes,
        })
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        self.file.path()
    }

    pub fn append_frame(&mut self, frame: &Frame) -> Result<(), WalFileError> {
        self.validate_payload_len(frame)?;
        self.file.append_bytes(&frame.encode())?;
        Ok(())
    }

    pub fn sync(&self) -> Result<(), WalFileError> {
        self.file.sync()?;
        Ok(())
    }

    pub fn recover(&self) -> Result<RecoveredWal, WalFileError> {
        recover_file(&self.file)
    }

    pub fn truncate_to_valid_prefix(&mut self) -> Result<RecoveredWal, WalFileError> {
        let recovered = recover_file(&self.file)?;
        let valid_prefix =
            u64::try_from(recovered.scan_result.valid_up_to).expect("valid WAL prefix must fit");

        match recovered.scan_result.stop_reason {
            ScanStopReason::CleanEof => {}
            ScanStopReason::TornTail { .. } => {
                if recovered.file_len > valid_prefix {
                    self.file.truncate_to(valid_prefix)?;
                }
            }
            ScanStopReason::Corruption { offset, error } => {
                return Err(WalFileError::Corruption { offset, error });
            }
        }

        Ok(recovered)
    }

    pub fn replace_with_frames(&mut self, frames: &[Frame]) -> Result<(), WalFileError> {
        for frame in frames {
            self.validate_payload_len(frame)?;
        }

        let mut bytes = Vec::new();
        for frame in frames {
            bytes.extend_from_slice(&frame.encode());
        }
        self.file.replace_with_bytes(&bytes)?;
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
}

fn recover_file(file: &AppendWalFile) -> Result<RecoveredWal, WalFileError> {
    let bytes = file.read_all()?;
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
    use crate::wal::{Frame, RecordType, ScanStopReason};

    use super::{WalFile, WalFileError};

    fn test_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("quota-core-{name}-{nanos}.wal"))
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
        assert_eq!(recovered.scan_result.frames.len(), 1);
        assert_eq!(
            recovered.scan_result.stop_reason,
            ScanStopReason::TornTail {
                offset: recovered.scan_result.valid_up_to,
            }
        );

        fs::remove_file(path).unwrap();
    }
}

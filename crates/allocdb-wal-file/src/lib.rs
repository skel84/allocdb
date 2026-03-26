use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct AppendWalFile {
    path: PathBuf,
    file: File,
}

impl AppendWalFile {
    /// Opens or creates one appendable WAL file.
    ///
    /// # Errors
    ///
    /// Returns [`std::io::Error`] if the file cannot be opened or created.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        let path = path.as_ref().to_path_buf();
        let file = open_append_file(&path)?;
        Ok(Self { path, file })
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Appends already encoded frame bytes.
    ///
    /// # Errors
    ///
    /// Returns [`std::io::Error`] if the append fails.
    pub fn append_bytes(&mut self, bytes: &[u8]) -> Result<(), std::io::Error> {
        self.file.write_all(bytes)
    }

    /// Forces appended WAL bytes to durable storage.
    ///
    /// # Errors
    ///
    /// Returns [`std::io::Error`] if syncing fails.
    pub fn sync(&self) -> Result<(), std::io::Error> {
        self.file.sync_data()
    }

    /// Reads the full WAL contents.
    ///
    /// # Errors
    ///
    /// Returns [`std::io::Error`] if the file cannot be read.
    pub fn read_all(&self) -> Result<Vec<u8>, std::io::Error> {
        let mut file = File::open(&self.path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        Ok(bytes)
    }

    /// Truncates the WAL to a known-good valid prefix and refreshes the append handle.
    ///
    /// # Errors
    ///
    /// Returns [`std::io::Error`] if the file cannot be reopened, truncated, or synced.
    pub fn truncate_to(&mut self, valid_prefix: u64) -> Result<(), std::io::Error> {
        let mut file = OpenOptions::new().write(true).open(&self.path)?;
        file.set_len(valid_prefix)?;
        file.seek(SeekFrom::Start(valid_prefix))?;
        file.sync_data()?;
        drop(file);
        self.file = open_append_file(&self.path)?;
        Ok(())
    }

    /// Replaces the on-disk WAL bytes atomically and refreshes the append handle.
    ///
    /// # Errors
    ///
    /// Returns [`std::io::Error`] if the temp-file rewrite, rename, or reopen fails.
    pub fn replace_with_bytes(&mut self, bytes: &[u8]) -> Result<(), std::io::Error> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }

        let temp_path = temp_path(&self.path);
        {
            let mut temp_file = File::create(&temp_path)?;
            temp_file.write_all(bytes)?;
            temp_file.sync_data()?;
        }

        fs::rename(&temp_path, &self.path)?;
        sync_parent_dir(&self.path)?;
        self.file = open_append_file(&self.path)?;
        Ok(())
    }
}

fn open_append_file(path: &Path) -> Result<File, std::io::Error> {
    OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(path)
}

fn temp_path(path: &Path) -> PathBuf {
    let mut temp_path = path.to_path_buf();
    let extension = temp_path
        .extension()
        .and_then(|value| value.to_str())
        .map_or_else(|| "tmp".to_owned(), |value| format!("{value}.tmp"));
    temp_path.set_extension(extension);
    temp_path
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

    use super::AppendWalFile;

    fn test_path(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("allocdb-wal-file-{name}-{nanos}.wal"))
    }

    #[test]
    fn append_and_read_round_trip() {
        let path = test_path("append");
        let mut wal = AppendWalFile::open(&path).unwrap();
        wal.append_bytes(b"abc").unwrap();
        wal.append_bytes(b"def").unwrap();
        wal.sync().unwrap();

        assert_eq!(wal.read_all().unwrap(), b"abcdef");

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn truncate_reopens_for_future_appends() {
        let path = test_path("truncate");
        let mut wal = AppendWalFile::open(&path).unwrap();
        wal.append_bytes(b"abcdef").unwrap();
        wal.sync().unwrap();
        wal.truncate_to(3).unwrap();
        wal.append_bytes(b"XYZ").unwrap();
        wal.sync().unwrap();

        assert_eq!(wal.read_all().unwrap(), b"abcXYZ");

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn replace_rewrites_contents_and_reopens_append_handle() {
        let path = test_path("replace");
        let mut wal = AppendWalFile::open(&path).unwrap();
        wal.append_bytes(b"old").unwrap();
        wal.sync().unwrap();
        wal.replace_with_bytes(b"new").unwrap();
        wal.append_bytes(b"!").unwrap();
        wal.sync().unwrap();

        assert_eq!(wal.read_all().unwrap(), b"new!");

        fs::remove_file(path).unwrap();
    }
}

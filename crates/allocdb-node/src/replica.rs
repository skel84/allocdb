use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use allocdb_core::config::Config;
use allocdb_core::ids::{Lsn, Slot};
use log::{error, info, warn};

use crate::engine::{
    CheckpointError, CheckpointResult, EngineConfig, EngineOpenError, ReadError,
    RecoverEngineError, SingleNodeEngine, SubmissionError, SubmissionResult,
};

#[cfg(all(test, unix))]
#[path = "replica_tests.rs"]
mod tests;

#[cfg(all(test, not(unix)))]
#[path = "non_unix_tests.rs"]
mod non_unix_tests;

const MAX_REPLICA_METADATA_BYTES: u64 = 256;
const REPLICA_METADATA_MAGIC: [u8; 4] = *b"RPLM";
const REPLICA_METADATA_VERSION: u8 = 2;
const REPLICA_PREPARE_LOG_MAGIC: [u8; 4] = *b"RPLP";
const REPLICA_PREPARE_LOG_VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplicaId(pub u64);

impl ReplicaId {
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl From<u64> for ReplicaId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplicaIdentity {
    pub replica_id: ReplicaId,
    pub shard_id: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicaRole {
    Primary,
    Backup,
    ViewUncertain,
    Recovering,
    Faulted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurableVote {
    pub view: u64,
    pub voted_for: ReplicaId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplicaMetadata {
    pub identity: ReplicaIdentity,
    pub current_view: u64,
    pub role: ReplicaRole,
    pub commit_lsn: Option<Lsn>,
    pub active_snapshot_lsn: Option<Lsn>,
    pub last_normal_view: Option<u64>,
    pub durable_vote: Option<DurableVote>,
}

impl ReplicaMetadata {
    #[must_use]
    pub const fn bootstrap(
        identity: ReplicaIdentity,
        commit_lsn: Option<Lsn>,
        active_snapshot_lsn: Option<Lsn>,
    ) -> Self {
        Self {
            identity,
            current_view: 0,
            role: ReplicaRole::Recovering,
            commit_lsn,
            active_snapshot_lsn,
            last_normal_view: None,
            durable_vote: None,
        }
    }

    /// Validates durable replicated metadata against the expected replica identity and the local
    /// allocator state already recovered from snapshot plus WAL.
    ///
    /// # Errors
    ///
    /// Returns [`ReplicaStartupValidationError`] when the persisted identity, view metadata,
    /// commit/snapshot relationship, or local applied state contradict the replicated startup
    /// contract.
    pub fn validate(
        &self,
        expected_identity: ReplicaIdentity,
        local_last_applied_lsn: Option<Lsn>,
        local_active_snapshot_lsn: Option<Lsn>,
    ) -> Result<(), ReplicaStartupValidationError> {
        if self.identity.replica_id != expected_identity.replica_id {
            return Err(ReplicaStartupValidationError::ReplicaIdMismatch {
                expected: expected_identity.replica_id,
                found: self.identity.replica_id,
            });
        }

        if self.identity.shard_id != expected_identity.shard_id {
            return Err(ReplicaStartupValidationError::ShardIdMismatch {
                expected: expected_identity.shard_id,
                found: self.identity.shard_id,
            });
        }

        if let Some(active_snapshot_lsn) = self.active_snapshot_lsn {
            let Some(commit_lsn) = self.commit_lsn else {
                return Err(ReplicaStartupValidationError::SnapshotWithoutCommit {
                    active_snapshot_lsn,
                });
            };
            if active_snapshot_lsn.get() > commit_lsn.get() {
                return Err(ReplicaStartupValidationError::SnapshotBeyondCommit {
                    active_snapshot_lsn,
                    commit_lsn,
                });
            }
        }

        if let Some(last_normal_view) = self.last_normal_view {
            if last_normal_view > self.current_view {
                return Err(ReplicaStartupValidationError::LastNormalViewAhead {
                    current_view: self.current_view,
                    last_normal_view,
                });
            }
        }

        if let Some(vote) = self.durable_vote {
            if let Some(last_normal_view) = self.last_normal_view {
                if vote.view < last_normal_view {
                    return Err(
                        ReplicaStartupValidationError::DurableVoteBelowLastNormalView {
                            last_normal_view,
                            voted_view: vote.view,
                        },
                    );
                }
            }
            if vote.view < self.current_view {
                return Err(ReplicaStartupValidationError::DurableVoteBelowCurrentView {
                    current_view: self.current_view,
                    voted_view: vote.view,
                });
            }
        }

        if self.active_snapshot_lsn != local_active_snapshot_lsn {
            return Err(ReplicaStartupValidationError::ActiveSnapshotMismatch {
                metadata_snapshot_lsn: self.active_snapshot_lsn,
                local_snapshot_lsn: local_active_snapshot_lsn,
            });
        }

        match (self.commit_lsn, local_last_applied_lsn) {
            (None, None) => {}
            (None, Some(last_applied_lsn)) => {
                return Err(ReplicaStartupValidationError::AppliedLsnExceedsCommitLsn {
                    last_applied_lsn,
                    commit_lsn: None,
                });
            }
            (Some(commit_lsn), Some(last_applied_lsn)) if last_applied_lsn == commit_lsn => {}
            (Some(commit_lsn), Some(last_applied_lsn))
                if last_applied_lsn.get() > commit_lsn.get() =>
            {
                return Err(ReplicaStartupValidationError::AppliedLsnExceedsCommitLsn {
                    last_applied_lsn,
                    commit_lsn: Some(commit_lsn),
                });
            }
            (Some(commit_lsn), last_applied_lsn) => {
                return Err(ReplicaStartupValidationError::AppliedLsnBehindCommitLsn {
                    last_applied_lsn,
                    commit_lsn,
                });
            }
        }

        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaPaths {
    pub metadata_path: PathBuf,
    pub snapshot_path: PathBuf,
    pub wal_path: PathBuf,
}

impl ReplicaPaths {
    #[must_use]
    pub fn new(
        metadata_path: impl AsRef<Path>,
        snapshot_path: impl AsRef<Path>,
        wal_path: impl AsRef<Path>,
    ) -> Self {
        Self {
            metadata_path: metadata_path.as_ref().to_path_buf(),
            snapshot_path: snapshot_path.as_ref().to_path_buf(),
            wal_path: wal_path.as_ref().to_path_buf(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaPreparedEntry {
    pub view: u64,
    pub lsn: Lsn,
    pub request_slot: Slot,
    pub payload: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicaMetadataDecodeError {
    BufferTooShort,
    InvalidMagic,
    UnsupportedVersion(u8),
    InvalidRole(u8),
    InvalidOptionTag { kind: &'static str, value: u8 },
    InvalidLayout,
}

#[derive(Debug)]
pub enum ReplicaMetadataFileError {
    Io(std::io::Error),
    Decode(ReplicaMetadataDecodeError),
    TooLarge { file_len: u64, max_bytes: u64 },
}

impl From<std::io::Error> for ReplicaMetadataFileError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<ReplicaMetadataDecodeError> for ReplicaMetadataFileError {
    fn from(error: ReplicaMetadataDecodeError) -> Self {
        Self::Decode(error)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicaStartupValidationError {
    ReplicaIdMismatch {
        expected: ReplicaId,
        found: ReplicaId,
    },
    ShardIdMismatch {
        expected: u64,
        found: u64,
    },
    SnapshotWithoutCommit {
        active_snapshot_lsn: Lsn,
    },
    SnapshotBeyondCommit {
        active_snapshot_lsn: Lsn,
        commit_lsn: Lsn,
    },
    LastNormalViewAhead {
        current_view: u64,
        last_normal_view: u64,
    },
    DurableVoteBelowCurrentView {
        current_view: u64,
        voted_view: u64,
    },
    DurableVoteBelowLastNormalView {
        last_normal_view: u64,
        voted_view: u64,
    },
    AppliedLsnExceedsCommitLsn {
        last_applied_lsn: Lsn,
        commit_lsn: Option<Lsn>,
    },
    AppliedLsnBehindCommitLsn {
        last_applied_lsn: Option<Lsn>,
        commit_lsn: Lsn,
    },
    ActiveSnapshotMismatch {
        metadata_snapshot_lsn: Option<Lsn>,
        local_snapshot_lsn: Option<Lsn>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicaMetadataLoadError {
    Io(std::io::ErrorKind),
    Decode(ReplicaMetadataDecodeError),
    TooLarge { file_len: u64, max_bytes: u64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicaFaultReason {
    PersistedFaultState,
    MetadataLoad(ReplicaMetadataLoadError),
    Validation(ReplicaStartupValidationError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplicaFault {
    pub reason: ReplicaFaultReason,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicaNodeStatus {
    Active,
    Faulted(ReplicaFault),
}

#[derive(Debug)]
pub enum ReplicaOpenError {
    Engine(EngineOpenError),
    MetadataFile(ReplicaMetadataFileError),
}

impl From<EngineOpenError> for ReplicaOpenError {
    fn from(error: EngineOpenError) -> Self {
        Self::Engine(error)
    }
}

impl From<ReplicaMetadataFileError> for ReplicaOpenError {
    fn from(error: ReplicaMetadataFileError) -> Self {
        Self::MetadataFile(error)
    }
}

#[derive(Debug)]
pub enum RecoverReplicaError {
    Recovery(RecoverEngineError),
    MetadataFile(ReplicaMetadataFileError),
}

impl From<RecoverEngineError> for RecoverReplicaError {
    fn from(error: RecoverEngineError) -> Self {
        Self::Recovery(error)
    }
}

impl From<ReplicaMetadataFileError> for RecoverReplicaError {
    fn from(error: ReplicaMetadataFileError) -> Self {
        Self::MetadataFile(error)
    }
}

#[derive(Debug)]
pub enum ReplicaCheckpointError {
    Inactive(ReplicaNodeStatus),
    Checkpoint(CheckpointError),
    MetadataFile(ReplicaMetadataFileError),
}

impl From<CheckpointError> for ReplicaCheckpointError {
    fn from(error: CheckpointError) -> Self {
        Self::Checkpoint(error)
    }
}

impl From<ReplicaMetadataFileError> for ReplicaCheckpointError {
    fn from(error: ReplicaMetadataFileError) -> Self {
        Self::MetadataFile(error)
    }
}

#[derive(Debug)]
pub enum ReplicaProtocolError {
    Inactive(ReplicaNodeStatus),
    UnsupportedNormalRole(ReplicaRole),
    RoleMismatch {
        expected: ReplicaRole,
        found: ReplicaRole,
    },
    RoleSetMismatch {
        expected: &'static str,
        found: ReplicaRole,
    },
    ViewRegression {
        current_view: u64,
        requested_view: u64,
    },
    ViewMismatch {
        expected: u64,
        found: u64,
    },
    PrepareLsnExhausted {
        last_lsn: Lsn,
    },
    PrepareOrderMismatch {
        expected_lsn: Lsn,
        found_lsn: Lsn,
    },
    PreparedEntryConflict {
        lsn: Lsn,
    },
    MissingPreparedEntry {
        lsn: Lsn,
    },
    PrepareStorage(std::io::ErrorKind),
    MetadataFile(ReplicaMetadataFileError),
    Submission(SubmissionError),
    AppliedLsnMismatch {
        expected: Lsn,
        found: Lsn,
    },
    Read(NotPrimaryReadError),
}

impl From<ReplicaMetadataFileError> for ReplicaProtocolError {
    fn from(error: ReplicaMetadataFileError) -> Self {
        Self::MetadataFile(error)
    }
}

impl From<SubmissionError> for ReplicaProtocolError {
    fn from(error: SubmissionError) -> Self {
        Self::Submission(error)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NotPrimaryReadError {
    ReplicaCrashed,
    Role(ReplicaRole),
    Fence(ReadError),
}

#[derive(Debug)]
pub struct ReplicaMetadataFile {
    path: PathBuf,
}

impl ReplicaMetadataFile {
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

    /// Loads replicated metadata if the sidecar file exists.
    ///
    /// # Errors
    ///
    /// Returns [`ReplicaMetadataFileError`] if the file cannot be read, exceeds the bounded
    /// metadata size, or fails to decode.
    pub fn load_metadata(&self) -> Result<Option<ReplicaMetadata>, ReplicaMetadataFileError> {
        let file = match File::open(&self.path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(ReplicaMetadataFileError::Io(error)),
        };

        let file_len = file.metadata()?.len();
        if file_len > MAX_REPLICA_METADATA_BYTES {
            return Err(ReplicaMetadataFileError::TooLarge {
                file_len,
                max_bytes: MAX_REPLICA_METADATA_BYTES,
            });
        }

        let mut bytes = Vec::new();
        file.take(MAX_REPLICA_METADATA_BYTES + 1)
            .read_to_end(&mut bytes)?;
        let read_len = bytes.len() as u64;
        if read_len > MAX_REPLICA_METADATA_BYTES {
            return Err(ReplicaMetadataFileError::TooLarge {
                file_len: read_len,
                max_bytes: MAX_REPLICA_METADATA_BYTES,
            });
        }
        Ok(Some(decode_replica_metadata(&bytes)?))
    }

    /// Persists one replica metadata image using temp-write, `fsync`, rename, and directory sync.
    ///
    /// # Errors
    ///
    /// Returns [`ReplicaMetadataFileError`] if the write, sync, or replace path fails. On
    /// non-Unix targets this currently returns `Unsupported`, because the durable directory-sync
    /// replace discipline is only implemented for Unix.
    pub fn write_metadata(
        &self,
        metadata: &ReplicaMetadata,
    ) -> Result<(), ReplicaMetadataFileError> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }

        let temp_path = self.temp_path();
        let mut file = File::create(&temp_path)?;
        file.write_all(&encode_replica_metadata(metadata))?;
        file.sync_data()?;
        drop(file);

        replace_file_durably(&temp_path, &self.path)?;
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

#[derive(Debug)]
struct ReplicaPrepareLogFile {
    path: PathBuf,
}

impl ReplicaPrepareLogFile {
    fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }

    fn write_entries(
        &self,
        entries: &BTreeMap<u64, ReplicaPreparedEntry>,
    ) -> Result<(), std::io::Error> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }

        let temp_path = metadata_temp_path(&self.path);
        let mut file = File::create(&temp_path)?;
        file.write_all(&encode_prepare_log(entries))?;
        file.sync_all()?;
        drop(file);

        replace_file_durably(&temp_path, &self.path)
    }
}

#[derive(Debug)]
pub struct ReplicaNode {
    metadata: ReplicaMetadata,
    metadata_file: ReplicaMetadataFile,
    prepare_log_file: ReplicaPrepareLogFile,
    prepared_entries: BTreeMap<u64, ReplicaPreparedEntry>,
    paths: ReplicaPaths,
    status: ReplicaNodeStatus,
    engine: Option<SingleNodeEngine>,
}

impl ReplicaNode {
    /// Opens one replica wrapper around a fresh local allocator engine.
    ///
    /// Missing replica metadata is bootstrapped durably before the replica becomes active. Invalid
    /// or faulted metadata leaves the replica in explicit `faulted` state and skips engine start.
    ///
    /// # Errors
    ///
    /// Returns [`ReplicaOpenError`] if the local engine cannot be opened or if metadata bootstrap
    /// persistence fails.
    pub fn open(
        core_config: Config,
        engine_config: EngineConfig,
        identity: ReplicaIdentity,
        paths: ReplicaPaths,
    ) -> Result<Self, ReplicaOpenError> {
        let metadata_file = ReplicaMetadataFile::new(&paths.metadata_path);
        match load_or_fault_existing_metadata(&metadata_file, identity) {
            LoadedMetadata::Missing => {
                let engine = SingleNodeEngine::open(core_config, engine_config, &paths.wal_path)?;
                let metadata =
                    ReplicaMetadata::bootstrap(identity, engine.db().last_applied_lsn(), None);
                metadata_file.write_metadata(&metadata)?;
                info!(
                    "bootstrapped replica metadata: replica_id={} shard_id={} view={} role={:?}",
                    metadata.identity.replica_id.get(),
                    metadata.identity.shard_id,
                    metadata.current_view,
                    metadata.role,
                );
                Ok(Self::active(metadata, metadata_file, paths, engine))
            }
            LoadedMetadata::Faulted(metadata, reason) => {
                warn!(
                    "replica enters faulted state before open-path engine start: replica_id={} \
                     shard_id={} reason={:?}",
                    metadata.identity.replica_id.get(),
                    metadata.identity.shard_id,
                    reason,
                );
                Ok(Self::faulted(metadata, metadata_file, paths, reason))
            }
            LoadedMetadata::Ready(metadata) => {
                let engine = SingleNodeEngine::open(core_config, engine_config, &paths.wal_path)?;
                match validate_active_metadata(&metadata, identity, &engine) {
                    Err(reason) => {
                        error!(
                            "faulting replica after open-path validation: replica_id={} \
                             shard_id={} reason={:?}",
                            metadata.identity.replica_id.get(),
                            metadata.identity.shard_id,
                            reason,
                        );
                        Ok(Self::faulted(metadata, metadata_file, paths, reason))
                    }
                    Ok(()) => Ok(Self::active(metadata, metadata_file, paths, engine)),
                }
            }
        }
    }

    /// Recovers one replica wrapper from local snapshot plus WAL state.
    ///
    /// Missing replica metadata is bootstrapped from the recovered local engine state. Invalid or
    /// faulted metadata leaves the replica in explicit `faulted` state and skips engine start.
    ///
    /// # Errors
    ///
    /// Returns [`RecoverReplicaError`] if snapshot/WAL recovery fails or if metadata bootstrap
    /// persistence fails.
    pub fn recover(
        core_config: Config,
        engine_config: EngineConfig,
        identity: ReplicaIdentity,
        paths: ReplicaPaths,
    ) -> Result<Self, RecoverReplicaError> {
        let metadata_file = ReplicaMetadataFile::new(&paths.metadata_path);
        match load_or_fault_existing_metadata(&metadata_file, identity) {
            LoadedMetadata::Missing => {
                let engine = SingleNodeEngine::recover(
                    core_config,
                    engine_config,
                    &paths.snapshot_path,
                    &paths.wal_path,
                )?;
                let metadata = ReplicaMetadata::bootstrap(
                    identity,
                    engine.db().last_applied_lsn(),
                    engine.active_snapshot_lsn(),
                );
                metadata_file.write_metadata(&metadata)?;
                info!(
                    "bootstrapped recovered replica metadata: replica_id={} shard_id={} \
                     commit_lsn={:?}",
                    metadata.identity.replica_id.get(),
                    metadata.identity.shard_id,
                    metadata.commit_lsn,
                );
                Ok(Self::active(metadata, metadata_file, paths, engine))
            }
            LoadedMetadata::Faulted(metadata, reason) => {
                warn!(
                    "replica enters faulted state before recover-path engine start: replica_id={} \
                     shard_id={} reason={:?}",
                    metadata.identity.replica_id.get(),
                    metadata.identity.shard_id,
                    reason,
                );
                Ok(Self::faulted(metadata, metadata_file, paths, reason))
            }
            LoadedMetadata::Ready(metadata) => {
                let engine = SingleNodeEngine::recover(
                    core_config,
                    engine_config,
                    &paths.snapshot_path,
                    &paths.wal_path,
                )?;
                match validate_active_metadata(&metadata, identity, &engine) {
                    Err(reason) => {
                        error!(
                            "faulting replica after recover-path validation: replica_id={} \
                             shard_id={} reason={:?}",
                            metadata.identity.replica_id.get(),
                            metadata.identity.shard_id,
                            reason,
                        );
                        Ok(Self::faulted(metadata, metadata_file, paths, reason))
                    }
                    Ok(()) => Ok(Self::active(metadata, metadata_file, paths, engine)),
                }
            }
        }
    }

    #[must_use]
    pub fn metadata(&self) -> &ReplicaMetadata {
        &self.metadata
    }

    #[must_use]
    pub fn status(&self) -> ReplicaNodeStatus {
        self.status
    }

    #[must_use]
    pub fn engine(&self) -> Option<&SingleNodeEngine> {
        self.engine.as_ref()
    }

    #[must_use]
    pub fn metadata_path(&self) -> &Path {
        self.metadata_file.path()
    }

    #[must_use]
    pub fn paths(&self) -> &ReplicaPaths {
        &self.paths
    }

    /// Persists the current in-memory replicated metadata image.
    ///
    /// # Errors
    ///
    /// Returns [`ReplicaMetadataFileError`] if the metadata sidecar cannot be written durably.
    pub fn persist_metadata(&self) -> Result<(), ReplicaMetadataFileError> {
        self.metadata_file.write_metadata(&self.metadata)
    }

    #[must_use]
    pub fn prepared_entry(&self, lsn: Lsn) -> Option<&ReplicaPreparedEntry> {
        self.prepared_entries.get(&lsn.get())
    }

    #[must_use]
    pub fn prepared_len(&self) -> usize {
        self.prepared_entries.len()
    }

    #[must_use]
    pub fn highest_prepared_lsn(&self) -> Option<Lsn> {
        self.prepared_entries
            .last_key_value()
            .map(|(lsn, _)| Lsn(*lsn))
    }

    #[must_use]
    pub fn prepare_log_path(&self) -> &Path {
        &self.prepare_log_file.path
    }

    /// Persists one local checkpoint through the wrapped single-node engine and updates the active
    /// snapshot anchor in replica metadata.
    ///
    /// # Errors
    ///
    /// Returns [`ReplicaCheckpointError`] if the replica is faulted, if the checkpoint operation
    /// fails, or if the updated metadata cannot be persisted durably.
    ///
    /// # Panics
    ///
    /// Panics only if an `active` replica no longer holds its required live wrapped engine.
    pub fn checkpoint_local_state(&mut self) -> Result<CheckpointResult, ReplicaCheckpointError> {
        match self.status {
            ReplicaNodeStatus::Active => {}
            status @ ReplicaNodeStatus::Faulted(_) => {
                return Err(ReplicaCheckpointError::Inactive(status));
            }
        }

        let result = self
            .engine
            .as_mut()
            .expect("active replica must keep one live engine")
            .checkpoint(&self.paths.snapshot_path)?;
        self.metadata.active_snapshot_lsn = result.snapshot_lsn;
        self.persist_metadata()?;
        Ok(result)
    }

    /// Moves one active replica into normal mode for the provided view.
    ///
    /// # Errors
    ///
    /// Returns [`ReplicaProtocolError`] if the replica is faulted, the requested role is not
    /// `primary` or `backup`, the view would move backwards, or the updated metadata cannot be
    /// persisted durably.
    pub fn configure_normal_role(
        &mut self,
        current_view: u64,
        role: ReplicaRole,
    ) -> Result<(), ReplicaProtocolError> {
        self.ensure_active()?;
        if !matches!(role, ReplicaRole::Primary | ReplicaRole::Backup) {
            return Err(ReplicaProtocolError::UnsupportedNormalRole(role));
        }
        let highest_known_view = self.highest_known_view();
        if current_view < highest_known_view {
            return Err(ReplicaProtocolError::ViewRegression {
                current_view: highest_known_view,
                requested_view: current_view,
            });
        }

        self.metadata.current_view = current_view;
        self.metadata.role = role;
        self.metadata.last_normal_view = Some(current_view);
        self.metadata.durable_vote = None;
        self.persist_metadata()?;
        Ok(())
    }

    /// Records one durable vote for a higher view without entering normal mode.
    ///
    /// # Errors
    ///
    /// Returns [`ReplicaProtocolError`] if the replica is faulted, the requested view would move
    /// backwards relative to the highest persisted local view knowledge, or the updated metadata
    /// cannot be persisted durably.
    pub fn record_durable_vote(
        &mut self,
        voted_view: u64,
        voted_for: ReplicaId,
    ) -> Result<(), ReplicaProtocolError> {
        self.ensure_active()?;
        let highest_known_view = self.highest_known_view();
        if voted_view < highest_known_view {
            return Err(ReplicaProtocolError::ViewRegression {
                current_view: highest_known_view,
                requested_view: voted_view,
            });
        }

        self.metadata.durable_vote = Some(DurableVote {
            view: voted_view,
            voted_for,
        });
        self.persist_metadata()?;
        Ok(())
    }

    /// Moves one active replica into explicit view-uncertain mode.
    ///
    /// # Errors
    ///
    /// Returns [`ReplicaProtocolError`] if the replica is faulted or if the metadata update cannot
    /// be persisted durably.
    pub fn enter_view_uncertain(&mut self) -> Result<(), ReplicaProtocolError> {
        self.ensure_active()?;
        if self.metadata.role == ReplicaRole::ViewUncertain {
            return Ok(());
        }

        self.metadata.role = ReplicaRole::ViewUncertain;
        self.persist_metadata()?;
        Ok(())
    }

    /// Discards any uncommitted prepared suffix from local replica state.
    ///
    /// # Errors
    ///
    /// Returns [`ReplicaProtocolError`] if the replica is faulted or if the prepared-entry sidecar
    /// cannot be updated durably.
    pub fn discard_uncommitted_suffix(&mut self) -> Result<(), ReplicaProtocolError> {
        self.ensure_active()?;
        self.prepared_entries.clear();
        self.persist_prepared_entries()
    }

    /// Validates and durably prepares one client request on the current primary.
    ///
    /// # Errors
    ///
    /// Returns [`ReplicaProtocolError`] if the replica is not an active primary, if the next
    /// prepared LSN would overflow, if the payload fails the existing single-node ingress
    /// validation path, or if the prepared-entry sidecar cannot be persisted.
    pub fn prepare_client_request(
        &mut self,
        request_slot: Slot,
        payload: &[u8],
    ) -> Result<ReplicaPreparedEntry, ReplicaProtocolError> {
        self.require_role(ReplicaRole::Primary)?;
        let next_lsn = self.next_prepared_lsn()?;
        let entry = ReplicaPreparedEntry {
            view: self.metadata.current_view,
            lsn: next_lsn,
            request_slot,
            payload: payload.to_vec(),
        };
        self.append_prepared_entry(entry.clone())?;
        Ok(entry)
    }

    /// Durably appends one already-assigned prepared entry without applying it to allocator state.
    ///
    /// # Errors
    ///
    /// Returns [`ReplicaProtocolError`] if the replica is not in normal mode, if the entry view or
    /// LSN order is invalid, if a different prepared payload already occupies the same LSN, if the
    /// payload fails single-node ingress validation, or if the sidecar write fails.
    pub fn append_prepared_entry(
        &mut self,
        entry: ReplicaPreparedEntry,
    ) -> Result<(), ReplicaProtocolError> {
        self.ensure_normal_role()?;
        if entry.view != self.metadata.current_view {
            return Err(ReplicaProtocolError::ViewMismatch {
                expected: self.metadata.current_view,
                found: entry.view,
            });
        }
        if let Some(existing) = self.prepared_entries.get(&entry.lsn.get()) {
            return if existing == &entry {
                Ok(())
            } else {
                Err(ReplicaProtocolError::PreparedEntryConflict { lsn: entry.lsn })
            };
        }

        let expected_lsn = self.next_prepared_lsn()?;
        if entry.lsn != expected_lsn {
            return Err(ReplicaProtocolError::PrepareOrderMismatch {
                expected_lsn,
                found_lsn: entry.lsn,
            });
        }

        self.engine_mut()?
            .validate_encoded_submission(entry.request_slot, &entry.payload)?;
        self.prepared_entries.insert(entry.lsn.get(), entry);
        self.persist_prepared_entries()
    }

    /// Applies prepared entries through the existing single-node executor up to one committed LSN.
    ///
    /// # Errors
    ///
    /// Returns [`ReplicaProtocolError`] if the replica is not in normal mode, if any committed LSN
    /// is missing from the prepared buffer, if the entry view no longer matches the local view, if
    /// the single-node executor rejects the payload, if the applied LSN diverges from the prepared
    /// position, or if the updated metadata / prepared-entry sidecar cannot be persisted.
    pub fn commit_prepared_through(
        &mut self,
        commit_lsn: Lsn,
    ) -> Result<Option<SubmissionResult>, ReplicaProtocolError> {
        self.ensure_normal_role()?;
        self.commit_prepared_through_impl(commit_lsn)
    }

    /// Applies prepared entries through one already reconstructed committed prefix while the
    /// replica remains outside normal mode.
    ///
    /// # Errors
    ///
    /// Returns [`ReplicaProtocolError`] if the replica is faulted, if the local role is not
    /// `backup` or `view_uncertain`, if any committed LSN is missing from the prepared buffer, if
    /// the entry view no longer matches the local view, if the single-node executor rejects the
    /// payload, if the applied LSN diverges from the prepared position, or if the updated metadata
    /// / prepared-entry sidecar cannot be persisted.
    pub fn reconstruct_committed_prefix_through(
        &mut self,
        commit_lsn: Lsn,
    ) -> Result<Option<SubmissionResult>, ReplicaProtocolError> {
        self.ensure_active()?;
        if !matches!(
            self.metadata.role,
            ReplicaRole::Backup | ReplicaRole::ViewUncertain
        ) {
            return Err(ReplicaProtocolError::RoleSetMismatch {
                expected: "backup or view_uncertain",
                found: self.metadata.role,
            });
        }
        self.commit_prepared_through_impl(commit_lsn)
    }

    /// Enforces the first replicated-release read rule: only the current primary may serve reads.
    ///
    /// # Errors
    ///
    /// Returns [`NotPrimaryReadError`] if the replica is faulted, is not currently the primary, or
    /// has not yet applied the required committed LSN locally.
    pub fn enforce_primary_read(&self, required_lsn: Lsn) -> Result<(), NotPrimaryReadError> {
        match self.status {
            ReplicaNodeStatus::Active => {}
            ReplicaNodeStatus::Faulted(_) => return Err(NotPrimaryReadError::ReplicaCrashed),
        }

        if self.metadata.role != ReplicaRole::Primary {
            return Err(NotPrimaryReadError::Role(self.metadata.role));
        }

        self.engine
            .as_ref()
            .ok_or(NotPrimaryReadError::ReplicaCrashed)?
            .enforce_read_fence(required_lsn)
            .map_err(NotPrimaryReadError::Fence)
    }

    fn ensure_active(&self) -> Result<(), ReplicaProtocolError> {
        match self.status {
            ReplicaNodeStatus::Active => Ok(()),
            status @ ReplicaNodeStatus::Faulted(_) => Err(ReplicaProtocolError::Inactive(status)),
        }
    }

    fn require_role(&self, expected: ReplicaRole) -> Result<(), ReplicaProtocolError> {
        self.ensure_active()?;
        if self.metadata.role == expected {
            Ok(())
        } else {
            Err(ReplicaProtocolError::RoleMismatch {
                expected,
                found: self.metadata.role,
            })
        }
    }

    fn ensure_normal_role(&self) -> Result<(), ReplicaProtocolError> {
        self.ensure_active()?;
        match self.metadata.role {
            ReplicaRole::Primary | ReplicaRole::Backup => Ok(()),
            found => Err(ReplicaProtocolError::RoleMismatch {
                expected: ReplicaRole::Backup,
                found,
            }),
        }
    }

    fn highest_known_view(&self) -> u64 {
        self.metadata
            .durable_vote
            .map_or(self.metadata.current_view, |vote| {
                self.metadata.current_view.max(vote.view)
            })
    }

    fn next_prepared_lsn(&self) -> Result<Lsn, ReplicaProtocolError> {
        let last_lsn = self
            .prepared_entries
            .last_key_value()
            .map(|(lsn, _)| Lsn(*lsn))
            .or(self.metadata.commit_lsn);
        match last_lsn {
            Some(last_lsn) => last_lsn
                .get()
                .checked_add(1)
                .map(Lsn)
                .ok_or(ReplicaProtocolError::PrepareLsnExhausted { last_lsn }),
            None => Ok(Lsn(1)),
        }
    }

    fn persist_prepared_entries(&self) -> Result<(), ReplicaProtocolError> {
        self.prepare_log_file
            .write_entries(&self.prepared_entries)
            .map_err(|error| ReplicaProtocolError::PrepareStorage(error.kind()))
    }

    fn commit_prepared_through_impl(
        &mut self,
        commit_lsn: Lsn,
    ) -> Result<Option<SubmissionResult>, ReplicaProtocolError> {
        let first_pending = self
            .metadata
            .commit_lsn
            .and_then(|lsn| lsn.get().checked_add(1))
            .unwrap_or(1);
        if commit_lsn.get() < first_pending {
            return Ok(None);
        }

        let entries = (first_pending..=commit_lsn.get())
            .map(|lsn| {
                self.prepared_entries
                    .get(&lsn)
                    .cloned()
                    .ok_or(ReplicaProtocolError::MissingPreparedEntry { lsn: Lsn(lsn) })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let mut last_result = None;
        for entry in &entries {
            if entry.view != self.metadata.current_view {
                return Err(ReplicaProtocolError::ViewMismatch {
                    expected: self.metadata.current_view,
                    found: entry.view,
                });
            }
            let result = self
                .engine_mut()?
                .submit_encoded(entry.request_slot, &entry.payload)?;
            if result.applied_lsn != entry.lsn {
                return Err(ReplicaProtocolError::AppliedLsnMismatch {
                    expected: entry.lsn,
                    found: result.applied_lsn,
                });
            }
            self.metadata.commit_lsn = Some(entry.lsn);
            self.metadata.active_snapshot_lsn = self
                .engine
                .as_ref()
                .and_then(SingleNodeEngine::active_snapshot_lsn);
            last_result = Some(result);
        }

        for entry in &entries {
            self.prepared_entries.remove(&entry.lsn.get());
        }

        self.persist_prepared_entries()?;
        self.persist_metadata()?;
        Ok(last_result)
    }

    fn engine_mut(&mut self) -> Result<&mut SingleNodeEngine, ReplicaProtocolError> {
        self.ensure_active()?;
        Ok(self
            .engine
            .as_mut()
            .expect("active replica must keep one live engine"))
    }

    fn active(
        metadata: ReplicaMetadata,
        metadata_file: ReplicaMetadataFile,
        paths: ReplicaPaths,
        engine: SingleNodeEngine,
    ) -> Self {
        Self {
            metadata,
            metadata_file,
            prepare_log_file: ReplicaPrepareLogFile::new(prepare_log_path(&paths.metadata_path)),
            prepared_entries: BTreeMap::new(),
            paths,
            status: ReplicaNodeStatus::Active,
            engine: Some(engine),
        }
    }

    fn faulted(
        metadata: ReplicaMetadata,
        metadata_file: ReplicaMetadataFile,
        paths: ReplicaPaths,
        reason: ReplicaFaultReason,
    ) -> Self {
        Self {
            metadata,
            metadata_file,
            prepare_log_file: ReplicaPrepareLogFile::new(prepare_log_path(&paths.metadata_path)),
            prepared_entries: BTreeMap::new(),
            paths,
            status: ReplicaNodeStatus::Faulted(ReplicaFault { reason }),
            engine: None,
        }
    }
}

enum LoadedMetadata {
    Missing,
    Ready(ReplicaMetadata),
    Faulted(ReplicaMetadata, ReplicaFaultReason),
}

fn load_or_fault_existing_metadata(
    metadata_file: &ReplicaMetadataFile,
    identity: ReplicaIdentity,
) -> LoadedMetadata {
    match metadata_file.load_metadata() {
        Ok(None) => LoadedMetadata::Missing,
        Ok(Some(metadata)) if metadata.role == ReplicaRole::Faulted => {
            LoadedMetadata::Faulted(metadata, ReplicaFaultReason::PersistedFaultState)
        }
        Ok(Some(metadata)) => LoadedMetadata::Ready(metadata),
        Err(error) => {
            let reason = match &error {
                ReplicaMetadataFileError::Io(io_error) => {
                    ReplicaFaultReason::MetadataLoad(ReplicaMetadataLoadError::Io(io_error.kind()))
                }
                ReplicaMetadataFileError::Decode(decode_error) => ReplicaFaultReason::MetadataLoad(
                    ReplicaMetadataLoadError::Decode(*decode_error),
                ),
                ReplicaMetadataFileError::TooLarge {
                    file_len,
                    max_bytes,
                } => ReplicaFaultReason::MetadataLoad(ReplicaMetadataLoadError::TooLarge {
                    file_len: *file_len,
                    max_bytes: *max_bytes,
                }),
            };
            error!(
                "faulting replica before engine start because metadata load failed: replica_id={} \
                 shard_id={} reason={:?}",
                identity.replica_id.get(),
                identity.shard_id,
                reason,
            );
            let metadata = ReplicaMetadata {
                identity,
                current_view: 0,
                role: ReplicaRole::Faulted,
                commit_lsn: None,
                active_snapshot_lsn: None,
                last_normal_view: None,
                durable_vote: None,
            };
            LoadedMetadata::Faulted(metadata, reason)
        }
    }
}

fn validate_active_metadata(
    metadata: &ReplicaMetadata,
    identity: ReplicaIdentity,
    engine: &SingleNodeEngine,
) -> Result<(), ReplicaFaultReason> {
    metadata
        .validate(
            identity,
            engine.db().last_applied_lsn(),
            engine.active_snapshot_lsn(),
        )
        .map_err(ReplicaFaultReason::Validation)
}

pub(crate) fn prepare_log_path(metadata_path: &Path) -> PathBuf {
    let mut path = metadata_path.to_path_buf();
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .map_or_else(|| "prepare".to_owned(), |value| format!("{value}.prepare"));
    path.set_extension(extension);
    path
}

fn metadata_temp_path(path: &Path) -> PathBuf {
    let mut temp_path = path.to_path_buf();
    let extension = temp_path
        .extension()
        .and_then(|value| value.to_str())
        .map_or_else(|| "tmp".to_owned(), |value| format!("{value}.tmp"));
    temp_path.set_extension(extension);
    temp_path
}

fn encode_prepare_log(entries: &BTreeMap<u64, ReplicaPreparedEntry>) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&REPLICA_PREPARE_LOG_MAGIC);
    bytes.push(REPLICA_PREPARE_LOG_VERSION);
    bytes.extend_from_slice(
        &u64::try_from(entries.len())
            .expect("prepared entry count must fit u64")
            .to_le_bytes(),
    );
    for entry in entries.values() {
        bytes.extend_from_slice(&entry.view.to_le_bytes());
        bytes.extend_from_slice(&entry.lsn.get().to_le_bytes());
        bytes.extend_from_slice(&entry.request_slot.get().to_le_bytes());
        bytes.extend_from_slice(
            &u64::try_from(entry.payload.len())
                .expect("prepared payload length must fit u64")
                .to_le_bytes(),
        );
        bytes.extend_from_slice(&entry.payload);
    }
    bytes
}

fn encode_replica_metadata(metadata: &ReplicaMetadata) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&REPLICA_METADATA_MAGIC);
    bytes.push(REPLICA_METADATA_VERSION);
    bytes.extend_from_slice(&metadata.identity.replica_id.get().to_le_bytes());
    bytes.extend_from_slice(&metadata.identity.shard_id.to_le_bytes());
    bytes.extend_from_slice(&metadata.current_view.to_le_bytes());
    bytes.push(encode_role(metadata.role));
    encode_optional_lsn(&mut bytes, metadata.commit_lsn);
    encode_optional_lsn(&mut bytes, metadata.active_snapshot_lsn);
    encode_optional_u64(&mut bytes, metadata.last_normal_view);
    encode_optional_vote(&mut bytes, metadata.durable_vote);
    bytes
}

fn decode_replica_metadata(bytes: &[u8]) -> Result<ReplicaMetadata, ReplicaMetadataDecodeError> {
    let mut cursor = MetadataCursor::new(bytes);
    if cursor.read_exact::<4>()? != REPLICA_METADATA_MAGIC {
        return Err(ReplicaMetadataDecodeError::InvalidMagic);
    }

    let version = cursor.read_u8()?;
    if version != REPLICA_METADATA_VERSION {
        return Err(ReplicaMetadataDecodeError::UnsupportedVersion(version));
    }

    let metadata = ReplicaMetadata {
        identity: ReplicaIdentity {
            replica_id: ReplicaId(cursor.read_u64()?),
            shard_id: cursor.read_u64()?,
        },
        current_view: cursor.read_u64()?,
        role: decode_role(cursor.read_u8()?)?,
        commit_lsn: cursor.read_optional_u64("commit_lsn")?.map(Lsn),
        active_snapshot_lsn: cursor.read_optional_u64("active_snapshot_lsn")?.map(Lsn),
        last_normal_view: cursor.read_optional_u64("last_normal_view")?,
        durable_vote: cursor.read_optional_vote()?,
    };
    cursor.finish()?;
    Ok(metadata)
}

const fn encode_role(role: ReplicaRole) -> u8 {
    match role {
        ReplicaRole::Primary => 1,
        ReplicaRole::Backup => 2,
        ReplicaRole::ViewUncertain => 3,
        ReplicaRole::Recovering => 4,
        ReplicaRole::Faulted => 5,
    }
}

fn decode_role(value: u8) -> Result<ReplicaRole, ReplicaMetadataDecodeError> {
    match value {
        1 => Ok(ReplicaRole::Primary),
        2 => Ok(ReplicaRole::Backup),
        3 => Ok(ReplicaRole::ViewUncertain),
        4 => Ok(ReplicaRole::Recovering),
        5 => Ok(ReplicaRole::Faulted),
        _ => Err(ReplicaMetadataDecodeError::InvalidRole(value)),
    }
}

fn encode_optional_lsn(bytes: &mut Vec<u8>, value: Option<Lsn>) {
    encode_optional_u64(bytes, value.map(Lsn::get));
}

fn encode_optional_u64(bytes: &mut Vec<u8>, value: Option<u64>) {
    match value {
        None => bytes.push(0),
        Some(value) => {
            bytes.push(1);
            bytes.extend_from_slice(&value.to_le_bytes());
        }
    }
}

fn encode_optional_vote(bytes: &mut Vec<u8>, vote: Option<DurableVote>) {
    match vote {
        None => bytes.push(0),
        Some(vote) => {
            bytes.push(1);
            bytes.extend_from_slice(&vote.view.to_le_bytes());
            bytes.extend_from_slice(&vote.voted_for.get().to_le_bytes());
        }
    }
}

struct MetadataCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> MetadataCursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn finish(&self) -> Result<(), ReplicaMetadataDecodeError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(ReplicaMetadataDecodeError::InvalidLayout)
        }
    }

    fn read_exact<const N: usize>(&mut self) -> Result<[u8; N], ReplicaMetadataDecodeError> {
        if self.offset + N > self.bytes.len() {
            return Err(ReplicaMetadataDecodeError::BufferTooShort);
        }

        let mut array = [0; N];
        array.copy_from_slice(&self.bytes[self.offset..self.offset + N]);
        self.offset += N;
        Ok(array)
    }

    fn read_u8(&mut self) -> Result<u8, ReplicaMetadataDecodeError> {
        Ok(self.read_exact::<1>()?[0])
    }

    fn read_u64(&mut self) -> Result<u64, ReplicaMetadataDecodeError> {
        Ok(u64::from_le_bytes(self.read_exact::<8>()?))
    }

    fn read_optional_u64(
        &mut self,
        kind: &'static str,
    ) -> Result<Option<u64>, ReplicaMetadataDecodeError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_u64()?)),
            value => Err(ReplicaMetadataDecodeError::InvalidOptionTag { kind, value }),
        }
    }

    fn read_optional_vote(&mut self) -> Result<Option<DurableVote>, ReplicaMetadataDecodeError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(DurableVote {
                view: self.read_u64()?,
                voted_for: ReplicaId(self.read_u64()?),
            })),
            value => Err(ReplicaMetadataDecodeError::InvalidOptionTag {
                kind: "durable_vote",
                value,
            }),
        }
    }
}

#[cfg(unix)]
fn replace_file_durably(temp_path: &Path, path: &Path) -> Result<(), std::io::Error> {
    fs::rename(temp_path, path)?;
    sync_parent_dir(path)?;
    Ok(())
}

#[cfg(not(unix))]
fn replace_file_durably(_temp_path: &Path, _path: &Path) -> Result<(), std::io::Error> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "replica metadata durable replace currently requires Unix directory-sync semantics",
    ))
}

#[cfg(unix)]
fn sync_parent_dir(path: &Path) -> Result<(), std::io::Error> {
    if let Some(parent) = path.parent() {
        OpenOptions::new().read(true).open(parent)?.sync_all()?;
    }
    Ok(())
}

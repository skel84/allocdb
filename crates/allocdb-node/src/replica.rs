use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use allocdb_core::config::Config;
use allocdb_core::ids::Lsn;
use log::{error, info, warn};

use crate::engine::{EngineConfig, EngineOpenError, RecoverEngineError, SingleNodeEngine};

#[cfg(test)]
#[path = "replica_tests.rs"]
mod tests;

const MAX_REPLICA_METADATA_BYTES: u64 = 256;
const REPLICA_METADATA_MAGIC: [u8; 4] = *b"RPLM";
const REPLICA_METADATA_VERSION: u8 = 1;

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

        let committed = self.commit_lsn.map_or(0, Lsn::get);
        if let Some(last_applied_lsn) = local_last_applied_lsn {
            if last_applied_lsn.get() > committed {
                return Err(ReplicaStartupValidationError::AppliedLsnExceedsCommitLsn {
                    last_applied_lsn,
                    commit_lsn: self.commit_lsn,
                });
            }
        }

        if self.active_snapshot_lsn != local_active_snapshot_lsn {
            return Err(ReplicaStartupValidationError::ActiveSnapshotMismatch {
                metadata_snapshot_lsn: self.active_snapshot_lsn,
                local_snapshot_lsn: local_active_snapshot_lsn,
            });
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
pub struct ReplicaNode {
    metadata: ReplicaMetadata,
    metadata_file: ReplicaMetadataFile,
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

    fn active(
        metadata: ReplicaMetadata,
        metadata_file: ReplicaMetadataFile,
        paths: ReplicaPaths,
        engine: SingleNodeEngine,
    ) -> Self {
        Self {
            metadata,
            metadata_file,
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
        ReplicaRole::Recovering => 3,
        ReplicaRole::Faulted => 4,
    }
}

fn decode_role(value: u8) -> Result<ReplicaRole, ReplicaMetadataDecodeError> {
    match value {
        1 => Ok(ReplicaRole::Primary),
        2 => Ok(ReplicaRole::Backup),
        3 => Ok(ReplicaRole::Recovering),
        4 => Ok(ReplicaRole::Faulted),
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

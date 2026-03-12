use std::path::Path;

use allocdb_core::command::{ClientRequest, CommandContext};
use allocdb_core::command_codec::{
    CommandCodecError, decode_client_request, encode_client_request,
};
use allocdb_core::config::{Config, ConfigError};
use allocdb_core::ids::{Lsn, OperationId, Slot};
use allocdb_core::recovery::{RecoveryError, recover_allocdb};
use allocdb_core::result::{CommandOutcome, ResultCode};
use allocdb_core::snapshot_file::SnapshotFile;
use allocdb_core::state_machine::AllocDb;
use allocdb_core::wal::{Frame, RecordType};
use allocdb_core::wal_file::{WalFile, WalFileError};

use crate::bounded_queue::{BoundedQueue, BoundedQueueError};

#[path = "engine_checkpoint.rs"]
mod checkpoint;
#[cfg(test)]
#[path = "engine_checkpoint_tests.rs"]
mod checkpoint_tests;
#[path = "engine_observe.rs"]
mod observe;
#[cfg(test)]
#[path = "engine_tests.rs"]
mod tests;
pub use checkpoint::{CheckpointError, CheckpointResult};
pub use observe::{EngineMetrics, ReadError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EngineConfig {
    pub max_submission_queue: u32,
    pub max_command_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EngineConfigError {
    ZeroCapacity(&'static str),
}

impl EngineConfig {
    /// Validates that the submission-layer bounds are internally consistent.
    ///
    /// # Errors
    ///
    /// Returns [`EngineConfigError`] when a required queue or payload bound is zero.
    pub fn validate(&self) -> Result<(), EngineConfigError> {
        if self.max_submission_queue == 0 {
            return Err(EngineConfigError::ZeroCapacity("max_submission_queue"));
        }

        if self.max_command_bytes == 0 {
            return Err(EngineConfigError::ZeroCapacity("max_command_bytes"));
        }

        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubmissionResult {
    pub applied_lsn: Lsn,
    pub outcome: CommandOutcome,
    pub from_retry_cache: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnqueueResult {
    Queued,
    Pending {
        operation_id: OperationId,
        command_fingerprint: u128,
    },
    Published(SubmissionResult),
}

#[derive(Debug)]
pub enum EngineOpenError {
    CoreConfig(ConfigError),
    EngineConfig(EngineConfigError),
    WalFile(WalFileError),
}

impl From<ConfigError> for EngineOpenError {
    fn from(error: ConfigError) -> Self {
        Self::CoreConfig(error)
    }
}

impl From<EngineConfigError> for EngineOpenError {
    fn from(error: EngineConfigError) -> Self {
        Self::EngineConfig(error)
    }
}

impl From<WalFileError> for EngineOpenError {
    fn from(error: WalFileError) -> Self {
        Self::WalFile(error)
    }
}

#[derive(Debug)]
pub enum SubmissionError {
    EngineHalted,
    InvalidRequest(CommandCodecError),
    CommandTooLarge {
        encoded_len: usize,
        max_command_bytes: usize,
    },
    Overloaded {
        queue_depth: u32,
        queue_capacity: u32,
    },
    WalFile(WalFileError),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubmissionErrorCategory {
    DefiniteFailure,
    Indefinite,
}

impl From<WalFileError> for SubmissionError {
    fn from(error: WalFileError) -> Self {
        Self::WalFile(error)
    }
}

impl SubmissionError {
    #[must_use]
    pub fn category(&self) -> SubmissionErrorCategory {
        match self {
            Self::EngineHalted | Self::WalFile(_) => SubmissionErrorCategory::Indefinite,
            Self::InvalidRequest(_) | Self::CommandTooLarge { .. } | Self::Overloaded { .. } => {
                SubmissionErrorCategory::DefiniteFailure
            }
        }
    }
}

#[derive(Debug)]
pub enum RecoverEngineError {
    Recovery(RecoveryError),
    EngineOpen(EngineOpenError),
}

impl From<RecoveryError> for RecoverEngineError {
    fn from(error: RecoveryError) -> Self {
        Self::Recovery(error)
    }
}

impl From<EngineOpenError> for RecoverEngineError {
    fn from(error: EngineOpenError) -> Self {
        Self::EngineOpen(error)
    }
}

#[derive(Debug)]
struct PendingSubmission {
    request: ClientRequest,
    request_slot: Slot,
    encoded: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PersistFailurePhase {
    BeforeAppend,
    AfterAppend,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ProcessedSubmission {
    operation_id: OperationId,
    command_fingerprint: u128,
    result: SubmissionResult,
}

#[derive(Debug)]
pub struct SingleNodeEngine {
    db: AllocDb,
    wal: WalFile,
    queue: BoundedQueue<PendingSubmission>,
    config: EngineConfig,
    next_lsn: u64,
    accepting_writes: bool,
    active_snapshot_lsn: Option<Lsn>,
    // One-shot failure injection used only to exercise ambiguous WAL outcomes in tests.
    injected_persist_failure: Option<PersistFailurePhase>,
}

impl SingleNodeEngine {
    /// Opens a fresh single-node engine with one allocator core and one WAL file.
    ///
    /// # Errors
    ///
    /// Returns [`EngineOpenError`] if core or engine configuration is invalid or if the WAL file
    /// cannot be opened.
    pub fn open(
        core_config: Config,
        engine_config: EngineConfig,
        wal_path: impl AsRef<Path>,
    ) -> Result<Self, EngineOpenError> {
        let db = AllocDb::new(core_config)?;
        Self::from_parts(db, engine_config, wal_path, None)
    }

    /// Recovers one engine from snapshot plus WAL, then reopens the live WAL path for new writes.
    ///
    /// # Errors
    ///
    /// Returns [`RecoverEngineError`] if recovery fails or if the live engine cannot be reopened.
    ///
    /// # Panics
    ///
    /// Panics only if validated queue bounds cannot fit the platform `usize`.
    pub fn recover(
        core_config: Config,
        engine_config: EngineConfig,
        snapshot_path: impl AsRef<Path>,
        wal_path: impl AsRef<Path>,
    ) -> Result<Self, RecoverEngineError> {
        engine_config.validate().map_err(EngineOpenError::from)?;
        let snapshot_file = SnapshotFile::new(snapshot_path);
        let wal = WalFile::open(wal_path.as_ref(), engine_config.max_command_bytes)
            .map_err(EngineOpenError::from)?;
        let recovered = recover_allocdb(core_config, &snapshot_file, &wal)?;
        Self::from_parts(
            recovered.db,
            engine_config,
            wal_path,
            recovered.loaded_snapshot_lsn,
        )
        .map_err(RecoverEngineError::from)
    }

    /// Builds one engine around an existing allocator state, for example after recovery.
    ///
    /// # Errors
    ///
    /// Returns [`EngineOpenError`] if engine configuration is invalid or if the WAL file cannot be
    /// opened.
    ///
    /// # Panics
    ///
    /// Panics only if validated queue bounds cannot fit the platform `usize`.
    pub fn from_parts(
        db: AllocDb,
        engine_config: EngineConfig,
        wal_path: impl AsRef<Path>,
        active_snapshot_lsn: Option<Lsn>,
    ) -> Result<Self, EngineOpenError> {
        engine_config.validate()?;
        let wal = WalFile::open(wal_path, engine_config.max_command_bytes)?;
        let next_lsn = db.last_applied_lsn().map_or(1, |lsn| lsn.get() + 1);

        Ok(Self {
            db,
            wal,
            queue: BoundedQueue::with_capacity(
                usize::try_from(engine_config.max_submission_queue)
                    .expect("validated max_submission_queue must fit usize"),
            ),
            config: engine_config,
            next_lsn,
            accepting_writes: true,
            active_snapshot_lsn,
            injected_persist_failure: None,
        })
    }

    #[must_use]
    pub fn db(&self) -> &AllocDb {
        &self.db
    }

    #[must_use]
    pub fn wal_path(&self) -> &Path {
        self.wal.path()
    }

    /// Validates and submits one already-decoded client request through the bounded engine path.
    ///
    /// # Errors
    ///
    /// Returns [`SubmissionError`] if the request is too large, the queue is overloaded, the
    /// engine is halted, or the WAL append/sync fails.
    ///
    /// # Panics
    ///
    /// Panics only if internal queue bookkeeping is already inconsistent after a successful
    /// enqueue.
    pub fn submit(
        &mut self,
        request_slot: Slot,
        request: ClientRequest,
    ) -> Result<SubmissionResult, SubmissionError> {
        match self.enqueue_client(request_slot, request)? {
            EnqueueResult::Queued => Ok(self
                .process_next()?
                .expect("queued submission must produce one result")),
            EnqueueResult::Pending {
                operation_id,
                command_fingerprint,
            } => self.process_until_operation(operation_id, command_fingerprint),
            EnqueueResult::Published(result) => Ok(result),
        }
    }

    /// Validates, decodes, and submits one encoded client request before sequencing.
    ///
    /// # Errors
    ///
    /// Returns [`SubmissionError`] if decoding fails, the request is too large, the queue is
    /// overloaded, the engine is halted, or the WAL append/sync fails.
    ///
    /// # Panics
    ///
    /// Panics only if internal queue bookkeeping is already inconsistent after a successful
    /// enqueue.
    pub fn submit_encoded(
        &mut self,
        request_slot: Slot,
        encoded: &[u8],
    ) -> Result<SubmissionResult, SubmissionError> {
        match self.enqueue_encoded(request_slot, encoded)? {
            EnqueueResult::Queued => Ok(self
                .process_next()?
                .expect("queued submission must produce one result")),
            EnqueueResult::Pending {
                operation_id,
                command_fingerprint,
            } => self.process_until_operation(operation_id, command_fingerprint),
            EnqueueResult::Published(result) => Ok(result),
        }
    }

    /// Validates and enqueues one already-decoded client request.
    ///
    /// # Errors
    ///
    /// Returns [`SubmissionError`] if the request is too large, the queue is overloaded, or the
    /// engine is halted.
    pub fn enqueue_client(
        &mut self,
        request_slot: Slot,
        request: ClientRequest,
    ) -> Result<EnqueueResult, SubmissionError> {
        let encoded = encode_client_request(request);
        self.enqueue_validated(request_slot, request, encoded)
    }

    /// Validates and enqueues one encoded client request without mutating allocator state.
    ///
    /// # Errors
    ///
    /// Returns [`SubmissionError`] if decoding fails, the request is too large, the queue is
    /// overloaded, or the engine is halted.
    pub fn enqueue_encoded(
        &mut self,
        request_slot: Slot,
        encoded: &[u8],
    ) -> Result<EnqueueResult, SubmissionError> {
        self.validate_bytes(encoded)?;
        let request = decode_client_request(encoded).map_err(SubmissionError::InvalidRequest)?;
        self.enqueue_validated(request_slot, request, encoded.to_vec())
    }

    /// Processes one queued submission by assigning an LSN, appending it to the WAL, syncing, and
    /// applying through the live allocator path.
    ///
    /// # Errors
    ///
    /// Returns [`SubmissionError`] if the engine is halted or the WAL append/sync fails.
    pub fn process_next(&mut self) -> Result<Option<SubmissionResult>, SubmissionError> {
        Ok(self.process_one()?.map(|processed| processed.result))
    }

    fn enqueue_validated(
        &mut self,
        request_slot: Slot,
        request: ClientRequest,
        encoded: Vec<u8>,
    ) -> Result<EnqueueResult, SubmissionError> {
        if !self.accepting_writes {
            return Err(SubmissionError::EngineHalted);
        }

        self.validate_bytes(&encoded)?;

        if let Some(existing) = self.lookup_existing(request_slot, request) {
            return Ok(EnqueueResult::Published(existing));
        }

        if self
            .queue
            .iter()
            .any(|pending| pending.request.operation_id == request.operation_id)
        {
            return Ok(EnqueueResult::Pending {
                operation_id: request.operation_id,
                command_fingerprint: request.command.fingerprint(),
            });
        }

        self.queue
            .push(PendingSubmission {
                request,
                request_slot,
                encoded,
            })
            .map_err(|error| match error {
                BoundedQueueError::Full => SubmissionError::Overloaded {
                    queue_depth: u32::try_from(self.queue.len()).expect("queue depth must fit u32"),
                    queue_capacity: self.config.max_submission_queue,
                },
            })?;

        Ok(EnqueueResult::Queued)
    }

    fn process_one(&mut self) -> Result<Option<ProcessedSubmission>, SubmissionError> {
        if !self.accepting_writes {
            return Err(SubmissionError::EngineHalted);
        }

        let Some(pending) = self.queue.pop_front() else {
            return Ok(None);
        };

        let operation_id = pending.request.operation_id;
        let command_fingerprint = pending.request.command.fingerprint();
        let applied_lsn = Lsn(self.next_lsn);
        let frame = Frame {
            lsn: applied_lsn,
            request_slot: pending.request_slot,
            record_type: RecordType::ClientCommand,
            payload: pending.encoded,
        };

        let injected_failure = self.take_injected_persist_failure();
        if injected_failure == Some(PersistFailurePhase::BeforeAppend) {
            return Err(
                self.halt_on_wal_error(std::io::Error::other("injected WAL failure before append"))
            );
        }

        if let Err(error) = self.wal.append_frame(&frame) {
            return Err(self.halt_on_wal_error(error));
        }

        if injected_failure == Some(PersistFailurePhase::AfterAppend) {
            return Err(
                self.halt_on_wal_error(std::io::Error::other("injected WAL failure after append"))
            );
        }

        if let Err(error) = self.wal.sync() {
            return Err(self.halt_on_wal_error(error));
        }

        let outcome = self.db.apply_client(
            CommandContext {
                lsn: applied_lsn,
                request_slot: pending.request_slot,
            },
            pending.request,
        );
        self.next_lsn += 1;

        Ok(Some(ProcessedSubmission {
            operation_id,
            command_fingerprint,
            result: SubmissionResult {
                applied_lsn,
                outcome,
                from_retry_cache: false,
            },
        }))
    }

    fn process_until_operation(
        &mut self,
        operation_id: OperationId,
        command_fingerprint: u128,
    ) -> Result<SubmissionResult, SubmissionError> {
        loop {
            let processed = self
                .process_one()?
                .expect("pending queued operation must eventually be processed");
            if processed.operation_id == operation_id {
                return Ok(if processed.command_fingerprint == command_fingerprint {
                    SubmissionResult {
                        from_retry_cache: true,
                        ..processed.result
                    }
                } else {
                    SubmissionResult {
                        applied_lsn: processed.result.applied_lsn,
                        outcome: CommandOutcome::new(ResultCode::OperationConflict),
                        from_retry_cache: true,
                    }
                });
            }
        }
    }

    fn lookup_existing(
        &self,
        request_slot: Slot,
        request: ClientRequest,
    ) -> Option<SubmissionResult> {
        let fingerprint = request.command.fingerprint();
        self.db
            .operation(request.operation_id, request_slot)
            .map(|record| {
                let outcome = if record.command_fingerprint == fingerprint {
                    CommandOutcome {
                        result_code: record.result_code,
                        reservation_id: record.result_reservation_id,
                        deadline_slot: record.result_deadline_slot,
                    }
                } else {
                    CommandOutcome::new(ResultCode::OperationConflict)
                };

                SubmissionResult {
                    applied_lsn: record.applied_lsn,
                    outcome,
                    from_retry_cache: true,
                }
            })
    }

    fn validate_bytes(&self, encoded: &[u8]) -> Result<(), SubmissionError> {
        if encoded.len() > self.config.max_command_bytes {
            return Err(SubmissionError::CommandTooLarge {
                encoded_len: encoded.len(),
                max_command_bytes: self.config.max_command_bytes,
            });
        }

        Ok(())
    }

    fn halt_on_wal_error(&mut self, error: impl Into<WalFileError>) -> SubmissionError {
        self.accepting_writes = false;
        SubmissionError::WalFile(error.into())
    }

    fn take_injected_persist_failure(&mut self) -> Option<PersistFailurePhase> {
        self.injected_persist_failure.take()
    }

    #[cfg(test)]
    fn inject_next_persist_failure(&mut self, phase: PersistFailurePhase) {
        self.injected_persist_failure = Some(phase);
    }
}

use std::path::Path;

use allocdb_core::ReservationState;
use allocdb_core::SlotOverflowError;
use allocdb_core::command::{ClientRequest, Command, CommandContext};
use allocdb_core::command_codec::{
    CommandCodecError, decode_client_request, encode_client_request, encode_internal_command,
};
use allocdb_core::config::{Config, ConfigError};
use allocdb_core::ids::{Lsn, OperationId, ReservationId, Slot};
use allocdb_core::recovery::{
    RecoveryBoundary, RecoveryError, RecoveryObserverError, recover_allocdb_with_observer,
};
use allocdb_core::result::{CommandOutcome, ResultCode};
use allocdb_core::snapshot_file::SnapshotFile;
use allocdb_core::state_machine::AllocDb;
use allocdb_core::wal::{Frame, RecordType};
use allocdb_core::wal_file::{WalFile, WalFileError};
use log::{error, trace, warn};

use crate::bounded_queue::{BoundedQueue, BoundedQueueError};

#[path = "engine_checkpoint.rs"]
mod checkpoint;
#[cfg(test)]
#[path = "engine_checkpoint_tests.rs"]
mod checkpoint_tests;
#[cfg(test)]
#[path = "engine_issue_31_tests.rs"]
mod issue_31_tests;
#[path = "engine_observe.rs"]
mod observe;
#[cfg(test)]
#[path = "engine_tests.rs"]
mod tests;
pub use checkpoint::{CheckpointError, CheckpointResult};
pub use observe::{EngineMetrics, ReadError, RecoveryStartupKind, RecoveryStatus};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EngineConfig {
    pub max_submission_queue: u32,
    pub max_command_bytes: usize,
    pub max_expirations_per_tick: u32,
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

        if self.max_expirations_per_tick == 0 {
            return Err(EngineConfigError::ZeroCapacity("max_expirations_per_tick"));
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
pub struct ExpirationTickResult {
    pub processed_count: u32,
    pub last_applied_lsn: Option<Lsn>,
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
    NextLsnExhausted { last_applied_lsn: Lsn },
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
    SlotOverflow(SlotOverflowError),
    CommandTooLarge {
        encoded_len: usize,
        max_command_bytes: usize,
    },
    LsnExhausted {
        last_applied_lsn: Lsn,
    },
    Overloaded {
        queue_depth: u32,
        queue_capacity: u32,
    },
    WalFile(WalFileError),
    CrashInjected(CrashPlan),
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
            Self::EngineHalted | Self::WalFile(_) | Self::CrashInjected(_) => {
                SubmissionErrorCategory::Indefinite
            }
            Self::InvalidRequest(_)
            | Self::SlotOverflow(_)
            | Self::CommandTooLarge { .. }
            | Self::LsnExhausted { .. }
            | Self::Overloaded { .. } => SubmissionErrorCategory::DefiniteFailure,
        }
    }
}

#[derive(Debug)]
pub enum RecoverEngineError {
    Recovery(RecoveryError),
    EngineOpen(EngineOpenError),
    CrashInjected(CrashPlan),
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
struct StartupRecovery {
    loaded_snapshot: bool,
    loaded_snapshot_lsn: Option<Lsn>,
    replayed_wal_frame_count: u32,
    replayed_wal_last_lsn: Option<Lsn>,
}

impl StartupRecovery {
    const fn fresh_start() -> Self {
        Self {
            loaded_snapshot: false,
            loaded_snapshot_lsn: None,
            replayed_wal_frame_count: 0,
            replayed_wal_last_lsn: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PersistFailurePhase {
    BeforeAppend,
    AfterAppend,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum CrashPoint {
    ClientBeforeWalAppend,
    ClientAfterWalSync,
    ClientAfterApply,
    InternalBeforeWalAppend,
    InternalAfterWalSync,
    InternalAfterApply,
    CheckpointAfterSnapshotWrite,
    CheckpointAfterWalRewrite,
    RecoveryAfterSnapshotLoad,
    RecoveryAfterWalTruncate,
    RecoveryAfterReplayFrame,
}

impl CrashPoint {
    #[cfg(test)]
    const fn stable_id(self) -> u8 {
        match self {
            Self::ClientBeforeWalAppend => 1,
            Self::ClientAfterWalSync => 2,
            Self::ClientAfterApply => 3,
            Self::InternalBeforeWalAppend => 4,
            Self::InternalAfterWalSync => 5,
            Self::InternalAfterApply => 6,
            Self::CheckpointAfterSnapshotWrite => 7,
            Self::CheckpointAfterWalRewrite => 8,
            Self::RecoveryAfterSnapshotLoad => 9,
            Self::RecoveryAfterWalTruncate => 10,
            Self::RecoveryAfterReplayFrame => 11,
        }
    }

    #[cfg(test)]
    pub(crate) const fn is_recovery_boundary(self) -> bool {
        matches!(
            self,
            Self::RecoveryAfterSnapshotLoad
                | Self::RecoveryAfterWalTruncate
                | Self::RecoveryAfterReplayFrame
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CrashPlan {
    pub seed: u64,
    pub point: CrashPoint,
}

impl CrashPlan {
    #[cfg(test)]
    pub(crate) fn from_seed(seed: u64, enabled_points: &[CrashPoint]) -> Self {
        assert!(
            !enabled_points.is_empty(),
            "crash plan requires at least one enabled point"
        );

        let mut points = enabled_points.to_vec();
        points.sort_by_key(|point| point.stable_id());
        points.dedup();

        let mixed = mix_seed(seed);
        let point = points[usize::try_from(mixed % u64::try_from(points.len()).unwrap())
            .expect("crash-plan index must fit usize")];

        Self { seed, point }
    }

    fn matches_recovery_boundary(self, boundary: RecoveryBoundary) -> bool {
        match boundary {
            RecoveryBoundary::AfterSnapshotLoad => {
                self.point == CrashPoint::RecoveryAfterSnapshotLoad
            }
            RecoveryBoundary::AfterWalTruncate => {
                self.point == CrashPoint::RecoveryAfterWalTruncate
            }
            RecoveryBoundary::AfterReplayFrame {
                replay_ordinal,
                replayable_frame_count,
                ..
            } => {
                self.point == CrashPoint::RecoveryAfterReplayFrame
                    && replay_ordinal == self.recovery_replay_ordinal(replayable_frame_count)
            }
        }
    }

    fn recovery_replay_ordinal(self, replayable_frame_count: u32) -> u32 {
        assert!(
            replayable_frame_count > 0,
            "replayable recovery frame count must be non-zero"
        );

        let replayable_frame_count = u64::from(replayable_frame_count);
        let selected = mix_seed(self.seed ^ 0xA5A5_A5A5_A5A5_A5A5) % replayable_frame_count;
        u32::try_from(selected).expect("selected recovery replay ordinal must fit u32") + 1
    }

    #[cfg(test)]
    pub(crate) fn selected_recovery_replay_ordinal(self, replayable_frame_count: u32) -> u32 {
        self.recovery_replay_ordinal(replayable_frame_count)
    }
}

const fn mix_seed(seed: u64) -> u64 {
    let state = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mixed = (state ^ (state >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    let mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    mixed ^ (mixed >> 31)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ProcessedSubmission {
    operation_id: OperationId,
    command_fingerprint: u128,
    result: SubmissionResult,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct DueExpiration {
    pub(crate) deadline_slot: Slot,
    pub(crate) reservation_id: ReservationId,
}

#[derive(Debug)]
pub struct SingleNodeEngine {
    db: AllocDb,
    wal: WalFile,
    queue: BoundedQueue<PendingSubmission>,
    config: EngineConfig,
    next_lsn: u64,
    accepting_writes: bool,
    exhausted_lsn: Option<Lsn>,
    active_snapshot_lsn: Option<Lsn>,
    startup_recovery: StartupRecovery,
    // One-shot failure injection used only to exercise ambiguous WAL outcomes in tests.
    injected_persist_failure: Option<PersistFailurePhase>,
    armed_crash: Option<CrashPlan>,
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
        Self::from_parts(
            db,
            engine_config,
            wal_path,
            None,
            StartupRecovery::fresh_start(),
        )
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
        Self::recover_with_crash_plan(core_config, engine_config, snapshot_path, wal_path, None)
    }

    pub(crate) fn recover_with_crash_plan(
        core_config: Config,
        engine_config: EngineConfig,
        snapshot_path: impl AsRef<Path>,
        wal_path: impl AsRef<Path>,
        crash_plan: Option<CrashPlan>,
    ) -> Result<Self, RecoverEngineError> {
        engine_config.validate().map_err(EngineOpenError::from)?;
        let snapshot_file = SnapshotFile::new(snapshot_path);
        let wal = WalFile::open(wal_path.as_ref(), engine_config.max_command_bytes)
            .map_err(EngineOpenError::from)?;
        let mut pending_crash = crash_plan;
        let recovered = recover_allocdb_with_observer(core_config, &snapshot_file, &wal, |point| {
            if pending_crash.is_some_and(|plan| plan.matches_recovery_boundary(point)) {
                let plan = pending_crash
                    .take()
                    .expect("matched recovery crash plan must still be armed");
                warn!(
                    "halting engine recovery on injected crash: seed={} point={:?} boundary={:?}",
                    plan.seed, plan.point, point,
                );
                return Err(plan);
            }

            Ok(())
        })
        .map_err(|error| match error {
            RecoveryObserverError::Recovery(error) => RecoverEngineError::Recovery(error),
            RecoveryObserverError::Observer(plan) => RecoverEngineError::CrashInjected(plan),
        })?;
        Self::from_parts(
            recovered.db,
            engine_config,
            wal_path,
            recovered.loaded_snapshot_lsn,
            StartupRecovery {
                loaded_snapshot: recovered.loaded_snapshot,
                loaded_snapshot_lsn: recovered.loaded_snapshot_lsn,
                replayed_wal_frame_count: recovered.replayed_wal_frame_count,
                replayed_wal_last_lsn: recovered.replayed_wal_last_lsn,
            },
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
    fn from_parts(
        db: AllocDb,
        engine_config: EngineConfig,
        wal_path: impl AsRef<Path>,
        active_snapshot_lsn: Option<Lsn>,
        startup_recovery: StartupRecovery,
    ) -> Result<Self, EngineOpenError> {
        engine_config.validate()?;
        let wal = WalFile::open(wal_path, engine_config.max_command_bytes)?;
        let next_lsn = match db.last_applied_lsn() {
            None => 1,
            Some(last_applied_lsn) => last_applied_lsn
                .get()
                .checked_add(1)
                .ok_or(EngineOpenError::NextLsnExhausted { last_applied_lsn })?,
        };

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
            exhausted_lsn: None,
            active_snapshot_lsn,
            startup_recovery,
            injected_persist_failure: None,
            armed_crash: None,
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

    /// Drains already-queued client submissions, then commits up to one bounded batch of due
    /// expiration commands for the provided logical slot.
    ///
    /// # Errors
    ///
    /// Returns [`SubmissionError`] if the engine is halted or if any WAL append/sync fails.
    pub fn tick_expirations(
        &mut self,
        current_wall_clock_slot: Slot,
    ) -> Result<ExpirationTickResult, SubmissionError> {
        if !self.accepting_writes {
            return Err(SubmissionError::EngineHalted);
        }
        if self.queue.len() == 0 {
            if let Some(error) = self.lsn_exhaustion_error() {
                return Err(error);
            }
        }

        self.process_queued_submissions()?;
        if let Some(error) = self.lsn_exhaustion_error() {
            return Err(error);
        }

        let due = self.collect_due_expirations(current_wall_clock_slot);
        let expiration_request_slot = self.expiration_request_slot(current_wall_clock_slot);
        let mut remaining_due = self.config.max_expirations_per_tick;
        let mut processed_count = 0_u32;
        let mut last_applied_lsn = None;
        for target in due {
            if remaining_due == 0 {
                break;
            }
            remaining_due -= 1;
            let result = self.apply_internal_command(
                expiration_request_slot,
                Command::Expire {
                    reservation_id: target.reservation_id,
                    deadline_slot: target.deadline_slot,
                },
            )?;
            processed_count = processed_count.saturating_add(1);
            last_applied_lsn = Some(result.applied_lsn);
        }

        Ok(ExpirationTickResult {
            processed_count,
            last_applied_lsn,
        })
    }

    fn process_queued_submissions(&mut self) -> Result<(), SubmissionError> {
        let mut drained_count = 0_u32;
        while self.process_one()?.is_some() {
            drained_count = drained_count.saturating_add(1);
        }
        if drained_count > 0 {
            trace!("drained queued submissions before expiration tick: count={drained_count}");
        }
        Ok(())
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

        if let Some(error) = self.lsn_exhaustion_error() {
            return Err(error);
        }

        self.validate_client_request_slot(request_slot, request.command)?;

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
        if let Some(error) = self.lsn_exhaustion_error() {
            return Err(error);
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
        if let Some(plan) = self.maybe_inject_crash(CrashPoint::ClientBeforeWalAppend) {
            return Err(SubmissionError::CrashInjected(plan));
        }
        if injected_failure == Some(PersistFailurePhase::BeforeAppend) {
            return Err(self.halt_on_wal_error(
                operation_id,
                pending.request_slot,
                applied_lsn,
                "before_append",
                std::io::Error::other("injected WAL failure before append"),
            ));
        }

        if let Err(error) = self.wal.append_frame(&frame) {
            return Err(self.halt_on_wal_error(
                operation_id,
                pending.request_slot,
                applied_lsn,
                "append",
                error,
            ));
        }

        if injected_failure == Some(PersistFailurePhase::AfterAppend) {
            return Err(self.halt_on_wal_error(
                operation_id,
                pending.request_slot,
                applied_lsn,
                "after_append_before_sync",
                std::io::Error::other("injected WAL failure after append"),
            ));
        }

        if let Err(error) = self.wal.sync() {
            return Err(self.halt_on_wal_error(
                operation_id,
                pending.request_slot,
                applied_lsn,
                "sync",
                error,
            ));
        }

        if let Some(plan) = self.maybe_inject_crash(CrashPoint::ClientAfterWalSync) {
            return Err(SubmissionError::CrashInjected(plan));
        }

        let outcome = self.db.apply_client(
            CommandContext {
                lsn: applied_lsn,
                request_slot: pending.request_slot,
            },
            pending.request,
        );
        if let Some(plan) = self.maybe_inject_crash(CrashPoint::ClientAfterApply) {
            return Err(SubmissionError::CrashInjected(plan));
        }
        self.advance_next_lsn(applied_lsn);

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

    pub(crate) fn collect_due_expirations(
        &self,
        current_wall_clock_slot: Slot,
    ) -> Vec<DueExpiration> {
        // Scan the full wheel so delayed ticks catch up on overdue expirations. The caller decides
        // how many due expirations to commit once the candidate set is known.
        let mut due = Vec::new();
        for bucket in 0..self.db.config().wheel_len() {
            let bucket_slot = Slot(u64::try_from(bucket).expect("wheel bucket index must fit u64"));
            for reservation_id in self.db.due_reservations(bucket_slot) {
                let Ok(record) = self
                    .db
                    .reservation(*reservation_id, current_wall_clock_slot)
                else {
                    continue;
                };

                if record.state == ReservationState::Reserved
                    && record.deadline_slot.get() <= current_wall_clock_slot.get()
                {
                    due.push(DueExpiration {
                        deadline_slot: record.deadline_slot,
                        reservation_id: *reservation_id,
                    });
                }
            }
        }

        due.sort_unstable();
        due
    }

    pub(crate) fn expiration_request_slot(&self, current_wall_clock_slot: Slot) -> Slot {
        self.db
            .last_request_slot()
            .map_or(current_wall_clock_slot, |last_request_slot| {
                Slot(current_wall_clock_slot.get().max(last_request_slot.get()))
            })
    }

    #[cfg(test)]
    pub(crate) const fn max_expirations_per_tick(&self) -> u32 {
        self.config.max_expirations_per_tick
    }

    #[cfg(test)]
    pub(crate) fn apply_due_expiration(
        &mut self,
        request_slot: Slot,
        target: DueExpiration,
    ) -> Result<SubmissionResult, SubmissionError> {
        self.apply_internal_command(
            request_slot,
            Command::Expire {
                reservation_id: target.reservation_id,
                deadline_slot: target.deadline_slot,
            },
        )
    }

    fn apply_internal_command(
        &mut self,
        request_slot: Slot,
        command: Command,
    ) -> Result<SubmissionResult, SubmissionError> {
        if !self.accepting_writes {
            return Err(SubmissionError::EngineHalted);
        }
        if let Some(error) = self.lsn_exhaustion_error() {
            return Err(error);
        }

        self.validate_internal_request_slot(request_slot, command)?;

        let applied_lsn = Lsn(self.next_lsn);
        let injected_failure = self.take_injected_persist_failure();
        if let Some(plan) = self.maybe_inject_crash(CrashPoint::InternalBeforeWalAppend) {
            return Err(SubmissionError::CrashInjected(plan));
        }
        if injected_failure == Some(PersistFailurePhase::BeforeAppend) {
            return Err(self.halt_on_internal_wal_error(
                request_slot,
                applied_lsn,
                "before_append",
                std::io::Error::other("injected WAL failure before append"),
            ));
        }

        let frame = Frame {
            lsn: applied_lsn,
            request_slot,
            record_type: RecordType::InternalCommand,
            payload: encode_internal_command(command),
        };

        if let Err(error) = self.wal.append_frame(&frame) {
            return Err(self.halt_on_internal_wal_error(
                request_slot,
                applied_lsn,
                "append",
                error,
            ));
        }

        if injected_failure == Some(PersistFailurePhase::AfterAppend) {
            return Err(self.halt_on_internal_wal_error(
                request_slot,
                applied_lsn,
                "after_append_before_sync",
                std::io::Error::other("injected WAL failure after append"),
            ));
        }

        if let Err(error) = self.wal.sync() {
            return Err(self.halt_on_internal_wal_error(request_slot, applied_lsn, "sync", error));
        }

        if let Some(plan) = self.maybe_inject_crash(CrashPoint::InternalAfterWalSync) {
            return Err(SubmissionError::CrashInjected(plan));
        }

        let outcome = self.db.apply_internal(
            CommandContext {
                lsn: applied_lsn,
                request_slot,
            },
            command,
        );
        if let Some(plan) = self.maybe_inject_crash(CrashPoint::InternalAfterApply) {
            return Err(SubmissionError::CrashInjected(plan));
        }
        self.advance_next_lsn(applied_lsn);

        Ok(SubmissionResult {
            applied_lsn,
            outcome,
            from_retry_cache: false,
        })
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

    fn validate_client_request_slot(
        &self,
        request_slot: Slot,
        command: Command,
    ) -> Result<(), SubmissionError> {
        self.db
            .validate_client_request_slot(request_slot, command)
            .map_err(SubmissionError::SlotOverflow)
    }

    fn validate_internal_request_slot(
        &self,
        request_slot: Slot,
        command: Command,
    ) -> Result<(), SubmissionError> {
        self.db
            .validate_internal_request_slot(request_slot, command)
            .map_err(SubmissionError::SlotOverflow)
    }

    fn lsn_exhaustion_error(&self) -> Option<SubmissionError> {
        self.exhausted_lsn
            .map(|last_applied_lsn| SubmissionError::LsnExhausted { last_applied_lsn })
    }

    fn writes_available(&self) -> bool {
        self.accepting_writes && self.exhausted_lsn.is_none()
    }

    fn advance_next_lsn(&mut self, applied_lsn: Lsn) {
        if let Some(next_lsn) = applied_lsn.get().checked_add(1) {
            self.next_lsn = next_lsn;
            return;
        }

        error!(
            "engine exhausted lsn space, future writes disabled: applied_lsn={}",
            applied_lsn.get(),
        );
        self.exhausted_lsn = Some(applied_lsn);
    }

    fn halt_on_wal_error(
        &mut self,
        operation_id: OperationId,
        request_slot: Slot,
        applied_lsn: Lsn,
        phase: &'static str,
        error: impl Into<WalFileError>,
    ) -> SubmissionError {
        let error = error.into();
        error!(
            "halting engine on WAL error, accepting_writes set to false: operation_id={} request_slot={} applied_lsn={} phase={} error={error:?}",
            operation_id.get(),
            request_slot.get(),
            applied_lsn.get(),
            phase,
        );
        self.accepting_writes = false;
        SubmissionError::WalFile(error)
    }

    fn halt_on_internal_wal_error(
        &mut self,
        request_slot: Slot,
        applied_lsn: Lsn,
        phase: &'static str,
        error: impl Into<WalFileError>,
    ) -> SubmissionError {
        let error = error.into();
        error!(
            "halting engine on internal WAL error, accepting_writes set to false: request_slot={} applied_lsn={} phase={} error={error:?}",
            request_slot.get(),
            applied_lsn.get(),
            phase,
        );
        self.accepting_writes = false;
        SubmissionError::WalFile(error)
    }

    fn take_injected_persist_failure(&mut self) -> Option<PersistFailurePhase> {
        self.injected_persist_failure.take()
    }

    #[cfg(test)]
    pub(crate) fn inject_next_persist_failure(&mut self, phase: PersistFailurePhase) {
        self.injected_persist_failure = Some(phase);
    }

    #[cfg(test)]
    pub(crate) fn arm_next_crash(&mut self, plan: CrashPlan) {
        assert!(
            !plan.point.is_recovery_boundary(),
            "runtime engine crash plan must not target recovery boundaries"
        );
        self.armed_crash = Some(plan);
    }

    fn maybe_inject_crash(&mut self, point: CrashPoint) -> Option<CrashPlan> {
        if self.armed_crash.is_some_and(|plan| plan.point == point) {
            let plan = self
                .armed_crash
                .take()
                .expect("matched crash plan must still be armed");
            warn!(
                "halting engine on injected crash: seed={} point={:?}",
                plan.seed, plan.point
            );
            self.accepting_writes = false;
            return Some(plan);
        }

        None
    }
}

use std::collections::BTreeMap;
use std::fmt;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use allocdb_core::ids::{Lsn, ResourceId, Slot};

use crate::replica::{ReplicaId, ReplicaRole};

const JEPSEN_HISTORY_VERSION: u32 = 1;
const JEPSEN_MANIFEST_VERSION: u32 = 1;

#[cfg(test)]
#[path = "jepsen_tests.rs"]
mod tests;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum JepsenWorkloadFamily {
    ReservationContention,
    AmbiguousWriteRetry,
    FailoverReadFences,
    ExpirationAndRecovery,
}

impl JepsenWorkloadFamily {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReservationContention => "reservation_contention",
            Self::AmbiguousWriteRetry => "ambiguous_write_retry",
            Self::FailoverReadFences => "failover_read_fences",
            Self::ExpirationAndRecovery => "expiration_and_recovery",
        }
    }

    /// Parses one workload-family name from persisted Jepsen text.
    ///
    /// # Errors
    ///
    /// Returns [`JepsenCodecError::InvalidField`] when `value` does not name one supported
    /// workload family.
    pub fn parse(value: &str) -> Result<Self, JepsenCodecError> {
        match value {
            "reservation_contention" => Ok(Self::ReservationContention),
            "ambiguous_write_retry" => Ok(Self::AmbiguousWriteRetry),
            "failover_read_fences" => Ok(Self::FailoverReadFences),
            "expiration_and_recovery" => Ok(Self::ExpirationAndRecovery),
            other => Err(JepsenCodecError::InvalidField {
                field: String::from("workload"),
                value: String::from(other),
            }),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum JepsenNemesisFamily {
    None,
    CrashRestart,
    PartitionHeal,
    MixedFailover,
}

impl JepsenNemesisFamily {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::CrashRestart => "crash_restart",
            Self::PartitionHeal => "partition_heal",
            Self::MixedFailover => "mixed_failover",
        }
    }

    /// Parses one nemesis-family name from persisted Jepsen text.
    ///
    /// # Errors
    ///
    /// Returns [`JepsenCodecError::InvalidField`] when `value` does not name one supported
    /// nemesis family.
    pub fn parse(value: &str) -> Result<Self, JepsenCodecError> {
        match value {
            "none" => Ok(Self::None),
            "crash_restart" => Ok(Self::CrashRestart),
            "partition_heal" => Ok(Self::PartitionHeal),
            "mixed_failover" => Ok(Self::MixedFailover),
            other => Err(JepsenCodecError::InvalidField {
                field: String::from("nemesis"),
                value: String::from(other),
            }),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JepsenRunSpec {
    pub run_id: String,
    pub workload: JepsenWorkloadFamily,
    pub nemesis: JepsenNemesisFamily,
    pub minimum_fault_window_secs: Option<u64>,
    pub release_blocking: bool,
}

#[must_use]
pub fn release_gate_plan() -> Vec<JepsenRunSpec> {
    let workloads = [
        JepsenWorkloadFamily::ReservationContention,
        JepsenWorkloadFamily::AmbiguousWriteRetry,
        JepsenWorkloadFamily::FailoverReadFences,
        JepsenWorkloadFamily::ExpirationAndRecovery,
    ];
    let mut runs = Vec::new();
    for workload in workloads {
        runs.push(JepsenRunSpec {
            run_id: format!("{}-control", workload.as_str()),
            workload,
            nemesis: JepsenNemesisFamily::None,
            minimum_fault_window_secs: None,
            release_blocking: true,
        });
        runs.push(JepsenRunSpec {
            run_id: format!("{}-crash-restart", workload.as_str()),
            workload,
            nemesis: JepsenNemesisFamily::CrashRestart,
            minimum_fault_window_secs: Some(30 * 60),
            release_blocking: true,
        });
        runs.push(JepsenRunSpec {
            run_id: format!("{}-partition-heal", workload.as_str()),
            workload,
            nemesis: JepsenNemesisFamily::PartitionHeal,
            minimum_fault_window_secs: Some(30 * 60),
            release_blocking: true,
        });
    }
    for workload in [
        JepsenWorkloadFamily::AmbiguousWriteRetry,
        JepsenWorkloadFamily::FailoverReadFences,
        JepsenWorkloadFamily::ExpirationAndRecovery,
    ] {
        runs.push(JepsenRunSpec {
            run_id: format!("{}-mixed-failover", workload.as_str()),
            workload,
            nemesis: JepsenNemesisFamily::MixedFailover,
            minimum_fault_window_secs: Some(30 * 60),
            release_blocking: true,
        });
    }
    runs
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JepsenOperationKind {
    Reserve,
    Confirm,
    Release,
    TickExpirations,
    GetResource,
    GetReservation,
}

impl JepsenOperationKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Reserve => "reserve",
            Self::Confirm => "confirm",
            Self::Release => "release",
            Self::TickExpirations => "tick_expirations",
            Self::GetResource => "get_resource",
            Self::GetReservation => "get_reservation",
        }
    }

    /// Parses one operation kind from persisted Jepsen text.
    ///
    /// # Errors
    ///
    /// Returns [`JepsenCodecError::InvalidField`] when `value` is not one supported operation
    /// name.
    pub fn parse(value: &str) -> Result<Self, JepsenCodecError> {
        match value {
            "reserve" => Ok(Self::Reserve),
            "confirm" => Ok(Self::Confirm),
            "release" => Ok(Self::Release),
            "tick_expirations" => Ok(Self::TickExpirations),
            "get_resource" => Ok(Self::GetResource),
            "get_reservation" => Ok(Self::GetReservation),
            other => Err(JepsenCodecError::InvalidField {
                field: String::from("operation"),
                value: String::from(other),
            }),
        }
    }

    #[must_use]
    pub const fn is_mutating(self) -> bool {
        matches!(
            self,
            Self::Reserve | Self::Confirm | Self::Release | Self::TickExpirations
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JepsenOperation {
    pub kind: JepsenOperationKind,
    pub operation_id: Option<u128>,
    pub resource_id: Option<ResourceId>,
    pub reservation_id: Option<u128>,
    pub holder_id: Option<u128>,
    pub required_lsn: Option<Lsn>,
    pub request_slot: Option<Slot>,
    pub ttl_slots: Option<u64>,
}

impl JepsenOperation {
    #[must_use]
    pub const fn is_mutating(&self) -> bool {
        self.kind.is_mutating()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JepsenOutcomeKind {
    CommittedWrite,
    ReadOk,
    DefiniteFailure,
    Ambiguous,
}

impl JepsenOutcomeKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CommittedWrite => "committed_write",
            Self::ReadOk => "read_ok",
            Self::DefiniteFailure => "definite_failure",
            Self::Ambiguous => "ambiguous",
        }
    }

    /// Parses one event-outcome kind from persisted Jepsen text.
    ///
    /// # Errors
    ///
    /// Returns [`JepsenCodecError::InvalidField`] when `value` is not one supported outcome tag.
    pub fn parse(value: &str) -> Result<Self, JepsenCodecError> {
        match value {
            "committed_write" => Ok(Self::CommittedWrite),
            "read_ok" => Ok(Self::ReadOk),
            "definite_failure" => Ok(Self::DefiniteFailure),
            "ambiguous" => Ok(Self::Ambiguous),
            other => Err(JepsenCodecError::InvalidField {
                field: String::from("outcome"),
                value: String::from(other),
            }),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JepsenWriteResult {
    Reserved {
        resource_id: ResourceId,
        holder_id: u128,
        reservation_id: u128,
        expires_at_slot: Slot,
    },
    Confirmed {
        resource_id: ResourceId,
        holder_id: u128,
        reservation_id: u128,
    },
    Released {
        resource_id: ResourceId,
        holder_id: u128,
        reservation_id: u128,
        released_lsn: Option<Lsn>,
    },
    TickExpired {
        expired: Vec<JepsenExpiredReservation>,
    },
}

impl JepsenWriteResult {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Reserved { .. } => "reserved",
            Self::Confirmed { .. } => "confirmed",
            Self::Released { .. } => "released",
            Self::TickExpired { .. } => "tick_expired",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JepsenExpiredReservation {
    pub resource_id: ResourceId,
    pub holder_id: u128,
    pub reservation_id: u128,
    pub released_lsn: Option<Lsn>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JepsenCommittedWrite {
    pub applied_lsn: Lsn,
    pub result: JepsenWriteResult,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JepsenReadTarget {
    Resource,
    Reservation,
}

impl JepsenReadTarget {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Resource => "resource",
            Self::Reservation => "reservation",
        }
    }

    /// Parses one read target from persisted Jepsen text.
    ///
    /// # Errors
    ///
    /// Returns [`JepsenCodecError::InvalidField`] when `value` is not one supported read target.
    pub fn parse(value: &str) -> Result<Self, JepsenCodecError> {
        match value {
            "resource" => Ok(Self::Resource),
            "reservation" => Ok(Self::Reservation),
            other => Err(JepsenCodecError::InvalidField {
                field: String::from("read_target"),
                value: String::from(other),
            }),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JepsenResourceState {
    Available,
    Reserved {
        holder_id: u128,
        reservation_id: u128,
        expires_at_slot: Slot,
    },
    Confirmed {
        holder_id: u128,
        reservation_id: u128,
    },
}

impl JepsenResourceState {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::Reserved { .. } => "reserved",
            Self::Confirmed { .. } => "confirmed",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JepsenReservationState {
    NotFound,
    Retired,
    Active {
        resource_id: ResourceId,
        holder_id: u128,
        expires_at_slot: Slot,
        confirmed: bool,
    },
    Released {
        resource_id: ResourceId,
        holder_id: u128,
        released_lsn: Option<Lsn>,
    },
}

impl JepsenReservationState {
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::NotFound => "not_found",
            Self::Retired => "retired",
            Self::Active { .. } => "active",
            Self::Released { .. } => "released",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JepsenReadState {
    Resource(JepsenResourceState),
    Reservation(JepsenReservationState),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JepsenSuccessfulRead {
    pub target: JepsenReadTarget,
    pub served_by: ReplicaId,
    pub served_role: ReplicaRole,
    pub observed_lsn: Option<Lsn>,
    pub state: JepsenReadState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JepsenDefiniteFailure {
    Busy,
    Conflict,
    NotFound,
    Retired,
    FenceNotApplied,
    NotPrimary,
    EngineHalted,
    InvalidRequest,
}

impl JepsenDefiniteFailure {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Busy => "busy",
            Self::Conflict => "conflict",
            Self::NotFound => "not_found",
            Self::Retired => "retired",
            Self::FenceNotApplied => "fence_not_applied",
            Self::NotPrimary => "not_primary",
            Self::EngineHalted => "engine_halted",
            Self::InvalidRequest => "invalid_request",
        }
    }

    /// Parses one definite failure reason from persisted Jepsen text.
    ///
    /// # Errors
    ///
    /// Returns [`JepsenCodecError::InvalidField`] when `value` is not one supported definite
    /// failure reason.
    pub fn parse(value: &str) -> Result<Self, JepsenCodecError> {
        match value {
            "busy" => Ok(Self::Busy),
            "conflict" => Ok(Self::Conflict),
            "not_found" => Ok(Self::NotFound),
            "retired" => Ok(Self::Retired),
            "fence_not_applied" => Ok(Self::FenceNotApplied),
            "not_primary" => Ok(Self::NotPrimary),
            "engine_halted" => Ok(Self::EngineHalted),
            "invalid_request" => Ok(Self::InvalidRequest),
            other => Err(JepsenCodecError::InvalidField {
                field: String::from("reason"),
                value: String::from(other),
            }),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JepsenAmbiguousOutcome {
    Timeout,
    TransportLoss,
    IndefiniteWrite,
}

impl JepsenAmbiguousOutcome {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::TransportLoss => "transport_loss",
            Self::IndefiniteWrite => "indefinite_write",
        }
    }

    /// Parses one ambiguous client outcome from persisted Jepsen text.
    ///
    /// # Errors
    ///
    /// Returns [`JepsenCodecError::InvalidField`] when `value` is not one supported ambiguous
    /// outcome reason.
    pub fn parse(value: &str) -> Result<Self, JepsenCodecError> {
        match value {
            "timeout" => Ok(Self::Timeout),
            "transport_loss" => Ok(Self::TransportLoss),
            "indefinite_write" => Ok(Self::IndefiniteWrite),
            other => Err(JepsenCodecError::InvalidField {
                field: String::from("reason"),
                value: String::from(other),
            }),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JepsenEventOutcome {
    CommittedWrite(JepsenCommittedWrite),
    SuccessfulRead(JepsenSuccessfulRead),
    DefiniteFailure(JepsenDefiniteFailure),
    Ambiguous(JepsenAmbiguousOutcome),
}

impl JepsenEventOutcome {
    #[must_use]
    pub const fn kind(&self) -> JepsenOutcomeKind {
        match self {
            Self::CommittedWrite(_) => JepsenOutcomeKind::CommittedWrite,
            Self::SuccessfulRead(_) => JepsenOutcomeKind::ReadOk,
            Self::DefiniteFailure(_) => JepsenOutcomeKind::DefiniteFailure,
            Self::Ambiguous(_) => JepsenOutcomeKind::Ambiguous,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JepsenHistoryEvent {
    pub sequence: u64,
    pub process: String,
    pub time_millis: u128,
    pub operation: JepsenOperation,
    pub outcome: JepsenEventOutcome,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JepsenLogicalCommandSummary {
    pub operation_id: u128,
    pub attempts: usize,
    pub ambiguous_attempts: usize,
    pub definite_failures: Vec<JepsenDefiniteFailure>,
    pub committed_result: Option<CommittedFingerprint>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JepsenReadViolation {
    pub replica_id: ReplicaId,
    pub served_role: ReplicaRole,
    pub required_lsn: Option<Lsn>,
    pub observed_lsn: Option<Lsn>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JepsenBlockingIssue {
    MissingOperationId {
        sequence: u64,
        kind: JepsenOperationKind,
    },
    DuplicateCommittedExecution {
        operation_id: u128,
        first_lsn: Lsn,
        second_lsn: Lsn,
    },
    UnresolvedAmbiguity {
        operation_id: u128,
    },
    DoubleAllocation {
        resource_id: ResourceId,
        existing_operation_id: u128,
        conflicting_operation_id: u128,
    },
    StaleSuccessfulRead(JepsenReadViolation),
    EarlyExpirationRelease {
        resource_id: ResourceId,
        reservation_id: u128,
        expires_at_slot: Slot,
        released_at_slot: Slot,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JepsenAnalysisReport {
    pub logical_commands: Vec<JepsenLogicalCommandSummary>,
    pub blockers: Vec<JepsenBlockingIssue>,
}

impl JepsenAnalysisReport {
    #[must_use]
    pub fn release_gate_passed(&self) -> bool {
        self.blockers.is_empty()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JepsenArtifactManifest {
    pub run_id: String,
    pub workload: JepsenWorkloadFamily,
    pub nemesis: JepsenNemesisFamily,
    pub created_at_millis: u128,
    pub history_file: String,
    pub analysis_file: String,
    pub qemu_logs_archive: Option<String>,
    pub blocker_count: usize,
}

#[derive(Debug)]
pub enum JepsenCodecError {
    Io(std::io::Error),
    MissingField(String),
    InvalidField { field: String, value: String },
    InvalidLine(String),
    UnsupportedVersion(u32),
}

impl From<std::io::Error> for JepsenCodecError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl fmt::Display for JepsenCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "i/o error: {error}"),
            Self::MissingField(field) => write!(formatter, "missing field `{field}`"),
            Self::InvalidField { field, value } => {
                write!(formatter, "invalid value for `{field}`: `{value}`")
            }
            Self::InvalidLine(line) => write!(formatter, "invalid line `{line}`"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported Jepsen codec version `{version}`")
            }
        }
    }
}

impl std::error::Error for JepsenCodecError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommittedFingerprint {
    pub applied_lsn: Lsn,
    pub resource_id: Option<ResourceId>,
    pub reservation_id: Option<u128>,
    pub holder_id: Option<u128>,
    pub released_lsn: Option<Lsn>,
    pub expires_at_slot: Option<Slot>,
}

#[must_use]
pub fn encode_history(events: &[JepsenHistoryEvent]) -> String {
    let mut lines = vec![format!("version={JEPSEN_HISTORY_VERSION}")];
    for event in events {
        lines.push(encode_history_event(event));
    }
    lines.push(String::new());
    lines.join("\n")
}

/// Decodes one persisted Jepsen history text file.
///
/// # Errors
///
/// Returns [`JepsenCodecError`] when the version header is missing or unsupported, or when one
/// event line is malformed.
pub fn decode_history(input: &str) -> Result<Vec<JepsenHistoryEvent>, JepsenCodecError> {
    let mut lines = input.lines().filter(|line| !line.trim().is_empty());
    let Some(version_line) = lines.next() else {
        return Ok(Vec::new());
    };
    let version_fields = parse_token_map(version_line)?;
    let version = parse_required_u32(&version_fields, "version")?;
    if version != JEPSEN_HISTORY_VERSION {
        return Err(JepsenCodecError::UnsupportedVersion(version));
    }

    let mut events = Vec::new();
    for line in lines {
        let fields = parse_token_map(line)?;
        events.push(decode_history_event(&fields)?);
    }
    Ok(events)
}

/// Loads one persisted Jepsen history from disk.
///
/// # Errors
///
/// Returns [`JepsenCodecError`] if the file cannot be read or if its contents are malformed.
pub fn load_history(path: impl AsRef<Path>) -> Result<Vec<JepsenHistoryEvent>, JepsenCodecError> {
    let mut bytes = String::new();
    File::open(path)?.read_to_string(&mut bytes)?;
    decode_history(&bytes)
}

/// Persists one Jepsen history atomically to disk.
///
/// # Errors
///
/// Returns [`JepsenCodecError`] if the destination cannot be created or if the atomic replace
/// fails.
pub fn persist_history(
    path: impl AsRef<Path>,
    events: &[JepsenHistoryEvent],
) -> Result<(), JepsenCodecError> {
    write_bytes_atomically(path.as_ref(), encode_history(events).as_bytes())
}

#[must_use]
pub fn encode_artifact_manifest(manifest: &JepsenArtifactManifest) -> String {
    let lines = vec![
        format!("version={JEPSEN_MANIFEST_VERSION}"),
        format!("run_id={}", manifest.run_id),
        format!("workload={}", manifest.workload.as_str()),
        format!("nemesis={}", manifest.nemesis.as_str()),
        format!("created_at_millis={}", manifest.created_at_millis),
        format!("history_file={}", manifest.history_file),
        format!("analysis_file={}", manifest.analysis_file),
        format!(
            "qemu_logs_archive={}",
            manifest.qemu_logs_archive.as_deref().unwrap_or("none")
        ),
        format!("blocker_count={}", manifest.blocker_count),
        String::new(),
    ];
    lines.join("\n")
}

/// Persists one Jepsen artifact manifest atomically to disk.
///
/// # Errors
///
/// Returns [`JepsenCodecError`] if the manifest path cannot be created or replaced.
pub fn persist_artifact_manifest(
    path: impl AsRef<Path>,
    manifest: &JepsenArtifactManifest,
) -> Result<(), JepsenCodecError> {
    write_bytes_atomically(path.as_ref(), encode_artifact_manifest(manifest).as_bytes())
}

/// Analyzes one Jepsen history according to `AllocDB`'s retry-aware release blockers.
///
/// # Errors
///
/// This function does not return one error value; instead it records all release-blocking findings
/// in the returned [`JepsenAnalysisReport`].
#[must_use]
pub fn analyze_history(events: &[JepsenHistoryEvent]) -> JepsenAnalysisReport {
    let (builders, committed, mut blockers) = collect_mutating_attempts(events);
    blockers.extend(check_successful_reads(events));
    blockers.extend(unresolved_ambiguity_blockers(&builders));
    blockers.extend(check_committed_release_blockers(committed));
    JepsenAnalysisReport {
        logical_commands: summarize_logical_commands(builders),
        blockers,
    }
}

#[must_use]
pub fn render_analysis_report(report: &JepsenAnalysisReport) -> String {
    let mut lines = vec![format!(
        "release_gate={}",
        if report.release_gate_passed() {
            "passed"
        } else {
            "blocked"
        }
    )];
    lines.push(format!(
        "logical_commands={}",
        report.logical_commands.len()
    ));
    lines.push(format!("blockers={}", report.blockers.len()));
    for blocker in &report.blockers {
        lines.push(render_blocker(blocker));
    }
    lines.push(String::new());
    lines.join("\n")
}

/// Creates one host-side artifact bundle for one Jepsen run.
///
/// # Errors
///
/// Returns [`JepsenCodecError`] if the bundle directory cannot be created, if the encoded history
/// or manifest cannot be persisted, or if the optional QEMU log archive cannot be copied into the
/// bundle.
pub fn create_artifact_bundle(
    output_root: impl AsRef<Path>,
    run_spec: &JepsenRunSpec,
    history: &[JepsenHistoryEvent],
    report: &JepsenAnalysisReport,
    qemu_logs_archive: Option<&Path>,
) -> Result<PathBuf, JepsenCodecError> {
    let output_root = output_root.as_ref();
    fs::create_dir_all(output_root)?;
    let created_at_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let bundle_dir = output_root.join(format!("{}-{}", run_spec.run_id, created_at_millis));
    fs::create_dir_all(&bundle_dir)?;

    let history_path = bundle_dir.join("history.txt");
    persist_history(&history_path, history)?;
    let analysis_path = bundle_dir.join("analysis.txt");
    write_bytes_atomically(&analysis_path, render_analysis_report(report).as_bytes())?;

    let qemu_logs_target = if let Some(source) = qemu_logs_archive {
        let file_name = source
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("qemu-logs.tar.gz");
        let target = bundle_dir.join(file_name);
        fs::copy(source, &target)?;
        Some(target)
    } else {
        None
    };

    let manifest = JepsenArtifactManifest {
        run_id: run_spec.run_id.clone(),
        workload: run_spec.workload,
        nemesis: run_spec.nemesis,
        created_at_millis,
        history_file: String::from("history.txt"),
        analysis_file: String::from("analysis.txt"),
        qemu_logs_archive: qemu_logs_target
            .as_ref()
            .and_then(|path| path.file_name())
            .and_then(|name| name.to_str())
            .map(String::from),
        blocker_count: report.blockers.len(),
    };
    persist_artifact_manifest(bundle_dir.join("manifest.txt"), &manifest)?;
    Ok(bundle_dir)
}

fn encode_history_event(event: &JepsenHistoryEvent) -> String {
    let mut tokens = vec![
        format!("sequence={}", event.sequence),
        format!("process={}", event.process),
        format!("time_millis={}", event.time_millis),
        format!("operation={}", event.operation.kind.as_str()),
    ];
    if let Some(operation_id) = event.operation.operation_id {
        tokens.push(format!("operation_id={operation_id}"));
    }
    if let Some(resource_id) = event.operation.resource_id {
        tokens.push(format!("resource_id={}", resource_id.get()));
    }
    if let Some(reservation_id) = event.operation.reservation_id {
        tokens.push(format!("reservation_id={reservation_id}"));
    }
    if let Some(holder_id) = event.operation.holder_id {
        tokens.push(format!("holder_id={holder_id}"));
    }
    if let Some(required_lsn) = event.operation.required_lsn {
        tokens.push(format!("required_lsn={}", required_lsn.get()));
    }
    if let Some(request_slot) = event.operation.request_slot {
        tokens.push(format!("request_slot={}", request_slot.get()));
    }
    if let Some(ttl_slots) = event.operation.ttl_slots {
        tokens.push(format!("ttl_slots={ttl_slots}"));
    }

    tokens.push(format!("outcome={}", event.outcome.kind().as_str()));
    match &event.outcome {
        JepsenEventOutcome::CommittedWrite(write) => {
            tokens.push(format!("applied_lsn={}", write.applied_lsn.get()));
            tokens.push(format!("write_result={}", write.result.as_str()));
            match &write.result {
                JepsenWriteResult::Reserved {
                    reservation_id,
                    holder_id,
                    expires_at_slot,
                    ..
                } => {
                    tokens.push(format!("committed_reservation_id={reservation_id}"));
                    tokens.push(format!("committed_holder_id={holder_id}"));
                    tokens.push(format!("expires_at_slot={}", expires_at_slot.get()));
                }
                JepsenWriteResult::Confirmed {
                    reservation_id,
                    holder_id,
                    ..
                } => {
                    tokens.push(format!("committed_reservation_id={reservation_id}"));
                    tokens.push(format!("committed_holder_id={holder_id}"));
                }
                JepsenWriteResult::Released {
                    reservation_id,
                    holder_id,
                    released_lsn,
                    ..
                } => {
                    tokens.push(format!("committed_reservation_id={reservation_id}"));
                    tokens.push(format!("committed_holder_id={holder_id}"));
                    tokens.push(format!(
                        "released_lsn={}",
                        released_lsn.map_or(String::from("none"), |lsn| lsn.get().to_string())
                    ));
                }
                JepsenWriteResult::TickExpired { expired } => {
                    tokens.push(format!("expired={}", encode_expired_reservations(expired)));
                }
            }
        }
        JepsenEventOutcome::SuccessfulRead(read) => {
            tokens.push(format!("read_target={}", read.target.as_str()));
            tokens.push(format!("served_by={}", read.served_by.get()));
            tokens.push(format!("served_role={}", encode_role(read.served_role)));
            tokens.push(format!(
                "observed_lsn={}",
                read.observed_lsn
                    .map_or(String::from("none"), |lsn| lsn.get().to_string())
            ));
            match &read.state {
                JepsenReadState::Resource(state) => {
                    tokens.push(format!("resource_state={}", encode_resource_state(state)));
                }
                JepsenReadState::Reservation(state) => {
                    tokens.push(format!(
                        "reservation_state={}",
                        encode_reservation_state(state)
                    ));
                }
            }
        }
        JepsenEventOutcome::DefiniteFailure(reason) => {
            tokens.push(format!("reason={}", reason.as_str()));
        }
        JepsenEventOutcome::Ambiguous(reason) => {
            tokens.push(format!("reason={}", reason.as_str()));
        }
    }
    tokens.join(" ")
}

fn decode_history_event(
    fields: &BTreeMap<String, String>,
) -> Result<JepsenHistoryEvent, JepsenCodecError> {
    let kind = JepsenOperationKind::parse(required_field(fields, "operation")?)?;
    let operation = JepsenOperation {
        kind,
        operation_id: optional_u128_field(fields, "operation_id")?,
        resource_id: optional_resource_id_field(fields, "resource_id")?,
        reservation_id: optional_u128_field(fields, "reservation_id")?,
        holder_id: optional_u128_field(fields, "holder_id")?,
        required_lsn: optional_lsn_field(fields, "required_lsn")?,
        request_slot: optional_slot_field(fields, "request_slot")?,
        ttl_slots: optional_u64_field(fields, "ttl_slots")?,
    };
    let outcome = match JepsenOutcomeKind::parse(required_field(fields, "outcome")?)? {
        JepsenOutcomeKind::CommittedWrite => {
            let applied_lsn = parse_required_lsn(fields, "applied_lsn")?;
            let write_result = decode_write_result(fields, &operation)?;
            JepsenEventOutcome::CommittedWrite(JepsenCommittedWrite {
                applied_lsn,
                result: write_result,
            })
        }
        JepsenOutcomeKind::ReadOk => JepsenEventOutcome::SuccessfulRead(JepsenSuccessfulRead {
            target: JepsenReadTarget::parse(required_field(fields, "read_target")?)?,
            served_by: ReplicaId(parse_required_u64(fields, "served_by")?),
            served_role: parse_role(required_field(fields, "served_role")?)?,
            observed_lsn: optional_lsn_field(fields, "observed_lsn")?,
            state: decode_read_state(fields)?,
        }),
        JepsenOutcomeKind::DefiniteFailure => JepsenEventOutcome::DefiniteFailure(
            JepsenDefiniteFailure::parse(required_field(fields, "reason")?)?,
        ),
        JepsenOutcomeKind::Ambiguous => JepsenEventOutcome::Ambiguous(
            JepsenAmbiguousOutcome::parse(required_field(fields, "reason")?)?,
        ),
    };

    Ok(JepsenHistoryEvent {
        sequence: parse_required_u64(fields, "sequence")?,
        process: String::from(required_field(fields, "process")?),
        time_millis: parse_required_u128(fields, "time_millis")?,
        operation,
        outcome,
    })
}

fn decode_write_result(
    fields: &BTreeMap<String, String>,
    operation: &JepsenOperation,
) -> Result<JepsenWriteResult, JepsenCodecError> {
    match required_field(fields, "write_result")? {
        "reserved" => Ok(JepsenWriteResult::Reserved {
            resource_id: operation
                .resource_id
                .ok_or_else(|| JepsenCodecError::MissingField(String::from("resource_id")))?,
            holder_id: parse_required_u128(fields, "committed_holder_id")?,
            reservation_id: parse_required_u128(fields, "committed_reservation_id")?,
            expires_at_slot: parse_required_slot(fields, "expires_at_slot")?,
        }),
        "confirmed" => Ok(JepsenWriteResult::Confirmed {
            resource_id: operation
                .resource_id
                .ok_or_else(|| JepsenCodecError::MissingField(String::from("resource_id")))?,
            holder_id: parse_required_u128(fields, "committed_holder_id")?,
            reservation_id: parse_required_u128(fields, "committed_reservation_id")?,
        }),
        "released" => Ok(JepsenWriteResult::Released {
            resource_id: operation
                .resource_id
                .ok_or_else(|| JepsenCodecError::MissingField(String::from("resource_id")))?,
            holder_id: parse_required_u128(fields, "committed_holder_id")?,
            reservation_id: parse_required_u128(fields, "committed_reservation_id")?,
            released_lsn: optional_lsn_field(fields, "released_lsn")?,
        }),
        "tick_expired" => Ok(JepsenWriteResult::TickExpired {
            expired: decode_expired_reservations(required_field(fields, "expired")?)?,
        }),
        other => Err(JepsenCodecError::InvalidField {
            field: String::from("write_result"),
            value: String::from(other),
        }),
    }
}

fn decode_read_state(
    fields: &BTreeMap<String, String>,
) -> Result<JepsenReadState, JepsenCodecError> {
    if let Some(resource_state) = fields.get("resource_state") {
        return decode_resource_state(resource_state).map(JepsenReadState::Resource);
    }
    if let Some(reservation_state) = fields.get("reservation_state") {
        return decode_reservation_state(reservation_state).map(JepsenReadState::Reservation);
    }
    Err(JepsenCodecError::MissingField(String::from(
        "resource_state or reservation_state",
    )))
}

fn parse_token_map(line: &str) -> Result<BTreeMap<String, String>, JepsenCodecError> {
    let mut fields = BTreeMap::new();
    for token in line.split_whitespace() {
        let Some((key, value)) = token.split_once('=') else {
            return Err(JepsenCodecError::InvalidLine(String::from(line)));
        };
        fields.insert(String::from(key), String::from(value));
    }
    Ok(fields)
}

fn required_field<'a>(
    fields: &'a BTreeMap<String, String>,
    field: &str,
) -> Result<&'a str, JepsenCodecError> {
    fields
        .get(field)
        .map(String::as_str)
        .ok_or_else(|| JepsenCodecError::MissingField(String::from(field)))
}

fn parse_required_u32(
    fields: &BTreeMap<String, String>,
    field: &str,
) -> Result<u32, JepsenCodecError> {
    required_field(fields, field)?
        .parse::<u32>()
        .map_err(|_| JepsenCodecError::InvalidField {
            field: String::from(field),
            value: fields[field].clone(),
        })
}

fn parse_required_u64(
    fields: &BTreeMap<String, String>,
    field: &str,
) -> Result<u64, JepsenCodecError> {
    required_field(fields, field)?
        .parse::<u64>()
        .map_err(|_| JepsenCodecError::InvalidField {
            field: String::from(field),
            value: fields[field].clone(),
        })
}

fn parse_required_u128(
    fields: &BTreeMap<String, String>,
    field: &str,
) -> Result<u128, JepsenCodecError> {
    required_field(fields, field)?
        .parse::<u128>()
        .map_err(|_| JepsenCodecError::InvalidField {
            field: String::from(field),
            value: fields[field].clone(),
        })
}

fn parse_required_lsn(
    fields: &BTreeMap<String, String>,
    field: &str,
) -> Result<Lsn, JepsenCodecError> {
    Ok(Lsn(parse_required_u64(fields, field)?))
}

fn parse_required_slot(
    fields: &BTreeMap<String, String>,
    field: &str,
) -> Result<Slot, JepsenCodecError> {
    Ok(Slot(parse_required_u64(fields, field)?))
}

fn optional_u64_field(
    fields: &BTreeMap<String, String>,
    field: &str,
) -> Result<Option<u64>, JepsenCodecError> {
    fields
        .get(field)
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|_| JepsenCodecError::InvalidField {
                    field: String::from(field),
                    value: value.clone(),
                })
        })
        .transpose()
}

fn optional_u128_field(
    fields: &BTreeMap<String, String>,
    field: &str,
) -> Result<Option<u128>, JepsenCodecError> {
    fields
        .get(field)
        .map(|value| {
            value
                .parse::<u128>()
                .map_err(|_| JepsenCodecError::InvalidField {
                    field: String::from(field),
                    value: value.clone(),
                })
        })
        .transpose()
}

fn optional_lsn_field(
    fields: &BTreeMap<String, String>,
    field: &str,
) -> Result<Option<Lsn>, JepsenCodecError> {
    fields
        .get(field)
        .map(|value| parse_optional_lsn(field, value))
        .transpose()
        .map(Option::flatten)
}

fn optional_slot_field(
    fields: &BTreeMap<String, String>,
    field: &str,
) -> Result<Option<Slot>, JepsenCodecError> {
    fields
        .get(field)
        .map(|value| parse_optional_slot(field, value))
        .transpose()
}

fn optional_resource_id_field(
    fields: &BTreeMap<String, String>,
    field: &str,
) -> Result<Option<ResourceId>, JepsenCodecError> {
    fields
        .get(field)
        .map(|value| {
            value
                .parse::<u128>()
                .map(ResourceId)
                .map_err(|_| JepsenCodecError::InvalidField {
                    field: String::from(field),
                    value: value.clone(),
                })
        })
        .transpose()
}

fn parse_optional_lsn(field: &str, value: &str) -> Result<Option<Lsn>, JepsenCodecError> {
    if value == "none" {
        return Ok(None);
    }

    value
        .parse::<u64>()
        .map(Lsn)
        .map(Some)
        .map_err(|_| JepsenCodecError::InvalidField {
            field: String::from(field),
            value: String::from(value),
        })
}

fn parse_optional_slot(field: &str, value: &str) -> Result<Slot, JepsenCodecError> {
    value
        .parse::<u64>()
        .map(Slot)
        .map_err(|_| JepsenCodecError::InvalidField {
            field: String::from(field),
            value: String::from(value),
        })
}

fn encode_expired_reservations(expired: &[JepsenExpiredReservation]) -> String {
    expired
        .iter()
        .map(|reservation| {
            format!(
                "{}:{}:{}:{}",
                reservation.resource_id.get(),
                reservation.holder_id,
                reservation.reservation_id,
                reservation
                    .released_lsn
                    .map_or(String::from("none"), |lsn| lsn.get().to_string())
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn decode_expired_reservations(
    value: &str,
) -> Result<Vec<JepsenExpiredReservation>, JepsenCodecError> {
    if value.is_empty() {
        return Ok(Vec::new());
    }
    value
        .split(',')
        .map(|entry| {
            let parts = entry.split(':').collect::<Vec<_>>();
            if parts.len() != 4 {
                return Err(JepsenCodecError::InvalidField {
                    field: String::from("expired"),
                    value: String::from(entry),
                });
            }
            let resource_id = parts[0].parse::<u128>().map(ResourceId).map_err(|_| {
                JepsenCodecError::InvalidField {
                    field: String::from("expired"),
                    value: String::from(entry),
                }
            })?;
            let holder_id =
                parts[1]
                    .parse::<u128>()
                    .map_err(|_| JepsenCodecError::InvalidField {
                        field: String::from("expired"),
                        value: String::from(entry),
                    })?;
            let reservation_id =
                parts[2]
                    .parse::<u128>()
                    .map_err(|_| JepsenCodecError::InvalidField {
                        field: String::from("expired"),
                        value: String::from(entry),
                    })?;
            let released_lsn = if parts[3] == "none" {
                None
            } else {
                Some(parts[3].parse::<u64>().map(Lsn).map_err(|_| {
                    JepsenCodecError::InvalidField {
                        field: String::from("expired"),
                        value: String::from(entry),
                    }
                })?)
            };
            Ok(JepsenExpiredReservation {
                resource_id,
                holder_id,
                reservation_id,
                released_lsn,
            })
        })
        .collect()
}

fn encode_resource_state(state: &JepsenResourceState) -> String {
    match state {
        JepsenResourceState::Available => String::from("available"),
        JepsenResourceState::Reserved {
            holder_id,
            reservation_id,
            expires_at_slot,
        } => format!(
            "reserved:{holder_id}:{reservation_id}:{}",
            expires_at_slot.get()
        ),
        JepsenResourceState::Confirmed {
            holder_id,
            reservation_id,
        } => format!("confirmed:{holder_id}:{reservation_id}"),
    }
}

fn decode_resource_state(value: &str) -> Result<JepsenResourceState, JepsenCodecError> {
    if value == "available" {
        return Ok(JepsenResourceState::Available);
    }
    let parts = value.split(':').collect::<Vec<_>>();
    match parts.as_slice() {
        ["reserved", holder_id, reservation_id, expires_at_slot] => {
            Ok(JepsenResourceState::Reserved {
                holder_id: parse_token_u128("resource_state", holder_id)?,
                reservation_id: parse_token_u128("resource_state", reservation_id)?,
                expires_at_slot: Slot(parse_token_u64("resource_state", expires_at_slot)?),
            })
        }
        ["confirmed", holder_id, reservation_id] => Ok(JepsenResourceState::Confirmed {
            holder_id: parse_token_u128("resource_state", holder_id)?,
            reservation_id: parse_token_u128("resource_state", reservation_id)?,
        }),
        _ => Err(JepsenCodecError::InvalidField {
            field: String::from("resource_state"),
            value: String::from(value),
        }),
    }
}

fn encode_reservation_state(state: &JepsenReservationState) -> String {
    match state {
        JepsenReservationState::NotFound => String::from("not_found"),
        JepsenReservationState::Retired => String::from("retired"),
        JepsenReservationState::Active {
            resource_id,
            holder_id,
            expires_at_slot,
            confirmed,
        } => format!(
            "active:{}:{holder_id}:{}:{confirmed}",
            resource_id.get(),
            expires_at_slot.get()
        ),
        JepsenReservationState::Released {
            resource_id,
            holder_id,
            released_lsn,
        } => format!(
            "released:{}:{holder_id}:{}",
            resource_id.get(),
            released_lsn.map_or(String::from("none"), |lsn| lsn.get().to_string())
        ),
    }
}

fn decode_reservation_state(value: &str) -> Result<JepsenReservationState, JepsenCodecError> {
    match value {
        "not_found" => return Ok(JepsenReservationState::NotFound),
        "retired" => return Ok(JepsenReservationState::Retired),
        _ => {}
    }
    let parts = value.split(':').collect::<Vec<_>>();
    match parts.as_slice() {
        ["active", resource_id, holder_id, expires_at_slot, confirmed] => {
            Ok(JepsenReservationState::Active {
                resource_id: ResourceId(parse_token_u128("reservation_state", resource_id)?),
                holder_id: parse_token_u128("reservation_state", holder_id)?,
                expires_at_slot: Slot(parse_token_u64("reservation_state", expires_at_slot)?),
                confirmed: parse_token_bool("reservation_state", confirmed)?,
            })
        }
        ["released", resource_id, holder_id, released_lsn] => {
            Ok(JepsenReservationState::Released {
                resource_id: ResourceId(parse_token_u128("reservation_state", resource_id)?),
                holder_id: parse_token_u128("reservation_state", holder_id)?,
                released_lsn: if *released_lsn == "none" {
                    None
                } else {
                    Some(Lsn(parse_token_u64("reservation_state", released_lsn)?))
                },
            })
        }
        _ => Err(JepsenCodecError::InvalidField {
            field: String::from("reservation_state"),
            value: String::from(value),
        }),
    }
}

fn parse_token_u64(field: &str, value: &str) -> Result<u64, JepsenCodecError> {
    value
        .parse::<u64>()
        .map_err(|_| JepsenCodecError::InvalidField {
            field: String::from(field),
            value: String::from(value),
        })
}

fn parse_token_u128(field: &str, value: &str) -> Result<u128, JepsenCodecError> {
    value
        .parse::<u128>()
        .map_err(|_| JepsenCodecError::InvalidField {
            field: String::from(field),
            value: String::from(value),
        })
}

fn parse_token_bool(field: &str, value: &str) -> Result<bool, JepsenCodecError> {
    value
        .parse::<bool>()
        .map_err(|_| JepsenCodecError::InvalidField {
            field: String::from(field),
            value: String::from(value),
        })
}

fn encode_role(role: ReplicaRole) -> &'static str {
    match role {
        ReplicaRole::Primary => "primary",
        ReplicaRole::Backup => "backup",
        ReplicaRole::ViewUncertain => "view_uncertain",
        ReplicaRole::Recovering => "recovering",
        ReplicaRole::Faulted => "faulted",
    }
}

fn parse_role(value: &str) -> Result<ReplicaRole, JepsenCodecError> {
    match value {
        "primary" => Ok(ReplicaRole::Primary),
        "backup" => Ok(ReplicaRole::Backup),
        "view_uncertain" => Ok(ReplicaRole::ViewUncertain),
        "recovering" => Ok(ReplicaRole::Recovering),
        "faulted" => Ok(ReplicaRole::Faulted),
        other => Err(JepsenCodecError::InvalidField {
            field: String::from("served_role"),
            value: String::from(other),
        }),
    }
}

fn write_bytes_atomically(path: &Path, bytes: &[u8]) -> Result<(), JepsenCodecError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let temp_path = path.with_extension(format!("{}.tmp", std::process::id()));
    let mut file = File::create(&temp_path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&temp_path, path).map_err(|error| {
        let _ = fs::remove_file(&temp_path);
        JepsenCodecError::Io(error)
    })?;
    Ok(())
}

fn fingerprint_write(write: &JepsenCommittedWrite) -> CommittedFingerprint {
    match &write.result {
        JepsenWriteResult::Reserved {
            resource_id,
            holder_id,
            reservation_id,
            expires_at_slot,
        } => CommittedFingerprint {
            applied_lsn: write.applied_lsn,
            resource_id: Some(*resource_id),
            reservation_id: Some(*reservation_id),
            holder_id: Some(*holder_id),
            released_lsn: None,
            expires_at_slot: Some(*expires_at_slot),
        },
        JepsenWriteResult::Confirmed {
            resource_id,
            holder_id,
            reservation_id,
        } => CommittedFingerprint {
            applied_lsn: write.applied_lsn,
            resource_id: Some(*resource_id),
            reservation_id: Some(*reservation_id),
            holder_id: Some(*holder_id),
            released_lsn: None,
            expires_at_slot: None,
        },
        JepsenWriteResult::Released {
            resource_id,
            holder_id,
            reservation_id,
            released_lsn,
        } => CommittedFingerprint {
            applied_lsn: write.applied_lsn,
            resource_id: Some(*resource_id),
            reservation_id: Some(*reservation_id),
            holder_id: Some(*holder_id),
            released_lsn: *released_lsn,
            expires_at_slot: None,
        },
        JepsenWriteResult::TickExpired { .. } => CommittedFingerprint {
            applied_lsn: write.applied_lsn,
            resource_id: None,
            reservation_id: None,
            holder_id: None,
            released_lsn: None,
            expires_at_slot: None,
        },
    }
}

fn render_blocker(blocker: &JepsenBlockingIssue) -> String {
    match blocker {
        JepsenBlockingIssue::MissingOperationId { sequence, kind } => {
            format!(
                "blocker=missing_operation_id sequence={sequence} operation={}",
                kind.as_str()
            )
        }
        JepsenBlockingIssue::DuplicateCommittedExecution {
            operation_id,
            first_lsn,
            second_lsn,
        } => format!(
            "blocker=duplicate_committed_execution operation_id={operation_id} first_lsn={} second_lsn={}",
            first_lsn.get(),
            second_lsn.get()
        ),
        JepsenBlockingIssue::UnresolvedAmbiguity { operation_id } => {
            format!("blocker=unresolved_ambiguity operation_id={operation_id}")
        }
        JepsenBlockingIssue::DoubleAllocation {
            resource_id,
            existing_operation_id,
            conflicting_operation_id,
        } => format!(
            "blocker=double_allocation resource_id={} existing_operation_id={existing_operation_id} conflicting_operation_id={conflicting_operation_id}",
            resource_id.get()
        ),
        JepsenBlockingIssue::StaleSuccessfulRead(violation) => format!(
            "blocker=stale_successful_read replica_id={} role={} required_lsn={} observed_lsn={}",
            violation.replica_id.get(),
            encode_role(violation.served_role),
            violation
                .required_lsn
                .map_or(String::from("none"), |lsn| lsn.get().to_string()),
            violation
                .observed_lsn
                .map_or(String::from("none"), |lsn| lsn.get().to_string()),
        ),
        JepsenBlockingIssue::EarlyExpirationRelease {
            resource_id,
            reservation_id,
            expires_at_slot,
            released_at_slot,
        } => format!(
            "blocker=early_expiration_release resource_id={} reservation_id={reservation_id} expires_at_slot={} released_at_slot={}",
            resource_id.get(),
            expires_at_slot.get(),
            released_at_slot.get()
        ),
    }
}

#[derive(Clone, Debug)]
struct CommittedRecord {
    operation_id: u128,
    operation: JepsenOperation,
    write: JepsenCommittedWrite,
}

#[derive(Default)]
struct LogicalSummaryBuilder {
    attempts: usize,
    ambiguous_attempts: usize,
    definite_failures: Vec<JepsenDefiniteFailure>,
    committed_result: Option<CommittedFingerprint>,
}

#[derive(Clone, Copy)]
struct ResourceLeaseState {
    operation_id: u128,
    reservation_id: u128,
    expires_at_slot: Slot,
}

fn collect_mutating_attempts(
    events: &[JepsenHistoryEvent],
) -> (
    BTreeMap<u128, LogicalSummaryBuilder>,
    BTreeMap<u64, CommittedRecord>,
    Vec<JepsenBlockingIssue>,
) {
    let mut builders = BTreeMap::<u128, LogicalSummaryBuilder>::new();
    let mut committed = BTreeMap::<u64, CommittedRecord>::new();
    let mut blockers = Vec::new();

    for event in events {
        if !event.operation.is_mutating() {
            continue;
        }
        let Some(operation_id) = event.operation.operation_id else {
            blockers.push(JepsenBlockingIssue::MissingOperationId {
                sequence: event.sequence,
                kind: event.operation.kind,
            });
            continue;
        };
        let builder = builders.entry(operation_id).or_default();
        builder.attempts += 1;
        match &event.outcome {
            JepsenEventOutcome::CommittedWrite(write) => {
                record_committed_attempt(
                    &mut blockers,
                    &mut committed,
                    builder,
                    operation_id,
                    event.operation.clone(),
                    write.clone(),
                );
            }
            JepsenEventOutcome::DefiniteFailure(failure) => {
                builder.definite_failures.push(*failure);
            }
            JepsenEventOutcome::Ambiguous(_) => {
                builder.ambiguous_attempts += 1;
            }
            JepsenEventOutcome::SuccessfulRead(_) => {}
        }
    }

    (builders, committed, blockers)
}

fn record_committed_attempt(
    blockers: &mut Vec<JepsenBlockingIssue>,
    committed: &mut BTreeMap<u64, CommittedRecord>,
    builder: &mut LogicalSummaryBuilder,
    operation_id: u128,
    operation: JepsenOperation,
    write: JepsenCommittedWrite,
) {
    let fingerprint = fingerprint_write(&write);
    if let Some(existing) = builder.committed_result {
        if existing != fingerprint {
            blockers.push(JepsenBlockingIssue::DuplicateCommittedExecution {
                operation_id,
                first_lsn: existing.applied_lsn,
                second_lsn: fingerprint.applied_lsn,
            });
        }
    } else {
        builder.committed_result = Some(fingerprint);
    }
    committed.insert(
        write.applied_lsn.get(),
        CommittedRecord {
            operation_id,
            operation,
            write,
        },
    );
}

fn check_successful_reads(events: &[JepsenHistoryEvent]) -> Vec<JepsenBlockingIssue> {
    events
        .iter()
        .filter_map(|event| match &event.outcome {
            JepsenEventOutcome::SuccessfulRead(read) => {
                stale_read_violation(read, event.operation.required_lsn)
                    .map(JepsenBlockingIssue::StaleSuccessfulRead)
            }
            _ => None,
        })
        .collect()
}

fn stale_read_violation(
    read: &JepsenSuccessfulRead,
    required_lsn: Option<Lsn>,
) -> Option<JepsenReadViolation> {
    let missing_primary_role = read.served_role != ReplicaRole::Primary;
    let missing_fence = required_lsn
        .is_some_and(|required| read.observed_lsn.is_none_or(|observed| observed < required));
    if missing_primary_role || missing_fence {
        Some(JepsenReadViolation {
            replica_id: read.served_by,
            served_role: read.served_role,
            required_lsn,
            observed_lsn: read.observed_lsn,
        })
    } else {
        None
    }
}

fn unresolved_ambiguity_blockers(
    builders: &BTreeMap<u128, LogicalSummaryBuilder>,
) -> Vec<JepsenBlockingIssue> {
    builders
        .iter()
        .filter_map(|(&operation_id, builder)| {
            if builder.ambiguous_attempts > 0
                && builder.committed_result.is_none()
                && builder.definite_failures.is_empty()
            {
                Some(JepsenBlockingIssue::UnresolvedAmbiguity { operation_id })
            } else {
                None
            }
        })
        .collect()
}

fn check_committed_release_blockers(
    committed: BTreeMap<u64, CommittedRecord>,
) -> Vec<JepsenBlockingIssue> {
    let mut blockers = Vec::new();
    let mut resources = BTreeMap::<u128, ResourceLeaseState>::new();
    for record in committed.into_values() {
        apply_committed_record(&mut resources, &mut blockers, &record);
    }
    blockers
}

fn apply_committed_record(
    resources: &mut BTreeMap<u128, ResourceLeaseState>,
    blockers: &mut Vec<JepsenBlockingIssue>,
    record: &CommittedRecord,
) {
    match &record.write.result {
        JepsenWriteResult::Reserved {
            resource_id,
            reservation_id,
            expires_at_slot,
            ..
        } => record_reserved_write(
            resources,
            blockers,
            *resource_id,
            *reservation_id,
            *expires_at_slot,
            record.operation_id,
        ),
        JepsenWriteResult::Confirmed {
            resource_id,
            reservation_id,
            ..
        } => record_confirmed_write(
            resources,
            blockers,
            *resource_id,
            *reservation_id,
            record.operation_id,
        ),
        JepsenWriteResult::Released {
            resource_id,
            reservation_id,
            ..
        } => {
            remove_matching_reservation(resources, *resource_id, *reservation_id);
        }
        JepsenWriteResult::TickExpired { expired } => {
            record_tick_expiration(resources, blockers, &record.operation, expired);
        }
    }
}

fn record_reserved_write(
    resources: &mut BTreeMap<u128, ResourceLeaseState>,
    blockers: &mut Vec<JepsenBlockingIssue>,
    resource_id: ResourceId,
    reservation_id: u128,
    expires_at_slot: Slot,
    operation_id: u128,
) {
    if let Some(existing) = resources.get(&resource_id.get()) {
        blockers.push(JepsenBlockingIssue::DoubleAllocation {
            resource_id,
            existing_operation_id: existing.operation_id,
            conflicting_operation_id: operation_id,
        });
    }
    resources.insert(
        resource_id.get(),
        ResourceLeaseState {
            operation_id,
            reservation_id,
            expires_at_slot,
        },
    );
}

fn record_confirmed_write(
    resources: &BTreeMap<u128, ResourceLeaseState>,
    blockers: &mut Vec<JepsenBlockingIssue>,
    resource_id: ResourceId,
    reservation_id: u128,
    operation_id: u128,
) {
    if let Some(existing) = resources.get(&resource_id.get()) {
        if existing.reservation_id != reservation_id {
            blockers.push(JepsenBlockingIssue::DoubleAllocation {
                resource_id,
                existing_operation_id: existing.operation_id,
                conflicting_operation_id: operation_id,
            });
        }
    }
}

fn record_tick_expiration(
    resources: &mut BTreeMap<u128, ResourceLeaseState>,
    blockers: &mut Vec<JepsenBlockingIssue>,
    operation: &JepsenOperation,
    expired: &[JepsenExpiredReservation],
) {
    let Some(release_slot) = operation.request_slot else {
        return;
    };
    for expired_reservation in expired {
        if let Some(existing) = resources.get(&expired_reservation.resource_id.get()) {
            if existing.reservation_id == expired_reservation.reservation_id {
                if release_slot < existing.expires_at_slot {
                    blockers.push(JepsenBlockingIssue::EarlyExpirationRelease {
                        resource_id: expired_reservation.resource_id,
                        reservation_id: expired_reservation.reservation_id,
                        expires_at_slot: existing.expires_at_slot,
                        released_at_slot: release_slot,
                    });
                }
                resources.remove(&expired_reservation.resource_id.get());
            }
        }
    }
}

fn remove_matching_reservation(
    resources: &mut BTreeMap<u128, ResourceLeaseState>,
    resource_id: ResourceId,
    reservation_id: u128,
) {
    if resources
        .get(&resource_id.get())
        .is_some_and(|existing| existing.reservation_id == reservation_id)
    {
        resources.remove(&resource_id.get());
    }
}

fn summarize_logical_commands(
    builders: BTreeMap<u128, LogicalSummaryBuilder>,
) -> Vec<JepsenLogicalCommandSummary> {
    builders
        .into_iter()
        .map(|(operation_id, builder)| JepsenLogicalCommandSummary {
            operation_id,
            attempts: builder.attempts,
            ambiguous_attempts: builder.ambiguous_attempts,
            definite_failures: builder.definite_failures,
            committed_result: builder.committed_result,
        })
        .collect()
}

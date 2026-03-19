use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use allocdb_core::config::Config;
use allocdb_core::ids::Lsn;

use crate::engine::{EngineConfig, RecoveryStartupKind};
use crate::replica::{ReplicaId, ReplicaPaths, ReplicaRole};

const CLUSTER_LAYOUT_VERSION: u32 = 1;
const LOCAL_CLUSTER_REPLICA_COUNT: u64 = 3;
const LOCAL_CLUSTER_INITIAL_VIEW: u64 = 1;
const CONTROL_PROTOCOL_VERSION: u32 = 1;
const CLUSTER_LAYOUT_FILE_NAME: &str = "cluster-layout.txt";
const CLUSTER_FAULT_STATE_VERSION: u32 = 1;
const CLUSTER_FAULT_STATE_FILE_NAME: &str = "cluster-faults.txt";
const CLUSTER_TIMELINE_VERSION: u32 = 1;
const CLUSTER_TIMELINE_FILE_NAME: &str = "cluster-timeline.log";
const REPLICA_LOG_DIR_NAME: &str = "logs";
const REPLICA_RUN_DIR_NAME: &str = "run";
const REPLICA_METADATA_FILE_NAME: &str = "replica.metadata";
const REPLICA_SNAPSHOT_FILE_NAME: &str = "state.snapshot";
const REPLICA_WAL_FILE_NAME: &str = "state.wal";
const DEFAULT_LOCAL_CLUSTER_MAX_BUNDLE_SIZE: u32 = 1;
const CONTROL_IO_TIMEOUT: Duration = Duration::from_millis(250);
const CONTROL_STATUS_RETRY_DELAY: Duration = Duration::from_millis(10);
const CONTROL_STATUS_MAX_ATTEMPTS: usize = 3;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalClusterReplicaConfig {
    pub replica_id: ReplicaId,
    pub role: ReplicaRole,
    pub workspace_dir: PathBuf,
    pub log_path: PathBuf,
    pub pid_path: PathBuf,
    pub paths: ReplicaPaths,
    pub control_addr: SocketAddr,
    pub client_addr: SocketAddr,
    pub protocol_addr: SocketAddr,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalClusterLayout {
    pub workspace_root: PathBuf,
    pub current_view: u64,
    pub core_config: Config,
    pub engine_config: EngineConfig,
    pub replicas: Vec<LocalClusterReplicaConfig>,
}

#[derive(Debug)]
pub enum LocalClusterLayoutError {
    Io(std::io::Error),
    MissingField(String),
    DuplicateField(String),
    InvalidLine(String),
    InvalidField { field: String, value: String },
    UnsupportedVersion(u32),
    InvalidReplicaCount(u64),
    UnknownReplicaId(u64),
}

impl From<std::io::Error> for LocalClusterLayoutError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl fmt::Display for LocalClusterLayoutError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "i/o error: {error}"),
            Self::MissingField(field) => write!(formatter, "missing layout field `{field}`"),
            Self::DuplicateField(field) => write!(formatter, "duplicate layout field `{field}`"),
            Self::InvalidLine(line) => write!(formatter, "invalid layout line `{line}`"),
            Self::InvalidField { field, value } => {
                write!(formatter, "invalid value for `{field}`: `{value}`")
            }
            Self::UnsupportedVersion(version) => {
                write!(
                    formatter,
                    "unsupported local cluster layout version `{version}`"
                )
            }
            Self::InvalidReplicaCount(count) => {
                write!(formatter, "unsupported replica count `{count}`")
            }
            Self::UnknownReplicaId(replica_id) => {
                write!(formatter, "unknown replica id `{replica_id}`")
            }
        }
    }
}

impl std::error::Error for LocalClusterLayoutError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ControlRequest {
    Status,
    Stop,
}

#[derive(Debug)]
pub enum ControlProtocolError {
    Io(std::io::Error),
    MissingField(String),
    DuplicateField(String),
    InvalidLine(String),
    InvalidField { field: String, value: String },
    Remote(String),
}

impl From<std::io::Error> for ControlProtocolError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl fmt::Display for ControlProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "control i/o error: {error}"),
            Self::MissingField(field) => write!(formatter, "missing control field `{field}`"),
            Self::DuplicateField(field) => write!(formatter, "duplicate control field `{field}`"),
            Self::InvalidLine(line) => write!(formatter, "invalid control line `{line}`"),
            Self::InvalidField { field, value } => {
                write!(formatter, "invalid value for `{field}`: `{value}`")
            }
            Self::Remote(message) => write!(formatter, "{message}"),
        }
    }
}

impl std::error::Error for ControlProtocolError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicaRuntimeState {
    Active,
    Faulted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaRuntimeStatus {
    pub process_id: u32,
    pub replica_id: ReplicaId,
    pub state: ReplicaRuntimeState,
    pub role: ReplicaRole,
    pub current_view: u64,
    pub commit_lsn: Option<Lsn>,
    pub active_snapshot_lsn: Option<Lsn>,
    pub accepting_writes: Option<bool>,
    pub startup_kind: Option<RecoveryStartupKind>,
    pub loaded_snapshot_lsn: Option<Lsn>,
    pub replayed_wal_frame_count: Option<u32>,
    pub replayed_wal_last_lsn: Option<Lsn>,
    pub fault_reason: Option<String>,
    pub workspace_dir: PathBuf,
    pub log_path: PathBuf,
    pub pid_path: PathBuf,
    pub metadata_path: PathBuf,
    pub prepare_log_path: PathBuf,
    pub snapshot_path: PathBuf,
    pub wal_path: PathBuf,
    pub control_addr: SocketAddr,
    pub client_addr: SocketAddr,
    pub protocol_addr: SocketAddr,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalClusterFaultState {
    pub workspace_root: PathBuf,
    pub isolated_replicas: Vec<ReplicaId>,
}

#[derive(Debug)]
pub enum LocalClusterFaultStateError {
    Io(std::io::Error),
    Decode(LocalClusterLayoutError),
    UnknownReplicaId(u64),
}

impl From<std::io::Error> for LocalClusterFaultStateError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl fmt::Display for LocalClusterFaultStateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "i/o error: {error}"),
            Self::Decode(error) => write!(formatter, "invalid fault-state file: {error}"),
            Self::UnknownReplicaId(replica_id) => {
                write!(formatter, "unknown replica id `{replica_id}`")
            }
        }
    }
}

impl std::error::Error for LocalClusterFaultStateError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalClusterTimelineEventKind {
    ClusterStart,
    ClusterStop,
    ReplicaCrash,
    ReplicaRestart,
    ReplicaIsolate,
    ReplicaHeal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalClusterTimelineEvent {
    pub sequence: u64,
    pub timestamp_millis: u128,
    pub kind: LocalClusterTimelineEventKind,
    pub replica_id: Option<ReplicaId>,
    pub detail: Option<String>,
}

#[derive(Debug)]
pub enum LocalClusterTimelineError {
    Io(std::io::Error),
    Decode(LocalClusterLayoutError),
}

impl From<std::io::Error> for LocalClusterTimelineError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl fmt::Display for LocalClusterTimelineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "i/o error: {error}"),
            Self::Decode(error) => write!(formatter, "invalid cluster timeline: {error}"),
        }
    }
}

impl std::error::Error for LocalClusterTimelineError {}

impl LocalClusterLayout {
    /// Loads one existing local-cluster layout or creates a fresh one in the provided workspace.
    ///
    /// # Errors
    ///
    /// Returns [`LocalClusterLayoutError`] if the workspace root cannot be created or
    /// canonicalized, if the existing layout file cannot be read or decoded, or if a new layout
    /// cannot be persisted.
    pub fn load_or_create(
        workspace_root: impl AsRef<Path>,
    ) -> Result<Self, LocalClusterLayoutError> {
        let workspace_root = prepare_workspace_root(workspace_root.as_ref())?;
        let layout_path = layout_path(&workspace_root);
        if layout_path.exists() {
            Self::load(&layout_path)
        } else {
            let layout = Self::new(workspace_root)?;
            layout.persist()?;
            Ok(layout)
        }
    }

    /// Loads one persisted local-cluster layout from disk.
    ///
    /// # Errors
    ///
    /// Returns [`LocalClusterLayoutError`] if the file cannot be read or if its contents are
    /// malformed.
    pub fn load(layout_path: impl AsRef<Path>) -> Result<Self, LocalClusterLayoutError> {
        let layout_path = layout_path.as_ref();
        let mut bytes = String::new();
        File::open(layout_path)?.read_to_string(&mut bytes)?;
        decode_layout(&bytes)
    }

    /// Persists the current local-cluster layout atomically to its workspace root.
    ///
    /// # Errors
    ///
    /// Returns [`LocalClusterLayoutError`] if the workspace or layout file cannot be created or
    /// updated.
    pub fn persist(&self) -> Result<(), LocalClusterLayoutError> {
        fs::create_dir_all(&self.workspace_root)?;
        let bytes = encode_layout(self);
        write_bytes_atomically(&self.layout_path(), bytes.as_bytes())?;
        Ok(())
    }

    #[must_use]
    pub fn layout_path(&self) -> PathBuf {
        layout_path(&self.workspace_root)
    }

    #[must_use]
    pub fn replica(&self, replica_id: ReplicaId) -> Option<&LocalClusterReplicaConfig> {
        self.replicas
            .iter()
            .find(|replica| replica.replica_id == replica_id)
    }

    fn new(workspace_root: PathBuf) -> Result<Self, LocalClusterLayoutError> {
        fs::create_dir_all(workspace_root.join(REPLICA_LOG_DIR_NAME))?;
        fs::create_dir_all(workspace_root.join(REPLICA_RUN_DIR_NAME))?;

        let core_config = default_local_cluster_core_config();
        let engine_config = default_local_cluster_engine_config();
        let mut seen_addrs = HashSet::new();
        let replicas = (1_u64..=LOCAL_CLUSTER_REPLICA_COUNT)
            .map(|replica_id| {
                let workspace_dir = workspace_root.join(format!("replica-{replica_id}"));
                let log_path = workspace_root
                    .join(REPLICA_LOG_DIR_NAME)
                    .join(format!("replica-{replica_id}.log"));
                let pid_path = workspace_root
                    .join(REPLICA_RUN_DIR_NAME)
                    .join(format!("replica-{replica_id}.pid"));
                let control_addr = reserve_loopback_addr(&mut seen_addrs)?;
                let client_addr = reserve_loopback_addr(&mut seen_addrs)?;
                let protocol_addr = reserve_loopback_addr(&mut seen_addrs)?;
                Ok(LocalClusterReplicaConfig {
                    replica_id: ReplicaId(replica_id),
                    role: if replica_id == 1 {
                        ReplicaRole::Primary
                    } else {
                        ReplicaRole::Backup
                    },
                    workspace_dir: workspace_dir.clone(),
                    log_path,
                    pid_path,
                    paths: ReplicaPaths::new(
                        workspace_dir.join(REPLICA_METADATA_FILE_NAME),
                        workspace_dir.join(REPLICA_SNAPSHOT_FILE_NAME),
                        workspace_dir.join(REPLICA_WAL_FILE_NAME),
                    ),
                    control_addr,
                    client_addr,
                    protocol_addr,
                })
            })
            .collect::<Result<Vec<_>, LocalClusterLayoutError>>()?;

        Ok(Self {
            workspace_root,
            current_view: LOCAL_CLUSTER_INITIAL_VIEW,
            core_config,
            engine_config,
            replicas,
        })
    }
}

impl LocalClusterFaultState {
    /// Loads one existing fault-state file or creates a fresh healthy state in the provided
    /// workspace.
    ///
    /// # Errors
    ///
    /// Returns [`LocalClusterFaultStateError`] if the workspace cannot be prepared, if an existing
    /// file cannot be decoded, or if a fresh file cannot be persisted.
    pub fn load_or_create(
        workspace_root: impl AsRef<Path>,
    ) -> Result<Self, LocalClusterFaultStateError> {
        let workspace_root = prepare_workspace_root(workspace_root.as_ref())
            .map_err(LocalClusterFaultStateError::Decode)?;
        let path = fault_state_path(&workspace_root);
        if path.exists() {
            Self::load(&path)
        } else {
            let state = Self::new(workspace_root);
            state.persist()?;
            Ok(state)
        }
    }

    /// Loads one persisted fault-state file from disk.
    ///
    /// # Errors
    ///
    /// Returns [`LocalClusterFaultStateError`] if the file cannot be read or decoded.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, LocalClusterFaultStateError> {
        let path = path.as_ref();
        let mut bytes = String::new();
        File::open(path)?.read_to_string(&mut bytes)?;
        decode_fault_state(&bytes)
    }

    /// Persists the current fault-state file atomically to its workspace root.
    ///
    /// # Errors
    ///
    /// Returns [`LocalClusterFaultStateError`] if the file cannot be updated.
    pub fn persist(&self) -> Result<(), LocalClusterFaultStateError> {
        fs::create_dir_all(&self.workspace_root)?;
        let bytes = encode_fault_state(self);
        write_bytes_atomically(&fault_state_path(&self.workspace_root), bytes.as_bytes())?;
        Ok(())
    }

    #[must_use]
    pub fn is_replica_isolated(&self, replica_id: ReplicaId) -> bool {
        self.isolated_replicas.contains(&replica_id)
    }

    /// Marks one replica isolated from external client and protocol traffic.
    ///
    /// # Errors
    ///
    /// Returns [`LocalClusterFaultStateError::UnknownReplicaId`] when the replica is outside the
    /// fixed local-cluster membership.
    pub fn isolate_replica(
        &mut self,
        replica_id: ReplicaId,
    ) -> Result<(), LocalClusterFaultStateError> {
        validate_replica_id(replica_id)
            .map_err(|_| LocalClusterFaultStateError::UnknownReplicaId(replica_id.get()))?;
        if !self.is_replica_isolated(replica_id) {
            self.isolated_replicas.push(replica_id);
            self.isolated_replicas
                .sort_by_key(|replica_id| replica_id.get());
        }
        Ok(())
    }

    /// Restores one replica to healthy connectivity in the local fault-state file.
    ///
    /// # Errors
    ///
    /// Returns [`LocalClusterFaultStateError::UnknownReplicaId`] when the replica is outside the
    /// fixed local-cluster membership.
    pub fn heal_replica(
        &mut self,
        replica_id: ReplicaId,
    ) -> Result<(), LocalClusterFaultStateError> {
        validate_replica_id(replica_id)
            .map_err(|_| LocalClusterFaultStateError::UnknownReplicaId(replica_id.get()))?;
        self.isolated_replicas.retain(|found| *found != replica_id);
        Ok(())
    }

    fn new(workspace_root: PathBuf) -> Self {
        Self {
            workspace_root,
            isolated_replicas: Vec::new(),
        }
    }
}

#[must_use]
pub fn default_local_cluster_core_config() -> Config {
    Config {
        shard_id: 0,
        max_resources: 1_024,
        max_reservations: 1_024,
        max_bundle_size: DEFAULT_LOCAL_CLUSTER_MAX_BUNDLE_SIZE,
        max_operations: 4_096,
        max_ttl_slots: 256,
        max_client_retry_window_slots: 128,
        reservation_history_window_slots: 64,
        max_expiration_bucket_len: 1_024,
    }
}

#[must_use]
pub const fn default_local_cluster_engine_config() -> EngineConfig {
    EngineConfig {
        max_submission_queue: 64,
        max_command_bytes: 4_096,
        max_expirations_per_tick: 64,
    }
}

#[must_use]
pub fn layout_path(workspace_root: &Path) -> PathBuf {
    workspace_root.join(CLUSTER_LAYOUT_FILE_NAME)
}

#[must_use]
pub fn fault_state_path(workspace_root: &Path) -> PathBuf {
    workspace_root.join(CLUSTER_FAULT_STATE_FILE_NAME)
}

#[must_use]
pub fn timeline_path(workspace_root: &Path) -> PathBuf {
    workspace_root.join(CLUSTER_TIMELINE_FILE_NAME)
}

/// Encodes one local-cluster layout to the text format used on disk.
#[must_use]
pub fn encode_layout_text(layout: &LocalClusterLayout) -> String {
    encode_layout(layout)
}

/// Loads one persisted local-cluster timeline.
///
/// # Errors
///
/// Returns [`LocalClusterTimelineError`] if the file cannot be read or if its contents are
/// malformed. A missing file yields an empty timeline.
pub fn load_local_cluster_timeline(
    workspace_root: impl AsRef<Path>,
) -> Result<Vec<LocalClusterTimelineEvent>, LocalClusterTimelineError> {
    let path = timeline_path(workspace_root.as_ref());
    if !path.exists() {
        return Ok(Vec::new());
    }

    let mut bytes = String::new();
    File::open(path)?.read_to_string(&mut bytes)?;
    decode_timeline(&bytes)
}

/// Appends one timeline event to the local-cluster timeline log.
///
/// # Errors
///
/// Returns [`LocalClusterTimelineError`] if the workspace cannot be created, if an existing
/// timeline cannot be decoded, or if the new event cannot be persisted.
pub fn append_local_cluster_timeline_event(
    workspace_root: impl AsRef<Path>,
    kind: LocalClusterTimelineEventKind,
    replica_id: Option<ReplicaId>,
    detail: Option<&str>,
) -> Result<LocalClusterTimelineEvent, LocalClusterTimelineError> {
    let workspace_root = prepare_workspace_root(workspace_root.as_ref())
        .map_err(LocalClusterTimelineError::Decode)?;
    let mut events = load_local_cluster_timeline(&workspace_root)?;
    let event = LocalClusterTimelineEvent {
        sequence: events
            .last()
            .map_or(1, |event| event.sequence.saturating_add(1)),
        timestamp_millis: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
        kind,
        replica_id,
        detail: detail.map(ToOwned::to_owned),
    };
    events.push(event.clone());
    persist_local_cluster_timeline(&workspace_root, &events)?;
    Ok(event)
}

/// Sends one `status` request to a running replica control socket.
///
/// # Errors
///
/// Returns [`ControlProtocolError`] if the socket cannot be reached, if the response cannot be
/// read, or if the returned status payload is malformed.
pub fn request_control_status(
    addr: SocketAddr,
) -> Result<ReplicaRuntimeStatus, ControlProtocolError> {
    request_control_status_with(addr, send_control_request, std::thread::sleep)
}

fn request_control_status_with<F, S>(
    addr: SocketAddr,
    mut send_request: F,
    mut sleep: S,
) -> Result<ReplicaRuntimeStatus, ControlProtocolError>
where
    F: FnMut(SocketAddr, ControlRequest) -> Result<String, ControlProtocolError>,
    S: FnMut(Duration),
{
    let mut last_error = None;
    for attempt in 0..CONTROL_STATUS_MAX_ATTEMPTS {
        match send_request(addr, ControlRequest::Status)
            .and_then(|response| decode_status_response(&response))
        {
            Ok(status) => return Ok(status),
            Err(error)
                if control_status_error_is_transient(&error)
                    && attempt + 1 < CONTROL_STATUS_MAX_ATTEMPTS =>
            {
                last_error = Some(error);
                sleep(CONTROL_STATUS_RETRY_DELAY);
            }
            Err(error) => return Err(error),
        }
    }
    Err(last_error.unwrap_or_else(|| {
        ControlProtocolError::Remote(String::from(
            "control status retry loop ended without error",
        ))
    }))
}

/// Sends one `stop` request to a running replica control socket.
///
/// # Errors
///
/// Returns [`ControlProtocolError`] if the socket cannot be reached, if the response cannot be
/// read, or if the replica does not acknowledge the stop request cleanly.
pub fn request_control_stop(addr: SocketAddr) -> Result<(), ControlProtocolError> {
    let response = send_control_request(addr, ControlRequest::Stop)?;
    let fields = parse_key_value_lines(&response).map_err(ControlProtocolError::from)?;
    match required_field(&fields, "status")? {
        "ok" => Ok(()),
        other => Err(ControlProtocolError::Remote(format!(
            "unexpected stop response status `{other}`"
        ))),
    }
}

/// Parses one incoming control command from the replica daemon request stream.
///
/// # Errors
///
/// Returns [`ControlProtocolError`] when the request does not contain one supported control
/// command.
pub fn parse_control_request(input: &str) -> Result<ControlRequest, ControlProtocolError> {
    match input.trim() {
        "status" => Ok(ControlRequest::Status),
        "stop" => Ok(ControlRequest::Stop),
        other => Err(ControlProtocolError::Remote(format!(
            "unknown control command `{other}`"
        ))),
    }
}

#[must_use]
pub fn encode_control_ack() -> String {
    String::from("status=ok\n")
}

#[must_use]
pub fn encode_control_error(message: &str) -> String {
    format!("status=error\nmessage={message}\n")
}

#[must_use]
pub fn encode_status_response(status: &ReplicaRuntimeStatus) -> String {
    let mut lines = vec![
        format!("status=ok"),
        format!("protocol_version={CONTROL_PROTOCOL_VERSION}"),
        format!("process_id={}", status.process_id),
        format!("replica_id={}", status.replica_id.get()),
        format!("state={}", encode_runtime_state(status.state)),
        format!("role={}", encode_role(status.role)),
        format!("current_view={}", status.current_view),
        format!("commit_lsn={}", encode_optional_lsn(status.commit_lsn)),
        format!(
            "active_snapshot_lsn={}",
            encode_optional_lsn(status.active_snapshot_lsn)
        ),
        format!(
            "accepting_writes={}",
            encode_optional_bool(status.accepting_writes)
        ),
        format!(
            "startup_kind={}",
            encode_optional_startup_kind(status.startup_kind)
        ),
        format!(
            "loaded_snapshot_lsn={}",
            encode_optional_lsn(status.loaded_snapshot_lsn)
        ),
        format!(
            "replayed_wal_frame_count={}",
            encode_optional_u32(status.replayed_wal_frame_count)
        ),
        format!(
            "replayed_wal_last_lsn={}",
            encode_optional_lsn(status.replayed_wal_last_lsn)
        ),
        format!(
            "fault_reason={}",
            status.fault_reason.as_deref().unwrap_or("none")
        ),
        format!("workspace_dir={}", status.workspace_dir.display()),
        format!("log_path={}", status.log_path.display()),
        format!("pid_path={}", status.pid_path.display()),
        format!("metadata_path={}", status.metadata_path.display()),
        format!("prepare_log_path={}", status.prepare_log_path.display()),
        format!("snapshot_path={}", status.snapshot_path.display()),
        format!("wal_path={}", status.wal_path.display()),
        format!("control_addr={}", status.control_addr),
        format!("client_addr={}", status.client_addr),
        format!("protocol_addr={}", status.protocol_addr),
    ];
    lines.push(String::new());
    lines.join("\n")
}

fn send_control_request(
    addr: SocketAddr,
    request: ControlRequest,
) -> Result<String, ControlProtocolError> {
    let mut stream = TcpStream::connect_timeout(&addr, CONTROL_IO_TIMEOUT)?;
    stream.set_read_timeout(Some(CONTROL_IO_TIMEOUT))?;
    stream.set_write_timeout(Some(CONTROL_IO_TIMEOUT))?;
    let request = match request {
        ControlRequest::Status => "status\n",
        ControlRequest::Stop => "stop\n",
    };
    stream.write_all(request.as_bytes())?;
    stream.shutdown(Shutdown::Write)?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    Ok(response)
}

fn decode_status_response(response: &str) -> Result<ReplicaRuntimeStatus, ControlProtocolError> {
    let fields = parse_key_value_lines(response).map_err(ControlProtocolError::from)?;
    match required_field(&fields, "status")? {
        "ok" => {}
        "error" => {
            let message = required_field(&fields, "message")?;
            return Err(ControlProtocolError::Remote(message.to_owned()));
        }
        other => {
            return Err(ControlProtocolError::InvalidField {
                field: String::from("status"),
                value: other.to_owned(),
            });
        }
    }

    Ok(ReplicaRuntimeStatus {
        process_id: parse_required_u32(&fields, "process_id")
            .map_err(ControlProtocolError::from)?,
        replica_id: ReplicaId(
            parse_required_u64(&fields, "replica_id").map_err(ControlProtocolError::from)?,
        ),
        state: parse_runtime_state(required_field(&fields, "state")?).map_err(|value| {
            ControlProtocolError::InvalidField {
                field: String::from("state"),
                value,
            }
        })?,
        role: parse_role(required_field(&fields, "role")?).map_err(|value| {
            ControlProtocolError::InvalidField {
                field: String::from("role"),
                value,
            }
        })?,
        current_view: parse_required_u64(&fields, "current_view")
            .map_err(ControlProtocolError::from)?,
        commit_lsn: parse_optional_lsn(&fields, "commit_lsn")
            .map_err(ControlProtocolError::from)?,
        active_snapshot_lsn: parse_optional_lsn(&fields, "active_snapshot_lsn")
            .map_err(ControlProtocolError::from)?,
        accepting_writes: parse_optional_bool(&fields, "accepting_writes")
            .map_err(ControlProtocolError::from)?,
        startup_kind: parse_optional_startup_kind(&fields, "startup_kind")
            .map_err(ControlProtocolError::from)?,
        loaded_snapshot_lsn: parse_optional_lsn(&fields, "loaded_snapshot_lsn")
            .map_err(ControlProtocolError::from)?,
        replayed_wal_frame_count: parse_optional_u32(&fields, "replayed_wal_frame_count")
            .map_err(ControlProtocolError::from)?,
        replayed_wal_last_lsn: parse_optional_lsn(&fields, "replayed_wal_last_lsn")
            .map_err(ControlProtocolError::from)?,
        fault_reason: parse_optional_string(&fields, "fault_reason"),
        workspace_dir: PathBuf::from(required_field(&fields, "workspace_dir")?),
        log_path: PathBuf::from(required_field(&fields, "log_path")?),
        pid_path: PathBuf::from(required_field(&fields, "pid_path")?),
        metadata_path: PathBuf::from(required_field(&fields, "metadata_path")?),
        prepare_log_path: PathBuf::from(required_field(&fields, "prepare_log_path")?),
        snapshot_path: PathBuf::from(required_field(&fields, "snapshot_path")?),
        wal_path: PathBuf::from(required_field(&fields, "wal_path")?),
        control_addr: parse_required_socket_addr(&fields, "control_addr")
            .map_err(ControlProtocolError::from)?,
        client_addr: parse_required_socket_addr(&fields, "client_addr")
            .map_err(ControlProtocolError::from)?,
        protocol_addr: parse_required_socket_addr(&fields, "protocol_addr")
            .map_err(ControlProtocolError::from)?,
    })
}

/// Decodes one text control-status payload returned by a replica daemon.
///
/// # Errors
///
/// Returns [`ControlProtocolError`] if the payload is malformed or contains one remote error.
pub fn decode_control_status_response(
    response: &str,
) -> Result<ReplicaRuntimeStatus, ControlProtocolError> {
    decode_status_response(response)
}

fn control_status_error_is_transient(error: &ControlProtocolError) -> bool {
    matches!(
        error,
        ControlProtocolError::Io(_)
            | ControlProtocolError::MissingField(_)
            | ControlProtocolError::InvalidLine(_)
            | ControlProtocolError::InvalidField { .. }
    )
}

fn encode_fault_state(state: &LocalClusterFaultState) -> String {
    let mut lines = vec![
        format!("version={CLUSTER_FAULT_STATE_VERSION}"),
        format!("workspace_root={}", state.workspace_root.display()),
    ];
    for replica_id in 1_u64..=LOCAL_CLUSTER_REPLICA_COUNT {
        let replica_id = ReplicaId(replica_id);
        lines.push(format!(
            "replica.{}.network={}",
            replica_id.get(),
            if state.is_replica_isolated(replica_id) {
                "isolated"
            } else {
                "healthy"
            }
        ));
    }
    lines.push(String::new());
    lines.join("\n")
}

fn decode_fault_state(bytes: &str) -> Result<LocalClusterFaultState, LocalClusterFaultStateError> {
    let fields = parse_key_value_lines(bytes).map_err(LocalClusterFaultStateError::Decode)?;
    let version =
        parse_required_u32(&fields, "version").map_err(LocalClusterFaultStateError::Decode)?;
    if version != CLUSTER_FAULT_STATE_VERSION {
        return Err(LocalClusterFaultStateError::Decode(
            LocalClusterLayoutError::UnsupportedVersion(version),
        ));
    }

    let workspace_root = PathBuf::from(
        required_field(&fields, "workspace_root").map_err(LocalClusterFaultStateError::Decode)?,
    );
    let mut isolated_replicas = Vec::new();
    for replica_id in 1_u64..=LOCAL_CLUSTER_REPLICA_COUNT {
        let field = format!("replica.{replica_id}.network");
        match required_field(&fields, &field).map_err(LocalClusterFaultStateError::Decode)? {
            "healthy" => {}
            "isolated" => {
                isolated_replicas.push(ReplicaId(replica_id));
            }
            value => {
                return Err(LocalClusterFaultStateError::Decode(
                    LocalClusterLayoutError::InvalidField {
                        field,
                        value: value.to_owned(),
                    },
                ));
            }
        }
    }

    Ok(LocalClusterFaultState {
        workspace_root,
        isolated_replicas,
    })
}

fn persist_local_cluster_timeline(
    workspace_root: &Path,
    events: &[LocalClusterTimelineEvent],
) -> Result<(), LocalClusterTimelineError> {
    fs::create_dir_all(workspace_root)?;
    let bytes = encode_timeline(events);
    write_bytes_atomically(&timeline_path(workspace_root), bytes.as_bytes())?;
    Ok(())
}

fn encode_timeline(events: &[LocalClusterTimelineEvent]) -> String {
    let mut lines = vec![format!("version={CLUSTER_TIMELINE_VERSION}"), String::new()];
    for event in events {
        lines.push(format!("sequence={}", event.sequence));
        lines.push(format!("timestamp_millis={}", event.timestamp_millis));
        lines.push(format!("kind={}", encode_timeline_kind(event.kind)));
        lines.push(format!(
            "replica_id={}",
            event.replica_id.map_or_else(
                || String::from("none"),
                |replica_id| replica_id.get().to_string()
            )
        ));
        lines.push(format!(
            "detail={}",
            event.detail.as_deref().unwrap_or("none")
        ));
        lines.push(String::new());
    }
    lines.join("\n")
}

fn decode_timeline(
    bytes: &str,
) -> Result<Vec<LocalClusterTimelineEvent>, LocalClusterTimelineError> {
    let mut lines = bytes.lines().peekable();
    let Some(version_line) = lines.next() else {
        return Ok(Vec::new());
    };
    let Some((version_key, version_value)) = version_line.split_once('=') else {
        return Err(LocalClusterTimelineError::Decode(
            LocalClusterLayoutError::InvalidLine(version_line.to_owned()),
        ));
    };
    if version_key != "version" {
        return Err(LocalClusterTimelineError::Decode(
            LocalClusterLayoutError::InvalidLine(version_line.to_owned()),
        ));
    }
    let version = version_value.parse::<u32>().map_err(|_| {
        LocalClusterTimelineError::Decode(LocalClusterLayoutError::InvalidField {
            field: String::from("version"),
            value: version_value.to_owned(),
        })
    })?;
    if version != CLUSTER_TIMELINE_VERSION {
        return Err(LocalClusterTimelineError::Decode(
            LocalClusterLayoutError::UnsupportedVersion(version),
        ));
    }
    if lines.peek().is_some_and(|line| line.is_empty()) {
        lines.next();
    }

    let mut blocks = Vec::new();
    let mut current_block = Vec::new();
    for line in lines {
        if line.is_empty() {
            if !current_block.is_empty() {
                blocks.push(current_block.join("\n"));
                current_block.clear();
            }
            continue;
        }
        current_block.push(line.to_owned());
    }
    if !current_block.is_empty() {
        blocks.push(current_block.join("\n"));
    }

    blocks
        .into_iter()
        .map(|block| {
            let fields =
                parse_key_value_lines(&block).map_err(LocalClusterTimelineError::Decode)?;
            let sequence = parse_required_u64(&fields, "sequence")
                .map_err(LocalClusterTimelineError::Decode)?;
            let timestamp_millis = required_field(&fields, "timestamp_millis")
                .map_err(LocalClusterTimelineError::Decode)?
                .parse::<u128>()
                .map_err(|_| {
                    LocalClusterTimelineError::Decode(LocalClusterLayoutError::InvalidField {
                        field: String::from("timestamp_millis"),
                        value: fields["timestamp_millis"].clone(),
                    })
                })?;
            let kind = parse_timeline_kind(
                required_field(&fields, "kind").map_err(LocalClusterTimelineError::Decode)?,
            )
            .map_err(|value| {
                LocalClusterTimelineError::Decode(LocalClusterLayoutError::InvalidField {
                    field: String::from("kind"),
                    value,
                })
            })?;
            let replica_id = match required_field(&fields, "replica_id")
                .map_err(LocalClusterTimelineError::Decode)?
            {
                "none" => None,
                value => Some(ReplicaId(value.parse::<u64>().map_err(|_| {
                    LocalClusterTimelineError::Decode(LocalClusterLayoutError::InvalidField {
                        field: String::from("replica_id"),
                        value: value.to_owned(),
                    })
                })?)),
            };
            let detail = parse_optional_string(&fields, "detail").filter(|value| !value.is_empty());
            Ok(LocalClusterTimelineEvent {
                sequence,
                timestamp_millis,
                kind,
                replica_id,
                detail,
            })
        })
        .collect()
}

fn reserve_loopback_addr(
    seen_addrs: &mut HashSet<SocketAddr>,
) -> Result<SocketAddr, LocalClusterLayoutError> {
    loop {
        // This is intentionally only a local-runner helper: the address is reserved by probing a
        // loopback listener and then immediately released before the child process binds it for
        // real. That leaves a small TOCTOU window, which is acceptable for the local three-replica
        // runner but not for a production-grade allocator of externally visible listeners.
        let listener = TcpListener::bind(("127.0.0.1", 0))?;
        let addr = listener.local_addr()?;
        drop(listener);
        if seen_addrs.insert(addr) {
            return Ok(addr);
        }
    }
}

fn prepare_workspace_root(workspace_root: &Path) -> Result<PathBuf, LocalClusterLayoutError> {
    fs::create_dir_all(workspace_root)?;
    Ok(fs::canonicalize(workspace_root)?)
}

fn encode_layout(layout: &LocalClusterLayout) -> String {
    let mut lines = vec![
        format!("version={CLUSTER_LAYOUT_VERSION}"),
        format!("workspace_root={}", layout.workspace_root.display()),
        format!("current_view={}", layout.current_view),
        format!("replica_count={}", layout.replicas.len()),
        format!("core.shard_id={}", layout.core_config.shard_id),
        format!("core.max_resources={}", layout.core_config.max_resources),
        format!(
            "core.max_reservations={}",
            layout.core_config.max_reservations
        ),
        format!(
            "core.max_bundle_size={}",
            layout.core_config.max_bundle_size
        ),
        format!("core.max_operations={}", layout.core_config.max_operations),
        format!("core.max_ttl_slots={}", layout.core_config.max_ttl_slots),
        format!(
            "core.max_client_retry_window_slots={}",
            layout.core_config.max_client_retry_window_slots
        ),
        format!(
            "core.reservation_history_window_slots={}",
            layout.core_config.reservation_history_window_slots
        ),
        format!(
            "core.max_expiration_bucket_len={}",
            layout.core_config.max_expiration_bucket_len
        ),
        format!(
            "engine.max_submission_queue={}",
            layout.engine_config.max_submission_queue
        ),
        format!(
            "engine.max_command_bytes={}",
            layout.engine_config.max_command_bytes
        ),
        format!(
            "engine.max_expirations_per_tick={}",
            layout.engine_config.max_expirations_per_tick
        ),
    ];

    for replica in &layout.replicas {
        let prefix = format!("replica.{}", replica.replica_id.get());
        lines.push(format!("{prefix}.role={}", encode_role(replica.role)));
        lines.push(format!(
            "{prefix}.workspace_dir={}",
            replica.workspace_dir.display()
        ));
        lines.push(format!("{prefix}.log_path={}", replica.log_path.display()));
        lines.push(format!("{prefix}.pid_path={}", replica.pid_path.display()));
        lines.push(format!(
            "{prefix}.metadata_path={}",
            replica.paths.metadata_path.display()
        ));
        lines.push(format!(
            "{prefix}.snapshot_path={}",
            replica.paths.snapshot_path.display()
        ));
        lines.push(format!(
            "{prefix}.wal_path={}",
            replica.paths.wal_path.display()
        ));
        lines.push(format!("{prefix}.control_addr={}", replica.control_addr));
        lines.push(format!("{prefix}.client_addr={}", replica.client_addr));
        lines.push(format!("{prefix}.protocol_addr={}", replica.protocol_addr));
    }
    lines.push(String::new());
    lines.join("\n")
}

fn decode_layout(bytes: &str) -> Result<LocalClusterLayout, LocalClusterLayoutError> {
    let fields = parse_key_value_lines(bytes)?;
    let version = parse_required_u32(&fields, "version")?;
    if version != CLUSTER_LAYOUT_VERSION {
        return Err(LocalClusterLayoutError::UnsupportedVersion(version));
    }

    let replica_count = parse_required_u64(&fields, "replica_count")?;
    if replica_count != LOCAL_CLUSTER_REPLICA_COUNT {
        return Err(LocalClusterLayoutError::InvalidReplicaCount(replica_count));
    }

    let workspace_root = PathBuf::from(required_field(&fields, "workspace_root")?);
    let current_view = parse_required_u64(&fields, "current_view")?;
    let core_config = Config {
        shard_id: parse_required_u64(&fields, "core.shard_id")?,
        max_resources: parse_required_u32(&fields, "core.max_resources")?,
        max_reservations: parse_required_u32(&fields, "core.max_reservations")?,
        max_bundle_size: parse_u32_or_default(
            &fields,
            "core.max_bundle_size",
            DEFAULT_LOCAL_CLUSTER_MAX_BUNDLE_SIZE,
        )?,
        max_operations: parse_required_u32(&fields, "core.max_operations")?,
        max_ttl_slots: parse_required_u64(&fields, "core.max_ttl_slots")?,
        max_client_retry_window_slots: parse_required_u64(
            &fields,
            "core.max_client_retry_window_slots",
        )?,
        reservation_history_window_slots: parse_required_u64(
            &fields,
            "core.reservation_history_window_slots",
        )?,
        max_expiration_bucket_len: parse_required_u32(&fields, "core.max_expiration_bucket_len")?,
    };
    let engine_config = EngineConfig {
        max_submission_queue: parse_required_u32(&fields, "engine.max_submission_queue")?,
        max_command_bytes: parse_required_usize(&fields, "engine.max_command_bytes")?,
        max_expirations_per_tick: parse_required_u32(&fields, "engine.max_expirations_per_tick")?,
    };

    let replicas = (1_u64..=LOCAL_CLUSTER_REPLICA_COUNT)
        .map(|replica_id| {
            let prefix = format!("replica.{replica_id}");
            Ok(LocalClusterReplicaConfig {
                replica_id: ReplicaId(replica_id),
                role: parse_role(required_field(&fields, &format!("{prefix}.role"))?).map_err(
                    |value| LocalClusterLayoutError::InvalidField {
                        field: format!("{prefix}.role"),
                        value,
                    },
                )?,
                workspace_dir: PathBuf::from(required_field(
                    &fields,
                    &format!("{prefix}.workspace_dir"),
                )?),
                log_path: PathBuf::from(required_field(&fields, &format!("{prefix}.log_path"))?),
                pid_path: PathBuf::from(required_field(&fields, &format!("{prefix}.pid_path"))?),
                paths: ReplicaPaths::new(
                    PathBuf::from(required_field(&fields, &format!("{prefix}.metadata_path"))?),
                    PathBuf::from(required_field(&fields, &format!("{prefix}.snapshot_path"))?),
                    PathBuf::from(required_field(&fields, &format!("{prefix}.wal_path"))?),
                ),
                control_addr: parse_required_socket_addr(
                    &fields,
                    &format!("{prefix}.control_addr"),
                )?,
                client_addr: parse_required_socket_addr(&fields, &format!("{prefix}.client_addr"))?,
                protocol_addr: parse_required_socket_addr(
                    &fields,
                    &format!("{prefix}.protocol_addr"),
                )?,
            })
        })
        .collect::<Result<Vec<_>, LocalClusterLayoutError>>()?;

    Ok(LocalClusterLayout {
        workspace_root,
        current_view,
        core_config,
        engine_config,
        replicas,
    })
}

fn write_bytes_atomically(path: &Path, bytes: &[u8]) -> Result<(), std::io::Error> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let temp_path = temp_path(path);
    let mut file = File::create(&temp_path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&temp_path, path)?;
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn temp_path(path: &Path) -> PathBuf {
    let mut temp_path = path.to_path_buf();
    let extension = temp_path
        .extension()
        .and_then(|value| value.to_str())
        .map_or_else(|| String::from("tmp"), |value| format!("{value}.tmp"));
    temp_path.set_extension(extension);
    temp_path
}

fn parse_key_value_lines(input: &str) -> Result<BTreeMap<String, String>, LocalClusterLayoutError> {
    let mut fields = BTreeMap::new();
    for raw_line in input.lines() {
        let line = raw_line.trim_end();
        if line.is_empty() {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            return Err(LocalClusterLayoutError::InvalidLine(line.to_owned()));
        };
        if fields.insert(key.to_owned(), value.to_owned()).is_some() {
            return Err(LocalClusterLayoutError::DuplicateField(key.to_owned()));
        }
    }
    Ok(fields)
}

fn required_field<'a>(
    fields: &'a BTreeMap<String, String>,
    key: &str,
) -> Result<&'a str, LocalClusterLayoutError> {
    fields
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| LocalClusterLayoutError::MissingField(key.to_owned()))
}

fn parse_required_u32(
    fields: &BTreeMap<String, String>,
    key: &str,
) -> Result<u32, LocalClusterLayoutError> {
    parse_required(fields, key)
}

fn parse_u32_or_default(
    fields: &BTreeMap<String, String>,
    key: &str,
    default: u32,
) -> Result<u32, LocalClusterLayoutError> {
    match fields.get(key) {
        Some(value) => value
            .parse::<u32>()
            .map_err(|_| LocalClusterLayoutError::InvalidField {
                field: key.to_owned(),
                value: value.to_owned(),
            }),
        None => Ok(default),
    }
}

fn parse_required_u64(
    fields: &BTreeMap<String, String>,
    key: &str,
) -> Result<u64, LocalClusterLayoutError> {
    parse_required(fields, key)
}

fn parse_required_usize(
    fields: &BTreeMap<String, String>,
    key: &str,
) -> Result<usize, LocalClusterLayoutError> {
    parse_required(fields, key)
}

fn parse_required_socket_addr(
    fields: &BTreeMap<String, String>,
    key: &str,
) -> Result<SocketAddr, LocalClusterLayoutError> {
    let value = required_field(fields, key)?;
    value
        .parse::<SocketAddr>()
        .map_err(|_| LocalClusterLayoutError::InvalidField {
            field: key.to_owned(),
            value: value.to_owned(),
        })
}

fn parse_required<T>(
    fields: &BTreeMap<String, String>,
    key: &str,
) -> Result<T, LocalClusterLayoutError>
where
    T: std::str::FromStr,
{
    let value = required_field(fields, key)?;
    value
        .parse::<T>()
        .map_err(|_| LocalClusterLayoutError::InvalidField {
            field: key.to_owned(),
            value: value.to_owned(),
        })
}

#[must_use]
pub fn encode_role(role: ReplicaRole) -> &'static str {
    match role {
        ReplicaRole::Primary => "primary",
        ReplicaRole::Backup => "backup",
        ReplicaRole::ViewUncertain => "view_uncertain",
        ReplicaRole::Recovering => "recovering",
        ReplicaRole::Faulted => "faulted",
    }
}

fn parse_role(value: &str) -> Result<ReplicaRole, String> {
    match value {
        "primary" => Ok(ReplicaRole::Primary),
        "backup" => Ok(ReplicaRole::Backup),
        "view_uncertain" => Ok(ReplicaRole::ViewUncertain),
        "recovering" => Ok(ReplicaRole::Recovering),
        "faulted" => Ok(ReplicaRole::Faulted),
        _ => Err(value.to_owned()),
    }
}

fn encode_runtime_state(state: ReplicaRuntimeState) -> &'static str {
    match state {
        ReplicaRuntimeState::Active => "active",
        ReplicaRuntimeState::Faulted => "faulted",
    }
}

fn parse_runtime_state(value: &str) -> Result<ReplicaRuntimeState, String> {
    match value {
        "active" => Ok(ReplicaRuntimeState::Active),
        "faulted" => Ok(ReplicaRuntimeState::Faulted),
        _ => Err(value.to_owned()),
    }
}

fn encode_timeline_kind(kind: LocalClusterTimelineEventKind) -> &'static str {
    match kind {
        LocalClusterTimelineEventKind::ClusterStart => "cluster_start",
        LocalClusterTimelineEventKind::ClusterStop => "cluster_stop",
        LocalClusterTimelineEventKind::ReplicaCrash => "replica_crash",
        LocalClusterTimelineEventKind::ReplicaRestart => "replica_restart",
        LocalClusterTimelineEventKind::ReplicaIsolate => "replica_isolate",
        LocalClusterTimelineEventKind::ReplicaHeal => "replica_heal",
    }
}

fn parse_timeline_kind(value: &str) -> Result<LocalClusterTimelineEventKind, String> {
    match value {
        "cluster_start" => Ok(LocalClusterTimelineEventKind::ClusterStart),
        "cluster_stop" => Ok(LocalClusterTimelineEventKind::ClusterStop),
        "replica_crash" => Ok(LocalClusterTimelineEventKind::ReplicaCrash),
        "replica_restart" => Ok(LocalClusterTimelineEventKind::ReplicaRestart),
        "replica_isolate" => Ok(LocalClusterTimelineEventKind::ReplicaIsolate),
        "replica_heal" => Ok(LocalClusterTimelineEventKind::ReplicaHeal),
        _ => Err(value.to_owned()),
    }
}

fn validate_replica_id(replica_id: ReplicaId) -> Result<(), LocalClusterLayoutError> {
    if (1..=LOCAL_CLUSTER_REPLICA_COUNT).contains(&replica_id.get()) {
        Ok(())
    } else {
        Err(LocalClusterLayoutError::UnknownReplicaId(replica_id.get()))
    }
}

fn encode_optional_lsn(lsn: Option<Lsn>) -> String {
    lsn.map_or_else(|| String::from("none"), |value| value.get().to_string())
}

fn parse_optional_lsn(
    fields: &BTreeMap<String, String>,
    key: &str,
) -> Result<Option<Lsn>, LocalClusterLayoutError> {
    parse_optional_u64(fields, key).map(|value| value.map(Lsn))
}

fn encode_optional_bool(value: Option<bool>) -> &'static str {
    match value {
        Some(true) => "true",
        Some(false) => "false",
        None => "none",
    }
}

fn parse_optional_bool(
    fields: &BTreeMap<String, String>,
    key: &str,
) -> Result<Option<bool>, LocalClusterLayoutError> {
    match required_field(fields, key)? {
        "true" => Ok(Some(true)),
        "false" => Ok(Some(false)),
        "none" => Ok(None),
        other => Err(LocalClusterLayoutError::InvalidField {
            field: key.to_owned(),
            value: other.to_owned(),
        }),
    }
}

fn encode_optional_u32(value: Option<u32>) -> String {
    value.map_or_else(|| String::from("none"), |value| value.to_string())
}

fn parse_optional_u32(
    fields: &BTreeMap<String, String>,
    key: &str,
) -> Result<Option<u32>, LocalClusterLayoutError> {
    match required_field(fields, key)? {
        "none" => Ok(None),
        value => {
            value
                .parse::<u32>()
                .map(Some)
                .map_err(|_| LocalClusterLayoutError::InvalidField {
                    field: key.to_owned(),
                    value: value.to_owned(),
                })
        }
    }
}

fn parse_optional_u64(
    fields: &BTreeMap<String, String>,
    key: &str,
) -> Result<Option<u64>, LocalClusterLayoutError> {
    match required_field(fields, key)? {
        "none" => Ok(None),
        value => {
            value
                .parse::<u64>()
                .map(Some)
                .map_err(|_| LocalClusterLayoutError::InvalidField {
                    field: key.to_owned(),
                    value: value.to_owned(),
                })
        }
    }
}

fn parse_optional_string(fields: &BTreeMap<String, String>, key: &str) -> Option<String> {
    fields.get(key).and_then(|value| {
        if value == "none" {
            None
        } else {
            Some(value.clone())
        }
    })
}

fn encode_optional_startup_kind(kind: Option<RecoveryStartupKind>) -> &'static str {
    match kind {
        Some(RecoveryStartupKind::FreshStart) => "fresh_start",
        Some(RecoveryStartupKind::WalOnly) => "wal_only",
        Some(RecoveryStartupKind::SnapshotOnly) => "snapshot_only",
        Some(RecoveryStartupKind::SnapshotAndWal) => "snapshot_and_wal",
        None => "none",
    }
}

fn parse_optional_startup_kind(
    fields: &BTreeMap<String, String>,
    key: &str,
) -> Result<Option<RecoveryStartupKind>, LocalClusterLayoutError> {
    match required_field(fields, key)? {
        "fresh_start" => Ok(Some(RecoveryStartupKind::FreshStart)),
        "wal_only" => Ok(Some(RecoveryStartupKind::WalOnly)),
        "snapshot_only" => Ok(Some(RecoveryStartupKind::SnapshotOnly)),
        "snapshot_and_wal" => Ok(Some(RecoveryStartupKind::SnapshotAndWal)),
        "none" => Ok(None),
        other => Err(LocalClusterLayoutError::InvalidField {
            field: key.to_owned(),
            value: other.to_owned(),
        }),
    }
}

impl From<LocalClusterLayoutError> for ControlProtocolError {
    fn from(error: LocalClusterLayoutError) -> Self {
        match error {
            LocalClusterLayoutError::Io(error) => Self::Io(error),
            LocalClusterLayoutError::MissingField(field) => Self::MissingField(field),
            LocalClusterLayoutError::DuplicateField(field) => Self::DuplicateField(field),
            LocalClusterLayoutError::InvalidLine(line) => Self::InvalidLine(line),
            LocalClusterLayoutError::InvalidField { field, value } => {
                Self::InvalidField { field, value }
            }
            LocalClusterLayoutError::UnsupportedVersion(version) => Self::InvalidField {
                field: String::from("version"),
                value: version.to_string(),
            },
            LocalClusterLayoutError::InvalidReplicaCount(count) => Self::InvalidField {
                field: String::from("replica_count"),
                value: count.to_string(),
            },
            LocalClusterLayoutError::UnknownReplicaId(replica_id) => Self::InvalidField {
                field: String::from("replica_id"),
                value: replica_id.to_string(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::{Cell, RefCell};

    fn fixture_layout() -> LocalClusterLayout {
        LocalClusterLayout {
            workspace_root: PathBuf::from("/tmp/allocdb-local-cluster"),
            current_view: 1,
            core_config: default_local_cluster_core_config(),
            engine_config: default_local_cluster_engine_config(),
            replicas: vec![
                LocalClusterReplicaConfig {
                    replica_id: ReplicaId(1),
                    role: ReplicaRole::Primary,
                    workspace_dir: PathBuf::from("/tmp/allocdb-local-cluster/replica-1"),
                    log_path: PathBuf::from("/tmp/allocdb-local-cluster/logs/replica-1.log"),
                    pid_path: PathBuf::from("/tmp/allocdb-local-cluster/run/replica-1.pid"),
                    paths: ReplicaPaths::new(
                        "/tmp/allocdb-local-cluster/replica-1/replica.metadata",
                        "/tmp/allocdb-local-cluster/replica-1/state.snapshot",
                        "/tmp/allocdb-local-cluster/replica-1/state.wal",
                    ),
                    control_addr: "127.0.0.1:18001".parse().unwrap(),
                    client_addr: "127.0.0.1:19001".parse().unwrap(),
                    protocol_addr: "127.0.0.1:20001".parse().unwrap(),
                },
                LocalClusterReplicaConfig {
                    replica_id: ReplicaId(2),
                    role: ReplicaRole::Backup,
                    workspace_dir: PathBuf::from("/tmp/allocdb-local-cluster/replica-2"),
                    log_path: PathBuf::from("/tmp/allocdb-local-cluster/logs/replica-2.log"),
                    pid_path: PathBuf::from("/tmp/allocdb-local-cluster/run/replica-2.pid"),
                    paths: ReplicaPaths::new(
                        "/tmp/allocdb-local-cluster/replica-2/replica.metadata",
                        "/tmp/allocdb-local-cluster/replica-2/state.snapshot",
                        "/tmp/allocdb-local-cluster/replica-2/state.wal",
                    ),
                    control_addr: "127.0.0.1:18002".parse().unwrap(),
                    client_addr: "127.0.0.1:19002".parse().unwrap(),
                    protocol_addr: "127.0.0.1:20002".parse().unwrap(),
                },
                LocalClusterReplicaConfig {
                    replica_id: ReplicaId(3),
                    role: ReplicaRole::Backup,
                    workspace_dir: PathBuf::from("/tmp/allocdb-local-cluster/replica-3"),
                    log_path: PathBuf::from("/tmp/allocdb-local-cluster/logs/replica-3.log"),
                    pid_path: PathBuf::from("/tmp/allocdb-local-cluster/run/replica-3.pid"),
                    paths: ReplicaPaths::new(
                        "/tmp/allocdb-local-cluster/replica-3/replica.metadata",
                        "/tmp/allocdb-local-cluster/replica-3/state.snapshot",
                        "/tmp/allocdb-local-cluster/replica-3/state.wal",
                    ),
                    control_addr: "127.0.0.1:18003".parse().unwrap(),
                    client_addr: "127.0.0.1:19003".parse().unwrap(),
                    protocol_addr: "127.0.0.1:20003".parse().unwrap(),
                },
            ],
        }
    }

    fn fixture_status() -> ReplicaRuntimeStatus {
        ReplicaRuntimeStatus {
            process_id: 42,
            replica_id: ReplicaId(1),
            state: ReplicaRuntimeState::Active,
            role: ReplicaRole::Primary,
            current_view: 7,
            commit_lsn: Some(Lsn(9)),
            active_snapshot_lsn: Some(Lsn(8)),
            accepting_writes: Some(true),
            startup_kind: Some(RecoveryStartupKind::SnapshotAndWal),
            loaded_snapshot_lsn: Some(Lsn(8)),
            replayed_wal_frame_count: Some(3),
            replayed_wal_last_lsn: Some(Lsn(9)),
            fault_reason: None,
            workspace_dir: PathBuf::from("/tmp/allocdb-local-cluster/replica-1"),
            log_path: PathBuf::from("/tmp/allocdb-local-cluster/logs/replica-1.log"),
            pid_path: PathBuf::from("/tmp/allocdb-local-cluster/run/replica-1.pid"),
            metadata_path: PathBuf::from("/tmp/allocdb-local-cluster/replica-1/replica.metadata"),
            prepare_log_path: PathBuf::from(
                "/tmp/allocdb-local-cluster/replica-1/replica.metadata.prepare",
            ),
            snapshot_path: PathBuf::from("/tmp/allocdb-local-cluster/replica-1/state.snapshot"),
            wal_path: PathBuf::from("/tmp/allocdb-local-cluster/replica-1/state.wal"),
            control_addr: "127.0.0.1:18001".parse().unwrap(),
            client_addr: "127.0.0.1:19001".parse().unwrap(),
            protocol_addr: "127.0.0.1:20001".parse().unwrap(),
        }
    }

    #[test]
    fn layout_round_trips_through_text_encoding() {
        let layout = fixture_layout();
        let encoded = encode_layout(&layout);
        let decoded = decode_layout(&encoded).unwrap();
        assert_eq!(decoded, layout);
    }

    #[test]
    fn decode_layout_defaults_missing_bundle_limit_for_legacy_payloads() {
        let layout = fixture_layout();
        let encoded = encode_layout(&layout);
        let legacy_encoded = encoded
            .lines()
            .filter(|line| !line.starts_with("core.max_bundle_size="))
            .collect::<Vec<_>>()
            .join("\n");

        let decoded = decode_layout(&legacy_encoded).unwrap();
        assert_eq!(
            decoded.core_config.max_bundle_size,
            DEFAULT_LOCAL_CLUSTER_MAX_BUNDLE_SIZE
        );
        assert_eq!(decoded, layout);
    }

    #[test]
    fn status_response_round_trips_through_text_encoding() {
        let status = fixture_status();

        let encoded = encode_status_response(&status);
        let decoded = decode_status_response(&encoded).unwrap();
        assert_eq!(decoded, status);
    }

    #[test]
    fn decode_control_status_response_round_trips_through_text_encoding() {
        let status = fixture_status();

        let encoded = encode_status_response(&status);
        let decoded = decode_control_status_response(&encoded).unwrap();
        assert_eq!(decoded, status);
    }

    #[test]
    fn decode_control_status_response_surfaces_remote_error() {
        let error = decode_control_status_response("status=error\nmessage=replica unavailable\n")
            .unwrap_err();
        assert!(matches!(
            error,
            ControlProtocolError::Remote(message) if message == "replica unavailable"
        ));
    }

    #[test]
    fn fault_state_round_trips_through_text_encoding() {
        let state = LocalClusterFaultState {
            workspace_root: PathBuf::from("/tmp/allocdb-local-cluster"),
            isolated_replicas: vec![ReplicaId(1), ReplicaId(3)],
        };

        let encoded = encode_fault_state(&state);
        let decoded = decode_fault_state(&encoded).unwrap();
        assert_eq!(decoded, state);
        assert!(decoded.is_replica_isolated(ReplicaId(1)));
        assert!(!decoded.is_replica_isolated(ReplicaId(2)));
    }

    #[test]
    fn timeline_round_trips_through_text_encoding() {
        let timeline = vec![
            LocalClusterTimelineEvent {
                sequence: 1,
                timestamp_millis: 11,
                kind: LocalClusterTimelineEventKind::ClusterStart,
                replica_id: None,
                detail: Some(String::from("cluster_started")),
            },
            LocalClusterTimelineEvent {
                sequence: 2,
                timestamp_millis: 12,
                kind: LocalClusterTimelineEventKind::ReplicaIsolate,
                replica_id: Some(ReplicaId(1)),
                detail: Some(String::from("scope=client+protocol")),
            },
        ];

        let encoded = encode_timeline(&timeline);
        let decoded = decode_timeline(&encoded).unwrap();
        assert_eq!(decoded, timeline);
    }

    #[test]
    fn control_command_parser_rejects_unknown_requests() {
        let error = parse_control_request("restart").unwrap_err();
        assert!(
            matches!(error, ControlProtocolError::Remote(message) if message.contains("restart"))
        );
    }

    #[test]
    fn request_control_status_retries_transient_decode_error_then_succeeds() {
        let addr = fixture_status().control_addr;
        let status = fixture_status();
        let encoded = encode_status_response(&status);
        let attempts = Cell::new(0_usize);
        let delays = RefCell::new(Vec::new());

        let result = request_control_status_with(
            addr,
            |request_addr, request| {
                assert_eq!(request_addr, addr);
                assert_eq!(request, ControlRequest::Status);
                let attempt = attempts.get();
                attempts.set(attempt.saturating_add(1));
                if attempt == 0 {
                    Ok(String::from("status=ok\nprocess_id=42\n"))
                } else {
                    Ok(encoded.clone())
                }
            },
            |delay| delays.borrow_mut().push(delay),
        )
        .unwrap();

        assert_eq!(result, status);
        assert_eq!(attempts.get(), 2);
        assert_eq!(delays.borrow().as_slice(), &[CONTROL_STATUS_RETRY_DELAY]);
        assert!(attempts.get() < CONTROL_STATUS_MAX_ATTEMPTS);
    }

    #[test]
    fn request_control_status_returns_non_transient_error_without_retry() {
        let addr = fixture_status().control_addr;
        let attempts = Cell::new(0_usize);
        let delays = RefCell::new(Vec::new());

        let error = request_control_status_with(
            addr,
            |request_addr, request| {
                assert_eq!(request_addr, addr);
                assert_eq!(request, ControlRequest::Status);
                attempts.set(attempts.get().saturating_add(1));
                Err(ControlProtocolError::Remote(String::from(
                    "replica refused status",
                )))
            },
            |delay| delays.borrow_mut().push(delay),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            ControlProtocolError::Remote(message) if message == "replica refused status"
        ));
        assert_eq!(attempts.get(), 1);
        assert!(delays.borrow().is_empty());
    }

    #[test]
    fn request_control_status_retries_truncated_field_value_then_succeeds() {
        let addr = fixture_status().control_addr;
        let status = fixture_status();
        let encoded = encode_status_response(&status);
        let attempts = Cell::new(0_usize);
        let delays = RefCell::new(Vec::new());

        let error_response = concat!(
            "status=ok\n",
            "protocol_version=1\n",
            "process_id=42\n",
            "replica_id=1\n",
            "state=active\n",
            "role=primary\n",
            "current_view=7\n",
            "commit_lsn=9\n",
            "active_snapshot_lsn=8\n",
            "accepting_writes=true\n",
            "startup_kind=snapshot_and_wal\n",
            "loaded_snapshot_lsn=8\n",
            "replayed_wal_frame_count=3\n",
            "replayed_wal_last_lsn=9\n",
            "fault_reason=none\n",
            "workspace_dir=/tmp/allocdb-local-cluster/replica-1\n",
            "log_path=/tmp/allocdb-local-cluster/logs/replica-1.log\n",
            "pid_path=/tmp/allocdb-local-cluster/run/replica-1.pid\n",
            "metadata_path=/tmp/allocdb-local-cluster/replica-1/replica.metadata\n",
            "prepare_log_path=/tmp/allocdb-local-cluster/replica-1/replica.metadata.prepare\n",
            "snapshot_path=/tmp/allocdb-local-cluster/replica-1/state.snapshot\n",
            "wal_path=/tmp/allocdb-local-cluster/replica-1/state.wal\n",
            "control_addr=127.0.0.\n",
            "client_addr=127.0.0.1:19001\n",
            "protocol_addr=127.0.0.1:20001\n",
        );
        let result = request_control_status_with(
            addr,
            |request_addr, request| {
                assert_eq!(request_addr, addr);
                assert_eq!(request, ControlRequest::Status);
                let attempt = attempts.get();
                attempts.set(attempt.saturating_add(1));
                if attempt == 0 {
                    Ok(String::from(error_response))
                } else {
                    Ok(encoded.clone())
                }
            },
            |delay| delays.borrow_mut().push(delay),
        )
        .unwrap();

        assert_eq!(result, status);
        assert_eq!(attempts.get(), 2);
        assert_eq!(delays.borrow().as_slice(), &[CONTROL_STATUS_RETRY_DELAY]);
    }
}

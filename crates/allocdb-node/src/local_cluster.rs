use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::time::Duration;

use allocdb_core::config::Config;
use allocdb_core::ids::Lsn;

use crate::engine::{EngineConfig, RecoveryStartupKind};
use crate::replica::{ReplicaId, ReplicaPaths, ReplicaRole};

const CLUSTER_LAYOUT_VERSION: u32 = 1;
const LOCAL_CLUSTER_REPLICA_COUNT: u64 = 3;
const LOCAL_CLUSTER_INITIAL_VIEW: u64 = 1;
const CONTROL_PROTOCOL_VERSION: u32 = 1;
const CLUSTER_LAYOUT_FILE_NAME: &str = "cluster-layout.txt";
const REPLICA_LOG_DIR_NAME: &str = "logs";
const REPLICA_RUN_DIR_NAME: &str = "run";
const REPLICA_METADATA_FILE_NAME: &str = "replica.metadata";
const REPLICA_SNAPSHOT_FILE_NAME: &str = "state.snapshot";
const REPLICA_WAL_FILE_NAME: &str = "state.wal";
const CONTROL_IO_TIMEOUT: Duration = Duration::from_millis(250);

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

#[must_use]
pub fn default_local_cluster_core_config() -> Config {
    Config {
        shard_id: 0,
        max_resources: 1_024,
        max_reservations: 1_024,
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

/// Sends one `status` request to a running replica control socket.
///
/// # Errors
///
/// Returns [`ControlProtocolError`] if the socket cannot be reached, if the response cannot be
/// read, or if the returned status payload is malformed.
pub fn request_control_status(
    addr: SocketAddr,
) -> Result<ReplicaRuntimeStatus, ControlProtocolError> {
    let response = send_control_request(addr, ControlRequest::Status)?;
    decode_status_response(&response)
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

    #[test]
    fn layout_round_trips_through_text_encoding() {
        let layout = fixture_layout();
        let encoded = encode_layout(&layout);
        let decoded = decode_layout(&encoded).unwrap();
        assert_eq!(decoded, layout);
    }

    #[test]
    fn status_response_round_trips_through_text_encoding() {
        let status = ReplicaRuntimeStatus {
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
        };

        let encoded = encode_status_response(&status);
        let decoded = decode_status_response(&encoded).unwrap();
        assert_eq!(decoded, status);
    }

    #[test]
    fn control_command_parser_rejects_unknown_requests() {
        let error = parse_control_request("restart").unwrap_err();
        assert!(
            matches!(error, ControlProtocolError::Remote(message) if message.contains("restart"))
        );
    }
}

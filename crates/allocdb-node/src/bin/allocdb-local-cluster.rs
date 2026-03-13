use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{self, Child, Command, ExitCode, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use allocdb_core::ids::Slot;
use allocdb_node::local_cluster::{
    ControlRequest, LocalClusterLayout, LocalClusterReplicaConfig, ReplicaRuntimeState,
    ReplicaRuntimeStatus, encode_control_ack, encode_control_error, encode_role,
    encode_status_response, layout_path, parse_control_request, request_control_status,
    request_control_stop,
};
use allocdb_node::replica::{
    ReplicaId, ReplicaIdentity, ReplicaNode, ReplicaNodeStatus, ReplicaRole,
};

const STARTUP_TIMEOUT: Duration = Duration::from_secs(5);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const LISTENER_POLL_INTERVAL: Duration = Duration::from_millis(50);

enum ParsedCommand {
    Help,
    Start {
        workspace_root: PathBuf,
    },
    Stop {
        workspace_root: PathBuf,
    },
    Status {
        workspace_root: PathBuf,
    },
    ReplicaDaemon {
        layout_file: PathBuf,
        replica_id: ReplicaId,
    },
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("{message}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    match parse_args(std::env::args().skip(1))? {
        ParsedCommand::Help => {
            print!("{}", usage());
            Ok(())
        }
        ParsedCommand::Start { workspace_root } => start_cluster(&workspace_root),
        ParsedCommand::Stop { workspace_root } => stop_cluster(&workspace_root),
        ParsedCommand::Status { workspace_root } => status_cluster(&workspace_root),
        ParsedCommand::ReplicaDaemon {
            layout_file,
            replica_id,
        } => run_replica_daemon(&layout_file, replica_id),
    }
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<ParsedCommand, String> {
    let mut args = args.into_iter();
    let Some(subcommand) = args.next() else {
        return Ok(ParsedCommand::Help);
    };

    match subcommand.as_str() {
        "--help" | "-h" => Ok(ParsedCommand::Help),
        "start" => Ok(ParsedCommand::Start {
            workspace_root: parse_workspace_flag(args)?,
        }),
        "stop" => Ok(ParsedCommand::Stop {
            workspace_root: parse_workspace_flag(args)?,
        }),
        "status" => Ok(ParsedCommand::Status {
            workspace_root: parse_workspace_flag(args)?,
        }),
        "replica-daemon" => parse_replica_daemon_args(args),
        other => Err(format!("unknown subcommand `{other}`\n\n{}", usage())),
    }
}

fn parse_workspace_flag(args: impl IntoIterator<Item = String>) -> Result<PathBuf, String> {
    let mut args = args.into_iter();
    let mut workspace_root = None;
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--workspace" => {
                let value = args.next().ok_or_else(usage)?;
                workspace_root = Some(PathBuf::from(value));
            }
            "--help" | "-h" => return Err(usage()),
            other => return Err(format!("unknown argument `{other}`\n\n{}", usage())),
        }
    }

    workspace_root.ok_or_else(usage)
}

fn parse_replica_daemon_args(
    args: impl IntoIterator<Item = String>,
) -> Result<ParsedCommand, String> {
    let mut args = args.into_iter();
    let mut layout_file = None;
    let mut replica_id = None;
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--layout-file" => {
                let value = args.next().ok_or_else(usage)?;
                layout_file = Some(PathBuf::from(value));
            }
            "--replica-id" => {
                let value = args.next().ok_or_else(usage)?;
                let parsed = value.parse::<u64>().map_err(|_| {
                    format!("invalid value for `--replica-id`: `{value}`\n\n{}", usage())
                })?;
                replica_id = Some(ReplicaId(parsed));
            }
            other => return Err(format!("unknown argument `{other}`\n\n{}", usage())),
        }
    }

    Ok(ParsedCommand::ReplicaDaemon {
        layout_file: layout_file.ok_or_else(usage)?,
        replica_id: replica_id.ok_or_else(usage)?,
    })
}

fn start_cluster(workspace_root: &Path) -> Result<(), String> {
    let layout = LocalClusterLayout::load_or_create(workspace_root)
        .map_err(|error| format!("failed to prepare local cluster layout: {error}"))?;
    ensure_cluster_not_running(&layout)?;
    clear_stale_pid_files(&layout)?;

    let current_exe = std::env::current_exe()
        .map_err(|error| format!("failed to locate current executable: {error}"))?;
    let layout_file = layout.layout_path();
    let mut children = Vec::new();
    let mut statuses = Vec::new();

    for replica in &layout.replicas {
        let child = spawn_replica_daemon(&current_exe, &layout_file, replica)?;
        children.push((replica.replica_id, child));
    }

    for (replica_id, child) in &mut children {
        let replica = layout
            .replica(*replica_id)
            .ok_or_else(|| format!("missing replica {} in layout", replica_id.get()))?;
        match wait_for_replica_ready(replica, child) {
            Ok(status) => statuses.push(status),
            Err(error) => {
                stop_spawned_children(&layout, &mut children);
                return Err(error);
            }
        }
    }

    println!("cluster started");
    println!("workspace={}", layout.workspace_root.display());
    println!("layout={}", layout.layout_path().display());
    for status in &statuses {
        print_live_status(status);
    }
    Ok(())
}

fn stop_cluster(workspace_root: &Path) -> Result<(), String> {
    let layout = load_existing_layout(workspace_root)?;
    let mut saw_running_replica = false;
    for replica in &layout.replicas {
        match request_control_stop(replica.control_addr) {
            Ok(()) => {
                saw_running_replica = true;
                println!(
                    "replica={} stop_requested control={}",
                    replica.replica_id.get(),
                    replica.control_addr
                );
            }
            Err(_) if replica.pid_path.exists() => {
                return Err(format!(
                    "replica {} did not answer on {} but still has pid file at {}",
                    replica.replica_id.get(),
                    replica.control_addr,
                    replica.pid_path.display()
                ));
            }
            Err(_) => {
                println!(
                    "replica={} already_stopped control={}",
                    replica.replica_id.get(),
                    replica.control_addr
                );
            }
        }
    }

    wait_for_cluster_shutdown(&layout)?;
    if saw_running_replica {
        println!("cluster stopped");
    }
    Ok(())
}

fn status_cluster(workspace_root: &Path) -> Result<(), String> {
    let layout = load_existing_layout(workspace_root)?;
    println!("workspace={}", layout.workspace_root.display());
    println!("layout={}", layout.layout_path().display());
    for replica in &layout.replicas {
        match request_control_status(replica.control_addr) {
            Ok(status) => print_live_status(&status),
            Err(_) if replica.pid_path.exists() => println!(
                "replica={} state=unreachable control={} pid_path={} log={}",
                replica.replica_id.get(),
                replica.control_addr,
                replica.pid_path.display(),
                replica.log_path.display()
            ),
            Err(_) => println!(
                "replica={} state=stopped control={} log={}",
                replica.replica_id.get(),
                replica.control_addr,
                replica.log_path.display()
            ),
        }
    }
    Ok(())
}

fn run_replica_daemon(layout_file: &Path, replica_id: ReplicaId) -> Result<(), String> {
    let layout = LocalClusterLayout::load(layout_file)
        .map_err(|error| format!("failed to load local cluster layout: {error}"))?;
    let replica = layout.replica(replica_id).ok_or_else(|| {
        format!(
            "replica {} is not present in {}",
            replica_id.get(),
            layout_file.display()
        )
    })?;
    fs::create_dir_all(&replica.workspace_dir)
        .map_err(|error| format!("failed to create replica workspace: {error}"))?;
    if let Some(parent) = replica.pid_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("failed to create pid directory: {error}"))?;
    }

    eprintln!(
        "replica={} workspace={} control={} client={} protocol={} startup=begin",
        replica.replica_id.get(),
        replica.workspace_dir.display(),
        replica.control_addr,
        replica.client_addr,
        replica.protocol_addr
    );

    let identity = ReplicaIdentity {
        replica_id,
        shard_id: layout.core_config.shard_id,
    };
    let mut node = ReplicaNode::recover(
        layout.core_config.clone(),
        layout.engine_config,
        identity,
        replica.paths.clone(),
    )
    .map_err(|error| format!("failed to recover replica {}: {error:?}", replica_id.get()))?;
    if node.status() == ReplicaNodeStatus::Active
        && matches!(node.metadata().role, ReplicaRole::Recovering)
    {
        node.configure_normal_role(layout.current_view, replica.role)
            .map_err(|error| {
                format!(
                    "failed to enter normal mode for replica {}: {error:?}",
                    replica_id.get()
                )
            })?;
    }

    write_pid_file(&replica.pid_path, process::id())?;
    let pid_guard = PidFileGuard::new(replica.pid_path.clone());

    let control_listener = bind_listener(replica.control_addr)?;
    let client_listener = bind_listener(replica.client_addr)?;
    let protocol_listener = bind_listener(replica.protocol_addr)?;
    eprintln!(
        "replica={} startup=ready status={}",
        replica.replica_id.get(),
        describe_node_status(node.status())
    );

    let loop_result = replica_event_loop(
        &mut node,
        replica,
        &control_listener,
        &client_listener,
        &protocol_listener,
    );
    drop(pid_guard);
    eprintln!(
        "replica={} shutdown={}",
        replica.replica_id.get(),
        if loop_result.is_ok() {
            "clean"
        } else {
            "error"
        }
    );
    loop_result
}

fn replica_event_loop(
    node: &mut ReplicaNode,
    replica: &LocalClusterReplicaConfig,
    control_listener: &TcpListener,
    client_listener: &TcpListener,
    protocol_listener: &TcpListener,
) -> Result<(), String> {
    loop {
        let mut handled_work = false;
        if let Some(stream) = accept_nonblocking(control_listener)? {
            handled_work = true;
            if handle_control_stream(stream, node, replica)? {
                return Ok(());
            }
        }
        if let Some(stream) = accept_nonblocking(client_listener)? {
            handled_work = true;
            let response = encode_control_error("client transport not implemented");
            write_response(stream, &response)?;
        }
        if let Some(stream) = accept_nonblocking(protocol_listener)? {
            handled_work = true;
            let response = encode_control_error("protocol transport not implemented");
            write_response(stream, &response)?;
        }
        if !handled_work {
            thread::sleep(LISTENER_POLL_INTERVAL);
        }
    }
}

fn handle_control_stream(
    mut stream: TcpStream,
    node: &mut ReplicaNode,
    replica: &LocalClusterReplicaConfig,
) -> Result<bool, String> {
    let mut request = String::new();
    // The control protocol is one request per connection. request_control_* half-closes the write
    // side after sending the command, so reading to EOF here yields the complete request body.
    stream
        .read_to_string(&mut request)
        .map_err(|error| format!("failed to read control request: {error}"))?;
    let parsed = parse_control_request(&request);
    match parsed {
        Ok(ControlRequest::Status) => {
            let response = encode_status_response(&build_runtime_status(node, replica));
            write_response(stream, &response)?;
            Ok(false)
        }
        Ok(ControlRequest::Stop) => {
            let response = encode_control_ack();
            write_response(stream, &response)?;
            Ok(true)
        }
        Err(error) => {
            let response = encode_control_error(&error.to_string());
            write_response(stream, &response)?;
            Ok(false)
        }
    }
}

fn build_runtime_status(
    node: &ReplicaNode,
    replica: &LocalClusterReplicaConfig,
) -> ReplicaRuntimeStatus {
    let metadata = *node.metadata();
    let (
        accepting_writes,
        startup_kind,
        loaded_snapshot_lsn,
        replayed_wal_frame_count,
        replayed_wal_last_lsn,
    ) = if let Some(engine) = node.engine() {
        let metrics = engine.metrics(Slot(0));
        (
            Some(metrics.accepting_writes),
            Some(metrics.recovery.startup_kind),
            metrics.recovery.loaded_snapshot_lsn,
            Some(metrics.recovery.replayed_wal_frame_count),
            metrics.recovery.replayed_wal_last_lsn,
        )
    } else {
        (None, None, None, None, None)
    };
    let fault_reason = match node.status() {
        ReplicaNodeStatus::Active => None,
        ReplicaNodeStatus::Faulted(fault) => Some(format!("{:?}", fault.reason)),
    };

    ReplicaRuntimeStatus {
        process_id: process::id(),
        replica_id: replica.replica_id,
        state: if node.status() == ReplicaNodeStatus::Active {
            ReplicaRuntimeState::Active
        } else {
            ReplicaRuntimeState::Faulted
        },
        role: metadata.role,
        current_view: metadata.current_view,
        commit_lsn: metadata.commit_lsn,
        active_snapshot_lsn: metadata.active_snapshot_lsn,
        accepting_writes,
        startup_kind,
        loaded_snapshot_lsn,
        replayed_wal_frame_count,
        replayed_wal_last_lsn,
        fault_reason,
        workspace_dir: replica.workspace_dir.clone(),
        log_path: replica.log_path.clone(),
        pid_path: replica.pid_path.clone(),
        metadata_path: replica.paths.metadata_path.clone(),
        prepare_log_path: node.prepare_log_path().to_path_buf(),
        snapshot_path: replica.paths.snapshot_path.clone(),
        wal_path: replica.paths.wal_path.clone(),
        control_addr: replica.control_addr,
        client_addr: replica.client_addr,
        protocol_addr: replica.protocol_addr,
    }
}

fn load_existing_layout(workspace_root: &Path) -> Result<LocalClusterLayout, String> {
    let workspace_root = fs::canonicalize(workspace_root).map_err(|error| {
        format!(
            "failed to resolve workspace {}: {error}",
            workspace_root.display()
        )
    })?;
    LocalClusterLayout::load(layout_path(&workspace_root))
        .map_err(|error| format!("failed to load local cluster layout: {error}"))
}

fn ensure_cluster_not_running(layout: &LocalClusterLayout) -> Result<(), String> {
    for replica in &layout.replicas {
        if request_control_status(replica.control_addr).is_ok() {
            return Err(format!(
                "replica {} is already running on {}",
                replica.replica_id.get(),
                replica.control_addr
            ));
        }
    }
    Ok(())
}

fn clear_stale_pid_files(layout: &LocalClusterLayout) -> Result<(), String> {
    for replica in &layout.replicas {
        if replica.pid_path.exists() {
            fs::remove_file(&replica.pid_path).map_err(|error| {
                format!(
                    "failed to remove stale pid file {}: {error}",
                    replica.pid_path.display()
                )
            })?;
        }
    }
    Ok(())
}

fn spawn_replica_daemon(
    current_exe: &Path,
    layout_file: &Path,
    replica: &LocalClusterReplicaConfig,
) -> Result<Child, String> {
    if let Some(parent) = replica.log_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("failed to create log directory: {error}"))?;
    }
    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&replica.log_path)
        .map_err(|error| {
            format!(
                "failed to open replica log {}: {error}",
                replica.log_path.display()
            )
        })?;
    let stderr_file = log_file
        .try_clone()
        .map_err(|error| format!("failed to clone log handle: {error}"))?;

    Command::new(current_exe)
        .arg("replica-daemon")
        .arg("--layout-file")
        .arg(layout_file)
        .arg("--replica-id")
        .arg(replica.replica_id.get().to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::from(log_file))
        .stderr(Stdio::from(stderr_file))
        .spawn()
        .map_err(|error| {
            format!(
                "failed to spawn replica {} daemon: {error}",
                replica.replica_id.get()
            )
        })
}

fn wait_for_replica_ready(
    replica: &LocalClusterReplicaConfig,
    child: &mut Child,
) -> Result<ReplicaRuntimeStatus, String> {
    let started = Instant::now();
    loop {
        if let Ok(status) = request_control_status(replica.control_addr) {
            return Ok(status);
        }
        if let Some(exit_status) = child
            .try_wait()
            .map_err(|error| format!("failed to inspect child process state: {error}"))?
        {
            return Err(format!(
                "replica {} exited before it became ready: {exit_status}",
                replica.replica_id.get()
            ));
        }
        if started.elapsed() >= STARTUP_TIMEOUT {
            return Err(format!(
                "replica {} did not become ready on {} within {:?}",
                replica.replica_id.get(),
                replica.control_addr,
                STARTUP_TIMEOUT
            ));
        }
        thread::sleep(LISTENER_POLL_INTERVAL);
    }
}

fn wait_for_cluster_shutdown(layout: &LocalClusterLayout) -> Result<(), String> {
    let started = Instant::now();
    loop {
        let mut all_stopped = true;
        for replica in &layout.replicas {
            if request_control_status(replica.control_addr).is_ok() || replica.pid_path.exists() {
                all_stopped = false;
                break;
            }
        }
        if all_stopped {
            return Ok(());
        }
        if started.elapsed() >= SHUTDOWN_TIMEOUT {
            return Err(format!("cluster did not stop within {SHUTDOWN_TIMEOUT:?}"));
        }
        thread::sleep(LISTENER_POLL_INTERVAL);
    }
}

fn stop_spawned_children(layout: &LocalClusterLayout, children: &mut [(ReplicaId, Child)]) {
    for replica in &layout.replicas {
        let _ = request_control_stop(replica.control_addr);
    }
    for (_, child) in children {
        if child.try_wait().ok().flatten().is_none() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn bind_listener(addr: std::net::SocketAddr) -> Result<TcpListener, String> {
    let listener =
        TcpListener::bind(addr).map_err(|error| format!("failed to bind {addr}: {error}"))?;
    listener
        .set_nonblocking(true)
        .map_err(|error| format!("failed to set listener nonblocking: {error}"))?;
    Ok(listener)
}

fn accept_nonblocking(listener: &TcpListener) -> Result<Option<TcpStream>, String> {
    match listener.accept() {
        Ok((stream, _addr)) => Ok(Some(stream)),
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
        Err(error) => Err(format!("failed to accept incoming connection: {error}")),
    }
}

fn write_response(mut stream: TcpStream, response: &str) -> Result<(), String> {
    stream
        .write_all(response.as_bytes())
        .map_err(|error| format!("failed to write response: {error}"))
}

fn write_pid_file(path: &Path, process_id: u32) -> Result<(), String> {
    fs::write(path, process_id.to_string())
        .map_err(|error| format!("failed to write pid file {}: {error}", path.display()))
}

fn describe_node_status(status: ReplicaNodeStatus) -> &'static str {
    match status {
        ReplicaNodeStatus::Active => "active",
        ReplicaNodeStatus::Faulted(_) => "faulted",
    }
}

fn print_live_status(status: &ReplicaRuntimeStatus) {
    println!(
        "replica={} state={} pid={} role={} view={} control={} client={} protocol={}",
        status.replica_id.get(),
        match status.state {
            ReplicaRuntimeState::Active => "active",
            ReplicaRuntimeState::Faulted => "faulted",
        },
        status.process_id,
        encode_role(status.role),
        status.current_view,
        status.control_addr,
        status.client_addr,
        status.protocol_addr
    );
    println!(
        "replica={} commit_lsn={} snapshot_lsn={} accepting_writes={} startup_kind={} log={}",
        status.replica_id.get(),
        display_optional_lsn(status.commit_lsn),
        display_optional_lsn(status.active_snapshot_lsn),
        display_optional_bool(status.accepting_writes),
        display_optional_startup_kind(status.startup_kind),
        status.log_path.display()
    );
    println!(
        "replica={} metadata={} prepare_log={} snapshot={} wal={} pid_path={}",
        status.replica_id.get(),
        status.metadata_path.display(),
        status.prepare_log_path.display(),
        status.snapshot_path.display(),
        status.wal_path.display(),
        status.pid_path.display()
    );
    if let Some(fault_reason) = &status.fault_reason {
        println!(
            "replica={} fault_reason={fault_reason}",
            status.replica_id.get()
        );
    }
}

fn display_optional_lsn(value: Option<allocdb_core::ids::Lsn>) -> String {
    value.map_or_else(|| String::from("none"), |value| value.get().to_string())
}

fn display_optional_bool(value: Option<bool>) -> &'static str {
    match value {
        Some(true) => "true",
        Some(false) => "false",
        None => "none",
    }
}

fn display_optional_startup_kind(
    value: Option<allocdb_node::engine::RecoveryStartupKind>,
) -> &'static str {
    match value {
        Some(allocdb_node::engine::RecoveryStartupKind::FreshStart) => "fresh_start",
        Some(allocdb_node::engine::RecoveryStartupKind::WalOnly) => "wal_only",
        Some(allocdb_node::engine::RecoveryStartupKind::SnapshotOnly) => "snapshot_only",
        Some(allocdb_node::engine::RecoveryStartupKind::SnapshotAndWal) => "snapshot_and_wal",
        None => "none",
    }
}

fn usage() -> String {
    String::from(
        "usage: cargo run -p allocdb-node --bin allocdb-local-cluster -- <start|stop|status> --workspace <path>\n\
         \n\
         commands:\n\
         \x20\x20start --workspace <path>\n\
         \x20\x20stop --workspace <path>\n\
         \x20\x20status --workspace <path>\n",
    )
}

struct PidFileGuard {
    path: PathBuf,
}

impl PidFileGuard {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl Drop for PidFileGuard {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_file(&self.path) {
            if error.kind() != std::io::ErrorKind::NotFound {
                eprintln!("failed to remove pid file {}: {error}", self.path.display());
            }
        }
    }
}

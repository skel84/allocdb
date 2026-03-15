use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{self, Child, Command, ExitCode, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use allocdb_core::ReservationLookupError;
use allocdb_core::command_codec::decode_client_request;
use allocdb_core::ids::{Lsn, Slot};
use allocdb_core::result::{CommandOutcome, ResultCode};
use allocdb_node::local_cluster::{
    ControlRequest, LocalClusterFaultState, LocalClusterLayout, LocalClusterReplicaConfig,
    LocalClusterTimelineEventKind, ReplicaRuntimeState, ReplicaRuntimeStatus,
    append_local_cluster_timeline_event, encode_control_ack, encode_control_error, encode_role,
    encode_status_response, fault_state_path, layout_path, load_local_cluster_timeline,
    parse_control_request, request_control_status, request_control_stop, timeline_path,
};
use allocdb_node::replica::{
    ReplicaId, ReplicaIdentity, ReplicaNode, ReplicaNodeStatus, ReplicaPreparedKind, ReplicaRole,
};
use allocdb_node::{
    ApiRequest, ApiResponse, MetricsResponse, ReservationResponse, ResourceResponse,
    SubmissionFailure, SubmissionFailureCode, SubmitResponse, TickExpirationsApplied,
    TickExpirationsResponse, decode_request, encode_response,
};
use log::{info, warn};

const STARTUP_TIMEOUT: Duration = Duration::from_secs(5);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const LISTENER_POLL_INTERVAL: Duration = Duration::from_millis(50);
const REPLICA_RPC_TIMEOUT: Duration = Duration::from_millis(500);
const MAX_STREAM_BYTES: usize = 1 << 20;
const PROTOCOL_REQUEST_PREPARE: u8 = 1;
const PROTOCOL_REQUEST_COMMIT: u8 = 2;
const PROTOCOL_RESPONSE_PREPARE_ACK: u8 = 1;
const PROTOCOL_RESPONSE_COMMIT_ACK: u8 = 2;
const PROTOCOL_PREPARED_KIND_CLIENT: u8 = 1;
const PROTOCOL_PREPARED_KIND_INTERNAL: u8 = 2;

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
    Crash {
        workspace_root: PathBuf,
        replica_id: ReplicaId,
    },
    Restart {
        workspace_root: PathBuf,
        replica_id: ReplicaId,
    },
    Isolate {
        workspace_root: PathBuf,
        replica_id: ReplicaId,
    },
    Heal {
        workspace_root: PathBuf,
        replica_id: ReplicaId,
    },
    ControlStatus {
        addr: SocketAddr,
    },
    ReplicaDaemon {
        layout_file: PathBuf,
        replica_id: ReplicaId,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ReplicaProtocolRequest {
    Prepare {
        kind: ReplicaPreparedKind,
        view: u64,
        lsn: u64,
        request_slot: Slot,
        payload: Vec<u8>,
    },
    Commit {
        view: u64,
        commit_lsn: u64,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReplicaProtocolResponse {
    PrepareAck {
        view: u64,
        lsn: u64,
        replica_id: ReplicaId,
    },
    CommitAck {
        view: u64,
        commit_lsn: u64,
        replica_id: ReplicaId,
    },
}

fn main() -> ExitCode {
    init_logging();
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("{message}");
            ExitCode::FAILURE
        }
    }
}

fn init_logging() {
    let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .try_init();
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
        ParsedCommand::Crash {
            workspace_root,
            replica_id,
        } => crash_replica(&workspace_root, replica_id),
        ParsedCommand::Restart {
            workspace_root,
            replica_id,
        } => restart_replica(&workspace_root, replica_id),
        ParsedCommand::Isolate {
            workspace_root,
            replica_id,
        } => isolate_replica(&workspace_root, replica_id),
        ParsedCommand::Heal {
            workspace_root,
            replica_id,
        } => heal_replica(&workspace_root, replica_id),
        ParsedCommand::ControlStatus { addr } => control_status(addr),
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
        "crash" => {
            let (workspace_root, replica_id) = parse_workspace_and_replica_flags(args)?;
            Ok(ParsedCommand::Crash {
                workspace_root,
                replica_id,
            })
        }
        "restart" => {
            let (workspace_root, replica_id) = parse_workspace_and_replica_flags(args)?;
            Ok(ParsedCommand::Restart {
                workspace_root,
                replica_id,
            })
        }
        "isolate" => {
            let (workspace_root, replica_id) = parse_workspace_and_replica_flags(args)?;
            Ok(ParsedCommand::Isolate {
                workspace_root,
                replica_id,
            })
        }
        "heal" => {
            let (workspace_root, replica_id) = parse_workspace_and_replica_flags(args)?;
            Ok(ParsedCommand::Heal {
                workspace_root,
                replica_id,
            })
        }
        "control-status" => Ok(ParsedCommand::ControlStatus {
            addr: parse_control_addr_flag(args)?,
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

fn parse_workspace_and_replica_flags(
    args: impl IntoIterator<Item = String>,
) -> Result<(PathBuf, ReplicaId), String> {
    let mut args = args.into_iter();
    let mut workspace_root = None;
    let mut replica_id = None;
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--workspace" => {
                let value = args.next().ok_or_else(usage)?;
                workspace_root = Some(PathBuf::from(value));
            }
            "--replica-id" => {
                let value = args.next().ok_or_else(usage)?;
                let parsed = value.parse::<u64>().map_err(|_| {
                    format!("invalid value for `--replica-id`: `{value}`\n\n{}", usage())
                })?;
                replica_id = Some(ReplicaId(parsed));
            }
            "--help" | "-h" => return Err(usage()),
            other => return Err(format!("unknown argument `{other}`\n\n{}", usage())),
        }
    }

    Ok((
        workspace_root.ok_or_else(usage)?,
        replica_id.ok_or_else(usage)?,
    ))
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

fn parse_control_addr_flag(args: impl IntoIterator<Item = String>) -> Result<SocketAddr, String> {
    let mut args = args.into_iter();
    let mut addr = None;
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--addr" => {
                let value = args.next().ok_or_else(usage)?;
                let parsed = value
                    .parse::<SocketAddr>()
                    .map_err(|_| format!("invalid value for `--addr`: `{value}`\n\n{}", usage()))?;
                addr = Some(parsed);
            }
            "--help" | "-h" => return Err(usage()),
            other => return Err(format!("unknown argument `{other}`\n\n{}", usage())),
        }
    }
    addr.ok_or_else(usage)
}

fn start_cluster(workspace_root: &Path) -> Result<(), String> {
    let layout = LocalClusterLayout::load_or_create(workspace_root)
        .map_err(|error| format!("failed to prepare local cluster layout: {error}"))?;
    let fault_state = LocalClusterFaultState::load_or_create(&layout.workspace_root)
        .map_err(|error| format!("failed to prepare local cluster fault state: {error}"))?;
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

    append_local_cluster_timeline_event(
        &layout.workspace_root,
        LocalClusterTimelineEventKind::ClusterStart,
        None,
        Some("cluster_started"),
    )
    .map_err(|error| format!("failed to record cluster start: {error}"))?;

    println!("cluster started");
    println!("workspace={}", layout.workspace_root.display());
    println!("layout={}", layout.layout_path().display());
    for status in &statuses {
        print_live_status(status, fault_state.is_replica_isolated(status.replica_id));
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
        append_local_cluster_timeline_event(
            &layout.workspace_root,
            LocalClusterTimelineEventKind::ClusterStop,
            None,
            Some("cluster_stopped"),
        )
        .map_err(|error| format!("failed to record cluster stop: {error}"))?;
        println!("cluster stopped");
    }
    Ok(())
}

fn status_cluster(workspace_root: &Path) -> Result<(), String> {
    let layout = load_existing_layout(workspace_root)?;
    let fault_state = LocalClusterFaultState::load_or_create(&layout.workspace_root)
        .map_err(|error| format!("failed to load local cluster fault state: {error}"))?;
    let timeline = load_local_cluster_timeline(&layout.workspace_root)
        .map_err(|error| format!("failed to load local cluster timeline: {error}"))?;
    println!("workspace={}", layout.workspace_root.display());
    println!("layout={}", layout.layout_path().display());
    println!(
        "fault_state={}",
        fault_state_path(&layout.workspace_root).display()
    );
    println!(
        "timeline={}",
        timeline_path(&layout.workspace_root).display()
    );
    println!("timeline_events={}", timeline.len());
    for replica in &layout.replicas {
        match request_control_status(replica.control_addr) {
            Ok(status) => {
                print_live_status(&status, fault_state.is_replica_isolated(replica.replica_id));
            }
            Err(_) if replica.pid_path.exists() => println!(
                "replica={} state=unreachable network={} control={} pid_path={} log={}",
                replica.replica_id.get(),
                if fault_state.is_replica_isolated(replica.replica_id) {
                    "isolated"
                } else {
                    "healthy"
                },
                replica.control_addr,
                replica.pid_path.display(),
                replica.log_path.display()
            ),
            Err(_) => println!(
                "replica={} state=stopped network={} control={} log={}",
                replica.replica_id.get(),
                if fault_state.is_replica_isolated(replica.replica_id) {
                    "isolated"
                } else {
                    "healthy"
                },
                replica.control_addr,
                replica.log_path.display()
            ),
        }
    }
    Ok(())
}

fn crash_replica(workspace_root: &Path, replica_id: ReplicaId) -> Result<(), String> {
    let layout = load_existing_layout(workspace_root)?;
    let replica = replica_config(&layout, replica_id)?;
    let process_id = resolve_live_replica_process_id(replica)?;
    kill_process(process_id)?;
    wait_for_process_exit(process_id)?;
    remove_stale_pid_file(&replica.pid_path)?;
    append_local_cluster_timeline_event(
        &layout.workspace_root,
        LocalClusterTimelineEventKind::ReplicaCrash,
        Some(replica_id),
        Some("signal=kill"),
    )
    .map_err(|error| format!("failed to record replica crash: {error}"))?;
    println!(
        "replica={} crashed pid={} control={}",
        replica_id.get(),
        process_id,
        replica.control_addr
    );
    Ok(())
}

fn restart_replica(workspace_root: &Path, replica_id: ReplicaId) -> Result<(), String> {
    let layout = load_existing_layout(workspace_root)?;
    let replica = replica_config(&layout, replica_id)?;
    ensure_replica_stopped(replica)?;
    remove_stale_pid_file(&replica.pid_path)?;
    let current_exe = std::env::current_exe()
        .map_err(|error| format!("failed to locate current executable: {error}"))?;
    let layout_file = layout.layout_path();
    let mut child = spawn_replica_daemon(&current_exe, &layout_file, replica)?;
    let status = wait_for_replica_ready(replica, &mut child)?;
    append_local_cluster_timeline_event(
        &layout.workspace_root,
        LocalClusterTimelineEventKind::ReplicaRestart,
        Some(replica_id),
        Some("replica_ready"),
    )
    .map_err(|error| format!("failed to record replica restart: {error}"))?;
    print_live_status(
        &status,
        LocalClusterFaultState::load_or_create(&layout.workspace_root)
            .map_err(|error| format!("failed to load local cluster fault state: {error}"))?
            .is_replica_isolated(replica_id),
    );
    Ok(())
}

fn isolate_replica(workspace_root: &Path, replica_id: ReplicaId) -> Result<(), String> {
    let layout = load_existing_layout(workspace_root)?;
    let replica = replica_config(&layout, replica_id)?;
    let mut fault_state = LocalClusterFaultState::load_or_create(&layout.workspace_root)
        .map_err(|error| format!("failed to load local cluster fault state: {error}"))?;
    fault_state
        .isolate_replica(replica_id)
        .map_err(|error| format!("failed to isolate replica {}: {error}", replica_id.get()))?;
    fault_state
        .persist()
        .map_err(|error| format!("failed to persist local cluster fault state: {error}"))?;
    append_local_cluster_timeline_event(
        &layout.workspace_root,
        LocalClusterTimelineEventKind::ReplicaIsolate,
        Some(replica_id),
        Some("scope=client+protocol"),
    )
    .map_err(|error| format!("failed to record replica isolation: {error}"))?;
    println!(
        "replica={} network=isolated client={} protocol={}",
        replica_id.get(),
        replica.client_addr,
        replica.protocol_addr
    );
    Ok(())
}

fn heal_replica(workspace_root: &Path, replica_id: ReplicaId) -> Result<(), String> {
    let layout = load_existing_layout(workspace_root)?;
    let replica = replica_config(&layout, replica_id)?;
    let mut fault_state = LocalClusterFaultState::load_or_create(&layout.workspace_root)
        .map_err(|error| format!("failed to load local cluster fault state: {error}"))?;
    fault_state
        .heal_replica(replica_id)
        .map_err(|error| format!("failed to heal replica {}: {error}", replica_id.get()))?;
    fault_state
        .persist()
        .map_err(|error| format!("failed to persist local cluster fault state: {error}"))?;
    append_local_cluster_timeline_event(
        &layout.workspace_root,
        LocalClusterTimelineEventKind::ReplicaHeal,
        Some(replica_id),
        Some("scope=client+protocol"),
    )
    .map_err(|error| format!("failed to record replica heal: {error}"))?;
    println!(
        "replica={} network=healthy client={} protocol={}",
        replica_id.get(),
        replica.client_addr,
        replica.protocol_addr
    );
    Ok(())
}

fn control_status(addr: SocketAddr) -> Result<(), String> {
    let status = request_control_status(addr)
        .map_err(|error| format!("failed to query control status from {addr}: {error}"))?;
    print!("{}", encode_status_response(&status));
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
    eprintln!(
        "replica={} event=startup_recovered {}",
        replica.replica_id.get(),
        format_replica_debug_summary(&node)
    );
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
        eprintln!(
            "replica={} event=startup_configured_normal_role target_role={} target_view={} {}",
            replica.replica_id.get(),
            encode_role(replica.role),
            layout.current_view,
            format_replica_debug_summary(&node)
        );
    }

    write_pid_file(&replica.pid_path, process::id())?;
    let pid_guard = PidFileGuard::new(replica.pid_path.clone());

    let control_listener = bind_listener(replica.control_addr)?;
    let client_listener = bind_listener(replica.client_addr)?;
    let protocol_listener = bind_listener(replica.protocol_addr)?;
    eprintln!(
        "replica={} startup=ready status={} {}",
        replica.replica_id.get(),
        describe_node_status(node.status()),
        format_replica_debug_summary(&node)
    );

    let loop_result = replica_event_loop(
        &mut node,
        &layout,
        &layout.workspace_root,
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
    layout: &LocalClusterLayout,
    workspace_root: &Path,
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
            handle_client_stream(stream, node, layout, workspace_root, replica)?;
        }
        if let Some(stream) = accept_nonblocking(protocol_listener)? {
            handled_work = true;
            handle_protocol_stream(stream, node, workspace_root, replica)?;
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

fn handle_client_stream(
    mut stream: TcpStream,
    node: &mut ReplicaNode,
    layout: &LocalClusterLayout,
    workspace_root: &Path,
    replica: &LocalClusterReplicaConfig,
) -> Result<(), String> {
    if replica_isolated(workspace_root, replica.replica_id)? {
        return write_response(
            stream,
            &encode_control_error("network isolated by local harness"),
        );
    }

    let request_bytes = match read_stream_bytes(&mut stream, "client request") {
        Ok(bytes) => bytes,
        Err(error) => return write_response(stream, &encode_control_error(&error)),
    };
    if request_bytes.is_empty() {
        return write_response(stream, &encode_control_error("invalid client request"));
    }

    let request = match decode_request(&request_bytes) {
        Ok(request) => request,
        Err(error) => {
            let response = encode_control_error(&format!("invalid client request: {error:?}"));
            return write_response(stream, &response);
        }
    };

    let response = match request {
        ApiRequest::Submit(request) => handle_replicated_submit(node, layout, replica, &request),
        ApiRequest::GetResource(request) => Ok(handle_resource_request(node, request)),
        ApiRequest::GetReservation(request) => Ok(handle_reservation_request(node, request)),
        ApiRequest::GetMetrics(request) => Ok(handle_metrics_request(node, request)),
        ApiRequest::TickExpirations(request) => {
            handle_replicated_tick_expirations(node, layout, replica, request)
        }
    };

    match response {
        Ok(bytes) => write_bytes_response(stream, &bytes),
        Err(message) => write_response(stream, &encode_control_error(&message)),
    }
}

fn handle_protocol_stream(
    mut stream: TcpStream,
    node: &mut ReplicaNode,
    workspace_root: &Path,
    replica: &LocalClusterReplicaConfig,
) -> Result<(), String> {
    if replica_isolated(workspace_root, replica.replica_id)? {
        return write_response(
            stream,
            &encode_control_error("network isolated by local harness"),
        );
    }

    let request_bytes = match read_stream_bytes(&mut stream, "protocol request") {
        Ok(bytes) => bytes,
        Err(error) => return write_response(stream, &encode_control_error(&error)),
    };
    if request_bytes.is_empty() {
        return write_response(stream, &encode_control_error("invalid protocol request"));
    }

    let request = decode_replica_protocol_request(&request_bytes)
        .map_err(|error| format!("failed to decode protocol request: {error}"));
    let response = match request {
        Ok(ReplicaProtocolRequest::Prepare {
            kind,
            view,
            lsn,
            request_slot,
            payload,
        }) => {
            handle_prepare_protocol_request(replica, node, kind, view, lsn, request_slot, payload)
        }
        Ok(ReplicaProtocolRequest::Commit { view, commit_lsn }) => {
            handle_commit_protocol_request(replica, node, view, commit_lsn)
        }
        Err(error) => Err(error),
    };

    match response {
        Ok(response) => {
            let bytes = encode_replica_protocol_response(response);
            write_bytes_response(stream, &bytes)
        }
        Err(message) => write_response(stream, &encode_control_error(&message)),
    }
}

fn handle_prepare_protocol_request(
    replica: &LocalClusterReplicaConfig,
    node: &mut ReplicaNode,
    kind: ReplicaPreparedKind,
    view: u64,
    lsn: u64,
    request_slot: Slot,
    payload: Vec<u8>,
) -> Result<ReplicaProtocolResponse, String> {
    let entry = allocdb_node::ReplicaPreparedEntry {
        kind,
        view,
        lsn: Lsn(lsn),
        request_slot,
        payload,
    };
    if let Err(error) = node.append_prepared_entry(entry) {
        eprintln!(
            "replica={} event=protocol_prepare_rejected role={} local_view={} request_view={} request_lsn={} error={error:?}",
            replica.replica_id.get(),
            encode_role(node.metadata().role),
            node.metadata().current_view,
            view,
            lsn
        );
        return Err(protocol_error_message(&error));
    }
    info!(
        "replica={} event=protocol_prepare_accepted role={} local_view={} request_view={} request_lsn={} request_slot={} kind={}",
        replica.replica_id.get(),
        encode_role(node.metadata().role),
        node.metadata().current_view,
        view,
        lsn,
        request_slot.get(),
        encode_protocol_prepared_kind(kind),
    );
    Ok(ReplicaProtocolResponse::PrepareAck {
        view,
        lsn,
        replica_id: replica.replica_id,
    })
}

fn handle_commit_protocol_request(
    replica: &LocalClusterReplicaConfig,
    node: &mut ReplicaNode,
    view: u64,
    commit_lsn: u64,
) -> Result<ReplicaProtocolResponse, String> {
    if node.metadata().current_view != view {
        eprintln!(
            "replica={} event=protocol_commit_view_mismatch role={} local_view={} commit_view={} commit_lsn={}",
            replica.replica_id.get(),
            encode_role(node.metadata().role),
            node.metadata().current_view,
            view,
            commit_lsn
        );
        return Err(format!(
            "protocol view mismatch: expected={} found={view}",
            node.metadata().current_view
        ));
    }
    if let Err(error) = node.commit_prepared_through(Lsn(commit_lsn)) {
        eprintln!(
            "replica={} event=protocol_commit_rejected role={} local_view={} commit_lsn={} error={error:?}",
            replica.replica_id.get(),
            encode_role(node.metadata().role),
            node.metadata().current_view,
            commit_lsn
        );
        return Err(protocol_error_message(&error));
    }
    info!(
        "replica={} event=protocol_commit_applied role={} local_view={} commit_view={} commit_lsn={} commit_lsn_after={}",
        replica.replica_id.get(),
        encode_role(node.metadata().role),
        node.metadata().current_view,
        view,
        commit_lsn,
        display_optional_lsn(node.metadata().commit_lsn),
    );
    Ok(ReplicaProtocolResponse::CommitAck {
        view,
        commit_lsn,
        replica_id: replica.replica_id,
    })
}

fn handle_replicated_submit(
    node: &mut ReplicaNode,
    layout: &LocalClusterLayout,
    replica: &LocalClusterReplicaConfig,
    request: &allocdb_node::SubmitRequest,
) -> Result<Vec<u8>, String> {
    if node.status() != ReplicaNodeStatus::Active {
        eprintln!(
            "replica={} event=client_submit_rejected status={} {}",
            replica.replica_id.get(),
            describe_node_status(node.status()),
            format_replica_debug_summary(node)
        );
        return Err(String::from("replica faulted"));
    }
    if node.metadata().role != ReplicaRole::Primary {
        eprintln!(
            "replica={} event=client_submit_not_primary role={} view={} commit_lsn={}",
            replica.replica_id.get(),
            encode_role(node.metadata().role),
            node.metadata().current_view,
            display_optional_lsn(node.metadata().commit_lsn)
        );
        return Err(format!(
            "not primary: role={}",
            encode_role(node.metadata().role)
        ));
    }

    if let Some(result) = lookup_retry_result(node, request.request_slot, &request.payload) {
        return Ok(encode_response(&ApiResponse::Submit(
            SubmitResponse::Committed(result.into()),
        )));
    }

    let entry = match prepare_or_resume_entry(node, request)? {
        PreparedSubmit::Entry(entry) => entry,
        PreparedSubmit::Response(response) => return Ok(response),
    };
    let quorum = collect_prepare_quorum(layout, replica, &entry);
    if quorum.acked_replicas < majority_quorum(layout) {
        log_quorum_unavailable(
            replica,
            &entry,
            quorum.acked_replicas,
            majority_quorum(layout),
        );
        return Ok(storage_failure_submit_response());
    }
    log_prepare_quorum_formed(replica, &entry, &quorum, majority_quorum(layout));

    let result = match commit_primary_entry(node, &entry)? {
        CommittedSubmit::Result(result) => result,
        CommittedSubmit::Response(response) => return Ok(response),
    };
    let broadcast = broadcast_commit_to_backups(layout, replica, &entry);
    log_commit_broadcast_complete(replica, &entry, &broadcast, layout.replicas.len());
    info!(
        "replica={} event=client_submit_committed view={} lsn={} result_code={:?} from_retry_cache={} acked_prepare_replicas={} acked_prepare_peers={} commit_ack_replicas={} commit_ack_peers={}",
        replica.replica_id.get(),
        entry.view,
        entry.lsn.get(),
        result.outcome.result_code,
        result.from_retry_cache,
        quorum.acked_replicas,
        format_replica_id_list(&quorum.acked_peers),
        broadcast.acked_replicas,
        format_replica_id_list(&broadcast.acked_peers),
    );

    Ok(encode_response(&ApiResponse::Submit(
        SubmitResponse::Committed(result.into()),
    )))
}

fn handle_replicated_tick_expirations(
    node: &mut ReplicaNode,
    layout: &LocalClusterLayout,
    replica: &LocalClusterReplicaConfig,
    request: allocdb_node::TickExpirationsRequest,
) -> Result<Vec<u8>, String> {
    if node.status() != ReplicaNodeStatus::Active {
        eprintln!(
            "replica={} event=tick_expirations_rejected status={} {}",
            replica.replica_id.get(),
            describe_node_status(node.status()),
            format_replica_debug_summary(node)
        );
        return Err(String::from("replica faulted"));
    }
    if node.metadata().role != ReplicaRole::Primary {
        eprintln!(
            "replica={} event=tick_expirations_not_primary role={} view={} commit_lsn={}",
            replica.replica_id.get(),
            encode_role(node.metadata().role),
            node.metadata().current_view,
            display_optional_lsn(node.metadata().commit_lsn)
        );
        return Err(format!(
            "not primary: role={}",
            encode_role(node.metadata().role)
        ));
    }

    let entries = loop {
        match node.prepare_expiration_tick(request.current_wall_clock_slot) {
            Ok(entries) => break entries,
            Err(allocdb_node::ReplicaProtocolError::PendingPreparedEntries { .. }) => {
                if drain_pending_internal_suffix(node, layout, replica)? {
                    continue;
                }
                return Ok(storage_failure_tick_response());
            }
            Err(allocdb_node::ReplicaProtocolError::Submission(error)) => {
                return Ok(tick_rejection_response(&error));
            }
            Err(error) => return Err(protocol_error_message(&error)),
        }
    };

    info!(
        "replica={} event=tick_expirations_prepared current_slot={} prepared_entries={} request_slot={}",
        replica.replica_id.get(),
        request.current_wall_clock_slot.get(),
        entries.len(),
        entries.first().map_or_else(
            || String::from("none"),
            |entry| entry.request_slot.get().to_string()
        ),
    );

    let mut processed_count = 0_u32;
    let mut last_applied_lsn = None;
    for entry in &entries {
        let result = match commit_tick_entry(node, layout, replica, entry)? {
            TickCommitResult::Committed(result) => result,
            TickCommitResult::StorageFailure => return Ok(storage_failure_tick_response()),
            TickCommitResult::Rejected(error) => return Ok(tick_rejection_response(&error)),
        };
        processed_count = processed_count.saturating_add(1);
        last_applied_lsn = Some(result.applied_lsn);
    }

    info!(
        "replica={} event=tick_expirations_complete processed_count={} last_applied_lsn={}",
        replica.replica_id.get(),
        processed_count,
        display_optional_lsn(last_applied_lsn),
    );

    Ok(encode_response(&ApiResponse::TickExpirations(
        TickExpirationsResponse::Applied(TickExpirationsApplied {
            processed_count,
            last_applied_lsn,
        }),
    )))
}

enum TickCommitResult {
    Committed(allocdb_node::SubmissionResult),
    StorageFailure,
    Rejected(allocdb_node::SubmissionError),
}

fn commit_tick_entry(
    node: &mut ReplicaNode,
    layout: &LocalClusterLayout,
    replica: &LocalClusterReplicaConfig,
    entry: &allocdb_node::ReplicaPreparedEntry,
) -> Result<TickCommitResult, String> {
    let quorum = collect_prepare_quorum(layout, replica, entry);
    if quorum.acked_replicas < majority_quorum(layout) {
        log_quorum_unavailable(
            replica,
            entry,
            quorum.acked_replicas,
            majority_quorum(layout),
        );
        return Ok(TickCommitResult::StorageFailure);
    }
    log_prepare_quorum_formed(replica, entry, &quorum, majority_quorum(layout));

    let result = match node.commit_prepared_through(entry.lsn) {
        Ok(Some(result)) => result,
        Ok(None) => {
            return Err(format!(
                "primary commit at lsn {} did not apply one prepared entry",
                entry.lsn.get()
            ));
        }
        Err(allocdb_node::ReplicaProtocolError::Submission(error)) => {
            return Ok(TickCommitResult::Rejected(error));
        }
        Err(error) => return Err(protocol_error_message(&error)),
    };
    let broadcast = broadcast_commit_to_backups(layout, replica, entry);
    log_commit_broadcast_complete(replica, entry, &broadcast, layout.replicas.len());
    info!(
        "replica={} event=tick_expirations_entry_committed view={} lsn={} result_code={:?} acked_prepare_replicas={} acked_prepare_peers={} commit_ack_replicas={} commit_ack_peers={}",
        replica.replica_id.get(),
        entry.view,
        entry.lsn.get(),
        result.outcome.result_code,
        quorum.acked_replicas,
        format_replica_id_list(&quorum.acked_peers),
        broadcast.acked_replicas,
        format_replica_id_list(&broadcast.acked_peers),
    );
    Ok(TickCommitResult::Committed(result))
}

enum PreparedSubmit {
    Entry(allocdb_node::ReplicaPreparedEntry),
    Response(Vec<u8>),
}

enum CommittedSubmit {
    Result(allocdb_node::SubmissionResult),
    Response(Vec<u8>),
}

struct PrepareQuorumResult {
    acked_replicas: usize,
    acked_peers: Vec<ReplicaId>,
}

struct CommitBroadcastResult {
    acked_replicas: usize,
    acked_peers: Vec<ReplicaId>,
}

fn prepare_or_resume_entry(
    node: &mut ReplicaNode,
    request: &allocdb_node::SubmitRequest,
) -> Result<PreparedSubmit, String> {
    match node.first_uncommitted_prepared_entry() {
        Some(entry) => {
            if entry.request_slot != request.request_slot || entry.payload != request.payload {
                Ok(PreparedSubmit::Response(storage_failure_submit_response()))
            } else {
                Ok(PreparedSubmit::Entry(entry))
            }
        }
        None => match node.prepare_client_request(request.request_slot, &request.payload) {
            Ok(entry) => Ok(PreparedSubmit::Entry(entry)),
            Err(allocdb_node::ReplicaProtocolError::Submission(error)) => Ok(
                PreparedSubmit::Response(submission_rejection_response(&error)),
            ),
            Err(error) => Err(protocol_error_message(&error)),
        },
    }
}

fn collect_prepare_quorum(
    layout: &LocalClusterLayout,
    replica: &LocalClusterReplicaConfig,
    entry: &allocdb_node::ReplicaPreparedEntry,
) -> PrepareQuorumResult {
    let mut acked_replicas = 1_usize;
    let mut acked_peers = Vec::new();
    for peer in &layout.replicas {
        if peer.replica_id == replica.replica_id {
            continue;
        }
        let request = ReplicaProtocolRequest::Prepare {
            kind: entry.kind,
            view: entry.view,
            lsn: entry.lsn.get(),
            request_slot: entry.request_slot,
            payload: entry.payload.clone(),
        };
        match send_protocol_request(peer.protocol_addr, &request) {
            Ok(response) if is_expected_prepare_ack(&response, peer.replica_id, entry) => {
                acked_replicas += 1;
                acked_peers.push(peer.replica_id);
            }
            Ok(other) => log_prepare_ack_rejected(replica, peer, entry, &other),
            Err(error) => log_prepare_ack_failed(replica, peer, entry, &error),
        }
    }
    PrepareQuorumResult {
        acked_replicas,
        acked_peers,
    }
}

fn commit_primary_entry(
    node: &mut ReplicaNode,
    entry: &allocdb_node::ReplicaPreparedEntry,
) -> Result<CommittedSubmit, String> {
    match node.commit_prepared_through(entry.lsn) {
        Ok(Some(result)) => Ok(CommittedSubmit::Result(result)),
        Ok(None) => Err(format!(
            "primary commit at lsn {} did not publish one client result",
            entry.lsn.get()
        )),
        Err(allocdb_node::ReplicaProtocolError::Submission(error)) => Ok(
            CommittedSubmit::Response(submission_rejection_response(&error)),
        ),
        Err(error) => Err(protocol_error_message(&error)),
    }
}

fn drain_pending_internal_suffix(
    node: &mut ReplicaNode,
    layout: &LocalClusterLayout,
    replica: &LocalClusterReplicaConfig,
) -> Result<bool, String> {
    let mut drained_any = false;
    loop {
        let Some(entry) = node.first_uncommitted_prepared_entry() else {
            return Ok(drained_any);
        };
        if entry.kind != ReplicaPreparedKind::Internal {
            return Ok(false);
        }

        let quorum = collect_prepare_quorum(layout, replica, &entry);
        if quorum.acked_replicas < majority_quorum(layout) {
            log_quorum_unavailable(
                replica,
                &entry,
                quorum.acked_replicas,
                majority_quorum(layout),
            );
            return Ok(false);
        }
        log_prepare_quorum_formed(replica, &entry, &quorum, majority_quorum(layout));

        match node.commit_prepared_through(entry.lsn) {
            Ok(Some(_)) => {}
            Ok(None) => {
                return Err(format!(
                    "primary commit at lsn {} did not apply one prepared entry",
                    entry.lsn.get()
                ));
            }
            Err(error) => return Err(protocol_error_message(&error)),
        }
        let broadcast = broadcast_commit_to_backups(layout, replica, &entry);
        log_commit_broadcast_complete(replica, &entry, &broadcast, layout.replicas.len());
        drained_any = true;
    }
}

fn broadcast_commit_to_backups(
    layout: &LocalClusterLayout,
    replica: &LocalClusterReplicaConfig,
    entry: &allocdb_node::ReplicaPreparedEntry,
) -> CommitBroadcastResult {
    let mut acked_replicas = 1_usize;
    let mut acked_peers = Vec::new();
    for peer in &layout.replicas {
        if peer.replica_id == replica.replica_id {
            continue;
        }
        let request = ReplicaProtocolRequest::Commit {
            view: entry.view,
            commit_lsn: entry.lsn.get(),
        };
        match send_protocol_request(peer.protocol_addr, &request) {
            Ok(ReplicaProtocolResponse::CommitAck {
                view,
                commit_lsn,
                replica_id,
            }) if view == entry.view
                && commit_lsn == entry.lsn.get()
                && replica_id == peer.replica_id =>
            {
                acked_replicas += 1;
                acked_peers.push(peer.replica_id);
            }
            Ok(other) => {
                warn!(
                    "replica={} event=commit_ack_rejected peer={} expected_view={} expected_commit_lsn={} response={other:?}",
                    replica.replica_id.get(),
                    peer.replica_id.get(),
                    entry.view,
                    entry.lsn.get(),
                );
            }
            Err(error) => {
                eprintln!(
                    "replica={} event=commit_broadcast_failed peer={} view={} commit_lsn={} error={error}",
                    replica.replica_id.get(),
                    peer.replica_id.get(),
                    entry.view,
                    entry.lsn.get(),
                );
            }
        }
    }
    CommitBroadcastResult {
        acked_replicas,
        acked_peers,
    }
}

fn submission_rejection_response(error: &allocdb_node::SubmissionError) -> Vec<u8> {
    let response = ApiResponse::Submit(SubmitResponse::Rejected(
        SubmissionFailure::from_submission_error(error),
    ));
    encode_response(&response)
}

fn tick_rejection_response(error: &allocdb_node::SubmissionError) -> Vec<u8> {
    encode_response(&ApiResponse::TickExpirations(
        TickExpirationsResponse::Rejected(SubmissionFailure::from_submission_error(error)),
    ))
}

fn storage_failure_submit_response() -> Vec<u8> {
    let response = ApiResponse::Submit(SubmitResponse::Rejected(SubmissionFailure {
        category: allocdb_node::SubmissionErrorCategory::Indefinite,
        code: SubmissionFailureCode::StorageFailure,
    }));
    encode_response(&response)
}

fn storage_failure_tick_response() -> Vec<u8> {
    encode_response(&ApiResponse::TickExpirations(
        TickExpirationsResponse::Rejected(SubmissionFailure {
            category: allocdb_node::SubmissionErrorCategory::Indefinite,
            code: SubmissionFailureCode::StorageFailure,
        }),
    ))
}

fn log_prepare_ack_rejected(
    replica: &LocalClusterReplicaConfig,
    peer: &LocalClusterReplicaConfig,
    entry: &allocdb_node::ReplicaPreparedEntry,
    response: &ReplicaProtocolResponse,
) {
    eprintln!(
        "replica={} event=prepare_ack_rejected peer={} expected_view={} expected_lsn={} response={response:?}",
        replica.replica_id.get(),
        peer.replica_id.get(),
        entry.view,
        entry.lsn.get(),
    );
}

fn log_prepare_ack_failed(
    replica: &LocalClusterReplicaConfig,
    peer: &LocalClusterReplicaConfig,
    entry: &allocdb_node::ReplicaPreparedEntry,
    error: &str,
) {
    eprintln!(
        "replica={} event=prepare_ack_failed peer={} view={} lsn={} error={error}",
        replica.replica_id.get(),
        peer.replica_id.get(),
        entry.view,
        entry.lsn.get(),
    );
}

fn log_quorum_unavailable(
    replica: &LocalClusterReplicaConfig,
    entry: &allocdb_node::ReplicaPreparedEntry,
    acked_replicas: usize,
    required_quorum: usize,
) {
    eprintln!(
        "replica={} event=quorum_unavailable lsn={} acked_replicas={} required_quorum={required_quorum}",
        replica.replica_id.get(),
        entry.lsn.get(),
        acked_replicas,
    );
}

fn log_prepare_quorum_formed(
    replica: &LocalClusterReplicaConfig,
    entry: &allocdb_node::ReplicaPreparedEntry,
    quorum: &PrepareQuorumResult,
    required_quorum: usize,
) {
    info!(
        "replica={} event=prepare_quorum_formed kind={} view={} lsn={} request_slot={} acked_replicas={} required_quorum={} acked_peers={}",
        replica.replica_id.get(),
        encode_protocol_prepared_kind(entry.kind),
        entry.view,
        entry.lsn.get(),
        entry.request_slot.get(),
        quorum.acked_replicas,
        required_quorum,
        format_replica_id_list(&quorum.acked_peers),
    );
}

fn log_commit_broadcast_complete(
    replica: &LocalClusterReplicaConfig,
    entry: &allocdb_node::ReplicaPreparedEntry,
    broadcast: &CommitBroadcastResult,
    replica_count: usize,
) {
    info!(
        "replica={} event=commit_broadcast_complete view={} commit_lsn={} acked_replicas={} replica_count={} acked_peers={}",
        replica.replica_id.get(),
        entry.view,
        entry.lsn.get(),
        broadcast.acked_replicas,
        replica_count,
        format_replica_id_list(&broadcast.acked_peers),
    );
}

fn format_replica_id_list(replica_ids: &[ReplicaId]) -> String {
    replica_ids
        .iter()
        .map(|replica_id| replica_id.get().to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn handle_resource_request(node: &ReplicaNode, request: allocdb_node::ResourceRequest) -> Vec<u8> {
    let response = match node.enforce_primary_read(request.required_lsn.unwrap_or(Lsn(0))) {
        Ok(()) => node
            .engine()
            .expect("primary read requires one live engine")
            .db()
            .resource(request.resource_id)
            .map_or(ResourceResponse::NotFound, |record| {
                ResourceResponse::Found(record.into())
            }),
        Err(allocdb_node::NotPrimaryReadError::Fence(
            allocdb_node::ReadError::RequiredLsnNotApplied {
                required_lsn,
                last_applied_lsn,
            },
        )) => ResourceResponse::FenceNotApplied {
            required_lsn,
            last_applied_lsn,
        },
        Err(
            allocdb_node::NotPrimaryReadError::Fence(allocdb_node::ReadError::EngineHalted)
            | allocdb_node::NotPrimaryReadError::ReplicaCrashed,
        ) => ResourceResponse::EngineHalted,
        Err(allocdb_node::NotPrimaryReadError::Role(role)) => {
            return encode_control_error_bytes(&format!("not primary: role={}", encode_role(role)));
        }
    };
    encode_response(&ApiResponse::GetResource(response))
}

fn handle_reservation_request(
    node: &ReplicaNode,
    request: allocdb_node::ReservationRequest,
) -> Vec<u8> {
    let response = match node.enforce_primary_read(request.required_lsn.unwrap_or(Lsn(0))) {
        Ok(()) => match node
            .engine()
            .expect("primary read requires one live engine")
            .db()
            .reservation(request.reservation_id, request.current_slot)
        {
            Ok(record) => ReservationResponse::Found(record.into()),
            Err(ReservationLookupError::NotFound) => ReservationResponse::NotFound,
            Err(ReservationLookupError::Retired) => ReservationResponse::Retired,
        },
        Err(allocdb_node::NotPrimaryReadError::Fence(
            allocdb_node::ReadError::RequiredLsnNotApplied {
                required_lsn,
                last_applied_lsn,
            },
        )) => ReservationResponse::FenceNotApplied {
            required_lsn,
            last_applied_lsn,
        },
        Err(
            allocdb_node::NotPrimaryReadError::Fence(allocdb_node::ReadError::EngineHalted)
            | allocdb_node::NotPrimaryReadError::ReplicaCrashed,
        ) => ReservationResponse::EngineHalted,
        Err(allocdb_node::NotPrimaryReadError::Role(role)) => {
            return encode_control_error_bytes(&format!("not primary: role={}", encode_role(role)));
        }
    };
    encode_response(&ApiResponse::GetReservation(response))
}

fn handle_metrics_request(node: &ReplicaNode, request: allocdb_node::MetricsRequest) -> Vec<u8> {
    match node.engine() {
        Some(engine) => encode_response(&ApiResponse::GetMetrics(MetricsResponse {
            metrics: engine.metrics(request.current_wall_clock_slot),
        })),
        None => encode_control_error_bytes("replica faulted"),
    }
}

fn lookup_retry_result(
    node: &ReplicaNode,
    request_slot: Slot,
    payload: &[u8],
) -> Option<allocdb_node::SubmissionResult> {
    let Ok(request) = decode_client_request(payload) else {
        return None;
    };
    let record = node
        .engine()
        .expect("active primary must keep one live engine")
        .db()
        .operation(request.operation_id, request_slot)?;

    let outcome = if record.command_fingerprint == request.command.fingerprint() {
        CommandOutcome {
            result_code: record.result_code,
            reservation_id: record.result_reservation_id,
            deadline_slot: record.result_deadline_slot,
        }
    } else {
        CommandOutcome::new(ResultCode::OperationConflict)
    };
    Some(allocdb_node::SubmissionResult {
        applied_lsn: record.applied_lsn,
        outcome,
        from_retry_cache: true,
    })
}

fn majority_quorum(layout: &LocalClusterLayout) -> usize {
    (layout.replicas.len() / 2) + 1
}

fn protocol_error_message(error: &allocdb_node::ReplicaProtocolError) -> String {
    format!("replica protocol error: {error:?}")
}

fn is_expected_prepare_ack(
    response: &ReplicaProtocolResponse,
    expected_replica_id: ReplicaId,
    entry: &allocdb_node::ReplicaPreparedEntry,
) -> bool {
    matches!(
        response,
        ReplicaProtocolResponse::PrepareAck {
            view,
            lsn,
            replica_id,
        } if *view == entry.view
            && *lsn == entry.lsn.get()
            && *replica_id == expected_replica_id
    )
}

fn read_stream_bytes(stream: &mut TcpStream, kind: &str) -> Result<Vec<u8>, String> {
    stream
        .set_read_timeout(Some(REPLICA_RPC_TIMEOUT))
        .map_err(|error| format!("failed to set {kind} read timeout: {error}"))?;
    let mut bytes = Vec::new();
    let max_stream_bytes =
        u64::try_from(MAX_STREAM_BYTES).expect("max stream bytes should fit in u64");
    stream
        .take(max_stream_bytes + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("failed to read {kind}: {error}"))?;
    if bytes.len() > MAX_STREAM_BYTES {
        return Err(format!("{kind} exceeds max size {MAX_STREAM_BYTES} bytes"));
    }
    Ok(bytes)
}

fn write_bytes_response(mut stream: TcpStream, response: &[u8]) -> Result<(), String> {
    stream
        .write_all(response)
        .map_err(|error| format!("failed to write response: {error}"))
}

fn encode_control_error_bytes(message: &str) -> Vec<u8> {
    encode_control_error(message).into_bytes()
}

fn send_protocol_request(
    addr: SocketAddr,
    request: &ReplicaProtocolRequest,
) -> Result<ReplicaProtocolResponse, String> {
    let mut stream = TcpStream::connect_timeout(&addr, REPLICA_RPC_TIMEOUT)
        .map_err(|error| format!("failed to connect to replica protocol {addr}: {error}"))?;
    stream
        .set_read_timeout(Some(REPLICA_RPC_TIMEOUT))
        .map_err(|error| format!("failed to set replica protocol read timeout: {error}"))?;
    stream
        .set_write_timeout(Some(REPLICA_RPC_TIMEOUT))
        .map_err(|error| format!("failed to set replica protocol write timeout: {error}"))?;

    let bytes = encode_replica_protocol_request(request)?;
    stream
        .write_all(&bytes)
        .map_err(|error| format!("failed to write replica protocol request to {addr}: {error}"))?;
    stream
        .shutdown(std::net::Shutdown::Write)
        .map_err(|error| {
            format!("failed to half-close replica protocol request to {addr}: {error}")
        })?;

    let response_bytes = read_stream_bytes(&mut stream, "protocol response")?;
    decode_replica_protocol_response(&response_bytes)
        .map_err(|error| format!("failed to decode replica protocol response from {addr}: {error}"))
}

fn encode_replica_protocol_request(request: &ReplicaProtocolRequest) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    match request {
        ReplicaProtocolRequest::Prepare {
            kind,
            view,
            lsn,
            request_slot,
            payload,
        } => {
            bytes.push(PROTOCOL_REQUEST_PREPARE);
            bytes.push(encode_protocol_prepared_kind(*kind));
            bytes.extend_from_slice(&view.to_le_bytes());
            bytes.extend_from_slice(&lsn.to_le_bytes());
            bytes.extend_from_slice(&request_slot.get().to_le_bytes());
            let payload_len = u32::try_from(payload.len())
                .map_err(|_| format!("protocol payload too large: {} bytes", payload.len()))?;
            bytes.extend_from_slice(&payload_len.to_le_bytes());
            bytes.extend_from_slice(payload);
        }
        ReplicaProtocolRequest::Commit { view, commit_lsn } => {
            bytes.push(PROTOCOL_REQUEST_COMMIT);
            bytes.extend_from_slice(&view.to_le_bytes());
            bytes.extend_from_slice(&commit_lsn.to_le_bytes());
        }
    }
    Ok(bytes)
}

fn decode_replica_protocol_request(bytes: &[u8]) -> Result<ReplicaProtocolRequest, String> {
    let mut cursor = 0_usize;
    let tag = read_u8(bytes, &mut cursor)?;
    let request = match tag {
        PROTOCOL_REQUEST_PREPARE => {
            let kind = decode_protocol_prepared_kind(read_u8(bytes, &mut cursor)?)?;
            let view = read_u64(bytes, &mut cursor)?;
            let lsn = read_u64(bytes, &mut cursor)?;
            let request_slot = Slot(read_u64(bytes, &mut cursor)?);
            let payload_len = usize::try_from(read_u32(bytes, &mut cursor)?)
                .map_err(|_| String::from("protocol payload length exceeds usize"))?;
            let payload = read_bytes(bytes, &mut cursor, payload_len)?;
            ReplicaProtocolRequest::Prepare {
                kind,
                view,
                lsn,
                request_slot,
                payload,
            }
        }
        PROTOCOL_REQUEST_COMMIT => ReplicaProtocolRequest::Commit {
            view: read_u64(bytes, &mut cursor)?,
            commit_lsn: read_u64(bytes, &mut cursor)?,
        },
        other => return Err(format!("invalid protocol request tag {other}")),
    };
    finish_decode(bytes, cursor)?;
    Ok(request)
}

const fn encode_protocol_prepared_kind(kind: ReplicaPreparedKind) -> u8 {
    match kind {
        ReplicaPreparedKind::Client => PROTOCOL_PREPARED_KIND_CLIENT,
        ReplicaPreparedKind::Internal => PROTOCOL_PREPARED_KIND_INTERNAL,
    }
}

fn decode_protocol_prepared_kind(value: u8) -> Result<ReplicaPreparedKind, String> {
    match value {
        PROTOCOL_PREPARED_KIND_CLIENT => Ok(ReplicaPreparedKind::Client),
        PROTOCOL_PREPARED_KIND_INTERNAL => Ok(ReplicaPreparedKind::Internal),
        other => Err(format!("invalid protocol prepared kind {other}")),
    }
}

fn encode_replica_protocol_response(response: ReplicaProtocolResponse) -> Vec<u8> {
    let mut bytes = Vec::new();
    match response {
        ReplicaProtocolResponse::PrepareAck {
            view,
            lsn,
            replica_id,
        } => {
            bytes.push(PROTOCOL_RESPONSE_PREPARE_ACK);
            bytes.extend_from_slice(&view.to_le_bytes());
            bytes.extend_from_slice(&lsn.to_le_bytes());
            bytes.extend_from_slice(&replica_id.get().to_le_bytes());
        }
        ReplicaProtocolResponse::CommitAck {
            view,
            commit_lsn,
            replica_id,
        } => {
            bytes.push(PROTOCOL_RESPONSE_COMMIT_ACK);
            bytes.extend_from_slice(&view.to_le_bytes());
            bytes.extend_from_slice(&commit_lsn.to_le_bytes());
            bytes.extend_from_slice(&replica_id.get().to_le_bytes());
        }
    }
    bytes
}

fn decode_replica_protocol_response(bytes: &[u8]) -> Result<ReplicaProtocolResponse, String> {
    let mut cursor = 0_usize;
    let tag = read_u8(bytes, &mut cursor)?;
    let response = match tag {
        PROTOCOL_RESPONSE_PREPARE_ACK => ReplicaProtocolResponse::PrepareAck {
            view: read_u64(bytes, &mut cursor)?,
            lsn: read_u64(bytes, &mut cursor)?,
            replica_id: ReplicaId(read_u64(bytes, &mut cursor)?),
        },
        PROTOCOL_RESPONSE_COMMIT_ACK => ReplicaProtocolResponse::CommitAck {
            view: read_u64(bytes, &mut cursor)?,
            commit_lsn: read_u64(bytes, &mut cursor)?,
            replica_id: ReplicaId(read_u64(bytes, &mut cursor)?),
        },
        other => {
            let message = String::from_utf8_lossy(bytes);
            return Err(format!(
                "invalid protocol response tag {other}; raw_response={message}"
            ));
        }
    };
    finish_decode(bytes, cursor)?;
    Ok(response)
}

fn read_u8(bytes: &[u8], cursor: &mut usize) -> Result<u8, String> {
    if *cursor >= bytes.len() {
        return Err(String::from("buffer too short"));
    }
    let value = bytes[*cursor];
    *cursor += 1;
    Ok(value)
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32, String> {
    let raw = read_bytes(bytes, cursor, std::mem::size_of::<u32>())?;
    let mut array = [0_u8; 4];
    array.copy_from_slice(&raw);
    Ok(u32::from_le_bytes(array))
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64, String> {
    let raw = read_bytes(bytes, cursor, std::mem::size_of::<u64>())?;
    let mut array = [0_u8; 8];
    array.copy_from_slice(&raw);
    Ok(u64::from_le_bytes(array))
}

fn read_bytes(bytes: &[u8], cursor: &mut usize, len: usize) -> Result<Vec<u8>, String> {
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| String::from("buffer length overflow"))?;
    if end > bytes.len() {
        return Err(String::from("buffer too short"));
    }
    let value = bytes[*cursor..end].to_vec();
    *cursor = end;
    Ok(value)
}

fn finish_decode(bytes: &[u8], cursor: usize) -> Result<(), String> {
    if cursor == bytes.len() {
        Ok(())
    } else {
        Err(String::from("trailing bytes"))
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

fn replica_config(
    layout: &LocalClusterLayout,
    replica_id: ReplicaId,
) -> Result<&LocalClusterReplicaConfig, String> {
    layout.replica(replica_id).ok_or_else(|| {
        format!(
            "replica {} is not present in {}",
            replica_id.get(),
            layout.layout_path().display()
        )
    })
}

fn replica_isolated(workspace_root: &Path, replica_id: ReplicaId) -> Result<bool, String> {
    LocalClusterFaultState::load_or_create(workspace_root)
        .map(|state| state.is_replica_isolated(replica_id))
        .map_err(|error| format!("failed to load local cluster fault state: {error}"))
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

fn ensure_replica_stopped(replica: &LocalClusterReplicaConfig) -> Result<(), String> {
    if let Ok(status) = request_control_status(replica.control_addr) {
        return Err(format!(
            "replica {} is already running on {} with pid {}",
            replica.replica_id.get(),
            replica.control_addr,
            status.process_id
        ));
    }
    if replica.pid_path.exists() {
        let process_id = read_pid_file(&replica.pid_path)?;
        if process_exists(process_id)? {
            return Err(format!(
                "replica {} still has a live pid {} at {}",
                replica.replica_id.get(),
                process_id,
                replica.pid_path.display()
            ));
        }
    }
    Ok(())
}

fn resolve_live_replica_process_id(replica: &LocalClusterReplicaConfig) -> Result<u32, String> {
    if let Ok(status) = request_control_status(replica.control_addr) {
        return Ok(status.process_id);
    }
    if !replica.pid_path.exists() {
        return Err(format!(
            "replica {} is already stopped",
            replica.replica_id.get()
        ));
    }
    let process_id = read_pid_file(&replica.pid_path)?;
    if process_exists(process_id)? {
        Ok(process_id)
    } else {
        remove_stale_pid_file(&replica.pid_path)?;
        Err(format!(
            "replica {} is already stopped",
            replica.replica_id.get()
        ))
    }
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

fn read_pid_file(path: &Path) -> Result<u32, String> {
    let bytes = fs::read_to_string(path)
        .map_err(|error| format!("failed to read pid file {}: {error}", path.display()))?;
    bytes.trim().parse::<u32>().map_err(|_| {
        format!(
            "invalid pid file {} contents `{}`",
            path.display(),
            bytes.trim()
        )
    })
}

fn remove_stale_pid_file(path: &Path) -> Result<(), String> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!(
            "failed to remove stale pid file {}: {error}",
            path.display()
        )),
    }
}

fn wait_for_process_exit(process_id: u32) -> Result<(), String> {
    let started = Instant::now();
    loop {
        if !process_exists(process_id)? {
            return Ok(());
        }
        if started.elapsed() >= SHUTDOWN_TIMEOUT {
            return Err(format!(
                "process {process_id} did not exit within {SHUTDOWN_TIMEOUT:?}"
            ));
        }
        thread::sleep(LISTENER_POLL_INTERVAL);
    }
}

#[cfg(unix)]
#[allow(unsafe_code)]
fn process_exists(process_id: u32) -> Result<bool, String> {
    // The local fault harness runs as a separate operator process, so it needs one direct
    // Unix PID probe instead of a `Child` handle.
    let process_id = i32::try_from(process_id)
        .map_err(|_| format!("process id {process_id} exceeds the local unix pid range"))?;
    let result = unsafe { libc::kill(process_id, 0) };
    if result == 0 {
        return Ok(true);
    }
    let error = std::io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::ESRCH) => Ok(false),
        _ => Err(format!("failed to inspect process {process_id}: {error}")),
    }
}

#[cfg(not(unix))]
fn process_exists(_process_id: u32) -> Result<bool, String> {
    Err(String::from(
        "process inspection is only supported for the local fault harness on unix hosts",
    ))
}

#[cfg(unix)]
#[allow(unsafe_code)]
fn kill_process(process_id: u32) -> Result<(), String> {
    // Crash injection is intentionally abrupt here: the fault harness must be able to stop one
    // replica without going through the replica control protocol.
    let process_id = i32::try_from(process_id)
        .map_err(|_| format!("process id {process_id} exceeds the local unix pid range"))?;
    let result = unsafe { libc::kill(process_id, libc::SIGKILL) };
    if result == 0 {
        Ok(())
    } else {
        Err(format!(
            "failed to signal process {}: {}",
            process_id,
            std::io::Error::last_os_error()
        ))
    }
}

#[cfg(not(unix))]
fn kill_process(_process_id: u32) -> Result<(), String> {
    Err(String::from(
        "replica crash injection is only supported for the local fault harness on unix hosts",
    ))
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

fn format_replica_debug_summary(node: &ReplicaNode) -> String {
    let metadata = node.metadata();
    format!(
        "role={} current_view={} commit_lsn={} snapshot_lsn={} last_normal_view={} durable_vote={} highest_prepared_lsn={}",
        encode_role(metadata.role),
        metadata.current_view,
        display_optional_lsn(metadata.commit_lsn),
        display_optional_lsn(metadata.active_snapshot_lsn),
        display_optional_u64(metadata.last_normal_view),
        display_optional_durable_vote(metadata.durable_vote),
        display_optional_lsn(node.highest_prepared_lsn())
    )
}

fn print_live_status(status: &ReplicaRuntimeStatus, network_isolated: bool) {
    println!(
        "replica={} state={} network={} pid={} role={} view={} control={} client={} protocol={}",
        status.replica_id.get(),
        match status.state {
            ReplicaRuntimeState::Active => "active",
            ReplicaRuntimeState::Faulted => "faulted",
        },
        if network_isolated {
            "isolated"
        } else {
            "healthy"
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

fn display_optional_u64(value: Option<u64>) -> String {
    value.map_or_else(|| String::from("none"), |value| value.to_string())
}

fn display_optional_durable_vote(value: Option<allocdb_node::replica::DurableVote>) -> String {
    value.map_or_else(
        || String::from("none"),
        |vote| format!("view{}->replica{}", vote.view, vote.voted_for.get()),
    )
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
        "usage: cargo run -p allocdb-node --bin allocdb-local-cluster -- <start|stop|status|crash|restart|isolate|heal> ...\n\
         \n\
         commands:\n\
         \x20\x20start --workspace <path>\n\
         \x20\x20stop --workspace <path>\n\
         \x20\x20status --workspace <path>\n\
         \x20\x20crash --workspace <path> --replica-id <id>\n\
         \x20\x20restart --workspace <path> --replica-id <id>\n\
         \x20\x20isolate --workspace <path> --replica-id <id>\n\
         \x20\x20heal --workspace <path> --replica-id <id>\n\
         \x20\x20control-status --addr <host:port>\n",
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Shutdown, TcpListener, TcpStream};

    fn test_prepared_entry(lsn: u64) -> allocdb_node::ReplicaPreparedEntry {
        allocdb_node::ReplicaPreparedEntry {
            kind: ReplicaPreparedKind::Client,
            view: 7,
            lsn: Lsn(lsn),
            request_slot: Slot(11),
            payload: vec![0xAA],
        }
    }

    #[test]
    fn prepare_ack_requires_expected_replica_identity() {
        let entry = test_prepared_entry(9);
        let matching = ReplicaProtocolResponse::PrepareAck {
            view: 7,
            lsn: 9,
            replica_id: ReplicaId(2),
        };
        let wrong_replica = ReplicaProtocolResponse::PrepareAck {
            view: 7,
            lsn: 9,
            replica_id: ReplicaId(3),
        };
        let wrong_lsn = ReplicaProtocolResponse::PrepareAck {
            view: 7,
            lsn: 10,
            replica_id: ReplicaId(2),
        };

        assert!(is_expected_prepare_ack(&matching, ReplicaId(2), &entry));
        assert!(!is_expected_prepare_ack(
            &wrong_replica,
            ReplicaId(2),
            &entry
        ));
        assert!(!is_expected_prepare_ack(&wrong_lsn, ReplicaId(2), &entry));
        assert!(!is_expected_prepare_ack(
            &ReplicaProtocolResponse::CommitAck {
                view: 7,
                commit_lsn: 9,
                replica_id: ReplicaId(2),
            },
            ReplicaId(2),
            &entry,
        ));
    }

    #[test]
    fn read_stream_bytes_rejects_oversized_payload() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let writer = std::thread::spawn(move || {
            let mut stream = TcpStream::connect(addr).unwrap();
            stream.write_all(&vec![0xAB; MAX_STREAM_BYTES + 1]).unwrap();
            stream.shutdown(Shutdown::Write).unwrap();
        });

        let (mut stream, _) = listener.accept().unwrap();
        let error = read_stream_bytes(&mut stream, "test request").unwrap_err();
        assert!(error.contains("exceeds max size"));

        writer.join().unwrap();
    }
}

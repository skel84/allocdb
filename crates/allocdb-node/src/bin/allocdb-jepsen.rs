use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use allocdb_core::ReservationState;
use allocdb_core::command::{ClientRequest, Command as AllocCommand};
use allocdb_core::ids::{ClientId, HolderId, Lsn, OperationId, ReservationId, ResourceId, Slot};
use allocdb_core::result::ResultCode;
use allocdb_node::jepsen::{
    JepsenAmbiguousOutcome, JepsenCommittedWrite, JepsenDefiniteFailure, JepsenEventOutcome,
    JepsenExpiredReservation, JepsenHistoryEvent, JepsenNemesisFamily, JepsenOperation,
    JepsenOperationKind, JepsenReadState, JepsenReadTarget, JepsenReservationState, JepsenRunSpec,
    JepsenSuccessfulRead, JepsenWorkloadFamily, JepsenWriteResult, analyze_history,
    create_artifact_bundle, load_history, persist_history, release_gate_plan,
    render_analysis_report,
};
use allocdb_node::local_cluster::{
    LocalClusterReplicaConfig, ReplicaRuntimeStatus, decode_control_status_response,
};
use allocdb_node::qemu_testbed::{QemuTestbedLayout, qemu_testbed_layout_path};
use allocdb_node::{
    ApiRequest, ApiResponse, MetricsRequest, ReplicaId, ReplicaIdentity, ReplicaMetadata,
    ReplicaMetadataFile, ReplicaNode, ReplicaPaths, ReplicaRole, ReservationRequest,
    ReservationResponse, ResourceRequest, ResourceResponse, SubmissionFailure,
    SubmissionFailureCode, SubmitRequest, SubmitResponse, TickExpirationsRequest,
    TickExpirationsResponse, decode_response, encode_request,
};

const CONTROL_HOST_SSH_PORT: u16 = 2220;
const REMOTE_CONTROL_SCRIPT_PATH: &str = "/usr/local/bin/allocdb-qemu-control";

enum ParsedCommand {
    Help,
    Plan,
    Analyze {
        history_file: PathBuf,
    },
    VerifyQemuSurface {
        workspace_root: PathBuf,
    },
    RunQemu {
        workspace_root: PathBuf,
        run_id: String,
        output_root: PathBuf,
    },
    ArchiveQemu {
        workspace_root: PathBuf,
        run_id: String,
        history_file: PathBuf,
        output_root: PathBuf,
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
        ParsedCommand::Plan => {
            print_release_gate_plan();
            Ok(())
        }
        ParsedCommand::Analyze { history_file } => analyze_history_file(&history_file),
        ParsedCommand::VerifyQemuSurface { workspace_root } => verify_qemu_surface(&workspace_root),
        ParsedCommand::RunQemu {
            workspace_root,
            run_id,
            output_root,
        } => run_qemu(&workspace_root, &run_id, &output_root),
        ParsedCommand::ArchiveQemu {
            workspace_root,
            run_id,
            history_file,
            output_root,
        } => archive_qemu_run(&workspace_root, &run_id, &history_file, &output_root),
    }
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<ParsedCommand, String> {
    let mut args = args.into_iter();
    let Some(subcommand) = args.next() else {
        return Ok(ParsedCommand::Help);
    };
    match subcommand.as_str() {
        "--help" | "-h" => Ok(ParsedCommand::Help),
        "plan" => Ok(ParsedCommand::Plan),
        "analyze" => Ok(ParsedCommand::Analyze {
            history_file: parse_history_flag(args)?,
        }),
        "verify-qemu-surface" => Ok(ParsedCommand::VerifyQemuSurface {
            workspace_root: parse_workspace_flag(args)?,
        }),
        "run-qemu" => parse_run_qemu_args(args),
        "archive-qemu" => parse_archive_qemu_args(args),
        other => Err(format!("unknown subcommand `{other}`\n\n{}", usage())),
    }
}

fn parse_workspace_flag(args: impl IntoIterator<Item = String>) -> Result<PathBuf, String> {
    let mut args = args.into_iter();
    let mut workspace_root = None;
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--workspace" => {
                workspace_root = Some(PathBuf::from(args.next().ok_or_else(usage)?));
            }
            "--help" | "-h" => return Err(usage()),
            other => return Err(format!("unknown argument `{other}`\n\n{}", usage())),
        }
    }
    workspace_root.ok_or_else(usage)
}

fn parse_history_flag(args: impl IntoIterator<Item = String>) -> Result<PathBuf, String> {
    let mut args = args.into_iter();
    let mut history_file = None;
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--history-file" => {
                history_file = Some(PathBuf::from(args.next().ok_or_else(usage)?));
            }
            "--help" | "-h" => return Err(usage()),
            other => return Err(format!("unknown argument `{other}`\n\n{}", usage())),
        }
    }
    history_file.ok_or_else(usage)
}

fn parse_archive_qemu_args(
    args: impl IntoIterator<Item = String>,
) -> Result<ParsedCommand, String> {
    let mut args = args.into_iter();
    let mut workspace_root = None;
    let mut run_id = None;
    let mut history_file = None;
    let mut output_root = None;
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--workspace" => {
                workspace_root = Some(PathBuf::from(args.next().ok_or_else(usage)?));
            }
            "--run-id" => {
                run_id = Some(args.next().ok_or_else(usage)?);
            }
            "--history-file" => {
                history_file = Some(PathBuf::from(args.next().ok_or_else(usage)?));
            }
            "--output-root" => {
                output_root = Some(PathBuf::from(args.next().ok_or_else(usage)?));
            }
            "--help" | "-h" => return Err(usage()),
            other => return Err(format!("unknown argument `{other}`\n\n{}", usage())),
        }
    }

    Ok(ParsedCommand::ArchiveQemu {
        workspace_root: workspace_root.ok_or_else(usage)?,
        run_id: run_id.ok_or_else(usage)?,
        history_file: history_file.ok_or_else(usage)?,
        output_root: output_root.ok_or_else(usage)?,
    })
}

fn parse_run_qemu_args(args: impl IntoIterator<Item = String>) -> Result<ParsedCommand, String> {
    let mut args = args.into_iter();
    let mut workspace_root = None;
    let mut run_id = None;
    let mut output_root = None;
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--workspace" => {
                workspace_root = Some(PathBuf::from(args.next().ok_or_else(usage)?));
            }
            "--run-id" => {
                run_id = Some(args.next().ok_or_else(usage)?);
            }
            "--output-root" => {
                output_root = Some(PathBuf::from(args.next().ok_or_else(usage)?));
            }
            "--help" | "-h" => return Err(usage()),
            other => return Err(format!("unknown argument `{other}`\n\n{}", usage())),
        }
    }

    Ok(ParsedCommand::RunQemu {
        workspace_root: workspace_root.ok_or_else(usage)?,
        run_id: run_id.ok_or_else(usage)?,
        output_root: output_root.ok_or_else(usage)?,
    })
}

fn usage() -> String {
    String::from(
        "usage:\n  allocdb-jepsen plan\n  allocdb-jepsen analyze --history-file <path>\n  allocdb-jepsen verify-qemu-surface --workspace <path>\n  allocdb-jepsen run-qemu --workspace <path> --run-id <run-id> --output-root <path>\n  allocdb-jepsen archive-qemu --workspace <path> --run-id <run-id> --history-file <path> --output-root <path>\n",
    )
}

fn print_release_gate_plan() {
    for run in release_gate_plan() {
        println!(
            "run_id={} workload={} nemesis={} minimum_fault_window_secs={} release_blocking={}",
            run.run_id,
            run.workload.as_str(),
            run.nemesis.as_str(),
            run.minimum_fault_window_secs
                .map_or(String::from("none"), |secs| secs.to_string()),
            run.release_blocking
        );
    }
}

fn analyze_history_file(history_file: &Path) -> Result<(), String> {
    let history =
        load_history(history_file).map_err(|error| format!("failed to load history: {error}"))?;
    let report = analyze_history(&history);
    print!("{}", render_analysis_report(&report));
    if report.release_gate_passed() {
        Ok(())
    } else {
        Err(String::from("Jepsen release gate is blocked"))
    }
}

fn verify_qemu_surface(workspace_root: &Path) -> Result<(), String> {
    let layout = load_qemu_layout(workspace_root)?;
    for guest in &layout.replica_guests {
        let client_addr = guest.client_addr().ok_or_else(|| {
            format!(
                "replica guest {} is missing one cluster client address",
                guest.name
            )
        })?;
        let client_host = client_addr.ip().to_string();
        let response_bytes = send_remote_api_request(
            &layout,
            &client_host,
            client_addr.port(),
            &ApiRequest::GetMetrics(MetricsRequest {
                current_wall_clock_slot: Slot(0),
            }),
        )?;
        let response = decode_qemu_api_response(&response_bytes)?;
        if !matches!(response, ApiResponse::GetMetrics(_)) {
            return Err(format!(
                "QEMU client surface is not ready for Jepsen: replica {} returned unexpected metrics probe response {response:?}",
                guest.replica_id.map_or(0, allocdb_node::ReplicaId::get)
            ));
        }

        let protocol_addr = guest.protocol_addr().ok_or_else(|| {
            format!(
                "replica guest {} is missing one cluster protocol address",
                guest.name
            )
        })?;
        let protocol_response = run_remote_tcp_request(
            &layout,
            &protocol_addr.ip().to_string(),
            protocol_addr.port(),
            &[],
        )?;
        validate_protocol_probe_response(
            guest
                .replica_id
                .map_or(ReplicaId(0), |replica_id| replica_id),
            &protocol_response,
        )?;
    }

    let primary = primary_replica(&layout)?;
    let primary_host = primary.client_addr.ip().to_string();
    let resource_id = unique_probe_resource_id();
    let submit_response = decode_qemu_api_response(&send_remote_api_request(
        &layout,
        &primary_host,
        primary.client_addr.port(),
        &ApiRequest::Submit(SubmitRequest::from_client_request(
            Slot(1),
            probe_create_request(resource_id),
        )),
    )?)?;
    let applied_lsn = extract_probe_commit_lsn(submit_response)?;

    let read_response = decode_qemu_api_response(&send_remote_api_request(
        &layout,
        &primary_host,
        primary.client_addr.port(),
        &ApiRequest::GetResource(ResourceRequest {
            resource_id: ResourceId(resource_id),
            required_lsn: Some(applied_lsn),
        }),
    )?)?;
    validate_probe_read_response(resource_id, &read_response)?;

    println!("qemu_surface=ready");
    Ok(())
}

fn run_qemu(workspace_root: &Path, run_id: &str, output_root: &Path) -> Result<(), String> {
    let run_spec = resolve_run_spec(run_id)?;
    let started_at = Instant::now();
    verify_qemu_surface(workspace_root)?;

    let layout = load_qemu_layout(workspace_root)?;
    let history = execute_qemu_run(&layout, &run_spec)?;
    fs::create_dir_all(output_root).map_err(|error| {
        format!(
            "failed to create output root {}: {error}",
            output_root.display()
        )
    })?;
    let history_path = output_root.join(format!("{}-history.txt", sanitize_run_id(run_id)));
    persist_history(&history_path, &history)
        .map_err(|error| format!("failed to persist Jepsen history: {error}"))?;

    let report = analyze_history(&history);
    let logs_archive = fetch_qemu_logs_archive(&layout, run_id, output_root)?;
    let bundle_dir = create_artifact_bundle(
        output_root,
        &run_spec,
        &history,
        &report,
        Some(&logs_archive),
    )
    .map_err(|error| format!("failed to create Jepsen artifact bundle: {error}"))?;

    println!("history_file={}", history_path.display());
    println!("artifact_bundle={}", bundle_dir.display());
    println!("qemu_logs_archive={}", logs_archive.display());
    print!("{}", render_analysis_report(&report));
    enforce_minimum_fault_window(&run_spec, started_at.elapsed())?;

    if report.release_gate_passed() {
        Ok(())
    } else {
        Err(String::from("Jepsen release gate is blocked"))
    }
}

fn resolve_run_spec(run_id: &str) -> Result<JepsenRunSpec, String> {
    release_gate_plan()
        .into_iter()
        .find(|candidate| candidate.run_id == run_id)
        .ok_or_else(|| format!("unknown Jepsen run id `{run_id}`"))
}

fn execute_qemu_run(
    layout: &QemuTestbedLayout,
    run_spec: &JepsenRunSpec,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    match run_spec.nemesis {
        JepsenNemesisFamily::None => execute_qemu_control_run(layout, run_spec),
        JepsenNemesisFamily::CrashRestart => execute_qemu_crash_restart_run(layout, run_spec),
        JepsenNemesisFamily::PartitionHeal => execute_qemu_partition_heal_run(layout, run_spec),
        JepsenNemesisFamily::MixedFailover => execute_qemu_mixed_failover_run(layout, run_spec),
    }
}

fn archive_qemu_run(
    workspace_root: &Path,
    run_id: &str,
    history_file: &Path,
    output_root: &Path,
) -> Result<(), String> {
    let run_spec = release_gate_plan()
        .into_iter()
        .find(|candidate| candidate.run_id == run_id)
        .ok_or_else(|| format!("unknown Jepsen run id `{run_id}`"))?;
    let history =
        load_history(history_file).map_err(|error| format!("failed to load history: {error}"))?;
    let report = analyze_history(&history);
    let layout = load_qemu_layout(workspace_root)?;
    let logs_archive = fetch_qemu_logs_archive(&layout, run_id, output_root)?;
    let bundle_dir = create_artifact_bundle(
        output_root,
        &run_spec,
        &history,
        &report,
        Some(&logs_archive),
    )
    .map_err(|error| format!("failed to create Jepsen artifact bundle: {error}"))?;
    println!("artifact_bundle={}", bundle_dir.display());
    println!("qemu_logs_archive={}", logs_archive.display());
    if report.release_gate_passed() {
        Ok(())
    } else {
        Err(String::from("Jepsen release gate is blocked"))
    }
}

#[derive(Default)]
struct HistoryBuilder {
    next_sequence: u64,
    events: Vec<JepsenHistoryEvent>,
}

impl HistoryBuilder {
    fn push(
        &mut self,
        process: impl Into<String>,
        operation: JepsenOperation,
        outcome: JepsenEventOutcome,
    ) {
        self.next_sequence = self.next_sequence.saturating_add(1);
        self.events.push(JepsenHistoryEvent {
            sequence: self.next_sequence,
            process: process.into(),
            time_millis: current_time_millis(),
            operation,
            outcome,
        });
    }

    fn finish(self) -> Vec<JepsenHistoryEvent> {
        self.events
    }
}

enum RemoteApiOutcome {
    Api(ApiResponse),
    Text(String),
}

struct ReserveCommit {
    applied_lsn: Lsn,
    reservation_id: ReservationId,
}

struct StagedReplicaWorkspace {
    root: PathBuf,
    replica_id: ReplicaId,
    paths: ReplicaPaths,
}

impl StagedReplicaWorkspace {
    fn new(replica_id: ReplicaId) -> Result<Self, String> {
        let root = temp_staging_dir(&format!("qemu-replica-{}", replica_id.get()))?;
        let workspace_dir = root.join(format!("replica-{}", replica_id.get()));
        fs::create_dir_all(&workspace_dir).map_err(|error| {
            format!(
                "failed to create staged workspace {}: {error}",
                workspace_dir.display()
            )
        })?;
        let paths = ReplicaPaths::new(
            workspace_dir.join("replica.metadata"),
            workspace_dir.join("state.snapshot"),
            workspace_dir.join("state.wal"),
        );
        Ok(Self {
            root,
            replica_id,
            paths,
        })
    }

    fn from_export(layout: &QemuTestbedLayout, replica_id: ReplicaId) -> Result<Self, String> {
        let staged = Self::new(replica_id)?;
        let archive = run_remote_control_command(
            layout,
            &[String::from("export-replica"), replica_id.get().to_string()],
            None,
        )?;
        run_local_tar_extract(&staged.root, &archive)?;
        Ok(staged)
    }

    fn import_to_remote(&self, layout: &QemuTestbedLayout) -> Result<(), String> {
        let archive =
            run_local_tar_archive(&self.root, &format!("replica-{}", self.replica_id.get()))?;
        let _ = run_remote_control_command(
            layout,
            &[
                String::from("import-replica"),
                self.replica_id.get().to_string(),
            ],
            Some(&archive),
        )?;
        Ok(())
    }
}

impl Drop for StagedReplicaWorkspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn temp_staging_dir(prefix: &str) -> Result<PathBuf, String> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let root = std::env::temp_dir().join(format!("allocdb-{prefix}-{millis}"));
    fs::create_dir_all(&root).map_err(|error| {
        format!(
            "failed to create temp staging dir {}: {error}",
            root.display()
        )
    })?;
    Ok(root)
}

fn run_remote_control_command(
    layout: &QemuTestbedLayout,
    args: &[String],
    stdin_bytes: Option<&[u8]>,
) -> Result<Vec<u8>, String> {
    let mut remote_args = ssh_args(layout);
    let remote_command = if args.is_empty() {
        format!("sudo {REMOTE_CONTROL_SCRIPT_PATH}")
    } else {
        format!("sudo {REMOTE_CONTROL_SCRIPT_PATH} {}", args.join(" "))
    };
    remote_args.push(remote_command);

    let output = if let Some(stdin_bytes) = stdin_bytes {
        let mut child = Command::new("ssh")
            .args(&remote_args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| format!("failed to spawn qemu control ssh command: {error}"))?;
        child
            .stdin
            .as_mut()
            .ok_or_else(|| String::from("qemu control ssh stdin was unavailable"))?
            .write_all(stdin_bytes)
            .map_err(|error| format!("failed to write qemu control stdin: {error}"))?;
        child
            .wait_with_output()
            .map_err(|error| format!("failed to wait for qemu control ssh command: {error}"))?
    } else {
        Command::new("ssh")
            .args(&remote_args)
            .output()
            .map_err(|error| format!("failed to run qemu control ssh command: {error}"))?
    };

    if output.status.success() {
        Ok(output.stdout)
    } else {
        Err(format!(
            "qemu control command `{}` failed: status={} stderr={}",
            args.join(" "),
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}

fn run_local_tar_extract(destination_root: &Path, archive: &[u8]) -> Result<(), String> {
    let mut child = Command::new("tar")
        .arg("xzf")
        .arg("-")
        .arg("-C")
        .arg(destination_root)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| format!("failed to spawn local tar extract: {error}"))?;
    child
        .stdin
        .as_mut()
        .ok_or_else(|| String::from("local tar extract stdin was unavailable"))?
        .write_all(archive)
        .map_err(|error| format!("failed to write local tar extract stdin: {error}"))?;
    let output = child
        .wait_with_output()
        .map_err(|error| format!("failed to wait for local tar extract: {error}"))?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "failed to extract staged replica archive into {}: status={} stderr={}",
            destination_root.display(),
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}

fn run_local_tar_archive(root: &Path, entry_name: &str) -> Result<Vec<u8>, String> {
    let output = Command::new("tar")
        .arg("czf")
        .arg("-")
        .arg("-C")
        .arg(root)
        .arg(entry_name)
        .output()
        .map_err(|error| format!("failed to run local tar archive: {error}"))?;
    if output.status.success() {
        Ok(output.stdout)
    } else {
        Err(format!(
            "failed to build staged replica archive from {}: status={} stderr={}",
            root.display(),
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}

fn prepare_log_path_for(metadata_path: &Path) -> PathBuf {
    let mut path = metadata_path.to_path_buf();
    let extension = path
        .extension()
        .and_then(|value| value.to_str())
        .map_or_else(
            || String::from("prepare"),
            |value| format!("{value}.prepare"),
        );
    path.set_extension(extension);
    path
}

fn copy_file_or_remove(source: &Path, destination: &Path) -> Result<(), String> {
    match fs::read(source) {
        Ok(bytes) => {
            fs::write(destination, bytes).map_err(|error| {
                format!(
                    "failed to write copied file {}: {error}",
                    destination.display()
                )
            })?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if destination.exists() {
                fs::remove_file(destination).map_err(|remove_error| {
                    format!(
                        "failed to remove stale copied file {}: {remove_error}",
                        destination.display()
                    )
                })?;
            }
            Ok(())
        }
        Err(error) => Err(format!("failed to read {}: {error}", source.display())),
    }
}

fn execute_qemu_control_run(
    layout: &QemuTestbedLayout,
    run_spec: &JepsenRunSpec,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let primary = primary_replica(layout)?;
    let backup = first_backup_replica(layout, Some(primary.replica_id))?;
    let base_id = unique_probe_resource_id();
    match run_spec.workload {
        JepsenWorkloadFamily::ReservationContention => {
            run_reservation_contention(layout, &primary, base_id)
        }
        JepsenWorkloadFamily::AmbiguousWriteRetry => {
            run_ambiguous_write_retry(layout, &primary, base_id)
        }
        JepsenWorkloadFamily::FailoverReadFences => {
            run_failover_read_fences(layout, &primary, &backup, base_id)
        }
        JepsenWorkloadFamily::ExpirationAndRecovery => {
            run_expiration_and_recovery(layout, &primary, base_id)
        }
    }
}

fn execute_qemu_crash_restart_run(
    layout: &QemuTestbedLayout,
    run_spec: &JepsenRunSpec,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let primary = primary_replica(layout)?;
    let failover_target = first_backup_replica(layout, Some(primary.replica_id))?;
    let supporting_backup = first_backup_replica(layout, Some(failover_target.replica_id))?;
    let base_id = unique_probe_resource_id();
    match run_spec.workload {
        JepsenWorkloadFamily::ReservationContention => run_crash_restart_reservation_contention(
            layout,
            &primary,
            &failover_target,
            &supporting_backup,
            base_id,
        ),
        JepsenWorkloadFamily::AmbiguousWriteRetry => run_crash_restart_ambiguous_retry(
            layout,
            &primary,
            &failover_target,
            &supporting_backup,
            base_id,
        ),
        JepsenWorkloadFamily::FailoverReadFences => run_crash_restart_failover_reads(
            layout,
            &primary,
            &failover_target,
            &supporting_backup,
            base_id,
        ),
        JepsenWorkloadFamily::ExpirationAndRecovery => run_crash_restart_expiration_recovery(
            layout,
            &primary,
            &failover_target,
            &supporting_backup,
            base_id,
        ),
    }
}

fn execute_qemu_partition_heal_run(
    layout: &QemuTestbedLayout,
    run_spec: &JepsenRunSpec,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let primary = primary_replica(layout)?;
    let failover_target = first_backup_replica(layout, Some(primary.replica_id))?;
    let supporting_backup = first_backup_replica(layout, Some(failover_target.replica_id))?;
    let base_id = unique_probe_resource_id();
    match run_spec.workload {
        JepsenWorkloadFamily::ReservationContention => run_partition_heal_reservation_contention(
            layout,
            &primary,
            &failover_target,
            &supporting_backup,
            base_id,
        ),
        JepsenWorkloadFamily::AmbiguousWriteRetry => run_partition_heal_ambiguous_retry(
            layout,
            &primary,
            &failover_target,
            &supporting_backup,
            base_id,
        ),
        JepsenWorkloadFamily::FailoverReadFences => run_partition_heal_failover_reads(
            layout,
            &primary,
            &failover_target,
            &supporting_backup,
            base_id,
        ),
        JepsenWorkloadFamily::ExpirationAndRecovery => run_partition_heal_expiration_recovery(
            layout,
            &primary,
            &failover_target,
            &supporting_backup,
            base_id,
        ),
    }
}

fn execute_qemu_mixed_failover_run(
    layout: &QemuTestbedLayout,
    run_spec: &JepsenRunSpec,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let primary = primary_replica(layout)?;
    let failover_target = first_backup_replica(layout, Some(primary.replica_id))?;
    let supporting_backup = first_backup_replica(layout, Some(failover_target.replica_id))?;
    let base_id = unique_probe_resource_id();
    match run_spec.workload {
        JepsenWorkloadFamily::ReservationContention => Err(String::from(
            "mixed failover runs are not defined for reservation_contention",
        )),
        JepsenWorkloadFamily::AmbiguousWriteRetry => run_mixed_failover_ambiguous_retry(
            layout,
            &primary,
            &failover_target,
            &supporting_backup,
            base_id,
        ),
        JepsenWorkloadFamily::FailoverReadFences => run_mixed_failover_reads(
            layout,
            &primary,
            &failover_target,
            &supporting_backup,
            base_id,
        ),
        JepsenWorkloadFamily::ExpirationAndRecovery => run_mixed_failover_expiration_recovery(
            layout,
            &primary,
            &failover_target,
            &supporting_backup,
            base_id,
        ),
    }
}

fn request_remote_control_status(
    layout: &QemuTestbedLayout,
    replica: &LocalClusterReplicaConfig,
) -> Result<ReplicaRuntimeStatus, String> {
    let response_bytes = run_remote_tcp_request(
        layout,
        &replica.control_addr.ip().to_string(),
        replica.control_addr.port(),
        b"status",
    )?;
    let response = String::from_utf8(response_bytes).map_err(|error| {
        format!(
            "invalid control status utf8 for replica {}: {error}",
            replica.replica_id.get()
        )
    })?;
    decode_control_status_response(&response).map_err(|error| {
        format!(
            "invalid control status for replica {}: {error}",
            replica.replica_id.get()
        )
    })
}

fn runtime_replica_configs(
    layout: &QemuTestbedLayout,
) -> Result<Vec<LocalClusterReplicaConfig>, String> {
    let mut replicas = layout.replica_layout.replicas.clone();
    for replica in &mut replicas {
        let status = request_remote_control_status(layout, replica)?;
        replica.role = status.role;
    }
    Ok(replicas)
}

fn primary_replica(layout: &QemuTestbedLayout) -> Result<LocalClusterReplicaConfig, String> {
    runtime_replica_configs(layout)?
        .into_iter()
        .find(|replica| replica.role == ReplicaRole::Primary)
        .ok_or_else(|| String::from("qemu cluster has no live primary"))
}

fn first_backup_replica(
    layout: &QemuTestbedLayout,
    exclude: Option<ReplicaId>,
) -> Result<LocalClusterReplicaConfig, String> {
    runtime_replica_configs(layout)?
        .into_iter()
        .find(|replica| {
            replica.role == ReplicaRole::Backup
                && exclude.is_none_or(|excluded| replica.replica_id != excluded)
        })
        .ok_or_else(|| String::from("qemu cluster has no live backup"))
}

fn runtime_replica_by_id(
    layout: &QemuTestbedLayout,
    replica_id: ReplicaId,
) -> Result<LocalClusterReplicaConfig, String> {
    runtime_replica_configs(layout)?
        .into_iter()
        .find(|replica| replica.replica_id == replica_id)
        .ok_or_else(|| format!("qemu cluster has no replica {}", replica_id.get()))
}

fn maybe_crash_replica(layout: &QemuTestbedLayout, replica_id: ReplicaId) -> Result<(), String> {
    match run_remote_control_command(
        layout,
        &[String::from("crash"), replica_id.get().to_string()],
        None,
    ) {
        Ok(_) => Ok(()),
        Err(error) if error.contains("already stopped") => Ok(()),
        Err(error) => Err(error),
    }
}

fn restart_replica(layout: &QemuTestbedLayout, replica_id: ReplicaId) -> Result<(), String> {
    let _ = run_remote_control_command(
        layout,
        &[String::from("restart"), replica_id.get().to_string()],
        None,
    )?;
    Ok(())
}

fn isolate_replica(layout: &QemuTestbedLayout, replica_id: ReplicaId) -> Result<(), String> {
    let _ = run_remote_control_command(
        layout,
        &[String::from("isolate"), replica_id.get().to_string()],
        None,
    )?;
    Ok(())
}

fn heal_replica(layout: &QemuTestbedLayout, replica_id: ReplicaId) -> Result<(), String> {
    let _ = run_remote_control_command(
        layout,
        &[String::from("heal"), replica_id.get().to_string()],
        None,
    )?;
    Ok(())
}

fn staged_replica_summary(
    layout: &QemuTestbedLayout,
    staged: &StagedReplicaWorkspace,
) -> Result<(u64, Option<Lsn>, Option<Lsn>), String> {
    let node = ReplicaNode::recover(
        layout.replica_layout.core_config.clone(),
        layout.replica_layout.engine_config,
        ReplicaIdentity {
            replica_id: staged.replica_id,
            shard_id: layout.replica_layout.core_config.shard_id,
        },
        staged.paths.clone(),
    )
    .map_err(|error| {
        format!(
            "failed to recover staged replica {} summary: {error:?}",
            staged.replica_id.get()
        )
    })?;
    Ok((
        node.metadata().current_view,
        node.metadata().commit_lsn,
        node.highest_prepared_lsn(),
    ))
}

fn rewrite_replica_from_source(
    layout: &QemuTestbedLayout,
    source: &StagedReplicaWorkspace,
    target: &StagedReplicaWorkspace,
    target_commit_lsn: Option<Lsn>,
    new_view: u64,
    new_role: ReplicaRole,
) -> Result<(), String> {
    let source_node = ReplicaNode::recover(
        layout.replica_layout.core_config.clone(),
        layout.replica_layout.engine_config,
        ReplicaIdentity {
            replica_id: source.replica_id,
            shard_id: layout.replica_layout.core_config.shard_id,
        },
        source.paths.clone(),
    )
    .map_err(|error| {
        format!(
            "failed to recover staged source replica {}: {error:?}",
            source.replica_id.get()
        )
    })?;
    let source_metadata = *source_node.metadata();
    let source_prepare_log_path = source_node.prepare_log_path().to_path_buf();
    drop(source_node);

    if source.replica_id != target.replica_id {
        copy_file_or_remove(&source.paths.snapshot_path, &target.paths.snapshot_path)?;
        copy_file_or_remove(&source.paths.wal_path, &target.paths.wal_path)?;
        copy_file_or_remove(
            &source_prepare_log_path,
            &prepare_log_path_for(&target.paths.metadata_path),
        )?;
    }

    let target_identity = ReplicaIdentity {
        replica_id: target.replica_id,
        shard_id: layout.replica_layout.core_config.shard_id,
    };
    ReplicaMetadataFile::new(&target.paths.metadata_path)
        .write_metadata(&ReplicaMetadata {
            identity: target_identity,
            current_view: source_metadata.current_view,
            role: ReplicaRole::Backup,
            commit_lsn: source_metadata.commit_lsn,
            active_snapshot_lsn: source_metadata.active_snapshot_lsn,
            last_normal_view: source_metadata.last_normal_view,
            durable_vote: None,
        })
        .map_err(|error| {
            format!(
                "failed to write staged metadata for replica {}: {error:?}",
                target.replica_id.get()
            )
        })?;

    let mut target_node = ReplicaNode::recover(
        layout.replica_layout.core_config.clone(),
        layout.replica_layout.engine_config,
        target_identity,
        target.paths.clone(),
    )
    .map_err(|error| {
        format!(
            "failed to recover rewritten staged replica {}: {error:?}",
            target.replica_id.get()
        )
    })?;
    if let Some(target_commit_lsn) = target_commit_lsn {
        if target_node
            .metadata()
            .commit_lsn
            .is_none_or(|commit_lsn| commit_lsn.get() < target_commit_lsn.get())
        {
            target_node
                .enter_view_uncertain()
                .map_err(|error| format!("failed to enter view uncertain: {error:?}"))?;
            target_node
                .reconstruct_committed_prefix_through(target_commit_lsn)
                .map_err(|error| format!("failed to reconstruct committed prefix: {error:?}"))?;
        }
    }
    target_node
        .discard_uncommitted_suffix()
        .map_err(|error| format!("failed to discard staged suffix: {error:?}"))?;
    target_node
        .configure_normal_role(new_view, new_role)
        .map_err(|error| format!("failed to configure staged role: {error:?}"))?;
    Ok(())
}

fn perform_failover(
    layout: &QemuTestbedLayout,
    old_primary: ReplicaId,
    new_primary: ReplicaId,
    supporting_backup: ReplicaId,
) -> Result<(), String> {
    maybe_crash_replica(layout, old_primary)?;
    maybe_crash_replica(layout, new_primary)?;
    maybe_crash_replica(layout, supporting_backup)?;

    let new_primary_stage = StagedReplicaWorkspace::from_export(layout, new_primary)?;
    let supporting_stage = StagedReplicaWorkspace::from_export(layout, supporting_backup)?;
    let new_primary_summary = staged_replica_summary(layout, &new_primary_stage)?;
    let supporting_summary = staged_replica_summary(layout, &supporting_stage)?;

    let target_commit_lsn = new_primary_summary
        .1
        .max(supporting_summary.1)
        .or_else(|| new_primary_summary.2.max(supporting_summary.2));
    let new_view = new_primary_summary
        .0
        .max(supporting_summary.0)
        .saturating_add(1);

    let source = if supporting_summary.1.unwrap_or(Lsn(0)).get()
        > new_primary_summary.1.unwrap_or(Lsn(0)).get()
        || (supporting_summary.1 == new_primary_summary.1
            && supporting_summary.2.unwrap_or(Lsn(0)).get()
                > new_primary_summary.2.unwrap_or(Lsn(0)).get())
    {
        &supporting_stage
    } else {
        &new_primary_stage
    };

    rewrite_replica_from_source(
        layout,
        source,
        &new_primary_stage,
        target_commit_lsn,
        new_view,
        ReplicaRole::Primary,
    )?;
    rewrite_replica_from_source(
        layout,
        source,
        &supporting_stage,
        target_commit_lsn,
        new_view,
        ReplicaRole::Backup,
    )?;

    supporting_stage.import_to_remote(layout)?;
    new_primary_stage.import_to_remote(layout)?;
    restart_replica(layout, supporting_backup)?;
    restart_replica(layout, new_primary)?;
    Ok(())
}

fn perform_rejoin(
    layout: &QemuTestbedLayout,
    current_primary: ReplicaId,
    target_replica: ReplicaId,
) -> Result<(), String> {
    maybe_crash_replica(layout, target_replica)?;
    let source_stage = StagedReplicaWorkspace::from_export(layout, current_primary)?;
    let source_summary = staged_replica_summary(layout, &source_stage)?;
    let target_stage = StagedReplicaWorkspace::new(target_replica)?;
    rewrite_replica_from_source(
        layout,
        &source_stage,
        &target_stage,
        source_summary.1,
        source_summary.0,
        ReplicaRole::Backup,
    )?;
    target_stage.import_to_remote(layout)?;
    restart_replica(layout, target_replica)?;
    Ok(())
}

fn run_crash_restart_reservation_contention(
    layout: &QemuTestbedLayout,
    primary: &LocalClusterReplicaConfig,
    failover_target: &LocalClusterReplicaConfig,
    supporting_backup: &LocalClusterReplicaConfig,
    base_id: u128,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id);
    create_qemu_resource(layout, primary, resource_id, OperationId(base_id + 100))?;

    maybe_crash_replica(layout, primary.replica_id)?;
    perform_failover(
        layout,
        primary.replica_id,
        failover_target.replica_id,
        supporting_backup.replica_id,
    )?;

    let current_primary = primary_replica(layout)?;
    let mut history = HistoryBuilder::default();
    let first = reserve_event(
        layout,
        &current_primary,
        OperationId(base_id + 1),
        resource_id,
        HolderId(101),
        Slot(10),
        5,
    )?;
    history.push(primary_process_name(&current_primary), first.0, first.1);

    perform_rejoin(layout, current_primary.replica_id, primary.replica_id)?;

    let second = reserve_event(
        layout,
        &current_primary,
        OperationId(base_id + 2),
        resource_id,
        HolderId(202),
        Slot(11),
        5,
    )?;
    history.push(primary_process_name(&current_primary), second.0, second.1);
    Ok(history.finish())
}

fn run_crash_restart_ambiguous_retry(
    layout: &QemuTestbedLayout,
    primary: &LocalClusterReplicaConfig,
    failover_target: &LocalClusterReplicaConfig,
    supporting_backup: &LocalClusterReplicaConfig,
    base_id: u128,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id + 1);
    let operation_id = OperationId(base_id + 10);
    create_qemu_resource(layout, primary, resource_id, OperationId(base_id + 101))?;

    maybe_crash_replica(layout, failover_target.replica_id)?;
    maybe_crash_replica(layout, supporting_backup.replica_id)?;

    let mut history = HistoryBuilder::default();
    let first = reserve_event(
        layout,
        primary,
        operation_id,
        resource_id,
        HolderId(303),
        Slot(20),
        4,
    )?;
    history.push(primary_process_name(primary), first.0, first.1);

    maybe_crash_replica(layout, primary.replica_id)?;
    perform_failover(
        layout,
        primary.replica_id,
        failover_target.replica_id,
        supporting_backup.replica_id,
    )?;

    let current_primary = primary_replica(layout)?;
    let retry = reserve_event(
        layout,
        &current_primary,
        operation_id,
        resource_id,
        HolderId(303),
        Slot(20),
        4,
    )?;
    history.push(primary_process_name(&current_primary), retry.0, retry.1);

    perform_rejoin(layout, current_primary.replica_id, primary.replica_id)?;
    Ok(history.finish())
}

fn run_crash_restart_failover_reads(
    layout: &QemuTestbedLayout,
    primary: &LocalClusterReplicaConfig,
    failover_target: &LocalClusterReplicaConfig,
    supporting_backup: &LocalClusterReplicaConfig,
    base_id: u128,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id + 2);
    create_qemu_resource(layout, primary, resource_id, OperationId(base_id + 102))?;

    let mut history = HistoryBuilder::default();
    let (reserve_operation, reserve_outcome, reserve_commit) = reserve_event(
        layout,
        primary,
        OperationId(base_id + 20),
        resource_id,
        HolderId(404),
        Slot(30),
        6,
    )?;
    let reserve_commit = reserve_commit.ok_or_else(|| {
        String::from("crash-restart failover_read_fences expected one committed reserve")
    })?;
    history.push(
        primary_process_name(primary),
        reserve_operation,
        reserve_outcome,
    );

    maybe_crash_replica(layout, primary.replica_id)?;
    perform_failover(
        layout,
        primary.replica_id,
        failover_target.replica_id,
        supporting_backup.replica_id,
    )?;

    let current_primary = primary_replica(layout)?;
    let primary_read = reservation_read_event(
        layout,
        &current_primary,
        reserve_commit.reservation_id,
        Slot(30),
        Some(reserve_commit.applied_lsn),
    )?;
    history.push(
        primary_process_name(&current_primary),
        primary_read.0,
        primary_read.1,
    );

    perform_rejoin(layout, current_primary.replica_id, primary.replica_id)?;
    let old_primary = runtime_replica_by_id(layout, primary.replica_id)?;
    let stale_read = reservation_read_event(
        layout,
        &old_primary,
        reserve_commit.reservation_id,
        Slot(30),
        Some(reserve_commit.applied_lsn),
    )?;
    history.push(
        backup_process_name(&old_primary),
        stale_read.0,
        stale_read.1,
    );
    Ok(history.finish())
}

fn run_crash_restart_expiration_recovery(
    layout: &QemuTestbedLayout,
    primary: &LocalClusterReplicaConfig,
    failover_target: &LocalClusterReplicaConfig,
    supporting_backup: &LocalClusterReplicaConfig,
    base_id: u128,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id + 3);
    create_qemu_resource(layout, primary, resource_id, OperationId(base_id + 103))?;

    let mut history = HistoryBuilder::default();
    let (reserve_operation, reserve_outcome, reserve_commit) = reserve_event(
        layout,
        primary,
        OperationId(base_id + 30),
        resource_id,
        HolderId(505),
        Slot(40),
        2,
    )?;
    let reserve_commit = reserve_commit.ok_or_else(|| {
        String::from("crash-restart expiration_and_recovery expected one committed reserve")
    })?;
    history.push(
        primary_process_name(primary),
        reserve_operation,
        reserve_outcome,
    );

    maybe_crash_replica(layout, primary.replica_id)?;
    perform_failover(
        layout,
        primary.replica_id,
        failover_target.replica_id,
        supporting_backup.replica_id,
    )?;

    let current_primary = primary_replica(layout)?;
    let tick = tick_expirations_event(
        layout,
        &current_primary,
        OperationId(base_id + 31),
        Slot(42),
        &[JepsenExpiredReservation {
            resource_id,
            holder_id: 505,
            reservation_id: reserve_commit.reservation_id.get(),
            released_lsn: None,
        }],
    )?;
    let tick_lsn = tick.2.ok_or_else(|| {
        String::from("crash-restart expiration_and_recovery expected one committed expiration tick")
    })?;
    history.push(primary_process_name(&current_primary), tick.0, tick.1);

    let resource_read =
        resource_available_event(layout, &current_primary, resource_id, Some(tick_lsn))?;
    history.push(
        primary_process_name(&current_primary),
        resource_read.0,
        resource_read.1,
    );
    let reservation_read = reservation_read_event(
        layout,
        &current_primary,
        reserve_commit.reservation_id,
        Slot(42),
        Some(tick_lsn),
    )?;
    history.push(
        primary_process_name(&current_primary),
        reservation_read.0,
        reservation_read.1,
    );

    perform_rejoin(layout, current_primary.replica_id, primary.replica_id)?;
    Ok(history.finish())
}

fn run_partition_heal_reservation_contention(
    layout: &QemuTestbedLayout,
    primary: &LocalClusterReplicaConfig,
    failover_target: &LocalClusterReplicaConfig,
    supporting_backup: &LocalClusterReplicaConfig,
    base_id: u128,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id + 10);
    let operation_id = OperationId(base_id + 11);
    create_qemu_resource(layout, primary, resource_id, OperationId(base_id + 110))?;

    isolate_replica(layout, failover_target.replica_id)?;
    isolate_replica(layout, supporting_backup.replica_id)?;

    let mut history = HistoryBuilder::default();
    let first = reserve_event(
        layout,
        primary,
        operation_id,
        resource_id,
        HolderId(601),
        Slot(50),
        5,
    )?;
    history.push(primary_process_name(primary), first.0, first.1);

    heal_replica(layout, failover_target.replica_id)?;
    heal_replica(layout, supporting_backup.replica_id)?;
    maybe_crash_replica(layout, primary.replica_id)?;
    perform_failover(
        layout,
        primary.replica_id,
        failover_target.replica_id,
        supporting_backup.replica_id,
    )?;

    let current_primary = primary_replica(layout)?;
    let retry = reserve_event(
        layout,
        &current_primary,
        operation_id,
        resource_id,
        HolderId(601),
        Slot(50),
        5,
    )?;
    history.push(primary_process_name(&current_primary), retry.0, retry.1);
    let second = reserve_event(
        layout,
        &current_primary,
        OperationId(base_id + 12),
        resource_id,
        HolderId(602),
        Slot(51),
        5,
    )?;
    history.push(primary_process_name(&current_primary), second.0, second.1);

    perform_rejoin(layout, current_primary.replica_id, primary.replica_id)?;
    Ok(history.finish())
}

fn run_partition_heal_ambiguous_retry(
    layout: &QemuTestbedLayout,
    primary: &LocalClusterReplicaConfig,
    failover_target: &LocalClusterReplicaConfig,
    supporting_backup: &LocalClusterReplicaConfig,
    base_id: u128,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id + 11);
    let operation_id = OperationId(base_id + 20);
    create_qemu_resource(layout, primary, resource_id, OperationId(base_id + 120))?;

    isolate_replica(layout, failover_target.replica_id)?;
    isolate_replica(layout, supporting_backup.replica_id)?;

    let mut history = HistoryBuilder::default();
    let first = reserve_event(
        layout,
        primary,
        operation_id,
        resource_id,
        HolderId(603),
        Slot(52),
        4,
    )?;
    history.push(primary_process_name(primary), first.0, first.1);

    heal_replica(layout, failover_target.replica_id)?;
    heal_replica(layout, supporting_backup.replica_id)?;
    maybe_crash_replica(layout, primary.replica_id)?;
    perform_failover(
        layout,
        primary.replica_id,
        failover_target.replica_id,
        supporting_backup.replica_id,
    )?;

    let current_primary = primary_replica(layout)?;
    let retry = reserve_event(
        layout,
        &current_primary,
        operation_id,
        resource_id,
        HolderId(603),
        Slot(52),
        4,
    )?;
    history.push(primary_process_name(&current_primary), retry.0, retry.1);
    perform_rejoin(layout, current_primary.replica_id, primary.replica_id)?;
    Ok(history.finish())
}

fn run_partition_heal_failover_reads(
    layout: &QemuTestbedLayout,
    primary: &LocalClusterReplicaConfig,
    failover_target: &LocalClusterReplicaConfig,
    supporting_backup: &LocalClusterReplicaConfig,
    base_id: u128,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id + 12);
    create_qemu_resource(layout, primary, resource_id, OperationId(base_id + 130))?;

    let mut history = HistoryBuilder::default();
    let (reserve_operation, reserve_outcome, reserve_commit) = reserve_event(
        layout,
        primary,
        OperationId(base_id + 21),
        resource_id,
        HolderId(604),
        Slot(53),
        6,
    )?;
    let reserve_commit = reserve_commit.ok_or_else(|| {
        String::from("partition-heal failover_read_fences expected one committed reserve")
    })?;
    history.push(
        primary_process_name(primary),
        reserve_operation,
        reserve_outcome,
    );

    isolate_replica(layout, failover_target.replica_id)?;
    isolate_replica(layout, supporting_backup.replica_id)?;
    let minority_write = reserve_event(
        layout,
        primary,
        OperationId(base_id + 22),
        resource_id,
        HolderId(605),
        Slot(54),
        2,
    )?;
    history.push(
        primary_process_name(primary),
        minority_write.0,
        minority_write.1,
    );

    heal_replica(layout, failover_target.replica_id)?;
    heal_replica(layout, supporting_backup.replica_id)?;
    maybe_crash_replica(layout, primary.replica_id)?;
    perform_failover(
        layout,
        primary.replica_id,
        failover_target.replica_id,
        supporting_backup.replica_id,
    )?;

    let current_primary = primary_replica(layout)?;
    let primary_read = reservation_read_event(
        layout,
        &current_primary,
        reserve_commit.reservation_id,
        Slot(54),
        Some(reserve_commit.applied_lsn),
    )?;
    history.push(
        primary_process_name(&current_primary),
        primary_read.0,
        primary_read.1,
    );
    perform_rejoin(layout, current_primary.replica_id, primary.replica_id)?;
    let old_primary = runtime_replica_by_id(layout, primary.replica_id)?;
    let stale_read = reservation_read_event(
        layout,
        &old_primary,
        reserve_commit.reservation_id,
        Slot(54),
        Some(reserve_commit.applied_lsn),
    )?;
    history.push(
        backup_process_name(&old_primary),
        stale_read.0,
        stale_read.1,
    );
    Ok(history.finish())
}

fn run_partition_heal_expiration_recovery(
    layout: &QemuTestbedLayout,
    primary: &LocalClusterReplicaConfig,
    failover_target: &LocalClusterReplicaConfig,
    supporting_backup: &LocalClusterReplicaConfig,
    base_id: u128,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id + 13);
    create_qemu_resource(layout, primary, resource_id, OperationId(base_id + 140))?;

    let mut history = HistoryBuilder::default();
    let (reserve_operation, reserve_outcome, reserve_commit) = reserve_event(
        layout,
        primary,
        OperationId(base_id + 23),
        resource_id,
        HolderId(606),
        Slot(55),
        2,
    )?;
    let reserve_commit = reserve_commit.ok_or_else(|| {
        String::from("partition-heal expiration_and_recovery expected one committed reserve")
    })?;
    history.push(
        primary_process_name(primary),
        reserve_operation,
        reserve_outcome,
    );

    isolate_replica(layout, failover_target.replica_id)?;
    isolate_replica(layout, supporting_backup.replica_id)?;
    let failed_tick = tick_expirations_event(
        layout,
        primary,
        OperationId(base_id + 24),
        Slot(57),
        &[JepsenExpiredReservation {
            resource_id,
            holder_id: 606,
            reservation_id: reserve_commit.reservation_id.get(),
            released_lsn: None,
        }],
    )?;
    history.push(primary_process_name(primary), failed_tick.0, failed_tick.1);

    heal_replica(layout, failover_target.replica_id)?;
    heal_replica(layout, supporting_backup.replica_id)?;
    maybe_crash_replica(layout, primary.replica_id)?;
    perform_failover(
        layout,
        primary.replica_id,
        failover_target.replica_id,
        supporting_backup.replica_id,
    )?;

    let current_primary = primary_replica(layout)?;
    let successful_tick = tick_expirations_event(
        layout,
        &current_primary,
        OperationId(base_id + 24),
        Slot(57),
        &[JepsenExpiredReservation {
            resource_id,
            holder_id: 606,
            reservation_id: reserve_commit.reservation_id.get(),
            released_lsn: None,
        }],
    )?;
    let tick_lsn = successful_tick.2.ok_or_else(|| {
        String::from(
            "partition-heal expiration_and_recovery expected one committed expiration tick",
        )
    })?;
    history.push(
        primary_process_name(&current_primary),
        successful_tick.0,
        successful_tick.1,
    );
    let resource_read =
        resource_available_event(layout, &current_primary, resource_id, Some(tick_lsn))?;
    history.push(
        primary_process_name(&current_primary),
        resource_read.0,
        resource_read.1,
    );
    perform_rejoin(layout, current_primary.replica_id, primary.replica_id)?;
    Ok(history.finish())
}

fn run_mixed_failover_ambiguous_retry(
    layout: &QemuTestbedLayout,
    primary: &LocalClusterReplicaConfig,
    failover_target: &LocalClusterReplicaConfig,
    supporting_backup: &LocalClusterReplicaConfig,
    base_id: u128,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id + 20);
    let operation_id = OperationId(base_id + 30);
    create_qemu_resource(layout, primary, resource_id, OperationId(base_id + 150))?;

    maybe_crash_replica(layout, failover_target.replica_id)?;
    isolate_replica(layout, supporting_backup.replica_id)?;

    let mut history = HistoryBuilder::default();
    let first = reserve_event(
        layout,
        primary,
        operation_id,
        resource_id,
        HolderId(701),
        Slot(60),
        4,
    )?;
    history.push(primary_process_name(primary), first.0, first.1);

    maybe_crash_replica(layout, primary.replica_id)?;
    heal_replica(layout, supporting_backup.replica_id)?;
    perform_failover(
        layout,
        primary.replica_id,
        failover_target.replica_id,
        supporting_backup.replica_id,
    )?;

    let current_primary = primary_replica(layout)?;
    let retry = reserve_event(
        layout,
        &current_primary,
        operation_id,
        resource_id,
        HolderId(701),
        Slot(60),
        4,
    )?;
    history.push(primary_process_name(&current_primary), retry.0, retry.1);
    perform_rejoin(layout, current_primary.replica_id, primary.replica_id)?;
    Ok(history.finish())
}

fn run_mixed_failover_reads(
    layout: &QemuTestbedLayout,
    primary: &LocalClusterReplicaConfig,
    failover_target: &LocalClusterReplicaConfig,
    supporting_backup: &LocalClusterReplicaConfig,
    base_id: u128,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id + 21);
    create_qemu_resource(layout, primary, resource_id, OperationId(base_id + 160))?;

    let mut history = HistoryBuilder::default();
    let (reserve_operation, reserve_outcome, reserve_commit) = reserve_event(
        layout,
        primary,
        OperationId(base_id + 31),
        resource_id,
        HolderId(702),
        Slot(61),
        6,
    )?;
    let reserve_commit = reserve_commit
        .ok_or_else(|| String::from("mixed failover read fences expected one committed reserve"))?;
    history.push(
        primary_process_name(primary),
        reserve_operation,
        reserve_outcome,
    );

    isolate_replica(layout, supporting_backup.replica_id)?;
    maybe_crash_replica(layout, primary.replica_id)?;
    heal_replica(layout, supporting_backup.replica_id)?;
    perform_failover(
        layout,
        primary.replica_id,
        failover_target.replica_id,
        supporting_backup.replica_id,
    )?;

    let current_primary = primary_replica(layout)?;
    let primary_read = reservation_read_event(
        layout,
        &current_primary,
        reserve_commit.reservation_id,
        Slot(61),
        Some(reserve_commit.applied_lsn),
    )?;
    history.push(
        primary_process_name(&current_primary),
        primary_read.0,
        primary_read.1,
    );
    perform_rejoin(layout, current_primary.replica_id, primary.replica_id)?;
    let old_primary = runtime_replica_by_id(layout, primary.replica_id)?;
    let stale_read = reservation_read_event(
        layout,
        &old_primary,
        reserve_commit.reservation_id,
        Slot(61),
        Some(reserve_commit.applied_lsn),
    )?;
    history.push(
        backup_process_name(&old_primary),
        stale_read.0,
        stale_read.1,
    );
    Ok(history.finish())
}

fn run_mixed_failover_expiration_recovery(
    layout: &QemuTestbedLayout,
    primary: &LocalClusterReplicaConfig,
    failover_target: &LocalClusterReplicaConfig,
    supporting_backup: &LocalClusterReplicaConfig,
    base_id: u128,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id + 22);
    create_qemu_resource(layout, primary, resource_id, OperationId(base_id + 170))?;

    let mut history = HistoryBuilder::default();
    let (reserve_operation, reserve_outcome, reserve_commit) = reserve_event(
        layout,
        primary,
        OperationId(base_id + 32),
        resource_id,
        HolderId(703),
        Slot(62),
        2,
    )?;
    let reserve_commit = reserve_commit.ok_or_else(|| {
        String::from("mixed failover expiration_and_recovery expected one committed reserve")
    })?;
    history.push(
        primary_process_name(primary),
        reserve_operation,
        reserve_outcome,
    );

    isolate_replica(layout, supporting_backup.replica_id)?;
    maybe_crash_replica(layout, primary.replica_id)?;
    heal_replica(layout, supporting_backup.replica_id)?;
    perform_failover(
        layout,
        primary.replica_id,
        failover_target.replica_id,
        supporting_backup.replica_id,
    )?;

    let current_primary = primary_replica(layout)?;
    let tick = tick_expirations_event(
        layout,
        &current_primary,
        OperationId(base_id + 33),
        Slot(64),
        &[JepsenExpiredReservation {
            resource_id,
            holder_id: 703,
            reservation_id: reserve_commit.reservation_id.get(),
            released_lsn: None,
        }],
    )?;
    let tick_lsn = tick.2.ok_or_else(|| {
        String::from(
            "mixed failover expiration_and_recovery expected one committed expiration tick",
        )
    })?;
    history.push(primary_process_name(&current_primary), tick.0, tick.1);
    let resource_read =
        resource_available_event(layout, &current_primary, resource_id, Some(tick_lsn))?;
    history.push(
        primary_process_name(&current_primary),
        resource_read.0,
        resource_read.1,
    );
    perform_rejoin(layout, current_primary.replica_id, primary.replica_id)?;
    Ok(history.finish())
}

fn run_reservation_contention(
    layout: &QemuTestbedLayout,
    primary: &LocalClusterReplicaConfig,
    base_id: u128,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id);
    create_qemu_resource(layout, primary, resource_id, OperationId(base_id + 100))?;

    let mut history = HistoryBuilder::default();
    let first = reserve_event(
        layout,
        primary,
        OperationId(base_id + 1),
        resource_id,
        HolderId(101),
        Slot(10),
        5,
    )?;
    history.push(primary_process_name(primary), first.0, first.1);

    let second = reserve_event(
        layout,
        primary,
        OperationId(base_id + 2),
        resource_id,
        HolderId(202),
        Slot(11),
        5,
    )?;
    history.push(primary_process_name(primary), second.0, second.1);
    Ok(history.finish())
}

fn run_ambiguous_write_retry(
    layout: &QemuTestbedLayout,
    primary: &LocalClusterReplicaConfig,
    base_id: u128,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id + 1);
    let operation_id = OperationId(base_id + 10);
    create_qemu_resource(layout, primary, resource_id, OperationId(base_id + 101))?;

    let mut history = HistoryBuilder::default();
    let first = reserve_event(
        layout,
        primary,
        operation_id,
        resource_id,
        HolderId(303),
        Slot(20),
        4,
    )?;
    history.push(primary_process_name(primary), first.0, first.1);

    let retry = reserve_event(
        layout,
        primary,
        operation_id,
        resource_id,
        HolderId(303),
        Slot(20),
        4,
    )?;
    history.push(primary_process_name(primary), retry.0, retry.1);
    Ok(history.finish())
}

fn run_failover_read_fences(
    layout: &QemuTestbedLayout,
    primary: &LocalClusterReplicaConfig,
    backup: &LocalClusterReplicaConfig,
    base_id: u128,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id + 2);
    create_qemu_resource(layout, primary, resource_id, OperationId(base_id + 102))?;

    let mut history = HistoryBuilder::default();
    let (reserve_operation, reserve_outcome, reserve_commit) = reserve_event(
        layout,
        primary,
        OperationId(base_id + 20),
        resource_id,
        HolderId(404),
        Slot(30),
        6,
    )?;
    let reserve_commit = reserve_commit.ok_or_else(|| {
        String::from("failover_read_fences control run expected one committed reserve")
    })?;
    history.push(
        primary_process_name(primary),
        reserve_operation,
        reserve_outcome,
    );

    let primary_read = reservation_read_event(
        layout,
        primary,
        reserve_commit.reservation_id,
        Slot(30),
        Some(reserve_commit.applied_lsn),
    )?;
    history.push(
        primary_process_name(primary),
        primary_read.0,
        primary_read.1,
    );

    let backup_read = reservation_read_event(
        layout,
        backup,
        reserve_commit.reservation_id,
        Slot(30),
        Some(reserve_commit.applied_lsn),
    )?;
    history.push(backup_process_name(backup), backup_read.0, backup_read.1);
    Ok(history.finish())
}

fn run_expiration_and_recovery(
    layout: &QemuTestbedLayout,
    primary: &LocalClusterReplicaConfig,
    base_id: u128,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id + 3);
    create_qemu_resource(layout, primary, resource_id, OperationId(base_id + 103))?;

    let mut history = HistoryBuilder::default();
    let (reserve_operation, reserve_outcome, reserve_commit) = reserve_event(
        layout,
        primary,
        OperationId(base_id + 30),
        resource_id,
        HolderId(505),
        Slot(40),
        2,
    )?;
    let reserve_commit = reserve_commit.ok_or_else(|| {
        String::from("expiration_and_recovery control run expected one committed reserve")
    })?;
    history.push(
        primary_process_name(primary),
        reserve_operation,
        reserve_outcome,
    );

    let tick = tick_expirations_event(
        layout,
        primary,
        OperationId(base_id + 31),
        Slot(42),
        &[JepsenExpiredReservation {
            resource_id,
            holder_id: 505,
            reservation_id: reserve_commit.reservation_id.get(),
            released_lsn: None,
        }],
    )?;
    let tick_lsn = tick.2.ok_or_else(|| {
        String::from("expiration_and_recovery control run expected one committed expiration tick")
    })?;
    history.push(primary_process_name(primary), tick.0, tick.1);

    let resource_read = resource_available_event(layout, primary, resource_id, Some(tick_lsn))?;
    history.push(
        primary_process_name(primary),
        resource_read.0,
        resource_read.1,
    );

    let reservation_read = reservation_read_event(
        layout,
        primary,
        reserve_commit.reservation_id,
        Slot(42),
        Some(tick_lsn),
    )?;
    history.push(
        primary_process_name(primary),
        reservation_read.0,
        reservation_read.1,
    );
    Ok(history.finish())
}

fn create_qemu_resource(
    layout: &QemuTestbedLayout,
    primary: &LocalClusterReplicaConfig,
    resource_id: ResourceId,
    operation_id: OperationId,
) -> Result<(), String> {
    let request = ApiRequest::Submit(SubmitRequest::from_client_request(
        Slot(1),
        ClientRequest {
            operation_id,
            client_id: ClientId(7),
            command: AllocCommand::CreateResource { resource_id },
        },
    ));
    match send_replica_api_request(layout, primary, &request)? {
        RemoteApiOutcome::Api(ApiResponse::Submit(SubmitResponse::Committed(response)))
            if response.outcome.result_code == ResultCode::Ok =>
        {
            Ok(())
        }
        RemoteApiOutcome::Api(other) => Err(format!(
            "resource setup for {} returned unexpected response {other:?}",
            resource_id.get()
        )),
        RemoteApiOutcome::Text(text) => Err(format!(
            "resource setup for {} returned undecodable response {text}",
            resource_id.get()
        )),
    }
}

fn reserve_event(
    layout: &QemuTestbedLayout,
    replica: &LocalClusterReplicaConfig,
    operation_id: OperationId,
    resource_id: ResourceId,
    holder_id: HolderId,
    request_slot: Slot,
    ttl_slots: u64,
) -> Result<(JepsenOperation, JepsenEventOutcome, Option<ReserveCommit>), String> {
    let operation = JepsenOperation {
        kind: JepsenOperationKind::Reserve,
        operation_id: Some(operation_id.get()),
        resource_id: Some(resource_id),
        reservation_id: None,
        holder_id: Some(holder_id.get()),
        required_lsn: None,
        request_slot: Some(request_slot),
        ttl_slots: Some(ttl_slots),
    };
    let request = ApiRequest::Submit(SubmitRequest::from_client_request(
        request_slot,
        ClientRequest {
            operation_id,
            client_id: ClientId(7),
            command: AllocCommand::Reserve {
                resource_id,
                holder_id,
                ttl_slots,
            },
        },
    ));
    match send_replica_api_request(layout, replica, &request)? {
        RemoteApiOutcome::Api(ApiResponse::Submit(response)) => {
            map_reserve_submit_response(operation, resource_id, holder_id, response)
        }
        RemoteApiOutcome::Api(other) => {
            Err(format!("reserve returned unexpected response {other:?}"))
        }
        RemoteApiOutcome::Text(text) if response_text_is_not_primary(&text) => Ok((
            operation,
            JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::NotPrimary),
            None,
        )),
        RemoteApiOutcome::Text(text) => {
            Err(format!("reserve returned undecodable response {text}"))
        }
    }
}

fn map_reserve_submit_response(
    operation: JepsenOperation,
    resource_id: ResourceId,
    holder_id: HolderId,
    response: SubmitResponse,
) -> Result<(JepsenOperation, JepsenEventOutcome, Option<ReserveCommit>), String> {
    match response {
        SubmitResponse::Committed(response) => match response.outcome.result_code {
            ResultCode::Ok => {
                let reservation_id = response
                    .outcome
                    .reservation_id
                    .ok_or_else(|| String::from("reserve commit missing reservation_id"))?;
                let expires_at_slot = response
                    .outcome
                    .deadline_slot
                    .ok_or_else(|| String::from("reserve commit missing deadline_slot"))?;
                Ok((
                    operation,
                    JepsenEventOutcome::CommittedWrite(JepsenCommittedWrite {
                        applied_lsn: response.applied_lsn,
                        result: JepsenWriteResult::Reserved {
                            resource_id,
                            holder_id: holder_id.get(),
                            reservation_id: reservation_id.get(),
                            expires_at_slot,
                        },
                    }),
                    Some(ReserveCommit {
                        applied_lsn: response.applied_lsn,
                        reservation_id,
                    }),
                ))
            }
            ResultCode::ResourceBusy => Ok((
                operation,
                JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::Busy),
                None,
            )),
            ResultCode::OperationConflict => Ok((
                operation,
                JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::Conflict),
                None,
            )),
            ResultCode::ResourceNotFound | ResultCode::ReservationNotFound => Ok((
                operation,
                JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::NotFound),
                None,
            )),
            ResultCode::ReservationRetired => Ok((
                operation,
                JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::Retired),
                None,
            )),
            other => Err(format!("unexpected reserve result code {other:?}")),
        },
        SubmitResponse::Rejected(failure) => {
            Ok((operation, outcome_from_submission_failure(failure), None))
        }
    }
}

fn tick_expirations_event(
    layout: &QemuTestbedLayout,
    replica: &LocalClusterReplicaConfig,
    operation_id: OperationId,
    current_wall_clock_slot: Slot,
    expired: &[JepsenExpiredReservation],
) -> Result<(JepsenOperation, JepsenEventOutcome, Option<Lsn>), String> {
    let operation = JepsenOperation {
        kind: JepsenOperationKind::TickExpirations,
        operation_id: Some(operation_id.get()),
        resource_id: None,
        reservation_id: None,
        holder_id: None,
        required_lsn: None,
        request_slot: Some(current_wall_clock_slot),
        ttl_slots: None,
    };
    let request = ApiRequest::TickExpirations(TickExpirationsRequest {
        current_wall_clock_slot,
    });
    match send_replica_api_request(layout, replica, &request)? {
        RemoteApiOutcome::Api(ApiResponse::TickExpirations(response)) => match response {
            TickExpirationsResponse::Applied(response) => {
                let applied_lsn = response.last_applied_lsn.ok_or_else(|| {
                    String::from("tick_expirations applied without one committed lsn")
                })?;
                let expired = expired
                    .iter()
                    .map(|entry| JepsenExpiredReservation {
                        resource_id: entry.resource_id,
                        holder_id: entry.holder_id,
                        reservation_id: entry.reservation_id,
                        released_lsn: Some(applied_lsn),
                    })
                    .collect();
                Ok((
                    operation,
                    JepsenEventOutcome::CommittedWrite(JepsenCommittedWrite {
                        applied_lsn,
                        result: JepsenWriteResult::TickExpired { expired },
                    }),
                    Some(applied_lsn),
                ))
            }
            TickExpirationsResponse::Rejected(failure) => {
                Ok((operation, outcome_from_submission_failure(failure), None))
            }
        },
        RemoteApiOutcome::Api(other) => Err(format!(
            "tick_expirations returned unexpected response {other:?}"
        )),
        RemoteApiOutcome::Text(text) if response_text_is_not_primary(&text) => Ok((
            operation,
            JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::NotPrimary),
            None,
        )),
        RemoteApiOutcome::Text(text) => Err(format!(
            "tick_expirations returned undecodable response {text}"
        )),
    }
}

fn resource_available_event(
    layout: &QemuTestbedLayout,
    replica: &LocalClusterReplicaConfig,
    resource_id: ResourceId,
    required_lsn: Option<Lsn>,
) -> Result<(JepsenOperation, JepsenEventOutcome), String> {
    let operation = JepsenOperation {
        kind: JepsenOperationKind::GetResource,
        operation_id: None,
        resource_id: Some(resource_id),
        reservation_id: None,
        holder_id: None,
        required_lsn,
        request_slot: None,
        ttl_slots: None,
    };
    let request = ApiRequest::GetResource(ResourceRequest {
        resource_id,
        required_lsn,
    });
    match send_replica_api_request(layout, replica, &request)? {
        RemoteApiOutcome::Api(ApiResponse::GetResource(ResourceResponse::Found(resource)))
            if matches!(resource.state, allocdb_core::ResourceState::Available) =>
        {
            Ok((
                operation,
                JepsenEventOutcome::SuccessfulRead(JepsenSuccessfulRead {
                    target: JepsenReadTarget::Resource,
                    served_by: replica.replica_id,
                    served_role: replica.role,
                    observed_lsn: required_lsn,
                    state: JepsenReadState::Resource(
                        allocdb_node::jepsen::JepsenResourceState::Available,
                    ),
                }),
            ))
        }
        RemoteApiOutcome::Api(ApiResponse::GetResource(ResourceResponse::NotFound)) => Ok((
            operation,
            JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::NotFound),
        )),
        RemoteApiOutcome::Api(ApiResponse::GetResource(ResourceResponse::FenceNotApplied {
            ..
        })) => Ok((
            operation,
            JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::FenceNotApplied),
        )),
        RemoteApiOutcome::Api(ApiResponse::GetResource(ResourceResponse::EngineHalted)) => Ok((
            operation,
            JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::EngineHalted),
        )),
        RemoteApiOutcome::Api(other) => Err(format!(
            "resource read for {} returned unexpected response {other:?}",
            resource_id.get()
        )),
        RemoteApiOutcome::Text(text) if response_text_is_not_primary(&text) => Ok((
            operation,
            JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::NotPrimary),
        )),
        RemoteApiOutcome::Text(text) => Err(format!(
            "resource read for {} returned undecodable response {text}",
            resource_id.get()
        )),
    }
}

fn reservation_read_event(
    layout: &QemuTestbedLayout,
    replica: &LocalClusterReplicaConfig,
    reservation_id: ReservationId,
    current_slot: Slot,
    required_lsn: Option<Lsn>,
) -> Result<(JepsenOperation, JepsenEventOutcome), String> {
    let operation = JepsenOperation {
        kind: JepsenOperationKind::GetReservation,
        operation_id: None,
        resource_id: None,
        reservation_id: Some(reservation_id.get()),
        holder_id: None,
        required_lsn,
        request_slot: Some(current_slot),
        ttl_slots: None,
    };
    let request = ApiRequest::GetReservation(ReservationRequest {
        reservation_id,
        current_slot,
        required_lsn,
    });
    match send_replica_api_request(layout, replica, &request)? {
        RemoteApiOutcome::Api(ApiResponse::GetReservation(ReservationResponse::Found(
            reservation,
        ))) => Ok((
            operation,
            JepsenEventOutcome::SuccessfulRead(JepsenSuccessfulRead {
                target: JepsenReadTarget::Reservation,
                served_by: replica.replica_id,
                served_role: replica.role,
                observed_lsn: required_lsn,
                state: JepsenReadState::Reservation(map_reservation_state(reservation)),
            }),
        )),
        RemoteApiOutcome::Api(ApiResponse::GetReservation(ReservationResponse::NotFound)) => Ok((
            operation,
            JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::NotFound),
        )),
        RemoteApiOutcome::Api(ApiResponse::GetReservation(ReservationResponse::Retired)) => Ok((
            operation,
            JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::Retired),
        )),
        RemoteApiOutcome::Api(ApiResponse::GetReservation(
            ReservationResponse::FenceNotApplied { .. },
        )) => Ok((
            operation,
            JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::FenceNotApplied),
        )),
        RemoteApiOutcome::Api(ApiResponse::GetReservation(ReservationResponse::EngineHalted)) => {
            Ok((
                operation,
                JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::EngineHalted),
            ))
        }
        RemoteApiOutcome::Api(other) => Err(format!(
            "reservation read for {} returned unexpected response {other:?}",
            reservation_id.get()
        )),
        RemoteApiOutcome::Text(text) if response_text_is_not_primary(&text) => Ok((
            operation,
            JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::NotPrimary),
        )),
        RemoteApiOutcome::Text(text) => Err(format!(
            "reservation read for {} returned undecodable response {text}",
            reservation_id.get()
        )),
    }
}

fn map_reservation_state(reservation: allocdb_node::ReservationView) -> JepsenReservationState {
    match reservation.state {
        ReservationState::Reserved => JepsenReservationState::Active {
            resource_id: reservation.resource_id,
            holder_id: reservation.holder_id.get(),
            expires_at_slot: reservation.deadline_slot,
            confirmed: false,
        },
        ReservationState::Confirmed => JepsenReservationState::Active {
            resource_id: reservation.resource_id,
            holder_id: reservation.holder_id.get(),
            expires_at_slot: reservation.deadline_slot,
            confirmed: true,
        },
        ReservationState::Released | ReservationState::Expired => {
            JepsenReservationState::Released {
                resource_id: reservation.resource_id,
                holder_id: reservation.holder_id.get(),
                released_lsn: reservation.released_lsn,
            }
        }
    }
}

fn outcome_from_submission_failure(failure: SubmissionFailure) -> JepsenEventOutcome {
    if failure.category == allocdb_node::SubmissionErrorCategory::Indefinite {
        return JepsenEventOutcome::Ambiguous(JepsenAmbiguousOutcome::IndefiniteWrite);
    }

    let failure = match failure.code {
        SubmissionFailureCode::Overloaded { .. } => JepsenDefiniteFailure::Busy,
        SubmissionFailureCode::EngineHalted | SubmissionFailureCode::LsnExhausted { .. } => {
            JepsenDefiniteFailure::EngineHalted
        }
        SubmissionFailureCode::InvalidRequest(_)
        | SubmissionFailureCode::SlotOverflow { .. }
        | SubmissionFailureCode::CommandTooLarge { .. } => JepsenDefiniteFailure::InvalidRequest,
        SubmissionFailureCode::StorageFailure => JepsenDefiniteFailure::EngineHalted,
    };
    JepsenEventOutcome::DefiniteFailure(failure)
}

fn send_replica_api_request(
    layout: &QemuTestbedLayout,
    replica: &LocalClusterReplicaConfig,
    request: &ApiRequest,
) -> Result<RemoteApiOutcome, String> {
    let response_bytes = send_remote_api_request(
        layout,
        &replica.client_addr.ip().to_string(),
        replica.client_addr.port(),
        request,
    )?;
    match decode_response(&response_bytes) {
        Ok(response) => Ok(RemoteApiOutcome::Api(response)),
        Err(_) => Ok(RemoteApiOutcome::Text(
            String::from_utf8_lossy(&response_bytes).trim().to_owned(),
        )),
    }
}

fn primary_process_name(replica: &LocalClusterReplicaConfig) -> String {
    format!("primary-{}", replica.replica_id.get())
}

fn backup_process_name(replica: &LocalClusterReplicaConfig) -> String {
    format!("backup-{}", replica.replica_id.get())
}

fn response_text_is_not_primary(text: &str) -> bool {
    text.contains("not primary")
}

fn current_time_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn load_qemu_layout(workspace_root: &Path) -> Result<QemuTestbedLayout, String> {
    let path = qemu_testbed_layout_path(workspace_root);
    QemuTestbedLayout::load(path)
        .map_err(|error| format!("failed to load qemu testbed layout: {error}"))
}

fn fetch_qemu_logs_archive(
    layout: &QemuTestbedLayout,
    run_id: &str,
    output_root: &Path,
) -> Result<PathBuf, String> {
    fs::create_dir_all(output_root).map_err(|error| {
        format!(
            "failed to create output root {}: {error}",
            output_root.display()
        )
    })?;
    let sanitized_run_id = sanitize_run_id(run_id);
    let created_at_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let remote_dir = format!(
        "/var/lib/allocdb-qemu/log-bundles/allocdb-jepsen-{sanitized_run_id}-{created_at_millis}"
    );
    let remote_parent = Path::new(&remote_dir)
        .parent()
        .and_then(Path::to_str)
        .ok_or_else(|| format!("remote log bundle path {remote_dir} has no parent"))?;
    let remote_name = Path::new(&remote_dir)
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("remote log bundle path {remote_dir} has no file name"))?;
    let archive_path = output_root.join(format!("{sanitized_run_id}-qemu-logs.tar.gz"));

    let mut remote_args = ssh_args(layout);
    remote_args.push(format!(
        "sudo {REMOTE_CONTROL_SCRIPT_PATH} collect-logs {remote_dir} >/dev/null && sudo tar czf - -C {remote_parent} {remote_name}"
    ));
    let output = Command::new("ssh")
        .args(remote_args)
        .output()
        .map_err(|error| format!("failed to fetch qemu logs over ssh: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "failed to fetch qemu log archive: status={} stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    let mut file = File::create(&archive_path)
        .map_err(|error| format!("failed to create {}: {error}", archive_path.display()))?;
    file.write_all(&output.stdout)
        .map_err(|error| format!("failed to write {}: {error}", archive_path.display()))?;
    file.sync_all()
        .map_err(|error| format!("failed to sync {}: {error}", archive_path.display()))?;
    Ok(archive_path)
}

fn send_remote_api_request(
    layout: &QemuTestbedLayout,
    host: &str,
    port: u16,
    request: &ApiRequest,
) -> Result<Vec<u8>, String> {
    let request_bytes = encode_request(request)
        .map_err(|error| format!("failed to encode api request: {error:?}"))?;
    run_remote_tcp_request(layout, host, port, &request_bytes)
}

fn run_remote_tcp_request(
    layout: &QemuTestbedLayout,
    host: &str,
    port: u16,
    request_bytes: &[u8],
) -> Result<Vec<u8>, String> {
    let request_hex = encode_hex(request_bytes);
    let mut args = ssh_args(layout);
    args.push(build_remote_tcp_probe_command(host, port, &request_hex));
    let output = Command::new("ssh")
        .args(args)
        .output()
        .map_err(|error| format!("failed to probe remote client surface: {error}"))?;
    if output.status.success() {
        Ok(output.stdout)
    } else {
        Err(format!(
            "remote probe failed: status={} stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}

fn build_remote_tcp_probe_command(host: &str, port: u16, request_hex: &str) -> String {
    format!(
        "python3 - {host} {port} {request_hex} <<'PY'\nimport socket, sys\nhost = sys.argv[1]\nport = int(sys.argv[2])\npayload = bytes.fromhex(sys.argv[3])\nwith socket.create_connection((host, port), timeout=2) as stream:\n    if payload:\n        stream.sendall(payload)\n    stream.shutdown(socket.SHUT_WR)\n    chunks = []\n    while True:\n        chunk = stream.recv(4096)\n        if not chunk:\n            break\n        chunks.append(chunk)\n    sys.stdout.buffer.write(b''.join(chunks))\nPY"
    )
}

fn validate_protocol_probe_response(
    replica_id: ReplicaId,
    response_bytes: &[u8],
) -> Result<(), String> {
    let protocol_text = String::from_utf8_lossy(response_bytes);
    if protocol_text.contains("protocol transport not implemented")
        || protocol_text.contains("network isolated by local harness")
    {
        return Err(format!(
            "QEMU protocol surface is not ready for Jepsen: replica {} returned `{protocol_text}`",
            replica_id.get()
        ));
    }
    Ok(())
}

fn extract_probe_commit_lsn(response: ApiResponse) -> Result<Lsn, String> {
    match response {
        ApiResponse::Submit(SubmitResponse::Committed(result)) => Ok(result.applied_lsn),
        other => Err(format!(
            "QEMU client surface is not ready for Jepsen: primary submit probe did not commit: {other:?}"
        )),
    }
}

fn validate_probe_read_response(resource_id: u128, response: &ApiResponse) -> Result<(), String> {
    if matches!(
        response,
        ApiResponse::GetResource(ResourceResponse::Found(resource))
            if resource.resource_id == ResourceId(resource_id)
    ) {
        Ok(())
    } else {
        Err(format!(
            "QEMU client surface is not ready for Jepsen: primary read probe returned unexpected response {response:?}"
        ))
    }
}

fn enforce_minimum_fault_window(run_spec: &JepsenRunSpec, elapsed: Duration) -> Result<(), String> {
    let Some(minimum_secs) = run_spec.minimum_fault_window_secs else {
        return Ok(());
    };
    let minimum = Duration::from_secs(minimum_secs);
    if elapsed < minimum {
        return Err(format!(
            "run `{}` finished before minimum fault window (required={}s observed={}s)",
            run_spec.run_id,
            minimum_secs,
            elapsed.as_secs()
        ));
    }
    Ok(())
}

fn decode_qemu_api_response(bytes: &[u8]) -> Result<ApiResponse, String> {
    decode_response(bytes).map_err(|error| {
        let text = String::from_utf8_lossy(bytes);
        format!("invalid qemu api response: {error:?}; raw_response={text}")
    })
}

fn encode_hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

fn unique_probe_resource_id() -> u128 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    9_000_000_000_000_000_000_u128.saturating_add(millis)
}

fn probe_create_request(resource_id: u128) -> ClientRequest {
    ClientRequest {
        operation_id: OperationId(resource_id),
        client_id: ClientId(7),
        command: AllocCommand::CreateResource {
            resource_id: ResourceId(resource_id),
        },
    }
}

fn sanitize_run_id(run_id: &str) -> String {
    let mut sanitized = String::new();
    for character in run_id.chars() {
        if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
            sanitized.push(character);
        } else {
            sanitized.push('_');
        }
    }
    if sanitized.is_empty() {
        String::from("run")
    } else {
        sanitized
    }
}

fn ssh_args(layout: &QemuTestbedLayout) -> Vec<String> {
    vec![
        String::from("-i"),
        layout.ssh_private_key_path().display().to_string(),
        String::from("-o"),
        String::from("StrictHostKeyChecking=no"),
        String::from("-o"),
        String::from("UserKnownHostsFile=/dev/null"),
        String::from("-p"),
        CONTROL_HOST_SSH_PORT.to_string(),
        String::from("allocdb@127.0.0.1"),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use allocdb_core::ResourceState;

    #[test]
    fn release_gate_plan_includes_faulted_qemu_runs() {
        let runs = release_gate_plan();
        assert!(runs.iter().any(|run| {
            run.workload == JepsenWorkloadFamily::FailoverReadFences
                && run.nemesis == JepsenNemesisFamily::CrashRestart
        }));
        assert!(runs.iter().any(|run| {
            run.workload == JepsenWorkloadFamily::ExpirationAndRecovery
                && run.nemesis == JepsenNemesisFamily::MixedFailover
        }));
    }

    #[test]
    fn indefinite_submission_failure_maps_to_ambiguous_outcome() {
        let outcome = outcome_from_submission_failure(SubmissionFailure {
            category: allocdb_node::SubmissionErrorCategory::Indefinite,
            code: SubmissionFailureCode::StorageFailure,
        });
        assert_eq!(
            outcome,
            JepsenEventOutcome::Ambiguous(JepsenAmbiguousOutcome::IndefiniteWrite)
        );
    }

    #[test]
    fn expired_reservation_maps_to_released_read_state() {
        let state = map_reservation_state(allocdb_node::ReservationView {
            reservation_id: ReservationId(11),
            resource_id: ResourceId(22),
            holder_id: HolderId(33),
            state: ReservationState::Expired,
            created_lsn: Lsn(1),
            deadline_slot: Slot(9),
            released_lsn: Some(Lsn(4)),
            retire_after_slot: Some(Slot(17)),
        });
        assert_eq!(
            state,
            JepsenReservationState::Released {
                resource_id: ResourceId(22),
                holder_id: 33,
                released_lsn: Some(Lsn(4)),
            }
        );
    }

    #[test]
    fn remote_tcp_probe_command_places_args_before_heredoc() {
        let command = build_remote_tcp_probe_command("127.0.0.1", 9000, "deadbeef");
        assert!(command.starts_with("python3 - 127.0.0.1 9000 deadbeef <<'PY'\n"));
        assert!(command.contains("payload = bytes.fromhex(sys.argv[3])"));
        assert!(command.ends_with("\nPY"));
    }

    #[test]
    fn protocol_probe_rejects_placeholder_responses() {
        let not_ready =
            validate_protocol_probe_response(ReplicaId(2), b"protocol transport not implemented")
                .unwrap_err();
        assert!(not_ready.contains("replica 2"));

        let isolated =
            validate_protocol_probe_response(ReplicaId(3), b"network isolated by local harness")
                .unwrap_err();
        assert!(isolated.contains("replica 3"));

        assert!(validate_protocol_probe_response(ReplicaId(4), b"").is_ok());
    }

    #[test]
    fn probe_submit_and_read_validation_cover_pass_and_fail_paths() {
        let applied_lsn = extract_probe_commit_lsn(ApiResponse::Submit(SubmitResponse::Committed(
            allocdb_node::SubmissionResult {
                applied_lsn: Lsn(9),
                outcome: allocdb_core::result::CommandOutcome::new(ResultCode::Ok),
                from_retry_cache: false,
            }
            .into(),
        )))
        .unwrap();
        assert_eq!(applied_lsn, Lsn(9));

        let submit_error =
            extract_probe_commit_lsn(ApiResponse::GetResource(ResourceResponse::NotFound))
                .unwrap_err();
        assert!(submit_error.contains("did not commit"));

        let ok_read =
            ApiResponse::GetResource(ResourceResponse::Found(allocdb_node::ResourceView {
                resource_id: ResourceId(41),
                state: ResourceState::Available,
                current_reservation_id: None,
                version: 1,
            }));
        assert!(validate_probe_read_response(41, &ok_read).is_ok());

        let read_error =
            validate_probe_read_response(41, &ApiResponse::GetResource(ResourceResponse::NotFound))
                .unwrap_err();
        assert!(read_error.contains("unexpected response"));
    }

    #[test]
    fn resolve_run_spec_and_minimum_fault_window_are_enforced() {
        let control = resolve_run_spec("reservation_contention-control").unwrap();
        assert!(enforce_minimum_fault_window(&control, Duration::from_secs(0)).is_ok());

        let faulted = resolve_run_spec("reservation_contention-crash-restart").unwrap();
        let error = enforce_minimum_fault_window(&faulted, Duration::from_secs(10)).unwrap_err();
        assert!(error.contains("minimum fault window"));

        let unknown = resolve_run_spec("missing-run").unwrap_err();
        assert!(unknown.contains("unknown Jepsen run id"));
    }
}

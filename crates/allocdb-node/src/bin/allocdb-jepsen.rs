use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::time::{SystemTime, UNIX_EPOCH};

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
use allocdb_node::local_cluster::LocalClusterReplicaConfig;
use allocdb_node::qemu_testbed::{QemuTestbedLayout, qemu_testbed_layout_path};
use allocdb_node::{
    ApiRequest, ApiResponse, MetricsRequest, ReplicaRole, ReservationRequest, ReservationResponse,
    ResourceRequest, ResourceResponse, SubmissionFailure, SubmissionFailureCode, SubmitRequest,
    SubmitResponse, TickExpirationsRequest, TickExpirationsResponse, decode_response,
    encode_request,
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
    }

    let primary = layout
        .replica_layout
        .replicas
        .iter()
        .find(|replica| replica.role == allocdb_node::ReplicaRole::Primary)
        .ok_or_else(|| String::from("qemu layout has no configured primary"))?;
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
    let applied_lsn = match submit_response {
        ApiResponse::Submit(SubmitResponse::Committed(response)) => response.applied_lsn,
        other => {
            return Err(format!(
                "QEMU client surface is not ready for Jepsen: primary submit probe did not commit: {other:?}"
            ));
        }
    };

    let read_response = decode_qemu_api_response(&send_remote_api_request(
        &layout,
        &primary_host,
        primary.client_addr.port(),
        &ApiRequest::GetResource(ResourceRequest {
            resource_id: ResourceId(resource_id),
            required_lsn: Some(applied_lsn),
        }),
    )?)?;
    if !matches!(
        read_response,
        ApiResponse::GetResource(ResourceResponse::Found(resource))
            if resource.resource_id == ResourceId(resource_id)
    ) {
        return Err(format!(
            "QEMU client surface is not ready for Jepsen: primary read probe returned unexpected response {read_response:?}"
        ));
    }

    println!("qemu_surface=ready");
    Ok(())
}

fn run_qemu(workspace_root: &Path, run_id: &str, output_root: &Path) -> Result<(), String> {
    let run_spec = resolve_run_spec(run_id)?;
    ensure_supported_qemu_run(&run_spec)?;
    verify_qemu_surface(workspace_root)?;

    let layout = load_qemu_layout(workspace_root)?;
    let history = execute_qemu_control_run(&layout, &run_spec)?;
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

fn ensure_supported_qemu_run(run_spec: &JepsenRunSpec) -> Result<(), String> {
    if run_spec.nemesis == JepsenNemesisFamily::None {
        return Ok(());
    }

    Err(format!(
        "run `{}` requires nemesis `{}` but `allocdb-jepsen run-qemu` currently supports only control runs while external failover orchestration is still missing",
        run_spec.run_id,
        run_spec.nemesis.as_str(),
    ))
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

fn execute_qemu_control_run(
    layout: &QemuTestbedLayout,
    run_spec: &JepsenRunSpec,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let primary = primary_replica(layout)?;
    let backup = first_backup_replica(layout)?;
    let base_id = unique_probe_resource_id();
    match run_spec.workload {
        JepsenWorkloadFamily::ReservationContention => {
            run_reservation_contention(layout, primary, base_id)
        }
        JepsenWorkloadFamily::AmbiguousWriteRetry => {
            run_ambiguous_write_retry(layout, primary, base_id)
        }
        JepsenWorkloadFamily::FailoverReadFences => {
            run_failover_read_fences(layout, primary, backup, base_id)
        }
        JepsenWorkloadFamily::ExpirationAndRecovery => {
            run_expiration_and_recovery(layout, primary, base_id)
        }
    }
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

fn primary_replica(layout: &QemuTestbedLayout) -> Result<&LocalClusterReplicaConfig, String> {
    layout
        .replica_layout
        .replicas
        .iter()
        .find(|replica| replica.role == ReplicaRole::Primary)
        .ok_or_else(|| String::from("qemu layout has no configured primary"))
}

fn first_backup_replica(layout: &QemuTestbedLayout) -> Result<&LocalClusterReplicaConfig, String> {
    layout
        .replica_layout
        .replicas
        .iter()
        .find(|replica| replica.role == ReplicaRole::Backup)
        .ok_or_else(|| String::from("qemu layout has no configured backup"))
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
    let script = "python3 - <<'PY'\nimport socket, sys\nhost = sys.argv[1]\nport = int(sys.argv[2])\npayload = bytes.fromhex(sys.argv[3])\nwith socket.create_connection((host, port), timeout=2) as stream:\n    if payload:\n        stream.sendall(payload)\n    stream.shutdown(socket.SHUT_WR)\n    chunks = []\n    while True:\n        chunk = stream.recv(4096)\n        if not chunk:\n            break\n        chunks.append(chunk)\n    sys.stdout.buffer.write(b''.join(chunks))\nPY";
    let mut args = ssh_args(layout);
    args.push(format!("{script} {host} {port} {request_hex}"));
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

    #[test]
    fn supported_qemu_run_rejects_non_control_nemesis() {
        let error = ensure_supported_qemu_run(&JepsenRunSpec {
            run_id: String::from("failover"),
            workload: JepsenWorkloadFamily::FailoverReadFences,
            nemesis: JepsenNemesisFamily::CrashRestart,
            minimum_fault_window_secs: Some(60),
            release_blocking: true,
        })
        .unwrap_err();
        assert!(error.contains("supports only control runs"));
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
}

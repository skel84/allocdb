use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use std::time::{SystemTime, UNIX_EPOCH};

use allocdb_core::command::{ClientRequest, Command as AllocCommand};
use allocdb_core::ids::{ClientId, OperationId, ResourceId, Slot};
use allocdb_node::jepsen::{
    analyze_history, create_artifact_bundle, load_history, release_gate_plan,
    render_analysis_report,
};
use allocdb_node::qemu_testbed::{QemuTestbedLayout, qemu_testbed_layout_path};
use allocdb_node::{
    ApiRequest, ApiResponse, MetricsRequest, ResourceRequest, ResourceResponse, SubmitRequest,
    SubmitResponse, decode_response, encode_request,
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

fn usage() -> String {
    String::from(
        "usage:\n  allocdb-jepsen plan\n  allocdb-jepsen analyze --history-file <path>\n  allocdb-jepsen verify-qemu-surface --workspace <path>\n  allocdb-jepsen archive-qemu --workspace <path> --run-id <run-id> --history-file <path> --output-root <path>\n",
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

use std::fs::{self, File};
use std::io::{IsTerminal, Read, Write};
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
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
use allocdb_node::kubevirt_testbed::{
    KubevirtGuestConfig, KubevirtTestbedConfig, KubevirtTestbedLayout, kubevirt_testbed_layout_path,
};
use allocdb_node::local_cluster::{
    LocalClusterLayout, LocalClusterReplicaConfig, ReplicaRuntimeState, ReplicaRuntimeStatus,
    decode_control_status_response,
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
const DEFAULT_KUBEVIRT_NAMESPACE: &str = "kubevirt";
const DEFAULT_KUBEVIRT_HELPER_POD_NAME: &str = "allocdb-bootstrap-helper";
const DEFAULT_KUBEVIRT_HELPER_IMAGE: &str = "nicolaka/netshoot:latest";
const DEFAULT_KUBEVIRT_HELPER_STAGE_DIR: &str = "/tmp/allocdb-stage";
const DEFAULT_KUBEVIRT_CONTROL_VM_NAME: &str = "allocdb-control";
const DEFAULT_KUBEVIRT_REPLICA1_VM_NAME: &str = "allocdb-replica-1";
const DEFAULT_KUBEVIRT_REPLICA2_VM_NAME: &str = "allocdb-replica-2";
const DEFAULT_KUBEVIRT_REPLICA3_VM_NAME: &str = "allocdb-replica-3";
const DEFAULT_GUEST_USER: &str = "allocdb";
const JEPSEN_RUN_STATUS_VERSION: u32 = 1;
const JEPSEN_LATEST_STATUS_FILE_NAME: &str = "allocdb-jepsen-latest-status.txt";
const DEFAULT_WATCH_REFRESH_MILLIS: u64 = 2_000;
const FAULT_WINDOW_OVERRIDE_ENV: &str = "ALLOCDB_JEPSEN_FAULT_WINDOW_SECS_OVERRIDE";
const EXTERNAL_RUNTIME_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(15);
const EXTERNAL_RUNTIME_DISCOVERY_RETRY_DELAY: Duration = Duration::from_millis(250);
const EXTERNAL_REMOTE_TCP_TIMEOUT_SECS: u64 = 10;
const WATCH_SPINNER_FRAMES: &[&str] = &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
const WATCH_PULSE_FRAMES: &[&str] = &[
    "▁", "▂", "▃", "▄", "▅", "▆", "▇", "█", "▇", "▆", "▅", "▄", "▃", "▂",
];
const WATCH_RULE_WIDTH: usize = 88;
const ANSI_RESET: &str = "\x1B[0m";
const ANSI_BOLD: &str = "\x1B[1m";
const ANSI_DIM: &str = "\x1B[2m";
const ANSI_CYAN: &str = "\x1B[38;5;51m";
const ANSI_BLUE: &str = "\x1B[38;5;39m";
const ANSI_GREEN: &str = "\x1B[38;5;48m";
const ANSI_YELLOW: &str = "\x1B[38;5;220m";
const ANSI_ORANGE: &str = "\x1B[38;5;208m";
const ANSI_RED: &str = "\x1B[38;5;196m";
const ANSI_MAGENTA: &str = "\x1B[38;5;207m";
const ANSI_WHITE: &str = "\x1B[38;5;15m";
static NEXT_PROBE_RESOURCE_ID: AtomicU64 = AtomicU64::new(0);
static NEXT_STAGING_WORKSPACE_ID: AtomicU64 = AtomicU64::new(0);

enum ParsedCommand {
    Help,
    Plan,
    Analyze {
        history_file: PathBuf,
    },
    CaptureKubevirtLayout {
        workspace_root: PathBuf,
        kubeconfig_path: Option<PathBuf>,
        namespace: String,
        helper_pod_name: String,
        helper_image: String,
        helper_stage_dir: PathBuf,
        ssh_private_key_path: PathBuf,
        control_vm_name: String,
        replica_vm_names: [String; 3],
    },
    VerifyQemuSurface {
        workspace_root: PathBuf,
    },
    VerifyKubevirtSurface {
        workspace_root: PathBuf,
    },
    RunQemu {
        workspace_root: PathBuf,
        run_id: String,
        output_root: PathBuf,
    },
    RunKubevirt {
        workspace_root: PathBuf,
        run_id: String,
        output_root: PathBuf,
    },
    WatchKubevirt {
        workspace_root: PathBuf,
        output_root: PathBuf,
        run_id: Option<String>,
        refresh_millis: u64,
        follow: bool,
    },
    WatchKubevirtFleet {
        lanes: Vec<KubevirtWatchLaneSpec>,
        refresh_millis: u64,
        follow: bool,
    },
    ArchiveQemu {
        workspace_root: PathBuf,
        run_id: String,
        history_file: PathBuf,
        output_root: PathBuf,
    },
    ArchiveKubevirt {
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
        ParsedCommand::CaptureKubevirtLayout {
            workspace_root,
            kubeconfig_path,
            namespace,
            helper_pod_name,
            helper_image,
            helper_stage_dir,
            ssh_private_key_path,
            control_vm_name,
            replica_vm_names,
        } => capture_kubevirt_layout(&CaptureKubevirtLayoutArgs {
            workspace_root: &workspace_root,
            kubeconfig_path: kubeconfig_path.as_deref(),
            namespace: &namespace,
            helper_pod_name: &helper_pod_name,
            helper_image: &helper_image,
            helper_stage_dir: &helper_stage_dir,
            ssh_private_key_path: &ssh_private_key_path,
            control_vm_name: &control_vm_name,
            replica_vm_names: &replica_vm_names,
        }),
        ParsedCommand::VerifyQemuSurface { workspace_root } => verify_qemu_surface(&workspace_root),
        ParsedCommand::VerifyKubevirtSurface { workspace_root } => {
            verify_kubevirt_surface(&workspace_root)
        }
        ParsedCommand::RunQemu {
            workspace_root,
            run_id,
            output_root,
        } => run_qemu(&workspace_root, &run_id, &output_root),
        ParsedCommand::RunKubevirt {
            workspace_root,
            run_id,
            output_root,
        } => run_kubevirt(&workspace_root, &run_id, &output_root),
        ParsedCommand::WatchKubevirt {
            workspace_root,
            output_root,
            run_id,
            refresh_millis,
            follow,
        } => watch_kubevirt(
            &workspace_root,
            &output_root,
            run_id.as_deref(),
            refresh_millis,
            follow,
        ),
        ParsedCommand::WatchKubevirtFleet {
            lanes,
            refresh_millis,
            follow,
        } => watch_kubevirt_fleet(&lanes, refresh_millis, follow),
        ParsedCommand::ArchiveQemu {
            workspace_root,
            run_id,
            history_file,
            output_root,
        } => archive_qemu_run(&workspace_root, &run_id, &history_file, &output_root),
        ParsedCommand::ArchiveKubevirt {
            workspace_root,
            run_id,
            history_file,
            output_root,
        } => archive_kubevirt_run(&workspace_root, &run_id, &history_file, &output_root),
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
        "capture-kubevirt-layout" => parse_capture_kubevirt_args(args),
        "verify-qemu-surface" => Ok(ParsedCommand::VerifyQemuSurface {
            workspace_root: parse_workspace_flag(args)?,
        }),
        "verify-kubevirt-surface" => Ok(ParsedCommand::VerifyKubevirtSurface {
            workspace_root: parse_workspace_flag(args)?,
        }),
        "run-qemu" => parse_run_qemu_args(args),
        "run-kubevirt" => parse_run_kubevirt_args(args),
        "watch-kubevirt" => parse_watch_kubevirt_args(args),
        "watch-kubevirt-fleet" => parse_watch_kubevirt_fleet_args(args),
        "archive-qemu" => parse_archive_qemu_args(args),
        "archive-kubevirt" => parse_archive_kubevirt_args(args),
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

fn parse_archive_kubevirt_args(
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

    Ok(ParsedCommand::ArchiveKubevirt {
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

fn parse_run_kubevirt_args(
    args: impl IntoIterator<Item = String>,
) -> Result<ParsedCommand, String> {
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

    Ok(ParsedCommand::RunKubevirt {
        workspace_root: workspace_root.ok_or_else(usage)?,
        run_id: run_id.ok_or_else(usage)?,
        output_root: output_root.ok_or_else(usage)?,
    })
}

fn parse_watch_kubevirt_args(
    args: impl IntoIterator<Item = String>,
) -> Result<ParsedCommand, String> {
    let mut args = args.into_iter();
    let mut workspace_root = None;
    let mut output_root = None;
    let mut run_id = None;
    let mut refresh_millis = Some(DEFAULT_WATCH_REFRESH_MILLIS);
    let mut follow = false;
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--workspace" => {
                workspace_root = Some(PathBuf::from(args.next().ok_or_else(usage)?));
            }
            "--output-root" => {
                output_root = Some(PathBuf::from(args.next().ok_or_else(usage)?));
            }
            "--run-id" => {
                run_id = Some(args.next().ok_or_else(usage)?);
            }
            "--refresh-millis" => {
                let value = args.next().ok_or_else(usage)?;
                let parsed = value.parse::<u64>().map_err(|error| {
                    format!(
                        "invalid --refresh-millis value `{value}`: {error}\n\n{}",
                        usage()
                    )
                })?;
                refresh_millis = Some(parsed);
            }
            "--follow" => {
                follow = true;
            }
            "--help" | "-h" => return Err(usage()),
            other => return Err(format!("unknown argument `{other}`\n\n{}", usage())),
        }
    }

    Ok(ParsedCommand::WatchKubevirt {
        workspace_root: workspace_root.ok_or_else(usage)?,
        output_root: output_root.ok_or_else(usage)?,
        run_id,
        refresh_millis: refresh_millis.ok_or_else(usage)?,
        follow,
    })
}

fn parse_watch_kubevirt_fleet_args(
    args: impl IntoIterator<Item = String>,
) -> Result<ParsedCommand, String> {
    let mut args = args.into_iter();
    let mut lanes = Vec::new();
    let mut refresh_millis = Some(DEFAULT_WATCH_REFRESH_MILLIS);
    let mut follow = false;
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--lane" => {
                let spec = args.next().ok_or_else(usage)?;
                lanes.push(parse_watch_kubevirt_lane_spec(&spec)?);
            }
            "--refresh-millis" => {
                let value = args.next().ok_or_else(usage)?;
                let parsed = value.parse::<u64>().map_err(|error| {
                    format!(
                        "invalid --refresh-millis value `{value}`: {error}\n\n{}",
                        usage()
                    )
                })?;
                refresh_millis = Some(parsed);
            }
            "--follow" => {
                follow = true;
            }
            "--help" | "-h" => return Err(usage()),
            other => return Err(format!("unknown argument `{other}`\n\n{}", usage())),
        }
    }

    if lanes.is_empty() {
        return Err(format!(
            "watch-kubevirt-fleet requires at least one --lane <name,workspace,output-root>\n\n{}",
            usage()
        ));
    }

    Ok(ParsedCommand::WatchKubevirtFleet {
        lanes,
        refresh_millis: refresh_millis.ok_or_else(usage)?,
        follow,
    })
}

fn parse_watch_kubevirt_lane_spec(spec: &str) -> Result<KubevirtWatchLaneSpec, String> {
    let mut parts = spec.splitn(3, ',');
    let name = parts.next().unwrap_or_default().trim();
    let workspace_root = parts.next().unwrap_or_default().trim();
    let output_root = parts.next().unwrap_or_default().trim();
    if name.is_empty() || workspace_root.is_empty() || output_root.is_empty() {
        return Err(format!(
            "invalid --lane value `{spec}`; expected <name,workspace,output-root>\n\n{}",
            usage()
        ));
    }
    Ok(KubevirtWatchLaneSpec {
        name: String::from(name),
        workspace_root: PathBuf::from(workspace_root),
        output_root: PathBuf::from(output_root),
    })
}

fn parse_capture_kubevirt_args(
    args: impl IntoIterator<Item = String>,
) -> Result<ParsedCommand, String> {
    let mut args = args.into_iter();
    let mut workspace_root = None;
    let mut kubeconfig_path = None;
    let mut namespace = Some(String::from(DEFAULT_KUBEVIRT_NAMESPACE));
    let mut helper_pod_name = Some(String::from(DEFAULT_KUBEVIRT_HELPER_POD_NAME));
    let mut helper_image = Some(String::from(DEFAULT_KUBEVIRT_HELPER_IMAGE));
    let mut helper_stage_dir = Some(PathBuf::from(DEFAULT_KUBEVIRT_HELPER_STAGE_DIR));
    let mut ssh_private_key_path = None;
    let mut control_vm_name = Some(String::from(DEFAULT_KUBEVIRT_CONTROL_VM_NAME));
    let mut replica1_vm_name = Some(String::from(DEFAULT_KUBEVIRT_REPLICA1_VM_NAME));
    let mut replica2_vm_name = Some(String::from(DEFAULT_KUBEVIRT_REPLICA2_VM_NAME));
    let mut replica3_vm_name = Some(String::from(DEFAULT_KUBEVIRT_REPLICA3_VM_NAME));

    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--workspace" => {
                workspace_root = Some(PathBuf::from(args.next().ok_or_else(usage)?));
            }
            "--namespace" => {
                namespace = Some(args.next().ok_or_else(usage)?);
            }
            "--kubeconfig" => {
                kubeconfig_path = Some(PathBuf::from(args.next().ok_or_else(usage)?));
            }
            "--helper-pod" => {
                helper_pod_name = Some(args.next().ok_or_else(usage)?);
            }
            "--helper-image" => {
                helper_image = Some(args.next().ok_or_else(usage)?);
            }
            "--helper-stage-dir" => {
                helper_stage_dir = Some(PathBuf::from(args.next().ok_or_else(usage)?));
            }
            "--ssh-private-key" => {
                ssh_private_key_path = Some(PathBuf::from(args.next().ok_or_else(usage)?));
            }
            "--control-vm" => {
                control_vm_name = Some(args.next().ok_or_else(usage)?);
            }
            "--replica-1-vm" => {
                replica1_vm_name = Some(args.next().ok_or_else(usage)?);
            }
            "--replica-2-vm" => {
                replica2_vm_name = Some(args.next().ok_or_else(usage)?);
            }
            "--replica-3-vm" => {
                replica3_vm_name = Some(args.next().ok_or_else(usage)?);
            }
            "--help" | "-h" => return Err(usage()),
            other => return Err(format!("unknown argument `{other}`\n\n{}", usage())),
        }
    }

    Ok(ParsedCommand::CaptureKubevirtLayout {
        workspace_root: workspace_root.ok_or_else(usage)?,
        kubeconfig_path,
        namespace: namespace.ok_or_else(usage)?,
        helper_pod_name: helper_pod_name.ok_or_else(usage)?,
        helper_image: helper_image.ok_or_else(usage)?,
        helper_stage_dir: helper_stage_dir.ok_or_else(usage)?,
        ssh_private_key_path: ssh_private_key_path.ok_or_else(usage)?,
        control_vm_name: control_vm_name.ok_or_else(usage)?,
        replica_vm_names: [
            replica1_vm_name.ok_or_else(usage)?,
            replica2_vm_name.ok_or_else(usage)?,
            replica3_vm_name.ok_or_else(usage)?,
        ],
    })
}

fn usage() -> String {
    String::from(
        "usage:\n  allocdb-jepsen plan\n  allocdb-jepsen analyze --history-file <path>\n  allocdb-jepsen capture-kubevirt-layout --workspace <path> --namespace <name> --ssh-private-key <path> [--kubeconfig <path>] [--helper-pod <name>] [--helper-image <image>] [--helper-stage-dir <path>] [--control-vm <name>] [--replica-1-vm <name>] [--replica-2-vm <name>] [--replica-3-vm <name>]\n  allocdb-jepsen verify-qemu-surface --workspace <path>\n  allocdb-jepsen verify-kubevirt-surface --workspace <path>\n  allocdb-jepsen run-qemu --workspace <path> --run-id <run-id> --output-root <path>\n  allocdb-jepsen run-kubevirt --workspace <path> --run-id <run-id> --output-root <path>\n  allocdb-jepsen watch-kubevirt --workspace <path> --output-root <path> [--run-id <run-id>] [--refresh-millis <ms>] [--follow]\n  allocdb-jepsen watch-kubevirt-fleet --lane <name,workspace,output-root> [--lane <name,workspace,output-root> ...] [--refresh-millis <ms>] [--follow]\n  allocdb-jepsen archive-qemu --workspace <path> --run-id <run-id> --history-file <path> --output-root <path>\n  allocdb-jepsen archive-kubevirt --workspace <path> --run-id <run-id> --history-file <path> --output-root <path>\n",
    )
}

trait ExternalTestbed {
    fn backend_name(&self) -> &'static str;
    fn workspace_root(&self) -> &Path;
    fn replica_layout(&self) -> &LocalClusterLayout;
    fn run_remote_tcp_request(
        &self,
        host: &str,
        port: u16,
        request_bytes: &[u8],
    ) -> Result<Vec<u8>, String>;
    fn run_remote_host_command(
        &self,
        remote_command: &str,
        stdin_bytes: Option<&[u8]>,
    ) -> Result<Vec<u8>, String>;
}

impl ExternalTestbed for QemuTestbedLayout {
    fn backend_name(&self) -> &'static str {
        "qemu"
    }

    fn workspace_root(&self) -> &Path {
        &self.config.workspace_root
    }

    fn replica_layout(&self) -> &LocalClusterLayout {
        &self.replica_layout
    }

    fn run_remote_tcp_request(
        &self,
        host: &str,
        port: u16,
        request_bytes: &[u8],
    ) -> Result<Vec<u8>, String> {
        run_qemu_remote_tcp_request(self, host, port, request_bytes)
    }

    fn run_remote_host_command(
        &self,
        remote_command: &str,
        stdin_bytes: Option<&[u8]>,
    ) -> Result<Vec<u8>, String> {
        run_qemu_remote_host_command(self, remote_command, stdin_bytes)
    }
}

impl ExternalTestbed for KubevirtTestbedLayout {
    fn backend_name(&self) -> &'static str {
        "kubevirt"
    }

    fn workspace_root(&self) -> &Path {
        &self.config.workspace_root
    }

    fn replica_layout(&self) -> &LocalClusterLayout {
        &self.replica_layout
    }

    fn run_remote_tcp_request(
        &self,
        host: &str,
        port: u16,
        request_bytes: &[u8],
    ) -> Result<Vec<u8>, String> {
        run_kubevirt_remote_tcp_request(self, host, port, request_bytes)
    }

    fn run_remote_host_command(
        &self,
        remote_command: &str,
        stdin_bytes: Option<&[u8]>,
    ) -> Result<Vec<u8>, String> {
        run_kubevirt_remote_host_command(self, remote_command, stdin_bytes)
    }
}

struct KubevirtHelperGuard {
    kubeconfig_path: Option<PathBuf>,
    namespace: String,
    pod_name: String,
    delete_on_drop: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct KubevirtWatchLaneSpec {
    name: String,
    workspace_root: PathBuf,
    output_root: PathBuf,
}

struct KubevirtWatchLaneContext {
    spec: KubevirtWatchLaneSpec,
    layout: KubevirtTestbedLayout,
    _helper: KubevirtHelperGuard,
}

struct CaptureKubevirtLayoutArgs<'a> {
    workspace_root: &'a Path,
    kubeconfig_path: Option<&'a Path>,
    namespace: &'a str,
    helper_pod_name: &'a str,
    helper_image: &'a str,
    helper_stage_dir: &'a Path,
    ssh_private_key_path: &'a Path,
    control_vm_name: &'a str,
    replica_vm_names: &'a [String; 3],
}

impl Drop for KubevirtHelperGuard {
    fn drop(&mut self) {
        if !self.delete_on_drop {
            return;
        }
        let _ = Command::new("kubectl")
            .args(kubectl_base_args(
                self.kubeconfig_path.as_deref(),
                &self.namespace,
            ))
            .args([
                "delete",
                &format!("pod/{}", self.pod_name),
                "--ignore-not-found=true",
                "--force",
                "--grace-period=0",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

fn kubectl_base_args(kubeconfig_path: Option<&Path>, namespace: &str) -> Vec<String> {
    let mut args = Vec::new();
    if let Some(kubeconfig_path) = kubeconfig_path {
        args.push(String::from("--kubeconfig"));
        args.push(kubeconfig_path.display().to_string());
    }
    args.push(String::from("-n"));
    args.push(String::from(namespace));
    args
}

fn capture_kubevirt_layout(args: &CaptureKubevirtLayoutArgs<'_>) -> Result<(), String> {
    if !args.ssh_private_key_path.exists() {
        return Err(format!(
            "kubevirt ssh private key {} does not exist",
            args.ssh_private_key_path.display()
        ));
    }

    let control_guest = KubevirtGuestConfig {
        name: String::from(args.control_vm_name),
        replica_id: None,
        addr: discover_kubevirt_vmi_ip(args.kubeconfig_path, args.namespace, args.control_vm_name)?,
    };
    let replica_guests = args
        .replica_vm_names
        .iter()
        .enumerate()
        .map(|(index, name)| {
            let replica_id = u64::try_from(index).unwrap_or(0).saturating_add(1);
            Ok(KubevirtGuestConfig {
                name: name.clone(),
                replica_id: Some(ReplicaId(replica_id)),
                addr: discover_kubevirt_vmi_ip(args.kubeconfig_path, args.namespace, name)?,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;

    let layout = KubevirtTestbedLayout::new(
        KubevirtTestbedConfig {
            workspace_root: args.workspace_root.to_path_buf(),
            kubeconfig_path: args.kubeconfig_path.map(Path::to_path_buf),
            namespace: String::from(args.namespace),
            helper_pod_name: String::from(args.helper_pod_name),
            helper_image: String::from(args.helper_image),
            helper_stage_dir: args.helper_stage_dir.to_path_buf(),
            ssh_private_key_path: args.ssh_private_key_path.to_path_buf(),
        },
        control_guest,
        replica_guests,
    )
    .map_err(|error| format!("failed to build kubevirt testbed layout: {error}"))?;
    layout
        .persist()
        .map_err(|error| format!("failed to persist kubevirt testbed layout: {error}"))?;

    println!("kubevirt_layout={}", layout.layout_path().display());
    println!(
        "control_vm={} control_ip={}",
        layout.control_guest.name, layout.control_guest.addr
    );
    for guest in &layout.replica_guests {
        println!(
            "replica_vm={} replica_id={} replica_ip={}",
            guest.name,
            guest.replica_id.map_or(0, ReplicaId::get),
            guest.addr
        );
    }
    Ok(())
}

fn discover_kubevirt_vmi_ip(
    kubeconfig_path: Option<&Path>,
    namespace: &str,
    vm_name: &str,
) -> Result<Ipv4Addr, String> {
    let mut command = Command::new("kubectl");
    command.args(kubectl_base_args(kubeconfig_path, namespace));
    command.args([
        "get",
        "virtualmachineinstances",
        vm_name,
        "-o",
        "jsonpath={.status.interfaces[0].ipAddress}",
    ]);
    let output = command
        .output()
        .map_err(|error| format!("failed to query kubevirt vmi {vm_name}: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "failed to query kubevirt vmi {vm_name}: status={} stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let value = String::from_utf8(output.stdout)
        .map_err(|error| format!("invalid kubevirt vmi ip output for {vm_name}: {error}"))?;
    value
        .trim()
        .parse::<Ipv4Addr>()
        .map_err(|error| format!("invalid kubevirt vmi ip for {vm_name}: {error}"))
}

fn load_kubevirt_layout(workspace_root: &Path) -> Result<KubevirtTestbedLayout, String> {
    let path = kubevirt_testbed_layout_path(workspace_root);
    KubevirtTestbedLayout::load(path)
        .map_err(|error| format!("failed to load kubevirt testbed layout: {error}"))
}

fn prepare_kubevirt_helper(layout: &KubevirtTestbedLayout) -> Result<KubevirtHelperGuard, String> {
    let phase = kubevirt_helper_phase(layout)?;
    let mut delete_on_drop = false;
    if !matches!(phase.as_deref().map(str::trim), Some("Running")) {
        if phase.is_some() {
            delete_kubevirt_helper_pod(layout)?;
        }
        apply_kubevirt_helper_pod(layout)?;
        delete_on_drop = kubevirt_helper_should_delete_on_drop();
    }
    wait_for_kubevirt_helper_ready(layout)?;
    prepare_kubevirt_helper_stage_dir(layout)?;
    copy_kubevirt_helper_ssh_key(layout)?;
    chmod_kubevirt_helper_ssh_key(layout)?;

    Ok(KubevirtHelperGuard {
        kubeconfig_path: layout.config.kubeconfig_path.clone(),
        namespace: layout.config.namespace.clone(),
        pod_name: layout.config.helper_pod_name.clone(),
        delete_on_drop,
    })
}

fn kubevirt_helper_phase(layout: &KubevirtTestbedLayout) -> Result<Option<String>, String> {
    let output = Command::new("kubectl")
        .args(kubectl_base_args(
            layout.config.kubeconfig_path.as_deref(),
            &layout.config.namespace,
        ))
        .args([
            "get",
            &format!("pod/{}", layout.config.helper_pod_name),
            "-o",
            "jsonpath={.status.phase}",
        ])
        .output()
        .map_err(|error| format!("failed to query kubevirt helper pod: {error}"))?;
    if output.status.success() {
        String::from_utf8(output.stdout)
            .map(Some)
            .map_err(|error| format!("invalid helper pod phase output: {error}"))
    } else {
        Ok(None)
    }
}

fn delete_kubevirt_helper_pod(layout: &KubevirtTestbedLayout) -> Result<(), String> {
    let status = Command::new("kubectl")
        .args(kubectl_base_args(
            layout.config.kubeconfig_path.as_deref(),
            &layout.config.namespace,
        ))
        .args([
            "delete",
            &format!("pod/{}", layout.config.helper_pod_name),
            "--ignore-not-found=true",
            "--wait=true",
        ])
        .status()
        .map_err(|error| format!("failed to delete kubevirt helper pod: {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "failed to delete kubevirt helper pod {}",
            layout.config.helper_pod_name
        ))
    }
}

fn apply_kubevirt_helper_pod(layout: &KubevirtTestbedLayout) -> Result<(), String> {
    let manifest = kubevirt_helper_manifest(layout);
    let mut child = Command::new("kubectl")
        .args(kubectl_base_args(
            layout.config.kubeconfig_path.as_deref(),
            &layout.config.namespace,
        ))
        .arg("apply")
        .arg("-f")
        .arg("-")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| format!("failed to create kubevirt helper pod: {error}"))?;
    child
        .stdin
        .as_mut()
        .ok_or_else(|| String::from("kubevirt helper manifest stdin was unavailable"))?
        .write_all(manifest.as_bytes())
        .map_err(|error| format!("failed to write kubevirt helper manifest: {error}"))?;
    let output = child
        .wait_with_output()
        .map_err(|error| format!("failed to wait for kubevirt helper apply: {error}"))?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "failed to apply kubevirt helper pod: status={} stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}

fn kubevirt_helper_manifest(layout: &KubevirtTestbedLayout) -> String {
    format!(
        "apiVersion: v1\nkind: Pod\nmetadata:\n  name: {name}\n  namespace: {namespace}\n  labels:\n    app.kubernetes.io/name: allocdb-jepsen-helper\nspec:\n  restartPolicy: Never\n  containers:\n    - name: helper\n      image: {image}\n      command: [\"/bin/sh\", \"-lc\", \"sleep infinity\"]\n",
        name = layout.config.helper_pod_name,
        namespace = layout.config.namespace,
        image = layout.config.helper_image,
    )
}

fn kubevirt_helper_should_delete_on_drop() -> bool {
    std::env::var("ALLOCDB_KEEP_KUBEVIRT_HELPER")
        .ok()
        .as_deref()
        != Some("1")
}

fn wait_for_kubevirt_helper_ready(layout: &KubevirtTestbedLayout) -> Result<(), String> {
    let status = Command::new("kubectl")
        .args(kubectl_base_args(
            layout.config.kubeconfig_path.as_deref(),
            &layout.config.namespace,
        ))
        .args([
            "wait",
            &format!("pod/{}", layout.config.helper_pod_name),
            "--for=condition=Ready",
            "--timeout=180s",
        ])
        .status()
        .map_err(|error| format!("failed to wait for kubevirt helper pod: {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "kubevirt helper pod {} did not become ready",
            layout.config.helper_pod_name
        ))
    }
}

fn prepare_kubevirt_helper_stage_dir(layout: &KubevirtTestbedLayout) -> Result<(), String> {
    let stage_command = format!(
        "mkdir -p {stage_dir} && chmod 700 {stage_dir}",
        stage_dir = layout.config.helper_stage_dir.display()
    );
    run_kubevirt_helper_exec(layout, &stage_command, "prepare kubevirt helper stage dir")
}

fn copy_kubevirt_helper_ssh_key(layout: &KubevirtTestbedLayout) -> Result<(), String> {
    let cp_target = format!(
        "{namespace}/{pod}:{stage_dir}/id_ed25519",
        namespace = layout.config.namespace,
        pod = layout.config.helper_pod_name,
        stage_dir = layout.config.helper_stage_dir.display()
    );
    let status = Command::new("kubectl")
        .args(
            layout
                .config
                .kubeconfig_path
                .as_deref()
                .map_or_else(Vec::new, |path| {
                    vec![String::from("--kubeconfig"), path.display().to_string()]
                }),
        )
        .arg("cp")
        .arg(&layout.config.ssh_private_key_path)
        .arg(&cp_target)
        .status()
        .map_err(|error| format!("failed to copy kubevirt helper ssh key: {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(String::from("failed to copy kubevirt helper ssh key"))
    }
}

fn chmod_kubevirt_helper_ssh_key(layout: &KubevirtTestbedLayout) -> Result<(), String> {
    let chmod_command = format!(
        "chmod 600 {stage_dir}/id_ed25519",
        stage_dir = layout.config.helper_stage_dir.display()
    );
    run_kubevirt_helper_exec(layout, &chmod_command, "chmod kubevirt helper ssh key")
}

fn run_kubevirt_helper_exec(
    layout: &KubevirtTestbedLayout,
    script: &str,
    description: &str,
) -> Result<(), String> {
    let status = Command::new("kubectl")
        .args(kubectl_base_args(
            layout.config.kubeconfig_path.as_deref(),
            &layout.config.namespace,
        ))
        .args([
            "exec",
            &layout.config.helper_pod_name,
            "--",
            "sh",
            "-lc",
            script,
        ])
        .status()
        .map_err(|error| format!("failed to {description}: {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("failed to {description}"))
    }
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
    verify_external_surface(&layout)?;
    println!("qemu_surface=ready");
    Ok(())
}

fn verify_kubevirt_surface(workspace_root: &Path) -> Result<(), String> {
    let layout = load_kubevirt_layout(workspace_root)?;
    let _helper = prepare_kubevirt_helper(&layout)?;
    verify_external_surface(&layout)?;
    println!("kubevirt_surface=ready");
    Ok(())
}

fn verify_external_surface<T: ExternalTestbed>(layout: &T) -> Result<(), String> {
    ensure_runtime_cluster_ready(layout)?;
    let probe_namespace = RequestNamespace::new();
    for replica in &layout.replica_layout().replicas {
        let response_bytes = send_remote_api_request(
            layout,
            &replica.client_addr.ip().to_string(),
            replica.client_addr.port(),
            &ApiRequest::GetMetrics(MetricsRequest {
                current_wall_clock_slot: probe_namespace.slot(0),
            }),
        )?;
        let response = decode_external_api_response(layout, &response_bytes)?;
        if !matches!(response, ApiResponse::GetMetrics(_)) {
            return Err(format!(
                "{} client surface is not ready for Jepsen: replica {} returned unexpected metrics probe response {response:?}",
                layout.backend_name(),
                replica.replica_id.get()
            ));
        }

        let protocol_response = run_remote_tcp_request(
            layout,
            &replica.protocol_addr.ip().to_string(),
            replica.protocol_addr.port(),
            &[],
        )?;
        validate_protocol_probe_response(
            layout.backend_name(),
            replica.replica_id,
            &protocol_response,
        )?;
    }

    let primary = primary_replica(layout)?;
    let primary_host = primary.client_addr.ip().to_string();
    let resource_id = unique_probe_resource_id();
    let submit_response = decode_external_api_response(
        layout,
        &send_remote_api_request(
            layout,
            &primary_host,
            primary.client_addr.port(),
            &ApiRequest::Submit(SubmitRequest::from_client_request(
                probe_namespace.slot(1),
                probe_create_request(resource_id, probe_namespace),
            )),
        )?,
    )?;
    let applied_lsn = extract_probe_commit_lsn(layout.backend_name(), submit_response)?;

    let read_response = decode_external_api_response(
        layout,
        &send_remote_api_request(
            layout,
            &primary_host,
            primary.client_addr.port(),
            &ApiRequest::GetResource(ResourceRequest {
                resource_id: ResourceId(resource_id),
                required_lsn: Some(applied_lsn),
            }),
        )?,
    )?;
    validate_probe_read_response(layout.backend_name(), resource_id, &read_response)?;
    Ok(())
}

fn run_qemu(workspace_root: &Path, run_id: &str, output_root: &Path) -> Result<(), String> {
    let layout = load_qemu_layout(workspace_root)?;
    run_external(&layout, run_id, output_root)
}

fn run_kubevirt(workspace_root: &Path, run_id: &str, output_root: &Path) -> Result<(), String> {
    let layout = load_kubevirt_layout(workspace_root)?;
    let _helper = prepare_kubevirt_helper(&layout)?;
    run_external(&layout, run_id, output_root)
}

fn run_external<T: ExternalTestbed>(
    layout: &T,
    run_id: &str,
    output_root: &Path,
) -> Result<(), String> {
    let run_spec = resolve_run_spec(run_id)?;
    let tracker = RunTracker::new(layout.backend_name(), &run_spec, output_root)?;
    let started_at = Instant::now();
    tracker.set_phase(
        RunTrackerPhase::VerifyingSurface,
        "verifying external surface",
    )?;
    if let Err(error) = verify_external_surface(layout) {
        let _ = tracker.fail(RunTrackerPhase::VerifyingSurface, &error);
        return Err(error);
    }
    tracker.set_phase(RunTrackerPhase::Executing, "executing Jepsen scenario")?;
    let history = match execute_external_run(layout, &run_spec, &tracker) {
        Ok(history) => history,
        Err(error) => {
            let _ = tracker.fail(RunTrackerPhase::Executing, &error);
            return Err(error);
        }
    };
    if let Err(error) = fs::create_dir_all(output_root).map_err(|error| {
        format!(
            "failed to create output root {}: {error}",
            output_root.display()
        )
    }) {
        let _ = tracker.fail(RunTrackerPhase::Executing, &error);
        return Err(error);
    }
    let history_path = output_root.join(format!("{}-history.txt", sanitize_run_id(run_id)));
    if let Err(error) = persist_history(&history_path, &history)
        .map_err(|error| format!("failed to persist Jepsen history: {error}"))
    {
        let _ = tracker.fail(RunTrackerPhase::Executing, &error);
        return Err(error);
    }

    tracker.set_phase(RunTrackerPhase::Analyzing, "analyzing Jepsen history")?;
    let report = analyze_history(&history);
    tracker.set_phase(RunTrackerPhase::Archiving, "collecting logs and artifacts")?;
    let logs_archive = match fetch_external_logs_archive(layout, run_id, output_root) {
        Ok(path) => path,
        Err(error) => {
            let _ = tracker.fail(RunTrackerPhase::Archiving, &error);
            return Err(error);
        }
    };
    let bundle_dir = create_artifact_bundle(
        output_root,
        &run_spec,
        &history,
        &report,
        Some(&logs_archive),
    )
    .map_err(|error| format!("failed to create Jepsen artifact bundle: {error}"));
    let bundle_dir = match bundle_dir {
        Ok(path) => path,
        Err(error) => {
            let _ = tracker.fail(RunTrackerPhase::Archiving, &error);
            return Err(error);
        }
    };

    println!("history_file={}", history_path.display());
    println!("artifact_bundle={}", bundle_dir.display());
    println!(
        "{}_logs_archive={}",
        layout.backend_name(),
        logs_archive.display()
    );
    print!("{}", render_analysis_report(&report));
    if let Err(error) = enforce_minimum_fault_window(&run_spec, started_at.elapsed()) {
        let _ = tracker.fail(RunTrackerPhase::Completed, &error);
        return Err(error);
    }
    tracker.complete(&history_path, &bundle_dir, &logs_archive, &report)?;

    if report.release_gate_passed() {
        Ok(())
    } else {
        let _ = tracker.fail(RunTrackerPhase::Completed, "Jepsen release gate is blocked");
        Err(String::from("Jepsen release gate is blocked"))
    }
}

fn resolve_run_spec(run_id: &str) -> Result<JepsenRunSpec, String> {
    release_gate_plan()
        .into_iter()
        .find(|candidate| candidate.run_id == run_id)
        .ok_or_else(|| format!("unknown Jepsen run id `{run_id}`"))
}

fn execute_external_run<T: ExternalTestbed>(
    layout: &T,
    run_spec: &JepsenRunSpec,
    tracker: &RunTracker,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let started_at = Instant::now();
    let minimum_fault_window = minimum_fault_window_duration(run_spec);
    let mut all_events = Vec::new();
    let mut next_sequence = 0_u64;
    let mut iteration = 0_u64;

    loop {
        iteration = iteration.saturating_add(1);
        if !matches!(run_spec.nemesis, JepsenNemesisFamily::None) {
            ensure_runtime_cluster_ready(layout)?;
        }
        // Surface verification already performed one committed write. Start each scenario slice
        // with a fresh request namespace after that probe so later iterations cannot rewind slots.
        let context = RunExecutionContext::new(tracker.clone(), next_sequence);
        let mut iteration_events = match run_spec.nemesis {
            JepsenNemesisFamily::None => execute_control_run(layout, run_spec, &context),
            JepsenNemesisFamily::CrashRestart => {
                execute_crash_restart_run(layout, run_spec, &context)
            }
            JepsenNemesisFamily::PartitionHeal => {
                execute_partition_heal_run(layout, run_spec, &context)
            }
            JepsenNemesisFamily::MixedFailover => {
                execute_mixed_failover_run(layout, run_spec, &context)
            }
        }?;
        if iteration_events.is_empty() {
            return Err(format!(
                "run `{}` iteration {} produced no Jepsen history events",
                run_spec.run_id, iteration
            ));
        }
        next_sequence = iteration_events
            .last()
            .map_or(next_sequence, |event| event.sequence);
        all_events.append(&mut iteration_events);

        let elapsed = started_at.elapsed();
        let _ = tracker.append_event(&render_fault_window_iteration_event(
            run_spec,
            iteration,
            elapsed,
            next_sequence,
        ));
        if fault_window_complete(minimum_fault_window, elapsed) {
            break;
        }
    }

    Ok(all_events)
}

fn archive_qemu_run(
    workspace_root: &Path,
    run_id: &str,
    history_file: &Path,
    output_root: &Path,
) -> Result<(), String> {
    let layout = load_qemu_layout(workspace_root)?;
    archive_external_run(&layout, run_id, history_file, output_root)
}

fn archive_kubevirt_run(
    workspace_root: &Path,
    run_id: &str,
    history_file: &Path,
    output_root: &Path,
) -> Result<(), String> {
    let layout = load_kubevirt_layout(workspace_root)?;
    let _helper = prepare_kubevirt_helper(&layout)?;
    archive_external_run(&layout, run_id, history_file, output_root)
}

fn archive_external_run<T: ExternalTestbed>(
    layout: &T,
    run_id: &str,
    history_file: &Path,
    output_root: &Path,
) -> Result<(), String> {
    let run_spec = resolve_run_spec(run_id)?;
    let history =
        load_history(history_file).map_err(|error| format!("failed to load history: {error}"))?;
    let report = analyze_history(&history);
    let logs_archive = fetch_external_logs_archive(layout, run_id, output_root)?;
    let bundle_dir = create_artifact_bundle(
        output_root,
        &run_spec,
        &history,
        &report,
        Some(&logs_archive),
    )
    .map_err(|error| format!("failed to create Jepsen artifact bundle: {error}"))?;
    println!("artifact_bundle={}", bundle_dir.display());
    println!(
        "{}_logs_archive={}",
        layout.backend_name(),
        logs_archive.display()
    );
    if report.release_gate_passed() {
        Ok(())
    } else {
        Err(String::from("Jepsen release gate is blocked"))
    }
}

#[derive(Clone, Copy)]
struct RequestNamespace {
    client_id: ClientId,
    slot_base: u64,
}

impl RequestNamespace {
    fn new() -> Self {
        let millis = u64::try_from(current_time_millis()).unwrap_or(u64::MAX / 1024);
        let nonce = NEXT_PROBE_RESOURCE_ID.fetch_add(1, Ordering::Relaxed);
        Self {
            client_id: ClientId((u128::from(millis) << 32) | u128::from(nonce)),
            slot_base: millis
                .saturating_mul(1024)
                .saturating_add(nonce.saturating_mul(128)),
        }
    }

    fn slot(self, offset: u64) -> Slot {
        Slot(self.slot_base.saturating_add(offset))
    }

    fn client_request(self, operation_id: OperationId, command: AllocCommand) -> ClientRequest {
        ClientRequest {
            operation_id,
            client_id: self.client_id,
            command,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RunTrackerState {
    Running,
    Passed,
    Failed,
}

impl RunTrackerState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Passed => "passed",
            Self::Failed => "failed",
        }
    }

    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "running" => Ok(Self::Running),
            "passed" => Ok(Self::Passed),
            "failed" => Ok(Self::Failed),
            other => Err(format!("invalid run state `{other}`")),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RunTrackerPhase {
    Waiting,
    VerifyingSurface,
    Executing,
    Analyzing,
    Archiving,
    Completed,
    Failed,
}

impl RunTrackerPhase {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Waiting => "waiting",
            Self::VerifyingSurface => "verifying_surface",
            Self::Executing => "executing",
            Self::Analyzing => "analyzing",
            Self::Archiving => "archiving",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }

    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "waiting" => Ok(Self::Waiting),
            "verifying_surface" => Ok(Self::VerifyingSurface),
            "executing" => Ok(Self::Executing),
            "analyzing" => Ok(Self::Analyzing),
            "archiving" => Ok(Self::Archiving),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            other => Err(format!("invalid run phase `{other}`")),
        }
    }
}

#[derive(Clone)]
struct RunTracker {
    backend_name: String,
    run_id: String,
    status_path: PathBuf,
    latest_status_path: PathBuf,
    events_path: PathBuf,
    started_at_millis: u128,
    minimum_fault_window_secs: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RunStatusSnapshot {
    backend_name: String,
    run_id: String,
    state: RunTrackerState,
    phase: RunTrackerPhase,
    detail: String,
    started_at_millis: u128,
    updated_at_millis: u128,
    elapsed_secs: u64,
    minimum_fault_window_secs: Option<u64>,
    history_events: u64,
    history_file: Option<PathBuf>,
    artifact_bundle: Option<PathBuf>,
    logs_archive: Option<PathBuf>,
    release_gate_passed: Option<bool>,
    blockers: Option<usize>,
    last_error: Option<String>,
}

impl RunTracker {
    fn new(
        backend_name: &str,
        run_spec: &JepsenRunSpec,
        output_root: &Path,
    ) -> Result<Self, String> {
        fs::create_dir_all(output_root).map_err(|error| {
            format!(
                "failed to create Jepsen output root {}: {error}",
                output_root.display()
            )
        })?;
        let run_id = sanitize_run_id(&run_spec.run_id);
        let tracker = Self {
            backend_name: String::from(backend_name),
            run_id: run_spec.run_id.clone(),
            status_path: output_root.join(format!("{run_id}-status.txt")),
            latest_status_path: output_root.join(JEPSEN_LATEST_STATUS_FILE_NAME),
            events_path: output_root.join(format!("{run_id}-events.log")),
            started_at_millis: current_time_millis(),
            minimum_fault_window_secs: effective_minimum_fault_window_secs(run_spec),
        };
        tracker.write_status(&RunStatusSnapshot {
            backend_name: tracker.backend_name.clone(),
            run_id: tracker.run_id.clone(),
            state: RunTrackerState::Running,
            phase: RunTrackerPhase::Waiting,
            detail: String::from("initializing"),
            started_at_millis: tracker.started_at_millis,
            updated_at_millis: tracker.started_at_millis,
            elapsed_secs: 0,
            minimum_fault_window_secs: tracker.minimum_fault_window_secs,
            history_events: 0,
            history_file: None,
            artifact_bundle: None,
            logs_archive: None,
            release_gate_passed: None,
            blockers: None,
            last_error: None,
        })?;
        tracker.append_event("run initialized")?;
        Ok(tracker)
    }

    fn snapshot(&self) -> Result<RunStatusSnapshot, String> {
        load_run_status_snapshot(&self.status_path)
    }

    fn set_phase(&self, phase: RunTrackerPhase, detail: &str) -> Result<(), String> {
        let mut snapshot = self.snapshot()?;
        snapshot.phase = phase;
        snapshot.detail = String::from(detail);
        snapshot.updated_at_millis = current_time_millis();
        snapshot.elapsed_secs =
            elapsed_secs_since(self.started_at_millis, snapshot.updated_at_millis);
        self.write_status(&snapshot)?;
        self.append_event(detail)
    }

    fn set_history_events(&self, history_events: u64) -> Result<(), String> {
        let mut snapshot = self.snapshot()?;
        snapshot.history_events = history_events;
        snapshot.updated_at_millis = current_time_millis();
        snapshot.elapsed_secs =
            elapsed_secs_since(self.started_at_millis, snapshot.updated_at_millis);
        self.write_status(&snapshot)
    }

    fn complete(
        &self,
        history_file: &Path,
        artifact_bundle: &Path,
        logs_archive: &Path,
        report: &allocdb_node::jepsen::JepsenAnalysisReport,
    ) -> Result<(), String> {
        let mut snapshot = self.snapshot()?;
        snapshot.state = if report.release_gate_passed() {
            RunTrackerState::Passed
        } else {
            RunTrackerState::Failed
        };
        snapshot.phase = RunTrackerPhase::Completed;
        snapshot.detail = String::from("run completed");
        snapshot.updated_at_millis = current_time_millis();
        snapshot.elapsed_secs =
            elapsed_secs_since(self.started_at_millis, snapshot.updated_at_millis);
        snapshot.history_file = Some(history_file.to_path_buf());
        snapshot.artifact_bundle = Some(artifact_bundle.to_path_buf());
        snapshot.logs_archive = Some(logs_archive.to_path_buf());
        snapshot.release_gate_passed = Some(report.release_gate_passed());
        snapshot.blockers = Some(report.blockers.len());
        snapshot.last_error = None;
        self.write_status(&snapshot)?;
        self.append_event("run completed")
    }

    fn fail(&self, phase: RunTrackerPhase, error: &str) -> Result<(), String> {
        let mut snapshot = self.snapshot()?;
        snapshot.state = RunTrackerState::Failed;
        snapshot.phase = phase;
        snapshot.detail = String::from("run failed");
        snapshot.updated_at_millis = current_time_millis();
        snapshot.elapsed_secs =
            elapsed_secs_since(self.started_at_millis, snapshot.updated_at_millis);
        snapshot.last_error = Some(String::from(error));
        self.write_status(&snapshot)?;
        self.append_event(&format!("error: {error}"))
    }

    fn append_event(&self, detail: &str) -> Result<(), String> {
        let line = format!("time_millis={} detail={detail}\n", current_time_millis());
        append_text_line(&self.events_path, &line)
    }

    fn write_status(&self, snapshot: &RunStatusSnapshot) -> Result<(), String> {
        let bytes = encode_run_status_snapshot(snapshot);
        write_text_atomically(&self.status_path, &bytes)?;
        write_text_atomically(&self.latest_status_path, &bytes)
    }
}

#[derive(Clone)]
struct RunExecutionContext {
    namespace: RequestNamespace,
    tracker: RunTracker,
    history_sequence_start: u64,
}

impl RunExecutionContext {
    fn new(tracker: RunTracker, history_sequence_start: u64) -> Self {
        Self {
            namespace: RequestNamespace::new(),
            tracker,
            history_sequence_start,
        }
    }

    fn slot(&self, offset: u64) -> Slot {
        self.namespace.slot(offset)
    }

    fn client_request(&self, operation_id: OperationId, command: AllocCommand) -> ClientRequest {
        self.namespace.client_request(operation_id, command)
    }
}

struct HistoryBuilder {
    next_sequence: u64,
    events: Vec<JepsenHistoryEvent>,
    tracker: Option<RunTracker>,
}

impl HistoryBuilder {
    fn new(tracker: Option<RunTracker>, next_sequence: u64) -> Self {
        Self {
            next_sequence,
            events: Vec::new(),
            tracker,
        }
    }

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
        if let Some(tracker) = &self.tracker {
            let _ = tracker.set_history_events(self.next_sequence);
            let _ = tracker.append_event(&format!("history sequence={}", self.next_sequence));
        }
    }

    fn finish(self) -> Vec<JepsenHistoryEvent> {
        self.events
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReplicaWatchSnapshot {
    replica_id: ReplicaId,
    status: Option<ReplicaRuntimeStatus>,
    queue_depth: Option<u64>,
    logical_slot_lag: Option<u64>,
    expiration_backlog: Option<u64>,
    accepting_writes: Option<bool>,
    error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RuntimeReplicaProbe {
    replica: LocalClusterReplicaConfig,
    status: Result<ReplicaRuntimeStatus, String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RuntimeReplicaTopology {
    active: usize,
    primaries: usize,
    backups: usize,
}

struct KubevirtWatchLaneSnapshot {
    name: String,
    snapshot: Option<RunStatusSnapshot>,
    replicas: Vec<ReplicaWatchSnapshot>,
    recent_events: Vec<WatchEvent>,
    lane_error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct WatchEvent {
    time_millis: u128,
    detail: String,
}

fn run_status_path(output_root: &Path, run_id: &str) -> PathBuf {
    output_root.join(format!("{}-status.txt", sanitize_run_id(run_id)))
}

fn run_events_path(output_root: &Path, run_id: &str) -> PathBuf {
    output_root.join(format!("{}-events.log", sanitize_run_id(run_id)))
}

fn elapsed_secs_since(started_at_millis: u128, updated_at_millis: u128) -> u64 {
    let elapsed_millis = updated_at_millis.saturating_sub(started_at_millis);
    u64::try_from(elapsed_millis / 1_000).unwrap_or(u64::MAX)
}

fn encode_run_status_snapshot(snapshot: &RunStatusSnapshot) -> String {
    let mut lines = vec![
        format!("version={JEPSEN_RUN_STATUS_VERSION}"),
        format!("backend={}", snapshot.backend_name),
        format!("run_id={}", snapshot.run_id),
        format!("state={}", snapshot.state.as_str()),
        format!("phase={}", snapshot.phase.as_str()),
        format!("detail={}", snapshot.detail),
        format!("started_at_millis={}", snapshot.started_at_millis),
        format!("updated_at_millis={}", snapshot.updated_at_millis),
        format!("elapsed_secs={}", snapshot.elapsed_secs),
        format!(
            "minimum_fault_window_secs={}",
            snapshot
                .minimum_fault_window_secs
                .map_or(String::from("none"), |value| value.to_string())
        ),
        format!("history_events={}", snapshot.history_events),
        format!(
            "history_file={}",
            snapshot
                .history_file
                .as_ref()
                .map_or_else(|| String::from("none"), |path| path.display().to_string())
        ),
        format!(
            "artifact_bundle={}",
            snapshot
                .artifact_bundle
                .as_ref()
                .map_or_else(|| String::from("none"), |path| path.display().to_string())
        ),
        format!(
            "logs_archive={}",
            snapshot
                .logs_archive
                .as_ref()
                .map_or_else(|| String::from("none"), |path| path.display().to_string())
        ),
        format!(
            "release_gate_passed={}",
            snapshot.release_gate_passed.map_or_else(
                || String::from("none"),
                |value| if value {
                    String::from("true")
                } else {
                    String::from("false")
                }
            )
        ),
        format!(
            "blockers={}",
            snapshot
                .blockers
                .map_or_else(|| String::from("none"), |value| value.to_string())
        ),
        format!(
            "last_error={}",
            snapshot
                .last_error
                .as_ref()
                .map_or_else(|| String::from("none"), Clone::clone)
        ),
    ];
    lines.push(String::new());
    lines.join("\n")
}

fn load_run_status_snapshot(path: &Path) -> Result<RunStatusSnapshot, String> {
    let mut bytes = String::new();
    File::open(path)
        .map_err(|error| format!("failed to open run status {}: {error}", path.display()))?
        .read_to_string(&mut bytes)
        .map_err(|error| format!("failed to read run status {}: {error}", path.display()))?;
    decode_run_status_snapshot(&bytes)
}

fn maybe_load_run_status_snapshot(path: &Path) -> Result<Option<RunStatusSnapshot>, String> {
    match File::open(path) {
        Ok(mut file) => {
            let mut bytes = String::new();
            file.read_to_string(&mut bytes).map_err(|error| {
                format!("failed to read run status {}: {error}", path.display())
            })?;
            decode_run_status_snapshot(&bytes).map(Some)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(format!(
            "failed to open run status {}: {error}",
            path.display()
        )),
    }
}

fn decode_run_status_snapshot(bytes: &str) -> Result<RunStatusSnapshot, String> {
    let mut fields = std::collections::BTreeMap::new();
    for line in bytes.lines().filter(|line| !line.trim().is_empty()) {
        let (key, value) = line
            .split_once('=')
            .ok_or_else(|| format!("invalid run status line `{line}`"))?;
        fields.insert(String::from(key), String::from(value));
    }

    let version = parse_required_u32(&fields, "version")?;
    if version != JEPSEN_RUN_STATUS_VERSION {
        return Err(format!("unsupported run status version `{version}`"));
    }

    Ok(RunStatusSnapshot {
        backend_name: required_field(&fields, "backend")?.to_owned(),
        run_id: required_field(&fields, "run_id")?.to_owned(),
        state: RunTrackerState::parse(required_field(&fields, "state")?)?,
        phase: RunTrackerPhase::parse(required_field(&fields, "phase")?)?,
        detail: required_field(&fields, "detail")?.to_owned(),
        started_at_millis: parse_required_u128(&fields, "started_at_millis")?,
        updated_at_millis: parse_required_u128(&fields, "updated_at_millis")?,
        elapsed_secs: parse_required_u64(&fields, "elapsed_secs")?,
        minimum_fault_window_secs: parse_optional_u64(required_field(
            &fields,
            "minimum_fault_window_secs",
        )?)?,
        history_events: parse_required_u64(&fields, "history_events")?,
        history_file: parse_optional_path(required_field(&fields, "history_file")?),
        artifact_bundle: parse_optional_path(required_field(&fields, "artifact_bundle")?),
        logs_archive: parse_optional_path(required_field(&fields, "logs_archive")?),
        release_gate_passed: parse_optional_bool(required_field(&fields, "release_gate_passed")?)?,
        blockers: parse_optional_usize(required_field(&fields, "blockers")?)?,
        last_error: parse_optional_string(required_field(&fields, "last_error")?),
    })
}

fn required_field<'a>(
    fields: &'a std::collections::BTreeMap<String, String>,
    field: &str,
) -> Result<&'a str, String> {
    fields
        .get(field)
        .map(String::as_str)
        .ok_or_else(|| format!("missing run status field `{field}`"))
}

fn parse_required_u32(
    fields: &std::collections::BTreeMap<String, String>,
    field: &str,
) -> Result<u32, String> {
    required_field(fields, field)?
        .parse::<u32>()
        .map_err(|error| format!("invalid `{field}`: {error}"))
}

fn parse_required_u64(
    fields: &std::collections::BTreeMap<String, String>,
    field: &str,
) -> Result<u64, String> {
    required_field(fields, field)?
        .parse::<u64>()
        .map_err(|error| format!("invalid `{field}`: {error}"))
}

fn parse_required_u128(
    fields: &std::collections::BTreeMap<String, String>,
    field: &str,
) -> Result<u128, String> {
    required_field(fields, field)?
        .parse::<u128>()
        .map_err(|error| format!("invalid `{field}`: {error}"))
}

fn parse_optional_u64(value: &str) -> Result<Option<u64>, String> {
    if value == "none" {
        Ok(None)
    } else {
        value
            .parse::<u64>()
            .map(Some)
            .map_err(|error| format!("invalid optional u64 `{value}`: {error}"))
    }
}

fn parse_optional_usize(value: &str) -> Result<Option<usize>, String> {
    if value == "none" {
        Ok(None)
    } else {
        value
            .parse::<usize>()
            .map(Some)
            .map_err(|error| format!("invalid optional usize `{value}`: {error}"))
    }
}

fn parse_optional_bool(value: &str) -> Result<Option<bool>, String> {
    match value {
        "none" => Ok(None),
        "true" => Ok(Some(true)),
        "false" => Ok(Some(false)),
        other => Err(format!("invalid optional bool `{other}`")),
    }
}

fn parse_optional_path(value: &str) -> Option<PathBuf> {
    if value == "none" {
        None
    } else {
        Some(PathBuf::from(value))
    }
}

fn parse_optional_string(value: &str) -> Option<String> {
    if value == "none" {
        None
    } else {
        Some(String::from(value))
    }
}

fn write_text_atomically(path: &Path, bytes: &str) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("failed to create {}: {error}", parent.display()))?;
    }
    let temp_path = path.with_extension(format!("{}.tmp", std::process::id()));
    let mut file = File::create(&temp_path)
        .map_err(|error| format!("failed to create {}: {error}", temp_path.display()))?;
    file.write_all(bytes.as_bytes())
        .map_err(|error| format!("failed to write {}: {error}", temp_path.display()))?;
    file.sync_all()
        .map_err(|error| format!("failed to sync {}: {error}", temp_path.display()))?;
    drop(file);
    fs::rename(&temp_path, path).map_err(|error| {
        let _ = fs::remove_file(&temp_path);
        format!("failed to replace {}: {error}", path.display())
    })
}

fn append_text_line(path: &Path, line: &str) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("failed to create {}: {error}", parent.display()))?;
    }
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|error| format!("failed to open {}: {error}", path.display()))?;
    file.write_all(line.as_bytes())
        .map_err(|error| format!("failed to append {}: {error}", path.display()))?;
    file.sync_all()
        .map_err(|error| format!("failed to sync {}: {error}", path.display()))
}

fn watch_kubevirt(
    workspace_root: &Path,
    output_root: &Path,
    run_id: Option<&str>,
    refresh_millis: u64,
    follow: bool,
) -> Result<(), String> {
    let layout = load_kubevirt_layout(workspace_root)?;
    let _helper = prepare_kubevirt_helper(&layout)?;
    let status_path = run_id.map_or_else(
        || output_root.join(JEPSEN_LATEST_STATUS_FILE_NAME),
        |run_id| run_status_path(output_root, run_id),
    );
    let refresh_millis = refresh_millis.max(250);

    loop {
        let snapshot = maybe_load_run_status_snapshot(&status_path)?;
        let replicas = collect_replica_watch_snapshots(&layout);
        let recent_events = load_recent_run_events(output_root, snapshot.as_ref(), run_id, 8);
        render_kubevirt_watch(
            &status_path,
            snapshot.as_ref(),
            &replicas,
            &recent_events,
            refresh_millis,
            follow,
        )?;
        if !follow
            && snapshot
                .as_ref()
                .is_some_and(|snapshot| snapshot.state != RunTrackerState::Running)
        {
            break;
        }
        thread::sleep(Duration::from_millis(refresh_millis));
    }
    Ok(())
}

fn watch_kubevirt_fleet(
    lanes: &[KubevirtWatchLaneSpec],
    refresh_millis: u64,
    follow: bool,
) -> Result<(), String> {
    let mut contexts = lanes.iter().cloned().map(|_| None).collect::<Vec<_>>();
    let refresh_millis = refresh_millis.max(250);

    loop {
        let mut snapshots = Vec::with_capacity(lanes.len());
        for (index, spec) in lanes.iter().enumerate() {
            if contexts[index].is_none() {
                contexts[index] = try_prepare_kubevirt_watch_lane_context(spec).ok();
            }
            let snapshot = match contexts[index].as_ref() {
                Some(context) => match collect_kubevirt_watch_lane_snapshot(context) {
                    Ok(snapshot) => snapshot,
                    Err(error) => KubevirtWatchLaneSnapshot {
                        name: spec.name.clone(),
                        snapshot: None,
                        replicas: Vec::new(),
                        recent_events: Vec::new(),
                        lane_error: Some(error),
                    },
                },
                None => KubevirtWatchLaneSnapshot {
                    name: spec.name.clone(),
                    snapshot: None,
                    replicas: Vec::new(),
                    recent_events: Vec::new(),
                    lane_error: Some(format!(
                        "lane workspace is not ready yet: expected {}",
                        kubevirt_testbed_layout_path(&spec.workspace_root).display()
                    )),
                },
            };
            snapshots.push(snapshot);
        }
        render_kubevirt_fleet_watch(&snapshots, refresh_millis, follow)?;
        if !follow {
            break;
        }
        thread::sleep(Duration::from_millis(refresh_millis));
    }

    Ok(())
}

fn try_prepare_kubevirt_watch_lane_context(
    spec: &KubevirtWatchLaneSpec,
) -> Result<KubevirtWatchLaneContext, String> {
    let layout = load_kubevirt_layout(&spec.workspace_root)?;
    let helper = prepare_kubevirt_helper(&layout)?;
    Ok(KubevirtWatchLaneContext {
        spec: spec.clone(),
        layout,
        _helper: helper,
    })
}

fn collect_kubevirt_watch_lane_snapshot(
    lane: &KubevirtWatchLaneContext,
) -> Result<KubevirtWatchLaneSnapshot, String> {
    let status_path = lane.spec.output_root.join(JEPSEN_LATEST_STATUS_FILE_NAME);
    let snapshot = maybe_load_run_status_snapshot(&status_path)?;
    let replicas = collect_replica_watch_snapshots(&lane.layout);
    let recent_events = load_recent_run_events(&lane.spec.output_root, snapshot.as_ref(), None, 4);
    Ok(KubevirtWatchLaneSnapshot {
        name: lane.spec.name.clone(),
        snapshot,
        replicas,
        recent_events,
        lane_error: None,
    })
}

fn collect_replica_watch_snapshots(layout: &KubevirtTestbedLayout) -> Vec<ReplicaWatchSnapshot> {
    let metrics_namespace = RequestNamespace::new();
    layout
        .replica_layout
        .replicas
        .iter()
        .map(|replica| {
            let status = request_remote_control_status(layout, replica);
            match status {
                Ok(status) => {
                    let metrics = send_remote_api_request(
                        layout,
                        &replica.client_addr.ip().to_string(),
                        replica.client_addr.port(),
                        &ApiRequest::GetMetrics(MetricsRequest {
                            current_wall_clock_slot: metrics_namespace.slot(0),
                        }),
                    )
                    .and_then(|bytes| decode_external_api_response(layout, &bytes));
                    match metrics {
                        Ok(ApiResponse::GetMetrics(response)) => ReplicaWatchSnapshot {
                            replica_id: replica.replica_id,
                            status: Some(status),
                            queue_depth: Some(u64::from(response.metrics.queue_depth)),
                            logical_slot_lag: Some(response.metrics.core.logical_slot_lag),
                            expiration_backlog: Some(u64::from(
                                response.metrics.core.expiration_backlog,
                            )),
                            accepting_writes: Some(response.metrics.accepting_writes),
                            error: None,
                        },
                        Ok(other) => ReplicaWatchSnapshot {
                            replica_id: replica.replica_id,
                            status: Some(status),
                            queue_depth: None,
                            logical_slot_lag: None,
                            expiration_backlog: None,
                            accepting_writes: None,
                            error: Some(format!("unexpected metrics response {other:?}")),
                        },
                        Err(error) => ReplicaWatchSnapshot {
                            replica_id: replica.replica_id,
                            status: Some(status),
                            queue_depth: None,
                            logical_slot_lag: None,
                            expiration_backlog: None,
                            accepting_writes: None,
                            error: Some(error),
                        },
                    }
                }
                Err(error) => ReplicaWatchSnapshot {
                    replica_id: replica.replica_id,
                    status: None,
                    queue_depth: None,
                    logical_slot_lag: None,
                    expiration_backlog: None,
                    accepting_writes: None,
                    error: Some(error),
                },
            }
        })
        .collect()
}

fn load_recent_run_events(
    output_root: &Path,
    snapshot: Option<&RunStatusSnapshot>,
    run_id: Option<&str>,
    limit: usize,
) -> Vec<WatchEvent> {
    let Some(events_path) = snapshot
        .map(|snapshot| run_events_path(output_root, &snapshot.run_id))
        .or_else(|| run_id.map(|run_id| run_events_path(output_root, run_id)))
    else {
        return Vec::new();
    };
    let Ok(bytes) = fs::read_to_string(&events_path) else {
        return Vec::new();
    };
    let mut events = bytes
        .lines()
        .filter_map(|line| parse_watch_event_line(line).ok())
        .collect::<Vec<_>>();
    if events.len() > limit {
        events.drain(..events.len().saturating_sub(limit));
    }
    events
}

fn render_kubevirt_watch(
    status_path: &Path,
    snapshot: Option<&RunStatusSnapshot>,
    replicas: &[ReplicaWatchSnapshot],
    recent_events: &[WatchEvent],
    refresh_millis: u64,
    follow: bool,
) -> Result<(), String> {
    let color = watch_color_enabled();
    let spinner = watch_spinner_frame(snapshot, refresh_millis);
    let pulse = watch_pulse_frame(snapshot);
    print!("\x1B[2J\x1B[H");
    println!(
        "{}",
        watch_style(
            color,
            &[ANSI_BOLD, ANSI_CYAN],
            &format!("{spinner} AllocDB Jepsen KubeVirt Watch {pulse}"),
        )
    );
    println!(
        "{}",
        watch_style(
            color,
            &[ANSI_DIM],
            &format!(
                "status_file={}   refresh={}ms   mode={}",
                status_path.display(),
                refresh_millis,
                if follow { "follow" } else { "one-shot" }
            ),
        )
    );
    println!("{}", watch_rule(color));
    render_run_summary(snapshot, color);
    println!();
    render_replica_watch_rows(replicas, color);
    println!();
    render_recent_events(snapshot, recent_events, color);
    std::io::stdout()
        .flush()
        .map_err(|error| format!("failed to flush watcher output: {error}"))
}

fn render_kubevirt_fleet_watch(
    lanes: &[KubevirtWatchLaneSnapshot],
    refresh_millis: u64,
    follow: bool,
) -> Result<(), String> {
    let color = watch_color_enabled();
    let anchor = lanes
        .iter()
        .find_map(|lane| {
            lane.snapshot
                .as_ref()
                .filter(|snapshot| snapshot.state == RunTrackerState::Running)
        })
        .or_else(|| lanes.iter().find_map(|lane| lane.snapshot.as_ref()));
    let spinner = watch_spinner_frame(anchor, refresh_millis);
    let pulse = watch_pulse_frame(anchor);
    print!("\x1B[2J\x1B[H");
    println!(
        "{}",
        watch_style(
            color,
            &[ANSI_BOLD, ANSI_CYAN],
            &format!("{spinner} AllocDB Jepsen KubeVirt Fleet Watch {pulse}"),
        )
    );
    println!(
        "{}",
        watch_style(
            color,
            &[ANSI_DIM],
            &format!(
                "lanes={}   refresh={}ms   mode={}   use one output root per lane",
                lanes.len(),
                refresh_millis,
                if follow { "follow" } else { "one-shot" }
            ),
        )
    );
    println!("{}", watch_rule(color));
    render_kubevirt_fleet_lane_rows(lanes, color);
    println!();
    render_kubevirt_fleet_replica_rows(lanes, color);
    println!();
    render_kubevirt_fleet_recent_events(lanes, color);
    std::io::stdout()
        .flush()
        .map_err(|error| format!("failed to flush fleet watcher output: {error}"))
}

fn render_run_summary(snapshot: Option<&RunStatusSnapshot>, color: bool) {
    println!(
        "{}",
        watch_style(color, &[ANSI_BOLD, ANSI_BLUE], "Run Summary")
    );
    if let Some(snapshot) = snapshot {
        render_summary_row(
            color,
            "Run",
            &watch_style(color, &[ANSI_BOLD, ANSI_WHITE], &snapshot.run_id),
        );
        render_summary_row(color, "State", &run_state_badge(snapshot, color));
        render_summary_row(color, "Phase", &run_phase_badge(snapshot.phase, color));
        render_summary_row(
            color,
            "Elapsed",
            &format_human_duration(snapshot.elapsed_secs),
        );
        render_summary_row(
            color,
            "Window",
            &format_fault_window_summary(snapshot, color),
        );
        render_summary_row(
            color,
            "History",
            &format!(
                "📜 {} {}",
                compact_counter(snapshot.history_events),
                pluralize(snapshot.history_events, "event", "events")
            ),
        );
        render_summary_row(color, "Gate", &format_release_gate_status(snapshot, color));
        render_summary_row(
            color,
            "Blockers",
            &format_blocker_count(snapshot.blockers, color),
        );
        render_summary_row(color, "Detail", &snapshot.detail);
        if let Some(error) = &snapshot.last_error {
            render_summary_row(
                color,
                "Last Error",
                &watch_style(
                    color,
                    &[ANSI_RED],
                    &truncate_for_watch(error, WATCH_RULE_WIDTH.saturating_sub(18)),
                ),
            );
        }
    } else {
        render_summary_row(
            color,
            "Run",
            &watch_style(
                color,
                &[ANSI_YELLOW],
                "💤 waiting for first run status file",
            ),
        );
        render_summary_row(
            color,
            "Hint",
            "start `run-kubevirt` in another terminal and this view will lock on",
        );
    }
}

fn render_replica_watch_rows(replicas: &[ReplicaWatchSnapshot], color: bool) {
    println!(
        "{}",
        watch_style(color, &[ANSI_BOLD, ANSI_BLUE], "Replica Vibes")
    );
    println!(
        "{}",
        watch_style(
            color,
            &[ANSI_DIM],
            "id  health   role       view   commit    wr   queue     lag       due   note"
        )
    );
    for replica in replicas {
        println!("{}", format_replica_watch_row(replica, color));
    }
}

fn render_kubevirt_fleet_lane_rows(lanes: &[KubevirtWatchLaneSnapshot], color: bool) {
    println!(
        "{}",
        watch_style(color, &[ANSI_BOLD, ANSI_BLUE], "Lane Pulse")
    );
    println!(
        "{}",
        watch_style(
            color,
            &[ANSI_DIM],
            "lane  run                              state      phase        elapsed  window             hist   lead  note"
        )
    );
    for (index, lane) in lanes.iter().enumerate() {
        println!("{}", format_kubevirt_fleet_lane_row(index, lane, color));
    }
}

fn format_replica_watch_row(replica: &ReplicaWatchSnapshot, color: bool) -> String {
    let health = replica.status.as_ref().map_or("down", |status| {
        if status.state == ReplicaRuntimeState::Active {
            "up"
        } else {
            "faulted"
        }
    });
    let view = replica
        .status
        .as_ref()
        .map_or(String::from("-"), |status| status.current_view.to_string());
    let commit_lsn = replica.status.as_ref().map_or_else(
        || String::from("-"),
        |status| {
            status
                .commit_lsn
                .map_or_else(|| String::from("-"), |value| compact_counter(value.get()))
        },
    );
    let writes = replica.accepting_writes.map_or(String::from("-"), |value| {
        if value {
            String::from("on")
        } else {
            String::from("off")
        }
    });
    let queue = replica
        .queue_depth
        .map_or_else(|| String::from("-"), compact_counter);
    let lag = replica
        .logical_slot_lag
        .map_or_else(|| String::from("-"), compact_counter);
    let backlog = replica
        .expiration_backlog
        .map_or_else(|| String::from("-"), compact_counter);
    let note = truncate_for_watch(&replica_watch_note(replica), 46);
    let health_cell = pad_watch_cell(health, 8);
    let role_cell = pad_watch_cell(&role_label(replica), 10);
    let view_cell = pad_watch_cell_right(&view, 6);
    let commit_cell = pad_watch_cell_right(&commit_lsn, 9);
    let writes_cell = pad_watch_cell(&writes, 4);
    let queue_cell = pad_watch_cell_right(&queue, 9);
    let lag_cell = pad_watch_cell_right(&lag, 9);
    let backlog_cell = pad_watch_cell_right(&backlog, 5);

    format!(
        "{} {} {} {} {} {} {} {} {} {}",
        watch_style(
            color,
            &[ANSI_BOLD, ANSI_WHITE],
            &pad_watch_cell_right(&replica.replica_id.get().to_string(), 2)
        ),
        style_health_cell(color, health, &health_cell),
        style_role_cell(color, replica, &role_cell),
        watch_style(color, &[ANSI_WHITE], &view_cell),
        watch_style(color, &[ANSI_WHITE], &commit_cell),
        style_writes_cell(color, &writes, &writes_cell),
        watch_style(color, &[ANSI_WHITE], &queue_cell),
        watch_style(color, &[ANSI_WHITE], &lag_cell),
        style_backlog_cell(color, replica.expiration_backlog, &backlog_cell),
        style_note_cell(color, replica, &note)
    )
}

fn format_kubevirt_fleet_lane_row(
    lane_index: usize,
    lane: &KubevirtWatchLaneSnapshot,
    color: bool,
) -> String {
    let lane_cell = pad_watch_cell(&lane.name, 5);
    let run = lane.snapshot.as_ref().map_or_else(
        || String::from("<waiting>"),
        |snapshot| truncate_for_watch(&snapshot.run_id, 32),
    );
    let run_cell = pad_watch_cell(&run, 32);
    let state_cell = lane.snapshot.as_ref().map_or_else(
        || watch_style(color, &[ANSI_DIM], &pad_watch_cell("waiting", 10)),
        |snapshot| run_state_badge(snapshot, color),
    );
    let phase_cell = lane.snapshot.as_ref().map_or_else(
        || watch_style(color, &[ANSI_DIM], &pad_watch_cell("waiting", 12)),
        |snapshot| run_phase_badge(snapshot.phase, color),
    );
    let elapsed_cell = pad_watch_cell_right(
        &lane.snapshot.as_ref().map_or_else(
            || String::from("-"),
            |snapshot| format_human_duration(snapshot.elapsed_secs),
        ),
        7,
    );
    let window_cell = pad_watch_cell(
        &lane
            .snapshot
            .as_ref()
            .map_or_else(|| String::from("warming up"), compact_fault_window_progress),
        18,
    );
    let history_cell = pad_watch_cell_right(
        &lane.snapshot.as_ref().map_or_else(
            || String::from("-"),
            |snapshot| compact_counter(snapshot.history_events),
        ),
        6,
    );
    let lead_cell = pad_watch_cell(&fleet_primary_label(&lane.replicas), 5);
    let note = truncate_for_watch(
        &lane.snapshot.as_ref().map_or_else(
            || {
                lane.lane_error
                    .clone()
                    .unwrap_or_else(|| String::from("waiting for first run status file"))
            },
            |snapshot| {
                snapshot
                    .last_error
                    .clone()
                    .filter(|error| error != "none")
                    .unwrap_or_else(|| snapshot.detail.clone())
            },
        ),
        28,
    );
    let note_cell = pad_watch_cell(&note, 28);

    format!(
        "{} {} {} {} {} {} {} {} {}",
        style_lane_name_cell(color, lane_index, &lane_cell),
        watch_style(color, &[ANSI_WHITE], &run_cell),
        state_cell,
        phase_cell,
        watch_style(color, &[ANSI_WHITE], &elapsed_cell),
        watch_style(color, &[ANSI_WHITE], &window_cell),
        watch_style(color, &[ANSI_WHITE], &history_cell),
        watch_style(color, &[ANSI_WHITE], &lead_cell),
        style_lane_note_cell(color, lane, &note_cell)
    )
}

fn render_kubevirt_fleet_replica_rows(lanes: &[KubevirtWatchLaneSnapshot], color: bool) {
    println!(
        "{}",
        watch_style(color, &[ANSI_BOLD, ANSI_BLUE], "Replica Vibes")
    );
    println!(
        "{}",
        watch_style(
            color,
            &[ANSI_DIM],
            "lane  id  health   role       view   commit    wr   queue     lag       due   note"
        )
    );
    for (index, lane) in lanes.iter().enumerate() {
        for replica in &lane.replicas {
            println!(
                "{}",
                format_kubevirt_fleet_replica_row(index, &lane.name, replica, color)
            );
        }
    }
}

fn format_kubevirt_fleet_replica_row(
    lane_index: usize,
    lane_name: &str,
    replica: &ReplicaWatchSnapshot,
    color: bool,
) -> String {
    let health = replica.status.as_ref().map_or("down", |status| {
        if status.state == ReplicaRuntimeState::Active {
            "up"
        } else {
            "faulted"
        }
    });
    let view = replica
        .status
        .as_ref()
        .map_or(String::from("-"), |status| status.current_view.to_string());
    let commit_lsn = replica.status.as_ref().map_or_else(
        || String::from("-"),
        |status| {
            status
                .commit_lsn
                .map_or_else(|| String::from("-"), |value| compact_counter(value.get()))
        },
    );
    let writes = replica.accepting_writes.map_or(String::from("-"), |value| {
        if value {
            String::from("on")
        } else {
            String::from("off")
        }
    });
    let queue = replica
        .queue_depth
        .map_or_else(|| String::from("-"), compact_counter);
    let lag = replica
        .logical_slot_lag
        .map_or_else(|| String::from("-"), compact_counter);
    let backlog = replica
        .expiration_backlog
        .map_or_else(|| String::from("-"), compact_counter);
    let note = truncate_for_watch(&replica_watch_note(replica), 40);
    let health_cell = pad_watch_cell(health, 8);
    let role_cell = pad_watch_cell(&role_label(replica), 10);
    let view_cell = pad_watch_cell_right(&view, 6);
    let commit_cell = pad_watch_cell_right(&commit_lsn, 9);
    let writes_cell = pad_watch_cell(&writes, 4);
    let queue_cell = pad_watch_cell_right(&queue, 9);
    let lag_cell = pad_watch_cell_right(&lag, 9);
    let backlog_cell = pad_watch_cell_right(&backlog, 5);

    format!(
        "{} {} {} {} {} {} {} {} {} {} {}",
        style_lane_name_cell(color, lane_index, &pad_watch_cell(lane_name, 5)),
        watch_style(
            color,
            &[ANSI_BOLD, ANSI_WHITE],
            &pad_watch_cell_right(&replica.replica_id.get().to_string(), 2)
        ),
        style_health_cell(color, health, &health_cell),
        style_role_cell(color, replica, &role_cell),
        watch_style(color, &[ANSI_WHITE], &view_cell),
        watch_style(color, &[ANSI_WHITE], &commit_cell),
        style_writes_cell(color, &writes, &writes_cell),
        watch_style(color, &[ANSI_WHITE], &queue_cell),
        watch_style(color, &[ANSI_WHITE], &lag_cell),
        style_backlog_cell(color, replica.expiration_backlog, &backlog_cell),
        style_note_cell(color, replica, &note)
    )
}

fn render_recent_events(
    snapshot: Option<&RunStatusSnapshot>,
    recent_events: &[WatchEvent],
    color: bool,
) {
    println!(
        "{}",
        watch_style(color, &[ANSI_BOLD, ANSI_BLUE], "Recent Events")
    );
    if recent_events.is_empty() {
        println!("{}", watch_style(color, &[ANSI_DIM], "  <none yet>"));
        return;
    }
    let base_time = snapshot
        .map(|snapshot| snapshot.started_at_millis)
        .or_else(|| recent_events.first().map(|event| event.time_millis))
        .unwrap_or(0);
    for event in recent_events {
        let offset = event.time_millis.saturating_sub(base_time);
        let icon = event_icon(&event.detail);
        let elapsed = format_relative_millis(offset);
        println!(
            "  {} {} {}",
            icon,
            watch_style(color, &[ANSI_DIM], &pad_watch_cell(&elapsed, 8)),
            style_event_detail(color, &event.detail)
        );
    }
}

fn render_kubevirt_fleet_recent_events(lanes: &[KubevirtWatchLaneSnapshot], color: bool) {
    println!(
        "{}",
        watch_style(color, &[ANSI_BOLD, ANSI_BLUE], "Lane Feed")
    );
    for (index, lane) in lanes.iter().enumerate() {
        println!(
            "  {} {}",
            style_lane_name_cell(color, index, &pad_watch_cell(&lane.name, 5)),
            watch_style(
                color,
                &[ANSI_DIM],
                lane.snapshot
                    .as_ref()
                    .map_or("<waiting>", |snapshot| snapshot.run_id.as_str())
            )
        );
        if lane.recent_events.is_empty() {
            println!("{}", watch_style(color, &[ANSI_DIM], "    <none yet>"));
            continue;
        }
        let base_time = lane
            .snapshot
            .as_ref()
            .map(|snapshot| snapshot.started_at_millis)
            .or_else(|| lane.recent_events.first().map(|event| event.time_millis))
            .unwrap_or(0);
        for event in &lane.recent_events {
            let offset = event.time_millis.saturating_sub(base_time);
            let icon = event_icon(&event.detail);
            let elapsed = format_relative_millis(offset);
            println!(
                "    {} {} {}",
                icon,
                watch_style(color, &[ANSI_DIM], &pad_watch_cell(&elapsed, 8)),
                style_event_detail(color, &truncate_for_watch(&event.detail, 72))
            );
        }
    }
}

fn compact_fault_window_progress(snapshot: &RunStatusSnapshot) -> String {
    match snapshot.minimum_fault_window_secs {
        None => String::from("ctrl"),
        Some(total) => {
            let current = snapshot.elapsed_secs.min(total);
            let percent = if total == 0 {
                100
            } else {
                current.saturating_mul(100) / total
            };
            format!("{} {:>3}%", progress_bar(current, total, 8), percent)
        }
    }
}

fn fleet_primary_label(replicas: &[ReplicaWatchSnapshot]) -> String {
    replicas
        .iter()
        .find(|replica| {
            replica.status.as_ref().is_some_and(|status| {
                status.state == ReplicaRuntimeState::Active && status.role == ReplicaRole::Primary
            })
        })
        .map_or_else(
            || String::from("-"),
            |replica| format!("r{}", replica.replica_id.get()),
        )
}

fn lane_style_codes(lane_index: usize) -> &'static [&'static str] {
    match lane_index % 3 {
        0 => &[ANSI_BOLD, ANSI_MAGENTA],
        1 => &[ANSI_BOLD, ANSI_CYAN],
        _ => &[ANSI_BOLD, ANSI_GREEN],
    }
}

fn style_lane_name_cell(color: bool, lane_index: usize, cell: &str) -> String {
    watch_style(color, lane_style_codes(lane_index), cell)
}

fn style_lane_note_cell(color: bool, lane: &KubevirtWatchLaneSnapshot, cell: &str) -> String {
    match lane.snapshot.as_ref() {
        Some(snapshot)
            if snapshot
                .last_error
                .as_ref()
                .is_some_and(|error| error != "none") =>
        {
            watch_style(color, &[ANSI_RED], cell)
        }
        Some(snapshot) if snapshot.state == RunTrackerState::Passed => {
            watch_style(color, &[ANSI_GREEN], cell)
        }
        Some(snapshot) if snapshot.state == RunTrackerState::Failed => {
            watch_style(color, &[ANSI_RED], cell)
        }
        None if lane.lane_error.is_some() => watch_style(color, &[ANSI_ORANGE], cell),
        Some(_) | None => watch_style(color, &[ANSI_DIM], cell),
    }
}

fn parse_watch_event_line(line: &str) -> Result<WatchEvent, String> {
    let (time_prefix, detail) = line
        .split_once(" detail=")
        .ok_or_else(|| format!("invalid watch event line `{line}`"))?;
    let time_millis = time_prefix
        .strip_prefix("time_millis=")
        .ok_or_else(|| format!("invalid watch event timestamp `{line}`"))?
        .parse::<u128>()
        .map_err(|error| format!("invalid watch event timestamp `{line}`: {error}"))?;
    Ok(WatchEvent {
        time_millis,
        detail: String::from(detail),
    })
}

fn watch_color_enabled() -> bool {
    std::io::stdout().is_terminal() && std::env::var_os("NO_COLOR").is_none()
}

fn watch_style(color: bool, codes: &[&str], text: &str) -> String {
    if !color {
        return String::from(text);
    }
    let mut styled = String::new();
    for code in codes {
        styled.push_str(code);
    }
    styled.push_str(text);
    styled.push_str(ANSI_RESET);
    styled
}

fn watch_rule(color: bool) -> String {
    watch_style(color, &[ANSI_DIM], &"━".repeat(WATCH_RULE_WIDTH))
}

fn watch_spinner_frame(snapshot: Option<&RunStatusSnapshot>, refresh_millis: u64) -> &'static str {
    let frame_seed = snapshot
        .map_or_else(current_time_millis, |snapshot| snapshot.updated_at_millis)
        / u128::from((refresh_millis.max(250) / 2).max(1));
    WATCH_SPINNER_FRAMES[(usize::try_from(frame_seed).unwrap_or(0)) % WATCH_SPINNER_FRAMES.len()]
}

fn watch_pulse_frame(snapshot: Option<&RunStatusSnapshot>) -> &'static str {
    let pulse_seed = snapshot.map_or(0, |snapshot| {
        usize::try_from(snapshot.elapsed_secs).unwrap_or(usize::MAX)
    });
    WATCH_PULSE_FRAMES[pulse_seed % WATCH_PULSE_FRAMES.len()]
}

fn render_summary_row(color: bool, label: &str, value: &str) {
    let label = pad_watch_cell(label, 10);
    println!("  {} {}", watch_style(color, &[ANSI_DIM], &label), value);
}

fn run_state_badge(snapshot: &RunStatusSnapshot, color: bool) -> String {
    match snapshot.state {
        RunTrackerState::Running => watch_style(color, &[ANSI_BOLD, ANSI_GREEN], "🟢 RUNNING"),
        RunTrackerState::Passed => watch_style(color, &[ANSI_BOLD, ANSI_GREEN], "✅ PASSED"),
        RunTrackerState::Failed => watch_style(color, &[ANSI_BOLD, ANSI_RED], "💥 FAILED"),
    }
}

fn run_phase_badge(phase: RunTrackerPhase, color: bool) -> String {
    let (icon, label, codes): (&str, &str, &[&str]) = match phase {
        RunTrackerPhase::Waiting => ("💤", "waiting", &[ANSI_DIM]),
        RunTrackerPhase::VerifyingSurface => ("🛰", "verifying", &[ANSI_BOLD, ANSI_CYAN]),
        RunTrackerPhase::Executing => ("⚙", "executing", &[ANSI_BOLD, ANSI_YELLOW]),
        RunTrackerPhase::Analyzing => ("🔎", "analyzing", &[ANSI_BOLD, ANSI_MAGENTA]),
        RunTrackerPhase::Archiving => ("📦", "archiving", &[ANSI_BOLD, ANSI_BLUE]),
        RunTrackerPhase::Completed => ("🏁", "completed", &[ANSI_BOLD, ANSI_GREEN]),
        RunTrackerPhase::Failed => ("🔥", "failed", &[ANSI_BOLD, ANSI_RED]),
    };
    watch_style(color, codes, &format!("{icon} {label}"))
}

fn format_fault_window_summary(snapshot: &RunStatusSnapshot, color: bool) -> String {
    match snapshot.minimum_fault_window_secs {
        Some(target_secs) => {
            let progress = progress_bar(snapshot.elapsed_secs, target_secs, 18);
            let percent_tenths = fault_window_percent_tenths(snapshot.elapsed_secs, target_secs);
            format!(
                "{} {} / {} ({}.{:01}%)",
                watch_style(color, &[ANSI_CYAN], &progress),
                format_human_duration(snapshot.elapsed_secs),
                format_human_duration(target_secs),
                percent_tenths / 10,
                percent_tenths % 10,
            )
        }
        None => watch_style(
            color,
            &[ANSI_GREEN],
            "🎮 control run • no minimum fault window",
        ),
    }
}

fn format_release_gate_status(snapshot: &RunStatusSnapshot, color: bool) -> String {
    match snapshot.release_gate_passed {
        Some(true) => watch_style(color, &[ANSI_BOLD, ANSI_GREEN], "🟢 clear"),
        Some(false) => watch_style(color, &[ANSI_BOLD, ANSI_RED], "⛔ blocked"),
        None => watch_style(color, &[ANSI_YELLOW], "🫧 pending"),
    }
}

fn format_blocker_count(blockers: Option<usize>, color: bool) -> String {
    match blockers {
        Some(0) => watch_style(color, &[ANSI_GREEN], "0"),
        Some(value) => watch_style(color, &[ANSI_RED], &value.to_string()),
        None => watch_style(color, &[ANSI_DIM], "none"),
    }
}

fn progress_bar(current: u64, total: u64, width: usize) -> String {
    if total == 0 {
        return "█".repeat(width.max(1));
    }
    let width = width.max(1);
    let clamped = current.min(total);
    let filled = usize::try_from(
        (u128::from(clamped) * u128::try_from(width).unwrap_or(0)) / u128::from(total),
    )
    .unwrap_or(width)
    .min(width);
    format!(
        "{}{}",
        "█".repeat(filled),
        "░".repeat(width.saturating_sub(filled))
    )
}

fn fault_window_percent_tenths(current: u64, total: u64) -> u64 {
    if total == 0 {
        return 1_000;
    }
    u64::try_from((u128::from(current.min(total)) * 1_000) / u128::from(total)).unwrap_or(1_000)
}

fn format_human_duration(secs: u64) -> String {
    let hours = secs / 3_600;
    let minutes = (secs % 3_600) / 60;
    let seconds = secs % 60;
    if hours > 0 {
        format!("{hours}h {minutes:02}m {seconds:02}s")
    } else if minutes > 0 {
        format!("{minutes}m {seconds:02}s")
    } else {
        format!("{seconds}s")
    }
}

fn format_relative_millis(millis: u128) -> String {
    if millis >= 60_000 {
        let secs = u64::try_from(millis / 1_000).unwrap_or(u64::MAX);
        format!("+{}", format_human_duration(secs))
    } else {
        let tenths = millis / 100;
        let whole = tenths / 10;
        let frac = tenths % 10;
        format!("+{whole}.{frac}s")
    }
}

fn compact_counter(value: u64) -> String {
    match value {
        0..=999 => value.to_string(),
        1_000..=999_999 => format_compact_decimal(value, 1_000, "K"),
        1_000_000..=999_999_999 => format_compact_decimal(value, 1_000_000, "M"),
        1_000_000_000..=999_999_999_999 => format_compact_decimal(value, 1_000_000_000, "B"),
        _ => format_compact_decimal(value, 1_000_000_000_000, "T"),
    }
}

fn format_compact_decimal(value: u64, divisor: u64, suffix: &str) -> String {
    let whole = value / divisor;
    if whole >= 100 {
        return format!("{whole}{suffix}");
    }
    let tenth = ((value % divisor) * 10) / divisor;
    format!("{whole}.{tenth}{suffix}")
}

fn pluralize<'a>(count: u64, singular: &'a str, plural: &'a str) -> &'a str {
    if count == 1 { singular } else { plural }
}

fn pad_watch_cell(text: &str, width: usize) -> String {
    format!("{text:<width$}")
}

fn pad_watch_cell_right(text: &str, width: usize) -> String {
    format!("{text:>width$}")
}

fn truncate_for_watch(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return String::from(text);
    }
    let keep = max_chars.saturating_sub(1);
    let prefix = text.chars().take(keep).collect::<String>();
    format!("{prefix}…")
}

fn replica_watch_note(replica: &ReplicaWatchSnapshot) -> String {
    if let Some(error) = &replica.error {
        return error.clone();
    }
    let Some(status) = &replica.status else {
        return String::from("☠ control link unavailable");
    };
    if status.state == ReplicaRuntimeState::Faulted {
        return status.fault_reason.clone().map_or_else(
            || String::from("💥 replica faulted"),
            |reason| format!("💥 {reason}"),
        );
    }
    if replica.accepting_writes == Some(false) {
        return String::from("🧯 writes paused");
    }
    if let Some(backlog) = replica.expiration_backlog.filter(|value| *value > 0) {
        return format!(
            "⏰ {} due expiration {}",
            compact_counter(backlog),
            pluralize(backlog, "tick", "ticks")
        );
    }
    if let Some(queue_depth) = replica.queue_depth.filter(|value| *value > 0) {
        return format!("📥 {} queued ops", compact_counter(queue_depth));
    }
    match status.role {
        ReplicaRole::Primary => String::from("👑 serving traffic"),
        ReplicaRole::Backup => String::from("🛡 hot standby"),
        ReplicaRole::ViewUncertain => String::from("🫥 view uncertain"),
        ReplicaRole::Recovering => String::from("🩹 recovering"),
        ReplicaRole::Faulted => String::from("💥 faulted"),
    }
}

fn replica_role_label(role: ReplicaRole) -> &'static str {
    match role {
        ReplicaRole::Primary => "primary",
        ReplicaRole::Backup => "backup",
        ReplicaRole::ViewUncertain => "uncertain",
        ReplicaRole::Recovering => "recover",
        ReplicaRole::Faulted => "faulted",
    }
}

fn role_label(replica: &ReplicaWatchSnapshot) -> String {
    match replica.status.as_ref().map(|status| status.role) {
        Some(role) => String::from(replica_role_label(role)),
        None => String::from("down"),
    }
}

fn style_health_cell(color: bool, health: &str, cell: &str) -> String {
    let codes = match health {
        "up" => &[ANSI_GREEN][..],
        "faulted" => &[ANSI_RED][..],
        _ => &[ANSI_ORANGE][..],
    };
    watch_style(color, codes, cell)
}

fn style_role_cell(color: bool, replica: &ReplicaWatchSnapshot, cell: &str) -> String {
    let codes = match replica.status.as_ref().map(|status| status.role) {
        Some(ReplicaRole::Primary) => &[ANSI_BOLD, ANSI_YELLOW][..],
        Some(ReplicaRole::Backup) => &[ANSI_CYAN][..],
        Some(ReplicaRole::ViewUncertain) => &[ANSI_ORANGE][..],
        Some(ReplicaRole::Recovering) => &[ANSI_MAGENTA][..],
        Some(ReplicaRole::Faulted) => &[ANSI_RED][..],
        None => &[ANSI_DIM][..],
    };
    watch_style(color, codes, cell)
}

fn style_writes_cell(color: bool, writes: &str, cell: &str) -> String {
    let codes = match writes {
        "on" => &[ANSI_GREEN][..],
        "off" => &[ANSI_YELLOW][..],
        _ => &[ANSI_DIM][..],
    };
    watch_style(color, codes, cell)
}

fn style_backlog_cell(color: bool, backlog: Option<u64>, cell: &str) -> String {
    let codes = match backlog {
        Some(0) => &[ANSI_GREEN][..],
        Some(_) => &[ANSI_ORANGE][..],
        None => &[ANSI_DIM][..],
    };
    watch_style(color, codes, cell)
}

fn style_note_cell(color: bool, replica: &ReplicaWatchSnapshot, note: &str) -> String {
    if replica.error.is_some() {
        return watch_style(color, &[ANSI_RED], note);
    }
    if replica
        .status
        .as_ref()
        .is_some_and(|status| status.role == ReplicaRole::Primary)
    {
        return watch_style(color, &[ANSI_BOLD, ANSI_YELLOW], note);
    }
    watch_style(color, &[ANSI_DIM], note)
}

fn event_icon(detail: &str) -> &'static str {
    if detail.starts_with("run initialized") {
        "🌱"
    } else if detail.starts_with("verifying external surface") {
        "🛰"
    } else if detail.starts_with("executing Jepsen scenario") {
        "⚙"
    } else if detail.starts_with("history sequence=") {
        "📜"
    } else if detail.starts_with("analyzing Jepsen history") {
        "🔎"
    } else if detail.starts_with("collecting logs and artifacts") {
        "📦"
    } else if detail.starts_with("run completed") {
        "🏁"
    } else if detail.starts_with("error:") {
        "💥"
    } else {
        "•"
    }
}

fn style_event_detail(color: bool, detail: &str) -> String {
    if detail.starts_with("error:") {
        return watch_style(color, &[ANSI_RED], detail);
    }
    if detail.starts_with("run completed") {
        return watch_style(color, &[ANSI_GREEN], detail);
    }
    if detail.starts_with("history sequence=") {
        return watch_style(color, &[ANSI_CYAN], detail);
    }
    watch_style(color, &[ANSI_WHITE], detail)
}

enum RemoteApiOutcome {
    Api(ApiResponse),
    Text(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResourceReadObservation {
    Available,
    Held {
        state: allocdb_core::ResourceState,
        current_reservation_id: Option<ReservationId>,
        version: u64,
    },
    NotFound,
    FenceNotApplied,
    EngineHalted,
    NotPrimary,
}

struct ReserveCommit {
    applied_lsn: Lsn,
    reservation_id: ReservationId,
}

#[derive(Clone, Copy)]
struct ReserveEventSpec {
    operation_id: OperationId,
    resource_id: ResourceId,
    holder_id: HolderId,
    request_slot: Slot,
    ttl_slots: u64,
}

struct StagedReplicaWorkspace {
    root: PathBuf,
    replica_id: ReplicaId,
    paths: ReplicaPaths,
}

impl StagedReplicaWorkspace {
    fn new<T: ExternalTestbed>(layout: &T, replica_id: ReplicaId) -> Result<Self, String> {
        let root = temp_staging_dir(&format!(
            "staged-{}-{}-replica-{}",
            layout.backend_name(),
            sanitized_workspace_label(layout.workspace_root()),
            replica_id.get()
        ))?;
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

    fn from_export<T: ExternalTestbed>(layout: &T, replica_id: ReplicaId) -> Result<Self, String> {
        let staged = Self::new(layout, replica_id)?;
        let archive = run_remote_control_command(
            layout,
            &[String::from("export-replica"), replica_id.get().to_string()],
            None,
        )?;
        run_local_tar_extract(&staged.root, &archive)?;
        Ok(staged)
    }

    fn import_to_remote<T: ExternalTestbed>(&self, layout: &T) -> Result<(), String> {
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
    let pid = std::process::id();
    let sequence = NEXT_STAGING_WORKSPACE_ID.fetch_add(1, Ordering::Relaxed);
    let root =
        std::env::temp_dir().join(format!("allocdb-{prefix}-pid{pid}-seq{sequence}-{millis}"));
    fs::create_dir_all(&root).map_err(|error| {
        format!(
            "failed to create temp staging dir {}: {error}",
            root.display()
        )
    })?;
    Ok(root)
}

fn run_remote_control_command<T: ExternalTestbed>(
    layout: &T,
    args: &[String],
    stdin_bytes: Option<&[u8]>,
) -> Result<Vec<u8>, String> {
    let remote_command = if args.is_empty() {
        format!("sudo {REMOTE_CONTROL_SCRIPT_PATH}")
    } else {
        format!("sudo {REMOTE_CONTROL_SCRIPT_PATH} {}", args.join(" "))
    };
    layout.run_remote_host_command(&remote_command, stdin_bytes)
}

fn disable_local_tar_copyfile_metadata(command: &mut Command) {
    #[cfg(target_os = "macos")]
    {
        command.env("COPYFILE_DISABLE", "1");
    }
}

fn run_local_tar_extract(destination_root: &Path, archive: &[u8]) -> Result<(), String> {
    let mut command = Command::new("tar");
    disable_local_tar_copyfile_metadata(&mut command);
    let mut child = command
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
    let mut command = Command::new("tar");
    disable_local_tar_copyfile_metadata(&mut command);
    let output = command
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

fn sanitized_workspace_label(path: &Path) -> String {
    sanitize_run_id(
        path.file_name()
            .and_then(|value| value.to_str())
            .filter(|value| !value.is_empty())
            .unwrap_or("workspace"),
    )
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
    match fs::copy(source, destination) {
        Ok(_) => Ok(()),
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
        Err(error) => Err(format!(
            "failed to copy {} -> {}: {error}",
            source.display(),
            destination.display()
        )),
    }
}

fn execute_control_run<T: ExternalTestbed>(
    layout: &T,
    run_spec: &JepsenRunSpec,
    context: &RunExecutionContext,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let primary = primary_replica(layout)?;
    let backup = first_backup_replica(layout, Some(primary.replica_id))?;
    let base_id = unique_probe_resource_id();
    match run_spec.workload {
        JepsenWorkloadFamily::ReservationContention => {
            run_reservation_contention(layout, &primary, base_id, context)
        }
        JepsenWorkloadFamily::AmbiguousWriteRetry => {
            run_ambiguous_write_retry(layout, &primary, base_id, context)
        }
        JepsenWorkloadFamily::FailoverReadFences => {
            run_failover_read_fences(layout, &primary, &backup, base_id, context)
        }
        JepsenWorkloadFamily::ExpirationAndRecovery => {
            run_expiration_and_recovery(layout, &primary, base_id, context)
        }
    }
}

fn execute_crash_restart_run<T: ExternalTestbed>(
    layout: &T,
    run_spec: &JepsenRunSpec,
    context: &RunExecutionContext,
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
            context,
        ),
        JepsenWorkloadFamily::AmbiguousWriteRetry => run_crash_restart_ambiguous_retry(
            layout,
            &primary,
            &failover_target,
            &supporting_backup,
            base_id,
            context,
        ),
        JepsenWorkloadFamily::FailoverReadFences => run_crash_restart_failover_reads(
            layout,
            &primary,
            &failover_target,
            &supporting_backup,
            base_id,
            context,
        ),
        JepsenWorkloadFamily::ExpirationAndRecovery => run_crash_restart_expiration_recovery(
            layout,
            &primary,
            &failover_target,
            &supporting_backup,
            base_id,
            context,
        ),
    }
}

fn execute_partition_heal_run<T: ExternalTestbed>(
    layout: &T,
    run_spec: &JepsenRunSpec,
    context: &RunExecutionContext,
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
            context,
        ),
        JepsenWorkloadFamily::AmbiguousWriteRetry => run_partition_heal_ambiguous_retry(
            layout,
            &primary,
            &failover_target,
            &supporting_backup,
            base_id,
            context,
        ),
        JepsenWorkloadFamily::FailoverReadFences => run_partition_heal_failover_reads(
            layout,
            &primary,
            &failover_target,
            &supporting_backup,
            base_id,
            context,
        ),
        JepsenWorkloadFamily::ExpirationAndRecovery => run_partition_heal_expiration_recovery(
            layout,
            &primary,
            &failover_target,
            &supporting_backup,
            base_id,
            context,
        ),
    }
}

fn execute_mixed_failover_run<T: ExternalTestbed>(
    layout: &T,
    run_spec: &JepsenRunSpec,
    context: &RunExecutionContext,
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
            context,
        ),
        JepsenWorkloadFamily::FailoverReadFences => run_mixed_failover_reads(
            layout,
            &primary,
            &failover_target,
            &supporting_backup,
            base_id,
            context,
        ),
        JepsenWorkloadFamily::ExpirationAndRecovery => run_mixed_failover_expiration_recovery(
            layout,
            &primary,
            &failover_target,
            &supporting_backup,
            base_id,
            context,
        ),
    }
}

fn request_remote_control_status<T: ExternalTestbed>(
    layout: &T,
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

fn runtime_replica_probes<T: ExternalTestbed>(layout: &T) -> Vec<RuntimeReplicaProbe> {
    layout
        .replica_layout()
        .replicas
        .iter()
        .cloned()
        .map(|mut replica| {
            let status = request_remote_control_status(layout, &replica);
            if let Ok(status) = status.as_ref() {
                replica.role = status.role;
            }
            RuntimeReplicaProbe { replica, status }
        })
        .collect()
}

fn runtime_probe_is_active(probe: &RuntimeReplicaProbe) -> bool {
    matches!(
        probe.status.as_ref(),
        Ok(status) if status.state == ReplicaRuntimeState::Active
    )
}

fn summarize_runtime_probes(probes: &[RuntimeReplicaProbe]) -> RuntimeReplicaTopology {
    let mut topology = RuntimeReplicaTopology {
        active: 0,
        primaries: 0,
        backups: 0,
    };
    for probe in probes {
        let Ok(status) = probe.status.as_ref() else {
            continue;
        };
        if status.state != ReplicaRuntimeState::Active {
            continue;
        }
        topology.active += 1;
        match status.role {
            ReplicaRole::Primary => topology.primaries += 1,
            ReplicaRole::Backup => topology.backups += 1,
            ReplicaRole::Faulted | ReplicaRole::ViewUncertain | ReplicaRole::Recovering => {}
        }
    }
    topology
}

fn live_runtime_replica_matching<F>(
    probes: &[RuntimeReplicaProbe],
    mut predicate: F,
) -> Option<LocalClusterReplicaConfig>
where
    F: FnMut(&LocalClusterReplicaConfig) -> bool,
{
    probes
        .iter()
        .filter(|probe| runtime_probe_is_active(probe))
        .map(|probe| probe.replica.clone())
        .find(|replica| predicate(replica))
}

fn render_runtime_probe_summary(probes: &[RuntimeReplicaProbe]) -> String {
    probes
        .iter()
        .map(|probe| match &probe.status {
            Ok(status) if status.state == ReplicaRuntimeState::Active => format!(
                "{}:{}@view{}",
                probe.replica.replica_id.get(),
                replica_role_label(status.role),
                status.current_view
            ),
            Ok(status) => format!(
                "{}:faulted({})",
                probe.replica.replica_id.get(),
                status
                    .fault_reason
                    .as_deref()
                    .unwrap_or("replica faulted")
                    .lines()
                    .next()
                    .unwrap_or("replica faulted")
                    .trim()
            ),
            Err(error) => format!(
                "{}:down({})",
                probe.replica.replica_id.get(),
                error.lines().next().unwrap_or(error).trim()
            ),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn wait_for_runtime_replica_role<T: ExternalTestbed>(
    layout: &T,
    replica_id: ReplicaId,
    role: ReplicaRole,
) -> Result<LocalClusterReplicaConfig, String> {
    wait_for_live_runtime_replica(
        layout,
        &format!(
            "replica {} as {}",
            replica_id.get(),
            replica_role_label(role)
        ),
        |replica| replica.replica_id == replica_id && replica.role == role,
    )
}

fn ensure_runtime_cluster_ready<T: ExternalTestbed>(layout: &T) -> Result<(), String> {
    for replica in &layout.replica_layout().replicas {
        let _ = heal_replica(layout, replica.replica_id);
    }

    let expected_replica_count = layout.replica_layout().replicas.len();
    let started_at = Instant::now();
    loop {
        let probes = runtime_replica_probes(layout);
        let topology = summarize_runtime_probes(&probes);
        if topology.active == expected_replica_count
            && topology.primaries == 1
            && topology.backups == expected_replica_count.saturating_sub(1)
        {
            return Ok(());
        }

        for probe in &probes {
            if !runtime_probe_is_active(probe) {
                let _ = restart_replica(layout, probe.replica.replica_id);
            }
        }

        if started_at.elapsed() >= EXTERNAL_RUNTIME_DISCOVERY_TIMEOUT {
            return Err(format!(
                "{} cluster did not converge to a healthy {}-replica topology within {}s; last_probe={}",
                layout.backend_name(),
                expected_replica_count,
                EXTERNAL_RUNTIME_DISCOVERY_TIMEOUT.as_secs(),
                render_runtime_probe_summary(&probes)
            ));
        }
        thread::sleep(EXTERNAL_RUNTIME_DISCOVERY_RETRY_DELAY);
    }
}

fn wait_for_live_runtime_replica<T, F>(
    layout: &T,
    description: &str,
    predicate: F,
) -> Result<LocalClusterReplicaConfig, String>
where
    T: ExternalTestbed,
    F: Fn(&LocalClusterReplicaConfig) -> bool,
{
    let started_at = Instant::now();
    loop {
        let probes = runtime_replica_probes(layout);
        if let Some(replica) = live_runtime_replica_matching(&probes, &predicate) {
            return Ok(replica);
        }
        if started_at.elapsed() >= EXTERNAL_RUNTIME_DISCOVERY_TIMEOUT {
            return Err(format!(
                "{} cluster did not surface {description} within {}s; last_probe={}",
                layout.backend_name(),
                EXTERNAL_RUNTIME_DISCOVERY_TIMEOUT.as_secs(),
                render_runtime_probe_summary(&probes)
            ));
        }
        thread::sleep(EXTERNAL_RUNTIME_DISCOVERY_RETRY_DELAY);
    }
}

fn primary_replica<T: ExternalTestbed>(layout: &T) -> Result<LocalClusterReplicaConfig, String> {
    wait_for_live_runtime_replica(layout, "a live primary", |replica| {
        replica.role == ReplicaRole::Primary
    })
}

fn first_backup_replica<T: ExternalTestbed>(
    layout: &T,
    exclude: Option<ReplicaId>,
) -> Result<LocalClusterReplicaConfig, String> {
    wait_for_live_runtime_replica(layout, "a live backup", |replica| {
        replica.role == ReplicaRole::Backup
            && exclude.is_none_or(|excluded| replica.replica_id != excluded)
    })
}

fn runtime_replica_by_id<T: ExternalTestbed>(
    layout: &T,
    replica_id: ReplicaId,
) -> Result<LocalClusterReplicaConfig, String> {
    wait_for_live_runtime_replica(
        layout,
        &format!("replica {}", replica_id.get()),
        |replica| replica.replica_id == replica_id,
    )
}

fn maybe_crash_replica<T: ExternalTestbed>(
    layout: &T,
    replica_id: ReplicaId,
) -> Result<(), String> {
    eprintln!(
        "backend={} event=crash_replica workspace={} replica={}",
        layout.backend_name(),
        layout.workspace_root().display(),
        replica_id.get()
    );
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

fn restart_replica<T: ExternalTestbed>(layout: &T, replica_id: ReplicaId) -> Result<(), String> {
    eprintln!(
        "backend={} event=restart_replica workspace={} replica={}",
        layout.backend_name(),
        layout.workspace_root().display(),
        replica_id.get()
    );
    let _ = run_remote_control_command(
        layout,
        &[String::from("restart"), replica_id.get().to_string()],
        None,
    )?;
    Ok(())
}

fn isolate_replica<T: ExternalTestbed>(layout: &T, replica_id: ReplicaId) -> Result<(), String> {
    eprintln!(
        "backend={} event=isolate_replica workspace={} replica={}",
        layout.backend_name(),
        layout.workspace_root().display(),
        replica_id.get()
    );
    let _ = run_remote_control_command(
        layout,
        &[String::from("isolate"), replica_id.get().to_string()],
        None,
    )?;
    Ok(())
}

fn heal_replica<T: ExternalTestbed>(layout: &T, replica_id: ReplicaId) -> Result<(), String> {
    eprintln!(
        "backend={} event=heal_replica workspace={} replica={}",
        layout.backend_name(),
        layout.workspace_root().display(),
        replica_id.get()
    );
    let _ = run_remote_control_command(
        layout,
        &[String::from("heal"), replica_id.get().to_string()],
        None,
    )?;
    Ok(())
}

fn staged_replica_summary<T: ExternalTestbed>(
    layout: &T,
    staged: &StagedReplicaWorkspace,
) -> Result<(u64, Option<Lsn>, Option<Lsn>), String> {
    let node = ReplicaNode::recover(
        layout.replica_layout().core_config.clone(),
        layout.replica_layout().engine_config,
        ReplicaIdentity {
            replica_id: staged.replica_id,
            shard_id: layout.replica_layout().core_config.shard_id,
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

fn format_staged_summary(summary: (u64, Option<Lsn>, Option<Lsn>)) -> String {
    format!(
        "view={} commit_lsn={} highest_prepared_lsn={}",
        summary.0,
        summary
            .1
            .map_or_else(|| String::from("none"), |lsn| lsn.get().to_string()),
        summary
            .2
            .map_or_else(|| String::from("none"), |lsn| lsn.get().to_string())
    )
}

fn format_optional_lsn(value: Option<Lsn>) -> String {
    value.map_or_else(|| String::from("none"), |lsn| lsn.get().to_string())
}

fn load_staged_source_metadata<T: ExternalTestbed>(
    layout: &T,
    source: &StagedReplicaWorkspace,
) -> Result<(ReplicaMetadata, PathBuf), String> {
    let source_node = ReplicaNode::recover(
        layout.replica_layout().core_config.clone(),
        layout.replica_layout().engine_config,
        ReplicaIdentity {
            replica_id: source.replica_id,
            shard_id: layout.replica_layout().core_config.shard_id,
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
    Ok((source_metadata, source_prepare_log_path))
}

fn log_rewrite_replica_begin<T: ExternalTestbed>(
    layout: &T,
    source: &StagedReplicaWorkspace,
    target: &StagedReplicaWorkspace,
    source_metadata: ReplicaMetadata,
    target_commit_lsn: Option<Lsn>,
    new_view: u64,
    new_role: ReplicaRole,
) {
    eprintln!(
        "backend={} event=rewrite_replica_from_source workspace={} source_replica={} target_replica={} source_role={:?} source_view={} source_commit_lsn={} source_snapshot_lsn={} target_commit_lsn={} new_view={} new_role={:?}",
        layout.backend_name(),
        layout.workspace_root().display(),
        source.replica_id.get(),
        target.replica_id.get(),
        source_metadata.role,
        source_metadata.current_view,
        format_optional_lsn(source_metadata.commit_lsn),
        format_optional_lsn(source_metadata.active_snapshot_lsn),
        format_optional_lsn(target_commit_lsn),
        new_view,
        new_role
    );
}

fn log_rewrite_replica_complete<T: ExternalTestbed>(
    layout: &T,
    target: &StagedReplicaWorkspace,
    target_node: &ReplicaNode,
) {
    eprintln!(
        "backend={} event=rewrite_replica_complete workspace={} target_replica={} final_role={:?} final_view={} final_commit_lsn={} final_snapshot_lsn={} final_highest_prepared_lsn={}",
        layout.backend_name(),
        layout.workspace_root().display(),
        target.replica_id.get(),
        target_node.metadata().role,
        target_node.metadata().current_view,
        format_optional_lsn(target_node.metadata().commit_lsn),
        format_optional_lsn(target_node.metadata().active_snapshot_lsn),
        format_optional_lsn(target_node.highest_prepared_lsn())
    );
}

fn rewrite_replica_from_source<T: ExternalTestbed>(
    layout: &T,
    source: &StagedReplicaWorkspace,
    target: &StagedReplicaWorkspace,
    target_commit_lsn: Option<Lsn>,
    new_view: u64,
    new_role: ReplicaRole,
) -> Result<(), String> {
    let (source_metadata, source_prepare_log_path) = load_staged_source_metadata(layout, source)?;
    log_rewrite_replica_begin(
        layout,
        source,
        target,
        source_metadata,
        target_commit_lsn,
        new_view,
        new_role,
    );

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
        shard_id: layout.replica_layout().core_config.shard_id,
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
        layout.replica_layout().core_config.clone(),
        layout.replica_layout().engine_config,
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
    log_rewrite_replica_complete(layout, target, &target_node);
    Ok(())
}

fn perform_failover<T: ExternalTestbed>(
    layout: &T,
    old_primary: ReplicaId,
    new_primary: ReplicaId,
    supporting_backup: ReplicaId,
) -> Result<(), String> {
    eprintln!(
        "backend={} event=perform_failover_begin workspace={} old_primary={} new_primary={} supporting_backup={}",
        layout.backend_name(),
        layout.workspace_root().display(),
        old_primary.get(),
        new_primary.get(),
        supporting_backup.get()
    );
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
    eprintln!(
        "backend={} event=perform_failover_plan workspace={} old_primary={} new_primary={} supporting_backup={} new_primary_summary=\"{}\" supporting_summary=\"{}\" chosen_source={} target_commit_lsn={} new_view={}",
        layout.backend_name(),
        layout.workspace_root().display(),
        old_primary.get(),
        new_primary.get(),
        supporting_backup.get(),
        format_staged_summary(new_primary_summary),
        format_staged_summary(supporting_summary),
        source.replica_id.get(),
        target_commit_lsn.map_or_else(|| String::from("none"), |lsn| lsn.get().to_string()),
        new_view
    );

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
    wait_for_runtime_replica_role(layout, new_primary, ReplicaRole::Primary)?;
    wait_for_runtime_replica_role(layout, supporting_backup, ReplicaRole::Backup)?;
    eprintln!(
        "backend={} event=perform_failover_complete workspace={} old_primary={} new_primary={} supporting_backup={} new_view={} target_commit_lsn={}",
        layout.backend_name(),
        layout.workspace_root().display(),
        old_primary.get(),
        new_primary.get(),
        supporting_backup.get(),
        new_view,
        target_commit_lsn.map_or_else(|| String::from("none"), |lsn| lsn.get().to_string())
    );
    Ok(())
}

fn perform_rejoin<T: ExternalTestbed>(
    layout: &T,
    current_primary: ReplicaId,
    target_replica: ReplicaId,
) -> Result<(), String> {
    eprintln!(
        "backend={} event=perform_rejoin_begin workspace={} current_primary={} target_replica={}",
        layout.backend_name(),
        layout.workspace_root().display(),
        current_primary.get(),
        target_replica.get()
    );
    maybe_crash_replica(layout, target_replica)?;
    let source_stage = StagedReplicaWorkspace::from_export(layout, current_primary)?;
    let source_summary = staged_replica_summary(layout, &source_stage)?;
    let target_stage = StagedReplicaWorkspace::new(layout, target_replica)?;
    eprintln!(
        "backend={} event=perform_rejoin_plan workspace={} current_primary={} target_replica={} source_summary=\"{}\"",
        layout.backend_name(),
        layout.workspace_root().display(),
        current_primary.get(),
        target_replica.get(),
        format_staged_summary(source_summary)
    );
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
    wait_for_runtime_replica_role(layout, target_replica, ReplicaRole::Backup)?;
    eprintln!(
        "backend={} event=perform_rejoin_complete workspace={} current_primary={} target_replica={} target_view={} target_commit_lsn={}",
        layout.backend_name(),
        layout.workspace_root().display(),
        current_primary.get(),
        target_replica.get(),
        source_summary.0,
        source_summary
            .1
            .map_or_else(|| String::from("none"), |lsn| lsn.get().to_string())
    );
    Ok(())
}

fn run_crash_restart_reservation_contention<T: ExternalTestbed>(
    layout: &T,
    primary: &LocalClusterReplicaConfig,
    failover_target: &LocalClusterReplicaConfig,
    supporting_backup: &LocalClusterReplicaConfig,
    base_id: u128,
    context: &RunExecutionContext,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id);
    create_qemu_resource(
        layout,
        primary,
        context,
        resource_id,
        OperationId(base_id + 100),
    )?;

    maybe_crash_replica(layout, primary.replica_id)?;
    perform_failover(
        layout,
        primary.replica_id,
        failover_target.replica_id,
        supporting_backup.replica_id,
    )?;

    let current_primary = primary_replica(layout)?;
    let mut history = HistoryBuilder::new(
        Some(context.tracker.clone()),
        context.history_sequence_start,
    );
    let first = reserve_event(
        layout,
        &current_primary,
        context,
        ReserveEventSpec {
            operation_id: OperationId(base_id + 1),
            resource_id,
            holder_id: HolderId(101),
            request_slot: Slot(10),
            ttl_slots: 5,
        },
    )?;
    history.push(primary_process_name(&current_primary), first.0, first.1);

    perform_rejoin(layout, current_primary.replica_id, primary.replica_id)?;

    let second = reserve_event(
        layout,
        &current_primary,
        context,
        ReserveEventSpec {
            operation_id: OperationId(base_id + 2),
            resource_id,
            holder_id: HolderId(202),
            request_slot: Slot(11),
            ttl_slots: 5,
        },
    )?;
    history.push(primary_process_name(&current_primary), second.0, second.1);
    Ok(history.finish())
}

fn run_crash_restart_ambiguous_retry<T: ExternalTestbed>(
    layout: &T,
    primary: &LocalClusterReplicaConfig,
    failover_target: &LocalClusterReplicaConfig,
    supporting_backup: &LocalClusterReplicaConfig,
    base_id: u128,
    context: &RunExecutionContext,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id + 1);
    let operation_id = OperationId(base_id + 10);
    create_qemu_resource(
        layout,
        primary,
        context,
        resource_id,
        OperationId(base_id + 101),
    )?;

    maybe_crash_replica(layout, failover_target.replica_id)?;
    maybe_crash_replica(layout, supporting_backup.replica_id)?;
    let mut history = HistoryBuilder::new(
        Some(context.tracker.clone()),
        context.history_sequence_start,
    );
    let first = reserve_event(
        layout,
        primary,
        context,
        ReserveEventSpec {
            operation_id,
            resource_id,
            holder_id: HolderId(303),
            request_slot: Slot(20),
            ttl_slots: 4,
        },
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
        context,
        ReserveEventSpec {
            operation_id,
            resource_id,
            holder_id: HolderId(303),
            request_slot: Slot(20),
            ttl_slots: 4,
        },
    )?;
    history.push(primary_process_name(&current_primary), retry.0, retry.1);

    perform_rejoin(layout, current_primary.replica_id, primary.replica_id)?;
    Ok(history.finish())
}

fn run_crash_restart_failover_reads<T: ExternalTestbed>(
    layout: &T,
    primary: &LocalClusterReplicaConfig,
    failover_target: &LocalClusterReplicaConfig,
    supporting_backup: &LocalClusterReplicaConfig,
    base_id: u128,
    context: &RunExecutionContext,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id + 2);
    create_qemu_resource(
        layout,
        primary,
        context,
        resource_id,
        OperationId(base_id + 102),
    )?;

    let mut history = HistoryBuilder::new(
        Some(context.tracker.clone()),
        context.history_sequence_start,
    );
    let (reserve_operation, reserve_outcome, reserve_commit) = reserve_event(
        layout,
        primary,
        context,
        ReserveEventSpec {
            operation_id: OperationId(base_id + 20),
            resource_id,
            holder_id: HolderId(404),
            request_slot: Slot(30),
            ttl_slots: 6,
        },
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
        context,
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
        context,
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

fn run_crash_restart_expiration_recovery<T: ExternalTestbed>(
    layout: &T,
    primary: &LocalClusterReplicaConfig,
    failover_target: &LocalClusterReplicaConfig,
    supporting_backup: &LocalClusterReplicaConfig,
    base_id: u128,
    context: &RunExecutionContext,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id + 3);
    create_qemu_resource(
        layout,
        primary,
        context,
        resource_id,
        OperationId(base_id + 103),
    )?;

    let mut history = HistoryBuilder::new(
        Some(context.tracker.clone()),
        context.history_sequence_start,
    );
    let (reserve_operation, reserve_outcome, reserve_commit) = reserve_event(
        layout,
        primary,
        context,
        ReserveEventSpec {
            operation_id: OperationId(base_id + 30),
            resource_id,
            holder_id: HolderId(505),
            request_slot: Slot(40),
            ttl_slots: 2,
        },
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
        context,
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
    let settled_tick_lsn = record_resource_available_after_expiration(
        layout,
        &current_primary,
        context,
        &mut history,
        ExpirationDrainPlan {
            resource_id,
            expired: &[JepsenExpiredReservation {
                resource_id,
                holder_id: 505,
                reservation_id: reserve_commit.reservation_id.get(),
                released_lsn: None,
            }],
            required_lsn: tick_lsn,
            operation_id_base: base_id + 32,
            slot_base: 43,
        },
    )?;
    let reservation_read = reservation_read_event(
        layout,
        &current_primary,
        context,
        reserve_commit.reservation_id,
        Slot(42),
        Some(settled_tick_lsn),
    )?;
    history.push(
        primary_process_name(&current_primary),
        reservation_read.0,
        reservation_read.1,
    );

    perform_rejoin(layout, current_primary.replica_id, primary.replica_id)?;
    Ok(history.finish())
}

fn run_partition_heal_reservation_contention<T: ExternalTestbed>(
    layout: &T,
    primary: &LocalClusterReplicaConfig,
    failover_target: &LocalClusterReplicaConfig,
    supporting_backup: &LocalClusterReplicaConfig,
    base_id: u128,
    context: &RunExecutionContext,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id + 10);
    let operation_id = OperationId(base_id + 11);
    create_qemu_resource(
        layout,
        primary,
        context,
        resource_id,
        OperationId(base_id + 110),
    )?;
    isolate_replica(layout, failover_target.replica_id)?;
    isolate_replica(layout, supporting_backup.replica_id)?;

    let mut history = HistoryBuilder::new(
        Some(context.tracker.clone()),
        context.history_sequence_start,
    );
    let first = reserve_event(
        layout,
        primary,
        context,
        ReserveEventSpec {
            operation_id,
            resource_id,
            holder_id: HolderId(601),
            request_slot: Slot(50),
            ttl_slots: 5,
        },
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
        context,
        ReserveEventSpec {
            operation_id,
            resource_id,
            holder_id: HolderId(601),
            request_slot: Slot(50),
            ttl_slots: 5,
        },
    )?;
    history.push(primary_process_name(&current_primary), retry.0, retry.1);
    let second = reserve_event(
        layout,
        &current_primary,
        context,
        ReserveEventSpec {
            operation_id: OperationId(base_id + 12),
            resource_id,
            holder_id: HolderId(602),
            request_slot: Slot(51),
            ttl_slots: 5,
        },
    )?;
    history.push(primary_process_name(&current_primary), second.0, second.1);

    perform_rejoin(layout, current_primary.replica_id, primary.replica_id)?;
    Ok(history.finish())
}

fn run_partition_heal_ambiguous_retry<T: ExternalTestbed>(
    layout: &T,
    primary: &LocalClusterReplicaConfig,
    failover_target: &LocalClusterReplicaConfig,
    supporting_backup: &LocalClusterReplicaConfig,
    base_id: u128,
    context: &RunExecutionContext,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id + 11);
    let operation_id = OperationId(base_id + 20);
    create_qemu_resource(
        layout,
        primary,
        context,
        resource_id,
        OperationId(base_id + 120),
    )?;

    isolate_replica(layout, failover_target.replica_id)?;
    isolate_replica(layout, supporting_backup.replica_id)?;

    let mut history = HistoryBuilder::new(
        Some(context.tracker.clone()),
        context.history_sequence_start,
    );
    let first = reserve_event(
        layout,
        primary,
        context,
        ReserveEventSpec {
            operation_id,
            resource_id,
            holder_id: HolderId(603),
            request_slot: Slot(52),
            ttl_slots: 4,
        },
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
        context,
        ReserveEventSpec {
            operation_id,
            resource_id,
            holder_id: HolderId(603),
            request_slot: Slot(52),
            ttl_slots: 4,
        },
    )?;
    history.push(primary_process_name(&current_primary), retry.0, retry.1);
    perform_rejoin(layout, current_primary.replica_id, primary.replica_id)?;
    Ok(history.finish())
}

fn run_partition_heal_failover_reads<T: ExternalTestbed>(
    layout: &T,
    primary: &LocalClusterReplicaConfig,
    failover_target: &LocalClusterReplicaConfig,
    supporting_backup: &LocalClusterReplicaConfig,
    base_id: u128,
    context: &RunExecutionContext,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id + 12);
    let minority_operation_id = OperationId(base_id + 22);
    create_qemu_resource(
        layout,
        primary,
        context,
        resource_id,
        OperationId(base_id + 130),
    )?;

    let mut history = HistoryBuilder::new(
        Some(context.tracker.clone()),
        context.history_sequence_start,
    );
    let (reserve_operation, reserve_outcome, reserve_commit) = reserve_event(
        layout,
        primary,
        context,
        ReserveEventSpec {
            operation_id: OperationId(base_id + 21),
            resource_id,
            holder_id: HolderId(604),
            request_slot: Slot(53),
            ttl_slots: 6,
        },
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
        context,
        ReserveEventSpec {
            operation_id: minority_operation_id,
            resource_id,
            holder_id: HolderId(605),
            request_slot: Slot(54),
            ttl_slots: 2,
        },
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
        context,
        reserve_commit.reservation_id,
        Slot(54),
        Some(reserve_commit.applied_lsn),
    )?;
    history.push(
        primary_process_name(&current_primary),
        primary_read.0,
        primary_read.1,
    );

    retry_partition_heal_failover_minority_write(
        layout,
        &current_primary,
        context,
        &mut history,
        minority_operation_id,
        resource_id,
    )?;

    perform_rejoin(layout, current_primary.replica_id, primary.replica_id)?;
    let old_primary = runtime_replica_by_id(layout, primary.replica_id)?;
    let stale_read = reservation_read_event(
        layout,
        &old_primary,
        context,
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

fn retry_partition_heal_failover_minority_write<T: ExternalTestbed>(
    layout: &T,
    primary: &LocalClusterReplicaConfig,
    context: &RunExecutionContext,
    history: &mut HistoryBuilder,
    operation_id: OperationId,
    resource_id: ResourceId,
) -> Result<(), String> {
    let retry = reserve_event(
        layout,
        primary,
        context,
        ReserveEventSpec {
            operation_id,
            resource_id,
            holder_id: HolderId(605),
            request_slot: Slot(54),
            ttl_slots: 2,
        },
    )?;
    history.push(primary_process_name(primary), retry.0, retry.1);
    Ok(())
}

fn run_partition_heal_expiration_recovery<T: ExternalTestbed>(
    layout: &T,
    primary: &LocalClusterReplicaConfig,
    failover_target: &LocalClusterReplicaConfig,
    supporting_backup: &LocalClusterReplicaConfig,
    base_id: u128,
    context: &RunExecutionContext,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id + 13);
    create_qemu_resource(
        layout,
        primary,
        context,
        resource_id,
        OperationId(base_id + 140),
    )?;

    let mut history = HistoryBuilder::new(
        Some(context.tracker.clone()),
        context.history_sequence_start,
    );
    let (reserve_operation, reserve_outcome, reserve_commit) = reserve_event(
        layout,
        primary,
        context,
        ReserveEventSpec {
            operation_id: OperationId(base_id + 23),
            resource_id,
            holder_id: HolderId(606),
            request_slot: Slot(55),
            ttl_slots: 2,
        },
    )?;
    let reserve_commit = reserve_commit.ok_or_else(|| {
        String::from("partition-heal expiration_and_recovery expected one committed reserve")
    })?;
    history.push(
        primary_process_name(primary),
        reserve_operation,
        reserve_outcome,
    );
    let expired_reservation = JepsenExpiredReservation {
        resource_id,
        holder_id: 606,
        reservation_id: reserve_commit.reservation_id.get(),
        released_lsn: None,
    };

    isolate_replica(layout, failover_target.replica_id)?;
    isolate_replica(layout, supporting_backup.replica_id)?;
    let failed_tick = tick_expirations_event(
        layout,
        primary,
        context,
        OperationId(base_id + 24),
        Slot(57),
        &[expired_reservation],
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
        context,
        OperationId(base_id + 24),
        Slot(57),
        &[expired_reservation],
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
    let _settled_tick_lsn = record_resource_available_after_expiration(
        layout,
        &current_primary,
        context,
        &mut history,
        ExpirationDrainPlan {
            resource_id,
            expired: &[expired_reservation],
            required_lsn: tick_lsn,
            operation_id_base: base_id + 25,
            slot_base: 58,
        },
    )?;
    perform_rejoin(layout, current_primary.replica_id, primary.replica_id)?;
    Ok(history.finish())
}

fn run_mixed_failover_ambiguous_retry<T: ExternalTestbed>(
    layout: &T,
    primary: &LocalClusterReplicaConfig,
    failover_target: &LocalClusterReplicaConfig,
    supporting_backup: &LocalClusterReplicaConfig,
    base_id: u128,
    context: &RunExecutionContext,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id + 20);
    let operation_id = OperationId(base_id + 30);
    create_qemu_resource(
        layout,
        primary,
        context,
        resource_id,
        OperationId(base_id + 150),
    )?;

    maybe_crash_replica(layout, failover_target.replica_id)?;
    isolate_replica(layout, supporting_backup.replica_id)?;

    let mut history = HistoryBuilder::new(
        Some(context.tracker.clone()),
        context.history_sequence_start,
    );
    let first = reserve_event(
        layout,
        primary,
        context,
        ReserveEventSpec {
            operation_id,
            resource_id,
            holder_id: HolderId(701),
            request_slot: Slot(60),
            ttl_slots: 4,
        },
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
        context,
        ReserveEventSpec {
            operation_id,
            resource_id,
            holder_id: HolderId(701),
            request_slot: Slot(60),
            ttl_slots: 4,
        },
    )?;
    history.push(primary_process_name(&current_primary), retry.0, retry.1);
    perform_rejoin(layout, current_primary.replica_id, primary.replica_id)?;
    Ok(history.finish())
}

fn run_mixed_failover_reads<T: ExternalTestbed>(
    layout: &T,
    primary: &LocalClusterReplicaConfig,
    failover_target: &LocalClusterReplicaConfig,
    supporting_backup: &LocalClusterReplicaConfig,
    base_id: u128,
    context: &RunExecutionContext,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id + 21);
    create_qemu_resource(
        layout,
        primary,
        context,
        resource_id,
        OperationId(base_id + 160),
    )?;

    let mut history = HistoryBuilder::new(
        Some(context.tracker.clone()),
        context.history_sequence_start,
    );
    let (reserve_operation, reserve_outcome, reserve_commit) = reserve_event(
        layout,
        primary,
        context,
        ReserveEventSpec {
            operation_id: OperationId(base_id + 31),
            resource_id,
            holder_id: HolderId(702),
            request_slot: Slot(61),
            ttl_slots: 6,
        },
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
        context,
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
        context,
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

fn run_mixed_failover_expiration_recovery<T: ExternalTestbed>(
    layout: &T,
    primary: &LocalClusterReplicaConfig,
    failover_target: &LocalClusterReplicaConfig,
    supporting_backup: &LocalClusterReplicaConfig,
    base_id: u128,
    context: &RunExecutionContext,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id + 22);
    create_qemu_resource(
        layout,
        primary,
        context,
        resource_id,
        OperationId(base_id + 170),
    )?;

    let mut history = HistoryBuilder::new(
        Some(context.tracker.clone()),
        context.history_sequence_start,
    );
    let (reserve_operation, reserve_outcome, reserve_commit) = reserve_event(
        layout,
        primary,
        context,
        ReserveEventSpec {
            operation_id: OperationId(base_id + 32),
            resource_id,
            holder_id: HolderId(703),
            request_slot: Slot(62),
            ttl_slots: 2,
        },
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
        context,
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
    let _settled_tick_lsn = record_resource_available_after_expiration(
        layout,
        &current_primary,
        context,
        &mut history,
        ExpirationDrainPlan {
            resource_id,
            expired: &[JepsenExpiredReservation {
                resource_id,
                holder_id: 703,
                reservation_id: reserve_commit.reservation_id.get(),
                released_lsn: None,
            }],
            required_lsn: tick_lsn,
            operation_id_base: base_id + 34,
            slot_base: 65,
        },
    )?;
    perform_rejoin(layout, current_primary.replica_id, primary.replica_id)?;
    Ok(history.finish())
}

fn run_reservation_contention<T: ExternalTestbed>(
    layout: &T,
    primary: &LocalClusterReplicaConfig,
    base_id: u128,
    context: &RunExecutionContext,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id);
    create_qemu_resource(
        layout,
        primary,
        context,
        resource_id,
        OperationId(base_id + 100),
    )?;

    let mut history = HistoryBuilder::new(
        Some(context.tracker.clone()),
        context.history_sequence_start,
    );
    let first = reserve_event(
        layout,
        primary,
        context,
        ReserveEventSpec {
            operation_id: OperationId(base_id + 1),
            resource_id,
            holder_id: HolderId(101),
            request_slot: Slot(10),
            ttl_slots: 5,
        },
    )?;
    history.push(primary_process_name(primary), first.0, first.1);

    let second = reserve_event(
        layout,
        primary,
        context,
        ReserveEventSpec {
            operation_id: OperationId(base_id + 2),
            resource_id,
            holder_id: HolderId(202),
            request_slot: Slot(11),
            ttl_slots: 5,
        },
    )?;
    history.push(primary_process_name(primary), second.0, second.1);
    Ok(history.finish())
}

fn run_ambiguous_write_retry<T: ExternalTestbed>(
    layout: &T,
    primary: &LocalClusterReplicaConfig,
    base_id: u128,
    context: &RunExecutionContext,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id + 1);
    let operation_id = OperationId(base_id + 10);
    create_qemu_resource(
        layout,
        primary,
        context,
        resource_id,
        OperationId(base_id + 101),
    )?;

    let mut history = HistoryBuilder::new(
        Some(context.tracker.clone()),
        context.history_sequence_start,
    );
    let first = reserve_event(
        layout,
        primary,
        context,
        ReserveEventSpec {
            operation_id,
            resource_id,
            holder_id: HolderId(303),
            request_slot: Slot(20),
            ttl_slots: 4,
        },
    )?;
    history.push(primary_process_name(primary), first.0, first.1);

    let retry = reserve_event(
        layout,
        primary,
        context,
        ReserveEventSpec {
            operation_id,
            resource_id,
            holder_id: HolderId(303),
            request_slot: Slot(20),
            ttl_slots: 4,
        },
    )?;
    history.push(primary_process_name(primary), retry.0, retry.1);
    Ok(history.finish())
}

fn run_failover_read_fences<T: ExternalTestbed>(
    layout: &T,
    primary: &LocalClusterReplicaConfig,
    backup: &LocalClusterReplicaConfig,
    base_id: u128,
    context: &RunExecutionContext,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id + 2);
    create_qemu_resource(
        layout,
        primary,
        context,
        resource_id,
        OperationId(base_id + 102),
    )?;

    let mut history = HistoryBuilder::new(
        Some(context.tracker.clone()),
        context.history_sequence_start,
    );
    let (reserve_operation, reserve_outcome, reserve_commit) = reserve_event(
        layout,
        primary,
        context,
        ReserveEventSpec {
            operation_id: OperationId(base_id + 20),
            resource_id,
            holder_id: HolderId(404),
            request_slot: Slot(30),
            ttl_slots: 6,
        },
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
        context,
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
        context,
        reserve_commit.reservation_id,
        Slot(30),
        Some(reserve_commit.applied_lsn),
    )?;
    history.push(backup_process_name(backup), backup_read.0, backup_read.1);
    Ok(history.finish())
}

fn run_expiration_and_recovery<T: ExternalTestbed>(
    layout: &T,
    primary: &LocalClusterReplicaConfig,
    base_id: u128,
    context: &RunExecutionContext,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    let resource_id = ResourceId(base_id + 3);
    create_qemu_resource(
        layout,
        primary,
        context,
        resource_id,
        OperationId(base_id + 103),
    )?;

    let mut history = HistoryBuilder::new(
        Some(context.tracker.clone()),
        context.history_sequence_start,
    );
    let (reserve_operation, reserve_outcome, reserve_commit) = reserve_event(
        layout,
        primary,
        context,
        ReserveEventSpec {
            operation_id: OperationId(base_id + 30),
            resource_id,
            holder_id: HolderId(505),
            request_slot: Slot(40),
            ttl_slots: 2,
        },
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
        context,
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
    let settled_tick_lsn = record_resource_available_after_expiration(
        layout,
        primary,
        context,
        &mut history,
        ExpirationDrainPlan {
            resource_id,
            expired: &[JepsenExpiredReservation {
                resource_id,
                holder_id: 505,
                reservation_id: reserve_commit.reservation_id.get(),
                released_lsn: None,
            }],
            required_lsn: tick_lsn,
            operation_id_base: base_id + 32,
            slot_base: 43,
        },
    )?;

    let reservation_read = reservation_read_event(
        layout,
        primary,
        context,
        reserve_commit.reservation_id,
        Slot(42),
        Some(settled_tick_lsn),
    )?;
    history.push(
        primary_process_name(primary),
        reservation_read.0,
        reservation_read.1,
    );
    Ok(history.finish())
}

fn create_qemu_resource<T: ExternalTestbed>(
    layout: &T,
    primary: &LocalClusterReplicaConfig,
    context: &RunExecutionContext,
    resource_id: ResourceId,
    operation_id: OperationId,
) -> Result<(), String> {
    let request = ApiRequest::Submit(SubmitRequest::from_client_request(
        context.slot(1),
        context.client_request(operation_id, AllocCommand::CreateResource { resource_id }),
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

fn reserve_event<T: ExternalTestbed>(
    layout: &T,
    replica: &LocalClusterReplicaConfig,
    context: &RunExecutionContext,
    spec: ReserveEventSpec,
) -> Result<(JepsenOperation, JepsenEventOutcome, Option<ReserveCommit>), String> {
    let operation = JepsenOperation {
        kind: JepsenOperationKind::Reserve,
        operation_id: Some(spec.operation_id.get()),
        resource_id: Some(spec.resource_id),
        reservation_id: None,
        holder_id: Some(spec.holder_id.get()),
        required_lsn: None,
        request_slot: Some(spec.request_slot),
        ttl_slots: Some(spec.ttl_slots),
    };
    let request = ApiRequest::Submit(SubmitRequest::from_client_request(
        context.slot(spec.request_slot.get()),
        context.client_request(
            spec.operation_id,
            AllocCommand::Reserve {
                resource_id: spec.resource_id,
                holder_id: spec.holder_id,
                ttl_slots: spec.ttl_slots,
            },
        ),
    ));
    match send_replica_api_request(layout, replica, &request)? {
        RemoteApiOutcome::Api(ApiResponse::Submit(response)) => {
            map_reserve_submit_response(operation, spec.resource_id, spec.holder_id, response)
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

fn tick_expirations_event<T: ExternalTestbed>(
    layout: &T,
    replica: &LocalClusterReplicaConfig,
    context: &RunExecutionContext,
    operation_id: OperationId,
    current_wall_clock_slot: Slot,
    expired: &[JepsenExpiredReservation],
) -> Result<(JepsenOperation, JepsenEventOutcome, Option<Lsn>), String> {
    let current_wall_clock_slot = context.slot(current_wall_clock_slot.get());
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

const MAX_EXPIRATION_RECOVERY_DRAIN_TICKS: u64 = 16;

#[derive(Clone, Copy)]
struct ExpirationDrainPlan<'a> {
    resource_id: ResourceId,
    expired: &'a [JepsenExpiredReservation],
    required_lsn: Lsn,
    operation_id_base: u128,
    slot_base: u64,
}

fn classify_resource_read_outcome(
    resource_id: ResourceId,
    outcome: RemoteApiOutcome,
) -> Result<ResourceReadObservation, String> {
    match outcome {
        RemoteApiOutcome::Api(ApiResponse::GetResource(ResourceResponse::Found(resource))) => {
            if matches!(resource.state, allocdb_core::ResourceState::Available) {
                Ok(ResourceReadObservation::Available)
            } else {
                Ok(ResourceReadObservation::Held {
                    state: resource.state,
                    current_reservation_id: resource.current_reservation_id,
                    version: resource.version,
                })
            }
        }
        RemoteApiOutcome::Api(ApiResponse::GetResource(ResourceResponse::NotFound)) => {
            Ok(ResourceReadObservation::NotFound)
        }
        RemoteApiOutcome::Api(ApiResponse::GetResource(ResourceResponse::FenceNotApplied {
            ..
        })) => Ok(ResourceReadObservation::FenceNotApplied),
        RemoteApiOutcome::Api(ApiResponse::GetResource(ResourceResponse::EngineHalted)) => {
            Ok(ResourceReadObservation::EngineHalted)
        }
        RemoteApiOutcome::Api(other) => Err(format!(
            "resource read for {} returned unexpected response {other:?}",
            resource_id.get()
        )),
        RemoteApiOutcome::Text(text) if response_text_is_not_primary(&text) => {
            Ok(ResourceReadObservation::NotPrimary)
        }
        RemoteApiOutcome::Text(text) => Err(format!(
            "resource read for {} returned undecodable response {text}",
            resource_id.get()
        )),
    }
}

fn observe_resource_read<T: ExternalTestbed>(
    layout: &T,
    replica: &LocalClusterReplicaConfig,
    resource_id: ResourceId,
    required_lsn: Option<Lsn>,
) -> Result<ResourceReadObservation, String> {
    let request = ApiRequest::GetResource(ResourceRequest {
        resource_id,
        required_lsn,
    });
    classify_resource_read_outcome(
        resource_id,
        send_replica_api_request(layout, replica, &request)?,
    )
}

fn drain_expiration_until_resource_available<T: ExternalTestbed>(
    layout: &T,
    replica: &LocalClusterReplicaConfig,
    context: &RunExecutionContext,
    history: &mut HistoryBuilder,
    mut plan: ExpirationDrainPlan<'_>,
) -> Result<Lsn, String> {
    for attempt in 0..=MAX_EXPIRATION_RECOVERY_DRAIN_TICKS {
        match observe_resource_read(layout, replica, plan.resource_id, Some(plan.required_lsn))? {
            ResourceReadObservation::Available => return Ok(plan.required_lsn),
            ResourceReadObservation::Held {
                state,
                current_reservation_id,
                version,
            } => {
                if attempt == MAX_EXPIRATION_RECOVERY_DRAIN_TICKS {
                    return Err(format!(
                        "resource {} remained held after {} follow-up expiration ticks: state={state:?} current_reservation_id={:?} version={} required_lsn={}",
                        plan.resource_id.get(),
                        MAX_EXPIRATION_RECOVERY_DRAIN_TICKS,
                        current_reservation_id,
                        version,
                        plan.required_lsn.get(),
                    ));
                }
                let tick = tick_expirations_event(
                    layout,
                    replica,
                    context,
                    OperationId(plan.operation_id_base + u128::from(attempt)),
                    Slot(plan.slot_base + attempt),
                    plan.expired,
                )?;
                history.push(primary_process_name(replica), tick.0, tick.1);
                plan.required_lsn = tick.2.ok_or_else(|| {
                    format!(
                        "expiration recovery expected a committed follow-up tick attempt={}",
                        attempt + 1
                    )
                })?;
            }
            ResourceReadObservation::NotFound => {
                return Err(format!(
                    "resource {} disappeared while waiting for expiration recovery",
                    plan.resource_id.get()
                ));
            }
            ResourceReadObservation::FenceNotApplied => {
                return Err(format!(
                    "resource {} fence was not applied at lsn {} during expiration recovery",
                    plan.resource_id.get(),
                    plan.required_lsn.get()
                ));
            }
            ResourceReadObservation::EngineHalted => {
                return Err(format!(
                    "resource {} read hit halted engine during expiration recovery",
                    plan.resource_id.get()
                ));
            }
            ResourceReadObservation::NotPrimary => {
                return Err(format!(
                    "resource {} read hit non-primary during expiration recovery",
                    plan.resource_id.get()
                ));
            }
        }
    }

    unreachable!("expiration recovery drain loop must return or fail");
}

fn record_resource_available_after_expiration<T: ExternalTestbed>(
    layout: &T,
    replica: &LocalClusterReplicaConfig,
    context: &RunExecutionContext,
    history: &mut HistoryBuilder,
    plan: ExpirationDrainPlan<'_>,
) -> Result<Lsn, String> {
    let settled_tick_lsn =
        drain_expiration_until_resource_available(layout, replica, context, history, plan)?;
    let resource_read =
        resource_available_event(layout, replica, plan.resource_id, Some(settled_tick_lsn))?;
    history.push(
        primary_process_name(replica),
        resource_read.0,
        resource_read.1,
    );
    Ok(settled_tick_lsn)
}

fn resource_available_event<T: ExternalTestbed>(
    layout: &T,
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
    match observe_resource_read(layout, replica, resource_id, required_lsn)? {
        ResourceReadObservation::Available => Ok((
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
        )),
        ResourceReadObservation::NotFound => Ok((
            operation,
            JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::NotFound),
        )),
        ResourceReadObservation::FenceNotApplied => Ok((
            operation,
            JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::FenceNotApplied),
        )),
        ResourceReadObservation::EngineHalted => Ok((
            operation,
            JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::EngineHalted),
        )),
        ResourceReadObservation::NotPrimary => Ok((
            operation,
            JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::NotPrimary),
        )),
        ResourceReadObservation::Held {
            state,
            current_reservation_id,
            version,
        } => Err(format!(
            "resource read for {} returned held state={state:?} current_reservation_id={current_reservation_id:?} version={version}",
            resource_id.get()
        )),
    }
}

fn reservation_read_event<T: ExternalTestbed>(
    layout: &T,
    replica: &LocalClusterReplicaConfig,
    context: &RunExecutionContext,
    reservation_id: ReservationId,
    current_slot: Slot,
    required_lsn: Option<Lsn>,
) -> Result<(JepsenOperation, JepsenEventOutcome), String> {
    let current_slot = context.slot(current_slot.get());
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

fn send_replica_api_request<T: ExternalTestbed>(
    layout: &T,
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

fn fetch_external_logs_archive<T: ExternalTestbed>(
    layout: &T,
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
    let archive_path = output_root.join(format!(
        "{}-{}-logs.tar.gz",
        sanitized_run_id,
        layout.backend_name()
    ));

    let output = layout.run_remote_host_command(
        &format!(
            "sudo {REMOTE_CONTROL_SCRIPT_PATH} collect-logs {remote_dir} >/dev/null && sudo tar czf - -C {remote_parent} {remote_name}"
        ),
        None,
    )?;

    let mut file = File::create(&archive_path)
        .map_err(|error| format!("failed to create {}: {error}", archive_path.display()))?;
    file.write_all(&output)
        .map_err(|error| format!("failed to write {}: {error}", archive_path.display()))?;
    file.sync_all()
        .map_err(|error| format!("failed to sync {}: {error}", archive_path.display()))?;
    Ok(archive_path)
}

fn send_remote_api_request<T: ExternalTestbed>(
    layout: &T,
    host: &str,
    port: u16,
    request: &ApiRequest,
) -> Result<Vec<u8>, String> {
    let request_bytes = encode_request(request)
        .map_err(|error| format!("failed to encode api request: {error:?}"))?;
    run_remote_tcp_request(layout, host, port, &request_bytes)
}

fn run_remote_tcp_request<T: ExternalTestbed>(
    layout: &T,
    host: &str,
    port: u16,
    request_bytes: &[u8],
) -> Result<Vec<u8>, String> {
    layout.run_remote_tcp_request(host, port, request_bytes)
}

fn build_remote_tcp_probe_command(host: &str, port: u16, request_hex: &str) -> String {
    let request_hex = if request_hex.is_empty() {
        String::from("-")
    } else {
        request_hex.to_string()
    };
    format!(
        "python3 - {host} {port} {request_hex} {EXTERNAL_REMOTE_TCP_TIMEOUT_SECS} <<'PY'\nimport socket, sys\nhost = sys.argv[1]\nport = int(sys.argv[2])\npayload = b'' if sys.argv[3] == '-' else bytes.fromhex(sys.argv[3])\ntimeout_secs = float(sys.argv[4])\nwith socket.create_connection((host, port), timeout=timeout_secs) as stream:\n    stream.settimeout(timeout_secs)\n    if payload:\n        stream.sendall(payload)\n    stream.shutdown(socket.SHUT_WR)\n    chunks = []\n    while True:\n        chunk = stream.recv(4096)\n        if not chunk:\n            break\n        chunks.append(chunk)\n    sys.stdout.buffer.write(b''.join(chunks))\nPY"
    )
}

fn validate_protocol_probe_response(
    backend_name: &str,
    replica_id: ReplicaId,
    response_bytes: &[u8],
) -> Result<(), String> {
    let protocol_text = String::from_utf8_lossy(response_bytes);
    if protocol_text.contains("protocol transport not implemented")
        || protocol_text.contains("network isolated by local harness")
    {
        return Err(format!(
            "{} protocol surface is not ready for Jepsen: replica {} returned `{protocol_text}`",
            backend_name,
            replica_id.get()
        ));
    }
    Ok(())
}

fn extract_probe_commit_lsn(backend_name: &str, response: ApiResponse) -> Result<Lsn, String> {
    match response {
        ApiResponse::Submit(SubmitResponse::Committed(result)) => Ok(result.applied_lsn),
        other => Err(format!(
            "{backend_name} client surface is not ready for Jepsen: primary submit probe did not commit: {other:?}"
        )),
    }
}

fn validate_probe_read_response(
    backend_name: &str,
    resource_id: u128,
    response: &ApiResponse,
) -> Result<(), String> {
    if matches!(
        response,
        ApiResponse::GetResource(ResourceResponse::Found(resource))
            if resource.resource_id == ResourceId(resource_id)
    ) {
        Ok(())
    } else {
        Err(format!(
            "{backend_name} client surface is not ready for Jepsen: primary read probe returned unexpected response {response:?}"
        ))
    }
}

fn enforce_minimum_fault_window(run_spec: &JepsenRunSpec, elapsed: Duration) -> Result<(), String> {
    let Some(minimum) = minimum_fault_window_duration(run_spec) else {
        return Ok(());
    };
    if elapsed < minimum {
        return Err(format!(
            "run `{}` finished before minimum fault window (required={}s observed={}s)",
            run_spec.run_id,
            minimum.as_secs(),
            elapsed.as_secs()
        ));
    }
    Ok(())
}

fn minimum_fault_window_duration(run_spec: &JepsenRunSpec) -> Option<Duration> {
    effective_minimum_fault_window_secs(run_spec).map(Duration::from_secs)
}

fn effective_minimum_fault_window_secs(run_spec: &JepsenRunSpec) -> Option<u64> {
    if let Some(override_secs) = debug_fault_window_override_secs() {
        return Some(override_secs);
    }
    run_spec.minimum_fault_window_secs
}

fn debug_fault_window_override_secs() -> Option<u64> {
    let raw = std::env::var(FAULT_WINDOW_OVERRIDE_ENV).ok()?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    trimmed.parse::<u64>().ok().filter(|secs| *secs > 0)
}

fn fault_window_complete(minimum_fault_window: Option<Duration>, elapsed: Duration) -> bool {
    minimum_fault_window.is_none_or(|minimum| elapsed >= minimum)
}

fn render_fault_window_iteration_event(
    run_spec: &JepsenRunSpec,
    iteration: u64,
    elapsed: Duration,
    history_events: u64,
) -> String {
    match minimum_fault_window_duration(run_spec) {
        Some(minimum) => {
            let remaining = minimum.saturating_sub(elapsed);
            format!(
                "fault window iteration={} elapsed={}s remaining={}s history_events={history_events}",
                iteration,
                elapsed.as_secs(),
                remaining.as_secs(),
            )
        }
        None => format!(
            "control iteration={} elapsed={}s history_events={history_events}",
            iteration,
            elapsed.as_secs(),
        ),
    }
}

fn decode_external_api_response<T: ExternalTestbed>(
    layout: &T,
    bytes: &[u8],
) -> Result<ApiResponse, String> {
    decode_response(bytes).map_err(|error| {
        let text = String::from_utf8_lossy(bytes);
        format!(
            "invalid {} api response: {error:?}; raw_response={text}",
            layout.backend_name()
        )
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
    let millis = current_time_millis();
    let nonce = u128::from(NEXT_PROBE_RESOURCE_ID.fetch_add(1, Ordering::Relaxed));
    (millis << 32) | nonce
}

fn probe_create_request(resource_id: u128, namespace: RequestNamespace) -> ClientRequest {
    namespace.client_request(
        OperationId(resource_id),
        AllocCommand::CreateResource {
            resource_id: ResourceId(resource_id),
        },
    )
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

fn qemu_ssh_args(layout: &QemuTestbedLayout) -> Vec<String> {
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

fn run_qemu_remote_host_command(
    layout: &QemuTestbedLayout,
    remote_command: &str,
    stdin_bytes: Option<&[u8]>,
) -> Result<Vec<u8>, String> {
    let mut remote_args = qemu_ssh_args(layout);
    remote_args.push(String::from(remote_command));

    let output = if let Some(stdin_bytes) = stdin_bytes {
        let mut child = Command::new("ssh")
            .args(&remote_args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| format!("failed to spawn qemu remote ssh command: {error}"))?;
        child
            .stdin
            .as_mut()
            .ok_or_else(|| String::from("qemu remote ssh stdin was unavailable"))?
            .write_all(stdin_bytes)
            .map_err(|error| format!("failed to write qemu remote ssh stdin: {error}"))?;
        child
            .wait_with_output()
            .map_err(|error| format!("failed to wait for qemu remote ssh command: {error}"))?
    } else {
        Command::new("ssh")
            .args(&remote_args)
            .output()
            .map_err(|error| format!("failed to run qemu remote ssh command: {error}"))?
    };

    if output.status.success() {
        Ok(output.stdout)
    } else {
        Err(format!(
            "qemu remote command failed: status={} stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}

fn run_qemu_remote_tcp_request(
    layout: &QemuTestbedLayout,
    host: &str,
    port: u16,
    request_bytes: &[u8],
) -> Result<Vec<u8>, String> {
    let request_hex = encode_hex(request_bytes);
    run_qemu_remote_host_command(
        layout,
        &build_remote_tcp_probe_command(host, port, &request_hex),
        None,
    )
}

fn kubevirt_helper_ssh_args(layout: &KubevirtTestbedLayout) -> Vec<String> {
    let mut args = kubectl_base_args(
        layout.config.kubeconfig_path.as_deref(),
        &layout.config.namespace,
    );
    args.extend([
        String::from("exec"),
        String::from("-i"),
        layout.config.helper_pod_name.clone(),
        String::from("--"),
        String::from("ssh"),
        String::from("-i"),
        layout
            .config
            .helper_stage_dir
            .join("id_ed25519")
            .display()
            .to_string(),
        String::from("-o"),
        String::from("StrictHostKeyChecking=no"),
        String::from("-o"),
        String::from("UserKnownHostsFile=/dev/null"),
        String::from("-o"),
        String::from("LogLevel=ERROR"),
        String::from("-o"),
        String::from("ConnectTimeout=5"),
        format!("{DEFAULT_GUEST_USER}@{}", layout.control_guest.addr),
    ]);
    args
}

fn run_kubevirt_remote_host_command(
    layout: &KubevirtTestbedLayout,
    remote_command: &str,
    stdin_bytes: Option<&[u8]>,
) -> Result<Vec<u8>, String> {
    let mut args = kubevirt_helper_ssh_args(layout);
    args.push(String::from(remote_command));

    let output = if let Some(stdin_bytes) = stdin_bytes {
        let mut child = Command::new("kubectl")
            .args(&args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| format!("failed to spawn kubevirt remote ssh command: {error}"))?;
        child
            .stdin
            .as_mut()
            .ok_or_else(|| String::from("kubevirt remote ssh stdin was unavailable"))?
            .write_all(stdin_bytes)
            .map_err(|error| format!("failed to write kubevirt remote ssh stdin: {error}"))?;
        child
            .wait_with_output()
            .map_err(|error| format!("failed to wait for kubevirt remote ssh command: {error}"))?
    } else {
        Command::new("kubectl")
            .args(&args)
            .output()
            .map_err(|error| format!("failed to run kubevirt remote ssh command: {error}"))?
    };

    if output.status.success() {
        Ok(output.stdout)
    } else {
        Err(format!(
            "kubevirt remote command failed: status={} stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}

fn run_kubevirt_remote_tcp_request(
    layout: &KubevirtTestbedLayout,
    host: &str,
    port: u16,
    request_bytes: &[u8],
) -> Result<Vec<u8>, String> {
    let request_hex = encode_hex(request_bytes);
    let probe_command = build_remote_tcp_probe_command(host, port, &request_hex);
    let mut args = kubectl_base_args(
        layout.config.kubeconfig_path.as_deref(),
        &layout.config.namespace,
    );
    args.extend([
        String::from("exec"),
        layout.config.helper_pod_name.clone(),
        String::from("--"),
        String::from("sh"),
        String::from("-lc"),
        probe_command,
    ]);
    let output = Command::new("kubectl")
        .args(&args)
        .output()
        .map_err(|error| format!("failed to run kubevirt remote tcp probe: {error}"))?;
    if output.status.success() {
        Ok(output.stdout)
    } else {
        Err(format!(
            "kubevirt remote tcp probe failed: status={} stderr={}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use allocdb_core::ResourceState;
    use std::fs;

    fn test_replica_config(replica_id: u64, role: ReplicaRole) -> LocalClusterReplicaConfig {
        let root = PathBuf::from(format!("/tmp/jepsen-test-replica-{replica_id}"));
        LocalClusterReplicaConfig {
            replica_id: ReplicaId(replica_id),
            role,
            workspace_dir: root.clone(),
            log_path: root.join("replica.log"),
            pid_path: root.join("replica.pid"),
            paths: ReplicaPaths::new(
                root.join("replica.metadata"),
                root.join("state.snapshot"),
                root.join("state.wal"),
            ),
            control_addr: format!("127.0.0.1:{}", 17_000 + replica_id)
                .parse()
                .unwrap(),
            client_addr: format!("127.0.0.1:{}", 18_000 + replica_id)
                .parse()
                .unwrap(),
            protocol_addr: format!("127.0.0.1:{}", 19_000 + replica_id)
                .parse()
                .unwrap(),
        }
    }

    fn test_replica_status(replica: &LocalClusterReplicaConfig) -> ReplicaRuntimeStatus {
        ReplicaRuntimeStatus {
            process_id: 42,
            replica_id: replica.replica_id,
            state: ReplicaRuntimeState::Active,
            role: replica.role,
            current_view: 7,
            commit_lsn: Some(Lsn(11)),
            active_snapshot_lsn: Some(Lsn(11)),
            accepting_writes: Some(replica.role == ReplicaRole::Primary),
            startup_kind: None,
            loaded_snapshot_lsn: Some(Lsn(11)),
            replayed_wal_frame_count: Some(0),
            replayed_wal_last_lsn: Some(Lsn(11)),
            fault_reason: None,
            workspace_dir: replica.workspace_dir.clone(),
            log_path: replica.log_path.clone(),
            pid_path: replica.pid_path.clone(),
            metadata_path: replica.paths.metadata_path.clone(),
            prepare_log_path: prepare_log_path_for(&replica.paths.metadata_path),
            snapshot_path: replica.paths.snapshot_path.clone(),
            wal_path: replica.paths.wal_path.clone(),
            control_addr: replica.control_addr,
            client_addr: replica.client_addr,
            protocol_addr: replica.protocol_addr,
        }
    }

    fn test_faulted_replica_status(
        replica: &LocalClusterReplicaConfig,
        reason: &str,
    ) -> ReplicaRuntimeStatus {
        let mut status = test_replica_status(replica);
        status.state = ReplicaRuntimeState::Faulted;
        status.fault_reason = Some(reason.to_string());
        status
    }

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
        assert!(command.starts_with(&format!(
            "python3 - 127.0.0.1 9000 deadbeef {EXTERNAL_REMOTE_TCP_TIMEOUT_SECS} <<'PY'\n"
        )));
        assert!(
            command.contains("payload = b'' if sys.argv[3] == '-' else bytes.fromhex(sys.argv[3])")
        );
        assert!(command.contains("timeout_secs = float(sys.argv[4])"));
        assert!(command.ends_with("\nPY"));
    }

    #[test]
    fn remote_tcp_probe_command_preserves_empty_payload_argument() {
        let command = build_remote_tcp_probe_command("127.0.0.1", 9000, "");
        assert!(command.starts_with(&format!(
            "python3 - 127.0.0.1 9000 - {EXTERNAL_REMOTE_TCP_TIMEOUT_SECS} <<'PY'\n"
        )));
        assert!(command.contains("payload = b'' if sys.argv[3] == '-'"));
    }

    #[test]
    fn protocol_probe_rejects_placeholder_responses() {
        let not_ready = validate_protocol_probe_response(
            "qemu",
            ReplicaId(2),
            b"protocol transport not implemented",
        )
        .unwrap_err();
        assert!(not_ready.contains("replica 2"));

        let isolated = validate_protocol_probe_response(
            "qemu",
            ReplicaId(3),
            b"network isolated by local harness",
        )
        .unwrap_err();
        assert!(isolated.contains("replica 3"));

        assert!(validate_protocol_probe_response("qemu", ReplicaId(4), b"").is_ok());
    }

    #[test]
    fn probe_submit_and_read_validation_cover_pass_and_fail_paths() {
        let applied_lsn = extract_probe_commit_lsn(
            "qemu",
            ApiResponse::Submit(SubmitResponse::Committed(
                allocdb_node::SubmissionResult {
                    applied_lsn: Lsn(9),
                    outcome: allocdb_core::result::CommandOutcome::new(ResultCode::Ok),
                    from_retry_cache: false,
                }
                .into(),
            )),
        )
        .unwrap();
        assert_eq!(applied_lsn, Lsn(9));

        let submit_error =
            extract_probe_commit_lsn("qemu", ApiResponse::GetResource(ResourceResponse::NotFound))
                .unwrap_err();
        assert!(submit_error.contains("did not commit"));

        let ok_read =
            ApiResponse::GetResource(ResourceResponse::Found(allocdb_node::ResourceView {
                resource_id: ResourceId(41),
                state: ResourceState::Available,
                current_reservation_id: None,
                version: 1,
            }));
        assert!(validate_probe_read_response("qemu", 41, &ok_read).is_ok());

        let read_error = validate_probe_read_response(
            "qemu",
            41,
            &ApiResponse::GetResource(ResourceResponse::NotFound),
        )
        .unwrap_err();
        assert!(read_error.contains("unexpected response"));
    }

    #[test]
    fn classify_resource_read_outcome_distinguishes_available_and_held_states() {
        let available = classify_resource_read_outcome(
            ResourceId(41),
            RemoteApiOutcome::Api(ApiResponse::GetResource(ResourceResponse::Found(
                allocdb_node::ResourceView {
                    resource_id: ResourceId(41),
                    state: ResourceState::Available,
                    current_reservation_id: None,
                    version: 2,
                },
            ))),
        )
        .unwrap();
        assert_eq!(available, ResourceReadObservation::Available);

        let held = classify_resource_read_outcome(
            ResourceId(41),
            RemoteApiOutcome::Api(ApiResponse::GetResource(ResourceResponse::Found(
                allocdb_node::ResourceView {
                    resource_id: ResourceId(41),
                    state: ResourceState::Reserved,
                    current_reservation_id: Some(ReservationId(7)),
                    version: 3,
                },
            ))),
        )
        .unwrap();
        assert_eq!(
            held,
            ResourceReadObservation::Held {
                state: ResourceState::Reserved,
                current_reservation_id: Some(ReservationId(7)),
                version: 3,
            }
        );
    }

    #[test]
    fn classify_resource_read_outcome_maps_not_primary_text() {
        let observation = classify_resource_read_outcome(
            ResourceId(11),
            RemoteApiOutcome::Text(String::from("not primary: role=backup")),
        )
        .unwrap();
        assert_eq!(observation, ResourceReadObservation::NotPrimary);
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

    #[test]
    fn copy_file_or_remove_copies_and_removes_stale_destination() {
        let root = std::env::temp_dir().join(format!(
            "allocdb-jepsen-copy-{}",
            unique_probe_resource_id()
        ));
        fs::create_dir_all(&root).unwrap();
        let source = root.join("source.bin");
        let destination = root.join("destination.bin");

        fs::write(&source, b"snapshot-bytes").unwrap();
        copy_file_or_remove(&source, &destination).unwrap();
        assert_eq!(fs::read(&destination).unwrap(), b"snapshot-bytes");

        fs::remove_file(&source).unwrap();
        copy_file_or_remove(&source, &destination).unwrap();
        assert!(!destination.exists());

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn unique_probe_resource_id_is_monotonic() {
        let first = unique_probe_resource_id();
        let second = unique_probe_resource_id();
        assert!(second > first);
    }

    #[test]
    fn request_namespace_monotonicity_covers_verify_then_execute_ordering() {
        let first = RequestNamespace::new();
        let second = RequestNamespace::new();
        assert_ne!(first.client_id, second.client_id);
        assert!(second.slot(1).get() >= first.slot(1).get());
        assert!(first.slot(11).get() > first.slot(10).get());
    }

    #[test]
    fn temp_staging_dir_uses_unique_paths_for_same_prefix() {
        let first = temp_staging_dir("replica-1").unwrap();
        let second = temp_staging_dir("replica-1").unwrap();
        assert_ne!(first, second);
        let _ = fs::remove_dir_all(first);
        let _ = fs::remove_dir_all(second);
    }

    #[test]
    fn run_status_snapshot_round_trips_through_text_codec() {
        let snapshot = RunStatusSnapshot {
            backend_name: String::from("kubevirt"),
            run_id: String::from("reservation_contention-control"),
            state: RunTrackerState::Running,
            phase: RunTrackerPhase::Executing,
            detail: String::from("executing Jepsen scenario"),
            started_at_millis: 11,
            updated_at_millis: 42,
            elapsed_secs: 31,
            minimum_fault_window_secs: Some(1800),
            history_events: 7,
            history_file: Some(PathBuf::from("/tmp/history.txt")),
            artifact_bundle: None,
            logs_archive: Some(PathBuf::from("/tmp/logs.tar.gz")),
            release_gate_passed: None,
            blockers: Some(0),
            last_error: Some(String::from("none yet")),
        };

        let encoded = encode_run_status_snapshot(&snapshot);
        let decoded = decode_run_status_snapshot(&encoded).unwrap();
        assert_eq!(decoded, snapshot);
    }

    #[test]
    fn compact_counter_formats_large_values() {
        assert_eq!(compact_counter(999), "999");
        assert_eq!(compact_counter(1_234), "1.2K");
        assert_eq!(compact_counter(12_500_000), "12.5M");
        assert_eq!(compact_counter(7_200_000_000), "7.2B");
    }

    #[test]
    fn parse_watch_event_line_extracts_timestamp_and_detail() {
        let event = parse_watch_event_line(
            "time_millis=1773492183417 detail=collecting logs and artifacts",
        )
        .unwrap();
        assert_eq!(event.time_millis, 1_773_492_183_417);
        assert_eq!(event.detail, "collecting logs and artifacts");
    }

    #[test]
    fn progress_bar_clamps_to_requested_width() {
        assert_eq!(progress_bar(0, 10, 5), "░░░░░");
        assert_eq!(progress_bar(5, 10, 5), "██░░░");
        assert_eq!(progress_bar(15, 10, 5), "█████");
    }

    #[test]
    fn fault_window_completion_distinguishes_control_and_long_fault_runs() {
        assert!(fault_window_complete(None, Duration::from_secs(0)));
        assert!(!fault_window_complete(
            Some(Duration::from_secs(1_800)),
            Duration::from_secs(1_799),
        ));
        assert!(fault_window_complete(
            Some(Duration::from_secs(1_800)),
            Duration::from_secs(1_800),
        ));
    }

    #[test]
    fn history_builder_preserves_nonzero_sequence_offsets() {
        let mut history = HistoryBuilder::new(None, 7);
        history.push(
            "replica-1",
            JepsenOperation {
                kind: JepsenOperationKind::Reserve,
                operation_id: Some(11),
                resource_id: Some(ResourceId(21)),
                reservation_id: None,
                holder_id: Some(31),
                required_lsn: None,
                request_slot: Some(Slot(41)),
                ttl_slots: Some(5),
            },
            JepsenEventOutcome::Ambiguous(JepsenAmbiguousOutcome::Timeout),
        );
        history.push(
            "replica-2",
            JepsenOperation {
                kind: JepsenOperationKind::Reserve,
                operation_id: Some(12),
                resource_id: Some(ResourceId(22)),
                reservation_id: None,
                holder_id: Some(32),
                required_lsn: None,
                request_slot: Some(Slot(42)),
                ttl_slots: Some(5),
            },
            JepsenEventOutcome::Ambiguous(JepsenAmbiguousOutcome::Timeout),
        );
        let events = history.finish();
        assert_eq!(events[0].sequence, 8);
        assert_eq!(events[1].sequence, 9);
    }

    #[test]
    fn analyzer_accepts_failover_read_fence_history_once_ambiguity_is_retried() {
        let reserve_operation_id = 21;
        let ambiguous_operation_id = 22;
        let resource_id = ResourceId(101);
        let reservation_id = ReservationId(55);
        let committed_lsn = Lsn(44);

        let mut history = HistoryBuilder::new(None, 0);
        history.push(
            "primary-1",
            JepsenOperation {
                kind: JepsenOperationKind::Reserve,
                operation_id: Some(reserve_operation_id),
                resource_id: Some(resource_id),
                reservation_id: None,
                holder_id: Some(604),
                required_lsn: None,
                request_slot: Some(Slot(53)),
                ttl_slots: Some(6),
            },
            JepsenEventOutcome::CommittedWrite(JepsenCommittedWrite {
                applied_lsn: committed_lsn,
                result: JepsenWriteResult::Reserved {
                    resource_id,
                    holder_id: 604,
                    reservation_id: reservation_id.get(),
                    expires_at_slot: Slot(900),
                },
            }),
        );
        history.push(
            "primary-1",
            JepsenOperation {
                kind: JepsenOperationKind::Reserve,
                operation_id: Some(ambiguous_operation_id),
                resource_id: Some(resource_id),
                reservation_id: None,
                holder_id: Some(605),
                required_lsn: None,
                request_slot: Some(Slot(54)),
                ttl_slots: Some(2),
            },
            JepsenEventOutcome::Ambiguous(JepsenAmbiguousOutcome::IndefiniteWrite),
        );
        history.push(
            "primary-2",
            JepsenOperation {
                kind: JepsenOperationKind::GetReservation,
                operation_id: None,
                resource_id: None,
                reservation_id: Some(reservation_id.get()),
                holder_id: None,
                required_lsn: Some(committed_lsn),
                request_slot: Some(Slot(54)),
                ttl_slots: None,
            },
            JepsenEventOutcome::SuccessfulRead(JepsenSuccessfulRead {
                target: JepsenReadTarget::Reservation,
                served_by: ReplicaId(2),
                served_role: ReplicaRole::Primary,
                observed_lsn: Some(committed_lsn),
                state: JepsenReadState::Reservation(JepsenReservationState::Active {
                    resource_id,
                    holder_id: 604,
                    expires_at_slot: Slot(900),
                    confirmed: false,
                }),
            }),
        );
        history.push(
            "primary-2",
            JepsenOperation {
                kind: JepsenOperationKind::Reserve,
                operation_id: Some(ambiguous_operation_id),
                resource_id: Some(resource_id),
                reservation_id: None,
                holder_id: Some(605),
                required_lsn: None,
                request_slot: Some(Slot(54)),
                ttl_slots: Some(2),
            },
            JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::Conflict),
        );
        history.push(
            "backup-1",
            JepsenOperation {
                kind: JepsenOperationKind::GetReservation,
                operation_id: None,
                resource_id: None,
                reservation_id: Some(reservation_id.get()),
                holder_id: None,
                required_lsn: Some(committed_lsn),
                request_slot: Some(Slot(54)),
                ttl_slots: None,
            },
            JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::NotPrimary),
        );

        let report = analyze_history(&history.finish());
        assert!(report.release_gate_passed());
        assert!(report.blockers.is_empty());
    }

    #[test]
    fn live_runtime_replica_matching_ignores_down_and_faulted_replicas() {
        let primary = test_replica_config(1, ReplicaRole::Primary);
        let backup = test_replica_config(2, ReplicaRole::Backup);
        let down = test_replica_config(3, ReplicaRole::Backup);
        let faulted = test_replica_config(4, ReplicaRole::Backup);
        let probes = vec![
            RuntimeReplicaProbe {
                replica: down,
                status: Err(String::from("connection refused")),
            },
            RuntimeReplicaProbe {
                replica: faulted.clone(),
                status: Ok(test_faulted_replica_status(&faulted, "stale metadata")),
            },
            RuntimeReplicaProbe {
                replica: backup.clone(),
                status: Ok(test_replica_status(&backup)),
            },
            RuntimeReplicaProbe {
                replica: primary.clone(),
                status: Ok(test_replica_status(&primary)),
            },
        ];

        let discovered =
            live_runtime_replica_matching(&probes, |replica| replica.role == ReplicaRole::Primary)
                .unwrap();

        assert_eq!(discovered.replica_id, primary.replica_id);
    }

    #[test]
    fn render_runtime_probe_summary_marks_live_faulted_and_down_replicas() {
        let primary = test_replica_config(1, ReplicaRole::Primary);
        let backup = test_replica_config(2, ReplicaRole::Backup);
        let faulted = test_replica_config(3, ReplicaRole::Backup);
        let probes = vec![
            RuntimeReplicaProbe {
                replica: primary.clone(),
                status: Ok(test_replica_status(&primary)),
            },
            RuntimeReplicaProbe {
                replica: backup.clone(),
                status: Err(String::from(
                    "connection refused\ncommand terminated with exit code 1",
                )),
            },
            RuntimeReplicaProbe {
                replica: faulted.clone(),
                status: Ok(test_faulted_replica_status(
                    &faulted,
                    "applied lsn behind commit lsn",
                )),
            },
        ];

        let summary = render_runtime_probe_summary(&probes);

        assert!(summary.contains("1:primary@view7"));
        assert!(summary.contains("2:down(connection refused)"));
        assert!(summary.contains("3:faulted(applied lsn behind commit lsn)"));
    }

    #[test]
    fn summarize_runtime_probes_counts_only_active_roles() {
        let primary = test_replica_config(1, ReplicaRole::Primary);
        let backup = test_replica_config(2, ReplicaRole::Backup);
        let faulted = test_replica_config(3, ReplicaRole::Backup);
        let probes = vec![
            RuntimeReplicaProbe {
                replica: primary.clone(),
                status: Ok(test_replica_status(&primary)),
            },
            RuntimeReplicaProbe {
                replica: backup.clone(),
                status: Ok(test_replica_status(&backup)),
            },
            RuntimeReplicaProbe {
                replica: faulted.clone(),
                status: Ok(test_faulted_replica_status(&faulted, "stale metadata")),
            },
        ];

        assert_eq!(
            summarize_runtime_probes(&probes),
            RuntimeReplicaTopology {
                active: 2,
                primaries: 1,
                backups: 1,
            }
        );
    }

    #[test]
    fn parse_watch_kubevirt_lane_spec_extracts_name_workspace_and_output_root() {
        let lane = parse_watch_kubevirt_lane_spec("lane-a,/tmp/work-a,/tmp/out-a").unwrap();
        assert_eq!(
            lane,
            KubevirtWatchLaneSpec {
                name: String::from("lane-a"),
                workspace_root: PathBuf::from("/tmp/work-a"),
                output_root: PathBuf::from("/tmp/out-a"),
            }
        );
    }

    #[test]
    fn parse_watch_kubevirt_lane_spec_rejects_missing_fields() {
        let error = parse_watch_kubevirt_lane_spec("lane-a,/tmp/work-a").unwrap_err();
        assert!(error.contains("expected <name,workspace,output-root>"));
    }

    #[test]
    fn compact_fault_window_progress_formats_control_and_faulted_runs() {
        let mut snapshot = RunStatusSnapshot {
            backend_name: String::from("kubevirt"),
            run_id: String::from("reservation_contention-control"),
            state: RunTrackerState::Running,
            phase: RunTrackerPhase::Executing,
            detail: String::from("executing"),
            started_at_millis: 0,
            updated_at_millis: 0,
            elapsed_secs: 45,
            minimum_fault_window_secs: None,
            history_events: 3,
            history_file: None,
            artifact_bundle: None,
            logs_archive: None,
            release_gate_passed: None,
            blockers: None,
            last_error: None,
        };
        assert_eq!(compact_fault_window_progress(&snapshot), "ctrl");
        snapshot.minimum_fault_window_secs = Some(1_800);
        snapshot.elapsed_secs = 900;
        assert!(compact_fault_window_progress(&snapshot).contains("50%"));
    }

    #[test]
    fn disable_local_tar_copyfile_metadata_sets_expected_env() {
        let mut command = Command::new("sh");
        disable_local_tar_copyfile_metadata(&mut command);
        let output = command
            .arg("-lc")
            .arg("printf %s \"${COPYFILE_DISABLE:-}\"")
            .output()
            .unwrap();
        let value = String::from_utf8(output.stdout).unwrap();
        if cfg!(target_os = "macos") {
            assert_eq!(value, "1");
        } else {
            assert!(value.is_empty());
        }
    }
}

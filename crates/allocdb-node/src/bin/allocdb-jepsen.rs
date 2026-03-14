use std::fs::{self, File};
use std::io::{Read, Write};
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
static NEXT_PROBE_RESOURCE_ID: AtomicU64 = AtomicU64::new(0);

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
        } => watch_kubevirt(
            &workspace_root,
            &output_root,
            run_id.as_deref(),
            refresh_millis,
        ),
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
            "--help" | "-h" => return Err(usage()),
            other => return Err(format!("unknown argument `{other}`\n\n{}", usage())),
        }
    }

    Ok(ParsedCommand::WatchKubevirt {
        workspace_root: workspace_root.ok_or_else(usage)?,
        output_root: output_root.ok_or_else(usage)?,
        run_id,
        refresh_millis: refresh_millis.ok_or_else(usage)?,
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
        "usage:\n  allocdb-jepsen plan\n  allocdb-jepsen analyze --history-file <path>\n  allocdb-jepsen capture-kubevirt-layout --workspace <path> --namespace <name> --ssh-private-key <path> [--kubeconfig <path>] [--helper-pod <name>] [--helper-image <image>] [--helper-stage-dir <path>] [--control-vm <name>] [--replica-1-vm <name>] [--replica-2-vm <name>] [--replica-3-vm <name>]\n  allocdb-jepsen verify-qemu-surface --workspace <path>\n  allocdb-jepsen verify-kubevirt-surface --workspace <path>\n  allocdb-jepsen run-qemu --workspace <path> --run-id <run-id> --output-root <path>\n  allocdb-jepsen run-kubevirt --workspace <path> --run-id <run-id> --output-root <path>\n  allocdb-jepsen watch-kubevirt --workspace <path> --output-root <path> [--run-id <run-id>] [--refresh-millis <ms>]\n  allocdb-jepsen archive-qemu --workspace <path> --run-id <run-id> --history-file <path> --output-root <path>\n  allocdb-jepsen archive-kubevirt --workspace <path> --run-id <run-id> --history-file <path> --output-root <path>\n",
    )
}

trait ExternalTestbed {
    fn backend_name(&self) -> &'static str;
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
    // Surface verification performs a real committed write to prove the external runtime is live.
    // Build the scenario namespace only after that probe so later requests cannot rewind slots.
    let context = RunExecutionContext::new(tracker.clone());
    tracker.set_phase(RunTrackerPhase::Executing, "executing Jepsen scenario")?;
    let history = match execute_external_run(layout, &run_spec, &context) {
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
    context: &RunExecutionContext,
) -> Result<Vec<JepsenHistoryEvent>, String> {
    match run_spec.nemesis {
        JepsenNemesisFamily::None => execute_control_run(layout, run_spec, context),
        JepsenNemesisFamily::CrashRestart => execute_crash_restart_run(layout, run_spec, context),
        JepsenNemesisFamily::PartitionHeal => execute_partition_heal_run(layout, run_spec, context),
        JepsenNemesisFamily::MixedFailover => execute_mixed_failover_run(layout, run_spec, context),
    }
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
            minimum_fault_window_secs: run_spec.minimum_fault_window_secs,
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
}

impl RunExecutionContext {
    fn new(tracker: RunTracker) -> Self {
        Self {
            namespace: RequestNamespace::new(),
            tracker,
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
    fn new(tracker: Option<RunTracker>) -> Self {
        Self {
            next_sequence: 0,
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
        )?;
        if snapshot
            .as_ref()
            .is_some_and(|snapshot| snapshot.state != RunTrackerState::Running)
        {
            break;
        }
        thread::sleep(Duration::from_millis(refresh_millis));
    }
    Ok(())
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
) -> Vec<String> {
    let Some(events_path) = snapshot
        .map(|snapshot| run_events_path(output_root, &snapshot.run_id))
        .or_else(|| run_id.map(|run_id| run_events_path(output_root, run_id)))
    else {
        return Vec::new();
    };
    let Ok(bytes) = fs::read_to_string(&events_path) else {
        return Vec::new();
    };
    let mut lines = bytes.lines().map(String::from).collect::<Vec<_>>();
    if lines.len() > limit {
        lines.drain(..lines.len().saturating_sub(limit));
    }
    lines
}

fn render_kubevirt_watch(
    status_path: &Path,
    snapshot: Option<&RunStatusSnapshot>,
    replicas: &[ReplicaWatchSnapshot],
    recent_events: &[String],
    refresh_millis: u64,
) -> Result<(), String> {
    print!("\x1B[2J\x1B[H");
    println!("AllocDB Jepsen KubeVirt Watch");
    println!("status_file={}", status_path.display());
    println!("refresh_millis={refresh_millis}");
    render_run_summary(snapshot);
    println!();
    render_replica_watch_rows(replicas);
    println!();
    render_recent_events(recent_events);
    std::io::stdout()
        .flush()
        .map_err(|error| format!("failed to flush watcher output: {error}"))
}

fn render_run_summary(snapshot: Option<&RunStatusSnapshot>) {
    match snapshot {
        Some(snapshot) => {
            println!(
                "run_id={} state={} phase={} elapsed={}s minimum_fault_window={} detail={}",
                snapshot.run_id,
                snapshot.state.as_str(),
                snapshot.phase.as_str(),
                snapshot.elapsed_secs,
                snapshot
                    .minimum_fault_window_secs
                    .map_or_else(|| String::from("none"), |value| value.to_string()),
                snapshot.detail
            );
            println!(
                "history_events={} blockers={} release_gate_passed={}",
                snapshot.history_events,
                snapshot
                    .blockers
                    .map_or_else(|| String::from("none"), |value| value.to_string()),
                snapshot.release_gate_passed.map_or_else(
                    || String::from("none"),
                    |value| if value {
                        String::from("true")
                    } else {
                        String::from("false")
                    }
                )
            );
            if let Some(error) = &snapshot.last_error {
                println!("last_error={error}");
            }
        }
        None => {
            println!("run_id=waiting state=none phase=waiting detail=waiting for run status file");
        }
    }
}

fn render_replica_watch_rows(replicas: &[ReplicaWatchSnapshot]) {
    println!("Replicas");
    println!("id  state   role      view  commit_lsn  writes  queue  lag  backlog  error");
    for replica in replicas {
        println!("{}", format_replica_watch_row(replica));
    }
}

fn format_replica_watch_row(replica: &ReplicaWatchSnapshot) -> String {
    let state = replica.status.as_ref().map_or("down", |status| {
        if status.state == ReplicaRuntimeState::Active {
            "up"
        } else {
            "degraded"
        }
    });
    let role = replica
        .status
        .as_ref()
        .map_or(String::from("unknown"), |status| {
            format!("{:?}", status.role).to_lowercase()
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
                .map_or_else(|| String::from("-"), |value| value.get().to_string())
        },
    );
    let writes = replica.accepting_writes.map_or(String::from("-"), |value| {
        if value {
            String::from("yes")
        } else {
            String::from("no")
        }
    });
    let queue = replica
        .queue_depth
        .map_or_else(|| String::from("-"), |value| value.to_string());
    let lag = replica
        .logical_slot_lag
        .map_or_else(|| String::from("-"), |value| value.to_string());
    let backlog = replica
        .expiration_backlog
        .map_or_else(|| String::from("-"), |value| value.to_string());
    let error = replica.error.as_deref().unwrap_or("-");
    format!(
        "{:<3} {:<7} {:<9} {:<5} {:<11} {:<6} {:<6} {:<4} {:<8} {}",
        replica.replica_id.get(),
        state,
        role,
        view,
        commit_lsn,
        writes,
        queue,
        lag,
        backlog,
        error
    )
}

fn render_recent_events(recent_events: &[String]) {
    println!("Recent Events");
    if recent_events.is_empty() {
        println!("<none>");
        return;
    }
    for line in recent_events {
        println!("{line}");
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

    fn from_export<T: ExternalTestbed>(layout: &T, replica_id: ReplicaId) -> Result<Self, String> {
        let staged = Self::new(replica_id)?;
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
    let root = std::env::temp_dir().join(format!("allocdb-{prefix}-{millis}"));
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

fn runtime_replica_configs<T: ExternalTestbed>(
    layout: &T,
) -> Result<Vec<LocalClusterReplicaConfig>, String> {
    let mut replicas = layout.replica_layout().replicas.clone();
    for replica in &mut replicas {
        let status = request_remote_control_status(layout, replica)?;
        replica.role = status.role;
    }
    Ok(replicas)
}

fn primary_replica<T: ExternalTestbed>(layout: &T) -> Result<LocalClusterReplicaConfig, String> {
    runtime_replica_configs(layout)?
        .into_iter()
        .find(|replica| replica.role == ReplicaRole::Primary)
        .ok_or_else(|| format!("{} cluster has no live primary", layout.backend_name()))
}

fn first_backup_replica<T: ExternalTestbed>(
    layout: &T,
    exclude: Option<ReplicaId>,
) -> Result<LocalClusterReplicaConfig, String> {
    runtime_replica_configs(layout)?
        .into_iter()
        .find(|replica| {
            replica.role == ReplicaRole::Backup
                && exclude.is_none_or(|excluded| replica.replica_id != excluded)
        })
        .ok_or_else(|| format!("{} cluster has no live backup", layout.backend_name()))
}

fn runtime_replica_by_id<T: ExternalTestbed>(
    layout: &T,
    replica_id: ReplicaId,
) -> Result<LocalClusterReplicaConfig, String> {
    runtime_replica_configs(layout)?
        .into_iter()
        .find(|replica| replica.replica_id == replica_id)
        .ok_or_else(|| {
            format!(
                "{} cluster has no replica {}",
                layout.backend_name(),
                replica_id.get()
            )
        })
}

fn maybe_crash_replica<T: ExternalTestbed>(
    layout: &T,
    replica_id: ReplicaId,
) -> Result<(), String> {
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
    let _ = run_remote_control_command(
        layout,
        &[String::from("restart"), replica_id.get().to_string()],
        None,
    )?;
    Ok(())
}

fn isolate_replica<T: ExternalTestbed>(layout: &T, replica_id: ReplicaId) -> Result<(), String> {
    let _ = run_remote_control_command(
        layout,
        &[String::from("isolate"), replica_id.get().to_string()],
        None,
    )?;
    Ok(())
}

fn heal_replica<T: ExternalTestbed>(layout: &T, replica_id: ReplicaId) -> Result<(), String> {
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

fn rewrite_replica_from_source<T: ExternalTestbed>(
    layout: &T,
    source: &StagedReplicaWorkspace,
    target: &StagedReplicaWorkspace,
    target_commit_lsn: Option<Lsn>,
    new_view: u64,
    new_role: ReplicaRole,
) -> Result<(), String> {
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
    Ok(())
}

fn perform_failover<T: ExternalTestbed>(
    layout: &T,
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

fn perform_rejoin<T: ExternalTestbed>(
    layout: &T,
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
    let mut history = HistoryBuilder::new(Some(context.tracker.clone()));
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

    let mut history = HistoryBuilder::new(Some(context.tracker.clone()));
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

    let mut history = HistoryBuilder::new(Some(context.tracker.clone()));
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

    let mut history = HistoryBuilder::new(Some(context.tracker.clone()));
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
        context,
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

    let mut history = HistoryBuilder::new(Some(context.tracker.clone()));
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

    let mut history = HistoryBuilder::new(Some(context.tracker.clone()));
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
    create_qemu_resource(
        layout,
        primary,
        context,
        resource_id,
        OperationId(base_id + 130),
    )?;

    let mut history = HistoryBuilder::new(Some(context.tracker.clone()));
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
            operation_id: OperationId(base_id + 22),
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

    let mut history = HistoryBuilder::new(Some(context.tracker.clone()));
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

    isolate_replica(layout, failover_target.replica_id)?;
    isolate_replica(layout, supporting_backup.replica_id)?;
    let failed_tick = tick_expirations_event(
        layout,
        primary,
        context,
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
        context,
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

    let mut history = HistoryBuilder::new(Some(context.tracker.clone()));
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

    let mut history = HistoryBuilder::new(Some(context.tracker.clone()));
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

    let mut history = HistoryBuilder::new(Some(context.tracker.clone()));
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

    let mut history = HistoryBuilder::new(Some(context.tracker.clone()));
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

    let mut history = HistoryBuilder::new(Some(context.tracker.clone()));
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

    let mut history = HistoryBuilder::new(Some(context.tracker.clone()));
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

    let mut history = HistoryBuilder::new(Some(context.tracker.clone()));
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

    let resource_read = resource_available_event(layout, primary, resource_id, Some(tick_lsn))?;
    history.push(
        primary_process_name(primary),
        resource_read.0,
        resource_read.1,
    );

    let reservation_read = reservation_read_event(
        layout,
        primary,
        context,
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
        "python3 - {host} {port} {request_hex} <<'PY'\nimport socket, sys\nhost = sys.argv[1]\nport = int(sys.argv[2])\npayload = b'' if sys.argv[3] == '-' else bytes.fromhex(sys.argv[3])\nwith socket.create_connection((host, port), timeout=2) as stream:\n    if payload:\n        stream.sendall(payload)\n    stream.shutdown(socket.SHUT_WR)\n    chunks = []\n    while True:\n        chunk = stream.recv(4096)\n        if not chunk:\n            break\n        chunks.append(chunk)\n    sys.stdout.buffer.write(b''.join(chunks))\nPY"
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
        assert!(
            command.contains("payload = b'' if sys.argv[3] == '-' else bytes.fromhex(sys.argv[3])")
        );
        assert!(command.ends_with("\nPY"));
    }

    #[test]
    fn remote_tcp_probe_command_preserves_empty_payload_argument() {
        let command = build_remote_tcp_probe_command("127.0.0.1", 9000, "");
        assert!(command.starts_with("python3 - 127.0.0.1 9000 - <<'PY'\n"));
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
}

use std::path::PathBuf;

use super::kubevirt::KubevirtWatchLaneSpec;
use super::{
    DEFAULT_KUBEVIRT_CONTROL_VM_NAME, DEFAULT_KUBEVIRT_HELPER_IMAGE,
    DEFAULT_KUBEVIRT_HELPER_POD_NAME, DEFAULT_KUBEVIRT_HELPER_STAGE_DIR,
    DEFAULT_KUBEVIRT_NAMESPACE, DEFAULT_KUBEVIRT_REPLICA1_VM_NAME,
    DEFAULT_KUBEVIRT_REPLICA2_VM_NAME, DEFAULT_KUBEVIRT_REPLICA3_VM_NAME,
    DEFAULT_WATCH_REFRESH_MILLIS,
};

#[derive(Debug)]
pub(super) enum ParsedCommand {
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

pub(super) fn parse_args(args: impl IntoIterator<Item = String>) -> Result<ParsedCommand, String> {
    let mut args = args.into_iter();
    let Some(subcommand) = args.next() else {
        return Ok(ParsedCommand::Help);
    };
    match subcommand.as_str() {
        "--help" | "-h" => Ok(ParsedCommand::Help),
        "plan" => {
            reject_trailing_args(args)?;
            Ok(ParsedCommand::Plan)
        }
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

pub(super) fn parse_watch_kubevirt_lane_spec(spec: &str) -> Result<KubevirtWatchLaneSpec, String> {
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

pub(super) fn usage() -> String {
    String::from(
        "usage:\n  allocdb-jepsen plan\n  allocdb-jepsen analyze --history-file <path>\n  allocdb-jepsen capture-kubevirt-layout --workspace <path> --namespace <name> --ssh-private-key <path> [--kubeconfig <path>] [--helper-pod <name>] [--helper-image <image>] [--helper-stage-dir <path>] [--control-vm <name>] [--replica-1-vm <name>] [--replica-2-vm <name>] [--replica-3-vm <name>]\n  allocdb-jepsen verify-qemu-surface --workspace <path>\n  allocdb-jepsen verify-kubevirt-surface --workspace <path>\n  allocdb-jepsen run-qemu --workspace <path> --run-id <run-id> --output-root <path>\n  allocdb-jepsen run-kubevirt --workspace <path> --run-id <run-id> --output-root <path>\n  allocdb-jepsen watch-kubevirt --workspace <path> --output-root <path> [--run-id <run-id>] [--refresh-millis <ms>] [--follow]\n  allocdb-jepsen watch-kubevirt-fleet --lane <name,workspace,output-root> [--lane <name,workspace,output-root> ...] [--refresh-millis <ms>] [--follow]\n  allocdb-jepsen archive-qemu --workspace <path> --run-id <run-id> --history-file <path> --output-root <path>\n  allocdb-jepsen archive-kubevirt --workspace <path> --run-id <run-id> --history-file <path> --output-root <path>\n",
    )
}

fn reject_trailing_args(args: impl IntoIterator<Item = String>) -> Result<(), String> {
    let trailing = args.into_iter().collect::<Vec<_>>();
    if trailing.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "unknown argument `{}`\n\n{}",
            trailing.join(" "),
            usage()
        ))
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

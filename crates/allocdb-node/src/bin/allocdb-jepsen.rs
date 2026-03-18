use std::process::ExitCode;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

#[path = "allocdb-jepsen/args.rs"]
mod args;
#[path = "allocdb-jepsen/cluster.rs"]
mod cluster;
#[path = "allocdb-jepsen/common.rs"]
mod common;
#[path = "allocdb-jepsen/events.rs"]
mod events;
#[path = "allocdb-jepsen/kubevirt.rs"]
mod kubevirt;
#[path = "allocdb-jepsen/nemesis.rs"]
mod nemesis;
#[path = "allocdb-jepsen/remote.rs"]
mod remote;
#[path = "allocdb-jepsen/runs.rs"]
mod runs;
#[path = "allocdb-jepsen/runtime.rs"]
mod runtime;
#[path = "allocdb-jepsen/scenarios.rs"]
mod scenarios;
#[path = "allocdb-jepsen/support.rs"]
mod support;
#[path = "allocdb-jepsen/surface.rs"]
mod surface;
#[path = "allocdb-jepsen/tracker.rs"]
mod tracker;
#[path = "allocdb-jepsen/watch.rs"]
mod watch;
#[path = "allocdb-jepsen/watch_render.rs"]
mod watch_render;

use args::{ParsedCommand, parse_args, usage};
use common::{
    append_text_line, current_time_millis, parse_optional_bool, parse_optional_path,
    parse_optional_string, parse_optional_u64, parse_optional_usize, parse_required_u32,
    parse_required_u64, parse_required_u128, required_field, unique_probe_resource_id,
    write_text_atomically,
};
use kubevirt::{CaptureKubevirtLayoutArgs, capture_kubevirt_layout};
use nemesis::{execute_mixed_failover_run, execute_partition_heal_run};
use remote::{
    build_remote_tcp_probe_command, decode_external_api_response, encode_hex, load_qemu_layout,
    sanitize_run_id, send_remote_api_request,
};
use runs::{archive_kubevirt_run, archive_qemu_run, run_kubevirt, run_qemu};
use scenarios::{execute_control_run, execute_crash_restart_run};
use support::RunExecutionContext;
use surface::{
    ExternalTestbed, analyze_history_file, print_release_gate_plan, verify_external_surface,
    verify_kubevirt_surface, verify_qemu_surface,
};
use watch::{watch_kubevirt, watch_kubevirt_fleet};

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

#[cfg(test)]
#[path = "allocdb-jepsen/tests.rs"]
mod tests;

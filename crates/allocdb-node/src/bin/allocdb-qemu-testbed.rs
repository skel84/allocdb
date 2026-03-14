use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Output};
use std::thread;
use std::time::{Duration, Instant};

use allocdb_node::qemu_testbed::{
    QemuGuestArch, QemuGuestConfig, QemuGuestKind, QemuTestbedConfig, QemuTestbedLayout,
    qemu_firmware_code_path, qemu_firmware_vars_template_path,
};
use base64::Engine as _;

const SSH_READY_TIMEOUT: Duration = Duration::from_secs(60);
const SSH_RETRY_INTERVAL: Duration = Duration::from_millis(500);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(20);
const PID_POLL_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SeedIsoTool {
    Hdiutil,
    Mkisofs(&'static str),
}

enum ParsedCommand {
    Help,
    Prepare {
        workspace_root: PathBuf,
        arch: Option<QemuGuestArch>,
        base_image_url: Option<String>,
        base_image_path: Option<PathBuf>,
        local_cluster_binary_path: Option<PathBuf>,
    },
    Start {
        workspace_root: PathBuf,
    },
    Stop {
        workspace_root: PathBuf,
    },
    Status {
        workspace_root: PathBuf,
    },
    SshControl {
        workspace_root: PathBuf,
    },
    Control {
        workspace_root: PathBuf,
        args: Vec<String>,
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
        ParsedCommand::Prepare {
            workspace_root,
            arch,
            base_image_url,
            base_image_path,
            local_cluster_binary_path,
        } => prepare_testbed(
            &workspace_root,
            arch,
            base_image_url,
            base_image_path,
            local_cluster_binary_path,
        ),
        ParsedCommand::Start { workspace_root } => start_testbed(&workspace_root),
        ParsedCommand::Stop { workspace_root } => stop_testbed(&workspace_root),
        ParsedCommand::Status { workspace_root } => status_testbed(&workspace_root),
        ParsedCommand::SshControl { workspace_root } => ssh_control(&workspace_root),
        ParsedCommand::Control {
            workspace_root,
            args,
        } => control_testbed(&workspace_root, &args),
    }
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<ParsedCommand, String> {
    let mut args = args.into_iter();
    let Some(subcommand) = args.next() else {
        return Ok(ParsedCommand::Help);
    };

    match subcommand.as_str() {
        "--help" | "-h" => Ok(ParsedCommand::Help),
        "prepare" => parse_prepare_args(args),
        "start" => Ok(ParsedCommand::Start {
            workspace_root: parse_workspace_flag(args)?,
        }),
        "stop" => Ok(ParsedCommand::Stop {
            workspace_root: parse_workspace_flag(args)?,
        }),
        "status" => Ok(ParsedCommand::Status {
            workspace_root: parse_workspace_flag(args)?,
        }),
        "ssh-control" => Ok(ParsedCommand::SshControl {
            workspace_root: parse_workspace_flag(args)?,
        }),
        "control" => parse_control_args(args),
        other => Err(format!("unknown subcommand `{other}`\n\n{}", usage())),
    }
}

fn parse_prepare_args(args: impl IntoIterator<Item = String>) -> Result<ParsedCommand, String> {
    let mut args = args.into_iter();
    let mut workspace_root = None;
    let mut arch = None;
    let mut base_image_url = None;
    let mut base_image_path = None;
    let mut local_cluster_binary_path = None;

    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--workspace" => {
                workspace_root = Some(PathBuf::from(args.next().ok_or_else(usage)?));
            }
            "--arch" => {
                let value = args.next().ok_or_else(usage)?;
                arch = Some(match value.as_str() {
                    "aarch64" => QemuGuestArch::Aarch64,
                    "x86_64" => QemuGuestArch::X86_64,
                    other => {
                        return Err(format!(
                            "invalid value for `--arch`: `{other}`\n\n{}",
                            usage()
                        ));
                    }
                });
            }
            "--base-image-url" => {
                base_image_url = Some(args.next().ok_or_else(usage)?);
            }
            "--base-image-path" => {
                base_image_path = Some(PathBuf::from(args.next().ok_or_else(usage)?));
            }
            "--local-cluster-bin" => {
                local_cluster_binary_path = Some(PathBuf::from(args.next().ok_or_else(usage)?));
            }
            "--help" | "-h" => return Err(usage()),
            other => return Err(format!("unknown argument `{other}`\n\n{}", usage())),
        }
    }

    Ok(ParsedCommand::Prepare {
        workspace_root: workspace_root.ok_or_else(usage)?,
        arch,
        base_image_url,
        base_image_path,
        local_cluster_binary_path,
    })
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

fn parse_control_args(args: impl IntoIterator<Item = String>) -> Result<ParsedCommand, String> {
    let mut args = args.into_iter();
    let mut workspace_root = None;
    let mut command_args = Vec::new();
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--workspace" => {
                workspace_root = Some(PathBuf::from(args.next().ok_or_else(usage)?));
            }
            "--" => {
                command_args.extend(args);
                break;
            }
            "--help" | "-h" => return Err(usage()),
            other if workspace_root.is_some() => command_args.push(String::from(other)),
            other => return Err(format!("unknown argument `{other}`\n\n{}", usage())),
        }
    }

    if command_args.is_empty() {
        return Err(format!("missing control command\n\n{}", usage()));
    }

    Ok(ParsedCommand::Control {
        workspace_root: workspace_root.ok_or_else(usage)?,
        args: command_args,
    })
}

fn usage() -> String {
    String::from(
        "usage:\n  allocdb-qemu-testbed prepare --workspace <path> [--arch <aarch64|x86_64>] [--base-image-url <url>] [--base-image-path <path>] [--local-cluster-bin <path>]\n  allocdb-qemu-testbed start --workspace <path>\n  allocdb-qemu-testbed stop --workspace <path>\n  allocdb-qemu-testbed status --workspace <path>\n  allocdb-qemu-testbed ssh-control --workspace <path>\n  allocdb-qemu-testbed control --workspace <path> -- <status|isolate|heal|crash|restart|reboot|export-replica|import-replica|collect-logs> ...\n",
    )
}

fn prepare_testbed(
    workspace_root: &Path,
    arch: Option<QemuGuestArch>,
    base_image_url: Option<String>,
    base_image_path: Option<PathBuf>,
    local_cluster_binary_path: Option<PathBuf>,
) -> Result<(), String> {
    let arch = arch.unwrap_or_else(QemuGuestArch::from_host);
    let base_image_url = base_image_url.unwrap_or_else(|| String::from(arch.default_image_url()));
    let base_image_path = base_image_path.unwrap_or_else(|| {
        workspace_root
            .join("qemu")
            .join("images")
            .join(image_file_name(&base_image_url))
    });
    let local_cluster_binary_path = local_cluster_binary_path
        .unwrap_or_else(|| PathBuf::from("target/release/allocdb-local-cluster"));

    let layout = QemuTestbedLayout::new(QemuTestbedConfig {
        workspace_root: workspace_root.to_path_buf(),
        arch,
        base_image_url,
        base_image_path,
        local_cluster_binary_path,
    })
    .map_err(|error| format!("failed to build qemu layout: {error}"))?;

    prepare_runtime_assets(&layout)?;
    layout
        .persist()
        .map_err(|error| format!("failed to persist qemu layout: {error}"))?;

    println!("qemu testbed prepared");
    println!("workspace={}", layout.config.workspace_root.display());
    println!("layout={}", layout.layout_path().display());
    println!("base_image={}", layout.config.base_image_path.display());
    println!(
        "control_ssh=ssh -i {} -p 2220 allocdb@127.0.0.1",
        layout.ssh_private_key_path().display()
    );
    Ok(())
}

fn start_testbed(workspace_root: &Path) -> Result<(), String> {
    let layout = load_existing_layout(workspace_root)?;
    ensure_testbed_not_running(&layout)?;
    clear_stale_pid_files(&layout)?;
    prepare_runtime_assets(&layout)?;
    start_prepared_testbed(&layout, spawn_guest, wait_for_control_guest_ssh, stop_guest)?;
    println!("qemu testbed started");
    println!("workspace={}", layout.config.workspace_root.display());
    println!("layout={}", layout.layout_path().display());
    println!(
        "control_ssh=ssh -i {} -p 2220 allocdb@127.0.0.1",
        layout.ssh_private_key_path().display()
    );
    Ok(())
}

fn start_prepared_testbed<FSpawn, FWait, FStop>(
    layout: &QemuTestbedLayout,
    mut spawn_guest_fn: FSpawn,
    mut wait_for_control_guest_ssh_fn: FWait,
    mut stop_guest_fn: FStop,
) -> Result<(), String>
where
    FSpawn: FnMut(&QemuTestbedLayout, &QemuGuestConfig) -> Result<(), String>,
    FWait: FnMut(&QemuTestbedLayout) -> Result<(), String>,
    FStop: FnMut(&QemuGuestConfig) -> Result<(), String>,
{
    let mut started_guests = Vec::new();
    for guest in std::iter::once(&layout.control_guest).chain(layout.replica_guests.iter()) {
        if let Err(error) = spawn_guest_fn(layout, guest) {
            rollback_started_guests(&started_guests, &mut stop_guest_fn);
            return Err(error);
        }
        started_guests.push(guest);
    }
    if let Err(error) = wait_for_control_guest_ssh_fn(layout) {
        rollback_started_guests(&started_guests, &mut stop_guest_fn);
        return Err(error);
    }
    Ok(())
}

fn rollback_started_guests<F>(started_guests: &[&QemuGuestConfig], stop_guest_fn: &mut F)
where
    F: FnMut(&QemuGuestConfig) -> Result<(), String>,
{
    for guest in started_guests.iter().rev() {
        let _ = stop_guest_fn(guest);
    }
}

fn stop_testbed(workspace_root: &Path) -> Result<(), String> {
    let layout = load_existing_layout(workspace_root)?;
    for guest in std::iter::once(&layout.control_guest).chain(layout.replica_guests.iter()) {
        stop_guest(guest)?;
    }
    println!("qemu testbed stopped");
    Ok(())
}

fn status_testbed(workspace_root: &Path) -> Result<(), String> {
    let layout = load_existing_layout(workspace_root)?;
    println!("workspace={}", layout.config.workspace_root.display());
    println!("layout={}", layout.layout_path().display());
    println!("arch={}", layout.config.arch.as_str());
    println!("base_image={}", layout.config.base_image_path.display());
    for guest in std::iter::once(&layout.control_guest).chain(layout.replica_guests.iter()) {
        println!(
            "guest={} kind={} state={} pid_path={} console={}",
            guest.name,
            match guest.kind {
                QemuGuestKind::Control => "control",
                QemuGuestKind::Replica => "replica",
            },
            if guest_process_running(guest)? {
                "running"
            } else {
                "stopped"
            },
            guest.pid_path.display(),
            guest.serial_log_path.display()
        );
    }
    match run_control_command_capture(&layout, &["status"]) {
        Ok(output) => {
            print!("{output}");
        }
        Err(error) => {
            println!("control_status=unavailable reason={error}");
        }
    }
    Ok(())
}

fn ssh_control(workspace_root: &Path) -> Result<(), String> {
    let layout = load_existing_layout(workspace_root)?;
    let status = Command::new("ssh")
        .args(ssh_args(&layout))
        .status()
        .map_err(|error| format!("failed to launch ssh: {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("ssh exited with status {status}"))
    }
}

fn control_testbed(workspace_root: &Path, args: &[String]) -> Result<(), String> {
    let layout = load_existing_layout(workspace_root)?;
    let status = run_control_command_status(&layout, args)?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("control command exited with status {status}"))
    }
}

fn prepare_runtime_assets(layout: &QemuTestbedLayout) -> Result<(), String> {
    ensure_command_on_path(layout.config.arch.qemu_binary())?;
    ensure_command_on_path("qemu-img")?;
    detect_seed_iso_tool()?;
    ensure_command_on_path("ssh")?;
    ensure_command_on_path("ssh-keygen")?;

    ensure_firmware_template(layout)?;
    ensure_runtime_dirs(layout)?;
    ensure_base_image(layout)?;
    ensure_local_cluster_binary(layout)?;
    ensure_ssh_key_pair(layout)?;
    ensure_guest_firmware_vars(layout)?;
    ensure_overlay_images(layout)?;
    write_seed_assets(layout)?;
    Ok(())
}

fn ensure_command_on_path(command: &str) -> Result<(), String> {
    if command_on_path(command)? {
        Ok(())
    } else {
        Err(format!("required command `{command}` is not on PATH"))
    }
}

fn command_on_path(command: &str) -> Result<bool, String> {
    let status = Command::new("sh")
        .arg("-lc")
        .arg(format!("command -v {command} >/dev/null 2>&1"))
        .status()
        .map_err(|error| format!("failed to check command `{command}`: {error}"))?;
    Ok(status.success())
}

fn detect_seed_iso_tool() -> Result<SeedIsoTool, String> {
    if cfg!(target_os = "macos") {
        ensure_command_on_path("hdiutil")?;
        Ok(SeedIsoTool::Hdiutil)
    } else {
        for command in ["mkisofs", "genisoimage"] {
            if command_on_path(command)? {
                return Ok(SeedIsoTool::Mkisofs(command));
            }
        }
        Err(String::from(
            "required seed ISO builder is not on PATH; install `mkisofs` or `genisoimage`",
        ))
    }
}

fn ensure_firmware_template(layout: &QemuTestbedLayout) -> Result<(), String> {
    let code_path = firmware_code_template_path(layout);
    let vars_path = firmware_vars_template_path(layout);
    if !code_path.exists() {
        return Err(format!(
            "missing firmware code image {}",
            code_path.display()
        ));
    }
    if !vars_path.exists() {
        return Err(format!(
            "missing firmware vars template {}",
            vars_path.display()
        ));
    }
    Ok(())
}

fn ensure_runtime_dirs(layout: &QemuTestbedLayout) -> Result<(), String> {
    fs::create_dir_all(layout.qemu_root().join("images"))
        .map_err(|error| format!("failed to create image directory: {error}"))?;
    fs::create_dir_all(layout.qemu_root().join("run"))
        .map_err(|error| format!("failed to create runtime directory: {error}"))?;
    fs::create_dir_all(layout.qemu_root().join("seed"))
        .map_err(|error| format!("failed to create seed directory: {error}"))?;
    fs::create_dir_all(layout.qemu_root().join("logs"))
        .map_err(|error| format!("failed to create log directory: {error}"))?;
    fs::create_dir_all(layout.qemu_root().join("firmware"))
        .map_err(|error| format!("failed to create firmware directory: {error}"))?;
    fs::create_dir_all(layout.qemu_root().join("ssh"))
        .map_err(|error| format!("failed to create ssh directory: {error}"))?;
    Ok(())
}

fn ensure_base_image(layout: &QemuTestbedLayout) -> Result<(), String> {
    if layout.config.base_image_path.exists() {
        return Ok(());
    }
    ensure_command_on_path("curl")?;
    let parent = layout.config.base_image_path.parent().ok_or_else(|| {
        format!(
            "base image path {} has no parent directory",
            layout.config.base_image_path.display()
        )
    })?;
    fs::create_dir_all(parent)
        .map_err(|error| format!("failed to create base image directory: {error}"))?;
    let temp_path = base_image_download_path(&layout.config.base_image_path);
    let _ = fs::remove_file(&temp_path);
    let status = Command::new("curl")
        .args([
            "-L",
            "--fail",
            "--output",
            &temp_path.display().to_string(),
            &layout.config.base_image_url,
        ])
        .status()
        .map_err(|error| {
            let _ = fs::remove_file(&temp_path);
            format!("failed to start curl: {error}")
        })?;
    if !status.success() {
        let _ = fs::remove_file(&temp_path);
        return Err(format!(
            "curl failed while downloading base image from {}",
            layout.config.base_image_url
        ));
    }
    fs::rename(&temp_path, &layout.config.base_image_path).map_err(|error| {
        let _ = fs::remove_file(&temp_path);
        format!(
            "failed to move downloaded base image into place at {}: {error}",
            layout.config.base_image_path.display()
        )
    })
}

fn base_image_download_path(base_image_path: &Path) -> PathBuf {
    let file_name = base_image_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("base-image");
    base_image_path.with_file_name(format!("{file_name}.download-{}", std::process::id()))
}

fn ensure_local_cluster_binary(layout: &QemuTestbedLayout) -> Result<(), String> {
    if layout.config.local_cluster_binary_path.exists() {
        Ok(())
    } else {
        Err(format!(
            "missing local-cluster binary {}; build it first with `cargo build --release -p allocdb-node --bin allocdb-local-cluster` or pass `--local-cluster-bin`",
            layout.config.local_cluster_binary_path.display()
        ))
    }
}

fn ensure_ssh_key_pair(layout: &QemuTestbedLayout) -> Result<(), String> {
    if layout.ssh_private_key_path().exists() && layout.ssh_public_key_path().exists() {
        return Ok(());
    }
    let status = Command::new("ssh-keygen")
        .args([
            "-t",
            "ed25519",
            "-N",
            "",
            "-f",
            &layout.ssh_private_key_path().display().to_string(),
        ])
        .status()
        .map_err(|error| format!("failed to run ssh-keygen: {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(String::from("ssh-keygen failed"))
    }
}

fn ensure_guest_firmware_vars(layout: &QemuTestbedLayout) -> Result<(), String> {
    let vars_template = firmware_vars_template_path(layout);
    for guest in std::iter::once(&layout.control_guest).chain(layout.replica_guests.iter()) {
        if guest.firmware_vars_path.exists() {
            continue;
        }
        fs::copy(&vars_template, &guest.firmware_vars_path).map_err(|error| {
            format!(
                "failed to copy firmware vars template to {}: {error}",
                guest.firmware_vars_path.display()
            )
        })?;
    }
    Ok(())
}

fn ensure_overlay_images(layout: &QemuTestbedLayout) -> Result<(), String> {
    let base_image_format = detect_qemu_image_format(&layout.config.base_image_path)?;
    for guest in std::iter::once(&layout.control_guest).chain(layout.replica_guests.iter()) {
        if guest.overlay_path.exists() {
            continue;
        }
        let status = Command::new("qemu-img")
            .args([
                "create",
                "-f",
                "qcow2",
                "-b",
                &layout.config.base_image_path.display().to_string(),
                "-F",
                &base_image_format,
                &guest.overlay_path.display().to_string(),
            ])
            .status()
            .map_err(|error| format!("failed to run qemu-img: {error}"))?;
        if !status.success() {
            return Err(format!(
                "qemu-img failed while creating {}",
                guest.overlay_path.display()
            ));
        }
    }
    Ok(())
}

fn detect_qemu_image_format(path: &Path) -> Result<String, String> {
    let output = Command::new("qemu-img")
        .args(["info", &path.display().to_string()])
        .output()
        .map_err(|error| {
            format!(
                "failed to run `qemu-img info` for {}: {error}",
                path.display()
            )
        })?;
    if !output.status.success() {
        return Err(format_command_failure(
            "qemu-img info",
            path,
            &output,
            "failed to inspect base image format",
        ));
    }
    parse_qemu_image_format(&String::from_utf8_lossy(&output.stdout)).ok_or_else(|| {
        format!(
            "failed to determine base image format from `qemu-img info {}` output",
            path.display()
        )
    })
}

fn write_seed_assets(layout: &QemuTestbedLayout) -> Result<(), String> {
    let binary_bytes = fs::read(&layout.config.local_cluster_binary_path).map_err(|error| {
        format!(
            "failed to read local-cluster binary {}: {error}",
            layout.config.local_cluster_binary_path.display()
        )
    })?;
    let binary_b64 = base64::engine::general_purpose::STANDARD.encode(binary_bytes);
    let ssh_public_key = fs::read_to_string(layout.ssh_public_key_path()).map_err(|error| {
        format!(
            "failed to read ssh public key {}: {error}",
            layout.ssh_public_key_path().display()
        )
    })?;
    let ssh_private_key = fs::read(layout.ssh_private_key_path()).map_err(|error| {
        format!(
            "failed to read ssh private key {}: {error}",
            layout.ssh_private_key_path().display()
        )
    })?;
    let ssh_private_key_b64 = base64::engine::general_purpose::STANDARD.encode(ssh_private_key);

    let control_meta_data = layout.render_control_guest_meta_data();
    let control_network_config = layout.render_control_guest_network_config();
    let control_user_data =
        layout.render_control_guest_user_data(&binary_b64, &ssh_public_key, &ssh_private_key_b64);
    write_guest_seed(
        &layout.control_guest,
        &control_meta_data,
        &control_network_config,
        &control_user_data,
    )?;
    for guest in &layout.replica_guests {
        let replica_meta_data = layout.render_replica_guest_meta_data(guest);
        let replica_network_config = layout.render_replica_guest_network_config(guest);
        let replica_user_data =
            layout.render_replica_guest_user_data(guest, &binary_b64, &ssh_public_key);
        write_guest_seed(
            guest,
            &replica_meta_data,
            &replica_network_config,
            &replica_user_data,
        )?;
    }
    Ok(())
}

fn write_guest_seed(
    guest: &QemuGuestConfig,
    meta_data: &str,
    network_config: &str,
    user_data: &str,
) -> Result<(), String> {
    fs::create_dir_all(&guest.seed_dir).map_err(|error| {
        format!(
            "failed to create seed directory {}: {error}",
            guest.seed_dir.display()
        )
    })?;
    write_text_file(&guest.seed_dir.join("meta-data"), meta_data)?;
    write_text_file(&guest.seed_dir.join("network-config"), network_config)?;
    write_text_file(&guest.seed_dir.join("user-data"), user_data)?;
    create_seed_iso(&guest.seed_dir, &guest.seed_iso_path)
}

fn write_text_file(path: &Path, contents: &str) -> Result<(), String> {
    let mut file = File::create(path)
        .map_err(|error| format!("failed to create {}: {error}", path.display()))?;
    file.write_all(contents.as_bytes())
        .map_err(|error| format!("failed to write {}: {error}", path.display()))
}

fn create_seed_iso(seed_dir: &Path, seed_iso_path: &Path) -> Result<(), String> {
    if seed_iso_path.exists() {
        fs::remove_file(seed_iso_path).map_err(|error| {
            format!(
                "failed to remove previous seed image {}: {error}",
                seed_iso_path.display()
            )
        })?;
    }
    let temp_output = seed_iso_path.with_extension("seedtmp");
    let _ = fs::remove_file(&temp_output);
    let produced_path = match detect_seed_iso_tool()? {
        SeedIsoTool::Hdiutil => {
            create_seed_iso_with_hdiutil(seed_dir, seed_iso_path, &temp_output)?
        }
        SeedIsoTool::Mkisofs(command) => {
            create_seed_iso_with_mkisofs(command, seed_dir, seed_iso_path, &temp_output)?
        }
    };
    fs::rename(&produced_path, seed_iso_path).map_err(|error| {
        format!(
            "failed to move {} to {}: {error}",
            produced_path.display(),
            seed_iso_path.display()
        )
    })?;
    Ok(())
}

fn create_seed_iso_with_hdiutil(
    seed_dir: &Path,
    seed_iso_path: &Path,
    temp_output: &Path,
) -> Result<PathBuf, String> {
    let output = Command::new("hdiutil")
        .args([
            "makehybrid",
            "-o",
            &temp_output.display().to_string(),
            &seed_dir.display().to_string(),
            "-iso",
            "-joliet",
            "-default-volume-name",
            "cidata",
        ])
        .output()
        .map_err(|error| format!("failed to run hdiutil: {error}"))?;
    if !output.status.success() {
        return Err(format_command_failure(
            "hdiutil makehybrid",
            seed_iso_path,
            &output,
            "hdiutil failed while building seed image",
        ));
    }
    find_hdiutil_output(temp_output)?.ok_or_else(|| {
        format!(
            "hdiutil did not produce a seed image for {}",
            seed_iso_path.display()
        )
    })
}

fn create_seed_iso_with_mkisofs(
    command: &str,
    seed_dir: &Path,
    seed_iso_path: &Path,
    temp_output: &Path,
) -> Result<PathBuf, String> {
    let output = Command::new(command)
        .args([
            "-output",
            &temp_output.display().to_string(),
            "-volid",
            "cidata",
            "-joliet",
            "-rock",
            &seed_dir.display().to_string(),
        ])
        .output()
        .map_err(|error| format!("failed to run {command}: {error}"))?;
    if !output.status.success() {
        return Err(format_command_failure(
            command,
            seed_iso_path,
            &output,
            "seed image build failed",
        ));
    }
    Ok(temp_output.to_path_buf())
}

fn find_hdiutil_output(temp_output: &Path) -> Result<Option<PathBuf>, String> {
    let file_name = temp_output
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| format!("invalid temporary seed output {}", temp_output.display()))?;
    let parent = temp_output.parent().ok_or_else(|| {
        format!(
            "temporary seed output {} has no parent directory",
            temp_output.display()
        )
    })?;

    let mut matches = fs::read_dir(parent)
        .map_err(|error| format!("failed to scan {}: {error}", parent.display()))?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|value| value.to_str())
                .is_some_and(|value| value.starts_with(file_name))
        })
        .collect::<Vec<_>>();
    matches.sort();
    Ok(matches.into_iter().next())
}

fn format_command_failure(command: &str, path: &Path, output: &Output, context: &str) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stderr = stderr.trim();
    if stderr.is_empty() {
        format!("{context} for {} via `{command}`", path.display())
    } else {
        format!("{context} for {} via `{command}`: {stderr}", path.display())
    }
}

fn parse_qemu_image_format(output: &str) -> Option<String> {
    output.lines().find_map(|line| {
        line.trim()
            .strip_prefix("file format:")
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(String::from)
    })
}

fn ensure_testbed_not_running(layout: &QemuTestbedLayout) -> Result<(), String> {
    for guest in std::iter::once(&layout.control_guest).chain(layout.replica_guests.iter()) {
        if guest_process_running(guest)? {
            return Err(format!("guest {} is already running", guest.name));
        }
    }
    Ok(())
}

fn clear_stale_pid_files(layout: &QemuTestbedLayout) -> Result<(), String> {
    for guest in std::iter::once(&layout.control_guest).chain(layout.replica_guests.iter()) {
        if guest.pid_path.exists() && !guest_process_running(guest)? {
            fs::remove_file(&guest.pid_path).map_err(|error| {
                format!(
                    "failed to remove stale pid file {}: {error}",
                    guest.pid_path.display()
                )
            })?;
        }
    }
    Ok(())
}

fn spawn_guest(layout: &QemuTestbedLayout, guest: &QemuGuestConfig) -> Result<(), String> {
    let command_argv = layout.qemu_command(guest);
    let (program, program_args) = command_argv
        .split_first()
        .ok_or_else(|| format!("missing qemu argv for guest {}", guest.name))?;
    let status = Command::new(program)
        .args(program_args)
        .status()
        .map_err(|error| format!("failed to launch {}: {error}", guest.name))?;
    if !status.success() {
        return Err(format!(
            "qemu exited with status {} while starting {}",
            status, guest.name
        ));
    }
    Ok(())
}

fn wait_for_control_guest_ssh(layout: &QemuTestbedLayout) -> Result<(), String> {
    let started_at = Instant::now();
    while started_at.elapsed() < SSH_READY_TIMEOUT {
        if run_control_ssh(layout, &["true"], false).is_ok() {
            return Ok(());
        }
        thread::sleep(SSH_RETRY_INTERVAL);
    }
    Err(String::from(
        "control guest ssh did not become ready within timeout",
    ))
}

fn stop_guest(guest: &QemuGuestConfig) -> Result<(), String> {
    if !guest.pid_path.exists() {
        return Ok(());
    }
    let process_id = read_pid_file(&guest.pid_path)?;
    if !process_exists(process_id)? {
        fs::remove_file(&guest.pid_path).map_err(|error| {
            format!(
                "failed to remove stale pid file {}: {error}",
                guest.pid_path.display()
            )
        })?;
        return Ok(());
    }
    send_signal(process_id, "-TERM")?;
    let started_at = Instant::now();
    while started_at.elapsed() < SHUTDOWN_TIMEOUT {
        if !process_exists(process_id)? {
            fs::remove_file(&guest.pid_path).map_err(|error| {
                format!(
                    "failed to remove pid file {}: {error}",
                    guest.pid_path.display()
                )
            })?;
            return Ok(());
        }
        thread::sleep(PID_POLL_INTERVAL);
    }
    send_signal(process_id, "-KILL")?;
    if guest.pid_path.exists() {
        fs::remove_file(&guest.pid_path).map_err(|error| {
            format!(
                "failed to remove pid file {}: {error}",
                guest.pid_path.display()
            )
        })?;
    }
    Ok(())
}

fn guest_process_running(guest: &QemuGuestConfig) -> Result<bool, String> {
    if !guest.pid_path.exists() {
        return Ok(false);
    }
    process_exists(read_pid_file(&guest.pid_path)?)
}

fn read_pid_file(path: &Path) -> Result<u32, String> {
    let bytes = fs::read_to_string(path)
        .map_err(|error| format!("failed to read pid file {}: {error}", path.display()))?;
    bytes
        .trim()
        .parse::<u32>()
        .map_err(|error| format!("invalid pid in {}: {error}", path.display()))
}

fn process_exists(process_id: u32) -> Result<bool, String> {
    let status = Command::new("kill")
        .args(["-0", &process_id.to_string()])
        .status()
        .map_err(|error| format!("failed to probe process {process_id}: {error}"))?;
    Ok(status.success())
}

fn send_signal(process_id: u32, signal_flag: &str) -> Result<(), String> {
    let status = Command::new("kill")
        .args([signal_flag, &process_id.to_string()])
        .status()
        .map_err(|error| format!("failed to signal process {process_id}: {error}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!(
            "kill {signal_flag} {process_id} exited with status {status}"
        ))
    }
}

fn load_existing_layout(workspace_root: &Path) -> Result<QemuTestbedLayout, String> {
    let canonical = fs::canonicalize(workspace_root).map_err(|error| {
        format!(
            "failed to resolve workspace {}: {error}",
            workspace_root.display()
        )
    })?;
    QemuTestbedLayout::load(canonical.join("qemu-testbed-layout.txt"))
        .map_err(|error| format!("failed to load qemu layout: {error}"))
}

fn run_control_command_capture(
    layout: &QemuTestbedLayout,
    args: &[&str],
) -> Result<String, String> {
    let mut remote_args = vec!["sudo", "/usr/local/bin/allocdb-qemu-control"];
    remote_args.extend_from_slice(args);
    let output = run_control_ssh(layout, &remote_args, true)?;
    if output.status.success() {
        String::from_utf8(output.stdout).map_err(|error| format!("invalid utf-8 from ssh: {error}"))
    } else {
        Err(format!("control ssh failed with status {}", output.status))
    }
}

fn run_control_command_status(
    layout: &QemuTestbedLayout,
    args: &[String],
) -> Result<std::process::ExitStatus, String> {
    let mut ssh_args = ssh_args(layout);
    ssh_args.push(String::from("sudo"));
    ssh_args.push(String::from("/usr/local/bin/allocdb-qemu-control"));
    ssh_args.extend(args.iter().cloned());
    Command::new("ssh")
        .args(ssh_args)
        .status()
        .map_err(|error| format!("failed to run control command: {error}"))
}

fn run_control_ssh(
    layout: &QemuTestbedLayout,
    remote_args: &[&str],
    capture: bool,
) -> Result<std::process::Output, String> {
    let mut args = ssh_args(layout);
    for argument in remote_args {
        args.push(String::from(*argument));
    }
    let mut command = Command::new("ssh");
    command.args(args);
    if capture {
        command
            .output()
            .map_err(|error| format!("failed to run ssh: {error}"))
    } else {
        let status = command
            .status()
            .map_err(|error| format!("failed to run ssh: {error}"))?;
        Ok(std::process::Output {
            status,
            stdout: Vec::new(),
            stderr: Vec::new(),
        })
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
        String::from("2220"),
        String::from("allocdb@127.0.0.1"),
    ]
}

fn image_file_name(url: &str) -> String {
    url.rsplit('/')
        .next()
        .filter(|segment| !segment.is_empty())
        .map_or_else(|| String::from("base-cloudimg.img"), String::from)
}

fn firmware_code_template_path(layout: &QemuTestbedLayout) -> PathBuf {
    qemu_firmware_code_path(layout.config.arch, Some(&layout.config.base_image_path))
}

fn firmware_vars_template_path(layout: &QemuTestbedLayout) -> PathBuf {
    qemu_firmware_vars_template_path(layout.config.arch, Some(&layout.config.base_image_path))
}

#[cfg(test)]
mod tests {
    use super::{parse_qemu_image_format, start_prepared_testbed};
    use allocdb_node::qemu_testbed::{QemuGuestArch, QemuTestbedConfig, QemuTestbedLayout};
    use std::cell::{Cell, RefCell};
    use std::collections::HashSet;
    use std::path::PathBuf;

    fn fixture_layout() -> QemuTestbedLayout {
        QemuTestbedLayout::new(QemuTestbedConfig {
            workspace_root: std::env::temp_dir().join("allocdb-qemu-testbed-start-prepared"),
            arch: QemuGuestArch::Aarch64,
            base_image_url: String::from(
                "https://cloud-images.ubuntu.com/releases/24.04/release/ubuntu-24.04-server-cloudimg-arm64.img",
            ),
            base_image_path: PathBuf::from("/tmp/base-cloudimg.img"),
            local_cluster_binary_path: PathBuf::from("/tmp/allocdb-local-cluster"),
        })
        .expect("fixture layout")
    }

    #[test]
    fn parse_qemu_image_format_extracts_format_line() {
        let output = "image: base.img\nfile format: raw\nvirtual size: 64 MiB (67108864 bytes)\n";
        assert_eq!(parse_qemu_image_format(output).as_deref(), Some("raw"));
    }

    #[test]
    fn start_prepared_testbed_rolls_back_spawn_failure_and_allows_retry() {
        let layout = fixture_layout();
        let running = RefCell::new(HashSet::new());
        let stopped = RefCell::new(Vec::new());
        let fail_once = Cell::new(true);
        let fail_guest = layout.replica_guests[0].name.clone();

        let first_error = start_prepared_testbed(
            &layout,
            |_, guest| {
                if guest.name == fail_guest && fail_once.replace(false) {
                    return Err(String::from("spawn failed"));
                }
                running.borrow_mut().insert(guest.name.clone());
                Ok(())
            },
            |_| Ok(()),
            |guest| {
                running.borrow_mut().remove(&guest.name);
                stopped.borrow_mut().push(guest.name.clone());
                Ok(())
            },
        )
        .unwrap_err();

        assert_eq!(first_error, "spawn failed");
        assert!(running.borrow().is_empty());
        assert_eq!(
            stopped.borrow().as_slice(),
            std::slice::from_ref(&layout.control_guest.name)
        );

        let retry_result = start_prepared_testbed(
            &layout,
            |_, guest| {
                running.borrow_mut().insert(guest.name.clone());
                Ok(())
            },
            |_| Ok(()),
            |guest| {
                running.borrow_mut().remove(&guest.name);
                Ok(())
            },
        );
        assert!(retry_result.is_ok());
        assert_eq!(running.borrow().len(), 4);
    }

    #[test]
    fn start_prepared_testbed_rolls_back_readiness_failure_and_allows_retry() {
        let layout = fixture_layout();
        let running = RefCell::new(HashSet::new());
        let stopped = RefCell::new(Vec::new());
        let wait_fail_once = Cell::new(true);

        let first_error = start_prepared_testbed(
            &layout,
            |_, guest| {
                running.borrow_mut().insert(guest.name.clone());
                Ok(())
            },
            |_| {
                if wait_fail_once.replace(false) {
                    Err(String::from(
                        "control guest ssh did not become ready within timeout",
                    ))
                } else {
                    Ok(())
                }
            },
            |guest| {
                running.borrow_mut().remove(&guest.name);
                stopped.borrow_mut().push(guest.name.clone());
                Ok(())
            },
        )
        .unwrap_err();

        assert_eq!(
            first_error,
            "control guest ssh did not become ready within timeout"
        );
        assert!(running.borrow().is_empty());
        assert_eq!(stopped.borrow().len(), 4);

        let retry_result = start_prepared_testbed(
            &layout,
            |_, guest| {
                running.borrow_mut().insert(guest.name.clone());
                Ok(())
            },
            |_| Ok(()),
            |guest| {
                running.borrow_mut().remove(&guest.name);
                Ok(())
            },
        );
        assert!(retry_result.is_ok());
        assert_eq!(running.borrow().len(), 4);
    }
}

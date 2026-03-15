use std::io::Write;
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use allocdb_node::ReplicaId;
use allocdb_node::kubevirt_testbed::{
    KubevirtGuestConfig, KubevirtTestbedConfig, KubevirtTestbedLayout, kubevirt_testbed_layout_path,
};

use crate::{DEFAULT_GUEST_USER, build_remote_tcp_probe_command, encode_hex};

pub(super) struct KubevirtHelperGuard {
    kubeconfig_path: Option<PathBuf>,
    namespace: String,
    pod_name: String,
    delete_on_drop: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct KubevirtWatchLaneSpec {
    pub(super) name: String,
    pub(super) workspace_root: PathBuf,
    pub(super) output_root: PathBuf,
}

pub(super) struct KubevirtWatchLaneContext {
    pub(super) spec: KubevirtWatchLaneSpec,
    pub(super) layout: KubevirtTestbedLayout,
    pub(super) _helper: KubevirtHelperGuard,
}

pub(super) struct CaptureKubevirtLayoutArgs<'a> {
    pub(super) workspace_root: &'a Path,
    pub(super) kubeconfig_path: Option<&'a Path>,
    pub(super) namespace: &'a str,
    pub(super) helper_pod_name: &'a str,
    pub(super) helper_image: &'a str,
    pub(super) helper_stage_dir: &'a Path,
    pub(super) ssh_private_key_path: &'a Path,
    pub(super) control_vm_name: &'a str,
    pub(super) replica_vm_names: &'a [String; 3],
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

pub(super) fn kubectl_base_args(kubeconfig_path: Option<&Path>, namespace: &str) -> Vec<String> {
    let mut args = Vec::new();
    if let Some(kubeconfig_path) = kubeconfig_path {
        args.push(String::from("--kubeconfig"));
        args.push(kubeconfig_path.display().to_string());
    }
    args.push(String::from("-n"));
    args.push(String::from(namespace));
    args
}

pub(super) fn capture_kubevirt_layout(args: &CaptureKubevirtLayoutArgs<'_>) -> Result<(), String> {
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

pub(super) fn load_kubevirt_layout(workspace_root: &Path) -> Result<KubevirtTestbedLayout, String> {
    let path = kubevirt_testbed_layout_path(workspace_root);
    KubevirtTestbedLayout::load(path)
        .map_err(|error| format!("failed to load kubevirt testbed layout: {error}"))
}

pub(super) fn prepare_kubevirt_helper(
    layout: &KubevirtTestbedLayout,
) -> Result<KubevirtHelperGuard, String> {
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

pub(super) fn run_kubevirt_remote_host_command(
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

pub(super) fn run_kubevirt_remote_tcp_request(
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

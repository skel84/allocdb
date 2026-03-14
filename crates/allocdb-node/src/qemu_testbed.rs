use std::fmt::{self, Write as _};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::{Path, PathBuf};

use crate::local_cluster::{
    LocalClusterLayout, LocalClusterReplicaConfig, default_local_cluster_core_config,
    default_local_cluster_engine_config, encode_layout_text,
};
use crate::replica::{ReplicaId, ReplicaPaths, ReplicaRole};

const QEMU_TESTBED_LAYOUT_VERSION: u32 = 1;
const QEMU_TESTBED_LAYOUT_FILE_NAME: &str = "qemu-testbed-layout.txt";
const QEMU_TESTBED_DIR_NAME: &str = "qemu";
const QEMU_SSH_DIR_NAME: &str = "ssh";
const CONTROL_GUEST_NAME: &str = "allocdb-control";
const CONTROL_GUEST_HOST_SSH_PORT: u16 = 2220;
const CONTROL_GUEST_MANAGEMENT_IP: Ipv4Addr = Ipv4Addr::new(172, 31, 0, 10);
const REPLICA_MANAGEMENT_IPS: [Ipv4Addr; 3] = [
    Ipv4Addr::new(172, 31, 0, 11),
    Ipv4Addr::new(172, 31, 0, 12),
    Ipv4Addr::new(172, 31, 0, 13),
];
const REPLICA_CLUSTER_IPS: [Ipv4Addr; 3] = [
    Ipv4Addr::new(172, 31, 1, 11),
    Ipv4Addr::new(172, 31, 1, 12),
    Ipv4Addr::new(172, 31, 1, 13),
];
const CONTROL_LISTENER_PORT: u16 = 17_000;
const CLIENT_LISTENER_PORT: u16 = 18_000;
const PROTOCOL_LISTENER_PORT: u16 = 19_000;
const MANAGEMENT_NETWORK_MCAST: &str = "230.55.0.10:45000";
const CLUSTER_NETWORK_MCAST: &str = "230.55.0.11:45001";
const GUEST_WORKSPACE_ROOT: &str = "/var/lib/allocdb";
const GUEST_LAYOUT_PATH: &str = "/var/lib/allocdb/cluster-layout.txt";
const GUEST_QEMU_CONTROL_HOME: &str = "/var/lib/allocdb-qemu";
const GUEST_QEMU_CONTROL_SCRIPT_PATH: &str = "/usr/local/bin/allocdb-qemu-control";
const GUEST_LOCAL_CLUSTER_BIN_PATH: &str = "/usr/local/bin/allocdb-local-cluster";
const GUEST_ALLOCDB_USER: &str = "allocdb";
const DEFAULT_AARCH64_IMAGE_URL: &str =
    "https://cloud-images.ubuntu.com/noble/current/noble-server-cloudimg-arm64.img";
const DEFAULT_X86_64_IMAGE_URL: &str =
    "https://cloud-images.ubuntu.com/noble/current/noble-server-cloudimg-amd64.img";
const DEFAULT_QEMU_MEMORY_MIB: u32 = 2_048;
const DEFAULT_QEMU_VCPUS: u8 = 2;
const QEMU_SHARE_DIR_ENV_VAR: &str = "QEMU_SHARE_DIR";
const QEMU_ACCEL_ENV_VAR: &str = "QEMU_ACCEL";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QemuGuestArch {
    Aarch64,
    X86_64,
}

impl QemuGuestArch {
    #[must_use]
    pub fn from_host() -> Self {
        match std::env::consts::ARCH {
            "aarch64" => Self::Aarch64,
            _ => Self::X86_64,
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Aarch64 => "aarch64",
            Self::X86_64 => "x86_64",
        }
    }

    #[must_use]
    pub const fn qemu_binary(self) -> &'static str {
        match self {
            Self::Aarch64 => "qemu-system-aarch64",
            Self::X86_64 => "qemu-system-x86_64",
        }
    }

    #[must_use]
    pub const fn default_image_url(self) -> &'static str {
        match self {
            Self::Aarch64 => DEFAULT_AARCH64_IMAGE_URL,
            Self::X86_64 => DEFAULT_X86_64_IMAGE_URL,
        }
    }

    #[must_use]
    pub const fn firmware_code_file_names(self) -> &'static [&'static str] {
        match self {
            Self::Aarch64 => &["edk2-aarch64-code.fd", "QEMU_EFI.fd", "AAVMF_CODE.fd"],
            Self::X86_64 => &["edk2-x86_64-code.fd", "OVMF_CODE.fd", "OVMF_CODE_4M.fd"],
        }
    }

    #[must_use]
    pub const fn firmware_vars_template_file_names(self) -> &'static [&'static str] {
        match self {
            Self::Aarch64 => &["edk2-arm-vars.fd", "AAVMF_VARS.fd"],
            Self::X86_64 => &["edk2-i386-vars.fd", "OVMF_VARS.fd", "OVMF_VARS_4M.fd"],
        }
    }

    #[must_use]
    pub fn machine_arg(self) -> String {
        let accel = qemu_accel();
        match self {
            Self::Aarch64 => format!("virt,accel={accel}"),
            Self::X86_64 => format!("q35,accel={accel}"),
        }
    }

    /// Parses the CLI or layout text architecture value.
    ///
    /// # Errors
    ///
    /// Returns [`QemuTestbedLayoutError::InvalidField`] when `value` is not a
    /// supported guest architecture name.
    pub fn parse(value: &str) -> Result<Self, QemuTestbedLayoutError> {
        match value {
            "aarch64" => Ok(Self::Aarch64),
            "x86_64" => Ok(Self::X86_64),
            other => Err(QemuTestbedLayoutError::InvalidField {
                field: String::from("arch"),
                value: String::from(other),
            }),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QemuTestbedConfig {
    pub workspace_root: PathBuf,
    pub arch: QemuGuestArch,
    pub base_image_url: String,
    pub base_image_path: PathBuf,
    pub local_cluster_binary_path: PathBuf,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QemuGuestKind {
    Control,
    Replica,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QemuGuestConfig {
    pub name: String,
    pub kind: QemuGuestKind,
    pub replica_id: Option<ReplicaId>,
    pub management_addr: Ipv4Addr,
    pub cluster_addr: Option<Ipv4Addr>,
    pub host_ssh_port: Option<u16>,
    pub uplink_mac: Option<String>,
    pub management_mac: String,
    pub cluster_mac: Option<String>,
    pub overlay_path: PathBuf,
    pub seed_dir: PathBuf,
    pub seed_iso_path: PathBuf,
    pub pid_path: PathBuf,
    pub serial_log_path: PathBuf,
    pub firmware_vars_path: PathBuf,
}

impl QemuGuestConfig {
    #[must_use]
    pub fn control_addr(&self) -> Option<SocketAddr> {
        self.replica_id.map(|_| {
            SocketAddr::V4(SocketAddrV4::new(
                self.management_addr,
                CONTROL_LISTENER_PORT,
            ))
        })
    }

    #[must_use]
    pub fn client_addr(&self) -> Option<SocketAddr> {
        self.cluster_addr
            .map(|ip| SocketAddr::V4(SocketAddrV4::new(ip, CLIENT_LISTENER_PORT)))
    }

    #[must_use]
    pub fn protocol_addr(&self) -> Option<SocketAddr> {
        self.cluster_addr
            .map(|ip| SocketAddr::V4(SocketAddrV4::new(ip, PROTOCOL_LISTENER_PORT)))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QemuTestbedLayout {
    pub config: QemuTestbedConfig,
    pub control_guest: QemuGuestConfig,
    pub replica_guests: Vec<QemuGuestConfig>,
    pub replica_layout: LocalClusterLayout,
}

#[derive(Debug)]
pub enum QemuTestbedLayoutError {
    Io(std::io::Error),
    MissingField(String),
    DuplicateField(String),
    InvalidLine(String),
    InvalidField { field: String, value: String },
    UnsupportedVersion(u32),
}

impl From<std::io::Error> for QemuTestbedLayoutError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl fmt::Display for QemuTestbedLayoutError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "i/o error: {error}"),
            Self::MissingField(field) => write!(formatter, "missing qemu-testbed field `{field}`"),
            Self::DuplicateField(field) => {
                write!(formatter, "duplicate qemu-testbed field `{field}`")
            }
            Self::InvalidLine(line) => write!(formatter, "invalid qemu-testbed line `{line}`"),
            Self::InvalidField { field, value } => {
                write!(formatter, "invalid value for `{field}`: `{value}`")
            }
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported qemu-testbed version `{version}`")
            }
        }
    }
}

impl std::error::Error for QemuTestbedLayoutError {}

impl QemuTestbedLayout {
    /// Creates one repeatable QEMU layout rooted in the provided workspace.
    ///
    /// # Errors
    ///
    /// Returns [`QemuTestbedLayoutError`] if the workspace cannot be prepared.
    pub fn new(config: QemuTestbedConfig) -> Result<Self, QemuTestbedLayoutError> {
        let workspace_root = prepare_workspace_root(&config.workspace_root)?;
        let config = QemuTestbedConfig {
            workspace_root: workspace_root.clone(),
            arch: config.arch,
            base_image_url: config.base_image_url,
            base_image_path: absolutize_path(&config.base_image_path)?,
            local_cluster_binary_path: absolutize_path(&config.local_cluster_binary_path)?,
        };

        let qemu_root = qemu_root(&workspace_root);
        let control_guest = QemuGuestConfig {
            name: String::from(CONTROL_GUEST_NAME),
            kind: QemuGuestKind::Control,
            replica_id: None,
            management_addr: CONTROL_GUEST_MANAGEMENT_IP,
            cluster_addr: None,
            host_ssh_port: Some(CONTROL_GUEST_HOST_SSH_PORT),
            uplink_mac: Some(mac_string([0x52, 0x54, 0x00, 0xaa, 0x00, 0x10])),
            management_mac: mac_string([0x52, 0x54, 0x00, 0xaa, 0x10, 0x10]),
            cluster_mac: None,
            overlay_path: qemu_images_dir(&qemu_root).join("control-overlay.qcow2"),
            seed_dir: qemu_seed_dir(&qemu_root).join(CONTROL_GUEST_NAME),
            seed_iso_path: qemu_seed_dir(&qemu_root).join("control-seed.iso"),
            pid_path: qemu_runtime_dir(&qemu_root).join("control.pid"),
            serial_log_path: qemu_log_dir(&qemu_root).join("control-console.log"),
            firmware_vars_path: qemu_firmware_dir(&qemu_root).join("control-vars.fd"),
        };

        let replica_guests = REPLICA_MANAGEMENT_IPS
            .iter()
            .zip(REPLICA_CLUSTER_IPS.iter())
            .enumerate()
            .map(|(index, (management_addr, cluster_addr))| {
                let replica_index =
                    u64::try_from(index).map_err(|_| QemuTestbedLayoutError::InvalidField {
                        field: String::from("replica_index"),
                        value: index.to_string(),
                    })?;
                let mac_suffix_index =
                    u8::try_from(index).map_err(|_| QemuTestbedLayoutError::InvalidField {
                        field: String::from("replica_mac_suffix_index"),
                        value: index.to_string(),
                    })?;
                let replica_id = ReplicaId(replica_index.saturating_add(1));
                let suffix = mac_suffix_index.saturating_add(0x11);
                Ok(QemuGuestConfig {
                    name: format!("allocdb-replica-{}", replica_id.get()),
                    kind: QemuGuestKind::Replica,
                    replica_id: Some(replica_id),
                    management_addr: *management_addr,
                    cluster_addr: Some(*cluster_addr),
                    host_ssh_port: None,
                    uplink_mac: None,
                    management_mac: mac_string([0x52, 0x54, 0x00, 0xaa, 0x10, suffix]),
                    cluster_mac: Some(mac_string([0x52, 0x54, 0x00, 0xaa, 0x20, suffix])),
                    overlay_path: qemu_images_dir(&qemu_root)
                        .join(format!("replica-{}-overlay.qcow2", replica_id.get())),
                    seed_dir: qemu_seed_dir(&qemu_root)
                        .join(format!("replica-{}", replica_id.get())),
                    seed_iso_path: qemu_seed_dir(&qemu_root)
                        .join(format!("replica-{}-seed.iso", replica_id.get())),
                    pid_path: qemu_runtime_dir(&qemu_root)
                        .join(format!("replica-{}.pid", replica_id.get())),
                    serial_log_path: qemu_log_dir(&qemu_root)
                        .join(format!("replica-{}-console.log", replica_id.get())),
                    firmware_vars_path: qemu_firmware_dir(&qemu_root)
                        .join(format!("replica-{}-vars.fd", replica_id.get())),
                })
            })
            .collect::<Result<Vec<_>, QemuTestbedLayoutError>>()?;

        Ok(Self {
            replica_layout: build_guest_local_cluster_layout(&replica_guests),
            config,
            control_guest,
            replica_guests,
        })
    }

    /// Loads one persisted QEMU layout from disk.
    ///
    /// # Errors
    ///
    /// Returns [`QemuTestbedLayoutError`] if the layout cannot be read or decoded.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, QemuTestbedLayoutError> {
        let path = path.as_ref();
        let mut bytes = String::new();
        File::open(path)?.read_to_string(&mut bytes)?;
        decode_layout(&bytes)
    }

    /// Persists the minimal QEMU layout description to the workspace root.
    ///
    /// # Errors
    ///
    /// Returns [`QemuTestbedLayoutError`] if the file cannot be written.
    pub fn persist(&self) -> Result<(), QemuTestbedLayoutError> {
        fs::create_dir_all(&self.config.workspace_root)?;
        let layout_path = self.layout_path();
        let temp_path = layout_path.with_extension(format!("{}.tmp", std::process::id()));
        let mut file = File::create(&temp_path)?;
        file.write_all(encode_layout(self).as_bytes())?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temp_path, &layout_path).map_err(|error| {
            let _ = fs::remove_file(&temp_path);
            QemuTestbedLayoutError::Io(error)
        })?;
        Ok(())
    }

    #[must_use]
    pub fn layout_path(&self) -> PathBuf {
        qemu_testbed_layout_path(&self.config.workspace_root)
    }

    #[must_use]
    pub fn qemu_root(&self) -> PathBuf {
        qemu_root(&self.config.workspace_root)
    }

    #[must_use]
    pub fn ssh_private_key_path(&self) -> PathBuf {
        self.qemu_root().join(QEMU_SSH_DIR_NAME).join("id_ed25519")
    }

    #[must_use]
    pub fn ssh_public_key_path(&self) -> PathBuf {
        self.qemu_root()
            .join(QEMU_SSH_DIR_NAME)
            .join("id_ed25519.pub")
    }

    #[must_use]
    pub fn control_guest_proxy_command(&self) -> String {
        format!("sudo {GUEST_QEMU_CONTROL_SCRIPT_PATH} status",)
    }

    #[must_use]
    pub fn replica_layout_text(&self) -> String {
        encode_layout_text(&self.replica_layout)
    }

    #[must_use]
    pub fn render_control_guest_meta_data(&self) -> String {
        render_meta_data(&self.control_guest.name)
    }

    #[must_use]
    pub fn render_replica_guest_meta_data(&self, guest: &QemuGuestConfig) -> String {
        render_meta_data(&guest.name)
    }

    #[must_use]
    pub fn render_control_guest_network_config(&self) -> String {
        format!(
            "version: 2\nethernets:\n  uplink0:\n    match:\n      macaddress: {uplink_mac}\n    set-name: uplink0\n    dhcp4: true\n  mgmt0:\n    match:\n      macaddress: {management_mac}\n    set-name: mgmt0\n    dhcp4: false\n    addresses:\n      - {management_ip}/24\n",
            uplink_mac = self.control_guest.uplink_mac.as_deref().unwrap_or_default(),
            management_mac = self.control_guest.management_mac,
            management_ip = self.control_guest.management_addr,
        )
    }

    #[must_use]
    pub fn render_replica_guest_network_config(&self, guest: &QemuGuestConfig) -> String {
        format!(
            "version: 2\nethernets:\n  mgmt0:\n    match:\n      macaddress: {management_mac}\n    set-name: mgmt0\n    dhcp4: false\n    addresses:\n      - {management_ip}/24\n  cluster0:\n    match:\n      macaddress: {cluster_mac}\n    set-name: cluster0\n    dhcp4: false\n    addresses:\n      - {cluster_ip}/24\n",
            management_mac = guest.management_mac,
            management_ip = guest.management_addr,
            cluster_mac = guest.cluster_mac.as_deref().unwrap_or_default(),
            cluster_ip = guest.cluster_addr.unwrap_or(Ipv4Addr::UNSPECIFIED),
        )
    }

    #[must_use]
    pub fn render_control_guest_user_data(
        &self,
        local_cluster_binary_b64: &str,
        ssh_public_key: &str,
        ssh_private_key_b64: &str,
    ) -> String {
        let control_script = self.render_control_guest_script();
        format!(
            "#cloud-config\nusers:\n  - default\n  - name: {user}\n    shell: /bin/bash\n    sudo: ALL=(ALL) NOPASSWD:ALL\n    ssh_authorized_keys:\n      - {ssh_public_key}\nwrite_files:\n  - path: {bin_path}\n    permissions: '0755'\n    encoding: b64\n    content: {binary_b64}\n  - path: {script_path}\n    permissions: '0755'\n    content: |\n{script_block}\n  - path: {qemu_control_home}/id_ed25519\n    permissions: '0600'\n    encoding: b64\n    content: {private_key_b64}\n  - path: {qemu_control_home}/id_ed25519.pub\n    permissions: '0644'\n    content: |\n      {ssh_public_key}\nruncmd:\n  - mkdir -p {qemu_control_home}/log-bundles\n  - chown -R {user}:{user} {qemu_control_home}\n",
            user = GUEST_ALLOCDB_USER,
            bin_path = GUEST_LOCAL_CLUSTER_BIN_PATH,
            binary_b64 = local_cluster_binary_b64,
            script_path = GUEST_QEMU_CONTROL_SCRIPT_PATH,
            script_block = indent_block(&control_script, 6),
            qemu_control_home = GUEST_QEMU_CONTROL_HOME,
            private_key_b64 = ssh_private_key_b64,
            ssh_public_key = ssh_public_key.trim(),
        )
    }

    #[must_use]
    pub fn render_replica_guest_user_data(
        &self,
        guest: &QemuGuestConfig,
        local_cluster_binary_b64: &str,
        ssh_public_key: &str,
    ) -> String {
        let replica_id = guest.replica_id.unwrap_or(ReplicaId(0)).get();
        let user = GUEST_ALLOCDB_USER;
        let bin_path = GUEST_LOCAL_CLUSTER_BIN_PATH;
        let layout_path = GUEST_LAYOUT_PATH;
        let service = format!(
            "[Unit]\nDescription=AllocDB replica daemon {replica_id}\nAfter=network-online.target cloud-final.service\nWants=network-online.target\n\n[Service]\nUser={user}\nGroup={user}\nExecStart={bin_path} replica-daemon --layout-file {layout_path} --replica-id {replica_id}\nStandardOutput=append:/var/log/allocdb/replica-{replica_id}.log\nStandardError=append:/var/log/allocdb/replica-{replica_id}.log\n\n[Install]\nWantedBy=multi-user.target\n",
        );
        format!(
            "#cloud-config\nusers:\n  - default\n  - name: {user}\n    shell: /bin/bash\n    sudo: ALL=(ALL) NOPASSWD:ALL\n    ssh_authorized_keys:\n      - {ssh_public_key}\nwrite_files:\n  - path: {bin_path}\n    permissions: '0755'\n    encoding: b64\n    content: {binary_b64}\n  - path: {layout_path}\n    permissions: '0644'\n    content: |\n{layout_block}\n  - path: /etc/systemd/system/allocdb-replica.service\n    permissions: '0644'\n    content: |\n{service_block}\nruncmd:\n  - mkdir -p {guest_workspace_root}/replica-1 {guest_workspace_root}/replica-2 {guest_workspace_root}/replica-3 /var/log/allocdb /run/allocdb\n  - chown -R {user}:{user} {guest_workspace_root} /var/log/allocdb /run/allocdb\n  - systemctl daemon-reload\n  - systemctl enable --now allocdb-replica.service\n",
            user = user,
            ssh_public_key = ssh_public_key.trim(),
            bin_path = bin_path,
            binary_b64 = local_cluster_binary_b64,
            layout_path = layout_path,
            layout_block = indent_block(&self.replica_layout_text(), 6),
            service_block = indent_block(&service, 6),
            guest_workspace_root = GUEST_WORKSPACE_ROOT,
        )
    }

    #[must_use]
    pub fn render_control_guest_script(&self) -> String {
        let mut replica_commands = String::new();
        let mut status_commands = String::new();
        for guest in &self.replica_guests {
            let replica_id = guest.replica_id.unwrap_or(ReplicaId(0)).get();
            let management_ip = guest.management_addr;
            let control_addr = guest
                .control_addr()
                .unwrap_or_else(|| SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0)));
            writeln!(
                replica_commands,
                "    {replica_id}) echo \"{management_ip}\" ;;"
            )
            .expect("writing to a string cannot fail");
            writeln!(status_commands, "  echo \"== replica {replica_id} ==\"")
                .expect("writing to a string cannot fail");
            writeln!(
                status_commands,
                "  {GUEST_LOCAL_CLUSTER_BIN_PATH} control-status --addr {control_addr} || true"
            )
            .expect("writing to a string cannot fail");
        }
        format!(
            "#!/usr/bin/env bash\nset -euo pipefail\n\nworkspace_root={workspace_root}\ncontrol_home={control_home}\nssh_key=\"$control_home/id_ed25519\"\nssh_opts=(-i \"$ssh_key\" -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null)\n\nreplica_ip() {{\n  case \"$1\" in\n{replica_commands}    *) echo \"unknown replica id: $1\" >&2; exit 1 ;;\n  esac\n}}\n\nreplica_ssh() {{\n  local replica_id=\"$1\"\n  shift\n  ssh \"${{ssh_opts[@]}}\" {user}@\"$(replica_ip \"$replica_id\")\" \"$@\"\n}}\n\ncollect_logs() {{\n  local output_dir=\"${{1:-$control_home/log-bundles/$(date +%Y%m%d-%H%M%S)}}\"\n  mkdir -p \"$output_dir\"\n  for replica_id in 1 2 3; do\n    local replica_dir=\"$output_dir/replica-$replica_id\"\n    mkdir -p \"$replica_dir\"\n    replica_ssh \"$replica_id\" \"sudo journalctl -u allocdb-replica.service --no-pager\" > \"$replica_dir/journal.log\" || true\n    replica_ssh \"$replica_id\" \"sudo cat {workspace_root}/cluster-faults.txt\" > \"$replica_dir/cluster-faults.txt\" || true\n    replica_ssh \"$replica_id\" \"sudo cat {workspace_root}/cluster-timeline.log\" > \"$replica_dir/cluster-timeline.log\" || true\n    {bin_path} control-status --addr \"$(replica_ip \"$replica_id\"):{control_port}\" > \"$replica_dir/status.txt\" || true\n  done\n  echo \"$output_dir\"\n}}\n\nexport_replica() {{\n  local replica_id=\"$1\"\n  replica_ssh \"$replica_id\" \"sudo tar czf - -C {workspace_root} replica-$replica_id\"\n}}\n\nimport_replica() {{\n  local replica_id=\"$1\"\n  replica_ssh \"$replica_id\" \"sudo rm -rf {workspace_root}/replica-$replica_id && sudo mkdir -p {workspace_root} && sudo tar xzf - -C {workspace_root} && sudo chown -R {user}:{user} {workspace_root}/replica-$replica_id\"\n}}\n\ncase \"${{1:-}}\" in\n  status)\n{status_commands}    ;;\n  isolate)\n    replica_ssh \"$2\" \"sudo {bin_path} isolate --workspace {workspace_root} --replica-id $2\"\n    ;;\n  heal)\n    replica_ssh \"$2\" \"sudo {bin_path} heal --workspace {workspace_root} --replica-id $2\"\n    ;;\n  crash)\n    replica_ssh \"$2\" \"sudo {bin_path} crash --workspace {workspace_root} --replica-id $2\"\n    ;;\n  restart)\n    replica_ssh \"$2\" \"sudo {bin_path} restart --workspace {workspace_root} --replica-id $2\"\n    ;;\n  reboot)\n    replica_ssh \"$2\" \"sudo /sbin/reboot\" || true\n    ;;\n  export-replica)\n    export_replica \"$2\"\n    ;;\n  import-replica)\n    import_replica \"$2\"\n    ;;\n  collect-logs)\n    collect_logs \"${{2:-}}\"\n    ;;\n  *)\n    echo \"usage: allocdb-qemu-control <status|isolate|heal|crash|restart|reboot|export-replica|import-replica|collect-logs> [replica-id|output-dir]\" >&2\n    exit 1\n    ;;\nesac\n",
            workspace_root = GUEST_WORKSPACE_ROOT,
            control_home = GUEST_QEMU_CONTROL_HOME,
            user = GUEST_ALLOCDB_USER,
            bin_path = GUEST_LOCAL_CLUSTER_BIN_PATH,
            control_port = CONTROL_LISTENER_PORT,
            replica_commands = replica_commands,
            status_commands = indent_block(&status_commands, 4),
        )
    }

    #[must_use]
    pub fn qemu_command(&self, guest: &QemuGuestConfig) -> Vec<String> {
        let firmware_code =
            qemu_firmware_code_path(self.config.arch, Some(&self.config.base_image_path));
        let mut command = vec![
            String::from(self.config.arch.qemu_binary()),
            String::from("-machine"),
            self.config.arch.machine_arg(),
            String::from("-cpu"),
            String::from(qemu_cpu_model()),
            String::from("-smp"),
            DEFAULT_QEMU_VCPUS.to_string(),
            String::from("-m"),
            DEFAULT_QEMU_MEMORY_MIB.to_string(),
            String::from("-display"),
            String::from("none"),
            String::from("-daemonize"),
            String::from("-name"),
            guest.name.clone(),
            String::from("-pidfile"),
            guest.pid_path.display().to_string(),
            String::from("-serial"),
            format!("file:{}", guest.serial_log_path.display()),
            String::from("-drive"),
            format!(
                "if=pflash,format=raw,readonly=on,file={}",
                firmware_code.display()
            ),
            String::from("-drive"),
            format!(
                "if=pflash,format=raw,file={}",
                guest.firmware_vars_path.display()
            ),
            String::from("-drive"),
            format!(
                "if=virtio,format=qcow2,file={}",
                guest.overlay_path.display()
            ),
            String::from("-drive"),
            format!(
                "if=virtio,format=raw,media=cdrom,file={}",
                guest.seed_iso_path.display()
            ),
            String::from("-netdev"),
            format!("socket,id=mgmt,mcast={MANAGEMENT_NETWORK_MCAST}"),
            String::from("-device"),
            format!("virtio-net-pci,netdev=mgmt,mac={}", guest.management_mac),
        ];

        if let Some(cluster_mac) = &guest.cluster_mac {
            command.push(String::from("-netdev"));
            command.push(format!("socket,id=cluster,mcast={CLUSTER_NETWORK_MCAST}"));
            command.push(String::from("-device"));
            command.push(format!("virtio-net-pci,netdev=cluster,mac={cluster_mac}"));
        }

        if let (Some(host_ssh_port), Some(uplink_mac)) = (guest.host_ssh_port, &guest.uplink_mac) {
            command.push(String::from("-netdev"));
            command.push(format!(
                "user,id=uplink,hostfwd=tcp:127.0.0.1:{host_ssh_port}-:22"
            ));
            command.push(String::from("-device"));
            command.push(format!("virtio-net-pci,netdev=uplink,mac={uplink_mac}"));
        }

        command
    }
}

#[must_use]
pub fn qemu_testbed_layout_path(workspace_root: &Path) -> PathBuf {
    workspace_root.join(QEMU_TESTBED_LAYOUT_FILE_NAME)
}

#[must_use]
pub fn qemu_firmware_code_path(arch: QemuGuestArch, base_image_path: Option<&Path>) -> PathBuf {
    resolve_qemu_firmware_path(arch.firmware_code_file_names(), base_image_path)
}

#[must_use]
pub fn qemu_firmware_vars_template_path(
    arch: QemuGuestArch,
    base_image_path: Option<&Path>,
) -> PathBuf {
    resolve_qemu_firmware_path(arch.firmware_vars_template_file_names(), base_image_path)
}

#[must_use]
pub fn qemu_accel() -> String {
    std::env::var(QEMU_ACCEL_ENV_VAR)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(default_qemu_accel)
}

#[must_use]
pub fn qemu_cpu_model() -> &'static str {
    if qemu_accel() == "tcg" { "max" } else { "host" }
}

fn qemu_root(workspace_root: &Path) -> PathBuf {
    workspace_root.join(QEMU_TESTBED_DIR_NAME)
}

fn default_qemu_accel() -> String {
    if cfg!(target_os = "macos") {
        String::from("hvf")
    } else if cfg!(target_os = "linux") {
        String::from("kvm")
    } else {
        String::from("tcg")
    }
}

fn resolve_qemu_firmware_path(file_names: &[&str], base_image_path: Option<&Path>) -> PathBuf {
    find_existing_qemu_firmware_path(file_names, base_image_path)
        .unwrap_or_else(|| qemu_share_dirs(base_image_path)[0].join(file_names[0]))
}

fn find_existing_qemu_firmware_path(
    file_names: &[&str],
    base_image_path: Option<&Path>,
) -> Option<PathBuf> {
    for directory in qemu_share_dirs(base_image_path) {
        for file_name in file_names {
            let candidate = directory.join(file_name);
            if candidate.exists() {
                return Some(candidate);
            }
        }
    }
    None
}

fn qemu_share_dirs(base_image_path: Option<&Path>) -> Vec<PathBuf> {
    let mut directories = Vec::new();
    if let Some(base_image_dir) = base_image_path.and_then(Path::parent) {
        directories.push(base_image_dir.to_path_buf());
    }
    if let Some(path) = std::env::var_os(QEMU_SHARE_DIR_ENV_VAR)
        .map(PathBuf::from)
        .filter(|path| !path.as_os_str().is_empty())
    {
        if !directories.contains(&path) {
            directories.push(path);
        }
    }
    for candidate in default_qemu_share_dir_candidates() {
        let candidate = PathBuf::from(candidate);
        if !directories.contains(&candidate) {
            directories.push(candidate);
        }
    }
    directories
}

fn default_qemu_share_dir_candidates() -> &'static [&'static str] {
    if cfg!(target_os = "macos") {
        &[
            "/opt/homebrew/share/qemu",
            "/usr/local/share/qemu",
            "/usr/share/qemu",
        ]
    } else {
        &[
            "/usr/share/qemu",
            "/usr/share/OVMF",
            "/usr/share/AAVMF",
            "/usr/local/share/qemu",
            "/opt/homebrew/share/qemu",
        ]
    }
}

fn qemu_images_dir(qemu_root: &Path) -> PathBuf {
    qemu_root.join("images")
}

fn qemu_runtime_dir(qemu_root: &Path) -> PathBuf {
    qemu_root.join("run")
}

fn qemu_seed_dir(qemu_root: &Path) -> PathBuf {
    qemu_root.join("seed")
}

fn qemu_log_dir(qemu_root: &Path) -> PathBuf {
    qemu_root.join("logs")
}

fn qemu_firmware_dir(qemu_root: &Path) -> PathBuf {
    qemu_root.join("firmware")
}

fn build_guest_local_cluster_layout(replicas: &[QemuGuestConfig]) -> LocalClusterLayout {
    let workspace_root = PathBuf::from(GUEST_WORKSPACE_ROOT);
    LocalClusterLayout {
        workspace_root: workspace_root.clone(),
        current_view: 1,
        core_config: default_local_cluster_core_config(),
        engine_config: default_local_cluster_engine_config(),
        replicas: replicas
            .iter()
            .map(|guest| {
                let replica_id = guest.replica_id.unwrap_or(ReplicaId(0));
                let control_addr = guest.control_addr().unwrap_or_else(|| {
                    SocketAddr::V4(SocketAddrV4::new(
                        Ipv4Addr::LOCALHOST,
                        CONTROL_LISTENER_PORT,
                    ))
                });
                let client_addr = guest.client_addr().unwrap_or_else(|| {
                    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, CLIENT_LISTENER_PORT))
                });
                let protocol_addr = guest.protocol_addr().unwrap_or_else(|| {
                    SocketAddr::V4(SocketAddrV4::new(
                        Ipv4Addr::LOCALHOST,
                        PROTOCOL_LISTENER_PORT,
                    ))
                });
                LocalClusterReplicaConfig {
                    replica_id,
                    role: if replica_id.get() == 1 {
                        ReplicaRole::Primary
                    } else {
                        ReplicaRole::Backup
                    },
                    workspace_dir: workspace_root.join(format!("replica-{}", replica_id.get())),
                    log_path: PathBuf::from(format!(
                        "/var/log/allocdb/replica-{}.log",
                        replica_id.get()
                    )),
                    pid_path: PathBuf::from(format!(
                        "/run/allocdb/replica-{}.pid",
                        replica_id.get()
                    )),
                    paths: ReplicaPaths::new(
                        PathBuf::from(format!(
                            "/var/lib/allocdb/replica-{}/replica.metadata",
                            replica_id.get()
                        )),
                        PathBuf::from(format!(
                            "/var/lib/allocdb/replica-{}/state.snapshot",
                            replica_id.get()
                        )),
                        PathBuf::from(format!(
                            "/var/lib/allocdb/replica-{}/state.wal",
                            replica_id.get()
                        )),
                    ),
                    control_addr,
                    client_addr,
                    protocol_addr,
                }
            })
            .collect(),
    }
}

fn prepare_workspace_root(workspace_root: &Path) -> Result<PathBuf, QemuTestbedLayoutError> {
    fs::create_dir_all(workspace_root)?;
    Ok(fs::canonicalize(workspace_root)?)
}

fn absolutize_path(path: &Path) -> Result<PathBuf, QemuTestbedLayoutError> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()?.join(path))
    }
}

fn mac_string(bytes: [u8; 6]) -> String {
    bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join(":")
}

fn indent_block(text: &str, spaces: usize) -> String {
    let prefix = " ".repeat(spaces);
    text.lines()
        .map(|line| format!("{prefix}{line}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn render_meta_data(name: &str) -> String {
    format!("instance-id: {name}\nlocal-hostname: {name}\n")
}

fn encode_layout(layout: &QemuTestbedLayout) -> String {
    format!(
        "version={version}\nworkspace_root={workspace_root}\narch={arch}\nbase_image_url={base_image_url}\nbase_image_path={base_image_path}\nlocal_cluster_binary_path={binary_path}\n",
        version = QEMU_TESTBED_LAYOUT_VERSION,
        workspace_root = layout.config.workspace_root.display(),
        arch = layout.config.arch.as_str(),
        base_image_url = layout.config.base_image_url,
        base_image_path = layout.config.base_image_path.display(),
        binary_path = layout.config.local_cluster_binary_path.display(),
    )
}

fn decode_layout(bytes: &str) -> Result<QemuTestbedLayout, QemuTestbedLayoutError> {
    let fields = parse_key_value_lines(bytes)?;
    let version_value = required_field(&fields, "version")?;
    let version =
        version_value
            .parse::<u32>()
            .map_err(|_| QemuTestbedLayoutError::InvalidField {
                field: String::from("version"),
                value: String::from(version_value),
            })?;
    if version != QEMU_TESTBED_LAYOUT_VERSION {
        return Err(QemuTestbedLayoutError::UnsupportedVersion(version));
    }

    let config = QemuTestbedConfig {
        workspace_root: PathBuf::from(required_field(&fields, "workspace_root")?),
        arch: QemuGuestArch::parse(required_field(&fields, "arch")?)?,
        base_image_url: String::from(required_field(&fields, "base_image_url")?),
        base_image_path: PathBuf::from(required_field(&fields, "base_image_path")?),
        local_cluster_binary_path: PathBuf::from(required_field(
            &fields,
            "local_cluster_binary_path",
        )?),
    };
    QemuTestbedLayout::new(config)
}

fn parse_key_value_lines(
    bytes: &str,
) -> Result<std::collections::BTreeMap<String, String>, QemuTestbedLayoutError> {
    let mut fields = std::collections::BTreeMap::new();
    for line in bytes.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            return Err(QemuTestbedLayoutError::InvalidLine(String::from(line)));
        };
        if fields
            .insert(String::from(key), String::from(value))
            .is_some()
        {
            return Err(QemuTestbedLayoutError::DuplicateField(String::from(key)));
        }
    }
    Ok(fields)
}

fn required_field<'a>(
    fields: &'a std::collections::BTreeMap<String, String>,
    key: &str,
) -> Result<&'a str, QemuTestbedLayoutError> {
    fields
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| QemuTestbedLayoutError::MissingField(String::from(key)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn fixture_layout() -> QemuTestbedLayout {
        QemuTestbedLayout::new(QemuTestbedConfig {
            workspace_root: std::env::temp_dir().join("allocdb-qemu-testbed-layout"),
            arch: QemuGuestArch::Aarch64,
            base_image_url: String::from(DEFAULT_AARCH64_IMAGE_URL),
            base_image_path: PathBuf::from("/tmp/base-cloudimg.img"),
            local_cluster_binary_path: PathBuf::from("/tmp/allocdb-local-cluster"),
        })
        .expect("fixture layout")
    }

    #[test]
    fn qemu_testbed_layout_round_trips_through_text_encoding() {
        let layout = fixture_layout();
        let encoded = encode_layout(&layout);
        let decoded = decode_layout(&encoded).expect("decode qemu layout");
        assert_eq!(decoded.config.arch, layout.config.arch);
        assert_eq!(decoded.config.base_image_url, layout.config.base_image_url);
        assert_eq!(
            decoded.config.base_image_path,
            layout.config.base_image_path
        );
        assert_eq!(
            decoded.config.local_cluster_binary_path,
            layout.config.local_cluster_binary_path
        );
        assert_eq!(
            decoded.control_guest.management_addr,
            CONTROL_GUEST_MANAGEMENT_IP
        );
        assert_eq!(decoded.replica_guests.len(), 3);
    }

    #[test]
    fn persisted_qemu_layout_round_trips_from_disk() {
        let workspace_root = std::env::temp_dir().join(format!(
            "allocdb-qemu-testbed-persist-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        let layout = QemuTestbedLayout::new(QemuTestbedConfig {
            workspace_root: workspace_root.clone(),
            arch: QemuGuestArch::Aarch64,
            base_image_url: String::from(DEFAULT_AARCH64_IMAGE_URL),
            base_image_path: PathBuf::from("/tmp/base-cloudimg.img"),
            local_cluster_binary_path: PathBuf::from("/tmp/allocdb-local-cluster"),
        })
        .expect("persist fixture layout");

        layout.persist().expect("persist qemu layout");
        let loaded = QemuTestbedLayout::load(layout.layout_path()).expect("load qemu layout");
        let temp_path = layout
            .layout_path()
            .with_extension(format!("{}.tmp", std::process::id()));

        assert_eq!(loaded.config.arch, layout.config.arch);
        assert_eq!(loaded.config.workspace_root, layout.config.workspace_root);
        assert_eq!(loaded.config.base_image_path, layout.config.base_image_path);
        assert_eq!(
            loaded.config.local_cluster_binary_path,
            layout.config.local_cluster_binary_path
        );
        assert!(!temp_path.exists());

        let _ = fs::remove_dir_all(workspace_root);
    }

    #[test]
    fn replica_layout_uses_management_for_control_and_cluster_for_data() {
        let layout = fixture_layout();
        let replica = &layout.replica_layout.replicas[0];
        assert_eq!(
            replica.control_addr.ip(),
            std::net::IpAddr::V4(REPLICA_MANAGEMENT_IPS[0])
        );
        assert_eq!(
            replica.client_addr.ip(),
            std::net::IpAddr::V4(REPLICA_CLUSTER_IPS[0])
        );
        assert_eq!(
            replica.protocol_addr.ip(),
            std::net::IpAddr::V4(REPLICA_CLUSTER_IPS[0])
        );
    }

    #[test]
    fn control_guest_user_data_contains_control_script_and_private_key() {
        let layout = fixture_layout();
        let user_data = layout.render_control_guest_user_data(
            "dGVzdA==",
            "ssh-ed25519 AAAATEST allocdb@test",
            "cHJpdmF0ZS1rZXk=",
        );
        assert!(user_data.contains(GUEST_QEMU_CONTROL_SCRIPT_PATH));
        assert!(user_data.contains("collect_logs()"));
        assert!(user_data.contains("collect-logs"));
        assert!(user_data.contains("export_replica()"));
        assert!(user_data.contains("export-replica"));
        assert!(user_data.contains("import-replica"));
        assert!(user_data.contains("control-status --addr 172.31.0.11:17000 || true"));
        assert!(user_data.contains("cHJpdmF0ZS1rZXk="));
    }

    #[test]
    fn replica_guest_user_data_contains_replica_service_and_layout() {
        let layout = fixture_layout();
        let user_data = layout.render_replica_guest_user_data(
            &layout.replica_guests[1],
            "dGVzdA==",
            "ssh-ed25519 AAAATEST allocdb@test",
        );
        assert!(user_data.contains("allocdb-replica.service"));
        assert!(user_data.contains("--replica-id 2"));
        assert!(user_data.contains("current_view=1"));
        assert!(!user_data.contains("Restart=always"));
        assert!(!user_data.contains("RestartSec=1"));
        assert!(
            user_data.contains("replica.2.control_addr=172.31.0.12:17000")
                || user_data.contains("control_addr=172.31.0.12:17000")
        );
    }

    #[test]
    fn qemu_command_contains_expected_networks_and_seed_assets() {
        let layout = fixture_layout();
        let control_command = layout.qemu_command(&layout.control_guest).join(" ");
        assert!(control_command.contains("qemu-system-aarch64"));
        assert!(control_command.contains(MANAGEMENT_NETWORK_MCAST));
        assert!(control_command.contains("hostfwd=tcp:127.0.0.1:2220-:22"));
        assert!(control_command.contains("control-seed.iso"));

        let replica_command = layout.qemu_command(&layout.replica_guests[0]).join(" ");
        assert!(replica_command.contains(CLUSTER_NETWORK_MCAST));
        assert!(replica_command.contains("replica-1-seed.iso"));
        assert!(!replica_command.contains("hostfwd=tcp:127.0.0.1:2221-:22"));
    }
}

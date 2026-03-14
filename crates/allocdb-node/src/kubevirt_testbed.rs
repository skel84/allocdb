use std::fmt::{self, Write as _};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::{Path, PathBuf};

use crate::local_cluster::{
    LocalClusterLayout, LocalClusterReplicaConfig, default_local_cluster_core_config,
    default_local_cluster_engine_config,
};
use crate::replica::{ReplicaId, ReplicaPaths, ReplicaRole};

const KUBEVIRT_TESTBED_LAYOUT_VERSION: u32 = 1;
const KUBEVIRT_TESTBED_LAYOUT_FILE_NAME: &str = "kubevirt-testbed-layout.txt";
const GUEST_WORKSPACE_ROOT: &str = "/var/lib/allocdb";
const CONTROL_LISTENER_PORT: u16 = 17_000;
const CLIENT_LISTENER_PORT: u16 = 18_000;
const PROTOCOL_LISTENER_PORT: u16 = 19_000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KubevirtTestbedConfig {
    pub workspace_root: PathBuf,
    pub kubeconfig_path: Option<PathBuf>,
    pub namespace: String,
    pub helper_pod_name: String,
    pub helper_image: String,
    pub helper_stage_dir: PathBuf,
    pub ssh_private_key_path: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KubevirtGuestConfig {
    pub name: String,
    pub replica_id: Option<ReplicaId>,
    pub addr: Ipv4Addr,
}

impl KubevirtGuestConfig {
    #[must_use]
    pub fn control_addr(&self) -> Option<SocketAddr> {
        self.replica_id
            .map(|_| SocketAddr::V4(SocketAddrV4::new(self.addr, CONTROL_LISTENER_PORT)))
    }

    #[must_use]
    pub fn client_addr(&self) -> Option<SocketAddr> {
        self.replica_id
            .map(|_| SocketAddr::V4(SocketAddrV4::new(self.addr, CLIENT_LISTENER_PORT)))
    }

    #[must_use]
    pub fn protocol_addr(&self) -> Option<SocketAddr> {
        self.replica_id
            .map(|_| SocketAddr::V4(SocketAddrV4::new(self.addr, PROTOCOL_LISTENER_PORT)))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KubevirtTestbedLayout {
    pub config: KubevirtTestbedConfig,
    pub control_guest: KubevirtGuestConfig,
    pub replica_guests: Vec<KubevirtGuestConfig>,
    pub replica_layout: LocalClusterLayout,
}

#[derive(Debug)]
pub enum KubevirtTestbedLayoutError {
    Io(std::io::Error),
    MissingField(String),
    DuplicateField(String),
    InvalidLine(String),
    InvalidField { field: String, value: String },
    UnsupportedVersion(u32),
}

impl From<std::io::Error> for KubevirtTestbedLayoutError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl fmt::Display for KubevirtTestbedLayoutError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "i/o error: {error}"),
            Self::MissingField(field) => {
                write!(formatter, "missing kubevirt-testbed field `{field}`")
            }
            Self::DuplicateField(field) => {
                write!(formatter, "duplicate kubevirt-testbed field `{field}`")
            }
            Self::InvalidLine(line) => write!(formatter, "invalid kubevirt-testbed line `{line}`"),
            Self::InvalidField { field, value } => {
                write!(formatter, "invalid value for `{field}`: `{value}`")
            }
            Self::UnsupportedVersion(version) => {
                write!(
                    formatter,
                    "unsupported kubevirt-testbed version `{version}`"
                )
            }
        }
    }
}

impl std::error::Error for KubevirtTestbedLayoutError {}

impl KubevirtTestbedLayout {
    /// Creates one repeatable `KubeVirt` layout rooted in the provided workspace.
    ///
    /// # Errors
    ///
    /// Returns [`KubevirtTestbedLayoutError`] if the workspace cannot be prepared.
    pub fn new(
        config: KubevirtTestbedConfig,
        control_guest: KubevirtGuestConfig,
        replica_guests: Vec<KubevirtGuestConfig>,
    ) -> Result<Self, KubevirtTestbedLayoutError> {
        let workspace_root = prepare_workspace_root(&config.workspace_root)?;
        let config = KubevirtTestbedConfig {
            workspace_root: workspace_root.clone(),
            kubeconfig_path: config
                .kubeconfig_path
                .as_deref()
                .map(absolutize_path)
                .transpose()?,
            namespace: config.namespace,
            helper_pod_name: config.helper_pod_name,
            helper_image: config.helper_image,
            helper_stage_dir: absolutize_path(&config.helper_stage_dir)?,
            ssh_private_key_path: absolutize_path(&config.ssh_private_key_path)?,
        };
        validate_replica_guests(&replica_guests)?;
        Ok(Self {
            replica_layout: build_guest_local_cluster_layout(&replica_guests),
            config,
            control_guest,
            replica_guests,
        })
    }

    /// Loads one persisted `KubeVirt` layout from disk.
    ///
    /// # Errors
    ///
    /// Returns [`KubevirtTestbedLayoutError`] if the layout cannot be read or decoded.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, KubevirtTestbedLayoutError> {
        let path = path.as_ref();
        let mut bytes = String::new();
        File::open(path)?.read_to_string(&mut bytes)?;
        decode_layout(&bytes)
    }

    /// Persists the minimal `KubeVirt` layout description to the workspace root.
    ///
    /// # Errors
    ///
    /// Returns [`KubevirtTestbedLayoutError`] if the file cannot be written.
    pub fn persist(&self) -> Result<(), KubevirtTestbedLayoutError> {
        fs::create_dir_all(&self.config.workspace_root)?;
        let layout_path = self.layout_path();
        let temp_path = layout_path.with_extension(format!("{}.tmp", std::process::id()));
        let mut file = File::create(&temp_path)?;
        file.write_all(encode_layout(self).as_bytes())?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temp_path, &layout_path).map_err(|error| {
            let _ = fs::remove_file(&temp_path);
            KubevirtTestbedLayoutError::Io(error)
        })?;
        Ok(())
    }

    #[must_use]
    pub fn layout_path(&self) -> PathBuf {
        kubevirt_testbed_layout_path(&self.config.workspace_root)
    }
}

#[must_use]
pub fn kubevirt_testbed_layout_path(workspace_root: &Path) -> PathBuf {
    workspace_root.join(KUBEVIRT_TESTBED_LAYOUT_FILE_NAME)
}

fn build_guest_local_cluster_layout(replicas: &[KubevirtGuestConfig]) -> LocalClusterLayout {
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
                    control_addr: guest.control_addr().unwrap_or_else(|| {
                        SocketAddr::V4(SocketAddrV4::new(
                            Ipv4Addr::LOCALHOST,
                            CONTROL_LISTENER_PORT,
                        ))
                    }),
                    client_addr: guest.client_addr().unwrap_or_else(|| {
                        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, CLIENT_LISTENER_PORT))
                    }),
                    protocol_addr: guest.protocol_addr().unwrap_or_else(|| {
                        SocketAddr::V4(SocketAddrV4::new(
                            Ipv4Addr::LOCALHOST,
                            PROTOCOL_LISTENER_PORT,
                        ))
                    }),
                }
            })
            .collect(),
    }
}

fn validate_replica_guests(
    replica_guests: &[KubevirtGuestConfig],
) -> Result<(), KubevirtTestbedLayoutError> {
    if replica_guests.len() != 3 {
        return Err(KubevirtTestbedLayoutError::InvalidField {
            field: String::from("replica_count"),
            value: replica_guests.len().to_string(),
        });
    }
    for (index, guest) in replica_guests.iter().enumerate() {
        let expected = u64::try_from(index).unwrap_or(0).saturating_add(1);
        if guest.replica_id != Some(ReplicaId(expected)) {
            return Err(KubevirtTestbedLayoutError::InvalidField {
                field: format!("replica.{}.id", index + 1),
                value: guest.replica_id.map_or_else(
                    || String::from("none"),
                    |replica_id| replica_id.get().to_string(),
                ),
            });
        }
    }
    Ok(())
}

fn prepare_workspace_root(workspace_root: &Path) -> Result<PathBuf, KubevirtTestbedLayoutError> {
    fs::create_dir_all(workspace_root)?;
    absolutize_path(workspace_root)
}

fn absolutize_path(path: &Path) -> Result<PathBuf, KubevirtTestbedLayoutError> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        std::env::current_dir()
            .map(|root| root.join(path))
            .map_err(KubevirtTestbedLayoutError::Io)
    }
}

fn encode_layout(layout: &KubevirtTestbedLayout) -> String {
    let mut output = String::new();
    let _ = writeln!(&mut output, "version={KUBEVIRT_TESTBED_LAYOUT_VERSION}");
    let _ = writeln!(
        &mut output,
        "workspace_root={}",
        layout.config.workspace_root.display()
    );
    if let Some(kubeconfig_path) = &layout.config.kubeconfig_path {
        let _ = writeln!(&mut output, "kubeconfig_path={}", kubeconfig_path.display());
    }
    let _ = writeln!(&mut output, "namespace={}", layout.config.namespace);
    let _ = writeln!(
        &mut output,
        "helper_pod_name={}",
        layout.config.helper_pod_name
    );
    let _ = writeln!(&mut output, "helper_image={}", layout.config.helper_image);
    let _ = writeln!(
        &mut output,
        "helper_stage_dir={}",
        layout.config.helper_stage_dir.display()
    );
    let _ = writeln!(
        &mut output,
        "ssh_private_key_path={}",
        layout.config.ssh_private_key_path.display()
    );
    let _ = writeln!(&mut output, "control.name={}", layout.control_guest.name);
    let _ = writeln!(&mut output, "control.addr={}", layout.control_guest.addr);
    for guest in &layout.replica_guests {
        let replica_id = guest.replica_id.unwrap_or(ReplicaId(0)).get();
        let _ = writeln!(&mut output, "replica.{replica_id}.name={}", guest.name);
        let _ = writeln!(&mut output, "replica.{replica_id}.addr={}", guest.addr);
    }
    output
}

fn decode_layout(bytes: &str) -> Result<KubevirtTestbedLayout, KubevirtTestbedLayoutError> {
    let fields = parse_fields(bytes)?;
    let version_field = required_field(&fields, "version")?;
    let version =
        version_field
            .parse::<u32>()
            .map_err(|_| KubevirtTestbedLayoutError::InvalidField {
                field: String::from("version"),
                value: version_field.to_string(),
            })?;
    if version != KUBEVIRT_TESTBED_LAYOUT_VERSION {
        return Err(KubevirtTestbedLayoutError::UnsupportedVersion(version));
    }

    let config = KubevirtTestbedConfig {
        workspace_root: PathBuf::from(required_field(&fields, "workspace_root")?),
        kubeconfig_path: fields.get("kubeconfig_path").map(PathBuf::from),
        namespace: String::from(required_field(&fields, "namespace")?),
        helper_pod_name: String::from(required_field(&fields, "helper_pod_name")?),
        helper_image: String::from(required_field(&fields, "helper_image")?),
        helper_stage_dir: PathBuf::from(required_field(&fields, "helper_stage_dir")?),
        ssh_private_key_path: PathBuf::from(required_field(&fields, "ssh_private_key_path")?),
    };
    let control_guest = KubevirtGuestConfig {
        name: String::from(required_field(&fields, "control.name")?),
        replica_id: None,
        addr: parse_ipv4(required_field(&fields, "control.addr")?, "control.addr")?,
    };
    let replica_guests = (1..=3)
        .map(|replica_id| {
            Ok(KubevirtGuestConfig {
                name: String::from(required_field(
                    &fields,
                    &format!("replica.{replica_id}.name"),
                )?),
                replica_id: Some(ReplicaId(replica_id)),
                addr: parse_ipv4(
                    required_field(&fields, &format!("replica.{replica_id}.addr"))?,
                    &format!("replica.{replica_id}.addr"),
                )?,
            })
        })
        .collect::<Result<Vec<_>, KubevirtTestbedLayoutError>>()?;
    KubevirtTestbedLayout::new(config, control_guest, replica_guests)
}

fn parse_fields(
    bytes: &str,
) -> Result<std::collections::BTreeMap<String, String>, KubevirtTestbedLayoutError> {
    let mut fields = std::collections::BTreeMap::new();
    for line in bytes.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            return Err(KubevirtTestbedLayoutError::InvalidLine(String::from(line)));
        };
        if fields
            .insert(String::from(key), String::from(value))
            .is_some()
        {
            return Err(KubevirtTestbedLayoutError::DuplicateField(String::from(
                key,
            )));
        }
    }
    Ok(fields)
}

fn required_field<'a>(
    fields: &'a std::collections::BTreeMap<String, String>,
    key: &str,
) -> Result<&'a str, KubevirtTestbedLayoutError> {
    fields
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| KubevirtTestbedLayoutError::MissingField(String::from(key)))
}

fn parse_ipv4(value: &str, field: &str) -> Result<Ipv4Addr, KubevirtTestbedLayoutError> {
    value
        .parse::<Ipv4Addr>()
        .map_err(|_| KubevirtTestbedLayoutError::InvalidField {
            field: String::from(field),
            value: String::from(value),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_layout() -> KubevirtTestbedLayout {
        KubevirtTestbedLayout::new(
            KubevirtTestbedConfig {
                workspace_root: std::env::temp_dir().join("allocdb-kubevirt-testbed-layout"),
                kubeconfig_path: Some(PathBuf::from("/tmp/kubeconfig")),
                namespace: String::from("kubevirt"),
                helper_pod_name: String::from("allocdb-bootstrap-helper"),
                helper_image: String::from("nicolaka/netshoot:latest"),
                helper_stage_dir: PathBuf::from("/tmp/allocdb-stage"),
                ssh_private_key_path: std::env::temp_dir().join("allocdb-kubevirt-testbed-key"),
            },
            KubevirtGuestConfig {
                name: String::from("allocdb-control"),
                replica_id: None,
                addr: Ipv4Addr::new(10, 42, 2, 48),
            },
            vec![
                KubevirtGuestConfig {
                    name: String::from("allocdb-replica-1"),
                    replica_id: Some(ReplicaId(1)),
                    addr: Ipv4Addr::new(10, 42, 0, 92),
                },
                KubevirtGuestConfig {
                    name: String::from("allocdb-replica-2"),
                    replica_id: Some(ReplicaId(2)),
                    addr: Ipv4Addr::new(10, 42, 2, 89),
                },
                KubevirtGuestConfig {
                    name: String::from("allocdb-replica-3"),
                    replica_id: Some(ReplicaId(3)),
                    addr: Ipv4Addr::new(10, 42, 0, 241),
                },
            ],
        )
        .expect("fixture layout should build")
    }

    #[test]
    fn layout_round_trips_through_text() {
        let layout = fixture_layout();
        let decoded = decode_layout(&encode_layout(&layout)).expect("decode kubevirt layout");
        assert_eq!(decoded, layout);
    }

    #[test]
    fn persist_and_load_round_trip() {
        let layout = fixture_layout();
        layout.persist().expect("persist kubevirt layout");
        let loaded =
            KubevirtTestbedLayout::load(layout.layout_path()).expect("load kubevirt layout");
        assert_eq!(loaded, layout);
    }

    #[test]
    fn replica_layout_uses_guest_ips_for_all_surfaces() {
        let layout = fixture_layout();
        let replica = &layout.replica_layout.replicas[1];
        assert_eq!(replica.replica_id, ReplicaId(2));
        assert_eq!(replica.control_addr, "10.42.2.89:17000".parse().unwrap());
        assert_eq!(replica.client_addr, "10.42.2.89:18000".parse().unwrap());
        assert_eq!(replica.protocol_addr, "10.42.2.89:19000".parse().unwrap());
    }

    #[test]
    fn layout_rejects_wrong_replica_count() {
        let error = KubevirtTestbedLayout::new(
            KubevirtTestbedConfig {
                workspace_root: std::env::temp_dir().join("allocdb-kubevirt-invalid"),
                kubeconfig_path: None,
                namespace: String::from("kubevirt"),
                helper_pod_name: String::from("allocdb-bootstrap-helper"),
                helper_image: String::from("nicolaka/netshoot:latest"),
                helper_stage_dir: PathBuf::from("/tmp/allocdb-stage"),
                ssh_private_key_path: std::env::temp_dir().join("allocdb-kubevirt-testbed-key"),
            },
            KubevirtGuestConfig {
                name: String::from("allocdb-control"),
                replica_id: None,
                addr: Ipv4Addr::new(10, 42, 2, 48),
            },
            vec![KubevirtGuestConfig {
                name: String::from("allocdb-replica-1"),
                replica_id: Some(ReplicaId(1)),
                addr: Ipv4Addr::new(10, 42, 0, 92),
            }],
        )
        .expect_err("invalid replica count should fail");
        assert!(matches!(
            error,
            KubevirtTestbedLayoutError::InvalidField { field, .. } if field == "replica_count"
        ));
    }
}

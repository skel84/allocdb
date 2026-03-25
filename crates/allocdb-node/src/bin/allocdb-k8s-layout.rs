use std::fs;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::path::PathBuf;
use std::process::ExitCode;
use std::thread;
use std::time::{Duration, Instant};

use allocdb_node::local_cluster::{
    LocalClusterLayout, LocalClusterReplicaConfig, default_local_cluster_core_config,
    default_local_cluster_engine_config, layout_path,
};
use allocdb_node::replica::{ReplicaId, ReplicaPaths, ReplicaRole};

const CONTROL_LISTENER_PORT: u16 = 17_000;
const CLIENT_LISTENER_PORT: u16 = 18_000;
const PROTOCOL_LISTENER_PORT: u16 = 19_000;
const DEFAULT_CLUSTER_DOMAIN: &str = "cluster.local";
const DEFAULT_RESOLVE_TIMEOUT_SECS: u64 = 30;
const REPLICA_COUNT: u64 = 3;

struct ParsedArgs {
    workspace_root: PathBuf,
    statefulset_name: String,
    headless_service: String,
    namespace: String,
    cluster_domain: String,
    resolve_timeout: Duration,
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
    let args = parse_args(std::env::args().skip(1))?;
    let layout = build_layout(&args)?;
    layout
        .persist()
        .map_err(|error| format!("failed to persist Kubernetes layout: {error}"))?;
    println!("{}", layout_path(&layout.workspace_root).display());
    Ok(())
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<ParsedArgs, String> {
    let mut args = args.into_iter();
    let mut workspace_root = None;
    let mut statefulset_name = None;
    let mut headless_service = None;
    let mut namespace = None;
    let mut cluster_domain = String::from(DEFAULT_CLUSTER_DOMAIN);
    let mut resolve_timeout = Duration::from_secs(DEFAULT_RESOLVE_TIMEOUT_SECS);

    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--workspace-root" => {
                workspace_root = Some(PathBuf::from(args.next().ok_or_else(usage)?));
            }
            "--statefulset-name" => {
                statefulset_name = Some(args.next().ok_or_else(usage)?);
            }
            "--headless-service" => {
                headless_service = Some(args.next().ok_or_else(usage)?);
            }
            "--namespace" => {
                namespace = Some(args.next().ok_or_else(usage)?);
            }
            "--cluster-domain" => {
                cluster_domain = args.next().ok_or_else(usage)?;
            }
            "--resolve-timeout-secs" => {
                let raw = args.next().ok_or_else(usage)?;
                let seconds = raw.parse::<u64>().map_err(|_| {
                    format!(
                        "invalid value for `--resolve-timeout-secs`: `{raw}`\n\n{}",
                        usage()
                    )
                })?;
                resolve_timeout = Duration::from_secs(seconds);
            }
            "--help" | "-h" => return Err(usage()),
            other => return Err(format!("unknown argument `{other}`\n\n{}", usage())),
        }
    }

    Ok(ParsedArgs {
        workspace_root: workspace_root.ok_or_else(usage)?,
        statefulset_name: statefulset_name.ok_or_else(usage)?,
        headless_service: headless_service.ok_or_else(usage)?,
        namespace: namespace.ok_or_else(usage)?,
        cluster_domain,
        resolve_timeout,
    })
}

fn usage() -> String {
    String::from(
        "usage: allocdb-k8s-layout --workspace-root <path> --statefulset-name <name> --headless-service <name> --namespace <name> [--cluster-domain <name>] [--resolve-timeout-secs <secs>]",
    )
}

fn build_layout(args: &ParsedArgs) -> Result<LocalClusterLayout, String> {
    fs::create_dir_all(&args.workspace_root)
        .map_err(|error| format!("failed to create workspace root: {error}"))?;

    let replica_ips = resolve_replica_ips(args)?;
    let replicas = replica_ips
        .iter()
        .enumerate()
        .map(|(index, ip)| build_replica_config(&args.workspace_root, index, *ip))
        .collect();

    Ok(LocalClusterLayout {
        workspace_root: args.workspace_root.clone(),
        current_view: 1,
        core_config: default_local_cluster_core_config(),
        engine_config: default_local_cluster_engine_config(),
        replicas,
    })
}

fn resolve_replica_ips(args: &ParsedArgs) -> Result<Vec<IpAddr>, String> {
    let started = Instant::now();
    loop {
        match try_resolve_replica_ips(args) {
            Ok(replica_ips) => return Ok(replica_ips),
            Err(error) if started.elapsed() < args.resolve_timeout => {
                eprintln!(
                    "waiting for StatefulSet peer DNS to resolve: {error} (elapsed={}s timeout={}s)",
                    started.elapsed().as_secs(),
                    args.resolve_timeout.as_secs()
                );
                thread::sleep(Duration::from_secs(1));
            }
            Err(error) => return Err(error),
        }
    }
}

fn try_resolve_replica_ips(args: &ParsedArgs) -> Result<Vec<IpAddr>, String> {
    (0..REPLICA_COUNT)
        .map(|ordinal| {
            let host = replica_host(
                &args.statefulset_name,
                ordinal,
                &args.headless_service,
                &args.namespace,
                &args.cluster_domain,
            );
            resolve_host_ip(&host).map_err(|error| {
                format!("failed to resolve replica ordinal {ordinal} host `{host}`: {error}")
            })
        })
        .collect()
}

fn resolve_host_ip(host: &str) -> Result<IpAddr, String> {
    let mut first = None;
    for addr in (host, CONTROL_LISTENER_PORT)
        .to_socket_addrs()
        .map_err(|error| error.to_string())?
    {
        if addr.is_ipv4() {
            return Ok(addr.ip());
        }
        if first.is_none() {
            first = Some(addr.ip());
        }
    }
    first.ok_or_else(|| String::from("name resolved without any socket addresses"))
}

fn build_replica_config(
    workspace_root: &std::path::Path,
    index: usize,
    ip: IpAddr,
) -> LocalClusterReplicaConfig {
    let replica_id = ReplicaId(u64::try_from(index).unwrap_or(0).saturating_add(1));
    let workspace_dir = workspace_root.join(format!("replica-{}", replica_id.get()));
    LocalClusterReplicaConfig {
        replica_id,
        role: if replica_id.get() == 1 {
            ReplicaRole::Primary
        } else {
            ReplicaRole::Backup
        },
        workspace_dir: workspace_dir.clone(),
        log_path: PathBuf::from("/proc/1/fd/2"),
        pid_path: PathBuf::from(format!("/run/allocdb/replica-{}.pid", replica_id.get())),
        paths: ReplicaPaths::new(
            workspace_dir.join("replica.metadata"),
            workspace_dir.join("state.snapshot"),
            workspace_dir.join("state.wal"),
        ),
        control_addr: SocketAddr::new(ip, CONTROL_LISTENER_PORT),
        client_addr: SocketAddr::new(ip, CLIENT_LISTENER_PORT),
        protocol_addr: SocketAddr::new(ip, PROTOCOL_LISTENER_PORT),
    }
}

fn replica_host(
    statefulset_name: &str,
    ordinal: u64,
    headless_service: &str,
    namespace: &str,
    cluster_domain: &str,
) -> String {
    format!("{statefulset_name}-{ordinal}.{headless_service}.{namespace}.svc.{cluster_domain}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn replica_host_uses_statefulset_dns_shape() {
        assert_eq!(
            replica_host(
                "allocdb",
                2,
                "allocdb-internal",
                "allocdb-system",
                "cluster.local"
            ),
            "allocdb-2.allocdb-internal.allocdb-system.svc.cluster.local"
        );
    }

    #[test]
    fn build_replica_config_uses_kubernetes_ports_and_paths() {
        let config = build_replica_config(
            std::path::Path::new("/var/lib/allocdb"),
            1,
            IpAddr::V4(Ipv4Addr::new(10, 42, 0, 12)),
        );

        assert_eq!(config.replica_id, ReplicaId(2));
        assert_eq!(config.role, ReplicaRole::Backup);
        assert_eq!(
            config.workspace_dir,
            PathBuf::from("/var/lib/allocdb/replica-2")
        );
        assert_eq!(config.log_path, PathBuf::from("/proc/1/fd/2"));
        assert_eq!(config.pid_path, PathBuf::from("/run/allocdb/replica-2.pid"));
        assert_eq!(
            config.control_addr,
            SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(10, 42, 0, 12)),
                CONTROL_LISTENER_PORT
            )
        );
        assert_eq!(
            config.client_addr,
            SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(10, 42, 0, 12)),
                CLIENT_LISTENER_PORT
            )
        );
        assert_eq!(
            config.protocol_addr,
            SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(10, 42, 0, 12)),
                PROTOCOL_LISTENER_PORT
            )
        );
    }
}

use std::fs;
use std::io::Read;
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{SystemTime, UNIX_EPOCH};

use allocdb_node::local_cluster::{
    LocalClusterFaultState, LocalClusterLayout, LocalClusterTimelineEventKind,
    load_local_cluster_timeline, request_control_status,
};
use allocdb_node::{ReplicaId, ReplicaMetadataFile, ReplicaRole};

fn temp_workspace(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("allocdb-local-cluster-{name}-{nanos}"))
}

fn cluster_binary() -> &'static str {
    env!("CARGO_BIN_EXE_allocdb-local-cluster")
}

fn run_cluster_command(workspace_root: &Path, command: &str) -> Output {
    Command::new(cluster_binary())
        .args([command, "--workspace"])
        .arg(workspace_root)
        .output()
        .expect("cluster command should run")
}

fn run_cluster_command_with_args(workspace_root: &Path, command: &str, args: &[&str]) -> Output {
    Command::new(cluster_binary())
        .arg(command)
        .args(["--workspace"])
        .arg(workspace_root)
        .args(args)
        .output()
        .expect("cluster command should run")
}

fn assert_success(output: &Output, context: &str) {
    assert!(
        output.status.success(),
        "{context} failed\nstatus: {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

struct ClusterGuard {
    workspace_root: PathBuf,
}

impl ClusterGuard {
    fn new(workspace_root: PathBuf) -> Self {
        Self { workspace_root }
    }
}

impl Drop for ClusterGuard {
    fn drop(&mut self) {
        let _ = run_cluster_command(&self.workspace_root, "stop");
        let _ = fs::remove_dir_all(&self.workspace_root);
    }
}

fn read_listener_response(addr: SocketAddr) -> String {
    let mut stream = TcpStream::connect(addr).expect("listener should accept connection");
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("listener response should be readable");
    response
}

#[test]
fn local_cluster_runner_starts_stops_and_reuses_stable_layout() {
    let workspace_root = temp_workspace("smoke");
    let _guard = ClusterGuard::new(workspace_root.clone());

    let start_output = run_cluster_command(&workspace_root, "start");
    assert_success(&start_output, "initial cluster start");

    let layout_path = allocdb_node::local_cluster::layout_path(&workspace_root);
    let initial_layout = LocalClusterLayout::load(&layout_path).unwrap();
    assert_eq!(initial_layout.replicas.len(), 3);

    for replica in &initial_layout.replicas {
        let status = request_control_status(replica.control_addr).unwrap();
        assert_eq!(status.replica_id, replica.replica_id);
        assert_eq!(status.current_view, initial_layout.current_view);
        assert_eq!(status.control_addr, replica.control_addr);
        assert_eq!(status.client_addr, replica.client_addr);
        assert_eq!(status.protocol_addr, replica.protocol_addr);
        assert!(status.pid_path.exists());

        let metadata = ReplicaMetadataFile::new(&replica.paths.metadata_path)
            .load_metadata()
            .unwrap()
            .unwrap();
        assert_eq!(metadata.identity.replica_id, replica.replica_id);
        assert_eq!(
            metadata.role,
            if replica.replica_id == ReplicaId(1) {
                ReplicaRole::Primary
            } else {
                ReplicaRole::Backup
            }
        );
        assert!(replica.paths.wal_path.exists());
    }

    let status_output = run_cluster_command(&workspace_root, "status");
    assert_success(&status_output, "cluster status");
    let rendered_status = String::from_utf8_lossy(&status_output.stdout);
    assert!(rendered_status.contains("replica=1 state=active"));
    assert!(rendered_status.contains("replica=2 state=active"));
    assert!(rendered_status.contains("replica=3 state=active"));

    let stop_output = run_cluster_command(&workspace_root, "stop");
    assert_success(&stop_output, "cluster stop");
    for replica in &initial_layout.replicas {
        assert!(!replica.pid_path.exists());
    }

    let restart_output = run_cluster_command(&workspace_root, "start");
    assert_success(&restart_output, "cluster restart");
    let restarted_layout = LocalClusterLayout::load(&layout_path).unwrap();
    assert_eq!(restarted_layout, initial_layout);
    for replica in &restarted_layout.replicas {
        let status = request_control_status(replica.control_addr).unwrap();
        assert_eq!(status.replica_id, replica.replica_id);
        assert_eq!(status.control_addr, replica.control_addr);
    }

    let final_stop_output = run_cluster_command(&workspace_root, "stop");
    assert_success(&final_stop_output, "final cluster stop");
}

#[test]
fn local_cluster_fault_harness_crashes_restarts_and_records_isolation() {
    let workspace_root = temp_workspace("fault-harness");
    let _guard = ClusterGuard::new(workspace_root.clone());

    let start_output = run_cluster_command(&workspace_root, "start");
    assert_success(&start_output, "initial cluster start");

    let layout_path = allocdb_node::local_cluster::layout_path(&workspace_root);
    let layout = LocalClusterLayout::load(&layout_path).unwrap();
    let replica_one = layout.replica(ReplicaId(1)).unwrap();
    let replica_two = layout.replica(ReplicaId(2)).unwrap();

    let healthy_client_response = read_listener_response(replica_one.client_addr);
    assert!(healthy_client_response.contains("client transport not implemented"));

    let isolate_output =
        run_cluster_command_with_args(&workspace_root, "isolate", &["--replica-id", "1"]);
    assert_success(&isolate_output, "replica isolate");
    let fault_state = LocalClusterFaultState::load_or_create(&workspace_root).unwrap();
    assert!(fault_state.is_replica_isolated(ReplicaId(1)));
    assert!(!fault_state.is_replica_isolated(ReplicaId(2)));
    let isolated_client_response = read_listener_response(replica_one.client_addr);
    assert!(isolated_client_response.contains("network isolated by local harness"));

    let status_output = run_cluster_command(&workspace_root, "status");
    assert_success(&status_output, "cluster status after isolate");
    let rendered_status = String::from_utf8_lossy(&status_output.stdout);
    assert!(rendered_status.contains("replica=1 state=active network=isolated"));

    let heal_output =
        run_cluster_command_with_args(&workspace_root, "heal", &["--replica-id", "1"]);
    assert_success(&heal_output, "replica heal");
    let healed_state = LocalClusterFaultState::load_or_create(&workspace_root).unwrap();
    assert!(!healed_state.is_replica_isolated(ReplicaId(1)));
    let healed_client_response = read_listener_response(replica_one.client_addr);
    assert!(healed_client_response.contains("client transport not implemented"));

    let crash_output =
        run_cluster_command_with_args(&workspace_root, "crash", &["--replica-id", "1"]);
    assert_success(&crash_output, "replica crash");
    assert!(request_control_status(replica_one.control_addr).is_err());
    assert!(request_control_status(replica_two.control_addr).is_ok());
    assert!(!replica_one.pid_path.exists());

    let restart_output =
        run_cluster_command_with_args(&workspace_root, "restart", &["--replica-id", "1"]);
    assert_success(&restart_output, "replica restart");
    let restarted_status = request_control_status(replica_one.control_addr).unwrap();
    assert_eq!(restarted_status.replica_id, ReplicaId(1));
    assert_eq!(restarted_status.control_addr, replica_one.control_addr);
    assert!(replica_one.pid_path.exists());

    let stop_output = run_cluster_command(&workspace_root, "stop");
    assert_success(&stop_output, "cluster stop");

    let timeline = load_local_cluster_timeline(&workspace_root).unwrap();
    let kinds = timeline.iter().map(|event| event.kind).collect::<Vec<_>>();
    assert_eq!(
        kinds,
        vec![
            LocalClusterTimelineEventKind::ClusterStart,
            LocalClusterTimelineEventKind::ReplicaIsolate,
            LocalClusterTimelineEventKind::ReplicaHeal,
            LocalClusterTimelineEventKind::ReplicaCrash,
            LocalClusterTimelineEventKind::ReplicaRestart,
            LocalClusterTimelineEventKind::ClusterStop,
        ]
    );
    assert_eq!(timeline[1].replica_id, Some(ReplicaId(1)));
    assert_eq!(timeline[3].replica_id, Some(ReplicaId(1)));
}

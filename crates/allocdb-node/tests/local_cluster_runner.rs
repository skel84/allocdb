use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{SystemTime, UNIX_EPOCH};

use allocdb_node::local_cluster::{LocalClusterLayout, request_control_status};
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

#[test]
fn local_cluster_runner_starts_stops_and_reuses_stable_layout() {
    let workspace_root = temp_workspace("smoke");
    let guard = ClusterGuard::new(workspace_root.clone());

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
    drop(guard);
}

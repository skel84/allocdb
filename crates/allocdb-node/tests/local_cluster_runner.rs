use std::fs;
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use allocdb_core::command::{ClientRequest, Command as AllocCommand};
use allocdb_core::ids::{ClientId, Lsn, OperationId, ResourceId, Slot};
use allocdb_node::local_cluster::{
    LocalClusterFaultState, LocalClusterLayout, LocalClusterTimelineEventKind,
    load_local_cluster_timeline, request_control_status,
};
use allocdb_node::{
    ApiRequest, ApiResponse, ReplicaId, ReplicaMetadataFile, ReplicaRole, ResourceRequest,
    ResourceResponse, SubmitRequest, SubmitResponse, decode_response, encode_request,
};

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

fn local_cluster_test_guard() -> MutexGuard<'static, ()> {
    static GUARD: OnceLock<Mutex<()>> = OnceLock::new();
    GUARD
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
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

fn start_cluster_with_retry(workspace_root: &Path) -> Output {
    let first = run_cluster_command(workspace_root, "start");
    if first.status.success() {
        return first;
    }

    let _ = run_cluster_command(workspace_root, "stop");
    let _ = fs::remove_dir_all(workspace_root);
    run_cluster_command(workspace_root, "start")
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

fn send_raw_bytes(addr: SocketAddr, request_bytes: &[u8]) -> Vec<u8> {
    let mut stream = TcpStream::connect(addr).expect("listener should accept request bytes");
    stream
        .write_all(request_bytes)
        .expect("request bytes should write");
    stream
        .shutdown(Shutdown::Write)
        .expect("request half-close should succeed");
    let mut response_bytes = Vec::new();
    stream
        .read_to_end(&mut response_bytes)
        .expect("response bytes should be readable");
    response_bytes
}

fn send_api_request(addr: SocketAddr, request: &ApiRequest) -> ApiResponse {
    let request_bytes = encode_request(request).expect("api request should encode");
    let mut stream = TcpStream::connect(addr).expect("listener should accept api request");
    stream
        .write_all(&request_bytes)
        .expect("api request should write");
    stream
        .shutdown(Shutdown::Write)
        .expect("api request half-close should succeed");
    let mut response_bytes = Vec::new();
    stream
        .read_to_end(&mut response_bytes)
        .expect("api response should be readable");
    decode_response(&response_bytes).unwrap_or_else(|error| {
        panic!(
            "api response should decode: {error:?}\nraw={:?}",
            String::from_utf8_lossy(&response_bytes)
        )
    })
}

fn send_raw_api_request(addr: SocketAddr, request: &ApiRequest) -> Vec<u8> {
    let request_bytes = encode_request(request).expect("api request should encode");
    let mut stream = TcpStream::connect(addr).expect("listener should accept api request");
    stream
        .write_all(&request_bytes)
        .expect("api request should write");
    stream
        .shutdown(Shutdown::Write)
        .expect("api request half-close should succeed");
    let mut response_bytes = Vec::new();
    stream
        .read_to_end(&mut response_bytes)
        .expect("api response should be readable");
    response_bytes
}

fn create_request(resource_id: u128, operation_id: u128) -> ClientRequest {
    ClientRequest {
        operation_id: OperationId(operation_id),
        client_id: ClientId(7),
        command: AllocCommand::CreateResource {
            resource_id: ResourceId(resource_id),
        },
    }
}

#[test]
fn local_cluster_runner_starts_stops_and_reuses_stable_layout() {
    let _serial_guard = local_cluster_test_guard();
    let workspace_root = temp_workspace("smoke");
    let _guard = ClusterGuard::new(workspace_root.clone());

    let start_output = start_cluster_with_retry(&workspace_root);
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

    let restart_output = start_cluster_with_retry(&workspace_root);
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
    let _serial_guard = local_cluster_test_guard();
    let workspace_root = temp_workspace("fault-harness");
    let _guard = ClusterGuard::new(workspace_root.clone());

    let start_output = start_cluster_with_retry(&workspace_root);
    assert_success(&start_output, "initial cluster start");

    let layout_path = allocdb_node::local_cluster::layout_path(&workspace_root);
    let layout = LocalClusterLayout::load(&layout_path).unwrap();
    let replica_one = layout.replica(ReplicaId(1)).unwrap();
    let replica_two = layout.replica(ReplicaId(2)).unwrap();

    let healthy_client_response =
        String::from_utf8_lossy(&send_raw_bytes(replica_one.client_addr, &[0xFF])).into_owned();
    assert!(healthy_client_response.contains("invalid client request"));

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
    let healed_client_response =
        String::from_utf8_lossy(&send_raw_bytes(replica_one.client_addr, &[0xFF])).into_owned();
    assert!(healed_client_response.contains("invalid client request"));

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

#[test]
fn local_cluster_client_transport_commits_reads_and_retries() {
    let _serial_guard = local_cluster_test_guard();
    let workspace_root = temp_workspace("client-transport");
    let _guard = ClusterGuard::new(workspace_root.clone());

    let start_output = start_cluster_with_retry(&workspace_root);
    assert_success(&start_output, "initial cluster start");

    let layout_path = allocdb_node::local_cluster::layout_path(&workspace_root);
    let layout = LocalClusterLayout::load(&layout_path).unwrap();
    let primary = layout.replica(ReplicaId(1)).unwrap();
    let backup = layout.replica(ReplicaId(2)).unwrap();

    let submit = send_api_request(
        primary.client_addr,
        &ApiRequest::Submit(SubmitRequest::from_client_request(
            Slot(1),
            create_request(41, 9001),
        )),
    );
    match submit {
        ApiResponse::Submit(SubmitResponse::Committed(response)) => {
            assert_eq!(response.applied_lsn, Lsn(1));
            assert!(!response.from_retry_cache);
        }
        other => panic!("expected committed submit response, got {other:?}"),
    }

    for replica in &layout.replicas {
        let status = request_control_status(replica.control_addr).unwrap();
        assert_eq!(status.commit_lsn, Some(Lsn(1)));
    }

    let read = send_api_request(
        primary.client_addr,
        &ApiRequest::GetResource(ResourceRequest {
            resource_id: ResourceId(41),
            required_lsn: Some(Lsn(1)),
        }),
    );
    match read {
        ApiResponse::GetResource(ResourceResponse::Found(resource)) => {
            assert_eq!(resource.resource_id, ResourceId(41));
        }
        other => panic!("expected found resource response, got {other:?}"),
    }

    let retry = send_api_request(
        primary.client_addr,
        &ApiRequest::Submit(SubmitRequest::from_client_request(
            Slot(1),
            create_request(41, 9001),
        )),
    );
    match retry {
        ApiResponse::Submit(SubmitResponse::Committed(response)) => {
            assert_eq!(response.applied_lsn, Lsn(1));
            assert!(response.from_retry_cache);
        }
        other => panic!("expected retry-cache submit response, got {other:?}"),
    }

    let backup_read = send_raw_api_request(
        backup.client_addr,
        &ApiRequest::GetResource(ResourceRequest {
            resource_id: ResourceId(41),
            required_lsn: Some(Lsn(1)),
        }),
    );
    let backup_read = String::from_utf8_lossy(&backup_read);
    assert!(backup_read.contains("not primary"));
}

#[test]
fn local_cluster_client_transport_retries_same_ambiguous_write() {
    let _serial_guard = local_cluster_test_guard();
    let workspace_root = temp_workspace("ambiguous-retry");
    let _guard = ClusterGuard::new(workspace_root.clone());

    let start_output = start_cluster_with_retry(&workspace_root);
    assert_success(&start_output, "initial cluster start");

    let layout_path = allocdb_node::local_cluster::layout_path(&workspace_root);
    let layout = LocalClusterLayout::load(&layout_path).unwrap();
    let primary = layout.replica(ReplicaId(1)).unwrap();

    let isolate_two =
        run_cluster_command_with_args(&workspace_root, "isolate", &["--replica-id", "2"]);
    assert_success(&isolate_two, "replica two isolate");
    let isolate_three =
        run_cluster_command_with_args(&workspace_root, "isolate", &["--replica-id", "3"]);
    assert_success(&isolate_three, "replica three isolate");

    let first_submit = send_api_request(
        primary.client_addr,
        &ApiRequest::Submit(SubmitRequest::from_client_request(
            Slot(2),
            create_request(77, 9100),
        )),
    );
    match first_submit {
        ApiResponse::Submit(SubmitResponse::Rejected(response)) => {
            assert_eq!(
                response.category,
                allocdb_node::SubmissionErrorCategory::Indefinite
            );
            assert_eq!(
                response.code,
                allocdb_node::SubmissionFailureCode::StorageFailure
            );
        }
        other => panic!("expected ambiguous submit rejection, got {other:?}"),
    }

    let conflicting_submit = send_api_request(
        primary.client_addr,
        &ApiRequest::Submit(SubmitRequest::from_client_request(
            Slot(3),
            create_request(88, 9200),
        )),
    );
    match conflicting_submit {
        ApiResponse::Submit(SubmitResponse::Rejected(response)) => {
            assert_eq!(
                response.category,
                allocdb_node::SubmissionErrorCategory::Indefinite
            );
            assert_eq!(
                response.code,
                allocdb_node::SubmissionFailureCode::StorageFailure
            );
        }
        other => panic!("expected pending-ambiguity rejection, got {other:?}"),
    }

    let heal_two = run_cluster_command_with_args(&workspace_root, "heal", &["--replica-id", "2"]);
    assert_success(&heal_two, "replica two heal");

    let retry_submit = send_api_request(
        primary.client_addr,
        &ApiRequest::Submit(SubmitRequest::from_client_request(
            Slot(2),
            create_request(77, 9100),
        )),
    );
    let committed_lsn = match retry_submit {
        ApiResponse::Submit(SubmitResponse::Committed(response)) => {
            assert!(!response.from_retry_cache);
            response.applied_lsn
        }
        other => panic!("expected committed retry response, got {other:?}"),
    };
    assert_eq!(committed_lsn, Lsn(1));

    let primary_status = request_control_status(primary.control_addr).unwrap();
    assert_eq!(primary_status.commit_lsn, Some(Lsn(1)));

    let read = send_api_request(
        primary.client_addr,
        &ApiRequest::GetResource(ResourceRequest {
            resource_id: ResourceId(77),
            required_lsn: Some(Lsn(1)),
        }),
    );
    assert!(matches!(
        read,
        ApiResponse::GetResource(ResourceResponse::Found(_))
    ));
}

use super::*;
use crate::events::{
    RemoteApiOutcome, ResourceReadObservation, classify_resource_read_outcome,
    map_reservation_state, outcome_from_submission_failure,
};
use crate::kubevirt::KubevirtWatchLaneSpec;
use crate::remote::{
    extract_probe_commit_lsn, validate_probe_read_response, validate_protocol_probe_response,
};
use crate::runs::{enforce_minimum_fault_window, fault_window_complete, resolve_run_spec};
use crate::runtime::{
    RuntimeReplicaProbe, RuntimeReplicaTopology, live_runtime_replica_matching,
    render_runtime_probe_summary, summarize_runtime_probes,
};
use crate::support::{
    HistoryBuilder, copy_file_or_remove, disable_local_tar_copyfile_metadata, prepare_log_path_for,
    temp_staging_dir,
};
use crate::tracker::{RequestNamespace, RunStatusSnapshot, RunTrackerState};
use crate::watch_render::{
    compact_counter, compact_fault_window_progress, parse_watch_event_line, progress_bar,
};
use allocdb_core::{
    ids::{HolderId, Lsn, ReservationId, ResourceId, Slot},
    result::ResultCode,
};
use allocdb_node::jepsen::{
    JepsenAmbiguousOutcome, JepsenCommittedWrite, JepsenDefiniteFailure, JepsenEventOutcome,
    JepsenNemesisFamily, JepsenOperation, JepsenOperationKind, JepsenReadState, JepsenReadTarget,
    JepsenReservationState, JepsenSuccessfulRead, JepsenWorkloadFamily, JepsenWriteResult,
    analyze_history, release_gate_plan,
};
use allocdb_node::local_cluster::{
    LocalClusterReplicaConfig, ReplicaRuntimeState, ReplicaRuntimeStatus,
};
use allocdb_node::{
    ApiResponse, LeaseViewState, ReplicaId, ReplicaPaths, ReplicaRole, ResourceResponse,
    ResourceViewState, SubmissionFailure, SubmissionFailureCode, SubmitResponse,
};
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use tracker::RunTrackerPhase;

fn test_replica_config(replica_id: u64, role: ReplicaRole) -> LocalClusterReplicaConfig {
    let root = PathBuf::from(format!("/tmp/jepsen-test-replica-{replica_id}"));
    LocalClusterReplicaConfig {
        replica_id: ReplicaId(replica_id),
        role,
        workspace_dir: root.clone(),
        log_path: root.join("replica.log"),
        pid_path: root.join("replica.pid"),
        paths: ReplicaPaths::new(
            root.join("replica.metadata"),
            root.join("state.snapshot"),
            root.join("state.wal"),
        ),
        control_addr: format!("127.0.0.1:{}", 17_000 + replica_id)
            .parse()
            .unwrap(),
        client_addr: format!("127.0.0.1:{}", 18_000 + replica_id)
            .parse()
            .unwrap(),
        protocol_addr: format!("127.0.0.1:{}", 19_000 + replica_id)
            .parse()
            .unwrap(),
    }
}

fn test_replica_status(replica: &LocalClusterReplicaConfig) -> ReplicaRuntimeStatus {
    ReplicaRuntimeStatus {
        process_id: 42,
        replica_id: replica.replica_id,
        state: ReplicaRuntimeState::Active,
        role: replica.role,
        current_view: 7,
        commit_lsn: Some(Lsn(11)),
        active_snapshot_lsn: Some(Lsn(11)),
        accepting_writes: Some(replica.role == ReplicaRole::Primary),
        startup_kind: None,
        loaded_snapshot_lsn: Some(Lsn(11)),
        replayed_wal_frame_count: Some(0),
        replayed_wal_last_lsn: Some(Lsn(11)),
        fault_reason: None,
        workspace_dir: replica.workspace_dir.clone(),
        log_path: replica.log_path.clone(),
        pid_path: replica.pid_path.clone(),
        metadata_path: replica.paths.metadata_path.clone(),
        prepare_log_path: prepare_log_path_for(&replica.paths.metadata_path),
        snapshot_path: replica.paths.snapshot_path.clone(),
        wal_path: replica.paths.wal_path.clone(),
        control_addr: replica.control_addr,
        client_addr: replica.client_addr,
        protocol_addr: replica.protocol_addr,
    }
}

fn test_faulted_replica_status(
    replica: &LocalClusterReplicaConfig,
    reason: &str,
) -> ReplicaRuntimeStatus {
    let mut status = test_replica_status(replica);
    status.state = ReplicaRuntimeState::Faulted;
    status.fault_reason = Some(reason.to_string());
    status
}

#[test]
fn release_gate_plan_includes_faulted_qemu_runs() {
    let runs = release_gate_plan();
    assert!(runs.iter().any(|run| {
        run.workload == JepsenWorkloadFamily::FailoverReadFences
            && run.nemesis == JepsenNemesisFamily::CrashRestart
    }));
    assert!(runs.iter().any(|run| {
        run.workload == JepsenWorkloadFamily::ExpirationAndRecovery
            && run.nemesis == JepsenNemesisFamily::MixedFailover
    }));
}

#[test]
fn indefinite_submission_failure_maps_to_ambiguous_outcome() {
    let outcome = outcome_from_submission_failure(&SubmissionFailure {
        category: allocdb_node::SubmissionErrorCategory::Indefinite,
        code: SubmissionFailureCode::StorageFailure,
    });
    assert_eq!(
        outcome,
        JepsenEventOutcome::Ambiguous(JepsenAmbiguousOutcome::IndefiniteWrite)
    );
}

#[test]
fn expired_reservation_maps_to_released_read_state() {
    let state = map_reservation_state(&allocdb_node::LeaseView {
        lease_id: ReservationId(11),
        holder_id: HolderId(33),
        lease_epoch: 2,
        state: LeaseViewState::Expired,
        created_lsn: Lsn(1),
        deadline_slot: Some(Slot(9)),
        released_lsn: Some(Lsn(4)),
        retire_after_slot: Some(Slot(17)),
        member_resource_ids: vec![ResourceId(22)],
    });
    assert_eq!(
        state,
        JepsenReservationState::Released {
            resource_id: ResourceId(22),
            holder_id: 33,
            released_lsn: Some(Lsn(4)),
        }
    );
}

#[test]
fn remote_tcp_probe_command_places_args_before_heredoc() {
    let command = build_remote_tcp_probe_command("127.0.0.1", 9000, "deadbeef");
    assert!(command.starts_with(&format!(
        "python3 - 127.0.0.1 9000 deadbeef {EXTERNAL_REMOTE_TCP_TIMEOUT_SECS} <<'PY'\n"
    )));
    assert!(
        command.contains("payload = b'' if sys.argv[3] == '-' else bytes.fromhex(sys.argv[3])")
    );
    assert!(command.contains("timeout_secs = float(sys.argv[4])"));
    assert!(command.ends_with("\nPY"));
}

#[test]
fn remote_tcp_probe_command_preserves_empty_payload_argument() {
    let command = build_remote_tcp_probe_command("127.0.0.1", 9000, "");
    assert!(command.starts_with(&format!(
        "python3 - 127.0.0.1 9000 - {EXTERNAL_REMOTE_TCP_TIMEOUT_SECS} <<'PY'\n"
    )));
    assert!(command.contains("payload = b'' if sys.argv[3] == '-'"));
}

#[test]
fn protocol_probe_rejects_placeholder_responses() {
    let not_ready = validate_protocol_probe_response(
        "qemu",
        ReplicaId(2),
        b"protocol transport not implemented",
    )
    .unwrap_err();
    assert!(not_ready.contains("replica 2"));

    let isolated = validate_protocol_probe_response(
        "qemu",
        ReplicaId(3),
        b"network isolated by local harness",
    )
    .unwrap_err();
    assert!(isolated.contains("replica 3"));

    assert!(validate_protocol_probe_response("qemu", ReplicaId(4), b"").is_ok());
}

#[test]
fn probe_submit_and_read_validation_cover_pass_and_fail_paths() {
    let applied_lsn = extract_probe_commit_lsn(
        "qemu",
        ApiResponse::Submit(SubmitResponse::Committed(
            allocdb_node::SubmissionResult {
                applied_lsn: Lsn(9),
                outcome: allocdb_core::result::CommandOutcome::new(ResultCode::Ok),
                from_retry_cache: false,
            }
            .into(),
        )),
    )
    .unwrap();
    assert_eq!(applied_lsn, Lsn(9));

    let submit_error =
        extract_probe_commit_lsn("qemu", ApiResponse::GetResource(ResourceResponse::NotFound))
            .unwrap_err();
    assert!(submit_error.contains("did not commit"));

    let ok_read = ApiResponse::GetResource(ResourceResponse::Found(allocdb_node::ResourceView {
        resource_id: ResourceId(41),
        state: ResourceViewState::Available,
        current_lease_id: None,
        version: 1,
    }));
    assert!(validate_probe_read_response("qemu", 41, &ok_read).is_ok());

    let read_error = validate_probe_read_response(
        "qemu",
        41,
        &ApiResponse::GetResource(ResourceResponse::NotFound),
    )
    .unwrap_err();
    assert!(read_error.contains("unexpected response"));
}

#[test]
fn classify_resource_read_outcome_distinguishes_available_and_held_states() {
    let available = classify_resource_read_outcome(
        ResourceId(41),
        RemoteApiOutcome::Api(ApiResponse::GetResource(ResourceResponse::Found(
            allocdb_node::ResourceView {
                resource_id: ResourceId(41),
                state: ResourceViewState::Available,
                current_lease_id: None,
                version: 2,
            },
        ))),
    )
    .unwrap();
    assert_eq!(available, ResourceReadObservation::Available);

    let held = classify_resource_read_outcome(
        ResourceId(41),
        RemoteApiOutcome::Api(ApiResponse::GetResource(ResourceResponse::Found(
            allocdb_node::ResourceView {
                resource_id: ResourceId(41),
                state: ResourceViewState::Reserved,
                current_lease_id: Some(ReservationId(7)),
                version: 3,
            },
        ))),
    )
    .unwrap();
    assert_eq!(
        held,
        ResourceReadObservation::Held {
            state: ResourceViewState::Reserved,
            current_reservation_id: Some(ReservationId(7)),
            version: 3,
        }
    );
}

#[test]
fn classify_resource_read_outcome_maps_not_primary_text() {
    let observation = classify_resource_read_outcome(
        ResourceId(11),
        RemoteApiOutcome::Text(String::from("not primary: role=backup")),
    )
    .unwrap();
    assert_eq!(observation, ResourceReadObservation::NotPrimary);
}

#[test]
fn classify_resource_read_outcome_rejects_mismatched_resource() {
    let error = classify_resource_read_outcome(
        ResourceId(11),
        RemoteApiOutcome::Api(ApiResponse::GetResource(ResourceResponse::Found(
            allocdb_node::ResourceView {
                resource_id: ResourceId(12),
                state: ResourceViewState::Available,
                current_lease_id: None,
                version: 0,
            },
        ))),
    )
    .unwrap_err();
    assert!(error.contains("mismatched resource 12"));
}

#[test]
fn resolve_run_spec_and_minimum_fault_window_are_enforced() {
    let control = resolve_run_spec("reservation_contention-control").unwrap();
    assert!(enforce_minimum_fault_window(&control, Duration::from_secs(0)).is_ok());

    let faulted = resolve_run_spec("reservation_contention-crash-restart").unwrap();
    let error = enforce_minimum_fault_window(&faulted, Duration::from_secs(10)).unwrap_err();
    assert!(error.contains("fault window"));
    assert!(error.contains("reservation_contention-crash-restart"));

    let unknown = resolve_run_spec("missing-run").unwrap_err();
    assert!(unknown.contains("unknown Jepsen run id"));
}

#[test]
fn copy_file_or_remove_copies_and_removes_stale_destination() {
    let root = std::env::temp_dir().join(format!(
        "allocdb-jepsen-copy-{}",
        unique_probe_resource_id()
    ));
    fs::create_dir_all(&root).unwrap();
    let source = root.join("source.bin");
    let destination = root.join("destination.bin");

    fs::write(&source, b"snapshot-bytes").unwrap();
    copy_file_or_remove(&source, &destination).unwrap();
    assert_eq!(fs::read(&destination).unwrap(), b"snapshot-bytes");

    fs::remove_file(&source).unwrap();
    copy_file_or_remove(&source, &destination).unwrap();
    assert!(!destination.exists());

    let _ = fs::remove_dir_all(&root);
}

#[test]
fn unique_probe_resource_id_is_monotonic() {
    let first = unique_probe_resource_id();
    let second = unique_probe_resource_id();
    assert!(second > first);
}

#[test]
fn request_namespace_monotonicity_covers_verify_then_execute_ordering() {
    let first = RequestNamespace::new();
    let second = RequestNamespace::new();
    assert_ne!(first.client_id(), second.client_id());
    assert!(second.slot(1).get() >= first.slot(1).get());
    assert!(first.slot(11).get() > first.slot(10).get());
}

#[test]
fn temp_staging_dir_uses_unique_paths_for_same_prefix() {
    let first = temp_staging_dir("replica-1").unwrap();
    let second = temp_staging_dir("replica-1").unwrap();
    assert_ne!(first, second);
    let _ = fs::remove_dir_all(first);
    let _ = fs::remove_dir_all(second);
}

#[test]
fn run_status_snapshot_round_trips_through_text_codec() {
    let snapshot = RunStatusSnapshot {
        backend_name: String::from("kubevirt"),
        run_id: String::from("reservation_contention-control"),
        state: RunTrackerState::Running,
        phase: RunTrackerPhase::Executing,
        detail: String::from("executing Jepsen scenario"),
        started_at_millis: 11,
        updated_at_millis: 42,
        elapsed_secs: 31,
        minimum_fault_window_secs: Some(1800),
        history_events: 7,
        history_file: Some(PathBuf::from("/tmp/history.txt")),
        artifact_bundle: None,
        logs_archive: Some(PathBuf::from("/tmp/logs.tar.gz")),
        release_gate_passed: None,
        blockers: Some(0),
        last_error: Some(String::from("none yet")),
    };

    let encoded = tracker::encode_run_status_snapshot(&snapshot);
    let decoded = tracker::decode_run_status_snapshot(&encoded).unwrap();
    assert_eq!(decoded, snapshot);
}

#[test]
fn run_status_snapshot_round_trips_multiline_error_and_detail() {
    let snapshot = RunStatusSnapshot {
        backend_name: String::from("kubevirt"),
        run_id: String::from("lane-a"),
        state: RunTrackerState::Failed,
        phase: RunTrackerPhase::Failed,
        detail: String::from("collecting logs\nand artifacts"),
        started_at_millis: 1,
        updated_at_millis: 2,
        elapsed_secs: 1,
        minimum_fault_window_secs: None,
        history_events: 0,
        history_file: None,
        artifact_bundle: None,
        logs_archive: None,
        release_gate_passed: Some(false),
        blockers: Some(1),
        last_error: Some(String::from("probe failed\nconnection refused")),
    };

    let encoded = tracker::encode_run_status_snapshot(&snapshot);
    let decoded = tracker::decode_run_status_snapshot(&encoded).unwrap();
    assert_eq!(decoded, snapshot);
}

#[test]
fn tracker_field_round_trips_newlines_and_backslashes() {
    let original = "C:\\tmp\\foo\nline2";
    let encoded = tracker::encode_tracker_field(original);
    let decoded = tracker::decode_tracker_field(&encoded);
    assert_eq!(decoded, original);
}

#[test]
fn compact_counter_formats_large_values() {
    assert_eq!(compact_counter(999), "999");
    assert_eq!(compact_counter(1_234), "1.2K");
    assert_eq!(compact_counter(12_500_000), "12.5M");
    assert_eq!(compact_counter(7_200_000_000), "7.2B");
}

#[test]
fn parse_watch_event_line_extracts_timestamp_and_detail() {
    let event =
        parse_watch_event_line("time_millis=1773492183417 detail=collecting logs and artifacts")
            .unwrap();
    assert_eq!(event.time_millis, 1_773_492_183_417);
    assert_eq!(event.detail, "collecting logs and artifacts");
}

#[test]
fn parse_watch_event_line_unescapes_multiline_detail() {
    let event = parse_watch_event_line("time_millis=7 detail=error:\\nconnection refused").unwrap();
    assert_eq!(event.time_millis, 7);
    assert_eq!(event.detail, "error:\nconnection refused");
}

#[test]
fn progress_bar_clamps_to_requested_width() {
    assert_eq!(progress_bar(0, 10, 5), "░░░░░");
    assert_eq!(progress_bar(5, 10, 5), "██░░░");
    assert_eq!(progress_bar(15, 10, 5), "█████");
}

#[test]
fn fault_window_completion_distinguishes_control_and_long_fault_runs() {
    assert!(fault_window_complete(None, Duration::from_secs(0)));
    assert!(!fault_window_complete(
        Some(Duration::from_secs(1_800)),
        Duration::from_secs(1_799),
    ));
    assert!(fault_window_complete(
        Some(Duration::from_secs(1_800)),
        Duration::from_secs(1_800),
    ));
}

#[test]
fn history_builder_preserves_nonzero_sequence_offsets() {
    let mut history = HistoryBuilder::new(None, 7);
    history.push(
        "replica-1",
        JepsenOperation {
            kind: JepsenOperationKind::Reserve,
            operation_id: Some(11),
            resource_id: Some(ResourceId(21)),
            reservation_id: None,
            holder_id: Some(31),
            required_lsn: None,
            request_slot: Some(Slot(41)),
            ttl_slots: Some(5),
        },
        JepsenEventOutcome::Ambiguous(JepsenAmbiguousOutcome::Timeout),
    );
    history.push(
        "replica-2",
        JepsenOperation {
            kind: JepsenOperationKind::Reserve,
            operation_id: Some(12),
            resource_id: Some(ResourceId(22)),
            reservation_id: None,
            holder_id: Some(32),
            required_lsn: None,
            request_slot: Some(Slot(42)),
            ttl_slots: Some(5),
        },
        JepsenEventOutcome::Ambiguous(JepsenAmbiguousOutcome::Timeout),
    );
    let events = history.finish();
    assert_eq!(events[0].sequence, 8);
    assert_eq!(events[1].sequence, 9);
}

#[test]
fn analyzer_accepts_failover_read_fence_history_once_ambiguity_is_retried() {
    let reserve_operation_id = 21;
    let ambiguous_operation_id = 22;
    let resource_id = ResourceId(101);
    let reservation_id = ReservationId(55);
    let committed_lsn = Lsn(44);

    let mut history = HistoryBuilder::new(None, 0);
    history.push(
        "primary-1",
        JepsenOperation {
            kind: JepsenOperationKind::Reserve,
            operation_id: Some(reserve_operation_id),
            resource_id: Some(resource_id),
            reservation_id: None,
            holder_id: Some(604),
            required_lsn: None,
            request_slot: Some(Slot(53)),
            ttl_slots: Some(6),
        },
        JepsenEventOutcome::CommittedWrite(JepsenCommittedWrite {
            applied_lsn: committed_lsn,
            result: JepsenWriteResult::Reserved {
                resource_id,
                holder_id: 604,
                reservation_id: reservation_id.get(),
                expires_at_slot: Slot(900),
            },
        }),
    );
    history.push(
        "primary-1",
        JepsenOperation {
            kind: JepsenOperationKind::Reserve,
            operation_id: Some(ambiguous_operation_id),
            resource_id: Some(resource_id),
            reservation_id: None,
            holder_id: Some(605),
            required_lsn: None,
            request_slot: Some(Slot(54)),
            ttl_slots: Some(2),
        },
        JepsenEventOutcome::Ambiguous(JepsenAmbiguousOutcome::IndefiniteWrite),
    );
    history.push(
        "primary-2",
        JepsenOperation {
            kind: JepsenOperationKind::GetReservation,
            operation_id: None,
            resource_id: None,
            reservation_id: Some(reservation_id.get()),
            holder_id: None,
            required_lsn: Some(committed_lsn),
            request_slot: Some(Slot(54)),
            ttl_slots: None,
        },
        JepsenEventOutcome::SuccessfulRead(JepsenSuccessfulRead {
            target: JepsenReadTarget::Reservation,
            served_by: ReplicaId(2),
            served_role: ReplicaRole::Primary,
            observed_lsn: Some(committed_lsn),
            state: JepsenReadState::Reservation(JepsenReservationState::Active {
                resource_id,
                holder_id: 604,
                expires_at_slot: Slot(900),
                confirmed: false,
            }),
        }),
    );
    history.push(
        "primary-2",
        JepsenOperation {
            kind: JepsenOperationKind::Reserve,
            operation_id: Some(ambiguous_operation_id),
            resource_id: Some(resource_id),
            reservation_id: None,
            holder_id: Some(605),
            required_lsn: None,
            request_slot: Some(Slot(54)),
            ttl_slots: Some(2),
        },
        JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::Conflict),
    );
    history.push(
        "backup-1",
        JepsenOperation {
            kind: JepsenOperationKind::GetReservation,
            operation_id: None,
            resource_id: None,
            reservation_id: Some(reservation_id.get()),
            holder_id: None,
            required_lsn: Some(committed_lsn),
            request_slot: Some(Slot(54)),
            ttl_slots: None,
        },
        JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::NotPrimary),
    );

    let report = analyze_history(&history.finish());
    assert!(report.release_gate_passed());
    assert!(report.blockers.is_empty());
}

#[test]
fn live_runtime_replica_matching_ignores_down_and_faulted_replicas() {
    let primary = test_replica_config(1, ReplicaRole::Primary);
    let backup = test_replica_config(2, ReplicaRole::Backup);
    let down = test_replica_config(3, ReplicaRole::Backup);
    let faulted = test_replica_config(4, ReplicaRole::Backup);
    let probes = vec![
        RuntimeReplicaProbe {
            replica: down,
            status: Err(String::from("connection refused")),
        },
        RuntimeReplicaProbe {
            replica: faulted.clone(),
            status: Ok(test_faulted_replica_status(&faulted, "stale metadata")),
        },
        RuntimeReplicaProbe {
            replica: backup.clone(),
            status: Ok(test_replica_status(&backup)),
        },
        RuntimeReplicaProbe {
            replica: primary.clone(),
            status: Ok(test_replica_status(&primary)),
        },
    ];

    let discovered =
        live_runtime_replica_matching(&probes, |replica| replica.role == ReplicaRole::Primary)
            .unwrap();

    assert_eq!(discovered.replica_id, primary.replica_id);
}

#[test]
fn render_runtime_probe_summary_marks_live_faulted_and_down_replicas() {
    let primary = test_replica_config(1, ReplicaRole::Primary);
    let backup = test_replica_config(2, ReplicaRole::Backup);
    let faulted = test_replica_config(3, ReplicaRole::Backup);
    let probes = vec![
        RuntimeReplicaProbe {
            replica: primary.clone(),
            status: Ok(test_replica_status(&primary)),
        },
        RuntimeReplicaProbe {
            replica: backup.clone(),
            status: Err(String::from(
                "connection refused\ncommand terminated with exit code 1",
            )),
        },
        RuntimeReplicaProbe {
            replica: faulted.clone(),
            status: Ok(test_faulted_replica_status(
                &faulted,
                "applied lsn behind commit lsn",
            )),
        },
    ];

    let summary = render_runtime_probe_summary(&probes);

    assert!(summary.contains("1:primary@view7"));
    assert!(summary.contains("2:down(connection refused)"));
    assert!(summary.contains("3:faulted(applied lsn behind commit lsn)"));
}

#[test]
fn summarize_runtime_probes_counts_only_active_roles() {
    let primary = test_replica_config(1, ReplicaRole::Primary);
    let backup = test_replica_config(2, ReplicaRole::Backup);
    let faulted = test_replica_config(3, ReplicaRole::Backup);
    let probes = vec![
        RuntimeReplicaProbe {
            replica: primary.clone(),
            status: Ok(test_replica_status(&primary)),
        },
        RuntimeReplicaProbe {
            replica: backup.clone(),
            status: Ok(test_replica_status(&backup)),
        },
        RuntimeReplicaProbe {
            replica: faulted.clone(),
            status: Ok(test_faulted_replica_status(&faulted, "stale metadata")),
        },
    ];

    assert_eq!(
        summarize_runtime_probes(&probes),
        RuntimeReplicaTopology {
            active: 2,
            primaries: 1,
            backups: 1,
        }
    );
}

#[test]
fn parse_watch_kubevirt_lane_spec_extracts_name_workspace_and_output_root() {
    let lane = args::parse_watch_kubevirt_lane_spec("lane-a,/tmp/work-a,/tmp/out-a").unwrap();
    assert_eq!(
        lane,
        KubevirtWatchLaneSpec {
            name: String::from("lane-a"),
            workspace_root: PathBuf::from("/tmp/work-a"),
            output_root: PathBuf::from("/tmp/out-a"),
        }
    );
}

#[test]
fn parse_watch_kubevirt_lane_spec_rejects_missing_fields() {
    let error = args::parse_watch_kubevirt_lane_spec("lane-a,/tmp/work-a").unwrap_err();
    assert!(error.contains("expected <name,workspace,output-root>"));
}

#[test]
fn parse_watch_kubevirt_lane_spec_rejects_blank_fields() {
    let error = args::parse_watch_kubevirt_lane_spec("lane-a,   ,/tmp/out-a").unwrap_err();
    assert!(error.contains("expected <name,workspace,output-root>"));
}

#[test]
fn parse_args_returns_help_without_subcommand() {
    assert!(matches!(
        args::parse_args(Vec::<String>::new()).unwrap(),
        ParsedCommand::Help
    ));
}

#[test]
fn parse_args_rejects_unknown_subcommand() {
    let error = args::parse_args([String::from("wat")]).unwrap_err();
    assert!(error.contains("unknown subcommand `wat`"));
}

#[test]
fn parse_args_rejects_trailing_plan_arguments() {
    let error = args::parse_args([
        String::from("plan"),
        String::from("--workspace"),
        String::from("/tmp"),
    ])
    .unwrap_err();
    assert!(error.contains("unknown argument `--workspace /tmp`"));
}

#[test]
fn parse_args_rejects_missing_required_flags() {
    let analyze_error = args::parse_args([String::from("analyze")]).unwrap_err();
    assert!(analyze_error.contains("usage:"));

    let verify_error = args::parse_args([String::from("verify-kubevirt-surface")]).unwrap_err();
    assert!(verify_error.contains("usage:"));
}

#[test]
fn compact_fault_window_progress_formats_control_and_faulted_runs() {
    let mut snapshot = RunStatusSnapshot {
        backend_name: String::from("kubevirt"),
        run_id: String::from("reservation_contention-control"),
        state: RunTrackerState::Running,
        phase: RunTrackerPhase::Executing,
        detail: String::from("executing"),
        started_at_millis: 0,
        updated_at_millis: 0,
        elapsed_secs: 45,
        minimum_fault_window_secs: None,
        history_events: 3,
        history_file: None,
        artifact_bundle: None,
        logs_archive: None,
        release_gate_passed: None,
        blockers: None,
        last_error: None,
    };
    assert_eq!(compact_fault_window_progress(&snapshot), "ctrl");
    snapshot.minimum_fault_window_secs = Some(1_800);
    snapshot.elapsed_secs = 900;
    assert!(compact_fault_window_progress(&snapshot).contains("50%"));
}

#[test]
fn disable_local_tar_copyfile_metadata_sets_expected_env() {
    let mut command = Command::new("sh");
    disable_local_tar_copyfile_metadata(&mut command);
    let output = command
        .arg("-lc")
        .arg("printf %s \"${COPYFILE_DISABLE:-}\"")
        .output()
        .unwrap();
    let value = String::from_utf8(output.stdout).unwrap();
    if cfg!(target_os = "macos") {
        assert_eq!(value, "1");
    } else {
        assert!(value.is_empty());
    }
}

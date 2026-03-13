use std::fs;

use allocdb_core::command::{ClientRequest, Command};
use allocdb_core::command_codec::encode_client_request;
use allocdb_core::config::Config;
use allocdb_core::ids::{ClientId, Lsn, OperationId, ResourceId, Slot};

use crate::engine::EngineConfig;
use crate::replica::{NotPrimaryReadError, ReplicaId, ReplicaNodeStatus, ReplicaRole};
use crate::replicated_simulation::{
    ClusterEndpoint, QueuedProtocolMessage, ReplicaObservation, ReplicaRuntimeStatus,
    ReplicatedScheduleAction, ReplicatedScheduleActionKind, ReplicatedScheduleObservation,
    ReplicatedScheduleObservationKind, ReplicatedSimulationError, ReplicatedSimulationHarness,
};

fn core_config() -> Config {
    Config {
        shard_id: 0,
        max_resources: 8,
        max_reservations: 8,
        max_operations: 16,
        max_ttl_slots: 16,
        max_client_retry_window_slots: 8,
        reservation_history_window_slots: 4,
        max_expiration_bucket_len: 8,
    }
}

fn engine_config() -> EngineConfig {
    EngineConfig {
        max_submission_queue: 4,
        max_command_bytes: 512,
        max_expirations_per_tick: 1,
    }
}

fn replica(replica_id: u64) -> ReplicaId {
    ReplicaId(replica_id)
}

fn create_payload(resource_id: u128, operation_id: u128) -> Vec<u8> {
    encode_client_request(ClientRequest {
        operation_id: OperationId(operation_id),
        client_id: ClientId(7),
        command: Command::CreateResource {
            resource_id: ResourceId(resource_id),
        },
    })
}

fn queue_message(
    label: &'static str,
    candidate_slots: &[u64],
    from: u64,
    to: u64,
    message_label: &'static str,
) -> ReplicatedScheduleAction {
    ReplicatedScheduleAction {
        label,
        candidate_slots: candidate_slots.iter().copied().map(Slot).collect(),
        action: ReplicatedScheduleActionKind::QueueProtocolMessage {
            from: replica(from),
            to: replica(to),
            message_label,
        },
    }
}

fn deliver_message(
    label: &'static str,
    candidate_slots: &[u64],
    message_label: &'static str,
) -> ReplicatedScheduleAction {
    ReplicatedScheduleAction {
        label,
        candidate_slots: candidate_slots.iter().copied().map(Slot).collect(),
        action: ReplicatedScheduleActionKind::DeliverProtocolMessage { message_label },
    }
}

fn drop_message(
    label: &'static str,
    candidate_slots: &[u64],
    message_label: &'static str,
) -> ReplicatedScheduleAction {
    ReplicatedScheduleAction {
        label,
        candidate_slots: candidate_slots.iter().copied().map(Slot).collect(),
        action: ReplicatedScheduleActionKind::DropProtocolMessage { message_label },
    }
}

fn set_connectivity(
    label: &'static str,
    candidate_slots: &[u64],
    from: ClusterEndpoint,
    to: ClusterEndpoint,
    allowed: bool,
) -> ReplicatedScheduleAction {
    ReplicatedScheduleAction {
        label,
        candidate_slots: candidate_slots.iter().copied().map(Slot).collect(),
        action: ReplicatedScheduleActionKind::SetConnectivity { from, to, allowed },
    }
}

fn crash_replica(
    label: &'static str,
    candidate_slots: &[u64],
    replica_id: u64,
) -> ReplicatedScheduleAction {
    ReplicatedScheduleAction {
        label,
        candidate_slots: candidate_slots.iter().copied().map(Slot).collect(),
        action: ReplicatedScheduleActionKind::CrashReplica {
            replica_id: replica(replica_id),
        },
    }
}

fn restart_replica(
    label: &'static str,
    candidate_slots: &[u64],
    replica_id: u64,
) -> ReplicatedScheduleAction {
    ReplicatedScheduleAction {
        label,
        candidate_slots: candidate_slots.iter().copied().map(Slot).collect(),
        action: ReplicatedScheduleActionKind::RestartReplica {
            replica_id: replica(replica_id),
        },
    }
}

fn run_schedule(
    name: &str,
    seed: u64,
    actions: &[ReplicatedScheduleAction],
) -> (
    ReplicatedSimulationHarness,
    Vec<ReplicatedScheduleObservation>,
) {
    let mut harness =
        ReplicatedSimulationHarness::new(name, seed, core_config(), engine_config()).unwrap();
    let transcript = harness.explore_schedule(actions).unwrap();
    (harness, transcript)
}

fn transcript_for_seed(
    actions: &[ReplicatedScheduleAction],
    seed: u64,
) -> Vec<ReplicatedScheduleObservation> {
    let (_, transcript) = run_schedule("replicated-seed-search", seed, actions);
    transcript
}

fn first_distinct_seed(actions: &[ReplicatedScheduleAction], baseline_seed: u64) -> u64 {
    let baseline = transcript_for_seed(actions, baseline_seed);
    (0_u64..256)
        .find(|seed| *seed != baseline_seed && transcript_for_seed(actions, *seed) != baseline)
        .expect("test schedule must have one alternate seed with a different transcript")
}

fn delivered_message<'a>(
    transcript: &'a [ReplicatedScheduleObservation],
    label: &'static str,
) -> (&'a QueuedProtocolMessage, ReplicaObservation) {
    match &transcript
        .iter()
        .find(|entry| entry.label == label)
        .expect("transcript must contain the requested delivery label")
        .outcome
    {
        ReplicatedScheduleObservationKind::ProtocolMessageDelivered {
            message, recipient, ..
        } => (message, *recipient),
        other => panic!("expected delivered protocol message, got {other:?}"),
    }
}

fn primary_harness(name: &str, seed: u64) -> ReplicatedSimulationHarness {
    let mut harness =
        ReplicatedSimulationHarness::new(name, seed, core_config(), engine_config()).unwrap();
    harness.configure_primary(replica(1), 1).unwrap();
    harness
}

fn replica_last_applied_lsn(harness: &ReplicatedSimulationHarness, replica_id: u64) -> Option<Lsn> {
    harness
        .replica(replica(replica_id))
        .unwrap()
        .unwrap()
        .engine()
        .unwrap()
        .db()
        .last_applied_lsn()
}

fn replica_prepared_len(harness: &ReplicatedSimulationHarness, replica_id: u64) -> usize {
    harness
        .replica(replica(replica_id))
        .unwrap()
        .unwrap()
        .prepared_len()
}

fn replica_has_resource(
    harness: &ReplicatedSimulationHarness,
    replica_id: u64,
    resource_id: u128,
) -> bool {
    harness
        .replica(replica(replica_id))
        .unwrap()
        .unwrap()
        .engine()
        .unwrap()
        .db()
        .resource(ResourceId(resource_id))
        .is_some()
}

fn pending_labels(harness: &ReplicatedSimulationHarness) -> Vec<&str> {
    harness
        .pending_messages()
        .iter()
        .map(|message| message.label.as_str())
        .collect()
}

fn set_replica_link(
    harness: &mut ReplicatedSimulationHarness,
    left: u64,
    right: u64,
    allowed: bool,
) {
    harness
        .set_connectivity(
            ClusterEndpoint::Replica(replica(left)),
            ClusterEndpoint::Replica(replica(right)),
            allowed,
        )
        .unwrap();
    harness
        .set_connectivity(
            ClusterEndpoint::Replica(replica(right)),
            ClusterEndpoint::Replica(replica(left)),
            allowed,
        )
        .unwrap();
}

#[test]
fn replicated_harness_bootstraps_three_real_replicas_with_independent_workspaces() {
    let harness = ReplicatedSimulationHarness::new(
        "replicated-bootstrap",
        0x500,
        core_config(),
        engine_config(),
    )
    .unwrap();

    let replica_one_paths = harness.replica_paths(replica(1)).unwrap().clone();
    let replica_two_paths = harness.replica_paths(replica(2)).unwrap().clone();
    let replica_three_paths = harness.replica_paths(replica(3)).unwrap().clone();

    for replica_id in [replica(1), replica(2), replica(3)] {
        let node = harness.replica(replica_id).unwrap().unwrap();
        assert_eq!(node.status(), ReplicaNodeStatus::Active);
        assert_eq!(node.metadata().identity.replica_id, replica_id);
        assert!(fs::metadata(node.metadata_path()).is_ok());
    }

    assert_ne!(replica_one_paths, replica_two_paths);
    assert_ne!(replica_one_paths, replica_three_paths);
    assert_ne!(replica_two_paths, replica_three_paths);
    assert!(
        harness
            .connectivity_allows(
                ClusterEndpoint::Client,
                ClusterEndpoint::Replica(replica(1))
            )
            .unwrap()
    );
    assert!(
        harness
            .connectivity_allows(
                ClusterEndpoint::Replica(replica(1)),
                ClusterEndpoint::Replica(replica(2)),
            )
            .unwrap()
    );
}

#[test]
fn replicated_schedule_transcript_is_reproducible() {
    let actions = [
        queue_message("queue-prepare-12", &[1], 1, 2, "prepare-12"),
        queue_message("queue-prepare-13", &[1], 1, 3, "prepare-13"),
        deliver_message("deliver-prepare-12", &[2], "prepare-12"),
        drop_message("drop-prepare-13", &[2], "prepare-13"),
        set_connectivity(
            "partition-client-replica-2",
            &[3],
            ClusterEndpoint::Client,
            ClusterEndpoint::Replica(replica(2)),
            false,
        ),
        crash_replica("crash-replica-2", &[4], 2),
        restart_replica("restart-replica-2", &[5], 2),
    ];
    let baseline_seed = 0x5eed;
    let alternate_seed = first_distinct_seed(&actions, baseline_seed);

    let (_, first_transcript) = run_schedule("replicated-first", baseline_seed, &actions);
    let (_, second_transcript) = run_schedule("replicated-second", baseline_seed, &actions);
    let (_, different_transcript) = run_schedule("replicated-different", alternate_seed, &actions);

    assert_eq!(first_transcript, second_transcript);
    assert_ne!(first_transcript, different_transcript);
    assert_eq!(
        first_transcript
            .iter()
            .map(|entry| entry.slot)
            .collect::<Vec<_>>(),
        vec![
            Slot(1),
            Slot(1),
            Slot(2),
            Slot(2),
            Slot(3),
            Slot(4),
            Slot(5)
        ]
    );

    let (_, recipient) = delivered_message(&first_transcript, "deliver-prepare-12");
    assert_eq!(
        recipient.runtime_status,
        ReplicaRuntimeStatus::Running(ReplicaNodeStatus::Active)
    );

    match &first_transcript
        .iter()
        .find(|entry| entry.label == "partition-client-replica-2")
        .expect("transcript must contain the connectivity label")
        .outcome
    {
        ReplicatedScheduleObservationKind::ConnectivityChanged { from, to, allowed } => {
            assert_eq!(*from, ClusterEndpoint::Client);
            assert_eq!(*to, ClusterEndpoint::Replica(replica(2)));
            assert!(!allowed);
        }
        other => panic!("expected connectivity observation, got {other:?}"),
    }

    match &first_transcript
        .iter()
        .find(|entry| entry.label == "crash-replica-2")
        .expect("transcript must contain the crash label")
        .outcome
    {
        ReplicatedScheduleObservationKind::ReplicaCrashed { before, after } => {
            assert_eq!(
                before.runtime_status,
                ReplicaRuntimeStatus::Running(ReplicaNodeStatus::Active)
            );
            assert_eq!(after.runtime_status, ReplicaRuntimeStatus::Crashed);
        }
        other => panic!("expected crash observation, got {other:?}"),
    }

    match &first_transcript
        .iter()
        .find(|entry| entry.label == "restart-replica-2")
        .expect("transcript must contain the restart label")
        .outcome
    {
        ReplicatedScheduleObservationKind::ReplicaRestarted { before, after } => {
            assert_eq!(before.runtime_status, ReplicaRuntimeStatus::Crashed);
            assert_eq!(
                after.runtime_status,
                ReplicaRuntimeStatus::Running(ReplicaNodeStatus::Active)
            );
        }
        other => panic!("expected restart observation, got {other:?}"),
    }
}

#[test]
fn connectivity_matrix_controls_delivery_until_partition_heals() {
    let mut harness = ReplicatedSimulationHarness::new(
        "replicated-connectivity",
        0x1337,
        core_config(),
        engine_config(),
    )
    .unwrap();

    harness
        .queue_protocol_message(replica(1), replica(2), "prepare-blocked")
        .unwrap();
    harness
        .set_connectivity(
            ClusterEndpoint::Replica(replica(1)),
            ClusterEndpoint::Replica(replica(2)),
            false,
        )
        .unwrap();

    let blocked = harness
        .deliver_protocol_message("prepare-blocked")
        .unwrap_err();
    assert!(matches!(
        blocked,
        ReplicatedSimulationError::MessageDeliveryBlocked {
            label,
            from: ClusterEndpoint::Replica(source),
            to: ClusterEndpoint::Replica(target),
        } if label == "prepare-blocked" && source == replica(1) && target == replica(2)
    ));
    assert_eq!(harness.pending_messages().len(), 1);

    let dropped = harness.drop_protocol_message("prepare-blocked").unwrap();
    match dropped {
        ReplicatedScheduleObservationKind::ProtocolMessageDropped {
            pending_messages, ..
        } => assert_eq!(pending_messages, 0),
        other => panic!("expected dropped message observation, got {other:?}"),
    }

    harness
        .queue_protocol_message(replica(1), replica(2), "prepare-healed")
        .unwrap();
    harness
        .set_connectivity(
            ClusterEndpoint::Replica(replica(1)),
            ClusterEndpoint::Replica(replica(2)),
            true,
        )
        .unwrap();

    let delivered = harness.deliver_protocol_message("prepare-healed").unwrap();
    match delivered {
        ReplicatedScheduleObservationKind::ProtocolMessageDelivered {
            message,
            recipient,
            pending_messages,
        } => {
            assert_eq!(message.label, "prepare-healed");
            assert_eq!(pending_messages, 0);
            assert_eq!(recipient.replica_id, replica(2));
            assert_eq!(
                recipient.runtime_status,
                ReplicaRuntimeStatus::Running(ReplicaNodeStatus::Active)
            );
        }
        other => panic!("expected delivered message observation, got {other:?}"),
    }

    let missing_delivery = harness
        .deliver_protocol_message("unknown-label")
        .unwrap_err();
    assert!(matches!(
        missing_delivery,
        ReplicatedSimulationError::MessageNotFound(label) if label == "unknown-label"
    ));

    let missing_drop = harness.drop_protocol_message("unknown-label").unwrap_err();
    assert!(matches!(
        missing_drop,
        ReplicatedSimulationError::MessageNotFound(label) if label == "unknown-label"
    ));
}

#[test]
fn invalid_replica_access_returns_unknown_replica() {
    let harness = ReplicatedSimulationHarness::new(
        "replicated-unknown-replica",
        0x999,
        core_config(),
        engine_config(),
    )
    .unwrap();

    let error = harness.replica_paths(replica(9)).unwrap_err();
    assert!(matches!(
        error,
        ReplicatedSimulationError::UnknownReplica(replica_id) if replica_id == replica(9)
    ));
}

#[test]
fn queue_and_deliver_reject_crashed_replicas() {
    let mut harness = ReplicatedSimulationHarness::new(
        "replicated-crashed-errors",
        0x998,
        core_config(),
        engine_config(),
    )
    .unwrap();

    harness.crash_replica(replica(2)).unwrap();

    let queued_from_crashed = harness
        .queue_protocol_message(replica(2), replica(1), "prepare-from-crashed")
        .unwrap_err();
    assert!(matches!(
        queued_from_crashed,
        ReplicatedSimulationError::ReplicaAlreadyCrashed(replica_id) if replica_id == replica(2)
    ));

    harness
        .queue_protocol_message(replica(1), replica(2), "prepare-to-crashed")
        .unwrap();
    let delivered_to_crashed = harness
        .deliver_protocol_message("prepare-to-crashed")
        .unwrap_err();
    assert!(matches!(
        delivered_to_crashed,
        ReplicatedSimulationError::ReplicaAlreadyCrashed(replica_id) if replica_id == replica(2)
    ));
    assert_eq!(harness.pending_messages().len(), 1);
}

#[test]
fn restart_rejects_running_replica() {
    let mut harness = ReplicatedSimulationHarness::new(
        "replicated-running-restart",
        0x997,
        core_config(),
        engine_config(),
    )
    .unwrap();

    let error = harness.restart_replica(replica(1)).unwrap_err();
    assert!(matches!(
        error,
        ReplicatedSimulationError::ReplicaAlreadyRunning(replica_id) if replica_id == replica(1)
    ));
}

#[test]
fn crash_and_restart_keep_replica_workspace_stable() {
    let mut harness = ReplicatedSimulationHarness::new(
        "replicated-restart",
        0x404,
        core_config(),
        engine_config(),
    )
    .unwrap();
    let before_paths = harness.replica_paths(replica(2)).unwrap().clone();

    let crashed = harness.crash_replica(replica(2)).unwrap();
    match crashed {
        ReplicatedScheduleObservationKind::ReplicaCrashed { before, after } => {
            assert_eq!(
                before.runtime_status,
                ReplicaRuntimeStatus::Running(ReplicaNodeStatus::Active)
            );
            assert_eq!(after.runtime_status, ReplicaRuntimeStatus::Crashed);
        }
        other => panic!("expected crash observation, got {other:?}"),
    }
    assert!(harness.replica(replica(2)).unwrap().is_none());

    let restarted = harness.restart_replica(replica(2)).unwrap();
    match restarted {
        ReplicatedScheduleObservationKind::ReplicaRestarted { before, after } => {
            assert_eq!(before.runtime_status, ReplicaRuntimeStatus::Crashed);
            assert_eq!(
                after.runtime_status,
                ReplicaRuntimeStatus::Running(ReplicaNodeStatus::Active)
            );
            assert_eq!(after.current_view, Some(0));
            assert_eq!(after.commit_lsn, None);
        }
        other => panic!("expected restart observation, got {other:?}"),
    }

    let after_paths = harness.replica_paths(replica(2)).unwrap().clone();
    assert_eq!(before_paths, after_paths);
    assert_eq!(
        harness.replica(replica(2)).unwrap().unwrap().status(),
        ReplicaNodeStatus::Active
    );
}

#[test]
fn quorum_write_publishes_after_majority_append_and_backups_wait_for_commit() {
    let mut harness = primary_harness("replicated-quorum-write", 0x5a1);

    let entry = harness
        .client_submit(replica(1), Slot(1), &create_payload(11, 1), "client-create")
        .unwrap();
    assert_eq!(entry.view, 1);
    assert_eq!(entry.lsn, Lsn(1));
    assert_eq!(harness.published_result(entry.lsn), None);
    assert_eq!(replica_prepared_len(&harness, 1), 1);
    assert_eq!(replica_last_applied_lsn(&harness, 1), None);
    assert_eq!(replica_last_applied_lsn(&harness, 2), None);
    assert_eq!(replica_last_applied_lsn(&harness, 3), None);
    assert_eq!(
        pending_labels(&harness),
        vec![
            "client-create-prepare-1-to-2",
            "client-create-prepare-1-to-3"
        ]
    );

    harness
        .deliver_protocol_message("client-create-prepare-1-to-2")
        .unwrap();
    assert_eq!(harness.published_result(entry.lsn), None);
    assert_eq!(replica_prepared_len(&harness, 2), 1);
    assert_eq!(replica_last_applied_lsn(&harness, 2), None);

    harness
        .deliver_protocol_message("client-create-prepare-1-to-2-ack")
        .unwrap();
    let committed = *harness
        .published_result(entry.lsn)
        .expect("primary should publish after majority append");
    assert_eq!(committed.applied_lsn, entry.lsn);
    assert_eq!(replica_last_applied_lsn(&harness, 1), Some(entry.lsn));
    assert_eq!(replica_last_applied_lsn(&harness, 2), None);
    assert_eq!(replica_last_applied_lsn(&harness, 3), None);
    assert_eq!(replica_prepared_len(&harness, 1), 0);
    assert_eq!(
        pending_labels(&harness),
        vec![
            "client-create-prepare-1-to-3",
            "commit-1-to-2",
            "commit-1-to-3",
        ]
    );

    harness.deliver_protocol_message("commit-1-to-2").unwrap();
    assert_eq!(replica_last_applied_lsn(&harness, 2), Some(entry.lsn));
    assert_eq!(replica_prepared_len(&harness, 2), 0);
    assert_eq!(replica_last_applied_lsn(&harness, 3), None);

    harness
        .deliver_protocol_message("client-create-prepare-1-to-3")
        .unwrap();
    assert_eq!(replica_prepared_len(&harness, 3), 1);
    assert_eq!(replica_last_applied_lsn(&harness, 3), None);
    harness.deliver_protocol_message("commit-1-to-3").unwrap();
    assert_eq!(replica_prepared_len(&harness, 3), 0);
    assert_eq!(replica_last_applied_lsn(&harness, 3), Some(entry.lsn));
    assert!(replica_has_resource(&harness, 1, 11));
    assert!(replica_has_resource(&harness, 2, 11));
    assert!(replica_has_resource(&harness, 3, 11));
}

#[test]
fn reads_are_served_only_from_the_primary_after_local_commit() {
    let mut harness = primary_harness("replicated-primary-read", 0x5a2);

    let entry = harness
        .client_submit(replica(1), Slot(1), &create_payload(21, 2), "read-create")
        .unwrap();
    let before_commit = harness.read_resource(replica(1), ResourceId(21), Some(entry.lsn));
    assert!(matches!(
        before_commit,
        Err(ReplicatedSimulationError::Read(NotPrimaryReadError::Fence(
            _
        )))
    ));

    harness
        .deliver_protocol_message("read-create-prepare-1-to-2")
        .unwrap();
    harness
        .deliver_protocol_message("read-create-prepare-1-to-2-ack")
        .unwrap();
    harness.deliver_protocol_message("commit-1-to-2").unwrap();

    let primary_read = harness
        .read_resource(replica(1), ResourceId(21), Some(entry.lsn))
        .unwrap()
        .expect("primary should serve committed resource");
    assert_eq!(primary_read.resource_id, ResourceId(21));

    let backup_read = harness.read_resource(replica(2), ResourceId(21), Some(entry.lsn));
    assert!(matches!(
        backup_read,
        Err(ReplicatedSimulationError::Read(NotPrimaryReadError::Role(
            ReplicaRole::Backup
        )))
    ));
}

#[test]
fn quorum_lost_primary_fails_closed_for_reads_and_writes() {
    let mut harness = primary_harness("replicated-quorum-loss", 0x5a3);

    let entry = harness
        .client_submit(replica(1), Slot(1), &create_payload(31, 3), "quorum-loss")
        .unwrap();
    harness
        .deliver_protocol_message("quorum-loss-prepare-1-to-2")
        .unwrap();
    harness
        .deliver_protocol_message("quorum-loss-prepare-1-to-2-ack")
        .unwrap();

    let before_loss = harness
        .read_resource(replica(1), ResourceId(31), Some(entry.lsn))
        .unwrap()
        .expect("primary should serve reads before quorum loss");
    assert_eq!(before_loss.resource_id, ResourceId(31));

    set_replica_link(&mut harness, 1, 2, false);
    set_replica_link(&mut harness, 1, 3, false);

    assert_eq!(harness.configured_primary(), None);
    assert_eq!(
        harness
            .replica(replica(1))
            .unwrap()
            .unwrap()
            .metadata()
            .role,
        ReplicaRole::ViewUncertain
    );

    let stale_read = harness.read_resource(replica(1), ResourceId(31), Some(entry.lsn));
    assert!(matches!(
        stale_read,
        Err(ReplicatedSimulationError::Read(NotPrimaryReadError::Role(
            ReplicaRole::ViewUncertain
        )))
    ));

    let stale_write = harness.client_submit(
        replica(1),
        Slot(2),
        &create_payload(32, 4),
        "quorum-loss-retry",
    );
    assert!(matches!(
        stale_write,
        Err(ReplicatedSimulationError::NotConfiguredPrimary {
            expected: None,
            found
        }) if found == replica(1)
    ));
}

#[test]
fn higher_view_takeover_reconstructs_prefix_and_rejects_stale_primary_reads() {
    let mut harness = primary_harness("replicated-view-change", 0x5a4);

    let entry = harness
        .client_submit(replica(1), Slot(1), &create_payload(41, 5), "view-change")
        .unwrap();
    harness
        .deliver_protocol_message("view-change-prepare-1-to-2")
        .unwrap();
    harness
        .deliver_protocol_message("view-change-prepare-1-to-3")
        .unwrap();
    harness
        .deliver_protocol_message("view-change-prepare-1-to-3-ack")
        .unwrap();
    harness.deliver_protocol_message("commit-1-to-3").unwrap();

    assert_eq!(replica_last_applied_lsn(&harness, 1), Some(entry.lsn));
    assert_eq!(replica_last_applied_lsn(&harness, 2), None);
    assert_eq!(replica_prepared_len(&harness, 2), 1);
    assert_eq!(replica_last_applied_lsn(&harness, 3), Some(entry.lsn));

    set_replica_link(&mut harness, 1, 2, false);
    set_replica_link(&mut harness, 1, 3, false);
    assert_eq!(harness.configured_primary(), None);

    harness.complete_view_change(replica(2), 2).unwrap();

    assert_eq!(harness.configured_primary(), Some(replica(2)));
    assert_eq!(
        harness
            .replica(replica(2))
            .unwrap()
            .unwrap()
            .metadata()
            .role,
        ReplicaRole::Primary
    );
    assert_eq!(
        harness
            .replica(replica(2))
            .unwrap()
            .unwrap()
            .metadata()
            .current_view,
        2
    );
    assert_eq!(replica_last_applied_lsn(&harness, 2), Some(entry.lsn));
    assert_eq!(replica_prepared_len(&harness, 2), 0);
    assert_eq!(
        harness
            .replica(replica(3))
            .unwrap()
            .unwrap()
            .metadata()
            .role,
        ReplicaRole::Backup
    );
    assert_eq!(
        harness
            .replica(replica(3))
            .unwrap()
            .unwrap()
            .metadata()
            .current_view,
        2
    );
    assert!(pending_labels(&harness).is_empty());

    let new_primary_read = harness
        .read_resource(replica(2), ResourceId(41), Some(entry.lsn))
        .unwrap()
        .expect("new primary should serve the committed prefix after failover");
    assert_eq!(new_primary_read.resource_id, ResourceId(41));

    let stale_primary_read = harness.read_resource(replica(1), ResourceId(41), Some(entry.lsn));
    assert!(matches!(
        stale_primary_read,
        Err(ReplicatedSimulationError::Read(NotPrimaryReadError::Role(
            ReplicaRole::ViewUncertain
        )))
    ));

    let stale_primary_write =
        harness.client_submit(replica(1), Slot(2), &create_payload(42, 6), "stale-primary");
    assert!(matches!(
        stale_primary_write,
        Err(ReplicatedSimulationError::NotConfiguredPrimary {
            expected: Some(expected),
            found
        }) if expected == replica(2) && found == replica(1)
    ));
}

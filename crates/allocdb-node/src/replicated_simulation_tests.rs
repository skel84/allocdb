use std::fs;

use allocdb_core::command::{ClientRequest, Command};
use allocdb_core::command_codec::encode_client_request;
use allocdb_core::config::Config;
use allocdb_core::ids::{ClientId, HolderId, Lsn, OperationId, ReservationId, ResourceId, Slot};
use allocdb_core::result::ResultCode;

use crate::engine::EngineConfig;
use crate::replica::{NotPrimaryReadError, ReplicaId, ReplicaNodeStatus, ReplicaRole};
use crate::replicated_simulation::{
    ClusterEndpoint, QueuedProtocolMessage, ReplicaObservation, ReplicaRejoinMethod,
    ReplicaRuntimeStatus, ReplicatedClientRequestOutcome, ReplicatedScheduleAction,
    ReplicatedScheduleActionKind, ReplicatedScheduleObservation, ReplicatedScheduleObservationKind,
    ReplicatedSimulationError, ReplicatedSimulationHarness,
};

fn core_config() -> Config {
    Config {
        shard_id: 0,
        max_resources: 8,
        max_reservations: 8,
        max_bundle_size: 1,
        max_operations: 16,
        max_ttl_slots: 16,
        max_client_retry_window_slots: 8,
        reservation_history_window_slots: 4,
        max_expiration_bucket_len: 8,
    }
}

fn bundle_core_config() -> Config {
    Config {
        max_bundle_size: 4,
        ..core_config()
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

fn reserve_payload(resource_id: u128, holder_id: u128, operation_id: u128) -> Vec<u8> {
    encode_client_request(ClientRequest {
        operation_id: OperationId(operation_id),
        client_id: ClientId(7),
        command: Command::Reserve {
            resource_id: ResourceId(resource_id),
            holder_id: HolderId(holder_id),
            ttl_slots: 4,
        },
    })
}

fn reserve_bundle_payload(resource_ids: &[u128], holder_id: u128, operation_id: u128) -> Vec<u8> {
    encode_client_request(ClientRequest {
        operation_id: OperationId(operation_id),
        client_id: ClientId(7),
        command: Command::ReserveBundle {
            resource_ids: resource_ids.iter().copied().map(ResourceId).collect(),
            holder_id: HolderId(holder_id),
            ttl_slots: 4,
        },
    })
}

fn confirm_payload(
    reservation_id: u128,
    holder_id: u128,
    lease_epoch: u64,
    operation_id: u128,
) -> Vec<u8> {
    encode_client_request(ClientRequest {
        operation_id: OperationId(operation_id),
        client_id: ClientId(7),
        command: Command::Confirm {
            reservation_id: ReservationId(reservation_id),
            holder_id: HolderId(holder_id),
            lease_epoch,
        },
    })
}

fn release_payload(
    reservation_id: u128,
    holder_id: u128,
    lease_epoch: u64,
    operation_id: u128,
) -> Vec<u8> {
    encode_client_request(ClientRequest {
        operation_id: OperationId(operation_id),
        client_id: ClientId(7),
        command: Command::Release {
            reservation_id: ReservationId(reservation_id),
            holder_id: HolderId(holder_id),
            lease_epoch,
        },
    })
}

fn revoke_payload(reservation_id: u128, operation_id: u128) -> Vec<u8> {
    encode_client_request(ClientRequest {
        operation_id: OperationId(operation_id),
        client_id: ClientId(7),
        command: Command::Revoke {
            reservation_id: ReservationId(reservation_id),
        },
    })
}

fn reclaim_payload(reservation_id: u128, operation_id: u128) -> Vec<u8> {
    encode_client_request(ClientRequest {
        operation_id: OperationId(operation_id),
        client_id: ClientId(7),
        command: Command::Reclaim {
            reservation_id: ReservationId(reservation_id),
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

fn bundle_primary_harness(name: &str, seed: u64) -> ReplicatedSimulationHarness {
    let mut harness =
        ReplicatedSimulationHarness::new(name, seed, bundle_core_config(), engine_config())
            .unwrap();
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

fn replica_commit_lsn(harness: &ReplicatedSimulationHarness, replica_id: u64) -> Option<Lsn> {
    harness
        .replica(replica(replica_id))
        .unwrap()
        .unwrap()
        .metadata()
        .commit_lsn
}

fn replica_snapshot_lsn(harness: &ReplicatedSimulationHarness, replica_id: u64) -> Option<Lsn> {
    harness
        .replica(replica(replica_id))
        .unwrap()
        .unwrap()
        .metadata()
        .active_snapshot_lsn
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

fn replica_reservation_member_ids(
    harness: &ReplicatedSimulationHarness,
    replica_id: u64,
    reservation_id: u128,
    current_slot: u64,
) -> Vec<ResourceId> {
    let node = harness.replica(replica(replica_id)).unwrap().unwrap();
    let reservation = node
        .engine()
        .unwrap()
        .db()
        .reservation(ReservationId(reservation_id), Slot(current_slot))
        .unwrap();
    node.engine()
        .unwrap()
        .db()
        .reservation_member_resource_ids(reservation)
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

fn commit_to_backup(
    harness: &mut ReplicatedSimulationHarness,
    label_prefix: &str,
    lsn: Lsn,
    backup: u64,
) {
    harness
        .deliver_protocol_message(&format!("{label_prefix}-prepare-{}-to-{backup}", lsn.get()))
        .unwrap();
    harness
        .deliver_protocol_message(&format!(
            "{label_prefix}-prepare-{}-to-{backup}-ack",
            lsn.get()
        ))
        .unwrap();
    harness
        .deliver_protocol_message(&format!("commit-{}-to-{backup}", lsn.get()))
        .unwrap();
}

fn commit_to_all_backups(harness: &mut ReplicatedSimulationHarness, label_prefix: &str, lsn: Lsn) {
    commit_to_backup(harness, label_prefix, lsn, 2);
    harness
        .deliver_protocol_message(&format!("{label_prefix}-prepare-{}-to-3", lsn.get()))
        .unwrap();
    harness
        .deliver_protocol_message(&format!("commit-{}-to-3", lsn.get()))
        .unwrap();
}

fn deliver_prepare_without_ack(
    harness: &mut ReplicatedSimulationHarness,
    label_prefix: &str,
    lsn: Lsn,
    backup: u64,
) {
    harness
        .deliver_protocol_message(&format!("{label_prefix}-prepare-{}-to-{backup}", lsn.get()))
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
fn retry_cache_hits_do_not_overwrite_canonical_published_results() {
    let mut harness = primary_harness("replicated-retry-cache-published-result", 0x5a21);

    let payload = create_payload(211, 201);
    let entry = harness
        .client_submit(replica(1), Slot(1), &payload, "canonical")
        .unwrap();
    commit_to_backup(&mut harness, "canonical", entry.lsn, 2);

    let original = *harness
        .published_result(entry.lsn)
        .expect("majority commit should publish one canonical result");
    assert!(!original.from_retry_cache);

    let retry = harness
        .client_submit_or_retry(replica(1), Slot(2), &payload, "canonical-retry")
        .unwrap();
    match retry {
        ReplicatedClientRequestOutcome::Published(result) => {
            assert_eq!(result.applied_lsn, original.applied_lsn);
            assert_eq!(result.outcome, original.outcome);
            assert!(result.from_retry_cache);
        }
        other @ ReplicatedClientRequestOutcome::Prepared(_) => {
            panic!("expected retry cache hit on the current primary, got {other:?}")
        }
    }
    assert_eq!(harness.published_result(entry.lsn), Some(&original));

    let conflicting = harness
        .client_submit_or_retry(
            replica(1),
            Slot(3),
            &create_payload(212, 201),
            "canonical-conflict",
        )
        .unwrap();
    match conflicting {
        ReplicatedClientRequestOutcome::Published(result) => {
            assert_eq!(result.applied_lsn, original.applied_lsn);
            assert_eq!(result.outcome.result_code, ResultCode::OperationConflict);
            assert!(result.from_retry_cache);
        }
        other @ ReplicatedClientRequestOutcome::Prepared(_) => {
            panic!("expected conflicting retry to resolve from cache, got {other:?}")
        }
    }
    assert_eq!(harness.published_result(entry.lsn), Some(&original));
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

#[test]
fn higher_view_takeover_does_not_promote_primary_only_prepared_suffix() {
    let mut harness = primary_harness("replicated-view-change-primary-only-prepared", 0x5a3f);
    let payload = create_payload(400, 59);

    let entry = harness
        .client_submit(replica(1), Slot(1), &payload, "primary-only")
        .unwrap();

    assert_eq!(harness.configured_primary(), Some(replica(1)));
    assert_eq!(replica_commit_lsn(&harness, 1), None);
    assert_eq!(replica_commit_lsn(&harness, 2), None);
    assert_eq!(replica_commit_lsn(&harness, 3), None);
    assert!(
        harness
            .replica(replica(1))
            .unwrap()
            .unwrap()
            .prepared_entry(entry.lsn)
            .is_some()
    );
    assert!(
        harness
            .replica(replica(2))
            .unwrap()
            .unwrap()
            .prepared_entry(entry.lsn)
            .is_none()
    );
    assert!(
        harness
            .replica(replica(3))
            .unwrap()
            .unwrap()
            .prepared_entry(entry.lsn)
            .is_none()
    );

    harness.complete_view_change(replica(2), 2).unwrap();

    assert_eq!(harness.configured_primary(), Some(replica(2)));
    assert_eq!(replica_commit_lsn(&harness, 2), None);
    assert_eq!(replica_last_applied_lsn(&harness, 2), None);
    assert_eq!(replica_prepared_len(&harness, 1), 0);
    assert_eq!(replica_prepared_len(&harness, 2), 0);
    assert_eq!(replica_prepared_len(&harness, 3), 0);
    assert!(!replica_has_resource(&harness, 1, 400));
    assert!(!replica_has_resource(&harness, 2, 400));
    assert!(!replica_has_resource(&harness, 3, 400));
    assert!(pending_labels(&harness).is_empty());

    let retry = harness
        .client_submit_or_retry(replica(2), Slot(2), &payload, "primary-only-retry")
        .unwrap();
    let retry_entry = match retry {
        ReplicatedClientRequestOutcome::Prepared(entry) => entry,
        other @ ReplicatedClientRequestOutcome::Published(_) => {
            panic!("expected a fresh prepare after view change, got {other:?}")
        }
    };
    assert_eq!(retry_entry.view, 2);
    assert_eq!(retry_entry.lsn, Lsn(1));
}

#[test]
fn higher_view_takeover_recovers_missing_prepared_suffix_from_another_voter() {
    let mut harness = primary_harness("replicated-view-change-copy-prepared", 0x5a40);

    let entry = harness
        .client_submit(
            replica(1),
            Slot(1),
            &create_payload(401, 60),
            "copy-prepared",
        )
        .unwrap();
    harness
        .deliver_protocol_message("copy-prepared-prepare-1-to-3")
        .unwrap();
    harness
        .deliver_protocol_message("copy-prepared-prepare-1-to-3-ack")
        .unwrap();

    assert_eq!(replica_commit_lsn(&harness, 1), Some(entry.lsn));
    assert_eq!(replica_last_applied_lsn(&harness, 1), Some(entry.lsn));
    assert_eq!(replica_commit_lsn(&harness, 2), None);
    assert_eq!(replica_last_applied_lsn(&harness, 2), None);
    assert_eq!(replica_commit_lsn(&harness, 3), None);
    assert_eq!(replica_last_applied_lsn(&harness, 3), None);
    assert!(
        harness
            .replica(replica(2))
            .unwrap()
            .unwrap()
            .prepared_entry(entry.lsn)
            .is_none()
    );
    assert!(
        harness
            .replica(replica(3))
            .unwrap()
            .unwrap()
            .prepared_entry(entry.lsn)
            .is_some()
    );

    set_replica_link(&mut harness, 1, 2, false);
    set_replica_link(&mut harness, 1, 3, false);
    assert_eq!(harness.configured_primary(), None);

    harness.complete_view_change(replica(2), 2).unwrap();

    assert_eq!(harness.configured_primary(), Some(replica(2)));
    assert_eq!(replica_commit_lsn(&harness, 2), Some(entry.lsn));
    assert_eq!(replica_last_applied_lsn(&harness, 2), Some(entry.lsn));
    assert_eq!(replica_prepared_len(&harness, 2), 0);
    assert!(replica_has_resource(&harness, 2, 401));

    let read = harness
        .read_resource(replica(2), ResourceId(401), Some(entry.lsn))
        .unwrap()
        .expect("new primary should serve the reconstructed committed entry");
    assert_eq!(read.resource_id, ResourceId(401));
}

#[test]
fn minority_partition_keeps_quorum_and_healed_backup_catches_up() {
    let mut harness = primary_harness("replicated-minority-partition", 0x5a41);

    set_replica_link(&mut harness, 1, 3, false);
    set_replica_link(&mut harness, 2, 3, false);

    let entry = harness
        .client_submit(replica(1), Slot(1), &create_payload(43, 61), "minority")
        .unwrap();
    commit_to_backup(&mut harness, "minority", entry.lsn, 2);

    assert_eq!(harness.configured_primary(), Some(replica(1)));
    assert_eq!(replica_last_applied_lsn(&harness, 1), Some(entry.lsn));
    assert_eq!(replica_last_applied_lsn(&harness, 2), Some(entry.lsn));
    assert_eq!(replica_last_applied_lsn(&harness, 3), None);
    assert!(pending_labels(&harness).contains(&"minority-prepare-1-to-3"));
    assert!(pending_labels(&harness).contains(&"commit-1-to-3"));

    let primary_read = harness
        .read_resource(replica(1), ResourceId(43), Some(entry.lsn))
        .unwrap()
        .expect("primary should keep serving while quorum remains");
    assert_eq!(primary_read.resource_id, ResourceId(43));

    set_replica_link(&mut harness, 1, 3, true);
    set_replica_link(&mut harness, 2, 3, true);
    harness
        .deliver_protocol_message("minority-prepare-1-to-3")
        .unwrap();
    harness.deliver_protocol_message("commit-1-to-3").unwrap();

    assert_eq!(replica_last_applied_lsn(&harness, 3), Some(entry.lsn));
    assert_eq!(replica_prepared_len(&harness, 3), 0);
    assert!(replica_has_resource(&harness, 3, 43));
}

#[test]
fn split_cluster_into_non_quorum_minorities_fails_closed_until_heal_and_rejoin() {
    let mut harness = primary_harness("replicated-full-split", 0x5a42);

    let baseline = harness
        .client_submit(replica(1), Slot(1), &create_payload(44, 62), "baseline")
        .unwrap();
    commit_to_backup(&mut harness, "baseline", baseline.lsn, 2);
    harness
        .deliver_protocol_message("baseline-prepare-1-to-3")
        .unwrap();
    harness.deliver_protocol_message("commit-1-to-3").unwrap();

    set_replica_link(&mut harness, 1, 2, false);
    set_replica_link(&mut harness, 1, 3, false);
    set_replica_link(&mut harness, 2, 3, false);

    assert_eq!(harness.configured_primary(), None);
    let stale_read = harness.read_resource(replica(1), ResourceId(44), Some(baseline.lsn));
    assert!(matches!(
        stale_read,
        Err(ReplicatedSimulationError::Read(NotPrimaryReadError::Role(
            ReplicaRole::ViewUncertain
        )))
    ));

    let no_quorum = harness.complete_view_change(replica(2), 2).unwrap_err();
    assert!(matches!(
        no_quorum,
        ReplicatedSimulationError::ViewChangeQuorumUnavailable {
            candidate,
            reachable: 1,
        } if candidate == replica(2)
    ));

    set_replica_link(&mut harness, 2, 3, true);
    harness.complete_view_change(replica(2), 2).unwrap();

    let healed = harness
        .client_submit(replica(2), Slot(2), &create_payload(45, 63), "healed")
        .unwrap();
    commit_to_backup(&mut harness, "healed", healed.lsn, 3);

    set_replica_link(&mut harness, 1, 2, true);
    set_replica_link(&mut harness, 1, 3, true);
    let method = harness.rejoin_replica(replica(1), replica(2)).unwrap();
    assert_eq!(method, ReplicaRejoinMethod::SuffixOnly);

    assert_eq!(replica_commit_lsn(&harness, 1), Some(healed.lsn));
    assert_eq!(replica_last_applied_lsn(&harness, 1), Some(healed.lsn));
    assert_eq!(replica_last_applied_lsn(&harness, 2), Some(healed.lsn));
    assert_eq!(replica_last_applied_lsn(&harness, 3), Some(healed.lsn));
    assert!(replica_has_resource(&harness, 1, 44));
    assert!(replica_has_resource(&harness, 1, 45));
}

#[test]
fn primary_crash_before_quorum_append_retries_once_after_failover() {
    let mut harness = primary_harness("replicated-crash-before-quorum", 0x5a43);
    let payload = create_payload(46, 64);

    let entry = harness
        .client_submit(replica(1), Slot(1), &payload, "before-quorum")
        .unwrap();
    assert_eq!(entry.lsn, Lsn(1));

    harness.crash_replica(replica(1)).unwrap();
    harness.complete_view_change(replica(2), 2).unwrap();

    let retry = harness
        .client_submit_or_retry(replica(2), Slot(2), &payload, "before-quorum-retry")
        .unwrap();
    let retry_entry = match retry {
        ReplicatedClientRequestOutcome::Prepared(entry) => entry,
        other @ ReplicatedClientRequestOutcome::Published(_) => {
            panic!("expected one new prepared retry after no-quorum crash, got {other:?}")
        }
    };
    assert_eq!(retry_entry.view, 2);
    assert_eq!(retry_entry.lsn, Lsn(1));

    commit_to_backup(&mut harness, "before-quorum-retry", retry_entry.lsn, 3);
    let published = *harness
        .published_result(retry_entry.lsn)
        .expect("retry should publish after the new quorum commits");
    assert!(!published.from_retry_cache);

    harness.restart_replica(replica(1)).unwrap();
    let method = harness.rejoin_replica(replica(1), replica(2)).unwrap();
    assert_eq!(method, ReplicaRejoinMethod::SuffixOnly);
    assert!(replica_has_resource(&harness, 1, 46));
    assert!(replica_has_resource(&harness, 2, 46));
    assert!(replica_has_resource(&harness, 3, 46));
}

#[test]
fn primary_crash_after_quorum_append_retries_from_reconstructed_commit() {
    let mut harness = primary_harness("replicated-crash-after-quorum", 0x5a44);
    let payload = create_payload(47, 65);

    let entry = harness
        .client_submit(replica(1), Slot(1), &payload, "after-quorum")
        .unwrap();
    harness
        .deliver_protocol_message("after-quorum-prepare-1-to-2")
        .unwrap();
    harness
        .deliver_protocol_message("after-quorum-prepare-1-to-2-ack")
        .unwrap();
    assert_eq!(replica_commit_lsn(&harness, 1), Some(entry.lsn));
    assert_eq!(replica_last_applied_lsn(&harness, 1), Some(entry.lsn));
    assert_eq!(replica_commit_lsn(&harness, 2), None);
    assert_eq!(replica_prepared_len(&harness, 2), 1);

    harness.crash_replica(replica(1)).unwrap();
    harness.complete_view_change(replica(2), 2).unwrap();

    let retry = harness
        .client_submit_or_retry(replica(2), Slot(2), &payload, "after-quorum-retry")
        .unwrap();
    match retry {
        ReplicatedClientRequestOutcome::Published(result) => {
            assert_eq!(result.applied_lsn, entry.lsn);
            assert_eq!(result.outcome.result_code, ResultCode::Ok);
            assert_eq!(result.outcome.reservation_id, None);
            assert_eq!(result.outcome.deadline_slot, None);
            assert!(result.from_retry_cache);
        }
        other @ ReplicatedClientRequestOutcome::Prepared(_) => {
            panic!("expected retry cache hit after majority-committed crash, got {other:?}")
        }
    }

    let method = harness.rejoin_replica(replica(3), replica(2)).unwrap();
    assert_eq!(method, ReplicaRejoinMethod::SuffixOnly);
    assert_eq!(replica_last_applied_lsn(&harness, 2), Some(entry.lsn));
    assert_eq!(replica_last_applied_lsn(&harness, 3), Some(entry.lsn));
    assert!(replica_has_resource(&harness, 2, 47));
    assert!(replica_has_resource(&harness, 3, 47));
}

#[test]
fn primary_crash_after_reply_preserves_read_and_retry_on_new_primary() {
    let mut harness = primary_harness("replicated-crash-after-reply", 0x5a45);
    let payload = create_payload(48, 66);

    let entry = harness
        .client_submit(replica(1), Slot(1), &payload, "after-reply")
        .unwrap();
    harness
        .deliver_protocol_message("after-reply-prepare-1-to-2")
        .unwrap();
    harness
        .deliver_protocol_message("after-reply-prepare-1-to-2-ack")
        .unwrap();
    harness.deliver_protocol_message("commit-1-to-2").unwrap();
    let committed = *harness
        .published_result(entry.lsn)
        .expect("client reply should already be published before the crash");

    harness.crash_replica(replica(1)).unwrap();
    harness.complete_view_change(replica(2), 2).unwrap();

    let primary_read = harness
        .read_resource(replica(2), ResourceId(48), Some(entry.lsn))
        .unwrap()
        .expect("new primary should serve the committed result after failover");
    assert_eq!(primary_read.resource_id, ResourceId(48));

    let retry = harness
        .client_submit_or_retry(replica(2), Slot(2), &payload, "after-reply-retry")
        .unwrap();
    match retry {
        ReplicatedClientRequestOutcome::Published(result) => {
            assert_eq!(result.applied_lsn, committed.applied_lsn);
            assert_eq!(result.outcome, committed.outcome);
            assert!(result.from_retry_cache);
        }
        other @ ReplicatedClientRequestOutcome::Prepared(_) => {
            panic!("expected retry cache hit after replied commit, got {other:?}")
        }
    }

    let method = harness.rejoin_replica(replica(3), replica(2)).unwrap();
    assert_eq!(method, ReplicaRejoinMethod::SuffixOnly);
    assert_eq!(replica_last_applied_lsn(&harness, 3), Some(entry.lsn));
    assert!(replica_has_resource(&harness, 3, 48));
}

#[test]
fn primary_crash_after_quorum_bundle_retry_hits_reconstructed_retry_cache() {
    let mut harness = bundle_primary_harness("replicated-bundle-after-quorum", 0x5a45_0001);

    let create_first = harness
        .client_submit(
            replica(1),
            Slot(1),
            &create_payload(91, 901),
            "bundle-after-quorum-create-first",
        )
        .unwrap();
    commit_to_all_backups(
        &mut harness,
        "bundle-after-quorum-create-first",
        create_first.lsn,
    );

    let create_second = harness
        .client_submit(
            replica(1),
            Slot(2),
            &create_payload(92, 902),
            "bundle-after-quorum-create-second",
        )
        .unwrap();
    commit_to_all_backups(
        &mut harness,
        "bundle-after-quorum-create-second",
        create_second.lsn,
    );

    let payload = reserve_bundle_payload(&[91, 92], 31, 903);
    let entry = harness
        .client_submit(replica(1), Slot(3), &payload, "bundle-after-quorum")
        .unwrap();
    harness
        .deliver_protocol_message("bundle-after-quorum-prepare-3-to-2")
        .unwrap();
    harness
        .deliver_protocol_message("bundle-after-quorum-prepare-3-to-2-ack")
        .unwrap();
    assert_eq!(replica_commit_lsn(&harness, 1), Some(entry.lsn));
    assert_eq!(replica_last_applied_lsn(&harness, 1), Some(entry.lsn));
    assert_eq!(replica_commit_lsn(&harness, 2), Some(create_second.lsn));
    assert_eq!(replica_prepared_len(&harness, 2), 1);

    harness.crash_replica(replica(1)).unwrap();
    harness.complete_view_change(replica(2), 2).unwrap();

    let retry = harness
        .client_submit_or_retry(replica(2), Slot(4), &payload, "bundle-after-quorum-retry")
        .unwrap();
    match retry {
        ReplicatedClientRequestOutcome::Published(result) => {
            assert_eq!(result.applied_lsn, entry.lsn);
            assert_eq!(result.outcome.result_code, ResultCode::Ok);
            assert_eq!(result.outcome.reservation_id, Some(ReservationId(3)));
            assert_eq!(result.outcome.deadline_slot, Some(Slot(7)));
            assert!(result.from_retry_cache);
        }
        other @ ReplicatedClientRequestOutcome::Prepared(_) => {
            panic!("expected bundle retry cache hit after majority-committed crash, got {other:?}")
        }
    }

    let first_member = harness
        .read_resource(replica(2), ResourceId(91), Some(entry.lsn))
        .unwrap()
        .expect("new primary should preserve first committed bundle member");
    assert_eq!(
        first_member.current_state,
        allocdb_core::ResourceState::Reserved
    );
    assert_eq!(first_member.current_reservation_id, Some(ReservationId(3)));

    let second_member = harness
        .read_resource(replica(2), ResourceId(92), Some(entry.lsn))
        .unwrap()
        .expect("new primary should preserve second committed bundle member");
    assert_eq!(
        second_member.current_state,
        allocdb_core::ResourceState::Reserved
    );
    assert_eq!(second_member.current_reservation_id, Some(ReservationId(3)));
    assert_eq!(
        replica_reservation_member_ids(&harness, 2, 3, 4),
        vec![ResourceId(91), ResourceId(92)]
    );

    let method = harness.rejoin_replica(replica(3), replica(2)).unwrap();
    assert_eq!(method, ReplicaRejoinMethod::SuffixOnly);
    assert_eq!(replica_last_applied_lsn(&harness, 2), Some(entry.lsn));
    assert_eq!(replica_last_applied_lsn(&harness, 3), Some(entry.lsn));
    assert_eq!(
        replica_reservation_member_ids(&harness, 3, 3, 4),
        vec![ResourceId(91), ResourceId(92)]
    );
}

#[test]
fn committed_bundle_membership_survives_failover_and_suffix_rejoin() {
    let mut harness = bundle_primary_harness("replicated-bundle-failover", 0x5a46_0001);

    let create_first = harness
        .client_submit(
            replica(1),
            Slot(1),
            &create_payload(81, 801),
            "bundle-create-first",
        )
        .unwrap();
    commit_to_all_backups(&mut harness, "bundle-create-first", create_first.lsn);

    let create_second = harness
        .client_submit(
            replica(1),
            Slot(2),
            &create_payload(82, 802),
            "bundle-create-second",
        )
        .unwrap();
    commit_to_all_backups(&mut harness, "bundle-create-second", create_second.lsn);

    let bundle = harness
        .client_submit(
            replica(1),
            Slot(3),
            &reserve_bundle_payload(&[81, 82], 91, 803),
            "bundle-reserve",
        )
        .unwrap();
    commit_to_backup(&mut harness, "bundle-reserve", bundle.lsn, 2);

    harness.crash_replica(replica(1)).unwrap();
    harness.complete_view_change(replica(2), 2).unwrap();

    let first_member = harness
        .read_resource(replica(2), ResourceId(81), Some(bundle.lsn))
        .unwrap()
        .expect("new primary should preserve first bundle member");
    assert_eq!(
        first_member.current_state,
        allocdb_core::ResourceState::Reserved
    );
    assert_eq!(first_member.current_reservation_id, Some(ReservationId(3)));

    let second_member = harness
        .read_resource(replica(2), ResourceId(82), Some(bundle.lsn))
        .unwrap()
        .expect("new primary should preserve second bundle member");
    assert_eq!(
        second_member.current_state,
        allocdb_core::ResourceState::Reserved
    );
    assert_eq!(second_member.current_reservation_id, Some(ReservationId(3)));

    assert_eq!(
        replica_reservation_member_ids(&harness, 2, 3, 3),
        vec![ResourceId(81), ResourceId(82)]
    );

    let method = harness.rejoin_replica(replica(3), replica(2)).unwrap();
    assert_eq!(method, ReplicaRejoinMethod::SuffixOnly);
    assert_eq!(
        replica_reservation_member_ids(&harness, 3, 3, 3),
        vec![ResourceId(81), ResourceId(82)]
    );

    let busy_member = harness
        .client_submit(
            replica(2),
            Slot(4),
            &reserve_payload(82, 92, 804),
            "bundle-member-busy",
        )
        .unwrap();
    commit_to_backup(&mut harness, "bundle-member-busy", busy_member.lsn, 3);
    let busy_result = harness
        .published_result(busy_member.lsn)
        .expect("conflicting member reserve should publish deterministically");
    assert_eq!(busy_result.outcome.result_code, ResultCode::ResourceBusy);
}

#[test]
#[allow(clippy::too_many_lines)]
fn committed_revoke_stays_non_reusable_across_failover_until_reclaim() {
    let mut harness = primary_harness("replicated-revoke-failover", 0x5a46);

    let create = harness
        .client_submit(
            replica(1),
            Slot(1),
            &create_payload(71, 701),
            "revoke-create",
        )
        .unwrap();
    commit_to_all_backups(&mut harness, "revoke-create", create.lsn);

    let reserve = harness
        .client_submit(
            replica(1),
            Slot(2),
            &reserve_payload(71, 91, 702),
            "revoke-reserve",
        )
        .unwrap();
    commit_to_all_backups(&mut harness, "revoke-reserve", reserve.lsn);

    let confirm = harness
        .client_submit(
            replica(1),
            Slot(2),
            &confirm_payload(2, 91, 1, 703),
            "revoke-confirm",
        )
        .unwrap();
    commit_to_all_backups(&mut harness, "revoke-confirm", confirm.lsn);

    let revoke = harness
        .client_submit(
            replica(1),
            Slot(2),
            &revoke_payload(2, 704),
            "revoke-command",
        )
        .unwrap();
    harness
        .deliver_protocol_message("revoke-command-prepare-4-to-2")
        .unwrap();
    harness
        .deliver_protocol_message("revoke-command-prepare-4-to-2-ack")
        .unwrap();
    deliver_prepare_without_ack(&mut harness, "revoke-command", revoke.lsn, 3);

    harness.crash_replica(replica(1)).unwrap();
    harness.complete_view_change(replica(2), 2).unwrap();
    let method = harness.rejoin_replica(replica(3), replica(2)).unwrap();
    assert_eq!(method, ReplicaRejoinMethod::SuffixOnly);

    let revoking_read = harness
        .read_resource(replica(2), ResourceId(71), Some(revoke.lsn))
        .unwrap()
        .expect("new primary should preserve committed revoke across failover");
    assert_eq!(revoking_read.current_reservation_id, Some(ReservationId(2)));
    assert_eq!(
        revoking_read.current_state,
        allocdb_core::ResourceState::Revoking
    );

    let stale_release = harness
        .client_submit(
            replica(2),
            Slot(3),
            &release_payload(2, 91, 1, 705),
            "revoke-stale-release",
        )
        .unwrap();
    commit_to_backup(&mut harness, "revoke-stale-release", stale_release.lsn, 3);
    let stale_release_result = harness
        .published_result(stale_release.lsn)
        .expect("stale holder release should publish deterministically");
    assert_eq!(
        stale_release_result.outcome.result_code,
        ResultCode::StaleEpoch
    );

    let early_reuse = harness
        .client_submit(
            replica(2),
            Slot(3),
            &reserve_payload(71, 99, 706),
            "revoke-early-reuse",
        )
        .unwrap();
    commit_to_backup(&mut harness, "revoke-early-reuse", early_reuse.lsn, 3);
    let early_reuse_result = harness
        .published_result(early_reuse.lsn)
        .expect("busy reserve should still publish deterministically");
    assert_eq!(
        early_reuse_result.outcome.result_code,
        ResultCode::ResourceBusy
    );

    let reclaim = harness
        .client_submit(
            replica(2),
            Slot(4),
            &reclaim_payload(2, 707),
            "revoke-reclaim",
        )
        .unwrap();
    commit_to_backup(&mut harness, "revoke-reclaim", reclaim.lsn, 3);

    let available_read = harness
        .read_resource(replica(2), ResourceId(71), Some(reclaim.lsn))
        .unwrap()
        .expect("resource should become available only after reclaim commits");
    assert_eq!(
        available_read.current_state,
        allocdb_core::ResourceState::Available
    );
    assert_eq!(available_read.current_reservation_id, None);
}

#[test]
fn stale_replica_rejoins_by_suffix_and_drops_uncommitted_suffix() {
    let mut harness = primary_harness("replicated-rejoin-suffix", 0x5a5);

    let first = harness
        .client_submit(replica(1), Slot(1), &create_payload(51, 7), "first")
        .unwrap();
    commit_to_backup(&mut harness, "first", first.lsn, 2);
    harness
        .deliver_protocol_message("first-prepare-1-to-3")
        .unwrap();
    harness.deliver_protocol_message("commit-1-to-3").unwrap();
    harness.checkpoint_replica(replica(1)).unwrap();

    let second = harness
        .client_submit(replica(1), Slot(2), &create_payload(52, 8), "second")
        .unwrap();
    commit_to_backup(&mut harness, "second", second.lsn, 2);
    harness
        .deliver_protocol_message("second-prepare-2-to-3")
        .unwrap();
    harness.deliver_protocol_message("commit-2-to-3").unwrap();
    harness.checkpoint_replica(replica(1)).unwrap();

    let third = harness
        .client_submit(replica(1), Slot(3), &create_payload(53, 9), "third")
        .unwrap();
    commit_to_backup(&mut harness, "third", third.lsn, 2);
    harness
        .deliver_protocol_message("third-prepare-3-to-3")
        .unwrap();
    harness.checkpoint_replica(replica(1)).unwrap();

    let fourth = harness
        .client_submit(replica(1), Slot(4), &create_payload(54, 10), "fourth")
        .unwrap();
    deliver_prepare_without_ack(&mut harness, "fourth", fourth.lsn, 3);

    assert_eq!(replica_commit_lsn(&harness, 3), Some(Lsn(2)));
    assert_eq!(replica_prepared_len(&harness, 3), 2);
    assert!(pending_labels(&harness).contains(&"commit-3-to-3"));
    assert!(pending_labels(&harness).contains(&"fourth-prepare-4-to-3-ack"));

    let method = harness.rejoin_replica(replica(3), replica(1)).unwrap();
    assert_eq!(method, ReplicaRejoinMethod::SuffixOnly);
    assert_eq!(replica_commit_lsn(&harness, 3), Some(third.lsn));
    assert_eq!(replica_last_applied_lsn(&harness, 3), Some(third.lsn));
    assert_eq!(replica_prepared_len(&harness, 3), 0);
    assert!(replica_has_resource(&harness, 3, 53));
    assert!(!replica_has_resource(&harness, 3, 54));
    assert!(
        pending_labels(&harness)
            .iter()
            .all(|label| !label.ends_with("-to-3") && !label.contains("-to-3-ack"))
    );
}

#[test]
fn stale_replica_rejoins_by_snapshot_transfer_when_primary_pruned_older_history() {
    let mut harness = primary_harness("replicated-rejoin-snapshot", 0x5a6);

    let first = harness
        .client_submit(replica(1), Slot(1), &create_payload(61, 11), "first")
        .unwrap();
    commit_to_backup(&mut harness, "first", first.lsn, 2);
    harness
        .deliver_protocol_message("first-prepare-1-to-3")
        .unwrap();
    harness.deliver_protocol_message("commit-1-to-3").unwrap();
    harness.checkpoint_replica(replica(1)).unwrap();

    set_replica_link(&mut harness, 1, 3, false);
    let second = harness
        .client_submit(replica(1), Slot(2), &create_payload(62, 12), "second")
        .unwrap();
    commit_to_backup(&mut harness, "second", second.lsn, 2);
    harness.checkpoint_replica(replica(1)).unwrap();

    let third = harness
        .client_submit(replica(1), Slot(3), &create_payload(63, 13), "third")
        .unwrap();
    commit_to_backup(&mut harness, "third", third.lsn, 2);
    harness.checkpoint_replica(replica(1)).unwrap();

    assert_eq!(replica_commit_lsn(&harness, 3), Some(first.lsn));
    assert_eq!(replica_snapshot_lsn(&harness, 3), None);

    let method = harness.rejoin_replica(replica(3), replica(1)).unwrap();
    assert_eq!(method, ReplicaRejoinMethod::SnapshotTransfer);
    assert_eq!(replica_commit_lsn(&harness, 3), Some(third.lsn));
    assert_eq!(replica_last_applied_lsn(&harness, 3), Some(third.lsn));
    assert_eq!(replica_snapshot_lsn(&harness, 3), Some(third.lsn));
    assert!(replica_has_resource(&harness, 3, 61));
    assert!(replica_has_resource(&harness, 3, 62));
    assert!(replica_has_resource(&harness, 3, 63));
    assert_eq!(replica_prepared_len(&harness, 3), 0);
}

#[test]
fn faulted_replica_rejoin_is_rejected() {
    let mut harness = primary_harness("replicated-rejoin-faulted", 0x5a7);
    let metadata_path = harness
        .replica_paths(replica(3))
        .unwrap()
        .metadata_path
        .clone();

    harness.crash_replica(replica(3)).unwrap();
    fs::write(&metadata_path, [0x01, 0x02, 0x03]).unwrap();
    let restarted = harness.restart_replica(replica(3)).unwrap();
    match restarted {
        ReplicatedScheduleObservationKind::ReplicaRestarted { after, .. } => {
            assert_eq!(
                after.runtime_status,
                ReplicaRuntimeStatus::Running(ReplicaNodeStatus::Faulted(
                    crate::replica::ReplicaFault {
                        reason: crate::replica::ReplicaFaultReason::MetadataLoad(
                            crate::replica::ReplicaMetadataLoadError::Decode(
                                crate::replica::ReplicaMetadataDecodeError::BufferTooShort,
                            ),
                        ),
                    },
                ))
            );
        }
        other => panic!("expected restart observation, got {other:?}"),
    }

    let error = harness.rejoin_replica(replica(3), replica(1)).unwrap_err();
    assert!(matches!(
        error,
        ReplicatedSimulationError::ReplicaFaulted(replica_id) if replica_id == replica(3)
    ));
    assert_eq!(
        harness.replica(replica(3)).unwrap().unwrap().status(),
        ReplicaNodeStatus::Faulted(crate::replica::ReplicaFault {
            reason: crate::replica::ReplicaFaultReason::MetadataLoad(
                crate::replica::ReplicaMetadataLoadError::Decode(
                    crate::replica::ReplicaMetadataDecodeError::BufferTooShort,
                ),
            ),
        })
    );

    harness.crash_replica(replica(3)).unwrap();
    let crashed_error = harness.rejoin_replica(replica(3), replica(1)).unwrap_err();
    assert!(matches!(
        crashed_error,
        ReplicatedSimulationError::ReplicaFaulted(replica_id) if replica_id == replica(3)
    ));
}

#[test]
fn rejoin_rejects_replica_that_knows_a_higher_view_than_the_primary() {
    let mut harness = primary_harness("replicated-rejoin-higher-view", 0x5a8);

    harness
        .replica_entry_mut(replica(3))
        .unwrap()
        .node
        .as_mut()
        .unwrap()
        .record_durable_vote(3, replica(2))
        .unwrap();
    harness
        .replica_entry_mut(replica(3))
        .unwrap()
        .node
        .as_mut()
        .unwrap()
        .enter_view_uncertain()
        .unwrap();

    let error = harness.rejoin_replica(replica(3), replica(1)).unwrap_err();
    assert!(matches!(
        error,
        ReplicatedSimulationError::ReplicaViewAheadOfPrimary {
            replica_id,
            highest_known_view: 3,
            primary_view: 1,
        } if replica_id == replica(3)
    ));
    assert_eq!(
        harness
            .replica(replica(3))
            .unwrap()
            .unwrap()
            .metadata()
            .durable_vote,
        Some(crate::replica::DurableVote {
            view: 3,
            voted_for: replica(2),
        })
    );
}

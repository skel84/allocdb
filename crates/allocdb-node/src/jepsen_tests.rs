use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use allocdb_core::ids::{Lsn, ResourceId, Slot};

use super::{
    JepsenAmbiguousOutcome, JepsenBlockingIssue, JepsenCommittedWrite, JepsenEventOutcome,
    JepsenExpiredReservation, JepsenHistoryEvent, JepsenOperation, JepsenOperationKind,
    JepsenReadState, JepsenReadTarget, JepsenResourceState, JepsenSuccessfulRead,
    JepsenWorkloadFamily, JepsenWriteResult, analyze_history, create_artifact_bundle,
    decode_history, encode_history, release_gate_plan,
};
use crate::replica::{ReplicaId, ReplicaRole};

fn temp_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("allocdb-jepsen-{name}-{nanos}"))
}

#[derive(Clone, Copy)]
struct ReserveEventSpec {
    sequence: u64,
    operation_id: u128,
    request_slot: u64,
    applied_lsn: u64,
    resource_id: u128,
    holder_id: u128,
    reservation_id: u128,
    expires_at_slot: u64,
}

fn reserve_event(spec: ReserveEventSpec) -> JepsenHistoryEvent {
    JepsenHistoryEvent {
        sequence: spec.sequence,
        process: String::from("client-1"),
        time_millis: u128::from(spec.sequence),
        operation: JepsenOperation {
            kind: JepsenOperationKind::Reserve,
            operation_id: Some(spec.operation_id),
            resource_id: Some(ResourceId(spec.resource_id)),
            reservation_id: None,
            holder_id: Some(spec.holder_id),
            required_lsn: None,
            request_slot: Some(Slot(spec.request_slot)),
            ttl_slots: Some(spec.expires_at_slot.saturating_sub(spec.request_slot)),
        },
        outcome: JepsenEventOutcome::CommittedWrite(JepsenCommittedWrite {
            applied_lsn: Lsn(spec.applied_lsn),
            result: JepsenWriteResult::Reserved {
                resource_id: ResourceId(spec.resource_id),
                holder_id: spec.holder_id,
                reservation_id: spec.reservation_id,
                expires_at_slot: Slot(spec.expires_at_slot),
            },
        }),
    }
}

fn release_event(
    sequence: u64,
    operation_id: u128,
    request_slot: u64,
    applied_lsn: u64,
    resource_id: u128,
    holder_id: u128,
    reservation_id: u128,
) -> JepsenHistoryEvent {
    JepsenHistoryEvent {
        sequence,
        process: String::from("client-1"),
        time_millis: u128::from(sequence),
        operation: JepsenOperation {
            kind: JepsenOperationKind::Release,
            operation_id: Some(operation_id),
            resource_id: Some(ResourceId(resource_id)),
            reservation_id: Some(reservation_id),
            holder_id: Some(holder_id),
            required_lsn: None,
            request_slot: Some(Slot(request_slot)),
            ttl_slots: None,
        },
        outcome: JepsenEventOutcome::CommittedWrite(JepsenCommittedWrite {
            applied_lsn: Lsn(applied_lsn),
            result: JepsenWriteResult::Released {
                resource_id: ResourceId(resource_id),
                holder_id,
                reservation_id,
                released_lsn: Some(Lsn(applied_lsn)),
            },
        }),
    }
}

#[test]
fn release_gate_plan_matches_documented_matrix() {
    let plan = release_gate_plan();
    assert_eq!(plan.len(), 15);
    assert_eq!(
        plan.iter()
            .filter(|run| run.workload == JepsenWorkloadFamily::ReservationContention)
            .count(),
        3
    );
    assert_eq!(
        plan.iter()
            .filter(|run| run.workload == JepsenWorkloadFamily::AmbiguousWriteRetry)
            .count(),
        4
    );
    assert!(
        plan.iter()
            .filter(|run| run.minimum_fault_window_secs.is_some())
            .all(|run| run.minimum_fault_window_secs == Some(30 * 60))
    );
}

#[test]
fn analysis_resolves_ambiguous_write_through_retry_cache() {
    let history = vec![
        JepsenHistoryEvent {
            sequence: 1,
            process: String::from("client-1"),
            time_millis: 1,
            operation: JepsenOperation {
                kind: JepsenOperationKind::Reserve,
                operation_id: Some(91),
                resource_id: Some(ResourceId(11)),
                reservation_id: None,
                holder_id: Some(7),
                required_lsn: None,
                request_slot: Some(Slot(5)),
                ttl_slots: Some(3),
            },
            outcome: JepsenEventOutcome::Ambiguous(JepsenAmbiguousOutcome::Timeout),
        },
        reserve_event(ReserveEventSpec {
            sequence: 2,
            operation_id: 91,
            request_slot: 5,
            applied_lsn: 3,
            resource_id: 11,
            holder_id: 7,
            reservation_id: 201,
            expires_at_slot: 8,
        }),
    ];

    let report = analyze_history(&history);
    assert!(report.release_gate_passed());
    assert_eq!(report.logical_commands.len(), 1);
    assert_eq!(report.logical_commands[0].ambiguous_attempts, 1);
    assert!(report.logical_commands[0].committed_result.is_some());
}

#[test]
fn analysis_flags_duplicate_committed_execution() {
    let history = vec![
        reserve_event(ReserveEventSpec {
            sequence: 1,
            operation_id: 44,
            request_slot: 5,
            applied_lsn: 2,
            resource_id: 11,
            holder_id: 7,
            reservation_id: 201,
            expires_at_slot: 8,
        }),
        reserve_event(ReserveEventSpec {
            sequence: 2,
            operation_id: 44,
            request_slot: 5,
            applied_lsn: 3,
            resource_id: 11,
            holder_id: 7,
            reservation_id: 202,
            expires_at_slot: 8,
        }),
    ];

    let report = analyze_history(&history);
    assert!(report.blockers.iter().any(|blocker| matches!(
        blocker,
        JepsenBlockingIssue::DuplicateCommittedExecution {
            operation_id: 44,
            ..
        }
    )));
}

#[test]
fn analysis_flags_stale_successful_read() {
    let history = vec![JepsenHistoryEvent {
        sequence: 1,
        process: String::from("reader"),
        time_millis: 1,
        operation: JepsenOperation {
            kind: JepsenOperationKind::GetResource,
            operation_id: None,
            resource_id: Some(ResourceId(11)),
            reservation_id: None,
            holder_id: None,
            required_lsn: Some(Lsn(9)),
            request_slot: None,
            ttl_slots: None,
        },
        outcome: JepsenEventOutcome::SuccessfulRead(JepsenSuccessfulRead {
            target: JepsenReadTarget::Resource,
            served_by: ReplicaId(2),
            served_role: ReplicaRole::Backup,
            observed_lsn: Some(Lsn(8)),
            state: JepsenReadState::Resource(JepsenResourceState::Available),
        }),
    }];

    let report = analyze_history(&history);
    assert!(
        report
            .blockers
            .iter()
            .any(|blocker| matches!(blocker, JepsenBlockingIssue::StaleSuccessfulRead(_)))
    );
}

#[test]
fn analysis_flags_early_expiration_release() {
    let history = vec![
        reserve_event(ReserveEventSpec {
            sequence: 1,
            operation_id: 41,
            request_slot: 5,
            applied_lsn: 2,
            resource_id: 11,
            holder_id: 7,
            reservation_id: 200,
            expires_at_slot: 8,
        }),
        JepsenHistoryEvent {
            sequence: 2,
            process: String::from("ticker"),
            time_millis: 2,
            operation: JepsenOperation {
                kind: JepsenOperationKind::TickExpirations,
                operation_id: Some(42),
                resource_id: None,
                reservation_id: None,
                holder_id: None,
                required_lsn: None,
                request_slot: Some(Slot(7)),
                ttl_slots: None,
            },
            outcome: JepsenEventOutcome::CommittedWrite(JepsenCommittedWrite {
                applied_lsn: Lsn(3),
                result: JepsenWriteResult::TickExpired {
                    expired: vec![JepsenExpiredReservation {
                        resource_id: ResourceId(11),
                        holder_id: 7,
                        reservation_id: 200,
                        released_lsn: Some(Lsn(3)),
                    }],
                },
            }),
        },
    ];

    let report = analyze_history(&history);
    assert!(report.blockers.iter().any(|blocker| matches!(
        blocker,
        JepsenBlockingIssue::EarlyExpirationRelease {
            resource_id,
            reservation_id: 200,
            ..
        } if *resource_id == ResourceId(11)
    )));
}

#[test]
fn history_codec_round_trips_none_lsn_and_tick_expired_without_resource_id() {
    let history = vec![
        JepsenHistoryEvent {
            sequence: 1,
            process: String::from("reader"),
            time_millis: 1,
            operation: JepsenOperation {
                kind: JepsenOperationKind::GetResource,
                operation_id: None,
                resource_id: Some(ResourceId(91)),
                reservation_id: None,
                holder_id: None,
                required_lsn: None,
                request_slot: None,
                ttl_slots: None,
            },
            outcome: JepsenEventOutcome::SuccessfulRead(JepsenSuccessfulRead {
                target: JepsenReadTarget::Resource,
                served_by: ReplicaId(2),
                served_role: ReplicaRole::Primary,
                observed_lsn: None,
                state: JepsenReadState::Resource(JepsenResourceState::Available),
            }),
        },
        JepsenHistoryEvent {
            sequence: 2,
            process: String::from("ticker"),
            time_millis: 2,
            operation: JepsenOperation {
                kind: JepsenOperationKind::TickExpirations,
                operation_id: Some(501),
                resource_id: None,
                reservation_id: None,
                holder_id: None,
                required_lsn: None,
                request_slot: Some(Slot(17)),
                ttl_slots: None,
            },
            outcome: JepsenEventOutcome::CommittedWrite(JepsenCommittedWrite {
                applied_lsn: Lsn(7),
                result: JepsenWriteResult::TickExpired {
                    expired: vec![JepsenExpiredReservation {
                        resource_id: ResourceId(91),
                        holder_id: 77,
                        reservation_id: 301,
                        released_lsn: None,
                    }],
                },
            }),
        },
    ];

    let encoded = encode_history(&history);
    let decoded = decode_history(&encoded).unwrap();
    assert_eq!(decoded, history);
}

#[test]
fn history_codec_round_trips_and_artifact_bundle_is_written() {
    let history = vec![
        reserve_event(ReserveEventSpec {
            sequence: 1,
            operation_id: 51,
            request_slot: 5,
            applied_lsn: 2,
            resource_id: 11,
            holder_id: 7,
            reservation_id: 200,
            expires_at_slot: 8,
        }),
        release_event(2, 52, 6, 3, 11, 7, 200),
    ];
    let encoded = encode_history(&history);
    let decoded = decode_history(&encoded).unwrap();
    assert_eq!(decoded, history);

    let output_root = temp_dir("bundle");
    let report = analyze_history(&history);
    let run_spec = release_gate_plan()
        .into_iter()
        .find(|run| run.run_id == "reservation_contention-control")
        .unwrap();
    let bundle_dir =
        create_artifact_bundle(&output_root, &run_spec, &history, &report, None).unwrap();
    assert!(bundle_dir.join("history.txt").exists());
    assert!(bundle_dir.join("analysis.txt").exists());
    assert!(bundle_dir.join("manifest.txt").exists());

    fs::remove_dir_all(output_root).unwrap();
}

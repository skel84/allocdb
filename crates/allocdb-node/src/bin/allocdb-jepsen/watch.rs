use std::collections::VecDeque;
use std::fs;
use std::path::Path;
use std::thread;
use std::time::Duration;

use allocdb_node::kubevirt_testbed::{KubevirtTestbedLayout, kubevirt_testbed_layout_path};
use allocdb_node::local_cluster::ReplicaRuntimeStatus;
use allocdb_node::{ApiRequest, ApiResponse, MetricsRequest, ReplicaId};

use super::kubevirt::{
    KubevirtWatchLaneContext, KubevirtWatchLaneSpec, load_kubevirt_layout, prepare_kubevirt_helper,
};
use super::runtime::request_remote_control_status;
use super::tracker::{
    RequestNamespace, RunStatusSnapshot, RunTrackerState, WatchEvent,
    maybe_load_run_status_snapshot, run_events_path, run_status_path,
};
use super::watch_render::{
    parse_watch_event_line, render_kubevirt_fleet_watch, render_kubevirt_watch,
};
use super::{
    JEPSEN_LATEST_STATUS_FILE_NAME, decode_external_api_response, send_remote_api_request,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ReplicaWatchSnapshot {
    pub(super) replica_id: ReplicaId,
    pub(super) status: Option<ReplicaRuntimeStatus>,
    pub(super) queue_depth: Option<u64>,
    pub(super) logical_slot_lag: Option<u64>,
    pub(super) expiration_backlog: Option<u64>,
    pub(super) accepting_writes: Option<bool>,
    pub(super) error: Option<String>,
}

pub(super) struct KubevirtWatchLaneSnapshot {
    pub(super) name: String,
    pub(super) snapshot: Option<RunStatusSnapshot>,
    pub(super) replicas: Vec<ReplicaWatchSnapshot>,
    pub(super) recent_events: Vec<WatchEvent>,
    pub(super) lane_error: Option<String>,
}

pub(super) fn watch_kubevirt(
    workspace_root: &Path,
    output_root: &Path,
    run_id: Option<&str>,
    refresh_millis: u64,
    follow: bool,
) -> Result<(), String> {
    let layout = load_kubevirt_layout(workspace_root)?;
    let _kubevirt_helper = prepare_kubevirt_helper(&layout)?;
    let status_path = run_id.map_or_else(
        || output_root.join(JEPSEN_LATEST_STATUS_FILE_NAME),
        |run_id| run_status_path(output_root, run_id),
    );
    let refresh_millis = refresh_millis.max(250);

    loop {
        let snapshot = maybe_load_run_status_snapshot(&status_path)?;
        let replicas = collect_replica_watch_snapshots(&layout);
        let recent_events = load_recent_run_events(output_root, snapshot.as_ref(), run_id, 8);
        render_kubevirt_watch(
            &status_path,
            snapshot.as_ref(),
            &replicas,
            &recent_events,
            refresh_millis,
            follow,
        )?;
        if should_stop_watch(snapshot.as_ref(), follow) {
            break;
        }
        thread::sleep(Duration::from_millis(refresh_millis));
    }
    Ok(())
}

pub(super) fn watch_kubevirt_fleet(
    lanes: &[KubevirtWatchLaneSpec],
    refresh_millis: u64,
    follow: bool,
) -> Result<(), String> {
    let mut contexts = lanes
        .iter()
        .map(|_| None::<KubevirtWatchLaneContext>)
        .collect::<Vec<_>>();
    let refresh_millis = refresh_millis.max(250);

    loop {
        let mut snapshots = Vec::with_capacity(lanes.len());
        for (index, spec) in lanes.iter().enumerate() {
            let mut lane_error = None;
            if contexts[index].is_none() {
                match try_prepare_kubevirt_watch_lane_context(spec) {
                    Ok(context) => contexts[index] = Some(context),
                    Err(error) => lane_error = Some(error),
                }
            }
            let snapshot = match contexts[index].as_ref() {
                Some(context) => match collect_kubevirt_watch_lane_snapshot(context) {
                    Ok(snapshot) => snapshot,
                    Err(error) => KubevirtWatchLaneSnapshot {
                        name: spec.name.clone(),
                        snapshot: None,
                        replicas: Vec::new(),
                        recent_events: Vec::new(),
                        lane_error: Some(error),
                    },
                },
                None => KubevirtWatchLaneSnapshot {
                    name: spec.name.clone(),
                    snapshot: None,
                    replicas: Vec::new(),
                    recent_events: Vec::new(),
                    lane_error: Some(lane_error.unwrap_or_else(|| {
                        format!(
                            "lane workspace is not ready yet: expected {}",
                            kubevirt_testbed_layout_path(&spec.workspace_root).display()
                        )
                    })),
                },
            };
            snapshots.push(snapshot);
        }
        render_kubevirt_fleet_watch(&snapshots, refresh_millis, follow)?;
        if !follow {
            break;
        }
        thread::sleep(Duration::from_millis(refresh_millis));
    }

    Ok(())
}

fn try_prepare_kubevirt_watch_lane_context(
    spec: &KubevirtWatchLaneSpec,
) -> Result<KubevirtWatchLaneContext, String> {
    let layout = load_kubevirt_layout(&spec.workspace_root)?;
    let helper = prepare_kubevirt_helper(&layout)?;
    Ok(KubevirtWatchLaneContext {
        spec: spec.clone(),
        layout,
        _kubevirt_helper: helper,
    })
}

fn collect_kubevirt_watch_lane_snapshot(
    lane: &KubevirtWatchLaneContext,
) -> Result<KubevirtWatchLaneSnapshot, String> {
    let status_path = lane.spec.output_root.join(JEPSEN_LATEST_STATUS_FILE_NAME);
    let snapshot = maybe_load_run_status_snapshot(&status_path)?;
    let replicas = collect_replica_watch_snapshots(&lane.layout);
    let recent_events = load_recent_run_events(&lane.spec.output_root, snapshot.as_ref(), None, 4);
    Ok(KubevirtWatchLaneSnapshot {
        name: lane.spec.name.clone(),
        snapshot,
        replicas,
        recent_events,
        lane_error: None,
    })
}

fn collect_replica_watch_snapshots(layout: &KubevirtTestbedLayout) -> Vec<ReplicaWatchSnapshot> {
    let metrics_namespace = RequestNamespace::new();
    layout
        .replica_layout
        .replicas
        .iter()
        .map(|replica| {
            let status = request_remote_control_status(layout, replica);
            match status {
                Ok(status) => {
                    let metrics = send_remote_api_request(
                        layout,
                        &replica.client_addr.ip().to_string(),
                        replica.client_addr.port(),
                        &ApiRequest::GetMetrics(MetricsRequest {
                            current_wall_clock_slot: metrics_namespace.slot(0),
                        }),
                    )
                    .and_then(|bytes| decode_external_api_response(layout, &bytes));
                    match metrics {
                        Ok(ApiResponse::GetMetrics(response)) => ReplicaWatchSnapshot {
                            replica_id: replica.replica_id,
                            status: Some(status),
                            queue_depth: Some(u64::from(response.metrics.queue_depth)),
                            logical_slot_lag: Some(response.metrics.core.logical_slot_lag),
                            expiration_backlog: Some(u64::from(
                                response.metrics.core.expiration_backlog,
                            )),
                            accepting_writes: Some(response.metrics.accepting_writes),
                            error: None,
                        },
                        Ok(other) => ReplicaWatchSnapshot {
                            replica_id: replica.replica_id,
                            status: Some(status),
                            queue_depth: None,
                            logical_slot_lag: None,
                            expiration_backlog: None,
                            accepting_writes: None,
                            error: Some(format!("unexpected metrics response {other:?}")),
                        },
                        Err(error) => ReplicaWatchSnapshot {
                            replica_id: replica.replica_id,
                            status: Some(status),
                            queue_depth: None,
                            logical_slot_lag: None,
                            expiration_backlog: None,
                            accepting_writes: None,
                            error: Some(error),
                        },
                    }
                }
                Err(error) => ReplicaWatchSnapshot {
                    replica_id: replica.replica_id,
                    status: None,
                    queue_depth: None,
                    logical_slot_lag: None,
                    expiration_backlog: None,
                    accepting_writes: None,
                    error: Some(error),
                },
            }
        })
        .collect()
}

fn load_recent_run_events(
    output_root: &Path,
    snapshot: Option<&RunStatusSnapshot>,
    run_id: Option<&str>,
    limit: usize,
) -> Vec<WatchEvent> {
    let Some(events_path) = snapshot
        .map(|snapshot| run_events_path(output_root, &snapshot.run_id))
        .or_else(|| run_id.map(|run_id| run_events_path(output_root, run_id)))
    else {
        return Vec::new();
    };
    let Ok(bytes) = fs::read_to_string(&events_path) else {
        return Vec::new();
    };
    let mut events = VecDeque::with_capacity(limit.max(1));
    for line in bytes.lines() {
        match parse_watch_event_line(line) {
            Ok(event) => {
                if events.len() == limit.max(1) {
                    events.pop_front();
                }
                events.push_back(event);
            }
            Err(error) => {
                log::debug!(
                    "discarding malformed watch event from {}: {}",
                    events_path.display(),
                    error
                );
            }
        }
    }
    events.into_iter().collect()
}

fn should_stop_watch(snapshot: Option<&RunStatusSnapshot>, follow: bool) -> bool {
    !follow || snapshot.is_some_and(|snapshot| snapshot.state != RunTrackerState::Running)
}

#[cfg(test)]
mod tests {
    use super::{load_recent_run_events, should_stop_watch};
    use crate::tracker::{
        RunStatusSnapshot, RunTrackerPhase, RunTrackerState, encode_tracker_field,
    };
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_output_root(label: &str) -> PathBuf {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let path = std::env::temp_dir().join(format!(
            "allocdb-watch-tests-{label}-{}-{millis}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("create watch temp dir");
        path
    }

    fn test_snapshot(run_id: &str, state: RunTrackerState) -> RunStatusSnapshot {
        RunStatusSnapshot {
            backend_name: String::from("kubevirt"),
            run_id: String::from(run_id),
            state,
            phase: RunTrackerPhase::Executing,
            detail: String::from("running"),
            started_at_millis: 1,
            updated_at_millis: 2,
            elapsed_secs: 0,
            minimum_fault_window_secs: Some(180),
            history_events: 0,
            history_file: None,
            artifact_bundle: None,
            logs_archive: None,
            release_gate_passed: None,
            blockers: None,
            last_error: None,
        }
    }

    #[test]
    fn should_stop_watch_honors_follow_and_terminal_states() {
        let running = test_snapshot("run-a", RunTrackerState::Running);
        let passed = test_snapshot("run-b", RunTrackerState::Passed);
        assert!(should_stop_watch(Some(&running), false));
        assert!(!should_stop_watch(Some(&running), true));
        assert!(should_stop_watch(Some(&passed), true));
        assert!(!should_stop_watch(None, true));
    }

    #[test]
    fn load_recent_run_events_keeps_only_latest_entries() {
        let output_root = temp_output_root("recent-events");
        let snapshot = test_snapshot("run-a", RunTrackerState::Running);
        let events_path = crate::tracker::run_events_path(&output_root, &snapshot.run_id);
        let contents = (1..=5)
            .map(|index| {
                format!(
                    "time_millis={} detail={}",
                    index,
                    encode_tracker_field(&format!("event {index}"))
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(&events_path, format!("{contents}\n")).expect("write watch events");

        let events = load_recent_run_events(&output_root, Some(&snapshot), None, 3);
        let details = events
            .into_iter()
            .map(|event| event.detail)
            .collect::<Vec<_>>();
        assert_eq!(details, vec!["event 3", "event 4", "event 5"]);
    }

    #[test]
    fn load_recent_run_events_discards_missing_and_malformed_lines() {
        let output_root = temp_output_root("malformed-events");
        let snapshot = test_snapshot("run-b", RunTrackerState::Running);
        assert!(load_recent_run_events(&output_root, Some(&snapshot), None, 4).is_empty());

        let events_path = crate::tracker::run_events_path(&output_root, &snapshot.run_id);
        fs::write(
            &events_path,
            format!(
                "not-an-event\n\
time_millis=1 detail={}\n\
time_millis=bad detail={}\n",
                encode_tracker_field("ok"),
                encode_tracker_field("ignored")
            ),
        )
        .expect("write malformed watch events");

        let events = load_recent_run_events(&output_root, Some(&snapshot), None, 4);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].detail, "ok");
    }
}

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
        if !follow {
            break;
        }
        if snapshot
            .as_ref()
            .is_some_and(|snapshot| snapshot.state != RunTrackerState::Running)
        {
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
    let mut events = bytes
        .lines()
        .filter_map(|line| parse_watch_event_line(line).ok())
        .collect::<Vec<_>>();
    if events.len() > limit {
        events.drain(..events.len().saturating_sub(limit));
    }
    events
}

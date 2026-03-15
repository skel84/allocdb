use allocdb_node::ReplicaRole;
use allocdb_node::local_cluster::{
    LocalClusterReplicaConfig, ReplicaRuntimeState, ReplicaRuntimeStatus,
    decode_control_status_response,
};

use crate::ExternalTestbed;
use crate::watch_render::replica_role_label;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct RuntimeReplicaProbe {
    pub(super) replica: LocalClusterReplicaConfig,
    pub(super) status: Result<ReplicaRuntimeStatus, String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct RuntimeReplicaTopology {
    pub(super) active: usize,
    pub(super) primaries: usize,
    pub(super) backups: usize,
}

pub(super) fn request_remote_control_status<T: ExternalTestbed>(
    layout: &T,
    replica: &LocalClusterReplicaConfig,
) -> Result<ReplicaRuntimeStatus, String> {
    let response_bytes = layout.run_remote_tcp_request(
        &replica.control_addr.ip().to_string(),
        replica.control_addr.port(),
        b"status",
    )?;
    let response = String::from_utf8(response_bytes).map_err(|error| {
        format!(
            "invalid control status utf8 for replica {}: {error}",
            replica.replica_id.get()
        )
    })?;
    decode_control_status_response(&response).map_err(|error| {
        format!(
            "invalid control status for replica {}: {error}",
            replica.replica_id.get()
        )
    })
}

pub(super) fn runtime_replica_probes_with_live_roles<T: ExternalTestbed>(
    layout: &T,
) -> Vec<RuntimeReplicaProbe> {
    layout
        .replica_layout()
        .replicas
        .iter()
        .cloned()
        .map(|mut replica| {
            // Enrich the cloned layout replica with the live role reported by control status.
            let status = request_remote_control_status(layout, &replica);
            if let Ok(status) = status.as_ref() {
                replica.role = status.role;
            }
            RuntimeReplicaProbe { replica, status }
        })
        .collect()
}

pub(super) fn runtime_probe_is_active(probe: &RuntimeReplicaProbe) -> bool {
    matches!(
        probe.status.as_ref(),
        Ok(status) if status.state == ReplicaRuntimeState::Active
    )
}

pub(super) fn summarize_runtime_probes(probes: &[RuntimeReplicaProbe]) -> RuntimeReplicaTopology {
    let mut topology = RuntimeReplicaTopology {
        active: 0,
        primaries: 0,
        backups: 0,
    };
    for probe in probes {
        let Ok(status) = probe.status.as_ref() else {
            continue;
        };
        if status.state != ReplicaRuntimeState::Active {
            continue;
        }
        topology.active += 1;
        match status.role {
            ReplicaRole::Primary => topology.primaries += 1,
            ReplicaRole::Backup => topology.backups += 1,
            ReplicaRole::Faulted | ReplicaRole::ViewUncertain | ReplicaRole::Recovering => {}
        }
    }
    topology
}

pub(super) fn live_runtime_replica_matching<F>(
    probes: &[RuntimeReplicaProbe],
    mut predicate: F,
) -> Option<LocalClusterReplicaConfig>
where
    F: FnMut(&LocalClusterReplicaConfig) -> bool,
{
    probes
        .iter()
        .filter(|probe| runtime_probe_is_active(probe))
        .map(|probe| probe.replica.clone())
        .find(|replica| predicate(replica))
}

pub(super) fn render_runtime_probe_summary(probes: &[RuntimeReplicaProbe]) -> String {
    probes
        .iter()
        .map(|probe| match &probe.status {
            Ok(status) if status.state == ReplicaRuntimeState::Active => format!(
                "{}:{}@view{}",
                probe.replica.replica_id.get(),
                replica_role_label(status.role),
                status.current_view
            ),
            Ok(status) => format!(
                "{}:faulted({})",
                probe.replica.replica_id.get(),
                status
                    .fault_reason
                    .as_deref()
                    .unwrap_or("replica faulted")
                    .lines()
                    .next()
                    .unwrap_or("replica faulted")
                    .trim()
            ),
            Err(error) => format!(
                "{}:down({})",
                probe.replica.replica_id.get(),
                error.lines().next().unwrap_or(error).trim()
            ),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

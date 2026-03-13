use std::collections::BTreeSet;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

use allocdb_core::config::Config;
use allocdb_core::ids::{Lsn, Slot};

use crate::engine::EngineConfig;
use crate::replica::{
    RecoverReplicaError, ReplicaId, ReplicaIdentity, ReplicaNode, ReplicaNodeStatus,
    ReplicaOpenError, ReplicaPaths, ReplicaRole,
};
use crate::simulation::{SimulatedSlotDriver, simulation_workspace_path};

#[cfg(test)]
#[path = "replicated_simulation_tests.rs"]
mod tests;

const REPLICA_COUNT: usize = 3;
const ENDPOINT_COUNT: usize = REPLICA_COUNT + 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClusterEndpoint {
    Client,
    Replica(ReplicaId),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReplicaRuntimeStatus {
    Running(ReplicaNodeStatus),
    Crashed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ReplicaObservation {
    pub replica_id: ReplicaId,
    pub runtime_status: ReplicaRuntimeStatus,
    pub role: Option<ReplicaRole>,
    pub current_view: Option<u64>,
    pub commit_lsn: Option<Lsn>,
    pub active_snapshot_lsn: Option<Lsn>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct QueuedProtocolMessage {
    pub message_id: u64,
    pub label: &'static str,
    pub from: ReplicaId,
    pub to: ReplicaId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReplicatedScheduleActionKind {
    QueueProtocolMessage {
        from: ReplicaId,
        to: ReplicaId,
        message_label: &'static str,
    },
    DeliverProtocolMessage {
        message_label: &'static str,
    },
    DropProtocolMessage {
        message_label: &'static str,
    },
    SetConnectivity {
        from: ClusterEndpoint,
        to: ClusterEndpoint,
        allowed: bool,
    },
    CrashReplica {
        replica_id: ReplicaId,
    },
    RestartReplica {
        replica_id: ReplicaId,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ReplicatedScheduleAction {
    pub label: &'static str,
    pub candidate_slots: Vec<Slot>,
    pub action: ReplicatedScheduleActionKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ReplicatedScheduleObservationKind {
    ProtocolMessageQueued {
        message: QueuedProtocolMessage,
        pending_messages: usize,
    },
    ProtocolMessageDelivered {
        message: QueuedProtocolMessage,
        recipient: ReplicaObservation,
        pending_messages: usize,
    },
    ProtocolMessageDropped {
        message: QueuedProtocolMessage,
        pending_messages: usize,
    },
    ConnectivityChanged {
        from: ClusterEndpoint,
        to: ClusterEndpoint,
        allowed: bool,
    },
    ReplicaCrashed {
        before: ReplicaObservation,
        after: ReplicaObservation,
    },
    ReplicaRestarted {
        before: ReplicaObservation,
        after: ReplicaObservation,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ReplicatedScheduleObservation {
    pub label: &'static str,
    pub slot: Slot,
    pub outcome: ReplicatedScheduleObservationKind,
}

#[derive(Debug)]
pub(crate) enum ReplicatedSimulationError {
    OpenReplica(ReplicaOpenError),
    RecoverReplica(RecoverReplicaError),
    UnknownReplica(ReplicaId),
    ReplicaAlreadyCrashed(ReplicaId),
    ReplicaAlreadyRunning(ReplicaId),
    MessageNotFound(&'static str),
    MessageDeliveryBlocked {
        label: &'static str,
        from: ClusterEndpoint,
        to: ClusterEndpoint,
    },
}

impl fmt::Display for ReplicatedSimulationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OpenReplica(error) => write!(formatter, "failed to open replica: {error:?}"),
            Self::RecoverReplica(error) => {
                write!(formatter, "failed to recover replica: {error:?}")
            }
            Self::UnknownReplica(replica_id) => {
                write!(formatter, "unknown replica: {}", replica_id.get())
            }
            Self::ReplicaAlreadyCrashed(replica_id) => {
                write!(formatter, "replica {} is already crashed", replica_id.get())
            }
            Self::ReplicaAlreadyRunning(replica_id) => {
                write!(formatter, "replica {} is already running", replica_id.get())
            }
            Self::MessageNotFound(label) => {
                write!(formatter, "protocol message {label} is not queued")
            }
            Self::MessageDeliveryBlocked { label, from, to } => write!(
                formatter,
                "protocol message {label} is blocked from {from:?} to {to:?}"
            ),
        }
    }
}

impl std::error::Error for ReplicatedSimulationError {}

#[derive(Debug)]
struct HarnessReplica {
    identity: ReplicaIdentity,
    paths: ReplicaPaths,
    node: Option<ReplicaNode>,
}

#[derive(Debug)]
struct ConnectivityMatrix {
    allowed: [[bool; ENDPOINT_COUNT]; ENDPOINT_COUNT],
}

impl ConnectivityMatrix {
    const fn fully_connected() -> Self {
        Self {
            allowed: [[true; ENDPOINT_COUNT]; ENDPOINT_COUNT],
        }
    }

    const fn allows(&self, from_index: usize, to_index: usize) -> bool {
        self.allowed[from_index][to_index]
    }

    fn set(&mut self, from_index: usize, to_index: usize, allowed: bool) {
        self.allowed[from_index][to_index] = allowed;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ResolvedReplicatedAction {
    label: &'static str,
    slot: Slot,
    action: ReplicatedScheduleActionKind,
}

#[derive(Debug)]
pub(crate) struct ReplicatedSimulationHarness {
    slot_driver: SimulatedSlotDriver,
    core_config: Config,
    engine_config: EngineConfig,
    replicas: Vec<HarnessReplica>,
    connectivity: ConnectivityMatrix,
    pending_messages: Vec<QueuedProtocolMessage>,
    next_message_id: u64,
}

impl ReplicatedSimulationHarness {
    pub(crate) fn new(
        name: &str,
        seed: u64,
        core_config: Config,
        engine_config: EngineConfig,
    ) -> Result<Self, ReplicatedSimulationError> {
        let mut replicas = Vec::with_capacity(REPLICA_COUNT);

        for replica_id in 1_u64..=u64::try_from(REPLICA_COUNT).expect("replica count fits u64") {
            let identity = ReplicaIdentity {
                replica_id: ReplicaId(replica_id),
                shard_id: core_config.shard_id,
            };
            let workspace = simulation_workspace_path(
                &format!("{name}-replica-{}", identity.replica_id.get()),
                seed,
            );
            let paths = ReplicaPaths::new(
                workspace.with_extension("replica"),
                workspace.with_extension("snapshot"),
                workspace.with_extension("wal"),
            );
            let node =
                ReplicaNode::open(core_config.clone(), engine_config, identity, paths.clone())
                    .map_err(ReplicatedSimulationError::OpenReplica)?;
            replicas.push(HarnessReplica {
                identity,
                paths,
                node: Some(node),
            });
        }

        Ok(Self {
            slot_driver: SimulatedSlotDriver::new(seed),
            core_config,
            engine_config,
            replicas,
            connectivity: ConnectivityMatrix::fully_connected(),
            pending_messages: Vec::new(),
            next_message_id: 1,
        })
    }

    pub(crate) const fn current_slot(&self) -> Slot {
        self.slot_driver.current_slot()
    }

    pub(crate) fn advance_to(&mut self, slot: Slot) {
        self.slot_driver.advance_to(slot);
    }

    pub(crate) fn replica(
        &self,
        replica_id: ReplicaId,
    ) -> Result<Option<&ReplicaNode>, ReplicatedSimulationError> {
        Ok(self.replica_entry(replica_id)?.node.as_ref())
    }

    pub(crate) fn replica_paths(
        &self,
        replica_id: ReplicaId,
    ) -> Result<&ReplicaPaths, ReplicatedSimulationError> {
        Ok(&self.replica_entry(replica_id)?.paths)
    }

    pub(crate) fn replica_observation(
        &self,
        replica_id: ReplicaId,
    ) -> Result<ReplicaObservation, ReplicatedSimulationError> {
        let replica = self.replica_entry(replica_id)?;
        Ok(match replica.node.as_ref() {
            Some(node) => ReplicaObservation {
                replica_id,
                runtime_status: ReplicaRuntimeStatus::Running(node.status()),
                role: Some(node.metadata().role),
                current_view: Some(node.metadata().current_view),
                commit_lsn: node.metadata().commit_lsn,
                active_snapshot_lsn: node.metadata().active_snapshot_lsn,
            },
            None => ReplicaObservation {
                replica_id,
                runtime_status: ReplicaRuntimeStatus::Crashed,
                role: None,
                current_view: None,
                commit_lsn: None,
                active_snapshot_lsn: None,
            },
        })
    }

    pub(crate) fn pending_messages(&self) -> &[QueuedProtocolMessage] {
        &self.pending_messages
    }

    pub(crate) fn connectivity_allows(
        &self,
        from: ClusterEndpoint,
        to: ClusterEndpoint,
    ) -> Result<bool, ReplicatedSimulationError> {
        let from_index = self.endpoint_index(from)?;
        let to_index = self.endpoint_index(to)?;
        Ok(self.connectivity.allows(from_index, to_index))
    }

    pub(crate) fn set_connectivity(
        &mut self,
        from: ClusterEndpoint,
        to: ClusterEndpoint,
        allowed: bool,
    ) -> Result<ReplicatedScheduleObservationKind, ReplicatedSimulationError> {
        let from_index = self.endpoint_index(from)?;
        let to_index = self.endpoint_index(to)?;
        self.connectivity.set(from_index, to_index, allowed);
        Ok(ReplicatedScheduleObservationKind::ConnectivityChanged { from, to, allowed })
    }

    pub(crate) fn queue_protocol_message(
        &mut self,
        from: ReplicaId,
        to: ReplicaId,
        message_label: &'static str,
    ) -> Result<ReplicatedScheduleObservationKind, ReplicatedSimulationError> {
        let _ = self.replica_entry(from)?;
        let _ = self.replica_entry(to)?;
        if self.replica_observation(from)?.runtime_status == ReplicaRuntimeStatus::Crashed {
            return Err(ReplicatedSimulationError::ReplicaAlreadyCrashed(from));
        }
        assert!(
            self.pending_messages
                .iter()
                .all(|message| message.label != message_label),
            "pending protocol messages must have unique labels"
        );
        let message = QueuedProtocolMessage {
            message_id: self.next_message_id,
            label: message_label,
            from,
            to,
        };
        self.next_message_id += 1;
        self.pending_messages.push(message.clone());
        Ok(ReplicatedScheduleObservationKind::ProtocolMessageQueued {
            message,
            pending_messages: self.pending_messages.len(),
        })
    }

    pub(crate) fn deliver_protocol_message(
        &mut self,
        message_label: &'static str,
    ) -> Result<ReplicatedScheduleObservationKind, ReplicatedSimulationError> {
        let index = self.pending_message_index(message_label)?;
        let message = self.pending_messages[index].clone();
        let from = ClusterEndpoint::Replica(message.from);
        let to = ClusterEndpoint::Replica(message.to);
        if !self.connectivity_allows(from, to)? {
            return Err(ReplicatedSimulationError::MessageDeliveryBlocked {
                label: message_label,
                from,
                to,
            });
        }

        let recipient = self.replica_observation(message.to)?;
        if recipient.runtime_status == ReplicaRuntimeStatus::Crashed {
            return Err(ReplicatedSimulationError::ReplicaAlreadyCrashed(message.to));
        }

        self.pending_messages.remove(index);
        Ok(
            ReplicatedScheduleObservationKind::ProtocolMessageDelivered {
                message,
                recipient,
                pending_messages: self.pending_messages.len(),
            },
        )
    }

    pub(crate) fn drop_protocol_message(
        &mut self,
        message_label: &'static str,
    ) -> Result<ReplicatedScheduleObservationKind, ReplicatedSimulationError> {
        let index = self.pending_message_index(message_label)?;
        let message = self.pending_messages.remove(index);
        Ok(ReplicatedScheduleObservationKind::ProtocolMessageDropped {
            message,
            pending_messages: self.pending_messages.len(),
        })
    }

    pub(crate) fn crash_replica(
        &mut self,
        replica_id: ReplicaId,
    ) -> Result<ReplicatedScheduleObservationKind, ReplicatedSimulationError> {
        let before = self.replica_observation(replica_id)?;
        {
            let replica = self.replica_entry_mut(replica_id)?;
            if replica.node.is_none() {
                return Err(ReplicatedSimulationError::ReplicaAlreadyCrashed(replica_id));
            }
            drop(replica.node.take());
        }
        let after = self.replica_observation(replica_id)?;
        Ok(ReplicatedScheduleObservationKind::ReplicaCrashed { before, after })
    }

    pub(crate) fn restart_replica(
        &mut self,
        replica_id: ReplicaId,
    ) -> Result<ReplicatedScheduleObservationKind, ReplicatedSimulationError> {
        let before = self.replica_observation(replica_id)?;
        let core_config = self.core_config.clone();
        let engine_config = self.engine_config;
        let (identity, paths) = {
            let replica = self.replica_entry_mut(replica_id)?;
            if replica.node.is_some() {
                return Err(ReplicatedSimulationError::ReplicaAlreadyRunning(replica_id));
            }
            (replica.identity, replica.paths.clone())
        };
        let recovered = ReplicaNode::recover(core_config, engine_config, identity, paths)
            .map_err(ReplicatedSimulationError::RecoverReplica)?;
        self.replica_entry_mut(replica_id)?.node = Some(recovered);
        let after = self.replica_observation(replica_id)?;
        Ok(ReplicatedScheduleObservationKind::ReplicaRestarted { before, after })
    }

    pub(crate) fn explore_schedule(
        &mut self,
        actions: &[ReplicatedScheduleAction],
    ) -> Result<Vec<ReplicatedScheduleObservation>, ReplicatedSimulationError> {
        let mut seen_labels = BTreeSet::new();
        let current_slot = self.current_slot();
        let mut pending = Vec::with_capacity(actions.len());

        for action in actions {
            assert!(
                seen_labels.insert(action.label),
                "replicated schedule actions must have unique labels"
            );
            let slot = self.resolve_schedule_slot(&action.candidate_slots);
            assert!(
                slot >= current_slot,
                "replicated schedule actions must not resolve before the current simulated slot"
            );
            pending.push(ResolvedReplicatedAction {
                label: action.label,
                slot,
                action: action.action,
            });
        }

        pending.sort_unstable_by_key(|action| (action.slot, action.label));
        let mut transcript = Vec::with_capacity(pending.len());

        while let Some(next_slot) = pending.first().map(|action| action.slot) {
            self.advance_to(next_slot);
            let ready_len = pending
                .iter()
                .take_while(|action| action.slot == next_slot)
                .count();
            let index = self.slot_driver.choose_index(ready_len);
            let action = pending.remove(index);
            transcript.push(self.execute_schedule_action(action)?);
        }

        Ok(transcript)
    }

    fn execute_schedule_action(
        &mut self,
        action: ResolvedReplicatedAction,
    ) -> Result<ReplicatedScheduleObservation, ReplicatedSimulationError> {
        let outcome = match action.action {
            ReplicatedScheduleActionKind::QueueProtocolMessage {
                from,
                to,
                message_label,
            } => self.queue_protocol_message(from, to, message_label)?,
            ReplicatedScheduleActionKind::DeliverProtocolMessage { message_label } => {
                self.deliver_protocol_message(message_label)?
            }
            ReplicatedScheduleActionKind::DropProtocolMessage { message_label } => {
                self.drop_protocol_message(message_label)?
            }
            ReplicatedScheduleActionKind::SetConnectivity { from, to, allowed } => {
                self.set_connectivity(from, to, allowed)?
            }
            ReplicatedScheduleActionKind::CrashReplica { replica_id } => {
                self.crash_replica(replica_id)?
            }
            ReplicatedScheduleActionKind::RestartReplica { replica_id } => {
                self.restart_replica(replica_id)?
            }
        };

        Ok(ReplicatedScheduleObservation {
            label: action.label,
            slot: self.current_slot(),
            outcome,
        })
    }

    fn resolve_schedule_slot(&mut self, candidate_slots: &[Slot]) -> Slot {
        assert!(
            !candidate_slots.is_empty(),
            "replicated schedule action requires at least one candidate slot"
        );

        let mut resolved = candidate_slots.to_vec();
        resolved.sort_unstable();
        resolved.dedup();
        let index = self.slot_driver.choose_index(resolved.len());
        resolved[index]
    }

    fn pending_message_index(
        &self,
        message_label: &'static str,
    ) -> Result<usize, ReplicatedSimulationError> {
        self.pending_messages
            .iter()
            .position(|message| message.label == message_label)
            .ok_or(ReplicatedSimulationError::MessageNotFound(message_label))
    }

    fn endpoint_index(
        &self,
        endpoint: ClusterEndpoint,
    ) -> Result<usize, ReplicatedSimulationError> {
        match endpoint {
            ClusterEndpoint::Client => Ok(0),
            ClusterEndpoint::Replica(replica_id) => {
                self.replica_index(replica_id).map(|index| index + 1)
            }
        }
    }

    fn replica_index(&self, replica_id: ReplicaId) -> Result<usize, ReplicatedSimulationError> {
        self.replicas
            .iter()
            .position(|replica| replica.identity.replica_id == replica_id)
            .ok_or(ReplicatedSimulationError::UnknownReplica(replica_id))
    }

    fn replica_entry(
        &self,
        replica_id: ReplicaId,
    ) -> Result<&HarnessReplica, ReplicatedSimulationError> {
        let index = self.replica_index(replica_id)?;
        Ok(&self.replicas[index])
    }

    fn replica_entry_mut(
        &mut self,
        replica_id: ReplicaId,
    ) -> Result<&mut HarnessReplica, ReplicatedSimulationError> {
        let index = self.replica_index(replica_id)?;
        Ok(&mut self.replicas[index])
    }
}

impl Drop for ReplicatedSimulationHarness {
    fn drop(&mut self) {
        for replica in &mut self.replicas {
            drop(replica.node.take());
            remove_if_exists(&replica.paths.wal_path);
            remove_if_exists(&replica.paths.snapshot_path);
            remove_if_exists(&replica.paths.metadata_path);
            remove_if_exists(&metadata_temp_path(&replica.paths.metadata_path));
        }
    }
}

fn metadata_temp_path(path: &Path) -> PathBuf {
    let mut temp_path = path.to_path_buf();
    let extension = temp_path
        .extension()
        .and_then(|value| value.to_str())
        .map_or_else(|| "tmp".to_owned(), |value| format!("{value}.tmp"));
    temp_path.set_extension(extension);
    temp_path
}

fn remove_if_exists(path: &Path) {
    match fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => panic!("failed to remove {}: {error}", path.display()),
    }
}

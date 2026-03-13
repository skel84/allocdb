use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

use allocdb_core::command_codec::decode_client_request;
use allocdb_core::config::Config;
use allocdb_core::ids::{Lsn, ResourceId, Slot};
use allocdb_core::result::{CommandOutcome, ResultCode};
use allocdb_core::snapshot_file::{SnapshotFile, SnapshotFileError};
use allocdb_core::state_machine::ResourceRecord;
use allocdb_core::wal::{Frame, RecordType, ScanStopReason};
use allocdb_core::wal_file::{RecoveredWal, WalFile, WalFileError};
use log::{debug, info, warn};

use crate::engine::{CheckpointResult, EngineConfig, SubmissionError, SubmissionResult};
use crate::replica::{
    NotPrimaryReadError, RecoverReplicaError, ReplicaCheckpointError, ReplicaId, ReplicaIdentity,
    ReplicaMetadata, ReplicaMetadataFile, ReplicaMetadataFileError, ReplicaNode, ReplicaNodeStatus,
    ReplicaOpenError, ReplicaPaths, ReplicaPreparedEntry, ReplicaProtocolError, ReplicaRole,
    prepare_log_path,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReplicaRejoinMethod {
    SuffixOnly,
    SnapshotTransfer,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ReplicatedClientRequestOutcome {
    Prepared(ReplicaPreparedEntry),
    Published(SubmissionResult),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ProtocolMessage {
    Opaque,
    Prepare {
        entry: ReplicaPreparedEntry,
    },
    PrepareAck {
        view: u64,
        lsn: Lsn,
        from: ReplicaId,
    },
    Commit {
        view: u64,
        commit_lsn: Lsn,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct QueuedProtocolMessage {
    pub message_id: u64,
    pub label: String,
    pub from: ReplicaId,
    pub to: ReplicaId,
    pub payload: ProtocolMessage,
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
    Checkpoint(ReplicaCheckpointError),
    Protocol(ReplicaProtocolError),
    Read(NotPrimaryReadError),
    MetadataFile(ReplicaMetadataFileError),
    SnapshotFile(SnapshotFileError),
    WalFile(WalFileError),
    UnknownReplica(ReplicaId),
    NotConfiguredPrimary {
        expected: Option<ReplicaId>,
        found: ReplicaId,
    },
    PendingCommitNotFound(Lsn),
    ReplicaAlreadyCrashed(ReplicaId),
    ReplicaAlreadyRunning(ReplicaId),
    MessageNotFound(String),
    MessageDeliveryBlocked {
        label: String,
        from: ClusterEndpoint,
        to: ClusterEndpoint,
    },
    ViewChangeQuorumUnavailable {
        candidate: ReplicaId,
        reachable: usize,
    },
    ViewChangeMissingPreparedEntry {
        candidate: ReplicaId,
        lsn: Lsn,
    },
    ReplicaFaulted(ReplicaId),
    RejoinPrimary(ReplicaId),
    ReplicaWalNotClean {
        replica_id: ReplicaId,
        stop_reason: ScanStopReason,
    },
    ReplicaViewAheadOfPrimary {
        replica_id: ReplicaId,
        highest_known_view: u64,
        primary_view: u64,
    },
    ReplicaCommitAheadOfPrimary {
        replica_id: ReplicaId,
        target_commit_lsn: Lsn,
        primary_commit_lsn: Option<Lsn>,
    },
    SnapshotTransferUnavailable {
        primary: ReplicaId,
        replica: ReplicaId,
        primary_replay_floor: u64,
    },
    PrimarySnapshotMissing {
        primary: ReplicaId,
        snapshot_lsn: Lsn,
    },
}

impl fmt::Display for ReplicatedSimulationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(result) = self.format_general_error(formatter) {
            return result;
        }
        if let Some(result) = self.format_view_change_error(formatter) {
            return result;
        }
        self.format_rejoin_error(formatter)
    }
}

impl std::error::Error for ReplicatedSimulationError {}

impl ReplicatedSimulationError {
    fn format_general_error(&self, formatter: &mut fmt::Formatter<'_>) -> Option<fmt::Result> {
        match self {
            Self::OpenReplica(error) => {
                Some(write!(formatter, "failed to open replica: {error:?}"))
            }
            Self::RecoverReplica(error) => {
                Some(write!(formatter, "failed to recover replica: {error:?}"))
            }
            Self::Checkpoint(error) => {
                Some(write!(formatter, "failed to checkpoint replica: {error:?}"))
            }
            Self::Protocol(error) => Some(write!(formatter, "replica protocol error: {error:?}")),
            Self::Read(error) => Some(write!(formatter, "replica read error: {error:?}")),
            Self::MetadataFile(error) => {
                Some(write!(formatter, "replica metadata error: {error:?}"))
            }
            Self::SnapshotFile(error) => Some(write!(formatter, "snapshot file error: {error:?}")),
            Self::WalFile(error) => Some(write!(formatter, "wal file error: {error:?}")),
            Self::UnknownReplica(replica_id) => {
                Some(write!(formatter, "unknown replica: {}", replica_id.get()))
            }
            Self::NotConfiguredPrimary { expected, found } => Some(write!(
                formatter,
                "replica {} is not the configured primary {:?}",
                found.get(),
                expected.map(ReplicaId::get)
            )),
            Self::PendingCommitNotFound(lsn) => Some(write!(
                formatter,
                "pending primary commit not found for lsn {}",
                lsn.get()
            )),
            Self::ReplicaAlreadyCrashed(replica_id) => Some(write!(
                formatter,
                "replica {} is already crashed",
                replica_id.get()
            )),
            Self::ReplicaAlreadyRunning(replica_id) => Some(write!(
                formatter,
                "replica {} is already running",
                replica_id.get()
            )),
            Self::MessageNotFound(label) => {
                Some(write!(formatter, "protocol message {label} is not queued"))
            }
            Self::MessageDeliveryBlocked { label, from, to } => Some(write!(
                formatter,
                "protocol message {label} is blocked from {from:?} to {to:?}"
            )),
            _ => None,
        }
    }

    fn format_view_change_error(&self, formatter: &mut fmt::Formatter<'_>) -> Option<fmt::Result> {
        match self {
            Self::ViewChangeQuorumUnavailable {
                candidate,
                reachable,
            } => Some(write!(
                formatter,
                "replica {} cannot complete view change because only {} active voters are reachable",
                candidate.get(),
                reachable
            )),
            Self::ViewChangeMissingPreparedEntry { candidate, lsn } => Some(write!(
                formatter,
                "replica {} cannot reconstruct committed lsn {} during view change",
                candidate.get(),
                lsn.get()
            )),
            _ => None,
        }
    }

    fn format_rejoin_error(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReplicaFaulted(replica_id) => {
                write!(formatter, "replica {} is faulted", replica_id.get())
            }
            Self::RejoinPrimary(replica_id) => {
                write!(
                    formatter,
                    "replica {} cannot rejoin itself",
                    replica_id.get()
                )
            }
            Self::ReplicaWalNotClean {
                replica_id,
                stop_reason,
            } => write!(
                formatter,
                "replica {} WAL is not clean enough for rejoin: {stop_reason:?}",
                replica_id.get()
            ),
            Self::ReplicaViewAheadOfPrimary {
                replica_id,
                highest_known_view,
                primary_view,
            } => write!(
                formatter,
                "replica {} knows higher view {} than primary view {} during rejoin",
                replica_id.get(),
                highest_known_view,
                primary_view
            ),
            Self::ReplicaCommitAheadOfPrimary {
                replica_id,
                target_commit_lsn,
                primary_commit_lsn,
            } => write!(
                formatter,
                "replica {} commit lsn {} is ahead of primary commit {:?}",
                replica_id.get(),
                target_commit_lsn.get(),
                primary_commit_lsn.map(Lsn::get)
            ),
            Self::SnapshotTransferUnavailable {
                primary,
                replica,
                primary_replay_floor,
            } => write!(
                formatter,
                "replica {} needs snapshot transfer from primary {} because retained WAL starts after lsn {}",
                replica.get(),
                primary.get(),
                primary_replay_floor
            ),
            Self::PrimarySnapshotMissing {
                primary,
                snapshot_lsn,
            } => write!(
                formatter,
                "primary {} is missing snapshot lsn {} required for snapshot transfer",
                primary.get(),
                snapshot_lsn.get()
            ),
            _ => unreachable!("non-rejoin error must already be formatted"),
        }
    }
}

impl From<ReplicaProtocolError> for ReplicatedSimulationError {
    fn from(error: ReplicaProtocolError) -> Self {
        Self::Protocol(error)
    }
}

impl From<ReplicaCheckpointError> for ReplicatedSimulationError {
    fn from(error: ReplicaCheckpointError) -> Self {
        Self::Checkpoint(error)
    }
}

impl From<NotPrimaryReadError> for ReplicatedSimulationError {
    fn from(error: NotPrimaryReadError) -> Self {
        Self::Read(error)
    }
}

impl From<ReplicaMetadataFileError> for ReplicatedSimulationError {
    fn from(error: ReplicaMetadataFileError) -> Self {
        Self::MetadataFile(error)
    }
}

impl From<SnapshotFileError> for ReplicatedSimulationError {
    fn from(error: SnapshotFileError) -> Self {
        Self::SnapshotFile(error)
    }
}

impl From<WalFileError> for ReplicatedSimulationError {
    fn from(error: WalFileError) -> Self {
        Self::WalFile(error)
    }
}

#[derive(Debug)]
struct HarnessReplica {
    identity: ReplicaIdentity,
    paths: ReplicaPaths,
    node: Option<ReplicaNode>,
}

#[derive(Clone, Debug)]
struct PrimaryCatchUpState {
    paths: ReplicaPaths,
    current_view: u64,
    commit_lsn: Option<Lsn>,
    active_snapshot_lsn: Option<Lsn>,
    retained_frames: Vec<Frame>,
    replay_floor: u64,
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
struct PendingPrimaryCommit {
    primary: ReplicaId,
    entry: ReplicaPreparedEntry,
    acked_replicas: BTreeSet<u64>,
    committed: bool,
}

#[derive(Debug)]
pub(crate) struct ReplicatedSimulationHarness {
    slot_driver: SimulatedSlotDriver,
    core_config: Config,
    engine_config: EngineConfig,
    replicas: Vec<HarnessReplica>,
    connectivity: ConnectivityMatrix,
    pending_messages: Vec<QueuedProtocolMessage>,
    configured_primary: Option<ReplicaId>,
    pending_commits: BTreeMap<u64, PendingPrimaryCommit>,
    published_results: BTreeMap<u64, SubmissionResult>,
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
            configured_primary: None,
            pending_commits: BTreeMap::new(),
            published_results: BTreeMap::new(),
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

    pub(crate) const fn configured_primary(&self) -> Option<ReplicaId> {
        self.configured_primary
    }

    pub(crate) fn configure_primary(
        &mut self,
        replica_id: ReplicaId,
        view: u64,
    ) -> Result<(), ReplicatedSimulationError> {
        for index in 0..self.replicas.len() {
            let current_replica_id = self.replicas[index].identity.replica_id;
            let role = if self.replicas[index].identity.replica_id == replica_id {
                ReplicaRole::Primary
            } else {
                ReplicaRole::Backup
            };
            let node = self.replicas[index].node.as_mut().ok_or(
                ReplicatedSimulationError::ReplicaAlreadyCrashed(current_replica_id),
            )?;
            node.configure_normal_role(view, role)?;
        }
        self.configured_primary = Some(replica_id);
        Ok(())
    }

    pub(crate) fn client_submit(
        &mut self,
        primary: ReplicaId,
        request_slot: Slot,
        payload: &[u8],
        label_prefix: &str,
    ) -> Result<ReplicaPreparedEntry, ReplicatedSimulationError> {
        self.fail_primary_closed_if_quorum_lost()?;
        if self.configured_primary != Some(primary) {
            return Err(ReplicatedSimulationError::NotConfiguredPrimary {
                expected: self.configured_primary,
                found: primary,
            });
        }

        let entry = self
            .replica_entry_mut(primary)?
            .node
            .as_mut()
            .ok_or(ReplicatedSimulationError::ReplicaAlreadyCrashed(primary))?
            .prepare_client_request(request_slot, payload)?;
        self.pending_commits.insert(
            entry.lsn.get(),
            PendingPrimaryCommit {
                primary,
                entry: entry.clone(),
                acked_replicas: BTreeSet::from([primary.get()]),
                committed: false,
            },
        );

        let backup_ids = self
            .replicas
            .iter()
            .map(|replica| replica.identity.replica_id)
            .filter(|replica_id| *replica_id != primary)
            .collect::<Vec<_>>();
        for backup_id in backup_ids {
            let message_label = format!(
                "{label_prefix}-prepare-{}-to-{}",
                entry.lsn.get(),
                backup_id.get()
            );
            self.queue_protocol_message_with_payload(
                primary,
                backup_id,
                message_label,
                ProtocolMessage::Prepare {
                    entry: entry.clone(),
                },
            )?;
        }

        Ok(entry)
    }

    pub(crate) fn client_submit_or_retry(
        &mut self,
        primary: ReplicaId,
        request_slot: Slot,
        payload: &[u8],
        label_prefix: &str,
    ) -> Result<ReplicatedClientRequestOutcome, ReplicatedSimulationError> {
        self.fail_primary_closed_if_quorum_lost()?;
        if self.configured_primary != Some(primary) {
            return Err(ReplicatedSimulationError::NotConfiguredPrimary {
                expected: self.configured_primary,
                found: primary,
            });
        }

        if let Some(result) = self.lookup_retry_result(primary, request_slot, payload)? {
            return Ok(ReplicatedClientRequestOutcome::Published(result));
        }

        self.client_submit(primary, request_slot, payload, label_prefix)
            .map(ReplicatedClientRequestOutcome::Prepared)
    }

    pub(crate) fn published_result(&self, lsn: Lsn) -> Option<&SubmissionResult> {
        self.published_results.get(&lsn.get())
    }

    pub(crate) fn checkpoint_replica(
        &mut self,
        replica_id: ReplicaId,
    ) -> Result<CheckpointResult, ReplicatedSimulationError> {
        let node = self
            .replica_entry_mut(replica_id)?
            .node
            .as_mut()
            .ok_or(ReplicatedSimulationError::ReplicaAlreadyCrashed(replica_id))?;
        let result = node.checkpoint_local_state()?;
        info!(
            "ReplicaCheckpointCompleted: replica_id={} snapshot_lsn={:?} previous_snapshot_lsn={:?} retained_frame_count={}",
            replica_id.get(),
            result.snapshot_lsn,
            result.previous_snapshot_lsn,
            result.retained_frame_count
        );
        Ok(result)
    }

    pub(crate) fn read_resource(
        &mut self,
        replica_id: ReplicaId,
        resource_id: ResourceId,
        required_lsn: Option<Lsn>,
    ) -> Result<Option<ResourceRecord>, ReplicatedSimulationError> {
        self.fail_primary_closed_if_quorum_lost()?;
        let node = self
            .replica(replica_id)?
            .ok_or(ReplicatedSimulationError::ReplicaAlreadyCrashed(replica_id))?;
        node.enforce_primary_read(required_lsn.unwrap_or(Lsn(0)))?;
        Ok(node
            .engine()
            .and_then(|engine| engine.db().resource(resource_id)))
    }

    pub(crate) fn rejoin_replica(
        &mut self,
        replica_id: ReplicaId,
        primary: ReplicaId,
    ) -> Result<ReplicaRejoinMethod, ReplicatedSimulationError> {
        let (source, target_metadata) = self.validate_rejoin_request(replica_id, primary)?;
        let (paths, target_recovered_wal) = self.prepare_rejoin_target(replica_id)?;
        let method = self.write_rejoin_state(
            replica_id,
            primary,
            &source,
            target_metadata,
            &target_recovered_wal,
            &paths,
        )?;
        self.restart_rejoined_replica(replica_id, &paths, source.current_view)?;

        let active_snapshot_lsn = self
            .replica(replica_id)?
            .and_then(|node| node.metadata().active_snapshot_lsn);
        info!(
            "ReplicaRejoined: replica_id={} primary={} method={:?} view={} commit_lsn={:?} active_snapshot_lsn={:?}",
            replica_id.get(),
            primary.get(),
            method,
            source.current_view,
            source.commit_lsn,
            active_snapshot_lsn
        );
        Ok(method)
    }

    pub(crate) fn complete_view_change(
        &mut self,
        new_primary: ReplicaId,
        new_view: u64,
    ) -> Result<(), ReplicatedSimulationError> {
        self.fail_primary_closed_if_quorum_lost()?;
        let voters = self.require_view_change_quorum(new_primary)?;
        let target_commit_lsn = self.view_change_target_commit_lsn(&voters)?;
        self.record_view_change_votes(&voters, new_primary, new_view)?;
        self.reconstruct_view_change_prefix(new_primary, &voters, target_commit_lsn)?;
        self.install_view_change_roles(&voters, new_primary, new_view)?;

        self.configured_primary = Some(new_primary);
        self.pending_commits
            .retain(|_, pending| pending.entry.view >= new_view);
        self.pending_messages.retain(|message| {
            protocol_message_view(&message.payload)
                .is_none_or(|message_view| message_view >= new_view)
        });
        info!(
            "ReplicaViewChangeCompleted: primary={} view={} voters={} target_commit_lsn={:?} pending_messages={} pending_commits={}",
            new_primary.get(),
            new_view,
            voters.len(),
            target_commit_lsn,
            self.pending_messages.len(),
            self.pending_commits.len()
        );
        Ok(())
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
        self.fail_primary_closed_if_quorum_lost()?;
        Ok(ReplicatedScheduleObservationKind::ConnectivityChanged { from, to, allowed })
    }

    pub(crate) fn queue_protocol_message(
        &mut self,
        from: ReplicaId,
        to: ReplicaId,
        message_label: &'static str,
    ) -> Result<ReplicatedScheduleObservationKind, ReplicatedSimulationError> {
        self.queue_protocol_message_with_payload(
            from,
            to,
            message_label.to_owned(),
            ProtocolMessage::Opaque,
        )
    }

    fn queue_protocol_message_with_payload(
        &mut self,
        from: ReplicaId,
        to: ReplicaId,
        message_label: String,
        payload: ProtocolMessage,
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
            payload,
        };
        self.next_message_id += 1;
        self.pending_messages.push(message.clone());
        log_protocol_message_event(
            "ProtocolMessageQueued",
            &message,
            self.pending_messages.len(),
            self.next_message_id,
        );
        Ok(ReplicatedScheduleObservationKind::ProtocolMessageQueued {
            message,
            pending_messages: self.pending_messages.len(),
        })
    }

    pub(crate) fn deliver_protocol_message(
        &mut self,
        message_label: &str,
    ) -> Result<ReplicatedScheduleObservationKind, ReplicatedSimulationError> {
        let index = self.pending_message_index(message_label)?;
        let message = self.pending_messages[index].clone();
        let from = ClusterEndpoint::Replica(message.from);
        let to = ClusterEndpoint::Replica(message.to);
        if !self.connectivity_allows(from, to)? {
            return Err(ReplicatedSimulationError::MessageDeliveryBlocked {
                label: message_label.to_owned(),
                from,
                to,
            });
        }
        if self.replica_observation(message.to)?.runtime_status == ReplicaRuntimeStatus::Crashed {
            return Err(ReplicatedSimulationError::ReplicaAlreadyCrashed(message.to));
        }

        self.pending_messages.remove(index);
        self.apply_protocol_message(&message)?;
        let recipient = self.replica_observation(message.to)?;
        log_protocol_message_event(
            "ProtocolMessageDelivered",
            &message,
            self.pending_messages.len(),
            self.next_message_id,
        );
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
        message_label: &str,
    ) -> Result<ReplicatedScheduleObservationKind, ReplicatedSimulationError> {
        let index = self.pending_message_index(message_label)?;
        let message = self.pending_messages.remove(index);
        log_protocol_message_event(
            "ProtocolMessageDropped",
            &message,
            self.pending_messages.len(),
            self.next_message_id,
        );
        Ok(ReplicatedScheduleObservationKind::ProtocolMessageDropped {
            message,
            pending_messages: self.pending_messages.len(),
        })
    }

    fn apply_protocol_message(
        &mut self,
        message: &QueuedProtocolMessage,
    ) -> Result<(), ReplicatedSimulationError> {
        match &message.payload {
            ProtocolMessage::Opaque => Ok(()),
            ProtocolMessage::Prepare { entry } => {
                self.replica_entry_mut(message.to)?
                    .node
                    .as_mut()
                    .ok_or(ReplicatedSimulationError::ReplicaAlreadyCrashed(message.to))?
                    .append_prepared_entry(entry.clone())?;
                let ack_label = format!("{}-ack", message.label);
                self.queue_protocol_message_with_payload(
                    message.to,
                    message.from,
                    ack_label,
                    ProtocolMessage::PrepareAck {
                        view: entry.view,
                        lsn: entry.lsn,
                        from: message.to,
                    },
                )?;
                Ok(())
            }
            ProtocolMessage::PrepareAck { view, lsn, from } => {
                self.handle_prepare_ack(message.to, *view, *lsn, *from)
            }
            ProtocolMessage::Commit { view, commit_lsn } => {
                let node = self
                    .replica_entry_mut(message.to)?
                    .node
                    .as_mut()
                    .ok_or(ReplicatedSimulationError::ReplicaAlreadyCrashed(message.to))?;
                if node.metadata().current_view != *view {
                    return Err(ReplicaProtocolError::ViewMismatch {
                        expected: node.metadata().current_view,
                        found: *view,
                    }
                    .into());
                }
                node.commit_prepared_through(*commit_lsn)?;
                Ok(())
            }
        }
    }

    fn handle_prepare_ack(
        &mut self,
        primary: ReplicaId,
        view: u64,
        lsn: Lsn,
        from: ReplicaId,
    ) -> Result<(), ReplicatedSimulationError> {
        self.fail_primary_closed_if_quorum_lost()?;
        if self.configured_primary != Some(primary) {
            return Err(ReplicatedSimulationError::NotConfiguredPrimary {
                expected: self.configured_primary,
                found: primary,
            });
        }
        let mut commit_view = None;
        {
            let pending = self
                .pending_commits
                .get_mut(&lsn.get())
                .ok_or(ReplicatedSimulationError::PendingCommitNotFound(lsn))?;
            if pending.primary != primary {
                return Err(ReplicatedSimulationError::NotConfiguredPrimary {
                    expected: Some(pending.primary),
                    found: primary,
                });
            }
            if pending.entry.view != view {
                return Err(ReplicaProtocolError::ViewMismatch {
                    expected: pending.entry.view,
                    found: view,
                }
                .into());
            }
            pending.acked_replicas.insert(from.get());
            if !pending.committed && pending.acked_replicas.len() >= majority_quorum() {
                commit_view = Some(pending.entry.view);
            }
        }

        if let Some(commit_view) = commit_view {
            let result = self
                .replica_entry_mut(primary)?
                .node
                .as_mut()
                .ok_or(ReplicatedSimulationError::ReplicaAlreadyCrashed(primary))?
                .commit_prepared_through(lsn)?
                .expect("primary quorum commit must publish one result");
            self.published_results.insert(lsn.get(), result);
            if let Some(pending) = self.pending_commits.get_mut(&lsn.get()) {
                pending.committed = true;
            }
            let backup_ids = self
                .replicas
                .iter()
                .map(|replica| replica.identity.replica_id)
                .filter(|replica_id| *replica_id != primary)
                .collect::<Vec<_>>();
            for backup_id in backup_ids {
                let commit_label = format!("commit-{}-to-{}", lsn.get(), backup_id.get());
                self.queue_protocol_message_with_payload(
                    primary,
                    backup_id,
                    commit_label,
                    ProtocolMessage::Commit {
                        view: commit_view,
                        commit_lsn: lsn,
                    },
                )?;
            }
        }

        Ok(())
    }

    pub(crate) fn crash_replica(
        &mut self,
        replica_id: ReplicaId,
    ) -> Result<ReplicatedScheduleObservationKind, ReplicatedSimulationError> {
        let before = self.replica_observation(replica_id)?;
        if before.runtime_status == ReplicaRuntimeStatus::Crashed {
            info!(
                "ReplicaCrashRejected: replica_id={} runtime_status={:?} role={:?} current_view={:?} commit_lsn={:?} active_snapshot_lsn={:?}",
                before.replica_id.get(),
                before.runtime_status,
                before.role,
                before.current_view,
                before.commit_lsn,
                before.active_snapshot_lsn
            );
            return Err(ReplicatedSimulationError::ReplicaAlreadyCrashed(replica_id));
        }
        log_replica_lifecycle_event("ReplicaCrashStarting", &before);
        drop(self.replica_entry_mut(replica_id)?.node.take());
        self.fail_primary_closed_if_quorum_lost()?;
        let after = self.replica_observation(replica_id)?;
        log_replica_lifecycle_event("ReplicaCrashed", &after);
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
                info!(
                    "ReplicaRestartRejected: replica_id={} runtime_status={:?} role={:?} current_view={:?} commit_lsn={:?} active_snapshot_lsn={:?}",
                    before.replica_id.get(),
                    before.runtime_status,
                    before.role,
                    before.current_view,
                    before.commit_lsn,
                    before.active_snapshot_lsn
                );
                return Err(ReplicatedSimulationError::ReplicaAlreadyRunning(replica_id));
            }
            (replica.identity, replica.paths.clone())
        };
        info!(
            "ReplicaRecoveryStarting: replica_id={} shard_id={} metadata_path={} snapshot_path={} wal_path={} runtime_status={:?} role={:?} current_view={:?} commit_lsn={:?} active_snapshot_lsn={:?}",
            identity.replica_id.get(),
            identity.shard_id,
            paths.metadata_path.display(),
            paths.snapshot_path.display(),
            paths.wal_path.display(),
            before.runtime_status,
            before.role,
            before.current_view,
            before.commit_lsn,
            before.active_snapshot_lsn
        );
        let recovered = ReplicaNode::recover(core_config, engine_config, identity, paths)
            .map_err(ReplicatedSimulationError::RecoverReplica)?;
        self.replica_entry_mut(replica_id)?.node = Some(recovered);
        self.fail_primary_closed_if_quorum_lost()?;
        let after = self.replica_observation(replica_id)?;
        log_replica_lifecycle_event("ReplicaRestarted", &after);
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
        message_label: &str,
    ) -> Result<usize, ReplicatedSimulationError> {
        self.pending_messages
            .iter()
            .position(|message| message.label == message_label)
            .ok_or_else(|| ReplicatedSimulationError::MessageNotFound(message_label.to_owned()))
    }

    fn lookup_retry_result(
        &self,
        primary: ReplicaId,
        request_slot: Slot,
        payload: &[u8],
    ) -> Result<Option<SubmissionResult>, ReplicatedSimulationError> {
        let request = decode_client_request(payload).map_err(|error| {
            ReplicatedSimulationError::Protocol(ReplicaProtocolError::Submission(
                SubmissionError::InvalidRequest(error),
            ))
        })?;
        let node = self
            .replica(primary)?
            .ok_or(ReplicatedSimulationError::ReplicaAlreadyCrashed(primary))?;
        let Some(record) = node
            .engine()
            .expect("active primary must keep one live engine")
            .db()
            .operation(request.operation_id, request_slot)
        else {
            return Ok(None);
        };

        let outcome = if record.command_fingerprint == request.command.fingerprint() {
            CommandOutcome {
                result_code: record.result_code,
                reservation_id: record.result_reservation_id,
                deadline_slot: record.result_deadline_slot,
            }
        } else {
            CommandOutcome::new(ResultCode::OperationConflict)
        };
        Ok(Some(SubmissionResult {
            applied_lsn: record.applied_lsn,
            outcome,
            from_retry_cache: true,
        }))
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

    fn fail_primary_closed_if_quorum_lost(&mut self) -> Result<(), ReplicatedSimulationError> {
        let Some(primary) = self.configured_primary else {
            return Ok(());
        };

        let has_quorum = self.reachable_active_voters(primary)?.len() >= majority_quorum();
        if has_quorum {
            return Ok(());
        }

        if let Some(node) = self.replica_entry_mut(primary)?.node.as_mut() {
            if node.status() == ReplicaNodeStatus::Active {
                node.enter_view_uncertain()?;
            }
        }
        self.configured_primary = None;
        info!("ReplicaPrimaryLostQuorum: replica_id={}", primary.get());
        Ok(())
    }

    fn require_view_change_quorum(
        &self,
        new_primary: ReplicaId,
    ) -> Result<Vec<ReplicaId>, ReplicatedSimulationError> {
        let voters = self.reachable_active_voters(new_primary)?;
        if voters.len() < majority_quorum() {
            return Err(ReplicatedSimulationError::ViewChangeQuorumUnavailable {
                candidate: new_primary,
                reachable: voters.len(),
            });
        }
        Ok(voters)
    }

    fn view_change_target_commit_lsn(
        &self,
        voters: &[ReplicaId],
    ) -> Result<Option<Lsn>, ReplicatedSimulationError> {
        let mut target_commit_lsn = None;
        for replica_id in voters {
            let Some(node) = self.replica(*replica_id)? else {
                continue;
            };
            target_commit_lsn = target_commit_lsn.max(node.metadata().commit_lsn);
            // The harness keeps a fixed three-replica majority. If one reachable voter still holds
            // a prepared suffix, the crashed primary held the same entry locally when it queued
            // that prepare, which is enough to recover a majority-appended prefix during failover.
            target_commit_lsn = target_commit_lsn.max(node.highest_prepared_lsn());
        }
        Ok(target_commit_lsn)
    }

    fn record_view_change_votes(
        &mut self,
        voters: &[ReplicaId],
        new_primary: ReplicaId,
        new_view: u64,
    ) -> Result<(), ReplicatedSimulationError> {
        for voter in voters {
            let node = self
                .replica_entry_mut(*voter)?
                .node
                .as_mut()
                .ok_or(ReplicatedSimulationError::ReplicaAlreadyCrashed(*voter))?;
            node.record_durable_vote(new_view, new_primary)?;
            if *voter != new_primary {
                node.enter_view_uncertain()?;
            }
        }
        Ok(())
    }

    fn reconstruct_view_change_prefix(
        &mut self,
        new_primary: ReplicaId,
        voters: &[ReplicaId],
        target_commit_lsn: Option<Lsn>,
    ) -> Result<(), ReplicatedSimulationError> {
        let Some(target_commit_lsn) = target_commit_lsn else {
            self.replica_entry_mut(new_primary)?
                .node
                .as_mut()
                .ok_or(ReplicatedSimulationError::ReplicaAlreadyCrashed(
                    new_primary,
                ))?
                .enter_view_uncertain()?;
            return Ok(());
        };

        let next_needed_lsn = self
            .replica(new_primary)?
            .ok_or(ReplicatedSimulationError::ReplicaAlreadyCrashed(
                new_primary,
            ))?
            .metadata()
            .commit_lsn
            .and_then(|lsn| lsn.get().checked_add(1))
            .unwrap_or(1);
        for raw_lsn in next_needed_lsn..=target_commit_lsn.get() {
            let lsn = Lsn(raw_lsn);
            let already_present = self
                .replica(new_primary)?
                .ok_or(ReplicatedSimulationError::ReplicaAlreadyCrashed(
                    new_primary,
                ))?
                .prepared_entry(lsn)
                .is_some();
            if already_present {
                continue;
            }

            let entry = self.find_prepared_entry(voters, new_primary, lsn)?.ok_or(
                ReplicatedSimulationError::ViewChangeMissingPreparedEntry {
                    candidate: new_primary,
                    lsn,
                },
            )?;
            self.replica_entry_mut(new_primary)?
                .node
                .as_mut()
                .ok_or(ReplicatedSimulationError::ReplicaAlreadyCrashed(
                    new_primary,
                ))?
                .append_prepared_entry(entry)?;
        }

        let node = self.replica_entry_mut(new_primary)?.node.as_mut().ok_or(
            ReplicatedSimulationError::ReplicaAlreadyCrashed(new_primary),
        )?;
        node.enter_view_uncertain()?;
        node.reconstruct_committed_prefix_through(target_commit_lsn)?;
        Ok(())
    }

    fn install_view_change_roles(
        &mut self,
        voters: &[ReplicaId],
        new_primary: ReplicaId,
        new_view: u64,
    ) -> Result<(), ReplicatedSimulationError> {
        for voter in voters {
            let role = if *voter == new_primary {
                ReplicaRole::Primary
            } else {
                ReplicaRole::Backup
            };
            let node = self
                .replica_entry_mut(*voter)?
                .node
                .as_mut()
                .ok_or(ReplicatedSimulationError::ReplicaAlreadyCrashed(*voter))?;
            node.discard_uncommitted_suffix()?;
            node.configure_normal_role(new_view, role)?;
        }
        Ok(())
    }

    fn reachable_active_voters(
        &self,
        candidate: ReplicaId,
    ) -> Result<Vec<ReplicaId>, ReplicatedSimulationError> {
        if !self.replica_is_active(candidate)? {
            return Ok(Vec::new());
        }

        let mut voters = Vec::with_capacity(REPLICA_COUNT);
        for replica in &self.replicas {
            let replica_id = replica.identity.replica_id;
            if !self.replica_is_active(replica_id)? {
                continue;
            }
            if self.replicas_are_bidirectionally_connected(candidate, replica_id)? {
                voters.push(replica_id);
            }
        }
        Ok(voters)
    }

    fn replica_is_active(&self, replica_id: ReplicaId) -> Result<bool, ReplicatedSimulationError> {
        Ok(matches!(
            self.replica_observation(replica_id)?.runtime_status,
            ReplicaRuntimeStatus::Running(ReplicaNodeStatus::Active)
        ))
    }

    fn replicas_are_bidirectionally_connected(
        &self,
        left: ReplicaId,
        right: ReplicaId,
    ) -> Result<bool, ReplicatedSimulationError> {
        if left == right {
            return Ok(true);
        }

        Ok(self.connectivity_allows(
            ClusterEndpoint::Replica(left),
            ClusterEndpoint::Replica(right),
        )? && self.connectivity_allows(
            ClusterEndpoint::Replica(right),
            ClusterEndpoint::Replica(left),
        )?)
    }

    fn find_prepared_entry(
        &self,
        voters: &[ReplicaId],
        candidate: ReplicaId,
        lsn: Lsn,
    ) -> Result<Option<ReplicaPreparedEntry>, ReplicatedSimulationError> {
        for voter in voters {
            if *voter == candidate {
                continue;
            }
            let Some(node) = self.replica(*voter)? else {
                continue;
            };
            if let Some(entry) = node.prepared_entry(lsn) {
                return Ok(Some(entry.clone()));
            }
        }
        Ok(None)
    }

    fn capture_primary_catch_up_state(
        &self,
        primary: ReplicaId,
    ) -> Result<PrimaryCatchUpState, ReplicatedSimulationError> {
        let node = self
            .replica(primary)?
            .ok_or(ReplicatedSimulationError::ReplicaAlreadyCrashed(primary))?;
        if node.status() != ReplicaNodeStatus::Active
            || node.metadata().role != ReplicaRole::Primary
        {
            return Err(ReplicatedSimulationError::NotConfiguredPrimary {
                expected: self.configured_primary,
                found: primary,
            });
        }

        let retained_frames = read_primary_wal_frames(
            primary,
            node.engine()
                .expect("active primary must keep one live engine")
                .wal_path(),
            self.engine_config.max_command_bytes,
        )?;
        Ok(PrimaryCatchUpState {
            paths: node.paths().clone(),
            current_view: node.metadata().current_view,
            commit_lsn: node.metadata().commit_lsn,
            active_snapshot_lsn: node.metadata().active_snapshot_lsn,
            replay_floor: retained_replay_floor(
                &retained_frames,
                node.metadata().active_snapshot_lsn,
            ),
            retained_frames,
        })
    }

    fn validate_rejoin_request(
        &mut self,
        replica_id: ReplicaId,
        primary: ReplicaId,
    ) -> Result<(PrimaryCatchUpState, ReplicaMetadata), ReplicatedSimulationError> {
        self.fail_primary_closed_if_quorum_lost()?;
        if replica_id == primary {
            return Err(ReplicatedSimulationError::RejoinPrimary(replica_id));
        }
        if self.configured_primary != Some(primary) {
            return Err(ReplicatedSimulationError::NotConfiguredPrimary {
                expected: self.configured_primary,
                found: primary,
            });
        }

        let source = self.capture_primary_catch_up_state(primary)?;
        let target_metadata = self.load_replica_metadata(replica_id)?;
        let highest_known_view = metadata_highest_known_view(target_metadata);
        if highest_known_view > source.current_view {
            return Err(ReplicatedSimulationError::ReplicaViewAheadOfPrimary {
                replica_id,
                highest_known_view,
                primary_view: source.current_view,
            });
        }
        if target_metadata.commit_lsn.is_some_and(|commit_lsn| {
            source
                .commit_lsn
                .is_none_or(|primary_commit| commit_lsn.get() > primary_commit.get())
        }) {
            return Err(ReplicatedSimulationError::ReplicaCommitAheadOfPrimary {
                replica_id,
                target_commit_lsn: target_metadata.commit_lsn.expect("checked above"),
                primary_commit_lsn: source.commit_lsn,
            });
        }

        Ok((source, target_metadata))
    }

    fn prepare_rejoin_target(
        &mut self,
        replica_id: ReplicaId,
    ) -> Result<(ReplicaPaths, RecoveredWal), ReplicatedSimulationError> {
        if let Some(node) = self.replica_entry_mut(replica_id)?.node.as_mut() {
            if node.status() != ReplicaNodeStatus::Active {
                return Err(ReplicatedSimulationError::ReplicaFaulted(replica_id));
            }
            node.discard_uncommitted_suffix()?;
        }

        let paths = {
            let replica = self.replica_entry_mut(replica_id)?;
            drop(replica.node.take());
            replica.paths.clone()
        };
        let recovered_wal = recover_replica_wal(
            replica_id,
            &paths.wal_path,
            self.engine_config.max_command_bytes,
        )?;
        Ok((paths, recovered_wal))
    }

    fn write_rejoin_state(
        &self,
        replica_id: ReplicaId,
        primary: ReplicaId,
        source: &PrimaryCatchUpState,
        target_metadata: ReplicaMetadata,
        target_recovered_wal: &RecoveredWal,
        paths: &ReplicaPaths,
    ) -> Result<ReplicaRejoinMethod, ReplicatedSimulationError> {
        let method = if target_metadata.commit_lsn.map_or(0, Lsn::get) >= source.replay_floor {
            ReplicaRejoinMethod::SuffixOnly
        } else {
            ReplicaRejoinMethod::SnapshotTransfer
        };

        let wal_frames = match method {
            ReplicaRejoinMethod::SuffixOnly => suffix_rejoin_frames(
                target_metadata.commit_lsn,
                &target_recovered_wal.scan_result.frames,
                &source.retained_frames,
            ),
            ReplicaRejoinMethod::SnapshotTransfer => {
                let snapshot_lsn = source.active_snapshot_lsn.ok_or(
                    ReplicatedSimulationError::SnapshotTransferUnavailable {
                        primary,
                        replica: replica_id,
                        primary_replay_floor: source.replay_floor,
                    },
                )?;
                copy_snapshot(
                    primary,
                    &source.paths.snapshot_path,
                    &paths.snapshot_path,
                    snapshot_lsn,
                )?;
                source.retained_frames.clone()
            }
        };

        rewrite_wal(
            &paths.wal_path,
            self.engine_config.max_command_bytes,
            &wal_frames,
        )?;
        remove_if_exists(&prepare_log_path(&paths.metadata_path));

        let mut rejoin_metadata = target_metadata;
        rejoin_metadata.current_view = source.current_view;
        rejoin_metadata.role = ReplicaRole::Backup;
        rejoin_metadata.commit_lsn = source.commit_lsn;
        rejoin_metadata.active_snapshot_lsn = match method {
            ReplicaRejoinMethod::SuffixOnly => target_metadata.active_snapshot_lsn,
            ReplicaRejoinMethod::SnapshotTransfer => source.active_snapshot_lsn,
        };
        rejoin_metadata.last_normal_view = Some(source.current_view);
        rejoin_metadata.durable_vote = None;
        ReplicaMetadataFile::new(&paths.metadata_path).write_metadata(&rejoin_metadata)?;

        Ok(method)
    }

    fn restart_rejoined_replica(
        &mut self,
        replica_id: ReplicaId,
        paths: &ReplicaPaths,
        current_view: u64,
    ) -> Result<(), ReplicatedSimulationError> {
        let identity = self.replica_entry(replica_id)?.identity;
        let recovered = ReplicaNode::recover(
            self.core_config.clone(),
            self.engine_config,
            identity,
            paths.clone(),
        )
        .map_err(ReplicatedSimulationError::RecoverReplica)?;
        let is_faulted = recovered.status() != ReplicaNodeStatus::Active;
        self.replica_entry_mut(replica_id)?.node = Some(recovered);
        if is_faulted {
            return Err(ReplicatedSimulationError::ReplicaFaulted(replica_id));
        }
        self.replica_entry_mut(replica_id)?
            .node
            .as_mut()
            .expect("recovered replica must be present")
            .configure_normal_role(current_view, ReplicaRole::Backup)?;
        self.prune_replica_protocol_state(replica_id);
        Ok(())
    }

    fn load_replica_metadata(
        &self,
        replica_id: ReplicaId,
    ) -> Result<ReplicaMetadata, ReplicatedSimulationError> {
        match self.replica(replica_id)? {
            Some(node)
                if node.status() == ReplicaNodeStatus::Active
                    && node.metadata().role != ReplicaRole::Faulted =>
            {
                Ok(*node.metadata())
            }
            Some(_) | None => Err(ReplicatedSimulationError::ReplicaFaulted(replica_id)),
        }
    }

    fn prune_replica_protocol_state(&mut self, replica_id: ReplicaId) {
        let pending_messages_before = self.pending_messages.len();
        self.pending_messages
            .retain(|message| message.from != replica_id && message.to != replica_id);
        for pending in self.pending_commits.values_mut() {
            pending.acked_replicas.remove(&replica_id.get());
        }
        info!(
            "ReplicaProtocolStatePruned: replica_id={} removed_messages={} pending_messages={} pending_commits={}",
            replica_id.get(),
            pending_messages_before.saturating_sub(self.pending_messages.len()),
            self.pending_messages.len(),
            self.pending_commits.len()
        );
    }
}

impl Drop for ReplicatedSimulationHarness {
    fn drop(&mut self) {
        for replica in &mut self.replicas {
            drop(replica.node.take());
            remove_if_exists(&replica.paths.wal_path);
            remove_if_exists(&replica.paths.snapshot_path);
            remove_if_exists(&replica.paths.metadata_path);
            remove_if_exists(&prepare_log_path(&replica.paths.metadata_path));
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

const fn majority_quorum() -> usize {
    (REPLICA_COUNT / 2) + 1
}

fn log_protocol_message_event(
    event: &str,
    message: &QueuedProtocolMessage,
    pending_messages: usize,
    next_message_id: u64,
) {
    debug!(
        "{event}: message_id={} label={} from={} to={} pending_messages={} next_message_id={}",
        message.message_id,
        message.label,
        message.from.get(),
        message.to.get(),
        pending_messages,
        next_message_id
    );
}

fn log_replica_lifecycle_event(event: &str, observation: &ReplicaObservation) {
    info!(
        "{event}: replica_id={} runtime_status={:?} role={:?} current_view={:?} commit_lsn={:?} active_snapshot_lsn={:?}",
        observation.replica_id.get(),
        observation.runtime_status,
        observation.role,
        observation.current_view,
        observation.commit_lsn,
        observation.active_snapshot_lsn
    );
}

fn protocol_message_view(message: &ProtocolMessage) -> Option<u64> {
    match message {
        ProtocolMessage::Opaque => None,
        ProtocolMessage::Prepare { entry } => Some(entry.view),
        ProtocolMessage::PrepareAck { view, .. } | ProtocolMessage::Commit { view, .. } => {
            Some(*view)
        }
    }
}

fn read_primary_wal_frames(
    replica_id: ReplicaId,
    path: &Path,
    max_command_bytes: usize,
) -> Result<Vec<Frame>, ReplicatedSimulationError> {
    let wal = WalFile::open(path, max_command_bytes)?;
    let recovered = wal.recover()?;
    match recovered.scan_result.stop_reason {
        ScanStopReason::CleanEof => Ok(recovered.scan_result.frames),
        stop_reason => Err(ReplicatedSimulationError::ReplicaWalNotClean {
            replica_id,
            stop_reason,
        }),
    }
}

fn recover_replica_wal(
    replica_id: ReplicaId,
    path: &Path,
    max_command_bytes: usize,
) -> Result<RecoveredWal, ReplicatedSimulationError> {
    let wal = WalFile::open(path, max_command_bytes)?;
    let recovered = wal.recover()?;
    if matches!(
        recovered.scan_result.stop_reason,
        ScanStopReason::Corruption { .. }
    ) {
        return Err(ReplicatedSimulationError::ReplicaWalNotClean {
            replica_id,
            stop_reason: recovered.scan_result.stop_reason,
        });
    }
    Ok(recovered)
}

fn retained_replay_floor(retained_frames: &[Frame], active_snapshot_lsn: Option<Lsn>) -> u64 {
    retained_frames
        .iter()
        .filter(|frame| !matches!(frame.record_type, RecordType::SnapshotMarker))
        .map(|frame| frame.lsn.get().saturating_sub(1))
        .min()
        .unwrap_or_else(|| active_snapshot_lsn.map_or(0, Lsn::get))
}

fn suffix_rejoin_frames(
    target_commit_lsn: Option<Lsn>,
    target_frames: &[Frame],
    primary_frames: &[Frame],
) -> Vec<Frame> {
    let target_commit = target_commit_lsn.map_or(0, Lsn::get);
    let mut frames = target_frames
        .iter()
        .filter(|frame| frame.lsn.get() <= target_commit)
        .cloned()
        .collect::<Vec<_>>();
    frames.extend(
        primary_frames
            .iter()
            .filter(|frame| {
                frame.lsn.get() > target_commit
                    && !matches!(frame.record_type, RecordType::SnapshotMarker)
            })
            .cloned(),
    );
    frames
}

fn rewrite_wal(
    path: &Path,
    max_command_bytes: usize,
    frames: &[Frame],
) -> Result<(), ReplicatedSimulationError> {
    let mut wal = WalFile::open(path, max_command_bytes)?;
    wal.replace_with_frames(frames)?;
    Ok(())
}

fn copy_snapshot(
    primary: ReplicaId,
    source_path: &Path,
    target_path: &Path,
    snapshot_lsn: Lsn,
) -> Result<(), ReplicatedSimulationError> {
    let snapshot = SnapshotFile::new(source_path).load_snapshot()?.ok_or(
        ReplicatedSimulationError::PrimarySnapshotMissing {
            primary,
            snapshot_lsn,
        },
    )?;
    SnapshotFile::new(target_path).write_snapshot(&snapshot)?;
    Ok(())
}

fn metadata_highest_known_view(metadata: ReplicaMetadata) -> u64 {
    metadata.durable_vote.map_or(metadata.current_view, |vote| {
        metadata.current_view.max(vote.view)
    })
}

fn remove_if_exists(path: &Path) {
    match fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => warn!(
            "failed to remove replicated simulation artifact {}: {error}",
            path.display()
        ),
    }
}

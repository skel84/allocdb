use allocdb_core::command::{ClientRequest, Command as AllocCommand};
use allocdb_core::ids::{HolderId, Lsn, OperationId, ReservationId, ResourceId, Slot};
use allocdb_core::result::ResultCode;
use allocdb_node::jepsen::{
    JepsenAmbiguousOutcome, JepsenCommittedWrite, JepsenDefiniteFailure, JepsenEventOutcome,
    JepsenExpiredReservation, JepsenOperation, JepsenOperationKind, JepsenReadState,
    JepsenReadTarget, JepsenReservationState, JepsenSuccessfulRead, JepsenWriteResult,
};
use allocdb_node::local_cluster::LocalClusterReplicaConfig;
use allocdb_node::{
    ApiRequest, ApiResponse, LeaseRequest, LeaseResponse, LeaseViewState, ResourceRequest,
    ResourceResponse, ResourceViewState, SubmissionFailure, SubmissionFailureCode, SubmitRequest,
    SubmitResponse, TickExpirationsRequest, TickExpirationsResponse, decode_response,
};

use crate::ExternalTestbed;
use crate::remote::send_remote_api_request;
use crate::support::{HistoryBuilder, RunExecutionContext};
use crate::tracker::RequestNamespace;

pub(super) const MAX_EXPIRATION_RECOVERY_DRAIN_TICKS: u64 = 16;

pub(super) enum RemoteApiOutcome {
    Api(ApiResponse),
    Text(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ResourceReadObservation {
    Available,
    Held {
        state: ResourceViewState,
        current_reservation_id: Option<ReservationId>,
        version: u64,
    },
    NotFound,
    FenceNotApplied,
    EngineHalted,
    NotPrimary,
}

pub(super) struct ReserveCommit {
    pub(super) applied_lsn: Lsn,
    pub(super) reservation_id: ReservationId,
    pub(super) lease_epoch: u64,
}

#[derive(Clone, Copy)]
pub(super) struct ReserveEventSpec {
    pub(super) operation_id: OperationId,
    pub(super) resource_id: ResourceId,
    pub(super) holder_id: HolderId,
    pub(super) request_slot: Slot,
    pub(super) ttl_slots: u64,
}

#[derive(Clone, Copy)]
pub(super) struct ReserveBundleEventSpec<'a> {
    pub(super) operation_id: OperationId,
    pub(super) resource_ids: &'a [ResourceId],
    pub(super) holder_id: HolderId,
    pub(super) request_slot: Slot,
    pub(super) ttl_slots: u64,
}

#[derive(Clone, Copy)]
pub(super) struct HolderLeaseEventSpec<'a> {
    pub(super) kind: JepsenOperationKind,
    pub(super) operation_id: OperationId,
    pub(super) reservation_id: ReservationId,
    pub(super) resource_id: ResourceId,
    pub(super) resource_ids: &'a [ResourceId],
    pub(super) holder_id: HolderId,
    pub(super) lease_epoch: u64,
    pub(super) request_slot: Slot,
}

#[derive(Clone, Copy)]
pub(super) struct AdminLeaseEventSpec {
    pub(super) kind: JepsenOperationKind,
    pub(super) operation_id: OperationId,
    pub(super) reservation_id: ReservationId,
    pub(super) request_slot: Slot,
}

#[derive(Clone, Copy)]
pub(super) struct ExpirationDrainPlan<'a> {
    pub(super) resource_id: ResourceId,
    pub(super) expired: &'a [JepsenExpiredReservation],
    pub(super) required_lsn: Lsn,
    pub(super) operation_id_base: u128,
    pub(super) slot_base: u64,
}

pub(super) fn create_qemu_resource<T: ExternalTestbed>(
    layout: &T,
    primary: &LocalClusterReplicaConfig,
    context: &RunExecutionContext,
    resource_id: ResourceId,
    operation_id: OperationId,
) -> Result<(), String> {
    let request = ApiRequest::Submit(SubmitRequest::from_client_request(
        context.slot(1),
        context.client_request(operation_id, AllocCommand::CreateResource { resource_id }),
    ));
    match send_replica_api_request(layout, primary, &request)? {
        RemoteApiOutcome::Api(ApiResponse::Submit(SubmitResponse::Committed(response)))
            if response.result_code == ResultCode::Ok =>
        {
            Ok(())
        }
        RemoteApiOutcome::Api(other) => Err(format!(
            "resource setup for {} returned unexpected response {other:?}",
            resource_id.get()
        )),
        RemoteApiOutcome::Text(text) => Err(format!(
            "resource setup for {} returned undecodable response {text}",
            resource_id.get()
        )),
    }
}

pub(super) fn reserve_event<T: ExternalTestbed>(
    layout: &T,
    replica: &LocalClusterReplicaConfig,
    context: &RunExecutionContext,
    spec: ReserveEventSpec,
) -> Result<(JepsenOperation, JepsenEventOutcome, Option<ReserveCommit>), String> {
    let operation = JepsenOperation {
        kind: JepsenOperationKind::Reserve,
        operation_id: Some(spec.operation_id.get()),
        resource_id: Some(spec.resource_id),
        resource_ids: Vec::new(),
        reservation_id: None,
        holder_id: Some(spec.holder_id.get()),
        lease_epoch: None,
        required_lsn: None,
        request_slot: Some(spec.request_slot),
        ttl_slots: Some(spec.ttl_slots),
    };
    let request = ApiRequest::Submit(SubmitRequest::from_client_request(
        context.slot(spec.request_slot.get()),
        context.client_request(
            spec.operation_id,
            AllocCommand::Reserve {
                resource_id: spec.resource_id,
                holder_id: spec.holder_id,
                ttl_slots: spec.ttl_slots,
            },
        ),
    ));
    match send_replica_api_request(layout, replica, &request)? {
        RemoteApiOutcome::Api(ApiResponse::Submit(response)) => {
            map_reserve_submit_response(operation, spec.resource_id, spec.holder_id, response)
        }
        RemoteApiOutcome::Api(other) => {
            Err(format!("reserve returned unexpected response {other:?}"))
        }
        RemoteApiOutcome::Text(text) if response_text_is_not_primary(&text) => Ok((
            operation,
            JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::NotPrimary),
            None,
        )),
        RemoteApiOutcome::Text(text) => {
            Err(format!("reserve returned undecodable response {text}"))
        }
    }
}

pub(super) fn reserve_bundle_event<T: ExternalTestbed>(
    layout: &T,
    replica: &LocalClusterReplicaConfig,
    context: &RunExecutionContext,
    spec: ReserveBundleEventSpec<'_>,
) -> Result<(JepsenOperation, JepsenEventOutcome, Option<ReserveCommit>), String> {
    let anchor_resource_id = *spec
        .resource_ids
        .first()
        .ok_or_else(|| String::from("reserve_bundle requires at least one resource"))?;
    let operation = JepsenOperation {
        kind: JepsenOperationKind::ReserveBundle,
        operation_id: Some(spec.operation_id.get()),
        resource_id: Some(anchor_resource_id),
        resource_ids: spec.resource_ids.to_vec(),
        reservation_id: None,
        holder_id: Some(spec.holder_id.get()),
        lease_epoch: None,
        required_lsn: None,
        request_slot: Some(spec.request_slot),
        ttl_slots: Some(spec.ttl_slots),
    };
    let request = ApiRequest::Submit(SubmitRequest::from_client_request(
        context.slot(spec.request_slot.get()),
        context.client_request(
            spec.operation_id,
            AllocCommand::ReserveBundle {
                resource_ids: spec.resource_ids.to_vec(),
                holder_id: spec.holder_id,
                ttl_slots: spec.ttl_slots,
            },
        ),
    ));
    match send_replica_api_request(layout, replica, &request)? {
        RemoteApiOutcome::Api(ApiResponse::Submit(response)) => {
            map_reserve_submit_response(operation, anchor_resource_id, spec.holder_id, response)
        }
        RemoteApiOutcome::Api(other) => Err(format!(
            "reserve_bundle returned unexpected response {other:?}"
        )),
        RemoteApiOutcome::Text(text) if response_text_is_not_primary(&text) => Ok((
            operation,
            JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::NotPrimary),
            None,
        )),
        RemoteApiOutcome::Text(text) => Err(format!(
            "reserve_bundle returned undecodable response {text}"
        )),
    }
}

pub(super) fn tick_expirations_event<T: ExternalTestbed>(
    layout: &T,
    replica: &LocalClusterReplicaConfig,
    context: &RunExecutionContext,
    operation_id: OperationId,
    current_wall_clock_slot: Slot,
    expired: &[JepsenExpiredReservation],
) -> Result<(JepsenOperation, JepsenEventOutcome, Option<Lsn>), String> {
    let current_wall_clock_slot = context.slot(current_wall_clock_slot.get());
    let operation = JepsenOperation {
        kind: JepsenOperationKind::TickExpirations,
        operation_id: Some(operation_id.get()),
        resource_id: None,
        resource_ids: Vec::new(),
        reservation_id: None,
        holder_id: None,
        lease_epoch: None,
        required_lsn: None,
        request_slot: Some(current_wall_clock_slot),
        ttl_slots: None,
    };
    let request = ApiRequest::TickExpirations(TickExpirationsRequest {
        current_wall_clock_slot,
    });
    match send_replica_api_request(layout, replica, &request)? {
        RemoteApiOutcome::Api(ApiResponse::TickExpirations(response)) => match response {
            TickExpirationsResponse::Applied(response) => {
                let applied_lsn = response.last_applied_lsn.ok_or_else(|| {
                    String::from("tick_expirations applied without one committed lsn")
                })?;
                let expired = expired
                    .iter()
                    .map(|entry| JepsenExpiredReservation {
                        resource_id: entry.resource_id,
                        holder_id: entry.holder_id,
                        reservation_id: entry.reservation_id,
                        released_lsn: Some(applied_lsn),
                    })
                    .collect();
                Ok((
                    operation,
                    JepsenEventOutcome::CommittedWrite(JepsenCommittedWrite {
                        applied_lsn,
                        result: JepsenWriteResult::TickExpired { expired },
                    }),
                    Some(applied_lsn),
                ))
            }
            TickExpirationsResponse::Rejected(failure) => {
                Ok((operation, outcome_from_submission_failure(&failure), None))
            }
        },
        RemoteApiOutcome::Api(other) => Err(format!(
            "tick_expirations returned unexpected response {other:?}"
        )),
        RemoteApiOutcome::Text(text) if response_text_is_not_primary(&text) => Ok((
            operation,
            JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::NotPrimary),
            None,
        )),
        RemoteApiOutcome::Text(text) => Err(format!(
            "tick_expirations returned undecodable response {text}"
        )),
    }
}

pub(super) fn reservation_read_event<T: ExternalTestbed>(
    layout: &T,
    replica: &LocalClusterReplicaConfig,
    context: &RunExecutionContext,
    reservation_id: ReservationId,
    current_slot: Slot,
    required_lsn: Option<Lsn>,
) -> Result<(JepsenOperation, JepsenEventOutcome), String> {
    let current_slot = context.slot(current_slot.get());
    let operation = JepsenOperation {
        kind: JepsenOperationKind::GetReservation,
        operation_id: None,
        resource_id: None,
        resource_ids: Vec::new(),
        reservation_id: Some(reservation_id.get()),
        holder_id: None,
        lease_epoch: None,
        required_lsn,
        request_slot: Some(current_slot),
        ttl_slots: None,
    };
    let request = ApiRequest::GetLease(LeaseRequest {
        lease_id: reservation_id,
        current_slot,
        required_lsn,
    });
    match send_replica_api_request(layout, replica, &request)? {
        RemoteApiOutcome::Api(ApiResponse::GetLease(LeaseResponse::Found(reservation))) => Ok((
            operation,
            JepsenEventOutcome::SuccessfulRead(JepsenSuccessfulRead {
                target: JepsenReadTarget::Reservation,
                served_by: replica.replica_id,
                served_role: replica.role,
                observed_lsn: required_lsn,
                state: JepsenReadState::Reservation(map_reservation_state(&reservation)),
            }),
        )),
        RemoteApiOutcome::Api(ApiResponse::GetLease(LeaseResponse::NotFound)) => Ok((
            operation,
            JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::NotFound),
        )),
        RemoteApiOutcome::Api(ApiResponse::GetLease(LeaseResponse::Retired)) => Ok((
            operation,
            JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::Retired),
        )),
        RemoteApiOutcome::Api(ApiResponse::GetLease(LeaseResponse::FenceNotApplied { .. })) => {
            Ok((
                operation,
                JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::FenceNotApplied),
            ))
        }
        RemoteApiOutcome::Api(ApiResponse::GetLease(LeaseResponse::EngineHalted)) => Ok((
            operation,
            JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::EngineHalted),
        )),
        RemoteApiOutcome::Api(other) => Err(format!(
            "reservation read for {} returned unexpected response {other:?}",
            reservation_id.get()
        )),
        RemoteApiOutcome::Text(text) if response_text_is_not_primary(&text) => Ok((
            operation,
            JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::NotPrimary),
        )),
        RemoteApiOutcome::Text(text) => Err(format!(
            "reservation read for {} returned undecodable response {text}",
            reservation_id.get()
        )),
    }
}

pub(super) fn record_resource_available_after_expiration<T: ExternalTestbed>(
    layout: &T,
    replica: &LocalClusterReplicaConfig,
    context: &RunExecutionContext,
    history: &mut HistoryBuilder,
    plan: ExpirationDrainPlan<'_>,
) -> Result<Lsn, String> {
    let settled_tick_lsn =
        drain_expiration_until_resource_available(layout, replica, context, history, plan)?;
    let resource_read =
        resource_available_event(layout, replica, plan.resource_id, Some(settled_tick_lsn))?;
    history.push(
        primary_process_name(replica),
        resource_read.0,
        resource_read.1,
    );
    Ok(settled_tick_lsn)
}

pub(super) fn classify_resource_read_outcome(
    resource_id: ResourceId,
    outcome: RemoteApiOutcome,
) -> Result<ResourceReadObservation, String> {
    match outcome {
        RemoteApiOutcome::Api(ApiResponse::GetResource(ResourceResponse::Found(resource))) => {
            if resource.resource_id != resource_id {
                return Err(format!(
                    "resource read for {} returned mismatched resource {}",
                    resource_id.get(),
                    resource.resource_id.get()
                ));
            }
            if matches!(resource.state, ResourceViewState::Available) {
                Ok(ResourceReadObservation::Available)
            } else {
                Ok(ResourceReadObservation::Held {
                    state: resource.state,
                    current_reservation_id: resource.current_lease_id,
                    version: resource.version,
                })
            }
        }
        RemoteApiOutcome::Api(ApiResponse::GetResource(ResourceResponse::NotFound)) => {
            Ok(ResourceReadObservation::NotFound)
        }
        RemoteApiOutcome::Api(ApiResponse::GetResource(ResourceResponse::FenceNotApplied {
            ..
        })) => Ok(ResourceReadObservation::FenceNotApplied),
        RemoteApiOutcome::Api(ApiResponse::GetResource(ResourceResponse::EngineHalted)) => {
            Ok(ResourceReadObservation::EngineHalted)
        }
        RemoteApiOutcome::Api(other) => Err(format!(
            "resource read for {} returned unexpected response {other:?}",
            resource_id.get()
        )),
        RemoteApiOutcome::Text(text) if response_text_is_not_primary(&text) => {
            Ok(ResourceReadObservation::NotPrimary)
        }
        RemoteApiOutcome::Text(text) => Err(format!(
            "resource read for {} returned undecodable response {text}",
            resource_id.get()
        )),
    }
}

pub(super) fn map_reservation_state(
    reservation: &allocdb_node::LeaseView,
) -> JepsenReservationState {
    match reservation.state {
        LeaseViewState::Reserved => JepsenReservationState::Active {
            resource_id: reservation.member_resource_ids[0],
            holder_id: reservation.holder_id.get(),
            expires_at_slot: reservation
                .deadline_slot
                .expect("live lease should expose deadline"),
            confirmed: false,
        },
        LeaseViewState::Active | LeaseViewState::Revoking => JepsenReservationState::Active {
            resource_id: reservation.member_resource_ids[0],
            holder_id: reservation.holder_id.get(),
            expires_at_slot: reservation
                .deadline_slot
                .expect("live lease should expose deadline"),
            confirmed: true,
        },
        LeaseViewState::Released | LeaseViewState::Expired | LeaseViewState::Revoked => {
            JepsenReservationState::Released {
                resource_id: reservation.member_resource_ids[0],
                holder_id: reservation.holder_id.get(),
                released_lsn: reservation.released_lsn,
            }
        }
    }
}

pub(super) fn outcome_from_submission_failure(failure: &SubmissionFailure) -> JepsenEventOutcome {
    if failure.category == allocdb_node::SubmissionErrorCategory::Indefinite {
        return JepsenEventOutcome::Ambiguous(JepsenAmbiguousOutcome::IndefiniteWrite);
    }

    let failure = match failure.code {
        SubmissionFailureCode::Overloaded { .. } => JepsenDefiniteFailure::Busy,
        SubmissionFailureCode::EngineHalted | SubmissionFailureCode::LsnExhausted { .. } => {
            JepsenDefiniteFailure::EngineHalted
        }
        SubmissionFailureCode::InvalidRequest(_)
        | SubmissionFailureCode::SlotOverflow { .. }
        | SubmissionFailureCode::CommandTooLarge { .. } => JepsenDefiniteFailure::InvalidRequest,
        SubmissionFailureCode::StorageFailure => JepsenDefiniteFailure::EngineHalted,
    };
    JepsenEventOutcome::DefiniteFailure(failure)
}

fn definite_failure_from_result_code(result_code: ResultCode) -> Option<JepsenDefiniteFailure> {
    match result_code {
        ResultCode::ResourceBusy => Some(JepsenDefiniteFailure::Busy),
        ResultCode::OperationConflict
        | ResultCode::InvalidState
        | ResultCode::HolderMismatch
        | ResultCode::BundleTooLarge
        | ResultCode::TtlOutOfRange
        | ResultCode::ResourceTableFull
        | ResultCode::ReservationTableFull
        | ResultCode::ReservationMemberTableFull
        | ResultCode::ExpirationIndexFull
        | ResultCode::OperationTableFull
        | ResultCode::SlotOverflow
        | ResultCode::AlreadyExists
        | ResultCode::Noop => Some(JepsenDefiniteFailure::Conflict),
        ResultCode::StaleEpoch => Some(JepsenDefiniteFailure::StaleEpoch),
        ResultCode::ResourceNotFound | ResultCode::ReservationNotFound => {
            Some(JepsenDefiniteFailure::NotFound)
        }
        ResultCode::ReservationRetired => Some(JepsenDefiniteFailure::Retired),
        ResultCode::Ok => None,
    }
}

pub(super) fn primary_process_name(replica: &LocalClusterReplicaConfig) -> String {
    format!("primary-{}", replica.replica_id.get())
}

pub(super) fn backup_process_name(replica: &LocalClusterReplicaConfig) -> String {
    format!("backup-{}", replica.replica_id.get())
}

pub(super) fn probe_create_request(
    resource_id: u128,
    namespace: RequestNamespace,
) -> ClientRequest {
    namespace.client_request(
        OperationId(resource_id),
        AllocCommand::CreateResource {
            resource_id: ResourceId(resource_id),
        },
    )
}

fn map_reserve_submit_response(
    operation: JepsenOperation,
    resource_id: ResourceId,
    holder_id: HolderId,
    response: SubmitResponse,
) -> Result<(JepsenOperation, JepsenEventOutcome, Option<ReserveCommit>), String> {
    match response {
        SubmitResponse::Committed(response) => match response.result_code {
            ResultCode::Ok => {
                let reservation_id = response
                    .lease_id
                    .ok_or_else(|| String::from("reserve commit missing reservation_id"))?;
                let lease_epoch = response
                    .lease_epoch
                    .ok_or_else(|| String::from("reserve commit missing lease_epoch"))?;
                let expires_at_slot = response
                    .deadline_slot
                    .ok_or_else(|| String::from("reserve commit missing deadline_slot"))?;
                Ok((
                    operation,
                    JepsenEventOutcome::CommittedWrite(JepsenCommittedWrite {
                        applied_lsn: response.applied_lsn,
                        result: JepsenWriteResult::Reserved {
                            resource_id,
                            lease_epoch,
                            holder_id: holder_id.get(),
                            reservation_id: reservation_id.get(),
                            expires_at_slot,
                        },
                    }),
                    Some(ReserveCommit {
                        applied_lsn: response.applied_lsn,
                        reservation_id,
                        lease_epoch,
                    }),
                ))
            }
            other => definite_failure_from_result_code(other)
                .map(|failure| {
                    (
                        operation,
                        JepsenEventOutcome::DefiniteFailure(failure),
                        None,
                    )
                })
                .ok_or_else(|| format!("unexpected reserve result code {other:?}")),
        },
        SubmitResponse::Rejected(failure) => {
            Ok((operation, outcome_from_submission_failure(&failure), None))
        }
    }
}

pub(super) fn holder_lease_event<T: ExternalTestbed>(
    layout: &T,
    replica: &LocalClusterReplicaConfig,
    context: &RunExecutionContext,
    spec: HolderLeaseEventSpec<'_>,
) -> Result<(JepsenOperation, JepsenEventOutcome), String> {
    let operation = JepsenOperation {
        kind: spec.kind,
        operation_id: Some(spec.operation_id.get()),
        resource_id: Some(spec.resource_id),
        resource_ids: spec.resource_ids.to_vec(),
        reservation_id: Some(spec.reservation_id.get()),
        holder_id: Some(spec.holder_id.get()),
        lease_epoch: Some(spec.lease_epoch),
        required_lsn: None,
        request_slot: Some(spec.request_slot),
        ttl_slots: None,
    };
    let command = match spec.kind {
        JepsenOperationKind::Confirm => AllocCommand::Confirm {
            reservation_id: spec.reservation_id,
            holder_id: spec.holder_id,
            lease_epoch: spec.lease_epoch,
        },
        JepsenOperationKind::Release => AllocCommand::Release {
            reservation_id: spec.reservation_id,
            holder_id: spec.holder_id,
            lease_epoch: spec.lease_epoch,
        },
        _ => {
            return Err(format!(
                "holder_lease_event does not support operation {:?}",
                spec.kind
            ));
        }
    };
    let request = ApiRequest::Submit(SubmitRequest::from_client_request(
        context.slot(spec.request_slot.get()),
        context.client_request(spec.operation_id, command),
    ));
    match send_replica_api_request(layout, replica, &request)? {
        RemoteApiOutcome::Api(ApiResponse::Submit(response)) => {
            map_holder_submit_response(operation, response)
        }
        RemoteApiOutcome::Api(other) => Err(format!(
            "holder lease operation returned unexpected response {other:?}"
        )),
        RemoteApiOutcome::Text(text) if response_text_is_not_primary(&text) => Ok((
            operation,
            JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::NotPrimary),
        )),
        RemoteApiOutcome::Text(text) => Err(format!(
            "holder lease operation returned undecodable response {text}"
        )),
    }
}

pub(super) fn admin_lease_event<T: ExternalTestbed>(
    layout: &T,
    replica: &LocalClusterReplicaConfig,
    context: &RunExecutionContext,
    spec: AdminLeaseEventSpec,
) -> Result<(JepsenOperation, JepsenEventOutcome), String> {
    let operation = JepsenOperation {
        kind: spec.kind,
        operation_id: Some(spec.operation_id.get()),
        resource_id: None,
        resource_ids: Vec::new(),
        reservation_id: Some(spec.reservation_id.get()),
        holder_id: None,
        lease_epoch: None,
        required_lsn: None,
        request_slot: Some(spec.request_slot),
        ttl_slots: None,
    };
    let command = match spec.kind {
        JepsenOperationKind::Revoke => AllocCommand::Revoke {
            reservation_id: spec.reservation_id,
        },
        JepsenOperationKind::Reclaim => AllocCommand::Reclaim {
            reservation_id: spec.reservation_id,
        },
        _ => {
            return Err(format!(
                "admin_lease_event does not support operation {:?}",
                spec.kind
            ));
        }
    };
    let request = ApiRequest::Submit(SubmitRequest::from_client_request(
        context.slot(spec.request_slot.get()),
        context.client_request(spec.operation_id, command),
    ));
    match send_replica_api_request(layout, replica, &request)? {
        RemoteApiOutcome::Api(ApiResponse::Submit(response)) => {
            map_admin_submit_response(operation, response)
        }
        RemoteApiOutcome::Api(other) => Err(format!(
            "admin lease operation returned unexpected response {other:?}"
        )),
        RemoteApiOutcome::Text(text) if response_text_is_not_primary(&text) => Ok((
            operation,
            JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::NotPrimary),
        )),
        RemoteApiOutcome::Text(text) => Err(format!(
            "admin lease operation returned undecodable response {text}"
        )),
    }
}

fn map_holder_submit_response(
    operation: JepsenOperation,
    response: SubmitResponse,
) -> Result<(JepsenOperation, JepsenEventOutcome), String> {
    match response {
        SubmitResponse::Committed(response) => match response.result_code {
            ResultCode::Ok => {
                let reservation_id = ReservationId(
                    operation
                        .reservation_id
                        .ok_or_else(|| String::from("holder operation missing reservation_id"))?,
                );
                let holder_id = operation
                    .holder_id
                    .ok_or_else(|| String::from("holder operation missing holder_id"))?;
                let resource_id = operation
                    .resource_id
                    .ok_or_else(|| String::from("holder operation missing resource_id"))?;
                let result = match operation.kind {
                    JepsenOperationKind::Confirm => JepsenWriteResult::Confirmed {
                        resource_id,
                        lease_epoch: operation
                            .lease_epoch
                            .ok_or_else(|| String::from("confirm missing lease_epoch"))?,
                        holder_id,
                        reservation_id: reservation_id.get(),
                    },
                    JepsenOperationKind::Release => JepsenWriteResult::Released {
                        resource_id,
                        holder_id,
                        reservation_id: reservation_id.get(),
                        released_lsn: Some(response.applied_lsn),
                    },
                    other => {
                        return Err(format!(
                            "holder operation mapper does not support result for {other:?}"
                        ));
                    }
                };
                Ok((
                    operation,
                    JepsenEventOutcome::CommittedWrite(JepsenCommittedWrite {
                        applied_lsn: response.applied_lsn,
                        result,
                    }),
                ))
            }
            other => definite_failure_from_result_code(other)
                .map(|failure| (operation, JepsenEventOutcome::DefiniteFailure(failure)))
                .ok_or_else(|| format!("unexpected holder result code {other:?}")),
        },
        SubmitResponse::Rejected(failure) => {
            Ok((operation, outcome_from_submission_failure(&failure)))
        }
    }
}

fn map_admin_submit_response(
    operation: JepsenOperation,
    response: SubmitResponse,
) -> Result<(JepsenOperation, JepsenEventOutcome), String> {
    match response {
        SubmitResponse::Committed(response) => match response.result_code {
            ResultCode::Ok => {
                let reservation_id = operation
                    .reservation_id
                    .ok_or_else(|| String::from("admin operation missing reservation_id"))?;
                let result = match operation.kind {
                    JepsenOperationKind::Revoke => JepsenWriteResult::Revoked { reservation_id },
                    JepsenOperationKind::Reclaim => JepsenWriteResult::Reclaimed { reservation_id },
                    other => {
                        return Err(format!(
                            "admin operation mapper does not support result for {other:?}"
                        ));
                    }
                };
                Ok((
                    operation,
                    JepsenEventOutcome::CommittedWrite(JepsenCommittedWrite {
                        applied_lsn: response.applied_lsn,
                        result,
                    }),
                ))
            }
            other => definite_failure_from_result_code(other)
                .map(|failure| (operation, JepsenEventOutcome::DefiniteFailure(failure)))
                .ok_or_else(|| format!("unexpected admin result code {other:?}")),
        },
        SubmitResponse::Rejected(failure) => {
            Ok((operation, outcome_from_submission_failure(&failure)))
        }
    }
}

fn observe_resource_read<T: ExternalTestbed>(
    layout: &T,
    replica: &LocalClusterReplicaConfig,
    resource_id: ResourceId,
    required_lsn: Option<Lsn>,
) -> Result<ResourceReadObservation, String> {
    let request = ApiRequest::GetResource(ResourceRequest {
        resource_id,
        required_lsn,
    });
    classify_resource_read_outcome(
        resource_id,
        send_replica_api_request(layout, replica, &request)?,
    )
}

fn drain_expiration_until_resource_available<T: ExternalTestbed>(
    layout: &T,
    replica: &LocalClusterReplicaConfig,
    context: &RunExecutionContext,
    history: &mut HistoryBuilder,
    mut plan: ExpirationDrainPlan<'_>,
) -> Result<Lsn, String> {
    for attempt in 0..=MAX_EXPIRATION_RECOVERY_DRAIN_TICKS {
        match observe_resource_read(layout, replica, plan.resource_id, Some(plan.required_lsn))? {
            ResourceReadObservation::Available => return Ok(plan.required_lsn),
            ResourceReadObservation::Held {
                state,
                current_reservation_id,
                version,
            } => {
                if attempt == MAX_EXPIRATION_RECOVERY_DRAIN_TICKS {
                    return Err(format!(
                        "resource {} remained held after {} follow-up expiration ticks: state={state:?} current_reservation_id={:?} version={} required_lsn={}",
                        plan.resource_id.get(),
                        MAX_EXPIRATION_RECOVERY_DRAIN_TICKS,
                        current_reservation_id,
                        version,
                        plan.required_lsn.get(),
                    ));
                }
                let tick = tick_expirations_event(
                    layout,
                    replica,
                    context,
                    OperationId(plan.operation_id_base + u128::from(attempt)),
                    Slot(plan.slot_base + attempt),
                    plan.expired,
                )?;
                history.push(primary_process_name(replica), tick.0, tick.1);
                plan.required_lsn = tick.2.ok_or_else(|| {
                    format!(
                        "expiration recovery expected a committed follow-up tick attempt={}",
                        attempt + 1
                    )
                })?;
            }
            ResourceReadObservation::NotFound => {
                return Err(format!(
                    "resource {} disappeared while waiting for expiration recovery",
                    plan.resource_id.get()
                ));
            }
            ResourceReadObservation::FenceNotApplied => {
                return Err(format!(
                    "resource {} fence was not applied at lsn {} during expiration recovery",
                    plan.resource_id.get(),
                    plan.required_lsn.get()
                ));
            }
            ResourceReadObservation::EngineHalted => {
                return Err(format!(
                    "resource {} read hit halted engine during expiration recovery",
                    plan.resource_id.get()
                ));
            }
            ResourceReadObservation::NotPrimary => {
                return Err(format!(
                    "resource {} read hit non-primary during expiration recovery",
                    plan.resource_id.get()
                ));
            }
        }
    }

    unreachable!("expiration recovery drain loop must return or fail");
}

fn resource_available_event<T: ExternalTestbed>(
    layout: &T,
    replica: &LocalClusterReplicaConfig,
    resource_id: ResourceId,
    required_lsn: Option<Lsn>,
) -> Result<(JepsenOperation, JepsenEventOutcome), String> {
    let operation = JepsenOperation {
        kind: JepsenOperationKind::GetResource,
        operation_id: None,
        resource_id: Some(resource_id),
        resource_ids: Vec::new(),
        reservation_id: None,
        holder_id: None,
        lease_epoch: None,
        required_lsn,
        request_slot: None,
        ttl_slots: None,
    };
    match observe_resource_read(layout, replica, resource_id, required_lsn)? {
        ResourceReadObservation::Available => Ok((
            operation,
            JepsenEventOutcome::SuccessfulRead(JepsenSuccessfulRead {
                target: JepsenReadTarget::Resource,
                served_by: replica.replica_id,
                served_role: replica.role,
                observed_lsn: required_lsn,
                state: JepsenReadState::Resource(
                    allocdb_node::jepsen::JepsenResourceState::Available,
                ),
            }),
        )),
        ResourceReadObservation::NotFound => Ok((
            operation,
            JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::NotFound),
        )),
        ResourceReadObservation::FenceNotApplied => Ok((
            operation,
            JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::FenceNotApplied),
        )),
        ResourceReadObservation::EngineHalted => Ok((
            operation,
            JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::EngineHalted),
        )),
        ResourceReadObservation::NotPrimary => Ok((
            operation,
            JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::NotPrimary),
        )),
        ResourceReadObservation::Held {
            state,
            current_reservation_id,
            version,
        } => Err(format!(
            "resource read for {} returned held state={state:?} current_reservation_id={current_reservation_id:?} version={version}",
            resource_id.get()
        )),
    }
}

fn send_replica_api_request<T: ExternalTestbed>(
    layout: &T,
    replica: &LocalClusterReplicaConfig,
    request: &ApiRequest,
) -> Result<RemoteApiOutcome, String> {
    let response_bytes = send_remote_api_request(
        layout,
        &replica.client_addr.ip().to_string(),
        replica.client_addr.port(),
        request,
    )?;
    match decode_response(&response_bytes) {
        Ok(response) => Ok(RemoteApiOutcome::Api(response)),
        Err(_) => Ok(RemoteApiOutcome::Text(
            String::from_utf8_lossy(&response_bytes).trim().to_owned(),
        )),
    }
}

fn response_text_is_not_primary(text: &str) -> bool {
    text.contains("not primary")
}

#[cfg(test)]
mod tests {
    use super::*;
    use allocdb_core::config::Config;
    use allocdb_core::result::CommandOutcome;
    use allocdb_node::encode_response;
    use allocdb_node::engine::{EngineConfig, ExpirationTickResult, SubmissionErrorCategory};
    use allocdb_node::local_cluster::LocalClusterLayout;
    use allocdb_node::{InvalidRequestReason, SubmissionResult};
    use std::collections::VecDeque;
    use std::net::SocketAddr;
    use std::path::{Path, PathBuf};
    use std::sync::Mutex;

    struct FakeExternalTestbed {
        workspace_root: PathBuf,
        replica_layout: LocalClusterLayout,
        responses: Mutex<VecDeque<Result<Vec<u8>, String>>>,
    }

    impl FakeExternalTestbed {
        fn new(responses: Vec<Result<Vec<u8>, String>>) -> Self {
            let workspace_root = PathBuf::from("/tmp/allocdb-jepsen-events-tests");
            let replica = LocalClusterReplicaConfig {
                replica_id: allocdb_node::ReplicaId(1),
                role: allocdb_node::ReplicaRole::Primary,
                workspace_dir: workspace_root.join("replica-1"),
                log_path: workspace_root.join("replica-1.log"),
                pid_path: workspace_root.join("replica-1.pid"),
                paths: allocdb_node::ReplicaPaths::new(
                    workspace_root.join("replica-1.metadata"),
                    workspace_root.join("replica-1.snapshot"),
                    workspace_root.join("replica-1.wal"),
                ),
                control_addr: SocketAddr::from(([127, 0, 0, 1], 17_001)),
                client_addr: SocketAddr::from(([127, 0, 0, 1], 18_001)),
                protocol_addr: SocketAddr::from(([127, 0, 0, 1], 19_001)),
            };
            Self {
                workspace_root: workspace_root.clone(),
                replica_layout: LocalClusterLayout {
                    workspace_root,
                    current_view: 1,
                    core_config: Config {
                        shard_id: 1,
                        max_resources: 64,
                        max_reservations: 64,
                        max_bundle_size: 1,
                        max_operations: 256,
                        max_ttl_slots: 64,
                        max_client_retry_window_slots: 64,
                        reservation_history_window_slots: 32,
                        max_expiration_bucket_len: 64,
                    },
                    engine_config: EngineConfig {
                        max_submission_queue: 64,
                        max_command_bytes: 4096,
                        max_expirations_per_tick: 16,
                    },
                    replicas: vec![replica],
                },
                responses: Mutex::new(VecDeque::from(responses)),
            }
        }
    }

    impl crate::surface::ExternalTestbed for FakeExternalTestbed {
        fn backend_name(&self) -> &'static str {
            "fake"
        }

        fn workspace_root(&self) -> &Path {
            &self.workspace_root
        }

        fn replica_layout(&self) -> &LocalClusterLayout {
            &self.replica_layout
        }

        fn run_remote_tcp_request(
            &self,
            _host: &str,
            _port: u16,
            _request_bytes: &[u8],
        ) -> Result<Vec<u8>, String> {
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| Err(String::from("no fake response queued")))
        }

        fn run_remote_host_command(
            &self,
            _remote_command: &str,
            _stdin_bytes: Option<&[u8]>,
        ) -> Result<Vec<u8>, String> {
            Err(String::from("fake host command is not implemented"))
        }
    }

    fn test_operation() -> JepsenOperation {
        JepsenOperation {
            kind: JepsenOperationKind::Reserve,
            operation_id: Some(9),
            resource_id: Some(ResourceId(7)),
            resource_ids: Vec::new(),
            reservation_id: None,
            holder_id: Some(11),
            lease_epoch: None,
            required_lsn: None,
            request_slot: Some(Slot(3)),
            ttl_slots: Some(5),
        }
    }

    fn encoded_api_response(response: &ApiResponse) -> Vec<u8> {
        encode_response(response)
    }

    #[test]
    fn map_reserve_submit_response_maps_expected_result_codes() {
        let operation = test_operation();

        let committed = map_reserve_submit_response(
            operation.clone(),
            ResourceId(7),
            HolderId(11),
            SubmitResponse::Committed(
                SubmissionResult {
                    applied_lsn: Lsn(21),
                    outcome: CommandOutcome::with_reservation_epoch(
                        ResultCode::Ok,
                        ReservationId(31),
                        1,
                        Slot(99),
                    ),
                    from_retry_cache: false,
                }
                .into(),
            ),
        )
        .unwrap();
        assert_eq!(committed.2.unwrap().reservation_id, ReservationId(31));

        let busy = map_reserve_submit_response(
            operation.clone(),
            ResourceId(7),
            HolderId(11),
            SubmitResponse::Committed(
                SubmissionResult {
                    applied_lsn: Lsn(21),
                    outcome: CommandOutcome::new(ResultCode::ResourceBusy),
                    from_retry_cache: false,
                }
                .into(),
            ),
        )
        .unwrap();
        assert_eq!(
            busy.1,
            JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::Busy)
        );

        let retired = map_reserve_submit_response(
            operation,
            ResourceId(7),
            HolderId(11),
            SubmitResponse::Committed(
                SubmissionResult {
                    applied_lsn: Lsn(21),
                    outcome: CommandOutcome::new(ResultCode::ReservationRetired),
                    from_retry_cache: false,
                }
                .into(),
            ),
        )
        .unwrap();
        assert_eq!(
            retired.1,
            JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::Retired)
        );
    }

    #[test]
    fn map_holder_submit_response_maps_confirm_and_stale_epoch() {
        let operation = JepsenOperation {
            kind: JepsenOperationKind::Confirm,
            operation_id: Some(19),
            resource_id: Some(ResourceId(7)),
            resource_ids: vec![ResourceId(7), ResourceId(8)],
            reservation_id: Some(31),
            holder_id: Some(11),
            lease_epoch: Some(2),
            required_lsn: None,
            request_slot: Some(Slot(5)),
            ttl_slots: None,
        };

        let committed = map_holder_submit_response(
            operation.clone(),
            SubmitResponse::Committed(
                SubmissionResult {
                    applied_lsn: Lsn(21),
                    outcome: CommandOutcome::new(ResultCode::Ok),
                    from_retry_cache: false,
                }
                .into(),
            ),
        )
        .unwrap();
        assert_eq!(
            committed.1,
            JepsenEventOutcome::CommittedWrite(JepsenCommittedWrite {
                applied_lsn: Lsn(21),
                result: JepsenWriteResult::Confirmed {
                    resource_id: ResourceId(7),
                    lease_epoch: 2,
                    holder_id: 11,
                    reservation_id: 31,
                },
            })
        );

        let stale = map_holder_submit_response(
            operation,
            SubmitResponse::Committed(
                SubmissionResult {
                    applied_lsn: Lsn(22),
                    outcome: CommandOutcome::new(ResultCode::StaleEpoch),
                    from_retry_cache: false,
                }
                .into(),
            ),
        )
        .unwrap();
        assert_eq!(
            stale.1,
            JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::StaleEpoch)
        );
    }

    #[test]
    fn map_admin_submit_response_maps_revoke_and_reclaim() {
        let revoke_operation = JepsenOperation {
            kind: JepsenOperationKind::Revoke,
            operation_id: Some(29),
            resource_id: None,
            resource_ids: Vec::new(),
            reservation_id: Some(41),
            holder_id: None,
            lease_epoch: None,
            required_lsn: None,
            request_slot: Some(Slot(6)),
            ttl_slots: None,
        };

        let revoke = map_admin_submit_response(
            revoke_operation.clone(),
            SubmitResponse::Committed(
                SubmissionResult {
                    applied_lsn: Lsn(31),
                    outcome: CommandOutcome::new(ResultCode::Ok),
                    from_retry_cache: false,
                }
                .into(),
            ),
        )
        .unwrap();
        assert_eq!(
            revoke.1,
            JepsenEventOutcome::CommittedWrite(JepsenCommittedWrite {
                applied_lsn: Lsn(31),
                result: JepsenWriteResult::Revoked { reservation_id: 41 },
            })
        );

        let reclaim = map_admin_submit_response(
            JepsenOperation {
                kind: JepsenOperationKind::Reclaim,
                ..revoke_operation
            },
            SubmitResponse::Committed(
                SubmissionResult {
                    applied_lsn: Lsn(32),
                    outcome: CommandOutcome::new(ResultCode::Ok),
                    from_retry_cache: false,
                }
                .into(),
            ),
        )
        .unwrap();
        assert_eq!(
            reclaim.1,
            JepsenEventOutcome::CommittedWrite(JepsenCommittedWrite {
                applied_lsn: Lsn(32),
                result: JepsenWriteResult::Reclaimed { reservation_id: 41 },
            })
        );
    }

    #[test]
    fn revoking_reservation_maps_to_confirmed_active_state() {
        let state = map_reservation_state(&allocdb_node::LeaseView {
            lease_id: ReservationId(11),
            holder_id: HolderId(33),
            lease_epoch: 2,
            state: LeaseViewState::Revoking,
            created_lsn: Lsn(1),
            deadline_slot: Some(Slot(9)),
            released_lsn: None,
            retire_after_slot: None,
            member_resource_ids: vec![ResourceId(22)],
        });
        assert_eq!(
            state,
            JepsenReservationState::Active {
                resource_id: ResourceId(22),
                holder_id: 33,
                expires_at_slot: Slot(9),
                confirmed: true,
            }
        );
    }

    #[test]
    fn revoked_reservation_maps_to_released_state() {
        let state = map_reservation_state(&allocdb_node::LeaseView {
            lease_id: ReservationId(11),
            holder_id: HolderId(33),
            lease_epoch: 2,
            state: LeaseViewState::Revoked,
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
    fn outcome_from_submission_failure_maps_rejection_codes() {
        let overloaded = outcome_from_submission_failure(&SubmissionFailure {
            category: SubmissionErrorCategory::DefiniteFailure,
            code: SubmissionFailureCode::Overloaded {
                queue_depth: 9,
                queue_capacity: 16,
            },
        });
        assert_eq!(
            overloaded,
            JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::Busy)
        );

        let invalid = outcome_from_submission_failure(&SubmissionFailure {
            category: SubmissionErrorCategory::DefiniteFailure,
            code: SubmissionFailureCode::InvalidRequest(InvalidRequestReason::InvalidLayout),
        });
        assert_eq!(
            invalid,
            JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::InvalidRequest)
        );

        let halted = outcome_from_submission_failure(&SubmissionFailure {
            category: SubmissionErrorCategory::DefiniteFailure,
            code: SubmissionFailureCode::EngineHalted,
        });
        assert_eq!(
            halted,
            JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::EngineHalted)
        );
    }

    #[test]
    fn drain_expiration_until_resource_available_reports_retry_exhaustion() {
        let mut responses = Vec::new();
        for attempt in 0..MAX_EXPIRATION_RECOVERY_DRAIN_TICKS {
            responses.push(Ok(encoded_api_response(&ApiResponse::GetResource(
                ResourceResponse::Found(allocdb_node::ResourceView {
                    resource_id: ResourceId(7),
                    state: ResourceViewState::Reserved,
                    current_lease_id: Some(ReservationId(70 + u128::from(attempt))),
                    version: attempt,
                }),
            ))));
            responses.push(Ok(encoded_api_response(&ApiResponse::TickExpirations(
                TickExpirationsResponse::Applied(
                    ExpirationTickResult {
                        processed_count: 1,
                        last_applied_lsn: Some(Lsn(40 + attempt)),
                    }
                    .into(),
                ),
            ))));
        }
        responses.push(Ok(encoded_api_response(&ApiResponse::GetResource(
            ResourceResponse::Found(allocdb_node::ResourceView {
                resource_id: ResourceId(7),
                state: ResourceViewState::Reserved,
                current_lease_id: Some(ReservationId(999)),
                version: 99,
            }),
        ))));

        let layout = FakeExternalTestbed::new(responses);
        let replica = layout.replica_layout.replicas[0].clone();
        let context = RunExecutionContext::new(
            crate::tracker::RunTracker::new(
                layout.backend_name(),
                &allocdb_node::jepsen::JepsenRunSpec {
                    run_id: String::from("test"),
                    workload: allocdb_node::jepsen::JepsenWorkloadFamily::ExpirationAndRecovery,
                    nemesis: allocdb_node::jepsen::JepsenNemesisFamily::None,
                    minimum_fault_window_secs: None,
                    release_blocking: false,
                },
                &std::env::temp_dir().join(format!("allocdb-events-tests-{}", std::process::id())),
            )
            .unwrap(),
            0,
        );
        let mut history = HistoryBuilder::new(None, 0);
        let error = drain_expiration_until_resource_available(
            &layout,
            &replica,
            &context,
            &mut history,
            ExpirationDrainPlan {
                resource_id: ResourceId(7),
                expired: &[],
                required_lsn: Lsn(1),
                operation_id_base: 1000,
                slot_base: 20,
            },
        )
        .unwrap_err();
        assert!(error.contains("remained held"));
        assert!(error.contains(&MAX_EXPIRATION_RECOVERY_DRAIN_TICKS.to_string()));
    }

    #[test]
    fn drain_expiration_until_resource_available_stops_once_resource_is_available() {
        let layout = FakeExternalTestbed::new(vec![
            Ok(encoded_api_response(&ApiResponse::GetResource(
                ResourceResponse::Found(allocdb_node::ResourceView {
                    resource_id: ResourceId(7),
                    state: ResourceViewState::Reserved,
                    current_lease_id: Some(ReservationId(70)),
                    version: 1,
                }),
            ))),
            Ok(encoded_api_response(&ApiResponse::TickExpirations(
                TickExpirationsResponse::Applied(
                    ExpirationTickResult {
                        processed_count: 1,
                        last_applied_lsn: Some(Lsn(44)),
                    }
                    .into(),
                ),
            ))),
            Ok(encoded_api_response(&ApiResponse::GetResource(
                ResourceResponse::Found(allocdb_node::ResourceView {
                    resource_id: ResourceId(7),
                    state: ResourceViewState::Available,
                    current_lease_id: None,
                    version: 2,
                }),
            ))),
        ]);
        let replica = layout.replica_layout.replicas[0].clone();
        let context = RunExecutionContext::new(
            crate::tracker::RunTracker::new(
                layout.backend_name(),
                &allocdb_node::jepsen::JepsenRunSpec {
                    run_id: String::from("test"),
                    workload: allocdb_node::jepsen::JepsenWorkloadFamily::ExpirationAndRecovery,
                    nemesis: allocdb_node::jepsen::JepsenNemesisFamily::None,
                    minimum_fault_window_secs: None,
                    release_blocking: false,
                },
                &std::env::temp_dir().join(format!(
                    "allocdb-events-tests-{}-available",
                    std::process::id()
                )),
            )
            .unwrap(),
            0,
        );
        let mut history = HistoryBuilder::new(None, 0);
        let lsn = drain_expiration_until_resource_available(
            &layout,
            &replica,
            &context,
            &mut history,
            ExpirationDrainPlan {
                resource_id: ResourceId(7),
                expired: &[],
                required_lsn: Lsn(1),
                operation_id_base: 2000,
                slot_base: 30,
            },
        )
        .unwrap();
        assert_eq!(lsn, Lsn(44));
    }

    #[test]
    fn response_text_not_primary_is_detected() {
        assert!(response_text_is_not_primary("not primary: role=backup"));
        assert!(!response_text_is_not_primary("unexpected failure"));
    }

    #[test]
    fn classify_resource_read_outcome_rejects_mismatched_resource() {
        let outcome = classify_resource_read_outcome(
            ResourceId(7),
            RemoteApiOutcome::Api(ApiResponse::GetResource(ResourceResponse::Found(
                allocdb_node::ResourceView {
                    resource_id: ResourceId(8),
                    state: ResourceViewState::Available,
                    current_lease_id: None,
                    version: 1,
                },
            ))),
        )
        .unwrap_err();
        assert!(outcome.contains("mismatched resource 8"));
    }
}

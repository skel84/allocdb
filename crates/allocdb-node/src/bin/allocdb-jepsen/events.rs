use allocdb_core::ReservationState;
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
    ApiRequest, ApiResponse, ReservationRequest, ReservationResponse, ResourceRequest,
    ResourceResponse, SubmissionFailure, SubmissionFailureCode, SubmitRequest, SubmitResponse,
    TickExpirationsRequest, TickExpirationsResponse, decode_response,
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
        state: allocdb_core::ResourceState,
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
            if response.outcome.result_code == ResultCode::Ok =>
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
        reservation_id: None,
        holder_id: Some(spec.holder_id.get()),
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
        reservation_id: None,
        holder_id: None,
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
                Ok((operation, outcome_from_submission_failure(failure), None))
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
        reservation_id: Some(reservation_id.get()),
        holder_id: None,
        required_lsn,
        request_slot: Some(current_slot),
        ttl_slots: None,
    };
    let request = ApiRequest::GetReservation(ReservationRequest {
        reservation_id,
        current_slot,
        required_lsn,
    });
    match send_replica_api_request(layout, replica, &request)? {
        RemoteApiOutcome::Api(ApiResponse::GetReservation(ReservationResponse::Found(
            reservation,
        ))) => Ok((
            operation,
            JepsenEventOutcome::SuccessfulRead(JepsenSuccessfulRead {
                target: JepsenReadTarget::Reservation,
                served_by: replica.replica_id,
                served_role: replica.role,
                observed_lsn: required_lsn,
                state: JepsenReadState::Reservation(map_reservation_state(reservation)),
            }),
        )),
        RemoteApiOutcome::Api(ApiResponse::GetReservation(ReservationResponse::NotFound)) => Ok((
            operation,
            JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::NotFound),
        )),
        RemoteApiOutcome::Api(ApiResponse::GetReservation(ReservationResponse::Retired)) => Ok((
            operation,
            JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::Retired),
        )),
        RemoteApiOutcome::Api(ApiResponse::GetReservation(
            ReservationResponse::FenceNotApplied { .. },
        )) => Ok((
            operation,
            JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::FenceNotApplied),
        )),
        RemoteApiOutcome::Api(ApiResponse::GetReservation(ReservationResponse::EngineHalted)) => {
            Ok((
                operation,
                JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::EngineHalted),
            ))
        }
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
            if matches!(resource.state, allocdb_core::ResourceState::Available) {
                Ok(ResourceReadObservation::Available)
            } else {
                Ok(ResourceReadObservation::Held {
                    state: resource.state,
                    current_reservation_id: resource.current_reservation_id,
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
    reservation: allocdb_node::ReservationView,
) -> JepsenReservationState {
    match reservation.state {
        ReservationState::Reserved => JepsenReservationState::Active {
            resource_id: reservation.resource_id,
            holder_id: reservation.holder_id.get(),
            expires_at_slot: reservation.deadline_slot,
            confirmed: false,
        },
        ReservationState::Confirmed => JepsenReservationState::Active {
            resource_id: reservation.resource_id,
            holder_id: reservation.holder_id.get(),
            expires_at_slot: reservation.deadline_slot,
            confirmed: true,
        },
        ReservationState::Released | ReservationState::Expired => {
            JepsenReservationState::Released {
                resource_id: reservation.resource_id,
                holder_id: reservation.holder_id.get(),
                released_lsn: reservation.released_lsn,
            }
        }
    }
}

pub(super) fn outcome_from_submission_failure(failure: SubmissionFailure) -> JepsenEventOutcome {
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
        SubmitResponse::Committed(response) => match response.outcome.result_code {
            ResultCode::Ok => {
                let reservation_id = response
                    .outcome
                    .reservation_id
                    .ok_or_else(|| String::from("reserve commit missing reservation_id"))?;
                let expires_at_slot = response
                    .outcome
                    .deadline_slot
                    .ok_or_else(|| String::from("reserve commit missing deadline_slot"))?;
                Ok((
                    operation,
                    JepsenEventOutcome::CommittedWrite(JepsenCommittedWrite {
                        applied_lsn: response.applied_lsn,
                        result: JepsenWriteResult::Reserved {
                            resource_id,
                            holder_id: holder_id.get(),
                            reservation_id: reservation_id.get(),
                            expires_at_slot,
                        },
                    }),
                    Some(ReserveCommit {
                        applied_lsn: response.applied_lsn,
                        reservation_id,
                    }),
                ))
            }
            ResultCode::ResourceBusy => Ok((
                operation,
                JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::Busy),
                None,
            )),
            ResultCode::OperationConflict => Ok((
                operation,
                JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::Conflict),
                None,
            )),
            ResultCode::ResourceNotFound | ResultCode::ReservationNotFound => Ok((
                operation,
                JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::NotFound),
                None,
            )),
            ResultCode::ReservationRetired => Ok((
                operation,
                JepsenEventOutcome::DefiniteFailure(JepsenDefiniteFailure::Retired),
                None,
            )),
            other => Err(format!("unexpected reserve result code {other:?}")),
        },
        SubmitResponse::Rejected(failure) => {
            Ok((operation, outcome_from_submission_failure(failure), None))
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
        reservation_id: None,
        holder_id: None,
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

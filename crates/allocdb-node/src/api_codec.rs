use allocdb_core::HealthMetrics;
use allocdb_core::ids::{HolderId, Lsn, ReservationId, ResourceId, Slot};
use allocdb_core::result::{CommandOutcome, ResultCode};
use allocdb_core::{ReservationState, ResourceState, SlotOverflowKind};

use crate::engine::{EngineMetrics, RecoveryStartupKind, RecoveryStatus, SubmissionErrorCategory};

use super::{
    ApiRequest, ApiResponse, InvalidRequestReason, MetricsRequest, MetricsResponse,
    ReservationRequest, ReservationResponse, ReservationView, ResourceRequest, ResourceResponse,
    ResourceView, SubmissionCommitted, SubmissionFailure, SubmissionFailureCode, SubmitRequest,
    SubmitResponse, TickExpirationsApplied, TickExpirationsRequest, TickExpirationsResponse,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApiCodecError {
    BufferTooShort,
    InvalidRequestTag(u8),
    InvalidResponseTag(u8),
    InvalidEnumValue { kind: &'static str, value: u8 },
    InvalidLayout,
    LengthTooLarge,
}

/// Encodes one transport-neutral API request.
///
/// # Errors
///
/// Returns [`ApiCodecError::LengthTooLarge`] if a variable-length field cannot fit in the wire
/// length prefix.
pub fn encode_request(request: &ApiRequest) -> Result<Vec<u8>, ApiCodecError> {
    let mut bytes = Vec::new();
    match request {
        ApiRequest::Submit(request) => {
            bytes.push(1);
            bytes.extend_from_slice(&request.request_slot.get().to_le_bytes());
            encode_bytes(&mut bytes, &request.payload)?;
        }
        ApiRequest::GetResource(request) => {
            bytes.push(2);
            encode_optional_lsn(&mut bytes, request.required_lsn);
            bytes.extend_from_slice(&request.resource_id.get().to_le_bytes());
        }
        ApiRequest::GetReservation(request) => {
            bytes.push(3);
            encode_optional_lsn(&mut bytes, request.required_lsn);
            bytes.extend_from_slice(&request.current_slot.get().to_le_bytes());
            bytes.extend_from_slice(&request.reservation_id.get().to_le_bytes());
        }
        ApiRequest::GetMetrics(request) => {
            bytes.push(4);
            bytes.extend_from_slice(&request.current_wall_clock_slot.get().to_le_bytes());
        }
        ApiRequest::TickExpirations(request) => {
            bytes.push(5);
            bytes.extend_from_slice(&request.current_wall_clock_slot.get().to_le_bytes());
        }
    }
    Ok(bytes)
}

/// Decodes one transport-neutral API request.
///
/// # Errors
///
/// Returns [`ApiCodecError`] when the request framing is truncated or structurally invalid.
pub fn decode_request(bytes: &[u8]) -> Result<ApiRequest, ApiCodecError> {
    let mut cursor = Cursor::new(bytes);
    let request = match cursor.read_u8()? {
        1 => ApiRequest::Submit(SubmitRequest {
            request_slot: Slot(cursor.read_u64()?),
            payload: cursor.read_bytes()?,
        }),
        2 => ApiRequest::GetResource(ResourceRequest {
            required_lsn: cursor.read_optional_u64()?.map(Lsn),
            resource_id: ResourceId(cursor.read_u128()?),
        }),
        3 => ApiRequest::GetReservation(ReservationRequest {
            required_lsn: cursor.read_optional_u64()?.map(Lsn),
            current_slot: Slot(cursor.read_u64()?),
            reservation_id: ReservationId(cursor.read_u128()?),
        }),
        4 => ApiRequest::GetMetrics(MetricsRequest {
            current_wall_clock_slot: Slot(cursor.read_u64()?),
        }),
        5 => ApiRequest::TickExpirations(TickExpirationsRequest {
            current_wall_clock_slot: Slot(cursor.read_u64()?),
        }),
        value => return Err(ApiCodecError::InvalidRequestTag(value)),
    };
    cursor.finish()?;
    Ok(request)
}

#[must_use]
pub fn encode_response(response: &ApiResponse) -> Vec<u8> {
    let mut bytes = Vec::new();
    match response {
        ApiResponse::Submit(SubmitResponse::Committed(response)) => {
            bytes.push(1);
            encode_submission_committed(&mut bytes, *response);
        }
        ApiResponse::Submit(SubmitResponse::Rejected(response)) => {
            bytes.push(2);
            encode_submission_failure(&mut bytes, *response);
        }
        ApiResponse::GetResource(ResourceResponse::Found(response)) => {
            bytes.push(3);
            encode_resource_view(&mut bytes, *response);
        }
        ApiResponse::GetResource(ResourceResponse::NotFound) => {
            bytes.push(4);
        }
        ApiResponse::GetResource(ResourceResponse::FenceNotApplied {
            required_lsn,
            last_applied_lsn,
        }) => {
            bytes.push(5);
            encode_read_fence(&mut bytes, *required_lsn, *last_applied_lsn);
        }
        ApiResponse::GetReservation(ReservationResponse::Found(response)) => {
            bytes.push(6);
            encode_reservation_view(&mut bytes, *response);
        }
        ApiResponse::GetReservation(ReservationResponse::NotFound) => {
            bytes.push(7);
        }
        ApiResponse::GetReservation(ReservationResponse::Retired) => {
            bytes.push(8);
        }
        ApiResponse::GetReservation(ReservationResponse::FenceNotApplied {
            required_lsn,
            last_applied_lsn,
        }) => {
            bytes.push(9);
            encode_read_fence(&mut bytes, *required_lsn, *last_applied_lsn);
        }
        ApiResponse::GetMetrics(response) => {
            bytes.push(10);
            encode_metrics(&mut bytes, response.metrics);
        }
        ApiResponse::GetResource(ResourceResponse::EngineHalted) => {
            bytes.push(11);
        }
        ApiResponse::GetReservation(ReservationResponse::EngineHalted) => {
            bytes.push(12);
        }
        ApiResponse::TickExpirations(TickExpirationsResponse::Applied(response)) => {
            bytes.push(13);
            encode_tick_expirations_applied(&mut bytes, *response);
        }
        ApiResponse::TickExpirations(TickExpirationsResponse::Rejected(response)) => {
            bytes.push(14);
            encode_submission_failure(&mut bytes, *response);
        }
    }
    bytes
}

/// Decodes one transport-neutral API response.
///
/// # Errors
///
/// Returns [`ApiCodecError`] when the response framing is truncated or structurally invalid.
pub fn decode_response(bytes: &[u8]) -> Result<ApiResponse, ApiCodecError> {
    let mut cursor = Cursor::new(bytes);
    let response = match cursor.read_u8()? {
        1 => ApiResponse::Submit(SubmitResponse::Committed(decode_submission_committed(
            &mut cursor,
        )?)),
        2 => ApiResponse::Submit(SubmitResponse::Rejected(decode_submission_failure(
            &mut cursor,
        )?)),
        3 => ApiResponse::GetResource(ResourceResponse::Found(decode_resource_view(&mut cursor)?)),
        4 => ApiResponse::GetResource(ResourceResponse::NotFound),
        5 => {
            let (required_lsn, last_applied_lsn) = decode_read_fence(&mut cursor)?;
            ApiResponse::GetResource(ResourceResponse::FenceNotApplied {
                required_lsn,
                last_applied_lsn,
            })
        }
        6 => ApiResponse::GetReservation(ReservationResponse::Found(decode_reservation_view(
            &mut cursor,
        )?)),
        7 => ApiResponse::GetReservation(ReservationResponse::NotFound),
        8 => ApiResponse::GetReservation(ReservationResponse::Retired),
        9 => {
            let (required_lsn, last_applied_lsn) = decode_read_fence(&mut cursor)?;
            ApiResponse::GetReservation(ReservationResponse::FenceNotApplied {
                required_lsn,
                last_applied_lsn,
            })
        }
        10 => ApiResponse::GetMetrics(MetricsResponse {
            metrics: decode_metrics(&mut cursor)?,
        }),
        11 => ApiResponse::GetResource(ResourceResponse::EngineHalted),
        12 => ApiResponse::GetReservation(ReservationResponse::EngineHalted),
        13 => ApiResponse::TickExpirations(TickExpirationsResponse::Applied(
            decode_tick_expirations_applied(&mut cursor)?,
        )),
        14 => ApiResponse::TickExpirations(TickExpirationsResponse::Rejected(
            decode_submission_failure(&mut cursor)?,
        )),
        value => return Err(ApiCodecError::InvalidResponseTag(value)),
    };
    cursor.finish()?;
    Ok(response)
}

fn encode_submission_committed(bytes: &mut Vec<u8>, response: SubmissionCommitted) {
    bytes.extend_from_slice(&response.applied_lsn.get().to_le_bytes());
    encode_command_outcome(bytes, response.outcome);
    encode_bool(bytes, response.from_retry_cache);
}

fn decode_submission_committed(
    cursor: &mut Cursor<'_>,
) -> Result<SubmissionCommitted, ApiCodecError> {
    Ok(SubmissionCommitted {
        applied_lsn: Lsn(cursor.read_u64()?),
        outcome: decode_command_outcome(cursor)?,
        from_retry_cache: cursor.read_bool()?,
    })
}

fn encode_submission_failure(bytes: &mut Vec<u8>, response: SubmissionFailure) {
    bytes.push(encode_submission_error_category(response.category));
    match response.code {
        SubmissionFailureCode::EngineHalted => {
            bytes.push(1);
        }
        SubmissionFailureCode::InvalidRequest(reason) => {
            bytes.push(2);
            encode_invalid_request_reason(bytes, reason);
        }
        SubmissionFailureCode::SlotOverflow {
            kind,
            request_slot,
            delta,
        } => {
            bytes.push(3);
            encode_slot_overflow_kind(bytes, kind);
            bytes.extend_from_slice(&request_slot.get().to_le_bytes());
            bytes.extend_from_slice(&delta.to_le_bytes());
        }
        SubmissionFailureCode::CommandTooLarge {
            encoded_len,
            max_command_bytes,
        } => {
            bytes.push(4);
            bytes.extend_from_slice(&encoded_len.to_le_bytes());
            bytes.extend_from_slice(&max_command_bytes.to_le_bytes());
        }
        SubmissionFailureCode::LsnExhausted { last_applied_lsn } => {
            bytes.push(5);
            bytes.extend_from_slice(&last_applied_lsn.get().to_le_bytes());
        }
        SubmissionFailureCode::Overloaded {
            queue_depth,
            queue_capacity,
        } => {
            bytes.push(6);
            bytes.extend_from_slice(&queue_depth.to_le_bytes());
            bytes.extend_from_slice(&queue_capacity.to_le_bytes());
        }
        SubmissionFailureCode::StorageFailure => {
            bytes.push(7);
        }
    }
}

fn decode_submission_failure(cursor: &mut Cursor<'_>) -> Result<SubmissionFailure, ApiCodecError> {
    let category = decode_submission_error_category(cursor.read_u8()?)?;
    let code = match cursor.read_u8()? {
        1 => SubmissionFailureCode::EngineHalted,
        2 => SubmissionFailureCode::InvalidRequest(decode_invalid_request_reason(cursor)?),
        3 => SubmissionFailureCode::SlotOverflow {
            kind: decode_slot_overflow_kind(cursor.read_u8()?)?,
            request_slot: Slot(cursor.read_u64()?),
            delta: cursor.read_u64()?,
        },
        4 => SubmissionFailureCode::CommandTooLarge {
            encoded_len: cursor.read_u64()?,
            max_command_bytes: cursor.read_u64()?,
        },
        5 => SubmissionFailureCode::LsnExhausted {
            last_applied_lsn: Lsn(cursor.read_u64()?),
        },
        6 => SubmissionFailureCode::Overloaded {
            queue_depth: cursor.read_u32()?,
            queue_capacity: cursor.read_u32()?,
        },
        7 => SubmissionFailureCode::StorageFailure,
        value => {
            return Err(ApiCodecError::InvalidEnumValue {
                kind: "submission_failure_code",
                value,
            });
        }
    };
    Ok(SubmissionFailure { category, code })
}

fn encode_invalid_request_reason(bytes: &mut Vec<u8>, reason: InvalidRequestReason) {
    match reason {
        InvalidRequestReason::BufferTooShort => bytes.push(1),
        InvalidRequestReason::InvalidCommandTag(value) => {
            bytes.push(2);
            bytes.push(value);
        }
        InvalidRequestReason::InvalidLayout => bytes.push(3),
    }
}

fn decode_invalid_request_reason(
    cursor: &mut Cursor<'_>,
) -> Result<InvalidRequestReason, ApiCodecError> {
    match cursor.read_u8()? {
        1 => Ok(InvalidRequestReason::BufferTooShort),
        2 => Ok(InvalidRequestReason::InvalidCommandTag(cursor.read_u8()?)),
        3 => Ok(InvalidRequestReason::InvalidLayout),
        value => Err(ApiCodecError::InvalidEnumValue {
            kind: "invalid_request_reason",
            value,
        }),
    }
}

fn encode_resource_view(bytes: &mut Vec<u8>, view: ResourceView) {
    bytes.extend_from_slice(&view.resource_id.get().to_le_bytes());
    bytes.push(encode_resource_state(view.state));
    encode_optional_u128(bytes, view.current_reservation_id.map(ReservationId::get));
    bytes.extend_from_slice(&view.version.to_le_bytes());
}

fn decode_resource_view(cursor: &mut Cursor<'_>) -> Result<ResourceView, ApiCodecError> {
    Ok(ResourceView {
        resource_id: ResourceId(cursor.read_u128()?),
        state: decode_resource_state(cursor.read_u8()?)?,
        current_reservation_id: cursor.read_optional_u128()?.map(ReservationId),
        version: cursor.read_u64()?,
    })
}

fn encode_reservation_view(bytes: &mut Vec<u8>, view: ReservationView) {
    bytes.extend_from_slice(&view.reservation_id.get().to_le_bytes());
    bytes.extend_from_slice(&view.resource_id.get().to_le_bytes());
    bytes.extend_from_slice(&view.holder_id.get().to_le_bytes());
    bytes.extend_from_slice(&view.lease_epoch.to_le_bytes());
    bytes.push(encode_reservation_state(view.state));
    bytes.extend_from_slice(&view.created_lsn.get().to_le_bytes());
    bytes.extend_from_slice(&view.deadline_slot.get().to_le_bytes());
    encode_optional_u64(bytes, view.released_lsn.map(Lsn::get));
    encode_optional_u64(bytes, view.retire_after_slot.map(Slot::get));
}

fn decode_reservation_view(cursor: &mut Cursor<'_>) -> Result<ReservationView, ApiCodecError> {
    Ok(ReservationView {
        reservation_id: ReservationId(cursor.read_u128()?),
        resource_id: ResourceId(cursor.read_u128()?),
        holder_id: HolderId(cursor.read_u128()?),
        lease_epoch: cursor.read_u64()?,
        state: decode_reservation_state(cursor.read_u8()?)?,
        created_lsn: Lsn(cursor.read_u64()?),
        deadline_slot: Slot(cursor.read_u64()?),
        released_lsn: cursor.read_optional_u64()?.map(Lsn),
        retire_after_slot: cursor.read_optional_u64()?.map(Slot),
    })
}

fn encode_read_fence(bytes: &mut Vec<u8>, required_lsn: Lsn, last_applied_lsn: Option<Lsn>) {
    bytes.extend_from_slice(&required_lsn.get().to_le_bytes());
    encode_optional_lsn(bytes, last_applied_lsn);
}

fn decode_read_fence(cursor: &mut Cursor<'_>) -> Result<(Lsn, Option<Lsn>), ApiCodecError> {
    Ok((
        Lsn(cursor.read_u64()?),
        cursor.read_optional_u64()?.map(Lsn),
    ))
}

fn encode_tick_expirations_applied(bytes: &mut Vec<u8>, response: TickExpirationsApplied) {
    bytes.extend_from_slice(&response.processed_count.to_le_bytes());
    encode_optional_lsn(bytes, response.last_applied_lsn);
}

fn decode_tick_expirations_applied(
    cursor: &mut Cursor<'_>,
) -> Result<TickExpirationsApplied, ApiCodecError> {
    Ok(TickExpirationsApplied {
        processed_count: cursor.read_u32()?,
        last_applied_lsn: cursor.read_optional_u64()?.map(Lsn),
    })
}

fn encode_metrics(bytes: &mut Vec<u8>, metrics: EngineMetrics) {
    bytes.extend_from_slice(&metrics.queue_depth.to_le_bytes());
    bytes.extend_from_slice(&metrics.queue_capacity.to_le_bytes());
    encode_bool(bytes, metrics.accepting_writes);
    encode_recovery_status(bytes, metrics.recovery);
    encode_health_metrics(bytes, metrics.core);
}

fn decode_metrics(cursor: &mut Cursor<'_>) -> Result<EngineMetrics, ApiCodecError> {
    Ok(EngineMetrics {
        queue_depth: cursor.read_u32()?,
        queue_capacity: cursor.read_u32()?,
        accepting_writes: cursor.read_bool()?,
        recovery: decode_recovery_status(cursor)?,
        core: decode_health_metrics(cursor)?,
    })
}

fn encode_recovery_status(bytes: &mut Vec<u8>, recovery: RecoveryStatus) {
    bytes.push(encode_recovery_startup_kind(recovery.startup_kind));
    encode_optional_lsn(bytes, recovery.loaded_snapshot_lsn);
    bytes.extend_from_slice(&recovery.replayed_wal_frame_count.to_le_bytes());
    encode_optional_lsn(bytes, recovery.replayed_wal_last_lsn);
    encode_optional_lsn(bytes, recovery.active_snapshot_lsn);
}

fn decode_recovery_status(cursor: &mut Cursor<'_>) -> Result<RecoveryStatus, ApiCodecError> {
    Ok(RecoveryStatus {
        startup_kind: decode_recovery_startup_kind(cursor.read_u8()?)?,
        loaded_snapshot_lsn: cursor.read_optional_u64()?.map(Lsn),
        replayed_wal_frame_count: cursor.read_u32()?,
        replayed_wal_last_lsn: cursor.read_optional_u64()?.map(Lsn),
        active_snapshot_lsn: cursor.read_optional_u64()?.map(Lsn),
    })
}

fn encode_health_metrics(bytes: &mut Vec<u8>, metrics: HealthMetrics) {
    encode_optional_lsn(bytes, metrics.last_applied_lsn);
    encode_optional_u64(bytes, metrics.last_request_slot.map(Slot::get));
    bytes.extend_from_slice(&metrics.logical_slot_lag.to_le_bytes());
    bytes.extend_from_slice(&metrics.expiration_backlog.to_le_bytes());
    bytes.extend_from_slice(&metrics.operation_table_used.to_le_bytes());
    bytes.extend_from_slice(&metrics.operation_table_capacity.to_le_bytes());
    bytes.push(metrics.operation_table_utilization_pct);
}

fn decode_health_metrics(cursor: &mut Cursor<'_>) -> Result<HealthMetrics, ApiCodecError> {
    Ok(HealthMetrics {
        last_applied_lsn: cursor.read_optional_u64()?.map(Lsn),
        last_request_slot: cursor.read_optional_u64()?.map(Slot),
        logical_slot_lag: cursor.read_u64()?,
        expiration_backlog: cursor.read_u32()?,
        operation_table_used: cursor.read_u32()?,
        operation_table_capacity: cursor.read_u32()?,
        operation_table_utilization_pct: cursor.read_u8()?,
    })
}

fn encode_command_outcome(bytes: &mut Vec<u8>, outcome: CommandOutcome) {
    bytes.push(encode_result_code(outcome.result_code));
    encode_optional_u128(bytes, outcome.reservation_id.map(ReservationId::get));
    encode_optional_u64(bytes, outcome.lease_epoch);
    encode_optional_u64(bytes, outcome.deadline_slot.map(Slot::get));
}

fn decode_command_outcome(cursor: &mut Cursor<'_>) -> Result<CommandOutcome, ApiCodecError> {
    Ok(CommandOutcome {
        result_code: decode_result_code(cursor.read_u8()?)?,
        reservation_id: cursor.read_optional_u128()?.map(ReservationId),
        lease_epoch: cursor.read_optional_u64()?,
        deadline_slot: cursor.read_optional_u64()?.map(Slot),
    })
}

fn encode_optional_lsn(bytes: &mut Vec<u8>, value: Option<Lsn>) {
    encode_optional_u64(bytes, value.map(Lsn::get));
}

fn encode_optional_u64(bytes: &mut Vec<u8>, value: Option<u64>) {
    match value {
        Some(value) => {
            bytes.push(1);
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        None => bytes.push(0),
    }
}

fn encode_optional_u128(bytes: &mut Vec<u8>, value: Option<u128>) {
    match value {
        Some(value) => {
            bytes.push(1);
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        None => bytes.push(0),
    }
}

fn encode_bool(bytes: &mut Vec<u8>, value: bool) {
    bytes.push(u8::from(value));
}

fn encode_bytes(bytes: &mut Vec<u8>, payload: &[u8]) -> Result<(), ApiCodecError> {
    let len = u32::try_from(payload.len()).map_err(|_| ApiCodecError::LengthTooLarge)?;
    bytes.extend_from_slice(&len.to_le_bytes());
    bytes.extend_from_slice(payload);
    Ok(())
}

fn encode_submission_error_category(category: SubmissionErrorCategory) -> u8 {
    match category {
        SubmissionErrorCategory::DefiniteFailure => 1,
        SubmissionErrorCategory::Indefinite => 2,
    }
}

fn decode_submission_error_category(value: u8) -> Result<SubmissionErrorCategory, ApiCodecError> {
    match value {
        1 => Ok(SubmissionErrorCategory::DefiniteFailure),
        2 => Ok(SubmissionErrorCategory::Indefinite),
        value => Err(ApiCodecError::InvalidEnumValue {
            kind: "submission_error_category",
            value,
        }),
    }
}

fn encode_resource_state(state: ResourceState) -> u8 {
    match state {
        ResourceState::Available => 1,
        ResourceState::Reserved => 2,
        ResourceState::Confirmed => 3,
        ResourceState::Revoking => 4,
    }
}

fn decode_resource_state(value: u8) -> Result<ResourceState, ApiCodecError> {
    match value {
        1 => Ok(ResourceState::Available),
        2 => Ok(ResourceState::Reserved),
        3 => Ok(ResourceState::Confirmed),
        4 => Ok(ResourceState::Revoking),
        value => Err(ApiCodecError::InvalidEnumValue {
            kind: "resource_state",
            value,
        }),
    }
}

fn encode_reservation_state(state: ReservationState) -> u8 {
    match state {
        ReservationState::Reserved => 1,
        ReservationState::Confirmed => 2,
        ReservationState::Released => 3,
        ReservationState::Expired => 4,
        ReservationState::Revoking => 5,
        ReservationState::Revoked => 6,
    }
}

fn decode_reservation_state(value: u8) -> Result<ReservationState, ApiCodecError> {
    match value {
        1 => Ok(ReservationState::Reserved),
        2 => Ok(ReservationState::Confirmed),
        3 => Ok(ReservationState::Released),
        4 => Ok(ReservationState::Expired),
        5 => Ok(ReservationState::Revoking),
        6 => Ok(ReservationState::Revoked),
        value => Err(ApiCodecError::InvalidEnumValue {
            kind: "reservation_state",
            value,
        }),
    }
}

fn encode_recovery_startup_kind(kind: RecoveryStartupKind) -> u8 {
    match kind {
        RecoveryStartupKind::FreshStart => 1,
        RecoveryStartupKind::WalOnly => 2,
        RecoveryStartupKind::SnapshotOnly => 3,
        RecoveryStartupKind::SnapshotAndWal => 4,
    }
}

fn decode_recovery_startup_kind(value: u8) -> Result<RecoveryStartupKind, ApiCodecError> {
    match value {
        1 => Ok(RecoveryStartupKind::FreshStart),
        2 => Ok(RecoveryStartupKind::WalOnly),
        3 => Ok(RecoveryStartupKind::SnapshotOnly),
        4 => Ok(RecoveryStartupKind::SnapshotAndWal),
        value => Err(ApiCodecError::InvalidEnumValue {
            kind: "recovery_startup_kind",
            value,
        }),
    }
}

fn encode_result_code(result_code: ResultCode) -> u8 {
    match result_code {
        ResultCode::Ok => 1,
        ResultCode::Noop => 2,
        ResultCode::AlreadyExists => 3,
        ResultCode::ResourceTableFull => 4,
        ResultCode::ResourceNotFound => 5,
        ResultCode::ResourceBusy => 6,
        ResultCode::BundleTooLarge => 7,
        ResultCode::TtlOutOfRange => 8,
        ResultCode::ReservationTableFull => 9,
        ResultCode::ReservationMemberTableFull => 10,
        ResultCode::ReservationNotFound => 11,
        ResultCode::ReservationRetired => 12,
        ResultCode::ExpirationIndexFull => 13,
        ResultCode::OperationTableFull => 14,
        ResultCode::OperationConflict => 15,
        ResultCode::InvalidState => 16,
        ResultCode::HolderMismatch => 17,
        ResultCode::StaleEpoch => 18,
        ResultCode::SlotOverflow => 19,
    }
}

fn decode_result_code(value: u8) -> Result<ResultCode, ApiCodecError> {
    match value {
        1 => Ok(ResultCode::Ok),
        2 => Ok(ResultCode::Noop),
        3 => Ok(ResultCode::AlreadyExists),
        4 => Ok(ResultCode::ResourceTableFull),
        5 => Ok(ResultCode::ResourceNotFound),
        6 => Ok(ResultCode::ResourceBusy),
        7 => Ok(ResultCode::BundleTooLarge),
        8 => Ok(ResultCode::TtlOutOfRange),
        9 => Ok(ResultCode::ReservationTableFull),
        10 => Ok(ResultCode::ReservationMemberTableFull),
        11 => Ok(ResultCode::ReservationNotFound),
        12 => Ok(ResultCode::ReservationRetired),
        13 => Ok(ResultCode::ExpirationIndexFull),
        14 => Ok(ResultCode::OperationTableFull),
        15 => Ok(ResultCode::OperationConflict),
        16 => Ok(ResultCode::InvalidState),
        17 => Ok(ResultCode::HolderMismatch),
        18 => Ok(ResultCode::StaleEpoch),
        19 => Ok(ResultCode::SlotOverflow),
        value => Err(ApiCodecError::InvalidEnumValue {
            kind: "result_code",
            value,
        }),
    }
}

fn encode_slot_overflow_kind(bytes: &mut Vec<u8>, kind: SlotOverflowKind) {
    bytes.push(match kind {
        SlotOverflowKind::OperationWindow => 1,
        SlotOverflowKind::Deadline => 2,
        SlotOverflowKind::ReservationHistory => 3,
    });
}

fn decode_slot_overflow_kind(value: u8) -> Result<SlotOverflowKind, ApiCodecError> {
    match value {
        1 => Ok(SlotOverflowKind::OperationWindow),
        2 => Ok(SlotOverflowKind::Deadline),
        3 => Ok(SlotOverflowKind::ReservationHistory),
        value => Err(ApiCodecError::InvalidEnumValue {
            kind: "slot_overflow_kind",
            value,
        }),
    }
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn finish(&self) -> Result<(), ApiCodecError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(ApiCodecError::InvalidLayout)
        }
    }

    fn read_exact<const N: usize>(&mut self) -> Result<[u8; N], ApiCodecError> {
        if self.offset + N > self.bytes.len() {
            return Err(ApiCodecError::BufferTooShort);
        }

        let mut array = [0_u8; N];
        array.copy_from_slice(&self.bytes[self.offset..self.offset + N]);
        self.offset += N;
        Ok(array)
    }

    fn read_u8(&mut self) -> Result<u8, ApiCodecError> {
        Ok(self.read_exact::<1>()?[0])
    }

    fn read_u32(&mut self) -> Result<u32, ApiCodecError> {
        Ok(u32::from_le_bytes(self.read_exact::<4>()?))
    }

    fn read_u64(&mut self) -> Result<u64, ApiCodecError> {
        Ok(u64::from_le_bytes(self.read_exact::<8>()?))
    }

    fn read_u128(&mut self) -> Result<u128, ApiCodecError> {
        Ok(u128::from_le_bytes(self.read_exact::<16>()?))
    }

    fn read_optional_u64(&mut self) -> Result<Option<u64>, ApiCodecError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_u64()?)),
            value => Err(ApiCodecError::InvalidEnumValue {
                kind: "optional_u64",
                value,
            }),
        }
    }

    fn read_optional_u128(&mut self) -> Result<Option<u128>, ApiCodecError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_u128()?)),
            value => Err(ApiCodecError::InvalidEnumValue {
                kind: "optional_u128",
                value,
            }),
        }
    }

    fn read_bool(&mut self) -> Result<bool, ApiCodecError> {
        match self.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(ApiCodecError::InvalidEnumValue {
                kind: "bool",
                value,
            }),
        }
    }

    fn read_bytes(&mut self) -> Result<Vec<u8>, ApiCodecError> {
        let len = usize::try_from(self.read_u32()?).map_err(|_| ApiCodecError::LengthTooLarge)?;
        if self.offset + len > self.bytes.len() {
            return Err(ApiCodecError::BufferTooShort);
        }

        let slice = &self.bytes[self.offset..self.offset + len];
        self.offset += len;
        Ok(slice.to_vec())
    }
}

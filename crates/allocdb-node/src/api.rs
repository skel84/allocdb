use allocdb_core::command::ClientRequest;
use allocdb_core::command_codec::{
    CommandCodecError, decode_client_request, encode_client_request,
};
use allocdb_core::ids::{HolderId, Lsn, ReservationId, ResourceId, Slot};
use allocdb_core::result::ResultCode;
use allocdb_core::{
    AllocDb, ReservationLookupError, ReservationRecord, ReservationState, ResourceRecord,
    ResourceState, SlotOverflowKind,
};

use crate::engine::{
    EngineMetrics, ExpirationTickResult, ReadError, SingleNodeEngine, SubmissionError,
    SubmissionErrorCategory, SubmissionResult,
};

#[path = "api_codec.rs"]
mod codec;
#[cfg(test)]
#[path = "api_tests.rs"]
mod tests;

pub use codec::{ApiCodecError, decode_request, decode_response, encode_request, encode_response};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApiRequest {
    Submit(SubmitRequest),
    GetResource(ResourceRequest),
    GetLease(LeaseRequest),
    GetMetrics(MetricsRequest),
    TickExpirations(TickExpirationsRequest),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubmitRequest {
    pub request_slot: Slot,
    pub payload: Vec<u8>,
}

impl SubmitRequest {
    #[must_use]
    pub fn from_client_request(request_slot: Slot, request: ClientRequest) -> Self {
        Self {
            request_slot,
            payload: encode_client_request(request),
        }
    }

    /// Decodes the embedded client request payload using the core command codec.
    ///
    /// # Errors
    ///
    /// Returns [`CommandCodecError`] when the payload is truncated or structurally invalid.
    pub fn decode_client_request(&self) -> Result<ClientRequest, CommandCodecError> {
        decode_client_request(&self.payload)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResourceRequest {
    pub resource_id: ResourceId,
    pub required_lsn: Option<Lsn>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LeaseRequest {
    pub lease_id: ReservationId,
    pub current_slot: Slot,
    pub required_lsn: Option<Lsn>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetricsRequest {
    pub current_wall_clock_slot: Slot,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TickExpirationsRequest {
    pub current_wall_clock_slot: Slot,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApiResponse {
    Submit(SubmitResponse),
    GetResource(ResourceResponse),
    GetLease(LeaseResponse),
    GetMetrics(MetricsResponse),
    TickExpirations(TickExpirationsResponse),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SubmitResponse {
    Committed(SubmissionCommitted),
    Rejected(SubmissionFailure),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubmissionCommitted {
    pub applied_lsn: Lsn,
    pub result_code: ResultCode,
    pub lease_id: Option<ReservationId>,
    pub lease_epoch: Option<u64>,
    pub deadline_slot: Option<Slot>,
    pub from_retry_cache: bool,
}

impl From<SubmissionResult> for SubmissionCommitted {
    fn from(value: SubmissionResult) -> Self {
        Self {
            applied_lsn: value.applied_lsn,
            result_code: value.outcome.result_code,
            lease_id: value.outcome.reservation_id,
            lease_epoch: value.outcome.lease_epoch,
            deadline_slot: value.outcome.deadline_slot,
            from_retry_cache: value.from_retry_cache,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubmissionFailure {
    pub category: SubmissionErrorCategory,
    pub code: SubmissionFailureCode,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubmissionFailureCode {
    EngineHalted,
    InvalidRequest(InvalidRequestReason),
    SlotOverflow {
        kind: SlotOverflowKind,
        request_slot: Slot,
        delta: u64,
    },
    CommandTooLarge {
        encoded_len: u64,
        max_command_bytes: u64,
    },
    LsnExhausted {
        last_applied_lsn: Lsn,
    },
    Overloaded {
        queue_depth: u32,
        queue_capacity: u32,
    },
    StorageFailure,
}

impl SubmissionFailure {
    #[must_use]
    ///
    /// # Panics
    ///
    /// Panics only if one in-memory length field exceeds `u64`, which cannot happen on supported
    /// targets because those fields are already bounded by `usize`.
    pub fn from_submission_error(error: &SubmissionError) -> Self {
        let category = error.category();
        let code = match *error {
            SubmissionError::EngineHalted => SubmissionFailureCode::EngineHalted,
            SubmissionError::InvalidRequest(error) => {
                SubmissionFailureCode::InvalidRequest(InvalidRequestReason::from_codec_error(error))
            }
            SubmissionError::SlotOverflow(error) => SubmissionFailureCode::SlotOverflow {
                kind: error.kind,
                request_slot: error.request_slot,
                delta: error.delta,
            },
            SubmissionError::CommandTooLarge {
                encoded_len,
                max_command_bytes,
            } => SubmissionFailureCode::CommandTooLarge {
                encoded_len: u64::try_from(encoded_len).expect("encoded length must fit u64"),
                max_command_bytes: u64::try_from(max_command_bytes)
                    .expect("max command bytes must fit u64"),
            },
            SubmissionError::LsnExhausted { last_applied_lsn } => {
                SubmissionFailureCode::LsnExhausted { last_applied_lsn }
            }
            SubmissionError::Overloaded {
                queue_depth,
                queue_capacity,
            } => SubmissionFailureCode::Overloaded {
                queue_depth,
                queue_capacity,
            },
            // Crash-injected commit ambiguity intentionally stays on the same indefinite
            // storage-failure wire path as real WAL faults so clients preserve retry-after-restart
            // behavior.
            SubmissionError::WalFile(_) | SubmissionError::CrashInjected(_) => {
                SubmissionFailureCode::StorageFailure
            }
        };

        Self { category, code }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvalidRequestReason {
    BufferTooShort,
    InvalidCommandTag(u8),
    InvalidLayout,
}

impl InvalidRequestReason {
    fn from_codec_error(error: CommandCodecError) -> Self {
        match error {
            CommandCodecError::BufferTooShort => Self::BufferTooShort,
            CommandCodecError::InvalidCommandTag(value) => Self::InvalidCommandTag(value),
            CommandCodecError::InvalidLayout => Self::InvalidLayout,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResourceResponse {
    Found(ResourceView),
    NotFound,
    EngineHalted,
    FenceNotApplied {
        required_lsn: Lsn,
        last_applied_lsn: Option<Lsn>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceViewState {
    Available,
    Reserved,
    Active,
    Revoking,
}

impl From<ResourceState> for ResourceViewState {
    fn from(value: ResourceState) -> Self {
        match value {
            ResourceState::Available => Self::Available,
            ResourceState::Reserved => Self::Reserved,
            ResourceState::Confirmed => Self::Active,
            ResourceState::Revoking => Self::Revoking,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResourceView {
    pub resource_id: ResourceId,
    pub state: ResourceViewState,
    pub current_lease_id: Option<ReservationId>,
    pub version: u64,
}

impl From<ResourceRecord> for ResourceView {
    fn from(value: ResourceRecord) -> Self {
        Self {
            resource_id: value.resource_id,
            state: value.current_state.into(),
            current_lease_id: value.current_reservation_id,
            version: value.version,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LeaseViewState {
    Reserved,
    Active,
    Revoking,
    Released,
    Expired,
    Revoked,
}

impl From<ReservationState> for LeaseViewState {
    fn from(value: ReservationState) -> Self {
        match value {
            ReservationState::Reserved => Self::Reserved,
            ReservationState::Confirmed => Self::Active,
            ReservationState::Revoking => Self::Revoking,
            ReservationState::Released => Self::Released,
            ReservationState::Expired => Self::Expired,
            ReservationState::Revoked => Self::Revoked,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LeaseResponse {
    Found(LeaseView),
    NotFound,
    Retired,
    EngineHalted,
    FenceNotApplied {
        required_lsn: Lsn,
        last_applied_lsn: Option<Lsn>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeaseView {
    pub lease_id: ReservationId,
    pub holder_id: HolderId,
    pub state: LeaseViewState,
    pub lease_epoch: u64,
    pub created_lsn: Lsn,
    pub deadline_slot: Option<Slot>,
    pub released_lsn: Option<Lsn>,
    pub retire_after_slot: Option<Slot>,
    pub member_resource_ids: Vec<ResourceId>,
}

impl LeaseView {
    #[must_use]
    pub fn from_db(db: &AllocDb, value: ReservationRecord) -> Self {
        Self {
            lease_id: value.reservation_id,
            holder_id: value.holder_id,
            state: value.state.into(),
            lease_epoch: value.lease_epoch,
            created_lsn: value.created_lsn,
            deadline_slot: Some(value.deadline_slot),
            released_lsn: value.released_lsn,
            retire_after_slot: value.retire_after_slot,
            member_resource_ids: db.reservation_member_resource_ids(value),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetricsResponse {
    pub metrics: EngineMetrics,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TickExpirationsResponse {
    Applied(TickExpirationsApplied),
    Rejected(SubmissionFailure),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TickExpirationsApplied {
    pub processed_count: u32,
    pub last_applied_lsn: Option<Lsn>,
}

impl From<ExpirationTickResult> for TickExpirationsApplied {
    fn from(value: ExpirationTickResult) -> Self {
        Self {
            processed_count: value.processed_count,
            last_applied_lsn: value.last_applied_lsn,
        }
    }
}

impl SingleNodeEngine {
    /// Handles one transport-neutral alpha API request against the live single-node engine.
    #[must_use]
    pub fn handle_api_request(&mut self, request: ApiRequest) -> ApiResponse {
        match request {
            ApiRequest::Submit(request) => {
                ApiResponse::Submit(self.handle_submit_request(&request))
            }
            ApiRequest::GetResource(request) => {
                ApiResponse::GetResource(self.handle_resource_request(request))
            }
            ApiRequest::GetLease(request) => {
                ApiResponse::GetLease(self.handle_lease_request(request))
            }
            ApiRequest::GetMetrics(request) => ApiResponse::GetMetrics(MetricsResponse {
                metrics: self.metrics(request.current_wall_clock_slot),
            }),
            ApiRequest::TickExpirations(request) => {
                ApiResponse::TickExpirations(self.handle_tick_expirations_request(request))
            }
        }
    }

    /// Decodes one wire request, routes it through the alpha API, and encodes the response.
    ///
    /// # Errors
    ///
    /// Returns [`ApiCodecError`] when the request framing is truncated or structurally invalid.
    pub fn handle_api_bytes(&mut self, bytes: &[u8]) -> Result<Vec<u8>, ApiCodecError> {
        let request = decode_request(bytes)?;
        let response = self.handle_api_request(request);
        Ok(encode_response(&response))
    }

    fn handle_submit_request(&mut self, request: &SubmitRequest) -> SubmitResponse {
        match self.submit_encoded(request.request_slot, &request.payload) {
            Ok(result) => SubmitResponse::Committed(result.into()),
            Err(error) => {
                SubmitResponse::Rejected(SubmissionFailure::from_submission_error(&error))
            }
        }
    }

    fn handle_resource_request(&self, request: ResourceRequest) -> ResourceResponse {
        if let Some(error) = self.read_error(request.required_lsn) {
            return match error {
                ReadError::EngineHalted => ResourceResponse::EngineHalted,
                ReadError::RequiredLsnNotApplied {
                    required_lsn,
                    last_applied_lsn,
                } => ResourceResponse::FenceNotApplied {
                    required_lsn,
                    last_applied_lsn,
                },
            };
        }

        self.db()
            .resource(request.resource_id)
            .map_or(ResourceResponse::NotFound, |record| {
                ResourceResponse::Found(record.into())
            })
    }

    fn handle_lease_request(&self, request: LeaseRequest) -> LeaseResponse {
        if let Some(error) = self.read_error(request.required_lsn) {
            return match error {
                ReadError::EngineHalted => LeaseResponse::EngineHalted,
                ReadError::RequiredLsnNotApplied {
                    required_lsn,
                    last_applied_lsn,
                } => LeaseResponse::FenceNotApplied {
                    required_lsn,
                    last_applied_lsn,
                },
            };
        }

        match self
            .db()
            .reservation(request.lease_id, request.current_slot)
        {
            Ok(record) => LeaseResponse::Found(LeaseView::from_db(self.db(), record)),
            Err(ReservationLookupError::NotFound) => LeaseResponse::NotFound,
            Err(ReservationLookupError::Retired) => LeaseResponse::Retired,
        }
    }

    fn handle_tick_expirations_request(
        &mut self,
        request: TickExpirationsRequest,
    ) -> TickExpirationsResponse {
        match self.tick_expirations(request.current_wall_clock_slot) {
            Ok(result) => TickExpirationsResponse::Applied(result.into()),
            Err(error) => {
                TickExpirationsResponse::Rejected(SubmissionFailure::from_submission_error(&error))
            }
        }
    }

    fn read_error(&self, required_lsn: Option<Lsn>) -> Option<ReadError> {
        self.enforce_read_fence(required_lsn.unwrap_or(Lsn(0)))
            .err()
    }
}

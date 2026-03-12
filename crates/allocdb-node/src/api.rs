use allocdb_core::command::ClientRequest;
use allocdb_core::command_codec::{
    CommandCodecError, decode_client_request, encode_client_request,
};
use allocdb_core::ids::{HolderId, Lsn, ReservationId, ResourceId, Slot};
use allocdb_core::result::CommandOutcome;
use allocdb_core::{
    ReservationLookupError, ReservationRecord, ReservationState, ResourceRecord, ResourceState,
};

use crate::engine::{
    EngineMetrics, ReadError, SingleNodeEngine, SubmissionError, SubmissionErrorCategory,
    SubmissionResult,
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
    GetReservation(ReservationRequest),
    GetMetrics(MetricsRequest),
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
pub struct ReservationRequest {
    pub reservation_id: ReservationId,
    pub current_slot: Slot,
    pub required_lsn: Option<Lsn>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetricsRequest {
    pub current_wall_clock_slot: Slot,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApiResponse {
    Submit(SubmitResponse),
    GetResource(ResourceResponse),
    GetReservation(ReservationResponse),
    GetMetrics(MetricsResponse),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubmitResponse {
    Committed(SubmissionCommitted),
    Rejected(SubmissionFailure),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubmissionCommitted {
    pub applied_lsn: Lsn,
    pub outcome: CommandOutcome,
    pub from_retry_cache: bool,
}

impl From<SubmissionResult> for SubmissionCommitted {
    fn from(value: SubmissionResult) -> Self {
        Self {
            applied_lsn: value.applied_lsn,
            outcome: value.outcome,
            from_retry_cache: value.from_retry_cache,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubmissionFailure {
    pub category: SubmissionErrorCategory,
    pub code: SubmissionFailureCode,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubmissionFailureCode {
    EngineHalted,
    InvalidRequest(InvalidRequestReason),
    CommandTooLarge {
        encoded_len: u64,
        max_command_bytes: u64,
    },
    Overloaded {
        queue_depth: u32,
        queue_capacity: u32,
    },
    StorageFailure,
}

impl SubmissionFailure {
    fn from_submission_error(error: &SubmissionError) -> Self {
        let category = error.category();
        let code = match *error {
            SubmissionError::EngineHalted => SubmissionFailureCode::EngineHalted,
            SubmissionError::InvalidRequest(error) => {
                SubmissionFailureCode::InvalidRequest(InvalidRequestReason::from_codec_error(error))
            }
            SubmissionError::CommandTooLarge {
                encoded_len,
                max_command_bytes,
            } => SubmissionFailureCode::CommandTooLarge {
                encoded_len: u64::try_from(encoded_len).expect("encoded length must fit u64"),
                max_command_bytes: u64::try_from(max_command_bytes)
                    .expect("max command bytes must fit u64"),
            },
            SubmissionError::Overloaded {
                queue_depth,
                queue_capacity,
            } => SubmissionFailureCode::Overloaded {
                queue_depth,
                queue_capacity,
            },
            SubmissionError::WalFile(_) => SubmissionFailureCode::StorageFailure,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceResponse {
    Found(ResourceView),
    NotFound,
    FenceNotApplied {
        required_lsn: Lsn,
        last_applied_lsn: Option<Lsn>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResourceView {
    pub resource_id: ResourceId,
    pub state: ResourceState,
    pub current_reservation_id: Option<ReservationId>,
    pub version: u64,
}

impl From<ResourceRecord> for ResourceView {
    fn from(value: ResourceRecord) -> Self {
        Self {
            resource_id: value.resource_id,
            state: value.current_state,
            current_reservation_id: value.current_reservation_id,
            version: value.version,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReservationResponse {
    Found(ReservationView),
    NotFound,
    Retired,
    FenceNotApplied {
        required_lsn: Lsn,
        last_applied_lsn: Option<Lsn>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReservationView {
    pub reservation_id: ReservationId,
    pub resource_id: ResourceId,
    pub holder_id: HolderId,
    pub state: ReservationState,
    pub created_lsn: Lsn,
    pub deadline_slot: Slot,
    pub released_lsn: Option<Lsn>,
    pub retire_after_slot: Option<Slot>,
}

impl From<ReservationRecord> for ReservationView {
    fn from(value: ReservationRecord) -> Self {
        Self {
            reservation_id: value.reservation_id,
            resource_id: value.resource_id,
            holder_id: value.holder_id,
            state: value.state,
            created_lsn: value.created_lsn,
            deadline_slot: value.deadline_slot,
            released_lsn: value.released_lsn,
            retire_after_slot: value.retire_after_slot,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetricsResponse {
    pub metrics: EngineMetrics,
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
            ApiRequest::GetReservation(request) => {
                ApiResponse::GetReservation(self.handle_reservation_request(request))
            }
            ApiRequest::GetMetrics(request) => ApiResponse::GetMetrics(MetricsResponse {
                metrics: self.metrics(request.current_wall_clock_slot),
            }),
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
        if let Some(error) = self.read_fence_error(request.required_lsn) {
            return ResourceResponse::FenceNotApplied {
                required_lsn: error.required_lsn,
                last_applied_lsn: error.last_applied_lsn,
            };
        }

        self.db()
            .resource(request.resource_id)
            .map_or(ResourceResponse::NotFound, |record| {
                ResourceResponse::Found(record.into())
            })
    }

    fn handle_reservation_request(&self, request: ReservationRequest) -> ReservationResponse {
        if let Some(error) = self.read_fence_error(request.required_lsn) {
            return ReservationResponse::FenceNotApplied {
                required_lsn: error.required_lsn,
                last_applied_lsn: error.last_applied_lsn,
            };
        }

        match self
            .db()
            .reservation(request.reservation_id, request.current_slot)
        {
            Ok(record) => ReservationResponse::Found(record.into()),
            Err(ReservationLookupError::NotFound) => ReservationResponse::NotFound,
            Err(ReservationLookupError::Retired) => ReservationResponse::Retired,
        }
    }

    fn read_fence_error(&self, required_lsn: Option<Lsn>) -> Option<ReadFenceError> {
        let required_lsn = required_lsn?;
        match self.enforce_read_fence(required_lsn) {
            Ok(()) => None,
            Err(ReadError::RequiredLsnNotApplied {
                required_lsn,
                last_applied_lsn,
            }) => Some(ReadFenceError {
                required_lsn,
                last_applied_lsn,
            }),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ReadFenceError {
    required_lsn: Lsn,
    last_applied_lsn: Option<Lsn>,
}

use crate::ids::{ReservationId, Slot};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResultCode {
    Ok,
    Noop,
    AlreadyExists,
    ResourceTableFull,
    ResourceNotFound,
    ResourceBusy,
    TtlOutOfRange,
    ReservationTableFull,
    ReservationNotFound,
    ReservationRetired,
    ExpirationIndexFull,
    OperationTableFull,
    OperationConflict,
    InvalidState,
    HolderMismatch,
    SlotOverflow,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommandOutcome {
    pub result_code: ResultCode,
    pub reservation_id: Option<ReservationId>,
    pub deadline_slot: Option<Slot>,
}

impl CommandOutcome {
    #[must_use]
    pub const fn new(result_code: ResultCode) -> Self {
        Self {
            result_code,
            reservation_id: None,
            deadline_slot: None,
        }
    }

    #[must_use]
    pub const fn with_reservation(
        result_code: ResultCode,
        reservation_id: ReservationId,
        deadline_slot: Slot,
    ) -> Self {
        Self {
            result_code,
            reservation_id: Some(reservation_id),
            deadline_slot: Some(deadline_slot),
        }
    }
}

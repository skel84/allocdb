use crate::ids::{ReservationId, Slot};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResultCode {
    Ok,
    Noop,
    AlreadyExists,
    ResourceTableFull,
    ResourceNotFound,
    ResourceBusy,
    BundleTooLarge,
    TtlOutOfRange,
    ReservationTableFull,
    ReservationMemberTableFull,
    ReservationNotFound,
    ReservationRetired,
    ExpirationIndexFull,
    OperationTableFull,
    OperationConflict,
    InvalidState,
    HolderMismatch,
    StaleEpoch,
    SlotOverflow,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommandOutcome {
    pub result_code: ResultCode,
    pub reservation_id: Option<ReservationId>,
    pub lease_epoch: Option<u64>,
    pub deadline_slot: Option<Slot>,
}

impl CommandOutcome {
    #[must_use]
    pub const fn new(result_code: ResultCode) -> Self {
        Self {
            result_code,
            reservation_id: None,
            lease_epoch: None,
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
            lease_epoch: None,
            deadline_slot: Some(deadline_slot),
        }
    }

    #[must_use]
    pub const fn with_reservation_epoch(
        result_code: ResultCode,
        reservation_id: ReservationId,
        lease_epoch: u64,
        deadline_slot: Slot,
    ) -> Self {
        Self {
            result_code,
            reservation_id: Some(reservation_id),
            lease_epoch: Some(lease_epoch),
            deadline_slot: Some(deadline_slot),
        }
    }
}

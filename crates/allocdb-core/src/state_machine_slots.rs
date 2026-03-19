use log::warn;

use crate::command::Command;
use crate::ids::Slot;
use crate::result::{CommandOutcome, ResultCode};
use crate::state_machine::{AllocDb, SlotOverflowError, SlotOverflowKind};

impl AllocDb {
    pub(crate) fn deadline_slot(
        request_slot: Slot,
        ttl_slots: u64,
    ) -> Result<Slot, SlotOverflowError> {
        Self::checked_slot_add(request_slot, ttl_slots, SlotOverflowKind::Deadline)
    }

    pub(crate) fn reservation_retire_after_slot(
        &self,
        request_slot: Slot,
    ) -> Result<Slot, SlotOverflowError> {
        Self::checked_slot_add(
            request_slot,
            self.config.reservation_history_window_slots,
            SlotOverflowKind::ReservationHistory,
        )
    }

    pub(crate) fn operation_retire_after_slot(
        &self,
        request_slot: Slot,
    ) -> Result<Slot, SlotOverflowError> {
        Self::checked_slot_add(
            request_slot,
            self.config.operation_window_slots(),
            SlotOverflowKind::OperationWindow,
        )
    }

    pub(crate) fn slot_overflow_outcome(
        operation: &'static str,
        error: SlotOverflowError,
    ) -> CommandOutcome {
        warn!(
            "{operation} rejected slot_overflow kind={:?} request_slot={} delta={}",
            error.kind,
            error.request_slot.get(),
            error.delta,
        );
        CommandOutcome::new(ResultCode::SlotOverflow)
    }

    pub(super) fn validate_command_slot(
        &self,
        request_slot: Slot,
        command: &Command,
    ) -> Result<(), SlotOverflowError> {
        match command {
            Command::Reserve { ttl_slots, .. } | Command::ReserveBundle { ttl_slots, .. } => {
                let _ = Self::deadline_slot(request_slot, *ttl_slots)?;
            }
            Command::Release { .. } | Command::Reclaim { .. } | Command::Expire { .. } => {
                let _ = self.reservation_retire_after_slot(request_slot)?;
            }
            Command::CreateResource { .. } | Command::Confirm { .. } | Command::Revoke { .. } => {}
        }
        Ok(())
    }

    fn checked_slot_add(
        request_slot: Slot,
        delta: u64,
        kind: SlotOverflowKind,
    ) -> Result<Slot, SlotOverflowError> {
        request_slot
            .get()
            .checked_add(delta)
            .map(Slot)
            .ok_or(SlotOverflowError {
                kind,
                request_slot,
                delta,
            })
    }
}

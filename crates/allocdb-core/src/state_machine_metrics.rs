use crate::config::Config;
use crate::ids::{Lsn, OperationId, ReservationId, ResourceId, Slot};
use crate::state_machine::{
    AllocDb, OperationRecord, ReservationLookupError, ReservationRecord, ReservationState,
    ResourceRecord,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HealthMetrics {
    pub last_applied_lsn: Option<Lsn>,
    pub last_request_slot: Option<Slot>,
    pub logical_slot_lag: u64,
    pub expiration_backlog: u32,
    pub operation_table_used: u32,
    pub operation_table_capacity: u32,
    pub operation_table_utilization_pct: u8,
}

impl AllocDb {
    #[must_use]
    pub fn last_applied_lsn(&self) -> Option<Lsn> {
        self.last_applied_lsn
    }

    #[must_use]
    pub fn last_request_slot(&self) -> Option<Slot> {
        self.last_request_slot
    }

    #[must_use]
    pub fn config(&self) -> &Config {
        &self.config
    }

    #[must_use]
    pub fn resource(&self, resource_id: ResourceId) -> Option<ResourceRecord> {
        self.resources.get(resource_id).copied()
    }

    /// Looks up one reservation while respecting the bounded reservation-history window.
    ///
    /// # Errors
    ///
    /// Returns [`ReservationLookupError::NotFound`] if the reservation does not exist and
    /// [`ReservationLookupError::Retired`] if it has aged out of the configured history window.
    pub fn reservation(
        &self,
        reservation_id: ReservationId,
        current_slot: Slot,
    ) -> Result<ReservationRecord, ReservationLookupError> {
        let Some(record) = self.reservations.get(reservation_id).copied() else {
            return Err(ReservationLookupError::NotFound);
        };

        if matches!(
            record.retire_after_slot,
            Some(retire_after_slot) if current_slot.get() > retire_after_slot.get()
        ) {
            return Err(ReservationLookupError::Retired);
        }

        Ok(record)
    }

    /// Returns the stored outcome for an operation while it remains inside the dedupe window.
    #[must_use]
    pub fn operation(
        &self,
        operation_id: OperationId,
        current_slot: Slot,
    ) -> Option<OperationRecord> {
        self.operations.get(operation_id).and_then(|record| {
            if current_slot.get() > record.retire_after_slot.get() {
                None
            } else {
                Some(*record)
            }
        })
    }

    /// Returns the reservations scheduled to expire at the provided logical slot.
    #[must_use]
    pub fn due_reservations(&self, slot: Slot) -> &[ReservationId] {
        let bucket_index = self.wheel_bucket_index(slot);
        &self.wheel[bucket_index]
    }

    /// Returns the current logical lag between wall-clock slot progression and the last applied
    /// client or internal request slot.
    #[must_use]
    pub fn logical_slot_lag(&self, current_wall_clock_slot: Slot) -> u64 {
        self.last_request_slot.map_or(0, |last_request_slot| {
            current_wall_clock_slot
                .get()
                .saturating_sub(last_request_slot.get())
        })
    }

    /// Counts due reservations whose `expire` command has not yet been applied.
    ///
    /// # Panics
    ///
    /// Panics only if internal state has already violated the configured reservation bound.
    #[must_use]
    pub fn expiration_backlog(&self, current_wall_clock_slot: Slot) -> u32 {
        let backlog = self
            .reservations
            .iter()
            .filter(|record| {
                record.state == ReservationState::Reserved
                    && record.deadline_slot.get() <= current_wall_clock_slot.get()
            })
            .count();
        u32::try_from(backlog).expect("reservation backlog must fit u32")
    }

    /// Returns the current bounded health snapshot for the single-node executor.
    ///
    /// # Panics
    ///
    /// Panics only if internal state has already violated the configured reservation or operation
    /// bounds.
    #[must_use]
    pub fn health_metrics(&self, current_wall_clock_slot: Slot) -> HealthMetrics {
        let operation_table_used =
            u32::try_from(self.operations.len()).expect("operation table len must fit u32");
        let operation_table_capacity = self.config.max_operations;
        let operation_table_utilization_pct =
            u8::try_from(operation_table_used.saturating_mul(100) / operation_table_capacity)
                .expect("operation table utilization percent must fit u8");

        HealthMetrics {
            last_applied_lsn: self.last_applied_lsn,
            last_request_slot: self.last_request_slot,
            logical_slot_lag: self.logical_slot_lag(current_wall_clock_slot),
            expiration_backlog: self.expiration_backlog(current_wall_clock_slot),
            operation_table_used,
            operation_table_capacity,
            operation_table_utilization_pct,
        }
    }
}

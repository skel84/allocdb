use core::cmp::Ordering;

use crate::command::{Command, CommandContext};
use crate::config::{Config, ConfigError};
use crate::ids::{HolderId, Lsn, OperationId, ReservationId, ResourceId, Slot};
use crate::result::{CommandOutcome, ResultCode};

#[path = "state_machine_apply.rs"]
mod apply;
#[cfg(test)]
#[path = "state_machine_tests.rs"]
mod tests;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceState {
    Available,
    Reserved,
    Confirmed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReservationState {
    Reserved,
    Confirmed,
    Released,
    Expired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResourceRecord {
    pub resource_id: ResourceId,
    pub current_state: ResourceState,
    pub current_reservation_id: Option<ReservationId>,
    pub version: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReservationRecord {
    pub reservation_id: ReservationId,
    pub resource_id: ResourceId,
    pub holder_id: HolderId,
    pub state: ReservationState,
    pub created_lsn: Lsn,
    pub deadline_slot: Slot,
    pub released_lsn: Option<Lsn>,
    pub retire_after_slot: Option<Slot>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OperationRecord {
    pub operation_id: OperationId,
    pub command_fingerprint: u128,
    pub result_code: ResultCode,
    pub result_reservation_id: Option<ReservationId>,
    pub result_deadline_slot: Option<Slot>,
    pub applied_lsn: Lsn,
    pub retire_after_slot: Slot,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReservationLookupError {
    NotFound,
    Retired,
}

#[derive(Debug)]
pub struct AllocDb {
    pub(crate) config: Config,
    pub(crate) resources: Vec<ResourceRecord>,
    pub(crate) reservations: Vec<ReservationRecord>,
    pub(crate) operations: Vec<OperationRecord>,
    pub(crate) wheel: Vec<Vec<ReservationId>>,
    pub(crate) last_applied_lsn: Option<Lsn>,
    pub(crate) last_request_slot: Option<Slot>,
}

impl AllocDb {
    /// Creates an empty deterministic allocator with fixed-capacity in-memory state.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] when the supplied configuration is internally inconsistent.
    ///
    /// # Panics
    ///
    /// Panics if a validated configuration still cannot fit the platform `usize`.
    pub fn new(config: Config) -> Result<Self, ConfigError> {
        config.validate()?;

        let mut wheel = Vec::with_capacity(config.wheel_len());
        let bucket_capacity = usize::try_from(config.max_expiration_bucket_len)
            .expect("validated max_expiration_bucket_len must fit usize");

        for _ in 0..config.wheel_len() {
            wheel.push(Vec::with_capacity(bucket_capacity));
        }

        Ok(Self {
            resources: Vec::with_capacity(
                usize::try_from(config.max_resources)
                    .expect("validated max_resources must fit usize"),
            ),
            reservations: Vec::with_capacity(
                usize::try_from(config.max_reservations)
                    .expect("validated max_reservations must fit usize"),
            ),
            operations: Vec::with_capacity(
                usize::try_from(config.max_operations)
                    .expect("validated max_operations must fit usize"),
            ),
            wheel,
            config,
            last_applied_lsn: None,
            last_request_slot: None,
        })
    }

    #[must_use]
    pub fn config(&self) -> &Config {
        &self.config
    }

    #[must_use]
    pub fn resource(&self, resource_id: ResourceId) -> Option<ResourceRecord> {
        self.resource_index(resource_id)
            .map(|index| self.resources[index])
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
        let Some(index) = self.reservation_index(reservation_id) else {
            return Err(ReservationLookupError::NotFound);
        };

        let record = self.reservations[index];
        if Self::reservation_is_retired(record, current_slot) {
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
        self.operation_index(operation_id).and_then(|index| {
            let record = self.operations[index];
            if current_slot.get() > record.retire_after_slot.get() {
                None
            } else {
                Some(record)
            }
        })
    }

    /// Returns the reservations scheduled to expire at the provided logical slot.
    #[must_use]
    pub fn due_reservations(&self, slot: Slot) -> &[ReservationId] {
        let bucket_index = self.wheel_bucket_index(slot);
        &self.wheel[bucket_index]
    }

    fn apply_command(&mut self, context: CommandContext, command: Command) -> CommandOutcome {
        match command {
            Command::CreateResource { resource_id } => self.apply_create_resource(resource_id),
            Command::Reserve {
                resource_id,
                holder_id,
                ttl_slots,
            } => self.apply_reserve(context, resource_id, holder_id, ttl_slots),
            Command::Confirm {
                reservation_id,
                holder_id,
            } => self.apply_confirm(context, reservation_id, holder_id),
            Command::Release {
                reservation_id,
                holder_id,
            } => self.apply_release(context, reservation_id, holder_id),
            Command::Expire {
                reservation_id,
                deadline_slot,
            } => self.apply_expire(context, reservation_id, deadline_slot),
        }
    }

    fn retire_state(&mut self, current_slot: Slot) {
        self.retire_reservations(current_slot);
        self.retire_operations(current_slot);
    }

    fn retire_reservations(&mut self, current_slot: Slot) {
        self.reservations.retain(|record| {
            !matches!(record.retire_after_slot, Some(slot) if current_slot.get() > slot.get())
        });
    }

    fn retire_operations(&mut self, current_slot: Slot) {
        self.operations
            .retain(|record| current_slot.get() <= record.retire_after_slot.get());
    }

    fn reservation_is_retired(record: ReservationRecord, current_slot: Slot) -> bool {
        matches!(record.retire_after_slot, Some(slot) if current_slot.get() > slot.get())
    }

    fn reservation_id_from_lsn(&self, lsn: Lsn) -> ReservationId {
        ReservationId((u128::from(self.config.shard_id) << 64) | u128::from(lsn.get()))
    }

    fn wheel_bucket_index(&self, slot: Slot) -> usize {
        let wheel_len_u64 = u64::try_from(self.wheel.len()).expect("wheel length must fit in u64");
        let bucket_index = slot.get() % wheel_len_u64;
        usize::try_from(bucket_index).expect("bucket index must fit usize")
    }

    fn schedule_expiration(&mut self, reservation_id: ReservationId, deadline_slot: Slot) -> bool {
        let bucket_index = self.wheel_bucket_index(deadline_slot);
        let bucket = &mut self.wheel[bucket_index];
        if bucket.len()
            == usize::try_from(self.config.max_expiration_bucket_len)
                .expect("validated max_expiration_bucket_len")
        {
            return false;
        }

        let insertion_point = bucket.partition_point(|existing| *existing < reservation_id);
        bucket.insert(insertion_point, reservation_id);
        true
    }

    fn unschedule_expiration(
        &mut self,
        reservation_id: ReservationId,
        deadline_slot: Slot,
    ) -> bool {
        let bucket_index = self.wheel_bucket_index(deadline_slot);
        let bucket = &mut self.wheel[bucket_index];
        match bucket.binary_search(&reservation_id) {
            Ok(index) => {
                bucket.remove(index);
                true
            }
            Err(_) => false,
        }
    }

    fn resource_index(&self, resource_id: ResourceId) -> Option<usize> {
        self.resources
            .binary_search_by_key(&resource_id, |record| record.resource_id)
            .ok()
    }

    fn reservation_index(&self, reservation_id: ReservationId) -> Option<usize> {
        self.reservations
            .binary_search_by_key(&reservation_id, |record| record.reservation_id)
            .ok()
    }

    fn operation_index(&self, operation_id: OperationId) -> Option<usize> {
        self.operations
            .binary_search_by_key(&operation_id, |record| record.operation_id)
            .ok()
    }

    fn insert_resource(&mut self, record: ResourceRecord) {
        let insertion_point = self
            .resources
            .partition_point(|existing| existing.resource_id < record.resource_id);
        self.resources.insert(insertion_point, record);
    }

    fn insert_reservation(&mut self, record: ReservationRecord) {
        let insertion_point = self
            .reservations
            .partition_point(|existing| existing.reservation_id < record.reservation_id);
        self.reservations.insert(insertion_point, record);
    }

    fn insert_operation(&mut self, record: OperationRecord) {
        let insertion_point = self
            .operations
            .partition_point(|existing| existing.operation_id < record.operation_id);
        self.operations.insert(insertion_point, record);
    }

    pub(crate) fn assert_invariants(&self) {
        for resource in &self.resources {
            match resource.current_state {
                ResourceState::Available => assert!(resource.current_reservation_id.is_none()),
                ResourceState::Reserved | ResourceState::Confirmed => {
                    let reservation_id = resource
                        .current_reservation_id
                        .expect("non-available resources must reference an active reservation");
                    let reservation = self
                        .reservation_index(reservation_id)
                        .map(|index| self.reservations[index])
                        .expect("active resource reservation must exist");
                    assert_eq!(reservation.resource_id, resource.resource_id);
                    match resource.current_state {
                        ResourceState::Reserved => {
                            assert_eq!(reservation.state, ReservationState::Reserved);
                        }
                        ResourceState::Confirmed => {
                            assert_eq!(reservation.state, ReservationState::Confirmed);
                        }
                        ResourceState::Available => unreachable!(),
                    }
                }
            }
        }

        for reservation in &self.reservations {
            match reservation.state {
                ReservationState::Reserved => {
                    let resource = self
                        .resource_index(reservation.resource_id)
                        .map(|index| self.resources[index])
                        .expect("reserved reservation resource must exist");
                    assert_eq!(resource.current_state, ResourceState::Reserved);
                    assert_eq!(
                        resource.current_reservation_id,
                        Some(reservation.reservation_id)
                    );

                    let bucket_index = self.wheel_bucket_index(reservation.deadline_slot);
                    let scheduled = self.wheel[bucket_index]
                        .binary_search(&reservation.reservation_id)
                        .is_ok();
                    assert!(scheduled, "reserved reservations must be scheduled");
                }
                ReservationState::Confirmed => {
                    let resource = self
                        .resource_index(reservation.resource_id)
                        .map(|index| self.resources[index])
                        .expect("confirmed reservation resource must exist");
                    assert_eq!(resource.current_state, ResourceState::Confirmed);
                    assert_eq!(
                        resource.current_reservation_id,
                        Some(reservation.reservation_id)
                    );

                    let bucket_index = self.wheel_bucket_index(reservation.deadline_slot);
                    let scheduled = self.wheel[bucket_index]
                        .binary_search(&reservation.reservation_id)
                        .is_ok();
                    assert!(!scheduled, "confirmed reservations must not stay scheduled");
                }
                ReservationState::Released | ReservationState::Expired => {
                    assert!(reservation.retire_after_slot.is_some());
                    let resource = self
                        .resource_index(reservation.resource_id)
                        .map(|index| self.resources[index])
                        .expect("terminal reservation resource must exist");
                    assert!(
                        resource.current_reservation_id != Some(reservation.reservation_id),
                        "terminal reservations must not stay active on the resource"
                    );

                    let bucket_index = self.wheel_bucket_index(reservation.deadline_slot);
                    let scheduled = self.wheel[bucket_index]
                        .binary_search(&reservation.reservation_id)
                        .is_ok();
                    assert!(!scheduled, "terminal reservations must not stay scheduled");
                }
            }
        }

        for bucket in &self.wheel {
            for pair in bucket.windows(2) {
                assert_eq!(
                    pair[0].cmp(&pair[1]),
                    Ordering::Less,
                    "wheel buckets must stay strictly ordered"
                );
            }
        }
    }
}

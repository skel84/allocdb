use core::cmp::Ordering;

use crate::command::{Command, CommandContext};
use crate::config::{Config, ConfigError};
use crate::fixed_map::{FixedMap, FixedMapError};
use crate::ids::{HolderId, Lsn, OperationId, ReservationId, ResourceId, Slot};
use crate::result::{CommandOutcome, ResultCode};
use crate::retire_queue::{RetireEntry, RetireQueue, RetireQueueError};

#[path = "state_machine_apply.rs"]
mod apply;
#[path = "state_machine_execution.rs"]
mod execution;
#[cfg(test)]
#[path = "state_machine_issue_32_tests.rs"]
mod issue_32_tests;
#[cfg(test)]
#[path = "state_machine_issue_33_tests.rs"]
mod issue_33_tests;
#[path = "state_machine_metrics.rs"]
mod metrics;
#[cfg(test)]
#[path = "state_machine_observe_tests.rs"]
mod observe_tests;
#[path = "state_machine_retire.rs"]
mod retire;
#[cfg(test)]
#[path = "state_machine_tests.rs"]
mod tests;
pub use metrics::HealthMetrics;

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
    pub(crate) resources: FixedMap<ResourceId, ResourceRecord>,
    pub(crate) reservations: FixedMap<ReservationId, ReservationRecord>,
    pub(crate) operations: FixedMap<OperationId, OperationRecord>,
    pub(crate) max_retired_reservation_id: Option<ReservationId>,
    pub(crate) reservation_retire_queue: RetireQueue<ReservationId>,
    pub(crate) operation_retire_queue: RetireQueue<OperationId>,
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
            resources: FixedMap::with_capacity(
                usize::try_from(config.max_resources)
                    .expect("validated max_resources must fit usize"),
            ),
            reservations: FixedMap::with_capacity(
                usize::try_from(config.max_reservations)
                    .expect("validated max_reservations must fit usize"),
            ),
            operations: FixedMap::with_capacity(
                usize::try_from(config.max_operations)
                    .expect("validated max_operations must fit usize"),
            ),
            reservation_retire_queue: RetireQueue::with_capacity(
                usize::try_from(config.max_reservations)
                    .expect("validated max_reservations must fit usize"),
            ),
            operation_retire_queue: RetireQueue::with_capacity(
                usize::try_from(config.max_operations)
                    .expect("validated max_operations must fit usize"),
            ),
            wheel,
            config,
            max_retired_reservation_id: None,
            last_applied_lsn: None,
            last_request_slot: None,
        })
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

    pub(crate) fn insert_resource(&mut self, record: ResourceRecord) {
        match self.resources.insert(record.resource_id, record) {
            Ok(()) => {}
            Err(FixedMapError::DuplicateKey | FixedMapError::Full) => {
                panic!("resource inserts must respect capacity and uniqueness")
            }
        }
    }

    pub(crate) fn insert_reservation(&mut self, record: ReservationRecord) {
        match self.reservations.insert(record.reservation_id, record) {
            Ok(()) => {}
            Err(FixedMapError::DuplicateKey | FixedMapError::Full) => {
                panic!("reservation inserts must respect capacity and uniqueness")
            }
        }
    }

    pub(crate) fn insert_operation(&mut self, record: OperationRecord) {
        match self.operations.insert(record.operation_id, record) {
            Ok(()) => {}
            Err(FixedMapError::DuplicateKey | FixedMapError::Full) => {
                panic!("operation inserts must respect capacity and uniqueness")
            }
        }
    }

    pub(crate) fn push_reservation_retirement(
        &mut self,
        reservation_id: ReservationId,
        retire_after_slot: Slot,
    ) {
        match self.reservation_retire_queue.push(RetireEntry {
            key: reservation_id,
            retire_after_slot,
        }) {
            Ok(()) => {}
            Err(RetireQueueError::Full) => {
                panic!("reservation retire queue must stay within reservation capacity")
            }
        }
    }

    pub(crate) fn push_operation_retirement(
        &mut self,
        operation_id: OperationId,
        retire_after_slot: Slot,
    ) {
        match self.operation_retire_queue.push(RetireEntry {
            key: operation_id,
            retire_after_slot,
        }) {
            Ok(()) => {}
            Err(RetireQueueError::Full) => {
                panic!("operation retire queue must stay within operation capacity")
            }
        }
    }

    pub(crate) fn record_retired_reservation_id(&mut self, reservation_id: ReservationId) {
        match self.max_retired_reservation_id {
            Some(current_max) if reservation_id <= current_max => {}
            _ => self.max_retired_reservation_id = Some(reservation_id),
        }
    }

    pub(crate) fn retired_reservation_lookup_contains(
        &self,
        reservation_id: ReservationId,
    ) -> bool {
        let Some(max_retired_reservation_id) = self.max_retired_reservation_id else {
            return false;
        };

        let reservation_shard_id =
            u64::try_from(reservation_id.get() >> 64).expect("reservation shard id must fit u64");
        if reservation_shard_id != self.config.shard_id {
            return false;
        }

        reservation_id <= max_retired_reservation_id
    }

    pub(crate) fn assert_invariants(&self) {
        for resource in self.resources.iter() {
            match resource.current_state {
                ResourceState::Available => assert!(resource.current_reservation_id.is_none()),
                ResourceState::Reserved | ResourceState::Confirmed => {
                    let reservation_id = resource
                        .current_reservation_id
                        .expect("non-available resources must reference an active reservation");
                    let reservation = self
                        .reservations
                        .get(reservation_id)
                        .copied()
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

        for reservation in self.reservations.iter() {
            match reservation.state {
                ReservationState::Reserved => {
                    let resource = self
                        .resources
                        .get(reservation.resource_id)
                        .copied()
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
                        .resources
                        .get(reservation.resource_id)
                        .copied()
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
                        .resources
                        .get(reservation.resource_id)
                        .copied()
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

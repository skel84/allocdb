use core::cmp::Ordering;

use log::{debug, warn};

use crate::command::{ClientRequest, Command, CommandContext};
use crate::config::{Config, ConfigError};
use crate::ids::{HolderId, Lsn, OperationId, ReservationId, ResourceId, Slot};
use crate::result::{CommandOutcome, ResultCode};

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
    config: Config,
    resources: Vec<ResourceRecord>,
    reservations: Vec<ReservationRecord>,
    operations: Vec<OperationRecord>,
    wheel: Vec<Vec<ReservationId>>,
    last_applied_lsn: Option<Lsn>,
    last_request_slot: Option<Slot>,
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

    /// Applies one client-visible command and stores its outcome for idempotent retry handling.
    ///
    /// # Panics
    ///
    /// Panics if log sequence numbers or request slots move backwards, or if existing state has
    /// already violated internal invariants.
    pub fn apply_client(
        &mut self,
        context: CommandContext,
        request: ClientRequest,
    ) -> CommandOutcome {
        self.begin_apply(context);
        self.retire_state(context.request_slot);

        let fingerprint = request.command.fingerprint();
        if let Some(index) = self.operation_index(request.operation_id) {
            let record = self.operations[index];
            if context.request_slot.get() > record.retire_after_slot.get() {
                self.operations.remove(index);
            } else if record.command_fingerprint == fingerprint {
                debug!(
                    "returning stored outcome for operation_id={} result_code={:?}",
                    request.operation_id.get(),
                    record.result_code
                );
                return CommandOutcome {
                    result_code: record.result_code,
                    reservation_id: record.result_reservation_id,
                    deadline_slot: record.result_deadline_slot,
                };
            } else {
                warn!(
                    "operation_id conflict detected operation_id={}",
                    request.operation_id.get()
                );
                return CommandOutcome::new(ResultCode::OperationConflict);
            }
        }

        if self.operations.len()
            == usize::try_from(self.config.max_operations).expect("validated max_operations")
        {
            warn!("operation table is full");
            return CommandOutcome::new(ResultCode::OperationTableFull);
        }

        let outcome = self.apply_command(context, request.command);
        let operation_record = OperationRecord {
            operation_id: request.operation_id,
            command_fingerprint: fingerprint,
            result_code: outcome.result_code,
            result_reservation_id: outcome.reservation_id,
            result_deadline_slot: outcome.deadline_slot,
            applied_lsn: context.lsn,
            retire_after_slot: Slot(
                context.request_slot.get() + self.config.operation_window_slots(),
            ),
        };
        self.insert_operation(operation_record);
        self.assert_invariants();
        outcome
    }

    /// Applies one internal command that is already part of the deterministic execution path.
    ///
    /// # Panics
    ///
    /// Panics if log sequence numbers or request slots move backwards, or if existing state has
    /// already violated internal invariants.
    pub fn apply_internal(&mut self, context: CommandContext, command: Command) -> CommandOutcome {
        self.begin_apply(context);
        self.retire_state(context.request_slot);
        let outcome = self.apply_command(context, command);
        self.assert_invariants();
        outcome
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

    fn apply_create_resource(&mut self, resource_id: ResourceId) -> CommandOutcome {
        if self.resource_index(resource_id).is_some() {
            warn!(
                "create_resource rejected already_exists resource_id={}",
                resource_id.get()
            );
            return CommandOutcome::new(ResultCode::AlreadyExists);
        }

        if self.resources.len()
            == usize::try_from(self.config.max_resources).expect("validated max_resources")
        {
            warn!("resource table is full");
            return CommandOutcome::new(ResultCode::ResourceTableFull);
        }

        self.insert_resource(ResourceRecord {
            resource_id,
            current_state: ResourceState::Available,
            current_reservation_id: None,
            version: 0,
        });
        debug!("created resource resource_id={}", resource_id.get());
        CommandOutcome::new(ResultCode::Ok)
    }

    fn apply_reserve(
        &mut self,
        context: CommandContext,
        resource_id: ResourceId,
        holder_id: HolderId,
        ttl_slots: u64,
    ) -> CommandOutcome {
        if ttl_slots == 0 || ttl_slots > self.config.max_ttl_slots {
            warn!(
                "reserve rejected ttl_out_of_range resource_id={} ttl_slots={}",
                resource_id.get(),
                ttl_slots
            );
            return CommandOutcome::new(ResultCode::TtlOutOfRange);
        }

        let Some(resource_index) = self.resource_index(resource_id) else {
            warn!(
                "reserve rejected resource_not_found resource_id={}",
                resource_id.get()
            );
            return CommandOutcome::new(ResultCode::ResourceNotFound);
        };

        let resource = self.resources[resource_index];
        if resource.current_state != ResourceState::Available {
            warn!(
                "reserve rejected resource_busy resource_id={}",
                resource_id.get()
            );
            return CommandOutcome::new(ResultCode::ResourceBusy);
        }

        if self.reservations.len()
            == usize::try_from(self.config.max_reservations).expect("validated max_reservations")
        {
            warn!("reservation table is full");
            return CommandOutcome::new(ResultCode::ReservationTableFull);
        }

        let reservation_id = self.reservation_id_from_lsn(context.lsn);
        let deadline_slot = Slot(context.request_slot.get() + ttl_slots);

        if !self.schedule_expiration(reservation_id, deadline_slot) {
            warn!(
                "reserve rejected expiration_index_full resource_id={} deadline_slot={}",
                resource_id.get(),
                deadline_slot.get()
            );
            return CommandOutcome::new(ResultCode::ExpirationIndexFull);
        }

        self.insert_reservation(ReservationRecord {
            reservation_id,
            resource_id,
            holder_id,
            state: ReservationState::Reserved,
            created_lsn: context.lsn,
            deadline_slot,
            released_lsn: None,
            retire_after_slot: None,
        });

        let resource = &mut self.resources[resource_index];
        resource.current_state = ResourceState::Reserved;
        resource.current_reservation_id = Some(reservation_id);
        resource.version += 1;

        debug!(
            "reserved resource_id={} reservation_id={} deadline_slot={}",
            resource_id.get(),
            reservation_id.get(),
            deadline_slot.get()
        );
        CommandOutcome::with_reservation(ResultCode::Ok, reservation_id, deadline_slot)
    }

    fn apply_confirm(
        &mut self,
        _context: CommandContext,
        reservation_id: ReservationId,
        holder_id: HolderId,
    ) -> CommandOutcome {
        let Some(reservation_index) = self.reservation_index(reservation_id) else {
            warn!(
                "confirm rejected reservation_not_found reservation_id={}",
                reservation_id.get()
            );
            return CommandOutcome::new(ResultCode::ReservationNotFound);
        };

        let reservation = self.reservations[reservation_index];
        if reservation.retire_after_slot.is_some() {
            warn!(
                "confirm rejected reservation_retired reservation_id={}",
                reservation_id.get()
            );
            return CommandOutcome::new(ResultCode::ReservationRetired);
        }

        if reservation.holder_id != holder_id {
            warn!(
                "confirm rejected holder_mismatch reservation_id={} holder_id={}",
                reservation_id.get(),
                holder_id.get()
            );
            return CommandOutcome::new(ResultCode::HolderMismatch);
        }

        if reservation.state != ReservationState::Reserved {
            warn!(
                "confirm rejected invalid_state reservation_id={} state={:?}",
                reservation_id.get(),
                reservation.state
            );
            return CommandOutcome::new(ResultCode::InvalidState);
        }

        let Some(resource_index) = self.resource_index(reservation.resource_id) else {
            panic!("active reservation must reference an existing resource");
        };

        let removed = self.unschedule_expiration(reservation_id, reservation.deadline_slot);
        assert!(
            removed,
            "reserved reservations must stay scheduled for expiration"
        );

        let reservation = &mut self.reservations[reservation_index];
        reservation.state = ReservationState::Confirmed;

        let resource = &mut self.resources[resource_index];
        resource.current_state = ResourceState::Confirmed;
        resource.version += 1;

        debug!("confirmed reservation_id={}", reservation_id.get());
        CommandOutcome::new(ResultCode::Ok)
    }

    fn apply_release(
        &mut self,
        context: CommandContext,
        reservation_id: ReservationId,
        holder_id: HolderId,
    ) -> CommandOutcome {
        let Some(reservation_index) = self.reservation_index(reservation_id) else {
            warn!(
                "release rejected reservation_not_found reservation_id={}",
                reservation_id.get()
            );
            return CommandOutcome::new(ResultCode::ReservationNotFound);
        };

        let reservation = self.reservations[reservation_index];
        if reservation.retire_after_slot.is_some() {
            warn!(
                "release rejected reservation_retired reservation_id={}",
                reservation_id.get()
            );
            return CommandOutcome::new(ResultCode::ReservationRetired);
        }

        if reservation.holder_id != holder_id {
            warn!(
                "release rejected holder_mismatch reservation_id={} holder_id={}",
                reservation_id.get(),
                holder_id.get()
            );
            return CommandOutcome::new(ResultCode::HolderMismatch);
        }

        match reservation.state {
            ReservationState::Reserved => {
                let removed = self.unschedule_expiration(reservation_id, reservation.deadline_slot);
                assert!(
                    removed,
                    "reserved reservations must stay scheduled for expiration"
                );
            }
            ReservationState::Confirmed => {}
            ReservationState::Released | ReservationState::Expired => {
                warn!(
                    "release rejected invalid_state reservation_id={} state={:?}",
                    reservation_id.get(),
                    reservation.state
                );
                return CommandOutcome::new(ResultCode::InvalidState);
            }
        }

        let Some(resource_index) = self.resource_index(reservation.resource_id) else {
            panic!("active reservation must reference an existing resource");
        };

        let retire_after_slot =
            Slot(context.request_slot.get() + self.config.reservation_history_window_slots);

        let reservation = &mut self.reservations[reservation_index];
        reservation.state = ReservationState::Released;
        reservation.released_lsn = Some(context.lsn);
        reservation.retire_after_slot = Some(retire_after_slot);

        let resource = &mut self.resources[resource_index];
        resource.current_state = ResourceState::Available;
        resource.current_reservation_id = None;
        resource.version += 1;

        debug!("released reservation_id={}", reservation_id.get());
        CommandOutcome::new(ResultCode::Ok)
    }

    fn apply_expire(
        &mut self,
        context: CommandContext,
        reservation_id: ReservationId,
        deadline_slot: Slot,
    ) -> CommandOutcome {
        let Some(reservation_index) = self.reservation_index(reservation_id) else {
            warn!(
                "expire rejected reservation_not_found reservation_id={}",
                reservation_id.get()
            );
            return CommandOutcome::new(ResultCode::ReservationNotFound);
        };

        let reservation = self.reservations[reservation_index];
        if reservation.deadline_slot != deadline_slot {
            debug!(
                "expire noop due to deadline mismatch reservation_id={} expected={} actual={}",
                reservation_id.get(),
                deadline_slot.get(),
                reservation.deadline_slot.get()
            );
            return CommandOutcome::new(ResultCode::Noop);
        }

        match reservation.state {
            ReservationState::Confirmed
            | ReservationState::Released
            | ReservationState::Expired => {
                debug!(
                    "expire noop for terminal/non-reserved reservation_id={} state={:?}",
                    reservation_id.get(),
                    reservation.state
                );
                return CommandOutcome::new(ResultCode::Noop);
            }
            ReservationState::Reserved => {}
        }

        let removed = self.unschedule_expiration(reservation_id, reservation.deadline_slot);
        assert!(
            removed,
            "reserved reservations must stay scheduled for expiration"
        );

        let Some(resource_index) = self.resource_index(reservation.resource_id) else {
            panic!("active reservation must reference an existing resource");
        };

        let retire_after_slot =
            Slot(context.request_slot.get() + self.config.reservation_history_window_slots);

        let reservation = &mut self.reservations[reservation_index];
        reservation.state = ReservationState::Expired;
        reservation.released_lsn = Some(context.lsn);
        reservation.retire_after_slot = Some(retire_after_slot);

        let resource = &mut self.resources[resource_index];
        resource.current_state = ResourceState::Available;
        resource.current_reservation_id = None;
        resource.version += 1;

        debug!("expired reservation_id={}", reservation_id.get());
        CommandOutcome::new(ResultCode::Ok)
    }

    fn begin_apply(&mut self, context: CommandContext) {
        if let Some(last_lsn) = self.last_applied_lsn {
            assert!(
                context.lsn.get() > last_lsn.get(),
                "applied LSNs must increase strictly"
            );
        }

        if let Some(last_slot) = self.last_request_slot {
            assert!(
                context.request_slot.get() >= last_slot.get(),
                "request slots must not move backwards"
            );
        }

        self.last_applied_lsn = Some(context.lsn);
        self.last_request_slot = Some(context.request_slot);
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

    fn assert_invariants(&self) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::ClientRequest;
    use crate::ids::{ClientId, HolderId, OperationId, ResourceId, Slot};

    fn config() -> Config {
        Config {
            shard_id: 0,
            max_resources: 8,
            max_reservations: 8,
            max_operations: 16,
            max_ttl_slots: 16,
            max_client_retry_window_slots: 8,
            reservation_history_window_slots: 4,
            max_expiration_bucket_len: 8,
        }
    }

    fn context(lsn: u64, request_slot: u64) -> CommandContext {
        CommandContext {
            lsn: Lsn(lsn),
            request_slot: Slot(request_slot),
        }
    }

    fn create(resource_id: u128) -> ClientRequest {
        ClientRequest {
            operation_id: OperationId(resource_id),
            client_id: ClientId(7),
            command: Command::CreateResource {
                resource_id: ResourceId(resource_id),
            },
        }
    }

    #[test]
    fn config_validation_rejects_invalid_history_window() {
        let mut invalid = config();
        invalid.reservation_history_window_slots = invalid.max_ttl_slots + 1;
        assert_eq!(invalid.validate(), Err(ConfigError::HistoryWindowTooLarge));
    }

    #[test]
    fn create_resource_is_idempotent_with_same_operation_id() {
        let mut db = AllocDb::new(config()).unwrap();

        let first = db.apply_client(context(1, 1), create(11));
        let second = db.apply_client(context(2, 1), create(11));

        assert_eq!(first.result_code, ResultCode::Ok);
        assert_eq!(second.result_code, ResultCode::Ok);
        assert_eq!(db.resources.len(), 1);
    }

    #[test]
    fn create_resource_rejects_conflicting_reuse_of_operation_id() {
        let mut db = AllocDb::new(config()).unwrap();

        let first = db.apply_client(context(1, 1), create(11));
        let conflicting = db.apply_client(
            context(2, 1),
            ClientRequest {
                operation_id: OperationId(11),
                client_id: ClientId(7),
                command: Command::CreateResource {
                    resource_id: ResourceId(22),
                },
            },
        );

        assert_eq!(first.result_code, ResultCode::Ok);
        assert_eq!(conflicting.result_code, ResultCode::OperationConflict);
        assert!(db.resource(ResourceId(22)).is_none());
    }

    #[test]
    fn reserve_assigns_deterministic_reservation_id_and_deadline() {
        let mut db = AllocDb::new(config()).unwrap();
        db.apply_client(context(1, 1), create(11));

        let outcome = db.apply_client(
            context(2, 10),
            ClientRequest {
                operation_id: OperationId(2),
                client_id: ClientId(7),
                command: Command::Reserve {
                    resource_id: ResourceId(11),
                    holder_id: HolderId(99),
                    ttl_slots: 3,
                },
            },
        );

        assert_eq!(outcome.result_code, ResultCode::Ok);
        assert_eq!(outcome.reservation_id, Some(ReservationId(2)));
        assert_eq!(outcome.deadline_slot, Some(Slot(13)));

        let resource = db.resource(ResourceId(11)).unwrap();
        assert_eq!(resource.current_state, ResourceState::Reserved);
        assert_eq!(resource.current_reservation_id, Some(ReservationId(2)));
    }

    #[test]
    fn reserve_rejects_busy_resource() {
        let mut db = AllocDb::new(config()).unwrap();
        db.apply_client(context(1, 1), create(11));
        let first = db.apply_client(
            context(2, 2),
            ClientRequest {
                operation_id: OperationId(2),
                client_id: ClientId(7),
                command: Command::Reserve {
                    resource_id: ResourceId(11),
                    holder_id: HolderId(1),
                    ttl_slots: 3,
                },
            },
        );
        let second = db.apply_client(
            context(3, 2),
            ClientRequest {
                operation_id: OperationId(3),
                client_id: ClientId(8),
                command: Command::Reserve {
                    resource_id: ResourceId(11),
                    holder_id: HolderId(2),
                    ttl_slots: 3,
                },
            },
        );

        assert_eq!(first.result_code, ResultCode::Ok);
        assert_eq!(second.result_code, ResultCode::ResourceBusy);
    }

    #[test]
    fn confirm_requires_matching_holder() {
        let mut db = AllocDb::new(config()).unwrap();
        db.apply_client(context(1, 1), create(11));
        db.apply_client(
            context(2, 2),
            ClientRequest {
                operation_id: OperationId(2),
                client_id: ClientId(7),
                command: Command::Reserve {
                    resource_id: ResourceId(11),
                    holder_id: HolderId(1),
                    ttl_slots: 3,
                },
            },
        );

        let wrong_holder = db.apply_client(
            context(3, 2),
            ClientRequest {
                operation_id: OperationId(3),
                client_id: ClientId(7),
                command: Command::Confirm {
                    reservation_id: ReservationId(2),
                    holder_id: HolderId(2),
                },
            },
        );
        let correct_holder = db.apply_client(
            context(4, 2),
            ClientRequest {
                operation_id: OperationId(4),
                client_id: ClientId(7),
                command: Command::Confirm {
                    reservation_id: ReservationId(2),
                    holder_id: HolderId(1),
                },
            },
        );

        assert_eq!(wrong_holder.result_code, ResultCode::HolderMismatch);
        assert_eq!(correct_holder.result_code, ResultCode::Ok);
        assert_eq!(
            db.resource(ResourceId(11)).unwrap().current_state,
            ResourceState::Confirmed
        );
    }

    #[test]
    fn release_returns_resource_to_available_and_retains_history() {
        let mut db = AllocDb::new(config()).unwrap();
        db.apply_client(context(1, 1), create(11));
        db.apply_client(
            context(2, 2),
            ClientRequest {
                operation_id: OperationId(2),
                client_id: ClientId(7),
                command: Command::Reserve {
                    resource_id: ResourceId(11),
                    holder_id: HolderId(1),
                    ttl_slots: 3,
                },
            },
        );

        let outcome = db.apply_client(
            context(3, 2),
            ClientRequest {
                operation_id: OperationId(3),
                client_id: ClientId(7),
                command: Command::Release {
                    reservation_id: ReservationId(2),
                    holder_id: HolderId(1),
                },
            },
        );

        assert_eq!(outcome.result_code, ResultCode::Ok);
        assert_eq!(
            db.resource(ResourceId(11)).unwrap().current_state,
            ResourceState::Available
        );
        assert_eq!(
            db.reservation(ReservationId(2), Slot(5)).unwrap().state,
            ReservationState::Released
        );
    }

    #[test]
    fn expire_is_noop_after_confirm() {
        let mut db = AllocDb::new(config()).unwrap();
        db.apply_client(context(1, 1), create(11));
        db.apply_client(
            context(2, 2),
            ClientRequest {
                operation_id: OperationId(2),
                client_id: ClientId(7),
                command: Command::Reserve {
                    resource_id: ResourceId(11),
                    holder_id: HolderId(1),
                    ttl_slots: 3,
                },
            },
        );
        db.apply_client(
            context(3, 2),
            ClientRequest {
                operation_id: OperationId(3),
                client_id: ClientId(7),
                command: Command::Confirm {
                    reservation_id: ReservationId(2),
                    holder_id: HolderId(1),
                },
            },
        );

        let outcome = db.apply_internal(
            context(4, 5),
            Command::Expire {
                reservation_id: ReservationId(2),
                deadline_slot: Slot(5),
            },
        );

        assert_eq!(outcome.result_code, ResultCode::Noop);
        assert_eq!(
            db.resource(ResourceId(11)).unwrap().current_state,
            ResourceState::Confirmed
        );
    }

    #[test]
    fn expire_releases_reserved_resource() {
        let mut db = AllocDb::new(config()).unwrap();
        db.apply_client(context(1, 1), create(11));
        db.apply_client(
            context(2, 2),
            ClientRequest {
                operation_id: OperationId(2),
                client_id: ClientId(7),
                command: Command::Reserve {
                    resource_id: ResourceId(11),
                    holder_id: HolderId(1),
                    ttl_slots: 3,
                },
            },
        );

        let outcome = db.apply_internal(
            context(3, 5),
            Command::Expire {
                reservation_id: ReservationId(2),
                deadline_slot: Slot(5),
            },
        );

        assert_eq!(outcome.result_code, ResultCode::Ok);
        assert_eq!(
            db.resource(ResourceId(11)).unwrap().current_state,
            ResourceState::Available
        );
        assert_eq!(
            db.reservation(ReservationId(2), Slot(5)).unwrap().state,
            ReservationState::Expired
        );
    }

    #[test]
    fn reservation_history_retires_after_window() {
        let mut db = AllocDb::new(config()).unwrap();
        db.apply_client(context(1, 1), create(11));
        db.apply_client(
            context(2, 2),
            ClientRequest {
                operation_id: OperationId(2),
                client_id: ClientId(7),
                command: Command::Reserve {
                    resource_id: ResourceId(11),
                    holder_id: HolderId(1),
                    ttl_slots: 3,
                },
            },
        );
        db.apply_client(
            context(3, 2),
            ClientRequest {
                operation_id: OperationId(3),
                client_id: ClientId(7),
                command: Command::Release {
                    reservation_id: ReservationId(2),
                    holder_id: HolderId(1),
                },
            },
        );

        assert_eq!(
            db.reservation(ReservationId(2), Slot(6)).unwrap().state,
            ReservationState::Released
        );

        db.apply_client(context(4, 7), create(12));

        assert_eq!(
            db.reservation(ReservationId(2), Slot(7)),
            Err(ReservationLookupError::NotFound)
        );
    }

    #[test]
    fn operation_dedupe_expires_after_window() {
        let mut db = AllocDb::new(config()).unwrap();

        let first = db.apply_client(context(1, 1), create(11));
        assert_eq!(first.result_code, ResultCode::Ok);
        assert!(db.operation(OperationId(11), Slot(1)).is_some());

        db.apply_client(context(2, 30), create(12));

        assert!(db.operation(OperationId(11), Slot(30)).is_none());
    }

    #[test]
    fn resource_table_capacity_fails_fast() {
        let mut config = config();
        config.max_resources = 1;
        let mut db = AllocDb::new(config).unwrap();

        let first = db.apply_client(context(1, 1), create(11));
        let second = db.apply_client(context(2, 1), create(12));

        assert_eq!(first.result_code, ResultCode::Ok);
        assert_eq!(second.result_code, ResultCode::ResourceTableFull);
    }

    #[test]
    fn expiration_bucket_capacity_fails_fast() {
        let mut config = config();
        config.max_expiration_bucket_len = 1;
        let mut db = AllocDb::new(config).unwrap();
        db.apply_client(context(1, 1), create(11));
        db.apply_client(context(2, 1), create(12));

        let first = db.apply_client(
            context(3, 2),
            ClientRequest {
                operation_id: OperationId(3),
                client_id: ClientId(7),
                command: Command::Reserve {
                    resource_id: ResourceId(11),
                    holder_id: HolderId(1),
                    ttl_slots: 3,
                },
            },
        );
        let second = db.apply_client(
            context(4, 2),
            ClientRequest {
                operation_id: OperationId(4),
                client_id: ClientId(7),
                command: Command::Reserve {
                    resource_id: ResourceId(12),
                    holder_id: HolderId(2),
                    ttl_slots: 3,
                },
            },
        );

        assert_eq!(first.result_code, ResultCode::Ok);
        assert_eq!(second.result_code, ResultCode::ExpirationIndexFull);
    }

    #[test]
    fn due_reservations_are_bucketed_by_deadline_slot() {
        let mut db = AllocDb::new(config()).unwrap();
        db.apply_client(context(1, 1), create(11));
        db.apply_client(
            context(2, 2),
            ClientRequest {
                operation_id: OperationId(2),
                client_id: ClientId(7),
                command: Command::Reserve {
                    resource_id: ResourceId(11),
                    holder_id: HolderId(1),
                    ttl_slots: 3,
                },
            },
        );

        assert_eq!(db.due_reservations(Slot(5)), &[ReservationId(2)]);
    }
}

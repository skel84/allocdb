use log::{debug, warn};

use crate::command::{ClientRequest, Command, CommandContext};
use crate::ids::{HolderId, ReservationId, ResourceId, Slot};
use crate::result::{CommandOutcome, ResultCode};

use crate::state_machine::{
    AllocDb, OperationRecord, ReservationRecord, ReservationState, ResourceRecord, ResourceState,
};

impl AllocDb {
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

    pub(super) fn apply_create_resource(&mut self, resource_id: ResourceId) -> CommandOutcome {
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

    pub(super) fn apply_reserve(
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

    pub(super) fn apply_confirm(
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

    pub(super) fn apply_release(
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

    pub(super) fn apply_expire(
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
}

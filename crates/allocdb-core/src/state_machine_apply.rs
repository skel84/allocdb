use log::{debug, warn};

use crate::command::CommandContext;
use crate::ids::{HolderId, ReservationId, ResourceId, Slot};
use crate::result::{CommandOutcome, ResultCode};

use crate::state_machine::{AllocDb, ReservationState, ResourceRecord, ResourceState};

impl AllocDb {
    pub(super) fn apply_create_resource(&mut self, resource_id: ResourceId) -> CommandOutcome {
        if self.resources.contains_key(resource_id) {
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
        self.apply_reserve_bundle(context, &[resource_id], holder_id, ttl_slots)
    }

    pub(super) fn apply_confirm(
        &mut self,
        _context: CommandContext,
        reservation_id: ReservationId,
        holder_id: HolderId,
    ) -> CommandOutcome {
        let Some(reservation) = self.reservations.get(reservation_id).copied() else {
            warn!(
                "confirm rejected reservation_not_found reservation_id={}",
                reservation_id.get()
            );
            return CommandOutcome::new(ResultCode::ReservationNotFound);
        };

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

        assert!(
            self.resources.contains_key(reservation.resource_id),
            "active reservation anchor must reference an existing resource"
        );

        let removed = self.unschedule_expiration(reservation_id, reservation.deadline_slot);
        assert!(
            removed,
            "reserved reservations must stay scheduled for expiration"
        );

        let reservation = self
            .reservations
            .get_mut(reservation_id)
            .expect("reservation must stay present across confirm");
        reservation.state = ReservationState::Confirmed;
        let member_count = reservation.member_count;

        for member_index in 0..member_count {
            let member = self
                .reservation_member(reservation_id, member_index)
                .expect("reservation member must stay present across confirm");
            let resource = self
                .resources
                .get_mut(member.resource_id)
                .expect("resource must stay present across confirm");
            resource.current_state = ResourceState::Confirmed;
            resource.version += 1;
        }

        debug!("confirmed reservation_id={}", reservation_id.get());
        CommandOutcome::new(ResultCode::Ok)
    }

    pub(super) fn apply_release(
        &mut self,
        context: CommandContext,
        reservation_id: ReservationId,
        holder_id: HolderId,
    ) -> CommandOutcome {
        let Some(reservation) = self.reservations.get(reservation_id).copied() else {
            warn!(
                "release rejected reservation_not_found reservation_id={}",
                reservation_id.get()
            );
            return CommandOutcome::new(ResultCode::ReservationNotFound);
        };

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

        assert!(
            self.resources.contains_key(reservation.resource_id),
            "active reservation must reference an existing resource"
        );

        let retire_after_slot = match self.reservation_retire_after_slot(context.request_slot) {
            Ok(retire_after_slot) => retire_after_slot,
            Err(error) => return Self::slot_overflow_outcome("release", error),
        };

        let reservation = self
            .reservations
            .get_mut(reservation_id)
            .expect("reservation must stay present across release");
        reservation.state = ReservationState::Released;
        reservation.released_lsn = Some(context.lsn);
        reservation.retire_after_slot = Some(retire_after_slot);
        let queued_reservation_id = reservation.reservation_id;
        let member_count = reservation.member_count;

        for member_index in 0..member_count {
            let member = self
                .reservation_member(reservation_id, member_index)
                .expect("reservation member must stay present across release");
            let resource = self
                .resources
                .get_mut(member.resource_id)
                .expect("resource must stay present across release");
            resource.current_state = ResourceState::Available;
            resource.current_reservation_id = None;
            resource.version += 1;
        }

        self.push_reservation_retirement(queued_reservation_id, retire_after_slot);
        debug!("released reservation_id={}", reservation_id.get());
        CommandOutcome::new(ResultCode::Ok)
    }

    pub(super) fn apply_expire(
        &mut self,
        context: CommandContext,
        reservation_id: ReservationId,
        deadline_slot: Slot,
    ) -> CommandOutcome {
        let Some(reservation) = self.reservations.get(reservation_id).copied() else {
            warn!(
                "expire rejected reservation_not_found reservation_id={}",
                reservation_id.get()
            );
            return CommandOutcome::new(ResultCode::ReservationNotFound);
        };

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

        assert!(
            self.resources.contains_key(reservation.resource_id),
            "active reservation must reference an existing resource"
        );

        let retire_after_slot = match self.reservation_retire_after_slot(context.request_slot) {
            Ok(retire_after_slot) => retire_after_slot,
            Err(error) => return Self::slot_overflow_outcome("expire", error),
        };

        let reservation = self
            .reservations
            .get_mut(reservation_id)
            .expect("reservation must stay present across expire");
        reservation.state = ReservationState::Expired;
        reservation.released_lsn = Some(context.lsn);
        reservation.retire_after_slot = Some(retire_after_slot);
        let queued_reservation_id = reservation.reservation_id;
        let member_count = reservation.member_count;

        for member_index in 0..member_count {
            let member = self
                .reservation_member(reservation_id, member_index)
                .expect("reservation member must stay present across expire");
            let resource = self
                .resources
                .get_mut(member.resource_id)
                .expect("resource must stay present across expire");
            resource.current_state = ResourceState::Available;
            resource.current_reservation_id = None;
            resource.version += 1;
        }

        self.push_reservation_retirement(queued_reservation_id, retire_after_slot);
        debug!("expired reservation_id={}", reservation_id.get());
        CommandOutcome::new(ResultCode::Ok)
    }
}

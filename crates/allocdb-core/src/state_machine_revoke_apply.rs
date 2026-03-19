use log::{debug, warn};

use crate::command::CommandContext;
use crate::ids::ReservationId;
use crate::result::{CommandOutcome, ResultCode};
use crate::state_machine::{AllocDb, ReservationState, ResourceState};

impl AllocDb {
    pub(super) fn apply_revoke(&mut self, reservation_id: ReservationId) -> CommandOutcome {
        let Some(reservation) = self.reservations.get(reservation_id).copied() else {
            warn!(
                "revoke rejected reservation_not_found reservation_id={}",
                reservation_id.get()
            );
            return CommandOutcome::new(ResultCode::ReservationNotFound);
        };

        if reservation.retire_after_slot.is_some() {
            warn!(
                "revoke rejected reservation_retired reservation_id={}",
                reservation_id.get()
            );
            return CommandOutcome::new(ResultCode::ReservationRetired);
        }

        if reservation.state != ReservationState::Confirmed {
            warn!(
                "revoke rejected invalid_state reservation_id={} state={:?}",
                reservation_id.get(),
                reservation.state
            );
            return CommandOutcome::new(ResultCode::InvalidState);
        }

        assert!(
            self.resources.contains_key(reservation.resource_id),
            "active reservation anchor must reference an existing resource"
        );

        let reservation = self
            .reservations
            .get_mut(reservation_id)
            .expect("reservation must stay present across revoke");
        reservation.state = ReservationState::Revoking;
        reservation.lease_epoch += 1;
        let current_epoch = reservation.lease_epoch;
        self.mark_member_resources(reservation_id, ResourceState::Revoking);

        debug!(
            "revoking reservation_id={} new_lease_epoch={}",
            reservation_id.get(),
            current_epoch
        );
        CommandOutcome::new(ResultCode::Ok)
    }

    pub(super) fn apply_reclaim(
        &mut self,
        context: CommandContext,
        reservation_id: ReservationId,
    ) -> CommandOutcome {
        let Some(reservation) = self.reservations.get(reservation_id).copied() else {
            warn!(
                "reclaim rejected reservation_not_found reservation_id={}",
                reservation_id.get()
            );
            return CommandOutcome::new(ResultCode::ReservationNotFound);
        };

        if reservation.retire_after_slot.is_some() {
            warn!(
                "reclaim rejected reservation_retired reservation_id={}",
                reservation_id.get()
            );
            return CommandOutcome::new(ResultCode::ReservationRetired);
        }

        if reservation.state != ReservationState::Revoking {
            warn!(
                "reclaim rejected invalid_state reservation_id={} state={:?}",
                reservation_id.get(),
                reservation.state
            );
            return CommandOutcome::new(ResultCode::InvalidState);
        }

        assert!(
            self.resources.contains_key(reservation.resource_id),
            "active reservation anchor must reference an existing resource"
        );

        let retire_after_slot = match self.reservation_retire_after_slot(context.request_slot) {
            Ok(retire_after_slot) => retire_after_slot,
            Err(error) => return Self::slot_overflow_outcome("reclaim", error),
        };

        let reservation = self
            .reservations
            .get_mut(reservation_id)
            .expect("reservation must stay present across reclaim");
        reservation.state = ReservationState::Revoked;
        reservation.released_lsn = Some(context.lsn);
        reservation.retire_after_slot = Some(retire_after_slot);
        let queued_reservation_id = reservation.reservation_id;
        self.release_member_resources(reservation_id);

        self.push_reservation_retirement(queued_reservation_id, retire_after_slot);
        debug!(
            "reclaimed reservation_id={} retired_at_slot={}",
            reservation_id.get(),
            retire_after_slot.get()
        );
        CommandOutcome::new(ResultCode::Ok)
    }

    pub(super) fn mark_member_resources(
        &mut self,
        reservation_id: ReservationId,
        state: ResourceState,
    ) {
        let member_count = self
            .reservations
            .get(reservation_id)
            .expect("reservation must stay present while updating members")
            .member_count;

        for member_index in 0..member_count {
            let member = self
                .reservation_member(reservation_id, member_index)
                .expect("reservation member must stay present while updating members");
            let resource = self
                .resources
                .get_mut(member.resource_id)
                .expect("resource must stay present while updating members");
            resource.current_state = state;
            resource.version += 1;
        }
    }

    pub(super) fn release_member_resources(&mut self, reservation_id: ReservationId) {
        let member_count = self
            .reservations
            .get(reservation_id)
            .expect("reservation must stay present while releasing members")
            .member_count;

        for member_index in 0..member_count {
            let member = self
                .reservation_member(reservation_id, member_index)
                .expect("reservation member must stay present while releasing members");
            let resource = self
                .resources
                .get_mut(member.resource_id)
                .expect("resource must stay present while releasing members");
            resource.current_state = ResourceState::Available;
            resource.current_reservation_id = None;
            resource.version += 1;
        }
    }
}

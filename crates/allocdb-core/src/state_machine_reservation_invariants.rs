use crate::ids::ResourceId;
use crate::state_machine::{
    AllocDb, AllocDbInvariantError, ReservationMemberRecord, ReservationRecord, ReservationState,
    ResourceState,
};

impl AllocDb {
    pub(super) fn validate_reservation_lifecycle_invariants(
        reservation: ReservationRecord,
        scheduled: bool,
    ) -> Result<(), AllocDbInvariantError> {
        match reservation.state {
            ReservationState::Reserved | ReservationState::Confirmed => {
                if let Some(retire_after_slot) = reservation.retire_after_slot {
                    return Err(AllocDbInvariantError::ActiveReservationHasRetireAfterSlot {
                        reservation_id: reservation.reservation_id,
                        reservation_state: reservation.state,
                        retire_after_slot,
                    });
                }
            }
            ReservationState::Released | ReservationState::Expired => {
                if reservation.retire_after_slot.is_none() {
                    return Err(
                        AllocDbInvariantError::TerminalReservationMissingRetireAfterSlot {
                            reservation_id: reservation.reservation_id,
                            reservation_state: reservation.state,
                        },
                    );
                }
            }
        }

        match reservation.state {
            ReservationState::Reserved => {
                if !scheduled {
                    return Err(AllocDbInvariantError::ActiveReservationNotScheduled {
                        reservation_id: reservation.reservation_id,
                        deadline_slot: reservation.deadline_slot,
                    });
                }
            }
            ReservationState::Confirmed
            | ReservationState::Released
            | ReservationState::Expired => {
                if scheduled {
                    return Err(AllocDbInvariantError::InactiveReservationStillScheduled {
                        reservation_id: reservation.reservation_id,
                        deadline_slot: reservation.deadline_slot,
                        reservation_state: reservation.state,
                    });
                }
            }
        }

        Ok(())
    }

    pub(super) fn validate_reservation_member_invariants_for(
        &self,
        reservation: ReservationRecord,
    ) -> Result<(), AllocDbInvariantError> {
        for member_index in 0..reservation.member_count {
            let Some(member) = self.reservation_member(reservation.reservation_id, member_index)
            else {
                return Err(AllocDbInvariantError::ReservationMissingMember {
                    reservation_id: reservation.reservation_id,
                    member_index,
                });
            };

            Self::validate_anchor_member(reservation, member_index, member)?;
            self.validate_duplicate_member_resource(reservation, member)?;
            self.validate_member_resource_state(reservation, member.resource_id)?;
        }

        Ok(())
    }

    fn validate_anchor_member(
        reservation: ReservationRecord,
        member_index: u32,
        member: ReservationMemberRecord,
    ) -> Result<(), AllocDbInvariantError> {
        if member_index == 0 && member.resource_id != reservation.resource_id {
            return Err(AllocDbInvariantError::ReservationAnchorMismatch {
                reservation_id: reservation.reservation_id,
                reservation_resource_id: reservation.resource_id,
                member_resource_id: member.resource_id,
            });
        }

        Ok(())
    }

    fn validate_duplicate_member_resource(
        &self,
        reservation: ReservationRecord,
        member: ReservationMemberRecord,
    ) -> Result<(), AllocDbInvariantError> {
        for prior_member_index in 0..member.member_index {
            let prior_member = self
                .reservation_member(reservation.reservation_id, prior_member_index)
                .expect("earlier reservation members must already exist");
            if prior_member.resource_id == member.resource_id {
                return Err(AllocDbInvariantError::ReservationDuplicateMemberResource {
                    reservation_id: reservation.reservation_id,
                    first_member_index: prior_member_index,
                    second_member_index: member.member_index,
                    resource_id: member.resource_id,
                });
            }
        }

        Ok(())
    }

    fn validate_member_resource_state(
        &self,
        reservation: ReservationRecord,
        resource_id: ResourceId,
    ) -> Result<(), AllocDbInvariantError> {
        let Some(resource) = self.resources.get(resource_id).copied() else {
            return Err(AllocDbInvariantError::ReservationMissingResource {
                reservation_id: reservation.reservation_id,
                resource_id,
            });
        };

        match reservation.state {
            ReservationState::Reserved => {
                if resource.current_state != ResourceState::Reserved
                    || resource.current_reservation_id != Some(reservation.reservation_id)
                {
                    return Err(AllocDbInvariantError::ReservationResourceStateMismatch {
                        reservation_id: reservation.reservation_id,
                        resource_id,
                        reservation_state: reservation.state,
                        resource_state: resource.current_state,
                        resource_current_reservation_id: resource.current_reservation_id,
                    });
                }
            }
            ReservationState::Confirmed => {
                if resource.current_state != ResourceState::Confirmed
                    || resource.current_reservation_id != Some(reservation.reservation_id)
                {
                    return Err(AllocDbInvariantError::ReservationResourceStateMismatch {
                        reservation_id: reservation.reservation_id,
                        resource_id,
                        reservation_state: reservation.state,
                        resource_state: resource.current_state,
                        resource_current_reservation_id: resource.current_reservation_id,
                    });
                }
            }
            ReservationState::Released | ReservationState::Expired => {
                if resource.current_reservation_id == Some(reservation.reservation_id) {
                    return Err(
                        AllocDbInvariantError::TerminalReservationStillActiveOnResource {
                            reservation_id: reservation.reservation_id,
                            resource_id,
                        },
                    );
                }
            }
        }

        Ok(())
    }
}

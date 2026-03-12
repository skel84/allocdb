use core::cmp::Ordering;

use crate::ids::{ReservationId, ResourceId, Slot};
use crate::state_machine::{AllocDb, ReservationState, ResourceState};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AllocDbInvariantError {
    ExpirationBucketOverCapacity {
        bucket_index: usize,
        count: usize,
        max: usize,
    },
    WheelBucketOutOfOrder {
        bucket_index: usize,
        previous: ReservationId,
        next: ReservationId,
    },
    WheelReferencesMissingReservation {
        bucket_index: usize,
        reservation_id: ReservationId,
    },
    WheelReferencesUnreservedReservation {
        bucket_index: usize,
        reservation_id: ReservationId,
        reservation_state: ReservationState,
    },
    WheelDeadlineMismatch {
        bucket_index: usize,
        reservation_id: ReservationId,
        deadline_slot: Slot,
    },
    AvailableResourceHasReservation {
        resource_id: ResourceId,
        reservation_id: ReservationId,
    },
    ActiveResourceMissingReservationId {
        resource_id: ResourceId,
        resource_state: ResourceState,
    },
    ActiveResourceMissingReservation {
        resource_id: ResourceId,
        reservation_id: ReservationId,
    },
    ResourceReservationMismatch {
        resource_id: ResourceId,
        reservation_id: ReservationId,
        reservation_resource_id: ResourceId,
    },
    ResourceReservationStateMismatch {
        resource_id: ResourceId,
        reservation_id: ReservationId,
        resource_state: ResourceState,
        reservation_state: ReservationState,
    },
    ReservationMissingResource {
        reservation_id: ReservationId,
        resource_id: ResourceId,
    },
    ReservationResourceStateMismatch {
        reservation_id: ReservationId,
        resource_id: ResourceId,
        reservation_state: ReservationState,
        resource_state: ResourceState,
        resource_current_reservation_id: Option<ReservationId>,
    },
    ActiveReservationHasRetireAfterSlot {
        reservation_id: ReservationId,
        reservation_state: ReservationState,
        retire_after_slot: Slot,
    },
    TerminalReservationMissingRetireAfterSlot {
        reservation_id: ReservationId,
        reservation_state: ReservationState,
    },
    ActiveReservationNotScheduled {
        reservation_id: ReservationId,
        deadline_slot: Slot,
    },
    InactiveReservationStillScheduled {
        reservation_id: ReservationId,
        deadline_slot: Slot,
        reservation_state: ReservationState,
    },
    TerminalReservationStillActiveOnResource {
        reservation_id: ReservationId,
        resource_id: ResourceId,
    },
}

impl AllocDb {
    pub(crate) fn assert_invariants(&self) {
        if let Err(error) = self.validate_invariants() {
            panic!("allocdb invariant violation: {error:?}");
        }
    }

    pub(crate) fn validate_invariants(&self) -> Result<(), AllocDbInvariantError> {
        self.validate_wheel_invariants()?;
        self.validate_resource_invariants()?;
        self.validate_reservation_invariants()?;
        Ok(())
    }

    fn validate_wheel_invariants(&self) -> Result<(), AllocDbInvariantError> {
        let max_bucket_len = usize::try_from(self.config.max_expiration_bucket_len)
            .expect("validated max_expiration_bucket_len must fit usize");

        for (bucket_index, bucket) in self.wheel.iter().enumerate() {
            if bucket.len() > max_bucket_len {
                return Err(AllocDbInvariantError::ExpirationBucketOverCapacity {
                    bucket_index,
                    count: bucket.len(),
                    max: max_bucket_len,
                });
            }

            for pair in bucket.windows(2) {
                if pair[0].cmp(&pair[1]) != Ordering::Less {
                    return Err(AllocDbInvariantError::WheelBucketOutOfOrder {
                        bucket_index,
                        previous: pair[0],
                        next: pair[1],
                    });
                }
            }

            for &reservation_id in bucket {
                let Some(reservation) = self.reservations.get(reservation_id).copied() else {
                    return Err(AllocDbInvariantError::WheelReferencesMissingReservation {
                        bucket_index,
                        reservation_id,
                    });
                };

                if reservation.state != ReservationState::Reserved {
                    return Err(
                        AllocDbInvariantError::WheelReferencesUnreservedReservation {
                            bucket_index,
                            reservation_id,
                            reservation_state: reservation.state,
                        },
                    );
                }

                if self.wheel_bucket_index(reservation.deadline_slot) != bucket_index {
                    return Err(AllocDbInvariantError::WheelDeadlineMismatch {
                        bucket_index,
                        reservation_id,
                        deadline_slot: reservation.deadline_slot,
                    });
                }
            }
        }

        Ok(())
    }

    fn validate_resource_invariants(&self) -> Result<(), AllocDbInvariantError> {
        for resource in self.resources.iter() {
            match resource.current_state {
                ResourceState::Available => {
                    if let Some(reservation_id) = resource.current_reservation_id {
                        return Err(AllocDbInvariantError::AvailableResourceHasReservation {
                            resource_id: resource.resource_id,
                            reservation_id,
                        });
                    }
                }
                ResourceState::Reserved | ResourceState::Confirmed => {
                    let Some(reservation_id) = resource.current_reservation_id else {
                        return Err(AllocDbInvariantError::ActiveResourceMissingReservationId {
                            resource_id: resource.resource_id,
                            resource_state: resource.current_state,
                        });
                    };
                    let Some(reservation) = self.reservations.get(reservation_id).copied() else {
                        return Err(AllocDbInvariantError::ActiveResourceMissingReservation {
                            resource_id: resource.resource_id,
                            reservation_id,
                        });
                    };
                    if reservation.resource_id != resource.resource_id {
                        return Err(AllocDbInvariantError::ResourceReservationMismatch {
                            resource_id: resource.resource_id,
                            reservation_id,
                            reservation_resource_id: reservation.resource_id,
                        });
                    }

                    let expected_reservation_state = match resource.current_state {
                        ResourceState::Reserved => ReservationState::Reserved,
                        ResourceState::Confirmed => ReservationState::Confirmed,
                        ResourceState::Available => unreachable!(),
                    };
                    if reservation.state != expected_reservation_state {
                        return Err(AllocDbInvariantError::ResourceReservationStateMismatch {
                            resource_id: resource.resource_id,
                            reservation_id,
                            resource_state: resource.current_state,
                            reservation_state: reservation.state,
                        });
                    }
                }
            }
        }

        Ok(())
    }

    fn validate_reservation_invariants(&self) -> Result<(), AllocDbInvariantError> {
        for reservation in self.reservations.iter() {
            let Some(resource) = self.resources.get(reservation.resource_id).copied() else {
                return Err(AllocDbInvariantError::ReservationMissingResource {
                    reservation_id: reservation.reservation_id,
                    resource_id: reservation.resource_id,
                });
            };
            let bucket_index = self.wheel_bucket_index(reservation.deadline_slot);
            let scheduled = self.wheel[bucket_index]
                .binary_search(&reservation.reservation_id)
                .is_ok();

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
                    if resource.current_state != ResourceState::Reserved
                        || resource.current_reservation_id != Some(reservation.reservation_id)
                    {
                        return Err(AllocDbInvariantError::ReservationResourceStateMismatch {
                            reservation_id: reservation.reservation_id,
                            resource_id: reservation.resource_id,
                            reservation_state: reservation.state,
                            resource_state: resource.current_state,
                            resource_current_reservation_id: resource.current_reservation_id,
                        });
                    }

                    if !scheduled {
                        return Err(AllocDbInvariantError::ActiveReservationNotScheduled {
                            reservation_id: reservation.reservation_id,
                            deadline_slot: reservation.deadline_slot,
                        });
                    }
                }
                ReservationState::Confirmed => {
                    if resource.current_state != ResourceState::Confirmed
                        || resource.current_reservation_id != Some(reservation.reservation_id)
                    {
                        return Err(AllocDbInvariantError::ReservationResourceStateMismatch {
                            reservation_id: reservation.reservation_id,
                            resource_id: reservation.resource_id,
                            reservation_state: reservation.state,
                            resource_state: resource.current_state,
                            resource_current_reservation_id: resource.current_reservation_id,
                        });
                    }

                    if scheduled {
                        return Err(AllocDbInvariantError::InactiveReservationStillScheduled {
                            reservation_id: reservation.reservation_id,
                            deadline_slot: reservation.deadline_slot,
                            reservation_state: reservation.state,
                        });
                    }
                }
                ReservationState::Released | ReservationState::Expired => {
                    if resource.current_reservation_id == Some(reservation.reservation_id) {
                        return Err(
                            AllocDbInvariantError::TerminalReservationStillActiveOnResource {
                                reservation_id: reservation.reservation_id,
                                resource_id: reservation.resource_id,
                            },
                        );
                    }

                    if scheduled {
                        return Err(AllocDbInvariantError::InactiveReservationStillScheduled {
                            reservation_id: reservation.reservation_id,
                            deadline_slot: reservation.deadline_slot,
                            reservation_state: reservation.state,
                        });
                    }
                }
            }
        }

        Ok(())
    }
}

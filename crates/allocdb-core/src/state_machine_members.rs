use crate::fixed_map::{FixedKey, FixedMapError, hash_u128};
use crate::ids::{ReservationId, ResourceId};
use crate::state_machine::{AllocDb, ReservationRecord};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ReservationMemberKey {
    pub reservation_id: ReservationId,
    pub member_index: u32,
}

impl FixedKey for ReservationMemberKey {
    fn hash64(self) -> u64 {
        let mixed = self.reservation_id.get() ^ (u128::from(self.member_index) << 96);
        hash_u128(mixed)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReservationMemberRecord {
    pub reservation_id: ReservationId,
    pub resource_id: ResourceId,
    pub member_index: u32,
}

impl AllocDb {
    pub(crate) fn reservation_member_key(
        reservation_id: ReservationId,
        member_index: u32,
    ) -> ReservationMemberKey {
        ReservationMemberKey {
            reservation_id,
            member_index,
        }
    }

    pub(crate) fn reservation_member(
        &self,
        reservation_id: ReservationId,
        member_index: u32,
    ) -> Option<ReservationMemberRecord> {
        self.reservation_members
            .get(Self::reservation_member_key(reservation_id, member_index))
            .copied()
    }

    #[must_use]
    ///
    /// # Panics
    ///
    /// Panics if `reservation.member_count` cannot fit `usize` on the current platform or if the
    /// trusted core has lost one of the live member records required by `reservation`.
    pub fn reservation_member_resource_ids(
        &self,
        reservation: ReservationRecord,
    ) -> Vec<ResourceId> {
        let mut resource_ids =
            Vec::with_capacity(usize::try_from(reservation.member_count).expect("u32 fits usize"));
        for member_index in 0..reservation.member_count {
            let member = self
                .reservation_member(reservation.reservation_id, member_index)
                .expect("live reservation must retain all member records");
            resource_ids.push(member.resource_id);
        }
        resource_ids
    }

    pub(crate) fn reservation_contains_resource(
        &self,
        reservation: ReservationRecord,
        resource_id: ResourceId,
    ) -> bool {
        (0..reservation.member_count).any(|member_index| {
            self.reservation_member(reservation.reservation_id, member_index)
                .is_some_and(|member| member.resource_id == resource_id)
        })
    }

    pub(crate) fn insert_reservation_member(&mut self, record: ReservationMemberRecord) {
        match self.reservation_members.insert(
            Self::reservation_member_key(record.reservation_id, record.member_index),
            record,
        ) {
            Ok(()) => {}
            Err(FixedMapError::DuplicateKey | FixedMapError::Full) => {
                panic!("reservation member inserts must respect capacity and uniqueness")
            }
        }
    }

    pub(crate) fn remove_reservation_members(
        &mut self,
        reservation_id: ReservationId,
        member_count: u32,
    ) {
        for member_index in 0..member_count {
            let removed = self
                .reservation_members
                .remove(Self::reservation_member_key(reservation_id, member_index));
            assert!(
                removed.is_some(),
                "reservation retirement must remove all member records"
            );
        }
    }
}

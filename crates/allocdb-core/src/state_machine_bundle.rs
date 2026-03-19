use log::{debug, warn};

use crate::command::CommandContext;
use crate::ids::{HolderId, ResourceId};
use crate::result::{CommandOutcome, ResultCode};
use crate::state_machine::{
    AllocDb, ReservationMemberRecord, ReservationRecord, ReservationState, ResourceState,
};

impl AllocDb {
    const INITIAL_LEASE_EPOCH: u64 = 1;

    pub(super) fn apply_reserve_bundle(
        &mut self,
        context: CommandContext,
        resource_ids: &[ResourceId],
        holder_id: HolderId,
        ttl_slots: u64,
    ) -> CommandOutcome {
        if let Some(outcome) = self.reserve_bundle_precheck(resource_ids, ttl_slots) {
            return outcome;
        }

        let reservation_id = self.reservation_id_from_lsn(context.lsn);
        let deadline_slot = match Self::deadline_slot(context.request_slot, ttl_slots) {
            Ok(deadline_slot) => deadline_slot,
            Err(error) => return Self::slot_overflow_outcome("reserve", error),
        };

        if !self.schedule_expiration(reservation_id, deadline_slot) {
            warn!(
                "reserve_bundle rejected expiration_index_full bundle_len={} deadline_slot={}",
                resource_ids.len(),
                deadline_slot.get()
            );
            return CommandOutcome::new(ResultCode::ExpirationIndexFull);
        }

        let member_count = u32::try_from(resource_ids.len()).expect("bundle len must fit u32");
        self.insert_reservation(ReservationRecord {
            reservation_id,
            resource_id: resource_ids[0],
            holder_id,
            lease_epoch: Self::INITIAL_LEASE_EPOCH,
            state: ReservationState::Reserved,
            created_lsn: context.lsn,
            deadline_slot,
            released_lsn: None,
            retire_after_slot: None,
            member_count,
        });

        for (member_index, resource_id) in resource_ids.iter().copied().enumerate() {
            self.insert_reservation_member(ReservationMemberRecord {
                reservation_id,
                resource_id,
                member_index: u32::try_from(member_index).expect("bundle index must fit u32"),
            });

            let resource = self
                .resources
                .get_mut(resource_id)
                .expect("existing resource must stay present");
            resource.current_state = ResourceState::Reserved;
            resource.current_reservation_id = Some(reservation_id);
            resource.version += 1;
        }

        debug!(
            "reserved bundle reservation_id={} lease_epoch={} bundle_len={} deadline_slot={}",
            reservation_id.get(),
            Self::INITIAL_LEASE_EPOCH,
            resource_ids.len(),
            deadline_slot.get()
        );
        CommandOutcome::with_reservation_epoch(
            ResultCode::Ok,
            reservation_id,
            Self::INITIAL_LEASE_EPOCH,
            deadline_slot,
        )
    }

    fn reserve_bundle_precheck(
        &self,
        resource_ids: &[ResourceId],
        ttl_slots: u64,
    ) -> Option<CommandOutcome> {
        self.validate_bundle_shape(resource_ids, ttl_slots)
            .or_else(|| self.validate_bundle_resources(resource_ids))
            .or_else(|| self.validate_bundle_capacity(resource_ids.len()))
    }

    fn validate_bundle_shape(
        &self,
        resource_ids: &[ResourceId],
        ttl_slots: u64,
    ) -> Option<CommandOutcome> {
        let max_bundle_size =
            usize::try_from(self.config.max_bundle_size).expect("validated max_bundle_size");
        if resource_ids.is_empty() || resource_ids.len() > max_bundle_size {
            warn!(
                "reserve_bundle rejected bundle_too_large bundle_len={} max_bundle_size={}",
                resource_ids.len(),
                self.config.max_bundle_size
            );
            return Some(CommandOutcome::new(ResultCode::BundleTooLarge));
        }

        if ttl_slots == 0 || ttl_slots > self.config.max_ttl_slots {
            warn!(
                "reserve_bundle rejected ttl_out_of_range bundle_len={} ttl_slots={}",
                resource_ids.len(),
                ttl_slots
            );
            return Some(CommandOutcome::new(ResultCode::TtlOutOfRange));
        }

        None
    }

    fn validate_bundle_resources(&self, resource_ids: &[ResourceId]) -> Option<CommandOutcome> {
        for (index, resource_id) in resource_ids.iter().copied().enumerate() {
            for duplicate in &resource_ids[..index] {
                if *duplicate == resource_id {
                    warn!(
                        "reserve_bundle rejected duplicate resource_id={} bundle_len={}",
                        resource_id.get(),
                        resource_ids.len()
                    );
                    return Some(CommandOutcome::new(ResultCode::ResourceBusy));
                }
            }

            let Some(resource) = self.resources.get(resource_id).copied() else {
                warn!(
                    "reserve_bundle rejected resource_not_found resource_id={}",
                    resource_id.get()
                );
                return Some(CommandOutcome::new(ResultCode::ResourceNotFound));
            };

            if resource.current_state != ResourceState::Available {
                warn!(
                    "reserve_bundle rejected resource_busy resource_id={}",
                    resource_id.get()
                );
                return Some(CommandOutcome::new(ResultCode::ResourceBusy));
            }
        }

        None
    }

    fn validate_bundle_capacity(&self, resource_count: usize) -> Option<CommandOutcome> {
        if self.reservations.len()
            == usize::try_from(self.config.max_reservations).expect("validated max_reservations")
        {
            warn!("reservation table is full");
            return Some(CommandOutcome::new(ResultCode::ReservationTableFull));
        }

        if self.reservation_members.len() + resource_count > self.config.max_reservation_members() {
            warn!(
                "reservation member table is full bundle_len={} current_members={}",
                resource_count,
                self.reservation_members.len()
            );
            return Some(CommandOutcome::new(ResultCode::ReservationMemberTableFull));
        }

        None
    }
}

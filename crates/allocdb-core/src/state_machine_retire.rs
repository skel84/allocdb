use crate::ids::{OperationId, ReservationId, Slot};
use crate::state_machine::AllocDb;

impl AllocDb {
    pub(crate) fn retire_reservations(&mut self, current_slot: Slot) {
        while let Some(entry) = self.reservation_retire_queue.front() {
            if current_slot.get() <= entry.retire_after_slot.get() {
                break;
            }

            let popped = self
                .reservation_retire_queue
                .pop_front()
                .expect("front entry must be poppable");
            debug_assert_eq!(popped, entry);

            let should_remove = self
                .reservations
                .get(entry.key)
                .is_some_and(|record| record.retire_after_slot == Some(entry.retire_after_slot));
            if should_remove {
                let removed = self.reservations.remove(entry.key);
                assert!(
                    removed.is_some(),
                    "queued reservation retirement must remove one entry"
                );
                self.record_retired_reservation_id(entry.key);
            }
        }
    }

    pub(crate) fn retire_operations(&mut self, current_slot: Slot) {
        while let Some(entry) = self.operation_retire_queue.front() {
            if current_slot.get() <= entry.retire_after_slot.get() {
                break;
            }

            let popped = self
                .operation_retire_queue
                .pop_front()
                .expect("front entry must be poppable");
            debug_assert_eq!(popped, entry);

            let should_remove = self
                .operations
                .get(entry.key)
                .is_some_and(|record| record.retire_after_slot == entry.retire_after_slot);
            if should_remove {
                let removed = self.operations.remove(entry.key);
                assert!(
                    removed.is_some(),
                    "queued operation retirement must remove one entry"
                );
            }
        }
    }

    pub(crate) fn rebuild_retire_queues(
        &mut self,
        reservation_entries: &mut Vec<(ReservationId, Slot, u64)>,
        operation_entries: &mut Vec<(OperationId, Slot, u64)>,
    ) {
        reservation_entries.sort_unstable_by_key(|entry| (entry.1.get(), entry.2));
        for (reservation_id, retire_after_slot, _) in reservation_entries.drain(..) {
            self.push_reservation_retirement(reservation_id, retire_after_slot);
        }

        operation_entries.sort_unstable_by_key(|entry| (entry.1.get(), entry.2));
        for (operation_id, retire_after_slot, _) in operation_entries.drain(..) {
            self.push_operation_retirement(operation_id, retire_after_slot);
        }
    }
}

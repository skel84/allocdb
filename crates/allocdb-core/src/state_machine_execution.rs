use log::{debug, warn};

use crate::command::{ClientRequest, Command, CommandContext};
use crate::result::{CommandOutcome, ResultCode};
use crate::state_machine::{AllocDb, OperationRecord};

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
        if let Err(error) =
            self.validate_client_request_slot(context.request_slot, &request.command)
        {
            return Self::slot_overflow_outcome("apply_client", error);
        }

        self.begin_apply(context);
        self.retire_state(context.request_slot);

        let fingerprint = request.command.fingerprint();
        if let Some(record) = self.operations.get(request.operation_id).copied() {
            if context.request_slot.get() > record.retire_after_slot.get() {
                let removed = self.operations.remove(request.operation_id);
                assert!(
                    removed.is_some(),
                    "existing operation record must be removable"
                );
            } else if record.command_fingerprint == fingerprint {
                debug!(
                    "returning stored outcome for operation_id={} result_code={:?}",
                    request.operation_id.get(),
                    record.result_code
                );
                return CommandOutcome {
                    result_code: record.result_code,
                    reservation_id: record.result_reservation_id,
                    lease_epoch: record.result_lease_epoch,
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

        let operation_retire_after_slot =
            match self.operation_retire_after_slot(context.request_slot) {
                Ok(retire_after_slot) => retire_after_slot,
                Err(error) => return Self::slot_overflow_outcome("apply_client", error),
            };

        let outcome = self.apply_command(context, request.command);
        let operation_record = OperationRecord {
            operation_id: request.operation_id,
            command_fingerprint: fingerprint,
            result_code: outcome.result_code,
            result_reservation_id: outcome.reservation_id,
            result_lease_epoch: outcome.lease_epoch,
            result_deadline_slot: outcome.deadline_slot,
            applied_lsn: context.lsn,
            retire_after_slot: operation_retire_after_slot,
        };
        self.insert_operation(operation_record);
        self.push_operation_retirement(
            operation_record.operation_id,
            operation_record.retire_after_slot,
        );
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
        if let Err(error) = self.validate_internal_request_slot(context.request_slot, &command) {
            return Self::slot_overflow_outcome("apply_internal", error);
        }

        self.begin_apply(context);
        self.retire_state(context.request_slot);
        let outcome = self.apply_command(context, command);
        self.assert_invariants();
        outcome
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

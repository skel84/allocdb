use log::warn;

use crate::command::{ClientRequest, Command, CommandContext};
use crate::config::{Config, ConfigError};
use crate::fixed_map::{FixedMap, FixedMapError};
use crate::ids::{HoldId, Lsn, OperationId, PoolId, Slot};
use crate::result::{CommandOutcome, ResultCode};
use crate::retire_queue::{RetireEntry, RetireQueue, RetireQueueError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PoolRecord {
    pub pool_id: PoolId,
    pub total_capacity: u64,
    pub held_capacity: u64,
    pub consumed_capacity: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HoldState {
    Held,
    Confirmed,
    Released,
    Expired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HoldRecord {
    pub hold_id: HoldId,
    pub pool_id: PoolId,
    pub quantity: u64,
    pub deadline_slot: Slot,
    pub state: HoldState,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OperationRecord {
    pub operation_id: OperationId,
    pub command: Command,
    pub result_code: ResultCode,
    pub result_pool_id: Option<PoolId>,
    pub result_hold_id: Option<HoldId>,
    pub applied_lsn: Lsn,
    pub retire_after_slot: Slot,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SlotOverflowKind {
    OperationWindow,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SlotOverflowError {
    pub kind: SlotOverflowKind,
    pub request_slot: Slot,
    pub delta: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReservationInvariantError {
    HeldCapacityExceedsTotal {
        pool_id: PoolId,
        held_capacity: u64,
        total_capacity: u64,
    },
    ConsumedCapacityExceedsTotal {
        pool_id: PoolId,
        consumed_capacity: u64,
        total_capacity: u64,
    },
    AccountedCapacityExceedsTotal {
        pool_id: PoolId,
        held_capacity: u64,
        consumed_capacity: u64,
        total_capacity: u64,
    },
}

#[derive(Debug)]
pub struct ReservationDb {
    pub(crate) config: Config,
    pub(crate) pools: FixedMap<PoolId, PoolRecord>,
    pub(crate) holds: FixedMap<HoldId, HoldRecord>,
    pub(crate) operations: FixedMap<OperationId, OperationRecord>,
    pub(crate) hold_retire_queue: RetireQueue<HoldId>,
    pub(crate) operation_retire_queue: RetireQueue<OperationId>,
    pub(crate) last_applied_lsn: Option<Lsn>,
    pub(crate) last_request_slot: Option<Slot>,
}

impl ReservationDb {
    /// Creates one empty deterministic reservation engine with fixed-capacity in-memory state.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] when the supplied configuration is internally inconsistent.
    pub fn new(config: Config) -> Result<Self, ConfigError> {
        config.validate()?;

        let max_pools =
            usize::try_from(config.max_pools).expect("validated max_pools must fit usize");
        let max_holds =
            usize::try_from(config.max_holds).expect("validated max_holds must fit usize");
        let max_operations = usize::try_from(config.max_operations)
            .expect("validated max_operations must fit usize");

        Ok(Self {
            config,
            pools: FixedMap::with_capacity(max_pools),
            holds: FixedMap::with_capacity(max_holds),
            operations: FixedMap::with_capacity(max_operations),
            hold_retire_queue: RetireQueue::with_capacity(max_holds),
            operation_retire_queue: RetireQueue::with_capacity(max_operations),
            last_applied_lsn: None,
            last_request_slot: None,
        })
    }

    #[must_use]
    pub fn last_applied_lsn(&self) -> Option<Lsn> {
        self.last_applied_lsn
    }

    #[must_use]
    pub fn last_request_slot(&self) -> Option<Slot> {
        self.last_request_slot
    }

    pub fn validate_client_request_slot(
        &self,
        request_slot: Slot,
        _command: &Command,
    ) -> Result<(), SlotOverflowError> {
        let _ = self.operation_retire_after_slot(request_slot)?;
        Ok(())
    }

    pub fn validate_internal_request_slot(
        &self,
        _request_slot: Slot,
        _command: &Command,
    ) -> Result<(), SlotOverflowError> {
        Ok(())
    }

    pub fn apply_client(
        &mut self,
        context: CommandContext,
        request: ClientRequest,
    ) -> CommandOutcome {
        if let Err(error) =
            self.validate_client_request_slot(context.request_slot, &request.command)
        {
            return Self::slot_overflow_outcome(error);
        }

        self.begin_apply(context);
        self.retire_state(context.request_slot);
        self.assert_invariants();
        CommandOutcome::new(ResultCode::InvalidState)
    }

    pub fn apply_internal(&mut self, context: CommandContext, command: Command) -> CommandOutcome {
        if let Err(error) = self.validate_internal_request_slot(context.request_slot, &command) {
            return Self::slot_overflow_outcome(error);
        }

        self.begin_apply(context);
        self.retire_state(context.request_slot);
        self.assert_invariants();
        CommandOutcome::new(ResultCode::InvalidState)
    }

    pub(crate) fn restore_pool(&mut self, record: PoolRecord) -> Result<(), FixedMapError> {
        self.pools.insert(record.pool_id, record)
    }

    pub(crate) fn restore_hold(&mut self, record: HoldRecord) -> Result<(), FixedMapError> {
        self.holds.insert(record.hold_id, record)
    }

    pub(crate) fn restore_operation(
        &mut self,
        record: OperationRecord,
    ) -> Result<(), FixedMapError> {
        self.operations.insert(record.operation_id, record)
    }

    pub(crate) fn rebuild_hold_retire_queue(&mut self, entries: &mut Vec<(HoldId, Slot, u64)>) {
        entries.sort_unstable_by_key(|(_, deadline_slot, ordinal)| (deadline_slot.get(), *ordinal));

        for (hold_id, deadline_slot, _) in entries.drain(..) {
            match self.hold_retire_queue.push(RetireEntry {
                key: hold_id,
                retire_after_slot: deadline_slot,
            }) {
                Ok(()) => {}
                Err(RetireQueueError::Full) => {
                    panic!("hold retirement rebuild must respect configured capacity")
                }
            }
        }
    }

    pub(crate) fn rebuild_operation_retire_queue(
        &mut self,
        entries: &mut Vec<(OperationId, Slot, u64)>,
    ) {
        entries.sort_unstable_by_key(|(_, retire_after_slot, applied_lsn)| {
            (retire_after_slot.get(), *applied_lsn)
        });

        for (operation_id, retire_after_slot, _) in entries.drain(..) {
            match self.operation_retire_queue.push(RetireEntry {
                key: operation_id,
                retire_after_slot,
            }) {
                Ok(()) => {}
                Err(RetireQueueError::Full) => {
                    panic!("operation retirement rebuild must respect configured capacity")
                }
            }
        }
    }

    pub(crate) fn set_progress(
        &mut self,
        last_applied_lsn: Option<Lsn>,
        last_request_slot: Option<Slot>,
    ) {
        self.last_applied_lsn = last_applied_lsn;
        self.last_request_slot = last_request_slot;
    }

    #[must_use]
    pub fn snapshot(&self) -> crate::snapshot::Snapshot {
        let mut pools: Vec<_> = self.pools.iter().copied().collect();
        pools.sort_unstable_by_key(|record| record.pool_id.get());

        let mut holds: Vec<_> = self.holds.iter().copied().collect();
        holds.sort_unstable_by_key(|record| record.hold_id.get());

        let mut operations: Vec<_> = self.operations.iter().copied().collect();
        operations.sort_unstable_by_key(|record| record.operation_id.get());

        crate::snapshot::Snapshot {
            last_applied_lsn: self.last_applied_lsn,
            last_request_slot: self.last_request_slot,
            pools,
            holds,
            operations,
        }
    }

    pub fn validate_invariants(&self) -> Result<(), ReservationInvariantError> {
        for pool in self.pools.iter() {
            if pool.held_capacity > pool.total_capacity {
                return Err(ReservationInvariantError::HeldCapacityExceedsTotal {
                    pool_id: pool.pool_id,
                    held_capacity: pool.held_capacity,
                    total_capacity: pool.total_capacity,
                });
            }
            if pool.consumed_capacity > pool.total_capacity {
                return Err(ReservationInvariantError::ConsumedCapacityExceedsTotal {
                    pool_id: pool.pool_id,
                    consumed_capacity: pool.consumed_capacity,
                    total_capacity: pool.total_capacity,
                });
            }
            if pool.held_capacity.saturating_add(pool.consumed_capacity) > pool.total_capacity {
                return Err(ReservationInvariantError::AccountedCapacityExceedsTotal {
                    pool_id: pool.pool_id,
                    held_capacity: pool.held_capacity,
                    consumed_capacity: pool.consumed_capacity,
                    total_capacity: pool.total_capacity,
                });
            }
        }
        Ok(())
    }

    fn begin_apply(&mut self, context: CommandContext) {
        if let Some(previous_lsn) = self.last_applied_lsn {
            assert!(
                context.lsn.get() > previous_lsn.get(),
                "LSN must advance monotonically"
            );
        }
        if let Some(previous_request_slot) = self.last_request_slot {
            assert!(
                context.request_slot.get() >= previous_request_slot.get(),
                "request slots must not move backwards"
            );
        }

        self.last_applied_lsn = Some(context.lsn);
        self.last_request_slot = Some(context.request_slot);
    }

    fn retire_state(&mut self, request_slot: Slot) {
        while let Some(entry) = self.operation_retire_queue.front() {
            if entry.retire_after_slot.get() > request_slot.get() {
                break;
            }

            let removed = self.operations.remove(entry.key);
            if removed.is_none() {
                warn!(
                    "operation retirement queue referenced missing operation_id={}",
                    entry.key.get()
                );
            }
            let popped = self.operation_retire_queue.pop_front();
            assert!(
                popped.is_some(),
                "peeked operation retirement entry must pop"
            );
        }
    }

    fn operation_retire_after_slot(&self, request_slot: Slot) -> Result<Slot, SlotOverflowError> {
        match request_slot
            .get()
            .checked_add(self.config.max_client_retry_window_slots)
        {
            Some(slot) => Ok(Slot(slot)),
            None => Err(SlotOverflowError {
                kind: SlotOverflowKind::OperationWindow,
                request_slot,
                delta: self.config.max_client_retry_window_slots,
            }),
        }
    }

    fn slot_overflow_outcome(_error: SlotOverflowError) -> CommandOutcome {
        CommandOutcome::new(ResultCode::SlotOverflow)
    }

    fn assert_invariants(&self) {
        if let Err(error) = self.validate_invariants() {
            panic!("reservation invariants violated: {error:?}");
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        command::{ClientRequest, Command, CommandContext},
        config::Config,
        ids::{ClientId, HoldId, Lsn, OperationId, PoolId, Slot},
        result::ResultCode,
    };

    use super::{HoldRecord, HoldState, PoolRecord, ReservationDb, ReservationInvariantError};

    fn config() -> Config {
        Config {
            max_pools: 8,
            max_holds: 8,
            max_operations: 8,
            max_batch_len: 8,
            max_client_retry_window_slots: 8,
            max_wal_payload_bytes: 1024,
            max_snapshot_bytes: 4096,
        }
    }

    #[test]
    fn new_creates_empty_db() {
        let db = ReservationDb::new(config()).unwrap();

        assert_eq!(db.last_applied_lsn(), None);
        assert_eq!(db.last_request_slot(), None);
        assert_eq!(db.snapshot().pools.len(), 0);
        assert_eq!(db.snapshot().holds.len(), 0);
        assert_eq!(db.snapshot().operations.len(), 0);
    }

    #[test]
    fn snapshot_orders_records_stably() {
        let mut db = ReservationDb::new(config()).unwrap();
        db.restore_pool(PoolRecord {
            pool_id: PoolId(9),
            total_capacity: 10,
            held_capacity: 0,
            consumed_capacity: 0,
        })
        .unwrap();
        db.restore_pool(PoolRecord {
            pool_id: PoolId(3),
            total_capacity: 10,
            held_capacity: 0,
            consumed_capacity: 0,
        })
        .unwrap();
        db.restore_hold(HoldRecord {
            hold_id: HoldId(7),
            pool_id: PoolId(9),
            quantity: 2,
            deadline_slot: Slot(5),
            state: HoldState::Held,
        })
        .unwrap();
        db.restore_hold(HoldRecord {
            hold_id: HoldId(4),
            pool_id: PoolId(3),
            quantity: 1,
            deadline_slot: Slot(4),
            state: HoldState::Released,
        })
        .unwrap();

        let snapshot = db.snapshot();
        assert_eq!(
            snapshot
                .pools
                .iter()
                .map(|record| record.pool_id.get())
                .collect::<Vec<_>>(),
            vec![3, 9]
        );
        assert_eq!(
            snapshot
                .holds
                .iter()
                .map(|record| record.hold_id.get())
                .collect::<Vec<_>>(),
            vec![4, 7]
        );
    }

    #[test]
    fn apply_client_advances_progress_but_does_not_implement_lifecycle_yet() {
        let mut db = ReservationDb::new(config()).unwrap();
        let outcome = db.apply_client(
            CommandContext {
                lsn: Lsn(1),
                request_slot: Slot(2),
            },
            ClientRequest {
                operation_id: OperationId(11),
                client_id: ClientId(12),
                command: Command::CreatePool {
                    pool_id: PoolId(13),
                    total_capacity: 5,
                },
            },
        );

        assert_eq!(outcome.result_code, ResultCode::InvalidState);
        assert_eq!(db.last_applied_lsn(), Some(Lsn(1)));
        assert_eq!(db.last_request_slot(), Some(Slot(2)));
    }

    #[test]
    fn validate_invariants_rejects_over_accounted_pool() {
        let mut db = ReservationDb::new(config()).unwrap();
        db.restore_pool(PoolRecord {
            pool_id: PoolId(1),
            total_capacity: 3,
            held_capacity: 2,
            consumed_capacity: 2,
        })
        .unwrap();

        assert_eq!(
            db.validate_invariants(),
            Err(ReservationInvariantError::AccountedCapacityExceedsTotal {
                pool_id: PoolId(1),
                held_capacity: 2,
                consumed_capacity: 2,
                total_capacity: 3,
            })
        );
    }
}

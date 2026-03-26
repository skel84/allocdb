use log::warn;

use crate::command::{ClientRequest, Command, CommandContext};
use crate::config::{Config, ConfigError};
use crate::fixed_map::{FixedMap, FixedMapError};
use crate::ids::{ClientId, ClientOperationKey, HoldId, Lsn, OperationId, PoolId, Slot};
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
    pub client_id: ClientId,
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
    pub(crate) operations: FixedMap<ClientOperationKey, OperationRecord>,
    pub(crate) hold_retire_queue: RetireQueue<HoldId>,
    pub(crate) operation_retire_queue: RetireQueue<ClientOperationKey>,
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

        let operation_key = ClientOperationKey::new(request.client_id, request.operation_id);
        if let Some(record) = self.operations.get(operation_key).copied() {
            if context.request_slot.get() > record.retire_after_slot.get() {
                let removed = self.operations.remove(operation_key);
                assert!(
                    removed.is_some(),
                    "existing operation record must be removable"
                );
            } else if record.command == request.command {
                return CommandOutcome {
                    result_code: record.result_code,
                    pool_id: record.result_pool_id,
                    hold_id: record.result_hold_id,
                };
            } else {
                warn!(
                    "operation_id conflict detected client_id={} operation_id={}",
                    request.client_id.get(),
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
                Err(error) => return Self::slot_overflow_outcome(error),
            };

        let outcome = self.apply_command(context, request.command);
        let operation_record = OperationRecord {
            client_id: request.client_id,
            operation_id: request.operation_id,
            command: request.command,
            result_code: outcome.result_code,
            result_pool_id: outcome.pool_id,
            result_hold_id: outcome.hold_id,
            applied_lsn: context.lsn,
            retire_after_slot: operation_retire_after_slot,
        };
        self.insert_operation(operation_record);
        self.push_operation_retirement(
            ClientOperationKey::new(operation_record.client_id, operation_record.operation_id),
            operation_record.retire_after_slot,
        );
        self.assert_invariants();
        outcome
    }

    pub fn apply_internal(&mut self, context: CommandContext, command: Command) -> CommandOutcome {
        if let Err(error) = self.validate_internal_request_slot(context.request_slot, &command) {
            return Self::slot_overflow_outcome(error);
        }

        self.begin_apply(context);
        self.retire_state(context.request_slot);
        let outcome = self.apply_command(context, command);
        self.assert_invariants();
        outcome
    }

    fn apply_command(&mut self, context: CommandContext, command: Command) -> CommandOutcome {
        match command {
            Command::CreatePool {
                pool_id,
                total_capacity,
            } => self.apply_create_pool(pool_id, total_capacity),
            Command::PlaceHold {
                pool_id,
                hold_id,
                quantity,
                deadline_slot,
            } => self.apply_place_hold(
                context.request_slot,
                pool_id,
                hold_id,
                quantity,
                deadline_slot,
            ),
            Command::ConfirmHold { hold_id } => {
                self.apply_confirm_hold(context.request_slot, hold_id)
            }
            Command::ReleaseHold { hold_id } => self.apply_release_hold(hold_id),
            Command::ExpireHold { hold_id } => {
                self.apply_expire_hold(context.request_slot, hold_id)
            }
        }
    }

    fn apply_create_pool(&mut self, pool_id: PoolId, total_capacity: u64) -> CommandOutcome {
        if self.pools.contains_key(pool_id) {
            warn!(
                "create_pool rejected already_exists pool_id={}",
                pool_id.get()
            );
            return CommandOutcome::with_pool(ResultCode::AlreadyExists, pool_id);
        }

        if self.pools.len() == usize::try_from(self.config.max_pools).expect("validated max_pools")
        {
            warn!("pool table is full");
            return CommandOutcome::new(ResultCode::PoolTableFull);
        }

        self.insert_pool(PoolRecord {
            pool_id,
            total_capacity,
            held_capacity: 0,
            consumed_capacity: 0,
        });
        CommandOutcome::with_pool(ResultCode::Ok, pool_id)
    }

    fn apply_place_hold(
        &mut self,
        request_slot: Slot,
        pool_id: PoolId,
        hold_id: HoldId,
        quantity: u64,
        deadline_slot: Slot,
    ) -> CommandOutcome {
        if quantity == 0 || deadline_slot.get() <= request_slot.get() {
            warn!(
                "place_hold rejected invalid_state pool_id={} hold_id={} quantity={} deadline_slot={} request_slot={}",
                pool_id.get(),
                hold_id.get(),
                quantity,
                deadline_slot.get(),
                request_slot.get()
            );
            return CommandOutcome::with_pool_and_hold(ResultCode::InvalidState, pool_id, hold_id);
        }

        let Some(pool) = self.pools.get(pool_id).copied() else {
            warn!(
                "place_hold rejected pool_not_found pool_id={}",
                pool_id.get()
            );
            return CommandOutcome::with_pool(ResultCode::PoolNotFound, pool_id);
        };

        if self.holds.contains_key(hold_id) {
            warn!(
                "place_hold rejected already_exists hold_id={}",
                hold_id.get()
            );
            return CommandOutcome::with_hold(ResultCode::AlreadyExists, hold_id);
        }

        if self.holds.len() == usize::try_from(self.config.max_holds).expect("validated max_holds")
        {
            warn!("hold table is full");
            return CommandOutcome::new(ResultCode::HoldTableFull);
        }

        let available_capacity = pool
            .total_capacity
            .checked_sub(pool.held_capacity + pool.consumed_capacity)
            .expect("validated pool accounting must not exceed total capacity");
        if quantity > available_capacity {
            warn!(
                "place_hold rejected insufficient_capacity pool_id={} quantity={} available={}",
                pool_id.get(),
                quantity,
                available_capacity
            );
            return CommandOutcome::with_pool(ResultCode::InsufficientCapacity, pool_id);
        }

        let pool = self
            .pools
            .get_mut(pool_id)
            .expect("pool must remain present during place_hold");
        pool.held_capacity += quantity;
        self.insert_hold(HoldRecord {
            hold_id,
            pool_id,
            quantity,
            deadline_slot,
            state: HoldState::Held,
        });
        self.push_hold_expiry(hold_id, deadline_slot);
        CommandOutcome::with_pool_and_hold(ResultCode::Ok, pool_id, hold_id)
    }

    fn apply_confirm_hold(&mut self, request_slot: Slot, hold_id: HoldId) -> CommandOutcome {
        let Some(mut hold) = self.holds.get(hold_id).copied() else {
            warn!(
                "confirm_hold rejected hold_not_found hold_id={}",
                hold_id.get()
            );
            return CommandOutcome::with_hold(ResultCode::HoldNotFound, hold_id);
        };

        match hold.state {
            HoldState::Confirmed | HoldState::Released => {
                return CommandOutcome::with_hold(ResultCode::InvalidState, hold_id);
            }
            HoldState::Expired => {
                return CommandOutcome::with_hold(ResultCode::HoldExpired, hold_id);
            }
            HoldState::Held => {}
        }

        if request_slot.get() >= hold.deadline_slot.get() {
            let pool = self
                .pools
                .get_mut(hold.pool_id)
                .expect("hold pool must remain present during expiration");
            pool.held_capacity -= hold.quantity;
            hold.state = HoldState::Expired;
            self.replace_hold(hold);
            return CommandOutcome::with_pool_and_hold(
                ResultCode::HoldExpired,
                hold.pool_id,
                hold.hold_id,
            );
        }

        let pool = self
            .pools
            .get_mut(hold.pool_id)
            .expect("hold pool must remain present during confirm");
        pool.held_capacity -= hold.quantity;
        pool.consumed_capacity += hold.quantity;
        hold.state = HoldState::Confirmed;
        self.replace_hold(hold);
        CommandOutcome::with_pool_and_hold(ResultCode::Ok, hold.pool_id, hold.hold_id)
    }

    fn apply_release_hold(&mut self, hold_id: HoldId) -> CommandOutcome {
        let Some(mut hold) = self.holds.get(hold_id).copied() else {
            warn!(
                "release_hold rejected hold_not_found hold_id={}",
                hold_id.get()
            );
            return CommandOutcome::with_hold(ResultCode::HoldNotFound, hold_id);
        };

        match hold.state {
            HoldState::Confirmed | HoldState::Released | HoldState::Expired => {
                return CommandOutcome::with_hold(ResultCode::InvalidState, hold_id);
            }
            HoldState::Held => {}
        }

        let pool = self
            .pools
            .get_mut(hold.pool_id)
            .expect("hold pool must remain present during release");
        pool.held_capacity -= hold.quantity;
        hold.state = HoldState::Released;
        self.replace_hold(hold);
        CommandOutcome::with_pool_and_hold(ResultCode::Ok, hold.pool_id, hold.hold_id)
    }

    fn apply_expire_hold(&mut self, request_slot: Slot, hold_id: HoldId) -> CommandOutcome {
        let Some(mut hold) = self.holds.get(hold_id).copied() else {
            warn!(
                "expire_hold rejected hold_not_found hold_id={}",
                hold_id.get()
            );
            return CommandOutcome::with_hold(ResultCode::HoldNotFound, hold_id);
        };

        match hold.state {
            HoldState::Confirmed | HoldState::Released | HoldState::Expired => {
                return CommandOutcome::with_hold(ResultCode::InvalidState, hold_id);
            }
            HoldState::Held => {}
        }

        if request_slot.get() < hold.deadline_slot.get() {
            return CommandOutcome::with_pool_and_hold(
                ResultCode::InvalidState,
                hold.pool_id,
                hold.hold_id,
            );
        }

        let pool = self
            .pools
            .get_mut(hold.pool_id)
            .expect("hold pool must remain present during expire");
        pool.held_capacity -= hold.quantity;
        hold.state = HoldState::Expired;
        self.replace_hold(hold);
        CommandOutcome::with_pool_and_hold(ResultCode::Ok, hold.pool_id, hold.hold_id)
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
        self.operations.insert(
            ClientOperationKey::new(record.client_id, record.operation_id),
            record,
        )
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
        entries: &mut Vec<(ClientOperationKey, Slot, u64)>,
    ) {
        entries.sort_unstable_by_key(|(_, retire_after_slot, applied_lsn)| {
            (retire_after_slot.get(), *applied_lsn)
        });

        for (operation_key, retire_after_slot, _) in entries.drain(..) {
            match self.operation_retire_queue.push(RetireEntry {
                key: operation_key,
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
        operations
            .sort_unstable_by_key(|record| (record.client_id.get(), record.operation_id.get()));

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

    fn insert_pool(&mut self, record: PoolRecord) {
        match self.pools.insert(record.pool_id, record) {
            Ok(()) => {}
            Err(FixedMapError::DuplicateKey | FixedMapError::Full) => {
                panic!("pool inserts must respect capacity and uniqueness")
            }
        }
    }

    fn insert_hold(&mut self, record: HoldRecord) {
        match self.holds.insert(record.hold_id, record) {
            Ok(()) => {}
            Err(FixedMapError::DuplicateKey | FixedMapError::Full) => {
                panic!("hold inserts must respect capacity and uniqueness")
            }
        }
    }

    fn replace_hold(&mut self, record: HoldRecord) {
        let removed = self.holds.remove(record.hold_id);
        assert!(removed.is_some(), "existing hold must be removable");
        self.insert_hold(record);
    }

    fn insert_operation(&mut self, record: OperationRecord) {
        let operation_key = ClientOperationKey::new(record.client_id, record.operation_id);
        match self.operations.insert(operation_key, record) {
            Ok(()) => {}
            Err(FixedMapError::DuplicateKey | FixedMapError::Full) => {
                panic!("operation inserts must respect capacity and uniqueness")
            }
        }
    }

    fn push_operation_retirement(
        &mut self,
        operation_key: ClientOperationKey,
        retire_after_slot: Slot,
    ) {
        match self.operation_retire_queue.push(RetireEntry {
            key: operation_key,
            retire_after_slot,
        }) {
            Ok(()) => {}
            Err(RetireQueueError::Full) => {
                panic!("operation retire queue must stay within operation capacity")
            }
        }
    }

    fn push_hold_expiry(&mut self, hold_id: HoldId, deadline_slot: Slot) {
        match self.hold_retire_queue.push(RetireEntry {
            key: hold_id,
            retire_after_slot: deadline_slot,
        }) {
            Ok(()) => {}
            Err(RetireQueueError::Full) => {
                panic!("hold retire queue must stay within hold capacity")
            }
        }
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
        while let Some(entry) = self.hold_retire_queue.front() {
            if entry.retire_after_slot.get() >= request_slot.get() {
                break;
            }

            let hold_id = entry.key;
            let popped = self.hold_retire_queue.pop_front();
            assert!(popped.is_some(), "peeked hold retirement entry must pop");

            let Some(mut hold) = self.holds.get(hold_id).copied() else {
                warn!(
                    "hold retirement queue referenced missing hold_id={}",
                    hold_id.get()
                );
                continue;
            };

            if hold.state != HoldState::Held {
                continue;
            }

            let pool = self
                .pools
                .get_mut(hold.pool_id)
                .expect("hold pool must remain present during automatic expiry");
            pool.held_capacity -= hold.quantity;
            hold.state = HoldState::Expired;
            self.replace_hold(hold);
        }

        while let Some(entry) = self.operation_retire_queue.front() {
            if entry.retire_after_slot.get() > request_slot.get() {
                break;
            }

            let removed = self.operations.remove(entry.key);
            if removed.is_none() {
                warn!(
                    "operation retirement queue referenced missing operation_id={}",
                    entry.key.operation_id.get()
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

    fn context(lsn: u64, request_slot: u64) -> CommandContext {
        CommandContext {
            lsn: Lsn(lsn),
            request_slot: Slot(request_slot),
        }
    }

    fn request_for(client_id: u128, operation_id: u128, command: Command) -> ClientRequest {
        ClientRequest {
            operation_id: OperationId(operation_id),
            client_id: ClientId(client_id),
            command,
        }
    }

    fn request(operation_id: u128, command: Command) -> ClientRequest {
        request_for(1, operation_id, command)
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
    fn duplicate_retry_after_success_returns_cached_outcome() {
        let mut db = ReservationDb::new(config()).unwrap();
        let create = request(
            1,
            Command::CreatePool {
                pool_id: PoolId(13),
                total_capacity: 5,
            },
        );

        let first = db.apply_client(context(1, 2), create);
        let retry = db.apply_client(context(2, 3), create);

        assert_eq!(first.result_code, ResultCode::Ok);
        assert_eq!(retry, first);
        assert_eq!(db.snapshot().pools.len(), 1);
        assert_eq!(db.snapshot().operations.len(), 1);
    }

    #[test]
    fn duplicate_retry_after_failure_returns_cached_outcome() {
        let mut db = ReservationDb::new(config()).unwrap();
        db.apply_client(
            context(1, 1),
            request(
                1,
                Command::CreatePool {
                    pool_id: PoolId(13),
                    total_capacity: 2,
                },
            ),
        );

        let place_hold = request(
            2,
            Command::PlaceHold {
                pool_id: PoolId(13),
                hold_id: HoldId(21),
                quantity: 3,
                deadline_slot: Slot(10),
            },
        );

        let first = db.apply_client(context(2, 2), place_hold);
        let retry = db.apply_client(context(3, 3), place_hold);

        assert_eq!(first.result_code, ResultCode::InsufficientCapacity);
        assert_eq!(retry, first);
        assert_eq!(db.snapshot().holds.len(), 0);
    }

    #[test]
    fn conflicting_operation_reuse_returns_conflict() {
        let mut db = ReservationDb::new(config()).unwrap();
        db.apply_client(
            context(1, 1),
            request(
                1,
                Command::CreatePool {
                    pool_id: PoolId(11),
                    total_capacity: 5,
                },
            ),
        );

        let conflicting = request(
            1,
            Command::CreatePool {
                pool_id: PoolId(12),
                total_capacity: 7,
            },
        );
        let outcome = db.apply_client(context(2, 2), conflicting);

        assert_eq!(outcome.result_code, ResultCode::OperationConflict);
        assert_eq!(db.snapshot().pools.len(), 1);
    }

    #[test]
    fn same_operation_id_is_scoped_per_client() {
        let mut db = ReservationDb::new(config()).unwrap();

        let first = db.apply_client(
            context(1, 1),
            request_for(
                1,
                1,
                Command::CreatePool {
                    pool_id: PoolId(11),
                    total_capacity: 5,
                },
            ),
        );
        let second = db.apply_client(
            context(2, 2),
            request_for(
                2,
                1,
                Command::CreatePool {
                    pool_id: PoolId(12),
                    total_capacity: 7,
                },
            ),
        );
        let retry = db.apply_client(
            context(3, 3),
            request_for(
                2,
                1,
                Command::CreatePool {
                    pool_id: PoolId(12),
                    total_capacity: 7,
                },
            ),
        );

        assert_eq!(first.result_code, ResultCode::Ok);
        assert_eq!(second.result_code, ResultCode::Ok);
        assert_eq!(retry, second);
        assert_eq!(db.snapshot().pools.len(), 2);
        assert_eq!(db.snapshot().operations.len(), 2);
    }

    #[test]
    fn place_hold_reduces_available_capacity_exactly_once() {
        let mut db = ReservationDb::new(config()).unwrap();
        db.apply_client(
            context(1, 1),
            request(
                1,
                Command::CreatePool {
                    pool_id: PoolId(11),
                    total_capacity: 10,
                },
            ),
        );

        let place_hold = request(
            2,
            Command::PlaceHold {
                pool_id: PoolId(11),
                hold_id: HoldId(21),
                quantity: 4,
                deadline_slot: Slot(9),
            },
        );
        let first = db.apply_client(context(2, 2), place_hold);
        let retry = db.apply_client(context(3, 3), place_hold);

        assert_eq!(first.result_code, ResultCode::Ok);
        assert_eq!(retry, first);
        assert_eq!(db.snapshot().pools[0].held_capacity, 4);
        assert_eq!(db.snapshot().pools[0].consumed_capacity, 0);
        assert_eq!(db.snapshot().holds[0].state, HoldState::Held);
    }

    #[test]
    fn place_hold_rejects_zero_quantity_and_elapsed_deadline() {
        let mut db = ReservationDb::new(config()).unwrap();
        db.apply_client(
            context(1, 1),
            request(
                1,
                Command::CreatePool {
                    pool_id: PoolId(11),
                    total_capacity: 10,
                },
            ),
        );

        let zero_quantity = db.apply_client(
            context(2, 2),
            request(
                2,
                Command::PlaceHold {
                    pool_id: PoolId(11),
                    hold_id: HoldId(21),
                    quantity: 0,
                    deadline_slot: Slot(5),
                },
            ),
        );
        let elapsed_deadline = db.apply_client(
            context(3, 4),
            request(
                3,
                Command::PlaceHold {
                    pool_id: PoolId(11),
                    hold_id: HoldId(22),
                    quantity: 1,
                    deadline_slot: Slot(4),
                },
            ),
        );

        assert_eq!(zero_quantity.result_code, ResultCode::InvalidState);
        assert_eq!(elapsed_deadline.result_code, ResultCode::InvalidState);
        assert_eq!(db.snapshot().holds.len(), 0);
        assert_eq!(db.snapshot().pools[0].held_capacity, 0);
    }

    #[test]
    fn confirm_hold_transitions_to_confirmed_and_consumes_capacity() {
        let mut db = ReservationDb::new(config()).unwrap();
        db.apply_client(
            context(1, 1),
            request(
                1,
                Command::CreatePool {
                    pool_id: PoolId(11),
                    total_capacity: 10,
                },
            ),
        );
        db.apply_client(
            context(2, 2),
            request(
                2,
                Command::PlaceHold {
                    pool_id: PoolId(11),
                    hold_id: HoldId(21),
                    quantity: 3,
                    deadline_slot: Slot(8),
                },
            ),
        );

        let outcome = db.apply_client(
            context(3, 3),
            request(
                3,
                Command::ConfirmHold {
                    hold_id: HoldId(21),
                },
            ),
        );

        assert_eq!(outcome.result_code, ResultCode::Ok);
        assert_eq!(db.snapshot().pools[0].held_capacity, 0);
        assert_eq!(db.snapshot().pools[0].consumed_capacity, 3);
        assert_eq!(db.snapshot().holds[0].state, HoldState::Confirmed);
    }

    #[test]
    fn release_hold_transitions_to_released_and_frees_capacity() {
        let mut db = ReservationDb::new(config()).unwrap();
        db.apply_client(
            context(1, 1),
            request(
                1,
                Command::CreatePool {
                    pool_id: PoolId(11),
                    total_capacity: 10,
                },
            ),
        );
        db.apply_client(
            context(2, 2),
            request(
                2,
                Command::PlaceHold {
                    pool_id: PoolId(11),
                    hold_id: HoldId(21),
                    quantity: 3,
                    deadline_slot: Slot(8),
                },
            ),
        );

        let outcome = db.apply_client(
            context(3, 7),
            request(
                3,
                Command::ReleaseHold {
                    hold_id: HoldId(21),
                },
            ),
        );

        assert_eq!(outcome.result_code, ResultCode::Ok);
        assert_eq!(db.snapshot().pools[0].held_capacity, 0);
        assert_eq!(db.snapshot().pools[0].consumed_capacity, 0);
        assert_eq!(db.snapshot().holds[0].state, HoldState::Released);
    }

    #[test]
    fn confirm_after_deadline_expires_hold_and_releases_capacity() {
        let mut db = ReservationDb::new(config()).unwrap();
        db.apply_client(
            context(1, 1),
            request(
                1,
                Command::CreatePool {
                    pool_id: PoolId(11),
                    total_capacity: 10,
                },
            ),
        );
        db.apply_client(
            context(2, 2),
            request(
                2,
                Command::PlaceHold {
                    pool_id: PoolId(11),
                    hold_id: HoldId(21),
                    quantity: 3,
                    deadline_slot: Slot(5),
                },
            ),
        );

        let outcome = db.apply_client(
            context(3, 5),
            request(
                3,
                Command::ConfirmHold {
                    hold_id: HoldId(21),
                },
            ),
        );

        assert_eq!(outcome.result_code, ResultCode::HoldExpired);
        assert_eq!(db.snapshot().pools[0].held_capacity, 0);
        assert_eq!(db.snapshot().pools[0].consumed_capacity, 0);
        assert_eq!(db.snapshot().holds[0].state, HoldState::Expired);
    }

    #[test]
    fn overdue_hold_auto_expires_before_later_request() {
        let mut db = ReservationDb::new(config()).unwrap();
        db.apply_client(
            context(1, 1),
            request(
                1,
                Command::CreatePool {
                    pool_id: PoolId(11),
                    total_capacity: 5,
                },
            ),
        );
        db.apply_client(
            context(2, 2),
            request(
                2,
                Command::PlaceHold {
                    pool_id: PoolId(11),
                    hold_id: HoldId(21),
                    quantity: 5,
                    deadline_slot: Slot(5),
                },
            ),
        );

        let outcome = db.apply_client(
            context(3, 20),
            request(
                3,
                Command::PlaceHold {
                    pool_id: PoolId(11),
                    hold_id: HoldId(22),
                    quantity: 5,
                    deadline_slot: Slot(30),
                },
            ),
        );

        assert_eq!(outcome.result_code, ResultCode::Ok);
        assert_eq!(outcome.pool_id, Some(PoolId(11)));
        assert_eq!(outcome.hold_id, Some(HoldId(22)));
        assert_eq!(db.snapshot().pools[0].held_capacity, 5);
        assert_eq!(
            db.snapshot()
                .holds
                .iter()
                .find(|record| record.hold_id == HoldId(21))
                .unwrap()
                .state,
            HoldState::Expired
        );
    }

    #[test]
    fn duplicate_place_hold_retry_after_deadline_returns_cached_outcome() {
        let mut db = ReservationDb::new(config()).unwrap();
        db.apply_client(
            context(1, 1),
            request(
                1,
                Command::CreatePool {
                    pool_id: PoolId(11),
                    total_capacity: 5,
                },
            ),
        );
        let first = db.apply_client(
            context(2, 2),
            request(
                2,
                Command::PlaceHold {
                    pool_id: PoolId(11),
                    hold_id: HoldId(21),
                    quantity: 5,
                    deadline_slot: Slot(5),
                },
            ),
        );

        let retry = db.apply_client(
            context(3, 6),
            request(
                2,
                Command::PlaceHold {
                    pool_id: PoolId(11),
                    hold_id: HoldId(21),
                    quantity: 5,
                    deadline_slot: Slot(5),
                },
            ),
        );

        assert_eq!(retry, first);
        assert_eq!(db.snapshot().pools[0].held_capacity, 0);
        assert_eq!(db.snapshot().holds[0].state, HoldState::Expired);
    }

    #[test]
    fn expire_hold_transitions_to_expired_once_due() {
        let mut db = ReservationDb::new(config()).unwrap();
        db.apply_client(
            context(1, 1),
            request(
                1,
                Command::CreatePool {
                    pool_id: PoolId(11),
                    total_capacity: 10,
                },
            ),
        );
        db.apply_client(
            context(2, 2),
            request(
                2,
                Command::PlaceHold {
                    pool_id: PoolId(11),
                    hold_id: HoldId(21),
                    quantity: 3,
                    deadline_slot: Slot(5),
                },
            ),
        );

        let first = db.apply_client(
            context(3, 5),
            request(
                3,
                Command::ExpireHold {
                    hold_id: HoldId(21),
                },
            ),
        );
        let retry = db.apply_client(
            context(4, 6),
            request(
                3,
                Command::ExpireHold {
                    hold_id: HoldId(21),
                },
            ),
        );

        assert_eq!(first.result_code, ResultCode::Ok);
        assert_eq!(retry, first);
        assert_eq!(db.snapshot().pools[0].held_capacity, 0);
        assert_eq!(db.snapshot().holds[0].state, HoldState::Expired);
    }

    #[test]
    fn operation_table_pressure_returns_deterministic_rejection() {
        let mut config = config();
        config.max_operations = 1;
        let mut db = ReservationDb::new(config).unwrap();

        db.apply_client(
            context(1, 1),
            request(
                1,
                Command::CreatePool {
                    pool_id: PoolId(11),
                    total_capacity: 10,
                },
            ),
        );

        let outcome = db.apply_client(
            context(2, 2),
            request(
                2,
                Command::PlaceHold {
                    pool_id: PoolId(11),
                    hold_id: HoldId(21),
                    quantity: 1,
                    deadline_slot: Slot(5),
                },
            ),
        );

        assert_eq!(outcome.result_code, ResultCode::OperationTableFull);
        assert_eq!(db.snapshot().holds.len(), 0);
    }

    #[test]
    fn snapshot_restore_and_replay_preserve_suffix_outcomes() {
        let mut live = ReservationDb::new(config()).unwrap();
        let prefix = [
            (
                context(1, 1),
                request(
                    1,
                    Command::CreatePool {
                        pool_id: PoolId(11),
                        total_capacity: 10,
                    },
                ),
            ),
            (
                context(2, 2),
                request(
                    2,
                    Command::PlaceHold {
                        pool_id: PoolId(11),
                        hold_id: HoldId(21),
                        quantity: 3,
                        deadline_slot: Slot(8),
                    },
                ),
            ),
        ];

        for (ctx, req) in prefix {
            let _ = live.apply_client(ctx, req);
        }

        let checkpoint = live.snapshot();
        let suffix = [
            (
                context(3, 3),
                request(
                    2,
                    Command::PlaceHold {
                        pool_id: PoolId(11),
                        hold_id: HoldId(21),
                        quantity: 3,
                        deadline_slot: Slot(8),
                    },
                ),
            ),
            (
                context(4, 4),
                request(
                    3,
                    Command::ConfirmHold {
                        hold_id: HoldId(21),
                    },
                ),
            ),
            (
                context(5, 5),
                request(
                    3,
                    Command::ConfirmHold {
                        hold_id: HoldId(21),
                    },
                ),
            ),
        ];

        let live_outcomes: Vec<_> = suffix
            .iter()
            .map(|(ctx, req)| live.apply_client(*ctx, *req))
            .collect();
        let live_final = live.snapshot();

        let mut replay = ReservationDb::from_snapshot(config(), checkpoint.clone()).unwrap();
        let replay_outcomes: Vec<_> = suffix
            .iter()
            .map(|(ctx, req)| replay.apply_client(*ctx, *req))
            .collect();

        assert_eq!(replay_outcomes, live_outcomes);
        assert_eq!(replay.snapshot(), live_final);
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

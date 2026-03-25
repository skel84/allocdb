use log::{debug, warn};

use crate::command::{ClientRequest, Command, CommandContext};
use crate::config::{Config, ConfigError};
use crate::fixed_map::{FixedMap, FixedMapError};
use crate::ids::{BucketId, Lsn, OperationId, Slot};
use crate::result::{CommandOutcome, ResultCode};
use crate::retire_queue::{RetireEntry, RetireQueue, RetireQueueError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BucketRecord {
    pub bucket_id: BucketId,
    pub limit: u64,
    pub balance: u64,
    pub last_refill_slot: Slot,
    pub refill_rate_per_slot: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OperationRecord {
    pub operation_id: OperationId,
    pub command_fingerprint: u128,
    pub result_code: ResultCode,
    pub result_bucket_id: Option<BucketId>,
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
pub enum QuotaInvariantError {
    BalanceExceedsLimit {
        bucket_id: BucketId,
        balance: u64,
        limit: u64,
    },
}

#[derive(Debug)]
pub struct QuotaDb {
    pub(crate) config: Config,
    pub(crate) buckets: FixedMap<BucketId, BucketRecord>,
    pub(crate) operations: FixedMap<OperationId, OperationRecord>,
    pub(crate) operation_retire_queue: RetireQueue<OperationId>,
    pub(crate) last_applied_lsn: Option<Lsn>,
    pub(crate) last_request_slot: Option<Slot>,
}

impl QuotaDb {
    /// Creates one empty deterministic quota engine with fixed-capacity in-memory state.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] when the supplied configuration is internally inconsistent.
    pub fn new(config: Config) -> Result<Self, ConfigError> {
        config.validate()?;

        let max_buckets =
            usize::try_from(config.max_buckets).expect("validated max_buckets must fit usize");
        let max_operations = usize::try_from(config.max_operations)
            .expect("validated max_operations must fit usize");

        Ok(Self {
            config,
            buckets: FixedMap::with_capacity(max_buckets),
            operations: FixedMap::with_capacity(max_operations),
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
                    bucket_id: record.result_bucket_id,
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
            result_bucket_id: outcome.bucket_id,
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

    fn apply_command(&mut self, context: CommandContext, command: Command) -> CommandOutcome {
        match command {
            Command::CreateBucket {
                bucket_id,
                limit,
                initial_balance,
                refill_rate_per_slot,
            } => self.apply_create_bucket(
                context,
                bucket_id,
                limit,
                initial_balance,
                refill_rate_per_slot,
            ),
            Command::Debit { bucket_id, amount } => self.apply_debit(context, bucket_id, amount),
        }
    }

    fn apply_create_bucket(
        &mut self,
        context: CommandContext,
        bucket_id: BucketId,
        limit: u64,
        initial_balance: u64,
        refill_rate_per_slot: u64,
    ) -> CommandOutcome {
        assert!(
            initial_balance <= limit,
            "validated create_bucket must satisfy initial_balance <= limit"
        );

        if self.buckets.contains_key(bucket_id) {
            warn!(
                "create_bucket rejected already_exists bucket_id={}",
                bucket_id.get()
            );
            return CommandOutcome::new(ResultCode::AlreadyExists);
        }

        if self.buckets.len()
            == usize::try_from(self.config.max_buckets).expect("validated max_buckets")
        {
            warn!("bucket table is full");
            return CommandOutcome::new(ResultCode::BucketTableFull);
        }

        self.insert_bucket(BucketRecord {
            bucket_id,
            limit,
            balance: initial_balance,
            last_refill_slot: context.request_slot,
            refill_rate_per_slot,
        });
        debug!("created bucket bucket_id={}", bucket_id.get());
        CommandOutcome::with_bucket(ResultCode::Ok, bucket_id)
    }

    fn apply_debit(
        &mut self,
        context: CommandContext,
        bucket_id: BucketId,
        amount: u64,
    ) -> CommandOutcome {
        assert!(
            amount > 0,
            "validated debit amount must be strictly positive"
        );

        let Some(_) = self.buckets.get(bucket_id) else {
            warn!(
                "debit rejected bucket_not_found bucket_id={}",
                bucket_id.get()
            );
            return CommandOutcome::new(ResultCode::BucketNotFound);
        };

        let refill = {
            let bucket = self
                .buckets
                .get_mut(bucket_id)
                .expect("bucket must remain present during debit");
            Self::refill_bucket_to_slot(bucket, context.request_slot)
        };
        if let Err(error) = refill {
            return Self::slot_overflow_outcome("debit", error);
        }

        let bucket = self
            .buckets
            .get(bucket_id)
            .copied()
            .expect("bucket must remain present after refill");
        if bucket.balance < amount {
            warn!(
                "debit rejected insufficient_funds bucket_id={} balance={} amount={}",
                bucket_id.get(),
                bucket.balance,
                amount
            );
            return CommandOutcome::new(ResultCode::InsufficientFunds);
        }

        let bucket = self
            .buckets
            .get_mut(bucket_id)
            .expect("bucket must remain present during debit");
        bucket.balance -= amount;
        debug!(
            "debited bucket bucket_id={} amount={} balance={}",
            bucket_id.get(),
            amount,
            bucket.balance
        );
        CommandOutcome::with_bucket(ResultCode::Ok, bucket_id)
    }

    fn refill_bucket_to_slot(
        bucket: &mut BucketRecord,
        request_slot: Slot,
    ) -> Result<(), SlotOverflowError> {
        assert!(
            request_slot.get() >= bucket.last_refill_slot.get(),
            "bucket refill slots must not move backwards"
        );

        let slots_elapsed = request_slot.get() - bucket.last_refill_slot.get();
        if slots_elapsed == 0 || bucket.refill_rate_per_slot == 0 || bucket.balance == bucket.limit
        {
            bucket.last_refill_slot = request_slot;
            return Ok(());
        }

        let missing = bucket.limit - bucket.balance;
        let slots_needed = missing
            .saturating_sub(1)
            .checked_div(bucket.refill_rate_per_slot)
            .expect("refill_rate_per_slot must stay non-zero")
            .saturating_add(1);

        if slots_elapsed >= slots_needed {
            bucket.balance = bucket.limit;
            bucket.last_refill_slot = request_slot;
            return Ok(());
        }

        let added = slots_elapsed
            .checked_mul(bucket.refill_rate_per_slot)
            .ok_or(SlotOverflowError {
                kind: SlotOverflowKind::OperationWindow,
                request_slot,
                delta: bucket.refill_rate_per_slot,
            })?;
        debug_assert!(added < missing);
        bucket.balance += added;
        bucket.last_refill_slot = request_slot;
        Ok(())
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

    fn retire_state(&mut self, current_slot: Slot) {
        self.retire_operations(current_slot);
    }

    fn retire_operations(&mut self, current_slot: Slot) {
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

    pub(crate) fn operation_retire_after_slot(
        &self,
        request_slot: Slot,
    ) -> Result<Slot, SlotOverflowError> {
        Self::checked_slot_add(
            request_slot,
            self.config.max_client_retry_window_slots,
            SlotOverflowKind::OperationWindow,
        )
    }

    fn checked_slot_add(
        request_slot: Slot,
        delta: u64,
        kind: SlotOverflowKind,
    ) -> Result<Slot, SlotOverflowError> {
        request_slot
            .get()
            .checked_add(delta)
            .map(Slot)
            .ok_or(SlotOverflowError {
                kind,
                request_slot,
                delta,
            })
    }

    pub(crate) fn slot_overflow_outcome(
        operation: &'static str,
        error: SlotOverflowError,
    ) -> CommandOutcome {
        warn!(
            "{operation} rejected slot_overflow kind={:?} request_slot={} delta={}",
            error.kind,
            error.request_slot.get(),
            error.delta
        );
        CommandOutcome::new(ResultCode::SlotOverflow)
    }

    pub(crate) fn restore_bucket(&mut self, record: BucketRecord) -> Result<(), FixedMapError> {
        self.buckets.insert(record.bucket_id, record)
    }

    pub(crate) fn restore_operation(
        &mut self,
        record: OperationRecord,
    ) -> Result<(), FixedMapError> {
        self.operations.insert(record.operation_id, record)
    }

    pub(crate) fn rebuild_operation_retire_queue(
        &mut self,
        operation_entries: &mut Vec<(OperationId, Slot, u64)>,
    ) {
        operation_entries.sort_unstable_by_key(|entry| (entry.1.get(), entry.2));
        for (operation_id, retire_after_slot, _) in operation_entries.drain(..) {
            self.push_operation_retirement(operation_id, retire_after_slot);
        }
    }

    pub(crate) fn set_progress(&mut self, lsn: Option<Lsn>, request_slot: Option<Slot>) {
        self.last_applied_lsn = lsn;
        self.last_request_slot = request_slot;
    }

    pub(crate) fn insert_bucket(&mut self, record: BucketRecord) {
        match self.buckets.insert(record.bucket_id, record) {
            Ok(()) => {}
            Err(FixedMapError::DuplicateKey | FixedMapError::Full) => {
                panic!("bucket inserts must respect capacity and uniqueness")
            }
        }
    }

    pub(crate) fn insert_operation(&mut self, record: OperationRecord) {
        match self.operations.insert(record.operation_id, record) {
            Ok(()) => {}
            Err(FixedMapError::DuplicateKey | FixedMapError::Full) => {
                panic!("operation inserts must respect capacity and uniqueness")
            }
        }
    }

    pub(crate) fn push_operation_retirement(
        &mut self,
        operation_id: OperationId,
        retire_after_slot: Slot,
    ) {
        match self.operation_retire_queue.push(RetireEntry {
            key: operation_id,
            retire_after_slot,
        }) {
            Ok(()) => {}
            Err(RetireQueueError::Full) => {
                panic!("operation retire queue must stay within operation capacity")
            }
        }
    }

    /// Captures one stable snapshot of the current trusted-core quota state.
    #[must_use]
    pub fn snapshot(&self) -> crate::snapshot::Snapshot {
        let mut buckets: Vec<_> = self.buckets.iter().copied().collect();
        buckets.sort_unstable_by_key(|record| record.bucket_id.get());

        let mut operations: Vec<_> = self.operations.iter().copied().collect();
        operations.sort_unstable_by_key(|record| record.operation_id.get());

        crate::snapshot::Snapshot {
            last_applied_lsn: self.last_applied_lsn,
            last_request_slot: self.last_request_slot,
            buckets,
            operations,
        }
    }

    /// Validates trusted-core quota invariants.
    ///
    /// # Errors
    ///
    /// Returns [`QuotaInvariantError`] when one restored record violates quota bounds.
    pub fn validate_invariants(&self) -> Result<(), QuotaInvariantError> {
        for bucket in self.buckets.iter() {
            if bucket.balance > bucket.limit {
                return Err(QuotaInvariantError::BalanceExceedsLimit {
                    bucket_id: bucket.bucket_id,
                    balance: bucket.balance,
                    limit: bucket.limit,
                });
            }
        }

        Ok(())
    }

    fn assert_invariants(&self) {
        if let Err(error) = self.validate_invariants() {
            panic!("quota invariants violated: {error:?}");
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        Config,
        command::{ClientRequest, Command, CommandContext},
        ids::{BucketId, ClientId, Lsn, OperationId, Slot},
        result::ResultCode,
    };

    use super::{BucketRecord, QuotaDb};

    fn config() -> Config {
        Config {
            max_buckets: 8,
            max_operations: 16,
            max_batch_len: 8,
            max_client_retry_window_slots: 8,
            max_wal_payload_bytes: 1024,
        }
    }

    fn context(lsn: u64, request_slot: u64) -> CommandContext {
        CommandContext {
            lsn: Lsn(lsn),
            request_slot: Slot(request_slot),
        }
    }

    fn request(operation_id: u128, command: Command) -> ClientRequest {
        ClientRequest {
            operation_id: OperationId(operation_id),
            client_id: ClientId(1),
            command,
        }
    }

    #[test]
    fn snapshot_orders_records_stably() {
        let mut db = QuotaDb::new(config()).unwrap();
        db.restore_bucket(BucketRecord {
            bucket_id: BucketId(9),
            limit: 10,
            balance: 5,
            last_refill_slot: Slot(1),
            refill_rate_per_slot: 0,
        })
        .unwrap();
        db.restore_bucket(BucketRecord {
            bucket_id: BucketId(3),
            limit: 8,
            balance: 2,
            last_refill_slot: Slot(1),
            refill_rate_per_slot: 0,
        })
        .unwrap();

        let snapshot = db.snapshot();
        assert_eq!(
            snapshot
                .buckets
                .iter()
                .map(|record| record.bucket_id.get())
                .collect::<Vec<_>>(),
            vec![3, 9]
        );
    }

    #[test]
    fn invariant_rejects_balance_above_limit() {
        let mut db = QuotaDb::new(config()).unwrap();
        db.restore_bucket(BucketRecord {
            bucket_id: BucketId(1),
            limit: 5,
            balance: 6,
            last_refill_slot: Slot(1),
            refill_rate_per_slot: 0,
        })
        .unwrap();

        assert!(db.validate_invariants().is_err());
    }

    #[test]
    fn duplicate_retry_after_success_returns_cached_outcome() {
        let mut db = QuotaDb::new(config()).unwrap();
        let create = request(
            1,
            Command::CreateBucket {
                bucket_id: BucketId(11),
                limit: 10,
                initial_balance: 7,
                refill_rate_per_slot: 0,
            },
        );

        let first = db.apply_client(context(1, 1), create);
        let retry = db.apply_client(context(2, 2), create);

        assert_eq!(first.result_code, ResultCode::Ok);
        assert_eq!(retry, first);
        assert_eq!(db.snapshot().buckets.len(), 1);
        assert_eq!(db.snapshot().operations.len(), 1);
    }

    #[test]
    fn duplicate_retry_after_failure_returns_cached_outcome() {
        let mut db = QuotaDb::new(config()).unwrap();
        db.apply_client(
            context(1, 1),
            request(
                1,
                Command::CreateBucket {
                    bucket_id: BucketId(11),
                    limit: 5,
                    initial_balance: 2,
                    refill_rate_per_slot: 0,
                },
            ),
        );

        let debit = request(
            2,
            Command::Debit {
                bucket_id: BucketId(11),
                amount: 4,
            },
        );

        let first = db.apply_client(context(2, 2), debit);
        let retry = db.apply_client(context(3, 3), debit);

        assert_eq!(first.result_code, ResultCode::InsufficientFunds);
        assert_eq!(retry, first);
        assert_eq!(db.snapshot().buckets[0].balance, 2);
    }

    #[test]
    fn conflicting_operation_reuse_returns_conflict() {
        let mut db = QuotaDb::new(config()).unwrap();
        let create = request(
            1,
            Command::CreateBucket {
                bucket_id: BucketId(11),
                limit: 10,
                initial_balance: 7,
                refill_rate_per_slot: 0,
            },
        );
        db.apply_client(context(1, 1), create);

        let conflicting = request(
            1,
            Command::CreateBucket {
                bucket_id: BucketId(12),
                limit: 8,
                initial_balance: 4,
                refill_rate_per_slot: 0,
            },
        );
        let outcome = db.apply_client(context(2, 2), conflicting);

        assert_eq!(outcome.result_code, ResultCode::OperationConflict);
        assert_eq!(db.snapshot().buckets.len(), 1);
    }

    #[test]
    fn debit_reduces_balance_exactly_once() {
        let mut db = QuotaDb::new(config()).unwrap();
        db.apply_client(
            context(1, 1),
            request(
                1,
                Command::CreateBucket {
                    bucket_id: BucketId(11),
                    limit: 10,
                    initial_balance: 7,
                    refill_rate_per_slot: 0,
                },
            ),
        );

        let debit = request(
            2,
            Command::Debit {
                bucket_id: BucketId(11),
                amount: 3,
            },
        );
        let first = db.apply_client(context(2, 2), debit);
        let retry = db.apply_client(context(3, 3), debit);

        assert_eq!(first.result_code, ResultCode::Ok);
        assert_eq!(retry, first);
        assert_eq!(db.snapshot().buckets[0].balance, 4);
    }

    #[test]
    fn operation_table_pressure_returns_deterministic_rejection() {
        let mut config = config();
        config.max_operations = 1;
        let mut db = QuotaDb::new(config).unwrap();

        db.apply_client(
            context(1, 1),
            request(
                1,
                Command::CreateBucket {
                    bucket_id: BucketId(11),
                    limit: 10,
                    initial_balance: 7,
                    refill_rate_per_slot: 0,
                },
            ),
        );

        let outcome = db.apply_client(
            context(2, 2),
            request(
                2,
                Command::Debit {
                    bucket_id: BucketId(11),
                    amount: 1,
                },
            ),
        );

        assert_eq!(outcome.result_code, ResultCode::OperationTableFull);
        assert_eq!(db.snapshot().buckets[0].balance, 7);
    }

    #[test]
    fn debit_refills_by_logical_slot_before_apply() {
        let mut db = QuotaDb::new(config()).unwrap();
        db.apply_client(
            context(1, 1),
            request(
                1,
                Command::CreateBucket {
                    bucket_id: BucketId(11),
                    limit: 10,
                    initial_balance: 0,
                    refill_rate_per_slot: 2,
                },
            ),
        );

        let outcome = db.apply_client(
            context(2, 4),
            request(
                2,
                Command::Debit {
                    bucket_id: BucketId(11),
                    amount: 5,
                },
            ),
        );

        assert_eq!(outcome.result_code, ResultCode::Ok);
        assert_eq!(db.snapshot().buckets[0].balance, 1);
        assert_eq!(db.snapshot().buckets[0].last_refill_slot, Slot(4));
    }

    #[test]
    fn refill_caps_at_limit_without_overflow() {
        let mut bucket = BucketRecord {
            bucket_id: BucketId(11),
            limit: u64::MAX,
            balance: 0,
            last_refill_slot: Slot(1),
            refill_rate_per_slot: 2,
        };

        QuotaDb::refill_bucket_to_slot(&mut bucket, Slot(u64::MAX)).unwrap();

        assert_eq!(bucket.balance, u64::MAX);
        assert_eq!(bucket.last_refill_slot, Slot(u64::MAX));
    }
}

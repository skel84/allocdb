use crate::Command;
use crate::config::{Config, ConfigError};
use crate::fixed_map::FixedMapError;
use crate::ids::{HoldId, Lsn, OperationId, PoolId, Slot};
use crate::result::ResultCode;
use crate::state_machine::{
    HoldRecord, HoldState, OperationRecord, PoolRecord, ReservationDb, ReservationInvariantError,
};

const MAGIC: u32 = 0x5253_5653;
const VERSION: u16 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Snapshot {
    pub last_applied_lsn: Option<Lsn>,
    pub last_request_slot: Option<Slot>,
    pub pools: Vec<PoolRecord>,
    pub holds: Vec<HoldRecord>,
    pub operations: Vec<OperationRecord>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotError {
    BufferTooShort,
    InvalidMagic(u32),
    InvalidVersion(u16),
    InvalidOptionTag(u8),
    InvalidCommandTag(u8),
    InvalidHoldState(u8),
    CountTooLarge,
    InconsistentWatermarks {
        last_applied_lsn: Option<Lsn>,
        last_request_slot: Option<Slot>,
    },
    PoolTableOverCapacity {
        count: usize,
        max: usize,
    },
    HoldTableOverCapacity {
        count: usize,
        max: usize,
    },
    OperationTableOverCapacity {
        count: usize,
        max: usize,
    },
    DuplicatePoolId(PoolId),
    DuplicateHoldId(HoldId),
    DuplicateOperationId(OperationId),
    InvalidResultCode(u8),
    Invariant(ReservationInvariantError),
    Config(ConfigError),
}

impl From<ConfigError> for SnapshotError {
    fn from(error: ConfigError) -> Self {
        Self::Config(error)
    }
}

impl From<ReservationInvariantError> for SnapshotError {
    fn from(error: ReservationInvariantError) -> Self {
        Self::Invariant(error)
    }
}

impl Snapshot {
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let pool_count = u32::try_from(self.pools.len()).expect("pool count must fit u32");
        let hold_count = u32::try_from(self.holds.len()).expect("hold count must fit u32");
        let operation_count =
            u32::try_from(self.operations.len()).expect("operation count must fit u32");
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&MAGIC.to_le_bytes());
        bytes.extend_from_slice(&VERSION.to_le_bytes());
        encode_option_u64(&mut bytes, self.last_applied_lsn.map(Lsn::get));
        encode_option_u64(&mut bytes, self.last_request_slot.map(Slot::get));
        bytes.extend_from_slice(&pool_count.to_le_bytes());
        bytes.extend_from_slice(&hold_count.to_le_bytes());
        bytes.extend_from_slice(&operation_count.to_le_bytes());

        for pool in &self.pools {
            bytes.extend_from_slice(&pool.pool_id.get().to_le_bytes());
            bytes.extend_from_slice(&pool.total_capacity.to_le_bytes());
            bytes.extend_from_slice(&pool.held_capacity.to_le_bytes());
            bytes.extend_from_slice(&pool.consumed_capacity.to_le_bytes());
        }

        for hold in &self.holds {
            bytes.extend_from_slice(&hold.hold_id.get().to_le_bytes());
            bytes.extend_from_slice(&hold.pool_id.get().to_le_bytes());
            bytes.extend_from_slice(&hold.quantity.to_le_bytes());
            bytes.extend_from_slice(&hold.deadline_slot.get().to_le_bytes());
            bytes.push(encode_hold_state(hold.state));
        }

        for operation in &self.operations {
            bytes.extend_from_slice(&operation.operation_id.get().to_le_bytes());
            encode_command(&mut bytes, operation.command);
            bytes.push(encode_result_code(operation.result_code));
            encode_option_u128(&mut bytes, operation.result_pool_id.map(PoolId::get));
            encode_option_u128(&mut bytes, operation.result_hold_id.map(HoldId::get));
            bytes.extend_from_slice(&operation.applied_lsn.get().to_le_bytes());
            bytes.extend_from_slice(&operation.retire_after_slot.get().to_le_bytes());
        }

        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, SnapshotError> {
        let mut cursor = 0usize;
        let magic = decode_u32(bytes, &mut cursor)?;
        if magic != MAGIC {
            return Err(SnapshotError::InvalidMagic(magic));
        }
        let version = decode_u16(bytes, &mut cursor)?;
        if version != VERSION {
            return Err(SnapshotError::InvalidVersion(version));
        }

        let last_applied_lsn = decode_option_u64(bytes, &mut cursor)?.map(Lsn);
        let last_request_slot = decode_option_u64(bytes, &mut cursor)?.map(Slot);
        validate_progress_watermarks(last_applied_lsn, last_request_slot)?;

        let pool_count = usize::try_from(decode_u32(bytes, &mut cursor)?)
            .map_err(|_| SnapshotError::CountTooLarge)?;
        let hold_count = usize::try_from(decode_u32(bytes, &mut cursor)?)
            .map_err(|_| SnapshotError::CountTooLarge)?;
        let operation_count = usize::try_from(decode_u32(bytes, &mut cursor)?)
            .map_err(|_| SnapshotError::CountTooLarge)?;

        let mut pools = Vec::with_capacity(pool_count);
        for _ in 0..pool_count {
            pools.push(PoolRecord {
                pool_id: PoolId(decode_u128(bytes, &mut cursor)?),
                total_capacity: decode_u64(bytes, &mut cursor)?,
                held_capacity: decode_u64(bytes, &mut cursor)?,
                consumed_capacity: decode_u64(bytes, &mut cursor)?,
            });
        }

        let mut holds = Vec::with_capacity(hold_count);
        for _ in 0..hold_count {
            holds.push(HoldRecord {
                hold_id: HoldId(decode_u128(bytes, &mut cursor)?),
                pool_id: PoolId(decode_u128(bytes, &mut cursor)?),
                quantity: decode_u64(bytes, &mut cursor)?,
                deadline_slot: Slot(decode_u64(bytes, &mut cursor)?),
                state: decode_hold_state(decode_u8(bytes, &mut cursor)?)?,
            });
        }

        let mut operations = Vec::with_capacity(operation_count);
        for _ in 0..operation_count {
            operations.push(OperationRecord {
                operation_id: OperationId(decode_u128(bytes, &mut cursor)?),
                command: decode_command(bytes, &mut cursor)?,
                result_code: decode_result_code(decode_u8(bytes, &mut cursor)?)?,
                result_pool_id: decode_option_u128(bytes, &mut cursor)?.map(PoolId),
                result_hold_id: decode_option_u128(bytes, &mut cursor)?.map(HoldId),
                applied_lsn: Lsn(decode_u64(bytes, &mut cursor)?),
                retire_after_slot: Slot(decode_u64(bytes, &mut cursor)?),
            });
        }

        Ok(Self {
            last_applied_lsn,
            last_request_slot,
            pools,
            holds,
            operations,
        })
    }
}

impl ReservationDb {
    pub fn from_snapshot(config: Config, snapshot: Snapshot) -> Result<Self, SnapshotError> {
        let Snapshot {
            last_applied_lsn,
            last_request_slot,
            pools,
            holds,
            operations,
        } = snapshot;

        let mut db = Self::new(config)?;
        validate_progress_watermarks(last_applied_lsn, last_request_slot)?;

        let max_pools =
            usize::try_from(db.config.max_pools).expect("validated max_pools must fit usize");
        let max_holds =
            usize::try_from(db.config.max_holds).expect("validated max_holds must fit usize");
        let max_operations = usize::try_from(db.config.max_operations)
            .expect("validated max_operations must fit usize");

        ensure_snapshot_capacity(pools.len(), max_pools, |count, max| {
            SnapshotError::PoolTableOverCapacity { count, max }
        })?;
        ensure_snapshot_capacity(holds.len(), max_holds, |count, max| {
            SnapshotError::HoldTableOverCapacity { count, max }
        })?;
        ensure_snapshot_capacity(operations.len(), max_operations, |count, max| {
            SnapshotError::OperationTableOverCapacity { count, max }
        })?;

        for record in pools {
            match db.restore_pool(record) {
                Ok(()) => {}
                Err(FixedMapError::DuplicateKey) => {
                    return Err(SnapshotError::DuplicatePoolId(record.pool_id));
                }
                Err(FixedMapError::Full) => {
                    return Err(SnapshotError::PoolTableOverCapacity {
                        count: max_pools.saturating_add(1),
                        max: max_pools,
                    });
                }
            }
        }

        let mut hold_retire_entries = Vec::new();
        for (ordinal, record) in holds.into_iter().enumerate() {
            hold_retire_entries.push((
                record.hold_id,
                record.deadline_slot,
                u64::try_from(ordinal).expect("hold ordinal must fit u64"),
            ));
            match db.restore_hold(record) {
                Ok(()) => {}
                Err(FixedMapError::DuplicateKey) => {
                    return Err(SnapshotError::DuplicateHoldId(record.hold_id));
                }
                Err(FixedMapError::Full) => {
                    return Err(SnapshotError::HoldTableOverCapacity {
                        count: max_holds.saturating_add(1),
                        max: max_holds,
                    });
                }
            }
        }

        let mut operation_retire_entries = Vec::new();
        for record in operations {
            operation_retire_entries.push((
                record.operation_id,
                record.retire_after_slot,
                record.applied_lsn.get(),
            ));
            match db.restore_operation(record) {
                Ok(()) => {}
                Err(FixedMapError::DuplicateKey) => {
                    return Err(SnapshotError::DuplicateOperationId(record.operation_id));
                }
                Err(FixedMapError::Full) => {
                    return Err(SnapshotError::OperationTableOverCapacity {
                        count: max_operations.saturating_add(1),
                        max: max_operations,
                    });
                }
            }
        }

        db.rebuild_hold_retire_queue(&mut hold_retire_entries);
        db.rebuild_operation_retire_queue(&mut operation_retire_entries);
        db.set_progress(last_applied_lsn, last_request_slot);
        db.validate_invariants()?;
        Ok(db)
    }
}

fn ensure_snapshot_capacity<F>(count: usize, max: usize, error: F) -> Result<(), SnapshotError>
where
    F: FnOnce(usize, usize) -> SnapshotError,
{
    if count > max {
        return Err(error(count, max));
    }
    Ok(())
}

fn validate_progress_watermarks(
    last_applied_lsn: Option<Lsn>,
    last_request_slot: Option<Slot>,
) -> Result<(), SnapshotError> {
    if last_applied_lsn.is_some() != last_request_slot.is_some() {
        return Err(SnapshotError::InconsistentWatermarks {
            last_applied_lsn,
            last_request_slot,
        });
    }
    Ok(())
}

fn encode_option_u64(bytes: &mut Vec<u8>, value: Option<u64>) {
    match value {
        Some(value) => {
            bytes.push(1);
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        None => bytes.push(0),
    }
}

fn encode_option_u128(bytes: &mut Vec<u8>, value: Option<u128>) {
    match value {
        Some(value) => {
            bytes.push(1);
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        None => bytes.push(0),
    }
}

fn decode_option_u64(bytes: &[u8], cursor: &mut usize) -> Result<Option<u64>, SnapshotError> {
    match decode_u8(bytes, cursor)? {
        0 => Ok(None),
        1 => Ok(Some(decode_u64(bytes, cursor)?)),
        value => Err(SnapshotError::InvalidOptionTag(value)),
    }
}

fn decode_option_u128(bytes: &[u8], cursor: &mut usize) -> Result<Option<u128>, SnapshotError> {
    match decode_u8(bytes, cursor)? {
        0 => Ok(None),
        1 => Ok(Some(decode_u128(bytes, cursor)?)),
        value => Err(SnapshotError::InvalidOptionTag(value)),
    }
}

fn encode_command(bytes: &mut Vec<u8>, command: Command) {
    match command {
        Command::CreatePool {
            pool_id,
            total_capacity,
        } => {
            bytes.push(1);
            bytes.extend_from_slice(&pool_id.get().to_le_bytes());
            bytes.extend_from_slice(&total_capacity.to_le_bytes());
        }
        Command::PlaceHold {
            pool_id,
            hold_id,
            quantity,
            deadline_slot,
        } => {
            bytes.push(2);
            bytes.extend_from_slice(&pool_id.get().to_le_bytes());
            bytes.extend_from_slice(&hold_id.get().to_le_bytes());
            bytes.extend_from_slice(&quantity.to_le_bytes());
            bytes.extend_from_slice(&deadline_slot.get().to_le_bytes());
        }
        Command::ConfirmHold { hold_id } => {
            bytes.push(3);
            bytes.extend_from_slice(&hold_id.get().to_le_bytes());
        }
        Command::ReleaseHold { hold_id } => {
            bytes.push(4);
            bytes.extend_from_slice(&hold_id.get().to_le_bytes());
        }
        Command::ExpireHold { hold_id } => {
            bytes.push(5);
            bytes.extend_from_slice(&hold_id.get().to_le_bytes());
        }
    }
}

fn decode_command(bytes: &[u8], cursor: &mut usize) -> Result<Command, SnapshotError> {
    match decode_u8(bytes, cursor)? {
        1 => Ok(Command::CreatePool {
            pool_id: PoolId(decode_u128(bytes, cursor)?),
            total_capacity: decode_u64(bytes, cursor)?,
        }),
        2 => Ok(Command::PlaceHold {
            pool_id: PoolId(decode_u128(bytes, cursor)?),
            hold_id: HoldId(decode_u128(bytes, cursor)?),
            quantity: decode_u64(bytes, cursor)?,
            deadline_slot: Slot(decode_u64(bytes, cursor)?),
        }),
        3 => Ok(Command::ConfirmHold {
            hold_id: HoldId(decode_u128(bytes, cursor)?),
        }),
        4 => Ok(Command::ReleaseHold {
            hold_id: HoldId(decode_u128(bytes, cursor)?),
        }),
        5 => Ok(Command::ExpireHold {
            hold_id: HoldId(decode_u128(bytes, cursor)?),
        }),
        value => Err(SnapshotError::InvalidCommandTag(value)),
    }
}

fn encode_result_code(result_code: ResultCode) -> u8 {
    match result_code {
        ResultCode::Ok => 0,
        ResultCode::AlreadyExists => 1,
        ResultCode::PoolTableFull => 2,
        ResultCode::PoolNotFound => 3,
        ResultCode::HoldTableFull => 4,
        ResultCode::HoldNotFound => 5,
        ResultCode::InsufficientCapacity => 6,
        ResultCode::HoldExpired => 7,
        ResultCode::InvalidState => 8,
        ResultCode::OperationConflict => 9,
        ResultCode::OperationTableFull => 10,
        ResultCode::SlotOverflow => 11,
    }
}

fn decode_result_code(value: u8) -> Result<ResultCode, SnapshotError> {
    match value {
        0 => Ok(ResultCode::Ok),
        1 => Ok(ResultCode::AlreadyExists),
        2 => Ok(ResultCode::PoolTableFull),
        3 => Ok(ResultCode::PoolNotFound),
        4 => Ok(ResultCode::HoldTableFull),
        5 => Ok(ResultCode::HoldNotFound),
        6 => Ok(ResultCode::InsufficientCapacity),
        7 => Ok(ResultCode::HoldExpired),
        8 => Ok(ResultCode::InvalidState),
        9 => Ok(ResultCode::OperationConflict),
        10 => Ok(ResultCode::OperationTableFull),
        11 => Ok(ResultCode::SlotOverflow),
        value => Err(SnapshotError::InvalidResultCode(value)),
    }
}

fn encode_hold_state(state: HoldState) -> u8 {
    match state {
        HoldState::Held => 1,
        HoldState::Confirmed => 2,
        HoldState::Released => 3,
        HoldState::Expired => 4,
    }
}

fn decode_hold_state(value: u8) -> Result<HoldState, SnapshotError> {
    match value {
        1 => Ok(HoldState::Held),
        2 => Ok(HoldState::Confirmed),
        3 => Ok(HoldState::Released),
        4 => Ok(HoldState::Expired),
        value => Err(SnapshotError::InvalidHoldState(value)),
    }
}

fn decode_u8(bytes: &[u8], cursor: &mut usize) -> Result<u8, SnapshotError> {
    if *cursor + 1 > bytes.len() {
        return Err(SnapshotError::BufferTooShort);
    }
    let value = bytes[*cursor];
    *cursor += 1;
    Ok(value)
}

fn decode_u16(bytes: &[u8], cursor: &mut usize) -> Result<u16, SnapshotError> {
    if *cursor + 2 > bytes.len() {
        return Err(SnapshotError::BufferTooShort);
    }
    let value = u16::from_le_bytes(bytes[*cursor..*cursor + 2].try_into().expect("slice size"));
    *cursor += 2;
    Ok(value)
}

fn decode_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32, SnapshotError> {
    if *cursor + 4 > bytes.len() {
        return Err(SnapshotError::BufferTooShort);
    }
    let value = u32::from_le_bytes(bytes[*cursor..*cursor + 4].try_into().expect("slice size"));
    *cursor += 4;
    Ok(value)
}

fn decode_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64, SnapshotError> {
    if *cursor + 8 > bytes.len() {
        return Err(SnapshotError::BufferTooShort);
    }
    let value = u64::from_le_bytes(bytes[*cursor..*cursor + 8].try_into().expect("slice size"));
    *cursor += 8;
    Ok(value)
}

fn decode_u128(bytes: &[u8], cursor: &mut usize) -> Result<u128, SnapshotError> {
    if *cursor + 16 > bytes.len() {
        return Err(SnapshotError::BufferTooShort);
    }
    let value = u128::from_le_bytes(bytes[*cursor..*cursor + 16].try_into().expect("slice size"));
    *cursor += 16;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use crate::{
        command::Command,
        config::Config,
        ids::{HoldId, Lsn, OperationId, PoolId, Slot},
        result::ResultCode,
        state_machine::{HoldRecord, HoldState, OperationRecord, PoolRecord, ReservationDb},
    };

    use super::{Snapshot, SnapshotError};

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

    fn seeded_snapshot() -> Snapshot {
        Snapshot {
            last_applied_lsn: Some(Lsn(2)),
            last_request_slot: Some(Slot(2)),
            pools: vec![PoolRecord {
                pool_id: PoolId(11),
                total_capacity: 8,
                held_capacity: 2,
                consumed_capacity: 1,
            }],
            holds: vec![HoldRecord {
                hold_id: HoldId(21),
                pool_id: PoolId(11),
                quantity: 2,
                deadline_slot: Slot(5),
                state: HoldState::Held,
            }],
            operations: vec![OperationRecord {
                operation_id: OperationId(1),
                command: Command::PlaceHold {
                    pool_id: PoolId(11),
                    hold_id: HoldId(21),
                    quantity: 2,
                    deadline_slot: Slot(5),
                },
                result_code: ResultCode::Ok,
                result_pool_id: Some(PoolId(11)),
                result_hold_id: Some(HoldId(21)),
                applied_lsn: Lsn(2),
                retire_after_slot: Slot(9),
            }],
        }
    }

    #[test]
    fn snapshot_round_trips() {
        let encoded = seeded_snapshot().encode();
        let decoded = Snapshot::decode(&encoded).unwrap();
        assert_eq!(decoded, seeded_snapshot());
    }

    #[test]
    fn reservation_db_restores_from_snapshot() {
        let db = ReservationDb::from_snapshot(config(), seeded_snapshot()).unwrap();

        assert_eq!(db.snapshot(), seeded_snapshot());
    }

    #[test]
    fn snapshot_rejects_invalid_option_tag() {
        let mut encoded = seeded_snapshot().encode();
        encoded[6] = 9;

        assert_eq!(
            Snapshot::decode(&encoded),
            Err(SnapshotError::InvalidOptionTag(9))
        );
    }
}

use crate::config::{Config, ConfigError};
use crate::fixed_map::FixedMapError;
use crate::ids::{BucketId, Lsn, OperationId, Slot};
use crate::result::ResultCode;
use crate::state_machine::{BucketRecord, OperationRecord, QuotaDb, QuotaInvariantError};

const MAGIC: u32 = 0x5154_4253;
const VERSION: u16 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Snapshot {
    pub last_applied_lsn: Option<Lsn>,
    pub last_request_slot: Option<Slot>,
    pub buckets: Vec<BucketRecord>,
    pub operations: Vec<OperationRecord>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotError {
    BufferTooShort,
    InvalidMagic(u32),
    InvalidVersion(u16),
    CountTooLarge,
    InconsistentWatermarks {
        last_applied_lsn: Option<Lsn>,
        last_request_slot: Option<Slot>,
    },
    BucketTableOverCapacity {
        count: usize,
        max: usize,
    },
    OperationTableOverCapacity {
        count: usize,
        max: usize,
    },
    DuplicateBucketId(BucketId),
    DuplicateOperationId(OperationId),
    InvalidResultCode(u8),
    Invariant(QuotaInvariantError),
    Config(ConfigError),
}

impl From<ConfigError> for SnapshotError {
    fn from(error: ConfigError) -> Self {
        Self::Config(error)
    }
}

impl From<QuotaInvariantError> for SnapshotError {
    fn from(error: QuotaInvariantError) -> Self {
        Self::Invariant(error)
    }
}

impl Snapshot {
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let bucket_count =
            u32::try_from(self.buckets.len()).expect("bucket count must fit u32 for snapshot");
        let operation_count = u32::try_from(self.operations.len())
            .expect("operation count must fit u32 for snapshot");
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&MAGIC.to_le_bytes());
        bytes.extend_from_slice(&VERSION.to_le_bytes());
        encode_option_u64(&mut bytes, self.last_applied_lsn.map(Lsn::get));
        encode_option_u64(&mut bytes, self.last_request_slot.map(Slot::get));
        bytes.extend_from_slice(&bucket_count.to_le_bytes());
        bytes.extend_from_slice(&operation_count.to_le_bytes());

        for bucket in &self.buckets {
            bytes.extend_from_slice(&bucket.bucket_id.get().to_le_bytes());
            bytes.extend_from_slice(&bucket.limit.to_le_bytes());
            bytes.extend_from_slice(&bucket.balance.to_le_bytes());
            bytes.extend_from_slice(&bucket.last_refill_slot.get().to_le_bytes());
            bytes.extend_from_slice(&bucket.refill_rate_per_slot.to_le_bytes());
        }

        for operation in &self.operations {
            bytes.extend_from_slice(&operation.operation_id.get().to_le_bytes());
            bytes.extend_from_slice(&operation.command_fingerprint.to_le_bytes());
            bytes.push(encode_result_code(operation.result_code));
            encode_option_u128(&mut bytes, operation.result_bucket_id.map(BucketId::get));
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

        let bucket_count = usize::try_from(decode_u32(bytes, &mut cursor)?)
            .map_err(|_| SnapshotError::CountTooLarge)?;
        let operation_count = usize::try_from(decode_u32(bytes, &mut cursor)?)
            .map_err(|_| SnapshotError::CountTooLarge)?;

        let mut buckets = Vec::with_capacity(bucket_count);
        for _ in 0..bucket_count {
            buckets.push(BucketRecord {
                bucket_id: BucketId(decode_u128(bytes, &mut cursor)?),
                limit: decode_u64(bytes, &mut cursor)?,
                balance: decode_u64(bytes, &mut cursor)?,
                last_refill_slot: Slot(decode_u64(bytes, &mut cursor)?),
                refill_rate_per_slot: decode_u64(bytes, &mut cursor)?,
            });
        }

        let mut operations = Vec::with_capacity(operation_count);
        for _ in 0..operation_count {
            operations.push(OperationRecord {
                operation_id: OperationId(decode_u128(bytes, &mut cursor)?),
                command_fingerprint: decode_u128(bytes, &mut cursor)?,
                result_code: decode_result_code(decode_u8(bytes, &mut cursor)?)?,
                result_bucket_id: decode_option_u128(bytes, &mut cursor)?.map(BucketId),
                applied_lsn: Lsn(decode_u64(bytes, &mut cursor)?),
                retire_after_slot: Slot(decode_u64(bytes, &mut cursor)?),
            });
        }

        Ok(Self {
            last_applied_lsn,
            last_request_slot,
            buckets,
            operations,
        })
    }
}

impl QuotaDb {
    pub fn from_snapshot(config: Config, snapshot: Snapshot) -> Result<Self, SnapshotError> {
        let Snapshot {
            last_applied_lsn,
            last_request_slot,
            buckets,
            operations,
        } = snapshot;

        let mut db = Self::new(config)?;
        validate_progress_watermarks(last_applied_lsn, last_request_slot)?;

        let max_buckets =
            usize::try_from(db.config.max_buckets).expect("validated max_buckets must fit usize");
        let max_operations = usize::try_from(db.config.max_operations)
            .expect("validated max_operations must fit usize");

        ensure_snapshot_capacity(buckets.len(), max_buckets, |count, max| {
            SnapshotError::BucketTableOverCapacity { count, max }
        })?;
        ensure_snapshot_capacity(operations.len(), max_operations, |count, max| {
            SnapshotError::OperationTableOverCapacity { count, max }
        })?;

        for record in buckets {
            match db.restore_bucket(record) {
                Ok(()) => {}
                Err(FixedMapError::DuplicateKey) => {
                    return Err(SnapshotError::DuplicateBucketId(record.bucket_id));
                }
                Err(FixedMapError::Full) => {
                    return Err(SnapshotError::BucketTableOverCapacity {
                        count: max_buckets.saturating_add(1),
                        max: max_buckets,
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
        _ => Err(SnapshotError::BufferTooShort),
    }
}

fn decode_option_u128(bytes: &[u8], cursor: &mut usize) -> Result<Option<u128>, SnapshotError> {
    match decode_u8(bytes, cursor)? {
        0 => Ok(None),
        1 => Ok(Some(decode_u128(bytes, cursor)?)),
        _ => Err(SnapshotError::BufferTooShort),
    }
}

fn encode_result_code(result_code: ResultCode) -> u8 {
    match result_code {
        ResultCode::Ok => 1,
        ResultCode::AlreadyExists => 2,
        ResultCode::BucketTableFull => 3,
        ResultCode::BucketNotFound => 4,
        ResultCode::InsufficientFunds => 5,
        ResultCode::OperationConflict => 6,
        ResultCode::OperationTableFull => 7,
        ResultCode::SlotOverflow => 8,
    }
}

fn decode_result_code(value: u8) -> Result<ResultCode, SnapshotError> {
    match value {
        1 => Ok(ResultCode::Ok),
        2 => Ok(ResultCode::AlreadyExists),
        3 => Ok(ResultCode::BucketTableFull),
        4 => Ok(ResultCode::BucketNotFound),
        5 => Ok(ResultCode::InsufficientFunds),
        6 => Ok(ResultCode::OperationConflict),
        7 => Ok(ResultCode::OperationTableFull),
        8 => Ok(ResultCode::SlotOverflow),
        _ => Err(SnapshotError::InvalidResultCode(value)),
    }
}

fn decode_u8(bytes: &[u8], cursor: &mut usize) -> Result<u8, SnapshotError> {
    let end = cursor.saturating_add(1);
    let slice = bytes
        .get(*cursor..end)
        .ok_or(SnapshotError::BufferTooShort)?;
    *cursor = end;
    Ok(slice[0])
}

fn decode_u16(bytes: &[u8], cursor: &mut usize) -> Result<u16, SnapshotError> {
    let end = cursor.saturating_add(2);
    let slice = bytes
        .get(*cursor..end)
        .ok_or(SnapshotError::BufferTooShort)?;
    *cursor = end;
    Ok(u16::from_le_bytes(
        slice.try_into().expect("slice has exact size"),
    ))
}

fn decode_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32, SnapshotError> {
    let end = cursor.saturating_add(4);
    let slice = bytes
        .get(*cursor..end)
        .ok_or(SnapshotError::BufferTooShort)?;
    *cursor = end;
    Ok(u32::from_le_bytes(
        slice.try_into().expect("slice has exact size"),
    ))
}

fn decode_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64, SnapshotError> {
    let end = cursor.saturating_add(8);
    let slice = bytes
        .get(*cursor..end)
        .ok_or(SnapshotError::BufferTooShort)?;
    *cursor = end;
    Ok(u64::from_le_bytes(
        slice.try_into().expect("slice has exact size"),
    ))
}

fn decode_u128(bytes: &[u8], cursor: &mut usize) -> Result<u128, SnapshotError> {
    let end = cursor.saturating_add(16);
    let slice = bytes
        .get(*cursor..end)
        .ok_or(SnapshotError::BufferTooShort)?;
    *cursor = end;
    Ok(u128::from_le_bytes(
        slice.try_into().expect("slice has exact size"),
    ))
}

#[cfg(test)]
mod tests {
    use crate::{
        Config,
        ids::{BucketId, Lsn, OperationId, Slot},
        result::ResultCode,
        state_machine::{BucketRecord, OperationRecord, QuotaDb},
    };

    use super::{Snapshot, SnapshotError};

    fn config() -> Config {
        Config {
            max_buckets: 8,
            max_operations: 16,
            max_batch_len: 8,
            max_client_retry_window_slots: 8,
            max_wal_payload_bytes: 1024,
        }
    }

    fn seeded_snapshot() -> Snapshot {
        Snapshot {
            last_applied_lsn: Some(Lsn(3)),
            last_request_slot: Some(Slot(3)),
            buckets: vec![BucketRecord {
                bucket_id: BucketId(11),
                limit: 9,
                balance: 7,
                last_refill_slot: Slot(3),
                refill_rate_per_slot: 2,
            }],
            operations: vec![OperationRecord {
                operation_id: OperationId(1),
                command_fingerprint: 99,
                result_code: ResultCode::Ok,
                result_bucket_id: Some(BucketId(11)),
                applied_lsn: Lsn(3),
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
    fn quota_db_restores_from_snapshot() {
        let db = QuotaDb::from_snapshot(config(), seeded_snapshot()).unwrap();

        assert_eq!(db.last_applied_lsn(), Some(Lsn(3)));
        assert_eq!(db.last_request_slot(), Some(Slot(3)));
        assert_eq!(db.snapshot(), seeded_snapshot());
    }

    #[test]
    fn snapshot_rejects_mismatched_progress_watermarks() {
        let mut snapshot = seeded_snapshot();
        snapshot.last_request_slot = None;

        let error = QuotaDb::from_snapshot(config(), snapshot).unwrap_err();
        assert!(matches!(
            error,
            SnapshotError::InconsistentWatermarks { .. }
        ));
    }
}

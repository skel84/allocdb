use crate::config::{Config, ConfigError};
use crate::ids::{Lsn, Slot};
use crate::state_machine::{
    AllocDb, OperationRecord, ReservationRecord, ReservationState, ResourceRecord, ResourceState,
};

const MAGIC: u32 = 0x4144_4253;
const VERSION: u16 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Snapshot {
    pub last_applied_lsn: Option<Lsn>,
    pub last_request_slot: Option<Slot>,
    pub resources: Vec<ResourceRecord>,
    pub reservations: Vec<ReservationRecord>,
    pub operations: Vec<OperationRecord>,
    pub wheel: Vec<Vec<crate::ids::ReservationId>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotError {
    BufferTooShort,
    InvalidMagic(u32),
    InvalidVersion(u16),
    InvalidStateTag(u8),
    CountTooLarge,
    InvalidLayout,
    Config(ConfigError),
}

impl From<ConfigError> for SnapshotError {
    fn from(error: ConfigError) -> Self {
        Self::Config(error)
    }
}

impl Snapshot {
    /// Encodes one snapshot using explicit little-endian fields.
    ///
    /// # Panics
    ///
    /// Panics if counts exceed `u32`.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&MAGIC.to_le_bytes());
        bytes.extend_from_slice(&VERSION.to_le_bytes());
        encode_optional_u64(&mut bytes, self.last_applied_lsn.map(Lsn::get));
        encode_optional_u64(&mut bytes, self.last_request_slot.map(Slot::get));
        encode_u32(
            &mut bytes,
            u32::try_from(self.resources.len()).expect("resource count must fit u32"),
        );
        encode_u32(
            &mut bytes,
            u32::try_from(self.reservations.len()).expect("reservation count must fit u32"),
        );
        encode_u32(
            &mut bytes,
            u32::try_from(self.operations.len()).expect("operation count must fit u32"),
        );
        encode_u32(
            &mut bytes,
            u32::try_from(self.wheel.len()).expect("wheel len must fit u32"),
        );

        for resource in &self.resources {
            bytes.extend_from_slice(&resource.resource_id.get().to_le_bytes());
            bytes.push(encode_resource_state(resource.current_state));
            encode_optional_u128(
                &mut bytes,
                resource
                    .current_reservation_id
                    .map(crate::ids::ReservationId::get),
            );
            bytes.extend_from_slice(&resource.version.to_le_bytes());
        }

        for reservation in &self.reservations {
            bytes.extend_from_slice(&reservation.reservation_id.get().to_le_bytes());
            bytes.extend_from_slice(&reservation.resource_id.get().to_le_bytes());
            bytes.extend_from_slice(&reservation.holder_id.get().to_le_bytes());
            bytes.push(encode_reservation_state(reservation.state));
            bytes.extend_from_slice(&reservation.created_lsn.get().to_le_bytes());
            bytes.extend_from_slice(&reservation.deadline_slot.get().to_le_bytes());
            encode_optional_u64(&mut bytes, reservation.released_lsn.map(Lsn::get));
            encode_optional_u64(&mut bytes, reservation.retire_after_slot.map(Slot::get));
        }

        for operation in &self.operations {
            bytes.extend_from_slice(&operation.operation_id.get().to_le_bytes());
            bytes.extend_from_slice(&operation.command_fingerprint.to_le_bytes());
            bytes.push(encode_result_code(operation.result_code));
            encode_optional_u128(
                &mut bytes,
                operation
                    .result_reservation_id
                    .map(crate::ids::ReservationId::get),
            );
            encode_optional_u64(&mut bytes, operation.result_deadline_slot.map(Slot::get));
            bytes.extend_from_slice(&operation.applied_lsn.get().to_le_bytes());
            bytes.extend_from_slice(&operation.retire_after_slot.get().to_le_bytes());
        }

        for bucket in &self.wheel {
            encode_u32(
                &mut bytes,
                u32::try_from(bucket.len()).expect("bucket len must fit u32"),
            );
            for reservation_id in bucket {
                bytes.extend_from_slice(&reservation_id.get().to_le_bytes());
            }
        }

        bytes
    }

    /// Decodes one snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError`] when the snapshot is incomplete or structurally invalid.
    pub fn decode(bytes: &[u8]) -> Result<Self, SnapshotError> {
        let mut cursor = Cursor::new(bytes);

        let magic = cursor.read_u32()?;
        if magic != MAGIC {
            return Err(SnapshotError::InvalidMagic(magic));
        }

        let version = cursor.read_u16()?;
        if version != VERSION {
            return Err(SnapshotError::InvalidVersion(version));
        }

        let last_applied_lsn = cursor.read_optional_u64()?.map(Lsn);
        let last_request_slot = cursor.read_optional_u64()?.map(Slot);
        let resource_count =
            usize::try_from(cursor.read_u32()?).map_err(|_| SnapshotError::CountTooLarge)?;
        let reservation_count =
            usize::try_from(cursor.read_u32()?).map_err(|_| SnapshotError::CountTooLarge)?;
        let operation_count =
            usize::try_from(cursor.read_u32()?).map_err(|_| SnapshotError::CountTooLarge)?;
        let wheel_len =
            usize::try_from(cursor.read_u32()?).map_err(|_| SnapshotError::CountTooLarge)?;

        let mut resources = Vec::with_capacity(resource_count);
        for _ in 0..resource_count {
            resources.push(ResourceRecord {
                resource_id: crate::ids::ResourceId(cursor.read_u128()?),
                current_state: decode_resource_state(cursor.read_u8()?)?,
                current_reservation_id: cursor.read_optional_u128()?.map(crate::ids::ReservationId),
                version: cursor.read_u64()?,
            });
        }

        let mut reservations = Vec::with_capacity(reservation_count);
        for _ in 0..reservation_count {
            reservations.push(ReservationRecord {
                reservation_id: crate::ids::ReservationId(cursor.read_u128()?),
                resource_id: crate::ids::ResourceId(cursor.read_u128()?),
                holder_id: crate::ids::HolderId(cursor.read_u128()?),
                state: decode_reservation_state(cursor.read_u8()?)?,
                created_lsn: Lsn(cursor.read_u64()?),
                deadline_slot: Slot(cursor.read_u64()?),
                released_lsn: cursor.read_optional_u64()?.map(Lsn),
                retire_after_slot: cursor.read_optional_u64()?.map(Slot),
            });
        }

        let mut operations = Vec::with_capacity(operation_count);
        for _ in 0..operation_count {
            operations.push(OperationRecord {
                operation_id: crate::ids::OperationId(cursor.read_u128()?),
                command_fingerprint: cursor.read_u128()?,
                result_code: decode_result_code(cursor.read_u8()?)?,
                result_reservation_id: cursor.read_optional_u128()?.map(crate::ids::ReservationId),
                result_deadline_slot: cursor.read_optional_u64()?.map(Slot),
                applied_lsn: Lsn(cursor.read_u64()?),
                retire_after_slot: Slot(cursor.read_u64()?),
            });
        }

        let mut wheel = Vec::with_capacity(wheel_len);
        for _ in 0..wheel_len {
            let bucket_len =
                usize::try_from(cursor.read_u32()?).map_err(|_| SnapshotError::CountTooLarge)?;
            let mut bucket = Vec::with_capacity(bucket_len);
            for _ in 0..bucket_len {
                bucket.push(crate::ids::ReservationId(cursor.read_u128()?));
            }
            wheel.push(bucket);
        }

        if !cursor.is_at_end() {
            return Err(SnapshotError::InvalidLayout);
        }

        Ok(Self {
            last_applied_lsn,
            last_request_slot,
            resources,
            reservations,
            operations,
            wheel,
        })
    }
}

impl AllocDb {
    /// Captures a snapshot of the current trusted-core state.
    #[must_use]
    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            last_applied_lsn: self.last_applied_lsn,
            last_request_slot: self.last_request_slot,
            resources: self.resources.clone(),
            reservations: self.reservations.clone(),
            operations: self.operations.clone(),
            wheel: self.wheel.clone(),
        }
    }

    /// Restores an allocator from one decoded snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError`] when configuration validation fails or when snapshot layout does
    /// not match the configured timing wheel.
    pub fn from_snapshot(config: Config, snapshot: Snapshot) -> Result<Self, SnapshotError> {
        config.validate()?;
        if snapshot.wheel.len() != config.wheel_len() {
            return Err(SnapshotError::InvalidLayout);
        }

        let db = Self {
            config,
            resources: snapshot.resources,
            reservations: snapshot.reservations,
            operations: snapshot.operations,
            wheel: snapshot.wheel,
            last_applied_lsn: snapshot.last_applied_lsn,
            last_request_slot: snapshot.last_request_slot,
        };
        db.assert_invariants();
        Ok(db)
    }
}

fn encode_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn encode_optional_u64(bytes: &mut Vec<u8>, value: Option<u64>) {
    match value {
        Some(value) => {
            bytes.push(1);
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        None => bytes.push(0),
    }
}

fn encode_optional_u128(bytes: &mut Vec<u8>, value: Option<u128>) {
    match value {
        Some(value) => {
            bytes.push(1);
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        None => bytes.push(0),
    }
}

fn encode_resource_state(state: ResourceState) -> u8 {
    match state {
        ResourceState::Available => 1,
        ResourceState::Reserved => 2,
        ResourceState::Confirmed => 3,
    }
}

fn decode_resource_state(tag: u8) -> Result<ResourceState, SnapshotError> {
    match tag {
        1 => Ok(ResourceState::Available),
        2 => Ok(ResourceState::Reserved),
        3 => Ok(ResourceState::Confirmed),
        _ => Err(SnapshotError::InvalidStateTag(tag)),
    }
}

fn encode_reservation_state(state: ReservationState) -> u8 {
    match state {
        ReservationState::Reserved => 1,
        ReservationState::Confirmed => 2,
        ReservationState::Released => 3,
        ReservationState::Expired => 4,
    }
}

fn decode_reservation_state(tag: u8) -> Result<ReservationState, SnapshotError> {
    match tag {
        1 => Ok(ReservationState::Reserved),
        2 => Ok(ReservationState::Confirmed),
        3 => Ok(ReservationState::Released),
        4 => Ok(ReservationState::Expired),
        _ => Err(SnapshotError::InvalidStateTag(tag)),
    }
}

fn decode_result_code(tag: u8) -> Result<crate::result::ResultCode, SnapshotError> {
    use crate::result::ResultCode;

    match tag {
        0 => Ok(ResultCode::Ok),
        1 => Ok(ResultCode::Noop),
        2 => Ok(ResultCode::AlreadyExists),
        3 => Ok(ResultCode::ResourceTableFull),
        4 => Ok(ResultCode::ResourceNotFound),
        5 => Ok(ResultCode::ResourceBusy),
        6 => Ok(ResultCode::TtlOutOfRange),
        7 => Ok(ResultCode::ReservationTableFull),
        8 => Ok(ResultCode::ReservationNotFound),
        9 => Ok(ResultCode::ReservationRetired),
        10 => Ok(ResultCode::ExpirationIndexFull),
        11 => Ok(ResultCode::OperationTableFull),
        12 => Ok(ResultCode::OperationConflict),
        13 => Ok(ResultCode::InvalidState),
        14 => Ok(ResultCode::HolderMismatch),
        _ => Err(SnapshotError::InvalidStateTag(tag)),
    }
}

fn encode_result_code(code: crate::result::ResultCode) -> u8 {
    use crate::result::ResultCode;

    match code {
        ResultCode::Ok => 0,
        ResultCode::Noop => 1,
        ResultCode::AlreadyExists => 2,
        ResultCode::ResourceTableFull => 3,
        ResultCode::ResourceNotFound => 4,
        ResultCode::ResourceBusy => 5,
        ResultCode::TtlOutOfRange => 6,
        ResultCode::ReservationTableFull => 7,
        ResultCode::ReservationNotFound => 8,
        ResultCode::ReservationRetired => 9,
        ResultCode::ExpirationIndexFull => 10,
        ResultCode::OperationTableFull => 11,
        ResultCode::OperationConflict => 12,
        ResultCode::InvalidState => 13,
        ResultCode::HolderMismatch => 14,
    }
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn is_at_end(&self) -> bool {
        self.offset == self.bytes.len()
    }

    fn read_exact<const N: usize>(&mut self) -> Result<[u8; N], SnapshotError> {
        if self.offset + N > self.bytes.len() {
            return Err(SnapshotError::BufferTooShort);
        }

        let mut array = [0_u8; N];
        array.copy_from_slice(&self.bytes[self.offset..self.offset + N]);
        self.offset += N;
        Ok(array)
    }

    fn read_u8(&mut self) -> Result<u8, SnapshotError> {
        Ok(self.read_exact::<1>()?[0])
    }

    fn read_u16(&mut self) -> Result<u16, SnapshotError> {
        Ok(u16::from_le_bytes(self.read_exact::<2>()?))
    }

    fn read_u32(&mut self) -> Result<u32, SnapshotError> {
        Ok(u32::from_le_bytes(self.read_exact::<4>()?))
    }

    fn read_u64(&mut self) -> Result<u64, SnapshotError> {
        Ok(u64::from_le_bytes(self.read_exact::<8>()?))
    }

    fn read_u128(&mut self) -> Result<u128, SnapshotError> {
        Ok(u128::from_le_bytes(self.read_exact::<16>()?))
    }

    fn read_optional_u64(&mut self) -> Result<Option<u64>, SnapshotError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_u64()?)),
            _ => Err(SnapshotError::InvalidLayout),
        }
    }

    fn read_optional_u128(&mut self) -> Result<Option<u128>, SnapshotError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_u128()?)),
            _ => Err(SnapshotError::InvalidLayout),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::command::{ClientRequest, Command, CommandContext};
    use crate::ids::{ClientId, HolderId, Lsn, OperationId, ResourceId, Slot};

    use super::{AllocDb, Snapshot, SnapshotError};
    use crate::config::Config;

    fn config() -> Config {
        Config {
            shard_id: 0,
            max_resources: 8,
            max_reservations: 8,
            max_operations: 16,
            max_ttl_slots: 16,
            max_client_retry_window_slots: 8,
            reservation_history_window_slots: 4,
            max_expiration_bucket_len: 8,
        }
    }

    fn context(lsn: u64, request_slot: u64) -> CommandContext {
        CommandContext {
            lsn: Lsn(lsn),
            request_slot: Slot(request_slot),
        }
    }

    #[test]
    fn snapshot_round_trips_allocator_state() {
        let mut db = AllocDb::new(config()).unwrap();
        db.apply_client(
            context(1, 1),
            ClientRequest {
                operation_id: OperationId(1),
                client_id: ClientId(7),
                command: Command::CreateResource {
                    resource_id: ResourceId(11),
                },
            },
        );
        db.apply_client(
            context(2, 2),
            ClientRequest {
                operation_id: OperationId(2),
                client_id: ClientId(7),
                command: Command::Reserve {
                    resource_id: ResourceId(11),
                    holder_id: HolderId(5),
                    ttl_slots: 3,
                },
            },
        );

        let snapshot = db.snapshot();
        let encoded = snapshot.encode();
        let decoded = Snapshot::decode(&encoded).unwrap();
        let restored = AllocDb::from_snapshot(config(), decoded).unwrap();

        assert_eq!(restored.snapshot(), snapshot);
    }

    #[test]
    fn snapshot_decode_rejects_corruption() {
        let mut bytes = Snapshot {
            last_applied_lsn: None,
            last_request_slot: None,
            resources: Vec::new(),
            reservations: Vec::new(),
            operations: Vec::new(),
            wheel: vec![Vec::new(); config().wheel_len()],
        }
        .encode();
        bytes[0] = 0;

        assert!(matches!(
            Snapshot::decode(&bytes),
            Err(SnapshotError::InvalidMagic(_))
        ));
    }

    #[test]
    fn from_snapshot_rejects_wheel_size_mismatch() {
        let snapshot = Snapshot {
            last_applied_lsn: None,
            last_request_slot: None,
            resources: Vec::new(),
            reservations: Vec::new(),
            operations: Vec::new(),
            wheel: vec![Vec::new(); 1],
        };

        let restored = AllocDb::from_snapshot(config(), snapshot);
        assert!(matches!(restored, Err(SnapshotError::InvalidLayout)));
    }
}

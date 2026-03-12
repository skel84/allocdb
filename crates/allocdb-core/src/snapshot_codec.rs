use crate::ids::{HolderId, Lsn, OperationId, ReservationId, ResourceId, Slot};
use crate::result::ResultCode;
use crate::state_machine::{
    OperationRecord, ReservationRecord, ReservationState, ResourceRecord, ResourceState,
};

use crate::snapshot::cursor::Cursor;
use crate::snapshot::{MAGIC, Snapshot, SnapshotError, VERSION};

pub(super) fn encode_snapshot(snapshot: &Snapshot) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&MAGIC.to_le_bytes());
    bytes.extend_from_slice(&VERSION.to_le_bytes());
    encode_optional_u64(&mut bytes, snapshot.last_applied_lsn.map(Lsn::get));
    encode_optional_u64(&mut bytes, snapshot.last_request_slot.map(Slot::get));
    encode_optional_u128(
        &mut bytes,
        snapshot.max_retired_reservation_id.map(ReservationId::get),
    );
    encode_u32(
        &mut bytes,
        u32::try_from(snapshot.resources.len()).expect("resource count must fit u32"),
    );
    encode_u32(
        &mut bytes,
        u32::try_from(snapshot.reservations.len()).expect("reservation count must fit u32"),
    );
    encode_u32(
        &mut bytes,
        u32::try_from(snapshot.operations.len()).expect("operation count must fit u32"),
    );
    encode_u32(
        &mut bytes,
        u32::try_from(snapshot.wheel.len()).expect("wheel len must fit u32"),
    );

    for resource in &snapshot.resources {
        bytes.extend_from_slice(&resource.resource_id.get().to_le_bytes());
        bytes.push(encode_resource_state(resource.current_state));
        encode_optional_u128(
            &mut bytes,
            resource.current_reservation_id.map(ReservationId::get),
        );
        bytes.extend_from_slice(&resource.version.to_le_bytes());
    }

    for reservation in &snapshot.reservations {
        bytes.extend_from_slice(&reservation.reservation_id.get().to_le_bytes());
        bytes.extend_from_slice(&reservation.resource_id.get().to_le_bytes());
        bytes.extend_from_slice(&reservation.holder_id.get().to_le_bytes());
        bytes.push(encode_reservation_state(reservation.state));
        bytes.extend_from_slice(&reservation.created_lsn.get().to_le_bytes());
        bytes.extend_from_slice(&reservation.deadline_slot.get().to_le_bytes());
        encode_optional_u64(&mut bytes, reservation.released_lsn.map(Lsn::get));
        encode_optional_u64(&mut bytes, reservation.retire_after_slot.map(Slot::get));
    }

    for operation in &snapshot.operations {
        bytes.extend_from_slice(&operation.operation_id.get().to_le_bytes());
        bytes.extend_from_slice(&operation.command_fingerprint.to_le_bytes());
        bytes.push(encode_result_code(operation.result_code));
        encode_optional_u128(
            &mut bytes,
            operation.result_reservation_id.map(ReservationId::get),
        );
        encode_optional_u64(&mut bytes, operation.result_deadline_slot.map(Slot::get));
        bytes.extend_from_slice(&operation.applied_lsn.get().to_le_bytes());
        bytes.extend_from_slice(&operation.retire_after_slot.get().to_le_bytes());
    }

    for bucket in &snapshot.wheel {
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

pub(super) fn decode_snapshot(bytes: &[u8]) -> Result<Snapshot, SnapshotError> {
    let mut cursor = Cursor::new(bytes);

    let magic = cursor.read_u32()?;
    if magic != MAGIC {
        return Err(SnapshotError::InvalidMagic(magic));
    }

    let version = cursor.read_u16()?;
    if version != 1 && version != VERSION {
        return Err(SnapshotError::InvalidVersion(version));
    }

    let last_applied_lsn = cursor.read_optional_u64()?.map(Lsn);
    let last_request_slot = cursor.read_optional_u64()?.map(Slot);
    let max_retired_reservation_id = if version == 1 {
        None
    } else {
        cursor.read_optional_u128()?.map(ReservationId)
    };
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
            resource_id: ResourceId(cursor.read_u128()?),
            current_state: decode_resource_state(cursor.read_u8()?)?,
            current_reservation_id: cursor.read_optional_u128()?.map(ReservationId),
            version: cursor.read_u64()?,
        });
    }

    let mut reservations = Vec::with_capacity(reservation_count);
    for _ in 0..reservation_count {
        reservations.push(ReservationRecord {
            reservation_id: ReservationId(cursor.read_u128()?),
            resource_id: ResourceId(cursor.read_u128()?),
            holder_id: HolderId(cursor.read_u128()?),
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
            operation_id: OperationId(cursor.read_u128()?),
            command_fingerprint: cursor.read_u128()?,
            result_code: decode_result_code(cursor.read_u8()?)?,
            result_reservation_id: cursor.read_optional_u128()?.map(ReservationId),
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
            bucket.push(ReservationId(cursor.read_u128()?));
        }
        wheel.push(bucket);
    }

    if !cursor.is_at_end() {
        return Err(SnapshotError::InvalidLayout);
    }

    Ok(Snapshot {
        last_applied_lsn,
        last_request_slot,
        max_retired_reservation_id,
        resources,
        reservations,
        operations,
        wheel,
    })
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

fn decode_result_code(tag: u8) -> Result<ResultCode, SnapshotError> {
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

fn encode_result_code(code: ResultCode) -> u8 {
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

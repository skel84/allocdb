use crate::ids::{HolderId, Lsn, OperationId, ReservationId, ResourceId, Slot};
use crate::result::ResultCode;
use crate::state_machine::{
    OperationRecord, ReservationMemberRecord, ReservationRecord, ReservationState, ResourceRecord,
    ResourceState,
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
        u32::try_from(snapshot.reservation_members.len())
            .expect("reservation member count must fit u32"),
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
        bytes.extend_from_slice(&reservation.lease_epoch.to_le_bytes());
        bytes.push(encode_reservation_state(reservation.state));
        bytes.extend_from_slice(&reservation.created_lsn.get().to_le_bytes());
        bytes.extend_from_slice(&reservation.deadline_slot.get().to_le_bytes());
        encode_optional_u64(&mut bytes, reservation.released_lsn.map(Lsn::get));
        encode_optional_u64(&mut bytes, reservation.retire_after_slot.map(Slot::get));
        bytes.extend_from_slice(&reservation.member_count.to_le_bytes());
    }

    for reservation_member in &snapshot.reservation_members {
        bytes.extend_from_slice(&reservation_member.reservation_id.get().to_le_bytes());
        bytes.extend_from_slice(&reservation_member.resource_id.get().to_le_bytes());
        bytes.extend_from_slice(&reservation_member.member_index.to_le_bytes());
    }

    for operation in &snapshot.operations {
        bytes.extend_from_slice(&operation.operation_id.get().to_le_bytes());
        bytes.extend_from_slice(&operation.command_fingerprint.to_le_bytes());
        bytes.push(encode_result_code(operation.result_code));
        encode_optional_u128(
            &mut bytes,
            operation.result_reservation_id.map(ReservationId::get),
        );
        encode_optional_u64(&mut bytes, operation.result_lease_epoch);
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
    if version != 1 && version != 2 && version != 3 && version != VERSION {
        return Err(SnapshotError::InvalidVersion(version));
    }

    let (
        last_applied_lsn,
        last_request_slot,
        max_retired_reservation_id,
        resource_count,
        reservation_count,
        reservation_member_count,
        operation_count,
        wheel_len,
    ) = decode_snapshot_header(&mut cursor, version)?;

    let mut resources = Vec::with_capacity(resource_count);
    for _ in 0..resource_count {
        resources.push(ResourceRecord {
            resource_id: ResourceId(cursor.read_u128()?),
            current_state: decode_resource_state(cursor.read_u8()?)?,
            current_reservation_id: cursor.read_optional_u128()?.map(ReservationId),
            version: cursor.read_u64()?,
        });
    }

    let reservations = decode_snapshot_reservations(&mut cursor, reservation_count, version)?;
    let reservation_members = decode_snapshot_reservation_members(
        &mut cursor,
        version,
        reservation_member_count,
        &reservations,
    )?;

    let mut operations = Vec::with_capacity(operation_count);
    for _ in 0..operation_count {
        operations.push(OperationRecord {
            operation_id: OperationId(cursor.read_u128()?),
            command_fingerprint: cursor.read_u128()?,
            result_code: decode_result_code(cursor.read_u8()?)?,
            result_reservation_id: cursor.read_optional_u128()?.map(ReservationId),
            result_lease_epoch: if version >= 4 {
                cursor.read_optional_u64()?
            } else {
                None
            },
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
        reservation_members,
        operations,
        wheel,
    })
}

type SnapshotHeader = (
    Option<Lsn>,
    Option<Slot>,
    Option<ReservationId>,
    usize,
    usize,
    usize,
    usize,
    usize,
);

fn decode_snapshot_header(
    cursor: &mut Cursor<'_>,
    version: u16,
) -> Result<SnapshotHeader, SnapshotError> {
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
    let reservation_member_count = if version >= 3 {
        usize::try_from(cursor.read_u32()?).map_err(|_| SnapshotError::CountTooLarge)?
    } else {
        0
    };
    let operation_count =
        usize::try_from(cursor.read_u32()?).map_err(|_| SnapshotError::CountTooLarge)?;
    let wheel_len =
        usize::try_from(cursor.read_u32()?).map_err(|_| SnapshotError::CountTooLarge)?;

    Ok((
        last_applied_lsn,
        last_request_slot,
        max_retired_reservation_id,
        resource_count,
        reservation_count,
        reservation_member_count,
        operation_count,
        wheel_len,
    ))
}

fn decode_snapshot_reservations(
    cursor: &mut Cursor<'_>,
    reservation_count: usize,
    version: u16,
) -> Result<Vec<ReservationRecord>, SnapshotError> {
    let mut reservations = Vec::with_capacity(reservation_count);
    for _ in 0..reservation_count {
        reservations.push(ReservationRecord {
            reservation_id: ReservationId(cursor.read_u128()?),
            resource_id: ResourceId(cursor.read_u128()?),
            holder_id: HolderId(cursor.read_u128()?),
            lease_epoch: if version >= 4 { cursor.read_u64()? } else { 1 },
            state: decode_reservation_state(cursor.read_u8()?)?,
            created_lsn: Lsn(cursor.read_u64()?),
            deadline_slot: Slot(cursor.read_u64()?),
            released_lsn: cursor.read_optional_u64()?.map(Lsn),
            retire_after_slot: cursor.read_optional_u64()?.map(Slot),
            member_count: if version >= 3 { cursor.read_u32()? } else { 1 },
        });
    }

    Ok(reservations)
}

fn decode_snapshot_reservation_members(
    cursor: &mut Cursor<'_>,
    version: u16,
    reservation_member_count: usize,
    reservations: &[ReservationRecord],
) -> Result<Vec<ReservationMemberRecord>, SnapshotError> {
    let mut reservation_members = Vec::with_capacity(if version >= 3 {
        reservation_member_count
    } else {
        reservations.len()
    });
    if version >= 3 {
        for _ in 0..reservation_member_count {
            reservation_members.push(ReservationMemberRecord {
                reservation_id: ReservationId(cursor.read_u128()?),
                resource_id: ResourceId(cursor.read_u128()?),
                member_index: cursor.read_u32()?,
            });
        }
    } else {
        for reservation in reservations {
            reservation_members.push(ReservationMemberRecord {
                reservation_id: reservation.reservation_id,
                resource_id: reservation.resource_id,
                member_index: 0,
            });
        }
    }

    Ok(reservation_members)
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
        ResourceState::Revoking => 4,
    }
}

fn decode_resource_state(tag: u8) -> Result<ResourceState, SnapshotError> {
    match tag {
        1 => Ok(ResourceState::Available),
        2 => Ok(ResourceState::Reserved),
        3 => Ok(ResourceState::Confirmed),
        4 => Ok(ResourceState::Revoking),
        _ => Err(SnapshotError::InvalidStateTag(tag)),
    }
}

fn encode_reservation_state(state: ReservationState) -> u8 {
    match state {
        ReservationState::Reserved => 1,
        ReservationState::Confirmed => 2,
        ReservationState::Released => 3,
        ReservationState::Expired => 4,
        ReservationState::Revoking => 5,
        ReservationState::Revoked => 6,
    }
}

fn decode_reservation_state(tag: u8) -> Result<ReservationState, SnapshotError> {
    match tag {
        1 => Ok(ReservationState::Reserved),
        2 => Ok(ReservationState::Confirmed),
        3 => Ok(ReservationState::Released),
        4 => Ok(ReservationState::Expired),
        5 => Ok(ReservationState::Revoking),
        6 => Ok(ReservationState::Revoked),
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
        6 => Ok(ResultCode::BundleTooLarge),
        7 => Ok(ResultCode::TtlOutOfRange),
        8 => Ok(ResultCode::ReservationTableFull),
        9 => Ok(ResultCode::ReservationMemberTableFull),
        10 => Ok(ResultCode::ReservationNotFound),
        11 => Ok(ResultCode::ReservationRetired),
        12 => Ok(ResultCode::ExpirationIndexFull),
        13 => Ok(ResultCode::OperationTableFull),
        14 => Ok(ResultCode::OperationConflict),
        15 => Ok(ResultCode::InvalidState),
        16 => Ok(ResultCode::HolderMismatch),
        17 => Ok(ResultCode::StaleEpoch),
        18 => Ok(ResultCode::SlotOverflow),
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
        ResultCode::BundleTooLarge => 6,
        ResultCode::TtlOutOfRange => 7,
        ResultCode::ReservationTableFull => 8,
        ResultCode::ReservationMemberTableFull => 9,
        ResultCode::ReservationNotFound => 10,
        ResultCode::ReservationRetired => 11,
        ResultCode::ExpirationIndexFull => 12,
        ResultCode::OperationTableFull => 13,
        ResultCode::OperationConflict => 14,
        ResultCode::InvalidState => 15,
        ResultCode::HolderMismatch => 16,
        ResultCode::StaleEpoch => 17,
        ResultCode::SlotOverflow => 18,
    }
}

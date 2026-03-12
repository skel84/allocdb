use crate::config::{Config, ConfigError};
use crate::fixed_map::FixedMapError;
use crate::ids::{Lsn, Slot};
use crate::state_machine::{
    AllocDb, AllocDbInvariantError, OperationRecord, ReservationRecord, ResourceRecord,
};

#[path = "snapshot_codec.rs"]
mod codec;
#[path = "snapshot_cursor.rs"]
mod cursor;
#[cfg(test)]
#[path = "snapshot_tests.rs"]
mod tests;

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
    InconsistentWatermarks {
        last_applied_lsn: Option<Lsn>,
        last_request_slot: Option<Slot>,
    },
    ResourceTableOverCapacity {
        count: usize,
        max: usize,
    },
    ReservationTableOverCapacity {
        count: usize,
        max: usize,
    },
    OperationTableOverCapacity {
        count: usize,
        max: usize,
    },
    DuplicateResourceId(crate::ids::ResourceId),
    DuplicateReservationId(crate::ids::ReservationId),
    DuplicateOperationId(crate::ids::OperationId),
    Invariant(AllocDbInvariantError),
    Config(ConfigError),
}

impl From<ConfigError> for SnapshotError {
    fn from(error: ConfigError) -> Self {
        Self::Config(error)
    }
}

impl From<AllocDbInvariantError> for SnapshotError {
    fn from(error: AllocDbInvariantError) -> Self {
        Self::Invariant(error)
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
        codec::encode_snapshot(self)
    }

    /// Decodes one snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError`] when the snapshot is incomplete or structurally invalid.
    pub fn decode(bytes: &[u8]) -> Result<Self, SnapshotError> {
        codec::decode_snapshot(bytes)
    }
}

impl AllocDb {
    /// Captures a snapshot of the current trusted-core state.
    #[must_use]
    pub fn snapshot(&self) -> Snapshot {
        let mut resources: Vec<_> = self.resources.iter().copied().collect();
        resources.sort_unstable_by_key(|record| record.resource_id.get());

        let mut reservations: Vec<_> = self.reservations.iter().copied().collect();
        reservations.sort_unstable_by_key(|record| record.reservation_id.get());

        let mut operations: Vec<_> = self.operations.iter().copied().collect();
        operations.sort_unstable_by_key(|record| record.operation_id.get());

        Snapshot {
            last_applied_lsn: self.last_applied_lsn,
            last_request_slot: self.last_request_slot,
            resources,
            reservations,
            operations,
            wheel: self.wheel.clone(),
        }
    }

    /// Restores an allocator from one decoded snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotError`] when configuration validation fails or when decoded snapshot
    /// contents violate configured capacity or trusted-core invariants.
    ///
    /// # Panics
    ///
    /// Panics only if validated capacities still cannot fit the platform `usize`.
    pub fn from_snapshot(config: Config, snapshot: Snapshot) -> Result<Self, SnapshotError> {
        let Snapshot {
            last_applied_lsn,
            last_request_slot,
            resources,
            reservations,
            operations,
            wheel,
        } = snapshot;

        let mut db = Self::new(config)?;
        if wheel.len() != db.config.wheel_len() {
            return Err(SnapshotError::InvalidLayout);
        }
        validate_progress_watermarks(last_applied_lsn, last_request_slot)?;

        let max_resources =
            usize::try_from(db.config.max_resources).expect("validated max_resources must fit");
        let max_reservations = usize::try_from(db.config.max_reservations)
            .expect("validated max_reservations must fit");
        let max_operations =
            usize::try_from(db.config.max_operations).expect("validated max_operations must fit");

        ensure_snapshot_capacity(resources.len(), max_resources, |count, max| {
            SnapshotError::ResourceTableOverCapacity { count, max }
        })?;
        ensure_snapshot_capacity(reservations.len(), max_reservations, |count, max| {
            SnapshotError::ReservationTableOverCapacity { count, max }
        })?;
        ensure_snapshot_capacity(operations.len(), max_operations, |count, max| {
            SnapshotError::OperationTableOverCapacity { count, max }
        })?;

        restore_resources(&mut db, resources, max_resources)?;
        let mut reservation_retire_entries =
            restore_reservations(&mut db, reservations, max_reservations)?;
        let mut operation_retire_entries = restore_operations(&mut db, operations, max_operations)?;
        db.rebuild_retire_queues(
            &mut reservation_retire_entries,
            &mut operation_retire_entries,
        );
        db.wheel = wheel;
        db.last_applied_lsn = last_applied_lsn;
        db.last_request_slot = last_request_slot;
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

fn restore_resources(
    db: &mut AllocDb,
    resources: Vec<ResourceRecord>,
    max_resources: usize,
) -> Result<(), SnapshotError> {
    for record in resources {
        match db.resources.insert(record.resource_id, record) {
            Ok(()) => {}
            Err(FixedMapError::DuplicateKey) => {
                return Err(SnapshotError::DuplicateResourceId(record.resource_id));
            }
            Err(FixedMapError::Full) => {
                return Err(SnapshotError::ResourceTableOverCapacity {
                    count: max_resources.saturating_add(1),
                    max: max_resources,
                });
            }
        }
    }

    Ok(())
}

fn restore_reservations(
    db: &mut AllocDb,
    reservations: Vec<ReservationRecord>,
    max_reservations: usize,
) -> Result<Vec<(crate::ids::ReservationId, Slot, u64)>, SnapshotError> {
    let mut reservation_retire_entries = Vec::new();
    for record in reservations {
        if let Some(retire_after_slot) = record.retire_after_slot {
            reservation_retire_entries.push((
                record.reservation_id,
                retire_after_slot,
                record.created_lsn.get(),
            ));
        }
        match db.reservations.insert(record.reservation_id, record) {
            Ok(()) => {}
            Err(FixedMapError::DuplicateKey) => {
                return Err(SnapshotError::DuplicateReservationId(record.reservation_id));
            }
            Err(FixedMapError::Full) => {
                return Err(SnapshotError::ReservationTableOverCapacity {
                    count: max_reservations.saturating_add(1),
                    max: max_reservations,
                });
            }
        }
    }

    Ok(reservation_retire_entries)
}

fn restore_operations(
    db: &mut AllocDb,
    operations: Vec<OperationRecord>,
    max_operations: usize,
) -> Result<Vec<(crate::ids::OperationId, Slot, u64)>, SnapshotError> {
    let mut operation_retire_entries = Vec::new();
    for record in operations {
        operation_retire_entries.push((
            record.operation_id,
            record.retire_after_slot,
            record.applied_lsn.get(),
        ));
        match db.operations.insert(record.operation_id, record) {
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

    Ok(operation_retire_entries)
}

use crate::config::{Config, ConfigError};
use crate::ids::{Lsn, Slot};
use crate::state_machine::{AllocDb, OperationRecord, ReservationRecord, ResourceRecord};

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
    /// Returns [`SnapshotError`] when configuration validation fails or when snapshot layout does
    /// not match the configured timing wheel.
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

        for record in resources {
            db.insert_resource(record);
        }
        let mut reservation_retire_entries = Vec::new();
        for record in reservations {
            if let Some(retire_after_slot) = record.retire_after_slot {
                reservation_retire_entries.push((
                    record.reservation_id,
                    retire_after_slot,
                    record.created_lsn.get(),
                ));
            }
            db.insert_reservation(record);
        }
        let mut operation_retire_entries = Vec::new();
        for record in operations {
            operation_retire_entries.push((
                record.operation_id,
                record.retire_after_slot,
                record.applied_lsn.get(),
            ));
            db.insert_operation(record);
        }
        db.rebuild_retire_queues(
            &mut reservation_retire_entries,
            &mut operation_retire_entries,
        );
        db.wheel = wheel;
        db.last_applied_lsn = last_applied_lsn;
        db.last_request_slot = last_request_slot;
        db.assert_invariants();
        Ok(db)
    }
}

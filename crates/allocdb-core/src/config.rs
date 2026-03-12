#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Config {
    pub shard_id: u64,
    pub max_resources: u32,
    pub max_reservations: u32,
    pub max_operations: u32,
    pub max_ttl_slots: u64,
    pub max_client_retry_window_slots: u64,
    pub reservation_history_window_slots: u64,
    pub max_expiration_bucket_len: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigError {
    ZeroCapacity(&'static str),
    HistoryWindowTooLarge,
    OperationWindowTooLarge,
    BucketCapacityTooLarge,
    WheelTooLarge,
}

impl Config {
    /// Returns the dedupe retention window measured in logical slots.
    ///
    /// # Panics
    ///
    /// Panics if called before [`Self::validate`] has confirmed that the additive window fits in
    /// `u64`.
    #[must_use]
    pub fn operation_window_slots(&self) -> u64 {
        self.max_ttl_slots
            .checked_add(self.max_client_retry_window_slots)
            .expect("validated operation window must fit in u64")
    }

    /// Validates that the configured capacities and retention windows are internally consistent.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] when a required capacity is zero or when one configured bound
    /// contradicts another.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.max_resources == 0 {
            return Err(ConfigError::ZeroCapacity("max_resources"));
        }

        if self.max_reservations == 0 {
            return Err(ConfigError::ZeroCapacity("max_reservations"));
        }

        if self.max_operations == 0 {
            return Err(ConfigError::ZeroCapacity("max_operations"));
        }

        if self.max_ttl_slots == 0 {
            return Err(ConfigError::ZeroCapacity("max_ttl_slots"));
        }

        if self.max_expiration_bucket_len == 0 {
            return Err(ConfigError::ZeroCapacity("max_expiration_bucket_len"));
        }

        if self.reservation_history_window_slots > self.max_ttl_slots {
            return Err(ConfigError::HistoryWindowTooLarge);
        }

        let Some(_) = self
            .max_ttl_slots
            .checked_add(self.max_client_retry_window_slots)
        else {
            return Err(ConfigError::OperationWindowTooLarge);
        };

        if self.max_expiration_bucket_len > self.max_reservations {
            return Err(ConfigError::BucketCapacityTooLarge);
        }

        let Some(_) = self.max_ttl_slots.checked_add(1) else {
            return Err(ConfigError::WheelTooLarge);
        };

        Ok(())
    }

    /// Returns the number of timing-wheel buckets required for the configured TTL range.
    ///
    /// # Panics
    ///
    /// Panics if called with a configuration that did not already pass [`Self::validate`].
    #[must_use]
    pub fn wheel_len(&self) -> usize {
        usize::try_from(self.max_ttl_slots + 1)
            .expect("validated max_ttl_slots must fit the wheel length")
    }
}

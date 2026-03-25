#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Config {
    pub max_buckets: u32,
    pub max_operations: u32,
    pub max_batch_len: u32,
    pub max_client_retry_window_slots: u64,
    pub max_wal_payload_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigError {
    ZeroCapacity(&'static str),
}

impl Config {
    /// Validates that configured bounds are non-zero and usable by fixed-capacity structures.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] when a required bound is zero.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.max_buckets == 0 {
            return Err(ConfigError::ZeroCapacity("max_buckets"));
        }
        if self.max_operations == 0 {
            return Err(ConfigError::ZeroCapacity("max_operations"));
        }
        if self.max_batch_len == 0 {
            return Err(ConfigError::ZeroCapacity("max_batch_len"));
        }
        if self.max_client_retry_window_slots == 0 {
            return Err(ConfigError::ZeroCapacity("max_client_retry_window_slots"));
        }
        if self.max_wal_payload_bytes == 0 {
            return Err(ConfigError::ZeroCapacity("max_wal_payload_bytes"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{Config, ConfigError};

    fn config() -> Config {
        Config {
            max_buckets: 8,
            max_operations: 16,
            max_batch_len: 8,
            max_client_retry_window_slots: 8,
            max_wal_payload_bytes: 1024,
        }
    }

    #[test]
    fn validate_accepts_non_zero_bounds() {
        assert_eq!(config().validate(), Ok(()));
    }

    #[test]
    fn validate_rejects_zero_capacity() {
        let mut config = config();
        config.max_operations = 0;

        assert_eq!(
            config.validate(),
            Err(ConfigError::ZeroCapacity("max_operations"))
        );
    }
}

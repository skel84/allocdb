use crate::ids::{HoldId, PoolId};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResultCode {
    Ok,
    AlreadyExists,
    PoolTableFull,
    PoolNotFound,
    HoldTableFull,
    HoldNotFound,
    InsufficientCapacity,
    HoldExpired,
    InvalidState,
    OperationConflict,
    OperationTableFull,
    SlotOverflow,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommandOutcome {
    pub result_code: ResultCode,
    pub pool_id: Option<PoolId>,
    pub hold_id: Option<HoldId>,
}

impl CommandOutcome {
    #[must_use]
    pub const fn new(result_code: ResultCode) -> Self {
        Self {
            result_code,
            pool_id: None,
            hold_id: None,
        }
    }

    #[must_use]
    pub const fn with_pool(result_code: ResultCode, pool_id: PoolId) -> Self {
        Self {
            result_code,
            pool_id: Some(pool_id),
            hold_id: None,
        }
    }

    #[must_use]
    pub const fn with_hold(result_code: ResultCode, hold_id: HoldId) -> Self {
        Self {
            result_code,
            pool_id: None,
            hold_id: Some(hold_id),
        }
    }

    #[must_use]
    pub const fn with_pool_and_hold(
        result_code: ResultCode,
        pool_id: PoolId,
        hold_id: HoldId,
    ) -> Self {
        Self {
            result_code,
            pool_id: Some(pool_id),
            hold_id: Some(hold_id),
        }
    }
}

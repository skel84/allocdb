use crate::ids::BucketId;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResultCode {
    Ok,
    AlreadyExists,
    BucketTableFull,
    BucketNotFound,
    InsufficientFunds,
    OperationConflict,
    OperationTableFull,
    SlotOverflow,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommandOutcome {
    pub result_code: ResultCode,
    pub bucket_id: Option<BucketId>,
}

impl CommandOutcome {
    #[must_use]
    pub const fn new(result_code: ResultCode) -> Self {
        Self {
            result_code,
            bucket_id: None,
        }
    }

    #[must_use]
    pub const fn with_bucket(result_code: ResultCode, bucket_id: BucketId) -> Self {
        Self {
            result_code,
            bucket_id: Some(bucket_id),
        }
    }
}

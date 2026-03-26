use crate::ids::{BucketId, ClientId, Lsn, OperationId, Slot};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommandContext {
    pub lsn: Lsn,
    pub request_slot: Slot,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClientRequest {
    pub operation_id: OperationId,
    pub client_id: ClientId,
    pub command: Command,
}

impl AsRef<ClientRequest> for ClientRequest {
    fn as_ref(&self) -> &ClientRequest {
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Command {
    CreateBucket {
        bucket_id: BucketId,
        limit: u64,
        initial_balance: u64,
        refill_rate_per_slot: u64,
    },
    Debit {
        bucket_id: BucketId,
        amount: u64,
    },
}

impl AsRef<Command> for Command {
    fn as_ref(&self) -> &Command {
        self
    }
}

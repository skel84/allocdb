use crate::ids::{ClientId, HoldId, Lsn, OperationId, PoolId, Slot};

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
    CreatePool {
        pool_id: PoolId,
        total_capacity: u64,
    },
    PlaceHold {
        pool_id: PoolId,
        hold_id: HoldId,
        quantity: u64,
        deadline_slot: Slot,
    },
    ConfirmHold {
        hold_id: HoldId,
    },
    ReleaseHold {
        hold_id: HoldId,
    },
    ExpireHold {
        hold_id: HoldId,
    },
}

impl AsRef<Command> for Command {
    fn as_ref(&self) -> &Command {
        self
    }
}

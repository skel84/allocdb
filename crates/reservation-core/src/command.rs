use crate::ids::{ClientId, HoldId, Lsn, OperationId, PoolId, Slot};

pub(crate) const TAG_CREATE_POOL: u8 = 1;
pub(crate) const TAG_PLACE_HOLD: u8 = 2;
pub(crate) const TAG_CONFIRM_HOLD: u8 = 3;
pub(crate) const TAG_RELEASE_HOLD: u8 = 4;
pub(crate) const TAG_EXPIRE_HOLD: u8 = 5;

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

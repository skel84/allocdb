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

impl Command {
    #[must_use]
    pub fn fingerprint(&self) -> u128 {
        let mut state = 0x7b0d_87a1_7d16_2203_135f_5f63_2570_6f37u128;

        match self {
            Self::CreateBucket {
                bucket_id,
                limit,
                initial_balance,
                refill_rate_per_slot,
            } => {
                state = mix(state, 1);
                state = mix(state, bucket_id.get());
                state = mix(state, u128::from(*limit));
                state = mix(state, u128::from(*initial_balance));
                mix(state, u128::from(*refill_rate_per_slot))
            }
            Self::Debit { bucket_id, amount } => {
                state = mix(state, 2);
                state = mix(state, bucket_id.get());
                mix(state, u128::from(*amount))
            }
        }
    }
}

fn mix(state: u128, value: u128) -> u128 {
    let mixed = state ^ value.wrapping_add(0x9e37_79b9_7f4a_7c15_6eed_0e9d_a4d9_4a4fu128);
    mixed
        .rotate_left(29)
        .wrapping_mul(0x94d0_49bb_1331_11ebu128)
}

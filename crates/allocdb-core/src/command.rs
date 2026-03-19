use crate::ids::{ClientId, HolderId, Lsn, OperationId, ReservationId, ResourceId, Slot};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommandContext {
    pub lsn: Lsn,
    pub request_slot: Slot,
}

#[derive(Clone, Debug, Eq, PartialEq)]
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Command {
    CreateResource {
        resource_id: ResourceId,
    },
    Reserve {
        resource_id: ResourceId,
        holder_id: HolderId,
        ttl_slots: u64,
    },
    ReserveBundle {
        resource_ids: Vec<ResourceId>,
        holder_id: HolderId,
        ttl_slots: u64,
    },
    Confirm {
        reservation_id: ReservationId,
        holder_id: HolderId,
        lease_epoch: u64,
    },
    Release {
        reservation_id: ReservationId,
        holder_id: HolderId,
        lease_epoch: u64,
    },
    Revoke {
        reservation_id: ReservationId,
    },
    Reclaim {
        reservation_id: ReservationId,
    },
    Expire {
        reservation_id: ReservationId,
        deadline_slot: Slot,
    },
}

impl AsRef<Command> for Command {
    fn as_ref(&self) -> &Command {
        self
    }
}

impl Command {
    #[must_use]
    ///
    /// # Panics
    ///
    /// Panics only if the in-memory bundle length cannot fit into `u128`, which cannot happen on
    /// supported targets because slice lengths are already bounded by `usize`.
    pub fn fingerprint(&self) -> u128 {
        let mut state = 0x6c62_272e_07bb_0142_62b8_2175_6295_c58du128;

        match self {
            Self::CreateResource { resource_id } => {
                state = mix(state, 1);
                mix(state, resource_id.get())
            }
            Self::Reserve {
                resource_id,
                holder_id,
                ttl_slots,
            } => {
                state = mix(state, 2);
                state = mix(state, resource_id.get());
                state = mix(state, holder_id.get());
                mix(state, u128::from(*ttl_slots))
            }
            Self::ReserveBundle {
                resource_ids,
                holder_id,
                ttl_slots,
            } => {
                state = mix(state, 3);
                state = mix(
                    state,
                    u128::try_from(resource_ids.len()).expect("bundle len must fit u128"),
                );
                for resource_id in resource_ids {
                    state = mix(state, resource_id.get());
                }
                state = mix(state, holder_id.get());
                mix(state, u128::from(*ttl_slots))
            }
            Self::Confirm {
                reservation_id,
                holder_id,
                lease_epoch,
            } => {
                state = mix(state, 4);
                state = mix(state, reservation_id.get());
                state = mix(state, holder_id.get());
                mix(state, u128::from(*lease_epoch))
            }
            Self::Release {
                reservation_id,
                holder_id,
                lease_epoch,
            } => {
                state = mix(state, 5);
                state = mix(state, reservation_id.get());
                state = mix(state, holder_id.get());
                mix(state, u128::from(*lease_epoch))
            }
            Self::Revoke { reservation_id } => {
                state = mix(state, 6);
                mix(state, reservation_id.get())
            }
            Self::Reclaim { reservation_id } => {
                state = mix(state, 7);
                mix(state, reservation_id.get())
            }
            Self::Expire {
                reservation_id,
                deadline_slot,
            } => {
                state = mix(state, 8);
                state = mix(state, reservation_id.get());
                mix(state, u128::from(deadline_slot.get()))
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

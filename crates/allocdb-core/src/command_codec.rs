use crate::command::{ClientRequest, Command};
use crate::ids::{ClientId, HolderId, OperationId, ReservationId, ResourceId, Slot};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommandCodecError {
    BufferTooShort,
    InvalidCommandTag(u8),
    InvalidLayout,
}

#[must_use]
pub fn encode_client_request(request: impl AsRef<ClientRequest>) -> Vec<u8> {
    let request = request.as_ref();
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&request.operation_id.get().to_le_bytes());
    bytes.extend_from_slice(&request.client_id.get().to_le_bytes());
    encode_command(&mut bytes, &request.command);
    bytes
}

/// Decodes one client-visible request payload.
///
/// # Errors
///
/// Returns [`CommandCodecError`] when the payload is truncated or structurally invalid.
pub fn decode_client_request(bytes: &[u8]) -> Result<ClientRequest, CommandCodecError> {
    let mut cursor = Cursor::new(bytes);
    let operation_id = OperationId(cursor.read_u128()?);
    let client_id = ClientId(cursor.read_u128()?);
    let command = decode_command(&mut cursor)?;
    cursor.finish()?;
    Ok(ClientRequest {
        operation_id,
        client_id,
        command,
    })
}

#[must_use]
pub fn encode_internal_command(command: impl AsRef<Command>) -> Vec<u8> {
    let command = command.as_ref();
    let mut bytes = Vec::new();
    encode_command(&mut bytes, command);
    bytes
}

/// Decodes one internal command payload.
///
/// # Errors
///
/// Returns [`CommandCodecError`] when the payload is truncated or structurally invalid.
pub fn decode_internal_command(bytes: &[u8]) -> Result<Command, CommandCodecError> {
    let mut cursor = Cursor::new(bytes);
    let command = decode_command(&mut cursor)?;
    cursor.finish()?;
    Ok(command)
}

fn encode_command(bytes: &mut Vec<u8>, command: &Command) {
    match command {
        Command::CreateResource { resource_id } => {
            bytes.push(1);
            bytes.extend_from_slice(&resource_id.get().to_le_bytes());
        }
        Command::Reserve {
            resource_id,
            holder_id,
            ttl_slots,
        } => {
            bytes.push(2);
            bytes.extend_from_slice(&resource_id.get().to_le_bytes());
            bytes.extend_from_slice(&holder_id.get().to_le_bytes());
            bytes.extend_from_slice(&ttl_slots.to_le_bytes());
        }
        Command::ReserveBundle {
            resource_ids,
            holder_id,
            ttl_slots,
        } => {
            bytes.push(3);
            bytes.extend_from_slice(
                &u32::try_from(resource_ids.len())
                    .expect("bundle len must fit u32")
                    .to_le_bytes(),
            );
            for resource_id in resource_ids {
                bytes.extend_from_slice(&resource_id.get().to_le_bytes());
            }
            bytes.extend_from_slice(&holder_id.get().to_le_bytes());
            bytes.extend_from_slice(&ttl_slots.to_le_bytes());
        }
        Command::Confirm {
            reservation_id,
            holder_id,
            lease_epoch,
        } => {
            bytes.push(4);
            bytes.extend_from_slice(&reservation_id.get().to_le_bytes());
            bytes.extend_from_slice(&holder_id.get().to_le_bytes());
            bytes.extend_from_slice(&lease_epoch.to_le_bytes());
        }
        Command::Release {
            reservation_id,
            holder_id,
            lease_epoch,
        } => {
            bytes.push(5);
            bytes.extend_from_slice(&reservation_id.get().to_le_bytes());
            bytes.extend_from_slice(&holder_id.get().to_le_bytes());
            bytes.extend_from_slice(&lease_epoch.to_le_bytes());
        }
        Command::Revoke { reservation_id } => {
            bytes.push(7);
            bytes.extend_from_slice(&reservation_id.get().to_le_bytes());
        }
        Command::Reclaim { reservation_id } => {
            bytes.push(8);
            bytes.extend_from_slice(&reservation_id.get().to_le_bytes());
        }
        Command::Expire {
            reservation_id,
            deadline_slot,
        } => {
            bytes.push(6);
            bytes.extend_from_slice(&reservation_id.get().to_le_bytes());
            bytes.extend_from_slice(&deadline_slot.get().to_le_bytes());
        }
    }
}

fn decode_command(cursor: &mut Cursor<'_>) -> Result<Command, CommandCodecError> {
    match cursor.read_u8()? {
        1 => Ok(Command::CreateResource {
            resource_id: ResourceId(cursor.read_u128()?),
        }),
        2 => Ok(Command::Reserve {
            resource_id: ResourceId(cursor.read_u128()?),
            holder_id: HolderId(cursor.read_u128()?),
            ttl_slots: cursor.read_u64()?,
        }),
        3 => {
            let resource_count = usize::try_from(cursor.read_u32()?)
                .map_err(|_| CommandCodecError::InvalidLayout)?;
            let mut resource_ids = Vec::with_capacity(resource_count);
            for _ in 0..resource_count {
                resource_ids.push(ResourceId(cursor.read_u128()?));
            }
            Ok(Command::ReserveBundle {
                resource_ids,
                holder_id: HolderId(cursor.read_u128()?),
                ttl_slots: cursor.read_u64()?,
            })
        }
        4 => Ok(Command::Confirm {
            reservation_id: ReservationId(cursor.read_u128()?),
            holder_id: HolderId(cursor.read_u128()?),
            lease_epoch: decode_optional_legacy_epoch(cursor)?,
        }),
        5 => Ok(Command::Release {
            reservation_id: ReservationId(cursor.read_u128()?),
            holder_id: HolderId(cursor.read_u128()?),
            lease_epoch: decode_optional_legacy_epoch(cursor)?,
        }),
        6 => Ok(Command::Expire {
            reservation_id: ReservationId(cursor.read_u128()?),
            deadline_slot: Slot(cursor.read_u64()?),
        }),
        7 => Ok(Command::Revoke {
            reservation_id: ReservationId(cursor.read_u128()?),
        }),
        8 => Ok(Command::Reclaim {
            reservation_id: ReservationId(cursor.read_u128()?),
        }),
        value => Err(CommandCodecError::InvalidCommandTag(value)),
    }
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn finish(&self) -> Result<(), CommandCodecError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(CommandCodecError::InvalidLayout)
        }
    }

    fn read_exact<const N: usize>(&mut self) -> Result<[u8; N], CommandCodecError> {
        if self.offset + N > self.bytes.len() {
            return Err(CommandCodecError::BufferTooShort);
        }

        let mut array = [0_u8; N];
        array.copy_from_slice(&self.bytes[self.offset..self.offset + N]);
        self.offset += N;
        Ok(array)
    }

    fn read_u8(&mut self) -> Result<u8, CommandCodecError> {
        Ok(self.read_exact::<1>()?[0])
    }

    fn read_u64(&mut self) -> Result<u64, CommandCodecError> {
        Ok(u64::from_le_bytes(self.read_exact::<8>()?))
    }

    fn read_u32(&mut self) -> Result<u32, CommandCodecError> {
        Ok(u32::from_le_bytes(self.read_exact::<4>()?))
    }

    fn read_u128(&mut self) -> Result<u128, CommandCodecError> {
        Ok(u128::from_le_bytes(self.read_exact::<16>()?))
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }
}

fn decode_optional_legacy_epoch(cursor: &mut Cursor<'_>) -> Result<u64, CommandCodecError> {
    match cursor.remaining() {
        0 => Ok(1),
        8 => cursor.read_u64(),
        _ => Err(CommandCodecError::InvalidLayout),
    }
}

#[cfg(test)]
mod tests {
    use crate::command::{ClientRequest, Command};
    use crate::ids::{ClientId, HolderId, OperationId, ReservationId, ResourceId, Slot};

    use super::{
        CommandCodecError, decode_client_request, decode_internal_command, encode_client_request,
        encode_internal_command,
    };

    #[test]
    fn client_request_round_trips() {
        let request = ClientRequest {
            operation_id: OperationId(11),
            client_id: ClientId(12),
            command: Command::Reserve {
                resource_id: ResourceId(13),
                holder_id: HolderId(14),
                ttl_slots: 15,
            },
        };

        let decoded = decode_client_request(&encode_client_request(&request)).unwrap();
        assert_eq!(decoded, request);
    }

    #[test]
    fn internal_command_round_trips() {
        let command = Command::Expire {
            reservation_id: ReservationId(99),
            deadline_slot: Slot(42),
        };

        let decoded = decode_internal_command(&encode_internal_command(&command)).unwrap();
        assert_eq!(decoded, command);
    }

    #[test]
    fn revoke_and_reclaim_round_trip() {
        for command in [
            Command::Revoke {
                reservation_id: ReservationId(41),
            },
            Command::Reclaim {
                reservation_id: ReservationId(42),
            },
        ] {
            let decoded = decode_internal_command(&encode_internal_command(&command)).unwrap();
            assert_eq!(decoded, command);
        }
    }

    #[test]
    fn decoder_accepts_legacy_expire_tag_payload() {
        let mut bytes = Vec::new();
        bytes.push(6);
        bytes.extend_from_slice(&ReservationId(99).get().to_le_bytes());
        bytes.extend_from_slice(&Slot(42).get().to_le_bytes());

        let decoded = decode_internal_command(&bytes).unwrap();
        assert_eq!(
            decoded,
            Command::Expire {
                reservation_id: ReservationId(99),
                deadline_slot: Slot(42),
            }
        );
    }

    #[test]
    fn reserve_bundle_round_trips() {
        let request = ClientRequest {
            operation_id: OperationId(11),
            client_id: ClientId(12),
            command: Command::ReserveBundle {
                resource_ids: vec![ResourceId(13), ResourceId(14)],
                holder_id: HolderId(15),
                ttl_slots: 16,
            },
        };

        let decoded = decode_client_request(&encode_client_request(&request)).unwrap();
        assert_eq!(decoded, request);
    }

    #[test]
    fn holder_commands_round_trip_with_lease_epoch() {
        let requests = [
            ClientRequest {
                operation_id: OperationId(11),
                client_id: ClientId(12),
                command: Command::Confirm {
                    reservation_id: ReservationId(13),
                    holder_id: HolderId(14),
                    lease_epoch: 15,
                },
            },
            ClientRequest {
                operation_id: OperationId(21),
                client_id: ClientId(22),
                command: Command::Release {
                    reservation_id: ReservationId(23),
                    holder_id: HolderId(24),
                    lease_epoch: 25,
                },
            },
        ];

        for request in requests {
            let decoded = decode_client_request(&encode_client_request(&request)).unwrap();
            assert_eq!(decoded, request);
        }
    }

    #[test]
    fn decoder_accepts_legacy_holder_command_without_lease_epoch() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&OperationId(11).get().to_le_bytes());
        bytes.extend_from_slice(&ClientId(12).get().to_le_bytes());
        bytes.push(4);
        bytes.extend_from_slice(&ReservationId(13).get().to_le_bytes());
        bytes.extend_from_slice(&HolderId(14).get().to_le_bytes());

        let decoded = decode_client_request(&bytes).unwrap();
        assert_eq!(
            decoded,
            ClientRequest {
                operation_id: OperationId(11),
                client_id: ClientId(12),
                command: Command::Confirm {
                    reservation_id: ReservationId(13),
                    holder_id: HolderId(14),
                    lease_epoch: 1,
                },
            }
        );
    }

    #[test]
    fn decoder_rejects_truncated_payload() {
        let request = ClientRequest {
            operation_id: OperationId(1),
            client_id: ClientId(2),
            command: Command::CreateResource {
                resource_id: ResourceId(3),
            },
        };
        let mut bytes = encode_client_request(&request);
        bytes.pop();

        assert_eq!(
            decode_client_request(&bytes),
            Err(CommandCodecError::BufferTooShort)
        );
    }

    #[test]
    fn decoder_rejects_unknown_command_tag() {
        assert_eq!(
            decode_internal_command(&[99]),
            Err(CommandCodecError::InvalidCommandTag(99))
        );
    }
}

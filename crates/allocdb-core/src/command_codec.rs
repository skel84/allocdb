use crate::command::{ClientRequest, Command};
use crate::ids::{ClientId, HolderId, OperationId, ReservationId, ResourceId, Slot};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommandCodecError {
    BufferTooShort,
    InvalidCommandTag(u8),
    InvalidLayout,
}

#[must_use]
pub fn encode_client_request(request: ClientRequest) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&request.operation_id.get().to_le_bytes());
    bytes.extend_from_slice(&request.client_id.get().to_le_bytes());
    encode_command(&mut bytes, request.command);
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
pub fn encode_internal_command(command: Command) -> Vec<u8> {
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

fn encode_command(bytes: &mut Vec<u8>, command: Command) {
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
        Command::Confirm {
            reservation_id,
            holder_id,
        } => {
            bytes.push(3);
            bytes.extend_from_slice(&reservation_id.get().to_le_bytes());
            bytes.extend_from_slice(&holder_id.get().to_le_bytes());
        }
        Command::Release {
            reservation_id,
            holder_id,
        } => {
            bytes.push(4);
            bytes.extend_from_slice(&reservation_id.get().to_le_bytes());
            bytes.extend_from_slice(&holder_id.get().to_le_bytes());
        }
        Command::Expire {
            reservation_id,
            deadline_slot,
        } => {
            bytes.push(5);
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
        3 => Ok(Command::Confirm {
            reservation_id: ReservationId(cursor.read_u128()?),
            holder_id: HolderId(cursor.read_u128()?),
        }),
        4 => Ok(Command::Release {
            reservation_id: ReservationId(cursor.read_u128()?),
            holder_id: HolderId(cursor.read_u128()?),
        }),
        5 => Ok(Command::Expire {
            reservation_id: ReservationId(cursor.read_u128()?),
            deadline_slot: Slot(cursor.read_u64()?),
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

    fn read_u128(&mut self) -> Result<u128, CommandCodecError> {
        Ok(u128::from_le_bytes(self.read_exact::<16>()?))
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

        let decoded = decode_client_request(&encode_client_request(request)).unwrap();
        assert_eq!(decoded, request);
    }

    #[test]
    fn internal_command_round_trips() {
        let command = Command::Expire {
            reservation_id: ReservationId(99),
            deadline_slot: Slot(42),
        };

        let decoded = decode_internal_command(&encode_internal_command(command)).unwrap();
        assert_eq!(decoded, command);
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
        let mut bytes = encode_client_request(request);
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

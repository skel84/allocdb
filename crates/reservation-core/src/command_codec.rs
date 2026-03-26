use crate::command::{ClientRequest, Command};
use crate::ids::{ClientId, HoldId, OperationId, PoolId, Slot};

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

pub fn decode_internal_command(bytes: &[u8]) -> Result<Command, CommandCodecError> {
    let mut cursor = Cursor::new(bytes);
    let command = decode_command(&mut cursor)?;
    cursor.finish()?;
    Ok(command)
}

fn encode_command(bytes: &mut Vec<u8>, command: &Command) {
    match command {
        Command::CreatePool {
            pool_id,
            total_capacity,
        } => {
            bytes.push(1);
            bytes.extend_from_slice(&pool_id.get().to_le_bytes());
            bytes.extend_from_slice(&total_capacity.to_le_bytes());
        }
        Command::PlaceHold {
            pool_id,
            hold_id,
            quantity,
            deadline_slot,
        } => {
            bytes.push(2);
            bytes.extend_from_slice(&pool_id.get().to_le_bytes());
            bytes.extend_from_slice(&hold_id.get().to_le_bytes());
            bytes.extend_from_slice(&quantity.to_le_bytes());
            bytes.extend_from_slice(&deadline_slot.get().to_le_bytes());
        }
        Command::ConfirmHold { hold_id } => {
            bytes.push(3);
            bytes.extend_from_slice(&hold_id.get().to_le_bytes());
        }
        Command::ReleaseHold { hold_id } => {
            bytes.push(4);
            bytes.extend_from_slice(&hold_id.get().to_le_bytes());
        }
        Command::ExpireHold { hold_id } => {
            bytes.push(5);
            bytes.extend_from_slice(&hold_id.get().to_le_bytes());
        }
    }
}

fn decode_command(cursor: &mut Cursor<'_>) -> Result<Command, CommandCodecError> {
    match cursor.read_u8()? {
        1 => Ok(Command::CreatePool {
            pool_id: PoolId(cursor.read_u128()?),
            total_capacity: cursor.read_u64()?,
        }),
        2 => Ok(Command::PlaceHold {
            pool_id: PoolId(cursor.read_u128()?),
            hold_id: HoldId(cursor.read_u128()?),
            quantity: cursor.read_u64()?,
            deadline_slot: Slot(cursor.read_u64()?),
        }),
        3 => Ok(Command::ConfirmHold {
            hold_id: HoldId(cursor.read_u128()?),
        }),
        4 => Ok(Command::ReleaseHold {
            hold_id: HoldId(cursor.read_u128()?),
        }),
        5 => Ok(Command::ExpireHold {
            hold_id: HoldId(cursor.read_u128()?),
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
    use crate::{
        command::{ClientRequest, Command},
        ids::{ClientId, HoldId, OperationId, PoolId, Slot},
    };

    use super::{
        CommandCodecError, decode_client_request, decode_internal_command, encode_client_request,
        encode_internal_command,
    };

    #[test]
    fn client_request_round_trips() {
        let request = ClientRequest {
            operation_id: OperationId(11),
            client_id: ClientId(12),
            command: Command::PlaceHold {
                pool_id: PoolId(13),
                hold_id: HoldId(14),
                quantity: 2,
                deadline_slot: Slot(15),
            },
        };

        let decoded = decode_client_request(&encode_client_request(request)).unwrap();
        assert_eq!(decoded, request);
    }

    #[test]
    fn internal_command_round_trips() {
        let command = Command::CreatePool {
            pool_id: PoolId(7),
            total_capacity: 9,
        };

        let decoded = decode_internal_command(&encode_internal_command(command)).unwrap();
        assert_eq!(decoded, command);
    }

    #[test]
    fn rejects_truncated_payload() {
        let error = decode_client_request(&[0_u8; 4]).unwrap_err();
        assert_eq!(error, CommandCodecError::BufferTooShort);
    }
}

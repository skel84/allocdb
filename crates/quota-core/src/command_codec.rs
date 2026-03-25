use crate::command::{ClientRequest, Command};
use crate::ids::{BucketId, ClientId, OperationId};

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
        Command::CreateBucket {
            bucket_id,
            limit,
            initial_balance,
            refill_rate_per_slot,
        } => {
            bytes.push(1);
            bytes.extend_from_slice(&bucket_id.get().to_le_bytes());
            bytes.extend_from_slice(&limit.to_le_bytes());
            bytes.extend_from_slice(&initial_balance.to_le_bytes());
            bytes.extend_from_slice(&refill_rate_per_slot.to_le_bytes());
        }
        Command::Debit { bucket_id, amount } => {
            bytes.push(2);
            bytes.extend_from_slice(&bucket_id.get().to_le_bytes());
            bytes.extend_from_slice(&amount.to_le_bytes());
        }
    }
}

fn decode_command(cursor: &mut Cursor<'_>) -> Result<Command, CommandCodecError> {
    match cursor.read_u8()? {
        1 => Ok(Command::CreateBucket {
            bucket_id: BucketId(cursor.read_u128()?),
            limit: cursor.read_u64()?,
            initial_balance: cursor.read_u64()?,
            refill_rate_per_slot: cursor.read_u64()?,
        }),
        2 => Ok(Command::Debit {
            bucket_id: BucketId(cursor.read_u128()?),
            amount: cursor.read_u64()?,
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
        ids::{BucketId, ClientId, OperationId},
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
            command: Command::Debit {
                bucket_id: BucketId(13),
                amount: 14,
            },
        };

        let decoded = decode_client_request(&encode_client_request(&request)).unwrap();
        assert_eq!(decoded, request);
    }

    #[test]
    fn internal_command_round_trips() {
        let command = Command::CreateBucket {
            bucket_id: BucketId(7),
            limit: 9,
            initial_balance: 5,
            refill_rate_per_slot: 2,
        };

        let decoded = decode_internal_command(&encode_internal_command(&command)).unwrap();
        assert_eq!(decoded, command);
    }

    #[test]
    fn rejects_truncated_payload() {
        let error = decode_client_request(&[0_u8; 4]).unwrap_err();
        assert_eq!(error, CommandCodecError::BufferTooShort);
    }
}

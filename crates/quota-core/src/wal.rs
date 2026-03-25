use crate::ids::{Lsn, Slot};

const MAGIC: u32 = 0x5154_424c;
const VERSION: u16 = 1;
const HEADER_LEN: usize = 31;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecordType {
    ClientCommand = 1,
    InternalCommand = 2,
    SnapshotMarker = 3,
}

impl RecordType {
    fn encode(self) -> u8 {
        self as u8
    }

    fn decode(value: u8) -> Result<Self, DecodeError> {
        match value {
            1 => Ok(Self::ClientCommand),
            2 => Ok(Self::InternalCommand),
            3 => Ok(Self::SnapshotMarker),
            _ => Err(DecodeError::InvalidRecordType(value)),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Frame {
    pub lsn: Lsn,
    pub request_slot: Slot,
    pub record_type: RecordType,
    pub payload: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DecodeError {
    BufferTooShort,
    InvalidMagic(u32),
    InvalidVersion(u16),
    InvalidRecordType(u8),
    InvalidChecksum,
    PayloadTooLarge,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScanResult {
    pub frames: Vec<Frame>,
    pub valid_up_to: usize,
    pub stop_reason: ScanStopReason,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScanStopReason {
    CleanEof,
    TornTail { offset: usize },
    Corruption { offset: usize, error: DecodeError },
}

impl Frame {
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let payload_len =
            u32::try_from(self.payload.len()).expect("payload length must fit in u32 for WAL");
        let mut bytes = Vec::with_capacity(HEADER_LEN + self.payload.len());
        bytes.extend_from_slice(&MAGIC.to_le_bytes());
        bytes.extend_from_slice(&VERSION.to_le_bytes());
        bytes.extend_from_slice(&self.lsn.get().to_le_bytes());
        bytes.extend_from_slice(&self.request_slot.get().to_le_bytes());
        bytes.push(self.record_type.encode());
        bytes.extend_from_slice(&payload_len.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
        bytes.extend_from_slice(&self.payload);

        let checksum = crc32c::crc32c(&bytes[8..]);
        bytes[27..31].copy_from_slice(&checksum.to_le_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        if bytes.len() < HEADER_LEN {
            return Err(DecodeError::BufferTooShort);
        }

        let magic = u32::from_le_bytes(bytes[0..4].try_into().expect("slice has exact size"));
        if magic != MAGIC {
            return Err(DecodeError::InvalidMagic(magic));
        }

        let version = u16::from_le_bytes(bytes[4..6].try_into().expect("slice has exact size"));
        if version != VERSION {
            return Err(DecodeError::InvalidVersion(version));
        }

        let lsn = Lsn(u64::from_le_bytes(
            bytes[6..14].try_into().expect("slice has exact size"),
        ));
        let request_slot = Slot(u64::from_le_bytes(
            bytes[14..22].try_into().expect("slice has exact size"),
        ));
        let record_type = RecordType::decode(bytes[22])?;
        let payload_len =
            u32::from_le_bytes(bytes[23..27].try_into().expect("slice has exact size"));
        let payload_len = usize::try_from(payload_len).expect("u32 payload must fit usize");
        let frame_len = HEADER_LEN + payload_len;
        if bytes.len() < frame_len {
            return Err(DecodeError::BufferTooShort);
        }

        let stored_checksum =
            u32::from_le_bytes(bytes[27..31].try_into().expect("slice has exact size"));
        let mut checksum_bytes = bytes[..frame_len].to_vec();
        checksum_bytes[27..31].copy_from_slice(&0u32.to_le_bytes());
        let computed_checksum = crc32c::crc32c(&checksum_bytes[8..]);
        if stored_checksum != computed_checksum {
            return Err(DecodeError::InvalidChecksum);
        }

        Ok(Self {
            lsn,
            request_slot,
            record_type,
            payload: bytes[31..frame_len].to_vec(),
        })
    }

    pub fn encoded_len(bytes: &[u8]) -> Result<usize, DecodeError> {
        if bytes.len() < HEADER_LEN {
            return Err(DecodeError::BufferTooShort);
        }

        let magic = u32::from_le_bytes(bytes[0..4].try_into().expect("slice has exact size"));
        if magic != MAGIC {
            return Err(DecodeError::InvalidMagic(magic));
        }

        let version = u16::from_le_bytes(bytes[4..6].try_into().expect("slice has exact size"));
        if version != VERSION {
            return Err(DecodeError::InvalidVersion(version));
        }

        let record_type = bytes[22];
        let _ = RecordType::decode(record_type)?;
        let payload_len =
            u32::from_le_bytes(bytes[23..27].try_into().expect("slice has exact size"));
        let payload_len = usize::try_from(payload_len).map_err(|_| DecodeError::PayloadTooLarge)?;
        Ok(HEADER_LEN + payload_len)
    }
}

#[must_use]
pub fn scan_frames(bytes: &[u8]) -> ScanResult {
    let mut frames = Vec::new();
    let mut offset = 0usize;
    let mut stop_reason = ScanStopReason::CleanEof;

    while offset < bytes.len() {
        let remaining = &bytes[offset..];
        let frame_len = match Frame::encoded_len(remaining) {
            Ok(frame_len) => frame_len,
            Err(DecodeError::BufferTooShort) => {
                stop_reason = ScanStopReason::TornTail { offset };
                break;
            }
            Err(error) => {
                stop_reason = ScanStopReason::Corruption { offset, error };
                break;
            }
        };

        if remaining.len() < frame_len {
            stop_reason = ScanStopReason::TornTail { offset };
            break;
        }

        match Frame::decode(&remaining[..frame_len]) {
            Ok(frame) => {
                frames.push(frame);
                offset += frame_len;
            }
            Err(DecodeError::BufferTooShort) => {
                stop_reason = ScanStopReason::TornTail { offset };
                break;
            }
            Err(error) => {
                stop_reason = ScanStopReason::Corruption { offset, error };
                break;
            }
        }
    }

    ScanResult {
        frames,
        valid_up_to: offset,
        stop_reason,
    }
}

#[cfg(test)]
mod tests {
    use super::{DecodeError, Frame, HEADER_LEN, RecordType, ScanStopReason, scan_frames};
    use crate::ids::{Lsn, Slot};

    fn frame(lsn: u64, slot: u64, payload: &[u8]) -> Frame {
        Frame {
            lsn: Lsn(lsn),
            request_slot: Slot(slot),
            record_type: RecordType::ClientCommand,
            payload: payload.to_vec(),
        }
    }

    #[test]
    fn frame_round_trips() {
        let encoded = frame(7, 11, b"abc").encode();
        let decoded = Frame::decode(&encoded).unwrap();

        assert_eq!(decoded.lsn, Lsn(7));
        assert_eq!(decoded.request_slot, Slot(11));
        assert_eq!(decoded.record_type, RecordType::ClientCommand);
        assert_eq!(decoded.payload, b"abc");
    }

    #[test]
    fn corrupted_checksum_is_rejected() {
        let mut encoded = frame(7, 11, b"abc").encode();
        encoded[HEADER_LEN] ^= 0xff;

        assert_eq!(Frame::decode(&encoded), Err(DecodeError::InvalidChecksum));
    }

    #[test]
    fn scan_reports_torn_tail() {
        let mut bytes = frame(1, 1, b"one").encode();
        let second = frame(2, 2, b"two").encode();
        bytes.extend_from_slice(&second[..second.len() - 1]);

        let scanned = scan_frames(&bytes);

        assert_eq!(scanned.frames.len(), 1);
        assert_eq!(
            scanned.stop_reason,
            ScanStopReason::TornTail {
                offset: frame(1, 1, b"one").encode().len(),
            }
        );
    }
}

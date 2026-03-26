#![allow(clippy::missing_panics_doc)]

pub const VERSION: u16 = 1;
pub const HEADER_LEN: usize = 31;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalFormat {
    pub magic: u32,
    pub checksum_start: usize,
}

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
pub struct RawFrame {
    pub lsn: u64,
    pub request_slot: u64,
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
    pub frames: Vec<RawFrame>,
    pub valid_up_to: usize,
    pub stop_reason: ScanStopReason,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScanStopReason {
    CleanEof,
    TornTail { offset: usize },
    Corruption { offset: usize, error: DecodeError },
}

impl RawFrame {
    #[must_use]
    pub fn encode_with(&self, format: WalFormat) -> Vec<u8> {
        let payload_len =
            u32::try_from(self.payload.len()).expect("payload length must fit in u32 for WAL");
        let mut bytes = Vec::with_capacity(HEADER_LEN + self.payload.len());
        bytes.extend_from_slice(&format.magic.to_le_bytes());
        bytes.extend_from_slice(&VERSION.to_le_bytes());
        bytes.extend_from_slice(&self.lsn.to_le_bytes());
        bytes.extend_from_slice(&self.request_slot.to_le_bytes());
        bytes.push(self.record_type.encode());
        bytes.extend_from_slice(&payload_len.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
        bytes.extend_from_slice(&self.payload);

        let checksum = crc32c::crc32c(&bytes[format.checksum_start..]);
        bytes[27..31].copy_from_slice(&checksum.to_le_bytes());
        bytes
    }

    /// # Errors
    ///
    /// Returns [`DecodeError`] when the buffer is incomplete or when framing or checksum validation
    /// fails.
    pub fn decode_with(bytes: &[u8], format: WalFormat) -> Result<Self, DecodeError> {
        if bytes.len() < HEADER_LEN {
            return Err(DecodeError::BufferTooShort);
        }

        let magic = u32::from_le_bytes(bytes[0..4].try_into().expect("slice has exact size"));
        if magic != format.magic {
            return Err(DecodeError::InvalidMagic(magic));
        }

        let version = u16::from_le_bytes(bytes[4..6].try_into().expect("slice has exact size"));
        if version != VERSION {
            return Err(DecodeError::InvalidVersion(version));
        }

        let lsn = u64::from_le_bytes(bytes[6..14].try_into().expect("slice has exact size"));
        let request_slot =
            u64::from_le_bytes(bytes[14..22].try_into().expect("slice has exact size"));
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
        let computed_checksum = crc32c::crc32c(&checksum_bytes[format.checksum_start..]);
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

    /// # Errors
    ///
    /// Returns [`DecodeError`] when the header is incomplete or internally invalid.
    pub fn encoded_len_with(bytes: &[u8], format: WalFormat) -> Result<usize, DecodeError> {
        if bytes.len() < HEADER_LEN {
            return Err(DecodeError::BufferTooShort);
        }

        let magic = u32::from_le_bytes(bytes[0..4].try_into().expect("slice has exact size"));
        if magic != format.magic {
            return Err(DecodeError::InvalidMagic(magic));
        }

        let version = u16::from_le_bytes(bytes[4..6].try_into().expect("slice has exact size"));
        if version != VERSION {
            return Err(DecodeError::InvalidVersion(version));
        }

        let _ = RecordType::decode(bytes[22])?;
        let payload_len =
            u32::from_le_bytes(bytes[23..27].try_into().expect("slice has exact size"));
        let payload_len = usize::try_from(payload_len).map_err(|_| DecodeError::PayloadTooLarge)?;
        Ok(HEADER_LEN + payload_len)
    }
}

#[must_use]
pub fn scan_frames_with(bytes: &[u8], format: WalFormat) -> ScanResult {
    let mut frames = Vec::new();
    let mut offset = 0usize;
    let mut stop_reason = ScanStopReason::CleanEof;

    while offset < bytes.len() {
        let remaining = &bytes[offset..];
        let frame_len = match RawFrame::encoded_len_with(remaining, format) {
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

        match RawFrame::decode_with(&remaining[..frame_len], format) {
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
    use super::{
        DecodeError, HEADER_LEN, RawFrame, RecordType, ScanStopReason, VERSION, WalFormat,
        scan_frames_with,
    };

    const FORMAT_FROM_HEADER: WalFormat = WalFormat {
        magic: 0x5154_424c,
        checksum_start: 6,
    };
    const FORMAT_AFTER_VERSION: WalFormat = WalFormat {
        magic: 0x4144_424c,
        checksum_start: 8,
    };

    fn frame(lsn: u64, slot: u64, payload: &[u8]) -> RawFrame {
        RawFrame {
            lsn,
            request_slot: slot,
            record_type: RecordType::ClientCommand,
            payload: payload.to_vec(),
        }
    }

    #[test]
    fn frame_round_trips_for_header_checksum_format() {
        let encoded = frame(7, 11, b"abc").encode_with(FORMAT_FROM_HEADER);
        let decoded = RawFrame::decode_with(&encoded, FORMAT_FROM_HEADER).unwrap();

        assert_eq!(decoded, frame(7, 11, b"abc"));
    }

    #[test]
    fn frame_round_trips_for_post_version_checksum_format() {
        let encoded = frame(7, 11, b"abc").encode_with(FORMAT_AFTER_VERSION);
        let decoded = RawFrame::decode_with(&encoded, FORMAT_AFTER_VERSION).unwrap();

        assert_eq!(decoded, frame(7, 11, b"abc"));
    }

    #[test]
    fn version_is_stable_and_shared() {
        let encoded = frame(1, 2, b"x").encode_with(FORMAT_FROM_HEADER);
        let version = u16::from_le_bytes(encoded[4..6].try_into().unwrap());

        assert_eq!(version, VERSION);
    }

    #[test]
    fn corrupted_checksum_is_rejected() {
        let mut encoded = frame(7, 11, b"abc").encode_with(FORMAT_FROM_HEADER);
        encoded[HEADER_LEN] ^= 0xff;

        assert_eq!(
            RawFrame::decode_with(&encoded, FORMAT_FROM_HEADER),
            Err(DecodeError::InvalidChecksum)
        );
    }

    #[test]
    fn scan_reports_torn_tail() {
        let mut bytes = frame(1, 1, b"one").encode_with(FORMAT_FROM_HEADER);
        let second = frame(2, 2, b"two").encode_with(FORMAT_FROM_HEADER);
        bytes.extend_from_slice(&second[..second.len() - 1]);

        let scanned = scan_frames_with(&bytes, FORMAT_FROM_HEADER);

        assert_eq!(scanned.frames.len(), 1);
        assert_eq!(
            scanned.stop_reason,
            ScanStopReason::TornTail {
                offset: frame(1, 1, b"one").encode_with(FORMAT_FROM_HEADER).len(),
            }
        );
    }
}

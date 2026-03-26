use crate::ids::{Lsn, Slot};
use allocdb_wal_frame::{RawFrame, WalFormat, scan_frames_with};

pub use allocdb_wal_frame::{DecodeError, RecordType, ScanStopReason};

const FORMAT: WalFormat = WalFormat {
    magic: 0x5154_424c,
    checksum_start: 6,
};

#[cfg(test)]
const HEADER_LEN: usize = allocdb_wal_frame::HEADER_LEN;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Frame {
    pub lsn: Lsn,
    pub request_slot: Slot,
    pub record_type: RecordType,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScanResult {
    pub frames: Vec<Frame>,
    pub valid_up_to: usize,
    pub stop_reason: ScanStopReason,
}

impl Frame {
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        self.to_raw().encode_with(FORMAT)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        Ok(Self::from_raw(RawFrame::decode_with(bytes, FORMAT)?))
    }

    pub fn encoded_len(bytes: &[u8]) -> Result<usize, DecodeError> {
        RawFrame::encoded_len_with(bytes, FORMAT)
    }

    fn to_raw(&self) -> RawFrame {
        RawFrame {
            lsn: self.lsn.get(),
            request_slot: self.request_slot.get(),
            record_type: self.record_type,
            payload: self.payload.clone(),
        }
    }

    fn from_raw(frame: RawFrame) -> Self {
        Self {
            lsn: Lsn(frame.lsn),
            request_slot: Slot(frame.request_slot),
            record_type: frame.record_type,
            payload: frame.payload,
        }
    }
}

#[must_use]
pub fn scan_frames(bytes: &[u8]) -> ScanResult {
    let scanned = scan_frames_with(bytes, FORMAT);
    ScanResult {
        frames: scanned.frames.into_iter().map(Frame::from_raw).collect(),
        valid_up_to: scanned.valid_up_to,
        stop_reason: scanned.stop_reason,
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

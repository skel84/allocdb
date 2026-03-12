use crate::snapshot::SnapshotError;

pub(super) struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    pub(super) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    pub(super) fn is_at_end(&self) -> bool {
        self.offset == self.bytes.len()
    }

    fn read_exact<const N: usize>(&mut self) -> Result<[u8; N], SnapshotError> {
        if self.offset + N > self.bytes.len() {
            return Err(SnapshotError::BufferTooShort);
        }

        let mut array = [0_u8; N];
        array.copy_from_slice(&self.bytes[self.offset..self.offset + N]);
        self.offset += N;
        Ok(array)
    }

    pub(super) fn read_u8(&mut self) -> Result<u8, SnapshotError> {
        Ok(self.read_exact::<1>()?[0])
    }

    pub(super) fn read_u16(&mut self) -> Result<u16, SnapshotError> {
        Ok(u16::from_le_bytes(self.read_exact::<2>()?))
    }

    pub(super) fn read_u32(&mut self) -> Result<u32, SnapshotError> {
        Ok(u32::from_le_bytes(self.read_exact::<4>()?))
    }

    pub(super) fn read_u64(&mut self) -> Result<u64, SnapshotError> {
        Ok(u64::from_le_bytes(self.read_exact::<8>()?))
    }

    pub(super) fn read_u128(&mut self) -> Result<u128, SnapshotError> {
        Ok(u128::from_le_bytes(self.read_exact::<16>()?))
    }

    pub(super) fn read_optional_u64(&mut self) -> Result<Option<u64>, SnapshotError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_u64()?)),
            _ => Err(SnapshotError::InvalidLayout),
        }
    }

    pub(super) fn read_optional_u128(&mut self) -> Result<Option<u128>, SnapshotError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => Ok(Some(self.read_u128()?)),
            _ => Err(SnapshotError::InvalidLayout),
        }
    }
}

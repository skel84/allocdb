use crate::ids::{OperationId, ReservationId, ResourceId};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FixedMapError {
    Full,
    DuplicateKey,
}

pub(crate) trait FixedKey: Copy + Eq {
    fn hash64(self) -> u64;
}

macro_rules! impl_fixed_key {
    ($type:ty, $getter:ident) => {
        impl FixedKey for $type {
            fn hash64(self) -> u64 {
                hash_u128(self.$getter())
            }
        }
    };
}

impl_fixed_key!(ResourceId, get);
impl_fixed_key!(ReservationId, get);
impl_fixed_key!(OperationId, get);

#[derive(Clone, Copy, Debug)]
enum Bucket<K> {
    Empty,
    Occupied { key: K, slot: usize },
}

#[derive(Debug)]
struct SlotEntry<K, V> {
    key: K,
    value: V,
}

#[derive(Debug)]
pub(crate) struct FixedMap<K, V> {
    buckets: Vec<Bucket<K>>,
    slots: Vec<Option<SlotEntry<K, V>>>,
    free: Vec<usize>,
    len: usize,
}

impl<K: FixedKey, V> FixedMap<K, V> {
    #[must_use]
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        let bucket_target = capacity.checked_mul(2).unwrap_or(capacity).max(2);
        let bucket_count = bucket_target.next_power_of_two();

        let mut buckets = Vec::with_capacity(bucket_count);
        buckets.resize_with(bucket_count, || Bucket::Empty);

        let mut slots = Vec::with_capacity(capacity);
        slots.resize_with(capacity, || None);

        let mut free = Vec::with_capacity(capacity);
        for slot in (0..capacity).rev() {
            free.push(slot);
        }

        Self {
            buckets,
            slots,
            free,
            len: 0,
        }
    }

    #[must_use]
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    #[must_use]
    pub(crate) fn contains_key(&self, key: K) -> bool {
        self.find_bucket(key).is_some()
    }

    pub(crate) fn get(&self, key: K) -> Option<&V> {
        let slot = self.bucket_slot(key)?;
        Some(
            &self.slots[slot]
                .as_ref()
                .expect("occupied slot must exist")
                .value,
        )
    }

    pub(crate) fn get_mut(&mut self, key: K) -> Option<&mut V> {
        let slot = self.bucket_slot(key)?;
        Some(
            &mut self.slots[slot]
                .as_mut()
                .expect("occupied slot must exist")
                .value,
        )
    }

    pub(crate) fn insert(&mut self, key: K, value: V) -> Result<(), FixedMapError> {
        if self.len == self.slots.len() {
            return Err(FixedMapError::Full);
        }

        let bucket_index = self.find_insert_bucket(key)?;
        let slot = self
            .free
            .pop()
            .expect("free slot must exist while under capacity");
        self.slots[slot] = Some(SlotEntry { key, value });
        self.buckets[bucket_index] = Bucket::Occupied { key, slot };
        self.len += 1;
        Ok(())
    }

    pub(crate) fn remove(&mut self, key: K) -> Option<V> {
        let bucket_index = self.find_bucket(key)?;
        let slot = match self
            .buckets
            .get(bucket_index)
            .copied()
            .expect("bucket index must exist")
        {
            Bucket::Empty => unreachable!(),
            Bucket::Occupied {
                key: existing,
                slot,
            } => {
                debug_assert!(existing == key);
                slot
            }
        };

        let removed = self.slots[slot].take().expect("occupied slot must exist");
        debug_assert!(removed.key == key);
        self.free.push(slot);
        self.len -= 1;
        self.close_deletion_gap(bucket_index);
        Some(removed.value)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &V> {
        self.slots
            .iter()
            .filter_map(|entry| entry.as_ref().map(|entry| &entry.value))
    }

    fn bucket_slot(&self, key: K) -> Option<usize> {
        let bucket = self.find_bucket(key)?;
        match self
            .buckets
            .get(bucket)
            .copied()
            .expect("bucket index must exist")
        {
            Bucket::Empty => None,
            Bucket::Occupied { slot, .. } => Some(slot),
        }
    }

    fn find_bucket(&self, key: K) -> Option<usize> {
        let mut bucket = self.bucket_index(key);
        for _ in 0..self.buckets.len() {
            match self
                .buckets
                .get(bucket)
                .copied()
                .expect("bucket index must exist")
            {
                Bucket::Empty => return None,
                Bucket::Occupied { key: existing, .. } if existing == key => return Some(bucket),
                Bucket::Occupied { .. } => bucket = self.next_bucket(bucket),
            }
        }
        None
    }

    fn find_insert_bucket(&self, key: K) -> Result<usize, FixedMapError> {
        let mut bucket = self.bucket_index(key);
        for _ in 0..self.buckets.len() {
            match self
                .buckets
                .get(bucket)
                .copied()
                .expect("bucket index must exist")
            {
                Bucket::Empty => return Ok(bucket),
                Bucket::Occupied { key: existing, .. } if existing == key => {
                    return Err(FixedMapError::DuplicateKey);
                }
                Bucket::Occupied { .. } => bucket = self.next_bucket(bucket),
            }
        }
        Err(FixedMapError::Full)
    }

    fn close_deletion_gap(&mut self, removed_bucket: usize) {
        let mut gap = removed_bucket;
        let mut current = self.next_bucket(gap);

        loop {
            match self
                .buckets
                .get(current)
                .copied()
                .expect("bucket index must exist")
            {
                Bucket::Empty => {
                    self.buckets[gap] = Bucket::Empty;
                    break;
                }
                Bucket::Occupied { key, slot } => {
                    let ideal = self.bucket_index(key);
                    // Keep scanning until the cluster ends. A key sitting in its ideal bucket does
                    // not terminate the repair because later wrapped keys can still probe through
                    // the current gap.
                    if self.probe_distance(ideal, current) > self.probe_distance(ideal, gap) {
                        self.buckets[gap] = Bucket::Occupied { key, slot };
                        gap = current;
                    }
                    current = self.next_bucket(current);
                }
            }
        }
    }

    fn bucket_index(&self, key: K) -> usize {
        let mask = self.buckets.len() - 1;
        usize::try_from(key.hash64()).expect("u64 hash must fit usize") & mask
    }

    fn next_bucket(&self, bucket: usize) -> usize {
        (bucket + 1) & (self.buckets.len() - 1)
    }

    fn probe_distance(&self, ideal: usize, bucket: usize) -> usize {
        (bucket + self.buckets.len() - ideal) & (self.buckets.len() - 1)
    }
}

fn hash_u128(value: u128) -> u64 {
    let bytes = value.to_le_bytes();
    let lower = u64::from_le_bytes(bytes[..8].try_into().expect("slice has exact size"));
    let upper = u64::from_le_bytes(bytes[8..].try_into().expect("slice has exact size"));
    mix_u64(lower ^ upper.rotate_left(29))
}

fn mix_u64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::{FixedKey, FixedMap, FixedMapError};

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct TestKey(u64);

    impl FixedKey for TestKey {
        fn hash64(self) -> u64 {
            self.0
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct CollidingKey(u64);

    impl FixedKey for CollidingKey {
        fn hash64(self) -> u64 {
            1
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct OperationLikeKey(u128);

    impl FixedKey for OperationLikeKey {
        fn hash64(self) -> u64 {
            super::hash_u128(self.0)
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct PositionedKey {
        id: u64,
        hash: u64,
    }

    impl FixedKey for PositionedKey {
        fn hash64(self) -> u64 {
            self.hash
        }
    }

    #[test]
    fn insert_get_and_remove_round_trip() {
        let mut map = FixedMap::with_capacity(4);
        map.insert(TestKey(11), 101).unwrap();

        assert_eq!(map.get(TestKey(11)), Some(&101));
        assert_eq!(map.remove(TestKey(11)), Some(101));
        assert_eq!(map.get(TestKey(11)), None);
        assert_eq!(map.len(), 0);
    }

    #[test]
    fn duplicate_key_is_rejected() {
        let mut map = FixedMap::with_capacity(2);
        map.insert(TestKey(1), 10).unwrap();

        assert_eq!(map.insert(TestKey(1), 11), Err(FixedMapError::DuplicateKey));
    }

    #[test]
    fn full_capacity_fails_explicitly() {
        let mut map = FixedMap::with_capacity(2);
        map.insert(TestKey(1), 10).unwrap();
        map.insert(TestKey(2), 20).unwrap();

        assert_eq!(map.insert(TestKey(3), 30), Err(FixedMapError::Full));
    }

    #[test]
    fn deletion_preserves_probe_chains() {
        let mut map = FixedMap::with_capacity(4);
        map.insert(CollidingKey(1), 10).unwrap();
        map.insert(CollidingKey(2), 20).unwrap();
        map.insert(CollidingKey(3), 30).unwrap();

        assert_eq!(map.remove(CollidingKey(1)), Some(10));
        assert_eq!(map.get(CollidingKey(2)), Some(&20));
        assert_eq!(map.get(CollidingKey(3)), Some(&30));
    }

    #[test]
    fn repeated_removals_preserve_lookup_for_operation_like_hashes() {
        let mut map = FixedMap::with_capacity(32);
        for value in 1..=32_u128 {
            map.insert(OperationLikeKey(value), value).unwrap();
        }

        for removed in 1..=24_u128 {
            assert_eq!(map.remove(OperationLikeKey(removed)), Some(removed));
            for remaining in (removed + 1)..=32_u128 {
                assert_eq!(map.get(OperationLikeKey(remaining)), Some(&remaining));
            }
        }
    }

    #[test]
    fn deletion_preserves_wrapped_probe_chain_past_home_bucket() {
        let mut map = FixedMap::with_capacity(4);
        let removed = PositionedKey { id: 1, hash: 6 };
        let home = PositionedKey { id: 2, hash: 7 };
        let wrapped = PositionedKey { id: 3, hash: 6 };
        let displaced = PositionedKey { id: 4, hash: 0 };

        map.insert(removed, 10).unwrap();
        map.insert(home, 20).unwrap();
        map.insert(wrapped, 30).unwrap();
        map.insert(displaced, 40).unwrap();

        assert_eq!(map.remove(removed), Some(10));
        assert_eq!(map.get(home), Some(&20));
        assert_eq!(map.get(wrapped), Some(&30));
        assert_eq!(map.get(displaced), Some(&40));
    }
}

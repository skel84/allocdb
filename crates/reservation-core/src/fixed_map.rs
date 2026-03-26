use crate::ids::{HoldId, OperationId, PoolId};

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

impl_fixed_key!(PoolId, get);
impl_fixed_key!(HoldId, get);
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

    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    #[allow(dead_code)]
    #[must_use]
    pub(crate) fn contains_key(&self, key: K) -> bool {
        self.find_bucket(key).is_some()
    }

    #[allow(dead_code)]
    pub(crate) fn get(&self, key: K) -> Option<&V> {
        let slot = self.bucket_slot(key)?;
        Some(
            &self.slots[slot]
                .as_ref()
                .expect("occupied slot must exist")
                .value,
        )
    }

    #[allow(dead_code)]
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
        let bucket_index = self.find_insert_bucket(key)?;
        let slot = self.free.pop().ok_or(FixedMapError::Full)?;
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
                    let distance_current = self.probe_distance(ideal, current);
                    let distance_gap = self.probe_distance(ideal, gap);
                    if distance_current > distance_gap {
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
        bucket_index_from_hash(key.hash64(), mask)
    }

    fn next_bucket(&self, bucket: usize) -> usize {
        (bucket + 1) & (self.buckets.len() - 1)
    }

    fn probe_distance(&self, ideal: usize, actual: usize) -> usize {
        actual.wrapping_sub(ideal) & (self.buckets.len() - 1)
    }
}

fn hash_u128(value: u128) -> u64 {
    let bytes = value.to_le_bytes();
    let low = u64::from_le_bytes(bytes[..8].try_into().expect("slice has exact size"));
    let high = u64::from_le_bytes(bytes[8..].try_into().expect("slice has exact size"));
    splitmix64(low ^ high.rotate_left(32) ^ 0x9e37_79b9_7f4a_7c15)
}

fn splitmix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(target_pointer_width = "64")]
fn bucket_index_from_hash(hash: u64, mask: usize) -> usize {
    usize::try_from(hash).expect("u64 hash must fit usize on 64-bit targets") & mask
}

#[cfg(target_pointer_width = "32")]
fn bucket_index_from_hash(hash: u64, mask: usize) -> usize {
    usize::try_from(hash & u64::from(u32::MAX)).expect("masked 32-bit hash must fit usize") & mask
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use crate::ids::PoolId;

    use super::{FixedMap, FixedMapError};

    #[test]
    fn fixed_map_round_trips_insert_and_get() {
        let mut map = FixedMap::with_capacity(2);
        map.insert(PoolId(7), 11_u64).unwrap();

        assert_eq!(map.len(), 1);
        assert_eq!(map.get(PoolId(7)), Some(&11));
        assert_eq!(map.get(PoolId(8)), None);
    }

    #[test]
    fn fixed_map_rejects_duplicate_key() {
        let mut map = FixedMap::with_capacity(1);
        map.insert(PoolId(7), 11_u64).unwrap();

        assert_eq!(
            map.insert(PoolId(7), 12_u64),
            Err(FixedMapError::DuplicateKey)
        );
    }

    #[test]
    fn fixed_map_removes_entries() {
        let mut map = FixedMap::with_capacity(2);
        map.insert(PoolId(7), 11_u64).unwrap();
        map.insert(PoolId(9), 13_u64).unwrap();

        assert_eq!(map.remove(PoolId(7)), Some(11));
        assert_eq!(map.get(PoolId(7)), None);
        assert_eq!(map.get(PoolId(9)), Some(&13));
    }

    #[test]
    fn hash_spreads_small_sequential_ids_across_buckets() {
        let mut seen = BTreeSet::new();
        for id in 0_u128..16 {
            let mut map = FixedMap::<PoolId, u64>::with_capacity(8);
            map.insert(PoolId(id), 1).unwrap();
            let used_bucket = map
                .buckets
                .iter()
                .enumerate()
                .find_map(|(index, bucket)| match bucket {
                    super::Bucket::Occupied { .. } => Some(index),
                    super::Bucket::Empty => None,
                })
                .unwrap();
            seen.insert(used_bucket);
        }

        assert!(seen.len() > 4, "expected spread across several buckets");
    }
}

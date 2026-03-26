#![allow(clippy::missing_panics_doc)]

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetireEntry<K, S> {
    pub key: K,
    pub retire_after_slot: S,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetireQueueError {
    Full,
}

#[derive(Debug)]
pub struct RetireQueue<K, S> {
    entries: Vec<Option<RetireEntry<K, S>>>,
    head: usize,
    len: usize,
}

impl<K: Copy, S: Copy> RetireQueue<K, S> {
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        let mut entries = Vec::with_capacity(capacity.max(1));
        entries.resize_with(capacity.max(1), || None);
        Self {
            entries,
            head: 0,
            len: 0,
        }
    }

    /// # Errors
    ///
    /// Returns [`RetireQueueError::Full`] when the queue has no free slot for another entry.
    pub fn push(&mut self, entry: RetireEntry<K, S>) -> Result<(), RetireQueueError> {
        if self.len == self.entries.len() {
            return Err(RetireQueueError::Full);
        }

        let index = (self.head + self.len) % self.entries.len();
        self.entries[index] = Some(entry);
        self.len += 1;
        Ok(())
    }

    #[must_use]
    pub fn front(&self) -> Option<RetireEntry<K, S>> {
        if self.len == 0 {
            None
        } else {
            self.entries[self.head]
        }
    }

    pub fn pop_front(&mut self) -> Option<RetireEntry<K, S>> {
        if self.len == 0 {
            return None;
        }

        let entry = self.entries[self.head].take();
        self.head = (self.head + 1) % self.entries.len();
        self.len -= 1;
        entry
    }
}

#[cfg(test)]
mod tests {
    use super::{RetireEntry, RetireQueue, RetireQueueError};

    #[test]
    fn queue_round_trips_entries() {
        let mut queue = RetireQueue::with_capacity(2);
        queue
            .push(RetireEntry {
                key: 1_u64,
                retire_after_slot: 7_u64,
            })
            .unwrap();

        assert_eq!(
            queue.front(),
            Some(RetireEntry {
                key: 1,
                retire_after_slot: 7,
            })
        );
        assert_eq!(
            queue.pop_front(),
            Some(RetireEntry {
                key: 1,
                retire_after_slot: 7,
            })
        );
        assert_eq!(queue.pop_front(), None);
    }

    #[test]
    fn queue_wraps_without_allocation() {
        let mut queue = RetireQueue::with_capacity(2);
        queue
            .push(RetireEntry {
                key: 1_u64,
                retire_after_slot: 1_u64,
            })
            .unwrap();
        queue
            .push(RetireEntry {
                key: 2_u64,
                retire_after_slot: 2_u64,
            })
            .unwrap();
        assert_eq!(
            queue.push(RetireEntry {
                key: 3_u64,
                retire_after_slot: 3_u64,
            }),
            Err(RetireQueueError::Full)
        );

        assert_eq!(queue.pop_front().unwrap().key, 1);
        queue
            .push(RetireEntry {
                key: 3_u64,
                retire_after_slot: 3_u64,
            })
            .unwrap();

        assert_eq!(queue.pop_front().unwrap().key, 2);
        assert_eq!(queue.pop_front().unwrap().key, 3);
    }
}

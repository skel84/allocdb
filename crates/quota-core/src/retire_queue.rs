use crate::ids::Slot;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RetireEntry<K> {
    pub key: K,
    pub retire_after_slot: Slot,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RetireQueueError {
    Full,
}

#[derive(Debug)]
pub(crate) struct RetireQueue<K> {
    entries: Vec<Option<RetireEntry<K>>>,
    head: usize,
    len: usize,
}

impl<K: Copy> RetireQueue<K> {
    #[must_use]
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        let mut entries = Vec::with_capacity(capacity.max(1));
        entries.resize_with(capacity.max(1), || None);
        Self {
            entries,
            head: 0,
            len: 0,
        }
    }

    pub(crate) fn push(&mut self, entry: RetireEntry<K>) -> Result<(), RetireQueueError> {
        if self.len == self.entries.len() {
            return Err(RetireQueueError::Full);
        }

        let index = (self.head + self.len) % self.entries.len();
        self.entries[index] = Some(entry);
        self.len += 1;
        Ok(())
    }

    pub(crate) fn front(&self) -> Option<RetireEntry<K>> {
        if self.len == 0 {
            None
        } else {
            self.entries[self.head]
        }
    }

    pub(crate) fn pop_front(&mut self) -> Option<RetireEntry<K>> {
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
    use crate::ids::Slot;

    use super::{RetireEntry, RetireQueue, RetireQueueError};

    #[test]
    fn queue_round_trips_entries() {
        let mut queue = RetireQueue::with_capacity(2);
        queue
            .push(RetireEntry {
                key: 1_u64,
                retire_after_slot: Slot(7),
            })
            .unwrap();

        assert_eq!(
            queue.front(),
            Some(RetireEntry {
                key: 1,
                retire_after_slot: Slot(7)
            })
        );
        assert_eq!(
            queue.pop_front(),
            Some(RetireEntry {
                key: 1,
                retire_after_slot: Slot(7)
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
                retire_after_slot: Slot(1),
            })
            .unwrap();
        queue
            .push(RetireEntry {
                key: 2_u64,
                retire_after_slot: Slot(2),
            })
            .unwrap();
        assert_eq!(
            queue.push(RetireEntry {
                key: 3_u64,
                retire_after_slot: Slot(3),
            }),
            Err(RetireQueueError::Full)
        );

        assert_eq!(queue.pop_front().unwrap().key, 1);
        queue
            .push(RetireEntry {
                key: 3_u64,
                retire_after_slot: Slot(3),
            })
            .unwrap();

        assert_eq!(queue.pop_front().unwrap().key, 2);
        assert_eq!(queue.pop_front().unwrap().key, 3);
    }
}

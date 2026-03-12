#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BoundedQueueError {
    Full,
}

#[derive(Debug)]
pub(crate) struct BoundedQueue<T> {
    entries: Vec<Option<T>>,
    head: usize,
    len: usize,
}

impl<T> BoundedQueue<T> {
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

    #[must_use]
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    #[must_use]
    pub(crate) fn capacity(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &T> {
        (0..self.len).map(|offset| {
            let index = (self.head + offset) % self.entries.len();
            self.entries[index]
                .as_ref()
                .expect("live queue range must contain initialized entries")
        })
    }

    pub(crate) fn push(&mut self, entry: T) -> Result<(), BoundedQueueError> {
        if self.len == self.entries.len() {
            return Err(BoundedQueueError::Full);
        }

        let index = (self.head + self.len) % self.entries.len();
        self.entries[index] = Some(entry);
        self.len += 1;
        Ok(())
    }

    pub(crate) fn pop_front(&mut self) -> Option<T> {
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
    use super::{BoundedQueue, BoundedQueueError};

    #[test]
    fn queue_round_trips_entries() {
        let mut queue = BoundedQueue::with_capacity(2);
        queue.push(11_u64).unwrap();
        queue.push(12_u64).unwrap();

        assert_eq!(queue.len(), 2);
        assert_eq!(queue.pop_front(), Some(11));
        assert_eq!(queue.pop_front(), Some(12));
        assert_eq!(queue.pop_front(), None);
    }

    #[test]
    fn queue_wraps_without_allocation() {
        let mut queue = BoundedQueue::with_capacity(2);
        queue.push(1_u64).unwrap();
        queue.push(2_u64).unwrap();
        assert_eq!(queue.push(3_u64), Err(BoundedQueueError::Full));

        assert_eq!(queue.pop_front(), Some(1));
        queue.push(3_u64).unwrap();

        assert_eq!(queue.pop_front(), Some(2));
        assert_eq!(queue.pop_front(), Some(3));
    }
}

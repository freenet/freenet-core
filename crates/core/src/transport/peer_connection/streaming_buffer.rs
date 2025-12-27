//! Lock-free streaming buffer implementation.
//!
//! This module provides a high-performance, lock-free buffer for reassembling
//! incoming stream fragments. The design uses `OnceLock<Bytes>` for each fragment
//! slot, enabling concurrent fragment insertion without mutex contention.
//!
//! # Key Properties
//!
//! - **Lock-free writes**: Each slot uses atomic compare-and-swap via `OnceLock`
//! - **Zero-copy**: Uses `Bytes` for reference-counted data sharing
//! - **Idempotent inserts**: Duplicate fragments are automatically no-ops
//! - **Pre-allocated**: Buffer size determined by stream header's `total_size`
//!
//! # Performance
//!
//! Based on spike validation (issue #1452):
//! - Insert throughput: 2,235 MB/s (vs 23 MB/s with RwLock)
//! - First-fragment latency: 25μs (vs 103μs with RwLock)

use bytes::Bytes;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::OnceLock;

use crate::transport::packet_data;

/// Maximum payload size per fragment (excluding metadata overhead).
/// This matches the constant from peer_connection.rs.
pub(crate) const FRAGMENT_PAYLOAD_SIZE: usize = packet_data::MAX_DATA_SIZE - 40;

/// A lock-free buffer for reassembling stream fragments.
///
/// Uses `OnceLock<Bytes>` slots to enable concurrent, lock-free insertion.
/// Each fragment can only be written once (write-once semantics), and
/// concurrent writes to different slots never block each other.
pub struct LockFreeStreamBuffer {
    /// Pre-allocated slots for each fragment. Each slot can be written exactly once.
    slots: Box<[OnceLock<Bytes>]>,
    /// Total expected bytes for the complete stream.
    total_bytes: u64,
    /// Number of fragments that have been successfully inserted.
    /// Used to determine when the stream is complete.
    inserted_count: AtomicUsize,
    /// Highest contiguous fragment index from the start.
    /// Fragment indices are 1-indexed (first fragment is index 1).
    /// This tracks: "all fragments from 1 to this index are present"
    highest_contiguous: AtomicU32,
}

impl LockFreeStreamBuffer {
    /// Creates a new buffer pre-allocated for the expected total size.
    ///
    /// # Arguments
    ///
    /// * `total_bytes` - Total expected bytes in the complete stream
    ///
    /// # Returns
    ///
    /// A new `LockFreeStreamBuffer` with enough slots to hold all fragments.
    pub fn new(total_bytes: u64) -> Self {
        let num_fragments = Self::calculate_fragment_count(total_bytes);
        let slots: Vec<OnceLock<Bytes>> = (0..num_fragments).map(|_| OnceLock::new()).collect();

        Self {
            slots: slots.into_boxed_slice(),
            total_bytes,
            inserted_count: AtomicUsize::new(0),
            highest_contiguous: AtomicU32::new(0),
        }
    }

    /// Calculates the number of fragments needed for a given byte count.
    fn calculate_fragment_count(total_bytes: u64) -> usize {
        if total_bytes == 0 {
            return 0;
        }
        // Round up division
        (total_bytes as usize).div_ceil(FRAGMENT_PAYLOAD_SIZE)
    }

    /// Inserts a fragment into the buffer.
    ///
    /// # Arguments
    ///
    /// * `fragment_index` - 1-indexed fragment number (first fragment is 1)
    /// * `data` - The fragment payload
    ///
    /// # Returns
    ///
    /// * `Ok(true)` - Fragment was inserted successfully
    /// * `Ok(false)` - Fragment was already present (duplicate, no-op)
    /// * `Err(InsertError)` - Fragment index out of bounds
    pub fn insert(&self, fragment_index: u32, data: Bytes) -> Result<bool, InsertError> {
        if fragment_index == 0 {
            return Err(InsertError::InvalidIndex {
                index: fragment_index,
                max: self.slots.len() as u32,
            });
        }

        let slot_index = (fragment_index - 1) as usize;
        if slot_index >= self.slots.len() {
            return Err(InsertError::InvalidIndex {
                index: fragment_index,
                max: self.slots.len() as u32,
            });
        }

        // Try to set the slot. OnceLock ensures only the first write succeeds.
        let slot = &self.slots[slot_index];
        match slot.set(data) {
            Ok(()) => {
                // Successfully inserted
                self.inserted_count.fetch_add(1, Ordering::Release);
                self.update_highest_contiguous(fragment_index);
                Ok(true)
            }
            Err(_) => {
                // Slot was already filled (duplicate fragment)
                Ok(false)
            }
        }
    }

    /// Updates the highest contiguous fragment index.
    ///
    /// This is called after each successful insert to track progress.
    fn update_highest_contiguous(&self, inserted_index: u32) {
        // Only update if this fragment extends the contiguous region
        loop {
            let current = self.highest_contiguous.load(Ordering::Acquire);

            // If this fragment is not the next expected one, don't update
            if inserted_index != current + 1 {
                // But we might need to re-check if other fragments filled gaps
                // Try to extend from current position
                let mut new_highest = current;
                for idx in (current + 1)..=self.slots.len() as u32 {
                    let slot_idx = (idx - 1) as usize;
                    if self.slots[slot_idx].get().is_some() {
                        new_highest = idx;
                    } else {
                        break;
                    }
                }

                if new_highest > current {
                    // Try to update, but it's okay if another thread beat us
                    let _ = self.highest_contiguous.compare_exchange(
                        current,
                        new_highest,
                        Ordering::Release,
                        Ordering::Relaxed,
                    );
                }
                return;
            }

            // This fragment extends the contiguous region
            // Try to update and continue scanning for more contiguous fragments
            let mut new_highest = inserted_index;
            for idx in (inserted_index + 1)..=self.slots.len() as u32 {
                let slot_idx = (idx - 1) as usize;
                if self.slots[slot_idx].get().is_some() {
                    new_highest = idx;
                } else {
                    break;
                }
            }

            match self.highest_contiguous.compare_exchange(
                current,
                new_highest,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(_) => continue, // Another thread updated, retry
            }
        }
    }

    /// Returns true if all fragments have been received.
    pub fn is_complete(&self) -> bool {
        self.inserted_count.load(Ordering::Acquire) == self.slots.len()
    }

    /// Returns the number of fragments that have been inserted.
    pub fn inserted_count(&self) -> usize {
        self.inserted_count.load(Ordering::Acquire)
    }

    /// Returns the total number of expected fragments.
    pub fn total_fragments(&self) -> usize {
        self.slots.len()
    }

    /// Returns the total expected bytes.
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    /// Returns the highest contiguous fragment index (1-indexed).
    ///
    /// This indicates that all fragments from 1 to this index are present.
    /// Returns 0 if no fragments are present.
    pub fn highest_contiguous(&self) -> u32 {
        self.highest_contiguous.load(Ordering::Acquire)
    }

    /// Gets a reference to a fragment if it has been inserted.
    ///
    /// # Arguments
    ///
    /// * `fragment_index` - 1-indexed fragment number
    ///
    /// # Returns
    ///
    /// `Some(&Bytes)` if the fragment is present, `None` otherwise.
    pub fn get(&self, fragment_index: u32) -> Option<&Bytes> {
        if fragment_index == 0 {
            return None;
        }
        let slot_index = (fragment_index - 1) as usize;
        self.slots.get(slot_index)?.get()
    }

    /// Assembles all fragments into a single contiguous buffer.
    ///
    /// # Returns
    ///
    /// * `Some(Vec<u8>)` - Complete assembled data if all fragments are present
    /// * `None` - If any fragments are missing
    pub fn assemble(&self) -> Option<Vec<u8>> {
        if !self.is_complete() {
            return None;
        }

        let mut result = Vec::with_capacity(self.total_bytes as usize);
        for slot in self.slots.iter() {
            let data = slot.get()?;
            result.extend_from_slice(data);
        }

        // Truncate to exact size (last fragment may be padded or we calculated extra)
        result.truncate(self.total_bytes as usize);
        Some(result)
    }

    /// Returns an iterator over the fragments in order.
    ///
    /// Yields `None` for missing fragments and `Some(&Bytes)` for present ones.
    pub fn iter(&self) -> impl Iterator<Item = Option<&Bytes>> {
        self.slots.iter().map(|slot| slot.get())
    }

    /// Returns an iterator over contiguous fragments starting from index 1.
    ///
    /// Stops at the first missing fragment.
    pub fn contiguous_iter(&self) -> impl Iterator<Item = &Bytes> {
        self.slots.iter().map_while(|slot| slot.get())
    }
}

/// Error returned when a fragment insertion fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InsertError {
    /// Fragment index is out of bounds.
    InvalidIndex {
        /// The provided index (1-indexed)
        index: u32,
        /// Maximum valid index
        max: u32,
    },
}

impl std::fmt::Display for InsertError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InsertError::InvalidIndex { index, max } => {
                write!(
                    f,
                    "fragment index {} is out of bounds (max: {})",
                    index, max
                )
            }
        }
    }
}

impl std::error::Error for InsertError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_buffer_empty() {
        let buffer = LockFreeStreamBuffer::new(0);
        assert_eq!(buffer.total_fragments(), 0);
        assert!(buffer.is_complete()); // Empty buffer is complete
    }

    #[test]
    fn test_new_buffer_single_fragment() {
        let buffer = LockFreeStreamBuffer::new(100);
        assert_eq!(buffer.total_fragments(), 1);
        assert!(!buffer.is_complete());
    }

    #[test]
    fn test_new_buffer_multiple_fragments() {
        // FRAGMENT_PAYLOAD_SIZE bytes per fragment
        let total = (FRAGMENT_PAYLOAD_SIZE * 3) as u64;
        let buffer = LockFreeStreamBuffer::new(total);
        assert_eq!(buffer.total_fragments(), 3);
    }

    #[test]
    fn test_new_buffer_partial_last_fragment() {
        // 2.5 fragments worth of data
        let total = (FRAGMENT_PAYLOAD_SIZE * 2 + FRAGMENT_PAYLOAD_SIZE / 2) as u64;
        let buffer = LockFreeStreamBuffer::new(total);
        assert_eq!(buffer.total_fragments(), 3);
    }

    #[test]
    fn test_insert_single_fragment() {
        let buffer = LockFreeStreamBuffer::new(100);
        let data = Bytes::from_static(b"hello");

        let result = buffer.insert(1, data.clone());
        assert!(result.is_ok());
        assert!(result.unwrap()); // First insert succeeds

        assert_eq!(buffer.inserted_count(), 1);
        assert!(buffer.is_complete());
        assert_eq!(buffer.get(1), Some(&data));
    }

    #[test]
    fn test_insert_duplicate_is_noop() {
        let buffer = LockFreeStreamBuffer::new(100);
        let data1 = Bytes::from_static(b"first");
        let data2 = Bytes::from_static(b"second");

        assert!(buffer.insert(1, data1.clone()).unwrap());
        assert!(!buffer.insert(1, data2).unwrap()); // Duplicate returns false

        // Original data is preserved
        assert_eq!(buffer.get(1), Some(&data1));
    }

    #[test]
    fn test_insert_invalid_index_zero() {
        let buffer = LockFreeStreamBuffer::new(100);
        let data = Bytes::from_static(b"hello");

        let result = buffer.insert(0, data);
        assert!(matches!(result, Err(InsertError::InvalidIndex { .. })));
    }

    #[test]
    fn test_insert_invalid_index_too_large() {
        let buffer = LockFreeStreamBuffer::new(100);
        let data = Bytes::from_static(b"hello");

        let result = buffer.insert(2, data); // Only 1 fragment expected
        assert!(matches!(result, Err(InsertError::InvalidIndex { .. })));
    }

    #[test]
    fn test_insert_out_of_order() {
        let total = (FRAGMENT_PAYLOAD_SIZE * 3) as u64;
        let buffer = LockFreeStreamBuffer::new(total);

        let frag1 = Bytes::from(vec![1u8; FRAGMENT_PAYLOAD_SIZE]);
        let frag2 = Bytes::from(vec![2u8; FRAGMENT_PAYLOAD_SIZE]);
        let frag3 = Bytes::from(vec![3u8; FRAGMENT_PAYLOAD_SIZE]);

        // Insert out of order
        assert!(buffer.insert(3, frag3.clone()).unwrap());
        assert!(!buffer.is_complete());
        assert_eq!(buffer.highest_contiguous(), 0);

        assert!(buffer.insert(1, frag1.clone()).unwrap());
        assert!(!buffer.is_complete());
        assert_eq!(buffer.highest_contiguous(), 1);

        assert!(buffer.insert(2, frag2.clone()).unwrap());
        assert!(buffer.is_complete());
        assert_eq!(buffer.highest_contiguous(), 3);
    }

    #[test]
    fn test_assemble_complete() {
        let buffer = LockFreeStreamBuffer::new(6);
        buffer.insert(1, Bytes::from_static(b"hello ")).unwrap();

        let assembled = buffer.assemble();
        assert!(assembled.is_some());
        assert_eq!(assembled.unwrap(), b"hello ");
    }

    #[test]
    fn test_assemble_incomplete() {
        let total = (FRAGMENT_PAYLOAD_SIZE * 2) as u64;
        let buffer = LockFreeStreamBuffer::new(total);
        buffer
            .insert(1, Bytes::from(vec![1u8; FRAGMENT_PAYLOAD_SIZE]))
            .unwrap();

        assert!(buffer.assemble().is_none());
    }

    #[test]
    fn test_assemble_truncates_to_total_bytes() {
        // Total is less than one full fragment
        let buffer = LockFreeStreamBuffer::new(10);
        buffer
            .insert(1, Bytes::from_static(b"hello world plus extra"))
            .unwrap();

        let assembled = buffer.assemble().unwrap();
        assert_eq!(assembled.len(), 10);
        assert_eq!(assembled, b"hello worl");
    }

    #[test]
    fn test_contiguous_iter() {
        let total = (FRAGMENT_PAYLOAD_SIZE * 4) as u64;
        let buffer = LockFreeStreamBuffer::new(total);

        buffer
            .insert(1, Bytes::from(vec![1u8; FRAGMENT_PAYLOAD_SIZE]))
            .unwrap();
        buffer
            .insert(2, Bytes::from(vec![2u8; FRAGMENT_PAYLOAD_SIZE]))
            .unwrap();
        // Skip fragment 3
        buffer
            .insert(4, Bytes::from(vec![4u8; FRAGMENT_PAYLOAD_SIZE]))
            .unwrap();

        let contiguous: Vec<_> = buffer.contiguous_iter().collect();
        assert_eq!(contiguous.len(), 2); // Only first 2 are contiguous
    }

    #[test]
    fn test_concurrent_inserts() {
        use std::sync::Arc;
        use std::thread;

        let total = (FRAGMENT_PAYLOAD_SIZE * 100) as u64;
        let buffer = Arc::new(LockFreeStreamBuffer::new(total));

        let handles: Vec<_> = (1..=100)
            .map(|i| {
                let buffer = Arc::clone(&buffer);
                thread::spawn(move || {
                    let data = Bytes::from(vec![i as u8; FRAGMENT_PAYLOAD_SIZE]);
                    buffer.insert(i as u32, data)
                })
            })
            .collect();

        for handle in handles {
            assert!(handle.join().unwrap().unwrap());
        }

        assert!(buffer.is_complete());
        assert_eq!(buffer.inserted_count(), 100);
    }

    #[test]
    fn test_concurrent_duplicate_inserts() {
        use std::sync::Arc;
        use std::thread;

        let buffer = Arc::new(LockFreeStreamBuffer::new(100));

        // 10 threads all trying to insert the same fragment
        let handles: Vec<_> = (0..10)
            .map(|i| {
                let buffer = Arc::clone(&buffer);
                thread::spawn(move || {
                    let data = Bytes::from(vec![i as u8; 50]);
                    buffer.insert(1, data)
                })
            })
            .collect();

        let results: Vec<_> = handles
            .into_iter()
            .map(|h| h.join().unwrap().unwrap())
            .collect();

        // Exactly one should succeed
        let success_count = results.iter().filter(|&&r| r).count();
        assert_eq!(success_count, 1);
        assert_eq!(buffer.inserted_count(), 1);
    }
}

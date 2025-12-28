//! Lock-free streaming buffer implementation.
//!
//! This module provides a high-performance, lock-free buffer for reassembling
//! incoming stream fragments. The design uses `AtomicPtr<Bytes>` for each fragment
//! slot, enabling concurrent fragment insertion without mutex contention and
//! progressive memory reclamation as fragments are consumed.
//!
//! # Key Properties
//!
//! - **Lock-free writes**: Each slot uses atomic compare-and-swap
//! - **Zero-copy**: Uses `Bytes` for reference-counted data sharing
//! - **Idempotent inserts**: Duplicate fragments are automatically no-ops
//! - **Pre-allocated**: Buffer size determined by stream header's `total_size`
//! - **Progressive reclamation**: Consumed fragments can be freed immediately
//!
//! # Performance
//!
//! Based on spike validation (issue #1452, PR iduartgomez/freenet-core#204):
//! - Insert throughput: 2,235 MB/s (vs 23 MB/s with RwLock)
//! - First-fragment latency: 25μs (vs 103μs with RwLock)
//! - 96× speedup over RwLock-based approach

use bytes::Bytes;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicU32, Ordering};
use tokio::sync::Notify;

use crate::transport::packet_data;

/// Maximum payload size per fragment (excluding metadata overhead).
/// This matches the constant from peer_connection.rs.
/// Public when bench feature is enabled for benchmarking.
#[cfg(feature = "bench")]
pub const FRAGMENT_PAYLOAD_SIZE: usize = packet_data::MAX_DATA_SIZE - 40;
#[cfg(not(feature = "bench"))]
pub(crate) const FRAGMENT_PAYLOAD_SIZE: usize = packet_data::MAX_DATA_SIZE - 40;

/// A lock-free buffer for reassembling stream fragments.
///
/// Uses `AtomicPtr<Bytes>` slots to enable concurrent, lock-free insertion
/// and progressive memory reclamation. Each fragment can only be written once
/// (write-once semantics), and concurrent writes to different slots never
/// block each other.
///
/// # Memory Reclamation
///
/// Unlike `OnceLock`, `AtomicPtr` allows clearing slots after consumption.
/// Call `mark_consumed(index)` to free memory for fragments that have been
/// processed. This is critical for large streams where holding all fragments
/// until completion would waste memory.
///
/// Based on spike implementation from iduartgomez/freenet-core#204.
pub struct LockFreeStreamBuffer {
    /// Pre-allocated slots for each fragment. Each slot is a pointer to heap-allocated Bytes.
    /// Fragment indices are 1-indexed, so fragment N is stored at slots[N-1].
    /// NULL means empty/cleared, non-NULL means fragment is present.
    fragments: Box<[AtomicPtr<Bytes>]>,
    /// Total expected bytes for the complete stream.
    total_size: u64,
    /// Total number of fragments expected.
    total_fragments: u32,
    /// Highest contiguous fragment index from the start (0 = none received).
    /// Uses atomic CAS for lock-free frontier advancement.
    contiguous_fragments: AtomicU32,
    /// Highest fragment index that has been consumed/freed (0 = none consumed).
    /// Fragments up to this index have been cleared from memory.
    consumed_frontier: AtomicU32,
    /// Notification for when new fragments arrive.
    /// Allows async consumers to wait efficiently.
    data_available: Notify,
}

impl LockFreeStreamBuffer {
    /// Creates a new buffer pre-allocated for the expected total size.
    ///
    /// # Arguments
    ///
    /// * `total_size` - Total expected bytes in the complete stream
    ///
    /// # Returns
    ///
    /// A new `LockFreeStreamBuffer` with enough slots to hold all fragments.
    pub fn new(total_size: u64) -> Self {
        let num_fragments = Self::calculate_fragment_count(total_size);
        let fragments: Vec<AtomicPtr<Bytes>> = (0..num_fragments)
            .map(|_| AtomicPtr::new(ptr::null_mut()))
            .collect();

        Self {
            fragments: fragments.into_boxed_slice(),
            total_size,
            total_fragments: num_fragments as u32,
            contiguous_fragments: AtomicU32::new(0),
            consumed_frontier: AtomicU32::new(0),
            data_available: Notify::new(),
        }
    }

    /// Calculates the number of fragments needed for a given byte count.
    fn calculate_fragment_count(total_size: u64) -> usize {
        if total_size == 0 {
            return 0;
        }
        // Round up division
        (total_size as usize).div_ceil(FRAGMENT_PAYLOAD_SIZE)
    }

    /// Inserts a fragment into the buffer.
    ///
    /// This is the core lock-free operation. Uses atomic CAS to insert
    /// the fragment - duplicates are idempotent no-ops.
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
    /// * `Err(InsertError)` - Fragment index out of bounds or already consumed
    pub fn insert(&self, fragment_index: u32, data: Bytes) -> Result<bool, InsertError> {
        if fragment_index == 0 || fragment_index > self.total_fragments {
            return Err(InsertError::InvalidIndex {
                index: fragment_index,
                max: self.total_fragments,
            });
        }

        // Check if this fragment has already been consumed
        if fragment_index <= self.consumed_frontier.load(Ordering::Acquire) {
            return Err(InsertError::AlreadyConsumed {
                index: fragment_index,
            });
        }

        let idx = (fragment_index - 1) as usize;

        // Allocate the Bytes on the heap
        let boxed = Box::new(data);
        let new_ptr = Box::into_raw(boxed);

        // Atomic CAS: only insert if slot is currently NULL
        match self.fragments[idx].compare_exchange(
            ptr::null_mut(),
            new_ptr,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                // Successfully inserted
                self.advance_frontier();
                self.data_available.notify_waiters();
                Ok(true)
            }
            Err(_) => {
                // Slot was already occupied - drop our allocation
                // SAFETY: new_ptr was just created by Box::into_raw and not shared
                unsafe {
                    drop(Box::from_raw(new_ptr));
                }
                Ok(false) // Duplicate, no-op
            }
        }
    }

    /// Advances the contiguous fragment frontier using lock-free CAS.
    ///
    /// This is called after each successful insert to track how many
    /// contiguous fragments are available from the start.
    ///
    /// # CAS Loop Behavior
    ///
    /// The loop uses `compare_exchange_weak` for lock-free advancement:
    /// - On success: the frontier advances and we try to advance more (there may be
    ///   additional contiguous fragments already inserted out of order)
    /// - On failure: another thread already advanced, so we reload the new value and retry
    ///
    /// ## Contention Analysis
    ///
    /// Under high contention (multiple threads inserting fragments that complete gaps),
    /// multiple threads may compete to advance the frontier. This is bounded:
    /// - Maximum loop iterations = number of total fragments (each fragment can only be inserted once)
    /// - Each failed CAS still represents forward progress by some thread
    /// - The work is split among all threads - no "CAS storms" where threads just spin
    ///
    /// This design was chosen over alternatives like `fetch_max` because we need to:
    /// 1. Check that the next fragment actually exists before advancing
    /// 2. Advance by exactly 1, not to an arbitrary higher value
    fn advance_frontier(&self) {
        loop {
            let current = self.contiguous_fragments.load(Ordering::Acquire);
            let next = current + 1;

            // Check if next fragment exists
            if next > self.total_fragments {
                return; // All fragments received
            }

            let idx = (next - 1) as usize;
            // Check if slot has a valid pointer (not NULL)
            if self.fragments[idx].load(Ordering::Acquire).is_null() {
                return; // Gap - can't advance further
            }

            // Try to advance frontier with CAS
            match self.contiguous_fragments.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => continue,  // Successfully advanced, try to advance more
                Err(_) => continue, // Another thread advanced, retry from new position
            }
        }
    }

    /// Returns true if all fragments have been received.
    pub fn is_complete(&self) -> bool {
        self.contiguous_fragments.load(Ordering::Acquire) == self.total_fragments
    }

    /// Returns the number of fragments that are currently present (not consumed).
    pub fn inserted_count(&self) -> usize {
        // Count non-NULL slots
        self.fragments
            .iter()
            .filter(|s| !s.load(Ordering::Acquire).is_null())
            .count()
    }

    /// Returns the total number of expected fragments.
    pub fn total_fragments(&self) -> usize {
        self.total_fragments as usize
    }

    /// Returns the total expected bytes.
    pub fn total_bytes(&self) -> u64 {
        self.total_size
    }

    /// Returns the highest contiguous fragment index (1-indexed).
    ///
    /// This indicates that all fragments from 1 to this index are present.
    /// Returns 0 if no fragments are present.
    pub fn highest_contiguous(&self) -> u32 {
        self.contiguous_fragments.load(Ordering::Acquire)
    }

    /// Returns the consumed frontier (1-indexed).
    ///
    /// Fragments up to and including this index have been freed.
    /// Returns 0 if no fragments have been consumed.
    pub fn consumed_frontier(&self) -> u32 {
        self.consumed_frontier.load(Ordering::Acquire)
    }

    /// Gets a reference to a fragment if it is present and not consumed.
    ///
    /// # Arguments
    ///
    /// * `fragment_index` - 1-indexed fragment number
    ///
    /// # Returns
    ///
    /// `Some(&Bytes)` if the fragment is present, `None` otherwise.
    ///
    /// # Safety
    ///
    /// The returned reference is valid as long as `mark_consumed` is not called
    /// for this index. Callers must ensure they don't hold references across
    /// consumption boundaries.
    pub fn get(&self, fragment_index: u32) -> Option<&Bytes> {
        if fragment_index == 0 || fragment_index > self.total_fragments {
            return None;
        }
        let idx = (fragment_index - 1) as usize;
        let ptr = self.fragments[idx].load(Ordering::Acquire);
        if ptr.is_null() {
            return None;
        }
        // SAFETY: If ptr is non-null, it was set by insert() and points to valid Bytes.
        // The reference is valid until mark_consumed is called for this index.
        Some(unsafe { &*ptr })
    }

    /// Takes a fragment, returning ownership and clearing the slot.
    ///
    /// This is useful for single-consumer scenarios where the consumer
    /// wants to own the data rather than clone it.
    ///
    /// # Arguments
    ///
    /// * `fragment_index` - 1-indexed fragment number
    ///
    /// # Returns
    ///
    /// `Some(Bytes)` if the fragment was present, `None` if already taken or not inserted.
    pub fn take(&self, fragment_index: u32) -> Option<Bytes> {
        if fragment_index == 0 || fragment_index > self.total_fragments {
            return None;
        }
        let idx = (fragment_index - 1) as usize;

        // Atomically swap the pointer to NULL
        let ptr = self.fragments[idx].swap(ptr::null_mut(), Ordering::AcqRel);
        if ptr.is_null() {
            return None;
        }

        // SAFETY: ptr was set by insert() and we just took exclusive ownership via swap
        let boxed = unsafe { Box::from_raw(ptr) };
        Some(*boxed)
    }

    /// Marks fragments up to and including the given index as consumed, freeing their memory.
    ///
    /// This enables progressive memory reclamation for streaming consumers.
    /// After calling this, fragments 1..=up_to_index will return `None` from `get()`.
    ///
    /// # Arguments
    ///
    /// * `up_to_index` - 1-indexed fragment number (inclusive)
    ///
    /// # Returns
    ///
    /// The number of fragments actually freed (may be less if some were already consumed).
    ///
    /// # Panics
    ///
    /// Panics if `up_to_index` is greater than `total_fragments`.
    pub fn mark_consumed(&self, up_to_index: u32) -> usize {
        assert!(
            up_to_index <= self.total_fragments,
            "mark_consumed index {} exceeds total_fragments {}",
            up_to_index,
            self.total_fragments
        );

        let mut freed_count = 0;
        let current_consumed = self.consumed_frontier.load(Ordering::Acquire);

        // Only process fragments we haven't already consumed
        for idx in current_consumed..up_to_index {
            let slot_idx = idx as usize;
            let ptr = self.fragments[slot_idx].swap(ptr::null_mut(), Ordering::AcqRel);
            if !ptr.is_null() {
                // SAFETY: ptr was set by insert() and we just took exclusive ownership via swap
                unsafe {
                    drop(Box::from_raw(ptr));
                }
                freed_count += 1;
            }
        }

        // Update the consumed frontier (use fetch_max for thread safety)
        self.consumed_frontier
            .fetch_max(up_to_index, Ordering::AcqRel);

        freed_count
    }

    /// Returns a reference to the notification handle.
    ///
    /// Use this to wait for new fragments to arrive.
    pub fn notifier(&self) -> &Notify {
        &self.data_available
    }

    /// Assembles all fragments into a single contiguous buffer.
    ///
    /// # Returns
    ///
    /// * `Some(Vec<u8>)` - Complete assembled data if all fragments are present
    /// * `None` - If any fragments are missing (including consumed ones)
    pub fn assemble(&self) -> Option<Vec<u8>> {
        if !self.is_complete() {
            return None;
        }

        // Check that no fragments have been consumed
        if self.consumed_frontier.load(Ordering::Acquire) > 0 {
            return None; // Can't assemble if fragments were consumed
        }

        let mut result = Vec::with_capacity(self.total_size as usize);
        for slot in self.fragments.iter() {
            let ptr = slot.load(Ordering::Acquire);
            if ptr.is_null() {
                return None;
            }
            // SAFETY: ptr is non-null and was set by insert()
            let data = unsafe { &*ptr };
            result.extend_from_slice(data);
        }

        // Truncate to exact size (last fragment may be smaller)
        result.truncate(self.total_size as usize);
        Some(result)
    }

    /// Returns an iterator over the fragments in order.
    ///
    /// Yields `None` for missing/consumed fragments and `Some(&Bytes)` for present ones.
    pub fn iter(&self) -> impl Iterator<Item = Option<&Bytes>> {
        self.fragments.iter().map(|slot| {
            let ptr = slot.load(Ordering::Acquire);
            if ptr.is_null() {
                None
            } else {
                // SAFETY: ptr is non-null and was set by insert()
                Some(unsafe { &*ptr })
            }
        })
    }

    /// Returns an iterator over contiguous fragments starting from index 1.
    ///
    /// Stops at the first missing fragment. This is the key method for
    /// streaming consumption - it yields only fragments that can be
    /// processed in order.
    pub fn iter_contiguous(&self) -> impl Iterator<Item = &Bytes> {
        self.fragments.iter().map_while(|slot| {
            let ptr = slot.load(Ordering::Acquire);
            if ptr.is_null() {
                None
            } else {
                // SAFETY: ptr is non-null and was set by insert()
                Some(unsafe { &*ptr })
            }
        })
    }

    /// Collects all contiguous fragments into a single buffer.
    ///
    /// Returns bytes that can be processed immediately without waiting
    /// for out-of-order fragments. Pre-allocates based on total_size
    /// to avoid reallocations.
    #[allow(dead_code)]
    pub fn collect_contiguous(&self) -> Vec<u8> {
        let mut result = Vec::with_capacity(self.total_size as usize);
        for data in self.iter_contiguous() {
            result.extend_from_slice(data);
        }
        result
    }
}

/// Ensures all remaining fragments are properly deallocated when the buffer is dropped.
impl Drop for LockFreeStreamBuffer {
    fn drop(&mut self) {
        for slot in self.fragments.iter() {
            let ptr = slot.load(Ordering::Acquire);
            if !ptr.is_null() {
                // SAFETY: We have exclusive access during drop, ptr was set by insert()
                unsafe {
                    drop(Box::from_raw(ptr));
                }
            }
        }
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
    /// Fragment has already been consumed and cannot be re-inserted.
    AlreadyConsumed {
        /// The fragment index that was already consumed
        index: u32,
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
            InsertError::AlreadyConsumed { index } => {
                write!(f, "fragment index {} has already been consumed", index)
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

        let contiguous: Vec<_> = buffer.iter_contiguous().collect();
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

    #[test]
    fn test_single_byte_stream() {
        let buffer = LockFreeStreamBuffer::new(1);
        assert_eq!(buffer.total_fragments(), 1);
        assert_eq!(buffer.total_bytes(), 1);

        buffer.insert(1, Bytes::from_static(b"X")).unwrap();
        assert!(buffer.is_complete());

        let assembled = buffer.assemble().unwrap();
        assert_eq!(assembled, b"X");
    }

    #[test]
    fn test_exact_fragment_boundary() {
        // Total size is exactly 2 fragments
        let total = (FRAGMENT_PAYLOAD_SIZE * 2) as u64;
        let buffer = LockFreeStreamBuffer::new(total);
        assert_eq!(buffer.total_fragments(), 2);

        buffer
            .insert(1, Bytes::from(vec![1u8; FRAGMENT_PAYLOAD_SIZE]))
            .unwrap();
        buffer
            .insert(2, Bytes::from(vec![2u8; FRAGMENT_PAYLOAD_SIZE]))
            .unwrap();

        let assembled = buffer.assemble().unwrap();
        assert_eq!(assembled.len(), total as usize);
    }

    #[test]
    fn test_iter_returns_all_fragments() {
        let total = (FRAGMENT_PAYLOAD_SIZE * 3) as u64;
        let buffer = LockFreeStreamBuffer::new(total);

        // Insert all fragments
        for i in 1..=3 {
            buffer
                .insert(i, Bytes::from(vec![i as u8; FRAGMENT_PAYLOAD_SIZE]))
                .unwrap();
        }

        let fragments: Vec<_> = buffer.iter().collect();
        assert_eq!(fragments.len(), 3);
        assert!(fragments.iter().all(|f| f.is_some()));
    }

    #[test]
    fn test_iter_with_gaps() {
        let total = (FRAGMENT_PAYLOAD_SIZE * 3) as u64;
        let buffer = LockFreeStreamBuffer::new(total);

        // Only insert first and third
        buffer
            .insert(1, Bytes::from(vec![1u8; FRAGMENT_PAYLOAD_SIZE]))
            .unwrap();
        buffer
            .insert(3, Bytes::from(vec![3u8; FRAGMENT_PAYLOAD_SIZE]))
            .unwrap();

        let fragments: Vec<_> = buffer.iter().collect();
        assert_eq!(fragments.len(), 3);
        assert!(fragments[0].is_some());
        assert!(fragments[1].is_none()); // Gap
        assert!(fragments[2].is_some());
    }

    #[test]
    fn test_get_missing_fragment_returns_none() {
        let total = (FRAGMENT_PAYLOAD_SIZE * 2) as u64;
        let buffer = LockFreeStreamBuffer::new(total);

        // Insert only first fragment
        buffer
            .insert(1, Bytes::from(vec![1u8; FRAGMENT_PAYLOAD_SIZE]))
            .unwrap();

        assert!(buffer.get(1).is_some());
        assert!(buffer.get(2).is_none()); // Valid index but not inserted
    }

    #[test]
    fn test_get_out_of_bounds_returns_none() {
        let buffer = LockFreeStreamBuffer::new(100);

        assert!(buffer.get(0).is_none()); // Invalid
        assert!(buffer.get(2).is_none()); // Out of bounds
        assert!(buffer.get(100).is_none()); // Way out of bounds
    }

    #[test]
    fn test_highest_contiguous_advances_after_gap_fill() {
        let total = (FRAGMENT_PAYLOAD_SIZE * 4) as u64;
        let buffer = LockFreeStreamBuffer::new(total);

        // Insert fragments 1, 3, 4 (gap at 2)
        buffer
            .insert(1, Bytes::from(vec![1u8; FRAGMENT_PAYLOAD_SIZE]))
            .unwrap();
        buffer
            .insert(3, Bytes::from(vec![3u8; FRAGMENT_PAYLOAD_SIZE]))
            .unwrap();
        buffer
            .insert(4, Bytes::from(vec![4u8; FRAGMENT_PAYLOAD_SIZE]))
            .unwrap();

        assert_eq!(buffer.highest_contiguous(), 1); // Only 1 is contiguous

        // Fill the gap
        buffer
            .insert(2, Bytes::from(vec![2u8; FRAGMENT_PAYLOAD_SIZE]))
            .unwrap();

        // Now all should be contiguous
        assert_eq!(buffer.highest_contiguous(), 4);
        assert!(buffer.is_complete());
    }

    #[test]
    fn test_collect_contiguous_with_gap() {
        let total = (FRAGMENT_PAYLOAD_SIZE * 3) as u64;
        let buffer = LockFreeStreamBuffer::new(total);

        buffer
            .insert(1, Bytes::from(vec![1u8; FRAGMENT_PAYLOAD_SIZE]))
            .unwrap();
        buffer
            .insert(3, Bytes::from(vec![3u8; FRAGMENT_PAYLOAD_SIZE]))
            .unwrap();
        // Gap at fragment 2

        let contiguous = buffer.collect_contiguous();
        assert_eq!(contiguous.len(), FRAGMENT_PAYLOAD_SIZE);
        assert!(contiguous.iter().all(|&b| b == 1));
    }

    #[tokio::test]
    async fn test_notifier_is_called_on_insert() {
        use std::sync::Arc;
        use tokio::sync::Barrier;

        let buffer = Arc::new(LockFreeStreamBuffer::new(100));
        let buffer_clone = Arc::clone(&buffer);
        let barrier = Arc::new(Barrier::new(2));
        let barrier_clone = Arc::clone(&barrier);

        // Spawn a task that waits for notification
        let waiter = tokio::spawn(async move {
            barrier_clone.wait().await;
            buffer_clone.notifier().notified().await;
            true
        });

        // Sync with waiter
        barrier.wait().await;
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Insert should trigger notification
        buffer.insert(1, Bytes::from_static(b"data")).unwrap();

        let result = tokio::time::timeout(tokio::time::Duration::from_millis(100), waiter).await;
        assert!(result.is_ok());
        assert!(result.unwrap().unwrap());
    }

    // ==================== Progressive Reclamation Tests ====================

    #[test]
    fn test_mark_consumed_basic() {
        let total = (FRAGMENT_PAYLOAD_SIZE * 3) as u64;
        let buffer = LockFreeStreamBuffer::new(total);

        // Insert all fragments
        for i in 1..=3 {
            buffer
                .insert(i, Bytes::from(vec![i as u8; FRAGMENT_PAYLOAD_SIZE]))
                .unwrap();
        }

        assert_eq!(buffer.inserted_count(), 3);
        assert_eq!(buffer.consumed_frontier(), 0);

        // Consume first fragment
        let freed = buffer.mark_consumed(1);
        assert_eq!(freed, 1);
        assert_eq!(buffer.consumed_frontier(), 1);
        assert_eq!(buffer.inserted_count(), 2); // Only 2 remain
        assert!(buffer.get(1).is_none()); // Consumed
        assert!(buffer.get(2).is_some()); // Still there

        // Consume remaining fragments
        let freed = buffer.mark_consumed(3);
        assert_eq!(freed, 2);
        assert_eq!(buffer.consumed_frontier(), 3);
        assert_eq!(buffer.inserted_count(), 0);
    }

    #[test]
    fn test_mark_consumed_idempotent() {
        let buffer = LockFreeStreamBuffer::new(100);
        buffer.insert(1, Bytes::from_static(b"hello")).unwrap();

        // First call frees the fragment
        let freed1 = buffer.mark_consumed(1);
        assert_eq!(freed1, 1);

        // Second call is a no-op (already consumed)
        let freed2 = buffer.mark_consumed(1);
        assert_eq!(freed2, 0);

        assert_eq!(buffer.consumed_frontier(), 1);
    }

    #[test]
    fn test_insert_after_consumed_fails() {
        let total = (FRAGMENT_PAYLOAD_SIZE * 2) as u64;
        let buffer = LockFreeStreamBuffer::new(total);

        // Insert and consume fragment 1
        buffer
            .insert(1, Bytes::from(vec![1u8; FRAGMENT_PAYLOAD_SIZE]))
            .unwrap();
        buffer.mark_consumed(1);

        // Try to re-insert fragment 1 - should fail
        let result = buffer.insert(1, Bytes::from(vec![1u8; FRAGMENT_PAYLOAD_SIZE]));
        assert!(matches!(
            result,
            Err(InsertError::AlreadyConsumed { index: 1 })
        ));

        // Fragment 2 should still be insertable
        let result = buffer.insert(2, Bytes::from(vec![2u8; FRAGMENT_PAYLOAD_SIZE]));
        assert!(result.is_ok());
    }

    #[test]
    fn test_take_fragment() {
        let buffer = LockFreeStreamBuffer::new(100);
        let data = Bytes::from_static(b"hello");
        buffer.insert(1, data.clone()).unwrap();

        // Take the fragment
        let taken = buffer.take(1);
        assert!(taken.is_some());
        assert_eq!(taken.unwrap(), data);

        // Slot is now empty
        assert!(buffer.get(1).is_none());
        assert_eq!(buffer.inserted_count(), 0);

        // Second take returns None
        assert!(buffer.take(1).is_none());
    }

    #[test]
    fn test_take_invalid_indices() {
        let buffer = LockFreeStreamBuffer::new(100);

        assert!(buffer.take(0).is_none()); // Invalid index
        assert!(buffer.take(2).is_none()); // Out of bounds
    }

    #[test]
    fn test_assemble_fails_after_consumption() {
        let total = (FRAGMENT_PAYLOAD_SIZE * 2) as u64;
        let buffer = LockFreeStreamBuffer::new(total);

        // Insert all fragments
        buffer
            .insert(1, Bytes::from(vec![1u8; FRAGMENT_PAYLOAD_SIZE]))
            .unwrap();
        buffer
            .insert(2, Bytes::from(vec![2u8; FRAGMENT_PAYLOAD_SIZE]))
            .unwrap();

        // Assembly works before consumption
        assert!(buffer.assemble().is_some());

        // Consume first fragment
        buffer.mark_consumed(1);

        // Assembly fails after consumption
        assert!(buffer.assemble().is_none());
    }

    #[test]
    fn test_iter_shows_consumed_as_none() {
        let total = (FRAGMENT_PAYLOAD_SIZE * 3) as u64;
        let buffer = LockFreeStreamBuffer::new(total);

        for i in 1..=3 {
            buffer
                .insert(i, Bytes::from(vec![i as u8; FRAGMENT_PAYLOAD_SIZE]))
                .unwrap();
        }

        // Consume first two
        buffer.mark_consumed(2);

        let fragments: Vec<_> = buffer.iter().collect();
        assert!(fragments[0].is_none()); // Consumed
        assert!(fragments[1].is_none()); // Consumed
        assert!(fragments[2].is_some()); // Still present
    }

    #[test]
    fn test_iter_contiguous_stops_at_consumed() {
        let total = (FRAGMENT_PAYLOAD_SIZE * 3) as u64;
        let buffer = LockFreeStreamBuffer::new(total);

        for i in 1..=3 {
            buffer
                .insert(i, Bytes::from(vec![i as u8; FRAGMENT_PAYLOAD_SIZE]))
                .unwrap();
        }

        // Consume first fragment
        buffer.mark_consumed(1);

        // iter_contiguous should stop immediately (slot 0 is now NULL)
        let contiguous: Vec<_> = buffer.iter_contiguous().collect();
        assert_eq!(contiguous.len(), 0);
    }

    #[test]
    fn test_consumed_frontier_accessor() {
        let total = (FRAGMENT_PAYLOAD_SIZE * 5) as u64;
        let buffer = LockFreeStreamBuffer::new(total);

        assert_eq!(buffer.consumed_frontier(), 0);

        for i in 1..=5 {
            buffer
                .insert(i, Bytes::from(vec![i as u8; FRAGMENT_PAYLOAD_SIZE]))
                .unwrap();
        }

        buffer.mark_consumed(2);
        assert_eq!(buffer.consumed_frontier(), 2);

        buffer.mark_consumed(4);
        assert_eq!(buffer.consumed_frontier(), 4);

        // Calling with lower value doesn't decrease frontier
        buffer.mark_consumed(3);
        assert_eq!(buffer.consumed_frontier(), 4); // Still 4, not 3
    }

    #[test]
    #[should_panic(expected = "mark_consumed index 5 exceeds total_fragments 3")]
    fn test_mark_consumed_panics_on_invalid_index() {
        let total = (FRAGMENT_PAYLOAD_SIZE * 3) as u64;
        let buffer = LockFreeStreamBuffer::new(total);
        buffer.mark_consumed(5); // Should panic
    }

    #[test]
    fn test_concurrent_mark_consumed() {
        use std::sync::Arc;
        use std::thread;

        let total = (FRAGMENT_PAYLOAD_SIZE * 100) as u64;
        let buffer = Arc::new(LockFreeStreamBuffer::new(total));

        // Insert all fragments first
        for i in 1..=100 {
            buffer
                .insert(i, Bytes::from(vec![i as u8; FRAGMENT_PAYLOAD_SIZE]))
                .unwrap();
        }

        // Multiple threads trying to consume overlapping ranges
        let handles: Vec<_> = (0..4)
            .map(|t| {
                let buffer = Arc::clone(&buffer);
                thread::spawn(move || {
                    // Each thread consumes a range
                    let _start = t * 25 + 1;
                    let end = (t + 1) * 25;
                    buffer.mark_consumed(end as u32)
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        // All fragments should be consumed
        assert_eq!(buffer.consumed_frontier(), 100);
        assert_eq!(buffer.inserted_count(), 0);
    }

    #[test]
    fn test_memory_reclamation_flow() {
        // Simulate a streaming consumer that reads and frees as it goes
        let total = (FRAGMENT_PAYLOAD_SIZE * 5) as u64;
        let buffer = LockFreeStreamBuffer::new(total);

        // Producer inserts all fragments
        for i in 1..=5 {
            buffer
                .insert(i, Bytes::from(vec![i as u8; FRAGMENT_PAYLOAD_SIZE]))
                .unwrap();
        }

        // Consumer reads and frees incrementally
        for i in 1..=5 {
            // Read the fragment
            let data = buffer.get(i);
            assert!(data.is_some());
            assert_eq!(data.unwrap()[0], i as u8);

            // Mark as consumed (frees memory)
            let freed = buffer.mark_consumed(i);
            assert_eq!(freed, 1);

            // Verify it's gone
            assert!(buffer.get(i).is_none());
            assert_eq!(buffer.consumed_frontier(), i);
        }

        // All consumed
        assert_eq!(buffer.inserted_count(), 0);
        assert_eq!(buffer.consumed_frontier(), 5);
    }

    #[test]
    fn test_concurrent_insert_and_consume() {
        use std::sync::Arc;
        use std::thread;

        let total = (FRAGMENT_PAYLOAD_SIZE * 10) as u64;
        let buffer = Arc::new(LockFreeStreamBuffer::new(total));

        // Producer thread
        let buffer_producer = Arc::clone(&buffer);
        let producer = thread::spawn(move || {
            for i in 1..=10 {
                let data = Bytes::from(vec![i as u8; FRAGMENT_PAYLOAD_SIZE]);
                buffer_producer.insert(i, data).unwrap();
                thread::sleep(std::time::Duration::from_micros(100));
            }
        });

        // Consumer thread (reads and consumes as they arrive)
        let buffer_consumer = Arc::clone(&buffer);
        let consumer = thread::spawn(move || {
            let mut consumed = 0;
            while consumed < 10 {
                let next = consumed + 1;
                if buffer_consumer.get(next as u32).is_some() {
                    buffer_consumer.mark_consumed(next as u32);
                    consumed = next;
                } else {
                    thread::yield_now();
                }
            }
        });

        producer.join().unwrap();
        consumer.join().unwrap();

        assert_eq!(buffer.consumed_frontier(), 10);
        assert_eq!(buffer.inserted_count(), 0);
    }
}

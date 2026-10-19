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
//! - **Lazily allocated slots**: Slot chunks are allocated as fragments arrive
//! - **Byte-accounted**: Completion is measured in payload bytes, which can
//!   never exceed the stream's `total_size`
//! - **Progressive reclamation**: Consumed fragments can be freed immediately
//!
//! # Performance
//!
//! Based on spike validation (issue #1452, PR iduartgomez/freenet-core#204):
//! - Insert throughput: 2,235 MB/s (vs 23 MB/s with RwLock)
//! - First-fragment latency: 25μs (vs 103μs with RwLock)
//! - 96× speedup over RwLock-based approach

use bytes::Bytes;
use event_listener::Event;
use std::ptr;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicPtr, AtomicU32, AtomicU64, Ordering};

use crate::simulation::{RealTime, TimeSource};
use crate::transport::packet_data;

/// Maximum payload size per fragment (excluding metadata overhead).
/// This matches the constant from peer_connection.rs.
/// Measured overhead: 41 bytes (StreamFragment fields + Option discriminant for metadata_bytes)
/// Public when bench feature is enabled for benchmarking.
#[cfg(feature = "bench")]
pub const FRAGMENT_PAYLOAD_SIZE: usize = packet_data::MAX_DATA_SIZE - 41;
#[cfg(not(feature = "bench"))]
pub(crate) const FRAGMENT_PAYLOAD_SIZE: usize = packet_data::MAX_DATA_SIZE - 41;

/// Fragment slots per lazily allocated chunk of a [`LockFreeStreamBuffer`].
///
/// A chunk is 2 KiB of slot pointers covering ~280 KiB of payload. Small
/// enough that the fragment which triggers a chunk's allocation is the same
/// order of size as the chunk; large enough that the table of chunk pointers
/// stays small (about 14 KiB at `MAX_STREAM_TOTAL_BYTES`).
const SLOTS_PER_CHUNK: usize = 256;

/// One lazily allocated chunk of fragment slots.
type SlotChunk = Box<[AtomicPtr<Bytes>]>;

/// Marker stored in a slot while an insert that has claimed it checks the
/// byte limit, before the fragment is published. Never dereferenced or freed;
/// readers treat it as empty. A dangling, well-aligned pointer is distinct
/// from every `Box<Bytes>` allocation.
const CLAIMED: *mut Bytes = ptr::NonNull::<Bytes>::dangling().as_ptr();

/// Marker left in a slot whose fragment a reader has taken (`take`,
/// `mark_consumed`). Readers treat it as empty, inserts as a duplicate, and
/// the frontier as filled, so a taken fragment can never be inserted again.
/// Like [`CLAIMED`], never dereferenced or freed, and distinct from every
/// allocation (and from `CLAIMED`).
const TAKEN: *mut Bytes = CLAIMED.wrapping_add(1);

fn is_claimed(ptr: *mut Bytes) -> bool {
    ptr == CLAIMED
}

/// Whether `ptr` is one of the slot markers rather than a fragment.
fn is_marker(ptr: *mut Bytes) -> bool {
    ptr == CLAIMED || ptr == TAKEN
}

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
/// # Slot allocation
///
/// Slots are allocated in chunks of [`SLOTS_PER_CHUNK`], each on the first
/// insert that lands in it. Construction allocates only the table of chunk
/// pointers, so slot memory is proportional to the fragments received.
///
/// # Completion
///
/// A stream is complete when its received payload bytes equal `total_size`
/// and every inserted fragment is contiguous from fragment 1. Inserts that
/// would take the received total past `total_size`, and empty fragments, are
/// rejected.
///
/// Potential improvements:
/// - **Ring buffer with sliding window**: For multi-consumer scenarios, track
///   per-consumer read positions and only retain fragments in the active window.
///   This would allow automatic memory reclamation even with multiple consumers,
///   without requiring explicit `stream_with_reclaim()`.
/// - **Freeing consumed chunks**: a chunk whose slots have all been consumed
///   could be released.
/// - **Consumer reference counting**: Track active consumers to automatically
///   reclaim fragments when all consumers have passed them.
///
/// Based on spike implementation from iduartgomez/freenet-core#204.
pub struct LockFreeStreamBuffer<T: TimeSource = RealTime> {
    /// Fragment slots, in chunks of [`SLOTS_PER_CHUNK`] allocated on first
    /// use. Each slot is a pointer to heap-allocated Bytes. Fragment numbers
    /// are 1-indexed, so fragment N is slot N-1. A NULL slot, or a slot in a
    /// chunk not yet allocated, means empty/cleared; non-NULL means present.
    ///
    /// Note: One extra slot is counted beyond the base fragment count to handle
    /// the case where fragment #1 has reduced payload due to embedded metadata
    /// (fix #2757). This ensures the overflow fragment can be stored.
    chunks: Box<[OnceLock<SlotChunk>]>,
    /// Total expected bytes for the complete stream.
    total_size: u64,
    /// Total number of fragment slots allocated (includes one extra for metadata overflow).
    total_fragments: u32,
    /// Payload bytes claimed by inserts that have passed the length check,
    /// including any whose slot write is still in progress. Never exceeds
    /// `total_size`.
    reserved_bytes: AtomicU64,
    /// Payload bytes of fragments whose slot write has completed.
    received_bytes: AtomicU64,
    /// Number of fragments successfully inserted (monotonic: consumption does
    /// not decrement it).
    inserted_fragments: AtomicU32,
    /// Highest contiguous fragment number from the start (0 = none received).
    /// Uses atomic CAS for lock-free frontier advancement.
    contiguous_fragments: AtomicU32,
    /// Highest fragment number that has been consumed/freed (0 = none consumed).
    /// Fragments up to this number have been cleared from memory.
    consumed_frontier: AtomicU32,
    /// Event notification for when new fragments arrive.
    ///
    /// # Lock-Free Considerations
    ///
    /// Fragment slot operations (AtomicPtr CAS) are lock-free. However,
    /// `event-listener::Event` uses a mutex internally for its waiter list.
    /// This means:
    /// - **Fast path (no waiters)**: `notify()` is essentially lock-free
    /// - **Slow path (waiters present)**: `notify()` takes a mutex briefly
    ///
    /// Since notification only happens during insert and the mutex is only
    /// held to wake waiters (not during the actual fragment write), contention
    /// is minimal in practice.
    data_available: Event,
    /// Time source for handling delays in async operations.
    /// In production, this is `RealTime` which delegates to tokio.
    /// In tests, this can be `VirtualTime` for deterministic timing.
    #[allow(dead_code)]
    time_source: T,
}

// Production constructor (backward-compatible, uses real time)
impl LockFreeStreamBuffer<RealTime> {
    /// Creates a new buffer for a stream of `total_size` bytes using real time.
    ///
    /// # Arguments
    ///
    /// * `total_size` - Total expected bytes in the complete stream
    ///
    /// # Returns
    ///
    /// A new `LockFreeStreamBuffer` with enough slots to hold all fragments.
    pub fn new(total_size: u64) -> Self {
        Self::new_with_time_source(total_size, RealTime::new())
    }
}

// Generic implementation (works with any TimeSource)
impl<T: TimeSource> LockFreeStreamBuffer<T> {
    /// Creates a new buffer for a stream of `total_size` bytes with a custom time source.
    ///
    /// # Arguments
    ///
    /// * `total_size` - Total expected bytes in the complete stream; at most
    ///   [`MAX_STREAM_TOTAL_BYTES`](super::streaming::MAX_STREAM_TOTAL_BYTES)
    /// * `time_source` - TimeSource for handling async delays (RealTime for production, VirtualTime for tests)
    ///
    /// # Returns
    ///
    /// A new `LockFreeStreamBuffer` able to hold all fragments. Only the
    /// table of chunk pointers is allocated here (see "Slot allocation").
    pub fn new_with_time_source(total_size: u64, time_source: T) -> Self {
        debug_assert!(
            total_size <= super::streaming::MAX_STREAM_TOTAL_BYTES,
            "stream length {total_size} exceeds MAX_STREAM_TOTAL_BYTES"
        );
        let num_fragments = Self::calculate_fragment_count(total_size);
        let chunks: Vec<OnceLock<SlotChunk>> = (0..num_fragments.div_ceil(SLOTS_PER_CHUNK))
            .map(|_| OnceLock::new())
            .collect();

        Self {
            chunks: chunks.into_boxed_slice(),
            total_size,
            total_fragments: num_fragments as u32,
            reserved_bytes: AtomicU64::new(0),
            received_bytes: AtomicU64::new(0),
            inserted_fragments: AtomicU32::new(0),
            contiguous_fragments: AtomicU32::new(0),
            consumed_frontier: AtomicU32::new(0),
            data_available: Event::new(),
            time_source,
        }
    }

    /// Base fragment count: the minimum number of fragments needed assuming
    /// all fragments carry full FRAGMENT_PAYLOAD_SIZE payloads.
    fn base_fragment_count(total_size: u64) -> usize {
        if total_size == 0 {
            return 0;
        }
        (total_size as usize).div_ceil(FRAGMENT_PAYLOAD_SIZE)
    }

    /// Calculates the number of fragment slots to allocate for a given byte count.
    ///
    /// Allocates one extra slot beyond the base calculation to handle the case
    /// where fragment #1 has reduced payload due to embedded metadata (fix #2757).
    /// When metadata is embedded in fragment #1, its payload is smaller, which
    /// may require one additional fragment to carry the overflow data.
    fn calculate_fragment_count(total_size: u64) -> usize {
        let base = Self::base_fragment_count(total_size);
        if base == 0 {
            return 0;
        }
        // +1 for potential metadata overflow in fragment #1
        base + 1
    }

    /// The slot for 0-based index `idx`, or `None` if its chunk has not been
    /// allocated (every slot in such a chunk is empty). `idx` must be below
    /// `total_fragments`.
    fn slot(&self, idx: usize) -> Option<&AtomicPtr<Bytes>> {
        self.chunks[idx / SLOTS_PER_CHUNK]
            .get()
            .map(|chunk| &chunk[idx % SLOTS_PER_CHUNK])
    }

    /// The slot for 0-based index `idx`, allocating its chunk if needed.
    /// `idx` must be below `total_fragments`.
    fn slot_or_alloc(&self, idx: usize) -> &AtomicPtr<Bytes> {
        let chunk_idx = idx / SLOTS_PER_CHUNK;
        let chunk = self.chunks[chunk_idx].get_or_init(|| {
            // The last chunk only covers the slots that exist.
            let start = chunk_idx * SLOTS_PER_CHUNK;
            let len = (self.total_fragments as usize - start).min(SLOTS_PER_CHUNK);
            (0..len).map(|_| AtomicPtr::new(ptr::null_mut())).collect()
        });
        &chunk[idx % SLOTS_PER_CHUNK]
    }

    /// The raw pointer in slot `idx`, markers included (NULL if the chunk is
    /// unallocated).
    fn load_raw(&self, idx: usize) -> *mut Bytes {
        self.slot(idx)
            .map_or(ptr::null_mut(), |slot| slot.load(Ordering::Acquire))
    }

    /// The fragment in slot `idx`: NULL if the slot is empty, its chunk is
    /// unallocated, an insert has claimed it but not yet published, or a
    /// reader has taken it.
    fn load(&self, idx: usize) -> *mut Bytes {
        let ptr = self.load_raw(idx);
        if is_marker(ptr) { ptr::null_mut() } else { ptr }
    }

    /// Atomically take the published fragment in slot `idx`, leaving
    /// [`TAKEN`] behind (NULL if there was none). A slot claimed by an
    /// in-progress insert is left alone.
    fn take_slot(&self, idx: usize) -> *mut Bytes {
        let Some(slot) = self.slot(idx) else {
            return ptr::null_mut();
        };
        let mut current = slot.load(Ordering::Acquire);
        loop {
            if current.is_null() || is_marker(current) {
                return ptr::null_mut();
            }
            match slot.compare_exchange_weak(current, TAKEN, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return current,
                Err(actual) => current = actual,
            }
        }
    }

    /// Number of slot chunks allocated so far.
    #[cfg(test)]
    pub(crate) fn allocated_chunks(&self) -> usize {
        self.chunks.iter().filter(|c| c.get().is_some()).count()
    }

    /// Number of chunks a fully populated buffer would allocate.
    #[cfg(test)]
    pub(crate) fn chunk_capacity(&self) -> usize {
        self.chunks.len()
    }

    /// Inserts a fragment into the buffer.
    ///
    /// This is the core lock-free operation. Uses atomic CAS to insert
    /// the fragment - duplicates are idempotent no-ops.
    ///
    /// Ordering that `StreamingInboundStream::poll_next` depends on: the slot
    /// becomes readable, THEN the contiguous frontier advances, THEN
    /// listeners are notified. A consumer can therefore read the last
    /// fragment before `is_complete()` turns true, and relies on the notify
    /// that follows (#5785). `store_slot_for_test` and
    /// `finish_insert_for_test` split this sequence for tests; keep them in
    /// step with any change here.
    ///
    /// Concurrent inserts are safe for the slots and the byte accounting:
    /// each fragment is published at most once and the received total never
    /// exceeds `total_size`. The FRONTIER assumes a single inserting task per
    /// buffer, which is how the transport receive loop uses it: two
    /// concurrent inserts of adjacent fragments could each miss the other's
    /// slot in `advance_frontier` under the language memory model, because
    /// the slot CAS and the frontier's slot load are not `SeqCst`.
    ///
    /// # Arguments
    ///
    /// * `fragment_number` - 1-indexed fragment number (first fragment is 1)
    /// * `data` - The fragment payload
    ///
    /// # Returns
    ///
    /// * `Ok(true)` - Fragment was inserted successfully
    /// * `Ok(false)` - Fragment was already present (duplicate, no-op)
    /// * `Err(InsertError)` - Fragment number out of bounds, already consumed,
    ///   empty, or more payload than the stream's `total_size` allows
    pub fn insert(&self, fragment_number: u32, data: Bytes) -> Result<bool, InsertError> {
        if fragment_number == 0 || fragment_number > self.total_fragments {
            return Err(InsertError::InvalidNumber {
                number: fragment_number,
                max: self.total_fragments,
            });
        }

        // Check if this fragment has already been consumed
        if fragment_number <= self.consumed_frontier.load(Ordering::Acquire) {
            return Err(InsertError::AlreadyConsumed {
                number: fragment_number,
            });
        }

        let idx = (fragment_number - 1) as usize;

        // At or below the contiguous frontier every slot was filled once and
        // is never empty again (a reader leaves `TAKEN`), so the claim below
        // fails and the insert is a duplicate even after the slot was taken.
        debug_assert!(
            fragment_number > self.contiguous_fragments.load(Ordering::Acquire)
                || !self.load_raw(idx).is_null(),
            "slot {fragment_number} at or below the frontier is empty"
        );

        // A present slot is a duplicate, whatever its length.
        if !self.load(idx).is_null() {
            return Ok(false);
        }

        // Every fragment carries payload, except that fragment #1 may be
        // empty: senders before the fix for that embedded metadata which left
        // no room for payload in it. It reserves nothing.
        let len = data.len() as u64;
        if len == 0 && fragment_number != 1 {
            return Err(InsertError::Empty {
                number: fragment_number,
            });
        }

        // Claim the slot first, so that concurrent duplicates of this
        // fragment see it occupied and return `Ok(false)` without touching
        // the byte reservation. Readers treat a claimed slot as empty.
        let slot = self.slot_or_alloc(idx);
        if slot
            .compare_exchange(
                ptr::null_mut(),
                CLAIMED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return Ok(false); // Duplicate, no-op
        }

        // Reserve this fragment's bytes, keeping the total within `total_size`.
        // Only one insert per slot gets here, so a reservation is never made
        // for a duplicate.
        let total_size = self.total_size;
        if self
            .reserved_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |reserved| {
                reserved.checked_add(len).filter(|next| *next <= total_size)
            })
            .is_err()
        {
            slot.store(ptr::null_mut(), Ordering::Release);
            return Err(InsertError::ExceedsTotalSize {
                number: fragment_number,
                total_size,
            });
        }

        // Publish the fragment.
        let new_ptr = Box::into_raw(Box::new(data));
        if slot
            .compare_exchange(CLAIMED, new_ptr, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            // Unreachable: only the claiming insert writes a claimed slot,
            // and `take_slot` leaves claimed slots alone. Undo regardless.
            self.reserved_bytes.fetch_sub(len, Ordering::AcqRel);
            // SAFETY: new_ptr was just created by Box::into_raw and not shared
            unsafe {
                drop(Box::from_raw(new_ptr));
            }
            return Ok(false);
        }

        // The count is published before the bytes, so a reader that sees the
        // final byte total also sees every fragment that contributed to it
        // (see `is_complete`).
        self.inserted_fragments.fetch_add(1, Ordering::Release);
        self.received_bytes.fetch_add(len, Ordering::Release);
        self.advance_frontier();
        // Notify all waiting listeners that new data is available
        self.data_available.notify(usize::MAX);
        Ok(true)
    }

    /// Test-only first half of [`Self::insert`]: the fragment becomes readable
    /// but the contiguous frontier has not advanced and no listener is
    /// notified. [`Self::finish_insert_for_test`] is the second half.
    #[cfg(test)]
    pub(crate) fn store_slot_for_test(&self, fragment_number: u32, data: Bytes) {
        let idx = (fragment_number - 1) as usize;
        let len = data.len() as u64;
        let prev = self
            .slot_or_alloc(idx)
            .swap(Box::into_raw(Box::new(data)), Ordering::AcqRel);
        assert!(prev.is_null(), "slot {fragment_number} was already filled");
        self.reserved_bytes.fetch_add(len, Ordering::AcqRel);
        self.inserted_fragments.fetch_add(1, Ordering::Release);
        self.received_bytes.fetch_add(len, Ordering::Release);
    }

    /// Test-only second half of [`Self::insert`]: advance the frontier, then
    /// notify, in that order.
    #[cfg(test)]
    pub(crate) fn finish_insert_for_test(&self) {
        self.advance_frontier();
        self.data_available.notify(usize::MAX);
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
            // A published or taken fragment fills the slot; an empty or
            // claimed one is a gap.
            let ptr = self.load_raw(idx);
            if ptr.is_null() || is_claimed(ptr) {
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

    /// Returns true once every byte of the stream has been received and all
    /// received fragments are contiguous from fragment 1.
    ///
    /// Measured in bytes, not fragments: the fragment count a stream needs
    /// depends on its fragment sizes (fragment #1 is shorter when it embeds
    /// metadata, fix #2757), so no fragment count identifies completion.
    pub fn is_complete(&self) -> bool {
        // `received_bytes` is published after `inserted_fragments` (see
        // `insert`), so reading it first makes the count current for it. Once
        // it equals `total_size` no further insert can succeed.
        if self.received_bytes.load(Ordering::Acquire) != self.total_size {
            return false;
        }
        let inserted = self.inserted_fragments.load(Ordering::Acquire);
        self.contiguous_fragments.load(Ordering::Acquire) >= inserted
    }

    /// Payload bytes received so far (never more than `total_bytes()`).
    pub fn received_bytes(&self) -> u64 {
        self.received_bytes.load(Ordering::Acquire)
    }

    /// Returns the number of fragments that are currently present (not consumed).
    pub fn inserted_count(&self) -> usize {
        // Count non-NULL slots; an unallocated chunk has none.
        self.chunks
            .iter()
            .filter_map(OnceLock::get)
            .flat_map(|chunk| chunk.iter())
            .filter(|s| {
                let ptr = s.load(Ordering::Acquire);
                !ptr.is_null() && !is_marker(ptr)
            })
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

    /// Returns the highest contiguous fragment number (1-indexed).
    ///
    /// This indicates that all fragments from 1 to this number are present.
    /// Returns 0 if no fragments are present.
    pub fn highest_contiguous(&self) -> u32 {
        self.contiguous_fragments.load(Ordering::Acquire)
    }

    /// Returns the consumed frontier (1-indexed).
    ///
    /// Fragments up to and including this number have been freed.
    /// Returns 0 if no fragments have been consumed.
    pub fn consumed_frontier(&self) -> u32 {
        self.consumed_frontier.load(Ordering::Acquire)
    }

    /// Gets a reference to a fragment if it is present and not consumed.
    ///
    /// # Arguments
    ///
    /// * `fragment_number` - 1-indexed fragment number
    ///
    /// # Returns
    ///
    /// `Some(&Bytes)` if the fragment is present, `None` otherwise.
    ///
    /// # Safety
    ///
    /// The returned reference is valid as long as `mark_consumed` is not called
    /// for this number. Callers must ensure they don't hold references across
    /// consumption boundaries.
    pub fn get(&self, fragment_number: u32) -> Option<&Bytes> {
        if fragment_number == 0 || fragment_number > self.total_fragments {
            return None;
        }
        let idx = (fragment_number - 1) as usize;
        let ptr = self.load(idx);
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
    /// * `fragment_number` - 1-indexed fragment number
    ///
    /// # Returns
    ///
    /// `Some(Bytes)` if the fragment was present, `None` if already taken or not inserted.
    pub fn take(&self, fragment_number: u32) -> Option<Bytes> {
        if fragment_number == 0 || fragment_number > self.total_fragments {
            return None;
        }
        let idx = (fragment_number - 1) as usize;

        // Atomically empty the slot. An unallocated chunk holds nothing.
        let ptr = self.take_slot(idx);
        if ptr.is_null() {
            return None;
        }

        // SAFETY: ptr was set by insert() and we just took exclusive ownership via swap
        let boxed = unsafe { Box::from_raw(ptr) };
        Some(*boxed)
    }

    /// Marks fragments up to and including the given number as consumed, freeing their memory.
    ///
    /// This enables progressive memory reclamation for streaming consumers.
    /// After calling this, fragments 1..=up_to_number will return `None` from `get()`.
    ///
    /// # Arguments
    ///
    /// * `up_to_number` - 1-indexed fragment number (inclusive)
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
            let ptr = self.take_slot(idx as usize);
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

    /// Returns a reference to the notification event.
    ///
    /// Use `event.listen().await` to wait for new fragments to arrive.
    pub fn notifier(&self) -> &Event {
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

        // Sized from the bytes actually received. `is_complete()` means they
        // equal `total_size` and all sit in contiguous slots from fragment 1.
        let target_size = self.total_size as usize;
        let mut result = Vec::with_capacity(self.received_bytes() as usize);
        for idx in 0..self.total_fragments as usize {
            let ptr = self.load(idx);
            if ptr.is_null() {
                // The extra overflow slot may be empty when no metadata was
                // embedded. Stop iterating once we've collected enough data.
                break;
            }
            // SAFETY: ptr is non-null and was set by insert()
            let data = unsafe { &*ptr };
            result.extend_from_slice(data);
            if result.len() >= target_size {
                break;
            }
        }

        if result.len() == target_size {
            Some(result)
        } else {
            None // Not enough contiguous data despite is_complete() being true
        }
    }

    /// Returns an iterator over the fragments in order.
    ///
    /// Yields `None` for missing/consumed fragments and `Some(&Bytes)` for present ones.
    pub fn iter(&self) -> impl Iterator<Item = Option<&Bytes>> {
        (0..self.total_fragments as usize).map(|idx| {
            let ptr = self.load(idx);
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
        (0..self.total_fragments as usize).map_while(|idx| {
            let ptr = self.load(idx);
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
    /// for out-of-order fragments. Sized by the data collected.
    #[allow(dead_code)]
    pub fn collect_contiguous(&self) -> Vec<u8> {
        let mut result = Vec::new();
        for data in self.iter_contiguous() {
            result.extend_from_slice(data);
        }
        result
    }
}

/// Ensures all remaining fragments are properly deallocated when the buffer is dropped.
impl<T: TimeSource> Drop for LockFreeStreamBuffer<T> {
    fn drop(&mut self) {
        for slot in self
            .chunks
            .iter()
            .filter_map(OnceLock::get)
            .flat_map(|chunk| chunk.iter())
        {
            let ptr = slot.load(Ordering::Acquire);
            if !ptr.is_null() && !is_marker(ptr) {
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
    /// Fragment number is out of bounds.
    InvalidNumber {
        /// The provided fragment number (1-indexed)
        number: u32,
        /// Maximum valid fragment number
        max: u32,
    },
    /// Fragment has already been consumed and cannot be re-inserted.
    AlreadyConsumed {
        /// The fragment number that was already consumed
        number: u32,
    },
    /// Fragment carries no payload.
    Empty {
        /// The fragment number
        number: u32,
    },
    /// Fragment would take the received payload past the stream's total size.
    ExceedsTotalSize {
        /// The fragment number
        number: u32,
        /// The stream's total size in bytes
        total_size: u64,
    },
}

impl std::fmt::Display for InsertError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InsertError::InvalidNumber { number, max } => {
                write!(
                    f,
                    "fragment number {} is out of bounds (max: {})",
                    number, max
                )
            }
            InsertError::AlreadyConsumed { number } => {
                write!(f, "fragment number {} has already been consumed", number)
            }
            InsertError::Empty { number } => {
                write!(f, "fragment number {} carries no payload", number)
            }
            InsertError::ExceedsTotalSize { number, total_size } => {
                write!(
                    f,
                    "fragment number {} exceeds the stream's total size of {} bytes",
                    number, total_size
                )
            }
        }
    }
}

impl std::error::Error for InsertError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::GlobalExecutor;
    use crate::simulation::VirtualTime;
    use std::time::Duration;

    #[test]
    fn test_new_buffer_empty() {
        let buffer = LockFreeStreamBuffer::new(0);
        assert_eq!(buffer.total_fragments(), 0);
        assert!(buffer.is_complete()); // Empty buffer is complete
    }

    /// Slot chunks are allocated as fragments land in them; constructing a
    /// buffer allocates only its chunk table.
    #[test]
    fn slot_chunks_are_allocated_only_as_fragments_arrive() {
        let total = super::super::streaming::MAX_STREAM_TOTAL_BYTES;
        let buffer = LockFreeStreamBuffer::new(total);
        assert!(buffer.chunk_capacity() > 500, "premise: a large buffer");
        assert_eq!(
            buffer.allocated_chunks(),
            0,
            "construction allocates no slots"
        );
        assert_eq!(buffer.inserted_count(), 0);
        assert!(buffer.get(1).is_none());
        assert!(buffer.take(buffer.total_fragments() as u32).is_none());
        assert!(buffer.iter_contiguous().next().is_none());

        // One fragment far into the stream allocates exactly its own chunk.
        let last = buffer.total_fragments() as u32;
        assert!(buffer.insert(last, Bytes::from_static(b"tail")).unwrap());
        assert_eq!(buffer.allocated_chunks(), 1);
        assert_eq!(buffer.get(last).map(|b| &b[..]), Some(&b"tail"[..]));
        assert_eq!(buffer.inserted_count(), 1);

        // The first fragment allocates the first chunk, and contiguity across
        // an unallocated middle stops where it should.
        assert!(buffer.insert(1, Bytes::from_static(b"head")).unwrap());
        assert_eq!(buffer.allocated_chunks(), 2);
        assert_eq!(buffer.highest_contiguous(), 1);
        assert_eq!(buffer.iter_contiguous().count(), 1);

        // Consuming across unallocated chunks frees what is there and nothing else.
        assert_eq!(buffer.mark_consumed(last), 2);
        assert_eq!(buffer.inserted_count(), 0);
    }

    /// Payload of `len` bytes with a non-repeating-per-fragment pattern,
    /// built by doubling copies (fast even at the stream cap).
    fn patterned(len: usize) -> Bytes {
        let mut data: Vec<u8> = (0..251u16).map(|i| i as u8).collect();
        while data.len() < len {
            let n = data.len().min(len - data.len());
            data.extend_from_within(..n);
        }
        data.truncate(len);
        data.into()
    }

    /// Insert `data` split at `cuts` (fragment boundaries), in reverse order
    /// so the frontier only advances on the last insert.
    fn insert_reversed(buffer: &LockFreeStreamBuffer, data: &Bytes, cuts: &[usize]) {
        let mut bounds = vec![0];
        bounds.extend_from_slice(cuts);
        bounds.push(data.len());
        for i in (0..bounds.len() - 1).rev() {
            let piece = data.slice(bounds[i]..bounds[i + 1]);
            assert!(buffer.insert(i as u32 + 1, piece).unwrap());
            assert_eq!(buffer.is_complete(), i == 0, "complete only at the end");
        }
    }

    /// A stream spanning several slot chunks reassembles, with a partial
    /// last fragment. (The length limit itself is pinned by
    /// `streaming::tests::declared_stream_length_bounds`.)
    #[test]
    fn stream_spanning_several_chunks_reassembles() {
        let total = 3 * SLOTS_PER_CHUNK * FRAGMENT_PAYLOAD_SIZE + 5;
        let data = patterned(total);
        let buffer = LockFreeStreamBuffer::new(total as u64);
        let cuts: Vec<usize> = (FRAGMENT_PAYLOAD_SIZE..total)
            .step_by(FRAGMENT_PAYLOAD_SIZE)
            .collect();
        insert_reversed(&buffer, &data, &cuts);
        let assembled = buffer.assemble().expect("complete stream assembles");
        assert!(
            assembled == data,
            "reassembled bytes must match what was sent"
        );
        assert_eq!(assembled.capacity(), total);
    }

    /// A stream spanning more than one slot chunk, with a short first
    /// fragment that pushes its tail into the overflow slot, reassembles.
    #[test]
    fn stream_across_chunks_using_the_overflow_slot_reassembles() {
        let total = (SLOTS_PER_CHUNK + 11) * FRAGMENT_PAYLOAD_SIZE - 3;
        let data = patterned(total);
        let buffer = LockFreeStreamBuffer::new(total as u64);
        let first = FRAGMENT_PAYLOAD_SIZE / 2;
        let cuts: Vec<usize> = (first..total).step_by(FRAGMENT_PAYLOAD_SIZE).collect();
        assert_eq!(
            cuts.len() + 1,
            buffer.total_fragments(),
            "uses the overflow slot"
        );
        insert_reversed(&buffer, &data, &cuts);
        assert_eq!(buffer.allocated_chunks(), 2);
        assert!(buffer.assemble().expect("complete") == data);
    }

    #[test]
    fn test_new_buffer_single_fragment() {
        let buffer = LockFreeStreamBuffer::new(100);
        // +1 overflow slot for potential metadata overhead in fragment #1
        assert_eq!(buffer.total_fragments(), 2);
        assert!(!buffer.is_complete());
    }

    #[test]
    fn test_new_buffer_multiple_fragments() {
        // FRAGMENT_PAYLOAD_SIZE bytes per fragment
        let total = (FRAGMENT_PAYLOAD_SIZE * 3) as u64;
        let buffer = LockFreeStreamBuffer::new(total);
        assert_eq!(buffer.total_fragments(), 4); // 3 base + 1 overflow
    }

    #[test]
    fn test_new_buffer_partial_last_fragment() {
        // 2.5 fragments worth of data
        let total = (FRAGMENT_PAYLOAD_SIZE * 2 + FRAGMENT_PAYLOAD_SIZE / 2) as u64;
        let buffer = LockFreeStreamBuffer::new(total);
        assert_eq!(buffer.total_fragments(), 4); // 3 base + 1 overflow
    }

    #[test]
    fn test_insert_single_fragment() {
        let buffer = LockFreeStreamBuffer::new(5);
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
    fn test_insert_invalid_number_zero() {
        let buffer = LockFreeStreamBuffer::new(100);
        let data = Bytes::from_static(b"hello");

        let result = buffer.insert(0, data);
        assert!(matches!(result, Err(InsertError::InvalidNumber { .. })));
    }

    #[test]
    fn test_insert_invalid_number_too_large() {
        let buffer = LockFreeStreamBuffer::new(100);
        let data = Bytes::from_static(b"hello");

        // Buffer has 2 slots (1 base + 1 overflow), so fragment 3 is out of bounds
        let result = buffer.insert(3, data);
        assert!(matches!(result, Err(InsertError::InvalidNumber { .. })));
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
    fn test_insert_rejects_bytes_past_total_size() {
        // A fragment longer than the whole stream is refused, not truncated.
        let buffer = LockFreeStreamBuffer::new(10);
        assert_eq!(
            buffer.insert(1, Bytes::from_static(b"hello world plus extra")),
            Err(InsertError::ExceedsTotalSize {
                number: 1,
                total_size: 10
            })
        );
        assert_eq!(buffer.inserted_count(), 0);
        assert_eq!(buffer.received_bytes(), 0);

        // So is one that would take the running total past it.
        assert!(buffer.insert(1, Bytes::from_static(b"hello")).unwrap());
        assert!(matches!(
            buffer.insert(2, Bytes::from_static(b" world")),
            Err(InsertError::ExceedsTotalSize { number: 2, .. })
        ));
        assert!(buffer.insert(2, Bytes::from_static(b" worl")).unwrap());
        assert_eq!(buffer.assemble().unwrap(), b"hello worl");
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

    /// Concurrent duplicates of one fragment never cost another fragment its
    /// place: with a total that fits exactly two fragments, duplicates of
    /// fragment 1 racing a genuine fragment 2 always yield one insert of
    /// each, and every call returns `Ok`.
    #[test]
    fn concurrent_duplicates_never_refuse_a_genuine_fragment() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        const DUPLICATES: usize = 8;
        for round in 0..300 {
            let buffer = Arc::new(LockFreeStreamBuffer::new(2 * FRAGMENT_PAYLOAD_SIZE as u64));
            let barrier = Arc::new(Barrier::new(DUPLICATES + 1));
            let mut handles = Vec::new();
            for _ in 0..DUPLICATES {
                let (buffer, barrier) = (Arc::clone(&buffer), Arc::clone(&barrier));
                handles.push(thread::spawn(move || {
                    barrier.wait();
                    (
                        1,
                        buffer.insert(1, Bytes::from(vec![1u8; FRAGMENT_PAYLOAD_SIZE])),
                    )
                }));
            }
            {
                let (buffer, barrier) = (Arc::clone(&buffer), Arc::clone(&barrier));
                handles.push(thread::spawn(move || {
                    barrier.wait();
                    (
                        2,
                        buffer.insert(2, Bytes::from(vec![2u8; FRAGMENT_PAYLOAD_SIZE])),
                    )
                }));
            }
            let mut inserted = [0usize; 3];
            for handle in handles {
                let (number, result) = handle.join().unwrap();
                match result {
                    Ok(true) => inserted[number] += 1,
                    Ok(false) => {}
                    Err(e) => panic!("round {round}: fragment {number} refused: {e}"),
                }
            }
            assert_eq!(inserted, [0, 1, 1], "round {round}");
            // Not `is_complete()`: the frontier assumes a single inserter
            // (see `insert`), and this test is about the slots and bytes.
            assert_eq!(
                buffer.received_bytes(),
                2 * FRAGMENT_PAYLOAD_SIZE as u64,
                "round {round}"
            );
            assert_eq!(buffer.inserted_count(), 2, "round {round}");
        }
    }

    /// A replay of a fragment a reclaiming reader has already taken is a
    /// duplicate, not new data: it does not count against the total, and the
    /// rest of the stream still arrives and completes it.
    #[test]
    fn replay_of_a_taken_fragment_does_not_block_the_rest() {
        let f = FRAGMENT_PAYLOAD_SIZE;
        let buffer = LockFreeStreamBuffer::new((4 * f) as u64);
        let frag = |n: u8| Bytes::from(vec![n; f]);
        assert!(buffer.insert(1, frag(1)).unwrap());
        assert!(buffer.insert(2, frag(2)).unwrap());
        assert_eq!(buffer.take(1), Some(frag(1)));

        assert_eq!(
            buffer.insert(1, frag(1)),
            Ok(false),
            "replay is a duplicate"
        );
        assert!(buffer.get(1).is_none(), "the taken slot stays empty");
        assert!(buffer.insert(3, frag(3)).unwrap());
        assert!(buffer.insert(4, frag(4)).unwrap());
        assert!(buffer.is_complete());
        assert_eq!(buffer.received_bytes(), (4 * f) as u64);
    }

    /// A fragment taken by a reader before the frontier has passed it is
    /// still a duplicate when replayed, and the frontier advances over it.
    #[test]
    fn a_fragment_taken_before_the_frontier_advances_stays_taken() {
        let f = FRAGMENT_PAYLOAD_SIZE;
        let frag = |n: u8| Bytes::from(vec![n; f]);
        let buffer = LockFreeStreamBuffer::new((2 * f) as u64);
        // Fragment 1 readable, frontier not yet advanced (the window in
        // `insert` between publishing the slot and advancing the frontier).
        buffer.store_slot_for_test(1, frag(1));
        assert_eq!(buffer.highest_contiguous(), 0);
        assert_eq!(buffer.take(1), Some(frag(1)));

        assert_eq!(
            buffer.insert(1, frag(1)),
            Ok(false),
            "a taken fragment is a duplicate"
        );
        assert!(buffer.get(1).is_none());
        buffer.finish_insert_for_test();
        assert_eq!(
            buffer.highest_contiguous(),
            1,
            "a taken slot counts as filled"
        );
        assert!(buffer.insert(2, frag(2)).unwrap());
        assert!(buffer.is_complete());
        assert_eq!(buffer.received_bytes(), (2 * f) as u64);
        assert_eq!(buffer.inserted_count(), 1, "only fragment 2 is still held");
    }

    /// Older senders could leave fragment #1 empty beside embedded metadata;
    /// it is accepted (reserving nothing), while an empty fragment elsewhere
    /// is still refused.
    #[test]
    fn an_empty_first_fragment_is_accepted_and_no_other() {
        let f = FRAGMENT_PAYLOAD_SIZE;
        let buffer = LockFreeStreamBuffer::new((2 * f) as u64);
        assert_eq!(buffer.insert(1, Bytes::new()), Ok(true));
        assert_eq!(
            buffer.insert(2, Bytes::new()),
            Err(InsertError::Empty { number: 2 })
        );
        assert!(buffer.insert(2, Bytes::from(vec![2u8; f])).unwrap());
        assert!(buffer.insert(3, Bytes::from(vec![3u8; f])).unwrap());
        assert!(buffer.is_complete());
        let mut expected = vec![2u8; f];
        expected.extend(vec![3u8; f]);
        assert_eq!(buffer.assemble().unwrap(), expected);
    }

    #[test]
    fn test_single_byte_stream() {
        let buffer = LockFreeStreamBuffer::new(1);
        assert_eq!(buffer.total_fragments(), 2); // 1 base + 1 overflow
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
        assert_eq!(buffer.total_fragments(), 3); // 2 base + 1 overflow

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

        // Insert all base fragments
        for i in 1..=3 {
            buffer
                .insert(i, Bytes::from(vec![i as u8; FRAGMENT_PAYLOAD_SIZE]))
                .unwrap();
        }

        let fragments: Vec<_> = buffer.iter().collect();
        assert_eq!(fragments.len(), 4); // 3 base + 1 overflow slot
        assert!(fragments[0].is_some());
        assert!(fragments[1].is_some());
        assert!(fragments[2].is_some());
        assert!(fragments[3].is_none()); // Overflow slot unused
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
        assert_eq!(fragments.len(), 4); // 3 base + 1 overflow slot
        assert!(fragments[0].is_some());
        assert!(fragments[1].is_none()); // Gap
        assert!(fragments[2].is_some());
        assert!(fragments[3].is_none()); // Overflow slot unused
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

        let time_source = VirtualTime::new();
        let buffer = Arc::new(LockFreeStreamBuffer::new_with_time_source(
            100,
            time_source.clone(),
        ));
        let buffer_clone = Arc::clone(&buffer);
        let barrier = Arc::new(Barrier::new(2));
        let barrier_clone = Arc::clone(&barrier);

        // Spawn a task that waits for notification
        let waiter = GlobalExecutor::spawn(async move {
            barrier_clone.wait().await;
            buffer_clone.notifier().listen().await;
            true
        });

        // Sync with waiter
        barrier.wait().await;
        time_source.advance(Duration::from_millis(10));

        // Insert should trigger notification
        buffer.insert(1, Bytes::from_static(b"data")).unwrap();

        // Use time_source.timeout instead of tokio::time::timeout
        let timeout_future = time_source.timeout(Duration::from_millis(100), waiter);
        let result = timeout_future.await;
        assert!(result.is_some());
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
            Err(InsertError::AlreadyConsumed { number: 1 })
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
    #[should_panic(expected = "mark_consumed index 5 exceeds total_fragments 4")]
    fn test_mark_consumed_panics_on_invalid_index() {
        let total = (FRAGMENT_PAYLOAD_SIZE * 3) as u64;
        let buffer = LockFreeStreamBuffer::new(total);
        buffer.mark_consumed(5); // Should panic (total_fragments = 3 base + 1 overflow = 4)
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

//! Piped stream forwarding for intermediate nodes.
//!
//! This module enables intermediate nodes to forward fragments immediately as they
//! arrive, without waiting for complete stream reassembly. This reduces latency and
//! memory usage compared to store-and-forward.
//!
//! **Note**: This module is infrastructure for Phase 2 integration. The types are
//! not yet wired into the main forwarding path.
//!
//! # Architecture
//!
//! ```text
//! ┌──────────────┐     fragments     ┌──────────────┐     forward      ┌──────────────┐
//! │   Source     │  ───────────────▶ │ Intermediate │  ─────────────▶  │ Destination  │
//! │    Node      │   (1, 2, 4, 3)    │    Node      │   (1, 2, 3, 4)   │    Node      │
//! └──────────────┘                   └──────────────┘                  └──────────────┘
//!                                           │
//!                                    ┌──────┴──────┐
//!                                    │ PipedStream │
//!                                    │             │
//!                                    │ next_to_fwd: 1
//!                                    │ buffered: {4}│ ← out-of-order
//!                                    └─────────────┘
//! ```
//!
//! # Memory Efficiency
//!
//! | Scenario | Memory Usage |
//! |----------|--------------|
//! | In-order delivery | 0 bytes buffer |
//! | Realistic reorder (chunks of 10) | ~12 KB |
//! | Worst case (fully reversed) | Full stream |
//!
//! # Usage
//!
//! ```ignore
//! // Create a piped stream with backpressure limits
//! let piped = PipedStream::new(
//!     stream_id,
//!     total_bytes,
//!     targets,
//!     PipedStreamConfig::default(),
//! );
//!
//! // Push fragments as they arrive - forwards immediately when in order
//! piped.push_fragment(1, payload1).await?; // forwards immediately
//! piped.push_fragment(3, payload3).await?; // buffers (waiting for 2)
//! piped.push_fragment(2, payload2).await?; // forwards 2, then 3
//! ```
//!
//! # Error Handling
//!
//! | Error | Cause | Recovery Strategy |
//! |-------|-------|-------------------|
//! | `BufferFull` | Too many out-of-order fragments | Apply upstream backpressure, wait for gaps to fill |
//! | `Cancelled` | Stream was cancelled | Stop processing, clean up resources |
//! | `InvalidFragment` | Fragment number out of range | Log and ignore (likely duplicate or corruption) |
//! | `SendFailed` | Target connection failed | Retry with exponential backoff, or mark target dead |
//!
//! **Phase 3 Integration Note**: Target failure tracking (which targets are dead,
//! retry logic) will be implemented when wiring `PipedStream` into the forwarding path.
//! This module provides the primitives; the integration layer handles failure policies.
//!
//! # Naming Convention
//!
//! All streaming modules use `fragment_number` (1-indexed) for fragment identifiers.
//! This is consistent across `piped_stream.rs`, `streaming_buffer.rs`, and `streaming.rs`.

// Allow dead code - this is Phase 2 infrastructure not yet integrated into forwarding path
#![allow(dead_code)]

use bytes::Bytes;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{Notify, Semaphore};

use super::StreamId;
use crate::simulation::{RealTime, TimeSource};

/// Configuration for piped stream behavior.
#[derive(Debug, Clone)]
pub struct PipedStreamConfig {
    /// Maximum number of out-of-order fragments to buffer per stream.
    /// Exceeding this limit returns an error (backpressure signal).
    pub max_buffered_fragments: usize,

    /// Maximum total bytes to buffer across out-of-order fragments.
    /// Provides memory pressure protection for large fragments.
    pub max_buffered_bytes: usize,

    /// Number of concurrent sends allowed per target.
    /// Controls how many fragments can be in-flight simultaneously.
    pub max_concurrent_sends: usize,
}

impl Default for PipedStreamConfig {
    fn default() -> Self {
        Self {
            // Buffer up to 100 out-of-order fragments (~140 KB at max fragment size)
            max_buffered_fragments: 100,
            // Cap total buffered bytes at 1 MB
            max_buffered_bytes: 1024 * 1024,
            // Allow 10 concurrent sends per target
            max_concurrent_sends: 10,
        }
    }
}

/// Error type for piped stream operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PipedStreamError {
    /// Buffer limit exceeded - slow consumer or network congestion.
    BufferFull {
        buffered_fragments: usize,
        buffered_bytes: usize,
    },
    /// Fragment index is invalid (0 or exceeds total).
    InvalidFragment { index: u32, total: u32 },
    /// Stream has been cancelled.
    Cancelled,
    /// Target connection failed.
    SendFailed {
        target_index: usize,
        message: String,
    },
    /// All targets have failed.
    AllTargetsFailed,
}

impl std::fmt::Display for PipedStreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PipedStreamError::BufferFull {
                buffered_fragments,
                buffered_bytes,
            } => {
                write!(
                    f,
                    "buffer full: {} fragments, {} bytes",
                    buffered_fragments, buffered_bytes
                )
            }
            PipedStreamError::InvalidFragment { index, total } => {
                write!(
                    f,
                    "invalid fragment index {} (total fragments: {})",
                    index, total
                )
            }
            PipedStreamError::Cancelled => write!(f, "stream cancelled"),
            PipedStreamError::SendFailed {
                target_index,
                message,
            } => {
                write!(f, "send to target {} failed: {}", target_index, message)
            }
            PipedStreamError::AllTargetsFailed => write!(f, "all targets failed"),
        }
    }
}

impl std::error::Error for PipedStreamError {}

/// A fragment ready to be forwarded to targets.
#[derive(Debug, Clone)]
pub(crate) struct ForwardFragment {
    /// The stream this fragment belongs to.
    pub(crate) stream_id: StreamId,
    /// Fragment number (1-indexed).
    pub(crate) fragment_number: u32,
    /// Total bytes in the complete stream.
    pub(crate) total_bytes: u64,
    /// Fragment payload.
    pub(crate) payload: Bytes,
}

/// Piped stream that forwards fragments to targets as they become available.
///
/// This struct buffers out-of-order fragments and immediately forwards
/// in-order fragments to all targets. It implements backpressure through
/// configurable limits on buffered fragments and concurrent sends.
pub struct PipedStream<T: TimeSource = RealTime> {
    /// Stream identifier.
    stream_id: StreamId,
    /// Total expected bytes in the stream.
    total_bytes: u64,
    /// Total expected fragments.
    total_fragments: u32,
    /// Next fragment number to forward (1-indexed).
    /// Fragments before this have already been forwarded.
    next_to_forward: AtomicU32,
    /// Out-of-order fragments waiting to be forwarded.
    /// Key is fragment_number, value is payload.
    ///
    /// # Design Choices
    ///
    /// - **parking_lot::Mutex**: Smaller, faster than std::sync::Mutex, no poisoning.
    ///   Poisoning isn't useful here since we don't hold state that could be corrupted.
    ///
    /// - **BTreeMap over HashMap**: O(log n) vs O(1) for individual operations, but
    ///   BTreeMap enables future optimization with `split_off()` to drain contiguous
    ///   ranges in O(log n + k) instead of O(k log n) individual removes.
    out_of_order: parking_lot::Mutex<BTreeMap<u32, Bytes>>,
    /// Current buffered bytes (for memory pressure tracking).
    buffered_bytes: AtomicU64,
    /// Configuration for limits and behavior.
    config: PipedStreamConfig,
    /// Semaphores for backpressure per target.
    /// Each semaphore limits concurrent in-flight sends to that target.
    send_semaphores: Vec<Arc<Semaphore>>,
    /// Whether the stream has been cancelled.
    cancelled: std::sync::atomic::AtomicBool,
    /// Notification for cancellation - wakes waiters in acquire_send_permit.
    cancel_notify: Notify,
    /// Time source for virtual time support in testing.
    time_source: T,
}

// Production constructor (backward-compatible, uses real time)
impl PipedStream<RealTime> {
    /// Creates a new piped stream with real time.
    ///
    /// # Arguments
    ///
    /// * `stream_id` - Unique identifier for this stream
    /// * `total_bytes` - Total expected bytes in the complete stream
    /// * `num_targets` - Number of target connections to forward to
    /// * `config` - Configuration for limits and behavior
    pub fn new(
        stream_id: StreamId,
        total_bytes: u64,
        num_targets: usize,
        config: PipedStreamConfig,
    ) -> Self {
        Self::new_with_time_source(stream_id, total_bytes, num_targets, config, RealTime::new())
    }
}

// Generic implementation (works with any TimeSource)
impl<T: TimeSource> PipedStream<T> {
    /// Creates a new piped stream with a custom time source.
    ///
    /// # Arguments
    ///
    /// * `stream_id` - Unique identifier for this stream
    /// * `total_bytes` - Total expected bytes in the complete stream
    /// * `num_targets` - Number of target connections to forward to
    /// * `config` - Configuration for limits and behavior
    /// * `time_source` - TimeSource for virtual time support (RealTime for production, VirtualTime for tests)
    pub fn new_with_time_source(
        stream_id: StreamId,
        total_bytes: u64,
        num_targets: usize,
        config: PipedStreamConfig,
        time_source: T,
    ) -> Self {
        // Calculate total fragments
        const FRAGMENT_PAYLOAD_SIZE: usize = crate::transport::packet_data::MAX_DATA_SIZE - 40;
        let total_fragments = if total_bytes == 0 {
            0
        } else {
            (total_bytes as usize).div_ceil(FRAGMENT_PAYLOAD_SIZE) as u32
        };

        // Create per-target semaphores for backpressure
        let send_semaphores = (0..num_targets)
            .map(|_| Arc::new(Semaphore::new(config.max_concurrent_sends)))
            .collect();

        Self {
            stream_id,
            total_bytes,
            total_fragments,
            next_to_forward: AtomicU32::new(1), // Fragments are 1-indexed
            out_of_order: parking_lot::Mutex::new(BTreeMap::new()),
            buffered_bytes: AtomicU64::new(0),
            config,
            send_semaphores,
            cancelled: std::sync::atomic::AtomicBool::new(false),
            cancel_notify: Notify::new(),
            time_source,
        }
    }

    /// Returns the stream ID.
    pub fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    /// Returns the total expected bytes.
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    /// Returns the total expected fragments.
    pub fn total_fragments(&self) -> u32 {
        self.total_fragments
    }

    /// Returns true if all fragments have been forwarded.
    pub fn is_complete(&self) -> bool {
        self.next_to_forward.load(Ordering::Acquire) > self.total_fragments
    }

    /// Returns the number of buffered out-of-order fragments.
    pub fn buffered_count(&self) -> usize {
        self.out_of_order.lock().len()
    }

    /// Returns the total buffered bytes.
    pub fn buffered_bytes(&self) -> u64 {
        self.buffered_bytes.load(Ordering::Relaxed)
    }

    /// Returns the next fragment number expected.
    pub fn next_expected(&self) -> u32 {
        self.next_to_forward.load(Ordering::Acquire)
    }

    /// Cancels the stream and wakes any waiters.
    pub fn cancel(&self) {
        self.cancelled
            .store(true, std::sync::atomic::Ordering::Release);
        // Wake any tasks waiting for permits
        self.cancel_notify.notify_waiters();
    }

    /// Returns true if the stream has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Pushes a fragment and returns fragments ready to forward.
    ///
    /// If the fragment is the next expected one, it's returned immediately
    /// along with any buffered fragments that are now contiguous.
    /// If out of order, it's buffered (subject to limits).
    ///
    /// # Returns
    ///
    /// A vector of fragments ready to forward, in order. Empty if the
    /// fragment was buffered or already forwarded.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The stream is cancelled
    /// - The buffer is full (backpressure)
    /// - The fragment index is invalid
    pub fn push_fragment(
        &self,
        fragment_number: u32,
        payload: Bytes,
    ) -> Result<Vec<ForwardFragment>, PipedStreamError> {
        // Check cancelled
        if self.is_cancelled() {
            return Err(PipedStreamError::Cancelled);
        }

        // Validate fragment number
        if fragment_number == 0 || fragment_number > self.total_fragments {
            return Err(PipedStreamError::InvalidFragment {
                index: fragment_number,
                total: self.total_fragments,
            });
        }

        let next = self.next_to_forward.load(Ordering::Acquire);

        // Already forwarded?
        if fragment_number < next {
            return Ok(vec![]);
        }

        // Is this the next expected fragment?
        if fragment_number == next {
            // Forward immediately and check for buffered continuations
            return self.forward_contiguous(fragment_number, payload);
        }

        // Out of order - need to buffer
        self.buffer_fragment(fragment_number, payload)?;
        Ok(vec![])
    }

    /// Buffers an out-of-order fragment.
    fn buffer_fragment(
        &self,
        fragment_number: u32,
        payload: Bytes,
    ) -> Result<(), PipedStreamError> {
        let payload_len = payload.len() as u64;

        // Check memory limits before acquiring lock
        let current_bytes = self.buffered_bytes.load(Ordering::Relaxed);
        if current_bytes + payload_len > self.config.max_buffered_bytes as u64 {
            return Err(PipedStreamError::BufferFull {
                buffered_fragments: self.out_of_order.lock().len(),
                buffered_bytes: current_bytes as usize,
            });
        }

        let mut buffer = self.out_of_order.lock();

        // Check fragment limit
        if buffer.len() >= self.config.max_buffered_fragments {
            return Err(PipedStreamError::BufferFull {
                buffered_fragments: buffer.len(),
                buffered_bytes: current_bytes as usize,
            });
        }

        // Insert (idempotent - duplicates are no-ops)
        if buffer.insert(fragment_number, payload).is_none() {
            // Only count bytes if this was a new insert
            self.buffered_bytes
                .fetch_add(payload_len, Ordering::Relaxed);
        }

        Ok(())
    }

    /// Forwards a contiguous run of fragments starting from fragment_number.
    ///
    /// # Concurrency
    ///
    /// The `next_to_forward` frontier is updated while holding the lock to prevent
    /// a race where another thread sees a stale frontier and incorrectly buffers
    /// a fragment that should be forwarded immediately.
    fn forward_contiguous(
        &self,
        fragment_number: u32,
        payload: Bytes,
    ) -> Result<Vec<ForwardFragment>, PipedStreamError> {
        let mut to_forward = Vec::new();

        // Add the incoming fragment
        to_forward.push(ForwardFragment {
            stream_id: self.stream_id,
            fragment_number,
            total_bytes: self.total_bytes,
            payload,
        });

        // Advance next_to_forward
        let mut current = fragment_number + 1;

        // Check for buffered continuations while holding the lock.
        // IMPORTANT: We update next_to_forward BEFORE releasing the lock to prevent
        // a race where another thread sees a stale frontier.
        {
            let mut buffer = self.out_of_order.lock();

            while let Some(buffered_payload) = buffer.remove(&current) {
                let payload_len = buffered_payload.len() as u64;
                self.buffered_bytes
                    .fetch_sub(payload_len, Ordering::Relaxed);

                to_forward.push(ForwardFragment {
                    stream_id: self.stream_id,
                    fragment_number: current,
                    total_bytes: self.total_bytes,
                    payload: buffered_payload,
                });
                current += 1;
            }

            // Update the frontier while still holding the lock
            self.next_to_forward.store(current, Ordering::Release);
        } // Lock released here, after frontier is updated

        Ok(to_forward)
    }

    /// Acquires a send permit for the specified target.
    ///
    /// This implements backpressure - if too many fragments are in-flight
    /// to this target, the caller will wait until a permit becomes available.
    ///
    /// # Cancellation Safety
    ///
    /// This method races permit acquisition against stream cancellation.
    /// If `cancel()` is called while waiting, this returns `Cancelled` immediately.
    ///
    /// # Returns
    ///
    /// A permit that must be held while the send is in progress.
    /// Dropping the permit signals completion of the send.
    pub async fn acquire_send_permit(
        &self,
        target_index: usize,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, PipedStreamError> {
        if self.is_cancelled() {
            return Err(PipedStreamError::Cancelled);
        }

        if target_index >= self.send_semaphores.len() {
            return Err(PipedStreamError::SendFailed {
                target_index,
                message: "invalid target index".into(),
            });
        }

        // Race permit acquisition against cancellation to avoid deadlock
        // if cancel() is called while we're waiting for a permit.
        let semaphore = self.send_semaphores[target_index].clone();
        tokio::select! {
            biased;  // Check cancellation first for faster response

            _ = self.cancel_notify.notified() => {
                Err(PipedStreamError::Cancelled)
            }

            result = semaphore.acquire_owned() => {
                // Double-check cancellation after acquiring permit
                if self.is_cancelled() {
                    return Err(PipedStreamError::Cancelled);
                }
                result.map_err(|_| PipedStreamError::Cancelled)
            }
        }
    }

    /// Returns the number of available send permits for a target.
    pub fn available_permits(&self, target_index: usize) -> usize {
        if target_index >= self.send_semaphores.len() {
            return 0;
        }
        self.send_semaphores[target_index].available_permits()
    }
}

impl<T: TimeSource> std::fmt::Debug for PipedStream<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipedStream")
            .field("stream_id", &self.stream_id)
            .field("total_bytes", &self.total_bytes)
            .field("total_fragments", &self.total_fragments)
            .field(
                "next_to_forward",
                &self.next_to_forward.load(Ordering::Relaxed),
            )
            .field("buffered_fragments", &self.out_of_order.lock().len())
            .field(
                "buffered_bytes",
                &self.buffered_bytes.load(Ordering::Relaxed),
            )
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::GlobalExecutor;
    use crate::simulation::VirtualTime;
    use std::time::Duration;

    fn make_stream_id() -> StreamId {
        StreamId::next()
    }

    #[test]
    fn test_in_order_forwarding() {
        let stream = PipedStream::new(
            make_stream_id(),
            4000, // ~3 fragments at ~1400 bytes each
            1,
            PipedStreamConfig::default(),
        );

        // Push fragment 1
        let result = stream
            .push_fragment(1, Bytes::from_static(b"fragment 1"))
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].fragment_number, 1);
        assert_eq!(stream.next_expected(), 2);

        // Push fragment 2
        let result = stream
            .push_fragment(2, Bytes::from_static(b"fragment 2"))
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].fragment_number, 2);
        assert_eq!(stream.next_expected(), 3);

        // Push fragment 3 (last)
        let result = stream
            .push_fragment(3, Bytes::from_static(b"fragment 3"))
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].fragment_number, 3);

        assert!(stream.is_complete());
        assert_eq!(stream.buffered_count(), 0);
    }

    #[test]
    fn test_out_of_order_buffering() {
        let stream = PipedStream::new(make_stream_id(), 4000, 1, PipedStreamConfig::default());

        // Push fragment 3 first (out of order)
        let result = stream
            .push_fragment(3, Bytes::from_static(b"fragment 3"))
            .unwrap();
        assert!(result.is_empty()); // Buffered
        assert_eq!(stream.buffered_count(), 1);
        assert_eq!(stream.next_expected(), 1);

        // Push fragment 2 (still out of order)
        let result = stream
            .push_fragment(2, Bytes::from_static(b"fragment 2"))
            .unwrap();
        assert!(result.is_empty()); // Buffered
        assert_eq!(stream.buffered_count(), 2);

        // Push fragment 1 - should trigger cascade
        let result = stream
            .push_fragment(1, Bytes::from_static(b"fragment 1"))
            .unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].fragment_number, 1);
        assert_eq!(result[1].fragment_number, 2);
        assert_eq!(result[2].fragment_number, 3);

        assert!(stream.is_complete());
        assert_eq!(stream.buffered_count(), 0);
    }

    #[test]
    fn test_duplicate_fragments() {
        let stream = PipedStream::new(make_stream_id(), 4000, 1, PipedStreamConfig::default());

        // Push fragment 1
        let result = stream
            .push_fragment(1, Bytes::from_static(b"fragment 1"))
            .unwrap();
        assert_eq!(result.len(), 1);

        // Push duplicate of fragment 1 - should be ignored
        let result = stream
            .push_fragment(1, Bytes::from_static(b"fragment 1 dup"))
            .unwrap();
        assert!(result.is_empty());

        // Push fragment 3 (buffer it)
        let _ = stream
            .push_fragment(3, Bytes::from_static(b"fragment 3"))
            .unwrap();
        assert_eq!(stream.buffered_count(), 1);

        // Push duplicate of buffered fragment 3 - count should stay same
        let _ = stream
            .push_fragment(3, Bytes::from_static(b"fragment 3 dup"))
            .unwrap();
        assert_eq!(stream.buffered_count(), 1);
    }

    #[test]
    fn test_buffer_fragment_limit() {
        let config = PipedStreamConfig {
            max_buffered_fragments: 2,
            max_buffered_bytes: 1024 * 1024,
            max_concurrent_sends: 10,
        };
        let stream = PipedStream::new(make_stream_id(), 100_000, 1, config);

        // Buffer 2 fragments (limit)
        stream
            .push_fragment(3, Bytes::from_static(b"frag 3"))
            .unwrap();
        stream
            .push_fragment(4, Bytes::from_static(b"frag 4"))
            .unwrap();
        assert_eq!(stream.buffered_count(), 2);

        // Try to buffer a 3rd - should fail
        let result = stream.push_fragment(5, Bytes::from_static(b"frag 5"));
        assert!(matches!(result, Err(PipedStreamError::BufferFull { .. })));
    }

    #[test]
    fn test_buffer_bytes_limit() {
        let config = PipedStreamConfig {
            max_buffered_fragments: 100,
            max_buffered_bytes: 20, // Very small limit
            max_concurrent_sends: 10,
        };
        let stream = PipedStream::new(make_stream_id(), 100_000, 1, config);

        // Buffer 10 bytes
        stream
            .push_fragment(2, Bytes::from_static(b"0123456789"))
            .unwrap();
        assert_eq!(stream.buffered_bytes(), 10);

        // Try to buffer 15 more bytes - exceeds 20 byte limit
        let result = stream.push_fragment(3, Bytes::from_static(b"012345678901234"));
        assert!(matches!(result, Err(PipedStreamError::BufferFull { .. })));
    }

    #[test]
    fn test_invalid_fragment_index() {
        let stream = PipedStream::new(make_stream_id(), 4000, 1, PipedStreamConfig::default());

        // Fragment 0 is invalid (1-indexed)
        let result = stream.push_fragment(0, Bytes::from_static(b"bad"));
        assert!(matches!(
            result,
            Err(PipedStreamError::InvalidFragment { index: 0, .. })
        ));

        // Fragment beyond total is invalid
        let result = stream.push_fragment(100, Bytes::from_static(b"bad"));
        assert!(matches!(
            result,
            Err(PipedStreamError::InvalidFragment { index: 100, .. })
        ));
    }

    #[test]
    fn test_cancellation() {
        let stream = PipedStream::new(make_stream_id(), 4000, 1, PipedStreamConfig::default());

        assert!(!stream.is_cancelled());
        stream.cancel();
        assert!(stream.is_cancelled());

        // Push after cancel should fail
        let result = stream.push_fragment(1, Bytes::from_static(b"data"));
        assert!(matches!(result, Err(PipedStreamError::Cancelled)));
    }

    #[tokio::test]
    async fn test_send_permits() {
        let config = PipedStreamConfig {
            max_buffered_fragments: 100,
            max_buffered_bytes: 1024 * 1024,
            max_concurrent_sends: 2,
        };
        let stream = PipedStream::new(make_stream_id(), 4000, 2, config);

        // Target 0: 2 permits available
        assert_eq!(stream.available_permits(0), 2);

        // Acquire one
        let permit1 = stream.acquire_send_permit(0).await.unwrap();
        assert_eq!(stream.available_permits(0), 1);

        // Acquire another
        let permit2 = stream.acquire_send_permit(0).await.unwrap();
        assert_eq!(stream.available_permits(0), 0);

        // Drop one - permit returns
        drop(permit1);
        assert_eq!(stream.available_permits(0), 1);

        // Target 1 is independent
        assert_eq!(stream.available_permits(1), 2);

        drop(permit2);
    }

    #[test]
    fn test_buffered_bytes_tracking() {
        let stream = PipedStream::new(make_stream_id(), 100_000, 1, PipedStreamConfig::default());

        // Buffer some fragments
        stream
            .push_fragment(2, Bytes::from_static(b"12345"))
            .unwrap(); // 5 bytes
        stream
            .push_fragment(3, Bytes::from_static(b"1234567890"))
            .unwrap(); // 10 bytes
        assert_eq!(stream.buffered_bytes(), 15);

        // Forward fragment 1 - should cascade and free buffered bytes
        let result = stream.push_fragment(1, Bytes::from_static(b"abc")).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(stream.buffered_bytes(), 0);
        assert_eq!(stream.buffered_count(), 0);
    }

    #[test]
    fn test_partial_cascade() {
        // Use 8500 bytes which results in 6 fragments at ~1424 bytes/fragment
        // ceil(8500 / 1424) = 6 fragments
        let stream = PipedStream::new(make_stream_id(), 8500, 1, PipedStreamConfig::default());
        assert_eq!(stream.total_fragments(), 6);

        // Buffer fragments 3, 5, 6 (gap at 4)
        stream
            .push_fragment(3, Bytes::from_static(b"frag 3"))
            .unwrap();
        stream
            .push_fragment(5, Bytes::from_static(b"frag 5"))
            .unwrap();
        stream
            .push_fragment(6, Bytes::from_static(b"frag 6"))
            .unwrap();
        assert_eq!(stream.buffered_count(), 3);

        // Push 1, 2 - should cascade to 3 but stop at gap
        stream
            .push_fragment(1, Bytes::from_static(b"frag 1"))
            .unwrap();
        let result = stream
            .push_fragment(2, Bytes::from_static(b"frag 2"))
            .unwrap();
        assert_eq!(result.len(), 2); // 2 and 3
        assert_eq!(result[0].fragment_number, 2);
        assert_eq!(result[1].fragment_number, 3);
        assert_eq!(stream.next_expected(), 4);
        assert_eq!(stream.buffered_count(), 2); // 5, 6 still buffered

        // Push 4 - should cascade through 5, 6
        let result = stream
            .push_fragment(4, Bytes::from_static(b"frag 4"))
            .unwrap();
        assert_eq!(result.len(), 3); // 4, 5, 6
        assert!(stream.is_complete());
    }

    #[tokio::test]
    async fn test_concurrent_push_fragments() {
        use std::sync::Arc;

        // Create a stream with 20 fragments
        let stream = Arc::new(PipedStream::new(
            make_stream_id(),
            28000, // ~20 fragments
            1,
            PipedStreamConfig::default(),
        ));

        let num_fragments = stream.total_fragments();
        assert!(num_fragments >= 10); // Sanity check

        // Spawn multiple tasks to push fragments concurrently
        let mut handles = Vec::new();
        for frag_num in 1..=num_fragments {
            let stream = Arc::clone(&stream);
            handles.push(GlobalExecutor::spawn(async move {
                let payload = Bytes::from(format!("fragment {}", frag_num));
                stream.push_fragment(frag_num, payload)
            }));
        }

        // Wait for all to complete
        let mut total_forwarded = 0;
        for handle in handles {
            if let Ok(Ok(fragments)) = handle.await {
                total_forwarded += fragments.len();
            }
        }

        // All fragments should have been forwarded exactly once
        assert_eq!(total_forwarded, num_fragments as usize);
        assert!(stream.is_complete());
        assert_eq!(stream.buffered_count(), 0);
    }

    #[tokio::test]
    async fn test_cancel_during_permit_wait() {
        use std::sync::Arc;

        // Create stream with only 1 concurrent send allowed
        let config = PipedStreamConfig {
            max_buffered_fragments: 100,
            max_buffered_bytes: 1024 * 1024,
            max_concurrent_sends: 1,
        };
        let time_source = VirtualTime::new();
        let stream = Arc::new(PipedStream::new_with_time_source(
            make_stream_id(),
            4000,
            1,
            config,
            time_source.clone(),
        ));

        // Acquire the only permit
        let _permit = stream.acquire_send_permit(0).await.unwrap();
        assert_eq!(stream.available_permits(0), 0);

        // Spawn a task that tries to acquire a permit (will block)
        let stream_clone = Arc::clone(&stream);
        let waiter = GlobalExecutor::spawn(async move { stream_clone.acquire_send_permit(0).await });

        // Give the waiter time to start waiting using virtual time
        time_source.advance(Duration::from_millis(10));

        // Cancel the stream - should wake the waiter
        stream.cancel();

        // The waiter should return Cancelled, not hang
        // Use a select with a timeout to ensure the waiter completes quickly
        let result = tokio::select! {
            waiter_result = waiter => {
                Some(waiter_result.expect("task should not panic"))
            }
            _ = time_source.sleep(Duration::from_millis(100)) => {
                None
            }
        };

        assert!(result.is_some(), "waiter should not timeout");
        assert!(matches!(result.unwrap(), Err(PipedStreamError::Cancelled)));
    }

    #[tokio::test]
    async fn test_concurrent_push_and_cancel() {
        use std::sync::Arc;

        let time_source = VirtualTime::new();
        let stream = Arc::new(PipedStream::new_with_time_source(
            make_stream_id(),
            14000, // ~10 fragments
            1,
            PipedStreamConfig::default(),
            time_source.clone(),
        ));

        // Start pushing fragments
        let stream_clone = Arc::clone(&stream);
        let time_source_clone = time_source.clone();
        let pusher = GlobalExecutor::spawn(async move {
            for i in 1..=10 {
                if stream_clone.is_cancelled() {
                    break;
                }
                let _ = stream_clone.push_fragment(i, Bytes::from(format!("frag {}", i)));
                time_source_clone.sleep(Duration::from_millis(1)).await;
            }
        });

        // Cancel after a short delay using virtual time
        time_source.advance(Duration::from_millis(5));
        stream.cancel();

        // Wait for pusher to finish
        let _ = pusher.await;

        // Stream should be cancelled
        assert!(stream.is_cancelled());

        // Further pushes should fail
        let result = stream.push_fragment(1, Bytes::from_static(b"data"));
        assert!(matches!(result, Err(PipedStreamError::Cancelled)));
    }

    #[test]
    fn test_buffer_full_recovery() {
        // Test that BufferFull can be recovered from by filling gaps
        let config = PipedStreamConfig {
            max_buffered_fragments: 2,
            max_buffered_bytes: 1024 * 1024,
            max_concurrent_sends: 10,
        };
        let stream = PipedStream::new(make_stream_id(), 10_000, 1, config);

        // Buffer 2 fragments (at limit)
        stream
            .push_fragment(3, Bytes::from_static(b"frag 3"))
            .unwrap();
        stream
            .push_fragment(4, Bytes::from_static(b"frag 4"))
            .unwrap();
        assert_eq!(stream.buffered_count(), 2);

        // Try to buffer a 3rd - should fail with BufferFull
        let result = stream.push_fragment(5, Bytes::from_static(b"frag 5"));
        assert!(matches!(result, Err(PipedStreamError::BufferFull { .. })));

        // Now fill the gap - this should cascade and free buffer space
        stream
            .push_fragment(1, Bytes::from_static(b"frag 1"))
            .unwrap();
        let result = stream
            .push_fragment(2, Bytes::from_static(b"frag 2"))
            .unwrap();

        // Fragments 2, 3, 4 should have been forwarded
        assert_eq!(result.len(), 3);
        assert_eq!(stream.buffered_count(), 0);

        // Now we can buffer again
        stream
            .push_fragment(6, Bytes::from_static(b"frag 6"))
            .unwrap();
        assert_eq!(stream.buffered_count(), 1);

        // And the previously rejected fragment 5 should work
        let result = stream
            .push_fragment(5, Bytes::from_static(b"frag 5"))
            .unwrap();
        assert_eq!(result.len(), 2); // 5 and 6
    }

    #[tokio::test]
    async fn test_all_permits_exhausted_then_released() {
        use std::sync::Arc;

        let config = PipedStreamConfig {
            max_buffered_fragments: 100,
            max_buffered_bytes: 1024 * 1024,
            max_concurrent_sends: 2,
        };
        let stream = Arc::new(PipedStream::new(make_stream_id(), 4000, 1, config));

        // Exhaust all permits
        let permit1 = stream.acquire_send_permit(0).await.unwrap();
        let permit2 = stream.acquire_send_permit(0).await.unwrap();
        assert_eq!(stream.available_permits(0), 0);

        // Spawn a waiter
        let stream_clone = Arc::clone(&stream);
        let waiter = GlobalExecutor::spawn(async move { stream_clone.acquire_send_permit(0).await });

        // Release one permit
        drop(permit1);

        // Waiter should succeed
        let result = waiter.await.unwrap();
        assert!(result.is_ok());

        // Clean up
        drop(permit2);
    }
}

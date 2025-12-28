//! Streaming infrastructure for incremental fragment consumption.
//!
//! This module provides the `StreamingInboundStream` type which implements
//! `futures::Stream` for incremental access to incoming data. Unlike the
//! existing `InboundStream` which waits for complete reassembly, this
//! allows consumers to process data as it arrives.
//!
//! # Components
//!
//! - [`StreamingInboundStream`]: A `futures::Stream` over incoming fragment data
//! - [`StreamHandle`]: A cloneable handle for accessing an inbound stream
//! - [`StreamRegistry`]: Global registry for transport-to-operations handoff
//!
//! # Usage
//!
//! ```ignore
//! // Get a handle from the registry
//! let handle = registry.get_stream(stream_id).await?;
//!
//! // Create a streaming view
//! let stream = handle.stream();
//!
//! // Process fragments incrementally
//! while let Some(result) = stream.next().await {
//!     match result {
//!         Ok(bytes) => process_chunk(bytes),
//!         Err(e) => handle_error(e),
//!     }
//! }
//! ```

use bytes::Bytes;
use dashmap::DashMap;
use futures::Stream;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use tokio::sync::mpsc;

use super::streaming_buffer::LockFreeStreamBuffer;
use super::StreamId;

/// Error type for streaming operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamError {
    /// Stream was cancelled or connection closed.
    Cancelled,
    /// Stream not found in registry.
    NotFound,
    /// Invalid fragment received.
    InvalidFragment { message: String },
}

impl std::fmt::Display for StreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamError::Cancelled => write!(f, "stream was cancelled"),
            StreamError::NotFound => write!(f, "stream not found in registry"),
            StreamError::InvalidFragment { message } => {
                write!(f, "invalid fragment: {}", message)
            }
        }
    }
}

impl std::error::Error for StreamError {}

/// Synchronization state for stream handles.
///
/// The lock-free buffer is stored separately (as Arc<LockFreeStreamBuffer>)
/// so that fragment insertion doesn't require holding this lock.
/// This struct only holds state that requires synchronization.
struct SyncState {
    /// True if the stream has been cancelled.
    cancelled: bool,
    /// Wakers for poll_next calls waiting on data.
    wakers: Vec<Waker>,
}

impl SyncState {
    fn new() -> Self {
        Self {
            cancelled: false,
            wakers: Vec::new(),
        }
    }

    fn wake_all(&mut self) {
        for waker in self.wakers.drain(..) {
            waker.wake();
        }
    }
}

/// A handle to an inbound stream that can be cloned for multiple consumers.
///
/// Each clone maintains its own read position, allowing independent consumers
/// to read the stream at different rates.
///
/// The design separates the lock-free buffer from synchronization state:
/// - `buffer`: Lock-free fragment storage using `OnceLock<Bytes>` slots
/// - `sync`: Cancelled flag and wakers (requires lock)
///
/// Fragment insertion goes directly to the buffer without acquiring any lock,
/// and the buffer's built-in `Notify` handles async notification.
#[derive(Clone)]
pub struct StreamHandle {
    /// Lock-free buffer for fragment storage.
    /// Accessed without holding any lock - all operations are atomic.
    buffer: Arc<LockFreeStreamBuffer>,
    /// Synchronization state (cancelled, wakers) - requires lock.
    sync: Arc<parking_lot::RwLock<SyncState>>,
    /// Stream ID for debugging.
    stream_id: StreamId,
    /// Total expected bytes.
    total_bytes: u64,
}

impl StreamHandle {
    /// Creates a new stream handle.
    fn new(stream_id: StreamId, total_bytes: u64) -> Self {
        Self {
            buffer: Arc::new(LockFreeStreamBuffer::new(total_bytes)),
            sync: Arc::new(parking_lot::RwLock::new(SyncState::new())),
            stream_id,
            total_bytes,
        }
    }

    /// Returns the stream ID.
    #[allow(dead_code)]
    pub(crate) fn stream_id(&self) -> StreamId {
        self.stream_id
    }

    /// Returns the total expected bytes.
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    /// Returns true if all fragments have been received.
    pub fn is_complete(&self) -> bool {
        self.buffer.is_complete()
    }

    /// Returns the number of fragments received so far.
    pub fn received_fragments(&self) -> usize {
        self.buffer.inserted_count()
    }

    /// Returns the total expected number of fragments.
    pub fn total_fragments(&self) -> usize {
        self.buffer.total_fragments()
    }

    /// Creates a new streaming view starting from fragment 1.
    ///
    /// Each call creates an independent stream with its own read position.
    /// Fragments are cloned but remain in the buffer, allowing multiple
    /// consumers to read the same data via `fork()`.
    ///
    /// # Memory Behavior
    ///
    /// All fragments remain in the buffer until the stream is dropped or
    /// explicitly cleared. This means:
    /// - **Pro**: Multiple consumers can read the same data independently
    /// - **Pro**: Safe to use with `fork()` for parallel processing
    /// - **Con**: Memory usage equals full stream size until completion
    ///
    /// # When to Use
    ///
    /// Use `stream()` when:
    /// - Multiple consumers need to read the same data
    /// - You need to fork the stream for parallel processing
    /// - Memory is not a concern for the stream size
    /// - You might need to re-read fragments
    ///
    /// For single-consumer scenarios with large streams, prefer
    /// [`stream_with_reclaim()`](Self::stream_with_reclaim) for better memory efficiency.
    pub fn stream(&self) -> StreamingInboundStream {
        StreamingInboundStream {
            handle: self.clone(),
            next_fragment: 1,
            bytes_read: 0,
            auto_reclaim: false,
        }
    }

    /// Creates a streaming view with automatic memory reclamation.
    ///
    /// Unlike [`stream()`](Self::stream), this version takes ownership of
    /// fragments as they are read, freeing memory progressively.
    ///
    /// # Memory Behavior
    ///
    /// Each fragment is removed from the buffer immediately after being read:
    /// - **Pro**: Memory usage stays constant regardless of stream size
    /// - **Pro**: Ideal for processing large streams (10MB+) without OOM
    /// - **Con**: Fragments cannot be read again once consumed
    /// - **Con**: Incompatible with `fork()` or multiple consumers
    ///
    /// # When to Use
    ///
    /// Use `stream_with_reclaim()` when:
    /// - Only one consumer will read the stream
    /// - The stream is large and memory is a concern
    /// - Data is processed once and discarded (e.g., forwarding, hashing)
    /// - You don't need `fork()` or parallel consumers
    ///
    /// # Warning
    ///
    /// **Do not use with `fork()` or multiple consumers.** Once a fragment is
    /// read by this stream, it is permanently removed from the buffer. Other
    /// consumers (including forked handles) will wait forever for fragments
    /// that have already been consumed.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Single consumer processing a large file
    /// let mut stream = handle.stream_with_reclaim();
    /// let mut hasher = Sha256::new();
    /// while let Some(chunk) = stream.next().await {
    ///     hasher.update(&chunk?);
    ///     // Memory for this chunk is freed immediately
    /// }
    /// ```
    pub fn stream_with_reclaim(&self) -> StreamingInboundStream {
        StreamingInboundStream {
            handle: self.clone(),
            next_fragment: 1,
            bytes_read: 0,
            auto_reclaim: true,
        }
    }

    /// Forks this handle, creating an independent consumer.
    ///
    /// The forked handle shares the same underlying buffer but maintains
    /// its own read position when used with `.stream()`.
    pub fn fork(&self) -> Self {
        self.clone()
    }

    /// Inserts a fragment into the stream buffer.
    ///
    /// This is called by the transport layer when a fragment arrives.
    /// The insert operation is lock-free - only the cancelled check and
    /// waker notification require synchronization.
    ///
    /// # Returns
    ///
    /// * `Ok(true)` - Fragment was inserted successfully
    /// * `Ok(false)` - Fragment was a duplicate (no-op)
    /// * `Err(StreamError)` - Invalid fragment index or stream cancelled
    pub(crate) fn push_fragment(
        &self,
        fragment_index: u32,
        data: Bytes,
    ) -> Result<bool, StreamError> {
        // Quick check if cancelled (read lock only)
        if self.sync.read().cancelled {
            return Err(StreamError::Cancelled);
        }

        // Lock-free insert into buffer
        // The buffer's internal Notify handles async notification
        match self.buffer.insert(fragment_index, data) {
            Ok(inserted) => {
                if inserted {
                    // Wake synchronous waiters (poll_next wakers)
                    self.sync.write().wake_all();
                }
                Ok(inserted)
            }
            Err(e) => Err(StreamError::InvalidFragment {
                message: e.to_string(),
            }),
        }
    }

    /// Cancels the stream, waking all waiters.
    pub(crate) fn cancel(&self) {
        let mut sync = self.sync.write();
        sync.cancelled = true;
        sync.wake_all();
        drop(sync); // Release lock before notifying

        // Wake async waiters blocked on buffer.notifier().listen()
        self.buffer.notifier().notify(usize::MAX);
    }

    /// Assembles the complete data if all fragments are present.
    ///
    /// This is a convenience method for waiting until the stream is complete
    /// and then getting all data at once.
    pub fn try_assemble(&self) -> Option<Vec<u8>> {
        self.buffer.assemble()
    }

    /// Waits for the stream to complete and returns the assembled data.
    ///
    /// This is useful when you don't need incremental processing.
    /// Uses the buffer's built-in Notify for efficient async waiting.
    pub async fn assemble(&self) -> Result<Vec<u8>, StreamError> {
        loop {
            // Check cancelled state
            if self.sync.read().cancelled {
                return Err(StreamError::Cancelled);
            }

            // Try to assemble (lock-free access to buffer)
            if self.buffer.is_complete() {
                if let Some(data) = self.buffer.assemble() {
                    return Ok(data);
                }
                return Err(StreamError::InvalidFragment {
                    message: "assembly failed".into(),
                });
            }

            // Wait for notification using buffer's lock-free Event
            // This is safe because we're not holding any lock across the await
            self.buffer.notifier().listen().await;
        }
    }
}

impl std::fmt::Debug for StreamHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamHandle")
            .field("stream_id", &self.stream_id)
            .field("total_bytes", &self.total_bytes)
            .field("received", &self.buffer.inserted_count())
            .field("total_fragments", &self.buffer.total_fragments())
            .field("complete", &self.buffer.is_complete())
            .field("cancelled", &self.sync.read().cancelled)
            .finish()
    }
}

/// A `futures::Stream` that yields contiguous fragments as they become available.
///
/// This stream yields `Bytes` chunks in order, waiting for out-of-order
/// fragments to arrive before yielding subsequent chunks.
///
/// # Memory Behavior
///
/// By default (`auto_reclaim = false`), fragments are cloned but remain in the buffer,
/// allowing multiple consumers to read the same data via `fork()`.
///
/// With `auto_reclaim = true`, fragments are taken from the buffer after reading,
/// freeing memory progressively. This is ideal for single-consumer scenarios with
/// large streams.
pub struct StreamingInboundStream {
    /// Handle to the shared stream state.
    handle: StreamHandle,
    /// Next fragment index to yield (1-indexed).
    next_fragment: u32,
    /// Total bytes read so far.
    bytes_read: u64,
    /// If true, fragments are taken (removed) from the buffer after reading.
    /// This enables progressive memory reclamation for single-consumer scenarios.
    auto_reclaim: bool,
}

impl StreamingInboundStream {
    /// Returns the next fragment index that will be yielded.
    #[allow(dead_code)]
    pub fn next_fragment_index(&self) -> u32 {
        self.next_fragment
    }

    /// Returns the total bytes read so far.
    #[allow(dead_code)]
    pub fn bytes_read(&self) -> u64 {
        self.bytes_read
    }

    /// Returns the stream ID.
    #[allow(dead_code)]
    pub(crate) fn stream_id(&self) -> StreamId {
        self.handle.stream_id
    }

    /// Returns whether auto-reclaim is enabled.
    #[allow(dead_code)]
    pub fn is_auto_reclaim(&self) -> bool {
        self.auto_reclaim
    }

    /// Tries to get the next fragment, either taking or cloning based on auto_reclaim.
    fn try_get_fragment(&self, idx: u32) -> Option<Bytes> {
        if self.auto_reclaim {
            // Take ownership and clear the slot
            self.handle.buffer.take(idx)
        } else {
            // Clone, leaving the fragment in place for other consumers
            self.handle.buffer.get(idx).cloned()
        }
    }
}

impl Stream for StreamingInboundStream {
    type Item = Result<Bytes, StreamError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let next_idx = self.next_fragment;

        // Check cancelled state (sync lock)
        if self.handle.sync.read().cancelled {
            return Poll::Ready(Some(Err(StreamError::Cancelled)));
        }

        // Lock-free access to buffer
        let total_fragments = self.handle.buffer.total_fragments() as u32;

        // Check if we've read all fragments
        if next_idx > total_fragments {
            return Poll::Ready(None); // Stream complete
        }

        // Try to get the next fragment (lock-free)
        if let Some(data) = self.try_get_fragment(next_idx) {
            self.next_fragment = next_idx + 1;
            self.bytes_read += data.len() as u64;
            return Poll::Ready(Some(Ok(data)));
        }

        // Fragment not yet available, register waker
        {
            let mut sync = self.handle.sync.write();
            // Re-check cancelled after acquiring write lock
            if sync.cancelled {
                return Poll::Ready(Some(Err(StreamError::Cancelled)));
            }
            // Re-check buffer (fragment may have arrived)
            if let Some(data) = self.try_get_fragment(next_idx) {
                drop(sync); // Release lock before modifying self
                self.next_fragment = next_idx + 1;
                self.bytes_read += data.len() as u64;
                return Poll::Ready(Some(Ok(data)));
            }
            sync.wakers.push(cx.waker().clone());
        }

        Poll::Pending
    }
}

/// Global registry for managing active inbound streams.
///
/// The registry provides the handoff point between the transport layer
/// (which receives fragments) and the operations layer (which consumes them).
///
/// Uses `DashMap` for lock-free concurrent access, avoiding the global
/// bottleneck of `tokio::sync::RwLock<HashMap>`.
pub struct StreamRegistry {
    /// Active streams indexed by stream ID.
    /// Uses DashMap for lock-free concurrent access.
    streams: DashMap<StreamId, StreamHandle>,
    /// Channel for notifying about new streams.
    #[allow(dead_code)]
    new_stream_tx: mpsc::Sender<StreamId>,
    /// Receiver for new stream notifications (held by listener).
    #[allow(dead_code)]
    new_stream_rx: parking_lot::Mutex<Option<mpsc::Receiver<StreamId>>>,
}

impl StreamRegistry {
    /// Creates a new stream registry.
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(64);
        Self {
            streams: DashMap::new(),
            new_stream_tx: tx,
            new_stream_rx: parking_lot::Mutex::new(Some(rx)),
        }
    }

    /// Registers a new stream with the given ID and expected size.
    ///
    /// Returns a handle for pushing fragments to the stream.
    ///
    /// If a stream with this ID already exists, returns the existing handle.
    pub(crate) async fn register(&self, stream_id: StreamId, total_bytes: u64) -> StreamHandle {
        // Use entry API to avoid race conditions
        let handle = self
            .streams
            .entry(stream_id)
            .or_insert_with(|| StreamHandle::new(stream_id, total_bytes))
            .clone();

        // Notify listeners about the new stream (ignore send errors)
        let _ = self.new_stream_tx.send(stream_id).await;

        handle
    }

    /// Gets a handle to an existing stream.
    ///
    /// Returns `None` if the stream doesn't exist.
    #[allow(dead_code)]
    pub(crate) fn get(&self, stream_id: StreamId) -> Option<StreamHandle> {
        self.streams.get(&stream_id).map(|r| r.clone())
    }

    /// Removes a stream from the registry.
    ///
    /// This should be called when a stream is complete or cancelled
    /// to prevent memory leaks.
    #[allow(dead_code)]
    pub(crate) fn remove(&self, stream_id: StreamId) -> Option<StreamHandle> {
        self.streams.remove(&stream_id).map(|(_, h)| h)
    }

    /// Takes the new stream receiver for listening to new stream registrations.
    ///
    /// This can only be called once. Subsequent calls return `None`.
    #[allow(dead_code)]
    pub(crate) fn take_new_stream_receiver(&self) -> Option<mpsc::Receiver<StreamId>> {
        self.new_stream_rx.lock().take()
    }

    /// Returns the number of active streams.
    pub fn stream_count(&self) -> usize {
        self.streams.len()
    }

    /// Cancels all streams and clears the registry.
    ///
    /// This prevents memory leaks by removing all entries after cancellation.
    pub fn cancel_all(&self) {
        // Cancel all streams first
        for entry in self.streams.iter() {
            entry.value().cancel();
        }
        // Clear the registry to prevent memory leaks
        self.streams.clear();
    }
}

impl Default for StreamRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    fn make_stream_id() -> StreamId {
        StreamId::next()
    }

    #[test]
    fn test_stream_handle_creation() {
        let id = make_stream_id();
        let handle = StreamHandle::new(id, 1000);

        assert_eq!(handle.stream_id(), id);
        assert_eq!(handle.total_bytes(), 1000);
        assert!(!handle.is_complete());
    }

    #[test]
    fn test_stream_handle_push_fragment() {
        let handle = StreamHandle::new(make_stream_id(), 100);

        let result = handle.push_fragment(1, Bytes::from_static(b"hello"));
        assert!(result.is_ok());
        assert!(result.unwrap()); // First insert succeeds

        // Duplicate is no-op
        let result = handle.push_fragment(1, Bytes::from_static(b"world"));
        assert!(result.is_ok());
        assert!(!result.unwrap()); // Duplicate returns false
    }

    #[test]
    fn test_stream_handle_invalid_fragment() {
        let handle = StreamHandle::new(make_stream_id(), 100);

        // Index 0 is invalid
        let result = handle.push_fragment(0, Bytes::from_static(b"hello"));
        assert!(matches!(result, Err(StreamError::InvalidFragment { .. })));

        // Index 2 is out of bounds (only 1 fragment expected)
        let result = handle.push_fragment(2, Bytes::from_static(b"hello"));
        assert!(matches!(result, Err(StreamError::InvalidFragment { .. })));
    }

    #[test]
    fn test_stream_handle_cancel() {
        let handle = StreamHandle::new(make_stream_id(), 100);

        handle.cancel();

        // Pushing after cancel should fail
        let result = handle.push_fragment(1, Bytes::from_static(b"hello"));
        assert!(matches!(result, Err(StreamError::Cancelled)));
    }

    #[test]
    fn test_stream_handle_fork() {
        let handle = StreamHandle::new(make_stream_id(), 100);
        handle
            .push_fragment(1, Bytes::from_static(b"hello"))
            .unwrap();

        let forked = handle.fork();

        // Both handles see the same data
        assert!(handle.is_complete());
        assert!(forked.is_complete());
        assert_eq!(handle.try_assemble(), forked.try_assemble());
    }

    #[tokio::test]
    async fn test_streaming_inbound_stream_basic() {
        let handle = StreamHandle::new(make_stream_id(), 15);

        // Push all fragments
        handle
            .push_fragment(1, Bytes::from_static(b"hello"))
            .unwrap();

        let mut stream = handle.stream();
        let chunk = stream.next().await;
        assert!(chunk.is_some());
        assert_eq!(chunk.unwrap().unwrap(), Bytes::from_static(b"hello"));

        // Stream should be exhausted
        let chunk = stream.next().await;
        assert!(chunk.is_none());
    }

    #[tokio::test]
    async fn test_streaming_inbound_stream_incremental() {
        use super::super::streaming_buffer::FRAGMENT_PAYLOAD_SIZE;

        let total = (FRAGMENT_PAYLOAD_SIZE * 3) as u64;
        let handle = StreamHandle::new(make_stream_id(), total);
        let mut stream = handle.stream();

        // Spawn a task to push fragments with delays
        let handle_clone = handle.clone();
        let producer = tokio::spawn(async move {
            for i in 1..=3 {
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                handle_clone
                    .push_fragment(i, Bytes::from(vec![i as u8; FRAGMENT_PAYLOAD_SIZE]))
                    .unwrap();
            }
        });

        // Consume incrementally
        let mut count = 0;
        while let Some(result) = stream.next().await {
            count += 1;
            let bytes = result.unwrap();
            assert_eq!(bytes.len(), FRAGMENT_PAYLOAD_SIZE);
        }

        producer.await.unwrap();
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn test_streaming_cancelled() {
        let handle = StreamHandle::new(make_stream_id(), 100);
        let mut stream = handle.stream();

        // Cancel the stream
        handle.cancel();

        // Next poll should return Cancelled error
        let result = stream.next().await;
        assert!(matches!(result, Some(Err(StreamError::Cancelled))));
    }

    #[tokio::test]
    async fn test_stream_registry_register_and_get() {
        let registry = StreamRegistry::new();
        let id = make_stream_id();

        let handle = registry.register(id, 1000).await;
        assert_eq!(handle.stream_id(), id);

        let retrieved = registry.get(id);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().stream_id(), id);
    }

    #[tokio::test]
    async fn test_stream_registry_register_existing() {
        let registry = StreamRegistry::new();
        let id = make_stream_id();

        let handle1 = registry.register(id, 1000).await;
        let handle2 = registry.register(id, 2000).await; // Different size

        // Should return the same handle (first registration wins)
        assert_eq!(handle1.total_bytes(), handle2.total_bytes());
    }

    #[tokio::test]
    async fn test_stream_registry_remove() {
        let registry = StreamRegistry::new();
        let id = make_stream_id();

        registry.register(id, 1000).await;
        assert_eq!(registry.stream_count(), 1);

        let removed = registry.remove(id);
        assert!(removed.is_some());
        assert_eq!(registry.stream_count(), 0);

        // Get should return None after removal
        assert!(registry.get(id).is_none());
    }

    #[tokio::test]
    async fn test_stream_registry_new_stream_notifications() {
        let registry = StreamRegistry::new();
        let mut rx = registry.take_new_stream_receiver().unwrap();

        let id1 = make_stream_id();
        let id2 = make_stream_id();

        registry.register(id1, 100).await;
        registry.register(id2, 200).await;

        // Should receive notifications
        let notified_id1 = rx.recv().await.unwrap();
        let notified_id2 = rx.recv().await.unwrap();

        assert!(notified_id1 == id1 || notified_id1 == id2);
        assert!(notified_id2 == id1 || notified_id2 == id2);
        assert_ne!(notified_id1, notified_id2);
    }

    #[tokio::test]
    async fn test_stream_handle_assemble_async() {
        use super::super::streaming_buffer::FRAGMENT_PAYLOAD_SIZE;

        let total = (FRAGMENT_PAYLOAD_SIZE * 2) as u64;
        let handle = StreamHandle::new(make_stream_id(), total);

        // Spawn producer
        let handle_clone = handle.clone();
        let producer = tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            handle_clone
                .push_fragment(1, Bytes::from(vec![1u8; FRAGMENT_PAYLOAD_SIZE]))
                .unwrap();
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            handle_clone
                .push_fragment(2, Bytes::from(vec![2u8; FRAGMENT_PAYLOAD_SIZE]))
                .unwrap();
        });

        // Wait for complete assembly
        let result = handle.assemble().await;
        producer.await.unwrap();

        assert!(result.is_ok());
        let data = result.unwrap();
        assert_eq!(data.len(), total as usize);
    }

    #[tokio::test]
    async fn test_multiple_independent_streams() {
        use super::super::streaming_buffer::FRAGMENT_PAYLOAD_SIZE;

        let total = (FRAGMENT_PAYLOAD_SIZE * 2) as u64;
        let handle = StreamHandle::new(make_stream_id(), total);

        // Push first fragment
        handle
            .push_fragment(1, Bytes::from(vec![1u8; FRAGMENT_PAYLOAD_SIZE]))
            .unwrap();

        // Create two independent streams
        let mut stream1 = handle.stream();
        let mut stream2 = handle.stream();

        // Both can read the first fragment independently
        let chunk1 = stream1.next().await.unwrap().unwrap();
        let chunk2 = stream2.next().await.unwrap().unwrap();
        assert_eq!(chunk1, chunk2);

        // Both are now waiting for fragment 2
        assert_eq!(stream1.next_fragment_index(), 2);
        assert_eq!(stream2.next_fragment_index(), 2);

        // Push second fragment
        handle
            .push_fragment(2, Bytes::from(vec![2u8; FRAGMENT_PAYLOAD_SIZE]))
            .unwrap();

        // Both can read independently
        let chunk1 = stream1.next().await.unwrap().unwrap();
        let chunk2 = stream2.next().await.unwrap().unwrap();
        assert_eq!(chunk1, chunk2);

        // Both streams are exhausted
        assert!(stream1.next().await.is_none());
        assert!(stream2.next().await.is_none());
    }

    #[test]
    fn test_zero_byte_stream() {
        let handle = StreamHandle::new(make_stream_id(), 0);

        assert_eq!(handle.total_bytes(), 0);
        assert_eq!(handle.total_fragments(), 0);
        assert!(handle.is_complete()); // Empty stream is complete
        assert_eq!(handle.try_assemble(), Some(vec![]));
    }

    #[tokio::test]
    async fn test_zero_byte_stream_streaming() {
        let handle = StreamHandle::new(make_stream_id(), 0);
        let mut stream = handle.stream();

        // Should immediately return None (no fragments)
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_assemble_cancelled_stream() {
        let handle = StreamHandle::new(make_stream_id(), 100);
        handle.cancel();

        let result = handle.assemble().await;
        assert!(matches!(result, Err(StreamError::Cancelled)));
    }

    #[tokio::test]
    async fn test_cancel_during_assemble() {
        let handle = StreamHandle::new(make_stream_id(), 100);
        let handle_clone = handle.clone();

        // Start assemble in background
        let assemble_task = tokio::spawn(async move { handle_clone.assemble().await });

        // Give it time to start waiting
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Cancel the stream
        handle.cancel();

        // Assemble should return Cancelled
        let result = tokio::time::timeout(tokio::time::Duration::from_millis(100), assemble_task)
            .await
            .expect("timeout")
            .expect("join");
        assert!(matches!(result, Err(StreamError::Cancelled)));
    }

    #[tokio::test]
    async fn test_poll_after_stream_exhausted() {
        let handle = StreamHandle::new(make_stream_id(), 5);
        handle
            .push_fragment(1, Bytes::from_static(b"hello"))
            .unwrap();

        let mut stream = handle.stream();

        // Read the only fragment
        let chunk = stream.next().await;
        assert!(chunk.is_some());

        // Multiple polls after exhaustion should all return None
        assert!(stream.next().await.is_none());
        assert!(stream.next().await.is_none());
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_out_of_order_fragments_stream_waits() {
        use super::super::streaming_buffer::FRAGMENT_PAYLOAD_SIZE;

        let total = (FRAGMENT_PAYLOAD_SIZE * 3) as u64;
        let handle = StreamHandle::new(make_stream_id(), total);

        // Push fragment 3 first (out of order)
        handle
            .push_fragment(3, Bytes::from(vec![3u8; FRAGMENT_PAYLOAD_SIZE]))
            .unwrap();

        let mut stream = handle.stream();

        // Create a task that tries to read - should block waiting for fragment 1
        let handle_clone = handle.clone();
        let read_task = tokio::spawn(async move {
            let mut s = handle_clone.stream();
            s.next().await
        });

        // Give it time to block
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Task should still be pending
        assert!(!read_task.is_finished());

        // Now push fragment 1
        handle
            .push_fragment(1, Bytes::from(vec![1u8; FRAGMENT_PAYLOAD_SIZE]))
            .unwrap();

        // Read task should complete
        let result = tokio::time::timeout(tokio::time::Duration::from_millis(100), read_task)
            .await
            .expect("timeout")
            .expect("join");
        assert!(result.is_some());
        assert!(result.unwrap().is_ok());

        // Now we should be able to read from our stream too
        let chunk = stream.next().await;
        assert!(chunk.is_some());
    }

    #[tokio::test]
    async fn test_cancel_while_streaming() {
        use super::super::streaming_buffer::FRAGMENT_PAYLOAD_SIZE;

        let total = (FRAGMENT_PAYLOAD_SIZE * 3) as u64;
        let handle = StreamHandle::new(make_stream_id(), total);

        // Push first fragment
        handle
            .push_fragment(1, Bytes::from(vec![1u8; FRAGMENT_PAYLOAD_SIZE]))
            .unwrap();

        let mut stream = handle.stream();

        // Read first fragment successfully
        let chunk = stream.next().await;
        assert!(chunk.is_some());
        assert!(chunk.unwrap().is_ok());

        // Cancel while waiting for second fragment
        let handle_clone = handle.clone();
        let cancel_task = tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            handle_clone.cancel();
        });

        // Try to read - should eventually return Cancelled
        let result = stream.next().await;
        assert!(matches!(result, Some(Err(StreamError::Cancelled))));

        cancel_task.await.unwrap();
    }

    #[tokio::test]
    async fn test_bytes_read_tracking() {
        let handle = StreamHandle::new(make_stream_id(), 15);
        handle
            .push_fragment(1, Bytes::from_static(b"hello world!!!"))
            .unwrap();

        let mut stream = handle.stream();
        assert_eq!(stream.bytes_read(), 0);

        let chunk = stream.next().await.unwrap().unwrap();
        assert_eq!(stream.bytes_read(), chunk.len() as u64);
    }

    #[tokio::test]
    async fn test_registry_cancel_all() {
        let registry = StreamRegistry::new();

        let id1 = make_stream_id();
        let id2 = make_stream_id();

        let handle1 = registry.register(id1, 100).await;
        let handle2 = registry.register(id2, 200).await;

        // Cancel all (now clears the registry too)
        registry.cancel_all();

        // Both should be cancelled
        let result1 = handle1.push_fragment(1, Bytes::from_static(b"test"));
        let result2 = handle2.push_fragment(1, Bytes::from_static(b"test"));

        assert!(matches!(result1, Err(StreamError::Cancelled)));
        assert!(matches!(result2, Err(StreamError::Cancelled)));

        // Registry should be empty (memory leak fix)
        assert_eq!(registry.stream_count(), 0);
    }

    #[tokio::test]
    async fn test_registry_cleanup_on_normal_completion() {
        // This test verifies that streams can be properly removed from the registry
        // when they complete normally (simulating the cleanup done in peer_connection.rs)
        let registry = StreamRegistry::new();

        let id1 = make_stream_id();
        let id2 = make_stream_id();
        let id3 = make_stream_id();

        // Register multiple streams
        let handle1 = registry.register(id1, 100).await;
        let _handle2 = registry.register(id2, 200).await;
        let _handle3 = registry.register(id3, 300).await;

        assert_eq!(registry.stream_count(), 3);

        // Complete stream 1's data
        handle1
            .push_fragment(1, Bytes::from_static(b"complete data here!"))
            .unwrap();
        assert!(handle1.is_complete());

        // Simulate normal completion cleanup (as done in peer_connection.rs)
        let removed = registry.remove(id1);
        assert!(removed.is_some());
        assert_eq!(registry.stream_count(), 2);

        // Remove another stream
        let removed = registry.remove(id2);
        assert!(removed.is_some());
        assert_eq!(registry.stream_count(), 1);

        // Remove last stream
        let removed = registry.remove(id3);
        assert!(removed.is_some());
        assert_eq!(registry.stream_count(), 0);

        // Registry should be completely empty - no memory leak
        assert!(registry.get(id1).is_none());
        assert!(registry.get(id2).is_none());
        assert!(registry.get(id3).is_none());
    }

    #[test]
    fn test_take_receiver_twice() {
        let registry = StreamRegistry::new();

        let rx1 = registry.take_new_stream_receiver();
        let rx2 = registry.take_new_stream_receiver();

        assert!(rx1.is_some());
        assert!(rx2.is_none()); // Second call returns None
    }

    #[test]
    fn test_registry_get_nonexistent() {
        let registry = StreamRegistry::new();
        let id = make_stream_id();

        let result = registry.get(id);
        assert!(result.is_none());
    }

    #[test]
    fn test_registry_remove_nonexistent() {
        let registry = StreamRegistry::new();
        let id = make_stream_id();

        let result = registry.remove(id);
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_concurrent_push_and_stream() {
        use super::super::streaming_buffer::FRAGMENT_PAYLOAD_SIZE;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let total = (FRAGMENT_PAYLOAD_SIZE * 10) as u64;
        let handle = StreamHandle::new(make_stream_id(), total);
        let fragments_received = Arc::new(AtomicUsize::new(0));

        // Spawn producer
        let handle_producer = handle.clone();
        let producer = tokio::spawn(async move {
            for i in 1..=10 {
                tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;
                handle_producer
                    .push_fragment(i, Bytes::from(vec![i as u8; FRAGMENT_PAYLOAD_SIZE]))
                    .unwrap();
            }
        });

        // Spawn multiple consumers
        let mut consumers = Vec::new();
        for _ in 0..3 {
            let h = handle.clone();
            let counter = Arc::clone(&fragments_received);
            consumers.push(tokio::spawn(async move {
                let mut stream = h.stream();
                let mut local_count = 0;
                while let Some(result) = stream.next().await {
                    result.unwrap();
                    local_count += 1;
                }
                counter.fetch_add(local_count, Ordering::SeqCst);
                local_count
            }));
        }

        producer.await.unwrap();

        // Each consumer should read all 10 fragments
        for consumer in consumers {
            let count = consumer.await.unwrap();
            assert_eq!(count, 10);
        }
    }

    #[test]
    fn test_stream_handle_debug() {
        let handle = StreamHandle::new(make_stream_id(), 100);
        handle
            .push_fragment(1, Bytes::from_static(b"test"))
            .unwrap();

        let debug_str = format!("{:?}", handle);
        assert!(debug_str.contains("StreamHandle"));
        assert!(debug_str.contains("total_bytes"));
        assert!(debug_str.contains("complete"));
    }

    #[tokio::test]
    async fn test_try_assemble_before_complete() {
        use super::super::streaming_buffer::FRAGMENT_PAYLOAD_SIZE;

        let total = (FRAGMENT_PAYLOAD_SIZE * 2) as u64;
        let handle = StreamHandle::new(make_stream_id(), total);

        // Push only first fragment
        handle
            .push_fragment(1, Bytes::from(vec![1u8; FRAGMENT_PAYLOAD_SIZE]))
            .unwrap();

        // try_assemble should return None (incomplete)
        assert!(handle.try_assemble().is_none());

        // Push second fragment
        handle
            .push_fragment(2, Bytes::from(vec![2u8; FRAGMENT_PAYLOAD_SIZE]))
            .unwrap();

        // Now try_assemble should work
        let assembled = handle.try_assemble();
        assert!(assembled.is_some());
        assert_eq!(assembled.unwrap().len(), total as usize);
    }

    #[test]
    fn test_stream_error_display() {
        let cancelled = StreamError::Cancelled;
        assert_eq!(format!("{}", cancelled), "stream was cancelled");

        let not_found = StreamError::NotFound;
        assert_eq!(format!("{}", not_found), "stream not found in registry");

        let invalid = StreamError::InvalidFragment {
            message: "test error".into(),
        };
        assert_eq!(format!("{}", invalid), "invalid fragment: test error");
    }

    // ==================== Auto-Reclaim Tests ====================

    #[tokio::test]
    async fn test_stream_with_reclaim_basic() {
        use super::super::streaming_buffer::FRAGMENT_PAYLOAD_SIZE;

        let total = (FRAGMENT_PAYLOAD_SIZE * 3) as u64;
        let handle = StreamHandle::new(make_stream_id(), total);

        // Push all fragments
        for i in 1..=3 {
            handle
                .push_fragment(i, Bytes::from(vec![i as u8; FRAGMENT_PAYLOAD_SIZE]))
                .unwrap();
        }

        assert_eq!(handle.buffer.inserted_count(), 3);

        // Use reclaiming stream
        let mut stream = handle.stream_with_reclaim();
        assert!(stream.is_auto_reclaim());

        // Read first fragment - should be removed from buffer
        let chunk = stream.next().await.unwrap().unwrap();
        assert_eq!(chunk[0], 1);
        assert_eq!(handle.buffer.inserted_count(), 2); // One removed

        // Read remaining fragments
        let _ = stream.next().await.unwrap().unwrap();
        let _ = stream.next().await.unwrap().unwrap();

        assert_eq!(handle.buffer.inserted_count(), 0); // All removed

        // Stream is exhausted
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_stream_without_reclaim_preserves_data() {
        use super::super::streaming_buffer::FRAGMENT_PAYLOAD_SIZE;

        let total = (FRAGMENT_PAYLOAD_SIZE * 2) as u64;
        let handle = StreamHandle::new(make_stream_id(), total);

        for i in 1..=2 {
            handle
                .push_fragment(i, Bytes::from(vec![i as u8; FRAGMENT_PAYLOAD_SIZE]))
                .unwrap();
        }

        // Use non-reclaiming stream (default)
        let mut stream = handle.stream();
        assert!(!stream.is_auto_reclaim());

        // Read all fragments
        let _ = stream.next().await.unwrap().unwrap();
        let _ = stream.next().await.unwrap().unwrap();

        // Fragments still present in buffer
        assert_eq!(handle.buffer.inserted_count(), 2);
    }

    #[tokio::test]
    async fn test_reclaim_incremental_with_delayed_fragments() {
        use super::super::streaming_buffer::FRAGMENT_PAYLOAD_SIZE;

        let total = (FRAGMENT_PAYLOAD_SIZE * 3) as u64;
        let handle = StreamHandle::new(make_stream_id(), total);
        let mut stream = handle.stream_with_reclaim();

        // Push and consume fragment 1
        handle
            .push_fragment(1, Bytes::from(vec![1u8; FRAGMENT_PAYLOAD_SIZE]))
            .unwrap();

        let chunk = stream.next().await.unwrap().unwrap();
        assert_eq!(chunk[0], 1);
        assert_eq!(handle.buffer.inserted_count(), 0);

        // Push fragments 2 and 3
        handle
            .push_fragment(2, Bytes::from(vec![2u8; FRAGMENT_PAYLOAD_SIZE]))
            .unwrap();
        handle
            .push_fragment(3, Bytes::from(vec![3u8; FRAGMENT_PAYLOAD_SIZE]))
            .unwrap();

        assert_eq!(handle.buffer.inserted_count(), 2);

        // Consume remaining
        let _ = stream.next().await.unwrap().unwrap();
        assert_eq!(handle.buffer.inserted_count(), 1);

        let _ = stream.next().await.unwrap().unwrap();
        assert_eq!(handle.buffer.inserted_count(), 0);
    }

    #[tokio::test]
    async fn test_reclaim_vs_fork_conflict() {
        use super::super::streaming_buffer::FRAGMENT_PAYLOAD_SIZE;

        let total = (FRAGMENT_PAYLOAD_SIZE * 2) as u64;
        let handle = StreamHandle::new(make_stream_id(), total);

        for i in 1..=2 {
            handle
                .push_fragment(i, Bytes::from(vec![i as u8; FRAGMENT_PAYLOAD_SIZE]))
                .unwrap();
        }

        // First consumer uses reclaim
        let mut reclaim_stream = handle.stream_with_reclaim();
        let _ = reclaim_stream.next().await.unwrap().unwrap(); // Takes fragment 1

        // Second consumer (forked) tries to read - fragment 1 is gone!
        let forked = handle.fork();
        let _forked_stream = forked.stream();

        // Fragment 1 was taken, so forked stream gets None (waits for it)
        // This demonstrates why auto_reclaim should only be used with single consumers
        // The forked stream would wait forever for fragment 1 which is gone

        // Fragment 2 is still there though
        assert!(handle.buffer.get(2).is_some());
    }

    #[tokio::test]
    async fn test_stream_memory_efficiency() {
        use super::super::streaming_buffer::FRAGMENT_PAYLOAD_SIZE;

        // Simulate a large stream
        let num_fragments = 20u32;
        let total = (FRAGMENT_PAYLOAD_SIZE * num_fragments as usize) as u64;
        let handle = StreamHandle::new(make_stream_id(), total);
        let handle_clone = handle.clone();

        // Producer pushes all fragments
        for i in 1..=num_fragments {
            handle
                .push_fragment(i, Bytes::from(vec![i as u8; FRAGMENT_PAYLOAD_SIZE]))
                .unwrap();
        }

        assert_eq!(handle_clone.buffer.inserted_count(), num_fragments as usize);

        // Consumer with reclaim
        let mut stream = handle_clone.stream_with_reclaim();

        // Read half the fragments
        for _ in 0..10 {
            let _ = stream.next().await.unwrap().unwrap();
        }

        // Memory for first half is freed
        assert_eq!(handle.buffer.inserted_count(), 10);

        // Read remaining
        for _ in 0..10 {
            let _ = stream.next().await.unwrap().unwrap();
        }

        // All memory freed
        assert_eq!(handle.buffer.inserted_count(), 0);
    }
}

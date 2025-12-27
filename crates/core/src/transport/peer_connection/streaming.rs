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
use futures::Stream;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use tokio::sync::{mpsc, Notify, RwLock};

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

/// Internal state shared between stream handles and the producer.
struct StreamState {
    /// The underlying lock-free buffer.
    buffer: LockFreeStreamBuffer,
    /// True if the stream has been cancelled.
    cancelled: bool,
    /// Wakers for poll_next calls waiting on data.
    wakers: Vec<Waker>,
}

impl StreamState {
    fn new(total_bytes: u64) -> Self {
        Self {
            buffer: LockFreeStreamBuffer::new(total_bytes),
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
#[derive(Clone)]
pub struct StreamHandle {
    /// Shared state with the producer and other consumers.
    state: Arc<parking_lot::RwLock<StreamState>>,
    /// Notification for when new fragments arrive (separate from state for async-safety).
    notify: Arc<Notify>,
    /// Stream ID for debugging.
    stream_id: StreamId,
    /// Total expected bytes.
    total_bytes: u64,
}

impl StreamHandle {
    /// Creates a new stream handle.
    fn new(stream_id: StreamId, total_bytes: u64) -> Self {
        Self {
            state: Arc::new(parking_lot::RwLock::new(StreamState::new(total_bytes))),
            notify: Arc::new(Notify::new()),
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
        self.state.read().buffer.is_complete()
    }

    /// Returns the number of fragments received so far.
    pub fn received_fragments(&self) -> usize {
        self.state.read().buffer.inserted_count()
    }

    /// Returns the total expected number of fragments.
    pub fn total_fragments(&self) -> usize {
        self.state.read().buffer.total_fragments()
    }

    /// Creates a new streaming view starting from fragment 1.
    ///
    /// Each call creates an independent stream with its own read position.
    pub fn stream(&self) -> StreamingInboundStream {
        StreamingInboundStream {
            handle: self.clone(),
            next_fragment: 1,
            bytes_read: 0,
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
    ///
    /// # Returns
    ///
    /// * `Ok(true)` - Fragment was inserted successfully
    /// * `Ok(false)` - Fragment was a duplicate (no-op)
    /// * `Err(StreamError)` - Invalid fragment index
    pub(crate) fn push_fragment(
        &self,
        fragment_index: u32,
        data: Bytes,
    ) -> Result<bool, StreamError> {
        let mut state = self.state.write();
        if state.cancelled {
            return Err(StreamError::Cancelled);
        }

        match state.buffer.insert(fragment_index, data) {
            Ok(inserted) => {
                if inserted {
                    state.wake_all();
                    // Notify async waiters (outside lock)
                    drop(state);
                    self.notify.notify_waiters();
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
        let mut state = self.state.write();
        state.cancelled = true;
        state.wake_all();
        // Notify async waiters (outside lock)
        drop(state);
        self.notify.notify_waiters();
    }

    /// Assembles the complete data if all fragments are present.
    ///
    /// This is a convenience method for waiting until the stream is complete
    /// and then getting all data at once.
    pub fn try_assemble(&self) -> Option<Vec<u8>> {
        self.state.read().buffer.assemble()
    }

    /// Waits for the stream to complete and returns the assembled data.
    ///
    /// This is useful when you don't need incremental processing.
    pub async fn assemble(&self) -> Result<Vec<u8>, StreamError> {
        loop {
            // Check state and try to assemble
            {
                let state = self.state.read();
                if state.cancelled {
                    return Err(StreamError::Cancelled);
                }
                if state.buffer.is_complete() {
                    // Assemble while holding the lock
                    if let Some(data) = state.buffer.assemble() {
                        return Ok(data);
                    }
                    return Err(StreamError::InvalidFragment {
                        message: "assembly failed".into(),
                    });
                }
            }
            // Lock is released here

            // Wait for notification of new fragments
            // Using the separate Arc<Notify> that can be borrowed across await
            self.notify.notified().await;
        }
    }
}

impl std::fmt::Debug for StreamHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.read();
        f.debug_struct("StreamHandle")
            .field("stream_id", &self.stream_id)
            .field("total_bytes", &self.total_bytes)
            .field("received", &state.buffer.inserted_count())
            .field("total_fragments", &state.buffer.total_fragments())
            .field("complete", &state.buffer.is_complete())
            .field("cancelled", &state.cancelled)
            .finish()
    }
}

/// A `futures::Stream` that yields contiguous fragments as they become available.
///
/// This stream yields `Bytes` chunks in order, waiting for out-of-order
/// fragments to arrive before yielding subsequent chunks.
pub struct StreamingInboundStream {
    /// Handle to the shared stream state.
    handle: StreamHandle,
    /// Next fragment index to yield (1-indexed).
    next_fragment: u32,
    /// Total bytes read so far.
    bytes_read: u64,
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
}

impl Stream for StreamingInboundStream {
    type Item = Result<Bytes, StreamError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let next_idx = self.next_fragment;

        // First check with read lock
        let (cancelled, total_fragments, maybe_data) = {
            let state = self.handle.state.read();
            let cancelled = state.cancelled;
            let total = state.buffer.total_fragments() as u32;
            let data = state.buffer.get(next_idx).cloned();
            (cancelled, total, data)
        };
        // Lock is now released

        // Check if stream is cancelled
        if cancelled {
            return Poll::Ready(Some(Err(StreamError::Cancelled)));
        }

        // Check if we've read all fragments
        if next_idx > total_fragments {
            return Poll::Ready(None); // Stream complete
        }

        // Try to get the next fragment (already fetched above)
        if let Some(data) = maybe_data {
            self.next_fragment = next_idx + 1;
            self.bytes_read += data.len() as u64;
            return Poll::Ready(Some(Ok(data)));
        }

        // Fragment not yet available, need write lock to register waker
        {
            let mut state = self.handle.state.write();
            // Re-check conditions after acquiring write lock (double-checked locking)
            if state.cancelled {
                return Poll::Ready(Some(Err(StreamError::Cancelled)));
            }
            if let Some(data) = state.buffer.get(next_idx).cloned() {
                // Fragment arrived while we were waiting for the lock
                drop(state); // Release lock before modifying self
                self.next_fragment = next_idx + 1;
                self.bytes_read += data.len() as u64;
                return Poll::Ready(Some(Ok(data)));
            }
            state.wakers.push(cx.waker().clone());
        }

        Poll::Pending
    }
}

/// Global registry for managing active inbound streams.
///
/// The registry provides the handoff point between the transport layer
/// (which receives fragments) and the operations layer (which consumes them).
pub struct StreamRegistry {
    /// Active streams indexed by stream ID.
    streams: RwLock<HashMap<StreamId, StreamHandle>>,
    /// Channel for notifying about new streams.
    #[allow(dead_code)]
    new_stream_tx: mpsc::Sender<StreamId>,
    /// Receiver for new stream notifications (held by listener).
    #[allow(dead_code)]
    new_stream_rx: RwLock<Option<mpsc::Receiver<StreamId>>>,
}

impl StreamRegistry {
    /// Creates a new stream registry.
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(64);
        Self {
            streams: RwLock::new(HashMap::new()),
            new_stream_tx: tx,
            new_stream_rx: RwLock::new(Some(rx)),
        }
    }

    /// Registers a new stream with the given ID and expected size.
    ///
    /// Returns a handle for pushing fragments to the stream.
    ///
    /// If a stream with this ID already exists, returns the existing handle.
    pub(crate) async fn register(
        &self,
        stream_id: StreamId,
        total_bytes: u64,
    ) -> StreamHandle {
        let mut streams = self.streams.write().await;

        if let Some(handle) = streams.get(&stream_id) {
            return handle.clone();
        }

        let handle = StreamHandle::new(stream_id, total_bytes);
        streams.insert(stream_id, handle.clone());

        // Notify listeners about the new stream (ignore send errors)
        let _ = self.new_stream_tx.send(stream_id).await;

        handle
    }

    /// Gets a handle to an existing stream.
    ///
    /// Returns `None` if the stream doesn't exist.
    #[allow(dead_code)]
    pub(crate) async fn get(&self, stream_id: StreamId) -> Option<StreamHandle> {
        self.streams.read().await.get(&stream_id).cloned()
    }

    /// Removes a stream from the registry.
    ///
    /// This should be called when a stream is complete or cancelled.
    #[allow(dead_code)]
    pub(crate) async fn remove(&self, stream_id: StreamId) -> Option<StreamHandle> {
        self.streams.write().await.remove(&stream_id)
    }

    /// Takes the new stream receiver for listening to new stream registrations.
    ///
    /// This can only be called once. Subsequent calls return `None`.
    #[allow(dead_code)]
    pub(crate) async fn take_new_stream_receiver(&self) -> Option<mpsc::Receiver<StreamId>> {
        self.new_stream_rx.write().await.take()
    }

    /// Returns the number of active streams.
    pub async fn stream_count(&self) -> usize {
        self.streams.read().await.len()
    }

    /// Cancels all streams and clears the registry.
    pub async fn cancel_all(&self) {
        let streams = self.streams.write().await;
        for handle in streams.values() {
            handle.cancel();
        }
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

        let retrieved = registry.get(id).await;
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
        assert_eq!(registry.stream_count().await, 1);

        let removed = registry.remove(id).await;
        assert!(removed.is_some());
        assert_eq!(registry.stream_count().await, 0);

        // Get should return None after removal
        assert!(registry.get(id).await.is_none());
    }

    #[tokio::test]
    async fn test_stream_registry_new_stream_notifications() {
        let registry = StreamRegistry::new();
        let mut rx = registry.take_new_stream_receiver().await.unwrap();

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
}

//! Orphan stream handling for race conditions.
//!
//! Streams may arrive before their metadata message due to network reordering.
//! This module provides a registry for "claiming" orphan streams when metadata arrives.
//!
//! # Race Condition Handling
//!
//! Two orderings are possible:
//!
//! 1. **Stream arrives first**: Transport registers stream as orphan. When `RequestStreaming`
//!    or `ResponseStreaming` arrives at operations layer, it claims the orphan.
//!
//! 2. **Metadata arrives first**: Operations layer registers a waiter. When stream
//!    fragments arrive at transport, the waiter is notified.
//!
//! # Usage
//!
//! ```ignore
//! // Transport layer: first fragment for unknown stream
//! orphan_registry.register_orphan(stream_id, handle);
//!
//! // Operations layer: metadata message arrives
//! let handle = orphan_registry.claim_or_wait(stream_id, timeout).await?;
//! ```

use std::time::{Duration, Instant};

use dashmap::DashMap;
use tokio::sync::oneshot;

use crate::transport::peer_connection::streaming::StreamHandle;
use crate::transport::peer_connection::StreamId;

/// Timeout for unclaimed orphan streams.
/// Orphan streams not claimed within this duration are garbage collected.
#[allow(dead_code)] // Phase 3 infrastructure - will be used when gc_expired is called periodically
pub const ORPHAN_STREAM_TIMEOUT: Duration = Duration::from_secs(30);

/// Default timeout when waiting for a stream to arrive after metadata.
#[allow(dead_code)] // Phase 3 infrastructure - will be used by streaming handlers
pub const STREAM_CLAIM_TIMEOUT: Duration = Duration::from_secs(10);

/// Registry for handling race conditions between stream fragments and metadata messages.
///
/// This registry enables safe handoff between the transport layer (which receives
/// stream fragments) and the operations layer (which receives metadata messages).
pub struct OrphanStreamRegistry {
    /// Streams awaiting metadata (arrived before RequestStreaming/ResponseStreaming).
    /// Maps StreamId -> (StreamHandle, timestamp when registered).
    orphan_streams: DashMap<StreamId, (StreamHandle, Instant)>,

    /// Waiters for streams that haven't arrived yet (metadata arrived first).
    /// Maps StreamId -> oneshot sender to deliver the StreamHandle.
    stream_waiters: DashMap<StreamId, oneshot::Sender<StreamHandle>>,
}

impl OrphanStreamRegistry {
    /// Creates a new empty registry.
    pub fn new() -> Self {
        Self {
            orphan_streams: DashMap::new(),
            stream_waiters: DashMap::new(),
        }
    }

    /// Register an orphan stream (stream arrived before metadata).
    ///
    /// If someone is already waiting for this stream, the handle is delivered
    /// immediately. Otherwise, it's stored as an orphan until claimed or timeout.
    #[allow(dead_code)] // Phase 3 infrastructure - will be used when transport registers orphans
    pub fn register_orphan(&self, stream_id: StreamId, handle: StreamHandle) {
        // Check if someone is already waiting for this stream
        if let Some((_, waiter)) = self.stream_waiters.remove(&stream_id) {
            // Deliver to the waiter immediately
            if waiter.send(handle).is_err() {
                tracing::warn!(
                    stream_id = %stream_id,
                    "Failed to deliver orphan stream to waiter (receiver dropped)"
                );
            } else {
                tracing::debug!(
                    stream_id = %stream_id,
                    "Delivered stream to waiting operation"
                );
            }
        } else {
            // Store as orphan for later claim
            tracing::debug!(
                stream_id = %stream_id,
                "Registered orphan stream (metadata not yet received)"
            );
            self.orphan_streams
                .insert(stream_id, (handle, Instant::now()));
        }
    }

    /// Try to claim an orphan stream, or register to wait for it.
    ///
    /// If the stream is already registered as an orphan, returns it immediately.
    /// Otherwise, waits up to `timeout` for the stream to arrive.
    ///
    /// # Errors
    ///
    /// Returns `OrphanStreamError::Timeout` if the stream doesn't arrive within
    /// the timeout period.
    #[allow(dead_code)] // Phase 3 infrastructure - will be used by streaming handlers
    pub async fn claim_or_wait(
        &self,
        stream_id: StreamId,
        timeout: Duration,
    ) -> Result<StreamHandle, OrphanStreamError> {
        // Check if orphan exists
        if let Some((_, (handle, _))) = self.orphan_streams.remove(&stream_id) {
            tracing::debug!(
                stream_id = %stream_id,
                "Claimed orphan stream immediately"
            );
            return Ok(handle);
        }

        // Register waiter
        let (tx, rx) = oneshot::channel();
        self.stream_waiters.insert(stream_id, tx);

        tracing::debug!(
            stream_id = %stream_id,
            timeout_ms = timeout.as_millis(),
            "Waiting for stream to arrive"
        );

        // Wait with timeout
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(handle)) => {
                tracing::debug!(
                    stream_id = %stream_id,
                    "Stream arrived while waiting"
                );
                Ok(handle)
            }
            Ok(Err(_)) => {
                // Sender was dropped (shouldn't happen in normal operation)
                self.stream_waiters.remove(&stream_id);
                tracing::warn!(
                    stream_id = %stream_id,
                    "Stream waiter cancelled unexpectedly"
                );
                Err(OrphanStreamError::WaiterCancelled)
            }
            Err(_) => {
                // Timeout expired
                self.stream_waiters.remove(&stream_id);
                tracing::warn!(
                    stream_id = %stream_id,
                    timeout_ms = timeout.as_millis(),
                    "Timeout waiting for stream"
                );
                Err(OrphanStreamError::Timeout)
            }
        }
    }

    /// Garbage collect expired orphan streams.
    ///
    /// Should be called periodically to clean up orphan streams that were
    /// never claimed. Each expired stream's handle is cancelled.
    #[allow(dead_code)] // Phase 3 infrastructure - will be called from a periodic cleanup task
    pub fn gc_expired(&self) {
        let now = Instant::now();
        let mut expired_count = 0;

        self.orphan_streams.retain(|stream_id, (handle, created)| {
            if now.duration_since(*created) > ORPHAN_STREAM_TIMEOUT {
                tracing::debug!(
                    stream_id = %stream_id,
                    age_secs = now.duration_since(*created).as_secs(),
                    "Garbage collecting expired orphan stream"
                );
                handle.cancel();
                expired_count += 1;
                false
            } else {
                true
            }
        });

        if expired_count > 0 {
            tracing::info!(
                expired_count,
                remaining = self.orphan_streams.len(),
                "Garbage collected expired orphan streams"
            );
        }
    }

    /// Returns the number of orphan streams currently registered.
    #[allow(dead_code)]
    pub fn orphan_count(&self) -> usize {
        self.orphan_streams.len()
    }

    /// Returns the number of waiters currently registered.
    #[allow(dead_code)]
    pub fn waiter_count(&self) -> usize {
        self.stream_waiters.len()
    }
}

impl Default for OrphanStreamRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Errors that can occur when claiming a stream.
#[allow(dead_code)] // Phase 3 infrastructure - will be used by streaming handlers
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrphanStreamError {
    /// Timeout waiting for stream to arrive.
    Timeout,
    /// Waiter was cancelled (sender dropped unexpectedly).
    WaiterCancelled,
}

impl std::fmt::Display for OrphanStreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OrphanStreamError::Timeout => write!(f, "timeout waiting for stream"),
            OrphanStreamError::WaiterCancelled => write!(f, "stream waiter was cancelled"),
        }
    }
}

impl std::error::Error for OrphanStreamError {}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper to create a test StreamHandle
    fn make_test_handle(stream_id: StreamId) -> StreamHandle {
        StreamHandle::new(stream_id, 1000)
    }

    #[test]
    fn test_orphan_registry_new() {
        let registry = OrphanStreamRegistry::new();
        assert_eq!(registry.orphan_count(), 0);
        assert_eq!(registry.waiter_count(), 0);
    }

    #[tokio::test]
    async fn test_orphan_claim_immediate() {
        let registry = OrphanStreamRegistry::new();
        let stream_id = StreamId::next();
        let handle = make_test_handle(stream_id);

        // Register orphan
        registry.register_orphan(stream_id, handle);
        assert_eq!(registry.orphan_count(), 1);

        // Claim immediately
        let claimed = registry
            .claim_or_wait(stream_id, Duration::from_secs(1))
            .await;
        assert!(claimed.is_ok());
        assert_eq!(registry.orphan_count(), 0);
    }

    #[tokio::test]
    async fn test_orphan_wait_then_register() {
        let registry = std::sync::Arc::new(OrphanStreamRegistry::new());
        let stream_id = StreamId::next();

        // Start waiting in background
        let registry_clone = registry.clone();
        let waiter = tokio::spawn(async move {
            registry_clone
                .claim_or_wait(stream_id, Duration::from_secs(5))
                .await
        });

        // Small delay to ensure waiter is registered
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(registry.waiter_count(), 1);

        // Register orphan (should deliver to waiter)
        let handle = make_test_handle(stream_id);
        registry.register_orphan(stream_id, handle);

        // Waiter should succeed
        let result = waiter.await.unwrap();
        assert!(result.is_ok());
        assert_eq!(registry.waiter_count(), 0);
    }

    #[tokio::test]
    async fn test_orphan_timeout() {
        let registry = OrphanStreamRegistry::new();
        let stream_id = StreamId::next();

        // Try to claim non-existent stream with short timeout
        let result = registry
            .claim_or_wait(stream_id, Duration::from_millis(50))
            .await;

        assert!(matches!(result, Err(OrphanStreamError::Timeout)));
    }

    #[test]
    fn test_gc_expired() {
        let registry = OrphanStreamRegistry::new();
        let stream_id = StreamId::next();
        let handle = make_test_handle(stream_id);

        // Insert with fake old timestamp by directly manipulating
        registry.orphan_streams.insert(
            stream_id,
            (handle, Instant::now() - Duration::from_secs(60)),
        );

        assert_eq!(registry.orphan_count(), 1);

        // GC should remove expired stream
        registry.gc_expired();
        assert_eq!(registry.orphan_count(), 0);
    }

    #[test]
    fn test_gc_preserves_fresh() {
        let registry = OrphanStreamRegistry::new();
        let stream_id = StreamId::next();
        let handle = make_test_handle(stream_id);

        // Register fresh orphan
        registry.register_orphan(stream_id, handle);
        assert_eq!(registry.orphan_count(), 1);

        // GC should preserve fresh stream
        registry.gc_expired();
        assert_eq!(registry.orphan_count(), 1);
    }
}

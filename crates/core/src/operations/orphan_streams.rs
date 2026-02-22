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
//! orphan_registry.register_orphan(peer_addr, stream_id, handle);
//!
//! // Operations layer: metadata message arrives
//! let handle = orphan_registry.claim_or_wait(peer_addr, stream_id, timeout).await?;
//! ```
//!
//! # Integration
//!
//! - Transport layer (`PeerConnection`) calls `register_orphan()` when streams arrive
//! - Operations handlers call `claim_or_wait()` when metadata arrives
//! - Periodic GC task cleans up expired orphans via `gc_expired()`

use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::Instant;

use dashmap::DashMap;
use tokio::sync::oneshot;

use crate::transport::peer_connection::streaming::StreamHandle;
use crate::transport::peer_connection::StreamId;

/// Timeout for unclaimed orphan streams.
/// Orphan streams not claimed within this duration are garbage collected.
/// Must be >= STREAM_CLAIM_TIMEOUT to avoid races where a waiter registers
/// just as the orphan is being cleaned up.
pub const ORPHAN_STREAM_TIMEOUT: Duration = Duration::from_secs(60);

/// Default timeout when waiting for a stream to arrive after metadata.
///
/// On resource-constrained CI runners, stream fragments can be delayed
/// significantly due to CPU contention (8 nodes doing WASM compilation
/// simultaneously), transport-level rate limiting, and Docker NAT overhead.
/// 60 seconds provides enough headroom while still failing promptly on
/// genuinely broken connections.
pub const STREAM_CLAIM_TIMEOUT: Duration = Duration::from_secs(60);

/// Registry for handling race conditions between stream fragments and metadata messages.
///
/// This registry enables safe handoff between the transport layer (which receives
/// stream fragments) and the operations layer (which receives metadata messages).
/// Key combining peer address and stream ID for collision-free lookups.
///
/// StreamIds are generated from thread-local counters per sender node, so
/// different peers can independently generate the same StreamId. Scoping
/// by `(SocketAddr, StreamId)` prevents collisions when two peers send
/// streams with identical IDs to the same receiver.
type StreamKey = (SocketAddr, StreamId);

pub struct OrphanStreamRegistry {
    /// Streams awaiting metadata (arrived before RequestStreaming/ResponseStreaming).
    /// Maps (peer_addr, StreamId) -> (StreamHandle, timestamp when registered).
    orphan_streams: DashMap<StreamKey, (StreamHandle, Instant)>,

    /// Waiters for streams that haven't arrived yet (metadata arrived first).
    /// Maps (peer_addr, StreamId) -> oneshot sender to deliver the StreamHandle.
    stream_waiters: DashMap<StreamKey, oneshot::Sender<StreamHandle>>,

    /// Streams that have already been claimed. Used for deduplication when
    /// both the embedded metadata (in fragment #1) and the separate metadata
    /// message arrive — only the first one should be processed.
    claimed_streams: DashMap<StreamKey, ()>,
}

impl OrphanStreamRegistry {
    /// Creates a new empty registry.
    pub fn new() -> Self {
        Self {
            orphan_streams: DashMap::new(),
            stream_waiters: DashMap::new(),
            claimed_streams: DashMap::new(),
        }
    }

    /// Register an orphan stream (stream arrived before metadata).
    ///
    /// If someone is already waiting for this stream, the handle is delivered
    /// immediately. Otherwise, it's stored as an orphan until claimed or timeout.
    ///
    /// `peer_addr` is the remote address of the peer that sent this stream,
    /// used to scope lookups and prevent StreamId collisions across peers.
    pub fn register_orphan(
        &self,
        peer_addr: SocketAddr,
        stream_id: StreamId,
        handle: StreamHandle,
    ) {
        let key = (peer_addr, stream_id);
        // Check if someone is already waiting for this stream
        if let Some((_, waiter)) = self.stream_waiters.remove(&key) {
            // Deliver to the waiter immediately
            if waiter.send(handle).is_err() {
                tracing::warn!(
                    %peer_addr,
                    stream_id = %stream_id,
                    "Failed to deliver orphan stream to waiter (receiver dropped)"
                );
            } else {
                tracing::debug!(
                    %peer_addr,
                    stream_id = %stream_id,
                    "Delivered stream to waiting operation"
                );
            }
        } else {
            // Store as orphan for later claim
            tracing::debug!(
                %peer_addr,
                stream_id = %stream_id,
                "Registered orphan stream (metadata not yet received)"
            );
            self.orphan_streams.insert(key, (handle, Instant::now()));
        }
    }

    /// Try to claim an orphan stream, or register to wait for it.
    ///
    /// This method is atomic with respect to deduplication: if the stream has
    /// already been claimed (e.g., via embedded metadata in fragment #1),
    /// returns `AlreadyClaimed` immediately without waiting.
    ///
    /// If the stream is already registered as an orphan, returns it immediately.
    /// Otherwise, waits up to `timeout` for the stream to arrive.
    ///
    /// # Errors
    ///
    /// Returns `OrphanStreamError::AlreadyClaimed` if another caller already
    /// claimed this stream (deduplication).
    /// Returns `OrphanStreamError::Timeout` if the stream doesn't arrive within
    /// the timeout period.
    /// `peer_addr` is the remote address of the peer that sent this stream,
    /// used to scope lookups and prevent StreamId collisions across peers.
    pub async fn claim_or_wait(
        &self,
        peer_addr: SocketAddr,
        stream_id: StreamId,
        timeout: Duration,
    ) -> Result<StreamHandle, OrphanStreamError> {
        let key = (peer_addr, stream_id);
        // Atomic dedup: try to insert into claimed_streams. If already present,
        // another caller already claimed this stream.
        use dashmap::mapref::entry::Entry;
        match self.claimed_streams.entry(key) {
            Entry::Occupied(_) => {
                tracing::debug!(
                    %peer_addr,
                    stream_id = %stream_id,
                    "Stream already claimed (dedup)"
                );
                return Err(OrphanStreamError::AlreadyClaimed);
            }
            Entry::Vacant(entry) => {
                entry.insert(());
            }
        }

        // Check if orphan exists
        if let Some((_, (handle, _))) = self.orphan_streams.remove(&key) {
            tracing::debug!(
                %peer_addr,
                stream_id = %stream_id,
                "Claimed orphan stream immediately"
            );
            return Ok(handle);
        }

        // Register waiter
        let (tx, rx) = oneshot::channel();
        self.stream_waiters.insert(key, tx);

        tracing::debug!(
            %peer_addr,
            stream_id = %stream_id,
            timeout_ms = timeout.as_millis(),
            "Waiting for stream to arrive"
        );

        // Wait with timeout
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(handle)) => {
                tracing::debug!(
                    %peer_addr,
                    stream_id = %stream_id,
                    "Stream arrived while waiting"
                );
                Ok(handle)
            }
            Ok(Err(_)) => {
                // Sender was dropped (shouldn't happen in normal operation)
                self.stream_waiters.remove(&key);
                // Remove our claim so a retry is possible
                self.claimed_streams.remove(&key);
                tracing::warn!(
                    %peer_addr,
                    stream_id = %stream_id,
                    "Stream waiter cancelled unexpectedly"
                );
                Err(OrphanStreamError::WaiterCancelled)
            }
            Err(_) => {
                // Timeout expired
                self.stream_waiters.remove(&key);
                // Remove our claim so a retry is possible
                self.claimed_streams.remove(&key);
                tracing::warn!(
                    %peer_addr,
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
    pub fn gc_expired(&self) {
        let now = Instant::now();
        let mut expired_count = 0;

        self.orphan_streams
            .retain(|(peer_addr, stream_id), (handle, created)| {
                if now.duration_since(*created) > ORPHAN_STREAM_TIMEOUT {
                    tracing::debug!(
                        %peer_addr,
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

        // Also prune claimed_streams to prevent unbounded growth.
        // Entries older than the orphan timeout can be safely removed since
        // no duplicate metadata message would arrive that late.
        // We don't track insertion time for claimed_streams, so we cap at a
        // reasonable size instead.
        if self.claimed_streams.len() > 1000 {
            self.claimed_streams.clear();
        }

        if expired_count > 0 {
            tracing::info!(
                expired_count,
                remaining = self.orphan_streams.len(),
                "Garbage collected expired orphan streams"
            );
        }
    }

    /// Returns the number of orphan streams currently registered.
    #[cfg(test)]
    pub fn orphan_count(&self) -> usize {
        self.orphan_streams.len()
    }

    /// Returns the number of waiters currently registered.
    #[cfg(test)]
    pub fn waiter_count(&self) -> usize {
        self.stream_waiters.len()
    }

    /// Start the background GC task for expired orphan streams.
    ///
    /// This spawns a task that runs periodically to clean up orphan streams
    /// that were never claimed. Should be called once after the registry is created.
    ///
    /// The task runs every 5 seconds and removes streams older than `ORPHAN_STREAM_TIMEOUT`.
    pub fn start_gc_task(registry: std::sync::Arc<Self>) {
        use crate::config::GlobalExecutor;

        GlobalExecutor::spawn(Self::gc_task(registry));
    }

    /// Background task to periodically garbage collect expired orphan streams.
    async fn gc_task(registry: std::sync::Arc<Self>) {
        use crate::config::GlobalRng;

        // Add random initial delay to prevent synchronized GC across peers
        let initial_delay = Duration::from_secs(GlobalRng::random_range(5u64..=15u64));
        tokio::time::sleep(initial_delay).await;

        const GC_INTERVAL: Duration = Duration::from_secs(5);
        let mut interval = tokio::time::interval(GC_INTERVAL);

        tracing::debug!("Orphan stream GC task started");

        loop {
            interval.tick().await;
            registry.gc_expired();
        }
    }
}

impl Default for OrphanStreamRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Errors that can occur when claiming a stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OrphanStreamError {
    /// Timeout waiting for stream to arrive.
    Timeout,
    /// Waiter was cancelled (sender dropped unexpectedly).
    WaiterCancelled,
    /// Stream was already claimed by another caller (deduplication).
    AlreadyClaimed,
}

impl std::fmt::Display for OrphanStreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OrphanStreamError::Timeout => write!(f, "timeout waiting for stream"),
            OrphanStreamError::WaiterCancelled => write!(f, "stream waiter was cancelled"),
            OrphanStreamError::AlreadyClaimed => write!(f, "stream already claimed (duplicate)"),
        }
    }
}

impl std::error::Error for OrphanStreamError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::GlobalExecutor;

    /// Small delay to allow async waiter registration before asserting.
    const WAITER_REGISTRATION_DELAY: Duration = Duration::from_millis(50);

    /// Age to use when simulating an expired orphan for GC tests.
    const EXPIRED_ORPHAN_AGE: Duration = Duration::from_secs(60);

    fn dummy_addr() -> SocketAddr {
        "127.0.0.1:9000".parse().unwrap()
    }

    fn dummy_addr_2() -> SocketAddr {
        "127.0.0.2:9000".parse().unwrap()
    }

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
        let addr = dummy_addr();

        // Register orphan
        registry.register_orphan(addr, stream_id, handle);
        assert_eq!(registry.orphan_count(), 1);

        // Claim immediately
        let claimed = registry
            .claim_or_wait(addr, stream_id, Duration::from_secs(1))
            .await;
        assert!(claimed.is_ok());
        assert_eq!(registry.orphan_count(), 0);
    }

    #[tokio::test]
    async fn test_orphan_wait_then_register() {
        let registry = std::sync::Arc::new(OrphanStreamRegistry::new());
        let stream_id = StreamId::next();
        let addr = dummy_addr();

        // Start waiting in background
        let registry_clone = registry.clone();
        let waiter = GlobalExecutor::spawn(async move {
            registry_clone
                .claim_or_wait(addr, stream_id, Duration::from_secs(5))
                .await
        });

        // Small delay to ensure waiter is registered
        tokio::time::sleep(WAITER_REGISTRATION_DELAY).await;
        assert_eq!(registry.waiter_count(), 1);

        // Register orphan (should deliver to waiter)
        let handle = make_test_handle(stream_id);
        registry.register_orphan(addr, stream_id, handle);

        // Waiter should succeed
        let result = waiter.await.unwrap();
        assert!(result.is_ok());
        assert_eq!(registry.waiter_count(), 0);
    }

    #[tokio::test]
    async fn test_duplicate_claim_returns_already_claimed() {
        let registry = OrphanStreamRegistry::new();
        let stream_id = StreamId::next();
        let handle = make_test_handle(stream_id);
        let addr = dummy_addr();

        // Register and claim once
        registry.register_orphan(addr, stream_id, handle);
        let result = registry
            .claim_or_wait(addr, stream_id, Duration::from_secs(1))
            .await;
        assert!(result.is_ok());

        // Second claim should return AlreadyClaimed immediately (no timeout wait)
        let result = registry
            .claim_or_wait(addr, stream_id, Duration::from_secs(5))
            .await;
        assert!(matches!(result, Err(OrphanStreamError::AlreadyClaimed)));
    }

    #[tokio::test]
    async fn test_orphan_timeout() {
        let registry = OrphanStreamRegistry::new();
        let stream_id = StreamId::next();
        let addr = dummy_addr();

        // Try to claim non-existent stream with short timeout
        let result = registry
            .claim_or_wait(addr, stream_id, WAITER_REGISTRATION_DELAY)
            .await;

        assert!(matches!(result, Err(OrphanStreamError::Timeout)));
    }

    #[test]
    fn test_gc_expired() {
        let registry = OrphanStreamRegistry::new();
        let stream_id = StreamId::next();
        let handle = make_test_handle(stream_id);
        let addr = dummy_addr();

        // Insert with fake old timestamp by directly manipulating
        registry
            .orphan_streams
            .insert((addr, stream_id), (handle, Instant::now() - EXPIRED_ORPHAN_AGE));

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
        let addr = dummy_addr();

        // Register fresh orphan
        registry.register_orphan(addr, stream_id, handle);
        assert_eq!(registry.orphan_count(), 1);

        // GC should preserve fresh stream
        registry.gc_expired();
        assert_eq!(registry.orphan_count(), 1);
    }

    #[tokio::test]
    async fn test_different_peers_same_stream_id_no_collision() {
        let registry = OrphanStreamRegistry::new();
        let stream_id = StreamId::next();
        let addr_a = dummy_addr();
        let addr_b = dummy_addr_2();

        let handle_a = make_test_handle(stream_id);
        let handle_b = make_test_handle(stream_id);

        // Two different peers register orphans with the SAME StreamId
        registry.register_orphan(addr_a, stream_id, handle_a);
        registry.register_orphan(addr_b, stream_id, handle_b);
        assert_eq!(registry.orphan_count(), 2);

        // Claiming from peer A should succeed
        let result_a = registry
            .claim_or_wait(addr_a, stream_id, Duration::from_secs(1))
            .await;
        assert!(result_a.is_ok());

        // Claiming from peer B should also succeed (not AlreadyClaimed)
        let result_b = registry
            .claim_or_wait(addr_b, stream_id, Duration::from_secs(1))
            .await;
        assert!(result_b.is_ok());

        assert_eq!(registry.orphan_count(), 0);
    }
}

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

use crate::transport::peer_connection::StreamId;
use crate::transport::peer_connection::streaming::StreamHandle;

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

/// Whichever side of the handoff got to a key first.
enum Slot {
    /// The stream arrived before its metadata (RequestStreaming /
    /// ResponseStreaming): the handle and when it was registered.
    Orphan(StreamHandle, Instant),
    /// The metadata arrived first: the claimant waiting for the stream.
    Waiter(oneshot::Sender<StreamHandle>),
}

pub struct OrphanStreamRegistry {
    /// One slot per key holding either the early stream or the early claimant.
    ///
    /// Both sides live in ONE map on purpose (#5731). `register_orphan`
    /// (transport task) and `claim_or_wait` (operations task) each
    /// check-then-insert under the key's entry lock, so they cannot both miss
    /// each other. With two maps — orphans and waiters — the claim could see no
    /// orphan and park a waiter while the transport saw no waiter and parked an
    /// orphan, stranding the stream until `STREAM_CLAIM_TIMEOUT`.
    slots: DashMap<StreamKey, Slot>,

    /// Streams that have already been claimed. Used for deduplication when
    /// both the embedded metadata (in fragment #1) and the separate metadata
    /// message arrive — only the first one should be processed.
    claimed_streams: DashMap<StreamKey, ()>,
}

impl OrphanStreamRegistry {
    /// Creates a new empty registry.
    pub fn new() -> Self {
        Self {
            slots: DashMap::new(),
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
        use dashmap::mapref::entry::Entry;
        let key = (peer_addr, stream_id);
        // Check and act under the key's entry lock, so a concurrent
        // `claim_or_wait` has either already parked the waiter we deliver to,
        // or will find our orphan (#5731).
        match self.slots.entry(key) {
            Entry::Occupied(occupied) if matches!(occupied.get(), Slot::Waiter(_)) => {
                // `remove` consumes the entry, releasing the shard lock before
                // the handle is sent.
                let Slot::Waiter(waiter) = occupied.remove() else {
                    unreachable!("the match guard checked for a waiter");
                };
                if let Err(handle) = waiter.send(handle) {
                    // The claimant went away (timed out or was dropped) between
                    // parking and now. Keep the stream for a retried claim
                    // instead of discarding it: go round again, which delivers
                    // to a newer waiter or parks the stream as an orphan. Each
                    // round removes one dead waiter, so this terminates.
                    tracing::debug!(
                        %peer_addr,
                        stream_id = %stream_id,
                        "Waiter gone before delivery; re-registering stream"
                    );
                    self.register_orphan(peer_addr, stream_id, handle);
                } else {
                    tracing::debug!(
                        %peer_addr,
                        stream_id = %stream_id,
                        "Delivered stream to waiting operation"
                    );
                }
            }
            // A duplicate registration replaces the earlier orphan, as before.
            Entry::Occupied(mut occupied) => {
                occupied.insert(Slot::Orphan(handle, Instant::now()));
            }
            Entry::Vacant(vacant) => {
                tracing::debug!(
                    %peer_addr,
                    stream_id = %stream_id,
                    "Registered orphan stream (metadata not yet received)"
                );
                vacant.insert(Slot::Orphan(handle, Instant::now()));
            }
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
        use dashmap::mapref::entry::Entry;
        let key = (peer_addr, stream_id);
        // Atomic dedup: try to insert into claimed_streams. If already present,
        // another caller already claimed this stream.
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

        // Take the orphan, or park a waiter, under the key's entry lock — the
        // same lock `register_orphan` holds, so the two cannot miss each
        // other (#5731).
        let rx = match self.slots.entry(key) {
            Entry::Occupied(occupied) if matches!(occupied.get(), Slot::Orphan(..)) => {
                let Slot::Orphan(handle, _) = occupied.remove() else {
                    unreachable!("the match guard checked for an orphan");
                };
                tracing::debug!(
                    %peer_addr,
                    stream_id = %stream_id,
                    "Claimed orphan stream immediately"
                );
                return Ok(handle);
            }
            // A LIVE waiter here means another claimant got past the
            // `claimed_streams` dedup, which `gc_expired`'s wholesale clear of
            // that map makes possible. Never displace it: that claimant keeps
            // the stream, and this one is the duplicate.
            Entry::Occupied(occupied) if matches!(occupied.get(), Slot::Waiter(tx) if !tx.is_closed()) =>
            {
                tracing::debug!(
                    %peer_addr,
                    stream_id = %stream_id,
                    "Stream already has a live waiter (dedup)"
                );
                return Err(OrphanStreamError::AlreadyClaimed);
            }
            // A dead waiter, left by a claim whose future was dropped
            // mid-wait: replace it.
            Entry::Occupied(mut occupied) => {
                let (tx, rx) = oneshot::channel();
                occupied.insert(Slot::Waiter(tx));
                rx
            }
            Entry::Vacant(vacant) => {
                let (tx, rx) = oneshot::channel();
                vacant.insert(Slot::Waiter(tx));
                rx
            }
        };

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
                self.remove_waiter(&key);
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
                self.remove_waiter(&key);
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

    /// Remove this claim's own waiter from `key` once its receiver is gone.
    ///
    /// Only a closed waiter is removed: ours is closed by the time this runs
    /// (its receiver was dropped with the timed-out wait, or its sender was
    /// already taken). An orphan, or another claimant's live waiter, at the
    /// same key is left alone.
    fn remove_waiter(&self, key: &StreamKey) {
        self.slots.remove_if(
            key,
            |_, slot| matches!(slot, Slot::Waiter(tx) if tx.is_closed()),
        );
    }

    /// Garbage collect expired orphan streams.
    ///
    /// Should be called periodically to clean up orphan streams that were
    /// never claimed. Each expired stream's handle is cancelled.
    pub fn gc_expired(&self) {
        let now = Instant::now();
        let mut expired_count = 0;

        self.slots
            .retain(|(peer_addr, stream_id), slot| match slot {
                Slot::Orphan(handle, created) => {
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
                }
                // A claim removes its own waiter when it times out or is
                // cancelled, but not when its future is dropped mid-wait
                // (task abort, shutdown). Collect those dead waiters here.
                Slot::Waiter(tx) => !tx.is_closed(),
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
                remaining = self.orphan_count(),
                "Garbage collected expired orphan streams"
            );
        }
    }

    /// Returns the number of orphan streams currently registered.
    pub(crate) fn orphan_count(&self) -> usize {
        self.slots
            .iter()
            .filter(|slot| matches!(slot.value(), Slot::Orphan(..)))
            .count()
    }

    /// Returns the number of waiters currently registered.
    #[cfg(test)]
    pub fn waiter_count(&self) -> usize {
        self.slots
            .iter()
            .filter(|slot| matches!(slot.value(), Slot::Waiter(_)))
            .count()
    }

    /// Drop every registered waiter without delivering a stream, the way this
    /// node's side of a claim can go away: each waiting claim then ends with
    /// `WaiterCancelled`.
    #[cfg(test)]
    pub fn drop_waiters(&self) {
        self.slots
            .retain(|_, slot| !matches!(slot, Slot::Waiter(_)));
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
        registry.slots.insert(
            (addr, stream_id),
            Slot::Orphan(handle, Instant::now() - EXPIRED_ORPHAN_AGE),
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
        let addr = dummy_addr();

        // Register fresh orphan
        registry.register_orphan(addr, stream_id, handle);
        assert_eq!(registry.orphan_count(), 1);

        // GC should preserve fresh stream
        registry.gc_expired();
        assert_eq!(registry.orphan_count(), 1);
    }

    /// Regression for #5731: `claim_or_wait` (operations layer, metadata
    /// arrived) and `register_orphan` (transport layer, first fragment
    /// arrived) run on different tasks. When they checked and inserted into
    /// two separate maps, both could miss each other — the claim saw no
    /// orphan and parked a waiter while the transport saw no waiter and
    /// parked an orphan — stranding the stream until `STREAM_CLAIM_TIMEOUT`.
    /// A streaming PUT then hung for 60 s (`test_put_with_subscribe_flag`
    /// timed out). Race the two calls from two OS threads, many times; every
    /// claim must get the stream, never time out.
    #[test]
    fn test_concurrent_claim_and_register_never_strand_the_stream() {
        const ITERATIONS: usize = 5_000;
        const CLAIM_TIMEOUT: Duration = Duration::from_secs(2);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        for iteration in 0..ITERATIONS {
            let registry = std::sync::Arc::new(OrphanStreamRegistry::new());
            let stream_id = StreamId::next();
            let addr = dummy_addr();
            let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));

            let transport = {
                let registry = registry.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    registry.register_orphan(addr, stream_id, make_test_handle(stream_id));
                })
            };
            let claimed = rt.block_on(async {
                barrier.wait();
                registry.claim_or_wait(addr, stream_id, CLAIM_TIMEOUT).await
            });
            transport.join().unwrap();

            assert!(
                claimed.is_ok(),
                "iteration {iteration}: claim raced register_orphan and lost the stream: \
                 {claimed:?} (orphans={}, waiters={})",
                registry.orphan_count(),
                registry.waiter_count()
            );
            assert_eq!(registry.orphan_count(), 0, "iteration {iteration}");
            assert_eq!(registry.waiter_count(), 0, "iteration {iteration}");
        }
    }

    /// `gc_expired` clears `claimed_streams` wholesale once it grows past its
    /// cap, so a duplicate claim can get past dedup while the first claimant
    /// is still parked. It must not displace that live waiter: the duplicate
    /// gets `AlreadyClaimed` and the original claimant still receives the
    /// stream.
    #[tokio::test]
    async fn test_duplicate_claim_never_displaces_live_waiter() {
        let registry = std::sync::Arc::new(OrphanStreamRegistry::new());
        let stream_id = StreamId::next();
        let addr = dummy_addr();

        let first = {
            let registry = registry.clone();
            GlobalExecutor::spawn(async move {
                registry
                    .claim_or_wait(addr, stream_id, Duration::from_secs(5))
                    .await
            })
        };
        tokio::time::sleep(WAITER_REGISTRATION_DELAY).await;
        assert_eq!(registry.waiter_count(), 1);

        // What gc_expired's cap-triggered clear does to the live claim.
        registry.claimed_streams.clear();
        let duplicate = registry
            .claim_or_wait(addr, stream_id, Duration::from_secs(5))
            .await;
        assert!(matches!(duplicate, Err(OrphanStreamError::AlreadyClaimed)));
        assert_eq!(registry.waiter_count(), 1, "live waiter must survive");

        registry.register_orphan(addr, stream_id, make_test_handle(stream_id));
        assert!(first.await.unwrap().is_ok());
        assert_eq!(registry.waiter_count(), 0);
        assert_eq!(registry.orphan_count(), 0);
    }

    /// A stream arriving for a waiter whose claim already went away (its
    /// receiver dropped) is kept as an orphan for a retried claim, not
    /// discarded.
    #[tokio::test]
    async fn test_stream_for_dead_waiter_is_kept_for_retry() {
        let registry = OrphanStreamRegistry::new();
        let stream_id = StreamId::next();
        let addr = dummy_addr();

        let (tx, rx) = oneshot::channel();
        drop(rx);
        registry.slots.insert((addr, stream_id), Slot::Waiter(tx));

        registry.register_orphan(addr, stream_id, make_test_handle(stream_id));
        assert_eq!(registry.waiter_count(), 0);
        assert_eq!(registry.orphan_count(), 1);

        let retried = registry
            .claim_or_wait(addr, stream_id, Duration::from_secs(1))
            .await;
        assert!(retried.is_ok());
    }

    /// A timed-out claim cleans up only its own dead waiter; GC collects dead
    /// waiters left by claims dropped mid-wait, and keeps live ones.
    #[tokio::test]
    async fn test_waiter_cleanup_removes_only_dead_waiters() {
        let registry = OrphanStreamRegistry::new();
        let addr = dummy_addr();

        // Timeout cleanup must leave an orphan at the key alone.
        let orphan_id = StreamId::next();
        registry.slots.insert(
            (addr, orphan_id),
            Slot::Orphan(make_test_handle(orphan_id), Instant::now()),
        );
        registry.remove_waiter(&(addr, orphan_id));
        assert_eq!(registry.orphan_count(), 1);

        let dead_id = StreamId::next();
        let (dead_tx, dead_rx) = oneshot::channel();
        drop(dead_rx);
        registry
            .slots
            .insert((addr, dead_id), Slot::Waiter(dead_tx));

        let live_id = StreamId::next();
        let (live_tx, _live_rx) = oneshot::channel();
        registry
            .slots
            .insert((addr, live_id), Slot::Waiter(live_tx));

        registry.remove_waiter(&(addr, live_id));
        assert_eq!(
            registry.waiter_count(),
            2,
            "a live waiter is not ours to remove"
        );

        registry.gc_expired();
        assert_eq!(
            registry.waiter_count(),
            1,
            "GC collects only the dead waiter"
        );
        assert!(registry.slots.contains_key(&(addr, live_id)));
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

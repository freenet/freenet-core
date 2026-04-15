//! Global broadcast queue for serializing outbound state-change broadcasts.
//!
//! When a contract state update triggers `BroadcastStateChange`, the node broadcasts
//! to all hosting peers. Without throttling, N concurrent streams each rate-limited
//! to ~1.25 MB/s saturate typical residential uplinks (5-10 MB/s), causing packet
//! loss and stream stalls. The FixedRate congestion controller ignores loss, so
//! senders never back off.
//!
//! `BroadcastQueue` limits the number of concurrent outbound broadcast streams
//! via a semaphore, deduplicates entries per (contract, peer), and replaces
//! older entries with newer state when a duplicate is enqueued.

use std::sync::Arc;
use std::time::Duration;

use freenet_stdlib::prelude::{ContractKey, WrappedState};

use crate::node::OpManager;
use crate::ring::PeerKeyLocation;

use super::p2p_protoc::P2pBridge;

/// Timeout for awaiting stream completion signal before releasing the permit
/// anyway. Prevents permanent permit leak if a stream task panics or hangs.
/// Used by `broadcast_to_single_peer` under both `simulation_tests` and
/// production, hence kept at module scope rather than inside the cfg-gated
/// `queue` submodule.
const STREAM_COMPLETION_TIMEOUT: Duration = Duration::from_secs(120);

// The `BroadcastQueue` struct (constants, types, impl) is only used in the
// production `p2p_protoc` path. Under `simulation_tests` the code routes
// through `broadcast_to_single_peer` directly (see p2p_protoc.rs), so the
// queue itself is dead code in that build. Gate it out to keep
// `cargo clippy --features simulation_tests -- -D warnings` clean.
#[cfg(not(feature = "simulation_tests"))]
mod queue {
    use std::collections::{HashMap, VecDeque};
    use std::sync::Arc;

    use freenet_stdlib::prelude::{ContractKey, WrappedState};
    use tokio::sync::{Mutex, Notify, Semaphore};

    use crate::node::OpManager;
    use crate::ring::PeerKeyLocation;

    use super::super::p2p_protoc::P2pBridge;
    use super::broadcast_to_single_peer;

    /// Maximum concurrent outbound broadcast streams for small payloads (< 64KB).
    /// Small payloads (deltas, chat messages) can fan out aggressively without
    /// saturating the uplink since they finish quickly.
    const DEFAULT_SMALL_PAYLOAD_CONCURRENCY: usize = 12;

    /// Maximum concurrent outbound broadcast streams for large payloads (>= 64KB).
    /// Large payloads (full state) are rate-limited to avoid uplink saturation.
    const DEFAULT_LARGE_PAYLOAD_CONCURRENCY: usize = 2;

    /// Payload size threshold for choosing the small vs large concurrency pool.
    /// Matches the streaming threshold used elsewhere in the broadcast path.
    const PAYLOAD_SIZE_THRESHOLD: usize = 64 * 1024;

    /// Maximum entries in the queue before oldest are dropped.
    const DEFAULT_MAX_QUEUE_DEPTH: usize = 256;

    /// Key for deduplicating broadcast entries: (contract, peer identity).
    type DedupeKey = (ContractKey, PeerKeyLocation);

    /// A pending broadcast entry in the queue.
    struct BroadcastEntry {
        key: ContractKey,
        target: PeerKeyLocation,
        new_state: WrappedState,
        /// Payload size in bytes, used to select concurrency pool.
        payload_size: usize,
    }

    /// Internal queue state: FIFO ordering via VecDeque + HashMap for dedup lookup.
    struct QueueState {
        /// FIFO order of dedup keys. Entries may be stale if replaced by dedup.
        order: VecDeque<DedupeKey>,
        /// Actual entries, keyed by (contract, peer). Dedup replaces the state in-place.
        entries: HashMap<DedupeKey, BroadcastEntry>,
    }

    impl QueueState {
        fn new() -> Self {
            Self {
                order: VecDeque::new(),
                entries: HashMap::new(),
            }
        }

        fn len(&self) -> usize {
            self.entries.len()
        }

        /// Pop the oldest entry. Skips stale keys (removed by eviction or dedup).
        fn pop_front(&mut self) -> Option<BroadcastEntry> {
            while let Some(key) = self.order.pop_front() {
                if let Some(entry) = self.entries.remove(&key) {
                    return Some(entry);
                }
                // Stale key (was evicted or already popped), skip
            }
            None
        }
    }

    /// Global broadcast queue that serializes outbound broadcast streams
    /// with bounded concurrency and deduplication.
    ///
    /// Uses dual concurrency pools: small payloads (< 64KB) get high concurrency
    /// (12 slots) for fast fan-out of deltas/chat messages, while large payloads
    /// (>= 64KB) get low concurrency (2 slots) to avoid saturating the uplink.
    #[derive(Clone)]
    pub(crate) struct BroadcastQueue {
        queue: Arc<Mutex<QueueState>>,
        notify: Arc<Notify>,
        small_payload_concurrency: usize,
        large_payload_concurrency: usize,
        max_queue_depth: usize,
    }

    impl BroadcastQueue {
        pub(crate) fn new() -> Self {
            Self {
                queue: Arc::new(Mutex::new(QueueState::new())),
                notify: Arc::new(Notify::new()),
                small_payload_concurrency: DEFAULT_SMALL_PAYLOAD_CONCURRENCY,
                large_payload_concurrency: DEFAULT_LARGE_PAYLOAD_CONCURRENCY,
                max_queue_depth: DEFAULT_MAX_QUEUE_DEPTH,
            }
        }

        /// Enqueue a broadcast for a single (contract, peer) pair.
        ///
        /// If an entry for the same contract+peer already exists, it is replaced
        /// with the newer state (the older state is stale and would be superseded
        /// anyway). If the queue is at capacity, the oldest entry is evicted.
        pub(crate) async fn enqueue(
            &self,
            key: ContractKey,
            target: PeerKeyLocation,
            new_state: WrappedState,
        ) {
            let dedup_key = (key, target.clone());
            let mut queue = self.queue.lock().await;

            // Replace-on-dedup: if same contract+peer exists, update state in-place
            if let Some(existing) = queue.entries.get_mut(&dedup_key) {
                existing.new_state = new_state;
                tracing::trace!(
                    contract = %dedup_key.0,
                    peer = ?target.socket_addr(),
                    "Broadcast queue: replaced stale entry with newer state"
                );
            } else {
                // Evict oldest if at capacity
                while queue.len() >= self.max_queue_depth {
                    if let Some(entry) = queue.pop_front() {
                        tracing::warn!(
                            contract = %entry.key,
                            peer = ?entry.target.socket_addr(),
                            queue_depth = self.max_queue_depth,
                            "Broadcast queue full, evicted oldest entry"
                        );
                    } else {
                        break;
                    }
                }
                let payload_size = new_state.size();
                queue.entries.insert(
                    dedup_key.clone(),
                    BroadcastEntry {
                        key,
                        target,
                        new_state,
                        payload_size,
                    },
                );
                queue.order.push_back(dedup_key);
            }

            drop(queue);
            self.notify.notify_one();
        }

        /// Start the background worker that drains the queue with bounded concurrency.
        ///
        /// The worker runs forever. It should be spawned as a background task.
        pub(crate) fn start_worker(
            &self,
            bridge: P2pBridge,
            op_manager: Arc<OpManager>,
        ) -> tokio::task::JoinHandle<()> {
            let queue = self.queue.clone();
            let notify = self.notify.clone();
            let small_semaphore = Arc::new(Semaphore::new(self.small_payload_concurrency));
            let large_semaphore = Arc::new(Semaphore::new(self.large_payload_concurrency));

            tokio::spawn(async move {
                loop {
                    // Register the notified future BEFORE checking the queue to avoid
                    // a race where enqueue() calls notify_one() between our "queue empty"
                    // check and the notified().await call.
                    let notified = notify.notified();

                    // Drain all available entries
                    let mut drained_any = false;
                    loop {
                        let entry = {
                            let mut q = queue.lock().await;
                            q.pop_front()
                        };

                        let Some(entry) = entry else {
                            break; // Queue empty
                        };
                        drained_any = true;

                        // Select concurrency pool based on payload size.
                        // Small payloads (deltas, chat messages) get high concurrency for
                        // fast fan-out. Large payloads get low concurrency to avoid saturation.
                        let sem = if entry.payload_size < PAYLOAD_SIZE_THRESHOLD {
                            small_semaphore.clone()
                        } else {
                            large_semaphore.clone()
                        };

                        // Acquire semaphore permit to limit concurrent streams.
                        // This blocks until a slot is available.
                        let permit = sem.acquire_owned().await;
                        let Ok(permit) = permit else {
                            tracing::error!("Broadcast queue semaphore closed unexpectedly");
                            return;
                        };

                        let bridge = bridge.clone();
                        let op_manager = op_manager.clone();

                        tokio::spawn(async move {
                            let _permit = permit; // Held until this task completes

                            broadcast_to_single_peer(
                                &bridge,
                                &op_manager,
                                entry.key,
                                entry.new_state,
                                entry.target,
                            )
                            .await;
                        });
                    }

                    if !drained_any {
                        // Queue was empty, wait for new entries
                        notified.await;
                    }
                    // If we drained entries, loop immediately to check for more
                    // (the pre-registered notified future is dropped, which is fine)
                }
            })
        }
    }
} // end `mod queue` (cfg-gated)

#[cfg(not(feature = "simulation_tests"))]
pub(crate) use queue::BroadcastQueue;

/// Send a state change broadcast to a single peer.
///
/// This is the per-target body extracted from `broadcast_state_to_peers`.
/// It handles delta computation, streaming vs inline decision, and telemetry.
///
/// For streaming sends, a completion oneshot is created internally and threaded
/// through the stream send path. The function awaits it (with timeout) so the
/// caller's semaphore permit is held until the actual stream transfer finishes.
pub(super) async fn broadcast_to_single_peer(
    bridge: &P2pBridge,
    op_manager: &Arc<OpManager>,
    key: ContractKey,
    new_state: WrappedState,
    target: PeerKeyLocation,
) {
    use crate::message::{DeltaOrFullState, NetMessage};
    use crate::node::network_bridge::NetworkBridge;
    use crate::operations::update::{BroadcastStreamingPayload, UpdateMsg};
    use crate::ring::PeerKey;
    use crate::transport::peer_connection::StreamId;

    let Some(peer_addr) = target.socket_addr() else {
        return;
    };

    let peer_key = PeerKey::from(target.pub_key().clone());

    // Get our summary for delta computation
    let our_summary = op_manager
        .interest_manager
        .get_contract_summary(op_manager, &key)
        .await;

    // Get peer's cached summary
    let their_summary = op_manager
        .interest_manager
        .get_peer_summary(&key, &peer_key);

    // Skip if summaries are equal (no change to send)
    if let (Some(ours), Some(theirs)) = (&our_summary, &their_summary) {
        if ours.as_ref() == theirs.as_ref() {
            tracing::trace!(
                contract = %key,
                peer = %peer_addr,
                "Skipping broadcast - peer already has our state"
            );
            return;
        }
    }

    // Compute delta if we have their summary
    let (payload, sent_delta) = match (&our_summary, &their_summary) {
        (Some(ours), Some(theirs)) => {
            match op_manager
                .interest_manager
                .compute_delta(op_manager, &key, theirs, ours, new_state.size())
                .await
            {
                Ok(Some(delta)) => (DeltaOrFullState::Delta(delta.as_ref().to_vec()), true),
                Ok(None) => {
                    tracing::debug!(
                        contract = %key,
                        "Delta computation returned no change, sending full state"
                    );
                    (
                        DeltaOrFullState::FullState(new_state.as_ref().to_vec()),
                        false,
                    )
                }
                Err(err) => {
                    tracing::debug!(
                        contract = %key,
                        error = %err,
                        "Delta computation failed, falling back to full state"
                    );
                    (
                        DeltaOrFullState::FullState(new_state.as_ref().to_vec()),
                        false,
                    )
                }
            }
        }
        _ => (
            DeltaOrFullState::FullState(new_state.as_ref().to_vec()),
            false,
        ),
    };

    let payload_size = payload.size();
    let update_tx = crate::message::Transaction::new::<crate::operations::update::UpdateMsg>();

    // Check if we should use streaming for full state broadcasts
    let use_streaming = matches!(&payload, DeltaOrFullState::FullState(_))
        && crate::operations::should_use_streaming(op_manager.streaming_threshold, payload_size);

    let send_result = if use_streaming {
        let sender_summary_bytes = our_summary
            .as_ref()
            .map(|s| s.as_ref().to_vec())
            .unwrap_or_default();
        let state_bytes = match payload {
            DeltaOrFullState::FullState(data) => data,
            _ => unreachable!("checked above"),
        };
        let streaming_payload = BroadcastStreamingPayload {
            state_bytes,
            sender_summary_bytes,
        };
        let payload_bytes = match bincode::serialize(&streaming_payload) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(
                    tx = %update_tx,
                    error = %e,
                    "Failed to serialize BroadcastStreamingPayload, skipping"
                );
                return;
            }
        };
        let sid = StreamId::next_operations();
        tracing::debug!(
            tx = %update_tx,
            contract = %key,
            peer = %peer_addr,
            stream_id = %sid,
            payload_size,
            "Using streaming for BroadcastTo (via queue)"
        );
        let msg = UpdateMsg::BroadcastToStreaming {
            id: update_tx,
            stream_id: sid,
            key,
            total_size: payload_bytes.len() as u64,
        };
        let net_msg: NetMessage = msg.into();
        // Serialize metadata for embedding in fragment #1 (fix #2757)
        let metadata = match bincode::serialize(&net_msg) {
            Ok(bytes) => Some(bytes::Bytes::from(bytes)),
            Err(e) => {
                tracing::warn!(
                    ?peer_addr,
                    error = %e,
                    "Failed to serialize BroadcastTo metadata for embedding"
                );
                None
            }
        };

        let send_res = bridge.send(peer_addr, net_msg).await;
        if send_res.is_ok() {
            // Create completion channel for the broadcast queue to track
            // when the actual stream transfer finishes.
            let (completion_tx, completion_rx) = tokio::sync::oneshot::channel();

            if let Err(err) = bridge
                .send_stream_with_completion(
                    peer_addr,
                    sid,
                    bytes::Bytes::from(payload_bytes),
                    metadata,
                    Some(completion_tx),
                )
                .await
            {
                tracing::warn!(
                    tx = %update_tx,
                    peer = %peer_addr,
                    error = %err,
                    "Failed to send broadcast stream data"
                );
            } else {
                // Wait for the stream transfer to actually complete before
                // releasing back to the queue worker (semaphore permit is held
                // by our caller). Timeout prevents permanent stall.
                match tokio::time::timeout(STREAM_COMPLETION_TIMEOUT, completion_rx).await {
                    Ok(Ok(())) => {
                        tracing::debug!(
                            tx = %update_tx,
                            peer = %peer_addr,
                            "Broadcast stream completed successfully"
                        );
                    }
                    Ok(Err(_)) => {
                        tracing::debug!(
                            tx = %update_tx,
                            peer = %peer_addr,
                            "Broadcast stream completion channel dropped (task ended)"
                        );
                    }
                    Err(_) => {
                        tracing::warn!(
                            tx = %update_tx,
                            peer = %peer_addr,
                            timeout_secs = STREAM_COMPLETION_TIMEOUT.as_secs(),
                            "Broadcast stream completion timed out, releasing permit"
                        );
                    }
                }
            }
        }
        send_res
    } else {
        let msg = UpdateMsg::BroadcastTo {
            id: update_tx,
            key,
            payload,
            sender_summary_bytes: our_summary
                .as_ref()
                .map(|s| s.as_ref().to_vec())
                .unwrap_or_default(),
        };
        bridge.send(peer_addr, msg.into()).await
    };

    let send_ok = send_result.is_ok();

    if let Err(err) = send_result {
        tracing::warn!(
            tx = %update_tx,
            peer = %peer_addr,
            error = %err,
            "Failed to send state change broadcast (queued)"
        );
    } else {
        // Track delta vs full state sends for testing (PR #2763)
        if sent_delta {
            op_manager
                .interest_manager
                .record_delta_send(new_state.size(), payload_size);
            crate::config::GlobalTestMetrics::record_delta_send();
        } else {
            op_manager.interest_manager.record_full_state_send();
            crate::config::GlobalTestMetrics::record_full_state_send();
        }

        // Issue #3046: Refresh the peer's interest TTL on every successful send
        op_manager
            .interest_manager
            .refresh_peer_interest(&key, &peer_key);
    }

    // PR #2763: Only update cached summary when we sent a delta
    if send_ok && sent_delta {
        if let Some(summary) = &our_summary {
            op_manager
                .interest_manager
                .update_peer_summary(&key, &peer_key, Some(summary.clone()));
        }
    }
}

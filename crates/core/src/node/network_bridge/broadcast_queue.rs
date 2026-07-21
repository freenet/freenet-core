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
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use freenet_stdlib::prelude::{ContractKey, WrappedState};

use crate::node::OpManager;
use crate::ring::PeerKeyLocation;
use crate::transport::BroadcastDeliveryOutcome;

use super::p2p_protoc::P2pBridge;

/// Timeout for awaiting stream completion signal before releasing the permit
/// anyway. Prevents permanent permit leak if a stream task panics or hangs.
/// Used by `broadcast_to_single_peer` under both `simulation_tests` and
/// production, hence kept at module scope rather than inside the cfg-gated
/// `queue` submodule.
const STREAM_COMPLETION_TIMEOUT: Duration = Duration::from_secs(120);

/// Process-global UPDATE-broadcast stream-assembly telemetry (#4440).
///
/// The streaming broadcast path (`broadcast_to_single_peer`'s `use_streaming`
/// branch) sends a multi-fragment state transfer to one subscriber peer. Each
/// invocation records exactly one attempt, and a failure on any of its three
/// exits is counted: the initial metadata send returning `Err` (the stream
/// never landed), `send_stream_with_completion` returning `Err` (dispatch
/// failed before any fragment), or a post-dispatch non-`Delivered`
/// `BroadcastDeliveryOutcome` (explicit `Dropped`, a dropped completion oneshot,
/// or a `STREAM_COMPLETION_TIMEOUT`). All three are stream-assembly / transfer
/// failures — the exact signal that flagged the v0.2.73 incident, where
/// nova/vega saw ~1500-2300 broadcast stream-assembly failures/hr against a ~0
/// baseline and central telemetry had no gauge for it. (The two early-send
/// exits are the congestion failure mode that would otherwise bias the gauge
/// LOW precisely when it matters most.)
///
/// These broadcast tasks are spawned per (contract, peer) from the global
/// `BroadcastQueue` worker, unreachable from the `Ring` telemetry-snapshot task
/// that emits `router_snapshot`. Like [`TRANSPORT_METRICS`], the failure site
/// therefore *publishes* into this process-global and the snapshot task *reads*
/// it on the existing ~5-minute cadence — no per-failure event is emitted. (The
/// analogous module-cache telemetry was likewise a process-global until #4488
/// threaded it as a per-node `Arc`; this static still mirrors `TRANSPORT_METRICS`.)
///
/// Both counters are monotonic; the snapshot task differences them across the
/// cadence to derive a per-window failure rate (see
/// `Ring::emit_router_snapshot_telemetry`).
///
/// Per-node meaning holds only in single-node-per-process production. In a
/// multi-node simulation every node shares this process-global, so the snapshot
/// reads the aggregate across all in-process nodes (the same caveat that drove
/// #4488 for the module-cache metrics).
///
/// [`TRANSPORT_METRICS`]: crate::transport::metrics::TRANSPORT_METRICS
pub(crate) static BROADCAST_STREAM_METRICS: BroadcastStreamMetrics = BroadcastStreamMetrics::new();

/// Monotonic counters for UPDATE-broadcast streaming transfers. See
/// [`BROADCAST_STREAM_METRICS`].
pub(crate) struct BroadcastStreamMetrics {
    /// Total streaming broadcast transfers attempted (one per peer that took the
    /// streaming branch and reached the completion-await point).
    streaming_attempts_total: AtomicU64,
    /// Total streaming broadcast transfers that did NOT reach `Delivered`
    /// (dropped, oneshot dropped, or completion timeout).
    streaming_failures_total: AtomicU64,
}

/// A point-in-time read of [`BROADCAST_STREAM_METRICS`] for telemetry emission.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BroadcastStreamMetricsSnapshot {
    pub streaming_attempts_total: u64,
    pub streaming_failures_total: u64,
}

impl BroadcastStreamMetrics {
    const fn new() -> Self {
        Self {
            streaming_attempts_total: AtomicU64::new(0),
            streaming_failures_total: AtomicU64::new(0),
        }
    }

    /// Record one completed streaming broadcast attempt. `delivered == false`
    /// means a stream-assembly / transfer failure. Cheap `Relaxed` atomics — the
    /// counters are summed/differenced by the collector, not used for ordering.
    fn record_attempt(&self, delivered: bool) {
        self.streaming_attempts_total
            .fetch_add(1, Ordering::Relaxed);
        if !delivered {
            self.streaming_failures_total
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Read both counters for telemetry.
    pub(crate) fn snapshot(&self) -> BroadcastStreamMetricsSnapshot {
        BroadcastStreamMetricsSnapshot {
            streaming_attempts_total: self.streaming_attempts_total.load(Ordering::Relaxed),
            streaming_failures_total: self.streaming_failures_total.load(Ordering::Relaxed),
        }
    }
}

/// Whether we should broadcast a state change for `key` at all: only if we
/// host it or are actively serving it (a live local-client or downstream
/// subscriber). Mirrors `node.rs::summary_if_hosted_or_in_use` (#4475) for the
/// broadcast fan-out path.
///
/// A node can be driven into `broadcast_state_to_peers` /
/// `broadcast_to_single_peer` for a contract it neither hosts nor serves —
/// "phantom" contracts it holds no local state for. For such a contract the
/// per-peer body would call `get_contract_summary` (→
/// `InterestManager::summarize_contract_state`), which issues a
/// `GetSummaryQuery` round-trip on the single-threaded contract-handling loop
/// that returns "Contract state not found in store" every time, and would then
/// fall through to "send full state" with nothing real to send. #4475 gated the
/// interest-sync summarize sites (path A); this is the residual path-B caller
/// that drove the plateaued ~100k/hr summarize WARNs observed on nova after the
/// #4475 rollout (#4473). With no local state there is nothing to broadcast to
/// the peer, so skipping is the correct behavior, not just a throttle.
///
/// Gating on `(is_hosting_contract || contract_in_use)` alone proved
/// insufficient (#4610): the inbound relay-SUBSCRIBE / placement-migration path
/// marks a contract hosted / in-use (a downstream subscriber renewal) WITHOUT
/// its state ever being fetched and stored, so "phantom"
/// (interested-but-stateless) contracts still passed and drove the residual
/// `summarize_contract_state` storm. The fix delegates to the single composed
/// predicate `Ring::should_summarize_or_broadcast` —
/// `(is_hosting_contract || contract_in_use) && contract_state_present` — which
/// is shared with `node.rs::summary_if_hosted_or_in_use` so the two paths cannot
/// drift. The `contract_state_present` term reads the on-disk STATE store (NOT
/// the in-memory hosting cache), so a phantom with no stored state is skipped
/// while an evicted-but-in-use contract whose state is still on disk keeps
/// broadcasting. See `HostingManager::should_summarize_or_broadcast`.
///
/// NOTE: it is surprising the broadcast/interest path runs at all for a contract
/// we hold no state for — that points at a routing/subscription leak upstream
/// (the inbound relay-SUBSCRIBE registering downstream-subscriber + interest
/// without state, tracked separately on #4440/#4610). This gate stops the storm
/// symptom; it does not fix that upstream question.
pub(super) fn should_broadcast_contract(op_manager: &Arc<OpManager>, key: &ContractKey) -> bool {
    op_manager.ring.should_summarize_or_broadcast(key)
}

/// Decision for one (contract, peer) fan-out send, derived WITHOUT any WASM
/// call — a byte comparison plus the shared in-memory delta cache. See
/// [`plan_fanout_send`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FanoutSendPlan {
    /// The peer already has our state: byte-identical summaries, or a cached
    /// EMPTY delta proving logical convergence despite differing summary
    /// bytes. Nothing to send.
    Skip,
    /// The peer needs our state: a cached NON-EMPTY delta (genuine
    /// divergence), or byte-differing summaries with no semantic verdict
    /// available and no probe budget left (the conservative pre-#4894
    /// fallback — never silently skip a possible real divergence).
    Send,
    /// Byte-differing summaries, no cached verdict, probe budget remaining:
    /// the caller should run the bounded WASM `get_state_delta` probe
    /// ([`InterestManager::peer_summary_has_pending_state`]) and decide from
    /// its verdict. Callers without an async context must treat this as
    /// `Send` (conservative).
    ///
    /// [`InterestManager::peer_summary_has_pending_state`]:
    /// crate::ring::interest::InterestManager::peer_summary_has_pending_state
    Probe,
}

/// The two summaries a fan-out send decision compares, bundled with NAMED
/// fields so call sites cannot positionally transpose them.
///
/// The underlying probe API takes the pair POSITIONALLY in the OPPOSITE order
/// to how the fan-out naturally reads
/// (`peer_summary_has_pending_state(.., their_summary, our_summary)`,
/// interest.rs), and so does the delta cache
/// (`cached_staleness_verdict(key, theirs, ours)`). A transposed positional
/// call site would compile, pass every unit test and source-scrape pin, and
/// compute `delta(our_state vs our OWN summary)` — always empty — wrongly
/// skipping nearly every broadcast (a network-wide update blackout). Bundling
/// the pair is the `.claude/rules/bug-prevention-patterns.md` "paired fields
/// that must co-occur — bundle in a sub-struct" fix shape: the only
/// positional-order decisions left live INSIDE [`plan_fanout_send`] /
/// [`fanout_send_needed`], directly next to the APIs they map onto, and every
/// call site names the fields (`SummaryPair { ours, theirs }`), so a swap
/// requires explicitly writing `ours: theirs, theirs: ours`.
#[derive(Clone, Copy)]
pub(super) struct SummaryPair<'a> {
    /// OUR current summary for the contract (the sender's local state).
    pub ours: &'a freenet_stdlib::prelude::StateSummary<'static>,
    /// The PEER's cached summary (what we believe the receiver holds).
    pub theirs: &'a freenet_stdlib::prelude::StateSummary<'static>,
}

/// Cache-only layer of the fan-out's semantic staleness decision (#4894's
/// fan-out counterpart).
///
/// The live broadcast fan-out used to skip a peer only when its cached summary
/// was BYTE-identical to ours. That is wrong for the same reason the
/// InterestSync `Summaries` byte-compare was wrong (#4894 / #4857 secondary
/// finding): a contract whose `summarize_state` serializes
/// non-deterministically (HashMap/HashSet iteration order, per-process
/// `RandomState`) yields different summary bytes for the SAME logical state on
/// different peers. The byte compare then never skips, `compute_delta` either
/// returns an empty delta (which the pre-fix arm "fell back" from by sending
/// FULL STATE) or is refused outright by the `is_delta_efficient` gate on
/// big-summary contracts — so a fully-converged pair re-flooded full state on
/// every heartbeat-driven sync and every fan-out (the nondeterministic-summary
/// heal storm; e.g. the `Eumk9HNQ` contract that "healed" hard while its state
/// never changed).
///
/// This helper reuses the #4894 machinery: byte-equal summaries short-circuit
/// to [`FanoutSendPlan::Skip`]; byte-differing summaries consult the shared
/// delta cache ([`InterestManager::cached_staleness_verdict`]) under the same
/// probe rationing (`plan_staleness_probe`, budget mirroring
/// `MAX_STALENESS_PROBES_PER_SUMMARIES`). Pure/sync so it is unit-testable
/// with a bare [`InterestManager`]; the async probe half lives in
/// [`fanout_send_needed`].
///
/// Convergence safety: identical to #4894 — the skip set is a strict SUBSET of
/// the pre-fix byte-compare skip set plus exactly those pairs whose
/// contract-computed delta is EMPTY (copies that already hold our state, for
/// which the removed send would have transferred nothing). A genuinely
/// diverged pair yields a non-empty delta and still sends; an unavailable
/// verdict falls back to the conservative byte-differ ⇒ send behavior.
///
/// [`InterestManager`]: crate::ring::interest::InterestManager
/// [`InterestManager::cached_staleness_verdict`]:
/// crate::ring::interest::InterestManager::cached_staleness_verdict
pub(super) fn plan_fanout_send<T: crate::util::time_source::TimeSource + Sync>(
    interest_manager: &crate::ring::interest::InterestManager<T>,
    key: &ContractKey,
    summaries: SummaryPair<'_>,
    probes_used: usize,
) -> FanoutSendPlan {
    use crate::node::{StalenessProbeAction, plan_staleness_probe};

    let SummaryPair { ours, theirs } = summaries;

    // Byte-identical summaries are trivially converged (the pre-existing skip).
    if ours.as_ref() == theirs.as_ref() {
        return FanoutSendPlan::Skip;
    }
    // Bytes differ: ask the shared delta cache before trusting the bytes.
    // Cache key order matches `compute_delta` / the Summaries arm:
    // (contract, THEIR summary, OUR summary). This is one of the two
    // positional mappings `SummaryPair` exists to confine here.
    let cached = interest_manager.cached_staleness_verdict(key, theirs.as_ref(), ours.as_ref());
    match plan_staleness_probe(cached, probes_used) {
        StalenessProbeAction::UseCached(true) => FanoutSendPlan::Send,
        StalenessProbeAction::UseCached(false) => FanoutSendPlan::Skip,
        StalenessProbeAction::RunProbe => FanoutSendPlan::Probe,
        // Budget spent: conservative pre-fix behavior (differing bytes ⇒
        // send). Re-evaluated on the next fan-out once the cache warms.
        StalenessProbeAction::BudgetExhaustedFallBack => FanoutSendPlan::Send,
    }
}

/// Full semantic staleness decision for one (contract, peer) fan-out send:
/// [`plan_fanout_send`] plus the bounded WASM `get_state_delta` probe on a
/// cache miss. Returns `true` when the peer needs our state (send), `false`
/// when it is converged (skip).
///
/// `probes_used` is the per-fan-out-invocation probe budget counter (mirrors
/// the `Summaries` handler's per-message `MAX_STALENESS_PROBES_PER_SUMMARIES`
/// budget in node.rs): only cache MISSES that reach the WASM probe consume it.
/// The production per-peer queue task calls this once per (contract, peer)
/// entry — at most ONE probe per invocation, trivially within budget, and
/// bounded overall by the same queue/semaphore caps that already bound
/// `compute_delta` WASM work per entry. The sim-inline fan-out shares one
/// counter across all targets of a fan-out, capping the WASM probes a single
/// fan-out pass can issue. Probe results land in the shared delta cache, so
/// repeated fan-outs for an unchanged pair cost no further WASM.
///
/// Note the probe deliberately bypasses the [`is_delta_efficient`] wire gate
/// (see `peer_summary_has_pending_state`): staleness detection wants the
/// semantic answer even for big-summary contracts, because the alternative it
/// replaces is a spurious FULL-STATE send on every fan-out — strictly more
/// expensive than one delta computation.
///
/// [`is_delta_efficient`]: crate::ring::interest::is_delta_efficient
pub(super) async fn fanout_send_needed(
    op_manager: &OpManager,
    key: &ContractKey,
    summaries: SummaryPair<'_>,
    probes_used: &mut usize,
) -> bool {
    match plan_fanout_send(&op_manager.interest_manager, key, summaries, *probes_used) {
        FanoutSendPlan::Send => true,
        FanoutSendPlan::Skip => false,
        FanoutSendPlan::Probe => {
            *probes_used += 1;
            let SummaryPair { ours, theirs } = summaries;
            // The probe API takes the pair positionally as (their, our) — the
            // other mapping `SummaryPair` exists to confine here. Transposing
            // these would compute delta(our state vs our OWN summary) =
            // always empty = wrongful skip of every broadcast.
            let verdict = op_manager
                .interest_manager
                .peer_summary_has_pending_state(op_manager, key, theirs, ours)
                .await;
            crate::ring::interest::summary_indicates_stale_peer(ours, theirs, verdict)
        }
    }
}

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

            // Phase 1.6 shadow telemetry (#4074): publish the post-mutation
            // depth while still under the lock so the depth gauge is exact;
            // the shadow demand aggregator reads it lock-free. Observation
            // only — see transport/shadow_demand.rs.
            crate::transport::shadow_demand::record_broadcast_queue_depth(queue.len());

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
                            let entry = q.pop_front();
                            // Phase 1.6 (#4074): publish post-drain depth
                            // under the lock for the shadow demand gauge.
                            crate::transport::shadow_demand::record_broadcast_queue_depth(q.len());
                            entry
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

/// Classify the result of awaiting the streaming completion oneshot into
/// "the message was actually delivered" vs "the permit can be released but the
/// message was dropped".
///
/// Issue #4235: the broadcast queue holds a semaphore permit for the duration
/// of a streaming broadcast and releases it when the completion signal fires.
/// The signal fires in *every* terminal case so the permit is never leaked —
/// including drops (peer channel closed, congestion timeout per #4145, no
/// connection, transport send error, cwnd-wait early return). Only a real
/// [`BroadcastDeliveryOutcome::Delivered`] must be treated as a send; treating
/// a drop as a delivery refreshes the peer's interest TTL on a transfer that
/// never landed and caches its summary, suppressing the next summary-mismatch
/// resend that should have detected the drop.
///
/// The argument is the result of `timeout(.., completion_rx).await`:
/// - `Ok(Ok(Delivered))` → delivered.
/// - `Ok(Ok(Dropped))`   → dropped (an explicit drop path signaled the permit).
/// - `Ok(Err(_))`        → dropped (oneshot dropped without a signal, e.g. the
///   cwnd-wait early return in `outbound_stream.rs`).
/// - `Err(_)`            → dropped (we timed out waiting for completion).
fn streaming_completion_delivered(completion: StreamCompletionResult) -> bool {
    matches!(completion, Ok(Ok(BroadcastDeliveryOutcome::Delivered)))
}

/// Result of awaiting the streaming completion oneshot under a timeout:
/// `timeout(.., completion_rx).await`. The inner `Ok`/`Err` distinguishes a
/// delivered/dropped signal from a dropped oneshot; the outer `Err` is the
/// wait timeout.
type StreamCompletionResult = Result<
    Result<BroadcastDeliveryOutcome, tokio::sync::oneshot::error::RecvError>,
    tokio::time::error::Elapsed,
>;

/// Apply the broadcast queue's post-send delivery gate to the interest manager.
///
/// This is the single production gate for #4235: it classifies the streaming
/// `completion` result and, ONLY on a real delivery, records the send telemetry,
/// refreshes the peer's interest TTL, and caches the peer summary. A drop or a
/// timeout releases the permit (handled by the caller) but must not touch the
/// interest manager — refreshing on a transfer that never landed extends the
/// peer's TTL falsely and caching the summary suppresses the next
/// summary-mismatch resend that should have re-sent the dropped state.
///
/// Returns the classified delivery outcome so the caller can log it.
///
/// The classification and the gated side effects are deliberately co-located in
/// one function so a regression test can drive the *real* gate. A future
/// refactor that mis-binds delivery here (e.g. reverting to a bare "the send was
/// enqueued" check) is caught by
/// `drop_outcome_does_not_refresh_interest_or_cache_summary`.
// The args mirror the streaming call site's locals; bundling them into a struct
// would obscure the (otherwise mechanical) gate this function exists to make
// testable.
#[allow(clippy::too_many_arguments)]
fn record_streaming_delivery<T: crate::util::time_source::TimeSource + Sync>(
    interest_manager: &crate::ring::interest::InterestManager<T>,
    completion: StreamCompletionResult,
    sent_delta: bool,
    key: &ContractKey,
    peer_key: &crate::ring::PeerKey,
    our_summary: Option<&freenet_stdlib::prelude::StateSummary<'static>>,
    state_size: usize,
    payload_size: usize,
) -> bool {
    let delivered = streaming_completion_delivered(completion);
    if delivered {
        record_delivery_to_interest(
            interest_manager,
            sent_delta,
            key,
            peer_key,
            our_summary,
            state_size,
            payload_size,
        );
    }
    delivered
}

/// The side effects a *delivered* broadcast applies to the interest manager:
/// record send telemetry, refresh the peer interest TTL, and cache the peer
/// summary (on ANY delivered broadcast — delta or full state — per #4145).
/// Factored out so both the streaming gate ([`record_streaming_delivery`]) and
/// the non-streaming path share one body.
fn record_delivery_to_interest<T: crate::util::time_source::TimeSource + Sync>(
    interest_manager: &crate::ring::interest::InterestManager<T>,
    sent_delta: bool,
    key: &ContractKey,
    peer_key: &crate::ring::PeerKey,
    our_summary: Option<&freenet_stdlib::prelude::StateSummary<'static>>,
    state_size: usize,
    payload_size: usize,
) {
    // Track delta vs full state sends for testing (PR #2763)
    if sent_delta {
        interest_manager.record_delta_send(state_size, payload_size);
        crate::config::GlobalTestMetrics::record_delta_send();
    } else {
        interest_manager.record_full_state_send();
        crate::config::GlobalTestMetrics::record_full_state_send();
    }

    // Issue #3046: Refresh the peer's interest TTL on every successful send
    interest_manager.refresh_peer_interest(key, peer_key);

    // Issue #4145: Cache the peer summary on ANY delivered broadcast — delta OR
    // full state — not just deltas.
    //
    // PR #2763 originally gated this on `sent_delta` because a streamed
    // full-state "success" didn't reliably mean the peer received the state:
    // caching `our_summary` for a peer that never got the state would make the
    // next delta unappliable (wrong base) and diverge. That gate created a
    // chicken-and-egg: a delta needs the peer's cached summary, but the summary
    // was only cached after a delta — so every NEW subscriber (and any peer
    // whose summary was cleared) starts on full state and is trapped sending
    // full state forever. Under sustained fan-out that is the #4233 full-state
    // broadcast storm.
    //
    // #4235 added a real-delivery signal (`BroadcastDeliveryOutcome::Delivered`).
    // This helper runs only on a delivered broadcast: for the streaming
    // (full-state) path the caller gates it behind
    // `record_streaming_delivery` → `streaming_completion_delivered`, and for
    // the non-streaming path it runs only inside the send-success arm.
    //
    // Caching `our_summary` on ANY delivered broadcast (delta or full state) is
    // safe even though `Delivered` is a SENDER-SIDE completion (the last
    // fragment was handed to the transport — see outbound_stream.rs ~434 — NOT a
    // receiver ACK), so on the streaming path a lost stream tail could leave the
    // peer without the state and the cached summary momentarily wrong. Two
    // backstops bound that window: the periodic InterestSync summary exchange
    // (~5 min, node.rs) re-reconciles what each peer actually has, and a delta
    // that fails to apply at the receiver triggers a ResyncRequest that clears
    // the sender's cached summary (node.rs ~2119). The streaming `Delivered`
    // signal is sender-side completion, so the rare tail-loss case is corrected
    // by those backstops rather than by an end-to-end ack here. Caching lets the
    // NEXT broadcast to this peer be a small delta instead of full state.
    // (Telemetry above still records delta-vs-full-state separately.)
    if let Some(summary) = our_summary {
        interest_manager.update_peer_summary(key, peer_key, Some(summary.clone()));
    }
}

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

    // Skip the summary/delta computation (and the per-peer send) entirely when
    // we hold no local state for `key`. The expensive `get_contract_summary`
    // call below is what drove the residual #4473 summarize storm on this
    // path-B caller. See `should_broadcast_contract`.
    if !should_broadcast_contract(op_manager, &key) {
        tracing::trace!(
            contract = %key,
            peer = %peer_addr,
            "Skipping broadcast - contract not hosted or in use"
        );
        return;
    }

    let peer_key = PeerKey::from(target.pub_key().clone());

    // Cost telemetry (cost-aware eviction, #4861): the per-target
    // summarize + delta computation below is send-time WASM work burned for
    // EVERY peer send of this contract — for a high-frequency tiny-payload
    // storm it is a dominant, previously-unmetered CPU cost. Time it via the
    // ring's injected TimeSource and attribute it on the `ExecCpuMicros`
    // axis (wall elapsed of the awaited computation; may include executor
    // queueing, which is still work this contract's send triggered).
    // Reported at BOTH exits (the summaries-equal skip and the payload
    // completion) so skipped sends still account for the summary work they
    // burned.
    let cost_clock = op_manager.ring.time_source.clone();
    let send_wasm_started = cost_clock.now();
    let report_send_wasm_cost = |op_manager: &Arc<OpManager>| {
        let elapsed = cost_clock
            .now()
            .saturating_duration_since(send_wasm_started);
        op_manager.ring.report_contract_resource_usage(
            *key.id(),
            crate::topology::meter::ResourceType::ExecCpuMicros,
            elapsed.as_micros() as f64,
        );
    };

    // Get our summary for delta computation
    let our_summary = op_manager
        .interest_manager
        .get_contract_summary(op_manager, &key)
        .await;

    // Get peer's cached summary
    let their_summary = op_manager
        .interest_manager
        .get_peer_summary(&key, &peer_key);

    // Semantic skip (#4894's fan-out counterpart). Byte-identical summaries
    // skip as before; byte-DIFFERING summaries are no longer trusted as proof
    // of divergence — a contract whose summary serializes
    // non-deterministically yields different bytes for the SAME logical
    // state, and re-sending full state for such a converged pair on every
    // fan-out is the nondeterministic-summary heal storm. `fanout_send_needed`
    // asks the shared delta cache / the contract itself (bounded probe)
    // whether the peer actually lacks state we hold.
    if let (Some(ours), Some(theirs)) = (&our_summary, &their_summary) {
        // Per-invocation probe budget: one (contract, peer) pair per call, so
        // at most one WASM probe per queue entry (see `fanout_send_needed`).
        let mut staleness_probes_used = 0usize;
        if !fanout_send_needed(
            op_manager,
            &key,
            SummaryPair { ours, theirs },
            &mut staleness_probes_used,
        )
        .await
        {
            tracing::trace!(
                contract = %key,
                peer = %peer_addr,
                "Skipping broadcast - peer already has our state (byte-equal \
                 or logically converged summaries)"
            );
            report_send_wasm_cost(op_manager);
            return;
        }
    }

    // Sender-side delta-incompatibility memo (the HQk7 resync loop): a
    // contract that repeatedly rejects deltas (its `update_state` only
    // accepts full states) turns every delta send into
    // delta → "Invalid update" → ResyncRequest → full-state resync → repeat.
    // While the memo is armed, skip the delta computation entirely and send
    // full state directly. See `crate::ring::delta_incompat`.
    let deltas_suppressed = op_manager.ring.delta_incompat.suppress_deltas(key.id());
    if deltas_suppressed {
        tracing::debug!(
            contract = %key,
            peer = %peer_addr,
            event = "delta_suppressed_incompat",
            "Contract is in delta-incompat backoff — sending full state instead of a delta"
        );
    }

    // Compute delta if we have their summary
    let (payload, sent_delta) = match (&our_summary, &their_summary) {
        _ if deltas_suppressed => (
            DeltaOrFullState::FullState(new_state.as_ref().to_vec()),
            false,
        ),
        (Some(ours), Some(theirs)) => {
            match op_manager
                .interest_manager
                .compute_delta(op_manager, &key, theirs, ours, new_state.size())
                .await
            {
                Ok(Some(delta)) => (DeltaOrFullState::Delta(delta.as_ref().to_vec()), true),
                Ok(None) => {
                    // The contract computed an EMPTY delta against the peer's
                    // summary: the peer is logically converged despite the
                    // byte-differing summaries. The pre-fix arm "fell back" to
                    // sending FULL STATE here, which is what re-flooded a
                    // converged-but-nondeterministic-summary contract on every
                    // fan-out (the heal storm). Nothing to send — skip.
                    tracing::trace!(
                        contract = %key,
                        peer = %peer_addr,
                        "Skipping broadcast - contract reported empty delta \
                         (peer converged)"
                    );
                    return;
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
    // Attribute the summarize + delta WASM burned for this send (#4861).
    report_send_wasm_cost(op_manager);

    let payload_size = payload.size();
    let update_tx = crate::message::Transaction::new::<crate::operations::update::UpdateMsg>();

    // Check if we should use streaming for full state broadcasts
    let use_streaming = matches!(&payload, DeltaOrFullState::FullState(_))
        && crate::operations::should_use_streaming(op_manager.streaming_threshold, payload_size);

    // Each branch below tracks whether the message was *actually delivered* to
    // the peer, as distinct from merely being enqueued for dispatch, and applies
    // the delivery gate itself. For the non-streaming path the two coincide (a
    // successful `bridge.send` is the terminal state we can observe). For the
    // streaming path they DON'T: the stream dispatch can be enqueued
    // successfully and then dropped (peer channel closed, congestion timeout per
    // #4145, no connection, transport error), and the completion oneshot fires
    // in all those cases purely to release the semaphore permit. Issue #4235:
    // only a real delivery should refresh the peer's interest TTL or cache its
    // summary — treating a drop as a delivery defeats the next summary-mismatch
    // round that would re-send the state.
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
        if send_res.is_err() {
            // Telemetry gauge (#4440): the initial metadata send failed, so the
            // streaming broadcast never landed. This is a real streaming-
            // broadcast failure — and exactly the congestion failure mode that
            // would otherwise bias the gauge LOW when it matters most.
            BROADCAST_STREAM_METRICS.record_attempt(false);
        } else {
            // Create completion channel for the broadcast queue to track
            // when the actual stream transfer finishes.
            let (completion_tx, completion_rx) = tokio::sync::oneshot::channel();

            // channel-safety: ok — broadcast_to_single_peer runs on the detached
            // broadcast-queue task, not the event loop; the StreamSend this
            // enqueues is drained by the loop, so it cannot self-stall it. The
            // #4001 `None` progress arg that brought this statement into the diff
            // does not change the send path.
            if let Err(err) = bridge
                .send_stream_with_completion(
                    peer_addr,
                    sid,
                    bytes::Bytes::from(payload_bytes),
                    metadata,
                    Some(completion_tx),
                    None,
                )
                .await
            {
                // Telemetry gauge (#4440): stream dispatch failed before any
                // fragment was handed to the transport — also a streaming-
                // broadcast failure.
                BROADCAST_STREAM_METRICS.record_attempt(false);
                tracing::warn!(
                    tx = %update_tx,
                    peer = %peer_addr,
                    error = %err,
                    "Failed to send broadcast stream data"
                );
            } else {
                // Wait for the stream transfer to actually complete before
                // releasing back to the queue worker (semaphore permit is held
                // by our caller). Timeout prevents permanent stall. The
                // completion signal carries a `BroadcastDeliveryOutcome` so we
                // distinguish a real delivery from a drop (#4235); a drop still
                // releases the permit but must NOT be recorded as a send.
                let completion =
                    tokio::time::timeout(STREAM_COMPLETION_TIMEOUT, completion_rx).await;
                // Classify AND apply the delivery gate in one production call
                // (#4235): only a real `Delivered` refreshes interest / caches
                // the summary. See `record_streaming_delivery`.
                let delivered = record_streaming_delivery(
                    &op_manager.interest_manager,
                    completion,
                    sent_delta,
                    &key,
                    &peer_key,
                    our_summary.as_ref(),
                    new_state.size(),
                    payload_size,
                );
                // Telemetry gauge (#4440): post-dispatch outcome — a drop,
                // dropped completion oneshot, or completion timeout is the
                // stream-assembly / transfer failure (`!delivered`). Together
                // with the two earlier exits above, exactly one
                // `record_attempt` fires per streaming broadcast invocation,
                // covering initial-send failure, stream-dispatch failure, and
                // post-dispatch drop/timeout/dropped-oneshot. Process-global
                // counter, read on the router_snapshot cadence — NOT a
                // per-failure event. This is the exact signal that flagged the
                // v0.2.73 incident.
                BROADCAST_STREAM_METRICS.record_attempt(delivered);
                if delivered {
                    tracing::debug!(
                        tx = %update_tx,
                        peer = %peer_addr,
                        "Broadcast stream completed successfully"
                    );
                } else {
                    tracing::debug!(
                        tx = %update_tx,
                        peer = %peer_addr,
                        timeout_secs = STREAM_COMPLETION_TIMEOUT.as_secs(),
                        "Broadcast stream dropped or timed out before delivery \
                         (permit released, interest NOT refreshed)"
                    );
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
        let res = bridge.send(peer_addr, msg.into()).await;
        // Non-streaming inline broadcasts have no separate transfer phase: a
        // successful enqueue is the terminal state we can observe, so delivery
        // tracks the send result (unchanged pre-#4235 behavior for this path).
        if res.is_ok() {
            // Delta-incompat attribution (HQk7 resync loop): remember that we
            // just delivered a DELTA to this peer so a prompt `ResyncRequest`
            // from it can be attributed to the delta failing to apply (deltas
            // only ever take this inline path — streaming is full-state-only).
            // See `crate::ring::delta_incompat`.
            if sent_delta {
                op_manager
                    .ring
                    .delta_incompat
                    .record_delta_sent(*key.id(), peer_addr);
            }
            // Record telemetry, refresh peer interest, and cache the peer
            // summary — see `record_delivery_to_interest`. The streaming branch
            // applies the same gate via `record_streaming_delivery` (#4235);
            // this inline branch shares that body.
            record_delivery_to_interest(
                &op_manager.interest_manager,
                sent_delta,
                &key,
                &peer_key,
                our_summary.as_ref(),
                new_state.size(),
                payload_size,
            );
        }
        res
    };

    if let Err(err) = &send_result {
        tracing::warn!(
            tx = %update_tx,
            peer = %peer_addr,
            error = %err,
            "Failed to send state change broadcast (queued)"
        );
    }

    // NOTE: telemetry / interest-refresh / summary-cache are intentionally NOT
    // applied here. Issue #4235: each branch above applies the delivery gate
    // itself — the streaming branch via `record_streaming_delivery` (gated on a
    // real `Delivered` completion, NOT on the enqueue succeeding), the inline
    // branch via `record_delivery_to_interest` (gated on the send succeeding). A
    // dropped stream still released the permit but must not refresh interest or
    // cache the summary.
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use freenet_stdlib::prelude::{
        CodeHash, ContractInstanceId, ContractKey, StateDelta, StateSummary,
    };

    use crate::ring::PeerKey;
    use crate::ring::interest::InterestManager;
    use crate::transport::{BroadcastDeliveryOutcome, TransportKeypair};
    use crate::util::time_source::SharedMockTimeSource;

    use super::{
        BroadcastStreamMetrics, FanoutSendPlan, SummaryPair, plan_fanout_send,
        record_streaming_delivery, streaming_completion_delivered,
    };

    /// `BroadcastStreamMetrics` counts every attempt and, separately, only the
    /// non-`delivered` attempts (#4440). Tests a LOCAL instance so it stays
    /// deterministic and never touches the concurrently-shared process-global
    /// `BROADCAST_STREAM_METRICS`.
    #[test]
    fn broadcast_stream_metrics_counts_attempts_and_failures() {
        let m = BroadcastStreamMetrics::new();
        let s = m.snapshot();
        assert_eq!(s.streaming_attempts_total, 0, "starts at zero");
        assert_eq!(s.streaming_failures_total, 0, "starts at zero");

        // A delivered attempt bumps attempts only.
        m.record_attempt(true);
        let s = m.snapshot();
        assert_eq!(s.streaming_attempts_total, 1);
        assert_eq!(s.streaming_failures_total, 0, "delivered is not a failure");

        // A non-delivered attempt bumps both — this is the stream-assembly
        // failure signal that flagged the v0.2.73 incident.
        m.record_attempt(false);
        let s = m.snapshot();
        assert_eq!(s.streaming_attempts_total, 2, "every attempt counts");
        assert_eq!(s.streaming_failures_total, 1, "the drop is counted");

        // Counters are monotonic and accumulate.
        m.record_attempt(false);
        m.record_attempt(true);
        let s = m.snapshot();
        assert_eq!(s.streaming_attempts_total, 4);
        assert_eq!(s.streaming_failures_total, 2);
    }

    fn make_contract_key(seed: u8) -> ContractKey {
        ContractKey::from_id_and_code(
            ContractInstanceId::new([seed; 32]),
            CodeHash::new([seed.wrapping_add(1); 32]),
        )
    }

    fn make_peer_key() -> PeerKey {
        PeerKey(TransportKeypair::new().public().clone())
    }

    /// A `RecvError` modeling the oneshot being dropped without a signal — the
    /// path `outbound_stream.rs` takes on a cwnd-wait early return. Awaiting a
    /// oneshot whose sender was dropped resolves to `Err(RecvError)`.
    async fn dropped_oneshot()
    -> Result<BroadcastDeliveryOutcome, tokio::sync::oneshot::error::RecvError> {
        let (tx, rx) = tokio::sync::oneshot::channel::<BroadcastDeliveryOutcome>();
        drop(tx);
        rx.await.map(|_| unreachable!("sender was dropped"))
    }

    /// An `Elapsed` modeling the broadcast queue timing out waiting for the
    /// completion signal.
    async fn elapsed_timeout() -> tokio::time::error::Elapsed {
        let (tx, rx) = tokio::sync::oneshot::channel::<BroadcastDeliveryOutcome>();
        // Keep tx alive so rx never resolves; force the timeout to elapse.
        let res = tokio::time::timeout(Duration::from_millis(1), rx).await;
        drop(tx);
        res.expect_err("never-resolving recv must time out")
    }

    /// Issue #4235 — core regression: ONLY an explicit `Delivered` outcome is a
    /// delivery. Every other completion result (an explicit `Dropped`, an
    /// oneshot dropped without a signal, or a wait timeout) is NOT a delivery
    /// even though all of them release the permit.
    ///
    /// Pre-fix the queue computed `send_ok = send_result.is_ok()`, which was
    /// `true` for the timeout and dropped-oneshot cases (the send had been
    /// enqueued), so those falsely counted as deliveries. The assertions on the
    /// `Dropped` / `Ok(Err)` / `Err(Elapsed)` cases below FAIL against that old
    /// logic.
    #[tokio::test]
    async fn streaming_completion_delivered_only_on_explicit_delivery() {
        // Real delivery → counts as delivered.
        assert!(
            streaming_completion_delivered(Ok(Ok(BroadcastDeliveryOutcome::Delivered))),
            "an explicit Delivered outcome must be treated as a delivery"
        );

        // Explicit drop (peer channel closed / congestion timeout #4145 /
        // no connection / transport send error) → NOT a delivery.
        assert!(
            !streaming_completion_delivered(Ok(Ok(BroadcastDeliveryOutcome::Dropped))),
            "an explicit Dropped outcome must NOT be treated as a delivery (#4235)"
        );

        // Oneshot dropped without a signal (cwnd-wait early return) →
        // NOT a delivery.
        assert!(
            !streaming_completion_delivered(Ok(dropped_oneshot().await)),
            "a dropped completion oneshot must NOT be treated as a delivery (#4235)"
        );

        // Queue timed out waiting for completion → NOT a delivery.
        assert!(
            !streaming_completion_delivered(Err(elapsed_timeout().await)),
            "a completion-wait timeout must NOT be treated as a delivery (#4235)"
        );
    }

    /// Issue #4235 — production-gate regression: drives the REAL gate the
    /// broadcast queue's streaming path applies — [`record_streaming_delivery`],
    /// the smallest extractable production unit that both classifies the
    /// completion result AND applies the side effects (record send / refresh
    /// interest TTL / cache summary) — against a real `InterestManager`, once
    /// per completion outcome.
    ///
    /// Unlike [`streaming_completion_delivered_only_on_explicit_delivery`],
    /// which guards the classifier helper in isolation, this test invokes the
    /// production gate function the queue actually calls. It therefore FAILS if
    /// a refactor reverts the production gate binding — e.g. switching
    /// `record_streaming_delivery` to apply the side effects unconditionally or
    /// on a bare "the send was enqueued" check rather than on a real
    /// `Delivered` outcome — even if the standalone classifier stays correct.
    ///
    /// Proves the user-visible consequence of the conflation: when the stream
    /// dispatch drops/times-out the message, the peer's interest TTL is NOT
    /// refreshed and its summary is NOT cached — so the next summary-mismatch
    /// round still fires — while a genuine delivery does refresh and cache.
    #[tokio::test]
    async fn drop_outcome_does_not_refresh_interest_or_cache_summary() {
        let our_summary = StateSummary::from(vec![9, 9, 9, 9]);

        // Each case pairs a completion result with whether it should be a
        // delivery.
        let dropped = dropped_oneshot().await;
        let timed_out = elapsed_timeout().await;
        let cases: Vec<(&str, super::StreamCompletionResult, bool)> = vec![
            (
                "delivered",
                Ok(Ok(BroadcastDeliveryOutcome::Delivered)),
                true,
            ),
            (
                "explicit-drop",
                Ok(Ok(BroadcastDeliveryOutcome::Dropped)),
                false,
            ),
            ("dropped-oneshot", Ok(dropped), false),
            ("timeout", Err(timed_out), false),
        ];

        for (name, completion, expect_delivered) in cases {
            let time_source = SharedMockTimeSource::new();
            let manager = InterestManager::new(time_source.clone());
            let contract = make_contract_key(7);
            let peer = make_peer_key();

            // Peer is interested but has NO cached summary yet (mimics a peer
            // whose summary mismatches ours, so a broadcast is queued).
            manager.register_peer_interest(&contract, peer.clone(), None, false);
            let baseline = manager
                .get_peer_interest(&contract, &peer)
                .expect("peer interest registered")
                .last_refreshed;

            // Let wall-clock advance so a refresh would be observable.
            time_source.advance_time(Duration::from_secs(5));

            // Drive the REAL production gate. `sent_delta = true` so the summary
            // cache (`update_peer_summary`) is exercised on the delivered arm.
            let delivered = record_streaming_delivery(
                &manager,
                completion,
                /* sent_delta */ true,
                &contract,
                &peer,
                Some(&our_summary),
                /* state_size */ 1024,
                /* payload_size */ 64,
            );
            assert_eq!(
                delivered, expect_delivered,
                "[{name}] classification mismatch"
            );

            let interest = manager
                .get_peer_interest(&contract, &peer)
                .expect("peer interest still registered");

            if expect_delivered {
                assert!(
                    interest.last_refreshed > baseline,
                    "[{name}] a real delivery MUST refresh the peer interest TTL"
                );
                assert_eq!(
                    manager.get_peer_summary(&contract, &peer),
                    Some(our_summary.clone()),
                    "[{name}] a real delivery MUST cache the peer summary"
                );
            } else {
                assert_eq!(
                    interest.last_refreshed, baseline,
                    "[{name}] a dropped/timed-out broadcast MUST NOT refresh the \
                     peer interest TTL (#4235)"
                );
                assert_eq!(
                    manager.get_peer_summary(&contract, &peer),
                    None,
                    "[{name}] a dropped/timed-out broadcast MUST NOT cache the peer \
                     summary, or the next summary-mismatch resend is suppressed (#4235)"
                );
            }
        }
    }

    /// Issue #4145 — the chicken-and-egg fix. A peer that starts with NO cached
    /// summary receives a *full-state* broadcast (`sent_delta = false`). After a
    /// real delivery its summary MUST be cached, so the NEXT broadcast can be a
    /// small delta instead of full state again.
    ///
    /// This is the bug #4145/#4233 describe: PR #2763 gated the summary cache on
    /// `sent_delta`, so a peer bootstrapped on full state never got a cached
    /// summary and was trapped sending full state forever (the broadcast storm).
    ///
    /// Pre-fix (`if sent_delta { update_peer_summary(..) }`) the `sent_delta =
    /// false` call below cached nothing, so `get_peer_summary` would stay `None`
    /// and this test FAILS. With the fix it caches on any delivery and the
    /// summary is present, mirroring the precondition
    /// `broadcast_to_single_peer` checks (a present peer summary → `compute_delta`
    /// → `sent_delta = true`) on the subsequent broadcast.
    #[tokio::test]
    async fn full_state_delivery_caches_summary_so_next_broadcast_is_delta() {
        let our_summary = StateSummary::from(vec![1, 2, 3, 4]);

        let time_source = SharedMockTimeSource::new();
        let manager = InterestManager::new(time_source.clone());
        let contract = make_contract_key(42);
        let peer = make_peer_key();

        // New subscriber: interested, but no cached summary yet — exactly the
        // state that forces a full-state broadcast on the first send.
        manager.register_peer_interest(&contract, peer.clone(), None, false);
        assert_eq!(
            manager.get_peer_summary(&contract, &peer),
            None,
            "precondition: a brand-new subscriber has no cached summary, so the \
             first broadcast must be full state"
        );

        // A FULL-STATE broadcast (`sent_delta = false`) is really Delivered.
        let delivered = record_streaming_delivery(
            &manager,
            Ok(Ok(BroadcastDeliveryOutcome::Delivered)),
            /* sent_delta */ false,
            &contract,
            &peer,
            Some(&our_summary),
            /* state_size */ 4096,
            /* payload_size */ 4096,
        );
        assert!(delivered, "a Delivered outcome must classify as delivered");

        // #4145 FIX: the summary is now cached even though we sent FULL STATE.
        // This is the assertion that fails on the old `if sent_delta` gate.
        assert_eq!(
            manager.get_peer_summary(&contract, &peer),
            Some(our_summary.clone()),
            "#4145: a delivered FULL-STATE broadcast must cache the peer summary, \
             so the next broadcast can be a delta — otherwise the peer is trapped \
             sending full state forever (the #4233 storm)"
        );

        // The cached summary is the exact precondition `broadcast_to_single_peer`
        // uses to compute a delta: `their_summary = get_peer_summary(..)` being
        // `Some` drives the delta branch (`sent_delta = true`) next time.
        let their_summary = manager.get_peer_summary(&contract, &peer);
        assert!(
            their_summary.is_some(),
            "#4145: with a cached peer summary the next broadcast takes the delta \
             path (compute_delta), not another full state"
        );
    }

    /// Issue #4145 / #2763 — divergence guard preserved. The #4145 fix caches on
    /// any *delivered* broadcast, but a DROPPED full-state stream (peer never
    /// received the state) MUST still NOT cache the summary — otherwise the next
    /// delta would be computed against a base the peer doesn't have, and the
    /// summary-mismatch resend that should re-send the state is suppressed.
    ///
    /// This is the full-state (`sent_delta = false`) counterpart to
    /// [`drop_outcome_does_not_refresh_interest_or_cache_summary`], pinning that
    /// the #4145 change did NOT weaken the #4235/#2763 drop guard for full state.
    #[tokio::test]
    async fn dropped_full_state_stream_does_not_cache_summary() {
        let our_summary = StateSummary::from(vec![5, 6, 7, 8]);

        let dropped = dropped_oneshot().await;
        let timed_out = elapsed_timeout().await;
        // Every non-delivery completion for a FULL-STATE (`sent_delta = false`)
        // stream must leave the summary uncached.
        let cases: Vec<(&str, super::StreamCompletionResult)> = vec![
            ("explicit-drop", Ok(Ok(BroadcastDeliveryOutcome::Dropped))),
            ("dropped-oneshot", Ok(dropped)),
            ("timeout", Err(timed_out)),
        ];

        for (name, completion) in cases {
            let time_source = SharedMockTimeSource::new();
            let manager = InterestManager::new(time_source.clone());
            let contract = make_contract_key(43);
            let peer = make_peer_key();

            manager.register_peer_interest(&contract, peer.clone(), None, false);

            let delivered = record_streaming_delivery(
                &manager,
                completion,
                /* sent_delta */ false,
                &contract,
                &peer,
                Some(&our_summary),
                /* state_size */ 4096,
                /* payload_size */ 4096,
            );
            assert!(
                !delivered,
                "[{name}] a dropped/timed-out full-state stream must NOT classify \
                 as delivered"
            );
            assert_eq!(
                manager.get_peer_summary(&contract, &peer),
                None,
                "[{name}] #4145 must not weaken the #2763/#4235 guard: a DROPPED \
                 full-state stream must NOT cache the summary (the peer never got \
                 the state), or the next summary-mismatch resend is suppressed"
            );
        }
    }

    /// Regression pin for the #4473 path-B summarize storm (counterpart to
    /// #4475's `interest_sync_periodic_arms_summarize_only_hosted_or_in_use_pin`).
    ///
    /// #4475 gated the interest-sync summarize sites (path A) but left the
    /// broadcast fan-out caller ungated: `broadcast_to_single_peer` called
    /// `get_contract_summary` (→ `summarize_contract_state`) once per
    /// (broadcast × target) with NO hosting/in-use gate, driving the residual
    /// ~100k/hr "Contract state not found in store" WARNs observed on nova for a
    /// small phantom set of contracts the node holds no state for. The fix gates
    /// the expensive summarize on `should_broadcast_contract`
    /// (`is_hosting_contract || contract_in_use`) BEFORE the
    /// `get_contract_summary` call.
    ///
    /// This pin fails on the pre-fix code (an ungated `get_contract_summary` in
    /// `broadcast_to_single_peer`) and guards against a future migration
    /// hand-inlining the per-peer body and dropping the gate again.
    #[test]
    fn broadcast_single_peer_gates_summarize_on_hosted_or_in_use_pin() {
        let src = include_str!("broadcast_queue.rs");

        // 1. The gate helper must delegate to the SINGLE composed predicate
        //    `Ring::should_summarize_or_broadcast` =
        //    `(is_hosting_contract || contract_in_use) && contract_state_present`,
        //    shared with node.rs::summary_if_hosted_or_in_use. The composition
        //    (incl. the load-bearing `&&` vs `||` that keeps phantom stateless
        //    contracts out — #4610) is behaviourally verified by
        //    `summarize_gate_skips_stateless_phantom_keeps_stateful_4610` in
        //    ring/hosting.rs. Here we only pin the delegation, so a future edit
        //    cannot re-inline a partial (is_hosting || in_use) gate.
        let helper_start = src
            .find("pub(super) fn should_broadcast_contract(")
            .expect("should_broadcast_contract helper not found");
        let helper_end = helper_start
            + src[helper_start..]
                .find("\n}\n")
                .expect("should_broadcast_contract body end not found");
        let helper_src = &src[helper_start..helper_end];
        assert!(
            helper_src.contains("should_summarize_or_broadcast"),
            "should_broadcast_contract must delegate to the composed \
             should_summarize_or_broadcast predicate (single source of truth, \
             #4610), not re-inline a partial (is_hosting || in_use) gate that \
             would re-admit phantom stateless contracts"
        );

        // 2. `broadcast_to_single_peer` must call the gate BEFORE the expensive
        //    `get_contract_summary`. Slice the function body and assert the gate
        //    call precedes the first `get_contract_summary(` in it.
        let fn_start = src
            .find("pub(super) async fn broadcast_to_single_peer(")
            .expect("broadcast_to_single_peer not found");
        let fn_src = &src[fn_start..];
        let gate_off = fn_src.find("should_broadcast_contract(op_manager").expect(
            "broadcast_to_single_peer must call should_broadcast_contract — a bare \
             get_contract_summary here reintroduces the #4473 storm",
        );
        let summarize_off = fn_src
            .find("get_contract_summary(")
            .expect("broadcast_to_single_peer get_contract_summary call not found");
        assert!(
            gate_off < summarize_off,
            "broadcast_to_single_peer must gate on should_broadcast_contract BEFORE \
             calling get_contract_summary (#4473) — otherwise the summarize storm \
             fires for every phantom contract before the gate can skip it"
        );
    }

    /// Source-scrape pin (HQk7 resync loop): `broadcast_to_single_peer` must
    /// consult the sender-side delta-incompatibility memo BEFORE computing a
    /// delta, and must record every delivered delta send for ResyncRequest
    /// attribution. If the gate is dropped (or moved after `compute_delta`),
    /// a delta-incapable contract goes back to
    /// delta → "Invalid update" → ResyncRequest → full-state resync → repeat
    /// (3,102 delta_apply_failed events for one contract in a 2h production
    /// window); if the attribution recording is dropped, the memo's
    /// sender-side arm signal (`note_resync_request`) can never fire.
    /// See `crate::ring::delta_incompat`.
    #[test]
    fn broadcast_to_single_peer_gates_deltas_on_incompat_memo() {
        let src = include_str!("broadcast_queue.rs");
        let fn_start = src
            .find("pub(super) async fn broadcast_to_single_peer(")
            .expect("broadcast_to_single_peer not found");
        let after = &src[fn_start..];
        let fn_end = after
            .find("\nmod tests {")
            .or_else(|| after.find("\n#[cfg(test)]"))
            .expect("end of broadcast_to_single_peer not found");
        let body = &after[..fn_end];

        // 1. The memo gate must run BEFORE the delta computation and map
        //    suppression to a full-state payload.
        let gate_pos = body
            .find(".suppress_deltas(")
            .expect("broadcast_to_single_peer must consult the delta-incompat memo");
        let delta_pos = body
            .find(".compute_delta(")
            .expect("compute_delta call not found");
        assert!(
            gate_pos < delta_pos,
            "the delta-incompat gate must be consulted BEFORE compute_delta \
             (gate {gate_pos} < compute_delta {delta_pos}) — otherwise the \
             doomed delta is still computed and sent"
        );
        assert!(
            body.contains("_ if deltas_suppressed => ("),
            "suppression must short-circuit the payload match to FullState"
        );
        // The guard arm must be FIRST in the payload match: Rust evaluates
        // arms in order, so `_ if deltas_suppressed` has to precede the
        // `(Some(ours), Some(theirs))` compute_delta arm — otherwise a
        // suppressed contract with both summaries present would compute and
        // send the doomed delta (or, post-#4901, hit the Ok(None) converged
        // skip) instead of forcing full state. The `.suppress_deltas(` call
        // above precedes the match regardless, so only this arm-ordering
        // assertion catches a reordering regression.
        let guard_arm = body
            .find("_ if deltas_suppressed => (")
            .expect("guard arm not found");
        let compute_arm = body
            .find("(Some(ours), Some(theirs)) => {")
            .expect("compute_delta arm `(Some(ours), Some(theirs))` not found");
        assert!(
            guard_arm < compute_arm,
            "the `_ if deltas_suppressed` guard arm must come BEFORE the \
             `(Some(ours), Some(theirs))` compute_delta arm (guard {guard_arm} \
             < compute {compute_arm}) — a suppressed delta-incapable contract \
             must never reach compute_delta"
        );

        // 2. Delivered delta sends must be recorded for ResyncRequest
        //    attribution, gated on sent_delta (full-state sends must NOT
        //    create attributions — a resync after a full-state send says
        //    nothing about delta compatibility).
        let record_pos = body
            .find(".record_delta_sent(")
            .expect("broadcast_to_single_peer must record delivered delta sends");
        let sent_delta_gate = body
            .find("if sent_delta {")
            .expect("record_delta_sent must be gated on sent_delta");
        assert!(
            sent_delta_gate < record_pos,
            "record_delta_sent must sit inside the `if sent_delta` gate \
             (gate {sent_delta_gate} < record {record_pos})"
        );
    }

    /// Source-scrape pin: the streaming branch of `broadcast_to_single_peer`
    /// must record the broadcast-stream gauge on ALL THREE of its exits (#4440),
    /// not just the success arm. The two early-failure exits — initial metadata
    /// `bridge.send(...)` returning Err, and `send_stream_with_completion(...)`
    /// returning Err — are exactly the congestion failure mode the v0.2.73
    /// incident exhibited. If a future edit drops one of those
    /// `record_attempt(false)` calls, the gauge would silently undercount and
    /// bias the incident signal LOW precisely when it matters most, with no
    /// test failure otherwise. (The post-dispatch `record_attempt(delivered)` is
    /// the third site.)
    ///
    /// Asserting against the process-global `BROADCAST_STREAM_METRICS` after
    /// running the broadcast would be racy (concurrent tests share the global),
    /// so this pins the call sites in source instead — mirroring
    /// `migration_counter_sites_present` in `ring/placement_migration_metrics.rs`.
    #[test]
    fn broadcast_to_single_peer_records_attempt_on_every_streaming_exit_pin() {
        let src = include_str!("broadcast_queue.rs");
        // Slice the `broadcast_to_single_peer` fn body so the unrelated
        // `record_attempt` calls in the metrics unit test (and this test's own
        // docs) don't count: from its signature to the start of the next fn.
        let fn_start = src
            .find("pub(super) async fn broadcast_to_single_peer(")
            .expect("broadcast_to_single_peer not found");
        let after = &src[fn_start..];
        // The next item after the fn is the `#[cfg(test)] mod tests`.
        let fn_end = after
            .find("\nmod tests {")
            .or_else(|| after.find("\n#[cfg(test)]"))
            .expect("end of broadcast_to_single_peer (start of tests module) not found");
        let body = &after[..fn_end];

        let record_calls = body.matches(".record_attempt(").count();
        assert_eq!(
            record_calls, 3,
            "broadcast_to_single_peer's streaming branch must call record_attempt \
             on all three exits (initial-send Err, dispatch Err, post-dispatch \
             outcome) — got {record_calls}. A dropped early-exit record silently \
             biases the v0.2.73 incident gauge LOW under congestion."
        );
        // Two of the three must be the explicit-failure form, so a refactor that
        // collapses an early exit into the success path (losing the `false`)
        // also trips this pin.
        let failure_calls = body.matches(".record_attempt(false)").count();
        assert_eq!(
            failure_calls, 2,
            "exactly the two early-failure exits must record record_attempt(false) \
             (got {failure_calls}); the third exit records record_attempt(delivered)"
        );
    }

    // ---- Semantic fan-out skip (#4894's fan-out counterpart / the ----------
    // ---- nondeterministic-summary heal storm) ------------------------------
    //
    // The live broadcast fan-out used to skip a peer only on BYTE-identical
    // summaries. A contract whose summary serializes non-deterministically
    // (HashMap/HashSet order) yields different bytes for the SAME logical
    // state across peers, so the byte compare never skipped; the delta path
    // then either returned an empty delta (which the pre-fix arm answered by
    // sending FULL STATE) or was refused by the `is_delta_efficient` gate on
    // big-summary contracts — so a fully-converged pair re-flooded full state
    // on every fan-out (contracts like `Eumk9HNQ` healing hard while their
    // state never changed). These tests exercise the cache-only decision core
    // `plan_fanout_send`; the wiring is pinned by
    // `fanout_path_uses_semantic_delta_skip_pin`.

    fn make_manager() -> InterestManager<SharedMockTimeSource> {
        InterestManager::new(SharedMockTimeSource::new())
    }

    /// Reproducing test (mirrors #4894's
    /// `nondeterministic_summary_does_not_flag_converged_peer_stale`): two
    /// peers with the SAME logical state but byte-differing summaries must NOT
    /// be re-sent full state by the fan-out once the contract has said the
    /// pair is converged (empty delta).
    #[test]
    fn nondeterministic_converged_summaries_skip_fanout_resend() {
        let manager = make_manager();
        let contract = make_contract_key(50);

        // Two summaries of the SAME logical state that serialize to DIFFERENT
        // bytes (models cross-peer HashMap/HashSet iteration-order divergence).
        let ours = StateSummary::from(vec![1u8, 2, 3]);
        let theirs = StateSummary::from(vec![3u8, 2, 1]);
        assert_ne!(
            ours.as_ref(),
            theirs.as_ref(),
            "precondition: summaries differ byte-wise (the pre-fix byte-compare \
             would NOT skip, and the delta path fell back to full state)"
        );

        // The contract, asked for the delta of our state against their
        // summary, returned EMPTY: logically converged. Model it exactly as
        // production does — via the shared delta cache.
        manager.cache_delta(
            &contract,
            theirs.as_ref(),
            ours.as_ref(),
            StateDelta::from(Vec::<u8>::new()),
        );

        // FIX: the fan-out must SKIP this peer — no full-state re-flood.
        assert_eq!(
            plan_fanout_send(
                &manager,
                &contract,
                SummaryPair {
                    ours: &ours,
                    theirs: &theirs
                },
                0
            ),
            FanoutSendPlan::Skip,
            "a converged-but-byte-differing pair must be skipped by the fan-out \
             (pre-fix: full state was re-sent on every fan-out — the heal storm)"
        );
    }

    /// Convergence safety: a genuinely diverged pair (non-empty delta) must
    /// STILL be sent/healed — the fix only removes spurious re-sends.
    #[test]
    fn genuinely_diverged_summaries_still_send() {
        let manager = make_manager();
        let contract = make_contract_key(51);

        let ours = StateSummary::from(vec![9u8, 9, 9]);
        let theirs = StateSummary::from(vec![1u8]);

        manager.cache_delta(
            &contract,
            theirs.as_ref(),
            ours.as_ref(),
            StateDelta::from(vec![42u8]),
        );

        assert_eq!(
            plan_fanout_send(
                &manager,
                &contract,
                SummaryPair {
                    ours: &ours,
                    theirs: &theirs
                },
                0
            ),
            FanoutSendPlan::Send,
            "a genuine divergence (non-empty delta) must still be sent"
        );
    }

    /// Byte-identical summaries skip WITHOUT consulting the cache or spending
    /// probe budget — even a (stale, cross-contract-polluted) cached non-empty
    /// delta for the same byte pair must not force a send.
    #[test]
    fn byte_equal_summaries_skip_before_cache_lookup() {
        let manager = make_manager();
        let contract = make_contract_key(52);

        let ours = StateSummary::from(vec![7u8, 7, 7]);
        let theirs = StateSummary::from(vec![7u8, 7, 7]);

        // Poison the cache for this (equal-bytes) pair: the byte-equal
        // short-circuit must win regardless.
        manager.cache_delta(
            &contract,
            theirs.as_ref(),
            ours.as_ref(),
            StateDelta::from(vec![1u8]),
        );

        assert_eq!(
            plan_fanout_send(
                &manager,
                &contract,
                SummaryPair {
                    ours: &ours,
                    theirs: &theirs
                },
                0
            ),
            FanoutSendPlan::Skip,
            "byte-identical summaries are trivially converged; the byte-equal \
             short-circuit must precede any delta-cache verdict"
        );
    }

    /// Per-invocation probe cap (mirrors the `Summaries` handler's
    /// `MAX_STALENESS_PROBES_PER_SUMMARIES` budget): a cache MISS probes only
    /// while budget remains; once exhausted the plan falls back to the
    /// conservative byte-differ ⇒ send behavior instead of probing — never to
    /// a silent skip.
    #[test]
    fn probe_budget_gates_wasm_probe_and_falls_back_to_send() {
        use crate::node::MAX_STALENESS_PROBES_PER_SUMMARIES;

        let manager = make_manager();
        let contract = make_contract_key(53);

        let ours = StateSummary::from(vec![1u8, 2, 3]);
        let theirs = StateSummary::from(vec![3u8, 2, 1]);

        // No cached verdict, budget available → probe the contract.
        assert_eq!(
            plan_fanout_send(
                &manager,
                &contract,
                SummaryPair {
                    ours: &ours,
                    theirs: &theirs
                },
                0
            ),
            FanoutSendPlan::Probe,
            "a cache miss within budget must run the bounded WASM probe"
        );
        assert_eq!(
            plan_fanout_send(
                &manager,
                &contract,
                SummaryPair {
                    ours: &ours,
                    theirs: &theirs
                },
                MAX_STALENESS_PROBES_PER_SUMMARIES - 1
            ),
            FanoutSendPlan::Probe,
            "the last budget slot is still spendable"
        );

        // Budget exhausted → conservative SEND (byte-differ fallback), no probe.
        assert_eq!(
            plan_fanout_send(
                &manager,
                &contract,
                SummaryPair {
                    ours: &ours,
                    theirs: &theirs
                },
                MAX_STALENESS_PROBES_PER_SUMMARIES
            ),
            FanoutSendPlan::Send,
            "an exhausted probe budget must fall back to the conservative \
             byte-differ ⇒ send behavior, never a silent skip"
        );

        // A cache HIT is free: it answers even with the budget exhausted.
        manager.cache_delta(
            &contract,
            theirs.as_ref(),
            ours.as_ref(),
            StateDelta::from(Vec::<u8>::new()),
        );
        assert_eq!(
            plan_fanout_send(
                &manager,
                &contract,
                SummaryPair {
                    ours: &ours,
                    theirs: &theirs
                },
                MAX_STALENESS_PROBES_PER_SUMMARIES * 10
            ),
            FanoutSendPlan::Skip,
            "cache hits never consume budget and still answer (converged ⇒ skip)"
        );
    }

    /// Source-scrape pin: the fan-out path must decide the per-peer send
    /// SEMANTICALLY — routing through `fanout_send_needed` (the
    /// `plan_fanout_send` cache layer + the bounded
    /// `peer_summary_has_pending_state` contract probe +
    /// `summary_indicates_stale_peer` policy) — and the `compute_delta`
    /// `Ok(None)` (empty delta = converged) arm must SKIP, not fall back to
    /// full state. Mirrors node.rs's
    /// `summaries_arm_uses_semantic_staleness_probe_pin`: the data-layer unit
    /// tests above stay green even if `broadcast_to_single_peer` is reverted
    /// to a bare byte compare + full-state fallback (re-opening the
    /// nondeterministic-summary heal storm), so this pins the WIRING.
    #[test]
    fn fanout_path_uses_semantic_delta_skip_pin() {
        let src = include_str!("broadcast_queue.rs");

        // --- broadcast_to_single_peer wiring ---
        let fn_start = src
            .find("pub(super) async fn broadcast_to_single_peer(")
            .expect("broadcast_to_single_peer not found");
        let after = &src[fn_start..];
        let fn_end = after
            .find("\nmod tests {")
            .or_else(|| after.find("\n#[cfg(test)]"))
            .expect("end of broadcast_to_single_peer (start of tests module) not found");
        let body = &after[..fn_end];

        assert!(
            body.contains("fanout_send_needed("),
            "broadcast_to_single_peer must route the per-peer skip decision \
             through fanout_send_needed — a bare summary byte comparison \
             re-opens the nondeterministic-summary heal storm"
        );

        // The Ok(None) arm (contract returned empty delta = converged) must
        // SKIP (return), never construct a FullState payload.
        let ok_none_off = body
            .find("Ok(None) =>")
            .expect("compute_delta Ok(None) arm not found in broadcast_to_single_peer");
        let err_off = body[ok_none_off..]
            .find("Err(err) =>")
            .expect("compute_delta Err arm not found after Ok(None) arm");
        let ok_none_arm = &body[ok_none_off..ok_none_off + err_off];
        assert!(
            !ok_none_arm.contains("FullState"),
            "the Ok(None) (empty delta = converged) arm must NOT fall back to \
             sending full state — that re-flood on every fan-out IS the heal \
             storm. Arm body:\n{ok_none_arm}"
        );
        assert!(
            ok_none_arm.contains("return;"),
            "the Ok(None) (empty delta = converged) arm must skip the send \
             entirely (return). Arm body:\n{ok_none_arm}"
        );

        // --- helper internals: the semantic machinery is actually consulted ---
        let helpers_start = src
            .find("pub(super) fn plan_fanout_send")
            .expect("plan_fanout_send not found");
        let helpers_end = src
            .find("// The `BroadcastQueue` struct (constants, types, impl)")
            .expect("queue module comment anchor not found");
        assert!(
            helpers_start < helpers_end,
            "plan_fanout_send / fanout_send_needed must be defined before the \
             queue module"
        );
        let helpers = &src[helpers_start..helpers_end];
        assert!(
            helpers.contains("plan_staleness_probe"),
            "plan_fanout_send must ration WASM probes through \
             plan_staleness_probe (the MAX_STALENESS_PROBES_PER_SUMMARIES cap)"
        );
        assert!(
            helpers.contains("cached_staleness_verdict"),
            "plan_fanout_send must consult the shared delta cache \
             (cached_staleness_verdict) before trusting summary bytes"
        );
        assert!(
            helpers.contains("peer_summary_has_pending_state"),
            "fanout_send_needed must resolve cache misses via the bounded \
             contract delta probe (peer_summary_has_pending_state)"
        );
        assert!(
            helpers.contains("summary_indicates_stale_peer"),
            "fanout_send_needed must decide from the probe verdict via \
             summary_indicates_stale_peer (semantic policy), not inline byte \
             inequality"
        );
    }

    /// Source-scrape pin (cost-aware eviction, #4861): the per-target
    /// summarize + delta computation in `broadcast_to_single_peer` is
    /// send-time WASM burned for EVERY peer send — a dominant, otherwise-
    /// unmetered CPU cost for a high-frequency tiny-payload storm. It must be
    /// attributed on the `ExecCpuMicros` axis at BOTH exits (the summaries-
    /// equal skip and the payload completion). Dropping a report silently
    /// blinds the cost-pressure eviction trigger to send-time CPU while every
    /// behavioral test stays green.
    #[test]
    fn broadcast_to_single_peer_reports_send_wasm_cost_pin() {
        let src = include_str!("broadcast_queue.rs");
        let fn_start = src
            .find("pub(super) async fn broadcast_to_single_peer(")
            .expect("broadcast_to_single_peer not found");
        let after = &src[fn_start..];
        let fn_end = after
            .find("\nmod tests {")
            .or_else(|| after.find("\n#[cfg(test)]"))
            .expect("end of broadcast_to_single_peer (start of tests module) not found");
        let body = &after[..fn_end];

        // The reporting closure must exist and be invoked at both exits.
        let report_invocations = body.matches("report_send_wasm_cost(op_manager)").count();
        assert_eq!(
            report_invocations, 2,
            "broadcast_to_single_peer must invoke report_send_wasm_cost at both \
             exits (summaries-equal skip + payload completion) — got \
             {report_invocations}. A dropped report blinds cost-pressure \
             eviction (#4861) to per-send WASM cost."
        );
        // And the closure must report on the ExecCpuMicros axis (split needle
        // so this test cannot self-count).
        let axis_needle = concat!("ResourceType::", "Exec", "CpuMicros");
        assert!(
            body.contains(axis_needle),
            "report_send_wasm_cost must attribute on the ExecCpuMicros axis"
        );
    }
}

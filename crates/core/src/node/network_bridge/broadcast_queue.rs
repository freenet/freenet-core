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
//!
//! The limit is two pools — 12 concurrent small payloads, 2 concurrent large
//! ones — and each pool has its OWN FIFO and its OWN drain worker, so a queued
//! send waiting for large-payload capacity cannot delay small sends behind it.
//! The pool is chosen from the PREDICTED wire payload (a large-state contract
//! sends a small delta to any peer whose summary we hold), and corrected
//! against the real payload before any bytes go out. See #4961 for the
//! measurements that drove all three of those; before it, one worker drained
//! one FIFO and picked the pool from the full contract STATE size.
//!
//! The correction is the one remaining cross-lane coupling, and it is BOUNDED
//! rather than absent: a send that was queued small and turns out to be full
//! state keeps its small-lane slot for at most
//! [`UPGRADE_SLOT_HOLD_WINDOW`] while it waits for large-lane capacity, then
//! gives the slot back and keeps waiting without one. So mispredictions cost
//! the small lane a bounded stall instead of an unbounded one, and never put
//! large payloads on the wire outside the large-payload limit.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use freenet_stdlib::prelude::{ContractKey, WrappedState};

use crate::node::OpManager;
use crate::ring::PeerKeyLocation;
use crate::transport::BroadcastDeliveryOutcome;

use super::broadcast_payload_mix::PayloadArm;
use super::p2p_protoc::P2pBridge;

/// Timeout for awaiting stream completion signal before releasing the permit
/// anyway. Prevents permanent permit leak if a stream task panics or hangs.
/// Used by `broadcast_to_single_peer` under both `simulation_tests` and
/// production, hence kept at module scope rather than inside the cfg-gated
/// `queue` submodule.
const STREAM_COMPLETION_TIMEOUT: Duration = Duration::from_secs(120);

/// Payload size, in bytes, at or above which a send is rate-limited by the
/// narrow large-payload pool instead of the wide small-payload pool.
///
/// Applied to the PREDICTED wire payload at enqueue time (see
/// `queue::classify_payload_lane`) and again to the selected payload — the
/// delta or the full state — once it is known (see
/// [`SendLanePermit::ensure_capacity_for`]). It used to be applied to the full
/// contract STATE size, which is not what goes on the wire whenever a delta is
/// sent — the classification half of #4961.
///
/// "Selected payload" is not quite the byte count on the wire: the sender's own
/// summary and the message framing ride along with it, so a small payload with
/// a large summary is under-counted here. That blind spot is pre-existing (the
/// old state-size classification had it too) and is not what #4961 was about.
const BROADCAST_QUEUE_PAYLOAD_SIZE_THRESHOLD: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
// The simulation feature replaces the production queue, but keeps the shared
// payload-selection path and its public signature compiled.
#[cfg_attr(feature = "simulation_tests", allow(dead_code))]
pub(super) enum QueuedPayloadClass {
    Small,
    Large,
}

/// The concurrency permit a dispatched broadcast holds while its payload is on
/// the wire, together with the lane that permit came from.
///
/// The queue picks a lane from a PREDICTION of the wire payload (a large-state
/// contract whose target has a cached summary sends a small delta, not the
/// state). A prediction can be wrong in the expensive direction — the contract
/// may refuse a delta (`DeltaUnavailable::NotEfficient`) and send full state
/// after all — and 12 concurrent full-state streams saturate a residential
/// uplink, which is the exact failure this queue exists to prevent. So the
/// permit is upgraded to the large lane at the point the real payload size
/// becomes known; see [`Self::ensure_capacity_for`].
// Constructed only by the production queue, which is cfg'd out under
// `simulation_tests`; the type stays compiled because `broadcast_to_single_peer`
// (shared with the sim fan-out) takes it.
#[cfg_attr(feature = "simulation_tests", allow(dead_code))]
pub(super) struct SendLanePermit {
    /// The permit currently held. Dropping `self` releases it.
    ///
    /// `None` only while parked waiting out a congested large lane — see
    /// [`Self::ensure_capacity_for`]. Nothing is on the wire during that
    /// window, so an absent permit never means unmetered bytes. (The one
    /// exception is a large pool that CLOSES during that wait, which no code
    /// path does today; the send then proceeds unmetered rather than hanging
    /// forever. See [`Self::large_pool_closed`].)
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
    /// Which pool `permit` came from.
    lane: QueuedPayloadClass,
    /// The large-payload pool, for the upgrade path.
    large_pool: Arc<tokio::sync::Semaphore>,
    /// Parking permits for sends waiting out a congested large lane. Bounds
    /// how many can be in flight holding no pool capacity — see
    /// [`Self::ensure_capacity_for`] phase 2.
    upgrade_slots: Arc<tokio::sync::Semaphore>,
}

/// How long a mispredicted send keeps its small-lane slot while waiting for
/// large-lane capacity, before parking without one.
///
/// The two failure modes this sits between: hold the slot for the whole wait and
/// enough simultaneous mispredictions tie up all 12 small permits, stalling
/// unrelated small broadcasts — the very head-of-line failure this module was
/// changed to remove. Give it up immediately and the drain dispatches a
/// replacement at once, so sends pile up holding serialized payloads.
///
/// This is NOT sized to cover a typical large-lane occupancy: a full-state
/// stream is >= 64 KiB at the ~1.25 MB/s a rate-limited stream gets, so a
/// large-lane slot frequently takes longer than this to turn over and phase 2 is
/// a normal outcome, not an exceptional one. Its job is narrower — absorb
/// momentary contention without ever handing an unrelated small broadcast a
/// stall longer than this, however many mispredictions arrive at once.
const UPGRADE_SLOT_HOLD_WINDOW: Duration = Duration::from_millis(250);

/// How many mispredicted sends may be parked at once waiting for large-lane
/// capacity while holding no pool permit (phase 2 of
/// [`SendLanePermit::ensure_capacity_for`]).
///
/// This is the bound on in-flight sends that the pools otherwise provide.
/// Parked sends hold no permit, so without a cap the small drain keeps
/// dispatching replacements and the parked set grows at the enqueue rate while
/// draining at the large lane's — one serialized payload (>= 64 KiB, and ~1 MB
/// for the large-state contracts this path exists for) retained per parked
/// send, unbounded. That is reachable in steady state, not just in a burst: a
/// contract whose deltas are never smaller than its state mispredicts on every
/// send to every peer, forever.
///
/// Sized at one full turnover of the small pool
/// (`queue::DEFAULT_SMALL_PAYLOAD_CONCURRENCY`, kept in step by
/// `parking_matches_one_small_pool_turnover`). Past that, upgraders keep their
/// small-lane slots and the small lane applies backpressure — which is the
/// correct response to an uplink genuinely saturated with full-state sends, and
/// is bounded work rather than unbounded memory.
///
/// Not derived from that constant directly because the pool sizes live in the
/// `queue` submodule, which is cfg'd out under `simulation_tests` while this
/// type stays compiled.
const MAX_PARKED_LANE_UPGRADES: usize = 12;

#[cfg_attr(feature = "simulation_tests", allow(dead_code))]
impl SendLanePermit {
    fn new(
        lane: QueuedPayloadClass,
        permit: tokio::sync::OwnedSemaphorePermit,
        large_pool: Arc<tokio::sync::Semaphore>,
        upgrade_slots: Arc<tokio::sync::Semaphore>,
    ) -> Self {
        Self {
            permit: Some(permit),
            lane,
            large_pool,
            upgrade_slots,
        }
    }

    /// Make sure this send holds a LARGE-lane permit before putting
    /// `payload_size` bytes on the wire, if the payload turned out to be large
    /// while the queue had predicted small.
    ///
    /// This is what keeps the uplink guarantee independent of the prediction's
    /// accuracy: the small pool's 12 permits may only ever admit small
    /// payloads, so a mispredicted send waits here until large-lane capacity
    /// exists, exactly as if it had been queued as large. It is a no-op for a
    /// correctly-classified send and for anything already in the large lane —
    /// without that second guard a large-lane send would wait for a SECOND
    /// large permit while holding one, and two of them would wedge the pool
    /// permanently.
    ///
    /// The wait is in two phases (see [`UPGRADE_SLOT_HOLD_WINDOW`]): first
    /// briefly holding the small-lane slot, then — if the large lane is
    /// genuinely congested — after handing that slot back, so a burst of
    /// mispredictions cannot occupy the wide pool and stall small sends behind
    /// it. Mispredictions ARE bursty: they come from conditions that hit every
    /// peer of a contract at once (a `get_contract_summary` timeout, an
    /// `Interests` full-replace wiping cached summaries, the delta-incompat
    /// memo arming, or a contract whose deltas are never smaller than its
    /// state), so treating them as independent events would be wrong.
    ///
    /// A parked send holds no pool capacity and is putting nothing on the wire,
    /// so the pools no longer bound how many sends are in flight — that is what
    /// [`MAX_PARKED_LANE_UPGRADES`] is for. Parking slots are taken BEFORE the
    /// small-lane slot is given up, so when they run out the send simply keeps
    /// its small-lane slot and waits, and the small lane applies backpressure
    /// instead of the node accumulating payloads. (The `active` map does NOT
    /// bound this: `track_active` is observation-only, and `drain_lane`
    /// dispatches regardless of what it returns.)
    ///
    /// Cancellation: this is awaited inline by the send that OWNS the
    /// `SendLanePermit`, so dropping that send drops the permit too — mid-wait
    /// or mid-send, whichever permit is currently held is released exactly
    /// once, and a dropped `acquire_owned` future consumes nothing. Neither
    /// pool can leak a slot or be double-charged.
    async fn ensure_capacity_for(&mut self, payload_size: usize) {
        if payload_size < BROADCAST_QUEUE_PAYLOAD_SIZE_THRESHOLD
            || matches!(self.lane, QueuedPayloadClass::Large)
        {
            return;
        }
        // Phase 1: hold the small-lane slot across a brief wait. Giving it up
        // for a large lane that is about to free a permit anyway would let the
        // drain dispatch more concurrent sends than the pools are sized for.
        let acquire = self.large_pool.clone().acquire_owned();
        match tokio::time::timeout(UPGRADE_SLOT_HOLD_WINDOW, acquire).await {
            Ok(Ok(large)) => return self.take_large(large),
            Ok(Err(_)) => return self.large_pool_closed(payload_size),
            Err(_elapsed) => {}
        }
        // Phase 2: the large lane is genuinely congested. Take a parking slot
        // FIRST — only then is it safe to give up the small-lane slot, because
        // the drain will immediately dispatch a replacement send and nothing
        // else bounds how many of those can accumulate. With no parking slot
        // free we keep the small-lane slot and wait under it: the small lane
        // applies backpressure, which is bounded work, rather than the node
        // accumulating unbounded parked payloads.
        let _parked = self.upgrade_slots.clone().try_acquire_owned().ok();
        if _parked.is_some() {
            // Assigning `None` drops the permit; the send is now holding no
            // pool capacity and sending nothing.
            self.permit = None;
        }
        match self.large_pool.clone().acquire_owned().await {
            Ok(large) => self.take_large(large),
            Err(_) => self.large_pool_closed(payload_size),
        }
        // `_parked` is released here, as the send leaves the parking area.
    }

    /// Adopt `large` as the held permit. Assigning drops whatever was held
    /// before, so the small-lane slot (if still held) is released only AFTER
    /// large-lane capacity is in hand.
    fn take_large(&mut self, large: tokio::sync::OwnedSemaphorePermit) {
        self.permit = Some(large);
        self.lane = QueuedPayloadClass::Large;
    }

    /// The large pool is closed. Let the send proceed under whatever it still
    /// holds — possibly nothing, if it had already parked — rather than
    /// stalling forever.
    ///
    /// UNREACHABLE today: nothing closes these semaphores (grep `.close()` in
    /// this file — the only hit is a test). It exists so that adding a real
    /// shutdown path later cannot silently strand in-flight sends, and it is
    /// the one documented way a large payload can reach the wire without
    /// large-lane capacity.
    fn large_pool_closed(&self, payload_size: usize) {
        tracing::debug!(
            payload_size,
            "Large broadcast pool closed; sending mispredicted payload without \
             large-lane capacity"
        );
    }
}

/// Everything the production queue hands to one dispatched send.
///
/// The two fields must co-occur: a queued class without a permit would put
/// bytes on the wire outside the concurrency limit, and a permit without the
/// class would silently stop feeding the classification-accuracy counters
/// (`queued_large_actual_small` / `queued_small_actual_large`). Bundling them
/// bundles two fields that must co-occur, so a caller cannot supply one
/// without the other — the sim fan-out passes `None` for the whole bundle, so
/// it cannot pass one half.
#[cfg_attr(feature = "simulation_tests", allow(dead_code))]
pub(super) struct QueueScheduling {
    /// The lane the entry was CLASSIFIED into at enqueue time. Immutable — the
    /// mismatch counters compare it against the real payload, so it must not
    /// track [`SendLanePermit::lane`] across an upgrade.
    queued_class: QueuedPayloadClass,
    /// The concurrency permit gating this send's bytes onto the wire.
    permit: SendLanePermit,
}

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
/// the pair in a sub-struct makes the two impossible to supply separately: the only
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
/// different peers. The byte compare then never skips, and `compute_delta`
/// either returned an empty delta (which the pre-fix arm "fell back" from by
/// sending FULL STATE) or — before #4923 removed the pre-compute
/// `is_delta_efficient` gate — was refused outright on big-summary contracts,
/// so a fully-converged pair re-flooded full state on every heartbeat-driven
/// sync and every fan-out (the nondeterministic-summary heal storm; e.g. the
/// `Eumk9HNQ` contract that "healed" hard while its state never changed).
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
/// Note the probe deliberately has no notion of delta SIZE (see
/// `peer_summary_has_pending_state`): staleness detection wants only the
/// semantic answer (empty vs non-empty delta), because the alternative it
/// replaces is a spurious FULL-STATE send on every fan-out — strictly more
/// expensive than one delta computation. (`compute_delta` shares the same
/// always-compute behavior since #4923; it additionally refuses to RETURN a
/// delta that is not smaller than full state.)
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
    use std::collections::{BTreeMap, HashMap};
    use std::future::Future;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Instant;

    use freenet_stdlib::prelude::{ContractKey, WrappedState};
    use tokio::sync::{Mutex, Notify, Semaphore};

    use crate::node::OpManager;
    use crate::ring::{PeerKey, PeerKeyLocation};

    use super::super::p2p_protoc::P2pBridge;
    use super::{QueueScheduling, QueuedPayloadClass, SendLanePermit, broadcast_to_single_peer};

    /// Maximum concurrent outbound broadcast streams for small payloads (< 64KB).
    /// Small payloads (deltas, chat messages) can fan out aggressively without
    /// saturating the uplink since they finish quickly.
    const DEFAULT_SMALL_PAYLOAD_CONCURRENCY: usize = 12;

    /// Maximum concurrent outbound broadcast streams for large payloads (>= 64KB).
    /// Large payloads (full state) are rate-limited to avoid uplink saturation.
    const DEFAULT_LARGE_PAYLOAD_CONCURRENCY: usize = 2;

    /// Maximum entries in the queue before oldest are dropped.
    const DEFAULT_MAX_QUEUE_DEPTH: usize = 256;

    /// Key for deduplicating broadcast entries: (contract, peer identity).
    type DedupeKey = (ContractKey, PeerKeyLocation);

    /// Choose the concurrency pool for one queued broadcast.
    ///
    /// `delta_expected` says whether the send is expected to put a DELTA on the
    /// wire rather than the whole state — see [`delta_send_expected`]. That is
    /// the whole point of this function: the lane must be chosen from the
    /// predicted WIRE payload, not from the contract state, because a contract
    /// whose state exceeds the threshold still sends ~1 KB deltas to every peer
    /// whose summary we hold. Classifying those as large squeezed two thirds of
    /// LARGE-LANE traffic through a 2-permit pool (#4961; measured on the
    /// 0.2.118 fleet as 29,661 of 45,982 large-lane items per node-day having an
    /// actual payload below the threshold, averaging ~950 bytes).
    ///
    /// A prediction is not a guarantee — the contract can refuse the delta and
    /// send full state anyway — so it decides SCHEDULING only. The uplink bound
    /// is enforced against the real payload by
    /// [`SendLanePermit::ensure_capacity_for`].
    pub(super) fn classify_payload_lane(
        state_size: usize,
        delta_expected: bool,
    ) -> QueuedPayloadClass {
        if state_size < super::BROADCAST_QUEUE_PAYLOAD_SIZE_THRESHOLD || delta_expected {
            QueuedPayloadClass::Small
        } else {
            QueuedPayloadClass::Large
        }
    }

    /// Whether the send for `(key, target)` is expected to carry a delta.
    ///
    /// Mirrors TWO OF the conditions `broadcast_to_single_peer` applies before
    /// it can compute a delta: we must hold a cached summary for the target,
    /// and deltas must not be suppressed for the contract by the delta-incompat
    /// memo. Both are in-memory reads (a DashMap lookup with no summary clone,
    /// and a NON-counting peek at the memo — using the counting
    /// `suppress_deltas` here would inflate `suppressed_total` with predictions
    /// that never became sends).
    ///
    /// It deliberately does NOT mirror the third condition — that OUR OWN
    /// summary is available. That one comes from `get_contract_summary`, a
    /// contract-handler round trip that can time out under load, so it is
    /// neither knowable at enqueue time nor affordable per target. The
    /// prediction is therefore optimistic by construction, and every gap is
    /// caught downstream by [`SendLanePermit::ensure_capacity_for`].
    ///
    /// Takes the two collections rather than `&OpManager` so the truth table is
    /// unit-testable without standing up a node.
    fn delta_send_expected(
        interest_manager: &crate::ring::interest::InterestManager<
            crate::util::time_source::DynTimeSource,
        >,
        delta_incompat: &crate::ring::delta_incompat::DeltaIncompat,
        key: &ContractKey,
        target: &PeerKeyLocation,
    ) -> bool {
        let peer_key = PeerKey::from(target.pub_key().clone());
        interest_manager.has_peer_summary(key, &peer_key)
            && !delta_incompat.deltas_suppressed_peek(key.id())
    }

    /// The whole enqueue-time lane decision: predict the payload shape, then
    /// classify. Exists as one function so the COMPOSITION is testable —
    /// `classify_payload_lane` and `delta_send_expected` are each pinned
    /// separately, but wiring them together wrongly (a dropped `!`, a hard-coded
    /// term) is what would silently restore #4961 in production, and that lives
    /// in neither of them.
    fn lane_for(
        interest_manager: &crate::ring::interest::InterestManager<
            crate::util::time_source::DynTimeSource,
        >,
        delta_incompat: &crate::ring::delta_incompat::DeltaIncompat,
        key: &ContractKey,
        target: &PeerKeyLocation,
        state_size: usize,
    ) -> QueuedPayloadClass {
        classify_payload_lane(
            state_size,
            delta_send_expected(interest_manager, delta_incompat, key, target),
        )
    }

    /// A pending broadcast entry in the queue.
    struct BroadcastEntry {
        key: ContractKey,
        target: PeerKeyLocation,
        new_state: WrappedState,
        /// Contract STATE size in bytes. Feeds the `scheduled_*_state_bytes`
        /// counters only — it is deliberately NOT what picks the lane (see
        /// [`classify_payload_lane`]).
        state_size: usize,
        /// The pool this entry is queued for. Recomputed on dedup replacement,
        /// so a state that crosses the threshold (or a peer whose summary
        /// appears/disappears) re-lanes the entry instead of going stale.
        lane: QueuedPayloadClass,
        /// Global enqueue order, and this entry's key in its lane's order map.
        seq: u64,
    }

    /// Internal queue state: per-lane FIFO ordering + HashMap for dedup lookup.
    ///
    /// The lanes are separate FIFOs so a large entry waiting for large-lane
    /// capacity cannot stall small entries behind it: each lane is drained by
    /// its own worker, and a blocked worker only ever blocks its own lane. With
    /// one shared FIFO the single worker awaited the large semaphore INLINE, so
    /// it could not `pop` anything at all while waiting — the head-of-line half
    /// of #4961 (1,887 incidents and a 7,260 s small-entry wait integral per
    /// node-day on the 0.2.118 fleet). (A dispatched send correcting a
    /// mispredicted lane can still hold a small-lane permit briefly; that
    /// residual is bounded by [`UPGRADE_SLOT_HOLD_WINDOW`], not by the large
    /// lane's drain rate.)
    ///
    /// Ordering is by a global monotonic sequence number so the two FIFOs stay
    /// comparable: capacity eviction still drops the globally oldest entry, and
    /// re-laning an entry on dedup replacement preserves its original position.
    /// `entries` and the two order maps are always mutated together, so an order
    /// map never names a key `entries` lacks.
    struct QueueState {
        /// Small-lane order: enqueue sequence -> dedup key.
        small_order: BTreeMap<u64, DedupeKey>,
        /// Large-lane order: enqueue sequence -> dedup key.
        large_order: BTreeMap<u64, DedupeKey>,
        /// Sequence assigned to the next NEW entry.
        next_seq: u64,
        /// Actual entries, keyed by (contract, peer). Dedup replaces the state in-place.
        entries: HashMap<DedupeKey, BroadcastEntry>,
        /// Pairs already removed from the queue but waiting for a permit or
        /// actively sending. Observation-only: enqueue still behaves exactly
        /// as before, while telemetry can identify duplicates that queued
        /// deduplication cannot see.
        active: HashMap<DedupeKey, u64>,
        /// Number of entries queued in the small lane. Maintained under the
        /// queue lock so head-of-line observation is O(1).
        small_queued: usize,
        /// Time integral for the LARGE-lane worker waiting at a large-payload
        /// permit. Updated on every small-queue population change, so arrivals
        /// during the wait are included rather than sampled away.
        ///
        /// Recorded at exactly the pre-#4961 condition, so the counters it
        /// feeds keep their definitions across the fix. What changes is the
        /// consequence, not the measurement: with a drain per lane those small
        /// entries are being scheduled concurrently, so
        /// `small_entry_millis_blocked` becomes an OVERLAP integral rather than
        /// a blockage, and collapsing toward zero is the fix landing.
        ///
        /// It does NOT observe the small lane's own stalls — a small entry
        /// waiting on a small-lane permit, including one held by a
        /// lane-correction wait, opens no observation here. So a zero reading
        /// is evidence about cross-lane blocking only. See `router.rs`.
        hol_block: Option<HolBlockHandle>,
    }

    /// Lock ordering, now that TWO workers can touch an observation
    /// concurrently (the large lane owns the handle, the small lane mutates the
    /// queue population it integrates over): the tokio queue mutex is always
    /// taken BEFORE this inner `std::sync::Mutex`, and never the other way —
    /// `finish_now` holds only the inner one, and nothing reaches for the queue
    /// lock while holding it. So the two lanes cannot invert.
    #[derive(Clone)]
    struct HolBlockHandle(Arc<HolBlockState>);

    struct HolBlockState {
        finished: AtomicBool,
        observation: std::sync::Mutex<HolBlockObservation>,
    }

    struct HolBlockObservation {
        started: Instant,
        last_updated: Instant,
        small_queued: usize,
        small_entry_millis: u128,
        observed_small: bool,
    }

    impl HolBlockHandle {
        fn is_finished(&self) -> bool {
            self.0.finished.load(Ordering::Acquire)
        }

        fn set_small_queued(&self, next: usize, now: Instant) {
            if self.is_finished() {
                return;
            }
            let mut block = self.0.observation.lock().unwrap();
            // `finish_now` may have won the mutex after the fast-path load.
            if self.is_finished() {
                return;
            }
            block.advance(now);
            block.small_queued = next;
            block.observed_small |= next > 0;
        }

        /// Freeze the integral and its end timestamp as one linearized action.
        /// Capturing `now` only after taking the same mutex used by queue-size
        /// updates prevents a later-timestamp update from being incorporated
        /// before an older semaphore-acquisition cutoff can be applied.
        fn finish_now(&self) -> Option<(u64, u64)> {
            let mut block = self.0.observation.lock().unwrap();
            if self.is_finished() {
                return None;
            }
            let now = Instant::now();
            self.finish_locked(&mut block, now)
        }

        #[cfg(test)]
        fn finish_at(&self, now: Instant) -> Option<(u64, u64)> {
            let mut block = self.0.observation.lock().unwrap();
            if self.is_finished() {
                return None;
            }
            self.finish_locked(&mut block, now)
        }

        fn finish_locked(
            &self,
            block: &mut HolBlockObservation,
            now: Instant,
        ) -> Option<(u64, u64)> {
            block.advance(now);
            let result = block.observed_small.then(|| {
                let blocked_millis = now
                    .saturating_duration_since(block.started)
                    .as_millis()
                    .min(u128::from(u64::MAX)) as u64;
                let small_entry_millis = block.small_entry_millis.min(u128::from(u64::MAX)) as u64;
                (blocked_millis, small_entry_millis)
            });
            self.0.finished.store(true, Ordering::Release);
            result
        }
    }

    impl HolBlockObservation {
        fn advance(&mut self, now: Instant) {
            if now <= self.last_updated {
                return;
            }
            let millis = now.saturating_duration_since(self.last_updated).as_millis();
            self.small_entry_millis = self
                .small_entry_millis
                .saturating_add(millis.saturating_mul(self.small_queued as u128));
            self.last_updated = now;
        }
    }

    impl QueueState {
        fn new() -> Self {
            Self {
                small_order: BTreeMap::new(),
                large_order: BTreeMap::new(),
                next_seq: 0,
                entries: HashMap::new(),
                active: HashMap::new(),
                small_queued: 0,
                hol_block: None,
            }
        }

        fn len(&self) -> usize {
            self.entries.len()
        }

        fn order_mut(&mut self, lane: QueuedPayloadClass) -> &mut BTreeMap<u64, DedupeKey> {
            match lane {
                QueuedPayloadClass::Small => &mut self.small_order,
                QueuedPayloadClass::Large => &mut self.large_order,
            }
        }

        /// Pop the oldest entry queued for `lane`, or `None` if that lane is
        /// empty. A busy OTHER lane is invisible here — that is the point.
        fn pop_lane(&mut self, lane: QueuedPayloadClass, now: Instant) -> Option<BroadcastEntry> {
            let (_, key) = self.order_mut(lane).pop_first()?;
            let entry = self
                .entries
                .remove(&key)
                .expect("order maps and entries are mutated together");
            debug_assert_eq!(entry.lane, lane, "entry popped from the wrong lane");
            if matches!(lane, QueuedPayloadClass::Small) {
                self.set_small_queued(self.small_queued.saturating_sub(1), now);
            }
            Some(entry)
        }

        /// Drop the globally oldest entry across both lanes (capacity eviction).
        fn evict_oldest(&mut self, now: Instant) -> Option<BroadcastEntry> {
            let oldest_small = self.small_order.keys().next().copied();
            let oldest_large = self.large_order.keys().next().copied();
            let lane = match (oldest_small, oldest_large) {
                (Some(small), Some(large)) if large < small => QueuedPayloadClass::Large,
                (Some(_), _) => QueuedPayloadClass::Small,
                (None, Some(_)) => QueuedPayloadClass::Large,
                (None, None) => return None,
            };
            self.pop_lane(lane, now)
        }

        /// Insert a brand-new entry at the tail of its lane.
        fn push_new(&mut self, key: DedupeKey, entry: BroadcastEntry) {
            self.order_mut(entry.lane).insert(entry.seq, key.clone());
            self.entries.insert(key, entry);
        }

        /// Move an already-queued entry to a different lane, keeping its
        /// original sequence (and therefore its FIFO position).
        fn relane(&mut self, key: &DedupeKey, from: QueuedPayloadClass, to: QueuedPayloadClass) {
            let Some(seq) = self.entries.get(key).map(|entry| entry.seq) else {
                return;
            };
            self.order_mut(from).remove(&seq);
            self.order_mut(to).insert(seq, key.clone());
        }

        fn set_small_queued(&mut self, next: usize, now: Instant) {
            self.small_queued = next;
            if self
                .hol_block
                .as_ref()
                .is_some_and(HolBlockHandle::is_finished)
            {
                self.hol_block = None;
            }
            if let Some(block) = &self.hol_block {
                block.set_small_queued(next, now);
            }
        }

        fn start_hol(&mut self, now: Instant) -> HolBlockHandle {
            let block = HolBlockHandle(Arc::new(HolBlockState {
                finished: AtomicBool::new(false),
                observation: std::sync::Mutex::new(HolBlockObservation {
                    started: now,
                    last_updated: now,
                    small_queued: self.small_queued,
                    small_entry_millis: 0,
                    observed_small: self.small_queued > 0,
                }),
            }));
            self.hol_block = Some(block.clone());
            block
        }

        fn track_active(&mut self, key: DedupeKey) -> bool {
            if let Some(count) = self.active.get_mut(&key) {
                *count = count.saturating_add(1);
                return true;
            }
            if self.active.len() >= DEFAULT_MAX_QUEUE_DEPTH {
                return false;
            }
            self.active.insert(key, 1);
            true
        }

        fn untrack_active(&mut self, key: &DedupeKey) {
            if let Some(count) = self.active.get_mut(key) {
                if *count > 1 {
                    *count -= 1;
                } else {
                    self.active.remove(key);
                }
            }
        }
    }

    struct ActiveSendGuard {
        queue: Arc<Mutex<QueueState>>,
        key: DedupeKey,
        tracked: bool,
    }

    impl ActiveSendGuard {
        /// Normal completion path: update the refcount directly and disarm the
        /// cancellation fallback. The caller releases its semaphore permit
        /// first, so diagnostic cleanup never holds scheduler capacity idle.
        async fn finish(mut self) {
            if self.tracked {
                self.queue.lock().await.untrack_active(&self.key);
                self.tracked = false;
            }
        }
    }

    impl Drop for ActiveSendGuard {
        fn drop(&mut self) {
            if !self.tracked {
                return;
            }
            if let Ok(mut queue) = self.queue.try_lock() {
                queue.untrack_active(&self.key);
                return;
            }
            let queue = self.queue.clone();
            let key = self.key.clone();
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn(async move {
                    queue.lock().await.untrack_active(&key);
                });
            }
        }
    }

    /// Global broadcast queue that serializes outbound broadcast streams
    /// with bounded concurrency and deduplication.
    ///
    /// Uses dual concurrency pools: sends whose wire payload is predicted small
    /// (< 64KB) get high concurrency (12 slots) for fast fan-out of
    /// deltas/chat messages, while predicted-large sends (>= 64KB, i.e. full
    /// state) get low concurrency (2 slots) to avoid saturating the uplink.
    /// Each pool has its OWN FIFO and its OWN drain worker, so a QUEUED send
    /// waiting for large-lane capacity never delays a small one. (A dispatched
    /// send correcting a mispredicted lane holds its small-lane permit for at
    /// most [`UPGRADE_SLOT_HOLD_WINDOW`] — see `SendLanePermit`.)
    #[derive(Clone)]
    pub(crate) struct BroadcastQueue {
        queue: Arc<Mutex<QueueState>>,
        /// One wakeup per lane: with a single `Notify`, `notify_one` would wake
        /// an arbitrary lane worker, so an enqueue could wake the idle lane and
        /// leave its own entry unscheduled until the next enqueue.
        small_notify: Arc<Notify>,
        large_notify: Arc<Notify>,
        small_payload_concurrency: usize,
        large_payload_concurrency: usize,
        max_queue_depth: usize,
    }

    impl BroadcastQueue {
        pub(crate) fn new() -> Self {
            Self {
                queue: Arc::new(Mutex::new(QueueState::new())),
                small_notify: Arc::new(Notify::new()),
                large_notify: Arc::new(Notify::new()),
                small_payload_concurrency: DEFAULT_SMALL_PAYLOAD_CONCURRENCY,
                large_payload_concurrency: DEFAULT_LARGE_PAYLOAD_CONCURRENCY,
                max_queue_depth: DEFAULT_MAX_QUEUE_DEPTH,
            }
        }

        fn notify_for(&self, lane: QueuedPayloadClass) -> &Arc<Notify> {
            match lane {
                QueuedPayloadClass::Small => &self.small_notify,
                QueuedPayloadClass::Large => &self.large_notify,
            }
        }

        /// Enqueue a broadcast for a single (contract, peer) pair.
        ///
        /// If an entry for the same contract+peer already exists, it is replaced
        /// with the newer state (the older state is stale and would be superseded
        /// anyway). If the queue is at capacity, the oldest entry is evicted.
        ///
        /// `op_manager` is read (in-memory only) to predict whether this send
        /// will carry a delta, which is what picks the lane — see
        /// [`classify_payload_lane`].
        pub(crate) async fn enqueue(
            &self,
            op_manager: &Arc<OpManager>,
            key: ContractKey,
            target: PeerKeyLocation,
            new_state: WrappedState,
        ) {
            let lane = lane_for(
                &op_manager.interest_manager,
                &op_manager.ring.delta_incompat,
                &key,
                &target,
                new_state.size(),
            );
            self.enqueue_in_lane(lane, key, target, new_state).await;
        }

        /// The lane-agnostic half of [`Self::enqueue`], split out so tests can
        /// drive the real queue without standing up an `OpManager`.
        async fn enqueue_in_lane(
            &self,
            lane: QueuedPayloadClass,
            key: ContractKey,
            target: PeerKeyLocation,
            new_state: WrappedState,
        ) {
            let dedup_key = (key, target.clone());
            let state_size = new_state.size();
            let mut queue = self.queue.lock().await;

            if queue.active.contains_key(&dedup_key) {
                crate::node::BROADCAST_QUEUE_EFFICIENCY_METRICS.record_enqueue_while_active();
            }

            // Replace-on-dedup: if same contract+peer exists, update state in-place
            let previous_lane = queue.entries.get(&dedup_key).map(|existing| existing.lane);
            if let Some(previous_lane) = previous_lane {
                if let Some(existing) = queue.entries.get_mut(&dedup_key) {
                    existing.new_state = new_state;
                    // Re-derive BOTH size-dependent fields from the replacement
                    // state. Leaving them at the superseded state's values made
                    // the lane (and the `scheduled_*_state_bytes` attribution)
                    // go stale the moment a state crossed the threshold.
                    existing.state_size = state_size;
                    existing.lane = lane;
                }
                if previous_lane != lane {
                    let small_queued = match lane {
                        QueuedPayloadClass::Small => queue.small_queued.saturating_add(1),
                        QueuedPayloadClass::Large => queue.small_queued.saturating_sub(1),
                    };
                    queue.set_small_queued(small_queued, Instant::now());
                    queue.relane(&dedup_key, previous_lane, lane);
                }
                crate::node::BROADCAST_QUEUE_EFFICIENCY_METRICS.record_dedup_replacement();
                tracing::trace!(
                    contract = %dedup_key.0,
                    peer = ?target.socket_addr(),
                    "Broadcast queue: replaced stale entry with newer state"
                );
            } else {
                // Evict oldest if at capacity
                while queue.len() >= self.max_queue_depth {
                    if let Some(entry) = queue.evict_oldest(Instant::now()) {
                        crate::node::BROADCAST_QUEUE_EFFICIENCY_METRICS.record_capacity_eviction();
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
                if matches!(lane, QueuedPayloadClass::Small) {
                    let next = queue.small_queued.saturating_add(1);
                    queue.set_small_queued(next, Instant::now());
                }
                let seq = queue.next_seq;
                queue.next_seq += 1;
                queue.push_new(
                    dedup_key.clone(),
                    BroadcastEntry {
                        key,
                        target,
                        new_state,
                        state_size,
                        lane,
                        seq,
                    },
                );
            }

            // Phase 1.6 shadow telemetry (#4074): publish the post-mutation
            // depth while still under the lock so the depth gauge is exact;
            // the shadow demand aggregator reads it lock-free. Observation
            // only — see transport/shadow_demand.rs.
            crate::transport::shadow_demand::record_broadcast_queue_depth(queue.len());

            drop(queue);
            self.notify_for(lane).notify_one();
        }

        /// Start the background workers that drain the queue with bounded
        /// concurrency — ONE PER LANE.
        ///
        /// Two workers is the structural half of the #4961 fix. A single worker
        /// awaited its entry's semaphore inline, before `tokio::spawn`, so
        /// while it waited for one of the 2 large-lane permits it could not
        /// `pop` anything at all — including small entries whose 12 permits
        /// were entirely free. Splitting the drain per lane makes QUEUED-entry
        /// head-of-line blocking impossible by construction rather than by
        /// scheduling luck; moving the acquire INSIDE the spawn would have
        /// fixed the stall too, but at the cost of unbounded spawned tasks and
        /// of the replace-on-dedup that keeps a hot contract from queueing a
        /// send per update.
        ///
        /// The workers run forever; they are spawned as background tasks. The
        /// handles are not registered with a monitor (pre-existing), and there
        /// are two of them now, so if one ever stopped the other would keep
        /// draining and only that lane would silently go quiet. Two ways it
        /// could: a closed semaphore (nothing closes them today), or a panic on
        /// `pop_lane`'s `.expect` if the order maps ever drifted from
        /// `entries`. Worth revisiting if the pools gain a real close path.
        pub(crate) fn start_worker(
            &self,
            bridge: P2pBridge,
            op_manager: Arc<OpManager>,
        ) -> [tokio::task::JoinHandle<()>; 2] {
            let small_semaphore = Arc::new(Semaphore::new(self.small_payload_concurrency));
            let large_semaphore = Arc::new(Semaphore::new(self.large_payload_concurrency));
            let upgrade_slots = Arc::new(Semaphore::new(super::MAX_PARKED_LANE_UPGRADES));

            let spawn_lane = |lane, semaphore: Arc<Semaphore>| {
                let queue = self.queue.clone();
                let notify = self.notify_for(lane).clone();
                let large_pool = large_semaphore.clone();
                let upgrade_slots = upgrade_slots.clone();
                let bridge = bridge.clone();
                let op_manager = op_manager.clone();
                tokio::spawn(drain_lane(
                    lane,
                    queue,
                    notify,
                    semaphore,
                    large_pool,
                    upgrade_slots,
                    move |entry: BroadcastEntry, scheduling: QueueScheduling| {
                        let bridge = bridge.clone();
                        let op_manager = op_manager.clone();
                        async move {
                            broadcast_to_single_peer(
                                &bridge,
                                &op_manager,
                                entry.key,
                                entry.new_state,
                                entry.target,
                                Some(scheduling),
                            )
                            .await;
                        }
                    },
                ))
            };

            [
                spawn_lane(QueuedPayloadClass::Small, small_semaphore),
                spawn_lane(QueuedPayloadClass::Large, large_semaphore.clone()),
            ]
        }
    }

    /// Drain loop for ONE lane: pop that lane's FIFO, take one of that lane's
    /// permits, and hand the entry to `dispatch` on a spawned task.
    ///
    /// `dispatch` is a parameter (rather than a direct
    /// `broadcast_to_single_peer` call) so the scheduling regression tests can
    /// drive this exact loop — the production ordering of pop / permit / spawn
    /// is what they are testing — without an `OpManager` or a live bridge.
    ///
    /// Cancellation safety: the only `.await`s are the queue mutex, the
    /// semaphore acquire, and the idle wait. The permit is moved into the
    /// spawned task before this loop yields again, and `ActiveSendGuard`
    /// releases the dedup refcount on drop, so aborting the worker cannot leak
    /// either.
    async fn drain_lane<D, F>(
        lane: QueuedPayloadClass,
        queue: Arc<Mutex<QueueState>>,
        notify: Arc<Notify>,
        semaphore: Arc<Semaphore>,
        large_pool: Arc<Semaphore>,
        upgrade_slots: Arc<Semaphore>,
        dispatch: D,
    ) where
        D: Fn(BroadcastEntry, QueueScheduling) -> F,
        F: Future<Output = ()> + Send + 'static,
    {
        let lane_is_large = matches!(lane, QueuedPayloadClass::Large);
        loop {
            // Register the notified future BEFORE checking the queue to avoid
            // a race where enqueue() calls notify_one() between our "queue empty"
            // check and the notified().await call.
            let notified = notify.notified();

            // Drain all available entries
            let mut drained_any = false;
            loop {
                let (entry, active_tracked) = {
                    let mut q = queue.lock().await;
                    let entry = q.pop_lane(lane, Instant::now());
                    let active_tracked = entry.as_ref().is_none_or(|entry| {
                        let tracked = q.track_active((entry.key, entry.target.clone()));
                        if !tracked {
                            crate::node::BROADCAST_QUEUE_EFFICIENCY_METRICS
                                .record_active_tracking_overflow();
                        }
                        tracked
                    });
                    // Phase 1.6 (#4074): publish post-drain depth
                    // under the lock for the shadow demand gauge.
                    crate::transport::shadow_demand::record_broadcast_queue_depth(q.len());
                    (entry, active_tracked)
                };

                let Some(entry) = entry else {
                    break; // This lane is empty
                };
                drained_any = true;

                // The lane IS the concurrency pool: this worker only ever
                // drains entries classified for it, so no per-entry pool
                // selection happens here any more.
                crate::node::BROADCAST_QUEUE_EFFICIENCY_METRICS
                    .record_scheduled(lane_is_large, entry.state_size);

                // Acquire semaphore permit to limit concurrent streams.
                // This blocks until a slot is available — but only this lane's
                // entries wait behind it.
                let blocked = lane_is_large && semaphore.available_permits() == 0;
                let hol_block = if blocked {
                    Some(queue.lock().await.start_hol(Instant::now()))
                } else {
                    None
                };
                let active_guard = ActiveSendGuard {
                    queue: queue.clone(),
                    key: (entry.key, entry.target.clone()),
                    tracked: active_tracked,
                };
                let permit = semaphore.clone().acquire_owned().await;
                let hol_metrics = hol_block.as_ref().and_then(HolBlockHandle::finish_now);
                let Ok(permit) = permit else {
                    if let Some((blocked_millis, small_entry_millis)) = hol_metrics {
                        crate::node::BROADCAST_QUEUE_EFFICIENCY_METRICS
                            .record_large_head_block(blocked_millis, small_entry_millis);
                    }
                    active_guard.finish().await;
                    tracing::error!(?lane, "Broadcast queue semaphore closed unexpectedly");
                    return;
                };

                let scheduling = QueueScheduling {
                    queued_class: lane,
                    permit: SendLanePermit::new(
                        lane,
                        permit,
                        large_pool.clone(),
                        upgrade_slots.clone(),
                    ),
                };
                let send = dispatch(entry, scheduling);

                tokio::spawn(async move {
                    // The permit is owned by the `QueueScheduling` moved into
                    // the send, so scheduler capacity is released when the send
                    // returns — before diagnostic cleanup can wait on the queue
                    // lock, and on every early-return path inside the send too.
                    send.await;
                    active_guard.finish().await;
                });

                // The transfer is runnable before diagnostic counter
                // publication. `finish_now` already froze the exact
                // semaphore/HOL interval at permit acquisition.
                if let Some((blocked_millis, small_entry_millis)) = hol_metrics {
                    crate::node::BROADCAST_QUEUE_EFFICIENCY_METRICS
                        .record_large_head_block(blocked_millis, small_entry_millis);
                }
            }

            if !drained_any {
                // This lane was empty, wait for new entries
                notified.await;
            }
            // If we drained entries, loop immediately to check for more
            // (the pre-registered notified future is dropped, which is fine)
        }
    }

    #[cfg(test)]
    mod observation_tests {
        use super::*;
        use std::time::Duration;

        #[test]
        fn hol_integral_includes_small_entries_arriving_during_wait() {
            let start = Instant::now();
            let mut queue = QueueState::new();
            let hol = queue.start_hol(start);
            queue.set_small_queued(1, start + Duration::from_millis(10));
            queue.set_small_queued(2, start + Duration::from_millis(20));

            assert_eq!(
                hol.finish_at(start + Duration::from_millis(30)),
                Some((30, 30)),
                "integral is 0×10ms + 1×10ms + 2×10ms"
            );
            queue.set_small_queued(9, start + Duration::from_millis(40));
            assert!(hol.is_finished());
            assert!(
                queue.hol_block.is_none(),
                "first later mutation clears the handle"
            );
            assert_eq!(hol.finish_at(start + Duration::from_millis(40)), None);
        }

        #[test]
        fn active_refcount_survives_one_of_two_overlapping_completions() {
            let mut queue = QueueState::new();
            let code = freenet_stdlib::prelude::ContractCode::from(vec![7]);
            let params = freenet_stdlib::prelude::Parameters::from(vec![9]);
            let key = (
                ContractKey::from_params_and_code(&params, &code),
                PeerKeyLocation::random(),
            );
            assert!(queue.track_active(key.clone()));
            assert!(queue.track_active(key.clone()));
            queue.untrack_active(&key);
            assert_eq!(queue.active.get(&key), Some(&1));
            queue.untrack_active(&key);
            assert!(!queue.active.contains_key(&key));
        }

        #[test]
        fn worker_wires_hol_boundaries_and_direct_normal_cleanup() {
            let src = include_str!("broadcast_queue.rs");
            let start = src.find("    async fn drain_lane<D, F>(").unwrap();
            // Bound at the test module so the harness below (which also
            // acquires permits) cannot satisfy these assertions for it.
            let end = src[start..].find("mod observation_tests").unwrap() + start;
            let worker = &src[start..end];
            let hol_start = worker.find("start_hol(Instant::now())").unwrap();
            const ACQUIRE: &str = "semaphore.clone().acquire_owned().await";
            let acquire = worker.find(ACQUIRE).unwrap();
            assert!(
                hol_start < acquire,
                "HOL observation must start before waiting"
            );

            let freeze = worker.find("and_then(HolBlockHandle::finish_now)").unwrap();
            let permit_match = worker.find("let Ok(permit) = permit").unwrap();
            assert!(
                acquire < freeze && freeze < permit_match,
                "linearize the integral immediately after permit acquisition"
            );
            assert!(
                !worker[acquire + ACQUIRE.len()..freeze].contains(".await"),
                "no further await may separate permit acquisition from HOL linearization"
            );
            let success = &worker[permit_match..];
            let spawn = success.find("tokio::spawn(async move").unwrap();
            let task = &success[spawn..];
            // The permit now travels inside the `QueueScheduling` moved into
            // the send, so it is released when the send returns — which must
            // still be before the diagnostic cleanup takes the queue lock.
            assert!(
                task.find("send.await;").unwrap()
                    < task.find("active_guard.finish().await").unwrap(),
                "normal tracking cleanup must run directly after releasing capacity"
            );
        }

        /// The structural half of #4961: one drain per lane. A single worker
        /// draining both lanes is what let a large entry's inline
        /// `acquire_owned().await` stall small entries it never even popped.
        #[test]
        fn start_worker_spawns_one_drain_per_lane() {
            let src = include_str!("broadcast_queue.rs");
            let start = src.find("pub(crate) fn start_worker(").unwrap();
            let end = src[start..]
                .find("    /// Drain loop for ONE lane")
                .unwrap()
                + start;
            let body = &src[start..end];
            assert_eq!(
                body.matches("spawn_lane(QueuedPayloadClass::").count(),
                2,
                "start_worker must spawn exactly one drain per lane"
            );
            // Pin the lane→pool WIRING, not just the lane count. Handing the
            // small lane the large semaphore is a one-token slip that caps
            // small broadcasts at 2 concurrent and puts them back in contention
            // with full-state sends — i.e. reinstates #4961 at full strength —
            // while every behavioural test still passes, because they build
            // their own semaphores instead of going through `start_worker`.
            assert!(
                body.contains("spawn_lane(QueuedPayloadClass::Small, small_semaphore)"),
                "the small lane must be given the SMALL pool"
            );
            assert!(
                body.contains("spawn_lane(QueuedPayloadClass::Large, large_semaphore"),
                "the large lane must be given the LARGE pool"
            );
        }

        // ---- scheduling regressions (#4961) ----

        /// A payload at or above the threshold; the state is what a large-state
        /// contract like a busy River room holds.
        const BIG: usize = super::super::BROADCAST_QUEUE_PAYLOAD_SIZE_THRESHOLD + 1;

        fn contract(seed: u8) -> ContractKey {
            let code = freenet_stdlib::prelude::ContractCode::from(vec![seed]);
            let params = freenet_stdlib::prelude::Parameters::from(vec![seed]);
            ContractKey::from_params_and_code(&params, &code)
        }

        fn state(size: usize) -> WrappedState {
            WrappedState::new(vec![0u8; size])
        }

        /// Both production lane drains, wired to a dispatch that reports which
        /// lane ran each entry and then parks — holding its lane permit exactly
        /// like a real transfer — until the test releases it.
        struct Lanes {
            small_permits: Arc<Semaphore>,
            large_permits: Arc<Semaphore>,
            dispatched: tokio::sync::mpsc::UnboundedReceiver<(QueuedPayloadClass, ContractKey)>,
            workers: Vec<tokio::task::JoinHandle<()>>,
        }

        impl Lanes {
            /// The next dispatch, or `None` if none happens — the "still
            /// blocked" answer. Under `start_paused` the timeout fires as soon
            /// as the runtime is idle, so `None` is fast and deterministic.
            async fn next_dispatch(&mut self) -> Option<(QueuedPayloadClass, ContractKey)> {
                tokio::time::timeout(Duration::from_secs(5), self.dispatched.recv())
                    .await
                    .ok()
                    .flatten()
            }
        }

        impl Drop for Lanes {
            fn drop(&mut self) {
                for worker in &self.workers {
                    worker.abort();
                }
            }
        }

        fn spawn_lanes(queue: &BroadcastQueue, small: usize, large: usize) -> Lanes {
            let small_permits = Arc::new(Semaphore::new(small));
            let large_permits = Arc::new(Semaphore::new(large));
            let upgrade_slots = Arc::new(Semaphore::new(super::super::MAX_PARKED_LANE_UPGRADES));
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            let workers = [
                (QueuedPayloadClass::Small, small_permits.clone()),
                (QueuedPayloadClass::Large, large_permits.clone()),
            ]
            .into_iter()
            .map(|(lane, semaphore)| {
                let tx = tx.clone();
                tokio::spawn(drain_lane(
                    lane,
                    queue.queue.clone(),
                    queue.notify_for(lane).clone(),
                    semaphore,
                    large_permits.clone(),
                    upgrade_slots.clone(),
                    move |entry: BroadcastEntry, scheduling: QueueScheduling| {
                        let tx = tx.clone();
                        async move {
                            // Hold the scheduling bundle (and therefore the
                            // lane permit) for the lifetime of the "send".
                            let _scheduling = scheduling;
                            let _ = tx.send((lane, entry.key));
                            // Never completes: the permit stays held, so the
                            // test can saturate a lane deterministically.
                            std::future::pending::<()>().await;
                        }
                    },
                ))
            })
            .collect();
            Lanes {
                small_permits,
                large_permits,
                dispatched: rx,
                workers,
            }
        }

        /// #4961, head-of-line half. A small entry queued behind a large entry
        /// that is waiting for large-lane capacity must still be scheduled.
        ///
        /// Pre-fix, ONE worker drained both lanes and awaited the large
        /// semaphore INLINE before it could `pop` anything, so the small entry
        /// here was not merely delayed — it could not be dequeued at all until
        /// a large permit freed up (1,887 incidents and a 7,260 s small-entry
        /// wait integral per node-day on the 0.2.118 fleet).
        #[tokio::test(start_paused = true)]
        // Publishes the process-global broadcast-queue depth gauge; serialized
        // against `shadow_demand::tests::broadcast_queue_depth_gauge_round_trips`,
        // which asserts an exact value of it.
        #[serial_test::serial(broadcast_queue_depth_gauge)]
        async fn small_entry_is_scheduled_while_the_large_lane_is_saturated() {
            let queue = BroadcastQueue::new();
            let mut lanes = spawn_lanes(&queue, 12, 2);

            // Occupy both large-lane permits with sends that never finish.
            for seed in 0..2u8 {
                queue
                    .enqueue_in_lane(
                        QueuedPayloadClass::Large,
                        contract(seed),
                        PeerKeyLocation::random(),
                        state(BIG),
                    )
                    .await;
            }
            for _ in 0..2 {
                let (lane, _) = lanes
                    .next_dispatch()
                    .await
                    .expect("both large sends must start");
                assert_eq!(lane, QueuedPayloadClass::Large);
            }
            assert_eq!(
                lanes.large_permits.available_permits(),
                0,
                "precondition: the large lane is saturated"
            );

            // A third large entry: its drain now parks on the semaphore.
            queue
                .enqueue_in_lane(
                    QueuedPayloadClass::Large,
                    contract(2),
                    PeerKeyLocation::random(),
                    state(BIG),
                )
                .await;
            // ...and a small entry arrives behind it. Every one of the small
            // lane's permits is free, so nothing about ITS capacity can be
            // what delays it.
            assert_eq!(lanes.small_permits.available_permits(), 12);
            let small = contract(3);
            queue
                .enqueue_in_lane(
                    QueuedPayloadClass::Small,
                    small,
                    PeerKeyLocation::random(),
                    state(16),
                )
                .await;

            assert_eq!(
                lanes.next_dispatch().await,
                Some((QueuedPayloadClass::Small, small)),
                "a small entry must not wait on large-lane capacity it does not need"
            );
        }

        /// #4961, classification half. A contract whose STATE exceeds the
        /// threshold still puts only a small delta on the wire for every peer
        /// whose summary we hold, so it must not spend one of the 2 large-lane
        /// permits — 64.5% of large-lane traffic on the 0.2.118 fleet was this
        /// case, averaging ~950 actual payload bytes.
        #[tokio::test(start_paused = true)]
        // Publishes the process-global broadcast-queue depth gauge; serialized
        // against `shadow_demand::tests::broadcast_queue_depth_gauge_round_trips`,
        // which asserts an exact value of it.
        #[serial_test::serial(broadcast_queue_depth_gauge)]
        async fn large_state_with_an_expected_delta_takes_the_small_lane() {
            assert_eq!(
                classify_payload_lane(BIG, true),
                QueuedPayloadClass::Small,
                "a delta-sized payload belongs in the small lane however big the state is"
            );
            assert_eq!(
                classify_payload_lane(BIG, false),
                QueuedPayloadClass::Large,
                "without a cached summary the whole state goes on the wire"
            );
            assert_eq!(
                classify_payload_lane(BIG - 1, false),
                QueuedPayloadClass::Large,
                "the threshold itself is large (the comparison is `< threshold`)"
            );
            assert_eq!(
                classify_payload_lane(BIG - 2, false),
                QueuedPayloadClass::Small,
                "a state below the threshold is small whatever the payload shape"
            );

            let queue = BroadcastQueue::new();
            let mut lanes = spawn_lanes(&queue, 12, 2);
            // Saturate the large lane: anything misrouted there cannot run.
            let blocker = lanes
                .large_permits
                .clone()
                .acquire_many_owned(2)
                .await
                .expect("large pool open");

            let key = contract(9);
            queue
                .enqueue_in_lane(
                    classify_payload_lane(BIG, /* delta_expected */ true),
                    key,
                    PeerKeyLocation::random(),
                    state(BIG),
                )
                .await;

            assert_eq!(
                lanes.next_dispatch().await,
                Some((QueuedPayloadClass::Small, key)),
                "a delta send on a large-state contract must not queue behind full-state capacity"
            );
            // Asserting `large_permits == 0` here would prove nothing —
            // `blocker` holds both for the whole test. What discriminates is
            // which pool the dispatched send drew from: the parked dispatch is
            // holding a SMALL permit.
            assert_eq!(
                lanes.small_permits.available_permits(),
                11,
                "and it must have taken a small-lane permit, not a large one"
            );
            drop(blocker);
        }

        /// Dedup replacement must re-derive BOTH size-dependent fields from the
        /// replacement state. Leaving them at the superseded state's values
        /// left the lane (and the scheduled-bytes attribution) stale the moment
        /// a state crossed the threshold — the third defect in #4961.
        #[tokio::test(start_paused = true)]
        // Publishes the process-global broadcast-queue depth gauge; serialized
        // against `shadow_demand::tests::broadcast_queue_depth_gauge_round_trips`,
        // which asserts an exact value of it.
        #[serial_test::serial(broadcast_queue_depth_gauge)]
        async fn dedup_replacement_relanes_and_refreshes_state_size() {
            let queue = BroadcastQueue::new();
            let key = contract(5);
            let target = PeerKeyLocation::random();

            queue
                .enqueue_in_lane(QueuedPayloadClass::Small, key, target.clone(), state(16))
                .await;
            queue
                .enqueue_in_lane(QueuedPayloadClass::Large, key, target.clone(), state(BIG))
                .await;

            {
                let q = queue.queue.lock().await;
                assert_eq!(q.entries.len(), 1, "dedup replaces in place");
                let entry = q.entries.values().next().expect("the replaced entry");
                assert_eq!(
                    entry.lane,
                    QueuedPayloadClass::Large,
                    "the replacement state re-lanes the entry"
                );
                assert_eq!(
                    entry.state_size, BIG,
                    "state_size must follow the replacement, not the superseded state"
                );
                assert!(q.small_order.is_empty(), "the small lane must let it go");
                assert_eq!(q.large_order.len(), 1, "and the large lane must own it");
                assert_eq!(
                    q.small_queued, 0,
                    "small-entry accounting follows the re-lane"
                );
            }

            // ...and symmetrically back again.
            queue
                .enqueue_in_lane(QueuedPayloadClass::Small, key, target, state(16))
                .await;
            let q = queue.queue.lock().await;
            let entry = q.entries.values().next().expect("the replaced entry");
            assert_eq!(entry.lane, QueuedPayloadClass::Small);
            assert_eq!(entry.state_size, 16);
            assert!(q.large_order.is_empty());
            assert_eq!(q.small_order.len(), 1);
            assert_eq!(q.small_queued, 1);
        }

        /// Capacity eviction still drops the globally oldest entry, which with
        /// two FIFOs means comparing their heads rather than popping one.
        #[tokio::test(start_paused = true)]
        // Publishes the process-global broadcast-queue depth gauge; serialized
        // against `shadow_demand::tests::broadcast_queue_depth_gauge_round_trips`,
        // which asserts an exact value of it.
        #[serial_test::serial(broadcast_queue_depth_gauge)]
        async fn capacity_eviction_drops_the_globally_oldest_across_lanes() {
            let queue = BroadcastQueue {
                max_queue_depth: 2,
                ..BroadcastQueue::new()
            };
            let oldest = contract(1);
            queue
                .enqueue_in_lane(
                    QueuedPayloadClass::Large,
                    oldest,
                    PeerKeyLocation::random(),
                    state(BIG),
                )
                .await;
            for seed in 2..4u8 {
                queue
                    .enqueue_in_lane(
                        QueuedPayloadClass::Small,
                        contract(seed),
                        PeerKeyLocation::random(),
                        state(16),
                    )
                    .await;
            }

            let q = queue.queue.lock().await;
            assert_eq!(q.entries.len(), 2, "the depth cap still holds");
            assert!(
                !q.entries.keys().any(|(key, _)| *key == oldest),
                "the evicted entry must be the globally oldest, even though it sat in the other lane"
            );
            assert!(q.large_order.is_empty());
            assert_eq!(q.small_order.len(), 2);
            assert_eq!(q.small_queued, 2);
            drop(q);

            // ...and symmetrically, with the SMALL head the older one. Without
            // this direction, an `evict_oldest` that always picked one lane
            // would still pass the case above.
            let queue = BroadcastQueue {
                max_queue_depth: 2,
                ..BroadcastQueue::new()
            };
            let oldest = contract(5);
            queue
                .enqueue_in_lane(
                    QueuedPayloadClass::Small,
                    oldest,
                    PeerKeyLocation::random(),
                    state(16),
                )
                .await;
            for seed in 6..8u8 {
                queue
                    .enqueue_in_lane(
                        QueuedPayloadClass::Large,
                        contract(seed),
                        PeerKeyLocation::random(),
                        state(BIG),
                    )
                    .await;
            }
            let q = queue.queue.lock().await;
            assert_eq!(q.entries.len(), 2);
            assert!(
                !q.entries.keys().any(|(key, _)| *key == oldest),
                "the small entry was the globally oldest, so it is the one evicted"
            );
            assert!(q.small_order.is_empty());
            assert_eq!(q.large_order.len(), 2);
            assert_eq!(q.small_queued, 0, "and the small-lane count follows it");
        }

        /// Each lane has its OWN `Notify`. With one shared `Notify` and
        /// `notify_one`, an enqueue can wake the idle lane and leave its own
        /// entry sitting until something else happens to wake the right worker.
        /// Directed: only the LARGE lane has work, so only a large-lane wakeup
        /// can dispatch it.
        #[tokio::test(start_paused = true)]
        #[serial_test::serial(broadcast_queue_depth_gauge)]
        async fn each_lane_is_woken_by_its_own_enqueue() {
            let queue = BroadcastQueue::new();
            let mut lanes = spawn_lanes(&queue, 12, 2);
            // Let both workers reach their idle wait before anything is queued.
            assert_eq!(lanes.next_dispatch().await, None, "nothing queued yet");

            let key = contract(11);
            queue
                .enqueue_in_lane(
                    QueuedPayloadClass::Large,
                    key,
                    PeerKeyLocation::random(),
                    state(BIG),
                )
                .await;
            assert_eq!(
                lanes.next_dispatch().await,
                Some((QueuedPayloadClass::Large, key)),
                "a large enqueue must wake the LARGE worker"
            );
        }

        fn small_lane_permit(small: &Arc<Semaphore>, large: &Arc<Semaphore>) -> SendLanePermit {
            parked_small_lane_permit(small, large, super::super::MAX_PARKED_LANE_UPGRADES)
        }

        /// As above, but with an explicit number of parking slots so a test can
        /// drive the exhausted-parking path.
        fn parked_small_lane_permit(
            small: &Arc<Semaphore>,
            large: &Arc<Semaphore>,
            parking: usize,
        ) -> SendLanePermit {
            SendLanePermit::new(
                QueuedPayloadClass::Small,
                small
                    .clone()
                    .try_acquire_owned()
                    .expect("small pool has capacity"),
                large.clone(),
                Arc::new(Semaphore::new(parking)),
            )
        }

        /// The prediction decides scheduling; it must NOT decide how many bytes
        /// may be in flight. A send whose payload turns out to be full state
        /// after all has to take a large-lane permit before anything goes on
        /// the wire, or 12 concurrent full-state streams saturate the uplink.
        ///
        /// Also pins the threshold boundary — `ensure_capacity_for`'s
        /// comparison must agree with `classify_payload_lane`'s, or a payload
        /// exactly at the threshold ships under whichever one is looser — and
        /// the two-phase wait that keeps a burst of mispredictions from
        /// occupying the small pool.
        #[tokio::test(start_paused = true)]
        async fn mispredicted_large_payload_waits_for_large_lane_capacity() {
            let small = Arc::new(Semaphore::new(12));
            let large = Arc::new(Semaphore::new(1));
            let mut permit = small_lane_permit(&small, &large);

            // A correctly-predicted small payload never touches the large lane,
            // and neither does one a single byte under the threshold.
            permit.ensure_capacity_for(1_000).await;
            permit.ensure_capacity_for(BIG - 2).await;
            assert_eq!(
                large.available_permits(),
                1,
                "no upgrade for a payload below the threshold"
            );
            assert_eq!(small.available_permits(), 11);

            // Now the payload turns out to be full state while the large lane
            // is busy. `BIG - 1` is exactly the threshold — the boundary case.
            let occupied = large
                .clone()
                .acquire_owned()
                .await
                .expect("large pool open");
            let mut upgrading = tokio::spawn(async move {
                permit.ensure_capacity_for(BIG - 1).await;
                permit
            });
            assert!(
                tokio::time::timeout(super::super::UPGRADE_SLOT_HOLD_WINDOW / 2, &mut upgrading)
                    .await
                    .is_err(),
                "a payload at the threshold must wait for large-lane capacity"
            );
            assert_eq!(
                small.available_permits(),
                11,
                "within the hold window the small permit is kept, so the drain \
                 cannot dispatch more concurrent sends than the pools are sized for"
            );

            // Past the hold window the slot goes back, so a burst of
            // mispredictions cannot occupy the wide pool. Nothing is on the
            // wire while it waits.
            assert!(
                tokio::time::timeout(super::super::UPGRADE_SLOT_HOLD_WINDOW * 2, &mut upgrading)
                    .await
                    .is_err(),
                "and it still must not send without large-lane capacity"
            );
            assert_eq!(
                small.available_permits(),
                12,
                "past the hold window the small-lane slot is released"
            );

            drop(occupied);
            let permit = tokio::time::timeout(Duration::from_secs(5), upgrading)
                .await
                .expect("the upgrade completes once capacity frees up")
                .expect("upgrade task");
            assert_eq!(
                large.available_permits(),
                0,
                "the send now holds the large-lane permit"
            );
            assert_eq!(small.available_permits(), 12);
            drop(permit);
            assert_eq!(large.available_permits(), 1, "and releases it when done");
        }

        /// The parking bound is meant to be one turnover of the small pool.
        /// The constant cannot reference the pool size (that lives in this
        /// cfg-gated module while `SendLanePermit` is compiled in both builds),
        /// so pin the tie here rather than letting the two drift.
        #[test]
        fn parking_matches_one_small_pool_turnover() {
            assert_eq!(
                super::super::MAX_PARKED_LANE_UPGRADES,
                DEFAULT_SMALL_PAYLOAD_CONCURRENCY,
                "at most one full turnover of the small pool may be parked at once"
            );
        }

        /// A send ALREADY in the large lane must not try to upgrade: it would
        /// wait for a SECOND large permit while holding one, and two such sends
        /// — the whole pool — would wedge full-state broadcasting permanently.
        #[tokio::test(start_paused = true)]
        async fn large_lane_send_does_not_upgrade_against_itself() {
            let large = Arc::new(Semaphore::new(1));
            let mut permit = SendLanePermit::new(
                QueuedPayloadClass::Large,
                large
                    .clone()
                    .try_acquire_owned()
                    .expect("large pool has capacity"),
                large.clone(),
                Arc::new(Semaphore::new(super::super::MAX_PARKED_LANE_UPGRADES)),
            );
            assert_eq!(large.available_permits(), 0, "the pool is now empty");

            // Must return WITHOUT WAITING. A 5-second bound would not catch the
            // guard being deleted: phase 2 drops this send's own permit back
            // into the pool and immediately re-acquires it, so the reverted code
            // still finishes — just `UPGRADE_SLOT_HOLD_WINDOW` later, having
            // churned the pool. Assert the virtual clock did not move at all.
            let started = tokio::time::Instant::now();
            permit.ensure_capacity_for(BIG).await;
            assert_eq!(
                tokio::time::Instant::now(),
                started,
                "a large-lane send must not wait on its own pool"
            );
            assert_eq!(
                large.available_permits(),
                0,
                "and must still hold the permit it started with"
            );
            drop(permit);
            assert_eq!(large.available_permits(), 1);
        }

        /// The parking bound. Once `MAX_PARKED_LANE_UPGRADES` sends are parked,
        /// the next one must KEEP its small-lane slot rather than park too:
        /// parked sends hold no pool permit, so without this the small drain
        /// keeps dispatching replacements and the parked set — one serialized
        /// payload each — grows without limit.
        #[tokio::test(start_paused = true)]
        async fn parking_area_is_bounded_and_falls_back_to_holding_the_slot() {
            let small = Arc::new(Semaphore::new(12));
            let large = Arc::new(Semaphore::new(1));
            let occupied = large
                .clone()
                .acquire_owned()
                .await
                .expect("large pool open");

            // Parking capacity 0: the only choice left is to hold the slot.
            let mut permit = parked_small_lane_permit(&small, &large, 0);
            let mut waiting = tokio::spawn(async move {
                permit.ensure_capacity_for(BIG).await;
                permit
            });
            assert!(
                tokio::time::timeout(super::super::UPGRADE_SLOT_HOLD_WINDOW * 4, &mut waiting)
                    .await
                    .is_err(),
                "still no large-lane capacity, so it must still be waiting"
            );
            assert_eq!(
                small.available_permits(),
                11,
                "with no parking slot free the send keeps its small-lane slot, so \
                 the drain cannot dispatch a replacement — backpressure, not \
                 unbounded parked payloads"
            );

            drop(occupied);
            let permit = tokio::time::timeout(Duration::from_secs(5), waiting)
                .await
                .expect("completes once large-lane capacity frees up")
                .expect("upgrade task");
            assert_eq!(small.available_permits(), 12);
            assert_eq!(large.available_permits(), 0);
            drop(permit);
        }

        /// Cancellation safety, which `ensure_capacity_for`'s doc claims: a send
        /// dropped mid-wait must release whatever it holds, in either phase.
        #[tokio::test(start_paused = true)]
        async fn cancelling_a_waiting_send_releases_its_permit_in_either_phase() {
            for park_first in [false, true] {
                let small = Arc::new(Semaphore::new(12));
                let large = Arc::new(Semaphore::new(1));
                let occupied = large
                    .clone()
                    .acquire_owned()
                    .await
                    .expect("large pool open");
                let mut permit = small_lane_permit(&small, &large);
                let waiting = tokio::spawn(async move {
                    permit.ensure_capacity_for(BIG).await;
                    permit
                });

                // Phase 1 holds the small slot; phase 2 has parked without one.
                let elapse = if park_first {
                    super::super::UPGRADE_SLOT_HOLD_WINDOW * 2
                } else {
                    super::super::UPGRADE_SLOT_HOLD_WINDOW / 2
                };
                assert!(
                    tokio::time::timeout(elapse, waiting).await.is_err(),
                    "[park_first={park_first}] precondition: still waiting"
                );
                // The timeout dropped the JoinHandle's future, but the task
                // keeps running; abort it the way a shutdown would.
                tokio::task::yield_now().await;
                drop(occupied);
                tokio::time::sleep(Duration::from_millis(1)).await;
                assert_eq!(
                    small.available_permits(),
                    12,
                    "[park_first={park_first}] the small-lane slot must come back"
                );
            }
        }

        /// Shutdown: a closed large pool must not strand the send forever. It
        /// proceeds without large-lane capacity — a deliberate, documented
        /// bypass, because the lane worker is exiting anyway.
        #[tokio::test(start_paused = true)]
        async fn closed_large_pool_does_not_strand_the_send() {
            let small = Arc::new(Semaphore::new(12));
            let large = Arc::new(Semaphore::new(1));
            let mut permit = small_lane_permit(&small, &large);
            large.close();

            tokio::time::timeout(Duration::from_secs(5), permit.ensure_capacity_for(BIG))
                .await
                .expect("a closed pool must not block the send forever");
            drop(permit);
            assert_eq!(
                small.available_permits(),
                12,
                "and the small-lane permit is still released exactly once"
            );
        }

        /// The production classifier INPUT. `classify_payload_lane` is pure and
        /// pinned above; this pins the predicate that feeds it, which is what
        /// `BroadcastQueue::enqueue` actually calls. Dropping the `!`, swapping
        /// `&&` for `||`, or hard-coding either term would otherwise leave every
        /// other test in the crate green.
        #[test]
        fn delta_send_expected_requires_a_cached_summary_and_an_unarmed_memo() {
            use crate::ring::delta_incompat::{DeltaIncompat, INCOMPAT_TRIP_THRESHOLD};
            use crate::ring::interest::InterestManager;
            use crate::util::time_source::{DynTimeSource, SharedMockTimeSource};
            use freenet_stdlib::prelude::StateSummary;

            let clock = SharedMockTimeSource::new();
            let time: DynTimeSource = Arc::new(clock);
            let interest = InterestManager::new(time.clone());
            let memo = DeltaIncompat::new(time);

            let key = contract(3);
            let target = PeerKeyLocation::random();
            let peer = PeerKey::from(target.pub_key().clone());

            assert!(
                !delta_send_expected(&interest, &memo, &key, &target),
                "no interest entry at all: the first send must be full state"
            );

            interest.register_peer_interest(&key, peer.clone(), None, false);
            assert!(
                !delta_send_expected(&interest, &memo, &key, &target),
                "interested but summary-less: nothing to compute a delta against"
            );

            interest.update_peer_summary(&key, &peer, StateSummary::from(vec![1, 2, 3]));
            assert!(
                delta_send_expected(&interest, &memo, &key, &target),
                "a cached summary is what makes the send a delta"
            );

            // Arm the delta-incompat memo for this contract: it rejects deltas,
            // so the send is full state despite the cached summary.
            let addr = |port: u16| -> std::net::SocketAddr {
                format!("127.0.0.1:{port}").parse().unwrap()
            };
            for i in 0..INCOMPAT_TRIP_THRESHOLD {
                memo.record_delta_sent(*key.id(), addr(6000 + i as u16));
                memo.note_resync_request(*key.id(), addr(6000 + i as u16));
            }
            assert!(
                memo.deltas_suppressed_peek(key.id()),
                "precondition: the memo is armed"
            );
            assert!(
                !delta_send_expected(&interest, &memo, &key, &target),
                "an armed memo means full state, cached summary or not"
            );
        }

        /// The composition `enqueue` actually calls. The classifier and the
        /// predicate are each pinned above; this pins that they are wired
        /// together the right way round, which is where a silent restoration of
        /// #4961 would live.
        #[test]
        fn lane_for_routes_a_large_state_delta_send_to_the_small_lane() {
            use crate::ring::delta_incompat::{DeltaIncompat, INCOMPAT_TRIP_THRESHOLD};
            use crate::ring::interest::InterestManager;
            use crate::util::time_source::{DynTimeSource, SharedMockTimeSource};
            use freenet_stdlib::prelude::StateSummary;

            let time: DynTimeSource = Arc::new(SharedMockTimeSource::new());
            let interest = InterestManager::new(time.clone());
            let memo = DeltaIncompat::new(time);
            let key = contract(4);
            let target = PeerKeyLocation::random();
            let peer = PeerKey::from(target.pub_key().clone());
            let lane = |state_size| lane_for(&interest, &memo, &key, &target, state_size);

            assert_eq!(
                lane(BIG),
                QueuedPayloadClass::Large,
                "no cached summary: the whole state goes on the wire"
            );
            assert_eq!(
                lane(16),
                QueuedPayloadClass::Small,
                "a small state is small however the payload is shaped"
            );

            interest.register_peer_interest(&key, peer.clone(), None, false);
            interest.update_peer_summary(&key, &peer, StateSummary::from(vec![1, 2, 3]));
            assert_eq!(
                lane(BIG),
                QueuedPayloadClass::Small,
                "THE #4961 CASE: a >64 KiB contract sending a delta to a peer whose \
                 summary we hold must not spend one of the 2 large-lane permits"
            );

            let addr = |port: u16| -> std::net::SocketAddr {
                format!("127.0.0.1:{port}").parse().unwrap()
            };
            for i in 0..INCOMPAT_TRIP_THRESHOLD {
                memo.record_delta_sent(*key.id(), addr(7000 + i as u16));
                memo.note_resync_request(*key.id(), addr(7000 + i as u16));
            }
            assert_eq!(
                lane(BIG),
                QueuedPayloadClass::Large,
                "...but once the contract is known to reject deltas, it is a \
                 full-state send again"
            );
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
    // #4952: this MUST be the upsert, not `update_peer_summary` — the latter
    // silently no-ops for a peer with no interest entry, which turned every
    // advertised co-host (fan-out targets come from NeighborHosting, a
    // population frequently untracked at broadcast time) into a full-state
    // fixed point: full state on every update, forever. The upsert creates the
    // entry (capped, no demand-counter writes) so the next send is a delta.
    if let Some(summary) = our_summary {
        interest_manager.upsert_peer_summary_from(
            key,
            peer_key,
            summary.clone(),
            crate::ring::interest::SummaryPopulationSource::Delivery,
        );
    }
}

/// Send a state change broadcast to a single peer.
///
/// This is the per-target body extracted from `broadcast_state_to_peers`.
/// It handles delta computation, streaming vs inline decision, and telemetry.
///
/// For streaming sends, a completion oneshot is created internally and threaded
/// through the stream send path. The function awaits it (with timeout) so the
/// semaphore permit — owned by `scheduling`, dropped when this function returns
/// — is held until the actual stream transfer finishes.
///
/// `scheduling` is `Some` exactly when the production queue dispatched this
/// send, and carries both the lane it was classified into and that lane's
/// permit. The sim fan-out calls in-line with `None` (no queue, no permits).
pub(super) async fn broadcast_to_single_peer(
    bridge: &P2pBridge,
    op_manager: &Arc<OpManager>,
    key: ContractKey,
    new_state: WrappedState,
    target: PeerKeyLocation,
    scheduling: Option<QueueScheduling>,
) {
    use crate::message::{DeltaOrFullState, NetMessage};
    use crate::node::network_bridge::NetworkBridge;
    use crate::operations::update::{BroadcastStreamingPayload, UpdateMsg};
    use crate::ring::PeerKey;
    use crate::transport::peer_connection::StreamId;

    let Some(peer_addr) = target.socket_addr() else {
        return;
    };

    // Split the bundle: the class is a fixed property of how this send was
    // SCHEDULED (the mismatch counters compare it against the real payload), the
    // permit is live state that may be upgraded once the payload is known.
    let (queued_class, mut lane_permit) = match scheduling {
        Some(QueueScheduling {
            queued_class,
            permit,
        }) => (Some(queued_class), Some(permit)),
        None => (None, None),
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

    // Cost telemetry (cost-aware eviction, #4861): attribute this per-peer
    // send's two costs to the contract:
    //   * `ExecCpuMicros` — the summarize + delta WASM work below, burned for
    //     EVERY peer send (dominant, otherwise-unmetered CPU for a
    //     high-frequency tiny-payload storm). Wall elapsed of the awaited
    //     computation via the ring's injected TimeSource; may include executor
    //     queueing, still work this send triggered.
    //   * `BroadcastFanoutCost` — the ACTUAL payload bytes put on the wire,
    //     known only HERE after delta selection (the #4903 review P1 fix: the
    //     dispatch site can no longer charge `full-state × targets`, which
    //     phantom-inflated a large-state contract that sends tiny deltas).
    // The CPU is reported at ALL exits (the WASM summarize+delta work is burned
    // regardless of whether the send lands): the two summary-skip exits and the
    // send-attempt path each report the CPU they burned. The payload BYTES are
    // reported ONLY on a real delivery (streaming `Delivered` / inline send Ok
    // — #4903 review round-3 Fix 4), NOT up-front: a dropped/timed-out stream or
    // a failed enqueue put nothing on the wire, so charging its bytes inflated
    // the contract's BroadcastFanoutCost with phantom fan-out. (This is why the
    // send-CPU and bytes reports are no longer batched — they now happen at
    // different points in the send lifecycle.)
    let cost_clock = op_manager.ring.time_source.clone();
    let send_wasm_started = cost_clock.now();
    // Reports ONLY the send-CPU (summarize+delta WASM), burned for every attempt
    // regardless of whether the send lands. The fan-out payload BYTES are charged
    // separately at the real-delivery sites below (#4903 review round-3 Fix 4),
    // so a dropped/timed-out stream or a failed enqueue never charges phantom
    // BroadcastFanoutCost.
    let report_send_cpu = |op_manager: &Arc<OpManager>| {
        use crate::topology::meter::ResourceType;
        let elapsed_us = cost_clock
            .now()
            .saturating_duration_since(send_wasm_started)
            .as_micros() as f64;
        op_manager.ring.report_contract_resource_usage(
            *key.id(),
            ResourceType::ExecCpuMicros,
            elapsed_us,
        );
    };

    // Get our summary for delta computation
    let our_summary = op_manager
        .interest_manager
        .get_contract_summary(op_manager, &key)
        .await;

    // Read and classify the peer-summary state in one interest-manager
    // critical section. The optional RAII guard below only records impact if
    // the send reaches its real delivery gate.
    let (their_summary, tracked_missing_reason, missing_attempt) = if our_summary.is_some() {
        match op_manager
            .interest_manager
            .begin_peer_summary_broadcast(&key, &peer_key)
        {
            crate::ring::interest::PeerSummaryForBroadcast::Known(summary) => {
                (Some(summary), None, None)
            }
            crate::ring::interest::PeerSummaryForBroadcast::Missing { reason, attempt } => {
                (None, reason, attempt)
            }
        }
    } else {
        (None, None, None)
    };
    let mut missing_attempt_guard = missing_attempt.map(|attempt| {
        op_manager
            .interest_manager
            .missing_summary_attempt_guard(attempt)
    });

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
            // Refresh the interest TTL even though nothing is sent (#3046,
            // #3093). A peer we believe is CONVERGED is not less interested
            // than a diverged one — skipping the payload must not expire the
            // interest.
            //
            // Only `record_delivery_to_interest` refreshed the TTL, and it runs
            // only on a delivered send, so every skip was silently aging the
            // entry toward `INTEREST_TTL`. That was unreachable in practice
            // while a renewal wiped the cached summary (no summary → no
            // `theirs` → no skip). Preserving the summary across renewals makes
            // the skip reachable, and without this the peer stops receiving
            // ANYTHING once the interest expires:
            // `test_interest_ttl_refresh_on_broadcast` caught exactly that,
            // decaying 98 → 29 → 0 broadcasts across the TTL boundary.
            //
            // TRADEOFF, stated honestly: `theirs` is OUR cached belief about the
            // peer (`interested_peers`), not evidence from it. So a wedged but
            // still-connected peer whose cached summary happens to match ours
            // gets refreshed indefinitely, where the TTL previously reaped it.
            // Two backstops bound that: disconnect drops the entry outright
            // (`InterestManager::remove_peer`), and the peer's own ~5-min
            // `Interests` heartbeat is a full REPLACE (node.rs), so an entry the
            // peer no longer claims is removed regardless of how recently we
            // refreshed it. TTL expiry is therefore not the mechanism that
            // reaps a live-but-wedged peer, and using it as one costs every
            // converged peer its updates.
            //
            // Refresh ONLY. Do not cache the summary or record delivery
            // telemetry here: nothing was delivered, and `sent_delta` has no
            // truthful value for a send that did not happen.
            op_manager
                .interest_manager
                .refresh_peer_interest(&key, &peer_key);
            report_send_cpu(op_manager);
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

    // Compute delta if we have their summary.
    //
    // Every arm below is tagged with the `PayloadArm` that produced it. The
    // three full-state arms have completely different remedies, and until
    // #3335 none of them were separately instrumented — the production
    // measurement could see that large states go out whole but not why. The
    // tag is carried to the real-delivery sites below and recorded there, so
    // the mix counts bytes that actually reached the wire.
    // Gate inputs for the `FullNotEfficient` arm, carried to the delivery site
    // so the payload-mix rollup can report the real (summary_size, state_size)
    // each refusal was observed under. `DeltaUnavailable::NotEfficient` has
    // always carried these, but its only reader was a `debug!` — compiled out
    // in release — so in production the refusals were unattributable. Since
    // #4923 the refusal itself is POST-compute (the computed delta was not
    // smaller than the state), so the ratio of these two inputs field-checks
    // the old pre-compute proxy rather than restating the trigger. See #3335.
    let mut not_efficient_gate_inputs: Option<(usize, usize)> = None;
    // #4961: WHY a tracked peer has no cached summary. Set only on the
    // `FullNoTheirSummaryTracked` arm below, where the entry exists to be read.
    let (payload, sent_delta, payload_arm) = match (&our_summary, &their_summary) {
        // Scoped to the both-summaries-present case ON PURPOSE. When a summary
        // is missing a delta was impossible regardless of the memo, so letting
        // the suppression guard win there would credit the memo for a full
        // state it did not cause — overstating `FullDeltaSuppressed` and
        // undercounting the no-summary arms, which is exactly the distinction
        // this instrumentation exists to draw. Behavior is unchanged either way
        // (a missing summary falls to the no-summary arms below, which also
        // send full state and also never reach `compute_delta`), so this is
        // purely about attributing the bytes to the right cause.
        (Some(_), Some(_)) if deltas_suppressed => (
            DeltaOrFullState::FullState(new_state.as_ref().to_vec()),
            false,
            PayloadArm::FullDeltaSuppressed,
        ),
        (Some(ours), Some(theirs)) => {
            match op_manager
                .interest_manager
                .compute_delta(op_manager, &key, theirs, ours, new_state.size())
                .await
            {
                Ok(Some(delta)) => (
                    DeltaOrFullState::Delta(delta.as_ref().to_vec()),
                    true,
                    PayloadArm::Delta,
                ),
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
                    // Same outcome as the summaries-equal skip above — "the peer
                    // is converged, send nothing" — so it owes the same TTL
                    // refresh, for the same reason and with the same tradeoff.
                    // This exit is self-limiting (the empty delta is cached, so
                    // the next fan-out takes the `Skip` path above and refreshes
                    // there), but a skip that ages the entry toward
                    // `INTEREST_TTL` is the same class of bug either way.
                    op_manager
                        .interest_manager
                        .refresh_peer_interest(&key, &peer_key);
                    // #4903 review P2: the summarize + delta WASM already ran,
                    // so account for the CPU it burned even though nothing is
                    // sent (no bytes). Previously this exit returned without
                    // reporting, undercounting the send-CPU axis.
                    report_send_cpu(op_manager);
                    return;
                }
                Err(err) => {
                    tracing::debug!(
                        contract = %key,
                        error = %err,
                        "Delta computation failed, falling back to full state"
                    );
                    // Split the refusal from a genuine failure: `NotEfficient`
                    // means the contract DID compute a delta but it was not
                    // smaller than our full state (post-#4923 semantics), so
                    // the full state sent here is the genuinely optimal
                    // payload — equal or fewer bytes than the refused delta.
                    let arm = match err {
                        crate::ring::interest::DeltaUnavailable::NotEfficient {
                            summary_size,
                            state_size,
                        } => {
                            not_efficient_gate_inputs = Some((summary_size, state_size));
                            PayloadArm::FullNotEfficient
                        }
                        crate::ring::interest::DeltaUnavailable::ComputeFailed(_) => {
                            PayloadArm::FullComputeFailed
                        }
                    };
                    (
                        DeltaOrFullState::FullState(new_state.as_ref().to_vec()),
                        false,
                        arm,
                    )
                }
            }
        }
        // No summary on one side, so no delta was possible. Which side is
        // missing decides the remedy, and until #3335's follow-up they were
        // indistinguishable in the rollup:
        //
        //   * ours missing  -> `get_contract_summary` failed (contract-handler
        //     timeout at BROADCAST_CH_TIMEOUT, WASM error, unexpected reply).
        //     A LOAD problem. It also poisons the peer: `sender_summary_bytes`
        //     below is `unwrap_or_default()`, so the peer caches an EMPTY
        //     summary as ours with no way to tell it from a real one.
        //
        //   * theirs missing -> a peer-summary CACHE gap, and the two
        //     sub-cases differ in HOW they self-heal. Broadcast targets come
        //     from `neighbor_hosting` (advertised co-hosts) since #4642 step 9
        //     dropped the interest-manager fan-out arm, so a target is often
        //     untracked in `interested_peers` at broadcast time (the
        //     heartbeat-registration chain exists — register_local_hosting →
        //     Interests → register — but frequently hasn't fired or was
        //     full-replace-wiped for this pair). Pre-#4952 that population
        //     was a fixed point: the post-delivery cache write was a silent
        //     `update_peer_summary` no-op, so every broadcast stayed full
        //     state, permanently. Since #4952 the delivery path UPSERTS, so
        //     untracked is transient (one full state seeds the summary) and
        //     this arm should decay toward first-send-only levels — a
        //     persistent residual now implicates the seeding/heartbeat chain
        //     (e.g. an Interests full-replace wiping the pair each ~5 min),
        //     not the old structural trap. Tracked-but-summaryless (arm 6)
        //     repairs on the next delivery, same as before.
        //
        // `get_peer_interest` is an in-memory DashMap read — no contract
        // handler round-trip — so this classification costs nothing on a path
        // that has already decided to put a whole state on the wire.
        (None, _) => (
            DeltaOrFullState::FullState(new_state.as_ref().to_vec()),
            false,
            PayloadArm::FullNoOurSummary,
        ),
        (Some(_), None) => {
            let arm = if tracked_missing_reason.is_some() {
                PayloadArm::FullNoTheirSummaryTracked
            } else {
                PayloadArm::FullNoTheirSummaryUntracked
            };
            (
                DeltaOrFullState::FullState(new_state.as_ref().to_vec()),
                false,
                arm,
            )
        }
    };
    let payload_size = payload.size();
    match (
        queued_class,
        payload_size < BROADCAST_QUEUE_PAYLOAD_SIZE_THRESHOLD,
    ) {
        (Some(QueuedPayloadClass::Large), true) => {
            crate::node::BROADCAST_QUEUE_EFFICIENCY_METRICS
                .record_queued_large_actual_small(payload_size);
        }
        (Some(QueuedPayloadClass::Small), false) => {
            crate::node::BROADCAST_QUEUE_EFFICIENCY_METRICS
                .record_queued_small_actual_large(payload_size);
        }
        _ => {}
    }
    // Attribute the summarize + delta WASM (CPU) burned to produce this send.
    // Reported for EVERY attempt (the WASM work ran regardless of whether the
    // send lands). The payload BYTES are charged separately, ONLY on a real
    // delivery below (#4903 review round-3 Fix 4) — not here — so a dropped or
    // failed send never charges phantom fan-out bytes. `payload_size` is the
    // real delta/full-state size chosen above, not a `full-state × targets`
    // over-estimate.
    //
    // MUST stay ABOVE `ensure_capacity_for`: all the WASM work is done by this
    // point, and that call can park for seconds waiting on uplink capacity.
    // `report_send_cpu` measures WALL time since `send_wasm_started`, so
    // reporting after the wait would bill a scheduler queue as contract CPU —
    // on the very axis (`ExecCpuMicros`) that cost-pressure eviction reads,
    // whose per-node floor is 50 ms/s. One 5 s wait would be 100× that floor
    // and could evict a contract for work it never did. Pinned by
    // `broadcast_to_single_peer_gates_wire_payload_on_lane_capacity_pin`.
    report_send_cpu(op_manager);

    // The lane was picked from a PREDICTION of this payload. Now that the real
    // size is known, make the permit match it before any bytes go out: a
    // mispredicted full state must wait for large-lane capacity exactly as if
    // it had been queued large, or 12 concurrent full-state streams saturate
    // the uplink — the failure this queue exists to prevent. The mismatch is
    // counted above first, so the prediction's accuracy stays measurable
    // independently of the correction. No-op for a correctly-classified send.
    if let Some(permit) = lane_permit.as_mut() {
        permit.ensure_capacity_for(payload_size).await;
    }

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
                    if let Some(guard) = missing_attempt_guard.as_mut() {
                        guard.mark_delivered(payload_size);
                    }
                    // #4903 review round-3 Fix 4: charge the fan-out payload
                    // bytes ONLY on a real delivery. A dropped/timed-out stream
                    // put no bytes on the wire; the send CPU was already charged
                    // for the attempt above.
                    op_manager.ring.report_contract_resource_usage(
                        *key.id(),
                        crate::topology::meter::ResourceType::BroadcastFanoutCost,
                        payload_size as f64,
                    );
                    // Same delivery gate as the cost axis above, so the mix and
                    // the cost axis always agree on what "sent" means (#3335).
                    op_manager.payload_mix.record_delivered(
                        payload_arm,
                        key.id(),
                        payload_size,
                        not_efficient_gate_inputs,
                        tracked_missing_reason,
                    );
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
            if let Some(guard) = missing_attempt_guard.as_mut() {
                guard.mark_delivered(payload_size);
            }
            // #4903 review round-3 Fix 4: charge the fan-out payload bytes on a
            // successful send only. For the inline path a successful enqueue is
            // the terminal delivery signal (see the branch comment above); the
            // send CPU was already charged for the attempt.
            op_manager.ring.report_contract_resource_usage(
                *key.id(),
                crate::topology::meter::ResourceType::BroadcastFanoutCost,
                payload_size as f64,
            );
            // Same delivery gate as the cost axis above, so the mix and the
            // cost axis always agree on what "sent" means (#3335).
            op_manager.payload_mix.record_delivered(
                payload_arm,
                key.id(),
                payload_size,
                not_efficient_gate_inputs,
                tracked_missing_reason,
            );
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

    /// #4952 — the untracked-co-host counterpart of the #4145 test above,
    /// driven through the REAL production gate. The peer is a fan-out target
    /// from `neighbor_hosting` with NO interest entry at all (not merely no
    /// summary). Pre-#4952 the post-delivery cache write was a silent
    /// `update_peer_summary` no-op, so this test FAILS on that code; it also
    /// fails on the semantic dodge the source pin can't catch (e.g. re-gating
    /// the upsert on prior tracking), because it asserts through the gate, not
    /// the source text.
    #[tokio::test]
    async fn untracked_peer_delivery_seeds_interest_and_summary() {
        let our_summary = StateSummary::from(vec![5, 6, 7, 8]);

        let time_source = SharedMockTimeSource::new();
        let manager = InterestManager::new(time_source.clone());
        let contract = make_contract_key(11);
        let peer = make_peer_key();

        // NO register_peer_interest: an advertised co-host untracked at
        // broadcast time — the #4952 population.
        assert!(
            manager.get_peer_interest(&contract, &peer).is_none(),
            "precondition: the peer must be untracked"
        );

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
        assert!(delivered);

        assert_eq!(
            manager.get_peer_summary(&contract, &peer),
            Some(our_summary),
            "#4952: a delivered full-state broadcast to an UNTRACKED peer must \
             seed the interest entry + summary, so the next broadcast is a \
             delta — otherwise the pair is a full-state fixed point"
        );
    }

    /// #4952 divergence guard for the untracked population: a NON-delivered
    /// full-state send must not fabricate an interest entry carrying a summary
    /// the peer never received (which would suppress the summary-mismatch
    /// resend — the #4235 failure mode, now newly reachable because the
    /// delivery path can create entries).
    #[tokio::test]
    async fn untracked_peer_drop_outcome_does_not_fabricate_interest() {
        let our_summary = StateSummary::from(vec![3, 3, 3]);
        let dropped = dropped_oneshot().await;
        let timed_out = elapsed_timeout().await;
        let cases: Vec<(&str, super::StreamCompletionResult)> = vec![
            ("explicit-drop", Ok(Ok(BroadcastDeliveryOutcome::Dropped))),
            ("dropped-oneshot", Ok(dropped)),
            ("timeout", Err(timed_out)),
        ];

        for (name, completion) in cases {
            let time_source = SharedMockTimeSource::new();
            let manager = InterestManager::new(time_source.clone());
            let contract = make_contract_key(12);
            let peer = make_peer_key();

            let delivered = record_streaming_delivery(
                &manager,
                completion,
                /* sent_delta */ false,
                &contract,
                &peer,
                Some(&our_summary),
                /* state_size */ 2048,
                /* payload_size */ 2048,
            );
            assert!(!delivered, "[{name}] must not classify as delivered");
            assert!(
                manager.get_peer_interest(&contract, &peer).is_none(),
                "[{name}] a non-delivered send must NOT fabricate an interest \
                 entry for an untracked peer — a summary the peer never \
                 received would suppress the mismatch resend (#4235)"
            );
        }
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
    /// #4952 pin: the post-delivery summary cache must route through
    /// `upsert_peer_summary`, never `update_peer_summary`. The latter is a
    /// silent no-op for a peer with no interest entry, and fan-out targets
    /// come from `neighbor_hosting` (advertised co-hosts) — a population that
    /// never registers interest — so an `update_` call here re-opens the
    /// full-state-forever fixed point that was 58% of fleet broadcast bytes.
    /// Matches whitespace-stripped source so a rustfmt reflow can't dodge it.
    #[test]
    fn record_delivery_routes_summary_cache_through_upsert() {
        let src = include_str!("broadcast_queue.rs");
        let fn_start = src
            .find("fn record_delivery_to_interest<")
            .expect("record_delivery_to_interest not found");
        let after = &src[fn_start..];
        let fn_end = after
            .find("\npub(super) async fn broadcast_to_single_peer(")
            .expect("end of record_delivery_to_interest not found");
        let body: String = after[..fn_end].split_whitespace().collect();
        assert!(
            body.contains(
                "interest_manager.upsert_peer_summary_from(key,peer_key,summary.clone(),"
            ),
            "post-delivery cache must upsert (create-if-absent) the peer summary"
        );
        assert!(
            !body.contains("interest_manager.update_peer_summary("),
            "update_peer_summary silently no-ops for untracked peers — the \
             #4952 fixed point. Use upsert_peer_summary here."
        );
    }

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
            body.contains("if deltas_suppressed => ("),
            "suppression must short-circuit the payload match to FullState"
        );
        // The guard is deliberately scoped to `(Some(_), Some(_))` rather than
        // `_`: with a summary missing a delta was impossible anyway, so a
        // wildcard guard would credit the memo for a full state it did not
        // cause and skew the #3335 payload-mix attribution. Safety is
        // unaffected — `compute_delta` lives only in the `(Some(ours),
        // Some(theirs))` arm, so a suppressed contract cannot reach it via
        // either path.
        assert!(
            body.contains("(Some(_), Some(_)) if deltas_suppressed => ("),
            "the suppression guard must be scoped to the both-summaries-present \
             case — a wildcard guard mis-attributes missing-summary full states \
             to FullDeltaSuppressed (#3335 payload-mix accuracy)"
        );
        // The guard arm must still be FIRST in the payload match: Rust
        // evaluates arms in order, so it has to precede the
        // `(Some(ours), Some(theirs))` compute_delta arm — otherwise a
        // suppressed contract with both summaries present would compute and
        // send the doomed delta (or, post-#4901, hit the Ok(None) converged
        // skip) instead of forcing full state. The `.suppress_deltas(` call
        // above precedes the match regardless, so only this arm-ordering
        // assertion catches a reordering regression.
        let guard_arm = body
            .find("if deltas_suppressed => (")
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
    // sending FULL STATE) or — before #4923 removed the pre-compute
    // `is_delta_efficient` gate — was refused outright on big-summary
    // contracts, so a fully-converged pair re-flooded full state on every
    // fan-out (contracts like `Eumk9HNQ` healing hard while their state never
    // changed). These tests exercise the cache-only decision core
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

    /// Source-scrape pin (cost-aware eviction, #4861 / #4903 review P1+P2 +
    /// round-3 Fix 4): `broadcast_to_single_peer` must charge its per-send CPU
    /// at ALL THREE exits — the summaries-equal skip, the empty-delta converged
    /// skip (the review-P2 exit that previously returned without reporting), and
    /// the send attempt — since the WASM summarize+delta work is burned
    /// regardless of whether the send lands. The fan-out payload BYTES, by
    /// contrast, are charged ONLY at the two real-delivery sites (streaming
    /// `Delivered` + inline send Ok), NEVER up-front — a dropped/failed send put
    /// nothing on the wire (round-3 Fix 4). Dropping a report silently blinds
    /// the cost-pressure eviction trigger while every behavioral test stays
    /// green.
    #[test]
    fn broadcast_to_single_peer_reports_send_cost_pin() {
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

        // The CPU-only closure must be invoked at all three exits (the WASM
        // summarize+delta work is burned regardless of whether the send lands).
        let report_invocations = body.matches("report_send_cpu(op_manager)").count();
        assert_eq!(
            report_invocations, 3,
            "broadcast_to_single_peer must invoke report_send_cpu at all three \
             exits (summaries-equal skip + empty-delta converged skip + send \
             attempt) — got {report_invocations}. A dropped report blinds \
             cost-pressure eviction (#4861) to per-send CPU."
        );
        // Bytes are attributed on the BroadcastFanoutCost axis at exactly the
        // two real-delivery sites (streaming `Delivered` + inline send Ok), and
        // the send-CPU on the ExecCpuMicros axis (split needles so this test
        // cannot self-count). The count of 2 (not 3) is the round-3 Fix 4 pin:
        // the CPU closure no longer charges bytes, so a dropped/failed send
        // charges no phantom fan-out.
        let cpu_needle = concat!("ResourceType::", "Exec", "CpuMicros");
        let bytes_needle = concat!("ResourceType::", "Broadcast", "FanoutCost");
        assert!(
            body.contains(cpu_needle),
            "report_send_cpu must attribute on the ExecCpuMicros axis"
        );
        assert_eq!(
            body.matches(bytes_needle).count(),
            2,
            "fan-out bytes must be charged on the BroadcastFanoutCost axis at \
             exactly the two real-delivery sites (streaming Delivered + inline \
             send Ok) — never up-front (review round-3 Fix 4)"
        );
        // The selected payload size (delta or full state) is what's charged, at
        // both delivery sites and nowhere else.
        assert_eq!(
            body.matches("payload_size as f64").count(),
            2,
            "each delivery-gated bytes report must charge the selected \
             payload_size (delta or full state), not the pre-delta full-state size"
        );
    }

    /// The uplink bound now depends on ONE call in `broadcast_to_single_peer`
    /// — the lane correction — and on it running in the right place. Neither
    /// is covered by a behavioural test: nothing in the crate calls
    /// `broadcast_to_single_peer` (it needs a live bridge and OpManager), so
    /// deleting the call, or sliding it past a send, leaves the whole suite
    /// green while up to 12 concurrent full-state streams go out on the
    /// small lane. Same reason the seven other pins in this file exist.
    #[test]
    fn broadcast_to_single_peer_gates_wire_payload_on_lane_capacity_pin() {
        let src = include_str!("broadcast_queue.rs");
        // Column-0 anchor. This test lives in the module AFTER the function, as
        // its sibling pins do, so a bare `find` would work — but it must stay
        // that way: an earlier copy of this string (e.g. moving this test into
        // `mod queue`'s test module, which precedes the function) silently
        // re-slices the SIBLING pins onto the wrong region. Two of them failed
        // exactly that way while this test was being written.
        let start = src
            .find("\npub(super) async fn broadcast_to_single_peer(")
            .expect("broadcast_to_single_peer renamed or removed");
        let end = src[start..]
            .find("\n#[cfg(test)]")
            .expect("end of broadcast_to_single_peer not found")
            + start;
        let body = &src[start..end];

        let upgrade = body
            .find("ensure_capacity_for(payload_size)")
            .expect("the actual wire payload must be gated on lane capacity");

        // Before EVERY send. `bridge.send` covers both the inline broadcast
        // and the streaming metadata; the fragments follow it.
        for send in ["bridge.send(peer_addr", "send_stream_with_completion("] {
            let at = body
                .find(send)
                .unwrap_or_else(|| panic!("send site `{send}` not found"));
            assert!(
                upgrade < at,
                "the lane correction (offset {upgrade}) must run BEFORE `{send}` \
                 (offset {at}) — bytes must never reach the wire under a permit \
                 that does not cover them"
            );
        }

        // ...and AFTER the CPU report. `report_send_cpu` measures wall time
        // since the summarize/delta work started, so reporting it on the far
        // side of an uplink-capacity wait bills scheduler queueing to the
        // contract's ExecCpuMicros — the axis cost-pressure eviction reads.
        let cpu = body
            .rfind("report_send_cpu(op_manager);")
            .expect("send-CPU report not found");
        assert!(
            cpu < upgrade,
            "report_send_cpu (offset {cpu}) must run BEFORE the lane correction \
             (offset {upgrade}), or a semaphore wait is charged as contract CPU"
        );
    }

    /// Source-scrape pin (#3046 / #3093): BOTH converged skips must refresh the
    /// peer's interest TTL.
    ///
    /// `record_delivery_to_interest` refreshes only on a DELIVERED send, so a
    /// peer we keep skipping — because we believe it already has our state —
    /// ages toward `INTEREST_TTL` and is reaped, after which it receives
    /// nothing at all. The skip is a permanent steady state for a converged
    /// subscriber, so this is not a rare corner.
    ///
    /// Pinned at the source because the behavioural evidence is indirect: the
    /// TTL decay only shows up as a broadcast count falling to zero across the
    /// TTL boundary in a long simulation, and the empty-delta exit is
    /// self-limiting (its verdict is cached, so the next fan-out takes the
    /// summaries-equal exit instead) — a revert there would leave every
    /// behavioural test green.

    #[test]
    fn broadcast_to_single_peer_refreshes_interest_on_every_skip_pin() {
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

        // Two skip exits (summaries-equal + empty-delta), one refresh each. The
        // delivered path refreshes via `record_delivery_to_interest`, not here,
        // so a third occurrence would mean a skip refresh drifted onto the send
        // path or the delivered path grew a duplicate.
        let refreshes = body
            .matches("refresh_peer_interest(&key,&peer_key)")
            .count()
            + body
                .matches("refresh_peer_interest(&key, &peer_key)")
                .count();
        assert_eq!(
            refreshes, 2,
            "broadcast_to_single_peer must refresh the interest TTL at BOTH \
             converged-skip exits (summaries-equal and empty-delta) — got \
             {refreshes}. Skipping the payload must not expire the interest."
        );

        // Bind each refresh to its own exit: the count alone would stay green
        // if both landed in the same arm.
        let equal_skip = body
            .find("Skipping broadcast - peer already has our state")
            .expect("summaries-equal skip arm not found");
        let empty_delta_skip = body
            .find("Skipping broadcast - contract reported empty delta")
            .expect("empty-delta skip arm not found");
        assert!(
            equal_skip < empty_delta_skip,
            "unexpected arm order; the offsets below assume summaries-equal \
             precedes empty-delta"
        );
        assert!(
            body[equal_skip..empty_delta_skip].contains("refresh_peer_interest("),
            "the summaries-equal skip must refresh the interest TTL before it \
             returns"
        );
        assert!(
            body[empty_delta_skip..].contains("refresh_peer_interest("),
            "the empty-delta (converged) skip must refresh the interest TTL \
             before it returns — same outcome as the skip above, same obligation"
        );

        // A skip delivered nothing, so it must not record delivery telemetry or
        // cache the peer's summary: `sent_delta` has no truthful value for a
        // send that did not happen, and caching our summary for a peer we never
        // sent to is the #2763/#4235 divergence hazard.
        assert!(
            !body[equal_skip..empty_delta_skip].contains("record_delivery_to_interest("),
            "the summaries-equal skip must refresh ONLY — recording a delivery \
             that did not happen corrupts the delta/full-state telemetry"
        );
    }

    /// Source-scrape pin for the payload-mix arm tagging (#3335).
    ///
    /// Same precedent as the cost-report pin above — a manually-mirrored
    /// telemetry counter, where the mirror and its source can silently diverge:
    /// a refactor that drops an arm tag, or re-tags a full-state fallback as
    /// `Delta`, silently corrupts the measurement that decides which fan-out
    /// fix to build — and every behavioral test stays green, because the
    /// fan-out still works. The whole point of this instrumentation is that
    /// the four full-state causes are distinguishable, so pin that each one
    /// is constructed exactly where it is decided.
    #[test]
    fn broadcast_to_single_peer_tags_every_payload_arm_pin() {
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

        // Every arm must be constructed in the selection match. A missing arm
        // means some payload is attributed to the wrong cause (or to none).
        for arm in [
            "PayloadArm::Delta",
            "PayloadArm::FullDeltaSuppressed",
            "PayloadArm::FullNotEfficient",
            "PayloadArm::FullComputeFailed",
            "PayloadArm::FullNoOurSummary",
            "PayloadArm::FullNoTheirSummaryUntracked",
            "PayloadArm::FullNoTheirSummaryTracked",
        ] {
            assert!(
                body.contains(arm),
                "broadcast_to_single_peer must tag the {arm} arm — an untagged \
                 fallback makes the #3335 payload-mix measurement attribute \
                 bytes to the wrong cause"
            );
        }

        // #5090: summary presence, missing-reason classification, and attempt
        // correlation must come from one atomic interest-manager operation.
        // Reintroducing separate reads would let population/removal race the
        // observation and make the lifecycle evidence internally inconsistent.
        assert_eq!(
            body.matches("begin_peer_summary_broadcast(&key, &peer_key)")
                .count(),
            1,
            "broadcast payload selection must use the atomic missing-summary \
             observation/classification operation exactly once"
        );

        // The mix is recorded at exactly the two real-delivery sites, the same
        // gate as BroadcastFanoutCost, so the two axes always agree on what
        // "sent" means. Recording up-front would count phantom fan-out.
        // Match against a whitespace-stripped copy of the body. rustfmt
        // re-wraps this call whenever its argument list changes width (adding
        // the gate-inputs argument split it across five lines), so any needle
        // carrying literal spacing silently rots into a false failure — or,
        // worse, a false PASS if the count happens to still match. Collapsing
        // first makes the pin depend on the code rather than on the formatter;
        // this is the same `collapsed` shape node.rs uses for its
        // `update_peer_summary(&key,pk,None)` pin.
        let collapsed: String = body.chars().filter(|c| !c.is_whitespace()).collect();
        let record_needle = concat!(
            ".record_",
            "delivered(payload_arm,key.id(),payload_size,not_efficient_gate_inputs,tracked_missing_reason,)"
        );
        assert_eq!(
            collapsed.matches(record_needle).count(),
            2,
            "payload mix must be recorded at exactly the two real-delivery \
             sites (streaming Delivered + inline send Ok) — recording up-front \
             would count dropped/failed sends as bytes on the wire"
        );
        // ...and it must be THIS node's accumulator, never a process global:
        // several nodes share a process in the simulation harness and the
        // aggregator drains destructively, so a global would let one node's
        // ticker steal another's records.
        assert_eq!(
            collapsed
                .matches(concat!("op_manager.payload_mix.record_", "delivered("))
                .count(),
            2,
            "the payload mix must be recorded on op_manager.payload_mix (the \
             per-node accumulator), not a process-global static"
        );

        // The `NotEfficient` / `ComputeFailed` split is the load-bearing one:
        // the first means no contract code ran and we shipped a whole state
        // anyway, the second means the WASM failed. Collapsing them back into
        // one arm loses the distinction the measurement exists to make.
        assert!(
            body.contains("DeltaUnavailable::NotEfficient")
                && body.contains("DeltaUnavailable::ComputeFailed"),
            "the delta-failure arm must keep the typed NotEfficient vs \
             ComputeFailed split — collapsing them re-blinds the measurement"
        );

        // The no-summary split must stay decided by WHICH side is missing, and
        // the atomic peer-summary observation must preserve tracked (reason is
        // Some) versus untracked (reason is None). Collapsing either one
        // re-creates the single `full_no_summary`
        // bucket that the 2026-07-25 measurement could not act on: it was the
        // largest consumer of wire bytes on the network with no way to tell a
        // contract-handler failure from a permanent peer-tracking gap.
        assert!(
            collapsed.contains("(None,_)=>") && collapsed.contains("(Some(_),None)=>"),
            "the no-summary arms must branch on WHICH side of the pair is \
             missing — a catch-all `_` arm re-blinds the split"
        );
        assert!(
            collapsed.contains(
                "PeerSummaryForBroadcast::Missing{reason,attempt}=>{(None,reason,attempt)}"
            ),
            "the atomic peer-summary result must carry the reason through so \
             tracked and untracked missing-summary sends remain distinct"
        );
    }
}

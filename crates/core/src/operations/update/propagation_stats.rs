//! Bounded periodic aggregator for UPDATE-propagation liveness logging.
//!
//! ## Why this exists
//!
//! PRs #4252 and #4272 demoted the three high-volume per-event UPDATE log
//! sites (`Contract state updated`, the `UPDATE_PROPAGATION` broadcast line,
//! and the `shadow_rtt_aggregate` local mirror) from INFO to DEBUG to stop a
//! River-driven log-spam disk-fill. The fix worked, but at the default
//! `RUST_LOG=info` level it left operators with **no liveness signal** about
//! whether a node is propagating contract updates at all — only failure-path
//! messages survive at INFO/WARN. For a healthy node carrying River traffic
//! the log goes silent. See issue #4281.
//!
//! This module restores that visibility **without** re-introducing per-event
//! volume: the broadcast path bumps cheap per-contract counters (a single
//! `DashMap` shard write), and a background task drains-and-emits a single
//! bounded INFO summary line (plus up to a fixed number of per-contract lines)
//! once per window.
//!
//! ## Budget
//!
//! At [`SUMMARY_WINDOW`] = 60s and [`TOP_K`] = 8, the worst-case INFO rate
//! from this emitter is `(TOP_K + 1)` lines per window — under 0.2 lines/sec
//! sustained — well inside the "well under 1 line/sec" budget the issue
//! requires. A window with zero activity emits nothing at all.
//!
//! ## Counter semantics
//!
//! Counters are *windowed*: `drain_window` snapshots every entry and clears
//! the map, so each emitted summary describes exactly the activity in the
//! window that just elapsed. Both the record and the drain mutate each
//! contract's counters under the same `DashMap` per-key write guard, so a
//! drain captures each record atomically (never half-applied). The tracking
//! map is size-capped ([`MAX_TRACKED_CONTRACTS`]) so a churn of
//! network-influenced contract keys cannot grow it unboundedly (per the
//! per-key-collection rule in `.claude/rules/code-style.md`).
//!
//! ## Per-leg outcomes and the origination stall WARN (#6143)
//!
//! `targets_avg` counts fan-out targets RESOLVED, which is decided before any
//! per-peer work. In #6143 a node logged `targets_avg=14.0` for each of its
//! own UPDATEs for 80 minutes while no receiver applied any of them, and every
//! place a leg can end without a send logged at debug or trace, which release
//! builds compile out (`release_max_level_info`). So the summary also counts
//! what each leg actually did ([`PropagationEvent`]): handed a delta or a full
//! state to the network bridge, or skipped and why; how the transport then
//! carried it (one packet, or a stream, and whether the stream's last
//! fragment left); the best-effort `try_notify` drops that lose a whole
//! fan-out or heal before it starts; and the receiver-side pre-merge skips.
//! The same counts go to the `broadcast_payload_mix` telemetry event so the
//! fleet can see them.
//!
//! The transport layer matters for #6143 itself: that node's telemetry
//! counted ~14 delta sends per update handed to the bridge, so the loss was
//! below it.
//!
//! On top of that, [`detect_stalls`] WARNs, at the end of window W, for a
//! contract that over windows W-1 and W together had:
//! - a local client update in W-1;
//! - every resolved leg ended unsent: the `skip_*` counts plus the legs the
//!   bridge refused (`send_failed`) reach the legs resolved (`targets_total`
//!   for fan-outs plus `heal_target` for heals, above zero). Or a whole
//!   fan-out was dropped (`fanout_event_dropped`);
//! - nothing at all handed to the network bridge (`sent_delta + sent_full`
//!   is zero across both windows).
//!
//! A queued leg replaced by a newer fan-out or heal for the same (contract,
//! peer) pair counts `skip_superseded`: its update rides on the newer leg,
//! whose own outcome decides whether anything was sent. Before #6143 a
//! superseded leg recorded nothing, so two client updates in a burst counted
//! twice the targets against at most half as many outcomes, and every leg
//! could never be accounted for. A heal registers its leg as `heal_target`,
//! as a fan-out registers its targets, so a heal's skip never stands in for
//! a fan-out leg's outcome.
//!
//! W is grace for a leg queued near the end of W-1, and judging the two
//! windows together catches an update applied at the end of W-1 whose
//! fan-out is processed in W. Every leg must be accounted for because
//! resolved targets alone prove nothing: a leg held while its (contract,
//! peer) pair is busy with a stream, or queued behind the large lane's
//! permits, records nothing while it waits, and that can outlast both
//! windows. So one refused leg beside siblings that are only held is not a
//! stall. The rule reads only bridge-level counts, so the WARN targets the
//! sender-side shapes: every leg skipped or refused, or the whole fan-out
//! dropped. What the transport then did is in the `wire_*` counts, which
//! never raise a WARN.
//!
//! What it cannot see:
//! - Loss at or after the transport: a stream that fails after the bridge
//!   took it, a single packet abandoned after its retransmits, a message lost
//!   on the QUIC path, and anything the receiver does with a message that
//!   arrived. Those are diagnosed from the `wire_*` counts here and the
//!   receiver-side `rx_*` counts across the fleet.
//! - Legs dropped from the broadcast queue at capacity (`evict_oldest`). They
//!   record no event here; the queue WARNs each eviction itself.
//! - Partial failure: one leg handed to the bridge clears both windows, so 13
//!   of 14 legs failing raises nothing. The per-contract counts show it.
//! - Any send for the contract clears it, including a send for a relayed
//!   update or an earlier update in W-1, so a stalled client update beside
//!   one that went out raises nothing.
//! - A fan-out that resolves no targets. That is the `no_targets` count, and
//!   `handle_broadcast_state_change` WARNs once per streak of them.
//! - Relayed updates. Only a client update makes a contract a candidate.
//! - A drain between the executor's fan-out and the client apply's
//!   `client_originated` (recorded in `update_contract` once the executor
//!   returns, after it emitted the fan-out). The legs then land in the
//!   window before the client update, which is not read with it.
//! - A no-targets retry whose re-emission fails. It uses the blocking
//!   `notify_node_event`, not `try_notify`, so a failure is logged and not
//!   counted as `fanout_event_dropped`.
//!
//! How it can be wrong:
//! - The drop counter cannot tell a client fan-out from a relayed one, so a
//!   relayed fan-out dropped beside a client update whose legs are still
//!   held counts as the client's. A relayed fan-out's or a heal's skips do
//!   not do this alone, since each registers its own legs too; but the legs
//!   of a fan-out whose targets were counted before W-1 can record their
//!   skips in W-1, and those count toward the client update's legs.
//! - A contract whose window-W counts were refused at the
//!   [`MAX_TRACKED_CONTRACTS`] cap reads as having sent nothing in W, so at
//!   the cap a send in W can go unseen and the WARN can be false.
//!
//! A client update whose legs were ALL skipped as `skip_summary_equal` (or
//! another converged skip) is a stall, not a success: it changed this node's
//! state, so no peer can already hold it, and a skip says this node's belief
//! about the peer is wrong. That belief going stale is the leading hypothesis
//! for #6143. One exception: a contract whose summary is lossy (two different
//! states can share one) can legitimately match a peer's summary after a real
//! change, so its WARNs are a property of the contract, not of the network.
//!
//! The per-contract INFO lines are the top [`TOP_K`] by update activity, so a
//! stalled contract can be left out of them, into the rollup count, by busier
//! ones. Its stall WARN is not affected.

use std::collections::HashMap;
use std::time::Duration;

use dashmap::DashMap;
use freenet_stdlib::prelude::ContractInstanceId;
use parking_lot::Mutex;

use crate::config::GlobalExecutor;

/// How often the summary task drains the counters and emits a window summary.
/// 60s is the natural starting point per issue #4281 — short enough for
/// usable debugging granularity, long enough to keep the log rate trivial.
pub(crate) const SUMMARY_WINDOW: Duration = Duration::from_secs(60);

/// Maximum number of per-contract lines emitted per window (top-K by update
/// activity). Contracts beyond the top-K are summarised in a single rollup
/// line ("…and N others"). Bounds the per-window INFO line count to
/// `TOP_K + 1`.
const TOP_K: usize = 8;

/// Maximum number of distinct contracts tracked at once. A new contract is
/// dropped (its activity goes uncounted for that window) once the map is at
/// capacity, rather than letting a key churn grow the map without bound.
/// 4096 is comfortably above the active-contract count of a busy gateway.
/// Each entry is at most [`MAX_COUNTERS_SIZE`] plus its key (pinned by
/// `a_tracked_contract_stays_within_its_memory_bound`), about 1.3 MB of
/// entries at the cap. A hash table holds 4096 entries in 8192 buckets
/// (hashbrown's 7/8 load factor rounds up to the next power of two), so the
/// map is about 2.6 MB at the cap. The previous window's client-updated
/// summaries kept for the stall check are bounded the same way: about 5 MB
/// for the two.
const MAX_TRACKED_CONTRACTS: usize = 4096;

/// Upper bound on `size_of::<ContractCounters>()` and
/// `size_of::<ContractSummary>()`, for the memory estimate above.
#[cfg(test)]
const MAX_COUNTERS_SIZE: usize = 288;

/// Most stall WARN lines in one window. Past it, one more WARN gives the
/// number left out. Each WARN is one contract, so this bounds the per-node
/// WARN rate when many of this node's contracts stall at once.
const MAX_STALL_WARNS_PER_WINDOW: usize = 16;

/// One countable thing that happened to an UPDATE's propagation for a
/// contract, other than the fan-out target resolution `record_broadcast`
/// already counts. See the module docs (#6143).
///
/// Counts are per event, not per leg: one leg is usually a `Sent*` event and
/// then a `Wire*` event, and a stream one more `WireStream*` outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PropagationEvent {
    /// A local client UPDATE that changed this node's state, and so emitted a
    /// fan-out. The denominator for the stall check.
    ClientOriginated,
    /// A delta handed to the network bridge (`bridge.send` returned `Ok`).
    SentDelta,
    /// A full state handed to the network bridge: an inline send that
    /// returned `Ok`, or a stream whose dispatch returned `Ok`.
    SentFull,
    /// Skipped: our cached copy of the peer's summary is byte-equal to ours.
    SkipSummaryEqual,
    /// Skipped: the shared delta cache holds an empty delta for this summary
    /// pair, so the peer is believed converged.
    SkipCachedConverged,
    /// Skipped: the bounded WASM staleness probe said the peer is converged.
    SkipProbeConverged,
    /// Skipped: `compute_delta` returned an empty delta (`Ok(None)`).
    SkipEmptyDelta,
    /// Skipped: `should_broadcast_contract` was false (not hosted or in use).
    SkipNotHosted,
    /// Skipped: the target had no socket address.
    SkipNoAddress,
    /// Ended unsent: a newer fan-out or heal replaced this leg's queued
    /// (contract, peer) entry before it ran. Its update rides on the newer
    /// leg, whose own outcome decides whether anything was sent; this leg
    /// recorded nothing before #6143.
    SkipSuperseded,
    /// The network bridge refused a leg: `bridge.send` or the stream dispatch
    /// returned `Err`, or the payload failed to serialize. That leg counts no
    /// `Sent*`, so this is a per-leg outcome disjoint from `SentDelta` /
    /// `SentFull`. A refusal further down, by the transport, is
    /// [`Self::WireSendFailed`].
    SendFailed,
    /// A `BroadcastStateChange` (a whole fan-out) was dropped because the
    /// event-loop notification channel was full.
    FanoutEventDropped,
    /// A `SyncStateToPeer` (a targeted heal) was dropped because the
    /// event-loop notification channel was full.
    HealEventDropped,
    /// A targeted heal resolved its peer and started a leg. The heal's
    /// counterpart of a fan-out's resolved targets (`targets_total`), so the
    /// stall check has a leg for each heal leg's outcome.
    HealTarget,
    /// Transport: an UPDATE broadcast message left as one UDP packet. It is
    /// retransmitted until acknowledged or abandoned, and nothing reports
    /// which, so this is the last thing observed of it.
    WireShort,
    /// Transport: an UPDATE broadcast went out as a stream, either an inline
    /// message too big for one packet or a full-state broadcast stream. Its
    /// fate is one of the three `WireStream*` events below.
    WireStream,
    /// Transport: an UPDATE broadcast message on a path that reports no
    /// per-message fate (the QUIC data path).
    WireUnobserved,
    /// Transport: a stream's last fragment left the sender. NOT a receiver
    /// acknowledgement; the UDP stream path has none.
    WireStreamDelivered,
    /// Transport: a stream failed before its last fragment left (cwnd wait
    /// timeout, send error, connection gone, or no outcome in time).
    WireStreamFailed,
    /// Transport: a stream failed because a fragment missed the node-wide
    /// egress deadline. The transport logs these per stream at debug only.
    WireStreamEgressAbandoned,
    /// Transport: the connection task could not send an inline UPDATE
    /// broadcast the bridge had already taken (the message failed to
    /// serialize, the connection refused it, or it was failing while
    /// draining its queue before shutdown). That leg
    /// counted a `Sent*` event when the bridge took it, possibly in an
    /// earlier window. A counter only, like every `Wire*` event.
    WireSendFailed,
    /// Receiver: an inbound broadcast skipped by the 60 s payload dedup cache.
    RxDedup,
    /// Receiver: an inbound broadcast skipped because the payload is already
    /// merged into local state.
    RxAlreadyApplied,
    /// Receiver: an inbound broadcast skipped by the merge-failure backoff
    /// (per-sender cooldown or a known-failed payload).
    RxBackoff,
}

impl PropagationEvent {
    /// Every event, in reporting order. Kept in lockstep with
    /// [`Self::index`] by `propagation_events_are_densely_indexed`.
    pub(crate) const ALL: [PropagationEvent; 24] = [
        PropagationEvent::ClientOriginated,
        PropagationEvent::SentDelta,
        PropagationEvent::SentFull,
        PropagationEvent::SkipSummaryEqual,
        PropagationEvent::SkipCachedConverged,
        PropagationEvent::SkipProbeConverged,
        PropagationEvent::SkipEmptyDelta,
        PropagationEvent::SkipNotHosted,
        PropagationEvent::SkipNoAddress,
        PropagationEvent::SkipSuperseded,
        PropagationEvent::SendFailed,
        PropagationEvent::FanoutEventDropped,
        PropagationEvent::HealEventDropped,
        PropagationEvent::HealTarget,
        PropagationEvent::WireShort,
        PropagationEvent::WireStream,
        PropagationEvent::WireUnobserved,
        PropagationEvent::WireStreamDelivered,
        PropagationEvent::WireStreamFailed,
        PropagationEvent::WireStreamEgressAbandoned,
        PropagationEvent::WireSendFailed,
        PropagationEvent::RxDedup,
        PropagationEvent::RxAlreadyApplied,
        PropagationEvent::RxBackoff,
    ];

    pub(crate) const COUNT: usize = Self::ALL.len();

    pub(crate) const fn index(self) -> usize {
        match self {
            PropagationEvent::ClientOriginated => 0,
            PropagationEvent::SentDelta => 1,
            PropagationEvent::SentFull => 2,
            PropagationEvent::SkipSummaryEqual => 3,
            PropagationEvent::SkipCachedConverged => 4,
            PropagationEvent::SkipProbeConverged => 5,
            PropagationEvent::SkipEmptyDelta => 6,
            PropagationEvent::SkipNotHosted => 7,
            PropagationEvent::SkipNoAddress => 8,
            PropagationEvent::SkipSuperseded => 9,
            PropagationEvent::SendFailed => 10,
            PropagationEvent::FanoutEventDropped => 11,
            PropagationEvent::HealEventDropped => 12,
            PropagationEvent::HealTarget => 13,
            PropagationEvent::WireShort => 14,
            PropagationEvent::WireStream => 15,
            PropagationEvent::WireUnobserved => 16,
            PropagationEvent::WireStreamDelivered => 17,
            PropagationEvent::WireStreamFailed => 18,
            PropagationEvent::WireStreamEgressAbandoned => 19,
            PropagationEvent::WireSendFailed => 20,
            PropagationEvent::RxDedup => 21,
            PropagationEvent::RxAlreadyApplied => 22,
            PropagationEvent::RxBackoff => 23,
        }
    }

    /// True for a sender-side skip (`Skip*`): a leg this node decided not to
    /// send. Positive evidence for the stall check ([`detect_stalls`]).
    pub(crate) const fn is_skip(self) -> bool {
        matches!(
            self,
            PropagationEvent::SkipSummaryEqual
                | PropagationEvent::SkipCachedConverged
                | PropagationEvent::SkipProbeConverged
                | PropagationEvent::SkipEmptyDelta
                | PropagationEvent::SkipNotHosted
                | PropagationEvent::SkipNoAddress
                | PropagationEvent::SkipSuperseded
        )
    }

    /// True for a receiver-side event (`Rx*`). These are recorded before any
    /// hosting check, keyed by a contract id an inbound message names, so they
    /// create a per-contract entry only for a contract this node hosts; see
    /// [`record_received_event`].
    pub(crate) const fn is_received(self) -> bool {
        matches!(
            self,
            PropagationEvent::RxDedup
                | PropagationEvent::RxAlreadyApplied
                | PropagationEvent::RxBackoff
        )
    }

    /// The `WireStream*` event for a stream's completion signal: `None` when
    /// none came (the sender was dropped unsignalled, or the wait ran out).
    /// A cwnd-wait timeout ends that way, as does a stream orphaned at
    /// connection teardown.
    pub(crate) fn for_stream_outcome(outcome: Option<crate::transport::StreamSendOutcome>) -> Self {
        use crate::transport::StreamSendOutcome;
        match outcome {
            Some(StreamSendOutcome::Delivered) => PropagationEvent::WireStreamDelivered,
            Some(StreamSendOutcome::EgressAbandoned) => PropagationEvent::WireStreamEgressAbandoned,
            Some(StreamSendOutcome::Dropped) | None => PropagationEvent::WireStreamFailed,
        }
    }

    /// Stable field name, used in the INFO line and as the telemetry field
    /// suffix. Changing one breaks log and dashboard queries.
    pub(crate) const fn label(self) -> &'static str {
        match self {
            PropagationEvent::ClientOriginated => "client_originated",
            PropagationEvent::SentDelta => "sent_delta",
            PropagationEvent::SentFull => "sent_full",
            PropagationEvent::SkipSummaryEqual => "skip_summary_equal",
            PropagationEvent::SkipCachedConverged => "skip_cached_converged",
            PropagationEvent::SkipProbeConverged => "skip_probe_converged",
            PropagationEvent::SkipEmptyDelta => "skip_empty_delta",
            PropagationEvent::SkipNotHosted => "skip_not_hosted",
            PropagationEvent::SkipNoAddress => "skip_no_address",
            PropagationEvent::SkipSuperseded => "skip_superseded",
            PropagationEvent::SendFailed => "send_failed",
            PropagationEvent::FanoutEventDropped => "fanout_event_dropped",
            PropagationEvent::HealEventDropped => "heal_event_dropped",
            PropagationEvent::HealTarget => "heal_target",
            PropagationEvent::WireShort => "wire_short",
            PropagationEvent::WireStream => "wire_stream",
            PropagationEvent::WireUnobserved => "wire_unobserved",
            PropagationEvent::WireStreamDelivered => "wire_stream_delivered",
            PropagationEvent::WireStreamFailed => "wire_stream_failed",
            PropagationEvent::WireStreamEgressAbandoned => "wire_stream_egress_abandoned",
            PropagationEvent::WireSendFailed => "wire_send_failed",
            PropagationEvent::RxDedup => "rx_dedup",
            PropagationEvent::RxAlreadyApplied => "rx_already_applied",
            PropagationEvent::RxBackoff => "rx_backoff",
        }
    }
}

/// Per-event counts, indexed by [`PropagationEvent::index`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct EventCounts([u64; PropagationEvent::COUNT]);

impl EventCounts {
    pub(crate) fn get(&self, ev: PropagationEvent) -> u64 {
        // `index()` is an exhaustive match and `ALL`'s length sizes the
        // array; `propagation_events_are_densely_indexed` keeps them in step,
        // and `get` degrades to 0 rather than panicking if they drift.
        self.0.get(ev.index()).copied().unwrap_or(0)
    }

    fn add(&mut self, ev: PropagationEvent, n: u64) {
        if let Some(slot) = self.0.get_mut(ev.index()) {
            *slot = slot.saturating_add(n);
        }
    }

    fn merge(&mut self, other: &EventCounts) {
        for ev in PropagationEvent::ALL {
            self.add(ev, other.get(ev));
        }
    }

    fn is_empty(&self) -> bool {
        self.0.iter().all(|&n| n == 0)
    }

    /// Payloads handed to the network bridge.
    pub(crate) fn sent(&self) -> u64 {
        self.get(PropagationEvent::SentDelta)
            .saturating_add(self.get(PropagationEvent::SentFull))
    }
}

/// Per-contract windowed counters.
///
/// Plain `u64` fields, not atomics: every read and write happens while holding
/// the `DashMap` per-key write guard (`entry`/`get_mut`), so a `record` and a
/// `drain` for the same contract are mutually exclusive. That makes each
/// multi-field record and each multi-field drain atomic *as a unit* — a drain
/// can never observe a half-written record (e.g. `updates` incremented but
/// `targets_total` not yet), which a set of independent per-field atomics
/// could split across two windows. Different contracts still update fully in
/// parallel (separate `DashMap` shards), so the hot path stays lock-free
/// across contracts.
#[derive(Default)]
struct ContractCounters {
    /// Number of broadcast fan-out attempts for this contract in the window
    /// (one per `BroadcastStateChange` that reached a terminal outcome).
    updates: u64,
    /// Sum of resolved broadcast targets across those attempts. Divided by
    /// `updates` at emit time to produce `targets_avg`.
    targets_total: u64,
    /// Number of attempts that resolved zero targets (the NO_TARGETS branch:
    /// the update did not propagate further from this node).
    ///
    /// A fully-covered fan-out (#5147) also resolves zero targets but is a
    /// SUCCESS, and is counted in [`Self::fully_covered`] instead. Keeping the
    /// two apart is load-bearing: in the clique regime #5147 targets, a
    /// fully-covered fan-out is the *expected* outcome, so folding it in here
    /// would report a propagation failure to operators in direct proportion to
    /// how well the feature works.
    no_targets: u64,
    /// Number of attempts that sent nothing because every eligible peer had
    /// already been served by the originator (#5147). Zero targets, but a
    /// completed fan-out rather than a failure.
    fully_covered: u64,
    /// Sum of interest-manager peers that failed to resolve to a live
    /// connection across the window (the `interest_resolve_failed` counter
    /// from `BroadcastTargetResult`). A persistently non-zero value points at
    /// stale interest state.
    interest_resolve_failed: u64,
    /// Per-leg outcomes and the other [`PropagationEvent`]s (#6143).
    events: EventCounts,
}

impl ContractCounters {
    /// True when this entry recorded no activity for the window.
    fn is_empty(&self) -> bool {
        self.updates == 0
            && self.targets_total == 0
            && self.no_targets == 0
            && self.fully_covered == 0
            && self.interest_resolve_failed == 0
            && self.events.is_empty()
    }
}

/// A drained, immutable snapshot of one contract's window activity, used for
/// sorting and formatting. Plain values (no atomics) so the aggregation logic
/// is a pure, testable function.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ContractSummary {
    pub(crate) contract: ContractInstanceId,
    pub(crate) updates: u64,
    pub(crate) targets_total: u64,
    pub(crate) no_targets: u64,
    /// Attempts that sent nothing because the originator had already served
    /// every eligible peer (#5147). A success, deliberately NOT folded into
    /// `no_targets`.
    pub(crate) fully_covered: u64,
    pub(crate) interest_resolve_failed: u64,
    pub(crate) events: EventCounts,
}

impl ContractSummary {
    /// True when the contract did anything this window worth a line: a
    /// fan-out, or any [`PropagationEvent`]. A heal-only or receive-only
    /// contract has no `updates` but still counts. `interest_resolve_failed`
    /// alone does not, since it is only ever recorded alongside an update.
    fn is_active(&self) -> bool {
        self.updates > 0 || !self.events.is_empty()
    }

    /// Mean resolved targets per update attempt, rounded to one decimal.
    /// Returns 0.0 when there were no attempts (avoids divide-by-zero).
    fn targets_avg(&self) -> f64 {
        if self.updates == 0 {
            0.0
        } else {
            // Round to one decimal place for a compact, stable log field.
            let avg = self.targets_total as f64 / self.updates as f64;
            (avg * 10.0).round() / 10.0
        }
    }
}

/// Aggregated view of a single drained window, ready to log.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct WindowSummary {
    /// Distinct contracts with at least one update attempt this window.
    pub(crate) contracts: usize,
    /// Total update fan-out attempts across all contracts this window.
    pub(crate) updates: u64,
    /// Total resolved targets across all attempts this window.
    pub(crate) targets_total: u64,
    /// Total attempts that resolved zero targets this window.
    pub(crate) no_targets: u64,
    /// Total fully-covered fan-outs this window (#5147): zero targets because
    /// the originator already served everyone, which is a success.
    pub(crate) fully_covered: u64,
    /// Total interest-resolve failures across all contracts this window.
    pub(crate) interest_resolve_failed: u64,
    /// [`PropagationEvent`] totals across all contracts this window.
    pub(crate) events: EventCounts,
    /// The top-K contracts by update activity (descending), for per-contract
    /// lines. Length is at most [`TOP_K`].
    pub(crate) top: Vec<ContractSummary>,
    /// Number of additional active contracts beyond the top-K (the rollup
    /// count). Zero when `contracts <= TOP_K`.
    pub(crate) others: usize,
}

impl WindowSummary {
    /// Mean resolved targets per update attempt across the whole window.
    pub(crate) fn targets_avg(&self) -> f64 {
        if self.updates == 0 {
            0.0
        } else {
            let avg = self.targets_total as f64 / self.updates as f64;
            (avg * 10.0).round() / 10.0
        }
    }
}

/// Pure aggregation: turn a set of drained per-contract summaries into a
/// bounded [`WindowSummary`]. Selects the top-`top_k` contracts by update
/// count, then by bridge sends (ties broken by contract id for determinism)
/// and rolls the rest up into an `others` count. Inactive contracts (see
/// [`ContractSummary::is_active`]) are ignored.
///
/// Extracted from the draining/IO so it can be unit-tested without a runtime.
pub(crate) fn aggregate_window(
    mut summaries: Vec<ContractSummary>,
    top_k: usize,
) -> Option<WindowSummary> {
    summaries.retain(ContractSummary::is_active);
    if summaries.is_empty() {
        return None;
    }

    let contracts = summaries.len();
    let updates: u64 = summaries.iter().map(|s| s.updates).sum();
    let targets_total: u64 = summaries.iter().map(|s| s.targets_total).sum();
    let no_targets: u64 = summaries.iter().map(|s| s.no_targets).sum();
    let fully_covered: u64 = summaries.iter().map(|s| s.fully_covered).sum();
    let interest_resolve_failed: u64 = summaries.iter().map(|s| s.interest_resolve_failed).sum();
    let mut events = EventCounts::default();
    for s in &summaries {
        events.merge(&s.events);
    }

    // Most active first; deterministic tie-break on the contract's raw bytes
    // so a given window always produces the same top-K regardless of map
    // iteration order. `ContractInstanceId` does not implement `Ord`, so we
    // compare its stable public byte representation explicitly rather than
    // relying on deref-coercion to `[u8; N]::cmp`.
    summaries.sort_by(|a, b| {
        b.updates
            .cmp(&a.updates)
            .then_with(|| b.events.sent().cmp(&a.events.sent()))
            .then_with(|| a.contract.as_bytes().cmp(b.contract.as_bytes()))
    });

    let others = contracts.saturating_sub(top_k);
    summaries.truncate(top_k);

    Some(WindowSummary {
        contracts,
        updates,
        targets_total,
        no_targets,
        fully_covered,
        interest_resolve_failed,
        events,
        top: summaries,
        others,
    })
}

/// A contract that stalled (#6143): a local client update in window W-1,
/// every resolved leg skipped or refused by the bridge (or a fan-out dropped)
/// over W-1 and W, and nothing handed to the network bridge in either window.
/// See [`detect_stalls`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StallReport {
    pub(crate) contract: ContractInstanceId,
    /// Client updates in W-1.
    pub(crate) client_originated: u64,
    /// Targets resolved for this contract in W-1 and W.
    pub(crate) targets_total: u64,
    /// Every event for this contract in W-1 and W, so the WARN can say where
    /// the legs went.
    pub(crate) events: EventCounts,
}

/// Whether `contract` stalled, judged on its window W-1 summary (`previous`)
/// and its window W summary (`current`, `None` when it did nothing in W).
/// Pure so the rule can be unit-tested without a runtime.
fn stall_of(previous: &ContractSummary, current: Option<&ContractSummary>) -> Option<StallReport> {
    let client_originated = previous.events.get(PropagationEvent::ClientOriginated);
    if client_originated == 0 {
        return None;
    }
    let mut events = previous.events;
    let mut targets_total = previous.targets_total;
    if let Some(current) = current {
        events.merge(&current.events);
        targets_total = targets_total.saturating_add(current.targets_total);
    }
    // Positive evidence that the update never left: every resolved leg
    // ended unsent (skipped, or refused by the bridge), or a whole fan-out
    // was dropped before it resolved any. Resolved targets alone are not
    // evidence, and neither is one refused leg among them: a leg held while
    // its (contract, peer) pair is busy with a stream, or queued behind the
    // large lane's permits, records nothing for as long as it waits. The
    // legs are the fan-outs' resolved targets plus the heals'
    // (`heal_target`): a heal's leg can skip too, and must not be read as a
    // fan-out leg's outcome.
    let legs = targets_total.saturating_add(events.get(PropagationEvent::HealTarget));
    let ended_unsent = PropagationEvent::ALL
        .iter()
        .filter(|ev| ev.is_skip() || **ev == PropagationEvent::SendFailed)
        .fold(0u64, |n, &ev| n.saturating_add(events.get(ev)));
    let every_leg_ended_unsent = legs > 0 && ended_unsent >= legs;
    let fanout_dropped = events.get(PropagationEvent::FanoutEventDropped) > 0;
    ((every_leg_ended_unsent || fanout_dropped) && events.sent() == 0).then_some(StallReport {
        contract: previous.contract,
        client_originated,
        targets_total,
        events,
    })
}

/// The contracts that stalled at the end of window W, given W-1's summaries
/// of the contracts with a client update (`previous`) and W's summaries
/// (`current`). Sorted by contract id, at most one report per contract.
///
/// A contract stalls when, over W-1 and W together, it had:
/// - a client update in W-1;
/// - every resolved leg accounted for as ended unsent (the `skip_*` counts,
///   `skip_superseded` included, plus `send_failed` reach the legs
///   resolved: `targets_total` plus `heal_target`, above zero), or a
///   fan-out dropped by `try_notify` (`fanout_event_dropped`);
/// - nothing handed to the bridge (`sent_delta + sent_full == 0`).
///
/// W is grace for a leg queued near the end of W-1, and taking the two
/// windows together catches an update applied at the end of W-1 whose
/// fan-out is processed in W. A contract whose client keeps posting while
/// every leg is skipped is reported every window.
///
/// Legs with no outcome yet are not evidence: a leg held behind a busy
/// (contract, peer) pair or the large lane's permits records nothing while
/// it waits, which can outlast both windows. So one refused leg beside legs
/// still held is not a stall; every leg must be accounted for. Transport
/// outcomes (`wire_*`, including `wire_send_failed`) are deliberately not
/// read: a leg the bridge took is not a stall here, however the transport
/// then fares with it, so a slow stream delivered many windows later never
/// raises a WARN, and neither does a failed one. Zero resolved targets with
/// no dropped fan-out is the `no_targets` outcome, which
/// `handle_broadcast_state_change` already WARNs.
pub(crate) fn detect_stalls(
    previous: &HashMap<ContractInstanceId, ContractSummary>,
    current: &[ContractSummary],
) -> Vec<StallReport> {
    let current: HashMap<ContractInstanceId, &ContractSummary> =
        current.iter().map(|s| (s.contract, s)).collect();
    let mut reports: Vec<StallReport> = previous
        .values()
        .filter_map(|p| stall_of(p, current.get(&p.contract).copied()))
        .collect();
    reports.sort_by(|a, b| a.contract.as_bytes().cmp(b.contract.as_bytes()));
    reports
}

/// Bounded per-contract UPDATE-propagation counters with a periodic summary
/// emitter. Cheap to feed from the hot broadcast path (one `DashMap` shard
/// lookup + a few relaxed atomic adds); drained and logged once per window by
/// the background task started with [`Self::start_summary_task`].
pub(crate) struct UpdatePropagationStats {
    contracts: DashMap<ContractInstanceId, ContractCounters>,
    /// The previous window's summaries of the contracts that had a client
    /// update, for [`detect_stalls`]. At most [`MAX_TRACKED_CONTRACTS`]
    /// entries, since each was a tracked contract. Touched only by the drain,
    /// which runs on one task, so the lock is never contended.
    previous_client_windows: Mutex<HashMap<ContractInstanceId, ContractSummary>>,
}

impl UpdatePropagationStats {
    pub(crate) fn new() -> Self {
        Self {
            contracts: DashMap::new(),
            previous_client_windows: Mutex::new(HashMap::new()),
        }
    }

    /// The entry for `contract`, created if absent, or `None` when a new
    /// contract would pass [`MAX_TRACKED_CONTRACTS`].
    fn counters(
        &self,
        contract: ContractInstanceId,
    ) -> Option<dashmap::mapref::one::RefMut<'_, ContractInstanceId, ContractCounters>> {
        // Cap only blocks *new* contracts; updates to already-tracked
        // contracts always proceed. `len()` can briefly race other inserters,
        // but the cap is a soft memory guard (not a correctness boundary), so
        // a small overshoot is acceptable.
        if !self.contracts.contains_key(&contract) && self.contracts.len() >= MAX_TRACKED_CONTRACTS
        {
            return None;
        }
        Some(self.contracts.entry(contract).or_default())
    }

    /// Count one [`PropagationEvent`] for `contract`.
    ///
    /// Same per-key write guard and tracking cap as [`Self::record_broadcast`].
    /// Production code reaches this through [`record_propagation_event`], which
    /// also feeds the telemetry window.
    ///
    /// A receiver-side event ([`PropagationEvent::is_received`]) recorded
    /// here never creates an entry; production records those through
    /// [`record_received_event`], which says whether this node hosts the
    /// contract.
    pub(crate) fn record_event(&self, contract: ContractInstanceId, ev: PropagationEvent) {
        if ev.is_received() {
            self.record_received(contract, ev, false);
        } else if let Some(mut entry) = self.counters(contract) {
            entry.events.add(ev, 1);
        }
    }

    /// Count a receiver-side event, creating an entry for `contract` only
    /// when this node hosts it (`hosted`), and otherwise only counting
    /// against an entry already tracked this window. Receiver events are
    /// recorded before any hosting check, keyed by whatever contract id an
    /// inbound broadcast names, so allocating for any id would let a peer
    /// sending broadcasts for random keys fill the [`MAX_TRACKED_CONTRACTS`]
    /// map and lock this node's own contracts out of it for the window.
    pub(crate) fn record_received(
        &self,
        contract: ContractInstanceId,
        ev: PropagationEvent,
        hosted: bool,
    ) {
        let entry = if hosted {
            self.counters(contract)
        } else {
            self.contracts.get_mut(&contract)
        };
        if let Some(mut entry) = entry {
            entry.events.add(ev, 1);
        }
    }

    /// Non-draining read of one contract's counters, for tests in other
    /// modules that drive the real production call sites.
    #[cfg(test)]
    pub(crate) fn events_for_test(&self, contract: &ContractInstanceId) -> EventCounts {
        self.contracts
            .get(contract)
            .map(|c| c.events)
            .unwrap_or_default()
    }

    /// Drain one window and run the stall check on it, without logging.
    #[cfg(test)]
    fn drain_stalls_for_test(&self) -> Vec<StallReport> {
        let drained = self.drain_window();
        self.check_stalls(&drained)
    }

    /// Record the terminal outcome of one logical broadcast for `contract`.
    ///
    /// * `targets` — number of resolved broadcast targets (0 ⇒ NO_TARGETS).
    /// * `interest_resolve_failed` — interest-manager peers that failed to
    ///   resolve to a live connection on this attempt.
    ///
    /// All four counter fields are mutated together while holding the
    /// `DashMap` per-key write guard, so the whole record is atomic against a
    /// concurrent [`Self::drain_window`] of the same contract — a drain can
    /// never capture a half-applied record. Different contracts live on
    /// different shards and still update in parallel.
    ///
    /// When a brand-new contract would push the map past
    /// [`MAX_TRACKED_CONTRACTS`], the record is dropped (uncounted) rather
    /// than growing the map — existing tracked contracts are unaffected.
    pub(crate) fn record_broadcast(
        &self,
        contract: ContractInstanceId,
        targets: usize,
        interest_resolve_failed: usize,
    ) {
        let Some(mut entry) = self.counters(contract) else {
            return;
        };
        entry.updates += 1;
        entry.targets_total += targets as u64;
        if targets == 0 {
            entry.no_targets += 1;
        }
        entry.interest_resolve_failed += interest_resolve_failed as u64;
    }

    /// Record a fan-out that sent nothing because the originator had already
    /// served every eligible peer (#5147).
    ///
    /// Deliberately a SEPARATE entry point from [`Self::record_broadcast`]
    /// rather than a `targets == 0` call into it. That call would land in the
    /// `no_targets` bucket, which is the operator-facing propagation-FAILURE
    /// counter — and in the clique regime #5147 targets, a fully-covered
    /// fan-out is the expected outcome, so the summary would report a failure
    /// in direct proportion to how well the feature works. Counting it here
    /// keeps the outcome attributed by the code that decided it, per the
    /// "metric describing a filtering decision" rule in
    /// `.claude/rules/bug-prevention-patterns.md`.
    ///
    /// `updates` is still incremented so the #4281 liveness summary sees one
    /// broadcast per apply, and `targets_total` is not, so `targets_avg`
    /// honestly reflects that no peer was sent to.
    pub(crate) fn record_fully_covered_broadcast(
        &self,
        contract: ContractInstanceId,
        interest_resolve_failed: usize,
    ) {
        let Some(mut entry) = self.counters(contract) else {
            return;
        };
        entry.updates += 1;
        entry.fully_covered += 1;
        entry.interest_resolve_failed += interest_resolve_failed as u64;
    }

    /// Snapshot every tracked contract's counters into per-contract window
    /// summaries, then clear the map.
    ///
    /// Uses [`DashMap::retain`], which holds each shard's write guard while it
    /// visits that shard's entries. Because [`Self::record_broadcast`] mutates
    /// under the same per-key guard, a contract's full multi-field record is
    /// captured atomically here — a drain can never split one record across
    /// two windows (Codex review P2). The closure always returns `false`, so
    /// **every** drained entry is removed: the snapshot fully captures the
    /// window's activity, and clearing the map means the next window starts
    /// with full [`MAX_TRACKED_CONTRACTS`] headroom instead of staying
    /// saturated with the previous window's one-off contracts (Codex review
    /// P2 — the cap blind-spot under key churn). Any `record_broadcast` that
    /// runs after a shard is visited simply re-creates the entry and is
    /// counted in the next window.
    fn drain_window(&self) -> Vec<ContractSummary> {
        let mut out = Vec::with_capacity(self.contracts.len());
        self.contracts.retain(|id, counters| {
            if !counters.is_empty() {
                out.push(ContractSummary {
                    contract: *id,
                    updates: counters.updates,
                    targets_total: counters.targets_total,
                    no_targets: counters.no_targets,
                    fully_covered: counters.fully_covered,
                    interest_resolve_failed: counters.interest_resolve_failed,
                    events: counters.events,
                });
            }
            // Remove every entry — clears the map so the cap resets each window.
            false
        });
        out
    }

    /// Run [`detect_stalls`] on the window just drained against the one
    /// before it, then keep this window's client-updated contracts for the
    /// next check.
    fn check_stalls(&self, drained: &[ContractSummary]) -> Vec<StallReport> {
        let mut previous = self.previous_client_windows.lock();
        let stalls = detect_stalls(&previous, drained);
        *previous = drained
            .iter()
            .filter(|s| s.events.get(PropagationEvent::ClientOriginated) > 0)
            .map(|s| (s.contract, s.clone()))
            .collect();
        stalls
    }

    /// Drain the current window, emit it at INFO, and WARN for any contract
    /// whose local client updates stalled (see [`detect_stalls`]). Returns
    /// the number of stalled contracts, for the telemetry window. That
    /// includes any left out of the per-contract WARNs by
    /// [`MAX_STALL_WARNS_PER_WINDOW`] and reported only in the rollup, so it
    /// can exceed the number of WARN lines.
    ///
    /// Public for the periodic task and for deterministic unit tests (which
    /// call it directly after recording, instead of waiting on the real-time
    /// ticker).
    pub(crate) fn drain_and_log(&self) -> u64 {
        let drained = self.drain_window();
        let stalls = self.check_stalls(&drained);
        log_stalls(&stalls);

        let Some(summary) = aggregate_window(drained, TOP_K) else {
            // Nothing happened this window — stay silent (budget rule).
            return stalls.len() as u64;
        };

        let e = &summary.events;
        tracing::info!(
            contracts = summary.contracts,
            updates = summary.updates,
            targets_avg = summary.targets_avg(),
            no_targets = summary.no_targets,
            fully_covered = summary.fully_covered,
            interest_resolve_failed = summary.interest_resolve_failed,
            client_originated = e.get(PropagationEvent::ClientOriginated),
            sent_delta = e.get(PropagationEvent::SentDelta),
            sent_full = e.get(PropagationEvent::SentFull),
            skip_summary_equal = e.get(PropagationEvent::SkipSummaryEqual),
            skip_cached_converged = e.get(PropagationEvent::SkipCachedConverged),
            skip_probe_converged = e.get(PropagationEvent::SkipProbeConverged),
            skip_empty_delta = e.get(PropagationEvent::SkipEmptyDelta),
            skip_not_hosted = e.get(PropagationEvent::SkipNotHosted),
            skip_no_address = e.get(PropagationEvent::SkipNoAddress),
            skip_superseded = e.get(PropagationEvent::SkipSuperseded),
            send_failed = e.get(PropagationEvent::SendFailed),
            wire_short = e.get(PropagationEvent::WireShort),
            wire_stream = e.get(PropagationEvent::WireStream),
            wire_unobserved = e.get(PropagationEvent::WireUnobserved),
            wire_stream_delivered = e.get(PropagationEvent::WireStreamDelivered),
            wire_stream_failed = e.get(PropagationEvent::WireStreamFailed),
            wire_stream_egress_abandoned = e.get(PropagationEvent::WireStreamEgressAbandoned),
            wire_send_failed = e.get(PropagationEvent::WireSendFailed),
            fanout_event_dropped = e.get(PropagationEvent::FanoutEventDropped),
            heal_event_dropped = e.get(PropagationEvent::HealEventDropped),
            heal_target = e.get(PropagationEvent::HealTarget),
            rx_dedup = e.get(PropagationEvent::RxDedup),
            rx_already_applied = e.get(PropagationEvent::RxAlreadyApplied),
            rx_backoff = e.get(PropagationEvent::RxBackoff),
            window_s = SUMMARY_WINDOW.as_secs(),
            phase = "summary",
            "update_propagation_summary"
        );

        for c in &summary.top {
            let e = &c.events;
            tracing::info!(
                contract = %format!("{:.8}", c.contract),
                updates = c.updates,
                targets_avg = c.targets_avg(),
                no_targets = c.no_targets,
                fully_covered = c.fully_covered,
                interest_resolve_failed = c.interest_resolve_failed,
                client_originated = e.get(PropagationEvent::ClientOriginated),
                sent_delta = e.get(PropagationEvent::SentDelta),
                sent_full = e.get(PropagationEvent::SentFull),
                skip_summary_equal = e.get(PropagationEvent::SkipSummaryEqual),
                skip_cached_converged = e.get(PropagationEvent::SkipCachedConverged),
                skip_probe_converged = e.get(PropagationEvent::SkipProbeConverged),
                skip_empty_delta = e.get(PropagationEvent::SkipEmptyDelta),
                skip_not_hosted = e.get(PropagationEvent::SkipNotHosted),
                skip_no_address = e.get(PropagationEvent::SkipNoAddress),
                skip_superseded = e.get(PropagationEvent::SkipSuperseded),
                send_failed = e.get(PropagationEvent::SendFailed),
                wire_short = e.get(PropagationEvent::WireShort),
                wire_stream = e.get(PropagationEvent::WireStream),
                wire_unobserved = e.get(PropagationEvent::WireUnobserved),
                wire_stream_delivered = e.get(PropagationEvent::WireStreamDelivered),
                wire_stream_failed = e.get(PropagationEvent::WireStreamFailed),
                wire_stream_egress_abandoned = e.get(PropagationEvent::WireStreamEgressAbandoned),
                wire_send_failed = e.get(PropagationEvent::WireSendFailed),
                fanout_event_dropped = e.get(PropagationEvent::FanoutEventDropped),
                heal_event_dropped = e.get(PropagationEvent::HealEventDropped),
                heal_target = e.get(PropagationEvent::HealTarget),
                rx_dedup = e.get(PropagationEvent::RxDedup),
                rx_already_applied = e.get(PropagationEvent::RxAlreadyApplied),
                rx_backoff = e.get(PropagationEvent::RxBackoff),
                phase = "summary_contract",
                "update_propagation_summary contract detail"
            );
        }

        if summary.others > 0 {
            tracing::info!(
                others = summary.others,
                phase = "summary_rollup",
                "update_propagation_summary …and {} other contract(s)",
                summary.others
            );
        }
        stalls.len() as u64
    }

    /// Spawn the background summary task and return its `JoinHandle` so the
    /// caller can register it with the node's `BackgroundTaskMonitor` (this is
    /// a lifetime task, not fire-and-forget — see `code-style.md`).
    ///
    /// Drains and logs once per [`SUMMARY_WINDOW`]. Scheduling uses real time
    /// (`tokio::time::interval`) like the other periodic tasks
    /// (`InterestManager::start_sweep_task`, `OrphanStreamRegistry::start_gc_task`);
    /// the counters themselves carry no timestamps, so this is
    /// deterministic-test-friendly without a `TimeSource` — tests call
    /// [`Self::drain_and_log`] directly.
    ///
    /// `on_stalls` receives each window's count of stalled contracts (not of
    /// WARN lines: the rolled-up ones are included). The node passes it to
    /// the `broadcast_payload_mix` telemetry window, so the event carries it
    /// next to the counts that explain it.
    pub(crate) fn start_summary_task(
        self: std::sync::Arc<Self>,
        on_stalls: impl Fn(u64) + Send + 'static,
    ) -> tokio::task::JoinHandle<()> {
        GlobalExecutor::spawn(async move {
            // No startup jitter on purpose. The sibling periodic tasks draw
            // `GlobalRng` for jitter to de-correlate *network* side-effects
            // across peers, but this task only writes to the local log, so
            // de-correlation buys nothing — and an extra `GlobalRng` draw at
            // node startup would perturb the deterministic simulation RNG
            // stream and flip seed-sensitive routing-coverage sim tests
            // (e.g. `test_get_routing_coverage_low_htl`). Keeping the task
            // RNG-free preserves simulation determinism.
            let mut ticker = tokio::time::interval(SUMMARY_WINDOW);
            // Not the default `Burst`: after a runtime stall or a suspend,
            // `Burst` fires the missed ticks back to back, so the windows
            // after the first are milliseconds long and the stall check's
            // one-window grace (`detect_stalls`) would be no grace at all.
            // `Delay` fires once for the missed span and then restarts the
            // period, so every window after it is a full one.
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // The first tick fires immediately; skip it so the first emit
            // describes a full window rather than an empty startup instant.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                on_stalls(self.drain_and_log());
            }
        })
    }
}

/// WARN for each stalled contract, at most [`MAX_STALL_WARNS_PER_WINDOW`] of
/// them, then one WARN with the number left out. Already at most one per
/// contract per window, and only for contracts with local client updates.
fn log_stalls(stalls: &[StallReport]) {
    for stall in stalls.iter().take(MAX_STALL_WARNS_PER_WINDOW) {
        log_stall(stall);
    }
    let left_out = stalls.len().saturating_sub(MAX_STALL_WARNS_PER_WINDOW);
    if left_out > 0 {
        tracing::warn!(
            stalled_contracts = stalls.len(),
            left_out,
            phase = "origination_stall_rollup",
            "update_propagation_stalled: {left_out} more contract(s) stalled this window \
             (#6143)"
        );
    }
}

fn log_stall(stall: &StallReport) {
    let e = &stall.events;
    tracing::warn!(
        contract = %stall.contract,
        client_originated = stall.client_originated,
        targets_total = stall.targets_total,
        skip_summary_equal = e.get(PropagationEvent::SkipSummaryEqual),
        skip_cached_converged = e.get(PropagationEvent::SkipCachedConverged),
        skip_probe_converged = e.get(PropagationEvent::SkipProbeConverged),
        skip_empty_delta = e.get(PropagationEvent::SkipEmptyDelta),
        skip_not_hosted = e.get(PropagationEvent::SkipNotHosted),
        skip_no_address = e.get(PropagationEvent::SkipNoAddress),
        skip_superseded = e.get(PropagationEvent::SkipSuperseded),
        send_failed = e.get(PropagationEvent::SendFailed),
        heal_target = e.get(PropagationEvent::HealTarget),
        fanout_event_dropped = e.get(PropagationEvent::FanoutEventDropped),
        window_s = SUMMARY_WINDOW.as_secs(),
        phase = "origination_stall",
        "update_propagation_stalled: every broadcast leg of a local client update \
         was skipped or refused, or its fan-out was dropped, and nothing was handed \
         to the network for the contract in that window or the next (#6143)"
    );
}

/// The two places a [`PropagationEvent`] is counted, held for code that has
/// no [`crate::node::OpManager`] at hand: the per-connection send task, which
/// sees how the transport carried each UPDATE broadcast.
#[derive(Clone)]
pub(crate) struct PropagationSink {
    stats: std::sync::Arc<UpdatePropagationStats>,
    mix: std::sync::Arc<crate::node::PayloadMix>,
}

impl PropagationSink {
    pub(crate) fn of(op_manager: &crate::node::OpManager) -> Self {
        Self {
            stats: op_manager.update_propagation_stats.clone(),
            mix: op_manager.payload_mix.clone(),
        }
    }

    /// Same as [`record_propagation_event`].
    pub(crate) fn record(&self, contract: &ContractInstanceId, ev: PropagationEvent) {
        self.stats.record_event(*contract, ev);
        self.mix.record_propagation_event(ev);
    }

    /// Same as [`record_propagation_stream_fate`].
    pub(crate) fn record_stream_fate(
        &self,
        contract: &ContractInstanceId,
        outcome: Option<crate::transport::StreamSendOutcome>,
    ) -> PropagationEvent {
        let ev = PropagationEvent::for_stream_outcome(outcome);
        self.record(contract, ev);
        ev
    }

    /// A sink of its own, for tests, with the stats and the telemetry window
    /// it records into.
    #[cfg(test)]
    pub(crate) fn for_test() -> (
        Self,
        std::sync::Arc<UpdatePropagationStats>,
        std::sync::Arc<crate::node::PayloadMix>,
    ) {
        let stats = std::sync::Arc::new(UpdatePropagationStats::new());
        let mix = std::sync::Arc::new(crate::node::PayloadMix::new());
        let sink = Self {
            stats: stats.clone(),
            mix: mix.clone(),
        };
        (sink, stats, mix)
    }
}

/// Count one [`PropagationEvent`] in BOTH the per-contract INFO summary and
/// the node-wide `broadcast_payload_mix` telemetry window.
///
/// The single entry point production code uses, so the two agree on every
/// event, with two exceptions where the node-wide telemetry counts an event
/// the per-contract summary leaves out: any event for a contract new to the
/// window once [`MAX_TRACKED_CONTRACTS`] are tracked, and a receiver-side
/// `rx_*` event for a contract this node does not host and has no entry for
/// in the window (see [`record_received_event`]).
pub(crate) fn record_propagation_event(
    op_manager: &crate::node::OpManager,
    contract: &ContractInstanceId,
    ev: PropagationEvent,
) {
    op_manager
        .update_propagation_stats
        .record_event(*contract, ev);
    op_manager.payload_mix.record_propagation_event(ev);
}

/// Count a receiver-side `rx_*` event like [`record_propagation_event`],
/// except that the per-contract summary creates an entry for `key` only when
/// this node hosts it (see [`UpdatePropagationStats::record_received`]).
/// Telemetry counts every one.
///
/// The gate is `is_hosting_contract`, narrower than the send side's
/// `should_broadcast_contract` (hosted or in use, and state on disk). That
/// one reads the state store, too costly for every inbound broadcast. So a
/// contract that is in use but not hosted gets its `rx_*` counts only once
/// something else (a client update, a fan-out) has created its entry in the
/// window; telemetry counts them regardless.
pub(crate) fn record_received_event(
    op_manager: &crate::node::OpManager,
    key: &freenet_stdlib::prelude::ContractKey,
    ev: PropagationEvent,
) {
    let hosted = op_manager.ring.is_hosting_contract(key);
    op_manager
        .update_propagation_stats
        .record_received(*key.id(), ev, hosted);
    op_manager.payload_mix.record_propagation_event(ev);
}

/// Count the fate of an UPDATE broadcast stream from its completion signal
/// (`None` when none came), like [`record_propagation_event`]. Returns the
/// event counted.
pub(crate) fn record_propagation_stream_fate(
    op_manager: &crate::node::OpManager,
    contract: &ContractInstanceId,
    outcome: Option<crate::transport::StreamSendOutcome>,
) -> PropagationEvent {
    let ev = PropagationEvent::for_stream_outcome(outcome);
    record_propagation_event(op_manager, contract, ev);
    ev
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a deterministic, distinct `ContractInstanceId` from a seed byte.
    fn cid(seed: u8) -> ContractInstanceId {
        ContractInstanceId::new([seed; 32])
    }

    #[test]
    fn aggregate_window_empty_returns_none() {
        assert!(aggregate_window(vec![], TOP_K).is_none());
    }

    #[test]
    fn aggregate_window_ignores_zero_update_contracts() {
        // A tracked contract with no update attempts this window contributes
        // nothing and must not inflate the contract count.
        let summaries = vec![ContractSummary {
            contract: cid(1),
            updates: 0,
            targets_total: 0,
            no_targets: 0,
            fully_covered: 0,
            interest_resolve_failed: 5,
            events: EventCounts::default(),
        }];
        assert!(aggregate_window(summaries, TOP_K).is_none());
    }

    #[test]
    fn aggregate_window_totals_and_average() {
        let summaries = vec![
            ContractSummary {
                contract: cid(1),
                updates: 2,
                targets_total: 10, // avg 5
                no_targets: 0,
                fully_covered: 0,
                interest_resolve_failed: 1,
                events: EventCounts::default(),
            },
            ContractSummary {
                contract: cid(2),
                updates: 3,
                targets_total: 0, // all no-target attempts
                no_targets: 3,
                fully_covered: 0,
                interest_resolve_failed: 4,
                events: EventCounts::default(),
            },
        ];
        let w = aggregate_window(summaries, TOP_K).expect("non-empty");
        assert_eq!(w.contracts, 2);
        assert_eq!(w.updates, 5);
        assert_eq!(w.targets_total, 10);
        assert_eq!(w.no_targets, 3);
        assert_eq!(w.interest_resolve_failed, 5);
        // Window-wide average: 10 targets / 5 updates = 2.0
        assert_eq!(w.targets_avg(), 2.0);
        assert_eq!(w.others, 0);
        // Most active contract (cid(2), 3 updates) sorts first.
        assert_eq!(w.top[0].contract, cid(2));
        assert_eq!(w.top[0].targets_avg(), 0.0);
        assert_eq!(w.top[1].contract, cid(1));
        assert_eq!(w.top[1].targets_avg(), 5.0);
    }

    #[test]
    fn aggregate_window_caps_top_k_and_counts_others() {
        // 5 active contracts, top_k = 2 ⇒ 2 detail lines + others = 3.
        let summaries: Vec<ContractSummary> = (0..5u8)
            .map(|i| ContractSummary {
                contract: cid(i),
                updates: u64::from(i) + 1, // 1,2,3,4,5
                targets_total: 0,
                no_targets: 0,
                fully_covered: 0,
                interest_resolve_failed: 0,
                events: EventCounts::default(),
            })
            .collect();
        let w = aggregate_window(summaries, 2).expect("non-empty");
        assert_eq!(w.contracts, 5);
        assert_eq!(w.top.len(), 2);
        assert_eq!(w.others, 3);
        // Highest update counts first: cid(4)=5 updates, cid(3)=4 updates.
        assert_eq!(w.top[0].contract, cid(4));
        assert_eq!(w.top[1].contract, cid(3));
    }

    #[test]
    fn aggregate_window_tie_break_is_deterministic_by_contract_id() {
        // Equal update counts ⇒ tie broken by contract id ascending, so the
        // top-K is stable across map iteration orders.
        let summaries = vec![
            ContractSummary {
                contract: cid(9),
                updates: 1,
                targets_total: 0,
                no_targets: 0,
                fully_covered: 0,
                interest_resolve_failed: 0,
                events: EventCounts::default(),
            },
            ContractSummary {
                contract: cid(3),
                updates: 1,
                targets_total: 0,
                no_targets: 0,
                fully_covered: 0,
                interest_resolve_failed: 0,
                events: EventCounts::default(),
            },
        ];
        let w = aggregate_window(summaries, 1).expect("non-empty");
        assert_eq!(w.top.len(), 1);
        assert_eq!(w.others, 1);
        // Lower id wins the tie.
        assert_eq!(w.top[0].contract, cid(3));
    }

    #[test]
    fn record_and_drain_accumulates_then_resets() {
        let stats = UpdatePropagationStats::new();
        let c = cid(7);

        // Two fan-outs: one with 3 targets, one with 0 (NO_TARGETS) + 2
        // interest-resolve failures.
        stats.record_broadcast(c, 3, 0);
        stats.record_broadcast(c, 0, 2);

        let drained = stats.drain_window();
        assert_eq!(drained.len(), 1);
        let s = &drained[0];
        assert_eq!(s.contract, c);
        assert_eq!(s.updates, 2);
        assert_eq!(s.targets_total, 3);
        assert_eq!(s.no_targets, 1);
        assert_eq!(s.interest_resolve_failed, 2);

        // The drained entry is removed in the same drain (not retained for a
        // window) so the next window starts with full cap headroom.
        assert_eq!(stats.contracts.len(), 0);

        // A second drain with no new activity yields nothing.
        let drained2 = stats.drain_window();
        assert!(drained2.is_empty());
        assert_eq!(stats.contracts.len(), 0);
    }

    #[test]
    fn drain_restores_cap_headroom_for_next_window() {
        // Codex P2 regression: a window that fills the cap with one-off
        // contracts must NOT block new contracts in the following window.
        let stats = UpdatePropagationStats::new();
        let id_at = |i: usize| {
            let mut bytes = [0u8; 32];
            bytes[..8].copy_from_slice(&(i as u64).to_le_bytes());
            ContractInstanceId::new(bytes)
        };

        // Window 1: fill to capacity, then drain.
        for i in 0..MAX_TRACKED_CONTRACTS {
            stats.record_broadcast(id_at(i), 1, 0);
        }
        assert_eq!(stats.contracts.len(), MAX_TRACKED_CONTRACTS);
        let drained = stats.drain_window();
        assert_eq!(drained.len(), MAX_TRACKED_CONTRACTS);
        assert_eq!(
            stats.contracts.len(),
            0,
            "drain must clear the map so the cap doesn't stay saturated"
        );

        // Window 2: a fresh set of contracts is accepted (not dropped).
        let fresh = id_at(MAX_TRACKED_CONTRACTS + 1);
        stats.record_broadcast(fresh, 2, 0);
        assert!(
            stats.contracts.get(&fresh).is_some(),
            "new contract must be tracked in the window after a full drain"
        );
        let drained2 = stats.drain_window();
        assert_eq!(drained2.len(), 1);
        assert_eq!(drained2[0].contract, fresh);
        assert_eq!(drained2[0].targets_total, 2);
    }

    #[test]
    fn record_broadcast_respects_tracking_cap() {
        let stats = UpdatePropagationStats::new();
        // Fill to capacity with distinct contracts.
        for i in 0..MAX_TRACKED_CONTRACTS {
            // Spread the seed across all 32 bytes so ids stay distinct past 255.
            let mut bytes = [0u8; 32];
            bytes[..8].copy_from_slice(&(i as u64).to_le_bytes());
            stats.record_broadcast(ContractInstanceId::new(bytes), 1, 0);
        }
        assert_eq!(stats.contracts.len(), MAX_TRACKED_CONTRACTS);

        // A brand-new contract is dropped (uncounted) at capacity.
        let overflow = {
            let mut bytes = [0u8; 32];
            bytes[0] = 0xFF;
            bytes[8] = 0xFF; // guaranteed distinct from the seeded range
            ContractInstanceId::new(bytes)
        };
        stats.record_broadcast(overflow, 1, 0);
        assert_eq!(stats.contracts.len(), MAX_TRACKED_CONTRACTS);
        assert!(stats.contracts.get(&overflow).is_none());
    }

    #[test]
    fn concurrent_record_and_drain_conserve_totals_without_field_split() {
        // Codex P2 regression: a record and a drain of the same contract must
        // be atomic as a unit. With independent per-field atomics a drain
        // could capture `updates` but miss `targets_total` (or vice-versa),
        // splitting one record across windows and dropping data. Here many
        // threads hammer record_broadcast on a single contract while another
        // thread drains repeatedly; afterwards the grand totals across all
        // drains must exactly equal what was recorded, and no drained summary
        // may ever show targets/interest activity with `updates == 0` (the
        // tell-tale of a split record).
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

        const THREADS: usize = 8;
        const PER_THREAD: u64 = 2_000;
        const TARGETS_EACH: usize = 2;
        const IRF_EACH: usize = 1;

        let stats = Arc::new(UpdatePropagationStats::new());
        let done = Arc::new(AtomicBool::new(false));
        let c = cid(123);

        let acc = std::thread::scope(|scope| {
            // Concurrent drainer: accumulates across windows and asserts no split.
            let drainer = {
                let stats = Arc::clone(&stats);
                let done = Arc::clone(&done);
                scope.spawn(move || {
                    let mut acc = (0u64, 0u64, 0u64, 0u64);
                    let mut drain_once = || {
                        for s in stats.drain_window() {
                            assert!(
                                s.updates > 0,
                                "field-split: drained summary has updates==0 with \
                                 non-zero other fields: {s:?}"
                            );
                            acc.0 += s.updates;
                            acc.1 += s.targets_total;
                            acc.2 += s.no_targets;
                            acc.3 += s.interest_resolve_failed;
                        }
                    };
                    while !done.load(AtomicOrdering::Relaxed) {
                        drain_once();
                        std::thread::yield_now();
                    }
                    // Final drain captures anything recorded after the last loop.
                    drain_once();
                    acc
                })
            };

            // Recorders.
            let mut recorders = Vec::new();
            for _ in 0..THREADS {
                let stats = Arc::clone(&stats);
                recorders.push(scope.spawn(move || {
                    for _ in 0..PER_THREAD {
                        stats.record_broadcast(c, TARGETS_EACH, IRF_EACH);
                    }
                }));
            }
            for r in recorders {
                r.join().unwrap();
            }
            done.store(true, AtomicOrdering::Relaxed);
            drainer.join().unwrap()
        });

        let expected = THREADS as u64 * PER_THREAD;
        assert_eq!(
            acc.0, expected,
            "every record must be counted exactly once across all windows"
        );
        assert_eq!(acc.1, expected * TARGETS_EACH as u64);
        assert_eq!(acc.2, 0);
        assert_eq!(acc.3, expected * IRF_EACH as u64);
    }

    #[test]
    fn drain_and_log_emits_info_summary_and_is_silent_when_idle() {
        use crate::test_utils::TestLogger;

        let logger = TestLogger::new().capture_logs().with_level("info").init();

        // Idle window: no records ⇒ no output at all (budget rule).
        let stats = UpdatePropagationStats::new();
        stats.drain_and_log();
        assert!(
            !logger.contains("update_propagation_summary"),
            "an idle window must emit nothing, got: {:?}",
            logger.logs()
        );

        // One contract, two fan-outs (5 + 3 targets) ⇒ avg 4.0, no NO_TARGETS.
        stats.record_broadcast(cid(1), 5, 0);
        stats.record_broadcast(cid(1), 3, 1);
        stats.drain_and_log();

        assert!(
            logger.contains("update_propagation_summary"),
            "active window must emit the summary, got: {:?}",
            logger.logs()
        );
        assert!(
            logger.contains("INFO"),
            "summary must be INFO-level, got: {:?}",
            logger.logs()
        );

        // Draining again with no new activity is silent and clears state.
        let before = logger.logs().len();
        stats.drain_and_log();
        let after_logs = logger.logs();
        assert!(
            after_logs.len() == before
                || !after_logs[before..]
                    .iter()
                    .any(|l| l.contains("update_propagation_summary")),
            "a post-drain idle window must not re-emit a summary, got: {:?}",
            &after_logs[before..]
        );
        assert_eq!(stats.contracts.len(), 0, "idle entries should be evicted");
    }

    /// Source-level pin: the summary emitter MUST stay at INFO. The whole
    /// point of #4281 is to restore an INFO liveness signal lost to #4272;
    /// a future refactor that demotes this to DEBUG silently re-opens the
    /// visibility gap. Anchored on the structured event name.
    #[test]
    fn summary_emitter_logs_at_info_pin_test() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/operations/update/propagation_stats.rs");
        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("must read own source at {}: {e}", path.display()));
        // The drain_and_log emitter is the only `phase = "summary"` site.
        let needle = "phase = \"summary\",";
        let idx = source
            .find(needle)
            .expect("summary emitter must still exist in source");
        let preceding = &source[..idx];
        let macro_idx = preceding
            .rfind("tracing::")
            .expect("a tracing macro must precede the summary emitter");
        let after_macro = &preceding[macro_idx + "tracing::".len()..];
        let macro_name = after_macro.split('!').next().unwrap_or("");
        assert_eq!(
            macro_name, "info",
            "update_propagation_summary emitter must stay at INFO — it is the \
             liveness signal #4281 restored after the #4272 DEBUG demotions. \
             Closest preceding macro is `tracing::{macro_name}!`."
        );
    }

    /// A fully-covered fan-out (#5147) must NOT read as a propagation failure.
    ///
    /// Regression test for the shipped-comment-vs-code split described on
    /// `broadcast_path_feeds_propagation_stats_pin_test`: the fully-covered arm
    /// originally called `record_broadcast(key, 0, …)` beneath a comment saying
    /// it avoided the `no_targets` counter, while `record_broadcast`
    /// unconditionally increments `no_targets` whenever `targets == 0`.
    ///
    /// The mutation that must make this red is exactly that regression —
    /// replace the `record_fully_covered_broadcast` call with
    /// `record_broadcast(c, 0, 0)` and `no_targets` becomes 1.
    ///
    /// `updates` is still asserted, because the opposite over-correction (not
    /// recording the outcome at all) would silently lose the #4281 liveness
    /// signal for every fan-out the feature suppresses — which, in the regime
    /// this design targets, is most of them.
    #[test]
    fn a_fully_covered_fanout_is_not_counted_as_a_propagation_failure() {
        let stats = UpdatePropagationStats::new();
        let c = cid(7);

        stats.record_fully_covered_broadcast(c, 0);

        let summaries = stats.drain_window();
        assert_eq!(summaries.len(), 1, "the outcome must be recorded at all");
        let s = &summaries[0];

        assert_eq!(
            s.no_targets, 0,
            "a fully-covered fan-out is a SUCCESS and must never land in \
             `no_targets`, the operator-facing propagation-failure counter — \
             otherwise the summary reports a failure in direct proportion to \
             how well #5147 is working"
        );
        assert_eq!(
            s.fully_covered, 1,
            "the outcome must be attributed to the counter that names it"
        );
        assert_eq!(
            s.updates, 1,
            "the #4281 liveness summary must still see one broadcast per apply"
        );
        assert_eq!(
            s.targets_total, 0,
            "no peer was sent to, so `targets_avg` must reflect zero rather \
             than inflating the mean"
        );

        // The counterfactual, asserted rather than described: the call the bug
        // used produces the false alarm. If a future edit makes
        // `record_broadcast(c, 0, _)` stop counting `no_targets`, this arm goes
        // red and the two entry points need re-examining.
        let regressed = UpdatePropagationStats::new();
        regressed.record_broadcast(c, 0, 0);
        let regressed_summaries = regressed.drain_window();
        assert_eq!(
            regressed_summaries[0].no_targets, 1,
            "`record_broadcast` with zero targets is the propagation-FAILURE \
             path — that is precisely why the fully-covered arm must not use it"
        );
    }

    /// Source-scrape pin for the broadcast-path wiring. The counters are only
    /// useful if the fan-out handler actually feeds them; a future task-per-tx
    /// migration or refactor of `handle_broadcast_state_change` could silently
    /// drop the `record_broadcast` calls (the exact "manually-mirrored
    /// telemetry counter rots after a migration" failure class called out in
    /// `.claude/rules/bug-prevention-patterns.md`).
    ///
    /// The handler records each fresh logical broadcast's outcome exactly
    /// once, and there are now THREE terminal outcomes:
    ///
    /// 1. NO_TARGETS on the fresh-broadcast path (gated on `!is_retry &&
    ///    !is_reemit`, so neither retry re-emissions nor #4359
    ///    deferred-broadcast re-emissions record a miss);
    /// 2. success on the targets-found path;
    /// 3. success with a ZERO target count on the #5147 fully-covered path —
    ///    every eligible peer was already named by the originator (or is the
    ///    sender), so the fan-out is complete with nothing to send.
    ///
    /// The third was added with #5147 and goes through its OWN entry point,
    /// [`UpdatePropagationStats::record_fully_covered_broadcast`], precisely so
    /// it cannot land in the `no_targets` bucket. Counting it there would make
    /// the operator-facing propagation summary report a propagation failure
    /// every time the feature works perfectly — the loudest possible false
    /// alarm, on the path the design expects to be common in the clique regime.
    ///
    /// The first version of #5147 got this wrong in the most instructive way:
    /// it called `record_broadcast(key, 0, …)` under a comment asserting that
    /// it did NOT go through `no_targets`, while `record_broadcast`
    /// unconditionally does `if targets == 0 { no_targets += 1 }`. The comment
    /// and the code disagreed, and this pin — asserting only a CALL COUNT —
    /// passed either way. Hence the separate-entry-point assertion below and
    /// the behavioural test
    /// `a_fully_covered_fanout_is_not_counted_as_a_propagation_failure`.
    ///
    /// This pins all three so no outcome arm is dropped, and pins the gate so a
    /// refactor can't start counting retry / deferred re-emissions as fresh
    /// misses.
    #[test]
    fn broadcast_path_feeds_propagation_stats_pin_test() {
        // `handle_broadcast_state_change` lives in the `broadcast` submodule
        // after the p2p_protoc.rs split.
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/node/network_bridge/p2p_protoc/broadcast.rs");
        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("must read broadcast.rs at {}: {e}", path.display()));

        let call_count = source
            .matches("update_propagation_stats.record_broadcast(")
            .count();
        assert_eq!(
            call_count, 2,
            "handle_broadcast_state_change must call \
             `update_propagation_stats.record_broadcast(...)` exactly twice \
             — the fresh no-target path and the targets-found success path. \
             Found {call_count}. A different count means an outcome arm was \
             dropped (silent telemetry rot) or recording leaked elsewhere."
        );

        // The #5147 fully-covered arm must use its OWN entry point. Routing it
        // back through `record_broadcast(..., 0, ...)` would silently restore
        // the false-alarm bug: that call increments `no_targets`, the
        // operator-facing propagation-FAILURE counter, on the outcome that
        // means the feature is working.
        let fully_covered_calls = source.matches(".record_fully_covered_broadcast(").count();
        assert_eq!(
            fully_covered_calls, 1,
            "the #5147 fully-covered fan-out must be recorded exactly once, via \
             `record_fully_covered_broadcast(...)`. Found {fully_covered_calls}. \
             If this dropped to 0 the arm was re-routed through \
             `record_broadcast`, which counts a zero-target fan-out as a \
             propagation failure."
        );

        // The fully-covered branch must NOT drop the #4359 stash: it sends
        // nothing, so a stashed state that reached nobody is not superseded by
        // it. Only the targets-found path, which does send, may drop it.
        let covered_pos = source
            .find("let fully_covered =")
            .expect("the #5147 fully-covered branch is missing");
        let retry_pos = source
            .find("self.broadcast_retries.entry(key)")
            .expect("the no-target retry branch is missing");
        let covered_branch = &source[covered_pos..retry_pos];
        assert!(
            !covered_branch.contains("pending_broadcasts.take(")
                || covered_branch.contains(".stash("),
            "the fully-covered branch must not DISCARD the #4359 \
             pending-broadcast stash. It sends zero bytes, so the stashed state \
             — which by definition reached nobody — is not superseded by it, \
             and the peers it suppressed hold the ORIGINATOR's payload, which \
             does not contain that state. Taking the stash without putting the \
             current state back deletes the only non-heartbeat recovery path \
             for content that has never propagated. Refreshing it is correct; \
             discarding it is not."
        );

        // The no-target record must be gated on `!is_retry && !is_reemit` so
        // neither retry re-emissions (Codex P2) nor #4359 deferred-broadcast
        // re-emissions get counted as fresh misses (re-review SHOULD-FIX 5:
        // a flush per interested peer must not inflate no_targets).
        assert!(
            source.contains("if !is_retry && !is_reemit {"),
            "the fresh-no-target record must be gated on `!is_retry && !is_reemit` so retry \
             and #4359 deferred re-emissions are not counted as fresh broadcasts."
        );
    }

    // ── #6143: per-leg outcomes and the origination stall WARN ────────────

    fn counts(pairs: &[(PropagationEvent, u64)]) -> EventCounts {
        let mut c = EventCounts::default();
        for (ev, n) in pairs {
            c.add(*ev, *n);
        }
        c
    }

    fn summary(contract: ContractInstanceId, targets: u64, events: EventCounts) -> ContractSummary {
        ContractSummary {
            contract,
            updates: u64::from(targets > 0),
            targets_total: targets,
            no_targets: 0,
            fully_covered: 0,
            interest_resolve_failed: 0,
            events,
        }
    }

    #[test]
    fn propagation_events_are_densely_indexed() {
        let mut labels = std::collections::HashSet::new();
        for (i, ev) in PropagationEvent::ALL.iter().enumerate() {
            assert_eq!(ev.index(), i, "{ev:?} is out of place in ALL");
            assert!(labels.insert(ev.label()), "duplicate label {}", ev.label());
        }
    }

    #[test]
    fn a_stream_outcome_maps_to_its_wire_event() {
        use crate::transport::StreamSendOutcome;
        for (outcome, want) in [
            (
                Some(StreamSendOutcome::Delivered),
                PropagationEvent::WireStreamDelivered,
            ),
            (
                Some(StreamSendOutcome::EgressAbandoned),
                PropagationEvent::WireStreamEgressAbandoned,
            ),
            (
                Some(StreamSendOutcome::Dropped),
                PropagationEvent::WireStreamFailed,
            ),
            (None, PropagationEvent::WireStreamFailed),
        ] {
            assert_eq!(
                PropagationEvent::for_stream_outcome(outcome),
                want,
                "{outcome:?}"
            );
        }
    }

    #[test]
    fn each_event_is_counted_against_its_own_contract() {
        let stats = UpdatePropagationStats::new();
        let (a, b) = (cid(1), cid(2));
        for ev in PropagationEvent::ALL {
            stats.record_event(a, ev);
        }
        stats.record_event(a, PropagationEvent::SkipSummaryEqual);
        stats.record_event(b, PropagationEvent::SkipNoAddress);
        stats.record_event(b, PropagationEvent::RxBackoff);

        let a_events = stats.events_for_test(&a);
        for ev in PropagationEvent::ALL {
            let want = if ev == PropagationEvent::SkipSummaryEqual {
                2
            } else {
                1
            };
            assert_eq!(a_events.get(ev), want, "{ev:?}");
        }
        let b_events = stats.events_for_test(&b);
        assert_eq!(b_events.get(PropagationEvent::RxBackoff), 1);
        assert_eq!(b_events.sent(), 0);
        assert_eq!(a_events.sent(), 2, "sent = sent_delta + sent_full");
    }

    /// A receiver-side event names whatever contract an inbound broadcast
    /// carries, before any hosting check, so it counts only against a
    /// contract already tracked: a peer replaying broadcasts for random keys
    /// must not fill the tracking cap and lock this node's own contracts out.
    #[test]
    fn a_received_event_never_allocates_a_tracking_entry() {
        let id_at = |i: usize| {
            let mut bytes = [0u8; 32];
            bytes[..8].copy_from_slice(&(i as u64).to_le_bytes());
            ContractInstanceId::new(bytes)
        };
        let stats = UpdatePropagationStats::new();
        for rx in [
            PropagationEvent::RxDedup,
            PropagationEvent::RxAlreadyApplied,
            PropagationEvent::RxBackoff,
        ] {
            assert!(rx.is_received());
            for i in 0..MAX_TRACKED_CONTRACTS + 1 {
                stats.record_event(id_at(i), rx);
            }
        }
        assert_eq!(
            stats.contracts.len(),
            0,
            "no entry for an untracked contract"
        );
        for i in 0..MAX_TRACKED_CONTRACTS + 1 {
            stats.record_received(id_at(i), PropagationEvent::RxDedup, false);
        }
        assert_eq!(
            stats.contracts.len(),
            0,
            "nor for one this node does not host"
        );

        let hosted = cid(201);
        stats.record_received(hosted, PropagationEvent::RxBackoff, true);
        assert_eq!(
            stats
                .events_for_test(&hosted)
                .get(PropagationEvent::RxBackoff),
            1,
            "a hosted contract gets an entry for its received events"
        );

        let own = cid(200);
        stats.record_event(own, PropagationEvent::ClientOriginated);
        assert_eq!(
            stats.contracts.len(),
            2,
            "this node's contracts are tracked"
        );
        stats.record_event(own, PropagationEvent::RxDedup);
        assert_eq!(
            stats.events_for_test(&own).get(PropagationEvent::RxDedup),
            1,
            "a tracked contract still counts its received events"
        );
        for ev in PropagationEvent::ALL {
            let rx = matches!(
                ev,
                PropagationEvent::RxDedup
                    | PropagationEvent::RxAlreadyApplied
                    | PropagationEvent::RxBackoff
            );
            assert_eq!(ev.is_received(), rx, "{ev:?}");
        }
    }

    /// A contract with no fan-out of its own (a heal-only or receive-only
    /// contract) still has a story to tell, so it must not be filtered out of
    /// the window the way an idle contract is.
    #[test]
    fn a_window_with_only_propagation_events_is_still_reported() {
        let w = aggregate_window(
            vec![summary(
                cid(4),
                0,
                counts(&[(PropagationEvent::RxDedup, 3)]),
            )],
            TOP_K,
        )
        .expect("an event-only contract is activity");
        assert_eq!(w.contracts, 1);
        assert_eq!(w.updates, 0);
        assert_eq!(w.events.get(PropagationEvent::RxDedup), 3);
    }

    /// A client update for `c` that resolved `targets` (none: no fan-out was
    /// processed) and recorded `events`, in the window being counted.
    fn client_update(
        stats: &UpdatePropagationStats,
        c: ContractInstanceId,
        targets: usize,
        events: &[(PropagationEvent, u64)],
    ) {
        stats.record_event(c, PropagationEvent::ClientOriginated);
        if targets > 0 {
            stats.record_broadcast(c, targets, 0);
        }
        record(stats, c, events);
    }

    fn record(
        stats: &UpdatePropagationStats,
        c: ContractInstanceId,
        events: &[(PropagationEvent, u64)],
    ) {
        for (ev, n) in events {
            for _ in 0..*n {
                stats.record_event(c, *ev);
            }
        }
    }

    /// The #6143 signature: a client update found 14 targets and every leg was
    /// skipped. The window it happened in is grace; the end of the next window
    /// reports it, exactly once.
    #[test]
    fn a_client_update_with_targets_and_no_send_for_two_windows_is_a_stall() {
        let stats = UpdatePropagationStats::new();
        let c = cid(9);
        client_update(&stats, c, 14, &[(PropagationEvent::SkipSummaryEqual, 14)]);
        assert!(stats.drain_stalls_for_test().is_empty(), "grace window");

        let reports = stats.drain_stalls_for_test();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].contract, c);
        assert_eq!(reports[0].client_originated, 1);
        assert_eq!(reports[0].targets_total, 14);
        assert_eq!(
            reports[0].events.get(PropagationEvent::SkipSummaryEqual),
            14,
            "the WARN must carry where the legs went"
        );
        assert!(
            stats.drain_stalls_for_test().is_empty(),
            "reported once, not every window after"
        );
    }

    /// An update applied at the end of W-1 whose fan-out is processed in W:
    /// the client update and the targets land in different windows. Judging
    /// W-1 alone sees a client update with no targets; judging W alone sees
    /// targets with no client update. Together they are a stall.
    #[test]
    fn an_update_straddling_two_windows_is_a_stall() {
        let stats = UpdatePropagationStats::new();
        let c = cid(10);
        stats.record_event(c, PropagationEvent::ClientOriginated);
        assert!(stats.drain_stalls_for_test().is_empty());
        stats.record_broadcast(c, 3, 0);
        record(&stats, c, &[(PropagationEvent::SkipSummaryEqual, 3)]);
        let reports = stats.drain_stalls_for_test();
        assert_eq!(reports.len(), 1, "{reports:?}");
        assert_eq!(reports[0].client_originated, 1);
        assert_eq!(reports[0].targets_total, 3, "W's targets count");
        assert_eq!(reports[0].events.get(PropagationEvent::SkipSummaryEqual), 3);
    }

    /// Every leg of a client update refused by the bridge (no `Sent*`) is a
    /// stall: the update never left the node. A transport refusal
    /// (`wire_send_failed`) is not evidence, wherever its leg's `Sent*` is:
    /// beside it in the same window, or two windows back and so not read (a
    /// message the bridge took in W-2 that failed while the connection
    /// drained its queue before shutdown).
    #[test]
    fn legs_refused_by_the_bridge_are_a_stall_but_a_transport_failure_is_not() {
        let stats = UpdatePropagationStats::new();
        let refused = cid(13);
        let taken = cid(14);
        let taken_earlier = cid(15);
        // `taken_earlier`'s leg went to the bridge in W-2.
        record(&stats, taken_earlier, &[(PropagationEvent::SentDelta, 1)]);
        assert!(stats.drain_stalls_for_test().is_empty());
        client_update(&stats, refused, 3, &[(PropagationEvent::SendFailed, 3)]);
        client_update(
            &stats,
            taken,
            3,
            &[
                (PropagationEvent::SentDelta, 3),
                (PropagationEvent::WireSendFailed, 3),
            ],
        );
        client_update(
            &stats,
            taken_earlier,
            1,
            &[(PropagationEvent::WireSendFailed, 1)],
        );
        assert!(stats.drain_stalls_for_test().is_empty(), "grace");
        let reports = stats.drain_stalls_for_test();
        assert_eq!(reports.len(), 1, "{reports:?}");
        assert_eq!(reports[0].contract, refused);
        assert_eq!(reports[0].events.get(PropagationEvent::SendFailed), 3);
    }

    /// One leg ended unsent while its siblings are only held (their pairs
    /// busy with a stream, or queued) is not a stall: a held leg records
    /// nothing, so the rule needs every resolved leg accounted for. The
    /// skips and bridge refusals together, across both windows, must reach
    /// the targets resolved.
    #[test]
    fn a_stall_needs_every_resolved_leg_to_have_ended_unsent() {
        let stats = UpdatePropagationStats::new();
        let (skip, refused) = (
            PropagationEvent::SkipSummaryEqual,
            PropagationEvent::SendFailed,
        );
        // One refused leg (a peer that disconnected), the other 13 held.
        client_update(&stats, cid(20), 14, &[(refused, 1)]);
        // One skipped leg, the other 13 held.
        client_update(&stats, cid(21), 14, &[(skip, 1)]);
        // One short of every leg.
        client_update(&stats, cid(22), 14, &[(skip, 10), (refused, 3)]);
        // Every leg, skips and refusals mixed.
        let mixed = cid(23);
        client_update(&stats, mixed, 14, &[(skip, 10), (refused, 4)]);
        // Half the legs ended in W-1, the rest in W.
        let straddling = cid(24);
        client_update(&stats, straddling, 14, &[(skip, 7)]);
        assert!(stats.drain_stalls_for_test().is_empty(), "grace");
        record(&stats, straddling, &[(refused, 7)]);
        let stalled: Vec<_> = stats
            .drain_stalls_for_test()
            .into_iter()
            .map(|r| r.contract)
            .collect();
        assert_eq!(stalled, {
            let mut want = vec![mixed, straddling];
            want.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
            want
        });
    }

    /// Every sender-side skip is evidence: a client update whose legs were
    /// all skipped for any one reason is a stall.
    #[test]
    fn every_skip_reason_is_evidence_of_a_stall() {
        let skips: Vec<_> = PropagationEvent::ALL
            .into_iter()
            .filter(|ev| ev.is_skip())
            .collect();
        assert_eq!(skips.len(), 7, "a new skip reason needs a look here");
        for skip in skips {
            let stats = UpdatePropagationStats::new();
            let c = cid(25);
            client_update(&stats, c, 3, &[(skip, 3)]);
            assert!(stats.drain_stalls_for_test().is_empty(), "{skip:?}: grace");
            let reports = stats.drain_stalls_for_test();
            assert_eq!(reports.len(), 1, "{skip:?} is not counted as evidence");
            assert_eq!(reports[0].events.get(skip), 3, "{skip:?}");
        }
    }

    /// The same straddle, with the fan-out dropped in W rather than resolved.
    #[test]
    fn a_fanout_dropped_in_the_next_window_is_a_stall() {
        let stats = UpdatePropagationStats::new();
        let c = cid(11);
        stats.record_event(c, PropagationEvent::ClientOriginated);
        assert!(stats.drain_stalls_for_test().is_empty());
        stats.record_event(c, PropagationEvent::FanoutEventDropped);
        let reports = stats.drain_stalls_for_test();
        assert_eq!(reports.len(), 1, "{reports:?}");
        assert_eq!(reports[0].targets_total, 0);
        assert_eq!(
            reports[0].events.get(PropagationEvent::FanoutEventDropped),
            1
        );
    }

    /// A client update whose whole fan-out was dropped by `try_notify` never
    /// resolves targets, so `targets_total` is zero. It is still a stall: the
    /// update never left the node.
    #[test]
    fn a_client_update_whose_fanout_was_dropped_is_a_stall() {
        let stats = UpdatePropagationStats::new();
        let c = cid(12);
        client_update(&stats, c, 0, &[(PropagationEvent::FanoutEventDropped, 1)]);
        assert!(stats.drain_stalls_for_test().is_empty(), "grace");
        let reports = stats.drain_stalls_for_test();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].targets_total, 0);
    }

    /// A leg handed to the bridge in either window clears the contract,
    /// whether it went with the update (W-1) or was queued near the boundary
    /// and went out in W. Every resolved leg was skipped (the send is another
    /// fan-out's leg), so without the send each would be a stall.
    #[test]
    fn a_send_in_either_window_clears_the_contract() {
        let skipped = (PropagationEvent::SkipSummaryEqual, 3);
        for sent in [PropagationEvent::SentDelta, PropagationEvent::SentFull] {
            let stats = UpdatePropagationStats::new();
            let (same, next) = (cid(13), cid(14));
            client_update(&stats, same, 3, &[(sent, 1), skipped]);
            client_update(&stats, next, 3, &[skipped]);
            assert!(stats.drain_stalls_for_test().is_empty());
            stats.record_event(next, sent);
            assert!(stats.drain_stalls_for_test().is_empty(), "{sent:?}");
            assert!(stats.drain_stalls_for_test().is_empty(), "{sent:?}");

            // A dropped fan-out is evidence, but a send still clears it, in
            // the same window or the next. (A rule that let a drop warn
            // whatever was sent fails here and nowhere else.)
            let stats = UpdatePropagationStats::new();
            let (dropped_same, dropped_next) = (cid(15), cid(16));
            let dropped = (PropagationEvent::FanoutEventDropped, 1);
            client_update(&stats, dropped_same, 0, &[dropped, (sent, 1)]);
            client_update(&stats, dropped_next, 0, &[dropped]);
            assert!(stats.drain_stalls_for_test().is_empty(), "{sent:?}: grace");
            stats.record_event(dropped_next, sent);
            assert!(
                stats.drain_stalls_for_test().is_empty(),
                "{sent:?} beside a dropped fan-out"
            );
        }
    }

    /// #6143 review round 6: a newer fan-out that replaces a queued leg ends
    /// that leg as `skip_superseded`. Two client updates of 14 targets each
    /// resolve 28 legs, but at most 14 reach a send, a skip or a refusal; the
    /// other 14 were superseded. Uncounted, every leg could never be
    /// accounted for, and this burst, the #6143 shape, could never warn.
    #[test]
    fn a_superseded_leg_is_accounted_for_as_ended_unsent() {
        let stats = UpdatePropagationStats::new();
        let superseded = (PropagationEvent::SkipSuperseded, 14);
        // The newer legs all skipped as converged.
        let skipped = cid(30);
        client_update(&stats, skipped, 14, &[]);
        client_update(
            &stats,
            skipped,
            14,
            &[superseded, (PropagationEvent::SkipSummaryEqual, 14)],
        );
        // The newer legs still held: no outcome yet, so not a stall.
        let held = cid(31);
        client_update(&stats, held, 14, &[]);
        client_update(&stats, held, 14, &[superseded]);
        // The newer legs sent.
        let sent = cid(32);
        client_update(&stats, sent, 14, &[]);
        client_update(
            &stats,
            sent,
            14,
            &[superseded, (PropagationEvent::SentDelta, 14)],
        );
        assert!(stats.drain_stalls_for_test().is_empty(), "grace");
        let reports = stats.drain_stalls_for_test();
        let stalled: Vec<_> = reports.iter().map(|r| r.contract).collect();
        assert_eq!(stalled, vec![skipped]);
        assert_eq!(reports[0].targets_total, 28);
        assert_eq!(reports[0].events.get(PropagationEvent::SkipSuperseded), 14);
    }

    /// #6143 review round 6: a heal (`SyncStateToPeer`) registers its own leg
    /// (`heal_target`), so its skip accounts for that leg and never for a
    /// fan-out leg still held. Repeated converged heals beside a client
    /// update whose 14 legs are all held are not a stall; heals that skip
    /// beside fan-out legs that all skipped are part of one.
    #[test]
    fn a_heal_skip_accounts_for_its_own_leg_only() {
        let stats = UpdatePropagationStats::new();
        let (heal, skip) = (
            PropagationEvent::HealTarget,
            PropagationEvent::SkipSummaryEqual,
        );
        let held = cid(33);
        client_update(&stats, held, 14, &[(heal, 14), (skip, 14)]);
        let every_leg = cid(34);
        client_update(&stats, every_leg, 3, &[(heal, 2), (skip, 5)]);
        // The heals land in W.
        let heals_next = cid(35);
        client_update(&stats, heals_next, 14, &[]);
        assert!(stats.drain_stalls_for_test().is_empty(), "grace");
        record(&stats, heals_next, &[(heal, 14), (skip, 14)]);
        let stalled: Vec<_> = stats
            .drain_stalls_for_test()
            .into_iter()
            .map(|r| r.contract)
            .collect();
        assert_eq!(stalled, vec![every_leg]);
    }

    /// Only W-1 and W are read: a send two windows before the client update
    /// does not clear it.
    #[test]
    fn a_send_before_the_update_window_does_not_clear_it() {
        let stats = UpdatePropagationStats::new();
        let c = cid(15);
        stats.record_broadcast(c, 2, 0);
        record(&stats, c, &[(PropagationEvent::SentDelta, 2)]);
        assert!(stats.drain_stalls_for_test().is_empty());
        client_update(&stats, c, 2, &[(PropagationEvent::SkipSummaryEqual, 2)]);
        assert!(stats.drain_stalls_for_test().is_empty());
        assert_eq!(stats.drain_stalls_for_test().len(), 1);
    }

    /// A stream the bridge took is never a stall, whenever its fate arrives
    /// in the two windows the rule reads (W-1 with the update, or W) and
    /// however it ends: transport outcomes are counters only. A fate any
    /// later is never read at all, since the contract has left the previous
    /// window's map; nothing needs testing there.
    ///
    /// With the send: the stream leg and two skipped legs (one of the update's,
    /// one of an earlier fan-out whose targets were counted before W-1), so
    /// the skips reach `targets_total` and a rule that discounted a failed
    /// stream's send would WARN here.
    ///
    /// Without it (the transport's view of a leg whose bridge-level count is
    /// missing), the stream is the only leg, so a rule that read a failed
    /// stream as an unsent leg would WARN on that too.
    #[test]
    fn a_stream_never_warns_however_and_whenever_it_ends() {
        use crate::test_utils::TestLogger;

        let logger = TestLogger::new().capture_logs().with_level("warn").init();
        for fate in [
            PropagationEvent::WireStreamDelivered,
            PropagationEvent::WireStreamFailed,
            PropagationEvent::WireStreamEgressAbandoned,
        ] {
            for with_sent in [true, false] {
                // The window, counted from the update's, that the fate lands in.
                for fate_window in [0, 1] {
                    let what = format!("{fate:?}, sent={with_sent}, window {fate_window}");
                    let stats = UpdatePropagationStats::new();
                    let c = cid(16);
                    let mut legs = vec![(PropagationEvent::WireStream, 1)];
                    let targets = if with_sent {
                        legs.push((PropagationEvent::SentFull, 1));
                        legs.push((PropagationEvent::SkipSummaryEqual, 2));
                        2
                    } else {
                        1
                    };
                    client_update(&stats, c, targets, &legs);
                    for window in 0..3 {
                        if window == fate_window {
                            stats.record_event(c, fate);
                        }
                        assert_eq!(stats.drain_and_log(), 0, "{what}: drain {window}");
                    }
                }
            }
        }
        let warns = logger.logs_matching(|l| l.contains("update_propagation_stalled"));
        assert!(warns.is_empty(), "{warns:#?}");
    }

    #[test]
    fn non_candidates_never_warn() {
        let stats = UpdatePropagationStats::new();
        // Handed to the bridge in the same window.
        client_update(&stats, cid(1), 5, &[(PropagationEvent::SentFull, 1)]);
        // No targets and no dropped fan-out: the NO_TARGETS path, which
        // already WARNs itself.
        client_update(&stats, cid(2), 0, &[]);
        stats.record_broadcast(cid(2), 0, 0);
        // No client update: relayed traffic that found every peer current.
        stats.record_broadcast(cid(3), 5, 0);
        stats.record_event(cid(3), PropagationEvent::SkipSummaryEqual);
        // A dropped fan-out with no client update.
        stats.record_event(cid(4), PropagationEvent::FanoutEventDropped);
        // Targets resolved, and every leg still held (its pair busy with a
        // stream) or queued, so nothing records an outcome: no evidence.
        client_update(&stats, cid(5), 14, &[]);
        assert!(stats.drain_stalls_for_test().is_empty());
        // Targets and a drop in W do not make W-1 a candidate without a
        // client update in W-1. (cid(5)'s held legs ALL failing at the bridge
        // in W WOULD be a stall: see
        // `a_stall_needs_every_resolved_leg_to_have_ended_unsent`.)
        stats.record_broadcast(cid(3), 5, 0);
        stats.record_event(cid(4), PropagationEvent::FanoutEventDropped);
        assert!(stats.drain_stalls_for_test().is_empty());
        assert!(stats.drain_stalls_for_test().is_empty());
    }

    /// A stall that keeps going while the client keeps posting is reported in
    /// every window, each report carrying the two windows it judged, and once
    /// more after the client stops.
    #[test]
    fn a_continuing_stall_is_reported_once_per_window() {
        let stats = UpdatePropagationStats::new();
        let c = cid(17);
        let stall = || client_update(&stats, c, 14, &[(PropagationEvent::SkipEmptyDelta, 14)]);
        stall();
        assert!(stats.drain_stalls_for_test().is_empty());
        for _ in 0..3 {
            stall();
            let reports = stats.drain_stalls_for_test();
            assert_eq!(reports.len(), 1);
            assert_eq!(reports[0].client_originated, 1, "W-1's client updates");
            assert_eq!(reports[0].events.get(PropagationEvent::SkipEmptyDelta), 28);
        }
        assert_eq!(stats.drain_stalls_for_test().len(), 1, "the last update");
        assert!(stats.drain_stalls_for_test().is_empty());
    }

    /// The memory estimate on [`MAX_TRACKED_CONTRACTS`] holds.
    #[test]
    fn a_tracked_contract_stays_within_its_memory_bound() {
        for (what, size) in [
            ("ContractCounters", std::mem::size_of::<ContractCounters>()),
            ("ContractSummary", std::mem::size_of::<ContractSummary>()),
        ] {
            assert!(size <= MAX_COUNTERS_SIZE, "{what}: {size} bytes");
        }
    }

    /// After a runtime stall or a suspend the summary task must not drain a
    /// burst of milliseconds-long windows: that would report a stall
    /// with no real grace window. One drain covers the missed span, and the
    /// next comes a full window after it (`Delay`), not at the next multiple
    /// of the period from the start (`Skip`).
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_missed_span_is_one_window_not_a_burst() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU64, Ordering};

        let settle = || async {
            for _ in 0..50 {
                tokio::task::yield_now().await;
            }
        };
        let stats = Arc::new(UpdatePropagationStats::new());
        let (drains, stalls) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
        let task = {
            let (drains, stalls) = (drains.clone(), stalls.clone());
            stats.clone().start_summary_task(move |n| {
                drains.fetch_add(1, Ordering::Relaxed);
                stalls.fetch_add(n, Ordering::Relaxed);
            })
        };
        settle().await;

        let c = cid(16);
        client_update(&stats, c, 3, &[(PropagationEvent::SkipSummaryEqual, 3)]);

        // The node was stalled for five and a half windows.
        let half = SUMMARY_WINDOW / 2;
        tokio::time::advance(SUMMARY_WINDOW * 5 + half).await;
        settle().await;
        assert_eq!(drains.load(Ordering::Relaxed), 1, "one drain, not five");
        assert_eq!(stalls.load(Ordering::Relaxed), 0, "that drain is grace");

        // `Skip` would drain here, at six windows from the start, half a
        // window after the last drain.
        tokio::time::advance(half).await;
        settle().await;
        assert_eq!(drains.load(Ordering::Relaxed), 1, "a full window, not half");

        tokio::time::advance(half).await;
        settle().await;
        assert_eq!(drains.load(Ordering::Relaxed), 2);
        assert_eq!(stalls.load(Ordering::Relaxed), 1);
        task.abort();
    }

    /// Up to [`MAX_STALL_WARNS_PER_WINDOW`] stalled contracts in a window give
    /// one WARN each and no rollup; one more gives that many WARNs and a
    /// rollup of 1. The count returned is all of them either way.
    #[test]
    fn stall_warns_are_capped_per_window() {
        use crate::test_utils::TestLogger;

        for (n, rollup) in [
            (MAX_STALL_WARNS_PER_WINDOW, None),
            (MAX_STALL_WARNS_PER_WINDOW + 1, Some("1 more contract(s)")),
        ] {
            let logger = TestLogger::new().capture_logs().with_level("warn").init();
            let stats = UpdatePropagationStats::new();
            for i in 0..n {
                client_update(
                    &stats,
                    cid(100 + i as u8),
                    2,
                    &[(PropagationEvent::SkipSummaryEqual, 2)],
                );
            }
            assert_eq!(stats.drain_and_log(), 0);
            assert_eq!(stats.drain_and_log(), n as u64);
            let logs = logger.logs();
            let per_contract = logs
                .iter()
                .filter(|l| l.contains("origination_stall") && !l.contains("rollup"))
                .count();
            assert_eq!(per_contract, MAX_STALL_WARNS_PER_WINDOW, "n={n}: {logs:#?}");
            let found = logs.iter().find(|l| l.contains("origination_stall_rollup"));
            match rollup {
                None => assert!(found.is_none(), "n={n}: {found:?}"),
                Some(text) => {
                    let line = found.expect("one rollup line");
                    assert!(line.contains(text), "n={n}: {line}");
                }
            }
        }
    }

    /// End to end through the real recorders and the real drain: the INFO line
    /// carries the per-leg counts, and the stall WARN fires on the second
    /// drain and only then.
    #[test]
    fn drain_and_log_reports_leg_outcomes_and_warns_on_a_stall() {
        use crate::test_utils::TestLogger;

        let logger = TestLogger::new().capture_logs().with_level("info").init();
        let stats = UpdatePropagationStats::new();
        let c = cid(42);
        stats.record_event(c, PropagationEvent::ClientOriginated);
        stats.record_broadcast(c, 3, 0);
        for _ in 0..3 {
            stats.record_event(c, PropagationEvent::SkipSummaryEqual);
        }

        // The captured lines carry ANSI colour codes between a field's name
        // and its value; strip them to match `name: value`.
        let plain = |logger: &TestLogger| {
            let mut out = String::new();
            for line in logger.logs() {
                let mut chars = line.chars();
                while let Some(ch) = chars.next() {
                    if ch == '\u{1b}' {
                        for c in chars.by_ref() {
                            if c == 'm' {
                                break;
                            }
                        }
                    } else {
                        out.push(ch);
                    }
                }
                out.push('\n');
            }
            out
        };

        assert_eq!(stats.drain_and_log(), 0, "first window is grace");
        let text = plain(&logger);
        assert!(text.contains("skip_summary_equal: 3"), "{text}");
        assert!(text.contains("client_originated: 1"), "{text}");
        assert!(text.contains("sent_delta: 0"), "{text}");
        assert!(!text.contains("update_propagation_stalled"), "{text}");

        assert_eq!(stats.drain_and_log(), 1);
        let text = plain(&logger);
        assert!(text.contains("update_propagation_stalled"), "{text}");
        assert!(text.contains("WARN"), "{text}");
    }

    /// Pin: the simulation fan-out (`broadcast_state_to_peers`, the only
    /// fan-out `simulation_tests` builds run) counts each leg's outcome like
    /// the production queue. Without the `Sent*` counts every client update in
    /// a simulation read as unsent, and without the skips none could stall.
    ///
    /// A source pin, not a behavioural test: that function exists only under
    /// `simulation_tests`, which the lib test job does not build, and the
    /// simulation job runs no lib test outside `sim_e2e_tests`. So each
    /// recording site is pinned in its place, on the function's body with
    /// comments and whitespace removed (so a commented-out call is gone and
    /// rustfmt cannot move a needle): the number of calls, and for each its
    /// context. The `SkipNotHosted` count runs once per target; every other
    /// site is in the per-target loop; each skip and the serialization
    /// failure end their leg with a `continue` in the same block; the refused
    /// send is counted in the `Err` arm of the send result and the `Sent*`
    /// count in its `else` arm, with the delta and full arms the right way
    /// round.
    #[test]
    fn the_simulation_fan_out_counts_its_legs() {
        let src = include_str!("../../node/network_bridge/p2p_protoc/broadcast.rs");
        let signature = "\n    async fn broadcast_state_to_peers(";
        assert_eq!(
            src.matches(signature).count(),
            1,
            "the simulation fan-out's signature must be unique"
        );
        let at = src.find(signature).expect("simulation fan-out not found") + 1;
        // The method is indented four spaces, so its closing brace is the
        // first line holding only `    }`; nothing inside it sits that far
        // left.
        let len = src[at..]
            .find("\n    }\n")
            .expect("end of the simulation fan-out not found");
        // Line comments and all whitespace removed. The body holds no string
        // with `//` in it, so cutting at `//` removes only comments.
        let body: String = src[at..at + len]
            .lines()
            .map(|line| line.split_once("//").map_or(line, |(code, _)| code))
            .flat_map(str::chars)
            .filter(|c| !c.is_whitespace())
            .collect();
        // The span of the block whose `{` is at `open`, without its braces.
        let block = |text: &str, open: usize| -> (usize, usize) {
            assert_eq!(&text[open..=open], "{", "not a block");
            let mut depth = 0usize;
            for (i, ch) in text[open..].char_indices() {
                match ch {
                    '{' => depth += 1,
                    '}' => {
                        depth -= 1;
                        if depth == 0 {
                            return (open + 1, open + i);
                        }
                    }
                    _ => {}
                }
            }
            panic!("unbalanced block at {open}");
        };
        // The block opened by the unique occurrence of `head`, which ends
        // in `{`.
        let block_after = |text: &str, head: &str| -> (usize, usize) {
            assert_eq!(
                text.matches(head).count(),
                1,
                "the simulation fan-out must hold `{head}` once"
            );
            let open = text.find(head).map(|i| i + head.len() - 1).unwrap_or(0);
            block(text, open)
        };
        // What follows `at` up to the end of its block, at that block's
        // nesting level only.
        let rest_of_block = |text: &str, at: usize| -> String {
            let mut depth = 0usize;
            let mut out = String::new();
            for ch in text[at..].chars() {
                match ch {
                    '{' => depth += 1,
                    '}' if depth == 0 => break,
                    '}' => depth -= 1,
                    _ if depth == 0 => out.push(ch),
                    _ => {}
                }
            }
            out
        };
        let calls = |needle: &str| body.matches(needle).count();

        // Not hosted: one count per target, then out.
        let not_hosted = "count(PropagationEvent::SkipNotHosted);";
        assert_eq!(calls(not_hosted), 1, "`{not_hosted}`");
        assert!(
            body.contains(&format!(
                "for_in&target_result.targets{{{not_hosted}}}return;"
            )),
            "the not-hosted skip must be counted once per target"
        );

        let (loop_start, loop_end) = block_after(&body, "fortargetin&target_result.targets{");
        let per_leg = &body[loop_start..loop_end];
        // The no-address skip is the loop's first statement and ends the leg.
        assert!(
            per_leg.starts_with(
                "letSome(peer_addr)=target.socket_addr()else{\
                 count(PropagationEvent::SkipNoAddress);continue;};"
            ),
            "the no-address skip must be counted, then end its leg"
        );
        // Each skip, and the serialization failure, ends its leg.
        for needle in [
            "count(PropagationEvent::SkipNoAddress);",
            "count(skip);",
            "count(PropagationEvent::SkipEmptyDelta);",
        ] {
            assert_eq!(calls(needle), 1, "`{needle}`");
            let at = per_leg.find(needle).expect("in the per-target loop");
            assert!(
                rest_of_block(per_leg, at + needle.len()).contains("continue;"),
                "`{needle}` must end its leg"
            );
        }
        let failed = "count(PropagationEvent::SendFailed);";
        assert_eq!(calls(failed), 2, "`{failed}`");
        assert_eq!(per_leg.matches(failed).count(), 2, "`{failed}` per leg");

        // The send result: refused in `Err`, sent in `else`.
        let head = "ifletErr(err)=send_result{";
        let (err_start, err_end) = block_after(per_leg, head);
        let err_arm = &per_leg[err_start..err_end];
        assert!(
            rest_of_block(err_arm, 0).contains(failed),
            "a refused send must be counted in the `Err` arm"
        );
        let serialize_failed = per_leg
            .match_indices(failed)
            .map(|(i, _)| i)
            .find(|&i| i < err_start || i >= err_end)
            .expect("the serialization failure's count");
        assert!(
            rest_of_block(per_leg, serialize_failed + failed.len()).contains("continue;"),
            "the serialization failure must end its leg"
        );
        assert!(
            per_leg[err_end..].starts_with("}else{"),
            "the send result must have an `else` arm"
        );
        let (else_start, else_end) = block(per_leg, err_end + "}else".len());
        let sent =
            "count(ifsent_delta{PropagationEvent::SentDelta}else{PropagationEvent::SentFull});";
        assert_eq!(calls(sent), 1, "`{sent}`");
        let else_arm = &per_leg[else_start..else_end];
        let in_else = else_arm
            .find(sent)
            .expect("the `Sent*` count in the `else` arm");
        assert_eq!(
            else_arm[..in_else].matches('{').count(),
            else_arm[..in_else].matches('}').count(),
            "the `Sent*` count must be a statement of the send result's `else` arm"
        );
    }

    /// Pin: every [`PropagationEvent`] is recorded somewhere in production
    /// code. Counting the recorder in tests proves nothing if no production
    /// path calls it with that variant — the "manually-mirrored counter rots"
    /// pattern in `.claude/rules/bug-prevention-patterns.md`.
    #[test]
    fn every_propagation_event_has_a_production_recording_site() {
        let sources = [
            include_str!("../../node/network_bridge/broadcast_queue.rs"),
            include_str!("../../node/network_bridge/p2p_protoc.rs"),
            include_str!("../../node/op_state_manager.rs"),
            include_str!("../update.rs"),
            include_str!("op_ctx_task.rs"),
        ];
        // Production code only: everything above the first top-level test
        // module, whatever its visibility.
        let production = |src: &'static str| -> &'static str {
            let test_mod = src.match_indices("\n#[cfg(test)]\n").find(|(i, m)| {
                let rest = &src[i + m.len()..];
                ["mod ", "pub(crate) mod ", "pub(super) mod "]
                    .iter()
                    .any(|p| rest.starts_with(p))
            });
            test_mod.map_or(src, |(i, _)| &src[..i])
        };
        let named = |src: &'static str, needle: &str| {
            production(src).lines().any(|line| {
                let line = line.trim_start();
                !line.starts_with("//") && line.contains(needle)
            })
        };
        // Both stream paths (the broadcast queue's full-state streams and the
        // connection task's inline broadcasts) must count a stream's start
        // and record its fate through the stream-fate recorder, which maps
        // the outcome to one of the three fate events.
        let by_outcome = [
            PropagationEvent::WireStream,
            PropagationEvent::WireStreamDelivered,
            PropagationEvent::WireStreamFailed,
            PropagationEvent::WireStreamEgressAbandoned,
        ];
        for (src, fate) in [
            (sources[0], "record_propagation_stream_fate("),
            (sources[1], ".record_stream_fate("),
        ] {
            assert!(
                named(src, "PropagationEvent::WireStream)") && named(src, fate),
                "a stream path no longer records its stream's start and fate"
            );
        }
        for ev in PropagationEvent::ALL {
            if by_outcome.contains(&ev) {
                continue;
            }
            let needle = format!("PropagationEvent::{ev:?}");
            let found = sources.iter().any(|src| named(src, &needle));
            assert!(found, "{needle} is never recorded by production code");
        }
    }
}

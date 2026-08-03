//! Which fan-out arm chose a broadcast payload, and how many bytes it put on
//! the wire.
//!
//! ## Why
//!
//! A production measurement on 2026-07-24 (posted to #3335) found that the
//! fleet sends ~60 GB per 3 h, that ~85 % of byte-weighted broadcast work is
//! on contracts holding >= 500 KB of state, and that 97.9 % of it lands on
//! contracts whose applies change nothing. What it could NOT determine is
//! *why* those large states go out whole, because
//! [`broadcast_to_single_peer`] has **six distinct paths that send FULL
//! STATE** and none of them were separately instrumented:
//!
//! 1. [`PayloadArm::FullDeltaSuppressed`] — the `delta_incompat` memo is armed
//!    (#4904): the contract is known to reject every delta, so full state is
//!    sent deliberately.
//! 2. [`PayloadArm::FullNotEfficient`] — the delta WAS computed but came back
//!    not smaller than our full state, so full state is the (equal or
//!    smaller) optimal payload. Until #4923 this arm instead meant the
//!    pre-compute [`crate::ring::interest::is_delta_efficient`] summary-size
//!    gate refused before computing anything — a fallback that was never
//!    smaller than the delta it declined, and 41 % of all wire bytes in the
//!    2026-07-24 measurement. Interpret pre-/post-#4923 telemetry for this
//!    arm accordingly.
//! 3. [`PayloadArm::FullComputeFailed`] — the contract's WASM failed, timed
//!    out, or answered unexpectedly.
//! 4. [`PayloadArm::FullNoOurSummary`] — *our* summary is missing, so there
//!    was nothing to diff from.
//! 5. [`PayloadArm::FullNoTheirSummaryUntracked`] — the peer's summary is
//!    missing AND the peer is not tracked in `interested_peers` at all. Before
//!    #4952 the cache write that would fix it was a silent no-op (permanent
//!    full state); since #4952 the delivery path upserts, so this arm is
//!    transient (first send per pair) and should decay toward zero — a
//!    persistent residual implicates the seeding/heartbeat chain instead.
//! 6. [`PayloadArm::FullNoTheirSummaryTracked`] — the peer's summary is
//!    missing but the peer IS tracked, so the next delivery repairs it.
//!    Measured at 26.9 % of broadcast bytes (357 KB mean) on the aged 0.2.109
//!    fleet — the largest remaining arm — so #4961 splits it AGAIN, by
//!    [`SummaryMissingReason`]: never seeded vs cleared by the peer's own
//!    `None` report vs cleared by a resync or a delta-apply failure. Those
//!    have three different fixes, published as
//!    `tracked_missing_<reason>_{sends,bytes}` plus a
//!    `tracked_missing_unattributed_*` residual that must stay at zero.
//!
//! Arms 4-6 were a single `full_no_summary` bucket when #4922 first shipped.
//! The 2026-07-25 measurement then found that bucket was the LARGEST single
//! consumer of wire bytes on the network, with no way to tell a contract-handler
//! failure (4, a load problem) from a peer-tracking gap (5, structural until
//! the #4952 upsert made it self-healing) from ordinary cold start (6,
//! self-healing). Those three have nothing in
//! common except the symptom, so the split is what makes the number actionable.
//! The pre-split `full_no_summary_sends` / `_bytes` fields are still published
//! as the sum of the three.
//!
//! Those arms have completely different remedies, so knowing which one emits
//! the bytes is the difference between a targeted fix and a guess. Deltas are
//! counted too ([`PayloadArm::Delta`]) so the mix is a ratio rather than a
//! bare count.
//!
//! ## What is counted
//!
//! Bytes are recorded at the **real-delivery** sites only — the same points
//! that charge [`ResourceType::BroadcastFanoutCost`][rt] — so a dropped,
//! timed-out, or failed-to-enqueue send never inflates the mix with phantom
//! fan-out. This deliberately mirrors the #4903 review round-3 Fix 4
//! accounting rule; see `broadcast_queue::broadcast_to_single_peer`.
//!
//! [rt]: crate::topology::meter::ResourceType::BroadcastFanoutCost
//!
//! ## The fan-out multiplier (#5062)
//!
//! Everything above measures what this node SENT. [`ApplyOrigin`] measures the
//! other half — what this node APPLIED, and whether it came from a local
//! client or from a peer — because the send counters alone cannot distinguish
//! the two topologies that produce the fleet's observed ~17.5 payloads per
//! state-changing one (#5091):
//!
//!   * one node fanning out to its ~17 co-hosts, versus
//!   * ~17 nodes each RE-fanning-out to their own ~17 co-hosts.
//!
//! Those differ by a further ~17x in cost, and by which fix applies: a payload
//! fix for the first, a propagation-topology fix for the second. The number
//! that separates them is
//!
//! ```text
//! sum(total_sends) / sum(applies_client_local_changed)
//! ```
//!
//! summed over the fleet as a RATIO OF SUMS, never a mean of per-window
//! ratios — most nodes serve no local clients, so a per-node ratio is `0/0`
//! most windows. `applies_network_relay_changed / applies_client_local_changed`
//! reads the re-broadcast amplification directly.
//!
//! ### What is deliberately NOT here
//!
//! #5062 also asks for this split PER CONTRACT. That is not in this rollup, on
//! volume grounds: a top-N array of per-contract origin counts roughly doubles
//! this event's contribution, on a collector already ingesting ~30 GB/day with
//! ~3-day retention, to refine a number nobody has read yet. Per-contract SENDS
//! already ship (`top_contracts_by_total_bytes`, #4979/#5055), so the moment
//! the node-level ratio says re-broadcast amplification is real, the
//! per-contract denominator is a small follow-up against a known-useful
//! measurement rather than a speculative one.
//!
//! ## Cost
//!
//! One short uncontended mutex acquire per **delivered broadcast** (not per
//! packet), covering a handful of integer adds and at most one bounded
//! `HashMap` touch, plus one more per completed update APPLY (strictly rarer
//! than sends, since one apply is what fans out to N peers). Everything else
//! happens in the aggregator task, and the hot path only ever WRITES. The lock
//! is what makes a rollup a consistent snapshot; see [`PayloadMix`] for why
//! per-field atomics were not enough.
//!
//! [`broadcast_to_single_peer`]: super::broadcast_queue::broadcast_to_single_peer

use std::collections::HashMap;
use std::time::Duration;

use freenet_stdlib::prelude::ContractInstanceId;
use parking_lot::Mutex;

use crate::node::background_task_monitor::BackgroundTaskMonitor;
use crate::ring::interest::SummaryMissingReason;
use crate::tracing::event_kind::{STATE_SIZE_BUCKET_COUNT, state_size_bucket};

/// Rollup cadence. Broadcasts are far less frequent than packets, so this is
/// a minute rather than the 1 Hz sampling the `shadow_demand` aggregators use;
/// one event per minute per node is a negligible addition to the telemetry
/// volume this is meant to explain.
const ROLLUP_WINDOW: Duration = Duration::from_secs(60);

/// Per-contract attribution cap for one window.
///
/// Bounded because the key is contract-controlled: any peer can fan out a
/// contract we host, so an unbounded map here would be an amplification
/// vector (see `.claude/rules/code-style.md`, "per-key collections"). Entries
/// beyond the cap are still counted in the per-arm totals; only their
/// per-contract attribution is dropped, and both the count and the BYTE total
/// of those unattributed sends are reported (`attribution_dropped_sends` /
/// `attribution_dropped_bytes`) so a capped window is never mistaken for a
/// complete one. Truncation of the reported top-N is a DIFFERENT omission,
/// published separately as `other_contracts_bytes`.
const MAX_TRACKED_CONTRACTS: usize = 256;

/// How many contracts the emitted rollup names.
const TOP_CONTRACTS_REPORTED: usize = 10;

/// Which arm of the fan-out's payload selection produced a broadcast.
///
/// See the module docs for what each arm means and why the split matters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum PayloadArm {
    /// A delta was computed and sent.
    Delta,
    /// Full state: the `delta_incompat` memo is armed for this contract.
    FullDeltaSuppressed,
    /// Full state: the computed delta was not smaller than the full state
    /// (post-#4923; previously: the pre-compute gate refused to compute one).
    FullNotEfficient,
    /// Full state: delta computation was attempted and failed.
    FullComputeFailed,
    /// Full state: **our own** summary is missing, so there was nothing to
    /// diff *from*. `get_contract_summary` returned `None` — the contract
    /// handler timed out (`BROADCAST_CH_TIMEOUT`, 10 s), errored, or answered
    /// unexpectedly. A load/contract problem, not a peer-tracking one.
    ///
    /// This arm still ships an EMPTY `sender_summary_bytes` on the wire (the
    /// field is `unwrap_or_default()` and cannot express "absent"), but that
    /// no longer poisons the receiver: both broadcast receive paths route
    /// through `update::op_ctx_task::seed_sender_summary_from_broadcast`,
    /// which refuses to cache an empty summary. The peer therefore keeps a
    /// truthful `None` (visible to `tracked_missing_*`) instead of a
    /// `Some(empty)` that looked healthy while being wrong.
    FullNoOurSummary,
    /// Full state: we have our summary but none for the peer, **and the peer
    /// has no `PeerInterest` entry for this contract at all**.
    ///
    /// This is the structural case. Broadcast targets are advertised co-hosts
    /// (`neighbor_hosting`), while the peer-summary cache lives in
    /// `InterestManager::interested_peers`; `update_peer_summary` is a silent
    /// no-op for a peer with no entry there. A target in this state can never
    /// acquire a cached summary from a delivery, so **every** broadcast to it
    /// is full state, permanently — the #4442 chicken-and-egg, re-opened for
    /// the advertised-co-host population by #4642 step 9.
    FullNoTheirSummaryUntracked,
    /// Full state: we have our summary, the peer IS tracked in
    /// `interested_peers`, but its cached summary is `None`.
    ///
    /// The recoverable case: a genuinely new peer, or one whose summary was
    /// cleared by a delta-apply failure / `ResyncRequest`. The next delivered
    /// broadcast caches a summary and the pair collapses to deltas.
    FullNoTheirSummaryTracked,
}

impl PayloadArm {
    /// Every arm, in reporting order. Exhaustive by construction: the
    /// `match` in [`PayloadArm::index`] fails to compile if a variant is
    /// added without being listed here.
    pub(crate) const ALL: [PayloadArm; 7] = [
        PayloadArm::Delta,
        PayloadArm::FullDeltaSuppressed,
        PayloadArm::FullNotEfficient,
        PayloadArm::FullComputeFailed,
        PayloadArm::FullNoOurSummary,
        PayloadArm::FullNoTheirSummaryUntracked,
        PayloadArm::FullNoTheirSummaryTracked,
    ];

    /// The three arms that together make up the legacy `full_no_summary`
    /// bucket. Published as an aggregate so dashboards written against the
    /// pre-split schema keep working; see [`payload_mix_json`].
    const NO_SUMMARY_SPLIT: [PayloadArm; 3] = [
        PayloadArm::FullNoOurSummary,
        PayloadArm::FullNoTheirSummaryUntracked,
        PayloadArm::FullNoTheirSummaryTracked,
    ];

    const COUNT: usize = Self::ALL.len();

    const fn index(self) -> usize {
        match self {
            PayloadArm::Delta => 0,
            PayloadArm::FullDeltaSuppressed => 1,
            PayloadArm::FullNotEfficient => 2,
            PayloadArm::FullComputeFailed => 3,
            PayloadArm::FullNoOurSummary => 4,
            PayloadArm::FullNoTheirSummaryUntracked => 5,
            PayloadArm::FullNoTheirSummaryTracked => 6,
        }
    }

    /// Stable wire label. Used as the telemetry field prefix, so changing one
    /// breaks existing dashboard queries.
    pub(crate) const fn label(self) -> &'static str {
        match self {
            PayloadArm::Delta => "delta",
            PayloadArm::FullDeltaSuppressed => "full_delta_suppressed",
            PayloadArm::FullNotEfficient => "full_not_efficient",
            PayloadArm::FullComputeFailed => "full_compute_failed",
            PayloadArm::FullNoOurSummary => "full_no_our_summary",
            PayloadArm::FullNoTheirSummaryUntracked => "full_no_their_summary_untracked",
            PayloadArm::FullNoTheirSummaryTracked => "full_no_their_summary_tracked",
        }
    }

    /// Whether this arm put a whole contract state on the wire.
    pub(crate) const fn is_full_state(self) -> bool {
        !matches!(self, PayloadArm::Delta)
    }
}

/// Where an update this node APPLIED came from — the denominator half of the
/// fan-out multiplier (#5062).
///
/// ## Why this is the missing measurement
///
/// The sender-side arm counters above say how many payloads this node put on
/// the wire, and #5091's receiver-side counters say how few of them changed
/// anything (fleet-measured at 17.5 payloads delivered per one that changes
/// state). Neither can separate the two topologies that produce that number
/// and have completely different remedies:
///
///   * one node fans out to its ~17 co-hosts, and
///   * ~17 nodes EACH re-fan-out to their own ~17 co-hosts.
///
/// They differ by a further ~17x in cost. Splitting applies by origin is what
/// tells them apart: under the first, essentially every broadcast a node emits
/// follows a locally-originated apply; under the second, all but a ~1/18th
/// slice follow a RELAYED apply that this node then re-broadcast.
///
/// ## Why the counter can be taken here at all
///
/// `Executor::bridged_upsert_contract_state_inner` calls `commit_state_update`
/// — the only production emitter of `NodeEvent::BroadcastStateChange` on the
/// UPDATE path — inside the `updated_state != current_state` branch, and
/// returns `UpsertResult::NoChange` otherwise. So a `changed == true` apply is
/// a 1:1 correlate of a broadcast fan-out being emitted, and no provenance
/// plumbing through the `ContractExecutor` trait is needed to attribute it:
/// `update_contract` already receives the origin as a
/// [`Priority`](crate::contract::Priority) and already learns `changed` from
/// the handler's reply.
///
/// The one gap in that correspondence is the broken-invariant suppression
/// (`ring::broken_invariants`), which can swallow a commit that this counter
/// still counts as changed. It is rare, deliberate, and already separately
/// observable, so it is left as a documented residual rather than a second
/// counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ApplyOrigin {
    /// Originated by a WS/HTTP client connected to THIS node — the true
    /// denominator of "sends per originated update".
    ClientLocal,
    /// Applied on behalf of a remote peer's relayed operation. A `changed`
    /// apply here is a RE-broadcast: the amplification #5062 exists to size.
    NetworkRelay,
    /// Node-internal background work. No `update_contract` call site produces
    /// this today; it is carried so that one appearing later shows up as its
    /// own number instead of silently inflating the relay arm.
    Background,
}

impl ApplyOrigin {
    /// Every origin, in reporting order.
    pub(crate) const ALL: [ApplyOrigin; 3] = [
        ApplyOrigin::ClientLocal,
        ApplyOrigin::NetworkRelay,
        ApplyOrigin::Background,
    ];

    pub(crate) const COUNT: usize = Self::ALL.len();

    const fn index(self) -> usize {
        match self {
            ApplyOrigin::ClientLocal => 0,
            ApplyOrigin::NetworkRelay => 1,
            ApplyOrigin::Background => 2,
        }
    }

    /// Stable wire label. Used as a telemetry field infix, so changing one
    /// breaks existing dashboard queries.
    pub(crate) const fn label(self) -> &'static str {
        match self {
            ApplyOrigin::ClientLocal => "client_local",
            ApplyOrigin::NetworkRelay => "network_relay",
            ApplyOrigin::Background => "background",
        }
    }

    /// Read the origin off the scheduling class the apply was queued under.
    ///
    /// [`Priority`](crate::contract::Priority) is a scheduling class that
    /// happens to be defined on exactly this axis, and all eight production
    /// `update_contract` call sites tag it explicitly and correctly today
    /// (three `ClientLocal`, five `NetworkRelay`). Reusing it is what makes
    /// this measurement a one-site change instead of a new parameter threaded
    /// through the `ContractExecutor` trait and ~160 call sites.
    ///
    /// The risk it carries, stated so a future reader does not have to
    /// rediscover it: `Priority::DEFAULT` is `NetworkRelay`, so a NEW call
    /// site that forgets to tag itself reads as "relayed" rather than as
    /// something unclassified. That biases the multiplier's denominator DOWN
    /// and the amplification numerator UP — i.e. it can only ever make the
    /// fan-out problem look worse than it is, never better, which is the safe
    /// direction for a measurement whose purpose is to decide whether to spend
    /// engineering effort on propagation topology. A new client-originated
    /// path that lands untagged is the case to watch for.
    pub(crate) const fn from_priority(priority: crate::contract::Priority) -> Self {
        match priority {
            crate::contract::Priority::ClientLocal => ApplyOrigin::ClientLocal,
            crate::contract::Priority::NetworkRelay => ApplyOrigin::NetworkRelay,
            crate::contract::Priority::Background => ApplyOrigin::Background,
        }
    }
}

/// One rollup window's counters.
///
/// Every field advances together under a single lock so a rollup takes a
/// consistent snapshot; see [`PayloadMix`].
struct Window {
    sends: [u64; PayloadArm::COUNT],
    bytes: [u64; PayloadArm::COUNT],
    /// Per-contract full-state byte attribution, bounded at
    /// [`MAX_TRACKED_CONTRACTS`].
    contract_full_state_bytes: HashMap<ContractInstanceId, u64>,
    /// Per-contract TOTAL broadcast bytes and sends, across EVERY arm.
    ///
    /// #4979: `contract_full_state_bytes` is written only under
    /// `arm.is_full_state()`, so it is a numerator with no denominator — it can
    /// say a contract emitted N full-state bytes but not what share of that
    /// contract's traffic those were, and it is blind to a contract whose cost
    /// is entirely in the `delta` arm. That blind spot is not hypothetical:
    /// #5056 found a contract at 55.6% of all broadcast sends whose "deltas"
    /// are full-state-sized, and attributing it needed a natural experiment
    /// over single-contract peers because no counter could answer directly.
    ///
    /// Also the input a per-contract outbound budget would be sized from
    /// (#5057): the ceiling should be chosen from the observed distribution,
    /// not guessed. A distribution silently missing its tail is exactly the
    /// wrong failure for that, which is why the cap overflow gets its own
    /// counters ([`Window::total_attribution_dropped_sends`]) rather than
    /// sharing the full-state ones.
    contract_total: HashMap<ContractInstanceId, (u64, u64)>,
    /// Sends [`MAX_TRACKED_CONTRACTS`] refused to admit to `contract_total`,
    /// and the bytes behind them. Counts SENDS, not distinct contracts — same
    /// reading as [`Window::attribution_dropped_sends`].
    ///
    /// Deliberately SEPARATE from those full-state counters. `contract_total`
    /// is written on EVERY arm, so it accumulates keys strictly faster than
    /// `contract_full_state_bytes` and can reach the cap while the full-state
    /// map still admits. Two consequences the schema has to be able to state:
    /// a `Delta`-only contract's drop is invisible to `attribution_dropped_*`
    /// (that counter is only written under `arm.is_full_state()`), and a
    /// contract admitted to the full-state map but refused here has full-state
    /// bytes with no total entry — a denominator smaller than its own
    /// numerator. Sharing one pair of counters would leave both unreadable.
    total_attribution_dropped_sends: u64,
    total_attribution_dropped_bytes: u64,
    /// Per-contract bytes for the [`PayloadArm::FullNotEfficient`] arm ONLY.
    ///
    /// #4956: the aggregate gate-input ratio came back at 1.000 (summary size
    /// == state size, max 839 KB each), which means some contract is feeding
    /// state-sized bytes in as a "summary". `contract_full_state_bytes` mixes
    /// every full-state arm together, so it cannot say WHICH contract, and the
    /// culprit stayed unidentifiable. This narrows it to the one arm that
    /// matters.
    contract_not_efficient_bytes: HashMap<ContractInstanceId, u64>,
    /// Full-state sends that could not be attributed to a contract because
    /// the cap was already reached. This counts SENDS, not distinct
    /// contracts: one over-cap contract broadcasting 1,000 times contributes
    /// 1,000. Naming it for what it counts avoids the "1,000 contracts were
    /// dropped" misreading; distinct-contract cardinality would need an
    /// unbounded set, which is exactly what the cap exists to prevent.
    attribution_dropped_sends: u64,
    /// Bytes behind those unattributed sends.
    ///
    /// This covers only contracts the cap refused to TRACK. Contracts that
    /// were tracked but fell outside the reported top-N are a separate
    /// omission, published as `other_contracts_bytes`; conflating the two made
    /// an 11-contract window look perfectly reconciled while 10 % of its bytes
    /// were unaccounted for.
    attribution_dropped_bytes: u64,
    /// The efficiency gate's observed INPUTS, summed and maxed over the
    /// window's [`PayloadArm::FullNotEfficient`] sends.
    ///
    /// `DeltaUnavailable::NotEfficient` has always carried `summary_size` and
    /// `state_size`, but its only consumer was a `tracing::debug!`, which is
    /// compiled out in release (`max_level_info`) — so in production the gate
    /// refused deltas with nobody able to see on what. Both sizes are the
    /// real observed values at refusal time.
    ///
    /// Semantics shifted with #4923 and the split is exactly what these
    /// numbers field-validate. PRE-#4923 a refusal fired when
    /// `summary * 2 >= state` without computing anything, so the ratio
    /// restated the trigger. POST-#4923 a refusal means the contract's
    /// COMPUTED delta was not smaller than the state, so `FullNotEfficient`
    /// sends should collapse to the rare genuinely-incompressible cases —
    /// and the summary:state ratio now tells whether the OLD proxy would
    /// have refused sends the new gate happily serves as deltas (the
    /// wire-vs-CPU inversion the fix removes).
    ///
    /// Sum + max rather than a histogram: the mean ratio answers "were these
    /// genuinely summary-heavy contracts", and the maxima bound the worst
    /// case, at four `u64`s instead of a bucket array.
    not_efficient_summary_bytes_sum: u64,
    not_efficient_state_bytes_sum: u64,
    not_efficient_summary_bytes_max: u64,
    not_efficient_state_bytes_max: u64,

    /// Why the peer had no cached summary, for the
    /// [`PayloadArm::FullNoTheirSummaryTracked`] arm only.
    ///
    /// That arm was 26.9% of broadcast bytes at a 357 KB mean on the aged
    /// 0.2.109 fleet — the largest remaining arm — but "the peer is tracked
    /// and has no summary" has three causes with three different fixes
    /// (never seeded / cleared by the peer's own `None` report / cleared by a
    /// resync or delta-apply failure), and the rollup could not tell them
    /// apart. These counters split it. Indexed by
    /// [`SummaryMissingReason::index`]; sums to the `tracked` arm's totals.
    tracked_missing_sends: [u64; SummaryMissingReason::ALL.len()],
    tracked_missing_bytes: [u64; SummaryMissingReason::ALL.len()],

    /// Update applies this node completed, by [`ApplyOrigin`] — the
    /// denominator of the fan-out multiplier (#5062).
    ///
    /// Indexed `[origin][changed as usize]`, so `[o][1]` counts the applies
    /// that actually mutated state (and therefore emitted a broadcast) and
    /// `[o][0] + [o][1]` is every completed apply for that origin.
    ///
    /// Deliberately recorded into the SAME window as the `sends` counters
    /// above and drained by the same atomic take, so a rollup's sends and its
    /// applies describe the same 60 s of work and the ratio between them is
    /// meaningful rather than smeared across window boundaries.
    ///
    /// Fixed cardinality (3 x 2 `u64`s), no per-contract map: see the module
    /// docs for why the per-contract refinement is deliberately not here.
    applies: [[u64; 2]; ApplyOrigin::COUNT],
}

impl Default for Window {
    fn default() -> Self {
        Self {
            sends: [0; PayloadArm::COUNT],
            bytes: [0; PayloadArm::COUNT],
            contract_full_state_bytes: HashMap::new(),
            contract_total: HashMap::new(),
            total_attribution_dropped_sends: 0,
            total_attribution_dropped_bytes: 0,
            contract_not_efficient_bytes: HashMap::new(),
            attribution_dropped_sends: 0,
            attribution_dropped_bytes: 0,
            not_efficient_summary_bytes_sum: 0,
            not_efficient_state_bytes_sum: 0,
            not_efficient_summary_bytes_max: 0,
            not_efficient_state_bytes_max: 0,
            tracked_missing_sends: [0; SummaryMissingReason::ALL.len()],
            tracked_missing_bytes: [0; SummaryMissingReason::ALL.len()],
            applies: [[0; 2]; ApplyOrigin::COUNT],
        }
    }
}

/// Per-arm send/byte counters plus bounded per-contract full-state
/// attribution.
///
/// ## One instance PER NODE, never a process global
///
/// This is deliberately owned by [`OpManager`](crate::node::OpManager) rather
/// than living in a `static`. A process-global accumulator drained by
/// per-node aggregators is actively wrong when several `NodeP2P` instances
/// share a process (the simulation harness): whichever node's ticker fires
/// first would drain everyone's records and publish them under its own
/// `local_peer_id`, while the other nodes emitted empty windows.
///
/// Note the sibling `shadow_demand` aggregators DO read process-global
/// counters, but non-destructively (load + a locally-tracked delta), so N of
/// them merely double-report. This accumulator is drained destructively, which
/// turns the same shape into misattribution — so it does not follow that
/// convention.
///
/// Keeping state on the struct also means tests instantiate an isolated
/// accumulator instead of sharing one, so they are not order-dependent.
///
/// ## Why one mutex rather than per-field atomics
///
/// The first version used relaxed atomics plus a `DashMap`. External review
/// caught two defects that are both really the same defect: a rollup drained
/// the arm counters and the contract map at different instants, so (a) an
/// increment landing between sampling a map entry and clearing it was lost
/// outright, and (b) a single broadcast could be counted in one window for
/// the aggregate fields and the next for the per-contract fields, leaving
/// `top_contracts_by_full_state_bytes` unable to reconcile against
/// `full_state_bytes`. For an accuracy-measurement feature that is a real
/// bug, not a rounding detail.
///
/// A single lock covering the whole window makes record and drain atomic
/// with respect to each other, which is the property the measurement needs.
/// The cost is acceptable because this is per *delivered broadcast*, not per
/// packet: an uncontended `parking_lot::Mutex` acquire is on the order of a
/// few atomic operations, and the surrounding send path has already done WASM
/// delta computation and a network write. Note this is the documented
/// exception in `.claude/rules/code-style.md` to preferring `DashMap` — we
/// need an atomic read-modify-write across MULTIPLE keys (all arms plus the
/// contract map) in one transaction, which is precisely when a global lock is
/// required.
pub(crate) struct PayloadMix {
    window: Mutex<Window>,
    /// Cumulative receiver-side outcomes. Unlike `window`, this is never
    /// drained: the existing router snapshot can recover the full monotonic
    /// value after a locally dropped telemetry sample (#5090).
    receiver_applies: Mutex<ReceiverApplyStats>,
}

/// Fixed receiver outcome axes. There are deliberately no peer or contract
/// identifiers here: each successful merge selects exactly one of four arms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReceiverApplyClass {
    DeltaChanged,
    DeltaNoOp,
    FullChanged,
    FullNoOp,
}

impl ReceiverApplyClass {
    pub(crate) const ALL: [Self; 4] = [
        Self::DeltaChanged,
        Self::DeltaNoOp,
        Self::FullChanged,
        Self::FullNoOp,
    ];
    pub(crate) const COUNT: usize = Self::ALL.len();

    pub(crate) const fn index(self) -> usize {
        match self {
            Self::DeltaChanged => 0,
            Self::DeltaNoOp => 1,
            Self::FullChanged => 2,
            Self::FullNoOp => 3,
        }
    }

    pub(crate) const fn from_apply(is_delta: bool, changed: bool) -> Self {
        match (is_delta, changed) {
            (true, true) => Self::DeltaChanged,
            (true, false) => Self::DeltaNoOp,
            (false, true) => Self::FullChanged,
            (false, false) => Self::FullNoOp,
        }
    }
}

/// Monotonic, fixed-cardinality receiver-side apply totals. The second array
/// dimension uses the shared state-size histogram taxonomy from `event_kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct ReceiverApplyStats {
    pub(crate) counts: [[u64; STATE_SIZE_BUCKET_COUNT]; ReceiverApplyClass::COUNT],
    /// Delta/full × terminal outcome × incoming-payload size, flattened with
    /// delta outcomes first. Within each kind: changed, no-op, dedup, backoff,
    /// failed. Keeping the dimensions joint is what lets telemetry answer
    /// whether a 49 MiB no-op was a full state or an oversized delta.
    pub(crate) terminal_counts: [[u64; STATE_SIZE_BUCKET_COUNT]; 10],
    pub(crate) terminal_bytes: [[u64; STATE_SIZE_BUCKET_COUNT]; 10],
}

#[derive(Clone, Copy)]
enum ReceiverTerminalOutcome {
    Changed,
    NoOp,
    Dedup,
    Backoff,
    Failed,
}

impl ReceiverTerminalOutcome {
    const fn index(self) -> usize {
        self as usize
    }
}

pub(crate) struct ReceiverTerminalGuard<'a> {
    mix: &'a PayloadMix,
    is_delta: bool,
    payload_bytes: usize,
    outcome: ReceiverTerminalOutcome,
}

impl ReceiverTerminalGuard<'_> {
    pub(crate) fn mark_dedup(&mut self) {
        self.outcome = ReceiverTerminalOutcome::Dedup;
    }

    pub(crate) fn mark_backoff(&mut self) {
        self.outcome = ReceiverTerminalOutcome::Backoff;
    }

    pub(crate) fn mark_applied(&mut self, changed: bool, state_size: usize) {
        self.mix
            .record_receiver_apply(self.is_delta, changed, state_size);
        self.outcome = if changed {
            ReceiverTerminalOutcome::Changed
        } else {
            ReceiverTerminalOutcome::NoOp
        };
    }
}

impl Drop for ReceiverTerminalGuard<'_> {
    fn drop(&mut self) {
        self.mix
            .record_receiver_terminal(self.is_delta, self.outcome, self.payload_bytes);
    }
}

impl PayloadMix {
    pub(crate) fn new() -> Self {
        Self {
            window: Mutex::new(Window::default()),
            receiver_applies: Mutex::new(ReceiverApplyStats::default()),
        }
    }

    fn record_receiver_apply(&self, is_delta: bool, changed: bool, state_size: usize) {
        let class = ReceiverApplyClass::from_apply(is_delta, changed).index();
        let bucket = state_size_bucket(state_size as u64);
        let mut stats = self.receiver_applies.lock();
        stats.counts[class][bucket] = stats.counts[class][bucket].saturating_add(1);
    }

    pub(crate) fn receiver_terminal_guard(
        &self,
        is_delta: bool,
        payload_bytes: usize,
    ) -> ReceiverTerminalGuard<'_> {
        ReceiverTerminalGuard {
            mix: self,
            is_delta,
            payload_bytes,
            outcome: ReceiverTerminalOutcome::Failed,
        }
    }

    fn record_receiver_terminal(
        &self,
        is_delta: bool,
        outcome: ReceiverTerminalOutcome,
        payload_bytes: usize,
    ) {
        let outcome_index = outcome.index();
        let kind_index = usize::from(!is_delta) * 5 + outcome_index;
        let bucket = state_size_bucket(payload_bytes as u64);
        let bytes = u64::try_from(payload_bytes).unwrap_or(u64::MAX);
        let mut stats = self.receiver_applies.lock();
        stats.terminal_counts[kind_index][bucket] =
            stats.terminal_counts[kind_index][bucket].saturating_add(1);
        stats.terminal_bytes[kind_index][bucket] =
            stats.terminal_bytes[kind_index][bucket].saturating_add(bytes);
    }

    /// Read cumulative receiver totals without resetting them.
    pub(crate) fn receiver_apply_stats(&self) -> ReceiverApplyStats {
        *self.receiver_applies.lock()
    }

    /// Record one **delivered** broadcast.
    ///
    /// Call this only where [`ResourceType::BroadcastFanoutCost`][rt] is
    /// charged, so the mix and the cost axis agree on what "sent" means.
    ///
    /// [rt]: crate::topology::meter::ResourceType::BroadcastFanoutCost
    /// `gate_inputs` carries the `(summary_size, state_size)` the
    /// wire-efficiency gate actually refused on. It is `Some` exactly when
    /// `arm` is [`PayloadArm::FullNotEfficient`] — the only arm for which the
    /// gate ran — and ignored otherwise, so a mis-paired call cannot corrupt
    /// the ratio.
    ///
    /// `missing_reason` carries why the peer had no cached summary. It is
    /// `Some` exactly when `arm` is
    /// [`PayloadArm::FullNoTheirSummaryTracked`] — the only arm for which a
    /// tracked entry with an absent summary exists to read — and ignored
    /// otherwise, same discipline as `gate_inputs`.
    pub(crate) fn record_delivered(
        &self,
        arm: PayloadArm,
        contract: &ContractInstanceId,
        payload_bytes: usize,
        gate_inputs: Option<(usize, usize)>,
        missing_reason: Option<SummaryMissingReason>,
    ) {
        let bytes = payload_bytes as u64;
        let idx = arm.index();
        let mut w = self.window.lock();
        if arm == PayloadArm::FullNoTheirSummaryTracked {
            if let Some(reason) = missing_reason {
                // Fallible indexing, NOT `[r]`. Adding a variant to
                // `SummaryMissingReason` forces `index()` and `as_str()` to be
                // updated (both are exhaustive matches) but does NOT force
                // `ALL` to be extended, and `ALL.len()` sizes these arrays. So
                // the plausible sequence "add variant, fix the two compile
                // errors, forget ALL" yields an out-of-range index — which as
                // `[r]` would PANIC inside a held mutex on the broadcast send
                // path. Degrading to the unattributed residual instead is
                // strictly better: the miss stays visible (the per-reason sums
                // no longer cover the arm total) and no send path dies for a
                // telemetry bookkeeping slip.
                // One bounds check covers both arrays: they are declared with
                // the same `ALL.len()` const expression, so they cannot differ.
                let r = reason.index();
                if r < w.tracked_missing_sends.len() {
                    w.tracked_missing_sends[r] = w.tracked_missing_sends[r].saturating_add(1);
                    w.tracked_missing_bytes[r] = w.tracked_missing_bytes[r].saturating_add(bytes);
                }
            }
        }
        if arm == PayloadArm::FullNotEfficient {
            if let Some((summary_size, state_size)) = gate_inputs {
                let (s, st) = (summary_size as u64, state_size as u64);
                w.not_efficient_summary_bytes_sum =
                    w.not_efficient_summary_bytes_sum.saturating_add(s);
                w.not_efficient_state_bytes_sum =
                    w.not_efficient_state_bytes_sum.saturating_add(st);
                w.not_efficient_summary_bytes_max = w.not_efficient_summary_bytes_max.max(s);
                w.not_efficient_state_bytes_max = w.not_efficient_state_bytes_max.max(st);
            }
        }
        // Saturating throughout: a wrapped counter would silently report a
        // tiny number for the heaviest contract, which is the opposite of
        // what this measurement is for.
        w.sends[idx] = w.sends[idx].saturating_add(1);
        w.bytes[idx] = w.bytes[idx].saturating_add(bytes);
        if arm == PayloadArm::FullNotEfficient {
            // Same cap discipline as the wider map: the key is
            // contract-controlled, so it must not grow unbounded. Overflow is
            // covered by the existing attribution_dropped_* counters below.
            if let Some(tally) = w.contract_not_efficient_bytes.get_mut(contract) {
                *tally = tally.saturating_add(bytes);
            } else if w.contract_not_efficient_bytes.len() < MAX_TRACKED_CONTRACTS {
                w.contract_not_efficient_bytes.insert(*contract, bytes);
            }
        }
        // EVERY arm, not just full-state: see `contract_total`'s docs. Same cap
        // discipline as the other maps — the key is contract-controlled — but
        // its OWN overflow counters, because this map fills faster than the
        // full-state one and a `Delta`-only drop never reaches the
        // `attribution_dropped_*` branch below.
        if let Some(tally) = w.contract_total.get_mut(contract) {
            tally.0 = tally.0.saturating_add(1);
            tally.1 = tally.1.saturating_add(bytes);
        } else if w.contract_total.len() < MAX_TRACKED_CONTRACTS {
            w.contract_total.insert(*contract, (1, bytes));
        } else {
            w.total_attribution_dropped_sends = w.total_attribution_dropped_sends.saturating_add(1);
            w.total_attribution_dropped_bytes =
                w.total_attribution_dropped_bytes.saturating_add(bytes);
        }
        if arm.is_full_state() {
            if let Some(tally) = w.contract_full_state_bytes.get_mut(contract) {
                *tally = tally.saturating_add(bytes);
            } else if w.contract_full_state_bytes.len() < MAX_TRACKED_CONTRACTS {
                w.contract_full_state_bytes.insert(*contract, bytes);
            } else {
                w.attribution_dropped_sends = w.attribution_dropped_sends.saturating_add(1);
                w.attribution_dropped_bytes = w.attribution_dropped_bytes.saturating_add(bytes);
            }
        }
    }

    /// Record one COMPLETED update apply and where it came from (#5062).
    ///
    /// `changed` must be the contract handler's own verdict on whether the
    /// merge mutated state, because that is precisely the condition under
    /// which the executor emits a broadcast fan-out — see [`ApplyOrigin`].
    ///
    /// Call this only on a terminal apply outcome (merged, or merged-to-no-
    /// change). Applies that ERRORED are deliberately excluded: nothing was
    /// committed and nothing was broadcast, so counting them would pad the
    /// denominator of a ratio whose whole purpose is to be compared against
    /// sends. Failures are already published receiver-side by #5091's
    /// `*_failed` terminal counters.
    /// [`Self::record_apply`], reading the origin off the scheduling class the
    /// apply was queued under.
    ///
    /// This is the form production calls. It exists so `operations::update`
    /// never has to name [`ApplyOrigin`] — `node::network_bridge` is private
    /// outside `node`, and more importantly the apply path has no business
    /// knowing the telemetry taxonomy. The `Priority` -> origin
    /// reinterpretation stays in one documented place; see
    /// [`ApplyOrigin::from_priority`] for the aliasing risk it carries.
    pub(crate) fn record_apply_with_priority(
        &self,
        priority: crate::contract::Priority,
        changed: bool,
    ) {
        self.record_apply(ApplyOrigin::from_priority(priority), changed);
    }

    pub(crate) fn record_apply(&self, origin: ApplyOrigin, changed: bool) {
        let idx = origin.index();
        let changed_idx = usize::from(changed);
        let mut w = self.window.lock();
        // Saturating for the same reason as the send counters: a wrapped
        // denominator would report an absurd multiplier rather than an
        // obviously-clamped one.
        w.applies[idx][changed_idx] = w.applies[idx][changed_idx].saturating_add(1);
    }

    /// Atomically take the current window, leaving a fresh empty one.
    ///
    /// One lock acquisition covers every field, so the aggregate counters and
    /// the per-contract tallies always describe the same set of broadcasts.
    fn take_window(&self) -> Window {
        std::mem::take(&mut *self.window.lock())
    }
}

impl Window {
    /// Per-arm `(sends, bytes)` in [`PayloadArm::ALL`] order.
    fn arms(&self) -> Vec<(PayloadArm, u64, u64)> {
        PayloadArm::ALL
            .iter()
            .map(|arm| {
                let idx = arm.index();
                (*arm, self.sends[idx], self.bytes[idx])
            })
            .collect()
    }

    /// Per-origin `(changed, total)` applies in [`ApplyOrigin::ALL`] order.
    fn applies(&self) -> Vec<(ApplyOrigin, u64, u64)> {
        ApplyOrigin::ALL
            .iter()
            .map(|origin| {
                let counts = self.applies[origin.index()];
                // `total` is emitted rather than `unchanged` so the ratio's
                // denominator is present as a published field instead of
                // something a consumer has to reconstruct by addition.
                (*origin, counts[1], counts[0].saturating_add(counts[1]))
            })
            .collect()
    }

    /// The wire-efficiency gate's inputs for this window.
    fn gate_stats(&self) -> NotEfficientGateStats {
        NotEfficientGateStats {
            summary_bytes_sum: self.not_efficient_summary_bytes_sum,
            state_bytes_sum: self.not_efficient_state_bytes_sum,
            summary_bytes_max: self.not_efficient_summary_bytes_max,
            state_bytes_max: self.not_efficient_state_bytes_max,
        }
    }

    /// Per-reason `(sends, bytes)` for the tracked-but-summaryless arm, in
    /// [`SummaryMissingReason::ALL`] order.
    fn tracked_missing(&self) -> Vec<(SummaryMissingReason, u64, u64)> {
        SummaryMissingReason::ALL
            .iter()
            .map(|reason| {
                let idx = reason.index();
                (
                    *reason,
                    self.tracked_missing_sends[idx],
                    self.tracked_missing_bytes[idx],
                )
            })
            .collect()
    }

    /// The top [`TOP_CONTRACTS_REPORTED`] contracts in the
    /// [`PayloadArm::FullNotEfficient`] arm, i.e. the ones whose delta the
    /// gate refused. Same stable ordering as [`Self::top_contracts`].
    fn top_not_efficient_contracts(&self) -> Vec<(ContractInstanceId, u64)> {
        let mut tallies: Vec<(ContractInstanceId, u64)> = self
            .contract_not_efficient_bytes
            .iter()
            .map(|(k, v)| (*k, *v))
            .collect();
        tallies.sort_by(|a, b| {
            b.1.cmp(&a.1)
                .then_with(|| a.0.as_bytes().cmp(b.0.as_bytes()))
        });
        tallies.truncate(TOP_CONTRACTS_REPORTED);
        tallies
    }

    /// The top [`TOP_CONTRACTS_REPORTED`] contracts by TOTAL broadcast bytes,
    /// with their send counts. Ranked by bytes, since that is the axis a
    /// budget would bound.
    fn top_contracts_total(&self) -> Vec<(ContractInstanceId, u64, u64)> {
        let mut tallies: Vec<(ContractInstanceId, u64, u64)> = self
            .contract_total
            .iter()
            .map(|(k, (sends, bytes))| (*k, *sends, *bytes))
            .collect();
        // Tie-break on raw bytes for the same reason as `top_contracts`: a
        // reported top-N that reorders on ties looks like churn.
        tallies.sort_by(|a, b| {
            b.2.cmp(&a.2)
                .then_with(|| a.0.as_bytes().cmp(b.0.as_bytes()))
        });
        tallies.truncate(TOP_CONTRACTS_REPORTED);
        tallies
    }

    /// Everything the published schema needs about `contract_total`: the
    /// reported top-N, how many distinct contracts were tracked, and what the
    /// cap refused.
    ///
    /// Bundled rather than four more positional `u64` parameters on
    /// [`payload_mix_json`], which already carries an adjacent run of them —
    /// a transposed pair there would misreport silently and reconcile fine.
    fn total_attribution(&self) -> TotalAttribution {
        TotalAttribution {
            contracts: self.top_contracts_total(),
            contracts_tracked: self.contract_total.len() as u64,
            dropped_sends: self.total_attribution_dropped_sends,
            dropped_bytes: self.total_attribution_dropped_bytes,
        }
    }

    /// The top [`TOP_CONTRACTS_REPORTED`] contracts by full-state bytes.
    fn top_contracts(&self) -> Vec<(ContractInstanceId, u64)> {
        let mut tallies: Vec<(ContractInstanceId, u64)> = self
            .contract_full_state_bytes
            .iter()
            .map(|(k, v)| (*k, *v))
            .collect();
        // `ContractInstanceId` has no `Ord`, so tie-break on its raw bytes: the
        // reported top-N must be stable across nodes and windows, otherwise a tie
        // reorders on every rollup and looks like churn.
        tallies.sort_by(|a, b| {
            b.1.cmp(&a.1)
                .then_with(|| a.0.as_bytes().cmp(b.0.as_bytes()))
        });
        tallies.truncate(TOP_CONTRACTS_REPORTED);
        tallies
    }
}

/// The per-contract TOTAL attribution over one window, across every arm.
///
/// See [`Window::contract_total`] for why this exists at all, and
/// [`Window::total_attribution_dropped_sends`] for why its cap overflow is
/// counted separately from the full-state map's.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct TotalAttribution {
    /// Top [`TOP_CONTRACTS_REPORTED`] as `(contract, sends, bytes)`.
    contracts: Vec<(ContractInstanceId, u64, u64)>,
    /// Distinct contracts the window attributed, bounded by
    /// [`MAX_TRACKED_CONTRACTS`].
    contracts_tracked: u64,
    dropped_sends: u64,
    dropped_bytes: u64,
}

/// The wire-efficiency gate's inputs over one window, for the
/// [`PayloadArm::FullNotEfficient`] sends only.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct NotEfficientGateStats {
    summary_bytes_sum: u64,
    state_bytes_sum: u64,
    summary_bytes_max: u64,
    state_bytes_max: u64,
}

/// Build the `broadcast_payload_mix` rollup JSON.
///
/// Pure so the schema is unit-testable without the telemetry sender, matching
/// the `shadow_demand` rollup builders.
// Flat parameters rather than a `&Window`: the tests build `arms` and the
// attribution totals independently to exercise reconciliation edge cases
// (truncation vs over-cap drops) that a real `Window` cannot easily be coaxed
// into, so taking the struct would make the schema harder to test, not easier.
// The `contract_total` group is the exception — it is bundled in
// [`TotalAttribution`] so its three numbers cannot be transposed against the
// full-state run that follows them.
#[allow(clippy::too_many_arguments)]
fn payload_mix_json(
    arms: &[(PayloadArm, u64, u64)],
    contracts: &[(ContractInstanceId, u64)],
    not_efficient_contracts: &[(ContractInstanceId, u64)],
    total: &TotalAttribution,
    tracked_full_state_bytes: u64,
    contracts_tracked: u64,
    attribution_dropped_sends: u64,
    attribution_dropped_bytes: u64,
    gate: NotEfficientGateStats,
    tracked_missing: &[(SummaryMissingReason, u64, u64)],
    applies: &[(ApplyOrigin, u64, u64)],
    window_secs: u64,
) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    let mut total_sends = 0u64;
    let mut total_bytes = 0u64;
    let mut full_state_bytes = 0u64;
    for (arm, sends, bytes) in arms {
        obj.insert(format!("{}_sends", arm.label()), (*sends).into());
        obj.insert(format!("{}_bytes", arm.label()), (*bytes).into());
        total_sends += sends;
        total_bytes += bytes;
        if arm.is_full_state() {
            full_state_bytes += bytes;
        }
    }
    // Back-compat aggregate: `full_no_summary_*` was a single arm before the
    // three-way split, and the production dashboards / analysis scripts query
    // it by name. Republishing the sum keeps those working while the split
    // fields answer WHICH of the three causes is responsible. Derived from the
    // same window, so the two can never disagree.
    let no_summary_sends: u64 = arms
        .iter()
        .filter(|(arm, _, _)| PayloadArm::NO_SUMMARY_SPLIT.contains(arm))
        .map(|(_, sends, _)| *sends)
        .sum();
    let no_summary_bytes: u64 = arms
        .iter()
        .filter(|(arm, _, _)| PayloadArm::NO_SUMMARY_SPLIT.contains(arm))
        .map(|(_, _, bytes)| *bytes)
        .sum();
    obj.insert("full_no_summary_sends".into(), no_summary_sends.into());
    obj.insert("full_no_summary_bytes".into(), no_summary_bytes.into());

    // The gate's own inputs, so `full_not_efficient` stops being a black box.
    // Zero sends means the sums are vacuous, so publish them as null rather
    // than a misleading 0.0 mean ratio.
    obj.insert(
        "not_efficient_summary_bytes_sum".into(),
        gate.summary_bytes_sum.into(),
    );
    obj.insert(
        "not_efficient_state_bytes_sum".into(),
        gate.state_bytes_sum.into(),
    );
    obj.insert(
        "not_efficient_summary_bytes_max".into(),
        gate.summary_bytes_max.into(),
    );
    obj.insert(
        "not_efficient_state_bytes_max".into(),
        gate.state_bytes_max.into(),
    );
    // Ratio-of-SUMS (aggregate summary bytes / aggregate state bytes), NOT a
    // mean of per-send ratios — the two diverge under mixed contract sizes,
    // and the aggregate form is the one that answers "were the refused bytes
    // refused on oversized inputs" (poisoned pairs read ~1.0; an honest fleet
    // reads well under 0.5). Named for what it computes.
    obj.insert(
        "not_efficient_summary_to_state_bytes_ratio".into(),
        if gate.state_bytes_sum == 0 {
            serde_json::Value::Null
        } else {
            serde_json::Number::from_f64(
                gate.summary_bytes_sum as f64 / gate.state_bytes_sum as f64,
            )
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null)
        },
    );

    // #4961: split the tracked-but-summaryless arm by WHY the summary is
    // absent. The arm was the largest remaining bandwidth consumer (26.9% of
    // broadcast bytes at a 357 KB mean on the aged 0.2.109 fleet) and the top
    // suspect for the 4-20s room propagation latency, but its three causes
    // have three different fixes and were indistinguishable in the rollup.
    let mut reason_sends = 0u64;
    let mut reason_bytes = 0u64;
    for (reason, sends, bytes) in tracked_missing {
        obj.insert(
            format!("tracked_missing_{}_sends", reason.as_str()),
            (*sends).into(),
        );
        obj.insert(
            format!("tracked_missing_{}_bytes", reason.as_str()),
            (*bytes).into(),
        );
        reason_sends += sends;
        reason_bytes += bytes;
    }
    // Reconciliation: the per-reason counters must account for the whole
    // `full_no_their_summary_tracked` arm. A non-zero residual means a send
    // reached that arm without a reason — publish it rather than let the
    // split quietly under-count and mis-aim the fix, which is exactly the
    // failure mode this instrumentation exists to prevent.
    let tracked_arm_sends: u64 = arms
        .iter()
        .filter(|(arm, _, _)| *arm == PayloadArm::FullNoTheirSummaryTracked)
        .map(|(_, sends, _)| *sends)
        .sum();
    let tracked_arm_bytes: u64 = arms
        .iter()
        .filter(|(arm, _, _)| *arm == PayloadArm::FullNoTheirSummaryTracked)
        .map(|(_, _, bytes)| *bytes)
        .sum();
    obj.insert(
        "tracked_missing_unattributed_sends".into(),
        tracked_arm_sends.saturating_sub(reason_sends).into(),
    );
    obj.insert(
        "tracked_missing_unattributed_bytes".into(),
        tracked_arm_bytes.saturating_sub(reason_bytes).into(),
    );

    obj.insert("total_sends".into(), total_sends.into());
    obj.insert("total_bytes".into(), total_bytes.into());
    obj.insert("full_state_bytes".into(), full_state_bytes.into());
    // The headline ratio: of everything we actually put on the wire, how much
    // was a whole state rather than a diff.
    let full_state_share = if total_bytes == 0 {
        0.0
    } else {
        full_state_bytes as f64 / total_bytes as f64
    };
    obj.insert(
        "full_state_byte_share".into(),
        serde_json::Number::from_f64(full_state_share)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
    );
    obj.insert(
        "top_contracts_by_full_state_bytes".into(),
        serde_json::Value::Array(
            contracts
                .iter()
                .map(
                    |(id, bytes)| serde_json::json!({ "contract": id.to_string(), "bytes": bytes }),
                )
                .collect(),
        ),
    );
    // #4979 / #5057: TOTAL bytes and sends per contract, across every arm. The
    // full-state array above is a numerator with no denominator — it cannot see
    // a contract whose entire cost sits in the `delta` arm, which is exactly the
    // #5056 case. Ranked by bytes, since that is the axis a per-contract budget
    // would bound.
    obj.insert(
        "top_contracts_by_total_bytes".into(),
        serde_json::Value::Array(
            total
                .contracts
                .iter()
                .map(|(id, sends, bytes)| {
                    serde_json::json!({
                        "contract": id.to_string(),
                        "sends": sends,
                        "bytes": bytes,
                    })
                })
                .collect(),
        ),
    );
    // This map's own cap overflow, NOT folded into `attribution_dropped_*`.
    // Those count full-state drops only, while every arm feeds `contract_total`,
    // so it caps first: a `Delta`-only contract refused here would otherwise
    // vanish from the schema entirely, and a contract the full-state map still
    // admits would show full-state bytes with no total entry. #5057 sizes a
    // per-contract budget from this distribution, so its truncated tail has to
    // be legible rather than merely absent.
    obj.insert(
        "contracts_tracked_total".into(),
        total.contracts_tracked.into(),
    );
    obj.insert(
        "total_attribution_dropped_sends".into(),
        total.dropped_sends.into(),
    );
    obj.insert(
        "total_attribution_dropped_bytes".into(),
        total.dropped_bytes.into(),
    );
    // The published schema must ADD UP, using only fields it publishes:
    //
    //   sum(top_contracts) + other_contracts_bytes + attribution_dropped_bytes
    //     == full_state_bytes
    //
    // Two DIFFERENT kinds of omission have to be reported separately, and an
    // earlier revision conflated them:
    //   * `other_contracts_bytes` — contracts we DID track but that fell
    //     outside the reported top-N. With 11..=MAX_TRACKED_CONTRACTS
    //     contracts this is non-zero while nothing was ever dropped.
    //   * `attribution_dropped_bytes` — contracts never tracked at all
    //     because the cap was already full.
    // Reporting only the second made an 11-contract window look perfectly
    // reconciled while 10 % of its bytes were unaccounted for.
    // #4956: which contracts the gate actually refused on. The aggregate
    // ratio says a state-sized "summary" is being fed in; this says by whom.
    obj.insert(
        "top_contracts_by_not_efficient_bytes".into(),
        serde_json::Value::Array(
            not_efficient_contracts
                .iter()
                .map(
                    |(id, bytes)| serde_json::json!({ "contract": id.to_string(), "bytes": bytes }),
                )
                .collect(),
        ),
    );
    let top_sum: u64 = contracts.iter().map(|(_, b)| *b).sum();
    let other_contracts_bytes = tracked_full_state_bytes.saturating_sub(top_sum);
    obj.insert("other_contracts_bytes".into(), other_contracts_bytes.into());
    obj.insert("contracts_tracked".into(), contracts_tracked.into());
    // Sends (not distinct contracts) that missed attribution entirely, and the
    // bytes behind them.
    obj.insert(
        "attribution_dropped_sends".into(),
        attribution_dropped_sends.into(),
    );
    obj.insert(
        "attribution_dropped_bytes".into(),
        attribution_dropped_bytes.into(),
    );
    // #5062: the fan-out multiplier's denominator. `total_sends` above is the
    // numerator, recorded into the same window and drained by the same atomic
    // take, so the two describe the same 60 s of work.
    //
    // NO ratio is published here, deliberately. The quantity that matters is a
    // ratio of SUMS over the fleet —
    //   sum(total_sends) / sum(applies_client_local_changed)
    // — and most nodes host no local clients, so a per-window per-node ratio is
    // `0/0` far more often than not. Publishing one would invite exactly the
    // mean-of-ratios aggregation that `not_efficient_summary_to_state_bytes_ratio`
    // above is named to steer consumers away from, on data where it is far more
    // degenerate. Raw counters only; the division belongs in the query.
    for (origin, changed, total) in applies {
        obj.insert(
            format!("applies_{}_changed", origin.label()),
            (*changed).into(),
        );
        obj.insert(format!("applies_{}_total", origin.label()), (*total).into());
    }

    obj.insert("window_secs".into(), window_secs.into());
    serde_json::Value::Object(obj)
}

/// Emit one `broadcast_payload_mix` rollup and reset the window.
///
/// Returns the payload so callers (and tests) can inspect what was sent.
pub(crate) fn emit_payload_mix_rollup(
    mix: &PayloadMix,
    local_peer_id: &str,
    window_secs: u64,
) -> serde_json::Value {
    // ONE atomic take: the arm counters and the per-contract tallies describe
    // exactly the same set of broadcasts, so the top-N list always reconciles
    // against `full_state_bytes`.
    let window = mix.take_window();
    let payload = payload_mix_json(
        &window.arms(),
        &window.top_contracts(),
        &window.top_not_efficient_contracts(),
        &window.total_attribution(),
        window.contract_full_state_bytes.values().sum(),
        window.contract_full_state_bytes.len() as u64,
        window.attribution_dropped_sends,
        window.attribution_dropped_bytes,
        window.gate_stats(),
        &window.tracked_missing(),
        &window.applies(),
        window_secs,
    );
    crate::tracing::telemetry::send_standalone_shadow_event_with_peer_id(
        "broadcast_payload_mix",
        local_peer_id,
        payload.clone(),
    );
    payload
}

/// The window length to report, given the time actually elapsed since the
/// previous rollup.
///
/// Reporting the nominal cadence would be wrong whenever the aggregator tick
/// slips: `MissedTickBehavior::Delay` lets a saturated runtime stretch the
/// real window past [`ROLLUP_WINDOW`] while broadcast workers keep recording,
/// so a constant `window_secs` inflates every rate derived from the totals.
/// That error is not random — it is largest exactly when the node is busiest,
/// which is the condition this instrumentation exists to characterise.
///
/// Floored at 1 s so a downstream rate computation can never divide by zero.
fn rollup_window_secs(elapsed: Duration) -> u64 {
    (elapsed.as_secs_f64().round() as u64).max(1)
}

/// Spawn the `broadcast_payload_mix` aggregator and register it with the
/// [`BackgroundTaskMonitor`].
///
/// Always-on and cheap: it takes one lock and drains a bounded map once per
/// [`ROLLUP_WINDOW`]. Observation only — nothing reads these counters to make
/// a decision, and nothing on the hot path ever reads them at all.
pub(crate) fn spawn_payload_mix_aggregator(
    mix: std::sync::Arc<PayloadMix>,
    local_peer_id: String,
    monitor: &BackgroundTaskMonitor,
) {
    let handle = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(ROLLUP_WINDOW);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await; // skip the immediate first tick
        // `tokio::time::Instant` (not `std::time::Instant`) so this reads the
        // same clock the ticker uses and stays controllable under a paused
        // test runtime.
        let mut last_rollup = tokio::time::Instant::now();
        loop {
            ticker.tick().await;
            let now = tokio::time::Instant::now();
            let elapsed = now.saturating_duration_since(last_rollup);
            last_rollup = now;
            emit_payload_mix_rollup(&mix, &local_peer_id, rollup_window_secs(elapsed));
        }
    });
    monitor.register("broadcast_payload_mix_aggregator", handle);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn contract(byte: u8) -> ContractInstanceId {
        ContractInstanceId::new([byte; 32])
    }

    #[test]
    fn receiver_applies_classify_all_outcomes_and_state_size_boundaries() {
        let mix = PayloadMix::new();
        let last_bounded = *crate::tracing::event_kind::STATE_SIZE_BUCKET_UPPER_BOUNDS
            .last()
            .unwrap() as usize;

        for (is_delta, changed, state_size, payload_bytes) in [
            (true, true, 64 * 1024, 11),
            (true, false, 64 * 1024 + 1, 22),
            (false, true, last_bounded, 33),
            (false, false, last_bounded + 1, 44),
        ] {
            let mut terminal = mix.receiver_terminal_guard(is_delta, payload_bytes);
            terminal.mark_applied(changed, state_size);
        }

        let stats = mix.receiver_apply_stats();
        let delta_changed = ReceiverApplyClass::DeltaChanged.index();
        let delta_no_op = ReceiverApplyClass::DeltaNoOp.index();
        let full_changed = ReceiverApplyClass::FullChanged.index();
        let full_no_op = ReceiverApplyClass::FullNoOp.index();

        assert_eq!(stats.counts[delta_changed][0], 1);
        assert_eq!(stats.counts[delta_no_op][1], 1);
        assert_eq!(stats.counts[full_changed][STATE_SIZE_BUCKET_COUNT - 2], 1);
        assert_eq!(stats.counts[full_no_op][STATE_SIZE_BUCKET_COUNT - 1], 1);

        let total_count: u64 = stats.counts.iter().flatten().sum();
        let total_payload_bytes: u64 = stats.terminal_bytes.iter().flatten().sum();
        assert_eq!(total_count, 4);
        assert_eq!(total_payload_bytes, 110);
    }

    #[test]
    fn receiver_apply_totals_are_cumulative_across_sender_window_drains() {
        let mix = PayloadMix::new();
        mix.receiver_terminal_guard(false, 100)
            .mark_applied(false, 3 * 1024 * 1024);
        let first = mix.receiver_apply_stats();

        // The legacy sender payload mix remains a drained one-minute window.
        // Draining it must not erase the cumulative router-snapshot source.
        let _ = mix.take_window();
        assert_eq!(mix.receiver_apply_stats(), first);

        mix.receiver_terminal_guard(false, 250)
            .mark_applied(false, 3 * 1024 * 1024);
        let second = mix.receiver_apply_stats();
        let class = ReceiverApplyClass::FullNoOp.index();
        let terminal_class = 5 + ReceiverTerminalOutcome::NoOp.index();
        let result_state_bucket = state_size_bucket(3 * 1024 * 1024);
        let incoming_payload_bucket = state_size_bucket(100);
        assert_eq!(second.counts[class][result_state_bucket], 2);
        assert_eq!(
            second.terminal_bytes[terminal_class][incoming_payload_bucket],
            350
        );
    }

    #[test]
    fn receiver_terminal_guard_accounts_for_dedup_backoff_and_failure_bytes() {
        let mix = PayloadMix::new();
        let large = 4 * 1024 * 1024;

        let mut dedup = mix.receiver_terminal_guard(true, large);
        dedup.mark_dedup();
        drop(dedup);

        let mut backoff = mix.receiver_terminal_guard(false, large + 1);
        backoff.mark_backoff();
        drop(backoff);

        // The default terminal outcome is failure, including early returns and
        // unwinds that occur before an explicit outcome is selected.
        drop(mix.receiver_terminal_guard(false, large + 2));

        let stats = mix.receiver_apply_stats();
        let bucket = state_size_bucket(large as u64);
        assert_eq!(stats.terminal_counts[2][bucket], 1);
        assert_eq!(stats.terminal_bytes[2][bucket], large as u64);
        assert_eq!(stats.terminal_counts[8][bucket], 1);
        assert_eq!(stats.terminal_bytes[8][bucket], (large + 1) as u64);
        assert_eq!(stats.terminal_counts[9][bucket], 1);
        assert_eq!(stats.terminal_bytes[9][bucket], (large + 2) as u64);
    }

    /// #4979 / #5056: a contract whose entire cost sits in the `delta` arm must
    /// still be attributable.
    ///
    /// `contract_full_state_bytes` is written only under `arm.is_full_state()`,
    /// so it is structurally blind to exactly the contract found in #5056 — one
    /// at 55.6% of all broadcast sends whose "deltas" are full-state-sized.
    /// Attributing that needed a natural experiment over single-contract peers
    /// because no counter could answer directly. This asserts the new total map
    /// sees it AND that the old map does not, so the gap is pinned rather than
    /// merely fixed.
    #[test]
    fn delta_only_contract_is_attributable_in_totals_but_not_full_state() {
        let mix = PayloadMix::new();
        // A contract that only ever sends deltas — the #5056 shape.
        mix.record_delivered(PayloadArm::Delta, &contract(1), 25_000, None, None);
        mix.record_delivered(PayloadArm::Delta, &contract(1), 25_000, None, None);
        // A second contract that only ever sends full state, for contrast.
        mix.record_delivered(
            PayloadArm::FullNoOurSummary,
            &contract(2),
            1_000,
            None,
            None,
        );

        let w = mix.take_window();

        let totals = w.top_contracts_total();
        let delta_only = totals.iter().find(|(id, _, _)| *id == contract(1)).expect(
            "a delta-only contract MUST appear in the total map — this is \
                     the #5056 blind spot the map exists to close",
        );
        assert_eq!(delta_only.1, 2, "both sends must be counted");
        assert_eq!(delta_only.2, 50_000, "both sends' bytes must be counted");

        // And it must outrank the full-state contract, since ranking by total
        // bytes is what a per-contract budget (#5057) would be sized from.
        assert_eq!(
            totals[0].0,
            contract(1),
            "the total map must rank by TOTAL bytes, so the expensive delta-only \
             contract leads — ranking by full-state bytes would hide it entirely"
        );

        // The pin: the pre-existing map genuinely cannot see it.
        assert!(
            !w.top_contracts().iter().any(|(id, _)| *id == contract(1)),
            "contract_full_state_bytes must remain full-state-only; if a delta \
             contract starts appearing there, the two maps have been conflated \
             and the full-state share becomes unreadable"
        );
        assert!(
            w.top_contracts().iter().any(|(id, _)| *id == contract(2)),
            "the full-state contract must still be attributed as before"
        );
    }

    /// #4956: the refused-delta arm must be attributable to a CONTRACT, not
    /// just counted in aggregate. The aggregate gate ratio proved a
    /// state-sized "summary" is being fed in; without this the culprit stays
    /// anonymous. Only `FullNotEfficient` may land in the narrow map — a
    /// different full-state arm sharing it would re-create the ambiguity.
    #[test]
    fn not_efficient_bytes_are_attributed_to_their_contract() {
        let mix = PayloadMix::new();
        mix.record_delivered(
            PayloadArm::FullNotEfficient,
            &contract(7),
            600,
            Some((600, 600)),
            None,
        );
        mix.record_delivered(
            PayloadArm::FullNotEfficient,
            &contract(7),
            400,
            Some((400, 400)),
            None,
        );
        // A different full-state arm must NOT pollute the narrow map.
        mix.record_delivered(
            PayloadArm::FullNoTheirSummaryUntracked,
            &contract(8),
            999,
            None,
            None,
        );
        let w = mix.take_window();
        let top = w.top_not_efficient_contracts();
        assert_eq!(top.len(), 1, "only the refused arm belongs here: {top:?}");
        assert_eq!(top[0].0, contract(7));
        assert_eq!(top[0].1, 1000, "per-contract bytes must accumulate");
        // The wider map still sees both, so the two views stay consistent.
        assert_eq!(w.contract_full_state_bytes[&contract(8)], 999);
    }

    /// Taking the window leaves the accumulator empty so consecutive rollups
    /// report windows, not lifetime totals.
    #[test]
    fn take_window_resets_the_window() {
        let mix = PayloadMix::new();
        mix.record_delivered(PayloadArm::Delta, &contract(1), 100, None, None);
        let first = mix.take_window().arms();
        assert_eq!(first[PayloadArm::Delta.index()].1, 1);
        assert_eq!(first[PayloadArm::Delta.index()].2, 100);
        let second = mix.take_window().arms();
        assert!(
            second
                .iter()
                .all(|(_, sends, bytes)| *sends == 0 && *bytes == 0),
            "second take must be empty, got {second:?}"
        );
    }

    /// Every arm lands in its own bucket — a mis-indexed arm would silently
    /// attribute bytes to the wrong cause, which is the entire point of this
    /// module.
    #[test]
    fn each_arm_counts_separately() {
        let mix = PayloadMix::new();
        for (i, arm) in PayloadArm::ALL.iter().enumerate() {
            for _ in 0..=i {
                mix.record_delivered(*arm, &contract(i as u8), 10, None, None);
            }
        }
        let drained = mix.take_window().arms();
        for (i, (arm, sends, bytes)) in drained.iter().enumerate() {
            assert_eq!(*arm, PayloadArm::ALL[i]);
            assert_eq!(*sends, i as u64 + 1, "wrong send count for {arm:?}");
            assert_eq!(*bytes, (i as u64 + 1) * 10, "wrong byte count for {arm:?}");
        }
    }

    /// The aggregate arm counters and the per-contract tallies must always
    /// describe the SAME set of broadcasts.
    ///
    /// Regression for the external-review finding: the first version drained
    /// the arm counters and the contract map at two different instants, so a
    /// broadcast landing in between was counted in one window for
    /// `full_state_bytes` and the next for the per-contract list (and an
    /// increment racing the map `clear()` was lost outright). This asserts the
    /// invariant an analyst actually relies on: the top-N list reconciles
    /// against the full-state total.
    #[test]
    fn per_contract_tallies_reconcile_with_arm_totals() {
        let mix = PayloadMix::new();
        mix.record_delivered(PayloadArm::FullNotEfficient, &contract(1), 500, None, None);
        mix.record_delivered(
            PayloadArm::FullNoTheirSummaryUntracked,
            &contract(2),
            300,
            None,
            None,
        );
        mix.record_delivered(
            PayloadArm::FullDeltaSuppressed,
            &contract(1),
            200,
            None,
            None,
        );
        mix.record_delivered(PayloadArm::Delta, &contract(3), 50, None, None); // not full state

        let window = mix.take_window();
        let full_state_total: u64 = window
            .arms()
            .iter()
            .filter(|(arm, _, _)| arm.is_full_state())
            .map(|(_, _, bytes)| bytes)
            .sum();
        assert_eq!(
            full_state_total, 1000,
            "full-state arm bytes should exclude the delta send"
        );
        // Contract 1 accumulated across two different full-state arms.
        assert_eq!(window.contract_full_state_bytes[&contract(1)], 700);
        assert_eq!(window.contract_full_state_bytes[&contract(2)], 300);
        assert_reconciles(&window);
    }

    /// The reconciliation invariant, asserted against the EMITTED JSON rather
    /// than the in-memory window.
    ///
    /// This distinction is the whole point: an earlier revision checked the
    /// internal map and passed, while the PUBLISHED schema silently failed to
    /// add up for any window holding 11..=MAX_TRACKED_CONTRACTS contracts —
    /// the top-N truncation dropped bytes that no emitted field accounted
    /// for. A consumer can only use the fields actually present in the event,
    /// so that is what the test must check.
    fn assert_reconciles(window: &Window) {
        let json = payload_mix_json(
            &window.arms(),
            &window.top_contracts(),
            &window.top_not_efficient_contracts(),
            &window.total_attribution(),
            window.contract_full_state_bytes.values().sum(),
            window.contract_full_state_bytes.len() as u64,
            window.attribution_dropped_sends,
            window.attribution_dropped_bytes,
            window.gate_stats(),
            &[],
            &[],
            60,
        );
        let top_sum: u64 = json["top_contracts_by_full_state_bytes"]
            .as_array()
            .expect("top contracts must be an array")
            .iter()
            .map(|e| e["bytes"].as_u64().expect("bytes must be a number"))
            .sum();
        let other = json["other_contracts_bytes"].as_u64().unwrap();
        let dropped = json["attribution_dropped_bytes"].as_u64().unwrap();
        let full_state = json["full_state_bytes"].as_u64().unwrap();
        assert_eq!(
            top_sum + other + dropped,
            full_state,
            "published schema must add up: sum(top_contracts) + \
             other_contracts_bytes + attribution_dropped_bytes == \
             full_state_bytes (got {top_sum} + {other} + {dropped} != \
             {full_state})"
        );
    }

    /// A window with more contracts than the top-N limit must still reconcile:
    /// the untruncated remainder has to appear in `other_contracts_bytes`.
    ///
    /// Regression for the external-review finding — eleven 100-byte contracts
    /// previously reported full_state_bytes = 1100, a top-list summing to
    /// 1000, and zero dropped bytes.
    #[test]
    fn window_with_more_contracts_than_top_n_still_reconciles() {
        let mix = PayloadMix::new();
        for i in 0..(TOP_CONTRACTS_REPORTED + 1) {
            mix.record_delivered(
                PayloadArm::FullNotEfficient,
                &contract(i as u8),
                100,
                None,
                None,
            );
        }
        let window = mix.take_window();
        assert_reconciles(&window);

        let json = payload_mix_json(
            &window.arms(),
            &window.top_contracts(),
            &window.top_not_efficient_contracts(),
            &window.total_attribution(),
            window.contract_full_state_bytes.values().sum(),
            window.contract_full_state_bytes.len() as u64,
            window.attribution_dropped_sends,
            window.attribution_dropped_bytes,
            window.gate_stats(),
            &[],
            &[],
            60,
        );
        assert_eq!(json["full_state_bytes"], 1100);
        assert_eq!(
            json["other_contracts_bytes"], 100,
            "the 11th contract's bytes must be reported as the untruncated \
             remainder, not silently lost"
        );
        assert_eq!(
            json["attribution_dropped_bytes"], 0,
            "nothing was DROPPED here — the cap was never reached; this is \
             truncation, which is a different field"
        );
        assert_eq!(json["contracts_tracked"], 11);
    }

    /// Over-cap drops and top-N truncation are different omissions and must
    /// reconcile together.
    #[test]
    fn truncation_and_over_cap_drops_reconcile_together() {
        let mix = PayloadMix::new();
        for i in 0..(MAX_TRACKED_CONTRACTS + 5) {
            let mut raw = [0u8; 32];
            raw[0] = (i % 256) as u8;
            raw[1] = (i / 256) as u8;
            mix.record_delivered(
                PayloadArm::FullNoTheirSummaryUntracked,
                &ContractInstanceId::new(raw),
                10,
                None,
                None,
            );
        }
        let window = mix.take_window();
        assert!(
            window.attribution_dropped_bytes > 0,
            "cap must have been hit"
        );
        assert_reconciles(&window);
    }

    /// The total map's cap overflow must be VISIBLE, and visible separately
    /// from the full-state map's.
    ///
    /// `contract_total` is written on every arm, so it fills strictly faster
    /// than `contract_full_state_bytes`. Two failures follow if the overflow is
    /// silent or shared:
    ///   * a `Delta`-only contract refused by the cap disappears from the
    ///     schema entirely — `attribution_dropped_*` is only written under
    ///     `arm.is_full_state()`, so nothing records it; and
    ///   * the total map can cap while the full-state map still admits, giving
    ///     a contract full-state bytes with no total entry, i.e. a denominator
    ///     smaller than its own numerator.
    /// #5057 wants to size a per-contract budget from this distribution, so a
    /// silently truncated tail is exactly the wrong failure.
    #[test]
    fn total_map_cap_overflow_is_reported_separately_from_full_state() {
        let mix = PayloadMix::new();
        let id = |i: usize| {
            let mut raw = [0u8; 32];
            raw[0] = (i % 256) as u8;
            raw[1] = (i / 256) as u8;
            ContractInstanceId::new(raw)
        };

        // Fill the total map to its cap with DELTA sends only. The full-state
        // map stays empty, so this is the case `attribution_dropped_*` cannot
        // see by construction.
        for i in 0..MAX_TRACKED_CONTRACTS {
            mix.record_delivered(PayloadArm::Delta, &id(i), 10, None, None);
        }
        // Three more delta sends, all refused by the cap.
        for i in MAX_TRACKED_CONTRACTS..(MAX_TRACKED_CONTRACTS + 3) {
            mix.record_delivered(PayloadArm::Delta, &id(i), 7, None, None);
        }

        let window = mix.take_window();
        let total = window.total_attribution();

        assert_eq!(
            total.contracts_tracked, MAX_TRACKED_CONTRACTS as u64,
            "the total map must be at its cap"
        );
        assert_eq!(
            total.dropped_sends, 3,
            "each refused send must be counted — a silently dropped tail is \
             what makes the distribution unusable for sizing a budget (#5057)"
        );
        assert_eq!(
            total.dropped_bytes, 21,
            "and the bytes behind those refused sends"
        );

        // The pre-existing full-state counters must be untouched: these were
        // Delta sends, so folding the two overflows together would invent
        // full-state drops that never happened.
        assert_eq!(
            window.attribution_dropped_sends, 0,
            "the full-state drop counter must stay zero — no full-state send \
             was ever recorded, so a non-zero value means the two overflows \
             were conflated"
        );
        assert_eq!(window.attribution_dropped_bytes, 0);

        // And the schema publishes all of it.
        let json = payload_mix_json(
            &window.arms(),
            &window.top_contracts(),
            &window.top_not_efficient_contracts(),
            &total,
            window.contract_full_state_bytes.values().sum(),
            window.contract_full_state_bytes.len() as u64,
            window.attribution_dropped_sends,
            window.attribution_dropped_bytes,
            window.gate_stats(),
            &window.tracked_missing(),
            &window.applies(),
            60,
        );
        assert_eq!(json["total_attribution_dropped_sends"], 3);
        assert_eq!(json["total_attribution_dropped_bytes"], 21);
        assert_eq!(
            json["contracts_tracked_total"], MAX_TRACKED_CONTRACTS as u64,
            "the total map's own tracked count, distinct from contracts_tracked \
             (which counts the full-state map) — here 256 vs 0"
        );
        assert_eq!(
            json["contracts_tracked"], 0,
            "sanity: the two tracked counts really are different maps"
        );
    }

    /// The total map can reach its cap while the full-state map still admits,
    /// which is how a contract ends up with full-state bytes and no total
    /// entry. The separate drop counters are what makes that legible.
    #[test]
    fn total_map_caps_before_full_state_map_and_says_so() {
        let mix = PayloadMix::new();
        let id = |i: usize| {
            let mut raw = [0u8; 32];
            raw[0] = (i % 256) as u8;
            raw[1] = (i / 256) as u8;
            ContractInstanceId::new(raw)
        };

        // Saturate the total map with deltas; the full-state map is still empty.
        for i in 0..MAX_TRACKED_CONTRACTS {
            mix.record_delivered(PayloadArm::Delta, &id(i), 10, None, None);
        }
        // A brand-new contract sends FULL STATE. The full-state map has room,
        // so it is attributed there — but the total map is full and refuses it.
        let newcomer = id(MAX_TRACKED_CONTRACTS + 1);
        mix.record_delivered(PayloadArm::FullNoOurSummary, &newcomer, 5_000, None, None);

        let window = mix.take_window();
        assert!(
            window
                .top_contracts()
                .iter()
                .any(|(cid, bytes)| *cid == newcomer && *bytes == 5_000),
            "precondition: the full-state map still had room for the newcomer"
        );
        assert!(
            !window
                .total_attribution()
                .contracts
                .iter()
                .any(|(cid, _, _)| *cid == newcomer),
            "precondition: the total map was full and refused it"
        );
        assert_eq!(
            window.total_attribution().dropped_bytes,
            5_000,
            "a contract with full-state bytes and NO total entry must be \
             reported as a total-map drop; otherwise the published numerator \
             exceeds its own denominator with nothing to explain why"
        );
        assert_eq!(
            window.attribution_dropped_bytes, 0,
            "the full-state map did admit it, so nothing was dropped there"
        );
    }

    /// Concurrent recorders racing a rollup must not lose or double-count
    /// bytes: every recorded byte lands in exactly one window.
    #[test]
    fn concurrent_records_racing_a_rollup_conserve_bytes() {
        use std::sync::Arc;

        const THREADS: usize = 4;
        const PER_THREAD: usize = 500;

        let mix = Arc::new(PayloadMix::new());
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

        // A rollup loop running concurrently with the writers, accumulating
        // what it drains.
        let drained_total = Arc::new(Mutex::new(0u64));
        let drainer = {
            let mix = Arc::clone(&mix);
            let stop = Arc::clone(&stop);
            let drained_total = Arc::clone(&drained_total);
            std::thread::spawn(move || {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let w = mix.take_window();
                    let sum: u64 = w.arms().iter().map(|(_, _, b)| b).sum();
                    *drained_total.lock() += sum;
                    // Sleep rather than spin. A hot loop here would burn a
                    // whole core for the duration of the test, and the suite
                    // runs tests in parallel — starving a timing-sensitive
                    // test elsewhere would make THIS test the cause of a flake
                    // somewhere else. 50 µs still yields hundreds of drains
                    // against 2,000 records, so the rollover race is amply
                    // exercised.
                    std::thread::sleep(Duration::from_micros(50));
                }
            })
        };

        let writers: Vec<_> = (0..THREADS)
            .map(|t| {
                let mix = Arc::clone(&mix);
                std::thread::spawn(move || {
                    for _ in 0..PER_THREAD {
                        mix.record_delivered(
                            PayloadArm::FullNotEfficient,
                            &contract(t as u8),
                            7,
                            None,
                            None,
                        );
                    }
                })
            })
            .collect();
        for w in writers {
            w.join().unwrap();
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        drainer.join().unwrap();

        // Whatever the final drainer pass missed is still in the accumulator.
        let leftover: u64 = mix.take_window().arms().iter().map(|(_, _, b)| b).sum();
        let total = *drained_total.lock() + leftover;
        assert_eq!(
            total,
            (THREADS * PER_THREAD * 7) as u64,
            "bytes were lost or double-counted across a concurrent rollover"
        );
    }

    /// Only full-state arms are counted as full-state bytes; a delta-heavy
    /// node must not look like it is flooding whole states.
    #[test]
    fn full_state_share_excludes_deltas() {
        let arms = vec![
            (PayloadArm::Delta, 3, 300),
            (PayloadArm::FullDeltaSuppressed, 0, 0),
            (PayloadArm::FullNotEfficient, 1, 700),
            (PayloadArm::FullComputeFailed, 0, 0),
            (PayloadArm::FullNoTheirSummaryUntracked, 0, 0),
        ];
        let json = payload_mix_json(
            &arms,
            &[],
            &[],
            &TotalAttribution::default(),
            0,
            0,
            0,
            0,
            NotEfficientGateStats::default(),
            &[],
            &[],
            60,
        );
        assert_eq!(json["total_bytes"], 1000);
        assert_eq!(json["full_state_bytes"], 700);
        assert_eq!(json["full_state_byte_share"], 0.7);
        assert_eq!(json["delta_sends"], 3);
        assert_eq!(json["full_not_efficient_bytes"], 700);
    }

    /// The three no-summary arms are reported separately AND republished as
    /// the pre-split `full_no_summary_*` aggregate.
    ///
    /// Both halves matter. The split is what makes the largest arm on the
    /// network actionable — a contract-handler failure, a permanent
    /// peer-tracking gap, and ordinary cold start need three different fixes.
    /// The aggregate is what stops the split from silently zeroing the field
    /// every existing dashboard and analysis script queries by name.
    #[test]
    fn no_summary_split_reports_each_cause_and_the_legacy_aggregate() {
        let mix = PayloadMix::new();
        mix.record_delivered(PayloadArm::FullNoOurSummary, &contract(1), 100, None, None);
        mix.record_delivered(
            PayloadArm::FullNoTheirSummaryUntracked,
            &contract(2),
            200,
            None,
            None,
        );
        mix.record_delivered(
            PayloadArm::FullNoTheirSummaryTracked,
            &contract(3),
            300,
            None,
            None,
        );
        mix.record_delivered(
            PayloadArm::FullNoTheirSummaryUntracked,
            &contract(2),
            400,
            None,
            None,
        );

        let window = mix.take_window();
        let json = payload_mix_json(
            &window.arms(),
            &window.top_contracts(),
            &window.top_not_efficient_contracts(),
            &window.total_attribution(),
            window.contract_full_state_bytes.values().sum(),
            window.contract_full_state_bytes.len() as u64,
            window.attribution_dropped_sends,
            window.attribution_dropped_bytes,
            window.gate_stats(),
            &[],
            &[],
            60,
        );

        assert_eq!(json["full_no_our_summary_bytes"], 100);
        assert_eq!(json["full_no_their_summary_untracked_bytes"], 600);
        assert_eq!(json["full_no_their_summary_untracked_sends"], 2);
        assert_eq!(json["full_no_their_summary_tracked_bytes"], 300);

        assert_eq!(
            json["full_no_summary_bytes"], 1000,
            "the pre-split aggregate must still be published as the sum of the \
             three causes, or the split silently zeroes every dashboard and \
             analysis script that queries `full_no_summary_bytes` by name"
        );
        assert_eq!(json["full_no_summary_sends"], 4);
    }

    /// Emit the rollup for a window built by `record_delivered`, so these
    /// tests exercise the real recording path rather than hand-built inputs.
    fn emit(mix: &PayloadMix) -> serde_json::Value {
        let window = mix.take_window();
        payload_mix_json(
            &window.arms(),
            &window.top_contracts(),
            &window.top_not_efficient_contracts(),
            &window.total_attribution(),
            window.contract_full_state_bytes.values().sum(),
            window.contract_full_state_bytes.len() as u64,
            window.attribution_dropped_sends,
            window.attribution_dropped_bytes,
            window.gate_stats(),
            &window.tracked_missing(),
            &window.applies(),
            60,
        )
    }

    /// #4961: the tracked arm splits by WHY the summary is absent, and the
    /// per-reason bytes reconcile exactly against the arm total.
    #[test]
    fn tracked_arm_splits_by_missing_reason_and_reconciles() {
        let mix = PayloadMix::new();
        mix.record_delivered(
            PayloadArm::FullNoTheirSummaryTracked,
            &contract(1),
            100,
            None,
            Some(SummaryMissingReason::NeverPopulated),
        );
        mix.record_delivered(
            PayloadArm::FullNoTheirSummaryTracked,
            &contract(2),
            250,
            None,
            Some(SummaryMissingReason::ClearedByNoneReport),
        );
        mix.record_delivered(
            PayloadArm::FullNoTheirSummaryTracked,
            &contract(3),
            30,
            None,
            Some(SummaryMissingReason::ClearedByResync),
        );
        mix.record_delivered(
            PayloadArm::FullNoTheirSummaryTracked,
            &contract(4),
            7,
            None,
            Some(SummaryMissingReason::ClearedByDeltaApplyFailure),
        );

        let json = emit(&mix);
        assert_eq!(json["tracked_missing_never_populated_bytes"], 100);
        assert_eq!(json["tracked_missing_never_populated_sends"], 1);
        assert_eq!(json["tracked_missing_none_report_bytes"], 250);
        assert_eq!(json["tracked_missing_resync_bytes"], 30);
        assert_eq!(json["tracked_missing_delta_apply_failed_bytes"], 7);

        assert_eq!(
            json["full_no_their_summary_tracked_bytes"], 387,
            "the arm total must equal the sum of its reasons"
        );
        assert_eq!(
            json["tracked_missing_unattributed_bytes"], 0,
            "every tracked send carried a reason, so the residual must be zero"
        );
        assert_eq!(json["tracked_missing_unattributed_sends"], 0);
    }

    /// A tracked send that arrives WITHOUT a reason must surface as an
    /// explicit residual, never be silently folded into another bucket.
    ///
    /// This is the guard against the failure mode that motivated the split:
    /// a future clear path that forgets to tag itself would otherwise make
    /// one of the four reasons look artificially small and mis-aim the fix.
    #[test]
    fn tracked_send_without_a_reason_is_reported_as_unattributed() {
        let mix = PayloadMix::new();
        mix.record_delivered(
            PayloadArm::FullNoTheirSummaryTracked,
            &contract(1),
            100,
            None,
            Some(SummaryMissingReason::NeverPopulated),
        );
        // No reason — e.g. a future clear site that forgot to tag itself.
        mix.record_delivered(
            PayloadArm::FullNoTheirSummaryTracked,
            &contract(2),
            900,
            None,
            None,
        );

        let json = emit(&mix);
        assert_eq!(json["tracked_missing_never_populated_bytes"], 100);
        assert_eq!(
            json["tracked_missing_unattributed_bytes"], 900,
            "an untagged tracked send must be visible as a residual, not \
             silently attributed to a reason that did not cause it"
        );
        assert_eq!(json["tracked_missing_unattributed_sends"], 1);
    }

    /// A reason passed on a NON-tracked arm is ignored, so a mis-paired call
    /// cannot inflate the split — the same discipline `gate_inputs` follows.
    #[test]
    fn missing_reason_is_ignored_on_arms_other_than_tracked() {
        let mix = PayloadMix::new();
        mix.record_delivered(
            PayloadArm::FullNoTheirSummaryUntracked,
            &contract(1),
            500,
            None,
            Some(SummaryMissingReason::NeverPopulated),
        );
        mix.record_delivered(
            PayloadArm::Delta,
            &contract(2),
            10,
            None,
            Some(SummaryMissingReason::ClearedByResync),
        );

        let json = emit(&mix);
        for reason in SummaryMissingReason::ALL {
            assert_eq!(
                json[format!("tracked_missing_{}_bytes", reason.as_str())],
                0,
                "a reason paired with a non-tracked arm must not be counted"
            );
        }
        assert_eq!(json["tracked_missing_unattributed_bytes"], 0);
    }

    /// The efficiency gate's inputs are reported, so `full_not_efficient`
    /// stops being a black box.
    ///
    /// The sizes were always carried by `DeltaUnavailable::NotEfficient` and
    /// always thrown away, because its only reader was a `debug!` that is
    /// compiled out in release builds. Post-#4923 the refusal is post-compute
    /// (the COMPUTED delta was not smaller than the state), and the reported
    /// summary:state ratio is what field-validates that change: it says
    /// whether the sends the old `summary * 2 >= state` proxy refused were
    /// genuinely summary-heavy, and the arm's volume says how rare a
    /// genuinely incompressible delta actually is.
    #[test]
    fn not_efficient_reports_the_gate_inputs_it_refused_on() {
        let mix = PayloadMix::new();
        mix.record_delivered(
            PayloadArm::FullNotEfficient,
            &contract(1),
            1000,
            Some((600, 1000)),
            None,
        );
        mix.record_delivered(
            PayloadArm::FullNotEfficient,
            &contract(1),
            2000,
            Some((1400, 2000)),
            None,
        );
        // A non-NotEfficient arm must never contribute, even if a caller
        // mistakenly passes sizes — otherwise the ratio silently drifts.
        mix.record_delivered(PayloadArm::Delta, &contract(1), 10, Some((99999, 1)), None);

        let window = mix.take_window();
        assert_eq!(window.gate_stats().summary_bytes_sum, 2000);
        assert_eq!(window.gate_stats().state_bytes_sum, 3000);
        assert_eq!(window.gate_stats().summary_bytes_max, 1400);
        assert_eq!(window.gate_stats().state_bytes_max, 2000);

        let json = payload_mix_json(
            &window.arms(),
            &window.top_contracts(),
            &window.top_not_efficient_contracts(),
            &window.total_attribution(),
            window.contract_full_state_bytes.values().sum(),
            window.contract_full_state_bytes.len() as u64,
            window.attribution_dropped_sends,
            window.attribution_dropped_bytes,
            window.gate_stats(),
            &[],
            &[],
            60,
        );
        assert_eq!(json["not_efficient_summary_bytes_sum"], 2000);
        assert_eq!(json["not_efficient_state_bytes_sum"], 3000);
        assert_eq!(json["not_efficient_summary_bytes_max"], 1400);
        assert_eq!(
            json["not_efficient_summary_to_state_bytes_ratio"],
            2000.0 / 3000.0
        );
    }

    /// With no `NotEfficient` sends the ratio must be null, not a misleading
    /// 0.0 that reads as "the gate refused on empty summaries".
    #[test]
    fn not_efficient_ratio_is_null_when_the_gate_never_fired() {
        let mix = PayloadMix::new();
        mix.record_delivered(PayloadArm::Delta, &contract(1), 10, None, None);
        let window = mix.take_window();
        let json = payload_mix_json(
            &window.arms(),
            &window.top_contracts(),
            &window.top_not_efficient_contracts(),
            &window.total_attribution(),
            0,
            0,
            0,
            0,
            window.gate_stats(),
            &[],
            &[],
            60,
        );
        assert!(json["not_efficient_summary_to_state_bytes_ratio"].is_null());
    }

    /// A window with no traffic must emit 0.0, not NaN — `Number::from_f64`
    /// rejects NaN and the field would silently become null.
    #[test]
    fn empty_window_reports_zero_share_not_nan() {
        let arms: Vec<_> = PayloadArm::ALL.iter().map(|a| (*a, 0, 0)).collect();
        let json = payload_mix_json(
            &arms,
            &[],
            &[],
            &TotalAttribution::default(),
            0,
            0,
            0,
            0,
            NotEfficientGateStats::default(),
            &[],
            &[],
            60,
        );
        assert_eq!(json["full_state_byte_share"], 0.0);
        assert_eq!(json["total_bytes"], 0);
    }

    /// Per-contract attribution is capped, and the overflow is reported as
    /// SENDS and BYTES rather than silently truncated.
    ///
    /// The counter deliberately counts sends, not distinct contracts: an
    /// external reviewer flagged that the original `contracts_dropped` name
    /// implied cardinality while the code incremented per send, so one
    /// over-cap contract broadcasting 1,000 times read as "1,000 contracts
    /// dropped". Tracking true cardinality would need an unbounded set, which
    /// is what the cap exists to prevent, so the field is named for what it
    /// measures and paired with the byte total that makes it actionable.
    #[test]
    fn contract_attribution_is_bounded_and_reports_overflow() {
        let mix = PayloadMix::new();
        for i in 0..(MAX_TRACKED_CONTRACTS + 20) {
            let mut raw = [0u8; 32];
            raw[0] = (i % 256) as u8;
            raw[1] = (i / 256) as u8;
            mix.record_delivered(
                PayloadArm::FullNotEfficient,
                &ContractInstanceId::new(raw),
                5,
                None,
                None,
            );
        }
        let window = mix.take_window();
        assert!(
            window.contract_full_state_bytes.len() <= MAX_TRACKED_CONTRACTS,
            "attribution map exceeded its cap: {}",
            window.contract_full_state_bytes.len()
        );
        assert_eq!(
            window.attribution_dropped_sends, 20,
            "overflow must be reported, not dropped silently"
        );
        assert_eq!(
            window.attribution_dropped_bytes, 100,
            "the bytes behind unattributed sends must be reported too"
        );
        assert!(window.top_contracts().len() <= TOP_CONTRACTS_REPORTED);
        // Taking the window resets everything, so the next window starts clean
        // rather than re-reporting this window's overflow.
        let next = mix.take_window();
        assert!(next.contract_full_state_bytes.is_empty());
        assert_eq!(next.attribution_dropped_sends, 0);
        assert_eq!(next.attribution_dropped_bytes, 0);
    }

    /// One over-cap contract sending repeatedly inflates the SEND count, not
    /// a contract count — the distinction the field name now makes explicit.
    #[test]
    fn repeated_sends_from_one_over_cap_contract_count_as_sends() {
        let mix = PayloadMix::new();
        for i in 0..MAX_TRACKED_CONTRACTS {
            let mut raw = [0u8; 32];
            raw[0] = (i % 256) as u8;
            raw[1] = (i / 256) as u8;
            mix.record_delivered(
                PayloadArm::FullNotEfficient,
                &ContractInstanceId::new(raw),
                1,
                None,
                None,
            );
        }
        // A single additional contract, broadcasting many times.
        let mut raw = [9u8; 32];
        raw[31] = 7;
        let over_cap = ContractInstanceId::new(raw);
        for _ in 0..1000 {
            mix.record_delivered(PayloadArm::FullNotEfficient, &over_cap, 3, None, None);
        }
        let window = mix.take_window();
        assert_eq!(window.attribution_dropped_sends, 1000);
        assert_eq!(window.attribution_dropped_bytes, 3000);
        assert!(!window.contract_full_state_bytes.contains_key(&over_cap));
    }

    /// Ties break deterministically on contract id so the reported top-N is
    /// stable across nodes and windows.
    #[test]
    fn top_contracts_sort_is_deterministic() {
        let mix = PayloadMix::new();
        mix.record_delivered(
            PayloadArm::FullNoTheirSummaryUntracked,
            &contract(9),
            100,
            None,
            None,
        );
        mix.record_delivered(
            PayloadArm::FullNoTheirSummaryUntracked,
            &contract(2),
            100,
            None,
            None,
        );
        mix.record_delivered(
            PayloadArm::FullNoTheirSummaryUntracked,
            &contract(5),
            500,
            None,
            None,
        );
        let top = mix.take_window().top_contracts();
        assert_eq!(top[0].0, contract(5), "largest first");
        assert_eq!(top[1].0, contract(2), "tie broken by contract id");
        assert_eq!(top[2].0, contract(9));
    }

    /// Deltas must never be attributed to a contract's full-state tally.
    #[test]
    fn delta_sends_are_not_attributed_as_full_state() {
        let mix = PayloadMix::new();
        mix.record_delivered(PayloadArm::Delta, &contract(7), 1234, None, None);
        let window = mix.take_window();
        assert!(
            window.contract_full_state_bytes.is_empty(),
            "delta bytes leaked into full-state attribution: {:?}",
            window.contract_full_state_bytes
        );
        // ...but the delta itself is still counted in the per-arm totals.
        assert_eq!(window.arms()[PayloadArm::Delta.index()].2, 1234);
    }

    /// The reported window must reflect the time actually elapsed, not the
    /// nominal cadence.
    ///
    /// Regression for the external-review finding: with
    /// `MissedTickBehavior::Delay`, a saturated runtime stretches the real
    /// window past `ROLLUP_WINDOW` while broadcast workers keep recording. A
    /// constant `window_secs` would then inflate every derived rate, and worst
    /// precisely when the node is busiest — the case this telemetry exists to
    /// characterise.
    #[test]
    fn reported_window_tracks_actual_elapsed_not_nominal_cadence() {
        // On-cadence tick: reports the nominal window.
        assert_eq!(rollup_window_secs(ROLLUP_WINDOW), 60);
        // Delayed tick: reports the LONGER real window, so a rate computed
        // downstream divides by the right number.
        assert_eq!(rollup_window_secs(Duration::from_secs(150)), 150);
        assert_eq!(
            rollup_window_secs(Duration::from_millis(90_400)),
            90,
            "sub-second remainder rounds to nearest"
        );
        // Never zero: a downstream `total / window_secs` must not divide by 0.
        assert_eq!(rollup_window_secs(Duration::ZERO), 1);
        assert_eq!(rollup_window_secs(Duration::from_millis(200)), 1);
    }

    /// Every full-state arm attributes to the contract; this is what names
    /// the offending contracts in the rollup.
    #[test]
    fn every_full_state_arm_attributes_to_its_contract() {
        for arm in PayloadArm::ALL.iter().filter(|a| a.is_full_state()) {
            let mix = PayloadMix::new();
            mix.record_delivered(*arm, &contract(3), 99, None, None);
            let top = mix.take_window().top_contracts();
            assert_eq!(
                top,
                vec![(contract(3), 99)],
                "{arm:?} must attribute its full-state bytes to the contract"
            );
        }
    }

    // ---- #5062: the fan-out multiplier's denominator ----

    /// Every `Priority` class maps to a distinct origin, and the mapping is
    /// the one the schema claims.
    ///
    /// This is the single point where a scheduling class is reinterpreted as
    /// provenance, so it is the single point where that reinterpretation can
    /// silently rot — e.g. a new `Priority` variant folded into an existing
    /// arm. An exhaustive check here is what makes the reuse safe.
    #[test]
    fn apply_origin_maps_each_priority_class_to_its_own_arm() {
        use crate::contract::Priority;

        assert_eq!(
            ApplyOrigin::from_priority(Priority::ClientLocal),
            ApplyOrigin::ClientLocal
        );
        assert_eq!(
            ApplyOrigin::from_priority(Priority::NetworkRelay),
            ApplyOrigin::NetworkRelay
        );
        assert_eq!(
            ApplyOrigin::from_priority(Priority::Background),
            ApplyOrigin::Background
        );

        // Distinct indices and distinct labels: two origins sharing either
        // would silently merge two arms of the measurement.
        let indices: Vec<usize> = ApplyOrigin::ALL.iter().map(|o| o.index()).collect();
        assert_eq!(indices, vec![0, 1, 2], "indices must be dense and ordered");
        let mut labels: Vec<&str> = ApplyOrigin::ALL.iter().map(|o| o.label()).collect();
        labels.sort_unstable();
        labels.dedup();
        assert_eq!(
            labels.len(),
            ApplyOrigin::COUNT,
            "every origin needs a unique wire label"
        );
    }

    /// The published schema splits applies by origin AND by whether the merge
    /// changed state — the `changed` arm being the one that corresponds 1:1
    /// with an emitted broadcast.
    #[test]
    fn applies_split_by_origin_and_by_whether_state_changed() {
        let mix = PayloadMix::new();
        // Two locally-originated updates, one of which was a no-op merge.
        mix.record_apply(ApplyOrigin::ClientLocal, true);
        mix.record_apply(ApplyOrigin::ClientLocal, false);
        // Three relayed applies, one of which actually moved state and so
        // triggered a re-broadcast from this node.
        mix.record_apply(ApplyOrigin::NetworkRelay, true);
        mix.record_apply(ApplyOrigin::NetworkRelay, false);
        mix.record_apply(ApplyOrigin::NetworkRelay, false);

        let json = emit(&mix);
        assert_eq!(json["applies_client_local_changed"], 1);
        assert_eq!(json["applies_client_local_total"], 2);
        assert_eq!(json["applies_network_relay_changed"], 1);
        assert_eq!(json["applies_network_relay_total"], 3);
        // Untouched origins are published as explicit zeros, not omitted: a
        // missing field and a zero field read identically to a careless query
        // but differently to a careful one.
        assert_eq!(json["applies_background_changed"], 0);
        assert_eq!(json["applies_background_total"], 0);
    }

    /// `total` is never less than `changed` for any origin — the invariant a
    /// consumer divides by.
    #[test]
    fn applies_total_covers_changed_for_every_origin() {
        let mix = PayloadMix::new();
        for (i, origin) in ApplyOrigin::ALL.iter().enumerate() {
            for _ in 0..=i {
                mix.record_apply(*origin, true);
            }
            mix.record_apply(*origin, false);
        }
        let json = emit(&mix);
        for (i, origin) in ApplyOrigin::ALL.iter().enumerate() {
            let changed = json[format!("applies_{}_changed", origin.label())]
                .as_u64()
                .expect("changed must be a number");
            let total = json[format!("applies_{}_total", origin.label())]
                .as_u64()
                .expect("total must be a number");
            assert_eq!(changed, i as u64 + 1, "{origin:?} changed count");
            assert_eq!(total, i as u64 + 2, "{origin:?} total count");
            assert!(
                total >= changed,
                "{origin:?}: total ({total}) must cover changed ({changed})"
            );
        }
    }

    /// The multiplier's numerator and denominator must come out of the SAME
    /// window, and the same atomic drain must reset both.
    ///
    /// This is the property that makes `total_sends / applies_*_changed`
    /// meaningful at all. If applies survived a drain that reset sends (or
    /// vice versa), the ratio would be computed across mismatched intervals —
    /// the exact defect the single-mutex design exists to prevent for the
    /// per-contract maps.
    #[test]
    fn applies_and_sends_share_one_window_and_one_drain() {
        let mix = PayloadMix::new();
        mix.record_apply(ApplyOrigin::ClientLocal, true);
        mix.record_delivered(PayloadArm::Delta, &contract(1), 100, None, None);
        mix.record_delivered(PayloadArm::Delta, &contract(1), 100, None, None);

        let first = emit(&mix);
        assert_eq!(first["applies_client_local_changed"], 1);
        assert_eq!(first["total_sends"], 2, "one apply fanned out to two peers");

        // Second window: the drain reset BOTH halves, so a quiet window
        // reports zeros rather than re-reporting the previous window's work.
        let second = emit(&mix);
        assert_eq!(second["applies_client_local_changed"], 0);
        assert_eq!(second["applies_client_local_total"], 0);
        assert_eq!(second["total_sends"], 0);
    }

    /// An empty window publishes every apply field as zero.
    #[test]
    fn empty_window_publishes_zero_applies_for_every_origin() {
        let json = emit(&PayloadMix::new());
        for origin in ApplyOrigin::ALL.iter() {
            assert_eq!(
                json[format!("applies_{}_changed", origin.label())],
                0,
                "{origin:?} changed must be present and zero"
            );
            assert_eq!(
                json[format!("applies_{}_total", origin.label())],
                0,
                "{origin:?} total must be present and zero"
            );
        }
    }

    /// Counters saturate rather than wrap.
    ///
    /// A wrapped denominator is worse than a clamped one: it reports a
    /// plausible-looking small number, which yields an absurd multiplier that
    /// reads as a real finding. Clamping at `u64::MAX` is obviously broken.
    #[test]
    fn apply_counters_saturate_rather_than_wrap() {
        let mix = PayloadMix::new();
        {
            let mut w = mix.window.lock();
            let idx = ApplyOrigin::NetworkRelay.index();
            w.applies[idx][1] = u64::MAX;
            w.applies[idx][0] = u64::MAX;
        }
        mix.record_apply(ApplyOrigin::NetworkRelay, true);
        mix.record_apply(ApplyOrigin::NetworkRelay, false);

        let json = emit(&mix);
        assert_eq!(
            json["applies_network_relay_changed"],
            u64::MAX,
            "changed count must clamp, not wrap to 0"
        );
        // `total` sums two saturated halves, and must itself clamp rather than
        // overflow the addition in `Window::applies`.
        assert_eq!(
            json["applies_network_relay_total"],
            u64::MAX,
            "total must clamp, not wrap"
        );
    }

    /// Source-scrape pin: `update_contract` must record BOTH terminal apply
    /// outcomes.
    ///
    /// The two arms are ~100 lines apart in `operations/update.rs` and the
    /// no-change one sits above a block of state-recovery fallback logic, so
    /// the plausible regression is that a refactor keeps the changed arm and
    /// drops the other. That would not fail any assertion — it would just make
    /// every no-op apply vanish from the denominator and shrink the reported
    /// multiplier, silently and in the flattering direction.
    #[test]
    fn update_contract_records_both_terminal_apply_outcomes_pin() {
        let src = include_str!("../../operations/update.rs");
        let start = src
            .find("pub(crate) async fn update_contract(")
            .expect("update_contract not found");
        let body = &src[start..];
        let end = body
            .find("\n/// Send proactive summary notifications")
            .expect("end of update_contract not found");
        let body = &body[..end];

        assert_eq!(
            body.matches("record_apply_with_priority(priority,").count(),
            2,
            "update_contract must record exactly two apply outcomes — the \
             state-changed merge and the merged-to-no-change arm — and both \
             must pass this call's OWN `priority` rather than a hardcoded \
             origin, because `priority` IS the provenance (#5062). Dropping \
             either arm silently biases the fan-out denominator."
        );
        // The changed arm must forward the handler's verdict, not a literal:
        // hardcoding `true` there would count every no-op merge as a
        // broadcast-emitting apply.
        assert!(
            body.contains("record_apply_with_priority(priority, state_changed)"),
            "the state-changed arm must record the handler's own \
             `state_changed` verdict"
        );
    }
}

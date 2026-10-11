//! Shadow-mode detector for **futile repair** on a (peer, contract) edge.
//!
//! # Why this exists
//!
//! Freenet requires a contract's merge to be commutative
//! (`crates/core/.claude/rules/contracts.md`: "Merge MUST be commutative").
//! Nothing enforces it, and nothing can: the core cannot verify contract
//! correctness. When a contract violates it, two peers hold states that no
//! exchange can reconcile — last-write-wins merges oscillate forever, and
//! mutually-rejecting merges (`merge(X, Y) == X` while `merge(Y, X) == Y`)
//! never move at all. Anti-entropy detects the divergence every heartbeat,
//! sends a heal, the heal cannot land, and the next heartbeat detects the same
//! divergence. That loop is unbounded and invisible.
//!
//! Measured on the live network on 2026-08-09, seven contract instances
//! accounted for **32.7% of all update applies** for exactly this reason.
//!
//! # What it measures
//!
//! The core cannot check a contract's merge, but it *can* check whether its own
//! repair worked. That is the signal here, and it is an OUTCOME rather than a
//! threshold on size or rate — a hostile contract cannot evade it by staying
//! small or slow, because staying small and slow while never converging still
//! registers.
//!
//! Per (contract, peer) edge:
//!
//! * A **repair attempt** is a `SyncStateToPeer` heal this node actually
//!   emitted for that edge (see `node::emit_stale_peer_syncs`). Contracts the
//!   heal loop skips — banned, no local state, or over the per-message emit
//!   budget — are NOT attempts: no repair was sent, so the peer cannot be
//!   blamed for not converging.
//! * An **outcome observation** is the next *two-sided* summary comparison on
//!   that edge: both this node and the peer reported a summary for the
//!   contract, and the anti-entropy staleness predicate ruled on them. This
//!   happens in `node::handle_interest_sync_message`, in the `Summaries` arm
//!   (full-bytes comparison) and in the `SummaryDigests` arm's
//!   `DigestVerdict::Agree` branch (the digest proved the peer's summary bytes
//!   are ours, so the same predicate runs on real operands).
//! * The attempt is **futile** when that observation still says the peer is
//!   stale, and **productive** when it says the two sides agree — but only when
//!   the verdict rests on real evidence; see [`OutcomeEvidence`].
//!
//! `consecutive_futile` counts futile outcomes back-to-back on one edge; a
//! single productive outcome resets it to zero. When it reaches
//! [`QUARANTINE_THRESHOLD`] the edge is recorded as one this node WOULD
//! quarantine.
//!
//! # Pairing an attempt with its outcome
//!
//! An attempt stays pending until the **next comparison on that edge, whenever
//! it happens.** There is deliberately no wall-clock deadline, because the
//! interval between two comparisons of one contract is NOT the anti-entropy
//! heartbeat. On links that still take the full-bytes fallback the reply is
//! byte-budgeted (`node::MAX_FALLBACK_SUMMARY_BYTES_PER_REPLY`) and carries
//! only a rotating window of the shared set, so on the heaviest links — which
//! are exactly the heavy-summary rooms this detector is hunting — a given
//! contract comes back round on the order of **ten hours**, not five minutes
//! (see the rustdoc on that constant). An earlier revision expired a pending
//! attempt after 30 minutes, justified as "six heartbeats of headroom"; on
//! those links every single attempt would have expired unsettled, so `futile`
//! and `productive` would both have stayed at zero and `would_quarantine`
//! could never fire. The detector would have been structurally blind on the
//! population it exists to find. (The post-#5238 digest path does not rotate,
//! so this bites hardest during a mixed-version rollout.)
//!
//! What the wall-clock deadline was really guarding against is a peer that
//! disconnects with a heal outstanding and comes back much later, so that an
//! unrelated comparison settles a long-dead attempt. That case is now handled
//! directly rather than by a timer: peer-interest teardown discards the edge
//! (see [`FutileRepairDetector::discard_peer_attempts`], wired to
//! `InterestManager::remove_all_peer_interests_for`, which runs after the
//! disconnect grace period). An attempt's lifetime is therefore the edge's own
//! lifetime.
//!
//! The residual confound — a long gap in which something OTHER than our heal
//! changed the state — is measured rather than gated:
//! [`FutileRepairSnapshot::outcomes_after_long_gap`] counts outcomes settled
//! more than [`LONG_GAP_THRESHOLD`] after their attempt. Those are still
//! classified; the counter is there so the field data can say how much of the
//! headline is carried by them.
//!
//! # Shadow mode
//!
//! Nothing here changes behaviour. There is no quarantine, no suppression, no
//! eviction, no rate limit, no early return. The detector observes and counts;
//! every heal that would have been emitted is still emitted. This release
//! exists to establish the real frequency in production and to prove the
//! detector fires on the known-true positives before it is allowed to act.
//!
//! # What a futile count does NOT prove
//!
//! A non-commutative merge is the motivating cause, but it is not the only way
//! to produce this signature, and the counters do not separate the causes. Read
//! a high count as "this edge is not converging", never as "this contract's
//! merge is broken". Specifically:
//!
//! * **`futile : productive` is not a repair-efficacy ratio.** Attempts are
//!   recorded only for anti-entropy heals, but an edge can converge by another
//!   route entirely — the proximity-overlap heal in the connect handler, or
//!   plain live UPDATE fan-out — and neither records anything, so the next
//!   comparison credits that recovery to whatever anti-entropy heal happened to
//!   be outstanding. `productive` means "the edge had converged by the next
//!   comparison", not "our heal is what converged it".
//! * **A contract with a degenerate delta scores healthiest.** The convergence
//!   verdict comes from the contract's own WASM `get_state_delta`
//!   (`interest::peer_summary_has_pending_state`), and an empty delta reads as
//!   converged, counts productive, and RESETS the streak. A contract that
//!   always returns an empty delta is therefore invisible here — the same trust
//!   assumption the broadcast delta-optimization path already makes, but worth
//!   stating because it runs opposite to this detector's purpose.
//! * **A stuck edge is counted by BOTH of its ends.** Divergence is symmetric:
//!   A sees B stale while B sees A stale (`node.rs`, `Summaries` arm), so both
//!   peers heal, both observe futility, and both count it. Every counter here
//!   is therefore PER OBSERVER. A fleet sum of `would_quarantine` is roughly
//!   **twice** the number of distinct stuck edges, and there is no per-edge
//!   identity on the wire to deduplicate with. Halve it, or read it as an upper
//!   bound.
//! * **Continuous write load looks the same.** A contract written faster than
//!   it converges is genuinely diverged at every observation with no merge
//!   defect at all, as is an edge whose heals are dropped on a full channel or
//!   lost in transit. Telling these apart is what the shadow-mode field data is
//!   for.
//!
//! # Bounded state
//!
//! Per-edge state lives in a fixed-capacity LRU ([`EDGE_CAPACITY`]); the key is
//! remote-influenced (any peer can be interested in any contract), so it MUST
//! be bounded — see the per-key-collection rule in `.claude/rules/code-style.md`.
//! Eviction is not silent: [`FutileRepairSnapshot::tracked_edges`],
//! [`FutileRepairSnapshot::evictions`] and
//! [`FutileRepairSnapshot::evictions_losing_streak`] make saturation directly
//! readable. That is a deliberate correction of the `ms_unt_age` failure
//! (`crate::ring::interest::MISSING_SUMMARY_HISTORY_SIZE`), where a saturated
//! LRU evicting continuously made "no record" indistinguishable from "evicted"
//! and rendered the counter useless in production.
//!
//! # Known limitations, BOTH of which must be resolved before anything ACTS
//!
//! Neither matters while the detector only counts. Both become
//! attacker-controlled the moment a streak gates behaviour.
//!
//! * **A peer can flush other peers' entries out of the LRU.** Each
//!   `Summaries` message can create up to `node::MAX_STALE_SYNCS_PER_SUMMARIES`
//!   (32) fresh keys, so roughly 1,024 such messages sweep the whole cache. The
//!   underlying amplification is pre-existing and capped by that same budget,
//!   and in shadow mode the consequence is only an undercount that
//!   `evictions_losing_streak` reports. Acting on a streak would make this a
//!   way to clear another peer's streak, so this cache needs per-peer fairness
//!   (or per-peer capacity) first.
//! * **A peer chooses which of its contracts get a real verdict.** Entries are
//!   evaluated in the order the sender wrote them and the probe budget is
//!   per-message, so padding a message with 32 novel-byte entries puts
//!   everything after them into [`OutcomeEvidence::ProbeBudgetExhausted`] —
//!   never classified, streak never advanced. That is the RIGHT trade for a
//!   measurement (counting those as futility is a guaranteed false positive
//!   that grows with load, which is the whole reason they are excluded), but it
//!   is an evasion channel for an enforcement mechanism. Watch
//!   `outcomes_probe_budget_exhausted` against `attempts` and
//!   `attempts_superseded`: an edge that is healed every round while never
//!   producing a verdict is the signature, and it is exactly what a contract
//!   hiding from a future quarantine would look like. Closing it means making
//!   the probe budget per-CONTRACT-position-independent (e.g. reserving budget
//!   for contracts with an outstanding attempt) rather than first-come.

use std::num::NonZeroUsize;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use freenet_stdlib::prelude::ContractKey;
use lru::LruCache;
use tokio::time::Instant;

use crate::ring::interest::PeerKey;

/// Consecutive futile repairs on one edge before it is recorded as one this
/// node WOULD quarantine.
///
/// Anti-entropy runs on a ~5-minute heartbeat, so five consecutive futile
/// repairs is at least ~25 minutes of an edge failing to converge despite this
/// node repeatedly shipping it state — and considerably longer on links whose
/// summary rotation is slow (see "Pairing an attempt with its outcome"). That
/// is far outside the ordinary transient (a lost heal, a write landing between
/// the heal and the next comparison, a peer mid-resync), while still firing
/// inside an hour on a genuinely non-convergent contract on a fast link. The
/// whole point of a shadow release is to find out whether that separation
/// actually holds in the field, so this is a starting hypothesis rather than a
/// tuned value.
///
/// SHADOW ONLY: crossing this threshold increments a counter. It does not
/// quarantine, suppress, throttle, or evict anything.
pub(crate) const QUARANTINE_THRESHOLD: u32 = 5;

/// Hard cap on tracked (contract, peer) edges.
///
/// Entries are created only by an emitted repair, never by an observation, so
/// the population is "edges this node has actually healed" rather than the full
/// (hosted contract x connected peer) product. On a busy gateway that product
/// is ~421K (nova: ~2,811 contracts x ~150 connections), but the diverging
/// subset that draws a heal is a small fraction of it, and the per-message emit
/// budget (`node::MAX_STALE_SYNCS_PER_SUMMARIES` = 32) bounds how fast new
/// edges can enter.
///
/// 32,768 entries at ~200 bytes (the key is a `ContractKey` plus a
/// `TransportPublicKey`; the value is ~24 bytes) is a worst case of ~7 MB, in
/// the same range as the 65,536-entry sibling LRU in
/// `crate::ring::interest`. It is chosen for headroom over the expected
/// diverging-edge population, not derived from a measured working set — which
/// is precisely why `tracked_edges` and the two eviction counters are exported.
/// If `tracked_edges` sits at capacity and `evictions_losing_streak` is
/// non-zero in production, this cap is too small and the futility counts are an
/// UNDERCOUNT (an evicted streak restarts at zero and may never reach the
/// threshold). Do not read the counters as a floor without checking those two
/// first.
pub(crate) const EDGE_CAPACITY: usize = 32_768;

/// An attempt settled more than this long after it was emitted is still
/// classified, but is ALSO counted in
/// [`FutileRepairSnapshot::outcomes_after_long_gap`].
///
/// This is a visibility marker, NOT a gate — see "Pairing an attempt with its
/// outcome" in the module docs for why gating on wall-clock age made the
/// detector blind on exactly the links it targets. One hour is well past the
/// ~5-minute heartbeat and past a few missed cycles, so a long gap means the
/// edge's summary rotation is slow (or the peer was quiet), and the chance that
/// something other than our heal moved the state in the meantime is materially
/// higher. If the headline futility count turns out to be carried by long-gap
/// outcomes, it is measuring rotation latency more than repair failure.
pub(crate) const LONG_GAP_THRESHOLD: Duration = Duration::from_secs(60 * 60);

/// Consecutive-futility rungs reported as a survival curve.
///
/// `ladder[i]` counts how many times any edge's consecutive-futile streak
/// REACHED `LADDER_RUNGS[i]`, at most once per rung per streak. The series is
/// monotonically non-increasing in `i`, so it reads directly as "of the streaks
/// that got to 1, how many got to 2, to 4, to 32" — which is the distribution
/// asked for, at fixed cardinality and with no per-contract or per-peer label.
pub(crate) const LADDER_RUNGS: [u32; 8] = [1, 2, 3, 4, 5, 8, 16, 32];

/// Number of rungs in [`LADDER_RUNGS`].
pub(crate) const LADDER_LEN: usize = LADDER_RUNGS.len();

/// What the staleness verdict handed to
/// [`FutileRepairDetector::record_repair_outcome`] actually rests on.
///
/// The anti-entropy handler collapses three very different situations into one
/// `is_stale: bool`, and only one of them is evidence about convergence. The
/// distinction is load-bearing for this detector because the non-evidence cases
/// default to STALE, and their frequency grows with how many contracts a peer
/// reports rather than with how broken anything is: everything past the
/// per-message probe budget (`node::MAX_STALENESS_PROBES_PER_SUMMARIES` = 32)
/// is classified stale every round with no divergence at all. Folding that into
/// `futile` would make the headline number track peer breadth and node load —
/// precisely the "metric re-derived away from the decision" failure in
/// `.claude/rules/bug-prevention-patterns.md`.
///
/// So only [`OutcomeEvidence::Verdict`] classifies. The other two are counted
/// in their own rows and leave the streak — and the outstanding attempt —
/// untouched, so a real verdict on the next exchange still settles it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OutcomeEvidence {
    /// The verdict rests on real evidence about this pair of summaries: the
    /// bytes were identical (trivially converged), or the contract's own
    /// `get_state_delta` answered (freshly probed, or served from the shared
    /// delta cache).
    Verdict,
    /// The summaries differed byte-wise and no semantic verdict was available
    /// because this message's WASM probe budget was already spent, so
    /// `interest::summary_indicates_stale_peer` fell back to the conservative
    /// bytes-differ-means-stale default. Correct as a HEAL decision (the heal
    /// is cheap and the contract is re-evaluated next heartbeat), useless as
    /// evidence that a repair failed.
    ProbeBudgetExhausted,
    /// The summaries differed byte-wise and the probe itself returned no
    /// verdict — the contract's `get_state_delta` errored, timed out, or the
    /// handler answered something unexpected. Same conservative default, same
    /// lack of evidence, different cause: this one tracks contract/runtime
    /// health rather than peer breadth.
    ProbeUnavailable,
}

/// Per-edge shadow state. Deliberately holds no state bytes, no summary, and no
/// timing history beyond the one pending attempt.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct EdgeState {
    /// When a repair was emitted whose outcome has not been observed yet.
    pending_since: Option<Instant>,
    /// Futile outcomes back-to-back; reset to zero by one productive outcome.
    consecutive_futile: u32,
    /// How many rungs of [`LADDER_RUNGS`] the CURRENT streak has already been
    /// counted for, so a long streak contributes to each rung once.
    ladder_recorded: u8,
    /// Whether this edge is currently at or above [`QUARANTINE_THRESHOLD`].
    /// Drives the `edges_at_threshold` gauge and stops one long streak
    /// re-counting the crossing on every further futile outcome.
    at_threshold: bool,
}

impl EdgeState {
    fn has_streak(&self) -> bool {
        self.consecutive_futile > 0
    }
}

/// Fixed-cardinality counters copied into telemetry snapshots.
///
/// Aggregate only: no contract key, no peer identity, no label whose
/// cardinality a remote peer could influence. Every counter is PER OBSERVER —
/// both ends of a diverged edge run this detector and both count it, so a fleet
/// sum is roughly double the distinct population (see the module docs).
///
/// The five outcome rows partition every outcome observation: the sum of
/// `futile`, `productive`, `observations_unpaired`,
/// `outcomes_probe_budget_exhausted` and `outcomes_probe_unavailable` is the
/// total number of comparisons that reached the detector.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct FutileRepairSnapshot {
    /// Repair attempts recorded — `SyncStateToPeer` heals actually emitted.
    pub(crate) attempts: u64,
    /// Attempts whose next evidence-backed comparison still showed divergence.
    ///
    /// NOT the numerator of a repair-efficacy ratio; see `productive`.
    pub(crate) futile: u64,
    /// Attempts whose next evidence-backed comparison showed the edge
    /// converged.
    ///
    /// This says the edge HAD converged by the next comparison, not that our
    /// heal is what converged it. An edge also converges via the
    /// proximity-overlap heal or ordinary live UPDATE fan-out, neither of which
    /// records an attempt, and the next comparison credits whichever
    /// anti-entropy heal was outstanding. So `futile : productive` bounds how
    /// often repair is FOLLOWED BY convergence; it is not a measure of this
    /// node's repair efficacy.
    pub(crate) productive: u64,
    /// Two-sided comparisons on an edge with no attempt outstanding. Not an
    /// error — a converged edge is re-compared every heartbeat without anyone
    /// healing it — but it is the denominator that keeps `futile + productive`
    /// from being read as "all anti-entropy comparisons".
    pub(crate) observations_unpaired: u64,
    /// Attempts replaced by a newer attempt on the same edge before any outcome
    /// was observed. A large value means attempts and outcomes are not
    /// interleaving as assumed and the futility counts are an undercount. It is
    /// expected to be non-zero on links whose summary rotation is slow, where
    /// several heartbeats can heal the same edge before the contract comes back
    /// round to be compared.
    pub(crate) attempts_superseded: u64,
    /// Attempts dropped unsettled because the edge was torn down — the peer
    /// stayed disconnected past the interest grace period, so nothing will ever
    /// settle the attempt. Any streak on that edge is dropped with it, which
    /// costs nothing: a departed peer's streak could not have continued.
    ///
    /// This replaces an earlier wall-clock expiry that made the detector blind
    /// on slow-rotation links; see "Pairing an attempt with its outcome".
    pub(crate) attempts_discarded: u64,
    /// Comparisons where the summaries differed but the per-message WASM probe
    /// budget was spent, so "stale" was a DEFAULT rather than a verdict
    /// ([`OutcomeEvidence::ProbeBudgetExhausted`]). Neither futile nor
    /// productive; the outstanding attempt is left outstanding.
    ///
    /// **Read this before the headline.** It grows with how many contracts a
    /// peer reports per message, so a large value means load is being
    /// classified as staleness in the heal path — safe for healing, but a
    /// load-correlated false positive if it were counted as futility.
    pub(crate) outcomes_probe_budget_exhausted: u64,
    /// Comparisons where the summaries differed and the contract's delta probe
    /// produced no verdict ([`OutcomeEvidence::ProbeUnavailable`]). Neither
    /// futile nor productive; the outstanding attempt is left outstanding.
    /// Tracks contract or runtime health rather than peer breadth.
    pub(crate) outcomes_probe_unavailable: u64,
    /// Of the classified outcomes (`futile + productive`), how many were
    /// settled more than [`LONG_GAP_THRESHOLD`] after their attempt. Not a
    /// separate class — these ARE counted in `futile`/`productive` — but the
    /// longer the gap, the likelier something other than our heal moved the
    /// state. If this dominates, the headline is measuring summary-rotation
    /// latency more than repair failure.
    pub(crate) outcomes_after_long_gap: u64,
    /// Times an edge's consecutive-futile streak reached
    /// [`QUARANTINE_THRESHOLD`] — the headline "would have been quarantined"
    /// count. Counted once per streak: a streak that keeps growing past the
    /// threshold does not re-count, but an edge that converges and later
    /// crosses again does.
    ///
    /// PER OBSERVER: a fleet sum is ~2x the number of distinct stuck edges,
    /// because both ends of a diverged edge count it.
    pub(crate) would_quarantine: u64,
    /// Edges currently at or above [`QUARANTINE_THRESHOLD`]. A gauge, not a
    /// counter — this is the live population a real quarantine would hold.
    ///
    /// An edge leaves this gauge only by converging, by being torn down with
    /// its peer, or by being evicted, so a peer that goes quiet mid-streak
    /// keeps its slot until one of those happens. Read the gauge as "edges that
    /// last looked stuck", not "edges stuck right now"; `would_quarantine` is
    /// the unambiguous count.
    pub(crate) edges_at_threshold: u64,
    /// Current LRU occupancy. Sitting at [`EDGE_CAPACITY`] means saturated.
    pub(crate) tracked_edges: u64,
    /// Entries evicted from the LRU by capacity pressure. Replacing the value
    /// of a key that is already tracked is not an eviction and is not counted.
    pub(crate) evictions: u64,
    /// Evictions whose entry carried a non-zero streak, so a genuine futility
    /// streak was forgotten and restarts at zero. Non-zero means the counts
    /// above are an undercount.
    pub(crate) evictions_losing_streak: u64,
    /// Survival curve over [`LADDER_RUNGS`]; see that constant.
    pub(crate) ladder: [u64; LADDER_LEN],
}

/// Number of scalar counters in [`FutileRepairSnapshot`] before the ladder.
pub(crate) const SNAPSHOT_SCALARS: usize = 14;

impl FutileRepairSnapshot {
    /// Flatten to the fixed-width row exported in
    /// `router::NetworkEfficiencyV1::futile`. Order is load-bearing — it is the
    /// wire contract for the collector — and is documented on that field.
    pub(crate) fn to_row(self) -> [u64; SNAPSHOT_SCALARS] {
        [
            self.attempts,
            self.futile,
            self.productive,
            self.observations_unpaired,
            self.attempts_superseded,
            self.attempts_discarded,
            self.outcomes_probe_budget_exhausted,
            self.outcomes_probe_unavailable,
            self.outcomes_after_long_gap,
            self.would_quarantine,
            self.edges_at_threshold,
            self.tracked_edges,
            self.evictions,
            self.evictions_losing_streak,
        ]
    }
}

struct Metrics {
    attempts: AtomicU64,
    futile: AtomicU64,
    productive: AtomicU64,
    observations_unpaired: AtomicU64,
    attempts_superseded: AtomicU64,
    attempts_discarded: AtomicU64,
    outcomes_probe_budget_exhausted: AtomicU64,
    outcomes_probe_unavailable: AtomicU64,
    outcomes_after_long_gap: AtomicU64,
    would_quarantine: AtomicU64,
    evictions: AtomicU64,
    evictions_losing_streak: AtomicU64,
    ladder: [AtomicU64; LADDER_LEN],
}

impl Metrics {
    fn new() -> Self {
        Self {
            attempts: AtomicU64::new(0),
            futile: AtomicU64::new(0),
            productive: AtomicU64::new(0),
            observations_unpaired: AtomicU64::new(0),
            attempts_superseded: AtomicU64::new(0),
            attempts_discarded: AtomicU64::new(0),
            outcomes_probe_budget_exhausted: AtomicU64::new(0),
            outcomes_probe_unavailable: AtomicU64::new(0),
            outcomes_after_long_gap: AtomicU64::new(0),
            would_quarantine: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            evictions_losing_streak: AtomicU64::new(0),
            ladder: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

/// Shadow-mode futile-repair detector. See the module docs.
pub(crate) struct FutileRepairDetector {
    edges: Mutex<LruCache<(ContractKey, PeerKey), EdgeState>>,
    /// Live count of edges at or above [`QUARANTINE_THRESHOLD`]. Maintained
    /// incrementally rather than scanned, and decremented on eviction and on
    /// teardown so a removed at-threshold edge does not leak into the gauge
    /// forever.
    edges_at_threshold: AtomicU64,
    metrics: Metrics,
}

impl Default for FutileRepairDetector {
    fn default() -> Self {
        Self::new()
    }
}

impl FutileRepairDetector {
    pub(crate) fn new() -> Self {
        Self::with_capacity(EDGE_CAPACITY)
    }

    /// Construct with an explicit capacity. Tests use a tiny cap to exercise
    /// eviction without allocating the production LRU.
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            edges: Mutex::new(LruCache::new(
                NonZeroUsize::new(capacity).expect("futile-repair capacity must be > 0"),
            )),
            edges_at_threshold: AtomicU64::new(0),
            metrics: Metrics::new(),
        }
    }

    /// Record that a `SyncStateToPeer` heal was emitted for this edge.
    ///
    /// Call ONLY where the heal is actually sent. A contract skipped for being
    /// banned, having no local state, or exceeding the per-message emit budget
    /// is not an attempt: no repair was made, so its failure to converge is not
    /// evidence about the peer or the contract's merge.
    pub(crate) fn record_repair_attempt(
        &self,
        contract: &ContractKey,
        peer: &PeerKey,
        now: Instant,
    ) {
        self.metrics.attempts.fetch_add(1, Ordering::Relaxed);
        let mut edges = self.lock();
        let key = (*contract, peer.clone());
        if let Some(state) = edges.get_mut(&key) {
            if state.pending_since.is_some() {
                self.metrics
                    .attempts_superseded
                    .fetch_add(1, Ordering::Relaxed);
            }
            state.pending_since = Some(now);
            return;
        }
        let state = EdgeState {
            pending_since: Some(now),
            ..EdgeState::default()
        };
        // `LruCache::push` returns the displaced entry for BOTH a capacity
        // eviction and a plain replacement of an already-present key, and only
        // the first is an eviction. The `get_mut` fast path above already
        // returns early for a present key, so today only the eviction case can
        // reach here — but `note_eviction` decrements an unsigned gauge, so a
        // replacement slipping through would underflow `edges_at_threshold` to
        // ~1.8e19 and destroy the series. Compare the returned key instead of
        // depending on that fast path staying where it is.
        if let Some((displaced_key, displaced)) = edges.push(key.clone(), state)
            && displaced_key != key
        {
            self.note_eviction(displaced);
        }
    }

    /// Record the result of a two-sided summary comparison for this edge.
    ///
    /// `converged` is the anti-entropy staleness verdict inverted: `true` when
    /// the predicate ruled the peer is NOT stale. Both operands must be real
    /// summaries — a comparison where either side reported nothing is not an
    /// outcome and must not be passed here, because there is no divergence to
    /// have repaired.
    ///
    /// `evidence` says what that verdict rests on, and only
    /// [`OutcomeEvidence::Verdict`] classifies the attempt. See that type: the
    /// other two are conservative defaults whose frequency tracks node load and
    /// peer breadth, so folding them in would make the headline number grow
    /// with how busy this node is rather than with how stuck the network is.
    ///
    /// Only an observation that follows an outstanding attempt is classified.
    /// Everything else is counted as unpaired and changes no streak: the point
    /// of the detector is whether OUR REPAIRS work, and an edge nobody healed
    /// says nothing about that either way.
    pub(crate) fn record_repair_outcome(
        &self,
        contract: &ContractKey,
        peer: &PeerKey,
        converged: bool,
        evidence: OutcomeEvidence,
        now: Instant,
    ) {
        // Checked BEFORE the pending attempt is consumed: a defaulted verdict
        // teaches us nothing about the outstanding heal, so the attempt stays
        // outstanding for the next comparison that does carry evidence.
        match evidence {
            OutcomeEvidence::ProbeBudgetExhausted => {
                self.metrics
                    .outcomes_probe_budget_exhausted
                    .fetch_add(1, Ordering::Relaxed);
                return;
            }
            OutcomeEvidence::ProbeUnavailable => {
                self.metrics
                    .outcomes_probe_unavailable
                    .fetch_add(1, Ordering::Relaxed);
                return;
            }
            OutcomeEvidence::Verdict => {}
        }

        let mut edges = self.lock();
        let key = (*contract, peer.clone());
        let Some(state) = edges.get_mut(&key) else {
            self.metrics
                .observations_unpaired
                .fetch_add(1, Ordering::Relaxed);
            return;
        };
        let Some(pending_since) = state.pending_since.take() else {
            self.metrics
                .observations_unpaired
                .fetch_add(1, Ordering::Relaxed);
            return;
        };
        // Visibility only — the outcome is classified either way. See
        // `LONG_GAP_THRESHOLD`.
        if now.saturating_duration_since(pending_since) > LONG_GAP_THRESHOLD {
            self.metrics
                .outcomes_after_long_gap
                .fetch_add(1, Ordering::Relaxed);
        }

        if converged {
            self.metrics.productive.fetch_add(1, Ordering::Relaxed);
            state.consecutive_futile = 0;
            state.ladder_recorded = 0;
            if std::mem::take(&mut state.at_threshold) {
                self.edges_at_threshold.fetch_sub(1, Ordering::Relaxed);
            }
            return;
        }

        self.metrics.futile.fetch_add(1, Ordering::Relaxed);
        state.consecutive_futile = state.consecutive_futile.saturating_add(1);
        let streak = state.consecutive_futile;
        while (state.ladder_recorded as usize) < LADDER_LEN
            && streak >= LADDER_RUNGS[state.ladder_recorded as usize]
        {
            self.metrics.ladder[state.ladder_recorded as usize].fetch_add(1, Ordering::Relaxed);
            state.ladder_recorded += 1;
        }
        if !state.at_threshold && streak >= QUARANTINE_THRESHOLD {
            state.at_threshold = true;
            self.metrics
                .would_quarantine
                .fetch_add(1, Ordering::Relaxed);
            self.edges_at_threshold.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Drop this peer's edges for `contracts`, because the peer's interest
    /// state is being torn down (it stayed disconnected past the grace period).
    ///
    /// This is what bounds an attempt's lifetime now that pairing is not
    /// wall-clock gated: nothing will ever settle a heal sent to a peer that is
    /// gone, so leaving the attempt outstanding would let a comparison after
    /// some later reconnect settle it as though the long-dead heal had just
    /// failed. See "Pairing an attempt with its outcome" in the module docs.
    pub(crate) fn discard_peer_attempts<'a>(
        &self,
        peer: &PeerKey,
        contracts: impl IntoIterator<Item = &'a ContractKey>,
    ) {
        let mut edges = self.lock();
        for contract in contracts {
            let Some(state) = edges.pop(&(*contract, peer.clone())) else {
                continue;
            };
            if state.pending_since.is_some() {
                self.metrics
                    .attempts_discarded
                    .fetch_add(1, Ordering::Relaxed);
            }
            if state.at_threshold {
                self.edges_at_threshold.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }

    pub(crate) fn snapshot(&self) -> FutileRepairSnapshot {
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        let tracked_edges = self.lock().len() as u64;
        FutileRepairSnapshot {
            attempts: load(&self.metrics.attempts),
            futile: load(&self.metrics.futile),
            productive: load(&self.metrics.productive),
            observations_unpaired: load(&self.metrics.observations_unpaired),
            attempts_superseded: load(&self.metrics.attempts_superseded),
            attempts_discarded: load(&self.metrics.attempts_discarded),
            outcomes_probe_budget_exhausted: load(&self.metrics.outcomes_probe_budget_exhausted),
            outcomes_probe_unavailable: load(&self.metrics.outcomes_probe_unavailable),
            outcomes_after_long_gap: load(&self.metrics.outcomes_after_long_gap),
            would_quarantine: load(&self.metrics.would_quarantine),
            edges_at_threshold: load(&self.edges_at_threshold),
            tracked_edges,
            evictions: load(&self.metrics.evictions),
            evictions_losing_streak: load(&self.metrics.evictions_losing_streak),
            ladder: std::array::from_fn(|i| load(&self.metrics.ladder[i])),
        }
    }

    fn note_eviction(&self, evicted: EdgeState) {
        self.metrics.evictions.fetch_add(1, Ordering::Relaxed);
        if evicted.has_streak() {
            self.metrics
                .evictions_losing_streak
                .fetch_add(1, Ordering::Relaxed);
        }
        if evicted.at_threshold {
            self.edges_at_threshold.fetch_sub(1, Ordering::Relaxed);
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, LruCache<(ContractKey, PeerKey), EdgeState>> {
        self.edges.lock().unwrap_or_else(|poisoned| {
            // Diagnostic-only state: a poisoned lock must never take down the
            // anti-entropy path it is observing.
            poisoned.into_inner()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use freenet_stdlib::prelude::{CodeHash, ContractInstanceId};

    /// Deterministic-and-distinct, mirroring the sibling helper in
    /// `crate::ring::interest`'s test module.
    fn contract(seed: u8) -> ContractKey {
        ContractKey::from_id_and_code(
            ContractInstanceId::new([seed; 32]),
            CodeHash::new([seed.wrapping_add(1); 32]),
        )
    }

    fn peer(seed: u8) -> PeerKey {
        let mut bytes = [0u8; 32];
        bytes[0] = seed;
        PeerKey(crate::transport::TransportPublicKey::from_bytes(bytes))
    }

    fn t0() -> Instant {
        Instant::now()
    }

    /// The load-bearing test: futility is an OUTCOME, not a count of attempts.
    ///
    /// Two edges receive the SAME number of repair attempts. The only
    /// difference is what the following comparison said. If the detector
    /// counted attempts (or counted every outcome as futile), both edges would
    /// look identical and this test fails — which is the mutation documented in
    /// the PR: replacing `converged` with `false` at the outcome site makes
    /// `productive` 0, `futile` 6, and `would_quarantine` 2.
    #[test]
    fn futility_counts_the_outcome_not_the_attempt() {
        let detector = FutileRepairDetector::with_capacity(16);
        let now = t0();
        let converging = contract(1);
        let stuck = contract(2);
        let p = peer(1);

        // Edge that converges: every repair lands.
        for _ in 0..QUARANTINE_THRESHOLD {
            detector.record_repair_attempt(&converging, &p, now);
            detector.record_repair_outcome(&converging, &p, true, OutcomeEvidence::Verdict, now);
        }
        // Edge that never converges: identical attempt count, opposite outcome.
        for _ in 0..QUARANTINE_THRESHOLD {
            detector.record_repair_attempt(&stuck, &p, now);
            detector.record_repair_outcome(&stuck, &p, false, OutcomeEvidence::Verdict, now);
        }

        let snap = detector.snapshot();
        assert_eq!(
            snap.attempts,
            u64::from(QUARANTINE_THRESHOLD) * 2,
            "both edges attempted the same number of repairs"
        );
        assert_eq!(
            snap.productive,
            u64::from(QUARANTINE_THRESHOLD),
            "the converging edge's repairs must all count as productive"
        );
        assert_eq!(
            snap.futile,
            u64::from(QUARANTINE_THRESHOLD),
            "only the non-convergent edge's repairs are futile"
        );
        assert_eq!(
            snap.would_quarantine, 1,
            "exactly one edge reached the threshold — a detector counting \
             attempts rather than outcomes would report two"
        );
        assert_eq!(
            snap.edges_at_threshold, 1,
            "the converging edge must never be at the threshold"
        );
    }

    /// A single productive repair clears the streak, so an edge that converges
    /// just before the threshold never reaches it however many times it has
    /// already failed.
    #[test]
    fn one_productive_repair_resets_the_streak() {
        let detector = FutileRepairDetector::with_capacity(16);
        let now = t0();
        let key = contract(3);
        let p = peer(1);

        for _ in 0..(QUARANTINE_THRESHOLD - 1) {
            detector.record_repair_attempt(&key, &p, now);
            detector.record_repair_outcome(&key, &p, false, OutcomeEvidence::Verdict, now);
        }
        detector.record_repair_attempt(&key, &p, now);
        detector.record_repair_outcome(&key, &p, true, OutcomeEvidence::Verdict, now);
        assert_eq!(detector.snapshot().would_quarantine, 0);

        // ...and the streak restarts from zero, so it takes the full threshold
        // again rather than one more failure.
        for _ in 0..(QUARANTINE_THRESHOLD - 1) {
            detector.record_repair_attempt(&key, &p, now);
            detector.record_repair_outcome(&key, &p, false, OutcomeEvidence::Verdict, now);
        }
        assert_eq!(
            detector.snapshot().would_quarantine,
            0,
            "the streak must restart at zero after a productive repair"
        );
        detector.record_repair_attempt(&key, &p, now);
        detector.record_repair_outcome(&key, &p, false, OutcomeEvidence::Verdict, now);
        let snap = detector.snapshot();
        assert_eq!(snap.would_quarantine, 1);
        assert_eq!(
            snap.productive, 1,
            "the one landed repair stays visible as productive"
        );
    }

    /// An unhealed edge is not evidence. Comparisons with no outstanding
    /// attempt are counted separately and move no streak.
    #[test]
    fn observations_without_an_attempt_are_unpaired() {
        let detector = FutileRepairDetector::with_capacity(16);
        let now = t0();
        let key = contract(4);
        let p = peer(1);

        for _ in 0..10 {
            detector.record_repair_outcome(&key, &p, false, OutcomeEvidence::Verdict, now);
        }
        let snap = detector.snapshot();
        assert_eq!(snap.observations_unpaired, 10);
        assert_eq!(
            snap.futile, 0,
            "no repair was attempted, so none was futile"
        );
        assert_eq!(snap.would_quarantine, 0);
        assert_eq!(snap.tracked_edges, 0, "observations must not create edges");

        // A second observation after the attempt is settled is also unpaired.
        detector.record_repair_attempt(&key, &p, now);
        detector.record_repair_outcome(&key, &p, false, OutcomeEvidence::Verdict, now);
        detector.record_repair_outcome(&key, &p, false, OutcomeEvidence::Verdict, now);
        let snap = detector.snapshot();
        assert_eq!(snap.futile, 1, "one attempt, one futile outcome");
        assert_eq!(snap.observations_unpaired, 11);
    }

    /// HIGH-1 regression: an attempt is settled by the NEXT comparison on the
    /// edge, however long that takes.
    ///
    /// The heavy-summary links this detector hunts are exactly the ones whose
    /// full-bytes summary rotation is byte-budgeted, so one contract comes back
    /// round on the order of ten hours rather than at the ~5-minute heartbeat
    /// (see `node::MAX_FALLBACK_SUMMARY_BYTES_PER_REPLY`). Under the 30-minute
    /// wall-clock expiry this replaces, every attempt on such a link expired
    /// unsettled: `futile` and `productive` both stayed 0 and
    /// `would_quarantine` could never fire.
    ///
    /// Mutation that must fail this test: reinstate any wall-clock gate that
    /// drops the attempt (`if now - pending_since > THIRTY_MINUTES { return; }`)
    /// ahead of classification.
    #[test]
    fn a_slow_rotation_still_settles_the_attempt() {
        let detector = FutileRepairDetector::with_capacity(16);
        let now = t0();
        let key = contract(5);
        let p = peer(1);
        // The reconstructed worst case from the fallback-rotation rustdoc.
        let ten_hours = Duration::from_secs(10 * 60 * 60);

        for _ in 0..QUARANTINE_THRESHOLD {
            detector.record_repair_attempt(&key, &p, now);
            detector.record_repair_outcome(
                &key,
                &p,
                false,
                OutcomeEvidence::Verdict,
                now + ten_hours,
            );
        }

        let snap = detector.snapshot();
        assert_eq!(
            snap.futile,
            u64::from(QUARANTINE_THRESHOLD),
            "an outcome observed a rotation period later must still settle its \
             attempt — a wall-clock expiry here blinds the detector on exactly \
             the heavy-summary links it exists to find"
        );
        assert_eq!(
            snap.would_quarantine, 1,
            "the streak must be able to reach the threshold on a slow link"
        );
        assert_eq!(
            snap.outcomes_after_long_gap,
            u64::from(QUARANTINE_THRESHOLD),
            "long-gap settlements are classified, but must stay separately \
             visible so the field data can say how much of the headline they \
             carry"
        );
        assert_eq!(
            snap.attempts_discarded, 0,
            "nothing was torn down, so nothing was discarded"
        );
    }

    /// The other half of HIGH-1: a settlement inside the long-gap window is not
    /// flagged, so `outcomes_after_long_gap` genuinely discriminates instead of
    /// counting everything.
    #[test]
    fn a_prompt_settlement_is_not_flagged_as_a_long_gap() {
        let detector = FutileRepairDetector::with_capacity(16);
        let now = t0();
        let key = contract(6);
        let p = peer(1);

        detector.record_repair_attempt(&key, &p, now);
        detector.record_repair_outcome(
            &key,
            &p,
            false,
            OutcomeEvidence::Verdict,
            now + LONG_GAP_THRESHOLD,
        );
        let snap = detector.snapshot();
        assert_eq!(snap.futile, 1);
        assert_eq!(
            snap.outcomes_after_long_gap, 0,
            "at the threshold exactly is not past it"
        );

        detector.record_repair_attempt(&key, &p, now);
        detector.record_repair_outcome(
            &key,
            &p,
            false,
            OutcomeEvidence::Verdict,
            now + LONG_GAP_THRESHOLD + Duration::from_secs(1),
        );
        assert_eq!(detector.snapshot().outcomes_after_long_gap, 1);
    }

    /// Peer teardown, not a timer, is what bounds an attempt's lifetime: after
    /// the edge is discarded a later comparison is unpaired rather than being
    /// settled as if the long-dead heal had just failed.
    #[test]
    fn peer_teardown_discards_the_outstanding_attempt() {
        let detector = FutileRepairDetector::with_capacity(16);
        let now = t0();
        let key = contract(7);
        let p = peer(1);
        let other = peer(2);

        detector.record_repair_attempt(&key, &p, now);
        detector.record_repair_attempt(&key, &other, now);
        detector.discard_peer_attempts(&p, [&key]);

        let snap = detector.snapshot();
        assert_eq!(snap.attempts_discarded, 1);
        assert_eq!(
            snap.tracked_edges, 1,
            "only the departing peer's edge is dropped"
        );

        // A comparison after the teardown has nothing to settle.
        detector.record_repair_outcome(&key, &p, false, OutcomeEvidence::Verdict, now);
        let snap = detector.snapshot();
        assert_eq!(
            snap.futile, 0,
            "a heal to a peer that left must not be settled by a comparison \
             made after it came back"
        );
        assert_eq!(snap.observations_unpaired, 1);

        // The surviving peer's edge is untouched.
        detector.record_repair_outcome(&key, &other, false, OutcomeEvidence::Verdict, now);
        assert_eq!(detector.snapshot().futile, 1);
    }

    /// Tearing down an at-threshold edge must release the live gauge, exactly
    /// as eviction does, or `edges_at_threshold` drifts upward forever.
    #[test]
    fn teardown_releases_the_at_threshold_gauge() {
        let detector = FutileRepairDetector::with_capacity(16);
        let now = t0();
        let key = contract(8);
        let p = peer(1);
        for _ in 0..QUARANTINE_THRESHOLD {
            detector.record_repair_attempt(&key, &p, now);
            detector.record_repair_outcome(&key, &p, false, OutcomeEvidence::Verdict, now);
        }
        assert_eq!(detector.snapshot().edges_at_threshold, 1);

        detector.discard_peer_attempts(&p, [&key]);
        let snap = detector.snapshot();
        assert_eq!(snap.edges_at_threshold, 0);
        assert_eq!(
            snap.would_quarantine, 1,
            "the crossing already happened and stays counted"
        );
        assert_eq!(
            snap.attempts_discarded, 0,
            "the last outcome settled the attempt, so there was nothing \
             outstanding to discard"
        );
    }

    /// HIGH-2: a verdict that is really the conservative default must not be
    /// counted as futility.
    ///
    /// Past `node::MAX_STALENESS_PROBES_PER_SUMMARIES` (32) byte-differing
    /// contracts in one `Summaries` message, `summary_indicates_stale_peer`
    /// defaults to STALE with no probe run. That frequency grows with peer
    /// breadth and node load, not with brokenness, so folding it into `futile`
    /// would make the headline number a load metric.
    ///
    /// Mutation that must fail this test: pass `OutcomeEvidence::Verdict` at
    /// the outcome sites regardless of how the verdict was reached — which is
    /// exactly the pre-fix behaviour, where `is_stale` alone was passed.
    #[test]
    fn a_defaulted_verdict_is_not_futility() {
        let detector = FutileRepairDetector::with_capacity(16);
        let now = t0();
        let key = contract(9);
        let p = peer(1);

        detector.record_repair_attempt(&key, &p, now);
        for _ in 0..QUARANTINE_THRESHOLD {
            // "stale", but only because the probe budget was spent.
            detector.record_repair_outcome(
                &key,
                &p,
                false,
                OutcomeEvidence::ProbeBudgetExhausted,
                now,
            );
        }
        for _ in 0..QUARANTINE_THRESHOLD {
            // "stale", but only because the contract's delta probe failed.
            detector.record_repair_outcome(&key, &p, false, OutcomeEvidence::ProbeUnavailable, now);
        }

        let snap = detector.snapshot();
        assert_eq!(
            snap.futile, 0,
            "a default is not evidence that a repair failed"
        );
        assert_eq!(snap.would_quarantine, 0);
        assert_eq!(
            snap.outcomes_probe_budget_exhausted,
            u64::from(QUARANTINE_THRESHOLD),
            "budget-exhausted defaults get their own row so the headline is \
             readable against them"
        );
        assert_eq!(
            snap.outcomes_probe_unavailable,
            u64::from(QUARANTINE_THRESHOLD),
            "probe failures are a distinct cause from budget exhaustion"
        );
        assert_eq!(
            snap.observations_unpaired, 0,
            "an evidence-free comparison is its own class, not an unpaired one"
        );

        // The attempt is still outstanding: the first comparison that DOES
        // carry evidence settles it.
        detector.record_repair_outcome(&key, &p, false, OutcomeEvidence::Verdict, now);
        let snap = detector.snapshot();
        assert_eq!(
            snap.futile, 1,
            "a defaulted comparison must not consume the outstanding attempt"
        );
        assert_eq!(snap.attempts, 1, "one heal was emitted, so one attempt");
    }

    /// The evidence-free classes must not silently swallow a real convergence
    /// either — the streak is untouched in BOTH directions.
    #[test]
    fn a_defaulted_verdict_does_not_reset_a_streak() {
        let detector = FutileRepairDetector::with_capacity(16);
        let now = t0();
        let key = contract(10);
        let p = peer(1);

        for _ in 0..(QUARANTINE_THRESHOLD - 1) {
            detector.record_repair_attempt(&key, &p, now);
            detector.record_repair_outcome(&key, &p, false, OutcomeEvidence::Verdict, now);
        }
        // A "converged" reading with no evidence behind it must not clear the
        // streak that four real verdicts built.
        detector.record_repair_outcome(&key, &p, true, OutcomeEvidence::ProbeUnavailable, now);
        detector.record_repair_attempt(&key, &p, now);
        detector.record_repair_outcome(&key, &p, false, OutcomeEvidence::Verdict, now);

        assert_eq!(
            detector.snapshot().would_quarantine,
            1,
            "an evidence-free reading must move the streak in neither direction"
        );
    }

    #[test]
    fn repeated_attempts_without_an_outcome_are_counted_as_superseded() {
        let detector = FutileRepairDetector::with_capacity(16);
        let now = t0();
        let key = contract(11);
        let p = peer(1);

        detector.record_repair_attempt(&key, &p, now);
        detector.record_repair_attempt(&key, &p, now);
        detector.record_repair_attempt(&key, &p, now);
        let snap = detector.snapshot();
        assert_eq!(snap.attempts, 3);
        assert_eq!(snap.attempts_superseded, 2);
        assert_eq!(snap.futile, 0);
        assert_eq!(
            snap.evictions, 0,
            "re-recording an attempt on a tracked edge replaces its value; \
             that is not an eviction and must never be counted as one — \
             `note_eviction` decrements an unsigned gauge"
        );
    }

    /// Re-recording an attempt on an edge that is ALREADY at the threshold must
    /// not be mistaken for an eviction.
    ///
    /// `LruCache::push` returns the displaced entry for a plain replacement as
    /// well as for a capacity eviction, and `note_eviction` decrements
    /// `edges_at_threshold`, which is unsigned: charging a replacement as an
    /// eviction drives the gauge below the population it is counting and then
    /// wraps it to ~1.8e19, and inflates `evictions` /
    /// `evictions_losing_streak` — the two rows a reader is told to check
    /// FIRST to decide whether the counts are trustworthy. Today the `get_mut`
    /// fast path returns before `push` is reached, so the guard at the push
    /// site is what makes this safe structurally rather than by the ordering of
    /// two statements. This test states the invariant so a refactor of that
    /// fast path has something to break.
    #[test]
    fn re_recording_an_attempt_on_an_at_threshold_edge_is_not_an_eviction() {
        let detector = FutileRepairDetector::with_capacity(16);
        let now = t0();
        let key = contract(12);
        let p = peer(1);

        for _ in 0..QUARANTINE_THRESHOLD {
            detector.record_repair_attempt(&key, &p, now);
            detector.record_repair_outcome(&key, &p, false, OutcomeEvidence::Verdict, now);
        }
        assert_eq!(detector.snapshot().edges_at_threshold, 1);

        // The next heartbeat heals the same stuck edge again.
        detector.record_repair_attempt(&key, &p, now);

        let snap = detector.snapshot();
        assert_eq!(
            snap.evictions, 0,
            "replacing a tracked edge's value is not an eviction: {snap:?}"
        );
        assert_eq!(
            snap.evictions_losing_streak, 0,
            "nothing was forgotten, so the undercount signal must stay clean: \
             {snap:?}"
        );
        assert_eq!(
            snap.edges_at_threshold, 1,
            "the at-threshold gauge must survive re-healing a stuck edge: \
             {snap:?}"
        );
        assert_eq!(snap.tracked_edges, 1);
    }

    /// The ladder is a survival curve: each streak counts once per rung it
    /// reaches, so the series is monotonically non-increasing.
    #[test]
    fn ladder_is_a_monotone_survival_curve() {
        let detector = FutileRepairDetector::with_capacity(16);
        let now = t0();
        let p = peer(1);
        // Three edges with streaks of 1, 4 and 32.
        for (seed, streak) in [(20u8, 1u32), (21, 4), (22, 32)] {
            let key = contract(seed);
            for _ in 0..streak {
                detector.record_repair_attempt(&key, &p, now);
                detector.record_repair_outcome(&key, &p, false, OutcomeEvidence::Verdict, now);
            }
        }
        let ladder = detector.snapshot().ladder;
        // Rungs are 1, 2, 3, 4, 5, 8, 16, 32.
        assert_eq!(ladder, [3, 2, 2, 2, 1, 1, 1, 1]);
        for window in ladder.windows(2) {
            assert!(
                window[0] >= window[1],
                "survival curve must be non-increasing, got {ladder:?}"
            );
        }
    }

    /// LRU eviction is visible, not silent — the `ms_unt_age` lesson.
    #[test]
    fn eviction_is_observable() {
        let detector = FutileRepairDetector::with_capacity(2);
        let now = t0();
        let p = peer(1);

        // Build a streak on edge 30, then push it out with two other edges.
        let victim = contract(30);
        detector.record_repair_attempt(&victim, &p, now);
        detector.record_repair_outcome(&victim, &p, false, OutcomeEvidence::Verdict, now);
        for seed in [31u8, 32] {
            detector.record_repair_attempt(&contract(seed), &p, now);
        }
        let snap = detector.snapshot();
        assert_eq!(snap.tracked_edges, 2, "occupancy is capped");
        assert_eq!(snap.evictions, 1);
        assert_eq!(
            snap.evictions_losing_streak, 1,
            "an evicted streak must be reported, or futility counts silently \
             undercount"
        );
    }

    /// An at-threshold edge evicted from the LRU must leave the live gauge,
    /// otherwise `edges_at_threshold` drifts upward forever.
    #[test]
    fn eviction_releases_the_at_threshold_gauge() {
        let detector = FutileRepairDetector::with_capacity(1);
        let now = t0();
        let p = peer(1);
        let victim = contract(40);
        for _ in 0..QUARANTINE_THRESHOLD {
            detector.record_repair_attempt(&victim, &p, now);
            detector.record_repair_outcome(&victim, &p, false, OutcomeEvidence::Verdict, now);
        }
        assert_eq!(detector.snapshot().edges_at_threshold, 1);
        detector.record_repair_attempt(&contract(41), &p, now);
        let snap = detector.snapshot();
        assert_eq!(snap.edges_at_threshold, 0);
        assert_eq!(
            snap.would_quarantine, 1,
            "the crossing already happened and stays counted"
        );
    }

    /// Distinct peers on the same contract are distinct edges: one peer failing
    /// to converge must not be charged to another that did.
    #[test]
    fn edges_are_per_peer_not_per_contract() {
        let detector = FutileRepairDetector::with_capacity(16);
        let now = t0();
        let key = contract(50);
        let stuck = peer(1);
        let healthy = peer(2);

        for _ in 0..QUARANTINE_THRESHOLD {
            detector.record_repair_attempt(&key, &stuck, now);
            detector.record_repair_outcome(&key, &stuck, false, OutcomeEvidence::Verdict, now);
            detector.record_repair_attempt(&key, &healthy, now);
            detector.record_repair_outcome(&key, &healthy, true, OutcomeEvidence::Verdict, now);
        }
        let snap = detector.snapshot();
        assert_eq!(snap.would_quarantine, 1);
        assert_eq!(snap.edges_at_threshold, 1);
        assert_eq!(snap.futile, u64::from(QUARANTINE_THRESHOLD));
        assert_eq!(snap.productive, u64::from(QUARANTINE_THRESHOLD));
    }

    #[test]
    fn snapshot_row_order_is_the_documented_wire_contract() {
        let snap = FutileRepairSnapshot {
            attempts: 1,
            futile: 2,
            productive: 3,
            observations_unpaired: 4,
            attempts_superseded: 5,
            attempts_discarded: 6,
            outcomes_probe_budget_exhausted: 7,
            outcomes_probe_unavailable: 8,
            outcomes_after_long_gap: 9,
            would_quarantine: 10,
            edges_at_threshold: 11,
            tracked_edges: 12,
            evictions: 13,
            evictions_losing_streak: 14,
            ladder: [0; LADDER_LEN],
        };
        assert_eq!(
            snap.to_row(),
            [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]
        );
    }

    /// The five outcome rows must partition every outcome observation, which is
    /// what lets a reader check the headline against the rows that qualify it.
    #[test]
    fn outcome_rows_partition_every_observation() {
        let detector = FutileRepairDetector::with_capacity(16);
        let now = t0();
        let p = peer(1);
        let mut observations = 0u64;

        for (seed, converged, evidence) in [
            (60u8, false, OutcomeEvidence::Verdict),
            (61, true, OutcomeEvidence::Verdict),
            (62, false, OutcomeEvidence::ProbeBudgetExhausted),
            (63, false, OutcomeEvidence::ProbeUnavailable),
        ] {
            let key = contract(seed);
            detector.record_repair_attempt(&key, &p, now);
            for _ in 0..3 {
                detector.record_repair_outcome(&key, &p, converged, evidence, now);
                observations += 1;
            }
        }

        let snap = detector.snapshot();
        assert_eq!(
            snap.futile
                + snap.productive
                + snap.observations_unpaired
                + snap.outcomes_probe_budget_exhausted
                + snap.outcomes_probe_unavailable,
            observations,
            "every call to record_repair_outcome must land in exactly one of \
             the five outcome rows: {snap:?}"
        );
    }
}

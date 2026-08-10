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
//!   stale, and **productive** when it says the two sides agree.
//!
//! `consecutive_futile` counts futile outcomes back-to-back on one edge; a
//! single productive outcome resets it to zero. When it reaches
//! [`QUARANTINE_THRESHOLD`] the edge is recorded as one this node WOULD
//! quarantine.
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
//! to produce this signature. A contract under continuous write load can be
//! genuinely diverged at every observation without any merge defect; so can an
//! edge whose heals are being dropped on a full channel or lost in transit.
//! The detector deliberately does not try to separate those — telling them
//! apart is what the shadow-mode field data is for. Read a high count as "this
//! edge is not converging", never as "this contract's merge is broken".
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
/// repairs is roughly 25 minutes of an edge failing to converge despite this
/// node repeatedly shipping it state. That is far outside the ordinary
/// transient (a lost heal, a write landing between the heal and the next
/// comparison, a peer mid-resync), while still firing inside an hour on a
/// genuinely non-convergent contract — the whole point of a shadow release is
/// to find out whether that separation actually holds in the field, so this is
/// a starting hypothesis rather than a tuned value.
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

/// A pending attempt older than this is not treated as the cause of the
/// observation that follows it.
///
/// Attempts and outcomes are paired by event order, not by a token on the wire,
/// so a peer that disconnects with a heal outstanding and reconnects hours
/// later would otherwise have that stale attempt settled by an unrelated
/// comparison. At the ~5-minute anti-entropy heartbeat, 30 minutes is six
/// cycles of headroom over a legitimate outcome while still discarding an
/// attempt whose context is gone. Expired attempts are counted separately
/// ([`FutileRepairSnapshot::attempts_expired`]) and classified as neither
/// futile nor productive.
pub(crate) const ATTEMPT_MAX_AGE: Duration = Duration::from_secs(30 * 60);

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

/// Per-edge shadow state. 16 bytes; deliberately holds no state bytes, no
/// summary, and no timing history beyond the one pending attempt.
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
/// cardinality a remote peer could influence.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct FutileRepairSnapshot {
    /// Repair attempts recorded — `SyncStateToPeer` heals actually emitted.
    pub(crate) attempts: u64,
    /// Attempts whose next two-sided comparison still showed divergence.
    pub(crate) futile: u64,
    /// Attempts whose next two-sided comparison showed the edge converged.
    pub(crate) productive: u64,
    /// Two-sided comparisons on an edge with no attempt outstanding. Not an
    /// error — a converged edge is re-compared every heartbeat without anyone
    /// healing it — but it is the denominator that keeps `futile + productive`
    /// from being read as "all anti-entropy comparisons".
    pub(crate) observations_unpaired: u64,
    /// Attempts replaced by a newer attempt on the same edge before any outcome
    /// was observed. Expected to be near zero: the heal is emitted after the
    /// comparison that settles the previous one, in the same handler call. A
    /// large value means attempts and outcomes are not interleaving as assumed
    /// and the futility counts are an undercount.
    pub(crate) attempts_superseded: u64,
    /// Attempts discarded unsettled because the next observation arrived more
    /// than [`ATTEMPT_MAX_AGE`] later.
    pub(crate) attempts_expired: u64,
    /// Times an edge's consecutive-futile streak reached
    /// [`QUARANTINE_THRESHOLD`] — the headline "would have been quarantined"
    /// count. Counted once per streak: a streak that keeps growing past the
    /// threshold does not re-count, but an edge that converges and later
    /// crosses again does.
    pub(crate) would_quarantine: u64,
    /// Edges currently at or above [`QUARANTINE_THRESHOLD`]. A gauge, not a
    /// counter — this is the live population a real quarantine would hold.
    ///
    /// An edge leaves this gauge only by converging or by being evicted, so a
    /// peer that disconnects mid-streak keeps its slot until the LRU forgets
    /// it. Read the gauge as "edges that last looked stuck", not "edges stuck
    /// right now"; `would_quarantine` is the unambiguous count.
    pub(crate) edges_at_threshold: u64,
    /// Current LRU occupancy. Sitting at [`EDGE_CAPACITY`] means saturated.
    pub(crate) tracked_edges: u64,
    /// Entries evicted from the LRU.
    pub(crate) evictions: u64,
    /// Evictions whose entry carried a non-zero streak, so a genuine futility
    /// streak was forgotten and restarts at zero. Non-zero means the counts
    /// above are an undercount.
    pub(crate) evictions_losing_streak: u64,
    /// Survival curve over [`LADDER_RUNGS`]; see that constant.
    pub(crate) ladder: [u64; LADDER_LEN],
}

/// Number of scalar counters in [`FutileRepairSnapshot`] before the ladder.
pub(crate) const SNAPSHOT_SCALARS: usize = 11;

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
            self.attempts_expired,
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
    attempts_expired: AtomicU64,
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
            attempts_expired: AtomicU64::new(0),
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
    /// incrementally rather than scanned, and decremented on eviction so an
    /// evicted at-threshold edge does not leak into the gauge forever.
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
        if let Some((_, evicted)) = edges.push(key, state) {
            self.note_eviction(evicted);
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
    /// Only an observation that follows an outstanding attempt is classified.
    /// Everything else is counted as unpaired and changes no streak: the point
    /// of the detector is whether OUR REPAIRS work, and an edge nobody healed
    /// says nothing about that either way.
    pub(crate) fn record_repair_outcome(
        &self,
        contract: &ContractKey,
        peer: &PeerKey,
        converged: bool,
        now: Instant,
    ) {
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
        if now.saturating_duration_since(pending_since) > ATTEMPT_MAX_AGE {
            self.metrics
                .attempts_expired
                .fetch_add(1, Ordering::Relaxed);
            return;
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

    pub(crate) fn snapshot(&self) -> FutileRepairSnapshot {
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        let tracked_edges = self.lock().len() as u64;
        FutileRepairSnapshot {
            attempts: load(&self.metrics.attempts),
            futile: load(&self.metrics.futile),
            productive: load(&self.metrics.productive),
            observations_unpaired: load(&self.metrics.observations_unpaired),
            attempts_superseded: load(&self.metrics.attempts_superseded),
            attempts_expired: load(&self.metrics.attempts_expired),
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
            detector.record_repair_outcome(&converging, &p, true, now);
        }
        // Edge that never converges: identical attempt count, opposite outcome.
        for _ in 0..QUARANTINE_THRESHOLD {
            detector.record_repair_attempt(&stuck, &p, now);
            detector.record_repair_outcome(&stuck, &p, false, now);
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
            detector.record_repair_outcome(&key, &p, false, now);
        }
        detector.record_repair_attempt(&key, &p, now);
        detector.record_repair_outcome(&key, &p, true, now);
        assert_eq!(detector.snapshot().would_quarantine, 0);

        // ...and the streak restarts from zero, so it takes the full threshold
        // again rather than one more failure.
        for _ in 0..(QUARANTINE_THRESHOLD - 1) {
            detector.record_repair_attempt(&key, &p, now);
            detector.record_repair_outcome(&key, &p, false, now);
        }
        assert_eq!(
            detector.snapshot().would_quarantine,
            0,
            "the streak must restart at zero after a productive repair"
        );
        detector.record_repair_attempt(&key, &p, now);
        detector.record_repair_outcome(&key, &p, false, now);
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
            detector.record_repair_outcome(&key, &p, false, now);
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
        detector.record_repair_outcome(&key, &p, false, now);
        detector.record_repair_outcome(&key, &p, false, now);
        let snap = detector.snapshot();
        assert_eq!(snap.futile, 1, "one attempt, one futile outcome");
        assert_eq!(snap.observations_unpaired, 11);
    }

    #[test]
    fn a_stale_attempt_expires_rather_than_being_settled() {
        let detector = FutileRepairDetector::with_capacity(16);
        let now = t0();
        let key = contract(5);
        let p = peer(1);

        detector.record_repair_attempt(&key, &p, now);
        detector.record_repair_outcome(
            &key,
            &p,
            false,
            now + ATTEMPT_MAX_AGE + Duration::from_secs(1),
        );
        let snap = detector.snapshot();
        assert_eq!(snap.attempts_expired, 1);
        assert_eq!(snap.futile, 0);
        assert_eq!(snap.productive, 0);

        // Inside the window it settles normally.
        detector.record_repair_attempt(&key, &p, now);
        detector.record_repair_outcome(&key, &p, false, now + ATTEMPT_MAX_AGE);
        assert_eq!(detector.snapshot().futile, 1);
    }

    #[test]
    fn repeated_attempts_without_an_outcome_are_counted_as_superseded() {
        let detector = FutileRepairDetector::with_capacity(16);
        let now = t0();
        let key = contract(6);
        let p = peer(1);

        detector.record_repair_attempt(&key, &p, now);
        detector.record_repair_attempt(&key, &p, now);
        detector.record_repair_attempt(&key, &p, now);
        let snap = detector.snapshot();
        assert_eq!(snap.attempts, 3);
        assert_eq!(snap.attempts_superseded, 2);
        assert_eq!(snap.futile, 0);
    }

    /// The ladder is a survival curve: each streak counts once per rung it
    /// reaches, so the series is monotonically non-increasing.
    #[test]
    fn ladder_is_a_monotone_survival_curve() {
        let detector = FutileRepairDetector::with_capacity(16);
        let now = t0();
        let p = peer(1);
        // Three edges with streaks of 1, 4 and 32.
        for (seed, streak) in [(10u8, 1u32), (11, 4), (12, 32)] {
            let key = contract(seed);
            for _ in 0..streak {
                detector.record_repair_attempt(&key, &p, now);
                detector.record_repair_outcome(&key, &p, false, now);
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

        // Build a streak on edge 20, then push it out with two other edges.
        let victim = contract(20);
        detector.record_repair_attempt(&victim, &p, now);
        detector.record_repair_outcome(&victim, &p, false, now);
        for seed in [21u8, 22] {
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
        let victim = contract(30);
        for _ in 0..QUARANTINE_THRESHOLD {
            detector.record_repair_attempt(&victim, &p, now);
            detector.record_repair_outcome(&victim, &p, false, now);
        }
        assert_eq!(detector.snapshot().edges_at_threshold, 1);
        detector.record_repair_attempt(&contract(31), &p, now);
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
        let key = contract(40);
        let stuck = peer(1);
        let healthy = peer(2);

        for _ in 0..QUARANTINE_THRESHOLD {
            detector.record_repair_attempt(&key, &stuck, now);
            detector.record_repair_outcome(&key, &stuck, false, now);
            detector.record_repair_attempt(&key, &healthy, now);
            detector.record_repair_outcome(&key, &healthy, true, now);
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
            attempts_expired: 6,
            would_quarantine: 7,
            edges_at_threshold: 8,
            tracked_edges: 9,
            evictions: 10,
            evictions_losing_streak: 11,
            ladder: [0; LADDER_LEN],
        };
        assert_eq!(snap.to_row(), [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
    }
}

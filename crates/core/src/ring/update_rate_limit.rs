//! Per-(peer, contract) UPDATE rate limiter — front-line defense against
//! the May 21 incident pattern where one peer floods one contract with
//! many UPDATE/s.
//!
//! ## What this does
//!
//! Maintains a `(sender_addr, contract_instance_id) → last_accepted_at`
//! map. When a new UPDATE arrives, check the time since the last
//! accepted UPDATE for the same pair. If under [`MIN_UPDATE_INTERVAL`],
//! reject; otherwise stamp the new time and accept.
//!
//! ## What this is NOT
//!
//! - **Not the MAD outlier detector.** That's `crate::governance` and
//!   `crate::contract::governance` — those react in *minutes* via the
//!   reaper loop. This module reacts in *milliseconds* at the receive
//!   boundary.
//! - **Not a token-bucket smoothing layer.** Sustained bursts are
//!   rejected outright, not queued. A flood pattern hits a flat ceiling.
//! - **Not aware of intent.** A "renewing subscription that happened to
//!   trigger an UPDATE" looks identical to "DOS flood at the same rate";
//!   the rate is a flat statistical threshold, and we choose it
//!   generously enough that legitimate traffic doesn't hit it.
//!
//! ## Default rate
//!
//! [`MIN_UPDATE_INTERVAL`] defaults to 100ms (≈10 UPDATEs/sec from a
//! single peer for a single contract). Realistic human-driven contract
//! updates (chat, settings change, post) are orders of magnitude slower
//! than this. The 4PjqN5… incident was producing many UPDATEs per
//! second sustained — well above this ceiling.
//!
//! ## Bounded growth
//!
//! Two layers:
//!
//! 1. **Hard cap on entry count** ([`MAX_TRACKED_PAIRS`]). At the cap,
//!    the oldest entries are evicted to make room — an attacker
//!    churning distinct `(sender, contract)` pairs cannot grow the map
//!    past this size. Critical because the address space is attacker-
//!    chosen (32-byte contract id × any source address).
//! 2. **Periodic TTL sweep** ([`UpdateRateLimiter::cleanup`]) drops
//!    entries idle for longer than [`CLEANUP_AGE`]. Hooked into the
//!    Ring's existing reaper tick.
//!
//! The cap alone is sufficient to bound memory; the TTL sweep
//! reclaims space from genuinely-idle pairs so the cap isn't
//! prematurely reached under normal traffic.
//!
//! ### Why the cap evicts rather than rejects (#4981)
//!
//! It used to reject: at the cap a *new* pair was refused and never
//! inserted, so it stayed "new" forever and every subsequent UPDATE
//! from it was dropped and re-counted. The module reasoned that "only
//! NEW pairs hit the cap, and the TTL sweep gives smooth recovery as
//! idle pairs roll off" — true only for *idle* pairs. An accepted
//! UPDATE restamps its entry, so a busy pair never ages out under
//! [`CLEANUP_AGE`]; the slots were held permanently by whoever got in
//! first and stayed active, and newcomers were locked out with no
//! recovery path. That is the permanently-refreshable GC exemption
//! `AGENTS.md` forbids ("any condition that exempts an entry from
//! garbage collection MUST either expire via TTL, or be overridden by
//! an absolute age threshold"): the TTL was refreshable without limit.
//!
//! Saturation is not evidence of an attack. Tracked pairs are distinct
//! senders × distinct contracts relayed from them, so 50 peers × ~330
//! contracts ≈ 16 500 exceeds the cap with no attacker involved. A fix
//! that assumes the newcomer is hostile penalises the ordinary case of
//! a healthy node outgrowing the bound.
//!
//! What eviction costs: an evicted pair's next UPDATE is treated as new
//! and therefore allowed, so an attacker churning fresh contract ids
//! gets one UPDATE through per id instead of being cut off after the
//! first 16 384. That ceiling was incidental rather than designed — the
//! cap exists to bound *memory* — but it was not nothing, and nothing
//! else in the node replaced it: the ban list is keyed by contract, the
//! MAD outlier detector is off by default and excludes contracts younger
//! than its ramp-up anyway, and the transport rate-limits handshakes
//! rather than traffic on an established connection. Since a node
//! forwards an UPDATE for a contract it does not host, an unbounded
//! fresh-id stream would make it a relay amplifier. So the ceiling is
//! replaced deliberately rather than dropped — see below.
//!
//! Sustained floods from a *stable* pair, which is the pattern this
//! limiter exists for, are unaffected by eviction: that entry is the
//! most recently used, so it is the last thing evicted.
//!
//! ### The fresh-pair budget, keyed by sender alone
//!
//! Eviction removes the *incidental* bound on fresh-id churn, so a
//! deliberate one takes its place: a token bucket keyed by `sender_addr`
//! ALONE ([`NEW_PAIR_BURST`], [`NEW_PAIR_REFILL_INTERVAL`]) that a
//! sender spends from only when it presents a `(sender, contract)` pair
//! the limiter is not currently tracking. Note "not currently tracking"
//! rather than "has never seen" — the two differ once eviction is in
//! play, and the difference is the caveat below.
//!
//! Two properties make this the right shape:
//!
//! - **Traffic for a pair the limiter is tracking never touches it.** A
//!   token is spent only when the pair is not currently in the map, so a
//!   peer relaying heavily for contracts the limiter holds is bounded by
//!   [`MIN_UPDATE_INTERVAL`] per pair and by nothing else.
//! - **It is charged before eviction, not after.** A sender past its
//!   budget is refused *before* a slot is reserved or anything is
//!   evicted, so churning fresh ids cannot push other peers' entries out
//!   of the map on the way to being throttled.
//!
//! The caveat, stated rather than glossed: "not currently in the map"
//! includes a pair that WAS tracked and then got evicted. The limiter
//! cannot tell a returning pair from a genuinely fresh one without
//! remembering what it evicted, which is exactly the unbounded memory
//! the cap exists to refuse. So on a node whose working set far exceeds
//! [`MAX_TRACKED_PAIRS`] — where the miss rate is high — this behaves
//! less like a fresh-id bound and more like a per-peer aggregate
//! ceiling.
//!
//! How high that miss rate goes depends on the access pattern, not
//! just the working-set size: measured at ~16 500 pairs against the
//! 16 384 cap, uniform-random access misses 1.33% of the time but
//! round-robin misses 96.77% — LRU's worst case, since every entry is
//! evicted exactly before it is next used. So the miss rate is NOT
//! reliably small, and the safety of this control does not rest on it
//! being small.
//!
//! What it rests on is the binding condition, which is generous
//! regardless of miss rate: **one peer must sustain ~210 UPDATE/s to
//! this node, continuously**, before anything is dropped (the 200/s
//! refill plus the burst amortised). Real gateway load is ~3.15/s on
//! nova and ~2.84/s on vega NODE-WIDE, across all peers — roughly two
//! orders of magnitude below the threshold for a single one of them.
//! That is why the sustained rate is set well above
//! [`MIN_UPDATE_INTERVAL`]'s per-pair ceiling rather than at it.
//!
//! If it ever does bind on real traffic it is visible as
//! `new_pair_budget_rejected_total` climbing on a node with no attacker,
//! which is the signal to raise the rate — the counter is on the
//! dashboard for exactly that reason.
//!
//! Its own map is bounded by [`MAX_TRACKED_SENDERS`], which is a
//! multiple of the node's connection cap rather than an attacker-chosen
//! space: `sender` is the immediate upstream hop, so only a connected
//! peer can occupy an entry.
//!
//! ## Semantic note: `sender_addr` is the immediate upstream hop
//!
//! The key uses the SocketAddr of the peer that *sent us* the
//! message — not the originator of the UPDATE transaction. This is
//! deliberate: the limiter is a **receiver-side resource guard**.
//! "How much of my CPU/memory am I willing to spend processing
//! UPDATEs from a single immediate peer for a single contract?" is
//! the question this answers. If A floods through B to reach us, B
//! is who hits our limiter — and that's correct, because B is who
//! is consuming our resources.
//!
//! Originator-level protection is a different layer (Phase 7 ban
//! enforcement, where a banned originator's traffic is rejected
//! even if relayed). The two layers compose; neither subsumes the
//! other.
//!
//! ## Design doc reference
//!
//! `docs/design/contract-hardening.md` Phase 2: *"`TrackedBackoff<(PeerId,
//! ContractInstanceId)>`. Apply at `SyncStateToPeer` emit + originator
//! UPDATE entry. Reject with typed marker."*
//!
//! Divergences from the design doc, with rationale:
//!
//! - **Flat ceiling instead of `TrackedBackoff` exponential.** A flat
//!   100ms ceiling catches the May 21 pattern; exponential repeat-
//!   offender cooldown can land as a follow-up if observation shows
//!   it's needed.
//! - **Inbound relay receive-boundary, not `SyncStateToPeer` emit.**
//!   The doc's wording targets outbound emit. We instead gate at
//!   inbound receive (4 wire-variant sites: `RequestUpdate`,
//!   `BroadcastTo`, `RequestUpdateStreaming`, `BroadcastToStreaming`).
//!   Receive-side guards a peer's own resources directly; emit-side
//!   guards against being an amplifier. Receive is the higher-value
//!   first cut; emit is a follow-up.
//! - **No typed wire-level error.** Rejected UPDATEs are dropped
//!   silently. The sender's own retry / governance scoring will
//!   detect the flood pattern from its end.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dashmap::DashMap;
use freenet_stdlib::prelude::ContractInstanceId;
use tokio::time::Instant;

use super::Ring;
use super::resync_rate_limit::{BucketOutcome, TokenBucketLimiter};
use crate::util::time_source::TimeSource;

/// Minimum interval between accepted UPDATEs for the same
/// `(sender, contract)` pair. Default 100ms (≈10/s). UPDATEs arriving
/// faster than this are dropped.
///
/// Calibration rationale: legitimate contract updates are
/// human-cadence (seconds to minutes between writes). The 4PjqN5…
/// May 21 incident was producing many UPDATEs/s sustained. 100ms is
/// generous enough to never trigger on a real user while blocking the
/// flood pattern instantly.
pub(crate) const MIN_UPDATE_INTERVAL: Duration = Duration::from_millis(100);

/// How long an idle `(sender, contract)` entry stays in the map before
/// being cleared by the periodic cleanup pass. Bounds memory while
/// preserving the rate-limit signal across short-term retries.
pub(crate) const CLEANUP_AGE: Duration = Duration::from_secs(5 * 60);

/// Hard upper bound on the number of tracked `(sender, contract)`
/// pairs. This is the "no unbounded per-key collection for attacker-
/// influenced keys" rule from `.claude/rules/code-style.md`. At 64
/// bytes/entry, 16 384 pairs ≈ 1 MB — tiny — but bounds the worst case
/// where an attacker chooses fresh contract ids.
///
/// # Re-measure the eviction scan before raising this
///
/// Memory is not the binding cost. An eviction pass holds
/// [`UpdateRateLimiter::eviction_lock`] across a scan that is LINEAR in
/// this constant — measured 41 ns per entry, so **672 µs at 16 384 but
/// 10.8 ms at 163 840**. At the current value that is a ~0.001% duty
/// cycle against real gateway load (~3.15 UPDATE/s on nova, ~2.84/s on
/// vega) and irrelevant; ten times larger it is a 10 ms blocking
/// section on a tokio worker.
///
/// This warning is here because the PR that added eviction also
/// installed the operator story "evictions climbing means saturation",
/// and the obvious response to that reading is to raise this constant.
/// Doing so is fine — but re-measure the hold time first, and if it
/// grows past a millisecond or two, the scan wants a different shape
/// (sampled victim selection, or a maintained ordering) rather than a
/// longer lock.
///
/// On reaching the cap the oldest entries are evicted to admit the
/// newcomer (see the module docs for why rejecting instead starved new
/// pairs permanently, #4981). The bound itself is unchanged: eviction
/// keeps the map at or below this size at all times.
pub(crate) const MAX_TRACKED_PAIRS: usize = 16_384;

/// When the map is full, evict `max_tracked_pairs / this` entries in one
/// pass rather than one entry per admission.
///
/// Finding the oldest entry means walking every shard, which cannot be
/// done while holding a shard guard (see [`UpdateRateLimiter::size`] for
/// the deadlock this module already hit once). Under the saturation this
/// fix targets the map is *persistently* full, so a one-entry-per-
/// admission policy would walk 16 384 entries on every new pair — at the
/// 50-100 new pairs/sec reported in #4981, a continuous full scan of the
/// map several times a second on the UPDATE receive path. Evicting a
/// batch amortises that to one scan per batch (≈1.5/sec at the same
/// rate) for the same eviction *policy*: the oldest entries go first.
///
/// 64 gives a 256-entry batch at the default cap, ≈1.5% of it. Small
/// enough that an evicted pair is genuinely among the least recently
/// used, large enough that the scan is not the dominant cost.
const EVICTION_BATCH_DIVISOR: usize = 64;

/// How often a capacity eviction may write a log line.
///
/// Eviction on a saturated node is continuous, so this is throttled to
/// keep it to roughly a line a minute while still being visible in
/// release builds — the whole point of logging it (#4981: the previous
/// drop path logged at `debug!`, which `release_max_level_info` compiles
/// out, so the only evidence a production node discarded legitimate
/// UPDATEs was a dashboard tile).
const EVICTION_LOG_INTERVAL: Duration = Duration::from_secs(60);

/// Bound on admission attempts before giving up with `CapacityExceeded`.
///
/// One attempt, one eviction, one insert is the expected path. The bound
/// exists because an admission can lose its slot to a concurrent caller,
/// and an unbounded retry loop would let sustained contention spin here
/// on the receive path.
///
/// Six rather than three. An attempt is consumed whenever the map went
/// from full to not-full and back between this caller's cap check and
/// its eviction — no work is done, but the attempt is spent — and the
/// last attempt cannot evict (there would be no attempt left to use the
/// slot), so the budget buys one fewer eviction than it looks like.
/// Measured on the worst ratio in the suite, a 1024 cap whose 16-entry
/// batch matches the 16 concurrent callers: at three attempts 7-26 of
/// 6 400 admissions were dropped across a dozen runs, at four 0-3, at
/// five 0-2, and at six none at all. Extra attempts are nearly free now
/// that the scan is serialised — a waiter blocks on the eviction lock
/// instead of running its own redundant scan.
const MAX_ADMISSION_ATTEMPTS: usize = 6;

/// Burst of brand-new `(sender, contract)` pairs a single sender may
/// introduce before [`NEW_PAIR_REFILL_INTERVAL`] starts to bind.
///
/// Sized to absorb the legitimate bursty cases — a peer that reconnects
/// and re-subscribes, or one that starts relaying for a batch of
/// contracts at once — with room to spare over the ~330 pairs per peer
/// implied by the saturation #4981 reported (≈16 500 pairs across ≈50
/// peers).
const NEW_PAIR_BURST: f64 = 1024.0;

/// Sustained rate at which one sender may present pairs the limiter is
/// not currently tracking, once its burst is spent: one per interval.
///
/// 5ms, i.e. 200/s — twenty times [`MIN_UPDATE_INTERVAL`]'s per-pair
/// ceiling. Deliberately not set equal to it: under saturation a pair
/// that was tracked and got evicted comes back through this same path
/// (see the module docs), so a rate tuned for genuinely-fresh ids would
/// throttle ordinary relayed traffic on exactly the busy node this fix
/// exists to keep serving. A peer must sustain 200 UPDATEs/s to this
/// node, continuously, before anything is dropped, while a fresh-id
/// flood is still bounded instead of unlimited.
const NEW_PAIR_REFILL_INTERVAL: Duration = Duration::from_millis(5);

/// Headroom multiple over the node's configured `max_connections` for
/// the new-pair budget's own map.
///
/// One entry per peer address that has presented a new pair. `sender` is
/// the immediate upstream hop, so the live set is bounded by the
/// connection cap; the multiple is headroom for address churn
/// (reconnects, NAT rebinding) between [`CLEANUP_AGE`] sweeps. At ~48
/// bytes an entry, 8x the default 200-connection cap is well under
/// 100 KB.
///
/// Derived from the configured value rather than a constant: a node
/// raised above [`Ring::DEFAULT_MAX_CONNECTIONS`] would otherwise run a
/// budget map smaller than its own peer count.
const SENDER_TRACKING_HEADROOM: usize = 8;

/// How often an exhausted new-pair budget may write a log line.
///
/// Shares [`EVICTION_LOG_INTERVAL`]'s reasoning: a sender that is being
/// throttled is being throttled continuously, so this must be throttled
/// too or it becomes the log flood it is meant to report.
const NEW_PAIR_LOG_INTERVAL: Duration = EVICTION_LOG_INTERVAL;

/// Outcome of an at-capacity eviction pass.
///
/// This is an enum rather than a count of removed entries because
/// "removed nothing" is almost never a reason to give up, and reading it
/// as one silently dropped legitimate UPDATEs. A pass removes nothing in
/// three situations, only the last of which is terminal:
///
/// 1. Another caller already freed capacity, so the cap no longer binds.
/// 2. Every victim this pass selected was removed concurrently — under
///    contention the likeliest case, since concurrent callers all
///    partition the *same* map and therefore select nearly the same
///    oldest entries.
/// 3. The map is empty, so there is nothing to evict and never will be.
///
/// 1, 2 and 3 all mean a slot is, or may already be, free: the right
/// move is to retry the admission, which is what
/// [`MAX_ADMISSION_ATTEMPTS`] exists for. Conflating them with the one
/// genuinely terminal case dropped ~0.9% of admissions at the production
/// cap under 16-thread contention, none of them for the documented
/// reason, and without consuming a single retry attempt (#4997 review).
///
/// Note that 3 — an empty map — is NOT terminal, and inferring "the cap
/// must be 0" from it was a second instance of the same mistake. A map
/// with a perfectly good cap reads as empty in two windows: while
/// [`UpdateRateLimiter::cleanup`] is sweeping, and while every caller
/// that has won a slot is still between its reservation and its insert
/// (reachable at small caps under contention, e.g. 8 slots and 64
/// callers). In both, `size` is legitimately at the cap while the map is
/// not, and slots are about to exist. The only terminal condition is a
/// cap of zero, which is a property of the configuration and is read
/// from it directly rather than inferred.
enum EvictionOutcome {
    /// This pass freed slots and kept one of them for the caller, which
    /// therefore does not have to win the cap check again — it inserts
    /// directly. See [`UpdateRateLimiter::evict_oldest`].
    Reserved,
    /// A slot is, or may already be, free, but this caller did not free
    /// it and holds no claim on it. Retry the admission.
    Retry,
    /// `max_tracked_pairs` is 0, so no admission can ever succeed.
    /// Terminal, and the only terminal case.
    CapIsZero,
}

/// Whether a throttled log line is due, stamping `slot` when it is.
///
/// Both saturation signals in this module fire continuously once they
/// fire at all, so both need this; sharing it keeps the two throttles
/// from drifting apart.
fn log_due(slot: &Mutex<Option<Instant>>, now: Instant, interval: Duration) -> bool {
    let mut last = match slot.lock() {
        Ok(guard) => guard,
        // Poisoned only if a previous holder panicked while formatting a
        // log line. Losing the throttle is not worth propagating a panic
        // onto the UPDATE receive path.
        Err(poisoned) => poisoned.into_inner(),
    };
    let due = match *last {
        Some(prev) => now.saturating_duration_since(prev) >= interval,
        None => true,
    };
    if due {
        *last = Some(now);
    }
    due
}

/// Outcome of an UPDATE rate-limit check. Callers must treat any
/// non-`Allowed` variant as "drop this message at the receive
/// boundary, do not spawn a relay driver."
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RateLimitDecision {
    /// Allowed — proceed with normal UPDATE handling. The accepted
    /// timestamp has been stamped atomically.
    Allowed,
    /// Rejected — too soon after the previous accepted UPDATE from this
    /// `(sender, contract)` pair. Carries the elapsed time vs the
    /// configured minimum for diagnostic logging.
    Rejected {
        elapsed: Duration,
        min_interval: Duration,
    },
    /// Rejected because the tracking map is at [`MAX_TRACKED_PAIRS`],
    /// this is a new pair, and eviction could not free a slot for it.
    ///
    /// Since #4981 this is close to unreachable: a full map always has
    /// an oldest entry to evict, so reaching here means every admission
    /// attempt lost its freed slot to a concurrent caller (see
    /// [`MAX_ADMISSION_ATTEMPTS`]). Kept distinct from `Rejected` so the
    /// dashboard can still tell "throttling working as intended" apart
    /// from "limiter overflowing", and because a rising count now means
    /// something sharper than it used to: not merely that the map is
    /// full, but that it is full *and* contended.
    CapacityExceeded,
    /// Rejected because the sender has spent its budget for presenting
    /// `(sender, contract)` pairs the limiter is not tracking (see the
    /// module docs).
    ///
    /// Only a pair that is not currently in the map can land here, so a
    /// sender's traffic for a tracked pair never does, however much of
    /// it there is. Below saturation that is the same as "never seen"
    /// and a rising count is the fresh-id churn pattern; once eviction
    /// is recycling pairs faster than they return, it also counts
    /// re-admissions, which is why the rate is set for aggregate traffic
    /// rather than for genuinely-fresh ids.
    SenderNewPairBudget,
}

impl RateLimitDecision {
    /// Convenience for the common branch shape `if !decision.is_allowed()`.
    pub fn is_allowed(self) -> bool {
        matches!(self, RateLimitDecision::Allowed)
    }
}

/// Per-(sender_addr, contract_instance_id) UPDATE rate limiter.
///
/// All state lives in a single [`DashMap`] keyed by the pair. Each
/// entry stores only the `Instant` of the most recent accepted UPDATE,
/// so per-entry memory is tiny.
pub(crate) struct UpdateRateLimiter {
    last_accepted: DashMap<(SocketAddr, ContractInstanceId), Instant>,
    /// Authoritative size counter for capacity enforcement. Held
    /// in sync with `last_accepted.len()` at every stable point
    /// (incremented by successful new-pair inserts in
    /// `check_and_record`; decremented by the count of dropped
    /// entries in `cleanup`).
    ///
    /// Why not just use `last_accepted.len()`? Because `len()` walks
    /// every shard, so it can't be called while holding any shard
    /// guard (the previous probe-then-insert iteration deadlocked on
    /// this). With this counter, the cap check is a single atomic
    /// `fetch_add`, which strictly serializes — no overshoot from
    /// concurrent inserts. Codex re-review of #4285 caught that the
    /// `len()` precheck overshoots by `num_concurrent_callers`,
    /// which can be hundreds.
    size: AtomicUsize,
    min_interval: Duration,
    max_tracked_pairs: usize,
    time_source: Arc<dyn TimeSource + Send + Sync>,
    /// Total accepted UPDATEs since limiter creation. Surfaced on the
    /// dashboard so operators can see the limiter's signal-to-noise.
    accepted_total: AtomicU64,
    /// Total rejected UPDATEs since limiter creation (rate-limit hits).
    rejected_total: AtomicU64,
    /// Total UPDATEs rejected because the tracking map was at capacity
    /// when a new `(sender, contract)` pair tried to register and
    /// eviction could not free a slot for it. Surfaced separately
    /// because since #4981 a non-zero value means the map is full *and*
    /// contended, not merely full.
    capacity_rejected_total: AtomicU64,
    /// Total entries evicted to admit new pairs at capacity. This is the
    /// operator's "the limiter is saturated" signal, and it replaces
    /// `capacity_rejected_total` in that role: a healthy busy node now
    /// shows evictions climbing and capacity rejections flat.
    capacity_evicted_total: AtomicU64,
    /// When the last eviction log line was written, for
    /// [`EVICTION_LOG_INTERVAL`] throttling. Only touched on the
    /// eviction path, which already walks the map, so the lock is not
    /// on the common path.
    last_eviction_log: Mutex<Option<Instant>>,
    /// Held across the eviction scan so only one caller selects victims
    /// at a time — see [`UpdateRateLimiter::evict_oldest`] for why
    /// concurrent evictors otherwise collide on the same victims. This
    /// is the outermost lock in the module: nothing else acquires it,
    /// and no shard guard is held when it is taken, so it cannot
    /// participate in a lock cycle.
    eviction_lock: Mutex<()>,
    /// Per-sender budget for introducing brand-new pairs — the
    /// deliberate replacement for the fresh-id ceiling eviction removed
    /// (see the module docs).
    ///
    /// Consulted only from the new-pair branch of `check_and_record`,
    /// while that branch holds a `last_accepted` shard guard. That is
    /// the only place either map is touched under the other's guard, so
    /// the lock order (`last_accepted` then `new_pair_budget`) has no
    /// counterpart to invert against.
    new_pair_budget: TokenBucketLimiter<SocketAddr>,
    /// Total UPDATEs dropped because the sender was over its new-pair
    /// budget.
    new_pair_budget_rejected_total: AtomicU64,
    /// Total new pairs admitted WITHOUT a budget check because the
    /// sender map was full. Should be zero: the map is sized well above
    /// the connection cap, and this failing open is a safety valve, not
    /// an expected path. A non-zero value means `max_tracked_senders` is
    /// undersized for this node.
    new_pair_budget_untracked_total: AtomicU64,
    /// When the last new-pair-budget log line was written, for
    /// [`NEW_PAIR_LOG_INTERVAL`] throttling.
    last_new_pair_log: Mutex<Option<Instant>>,
}

impl UpdateRateLimiter {
    /// `max_connections` is this node's configured connection cap; the
    /// per-sender budget's map is sized from it (see
    /// [`SENDER_TRACKING_HEADROOM`]).
    pub fn new(time_source: Arc<dyn TimeSource + Send + Sync>, max_connections: usize) -> Self {
        Self::with_new_pair_budget(
            time_source,
            MIN_UPDATE_INTERVAL,
            MAX_TRACKED_PAIRS,
            NEW_PAIR_BURST,
            max_connections.saturating_mul(SENDER_TRACKING_HEADROOM),
        )
    }

    /// Test/bench constructor: the production path is [`Self::new`],
    /// which sizes the per-sender budget from the node's own connection
    /// cap rather than the default.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn with_config(
        time_source: Arc<dyn TimeSource + Send + Sync>,
        min_interval: Duration,
        max_tracked_pairs: usize,
    ) -> Self {
        Self::with_new_pair_budget(
            time_source,
            min_interval,
            max_tracked_pairs,
            NEW_PAIR_BURST,
            Ring::DEFAULT_MAX_CONNECTIONS * SENDER_TRACKING_HEADROOM,
        )
    }

    /// [`Self::with_config`] with the per-sender new-pair budget sized
    /// explicitly. Fixtures that deliberately flood fresh pairs from one
    /// sender to exercise the capacity/eviction path pass a burst large
    /// enough that the budget cannot be what they observe.
    pub fn with_new_pair_budget(
        time_source: Arc<dyn TimeSource + Send + Sync>,
        min_interval: Duration,
        max_tracked_pairs: usize,
        new_pair_burst: f64,
        max_tracked_senders: usize,
    ) -> Self {
        Self {
            new_pair_budget: TokenBucketLimiter::new(
                time_source.clone(),
                new_pair_burst,
                NEW_PAIR_REFILL_INTERVAL,
                max_tracked_senders,
            ),
            new_pair_budget_rejected_total: AtomicU64::new(0),
            new_pair_budget_untracked_total: AtomicU64::new(0),
            last_new_pair_log: Mutex::new(None),
            last_accepted: DashMap::new(),
            size: AtomicUsize::new(0),
            min_interval,
            max_tracked_pairs,
            time_source,
            accepted_total: AtomicU64::new(0),
            rejected_total: AtomicU64::new(0),
            capacity_rejected_total: AtomicU64::new(0),
            capacity_evicted_total: AtomicU64::new(0),
            last_eviction_log: Mutex::new(None),
            eviction_lock: Mutex::new(()),
        }
    }

    /// Check whether an UPDATE from `sender` for `contract` is allowed
    /// right now, and atomically stamp the accepted timestamp if so.
    ///
    /// Two-property atomicity:
    ///
    /// 1. **Same-pair compare-and-stamp** is atomic via
    ///    [`DashMap::entry`] holding the per-shard guard across
    ///    timestamp comparison and update. Exactly one caller per
    ///    pair wins per `min_interval` window.
    ///
    /// 2. **Capacity enforcement** is strict via the [`Self::size`]
    ///    atomic counter. New-pair insertion reserves a slot with
    ///    `fetch_add` BEFORE inserting; if the post-increment value
    ///    exceeds the cap, the reservation is returned via
    ///    `fetch_sub` and the call returns `CapacityExceeded`.
    ///    Concurrent distinct-key inserts strictly serialize through
    ///    the counter — no overshoot.
    ///
    /// If rejected on rate, no map mutation is made: a barrage of
    /// rejected attempts doesn't extend the rate window (the existing
    /// `last_accepted` timestamp is unchanged) and doesn't grow memory.
    ///
    /// At capacity a new pair evicts the oldest entries rather than
    /// being refused (#4981) — see the module docs for why refusing
    /// starved newcomers permanently, and what eviction costs.
    pub fn check_and_record(
        &self,
        sender: SocketAddr,
        contract: ContractInstanceId,
    ) -> RateLimitDecision {
        let now = self.time_source.now();
        let key = (sender, contract);

        use dashmap::mapref::entry::Entry;
        // Whether this call has already spent one of `sender`'s new-pair
        // tokens. A retry after eviction re-enters the Vacant branch, and
        // one admission must cost one token, not one per attempt.
        let mut budget_spent = false;
        // Whether this call is holding a slot it freed by evicting (see
        // `EvictionOutcome::Reserved`). It is counted in `size`, so every
        // path out of the loop must either consume it with an insert or
        // hand it back.
        let mut reserved = false;
        // Each iteration either decides, or frees capacity and retries.
        // See `MAX_ADMISSION_ATTEMPTS` for why this is bounded.
        for attempt in 0..MAX_ADMISSION_ATTEMPTS {
            match self.last_accepted.entry(key) {
                Entry::Occupied(mut entry) => {
                    if reserved {
                        // The pair turned up while we were evicting, so
                        // the slot we kept is not needed. Hand it back
                        // now: both exits below return, so this is the
                        // only chance to.
                        self.size.fetch_sub(1, Ordering::Relaxed);
                    }
                    // Existing pair: atomic compare-and-stamp under
                    // shard guard.
                    let last = *entry.get();
                    let elapsed = now.saturating_duration_since(last);
                    if elapsed < self.min_interval {
                        self.rejected_total.fetch_add(1, Ordering::Relaxed);
                        return RateLimitDecision::Rejected {
                            elapsed,
                            min_interval: self.min_interval,
                        };
                    }
                    *entry.get_mut() = now;
                    self.accepted_total.fetch_add(1, Ordering::Relaxed);
                    return RateLimitDecision::Allowed;
                }
                Entry::Vacant(entry) => {
                    // Already holding a slot we freed by evicting: skip
                    // the cap check and spend it.
                    if !reserved {
                        // Brand-new pair. Charge the sender's new-pair
                        // budget BEFORE reserving a slot or evicting
                        // anything, so a sender churning fresh contract
                        // ids cannot push other peers' entries out of the
                        // map on its way to being throttled. See the
                        // module docs.
                        if !budget_spent {
                            match self.new_pair_budget.check_and_record_detailed(sender) {
                                BucketOutcome::Allowed => budget_spent = true,
                                BucketOutcome::RateLimited => {
                                    drop(entry);
                                    self.new_pair_budget_rejected_total
                                        .fetch_add(1, Ordering::Relaxed);
                                    self.log_new_pair_budget(now, sender);
                                    return RateLimitDecision::SenderNewPairBudget;
                                }
                                // The budget map is full, so this sender
                                // has no bucket and nothing is known
                                // about its rate. Fail OPEN: refusing
                                // here would starve a newcomer for a
                                // sizing accident, and a bounded map
                                // that permanently refuses newcomers
                                // once full is precisely the #4981 shape
                                // this PR removes — `Bucket::refill`
                                // restamps on every check, so an active
                                // sender's entry never ages out and the
                                // newcomer would never recover.
                                BucketOutcome::Untracked => {
                                    self.new_pair_budget_untracked_total
                                        .fetch_add(1, Ordering::Relaxed);
                                    budget_spent = true;
                                }
                            }
                        }
                        // Reserve a slot via the authoritative counter
                        // BEFORE inserting. `fetch_add` is the
                        // serialization point — concurrent new-key
                        // inserts strictly serialize, no overshoot beyond
                        // the cap.
                        let prev = self.size.fetch_add(1, Ordering::Relaxed);
                        if prev >= self.max_tracked_pairs {
                            // Cap reached. Roll back and release the
                            // shard guard BEFORE evicting: `evict_oldest`
                            // walks every shard, and holding a guard
                            // across that deadlocks (the same hazard
                            // documented on `size`).
                            self.size.fetch_sub(1, Ordering::Relaxed);
                            drop(entry);
                            // On the final attempt there is no retry left
                            // to spend a freed slot on, so evicting now
                            // would throw away an O(map) scan and shrink
                            // the map for no admission.
                            if attempt + 1 == MAX_ADMISSION_ATTEMPTS {
                                break;
                            }
                            match self.evict_oldest(now) {
                                // We freed the room and kept a slot: the
                                // next iteration inserts into it without
                                // re-running the cap check.
                                EvictionOutcome::Reserved => {
                                    reserved = true;
                                    continue;
                                }
                                // Someone else freed room, or may have.
                                // Spend a retry attempt rather than
                                // dropping.
                                EvictionOutcome::Retry => continue,
                                // Nothing to evict and nothing ever will
                                // be. Configuration, not saturation.
                                EvictionOutcome::CapIsZero => {
                                    self.capacity_rejected_total.fetch_add(1, Ordering::Relaxed);
                                    return RateLimitDecision::CapacityExceeded;
                                }
                            }
                        }
                    }
                    entry.insert(now);
                    self.accepted_total.fetch_add(1, Ordering::Relaxed);
                    return RateLimitDecision::Allowed;
                }
            }
        }

        if reserved {
            // Unreachable by construction: a slot is only kept on a
            // non-final attempt, and the attempt after it either inserts
            // into it or hands it back. Returning it anyway means a
            // future restructuring cannot leak one — a leaked
            // reservation is never reclaimed and permanently shrinks the
            // effective cap.
            self.size.fetch_sub(1, Ordering::Relaxed);
        }
        // Every attempt lost its freed slot to a concurrent caller.
        self.capacity_rejected_total.fetch_add(1, Ordering::Relaxed);
        RateLimitDecision::CapacityExceeded
    }

    /// Evict the oldest tracked pairs to make room for a new one.
    ///
    /// Must be called with no shard guard held — it walks every shard.
    ///
    /// Removes up to `max_tracked_pairs / EVICTION_BATCH_DIVISOR`
    /// entries (at least one) in a single pass, oldest first, so the
    /// O(map) scan is amortised across a batch of admissions rather than
    /// paid per admission. `select_nth_unstable_by_key` partitions
    /// rather than sorts, so the pass is linear.
    ///
    /// Removing exactly `batch` keys — rather than retaining everything
    /// newer than a cutoff timestamp — matters because timestamps tie:
    /// entries stamped within the same clock tick (and, in tests, from a
    /// mock clock that has not advanced) share an `Instant`, and a
    /// cutoff comparison would take every tied entry with it.
    fn evict_oldest(&self, now: Instant) -> EvictionOutcome {
        // The one terminal condition, read from the configuration rather
        // than inferred from an empty map — see `EvictionOutcome`.
        if self.max_tracked_pairs == 0 {
            return EvictionOutcome::CapIsZero;
        }
        // Another caller already made room, so there is nothing to evict
        // and the admission should simply retry. Without this early
        // return every concurrent newcomer pays the full
        // O(max_tracked_pairs) scan even though the cap no longer binds.
        // The authoritative cap check is still the `fetch_add`
        // reservation in `check_and_record`, so a stale read here can
        // never let the map exceed the cap; the worst case is one wasted
        // retry.
        if self.size.load(Ordering::Relaxed) < self.max_tracked_pairs {
            return EvictionOutcome::Retry;
        }

        // One evictor at a time. Concurrent evictors partition the SAME
        // map, so they select nearly the SAME victims and then race to
        // remove entries each other has already taken: measured at the
        // production cap under 16-thread contention, 44% of passes
        // removed nothing at all and the average pass removed 24 entries
        // for a full 16 384-entry scan — against a 256-entry batch. That
        // defeats the amortisation the batch exists for AND starves the
        // callers whose victims were stolen, which is what the retry
        // budget was being spent on. Serialising costs a waiter nothing
        // it was not already paying (it was running its own redundant
        // scan), and the re-check below means a waiter that arrives
        // after the batch is freed does no scan at all.
        let evicting = match self.eviction_lock.lock() {
            Ok(guard) => guard,
            // Poisoned only if a previous holder panicked mid-scan. The
            // map is still consistent — `remove` is atomic per entry —
            // so recover rather than propagate a panic onto the UPDATE
            // receive path.
            Err(poisoned) => poisoned.into_inner(),
        };
        // The holder we queued behind may have freed the room we needed.
        if self.size.load(Ordering::Relaxed) < self.max_tracked_pairs {
            return EvictionOutcome::Retry;
        }

        let batch = (self.max_tracked_pairs / EVICTION_BATCH_DIVISOR).max(1);
        let mut entries: Vec<(Instant, (SocketAddr, ContractInstanceId))> = self
            .last_accepted
            .iter()
            .map(|e| (*e.value(), *e.key()))
            .collect();
        if entries.is_empty() {
            // Not terminal: `size` reads at the cap while the map does
            // not, so slots are about to exist. See `EvictionOutcome`.
            return EvictionOutcome::Retry;
        }
        let batch = batch.min(entries.len());
        entries.select_nth_unstable_by_key(batch - 1, |(stamped, _)| *stamped);

        let mut removed = 0usize;
        for (_, victim) in &entries[..batch] {
            // A concurrent caller may have removed it already; only
            // count what this call actually took out, so `size` stays
            // in step with the map.
            if self.last_accepted.remove(victim).is_some() {
                removed += 1;
            }
        }
        if removed > 0 {
            // Return `removed - 1` slots and KEEP one for the caller.
            //
            // Without this the caller has to win the cap check again
            // against everyone else, and whether it does depends on the
            // batch size relative to the number of concurrent callers:
            // at the production cap (batch 256, 16 threads) it nearly
            // always wins, but at a 1024 cap the batch is 16 and callers
            // lose a freed slot often enough to drop ~0.19 admissions in
            // 1000. Keeping a slot makes "a caller that frees room gets
            // to use it" true by construction instead of by margin.
            //
            // That residual is small because the serialised scan already
            // removed most of it; this closes the rest and, more to the
            // point, makes the property independent of the batch/caller
            // ratio rather than true only at the production cap.
            //
            // The strict cap is unaffected: `size` counts map entries
            // plus outstanding reservations, and every insert still
            // passes the `fetch_add` gate. Batching is unaffected too —
            // the batch still leaves `removed - 1` slots of headroom for
            // the admissions that follow, which is what amortises the
            // scan.
            self.size.fetch_sub(removed - 1, Ordering::Relaxed);
            self.capacity_evicted_total
                .fetch_add(removed as u64, Ordering::Relaxed);
            // Log outside the lock: formatting and the tracing subscriber
            // are not work the next evictor should queue behind.
            drop(evicting);
            self.log_eviction(now, removed);
            return EvictionOutcome::Reserved;
        }
        // Every victim this pass selected was taken by a concurrent
        // caller — slots were freed, just not by us — which is a reason
        // to retry, not to drop.
        EvictionOutcome::Retry
    }

    /// Emit the saturation log line, at most once per
    /// [`EVICTION_LOG_INTERVAL`].
    ///
    /// `info!` rather than `debug!` on purpose: `debug!` is compiled out
    /// of release builds by `release_max_level_info`, which is why the
    /// old drop path left no greppable evidence on a production node
    /// (#4981).
    fn log_eviction(&self, now: Instant, removed: usize) {
        if !log_due(&self.last_eviction_log, now, EVICTION_LOG_INTERVAL) {
            return;
        }
        tracing::info!(
            evicted = removed,
            evicted_total = self.capacity_evicted_total.load(Ordering::Relaxed),
            tracked = self.size.load(Ordering::Relaxed),
            max_tracked_pairs = self.max_tracked_pairs,
            "UPDATE rate limiter at capacity: evicted least-recently-used \
             (sender, contract) pairs to admit new ones. Expected on a node \
             relaying for many peers and contracts; an evicted pair's next \
             UPDATE is treated as new. Throttled to one line per minute."
        );
    }

    /// Emit the fresh-id-churn log line, at most once per
    /// [`NEW_PAIR_LOG_INTERVAL`].
    ///
    /// Throttled for the same reason the drop is worth logging at all: a
    /// sender over its budget is over it on every message, so an
    /// unthrottled line here would be a steady stream on exactly the
    /// node under load. `info!` for the #4981 reason — `debug!` is
    /// compiled out of release builds.
    fn log_new_pair_budget(&self, now: Instant, sender: SocketAddr) {
        if !log_due(&self.last_new_pair_log, now, NEW_PAIR_LOG_INTERVAL) {
            return;
        }

        tracing::info!(
            %sender,
            dropped_total = self.new_pair_budget_rejected_total.load(Ordering::Relaxed),
            burst = NEW_PAIR_BURST,
            refill_interval_ms = NEW_PAIR_REFILL_INTERVAL.as_millis() as u64,
            "UPDATE rate limiter: peer is presenting (sender, contract) pairs this \
             node is not tracking faster than its budget allows, so those UPDATEs \
             are being dropped. Its traffic for tracked pairs is unaffected. If this \
             peer is not churning contract ids, the budget is set too low for this \
             node's working set. Throttled to one line per minute."
        );
    }

    /// Drop entries whose timestamp is older than [`CLEANUP_AGE`].
    /// Call periodically from a reaper loop to bound memory.
    ///
    /// Decrements the [`Self::size`] counter as it goes, so the strict
    /// capacity enforcement stays accurate as idle pairs roll off.
    ///
    /// Per removal rather than once at the end: batching the decrement
    /// leaves `size` reading full for the whole sweep, so a concurrent
    /// admission sees a full map that is in fact being emptied, and
    /// spends attempts evicting from it (#4997 review).
    pub fn cleanup(&self) {
        let now = self.time_source.now();
        let cutoff = match now.checked_sub(CLEANUP_AGE) {
            Some(t) => t,
            None => return, // clock not advanced enough to bother
        };
        self.last_accepted.retain(|_, last| {
            let keep = *last >= cutoff;
            if !keep {
                self.size.fetch_sub(1, Ordering::Relaxed);
            }
            keep
        });
        // Same cadence for the per-sender new-pair budget: reclaims the
        // entries of peers that have gone quiet (and whose buckets have
        // fully refilled, so dropping one cannot change a decision).
        self.new_pair_budget.cleanup();
    }

    /// Total accepted UPDATEs since creation. Surfaced on the node
    /// status dashboard via `RingStatsSnapshot` ("UPDATEs relayed").
    pub fn accepted_total(&self) -> u64 {
        self.accepted_total.load(Ordering::Relaxed)
    }

    /// Total rejected UPDATEs since creation. Surfaced on the node
    /// status dashboard via `RingStatsSnapshot` ("Rate-limited") — a
    /// rising value is the operator's signal that the per-(sender,
    /// contract) limiter may be dropping legitimate relayed traffic.
    pub fn rejected_total(&self) -> u64 {
        self.rejected_total.load(Ordering::Relaxed)
    }

    /// Total UPDATEs dropped because the tracking map was at capacity
    /// and eviction could not free a slot for a new `(sender, contract)`
    /// pair. Surfaced separately from `rejected_total` on the dashboard
    /// ("Capacity-dropped").
    ///
    /// Since #4981 a full map is no longer sufficient to land here — the
    /// oldest entries are evicted instead — so a rising value means the
    /// map is full *and* contended: every admission attempt lost its
    /// freed slot to a concurrent caller. Saturation itself now reads on
    /// [`Self::capacity_evicted_total`]. It is emphatically NOT an
    /// attacker signal: an ordinary node relaying for enough peers and
    /// contracts saturates this map on its own.
    pub fn capacity_rejected_total(&self) -> u64 {
        self.capacity_rejected_total.load(Ordering::Relaxed)
    }

    /// Total entries evicted to admit new pairs at capacity. Surfaced on
    /// the dashboard ("Capacity-evicted") as the saturation signal: a
    /// rising value means the node is relaying for more `(sender,
    /// contract)` pairs than [`MAX_TRACKED_PAIRS`], which is expected on
    /// a busy node and no longer costs those pairs their UPDATEs.
    pub fn capacity_evicted_total(&self) -> u64 {
        self.capacity_evicted_total.load(Ordering::Relaxed)
    }

    /// Total UPDATEs dropped because the sending peer was over its
    /// budget for introducing brand-new `(sender, contract)` pairs.
    /// Surfaced on the dashboard ("Fresh-id-dropped").
    ///
    /// This is the fresh-id-churn signal, and it is the one counter here
    /// that genuinely does suggest a peer is churning identities — the
    /// role `capacity_rejected_total` used to be given before eviction
    /// made saturation an ordinary condition. It never counts a peer's
    /// traffic for contracts already being tracked.
    pub fn new_pair_budget_rejected_total(&self) -> u64 {
        self.new_pair_budget_rejected_total.load(Ordering::Relaxed)
    }

    /// Total new pairs admitted WITHOUT a budget check because the
    /// sender map was full. Surfaced on the dashboard
    /// ("Fresh-id-unmetered") because a safety valve nobody can see
    /// firing is the #4981 failure mode: this should be zero, and a
    /// non-zero value means `max_tracked_senders` is undersized for this
    /// node, so the budget is not actually bounding those senders.
    pub fn new_pair_budget_untracked_total(&self) -> u64 {
        self.new_pair_budget_untracked_total.load(Ordering::Relaxed)
    }

    /// Number of senders tracked by the new-pair budget.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn tracked_senders(&self) -> usize {
        self.new_pair_budget.len()
    }

    /// Number of tracked `(sender, contract)` pairs. Used by tests and
    /// the dashboard for size visibility.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn len(&self) -> usize {
        self.last_accepted.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::time_source::SharedMockTimeSource;

    fn mk_sender(byte: u8) -> SocketAddr {
        SocketAddr::from(([10, 0, 0, byte], 30000 + byte as u16))
    }

    fn mk_contract(byte: u8) -> ContractInstanceId {
        ContractInstanceId::new([byte; 32])
    }

    fn mk_limiter() -> (UpdateRateLimiter, SharedMockTimeSource) {
        let ts = SharedMockTimeSource::new();
        let limiter = UpdateRateLimiter::new(Arc::new(ts.clone()), Ring::DEFAULT_MAX_CONNECTIONS);
        (limiter, ts)
    }

    // Local alias so test bodies can keep using `ts.advance(d)`
    trait Advance {
        fn advance(&self, d: Duration);
    }
    impl Advance for SharedMockTimeSource {
        fn advance(&self, d: Duration) {
            self.advance_time(d);
        }
    }

    #[test]
    fn first_update_for_pair_is_allowed() {
        let (l, _ts) = mk_limiter();
        let d = l.check_and_record(mk_sender(1), mk_contract(1));
        assert_eq!(d, RateLimitDecision::Allowed);
        assert_eq!(l.accepted_total(), 1);
        assert_eq!(l.rejected_total(), 0);
    }

    #[test]
    fn second_update_within_min_interval_is_rejected() {
        let (l, ts) = mk_limiter();
        assert!(
            l.check_and_record(mk_sender(1), mk_contract(1))
                .is_allowed()
        );
        // Only 10ms later — well under 100ms default.
        ts.advance(Duration::from_millis(10));
        let d = l.check_and_record(mk_sender(1), mk_contract(1));
        assert!(
            matches!(d, RateLimitDecision::Rejected { .. }),
            "second UPDATE 10ms after first must be rejected, got {d:?}"
        );
        assert_eq!(l.accepted_total(), 1);
        assert_eq!(l.rejected_total(), 1);
    }

    #[test]
    fn update_after_min_interval_is_allowed() {
        let (l, ts) = mk_limiter();
        assert!(
            l.check_and_record(mk_sender(1), mk_contract(1))
                .is_allowed()
        );
        // 200ms later — past the 100ms default.
        ts.advance(Duration::from_millis(200));
        let d = l.check_and_record(mk_sender(1), mk_contract(1));
        assert_eq!(d, RateLimitDecision::Allowed);
        assert_eq!(l.accepted_total(), 2);
        assert_eq!(l.rejected_total(), 0);
    }

    #[test]
    fn different_senders_same_contract_independent() {
        let (l, ts) = mk_limiter();
        // Sender 1 accepts.
        assert!(
            l.check_and_record(mk_sender(1), mk_contract(1))
                .is_allowed()
        );
        // Sender 2 immediately also accepts — different key.
        ts.advance(Duration::from_millis(1));
        assert!(
            l.check_and_record(mk_sender(2), mk_contract(1))
                .is_allowed()
        );
        // Sender 1 retry 1ms later still rejected.
        let d = l.check_and_record(mk_sender(1), mk_contract(1));
        assert!(matches!(d, RateLimitDecision::Rejected { .. }));
    }

    #[test]
    fn same_sender_different_contracts_independent() {
        let (l, _ts) = mk_limiter();
        assert!(
            l.check_and_record(mk_sender(1), mk_contract(1))
                .is_allowed()
        );
        // Same sender, different contract — independent rate limit.
        assert!(
            l.check_and_record(mk_sender(1), mk_contract(2))
                .is_allowed()
        );
        assert_eq!(l.accepted_total(), 2);
    }

    #[test]
    fn rejected_attempts_do_not_extend_window() {
        // If a flooding peer keeps trying every 10ms, we want the
        // first attempt past the 100ms window to succeed — i.e. the
        // rejected attempts MUST NOT push the last_accepted timestamp
        // forward. Otherwise a sustained flood would lock the pair out
        // indefinitely (the existing peer would never recover).
        let (l, ts) = mk_limiter();
        assert!(
            l.check_and_record(mk_sender(1), mk_contract(1))
                .is_allowed()
        );
        // 9 sub-100ms attempts, all rejected.
        for _ in 0..9 {
            ts.advance(Duration::from_millis(10));
            assert!(
                !l.check_and_record(mk_sender(1), mk_contract(1))
                    .is_allowed()
            );
        }
        // Now we're at 90ms — still rejected.
        ts.advance(Duration::from_millis(5));
        assert!(
            !l.check_and_record(mk_sender(1), mk_contract(1))
                .is_allowed()
        );
        // One more advance puts us past 100ms from the FIRST accept.
        ts.advance(Duration::from_millis(10));
        assert!(
            l.check_and_record(mk_sender(1), mk_contract(1))
                .is_allowed(),
            "after 105ms+ from original accept, next attempt MUST be allowed — \
             rejected attempts must not have moved the window forward"
        );
    }

    #[test]
    fn cleanup_removes_stale_entries() {
        let (l, ts) = mk_limiter();
        l.check_and_record(mk_sender(1), mk_contract(1));
        l.check_and_record(mk_sender(2), mk_contract(2));
        assert_eq!(l.len(), 2);

        // Advance past CLEANUP_AGE.
        ts.advance(CLEANUP_AGE + Duration::from_secs(1));
        l.cleanup();
        assert_eq!(l.len(), 0, "all stale entries must be cleared");
    }

    #[test]
    fn cleanup_preserves_fresh_entries() {
        let (l, ts) = mk_limiter();
        l.check_and_record(mk_sender(1), mk_contract(1));
        // Advance partway through the cleanup age.
        ts.advance(CLEANUP_AGE / 2);
        l.cleanup();
        assert_eq!(l.len(), 1, "fresh entry must be preserved");
    }

    #[test]
    fn counters_track_accepts_and_rejects() {
        let (l, ts) = mk_limiter();
        for i in 0..5 {
            // Five accepts: stagger by min_interval.
            ts.advance(MIN_UPDATE_INTERVAL + Duration::from_millis(1));
            assert!(
                l.check_and_record(mk_sender(1), mk_contract(1))
                    .is_allowed(),
                "iter {i}"
            );
        }
        // Three rejects: hammer with no advance.
        for _ in 0..3 {
            assert!(
                !l.check_and_record(mk_sender(1), mk_contract(1))
                    .is_allowed()
            );
        }
        assert_eq!(l.accepted_total(), 5);
        assert_eq!(l.rejected_total(), 3);
    }

    /// Pin test exhibiting the May 21 incident pattern: a single sender
    /// hammering a single contract at ~10 UPDATEs/s. With the limiter,
    /// at most ~10/s are accepted (one per MIN_UPDATE_INTERVAL window);
    /// the rest are dropped. The "real" 4PjqN5… incident was producing
    /// far more than this, so the rejection rate is overwhelming.
    #[test]
    fn may21_flood_pattern_is_throttled() {
        let (l, ts) = mk_limiter();
        let sender = mk_sender(1);
        let contract = mk_contract(1);

        // Simulate 1 second of flooding at 1ms per attempt (1000
        // attempts/s — 100× over the 10/s ceiling).
        for _ in 0..1000 {
            l.check_and_record(sender, contract);
            ts.advance(Duration::from_millis(1));
        }
        // Expected admits: floor(1000ms / 100ms) + 1 (first one is
        // unconditional) = ~11 admissions, ~989 rejections.
        let accepted = l.accepted_total();
        let rejected = l.rejected_total();
        assert!(
            (9..=12).contains(&accepted),
            "expected ~10 admits over 1s of flooding, got {accepted}"
        );
        assert_eq!(accepted + rejected, 1000);
        // The reject rate must be high — the flood is mostly dropped.
        assert!(
            rejected as f64 / 1000.0 > 0.95,
            "expected >95% rejection rate, got {}",
            rejected as f64 / 1000.0
        );
    }

    /// Pin: the cap still bounds the map, but reaching it admits the
    /// newcomer by evicting rather than refusing it (#4981).
    ///
    /// This replaces the assertion the previous version of this test
    /// made — that a new pair past the cap gets `CapacityExceeded` —
    /// which pinned the starvation bug as if it were the contract. What
    /// it checked that still holds is kept: the map never exceeds the
    /// cap, and existing pairs keep working. See
    /// `at_capacity_evicts_the_oldest_pairs_not_arbitrary_ones` for the
    /// eviction order and
    /// `busy_pairs_cannot_hold_slots_against_newcomers` for the
    /// starvation regression itself.
    #[test]
    fn at_capacity_new_pair_is_admitted_and_the_map_stays_bounded() {
        // Small cap so the test is fast. At cap 8 the eviction batch is
        // `max(8 / 64, 1)` = exactly one entry.
        let ts = SharedMockTimeSource::new();
        let limiter = UpdateRateLimiter::with_config(
            Arc::new(ts.clone()),
            MIN_UPDATE_INTERVAL,
            8, // tiny cap for test speed
        );

        // Fill the map with 8 distinct pairs — all should be Allowed.
        for i in 0..8 {
            let d = limiter.check_and_record(mk_sender(i + 1), mk_contract(i + 1));
            assert_eq!(d, RateLimitDecision::Allowed, "pair {i} should be allowed");
            ts.advance(Duration::from_millis(1));
        }
        assert_eq!(limiter.len(), 8);

        // Pair #9 is a new key at capacity: admitted, by evicting.
        let d = limiter.check_and_record(mk_sender(99), mk_contract(99));
        assert_eq!(
            d,
            RateLimitDecision::Allowed,
            "a new pair at capacity must be admitted, not starved (#4981)"
        );
        assert_eq!(
            limiter.capacity_rejected_total(),
            0,
            "admission by eviction must not count as a capacity rejection"
        );
        assert_eq!(limiter.capacity_evicted_total(), 1);
        assert_eq!(
            limiter.len(),
            8,
            "the cap is still a hard bound: one in, one out"
        );

        // An already-tracked pair keeps working after min_interval.
        ts.advance(MIN_UPDATE_INTERVAL + Duration::from_millis(1));
        let d = limiter.check_and_record(mk_sender(8), mk_contract(8));
        assert_eq!(
            d,
            RateLimitDecision::Allowed,
            "existing pair must keep working at cap"
        );
    }

    /// The compensating control for what eviction gave up: a sender
    /// churning brand-new contract ids is cut off once it has spent its
    /// burst, instead of getting one UPDATE through per id forever.
    ///
    /// Before eviction, a full map refused every fresh pair outright,
    /// which stopped this pattern as a side effect. Nothing else in the
    /// node does (the ban list is keyed by contract, the MAD detector is
    /// off by default, and the transport rate-limits handshakes rather
    /// than established traffic), so the ceiling is replaced here rather
    /// than dropped (#4997 review).
    #[test]
    fn a_sender_churning_fresh_contract_ids_is_cut_off_after_its_burst() {
        const BURST: usize = 8;
        let ts = SharedMockTimeSource::new();
        let limiter = UpdateRateLimiter::with_new_pair_budget(
            Arc::new(ts.clone()),
            MIN_UPDATE_INTERVAL,
            MAX_TRACKED_PAIRS,
            BURST as f64,
            Ring::DEFAULT_MAX_CONNECTIONS * SENDER_TRACKING_HEADROOM,
        );
        let attacker = mk_sender(1);

        // The burst gets through: these are the pairs a legitimate peer
        // would be introducing when it starts relaying for a batch of
        // contracts.
        for i in 0..BURST {
            assert_eq!(
                limiter.check_and_record(attacker, mk_contract(i as u8)),
                RateLimitDecision::Allowed,
                "fresh id {i} is within the burst"
            );
        }

        // Past it, fresh ids are refused — and refused for a reason that
        // says so, not as a capacity drop.
        for i in BURST..(BURST + 32) {
            assert_eq!(
                limiter.check_and_record(attacker, mk_contract(i as u8)),
                RateLimitDecision::SenderNewPairBudget,
                "fresh id {i} is past the burst and must be refused"
            );
        }
        assert_eq!(limiter.new_pair_budget_rejected_total(), 32);
        assert_eq!(
            limiter.len(),
            BURST,
            "a throttled sender must not have grown the tracking map"
        );
        assert_eq!(
            limiter.capacity_evicted_total(),
            0,
            "a throttled sender must not have evicted anything — the budget is \
             charged BEFORE the capacity path so churn cannot push other peers out"
        );

        // The budget refills, so this throttles rather than bans: after
        // one interval the sender may introduce one more pair.
        ts.advance(NEW_PAIR_REFILL_INTERVAL);
        assert_eq!(
            limiter.check_and_record(attacker, mk_contract(200)),
            RateLimitDecision::Allowed,
            "one refill interval must buy exactly one more fresh pair"
        );
        assert_eq!(
            limiter.check_and_record(attacker, mk_contract(201)),
            RateLimitDecision::SenderNewPairBudget,
            "and only one"
        );
    }

    /// The budget must never touch a sender's ESTABLISHED traffic, which
    /// is the availability the #4981 fix exists to restore. A peer that
    /// keeps updating contracts the limiter already tracks is bounded by
    /// `min_interval` per pair and by nothing else, however long it goes
    /// on.
    #[test]
    fn the_new_pair_budget_never_throttles_established_pairs() {
        const BURST: usize = 4;
        let ts = SharedMockTimeSource::new();
        let limiter = UpdateRateLimiter::with_new_pair_budget(
            Arc::new(ts.clone()),
            MIN_UPDATE_INTERVAL,
            MAX_TRACKED_PAIRS,
            BURST as f64,
            Ring::DEFAULT_MAX_CONNECTIONS * SENDER_TRACKING_HEADROOM,
        );
        let peer = mk_sender(1);

        for i in 0..BURST {
            assert_eq!(
                limiter.check_and_record(peer, mk_contract(i as u8)),
                RateLimitDecision::Allowed
            );
        }
        // Budget fully spent: a fresh pair is refused from here on.
        assert_eq!(
            limiter.check_and_record(peer, mk_contract(99)),
            RateLimitDecision::SenderNewPairBudget
        );

        // 200 rounds over the same established pairs — far more traffic
        // than the burst — and not one of them is budget-throttled.
        for round in 0..200 {
            ts.advance(MIN_UPDATE_INTERVAL + Duration::from_millis(1));
            for i in 0..BURST {
                assert_eq!(
                    limiter.check_and_record(peer, mk_contract(i as u8)),
                    RateLimitDecision::Allowed,
                    "round {round}, established pair {i} must be unaffected by the \
                     new-pair budget"
                );
            }
        }
        assert_eq!(
            limiter.new_pair_budget_rejected_total(),
            1,
            "only the one fresh pair was refused; established traffic never \
             touches the budget"
        );
        assert_eq!(limiter.accepted_total(), (BURST + BURST * 200) as u64);
    }

    /// The saturated case, which is the one that decides whether this
    /// control is safe to ship: on a node whose working set far exceeds
    /// the cap, a pair is evicted before its next UPDATE arrives, so
    /// ordinary relayed traffic keeps re-entering the new-pair path.
    ///
    /// The limiter cannot tell a returning pair from a fresh one, so it
    /// IS charged — which is why the sustained rate is 200/s and not the
    /// per-pair 10/s. This pins the consequence both ways: a peer
    /// running at that sustained rate is never throttled, however long
    /// it goes on, and the bound is nonetheless real.
    ///
    /// Without this fixture the only budget test at a realistic cap was
    /// one that never saturated, which could not have caught a rate
    /// tuned for genuinely-fresh ids throttling relayed traffic (#4997
    /// review).
    #[test]
    fn under_saturation_re_admitted_pairs_are_not_throttled_at_the_sustained_rate() {
        // Half the contracts fit, so by the time one comes round again
        // it has been evicted: every check takes the new-pair path.
        const CAP: usize = 64;
        const CONTRACTS: u8 = 128;
        const CYCLES: usize = 20;

        let ts = SharedMockTimeSource::new();
        let limiter =
            UpdateRateLimiter::with_config(Arc::new(ts.clone()), MIN_UPDATE_INTERVAL, CAP);
        let peer = mk_sender(1);

        for cycle in 0..CYCLES {
            for c in 0..CONTRACTS {
                assert_eq!(
                    limiter.check_and_record(peer, mk_contract(c)),
                    RateLimitDecision::Allowed,
                    "cycle {cycle}, contract {c}: a peer relaying at the sustained rate \
                     must not be throttled, even though eviction keeps making its pairs \
                     look new"
                );
                ts.advance(NEW_PAIR_REFILL_INTERVAL);
            }
        }

        assert!(
            limiter.capacity_evicted_total() > 0,
            "fixture must actually saturate, or it proves nothing about re-admission"
        );
        assert_eq!(
            limiter.new_pair_budget_rejected_total(),
            0,
            "nothing may be dropped at the sustained rate"
        );

        // The bound is still real: with the clock stopped, the same peer
        // spends its burst and is then cut off. It takes more than
        // NEW_PAIR_BURST pairs to get there, which is the point — the
        // burst is what keeps a legitimate reconnect flurry through.
        let attempts = NEW_PAIR_BURST as usize * 2;
        let mut refused = 0;
        for c in 0..attempts {
            let mut id = [0u8; 32];
            id[..8].copy_from_slice(&(c as u64).to_be_bytes());
            id[8] = 0xEE; // disjoint from mk_contract's key space
            if limiter.check_and_record(peer, ContractInstanceId::new(id))
                == RateLimitDecision::SenderNewPairBudget
            {
                refused += 1;
            }
        }
        assert!(
            refused > 0,
            "a peer presenting unfamiliar pairs with no time passing must eventually \
             be refused, or the budget bounds nothing"
        );
        assert!(
            refused < attempts,
            "and the burst must let a substantial run through before that: \
             {refused} of {attempts} refused"
        );
    }

    /// The budget is per sender: one peer churning fresh ids must not
    /// throttle anyone else.
    #[test]
    fn the_new_pair_budget_is_scoped_per_sender() {
        const BURST: usize = 2;
        let ts = SharedMockTimeSource::new();
        let limiter = UpdateRateLimiter::with_new_pair_budget(
            Arc::new(ts.clone()),
            MIN_UPDATE_INTERVAL,
            MAX_TRACKED_PAIRS,
            BURST as f64,
            Ring::DEFAULT_MAX_CONNECTIONS * SENDER_TRACKING_HEADROOM,
        );
        let noisy = mk_sender(1);
        let quiet = mk_sender(2);

        for i in 0..BURST {
            assert_eq!(
                limiter.check_and_record(noisy, mk_contract(i as u8)),
                RateLimitDecision::Allowed
            );
        }
        assert_eq!(
            limiter.check_and_record(noisy, mk_contract(50)),
            RateLimitDecision::SenderNewPairBudget
        );

        for i in 0..BURST {
            assert_eq!(
                limiter.check_and_record(quiet, mk_contract(i as u8)),
                RateLimitDecision::Allowed,
                "a second sender has its own budget, untouched by the first"
            );
        }
        assert_eq!(limiter.tracked_senders(), 2);
    }

    /// The sender map is bounded, and by a space the attacker does not
    /// choose: `sender` is the immediate upstream hop, so only a peer
    /// that has completed a handshake can occupy an entry.
    ///
    /// And — the half that matters — a sender arriving at a FULL map is
    /// still admitted. Bounding alone is not the property: a bounded map
    /// that refuses newcomers once full is #4981 exactly, one map over,
    /// inside the control added to fix #4981. `Bucket::refill` restamps
    /// on every check including denied ones, so an active sender's entry
    /// never ages out and a refused newcomer would never recover.
    ///
    /// Asserting only `tracked_senders() <= MAX` did not catch that: a
    /// fail-CLOSED implementation passed the whole suite 15/15 (#4997
    /// re-review).
    #[test]
    fn the_new_pair_budget_map_is_bounded_and_fails_open_when_full() {
        const MAX_SENDERS: usize = 4;
        let ts = SharedMockTimeSource::new();
        let limiter = UpdateRateLimiter::with_new_pair_budget(
            Arc::new(ts.clone()),
            MIN_UPDATE_INTERVAL,
            MAX_TRACKED_PAIRS,
            NEW_PAIR_BURST,
            MAX_SENDERS,
        );

        for i in 0..(MAX_SENDERS * 4) {
            let d = limiter.check_and_record(mk_sender(i as u8), mk_contract(1));
            assert_eq!(
                d,
                RateLimitDecision::Allowed,
                "sender {i}: a full sender map must fail OPEN — refusing here is \
                 #4981 one map over, and this sender has spent nothing"
            );
            assert!(
                limiter.tracked_senders() <= MAX_SENDERS,
                "sender {i}: the budget map must never exceed its cap"
            );
        }
        assert_eq!(limiter.tracked_senders(), MAX_SENDERS);

        // The untracked admissions are counted, so an undersized map is
        // an operator-visible condition rather than a silent one.
        assert_eq!(
            limiter.new_pair_budget_untracked_total(),
            (MAX_SENDERS * 3) as u64,
            "every admission past the cap must be counted as untracked"
        );
        assert_eq!(
            limiter.new_pair_budget_rejected_total(),
            0,
            "a full map is a sizing accident, not evidence about any sender"
        );
    }

    /// Source-scrape pin: `evict_oldest` has exactly ONE terminal exit,
    /// and it is the zero-cap guard. Every other way out must be a
    /// retry.
    ///
    /// This exists because no behavioural test can reach the
    /// `removed == 0` exit any more. Once the scan is serialised,
    /// concurrent evictors stop stealing each other's victims, so
    /// turning that exit terminal survives the entire suite (verified:
    /// 29/29, and 15/15 on the concurrency fixture that looks like it
    /// would catch it).
    ///
    /// The exit must nonetheless stay a retry, for a cause no evictor
    /// creates: **`cleanup` does not take the eviction lock**, and it
    /// removes entries older than `CLEANUP_AGE` — precisely the oldest
    /// entries, which is precisely what `select_nth_unstable_by_key`
    /// selects. A reaper sweep landing between the collect and the
    /// removes takes the whole batch out from under the evictor, and
    /// that is a once-a-minute window on every node. Making it terminal
    /// would drop a legitimate UPDATE there, silently, with the retry
    /// budget untouched — the #4981 shape again.
    ///
    /// A source pin rather than a behavioural one, because the window is
    /// a race between a reaper tick and a receive-path scan that no
    /// fixture can schedule. Pinning the SHAPE (one terminal exit,
    /// guarded by the cap) is what is actually available.
    #[test]
    fn evict_oldest_has_exactly_one_terminal_exit() {
        const THIS_FILE: &str = include_str!("update_rate_limit.rs");

        let start = THIS_FILE
            .find(concat!("fn evict_", "oldest("))
            .expect("evict_oldest must exist; if renamed, update this pin");
        let tail = &THIS_FILE[start..];
        // Bound to this function: the next item at impl indentation.
        let end = tail[1..]
            .find("\n    fn ")
            .map(|i| i + 1)
            .unwrap_or(tail.len());
        let body = &tail[..end];

        let terminal = concat!("EvictionOutcome::", "CapIsZero");
        assert_eq!(
            body.matches(terminal).count(),
            1,
            "evict_oldest must have exactly ONE terminal exit. Every other way out \
             means slots are, or are about to be, free — including an empty map, \
             which `cleanup` can produce mid-scan — and dropping there loses a \
             legitimate UPDATE without spending the retry budget (#4981, #4997)."
        );

        let guard_pos = body
            .find("self.max_tracked_pairs == 0")
            .expect("the terminal exit must be guarded by the cap being zero");
        let terminal_pos = body.find(terminal).expect("checked above");
        assert!(
            guard_pos < terminal_pos,
            "the one terminal exit must be the zero-cap guard, not a condition \
             inferred from the map's contents"
        );
    }

    /// An empty map is NOT a reason to give up, even though the map
    /// being empty looks like there is nothing to evict.
    ///
    /// `size` reads at the cap while the map does not in two real
    /// windows — while `cleanup` is sweeping, and while every caller
    /// that won a slot is still between its reservation and its insert
    /// (reachable at small caps under contention: 8 slots, 64 callers).
    /// Inferring "the cap must be 0" from an empty map dropped the
    /// UPDATE in both, without spending a single retry attempt — the
    /// same mistake as reading a zero-removal eviction as terminal, one
    /// branch over (#4997 review).
    ///
    /// White-box because the windows are races: the condition is forced
    /// directly rather than raced for, so the test is deterministic.
    #[test]
    fn an_empty_map_at_a_nonzero_cap_retries_rather_than_dropping() {
        let ts = SharedMockTimeSource::new();
        let limiter = UpdateRateLimiter::with_config(Arc::new(ts.clone()), MIN_UPDATE_INTERVAL, 8);

        // The map is empty and `size` says otherwise, exactly as during
        // a `cleanup` sweep.
        limiter.size.store(8, Ordering::Relaxed);
        assert_eq!(limiter.len(), 0);
        assert!(
            matches!(limiter.evict_oldest(ts.now()), EvictionOutcome::Retry),
            "an empty map at a non-zero cap means slots are about to exist, not that \
             the cap is zero"
        );

        // And with a zero cap it IS terminal, whatever the map says.
        let zero = UpdateRateLimiter::with_config(Arc::new(ts.clone()), MIN_UPDATE_INTERVAL, 0);
        assert!(matches!(
            zero.evict_oldest(ts.now()),
            EvictionOutcome::CapIsZero
        ));
    }

    /// After a sweep, `size` equals the map length — the accounting
    /// invariant the cap enforcement rests on, across a sweep that both
    /// keeps and drops entries.
    ///
    /// Deliberately NOT claiming to pin the mid-sweep window: `cleanup`
    /// decrements per removal rather than settling up at the end, so a
    /// concurrent admission never sees a full `size` over an emptying
    /// map, but that window is not observable from outside and this test
    /// passes either way. What covers the consequence is
    /// `an_empty_map_at_a_nonzero_cap_retries_rather_than_dropping`,
    /// which forces the condition directly.
    #[test]
    fn cleanup_leaves_the_size_counter_equal_to_the_map_length() {
        let ts = SharedMockTimeSource::new();
        let limiter = UpdateRateLimiter::with_config(Arc::new(ts.clone()), MIN_UPDATE_INTERVAL, 8);
        for i in 1..=8u8 {
            assert!(
                limiter
                    .check_and_record(mk_sender(i), mk_contract(i))
                    .is_allowed()
            );
        }
        // Age out half of them, so the sweep both keeps and drops.
        ts.advance(CLEANUP_AGE + Duration::from_secs(1));
        for i in 1..=4u8 {
            assert!(
                limiter
                    .check_and_record(mk_sender(i), mk_contract(i))
                    .is_allowed()
            );
        }
        limiter.cleanup();
        assert_eq!(limiter.len(), 4);
        assert_eq!(
            limiter.size.load(Ordering::Relaxed),
            limiter.len(),
            "size must equal the map length after a sweep"
        );
    }

    /// Boundary case: a zero cap is the ONLY configuration in which the
    /// limiter still refuses a new pair outright, and it must terminate
    /// rather than spin.
    ///
    /// This is the one path `EvictionOutcome::CapIsZero` exists for. It
    /// was untested, which mattered because the eviction rework turned
    /// "evicted nothing" into a retry: had that retry not distinguished
    /// an empty map from a contended one, a zero cap would loop through
    /// its whole attempt budget on every call forever.
    #[test]
    fn a_zero_cap_refuses_every_pair_without_spinning() {
        let ts = SharedMockTimeSource::new();
        let limiter = UpdateRateLimiter::with_config(Arc::new(ts.clone()), MIN_UPDATE_INTERVAL, 0);

        for i in 1..=4u8 {
            assert_eq!(
                limiter.check_and_record(mk_sender(i), mk_contract(i)),
                RateLimitDecision::CapacityExceeded,
                "with a zero cap there is no slot for pair {i} and nothing to evict"
            );
        }
        assert_eq!(limiter.len(), 0);
        assert_eq!(limiter.capacity_rejected_total(), 4);
        assert_eq!(
            limiter.capacity_evicted_total(),
            0,
            "an empty map has nothing to evict"
        );
        assert_eq!(limiter.accepted_total(), 0);
    }

    /// Cap used by the eviction-order tests. 128 gives an eviction batch
    /// of `128 / EVICTION_BATCH_DIVISOR` = 2, and the size is what makes
    /// these tests decisive: an arbitrary-victim implementation has to
    /// pick the *exact* oldest pairs out of 128 candidates, three rounds
    /// running, to survive. At the cap-8 fixtures used elsewhere the
    /// batch clamps to 1 and an arbitrary victim is the right one 1 time
    /// in 8, which is not a pin (measured: an implementation with the
    /// ordering deleted passed 349 of 400 runs, #4997 review).
    const ORDER_TEST_CAP: usize = 128;
    /// Number of entries the eviction-order tests drive out: three
    /// batches of two.
    const ORDER_TEST_EVICTED: usize = 6;

    /// Distinct `(sender, contract)` pairs indexed well past 256, which
    /// is where the `u8`-keyed helpers above stop.
    fn mk_pair(i: usize) -> (SocketAddr, ContractInstanceId) {
        let sender = SocketAddr::from(([10, 1, (i >> 8) as u8, (i & 0xff) as u8], 30000));
        (sender, ContractInstanceId::new([0xAB; 32]))
    }

    /// Admit fresh pairs, starting at index `from`, until the limiter has
    /// evicted `ORDER_TEST_EVICTED` entries. Returns the next unused
    /// index.
    ///
    /// Each newcomer at capacity evicts a batch of 2 and inserts 1, so
    /// the map alternates between cap and cap-1 and only every other
    /// newcomer triggers a batch.
    fn admit_until_evicted(
        limiter: &UpdateRateLimiter,
        ts: &SharedMockTimeSource,
        from: usize,
    ) -> usize {
        let mut i = from;
        while limiter.capacity_evicted_total() < ORDER_TEST_EVICTED as u64 {
            let (s, c) = mk_pair(i);
            assert_eq!(
                limiter.check_and_record(s, c),
                RateLimitDecision::Allowed,
                "newcomer {i} must be admitted"
            );
            i += 1;
            ts.advance(Duration::from_micros(1));
            assert!(
                i < from + ORDER_TEST_CAP,
                "newcomers should have driven {ORDER_TEST_EVICTED} evictions long before this"
            );
        }
        assert_eq!(
            limiter.capacity_evicted_total(),
            ORDER_TEST_EVICTED as u64,
            "batches of 2 must land exactly on {ORDER_TEST_EVICTED}"
        );
        i
    }

    /// The victims are the least recently used pairs, not arbitrary ones.
    ///
    /// Membership is observed through the limiter's own behaviour rather
    /// than a test-only accessor: a tracked pair stamped within
    /// `min_interval` answers `Rejected`, while an evicted pair is seen
    /// as new and answers `Allowed`. So "was it evicted?" is exactly
    /// "does an immediate re-check come back Allowed?".
    ///
    /// That discriminator only holds while EVERY pair's stamp is inside
    /// `min_interval`, which is why the fixture advances by microseconds
    /// and asserts the whole run stays inside the window. The previous
    /// version of this test let the clock run 108ms before probing, so
    /// the pair it checked for eviction was outside the window and
    /// answered `Allowed` whether or not it had been evicted — the
    /// assertion could not fail (#4997 review).
    #[test]
    fn at_capacity_evicts_the_oldest_pairs_not_arbitrary_ones() {
        let ts = SharedMockTimeSource::new();
        let limiter = UpdateRateLimiter::with_config(
            Arc::new(ts.clone()),
            MIN_UPDATE_INTERVAL,
            ORDER_TEST_CAP,
        );
        assert_eq!(
            ORDER_TEST_CAP / EVICTION_BATCH_DIVISOR,
            2,
            "fixture assumes a 2-entry batch"
        );
        let start = ts.now();

        // Fill, one microsecond apart, so ages are strictly ordered:
        // pair 0 is the oldest, pair 127 the newest.
        for i in 0..ORDER_TEST_CAP {
            let (s, c) = mk_pair(i);
            assert_eq!(
                limiter.check_and_record(s, c),
                RateLimitDecision::Allowed,
                "fill {i}"
            );
            ts.advance(Duration::from_micros(1));
        }
        assert_eq!(limiter.len(), ORDER_TEST_CAP);

        admit_until_evicted(&limiter, &ts, ORDER_TEST_CAP);

        assert!(
            ts.now().saturating_duration_since(start) < MIN_UPDATE_INTERVAL,
            "the fixture must stay inside min_interval, or a surviving pair and an \
             evicted one both answer Allowed and the assertions below are vacuous"
        );

        // Survivors first: a `Rejected` check does not mutate the map,
        // whereas an `Allowed` one re-inserts and can evict again.
        for i in ORDER_TEST_EVICTED..ORDER_TEST_CAP {
            let (s, c) = mk_pair(i);
            assert!(
                matches!(
                    limiter.check_and_record(s, c),
                    RateLimitDecision::Rejected { .. }
                ),
                "pair {i} is newer than the {ORDER_TEST_EVICTED} oldest and must have survived"
            );
        }
        for i in 0..ORDER_TEST_EVICTED {
            let (s, c) = mk_pair(i);
            assert_eq!(
                limiter.check_and_record(s, c),
                RateLimitDecision::Allowed,
                "pair {i} is among the {ORDER_TEST_EVICTED} oldest and must have been evicted"
            );
        }
    }

    /// Accepting an UPDATE for a tracked pair restamps it, which moves it
    /// to the BACK of the eviction order.
    ///
    /// This is the property the module's cost argument rests on: a
    /// sustained flood from a *stable* pair is unaffected by eviction,
    /// because that pair is the most recently used and therefore the last
    /// thing evicted. It needs its own fixture because the refresh is
    /// what destroys the membership discriminator — a pair refreshed past
    /// `min_interval` leaves every un-refreshed pair outside the window,
    /// so their membership stops being observable.
    ///
    /// The fixture works around that by refreshing all but a chosen few:
    /// the 6 pairs left un-refreshed are the only ones outside the
    /// window, and they are exactly the ones that must be evicted, so
    /// every pair the test asserts on is inside the window.
    #[test]
    fn a_refreshed_pair_moves_to_the_back_of_the_eviction_order() {
        let ts = SharedMockTimeSource::new();
        let limiter = UpdateRateLimiter::with_config(
            Arc::new(ts.clone()),
            MIN_UPDATE_INTERVAL,
            ORDER_TEST_CAP,
        );

        // Fill oldest-first, as above.
        for i in 0..ORDER_TEST_CAP {
            let (s, c) = mk_pair(i);
            assert_eq!(
                limiter.check_and_record(s, c),
                RateLimitDecision::Allowed,
                "fill {i}"
            );
            ts.advance(Duration::from_micros(1));
        }

        // Past `min_interval`, refresh every pair EXCEPT indices
        // 6..ORDER_TEST_EVICTED+6. Crucially that includes pairs 0..6 —
        // the entries that were the oldest — so if eviction ignored the
        // restamp it would take exactly those, and this test fails.
        ts.advance(MIN_UPDATE_INTERVAL);
        let victims = ORDER_TEST_EVICTED..(2 * ORDER_TEST_EVICTED);
        let refreshed = ts.now();
        for i in (0..ORDER_TEST_CAP).filter(|i| !victims.contains(i)) {
            let (s, c) = mk_pair(i);
            assert_eq!(
                limiter.check_and_record(s, c),
                RateLimitDecision::Allowed,
                "refresh {i} must be accepted a full min_interval after the fill"
            );
        }
        assert_eq!(
            limiter.capacity_evicted_total(),
            0,
            "restamping tracked pairs must not evict anything — it never takes the \
             capacity path"
        );

        admit_until_evicted(&limiter, &ts, ORDER_TEST_CAP);

        assert!(
            ts.now().saturating_duration_since(refreshed) < MIN_UPDATE_INTERVAL,
            "every refreshed pair must still be inside min_interval, or the \
             assertions below are vacuous"
        );

        // Every refreshed pair survived — including 0..6, which were the
        // oldest before the refresh. The only entries gone are the ones
        // left un-refreshed.
        for i in (0..ORDER_TEST_CAP).filter(|i| !victims.contains(i)) {
            let (s, c) = mk_pair(i);
            assert!(
                matches!(
                    limiter.check_and_record(s, c),
                    RateLimitDecision::Rejected { .. }
                ),
                "pair {i} was refreshed, so it must be at the back of the eviction \
                 order and must have survived"
            );
        }
    }

    /// The #4981 regression: a busy pair must not be able to hold its
    /// slot indefinitely against new arrivals.
    ///
    /// Every tracked pair keeps refreshing, which is what made the old
    /// code starve newcomers forever — an accepted UPDATE restamps the
    /// entry, so nothing ever aged out under `CLEANUP_AGE` and the cap
    /// was permanently held by whoever got in first. Under that code
    /// every newcomer here is `CapacityExceeded`; the pass condition is
    /// that all of them get through.
    #[test]
    fn busy_pairs_cannot_hold_slots_against_newcomers() {
        let ts = SharedMockTimeSource::new();
        let limiter = UpdateRateLimiter::with_config(Arc::new(ts.clone()), MIN_UPDATE_INTERVAL, 8);

        let start = ts.now();
        for i in 1..=8u8 {
            assert!(
                limiter
                    .check_and_record(mk_sender(i), mk_contract(i))
                    .is_allowed()
            );
            ts.advance(Duration::from_millis(1));
        }

        // Well under CLEANUP_AGE throughout, so the TTL sweep is not
        // what rescues the newcomers here — eviction is.
        for round in 0..20u8 {
            // The incumbents stay busy, restamping themselves.
            ts.advance(MIN_UPDATE_INTERVAL + Duration::from_millis(1));
            for i in 1..=8u8 {
                limiter.check_and_record(mk_sender(i), mk_contract(i));
            }

            let newcomer = limiter.check_and_record(mk_sender(100 + round), mk_contract(200));
            assert_eq!(
                newcomer,
                RateLimitDecision::Allowed,
                "round {round}: a newcomer must not be starved by busy incumbents"
            );
            assert!(
                limiter.len() <= 8,
                "round {round}: the cap must still bound the map"
            );
        }

        assert_eq!(
            limiter.capacity_rejected_total(),
            0,
            "no UPDATE should have been dropped for capacity"
        );
        assert!(
            ts.now().saturating_duration_since(start) < CLEANUP_AGE,
            "sanity: the fixture must stay inside CLEANUP_AGE, so the TTL sweep \
             cannot be what admitted the newcomers"
        );
    }

    /// Eviction is batched at realistic cap sizes, so the O(map) scan is
    /// amortised instead of paid on every admission.
    ///
    /// Cap 128 gives a batch of `128 / EVICTION_BATCH_DIVISOR` = 2. The
    /// tests above all use cap 8, where the batch clamps to 1, so
    /// without this one the divisor arithmetic is never exercised.
    #[test]
    fn eviction_is_batched_at_larger_caps() {
        let ts = SharedMockTimeSource::new();
        let cap = 128;
        let limiter =
            UpdateRateLimiter::with_config(Arc::new(ts.clone()), MIN_UPDATE_INTERVAL, cap);
        let expected_batch = cap / EVICTION_BATCH_DIVISOR;
        assert_eq!(expected_batch, 2, "fixture assumes a 2-entry batch");

        for i in 0..cap {
            let sender = SocketAddr::from(([10, 1, (i / 256) as u8, (i % 256) as u8], 30000));
            assert!(
                limiter
                    .check_and_record(sender, mk_contract(1))
                    .is_allowed()
            );
            ts.advance(Duration::from_millis(1));
        }
        assert_eq!(limiter.len(), cap);

        assert!(
            limiter
                .check_and_record(mk_sender(99), mk_contract(99))
                .is_allowed()
        );
        assert_eq!(
            limiter.capacity_evicted_total(),
            expected_batch as u64,
            "one admission at capacity must evict a whole batch"
        );
        assert_eq!(
            limiter.len(),
            cap - expected_batch + 1,
            "the batch leaves headroom, so the next admissions skip the scan"
        );

        // The freed headroom is real: the next admission must not evict
        // again.
        assert!(
            limiter
                .check_and_record(mk_sender(98), mk_contract(98))
                .is_allowed()
        );
        assert_eq!(
            limiter.capacity_evicted_total(),
            expected_batch as u64,
            "an admission with headroom must not trigger another eviction"
        );
    }

    /// Pin the atomicity of `check_and_record` under concurrent
    /// callers. Without `DashMap::entry()` holding the shard guard
    /// across the time-comparison + stamp, two threads racing the
    /// same `(sender, contract)` pair could both decide `Allowed`,
    /// both increment `accepted_total`, and both spawn relay work.
    /// Codex review of PR #4285 caught this. The fixed implementation
    /// must serialize exactly one Allowed per `min_interval` window
    /// per pair.
    #[test]
    fn concurrent_check_and_record_admits_one_per_window() {
        use std::sync::{Arc as StdArc, Barrier};
        use std::thread;

        let ts = SharedMockTimeSource::new();
        let limiter = StdArc::new(UpdateRateLimiter::new(
            Arc::new(ts.clone()),
            Ring::DEFAULT_MAX_CONNECTIONS,
        ));
        let sender = mk_sender(1);
        let contract = mk_contract(1);

        const THREADS: usize = 16;
        let barrier = StdArc::new(Barrier::new(THREADS));
        let mut handles = Vec::with_capacity(THREADS);

        for _ in 0..THREADS {
            let l = limiter.clone();
            let b = barrier.clone();
            handles.push(thread::spawn(move || {
                b.wait();
                l.check_and_record(sender, contract)
            }));
        }

        let mut allowed = 0;
        let mut rejected = 0;
        for h in handles {
            match h.join().unwrap() {
                RateLimitDecision::Allowed => allowed += 1,
                RateLimitDecision::Rejected { .. } => rejected += 1,
                RateLimitDecision::CapacityExceeded => panic!("unexpected cap"),
                RateLimitDecision::SenderNewPairBudget => {
                    panic!("one pair cannot exhaust a new-pair budget")
                }
            }
        }
        assert_eq!(
            allowed, 1,
            "exactly ONE concurrent caller must be admitted per window; \
             got {allowed} admits, {rejected} rejects"
        );
        assert_eq!(rejected, THREADS - 1);
        assert_eq!(limiter.accepted_total(), 1);
        assert_eq!(limiter.rejected_total(), (THREADS - 1) as u64);
    }

    /// Source-grep pin: the rate-limit gate at the UPDATE dispatch
    /// site in `node.rs` must cover ALL four UPDATE wire variants
    /// (`RequestUpdate`, `BroadcastTo`, `RequestUpdateStreaming`,
    /// `BroadcastToStreaming`). Codex review of #4285 caught a
    /// previous iteration that only gated `RequestUpdate`, letting
    /// a flooder bypass by switching opcode.
    ///
    /// Strategy: scrape `node.rs` for the UPDATE dispatch block and
    /// assert (a) the rate-limit call site appears, (b) it appears
    /// BEFORE any `start_relay_*` spawn, and (c) the four wire-variant
    /// names are mentioned in the same block (proving the dispatch
    /// matches on all of them).
    #[test]
    fn update_dispatch_gates_all_four_wire_variants() {
        const NODE_SRC: &str = include_str!("../node.rs");

        // Find the UPDATE handler block.
        let block_start = NODE_SRC
            .find("NetMessageV1::Update(ref op) =>")
            .expect("could not locate UPDATE dispatch block in node.rs");

        // Bound the search to the END of the UPDATE arm — find the
        // next `NetMessageV1::` arm that starts at the same match
        // level. The UPDATE block must not "spill" into the next
        // handler for our assertions (Codex re-review nit on #4285
        // — the previous 8KB slice could include the next handler).
        let tail = &NODE_SRC[block_start + 1..];
        let block_len = tail
            .find("\n        NetMessageV1::")
            .or_else(|| tail.find("\n    NetMessageV1::"))
            .unwrap_or(tail.len());
        let block = &NODE_SRC[block_start..block_start + 1 + block_len];

        // (a) the gate is invoked.
        let rate_limit_pos = block
            .find("update_rate_limiter")
            .expect("update_rate_limiter not invoked in UPDATE dispatch block");

        // (b) the gate fires BEFORE the first relay spawn — otherwise
        //     the spawn cost is paid even on rejection.
        let first_spawn_pos = block
            .find("start_relay_request_update(")
            .expect("start_relay_request_update spawn not found in block");
        assert!(
            rate_limit_pos < first_spawn_pos,
            "rate limit gate (offset {rate_limit_pos}) must appear BEFORE \
             the first relay spawn (offset {first_spawn_pos}) so rejected \
             messages don't pay the spawn cost"
        );

        // (c) EVERY wire variant is matched in the dispatch, and each spawn
        //     site is also present. Kept in step with the variant list in
        //     `contract_ban_list.rs`; see the note there about #5147.
        for variant in [
            "UpdateMsg::RequestUpdate {",
            "UpdateMsg::BroadcastTo {",
            "UpdateMsg::RequestUpdateStreaming {",
            "UpdateMsg::BroadcastToStreaming {",
            // #5147 appended these two. The list was NOT extended when they
            // landed, so this guard — whose entire job is to fail when a new
            // UPDATE wire variant appears ungated — silently passed on the very
            // change that added two. No live bypass resulted (the key is
            // extracted from all six variants at node.rs before both gates run),
            // but the guard had stopped guarding.
            "UpdateMsg::BroadcastToV2 {",
            "UpdateMsg::BroadcastToStreamingV2 {",
        ] {
            assert!(
                block.contains(variant),
                "UPDATE dispatch block missing wire variant: `{variant}`. \
                 If a new UPDATE wire variant was added, gate it through \
                 the rate limiter and update this list. If a variant was \
                 removed, update this list."
            );
        }
        // Spawn-site cross-check — the dispatch must invoke all four
        // relay drivers. This pins the variants to actual driver
        // calls (not just match arms), making the test less brittle
        // to comment-only mentions of a variant name.
        for spawn in [
            "start_relay_request_update(",
            "start_relay_broadcast_to(",
            "start_relay_request_update_streaming(",
            "start_relay_broadcast_to_streaming(",
        ] {
            let count = block.matches(spawn).count();
            assert!(
                count >= 1,
                "UPDATE dispatch block does not invoke `{spawn}` — the \
                 corresponding wire variant is not actually gated."
            );
        }
    }

    /// Strict-cap pin: under concurrent insertion of distinct keys
    /// (no shared key), the total accepted count must NOT exceed the
    /// cap regardless of how many threads race. Codex re-review of
    /// PR #4285 caught that the previous `len()`-precheck pattern
    /// could overshoot by up to `num_concurrent_callers` because
    /// every racing caller saw `len < cap` at probe time. The fix
    /// uses an `AtomicUsize::fetch_add` reservation, which strictly
    /// serializes.
    #[test]
    fn concurrent_distinct_keys_do_not_overshoot_cap() {
        use std::sync::{Arc as StdArc, Barrier};
        use std::thread;

        const CAP: usize = 8;
        const THREADS: usize = 64; // 8× the cap to stress the race

        let ts = SharedMockTimeSource::new();
        let limiter = StdArc::new(UpdateRateLimiter::with_config(
            Arc::new(ts.clone()),
            MIN_UPDATE_INTERVAL,
            CAP,
        ));
        let barrier = StdArc::new(Barrier::new(THREADS));
        let mut handles = Vec::with_capacity(THREADS);

        for i in 0..THREADS {
            let l = limiter.clone();
            let b = barrier.clone();
            handles.push(thread::spawn(move || {
                b.wait();
                // Each thread tries a DISTINCT key, so they all
                // exercise the new-pair (Vacant) path concurrently.
                l.check_and_record(mk_sender((i + 1) as u8), mk_contract((i + 1) as u8))
            }));
        }

        let mut allowed = 0;
        let mut cap_rejected = 0;
        let mut rate_rejected = 0;
        for h in handles {
            match h.join().unwrap() {
                RateLimitDecision::Allowed => allowed += 1,
                RateLimitDecision::CapacityExceeded => cap_rejected += 1,
                RateLimitDecision::Rejected { .. } => rate_rejected += 1,
                RateLimitDecision::SenderNewPairBudget => {
                    panic!("each thread uses a distinct sender, so no budget can be spent")
                }
            }
        }
        // Critical invariant, unchanged by #4981: the map never exceeds
        // the cap, no matter how many callers race. Eviction makes this
        // a sharper test than it was — 64 threads now race to evict and
        // insert concurrently, and the reservation counter still has to
        // hold the line.
        //
        // It is an upper bound, NOT an equality. Admissions are not
        // conserved under eviction: a caller can free a slot and then
        // lose it to another thread on every remaining attempt, so a run
        // can legitimately end one or more entries BELOW the cap. Pinning
        // `len() == CAP` made this test fail ~1 run in 100 (#4997
        // review measured it at 44/800 before the eviction scan was
        // serialised, 7/800 after) — a flaky test asserting something the
        // implementation never promised.
        assert!(
            limiter.len() <= CAP,
            "strict cap: map size must never exceed CAP after a 64-thread \
             concurrent flood of distinct keys, got {}",
            limiter.len()
        );
        // What this test used to assert — `allowed == CAP`, every other
        // thread `CapacityExceeded` — was the starvation of #4981 stated
        // as a requirement. At capacity a newcomer now evicts, so a
        // flood of distinct keys admits more than CAP of them while the
        // map stays bounded. What must still hold: at least the first
        // CAP get in, nothing is lost, and no thread is rate-rejected
        // (every key is distinct).
        assert!(
            allowed >= CAP,
            "at least CAP admissions expected under flood, got {allowed}"
        );
        assert_eq!(allowed + cap_rejected + rate_rejected, THREADS);
        assert_eq!(rate_rejected, 0);
        // Capacity rejection is now only reachable by losing an evicted
        // slot to another thread `MAX_ADMISSION_ATTEMPTS` times running,
        // so it is contention-dependent rather than a fixed count.
        assert_eq!(limiter.capacity_rejected_total(), cap_rejected as u64);
        // Conservation, which IS exact: every `Allowed` inserted one
        // entry and every eviction removed one, and nothing else mutates
        // the map here (no `cleanup`, and distinct keys mean no caller
        // ever takes the Occupied restamp path). So whatever the final
        // size turns out to be, it must be exactly what was put in minus
        // what was taken out.
        //
        // This replaces `evicted == allowed - CAP`, which assumed the map
        // ends exactly full and was wrong for the same reason the size
        // assertion above was.
        assert_eq!(
            limiter.len() as u64,
            allowed as u64 - limiter.capacity_evicted_total(),
            "inserts minus evictions must account for every tracked entry: \
             len={} allowed={allowed} evicted={}",
            limiter.len(),
            limiter.capacity_evicted_total()
        );
    }

    /// Pin: both saturation signals stay visible in release builds.
    ///
    /// `crates/core/Cargo.toml` sets `release_max_level_info`, so
    /// anything logged at `debug!` is compiled out of shipped binaries.
    /// That is half of why #4981 went unnoticed for so long: a
    /// production node dropped legitimate relayed UPDATEs and left no
    /// greppable evidence, so the only signal was a dashboard tile a
    /// user happened to ask about. Behavioural tests cannot see a log
    /// level, so this scrapes the source instead — a silent downgrade to
    /// `debug!` would otherwise restore the invisibility with every
    /// other test still green.
    ///
    /// Needles are split with `concat!` so they do not match their own
    /// source through `include_str!`, and both haystacks are compared
    /// with whitespace stripped so rustfmt reflowing a call cannot make
    /// the pin vacuous.
    #[test]
    fn capacity_signals_are_logged_above_debug_level() {
        // Backslashes go too, not just whitespace: these haystacks are
        // SOURCE, so a long message carries `\` line continuations, and
        // where they fall is rustfmt's choice. Stripping whitespace
        // alone leaves the backslash behind and a needle spanning a
        // continuation silently stops matching — which is the same
        // reflow fragility the stripping exists to remove.
        fn strip_ws(s: &str) -> String {
            s.chars()
                .filter(|c| !c.is_whitespace() && *c != '\\')
                .collect()
        }

        /// The macro invoked for the log line whose message contains
        /// `marker`: the nearest `tracing::<level>!` at or before it.
        ///
        /// Searching backwards to the nearest `tracing::` rather than
        /// scanning a fixed-size window, because a window has to be
        /// bigger than the call's structured fields and every field
        /// added eats into it — the eviction site was already within ~19
        /// characters of breaking a 200-char window (#4997 review).
        fn level_of(haystack: &str, marker: &str, what: &str) -> String {
            let pos = haystack.find(marker).unwrap_or_else(|| {
                panic!("the {what} log line must exist; if it was renamed, update this pin rather than deleting it")
            });
            let start = haystack[..pos]
                .rfind(&strip_ws("tracing::"))
                .unwrap_or_else(|| panic!("no tracing:: macro precedes the {what} log line"));
            let tail = &haystack[start..pos];
            tail[..tail.find('!').unwrap_or(tail.len())].to_string()
        }

        // The limiter's own saturation line, in this file.
        let this_file = strip_ws(include_str!("update_rate_limit.rs"));
        assert_eq!(
            level_of(
                &this_file,
                &strip_ws(concat!("UPDATE rate limiter at ", "capacity: evicted")),
                "eviction",
            ),
            strip_ws("tracing::info"),
            "the eviction log must be emitted at info! or higher; debug! is compiled out of \
             release builds by release_max_level_info (#4981)"
        );

        // The fresh-id-churn line, also in this file. This one is the
        // ONLY release-visible evidence of that drop — the dispatch site
        // logs it at `debug!` on purpose, because a sender over its
        // budget is over it on every message and an unthrottled line
        // there would be a flood. Downgrading this one would make the
        // whole signal invisible in release.
        assert_eq!(
            level_of(
                &this_file,
                &strip_ws(concat!(
                    "UPDATE rate limiter: peer is presenting ",
                    "(sender, contract) pairs this node is not tracking"
                )),
                "new-pair-budget",
            ),
            strip_ws("tracing::info"),
            "the new-pair-budget log must be emitted at info! or higher; debug! is compiled out \
             of release builds by release_max_level_info (#4981)"
        );

        // The residual capacity drop, in the UPDATE dispatch path.
        let node_rs = strip_ws(include_str!("../node.rs"));
        assert_eq!(
            level_of(
                &node_rs,
                &strip_ws(concat!("update_dispatch_", "capacity_dropped")),
                "capacity-drop",
            ),
            strip_ws("tracing::info"),
            "dropping an UPDATE for capacity must be logged at info! or higher, so the drop is \
             greppable on a production node (#4981)"
        );
    }

    /// Pin: cleanup decrements the `size` counter by the number of
    /// dropped entries. After all entries roll off, the cap should
    /// be fully available again — strict-cap accounting tracks the
    /// map. Caught a related issue while refactoring the cap
    /// implementation.
    #[test]
    fn cleanup_decrements_size_counter() {
        let ts = SharedMockTimeSource::new();
        let limiter = UpdateRateLimiter::with_config(
            Arc::new(ts.clone()),
            MIN_UPDATE_INTERVAL,
            4, // small cap
        );
        // Fill to cap.
        for i in 0..4 {
            assert_eq!(
                limiter.check_and_record(mk_sender(i + 1), mk_contract(i + 1)),
                RateLimitDecision::Allowed
            );
        }
        // The 5th is admitted by evicting (#4981 changed this from
        // CapacityExceeded); the map stays at the cap either way, which
        // is what this test is really about.
        assert_eq!(
            limiter.check_and_record(mk_sender(5), mk_contract(5)),
            RateLimitDecision::Allowed
        );
        assert_eq!(limiter.len(), 4, "eviction keeps the map at the cap");
        // Age out all entries.
        ts.advance(CLEANUP_AGE + Duration::from_secs(1));
        limiter.cleanup();
        assert_eq!(limiter.len(), 0);
        // Now we should be able to add 4 more without hitting the cap.
        for i in 10..14 {
            assert_eq!(
                limiter.check_and_record(mk_sender(i), mk_contract(i)),
                RateLimitDecision::Allowed,
                "after cleanup, new pair (sender={i}) should be admitted"
            );
        }
    }

    /// Regression pin for the spurious capacity drops #4997's review
    /// measured: under concurrent admission of new pairs at a saturated
    /// cap, `CapacityExceeded` must be vanishingly rare.
    ///
    /// The bug it pins: `evict_oldest` returning "removed nothing" was
    /// read as "the map is empty, so the cap is 0" and the UPDATE was
    /// dropped. Under contention that return means the opposite — every
    /// victim this caller selected was taken by another caller, so slots
    /// WERE just freed — and it did not even spend the retry budget that
    /// exists for the race. Measured at the production cap with 16
    /// threads: 2 699 drops in 320 000 admissions (0.84%), every trial
    /// affected, 100% of them from that branch, retry budget untouched.
    /// After the fix, 7 in 320 000 (0.002%).
    ///
    /// What it actually pins, stated precisely because an earlier
    /// version of this comment credited the wrong fix: making the
    /// caller's `EvictionOutcome::Retry` arm terminal takes this fixture
    /// to 671 drops in 6 400 against a threshold of 6. That arm covers
    /// the early-out and the under-lock re-check, which is where the
    /// measured drops came from — so this pins the caller's handling of
    /// a non-terminal eviction, and with it the serialised scan.
    ///
    /// It does NOT pin the `removed == 0` exit inside `evict_oldest`.
    /// Making that one terminal survives this fixture 15/15 and the
    /// whole suite 29/29, because once the scan is serialised concurrent
    /// evictors no longer steal each other's victims, so `removed == 0`
    /// never occurs here. That exit is real for a different reason and
    /// is pinned by source scrape instead — see
    /// `evict_oldest_has_exactly_one_terminal_exit`.
    ///
    /// The threshold is a bound rather than zero because losing a freed
    /// slot to a concurrent caller on every attempt is genuinely
    /// possible — that is what `CapacityExceeded` is documented to mean.
    #[test]
    fn concurrent_admission_at_capacity_almost_never_drops() {
        use std::sync::{Arc as StdArc, Barrier};
        use std::thread;

        const CAP: usize = 1024;
        const THREADS: usize = 16;
        const PER_THREAD: usize = 400;
        const ADMISSIONS: usize = THREADS * PER_THREAD;
        /// One drop per this many admissions is the ceiling. The bug
        /// produced ~8 per thousand; the fix produces ~0.02.
        ///
        /// Widened from 1 to 2: review measured a worst case of 7 drops
        /// against a threshold of 6 across 540 trials (~1 in 600), so the
        /// old value was a known flake rather than the claimed order of
        /// magnitude of headroom. Widening is the right fix here and a
        /// retry would not be — the assertion bounds a genuinely
        /// stochastic quantity (losing a freed slot to a concurrent
        /// caller), not a deterministic property.
        const MAX_DROPS_PER_THOUSAND: usize = 2;

        let ts = SharedMockTimeSource::new();
        let limiter = StdArc::new(UpdateRateLimiter::with_new_pair_budget(
            Arc::new(ts.clone()),
            MIN_UPDATE_INTERVAL,
            CAP,
            // The new-pair budget is not what this test is about, and a
            // synthetic flood of fresh pairs would otherwise be stopped
            // by it before reaching the capacity path.
            f64::from(u32::MAX),
            THREADS,
        ));
        let barrier = StdArc::new(Barrier::new(THREADS));
        let mut handles = Vec::with_capacity(THREADS);

        for t in 0..THREADS {
            let l = limiter.clone();
            let b = barrier.clone();
            handles.push(thread::spawn(move || {
                b.wait();
                for i in 0..PER_THREAD {
                    // Distinct sender per thread, distinct contract per
                    // iteration: every call takes the new-pair path.
                    let sender = mk_sender(t as u8);
                    let mut id = [0u8; 32];
                    id[..8].copy_from_slice(&(i as u64).to_be_bytes());
                    l.check_and_record(sender, ContractInstanceId::new(id));
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        assert!(
            limiter.capacity_evicted_total() > 0,
            "fixture must actually saturate the cap, or it pins nothing"
        );
        let drops = limiter.capacity_rejected_total();
        assert!(
            drops as usize * 1000 <= ADMISSIONS * MAX_DROPS_PER_THOUSAND,
            "at most {MAX_DROPS_PER_THOUSAND} drop per 1000 admissions, got {drops} \
             in {ADMISSIONS}. An eviction that removed nothing means a concurrent \
             caller freed the slots, so the admission must retry, not drop."
        );
        assert!(limiter.len() <= CAP);
    }
}

//! Rate limiting for the ResyncRequest/ResyncResponse full-state protocol
//! (#4861).
//!
//! ## Why
//!
//! When a delta broadcast fails to apply, the UPDATE relay emits a
//! `ResyncRequest`, and the receiver replies with a full-state `ResyncResponse`
//! that is re-merged. For a "poison" contract whose deltas fail from many
//! senders this becomes a self-sustaining full-state storm (~16 resyncs/min per
//! contract in the incident): each failure pulls a fresh full state that fails
//! again. The per-contract merge backoff (`crate::ring::merge_backoff`) stops
//! re-running the merge, but two amplification edges remain and this module
//! caps both:
//!
//! - **Emit side** (per-contract): a hard ceiling on how often *this* node emits
//!   a `ResyncRequest` for a given contract, independent of the merge outcome.
//! - **Responder side** (per `(peer, contract)`): a hard ceiling on how often
//!   this node answers a *given peer's* `ResyncRequest` for a contract. This is
//!   the mixed-version-rollout guard: a not-yet-upgraded peer that still emits
//!   unlimited `ResyncRequest`s cannot make an upgraded peer full-state-reply in
//!   a loop.
//!
//! ## Mechanism: token bucket
//!
//! Each key owns a token bucket with capacity `burst` refilling one token per
//! `refill_interval`. A check consumes one token if available (allow) or is
//! denied. This gives a small burst for a genuinely-diverged contract to resync
//! promptly, then a flat steady-state ceiling of `1 / refill_interval`.
//!
//! ## Bounded growth
//!
//! Same discipline as [`crate::ring::update_rate_limit::UpdateRateLimiter`]: a
//! [`DashMap`] with a strict [`AtomicUsize`] size cap so an attacker churning
//! keys cannot grow the map, plus a periodic TTL sweep hooked into the Ring
//! reaper.
//!
//! There is deliberately NO per-connection cleanup on peer disconnect for the
//! per-`(peer, contract)` responder map (#4864 review — declined): a
//! disconnected peer's entries simply age out via the [`CLEANUP_AGE`] TTL sweep,
//! and the total is bounded by the strict [`MAX_TRACKED_KEYS`] cap plus the
//! global per-contract cap. Under extreme `(peer, contract)` churn a NEW key at
//! capacity is denied (`check_and_record` returns `false`), which for the
//! responder means "don't send this response" — a safe, conservative outcome,
//! not a correctness bug. This matches the `UpdateRateLimiter` precedent, which
//! also relies on the TTL sweep rather than disconnect hooks.

use std::collections::VecDeque;
use std::hash::Hash;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use freenet_stdlib::prelude::ContractInstanceId;
use tokio::time::Instant;

use crate::util::time_source::TimeSource;

/// Per-contract minimum interval between emitted `ResyncRequest`s. A diverged
/// contract only needs an occasional full-state pull; a storm hits this ceiling.
pub(crate) const EMIT_REFILL_INTERVAL: Duration = Duration::from_secs(60);
/// Burst of emitted `ResyncRequest`s allowed before the steady-state ceiling.
pub(crate) const EMIT_BURST: f64 = 2.0;

/// Per-`(peer, contract)` minimum interval between sent `ResyncResponse`s.
pub(crate) const RESPOND_REFILL_INTERVAL: Duration = Duration::from_secs(30);
/// Burst of full-state responses to a single peer for a contract.
pub(crate) const RESPOND_BURST: f64 = 1.0;

/// GLOBAL per-contract minimum interval between sent `ResyncResponse`s,
/// aggregated across ALL requesters (#4861). Per-`(peer, contract)` limiting
/// alone is insufficient: production saw ~45 distinct requester IPs for one
/// forked contract driving ~9,733 full-state responses in a day. This caps a
/// single contract's total resync-response cost at ≈12/min regardless of how
/// many distinct peers ask.
pub(crate) const RESPOND_GLOBAL_REFILL_INTERVAL: Duration = Duration::from_secs(5);
/// Burst of full-state responses for a contract across all requesters.
pub(crate) const RESPOND_GLOBAL_BURST: f64 = 2.0;

/// Hard cap on tracked keys (per limiter). At ~64 bytes/entry, 16 384 ≈ 1 MB.
pub(crate) const MAX_TRACKED_KEYS: usize = 16_384;

/// Idle-key cleanup age. A fully-refilled key idle this long is dropped (a fresh
/// key gets a full bucket, so dropping a recovered one loses nothing).
const CLEANUP_AGE: Duration = Duration::from_secs(5 * 60);

struct Bucket {
    tokens: f64,
    last_refill: Instant,
}

impl Bucket {
    /// Refill by elapsed-time / interval, capped at `capacity`.
    fn refill(&mut self, now: Instant, capacity: f64, interval: Duration) {
        let elapsed = now.saturating_duration_since(self.last_refill);
        let gained = elapsed.as_secs_f64() / interval.as_secs_f64();
        self.tokens = (self.tokens + gained).min(capacity);
        self.last_refill = now;
    }
}

/// Outcome of a [`TokenBucketLimiter::check_and_record_detailed`] check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BucketOutcome {
    /// A token was consumed; the event is allowed.
    Allowed,
    /// The key's bucket is empty: it is over its rate.
    RateLimited,
    /// The key is not tracked and the map has no room for it, so nothing
    /// is known about its rate. Distinct from `RateLimited` because it
    /// says nothing about THIS key — it is a statement about the map.
    Untracked,
}

/// A token-bucket rate limiter keyed by `K`. One token per key is consumed per
/// allowed event; the bucket holds up to `capacity` and refills one token per
/// `refill_interval`.
pub(crate) struct TokenBucketLimiter<K: Eq + Hash + Clone> {
    buckets: DashMap<K, Bucket>,
    size: AtomicUsize,
    max_tracked: usize,
    capacity: f64,
    refill_interval: Duration,
    time_source: Arc<dyn TimeSource + Send + Sync>,
    allowed_total: AtomicU64,
    suppressed_total: AtomicU64,
}

impl<K: Eq + Hash + Clone> TokenBucketLimiter<K> {
    pub fn new(
        time_source: Arc<dyn TimeSource + Send + Sync>,
        capacity: f64,
        refill_interval: Duration,
        max_tracked: usize,
    ) -> Self {
        Self {
            buckets: DashMap::new(),
            size: AtomicUsize::new(0),
            max_tracked,
            capacity,
            refill_interval,
            time_source,
            allowed_total: AtomicU64::new(0),
            suppressed_total: AtomicU64::new(0),
        }
    }

    /// Check-and-consume one token for `key`. Returns `true` when the event is
    /// allowed (a token was consumed), `false` when rate-limited or the tracking
    /// map is at capacity for a new key.
    pub fn check_and_record(&self, key: K) -> bool {
        matches!(self.check_and_record_detailed(key), BucketOutcome::Allowed)
    }

    /// [`Self::check_and_record`] without collapsing "this key is over its
    /// rate" and "the tracking map has no room for this key" into one
    /// `false`.
    ///
    /// The distinction matters to any caller whose keys are NOT
    /// attacker-chosen: for those, a full map is a sizing accident rather
    /// than evidence about the key, and refusing on it starves a
    /// newcomer for someone else's behaviour. `check_and_record` keeps
    /// the collapsed form so existing callers are unchanged.
    pub fn check_and_record_detailed(&self, key: K) -> BucketOutcome {
        let now = self.time_source.now();
        use dashmap::mapref::entry::Entry;
        match self.buckets.entry(key) {
            Entry::Occupied(mut occ) => {
                let bucket = occ.get_mut();
                bucket.refill(now, self.capacity, self.refill_interval);
                if bucket.tokens >= 1.0 {
                    bucket.tokens -= 1.0;
                    self.allowed_total.fetch_add(1, Ordering::Relaxed);
                    BucketOutcome::Allowed
                } else {
                    self.suppressed_total.fetch_add(1, Ordering::Relaxed);
                    BucketOutcome::RateLimited
                }
            }
            Entry::Vacant(vac) => {
                // Reserve a slot before inserting (strict cap, see UpdateRateLimiter).
                let prev = self.size.fetch_add(1, Ordering::Relaxed);
                if prev >= self.max_tracked {
                    self.size.fetch_sub(1, Ordering::Relaxed);
                    self.suppressed_total.fetch_add(1, Ordering::Relaxed);
                    return BucketOutcome::Untracked;
                }
                // A brand-new key starts with a full bucket and spends one token.
                vac.insert(Bucket {
                    tokens: self.capacity - 1.0,
                    last_refill: now,
                });
                self.allowed_total.fetch_add(1, Ordering::Relaxed);
                BucketOutcome::Allowed
            }
        }
    }

    /// Drop fully-refilled keys idle longer than [`CLEANUP_AGE`]. Call from the
    /// Ring reaper. A dropped key is indistinguishable from a fresh one (both get
    /// a full bucket), so this never changes behavior — only bounds memory.
    /// Retained entries are left unmutated (their `last_refill` stays the real
    /// last-use time, so the next check refills correctly).
    pub fn cleanup(&self) {
        let now = self.time_source.now();
        let capacity = self.capacity;
        let interval = self.refill_interval;
        let mut removed = 0usize;
        self.buckets.retain(|_, bucket| {
            let elapsed = now.saturating_duration_since(bucket.last_refill);
            let gained = elapsed.as_secs_f64() / interval.as_secs_f64();
            let tokens_now = (bucket.tokens + gained).min(capacity);
            let recovered = tokens_now >= capacity;
            let idle = elapsed > CLEANUP_AGE;
            if recovered && idle {
                removed += 1;
                false
            } else {
                true
            }
        });
        if removed > 0 {
            self.size.fetch_sub(removed, Ordering::Relaxed);
        }
    }

    /// Total events allowed since creation (dashboard / tests).
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn allowed_total(&self) -> u64 {
        self.allowed_total.load(Ordering::Relaxed)
    }

    /// Total events suppressed (rate-limited or capacity) since creation.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn suppressed_total(&self) -> u64 {
        self.suppressed_total.load(Ordering::Relaxed)
    }

    /// Number of tracked keys (tests / dashboard).
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn len(&self) -> usize {
        self.buckets.len()
    }
}

/// A per-contract `ResyncRequest` emit limiter.
pub(crate) type ResyncEmitLimiter = TokenBucketLimiter<ContractInstanceId>;
/// A per-`(peer, contract)` `ResyncResponse` responder limiter.
pub(crate) type ResyncResponseLimiter = TokenBucketLimiter<(SocketAddr, ContractInstanceId)>;
/// A GLOBAL per-contract `ResyncResponse` limiter (aggregated over all peers).
pub(crate) type ResyncResponseGlobalLimiter = TokenBucketLimiter<ContractInstanceId>;

/// Build the emit-side limiter with the tuned defaults.
pub(crate) fn new_emit_limiter(
    time_source: Arc<dyn TimeSource + Send + Sync>,
) -> ResyncEmitLimiter {
    TokenBucketLimiter::new(
        time_source,
        EMIT_BURST,
        EMIT_REFILL_INTERVAL,
        MAX_TRACKED_KEYS,
    )
}

/// Build the per-`(peer, contract)` responder-side limiter with tuned defaults.
pub(crate) fn new_response_limiter(
    time_source: Arc<dyn TimeSource + Send + Sync>,
) -> ResyncResponseLimiter {
    TokenBucketLimiter::new(
        time_source,
        RESPOND_BURST,
        RESPOND_REFILL_INTERVAL,
        MAX_TRACKED_KEYS,
    )
}

/// Build the GLOBAL per-contract responder-side limiter with tuned defaults.
pub(crate) fn new_response_global_limiter(
    time_source: Arc<dyn TimeSource + Send + Sync>,
) -> ResyncResponseGlobalLimiter {
    TokenBucketLimiter::new(
        time_source,
        RESPOND_GLOBAL_BURST,
        RESPOND_GLOBAL_REFILL_INTERVAL,
        MAX_TRACKED_KEYS,
    )
}

/// TTL for an outstanding `ResyncRequest` correlation entry (#4864 round-8). One
/// [`EMIT_REFILL_INTERVAL`] (60s): a legitimate `ResyncResponse` arrives well
/// within this, so a later one is treated as unsolicited and healed by the ~5-min
/// anti-entropy heartbeat instead of executing a full-state WASM merge.
pub(crate) const OUTSTANDING_RESYNC_TTL: Duration = Duration::from_secs(60);

/// One outstanding `ResyncRequest` we emitted (#5510 N7).
#[derive(Debug, Clone, Copy)]
struct OutstandingRequest {
    /// Identifies THIS request among the pair's outstanding ones.
    generation: u64,
    /// When it was emitted, for the TTL.
    emitted: Instant,
}

/// Per-pair cap on concurrently-outstanding requests.
///
/// The chain bound already keeps a well-behaved pair far below this. The cap is
/// here because a pair is not obliged to be well-behaved, and without it one
/// pair could occupy the whole map's budget and push every other pair out.
const MAX_OUTSTANDING_PER_PAIR: usize = 8;

/// Correlation map for outstanding `ResyncRequest`s (#4864 round-8, Codex P1).
///
/// A received `ResyncResponse` triggers a full-state WASM merge on the resync
/// apply path that is deliberately NOT gated by the merge-failure backoff (a
/// full-state apply is fork-flippable, so gating it there could suppress a
/// genuine heal). Without correlation, that makes the apply path an unmetered
/// DoS surface: a peer can stream or replay unsolicited `ResyncResponse`s, each
/// burning up to a full WASM budget, bypassing every emitter-side gate (the emit
/// limiter only bounds OUR requests).
///
/// This records `(contract, target_addr)` when WE emit a `ResyncRequest`; the
/// receive arm require-and-consumes a matching `(contract, source_addr)` entry
/// BEFORE the apply, dropping the response (no WASM) on no match.
/// Consume-on-first-match makes replay dead — a second copy of a solicited
/// response finds no entry.
///
/// Mixed-version safe: an old peer only ever sends a `ResyncResponse` in reply to
/// OUR `ResyncRequest`, so a legitimate response always has a matching entry
/// unless it raced the [`OUTSTANDING_RESYNC_TTL`] — that corner heals via
/// anti-entropy. Bounded exactly like the limiters (strict [`MAX_TRACKED_KEYS`]
/// cap + reaper TTL sweep) so a request storm can't grow the map without bound.
pub(crate) struct OutstandingResyncRequests {
    /// `(contract, peer)` → the time we emitted the request.
    /// One entry per PAIR, holding every outstanding request for that pair
    /// oldest-first (#5510 N7).
    ///
    /// A single `Instant` per pair was wrong in a narrow but real way. A
    /// `ResyncResponse` carries no request id, so the only thing that can match
    /// it to a request is arrival order. With one slot, a late response to the
    /// PREVIOUS window's request — generated before the drop that is now being
    /// repaired, arriving after the next request was recorded — overwrote or
    /// consumed the record for the CURRENT request, and the chain's "did mine
    /// land?" check then read false and exited as though answered. If the
    /// current request had in fact been throttled by the responder or lost, the
    /// update waited for anti-entropy.
    entries: DashMap<(ContractInstanceId, SocketAddr), VecDeque<OutstandingRequest>>,
    /// Total outstanding REQUESTS, not pairs, so the cap bounds memory.
    size: AtomicUsize,
    max_tracked: usize,
    /// Source of generation ids. A counter rather than the emit `Instant`,
    /// because two records can share an instant under an injected clock and two
    /// generations must never be confused for one.
    next_generation: AtomicU64,
    ttl: Duration,
    time_source: Arc<dyn TimeSource + Send + Sync>,
}

impl OutstandingResyncRequests {
    pub fn new(time_source: Arc<dyn TimeSource + Send + Sync>) -> Self {
        Self {
            entries: DashMap::new(),
            next_generation: AtomicU64::new(0),
            size: AtomicUsize::new(0),
            max_tracked: MAX_TRACKED_KEYS,
            ttl: OUTSTANDING_RESYNC_TTL,
            time_source,
        }
    }

    /// Record that we emitted a `ResyncRequest` for `contract` to `target`.
    ///
    /// Re-emitting to the same `(contract, target)` refreshes the timestamp. A
    /// brand-new key at the strict cap is DROPPED (fail-closed): the eventual
    /// response is then treated as unsolicited, so we forgo one heal that
    /// anti-entropy recovers — never a correctness loss, and the cap keeps an
    /// attacker from growing the map by inducing our emissions.
    /// Returns `true` iff the request is now CORRELATED (an entry exists). A
    /// `false` return means the strict cap rejected a brand-new key, so no
    /// entry was stored — callers that later read [`Self::is_outstanding`] as a
    /// "did it land?" signal MUST NOT treat that absence as "landed" (#4863).
    #[must_use]
    pub fn record(&self, contract: ContractInstanceId, target: SocketAddr) -> bool {
        self.record_generation(contract, target).is_some()
    }

    /// As [`Self::record`], but returns the GENERATION of the request recorded,
    /// so a caller can later ask whether ITS OWN request is still outstanding
    /// rather than whether any request for the pair is (#5510 N7).
    ///
    /// `None` means the strict cap rejected it and nothing was stored; the
    /// request is UNCORRELATED and [`Self::is_outstanding`] will read `false`
    /// for it forever, which must not be taken as "landed" (#4863).
    #[must_use]
    pub fn record_generation(
        &self,
        contract: ContractInstanceId,
        target: SocketAddr,
    ) -> Option<u64> {
        let now = self.time_source.now();
        let prev = self.size.fetch_add(1, Ordering::Relaxed);
        if prev >= self.max_tracked {
            self.size.fetch_sub(1, Ordering::Relaxed);
            return None;
        }
        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
        let mut queue = self.entries.entry((contract, target)).or_default();
        // Per-pair cap. The chain bound already keeps this small, but a pair is
        // not obliged to be well-behaved: without a cap one pair could hold the
        // whole map's budget.
        if queue.len() >= MAX_OUTSTANDING_PER_PAIR {
            queue.pop_front();
            self.size.fetch_sub(1, Ordering::Relaxed);
        }
        queue.push_back(OutstandingRequest {
            generation,
            emitted: now,
        });
        Some(generation)
    }

    /// Require-and-consume a matching outstanding entry for a received
    /// `ResyncResponse` from `source`. Returns `true` iff a NON-EXPIRED matching
    /// entry existed — the caller may then apply the state. The entry is REMOVED
    /// on any match (expired or not), so a replay/duplicate of the same response
    /// finds nothing and is rejected.
    pub fn consume(&self, contract: ContractInstanceId, source: SocketAddr) -> bool {
        let now = self.time_source.now();
        let mut empty = false;
        let matched = match self.entries.get_mut(&(contract, source)) {
            Some(mut queue) => {
                // OLDEST first. A response carries no request id, so arrival
                // order is the only correspondence available, and the oldest
                // outstanding request is the one a response most plausibly
                // answers. Consuming the newest instead would let a late reply
                // to an old request cancel the correlation of the request
                // currently being retried.
                let front = queue.pop_front();
                empty = queue.is_empty();
                match front {
                    Some(req) => {
                        self.size.fetch_sub(1, Ordering::Relaxed);
                        now.saturating_duration_since(req.emitted) <= self.ttl
                    }
                    None => false,
                }
            }
            None => false,
        };
        if empty {
            self.entries
                .remove_if(&(contract, source), |_, q| q.is_empty());
        }
        matched
    }

    /// Read-only: is there a NON-EXPIRED outstanding request for
    /// `(contract, peer)`?
    ///
    /// Unlike [`Self::consume`] this does NOT remove the entry, so the receive
    /// arm's one-shot authorization stays intact.
    ///
    /// This is the "did our `ResyncRequest` land?" signal for the queue-full
    /// resync retry (#4863). The entry is removed exactly when a matching
    /// `ResyncResponse` is accepted at the receive arm.
    ///
    /// ONLY MEANINGFUL FOR A CORRELATED REQUEST. Callers must gate on
    /// [`Self::record`] having returned `true`; for an UNCORRELATED request
    /// (brand-new key rejected by the strict cap) this returns `false`
    /// immediately and forever, which must NOT be read as "landed". A
    /// re-dispatch still heals in that case: the responder clears its cached
    /// summary of us on receiving the `ResyncRequest` itself (`node.rs`,
    /// `update_peer_summary(.., None)`), which is the #4857 heal and does not
    /// depend on our correlation record at all. Only the full-state
    /// `ResyncResponse` needs the record. Treating the cap rejection as
    /// "landed" would silently disable the retry node-wide while the map is
    /// full.
    ///
    /// So for a correlated request `false` means one of:
    ///
    /// - the response arrived and was authorized at the receive arm — the
    ///   redundant re-dispatch would only make the sender clear its summary and
    ///   ship full state again, onto a receiver whose queue was just full;
    /// - the entry expired (TTL) — unreachable inside the retry window, which
    ///   is bounded by one reservation interval well under the TTL.
    ///
    /// `true` (request still outstanding) is the case where a retry can help.
    ///
    /// Note this observes AUTHORIZATION, not application: the receive arm
    /// consumes the entry BEFORE applying the state, and the apply can still
    /// fail (including with another `ContractQueueFull`). Absence therefore
    /// means "the response arrived and we authorized it", not "the state
    /// merged".
    ///
    /// **No production caller since #5510 N7**, which moved both the chain's X2
    /// check and the #4863 re-delivery gate onto
    /// [`Self::is_generation_outstanding`]. Kept because the pair-level question
    /// is still the right one for a test to ask — "is anything outstanding for
    /// this pair" — and because a future caller that genuinely wants the
    /// aggregate should find it here rather than reinvent it. Anything asking
    /// "did MY request land" must use the generation form: this one answers
    /// `false` as soon as any response retires any generation.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn is_outstanding(&self, contract: ContractInstanceId, peer: SocketAddr) -> bool {
        let now = self.time_source.now();
        self.entries.get(&(contract, peer)).is_some_and(|queue| {
            queue
                .iter()
                .any(|req| now.saturating_duration_since(req.emitted) <= self.ttl)
        })
    }

    /// Is the request with THIS generation still outstanding (#5510 N7)?
    ///
    /// The distinction from [`Self::is_outstanding`] is the whole point: a
    /// chain asking "did any request for this pair go unanswered" gets the
    /// wrong answer when a late response to an earlier window consumed an
    /// entry. Asking about its own generation cannot be confused that way.
    pub fn is_generation_outstanding(
        &self,
        contract: ContractInstanceId,
        peer: SocketAddr,
        generation: u64,
    ) -> bool {
        let now = self.time_source.now();
        self.entries.get(&(contract, peer)).is_some_and(|queue| {
            queue.iter().any(|req| {
                req.generation == generation
                    && now.saturating_duration_since(req.emitted) <= self.ttl
            })
        })
    }

    /// Drop entries older than the TTL. Call from the Ring reaper so a burst of
    /// requests whose responses never arrive cannot pin memory past the TTL.
    pub fn cleanup(&self) {
        let now = self.time_source.now();
        let ttl = self.ttl;
        let mut removed = 0usize;
        self.entries.retain(|_, queue| {
            let before = queue.len();
            queue.retain(|req| now.saturating_duration_since(req.emitted) <= ttl);
            removed += before - queue.len();
            !queue.is_empty()
        });
        if removed > 0 {
            self.size.fetch_sub(removed, Ordering::Relaxed);
        }
    }

    /// Number of tracked outstanding requests (tests / dashboard).
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

/// Build the outstanding-request correlation map.
pub(crate) fn new_outstanding_resync_requests(
    time_source: Arc<dyn TimeSource + Send + Sync>,
) -> OutstandingResyncRequests {
    OutstandingResyncRequests::new(time_source)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::time_source::SharedMockTimeSource;

    fn mk_contract(byte: u8) -> ContractInstanceId {
        ContractInstanceId::new([byte; 32])
    }

    fn mk_peer(byte: u8) -> SocketAddr {
        SocketAddr::from(([10, 0, 0, byte], 30000 + byte as u16))
    }

    #[test]
    fn outstanding_resync_solicited_response_consumes_and_replay_is_rejected() {
        let ts = SharedMockTimeSource::new();
        let m = new_outstanding_resync_requests(Arc::new(ts.clone()));
        let c = mk_contract(1);
        let peer = mk_peer(1);

        // No entry → an unsolicited response is rejected and applies nothing.
        assert!(
            !m.consume(c, peer),
            "a response with no outstanding request must be rejected"
        );

        // We emit a request → the response from that peer is authorized ONCE.
        assert!(m.record(c, peer), "record must correlate under the cap");
        assert_eq!(m.len(), 1);
        assert!(
            m.consume(c, peer),
            "the solicited response must be accepted"
        );
        assert_eq!(m.len(), 0, "consume must remove the entry");

        // Replay of the same response finds no entry → rejected (replay is dead).
        assert!(
            !m.consume(c, peer),
            "a replayed/duplicate response must be rejected after the first consume"
        );
    }

    /// #5510 N7: a LATE response to an earlier window must not cancel the
    /// correlation of the request currently outstanding.
    ///
    /// A `ResyncResponse` carries no request id, so nothing in the message says
    /// which request it answers. With one slot per pair, a response generated
    /// before the swallowed broadcast and arriving after the next request was
    /// recorded consumed that next request's record. The chain's "did mine
    /// land?" check then read false and it exited as though answered — and if
    /// its request had in fact been throttled by the responder or lost, the
    /// update waited for anti-entropy.
    ///
    /// Generations make the question answerable: consumption is oldest-first,
    /// so a late reply retires the request it plausibly answers, and the chain
    /// asks about ITS OWN generation rather than about the pair.
    ///
    /// This is the out-of-order case `.claude/rules/testing.md` asks to cover.
    #[test]
    fn a_late_response_to_an_earlier_window_does_not_retire_the_current_request() {
        let ts = SharedMockTimeSource::new();
        let m = new_outstanding_resync_requests(Arc::new(ts.clone()));
        let c = mk_contract(7);
        let peer = mk_peer(7);

        // Window 1's request.
        let first = m
            .record_generation(c, peer)
            .expect("first record must correlate under the cap");

        // The window closes and the chain records window 2's request. Both are
        // now outstanding; the responder has answered neither.
        ts.advance_time(Duration::from_secs(30));
        let second = m
            .record_generation(c, peer)
            .expect("second record must correlate");
        assert_ne!(
            first, second,
            "each record must take a distinct generation, or the two requests \
             cannot be told apart at all"
        );

        // NOW window 1's response arrives, late. It retires window 1's request,
        // oldest-first, and must leave window 2's alone.
        assert!(
            m.consume(c, peer),
            "the late response is still within the TTL, so it authorizes"
        );
        assert!(
            !m.is_generation_outstanding(c, peer, first),
            "the response must retire the OLDEST outstanding request, which is \
             the one it plausibly answers"
        );
        assert!(
            m.is_generation_outstanding(c, peer, second),
            "and the CURRENT request must remain outstanding: nothing has \
             answered it. Retiring it here is what made a chain exit believing \
             its own repair had landed (#5510 N7)"
        );

        // A second response retires window 2's request in turn.
        assert!(m.consume(c, peer), "the second response authorizes too");
        assert!(
            !m.is_generation_outstanding(c, peer, second),
            "and now nothing is outstanding for the pair"
        );
        assert!(
            !m.is_outstanding(c, peer),
            "the pair-level view must agree once every generation is retired"
        );
    }

    /// #4863: `is_outstanding` is the queue-full retry's "did our request land?"
    /// signal. It must be READ-ONLY (leaving the receive arm's one-shot
    /// authorization intact), scoped per `(contract, peer)`, and TTL-aware — an
    /// expired entry's response would be rejected anyway, so it is not a live
    /// request.
    #[test]
    fn outstanding_resync_is_outstanding_is_read_only_and_ttl_aware() {
        let ts = SharedMockTimeSource::new();
        let m = new_outstanding_resync_requests(Arc::new(ts.clone()));
        let c = mk_contract(1);
        let peer = mk_peer(1);

        // Nothing recorded → not outstanding.
        assert!(!m.is_outstanding(c, peer));

        assert!(m.record(c, peer), "record must correlate under the cap");
        assert!(m.is_outstanding(c, peer), "a fresh record is outstanding");

        // Scoped per (contract, peer) — a different peer or contract is unrelated.
        assert!(!m.is_outstanding(c, mk_peer(2)));
        assert!(!m.is_outstanding(mk_contract(2), peer));

        // READ-ONLY: repeated checks must not remove the entry, so the receive
        // arm's require-and-consume still succeeds afterwards.
        assert!(m.is_outstanding(c, peer));
        assert_eq!(m.len(), 1, "is_outstanding must not remove the entry");
        assert!(
            m.consume(c, peer),
            "the receive arm's one-shot authorization must survive is_outstanding"
        );

        // Once consumed (the ResyncResponse landed and was applied), the request
        // is no longer outstanding — this is what makes the retry skip.
        assert!(
            !m.is_outstanding(c, peer),
            "a consumed request must read as landed so the #4863 retry skips"
        );

        // TTL-aware: an entry past the TTL is not a live request (its response
        // would be rejected as unsolicited, so re-dispatching cannot heal).
        assert!(m.record(c, peer), "record must correlate under the cap");
        ts.advance_time(OUTSTANDING_RESYNC_TTL - Duration::from_millis(1));
        assert!(
            m.is_outstanding(c, peer),
            "just under the TTL is still live"
        );
        ts.advance_time(Duration::from_millis(2));
        assert!(!m.is_outstanding(c, peer), "past the TTL is not live");
    }

    #[test]
    fn outstanding_resync_is_scoped_per_contract_and_peer() {
        let ts = SharedMockTimeSource::new();
        let m = new_outstanding_resync_requests(Arc::new(ts.clone()));
        let c = mk_contract(1);
        assert!(
            m.record(c, mk_peer(1)),
            "record must correlate under the cap"
        );
        // A response for the SAME contract from a DIFFERENT peer is unsolicited.
        assert!(!m.consume(c, mk_peer(2)));
        // A response for a DIFFERENT contract from the recorded peer is unsolicited.
        assert!(!m.consume(mk_contract(2), mk_peer(1)));
        // The real match still stands and consumes.
        assert!(m.consume(c, mk_peer(1)));
    }

    #[test]
    fn outstanding_resync_ttl_expiry_rejects_and_cleanup_reaps() {
        let ts = SharedMockTimeSource::new();
        let m = new_outstanding_resync_requests(Arc::new(ts.clone()));
        let c = mk_contract(1);
        let peer = mk_peer(1);

        // Just UNDER the TTL → still accepted.
        assert!(m.record(c, peer), "record must correlate under the cap");
        ts.advance_time(OUTSTANDING_RESYNC_TTL - Duration::from_millis(1));
        assert!(
            m.consume(c, peer),
            "a response within the TTL must be accepted"
        );

        // Past the TTL → the stale entry is consumed-and-REJECTED.
        assert!(m.record(c, peer), "record must correlate under the cap");
        ts.advance_time(OUTSTANDING_RESYNC_TTL + Duration::from_millis(1));
        assert!(
            !m.consume(c, peer),
            "a response arriving past the TTL must be rejected as unsolicited"
        );
        assert_eq!(m.len(), 0, "a consumed (even if stale) entry is removed");

        // cleanup() reaps expired entries whose response never arrived.
        assert!(
            m.record(mk_contract(9), mk_peer(9)),
            "record must correlate under the cap"
        );
        assert_eq!(m.len(), 1);
        ts.advance_time(OUTSTANDING_RESYNC_TTL + Duration::from_secs(1));
        m.cleanup();
        assert_eq!(m.len(), 0, "cleanup must reap entries older than the TTL");
    }

    #[test]
    fn outstanding_resync_is_strictly_capped() {
        let ts = SharedMockTimeSource::new();
        let m = new_outstanding_resync_requests(Arc::new(ts.clone()));
        // Fill to the cap with distinct (contract, peer) keys.
        for i in 0..MAX_TRACKED_KEYS {
            let c = ContractInstanceId::new([(i % 256) as u8; 32]);
            let peer = SocketAddr::from(([10, (i >> 16) as u8, (i >> 8) as u8, i as u8], 40000));
            assert!(m.record(c, peer), "under-cap record must correlate");
        }
        assert_eq!(m.len(), MAX_TRACKED_KEYS, "map fills to exactly the cap");
        // One more distinct key is dropped (fail-closed), not admitted.
        let overflow_c = ContractInstanceId::new([0xAB; 32]);
        let overflow_peer = SocketAddr::from(([172, 16, 0, 1], 41000));
        // #4863: the over-cap record must REPORT the rejection. This bool is what
        // stops the queue-full retry from mistaking a never-correlated request for
        // a landed one and disabling itself node-wide while the map is full.
        assert!(
            !m.record(overflow_c, overflow_peer),
            "over-cap record must report the request as UNCORRELATED so the \
             queue-full retry falls back to unconditional re-dispatch (#4863)"
        );
        assert_eq!(m.len(), MAX_TRACKED_KEYS, "over-cap insert is dropped");
        // It reads as NOT outstanding — which is exactly why the retry must not
        // gate on `is_outstanding` alone for an uncorrelated request. Assert this
        // BEFORE the mutating `consume` below so the read is unambiguous.
        assert!(!m.is_outstanding(overflow_c, overflow_peer));
        // Its response is therefore treated as unsolicited.
        assert!(!m.consume(overflow_c, overflow_peer));
    }

    #[test]
    fn emit_allows_burst_then_throttles() {
        let ts = SharedMockTimeSource::new();
        let l = new_emit_limiter(Arc::new(ts.clone()));
        let c = mk_contract(1);
        // Burst of 2 allowed immediately.
        assert!(l.check_and_record(c));
        assert!(l.check_and_record(c));
        // Third within the window is throttled.
        assert!(!l.check_and_record(c));
        assert_eq!(l.allowed_total(), 2);
        assert_eq!(l.suppressed_total(), 1);
    }

    #[test]
    fn emit_refills_one_per_interval() {
        let ts = SharedMockTimeSource::new();
        let l = new_emit_limiter(Arc::new(ts.clone()));
        let c = mk_contract(1);
        // Drain the burst.
        assert!(l.check_and_record(c));
        assert!(l.check_and_record(c));
        assert!(!l.check_and_record(c));
        // After one refill interval, exactly one more is allowed.
        ts.advance_time(EMIT_REFILL_INTERVAL + Duration::from_secs(1));
        assert!(l.check_and_record(c));
        assert!(!l.check_and_record(c), "only one token refilled");
    }

    #[test]
    fn emit_flood_is_bounded_to_ceil_window_over_interval() {
        // Model of `may21_flood_pattern_is_throttled`: a flood of delta
        // failures over a 5-minute window admits at most burst + window/interval
        // ResyncRequests.
        let ts = SharedMockTimeSource::new();
        let l = new_emit_limiter(Arc::new(ts.clone()));
        let c = mk_contract(1);
        // 300 attempts, one per second (5 minutes).
        for _ in 0..300 {
            l.check_and_record(c);
            ts.advance_time(Duration::from_secs(1));
        }
        // Expected admits ≈ burst(2) + 300s/60s = 2 + 5 = 7. Allow a small band.
        let allowed = l.allowed_total();
        assert!(
            (6..=8).contains(&allowed),
            "flood over 5 min must admit ~7 ResyncRequests, got {allowed}"
        );
        assert_eq!(allowed + l.suppressed_total(), 300);
    }

    #[test]
    fn emit_different_contracts_independent() {
        let ts = SharedMockTimeSource::new();
        let l = new_emit_limiter(Arc::new(ts.clone()));
        // Each contract gets its own bucket.
        assert!(l.check_and_record(mk_contract(1)));
        assert!(l.check_and_record(mk_contract(2)));
        assert!(l.check_and_record(mk_contract(1)));
        assert!(l.check_and_record(mk_contract(2)));
        // Now both are drained.
        assert!(!l.check_and_record(mk_contract(1)));
        assert!(!l.check_and_record(mk_contract(2)));
    }

    #[test]
    fn responder_burst_one_then_throttles() {
        let ts = SharedMockTimeSource::new();
        let l = new_response_limiter(Arc::new(ts.clone()));
        let key = (mk_peer(1), mk_contract(1));
        assert!(l.check_and_record(key));
        assert!(!l.check_and_record(key), "responder burst is 1");
        ts.advance_time(RESPOND_REFILL_INTERVAL + Duration::from_secs(1));
        assert!(l.check_and_record(key));
    }

    #[test]
    fn responder_flood_from_one_peer_is_bounded() {
        // A not-yet-upgraded peer floods ResyncRequests for one contract; the
        // upgraded responder answers at most ~window/interval times.
        let ts = SharedMockTimeSource::new();
        let l = new_response_limiter(Arc::new(ts.clone()));
        let key = (mk_peer(1), mk_contract(1));
        for _ in 0..300 {
            l.check_and_record(key);
            ts.advance_time(Duration::from_secs(1));
        }
        // burst(1) + 300s/30s = 1 + 10 = 11.
        let allowed = l.allowed_total();
        assert!(
            (10..=12).contains(&allowed),
            "responder flood over 5 min must send ~11 responses, got {allowed}"
        );
    }

    #[test]
    fn global_responder_cap_bounds_across_many_distinct_peers() {
        // #4861: per-(peer, contract) limiting is insufficient — production saw
        // ~45 distinct requester IPs for one forked contract. The GLOBAL
        // per-contract limiter caps total responses regardless of requester
        // count. Simulate 45 distinct peers each requesting the SAME contract
        // once per second for 5 minutes; the global limiter (keyed by contract
        // only) admits at most burst + window/interval.
        let ts = SharedMockTimeSource::new();
        let l = new_response_global_limiter(Arc::new(ts.clone()));
        let contract = mk_contract(1);
        for _ in 0..300 {
            // 45 distinct peers hammer within the same second — all hit the one
            // per-contract bucket, so only the first with a token gets through.
            for p in 0..45u8 {
                let _ = p; // peer identity is irrelevant to the GLOBAL limiter
                l.check_and_record(contract);
            }
            ts.advance_time(Duration::from_secs(1));
        }
        // burst(2) + 300s / 5s = 2 + 60 = 62 admits, out of 300*45 = 13500 asks.
        let allowed = l.allowed_total();
        assert!(
            (60..=64).contains(&allowed),
            "global cap must bound one contract to ~62 responses over 5 min \
             regardless of requester count, got {allowed}"
        );
        assert!(
            allowed < 100,
            "global cap keeps a single forked contract well under the ~9,733/day \
             production storm, got {allowed} in 5 min"
        );
    }

    #[test]
    fn responder_different_peers_independent() {
        let ts = SharedMockTimeSource::new();
        let l = new_response_limiter(Arc::new(ts.clone()));
        let c = mk_contract(1);
        // Two peers requesting the same contract are independent.
        assert!(l.check_and_record((mk_peer(1), c)));
        assert!(l.check_and_record((mk_peer(2), c)));
        // Each is now throttled for its own pair.
        assert!(!l.check_and_record((mk_peer(1), c)));
        assert!(!l.check_and_record((mk_peer(2), c)));
    }

    #[test]
    fn tracked_keys_capped() {
        let ts = SharedMockTimeSource::new();
        let l = TokenBucketLimiter::new(Arc::new(ts.clone()), 1.0, RESPOND_REFILL_INTERVAL, 4);
        for i in 0..4u8 {
            assert!(l.check_and_record((mk_peer(i), mk_contract(i))));
        }
        assert_eq!(l.len(), 4);
        // 5th distinct key is dropped (capacity), suppressed.
        assert!(!l.check_and_record((mk_peer(99), mk_contract(99))));
        assert_eq!(l.len(), 4);
    }

    #[test]
    fn cleanup_removes_recovered_idle_keys() {
        let ts = SharedMockTimeSource::new();
        let l = new_emit_limiter(Arc::new(ts.clone()));
        l.check_and_record(mk_contract(1));
        l.check_and_record(mk_contract(2));
        assert_eq!(l.len(), 2);
        // Fully recover + idle past CLEANUP_AGE.
        ts.advance_time(CLEANUP_AGE + EMIT_REFILL_INTERVAL * 3);
        l.cleanup();
        assert_eq!(l.len(), 0, "recovered idle keys must be swept");
        assert_eq!(l.size.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn cleanup_preserves_recently_used_keys() {
        let ts = SharedMockTimeSource::new();
        let l = new_emit_limiter(Arc::new(ts.clone()));
        l.check_and_record(mk_contract(1));
        // Only a short time passes — not idle enough to sweep.
        ts.advance_time(Duration::from_secs(10));
        l.cleanup();
        assert_eq!(l.len(), 1, "recently-used key must be preserved");
    }
}

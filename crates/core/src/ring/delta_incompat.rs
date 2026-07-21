//! Sender-side memo of contracts whose deltas are known-doomed.
//!
//! ## Why
//!
//! Some deployed contracts cannot apply *any* delta: their `update_state`
//! accepts only `UpdateData::State` and rejects `UpdateData::Delta` outright
//! with `InvalidUpdate`. The canonical case is the HQk7 web-container (built
//! against freenet-stdlib 0.1.35): every delta the summary-diff fan-out sends
//! it is rejected, so each broadcast produces
//!
//! ```text
//! delta send → "Invalid update" → delta_apply_failed → ResyncRequest →
//! full-state ResyncResponse → (state changes) → next delta send → …
//! ```
//!
//! a self-sustaining resync loop (3,102 `delta_apply_failed` events for one
//! contract in a 2h production window). The existing defenses don't break the
//! cycle:
//!
//! - [`crate::ring::merge_backoff`] is RECEIVER-side: it stops the receiver
//!   re-running doomed merges, but the sender keeps computing and sending
//!   fresh doomed deltas.
//! - The merge backoff's failed-payload memo can't help either, because every
//!   state change produces fresh delta bytes — the memo never matches.
//! - The resync rate limiters bound the full-state amplification, not the
//!   doomed delta sends that trigger it.
//!
//! What is missing is a SENDER-side memo: "this contract can't take deltas —
//! send full state directly." That is this module.
//!
//! ## Mechanism
//!
//! Two signals arm the memo, both counted per contract:
//!
//! 1. **Resync-after-our-delta** (sender-side observation): a `ResyncRequest`
//!    arrives from a peer we sent a delta to for that contract within the
//!    attribution window ([`DELTA_ATTRIBUTION_WINDOW`]). A receiver that
//!    fails a delta apply immediately ResyncRequests the delta's sender
//!    (`operations/update/op_ctx_task.rs`), so this is the wire-visible form
//!    of the receiver's `delta_apply_failed`.
//! 2. **Local delta-apply failure** (receiver-side observation): our own WASM
//!    apply of a received delta failed with an `Invalid`-class contract
//!    rejection. We fan the same contract out to *our* downstreams, so what
//!    we just learned applies to our own delta sends too.
//!
//! [`INCOMPAT_TRIP_THRESHOLD`] consecutive failure signals (with no
//! intervening successful delta apply) arm the memo for
//! [`DELTA_INCOMPAT_TTL`]. Trip-at-1 would flip a healthy contract to
//! full-state fan-out on a single benign stale-version rejection (the same
//! rationale as `merge_backoff::INVALID_TRIP_THRESHOLD`); a delta-incapable
//! contract fails every delta from every peer, so it trips within one or two
//! broadcast rounds anyway.
//!
//! While armed, [`DeltaIncompat::suppress_deltas`] tells the broadcast path
//! (`broadcast_queue::broadcast_to_single_peer`) to skip delta computation
//! and send full state directly.
//!
//! ## Self-healing
//!
//! - A successful delta apply for the contract clears the memo AND the
//!   failure counter ([`DeltaIncompat::record_delta_success`]) — proof the
//!   contract does take deltas.
//! - Otherwise the memo expires after [`DELTA_INCOMPAT_TTL`]; the next delta
//!   either works (memo stays clear) or re-arms it.
//!
//! ## Bounded growth
//!
//! Same discipline as the sibling ring maps ([`crate::ring::merge_backoff`],
//! [`crate::ring::resync_rate_limit`]): strict size caps enforced at insert
//! (vacant-at-capacity is skipped — an attacker churning contract ids or
//! source ports cannot grow the maps; the failure mode is "no suppression /
//! no attribution", never unbounded memory), plus a periodic TTL sweep hooked
//! into the Ring reaper.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use freenet_stdlib::prelude::ContractInstanceId;
use tokio::time::Instant;

use crate::util::time_source::TimeSource;

/// How long a contract stays in full-state-only mode once the memo arms.
///
/// Long enough to kill the resync loop's steady state (~1 cycle/min per peer
/// in the HQk7 incident); short enough that a spuriously-armed healthy
/// contract pays at most 10 min of full-state fan-out before deltas are
/// retried. Matches `merge_backoff::FAILED_PAYLOAD_TTL`.
pub(crate) const DELTA_INCOMPAT_TTL: Duration = Duration::from_secs(10 * 60);

/// How long after sending a delta an incoming `ResyncRequest` from that peer
/// is attributed to the delta failing to apply. Receivers ResyncRequest the
/// sender immediately on a failed delta apply, so the true gap is round-trip
/// plus queueing; 60s matches `resync_rate_limit::OUTSTANDING_RESYNC_TTL`.
pub(crate) const DELTA_ATTRIBUTION_WINDOW: Duration = Duration::from_secs(60);

/// Consecutive failure signals (no intervening successful delta apply)
/// required to arm the memo. See module docs for the trip-at-1 rationale.
pub(crate) const INCOMPAT_TRIP_THRESHOLD: u32 = 3;

/// Hard cap on tracked contracts. At ~64 bytes/entry ≈ 0.5 MB worst case.
pub(crate) const MAX_TRACKED_CONTRACTS: usize = 8_192;

/// Hard cap on tracked `(contract, peer)` delta-send attributions.
pub(crate) const MAX_TRACKED_DELTA_SENDS: usize = 16_384;

/// Idle contract entries (no failure signal this long, memo expired) are
/// swept. Spans the TTL so an armed entry is never dropped early.
const CONTRACT_CLEANUP_AGE: Duration = Duration::from_secs(20 * 60);

struct ContractEntry {
    /// Consecutive failure signals since the last successful delta apply.
    consecutive_failures: u32,
    /// While `Some(t)` with `t > now`, deltas for this contract are
    /// suppressed in favor of full state.
    armed_until: Option<Instant>,
    /// Last failure signal, for idle cleanup.
    last_event: Instant,
}

/// Sender-side "this contract can't take deltas" memo. See module docs.
pub(crate) struct DeltaIncompat {
    contracts: DashMap<ContractInstanceId, ContractEntry>,
    contracts_size: AtomicUsize,
    /// `(contract, peer) -> when we last delivered a delta to that peer`,
    /// consulted (and consumed) when the peer ResyncRequests the contract.
    recent_delta_sends: DashMap<(ContractInstanceId, SocketAddr), Instant>,
    sends_size: AtomicUsize,
    time_source: Arc<dyn TimeSource + Send + Sync>,
    /// Total times the memo armed (telemetry/testing).
    armed_total: AtomicU64,
    /// Total delta sends suppressed in favor of full state (telemetry/testing).
    suppressed_total: AtomicU64,
}

impl DeltaIncompat {
    pub fn new(time_source: Arc<dyn TimeSource + Send + Sync>) -> Self {
        Self {
            contracts: DashMap::new(),
            contracts_size: AtomicUsize::new(0),
            recent_delta_sends: DashMap::new(),
            sends_size: AtomicUsize::new(0),
            time_source,
            armed_total: AtomicU64::new(0),
            suppressed_total: AtomicU64::new(0),
        }
    }

    /// Record that we delivered a DELTA broadcast for `contract` to `peer`.
    ///
    /// Called from the broadcast send path on a delivered delta so a
    /// subsequent `ResyncRequest` from that peer can be attributed to the
    /// delta failing to apply.
    pub fn record_delta_sent(&self, contract: ContractInstanceId, peer: SocketAddr) {
        let now = self.time_source.now();
        match self.recent_delta_sends.entry((contract, peer)) {
            dashmap::mapref::entry::Entry::Occupied(mut e) => {
                *e.get_mut() = now;
            }
            dashmap::mapref::entry::Entry::Vacant(e) => {
                // Strict cap: at capacity, skip. Failure mode is a missed
                // attribution (no arming from this peer), never map growth.
                if self.sends_size.load(Ordering::Relaxed) >= MAX_TRACKED_DELTA_SENDS {
                    return;
                }
                e.insert(now);
                self.sends_size.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// A `ResyncRequest` for `contract` arrived from `peer`. If we delivered
    /// a delta to that peer within [`DELTA_ATTRIBUTION_WINDOW`], count it as
    /// a delta-incompatibility failure signal (consuming the attribution so a
    /// replayed/duplicate request cannot double-count). Returns `true` if the
    /// signal was counted.
    pub fn note_resync_request(&self, contract: ContractInstanceId, peer: SocketAddr) -> bool {
        let now = self.time_source.now();
        let Some((_, sent_at)) = self.recent_delta_sends.remove(&(contract, peer)) else {
            return false;
        };
        self.sends_size.fetch_sub(1, Ordering::Relaxed);
        if now.saturating_duration_since(sent_at) > DELTA_ATTRIBUTION_WINDOW {
            // Stale attribution — not evidence about the delta we sent.
            return false;
        }
        self.note_failure(contract, now);
        true
    }

    /// Our own WASM apply of a RECEIVED delta for `contract` failed with an
    /// `Invalid`-class contract rejection (the receiver-side form of the same
    /// signal; see module docs).
    pub fn note_delta_apply_failed(&self, contract: ContractInstanceId) {
        let now = self.time_source.now();
        self.note_failure(contract, now);
    }

    fn note_failure(&self, contract: ContractInstanceId, now: Instant) {
        let mut armed = false;
        match self.contracts.entry(contract) {
            dashmap::mapref::entry::Entry::Occupied(mut e) => {
                let entry = e.get_mut();
                entry.consecutive_failures = entry.consecutive_failures.saturating_add(1);
                entry.last_event = now;
                if entry.consecutive_failures >= INCOMPAT_TRIP_THRESHOLD {
                    let was_armed = matches!(entry.armed_until, Some(until) if until > now);
                    entry.armed_until = Some(now + DELTA_INCOMPAT_TTL);
                    armed = !was_armed;
                }
            }
            dashmap::mapref::entry::Entry::Vacant(e) => {
                // Strict cap: at capacity, skip (no suppression for the new
                // contract, but no unbounded growth either).
                if self.contracts_size.load(Ordering::Relaxed) >= MAX_TRACKED_CONTRACTS {
                    return;
                }
                e.insert(ContractEntry {
                    consecutive_failures: 1,
                    armed_until: None,
                    last_event: now,
                });
                self.contracts_size.fetch_add(1, Ordering::Relaxed);
            }
        }
        if armed {
            self.armed_total.fetch_add(1, Ordering::Relaxed);
            tracing::info!(
                contract = %contract,
                ttl_secs = DELTA_INCOMPAT_TTL.as_secs(),
                event = "delta_incompat_armed",
                "Contract repeatedly rejects deltas — sending full state instead for the TTL"
            );
        }
    }

    /// Should the broadcast path skip delta computation and send full state
    /// for `contract`? `true` while the memo is armed and unexpired.
    pub fn suppress_deltas(&self, contract: &ContractInstanceId) -> bool {
        let now = self.time_source.now();
        let suppressed = self
            .contracts
            .get(contract)
            .is_some_and(|e| matches!(e.armed_until, Some(until) if until > now));
        if suppressed {
            self.suppressed_total.fetch_add(1, Ordering::Relaxed);
        }
        suppressed
    }

    /// A delta for `contract` applied cleanly — the contract demonstrably
    /// takes deltas, so drop the memo and the failure counter.
    pub fn record_delta_success(&self, contract: &ContractInstanceId) {
        if self.contracts.remove(contract).is_some() {
            self.contracts_size.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Periodic sweep (Ring reaper): drop stale delta-send attributions and
    /// idle, unarmed contract entries.
    pub fn cleanup(&self) {
        let now = self.time_source.now();
        let mut removed_sends = 0usize;
        self.recent_delta_sends.retain(|_, sent_at| {
            let keep = now.saturating_duration_since(*sent_at) <= DELTA_ATTRIBUTION_WINDOW;
            if !keep {
                removed_sends += 1;
            }
            keep
        });
        if removed_sends > 0 {
            self.sends_size.fetch_sub(removed_sends, Ordering::Relaxed);
        }
        let mut removed_contracts = 0usize;
        self.contracts.retain(|_, e| {
            let armed = matches!(e.armed_until, Some(until) if until > now);
            let keep = armed || now.saturating_duration_since(e.last_event) <= CONTRACT_CLEANUP_AGE;
            if !keep {
                removed_contracts += 1;
            }
            keep
        });
        if removed_contracts > 0 {
            self.contracts_size
                .fetch_sub(removed_contracts, Ordering::Relaxed);
        }
    }

    /// Total times the memo armed (telemetry/testing).
    #[allow(dead_code)]
    pub fn armed_total(&self) -> u64 {
        self.armed_total.load(Ordering::Relaxed)
    }

    /// Total delta sends suppressed (telemetry/testing).
    #[allow(dead_code)]
    pub fn suppressed_total(&self) -> u64 {
        self.suppressed_total.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::time_source::SharedMockTimeSource;

    fn contract(byte: u8) -> ContractInstanceId {
        ContractInstanceId::new([byte; 32])
    }

    fn peer(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    fn setup() -> (SharedMockTimeSource, DeltaIncompat) {
        let ts = SharedMockTimeSource::new();
        let memo = DeltaIncompat::new(Arc::new(ts.clone()));
        (ts, memo)
    }

    /// The core Bug-2 repro: repeated resync-after-delta signals arm the
    /// memo, after which delta sends are suppressed (full-state fallback).
    #[test]
    fn resync_after_delta_arms_after_threshold() {
        let (_ts, memo) = setup();
        let c = contract(1);
        for i in 0..INCOMPAT_TRIP_THRESHOLD {
            assert!(
                !memo.suppress_deltas(&c),
                "must not suppress before the threshold trips (i={i})"
            );
            memo.record_delta_sent(c, peer(4000 + i as u16));
            assert!(
                memo.note_resync_request(c, peer(4000 + i as u16)),
                "attributed resync must count as a failure signal"
            );
        }
        assert!(
            memo.suppress_deltas(&c),
            "after {INCOMPAT_TRIP_THRESHOLD} attributed resyncs the sender must \
             fall back to full-state sends"
        );
        assert_eq!(memo.armed_total(), 1);
    }

    /// A ResyncRequest with no recent delta send to that peer is NOT evidence
    /// of delta incompatibility (resyncs also fire for staleness heals).
    #[test]
    fn unattributed_resync_does_not_count() {
        let (_ts, memo) = setup();
        let c = contract(2);
        for _ in 0..10 {
            assert!(!memo.note_resync_request(c, peer(4100)));
        }
        assert!(!memo.suppress_deltas(&c));
        assert_eq!(memo.armed_total(), 0);
    }

    /// Attribution expires after the window: a late ResyncRequest (e.g. a
    /// periodic staleness heal long after our delta) does not count.
    #[test]
    fn stale_attribution_does_not_count() {
        let (ts, memo) = setup();
        let c = contract(3);
        memo.record_delta_sent(c, peer(4200));
        ts.advance_time(DELTA_ATTRIBUTION_WINDOW + Duration::from_secs(1));
        assert!(!memo.note_resync_request(c, peer(4200)));
        assert!(!memo.suppress_deltas(&c));
    }

    /// Receiver-side arm: local delta-apply failures count toward the same
    /// threshold.
    #[test]
    fn local_delta_apply_failures_arm() {
        let (_ts, memo) = setup();
        let c = contract(4);
        for _ in 0..INCOMPAT_TRIP_THRESHOLD {
            assert!(!memo.suppress_deltas(&c));
            memo.note_delta_apply_failed(c);
        }
        assert!(memo.suppress_deltas(&c));
    }

    /// Self-healing #1: a successful delta apply clears both the memo and the
    /// failure counter — the next failure starts the count from scratch.
    #[test]
    fn delta_success_clears_memo_and_counter() {
        let (_ts, memo) = setup();
        let c = contract(5);
        for _ in 0..INCOMPAT_TRIP_THRESHOLD {
            memo.note_delta_apply_failed(c);
        }
        assert!(memo.suppress_deltas(&c));

        memo.record_delta_success(&c);
        assert!(
            !memo.suppress_deltas(&c),
            "a successful delta apply must clear the memo"
        );

        // Counter reset too: threshold-1 further failures must not re-arm.
        for _ in 0..(INCOMPAT_TRIP_THRESHOLD - 1) {
            memo.note_delta_apply_failed(c);
        }
        assert!(
            !memo.suppress_deltas(&c),
            "the failure counter must restart from zero after a success"
        );
    }

    /// Self-healing #2: the memo expires after the TTL so deltas are retried.
    #[test]
    fn memo_expires_after_ttl() {
        let (ts, memo) = setup();
        let c = contract(6);
        for _ in 0..INCOMPAT_TRIP_THRESHOLD {
            memo.note_delta_apply_failed(c);
        }
        assert!(memo.suppress_deltas(&c));
        ts.advance_time(DELTA_INCOMPAT_TTL + Duration::from_secs(1));
        assert!(
            !memo.suppress_deltas(&c),
            "the memo must expire after DELTA_INCOMPAT_TTL"
        );
    }

    /// A re-failure after expiry re-arms immediately (the counter is already
    /// past the threshold — the contract is still broken).
    #[test]
    fn refailure_after_expiry_rearms() {
        let (ts, memo) = setup();
        let c = contract(7);
        for _ in 0..INCOMPAT_TRIP_THRESHOLD {
            memo.note_delta_apply_failed(c);
        }
        ts.advance_time(DELTA_INCOMPAT_TTL + Duration::from_secs(1));
        assert!(!memo.suppress_deltas(&c));
        memo.note_delta_apply_failed(c);
        assert!(
            memo.suppress_deltas(&c),
            "a failure after expiry must re-arm without re-counting from zero"
        );
        assert_eq!(memo.armed_total(), 2);
    }

    /// Strict size caps: neither map grows past its ceiling under key churn.
    #[test]
    fn maps_are_capacity_capped() {
        let (_ts, memo) = setup();
        for i in 0..(MAX_TRACKED_DELTA_SENDS + 100) {
            let mut id = [0u8; 32];
            id[..8].copy_from_slice(&(i as u64).to_be_bytes());
            memo.record_delta_sent(ContractInstanceId::new(id), peer(5000));
        }
        assert!(memo.recent_delta_sends.len() <= MAX_TRACKED_DELTA_SENDS);

        for i in 0..(MAX_TRACKED_CONTRACTS + 100) {
            let mut id = [0u8; 32];
            id[..8].copy_from_slice(&(i as u64).to_be_bytes());
            memo.note_delta_apply_failed(ContractInstanceId::new(id));
        }
        assert!(memo.contracts.len() <= MAX_TRACKED_CONTRACTS);
    }

    /// Cleanup sweeps stale attributions and idle unarmed entries but never
    /// an armed one.
    #[test]
    fn cleanup_sweeps_stale_but_keeps_armed() {
        let (ts, memo) = setup();
        let armed = contract(8);
        for _ in 0..INCOMPAT_TRIP_THRESHOLD {
            memo.note_delta_apply_failed(armed);
        }
        let idle = contract(9);
        memo.note_delta_apply_failed(idle);
        memo.record_delta_sent(contract(10), peer(6000));

        ts.advance_time(DELTA_ATTRIBUTION_WINDOW + Duration::from_secs(1));
        memo.cleanup();
        assert!(
            memo.recent_delta_sends.is_empty(),
            "stale delta-send attributions must be swept"
        );
        assert!(memo.suppress_deltas(&armed), "armed entry must survive");

        ts.advance_time(CONTRACT_CLEANUP_AGE);
        memo.cleanup();
        assert!(
            !memo.contracts.contains_key(&idle),
            "idle unarmed entry must be swept"
        );
    }
}

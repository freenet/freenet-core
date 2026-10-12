//! Stranded-holder recovery: the receiving-side gate and rate limit for the
//! backward bootstrap of `docs/design/demand-driven-hosting.md` §5d(b) (#5846).
//!
//! ## The situation
//!
//! A peer holds a contract and has real demand for it (a local client or a
//! downstream subscriber), but the peers between it and the contract's key do
//! not host it, typically because the near-key hosts churned out. Its subscribe
//! routes toward the key and finds nothing: every peer on the way lacks the
//! contract, and the holder, as the requester, is excluded from the search.
//! Without this recovery the holder stays a host nobody near the key knows
//! about, and its renewal fails every cycle.
//!
//! ## The recovery
//!
//! The FIRST hop of that subscribe, once the search beyond it comes back empty,
//! pulls the state back from the requester (a directed GET) and becomes a real
//! host: the requester becomes its downstream, it takes its own lease and
//! announces. It now has demand of its own, so it re-roots toward the key the
//! same way, and the chain toward the key re-forms one hop at a time until it
//! reaches the root or joins an existing mesh.
//!
//! ## Why this is not the placement-push anti-pattern
//!
//! do NOT turn this into a push — see `.claude/rules/hosting-invariants.md`
//! (anti-pattern "holding-driven placement push", invariant 4 "self-healing
//! re-root is not a state-carrying PUT"). Every condition below is what keeps
//! it on the right side of that line:
//!
//! - **Demand-gated.** It runs only inside a SUBSCRIBE the requester sent, and
//!   a holder sends one only for real demand (a client subscribe, or a renewal
//!   that passed the `contract_in_use` gate). Holding a copy triggers nothing.
//! - **Pull, not push.** The state moves because the peer that will host it
//!   asked for it, over the ordinary GET path. The holder never sends state
//!   unasked and never routes a PUT toward the key.
//! - **Claimed holding.** The requester must be a connected peer that
//!   advertises hosting the contract. That advertisement is the requester's
//!   own claim and is not verified, so a peer can make us fetch a contract it
//!   chooses and, through the re-root, place it along the way to the key, each
//!   copy with one downstream. That is what the same peer can already do with
//!   a PUT followed by a SUBSCRIBE; the rate limits below and the eviction
//!   budget the copies compete in bound it.
//! - **Keyward only.** This peer must be strictly closer to the key than the
//!   requester, so the new upstream relation is a strict descent toward the key
//!   (acyclic, design §4) and the requester's computed upstream becomes us.
//! - **Single-target.** One request makes at most one new host: the peer that
//!   received it from the holder. Deeper hops see a requester that does not
//!   advertise the contract and answer NotFound as before. (A holder that
//!   retries a refused or timed-out subscribe at another neighbour sends a new
//!   request; each is bounded the same way.)
//! - **Rate-limited on the receiving side** ([`StrandedRecoveryLimiter`]):
//!   token buckets bound how many recoveries start; at most
//!   [`MAX_IN_FLIGHT_FETCHES`] fetches run at once, one per requester and one
//!   per contract, each cancelled at [`FETCH_DEADLINE`]. That bounds the
//!   fetches this node waits on and assembles, not the bytes on the wire: a
//!   cancelled fetch does not stop its sender. So a pair whose fetch failed,
//!   for any reason, is not fetched again for [`FAILED_FETCH_BACKOFF`], which
//!   is what stops a holder re-uploading a state this node cannot take.
//!   Neither one requester nor many stranded holders at once can turn it into
//!   a fetch storm (#4630, the 0.2.87 migration storm).
//!
//! ## Accepted residual: a NotFound is not proof of absence
//!
//! The wire answer is `Subscribed` or `NotFound`, nothing finer. A `NotFound`
//! from deeper in the search can mean a deeper hop ran out of HTL, timed out
//! or was refused, not that no host exists. So a recovery can create a host
//! while a real one exists past a cut-short search: two mesh components for a
//! while. That is the case design §5d(a) already accepts ("no closer peers" is
//! a local statement): the components merge through the contract's
//! commutative merge once they connect, and the recovered copy is the holder's
//! current state, not a stale one. Only a timeout at the FIRST hop is
//! excluded, because that one this peer can see.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use freenet_stdlib::prelude::ContractInstanceId;

use super::Location;
use super::resync_rate_limit::{MAX_TRACKED_KEYS, TokenBucketLimiter};
use crate::util::time_source::TimeSource;

/// Node-wide recoveries allowed in a burst.
pub(crate) const GLOBAL_BURST: f64 = 10.0;
/// Node-wide steady-state rate: one recovery per this interval (20 a minute).
/// Matches the outbound side, where a node starts at most
/// `MAX_RECOVERY_ATTEMPTS_PER_INTERVAL` (10) renewals per 30 s cycle.
pub(crate) const GLOBAL_REFILL_INTERVAL: Duration = Duration::from_secs(3);
/// Recoveries one requester may trigger here in a burst. A member's node with a
/// handful of stranded rooms recovers them in one burst.
pub(crate) const PER_REQUESTER_BURST: f64 = 4.0;
/// Steady-state rate per requester.
pub(crate) const PER_REQUESTER_REFILL_INTERVAL: Duration = Duration::from_secs(15);
/// At most one recovery attempt per contract per renewal cycle. After a
/// success the contract is local, so further subscribes are local hits and
/// never reach this gate; this only bounds repeated failed attempts.
pub(crate) const PER_CONTRACT_REFILL_INTERVAL: Duration = Duration::from_secs(30);

/// Recovery fetches one node runs at once. The token buckets bound how many
/// recoveries start; this bounds how many it waits on and assembles at once,
/// since a recovery pulls a whole contract state (up to `MAX_STATE_SIZE`). With two at a time a slow or
/// large fetch paces further recoveries instead of piling them up. A slot is
/// held by the fetch task itself, for at most [`FETCH_DEADLINE`], so it cannot
/// be freed while the fetch is still running. It bounds fetches waited on and
/// assembled here, not bytes still arriving after a cancel; see
/// [`FAILED_FETCH_BACKOFF`].
pub(crate) const MAX_IN_FLIGHT_FETCHES: usize = 2;

/// Longest a recovery fetch may run, streaming included; it is cancelled at
/// this point. Long enough for a large state over a slow uplink, short enough
/// that a peer trickling bytes cannot hold a fetch slot for longer.
pub(crate) const FETCH_DEADLINE: Duration = Duration::from_secs(120);

/// How long a (requester, contract) pair is not fetched again after a fetch of
/// it failed: it hit [`FETCH_DEADLINE`], or the state arrived and was not
/// stored, or the requester did not serve it. Retrying sooner would only make
/// the holder upload the same state again for the same result.
pub(crate) const FAILED_FETCH_BACKOFF: Duration = Duration::from_secs(600);

/// Why [`StrandedRecoveryLimiter::try_admit`] refused a recovery. Each cause is
/// counted separately: the remedies differ.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Refusal {
    /// This very (requester, contract) fetch is already running here: the
    /// requester gave up on an earlier attempt and retried. Answered NotFound
    /// like the other refusals; the running fetch still forms the host, which
    /// the holder's next renewal finds as its computed upstream.
    AlreadyFetching,
    /// A fetch of this contract from this requester failed within
    /// [`FAILED_FETCH_BACKOFF`].
    RecentlyFailed,
    /// All [`MAX_IN_FLIGHT_FETCHES`] fetch slots are taken.
    SlotsBusy,
    /// This requester already has a fetch in flight here. One per requester,
    /// so a single peer cannot hold every slot.
    RequesterBusy,
    /// This contract is already being fetched here, so a second fetch of the
    /// same state would only duplicate the upload.
    ContractBusy,
    /// A token bucket (per requester, per contract or node-wide) is empty.
    RateLimited,
}

/// Why a terminus did not attempt the backward bootstrap.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SkipReason {
    /// The requester is not a connected ring peer (e.g. our own client's
    /// subscribe, which is driven here with our own address).
    RequesterUnknown,
    /// The requester does not advertise hosting this contract.
    RequesterNotAdvertising,
    /// Our own location, or the requester's, is unknown.
    LocationUnknown,
    /// We are not strictly closer to the key than the requester.
    NotKeyward,
    /// The contract is banned on this node.
    Banned,
}

/// Inputs to [`decide`], read from live state by the caller.
pub(crate) struct RecoveryInputs {
    /// Our ring location.
    pub own_location: Option<Location>,
    /// The requester's ring location, `None` if the requester is not a
    /// connected ring peer.
    pub requester_location: Option<Option<Location>>,
    /// Whether the requester advertises hosting the contract
    /// (`NeighborHostingManager::neighbors_with_contract_id`).
    pub requester_advertises: bool,
    /// Whether the contract is banned on this node.
    pub banned: bool,
    /// The contract's ring location.
    pub contract_location: Location,
}

/// The pure gate: may this terminus pull the contract back from its requester?
/// Rate limiting is separate ([`StrandedRecoveryLimiter::admit`]) so this stays
/// deterministic and cheap to test.
pub(crate) fn decide(inputs: &RecoveryInputs) -> Result<(), SkipReason> {
    if inputs.banned {
        return Err(SkipReason::Banned);
    }
    let Some(requester_location) = inputs.requester_location else {
        return Err(SkipReason::RequesterUnknown);
    };
    if !inputs.requester_advertises {
        return Err(SkipReason::RequesterNotAdvertising);
    }
    let (Some(own), Some(requester)) = (inputs.own_location, requester_location) else {
        return Err(SkipReason::LocationUnknown);
    };
    // Strict `<`: equidistant peers are not keyward of each other, the same
    // tie rule as the computed upstream (design §6, "ties fail safe").
    if own.distance(inputs.contract_location) < requester.distance(inputs.contract_location) {
        Ok(())
    } else {
        Err(SkipReason::NotKeyward)
    }
}

/// Receiving-side rate limit for the backward bootstrap.
///
/// Three token buckets: per requester, then per contract, then node-wide.
/// The requester bucket goes first because its keys are bounded by our
/// connection count, so a requester cycling through contract ids exhausts its
/// own bucket before it can fill the per-contract map or draw on the node-wide
/// bucket.
pub(crate) struct StrandedRecoveryLimiter {
    per_requester: TokenBucketLimiter<SocketAddr>,
    per_contract: TokenBucketLimiter<ContractInstanceId>,
    global: TokenBucketLimiter<()>,
    /// The fetches in flight, at most [`MAX_IN_FLIGHT_FETCHES`] entries.
    in_flight: Arc<parking_lot::Mutex<Vec<(SocketAddr, ContractInstanceId)>>>,
    /// Pairs whose fetch failed, with when they may be tried again.
    /// Entries only expire (nothing refreshes them), so refusing new ones at
    /// the cap cannot starve anyone for longer than [`FAILED_FETCH_BACKOFF`].
    failed: dashmap::DashMap<(SocketAddr, ContractInstanceId), tokio::time::Instant>,
    time_source: Arc<dyn TimeSource + Send + Sync>,
}

/// A held recovery-fetch slot; released on drop, on every exit path. Moved
/// into the fetch task so it lives exactly as long as the fetch.
pub(crate) struct InFlightFetch {
    in_flight: Arc<parking_lot::Mutex<Vec<(SocketAddr, ContractInstanceId)>>>,
    entry: (SocketAddr, ContractInstanceId),
}

impl Drop for InFlightFetch {
    fn drop(&mut self) {
        let mut in_flight = self.in_flight.lock();
        if let Some(i) = in_flight.iter().position(|e| *e == self.entry) {
            in_flight.swap_remove(i);
        }
    }
}

impl StrandedRecoveryLimiter {
    pub(crate) fn new(time_source: Arc<dyn TimeSource + Send + Sync>) -> Self {
        Self {
            per_requester: TokenBucketLimiter::new(
                time_source.clone(),
                PER_REQUESTER_BURST,
                PER_REQUESTER_REFILL_INTERVAL,
                MAX_TRACKED_KEYS,
            ),
            per_contract: TokenBucketLimiter::new(
                time_source.clone(),
                1.0,
                PER_CONTRACT_REFILL_INTERVAL,
                MAX_TRACKED_KEYS,
            ),
            global: TokenBucketLimiter::new(
                time_source.clone(),
                GLOBAL_BURST,
                GLOBAL_REFILL_INTERVAL,
                1,
            ),
            in_flight: Arc::new(parking_lot::Mutex::new(Vec::new())),
            failed: dashmap::DashMap::new(),
            time_source,
        }
    }

    /// Remember that the fetch of `contract` from `requester` failed, so it is
    /// not tried again for [`FAILED_FETCH_BACKOFF`].
    pub(crate) fn record_failed_fetch(&self, requester: SocketAddr, contract: ContractInstanceId) {
        if self.failed.len() < MAX_TRACKED_KEYS {
            self.failed.insert(
                (requester, contract),
                self.time_source.now() + FAILED_FETCH_BACKOFF,
            );
        }
    }

    /// Admit one recovery for `(requester, contract)`, returning the fetch slot
    /// to hold for the fetch's lifetime. A refusal means skip the recovery and
    /// answer NotFound as before; the holder retries on its next renewal.
    ///
    /// The slot is checked first and the token buckets only if one is free, so
    /// a request refused for lack of a slot spends no tokens. Within the
    /// buckets a refusal can spend tokens in the narrower ones it passed, which
    /// only ever makes recovery rarer, never more frequent.
    pub(crate) fn try_admit(
        &self,
        requester: SocketAddr,
        contract: ContractInstanceId,
    ) -> Result<InFlightFetch, Refusal> {
        // Hold the lock across the bucket checks so two requests cannot both
        // pass the slot checks and then both insert.
        let mut in_flight = self.in_flight.lock();
        if in_flight.contains(&(requester, contract)) {
            return Err(Refusal::AlreadyFetching);
        }
        if self
            .failed
            .get(&(requester, contract))
            .is_some_and(|until| self.time_source.now() < *until)
        {
            return Err(Refusal::RecentlyFailed);
        }
        if in_flight.len() >= MAX_IN_FLIGHT_FETCHES {
            return Err(Refusal::SlotsBusy);
        }
        if in_flight.iter().any(|(r, _)| *r == requester) {
            return Err(Refusal::RequesterBusy);
        }
        if in_flight.iter().any(|(_, c)| *c == contract) {
            return Err(Refusal::ContractBusy);
        }
        let admitted = self.per_requester.check_and_record(requester)
            && self.per_contract.check_and_record(contract)
            && self.global.check_and_record(());
        if !admitted {
            return Err(Refusal::RateLimited);
        }
        in_flight.push((requester, contract));
        Ok(InFlightFetch {
            in_flight: self.in_flight.clone(),
            entry: (requester, contract),
        })
    }

    /// Drop idle, fully-refilled buckets. Called from the Ring reaper.
    pub(crate) fn cleanup(&self) {
        self.per_requester.cleanup();
        self.per_contract.cleanup();
        self.global.cleanup();
        let now = self.time_source.now();
        self.failed.retain(|_, until| now < *until);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::time_source::SharedMockTimeSource;

    fn inputs(own: f64, requester: f64, key: f64) -> RecoveryInputs {
        RecoveryInputs {
            own_location: Some(Location::new(own)),
            requester_location: Some(Some(Location::new(requester))),
            requester_advertises: true,
            banned: false,
            contract_location: Location::new(key),
        }
    }

    #[test]
    fn keyward_advertising_requester_is_recovered() {
        assert_eq!(decide(&inputs(0.51, 0.90, 0.50)), Ok(()));
    }

    #[test]
    fn requester_closer_to_the_key_is_not_recovered() {
        // We are farther from the key than the holder: hosting here would point
        // the holder's upstream away from the key.
        assert_eq!(
            decide(&inputs(0.90, 0.51, 0.50)),
            Err(SkipReason::NotKeyward)
        );
    }

    #[test]
    fn equidistant_requester_is_not_recovered() {
        assert_eq!(
            decide(&inputs(0.40, 0.60, 0.50)),
            Err(SkipReason::NotKeyward)
        );
    }

    #[test]
    fn requester_that_does_not_advertise_is_not_recovered() {
        // A deeper hop of the same subscribe sees the previous hop as its
        // requester, which does not hold the contract: no recovery there, so a
        // request makes at most one new host.
        let mut i = inputs(0.51, 0.90, 0.50);
        i.requester_advertises = false;
        assert_eq!(decide(&i), Err(SkipReason::RequesterNotAdvertising));
    }

    #[test]
    fn unknown_requester_is_not_recovered() {
        let mut i = inputs(0.51, 0.90, 0.50);
        i.requester_location = None;
        assert_eq!(decide(&i), Err(SkipReason::RequesterUnknown));
    }

    #[test]
    fn unknown_locations_are_not_recovered() {
        let mut i = inputs(0.51, 0.90, 0.50);
        i.own_location = None;
        assert_eq!(decide(&i), Err(SkipReason::LocationUnknown));
        let mut i = inputs(0.51, 0.90, 0.50);
        i.requester_location = Some(None);
        assert_eq!(decide(&i), Err(SkipReason::LocationUnknown));
    }

    #[test]
    fn banned_contract_is_not_recovered() {
        let mut i = inputs(0.51, 0.90, 0.50);
        i.banned = true;
        assert_eq!(decide(&i), Err(SkipReason::Banned));
    }

    fn addr(n: u8) -> SocketAddr {
        SocketAddr::from(([10, 0, 0, n], 1000))
    }

    fn contract(n: u8) -> ContractInstanceId {
        ContractInstanceId::new([n; 32])
    }

    /// Admit and immediately end the fetch: exercises the token buckets alone.
    fn admit(l: &StrandedRecoveryLimiter, r: SocketAddr, c: ContractInstanceId) -> bool {
        l.try_admit(r, c).is_ok()
    }

    fn limiter() -> (StrandedRecoveryLimiter, SharedMockTimeSource) {
        let clock = SharedMockTimeSource::new();
        (StrandedRecoveryLimiter::new(Arc::new(clock.clone())), clock)
    }

    #[test]
    fn one_attempt_per_contract_per_cycle() {
        let (l, clock) = limiter();
        assert!(admit(&l, addr(1), contract(1)));
        // A second holder of the same contract inside the cycle is refused.
        assert!(!admit(&l, addr(2), contract(1)));
        clock.advance_time(PER_CONTRACT_REFILL_INTERVAL);
        assert!(admit(&l, addr(2), contract(1)));
    }

    #[test]
    fn one_requester_is_capped_at_its_burst() {
        let (l, clock) = limiter();
        let burst = PER_REQUESTER_BURST as u8;
        for c in 0..burst {
            assert!(
                admit(&l, addr(1), contract(c)),
                "attempt {c} within the burst"
            );
        }
        assert!(
            !admit(&l, addr(1), contract(burst)),
            "one requester cannot exceed its burst"
        );
        // Another requester is unaffected.
        assert!(admit(&l, addr(2), contract(burst + 1)));
        clock.advance_time(PER_REQUESTER_REFILL_INTERVAL);
        assert!(admit(&l, addr(1), contract(burst + 2)));
    }

    #[test]
    fn many_stranded_holders_are_capped_node_wide() {
        // Many distinct holders, each with a distinct contract: only the
        // node-wide bucket can bound this.
        let (l, clock) = limiter();
        let admitted = (0..100u8)
            .filter(|n| admit(&l, addr(*n), contract(*n)))
            .count();
        assert_eq!(admitted, GLOBAL_BURST as usize);
        // Steady state: one more per refill interval, however many ask.
        clock.advance_time(GLOBAL_REFILL_INTERVAL);
        let admitted = (100..200u8)
            .filter(|n| admit(&l, addr(*n), contract(*n)))
            .count();
        assert_eq!(admitted, 1);
    }

    #[test]
    fn fetch_slots_are_capped_and_released_on_drop() {
        let (l, _clock) = limiter();
        let held: Vec<_> = (0..MAX_IN_FLIGHT_FETCHES as u8)
            .map(|n| l.try_admit(addr(n), contract(n)).expect("a free slot"))
            .collect();
        assert_eq!(
            l.try_admit(addr(50), contract(50)).err(),
            Some(Refusal::SlotsBusy),
            "no more than MAX_IN_FLIGHT_FETCHES at once"
        );
        drop(held);
        assert!(
            l.try_admit(addr(50), contract(50)).is_ok(),
            "slots come back when fetches end"
        );
    }

    #[test]
    fn one_fetch_per_requester_and_per_contract() {
        let (l, _clock) = limiter();
        let _held = l.try_admit(addr(1), contract(1)).expect("free");
        assert_eq!(
            l.try_admit(addr(1), contract(2)).err(),
            Some(Refusal::RequesterBusy),
            "one peer cannot hold every slot"
        );
        assert_eq!(
            l.try_admit(addr(2), contract(1)).err(),
            Some(Refusal::ContractBusy),
            "a contract already being fetched is not fetched twice"
        );
    }

    #[test]
    fn a_retry_of_the_same_fetch_is_told_it_is_already_running() {
        let (l, _clock) = limiter();
        let _held = l.try_admit(addr(1), contract(1)).expect("free");
        assert_eq!(
            l.try_admit(addr(1), contract(1)).err(),
            Some(Refusal::AlreadyFetching)
        );
    }

    #[test]
    fn a_failed_fetch_is_not_retried_for_the_backoff() {
        let (l, clock) = limiter();
        l.record_failed_fetch(addr(1), contract(1));
        assert_eq!(
            l.try_admit(addr(1), contract(1)).err(),
            Some(Refusal::RecentlyFailed)
        );
        // Other pairs are unaffected.
        assert!(admit(&l, addr(1), contract(2)));
        assert!(admit(&l, addr(2), contract(1)));
        clock.advance_time(FAILED_FETCH_BACKOFF);
        l.cleanup();
        assert!(l.failed.is_empty(), "expired entries are swept");
        assert!(admit(&l, addr(1), contract(1)), "retried after the backoff");
    }

    #[test]
    fn a_busy_slot_refusal_spends_no_tokens() {
        let (l, _clock) = limiter();
        let _held: Vec<_> = (0..MAX_IN_FLIGHT_FETCHES as u8)
            .map(|n| l.try_admit(addr(n), contract(n)).expect("a free slot"))
            .collect();
        // Many refused attempts from one requester while the slots are busy...
        for c in 100..150u8 {
            assert_eq!(
                l.try_admit(addr(9), contract(c)).err(),
                Some(Refusal::SlotsBusy)
            );
        }
        drop(_held);
        // ...leave its whole burst intact.
        for c in 0..PER_REQUESTER_BURST as u8 {
            assert!(
                admit(&l, addr(9), contract(200 + c)),
                "token {c} still there"
            );
        }
    }

    #[test]
    fn cleanup_drops_idle_buckets_and_keeps_behaviour() {
        let (l, clock) = limiter();
        assert!(admit(&l, addr(1), contract(1)));
        clock.advance_time(Duration::from_secs(600));
        l.cleanup();
        assert_eq!(l.per_requester.len(), 0);
        assert_eq!(l.per_contract.len(), 0);
        assert!(admit(&l, addr(1), contract(1)), "a swept key starts fresh");
    }

    #[test]
    fn requester_refused_by_its_own_bucket_does_not_drain_the_global_one() {
        let (l, _clock) = limiter();
        let burst = PER_REQUESTER_BURST as u8;
        // One requester tries far more than its burst.
        for c in 0..50u8 {
            admit(&l, addr(1), contract(c));
        }
        // It used only `burst` node-wide tokens, so other requesters still get
        // the rest of the node-wide burst.
        let others = (100..200u8)
            .filter(|n| admit(&l, addr(*n), contract(*n)))
            .count();
        assert_eq!(others, GLOBAL_BURST as usize - burst as usize);
    }
}

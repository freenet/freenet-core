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
//! - **Proof of holding.** The requester must be a connected peer that
//!   advertises hosting the contract. A peer cannot be talked into fetching
//!   from someone that does not claim to have it.
//! - **Keyward only.** This peer must be strictly closer to the key than the
//!   requester, so the new upstream relation is a strict descent toward the key
//!   (acyclic, design §4) and the requester's computed upstream becomes us.
//! - **Single-target.** One request makes at most one new host: the peer that
//!   received it from the holder. Deeper hops see a requester that does not
//!   advertise the contract and answer NotFound as before.
//! - **Rate-limited on the receiving side** ([`StrandedRecoveryLimiter`]), so
//!   neither one requester nor many stranded holders at once can turn it into
//!   a fetch storm (#4630, the 0.2.87 migration storm).

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
            global: TokenBucketLimiter::new(time_source, GLOBAL_BURST, GLOBAL_REFILL_INTERVAL, 1),
        }
    }

    /// Consume one attempt for `(requester, contract)`. `false` means skip the
    /// recovery and answer NotFound as before; the holder retries on its next
    /// renewal. A refusal can spend tokens in the narrower buckets it passed,
    /// which only ever makes recovery rarer, never more frequent.
    pub(crate) fn admit(&self, requester: SocketAddr, contract: ContractInstanceId) -> bool {
        self.per_requester.check_and_record(requester)
            && self.per_contract.check_and_record(contract)
            && self.global.check_and_record(())
    }

    /// Drop idle, fully-refilled buckets. Called from the Ring reaper.
    pub(crate) fn cleanup(&self) {
        self.per_requester.cleanup();
        self.per_contract.cleanup();
        self.global.cleanup();
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

    fn limiter() -> (StrandedRecoveryLimiter, SharedMockTimeSource) {
        let clock = SharedMockTimeSource::new();
        (StrandedRecoveryLimiter::new(Arc::new(clock.clone())), clock)
    }

    #[test]
    fn one_attempt_per_contract_per_cycle() {
        let (l, clock) = limiter();
        assert!(l.admit(addr(1), contract(1)));
        // A second holder of the same contract inside the cycle is refused.
        assert!(!l.admit(addr(2), contract(1)));
        clock.advance_time(PER_CONTRACT_REFILL_INTERVAL);
        assert!(l.admit(addr(2), contract(1)));
    }

    #[test]
    fn one_requester_is_capped_at_its_burst() {
        let (l, clock) = limiter();
        let burst = PER_REQUESTER_BURST as u8;
        for c in 0..burst {
            assert!(
                l.admit(addr(1), contract(c)),
                "attempt {c} within the burst"
            );
        }
        assert!(
            !l.admit(addr(1), contract(burst)),
            "one requester cannot exceed its burst"
        );
        // Another requester is unaffected.
        assert!(l.admit(addr(2), contract(burst + 1)));
        clock.advance_time(PER_REQUESTER_REFILL_INTERVAL);
        assert!(l.admit(addr(1), contract(burst + 2)));
    }

    #[test]
    fn many_stranded_holders_are_capped_node_wide() {
        // Many distinct holders, each with a distinct contract: only the
        // node-wide bucket can bound this.
        let (l, clock) = limiter();
        let admitted = (0..100u8)
            .filter(|n| l.admit(addr(*n), contract(*n)))
            .count();
        assert_eq!(admitted, GLOBAL_BURST as usize);
        // Steady state: one more per refill interval, however many ask.
        clock.advance_time(GLOBAL_REFILL_INTERVAL);
        let admitted = (100..200u8)
            .filter(|n| l.admit(addr(*n), contract(*n)))
            .count();
        assert_eq!(admitted, 1);
    }

    #[test]
    fn requester_refused_by_its_own_bucket_does_not_drain_the_global_one() {
        let (l, _clock) = limiter();
        let burst = PER_REQUESTER_BURST as u8;
        // One requester tries far more than its burst.
        for c in 0..50u8 {
            l.admit(addr(1), contract(c));
        }
        // It used only `burst` node-wide tokens, so other requesters still get
        // the rest of the node-wide burst.
        let others = (100..200u8)
            .filter(|n| l.admit(addr(*n), contract(*n)))
            .count();
        assert_eq!(others, GLOBAL_BURST as usize - burst as usize);
    }
}

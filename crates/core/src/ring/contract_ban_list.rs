//! Per-contract ban list — Phase 7 of the contract-hardening plan.
//!
//! When `GovernanceManager` transitions a contract into the `Banned`
//! state (repeat offender — re-flagged within the ban_window after a
//! prior eviction), the contract id is pushed into this list along
//! with a TTL. While in the list, all inbound wire-protocol REQUEST
//! messages for that contract are dropped at the receive boundary.
//!
//! ## What this rejects
//!
//! - `PutMsg::Request` / `PutMsg::RequestStreaming` — refuse to store.
//! - `GetMsg::Request` / `GetMsg::RequestStreaming` — refuse to serve.
//! - `UpdateMsg::*` — refuse to apply or fan out.
//! - `SubscribeMsg::Request` — refuse to register interest.
//!
//! ## What this does NOT reject
//!
//! - **Responses to our own outbound requests.** If we sent a request
//!   before the ban kicked in, allowing the reply completes that
//!   transaction cleanly. The next request we make for the contract
//!   would be self-blocked at the egress (Phase 7 follow-up); for now
//!   we focus on the receive side.
//! - **Peer-level messages** (ConnectMsg, etc.) — those are the peer
//!   layer's concern, not the contract layer.
//!
//! ## Two ways entries get added
//!
//! 1. **Automatic** (this PR): `GovernanceManager` transitions a
//!    contract into `Banned`. The reaper loop sees the decision and
//!    pushes the id to this list with the configured `ban_ttl`. When
//!    the matching `BanLifted` transition fires, the entry is removed.
//!
//! 2. **Operator CLI** (issue #4274, follow-up): operator runs
//!    `freenet contract ban <key>` or sets a config flag. Hooks into
//!    the same list via [`ContractBanList::ban`].
//!
//! ## Cleanup
//!
//! The `Banned` state has its own TTL (`ban_ttl`, default 1 hour) and
//! `GovernanceManager` emits a `BanLifted` decision when the TTL
//! expires. That decision drives [`ContractBanList::unban`]. So the
//! list shouldn't accumulate stale entries under normal operation.
//!
//! As a defense-in-depth, [`ContractBanList::cleanup`] sweeps any
//! entries whose `expires_at` has passed but for some reason weren't
//! removed by an explicit `unban` — e.g. if the GovernanceManager
//! state was reset without flushing the list.

use std::sync::Arc;

use dashmap::DashMap;
use freenet_stdlib::prelude::ContractInstanceId;
use tokio::time::Instant;

use crate::util::time_source::TimeSource;

/// Why a contract was added to the ban list. Surface on the dashboard
/// so operators can distinguish automatic (governance-driven) bans
/// from manual (operator-CLI) bans.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BanReason {
    /// Auto-flipped by `GovernanceManager` after `BanTriggered`
    /// (repeat-eviction within the ban_window).
    AutoMad,
    /// Operator-driven via the CLI / config flag (#4274). Reserved
    /// for the follow-up PR that wires the operator-facing surface.
    #[allow(dead_code)]
    Operator,
}

#[derive(Clone, Copy, Debug)]
struct BanEntry {
    /// Wall-clock-equivalent moment when this ban automatically
    /// lifts. Computed at insertion time as `now + ban_ttl`; the
    /// `GovernanceManager` does the same arithmetic when it emits the
    /// matching `BanLifted` decision, so the two converge.
    expires_at: Instant,
    reason: BanReason,
}

/// Per-(contract instance) ban list. Read by the wire-message
/// dispatch site to drop inbound requests; written by the governance
/// reaper loop on `BanTriggered` / `BanLifted` transitions.
pub(crate) struct ContractBanList {
    entries: DashMap<ContractInstanceId, BanEntry>,
    time_source: Arc<dyn TimeSource + Send + Sync>,
}

impl ContractBanList {
    pub fn new(time_source: Arc<dyn TimeSource + Send + Sync>) -> Self {
        Self {
            entries: DashMap::new(),
            time_source,
        }
    }

    /// Fast-path predicate used at every inbound wire dispatch arm
    /// carrying a `ContractKey`. Returns true if the contract is
    /// banned right now — caller drops the message.
    ///
    /// The expiry check here is a safety net: under normal operation
    /// the explicit `unban()` call from the reaper's `BanLifted`
    /// decision removes the entry before we ever look at the
    /// timestamp. But if the reaper hasn't ticked since the TTL
    /// elapsed (e.g. just after a long-paused interval in a
    /// simulation), this returns false anyway and the entry will be
    /// cleared on the next `cleanup()`.
    pub fn is_banned(&self, contract: &ContractInstanceId) -> bool {
        match self.entries.get(contract) {
            None => false,
            Some(entry) => {
                let now = self.time_source.now();
                now < entry.expires_at
            }
        }
    }

    /// Add a contract to the ban list with the given expiry instant
    /// and reason. Idempotent: re-banning extends the expiry to
    /// whichever value is later (the new one — typically the reaper
    /// is calling this because a fresh `BanTriggered` just fired).
    pub fn ban(&self, contract: ContractInstanceId, expires_at: Instant, reason: BanReason) {
        use dashmap::mapref::entry::Entry;
        match self.entries.entry(contract) {
            Entry::Occupied(mut e) => {
                let cur = e.get_mut();
                // Keep whichever expiry is later — re-bans should
                // never shorten an existing ban window.
                if expires_at > cur.expires_at {
                    cur.expires_at = expires_at;
                }
                // Reason: operator wins over auto, since explicit
                // operator action should not be overwritten by a
                // later auto-flip.
                if matches!(reason, BanReason::Operator) {
                    cur.reason = BanReason::Operator;
                }
            }
            Entry::Vacant(e) => {
                e.insert(BanEntry { expires_at, reason });
            }
        }
    }

    /// Remove a contract from the ban list, regardless of expiry.
    /// Called from the reaper loop when a `BanLifted` decision fires.
    pub fn unban(&self, contract: &ContractInstanceId) {
        self.entries.remove(contract);
    }

    /// Drop entries whose `expires_at` has passed. Called periodically
    /// from the governance reaper tick as a defense-in-depth in case
    /// an explicit `unban` was skipped (process restart, race during
    /// configuration reload, etc).
    pub fn cleanup(&self) {
        let now = self.time_source.now();
        self.entries.retain(|_, entry| entry.expires_at > now);
    }

    /// Number of currently-banned contracts. Used by tests and the
    /// dashboard's governance card for "N contracts on ban list."
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::governance::{GovernanceState, ReaperDecision, TransitionReason};
    use crate::ring::Ring;
    use crate::util::time_source::SharedMockTimeSource;
    use std::time::Duration;

    fn mk_contract(byte: u8) -> ContractInstanceId {
        ContractInstanceId::new([byte; 32])
    }

    fn mk_decision(
        contract: ContractInstanceId,
        reason: TransitionReason,
        at: Instant,
    ) -> ReaperDecision {
        // The from/to states aren't load-bearing for the
        // `apply_ban_decisions` wiring — only `reason` is — but
        // populate them with a plausible pair so the value reads
        // naturally in test failure output.
        let (from, to) = match reason {
            TransitionReason::BanTriggered => (GovernanceState::Evicted, GovernanceState::Banned),
            TransitionReason::BanLifted => (GovernanceState::Banned, GovernanceState::Normal),
            _ => (GovernanceState::Normal, GovernanceState::Normal),
        };
        ReaperDecision {
            key: contract,
            from,
            to,
            reason,
            at,
            actionable: true,
        }
    }

    fn mk_ban_list() -> (ContractBanList, SharedMockTimeSource) {
        let ts = SharedMockTimeSource::new();
        let bl = ContractBanList::new(Arc::new(ts.clone()));
        (bl, ts)
    }

    #[test]
    fn unbanned_contract_returns_false() {
        let (bl, _ts) = mk_ban_list();
        assert!(!bl.is_banned(&mk_contract(1)));
    }

    #[test]
    fn banned_contract_returns_true_until_expiry() {
        let (bl, ts) = mk_ban_list();
        let contract = mk_contract(1);
        let now = ts.now();
        bl.ban(contract, now + Duration::from_secs(60), BanReason::AutoMad);
        assert!(bl.is_banned(&contract));
        // Just before expiry.
        ts.advance_time(Duration::from_secs(59));
        assert!(bl.is_banned(&contract));
        // At expiry — boundary check uses `<` so equal-time is NOT
        // banned (the contract has done its time).
        ts.advance_time(Duration::from_secs(1));
        assert!(!bl.is_banned(&contract));
    }

    #[test]
    fn unban_removes_entry_immediately() {
        let (bl, ts) = mk_ban_list();
        let contract = mk_contract(1);
        bl.ban(
            contract,
            ts.now() + Duration::from_secs(60),
            BanReason::AutoMad,
        );
        assert!(bl.is_banned(&contract));
        bl.unban(&contract);
        assert!(!bl.is_banned(&contract));
        assert_eq!(bl.len(), 0);
    }

    #[test]
    fn rebanning_extends_window_not_shortens() {
        let (bl, ts) = mk_ban_list();
        let contract = mk_contract(1);
        let start = ts.now();
        bl.ban(
            contract,
            start + Duration::from_secs(120),
            BanReason::AutoMad,
        );
        // Re-ban with an EARLIER expiry — must not shorten.
        bl.ban(
            contract,
            start + Duration::from_secs(30),
            BanReason::AutoMad,
        );
        ts.advance_time(Duration::from_secs(60));
        assert!(
            bl.is_banned(&contract),
            "re-banning with earlier expiry must not shorten the existing ban"
        );
    }

    #[test]
    fn rebanning_with_later_expiry_extends() {
        let (bl, ts) = mk_ban_list();
        let contract = mk_contract(1);
        let start = ts.now();
        bl.ban(
            contract,
            start + Duration::from_secs(60),
            BanReason::AutoMad,
        );
        bl.ban(
            contract,
            start + Duration::from_secs(120),
            BanReason::AutoMad,
        );
        ts.advance_time(Duration::from_secs(90));
        assert!(
            bl.is_banned(&contract),
            "extended ban window must keep contract banned past original expiry"
        );
    }

    #[test]
    fn operator_reason_wins_over_auto() {
        let (bl, ts) = mk_ban_list();
        let contract = mk_contract(1);
        let now = ts.now();
        bl.ban(contract, now + Duration::from_secs(60), BanReason::AutoMad);
        bl.ban(contract, now + Duration::from_secs(60), BanReason::Operator);
        // We don't expose `reason` publicly yet (Phase 7 follow-up
        // surfaces it on the dashboard), but the invariant matters:
        // operator action shouldn't be overwritten by a later auto
        // flip. Test via a fresh override.
        bl.ban(contract, now + Duration::from_secs(60), BanReason::AutoMad);
        assert!(bl.is_banned(&contract));
        // Direct inspect via internal field for test.
        let entry = bl.entries.get(&contract).unwrap();
        assert_eq!(entry.reason, BanReason::Operator);
    }

    #[test]
    fn cleanup_drops_expired_entries() {
        let (bl, ts) = mk_ban_list();
        bl.ban(
            mk_contract(1),
            ts.now() + Duration::from_secs(10),
            BanReason::AutoMad,
        );
        bl.ban(
            mk_contract(2),
            ts.now() + Duration::from_secs(60),
            BanReason::AutoMad,
        );
        assert_eq!(bl.len(), 2);
        ts.advance_time(Duration::from_secs(20));
        bl.cleanup();
        assert_eq!(
            bl.len(),
            1,
            "cleanup must drop contract 1 (expired) but keep contract 2 (fresh)"
        );
    }

    #[test]
    fn distinct_contracts_independent() {
        let (bl, ts) = mk_ban_list();
        bl.ban(
            mk_contract(1),
            ts.now() + Duration::from_secs(60),
            BanReason::AutoMad,
        );
        assert!(bl.is_banned(&mk_contract(1)));
        assert!(!bl.is_banned(&mk_contract(2)));
        assert_eq!(bl.len(), 1);
    }

    // Wiring tests — `Ring::apply_ban_decisions` is the bridge between
    // governance decisions and the ban list. Even with the unit tests
    // above passing, a missing or reversed match arm in the wiring
    // would silently break Phase 7. These tests pin the contract.

    #[test]
    fn ban_decision_adds_to_list() {
        let (bl, ts) = mk_ban_list();
        let contract = mk_contract(7);
        let now = ts.now();
        let decisions = vec![mk_decision(contract, TransitionReason::BanTriggered, now)];
        Ring::apply_ban_decisions(&bl, &decisions, now + Duration::from_secs(60));
        assert!(
            bl.is_banned(&contract),
            "BanTriggered decision must add the contract to the ban list"
        );
    }

    #[test]
    fn lifted_decision_removes_from_list() {
        let (bl, ts) = mk_ban_list();
        let contract = mk_contract(7);
        bl.ban(
            contract,
            ts.now() + Duration::from_secs(60),
            BanReason::AutoMad,
        );
        let decisions = vec![mk_decision(contract, TransitionReason::BanLifted, ts.now())];
        Ring::apply_ban_decisions(&bl, &decisions, ts.now() + Duration::from_secs(60));
        assert!(
            !bl.is_banned(&contract),
            "BanLifted decision must remove the contract from the ban list"
        );
    }

    #[test]
    fn unrelated_decisions_do_not_touch_ban_list() {
        let (bl, ts) = mk_ban_list();
        let contract = mk_contract(7);
        // Non-ban transitions (Evicted, etc.) should be no-ops here —
        // they're handled elsewhere (eviction queue, dashboard).
        let decisions = vec![mk_decision(contract, TransitionReason::Evicted, ts.now())];
        Ring::apply_ban_decisions(&bl, &decisions, ts.now() + Duration::from_secs(60));
        assert!(
            !bl.is_banned(&contract),
            "non-ban transition reasons must not affect the ban list"
        );
        assert_eq!(bl.len(), 0);
    }

    #[test]
    fn batch_mixed_decisions_apply_in_order() {
        let (bl, ts) = mk_ban_list();
        let a = mk_contract(1);
        let b = mk_contract(2);
        let c = mk_contract(3);
        // Pre-ban `b` so a BanLifted in the same batch can clear it.
        bl.ban(b, ts.now() + Duration::from_secs(60), BanReason::AutoMad);
        let now = ts.now();
        let decisions = vec![
            mk_decision(a, TransitionReason::BanTriggered, now),
            mk_decision(b, TransitionReason::BanLifted, now),
            mk_decision(c, TransitionReason::BanTriggered, now),
        ];
        Ring::apply_ban_decisions(&bl, &decisions, now + Duration::from_secs(60));
        assert!(bl.is_banned(&a), "contract A must be newly banned");
        assert!(!bl.is_banned(&b), "contract B must be unbanned");
        assert!(bl.is_banned(&c), "contract C must be newly banned");
    }
}

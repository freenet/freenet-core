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
//! - `GetMsg::Request` — refuse to serve. (No `RequestStreaming`
//!   variant exists for GET; only responses stream.)
//! - `UpdateMsg::*` Request / Broadcast variants — refuse to apply or
//!   fan out.
//! - `SubscribeMsg::Request` — refuse to register interest.
//!   `Unsubscribe` deliberately passes through so cleanup proceeds.
//!
//! ## What this does NOT reject
//!
//! - **Responses to our own outbound requests.** If we sent a request
//!   before the ban kicked in, allowing the reply completes that
//!   transaction cleanly. The next request we originate for the contract
//!   is self-blocked at the egress entry points
//!   (`operations::reject_if_contract_banned`, called from every
//!   `start_client_*` driver — #4300).
//! - **Peer-level messages** (ConnectMsg, etc.) — those are the peer
//!   layer's concern, not the contract layer.
//! ## Egress self-blocking (#4300)
//!
//! Beyond the receive boundary, the ban also gates every path by which
//! this node could *transmit* state for a banned contract. These gates
//! live at the egress sites, not here:
//!
//! - **Client-originated requests.** `start_client_put` / `_get` /
//!   `_subscribe` / `_update` call `operations::reject_if_contract_banned`
//!   before spawning the driver, returning `OpError::ContractBanned` to
//!   the local client.
//! - **Delegate-driven UPDATE fan-out.** `handle_broadcast_state_change`
//!   (p2p_protoc.rs) skips the broadcast for a banned contract.
//! - **Proactive state egress for already-hosted contracts**
//!   (`NeighborHosting` overlap sync, `InterestSync::Summaries` stale
//!   repair). Gated in node.rs (PR #4299).
//!
//! Together with the receive-side drops, a banned contract can neither
//! receive new state via this node nor transmit new state via it.
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
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use dashmap::DashMap;
use freenet_stdlib::prelude::ContractInstanceId;
use tokio::time::Instant;

use crate::util::time_source::TimeSource;

/// Hard upper bound on the number of contracts that can be on the ban
/// list at once. Mirrors the defense-in-depth pattern Phase 2's
/// [`crate::ring::update_rate_limit::MAX_TRACKED_PAIRS`] established:
/// a bounded collection whose keys can become attacker-influenced.
///
/// Today the only writer is the governance reaper, and the reaper can
/// only ban contracts the local node observes — so the natural bound
/// is the local contract population (~hundreds). But once the
/// operator-CLI surface (issue #4274) lands and starts writing via
/// [`BanReason::Operator`], the input becomes operator-influenced: a
/// misconfigured config file, a runaway script, or an attacker who
/// reaches the management socket could otherwise push arbitrary keys
/// into the list and silently drop legitimate contracts at the wire
/// boundary (`is_banned` is checked before every inbound
/// PUT/GET/UPDATE/SUBSCRIBE).
///
/// 8 192 is well above any expected production size (the reaper-driven
/// list is bounded by the local contract population) while keeping the
/// worst-case memory trivially small (~8 K entries × a handful of bytes).
pub(crate) const MAX_BANNED_CONTRACTS: usize = 8_192;

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

/// Outcome of a [`ContractBanList::ban`] call. Mirrors the
/// `RateLimitDecision` shape in [`crate::ring::update_rate_limit`] so
/// the cap behaviour is observable (callers / the dashboard can tell
/// "ban applied" apart from "ban list full").
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BanOutcome {
    /// The contract was newly added to the ban list (a slot was
    /// reserved via the size counter).
    Banned,
    /// The contract was already on the list; its expiry/reason were
    /// updated in place. No new slot was consumed.
    Updated,
    /// The ban list was at [`MAX_BANNED_CONTRACTS`] and this add could
    /// not be admitted, so the contract is NOT banned. Two ways to hit
    /// this: an [`BanReason::Operator`] add when the list is full (it can
    /// never displace a reaper-driven ban), or a [`BanReason::AutoMad`]
    /// add when the list is full *and* entirely reaper-driven (no
    /// `Operator` entry to evict).
    CapacityExceeded,
}

/// Per-(contract instance) ban list. Read by the wire-message
/// dispatch site to drop inbound requests; written by the governance
/// reaper loop on `BanTriggered` / `BanLifted` transitions.
pub(crate) struct ContractBanList {
    entries: DashMap<ContractInstanceId, BanEntry>,
    /// Authoritative size counter for capacity enforcement. Held in
    /// sync with `entries.len()` at every stable point: incremented by
    /// successful new-key inserts in [`Self::ban`], decremented by
    /// [`Self::unban`] and by the count of dropped entries in
    /// [`Self::cleanup`].
    ///
    /// Why not just read `entries.len()`? Because `len()` walks every
    /// shard, so it can't be consulted while holding a shard guard, and
    /// a probe-then-insert (`if len() < cap { insert }`) overshoots the
    /// cap by up to `num_concurrent_callers` under a concurrent flood.
    /// The `fetch_add` reservation here strictly serializes — the same
    /// fix Phase 2's review (#4285) applied to `UpdateRateLimiter`.
    size: AtomicUsize,
    /// Maximum number of simultaneously-banned contracts. Defaults to
    /// [`MAX_BANNED_CONTRACTS`]; overridable in tests via
    /// [`Self::with_max`].
    max_banned: usize,
    /// Total bans rejected because the list was at capacity (operator
    /// adds that couldn't displace a reaper-driven ban). A non-zero
    /// value is the operator's signal that the cap is being hit —
    /// surfaced on the dashboard alongside the ban count.
    capacity_rejected_total: AtomicU64,
    time_source: Arc<dyn TimeSource + Send + Sync>,
}

impl ContractBanList {
    pub fn new(time_source: Arc<dyn TimeSource + Send + Sync>) -> Self {
        Self::with_max(time_source, MAX_BANNED_CONTRACTS)
    }

    /// Construct with an explicit cap. Used by tests to exercise the
    /// capacity path with a small list; production always uses
    /// [`Self::new`] (cap = [`MAX_BANNED_CONTRACTS`]).
    pub fn with_max(time_source: Arc<dyn TimeSource + Send + Sync>, max_banned: usize) -> Self {
        Self {
            entries: DashMap::new(),
            size: AtomicUsize::new(0),
            max_banned,
            capacity_rejected_total: AtomicU64::new(0),
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
    /// and reason.
    ///
    /// Idempotent for an already-banned contract: re-banning extends
    /// the expiry to whichever value is later (never shortens it). The
    /// stored reason is the *higher-priority* of the existing and
    /// incoming reasons: [`BanReason::AutoMad`] dominates
    /// [`BanReason::Operator`], so once a contract has been auto-banned
    /// by the node's own security logic, a later operator re-ban cannot
    /// downgrade it to an evictable `Operator` entry. This path consumes
    /// no new slot and is never capacity-rejected.
    ///
    /// New-contract inserts are bounded by [`Self::max_banned`]
    /// (production: [`MAX_BANNED_CONTRACTS`]). A slot is reserved with
    /// an atomic `fetch_add` BEFORE inserting, so concurrent distinct-
    /// key inserts strictly serialize through the counter and cannot
    /// overshoot the cap (the probe-then-insert race Phase 2's review
    /// caught — see [`Self::size`]).
    ///
    /// **Priority when the list is full:** reaper-driven
    /// ([`BanReason::AutoMad`]) bans take precedence over operator-
    /// driven ([`BanReason::Operator`]) bans. If an `AutoMad` ban
    /// arrives at capacity, it evicts one existing `Operator` entry to
    /// make room; if no `Operator` entry exists (the list is entirely
    /// reaper-driven) the add is rejected. An `Operator` ban at
    /// capacity is always rejected — operator input must never displace
    /// a security decision the node made itself.
    ///
    /// Returns a [`BanOutcome`] describing what happened.
    pub fn ban(
        &self,
        contract: ContractInstanceId,
        expires_at: Instant,
        reason: BanReason,
    ) -> BanOutcome {
        use dashmap::mapref::entry::Entry;
        // Loop (not recursion) so that an AutoMad ban that has to evict
        // an Operator entry to make room re-attempts iteratively. Each
        // iteration either succeeds, fails terminally, or evicts exactly
        // one Operator entry; the number of Operator entries strictly
        // decreases per eviction, so the loop terminates and uses O(1)
        // stack regardless of how full the list is.
        loop {
            match self.entries.entry(contract) {
                Entry::Occupied(mut e) => {
                    let cur = e.get_mut();
                    // Keep whichever expiry is later — re-bans should
                    // never shorten an existing ban window.
                    if expires_at > cur.expires_at {
                        cur.expires_at = expires_at;
                    }
                    // Reason priority: AutoMad dominates Operator. A
                    // reaper-driven (security) ban must never be
                    // downgraded to an evictable Operator entry by a
                    // later operator re-ban — that would let operator
                    // input (or an attacker reaching the CLI) make a
                    // node-made security decision displaceable under the
                    // eviction rule below. Equivalently: the entry
                    // becomes/stays AutoMad if either side is AutoMad.
                    if matches!(reason, BanReason::AutoMad) {
                        cur.reason = BanReason::AutoMad;
                    }
                    return BanOutcome::Updated;
                }
                Entry::Vacant(e) => {
                    // New contract: reserve a slot via the authoritative
                    // counter BEFORE inserting. `fetch_add` is the
                    // serialization point — concurrent new-key inserts
                    // strictly serialize, no overshoot beyond the cap.
                    let prev = self.size.fetch_add(1, Ordering::Relaxed);
                    if prev < self.max_banned {
                        e.insert(BanEntry { expires_at, reason });
                        return BanOutcome::Banned;
                    }

                    // At capacity. Roll back the speculative reservation
                    // and release the per-shard guard before doing
                    // anything that touches other shards.
                    self.size.fetch_sub(1, Ordering::Relaxed);
                    drop(e);

                    // Reaper-driven bans take precedence: try to evict
                    // one Operator entry to free a slot, then re-attempt
                    // the insert from the top of the loop. The guard is
                    // released above because `evict_one_operator` walks
                    // every shard via `retain` — holding a shard guard
                    // across it would deadlock. The freed slot is NOT
                    // held between the eviction and the re-attempt;
                    // another thread may grab it, in which case the next
                    // iteration either evicts again or falls through to
                    // CapacityExceeded.
                    if matches!(reason, BanReason::AutoMad) && self.evict_one_operator() {
                        continue;
                    }

                    self.capacity_rejected_total.fetch_add(1, Ordering::Relaxed);
                    tracing::debug!(
                        %contract,
                        ?reason,
                        max_banned = self.max_banned,
                        "contract ban rejected: ban list at capacity"
                    );
                    return BanOutcome::CapacityExceeded;
                }
            }
        }
    }

    /// Evict a single `Operator`-reason entry, if any exists, to free a
    /// slot for a higher-priority `AutoMad` ban. Returns `true` if an
    /// entry was removed (and the size counter decremented), `false` if
    /// the list contains no `Operator` entries.
    ///
    /// Uses `retain` to stop after the first removal so this is O(list)
    /// worst case but typically returns early. No particular eviction
    /// order is promised — at the cap, any operator entry is a fair
    /// victim, and operator bans are advisory relative to the node's
    /// own security decisions.
    fn evict_one_operator(&self) -> bool {
        let mut evicted = false;
        self.entries.retain(|_, entry| {
            if !evicted && matches!(entry.reason, BanReason::Operator) {
                evicted = true;
                false // drop this entry
            } else {
                true
            }
        });
        if evicted {
            self.size.fetch_sub(1, Ordering::Relaxed);
        }
        evicted
    }

    /// Remove a contract from the ban list, regardless of expiry.
    /// Called from the reaper loop when a `BanLifted` decision fires.
    ///
    /// Decrements the [`Self::size`] counter when an entry was actually
    /// present, so the strict capacity accounting stays in sync.
    pub fn unban(&self, contract: &ContractInstanceId) {
        if self.entries.remove(contract).is_some() {
            self.size.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Drop entries whose `expires_at` has passed. Called periodically
    /// from the governance reaper tick as a defense-in-depth in case
    /// an explicit `unban` was skipped (process restart, race during
    /// configuration reload, etc).
    ///
    /// Decrements the [`Self::size`] counter by the number of removed
    /// entries so the cap recovers as expired bans roll off.
    pub fn cleanup(&self) {
        let now = self.time_source.now();
        let mut removed = 0usize;
        self.entries.retain(|_, entry| {
            let keep = entry.expires_at > now;
            if !keep {
                removed += 1;
            }
            keep
        });
        if removed > 0 {
            self.size.fetch_sub(removed, Ordering::Relaxed);
        }
    }

    /// Number of currently-banned contracts. Used by tests and the
    /// dashboard's governance card for "N contracts on ban list."
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Total bans rejected because the list was at capacity. A non-zero
    /// value tells operators the cap is being hit — surfaced on the
    /// dashboard's governance card alongside the ban count. Mirrors
    /// [`crate::ring::update_rate_limit::UpdateRateLimiter::capacity_rejected_total`].
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn capacity_rejected_total(&self) -> u64 {
        self.capacity_rejected_total.load(Ordering::Relaxed)
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
        #[allow(clippy::wildcard_enum_match_arm)]
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

    // Superseded by the #4303 cap (PR #4370): before the size cap, an
    // operator re-ban "won" and overwrote the stored reason to Operator.
    // The cap introduced an eviction rule where Operator entries are the
    // evictable (lower-priority) ones, so that old semantic would let an
    // operator re-ban downgrade a node-made security (AutoMad) ban into a
    // displaceable entry. The behavior was deliberately inverted —
    // AutoMad now dominates (see `auto_reason_dominates_operator_on_reban`
    // below). Kept here, ignored, as historical documentation of the
    // pre-cap behavior and why it changed.
    #[ignore = "superseded by #4303 cap: AutoMad now dominates Operator on re-ban (PR #4370)"]
    #[test]
    fn operator_reason_wins_over_auto() {
        let (bl, ts) = mk_ban_list();
        let contract = mk_contract(1);
        let now = ts.now();
        bl.ban(contract, now + Duration::from_secs(60), BanReason::AutoMad);
        bl.ban(contract, now + Duration::from_secs(60), BanReason::Operator);
        // Pre-cap invariant (NO LONGER TRUE): operator action shouldn't
        // be overwritten by a later auto flip.
        bl.ban(contract, now + Duration::from_secs(60), BanReason::AutoMad);
        assert!(bl.is_banned(&contract));
        let entry = bl.entries.get(&contract).unwrap();
        assert_eq!(entry.reason, BanReason::Operator);
    }

    #[test]
    fn auto_reason_dominates_operator_on_reban() {
        // Semantics changed by the #4303 cap: AutoMad now DOMINATES
        // Operator on a re-ban (previously Operator won). The cap's
        // eviction rule makes Operator the evictable / lower-priority
        // reason, so a reaper-driven (security) ban must not be
        // downgraded to Operator by a later operator re-ban — otherwise
        // an operator (or attacker reaching the CLI) could make a
        // node-made security decision displaceable. See
        // `auto_ban_evicts_operator_entry_at_cap`.
        let (bl, ts) = mk_ban_list();
        let contract = mk_contract(1);
        let now = ts.now();
        // Operator-ban first, then a reaper auto-ban: must end AutoMad.
        bl.ban(contract, now + Duration::from_secs(60), BanReason::Operator);
        bl.ban(contract, now + Duration::from_secs(60), BanReason::AutoMad);
        assert!(bl.is_banned(&contract));
        assert_eq!(
            bl.entries.get(&contract).unwrap().reason,
            BanReason::AutoMad,
            "a reaper auto-ban must upgrade an existing operator entry to AutoMad"
        );

        // And a later operator re-ban must NOT downgrade it back.
        bl.ban(contract, now + Duration::from_secs(60), BanReason::Operator);
        assert_eq!(
            bl.entries.get(&contract).unwrap().reason,
            BanReason::AutoMad,
            "an operator re-ban must never downgrade a security (AutoMad) ban"
        );
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

    // ---- MAX_BANNED_CONTRACTS cap (issue #4303) ----
    //
    // Mirrors the `UpdateRateLimiter` cap tests in
    // `update_rate_limit.rs`: a hard size cap enforced via an
    // `AtomicUsize::fetch_add` reservation, with strict (no-overshoot)
    // accounting under concurrency.

    fn mk_ban_list_with_max(max: usize) -> (ContractBanList, SharedMockTimeSource) {
        let ts = SharedMockTimeSource::new();
        let bl = ContractBanList::with_max(Arc::new(ts.clone()), max);
        (bl, ts)
    }

    #[test]
    fn ban_returns_outcome() {
        let (bl, ts) = mk_ban_list();
        let c = mk_contract(1);
        let exp = ts.now() + Duration::from_secs(60);
        assert_eq!(bl.ban(c, exp, BanReason::AutoMad), BanOutcome::Banned);
        // Re-banning the same contract updates in place, no new slot.
        assert_eq!(bl.ban(c, exp, BanReason::AutoMad), BanOutcome::Updated);
        assert_eq!(bl.len(), 1);
    }

    #[test]
    fn capacity_exceeded_when_cap_reached() {
        let (bl, ts) = mk_ban_list_with_max(8);
        let exp = ts.now() + Duration::from_secs(60);
        // Fill to the cap — all reaper-driven so none are evictable.
        for i in 0..8 {
            assert_eq!(
                bl.ban(mk_contract(i + 1), exp, BanReason::AutoMad),
                BanOutcome::Banned,
                "ban {i} below the cap must be admitted"
            );
        }
        assert_eq!(bl.len(), 8);
        // The 9th distinct contract is past the cap.
        assert_eq!(
            bl.ban(mk_contract(99), exp, BanReason::AutoMad),
            BanOutcome::CapacityExceeded,
            "new contract past the cap must be CapacityExceeded"
        );
        // Size must NOT overshoot: the rejected add rolled back its
        // speculative reservation.
        assert_eq!(bl.len(), 8, "cap rejection must not grow the list");
        assert_eq!(bl.capacity_rejected_total(), 1);
        // The rejected contract is genuinely not banned.
        assert!(!bl.is_banned(&mk_contract(99)));
        // An already-banned contract keeps working at the cap (the
        // Occupied/Updated path is never capacity-rejected).
        assert_eq!(
            bl.ban(mk_contract(1), exp, BanReason::AutoMad),
            BanOutcome::Updated,
            "existing contract must keep working at the cap"
        );
    }

    #[test]
    fn operator_ban_rejected_at_cap_when_only_auto_present() {
        let (bl, ts) = mk_ban_list_with_max(4);
        let exp = ts.now() + Duration::from_secs(60);
        for i in 0..4 {
            bl.ban(mk_contract(i + 1), exp, BanReason::AutoMad);
        }
        // Operator add at the cap, with no Operator entry to displace,
        // must be rejected — operator input must not displace a
        // node-made security decision.
        assert_eq!(
            bl.ban(mk_contract(50), exp, BanReason::Operator),
            BanOutcome::CapacityExceeded,
            "operator ban at cap must not displace reaper-driven bans"
        );
        assert!(!bl.is_banned(&mk_contract(50)));
        assert_eq!(bl.len(), 4);
    }

    #[test]
    fn auto_ban_evicts_operator_entry_at_cap() {
        let (bl, ts) = mk_ban_list_with_max(4);
        let exp = ts.now() + Duration::from_secs(60);
        // One operator entry, three reaper entries — list is full.
        bl.ban(mk_contract(1), exp, BanReason::Operator);
        bl.ban(mk_contract(2), exp, BanReason::AutoMad);
        bl.ban(mk_contract(3), exp, BanReason::AutoMad);
        bl.ban(mk_contract(4), exp, BanReason::AutoMad);
        assert_eq!(bl.len(), 4);

        // A reaper-driven ban at the cap must take precedence: it
        // evicts the operator entry and is itself admitted.
        assert_eq!(
            bl.ban(mk_contract(5), exp, BanReason::AutoMad),
            BanOutcome::Banned,
            "reaper ban at cap must evict an operator entry and succeed"
        );
        assert!(bl.is_banned(&mk_contract(5)), "new reaper ban is active");
        assert!(
            !bl.is_banned(&mk_contract(1)),
            "the operator entry must have been evicted to make room"
        );
        // Still exactly at the cap — one in, one out.
        assert_eq!(bl.len(), 4);
        // No capacity rejection was recorded (the add succeeded).
        assert_eq!(bl.capacity_rejected_total(), 0);
    }

    #[test]
    fn auto_ban_rejected_at_cap_when_no_operator_to_evict() {
        let (bl, ts) = mk_ban_list_with_max(3);
        let exp = ts.now() + Duration::from_secs(60);
        // Entirely reaper-driven and full — nothing to evict.
        for i in 0..3 {
            bl.ban(mk_contract(i + 1), exp, BanReason::AutoMad);
        }
        assert_eq!(
            bl.ban(mk_contract(9), exp, BanReason::AutoMad),
            BanOutcome::CapacityExceeded,
            "reaper ban at a fully-reaper-driven cap has nothing to evict"
        );
        assert_eq!(bl.len(), 3);
        assert_eq!(bl.capacity_rejected_total(), 1);
    }

    #[test]
    fn unban_decrements_size_counter_freeing_a_slot() {
        let (bl, ts) = mk_ban_list_with_max(2);
        let exp = ts.now() + Duration::from_secs(60);
        bl.ban(mk_contract(1), exp, BanReason::AutoMad);
        bl.ban(mk_contract(2), exp, BanReason::AutoMad);
        // At cap.
        assert_eq!(
            bl.ban(mk_contract(3), exp, BanReason::AutoMad),
            BanOutcome::CapacityExceeded
        );
        // Free a slot.
        bl.unban(&mk_contract(1));
        assert_eq!(bl.len(), 1);
        // The cap must now have room again.
        assert_eq!(
            bl.ban(mk_contract(3), exp, BanReason::AutoMad),
            BanOutcome::Banned,
            "unban must free a slot for new bans (size counter decremented)"
        );
        assert_eq!(bl.len(), 2);
    }

    #[test]
    fn unban_missing_contract_does_not_underflow_counter() {
        let (bl, ts) = mk_ban_list_with_max(2);
        let exp = ts.now() + Duration::from_secs(60);
        // Unban a contract that was never banned — must be a no-op and
        // must NOT decrement the (already zero) size counter.
        bl.unban(&mk_contract(7));
        bl.ban(mk_contract(1), exp, BanReason::AutoMad);
        bl.ban(mk_contract(2), exp, BanReason::AutoMad);
        // Counter is accurate: at cap.
        assert_eq!(
            bl.ban(mk_contract(3), exp, BanReason::AutoMad),
            BanOutcome::CapacityExceeded,
            "spurious unban must not corrupt the size counter"
        );
    }

    #[test]
    fn cleanup_decrements_size_counter_freeing_slots() {
        let (bl, ts) = mk_ban_list_with_max(2);
        let now = ts.now();
        bl.ban(
            mk_contract(1),
            now + Duration::from_secs(10),
            BanReason::AutoMad,
        );
        bl.ban(
            mk_contract(2),
            now + Duration::from_secs(60),
            BanReason::AutoMad,
        );
        // At cap.
        assert_eq!(
            bl.ban(
                mk_contract(3),
                now + Duration::from_secs(60),
                BanReason::AutoMad
            ),
            BanOutcome::CapacityExceeded
        );
        // Expire contract 1 and sweep it.
        ts.advance_time(Duration::from_secs(20));
        bl.cleanup();
        assert_eq!(bl.len(), 1, "cleanup drops the expired entry");
        // The freed slot must be re-usable — counter was decremented.
        assert_eq!(
            bl.ban(
                mk_contract(3),
                ts.now() + Duration::from_secs(60),
                BanReason::AutoMad
            ),
            BanOutcome::Banned,
            "cleanup must free a slot for new bans (size counter decremented)"
        );
        assert_eq!(bl.len(), 2);
    }

    #[test]
    fn concurrent_distinct_bans_do_not_overshoot_cap() {
        use std::sync::{Arc as StdArc, Barrier};
        use std::thread;

        const CAP: usize = 8;
        const THREADS: usize = 64; // 8× the cap to stress the race

        let ts = SharedMockTimeSource::new();
        let bl = StdArc::new(ContractBanList::with_max(Arc::new(ts.clone()), CAP));
        let exp = ts.now() + Duration::from_secs(60);
        let barrier = StdArc::new(Barrier::new(THREADS));
        let mut handles = Vec::with_capacity(THREADS);

        for i in 0..THREADS {
            let bl = bl.clone();
            let b = barrier.clone();
            handles.push(thread::spawn(move || {
                b.wait();
                // Each thread bans a DISTINCT contract so they all
                // exercise the Vacant (new-key reservation) path.
                bl.ban(mk_contract((i + 1) as u8), exp, BanReason::AutoMad)
            }));
        }

        let mut banned = 0;
        let mut cap_rejected = 0;
        let mut updated = 0;
        for h in handles {
            match h.join().unwrap() {
                BanOutcome::Banned => banned += 1,
                BanOutcome::CapacityExceeded => cap_rejected += 1,
                BanOutcome::Updated => updated += 1,
            }
        }
        // Strict cap: the list size must equal CAP exactly, not
        // CAP + overshoot. This is the property the `fetch_add`
        // reservation guarantees over a `len()` precheck.
        assert_eq!(
            bl.len(),
            CAP,
            "strict cap: list size must equal CAP after a 64-thread flood, got {}",
            bl.len()
        );
        assert_eq!(banned, CAP, "exactly CAP bans admitted, got {banned}");
        assert_eq!(cap_rejected, THREADS - CAP);
        assert_eq!(updated, 0, "all keys distinct, no Updated outcomes");
        assert_eq!(bl.capacity_rejected_total(), (THREADS - CAP) as u64);
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
    fn non_actionable_decision_is_skipped() {
        // DryRun-mode safety: even if a BanTriggered somehow reaches
        // the wiring with `actionable: false` (a future Shadow mode,
        // a misconfigured GovernanceManager), it must not enter the
        // ban list. Otherwise DryRun silently enforces.
        let (bl, ts) = mk_ban_list();
        let contract = mk_contract(9);
        let mut decision = mk_decision(contract, TransitionReason::BanTriggered, ts.now());
        decision.actionable = false;
        Ring::apply_ban_decisions(&bl, &[decision], ts.now() + Duration::from_secs(60));
        assert!(
            !bl.is_banned(&contract),
            "non-actionable BanTriggered must not land on the ban list"
        );

        // And the same for BanLifted — a non-actionable lift must
        // not silently delete an enforcement-mode ban.
        bl.ban(
            contract,
            ts.now() + Duration::from_secs(60),
            BanReason::AutoMad,
        );
        let mut decision = mk_decision(contract, TransitionReason::BanLifted, ts.now());
        decision.actionable = false;
        Ring::apply_ban_decisions(&bl, &[decision], ts.now() + Duration::from_secs(60));
        assert!(
            bl.is_banned(&contract),
            "non-actionable BanLifted must not remove an existing ban"
        );
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

    // Source-grep pin tests for the wire-boundary and egress gates in
    // node.rs. The gates themselves are one-liner `is_banned(...)`
    // checks — a refactor of `process_message` that drops or
    // reorders one would silently disable enforcement and pass every
    // other test in the suite. Mirrors the pattern from Phase 2's
    // `update_dispatch_gates_all_four_wire_variants` test in
    // `update_rate_limit.rs`. If you're here because a test below
    // failed: the message tells you which gate moved or disappeared
    // — re-establish the check before re-running the suite.

    /// Returns the slice of `node.rs` covering one `NetMessageV1::X`
    /// arm. Bounds the slice at the next `NetMessageV1::` arm so an
    /// assertion can't accidentally match the next handler.
    fn dispatch_block<'a>(src: &'a str, arm_header: &str) -> &'a str {
        let start = src
            .find(arm_header)
            .unwrap_or_else(|| panic!("could not locate `{arm_header}` in node.rs"));
        let tail = &src[start + 1..];
        let len = tail
            .find("\n        NetMessageV1::")
            .or_else(|| tail.find("\n    NetMessageV1::"))
            .unwrap_or(tail.len());
        &src[start..start + 1 + len]
    }

    #[test]
    fn put_dispatch_gates_banned_contracts() {
        const NODE_SRC: &str = include_str!("../node.rs");
        let block = dispatch_block(NODE_SRC, "NetMessageV1::Put(ref op) =>");
        let gate_pos = block
            .find("contract_ban_list.is_banned")
            .expect("PUT dispatch is missing the ban-list gate");
        let spawn_pos = block
            .find("start_relay_put")
            .expect("PUT dispatch is missing start_relay_put");
        assert!(
            gate_pos < spawn_pos,
            "PUT ban-list gate (offset {gate_pos}) must run BEFORE \
             start_relay_put (offset {spawn_pos}) so banned requests \
             don't pay the spawn cost"
        );
        // Both inbound request variants must be matched so a flooder
        // can't bypass by switching to streaming (mirrors the Phase 2
        // four-variant pin).
        for variant in ["PutMsg::Request {", "PutMsg::RequestStreaming {"] {
            assert!(
                block.contains(variant),
                "PUT dispatch block is missing wire variant `{variant}`"
            );
        }
    }

    #[test]
    fn get_dispatch_gates_banned_contracts() {
        const NODE_SRC: &str = include_str!("../node.rs");
        let block = dispatch_block(NODE_SRC, "NetMessageV1::Get(ref op) =>");
        let gate_pos = block
            .find("contract_ban_list.is_banned")
            .expect("GET dispatch is missing the ban-list gate");
        let spawn_pos = block
            .find("start_relay_get")
            .expect("GET dispatch is missing start_relay_get");
        assert!(
            gate_pos < spawn_pos,
            "GET ban-list gate (offset {gate_pos}) must run BEFORE \
             start_relay_get (offset {spawn_pos})"
        );
        assert!(
            block.contains("GetMsg::Request {"),
            "GET dispatch block is missing wire variant `GetMsg::Request {{`"
        );
    }

    #[test]
    fn update_dispatch_gates_banned_contracts() {
        const NODE_SRC: &str = include_str!("../node.rs");
        let block = dispatch_block(NODE_SRC, "NetMessageV1::Update(ref op) =>");
        let gate_pos = block
            .find("contract_ban_list.is_banned")
            .expect("UPDATE dispatch is missing the ban-list gate");
        let rate_limit_pos = block
            .find("update_rate_limiter")
            .expect("UPDATE dispatch is missing the rate limiter");
        assert!(
            gate_pos < rate_limit_pos,
            "UPDATE ban-list gate (offset {gate_pos}) must run BEFORE \
             the rate limiter (offset {rate_limit_pos}) so banned \
             traffic doesn't consume the rate-limit budget"
        );
        // All four UPDATE wire variants must remain matched; if a new
        // one is added it must be gated through both the ban list
        // and the rate limiter.
        for variant in [
            "UpdateMsg::RequestUpdate {",
            "UpdateMsg::BroadcastTo {",
            "UpdateMsg::RequestUpdateStreaming {",
            "UpdateMsg::BroadcastToStreaming {",
        ] {
            assert!(
                block.contains(variant),
                "UPDATE dispatch block is missing wire variant `{variant}`"
            );
        }
    }

    #[test]
    fn subscribe_dispatch_gates_banned_contracts() {
        const NODE_SRC: &str = include_str!("../node.rs");
        let block = dispatch_block(NODE_SRC, "NetMessageV1::Subscribe(ref op) =>");
        let gate_pos = block
            .find("contract_ban_list.is_banned")
            .expect("SUBSCRIBE dispatch is missing the ban-list gate");
        // Match the call site (`(`-terminated) — `start_relay_subscribe`
        // appears in a doc comment too, and `block.find()` returns the
        // first match.
        let driver_pos = block
            .find("start_relay_subscribe(")
            .expect("SUBSCRIBE dispatch is missing start_relay_subscribe call");
        assert!(
            gate_pos < driver_pos,
            "SUBSCRIBE ban-list gate (offset {gate_pos}) must run \
             BEFORE start_relay_subscribe call (offset {driver_pos})"
        );
        // Gate must match Request (block registration); Unsubscribe
        // deliberately passes through so cleanup proceeds.
        assert!(
            block.contains("SubscribeMsg::Request {"),
            "SUBSCRIBE dispatch block is missing `SubscribeMsg::Request {{`"
        );
    }

    #[test]
    fn neighbor_hosting_gates_banned_egress() {
        const NODE_SRC: &str = include_str!("../node.rs");
        let block = dispatch_block(NODE_SRC, "NetMessageV1::NeighborHosting");
        let gate_pos = block.find("contract_ban_list.is_banned").expect(
            "NeighborHosting overlap-sync block is missing the ban-list egress gate — \
             banned contracts would continue to be pushed to sibling peers",
        );
        let emit_pos = block
            .find("NodeEvent::SyncStateToPeer")
            .expect("NeighborHosting block is missing SyncStateToPeer emit");
        assert!(
            gate_pos < emit_pos,
            "NeighborHosting ban-list gate (offset {gate_pos}) must \
             precede SyncStateToPeer emit (offset {emit_pos})"
        );
    }

    #[test]
    fn interest_sync_summaries_gates_banned_egress() {
        // The stale-summary repair loop lives inside
        // `handle_interest_sync_message` (the helper that the
        // NetMessageV1::InterestSync arm delegates to), not in
        // `process_message` itself. Search the whole file scoped to
        // the `for contract in stale_contracts` loop.
        const NODE_SRC: &str = include_str!("../node.rs");
        let stale_loop = NODE_SRC
            .find("for contract in stale_contracts")
            .expect("node.rs is missing the stale_contracts loop");
        // Bound the search to the loop body — the loop ends when
        // indentation returns to the loop's level. A loose heuristic:
        // take 2_000 bytes after the loop header (the body is short).
        let scan = &NODE_SRC[stale_loop..stale_loop.saturating_add(2_000).min(NODE_SRC.len())];
        let gate_pos = scan.find("contract_ban_list.is_banned").expect(
            "stale-summary repair loop is missing the ban-list egress gate — \
             banned contracts would continue to be pushed to peers that report \
             stale summaries (defeats the Phase 7 wire-boundary drop)",
        );
        let emit_pos = scan
            .find("NodeEvent::SyncStateToPeer")
            .expect("stale-summary loop is missing SyncStateToPeer emit");
        assert!(
            gate_pos < emit_pos,
            "InterestSync ban-list gate (offset {gate_pos}) must \
             precede SyncStateToPeer emit (offset {emit_pos})"
        );
    }

    // End-to-end regression test (May 21 incident pattern) lives in
    // `contract/governance.rs` next to the BanTriggered/BanLifted
    // unit tests, where the test helpers for the state-machine setup
    // (mk_mgr_shared, etc.) already exist. See
    // `governance_to_ban_list_end_to_end` in that module.
}

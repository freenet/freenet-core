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
//!   transaction cleanly. The next request we make for the contract
//!   would be self-blocked at the egress (Phase 7 follow-up); for now
//!   we focus on the receive side.
//! - **Peer-level messages** (ConnectMsg, etc.) — those are the peer
//!   layer's concern, not the contract layer.
//! - **Proactive state egress for already-hosted contracts**
//!   (`NeighborHosting` overlap sync, `InterestSync::Summaries` stale
//!   repair). These paths are also gated (see node.rs) but the gates
//!   live at the egress sites, not here. See issue tracking egress
//!   self-blocking for the full design.
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

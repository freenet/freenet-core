//! Delegate contract subscriptions as **local client demand** (#4669 part 1,
//! #5467 Phase 1).
//!
//! # The defect this closes
//!
//! Before this module, both delegate-subscribe paths — the V1
//! [`OutboundDelegateMsg::SubscribeContractRequest`] handler in
//! [`crate::contract`] and the V2 `subscribe_contract()` host function in
//! [`crate::wasm_runtime::native_api`] — did exactly one thing: insert
//! `(contract_id, delegate_key)` into the process-global
//! [`DELEGATE_SUBSCRIPTIONS`](crate::wasm_runtime::DELEGATE_SUBSCRIPTIONS)
//! registry. Nothing in `ring/` reads that registry. It drives
//! `ContractNotification` delivery and nothing else.
//!
//! So a delegate subscription did **not** set
//! [`contract_in_use`](crate::ring::Ring::contract_in_use), did **not** appear
//! in `contracts_needing_renewal()`, and did **not** raise the contract's
//! eviction tier. A delegate could not keep a contract in the update mesh: it
//! saw remote updates only while the node happened to be subscribed by some
//! other route (a WebSocket client), and the pin silently did not take. That is
//! what blocks freenet/delta#30 and Harvest.
//!
//! # What this adds
//!
//! The **demand half**. It does not replace the reactive half — the
//! `DELEGATE_SUBSCRIPTIONS` insert and `ContractNotification` delivery are
//! untouched, so River's private-room secret rotation
//! (`river/delegates/chat-delegate/src/subscription.rs`) keeps working exactly
//! as before.
//!
//! # Why a synthetic `ClientId` rather than a `has_delegate_subscriptions` term
//!
//! #4669 offers two options and calls them functionally equivalent: a stable
//! synthetic subscriber identity run through the normal client path (@sanity's
//! stated preference), or a new `has_delegate_subscriptions` term on
//! `contract_in_use`. This module implements the first, because the second is
//! the "manually-mirrored counter" shape from
//! `.claude/rules/bug-prevention-patterns.md`: `client_subscriptions` is read by
//! `contract_in_use`, `in_use_contract_ids`, `local_and_downstream_counts` (the
//! eviction ordering key), `contracts_needing_renewal` branches 1 **and** 2,
//! `is_receiving_updates`, `generate_topology_snapshot` and
//! `teardown_evicted_in_use_contract`. A parallel delegate term would have to be
//! added to each, and the next consumer added to `client_subscriptions` would
//! silently not get it. Registering in the one map means every one of those
//! consumers is correct by construction, now and later.
//!
//! # Interaction with the hosting invariants (`.claude/rules/hosting-invariants.md`)
//!
//! - **Invariant 3 (eviction ordering).** `local_and_downstream_counts` reads
//!   `client_subscriptions.len()`, so a delegate pin lands in the **local**
//!   tier — the tier evicted LAST. That is the correct tier (a delegate
//!   subscription genuinely cannot re-home; it is this node's own resident
//!   agent asking) and it is deliberate, but it means an app can pin contracts
//!   into the top tier through its delegate. #5467 open question 1 asks where
//!   that bound lives and explicitly wants a deliberate call rather than a
//!   constant, so **this module imposes no bound** — see the PR body. Today's
//!   de-facto limit is that both subscribe paths refuse a contract the node
//!   does not already hold (`lookup_key` / `resolve_contract_key` must
//!   resolve), so a delegate can only pin what is already local.
//! - **Invariant 2 (demand-driven hosting).** A delegate subscription is real
//!   demand from a real resident component. It is not holding-driven: nothing
//!   here makes a peer host a contract it was not already holding.
//! - **Invariant 3's time-boundedness.** `contract_in_use`'s contract is that
//!   every demand source is time-bounded (clients disconnect; downstream leases
//!   expire). A delegate pin is bounded only by process lifetime and explicit
//!   `UnregisterDelegate` today, because `DELEGATE_SUBSCRIPTIONS` is in-memory
//!   and there is no unsubscribe (#2830) and no sleep/wake horizon yet. Closing
//!   that is #4669 parts 2-4 / #5467 Phase 3; until it lands a delegate pin
//!   lapses on node restart and nowhere else.
//!
//! # No new spawn path
//!
//! Registering demand is the whole change. The existing renewal loop
//! (`Ring::recover_orphaned_subscriptions`, `SUBSCRIPTION_RECOVERY_INTERVAL` =
//! 30 s) already picks up "has client subscriptions but no active network
//! subscription" in `contracts_needing_renewal()` branch 2 and issues the
//! network SUBSCRIBE through the single storm-safe
//! `Ring::spawn_renewal_subscribe_task` path (jitter, per-contract pending
//! dedup, backoff, ban gate, outer-cancel deadline). Adding a second spawn site
//! here would duplicate that scaffolding, which is exactly the drift the "one
//! helper, not two" note on that function exists to prevent.

use std::sync::LazyLock;

use dashmap::DashSet;
use freenet_stdlib::prelude::{ContractInstanceId, ContractKey, DelegateKey};

use crate::client_events::ClientId;
use crate::node::OpManager;

/// Base of the [`ClientId`] range reserved for delegates.
///
/// Real client ids come from a thread-local counter seeded at
/// `1 + thread_index * COUNTER_BLOCK` and incremented by one per connection
/// (`client_events::types::ClientId::next`), so reaching `1 << 63` is not
/// reachable in any real process. Setting the top bit therefore makes a
/// collision between a delegate's synthetic id and a genuine WebSocket client's
/// id **structurally impossible**, not merely unlikely — which matters because
/// a collision would let a client disconnect
/// (`remove_client_from_all_subscriptions`) silently drop a delegate's pin, or
/// a delegate unregister drop a live client's subscription.
const DELEGATE_CLIENT_ID_BASE: usize = 1usize << (usize::BITS - 1);

/// Every synthetic delegate client id minted in this process, for the
/// [`is_delegate_client`] predicate and for diagnostics (#5467 Phase 0 will
/// surface these). Bounded by the number of distinct delegates that have ever
/// subscribed on this node, and entries are removed by
/// [`drop_delegate_demand`].
static MINTED: LazyLock<DashSet<usize>> = LazyLock::new(DashSet::default);

/// The stable synthetic [`ClientId`] standing in for `delegate` as a local
/// subscriber.
///
/// Derived from the delegate key's own bytes rather than from a counter, so it
/// is:
/// - **stable across restarts**, which #4669 part 3 (replay of a delegate's
///   persisted subscription set without running its WASM) will need;
/// - **stateless**, so nothing has to be allocated, torn down, or kept in sync
///   with the delegate lifecycle.
///
/// Collision analysis: 63 bits of BLAKE3 output. Collision with a real client
/// id is impossible (see [`DELEGATE_CLIENT_ID_BASE`]). Collision between two
/// distinct delegates is a birthday event at ~2^31.5 delegates on one node, and
/// its consequence would be two delegates sharing one demand registration —
/// each keeping the other's contracts pinned — not a correctness or
/// authorization failure. Neither is reachable in practice.
pub(crate) fn client_id_for(delegate: &DelegateKey) -> ClientId {
    let hash = blake3::hash(delegate.bytes());
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&hash.as_bytes()[..8]);
    // Clear the top bit of the hash, then set it from the reserved base, so the
    // result is always in the reserved half regardless of the hash's own MSB.
    let id = DELEGATE_CLIENT_ID_BASE | ((u64::from_le_bytes(buf) as usize) & !DELEGATE_CLIENT_ID_BASE);
    MINTED.insert(id);
    ClientId(id)
}

/// Whether `client_id` is a delegate's synthetic subscriber identity rather
/// than a real WebSocket client.
///
/// Checks the reserved range, not the minted set: the range is the invariant,
/// and the answer must not depend on whether this process happens to have
/// minted that particular id yet.
pub(crate) fn is_delegate_client(client_id: ClientId) -> bool {
    usize::from(client_id) >= DELEGATE_CLIENT_ID_BASE
}

/// Register `delegate`'s subscription to `contract` as local client demand.
///
/// Idempotent: `add_client_subscription` inserts into a `HashSet`, so a
/// repeated subscribe by the same delegate to the same contract is a no-op.
///
/// Deliberately does **not** call `InterestManager::add_local_client`, unlike
/// the WebSocket client path. Two reasons, and both matter:
/// 1. `add_local_client` is a non-idempotent counter, while both delegate
///    subscribe paths are idempotent by construction — so pairing them would
///    inflate `local_client_count` on every repeat subscribe.
/// 2. It would buy nothing. `InterestManager::is_interested` is
///    `hosting || local_client_count > 0 || downstream_subscriber_count > 0`,
///    and `hosting` is already true for every contract this node holds
///    (`register_local_hosting` runs on the PUT and GET store paths) — which is
///    a precondition of subscribing at all, since both subscribe paths refuse a
///    contract that is not local. So the contract already participates in
///    anti-entropy.
///
/// The matching consequence is that [`drop_delegate_demand`] must NOT call
/// `remove_local_client` either — doing so would decrement a count this module
/// never incremented, stealing it from a real WebSocket client.
///
/// One asymmetry follows from this and is deliberate. A delegate registration
/// counts in `client_subscriptions.len()` but not in
/// `InterestManager::local_client_count`, so the subscriber-primary eviction's
/// `remove_evicted_in_use(key, local_client_count, downstream)` — which passes
/// the count `teardown_evicted_in_use_contract` read out of
/// `client_subscriptions` — over-decrements `local_client_count` by the number
/// of delegate registrations on that key. It is harmless: that call site fires
/// only while the contract is being shed entirely, and the very next step in
/// the same sweep is `unregister_local_hosting`, which clears the contract's
/// interest regardless (`ring.rs`, the eviction sweep; likewise the PUT and GET
/// eviction handlers). `remove_local_client` saturates at zero, so nothing
/// underflows.
pub(crate) fn register_subscription(
    op_manager: &OpManager,
    delegate: &DelegateKey,
    contract: &ContractKey,
) {
    let client_id = client_id_for(delegate);
    op_manager
        .ring
        .add_client_subscription(contract.id(), client_id);
    tracing::debug!(
        delegate = %delegate,
        contract = %contract,
        %client_id,
        "delegate subscription registered as local client demand"
    );
}

/// Drop the demand `delegate` holds on `contract`, if any, and collapse the
/// upstream subscription when nothing else is left interested.
///
/// The collapse half is not optional. Dropping the registration alone leaves
/// the node holding an upstream lease for a contract nothing wants: renewal is
/// gated on `contract_in_use`, so the lease does eventually lapse on its own,
/// but until it does the upstream peer keeps us in its fan-out. The
/// client-disconnect path (`client_events.rs`, `ClientRequest::Disconnect`)
/// collapses immediately for the same reason, and this path is the delegate's
/// equivalent, so it uses the same gate.
pub(crate) fn drop_subscription(
    op_manager: &std::sync::Arc<OpManager>,
    delegate: &DelegateKey,
    contract: &ContractKey,
) {
    let client_id = client_id_for(delegate);
    op_manager
        .ring
        .remove_client_subscription(contract.id(), client_id);
    tracing::debug!(
        delegate = %delegate,
        contract = %contract,
        %client_id,
        "delegate subscription demand dropped"
    );
    collapse_if_no_interest(op_manager, contract);
}

/// Send `Unsubscribe` upstream for `contract` when nothing on this node is
/// interested in it any more.
///
/// The gate is the reconcile controller's own
/// (`reconcile_wants_collapse` = `!contract_in_use`), not a local
/// re-derivation of it — a second copy of that predicate is exactly the
/// mirrored-condition drift `.claude/rules/bug-prevention-patterns.md` warns
/// about.
fn collapse_if_no_interest(op_manager: &std::sync::Arc<OpManager>, contract: &ContractKey) {
    if !op_manager.reconcile_wants_collapse(
        contract,
        crate::node::network_status::ReconcileShadowSite::Collapse,
    ) {
        return;
    }
    let op_mgr = op_manager.clone();
    let contract = *contract;
    crate::config::GlobalExecutor::spawn(async move {
        op_mgr.send_unsubscribe_upstream(&contract).await;
    });
}

/// Drop every demand registration held by `delegate` and collapse the upstream
/// subscription for any contract left with no remaining interest.
///
/// This mirrors the WebSocket `ClientRequest::Disconnect` path
/// (`client_events.rs`) minus its `InterestManager::remove_local_client` call —
/// see [`register_subscription`] for why that pairing is deliberately absent.
pub(crate) fn drop_delegate_demand(op_manager: &std::sync::Arc<OpManager>, delegate: &DelegateKey) {
    let client_id = client_id_for(delegate);
    MINTED.remove(&usize::from(client_id));
    let result = op_manager
        .ring
        .remove_client_from_all_subscriptions(client_id);
    if result.affected_contracts.is_empty() {
        return;
    }
    tracing::debug!(
        delegate = %delegate,
        subscriptions_cleaned = result.affected_contracts.len(),
        "dropped delegate subscription demand"
    );
    // Send Unsubscribe upstream for contracts with no remaining interest —
    // the same reconcile-controller collapse gate the client-disconnect path
    // uses (`reconcile_wants_collapse` = `!contract_in_use`).
    for contract in &result.affected_contracts {
        collapse_if_no_interest(op_manager, contract);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn delegate_key(seed: u8) -> DelegateKey {
        DelegateKey::new(
            [seed; 32],
            freenet_stdlib::prelude::CodeHash::from_code(&[seed]),
        )
    }

    #[test]
    fn delegate_client_ids_are_in_the_reserved_range() {
        for seed in 0u8..32 {
            let id = client_id_for(&delegate_key(seed));
            assert!(
                usize::from(id) >= DELEGATE_CLIENT_ID_BASE,
                "delegate client id {id} must be in the reserved half so it can \
                 never collide with a real WebSocket ClientId"
            );
            assert!(is_delegate_client(id));
        }
    }

    #[test]
    fn real_client_ids_are_not_delegate_client_ids() {
        // The counter-derived ids a real connection gets.
        for _ in 0..64 {
            assert!(!is_delegate_client(ClientId::next()));
        }
        assert!(!is_delegate_client(ClientId::FIRST));
    }

    #[test]
    fn delegate_client_id_is_stable_and_distinct() {
        let a = delegate_key(1);
        let b = delegate_key(2);
        assert_eq!(
            client_id_for(&a),
            client_id_for(&a),
            "the synthetic id must be stable for a given delegate — #4669 part 3 \
             replays a persisted subscription set against it"
        );
        assert_ne!(client_id_for(&a), client_id_for(&b));
    }
}

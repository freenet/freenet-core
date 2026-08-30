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
//! `is_receiving_updates`, `generate_topology_snapshot`,
//! `teardown_evicted_in_use_contract` and governance's `beneficiary_counts`. A
//! parallel delegate term would have to be added to each, and the next consumer
//! added to `client_subscriptions` would silently not get it. Registering in the
//! one map means every one of those consumers is correct by construction, now
//! and later.
//!
//! That breadth cuts both ways and the governance one is worth stating outright
//! rather than leaving as a surprise: `beneficiary_counts` derives a contract's
//! governance BENEFIT from `client_subscriptions.len()`, so a delegate pin also
//! raises the contract's benefit score and makes a resource-usage ban less
//! likely. That is the intended reading — a delegate subscription is real local
//! demand and should count as a beneficiary exactly like a WebSocket client —
//! but it does mean an app can raise its own contracts' benefit through its
//! delegate, which is the same unbounded-pinning surface #5467 open question 1
//! is about, reached by a second route.
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
//!   de-facto limit is that a delegate can only pin a contract this node is
//!   already HOSTING (see [`register_subscription`], which is gated more
//!   tightly than the subscribe paths themselves). It cannot enumerate the
//!   network and pin it; it can still pin everything its own app has PUT
//!   locally, which is the hole the PR body names.
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

use freenet_stdlib::prelude::{ContractKey, DelegateKey};

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
    let id =
        DELEGATE_CLIENT_ID_BASE | ((u64::from_le_bytes(buf) as usize) & !DELEGATE_CLIENT_ID_BASE);
    ClientId(id)
}

/// Whether `client_id` is a delegate's synthetic subscriber identity rather
/// than a real WebSocket client.
///
/// Test-only: nothing in production needs to tell the two apart, because the
/// point of the reserved range is that neither side ever has to. It exists so
/// [`DELEGATE_CLIENT_ID_BASE`]'s no-collision claim is asserted rather than
/// only asserted in prose. Un-gate it when a real consumer appears (#5467
/// Phase 0's per-delegate diagnostics is the likely one).
#[cfg(test)]
fn is_delegate_client(client_id: ClientId) -> bool {
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
/// Gated on the node actually HOSTING the contract, which is a stricter test
/// than the one the two subscribe paths use to decide whether the subscribe
/// succeeds. They gate on the contract *resolving* (`lookup_key` /
/// `resolve_contract_key`), which reads the contract store's code index — and
/// that index outlives the hosting cache entry during the window between a
/// contract being evicted and its disk reclamation completing.
///
/// Registering demand in that window would be actively harmful, not merely
/// useless. `contracts_needing_renewal` branch 2 resolves the instance id back
/// to a `ContractKey` **through the hosting cache**, so a contract absent from
/// it is never renewed and no network SUBSCRIBE is ever issued; meanwhile
/// `reclaim_evicted_contract` early-returns on `contract_in_use`, so the
/// pending reclamation is blocked for the life of the process. The result is a
/// pin that neither fetches the contract nor releases it.
///
/// So this returns `false` and registers nothing rather than creating that
/// state. The subscribe itself still succeeds and the notification hook is
/// still recorded, so nothing regresses against the previous behavior — but the
/// delegate gets no pin, which is the same silent no-pin outcome #5467 exists
/// to eliminate. The real fix is for a subscribe to BOOTSTRAP a contract the
/// node does not hold, via a network GET; that needs `perform_contract_get` to
/// reach the network at all (today it is a bare local `state_store.get`) and is
/// the follow-up PR. Until then this is the honest interim: no pin is better
/// than a stuck one.
///
/// Note it narrows the window rather than closing it — a contract evicted
/// immediately after this check lands in the same state. That residual is the
/// same one the bootstrap work closes.
pub(crate) fn register_subscription(
    op_manager: &OpManager,
    delegate: &DelegateKey,
    contract: &ContractKey,
) -> bool {
    if !op_manager.ring.is_hosting_contract(contract) {
        tracing::debug!(
            delegate = %delegate,
            contract = %contract,
            "delegate subscribed to a contract this node resolves but does not \
             host; recording the notification hook without demand — see \
             `delegate_demand::register_subscription`"
        );
        return false;
    }
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
    true
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

    // =====================================================================
    // The falsifier: real `OpManager` over a real `Ring`.
    //
    // These are the three assertions #4669's own Testing section asks for
    // ("Delegate subscribe sets `contract_in_use`, appears in
    // `contracts_needing_renewal`, exempts from eviction"). They fail against
    // main, because on main a delegate subscribe touches only
    // `DELEGATE_SUBSCRIPTIONS` and nothing in `ring/` reads that map.
    //
    // They are in-crate rather than in `crates/core/tests/` on purpose, and it
    // is worth writing down why, because "add a multi-node test" is the
    // reflex here and it does not work for this defect:
    //
    //  - `mod ring` and `mod node` are crate-private, and `TestContext` hands
    //    an integration test only a label, a temp dir and a WebSocket port
    //    (`test_utils.rs:1245`), so no integration test can observe demand.
    //    `NodeQuery::SubscriptionInfo` looks like the exception and is not: it
    //    reports the executor's `update_notifications` map
    //    (`contract/executor.rs::get_subscription_info`), i.e. WebSocket
    //    notification channels, not ring demand.
    //  - Inferring demand from update delivery does not work either. Live
    //    fan-out targets come from `advertised_cohost_pub_keys`
    //    (`operations/update.rs`), and a peer advertises as a co-host the
    //    moment it CACHES the contract (`register_local_hosting`, called from
    //    the every-hop PUT/GET store path) with no `contract_in_use` check. So
    //    a peer receives the update whether or not demand registered, and a
    //    "did the notification arrive" test passes on main.
    //
    // The multi-node test that DOES exist for this change
    // (`crates/core/tests/operations.rs`) is a no-regression guard for the
    // notification path River depends on, and says in its own doc comment that
    // it cannot discriminate this fix.
    // =====================================================================

    use crate::ring::cost_pressure_seam_tests::seam_fixture;

    fn contract_key(seed: u8) -> ContractKey {
        ContractKey::from_id_and_code(
            freenet_stdlib::prelude::ContractInstanceId::new([seed; 32]),
            freenet_stdlib::prelude::CodeHash::new([seed.wrapping_add(1); 32]),
        )
    }

    #[tokio::test(start_paused = true)]
    async fn delegate_subscription_registers_demand_in_the_local_tier() {
        let fixture = seam_fixture("delegate-demand-4669-registers").await;
        let op_manager = fixture.op_manager.clone();
        let ring = &op_manager.ring;

        let key = contract_key(11);
        // Host it the way a PUT would, so `contracts_needing_renewal` branch 2
        // can resolve the instance id back to a `ContractKey` (it looks the key
        // up in the hosting cache).
        let _ = ring.host_contract(
            key,
            121,
            crate::ring::AccessType::Put,
            crate::ring::HostingCause::Other,
        );

        assert!(
            !ring.contract_in_use(&key),
            "precondition: hosting a contract is not by itself demand — if this \
             fires, the fixture is granting demand from somewhere else and the \
             rest of this test proves nothing"
        );
        assert!(
            !ring.contracts_needing_renewal().contains(&key),
            "precondition: a hosted, undemanded contract must not be renewed \
             (this also rules out the `has_recent_local_client_access` branch, \
              which would otherwise make the assertions below vacuous)"
        );
        assert_eq!((0, 0), ring.local_and_downstream_counts(&key));

        register_subscription(&op_manager, &delegate_key(7), &key);

        assert!(
            ring.contract_in_use(&key),
            "#4669: a delegate subscription must count as demand"
        );
        assert!(
            ring.contracts_needing_renewal().contains(&key),
            "#4669: demand must put the contract in the renewal set, which is \
             what keeps it in the update mesh"
        );
        assert_eq!(
            (1, 0),
            ring.local_and_downstream_counts(&key),
            "hosting-invariants invariant 3: a delegate pin is a LOCAL \
             subscription — the tier evicted LAST — not a downstream one"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn demand_is_not_registered_for_a_contract_the_node_does_not_host() {
        let fixture = seam_fixture("delegate-demand-4669-unhosted").await;
        let op_manager = fixture.op_manager.clone();
        let ring = &op_manager.ring;

        // Deliberately NOT hosted. The two subscribe paths would still let a
        // delegate subscribe here, because they gate on the contract store's
        // code index, which outlives the hosting cache entry across the
        // eviction-to-reclamation window.
        let key = contract_key(14);
        assert!(!ring.is_hosting_contract(&key), "precondition");

        assert!(
            !register_subscription(&op_manager, &delegate_key(61), &key),
            "registering demand for a contract the node does not host creates a \
             pin that can never be renewed (`contracts_needing_renewal` branch 2 \
             resolves through the hosting cache) and never reclaimed \
             (`reclaim_evicted_contract` early-returns on `contract_in_use`)"
        );
        assert!(!ring.contract_in_use(&key));
        assert_eq!((0, 0), ring.local_and_downstream_counts(&key));
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_one_delegates_demand_leaves_another_delegates_intact() {
        let fixture = seam_fixture("delegate-demand-4669-independent").await;
        let op_manager = fixture.op_manager.clone();
        let ring = &op_manager.ring;

        let key = contract_key(12);
        let _ = ring.host_contract(
            key,
            121,
            crate::ring::AccessType::Put,
            crate::ring::HostingCause::Other,
        );

        let first = delegate_key(21);
        let second = delegate_key(22);
        register_subscription(&op_manager, &first, &key);
        register_subscription(&op_manager, &second, &key);
        assert_eq!((2, 0), ring.local_and_downstream_counts(&key));

        drop_subscription(&op_manager, &first, &key);
        assert!(
            ring.contract_in_use(&key),
            "one delegate unsubscribing must not drop another delegate's pin — \
             the two hold distinct synthetic client ids"
        );
        assert_eq!((1, 0), ring.local_and_downstream_counts(&key));

        drop_subscription(&op_manager, &second, &key);
        assert!(
            !ring.contract_in_use(&key),
            "the last delegate unsubscribing must release the pin, or the \
             contract is pinned forever with nothing wanting it"
        );
        assert!(!ring.contracts_needing_renewal().contains(&key));
    }

    #[tokio::test(start_paused = true)]
    async fn unregistering_a_delegate_drops_every_contract_it_pinned() {
        let fixture = seam_fixture("delegate-demand-4669-unregister").await;
        let op_manager = fixture.op_manager.clone();
        let ring = &op_manager.ring;

        let pinned = [contract_key(31), contract_key(32), contract_key(33)];
        for key in pinned {
            let _ = ring.host_contract(
                key,
                121,
                crate::ring::AccessType::Put,
                crate::ring::HostingCause::Other,
            );
        }

        let delegate = delegate_key(41);
        // A second delegate keeps its own pin on one of them, to prove the
        // teardown is scoped to the delegate being unregistered.
        let survivor = delegate_key(42);
        for key in pinned {
            register_subscription(&op_manager, &delegate, &key);
        }
        register_subscription(&op_manager, &survivor, &pinned[2]);

        drop_delegate_demand(&op_manager, &delegate);

        assert!(!ring.contract_in_use(&pinned[0]));
        assert!(!ring.contract_in_use(&pinned[1]));
        assert!(
            ring.contract_in_use(&pinned[2]),
            "unregistering one delegate must not drop a DIFFERENT delegate's \
             pin on the same contract"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn repeated_subscribe_by_the_same_delegate_is_idempotent() {
        let fixture = seam_fixture("delegate-demand-4669-idempotent").await;
        let op_manager = fixture.op_manager.clone();
        let ring = &op_manager.ring;

        let key = contract_key(13);
        let _ = ring.host_contract(
            key,
            121,
            crate::ring::AccessType::Put,
            crate::ring::HostingCause::Other,
        );

        let delegate = delegate_key(51);
        for _ in 0..5 {
            register_subscription(&op_manager, &delegate, &key);
        }
        assert_eq!(
            (1, 0),
            ring.local_and_downstream_counts(&key),
            "a delegate re-subscribing must not inflate the local-subscriber \
             count — that count is the eviction ordering key, so an inflated \
             one would silently outrank real demand"
        );

        // ...and a single drop must therefore be enough to release it.
        drop_subscription(&op_manager, &delegate, &key);
        assert!(!ring.contract_in_use(&key));
    }

    // =====================================================================
    // Source-scrape pins: BOTH subscribe paths must reach this module.
    //
    // There are two delegate-subscribe entry points that converge on
    // `DELEGATE_SUBSCRIPTIONS` (the V1 `SubscribeContractRequest` arm and the
    // V2 `subscribe_contract()` host function), and they have to converge on
    // the demand registration too. A future change that adds a third, or
    // rewrites one of the two, has no compile-time reason to keep the demand
    // call — which is exactly the "manually-inlined originator side effects"
    // shape in `.claude/rules/bug-prevention-patterns.md` (#3851, #4223): the
    // omission is silent, the subscribe still reports success, and every unit
    // test still passes. These pins fail closed instead.
    // =====================================================================

    #[test]
    fn v1_subscribe_arm_registers_demand() {
        const SOURCE: &str = include_str!("../contract.rs");
        let arm = SOURCE
            .find("for req in subscribe_requests")
            .expect("the V1 SubscribeContractRequest loop must still exist");
        let body = &SOURCE[arm..arm + 3000.min(SOURCE.len() - arm)];
        assert!(
            body.contains("DELEGATE_SUBSCRIPTIONS"),
            "the V1 subscribe arm must still record the notification hook — \
             this change ADDS demand, it does not replace notification \
             delivery, which River's private-room secret rotation depends on"
        );
        assert!(
            body.contains("delegate_demand::register_subscription("),
            "the V1 SubscribeContractRequest arm must register demand \
             (#4669). Without it the subscribe succeeds, the delegate is \
             notified while some other route keeps the node subscribed, and \
             the pin silently does not take — which is the entire defect."
        );
    }

    #[test]
    fn both_runtime_executor_constructors_install_the_subscribe_callback() {
        const SOURCE: &str = include_str!("executor/runtime.rs");
        let installs = SOURCE.matches("set_delegate_subscribe_callback(").count();
        assert_eq!(
            2, installs,
            "every `Executor<Runtime>` constructor that installs the state-write \
             and state-admit callbacks must also install the delegate-subscribe \
             callback, or a V2 delegate subscribing on an executor built by the \
             constructor that forgot it registers no demand — silently. If a \
             constructor was added or removed, update this pin deliberately \
             rather than loosening it."
        );
        assert_eq!(
            installs,
            SOURCE
                .matches("delegate_demand::register_subscription(")
                .count(),
            "each installed subscribe callback must delegate to \
             `delegate_demand::register_subscription` — the V1 arm calls the \
             same helper, and the two paths converging on one helper is what \
             stops them drifting"
        );
    }

    #[test]
    fn v2_subscribe_host_fn_registers_demand_only_after_a_successful_resolve() {
        const SOURCE: &str = include_str!("../wasm_runtime/native_api.rs");
        let start = SOURCE
            .find("fn subscribe_contract_sync(")
            .expect("the V2 subscribe host function must still exist");
        let body = &SOURCE[start..start + 2000.min(SOURCE.len() - start)];

        let resolve = body
            .find("resolve_contract_key(")
            .expect("V2 subscribe must resolve the contract key before anything else");
        let register = body
            .find("delegate_subscribe_callback")
            .expect("V2 subscribe must invoke the demand callback (#4669)");
        assert!(
            resolve < register,
            "the demand callback must run only after the contract key resolves \
             — registering demand for a contract this node does not hold would \
             pin a key no hosting-cache lookup can match, and the delegate is \
             told the subscribe FAILED, so it holds no record to unsubscribe with"
        );
    }

    #[test]
    fn unregistering_a_delegate_drops_its_demand() {
        const SOURCE: &str = include_str!("executor/runtime/delegates.rs");
        assert!(
            SOURCE.contains("delegate_demand::drop_delegate_demand("),
            "`UnregisterDelegate` cleanup must drop the delegate's demand as \
             well as its notification hooks. There is no unsubscribe (#2830) \
             and `DELEGATE_SUBSCRIPTIONS` is in-memory, so a demand record left \
             behind here is a pin nothing can release for the life of the process."
        );
    }
}

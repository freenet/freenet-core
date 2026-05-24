//! Structural pin tests for the SUBSCRIBE module.
//!
//! Anchors: the unsubscribe inbound handler shape, the SUBSCRIBE
//! dispatch site in `node.rs`, the wire-format compatibility of
//! `SubscribeMsg::ForwardingAck`, and the sub-op GET migration.

use super::*;

/// Pin: `handle_unsubscribe_inbound` preserves the four behavioral
/// branches inherited from the legacy
/// `process_message::Unsubscribe` arm, plus the counter-symmetry
/// guard on the global downstream counter.
#[test]
fn handle_unsubscribe_inbound_preserves_legacy_branches() {
    const SOURCE: &str = include_str!("../subscribe.rs");
    let fn_start = SOURCE
        .find("pub(crate) async fn handle_unsubscribe_inbound(")
        .expect(
            "handle_unsubscribe_inbound not found in subscribe.rs — \
             rename or removal must trip this pin",
        );
    let next_item = SOURCE[fn_start..]
        .find("\n#[cfg(test)]")
        .expect("subsequent item anchor not found in subscribe.rs");
    let body = &SOURCE[fn_start..fn_start + next_item];

    // Branch (a): contract-not-found → log + early return without
    // mutating downstream state.
    assert!(
        body.contains("Contract not found locally, ignoring unsubscribe"),
        "handle_unsubscribe_inbound must keep the contract-not-found \
         early-return branch — silent drop is the expected legacy \
         behavior when the relay doesn't host the contract"
    );

    // Branch (b): contract lookup error → log + early return.
    assert!(
        body.contains("Contract lookup failed while handling unsubscribe"),
        "handle_unsubscribe_inbound must keep the contract-lookup-error \
         branch — `has_contract` errors must not corrupt downstream state"
    );

    // Branch (c): unresolvable sender peer → warn, skip removal.
    assert!(
        body.contains("could not resolve sender peer, downstream entry not removed"),
        "handle_unsubscribe_inbound must keep the unresolvable-sender warn \
         branch — silent removal of an arbitrary peer would corrupt \
         downstream-subscriber accounting"
    );

    // Branch (d): happy path — remove downstream + interest, conditionally
    // propagate upstream.
    assert!(
        body.contains("remove_downstream_subscriber(&key, peer)"),
        "handle_unsubscribe_inbound must call `ring.remove_downstream_subscriber` \
         to remove the sender from the per-contract downstream list"
    );
    assert!(
        body.contains("remove_peer_interest(&key, peer)"),
        "handle_unsubscribe_inbound must call `interest_manager.remove_peer_interest` \
         to drop the sender from the per-peer interest registry"
    );
    assert!(
        body.contains("should_unsubscribe_upstream(&key)"),
        "handle_unsubscribe_inbound must gate the upstream propagation on \
         `ring.should_unsubscribe_upstream` — chain-propagating without \
         the gate would over-unsubscribe contracts that still have \
         downstream subscribers"
    );
    assert!(
        body.contains("send_unsubscribe_upstream(&key)"),
        "handle_unsubscribe_inbound must call `op_manager.send_unsubscribe_upstream` \
         when interest hits zero — without this the chain breaks and \
         intermediate peers stay subscribed forever"
    );

    // Counter-symmetry pin: the legacy code only decremented the
    // global downstream counter when the per-contract removal actually
    // succeeded (was_downstream || was_interested). Re-introducing an
    // unconditional decrement would underflow the gauge.
    assert!(
        body.contains("was_downstream || was_interested"),
        "handle_unsubscribe_inbound must gate the global downstream-counter \
         decrement on `was_downstream || was_interested` to stay in sync \
         with `register_downstream_subscriber`'s increment"
    );
}

/// Pin: the SUBSCRIBE dispatch site in `node.rs` MUST route Unsubscribe to
/// `handle_unsubscribe_inbound`. Mirrors the structural pin in
/// `node.rs::tests::callback_forward_tests::subscribe_branch_dispatches_relay_driver`
/// but anchored from this side so a refactor that renames either side trips
/// the guard.
#[test]
fn subscribe_dispatch_routes_unsubscribe_to_inbound_handler() {
    const SOURCE: &str = include_str!("../../node.rs");
    let anchor = "NetMessageV1::Subscribe(ref op) => {";
    let branch_start = SOURCE
        .find(anchor)
        .expect("SUBSCRIBE branch not found in node.rs");
    let next_variant = "// Non-transactional message types:";
    let window_end = SOURCE[branch_start..]
        .find(next_variant)
        .expect("could not find end of SUBSCRIBE arm")
        + branch_start;
    let window = &SOURCE[branch_start..window_end];

    assert!(
        window.contains("handle_unsubscribe_inbound("),
        "SUBSCRIBE dispatch must call `handle_unsubscribe_inbound` for \
         `SubscribeMsg::Unsubscribe` — without this the wire variant \
         becomes a silent no-op"
    );
    assert!(
        window.contains("SubscribeMsg::Unsubscribe {"),
        "SUBSCRIBE dispatch must destructure the Unsubscribe variant to \
         extract `id` and `instance_id` for the inbound handler"
    );
}

/// Pin: `finalize_originator_subscribe` MUST call all six originator
/// finalization side effects. The hand-inlined sequence at the task-per-tx
/// driver's `ReplyClass::Subscribed` branch previously omitted
/// `fetch_contract_if_missing` and `announce_contract_hosted`, causing
/// issue #4223 (subscribed peers returning `get_not_found` on the same
/// contract). Each missing call has a documented production failure
/// mode — if any is dropped, this test fails and points at the issue.
#[test]
fn finalize_originator_subscribe_contains_all_required_side_effects() {
    const SOURCE: &str = include_str!("../subscribe.rs");
    let fn_start = SOURCE
        .find("pub(super) async fn finalize_originator_subscribe(")
        .expect(
            "finalize_originator_subscribe not found in subscribe.rs — \
             rename or removal must trip this pin (issue #4223)",
        );
    // Anchor end: scan to the next top-level `pub(` item declaration.
    let body_after = &SOURCE[fn_start..];
    let next_pub = body_after[1..]
        .find("\npub(")
        .expect("no subsequent pub item found after finalize_originator_subscribe");
    let body = &body_after[..1 + next_pub];

    // (1) upstream-peer registration → enables `send_unsubscribe_upstream` (#3874).
    assert!(
        body.contains("register_peer_interest"),
        "finalize_originator_subscribe must register the responding peer as \
         upstream interest — without it `send_unsubscribe_upstream` cannot \
         find the peer to notify on client disconnect (#3874)"
    );
    // (2) install lease in active_subscriptions.
    assert!(
        body.contains("ring.subscribe(key)"),
        "finalize_originator_subscribe must call `ring.subscribe` to install \
         the lease in `active_subscriptions` — without it the contract is \
         not picked up by `contracts_needing_renewal` and the subscription \
         silently dies at TTL expiry (#3851)"
    );
    // (3) clear pending backoff state.
    assert!(
        body.contains("complete_subscription_request(&key, true)"),
        "finalize_originator_subscribe must call `complete_subscription_request` \
         with success=true to clear the pending mark and reset backoff"
    );
    // (4) fetch contract body if missing → THIS is the core #4223 fix.
    assert!(
        body.contains("fetch_contract_if_missing"),
        "finalize_originator_subscribe MUST call `fetch_contract_if_missing` \
         so the originator has the contract body locally and can answer \
         subsequent GETs from local state instead of returning NotFound \
         (#4223 — 37% of GETs through subscriber peers were failing)"
    );
    // (5) announce_contract_hosted → tells neighbors to include us in UPDATE broadcasts.
    assert!(
        body.contains("announce_contract_hosted"),
        "finalize_originator_subscribe MUST call `announce_contract_hosted` \
         so neighbors include us as an UPDATE broadcast target — without \
         this, UPDATEs may not reach the subscriber even after the contract \
         body is local (#3851)"
    );
    // (6) add_local_client gated on !is_renewal.
    assert!(
        body.contains("add_local_client(&key)"),
        "finalize_originator_subscribe must call `add_local_client` so \
         inbound ChangeInterests for this contract get processed"
    );
    assert!(
        body.contains("!is_renewal"),
        "finalize_originator_subscribe must gate `add_local_client` on \
         `!is_renewal` — counter increments on every renewal cycle would \
         leak the local_client_count gauge unboundedly"
    );
}

/// Pin: the task-per-tx subscribe driver's `ReplyClass::Subscribed`
/// branch MUST delegate to `finalize_originator_subscribe`. Inlining
/// the side effects directly at the branch is what caused issue #4223
/// — the original inline sequence was missing
/// `fetch_contract_if_missing` and `announce_contract_hosted`. By
/// pinning the call site to the helper, any future refactor that
/// reverts to inlining will trip this test.
#[test]
fn drive_client_subscribe_inner_calls_finalize_helper_on_subscribed() {
    const SOURCE: &str = include_str!("op_ctx_task.rs");
    let fn_start = SOURCE
        .find("async fn drive_client_subscribe_inner(")
        .expect("drive_client_subscribe_inner not found");
    let branch_anchor = "ReplyClass::Subscribed { key } =>";
    let branch_start = SOURCE[fn_start..]
        .find(branch_anchor)
        .expect("ReplyClass::Subscribed branch not found in drive_client_subscribe_inner")
        + fn_start;
    let branch_end = SOURCE[branch_start..]
        .find("ReplyClass::NotFound =>")
        .expect("end of Subscribed branch not found")
        + branch_start;
    let branch = &SOURCE[branch_start..branch_end];

    assert!(
        branch.contains("finalize_originator_subscribe"),
        "Subscribed branch must delegate to `finalize_originator_subscribe` \
         — inlining the side effects is what caused #4223 (missing \
         fetch_contract_if_missing + announce_contract_hosted). Keep the \
         helper as the single source of truth."
    );

    // Negative pins: the inlined calls must NOT come back. If a future
    // refactor re-inlines `ring.subscribe(key)` etc. directly into this
    // branch instead of going through the helper, the helper might not
    // be called at all and we'd silently regress.
    assert!(
        !branch.contains("op_manager.ring.subscribe(key)"),
        "Subscribed branch must not call `ring.subscribe` directly — go \
         through `finalize_originator_subscribe` so the fetch + announce \
         steps stay grouped with the lease install"
    );
    assert!(
        !branch.contains("op_manager.ring.complete_subscription_request"),
        "Subscribed branch must not call `complete_subscription_request` \
         directly — go through `finalize_originator_subscribe`"
    );
}

/// Pin: `SubscribeMsg::ForwardingAck` wire-format compatibility. The
/// variant has no production reader, but a serde round-trip guards
/// against bincode-discriminant shifts that would break cross-version
/// compatibility.
#[test]
fn subscribe_forwarding_ack_serde_roundtrip() {
    let id = Transaction::new::<SubscribeMsg>();
    let instance_id = ContractInstanceId::new([42; 32]);
    let msg = SubscribeMsg::ForwardingAck { id, instance_id };

    let serialized = bincode::serialize(&msg).expect("serialize");
    let deserialized: SubscribeMsg = bincode::deserialize(&serialized).expect("deserialize");

    match deserialized {
        SubscribeMsg::ForwardingAck {
            id: deser_id,
            instance_id: deser_iid,
        } => {
            assert_eq!(deser_id, id);
            assert_eq!(deser_iid, instance_id);
        }
        other @ SubscribeMsg::Request { .. }
        | other @ SubscribeMsg::Response { .. }
        | other @ SubscribeMsg::Unsubscribe { .. } => {
            panic!("Expected ForwardingAck, got {other}")
        }
    }
}

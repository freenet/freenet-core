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

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

/// Extract the comment-stripped body of the named function from `source`
/// (brace-counted from the first `{` after the anchor). Panics if the anchor
/// is absent or the braces never balance. Comment stripping mirrors the pattern
/// used by the op_ctx_task pins so doc strings mentioning an API name as context
/// do not contaminate the substring/ordering scans.
fn extract_fn_body_stripped(source: &str, anchor: &str) -> String {
    let fn_start = source
        .find(anchor)
        .unwrap_or_else(|| panic!("`{anchor}` not found — rename or removal must trip this pin"));
    let body_open = source[fn_start..]
        .find('{')
        .expect("function body open brace not found")
        + fn_start;
    let mut depth = 0i32;
    let mut body_end = body_open;
    for (i, c) in source[body_open..].char_indices() {
        match c {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    body_end = body_open + i + 1;
                    break;
                }
            }
            _ => {}
        }
    }
    assert!(
        body_end > body_open,
        "could not find balanced closing brace for `{anchor}` — body anchor is broken"
    );
    source[fn_start..body_end]
        .lines()
        .map(|line| match line.find("//") {
            Some(idx) => &line[..idx],
            None => line,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Pin: the host-formation side-effect sequence MUST contain all five shared
/// side effects, AND the fetch must come before the conditional announce.
///
/// Piece D (computed-upstream chains) moved these side effects out of
/// `finalize_originator_subscribe` into the shared `finalize_host_subscribe`
/// helper (so a chain host runs the identical sequence). This pin now scrapes
/// `finalize_host_subscribe` — the new home of the #4223/#3851 bug class — and a
/// sibling pin (`finalize_originator_subscribe_delegates_and_adds_local_client`)
/// asserts the originator delegates to it. The hand-inlined sequence at the
/// task-per-tx driver's `ReplyClass::Subscribed` branch previously omitted
/// `fetch_contract_if_missing` and `announce_contract_hosted`, causing issue
/// #4223 (subscribed peers returning `get_not_found`). Each missing call has a
/// documented production failure mode — if any is dropped, this test fails and
/// points at the issue.
///
/// Assertion shape: substring-match on the API surface (e.g. `ring.subscribe(`)
/// rather than on full call expressions with variable names, so a variable
/// rename does not silently break the pin while still passing the bug class.
#[test]
fn finalize_host_subscribe_contains_all_required_side_effects() {
    const SOURCE: &str = include_str!("../subscribe.rs");
    let body = extract_fn_body_stripped(SOURCE, "pub(super) async fn finalize_host_subscribe(");
    let body = body.as_str();

    // (1) upstream-peer registration → enables `send_unsubscribe_upstream` (#3874).
    assert!(
        body.contains("register_peer_interest"),
        "finalize_host_subscribe must register the responding peer as upstream \
         interest — without it `send_unsubscribe_upstream` cannot find the peer \
         to notify on client disconnect (#3874)"
    );
    // (2) install lease in active_subscriptions. Anchor on the API,
    // not the variable name, so renaming `key` does not break the pin.
    assert!(
        body.contains("ring.subscribe("),
        "finalize_host_subscribe must call `ring.subscribe(...)` to install the \
         lease in `active_subscriptions` — without it the contract is not picked \
         up by `contracts_needing_renewal` and the subscription silently dies at \
         TTL expiry (#3851)"
    );
    // (3) clear pending backoff state.
    assert!(
        body.contains("complete_subscription_request(") && body.contains(", true)"),
        "finalize_host_subscribe must call `complete_subscription_request(..., true)` \
         to clear the pending mark and reset backoff"
    );
    // (4) fetch contract body if missing → THIS is the core #4223 fix.
    assert!(
        body.contains("fetch_contract_if_missing"),
        "finalize_host_subscribe MUST call `fetch_contract_if_missing` so the \
         host has the contract body locally and can answer subsequent GETs from \
         local state instead of returning NotFound (#4223)"
    );
    // (5) announce_contract_hosted, gated on + after the fetch.
    assert!(
        body.contains("announce_contract_hosted"),
        "finalize_host_subscribe MUST call `announce_contract_hosted` (gated on \
         fetch success) so neighbors include us as an UPDATE broadcast target (#3851)"
    );
    let fetch_pos = body
        .find("fetch_contract_if_missing")
        .expect("fetch call site already asserted above");
    let announce_pos = body
        .find("announce_contract_hosted")
        .expect("announce call site already asserted above");
    assert!(
        fetch_pos < announce_pos,
        "fetch_contract_if_missing must appear BEFORE announce_contract_hosted in \
         finalize_host_subscribe — announcing before the body is local would tell \
         neighbors to forward UPDATEs to a peer that cannot validate them (Codex \
         HIGH finding on PR #4224)"
    );
    // (5b) The announce MUST be conditionally gated on the fetch result, not
    // merely ordered after it.
    let have_body_pos = body
        .find("have_body")
        .expect("finalize_host_subscribe must bind `have_body` from the fetch match");
    assert!(
        body.contains("if have_body"),
        "finalize_host_subscribe MUST gate `announce_contract_hosted` on an \
         `if have_body {{ ... }}` conditional — the gate is the Codex HIGH fix \
         invariant, not just the ordering"
    );
    assert!(
        have_body_pos < announce_pos,
        "the `have_body` binding MUST appear before the `announce_contract_hosted` \
         call site so the announce is actually gated by the fetch result"
    );
}

/// Pin: `finalize_originator_subscribe` MUST delegate the shared host-formation
/// sequence to `finalize_host_subscribe` (piece D) AND perform the
/// originator-only local-client-interest step, gated on `!is_renewal`.
///
/// Delegation keeps the originator and chain-host paths in lock-step: if a
/// future refactor re-inlines the side effects here instead of delegating, the
/// two paths can drift (the #4223 class). The `add_local_client` gate must stay
/// on `!is_renewal` because `add_client` is NOT idempotent.
#[test]
fn finalize_originator_subscribe_delegates_and_adds_local_client() {
    const SOURCE: &str = include_str!("../subscribe.rs");
    let body = extract_fn_body_stripped(SOURCE, "pub(super) async fn finalize_originator_subscribe(");
    let body = body.as_str();

    assert!(
        body.contains("finalize_host_subscribe("),
        "finalize_originator_subscribe MUST delegate to `finalize_host_subscribe` \
         for the shared host-formation sequence (register upstream, ring.subscribe, \
         clear backoff, fetch body, announce-if-present) — re-inlining risks the \
         two paths drifting (the #4223 class)"
    );
    // Originator-only step: register local-client interest, gated on !is_renewal.
    assert!(
        body.contains("add_local_client("),
        "finalize_originator_subscribe must call `add_local_client(...)` so inbound \
         ChangeInterests for this contract get processed"
    );
    assert!(
        body.contains("!is_renewal"),
        "finalize_originator_subscribe must gate `add_local_client` on `!is_renewal` \
         — `add_client` is NOT idempotent (`ring::interest::Contract::add_client` \
         increments `local_client_count` on every call), so an unconditional call \
         on every ~2-minute renewal cycle would leak the gauge unboundedly"
    );
}

/// Pin: the task-per-tx subscribe driver's `ReplyClass::Subscribed`
/// branch MUST delegate to `finalize_originator_subscribe`. Inlining
/// the side effects directly at the branch is what caused issue #4223
/// — the original inline sequence was missing
/// `fetch_contract_if_missing` and `announce_contract_hosted`. By
/// pinning the call site to the helper, any future refactor that
/// reverts to inlining will trip this test.
///
/// Negative-pin shape: matches on the API surface (`ring.subscribe(`)
/// rather than full expressions, so a variable rename can't slip a
/// re-inlined call past the guard.
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
    let raw_branch = &SOURCE[branch_start..branch_end];

    // Strip line comments so doc / explanatory comments that mention
    // the API names as negative context (e.g. "delegated to the
    // helper which calls announce_contract_hosted") don't trip the
    // negative substring pins. Mirrors the helper-side pin and the
    // relay-side pin in op_ctx_task.rs.
    let branch: String = raw_branch
        .lines()
        .map(|line| match line.find("//") {
            Some(idx) => &line[..idx],
            None => line,
        })
        .collect::<Vec<_>>()
        .join("\n");
    let branch = branch.as_str();

    assert!(
        branch.contains("finalize_originator_subscribe"),
        "Subscribed branch must delegate to `finalize_originator_subscribe` \
         — inlining the side effects is what caused #4223 (missing \
         fetch_contract_if_missing + announce_contract_hosted). Keep the \
         helper as the single source of truth."
    );

    // Negative pins: the inlined calls must NOT come back. Anchor on
    // the API surface — `ring.subscribe(` rather than
    // `op_manager.ring.subscribe(key)` — so a rename of either the
    // receiver (e.g. `om.ring.subscribe(...)`) or the contract-key
    // variable can't silently bypass the guard.
    assert!(
        !branch.contains("ring.subscribe("),
        "Subscribed branch must not call `ring.subscribe(...)` directly — go \
         through `finalize_originator_subscribe` so the fetch + announce \
         steps stay grouped with the lease install"
    );
    assert!(
        !branch.contains("complete_subscription_request("),
        "Subscribed branch must not call `complete_subscription_request(...)` \
         directly — go through `finalize_originator_subscribe`"
    );
    assert!(
        !branch.contains("announce_contract_hosted"),
        "Subscribed branch must not call `announce_contract_hosted` directly \
         — the fetch-success gate lives inside `finalize_originator_subscribe`; \
         inlining it would re-introduce the Codex HIGH finding on PR #4224 \
         (announcing without the contract body)"
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

/// Regression test: `SubscribeMsg::Response.hop_count` roundtrips through
/// bincode for both `Subscribed` and `NotFound` result variants.
///
/// Before #4248 the SUBSCRIBE telemetry path emitted `hop_count: None` at
/// every terminal SUBSCRIBE event (`SubscribeSuccess`, `SubscribeNotFound`)
/// because no value was being threaded through the wire.  The fix carries
/// the field on `SubscribeMsg::Response` so the originator has it when
/// constructing the log event.  This test asserts that the new field
/// survives round-trip serialisation for both result variants — i.e., the
/// wire format actually carries it.
///
/// bincode-positional caveat: any future positional change here will
/// break older binaries; see the `MIN_COMPATIBLE_VERSION` bump that
/// accompanies this PR.
#[test]
fn test_subscribe_msg_response_hop_count_roundtrip() {
    use freenet_stdlib::prelude::{CodeHash, ContractKey};
    let key =
        ContractKey::from_id_and_code(ContractInstanceId::new([7u8; 32]), CodeHash::new([8u8; 32]));
    let instance_id = *key.id();
    let cases: &[(&str, usize)] = &[
        ("zero", 0),
        ("one", 1),
        ("mid", 4),
        ("htl", 10),
        ("large", 64),
    ];
    for (label, hop_count) in cases.iter().copied() {
        // Subscribed variant
        let subscribed = SubscribeMsg::Response {
            id: Transaction::new::<SubscribeMsg>(),
            instance_id,
            result: SubscribeMsgResult::Subscribed { key },
            hop_count,
        };
        let bytes = bincode::serialize(&subscribed).expect(label);
        let restored: SubscribeMsg = bincode::deserialize(&bytes).expect(label);
        match restored {
            SubscribeMsg::Response { hop_count: hc, .. } => assert_eq!(
                hc, hop_count,
                "Subscribed Response.hop_count must roundtrip ({label})"
            ),
            SubscribeMsg::Request { .. }
            | SubscribeMsg::Unsubscribe { .. }
            | SubscribeMsg::ForwardingAck { .. } => {
                panic!("expected Response for {label}")
            }
        }

        // NotFound variant
        let notfound = SubscribeMsg::Response {
            id: Transaction::new::<SubscribeMsg>(),
            instance_id,
            result: SubscribeMsgResult::NotFound,
            hop_count,
        };
        let bytes = bincode::serialize(&notfound).expect(label);
        let restored: SubscribeMsg = bincode::deserialize(&bytes).expect(label);
        match restored {
            SubscribeMsg::Response { hop_count: hc, .. } => assert_eq!(
                hc, hop_count,
                "NotFound Response.hop_count must roundtrip ({label})"
            ),
            SubscribeMsg::Request { .. }
            | SubscribeMsg::Unsubscribe { .. }
            | SubscribeMsg::ForwardingAck { .. } => {
                panic!("expected Response for {label}")
            }
        }
    }
}

/// Pin: the inbound `SubscribeHint` arm in `node.rs` MUST consult the
/// backpressure-aware migration-admission gate (`migration_admission_decision`,
/// which keys on INTERESTED module-cache occupancy) BEFORE calling
/// `start_directed_subscribe`, AND must force a fresh interest-shadow recompute
/// (`refresh_interest_gauges_now`) BEFORE that gate so the gate reads the
/// cache's current hot occupancy rather than a throttled (≤10s-stale) gauge that
/// a burst of hints could over-admit against. Without the gate the node accepts
/// unbounded placement migration and reproduces the #4534 "contract queue full"
/// outage / gateway OOM; without the pre-gate refresh a burst silently bypasses
/// the gate. Anchored from this side so removing or reordering either step trips
/// the build.
#[test]
fn subscribe_hint_arm_gates_migration_on_cache_backpressure() {
    const SOURCE: &str = include_str!("../../node.rs");
    let anchor = "NetMessageV1::SubscribeHint(hint) => {";
    let branch_start = SOURCE
        .find(anchor)
        .expect("SubscribeHint arm not found in node.rs");
    let next_variant = "NetMessageV1::Aborted(tx) => {";
    let window_end = SOURCE[branch_start..]
        .find(next_variant)
        .expect("could not find end of SubscribeHint arm")
        + branch_start;
    let window = &SOURCE[branch_start..window_end];

    let refresh = window.find("refresh_interest_gauges_now(").expect(
        "SubscribeHint arm must force a fresh gauges-only interest recompute \
         (`refresh_interest_gauges_now`) so the gate reads current hot occupancy, \
         not a stale-low throttled gauge a burst could over-admit against (#4534)",
    );
    let gate = window.find("migration_admission_decision(").expect(
        "SubscribeHint arm must call `migration_admission_decision` to gate \
         placement migration on module-cache backpressure (#4534)",
    );
    let subscribe = window
        .find("start_directed_subscribe(")
        .expect("SubscribeHint arm must call `start_directed_subscribe` to act on the hint");
    assert!(
        refresh < gate,
        "the interest-shadow refresh must run BEFORE migration_admission_decision, \
         else the gate reads a stale hot-occupancy gauge and a burst of hints can \
         over-admit past the ceiling (#4534)"
    );
    assert!(
        gate < subscribe,
        "the migration-admission gate must run BEFORE start_directed_subscribe, \
         else the node starts hosting the migrated contract before checking cache \
         headroom (#4534)"
    );
}

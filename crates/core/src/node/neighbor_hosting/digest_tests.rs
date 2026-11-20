//! Unit tests for the digest-first anti-entropy exchange (#5157). One test per
//! row of the scenario matrix in the design, named after the row.

use super::*;
use crate::transport::TransportKeypair;
use crate::util::time_source::SharedMockTimeSource;
use freenet_stdlib::prelude::CodeHash;

fn id_n(n: u32) -> ContractInstanceId {
    let mut b = [0u8; 32];
    b[..4].copy_from_slice(&n.to_le_bytes());
    ContractInstanceId::new(b)
}

fn key_n(n: u32) -> ContractKey {
    ContractKey::from_id_and_code(id_n(n), CodeHash::new([7u8; 32]))
}

fn peer() -> TransportPublicKey {
    TransportKeypair::new().public().clone()
}

fn xor_of(ids: &HashSet<ContractInstanceId>) -> u64 {
    ids.iter().fold(0, |acc, id| acc ^ id_digest(id))
}

fn announce(
    added: Vec<ContractInstanceId>,
    removed: Vec<ContractInstanceId>,
) -> NeighborHostingMessage {
    NeighborHostingMessage::HostingAnnounce {
        added,
        removed,
        is_response: true,
    }
}

/// Make `viewer` hold exactly `subject`'s current set, as the full response
/// does, and return the subject's key as the viewer knows it.
fn sync_view(
    viewer: &NeighborHostingManager,
    subject: &NeighborHostingManager,
    as_peer: &TransportPublicKey,
) {
    let response = subject.full_state_response();
    viewer.handle_message(as_peer, response);
}

fn host_n(m: &NeighborHostingManager, range: std::ops::Range<u32>) {
    for n in range {
        m.on_contract_hosted(&key_n(n));
    }
}

fn digest_request_for(
    viewer: &NeighborHostingManager,
    as_peer: &TransportPublicKey,
) -> NeighborHostingMessage {
    viewer.anti_entropy_request(Some(as_peer), true)
}

fn serialized_len(m: &NeighborHostingMessage) -> usize {
    bincode::serialize(m).expect("serialize").len()
}

/// Row 1: new <-> new, set unchanged: the reply is `HostingStateUnchanged`, a
/// few bytes, and the requester's view is untouched.
#[test]
fn row1_unchanged_set_gets_the_short_reply() {
    let a = NeighborHostingManager::new();
    let b = NeighborHostingManager::new();
    let a_key = peer();
    let b_key = peer();
    host_n(&a, 0..50);
    sync_view(&b, &a, &a_key);
    let before = b.contracts_for_peer(&a_key);
    assert_eq!(before.len(), 50, "premise: B holds A's full set");

    let request = digest_request_for(&b, &a_key);
    assert!(matches!(
        request,
        NeighborHostingMessage::HostingStateDigestRequest { count: 50, .. }
    ));
    let reply = a
        .handle_message(&b_key, request)
        .response
        .expect("a digest request is always answered");
    assert!(matches!(
        reply,
        NeighborHostingMessage::HostingStateUnchanged
    ));
    assert!(
        serialized_len(&reply) <= 8,
        "the unchanged reply must be tiny, was {} bytes",
        serialized_len(&reply)
    );
    // A full response for the same set is 20 + 32n bytes.
    assert!(serialized_len(&a.full_state_response()) > 1_600);

    let applied = b.handle_message(&a_key, reply);
    assert!(applied.response.is_none() && applied.overlapping_contracts.is_empty());
    assert_eq!(
        b.contracts_for_peer(&a_key),
        before,
        "the view is untouched"
    );
    assert_eq!(
        a.anti_entropy_stats(),
        AntiEntropyStats {
            digest_matched: 1,
            digest_mismatched: 0,
            legacy_replies_skipped: 0
        }
    );
}

/// Row 2: new <-> new, set changed: the reply is the full set, the requester's
/// view is replaced, and its digest then matches the responder's.
#[test]
fn row2_changed_set_gets_the_full_set_and_the_view_and_digest_follow() {
    let a = NeighborHostingManager::new();
    let b = NeighborHostingManager::new();
    let a_key = peer();
    let b_key = peer();
    host_n(&a, 0..10);
    sync_view(&b, &a, &a_key);
    // A hosts two more and evicts one, and B hears none of it.
    host_n(&a, 100..102);
    a.on_contract_unhosted(&key_n(3));

    let reply = a
        .handle_message(&b_key, digest_request_for(&b, &a_key))
        .response
        .expect("answered");
    let NeighborHostingMessage::HostingStateResponse { contracts } = &reply else {
        panic!("a differing digest must get the full set, got {reply:?}");
    };
    assert_eq!(contracts.len(), 11);
    b.handle_message(&a_key, reply);
    assert_eq!(b.contracts_for_peer(&a_key), a.advertised_ids());
    assert_eq!(b.view_summary(&a_key), a.own_summary());
    assert_eq!(a.anti_entropy_stats().digest_mismatched, 1);

    // And now the next round is the short one.
    let again = a
        .handle_message(&b_key, digest_request_for(&b, &a_key))
        .response
        .expect("answered");
    assert!(matches!(
        again,
        NeighborHostingMessage::HostingStateUnchanged
    ));
}

/// Row 3: an announce in flight during the exchange. Whichever way the race
/// falls the pair converges and never agrees on a wrong set.
#[test]
fn row3_announce_in_flight_during_the_digest_exchange_converges() {
    let a = NeighborHostingManager::new();
    let b = NeighborHostingManager::new();
    let a_key = peer();
    let b_key = peer();
    host_n(&a, 0..5);
    sync_view(&b, &a, &a_key);

    // B builds its request, THEN A hosts a new contract (its announce has not
    // reached B), then A handles the request: mismatch, so the full set.
    let request = digest_request_for(&b, &a_key);
    let announcement = a.on_contract_hosted(&key_n(500)).expect("new contract");
    let reply = a
        .handle_message(&b_key, request)
        .response
        .expect("answered");
    assert!(matches!(
        reply,
        NeighborHostingMessage::HostingStateResponse { .. }
    ));
    b.handle_message(&a_key, reply);
    // The announce then arrives: applying it to an already-correct view is a no-op.
    b.handle_message(&a_key, announcement);
    assert_eq!(b.contracts_for_peer(&a_key), a.advertised_ids());
    assert_eq!(b.view_summary(&a_key), a.own_summary());

    // The other order: the announce reaches B before B builds its request.
    let announcement = a.on_contract_hosted(&key_n(501)).expect("new contract");
    b.handle_message(&a_key, announcement);
    let reply = a
        .handle_message(&b_key, digest_request_for(&b, &a_key))
        .response
        .expect("answered");
    assert!(matches!(
        reply,
        NeighborHostingMessage::HostingStateUnchanged
    ));
    assert_eq!(b.contracts_for_peer(&a_key), a.advertised_ids());
}

/// Rows 4 and 5, manager side: a peer the gate did not approve is sent the
/// legacy request, whatever the view holds.
#[test]
fn rows4_5_an_unapproved_peer_gets_the_legacy_request() {
    let b = NeighborHostingManager::new();
    let a_key = peer();
    b.handle_message(&a_key, announce(vec![id_n(1)], vec![]));
    assert!(matches!(
        b.anti_entropy_request(Some(&a_key), false),
        NeighborHostingMessage::HostingStateRequest
    ));
    assert!(matches!(
        b.anti_entropy_request(None, false),
        NeighborHostingMessage::HostingStateRequest
    ));
    // An approved peer we hold no view of is asked with the empty digest.
    assert!(matches!(
        b.anti_entropy_request(Some(&peer()), true),
        NeighborHostingMessage::HostingStateDigestRequest {
            digest: 0,
            count: 0
        }
    ));
}

/// Row 6: an old requester, a new responder. The first request on a connection
/// is answered in full; later ones at most once per `LEGACY_FULL_REPLY_INTERVAL`,
/// and each skipped one is counted.
#[test]
fn row6_legacy_requests_are_answered_once_per_interval() {
    let clock = SharedMockTimeSource::new();
    let a = NeighborHostingManager::with_options(Arc::new(clock.clone()), false);
    let old_peer = peer();
    host_n(&a, 0..20);

    let ask = |a: &NeighborHostingManager| {
        a.handle_message(&old_peer, NeighborHostingMessage::HostingStateRequest)
            .response
    };
    assert!(
        matches!(
            ask(&a),
            Some(NeighborHostingMessage::HostingStateResponse { .. })
        ),
        "the first request on a connection is always answered in full"
    );
    // Heartbeats every 5 minutes for the next 55 minutes: all throttled.
    for _ in 0..11 {
        clock.advance_time(Duration::from_secs(5 * 60));
        assert!(ask(&a).is_none(), "throttled inside the interval");
    }
    assert_eq!(a.anti_entropy_stats().legacy_replies_skipped, 11);

    // 59 minutes 59 seconds after the first reply: still throttled.
    clock.advance_time(Duration::from_secs(4 * 60 + 59));
    assert!(ask(&a).is_none(), "one second short of the interval");
    clock.advance_time(Duration::from_secs(1));
    assert!(
        matches!(
            ask(&a),
            Some(NeighborHostingMessage::HostingStateResponse { .. })
        ),
        "answered again once the interval has passed"
    );
    assert_eq!(a.anti_entropy_stats().legacy_replies_skipped, 12);
    assert!(
        ask(&a).is_none(),
        "and the interval restarts from that reply"
    );
    assert_eq!(a.anti_entropy_stats().legacy_replies_skipped, 13);
}

/// Row 6 variant: peers are throttled independently of each other.
#[test]
fn row6_the_legacy_throttle_is_per_peer() {
    let a = NeighborHostingManager::with_options(Arc::new(SharedMockTimeSource::new()), false);
    let (p1, p2) = (peer(), peer());
    for p in [&p1, &p2] {
        assert!(
            a.handle_message(p, NeighborHostingMessage::HostingStateRequest)
                .response
                .is_some()
        );
    }
    assert!(
        a.handle_message(&p1, NeighborHostingMessage::HostingStateRequest)
            .response
            .is_none()
    );
}

/// Row 7: a reconnect. The disconnect clears the peer's throttle entry and its
/// view, so the first exchange on the new connection is full and rebuilds it.
#[test]
fn row7_reconnect_starts_with_a_full_exchange() {
    let a = NeighborHostingManager::with_options(Arc::new(SharedMockTimeSource::new()), false);
    let b = NeighborHostingManager::new();
    let (a_key, b_key) = (peer(), peer());
    host_n(&a, 0..8);
    let ask = || {
        a.handle_message(&b_key, NeighborHostingMessage::HostingStateRequest)
            .response
    };
    assert!(ask().is_some());
    assert!(ask().is_none(), "throttled while connected");

    sync_view(&b, &a, &a_key);
    a.on_peer_disconnected(&b_key);
    b.on_peer_disconnected(&a_key);
    assert!(
        b.contracts_for_peer(&a_key).is_empty(),
        "the view is dropped"
    );
    assert!(
        ask().is_some(),
        "the first request on the new connection is answered"
    );

    // The rebuilt view comes from the on-connect request: B's view is empty, so
    // even a digest request gets the full set.
    let reply = a
        .handle_message(&b_key, digest_request_for(&b, &a_key))
        .response
        .expect("answered");
    assert!(matches!(
        reply,
        NeighborHostingMessage::HostingStateResponse { .. }
    ));
    b.handle_message(&a_key, reply);
    assert_eq!(b.contracts_for_peer(&a_key), a.advertised_ids());
}

/// Row 8: a forced digest collision. Two different ids with the same first 8
/// bytes give the same digest, so the responder says `Unchanged` although the
/// requester's view is wrong. This is the accepted cost of a 64-bit digest: it
/// lasts until the next change to either set, which changes the digest or the
/// count and heals the view. Documented here so it is a decision, not a surprise.
#[test]
fn row8_a_digest_collision_is_missed_until_the_next_change() {
    let colliding = |tail: u8| {
        let mut b = [0u8; 32];
        b[..8].copy_from_slice(&[9, 8, 7, 6, 5, 4, 3, 2]);
        b[31] = tail;
        ContractInstanceId::new(b)
    };
    let (x, y) = (colliding(1), colliding(2));
    assert_ne!(x, y);
    assert_eq!(id_digest(&x), id_digest(&y), "premise: the digests collide");

    let a = NeighborHostingManager::new();
    let b = NeighborHostingManager::new();
    let (a_key, b_key) = (peer(), peer());
    a.on_contract_hosted(&ContractKey::from_id_and_code(x, CodeHash::new([1; 32])));
    // B believes A hosts y, not x.
    b.handle_message(&a_key, announce(vec![y], vec![]));

    let reply = a
        .handle_message(&b_key, digest_request_for(&b, &a_key))
        .response
        .expect("answered");
    assert!(
        matches!(reply, NeighborHostingMessage::HostingStateUnchanged),
        "the collision is not detected"
    );
    assert_eq!(
        b.contracts_for_peer(&a_key),
        HashSet::from([y]),
        "view stays wrong"
    );

    // The next change to A's set changes the count, so the next round heals B.
    a.on_contract_hosted(&key_n(77));
    let reply = a
        .handle_message(&b_key, digest_request_for(&b, &a_key))
        .response
        .expect("answered");
    b.handle_message(&a_key, reply);
    assert_eq!(b.contracts_for_peer(&a_key), a.advertised_ids());
}

/// Row 9: a neighbour advertising more than the per-neighbour cap. The view is
/// capped, so its count can never equal the responder's; every cycle gets the
/// full set, as before this change, and nothing panics.
#[test]
fn row9_an_over_cap_neighbour_gets_the_full_set_every_cycle() {
    let a = NeighborHostingManager::new();
    let b = NeighborHostingManager::new();
    let (a_key, b_key) = (peer(), peer());
    let total = MAX_ADVERTISED_CONTRACTS_PER_NEIGHBOR as u32 + 10;
    host_n(&a, 0..total);
    sync_view(&b, &a, &a_key);
    assert_eq!(
        b.contracts_for_peer(&a_key).len(),
        MAX_ADVERTISED_CONTRACTS_PER_NEIGHBOR,
        "premise: B's view is capped"
    );
    for _ in 0..2 {
        let reply = a
            .handle_message(&b_key, digest_request_for(&b, &a_key))
            .response
            .expect("answered");
        let NeighborHostingMessage::HostingStateResponse { contracts } = &reply else {
            panic!("an over-cap set can never match: {reply:?}");
        };
        assert_eq!(contracts.len(), total as usize);
        b.handle_message(&a_key, reply);
    }
    assert_eq!(a.anti_entropy_stats().digest_mismatched, 2);
    assert_eq!(a.anti_entropy_stats().digest_matched, 0);
}

/// Row 10: the digest moves only on a real change. A duplicate add and a removal
/// of an absent id leave it alone, in the view and in our own set, and after any
/// mix of operations it equals the XOR recomputed from the members.
#[test]
fn row10_xor_runs_only_on_a_real_change() {
    // The view.
    let m = NeighborHostingManager::new();
    let p = peer();
    m.handle_message(&p, announce(vec![id_n(1), id_n(2)], vec![]));
    let settled = m.view_summary(&p);
    m.handle_message(&p, announce(vec![id_n(1)], vec![]));
    assert_eq!(
        m.view_summary(&p),
        settled,
        "a duplicate add changes nothing"
    );
    m.handle_message(&p, announce(vec![], vec![id_n(99)]));
    assert_eq!(
        m.view_summary(&p),
        settled,
        "removing an absent id changes nothing"
    );
    m.handle_message(&p, announce(vec![id_n(3), id_n(3)], vec![id_n(2)]));
    let ids = m.contracts_for_peer(&p);
    assert_eq!(ids, HashSet::from([id_n(1), id_n(3)]));
    assert_eq!(m.view_summary(&p), (xor_of(&ids), 2));

    // Our own set.
    let own = NeighborHostingManager::new();
    own.on_contract_hosted(&key_n(1));
    let settled = own.own_summary();
    assert!(own.on_contract_hosted(&key_n(1)).is_none());
    assert!(own.on_contract_unhosted(&key_n(42)).is_none());
    assert_eq!(own.own_summary(), settled);
    own.on_contract_hosted(&key_n(2));
    own.on_contract_unhosted(&key_n(1));
    // The eviction race helper removes and may put it back: net zero when it does.
    let before = own.own_summary();
    assert!(
        own.on_contract_unhosted_unless_rehosted(&key_n(2), || true)
            .is_none()
    );
    assert_eq!(
        own.own_summary(),
        before,
        "an undone removal restores the digest"
    );
    own.initialize_from_hosting_cache([key_n(2), key_n(3), key_n(3)].into_iter());
    let ids = own.advertised_ids();
    assert_eq!(own.own_summary(), (xor_of(&ids), ids.len() as u32));

    // A full response replaces the view with a duplicate-laden list.
    m.handle_message(
        &p,
        NeighborHostingMessage::HostingStateResponse {
            contracts: vec![id_n(5), id_n(5), id_n(6)],
        },
    );
    let ids = m.contracts_for_peer(&p);
    assert_eq!(ids, HashSet::from([id_n(5), id_n(6)]));
    assert_eq!(m.view_summary(&p), (xor_of(&ids), 2));
}

/// Row 11, manager side: with the throttle off a legacy request is answered in
/// full every time, as a pre-#5157 responder does.
#[test]
fn row11_an_unthrottled_responder_answers_every_legacy_request() {
    let a = NeighborHostingManager::with_options(Arc::new(SharedMockTimeSource::new()), true);
    let p = peer();
    for _ in 0..5 {
        assert!(
            a.handle_message(&p, NeighborHostingMessage::HostingStateRequest)
                .response
                .is_some()
        );
    }
    assert_eq!(a.anti_entropy_stats().legacy_replies_skipped, 0);
}

/// Both digest messages are tiny and the legacy exchange is untouched: a legacy
/// request is answered with the same full set a digest mismatch gets.
#[test]
fn the_full_reply_is_identical_for_legacy_and_digest_requests() {
    let a = NeighborHostingManager::new();
    host_n(&a, 0..12);
    let p = peer();
    let legacy = a
        .handle_message(&p, NeighborHostingMessage::HostingStateRequest)
        .response
        .expect("answered");
    let digest = a
        .handle_message(
            &p,
            NeighborHostingMessage::HostingStateDigestRequest {
                digest: 1,
                count: 1,
            },
        )
        .response
        .expect("answered");
    assert_eq!(
        bincode::serialize(&legacy).unwrap(),
        bincode::serialize(&digest).unwrap()
    );
}

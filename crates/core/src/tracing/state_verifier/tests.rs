use super::*;
use crate::message::Transaction;
use crate::node::PeerId;
use crate::operations::put::PutMsg;
use crate::operations::update::UpdateMsg;
use crate::ring::PeerKeyLocation;
use crate::tracing::{EventKind, InterestSyncEvent, NetLogMessage, PutEvent, UpdateEvent};
use crate::transport::TransportPublicKey;
use chrono::{DateTime, Utc};
use freenet_stdlib::prelude::*;
use std::net::SocketAddr;

fn make_peer_id(port: u16) -> PeerId {
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let key = TransportPublicKey::from_bytes([port as u8; 32]);
    PeerId::new(addr, key)
}

fn make_peer_key_location(port: u16) -> PeerKeyLocation {
    let key = TransportPublicKey::from_bytes([port as u8; 32]);
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    PeerKeyLocation::new(key, addr)
}

fn make_contract_key() -> ContractKey {
    ContractKey::from_id_and_code(ContractInstanceId::new([1u8; 32]), CodeHash::new([2u8; 32]))
}

fn make_tx() -> Transaction {
    Transaction::new::<PutMsg>()
}

fn make_update_tx() -> Transaction {
    Transaction::new::<UpdateMsg>()
}

///// Fixed base timestamp for deterministic tests (avoids Utc::now()).
fn base_time() -> DateTime<Utc> {
    chrono::DateTime::parse_from_rfc3339("2025-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc)
}

fn now() -> DateTime<Utc> {
    base_time()
}

#[test]
fn test_clean_convergence() {
    let key = make_contract_key();
    let tx = make_tx();
    let peer1 = make_peer_id(3001);
    let peer2 = make_peer_id(3002);

    let events = vec![
        // Peer 1 stores via PUT
        NetLogMessage {
            tx,
            datetime: now(),
            peer_id: peer1.clone(),
            kind: EventKind::Put(PutEvent::PutSuccess {
                id: tx,
                requester: make_peer_key_location(3001),
                target: make_peer_key_location(3001),
                key,
                hop_count: Some(0),
                elapsed_ms: 10,
                timestamp: 100,
                state_hash: Some("aabb0011".to_string()),
            }),
        },
        // Peer 2 receives broadcast and stores same state
        NetLogMessage {
            tx,
            datetime: now(),
            peer_id: peer2.clone(),
            kind: EventKind::Put(PutEvent::BroadcastReceived {
                id: tx,
                requester: make_peer_key_location(3001),
                key,
                value: WrappedState::new(vec![1, 2, 3]),
                target: make_peer_key_location(3002),
                timestamp: 110,
                state_hash: Some("aabb0011".to_string()),
            }),
        },
    ];

    let verifier = StateVerifier::from_events(events);
    let report = verifier.verify();

    assert!(
        report.is_clean(),
        "Expected clean report: {}",
        report.display()
    );
    assert_eq!(report.contracts_analyzed, 1);
    assert_eq!(report.state_events, 2);
}

#[test]
fn test_detects_final_divergence() {
    let key = make_contract_key();
    let tx = make_tx();
    let peer1 = make_peer_id(3001);
    let peer2 = make_peer_id(3002);

    let events = vec![
        // Peer 1 stores state "aaaa"
        NetLogMessage {
            tx,
            datetime: now(),
            peer_id: peer1.clone(),
            kind: EventKind::Put(PutEvent::PutSuccess {
                id: tx,
                requester: make_peer_key_location(3001),
                target: make_peer_key_location(3001),
                key,
                hop_count: Some(0),
                elapsed_ms: 10,
                timestamp: 100,
                state_hash: Some("aaaa0000".to_string()),
            }),
        },
        // Peer 2 stores DIFFERENT state "bbbb"
        NetLogMessage {
            tx,
            datetime: now(),
            peer_id: peer2.clone(),
            kind: EventKind::Put(PutEvent::PutSuccess {
                id: tx,
                requester: make_peer_key_location(3002),
                target: make_peer_key_location(3002),
                key,
                hop_count: Some(0),
                elapsed_ms: 10,
                timestamp: 100,
                state_hash: Some("bbbb0000".to_string()),
            }),
        },
    ];

    let verifier = StateVerifier::from_events(events);
    let report = verifier.verify();

    assert!(!report.is_clean());
    assert_eq!(report.divergences().len(), 1);

    let div = &report.divergences()[0];
    if let StateAnomaly::FinalDivergence { peer_states, .. } = div {
        assert_eq!(peer_states.len(), 2);
    } else {
        panic!("Expected FinalDivergence anomaly");
    }
}

#[test]
fn test_detects_missing_broadcast() {
    let key = make_contract_key();
    let tx = make_tx();
    let peer1 = make_peer_id(3001);
    let target_pkl = make_peer_key_location(3002);

    let events = vec![
        // Peer 1 stores via PUT
        NetLogMessage {
            tx,
            datetime: now(),
            peer_id: peer1.clone(),
            kind: EventKind::Put(PutEvent::PutSuccess {
                id: tx,
                requester: make_peer_key_location(3001),
                target: make_peer_key_location(3001),
                key,
                hop_count: Some(0),
                elapsed_ms: 10,
                timestamp: 100,
                state_hash: Some("aaaa0000".to_string()),
            }),
        },
        // Peer 1 emits broadcast to peer 2
        NetLogMessage {
            tx,
            datetime: now(),
            peer_id: peer1.clone(),
            kind: EventKind::Put(PutEvent::BroadcastEmitted {
                id: tx,
                upstream: make_peer_key_location(3001),
                broadcast_to: vec![target_pkl],
                broadcasted_to: 1,
                key,
                value: WrappedState::new(vec![1, 2, 3]),
                sender: make_peer_key_location(3001),
                timestamp: 105,
                state_hash: Some("aaaa0000".to_string()),
            }),
        },
        // No BroadcastReceived from peer 2 - it was lost!
    ];

    let verifier = StateVerifier::from_events(events);
    let report = verifier.verify();

    let missing = report.missing_broadcasts();
    assert_eq!(
        missing.len(),
        1,
        "Expected 1 missing broadcast, report:\n{}",
        report.display()
    );

    if let StateAnomaly::MissingBroadcast {
        missing_targets, ..
    } = missing[0]
    {
        assert_eq!(missing_targets.len(), 1);
        assert_eq!(missing_targets[0].port(), 3002);
    } else {
        panic!("Expected MissingBroadcast anomaly");
    }
}

#[test]
fn test_detects_unapplied_update_broadcast() {
    let key = make_contract_key();
    let put_tx = make_tx();
    let update_tx = make_update_tx();
    let peer1 = make_peer_id(3001);
    let peer2 = make_peer_id(3002);

    let events = vec![
        // Both peers have the contract via PUT
        NetLogMessage {
            tx: put_tx,
            datetime: now(),
            peer_id: peer1.clone(),
            kind: EventKind::Put(PutEvent::PutSuccess {
                id: put_tx,
                requester: make_peer_key_location(3001),
                target: make_peer_key_location(3001),
                key,
                hop_count: Some(0),
                elapsed_ms: 10,
                timestamp: 100,
                state_hash: Some("aaaa0000".to_string()),
            }),
        },
        // Peer 1 updates
        NetLogMessage {
            tx: update_tx,
            datetime: now(),
            peer_id: peer1.clone(),
            kind: EventKind::Update(UpdateEvent::UpdateSuccess {
                id: update_tx,
                requester: make_peer_key_location(3001),
                target: make_peer_key_location(3001),
                key,
                timestamp: 200,
                state_hash_before: Some("aaaa0000".to_string()),
                state_hash_after: Some("cccc0000".to_string()),
            }),
        },
        // Peer 1 emits broadcast to peer 2
        NetLogMessage {
            tx: update_tx,
            datetime: now(),
            peer_id: peer1.clone(),
            kind: EventKind::Update(UpdateEvent::BroadcastEmitted {
                id: update_tx,
                upstream: make_peer_key_location(3001),
                broadcast_to: vec![make_peer_key_location(3002)],
                broadcasted_to: 1,
                key,
                value: WrappedState::new(vec![1, 2, 3, 4]),
                sender: make_peer_key_location(3001),
                timestamp: 210,
                state_hash: Some("cccc0000".to_string()),
            }),
        },
        // Peer 2 receives the broadcast
        NetLogMessage {
            tx: update_tx,
            datetime: now(),
            peer_id: peer2.clone(),
            kind: EventKind::Update(UpdateEvent::BroadcastReceived {
                id: update_tx,
                requester: make_peer_key_location(3001),
                key,
                value: WrappedState::new(vec![1, 2, 3, 4]),
                target: make_peer_key_location(3002),
                timestamp: 220,
                state_hash: Some("cccc0000".to_string()),
            }),
        },
        // But no BroadcastApplied from peer 2 - delta merge failed!
    ];

    let verifier = StateVerifier::from_events(events);
    let report = verifier.verify();

    let unapplied = report.unapplied_broadcasts();
    assert_eq!(
        unapplied.len(),
        1,
        "Expected 1 unapplied broadcast, report:\n{}",
        report.display()
    );
}

#[test]
fn test_full_update_cycle_clean() {
    let key = make_contract_key();
    let put_tx = make_tx();
    let update_tx = make_update_tx();
    let peer1 = make_peer_id(3001);
    let peer2 = make_peer_id(3002);

    let events = vec![
        // Both peers store initial state via PUT
        NetLogMessage {
            tx: put_tx,
            datetime: now(),
            peer_id: peer1.clone(),
            kind: EventKind::Put(PutEvent::PutSuccess {
                id: put_tx,
                requester: make_peer_key_location(3001),
                target: make_peer_key_location(3001),
                key,
                hop_count: Some(0),
                elapsed_ms: 10,
                timestamp: 100,
                state_hash: Some("aaaa0000".to_string()),
            }),
        },
        NetLogMessage {
            tx: put_tx,
            datetime: now(),
            peer_id: peer2.clone(),
            kind: EventKind::Put(PutEvent::BroadcastReceived {
                id: put_tx,
                requester: make_peer_key_location(3001),
                key,
                value: WrappedState::new(vec![1, 2, 3]),
                target: make_peer_key_location(3002),
                timestamp: 110,
                state_hash: Some("aaaa0000".to_string()),
            }),
        },
        // Peer 1 updates state
        NetLogMessage {
            tx: update_tx,
            datetime: now(),
            peer_id: peer1.clone(),
            kind: EventKind::Update(UpdateEvent::UpdateSuccess {
                id: update_tx,
                requester: make_peer_key_location(3001),
                target: make_peer_key_location(3001),
                key,
                timestamp: 200,
                state_hash_before: Some("aaaa0000".to_string()),
                state_hash_after: Some("cccc0000".to_string()),
            }),
        },
        // Peer 1 broadcasts to peer 2
        NetLogMessage {
            tx: update_tx,
            datetime: now(),
            peer_id: peer1.clone(),
            kind: EventKind::Update(UpdateEvent::BroadcastEmitted {
                id: update_tx,
                upstream: make_peer_key_location(3001),
                broadcast_to: vec![make_peer_key_location(3002)],
                broadcasted_to: 1,
                key,
                value: WrappedState::new(vec![1, 2, 3, 4]),
                sender: make_peer_key_location(3001),
                timestamp: 210,
                state_hash: Some("cccc0000".to_string()),
            }),
        },
        // Peer 2 receives broadcast
        NetLogMessage {
            tx: update_tx,
            datetime: now(),
            peer_id: peer2.clone(),
            kind: EventKind::Update(UpdateEvent::BroadcastReceived {
                id: update_tx,
                requester: make_peer_key_location(3001),
                key,
                value: WrappedState::new(vec![1, 2, 3, 4]),
                target: make_peer_key_location(3002),
                timestamp: 220,
                state_hash: Some("cccc0000".to_string()),
            }),
        },
        // Peer 2 applies broadcast
        NetLogMessage {
            tx: update_tx,
            datetime: now(),
            peer_id: peer2.clone(),
            kind: EventKind::Update(UpdateEvent::BroadcastApplied {
                id: update_tx,
                key,
                target: make_peer_key_location(3002),
                timestamp: 225,
                state_hash_before: Some("aaaa0000".to_string()),
                state_hash_after: Some("cccc0000".to_string()),
                changed: true,
            }),
        },
    ];

    let verifier = StateVerifier::from_events(events);
    let report = verifier.verify();

    assert!(
        report.is_clean(),
        "Expected clean report:\n{}",
        report.display()
    );
    assert_eq!(report.contracts_analyzed, 1);
    assert_eq!(report.state_events, 4); // PutSuccess + BroadcastReceived + UpdateSuccess + BroadcastApplied
}

#[test]
fn test_empty_events() {
    let verifier = StateVerifier::from_events(vec![]);
    let report = verifier.verify();

    assert!(report.is_clean());
    assert_eq!(report.contracts_analyzed, 0);
    assert_eq!(report.total_events, 0);
}

#[test]
fn test_report_display() {
    let key = make_contract_key();
    let tx = make_tx();
    let peer1 = make_peer_id(3001);
    let peer2 = make_peer_id(3002);

    let events = vec![
        NetLogMessage {
            tx,
            datetime: now(),
            peer_id: peer1.clone(),
            kind: EventKind::Put(PutEvent::PutSuccess {
                id: tx,
                requester: make_peer_key_location(3001),
                target: make_peer_key_location(3001),
                key,
                hop_count: Some(0),
                elapsed_ms: 10,
                timestamp: 100,
                state_hash: Some("aaaa0000".to_string()),
            }),
        },
        NetLogMessage {
            tx,
            datetime: now(),
            peer_id: peer2.clone(),
            kind: EventKind::Put(PutEvent::PutSuccess {
                id: tx,
                requester: make_peer_key_location(3002),
                target: make_peer_key_location(3002),
                key,
                hop_count: Some(0),
                elapsed_ms: 10,
                timestamp: 100,
                state_hash: Some("bbbb0000".to_string()),
            }),
        },
    ];

    let verifier = StateVerifier::from_events(events);
    let report = verifier.verify();
    let display = report.display();

    assert!(display.contains("State Verification Report"));
    assert!(display.contains("Anomalies:"));
    assert!(display.contains("FINAL_DIVERGENCE"));
}

// ---- Tests for new detectors ----

#[test]
fn test_suspected_partition_detected() {
    let key = make_contract_key();
    let tx1 = make_tx();
    let tx2 = make_tx();
    let peer1 = make_peer_id(3001);
    let peer2 = make_peer_id(3002);
    let pkl3 = make_peer_key_location(3003);
    let pkl4 = make_peer_key_location(3004);

    let events = vec![
        // Two broadcasts from different senders, both missing peers 3 and 4
        NetLogMessage {
            tx: tx1,
            datetime: now(),
            peer_id: peer1.clone(),
            kind: EventKind::Put(PutEvent::BroadcastEmitted {
                id: tx1,
                upstream: make_peer_key_location(3001),
                broadcast_to: vec![make_peer_key_location(3002), pkl3.clone(), pkl4.clone()],
                broadcasted_to: 3,
                key,
                value: WrappedState::new(vec![1]),
                sender: make_peer_key_location(3001),
                timestamp: 100,
                state_hash: Some("aaaa".into()),
            }),
        },
        // Peer 2 receives tx1
        NetLogMessage {
            tx: tx1,
            datetime: now(),
            peer_id: peer2.clone(),
            kind: EventKind::Put(PutEvent::BroadcastReceived {
                id: tx1,
                requester: make_peer_key_location(3001),
                key,
                value: WrappedState::new(vec![1]),
                target: make_peer_key_location(3002),
                timestamp: 105,
                state_hash: Some("aaaa".into()),
            }),
        },
        // Second broadcast from peer2, also missing 3 and 4
        NetLogMessage {
            tx: tx2,
            datetime: now(),
            peer_id: peer2.clone(),
            kind: EventKind::Put(PutEvent::BroadcastEmitted {
                id: tx2,
                upstream: make_peer_key_location(3002),
                broadcast_to: vec![make_peer_key_location(3001), pkl3, pkl4],
                broadcasted_to: 3,
                key,
                value: WrappedState::new(vec![2]),
                sender: make_peer_key_location(3002),
                timestamp: 110,
                state_hash: Some("aaaa".into()),
            }),
        },
        // Peer 1 receives tx2
        NetLogMessage {
            tx: tx2,
            datetime: now(),
            peer_id: peer1.clone(),
            kind: EventKind::Put(PutEvent::BroadcastReceived {
                id: tx2,
                requester: make_peer_key_location(3002),
                key,
                value: WrappedState::new(vec![2]),
                target: make_peer_key_location(3001),
                timestamp: 115,
                state_hash: Some("aaaa".into()),
            }),
        },
        // Peers 3 and 4 NEVER receive anything -> partition
    ];

    let report = StateVerifier::from_events(events).verify();
    assert!(
        !report.suspected_partitions().is_empty(),
        "Expected partition: {}",
        report.display()
    );
}

#[test]
fn test_stale_peer_detected() {
    let key = make_contract_key();
    let put_tx = make_tx();
    let upd1 = make_update_tx();
    let upd2 = make_update_tx();
    let peer1 = make_peer_id(3001);
    let peer2 = make_peer_id(3002);
    let peer3 = make_peer_id(3003); // will be stale

    let t0 = now();
    let t1 = t0 + chrono::Duration::seconds(1);
    let t2 = t0 + chrono::Duration::seconds(2);
    let t3 = t0 + chrono::Duration::seconds(3);

    let events = vec![
        // All three peers get the initial PUT
        NetLogMessage {
            tx: put_tx,
            datetime: t0,
            peer_id: peer1.clone(),
            kind: EventKind::Put(PutEvent::PutSuccess {
                id: put_tx,
                requester: make_peer_key_location(3001),
                target: make_peer_key_location(3001),
                key,
                hop_count: Some(0),
                elapsed_ms: 10,
                timestamp: 100,
                state_hash: Some("aaa".into()),
            }),
        },
        NetLogMessage {
            tx: put_tx,
            datetime: t0,
            peer_id: peer2.clone(),
            kind: EventKind::Put(PutEvent::BroadcastReceived {
                id: put_tx,
                requester: make_peer_key_location(3001),
                key,
                value: WrappedState::new(vec![1]),
                target: make_peer_key_location(3002),
                timestamp: 101,
                state_hash: Some("aaa".into()),
            }),
        },
        NetLogMessage {
            tx: put_tx,
            datetime: t0,
            peer_id: peer3.clone(),
            kind: EventKind::Put(PutEvent::BroadcastReceived {
                id: put_tx,
                requester: make_peer_key_location(3001),
                key,
                value: WrappedState::new(vec![1]),
                target: make_peer_key_location(3003),
                timestamp: 102,
                state_hash: Some("aaa".into()),
            }),
        },
        // Peers 1 and 2 get two updates, peer3 gets nothing
        NetLogMessage {
            tx: upd1,
            datetime: t1,
            peer_id: peer1.clone(),
            kind: EventKind::Update(UpdateEvent::UpdateSuccess {
                id: upd1,
                requester: make_peer_key_location(3001),
                target: make_peer_key_location(3001),
                key,
                timestamp: 200,
                state_hash_before: Some("aaa".into()),
                state_hash_after: Some("bbb".into()),
            }),
        },
        NetLogMessage {
            tx: upd1,
            datetime: t2,
            peer_id: peer2.clone(),
            kind: EventKind::Update(UpdateEvent::BroadcastApplied {
                id: upd1,
                key,
                target: make_peer_key_location(3002),
                timestamp: 210,
                state_hash_before: Some("aaa".into()),
                state_hash_after: Some("bbb".into()),
                changed: true,
            }),
        },
        NetLogMessage {
            tx: upd2,
            datetime: t3,
            peer_id: peer1.clone(),
            kind: EventKind::Update(UpdateEvent::UpdateSuccess {
                id: upd2,
                requester: make_peer_key_location(3001),
                target: make_peer_key_location(3001),
                key,
                timestamp: 300,
                state_hash_before: Some("bbb".into()),
                state_hash_after: Some("ccc".into()),
            }),
        },
    ];

    let report = StateVerifier::from_events(events).verify();
    let stale = report.stale_peers();
    assert_eq!(stale.len(), 1, "Expected stale peer: {}", report.display());
    if let StateAnomaly::StalePeer { peer, .. } = stale[0] {
        assert_eq!(peer.port(), 3003);
    }
}

#[test]
fn test_state_oscillation_detected() {
    let key = make_contract_key();
    let peer1 = make_peer_id(3001);

    // Create a sequence: a -> b -> a -> b -> a (oscillation)
    let txs: Vec<Transaction> = (0..5).map(|_| make_update_tx()).collect();
    let mut events = vec![NetLogMessage {
        tx: txs[0],
        datetime: now(),
        peer_id: peer1.clone(),
        kind: EventKind::Put(PutEvent::PutSuccess {
            id: txs[0],
            requester: make_peer_key_location(3001),
            target: make_peer_key_location(3001),
            key,
            hop_count: Some(0),
            elapsed_ms: 10,
            timestamp: 100,
            state_hash: Some("aaa".into()),
        }),
    }];

    let hashes = ["bbb", "aaa", "bbb", "aaa"];
    for (i, hash) in hashes.iter().enumerate() {
        events.push(NetLogMessage {
            tx: txs[i + 1],
            datetime: now() + chrono::Duration::seconds(i as i64 + 1),
            peer_id: peer1.clone(),
            kind: EventKind::Update(UpdateEvent::BroadcastApplied {
                id: txs[i + 1],
                key,
                target: make_peer_key_location(3001),
                timestamp: 200 + i as u64,
                state_hash_before: Some(if i % 2 == 0 { "aaa" } else { "bbb" }.into()),
                state_hash_after: Some(hash.to_string()),
                changed: true,
            }),
        });
    }

    let report = StateVerifier::from_events(events).verify();
    assert!(
        !report.state_oscillations().is_empty(),
        "Expected oscillation: {}",
        report.display()
    );
}

#[test]
fn test_zombie_transaction_detected() {
    let key = make_contract_key();
    let tx = make_tx();
    let peer1 = make_peer_id(3001);

    let events = vec![
        // PUT request started but never completed
        NetLogMessage {
            tx,
            datetime: now(),
            peer_id: peer1.clone(),
            kind: EventKind::Put(PutEvent::Request {
                id: tx,
                requester: make_peer_key_location(3001),
                target: make_peer_key_location(3001),
                key,
                htl: 10,
                timestamp: 100,
            }),
        },
        // No PutSuccess or PutFailure or Timeout
    ];

    let report = StateVerifier::from_events(events).verify();
    assert_eq!(
        report.zombie_transactions().len(),
        1,
        "Expected zombie: {}",
        report.display()
    );
}

#[test]
fn test_zombie_not_flagged_on_completion() {
    let key = make_contract_key();
    let tx = make_tx();
    let peer1 = make_peer_id(3001);

    let events = vec![
        NetLogMessage {
            tx,
            datetime: now(),
            peer_id: peer1.clone(),
            kind: EventKind::Put(PutEvent::Request {
                id: tx,
                requester: make_peer_key_location(3001),
                target: make_peer_key_location(3001),
                key,
                htl: 10,
                timestamp: 100,
            }),
        },
        NetLogMessage {
            tx,
            datetime: now(),
            peer_id: peer1.clone(),
            kind: EventKind::Put(PutEvent::PutSuccess {
                id: tx,
                requester: make_peer_key_location(3001),
                target: make_peer_key_location(3001),
                key,
                hop_count: Some(0),
                elapsed_ms: 10,
                timestamp: 110,
                state_hash: Some("aaaa".into()),
            }),
        },
    ];

    let report = StateVerifier::from_events(events).verify();
    assert!(
        report.zombie_transactions().is_empty(),
        "Should not flag completed tx: {}",
        report.display()
    );
}

#[test]
fn test_broadcast_storm_detected() {
    let key = make_contract_key();
    let tx = make_tx();
    let peer1 = make_peer_id(3001);

    // Emit many broadcasts for the same transaction (simulating a loop)
    let mut events = vec![NetLogMessage {
        tx,
        datetime: now(),
        peer_id: peer1.clone(),
        kind: EventKind::Put(PutEvent::PutSuccess {
            id: tx,
            requester: make_peer_key_location(3001),
            target: make_peer_key_location(3001),
            key,
            hop_count: Some(0),
            elapsed_ms: 10,
            timestamp: 100,
            state_hash: Some("aaa".into()),
        }),
    }];

    // 10 broadcast emissions for same tx (way more than expected)
    for i in 0..10 {
        events.push(NetLogMessage {
            tx,
            datetime: now() + chrono::Duration::milliseconds(i),
            peer_id: peer1.clone(),
            kind: EventKind::Put(PutEvent::BroadcastEmitted {
                id: tx,
                upstream: make_peer_key_location(3001),
                broadcast_to: vec![make_peer_key_location(3002)],
                broadcasted_to: 1,
                key,
                value: WrappedState::new(vec![1]),
                sender: make_peer_key_location(3001),
                timestamp: 110 + i as u64,
                state_hash: Some("aaa".into()),
            }),
        });
    }

    let report = StateVerifier::from_events(events).verify();
    assert!(
        !report.broadcast_storms().is_empty(),
        "Expected broadcast storm: {}",
        report.display()
    );
}

#[test]
fn test_delta_sync_cascade_detected() {
    let key = make_contract_key();
    let peer1 = make_peer_id(3001);

    let t0 = now();
    let mut events = Vec::new();

    // 4 resync requests within 60 seconds from different peers
    for i in 0..4u16 {
        events.push(NetLogMessage {
            tx: *Transaction::NULL,
            datetime: t0 + chrono::Duration::seconds(i as i64 * 10),
            peer_id: peer1.clone(),
            kind: EventKind::InterestSync(InterestSyncEvent::ResyncRequestReceived {
                key,
                from_peer: make_peer_key_location(4000 + i),
                timestamp: 100 + i as u64 * 10,
            }),
        });
    }

    let report = StateVerifier::from_events(events).verify();
    assert!(
        !report.delta_sync_cascades().is_empty(),
        "Expected delta sync cascade: {}",
        report.display()
    );
}

#[test]
fn test_delta_sync_no_cascade_below_threshold() {
    let key = make_contract_key();
    let peer1 = make_peer_id(3001);

    // Only 2 resyncs - below threshold of 3
    let events = vec![
        NetLogMessage {
            tx: *Transaction::NULL,
            datetime: now(),
            peer_id: peer1.clone(),
            kind: EventKind::InterestSync(InterestSyncEvent::ResyncRequestReceived {
                key,
                from_peer: make_peer_key_location(4000),
                timestamp: 100,
            }),
        },
        NetLogMessage {
            tx: *Transaction::NULL,
            datetime: now(),
            peer_id: peer1.clone(),
            kind: EventKind::InterestSync(InterestSyncEvent::ResyncRequestReceived {
                key,
                from_peer: make_peer_key_location(4001),
                timestamp: 110,
            }),
        },
    ];

    let report = StateVerifier::from_events(events).verify();
    assert!(
        report.delta_sync_cascades().is_empty(),
        "Should not flag cascade with only 2 resyncs",
    );
}

#[test]
fn test_update_ordering_anomaly() {
    let key = make_contract_key();
    let tx_a = make_update_tx();
    let tx_b = make_update_tx();
    let peer1 = make_peer_id(3001);
    let peer2 = make_peer_id(3002);

    let events = vec![
        // Both peers start with same state
        NetLogMessage {
            tx: make_tx(),
            datetime: now(),
            peer_id: peer1.clone(),
            kind: EventKind::Put(PutEvent::PutSuccess {
                id: make_tx(),
                requester: make_peer_key_location(3001),
                target: make_peer_key_location(3001),
                key,
                hop_count: Some(0),
                elapsed_ms: 10,
                timestamp: 100,
                state_hash: Some("init".into()),
            }),
        },
        NetLogMessage {
            tx: make_tx(),
            datetime: now(),
            peer_id: peer2.clone(),
            kind: EventKind::Put(PutEvent::PutSuccess {
                id: make_tx(),
                requester: make_peer_key_location(3002),
                target: make_peer_key_location(3002),
                key,
                hop_count: Some(0),
                elapsed_ms: 10,
                timestamp: 100,
                state_hash: Some("init".into()),
            }),
        },
        // Peer1 applies A then B -> hash "ab_result"
        NetLogMessage {
            tx: tx_a,
            datetime: now(),
            peer_id: peer1.clone(),
            kind: EventKind::Update(UpdateEvent::BroadcastApplied {
                id: tx_a,
                key,
                target: make_peer_key_location(3001),
                timestamp: 200,
                state_hash_before: Some("init".into()),
                state_hash_after: Some("after_a".into()),
                changed: true,
            }),
        },
        NetLogMessage {
            tx: tx_b,
            datetime: now(),
            peer_id: peer1.clone(),
            kind: EventKind::Update(UpdateEvent::BroadcastApplied {
                id: tx_b,
                key,
                target: make_peer_key_location(3001),
                timestamp: 210,
                state_hash_before: Some("after_a".into()),
                state_hash_after: Some("ab_result".into()),
                changed: true,
            }),
        },
        // Peer2 applies B then A -> hash "ba_result" (different!)
        NetLogMessage {
            tx: tx_b,
            datetime: now(),
            peer_id: peer2.clone(),
            kind: EventKind::Update(UpdateEvent::BroadcastApplied {
                id: tx_b,
                key,
                target: make_peer_key_location(3002),
                timestamp: 200,
                state_hash_before: Some("init".into()),
                state_hash_after: Some("after_b".into()),
                changed: true,
            }),
        },
        NetLogMessage {
            tx: tx_a,
            datetime: now(),
            peer_id: peer2.clone(),
            kind: EventKind::Update(UpdateEvent::BroadcastApplied {
                id: tx_a,
                key,
                target: make_peer_key_location(3002),
                timestamp: 210,
                state_hash_before: Some("after_b".into()),
                state_hash_after: Some("ba_result".into()),
                changed: true,
            }),
        },
    ];

    let report = StateVerifier::from_events(events).verify();
    let ordering = report
        .anomalies
        .iter()
        .filter(|a| matches!(a, StateAnomaly::UpdateOrderingAnomaly { .. }))
        .count();
    assert!(
        ordering > 0,
        "Expected update ordering anomaly: {}",
        report.display()
    );
}

#[test]
fn test_no_ordering_anomaly_when_commutative() {
    let key = make_contract_key();
    let tx_a = make_update_tx();
    let tx_b = make_update_tx();
    let peer1 = make_peer_id(3001);
    let peer2 = make_peer_id(3002);

    let events = vec![
        NetLogMessage {
            tx: make_tx(),
            datetime: now(),
            peer_id: peer1.clone(),
            kind: EventKind::Put(PutEvent::PutSuccess {
                id: make_tx(),
                requester: make_peer_key_location(3001),
                target: make_peer_key_location(3001),
                key,
                hop_count: Some(0),
                elapsed_ms: 10,
                timestamp: 100,
                state_hash: Some("init".into()),
            }),
        },
        NetLogMessage {
            tx: make_tx(),
            datetime: now(),
            peer_id: peer2.clone(),
            kind: EventKind::Put(PutEvent::PutSuccess {
                id: make_tx(),
                requester: make_peer_key_location(3002),
                target: make_peer_key_location(3002),
                key,
                hop_count: Some(0),
                elapsed_ms: 10,
                timestamp: 100,
                state_hash: Some("init".into()),
            }),
        },
        // Peer1: A then B -> "same_result"
        NetLogMessage {
            tx: tx_a,
            datetime: now(),
            peer_id: peer1.clone(),
            kind: EventKind::Update(UpdateEvent::BroadcastApplied {
                id: tx_a,
                key,
                target: make_peer_key_location(3001),
                timestamp: 200,
                state_hash_before: Some("init".into()),
                state_hash_after: Some("mid".into()),
                changed: true,
            }),
        },
        NetLogMessage {
            tx: tx_b,
            datetime: now(),
            peer_id: peer1.clone(),
            kind: EventKind::Update(UpdateEvent::BroadcastApplied {
                id: tx_b,
                key,
                target: make_peer_key_location(3001),
                timestamp: 210,
                state_hash_before: Some("mid".into()),
                state_hash_after: Some("same_result".into()),
                changed: true,
            }),
        },
        // Peer2: B then A -> "same_result" (commutative, no anomaly)
        NetLogMessage {
            tx: tx_b,
            datetime: now(),
            peer_id: peer2.clone(),
            kind: EventKind::Update(UpdateEvent::BroadcastApplied {
                id: tx_b,
                key,
                target: make_peer_key_location(3002),
                timestamp: 200,
                state_hash_before: Some("init".into()),
                state_hash_after: Some("mid2".into()),
                changed: true,
            }),
        },
        NetLogMessage {
            tx: tx_a,
            datetime: now(),
            peer_id: peer2.clone(),
            kind: EventKind::Update(UpdateEvent::BroadcastApplied {
                id: tx_a,
                key,
                target: make_peer_key_location(3002),
                timestamp: 210,
                state_hash_before: Some("mid2".into()),
                state_hash_after: Some("same_result".into()),
                changed: true,
            }),
        },
    ];

    let report = StateVerifier::from_events(events).verify();
    let ordering = report
        .anomalies
        .iter()
        .filter(|a| matches!(a, StateAnomaly::UpdateOrderingAnomaly { .. }))
        .count();
    assert_eq!(
        ordering,
        0,
        "Should not flag commutative merge: {}",
        report.display()
    );
}

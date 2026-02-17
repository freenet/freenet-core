//! Anomaly detection algorithms for the state verifier.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::SocketAddr;

use chrono::{DateTime, Utc};

use super::types::{
    BroadcastSource, ContractStateHistory, DivergencePoint, StateAnomaly, StateTransition,
    TransitionKind,
};
use crate::message::Transaction;
use crate::tracing::{
    EventKind, GetEvent, InterestSyncEvent, PutEvent, SubscribeEvent, UpdateEvent,
};

impl super::StateVerifier {
    /// Detect broadcasts that were emitted but never received by some targets.
    pub(super) fn detect_missing_broadcasts(
        &self,
        history: &ContractStateHistory,
        anomalies: &mut Vec<StateAnomaly>,
    ) {
        for broadcast in &history.emitted_broadcasts {
            let missing: Vec<SocketAddr> = broadcast
                .targets
                .iter()
                .filter(|target| {
                    !history
                        .received_broadcasts
                        .contains(&(broadcast.transaction, **target))
                        && !history
                            .applied_broadcasts
                            .contains(&(broadcast.transaction, **target))
                })
                .copied()
                .collect();

            if !missing.is_empty() {
                anomalies.push(StateAnomaly::MissingBroadcast {
                    contract_key: history.contract_key.clone(),
                    transaction: broadcast.transaction,
                    sender: broadcast.sender,
                    missing_targets: missing,
                    state_hash: broadcast.state_hash.clone(),
                    source_op: broadcast.source_op.clone(),
                });
            }
        }
    }

    /// Detect broadcasts that were received but never applied (UPDATE only;
    /// PUT broadcasts are applied on receipt).
    pub(super) fn detect_unapplied_broadcasts(
        &self,
        history: &ContractStateHistory,
        anomalies: &mut Vec<StateAnomaly>,
    ) {
        for &(tx, peer) in &history.received_broadcasts {
            // Only check UPDATE broadcasts - PUT broadcasts don't have a separate apply step
            let is_update_broadcast = history
                .emitted_broadcasts
                .iter()
                .any(|b| b.transaction == tx && b.source_op == BroadcastSource::Update);

            if is_update_broadcast && !history.applied_broadcasts.contains(&(tx, peer)) {
                // Find the received state hash from the transitions
                let received_hash = self.events.iter().find_map(|e| {
                    if e.tx == tx && e.peer_id.addr == peer {
                        if let EventKind::Update(UpdateEvent::BroadcastReceived {
                            state_hash,
                            ..
                        }) = &e.kind
                        {
                            return state_hash.clone();
                        }
                    }
                    None
                });

                anomalies.push(StateAnomaly::BroadcastNotApplied {
                    contract_key: history.contract_key.clone(),
                    transaction: tx,
                    peer,
                    received_state_hash: received_hash,
                });
            }
        }
    }

    /// Detect unexpected state changes. This looks for cases where a peer's state
    /// transitions to a hash that differs from what most other peers have at the
    /// same point in the history.
    pub(super) fn detect_unexpected_state_changes(
        &self,
        history: &ContractStateHistory,
        anomalies: &mut Vec<StateAnomaly>,
    ) {
        // Track the set of "known good" hashes - any hash that has been stored by
        // at least one peer at any point. State transitions to unknown hashes may
        // indicate a merge/CRDT issue.
        let mut known_hashes: HashSet<String> = HashSet::new();

        for transition in &history.transitions {
            if let Some(ref before) = transition.state_before {
                known_hashes.insert(before.clone());
            }

            // On the very first transition for a contract, every hash is new.
            // After that, if a BroadcastApplied produces a hash we've never seen
            // from any other peer's stored state, flag it.
            let is_novel = !known_hashes.contains(&transition.state_after);

            // Only flag novel hashes from broadcast-applied events (merges),
            // since PutStored and UpdateApplied can legitimately introduce new hashes.
            if is_novel
                && matches!(
                    transition.kind,
                    TransitionKind::UpdateBroadcastApplied { .. }
                )
            {
                if let Some(ref before) = transition.state_before {
                    anomalies.push(StateAnomaly::UnexpectedStateChange {
                        contract_key: history.contract_key.clone(),
                        peer: transition.peer,
                        transaction: transition.transaction,
                        previous_hash: before.clone(),
                        new_hash: transition.state_after.clone(),
                        transition_kind: transition.kind.clone(),
                    });
                }
            }

            known_hashes.insert(transition.state_after.clone());
        }
    }

    /// Detect final divergence: after all events, do peers disagree on state?
    /// If so, trace back to find the first point of divergence.
    pub(super) fn detect_final_divergence(
        &self,
        history: &ContractStateHistory,
        anomalies: &mut Vec<StateAnomaly>,
    ) {
        if history.final_peer_states.len() < 2 {
            return;
        }

        let unique_hashes: HashSet<&String> = history.final_peer_states.values().collect();
        if unique_hashes.len() <= 1 {
            return; // All peers agree
        }

        // Find the majority hash (the one most peers have)
        let mut hash_counts: HashMap<&String, usize> = HashMap::new();
        for hash in history.final_peer_states.values() {
            *hash_counts.entry(hash).or_insert(0) += 1;
        }
        let majority_hash = hash_counts
            .iter()
            .max_by_key(|(_, count)| *count)
            .map(|(hash, _)| (*hash).clone())
            .unwrap_or_default();

        // Find the first transition where a peer's state diverged from the majority
        let first_divergence_point = self.find_first_divergence(history, &majority_hash);

        anomalies.push(StateAnomaly::FinalDivergence {
            contract_key: history.contract_key.clone(),
            peer_states: history
                .final_peer_states
                .iter()
                .map(|(addr, hash)| (*addr, hash.clone()))
                .collect(),
            first_divergence_point,
        });
    }

    /// Find the first transition that resulted in a divergent state from the majority.
    pub(super) fn find_first_divergence(
        &self,
        history: &ContractStateHistory,
        majority_hash: &str,
    ) -> Option<DivergencePoint> {
        // Walk transitions in order. Track per-peer states. The first time a peer
        // ends up with a different hash than the majority final state, that's our
        // divergence point.
        let divergent_peers: HashSet<SocketAddr> = history
            .final_peer_states
            .iter()
            .filter(|(_, hash)| hash.as_str() != majority_hash)
            .map(|(addr, _)| *addr)
            .collect();

        // Find the first transition on a divergent peer that moved it away from
        // what would become the majority state.
        for transition in &history.transitions {
            if divergent_peers.contains(&transition.peer) && transition.state_after != majority_hash
            {
                // Check if this is a meaningful divergence (not just an intermediate state)
                // by seeing if this is the LAST transition for this peer
                let is_final_state = history
                    .final_peer_states
                    .get(&transition.peer)
                    .map(|h| h == &transition.state_after)
                    .unwrap_or(false);

                if is_final_state {
                    return Some(DivergencePoint {
                        transition: transition.clone(),
                        majority_hash: majority_hash.to_string(),
                        divergent_hash: transition.state_after.clone(),
                    });
                }
            }
        }

        None
    }

    // ---- New detectors ----

    /// Correlate missing broadcasts: if >=2 broadcasts from different senders
    /// all miss the SAME set of targets, that's a partition signature.
    pub(super) fn detect_suspected_partition(
        &self,
        history: &ContractStateHistory,
        anomalies: &mut Vec<StateAnomaly>,
    ) {
        // Group broadcasts by sorted missing_targets
        let mut missing_by_targets: HashMap<Vec<SocketAddr>, Vec<(SocketAddr, DateTime<Utc>)>> =
            HashMap::new();

        for broadcast in &history.emitted_broadcasts {
            let mut missing: Vec<SocketAddr> = broadcast
                .targets
                .iter()
                .filter(|t| {
                    !history
                        .received_broadcasts
                        .contains(&(broadcast.transaction, **t))
                        && !history
                            .applied_broadcasts
                            .contains(&(broadcast.transaction, **t))
                })
                .copied()
                .collect();

            if !missing.is_empty() {
                missing.sort();
                missing_by_targets
                    .entry(missing)
                    .or_default()
                    .push((broadcast.sender, broadcast.timestamp));
            }
        }

        for (isolated, sender_times) in missing_by_targets {
            if sender_times.len() >= 2 {
                let senders: Vec<SocketAddr> = sender_times.iter().map(|(s, _)| *s).collect();
                // Safe: we checked sender_times.len() >= 2 above
                let Some(first) = sender_times.iter().map(|(_, t)| *t).min() else {
                    continue;
                };
                let Some(last) = sender_times.iter().map(|(_, t)| *t).max() else {
                    continue;
                };
                anomalies.push(StateAnomaly::SuspectedPartition {
                    contract_key: history.contract_key.clone(),
                    sending_group: senders,
                    isolated_group: isolated,
                    first_missed: first,
                    last_missed: last,
                    missed_count: sender_times.len(),
                });
            }
        }
    }

    /// Detect peers stuck on the initial PUT state while others have updates.
    pub(super) fn detect_stale_peers(
        &self,
        history: &ContractStateHistory,
        anomalies: &mut Vec<StateAnomaly>,
    ) {
        if history.transitions.is_empty() {
            return;
        }

        // Collect per-peer: last transition kind, hash, timestamp
        let mut peer_last: BTreeMap<SocketAddr, (&TransitionKind, &str, DateTime<Utc>)> =
            BTreeMap::new();
        for t in &history.transitions {
            peer_last.insert(t.peer, (&t.kind, &t.state_after, t.timestamp));
        }

        // Count update transitions total
        let update_transitions: Vec<&StateTransition> = history
            .transitions
            .iter()
            .filter(|t| {
                matches!(
                    t.kind,
                    TransitionKind::UpdateApplied | TransitionKind::UpdateBroadcastApplied { .. }
                )
            })
            .collect();

        if update_transitions.len() < 2 {
            return;
        }

        for (&peer, &(kind, hash, ts)) in &peer_last {
            let is_put_only = matches!(
                kind,
                TransitionKind::PutStored | TransitionKind::PutBroadcastReceived
            );
            if !is_put_only {
                continue;
            }
            // Count update transitions after this peer's last event
            let missed = update_transitions
                .iter()
                .filter(|t| t.timestamp > ts)
                .count();
            if missed >= 2 {
                anomalies.push(StateAnomaly::StalePeer {
                    contract_key: history.contract_key.clone(),
                    peer,
                    stuck_on_hash: hash.to_string(),
                    last_event_time: ts,
                    missed_updates: missed,
                });
            }
        }
    }

    /// Detect asymmetric broadcast delivery (A→B works but B→A doesn't).
    pub(super) fn detect_one_way_propagation(
        &self,
        history: &ContractStateHistory,
        anomalies: &mut Vec<StateAnomaly>,
    ) {
        // Build directed graph: (sender, receiver) -> delivery count
        let mut deliveries: HashMap<(SocketAddr, SocketAddr), usize> = HashMap::new();
        let mut intended: HashMap<(SocketAddr, SocketAddr), usize> = HashMap::new();

        for broadcast in &history.emitted_broadcasts {
            for target in &broadcast.targets {
                *intended.entry((broadcast.sender, *target)).or_insert(0) += 1;
                let received = history
                    .received_broadcasts
                    .contains(&(broadcast.transaction, *target))
                    || history
                        .applied_broadcasts
                        .contains(&(broadcast.transaction, *target));
                if received {
                    *deliveries.entry((broadcast.sender, *target)).or_insert(0) += 1;
                }
            }
        }

        // Check for asymmetry: A→B has deliveries but B→A has 0
        let mut checked = HashSet::new();
        for &(a, b) in deliveries.keys() {
            if checked.contains(&(a, b)) || checked.contains(&(b, a)) {
                continue;
            }
            checked.insert((a, b));
            let forward = *deliveries.get(&(a, b)).unwrap_or(&0);
            let reverse = *deliveries.get(&(b, a)).unwrap_or(&0);
            let b_intended_to_a = *intended.get(&(b, a)).unwrap_or(&0);

            // Only flag if B actually tried to send to A but none got through
            if forward > 0 && reverse == 0 && b_intended_to_a > 0 {
                anomalies.push(StateAnomaly::OneWayPropagationFailure {
                    contract_key: history.contract_key.clone(),
                    sender: a,
                    receiver: b,
                    forward_deliveries: forward,
                    reverse_deliveries: reverse,
                });
            }
        }
    }

    /// Detect a peer's state hash reverting to a previously-seen value.
    pub(super) fn detect_state_oscillation(
        &self,
        history: &ContractStateHistory,
        anomalies: &mut Vec<StateAnomaly>,
    ) {
        // Group transitions by peer
        let mut per_peer: BTreeMap<SocketAddr, Vec<&str>> = BTreeMap::new();
        for t in &history.transitions {
            per_peer.entry(t.peer).or_default().push(&t.state_after);
        }

        for (peer, hashes) in per_peer {
            if hashes.len() < 3 {
                continue;
            }
            // Count reversions: how many times we see a hash we've seen before
            let mut seen: HashSet<&str> = HashSet::new();
            let mut reversion_hashes: HashSet<&str> = HashSet::new();
            let mut flip_count = 0usize;

            for &h in &hashes {
                if seen.contains(h) {
                    flip_count += 1;
                    reversion_hashes.insert(h);
                }
                seen.insert(h);
            }

            if flip_count >= 2 {
                anomalies.push(StateAnomaly::StateOscillation {
                    contract_key: history.contract_key.clone(),
                    peer,
                    oscillating_hashes: reversion_hashes.into_iter().map(String::from).collect(),
                    flip_count,
                });
            }
        }
    }

    /// Detect non-commutative merges: same two updates applied in different
    /// order on different peers produce different resulting hashes.
    pub(super) fn detect_update_ordering(
        &self,
        history: &ContractStateHistory,
        anomalies: &mut Vec<StateAnomaly>,
    ) {
        // Collect per-peer update transaction order (only updates, not puts)
        let mut per_peer_updates: BTreeMap<SocketAddr, Vec<(Transaction, String)>> =
            BTreeMap::new();
        for t in &history.transitions {
            if matches!(
                t.kind,
                TransitionKind::UpdateApplied | TransitionKind::UpdateBroadcastApplied { .. }
            ) {
                per_peer_updates
                    .entry(t.peer)
                    .or_default()
                    .push((t.transaction, t.state_after.clone()));
            }
        }

        let peers: Vec<SocketAddr> = per_peer_updates.keys().copied().collect();
        for i in 0..peers.len() {
            for j in (i + 1)..peers.len() {
                let updates_a = &per_peer_updates[&peers[i]];
                let updates_b = &per_peer_updates[&peers[j]];

                // Find pairs of transactions that appear on both peers
                let txs_a: Vec<Transaction> = updates_a.iter().map(|(tx, _)| *tx).collect();
                let txs_b: Vec<Transaction> = updates_b.iter().map(|(tx, _)| *tx).collect();

                let common: HashSet<Transaction> = txs_a
                    .iter()
                    .copied()
                    .collect::<HashSet<_>>()
                    .intersection(&txs_b.iter().copied().collect())
                    .copied()
                    .collect();

                if common.len() < 2 {
                    continue;
                }

                // Check if any two common txs appear in different order
                let order_a: Vec<Transaction> = txs_a
                    .iter()
                    .filter(|t| common.contains(t))
                    .copied()
                    .collect();
                let order_b: Vec<Transaction> = txs_b
                    .iter()
                    .filter(|t| common.contains(t))
                    .copied()
                    .collect();

                for k in 0..order_a.len() {
                    for l in (k + 1)..order_a.len() {
                        let tx1 = order_a[k];
                        let tx2 = order_a[l];
                        // Check if these appear in reversed order on peer B
                        let pos_b1 = order_b.iter().position(|t| *t == tx1);
                        let pos_b2 = order_b.iter().position(|t| *t == tx2);
                        if let (Some(pb1), Some(pb2)) = (pos_b1, pos_b2) {
                            if pb1 > pb2 {
                                // Different order! Check if the hash after both differs
                                let hash_a = updates_a
                                    .iter()
                                    .rfind(|(tx, _)| *tx == tx2)
                                    .map(|(_, h)| h.as_str());
                                let hash_b = updates_b
                                    .iter()
                                    .rfind(|(tx, _)| *tx == tx1)
                                    .map(|(_, h)| h.as_str());

                                if let (Some(ha), Some(hb)) = (hash_a, hash_b) {
                                    if ha != hb {
                                        anomalies.push(StateAnomaly::UpdateOrderingAnomaly {
                                            contract_key: history.contract_key.clone(),
                                            tx_a: tx1,
                                            tx_b: tx2,
                                            peer_ab: peers[i],
                                            hash_ab: ha.to_string(),
                                            peer_ba: peers[j],
                                            hash_ba: hb.to_string(),
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// Detect broadcast storms: a single transaction emitting an unexpectedly
    /// large number of broadcasts.
    pub(super) fn detect_broadcast_storms(
        &self,
        history: &ContractStateHistory,
        anomalies: &mut Vec<StateAnomaly>,
    ) {
        let expected_max = history.final_peer_states.len().max(1);

        // Group broadcasts by transaction
        let mut by_tx: HashMap<Transaction, (usize, BroadcastSource)> = HashMap::new();
        for b in &history.emitted_broadcasts {
            let entry = by_tx
                .entry(b.transaction)
                .or_insert((0, b.source_op.clone()));
            entry.0 += 1;
        }

        for (tx, (count, source)) in by_tx {
            // Flag if count > 2x expected_max or count > 10 (absolute threshold)
            if count > expected_max * 2 && count > 3 {
                anomalies.push(StateAnomaly::BroadcastStorm {
                    contract_key: history.contract_key.clone(),
                    transaction: tx,
                    broadcast_count: count,
                    expected_max,
                    source_op: source,
                });
            }
        }
    }

    /// Detect subscription asymmetry: a peer subscribed but never targeted
    /// by broadcasts for that contract.
    pub(super) fn detect_subscription_asymmetry(
        &self,
        history: &ContractStateHistory,
        anomalies: &mut Vec<StateAnomaly>,
    ) {
        if history.emitted_broadcasts.is_empty() {
            return;
        }

        // Find SubscribeSuccess events matching this contract
        let mut subscribed: HashMap<SocketAddr, Transaction> = HashMap::new();
        for event in &self.events {
            if let EventKind::Subscribe(SubscribeEvent::SubscribeSuccess {
                id,
                key,
                requester,
                ..
            }) = &event.kind
            {
                let key_str = format!("{:?}", key);
                if key_str == history.contract_key {
                    if let Some(addr) = requester.socket_addr() {
                        subscribed.insert(addr, *id);
                    }
                }
            }
        }

        // Collect all peers ever targeted by broadcasts
        let mut targeted_peers: HashSet<SocketAddr> = HashSet::new();
        for b in &history.emitted_broadcasts {
            for t in &b.targets {
                targeted_peers.insert(*t);
            }
        }

        for (peer, sub_tx) in subscribed {
            if !targeted_peers.contains(&peer) {
                anomalies.push(StateAnomaly::SubscriptionAsymmetry {
                    contract_key: history.contract_key.clone(),
                    peer,
                    subscribe_tx: sub_tx,
                    broadcasts_without_peer: history.emitted_broadcasts.len(),
                });
            }
        }
    }

    /// Detect transactions that started but never completed.
    pub(super) fn detect_zombie_transactions(&self, anomalies: &mut Vec<StateAnomaly>) {
        // Track: tx -> (op_type, peer, started_at, completed)
        let mut tx_states: HashMap<Transaction, (String, SocketAddr, DateTime<Utc>, bool)> =
            HashMap::new();

        for event in &self.events {
            match &event.kind {
                EventKind::Put(PutEvent::Request { id, .. }) => {
                    tx_states.entry(*id).or_insert((
                        "put".into(),
                        event.peer_id.addr,
                        event.datetime,
                        false,
                    ));
                }
                EventKind::Put(PutEvent::PutSuccess { id, .. })
                | EventKind::Put(PutEvent::PutFailure { id, .. }) => {
                    if let Some(entry) = tx_states.get_mut(id) {
                        entry.3 = true;
                    }
                }
                EventKind::Get(GetEvent::Request { id, .. }) => {
                    tx_states.entry(*id).or_insert((
                        "get".into(),
                        event.peer_id.addr,
                        event.datetime,
                        false,
                    ));
                }
                EventKind::Get(GetEvent::GetSuccess { id, .. })
                | EventKind::Get(GetEvent::GetNotFound { id, .. })
                | EventKind::Get(GetEvent::GetFailure { id, .. }) => {
                    if let Some(entry) = tx_states.get_mut(id) {
                        entry.3 = true;
                    }
                }
                EventKind::Update(UpdateEvent::Request { id, .. }) => {
                    tx_states.entry(*id).or_insert((
                        "update".into(),
                        event.peer_id.addr,
                        event.datetime,
                        false,
                    ));
                }
                EventKind::Update(UpdateEvent::UpdateSuccess { id, .. }) => {
                    if let Some(entry) = tx_states.get_mut(id) {
                        entry.3 = true;
                    }
                }
                EventKind::Subscribe(SubscribeEvent::Request { id, .. }) => {
                    tx_states.entry(*id).or_insert((
                        "subscribe".into(),
                        event.peer_id.addr,
                        event.datetime,
                        false,
                    ));
                }
                EventKind::Subscribe(SubscribeEvent::SubscribeSuccess { id, .. })
                | EventKind::Subscribe(SubscribeEvent::SubscribeNotFound { id, .. }) => {
                    if let Some(entry) = tx_states.get_mut(id) {
                        entry.3 = true;
                    }
                }
                EventKind::Timeout { id, .. } => {
                    if let Some(entry) = tx_states.get_mut(id) {
                        entry.3 = true;
                    }
                }
                _ => {}
            }
        }

        for (tx, (op_type, peer, started_at, completed)) in tx_states {
            if !completed {
                anomalies.push(StateAnomaly::ZombieTransaction {
                    operation_type: op_type,
                    transaction: tx,
                    peer,
                    started_at,
                });
            }
        }
    }

    /// Detect clustered delta sync failures (>=3 resyncs for same contract in 60s).
    pub(super) fn detect_delta_sync_cascades(&self, anomalies: &mut Vec<StateAnomaly>) {
        // Collect resync events per contract
        let mut resyncs: BTreeMap<String, Vec<(SocketAddr, DateTime<Utc>)>> = BTreeMap::new();
        let mut resync_bytes: BTreeMap<String, usize> = BTreeMap::new();

        for event in &self.events {
            match &event.kind {
                EventKind::InterestSync(InterestSyncEvent::ResyncRequestReceived {
                    key,
                    from_peer,
                    ..
                }) => {
                    let key_str = format!("{:?}", key);
                    if let Some(addr) = from_peer.socket_addr() {
                        resyncs
                            .entry(key_str)
                            .or_default()
                            .push((addr, event.datetime));
                    }
                }
                EventKind::InterestSync(InterestSyncEvent::ResyncResponseSent {
                    key,
                    state_size,
                    ..
                }) => {
                    let key_str = format!("{:?}", key);
                    *resync_bytes.entry(key_str).or_insert(0) += state_size;
                }
                _ => {}
            }
        }

        let window = chrono::Duration::seconds(60);
        for (contract_key, mut events) in resyncs {
            events.sort_by_key(|(_, t)| *t);
            if events.len() < 3 {
                continue;
            }

            // Sliding window: find clusters of >=3 within 60s
            let mut i = 0;
            while i < events.len() {
                let start = events[i].1;
                let mut j = i;
                while j < events.len() && events[j].1 - start <= window {
                    j += 1;
                }
                let cluster_size = j - i;
                if cluster_size >= 3 {
                    let peers: Vec<SocketAddr> = events[i..j].iter().map(|(a, _)| *a).collect();
                    anomalies.push(StateAnomaly::DeltaSyncFailureCascade {
                        contract_key: contract_key.clone(),
                        resync_count: cluster_size,
                        requesting_peers: peers,
                        window_start: events[i].1,
                        window_end: events[j - 1].1,
                        total_resync_bytes: *resync_bytes.get(&contract_key).unwrap_or(&0),
                    });
                    break; // One cascade per contract is enough
                }
                i += 1;
            }
        }
    }
}

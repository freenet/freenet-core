//! Automatic state verification through telemetry linearization.
//!
//! This module analyzes network event logs to build a causal history of state
//! changes per contract and detect anomalies that lead to state incoherence.
//!
//! # Problem
//!
//! The existing convergence checker (`check_convergence`) only takes a point-in-time
//! snapshot. It tells you IF states diverged, but not WHY or WHERE in the event
//! sequence the divergence originated. This module fills that gap.
//!
//! # Approach
//!
//! 1. **Linearize**: Collect all state-mutating events across all peers, ordered
//!    chronologically, producing a per-contract timeline of state transitions.
//!
//! 2. **Build causal graph**: For each contract, track how state flows through the
//!    network: which peer stored it first, which broadcasts were emitted, which were
//!    received and applied.
//!
//! 3. **Detect anomalies**: Compare the expected propagation pattern against what
//!    actually happened. Flag missing broadcasts, unapplied updates, state regressions,
//!    and final divergences with their likely root cause.
//!
//! # Usage
//!
//! ```ignore
//! use freenet::tracing::state_verifier::StateVerifier;
//!
//! // From in-memory test events
//! let verifier = StateVerifier::from_events(events);
//! let report = verifier.verify();
//!
//! if !report.anomalies.is_empty() {
//!     eprintln!("{}", report.display());
//!     for anomaly in &report.anomalies {
//!         eprintln!("  {}", anomaly);
//!     }
//! }
//! ```

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt,
    net::SocketAddr,
};

use chrono::{DateTime, Utc};

use super::{EventKind, NetLogMessage, PutEvent, UpdateEvent};
use crate::message::Transaction;

/// A single state transition on a specific peer for a specific contract.
#[derive(Debug, Clone)]
pub struct StateTransition {
    /// Chronological position in the global event log.
    pub sequence: usize,
    /// When this event was recorded.
    pub timestamp: DateTime<Utc>,
    /// The transaction that caused this transition.
    pub transaction: Transaction,
    /// The peer where this transition occurred.
    pub peer: SocketAddr,
    /// What kind of transition this was.
    pub kind: TransitionKind,
    /// State hash before the transition (if available).
    pub state_before: Option<String>,
    /// State hash after the transition.
    pub state_after: String,
}

/// The type of state transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransitionKind {
    /// Initial PUT stored on this peer.
    PutStored,
    /// Local update applied (originator).
    UpdateApplied,
    /// Broadcast from a PUT was received and stored.
    PutBroadcastReceived,
    /// Broadcast from an UPDATE was received and applied.
    UpdateBroadcastApplied {
        /// Whether the local state actually changed.
        changed: bool,
    },
}

impl fmt::Display for TransitionKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransitionKind::PutStored => write!(f, "PutStored"),
            TransitionKind::UpdateApplied => write!(f, "UpdateApplied"),
            TransitionKind::PutBroadcastReceived => write!(f, "PutBroadcastReceived"),
            TransitionKind::UpdateBroadcastApplied { changed } => {
                write!(f, "UpdateBroadcastApplied(changed={})", changed)
            }
        }
    }
}

/// A broadcast that was emitted but whose reception status is tracked.
#[derive(Debug, Clone)]
pub struct TrackedBroadcast {
    /// The transaction for this broadcast.
    pub transaction: Transaction,
    /// The peer that emitted the broadcast.
    pub sender: SocketAddr,
    /// The peers the broadcast was sent to.
    pub targets: Vec<SocketAddr>,
    /// State hash that was broadcast.
    pub state_hash: Option<String>,
    /// Timestamp of emission.
    pub timestamp: DateTime<Utc>,
    /// Whether this is from a PUT or UPDATE operation.
    pub source_op: BroadcastSource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BroadcastSource {
    Put,
    Update,
}

/// A detected anomaly in the state propagation.
#[derive(Debug, Clone)]
pub enum StateAnomaly {
    /// A broadcast was emitted but never received by one or more targets.
    MissingBroadcast {
        contract_key: String,
        transaction: Transaction,
        sender: SocketAddr,
        missing_targets: Vec<SocketAddr>,
        state_hash: Option<String>,
        source_op: BroadcastSource,
    },
    /// A broadcast was received but never applied (no corresponding BroadcastApplied).
    BroadcastNotApplied {
        contract_key: String,
        transaction: Transaction,
        peer: SocketAddr,
        received_state_hash: Option<String>,
    },
    /// A peer's state hash changed to a value not consistent with the expected
    /// progression (potential state regression or corruption).
    UnexpectedStateChange {
        contract_key: String,
        peer: SocketAddr,
        transaction: Transaction,
        previous_hash: String,
        new_hash: String,
        transition_kind: TransitionKind,
    },
    /// After all events, peers hold different states for the same contract.
    /// Includes the likely root cause transition.
    FinalDivergence {
        contract_key: String,
        /// (peer_addr, state_hash) for each replica.
        peer_states: Vec<(SocketAddr, String)>,
        /// The first transition where states started to diverge.
        first_divergence_point: Option<DivergencePoint>,
    },
}

/// Information about where divergence first appeared.
#[derive(Debug, Clone)]
pub struct DivergencePoint {
    /// The transition that introduced the divergent state.
    pub transition: StateTransition,
    /// The hash that other peers have.
    pub majority_hash: String,
    /// The divergent hash this peer ended up with.
    pub divergent_hash: String,
}

impl fmt::Display for StateAnomaly {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StateAnomaly::MissingBroadcast {
                contract_key,
                transaction,
                sender,
                missing_targets,
                state_hash,
                source_op,
            } => {
                write!(
                    f,
                    "MISSING_BROADCAST: contract={}, tx={}, source={:?}, sender={}, \
                     state={}, missing_targets={:?}",
                    contract_key,
                    transaction,
                    source_op,
                    sender,
                    state_hash.as_deref().unwrap_or("?"),
                    missing_targets
                )
            }
            StateAnomaly::BroadcastNotApplied {
                contract_key,
                transaction,
                peer,
                received_state_hash,
            } => {
                write!(
                    f,
                    "BROADCAST_NOT_APPLIED: contract={}, tx={}, peer={}, received_state={}",
                    contract_key,
                    transaction,
                    peer,
                    received_state_hash.as_deref().unwrap_or("?")
                )
            }
            StateAnomaly::UnexpectedStateChange {
                contract_key,
                peer,
                transaction,
                previous_hash,
                new_hash,
                transition_kind,
            } => {
                write!(
                    f,
                    "UNEXPECTED_STATE_CHANGE: contract={}, peer={}, tx={}, kind={}, \
                     {} -> {}",
                    contract_key, peer, transaction, transition_kind, previous_hash, new_hash
                )
            }
            StateAnomaly::FinalDivergence {
                contract_key,
                peer_states,
                first_divergence_point,
            } => {
                write!(
                    f,
                    "FINAL_DIVERGENCE: contract={}, states=[{}]",
                    contract_key,
                    peer_states
                        .iter()
                        .map(|(addr, hash)| format!("{}={}", addr, hash))
                        .collect::<Vec<_>>()
                        .join(", ")
                )?;
                if let Some(dp) = first_divergence_point {
                    write!(
                        f,
                        ", first_divergence_at: peer={} tx={} ({} vs majority {})",
                        dp.transition.peer,
                        dp.transition.transaction,
                        dp.divergent_hash,
                        dp.majority_hash
                    )?;
                }
                Ok(())
            }
        }
    }
}

/// Per-contract linearized state history.
#[derive(Debug, Clone)]
pub struct ContractStateHistory {
    /// The contract key (formatted for display).
    pub contract_key: String,
    /// All state transitions in chronological order.
    pub transitions: Vec<StateTransition>,
    /// Broadcasts emitted (transaction -> broadcast info).
    pub emitted_broadcasts: Vec<TrackedBroadcast>,
    /// Broadcasts received (transaction, peer) pairs.
    pub received_broadcasts: HashSet<(Transaction, SocketAddr)>,
    /// Broadcasts applied (transaction, peer) pairs.
    pub applied_broadcasts: HashSet<(Transaction, SocketAddr)>,
    /// Final state per peer after all events.
    pub final_peer_states: BTreeMap<SocketAddr, String>,
}

/// Full verification report.
#[derive(Debug, Clone)]
pub struct VerificationReport {
    /// Per-contract state history.
    pub contract_histories: Vec<ContractStateHistory>,
    /// All detected anomalies.
    pub anomalies: Vec<StateAnomaly>,
    /// Total events analyzed.
    pub total_events: usize,
    /// Total state-mutating events.
    pub state_events: usize,
    /// Number of contracts analyzed.
    pub contracts_analyzed: usize,
}

impl VerificationReport {
    /// Returns true if no anomalies were detected.
    pub fn is_clean(&self) -> bool {
        self.anomalies.is_empty()
    }

    /// Returns only divergence anomalies.
    pub fn divergences(&self) -> Vec<&StateAnomaly> {
        self.anomalies
            .iter()
            .filter(|a| matches!(a, StateAnomaly::FinalDivergence { .. }))
            .collect()
    }

    /// Returns only missing broadcast anomalies.
    pub fn missing_broadcasts(&self) -> Vec<&StateAnomaly> {
        self.anomalies
            .iter()
            .filter(|a| matches!(a, StateAnomaly::MissingBroadcast { .. }))
            .collect()
    }

    /// Returns only unapplied broadcast anomalies.
    pub fn unapplied_broadcasts(&self) -> Vec<&StateAnomaly> {
        self.anomalies
            .iter()
            .filter(|a| matches!(a, StateAnomaly::BroadcastNotApplied { .. }))
            .collect()
    }

    /// Produce a human-readable summary.
    pub fn display(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!(
            "=== State Verification Report ===\n\
             Events analyzed: {} ({} state-mutating)\n\
             Contracts: {}\n\
             Anomalies: {}\n",
            self.total_events,
            self.state_events,
            self.contracts_analyzed,
            self.anomalies.len(),
        ));

        if self.anomalies.is_empty() {
            out.push_str("  No anomalies detected. All contracts converged.\n");
        } else {
            let missing = self.missing_broadcasts().len();
            let unapplied = self.unapplied_broadcasts().len();
            let divergences = self.divergences().len();
            let unexpected = self
                .anomalies
                .iter()
                .filter(|a| matches!(a, StateAnomaly::UnexpectedStateChange { .. }))
                .count();

            out.push_str(&format!(
                "  Missing broadcasts: {}\n\
                 Unapplied broadcasts: {}\n\
                 Unexpected state changes: {}\n\
                 Final divergences: {}\n",
                missing, unapplied, unexpected, divergences
            ));
        }

        for history in &self.contract_histories {
            out.push_str(&format!(
                "\n--- Contract: {} ---\n\
                 Transitions: {}, Broadcasts emitted: {}\n\
                 Final states: {}\n",
                history.contract_key,
                history.transitions.len(),
                history.emitted_broadcasts.len(),
                history
                    .final_peer_states
                    .iter()
                    .map(|(addr, hash)| format!("  {}={}", addr, hash))
                    .collect::<Vec<_>>()
                    .join("\n"),
            ));
        }

        if !self.anomalies.is_empty() {
            out.push_str("\n--- Anomalies ---\n");
            for (i, anomaly) in self.anomalies.iter().enumerate() {
                out.push_str(&format!("  [{}] {}\n", i + 1, anomaly));
            }
        }

        out
    }
}

/// The main state verifier. Consumes event logs and produces a verification report.
pub struct StateVerifier {
    events: Vec<NetLogMessage>,
}

impl StateVerifier {
    /// Create a verifier from a list of network log messages.
    pub fn from_events(events: Vec<NetLogMessage>) -> Self {
        Self { events }
    }

    /// Create a verifier from an EventLogAggregator (AOF files or WebSocket collector).
    ///
    /// This allows post-mortem analysis of event logs from real or simulated nodes:
    ///
    /// ```ignore
    /// use freenet::tracing::{EventLogAggregator, StateVerifier};
    /// use std::path::PathBuf;
    ///
    /// let aggregator = EventLogAggregator::from_aof_files(vec![
    ///     (PathBuf::from("/tmp/node_a/event_log"), Some("node-a".into())),
    ///     (PathBuf::from("/tmp/node_b/event_log"), Some("node-b".into())),
    /// ]).await?;
    ///
    /// let verifier = StateVerifier::from_aggregator(&aggregator).await?;
    /// let report = verifier.verify();
    /// println!("{}", report.display());
    /// ```
    pub async fn from_aggregator<S: super::event_aggregator::EventSource>(
        aggregator: &super::event_aggregator::EventLogAggregator<S>,
    ) -> Result<Self, anyhow::Error> {
        let events = aggregator.get_all_events().await?;
        Ok(Self { events })
    }

    /// Create a verifier directly from AOF event log file paths.
    ///
    /// Convenience method for the common case of analyzing node event logs
    /// after a test or production run.
    ///
    /// ```ignore
    /// use freenet::tracing::StateVerifier;
    /// use std::path::PathBuf;
    ///
    /// let verifier = StateVerifier::from_aof_paths(vec![
    ///     (PathBuf::from("/tmp/node_a/_EVENT_LOG_LOCAL"), Some("node-a".into())),
    ///     (PathBuf::from("/tmp/node_b/_EVENT_LOG_LOCAL"), Some("node-b".into())),
    /// ]).await?;
    ///
    /// let report = verifier.verify();
    /// if !report.is_clean() {
    ///     eprintln!("{}", report.display());
    /// }
    /// ```
    pub async fn from_aof_paths(
        paths: Vec<(std::path::PathBuf, Option<String>)>,
    ) -> Result<Self, anyhow::Error> {
        let aggregator = super::event_aggregator::EventLogAggregator::from_aof_files(paths).await?;
        Self::from_aggregator(&aggregator).await
    }

    /// Run full verification and return a report.
    pub fn verify(&self) -> VerificationReport {
        let histories = self.build_histories();
        let anomalies = self.detect_anomalies(&histories);

        let state_events: usize = histories.iter().map(|h| h.transitions.len()).sum();

        VerificationReport {
            contracts_analyzed: histories.len(),
            total_events: self.events.len(),
            state_events,
            anomalies,
            contract_histories: histories,
        }
    }

    /// Build per-contract state histories from the event log.
    fn build_histories(&self) -> Vec<ContractStateHistory> {
        // Group events by contract key
        let mut contract_events: BTreeMap<String, Vec<(usize, &NetLogMessage)>> = BTreeMap::new();

        for (seq, event) in self.events.iter().enumerate() {
            if let Some(key) = event.kind.contract_key() {
                let key_str = format!("{:?}", key);
                contract_events
                    .entry(key_str)
                    .or_default()
                    .push((seq, event));
            }
        }

        contract_events
            .into_iter()
            .map(|(contract_key, events)| self.build_contract_history(contract_key, &events))
            .collect()
    }

    /// Build a history for a single contract from its events.
    fn build_contract_history(
        &self,
        contract_key: String,
        events: &[(usize, &NetLogMessage)],
    ) -> ContractStateHistory {
        let mut transitions = Vec::new();
        let mut emitted_broadcasts = Vec::new();
        let mut received_broadcasts = HashSet::new();
        let mut applied_broadcasts = HashSet::new();
        let mut peer_states: BTreeMap<SocketAddr, String> = BTreeMap::new();

        for &(seq, event) in events {
            let peer = event.peer_id.addr;

            match &event.kind {
                // PUT events
                EventKind::Put(put_event) => match put_event {
                    PutEvent::PutSuccess { state_hash, .. } => {
                        if let Some(hash) = state_hash {
                            let prev = peer_states.get(&peer).cloned();
                            peer_states.insert(peer, hash.clone());
                            transitions.push(StateTransition {
                                sequence: seq,
                                timestamp: event.datetime,
                                transaction: event.tx,
                                peer,
                                kind: TransitionKind::PutStored,
                                state_before: prev,
                                state_after: hash.clone(),
                            });
                        }
                    }
                    PutEvent::BroadcastEmitted {
                        broadcast_to,
                        value: _,
                        state_hash,
                        ..
                    } => {
                        let targets: Vec<SocketAddr> = broadcast_to
                            .iter()
                            .filter_map(|pkl| pkl.socket_addr())
                            .collect();
                        emitted_broadcasts.push(TrackedBroadcast {
                            transaction: event.tx,
                            sender: peer,
                            targets,
                            state_hash: state_hash.clone(),
                            timestamp: event.datetime,
                            source_op: BroadcastSource::Put,
                        });
                    }
                    PutEvent::BroadcastReceived { state_hash, .. } => {
                        received_broadcasts.insert((event.tx, peer));
                        // For PUT broadcasts, receiving IS storing (no separate apply step)
                        if let Some(hash) = state_hash {
                            let prev = peer_states.get(&peer).cloned();
                            peer_states.insert(peer, hash.clone());
                            transitions.push(StateTransition {
                                sequence: seq,
                                timestamp: event.datetime,
                                transaction: event.tx,
                                peer,
                                kind: TransitionKind::PutBroadcastReceived,
                                state_before: prev,
                                state_after: hash.clone(),
                            });
                            applied_broadcasts.insert((event.tx, peer));
                        }
                    }
                    _ => {}
                },

                // UPDATE events
                EventKind::Update(update_event) => match update_event {
                    UpdateEvent::UpdateSuccess {
                        state_hash_before,
                        state_hash_after,
                        ..
                    } => {
                        if let Some(hash_after) = state_hash_after {
                            let prev = state_hash_before
                                .clone()
                                .or_else(|| peer_states.get(&peer).cloned());
                            peer_states.insert(peer, hash_after.clone());
                            transitions.push(StateTransition {
                                sequence: seq,
                                timestamp: event.datetime,
                                transaction: event.tx,
                                peer,
                                kind: TransitionKind::UpdateApplied,
                                state_before: prev,
                                state_after: hash_after.clone(),
                            });
                        }
                    }
                    UpdateEvent::BroadcastEmitted {
                        broadcast_to,
                        value: _,
                        state_hash,
                        ..
                    } => {
                        let targets: Vec<SocketAddr> = broadcast_to
                            .iter()
                            .filter_map(|pkl| pkl.socket_addr())
                            .collect();
                        emitted_broadcasts.push(TrackedBroadcast {
                            transaction: event.tx,
                            sender: peer,
                            targets,
                            state_hash: state_hash.clone(),
                            timestamp: event.datetime,
                            source_op: BroadcastSource::Update,
                        });
                    }
                    UpdateEvent::BroadcastReceived { .. } => {
                        received_broadcasts.insert((event.tx, peer));
                    }
                    UpdateEvent::BroadcastApplied {
                        state_hash_before,
                        state_hash_after,
                        changed,
                        ..
                    } => {
                        applied_broadcasts.insert((event.tx, peer));
                        if let Some(hash_after) = state_hash_after {
                            let prev = state_hash_before
                                .clone()
                                .or_else(|| peer_states.get(&peer).cloned());
                            peer_states.insert(peer, hash_after.clone());
                            transitions.push(StateTransition {
                                sequence: seq,
                                timestamp: event.datetime,
                                transaction: event.tx,
                                peer,
                                kind: TransitionKind::UpdateBroadcastApplied { changed: *changed },
                                state_before: prev,
                                state_after: hash_after.clone(),
                            });
                        }
                    }
                    _ => {}
                },
                _ => {}
            }
        }

        ContractStateHistory {
            contract_key,
            transitions,
            emitted_broadcasts,
            received_broadcasts,
            applied_broadcasts,
            final_peer_states: peer_states,
        }
    }

    /// Detect anomalies across all contract histories.
    fn detect_anomalies(&self, histories: &[ContractStateHistory]) -> Vec<StateAnomaly> {
        let mut anomalies = Vec::new();

        for history in histories {
            self.detect_missing_broadcasts(history, &mut anomalies);
            self.detect_unapplied_broadcasts(history, &mut anomalies);
            self.detect_unexpected_state_changes(history, &mut anomalies);
            self.detect_final_divergence(history, &mut anomalies);
        }

        anomalies
    }

    /// Detect broadcasts that were emitted but never received by some targets.
    fn detect_missing_broadcasts(
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
    fn detect_unapplied_broadcasts(
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
    fn detect_unexpected_state_changes(
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
    fn detect_final_divergence(
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
    fn find_first_divergence(
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::PeerId;
    use crate::operations::put::PutMsg;
    use crate::operations::update::UpdateMsg;
    use crate::ring::PeerKeyLocation;
    use crate::transport::TransportPublicKey;
    use freenet_stdlib::prelude::*;

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
        ContractKey::from_id_and_code(
            ContractInstanceId::new([1u8; 32]),
            CodeHash::new([2u8; 32]),
        )
    }

    fn make_tx() -> Transaction {
        Transaction::new::<PutMsg>()
    }

    fn make_update_tx() -> Transaction {
        Transaction::new::<UpdateMsg>()
    }

    fn now() -> DateTime<Utc> {
        Utc::now()
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
}

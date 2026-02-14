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

use super::{EventKind, InterestSyncEvent, NetLogMessage, PutEvent, SubscribeEvent, UpdateEvent};
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

    // --- Network topology anomalies ---
    /// Multiple broadcasts from different senders all fail to reach the same
    /// set of target peers, suggesting a network partition.
    SuspectedPartition {
        contract_key: String,
        /// Peers that emitted broadcasts during the window.
        sending_group: Vec<SocketAddr>,
        /// Peers that never received those broadcasts.
        isolated_group: Vec<SocketAddr>,
        /// Time window of the suspected partition.
        first_missed: DateTime<Utc>,
        last_missed: DateTime<Utc>,
        /// Number of broadcasts missed during this window.
        missed_count: usize,
    },
    /// A peer received the initial contract state but has not received any
    /// subsequent updates while other peers have progressed.
    StalePeer {
        contract_key: String,
        /// The peer that appears stale.
        peer: SocketAddr,
        /// The hash the peer is stuck on.
        stuck_on_hash: String,
        /// When the peer last had any state event.
        last_event_time: DateTime<Utc>,
        /// How many subsequent update transitions this peer missed.
        missed_updates: usize,
    },
    /// Broadcasts from peer A reach peer B, but broadcasts from peer B
    /// never reach peer A (asymmetric / unidirectional link).
    OneWayPropagationFailure {
        contract_key: String,
        /// Peer whose broadcasts are received.
        sender: SocketAddr,
        /// Peer that never receives from the other direction.
        receiver: SocketAddr,
        /// Broadcasts from sender that receiver got.
        forward_deliveries: usize,
        /// Broadcasts from receiver that sender got (should be 0).
        reverse_deliveries: usize,
    },

    // --- State-machine / CRDT anomalies ---
    /// A peer's state hash oscillates between the same values, indicating
    /// conflicting updates applied alternately without converging.
    StateOscillation {
        contract_key: String,
        peer: SocketAddr,
        /// The hashes that are oscillating.
        oscillating_hashes: Vec<String>,
        /// Number of times the state reverted to a previously-seen hash.
        flip_count: usize,
    },
    /// Two updates applied in different order on different peers produced
    /// different state hashes, indicating a non-commutative merge (CRDT bug).
    UpdateOrderingAnomaly {
        contract_key: String,
        tx_a: Transaction,
        tx_b: Transaction,
        /// Peer that applied A then B.
        peer_ab: SocketAddr,
        hash_ab: String,
        /// Peer that applied B then A.
        peer_ba: SocketAddr,
        hash_ba: String,
    },

    // --- Operational anomalies ---
    /// A transaction has a Request event but never completed (no Success,
    /// Failure, or Timeout). Indicates a stuck operation / leaked resource.
    ZombieTransaction {
        operation_type: String,
        transaction: Transaction,
        peer: SocketAddr,
        started_at: DateTime<Utc>,
    },
    /// A single transaction triggered an unexpectedly large number of
    /// broadcast emissions, suggesting a re-broadcast loop.
    BroadcastStorm {
        contract_key: String,
        transaction: Transaction,
        /// Number of BroadcastEmitted events for this transaction.
        broadcast_count: usize,
        /// Expected maximum (unique peers holding the contract).
        expected_max: usize,
        source_op: BroadcastSource,
    },
    /// Multiple peers requested full state resync for the same contract
    /// within a short window, indicating systematic delta application failure.
    DeltaSyncFailureCascade {
        contract_key: String,
        resync_count: usize,
        requesting_peers: Vec<SocketAddr>,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
        total_resync_bytes: usize,
    },
    /// A peer subscribed to a contract but is never listed in any
    /// broadcast_to targets for that contract.
    SubscriptionAsymmetry {
        contract_key: String,
        peer: SocketAddr,
        subscribe_tx: Transaction,
        broadcasts_without_peer: usize,
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
            StateAnomaly::SuspectedPartition {
                contract_key,
                sending_group,
                isolated_group,
                missed_count,
                first_missed,
                last_missed,
            } => {
                write!(
                    f,
                    "SUSPECTED_PARTITION: contract={}, {} broadcasts from {:?} never reached {:?} \
                     (window: {} to {})",
                    contract_key,
                    missed_count,
                    sending_group,
                    isolated_group,
                    first_missed.format("%H:%M:%S%.3f"),
                    last_missed.format("%H:%M:%S%.3f"),
                )
            }
            StateAnomaly::StalePeer {
                contract_key,
                peer,
                stuck_on_hash,
                missed_updates,
                ..
            } => {
                write!(
                    f,
                    "STALE_PEER: contract={}, peer={}, stuck_on={}, missed {} updates",
                    contract_key, peer, stuck_on_hash, missed_updates
                )
            }
            StateAnomaly::OneWayPropagationFailure {
                contract_key,
                sender,
                receiver,
                forward_deliveries,
                reverse_deliveries,
            } => {
                write!(
                    f,
                    "ONE_WAY_PROPAGATION: contract={}, {}→{} has {} deliveries but reverse has {}",
                    contract_key, sender, receiver, forward_deliveries, reverse_deliveries
                )
            }
            StateAnomaly::StateOscillation {
                contract_key,
                peer,
                oscillating_hashes,
                flip_count,
            } => {
                write!(
                    f,
                    "STATE_OSCILLATION: contract={}, peer={}, flipped {} times between {:?}",
                    contract_key, peer, flip_count, oscillating_hashes
                )
            }
            StateAnomaly::UpdateOrderingAnomaly {
                contract_key,
                tx_a,
                tx_b,
                peer_ab,
                hash_ab,
                peer_ba,
                hash_ba,
            } => {
                write!(
                    f,
                    "UPDATE_ORDERING: contract={}, peer {} applied ({},{})→{}, \
                     peer {} applied ({},{})→{} (non-commutative merge)",
                    contract_key, peer_ab, tx_a, tx_b, hash_ab, peer_ba, tx_b, tx_a, hash_ba
                )
            }
            StateAnomaly::ZombieTransaction {
                operation_type,
                transaction,
                peer,
                started_at,
            } => {
                write!(
                    f,
                    "ZOMBIE_TRANSACTION: op={}, tx={}, peer={}, started={}",
                    operation_type,
                    transaction,
                    peer,
                    started_at.format("%H:%M:%S%.3f")
                )
            }
            StateAnomaly::BroadcastStorm {
                contract_key,
                transaction,
                broadcast_count,
                expected_max,
                source_op,
            } => {
                write!(
                    f,
                    "BROADCAST_STORM: contract={}, tx={}, source={:?}, \
                     {} broadcasts (expected max {})",
                    contract_key, transaction, source_op, broadcast_count, expected_max
                )
            }
            StateAnomaly::DeltaSyncFailureCascade {
                contract_key,
                resync_count,
                requesting_peers,
                total_resync_bytes,
                ..
            } => {
                write!(
                    f,
                    "DELTA_SYNC_CASCADE: contract={}, {} resyncs from {:?}, {} bytes transferred",
                    contract_key, resync_count, requesting_peers, total_resync_bytes
                )
            }
            StateAnomaly::SubscriptionAsymmetry {
                contract_key,
                peer,
                broadcasts_without_peer,
                ..
            } => {
                write!(
                    f,
                    "SUBSCRIPTION_ASYMMETRY: contract={}, peer={} subscribed but excluded from {} broadcasts",
                    contract_key, peer, broadcasts_without_peer
                )
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

    pub fn suspected_partitions(&self) -> Vec<&StateAnomaly> {
        self.anomalies
            .iter()
            .filter(|a| matches!(a, StateAnomaly::SuspectedPartition { .. }))
            .collect()
    }

    pub fn stale_peers(&self) -> Vec<&StateAnomaly> {
        self.anomalies
            .iter()
            .filter(|a| matches!(a, StateAnomaly::StalePeer { .. }))
            .collect()
    }

    pub fn state_oscillations(&self) -> Vec<&StateAnomaly> {
        self.anomalies
            .iter()
            .filter(|a| matches!(a, StateAnomaly::StateOscillation { .. }))
            .collect()
    }

    pub fn zombie_transactions(&self) -> Vec<&StateAnomaly> {
        self.anomalies
            .iter()
            .filter(|a| matches!(a, StateAnomaly::ZombieTransaction { .. }))
            .collect()
    }

    pub fn broadcast_storms(&self) -> Vec<&StateAnomaly> {
        self.anomalies
            .iter()
            .filter(|a| matches!(a, StateAnomaly::BroadcastStorm { .. }))
            .collect()
    }

    pub fn delta_sync_cascades(&self) -> Vec<&StateAnomaly> {
        self.anomalies
            .iter()
            .filter(|a| matches!(a, StateAnomaly::DeltaSyncFailureCascade { .. }))
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
                    PutEvent::PutSuccess {
                        state_hash: Some(hash),
                        ..
                    } => {
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
                        state_hash_after: Some(hash_after),
                        ..
                    } => {
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
            // Original detectors
            self.detect_missing_broadcasts(history, &mut anomalies);
            self.detect_unapplied_broadcasts(history, &mut anomalies);
            self.detect_unexpected_state_changes(history, &mut anomalies);
            self.detect_final_divergence(history, &mut anomalies);
            // Network topology
            self.detect_suspected_partition(history, &mut anomalies);
            self.detect_stale_peers(history, &mut anomalies);
            self.detect_one_way_propagation(history, &mut anomalies);
            // State-machine / CRDT
            self.detect_state_oscillation(history, &mut anomalies);
            self.detect_update_ordering(history, &mut anomalies);
            // Operational (per-contract)
            self.detect_broadcast_storms(history, &mut anomalies);
            self.detect_subscription_asymmetry(history, &mut anomalies);
        }

        // Cross-contract / global detectors
        self.detect_zombie_transactions(&mut anomalies);
        self.detect_delta_sync_cascades(&mut anomalies);

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

    // ---- New detectors ----

    /// Correlate missing broadcasts: if >=2 broadcasts from different senders
    /// all miss the SAME set of targets, that's a partition signature.
    fn detect_suspected_partition(
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
                let first = sender_times.iter().map(|(_, t)| *t).min().unwrap();
                let last = sender_times.iter().map(|(_, t)| *t).max().unwrap();
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
    fn detect_stale_peers(
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
    fn detect_one_way_propagation(
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
    fn detect_state_oscillation(
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
    fn detect_update_ordering(
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
    fn detect_broadcast_storms(
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
    fn detect_subscription_asymmetry(
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
    fn detect_zombie_transactions(&self, anomalies: &mut Vec<StateAnomaly>) {
        use super::GetEvent;

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
    fn detect_delta_sync_cascades(&self, anomalies: &mut Vec<StateAnomaly>) {
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
        ContractKey::from_id_and_code(ContractInstanceId::new([1u8; 32]), CodeHash::new([2u8; 32]))
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
}

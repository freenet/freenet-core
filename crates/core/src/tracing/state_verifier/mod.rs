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

mod detectors;
mod report;
pub mod types;

#[cfg(test)]
mod tests;

pub use report::VerificationReport;
pub use types::{
    BroadcastSource, ContractStateHistory, DivergencePoint, StateAnomaly, StateTransition,
    TrackedBroadcast, TransitionKind,
};

use std::collections::{BTreeMap, HashSet};
use std::net::SocketAddr;

use super::{EventKind, NetLogMessage, PutEvent, UpdateEvent};
use types::BroadcastSource as BS;

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
                            source_op: BS::Put,
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
                            source_op: BS::Update,
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
            // Propagation
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
}

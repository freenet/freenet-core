//! Data types for state verification anomalies and contract histories.

use std::{
    collections::{BTreeMap, HashSet},
    fmt,
    net::SocketAddr,
};

use chrono::{DateTime, Utc};

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

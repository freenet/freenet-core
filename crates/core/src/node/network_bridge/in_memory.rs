//! A in-memory transport implementation for simulation testing.
//!
//! This module provides the messaging infrastructure for in-memory simulation testing.
//! The actual event loop is now handled by the production `P2pConnManager` with
//! `InMemorySocket` - this module provides the peer registry and fault injection
//! infrastructure that the transport layer uses.
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, LazyLock, RwLock},
    time::Duration,
};

/// Divisor for determining shuffle probability in noise mode.
/// When `msg_hash % NOISE_SHUFFLE_DIVISOR == 0`, the message queue is shuffled.
/// A value of 5 gives approximately 20% shuffle probability.
const NOISE_SHUFFLE_DIVISOR: u64 = 5;

use crossbeam::channel::{self, Sender};
use rand::{prelude::StdRng, seq::SliceRandom, SeedableRng};
use tokio::sync::Mutex;

use super::ConnectionError;
use crate::{
    config::GlobalExecutor,
    node::PeerId,
    simulation::{FaultConfig, SimulationRng, TimeSource, VirtualTime},
    transport::TransportPublicKey,
};

/// A message pending delivery at a virtual time deadline.
#[derive(Debug)]
pub struct PendingDelivery {
    /// Virtual time deadline (nanoseconds) when message should be delivered
    pub deadline: u64,
    /// The message to deliver
    pub msg: MessageOnTransit,
    /// Target address for delivery
    pub target_addr: SocketAddr,
}

/// Statistics for fault injection effects.
///
/// Tracks how many messages were dropped, delayed, partitioned, etc.
/// Use these stats to verify fault injection is working as expected.
#[derive(Debug, Clone, Default)]
pub struct NetworkStats {
    /// Total messages sent through the transport
    pub messages_sent: u64,
    /// Messages delivered immediately
    pub messages_delivered: u64,
    /// Messages dropped due to message loss rate
    pub messages_dropped_loss: u64,
    /// Messages dropped due to network partitions
    pub messages_dropped_partition: u64,
    /// Messages dropped due to crashed nodes
    pub messages_dropped_crash: u64,
    /// Messages queued for delayed delivery (VirtualTime mode)
    pub messages_queued: u64,
    /// Messages delivered after delay
    pub messages_delayed_delivered: u64,
    /// Total latency added (in nanoseconds) across all delayed messages
    pub total_latency_nanos: u64,
}

impl NetworkStats {
    /// Returns the total number of dropped messages.
    pub fn total_dropped(&self) -> u64 {
        self.messages_dropped_loss + self.messages_dropped_partition + self.messages_dropped_crash
    }

    /// Returns the message loss ratio (0.0 to 1.0).
    pub fn loss_ratio(&self) -> f64 {
        if self.messages_sent == 0 {
            0.0
        } else {
            self.total_dropped() as f64 / self.messages_sent as f64
        }
    }

    /// Returns the average latency added per delayed message.
    pub fn average_latency(&self) -> Duration {
        if self.messages_delayed_delivered == 0 {
            Duration::ZERO
        } else {
            Duration::from_nanos(self.total_latency_nanos / self.messages_delayed_delivered)
        }
    }
}

/// State for deterministic fault injection.
///
/// Contains both the fault configuration and a seeded RNG for reproducible
/// fault injection decisions. Optionally supports VirtualTime for deterministic
/// latency injection.
pub struct FaultInjectorState {
    /// The fault configuration (message loss, partitions, crashes, latency)
    pub config: FaultConfig,
    /// Seeded RNG for deterministic fault decisions
    pub rng: SimulationRng,
    /// Optional VirtualTime for deterministic latency (Phase 2)
    pub virtual_time: Option<VirtualTime>,
    /// Messages pending delivery when VirtualTime is enabled
    pub pending_deliveries: Vec<PendingDelivery>,
    /// Statistics for tracking fault injection effects
    pub stats: NetworkStats,
}

impl FaultInjectorState {
    /// Creates a new fault injector state with the given config and seed.
    pub fn new(config: FaultConfig, seed: u64) -> Self {
        Self {
            config,
            rng: SimulationRng::new(seed),
            virtual_time: None,
            pending_deliveries: Vec::new(),
            stats: NetworkStats::default(),
        }
    }

    /// Enables VirtualTime mode for deterministic latency injection.
    pub fn with_virtual_time(mut self, vt: VirtualTime) -> Self {
        self.virtual_time = Some(vt);
        self
    }

    /// Returns the current VirtualTime if enabled.
    #[allow(dead_code)] // Part of public API for future use
    pub fn virtual_time(&self) -> Option<&VirtualTime> {
        self.virtual_time.as_ref()
    }

    /// Advances virtual time and delivers pending messages whose deadline has passed.
    ///
    /// Returns the number of messages delivered.
    pub fn advance_time(&mut self) -> usize {
        let Some(ref vt) = self.virtual_time else {
            return 0;
        };

        let now = vt.now_nanos();
        let mut delivered = 0;

        // Drain messages with deadline <= now
        let ready_indices: Vec<usize> = self
            .pending_deliveries
            .iter()
            .enumerate()
            .filter(|(_, p)| p.deadline <= now)
            .map(|(i, _)| i)
            .collect();

        // Remove in reverse order to maintain indices
        for idx in ready_indices.into_iter().rev() {
            let pending = self.pending_deliveries.remove(idx);
            let registry = PEER_REGISTRY.read().unwrap();
            if let Err(e) = registry.send(pending.target_addr, pending.msg) {
                tracing::trace!(
                    "VirtualTime delivery failed: {} (peer may have disconnected)",
                    e
                );
            } else {
                delivered += 1;
                self.stats.messages_delayed_delivered += 1;
            }
        }

        delivered
    }

    /// Returns a reference to the network statistics.
    pub fn stats(&self) -> &NetworkStats {
        &self.stats
    }

    /// Resets the network statistics.
    pub fn reset_stats(&mut self) {
        self.stats = NetworkStats::default();
    }
}

/// Network-scoped fault injectors for controlling message delivery in tests.
///
/// Each network (identified by name) can have its own fault injector configuration.
/// This prevents test interference when multiple tests run concurrently.
static FAULT_INJECTORS: LazyLock<
    RwLock<HashMap<String, Arc<std::sync::Mutex<FaultInjectorState>>>>,
> = LazyLock::new(|| RwLock::new(HashMap::new()));

/// Sets the fault injector for a specific network.
///
/// Each network (identified by name) can have its own fault injector, preventing
/// test interference when multiple tests run concurrently.
///
/// # Arguments
/// * `network_name` - The unique name of the network (from SimNetwork)
/// * `state` - The fault injector state, or None to clear
///
/// # Example
/// ```ignore
/// use freenet::simulation::FaultConfig;
/// use freenet::node::network_bridge::FaultInjectorState;
///
/// let config = FaultConfig::builder()
///     .message_loss_rate(0.1)
///     .latency_range(Duration::from_millis(10)..Duration::from_millis(50))
///     .build();
/// let state = FaultInjectorState::new(config, 12345);
/// set_fault_injector("my-test-network", Some(Arc::new(std::sync::Mutex::new(state))));
/// ```
pub fn set_fault_injector(
    network_name: &str,
    state: Option<Arc<std::sync::Mutex<FaultInjectorState>>>,
) {
    let mut injectors = FAULT_INJECTORS.write().unwrap();
    match state {
        Some(s) => {
            injectors.insert(network_name.to_string(), s);
        }
        None => {
            injectors.remove(network_name);
        }
    }
}

/// Returns the fault injector for a specific network, if set.
pub fn get_fault_injector(network_name: &str) -> Option<Arc<std::sync::Mutex<FaultInjectorState>>> {
    FAULT_INJECTORS.read().unwrap().get(network_name).cloned()
}

/// Clears all fault injectors. Useful for test cleanup.
#[allow(dead_code)]
pub fn clear_all_fault_injectors() {
    FAULT_INJECTORS.write().unwrap().clear();
}

/// Reason for dropping a message.
#[derive(Debug, Clone, Copy)]
pub enum DropReason {
    /// Message dropped due to random loss (message_loss_rate)
    RandomLoss,
    /// Message dropped due to network partition
    Partition,
    /// Message dropped because sender or receiver crashed
    NodeCrashed,
}

/// Result of checking whether a message should be delivered.
#[derive(Debug)]
pub enum DeliveryDecision {
    /// Deliver immediately
    Deliver,
    /// Deliver after the specified delay (real-time mode)
    DelayedDelivery(Duration),
    /// Queue for delivery at virtual time deadline (VirtualTime mode)
    QueuedDelivery {
        /// Virtual time deadline in nanoseconds
        deadline: u64,
        /// Latency added (stored for debugging/logging purposes)
        #[allow(dead_code)]
        latency: Duration,
    },
    /// Drop the message with reason
    Drop(DropReason),
}

/// Checks if a message should be delivered and with what delay.
///
/// Returns a DeliveryDecision indicating whether to deliver, delay, or drop.
/// Also updates network stats.
///
/// # Arguments
/// * `network_name` - The network this message belongs to (for scoped fault injection)
/// * `from` - Source address
/// * `to` - Destination address
fn check_delivery(network_name: &str, from: SocketAddr, to: SocketAddr) -> DeliveryDecision {
    let Some(injector) = get_fault_injector(network_name) else {
        return DeliveryDecision::Deliver; // No fault injection configured for this network
    };

    let mut state = injector.lock().unwrap();
    state.stats.messages_sent += 1;

    // Check if sender or receiver is crashed
    if state.config.is_crashed(&from) || state.config.is_crashed(&to) {
        tracing::trace!(?from, ?to, "Fault injector: message dropped (node crashed)");
        state.stats.messages_dropped_crash += 1;
        return DeliveryDecision::Drop(DropReason::NodeCrashed);
    }

    // Get current virtual time if available
    let current_time = state
        .virtual_time
        .as_ref()
        .map(|vt| vt.now_nanos())
        .unwrap_or(0);

    // Check for partition
    if state.config.is_partitioned(&from, &to, current_time) {
        tracing::trace!(
            ?from,
            ?to,
            "Fault injector: message dropped (network partition)"
        );
        state.stats.messages_dropped_partition += 1;
        return DeliveryDecision::Drop(DropReason::Partition);
    }

    // Check message loss rate using seeded RNG for determinism
    if state.config.should_drop_message(&state.rng) {
        tracing::trace!(?from, ?to, "Fault injector: message dropped (random loss)");
        state.stats.messages_dropped_loss += 1;
        return DeliveryDecision::Drop(DropReason::RandomLoss);
    }

    // Check for latency injection
    if let Some(latency) = state.config.generate_latency(&state.rng) {
        // If VirtualTime is enabled, use queued delivery; otherwise, use real-time delay
        if let Some(ref vt) = state.virtual_time {
            let deadline = vt.now_nanos() + latency.as_nanos() as u64;
            tracing::trace!(
                ?from,
                ?to,
                ?latency,
                deadline,
                "Fault injector: message queued (VirtualTime)"
            );
            state.stats.messages_queued += 1;
            state.stats.total_latency_nanos += latency.as_nanos() as u64;
            return DeliveryDecision::QueuedDelivery { deadline, latency };
        } else {
            tracing::trace!(?from, ?to, ?latency, "Fault injector: message delayed");
            state.stats.total_latency_nanos += latency.as_nanos() as u64;
            return DeliveryDecision::DelayedDelivery(latency);
        }
    }

    state.stats.messages_delivered += 1;
    DeliveryDecision::Deliver
}

/// Registry that maps socket addresses to their dedicated message channels.
/// This enables direct peer-to-peer message routing without busy-polling.
#[derive(Default)]
struct PeerRegistry {
    /// Maps peer socket address to (public_key, message_sender)
    peers: HashMap<SocketAddr, (TransportPublicKey, Sender<MessageOnTransit>)>,
}

impl PeerRegistry {
    fn register(
        &mut self,
        addr: SocketAddr,
        pub_key: TransportPublicKey,
        sender: Sender<MessageOnTransit>,
    ) {
        self.peers.insert(addr, (pub_key, sender));
    }

    fn unregister(&mut self, addr: &SocketAddr) {
        self.peers.remove(addr);
    }

    fn send(&self, target_addr: SocketAddr, msg: MessageOnTransit) -> Result<(), ConnectionError> {
        if let Some((_, sender)) = self.peers.get(&target_addr) {
            sender
                .send(msg)
                .map_err(|_| ConnectionError::SendNotCompleted(target_addr))
        } else {
            tracing::warn!("No peer registered at {}", target_addr);
            Err(ConnectionError::SendNotCompleted(target_addr))
        }
    }

    fn get_public_key(&self, addr: &SocketAddr) -> Option<TransportPublicKey> {
        self.peers.get(addr).map(|(pk, _)| pk.clone())
    }

    fn is_registered(&self, addr: &SocketAddr) -> bool {
        self.peers.contains_key(addr)
    }
}

/// Checks if a peer is registered in the global peer registry.
/// Used by SimNetwork to wait for gateways to be ready before starting regular nodes.
pub fn is_peer_registered(addr: &SocketAddr) -> bool {
    PEER_REGISTRY.read().unwrap().is_registered(addr)
}

/// Unregisters a peer from the global peer registry.
/// Used by SimNetwork when a node is crashed or being restarted.
pub fn unregister_peer(addr: &SocketAddr) {
    PEER_REGISTRY.write().unwrap().unregister(addr);
}

/// Global peer registry for all in-memory peers
static PEER_REGISTRY: LazyLock<RwLock<PeerRegistry>> =
    LazyLock::new(|| RwLock::new(PeerRegistry::default()));

/// A message in transit between peers in the in-memory transport.
#[derive(Clone, Debug)]
pub struct MessageOnTransit {
    /// The peer that sent the message
    pub origin: PeerId,
    /// The intended target peer
    #[allow(dead_code)] // Kept for debugging and potential future use
    pub target: PeerId,
    /// The serialized message data
    pub data: Vec<u8>,
}

#[derive(Clone, Debug)]
struct InMemoryTransport {
    interface_peer: PeerId,
    /// received messages per each peer awaiting processing
    msg_stack_queue: Arc<Mutex<Vec<MessageOnTransit>>>,
    /// Network name for scoped fault injection
    network_name: Arc<str>,
}

impl InMemoryTransport {
    fn new(interface_peer: PeerId, add_noise: bool, rng_seed: u64, network_name: &str) -> Self {
        let msg_stack_queue = Arc::new(Mutex::new(Vec::new()));

        // Create a dedicated channel for this peer
        let (tx, rx) = channel::unbounded();

        // Register this peer in the global registry
        {
            let mut registry = PEER_REGISTRY.write().unwrap();
            registry.register(interface_peer.addr, interface_peer.pub_key.clone(), tx);
        }

        // Spawn a task to receive messages from our dedicated channel
        let msg_stack_queue_cp = msg_stack_queue.clone();
        let ip = interface_peer.clone();
        GlobalExecutor::spawn(async move {
            loop {
                match rx.try_recv() {
                    Ok(msg) => {
                        tracing::trace!(
                            "Inbound message received for peer {} from {}",
                            ip,
                            msg.origin
                        );
                        let mut queue = msg_stack_queue_cp.lock().await;

                        if add_noise {
                            // Derive shuffle decision from message content for determinism
                            // This avoids depending on async arrival order
                            let msg_hash = Self::hash_message(&msg.data);
                            queue.push(msg);

                            // Shuffle based on message content (probability ~1/NOISE_SHUFFLE_DIVISOR)
                            if msg_hash % NOISE_SHUFFLE_DIVISOR == 0 {
                                // Derive shuffle seed from base seed + message hash
                                let shuffle_seed = rng_seed.wrapping_add(msg_hash);
                                let mut shuffle_rng = StdRng::seed_from_u64(shuffle_seed);
                                queue.shuffle(&mut shuffle_rng);
                            }
                        } else {
                            queue.push(msg);
                        }
                    }
                    Err(channel::TryRecvError::Disconnected) => {
                        tracing::debug!("Channel closed for peer {}", ip);
                        break;
                    }
                    Err(channel::TryRecvError::Empty) => {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                }
            }
            tracing::debug!("Stopped receiving messages in {ip}");
        });

        Self {
            interface_peer,
            msg_stack_queue,
            network_name: network_name.into(),
        }
    }

    /// Compute a simple hash of message data for deterministic shuffle decisions.
    /// Uses FNV-1a for speed (we don't need cryptographic strength here).
    fn hash_message(data: &[u8]) -> u64 {
        const FNV_OFFSET: u64 = 0xcbf29ce484222325;
        const FNV_PRIME: u64 = 0x100000001b3;

        let mut hash = FNV_OFFSET;
        for byte in data {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash
    }

    fn send(&self, target: PeerId, message: Vec<u8>) -> Result<(), ConnectionError> {
        // Check fault injector before sending (scoped to this network)
        match check_delivery(&self.network_name, self.interface_peer.addr, target.addr) {
            DeliveryDecision::Drop(_reason) => {
                // Message dropped by fault injection - return success to caller
                // (from sender's perspective, the message was sent)
                return Ok(());
            }
            DeliveryDecision::DelayedDelivery(delay) => {
                // Delay the message delivery using real-time (tokio::time::sleep)
                let msg = MessageOnTransit {
                    origin: self.interface_peer.clone(),
                    target: target.clone(),
                    data: message,
                };
                let target_addr = target.addr;

                // Spawn a task to deliver after delay
                GlobalExecutor::spawn(async move {
                    tokio::time::sleep(delay).await;
                    let registry = PEER_REGISTRY.read().unwrap();
                    if let Err(e) = registry.send(target_addr, msg) {
                        tracing::trace!(
                            "Delayed message delivery failed: {} (peer may have disconnected)",
                            e
                        );
                    }
                });
                return Ok(());
            }
            DeliveryDecision::QueuedDelivery { deadline, .. } => {
                // Queue the message for delivery at virtual time deadline
                let msg = MessageOnTransit {
                    origin: self.interface_peer.clone(),
                    target: target.clone(),
                    data: message,
                };
                let target_addr = target.addr;

                // Add to pending deliveries queue
                if let Some(injector) = get_fault_injector(&self.network_name) {
                    let mut state = injector.lock().unwrap();
                    state.pending_deliveries.push(PendingDelivery {
                        deadline,
                        msg,
                        target_addr,
                    });
                }
                return Ok(());
            }
            DeliveryDecision::Deliver => {
                // Immediate delivery
            }
        }

        let msg = MessageOnTransit {
            origin: self.interface_peer.clone(),
            target: target.clone(),
            data: message,
        };

        // Send directly to the target peer's channel
        let registry = PEER_REGISTRY.read().unwrap();
        registry.send(target.addr, msg)
    }
}

impl Drop for InMemoryTransport {
    fn drop(&mut self) {
        // Unregister from the global registry when dropped
        let mut registry = PEER_REGISTRY.write().unwrap();
        registry.unregister(&self.interface_peer.addr);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simulation::FaultConfigBuilder;

    #[test]
    fn test_network_stats_default() {
        let stats = NetworkStats::default();
        assert_eq!(stats.messages_sent, 0);
        assert_eq!(stats.messages_delivered, 0);
        assert_eq!(stats.total_dropped(), 0);
        assert_eq!(stats.loss_ratio(), 0.0);
        assert_eq!(stats.average_latency(), Duration::ZERO);
    }

    #[test]
    fn test_network_stats_calculations() {
        let stats = NetworkStats {
            messages_sent: 100,
            messages_delivered: 80,
            messages_dropped_loss: 10,
            messages_dropped_partition: 5,
            messages_dropped_crash: 5,
            messages_queued: 0,
            messages_delayed_delivered: 20,
            total_latency_nanos: 200_000_000, // 200ms total
        };

        assert_eq!(stats.total_dropped(), 20);
        assert!((stats.loss_ratio() - 0.2).abs() < 0.001);
        assert_eq!(stats.average_latency(), Duration::from_millis(10));
    }

    #[test]
    fn test_fault_injector_state_new() {
        let config = FaultConfigBuilder::default().build();
        let state = FaultInjectorState::new(config, 12345);

        assert!(state.virtual_time.is_none());
        assert!(state.pending_deliveries.is_empty());
        assert_eq!(state.stats.messages_sent, 0);
    }

    #[test]
    fn test_fault_injector_state_with_virtual_time() {
        let config = FaultConfigBuilder::default().build();
        let vt = VirtualTime::new();
        let state = FaultInjectorState::new(config, 12345).with_virtual_time(vt);

        assert!(state.virtual_time.is_some());
    }

    #[test]
    fn test_fault_injector_reset_stats() {
        let config = FaultConfigBuilder::default().build();
        let mut state = FaultInjectorState::new(config, 12345);

        state.stats.messages_sent = 100;
        state.stats.messages_dropped_loss = 10;

        state.reset_stats();

        assert_eq!(state.stats.messages_sent, 0);
        assert_eq!(state.stats.messages_dropped_loss, 0);
    }

    #[test]
    fn test_drop_reason_variants() {
        // Test that all drop reason variants can be created
        let _ = DropReason::RandomLoss;
        let _ = DropReason::Partition;
        let _ = DropReason::NodeCrashed;
    }

    #[test]
    fn test_delivery_decision_variants() {
        // Test that all delivery decision variants can be created
        let _ = DeliveryDecision::Deliver;
        let _ = DeliveryDecision::DelayedDelivery(Duration::from_millis(100));
        let _ = DeliveryDecision::QueuedDelivery {
            deadline: 100_000_000,
            latency: Duration::from_millis(100),
        };
        let _ = DeliveryDecision::Drop(DropReason::RandomLoss);
    }
}

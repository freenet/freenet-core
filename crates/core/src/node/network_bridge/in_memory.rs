//! Fault injection infrastructure for in-memory simulation testing.
//!
//! This module provides the fault injection state and configuration that controls
//! message delivery in `InMemorySocket`. It enables deterministic testing of
//! network failures including message loss, partitions, node crashes, and latency.
//!
//! The actual socket implementation lives in `transport/in_memory_socket.rs`.

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, LazyLock, RwLock},
    time::Duration,
};

use crate::simulation::{FaultConfig, SimulationRng, TimeSource, VirtualTime};

/// A message pending delivery at a virtual time deadline.
#[derive(Debug)]
pub struct PendingMessage {
    /// Virtual time deadline (nanoseconds) when message should be delivered
    pub deadline: u64,
    /// The message data to deliver
    pub data: Vec<u8>,
    /// Source address
    pub from: SocketAddr,
    /// Target address
    pub target: SocketAddr,
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
    /// Optional VirtualTime for deterministic latency
    pub virtual_time: Option<VirtualTime>,
    /// Messages pending delivery when VirtualTime is enabled
    pub pending_messages: Vec<PendingMessage>,
    /// Statistics for tracking fault injection effects
    pub stats: NetworkStats,
    /// Network name for delivering pending messages
    pub network_name: Option<String>,
}

impl FaultInjectorState {
    /// Creates a new fault injector state with the given config and seed.
    pub fn new(config: FaultConfig, seed: u64) -> Self {
        Self {
            config,
            rng: SimulationRng::new(seed),
            virtual_time: None,
            pending_messages: Vec::new(),
            stats: NetworkStats::default(),
            network_name: None,
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
        use crate::transport::in_memory_socket::deliver_packet_to_network;

        let Some(ref vt) = self.virtual_time else {
            return 0;
        };

        let Some(ref network_name) = self.network_name else {
            tracing::warn!("advance_time called but network_name not set");
            return 0;
        };

        let now = vt.now_nanos();
        let mut delivered = 0;

        // Drain messages with deadline <= now
        let ready_indices: Vec<usize> = self
            .pending_messages
            .iter()
            .enumerate()
            .filter(|(_, p)| p.deadline <= now)
            .map(|(i, _)| i)
            .collect();

        // Remove in reverse order to maintain indices
        for idx in ready_indices.into_iter().rev() {
            let pending = self.pending_messages.remove(idx);
            // Use socket-level delivery
            if deliver_packet_to_network(network_name, pending.target, pending.data, pending.from) {
                delivered += 1;
                self.stats.messages_delayed_delivered += 1;
            } else {
                tracing::trace!(
                    target = %pending.target,
                    "VirtualTime delivery failed (socket may have been dropped)"
                );
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
            // Set the network name on the state
            {
                let mut state = s.lock().unwrap();
                state.network_name = Some(network_name.to_string());
            }
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
        assert!(state.pending_messages.is_empty());
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
}

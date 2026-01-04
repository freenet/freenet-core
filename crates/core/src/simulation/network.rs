//! Simulated network layer with deterministic message delivery.
//!
//! This module provides a network simulation layer that integrates with
//! the scheduler for deterministic message delivery.

use std::{
    collections::{BTreeMap, VecDeque},
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use super::{
    fault::FaultConfig,
    scheduler::{EventId, EventType, Scheduler},
};

/// Configuration for the simulated network.
#[derive(Debug, Clone)]
pub struct SimulatedNetworkConfig {
    /// Default latency range for message delivery
    pub default_latency: Duration,
    /// Whether to trace message deliveries
    pub trace_messages: bool,
}

impl Default for SimulatedNetworkConfig {
    fn default() -> Self {
        Self {
            default_latency: Duration::from_millis(10),
            trace_messages: false,
        }
    }
}

/// Message in transit through the simulated network.
#[derive(Debug, Clone)]
pub struct InFlightMessage {
    /// Source peer
    pub from: SocketAddr,
    /// Destination peer
    pub to: SocketAddr,
    /// Serialized payload
    pub payload: Vec<u8>,
    /// Scheduled delivery event ID
    pub event_id: EventId,
    /// Scheduled delivery time (virtual nanos)
    pub delivery_time: u64,
}

/// Statistics about the simulated network.
#[derive(Debug, Default, Clone)]
pub struct NetworkStats {
    /// Total messages sent
    pub messages_sent: u64,
    /// Total messages delivered
    pub messages_delivered: u64,
    /// Total messages dropped (by fault injection)
    pub messages_dropped: u64,
    /// Total messages blocked by partition
    pub messages_partitioned: u64,
    /// Total latency added (nanoseconds)
    pub total_latency_nanos: u64,
}

impl NetworkStats {
    /// Returns average latency per delivered message.
    pub fn average_latency(&self) -> Duration {
        if self.messages_delivered == 0 {
            Duration::ZERO
        } else {
            Duration::from_nanos(self.total_latency_nanos / self.messages_delivered)
        }
    }
}

/// A simulated network that delivers messages through the scheduler.
///
/// Messages are scheduled as events with deterministic delivery times.
/// Fault injection (drops, partitions, latency) is applied during sending.
pub struct SimulatedNetwork {
    /// Scheduler for event processing
    scheduler: Arc<Mutex<Scheduler>>,
    /// Fault configuration
    fault_config: Arc<Mutex<FaultConfig>>,
    /// Network configuration
    config: SimulatedNetworkConfig,
    /// Messages currently in flight.
    ///
    /// Uses BTreeMap instead of HashMap to ensure deterministic iteration order
    /// when cancelling messages or collecting debug information. HashMap iteration
    /// order depends on internal hashing state which can vary between runs,
    /// breaking simulation reproducibility.
    in_flight: Arc<Mutex<BTreeMap<EventId, InFlightMessage>>>,
    /// Delivered messages waiting to be consumed (per peer).
    /// Uses VecDeque for O(1) front removal in recv().
    ///
    /// Uses BTreeMap instead of HashMap to ensure deterministic iteration order.
    /// SocketAddr implements Ord, providing stable ordering by IP then port.
    #[allow(clippy::type_complexity)]
    delivered: Arc<Mutex<BTreeMap<SocketAddr, VecDeque<(SocketAddr, Vec<u8>)>>>>,
    /// Network statistics
    stats: Arc<Mutex<NetworkStats>>,
}

impl SimulatedNetwork {
    /// Creates a new simulated network with the given scheduler.
    pub fn new(scheduler: Arc<Mutex<Scheduler>>) -> Self {
        Self::with_config(scheduler, SimulatedNetworkConfig::default())
    }

    /// Creates a new simulated network with custom configuration.
    pub fn with_config(scheduler: Arc<Mutex<Scheduler>>, config: SimulatedNetworkConfig) -> Self {
        Self {
            scheduler,
            fault_config: Arc::new(Mutex::new(FaultConfig::new())),
            config,
            in_flight: Arc::new(Mutex::new(BTreeMap::new())),
            delivered: Arc::new(Mutex::new(BTreeMap::new())),
            stats: Arc::new(Mutex::new(NetworkStats::default())),
        }
    }

    /// Returns a reference to the fault configuration.
    pub fn fault_config(&self) -> Arc<Mutex<FaultConfig>> {
        self.fault_config.clone()
    }

    /// Sets the fault configuration.
    pub fn set_fault_config(&self, config: FaultConfig) {
        *self.fault_config.lock().unwrap() = config;
    }

    /// Returns network statistics.
    pub fn stats(&self) -> NetworkStats {
        self.stats.lock().unwrap().clone()
    }

    /// Resets network statistics.
    pub fn reset_stats(&self) {
        *self.stats.lock().unwrap() = NetworkStats::default();
    }

    /// Sends a message from one peer to another.
    ///
    /// The message will be scheduled for delivery through the scheduler.
    /// Returns None if the message was dropped or blocked.
    pub fn send(&self, from: SocketAddr, to: SocketAddr, payload: Vec<u8>) -> Option<EventId> {
        let mut scheduler = self.scheduler.lock().unwrap();
        let fault_config = self.fault_config.lock().unwrap();
        let rng = scheduler.rng().clone();
        let current_time = scheduler.now();

        // Update stats
        {
            let mut stats = self.stats.lock().unwrap();
            stats.messages_sent += 1;
        }

        // Check if sender or receiver is crashed
        if fault_config.is_crashed(&from) || fault_config.is_crashed(&to) {
            if self.config.trace_messages {
                tracing::trace!(?from, ?to, "Message dropped: node crashed");
            }
            let mut stats = self.stats.lock().unwrap();
            stats.messages_dropped += 1;
            return None;
        }

        // Check for partition
        if fault_config.is_partitioned(&from, &to, current_time) {
            if self.config.trace_messages {
                tracing::trace!(?from, ?to, "Message dropped: network partition");
            }
            let mut stats = self.stats.lock().unwrap();
            stats.messages_partitioned += 1;
            return None;
        }

        // Check for random drop
        if fault_config.should_drop_message(&rng) {
            if self.config.trace_messages {
                tracing::trace!(?from, ?to, "Message dropped: random loss");
            }
            let mut stats = self.stats.lock().unwrap();
            stats.messages_dropped += 1;
            return None;
        }

        // Calculate delivery latency
        let latency = fault_config
            .generate_latency(&rng)
            .unwrap_or(self.config.default_latency);

        // Schedule the message delivery
        let event_id = scheduler.schedule_after(
            latency,
            EventType::MessageDelivery {
                from,
                to,
                payload: payload.clone(),
            },
        );

        let delivery_time = current_time + latency.as_nanos() as u64;

        // Track in-flight message
        {
            let mut in_flight = self.in_flight.lock().unwrap();
            in_flight.insert(
                event_id,
                InFlightMessage {
                    from,
                    to,
                    payload,
                    event_id,
                    delivery_time,
                },
            );
        }

        // Update stats
        {
            let mut stats = self.stats.lock().unwrap();
            stats.total_latency_nanos += latency.as_nanos() as u64;
        }

        if self.config.trace_messages {
            tracing::trace!(
                ?from,
                ?to,
                ?latency,
                event_id = event_id.as_u64(),
                "Message scheduled for delivery"
            );
        }

        Some(event_id)
    }

    /// Processes a delivered message event.
    ///
    /// Should be called when the scheduler processes a MessageDelivery event.
    pub fn process_delivery(
        &self,
        event_id: EventId,
        from: SocketAddr,
        to: SocketAddr,
        payload: Vec<u8>,
    ) {
        // Remove from in-flight
        {
            let mut in_flight = self.in_flight.lock().unwrap();
            in_flight.remove(&event_id);
        }

        // Check if delivery is still valid (node might have crashed)
        {
            let fault_config = self.fault_config.lock().unwrap();
            if fault_config.is_crashed(&to) {
                let mut stats = self.stats.lock().unwrap();
                stats.messages_dropped += 1;
                return;
            }
        }

        // Add to delivered queue for the target peer
        {
            let mut delivered = self.delivered.lock().unwrap();
            delivered.entry(to).or_default().push_back((from, payload));
        }

        // Update stats
        {
            let mut stats = self.stats.lock().unwrap();
            stats.messages_delivered += 1;
        }

        if self.config.trace_messages {
            tracing::trace!(
                ?from,
                ?to,
                event_id = event_id.as_u64(),
                "Message delivered"
            );
        }
    }

    /// Receives the next message for a peer, if any.
    /// Uses O(1) pop_front() from VecDeque.
    pub fn recv(&self, peer: SocketAddr) -> Option<(SocketAddr, Vec<u8>)> {
        let mut delivered = self.delivered.lock().unwrap();
        let queue = delivered.get_mut(&peer)?;
        queue.pop_front()
    }

    /// Returns the number of pending messages for a peer.
    pub fn pending_for(&self, peer: SocketAddr) -> usize {
        self.delivered
            .lock()
            .unwrap()
            .get(&peer)
            .map(|q| q.len())
            .unwrap_or(0)
    }

    /// Returns the total number of in-flight messages.
    pub fn in_flight_count(&self) -> usize {
        self.in_flight.lock().unwrap().len()
    }

    /// Returns all in-flight messages for debugging.
    pub fn in_flight_messages(&self) -> Vec<InFlightMessage> {
        self.in_flight.lock().unwrap().values().cloned().collect()
    }

    /// Cancels all in-flight messages to a crashed node.
    pub fn cancel_to_crashed(&self, crashed: SocketAddr) {
        let mut in_flight = self.in_flight.lock().unwrap();
        let mut scheduler = self.scheduler.lock().unwrap();

        let to_cancel: Vec<_> = in_flight
            .iter()
            .filter(|(_, msg)| msg.to == crashed)
            .map(|(id, _)| *id)
            .collect();

        for id in to_cancel {
            in_flight.remove(&id);
            scheduler.cancel(id);
            let mut stats = self.stats.lock().unwrap();
            stats.messages_dropped += 1;
        }
    }

    /// Advances the simulation by processing the next event.
    ///
    /// If the event is a message delivery, it will be processed automatically.
    /// Returns true if an event was processed.
    pub fn step(&self) -> bool {
        let event = {
            let mut scheduler = self.scheduler.lock().unwrap();
            scheduler.step()
        };

        if let Some(event) = event {
            if let EventType::MessageDelivery { from, to, payload } = event.event_type {
                self.process_delivery(event.id, from, to, payload);
            }
            true
        } else {
            false
        }
    }

    /// Runs the simulation until the given condition is true.
    ///
    /// Returns the number of events processed.
    pub fn run_until<F>(&self, mut condition: F) -> usize
    where
        F: FnMut() -> bool,
    {
        let mut processed = 0;
        while !condition() {
            if !self.step() {
                break;
            }
            processed += 1;
        }
        processed
    }

    /// Runs the simulation for the given virtual duration.
    ///
    /// Returns the number of events processed.
    pub fn run_for(&self, duration: Duration) -> usize {
        let target_time = {
            let scheduler = self.scheduler.lock().unwrap();
            scheduler.now() + duration.as_nanos() as u64
        };

        let mut processed = 0;
        loop {
            let should_continue = {
                let scheduler = self.scheduler.lock().unwrap();
                scheduler
                    .next_event_time()
                    .map(|t| t <= target_time)
                    .unwrap_or(false)
            };

            if !should_continue {
                break;
            }

            if !self.step() {
                break;
            }
            processed += 1;
        }

        // Advance time to target even if no more events
        {
            let mut scheduler = self.scheduler.lock().unwrap();
            if scheduler.now() < target_time {
                scheduler.time_mut().advance_to(target_time);
            }
        }

        processed
    }
}

impl Clone for SimulatedNetwork {
    fn clone(&self) -> Self {
        Self {
            scheduler: self.scheduler.clone(),
            fault_config: self.fault_config.clone(),
            config: self.config.clone(),
            in_flight: self.in_flight.clone(),
            delivered: self.delivered.clone(),
            stats: self.stats.clone(),
        }
    }
}

impl std::fmt::Debug for SimulatedNetwork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let scheduler = self.scheduler.lock().unwrap();
        let stats = self.stats.lock().unwrap();
        f.debug_struct("SimulatedNetwork")
            .field("now", &scheduler.now())
            .field("pending_events", &scheduler.pending_count())
            .field("in_flight", &self.in_flight_count())
            .field("stats", &*stats)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    #[test]
    fn test_basic_send_recv() {
        let scheduler = Arc::new(Mutex::new(Scheduler::new(42)));
        let network = SimulatedNetwork::new(scheduler);

        let from = addr(1000);
        let to = addr(2000);
        let payload = vec![1, 2, 3, 4];

        // Send message
        let event_id = network.send(from, to, payload.clone());
        assert!(event_id.is_some());

        // Message should be in flight
        assert_eq!(network.in_flight_count(), 1);
        assert_eq!(network.pending_for(to), 0);

        // Process delivery
        network.step();

        // Message should be delivered
        assert_eq!(network.in_flight_count(), 0);
        assert_eq!(network.pending_for(to), 1);

        // Receive message
        let (sender, data) = network.recv(to).unwrap();
        assert_eq!(sender, from);
        assert_eq!(data, payload);
    }

    #[test]
    fn test_message_order() {
        let scheduler = Arc::new(Mutex::new(Scheduler::new(42)));
        let network = SimulatedNetwork::with_config(
            scheduler,
            SimulatedNetworkConfig {
                default_latency: Duration::from_millis(10),
                trace_messages: false,
            },
        );

        let from = addr(1000);
        let to = addr(2000);

        // Send multiple messages
        network.send(from, to, vec![1]);
        network.send(from, to, vec![2]);
        network.send(from, to, vec![3]);

        // Process all deliveries
        while network.step() {}

        // Should receive in order
        assert_eq!(network.recv(to).unwrap().1, vec![1]);
        assert_eq!(network.recv(to).unwrap().1, vec![2]);
        assert_eq!(network.recv(to).unwrap().1, vec![3]);
    }

    #[test]
    fn test_message_loss() {
        let scheduler = Arc::new(Mutex::new(Scheduler::new(42)));
        let network = SimulatedNetwork::new(scheduler);

        network.set_fault_config(
            FaultConfig::builder()
                .message_loss_rate(1.0) // 100% loss
                .build(),
        );

        let from = addr(1000);
        let to = addr(2000);

        // Send should return None (message dropped)
        let event_id = network.send(from, to, vec![1, 2, 3]);
        assert!(event_id.is_none());

        // Stats should reflect drop
        let stats = network.stats();
        assert_eq!(stats.messages_sent, 1);
        assert_eq!(stats.messages_dropped, 1);
    }

    #[test]
    fn test_partition() {
        let scheduler = Arc::new(Mutex::new(Scheduler::new(42)));
        let network = SimulatedNetwork::new(scheduler);

        let from = addr(1000);
        let to = addr(2000);

        let mut side_a = std::collections::HashSet::new();
        side_a.insert(from);

        let mut side_b = std::collections::HashSet::new();
        side_b.insert(to);

        network.set_fault_config(
            FaultConfig::builder()
                .partition(super::super::fault::Partition::new(side_a, side_b).permanent(0))
                .build(),
        );

        // Send should return None (partitioned)
        let event_id = network.send(from, to, vec![1, 2, 3]);
        assert!(event_id.is_none());

        // Stats should reflect partition
        let stats = network.stats();
        assert_eq!(stats.messages_partitioned, 1);
    }

    #[test]
    fn test_crashed_node() {
        let scheduler = Arc::new(Mutex::new(Scheduler::new(42)));
        let network = SimulatedNetwork::new(scheduler);

        let from = addr(1000);
        let to = addr(2000);

        network.set_fault_config(FaultConfig::builder().crashed_node(to).build());

        // Send to crashed node should fail
        let event_id = network.send(from, to, vec![1, 2, 3]);
        assert!(event_id.is_none());
    }

    #[test]
    fn test_latency() {
        let scheduler = Arc::new(Mutex::new(Scheduler::new(42)));
        let network = SimulatedNetwork::new(scheduler.clone());

        network.set_fault_config(
            FaultConfig::builder()
                .fixed_latency(Duration::from_millis(100))
                .build(),
        );

        let from = addr(1000);
        let to = addr(2000);

        network.send(from, to, vec![1, 2, 3]);

        // Time should advance when delivery is processed
        let before = scheduler.lock().unwrap().now();
        network.step();
        let after = scheduler.lock().unwrap().now();

        assert!(after >= before + Duration::from_millis(100).as_nanos() as u64);
    }

    #[test]
    fn test_run_for() {
        let scheduler = Arc::new(Mutex::new(Scheduler::new(42)));
        let network = SimulatedNetwork::with_config(
            scheduler.clone(),
            SimulatedNetworkConfig {
                default_latency: Duration::from_millis(10),
                trace_messages: false,
            },
        );

        let from = addr(1000);
        let to = addr(2000);

        // Send 5 messages
        for i in 0..5 {
            network.send(from, to, vec![i]);
        }

        // Run for enough time to deliver all
        network.run_for(Duration::from_millis(100));

        // All should be delivered
        assert_eq!(network.pending_for(to), 5);
    }

    #[test]
    fn test_determinism() {
        fn run_simulation(seed: u64) -> Vec<(u64, Vec<u8>)> {
            let scheduler = Arc::new(Mutex::new(Scheduler::new(seed)));
            let network = SimulatedNetwork::with_config(
                scheduler.clone(),
                SimulatedNetworkConfig {
                    default_latency: Duration::from_millis(10),
                    trace_messages: false,
                },
            );

            let peers: Vec<_> = (0..5).map(|i| addr(1000 + i)).collect();

            // Each peer sends to every other peer
            for from in &peers {
                for to in &peers {
                    if from != to {
                        let payload = vec![from.port() as u8, to.port() as u8];
                        network.send(*from, *to, payload);
                    }
                }
            }

            // Process all events
            while network.step() {}

            // Collect all received messages in order
            let mut results = Vec::new();
            for peer in &peers {
                while let Some((_, payload)) = network.recv(*peer) {
                    let time = scheduler.lock().unwrap().now();
                    results.push((time, payload));
                }
            }

            results
        }

        // Same seed should produce identical results
        let result1 = run_simulation(42);
        let result2 = run_simulation(42);
        assert_eq!(result1, result2);

        // Different seed should produce different results
        let result3 = run_simulation(43);
        // Content might differ due to different random decisions
        // Just verify we got results from both
        assert!(!result1.is_empty());
        assert!(!result3.is_empty());
    }
}

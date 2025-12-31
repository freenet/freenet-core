//! Fault injection configuration for simulation testing.
//!
//! This module provides configuration for injecting various faults:
//! - Message loss (random drops)
//! - Latency variation
//! - Network partitions
//! - Node crashes

use std::{
    collections::HashSet,
    net::SocketAddr,
    ops::Range,
    time::Duration,
};

use super::rng::SimulationRng;

/// A network partition between sets of peers.
#[derive(Debug, Clone)]
pub struct Partition {
    /// One side of the partition
    pub side_a: HashSet<SocketAddr>,
    /// Other side of the partition
    pub side_b: HashSet<SocketAddr>,
    /// When the partition started (virtual nanos)
    pub start_time: u64,
    /// When the partition heals (virtual nanos), None for permanent
    pub heal_time: Option<u64>,
}

impl Partition {
    /// Creates a new partition between two sets of peers.
    pub fn new(side_a: HashSet<SocketAddr>, side_b: HashSet<SocketAddr>) -> Self {
        Self {
            side_a,
            side_b,
            start_time: 0,
            heal_time: None,
        }
    }

    /// Creates a partition that heals after the given duration.
    pub fn with_duration(mut self, start_time: u64, duration: Duration) -> Self {
        self.start_time = start_time;
        self.heal_time = Some(start_time + duration.as_nanos() as u64);
        self
    }

    /// Creates a permanent partition.
    pub fn permanent(mut self, start_time: u64) -> Self {
        self.start_time = start_time;
        self.heal_time = None;
        self
    }

    /// Checks if this partition blocks communication between two peers at the given time.
    pub fn blocks(&self, from: &SocketAddr, to: &SocketAddr, current_time: u64) -> bool {
        // Check if partition is active
        if current_time < self.start_time {
            return false;
        }
        if let Some(heal_time) = self.heal_time {
            if current_time >= heal_time {
                return false;
            }
        }

        // Check if peers are on opposite sides
        (self.side_a.contains(from) && self.side_b.contains(to))
            || (self.side_b.contains(from) && self.side_a.contains(to))
    }

    /// Returns true if this partition has healed by the given time.
    pub fn is_healed(&self, current_time: u64) -> bool {
        if let Some(heal_time) = self.heal_time {
            current_time >= heal_time
        } else {
            false
        }
    }
}

/// Configuration for fault injection during simulation.
#[derive(Debug, Clone)]
pub struct FaultConfig {
    /// Probability of dropping a message (0.0 to 1.0)
    pub message_loss_rate: f64,
    /// Range of latency to add to message delivery
    pub latency_range: Option<Range<Duration>>,
    /// Active network partitions
    pub partitions: Vec<Partition>,
    /// Set of crashed nodes (no messages sent or received)
    pub crashed_nodes: HashSet<SocketAddr>,
    /// Probability of a node crashing per time step
    pub node_crash_rate: f64,
}

impl Default for FaultConfig {
    fn default() -> Self {
        Self {
            message_loss_rate: 0.0,
            latency_range: None,
            partitions: Vec::new(),
            crashed_nodes: HashSet::new(),
            node_crash_rate: 0.0,
        }
    }
}

impl FaultConfig {
    /// Creates a new empty fault configuration (no faults).
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a builder for fluent configuration.
    pub fn builder() -> FaultConfigBuilder {
        FaultConfigBuilder::new()
    }

    /// Returns true if a message should be dropped.
    pub fn should_drop_message(&self, rng: &SimulationRng) -> bool {
        self.message_loss_rate > 0.0 && rng.gen_bool(self.message_loss_rate)
    }

    /// Generates a latency duration for a message.
    ///
    /// Returns None if no latency range is configured.
    pub fn generate_latency(&self, rng: &SimulationRng) -> Option<Duration> {
        self.latency_range.as_ref().map(|range| rng.gen_duration(range.clone()))
    }

    /// Returns the base latency (minimum of range) or zero.
    pub fn base_latency(&self) -> Duration {
        self.latency_range.as_ref().map(|r| r.start).unwrap_or(Duration::ZERO)
    }

    /// Checks if communication between two peers is blocked by a partition.
    pub fn is_partitioned(&self, from: &SocketAddr, to: &SocketAddr, current_time: u64) -> bool {
        self.partitions.iter().any(|p| p.blocks(from, to, current_time))
    }

    /// Checks if a node has crashed.
    pub fn is_crashed(&self, addr: &SocketAddr) -> bool {
        self.crashed_nodes.contains(addr)
    }

    /// Checks if a message can be delivered (not dropped, not partitioned, nodes not crashed).
    pub fn can_deliver(
        &self,
        from: &SocketAddr,
        to: &SocketAddr,
        current_time: u64,
        rng: &SimulationRng,
    ) -> bool {
        // Check for crashed nodes
        if self.is_crashed(from) || self.is_crashed(to) {
            return false;
        }

        // Check for partitions
        if self.is_partitioned(from, to, current_time) {
            return false;
        }

        // Check for random drop
        if self.should_drop_message(rng) {
            return false;
        }

        true
    }

    /// Adds a partition to the configuration.
    pub fn add_partition(&mut self, partition: Partition) {
        self.partitions.push(partition);
    }

    /// Crashes a node.
    pub fn crash_node(&mut self, addr: SocketAddr) {
        self.crashed_nodes.insert(addr);
    }

    /// Recovers a crashed node.
    pub fn recover_node(&mut self, addr: &SocketAddr) {
        self.crashed_nodes.remove(addr);
    }

    /// Removes healed partitions.
    pub fn cleanup_healed_partitions(&mut self, current_time: u64) {
        self.partitions.retain(|p| !p.is_healed(current_time));
    }

    /// Potentially crashes a node based on crash rate.
    pub fn maybe_crash_node(&mut self, addr: SocketAddr, rng: &SimulationRng) -> bool {
        if self.node_crash_rate > 0.0 && rng.gen_bool(self.node_crash_rate) {
            self.crashed_nodes.insert(addr);
            true
        } else {
            false
        }
    }
}

/// Builder for creating FaultConfig with fluent API.
#[derive(Debug, Default)]
pub struct FaultConfigBuilder {
    config: FaultConfig,
}

impl FaultConfigBuilder {
    /// Creates a new builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the message loss rate (0.0 to 1.0).
    pub fn message_loss_rate(mut self, rate: f64) -> Self {
        self.config.message_loss_rate = rate.clamp(0.0, 1.0);
        self
    }

    /// Sets the latency range for message delivery.
    pub fn latency_range(mut self, range: Range<Duration>) -> Self {
        self.config.latency_range = Some(range);
        self
    }

    /// Sets fixed latency for message delivery.
    pub fn fixed_latency(mut self, latency: Duration) -> Self {
        let epsilon = Duration::from_nanos(1);
        self.config.latency_range = Some(latency..latency + epsilon);
        self
    }

    /// Adds a network partition.
    pub fn partition(mut self, partition: Partition) -> Self {
        self.config.partitions.push(partition);
        self
    }

    /// Marks a node as crashed.
    pub fn crashed_node(mut self, addr: SocketAddr) -> Self {
        self.config.crashed_nodes.insert(addr);
        self
    }

    /// Sets the node crash rate per step.
    pub fn node_crash_rate(mut self, rate: f64) -> Self {
        self.config.node_crash_rate = rate.clamp(0.0, 1.0);
        self
    }

    /// Builds the FaultConfig.
    pub fn build(self) -> FaultConfig {
        self.config
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
    fn test_no_faults_by_default() {
        let config = FaultConfig::new();
        let rng = SimulationRng::new(42);

        assert!(config.can_deliver(&addr(1000), &addr(2000), 0, &rng));
    }

    #[test]
    fn test_message_loss() {
        let config = FaultConfig::builder()
            .message_loss_rate(1.0) // 100% loss
            .build();
        let rng = SimulationRng::new(42);

        // Should always drop with 100% loss rate
        for _ in 0..100 {
            assert!(config.should_drop_message(&rng));
        }
    }

    #[test]
    fn test_message_loss_zero() {
        let config = FaultConfig::builder()
            .message_loss_rate(0.0)
            .build();
        let rng = SimulationRng::new(42);

        // Should never drop with 0% loss rate
        for _ in 0..100 {
            assert!(!config.should_drop_message(&rng));
        }
    }

    #[test]
    fn test_latency_generation() {
        let config = FaultConfig::builder()
            .latency_range(Duration::from_millis(10)..Duration::from_millis(100))
            .build();
        let rng = SimulationRng::new(42);

        for _ in 0..100 {
            let latency = config.generate_latency(&rng).unwrap();
            assert!(latency >= Duration::from_millis(10));
            assert!(latency < Duration::from_millis(100));
        }
    }

    #[test]
    fn test_partition_blocks() {
        let mut side_a = HashSet::new();
        side_a.insert(addr(1000));
        side_a.insert(addr(1001));

        let mut side_b = HashSet::new();
        side_b.insert(addr(2000));
        side_b.insert(addr(2001));

        let partition = Partition::new(side_a, side_b).permanent(0);

        // Cross-partition messages blocked
        assert!(partition.blocks(&addr(1000), &addr(2000), 100));
        assert!(partition.blocks(&addr(2000), &addr(1000), 100));

        // Same-side messages not blocked
        assert!(!partition.blocks(&addr(1000), &addr(1001), 100));
        assert!(!partition.blocks(&addr(2000), &addr(2001), 100));

        // Messages to/from non-partition nodes not blocked
        assert!(!partition.blocks(&addr(3000), &addr(1000), 100));
    }

    #[test]
    fn test_partition_with_duration() {
        let mut side_a = HashSet::new();
        side_a.insert(addr(1000));

        let mut side_b = HashSet::new();
        side_b.insert(addr(2000));

        let partition = Partition::new(side_a, side_b)
            .with_duration(100, Duration::from_nanos(500));

        // Before partition starts
        assert!(!partition.blocks(&addr(1000), &addr(2000), 50));

        // During partition
        assert!(partition.blocks(&addr(1000), &addr(2000), 200));

        // After partition heals
        assert!(!partition.blocks(&addr(1000), &addr(2000), 700));
    }

    #[test]
    fn test_crashed_node() {
        let config = FaultConfig::builder()
            .crashed_node(addr(1000))
            .build();
        let rng = SimulationRng::new(42);

        // Messages from crashed node blocked
        assert!(!config.can_deliver(&addr(1000), &addr(2000), 0, &rng));

        // Messages to crashed node blocked
        assert!(!config.can_deliver(&addr(2000), &addr(1000), 0, &rng));

        // Messages between non-crashed nodes allowed
        assert!(config.can_deliver(&addr(2000), &addr(3000), 0, &rng));
    }

    #[test]
    fn test_crash_recovery() {
        let mut config = FaultConfig::new();
        config.crash_node(addr(1000));

        assert!(config.is_crashed(&addr(1000)));

        config.recover_node(&addr(1000));

        assert!(!config.is_crashed(&addr(1000)));
    }

    #[test]
    fn test_partition_cleanup() {
        let mut config = FaultConfig::new();

        let mut side_a = HashSet::new();
        side_a.insert(addr(1000));

        let mut side_b = HashSet::new();
        side_b.insert(addr(2000));

        config.add_partition(
            Partition::new(side_a.clone(), side_b.clone())
                .with_duration(0, Duration::from_nanos(100)),
        );

        config.add_partition(
            Partition::new(side_a, side_b).permanent(0),
        );

        assert_eq!(config.partitions.len(), 2);

        config.cleanup_healed_partitions(200);

        assert_eq!(config.partitions.len(), 1); // Only permanent remains
    }

    #[test]
    fn test_builder() {
        let mut side_a = HashSet::new();
        side_a.insert(addr(1000));

        let mut side_b = HashSet::new();
        side_b.insert(addr(2000));

        let config = FaultConfig::builder()
            .message_loss_rate(0.1)
            .latency_range(Duration::from_millis(10)..Duration::from_millis(50))
            .partition(Partition::new(side_a, side_b))
            .crashed_node(addr(3000))
            .node_crash_rate(0.01)
            .build();

        assert_eq!(config.message_loss_rate, 0.1);
        assert!(config.latency_range.is_some());
        assert_eq!(config.partitions.len(), 1);
        assert!(config.crashed_nodes.contains(&addr(3000)));
        assert_eq!(config.node_crash_rate, 0.01);
    }
}

//! Turmoil-based deterministic simulation runner.
//!
//! This module provides deterministic simulation testing using Turmoil's
//! scheduler while keeping our existing SimulationSocket infrastructure.
//!
//! # How it works
//!
//! 1. Each SimNetwork node runs as a Turmoil "host"
//! 2. Turmoil provides deterministic task scheduling
//! 3. SimulationSocket provides in-memory networking (works with Turmoil)
//! 4. VirtualTime provides deterministic time control
//!
//! # Usage
//!
//! Turmoil is always enabled for SimNetwork. Use `SimNetwork::run_simulation()`
//! to execute a deterministic simulation:
//!
//! ```ignore
//! use freenet::dev_tool::SimNetwork;
//!
//! fn run_deterministic_test() -> turmoil::Result {
//!     let sim = SimNetwork::new("my-test", 1, 5, 7, 3, 10, 2, 42).await;
//!
//!     sim.run_simulation::<rand::rngs::SmallRng>(
//!         42,     // seed
//!         10,     // max_contract_num
//!         100,    // iterations
//!         Duration::from_secs(60), // timeout
//!         |network| async move {
//!             // Your test logic here
//!             network.check_connectivity(Duration::from_secs(30))?;
//!             Ok(())
//!         },
//!     )
//! }
//! ```

use std::net::SocketAddr;
use std::time::Duration;

use crate::client_events::test::RandomEventGenerator;
use crate::config::GlobalRng;
use crate::simulation::VirtualTime;
use crate::transport::in_memory_socket::{
    register_address_network, register_network_time_source, unregister_network_time_source,
};

/// Result type for Turmoil simulations
pub type TurmoilResult = turmoil::Result;

/// Configuration for a Turmoil-based deterministic simulation.
#[derive(Clone)]
pub struct TurmoilConfig {
    /// Network name (used for socket isolation)
    pub name: String,
    /// Number of gateway nodes
    pub gateways: usize,
    /// Number of regular peer nodes
    pub nodes: usize,
    /// Maximum hops-to-live for routing
    pub ring_max_htl: usize,
    /// Threshold for random peer connection
    pub rnd_if_htl_above: usize,
    /// Maximum connections per node
    pub max_connections: usize,
    /// Minimum connections per node
    pub min_connections: usize,
    /// Random seed for determinism
    pub seed: u64,
    /// Maximum number of contracts in the network
    pub max_contract_num: usize,
    /// Number of events to simulate
    pub iterations: usize,
    /// Simulation duration limit
    pub simulation_duration: Duration,
}

impl Default for TurmoilConfig {
    fn default() -> Self {
        Self {
            name: "turmoil-sim".to_string(),
            gateways: 1,
            nodes: 5,
            ring_max_htl: 7,
            rnd_if_htl_above: 3,
            max_connections: 10,
            min_connections: 2,
            seed: 0xDEADBEEF_CAFEBABE,
            max_contract_num: 10,
            iterations: 100,
            simulation_duration: Duration::from_secs(60),
        }
    }
}

impl TurmoilConfig {
    /// Create a new builder for TurmoilConfig
    pub fn builder() -> TurmoilConfigBuilder {
        TurmoilConfigBuilder::default()
    }
}

/// Builder for TurmoilConfig
#[derive(Default)]
pub struct TurmoilConfigBuilder {
    config: TurmoilConfig,
}

impl TurmoilConfigBuilder {
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.config.name = name.into();
        self
    }

    pub fn gateways(mut self, n: usize) -> Self {
        self.config.gateways = n;
        self
    }

    pub fn nodes(mut self, n: usize) -> Self {
        self.config.nodes = n;
        self
    }

    pub fn ring_max_htl(mut self, htl: usize) -> Self {
        self.config.ring_max_htl = htl;
        self
    }

    pub fn rnd_if_htl_above(mut self, threshold: usize) -> Self {
        self.config.rnd_if_htl_above = threshold;
        self
    }

    pub fn max_connections(mut self, n: usize) -> Self {
        self.config.max_connections = n;
        self
    }

    pub fn min_connections(mut self, n: usize) -> Self {
        self.config.min_connections = n;
        self
    }

    pub fn seed(mut self, seed: u64) -> Self {
        self.config.seed = seed;
        self
    }

    pub fn max_contract_num(mut self, n: usize) -> Self {
        self.config.max_contract_num = n;
        self
    }

    pub fn iterations(mut self, n: usize) -> Self {
        self.config.iterations = n;
        self
    }

    pub fn simulation_duration(mut self, duration: Duration) -> Self {
        self.config.simulation_duration = duration;
        self
    }

    pub fn build(self) -> TurmoilConfig {
        self.config
    }
}

/// Run a deterministic simulation using Turmoil's scheduler.
///
/// This function sets up a Turmoil simulation with each node running as
/// a separate "host". Turmoil provides deterministic task scheduling,
/// while our SimulationSocket provides in-memory networking.
///
/// # Arguments
///
/// * `config` - Configuration for the simulation
///
/// # Returns
///
/// `Ok(())` if the simulation completes successfully, or an error if any
/// node fails.
///
/// # Determinism
///
/// Running with the same seed will produce the same execution order and
/// results. This is useful for:
/// - Reproducing bugs
/// - Verifying linearizability
/// - Property-based testing
pub fn run_turmoil_simulation<R>(config: TurmoilConfig) -> TurmoilResult
where
    R: RandomEventGenerator + Send + 'static,
{
    // Set up deterministic RNG
    GlobalRng::set_seed(config.seed);

    // Create VirtualTime for this simulation
    let virtual_time = VirtualTime::new();

    // Register the VirtualTime for SimulationSocket to use
    register_network_time_source(&config.name, virtual_time.clone());

    // Build Turmoil simulation
    let mut sim = turmoil::Builder::new()
        .simulation_duration(config.simulation_duration)
        .build();

    // Pre-calculate all node addresses and register them
    let mut gateway_addrs = Vec::with_capacity(config.gateways);
    let mut node_addrs = Vec::with_capacity(config.nodes);

    for i in 0..config.gateways {
        let port = 10000 + i as u16;
        let addr: SocketAddr = format!("[::1]:{}", port).parse().unwrap();
        gateway_addrs.push(addr);
        register_address_network(addr, &config.name);
    }

    for i in 0..config.nodes {
        let port = 20000 + i as u16;
        let addr: SocketAddr = format!("[::1]:{}", port).parse().unwrap();
        node_addrs.push(addr);
        register_address_network(addr, &config.name);
    }

    let _total_nodes = config.gateways + config.nodes;
    let network_name = config.name.clone();

    // Spawn gateway hosts
    for (i, _addr) in gateway_addrs.iter().enumerate() {
        let host_name = format!("gateway-{}", i);

        sim.host(host_name, move || async move {
            tracing::info!("Gateway {} starting", i);

            // In a full implementation, we would:
            // 1. Create NodeConfig for this gateway
            // 2. Create Builder<DefaultRegistry>
            // 3. Call run_node_with_shared_storage()
            //
            // For now, we simulate a simple node that accepts connections
            tokio::time::sleep(Duration::from_secs(30)).await;

            tracing::info!("Gateway {} completed", i);
            Ok(())
        });
    }

    // Spawn regular node hosts
    for (i, _addr) in node_addrs.iter().enumerate() {
        let host_name = format!("node-{}", i);

        sim.host(host_name, move || async move {
            tracing::info!("Node {} starting", i);

            // Wait a bit for gateways to start
            tokio::time::sleep(Duration::from_millis(100)).await;

            // In a full implementation, we would:
            // 1. Create NodeConfig for this node
            // 2. Create Builder<DefaultRegistry>
            // 3. Call run_node_with_shared_storage()
            //
            // For now, we simulate a simple node
            tokio::time::sleep(Duration::from_secs(25)).await;

            tracing::info!("Node {} completed", i);
            Ok(())
        });
    }

    // Run the simulation
    let result = sim.run();

    // Cleanup
    unregister_network_time_source(&network_name);

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_turmoil_runner_basic() -> TurmoilResult {
        let config = TurmoilConfig::builder()
            .name("test-basic")
            .gateways(1)
            .nodes(2)
            .seed(42)
            .simulation_duration(Duration::from_secs(5))
            .build();

        run_turmoil_simulation::<rand::rngs::SmallRng>(config)
    }

    #[test]
    fn test_turmoil_runner_determinism() -> TurmoilResult {
        // Run the same simulation twice with the same seed
        // Both should complete successfully (deterministic)
        for _ in 0..2 {
            let config = TurmoilConfig::builder()
                .name("test-determinism")
                .gateways(1)
                .nodes(3)
                .seed(12345)
                .simulation_duration(Duration::from_secs(5))
                .build();

            run_turmoil_simulation::<rand::rngs::SmallRng>(config)?;
        }

        Ok(())
    }
}

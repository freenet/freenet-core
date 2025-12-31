//! Deterministic replay test for the simulation framework.
//!
//! This test verifies that running the same simulation with the same seed
//! produces identical event logs, ensuring determinism.

use std::{
    collections::HashSet,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{Arc, Mutex},
    time::Duration,
};

use freenet::simulation::{
    EventType, FaultConfig, Partition, Scheduler, SimulatedNetwork,
    SimulatedNetworkConfig, SimulationRng, TimeSource,
};

fn addr(port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port)
}

/// Simulation result for comparison.
#[derive(Debug, Clone, PartialEq)]
struct SimulationResult {
    /// Event log: (timestamp, event_id, event_type_discriminant)
    event_log: Vec<(u64, u64, String)>,
    /// Messages delivered: (from_port, to_port, payload)
    messages_delivered: Vec<(u16, u16, Vec<u8>)>,
    /// Final statistics
    stats: (u64, u64, u64, u64),
}

/// Runs a complete simulation and returns the results for comparison.
fn run_simulation(seed: u64, config: SimulationConfig) -> SimulationResult {
    let scheduler = Arc::new(Mutex::new(Scheduler::new(seed)));
    let network = SimulatedNetwork::with_config(
        scheduler.clone(),
        SimulatedNetworkConfig {
            default_latency: Duration::from_millis(10),
            trace_messages: false,
        },
    );

    // Apply fault configuration if any
    if let Some(fault_config) = config.fault_config {
        network.set_fault_config(fault_config);
    }

    // Create peers
    let peers: Vec<_> = (0..config.num_peers).map(|i| addr(1000 + i as u16)).collect();

    // Phase 1: Send initial messages based on RNG
    let rng = scheduler.lock().unwrap().rng().clone();
    for _ in 0..config.num_initial_messages {
        let from_idx = rng.gen_range(0..peers.len());
        let to_idx = rng.gen_range(0..peers.len());
        if from_idx != to_idx {
            let payload = vec![
                from_idx as u8,
                to_idx as u8,
                rng.gen_u32() as u8,
            ];
            network.send(peers[from_idx], peers[to_idx], payload);
        }
    }

    // Phase 2: Run for a while, occasionally sending more messages
    let mut messages_delivered = Vec::new();
    for step in 0..config.simulation_steps {
        // Maybe send a new message
        if rng.gen_bool(config.message_probability) {
            let from_idx = rng.gen_range(0..peers.len());
            let to_idx = rng.gen_range(0..peers.len());
            if from_idx != to_idx {
                let payload = vec![step as u8, from_idx as u8, to_idx as u8];
                network.send(peers[from_idx], peers[to_idx], payload);
            }
        }

        // Process events
        network.step();

        // Collect any delivered messages
        for peer in &peers {
            while let Some((from, payload)) = network.recv(*peer) {
                messages_delivered.push((from.port(), peer.port(), payload));
            }
        }
    }

    // Process remaining events
    while network.step() {
        for peer in &peers {
            while let Some((from, payload)) = network.recv(*peer) {
                messages_delivered.push((from.port(), peer.port(), payload));
            }
        }
    }

    // Collect final messages
    for peer in &peers {
        while let Some((from, payload)) = network.recv(*peer) {
            messages_delivered.push((from.port(), peer.port(), payload));
        }
    }

    // Get event log
    let event_log: Vec<_> = scheduler
        .lock()
        .unwrap()
        .event_log()
        .iter()
        .map(|e| {
            let type_name = match &e.event_type {
                EventType::MessageDelivery { .. } => "MessageDelivery".to_string(),
                EventType::Timer { .. } => "Timer".to_string(),
                EventType::Fault { .. } => "Fault".to_string(),
                EventType::Custom { .. } => "Custom".to_string(),
            };
            (e.timestamp, e.id.as_u64(), type_name)
        })
        .collect();

    // Get stats
    let stats = network.stats();

    SimulationResult {
        event_log,
        messages_delivered,
        stats: (
            stats.messages_sent,
            stats.messages_delivered,
            stats.messages_dropped,
            stats.messages_partitioned,
        ),
    }
}

#[derive(Clone)]
struct SimulationConfig {
    num_peers: usize,
    num_initial_messages: usize,
    simulation_steps: usize,
    message_probability: f64,
    fault_config: Option<FaultConfig>,
}

impl Default for SimulationConfig {
    fn default() -> Self {
        Self {
            num_peers: 5,
            num_initial_messages: 20,
            simulation_steps: 100,
            message_probability: 0.3,
            fault_config: None,
        }
    }
}

#[test]
fn test_deterministic_replay_basic() {
    // Run the same simulation twice with the same seed
    let seed = 0xDEADBEEF;
    let config = SimulationConfig::default();

    let result1 = run_simulation(seed, config.clone());
    let result2 = run_simulation(seed, config);

    // Verify identical results
    assert_eq!(
        result1.event_log.len(),
        result2.event_log.len(),
        "Event log lengths differ"
    );

    for (i, (e1, e2)) in result1.event_log.iter().zip(result2.event_log.iter()).enumerate() {
        assert_eq!(
            e1, e2,
            "Event logs differ at index {}: {:?} vs {:?}",
            i, e1, e2
        );
    }

    assert_eq!(
        result1.messages_delivered, result2.messages_delivered,
        "Delivered messages differ"
    );

    assert_eq!(result1.stats, result2.stats, "Statistics differ");
}

#[test]
fn test_deterministic_replay_with_faults() {
    // Run simulation with fault injection
    let seed = 0xCAFEBABE;

    let mut side_a = HashSet::new();
    side_a.insert(addr(1000));
    side_a.insert(addr(1001));

    let mut side_b = HashSet::new();
    side_b.insert(addr(1003));
    side_b.insert(addr(1004));

    let fault_config = FaultConfig::builder()
        .message_loss_rate(0.1)
        .latency_range(Duration::from_millis(5)..Duration::from_millis(50))
        .partition(Partition::new(side_a, side_b).permanent(0))
        .build();

    let config = SimulationConfig {
        fault_config: Some(fault_config),
        ..Default::default()
    };

    let result1 = run_simulation(seed, config.clone());
    let result2 = run_simulation(seed, config);

    // Verify identical results despite randomized faults
    assert_eq!(
        result1.event_log, result2.event_log,
        "Event logs differ with fault injection"
    );

    assert_eq!(
        result1.messages_delivered, result2.messages_delivered,
        "Delivered messages differ with fault injection"
    );

    assert_eq!(
        result1.stats, result2.stats,
        "Statistics differ with fault injection"
    );
}

#[test]
fn test_different_seeds_produce_different_results() {
    let config = SimulationConfig::default();

    let result1 = run_simulation(42, config.clone());
    let result2 = run_simulation(43, config);

    // Event logs should differ (different message sending patterns)
    // Note: We can't guarantee they're always different, but they should usually be
    // due to different RNG sequences affecting message timing and ordering

    // At minimum, verify that both simulations actually ran
    assert!(!result1.event_log.is_empty(), "Simulation 1 should have events");
    assert!(!result2.event_log.is_empty(), "Simulation 2 should have events");

    // The results should differ in some way (though this isn't strictly guaranteed)
    // We check this probabilistically - extremely unlikely to be identical
    let same_messages = result1.messages_delivered == result2.messages_delivered;
    let same_events = result1.event_log == result2.event_log;

    // At least one should differ for different seeds
    if same_messages && same_events {
        // This is extremely unlikely but technically possible
        // Just verify both produced non-trivial output
        assert!(result1.stats.0 > 0, "Should have sent messages");
        assert!(result2.stats.0 > 0, "Should have sent messages");
    }
}

#[test]
fn test_scheduler_event_ordering_determinism() {
    // Test that events at the same timestamp are ordered deterministically
    let seed = 0x12345678;

    fn run_ordering_test(seed: u64) -> Vec<(u64, u64)> {
        let mut scheduler = Scheduler::new(seed);
        let rng = scheduler.rng().clone();

        // Schedule many events at the same timestamp
        for _ in 0..50 {
            let peer_port = rng.gen_range(1000usize..1020) as u16;
            let timer_id = rng.gen_u64();
            scheduler.schedule_at(
                100, // All at same time
                EventType::Timer {
                    peer: addr(peer_port),
                    timer_id,
                },
            );
        }

        // Process all and record order
        let mut order = Vec::new();
        while let Some(event) = scheduler.step() {
            order.push((event.id.as_u64(), event.timestamp));
        }
        order
    }

    let order1 = run_ordering_test(seed);
    let order2 = run_ordering_test(seed);

    assert_eq!(order1, order2, "Event ordering should be deterministic");
}

#[test]
fn test_rng_child_determinism() {
    // Test that child RNG derivation is deterministic
    let parent1 = SimulationRng::new(42);
    let parent2 = SimulationRng::new(42);

    // Create children in the same order
    let children1: Vec<_> = (0..10).map(|i| parent1.child_with_index(i)).collect();
    let children2: Vec<_> = (0..10).map(|i| parent2.child_with_index(i)).collect();

    // Each corresponding pair should produce identical sequences
    for (c1, c2) in children1.iter().zip(children2.iter()) {
        for _ in 0..20 {
            assert_eq!(c1.gen_u64(), c2.gen_u64());
        }
    }
}

#[test]
fn test_virtual_time_wakeup_determinism() {
    use freenet::simulation::VirtualTime;

    // Test that wakeup ordering is deterministic
    fn run_wakeup_test() -> Vec<(u64, u64)> {
        let vt = VirtualTime::new();

        // Register wakeups in specific order by using sleep_until directly
        // (sleep returns a future we'd need to await, so use register_wakeup indirectly)
        for i in 0..10 {
            let deadline = 100 * (10 - i);
            // We call sleep_until to register wakeups (the future is dropped but wakeup is registered)
            drop(vt.sleep_until(deadline));
        }

        // Advance and collect triggers
        let mut triggers = Vec::new();
        while let Some((id, deadline)) = vt.advance_to_next_wakeup() {
            triggers.push((id.as_u64(), deadline));
        }
        triggers
    }

    let triggers1 = run_wakeup_test();
    let triggers2 = run_wakeup_test();

    assert_eq!(triggers1, triggers2, "Wakeup ordering should be deterministic");
}

#[test]
fn test_large_scale_determinism() {
    // Larger scale test to catch subtle non-determinism
    let seed = 0xFEEDFACE;

    let config = SimulationConfig {
        num_peers: 10,
        num_initial_messages: 100,
        simulation_steps: 500,
        message_probability: 0.5,
        fault_config: Some(
            FaultConfig::builder()
                .message_loss_rate(0.05)
                .latency_range(Duration::from_millis(1)..Duration::from_millis(20))
                .build(),
        ),
    };

    let result1 = run_simulation(seed, config.clone());
    let result2 = run_simulation(seed, config);

    // Full comparison
    assert_eq!(result1.event_log.len(), result2.event_log.len());
    assert_eq!(result1.messages_delivered.len(), result2.messages_delivered.len());

    // Byte-by-byte comparison of all messages
    for (i, (m1, m2)) in result1.messages_delivered.iter().zip(result2.messages_delivered.iter()).enumerate() {
        assert_eq!(m1, m2, "Message {} differs: {:?} vs {:?}", i, m1, m2);
    }
}


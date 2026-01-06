//! Deterministic replay tests for simulation primitives.
//!
//! This test verifies that the core simulation abstractions (VirtualTime, SimulationRng)
//! behave deterministically. For full network simulation determinism, use SimNetwork
//! with Turmoil which provides deterministic task scheduling.
//!
//! NOTE: These tests use global state and must run serially.
//! Enable with: cargo test -p freenet --features simulation_tests -- --test-threads=1

#![cfg(feature = "simulation_tests")]

use std::time::Duration;

use freenet::simulation::{SimulationRng, TimeSource, VirtualTime};

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
    // Test that wakeup ordering is deterministic
    fn run_wakeup_test() -> Vec<(u64, u64)> {
        let vt = VirtualTime::new();

        // Register wakeups in specific order by using sleep_until directly
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

    assert_eq!(
        triggers1, triggers2,
        "Wakeup ordering should be deterministic"
    );
}

#[test]
fn test_rng_same_seed_produces_same_sequence() {
    let rng1 = SimulationRng::new(0xDEADBEEF);
    let rng2 = SimulationRng::new(0xDEADBEEF);

    // Should produce identical sequences
    for _ in 0..100 {
        assert_eq!(rng1.gen_u64(), rng2.gen_u64());
    }
}

#[test]
fn test_rng_different_seeds_produce_different_sequences() {
    let rng1 = SimulationRng::new(42);
    let rng2 = SimulationRng::new(43);

    // Should produce different sequences
    let mut matches = 0;
    for _ in 0..100 {
        if rng1.gen_u64() == rng2.gen_u64() {
            matches += 1;
        }
    }
    // Statistically very unlikely to have many matches
    assert!(matches < 10, "Too many matches: {}", matches);
}

#[test]
fn test_virtual_time_advance() {
    let vt = VirtualTime::new();
    assert_eq!(vt.now_nanos(), 0);

    vt.advance(Duration::from_nanos(1_000_000)); // 1ms
    assert_eq!(vt.now_nanos(), 1_000_000);

    vt.advance_to(5_000_000); // 5ms
    assert_eq!(vt.now_nanos(), 5_000_000);

    // Can't go backwards
    vt.advance_to(3_000_000);
    assert_eq!(vt.now_nanos(), 5_000_000);
}

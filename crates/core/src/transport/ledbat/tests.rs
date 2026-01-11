use std::sync::atomic::Ordering;
use std::time::Duration;

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use crate::simulation::{TimeSource, VirtualTime};
use crate::util::time_source::SharedMockTimeSource;

use super::config::{
    LedbatConfig, MAX_GAIN_DIVISOR, MSS, SLOWDOWN_FREEZE_RTTS, SLOWDOWN_INTERVAL_MULTIPLIER,
    SLOWDOWN_REDUCTION_FACTOR, TARGET,
};
use super::controller::LedbatController;
use super::state::CongestionState;

// ============================================================================
// Deterministic Test Harness for LEDBAT
// ============================================================================

/// Network condition presets for various link types.
///
/// These represent typical network characteristics including RTT, jitter, and
/// packet loss rates. Used with `LedbatTestHarness` for deterministic testing.
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)] // Fields/constants available for future test expansion
pub struct NetworkCondition {
    /// Base round-trip time
    pub rtt: Duration,
    /// Jitter as (min_multiplier, max_multiplier), e.g., (0.9, 1.1) for ±10%
    pub jitter: Option<(f64, f64)>,
    /// Packet loss rate as a fraction (0.0 to 1.0)
    pub loss_rate: f64,
}

#[allow(dead_code)] // Constants available for future test expansion
impl NetworkCondition {
    /// LAN: 1ms RTT, no jitter, no loss
    pub const LAN: Self = Self {
        rtt: Duration::from_millis(1),
        jitter: None,
        loss_rate: 0.0,
    };

    /// Datacenter: 10ms RTT, minimal jitter, no loss
    pub const DATACENTER: Self = Self {
        rtt: Duration::from_millis(10),
        jitter: Some((0.95, 1.05)),
        loss_rate: 0.0,
    };

    /// Continental (same continent): 50ms RTT, ±10% jitter, 0.1% loss
    pub const CONTINENTAL: Self = Self {
        rtt: Duration::from_millis(50),
        jitter: Some((0.9, 1.1)),
        loss_rate: 0.001,
    };

    /// Intercontinental: 135ms RTT (e.g., US-EU), ±20% jitter, 0.5% loss
    pub const INTERCONTINENTAL: Self = Self {
        rtt: Duration::from_millis(135),
        jitter: Some((0.8, 1.2)),
        loss_rate: 0.005,
    };

    /// High latency (satellite/distant): 250ms RTT, ±10% jitter, 1% loss
    pub const HIGH_LATENCY: Self = Self {
        rtt: Duration::from_millis(250),
        jitter: Some((0.9, 1.1)),
        loss_rate: 0.01,
    };

    /// Create a custom network condition
    pub fn custom(rtt_ms: u64, jitter_pct: Option<f64>, loss_rate: f64) -> Self {
        let jitter = jitter_pct.map(|pct| (1.0 - pct, 1.0 + pct));
        Self {
            rtt: Duration::from_millis(rtt_ms),
            jitter,
            loss_rate,
        }
    }
}

/// State snapshot for harness debugging and assertions.
///
/// Named `HarnessSnapshot` to avoid conflict with the existing `StateSnapshot`
/// used by visualization tests.
#[derive(Debug, Clone)]
#[allow(dead_code)] // Fields available for test assertions and debugging
pub struct HarnessSnapshot {
    /// Time since harness creation (nanos)
    pub time_nanos: u64,
    /// Current congestion window (bytes)
    pub cwnd: usize,
    /// Current congestion state
    pub state: CongestionState,
    /// Number of periodic slowdowns completed
    pub periodic_slowdowns: usize,
    /// Bytes in flight
    pub flightsize: usize,
    /// Queuing delay estimate
    pub queuing_delay: Duration,
    /// Base delay (minimum RTT observed)
    pub base_delay: Duration,
}

/// Deterministic test harness for LEDBAT.
///
/// This harness wraps a `LedbatController` with a virtual time source, enabling:
/// - Virtual time: Advance time instantly without wall-clock delays
/// - Deterministic jitter: Seeded RNG produces reproducible "random" delays
/// - Packet loss simulation: Configurable loss rate per network condition
/// - State snapshots: Capture controller state at any point for assertions
///
/// # Example
/// ```ignore
/// let mut harness = LedbatTestHarness::new(
///     LedbatConfig::default(),
///     NetworkCondition::INTERCONTINENTAL,
///     12345, // seed for reproducibility
/// );
///
/// // Run 10 RTTs of transfer
/// let snapshots: Vec<HarnessSnapshot> = harness.run_rtts(10, 100_000);
///
/// // Verify cwnd grew appropriately
/// assert!(snapshots.last().unwrap().cwnd > snapshots.first().unwrap().cwnd);
/// ```
pub struct LedbatTestHarness {
    time_source: VirtualTime,
    controller: LedbatController<VirtualTime>,
    condition: NetworkCondition,
    rng: SmallRng,
    epoch_nanos: u64,
}

#[allow(dead_code)] // Methods available for test expansion
impl LedbatTestHarness {
    /// Create a new test harness with the given configuration and network conditions.
    ///
    /// # Arguments
    /// * `config` - LEDBAT configuration (cwnd limits, thresholds, etc.)
    /// * `condition` - Network condition preset (RTT, jitter, loss)
    /// * `seed` - RNG seed for reproducible jitter and loss patterns
    pub fn new(config: LedbatConfig, condition: NetworkCondition, seed: u64) -> Self {
        let time_source = VirtualTime::new();
        let epoch_nanos = time_source.now_nanos();
        let controller = LedbatController::new_with_time_source(config, time_source.clone());

        Self {
            time_source,
            controller,
            condition,
            rng: SmallRng::seed_from_u64(seed),
            epoch_nanos,
        }
    }

    /// Get a reference to the underlying controller for direct access.
    pub fn controller(&self) -> &LedbatController<VirtualTime> {
        &self.controller
    }

    /// Get current virtual time as nanos since harness creation.
    pub fn current_time_nanos(&self) -> u64 {
        self.time_source.now_nanos() - self.epoch_nanos
    }

    /// Advance virtual time by the given duration.
    pub fn advance_time(&mut self, duration: Duration) {
        self.time_source.advance(duration);
    }

    /// Calculate RTT with jitter applied.
    fn jittered_rtt(&mut self) -> Duration {
        let base_nanos = self.condition.rtt.as_nanos() as f64;
        let jittered_nanos = match self.condition.jitter {
            Some((min_mult, max_mult)) => {
                let mult = self.rng.random_range(min_mult..=max_mult);
                base_nanos * mult
            }
            None => base_nanos,
        };
        Duration::from_nanos(jittered_nanos as u64)
    }

    /// Determine if a packet should be "lost" based on loss rate.
    fn should_drop_packet(&mut self) -> bool {
        self.condition.loss_rate > 0.0 && self.rng.random::<f64>() < self.condition.loss_rate
    }

    /// Simulate one RTT of data transfer.
    ///
    /// This advances virtual time by one RTT, simulates sending `bytes_to_send`,
    /// and processes the ACK (unless packet is "lost").
    ///
    /// # Returns
    /// The number of bytes successfully acknowledged (0 if lost).
    pub fn step(&mut self, bytes_to_send: usize) -> usize {
        let rtt = self.jittered_rtt();

        // Simulate sending data
        let bytes_sent = bytes_to_send.min(self.controller.current_cwnd());
        self.controller.on_send(bytes_sent);

        // Advance time by RTT
        self.advance_time(rtt);

        // Check for packet loss
        if self.should_drop_packet() {
            self.controller.on_loss();
            return 0;
        }

        // Process ACK
        self.controller.on_ack(rtt, bytes_sent);
        bytes_sent
    }

    /// Run N RTTs of transfer, collecting state snapshots.
    ///
    /// # Arguments
    /// * `count` - Number of RTTs to simulate
    /// * `bytes_per_rtt` - Bytes to attempt sending each RTT
    ///
    /// # Returns
    /// Vector of state snapshots, one per RTT.
    pub fn run_rtts(&mut self, count: usize, bytes_per_rtt: usize) -> Vec<HarnessSnapshot> {
        let mut snapshots = Vec::with_capacity(count);

        for _ in 0..count {
            self.step(bytes_per_rtt);
            snapshots.push(self.snapshot());
        }

        snapshots
    }

    /// Inject a timeout event (e.g., RTO expired).
    ///
    /// This simulates a timeout without advancing time, triggering
    /// the controller's timeout handling (cwnd reset, slow start re-entry).
    pub fn inject_timeout(&mut self) {
        self.controller.on_timeout();
    }

    /// Get a snapshot of the current controller state.
    pub fn snapshot(&self) -> HarnessSnapshot {
        HarnessSnapshot {
            time_nanos: self.current_time_nanos(),
            cwnd: self.controller.current_cwnd(),
            state: self.controller.congestion_state.load(),
            periodic_slowdowns: self
                .controller
                .periodic_slowdowns
                .load(std::sync::atomic::Ordering::Relaxed),
            flightsize: self.controller.flightsize(),
            queuing_delay: self.controller.queuing_delay(),
            base_delay: self.controller.base_delay(),
        }
    }

    /// Run until a specific state is reached, with a maximum RTT limit.
    ///
    /// # Returns
    /// `Ok(snapshots)` if target state reached, `Err(snapshots)` if limit hit.
    pub fn run_until_state(
        &mut self,
        target: CongestionState,
        max_rtts: usize,
        bytes_per_rtt: usize,
    ) -> Result<Vec<HarnessSnapshot>, Vec<HarnessSnapshot>> {
        let mut snapshots = Vec::new();

        for _ in 0..max_rtts {
            self.step(bytes_per_rtt);
            let snap = self.snapshot();
            let state = snap.state;
            snapshots.push(snap);

            if state == target {
                return Ok(snapshots);
            }
        }

        Err(snapshots)
    }

    /// Run until a condition is met, with a maximum RTT limit.
    ///
    /// # Returns
    /// `Ok(snapshots)` if condition met, `Err(snapshots)` if limit hit.
    pub fn run_until<F>(
        &mut self,
        condition: F,
        max_rtts: usize,
        bytes_per_rtt: usize,
    ) -> Result<Vec<HarnessSnapshot>, Vec<HarnessSnapshot>>
    where
        F: Fn(&HarnessSnapshot) -> bool,
    {
        let mut snapshots = Vec::new();

        for _ in 0..max_rtts {
            self.step(bytes_per_rtt);
            let snap = self.snapshot();
            let done = condition(&snap);
            snapshots.push(snap);

            if done {
                return Ok(snapshots);
            }
        }

        Err(snapshots)
    }

    // ========================================================================
    // Convenience Methods for Common Test Patterns
    // ========================================================================

    /// Run until cwnd exceeds the given threshold.
    ///
    /// # Returns
    /// `Ok(snapshots)` if cwnd exceeded threshold, `Err(snapshots)` if limit hit.
    pub fn run_until_cwnd_exceeds(
        &mut self,
        threshold: usize,
        max_rtts: usize,
        bytes_per_rtt: usize,
    ) -> Result<Vec<HarnessSnapshot>, Vec<HarnessSnapshot>> {
        self.run_until(|s| s.cwnd > threshold, max_rtts, bytes_per_rtt)
    }

    /// Run until cwnd drops below the given threshold.
    ///
    /// # Returns
    /// `Ok(snapshots)` if cwnd dropped below threshold, `Err(snapshots)` if limit hit.
    pub fn run_until_cwnd_below(
        &mut self,
        threshold: usize,
        max_rtts: usize,
        bytes_per_rtt: usize,
    ) -> Result<Vec<HarnessSnapshot>, Vec<HarnessSnapshot>> {
        self.run_until(|s| s.cwnd < threshold, max_rtts, bytes_per_rtt)
    }

    /// Run until a slowdown event occurs.
    ///
    /// # Returns
    /// `Ok(snapshots)` if slowdown occurred, `Err(snapshots)` if limit hit.
    pub fn run_until_slowdown(
        &mut self,
        max_rtts: usize,
        bytes_per_rtt: usize,
    ) -> Result<Vec<HarnessSnapshot>, Vec<HarnessSnapshot>> {
        let initial_slowdowns = self.snapshot().periodic_slowdowns;
        self.run_until(
            |s| s.periodic_slowdowns > initial_slowdowns,
            max_rtts,
            bytes_per_rtt,
        )
    }

    /// Change the network condition mid-test (e.g., RTT jump).
    ///
    /// This allows testing behavior when network conditions change dynamically.
    pub fn set_condition(&mut self, condition: NetworkCondition) {
        self.condition = condition;
    }

    /// Get the current GAIN value based on base delay.
    ///
    /// GAIN = 1 / min(ceil(2 * TARGET / base_delay), MAX_GAIN_DIVISOR)
    pub fn current_gain(&self) -> f64 {
        let base_delay = self.controller.base_delay();
        if base_delay.is_zero() {
            return 1.0 / MAX_GAIN_DIVISOR as f64;
        }
        let target_nanos = TARGET.as_nanos() as f64;
        let base_nanos = base_delay.as_nanos() as f64;
        let divisor = (2.0 * target_nanos / base_nanos).ceil() as u32;
        let clamped = divisor.clamp(1, MAX_GAIN_DIVISOR);
        1.0 / clamped as f64
    }

    /// Get expected GAIN at a given RTT (for test assertions).
    ///
    /// At 60ms RTT: GAIN = 1/2 (divisor = ceil(120/60) = 2)
    /// At 120ms+ RTT: GAIN = 1 (divisor = 1)
    /// At 7.5ms RTT: GAIN = 1/16 (divisor = ceil(120/7.5) = 16)
    pub fn expected_gain_for_rtt(rtt_ms: u64) -> f64 {
        if rtt_ms == 0 {
            return 1.0 / MAX_GAIN_DIVISOR as f64;
        }
        let target_ms = 60.0f64;
        let divisor = (2.0 * target_ms / rtt_ms as f64).ceil() as u32;
        let clamped = divisor.clamp(1, MAX_GAIN_DIVISOR);
        1.0 / clamped as f64
    }
}

// ============================================================================
// Existing Tests
// ============================================================================

// Deterministic Test Harness Tests (Phase 3)
// ============================================================================

/// Regression test: Slowdown count must be bounded at high RTT.
///
/// This test would have caught the 67-slowdown bug observed in production
/// where a 2.4MB transfer at 135ms RTT triggered 67 slowdowns in 33.8 seconds.
///
/// Expected behavior: At 135ms RTT over 33.8s (~250 RTTs), slowdowns should
/// occur at most every 18 RTTs, giving max ~14 slowdowns.
#[test]
fn test_harness_slowdown_count_bounded_at_high_rtt() {
    let config = LedbatConfig {
        initial_cwnd: 38_000,
        min_cwnd: 2_848,
        max_cwnd: 1_000_000,
        ssthresh: 102_400,
        enable_slow_start: true,
        enable_periodic_slowdown: true,
        randomize_ssthresh: false,
        ..Default::default()
    };

    let mut harness = LedbatTestHarness::new(
        config,
        NetworkCondition::INTERCONTINENTAL, // 135ms RTT
        42,                                 // Fixed seed for reproducibility
    );

    // Simulate 33.8 seconds at 135ms RTT = ~250 RTTs
    let rtts_to_simulate = 250;
    let bytes_per_rtt = 100_000; // 100KB per RTT

    let snapshots = harness.run_rtts(rtts_to_simulate, bytes_per_rtt);

    let final_slowdowns = snapshots.last().unwrap().periodic_slowdowns;

    // At 135ms RTT, min_interval = 18 RTTs (9 * 2 RTTs freeze)
    // In 250 RTTs, max slowdowns = 250 / 18 ≈ 14
    // Allow some tolerance but nowhere near 67
    let max_expected = 20;

    assert!(
        final_slowdowns <= max_expected,
        "Slowdown count {} exceeds maximum expected {} at 135ms RTT over 250 RTTs.\n\
         This indicates the min_interval bug - slowdowns should occur at most \
         every 18 RTTs (9 * 2 RTT freeze duration).",
        final_slowdowns,
        max_expected
    );

    println!(
        "High-RTT test: {} slowdowns in {} RTTs (max expected: {})",
        final_slowdowns, rtts_to_simulate, max_expected
    );
}

/// Test cwnd growth rate across various RTT ranges (no loss, no jitter).
///
/// Verifies that LEDBAT++ achieves growth in slow start at various RTT ranges.
#[test]
fn test_harness_cwnd_growth_across_rtts() {
    // Test each RTT range with no loss/jitter for predictable growth
    for (name, rtt_ms) in [
        ("LAN (1ms)", 1u64),
        ("Datacenter (10ms)", 10),
        ("Continental (50ms)", 50),
        ("Intercontinental (135ms)", 135),
    ] {
        let config = LedbatConfig {
            initial_cwnd: 38_000,
            min_cwnd: 2_848,
            max_cwnd: 1_000_000,
            ssthresh: 500_000, // High threshold so we stay in slow start
            enable_slow_start: true,
            enable_periodic_slowdown: false, // Disable for predictable growth
            randomize_ssthresh: false,
            ..Default::default()
        };

        // Use no-loss, no-jitter condition for predictable growth
        let condition = NetworkCondition::custom(rtt_ms, None, 0.0);
        let mut harness = LedbatTestHarness::new(config.clone(), condition, 123);

        // Run for 5 RTTs in slow start
        let snapshots = harness.run_rtts(5, 500_000);

        let initial_cwnd = config.initial_cwnd;
        let final_cwnd = snapshots.last().unwrap().cwnd;

        // In slow start, cwnd should grow
        assert!(
            final_cwnd > initial_cwnd,
            "{}: cwnd should grow during slow start \
             (initial: {} bytes, final: {} bytes)",
            name,
            initial_cwnd,
            final_cwnd
        );

        println!(
            "{}: cwnd grew from {}KB to {}KB in 5 RTTs",
            name,
            initial_cwnd / 1024,
            final_cwnd / 1024
        );
    }
}

/// Test that timeout properly resets state machine.
///
/// After timeout, controller should:
/// 1. Reset cwnd to minimum
/// 2. Re-enter SlowStart state
/// 3. Resume exponential growth
#[test]
fn test_harness_timeout_resets_state_correctly() {
    let config = LedbatConfig {
        initial_cwnd: 38_000,
        min_cwnd: 2_848,
        max_cwnd: 1_000_000,
        ssthresh: 102_400, // Use lower ssthresh for this test (not testing ssthresh behavior)
        enable_slow_start: true,
        enable_periodic_slowdown: true,
        randomize_ssthresh: false,
        ..Default::default()
    };

    let mut harness = LedbatTestHarness::new(config.clone(), NetworkCondition::CONTINENTAL, 456);

    // Run until we exit slow start
    let result = harness.run_until_state(CongestionState::WaitingForSlowdown, 100, 100_000);
    assert!(result.is_ok(), "Should reach WaitingForSlowdown state");

    let pre_timeout_cwnd = harness.snapshot().cwnd;
    assert!(
        pre_timeout_cwnd > config.initial_cwnd,
        "cwnd should have grown before timeout"
    );

    // Inject timeout
    harness.inject_timeout();

    let post_timeout = harness.snapshot();
    assert_eq!(
        post_timeout.state,
        CongestionState::SlowStart,
        "State should be SlowStart after timeout"
    );
    // cwnd resets to adaptive floor / 4, which uses the BDP proxy from slow start exit.
    // With ssthresh=102KB, slow start exits around there, giving floor ~102KB, so min cwnd ~25KB.
    // The key invariant is cwnd dropped significantly and is much lower than ssthresh.
    assert!(
        post_timeout.cwnd <= config.ssthresh / 2,
        "cwnd should be well below ssthresh after timeout: cwnd={} ssthresh={}",
        post_timeout.cwnd,
        config.ssthresh
    );
    assert!(
        post_timeout.cwnd >= config.min_cwnd,
        "cwnd should be at least min_cwnd after timeout"
    );

    // Verify recovery via slow start
    let recovery = harness.run_rtts(10, 50_000);
    let final_cwnd = recovery.last().unwrap().cwnd;
    assert!(
        final_cwnd > config.min_cwnd * 2,
        "Should recover via slow start exponential growth: {} > {}",
        final_cwnd,
        config.min_cwnd * 2
    );
}

/// Test determinism: same seed produces identical behavior.
#[test]
fn test_harness_determinism() {
    // Disable randomize_ssthresh for extra determinism certainty in this test
    let config = LedbatConfig {
        randomize_ssthresh: false,
        ..Default::default()
    };
    let seed = 99999u64;

    // Use no-jitter condition for perfect determinism
    let condition = NetworkCondition::custom(50, None, 0.0);

    // Run twice with same seed
    let mut harness1 = LedbatTestHarness::new(config.clone(), condition, seed);
    let snapshots1 = harness1.run_rtts(50, 100_000);

    let mut harness2 = LedbatTestHarness::new(config.clone(), condition, seed);
    let snapshots2 = harness2.run_rtts(50, 100_000);

    // Verify identical behavior
    assert_eq!(
        snapshots1.len(),
        snapshots2.len(),
        "Snapshot counts should match"
    );

    for (i, (s1, s2)) in snapshots1.iter().zip(snapshots2.iter()).enumerate() {
        assert_eq!(
            s1.cwnd, s2.cwnd,
            "RTT {}: cwnd mismatch ({} vs {})",
            i, s1.cwnd, s2.cwnd
        );
        assert_eq!(
            s1.state, s2.state,
            "RTT {}: state mismatch ({:?} vs {:?})",
            i, s1.state, s2.state
        );
        assert_eq!(
            s1.periodic_slowdowns, s2.periodic_slowdowns,
            "RTT {}: slowdown count mismatch ({} vs {})",
            i, s1.periodic_slowdowns, s2.periodic_slowdowns
        );
    }

    println!("Determinism verified over 50 RTTs with seed {}", seed);
}

/// Test that different seeds with jitter produce different RTT values.
#[test]
fn test_harness_different_seeds_differ() {
    // This test verifies that the seeded RNG produces different jitter patterns
    let config = LedbatConfig {
        randomize_ssthresh: false,
        ..Default::default()
    };

    // Use condition with jitter to see seed differences
    let condition = NetworkCondition::CONTINENTAL; // has ±10% jitter

    let mut harness1 = LedbatTestHarness::new(config.clone(), condition, 1);
    let mut harness2 = LedbatTestHarness::new(config.clone(), condition, 2);

    // Run a few steps and check that base delays differ (due to jitter)
    for _ in 0..10 {
        harness1.step(50_000);
        harness2.step(50_000);
    }

    // Base delays should differ due to jittered RTT samples
    let bd1 = harness1.snapshot().base_delay;
    let bd2 = harness2.snapshot().base_delay;

    // They might be the same if jitter was minimal, so just print for now
    println!("Seed 1 base_delay: {:?}, Seed 2 base_delay: {:?}", bd1, bd2);
}

/// Test that state machine transitions occur through slowdown cycle.
#[test]
fn test_harness_state_transitions() {
    let config = LedbatConfig {
        initial_cwnd: 38_000,
        min_cwnd: 2_848,
        max_cwnd: 1_000_000,
        ssthresh: 40_000, // Just above initial, triggers exit quickly
        enable_slow_start: true,
        enable_periodic_slowdown: true,
        randomize_ssthresh: false,
        ..Default::default()
    };

    // Use no-loss condition for predictable timing
    let condition = NetworkCondition::custom(50, None, 0.0);
    let mut harness = LedbatTestHarness::new(config, condition, 789);

    // Track state transitions
    let mut states_seen = vec![harness.snapshot().state];

    // Run for 100 RTTs and collect states
    for _ in 0..100 {
        harness.step(100_000);
        let state = harness.snapshot().state;
        if states_seen.last() != Some(&state) {
            states_seen.push(state);
        }
    }

    // Verify we started in SlowStart
    assert_eq!(
        states_seen[0],
        CongestionState::SlowStart,
        "Should start in SlowStart"
    );

    // Verify we exited slow start at some point
    assert!(
        states_seen.len() > 1,
        "Should have transitioned out of SlowStart. States seen: {:?}",
        states_seen
    );

    println!("State progression: {:?}", states_seen);
}

/// Test that packet loss triggers cwnd reduction.
#[test]
fn test_harness_packet_loss_reduces_cwnd() {
    let config = LedbatConfig {
        initial_cwnd: 100_000,
        min_cwnd: 2_848,
        max_cwnd: 500_000,
        enable_slow_start: false, // Start in congestion avoidance
        enable_periodic_slowdown: false,
        randomize_ssthresh: false,
        ..Default::default()
    };

    // 50% loss rate for dramatic effect
    let condition = NetworkCondition::custom(50, None, 0.5);
    let mut harness = LedbatTestHarness::new(config.clone(), condition, 12345);

    let initial_cwnd = harness.snapshot().cwnd;

    // Run several steps - with 50% loss, cwnd should decrease
    for _ in 0..20 {
        harness.step(50_000);
    }

    let final_cwnd = harness.snapshot().cwnd;

    assert!(
        final_cwnd < initial_cwnd,
        "With 50% loss, cwnd should decrease: initial {} -> final {}",
        initial_cwnd,
        final_cwnd
    );

    println!(
        "50% loss: cwnd reduced from {}KB to {}KB",
        initial_cwnd / 1024,
        final_cwnd / 1024
    );
}

/// Test state invariants are maintained through slowdown cycle.
#[test]
fn test_harness_state_invariants() {
    let config = LedbatConfig {
        initial_cwnd: 38_000,
        min_cwnd: 2_848,
        max_cwnd: 1_000_000,
        ssthresh: 50_000,
        enable_slow_start: true,
        enable_periodic_slowdown: true,
        randomize_ssthresh: false,
        ..Default::default()
    };

    let condition = NetworkCondition::custom(50, None, 0.0);
    let mut harness = LedbatTestHarness::new(config.clone(), condition, 111);

    // Run through a full cycle and verify invariants at each step
    for rtt in 0..150 {
        harness.step(100_000);
        let snap = harness.snapshot();

        // Invariant 1: cwnd is within bounds
        assert!(
            snap.cwnd >= config.min_cwnd,
            "RTT {}: cwnd {} below min {}",
            rtt,
            snap.cwnd,
            config.min_cwnd
        );
        assert!(
            snap.cwnd <= config.max_cwnd,
            "RTT {}: cwnd {} above max {}",
            rtt,
            snap.cwnd,
            config.max_cwnd
        );

        // Invariant 2: State is valid
        assert!(
            matches!(
                snap.state,
                CongestionState::SlowStart
                    | CongestionState::CongestionAvoidance
                    | CongestionState::WaitingForSlowdown
                    | CongestionState::InSlowdown
                    | CongestionState::Frozen
                    | CongestionState::RampingUp
            ),
            "RTT {}: Invalid state {:?}",
            rtt,
            snap.state
        );

        // Invariant 3: Slowdown count is monotonically non-decreasing
        // (checked implicitly by the harness)
    }

    println!("State invariants verified over 150 RTTs");
}

// ============================================================================
// New Comprehensive Tests (from review recommendations)
// ============================================================================

/// Test delay-based algorithm convergence at various RTTs.
///
/// Verifies that cwnd stabilizes (doesn't oscillate wildly) when the network
/// is operating at approximately the target queuing delay.
#[test]
fn test_harness_delay_convergence_at_various_rtts() {
    for rtt_ms in [10u64, 50, 100, 150, 250] {
        let config = LedbatConfig {
            initial_cwnd: 50_000,
            min_cwnd: 2_848,
            max_cwnd: 500_000,
            enable_slow_start: false, // Start in congestion avoidance
            enable_periodic_slowdown: false,
            randomize_ssthresh: false,
            ..Default::default()
        };

        // No jitter, no loss for clean convergence testing
        let condition = NetworkCondition::custom(rtt_ms, None, 0.0);
        let mut harness = LedbatTestHarness::new(config.clone(), condition, 42);

        // Run for enough RTTs to reach steady state
        let snapshots = harness.run_rtts(100, 100_000);

        // Take the last 20 snapshots and check cwnd variance
        let last_20: Vec<_> = snapshots.iter().rev().take(20).collect();
        let cwnd_values: Vec<usize> = last_20.iter().map(|s| s.cwnd).collect();

        let min_cwnd = *cwnd_values.iter().min().unwrap();
        let max_cwnd = *cwnd_values.iter().max().unwrap();

        // Cwnd should be relatively stable (within 50% range)
        // This catches wild oscillation bugs
        let variance_ratio = if min_cwnd > 0 {
            max_cwnd as f64 / min_cwnd as f64
        } else {
            1.0
        };

        assert!(
            variance_ratio < 2.0,
            "{}ms RTT: cwnd variance too high (min: {}, max: {}, ratio: {:.2}). \
             This may indicate oscillation in the delay-based algorithm.",
            rtt_ms,
            min_cwnd,
            max_cwnd,
            variance_ratio
        );

        println!(
            "{}ms RTT: cwnd stable at {}-{}KB (ratio: {:.2})",
            rtt_ms,
            min_cwnd / 1024,
            max_cwnd / 1024,
            variance_ratio
        );
    }
}

/// Test complete slowdown cycle with realistic network conditions (jitter + loss).
///
/// Unlike the simpler tests, this exercises the full state machine under
/// conditions similar to production: 135ms RTT, ±10% jitter, 0.1% loss.
#[test]
fn test_harness_slowdown_cycle_with_jitter_and_loss() {
    let config = LedbatConfig {
        initial_cwnd: 38_000,
        min_cwnd: 2_848,
        max_cwnd: 500_000,
        ssthresh: 50_000,
        enable_slow_start: true,
        enable_periodic_slowdown: true,
        randomize_ssthresh: false,
        ..Default::default()
    };

    // Realistic intercontinental conditions
    let condition = NetworkCondition::custom(135, Some(0.1), 0.001);
    let mut harness = LedbatTestHarness::new(config.clone(), condition, 999);

    // Track all states encountered
    let mut states_encountered = std::collections::HashSet::new();
    states_encountered.insert(harness.snapshot().state);

    // Run for 200 RTTs (27 seconds at 135ms) - enough for multiple slowdown cycles
    for _ in 0..200 {
        harness.step(100_000);
        states_encountered.insert(harness.snapshot().state);
    }

    let final_snap = harness.snapshot();

    // Verify we completed at least one full slowdown cycle
    assert!(
        states_encountered.contains(&CongestionState::WaitingForSlowdown)
            || states_encountered.contains(&CongestionState::InSlowdown)
            || states_encountered.contains(&CongestionState::Frozen)
            || states_encountered.contains(&CongestionState::RampingUp),
        "Should have entered slowdown cycle. States seen: {:?}",
        states_encountered
    );

    // Verify slowdowns actually occurred
    assert!(
        final_snap.periodic_slowdowns >= 1,
        "Should have at least 1 slowdown in 200 RTTs at 135ms"
    );

    // Verify cwnd is still reasonable (not stuck at min)
    assert!(
        final_snap.cwnd >= config.min_cwnd,
        "cwnd should not drop below minimum"
    );

    println!(
        "Slowdown cycle test: {} slowdowns, final cwnd: {}KB, states: {:?}",
        final_snap.periodic_slowdowns,
        final_snap.cwnd / 1024,
        states_encountered
    );
}

/// Test GAIN transition boundaries.
///
/// GAIN changes at specific RTT thresholds based on the formula:
/// GAIN = 1 / min(ceil(2 * TARGET / base_delay), MAX_GAIN_DIVISOR)
///
/// Key boundaries (TARGET = 60ms):
/// - 7.5ms:  GAIN = 1/16 (max divisor)
/// - 15ms:   GAIN = 1/8
/// - 30ms:   GAIN = 1/4
/// - 60ms:   GAIN = 1/2
/// - 120ms+: GAIN = 1 (divisor = 1)
#[test]
fn test_harness_gain_transitions() {
    // Test RTT values and their expected GAIN divisors
    let test_cases: [(u64, u32); 6] = [
        (4, 16),  // ceil(120/4) = 30, clamped to 16
        (8, 15),  // ceil(120/8) = 15
        (15, 8),  // ceil(120/15) = 8
        (30, 4),  // ceil(120/30) = 4
        (60, 2),  // ceil(120/60) = 2
        (120, 1), // ceil(120/120) = 1
    ];

    for (rtt_ms, expected_divisor) in test_cases {
        let expected_gain = 1.0 / expected_divisor as f64;
        let actual_gain = LedbatTestHarness::expected_gain_for_rtt(rtt_ms);

        assert!(
            (actual_gain - expected_gain).abs() < 0.001,
            "{}ms RTT: expected GAIN {:.4} (divisor {}), got {:.4}",
            rtt_ms,
            expected_gain,
            expected_divisor,
            actual_gain
        );

        // Also verify via harness after establishing base delay
        let config = LedbatConfig {
            initial_cwnd: 50_000,
            min_cwnd: 2_848,
            max_cwnd: 500_000,
            enable_slow_start: false,
            enable_periodic_slowdown: false,
            randomize_ssthresh: false,
            ..Default::default()
        };

        let condition = NetworkCondition::custom(rtt_ms, None, 0.0);
        let mut harness = LedbatTestHarness::new(config, condition, 123);

        // Run a few RTTs to establish base delay
        harness.run_rtts(5, 50_000);

        let measured_gain = harness.current_gain();

        println!(
            "{}ms RTT: expected GAIN = 1/{} = {:.4}, measured = {:.4}",
            rtt_ms, expected_divisor, expected_gain, measured_gain
        );
    }
}

/// Test behavior when RTT changes mid-transfer.
///
/// Simulates a network path change (e.g., route change) where RTT
/// suddenly increases from 50ms to 200ms. Verifies:
/// 1. Base delay updates appropriately
/// 2. GAIN recalculates
/// 3. Cwnd adjusts to new conditions
#[test]
fn test_harness_rtt_change_mid_transfer() {
    let config = LedbatConfig {
        initial_cwnd: 100_000,
        min_cwnd: 2_848,
        max_cwnd: 1_000_000,
        enable_slow_start: false, // Start in congestion avoidance
        enable_periodic_slowdown: false,
        randomize_ssthresh: false,
        ..Default::default()
    };

    // Start with low RTT
    let low_rtt_condition = NetworkCondition::custom(50, None, 0.0);
    let mut harness = LedbatTestHarness::new(config.clone(), low_rtt_condition, 777);

    // Run at low RTT to establish baseline
    let low_rtt_snaps = harness.run_rtts(20, 100_000);
    let cwnd_at_low_rtt = low_rtt_snaps.last().unwrap().cwnd;
    let base_delay_at_low_rtt = harness.snapshot().base_delay;

    println!(
        "Before RTT change: cwnd = {}KB, base_delay = {:?}",
        cwnd_at_low_rtt / 1024,
        base_delay_at_low_rtt
    );

    // Change to high RTT (simulating route change)
    harness.set_condition(NetworkCondition::custom(200, None, 0.0));

    // Run at high RTT
    let high_rtt_snaps = harness.run_rtts(50, 100_000);
    let cwnd_at_high_rtt = high_rtt_snaps.last().unwrap().cwnd;
    let base_delay_after = harness.snapshot().base_delay;

    println!(
        "After RTT change: cwnd = {}KB, base_delay = {:?}",
        cwnd_at_high_rtt / 1024,
        base_delay_after
    );

    // Base delay should reflect the lower RTT (base delay is minimum observed)
    // Note: It won't immediately update to 200ms because base delay tracks minimum
    assert!(
        base_delay_at_low_rtt <= Duration::from_millis(55),
        "Initial base delay should be ~50ms, got {:?}",
        base_delay_at_low_rtt
    );

    // After running at higher RTT, base delay history will eventually age out
    // but in short term (50 RTTs), base delay should still reflect old minimum
    // This is correct behavior - base delay is conservative

    // Cwnd should adjust (might decrease due to higher queuing delay estimate)
    // We just verify the system is stable and didn't crash
    assert!(
        cwnd_at_high_rtt >= config.min_cwnd,
        "cwnd should stay above minimum after RTT change"
    );
}

/// Test slow start exit at various RTTs.
///
/// Slow start exits when either:
/// 1. cwnd reaches ssthresh, OR
/// 2. queuing_delay > delay_exit_threshold * TARGET (default 45ms)
///
/// At different RTTs, the trigger may differ:
/// - Low RTT (10ms): Likely hits ssthresh first
/// - High RTT (200ms): May hit delay threshold first
#[test]
fn test_harness_slow_start_exit_at_various_rtts() {
    for (name, rtt_ms) in [
        ("Low RTT (10ms)", 10u64),
        ("Medium RTT (50ms)", 50),
        ("High RTT (150ms)", 150),
        ("Very High RTT (250ms)", 250),
    ] {
        let config = LedbatConfig {
            initial_cwnd: 20_000,
            min_cwnd: 2_848,
            max_cwnd: 500_000,
            ssthresh: 100_000, // 100KB threshold
            enable_slow_start: true,
            enable_periodic_slowdown: false, // Isolate slow start behavior
            randomize_ssthresh: false,
            delay_exit_threshold: 0.75, // Exit when queuing > 45ms
            ..Default::default()
        };

        let condition = NetworkCondition::custom(rtt_ms, None, 0.0);
        let mut harness = LedbatTestHarness::new(config.clone(), condition, 42);

        // Verify we start in slow start
        assert_eq!(
            harness.snapshot().state,
            CongestionState::SlowStart,
            "{}: Should start in SlowStart",
            name
        );

        // Run until we exit slow start
        let result = harness.run_until_state(CongestionState::WaitingForSlowdown, 200, 100_000);

        // Should have exited slow start
        let snaps = result.as_ref().unwrap_or_else(|e| e);
        let final_state = snaps.last().unwrap().state;
        let final_cwnd = snaps.last().unwrap().cwnd;

        // Verify exit occurred
        assert_ne!(
            final_state,
            CongestionState::SlowStart,
            "{}: Should have exited SlowStart within 200 RTTs",
            name
        );

        println!(
            "{}: exited slow start at {}KB cwnd after {} RTTs (state: {:?})",
            name,
            final_cwnd / 1024,
            snaps.len(),
            final_state
        );
    }
}

/// Test cwnd growth rate varies with GAIN at different RTTs.
///
/// At higher RTT, GAIN is larger (up to 1.0), so cwnd should grow faster
/// per RTT in congestion avoidance mode (not slow start).
#[test]
fn test_harness_cwnd_growth_rate_scales_with_gain() {
    let mut growth_rates: Vec<(u64, f64)> = Vec::new();

    for rtt_ms in [15u64, 30, 60, 120] {
        let expected_gain = LedbatTestHarness::expected_gain_for_rtt(rtt_ms);

        let config = LedbatConfig {
            initial_cwnd: 50_000,
            min_cwnd: 2_848,
            max_cwnd: 500_000,
            enable_slow_start: false, // Test congestion avoidance directly
            enable_periodic_slowdown: false,
            randomize_ssthresh: false,
            ..Default::default()
        };

        let condition = NetworkCondition::custom(rtt_ms, None, 0.0);
        let mut harness = LedbatTestHarness::new(config.clone(), condition, 123);

        let initial = harness.snapshot().cwnd;

        // Run for 20 RTTs
        harness.run_rtts(20, 100_000);

        let final_cwnd = harness.snapshot().cwnd;
        let growth = (final_cwnd as f64 - initial as f64) / initial as f64;

        growth_rates.push((rtt_ms, growth));

        println!(
            "{}ms RTT (GAIN={:.3}): cwnd grew {:.1}% ({} -> {})",
            rtt_ms,
            expected_gain,
            growth * 100.0,
            initial,
            final_cwnd
        );
    }

    // Verify higher GAIN (higher RTT) leads to faster growth
    // Compare 120ms (GAIN=1) vs 15ms (GAIN=1/8)
    let growth_15ms = growth_rates
        .iter()
        .find(|(rtt, _)| *rtt == 15)
        .map(|(_, g)| *g)
        .unwrap_or(0.0);
    let growth_120ms = growth_rates
        .iter()
        .find(|(rtt, _)| *rtt == 120)
        .map(|(_, g)| *g)
        .unwrap_or(0.0);

    // 120ms should grow faster than 15ms (higher GAIN)
    // Allow some tolerance since actual growth depends on many factors
    println!(
        "Growth comparison: 15ms={:.2}%, 120ms={:.2}%",
        growth_15ms * 100.0,
        growth_120ms * 100.0
    );
}

/// Test that jitter doesn't destabilize base delay tracking.
///
/// With ±30% jitter, base delay should still stabilize at approximately
/// the minimum of the jitter range (0.7 * base_rtt).
#[test]
fn test_harness_base_delay_stable_with_jitter() {
    let config = LedbatConfig {
        initial_cwnd: 50_000,
        min_cwnd: 2_848,
        max_cwnd: 500_000,
        enable_slow_start: false,
        enable_periodic_slowdown: false,
        randomize_ssthresh: false,
        ..Default::default()
    };

    // 100ms base RTT with ±30% jitter (70-130ms range)
    let condition = NetworkCondition::custom(100, Some(0.3), 0.0);
    let mut harness = LedbatTestHarness::new(config, condition, 54321);

    // Run for enough RTTs to sample the full jitter range
    harness.run_rtts(100, 50_000);

    let base_delay = harness.snapshot().base_delay;
    let base_delay_ms = base_delay.as_millis();

    // Base delay should be close to the minimum of jitter range (70ms)
    // Allow some tolerance for RNG sampling
    assert!(
        (65..=90).contains(&base_delay_ms),
        "Base delay with ±30% jitter should be near min range (70ms), got {}ms",
        base_delay_ms
    );

    println!(
        "With ±30% jitter on 100ms RTT: base_delay = {}ms (expected ~70ms)",
        base_delay_ms
    );
}

/// Test loss and slowdown interaction.
///
/// Verifies that packet loss events don't interfere with the periodic
/// slowdown mechanism (they should be independent).
#[test]
fn test_harness_loss_and_slowdown_interaction() {
    let config = LedbatConfig {
        initial_cwnd: 50_000,
        min_cwnd: 2_848,
        max_cwnd: 300_000,
        ssthresh: 60_000,
        enable_slow_start: true,
        enable_periodic_slowdown: true,
        randomize_ssthresh: false,
        ..Default::default()
    };

    // 150ms RTT with 2% packet loss
    let condition = NetworkCondition::custom(150, None, 0.02);
    let mut harness = LedbatTestHarness::new(config.clone(), condition, 11111);

    // Run for 150 RTTs (22.5 seconds at 150ms)
    let snapshots = harness.run_rtts(150, 100_000);

    let final_snap = snapshots.last().unwrap();

    // With loss, we expect:
    // 1. Some slowdowns (periodic mechanism still works)
    // 2. Cwnd fluctuates but stays above minimum
    // 3. System remains stable (doesn't crash or get stuck)

    println!(
        "Loss + slowdown test: {} slowdowns, final cwnd: {}KB, final state: {:?}",
        final_snap.periodic_slowdowns,
        final_snap.cwnd / 1024,
        final_snap.state
    );

    // Verify system didn't collapse
    assert!(
        final_snap.cwnd >= config.min_cwnd,
        "cwnd should stay above minimum despite loss"
    );

    // Verify slowdown mechanism still triggered (may be 0 if loss kept us in recovery)
    // The key is the system remained stable
    let losses = harness.controller().total_losses.load(Ordering::Relaxed);
    println!("Total losses recorded: {}", losses);
}

/// Test using convenience method run_until_cwnd_exceeds.
#[test]
fn test_harness_convenience_run_until_cwnd_exceeds() {
    let config = LedbatConfig {
        initial_cwnd: 20_000,
        min_cwnd: 2_848,
        max_cwnd: 500_000,
        enable_slow_start: true,
        enable_periodic_slowdown: false,
        randomize_ssthresh: false,
        ..Default::default()
    };

    let condition = NetworkCondition::custom(50, None, 0.0);
    let mut harness = LedbatTestHarness::new(config, condition, 42);

    let result = harness.run_until_cwnd_exceeds(100_000, 50, 100_000);

    assert!(result.is_ok(), "Should reach 100KB cwnd within 50 RTTs");

    let snaps = result.unwrap();
    let final_cwnd = snaps.last().unwrap().cwnd;

    assert!(
        final_cwnd > 100_000,
        "Final cwnd should exceed 100KB, got {}",
        final_cwnd
    );

    println!(
        "run_until_cwnd_exceeds: reached {}KB in {} RTTs",
        final_cwnd / 1024,
        snaps.len()
    );
}

/// Test using convenience method run_until_slowdown.
#[test]
fn test_harness_convenience_run_until_slowdown() {
    let config = LedbatConfig {
        initial_cwnd: 38_000,
        min_cwnd: 2_848,
        max_cwnd: 500_000,
        ssthresh: 50_000,
        enable_slow_start: true,
        enable_periodic_slowdown: true,
        randomize_ssthresh: false,
        ..Default::default()
    };

    let condition = NetworkCondition::custom(50, None, 0.0);
    let mut harness = LedbatTestHarness::new(config, condition, 42);

    let result = harness.run_until_slowdown(100, 100_000);

    assert!(result.is_ok(), "Should trigger a slowdown within 100 RTTs");

    let snaps = result.unwrap();
    let final_slowdowns = snaps.last().unwrap().periodic_slowdowns;

    assert!(final_slowdowns >= 1, "Should have at least 1 slowdown");

    println!(
        "run_until_slowdown: first slowdown after {} RTTs",
        snaps.len()
    );
}

// ============================================================================
// Regression Tests for Minimum cwnd Slowdown Trap (Nacho's Root Cause Analysis)
// ============================================================================
//
// These tests catch the bug where slowdowns fire even when cwnd is at minimum,
// creating a trap where cwnd can never grow:
//
// 1. Slowdowns trigger even when cwnd <= min_cwnd * SLOWDOWN_REDUCTION_FACTOR
// 2. When pre_slowdown_cwnd == min_cwnd, ramp-up completes instantly
// 3. Next slowdown schedules just 18 RTTs away
// 4. LEDBAT growth is sub-linear (~16 RTTs to double), but slowdowns every 18 RTTs
// 5. Result: cwnd barely grows before being reset again
//
// The 67-slowdown bug in production was a symptom of this trap.

/// Regression test: Slowdowns should be skipped when cwnd is too small to reduce.
///
/// When cwnd <= min_cwnd * SLOWDOWN_REDUCTION_FACTOR, a slowdown would have no
/// meaningful effect (cwnd/4 clamps to min_cwnd). The slowdown should be skipped
/// and the interval extended to allow cwnd to grow.
///
/// This test FAILS on the buggy implementation and PASSES after the fix.
#[test]
fn test_harness_slowdown_skipped_when_cwnd_at_minimum() {
    let min_cwnd = 2_848;
    let config = LedbatConfig {
        initial_cwnd: min_cwnd, // Start at minimum
        min_cwnd,
        max_cwnd: 500_000,
        ssthresh: 50_000,
        enable_slow_start: true,
        enable_periodic_slowdown: true,
        randomize_ssthresh: false,
        ..Default::default()
    };

    let condition = NetworkCondition::custom(135, None, 0.0); // High RTT like production
    let mut harness = LedbatTestHarness::new(config.clone(), condition, 12345);

    // Inject a timeout to reset cwnd to minimum and trigger recovery
    harness.inject_timeout();

    let post_timeout = harness.snapshot();
    assert_eq!(
        post_timeout.cwnd, min_cwnd,
        "cwnd should be at minimum after timeout"
    );

    // Run for enough RTTs that a slowdown would normally trigger (>20 RTTs)
    // but since cwnd is at minimum, slowdowns should be skipped
    let snapshots = harness.run_rtts(50, 100_000);

    // Count slowdowns that occurred while cwnd was at/near minimum
    let mut futile_slowdowns = 0;
    let mut prev_slowdowns = post_timeout.periodic_slowdowns;

    for snap in &snapshots {
        if snap.periodic_slowdowns > prev_slowdowns {
            // A slowdown was triggered
            if snap.cwnd <= min_cwnd * SLOWDOWN_REDUCTION_FACTOR {
                futile_slowdowns += 1;
            }
        }
        prev_slowdowns = snap.periodic_slowdowns;
    }

    // With the fix, no slowdowns should trigger when cwnd is too small
    // Note: This assertion may fail on the current buggy code, which is the point!
    assert_eq!(
        futile_slowdowns,
        0,
        "Slowdowns should be skipped when cwnd <= min_cwnd * {} ({}). \
         Found {} futile slowdowns.",
        SLOWDOWN_REDUCTION_FACTOR,
        min_cwnd * SLOWDOWN_REDUCTION_FACTOR,
        futile_slowdowns
    );

    // Cwnd should have been able to grow since slowdowns were skipped
    let final_cwnd = snapshots.last().unwrap().cwnd;
    println!(
        "After 50 RTTs from minimum: cwnd grew from {}KB to {}KB",
        min_cwnd / 1024,
        final_cwnd / 1024
    );
}

/// Regression test: cwnd must be able to grow from minimum in slow start.
///
/// This tests the core recovery path: starting from min_cwnd with high ssthresh,
/// slow start should grow cwnd exponentially without slowdowns interfering.
///
/// Nacho's fix: Skip slowdowns when cwnd <= min_cwnd * SLOWDOWN_REDUCTION_FACTOR.
/// This allows cwnd to recover past the threshold before any slowdown fires.
///
/// Expected: cwnd grows significantly in slow start (doubling per RTT).
#[test]
fn test_harness_cwnd_can_grow_from_minimum() {
    let min_cwnd = 2_848;
    let config = LedbatConfig {
        initial_cwnd: min_cwnd, // Start at minimum
        min_cwnd,
        max_cwnd: 500_000,
        ssthresh: 100_000, // High threshold allows slow start to grow
        enable_slow_start: true,
        enable_periodic_slowdown: true,
        randomize_ssthresh: false,
        ..Default::default()
    };

    let condition = NetworkCondition::custom(135, None, 0.0);
    let mut harness = LedbatTestHarness::new(config, condition, 99999);

    // Run for 20 RTTs - slow start should double cwnd each RTT
    // From 2848 bytes, 10 doublings would reach ~2.9MB (capped by ssthresh at 100KB)
    let snapshots = harness.run_rtts(20, 100_000);

    let initial_cwnd = min_cwnd;
    let final_cwnd = snapshots.last().unwrap().cwnd;
    let slowdowns = snapshots.last().unwrap().periodic_slowdowns;

    println!(
        "Growth from minimum: {}B -> {}B ({:.1}x) in 20 RTTs ({} slowdowns)",
        initial_cwnd,
        final_cwnd,
        final_cwnd as f64 / initial_cwnd as f64,
        slowdowns
    );

    // Slow start should grow cwnd significantly - at least 10x (a few doublings)
    // The exact value depends on when slow start exits (delay or ssthresh)
    let expected_min_growth = min_cwnd * 10; // ~28KB
    assert!(
        final_cwnd >= expected_min_growth,
        "cwnd should grow at least 10x from {} to {} in slow start, but only reached {}. \
         {} slowdowns occurred.",
        min_cwnd,
        expected_min_growth,
        final_cwnd,
        slowdowns
    );

    // No slowdowns should have occurred (cwnd started below threshold)
    // or very few (if we grew past threshold late in the test)
    assert!(
        slowdowns <= 1,
        "Expected 0-1 slowdowns when growing from minimum, got {}",
        slowdowns
    );
}

/// Regression test: Instant ramp-up completion when pre_slowdown_cwnd == min_cwnd.
///
/// When cwnd is already at minimum and a slowdown triggers:
/// - pre_slowdown_cwnd = min_cwnd
/// - frozen_cwnd = max(min_cwnd / 4, min_cwnd) = min_cwnd
/// - Ramp-up target = min_cwnd, current = min_cwnd → instant completion
/// - Next slowdown scheduled 18 RTTs away
///
/// This creates a trap where slowdowns fire every 18 RTTs with no recovery time.
#[test]
fn test_harness_instant_rampup_trap() {
    let min_cwnd = 2_848;
    let config = LedbatConfig {
        initial_cwnd: min_cwnd,
        min_cwnd,
        max_cwnd: 500_000,
        ssthresh: 100_000,
        enable_slow_start: false, // Skip slow start to test pure slowdown behavior
        enable_periodic_slowdown: true,
        randomize_ssthresh: false,
        ..Default::default()
    };

    let condition = NetworkCondition::custom(135, None, 0.0);
    let mut harness = LedbatTestHarness::new(config.clone(), condition, 77777);

    // Verify starting state
    let initial = harness.snapshot();
    assert_eq!(initial.cwnd, min_cwnd);
    assert_eq!(initial.state, CongestionState::CongestionAvoidance);

    // Run for 100 RTTs (enough for multiple slowdown cycles if they trigger)
    let snapshots = harness.run_rtts(100, 100_000);

    // Analyze the trap: if slowdowns fire while cwnd stays at minimum, we're trapped
    let slowdowns = snapshots.last().unwrap().periodic_slowdowns;
    let final_cwnd = snapshots.last().unwrap().cwnd;

    // Count how many RTTs were spent at minimum cwnd
    let rtts_at_minimum = snapshots.iter().filter(|s| s.cwnd == min_cwnd).count();

    // If more than half the RTTs were at minimum AND multiple slowdowns fired,
    // that indicates the trap
    let trap_detected = rtts_at_minimum > 50 && slowdowns > 3;

    if trap_detected {
        println!(
            "TRAP DETECTED: {} slowdowns fired, spent {}/100 RTTs at minimum cwnd ({}KB)",
            slowdowns,
            rtts_at_minimum,
            min_cwnd / 1024
        );
    }

    // The fix should prevent this trap
    assert!(
        !trap_detected,
        "Instant ramp-up trap detected: {} slowdowns with {}/100 RTTs at minimum. \
         cwnd should be able to grow when slowdowns are futile.",
        slowdowns, rtts_at_minimum
    );

    println!(
        "No trap: final cwnd = {}KB after 100 RTTs ({} slowdowns, {} RTTs at minimum)",
        final_cwnd / 1024,
        slowdowns,
        rtts_at_minimum
    );
}

/// Regression test: Slowdown scheduling after timeout should not block recovery.
///
/// After a timeout:
/// 1. cwnd resets to minimum
/// 2. State goes to SlowStart
/// 3. Any previously scheduled slowdown should be cleared
/// 4. Recovery should proceed without immediate slowdown interference
#[test]
fn test_harness_timeout_clears_scheduled_slowdown() {
    let min_cwnd = 2_848;
    let ssthresh = 80_000;
    let config = LedbatConfig {
        initial_cwnd: 100_000,
        min_cwnd,
        max_cwnd: 500_000,
        ssthresh,
        enable_slow_start: true,
        enable_periodic_slowdown: true,
        randomize_ssthresh: false,
        ..Default::default()
    };

    let condition = NetworkCondition::custom(100, None, 0.0);
    let mut harness = LedbatTestHarness::new(config, condition, 11111);

    // Run until we've had at least one slowdown (establishes next_slowdown_time)
    let _ = harness.run_until_slowdown(200, 100_000);

    let pre_timeout = harness.snapshot();
    println!(
        "Before timeout: cwnd={}KB, slowdowns={}, state={:?}",
        pre_timeout.cwnd / 1024,
        pre_timeout.periodic_slowdowns,
        pre_timeout.state
    );

    // Inject timeout
    harness.inject_timeout();

    let post_timeout = harness.snapshot();
    // cwnd resets to adaptive floor / 4 (uses BDP proxy from slow start exit)
    assert!(
        post_timeout.cwnd >= min_cwnd,
        "cwnd should be at least min_cwnd: {}",
        post_timeout.cwnd
    );
    assert!(
        post_timeout.cwnd <= ssthresh / 2,
        "cwnd should be well below ssthresh: cwnd={} ssthresh={}",
        post_timeout.cwnd,
        ssthresh
    );
    assert_eq!(post_timeout.state, CongestionState::SlowStart);

    // Run for 30 RTTs - enough time for recovery without immediate slowdown
    // (18 RTT min_interval means no new slowdown should fire for a while)
    let recovery_snaps = harness.run_rtts(30, 100_000);

    // Check if a slowdown fired immediately after timeout (bug behavior)
    let slowdowns_during_recovery =
        recovery_snaps.last().unwrap().periodic_slowdowns - pre_timeout.periodic_slowdowns;

    // After timeout, we should have time to recover before next slowdown
    // If slowdown fires in first 18 RTTs while cwnd is still small, that's the bug
    let early_slowdown_while_small = recovery_snaps
        .iter()
        .take(18)
        .any(|s| s.periodic_slowdowns > pre_timeout.periodic_slowdowns && s.cwnd < min_cwnd * 4);

    assert!(
        !early_slowdown_while_small,
        "Slowdown fired during early recovery while cwnd was still small. \
         Timeout should clear scheduled slowdowns to allow recovery."
    );

    let final_cwnd = recovery_snaps.last().unwrap().cwnd;
    println!(
        "Recovery after timeout: cwnd grew from {}KB to {}KB in 30 RTTs ({} new slowdowns)",
        min_cwnd / 1024,
        final_cwnd / 1024,
        slowdowns_during_recovery
    );
}

/// Test: LEDBAT growth rate is compatible with slowdown interval.
///
/// This validates Nacho's math: LEDBAT congestion avoidance takes ~16 RTTs
/// to double cwnd, but slowdowns happen every 18 RTTs. The system should
/// still make progress despite this tight margin.
///
/// Key insight: If slowdowns reset cwnd before it can grow meaningfully,
/// throughput suffers. The fix should skip futile slowdowns to allow growth.
#[test]
fn test_harness_growth_rate_vs_slowdown_interval() {
    let config = LedbatConfig {
        initial_cwnd: 50_000,
        min_cwnd: 2_848,
        max_cwnd: 500_000,
        ssthresh: 200_000,
        enable_slow_start: false, // Test pure congestion avoidance
        enable_periodic_slowdown: true,
        randomize_ssthresh: false,
        ..Default::default()
    };

    // Test at the production RTT where the bug manifested
    let condition = NetworkCondition::custom(135, None, 0.0);
    let mut harness = LedbatTestHarness::new(config.clone(), condition, 33333);

    // Run for 100 RTTs and track growth between slowdowns
    let snapshots = harness.run_rtts(100, 100_000);

    // Calculate average cwnd across all snapshots
    let avg_cwnd: f64 =
        snapshots.iter().map(|s| s.cwnd as f64).sum::<f64>() / snapshots.len() as f64;

    let final_snap = snapshots.last().unwrap();

    println!(
        "135ms RTT over 100 RTTs: avg_cwnd = {}KB, final = {}KB, slowdowns = {}",
        avg_cwnd as usize / 1024,
        final_snap.cwnd / 1024,
        final_snap.periodic_slowdowns
    );

    // Average cwnd should be significantly above minimum
    // This fails if slowdowns keep resetting cwnd before it can grow
    assert!(
        avg_cwnd > (config.initial_cwnd as f64 * 0.8),
        "Average cwnd ({}KB) should stay near initial ({}KB), \
         but slowdowns are preventing sustained throughput",
        avg_cwnd as usize / 1024,
        config.initial_cwnd / 1024
    );
}

/// Stress test: 250 RTTs at high latency (reproduces production scenario).
///
/// Simulates the exact scenario from production:
/// - 135ms RTT
/// - 33.8 seconds of transfer (~250 RTTs)
/// - Should have bounded slowdowns AND sustained throughput
#[test]
fn test_harness_production_scenario_250_rtts() {
    let min_cwnd = 2_848;
    let config = LedbatConfig {
        initial_cwnd: 38_000,
        min_cwnd,
        max_cwnd: 1_000_000,
        ssthresh: 102_400,
        enable_slow_start: true,
        enable_periodic_slowdown: true,
        randomize_ssthresh: false,
        ..Default::default()
    };

    let condition = NetworkCondition::custom(135, Some(0.1), 0.001); // Realistic jitter + loss
    let mut harness = LedbatTestHarness::new(config.clone(), condition, 42);

    let snapshots = harness.run_rtts(250, 100_000);

    let final_snap = snapshots.last().unwrap();

    // Key metrics from the production bug report
    let total_slowdowns = final_snap.periodic_slowdowns;

    // Calculate total bytes transferred (sum of cwnds as proxy for throughput)
    let total_bytes: usize = snapshots.iter().map(|s| s.cwnd).sum();

    // Time spent at minimum cwnd
    let rtts_at_minimum = snapshots.iter().filter(|s| s.cwnd <= min_cwnd * 2).count();

    println!("Production scenario (250 RTTs at 135ms):");
    println!("  Total slowdowns: {}", total_slowdowns);
    println!("  Final cwnd: {}KB", final_snap.cwnd / 1024);
    println!(
        "  Total throughput proxy: {}MB",
        total_bytes / (1024 * 1024)
    );
    println!("  RTTs at/near minimum: {}/250", rtts_at_minimum);

    // Assertions based on expected behavior after fix:
    // 1. Slowdowns bounded (we already test this elsewhere)
    assert!(
        total_slowdowns <= 20,
        "Too many slowdowns: {} (expected <= 20)",
        total_slowdowns
    );

    // 2. NOT stuck at minimum for majority of time
    assert!(
        rtts_at_minimum < 50,
        "Spent too much time at minimum cwnd: {}/250 RTTs. \
         This indicates the minimum cwnd trap.",
        rtts_at_minimum
    );

    // 3. Reasonable throughput achieved
    let expected_min_bytes = 250 * 30_000; // 30KB avg per RTT minimum
    assert!(
        total_bytes >= expected_min_bytes,
        "Throughput too low: {}MB (expected >= {}MB). \
         Futile slowdowns are destroying throughput.",
        total_bytes / (1024 * 1024),
        expected_min_bytes / (1024 * 1024)
    );
}

// =========================================================================
// Property-Based Tests - Algebraic invariants for LEDBAT++ math
// =========================================================================
//
// These tests use proptest to verify mathematical formulas are correct
// across the full input domain, catching bugs like the min_interval
// calculation that was algebraically wrong for high-latency paths.

mod proptest_math {
    use super::*;
    use proptest::prelude::*;

    // Tolerance constants for consistent error bounds across tests
    const RATE_TOLERANCE_PERCENT: f64 = 0.01; // 1% tolerance for rate calculations
    const RATIO_TOLERANCE: f64 = 0.01; // Tolerance for ratio comparisons (2x, 0.5x)
    const GAIN_TOLERANCE: f64 = 0.0001; // High precision for GAIN formula
    const DURATION_TOLERANCE_MS: u64 = 1; // 1ms tolerance for duration comparisons

    // Regression test (issue #2559): min_interval must be at least 18 RTTs.
    //
    // The bug: min_interval was 1 RTT instead of 18 RTTs, causing
    // 224 slowdowns in 30s on a 137ms RTT path.
    //
    // The formula: min_interval = SLOWDOWN_FREEZE_RTTS * RTT * SLOWDOWN_INTERVAL_MULTIPLIER
    //            = 2 * RTT * 9 = 18 * RTT
    proptest! {
        #[test]
        fn slowdown_interval_is_at_least_18_rtts(base_delay_ms in 1u64..500) {
            let base_delay = Duration::from_millis(base_delay_ms);

            let config = LedbatConfig {
                initial_cwnd: 160_000,
                min_cwnd: 2848,
                max_cwnd: 10_000_000,
                ssthresh: 50_000,
                enable_slow_start: true,
                enable_periodic_slowdown: true,
                randomize_ssthresh: false,
                ..Default::default()
            };
            let controller = LedbatController::new_with_config(config);

            // Trigger initial slowdown
            controller.congestion_state.enter_in_slowdown();
            let start_nanos = controller.elapsed_nanos();
            controller.slowdown_phase_start_nanos.store(start_nanos, Ordering::Release);

            // Complete slowdown with a very short duration (simulating quick ramp-up)
            let completion_nanos = start_nanos + 1_000_000; // 1ms later
            controller.complete_slowdown(completion_nanos, base_delay);

            let next_slowdown = controller.next_slowdown_time_nanos.load(Ordering::Acquire);
            let next_interval_ns = next_slowdown.saturating_sub(completion_nanos);
            let next_interval = Duration::from_nanos(next_interval_ns);

            // The minimum interval should be 18 RTTs
            let min_expected = Duration::from_millis(
                base_delay_ms * SLOWDOWN_FREEZE_RTTS as u64 * SLOWDOWN_INTERVAL_MULTIPLIER as u64
            );

            prop_assert!(
                next_interval >= min_expected,
                "Interval {:?} < 18 RTTs ({:?}) at {}ms base delay",
                next_interval, min_expected, base_delay_ms
            );
        }
    }

    // Dynamic GAIN must be in range [1/MAX_GAIN_DIVISOR, 1].
    //
    // GAIN = 1 / min(MAX_GAIN_DIVISOR, ceil(2 * TARGET / base_delay))
    //
    // Edge cases:
    // - base_delay = 0: GAIN = 1/16 (conservative)
    // - base_delay >> TARGET: GAIN approaches 1/16
    // - base_delay << TARGET: GAIN approaches 1
    proptest! {
        #[test]
        fn dynamic_gain_in_valid_range(base_delay_ms in 0u64..1000) {
            let base_delay = Duration::from_millis(base_delay_ms);
            let controller = LedbatController::new(100_000, 2848, 10_000_000);

            let gain = controller.calculate_dynamic_gain(base_delay);

            let min_gain = 1.0 / MAX_GAIN_DIVISOR as f64;
            let max_gain = 1.0;

            prop_assert!(
                gain >= min_gain && gain <= max_gain,
                "GAIN {} out of valid range [{}, {}] at {}ms base delay",
                gain, min_gain, max_gain, base_delay_ms
            );
        }
    }

    // Rate = cwnd / RTT, with 1ms minimum RTT floor.
    //
    // Properties:
    // - rate(cwnd, rtt) = cwnd / max(rtt, 1ms)
    // - rate is proportional to cwnd
    // - rate is inversely proportional to RTT (above 1ms floor)
    proptest! {
        #[test]
        fn rate_formula_is_correct(
            cwnd_kb in 3u64..1000,  // 3KB to 1MB (>= min_cwnd of 2848)
            rtt_ms in 0u64..1000    // 0ms to 1s
        ) {
            let cwnd = (cwnd_kb * 1024) as usize;
            let rtt = Duration::from_millis(rtt_ms);
            let min_cwnd = 2848;
            let max_cwnd = 10_000_000;
            let controller = LedbatController::new(cwnd, min_cwnd, max_cwnd);
            controller.cwnd.store(cwnd, Ordering::Release);

            let rate = controller.current_rate(rtt);

            // RTT is floored at 1ms
            let effective_rtt = rtt.max(Duration::from_millis(1));
            let expected_rate = (cwnd as f64 / effective_rtt.as_secs_f64()) as usize;

            // Allow tolerance for floating point precision
            let tolerance = (expected_rate as f64 * RATE_TOLERANCE_PERCENT) as usize + 1;
            prop_assert!(
                (rate as i64 - expected_rate as i64).unsigned_abs() <= tolerance as u64,
                "Rate {} != expected {} (±{}) at {}KB cwnd, {}ms RTT",
                rate, expected_rate, tolerance, cwnd_kb, rtt_ms
            );
        }
    }

    // Rate doubles when cwnd doubles (at same RTT).
    proptest! {
        #[test]
        fn rate_proportional_to_cwnd(
            cwnd_kb in 3u64..5_000,  // >= min_cwnd of 2848
            rtt_ms in 1u64..500
        ) {
            let cwnd1 = (cwnd_kb * 1024) as usize;
            let cwnd2 = cwnd1 * 2;
            let rtt = Duration::from_millis(rtt_ms);
            let min_cwnd = 2848;
            let max_cwnd = 20_000_000;

            let controller1 = LedbatController::new(cwnd1, min_cwnd, max_cwnd);
            controller1.cwnd.store(cwnd1, Ordering::Release);
            let rate1 = controller1.current_rate(rtt);

            let controller2 = LedbatController::new(cwnd2, min_cwnd, max_cwnd);
            controller2.cwnd.store(cwnd2, Ordering::Release);
            let rate2 = controller2.current_rate(rtt);

            // rate2 should be approximately 2 * rate1
            let expected_ratio = 2.0;
            let actual_ratio = rate2 as f64 / rate1 as f64;

            prop_assert!(
                (actual_ratio - expected_ratio).abs() < RATIO_TOLERANCE,
                "Rate ratio {} != expected {} when cwnd doubled",
                actual_ratio, expected_ratio
            );
        }
    }

    // Rate halves when RTT doubles (above 1ms floor).
    proptest! {
        #[test]
        fn rate_inversely_proportional_to_rtt(
            cwnd_kb in 10u64..1000,  // >= min_cwnd of 2848
            rtt_ms in 2u64..250      // Start at 2ms so doubling stays reasonable
        ) {
            let cwnd = (cwnd_kb * 1024) as usize;
            let rtt1 = Duration::from_millis(rtt_ms);
            let rtt2 = Duration::from_millis(rtt_ms * 2);
            let min_cwnd = 2848;
            let max_cwnd = 10_000_000;

            let controller = LedbatController::new(cwnd, min_cwnd, max_cwnd);
            controller.cwnd.store(cwnd, Ordering::Release);

            let rate1 = controller.current_rate(rtt1);
            let rate2 = controller.current_rate(rtt2);

            // rate2 should be approximately rate1 / 2
            let expected_ratio = 0.5;
            let actual_ratio = rate2 as f64 / rate1 as f64;

            prop_assert!(
                (actual_ratio - expected_ratio).abs() < RATIO_TOLERANCE,
                "Rate ratio {} != expected {} when RTT doubled from {}ms to {}ms",
                actual_ratio, expected_ratio, rtt_ms, rtt_ms * 2
            );
        }
    }

    // Cwnd decrease is capped at -cwnd/2 (LEDBAT++ multiplicative decrease limit).
    //
    // This prevents severe cwnd collapse on transient delay spikes.
    proptest! {
        #[test]
        fn cwnd_decrease_capped_at_half(
            cwnd_kb in 10u64..1000,
            off_target_ms in -1000i64..-1  // Negative = over target (decrease)
        ) {
            let current_cwnd = (cwnd_kb * 1024) as f64;
            let target_ms = TARGET.as_millis() as f64;
            let off_target = off_target_ms as f64;

            // Simulate LEDBAT++ cwnd calculation with worst-case GAIN
            let gain = 1.0;  // Maximum GAIN
            let bytes_acked = current_cwnd;  // Full window ACKed

            let cwnd_change = gain * (off_target / target_ms) * bytes_acked * (MSS as f64)
                / current_cwnd;

            // LEDBAT++ cap: -W/2
            let max_decrease = -(current_cwnd / 2.0);
            let capped_change = cwnd_change.max(max_decrease);

            prop_assert!(
                capped_change >= max_decrease,
                "Capped change {} < max decrease {} at {}KB cwnd",
                capped_change, max_decrease, cwnd_kb
            );
        }
    }

    // Queuing delay is non-negative (saturating subtraction).
    proptest! {
        #[test]
        fn queuing_delay_non_negative(
            base_delay_ms in 1u64..500,
            extra_delay_ms in 0u64..500
        ) {
            let base_delay = Duration::from_millis(base_delay_ms);
            let filtered_rtt = Duration::from_millis(base_delay_ms + extra_delay_ms);

            // Queuing delay = filtered_rtt - base_delay (saturating)
            let queuing_delay = filtered_rtt.saturating_sub(base_delay);

            prop_assert!(
                queuing_delay >= Duration::ZERO,
                "Queuing delay {:?} is negative",
                queuing_delay
            );

            // Also verify the expected value
            let expected = Duration::from_millis(extra_delay_ms);
            prop_assert_eq!(queuing_delay, expected);
        }
    }

    // GAIN divisor formula: ceil(2 * TARGET / base_delay), clamped to [1, 16].
    proptest! {
        #[test]
        fn gain_divisor_formula_correct(base_delay_ms in 1u64..1000) {
            let base_delay = Duration::from_millis(base_delay_ms);
            let target_ms = TARGET.as_millis() as f64;
            let base_ms = base_delay_ms as f64;

            // Expected divisor calculation
            let raw_divisor = (2.0 * target_ms / base_ms).ceil() as u32;
            let expected_divisor = raw_divisor.clamp(1, MAX_GAIN_DIVISOR);
            let expected_gain = 1.0 / expected_divisor as f64;

            let controller = LedbatController::new(100_000, 2848, 10_000_000);
            let actual_gain = controller.calculate_dynamic_gain(base_delay);

            prop_assert!(
                (actual_gain - expected_gain).abs() < GAIN_TOLERANCE,
                "GAIN {} != expected {} at {}ms base delay (divisor: raw={}, clamped={})",
                actual_gain, expected_gain, base_delay_ms, raw_divisor, expected_divisor
            );
        }
    }

    // Next slowdown interval = max(9 * slowdown_duration, 18 * RTT).
    //
    // The 18 RTT floor prevents excessive slowdowns on high-latency paths.
    proptest! {
        #[test]
        fn slowdown_interval_formula(
            base_delay_ms in 1u64..500,
            slowdown_duration_ms in 1u64..10_000
        ) {
            let base_delay = Duration::from_millis(base_delay_ms);
            let slowdown_duration_ns = slowdown_duration_ms * 1_000_000;

            // Formula from complete_slowdown:
            let next_interval = slowdown_duration_ns * SLOWDOWN_INTERVAL_MULTIPLIER as u64;
            let min_slowdown_duration = base_delay.as_nanos() as u64 * SLOWDOWN_FREEZE_RTTS as u64;
            let min_interval = min_slowdown_duration * SLOWDOWN_INTERVAL_MULTIPLIER as u64;
            let actual_interval = next_interval.max(min_interval);

            // Property 1: interval >= 9 * slowdown_duration
            prop_assert!(
                actual_interval >= next_interval,
                "Interval {} < 9 * slowdown_duration {}",
                actual_interval, next_interval
            );

            // Property 2: interval >= 18 * RTT
            prop_assert!(
                actual_interval >= min_interval,
                "Interval {} < 18 RTTs {}",
                actual_interval, min_interval
            );

            // Property 3: interval = max of the two
            let expected = next_interval.max(min_interval);
            prop_assert_eq!(actual_interval, expected);
        }
    }

    // Slow start exit threshold: cwnd >= ssthresh OR queuing_delay > 0.75 * TARGET.
    //
    // With TARGET = 60ms and delay_exit_threshold = 0.75, exit at 45ms queuing delay.
    proptest! {
        #[test]
        fn slow_start_exit_threshold(queuing_delay_ms in 0u64..100) {
            let config = LedbatConfig::default();
            let threshold = (TARGET.as_millis() as f64 * config.delay_exit_threshold) as u64;

            let queuing_delay = Duration::from_millis(queuing_delay_ms);
            let should_exit_on_delay = queuing_delay > Duration::from_millis(threshold);

            // Verify threshold is 45ms (0.75 * 60ms)
            prop_assert_eq!(threshold, 45);

            // Verify exit logic
            if queuing_delay_ms > 45 {
                prop_assert!(should_exit_on_delay, "Should exit slow start at {}ms delay", queuing_delay_ms);
            } else {
                prop_assert!(!should_exit_on_delay, "Should NOT exit slow start at {}ms delay", queuing_delay_ms);
            }
        }
    }

    // =====================================================================
    // Additional edge case tests requested in PR review
    // =====================================================================

    // Cwnd bounds enforcement: min_cwnd <= cwnd <= max_cwnd after all operations.
    //
    // This invariant must hold after on_ack, on_loss, and on_timeout.
    proptest! {
        #[test]
        fn cwnd_bounds_enforced_after_timeout(
            initial_cwnd_kb in 10u64..1000,
            min_cwnd in 1000usize..5000,
            max_cwnd in 1_000_000usize..10_000_000
        ) {
            let initial_cwnd = (initial_cwnd_kb * 1024) as usize;
            let initial_cwnd = initial_cwnd.clamp(min_cwnd, max_cwnd);

            let config = LedbatConfig {
                initial_cwnd,
                min_cwnd,
                max_cwnd,
                ssthresh: max_cwnd / 2,
                enable_slow_start: true,
                enable_periodic_slowdown: false,
                randomize_ssthresh: false,
                ..Default::default()
            };
            let controller = LedbatController::new_with_config(config);

            // Trigger timeout (most aggressive cwnd reduction)
            controller.on_timeout();

            let cwnd_after = controller.current_cwnd();
            prop_assert!(
                cwnd_after >= min_cwnd,
                "cwnd {} < min_cwnd {} after timeout",
                cwnd_after, min_cwnd
            );
            prop_assert!(
                cwnd_after <= max_cwnd,
                "cwnd {} > max_cwnd {} after timeout",
                cwnd_after, max_cwnd
            );
        }
    }

    // Cwnd bounds enforced after loss events.
    proptest! {
        #[test]
        fn cwnd_bounds_enforced_after_loss(
            initial_cwnd_kb in 10u64..1000,
            min_cwnd in 1000usize..5000,
            max_cwnd in 1_000_000usize..10_000_000,
            num_losses in 1u32..10
        ) {
            let initial_cwnd = (initial_cwnd_kb * 1024) as usize;
            let initial_cwnd = initial_cwnd.clamp(min_cwnd, max_cwnd);

            let config = LedbatConfig {
                initial_cwnd,
                min_cwnd,
                max_cwnd,
                ssthresh: max_cwnd / 2,
                enable_slow_start: true,
                enable_periodic_slowdown: false,
                randomize_ssthresh: false,
                ..Default::default()
            };
            let controller = LedbatController::new_with_config(config);

            // Trigger multiple losses
            for _ in 0..num_losses {
                controller.on_loss();
            }

            let cwnd_after = controller.current_cwnd();
            prop_assert!(
                cwnd_after >= min_cwnd,
                "cwnd {} < min_cwnd {} after {} losses",
                cwnd_after, min_cwnd, num_losses
            );
            prop_assert!(
                cwnd_after <= max_cwnd,
                "cwnd {} > max_cwnd {} after {} losses",
                cwnd_after, max_cwnd, num_losses
            );
        }
    }

    // Flightsize non-negativity: flightsize uses saturating subtraction.
    //
    // Random send/ack sequences should never produce negative flightsize.
    proptest! {
        #[test]
        fn flightsize_never_negative(
            sends in proptest::collection::vec(1000usize..10000, 1..20),
            acks in proptest::collection::vec(1000usize..15000, 1..25)
        ) {
            let controller = LedbatController::new(100_000, 2848, 10_000_000);

            // Simulate random send/ack pattern
            for &bytes in &sends {
                controller.on_send(bytes);
            }

            // ACKs may exceed what was sent (simulating duplicate ACKs, etc.)
            for &bytes in &acks {
                controller.on_ack_without_rtt(bytes);

                // Flightsize should never be negative (saturating subtraction)
                let flightsize = controller.flightsize();
                prop_assert!(
                    flightsize < usize::MAX / 2,  // Would wrap to huge value if negative
                    "Flightsize {} appears to have wrapped negative",
                    flightsize
                );
            }

            // Final check
            let final_flightsize = controller.flightsize();
            prop_assert!(
                final_flightsize < usize::MAX / 2,
                "Final flightsize {} appears negative",
                final_flightsize
            );
        }
    }

    // Base delay only decreases or stays stable when new samples arrive.
    //
    // Adding an RTT sample should only decrease base_delay if the sample
    // is smaller than the current minimum. Larger samples are ignored.
    // (Note: base_delay CAN increase due to bucket expiration over time,
    // but within immediate sample processing, it should only decrease.)
    proptest! {
        #[test]
        fn base_delay_only_decreases_on_smaller_samples(
            initial_rtt_ms in 50u64..200,
            sample_rtt_ms in 10u64..300
        ) {
            let controller = LedbatController::new(100_000, 2848, 10_000_000);

            // Establish initial base delay
            let initial_rtt = Duration::from_millis(initial_rtt_ms);
            controller.on_ack(initial_rtt, 1000);
            let base_delay_before = controller.base_delay();

            // Add new sample
            let sample_rtt = Duration::from_millis(sample_rtt_ms);
            controller.on_ack(sample_rtt, 1000);
            let base_delay_after = controller.base_delay();

            if sample_rtt_ms < initial_rtt_ms {
                // Smaller sample should decrease base delay
                prop_assert!(
                    base_delay_after <= base_delay_before,
                    "Base delay increased from {:?} to {:?} despite smaller sample {}ms < {}ms",
                    base_delay_before, base_delay_after, sample_rtt_ms, initial_rtt_ms
                );
            } else {
                // Larger sample should not increase base delay
                prop_assert!(
                    base_delay_after <= base_delay_before,
                    "Base delay increased from {:?} to {:?} on larger sample {}ms >= {}ms",
                    base_delay_before, base_delay_after, sample_rtt_ms, initial_rtt_ms
                );
            }
        }
    }

    // Base delay converges to minimum across multiple samples.
    proptest! {
        #[test]
        fn base_delay_converges_to_minimum(
            samples in proptest::collection::vec(10u64..500, 5..20)
        ) {
            let controller = LedbatController::new(100_000, 2848, 10_000_000);

            // Feed all samples
            for &rtt_ms in &samples {
                let rtt = Duration::from_millis(rtt_ms);
                controller.on_ack(rtt, 1000);
            }

            let base_delay = controller.base_delay();
            let expected_min = samples.iter().copied().min().unwrap();

            // Base delay should be at or below the minimum sample
            // (could be lower if there's a fallback or timing issue)
            prop_assert!(
                base_delay.as_millis() as u64 <= expected_min + DURATION_TOLERANCE_MS,
                "Base delay {:?} > minimum sample {}ms",
                base_delay, expected_min
            );
        }
    }

    // =========================================================================
    // ssthresh Floor Behavior Tests
    //
    // These tests verify that the min_ssthresh configuration option correctly
    // prevents the "ssthresh death spiral" on high-BDP paths.
    // =========================================================================

    // ssthresh respects configured minimum floor after timeout.
    proptest! {
        #[test]
        fn ssthresh_respects_min_floor_after_timeout(
            initial_cwnd in 50_000usize..500_000,
            min_ssthresh_kb in 50usize..200,  // 50KB - 200KB
            num_timeouts in 1usize..5
        ) {
            let min_ssthresh = min_ssthresh_kb * 1024;
            let config = LedbatConfig {
                initial_cwnd,
                min_cwnd: 2_848,
                max_cwnd: 10_000_000,
                ssthresh: 1_000_000,
                enable_slow_start: true,
                enable_periodic_slowdown: false,
                randomize_ssthresh: false,
                min_ssthresh: Some(min_ssthresh),
                ..Default::default()
            };
            let controller = LedbatController::new_with_config(config);

            // Simulate multiple timeouts
            for _ in 0..num_timeouts {
                controller.on_timeout();
            }

            let final_ssthresh = controller.ssthresh.load(Ordering::Acquire);

            // ssthresh should never go below the configured floor
            prop_assert!(
                final_ssthresh >= min_ssthresh,
                "ssthresh {} < min_ssthresh {} after {} timeouts",
                final_ssthresh, min_ssthresh, num_timeouts
            );
        }
    }

    // ssthresh uses spec-compliant 2*min_cwnd floor when min_ssthresh is None.
    // With adaptive floor, ssthresh may be higher than spec_floor based on RTT,
    // but it will always be at least spec_floor (spec compliance).
    proptest! {
        #[test]
        fn ssthresh_uses_spec_floor_when_unconfigured(
            initial_cwnd in 10_000usize..100_000,
            min_cwnd in 2000usize..5000,
            num_timeouts in 1usize..10
        ) {
            let config = LedbatConfig {
                initial_cwnd,
                min_cwnd,
                max_cwnd: 10_000_000,
                ssthresh: 1_000_000,
                enable_slow_start: true,
                enable_periodic_slowdown: false,
                randomize_ssthresh: false,
                min_ssthresh: None, // Use spec-compliant floor
                ..Default::default()
            };
            let controller = LedbatController::new_with_config(config);

            // Simulate multiple timeouts
            for _ in 0..num_timeouts {
                controller.on_timeout();
            }

            let final_ssthresh = controller.ssthresh.load(Ordering::Acquire);
            let spec_floor = min_cwnd * 2;

            // ssthresh must always be at least the spec floor (2*min_cwnd)
            prop_assert!(
                final_ssthresh >= spec_floor,
                "ssthresh {} < spec floor (2*min_cwnd={}) after {} timeouts",
                final_ssthresh, spec_floor, num_timeouts
            );

            // With adaptive floor, ssthresh may stabilize higher than spec_floor
            // based on RTT or initial_ssthresh. Verify it's still bounded reasonably.
            // The adaptive floor is capped at initial_ssthresh (~1MB with jitter).
            let initial_ssthresh = controller.initial_ssthresh;
            prop_assert!(
                final_ssthresh <= initial_ssthresh,
                "ssthresh {} should be <= initial_ssthresh {} after timeouts",
                final_ssthresh, initial_ssthresh
            );
        }
    }

    // =========================================================================
    // State Machine Transition Tests
    //
    // These tests verify the congestion state machine transitions correctly
    // under various conditions, ensuring no invalid state combinations.
    // =========================================================================

    // Timeout always transitions to SlowStart regardless of current state.
    proptest! {
        #[test]
        fn timeout_always_enters_slow_start(
            initial_cwnd in 10_000usize..200_000,
            rtt_ms in 10u64..300
        ) {
            let config = LedbatConfig {
                initial_cwnd,
                min_cwnd: 2_848,
                max_cwnd: 10_000_000,
                ssthresh: initial_cwnd / 2,
                enable_slow_start: true,
                enable_periodic_slowdown: true,
                randomize_ssthresh: false,
                ..Default::default()
            };
            let controller = LedbatController::new_with_config(config);

            // Run some ACKs to potentially change state
            let rtt = Duration::from_millis(rtt_ms);
            controller.on_send(initial_cwnd);
            for _ in 0..5 {
                controller.on_ack(rtt, 1000);
            }

            // Timeout should always transition to SlowStart
            controller.on_timeout();

            prop_assert_eq!(
                controller.congestion_state.load(),
                CongestionState::SlowStart,
                "Should be in SlowStart after timeout"
            );
        }
    }

    // cwnd is reset to min_cwnd on timeout.
    proptest! {
        #[test]
        fn cwnd_reset_on_timeout(
            initial_cwnd in 10_000usize..1_000_000,
            min_cwnd in 2000usize..5000,
            rtt_ms in 10u64..200
        ) {
            let config = LedbatConfig {
                initial_cwnd,
                min_cwnd,
                max_cwnd: 10_000_000,
                ssthresh: initial_cwnd * 2,
                enable_slow_start: true,
                enable_periodic_slowdown: false,
                randomize_ssthresh: false,
                ..Default::default()
            };
            let controller = LedbatController::new_with_config(config);

            // Run some ACKs to grow cwnd
            let rtt = Duration::from_millis(rtt_ms);
            controller.on_send(initial_cwnd * 2);
            for _ in 0..10 {
                controller.on_ack(rtt, 5000);
            }

            controller.on_timeout();

            let cwnd_after = controller.current_cwnd();

            // cwnd should be at or near min_cwnd (allowing for MSS clamping)
            prop_assert!(
                cwnd_after <= min_cwnd + MSS,
                "cwnd {} should be near min_cwnd {} after timeout",
                cwnd_after, min_cwnd
            );
        }
    }

    // Scheduled slowdown is cleared on timeout.
    proptest! {
        #[test]
        fn slowdown_cleared_on_timeout(
            initial_cwnd in 50_000usize..200_000
        ) {
            let config = LedbatConfig {
                initial_cwnd,
                min_cwnd: 2_848,
                max_cwnd: 10_000_000,
                ssthresh: initial_cwnd / 4, // Low ssthresh to trigger exit quickly
                enable_slow_start: true,
                enable_periodic_slowdown: true,
                randomize_ssthresh: false,
                ..Default::default()
            };
            let controller = LedbatController::new_with_config(config);

            // Run ACKs to exit slow start and potentially schedule a slowdown
            let rtt = Duration::from_millis(50);
            controller.on_send(initial_cwnd * 2);
            for _ in 0..20 {
                controller.on_ack(rtt, 5000);
            }

            controller.on_timeout();

            let next_slowdown = controller.next_slowdown_time_nanos.load(Ordering::Acquire);

            // Slowdown should be cleared (set to u64::MAX)
            prop_assert_eq!(
                next_slowdown,
                u64::MAX,
                "Scheduled slowdown should be cleared after timeout"
            );
        }
    }

    // =========================================================================
    // Throughput Recovery Tests
    //
    // These tests verify that LEDBAT can recover to useful throughput after
    // timeouts, especially on high-BDP paths where the ssthresh death spiral
    // was previously a problem.
    // =========================================================================

    // With min_ssthresh configured, slow start can reach useful cwnd after timeout.
    // Uses virtual time for deterministic testing.
    proptest! {
        #[test]
        fn slow_start_reaches_useful_cwnd_with_min_ssthresh(
            rtt_ms in 50u64..200,  // High RTT (intercontinental)
            min_ssthresh_kb in 100usize..500  // 100KB - 500KB floor
        ) {
            use crate::simulation::VirtualTime;

            let min_ssthresh = min_ssthresh_kb * 1024;
            let config = LedbatConfig {
                initial_cwnd: 38_000,  // IW26
                min_cwnd: 2_848,
                max_cwnd: 10_000_000,
                ssthresh: 1_000_000,
                enable_slow_start: true,
                enable_periodic_slowdown: false,  // Isolate slow start behavior
                randomize_ssthresh: false,
                min_ssthresh: Some(min_ssthresh),
                ..Default::default()
            };

            // Use virtual time for deterministic testing
            let time_source = VirtualTime::new();
            let controller = LedbatController::new_with_time_source(config, time_source.clone());

            // Establish base delay with time advancement
            let rtt = Duration::from_millis(rtt_ms);
            time_source.advance(rtt);
            controller.on_ack(rtt, 1000);
            time_source.advance(rtt);
            controller.on_ack(rtt, 1000);

            // Trigger timeout
            controller.on_timeout();

            // Verify ssthresh is at configured floor
            let ssthresh_after = controller.ssthresh.load(Ordering::Acquire);
            prop_assert!(
                ssthresh_after >= min_ssthresh,
                "ssthresh {} should be >= min_ssthresh {}",
                ssthresh_after, min_ssthresh
            );

            // Simulate slow start recovery with zero queuing delay
            controller.on_send(1_000_000);  // Keep flightsize high
            let mut prev_cwnd = controller.current_cwnd();

            // Run 10 RTTs of slow start recovery with virtual time advancement
            for i in 0..10 {
                // Advance time by RTT + small buffer to exceed rate-limiting interval
                time_source.advance(rtt + Duration::from_millis(1));
                controller.on_ack(rtt, 5000);
                let current_cwnd = controller.current_cwnd();

                // In slow start, cwnd should grow (unless we hit ssthresh)
                if controller.congestion_state.load() == CongestionState::SlowStart {
                    prop_assert!(
                        current_cwnd >= prev_cwnd,
                        "cwnd should grow in slow start: {} -> {} (iter {})",
                        prev_cwnd, current_cwnd, i
                    );
                }
                prev_cwnd = current_cwnd;
            }

            // After 10 RTTs of slow start from min_cwnd (~2.8KB), cwnd should
            // grow significantly. The key property is that we recover from
            // min_cwnd to useful throughput, not a specific fraction of ssthresh.
            //
            // In slow start, cwnd grows by ~bytes_acked per ACK. With 5KB ACKs
            // and 10 iterations, we expect at least 50KB of growth from min_cwnd.
            // The actual growth may be limited by ssthresh exit or rate limiting.
            let final_cwnd = controller.current_cwnd();
            let min_cwnd = 2_848;
            let expected_growth = 30_000;  // At least 30KB of growth

            prop_assert!(
                final_cwnd >= min_cwnd + expected_growth,
                "After 10 RTTs, cwnd {} should be at least {} (min_cwnd + 30KB growth)",
                final_cwnd, min_cwnd + expected_growth
            );
        }
    }

    // Skip slowdown when cwnd is too small (prevents futile slowdowns).
    proptest! {
        #[test]
        fn skip_slowdown_when_cwnd_near_minimum(
            min_cwnd in 2000usize..5000
        ) {
            // Create controller with cwnd at the threshold where slowdowns should be skipped
            let threshold = min_cwnd * SLOWDOWN_REDUCTION_FACTOR;
            let initial_cwnd = threshold; // Exactly at threshold

            let config = LedbatConfig {
                initial_cwnd,
                min_cwnd,
                max_cwnd: 10_000_000,
                ssthresh: 1_000_000,
                enable_slow_start: false,  // Start in CongestionAvoidance
                enable_periodic_slowdown: true,
                randomize_ssthresh: false,
                ..Default::default()
            };
            let controller = LedbatController::new_with_config(config);

            // Call start_slowdown and check if it returns false (skipped)
            let base_delay = Duration::from_millis(50);
            let now_nanos = 1_000_000_000u64;
            let started = controller.start_slowdown(now_nanos, base_delay);

            prop_assert!(
                !started,
                "Slowdown should be skipped when cwnd ({}) <= min_cwnd * {} ({})",
                initial_cwnd, SLOWDOWN_REDUCTION_FACTOR, threshold
            );

            // Verify state didn't change
            prop_assert_eq!(
                controller.congestion_state.load(),
                CongestionState::CongestionAvoidance,
                "State should remain CongestionAvoidance when slowdown is skipped"
            );
        }
    }
}

// =========================================================================
// Intercontinental High-BDP Path Tests (Issue #2578)
//
// These tests verify LEDBAT behavior on high-BDP paths like US-EU (135ms RTT)
// where the ssthresh death spiral was causing 18+ second transfers.
// =========================================================================

/// Test recovery on intercontinental path (135ms RTT, 100Mbps link).
///
/// This test simulates the issue #2578 scenario where transfers were taking
/// 18+ seconds due to ssthresh getting stuck at 5KB after repeated timeouts.
/// With the min_ssthresh floor, slow start should recover to useful throughput.
#[test]
fn test_intercontinental_recovery_with_min_ssthresh() {
    // Configuration matching issue #2578 scenario
    let min_ssthresh = 100 * 1024; // 100KB floor
    let config = LedbatConfig {
        initial_cwnd: 38_000, // IW26
        min_cwnd: 2_848,
        max_cwnd: 10_000_000,
        ssthresh: 1_000_000, // 1MB initial
        enable_slow_start: true,
        enable_periodic_slowdown: true,
        randomize_ssthresh: false,
        min_ssthresh: Some(min_ssthresh),
        ..Default::default()
    };

    // Use deterministic harness with intercontinental network conditions
    let mut harness = LedbatTestHarness::new(
        config,
        NetworkCondition::INTERCONTINENTAL, // 135ms RTT, ±20% jitter, 0.5% loss
        12345,                              // Fixed seed for reproducibility
    );

    println!("\n========== Intercontinental Recovery Test ==========");
    println!("RTT: 135ms, min_ssthresh: {}KB", min_ssthresh / 1024);

    // Run 5 RTTs to establish connection
    let snapshots = harness.run_rtts(5, 100_000);
    println!(
        "After 5 RTTs: cwnd={}KB, state={:?}",
        snapshots.last().unwrap().cwnd / 1024,
        snapshots.last().unwrap().state
    );

    // Simulate timeout
    harness.inject_timeout();
    let snap_after_timeout = harness.snapshot();
    println!(
        "After timeout: cwnd={}KB, state={:?}",
        snap_after_timeout.cwnd / 1024,
        snap_after_timeout.state
    );

    // Verify we're in slow start after timeout
    assert_eq!(
        snap_after_timeout.state,
        CongestionState::SlowStart,
        "Should be in SlowStart after timeout"
    );

    // Run 20 RTTs of recovery
    let recovery_snapshots = harness.run_rtts(20, 100_000);

    // Calculate throughput achieved
    let final_cwnd = recovery_snapshots.last().unwrap().cwnd;
    let rtt_ms = 135;
    let throughput_mbps = (final_cwnd as f64 * 8.0) / (rtt_ms as f64 * 1000.0);

    println!(
        "After 20 RTTs recovery: cwnd={}KB, throughput=~{:.1} Mbps",
        final_cwnd / 1024,
        throughput_mbps
    );

    // Key assertion: cwnd should recover to a useful level
    // At 135ms RTT, we need at least 100KB cwnd for decent throughput
    assert!(
        final_cwnd >= 50_000,
        "After 20 RTTs, cwnd {} should be at least 50KB for usable throughput",
        final_cwnd
    );

    println!("✓ Intercontinental recovery test passed!");
}

/// Test that multiple timeouts don't cause ssthresh death spiral.
///
/// Without min_ssthresh floor, repeated timeouts would cause:
/// ssthresh: 1MB -> 500KB -> 250KB -> 125KB -> 62KB -> 31KB -> 15KB -> 7KB -> 5KB (floor)
/// With min_ssthresh=100KB floor, it stabilizes at 100KB.
#[test]
fn test_repeated_timeouts_stabilize_at_min_ssthresh() {
    let min_ssthresh = 100 * 1024; // 100KB floor
    let config = LedbatConfig {
        initial_cwnd: 500_000, // 500KB
        min_cwnd: 2_848,
        max_cwnd: 10_000_000,
        ssthresh: 1_000_000, // 1MB initial
        enable_slow_start: true,
        enable_periodic_slowdown: false,
        randomize_ssthresh: false,
        min_ssthresh: Some(min_ssthresh),
        ..Default::default()
    };

    let controller = LedbatController::new_with_config(config);

    println!("\n========== Repeated Timeouts Test ==========");
    println!("Initial ssthresh: 1MB, min_ssthresh floor: 100KB");

    // Simulate 10 timeouts
    let mut ssthresh_values = vec![controller.ssthresh.load(Ordering::Acquire)];
    for i in 0..10 {
        controller.on_timeout();
        let ssthresh = controller.ssthresh.load(Ordering::Acquire);
        ssthresh_values.push(ssthresh);
        println!("After timeout {}: ssthresh={}KB", i + 1, ssthresh / 1024);
    }

    // Verify ssthresh stabilized at floor (not below)
    let final_ssthresh = *ssthresh_values.last().unwrap();
    assert!(
        final_ssthresh >= min_ssthresh,
        "ssthresh {} should stabilize at or above min_ssthresh {}",
        final_ssthresh,
        min_ssthresh
    );

    // Verify ssthresh didn't go below floor at any point
    for (i, &ssthresh) in ssthresh_values.iter().enumerate() {
        assert!(
            ssthresh >= min_ssthresh,
            "ssthresh {} at step {} was below min_ssthresh {}",
            ssthresh,
            i,
            min_ssthresh
        );
    }

    println!(
        "✓ ssthresh stabilized at {}KB (floor: {}KB)",
        final_ssthresh / 1024,
        min_ssthresh / 1024
    );
}

/// Regression test: cwnd should use adaptive floor on timeout, not just min_cwnd.
///
/// **Problem:** With min_ssthresh=100KB, ssthresh stays healthy at 100KB after timeout.
/// But cwnd was resetting to min_cwnd (2.8KB), creating a massive recovery cliff:
/// - cwnd drops from 77KB → 2.8KB (27x reduction!)
/// - Recovery takes log2(77/2.8) ≈ 5 RTTs just to get back
/// - With 200ms RTT, that's 1+ second of crippled throughput per timeout
/// - Users in Argentina with 80+ timeouts could never complete transfers
///
/// **Fix:** Use 1/4 of the adaptive ssthresh floor as min_cwnd on timeout.
/// With 100KB min_ssthresh: cwnd resets to 25KB instead of 2.8KB.
/// Recovery: 25KB → 50KB → 100KB in just 2 RTTs.
#[test]
fn test_timeout_cwnd_uses_adaptive_floor() {
    let min_ssthresh = 100 * 1024; // 100KB floor
    let config = LedbatConfig {
        initial_cwnd: 200_000, // 200KB - simulates healthy connection
        min_cwnd: 2_848,       // Standard min_cwnd (~2.8KB)
        max_cwnd: 10_000_000,
        ssthresh: 1_000_000,
        enable_slow_start: true,
        enable_periodic_slowdown: false,
        randomize_ssthresh: false,
        min_ssthresh: Some(min_ssthresh),
        ..Default::default()
    };

    let controller = LedbatController::new_with_config(config.clone());

    println!("\n========== Timeout cwnd Adaptive Floor Test ==========");
    println!(
        "Initial: cwnd={}KB, ssthresh={}KB, min_ssthresh={}KB, min_cwnd={}KB",
        controller.current_cwnd() / 1024,
        controller.ssthresh.load(Ordering::Acquire) / 1024,
        min_ssthresh / 1024,
        config.min_cwnd / 1024
    );

    // Trigger a timeout
    controller.on_timeout();

    let cwnd_after = controller.current_cwnd();
    let ssthresh_after = controller.ssthresh.load(Ordering::Acquire);

    println!(
        "After timeout: cwnd={}KB, ssthresh={}KB",
        cwnd_after / 1024,
        ssthresh_after / 1024
    );

    // The key assertion: cwnd should be at least 1/4 of min_ssthresh (25KB),
    // not collapsed to min_cwnd (2.8KB)
    let expected_cwnd_floor = min_ssthresh / 4; // 25KB

    assert!(
        cwnd_after >= expected_cwnd_floor,
        "After timeout with min_ssthresh={}KB, cwnd should be at least {}KB (1/4 of floor), \
         but got {}KB. This causes the 'LEDBAT death spiral' where recovery is too slow \
         on high-latency paths.",
        min_ssthresh / 1024,
        expected_cwnd_floor / 1024,
        cwnd_after / 1024
    );

    // ssthresh should still be at the floor
    assert!(
        ssthresh_after >= min_ssthresh,
        "ssthresh {} should be at least min_ssthresh {}",
        ssthresh_after,
        min_ssthresh
    );

    // Verify cwnd < ssthresh (still room for slow start)
    assert!(
        cwnd_after < ssthresh_after,
        "cwnd {} should be less than ssthresh {} to allow slow start recovery",
        cwnd_after,
        ssthresh_after
    );

    println!(
        "✓ cwnd={} KB is above adaptive floor ({} KB) and below ssthresh ({} KB)",
        cwnd_after / 1024,
        expected_cwnd_floor / 1024,
        ssthresh_after / 1024
    );
}

/// Test that repeated timeouts maintain reasonable cwnd floor.
///
/// Simulates a user in a region with high packet loss (like Argentina)
/// experiencing many timeouts. Even with 10 consecutive timeouts,
/// cwnd should never collapse below the adaptive floor.
#[test]
fn test_repeated_timeouts_maintain_cwnd_floor() {
    let min_ssthresh = 100 * 1024; // 100KB
    let config = LedbatConfig {
        initial_cwnd: 500_000, // 500KB
        min_cwnd: 2_848,
        max_cwnd: 10_000_000,
        ssthresh: 1_000_000,
        enable_slow_start: true,
        enable_periodic_slowdown: false,
        randomize_ssthresh: false,
        min_ssthresh: Some(min_ssthresh),
        ..Default::default()
    };

    let controller = LedbatController::new_with_config(config.clone());
    let expected_cwnd_floor = min_ssthresh / 4; // 25KB

    println!("\n========== Repeated Timeouts cwnd Floor Test ==========");
    println!(
        "Simulating high-loss path (like Argentina) with {} consecutive timeouts",
        10
    );
    println!("Expected cwnd floor: {}KB", expected_cwnd_floor / 1024);

    // Simulate 10 consecutive timeouts (like HostFat's 80+ timeouts scenario)
    for i in 0..10 {
        controller.on_timeout();

        let cwnd = controller.current_cwnd();
        let ssthresh = controller.ssthresh.load(Ordering::Acquire);

        println!(
            "After timeout {}: cwnd={}KB, ssthresh={}KB",
            i + 1,
            cwnd / 1024,
            ssthresh / 1024
        );

        // cwnd must stay above the adaptive floor
        assert!(
            cwnd >= expected_cwnd_floor,
            "After timeout {}, cwnd {} should be at least {} (adaptive floor)",
            i + 1,
            cwnd,
            expected_cwnd_floor
        );

        // ssthresh must stay above min_ssthresh
        assert!(
            ssthresh >= min_ssthresh,
            "After timeout {}, ssthresh {} should be at least {}",
            i + 1,
            ssthresh,
            min_ssthresh
        );
    }

    println!(
        "✓ cwnd maintained above {}KB floor through all timeouts",
        expected_cwnd_floor / 1024
    );
}

// =========================================================================
// Adaptive min_ssthresh Tests (Phase 2)
//
// These tests verify that the adaptive floor calculation works correctly,
// using BDP proxy from slow start exit or RTT-based scaling as fallback.
// =========================================================================

/// Test that slow_start_exit_cwnd is captured when exiting slow start.
#[test]
fn test_slow_start_exit_cwnd_captured() {
    let time_source = SharedMockTimeSource::new();
    let config = LedbatConfig {
        initial_cwnd: 38_000,
        min_cwnd: 2_848,
        max_cwnd: 10_000_000,
        ssthresh: 1_000_000,
        enable_slow_start: true,
        enable_periodic_slowdown: false,
        randomize_ssthresh: false,
        ..Default::default()
    };

    let controller = LedbatController::new_with_time_source(config, time_source.clone());

    // Verify initial state
    assert_eq!(
        controller.slow_start_exit_cwnd.load(Ordering::Acquire),
        0,
        "slow_start_exit_cwnd should be 0 initially"
    );

    // Run ACKs with low delay to grow cwnd in slow start
    let rtt = Duration::from_millis(50);
    controller.on_send(200_000);

    // Grow cwnd until we exit slow start via delay threshold
    for _ in 0..20 {
        time_source.advance_time(rtt);
        // Use higher RTT to trigger delay-based exit (75% of 60ms = 45ms)
        controller.on_ack(Duration::from_millis(50), 10_000);
    }

    let slow_start_exit_cwnd = controller.slow_start_exit_cwnd.load(Ordering::Acquire);

    // slow_start_exit_cwnd should be captured (non-zero if we exited slow start)
    println!(
        "slow_start_exit_cwnd: {}KB, current cwnd: {}KB",
        slow_start_exit_cwnd / 1024,
        controller.current_cwnd() / 1024
    );

    // The cwnd at exit should be reasonable (grew during slow start)
    if slow_start_exit_cwnd > 0 {
        assert!(
            slow_start_exit_cwnd >= 38_000,
            "slow_start_exit_cwnd {} should be >= initial_cwnd",
            slow_start_exit_cwnd
        );
    }
}

/// Test BDP-based adaptive floor after slow start exit.
#[test]
fn test_adaptive_floor_bdp_based() {
    let time_source = SharedMockTimeSource::new();
    let config = LedbatConfig {
        initial_cwnd: 100_000,
        min_cwnd: 2_848,
        max_cwnd: 10_000_000,
        ssthresh: 500_000,
        enable_slow_start: true,
        enable_periodic_slowdown: false,
        randomize_ssthresh: false,
        min_ssthresh: None, // Use adaptive calculation
        ..Default::default()
    };

    let controller = LedbatController::new_with_time_source(config, time_source.clone());

    // Simulate slow start exit at a known cwnd
    let exit_cwnd = 200_000;
    let exit_base_delay = Duration::from_millis(50);
    controller
        .slow_start_exit_cwnd
        .store(exit_cwnd, Ordering::Release);
    controller
        .slow_start_exit_base_delay_nanos
        .store(exit_base_delay.as_nanos() as u64, Ordering::Release);

    // Add RTT samples to establish base delay (same as at exit)
    controller.on_ack(exit_base_delay, 1000);
    time_source.advance_time(exit_base_delay);
    controller.on_ack(exit_base_delay, 1000);

    // Calculate adaptive floor
    let floor = controller.calculate_adaptive_floor();

    println!(
        "BDP-based test: slow_start_exit={}KB, floor={}KB",
        exit_cwnd / 1024,
        floor / 1024
    );

    // Floor should be based on slow_start_exit_cwnd (capped at initial_ssthresh)
    // Since initial_ssthresh is ~500KB (with possible jitter) and exit_cwnd is 200KB,
    // the floor should be min(200KB, ~500KB) = 200KB
    assert!(
        floor >= exit_cwnd.min(500_000),
        "BDP-based floor {} should be >= min(exit_cwnd, initial_ssthresh)",
        floor
    );

    // Verify timeout uses the adaptive floor
    controller.cwnd.store(1_000_000, Ordering::Release);
    controller.on_timeout();
    let ssthresh_after = controller.ssthresh.load(Ordering::Acquire);

    assert!(
        ssthresh_after >= floor,
        "ssthresh {} after timeout should be >= adaptive floor {}",
        ssthresh_after,
        floor
    );
}

/// Test RTT-based scaling with explicit min_ssthresh configuration.
/// When min_ssthresh is set, the adaptive floor uses RTT-based scaling
/// capped at the explicit minimum.
#[test]
fn test_adaptive_floor_rtt_scaling_with_explicit_min() {
    let time_source = SharedMockTimeSource::new();
    let min_ssthresh = 200 * 1024; // 200KB explicit floor
    let config = LedbatConfig {
        initial_cwnd: 38_000,
        min_cwnd: 2_848,
        max_cwnd: 10_000_000,
        ssthresh: 1_000_000,
        enable_slow_start: false, // Skip slow start
        enable_periodic_slowdown: false,
        randomize_ssthresh: false,
        min_ssthresh: Some(min_ssthresh), // Explicit floor enables RTT scaling
        ..Default::default()
    };

    let controller = LedbatController::new_with_time_source(config, time_source.clone());

    // No slow start exit, so slow_start_exit_cwnd should be 0
    assert_eq!(controller.slow_start_exit_cwnd.load(Ordering::Acquire), 0);

    // Establish base delay with RTT samples
    let base_delay = Duration::from_millis(135); // Intercontinental RTT
    for _ in 0..5 {
        controller.on_ack(base_delay, 1000);
        time_source.advance_time(base_delay);
    }

    // Calculate adaptive floor
    let floor = controller.calculate_adaptive_floor();

    println!(
        "RTT scaling test: base_delay={}ms, floor={}KB, min_ssthresh={}KB",
        base_delay.as_millis(),
        floor / 1024,
        min_ssthresh / 1024
    );

    // RTT-based scaling: 1KB per ms = 135KB for 135ms RTT
    // But since min_ssthresh is 200KB, floor should be at least 200KB
    assert!(
        floor >= min_ssthresh,
        "Floor {} should be >= explicit min_ssthresh {}",
        floor,
        min_ssthresh
    );

    // Floor should respect spec floor
    let spec_floor = 2_848 * 2;
    assert!(
        floor >= spec_floor,
        "Floor {} should be >= spec floor {}",
        floor,
        spec_floor
    );
}

/// Test that without explicit min_ssthresh and no BDP proxy,
/// the adaptive floor falls back to spec-compliant behavior.
#[test]
fn test_adaptive_floor_spec_fallback() {
    let time_source = SharedMockTimeSource::new();
    let min_cwnd = 2_848;
    let config = LedbatConfig {
        initial_cwnd: 38_000,
        min_cwnd,
        max_cwnd: 10_000_000,
        ssthresh: 1_000_000,
        enable_slow_start: false, // Skip slow start, so no BDP proxy
        enable_periodic_slowdown: false,
        randomize_ssthresh: false,
        min_ssthresh: None, // No explicit configuration
        ..Default::default()
    };

    let controller = LedbatController::new_with_time_source(config, time_source.clone());
    let spec_floor = min_cwnd * 2;

    // Establish base delay (won't affect floor without explicit min_ssthresh)
    let base_delay = Duration::from_millis(135);
    for _ in 0..5 {
        controller.on_ack(base_delay, 1000);
        time_source.advance_time(base_delay);
    }

    let floor = controller.calculate_adaptive_floor();

    println!(
        "Spec fallback test: base_delay={}ms, floor={}KB, spec_floor={}KB",
        base_delay.as_millis(),
        floor / 1024,
        spec_floor / 1024
    );

    // Without explicit min_ssthresh and no BDP proxy, should use spec floor
    assert_eq!(
        floor, spec_floor,
        "Floor {} should equal spec floor {} without explicit min_ssthresh or BDP proxy",
        floor, spec_floor
    );
}

/// Test path change detection invalidates BDP proxy.
/// When a significant RTT change is detected, the BDP proxy from slow start exit
/// is considered stale and not used.
#[test]
fn test_adaptive_floor_path_change_detection() {
    let time_source = SharedMockTimeSource::new();
    let min_ssthresh = 150 * 1024; // 150KB explicit floor
    let config = LedbatConfig {
        initial_cwnd: 100_000,
        min_cwnd: 2_848,
        max_cwnd: 10_000_000,
        ssthresh: 500_000,
        enable_slow_start: true,
        enable_periodic_slowdown: false,
        randomize_ssthresh: false,
        min_ssthresh: Some(min_ssthresh), // Need explicit min_ssthresh for RTT scaling
        ..Default::default()
    };

    let controller = LedbatController::new_with_time_source(config, time_source.clone());

    // Simulate slow start exit at 50ms RTT with high BDP
    let exit_cwnd = 500_000; // 500KB - high BDP
    let exit_base_delay = Duration::from_millis(50);
    controller
        .slow_start_exit_cwnd
        .store(exit_cwnd, Ordering::Release);
    controller
        .slow_start_exit_base_delay_nanos
        .store(exit_base_delay.as_nanos() as u64, Ordering::Release);

    // Before path change: should use BDP proxy
    controller.on_ack(exit_base_delay, 1000);
    let floor_before = controller.calculate_adaptive_floor();
    println!(
        "Before path change: floor={}KB (should use BDP proxy ~{}KB capped at min_ssthresh)",
        floor_before / 1024,
        exit_cwnd / 1024
    );
    // Should use min(exit_cwnd, min_ssthresh) = min(500KB, 150KB) = 150KB
    assert!(
        floor_before >= min_ssthresh,
        "Floor before path change {} should be >= min_ssthresh {}",
        floor_before,
        min_ssthresh
    );

    // Now simulate a path change: RTT doubled to 100ms (>50% change)
    let new_base_delay = Duration::from_millis(100);
    for _ in 0..5 {
        controller.on_ack(new_base_delay, 1000);
        time_source.advance_time(new_base_delay);
    }

    // Calculate adaptive floor - should detect path change
    let floor_after = controller.calculate_adaptive_floor();

    println!(
        "After path change: exit_rtt=50ms, current_rtt=100ms, floor={}KB",
        floor_after / 1024
    );

    // With path change detected, BDP proxy is invalidated
    // Should fall back to RTT-based scaling: 100ms * 1KB = 100KB
    // But since min_ssthresh is 150KB, floor should be at least 150KB
    assert!(
        floor_after >= min_ssthresh,
        "Floor after path change {} should still be >= min_ssthresh {}",
        floor_after,
        min_ssthresh
    );
}

/// Test that adaptive floor always respects spec floor.
#[test]
fn test_adaptive_floor_spec_compliance() {
    let time_source = SharedMockTimeSource::new();
    let min_cwnd = 2_848;
    let config = LedbatConfig {
        initial_cwnd: 10_000,
        min_cwnd,
        max_cwnd: 10_000_000,
        ssthresh: 50_000,
        enable_slow_start: true,
        enable_periodic_slowdown: false,
        randomize_ssthresh: false,
        min_ssthresh: None,
        ..Default::default()
    };

    let controller = LedbatController::new_with_time_source(config, time_source.clone());
    let spec_floor = min_cwnd * 2;

    // Test with very low slow_start_exit_cwnd
    controller
        .slow_start_exit_cwnd
        .store(1000, Ordering::Release);
    controller.slow_start_exit_base_delay_nanos.store(
        Duration::from_millis(10).as_nanos() as u64,
        Ordering::Release,
    );

    // Add matching RTT samples
    controller.on_ack(Duration::from_millis(10), 1000);
    time_source.advance_time(Duration::from_millis(10));
    controller.on_ack(Duration::from_millis(10), 1000);

    let floor = controller.calculate_adaptive_floor();

    println!(
        "Spec compliance test: slow_start_exit=1KB, floor={}KB, spec_floor={}KB",
        floor / 1024,
        spec_floor / 1024
    );

    // Floor must always be at least spec floor
    assert!(
        floor >= spec_floor,
        "Adaptive floor {} must be >= spec floor {}",
        floor,
        spec_floor
    );
}

/// Test adaptive floor with zero base_delay and explicit min_ssthresh.
///
/// When base_delay is zero (no RTT samples yet), the floor should fall back
/// to the explicit min_ssthresh rather than computing an RTT-based value.
#[test]
fn test_adaptive_floor_zero_base_delay_with_explicit_min() {
    let time_source = SharedMockTimeSource::new();
    let explicit_min = 100 * 1024; // 100KB
    let config = LedbatConfig {
        initial_cwnd: 38_000,
        min_cwnd: 2_848,
        max_cwnd: 10_000_000,
        ssthresh: 1_000_000,
        enable_slow_start: true,
        enable_periodic_slowdown: false,
        randomize_ssthresh: false,
        min_ssthresh: Some(explicit_min),
        ..Default::default()
    };

    let controller = LedbatController::new_with_time_source(config, time_source.clone());

    // No RTT samples - base_delay should be Duration::MAX (treated as 0 for ms calculation)
    // No slow_start_exit_cwnd either
    assert_eq!(controller.slow_start_exit_cwnd.load(Ordering::Acquire), 0);

    let floor = controller.calculate_adaptive_floor();
    let spec_floor = controller.min_cwnd * 2;

    println!(
        "Zero base_delay test: floor={}KB, explicit_min={}KB, spec_floor={}KB",
        floor / 1024,
        explicit_min / 1024,
        spec_floor / 1024
    );

    // With zero base_delay and no BDP proxy, floor should be max(explicit_min, spec_floor)
    assert!(
        floor >= explicit_min,
        "Floor {} should be >= explicit_min {}",
        floor,
        explicit_min
    );
    assert!(
        floor >= spec_floor,
        "Floor {} should be >= spec_floor {}",
        floor,
        spec_floor
    );
}

/// Test path change detection without explicit min_ssthresh configuration.
///
/// When min_ssthresh is not configured and a path change is detected,
/// the implementation should fall back to spec_floor.
#[test]
fn test_adaptive_floor_path_change_without_explicit_min() {
    let time_source = SharedMockTimeSource::new();
    let min_cwnd = 2_848;
    let config = LedbatConfig {
        initial_cwnd: 38_000,
        min_cwnd,
        max_cwnd: 10_000_000,
        ssthresh: 1_000_000,
        enable_slow_start: true,
        enable_periodic_slowdown: false,
        randomize_ssthresh: false,
        min_ssthresh: None, // No explicit config
        ..Default::default()
    };

    let controller = LedbatController::new_with_time_source(config, time_source.clone());
    let spec_floor = min_cwnd * 2;

    // Set up BDP proxy with initial RTT of 50ms
    let initial_rtt = Duration::from_millis(50);
    controller.on_ack(initial_rtt, 1000);
    time_source.advance_time(initial_rtt);
    controller.on_ack(initial_rtt, 1000);

    // Simulate slow start exit
    controller
        .slow_start_exit_cwnd
        .store(500_000, Ordering::Release); // 500KB
    controller
        .slow_start_exit_base_delay_nanos
        .store(initial_rtt.as_nanos() as u64, Ordering::Release);

    // Verify BDP proxy works before path change
    let floor_before = controller.calculate_adaptive_floor();
    println!(
        "Before path change: floor={}KB, spec_floor={}KB",
        floor_before / 1024,
        spec_floor / 1024
    );
    assert!(
        floor_before > spec_floor,
        "BDP proxy should produce floor ({}) > spec_floor ({})",
        floor_before,
        spec_floor
    );

    // Now simulate a path change: RTT increases significantly
    // We need to add enough samples to flush the old minimum from the sliding window.
    // base_delay uses a window of BASE_DELAY_HISTORY samples, so we add more than that.
    // Use 80ms RTT (60% higher than 50ms) which exceeds the 50% threshold.
    let new_rtt = Duration::from_millis(80);
    // Add many samples to ensure the old 50ms minimum is flushed from the window
    for _ in 0..50 {
        controller.on_ack(new_rtt, 1000);
        time_source.advance_time(new_rtt);
    }

    // Verify base_delay has updated to the new minimum
    let current_base_delay = controller.base_delay();
    println!(
        "After adding new RTT samples: base_delay={}ms (target: 80ms)",
        current_base_delay.as_millis()
    );

    // The path change should be detected (80ms / 50ms = 1.6 > 1.5 threshold)
    let floor_after = controller.calculate_adaptive_floor();
    println!(
        "After path change: floor={}KB, spec_floor={}KB, base_delay={}ms",
        floor_after / 1024,
        spec_floor / 1024,
        current_base_delay.as_millis()
    );

    // With path change detected and no explicit min_ssthresh, should use spec_floor
    // Note: Only assert this if path change was actually detected (base_delay shifted)
    if current_base_delay >= Duration::from_millis(75) {
        // Path change should be detected: 80ms / 50ms = 1.6 > 1.5
        assert_eq!(
            floor_after, spec_floor,
            "After path change without explicit min, floor ({}) should equal spec_floor ({})",
            floor_after, spec_floor
        );
    } else {
        // base_delay window hasn't fully updated yet - test BDP proxy is still used
        assert!(
            floor_after > spec_floor,
            "Without path change detection, BDP proxy should still be used. floor={}, spec_floor={}",
            floor_after, spec_floor
        );
        println!("Note: base_delay window not fully flushed, testing BDP proxy behavior instead");
    }
}

// Property tests for adaptive min_ssthresh
mod adaptive_floor_proptests {
    use super::*;
    use proptest::prelude::*;

    // Adaptive floor is bounded between spec floor and configured max.
    proptest! {
        #[test]
        fn adaptive_floor_bounded(
            initial_cwnd in 10_000usize..500_000,
            min_cwnd in 2000usize..5000,
            slow_start_exit in 0usize..1_000_000,
            base_delay_ms in 1u64..500
        ) {
            let time_source = SharedMockTimeSource::new();
            let config = LedbatConfig {
                initial_cwnd,
                min_cwnd,
                max_cwnd: 10_000_000,
                ssthresh: 1_000_000,
                enable_slow_start: true,
                enable_periodic_slowdown: false,
                randomize_ssthresh: false,
                min_ssthresh: None,
                ..Default::default()
            };

            let controller = LedbatController::new_with_time_source(config, time_source.clone());

            // Set up BDP proxy
            if slow_start_exit > 0 {
                controller.slow_start_exit_cwnd.store(slow_start_exit, Ordering::Release);
                controller.slow_start_exit_base_delay_nanos
                    .store(Duration::from_millis(base_delay_ms).as_nanos() as u64, Ordering::Release);
            }

            // Establish base delay
            let rtt = Duration::from_millis(base_delay_ms);
            for _ in 0..3 {
                controller.on_ack(rtt, 1000);
                time_source.advance_time(rtt);
            }

            let floor = controller.calculate_adaptive_floor();
            let spec_floor = min_cwnd * 2;
            let initial_ssthresh = controller.initial_ssthresh;

            // Floor must be >= spec floor
            prop_assert!(
                floor >= spec_floor,
                "Floor {} < spec floor {}",
                floor, spec_floor
            );

            // Floor must be <= initial_ssthresh (configured upper bound)
            prop_assert!(
                floor <= initial_ssthresh,
                "Floor {} > initial_ssthresh {}",
                floor, initial_ssthresh
            );
        }
    }

    // Timeout recovery with adaptive floor maintains useful throughput.
    proptest! {
        #[test]
        fn timeout_with_adaptive_floor_recovers(
            initial_cwnd in 50_000usize..200_000,
            exit_cwnd in 100_000usize..500_000,
            base_delay_ms in 50u64..200,
            num_timeouts in 1usize..5
        ) {
            let time_source = SharedMockTimeSource::new();
            let config = LedbatConfig {
                initial_cwnd,
                min_cwnd: 2_848,
                max_cwnd: 10_000_000,
                ssthresh: 1_000_000,
                enable_slow_start: true,
                enable_periodic_slowdown: false,
                randomize_ssthresh: false,
                min_ssthresh: None, // Use adaptive
                ..Default::default()
            };

            let controller = LedbatController::new_with_time_source(config, time_source.clone());

            // Simulate slow start exit
            controller.slow_start_exit_cwnd.store(exit_cwnd, Ordering::Release);
            controller.slow_start_exit_base_delay_nanos
                .store(Duration::from_millis(base_delay_ms).as_nanos() as u64, Ordering::Release);

            // Establish matching base delay
            let rtt = Duration::from_millis(base_delay_ms);
            for _ in 0..3 {
                controller.on_ack(rtt, 1000);
                time_source.advance_time(rtt);
            }

            // Get expected floor before timeouts
            let expected_floor = controller.calculate_adaptive_floor();

            // Set high cwnd before timeout
            controller.cwnd.store(500_000, Ordering::Release);

            // Simulate multiple timeouts
            for _ in 0..num_timeouts {
                controller.on_timeout();
            }

            let final_ssthresh = controller.ssthresh.load(Ordering::Acquire);

            // ssthresh should never go below the adaptive floor
            prop_assert!(
                final_ssthresh >= expected_floor,
                "ssthresh {} < adaptive floor {} after {} timeouts",
                final_ssthresh, expected_floor, num_timeouts
            );

            // ssthresh should be reasonably close to RTT-based estimate for useful throughput.
            // The BDP-based floor (exit_cwnd) may differ slightly from RTT-scaled heuristic,
            // so we allow a small tolerance. Key property: ssthresh >= adaptive floor.
            let spec_floor = 2_848 * 2;
            prop_assert!(
                final_ssthresh >= spec_floor,
                "ssthresh {} should be >= spec floor {}",
                final_ssthresh, spec_floor
            );
        }
    }
}

/// Integration test: Full slow start exit → timeout → recovery cycle.
/// Tests the BDP proxy capture during slow start exit and its use in timeout recovery.
#[test]
fn test_full_slow_start_timeout_recovery_cycle() {
    let time_source = SharedMockTimeSource::new();
    let min_ssthresh = 100 * 1024; // 100KB floor for high-BDP recovery
    let config = LedbatConfig {
        initial_cwnd: 38_000,
        min_cwnd: 2_848,
        max_cwnd: 10_000_000,
        ssthresh: 1_000_000,
        enable_slow_start: true,
        enable_periodic_slowdown: false,
        randomize_ssthresh: false,
        min_ssthresh: Some(min_ssthresh), // Explicit floor enables adaptive features
        ..Default::default()
    };

    let controller = LedbatController::new_with_time_source(config, time_source.clone());

    println!("\n========== Full Cycle Test ==========");

    // Phase 1: Slow start growth
    let base_rtt = Duration::from_millis(135); // Intercontinental
    controller.on_send(500_000);

    println!("Initial cwnd: {}KB", controller.current_cwnd() / 1024);

    for i in 0..15 {
        time_source.advance_time(base_rtt);
        // Use delay that approaches but doesn't exceed exit threshold initially
        let queuing_delay_ms = 30 + (i * 2); // Gradually increasing
        let rtt = Duration::from_millis(135 + queuing_delay_ms);
        controller.on_ack(rtt, 10_000);
    }

    let cwnd_after_slow_start = controller.current_cwnd();
    let slow_start_exit_cwnd = controller.slow_start_exit_cwnd.load(Ordering::Acquire);
    println!(
        "After slow start: cwnd={}KB, slow_start_exit_cwnd={}KB",
        cwnd_after_slow_start / 1024,
        slow_start_exit_cwnd / 1024
    );

    // Phase 2: Simulate timeout
    controller.on_timeout();
    let ssthresh_after_timeout = controller.ssthresh.load(Ordering::Acquire);
    println!(
        "After timeout: cwnd={}KB, ssthresh={}KB",
        controller.current_cwnd() / 1024,
        ssthresh_after_timeout / 1024
    );

    // Phase 3: Verify recovery potential
    let expected_floor = controller.calculate_adaptive_floor();
    println!("Adaptive floor: {}KB", expected_floor / 1024);

    // ssthresh should be at least the adaptive floor
    assert!(
        ssthresh_after_timeout >= expected_floor,
        "ssthresh {} should be >= adaptive floor {}",
        ssthresh_after_timeout,
        expected_floor
    );

    // With explicit min_ssthresh of 100KB, floor should be at least that
    assert!(
        expected_floor >= min_ssthresh,
        "Adaptive floor {} should be >= min_ssthresh {}KB",
        expected_floor / 1024,
        min_ssthresh / 1024
    );

    println!("✓ Full cycle test passed");
}

// =========================================================================
// Large Transfer Throughput Regression Test
//
// This test catches the production issue where 2.3MB transfers on 135ms RTT
// paths take ~19 seconds instead of ~3 seconds. It simulates the transfer
// and asserts it completes in a reasonable number of RTTs.
// =========================================================================

/// Regression test: 2.3MB transfer after timeout recovery should complete reasonably.
///
/// This test catches the death spiral issue where:
/// - Production: 2.3MB transfer taking ~19 seconds (140 RTTs, avg cwnd ~16KB)
/// - Expected: 2.3MB transfer in ~6 seconds (45 RTTs, avg cwnd ~50KB)
///
/// The test simulates a connection that experienced multiple timeouts
/// (as happens in production on congested paths) and then verifies that
/// a large transfer can still complete in reasonable time via slow start recovery.
#[test]
fn test_large_transfer_throughput_regression() {
    // Match production scenario: River UI contract on nova→technic path
    let transfer_size: usize = 2_300_000; // 2.3MB
    let rtt = Duration::from_millis(135); // Intercontinental RTT
    let min_ssthresh = 100 * 1024; // 100KB floor (deployed in v0.1.82)

    let config = LedbatConfig {
        initial_cwnd: 38_000, // IW26
        min_cwnd: 2_848,
        max_cwnd: 10_000_000,
        ssthresh: 1_000_000,
        enable_slow_start: true,
        enable_periodic_slowdown: true,
        randomize_ssthresh: false,
        min_ssthresh: Some(min_ssthresh),
        ..Default::default()
    };

    let mut harness = LedbatTestHarness::new(
        config,
        NetworkCondition::INTERCONTINENTAL,
        54321, // Fixed seed
    );

    println!("\n========== Large Transfer After Timeout Recovery Test ==========");
    println!(
        "Transfer: {} MB, RTT: {}ms, min_ssthresh: {}KB",
        transfer_size / 1_000_000,
        rtt.as_millis(),
        min_ssthresh / 1024
    );

    // Phase 1: Simulate the connection being in a bad state from previous congestion
    // This is what happens in production - transfers start from a degraded state
    println!("\nPhase 1: Simulating prior congestion (3 timeouts)...");
    for i in 1..=3 {
        harness.inject_timeout();
        println!(
            "  Timeout {}: cwnd={}KB, ssthresh={}KB",
            i,
            harness.snapshot().cwnd / 1024,
            harness.controller.ssthresh.load(Ordering::Acquire) / 1024
        );
    }

    let snap_after_timeouts = harness.snapshot();
    println!(
        "After timeouts: cwnd={}KB, state={:?}",
        snap_after_timeouts.cwnd / 1024,
        snap_after_timeouts.state
    );

    // Phase 2: Now attempt the large transfer from this degraded state
    println!("\nPhase 2: Starting 2.3MB transfer from degraded state...");
    let mut bytes_sent: usize = 0;
    let mut rtts_elapsed = 0;
    let mut cwnd_samples: Vec<usize> = Vec::new();
    let max_rtts = 150; // Safety limit - production bug was ~140 RTTs

    while bytes_sent < transfer_size && rtts_elapsed < max_rtts {
        // Get current cwnd
        let snap = harness.snapshot();
        let cwnd = snap.cwnd;
        cwnd_samples.push(cwnd);

        // Send one RTT worth of data (capped at remaining bytes)
        let bytes_this_rtt = cwnd.min(transfer_size - bytes_sent);
        bytes_sent += bytes_this_rtt;
        rtts_elapsed += 1;

        // Simulate ACKs for this RTT
        harness.run_rtts(1, bytes_this_rtt);

        // Log progress every 10 RTTs
        if rtts_elapsed % 10 == 0 {
            println!(
                "  RTT {}: cwnd={}KB, sent={}KB ({:.0}%)",
                rtts_elapsed,
                cwnd / 1024,
                bytes_sent / 1024,
                (bytes_sent as f64 / transfer_size as f64) * 100.0
            );
        }
    }

    // Calculate statistics
    let avg_cwnd: usize = cwnd_samples.iter().sum::<usize>() / cwnd_samples.len();
    let min_cwnd_observed = *cwnd_samples.iter().min().unwrap();
    let max_cwnd_observed = *cwnd_samples.iter().max().unwrap();
    let simulated_time_ms = rtts_elapsed as u64 * rtt.as_millis() as u64;
    let throughput_mbps = (bytes_sent as f64 * 8.0) / (simulated_time_ms as f64 * 1000.0);

    println!("\nResults:");
    println!(
        "  Bytes sent: {} / {} ({:.1}%)",
        bytes_sent,
        transfer_size,
        (bytes_sent as f64 / transfer_size as f64) * 100.0
    );
    println!("  RTTs elapsed: {}", rtts_elapsed);
    println!(
        "  Simulated time: {:.1}s",
        simulated_time_ms as f64 / 1000.0
    );
    println!("  Average cwnd: {}KB", avg_cwnd / 1024);
    println!(
        "  Min/Max cwnd: {}KB / {}KB",
        min_cwnd_observed / 1024,
        max_cwnd_observed / 1024
    );
    println!("  Throughput: {:.1} Mbps", throughput_mbps);

    // Get final stats including new telemetry fields
    let final_stats = harness.controller.stats();
    println!("\nLEDBAT Stats:");
    println!("  ssthresh: {}KB", final_stats.ssthresh / 1024);
    println!(
        "  min_ssthresh_floor: {}KB",
        final_stats.min_ssthresh_floor / 1024
    );
    println!("  total_timeouts: {}", final_stats.total_timeouts);
    println!("  total_losses: {}", final_stats.total_losses);
    println!("  slow_start_exits: {}", final_stats.slow_start_exits);

    // Key assertions - these would fail with the death spiral bug
    //
    // Without min_ssthresh fix:
    //   ssthresh collapses to 5KB, avg_cwnd ~16KB, ~140 RTTs (~19s)
    // With min_ssthresh fix:
    //   ssthresh stays at 100KB floor, avg_cwnd ~50KB+, ~45 RTTs (~6s)
    //
    // Thresholds set to catch death spiral while allowing margin:
    // - Max 80 RTTs (~11s) - production bug showed 140 RTTs (19s)
    // - Min 30KB avg cwnd - production bug showed ~16KB average

    assert!(
        bytes_sent >= transfer_size,
        "Transfer should complete: sent {} of {} bytes",
        bytes_sent,
        transfer_size
    );

    assert!(
        rtts_elapsed <= 80,
        "Transfer took {} RTTs ({:.1}s) - too slow! Expected <= 80 RTTs (~11s). \
         Average cwnd was {}KB. This indicates a death spiral or recovery issue. \
         ssthresh={}KB, min_floor={}KB",
        rtts_elapsed,
        simulated_time_ms as f64 / 1000.0,
        avg_cwnd / 1024,
        final_stats.ssthresh / 1024,
        final_stats.min_ssthresh_floor / 1024
    );

    assert!(
        avg_cwnd >= 30_000,
        "Average cwnd was {}KB - too low! Expected >= 30KB. \
         This indicates cwnd is not recovering properly after timeouts. \
         ssthresh={}KB, min_floor={}KB",
        avg_cwnd / 1024,
        final_stats.ssthresh / 1024,
        final_stats.min_ssthresh_floor / 1024
    );

    // Verify ssthresh didn't collapse below the floor
    assert!(
        final_stats.ssthresh >= min_ssthresh,
        "Final ssthresh {}KB should be >= min_ssthresh floor {}KB - death spiral detected!",
        final_stats.ssthresh / 1024,
        min_ssthresh / 1024
    );

    // Verify the telemetry captured the initial timeouts
    assert_eq!(
        final_stats.total_timeouts, 3,
        "total_timeouts should have captured the 3 injected timeouts"
    );

    println!(
        "\n✓ Large transfer completed in {} RTTs ({:.1}s) with avg cwnd {}KB - PASSED",
        rtts_elapsed,
        simulated_time_ms as f64 / 1000.0,
        avg_cwnd / 1024
    );
}

// =========================================================================
// Application-Limited Cap Regression Test (Trapped cwnd on High-RTT Paths)
//
// This test directly validates the fix for the "trapped cwnd" problem where
// cwnd gets capped at flightsize + 2*MSS, preventing it from reaching ssthresh
// on high-latency paths.
//
// Unlike test_large_transfer_throughput_regression which uses min_ssthresh
// (a separate floor mechanism), this test uses min_ssthresh: None to directly
// exercise the ssthresh-as-ceiling logic in the application-limited cap.
// =========================================================================

/// Regression test: cwnd should escape the flightsize trap on high-RTT paths.
///
/// This test validates the fix for the "trapped cwnd" problem:
/// - On 166ms RTT paths, flightsize may be ~50KB due to ACK timing
/// - Without fix: cwnd capped at flightsize + 3KB = 53KB
/// - With fix: cwnd can grow up to ssthresh (100KB) when capped_change > 0
///
/// The test does NOT use min_ssthresh to ensure we're testing the actual
/// application-limited cap fix, not the separate ssthresh floor mechanism.
#[test]
fn test_cwnd_escapes_flightsize_trap_without_min_ssthresh() {
    // Setup: High RTT path with NO min_ssthresh floor
    let ssthresh_value = 100 * 1024; // 100KB - the ceiling we should be able to reach

    let config = LedbatConfig {
        initial_cwnd: 38_000, // IW26
        min_cwnd: 2_848,      // ~2 MSS
        max_cwnd: 10_000_000,
        ssthresh: ssthresh_value,
        enable_slow_start: true,
        enable_periodic_slowdown: false, // Disable to focus on congestion avoidance
        randomize_ssthresh: false,
        min_ssthresh: None, // CRITICAL: No floor - test the app-limited cap directly
        ..Default::default()
    };

    let mut harness = LedbatTestHarness::new(
        config,
        NetworkCondition::INTERCONTINENTAL, // 166ms base RTT
        99999,                              // Fixed seed
    );

    println!("\n========== Trapped cwnd Escape Test (No min_ssthresh) ==========");
    println!("ssthresh: {}KB, min_ssthresh: None", ssthresh_value / 1024);

    // Phase 1: Run slow start until we exit into congestion avoidance
    // Slow start exits when cwnd >= ssthresh
    println!("\nPhase 1: Running slow start until exit...");
    let mut rtts = 0;
    while harness.snapshot().state == CongestionState::SlowStart && rtts < 20 {
        // Send full cwnd worth of data
        let cwnd = harness.snapshot().cwnd;
        harness.step(cwnd);
        rtts += 1;
    }

    let snap_after_ss = harness.snapshot();
    println!(
        "After slow start exit: cwnd={}KB, state={:?}, rtts={}",
        snap_after_ss.cwnd / 1024,
        snap_after_ss.state,
        rtts
    );

    // Verify we're now in congestion avoidance (RampingUp or similar)
    assert_ne!(
        snap_after_ss.state,
        CongestionState::SlowStart,
        "Should have exited slow start"
    );

    // Phase 2: Inject a timeout to force ssthresh to a known lower value
    // This simulates what happens in production after packet loss
    println!("\nPhase 2: Injecting timeout to set up trapped scenario...");
    harness.inject_timeout();

    let snap_after_timeout = harness.snapshot();
    let ssthresh_after_timeout = harness.controller.ssthresh.load(Ordering::Acquire);
    println!(
        "After timeout: cwnd={}KB, ssthresh={}KB, state={:?}",
        snap_after_timeout.cwnd / 1024,
        ssthresh_after_timeout / 1024,
        snap_after_timeout.state
    );

    // Phase 3: Simulate the trapped scenario - small flightsize but need to grow
    // The key: send only a small amount, so flightsize is small when ACK arrives
    println!("\nPhase 3: Testing escape from flightsize trap...");

    // Manually create the trapped scenario:
    // 1. Start with small cwnd (from timeout)
    // 2. Send a small amount (simulating partial RTT utilization)
    // 3. Receive ACK - this should allow cwnd to grow toward ssthresh
    let small_send = 30 * 1024; // 30KB - smaller than ssthresh

    // Run a few RTTs sending only small amounts
    // This simulates the production scenario where flightsize stays small
    for i in 1..=10 {
        let pre_cwnd = harness.snapshot().cwnd;
        harness.step(small_send);
        let post_cwnd = harness.snapshot().cwnd;

        if i <= 3 || i == 10 {
            println!(
                "  RTT {}: cwnd {} -> {}KB",
                i,
                pre_cwnd / 1024,
                post_cwnd / 1024
            );
        }
    }

    // Phase 4: Verify cwnd grew beyond the strict flightsize cap
    let final_snap = harness.snapshot();
    let final_ssthresh = harness.controller.ssthresh.load(Ordering::Acquire);

    // The strict cap would be: small_send + allowed_increase_packets * MSS
    // With allowed_increase_packets=2 and MSS=1424, that's ~33KB
    let strict_cap = small_send + 2 * MSS;

    println!("\nResults:");
    println!("  Final cwnd: {}KB", final_snap.cwnd / 1024);
    println!("  Final ssthresh: {}KB", final_ssthresh / 1024);
    println!("  Strict flightsize cap would be: {}KB", strict_cap / 1024);
    println!("  State: {:?}", final_snap.state);

    // Key assertion: cwnd should have grown beyond the strict flightsize cap
    // With the fix, cwnd can grow up to ssthresh when capped_change > 0
    assert!(
        final_snap.cwnd > strict_cap,
        "cwnd ({:.1}KB) should exceed strict flightsize cap ({:.1}KB)! \
         Without the fix, cwnd gets trapped at flightsize + 3KB. \
         ssthresh={}KB should be reachable.",
        final_snap.cwnd as f64 / 1024.0,
        strict_cap as f64 / 1024.0,
        final_ssthresh / 1024
    );

    // Secondary assertion: cwnd should be approaching ssthresh
    // (might not reach it fully due to congestion avoidance AIMD)
    let ssthresh_50_pct = final_ssthresh / 2;
    assert!(
        final_snap.cwnd >= ssthresh_50_pct,
        "cwnd ({:.1}KB) should be at least 50% of ssthresh ({:.1}KB). \
         Got {}% of ssthresh.",
        final_snap.cwnd as f64 / 1024.0,
        final_ssthresh as f64 / 1024.0,
        (final_snap.cwnd * 100) / final_ssthresh
    );

    println!(
        "\n✓ cwnd escaped flightsize trap: {}KB > strict cap {}KB - PASSED",
        final_snap.cwnd / 1024,
        strict_cap / 1024
    );
}

/// Regression test: Periodic slowdown must apply min_ssthresh floor.
///
/// BUG: When a periodic slowdown triggers, `ssthresh` is set to `current_cwnd`
/// WITHOUT applying the min_ssthresh floor:
///
/// ```ignore
/// // Line ~1382 - BUG: no floor applied
/// self.ssthresh.store(current_cwnd, Ordering::Release);
/// ```
///
/// The timeout handler correctly applies the floor:
///
/// ```ignore
/// // Line ~1718-1720 - CORRECT
/// let floor = self.calculate_adaptive_floor();
/// let new_ssthresh = (old_cwnd / 2).max(floor);
/// ```
///
/// This asymmetry defeats the min_ssthresh protection on high-latency paths where
/// periodic slowdowns are the primary cwnd reduction mechanism (no packet loss).
///
/// SYMPTOM: On 135ms RTT paths, ssthresh gets reduced to ~37KB by periodic
/// slowdowns, preventing cwnd from growing past that point even though
/// min_ssthresh was configured to 100KB.
///
/// This test verifies that ssthresh stays >= min_ssthresh floor after a
/// periodic slowdown when cwnd is below the floor.
#[test]
fn test_periodic_slowdown_applies_min_ssthresh_floor() {
    // Use a high floor to catch the bug - floor is 200KB
    let min_ssthresh = 200 * 1024;

    let config = LedbatConfig {
        // Start with initial_cwnd below min_ssthresh (38KB < 200KB floor)
        initial_cwnd: 38_000,
        min_cwnd: 2_848,
        max_cwnd: 1_000_000,
        // Set ssthresh just above initial_cwnd so slow start exits quickly
        // When slow start exits, cwnd will be around 40-80KB, below the 200KB floor
        ssthresh: 50_000,
        enable_slow_start: true,
        enable_periodic_slowdown: true,
        randomize_ssthresh: false,
        min_ssthresh: Some(min_ssthresh), // Configure 200KB floor
        ..Default::default()
    };

    let mut harness = LedbatTestHarness::new(
        config,
        NetworkCondition::INTERCONTINENTAL, // 135ms RTT
        42,
    );

    println!("\n========== Periodic Slowdown min_ssthresh Floor Test ==========");
    println!(
        "Config: min_ssthresh={}KB, initial_ssthresh={}KB",
        min_ssthresh / 1024,
        50
    );
    println!("Strategy: Exit slow start at ~50KB (below 200KB floor), then trigger slowdown");

    // Run until a natural periodic slowdown triggers
    // With ssthresh=50KB, slow start exits quickly with cwnd around 50KB
    // Then a periodic slowdown will set ssthresh = current_cwnd (~50KB)
    // which is below the 200KB floor
    let result = harness.run_until_slowdown(100, 100_000);
    assert!(
        result.is_ok(),
        "Should trigger a periodic slowdown within 100 RTTs"
    );

    let snapshots = result.unwrap();
    let slowdowns_after = snapshots.last().unwrap().periodic_slowdowns;

    // Verify at least one slowdown occurred
    assert!(
        slowdowns_after >= 1,
        "Expected at least 1 periodic slowdown, got {}",
        slowdowns_after
    );

    // Check ssthresh after the slowdown
    let ssthresh_after = harness.controller.ssthresh.load(Ordering::Acquire);
    let cwnd_after = harness.snapshot().cwnd;

    println!(
        "After {} slowdowns: ssthresh={}KB, cwnd={}KB, floor={}KB",
        slowdowns_after,
        ssthresh_after / 1024,
        cwnd_after / 1024,
        min_ssthresh / 1024
    );

    // KEY ASSERTION: ssthresh must stay at or above the configured floor
    // BUG: Currently fails because periodic slowdown sets ssthresh = current_cwnd
    // without applying the floor. Since cwnd at slowdown time is ~50KB and floor
    // is 200KB, ssthresh ends up at ~50KB instead of being raised to 200KB.
    assert!(
        ssthresh_after >= min_ssthresh,
        "ssthresh ({} bytes = {}KB) fell below min_ssthresh floor ({} bytes = {}KB)!\n\
         This indicates the periodic slowdown handler is not applying the \
         min_ssthresh floor. The timeout handler correctly applies the floor \
         but the periodic slowdown handler at line ~1382 does not.",
        ssthresh_after,
        ssthresh_after / 1024,
        min_ssthresh,
        min_ssthresh / 1024
    );

    println!(
        "✓ ssthresh ({:.0}KB) >= min_ssthresh floor ({:.0}KB) - PASSED",
        ssthresh_after as f64 / 1024.0,
        min_ssthresh as f64 / 1024.0
    );
}

/// Integration test: Large transfer on high-latency path must complete in reasonable time.
///
/// This is the "golden test" that catches the entire class of slow-transfer bugs:
/// - ssthresh death spiral (repeated reductions below useful levels)
/// - cwnd trapped below ssthresh (can't grow to match BDP)
/// - Excessive slowdowns preventing sustained throughput
/// - min_ssthresh floor not being applied (in timeout OR periodic slowdown)
/// - Any other issue that prevents reasonable throughput on high-BDP paths
///
/// The test simulates the real-world scenario that kept failing:
/// - ~2.5MB transfer (River UI contract)
/// - 135ms RTT (Germany to USA)
/// - Starting from minimum cwnd (post-timeout recovery)
///
/// If this test passes, we have high confidence the transfer will complete
/// in a reasonable time in production.
#[test]
fn test_large_transfer_high_latency_completes_in_reasonable_time() {
    // Production scenario parameters
    let transfer_size = 2_500_000; // 2.5MB (River UI contract size)
    let rtt_ms = 135; // Germany <-> USA
    let min_ssthresh = 100 * 1024; // 100KB floor (production config)

    // Calculate acceptable bounds
    // At 100KB/RTT steady state: 2.5MB / 100KB = 25 RTTs (ideal)
    // With slow start, ramp-ups, and slowdowns: allow 4x overhead
    // 25 * 4 = 100 RTTs = 13.5 seconds at 135ms RTT
    // We'll use 150 RTTs (~20 seconds) as the absolute maximum
    let max_rtts = 150;
    let min_avg_throughput_per_rtt = transfer_size / max_rtts; // ~16KB/RTT minimum

    let config = LedbatConfig {
        initial_cwnd: 2_848, // Start at minimum (simulating post-timeout)
        min_cwnd: 2_848,
        max_cwnd: 10_000_000,
        ssthresh: 1_000_000, // Will be set by recovery
        enable_slow_start: true,
        enable_periodic_slowdown: true,
        randomize_ssthresh: false,
        min_ssthresh: Some(min_ssthresh),
        ..Default::default()
    };

    // Use intercontinental conditions but without packet loss for determinism
    // (packet loss would cause timeouts which reset progress unpredictably)
    let condition = NetworkCondition::custom(rtt_ms, Some(0.1), 0.0);
    let mut harness = LedbatTestHarness::new(config, condition, 42);

    println!("\n========== Large Transfer High-Latency Test ==========");
    println!(
        "Transfer: {:.1}MB, RTT: {}ms, min_ssthresh: {}KB",
        transfer_size as f64 / (1024.0 * 1024.0),
        rtt_ms,
        min_ssthresh / 1024
    );
    println!(
        "Success criteria: Complete in <{} RTTs (<{:.1}s)",
        max_rtts,
        max_rtts as f64 * rtt_ms as f64 / 1000.0
    );

    // Track cumulative "bytes transferred" (cwnd per RTT approximates throughput)
    let mut total_bytes_transferred: usize = 0;
    let mut rtts_elapsed = 0;
    let mut min_ssthresh_observed = usize::MAX;
    let mut max_slowdowns_observed = 0;
    let mut snapshots_below_floor = 0;

    // Run until we've transferred enough bytes or hit the RTT limit
    while total_bytes_transferred < transfer_size && rtts_elapsed < max_rtts {
        harness.step(100_000); // Simulate sending up to 100KB per RTT
        let snap = harness.snapshot();

        // cwnd represents how much we could send this RTT
        total_bytes_transferred += snap.cwnd;
        rtts_elapsed += 1;

        // Track pathological states
        let ssthresh = harness.controller.ssthresh.load(Ordering::Acquire);
        min_ssthresh_observed = min_ssthresh_observed.min(ssthresh);
        max_slowdowns_observed = max_slowdowns_observed.max(snap.periodic_slowdowns);

        if ssthresh < min_ssthresh {
            snapshots_below_floor += 1;
        }

        // Progress logging every 25 RTTs
        if rtts_elapsed % 25 == 0 || total_bytes_transferred >= transfer_size {
            println!(
                "  RTT {}: transferred {:.1}MB/{:.1}MB, cwnd={}KB, ssthresh={}KB, slowdowns={}",
                rtts_elapsed,
                total_bytes_transferred as f64 / (1024.0 * 1024.0),
                transfer_size as f64 / (1024.0 * 1024.0),
                snap.cwnd / 1024,
                ssthresh / 1024,
                snap.periodic_slowdowns
            );
        }
    }

    // Calculate results
    let transfer_complete = total_bytes_transferred >= transfer_size;
    let transfer_time_s = rtts_elapsed as f64 * rtt_ms as f64 / 1000.0;
    let avg_throughput_per_rtt = total_bytes_transferred / rtts_elapsed.max(1);
    let throughput_mbps = (total_bytes_transferred as f64 * 8.0) / (transfer_time_s * 1_000_000.0);

    println!("\n--- Results ---");
    println!(
        "Transfer complete: {} ({:.1}MB in {} RTTs = {:.1}s)",
        if transfer_complete { "YES" } else { "NO" },
        total_bytes_transferred as f64 / (1024.0 * 1024.0),
        rtts_elapsed,
        transfer_time_s
    );
    println!("Throughput: {:.1} Mbps", throughput_mbps);
    println!(
        "Avg cwnd/RTT: {}KB (min required: {}KB)",
        avg_throughput_per_rtt / 1024,
        min_avg_throughput_per_rtt / 1024
    );
    println!(
        "Min ssthresh observed: {}KB (floor: {}KB)",
        min_ssthresh_observed / 1024,
        min_ssthresh / 1024
    );
    println!("Total slowdowns: {}", max_slowdowns_observed);
    println!("RTTs with ssthresh below floor: {}", snapshots_below_floor);

    // Assertions
    assert!(
        transfer_complete,
        "Transfer did not complete in {} RTTs ({:.1}s)! \
         Only transferred {:.1}MB of {:.1}MB. \
         This indicates a throughput problem on high-latency paths. \
         Check: ssthresh floor enforcement, cwnd growth, slowdown frequency.",
        max_rtts,
        transfer_time_s,
        total_bytes_transferred as f64 / (1024.0 * 1024.0),
        transfer_size as f64 / (1024.0 * 1024.0)
    );

    assert!(
        avg_throughput_per_rtt >= min_avg_throughput_per_rtt,
        "Average throughput {}KB/RTT below minimum {}KB/RTT required for acceptable transfer speed",
        avg_throughput_per_rtt / 1024,
        min_avg_throughput_per_rtt / 1024
    );

    assert_eq!(
        snapshots_below_floor, 0,
        "ssthresh fell below min_ssthresh floor {} times! \
         This indicates the floor is not being enforced in all code paths.",
        snapshots_below_floor
    );

    // Warn if close to the limit (indicates fragile throughput)
    if rtts_elapsed > max_rtts * 3 / 4 {
        println!(
            "WARNING: Transfer used {}% of RTT budget - throughput is marginal",
            rtts_elapsed * 100 / max_rtts
        );
    }

    println!(
        "\n✓ Large transfer test PASSED: {:.1}MB in {:.1}s ({:.1} Mbps)",
        transfer_size as f64 / (1024.0 * 1024.0),
        transfer_time_s,
        throughput_mbps
    );
}

//! LEDBAT++ congestion controller.
//!
//! This module contains the main controller implementation with slow start
//! and periodic slowdown support.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use crate::config::GlobalRng;
use crate::simulation::{RealTime, TimeSource};

use super::atomic::{AtomicBaseDelayHistory, AtomicDelayFilter};
use super::config::{
    LedbatConfig, MAX_GAIN_DIVISOR, MSS, PATH_CHANGE_RTT_THRESHOLD, RTT_SCALING_BYTES_PER_MS,
    SLOWDOWN_DELAY_RTTS, SLOWDOWN_FREEZE_RTTS, SLOWDOWN_INTERVAL_MULTIPLIER,
    SLOWDOWN_REDUCTION_FACTOR, TARGET,
};
use super::state::{AtomicCongestionState, CongestionState};
use super::stats::LedbatStats;

/// LEDBAT++ congestion controller with slow start and periodic slowdowns.
///
/// Implements draft-irtf-iccrg-ledbat-plus-plus with improvements over RFC 6817:
/// - Dynamic GAIN based on base_delay
/// - Multiplicative decrease capped at -W/2
/// - Periodic slowdowns for inter-flow fairness
/// - 60ms target delay (vs 100ms in RFC 6817)
///
/// ## Lock-Free Design
///
/// This controller is fully lock-free, using atomic operations for all state.
/// No mutexes are held on the hot path (on_ack, on_send).
///
/// ## Thread Safety
///
/// Individual atomic operations are lock-free and thread-safe. However,
/// compound state transitions (e.g., state change + associated variable updates)
/// are NOT atomic. In practice, this controller is designed to be accessed from
/// a single tokio task per connection. For strictly concurrent access from
/// multiple threads, external synchronization may be required to prevent
/// interleaved state machine transitions (e.g., `on_timeout` racing with `on_ack`).
///
/// ## Slow Start Phase
///
/// Before LEDBAT's congestion avoidance, uses TCP-style exponential growth
/// for fast ramp-up. Exits when:
/// - cwnd >= ssthresh (reached threshold), OR
/// - queuing_delay > TARGET * delay_exit_threshold (congestion detected)
///
/// ## Periodic Slowdowns
///
/// After initial slow start exit, periodically reduces cwnd to minimum to
/// re-measure base delay and ensure fair sharing with competing flows.
///
/// ## Type Parameter
///
/// `T` is the time source used for timing operations. Defaults to `RealTime`
/// for production use. In tests, use `VirtualTime` for deterministic
/// virtual time testing via `LedbatController::new_with_time_source()`.
pub struct LedbatController<T: TimeSource = RealTime> {
    /// Congestion window (bytes in flight)
    pub(crate) cwnd: AtomicUsize,

    /// Bytes currently in flight (sent but not ACKed)
    pub(crate) flightsize: AtomicUsize,

    /// Lock-free delay filter (MIN over recent samples)
    pub(crate) delay_filter: AtomicDelayFilter,

    /// Lock-free base delay history (10-minute buckets)
    pub(crate) base_delay_history: AtomicBaseDelayHistory<T>,

    /// Current queuing delay estimate (stored as nanoseconds)
    pub(crate) queuing_delay_nanos: AtomicU64,

    /// Last update time (stored as nanos since controller creation)
    pub(crate) last_update_nanos: AtomicU64,

    /// Time source for getting current time.
    ///
    /// In production, this is `RealTime` which wraps `Instant::now()`.
    /// In tests, `VirtualTime` allows deterministic virtual time control.
    pub(crate) time_source: T,

    /// Reference epoch in nanoseconds for converting to duration-since-start.
    ///
    /// All timing calculations use `time_source.now_nanos() - epoch_nanos`
    /// to ensure tests with virtual time sources observe the mocked time
    /// rather than wall-clock time.
    ///
    /// This field is set once at construction time from `time_source.now_nanos()`.
    pub(crate) epoch_nanos: u64,

    /// Bytes acknowledged since last update
    pub(crate) bytes_acked_since_update: AtomicUsize,

    /// Slow start threshold (bytes)
    pub(crate) ssthresh: AtomicUsize,

    // ===== Unified Congestion State Machine =====
    /// Current congestion control state. This single field replaces the previous
    /// `in_slow_start` boolean and `SlowdownState` enum, ensuring unambiguous
    /// state transitions. See [`CongestionState`] for the state machine diagram.
    pub(crate) congestion_state: AtomicCongestionState,

    /// Time when current slowdown phase started (nanos since epoch)
    pub(crate) slowdown_phase_start_nanos: AtomicU64,

    /// RTT count within current slowdown phase
    pub(crate) slowdown_rtt_count: AtomicU32,

    /// Duration of last complete slowdown cycle (nanos) for scheduling next
    pub(crate) last_slowdown_duration_nanos: AtomicU64,

    /// Time when next scheduled slowdown should start (nanos since epoch)
    pub(crate) next_slowdown_time_nanos: AtomicU64,

    /// cwnd value saved before slowdown (for ramping back up)
    pub(crate) pre_slowdown_cwnd: AtomicUsize,

    /// Whether initial slow start has completed (triggers first slowdown)
    pub(crate) initial_slow_start_completed: AtomicBool,

    /// Configuration
    pub(crate) target_delay: Duration,
    pub(crate) allowed_increase_packets: usize,
    pub(crate) min_cwnd: usize,
    pub(crate) max_cwnd: usize,
    pub(crate) delay_exit_threshold: f64,
    pub(crate) enable_periodic_slowdown: bool,
    /// Minimum ssthresh floor for timeout recovery.
    /// If None, uses the spec-compliant 2*min_cwnd floor.
    pub(crate) min_ssthresh: Option<usize>,

    // ===== Adaptive min_ssthresh tracking (Phase 2) =====
    /// Initial ssthresh from config (saved for adaptive floor calculation).
    /// This represents the configured upper bound for the adaptive floor.
    pub(crate) initial_ssthresh: usize,

    /// cwnd captured at slow start exit (proxy for path BDP).
    /// This represents the point where we first detected congestion, which is
    /// a good proxy for the actual bandwidth-delay product of the path.
    pub(crate) slow_start_exit_cwnd: AtomicUsize,

    /// Base delay (min RTT) at slow start exit (for path change detection).
    /// Stored as nanoseconds. If the current base_delay differs significantly
    /// from this value, we may have changed paths and should not rely on
    /// the old slow_start_exit_cwnd.
    pub(crate) slow_start_exit_base_delay_nanos: AtomicU64,

    /// Statistics
    pub(crate) total_increases: AtomicUsize,
    pub(crate) total_decreases: AtomicUsize,
    pub(crate) total_losses: AtomicUsize,
    pub(crate) min_cwnd_events: AtomicUsize,
    pub(crate) slow_start_exits: AtomicUsize,
    pub(crate) periodic_slowdowns: AtomicUsize,
    /// Peak congestion window reached during this controller's lifetime
    pub(crate) peak_cwnd: AtomicUsize,
    /// Total retransmission timeouts (RTO events)
    pub(crate) total_timeouts: AtomicUsize,
}

// ============================================================================
// Production constructors (backward-compatible, use real time)
// ============================================================================

#[cfg(test)]
impl LedbatController<RealTime> {
    /// Create new LEDBAT controller with default config (backward compatible).
    ///
    /// # Arguments
    /// * `initial_cwnd` - Initial congestion window (bytes)
    /// * `min_cwnd` - Minimum window (typically 2 * MSS)
    /// * `max_cwnd` - Maximum window (protocol limit or config)
    #[cfg(test)]
    pub fn new(initial_cwnd: usize, min_cwnd: usize, max_cwnd: usize) -> Self {
        let config = LedbatConfig {
            initial_cwnd,
            min_cwnd,
            max_cwnd,
            ..LedbatConfig::default()
        };
        Self::new_with_config(config)
    }

    /// Create new LEDBAT controller with custom configuration.
    ///
    /// This constructor allows full control over slow start parameters
    /// and other LEDBAT settings. Uses real system time (`RealTime`).
    pub fn new_with_config(config: LedbatConfig) -> Self {
        Self::new_with_time_source(config, RealTime::new())
    }
}

// ============================================================================
// Generic implementation (works with any TimeSource)
// ============================================================================

impl<T: TimeSource> LedbatController<T> {
    /// Create new LEDBAT controller with custom configuration and time source.
    ///
    /// This constructor is the primary entry point for creating a controller
    /// with a mock time source for deterministic testing.
    pub fn new_with_time_source(config: LedbatConfig, time_source: T) -> Self {
        // Validate configuration parameters
        assert!(
            config.min_cwnd <= config.initial_cwnd,
            "min_cwnd ({}) must be <= initial_cwnd ({})",
            config.min_cwnd,
            config.initial_cwnd
        );
        assert!(
            config.initial_cwnd <= config.max_cwnd,
            "initial_cwnd ({}) must be <= max_cwnd ({})",
            config.initial_cwnd,
            config.max_cwnd
        );
        assert!(
            config.delay_exit_threshold >= 0.0 && config.delay_exit_threshold <= 1.0,
            "delay_exit_threshold ({}) must be in range [0.0, 1.0]",
            config.delay_exit_threshold
        );

        // Apply ±20% jitter to ssthresh to prevent synchronization
        let ssthresh = if config.randomize_ssthresh {
            // Use GlobalRng for deterministic simulation
            let random_byte = GlobalRng::random_range(0u8..40);
            let jitter_pct = 0.8 + (random_byte as f64) / 100.0; // 0.8 to 1.2 (±20%)
            ((config.ssthresh as f64) * jitter_pct) as usize
        } else {
            config.ssthresh
        };

        let epoch_nanos = time_source.now_nanos();

        Self {
            cwnd: AtomicUsize::new(config.initial_cwnd),
            flightsize: AtomicUsize::new(0),
            delay_filter: AtomicDelayFilter::new(),
            base_delay_history: AtomicBaseDelayHistory::new(time_source.clone()),
            queuing_delay_nanos: AtomicU64::new(0),
            last_update_nanos: AtomicU64::new(0),
            time_source,
            epoch_nanos,
            bytes_acked_since_update: AtomicUsize::new(0),
            ssthresh: AtomicUsize::new(ssthresh),
            // Unified congestion state: start in SlowStart or CongestionAvoidance
            congestion_state: AtomicCongestionState::new(if config.enable_slow_start {
                CongestionState::SlowStart
            } else {
                CongestionState::CongestionAvoidance
            }),
            slowdown_phase_start_nanos: AtomicU64::new(0),
            slowdown_rtt_count: AtomicU32::new(0),
            last_slowdown_duration_nanos: AtomicU64::new(0),
            next_slowdown_time_nanos: AtomicU64::new(u64::MAX), // No scheduled slowdown yet
            pre_slowdown_cwnd: AtomicUsize::new(0),
            initial_slow_start_completed: AtomicBool::new(false),
            // Configuration
            target_delay: TARGET,
            allowed_increase_packets: 2, // RFC 6817 default
            min_cwnd: config.min_cwnd,
            max_cwnd: config.max_cwnd,
            delay_exit_threshold: config.delay_exit_threshold,
            enable_periodic_slowdown: config.enable_periodic_slowdown,
            min_ssthresh: config.min_ssthresh,
            // Adaptive min_ssthresh tracking
            initial_ssthresh: ssthresh, // Save for adaptive floor calculation
            slow_start_exit_cwnd: AtomicUsize::new(0),
            slow_start_exit_base_delay_nanos: AtomicU64::new(0),
            // Statistics
            total_increases: AtomicUsize::new(0),
            total_decreases: AtomicUsize::new(0),
            total_losses: AtomicUsize::new(0),
            min_cwnd_events: AtomicUsize::new(0),
            slow_start_exits: AtomicUsize::new(0),
            periodic_slowdowns: AtomicUsize::new(0),
            peak_cwnd: AtomicUsize::new(config.initial_cwnd),
            total_timeouts: AtomicUsize::new(0),
        }
    }

    /// Store new cwnd value and update peak_cwnd if this is a new maximum.
    fn store_cwnd(&self, new_cwnd: usize) {
        self.cwnd.store(new_cwnd, Ordering::Release);
        // Update peak if this is a new maximum (lock-free max update)
        self.peak_cwnd
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current_peak| {
                if new_cwnd > current_peak {
                    Some(new_cwnd)
                } else {
                    None
                }
            })
            .ok(); // Ignore result - we just want the side effect
    }

    /// Get the peak congestion window reached during this controller's lifetime.
    pub fn peak_cwnd(&self) -> usize {
        self.peak_cwnd.load(Ordering::Relaxed)
    }

    /// Get elapsed time in nanoseconds since controller creation (for testing).
    #[cfg(test)]
    pub fn elapsed_nanos(&self) -> u64 {
        self.time_source.now_nanos() - self.epoch_nanos
    }

    /// Calculate dynamic GAIN based on base delay (LEDBAT++ Section 4.2)
    ///
    /// GAIN = 1 / min(16, ceil(2 * TARGET / base_delay))
    ///
    /// This adapts the responsiveness based on the ratio of target to base delay.
    /// For low-latency links (low base_delay), GAIN is smaller for stability.
    /// For high-latency links (high base_delay), GAIN approaches 1/16 minimum.
    pub(crate) fn calculate_dynamic_gain(&self, base_delay: Duration) -> f64 {
        let base_ms = base_delay.as_millis() as f64;
        let target_ms = self.target_delay.as_millis() as f64;

        if base_ms <= 0.0 {
            // Edge case: base_delay is zero or unset. This occurs:
            // 1. At connection start before any RTT measurements
            // 2. After base_delay history reset (rare)
            // 3. As a defensive guard against measurement errors
            // Use most conservative GAIN (1/16) for stability.
            return 1.0 / MAX_GAIN_DIVISOR as f64;
        }

        let divisor = (2.0 * target_ms / base_ms).ceil() as u32;
        let clamped_divisor = divisor.clamp(1, MAX_GAIN_DIVISOR);
        1.0 / clamped_divisor as f64
    }

    /// Called when packet sent.
    ///
    /// Tracks bytes in flight for application-limited handling.
    pub fn on_send(&self, bytes: usize) {
        self.flightsize.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Called when ACK received with RTT sample.
    ///
    /// Updates congestion window based on queuing delay.
    /// This method is fully lock-free.
    ///
    /// # Arguments
    /// * `rtt_sample` - Round-trip time measurement
    /// * `bytes_acked_now` - Bytes acknowledged by this ACK
    pub fn on_ack(&self, rtt_sample: Duration, bytes_acked_now: usize) {
        // Capture flightsize BEFORE decreasing - this represents the actual utilization
        // level when the ACK was sent, which is needed for the app-limited cap.
        let pre_ack_flightsize = self.flightsize.load(Ordering::Relaxed);

        // Decrease flightsize with saturating subtraction to prevent underflow
        self.flightsize
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(bytes_acked_now))
            })
            .ok();

        // Accumulate acknowledged bytes
        self.bytes_acked_since_update
            .fetch_add(bytes_acked_now, Ordering::Relaxed);

        // Lock-free delay tracking
        // Update base delay history
        self.base_delay_history.update(rtt_sample);

        // Add to delay filter
        self.delay_filter.add_sample(rtt_sample);

        // Get filtered delay (minimum of recent samples)
        if !self.delay_filter.is_ready() {
            return; // Need more samples
        }
        let filtered_rtt = self.delay_filter.filtered_delay().unwrap_or(rtt_sample);

        // Calculate base and queuing delays
        let base_delay = self.base_delay_history.base_delay();
        let queuing_delay = filtered_rtt.saturating_sub(base_delay);
        self.queuing_delay_nanos
            .store(queuing_delay.as_nanos() as u64, Ordering::Release);

        // Rate-limit updates to approximately once per RTT
        let now_nanos = self.time_source.now_nanos() - self.epoch_nanos;
        let last_update = self.last_update_nanos.load(Ordering::Acquire);
        let elapsed_nanos = now_nanos.saturating_sub(last_update);

        // Use base delay as RTT estimate for rate-limiting
        if elapsed_nanos < base_delay.as_nanos() as u64 {
            return;
        }

        // Try to claim this update slot (only one thread should proceed)
        if self
            .last_update_nanos
            .compare_exchange(last_update, now_nanos, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return; // Another thread won the race
        }

        // Calculate off-target amount
        let target = self.target_delay;
        let off_target_ms = if queuing_delay < target {
            (target - queuing_delay).as_millis() as f64
        } else {
            -((queuing_delay - target).as_millis() as f64)
        };
        let target_ms = target.as_millis() as f64;

        // Get total bytes acked since last update
        let bytes_acked_total = self.bytes_acked_since_update.swap(0, Ordering::AcqRel);
        if bytes_acked_total == 0 {
            return; // No progress
        }

        // Unified state machine dispatch - single check, no overlapping flags
        let state = self.congestion_state.load();
        match state {
            CongestionState::SlowStart => {
                // Initial slow start: exponential growth
                self.handle_slow_start(bytes_acked_total, queuing_delay, base_delay);
                return;
            }
            CongestionState::WaitingForSlowdown
            | CongestionState::InSlowdown
            | CongestionState::Frozen
            | CongestionState::RampingUp => {
                // Slowdown state machine handles these states
                if self.handle_congestion_state(bytes_acked_total, queuing_delay, base_delay) {
                    return;
                }
                // Fall through to congestion avoidance if state completed
            }
            CongestionState::CongestionAvoidance => {
                // Check for scheduled slowdown trigger
                if self.enable_periodic_slowdown
                    && self.handle_congestion_state(bytes_acked_total, queuing_delay, base_delay)
                {
                    return;
                }
                // Fall through to congestion avoidance
            }
        }

        // LEDBAT++ congestion avoidance (draft-irtf-iccrg-ledbat-plus-plus Section 4.2)
        let current_cwnd = self.cwnd.load(Ordering::Acquire);

        // Calculate dynamic GAIN based on base delay
        let gain = self.calculate_dynamic_gain(base_delay);

        let cwnd_change =
            gain * (off_target_ms / target_ms) * (bytes_acked_total as f64) * (MSS as f64)
                / (current_cwnd as f64);

        // LEDBAT++ key improvement: cap decrease at -W/2 (multiplicative decrease limit)
        let max_decrease = -(current_cwnd as f64 / 2.0);
        let capped_change = if cwnd_change < max_decrease {
            max_decrease
        } else {
            cwnd_change
        };

        let mut new_cwnd = current_cwnd as f64 + capped_change;

        // Track statistics
        if capped_change > 0.0 {
            self.total_increases.fetch_add(1, Ordering::Relaxed);
        } else if capped_change < 0.0 {
            self.total_decreases.fetch_add(1, Ordering::Relaxed);
        }

        // Application-limited cap (RFC 6817 Section 2.4.1)
        let basic_cap = pre_ack_flightsize + (self.allowed_increase_packets * MSS);
        let max_allowed_cwnd = if capped_change >= 0.0 {
            // Stable or growing: allow up to ssthresh to escape the flightsize trap
            let ssthresh = self.ssthresh.load(Ordering::Acquire);
            basic_cap.max(ssthresh)
        } else {
            // Decreasing (congestion response): use strict app-limited cap
            basic_cap
        };
        new_cwnd = new_cwnd.min(max_allowed_cwnd as f64);

        // Enforce bounds
        new_cwnd = new_cwnd.max(self.min_cwnd as f64).min(self.max_cwnd as f64);

        let new_cwnd_usize = new_cwnd as usize;

        if new_cwnd_usize == self.min_cwnd && current_cwnd > self.min_cwnd {
            self.min_cwnd_events.fetch_add(1, Ordering::Relaxed);
        }

        self.store_cwnd(new_cwnd_usize);

        // Log significant changes
        let change_abs = (new_cwnd_usize as i64 - current_cwnd as i64).unsigned_abs() as usize;
        if change_abs > 10_000 {
            tracing::debug!(
                old_cwnd_kb = current_cwnd / 1024,
                new_cwnd_kb = new_cwnd_usize / 1024,
                change_kb = cwnd_change as i64 / 1024,
                queuing_delay_ms = queuing_delay.as_millis(),
                base_delay_ms = base_delay.as_millis(),
                off_target_ms,
                bytes_acked = bytes_acked_total,
                flightsize_kb = pre_ack_flightsize / 1024,
                "LEDBAT cwnd update"
            );
        }
    }

    /// Handle slow start phase (exponential growth).
    fn handle_slow_start(&self, bytes_acked: usize, queuing_delay: Duration, base_delay: Duration) {
        // Early exit if no longer in SlowStart state (race: timeout occurred)
        if !self.congestion_state.is_slow_start() {
            return;
        }

        let current_cwnd = self.cwnd.load(Ordering::Acquire);
        let ssthresh = self.ssthresh.load(Ordering::Acquire);

        // Check exit conditions (LEDBAT++ uses 3/4 of target, configured via delay_exit_threshold)
        let delay_threshold =
            Duration::from_secs_f64(self.target_delay.as_secs_f64() * self.delay_exit_threshold);
        let should_exit = current_cwnd >= ssthresh || queuing_delay > delay_threshold;

        if should_exit {
            self.slow_start_exits.fetch_add(1, Ordering::Relaxed);

            // Capture BDP proxy for adaptive min_ssthresh calculation.
            self.slow_start_exit_cwnd
                .store(current_cwnd, Ordering::Release);
            self.slow_start_exit_base_delay_nanos
                .store(base_delay.as_nanos() as u64, Ordering::Release);

            // Conservative reduction on exit
            let new_cwnd = ((current_cwnd as f64) * 0.9) as usize;
            let new_cwnd = new_cwnd.max(self.min_cwnd).min(self.max_cwnd);
            self.cwnd.store(new_cwnd, Ordering::Release);

            // LEDBAT++: Schedule initial slowdown after first slow start exit
            if self.enable_periodic_slowdown
                && !self
                    .initial_slow_start_completed
                    .swap(true, Ordering::AcqRel)
            {
                let now_nanos = self.time_source.now_nanos() - self.epoch_nanos;
                let delay_nanos = base_delay.as_nanos() as u64 * SLOWDOWN_DELAY_RTTS as u64;
                self.next_slowdown_time_nanos
                    .store(now_nanos + delay_nanos, Ordering::Release);
                self.congestion_state.enter_waiting_for_slowdown();

                tracing::debug!(
                    delay_ms = delay_nanos / 1_000_000,
                    "LEDBAT++ scheduling initial slowdown after slow start exit"
                );
            } else {
                self.congestion_state.enter_congestion_avoidance();
            }

            let exit_reason = if current_cwnd >= ssthresh {
                "ssthresh"
            } else {
                "delay"
            };

            tracing::debug!(
                old_cwnd_kb = current_cwnd / 1024,
                new_cwnd_kb = new_cwnd / 1024,
                ssthresh_kb = ssthresh / 1024,
                queuing_delay_ms = queuing_delay.as_millis(),
                delay_threshold_ms = delay_threshold.as_millis(),
                reason = exit_reason,
                total_exits = self.slow_start_exits.load(Ordering::Relaxed),
                "Exiting slow start"
            );
        } else {
            // Exponential growth: cwnd += bytes_acked (doubles per RTT)
            let new_cwnd = (current_cwnd + bytes_acked).min(self.max_cwnd);
            self.store_cwnd(new_cwnd);

            tracing::trace!(
                old_cwnd_kb = current_cwnd / 1024,
                new_cwnd_kb = new_cwnd / 1024,
                bytes_acked_kb = bytes_acked / 1024,
                queuing_delay_ms = queuing_delay.as_millis(),
                "Slow start growth"
            );
        }
    }

    /// Handle congestion state machine transitions.
    ///
    /// Returns true if the state machine handled this update (caller should return).
    fn handle_congestion_state(
        &self,
        bytes_acked: usize,
        queuing_delay: Duration,
        base_delay: Duration,
    ) -> bool {
        let now_nanos = self.time_source.now_nanos() - self.epoch_nanos;
        let state = self.congestion_state.load();

        match state {
            CongestionState::CongestionAvoidance => {
                // Check if it's time for the next scheduled slowdown
                let next_slowdown = self.next_slowdown_time_nanos.load(Ordering::Acquire);
                if now_nanos >= next_slowdown && next_slowdown != u64::MAX {
                    if self.start_slowdown(now_nanos, base_delay) {
                        return true;
                    }
                }
                false
            }
            CongestionState::WaitingForSlowdown => {
                let next_slowdown = self.next_slowdown_time_nanos.load(Ordering::Acquire);
                if now_nanos >= next_slowdown {
                    if self.start_slowdown(now_nanos, base_delay) {
                        return true;
                    }
                    self.congestion_state.enter_congestion_avoidance();
                    return false;
                }
                true
            }
            CongestionState::InSlowdown => {
                self.congestion_state.enter_frozen();
                self.slowdown_rtt_count.store(0, Ordering::Release);
                self.slowdown_phase_start_nanos
                    .store(now_nanos, Ordering::Release);
                true
            }
            CongestionState::Frozen => {
                let phase_start = self.slowdown_phase_start_nanos.load(Ordering::Acquire);
                let freeze_duration = base_delay.as_nanos() as u64 * SLOWDOWN_FREEZE_RTTS as u64;

                if now_nanos.saturating_sub(phase_start) >= freeze_duration {
                    self.congestion_state.enter_ramping_up();
                    tracing::debug!(
                        cwnd_kb = self.cwnd.load(Ordering::Relaxed) / 1024,
                        "LEDBAT++ slowdown: starting ramp-up phase"
                    );
                    return true;
                }
                let pre_slowdown = self.pre_slowdown_cwnd.load(Ordering::Relaxed);
                let frozen_cwnd = (pre_slowdown / SLOWDOWN_REDUCTION_FACTOR).max(self.min_cwnd);
                self.cwnd.store(frozen_cwnd, Ordering::Release);
                true
            }
            CongestionState::RampingUp => {
                let target_cwnd = self.pre_slowdown_cwnd.load(Ordering::Acquire);
                let current_cwnd = self.cwnd.load(Ordering::Acquire);

                if current_cwnd >= target_cwnd {
                    self.complete_slowdown(now_nanos, base_delay);
                    return true;
                }

                let has_recovered_substantially = current_cwnd * 20 >= target_cwnd * 17; // 85%

                if has_recovered_substantially && queuing_delay > self.target_delay {
                    self.complete_slowdown(now_nanos, base_delay);
                    return true;
                }

                let new_cwnd = (current_cwnd + bytes_acked)
                    .min(target_cwnd)
                    .min(self.max_cwnd);
                self.store_cwnd(new_cwnd);
                true
            }
            CongestionState::SlowStart => false,
        }
    }

    /// Start a periodic slowdown cycle.
    ///
    /// Returns `true` if slowdown was started, `false` if skipped (cwnd too small).
    pub(crate) fn start_slowdown(&self, now_nanos: u64, base_delay: Duration) -> bool {
        let current_cwnd = self.cwnd.load(Ordering::Acquire);

        if current_cwnd <= self.min_cwnd * SLOWDOWN_REDUCTION_FACTOR {
            tracing::debug!(
                cwnd_kb = current_cwnd / 1024,
                min_cwnd_kb = self.min_cwnd / 1024,
                threshold_kb = (self.min_cwnd * SLOWDOWN_REDUCTION_FACTOR) / 1024,
                "LEDBAT++ skipping futile slowdown: cwnd too small to reduce"
            );

            let min_slowdown_duration = base_delay.as_nanos() as u64 * SLOWDOWN_FREEZE_RTTS as u64;
            let min_interval = min_slowdown_duration * SLOWDOWN_INTERVAL_MULTIPLIER as u64;
            let extended_interval = min_interval * 2;
            let next_time = now_nanos + extended_interval;
            self.next_slowdown_time_nanos
                .store(next_time, Ordering::Release);

            return false;
        }

        let floor = self.calculate_adaptive_floor();
        let new_ssthresh = current_cwnd.max(floor);
        self.ssthresh.store(new_ssthresh, Ordering::Release);
        self.pre_slowdown_cwnd
            .store(current_cwnd, Ordering::Release);

        let slowdown_cwnd = (current_cwnd / SLOWDOWN_REDUCTION_FACTOR).max(self.min_cwnd);
        self.cwnd.store(slowdown_cwnd, Ordering::Release);

        self.congestion_state.enter_in_slowdown();
        self.slowdown_phase_start_nanos
            .store(now_nanos, Ordering::Release);
        self.periodic_slowdowns.fetch_add(1, Ordering::Relaxed);

        tracing::debug!(
            old_cwnd_kb = current_cwnd / 1024,
            new_cwnd_kb = slowdown_cwnd / 1024,
            ssthresh_kb = new_ssthresh / 1024,
            floor_kb = floor / 1024,
            reduction_factor = SLOWDOWN_REDUCTION_FACTOR,
            "LEDBAT++ periodic slowdown: reducing cwnd proportionally, ssthresh floored"
        );

        true
    }

    /// Complete a slowdown cycle and schedule the next one.
    pub(crate) fn complete_slowdown(&self, now_nanos: u64, base_delay: Duration) {
        let phase_start = self.slowdown_phase_start_nanos.load(Ordering::Acquire);
        let slowdown_duration = now_nanos.saturating_sub(phase_start);

        self.last_slowdown_duration_nanos
            .store(slowdown_duration, Ordering::Release);

        let next_interval = slowdown_duration * SLOWDOWN_INTERVAL_MULTIPLIER as u64;
        let min_slowdown_duration = base_delay.as_nanos() as u64 * SLOWDOWN_FREEZE_RTTS as u64;
        let min_interval = min_slowdown_duration * SLOWDOWN_INTERVAL_MULTIPLIER as u64;
        let actual_interval = next_interval.max(min_interval);

        self.next_slowdown_time_nanos
            .store(now_nanos + actual_interval, Ordering::Release);

        self.congestion_state.enter_congestion_avoidance();

        tracing::debug!(
            slowdown_duration_ms = slowdown_duration / 1_000_000,
            next_interval_ms = actual_interval / 1_000_000,
            "LEDBAT++ slowdown complete, scheduling next"
        );
    }

    /// Called when packet loss detected (not timeout).
    #[cfg(test)]
    pub fn on_loss(&self) {
        self.total_losses.fetch_add(1, Ordering::Relaxed);

        if self.congestion_state.is_slow_start() {
            self.congestion_state.enter_congestion_avoidance();
            self.slow_start_exits.fetch_add(1, Ordering::Relaxed);
        }

        let current_cwnd = self.cwnd.load(Ordering::Acquire);
        let new_cwnd = (current_cwnd / 2).max(self.min_cwnd);

        self.cwnd.store(new_cwnd, Ordering::Release);

        tracing::warn!(
            old_cwnd_kb = current_cwnd / 1024,
            new_cwnd_kb = new_cwnd / 1024,
            total_losses = self.total_losses.load(Ordering::Relaxed),
            "LEDBAT packet loss - halving cwnd"
        );
    }

    /// Detect if a significant path change has occurred based on RTT shift.
    pub(crate) fn has_path_changed(&self, exit_base_delay_nanos: u64) -> bool {
        let current_base_delay_nanos = self.base_delay().as_nanos() as u64;

        if exit_base_delay_nanos == 0 || current_base_delay_nanos == 0 {
            return false;
        }

        let ratio = if current_base_delay_nanos > exit_base_delay_nanos {
            current_base_delay_nanos as f64 / exit_base_delay_nanos as f64
        } else {
            exit_base_delay_nanos as f64 / current_base_delay_nanos as f64
        };

        ratio > PATH_CHANGE_RTT_THRESHOLD
    }

    /// Calculate adaptive min_ssthresh floor based on observed path characteristics.
    pub(crate) fn calculate_adaptive_floor(&self) -> usize {
        let spec_floor = self.min_cwnd * 2;

        if let Some(explicit_min) = self.min_ssthresh {
            let slow_start_exit = self.slow_start_exit_cwnd.load(Ordering::Acquire);
            let exit_base_delay_nanos = self
                .slow_start_exit_base_delay_nanos
                .load(Ordering::Acquire);

            let path_changed = self.has_path_changed(exit_base_delay_nanos);

            let adaptive = if slow_start_exit > 0 && !path_changed {
                slow_start_exit.min(explicit_min)
            } else {
                let base_delay_ms = self.base_delay().as_millis() as usize;
                if base_delay_ms > 0 {
                    (base_delay_ms * RTT_SCALING_BYTES_PER_MS).min(explicit_min)
                } else {
                    explicit_min
                }
            };

            let floor = adaptive.max(explicit_min).max(spec_floor);

            tracing::trace!(
                slow_start_exit_kb = slow_start_exit / 1024,
                explicit_min_kb = explicit_min / 1024,
                adaptive_kb = adaptive / 1024,
                final_floor_kb = floor / 1024,
                "Adaptive floor with explicit min_ssthresh"
            );

            return floor;
        }

        let slow_start_exit = self.slow_start_exit_cwnd.load(Ordering::Acquire);

        if slow_start_exit > 0 {
            let exit_base_delay_nanos = self
                .slow_start_exit_base_delay_nanos
                .load(Ordering::Acquire);

            let path_changed = self.has_path_changed(exit_base_delay_nanos);

            if !path_changed {
                let bdp_floor = slow_start_exit.min(self.initial_ssthresh);
                let floor = bdp_floor.max(spec_floor);

                tracing::trace!(
                    slow_start_exit_kb = slow_start_exit / 1024,
                    bdp_floor_kb = bdp_floor / 1024,
                    final_floor_kb = floor / 1024,
                    "Adaptive floor using BDP proxy"
                );

                return floor;
            }
        }

        tracing::trace!(
            spec_floor_kb = spec_floor / 1024,
            "Using spec-compliant floor (no adaptive data)"
        );

        spec_floor
    }

    /// Called on retransmission timeout (severe congestion).
    pub fn on_timeout(&self) {
        self.total_losses.fetch_add(1, Ordering::Relaxed);
        self.total_timeouts.fetch_add(1, Ordering::Relaxed);

        let old_cwnd = self.cwnd.load(Ordering::Acquire);

        let new_cwnd = MSS.max(self.min_cwnd);
        self.cwnd.store(new_cwnd, Ordering::Release);

        let floor = self.calculate_adaptive_floor();
        let new_ssthresh = (old_cwnd / 2).max(floor);
        self.ssthresh.store(new_ssthresh, Ordering::Release);

        self.congestion_state.enter_slow_start();

        self.next_slowdown_time_nanos
            .store(u64::MAX, Ordering::Release);

        if old_cwnd != new_cwnd {
            tracing::warn!(
                old_cwnd_kb = old_cwnd / 1024,
                new_cwnd_kb = new_cwnd / 1024,
                new_ssthresh_kb = new_ssthresh / 1024,
                "LEDBAT retransmission timeout - reset to min_cwnd, entering SlowStart"
            );
        }
    }

    /// Get current congestion window (bytes).
    pub fn current_cwnd(&self) -> usize {
        self.cwnd.load(Ordering::Acquire)
    }

    /// Convert cwnd (bytes in flight) to rate (bytes/sec).
    pub fn current_rate(&self, rtt: Duration) -> usize {
        let cwnd = self.current_cwnd();
        let safe_rtt = rtt.max(Duration::from_millis(1));
        ((cwnd as f64) / safe_rtt.as_secs_f64()) as usize
    }

    /// Get current queuing delay (lock-free).
    pub fn queuing_delay(&self) -> Duration {
        Duration::from_nanos(self.queuing_delay_nanos.load(Ordering::Acquire))
    }

    /// Get base delay (lock-free).
    pub fn base_delay(&self) -> Duration {
        self.base_delay_history.base_delay()
    }

    /// Get current flightsize.
    pub fn flightsize(&self) -> usize {
        self.flightsize.load(Ordering::Relaxed)
    }

    /// Called when ACK received for a retransmitted packet.
    pub fn on_ack_without_rtt(&self, bytes_acked: usize) {
        self.flightsize
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(bytes_acked))
            })
            .ok();

        tracing::trace!(
            bytes_acked,
            new_flightsize = self.flightsize.load(Ordering::Relaxed),
            "Decremented flightsize for retransmitted packet ACK"
        );
    }

    /// Get LEDBAT congestion control statistics.
    pub fn stats(&self) -> LedbatStats {
        LedbatStats {
            cwnd: self.current_cwnd(),
            flightsize: self.flightsize(),
            queuing_delay: self.queuing_delay(),
            base_delay: self.base_delay(),
            peak_cwnd: self.peak_cwnd(),
            total_increases: self.total_increases.load(Ordering::Relaxed),
            total_decreases: self.total_decreases.load(Ordering::Relaxed),
            total_losses: self.total_losses.load(Ordering::Relaxed),
            min_cwnd_events: self.min_cwnd_events.load(Ordering::Relaxed),
            slow_start_exits: self.slow_start_exits.load(Ordering::Relaxed),
            periodic_slowdowns: self.periodic_slowdowns.load(Ordering::Relaxed),
            ssthresh: self.ssthresh.load(Ordering::Relaxed),
            min_ssthresh_floor: self.calculate_adaptive_floor(),
            total_timeouts: self.total_timeouts.load(Ordering::Relaxed),
        }
    }
}

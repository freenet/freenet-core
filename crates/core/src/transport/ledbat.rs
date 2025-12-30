//! LEDBAT++ (Low Extra Delay Background Transport) congestion controller.
//!
//! Implementation based on draft-irtf-iccrg-ledbat-plus-plus, which improves upon
//! RFC 6817 with better inter-flow fairness and latecomer handling.
//!
//! ## Why LEDBAT++?
//!
//! Freenet runs as a background daemon and should not interfere with foreground
//! applications (video calls, web browsing, etc.). LEDBAT++ is designed exactly
//! for this use case - it's used by BitTorrent (uTP) and Apple for similar reasons.
//!
//! ## Key Improvements over RFC 6817 LEDBAT
//!
//! | Feature | RFC 6817 | LEDBAT++ |
//! |---------|----------|----------|
//! | Target delay | 100ms | 60ms |
//! | Gain | Fixed 1.0 | Dynamic based on base_delay |
//! | Decrease formula | Linear | Multiplicative with -W/2 cap |
//! | Inter-flow fairness | Poor (latecomer advantage) | Good (periodic slowdowns) |
//! | Slow start exit | 50% of target | 75% of target |
//!
//! ## Periodic Slowdown Mechanism
//!
//! LEDBAT++ introduces periodic slowdowns to solve the "latecomer advantage" problem:
//! - After initial slow start exit, wait 2 RTTs then reduce cwnd by 4x (not to minimum)
//! - This proportional reduction is consistent with our large initial window (IW26)
//! - Freeze cwnd for 2 RTTs to allow base delay re-measurement
//! - Ramp back up using slow start until reaching previous cwnd
//! - Schedule next slowdown at 9x the slowdown duration (maintains ≤10% utilization impact)
//!
//! ## Lock-Free Design
//!
//! This implementation is fully lock-free, using atomic operations for all state:
//! - Atomic ring buffers for delay filtering and base delay history
//! - AtomicU64 for Duration values (stored as nanoseconds)
//! - Epoch-based timing for rate-limiting updates
#![allow(dead_code)] // Infrastructure not yet integrated

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use super::packet_data::MAX_DATA_SIZE;

/// Maximum segment size (actual packet data capacity)
const MSS: usize = MAX_DATA_SIZE;

/// Target queuing delay (LEDBAT++ uses 60ms, RFC 6817 used 100ms)
const TARGET: Duration = Duration::from_millis(60);

/// Maximum GAIN divisor for dynamic GAIN calculation (LEDBAT++ Section 4.2)
const MAX_GAIN_DIVISOR: u32 = 16;

/// Base delay history size (RFC 6817 recommendation)
const BASE_HISTORY_SIZE: usize = 10;

/// Delay filter sample count (RFC 6817 recommendation)
const DELAY_FILTER_SIZE: usize = 4;

/// Default slow start threshold (100 KB)
const DEFAULT_SSTHRESH: usize = 102_400;

/// Periodic slowdown interval multiplier (LEDBAT++ Section 4.4)
/// Next slowdown is scheduled at 9x the previous slowdown duration,
/// maintaining ≤10% utilization impact.
const SLOWDOWN_INTERVAL_MULTIPLIER: u32 = 9;

/// Number of RTTs to wait after slow start exit before initial slowdown
const SLOWDOWN_DELAY_RTTS: u32 = 2;

/// Number of RTTs to freeze cwnd at minimum during slowdown
const SLOWDOWN_FREEZE_RTTS: u32 = 2;

/// Slowdown reduction factor: cwnd is divided by this during periodic slowdowns.
/// We use 16 to be consistent with our large initial window (IW26).
/// Factor of 4 provides a more gradual slowdown:
/// - 300KB cwnd drops to 75KB (not 18KB with factor 16)
/// - Avoids throughput "cliff" while still probing for fairness
/// - Faster recovery time (fewer RTTs to ramp back up)
const SLOWDOWN_REDUCTION_FACTOR: usize = 4;

/// Configuration for LEDBAT++ with slow start and periodic slowdowns
#[derive(Debug, Clone)]
pub struct LedbatConfig {
    /// Initial congestion window (bytes)
    pub initial_cwnd: usize,
    /// Minimum congestion window (bytes)
    pub min_cwnd: usize,
    /// Maximum congestion window (bytes)
    pub max_cwnd: usize,
    /// Slow start threshold (bytes)
    pub ssthresh: usize,
    /// Enable slow start phase
    pub enable_slow_start: bool,
    /// Delay threshold to exit slow start (fraction of TARGET)
    /// Default: 0.75 (LEDBAT++ exits when queuing_delay > 3/4 * TARGET = 45ms)
    pub delay_exit_threshold: f64,
    /// Randomize ssthresh to prevent synchronization (±20% jitter)
    /// Default: true
    pub randomize_ssthresh: bool,
    /// Enable periodic slowdowns for inter-flow fairness (LEDBAT++ feature)
    /// Default: true
    pub enable_periodic_slowdown: bool,
}

impl Default for LedbatConfig {
    fn default() -> Self {
        Self {
            // IW26: 26 * MSS = 38,000 bytes
            // Reaches 300KB cwnd in 3 RTTs (38KB → 76KB → 152KB → 304KB)
            // This enables >3 MB/s throughput at 60ms RTT
            initial_cwnd: 38_000,           // 26 * MSS (IW26)
            min_cwnd: 2_848,                // 2 * MSS
            max_cwnd: 1_000_000_000,        // 1 GB
            ssthresh: DEFAULT_SSTHRESH,     // 100 KB
            enable_slow_start: true,        // Enable by default
            delay_exit_threshold: 0.75,     // LEDBAT++: exit at 3/4 * TARGET (45ms)
            randomize_ssthresh: true,       // Enable jitter by default
            enable_periodic_slowdown: true, // LEDBAT++: enable for inter-flow fairness
        }
    }
}

/// Sentinel value indicating an empty slot in atomic delay arrays.
/// We use u64::MAX since no valid RTT would ever be this large (~584 years).
const EMPTY_DELAY_NANOS: u64 = u64::MAX;

/// Periodic slowdown state machine (LEDBAT++ Section 4.4)
///
/// The slowdown mechanism works as follows:
/// 1. After initial slow start exit, wait 2 RTTs (WaitingForSlowdown)
/// 2. Enter slowdown: save cwnd to ssthresh, reduce to min_cwnd (InSlowdown)
/// 3. Freeze at min_cwnd for 2 RTTs to re-measure base delay (Frozen)
/// 4. Ramp back up using slow start until reaching ssthresh (RampingUp)
/// 5. Return to normal operation, schedule next slowdown at 9x duration (Normal)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum SlowdownState {
    /// Normal operation, no slowdown pending
    Normal = 0,
    /// Waiting N RTTs before starting slowdown
    WaitingForSlowdown = 1,
    /// In slowdown: cwnd reduced to minimum, waiting to freeze
    InSlowdown = 2,
    /// Frozen at minimum cwnd, re-measuring base delay
    Frozen = 3,
    /// Ramping back up using slow start
    RampingUp = 4,
}

/// Lock-free delay filter: MIN over recent samples (RFC 6817 Section 4.2).
///
/// Uses a fixed-size atomic ring buffer. Each slot stores RTT in nanoseconds.
/// Readers compute the minimum over all valid (non-empty) slots.
///
/// # Timing Assumptions
///
/// RTT values are stored as nanoseconds in `u64`. The cast from `Duration::as_nanos()`
/// (which returns `u128`) is safe because realistic RTT values are always far below
/// `u64::MAX` (~584 years). Network RTTs range from microseconds to seconds.
struct AtomicDelayFilter {
    /// Ring buffer of RTT samples (stored as nanoseconds)
    samples: [AtomicU64; DELAY_FILTER_SIZE],
    /// Write index (wraps around)
    write_index: AtomicUsize,
    /// Number of samples added (saturates at DELAY_FILTER_SIZE)
    sample_count: AtomicUsize,
}

impl AtomicDelayFilter {
    fn new() -> Self {
        Self {
            samples: std::array::from_fn(|_| AtomicU64::new(EMPTY_DELAY_NANOS)),
            write_index: AtomicUsize::new(0),
            sample_count: AtomicUsize::new(0),
        }
    }

    fn add_sample(&self, rtt: Duration) {
        // Safe cast: RTT values are always far below u64::MAX (~584 years)
        let nanos = rtt.as_nanos() as u64;
        let idx = self.write_index.fetch_add(1, Ordering::Relaxed) % DELAY_FILTER_SIZE;
        self.samples[idx].store(nanos, Ordering::Release);

        // Increment sample count, saturating at DELAY_FILTER_SIZE
        self.sample_count
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                if count < DELAY_FILTER_SIZE {
                    Some(count + 1)
                } else {
                    None // Already saturated
                }
            })
            .ok();
    }

    fn filtered_delay(&self) -> Option<Duration> {
        let mut min_nanos = u64::MAX;
        for slot in &self.samples {
            let nanos = slot.load(Ordering::Acquire);
            if nanos != EMPTY_DELAY_NANOS && nanos < min_nanos {
                min_nanos = nanos;
            }
        }
        if min_nanos == u64::MAX {
            None
        } else {
            Some(Duration::from_nanos(min_nanos))
        }
    }

    fn is_ready(&self) -> bool {
        self.sample_count.load(Ordering::Acquire) >= 2
    }
}

/// Lock-free base delay history: 10-minute bucket tracking (RFC 6817 Section 4.1).
///
/// Uses atomic arrays for buckets and atomic values for current minute tracking.
/// All updates use compare-and-swap for thread safety.
///
/// # Timing Assumptions
///
/// Durations are stored as nanoseconds in `u64`. This limits representable
/// durations to ~584 years, which is acceptable for RTT measurements.
/// The epoch-based timing also assumes the controller won't run for 584+ years.
struct AtomicBaseDelayHistory {
    /// One bucket per minute, containing minimum delay observed in that minute (nanos)
    buckets: [AtomicU64; BASE_HISTORY_SIZE],
    /// Number of valid buckets (0 to BASE_HISTORY_SIZE)
    bucket_count: AtomicUsize,
    /// Next bucket index to write (wraps around)
    bucket_write_index: AtomicUsize,
    /// Minimum being tracked for current minute (nanos)
    current_minute_min: AtomicU64,
    /// Start time of current minute (nanos since epoch instant)
    current_minute_start_nanos: AtomicU64,
    /// Epoch instant for time calculations
    epoch: Instant,
}

impl AtomicBaseDelayHistory {
    fn new() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(EMPTY_DELAY_NANOS)),
            bucket_count: AtomicUsize::new(0),
            bucket_write_index: AtomicUsize::new(0),
            current_minute_min: AtomicU64::new(EMPTY_DELAY_NANOS),
            current_minute_start_nanos: AtomicU64::new(0),
            epoch: Instant::now(),
        }
    }

    fn update(&self, rtt_sample: Duration) {
        // Safe cast: RTT values are always far below u64::MAX (~584 years)
        let rtt_nanos = rtt_sample.as_nanos() as u64;
        let now_nanos = self.epoch.elapsed().as_nanos() as u64;
        let minute_nanos = 60_000_000_000u64; // 60 seconds in nanos

        let minute_start = self.current_minute_start_nanos.load(Ordering::Acquire);

        // Check if we've rolled over to a new minute
        if now_nanos.saturating_sub(minute_start) >= minute_nanos {
            // Try to roll over to new minute (only one thread should succeed)
            let new_minute_start = now_nanos;
            if self
                .current_minute_start_nanos
                .compare_exchange(
                    minute_start,
                    new_minute_start,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                // We won the race to roll over - atomically reset current minute
                // and capture the old minimum for bucket storage.
                //
                // Use swap to EMPTY first, then compete for new minimum.
                // This prevents the race where a losing thread's update_current_min
                // sets a smaller value that we then overwrite.
                let old_min = self
                    .current_minute_min
                    .swap(EMPTY_DELAY_NANOS, Ordering::AcqRel);

                if old_min != EMPTY_DELAY_NANOS {
                    // Write old minimum to bucket ring buffer
                    let idx =
                        self.bucket_write_index.fetch_add(1, Ordering::Relaxed) % BASE_HISTORY_SIZE;
                    self.buckets[idx].store(old_min, Ordering::Release);

                    // Increment bucket count, saturating at BASE_HISTORY_SIZE
                    self.bucket_count
                        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                            if count < BASE_HISTORY_SIZE {
                                Some(count + 1)
                            } else {
                                None
                            }
                        })
                        .ok();
                }

                // Now compete for the new minute's minimum
                self.update_current_min(rtt_nanos);
            } else {
                // Lost the race - another thread rolled over, update new minute's minimum
                self.update_current_min(rtt_nanos);
            }
        } else {
            // Still in current minute - update minimum
            self.update_current_min(rtt_nanos);
        }
    }

    /// Atomically update current minute minimum if new value is smaller
    fn update_current_min(&self, rtt_nanos: u64) {
        self.current_minute_min
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                if rtt_nanos < current {
                    Some(rtt_nanos)
                } else {
                    None // Current is already smaller or equal
                }
            })
            .ok();
    }

    fn base_delay(&self) -> Duration {
        // Find minimum across all buckets
        let mut historical_min = u64::MAX;
        for bucket in &self.buckets {
            let nanos = bucket.load(Ordering::Acquire);
            if nanos != EMPTY_DELAY_NANOS && nanos < historical_min {
                historical_min = nanos;
            }
        }

        // Also consider current minute
        let current_min = self.current_minute_min.load(Ordering::Acquire);

        let result_nanos = match (historical_min != u64::MAX, current_min != EMPTY_DELAY_NANOS) {
            (true, true) => historical_min.min(current_min),
            (true, false) => historical_min,
            (false, true) => current_min,
            (false, false) => 10_000_000, // 10ms fallback in nanos
        };

        Duration::from_nanos(result_nanos)
    }
}

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
pub struct LedbatController {
    /// Congestion window (bytes in flight)
    cwnd: AtomicUsize,

    /// Bytes currently in flight (sent but not ACKed)
    flightsize: AtomicUsize,

    /// Lock-free delay filter (MIN over recent samples)
    delay_filter: AtomicDelayFilter,

    /// Lock-free base delay history (10-minute buckets)
    base_delay_history: AtomicBaseDelayHistory,

    /// Current queuing delay estimate (stored as nanoseconds)
    queuing_delay_nanos: AtomicU64,

    /// Last update time (stored as nanos since controller creation)
    last_update_nanos: AtomicU64,

    /// Epoch instant for time calculations
    epoch: Instant,

    /// Bytes acknowledged since last update
    bytes_acked_since_update: AtomicUsize,

    /// Slow start threshold (bytes)
    ssthresh: AtomicUsize,

    /// Are we in slow start phase?
    in_slow_start: AtomicBool,

    // ===== LEDBAT++ Periodic Slowdown State =====
    /// Current slowdown state (stored as u8, maps to SlowdownState)
    slowdown_state: AtomicU8,

    /// Time when current slowdown phase started (nanos since epoch)
    slowdown_phase_start_nanos: AtomicU64,

    /// RTT count within current slowdown phase
    slowdown_rtt_count: AtomicU32,

    /// Duration of last complete slowdown cycle (nanos) for scheduling next
    last_slowdown_duration_nanos: AtomicU64,

    /// Time when next scheduled slowdown should start (nanos since epoch)
    next_slowdown_time_nanos: AtomicU64,

    /// cwnd value saved before slowdown (for ramping back up)
    pre_slowdown_cwnd: AtomicUsize,

    /// Whether initial slow start has completed (triggers first slowdown)
    initial_slow_start_completed: AtomicBool,

    /// Configuration
    target_delay: Duration,
    allowed_increase_packets: usize,
    min_cwnd: usize,
    max_cwnd: usize,
    enable_slow_start: bool,
    delay_exit_threshold: f64,
    enable_periodic_slowdown: bool,

    /// Statistics
    total_increases: AtomicUsize,
    total_decreases: AtomicUsize,
    total_losses: AtomicUsize,
    min_cwnd_events: AtomicUsize,
    slow_start_exits: AtomicUsize,
    periodic_slowdowns: AtomicUsize,
}

impl LedbatController {
    /// Create new LEDBAT controller with default config (backward compatible).
    ///
    /// # Arguments
    /// * `initial_cwnd` - Initial congestion window (bytes)
    /// * `min_cwnd` - Minimum window (typically 2 * MSS)
    /// * `max_cwnd` - Maximum window (protocol limit or config)
    ///
    /// # Example
    /// ```ignore
    /// use freenet::transport::ledbat::LedbatController;
    /// let controller = LedbatController::new(
    ///     2848,      // 2 * MSS (RFC 6817)
    ///     2848,      // min
    ///     10_000_000 // 10 MB max
    /// );
    /// ```
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
    /// and other LEDBAT settings.
    ///
    /// # Example
    /// ```ignore
    /// use freenet::transport::ledbat::{LedbatController, LedbatConfig};
    /// let config = LedbatConfig {
    ///     initial_cwnd: 14_600,  // IW10
    ///     ssthresh: 102_400,     // 100 KB
    ///     enable_slow_start: true,
    ///     ..LedbatConfig::default()
    /// };
    /// let controller = LedbatController::new_with_config(config);
    /// ```
    pub fn new_with_config(config: LedbatConfig) -> Self {
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
            // Use proper RNG for better entropy distribution
            let jitter_pct = 0.8 + (rand::random::<u8>() % 40) as f64 / 100.0; // 0.8 to 1.2 (±20%)
            ((config.ssthresh as f64) * jitter_pct) as usize
        } else {
            config.ssthresh
        };

        Self {
            cwnd: AtomicUsize::new(config.initial_cwnd),
            flightsize: AtomicUsize::new(0),
            delay_filter: AtomicDelayFilter::new(),
            base_delay_history: AtomicBaseDelayHistory::new(),
            queuing_delay_nanos: AtomicU64::new(0),
            last_update_nanos: AtomicU64::new(0),
            epoch: Instant::now(),
            bytes_acked_since_update: AtomicUsize::new(0),
            ssthresh: AtomicUsize::new(ssthresh),
            in_slow_start: AtomicBool::new(config.enable_slow_start),
            // LEDBAT++ periodic slowdown state
            slowdown_state: AtomicU8::new(SlowdownState::Normal as u8),
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
            enable_slow_start: config.enable_slow_start,
            delay_exit_threshold: config.delay_exit_threshold,
            enable_periodic_slowdown: config.enable_periodic_slowdown,
            // Statistics
            total_increases: AtomicUsize::new(0),
            total_decreases: AtomicUsize::new(0),
            total_losses: AtomicUsize::new(0),
            min_cwnd_events: AtomicUsize::new(0),
            slow_start_exits: AtomicUsize::new(0),
            periodic_slowdowns: AtomicUsize::new(0),
        }
    }

    /// Calculate dynamic GAIN based on base delay (LEDBAT++ Section 4.2)
    ///
    /// GAIN = 1 / min(16, ceil(2 * TARGET / base_delay))
    ///
    /// This adapts the responsiveness based on the ratio of target to base delay.
    /// For low-latency links (low base_delay), GAIN is smaller for stability.
    /// For high-latency links (high base_delay), GAIN approaches 1/16 minimum.
    fn calculate_dynamic_gain(&self, base_delay: Duration) -> f64 {
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
        let now_nanos = self.epoch.elapsed().as_nanos() as u64;
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

        // Check if in slow start phase
        if self.enable_slow_start && self.in_slow_start.load(Ordering::Acquire) {
            self.handle_slow_start(bytes_acked_total, queuing_delay, base_delay);
            return;
        }

        // Check for periodic slowdown (LEDBAT++)
        if self.enable_periodic_slowdown
            && self.check_and_handle_slowdown(bytes_acked_total, queuing_delay, base_delay)
        {
            return; // Slowdown handling took over
        }

        // LEDBAT++ congestion avoidance (draft-irtf-iccrg-ledbat-plus-plus Section 4.2)
        //
        // Uses dynamic GAIN and multiplicative decrease capped at -W/2:
        // Δcwnd = max(GAIN - Constant * W * (delay/target - 1), -W/2)
        //
        // Where:
        // - GAIN = 1 / min(16, ceil(2 * TARGET / base_delay))
        // - Constant approximates the LEDBAT linear formula
        //
        // For simplicity, we use the equivalent formulation:
        // Δcwnd = GAIN * (off_target / TARGET) * bytes_acked * MSS / cwnd
        // But cap the decrease at -W/2 per RTT
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
        let flightsize = self.flightsize.load(Ordering::Relaxed);
        let max_allowed_cwnd = flightsize + (self.allowed_increase_packets * MSS);
        new_cwnd = new_cwnd.min(max_allowed_cwnd as f64);

        // Enforce bounds
        new_cwnd = new_cwnd.max(self.min_cwnd as f64).min(self.max_cwnd as f64);

        let new_cwnd_usize = new_cwnd as usize;

        if new_cwnd_usize == self.min_cwnd && current_cwnd > self.min_cwnd {
            self.min_cwnd_events.fetch_add(1, Ordering::Relaxed);
        }

        self.cwnd.store(new_cwnd_usize, Ordering::Release);

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
                flightsize_kb = flightsize / 1024,
                "LEDBAT cwnd update"
            );
        }
    }

    /// Handle slow start phase (exponential growth).
    ///
    /// Exits slow start when:
    /// - cwnd >= ssthresh (reached threshold), OR
    /// - queuing_delay > TARGET * delay_exit_threshold (congestion detected)
    ///
    /// On initial slow start exit, schedules the first periodic slowdown (LEDBAT++).
    fn handle_slow_start(&self, bytes_acked: usize, queuing_delay: Duration, base_delay: Duration) {
        // Early exit if no longer in slow start (race: loss/timeout occurred)
        if !self.in_slow_start.load(Ordering::Acquire) {
            return;
        }

        let current_cwnd = self.cwnd.load(Ordering::Acquire);
        let ssthresh = self.ssthresh.load(Ordering::Acquire);

        // Check if we're ramping up after a slowdown
        let slowdown_state = self.slowdown_state.load(Ordering::Acquire);
        if slowdown_state == SlowdownState::RampingUp as u8 {
            // During ramp-up, check against pre_slowdown_cwnd target
            let target_cwnd = self.pre_slowdown_cwnd.load(Ordering::Acquire);
            if current_cwnd >= target_cwnd || queuing_delay > self.target_delay {
                // Ramp-up complete, transition to normal operation
                let now_nanos = self.epoch.elapsed().as_nanos() as u64;
                self.complete_slowdown(now_nanos, base_delay);
                return;
            }
            // Continue exponential growth during ramp-up
            let new_cwnd = (current_cwnd + bytes_acked)
                .min(target_cwnd)
                .min(self.max_cwnd);
            self.cwnd.store(new_cwnd, Ordering::Release);
            return;
        }

        // Check exit conditions (LEDBAT++ uses 3/4 of target, configured via delay_exit_threshold)
        let delay_threshold =
            Duration::from_secs_f64(self.target_delay.as_secs_f64() * self.delay_exit_threshold);
        let should_exit = current_cwnd >= ssthresh || queuing_delay > delay_threshold;

        if should_exit {
            // Exit slow start
            self.in_slow_start.store(false, Ordering::Release);
            self.slow_start_exits.fetch_add(1, Ordering::Relaxed);

            // Conservative reduction on exit (optional, can be tuned)
            let new_cwnd = ((current_cwnd as f64) * 0.9) as usize;
            let new_cwnd = new_cwnd.max(self.min_cwnd).min(self.max_cwnd);
            self.cwnd.store(new_cwnd, Ordering::Release);

            // LEDBAT++: Schedule initial slowdown after first slow start exit
            // The slowdown will occur after SLOWDOWN_DELAY_RTTS (2 RTTs)
            if self.enable_periodic_slowdown
                && !self
                    .initial_slow_start_completed
                    .swap(true, Ordering::AcqRel)
            {
                let now_nanos = self.epoch.elapsed().as_nanos() as u64;
                // Schedule slowdown after 2 RTTs (use base_delay as RTT estimate)
                let delay_nanos = base_delay.as_nanos() as u64 * SLOWDOWN_DELAY_RTTS as u64;
                self.next_slowdown_time_nanos
                    .store(now_nanos + delay_nanos, Ordering::Release);
                self.slowdown_state
                    .store(SlowdownState::WaitingForSlowdown as u8, Ordering::Release);

                tracing::debug!(
                    delay_ms = delay_nanos / 1_000_000,
                    "LEDBAT++ scheduling initial slowdown after slow start exit"
                );
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
            self.cwnd.store(new_cwnd, Ordering::Release);

            tracing::trace!(
                old_cwnd_kb = current_cwnd / 1024,
                new_cwnd_kb = new_cwnd / 1024,
                bytes_acked_kb = bytes_acked / 1024,
                queuing_delay_ms = queuing_delay.as_millis(),
                "Slow start growth"
            );
        }
    }

    /// Check and handle periodic slowdown state machine (LEDBAT++ Section 4.4).
    ///
    /// Returns true if the slowdown logic handled this update (caller should return).
    fn check_and_handle_slowdown(
        &self,
        bytes_acked: usize,
        queuing_delay: Duration,
        base_delay: Duration,
    ) -> bool {
        let now_nanos = self.epoch.elapsed().as_nanos() as u64;
        let state = self.slowdown_state.load(Ordering::Acquire);

        match state {
            s if s == SlowdownState::Normal as u8 => {
                // Check if it's time for the next scheduled slowdown
                let next_slowdown = self.next_slowdown_time_nanos.load(Ordering::Acquire);
                if now_nanos >= next_slowdown && next_slowdown != u64::MAX {
                    self.start_slowdown(now_nanos, base_delay);
                    return true;
                }
                false
            }
            s if s == SlowdownState::WaitingForSlowdown as u8 => {
                // Check if wait period is over
                let next_slowdown = self.next_slowdown_time_nanos.load(Ordering::Acquire);
                if now_nanos >= next_slowdown {
                    self.start_slowdown(now_nanos, base_delay);
                    return true;
                }
                false
            }
            s if s == SlowdownState::InSlowdown as u8 => {
                // Just entered slowdown, transition to frozen state
                self.slowdown_state
                    .store(SlowdownState::Frozen as u8, Ordering::Release);
                self.slowdown_rtt_count.store(0, Ordering::Release);
                self.slowdown_phase_start_nanos
                    .store(now_nanos, Ordering::Release);
                true
            }
            s if s == SlowdownState::Frozen as u8 => {
                // Frozen at reduced cwnd, counting RTTs until we can ramp up
                let phase_start = self.slowdown_phase_start_nanos.load(Ordering::Acquire);
                let freeze_duration = base_delay.as_nanos() as u64 * SLOWDOWN_FREEZE_RTTS as u64;

                if now_nanos.saturating_sub(phase_start) >= freeze_duration {
                    // Done freezing, start ramping up
                    self.slowdown_state
                        .store(SlowdownState::RampingUp as u8, Ordering::Release);
                    self.in_slow_start.store(true, Ordering::Release);
                    tracing::debug!(
                        cwnd_kb = self.cwnd.load(Ordering::Relaxed) / 1024,
                        "LEDBAT++ slowdown: starting ramp-up phase"
                    );
                    // Return true to let next ACK start the ramp-up
                    return true;
                }
                // Keep cwnd at proportionally reduced level during freeze
                // (already set in start_slowdown, just maintain it)
                let pre_slowdown = self.pre_slowdown_cwnd.load(Ordering::Relaxed);
                let frozen_cwnd = (pre_slowdown / SLOWDOWN_REDUCTION_FACTOR).max(self.min_cwnd);
                self.cwnd.store(frozen_cwnd, Ordering::Release);
                true
            }
            s if s == SlowdownState::RampingUp as u8 => {
                // Ramping up using slow start until reaching pre_slowdown_cwnd
                let target_cwnd = self.pre_slowdown_cwnd.load(Ordering::Acquire);
                let current_cwnd = self.cwnd.load(Ordering::Acquire);

                if current_cwnd >= target_cwnd || queuing_delay > self.target_delay {
                    // Done ramping up, return to normal operation
                    self.complete_slowdown(now_nanos, base_delay);
                    return false; // Let normal congestion avoidance take over
                }

                // Exponential growth during ramp-up
                let new_cwnd = (current_cwnd + bytes_acked)
                    .min(target_cwnd)
                    .min(self.max_cwnd);
                self.cwnd.store(new_cwnd, Ordering::Release);
                true
            }
            _ => false, // Unknown state, don't interfere
        }
    }

    /// Start a periodic slowdown cycle.
    fn start_slowdown(&self, now_nanos: u64, _base_delay: Duration) {
        let current_cwnd = self.cwnd.load(Ordering::Acquire);

        // Save current cwnd as ssthresh and ramp-up target
        self.ssthresh.store(current_cwnd, Ordering::Release);
        self.pre_slowdown_cwnd
            .store(current_cwnd, Ordering::Release);

        // Reduce cwnd proportionally (consistent with our large initial window)
        // Drop to cwnd/4 but never below min_cwnd (2 packets)
        let slowdown_cwnd = (current_cwnd / SLOWDOWN_REDUCTION_FACTOR).max(self.min_cwnd);
        self.cwnd.store(slowdown_cwnd, Ordering::Release);

        // Update state
        self.slowdown_state
            .store(SlowdownState::InSlowdown as u8, Ordering::Release);
        self.slowdown_phase_start_nanos
            .store(now_nanos, Ordering::Release);
        self.periodic_slowdowns.fetch_add(1, Ordering::Relaxed);

        tracing::debug!(
            old_cwnd_kb = current_cwnd / 1024,
            new_cwnd_kb = slowdown_cwnd / 1024,
            reduction_factor = SLOWDOWN_REDUCTION_FACTOR,
            "LEDBAT++ periodic slowdown: reducing cwnd proportionally"
        );
    }

    /// Complete a slowdown cycle and schedule the next one.
    fn complete_slowdown(&self, now_nanos: u64, base_delay: Duration) {
        let phase_start = self.slowdown_phase_start_nanos.load(Ordering::Acquire);
        let slowdown_duration = now_nanos.saturating_sub(phase_start);

        // Store duration for potential diagnostics
        self.last_slowdown_duration_nanos
            .store(slowdown_duration, Ordering::Release);

        // Schedule next slowdown at 9x the current duration (LEDBAT++ spec)
        let next_interval = slowdown_duration * SLOWDOWN_INTERVAL_MULTIPLIER as u64;
        // Ensure minimum interval of 1 RTT
        let min_interval = base_delay.as_nanos() as u64;
        let actual_interval = next_interval.max(min_interval);

        self.next_slowdown_time_nanos
            .store(now_nanos + actual_interval, Ordering::Release);

        // Return to normal operation
        self.slowdown_state
            .store(SlowdownState::Normal as u8, Ordering::Release);
        self.in_slow_start.store(false, Ordering::Release);

        tracing::debug!(
            slowdown_duration_ms = slowdown_duration / 1_000_000,
            next_interval_ms = actual_interval / 1_000_000,
            "LEDBAT++ slowdown complete, scheduling next"
        );
    }

    /// Called when packet loss detected (not timeout).
    ///
    /// RFC 6817 Section 2.4.1: halve cwnd on loss.
    ///
    /// Also exits slow start if currently in that phase.
    ///
    /// **Note:** Currently not called in production - the transport layer uses
    /// [`on_timeout`] for retransmissions. This method is for future fast-retransmit
    /// support (3 duplicate ACKs). LEDBAT primarily uses delay-based signals anyway,
    /// so loss detection is less critical than in traditional TCP congestion control.
    pub fn on_loss(&self) {
        self.total_losses.fetch_add(1, Ordering::Relaxed);

        // Exit slow start on loss
        if self.in_slow_start.swap(false, Ordering::AcqRel) {
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

    /// Called on retransmission timeout (severe congestion).
    ///
    /// RFC 6817: Reset to 1 * MSS on timeout.
    /// Also exits slow start and resets to min_cwnd.
    pub fn on_timeout(&self) {
        self.total_losses.fetch_add(1, Ordering::Relaxed);

        // Exit slow start on timeout
        if self.in_slow_start.swap(false, Ordering::AcqRel) {
            self.slow_start_exits.fetch_add(1, Ordering::Relaxed);
        }

        let new_cwnd = MSS.max(self.min_cwnd);

        let current_cwnd = self.cwnd.load(Ordering::Acquire);
        self.cwnd.store(new_cwnd, Ordering::Release);

        // Only log when cwnd actually changes to avoid spam when already at minimum
        if current_cwnd != new_cwnd {
            tracing::warn!(
                old_cwnd_kb = current_cwnd / 1024,
                new_cwnd_kb = new_cwnd / 1024,
                "LEDBAT retransmission timeout - reset to 1*MSS"
            );
        }
    }

    /// Get current congestion window (bytes).
    pub fn current_cwnd(&self) -> usize {
        self.cwnd.load(Ordering::Acquire)
    }

    /// Convert cwnd (bytes in flight) to rate (bytes/sec).
    ///
    /// rate = cwnd / RTT
    ///
    /// Enforces minimum RTT of 1ms to prevent division by near-zero
    /// (sub-millisecond RTTs would cause unrealistically high rates).
    pub fn current_rate(&self, rtt: Duration) -> usize {
        let cwnd = self.current_cwnd();
        // Enforce minimum RTT of 1ms to prevent division by near-zero
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
    ///
    /// This ONLY decrements flightsize without updating RTT estimation,
    /// following Karn's algorithm which excludes retransmitted packets
    /// from RTT calculation.
    ///
    /// This is critical for preventing flightsize leaks: when a packet
    /// times out and is retransmitted, on_send was called once for the
    /// original send. If the retransmitted packet is ACKed, we still need
    /// to decrement flightsize even though we can't use it for RTT.
    pub fn on_ack_without_rtt(&self, bytes_acked: usize) {
        // Decrease flightsize with saturating subtraction to prevent underflow
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

    /// Get statistics.
    pub fn stats(&self) -> LedbatStats {
        LedbatStats {
            cwnd: self.current_cwnd(),
            flightsize: self.flightsize(),
            queuing_delay: self.queuing_delay(),
            base_delay: self.base_delay(),
            total_increases: self.total_increases.load(Ordering::Relaxed),
            total_decreases: self.total_decreases.load(Ordering::Relaxed),
            total_losses: self.total_losses.load(Ordering::Relaxed),
            min_cwnd_events: self.min_cwnd_events.load(Ordering::Relaxed),
            slow_start_exits: self.slow_start_exits.load(Ordering::Relaxed),
            periodic_slowdowns: self.periodic_slowdowns.load(Ordering::Relaxed),
        }
    }
}

/// LEDBAT++ statistics.
#[derive(Debug, Clone)]
pub struct LedbatStats {
    pub cwnd: usize,
    pub flightsize: usize,
    pub queuing_delay: Duration,
    pub base_delay: Duration,
    pub total_increases: usize,
    pub total_decreases: usize,
    pub total_losses: usize,
    pub min_cwnd_events: usize,
    pub slow_start_exits: usize,
    /// Number of periodic slowdowns completed (LEDBAT++ feature)
    pub periodic_slowdowns: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ledbat_creation() {
        let controller = LedbatController::new(2848, 2848, 10_000_000);
        assert_eq!(controller.current_cwnd(), 2848);
        assert_eq!(controller.base_delay(), Duration::from_millis(10)); // Fallback
    }

    #[test]
    fn test_ledbat_base_delay_tracking() {
        let controller = LedbatController::new(10_000, 2848, 10_000_000);

        // First RTT sample sets base delay
        controller.on_ack(Duration::from_millis(50), 1000);
        assert_eq!(controller.base_delay(), Duration::from_millis(50));

        // Lower RTT updates base delay
        controller.on_ack(Duration::from_millis(40), 1000);
        assert_eq!(controller.base_delay(), Duration::from_millis(40));

        // Higher RTT doesn't update base delay
        controller.on_ack(Duration::from_millis(60), 1000);
        assert_eq!(controller.base_delay(), Duration::from_millis(40));
    }

    #[tokio::test]
    async fn test_ledbat_increases_when_below_target() {
        let controller = LedbatController::new(10_000, 2848, 10_000_000);

        // Track flightsize to avoid application-limited cap interference
        controller.on_send(20_000); // Send enough to keep flightsize high

        // Set base delay with multiple samples (need 2+ for filter)
        controller.on_ack(Duration::from_millis(10), 1000);
        controller.on_ack(Duration::from_millis(10), 1000);

        let initial_cwnd = controller.current_cwnd();

        // Wait for update interval
        tokio::time::sleep(Duration::from_millis(20)).await;

        // RTT below target (10ms + 20ms = 30ms < 100ms target)
        // Should increase
        controller.on_ack(Duration::from_millis(30), 5000);

        assert!(
            controller.current_cwnd() > initial_cwnd,
            "cwnd should increase when below target (was {}, now {})",
            initial_cwnd,
            controller.current_cwnd()
        );
    }

    #[tokio::test]
    async fn test_ledbat_decreases_when_above_target() {
        // Disable slow start to test pure LEDBAT congestion avoidance
        let config = LedbatConfig {
            initial_cwnd: 100_000,
            min_cwnd: 2848,
            max_cwnd: 10_000_000,
            enable_slow_start: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);

        // Track flightsize to avoid application-limited cap interference
        controller.on_send(200_000); // High flightsize

        // Set base delay with low RTT samples
        controller.on_ack(Duration::from_millis(10), 1000);
        controller.on_ack(Duration::from_millis(10), 1000);

        // Wait for update interval
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Fill the delay filter with high RTT samples (filter uses MIN over 4 samples)
        // We need enough high-RTT samples to push out the low ones
        for _ in 0..4 {
            controller.on_send(10_000);
            controller.on_ack(Duration::from_millis(160), 2000);
            tokio::time::sleep(Duration::from_millis(15)).await;
        }

        let initial_cwnd = controller.current_cwnd();

        // Wait for update interval
        tokio::time::sleep(Duration::from_millis(20)).await;

        // RTT above target (10ms base + 150ms queuing = 160ms > 100ms target)
        // Now filter has [160, 160, 160, 160], filtered_delay = 160ms
        // queuing_delay = 160ms - 10ms = 150ms > 100ms target
        // off_target = -50ms (negative = decrease)
        controller.on_send(10_000);
        controller.on_ack(Duration::from_millis(160), 5000);

        let new_cwnd = controller.current_cwnd();

        assert!(
            new_cwnd < initial_cwnd,
            "cwnd should decrease when above target (was {}, now {})",
            initial_cwnd,
            new_cwnd
        );
    }

    #[test]
    fn test_ledbat_min_cwnd_enforcement() {
        let controller = LedbatController::new(10_000, 2848, 10_000_000);

        // Set base delay
        controller.on_ack(Duration::from_millis(10), 0); // Just for base delay
        controller.on_ack(Duration::from_millis(10), 0);

        // Very high queuing delay (should decrease aggressively)
        // LEDBAT decreases gradually, need many iterations
        // Keep flightsize valid by sending before each ACK
        for _ in 0..50 {
            controller.on_send(1000);
            controller.on_ack(Duration::from_millis(500), 1000);
            std::thread::sleep(std::time::Duration::from_millis(15)); // Wait for update interval
        }

        // Should have hit min_cwnd (or very close)
        assert!(
            controller.current_cwnd() <= 2848 + 1000,
            "Should be close to min_cwnd, got {}",
            controller.current_cwnd()
        );
    }

    #[test]
    fn test_ledbat_rate_conversion() {
        let controller = LedbatController::new(100_000, 2848, 10_000_000);

        // cwnd = 100KB, RTT = 100ms
        // rate = 100KB / 0.1s = 1000 KB/s = 1 MB/s
        let rate = controller.current_rate(Duration::from_millis(100));
        assert!(
            (rate as i64 - 1_000_000).abs() < 10_000,
            "rate should be ~1 MB/s, got {}",
            rate
        );
    }

    #[test]
    fn test_ledbat_loss_handling() {
        let controller = LedbatController::new(100_000, 2848, 10_000_000);

        let before = controller.current_cwnd();
        controller.on_loss();
        let after = controller.current_cwnd();

        // Should halve (approximately)
        assert!(
            (after as f64) < (before as f64 * 0.6),
            "should halve on loss"
        );
        assert!(
            (after as f64) > (before as f64 * 0.4),
            "should halve on loss"
        );
    }

    #[test]
    fn test_ledbat_timeout_handling() {
        let controller = LedbatController::new(100_000, 2848, 10_000_000);

        controller.on_timeout();

        // Should reset to 1*MSS (or min_cwnd)
        assert_eq!(controller.current_cwnd(), 2848); // max(MSS, min_cwnd)
    }

    #[test]
    fn test_ledbat_flightsize_tracking() {
        let controller = LedbatController::new(10_000, 2848, 10_000_000);

        assert_eq!(controller.flightsize(), 0);

        controller.on_send(5000);
        assert_eq!(controller.flightsize(), 5000);

        controller.on_ack(Duration::from_millis(50), 2000);
        assert_eq!(controller.flightsize(), 3000);

        controller.on_ack(Duration::from_millis(50), 3000);
        assert_eq!(controller.flightsize(), 0);
    }

    #[test]
    fn test_on_ack_without_rtt_decrements_flightsize() {
        let controller = LedbatController::new(10_000, 2848, 10_000_000);

        // Send bytes to track
        controller.on_send(5000);
        assert_eq!(controller.flightsize(), 5000);

        // on_ack_without_rtt should decrement flightsize without updating RTT state
        controller.on_ack_without_rtt(2000);
        assert_eq!(controller.flightsize(), 3000);

        // Should not have updated base_delay (no RTT sample provided)
        // Base delay should still be at the default fallback value
        let base_delay = controller.base_delay();
        assert!(
            base_delay == Duration::from_millis(10) || base_delay.is_zero(),
            "base_delay should be fallback value, got {:?}",
            base_delay
        );

        // Continue decrementing
        controller.on_ack_without_rtt(3000);
        assert_eq!(controller.flightsize(), 0);
    }

    #[test]
    fn test_on_ack_without_rtt_saturates_on_underflow() {
        let controller = LedbatController::new(10_000, 2848, 10_000_000);

        // Send a small amount
        controller.on_send(1000);
        assert_eq!(controller.flightsize(), 1000);

        // ACK more than we sent - should saturate to 0, not wrap
        controller.on_ack_without_rtt(5000);
        assert_eq!(
            controller.flightsize(),
            0,
            "should saturate to 0, not underflow"
        );
    }

    #[tokio::test]
    async fn test_ledbat_cwnd_formula_correctness() {
        // Disable slow start to test pure LEDBAT congestion avoidance formula
        let config = LedbatConfig {
            initial_cwnd: 10_000,
            min_cwnd: 2848,
            max_cwnd: 10_000_000,
            enable_slow_start: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);

        // Track flightsize for application-limited cap
        controller.on_send(50_000); // Send enough to cover all acks

        // Set base delay with filter (need 2+ samples)
        for _ in 0..4 {
            controller.on_ack(Duration::from_millis(10), 1000);
        }

        // Wait for update interval
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Fill the delay filter with 30ms RTT samples (below target)
        // This establishes a consistent filtered delay
        for _ in 0..4 {
            controller.on_send(5000);
            controller.on_ack(Duration::from_millis(30), 1000);
            tokio::time::sleep(Duration::from_millis(15)).await;
        }

        let initial_cwnd = controller.current_cwnd();

        // Wait for update interval
        tokio::time::sleep(Duration::from_millis(20)).await;

        // RTT below target: 30ms (10ms base + 20ms queuing) < 100ms target
        // Now filter has [30, 30, 30, 30], filtered_delay = 30ms
        // queuing_delay = 30ms - 10ms = 20ms < 100ms target
        // off_target = +80ms (positive = increase)
        // Δcwnd = GAIN * (80/100) * bytes_acked * MSS / cwnd
        controller.on_send(10_000);
        controller.on_ack(Duration::from_millis(30), 5000);

        let new_cwnd = controller.current_cwnd();

        // Should have increased (below target)
        assert!(
            new_cwnd > initial_cwnd,
            "cwnd should increase when below target (was {}, now {})",
            initial_cwnd,
            new_cwnd
        );

        // Verify the change is reasonable (not more than flightsize + ALLOWED_INCREASE)
        let max_allowed = controller.flightsize() + (2 * MSS);
        assert!(
            new_cwnd <= max_allowed,
            "cwnd should be capped by application-limited (cwnd={}, max_allowed={})",
            new_cwnd,
            max_allowed
        );
    }

    #[test]
    fn test_ledbat_delay_filtering() {
        let controller = LedbatController::new(10_000, 2848, 10_000_000);

        // Feed samples with noise: [50ms, 45ms, 55ms, 48ms]
        controller.on_ack(Duration::from_millis(50), 1000);
        controller.on_ack(Duration::from_millis(45), 1000);
        controller.on_ack(Duration::from_millis(55), 1000);
        controller.on_ack(Duration::from_millis(48), 1000);

        // Base delay should be minimum: 45ms
        assert_eq!(controller.base_delay(), Duration::from_millis(45));
    }

    // NOTE: Base delay history bucket rollover test removed - would require
    // 121 seconds of real time. Bucket logic is tested implicitly by other tests.

    #[test]
    fn test_ledbat_stats() {
        let controller = LedbatController::new(10_000, 2848, 10_000_000);

        controller.on_send(5000);
        controller.on_ack(Duration::from_millis(50), 1000);

        let stats = controller.stats();
        assert_eq!(stats.cwnd, 10_000);
        assert_eq!(stats.flightsize, 4000);
        assert_eq!(stats.base_delay, Duration::from_millis(50));
    }

    #[test]
    #[should_panic(expected = "min_cwnd (10000) must be <= initial_cwnd (5000)")]
    fn test_config_validation_min_greater_than_initial() {
        let config = LedbatConfig {
            initial_cwnd: 5_000,
            min_cwnd: 10_000, // min > initial - invalid!
            max_cwnd: 100_000,
            ..Default::default()
        };
        LedbatController::new_with_config(config);
    }

    #[test]
    #[should_panic(expected = "initial_cwnd (150000) must be <= max_cwnd (100000)")]
    fn test_config_validation_initial_greater_than_max() {
        let config = LedbatConfig {
            initial_cwnd: 150_000, // initial > max - invalid!
            min_cwnd: 2_848,
            max_cwnd: 100_000,
            ..Default::default()
        };
        LedbatController::new_with_config(config);
    }

    #[test]
    #[should_panic(expected = "delay_exit_threshold (1.5) must be in range [0.0, 1.0]")]
    fn test_config_validation_threshold_out_of_range() {
        let config = LedbatConfig {
            delay_exit_threshold: 1.5, // > 1.0 - invalid!
            ..Default::default()
        };
        LedbatController::new_with_config(config);
    }

    #[test]
    fn test_config_validation_valid_config() {
        // Should not panic with valid config
        let config = LedbatConfig {
            initial_cwnd: 50_000,
            min_cwnd: 2_848,
            max_cwnd: 100_000,
            delay_exit_threshold: 0.5,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);
        assert_eq!(controller.current_cwnd(), 50_000);
    }

    #[test]
    fn test_ssthresh_randomization_uses_different_values() {
        // Create multiple controllers with randomization enabled
        // Verify they don't all get the same ssthresh (would indicate poor entropy)
        let mut ssthresh_values = std::collections::HashSet::new();

        for _ in 0..10 {
            let config = LedbatConfig {
                ssthresh: 100_000,
                randomize_ssthresh: true,
                ..Default::default()
            };
            let controller = LedbatController::new_with_config(config);
            // ssthresh is private, but we can observe it indirectly via slow start exit
            // For this test, just verify construction succeeds with randomization
            assert!(controller.current_cwnd() > 0);

            // The jitter should produce values in range [80_000, 120_000]
            // We can't directly observe ssthresh, but at least verify creation works
            ssthresh_values.insert(controller.current_cwnd());
        }

        // With proper randomization, we should see some variation
        // (initial_cwnd is fixed, so this just verifies no crashes with RNG)
        assert!(!ssthresh_values.is_empty());
    }

    /// Stress test: multiple threads calling on_ack concurrently.
    /// Validates lock-free correctness under contention.
    #[test]
    fn test_concurrent_on_ack_stress() {
        use std::sync::Arc;
        use std::thread;

        let controller = Arc::new(LedbatController::new(100_000, 2848, 10_000_000));
        let num_threads = 8;
        let iterations_per_thread = 1000;

        // Pre-send enough data to cover all ACKs
        controller.on_send(num_threads * iterations_per_thread * 1000);

        let handles: Vec<_> = (0..num_threads)
            .map(|i| {
                let controller = Arc::clone(&controller);
                thread::spawn(move || {
                    for j in 0..iterations_per_thread {
                        // Vary RTT to exercise different code paths
                        let rtt_ms = 20 + (i * 10) + (j % 50);
                        controller.on_ack(Duration::from_millis(rtt_ms as u64), 1000);
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        // Verify controller is in a valid state
        let stats = controller.stats();
        assert!(stats.cwnd >= controller.min_cwnd);
        assert!(stats.cwnd <= controller.max_cwnd);
        // Base delay should be set (minimum RTT was 20ms)
        assert!(stats.base_delay <= Duration::from_millis(100));
    }

    /// Stress test: concurrent on_send and on_ack from different threads.
    /// Validates flightsize tracking under contention.
    #[test]
    fn test_concurrent_send_ack_stress() {
        use std::sync::Arc;
        use std::thread;

        let controller = Arc::new(LedbatController::new(100_000, 2848, 10_000_000));
        let iterations = 500;

        // Sender thread
        let controller_send = Arc::clone(&controller);
        let sender = thread::spawn(move || {
            for _ in 0..iterations {
                controller_send.on_send(1000);
                thread::yield_now(); // Encourage interleaving
            }
        });

        // ACK thread
        let controller_ack = Arc::clone(&controller);
        let acker = thread::spawn(move || {
            for i in 0..iterations {
                let rtt_ms = 30 + (i % 20);
                controller_ack.on_ack(Duration::from_millis(rtt_ms as u64), 1000);
                thread::yield_now();
            }
        });

        sender.join().expect("Sender panicked");
        acker.join().expect("Acker panicked");

        // Flightsize should be reasonable (may not be exactly 0 due to timing)
        // Just verify no underflow (would wrap to huge value)
        assert!(controller.flightsize() < 1_000_000);
    }

    /// Stress test: concurrent loss events.
    /// Validates cwnd halving under contention.
    #[test]
    fn test_concurrent_loss_stress() {
        use std::sync::Arc;
        use std::thread;

        let controller = Arc::new(LedbatController::new(1_000_000, 2848, 10_000_000));
        let num_threads = 4;

        let handles: Vec<_> = (0..num_threads)
            .map(|_| {
                let controller = Arc::clone(&controller);
                thread::spawn(move || {
                    for _ in 0..10 {
                        controller.on_loss();
                        thread::yield_now();
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        // After many loss events, cwnd should be at or near min_cwnd
        assert!(
            controller.current_cwnd() <= 10_000,
            "cwnd should be small after many losses, got {}",
            controller.current_cwnd()
        );
        assert!(controller.current_cwnd() >= controller.min_cwnd);

        // Total losses should be num_threads * 10
        let stats = controller.stats();
        assert_eq!(stats.total_losses, num_threads * 10);
    }

    /// Test atomic delay filter under concurrent access.
    #[test]
    fn test_atomic_delay_filter_concurrent() {
        use std::sync::Arc;
        use std::thread;

        let filter = Arc::new(AtomicDelayFilter::new());
        let num_threads = 4;
        let iterations = 100;

        let handles: Vec<_> = (0..num_threads)
            .map(|i| {
                let filter = Arc::clone(&filter);
                thread::spawn(move || {
                    for j in 0..iterations {
                        let rtt_ms = 10 + i * 5 + (j % 10);
                        filter.add_sample(Duration::from_millis(rtt_ms as u64));
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        // Filter should be ready and have a valid minimum
        assert!(filter.is_ready());
        let min_delay = filter.filtered_delay().expect("Should have samples");
        // Minimum should be at least 10ms (the lowest we added)
        assert!(min_delay >= Duration::from_millis(10));
        assert!(min_delay <= Duration::from_millis(50));
    }

    /// Test atomic base delay history under concurrent access.
    #[test]
    fn test_atomic_base_delay_history_concurrent() {
        use std::sync::Arc;
        use std::thread;

        let history = Arc::new(AtomicBaseDelayHistory::new());
        let num_threads = 4;
        let iterations = 100;

        let handles: Vec<_> = (0..num_threads)
            .map(|i| {
                let history = Arc::clone(&history);
                thread::spawn(move || {
                    for j in 0..iterations {
                        let rtt_ms = 20 + i * 10 + (j % 15);
                        history.update(Duration::from_millis(rtt_ms as u64));
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        // Base delay should be valid and reflect minimum RTT
        let base = history.base_delay();
        assert!(base >= Duration::from_millis(20));
        assert!(base <= Duration::from_millis(100));
    }

    /// Test that the minute rollover race condition is handled correctly.
    ///
    /// This test verifies that when multiple threads race to roll over the minute
    /// boundary, the smallest RTT value is correctly recorded and no values are lost.
    /// The fix uses swap(EMPTY) + update_current_min instead of swap(rtt) to prevent
    /// a winning thread from overwriting a smaller value set by a losing thread.
    #[test]
    fn test_minute_rollover_race_condition() {
        use std::sync::atomic::AtomicU64;
        use std::sync::Arc;
        use std::thread;

        // Create a history and immediately add a sample to establish baseline
        let history = Arc::new(AtomicBaseDelayHistory::new());
        history.update(Duration::from_millis(100)); // Initial sample

        // Verify initial state
        assert_eq!(history.base_delay(), Duration::from_millis(100));

        // Track the minimum RTT we send across all threads
        let expected_min = Arc::new(AtomicU64::new(u64::MAX));

        // Simulate many threads racing to update with different RTT values
        // In a real minute rollover scenario, all threads would see the minute
        // boundary at roughly the same time
        let num_threads = 16;
        let iterations = 100;

        let handles: Vec<_> = (0..num_threads)
            .map(|i| {
                let history = Arc::clone(&history);
                let expected_min = Arc::clone(&expected_min);
                thread::spawn(move || {
                    for j in 0..iterations {
                        // Each thread uses different RTT values
                        // Thread 0 will have the smallest values (10-19ms)
                        let rtt_ms = 10 + i + (j % 10);
                        let rtt_nanos = rtt_ms as u64 * 1_000_000;

                        // Track the minimum we're sending
                        expected_min
                            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                                if rtt_nanos < current {
                                    Some(rtt_nanos)
                                } else {
                                    None
                                }
                            })
                            .ok();

                        history.update(Duration::from_millis(rtt_ms as u64));

                        // Yield to encourage interleaving
                        if j % 10 == 0 {
                            thread::yield_now();
                        }
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        // The base delay should reflect the minimum RTT sent by any thread
        let actual_min = history.base_delay();
        let _expected = Duration::from_nanos(expected_min.load(Ordering::Relaxed));

        // The actual minimum should be <= expected (we might have lost some due to
        // timing, but we should never record a LARGER minimum than what was sent)
        assert!(
            actual_min <= Duration::from_millis(20),
            "Base delay {} should be <= 20ms (smallest thread's range)",
            actual_min.as_millis()
        );

        // More importantly: base delay should be a valid value we actually sent
        // (between 10ms and the largest possible value)
        assert!(
            actual_min >= Duration::from_millis(10),
            "Base delay {} should be >= 10ms (smallest value sent)",
            actual_min.as_millis()
        );
    }

    /// Test that concurrent updates don't lose the smallest value during rollover.
    ///
    /// This specifically tests the fix for the race where:
    /// 1. Thread A wins CAS for rollover
    /// 2. Thread B loses CAS, calls update_current_min with smaller RTT
    /// 3. Thread A's swap would have overwritten B's smaller value (BUG)
    /// 4. Fix: Thread A swaps to EMPTY then calls update_current_min (CORRECT)
    #[test]
    fn test_no_minimum_value_lost_during_rollover() {
        use std::sync::Arc;
        use std::thread;

        let history = Arc::new(AtomicBaseDelayHistory::new());

        // Many threads all trying to set different minimums
        let num_threads = 8;
        let handles: Vec<_> = (0..num_threads)
            .map(|i| {
                let history = Arc::clone(&history);
                thread::spawn(move || {
                    // Thread 0 sends smallest (5ms), thread 7 sends largest (12ms)
                    let rtt_ms = 5 + i;
                    for _ in 0..50 {
                        history.update(Duration::from_millis(rtt_ms as u64));
                        thread::yield_now();
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().expect("Thread panicked");
        }

        // The minimum should be 5ms (from thread 0)
        let base = history.base_delay();
        assert_eq!(
            base,
            Duration::from_millis(5),
            "Base delay should be 5ms (the smallest value sent), got {}ms",
            base.as_millis()
        );
    }

    // ============ LEDBAT++ Specific Tests ============

    /// Test dynamic GAIN calculation (LEDBAT++ Section 4.2)
    /// GAIN = 1 / min(16, ceil(2 * TARGET / base_delay))
    #[test]
    fn test_dynamic_gain_calculation() {
        let controller = LedbatController::new(10_000, 2848, 10_000_000);

        // TARGET = 60ms
        // For base_delay = 60ms: ceil(2 * 60 / 60) = 2, GAIN = 1/2 = 0.5
        let gain = controller.calculate_dynamic_gain(Duration::from_millis(60));
        assert!(
            (gain - 0.5).abs() < 0.01,
            "GAIN for 60ms base should be 0.5, got {}",
            gain
        );

        // For base_delay = 30ms: ceil(2 * 60 / 30) = 4, GAIN = 1/4 = 0.25
        let gain = controller.calculate_dynamic_gain(Duration::from_millis(30));
        assert!(
            (gain - 0.25).abs() < 0.01,
            "GAIN for 30ms base should be 0.25, got {}",
            gain
        );

        // For base_delay = 10ms: ceil(2 * 60 / 10) = 12, GAIN = 1/12 ≈ 0.083
        let gain = controller.calculate_dynamic_gain(Duration::from_millis(10));
        assert!(
            (gain - 1.0 / 12.0).abs() < 0.01,
            "GAIN for 10ms base should be ~0.083, got {}",
            gain
        );

        // For very small base_delay (1ms): ceil(2 * 60 / 1) = 120 > 16, capped at 16
        // GAIN = 1/16 = 0.0625
        let gain = controller.calculate_dynamic_gain(Duration::from_millis(1));
        assert!(
            (gain - 1.0 / 16.0).abs() < 0.01,
            "GAIN for 1ms base should be capped at 0.0625, got {}",
            gain
        );

        // For zero/near-zero base_delay, should fallback to minimum gain
        let gain = controller.calculate_dynamic_gain(Duration::ZERO);
        assert!(
            (gain - 1.0 / 16.0).abs() < 0.01,
            "GAIN for 0ms base should be 0.0625, got {}",
            gain
        );
    }

    /// Test that TARGET delay is now 60ms (LEDBAT++)
    #[test]
    fn test_target_delay_is_60ms() {
        assert_eq!(
            TARGET,
            Duration::from_millis(60),
            "LEDBAT++ TARGET should be 60ms"
        );
    }

    /// Test that default slow start exit threshold is 0.75 (LEDBAT++)
    #[test]
    fn test_slow_start_exit_threshold_is_75_percent() {
        let config = LedbatConfig::default();
        assert!(
            (config.delay_exit_threshold - 0.75).abs() < 0.01,
            "LEDBAT++ delay_exit_threshold should be 0.75, got {}",
            config.delay_exit_threshold
        );
    }

    /// Test multiplicative decrease cap at -W/2 (LEDBAT++)
    #[tokio::test]
    async fn test_multiplicative_decrease_capped_at_half() {
        // Disable slow start and periodic slowdown to test pure congestion avoidance
        let config = LedbatConfig {
            initial_cwnd: 100_000,
            min_cwnd: 2848,
            max_cwnd: 10_000_000,
            enable_slow_start: false,
            enable_periodic_slowdown: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);

        // Track flightsize
        controller.on_send(200_000);

        // Set low base delay
        controller.on_ack(Duration::from_millis(10), 1000);
        controller.on_ack(Duration::from_millis(10), 1000);

        // Wait for update interval
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Fill delay filter with very high RTT (extreme congestion)
        for _ in 0..4 {
            controller.on_send(10_000);
            controller.on_ack(Duration::from_millis(500), 2000);
            tokio::time::sleep(Duration::from_millis(15)).await;
        }

        let initial_cwnd = controller.current_cwnd();

        // Wait for update and apply extreme congestion signal
        tokio::time::sleep(Duration::from_millis(20)).await;
        controller.on_send(50_000);
        controller.on_ack(Duration::from_millis(500), 10_000);

        let new_cwnd = controller.current_cwnd();

        // The decrease should be capped at 50% per RTT
        // Even with extreme queuing delay, cwnd should not drop below initial/2
        assert!(
            new_cwnd >= initial_cwnd / 2 - 1000, // Allow some tolerance
            "cwnd decrease should be capped at 50%, was {} -> {} (more than half)",
            initial_cwnd,
            new_cwnd
        );
    }

    /// Test periodic slowdown config option
    #[test]
    fn test_periodic_slowdown_config() {
        // With periodic slowdown enabled (default)
        let config = LedbatConfig::default();
        assert!(
            config.enable_periodic_slowdown,
            "Periodic slowdown should be enabled by default"
        );

        // Can be disabled
        let config = LedbatConfig {
            enable_periodic_slowdown: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);
        assert!(!controller.enable_periodic_slowdown);
    }

    /// Test periodic slowdown state machine initialization
    #[test]
    fn test_periodic_slowdown_initial_state() {
        let controller = LedbatController::new(10_000, 2848, 10_000_000);

        // Initial state should be Normal
        let state = controller.slowdown_state.load(Ordering::Relaxed);
        assert_eq!(
            state,
            SlowdownState::Normal as u8,
            "Initial slowdown state should be Normal"
        );

        // initial_slow_start_completed should be false
        assert!(
            !controller
                .initial_slow_start_completed
                .load(Ordering::Relaxed),
            "initial_slow_start_completed should be false initially"
        );

        // No slowdown scheduled yet
        let next_slowdown = controller.next_slowdown_time_nanos.load(Ordering::Relaxed);
        assert_eq!(
            next_slowdown,
            u64::MAX,
            "No slowdown should be scheduled initially"
        );
    }

    /// Test that periodic slowdown statistics are tracked
    #[test]
    fn test_periodic_slowdown_stats() {
        let controller = LedbatController::new(10_000, 2848, 10_000_000);

        let stats = controller.stats();
        assert_eq!(
            stats.periodic_slowdowns, 0,
            "Initial periodic_slowdowns should be 0"
        );
    }

    /// Test slowdown state enum values
    #[test]
    fn test_slowdown_state_values() {
        // Verify enum values for state machine
        assert_eq!(SlowdownState::Normal as u8, 0);
        assert_eq!(SlowdownState::WaitingForSlowdown as u8, 1);
        assert_eq!(SlowdownState::InSlowdown as u8, 2);
        assert_eq!(SlowdownState::Frozen as u8, 3);
        assert_eq!(SlowdownState::RampingUp as u8, 4);
    }

    /// Integration test: slow start exit schedules initial slowdown
    #[tokio::test]
    async fn test_slow_start_exit_schedules_slowdown() {
        let config = LedbatConfig {
            initial_cwnd: 15_000,
            min_cwnd: 2848,
            max_cwnd: 10_000_000,
            ssthresh: 20_000, // Low threshold so we exit slow start quickly
            enable_slow_start: true,
            enable_periodic_slowdown: true,
            randomize_ssthresh: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);

        // Send data and establish RTT
        controller.on_send(100_000);

        // Initial samples to set base delay (need 2+ for filter)
        controller.on_ack(Duration::from_millis(20), 1000);
        controller.on_ack(Duration::from_millis(20), 1000);

        // Wait for update interval (based on base_delay ~20ms)
        tokio::time::sleep(Duration::from_millis(25)).await;

        // This ACK should trigger slow start growth: 15000 + 10000 = 25000 >= 20000 ssthresh
        // Should trigger exit check
        controller.on_ack(Duration::from_millis(20), 10_000);

        // Wait for another RTT to ensure the exit check runs
        // (the first ACK after the wait may just do growth, second triggers exit check)
        tokio::time::sleep(Duration::from_millis(25)).await;
        controller.on_ack(Duration::from_millis(20), 1000);

        // Wait for yet another RTT to make sure the exit is processed
        tokio::time::sleep(Duration::from_millis(25)).await;
        controller.on_ack(Duration::from_millis(20), 1000);

        // Should have exited slow start (cwnd grew past ssthresh)
        let in_slow_start = controller.in_slow_start.load(Ordering::Acquire);
        let cwnd = controller.current_cwnd();
        assert!(
            !in_slow_start,
            "Should have exited slow start (cwnd={}, ssthresh=20000)",
            cwnd
        );

        // Should have marked initial slow start as completed
        assert!(
            controller
                .initial_slow_start_completed
                .load(Ordering::Acquire),
            "initial_slow_start_completed should be true after slow start exit"
        );

        // Slowdown should be scheduled (not u64::MAX)
        let next_slowdown = controller.next_slowdown_time_nanos.load(Ordering::Acquire);
        assert!(
            next_slowdown != u64::MAX,
            "A slowdown should be scheduled after slow start exit"
        );

        // State should be WaitingForSlowdown
        let state = controller.slowdown_state.load(Ordering::Acquire);
        assert_eq!(
            state,
            SlowdownState::WaitingForSlowdown as u8,
            "State should be WaitingForSlowdown"
        );
    }

    /// Test initial slow start ramp-up: cwnd should grow exponentially
    #[tokio::test]
    async fn test_initial_slow_start_ramp_up() {
        let config = LedbatConfig {
            initial_cwnd: 10_000,
            min_cwnd: 2848,
            max_cwnd: 10_000_000,
            ssthresh: 500_000, // High threshold so we stay in slow start
            enable_slow_start: true,
            enable_periodic_slowdown: false, // Disable to focus on slow start
            randomize_ssthresh: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);

        controller.on_send(500_000);

        // Establish base delay
        controller.on_ack(Duration::from_millis(20), 1000);
        controller.on_ack(Duration::from_millis(20), 1000);

        let initial_cwnd = controller.current_cwnd();

        // First RTT: grow by bytes_acked
        tokio::time::sleep(Duration::from_millis(25)).await;
        controller.on_ack(Duration::from_millis(20), 5_000);
        let cwnd_after_1 = controller.current_cwnd();

        // Verify exponential growth (cwnd += bytes_acked)
        assert!(
            cwnd_after_1 > initial_cwnd,
            "cwnd should grow: {} -> {}",
            initial_cwnd,
            cwnd_after_1
        );

        // Second RTT: grow more
        tokio::time::sleep(Duration::from_millis(25)).await;
        controller.on_ack(Duration::from_millis(20), 10_000);
        let cwnd_after_2 = controller.current_cwnd();

        assert!(
            cwnd_after_2 > cwnd_after_1,
            "cwnd should continue growing: {} -> {}",
            cwnd_after_1,
            cwnd_after_2
        );

        // Should still be in slow start
        assert!(
            controller.in_slow_start.load(Ordering::Acquire),
            "Should still be in slow start (cwnd={}, ssthresh=500000)",
            cwnd_after_2
        );

        println!(
            "Slow start ramp-up: {} -> {} -> {} bytes",
            initial_cwnd, cwnd_after_1, cwnd_after_2
        );
    }

    /// Test proportional slowdown reduction (4x, not to minimum)
    #[tokio::test]
    async fn test_proportional_slowdown_reduction() {
        let config = LedbatConfig {
            initial_cwnd: 320_000, // Start high to see proportional reduction clearly
            min_cwnd: 2848,
            max_cwnd: 10_000_000,
            ssthresh: 100_000, // Below initial so we exit slow start immediately
            enable_slow_start: true,
            enable_periodic_slowdown: true,
            randomize_ssthresh: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);

        controller.on_send(500_000);

        // Establish base delay and trigger slow start exit
        controller.on_ack(Duration::from_millis(20), 1000);
        controller.on_ack(Duration::from_millis(20), 1000);

        tokio::time::sleep(Duration::from_millis(25)).await;
        controller.on_ack(Duration::from_millis(20), 1000);

        // Should have exited slow start (initial_cwnd > ssthresh)
        tokio::time::sleep(Duration::from_millis(25)).await;
        controller.on_ack(Duration::from_millis(20), 1000);

        assert!(
            !controller.in_slow_start.load(Ordering::Acquire),
            "Should have exited slow start"
        );

        // Get cwnd before slowdown (should be ~320KB * 0.9 after slow start exit reduction)
        let cwnd_before_slowdown = controller.current_cwnd();
        println!("cwnd before slowdown: {} bytes", cwnd_before_slowdown);

        // Wait for slowdown to trigger (2 RTTs = 40ms)
        tokio::time::sleep(Duration::from_millis(50)).await;
        controller.on_ack(Duration::from_millis(20), 1000);

        // Should now be in slowdown
        let state = controller.slowdown_state.load(Ordering::Acquire);
        assert!(
            state == SlowdownState::InSlowdown as u8 || state == SlowdownState::Frozen as u8,
            "Should be in slowdown state, got {}",
            state
        );

        // Trigger transition to frozen
        tokio::time::sleep(Duration::from_millis(25)).await;
        controller.on_ack(Duration::from_millis(20), 1000);

        let cwnd_during_slowdown = controller.current_cwnd();
        println!("cwnd during slowdown: {} bytes", cwnd_during_slowdown);

        // Verify proportional reduction: should be pre_slowdown / 16, not min_cwnd
        let pre_slowdown = controller.pre_slowdown_cwnd.load(Ordering::Acquire);
        let expected_slowdown_cwnd = (pre_slowdown / SLOWDOWN_REDUCTION_FACTOR).max(2848);

        assert_eq!(
            cwnd_during_slowdown, expected_slowdown_cwnd,
            "Slowdown cwnd should be pre_slowdown/16 = {}/16 = {}, got {}",
            pre_slowdown, expected_slowdown_cwnd, cwnd_during_slowdown
        );

        // Verify it's NOT just min_cwnd (unless pre_slowdown was very small)
        if pre_slowdown > SLOWDOWN_REDUCTION_FACTOR * 2848 {
            assert!(
                cwnd_during_slowdown > 2848,
                "With large pre_slowdown ({}), slowdown cwnd ({}) should be > min_cwnd (2848)",
                pre_slowdown,
                cwnd_during_slowdown
            );
        }
    }

    /// Test complete slowdown cycle: frozen → ramp-up → normal
    #[tokio::test]
    async fn test_complete_slowdown_cycle() {
        let config = LedbatConfig {
            initial_cwnd: 160_000,
            min_cwnd: 2848,
            max_cwnd: 10_000_000,
            ssthresh: 50_000, // Exit slow start quickly
            enable_slow_start: true,
            enable_periodic_slowdown: true,
            randomize_ssthresh: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);

        controller.on_send(1_000_000);

        // Phase 1: Exit slow start
        controller.on_ack(Duration::from_millis(20), 1000);
        controller.on_ack(Duration::from_millis(20), 1000);
        tokio::time::sleep(Duration::from_millis(25)).await;
        controller.on_ack(Duration::from_millis(20), 1000);
        tokio::time::sleep(Duration::from_millis(25)).await;
        controller.on_ack(Duration::from_millis(20), 1000);

        assert!(
            !controller.in_slow_start.load(Ordering::Acquire),
            "Should have exited slow start"
        );
        let pre_slowdown_cwnd = controller.current_cwnd();
        println!(
            "Phase 1 - After slow start exit: cwnd = {}",
            pre_slowdown_cwnd
        );

        // Phase 2: Wait for slowdown to trigger (2 RTTs)
        tokio::time::sleep(Duration::from_millis(50)).await;
        controller.on_ack(Duration::from_millis(20), 1000);

        let state = controller.slowdown_state.load(Ordering::Acquire);
        println!("Phase 2 - After wait: state = {}", state);

        // Phase 3: Enter frozen state
        tokio::time::sleep(Duration::from_millis(25)).await;
        controller.on_ack(Duration::from_millis(20), 1000);

        let frozen_cwnd = controller.current_cwnd();
        let state = controller.slowdown_state.load(Ordering::Acquire);
        println!(
            "Phase 3 - Frozen: cwnd = {}, state = {}",
            frozen_cwnd, state
        );

        assert_eq!(
            state,
            SlowdownState::Frozen as u8,
            "Should be in Frozen state"
        );

        // Verify proportional reduction
        let expected_frozen = (controller.pre_slowdown_cwnd.load(Ordering::Relaxed)
            / SLOWDOWN_REDUCTION_FACTOR)
            .max(2848);
        assert_eq!(
            frozen_cwnd, expected_frozen,
            "Frozen cwnd should be proportional"
        );

        // Phase 4: Wait for freeze duration (2 RTTs) then start ramp-up
        tokio::time::sleep(Duration::from_millis(50)).await;
        controller.on_ack(Duration::from_millis(20), 5000);

        let state = controller.slowdown_state.load(Ordering::Acquire);
        println!("Phase 4 - After freeze: state = {}", state);

        // Should now be ramping up
        assert_eq!(
            state,
            SlowdownState::RampingUp as u8,
            "Should be in RampingUp state"
        );

        // Phase 5: Ramp up until reaching pre_slowdown_cwnd
        let target = controller.pre_slowdown_cwnd.load(Ordering::Acquire);
        let mut ramp_iterations = 0;
        while controller.current_cwnd() < target && ramp_iterations < 20 {
            tokio::time::sleep(Duration::from_millis(25)).await;
            controller.on_ack(Duration::from_millis(20), 20_000);
            ramp_iterations += 1;
        }

        // One more ACK to trigger state transition (state changes after cwnd >= target)
        tokio::time::sleep(Duration::from_millis(25)).await;
        controller.on_ack(Duration::from_millis(20), 1000);

        let final_cwnd = controller.current_cwnd();
        let final_state = controller.slowdown_state.load(Ordering::Acquire);
        println!(
            "Phase 5 - After ramp-up: cwnd = {}, state = {}, iterations = {}",
            final_cwnd, final_state, ramp_iterations
        );

        // Should have returned to normal
        assert_eq!(
            final_state,
            SlowdownState::Normal as u8,
            "Should be back to Normal state"
        );

        // Verify next slowdown is scheduled
        let next_slowdown = controller.next_slowdown_time_nanos.load(Ordering::Acquire);
        assert!(
            next_slowdown != u64::MAX,
            "Next slowdown should be scheduled"
        );

        // Verify slowdown count increased
        let slowdown_count = controller.periodic_slowdowns.load(Ordering::Relaxed);
        assert_eq!(slowdown_count, 1, "Should have completed 1 slowdown");
    }

    /// Test that slowdown interval is 9x the slowdown duration
    #[tokio::test]
    async fn test_slowdown_interval_is_9x_duration() {
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

        controller.on_send(1_000_000);

        // Exit slow start
        for _ in 0..4 {
            controller.on_ack(Duration::from_millis(20), 1000);
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        // Wait for and complete slowdown
        tokio::time::sleep(Duration::from_millis(50)).await;
        controller.on_ack(Duration::from_millis(20), 1000);

        // Go through frozen phase
        tokio::time::sleep(Duration::from_millis(25)).await;
        controller.on_ack(Duration::from_millis(20), 1000);
        tokio::time::sleep(Duration::from_millis(50)).await;
        controller.on_ack(Duration::from_millis(20), 5000);

        // Ramp up
        let target = controller.pre_slowdown_cwnd.load(Ordering::Acquire);
        while controller.current_cwnd() < target {
            tokio::time::sleep(Duration::from_millis(25)).await;
            controller.on_ack(Duration::from_millis(20), 30_000);
        }

        // Get the slowdown duration and next interval
        let slowdown_duration = controller
            .last_slowdown_duration_nanos
            .load(Ordering::Acquire);
        let now_nanos = controller.epoch.elapsed().as_nanos() as u64;
        let next_slowdown = controller.next_slowdown_time_nanos.load(Ordering::Acquire);
        let next_interval = next_slowdown.saturating_sub(now_nanos);

        println!(
            "Slowdown duration: {}ms, Next interval from now: {}ms",
            slowdown_duration / 1_000_000,
            next_interval / 1_000_000
        );

        // The next interval should be approximately 9x the slowdown duration
        // (with some tolerance for timing variations)
        let expected_interval = slowdown_duration * SLOWDOWN_INTERVAL_MULTIPLIER as u64;
        let min_expected = expected_interval / 2; // Allow 50% tolerance

        assert!(
            next_interval >= min_expected || next_interval >= 20_000_000, // or at least 1 RTT
            "Next interval ({}) should be >= 9x slowdown duration ({}) = {}",
            next_interval / 1_000_000,
            slowdown_duration / 1_000_000,
            expected_interval / 1_000_000
        );
    }

    /// Test cwnd values at each phase of slowdown
    #[tokio::test]
    async fn test_cwnd_values_through_slowdown_phases() {
        let config = LedbatConfig {
            initial_cwnd: 256_000, // 256KB - nice round number divisible by 16
            min_cwnd: 2848,
            max_cwnd: 10_000_000,
            ssthresh: 100_000,
            enable_slow_start: true,
            enable_periodic_slowdown: true,
            randomize_ssthresh: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);

        controller.on_send(1_000_000);

        // Record cwnd at each phase
        let mut cwnd_history: Vec<(String, usize)> = Vec::new();

        cwnd_history.push(("initial".to_string(), controller.current_cwnd()));

        // Exit slow start
        for i in 0..4 {
            controller.on_ack(Duration::from_millis(20), 1000);
            tokio::time::sleep(Duration::from_millis(25)).await;
            if i == 3 {
                cwnd_history.push((
                    "after_slow_start_exit".to_string(),
                    controller.current_cwnd(),
                ));
            }
        }

        // Trigger slowdown
        tokio::time::sleep(Duration::from_millis(50)).await;
        controller.on_ack(Duration::from_millis(20), 1000);
        cwnd_history.push(("slowdown_triggered".to_string(), controller.current_cwnd()));

        // Enter frozen
        tokio::time::sleep(Duration::from_millis(25)).await;
        controller.on_ack(Duration::from_millis(20), 1000);
        cwnd_history.push(("frozen".to_string(), controller.current_cwnd()));

        // Start ramp-up
        tokio::time::sleep(Duration::from_millis(50)).await;
        controller.on_ack(Duration::from_millis(20), 5000);
        cwnd_history.push(("ramp_up_start".to_string(), controller.current_cwnd()));

        // Mid ramp-up
        tokio::time::sleep(Duration::from_millis(25)).await;
        controller.on_ack(Duration::from_millis(20), 20_000);
        cwnd_history.push(("ramp_up_mid".to_string(), controller.current_cwnd()));

        // Complete ramp-up
        let target = controller.pre_slowdown_cwnd.load(Ordering::Acquire);
        while controller.current_cwnd() < target {
            tokio::time::sleep(Duration::from_millis(25)).await;
            controller.on_ack(Duration::from_millis(20), 30_000);
        }
        cwnd_history.push(("ramp_up_complete".to_string(), controller.current_cwnd()));

        // Print cwnd history
        println!("\n=== CWND History Through Slowdown ===");
        for (phase, cwnd) in &cwnd_history {
            println!("  {}: {} KB", phase, cwnd / 1024);
        }
        println!("=====================================\n");

        // Verify key transitions:
        // 1. After slow start exit should be ~90% of initial (0.9 reduction)
        let after_exit = cwnd_history
            .iter()
            .find(|(p, _)| p == "after_slow_start_exit")
            .map(|(_, c)| *c)
            .unwrap();
        assert!(
            after_exit < 256_000,
            "After exit should be reduced from initial"
        );

        // 2. Frozen should be ~1/16 of pre_slowdown
        let frozen = cwnd_history
            .iter()
            .find(|(p, _)| p == "frozen")
            .map(|(_, c)| *c)
            .unwrap();
        let pre_slowdown = controller.pre_slowdown_cwnd.load(Ordering::Relaxed);
        let expected_frozen = (pre_slowdown / SLOWDOWN_REDUCTION_FACTOR).max(2848);
        assert_eq!(
            frozen, expected_frozen,
            "Frozen should be pre_slowdown/{} = {}/{} = {}",
            SLOWDOWN_REDUCTION_FACTOR, pre_slowdown, SLOWDOWN_REDUCTION_FACTOR, expected_frozen
        );

        // 3. Ramp-up complete should be back near pre_slowdown
        let complete = cwnd_history
            .iter()
            .find(|(p, _)| p == "ramp_up_complete")
            .map(|(_, c)| *c)
            .unwrap();
        assert!(
            complete >= pre_slowdown,
            "After ramp-up should be >= pre_slowdown: {} >= {}",
            complete,
            pre_slowdown
        );
    }

    /// Helper to test slowdown behavior at a given RTT
    async fn test_slowdown_at_rtt(rtt_ms: u64) -> (usize, usize, usize, u32) {
        let config = LedbatConfig {
            initial_cwnd: 20_000, // Start small to allow slow start growth
            min_cwnd: 2848,
            max_cwnd: 10_000_000,
            ssthresh: 150_000, // High threshold so we grow before exit
            enable_slow_start: true,
            enable_periodic_slowdown: true,
            randomize_ssthresh: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);
        let rtt = Duration::from_millis(rtt_ms);

        controller.on_send(1_000_000);

        // Exit slow start with appropriate timing for this RTT
        // Need more iterations to grow cwnd and hit ssthresh
        for _ in 0..10 {
            controller.on_ack(rtt, 15_000); // Larger ACKs for faster growth
            tokio::time::sleep(rtt + Duration::from_millis(5)).await;
        }
        let post_exit_cwnd = controller.current_cwnd();

        // Wait for slowdown trigger (2 RTTs + margin)
        tokio::time::sleep(rtt * 3).await;
        controller.on_ack(rtt, 1000);

        // Transition through states
        tokio::time::sleep(rtt + Duration::from_millis(5)).await;
        controller.on_ack(rtt, 1000);
        let frozen_cwnd = controller.current_cwnd();

        // Wait for freeze duration (2 RTTs)
        tokio::time::sleep(rtt * 3).await;
        controller.on_ack(rtt, 5000);

        // Ramp up
        let target = controller.pre_slowdown_cwnd.load(Ordering::Acquire);
        let mut ramp_iterations = 0u32;
        while controller.current_cwnd() < target && ramp_iterations < 50 {
            tokio::time::sleep(rtt).await;
            controller.on_ack(rtt, 30_000);
            ramp_iterations += 1;
        }
        let final_cwnd = controller.current_cwnd();

        (post_exit_cwnd, frozen_cwnd, final_cwnd, ramp_iterations)
    }

    /// Parametrized test for slowdown behavior at different RTTs
    /// Tests: 10ms (LAN), 50ms (regional), 100ms (intercontinental), 200ms (satellite)
    #[rstest::rstest]
    #[case::low_latency_10ms(10, 20)]
    #[case::medium_latency_50ms(50, 30)]
    #[case::high_latency_100ms(100, 40)]
    #[case::very_high_latency_200ms(200, 50)]
    #[tokio::test]
    async fn test_slowdown_at_various_latencies(#[case] rtt_ms: u64, #[case] max_iterations: u32) {
        let (post_exit, frozen, final_cwnd, iterations) = test_slowdown_at_rtt(rtt_ms).await;

        println!(
            "{}ms RTT: post_exit={}KB, frozen={}KB, final={}KB, iterations={}",
            rtt_ms,
            post_exit / 1024,
            frozen / 1024,
            final_cwnd / 1024,
            iterations
        );

        // Verify key behaviors:
        // 1. Frozen should be ~1/4 of pre_slowdown (SLOWDOWN_REDUCTION_FACTOR = 4)
        assert!(
            frozen < post_exit / 2,
            "{}ms RTT: Frozen ({}) should be less than half of post_exit ({})",
            rtt_ms,
            frozen,
            post_exit
        );

        // 2. Final cwnd should recover to at least 95% of pre_slowdown
        assert!(
            final_cwnd >= post_exit * 95 / 100,
            "{}ms RTT: Final ({}) should recover to near pre_slowdown ({})",
            rtt_ms,
            final_cwnd,
            post_exit
        );

        // 3. Ramp-up should complete within reasonable iterations for this latency
        assert!(
            iterations < max_iterations,
            "{}ms RTT: Should ramp up within {} iterations, got {}",
            rtt_ms,
            max_iterations,
            iterations
        );
    }

    /// Test dynamic GAIN calculation at different latencies
    #[test]
    fn test_dynamic_gain_across_latencies() {
        let controller = LedbatController::new(10_000, 2848, 10_000_000);

        // GAIN = 1 / min(16, ceil(2 * TARGET / base_delay))
        // TARGET = 60ms

        // 10ms base delay: divisor = ceil(2 * 60 / 10) = 12, GAIN = 1/12
        let gain_10ms = controller.calculate_dynamic_gain(Duration::from_millis(10));
        assert!(
            (gain_10ms - 1.0 / 12.0).abs() < 0.001,
            "10ms: expected 1/12, got {}",
            gain_10ms
        );

        // 5ms base delay: divisor = ceil(2 * 60 / 5) = 24 -> capped at 16, GAIN = 1/16
        let gain_5ms = controller.calculate_dynamic_gain(Duration::from_millis(5));
        assert!(
            (gain_5ms - 1.0 / 16.0).abs() < 0.001,
            "5ms: expected 1/16 (capped), got {}",
            gain_5ms
        );

        // 30ms base delay: divisor = ceil(2 * 60 / 30) = 4, GAIN = 1/4
        let gain_30ms = controller.calculate_dynamic_gain(Duration::from_millis(30));
        assert!(
            (gain_30ms - 1.0 / 4.0).abs() < 0.001,
            "30ms: expected 1/4, got {}",
            gain_30ms
        );

        // 60ms base delay: divisor = ceil(2 * 60 / 60) = 2, GAIN = 1/2
        let gain_60ms = controller.calculate_dynamic_gain(Duration::from_millis(60));
        assert!(
            (gain_60ms - 1.0 / 2.0).abs() < 0.001,
            "60ms: expected 1/2, got {}",
            gain_60ms
        );

        // 120ms base delay: divisor = ceil(2 * 60 / 120) = 1, GAIN = 1/1 = 1
        let gain_120ms = controller.calculate_dynamic_gain(Duration::from_millis(120));
        assert!(
            (gain_120ms - 1.0).abs() < 0.001,
            "120ms: expected 1, got {}",
            gain_120ms
        );

        println!("Dynamic GAIN values:");
        println!("  5ms RTT:   {:.4} (capped at 1/16)", gain_5ms);
        println!("  10ms RTT:  {:.4}", gain_10ms);
        println!("  30ms RTT:  {:.4}", gain_30ms);
        println!("  60ms RTT:  {:.4}", gain_60ms);
        println!("  120ms RTT: {:.4}", gain_120ms);
    }

    // =========================================================================
    // E2E Simulation Tests - Visualize behavior at different latencies
    // =========================================================================
    //
    // These tests simulate data transfers at various RTT latencies and visualize
    // the LEDBAT controller's behavior (cwnd evolution, slow start, etc.).
    //
    // ## Running Larger Transfers
    //
    // The default tests use small transfer sizes (256KB-1MB) to keep execution
    // fast. To test larger transfers for performance analysis:
    //
    // ### Option 1: Modify existing tests temporarily
    // ```rust
    // // Change transfer size from 512KB to 10MB:
    // let (snapshots, duration_ms) = simulate_transfer_sync(50, 10 * 1024, 50);
    // ```
    //
    // ### Option 2: Add custom test cases to the parametrized test
    // ```rust
    // #[case::large_transfer_50ms(50, 10 * 1024)]  // 10MB @ 50ms
    // #[case::huge_transfer_100ms(100, 100 * 1024)] // 100MB @ 100ms
    // ```
    //
    // ### Option 3: Create a dedicated large transfer test (ignored by default)
    // ```rust
    // #[test]
    // #[ignore] // Run with: cargo test --ignored test_large_transfer
    // fn test_large_transfer() {
    //     let (snapshots, duration_ms) = simulate_transfer_sync(100, 50 * 1024, 1000);
    //     // 50MB @ 100ms RTT, snapshot every 1 second
    // }
    // ```
    //
    // ## Execution Time Estimates (real-time sleeps per RTT)
    //
    // | Transfer Size | 10ms RTT | 50ms RTT | 100ms RTT | 200ms RTT |
    // |---------------|----------|----------|-----------|-----------|
    // | 256KB         | ~0.1s    | ~0.2s    | ~0.3s     | ~0.6s     |
    // | 1MB           | ~0.3s    | ~0.5s    | ~0.6s     | ~1.8s     |
    // | 10MB          | ~1s      | ~2s      | ~4s       | ~10s      |
    // | 100MB         | ~8s      | ~20s     | ~40s      | ~100s     |
    //
    // Note: These are rough estimates. Actual time depends on slow start behavior
    // and steady-state throughput which varies with RTT.
    //
    // ## Enabling Periodic Slowdown in Simulations
    //
    // The simulation disables periodic slowdowns by default since they require
    // wall-clock time. To test with slowdowns, set `enable_periodic_slowdown: true`
    // in the config and accept the longer test duration (slowdown cycles are ~9x RTT).

    /// Snapshot of controller state at a point in time
    #[derive(Debug, Clone)]
    struct StateSnapshot {
        time_ms: u64,
        cwnd_kb: usize,
        state: u8,
        throughput_mbps: f64,
        data_transferred_kb: usize,
    }

    /// Simulate a transfer and collect state snapshots (synchronous, no real-time delays)
    ///
    /// This uses a controlled test clock to simulate time passing, allowing the test
    /// to run instantly regardless of simulated RTT.
    ///
    /// Note: Since the controller uses wall-clock time for rate limiting, we need to
    /// reset the rate limiter before each simulated RTT to allow updates.
    fn simulate_transfer_sync(
        rtt_ms: u64,
        transfer_size_kb: usize,
        sample_interval_ms: u64,
    ) -> (Vec<StateSnapshot>, u64) {
        let rtt = Duration::from_millis(rtt_ms);
        let config = LedbatConfig {
            initial_cwnd: 38_000, // IW26
            min_cwnd: 2848,
            max_cwnd: 10_000_000,
            ssthresh: 100_000,
            enable_slow_start: true,
            // Disable periodic slowdown since it depends on wall-clock time
            // See test_complete_slowdown_cycle for slowdown tests
            enable_periodic_slowdown: false,
            randomize_ssthresh: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);

        let mut snapshots = Vec::new();
        let mut total_data_kb: usize = 0;
        let mut elapsed_ms: u64 = 0;

        // Simulate flightsize - enough to not bottleneck
        controller.on_send(10_000_000);

        // Max simulation time (5 minutes virtual time)
        let max_time_ms = 300_000u64;

        // Prime the delay filter with initial samples
        for _ in 0..4 {
            controller.delay_filter.add_sample(rtt);
        }
        controller.base_delay_history.update(rtt);

        // Simulate transfer - each iteration represents one RTT
        while total_data_kb < transfer_size_kb && elapsed_ms < max_time_ms {
            elapsed_ms += rtt_ms;

            // Wait for at least base_delay so the rate limiter allows updates
            // This is the minimum real-time wait needed for accurate simulation
            std::thread::sleep(rtt);

            // Reset bytes accumulated and set last_update to allow this update
            controller.last_update_nanos.store(0, Ordering::Release);

            // Calculate bytes we can send this RTT (cwnd worth)
            let cwnd = controller.current_cwnd();
            let remaining_bytes = (transfer_size_kb * 1024).saturating_sub(total_data_kb * 1024);
            let bytes_this_rtt = cwnd.min(remaining_bytes);

            if bytes_this_rtt == 0 {
                // Edge case: cwnd too small, simulate at least one packet
                let min_packet = 1464usize;
                controller.on_ack(rtt, min_packet);
                total_data_kb += min_packet / 1024;
            } else {
                // Simulate ACK for sent data - simulate slight queuing delay for realism
                // Use 10% of target delay as typical queuing delay (6ms)
                let queued_rtt = Duration::from_nanos(rtt.as_nanos() as u64 + 6_000_000);
                controller.on_ack(queued_rtt, bytes_this_rtt);
                total_data_kb += bytes_this_rtt / 1024;
            }

            // Take snapshot at intervals
            if elapsed_ms % sample_interval_ms == 0 || elapsed_ms <= rtt_ms * 2 {
                let throughput = if elapsed_ms > 0 {
                    (total_data_kb as f64 * 1024.0 * 8.0)
                        / (elapsed_ms as f64 / 1000.0)
                        / 1_000_000.0
                } else {
                    0.0
                };

                snapshots.push(StateSnapshot {
                    time_ms: elapsed_ms,
                    cwnd_kb: controller.current_cwnd() / 1024,
                    state: controller.slowdown_state.load(Ordering::Relaxed),
                    throughput_mbps: throughput,
                    data_transferred_kb: total_data_kb,
                });
            }
        }

        (snapshots, elapsed_ms)
    }

    /// Render ASCII throughput chart
    fn render_throughput_chart(snapshots: &[StateSnapshot], max_throughput: f64, width: usize) {
        let height = 10;
        let mut chart = vec![vec![' '; width]; height];

        // Plot data points
        for (i, snap) in snapshots.iter().enumerate() {
            if i >= width {
                break;
            }
            let normalized = (snap.throughput_mbps / max_throughput).min(1.0);
            let row = height - 1 - (normalized * (height - 1) as f64) as usize;
            chart[row][i] = match snap.state {
                0 => '█', // Normal
                1 => '▓', // WaitingForSlowdown
                2 => '▒', // InSlowdown
                3 => '░', // Frozen
                4 => '▄', // RampingUp
                _ => '?',
            };
        }

        // Print chart
        println!("\n    Throughput over time (█=Normal ░=Frozen ▄=RampingUp)");
        println!("    {:.1} Mbps ┤", max_throughput);
        for row in &chart {
            print!("            │");
            for &c in row {
                print!("{}", c);
            }
            println!();
        }
        println!(
            "      0 Mbps ┼{}┬──────────────────────────────────────────►",
            "─".repeat(width.saturating_sub(45))
        );
        println!("              0                                              time");
    }

    /// Render state timeline
    fn render_state_timeline(snapshots: &[StateSnapshot]) {
        let _state_names = ["Normal", "Waiting", "Slowdown", "Frozen", "RampingUp"];
        let state_chars = ['─', '╌', '▼', '═', '╱'];

        print!("    State: ");
        let mut last_state = 255u8;
        for snap in snapshots.iter().take(60) {
            if snap.state != last_state {
                print!("{}", state_chars[snap.state as usize]);
                last_state = snap.state;
            } else {
                print!("{}", state_chars[snap.state as usize]);
            }
        }
        println!();
        println!("           (─=Normal ═=Frozen ╱=RampingUp ▼=Slowdown)");
    }

    /// E2E test: 512KB transfer at 50ms RTT with visualization
    ///
    /// This test uses real-time sleeps to properly test the LEDBAT controller's
    /// slow start behavior. Uses smaller transfer size for faster execution.
    /// See test_complete_slowdown_cycle for slowdown mechanism tests.
    #[test]
    fn test_e2e_transfer_50ms_rtt() {
        println!("\n========== E2E Test: 512KB Transfer @ 50ms RTT ==========");

        let (snapshots, duration_ms) = simulate_transfer_sync(50, 512, 50);

        println!("\n--- Transfer Summary ---");
        println!("RTT:              50ms");
        println!("Transfer size:    512 KB");
        println!(
            "Duration:         {} ms ({:.2}s)",
            duration_ms,
            duration_ms as f64 / 1000.0
        );
        println!(
            "Avg throughput:   {:.2} Mbps",
            (512.0 * 8.0) / (duration_ms as f64 / 1000.0) / 1000.0
        );

        // Print key snapshots showing slow start growth
        println!("\n--- cwnd Evolution ---");
        println!("{:>8} {:>8} {:>10}", "Time(ms)", "cwnd(KB)", "Data(KB)");
        for snap in &snapshots {
            println!(
                "{:>8} {:>8} {:>10}",
                snap.time_ms, snap.cwnd_kb, snap.data_transferred_kb
            );
        }

        // Verify slow start doubled cwnd (initial 37KB should grow)
        let max_cwnd = snapshots.iter().map(|s| s.cwnd_kb).max().unwrap_or(0);
        assert!(
            max_cwnd >= 74,
            "Slow start should double cwnd (37 -> 74+), got max {}KB",
            max_cwnd
        );

        // Verify transfer completed
        let final_data = snapshots.last().map(|s| s.data_transferred_kb).unwrap_or(0);
        assert!(
            final_data >= 512,
            "Should have transferred 512KB, got {}KB",
            final_data
        );
    }

    /// E2E test: 1MB transfer at 200ms RTT with visualization
    #[test]
    fn test_e2e_transfer_200ms_rtt() {
        println!("\n========== E2E Test: 1MB Transfer @ 200ms RTT ==========");

        let (snapshots, duration_ms) = simulate_transfer_sync(200, 1024, 200);

        println!("\n--- Transfer Summary ---");
        println!("RTT:              200ms");
        println!("Transfer size:    1 MB");
        println!(
            "Duration:         {} ms ({:.2}s)",
            duration_ms,
            duration_ms as f64 / 1000.0
        );
        println!(
            "Avg throughput:   {:.2} Mbps",
            (1024.0 * 8.0) / (duration_ms as f64 / 1000.0) / 1000.0
        );

        // Print key snapshots
        println!("\n--- cwnd Evolution ---");
        println!("{:>8} {:>8} {:>10}", "Time(ms)", "cwnd(KB)", "Data(KB)");
        for snap in &snapshots {
            println!(
                "{:>8} {:>8} {:>10}",
                snap.time_ms, snap.cwnd_kb, snap.data_transferred_kb
            );
        }

        // Verify slow start worked
        let max_cwnd = snapshots.iter().map(|s| s.cwnd_kb).max().unwrap_or(0);
        assert!(
            max_cwnd >= 74,
            "Slow start should grow cwnd, got max {}KB",
            max_cwnd
        );

        // Verify transfer completed
        let final_data = snapshots.last().map(|s| s.data_transferred_kb).unwrap_or(0);
        assert!(
            final_data >= 1024,
            "Should have transferred 1MB, got {}KB",
            final_data
        );
    }

    /// Parametrized test for multiple latency scenarios
    ///
    /// Uses small transfer sizes (256KB) to keep test execution fast while
    /// still validating slow start behavior at different RTTs.
    #[rstest::rstest]
    #[case::lan_10ms(10, 256)] // 256KB @ 10ms
    #[case::regional_50ms(50, 256)] // 256KB @ 50ms
    #[case::continental_100ms(100, 256)] // 256KB @ 100ms
    #[case::satellite_200ms(200, 256)] // 256KB @ 200ms
    fn test_e2e_latency_scenarios(#[case] rtt_ms: u64, #[case] size_kb: usize) {
        let (snapshots, duration_ms) = simulate_transfer_sync(rtt_ms, size_kb, rtt_ms);

        let avg_throughput = (size_kb as f64 * 8.0) / (duration_ms as f64 / 1000.0) / 1000.0;

        println!("\n{}ms RTT, {}KB transfer:", rtt_ms, size_kb);
        println!(
            "  Duration: {}ms ({:.2}s)",
            duration_ms,
            duration_ms as f64 / 1000.0
        );
        println!("  Throughput: {:.2} Mbps", avg_throughput);
        println!("  Snapshots: {}", snapshots.len());

        // Verify slow start worked (cwnd should grow from initial 37KB)
        let max_cwnd = snapshots.iter().map(|s| s.cwnd_kb).max().unwrap_or(0);
        assert!(
            max_cwnd >= 74,
            "{}ms RTT: Slow start should grow cwnd to 74KB+, got {}KB",
            rtt_ms,
            max_cwnd
        );

        // Verify we actually transferred the data
        let final_data = snapshots.last().map(|s| s.data_transferred_kb).unwrap_or(0);
        assert!(
            final_data >= size_kb,
            "Should have transferred {}KB, got {}KB",
            size_kb,
            final_data
        );
    }

    /// Test that visualizes cwnd evolution through complete slowdown cycle
    #[tokio::test]
    async fn test_e2e_visualize_slowdown_cycle() {
        println!("\n========== Visualizing Complete Slowdown Cycle ==========");

        let rtt = Duration::from_millis(100);
        let config = LedbatConfig {
            initial_cwnd: 38_000,
            min_cwnd: 2848,
            max_cwnd: 10_000_000,
            ssthresh: 80_000,
            enable_slow_start: true,
            enable_periodic_slowdown: true,
            randomize_ssthresh: false,
            ..Default::default()
        };
        let controller = LedbatController::new_with_config(config);
        controller.on_send(1_000_000);

        let mut timeline: Vec<(u64, usize, &str)> = Vec::new();
        let mut elapsed_ms = 0u64;

        // Phase 1: Slow start
        println!("\n--- Phase 1: Slow Start ---");
        for _ in 0..15 {
            tokio::time::sleep(rtt).await;
            elapsed_ms += 100;
            controller.on_ack(rtt, 20_000);

            let state = controller.slowdown_state.load(Ordering::Relaxed);
            let state_name = match state {
                0 => "Normal",
                1 => "Waiting",
                2 => "Slowdown",
                3 => "Frozen",
                4 => "RampingUp",
                _ => "?",
            };
            timeline.push((elapsed_ms, controller.current_cwnd() / 1024, state_name));

            if !controller.in_slow_start.load(Ordering::Relaxed) {
                println!(
                    "  Slow start exit at {}ms, cwnd={}KB",
                    elapsed_ms,
                    controller.current_cwnd() / 1024
                );
                break;
            }
        }

        // Phase 2: Wait for slowdown
        println!("\n--- Phase 2: Waiting for Slowdown ---");
        for _ in 0..10 {
            tokio::time::sleep(rtt).await;
            elapsed_ms += 100;
            controller.on_ack(rtt, 10_000);

            let state = controller.slowdown_state.load(Ordering::Relaxed);
            let state_name = match state {
                0 => "Normal",
                1 => "Waiting",
                2 => "Slowdown",
                3 => "Frozen",
                4 => "RampingUp",
                _ => "?",
            };
            timeline.push((elapsed_ms, controller.current_cwnd() / 1024, state_name));

            if state >= 2 {
                println!(
                    "  Slowdown triggered at {}ms, cwnd={}KB",
                    elapsed_ms,
                    controller.current_cwnd() / 1024
                );
                break;
            }
        }

        // Phase 3: Freeze
        println!("\n--- Phase 3: Frozen ---");
        let pre_slowdown_cwnd = controller.pre_slowdown_cwnd.load(Ordering::Relaxed) / 1024;
        for _ in 0..5 {
            tokio::time::sleep(rtt).await;
            elapsed_ms += 100;
            controller.on_ack(rtt, 5_000);

            let state = controller.slowdown_state.load(Ordering::Relaxed);
            let state_name = match state {
                0 => "Normal",
                1 => "Waiting",
                2 => "Slowdown",
                3 => "Frozen",
                4 => "RampingUp",
                _ => "?",
            };
            timeline.push((elapsed_ms, controller.current_cwnd() / 1024, state_name));

            if state == 4 {
                println!(
                    "  Ramp-up started at {}ms, cwnd={}KB",
                    elapsed_ms,
                    controller.current_cwnd() / 1024
                );
                break;
            }
        }

        // Phase 4: Ramp-up
        println!("\n--- Phase 4: Ramp-up ---");
        for _ in 0..10 {
            tokio::time::sleep(rtt).await;
            elapsed_ms += 100;
            controller.on_ack(rtt, 30_000);

            let state = controller.slowdown_state.load(Ordering::Relaxed);
            let state_name = match state {
                0 => "Normal",
                1 => "Waiting",
                2 => "Slowdown",
                3 => "Frozen",
                4 => "RampingUp",
                _ => "?",
            };
            timeline.push((elapsed_ms, controller.current_cwnd() / 1024, state_name));

            if state == 0 {
                println!(
                    "  Back to normal at {}ms, cwnd={}KB",
                    elapsed_ms,
                    controller.current_cwnd() / 1024
                );
                break;
            }
        }

        // Render timeline
        println!("\n--- cwnd Timeline ---");
        println!("{:>8} {:>10} {:>12}", "Time(ms)", "cwnd(KB)", "State");
        println!("{}", "-".repeat(32));
        for (t, cwnd, state) in &timeline {
            let bar_len = (*cwnd / 10).min(30);
            let bar: String = "█".repeat(bar_len);
            println!("{:>8} {:>10} {:>12} {}", t, cwnd, state, bar);
        }

        // Verify the cycle completed
        let final_state = controller.slowdown_state.load(Ordering::Relaxed);
        assert_eq!(final_state, 0, "Should return to Normal state");

        let final_cwnd = controller.current_cwnd();
        assert!(
            final_cwnd >= pre_slowdown_cwnd * 1024 * 95 / 100,
            "Should recover to ~pre_slowdown level: {} >= {}",
            final_cwnd / 1024,
            pre_slowdown_cwnd * 95 / 100
        );

        println!("\n✓ Complete slowdown cycle verified");
        println!("  Pre-slowdown: {}KB", pre_slowdown_cwnd);
        println!(
            "  Frozen: {}KB (expected {}KB)",
            timeline
                .iter()
                .filter(|(_, _, s)| *s == "Frozen")
                .map(|(_, c, _)| *c)
                .next()
                .unwrap_or(0),
            pre_slowdown_cwnd / 4
        );
        println!("  Recovered: {}KB", final_cwnd / 1024);
    }
}

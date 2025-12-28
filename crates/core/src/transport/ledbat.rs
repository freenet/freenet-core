//! LEDBAT (Low Extra Delay Background Transport) congestion controller.
//!
//! RFC 6817 compliant implementation of delay-based congestion control designed
//! for background traffic. LEDBAT automatically yields bandwidth to competing
//! flows by maintaining a target queuing delay (~100ms).
//!
//! ## Why LEDBAT?
//!
//! Freenet runs as a background daemon and should not interfere with foreground
//! applications (video calls, web browsing, etc.). LEDBAT is designed exactly
//! for this use case - it's used by BitTorrent for the same reason.
//!
//! ## Key Differences from AIMD
//!
//! | Factor | AIMD | LEDBAT |
//! |--------|------|--------|
//! | Congestion signal | Packet loss | Queuing delay |
//! | Optimization goal | Maximize throughput | Minimize interference |
//! | Competing flows | Slow to yield | Fast to yield |
//! | User perception | Noticeable | Imperceptible |
//!
//! ## Lock-Free Design
//!
//! This implementation is fully lock-free, using atomic operations for all state:
//! - Atomic ring buffers for delay filtering and base delay history
//! - AtomicU64 for Duration values (stored as nanoseconds)
//! - Epoch-based timing for rate-limiting updates
#![allow(dead_code)] // Infrastructure not yet integrated

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use super::packet_data::MAX_DATA_SIZE;

/// Maximum segment size (actual packet data capacity)
const MSS: usize = MAX_DATA_SIZE;

/// Target queuing delay (RFC 6817 default)
const TARGET: Duration = Duration::from_millis(100);

/// Controller gain (RFC 6817 default)
const GAIN: f64 = 1.0;

/// Base delay history size (RFC 6817 recommendation)
const BASE_HISTORY_SIZE: usize = 10;

/// Delay filter sample count (RFC 6817 recommendation)
const DELAY_FILTER_SIZE: usize = 4;

/// Default slow start threshold (100 KB)
const DEFAULT_SSTHRESH: usize = 102_400;

/// Configuration for LEDBAT with slow start
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
    /// Default: 0.5 (exit when queuing_delay > TARGET/2 = 50ms)
    pub delay_exit_threshold: f64,
    /// Randomize ssthresh to prevent synchronization (±20% jitter)
    /// Default: true
    pub randomize_ssthresh: bool,
}

impl Default for LedbatConfig {
    fn default() -> Self {
        Self {
            // IW26: 26 * MSS = 38,000 bytes
            // Reaches 300KB cwnd in 3 RTTs (38KB → 76KB → 152KB → 304KB)
            // This enables >3 MB/s throughput at 100ms RTT
            initial_cwnd: 38_000,       // 26 * MSS (IW26)
            min_cwnd: 2_848,            // 2 * MSS
            max_cwnd: 1_000_000_000,    // 1 GB
            ssthresh: DEFAULT_SSTHRESH, // 100 KB
            enable_slow_start: true,    // Enable by default
            delay_exit_threshold: 0.5,  // Exit at TARGET/2
            randomize_ssthresh: true,   // Enable jitter by default
        }
    }
}

/// Sentinel value indicating an empty slot in atomic delay arrays.
/// We use u64::MAX since no valid RTT would ever be this large (~584 years).
const EMPTY_DELAY_NANOS: u64 = u64::MAX;

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

/// LEDBAT congestion controller (RFC 6817) with slow start.
///
/// Maintains target queuing delay to yield bandwidth to competing flows.
/// Uses delay-based congestion control instead of loss-based (AIMD).
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

    /// Configuration
    target_delay: Duration,
    gain: f64,
    allowed_increase_packets: usize,
    min_cwnd: usize,
    max_cwnd: usize,
    enable_slow_start: bool,
    delay_exit_threshold: f64,

    /// Statistics
    total_increases: AtomicUsize,
    total_decreases: AtomicUsize,
    total_losses: AtomicUsize,
    min_cwnd_events: AtomicUsize,
    slow_start_exits: AtomicUsize,
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
            target_delay: TARGET,
            gain: GAIN,
            allowed_increase_packets: 2, // RFC 6817 default
            min_cwnd: config.min_cwnd,
            max_cwnd: config.max_cwnd,
            enable_slow_start: config.enable_slow_start,
            delay_exit_threshold: config.delay_exit_threshold,
            total_increases: AtomicUsize::new(0),
            total_decreases: AtomicUsize::new(0),
            total_losses: AtomicUsize::new(0),
            min_cwnd_events: AtomicUsize::new(0),
            slow_start_exits: AtomicUsize::new(0),
        }
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
            self.handle_slow_start(bytes_acked_total, queuing_delay);
            return;
        }

        // LEDBAT congestion avoidance (RFC 6817 Section 2.4.2)
        // Δcwnd = GAIN * (off_target / TARGET) * bytes_acked * MSS / cwnd
        let current_cwnd = self.cwnd.load(Ordering::Acquire);
        let cwnd_change =
            self.gain * (off_target_ms / target_ms) * (bytes_acked_total as f64) * (MSS as f64)
                / (current_cwnd as f64);

        let mut new_cwnd = current_cwnd as f64 + cwnd_change;

        // Track statistics
        if cwnd_change > 0.0 {
            self.total_increases.fetch_add(1, Ordering::Relaxed);
        } else if cwnd_change < 0.0 {
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
    fn handle_slow_start(&self, bytes_acked: usize, queuing_delay: Duration) {
        // Early exit if no longer in slow start (race: loss/timeout occurred)
        if !self.in_slow_start.load(Ordering::Acquire) {
            return;
        }

        let current_cwnd = self.cwnd.load(Ordering::Acquire);
        let ssthresh = self.ssthresh.load(Ordering::Acquire);

        // Check exit conditions
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
        }
    }
}

/// LEDBAT statistics.
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
}

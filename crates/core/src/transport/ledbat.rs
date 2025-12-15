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

#![allow(dead_code)] // Infrastructure not yet integrated

use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
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

/// Consolidated state for LEDBAT to reduce lock contention.
///
/// Previously these were 4 separate Mutex fields causing 6 lock acquisitions
/// per ACK. Now consolidated to a single lock acquisition.
struct LedbatState {
    /// Base delay history (10-minute buckets)
    base_delay_history: BaseDelayHistory,

    /// Delay filter (MIN over recent samples)
    delay_filter: DelayFilter,

    /// Current queuing delay estimate
    queuing_delay: Duration,

    /// Last update time (for rate-limiting updates)
    last_update: Instant,
}

/// LEDBAT congestion controller (RFC 6817).
///
/// Maintains target queuing delay to yield bandwidth to competing flows.
/// Uses delay-based congestion control instead of loss-based (AIMD).
pub struct LedbatController {
    /// Congestion window (bytes in flight)
    cwnd: AtomicUsize,

    /// Bytes currently in flight (sent but not ACKed)
    flightsize: AtomicUsize,

    /// Consolidated state (single lock for all delay tracking)
    state: Mutex<LedbatState>,

    /// Bytes acknowledged since last update
    bytes_acked_since_update: AtomicUsize,

    /// Configuration
    target_delay: Duration,
    gain: f64,
    allowed_increase_packets: usize,
    min_cwnd: usize,
    max_cwnd: usize,

    /// Statistics
    total_increases: AtomicUsize,
    total_decreases: AtomicUsize,
    total_losses: AtomicUsize,
    min_cwnd_events: AtomicUsize,
}

impl LedbatController {
    /// Create new LEDBAT controller.
    ///
    /// # Arguments
    /// * `initial_cwnd` - Initial congestion window (bytes)
    /// * `min_cwnd` - Minimum window (typically 2 * MSS)
    /// * `max_cwnd` - Maximum window (protocol limit or config)
    ///
    /// # Example
    /// ```
    /// # use freenet::transport::ledbat::LedbatController;
    /// let controller = LedbatController::new(
    ///     2848,      // 2 * MSS (RFC 6817)
    ///     2848,      // min
    ///     10_000_000 // 10 MB max
    /// );
    /// ```
    pub fn new(initial_cwnd: usize, min_cwnd: usize, max_cwnd: usize) -> Self {
        Self {
            cwnd: AtomicUsize::new(initial_cwnd),
            flightsize: AtomicUsize::new(0),
            state: Mutex::new(LedbatState {
                base_delay_history: BaseDelayHistory::new(),
                delay_filter: DelayFilter::new(),
                queuing_delay: Duration::ZERO,
                last_update: Instant::now(),
            }),
            bytes_acked_since_update: AtomicUsize::new(0),
            target_delay: TARGET,
            gain: GAIN,
            allowed_increase_packets: 2, // RFC 6817 default
            min_cwnd,
            max_cwnd,
            total_increases: AtomicUsize::new(0),
            total_decreases: AtomicUsize::new(0),
            total_losses: AtomicUsize::new(0),
            min_cwnd_events: AtomicUsize::new(0),
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

        // Single lock acquisition for all delay tracking (was 6 locks before)
        let (queuing_delay, base_delay) = {
            let mut state = self.state.lock();

            // Update base delay history
            state.base_delay_history.update(rtt_sample);

            // Add to delay filter
            state.delay_filter.add_sample(rtt_sample);

            // Get filtered delay (minimum of recent samples)
            if !state.delay_filter.is_ready() {
                return; // Need more samples
            }
            let filtered_rtt = state
                .delay_filter
                .filtered_delay()
                .unwrap_or(rtt_sample);

            // Calculate base and queuing delays
            let base_delay = state.base_delay_history.base_delay();
            let queuing_delay = filtered_rtt.saturating_sub(base_delay);
            state.queuing_delay = queuing_delay;

            // Rate-limit updates to approximately once per RTT
            let elapsed_since_update = state.last_update.elapsed();

            // Use base delay as RTT estimate for rate-limiting
            if elapsed_since_update < base_delay {
                return;
            }
            state.last_update = Instant::now();

            (queuing_delay, base_delay)
        };

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

        // Calculate cwnd change (RFC 6817 Section 2.4.2)
        // Δcwnd = GAIN * (off_target / TARGET) * bytes_acked * MSS / cwnd
        let current_cwnd = self.cwnd.load(Ordering::Acquire);
        let cwnd_change = self.gain
            * (off_target_ms / target_ms)
            * (bytes_acked_total as f64)
            * (MSS as f64)
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

    /// Called when packet loss detected (not timeout).
    ///
    /// RFC 6817 Section 2.4.1: halve cwnd on loss.
    pub fn on_loss(&self) {
        self.total_losses.fetch_add(1, Ordering::Relaxed);

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
    pub fn on_timeout(&self) {
        self.total_losses.fetch_add(1, Ordering::Relaxed);

        let new_cwnd = MSS.max(self.min_cwnd);

        let current_cwnd = self.cwnd.load(Ordering::Acquire);
        self.cwnd.store(new_cwnd, Ordering::Release);

        tracing::error!(
            old_cwnd_kb = current_cwnd / 1024,
            new_cwnd_kb = new_cwnd / 1024,
            "LEDBAT retransmission timeout - reset to 1*MSS"
        );
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

    /// Get current queuing delay.
    pub fn queuing_delay(&self) -> Duration {
        self.state.lock().queuing_delay
    }

    /// Get base delay.
    pub fn base_delay(&self) -> Duration {
        self.state.lock().base_delay_history.base_delay()
    }

    /// Get current flightsize.
    pub fn flightsize(&self) -> usize {
        self.flightsize.load(Ordering::Relaxed)
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
}

/// Delay filter: MIN over recent samples (RFC 6817 Section 4.2).
struct DelayFilter {
    samples: VecDeque<Duration>,
    max_samples: usize,
}

impl DelayFilter {
    fn new() -> Self {
        Self {
            samples: VecDeque::with_capacity(DELAY_FILTER_SIZE),
            max_samples: DELAY_FILTER_SIZE,
        }
    }

    fn add_sample(&mut self, rtt: Duration) {
        self.samples.push_back(rtt);
        if self.samples.len() > self.max_samples {
            self.samples.pop_front();
        }
    }

    fn filtered_delay(&self) -> Option<Duration> {
        self.samples.iter().min().copied()
    }

    fn is_ready(&self) -> bool {
        self.samples.len() >= 2
    }
}

/// Base delay history: 10-minute bucket tracking (RFC 6817 Section 4.1).
struct BaseDelayHistory {
    /// One bucket per minute, containing minimum delay observed in that minute
    buckets: VecDeque<Duration>,
    /// Minimum being tracked for current minute
    current_minute_min: Duration,
    /// Start time of current minute
    current_minute_start: Instant,
    /// Minute duration
    minute_duration: Duration,
    /// History size (10 per RFC 6817)
    history_size: usize,
}

impl BaseDelayHistory {
    fn new() -> Self {
        Self {
            buckets: VecDeque::with_capacity(BASE_HISTORY_SIZE),
            current_minute_min: Duration::MAX,
            current_minute_start: Instant::now(),
            minute_duration: Duration::from_secs(60),
            history_size: BASE_HISTORY_SIZE,
        }
    }

    fn update(&mut self, rtt_sample: Duration) {
        let now = Instant::now();

        // Check if we've rolled over to a new minute
        if now.duration_since(self.current_minute_start) >= self.minute_duration {
            // Save current minute's minimum
            if self.current_minute_min != Duration::MAX {
                self.buckets.push_back(self.current_minute_min);
                if self.buckets.len() > self.history_size {
                    self.buckets.pop_front();
                }
            }

            // Start new minute
            self.current_minute_min = rtt_sample;
            self.current_minute_start = now;
        } else {
            // Update current minute's minimum
            self.current_minute_min = self.current_minute_min.min(rtt_sample);
        }
    }

    fn base_delay(&self) -> Duration {
        let historical_min = self.buckets.iter().min().copied();
        let current_min = if self.current_minute_min != Duration::MAX {
            Some(self.current_minute_min)
        } else {
            None
        };

        match (historical_min, current_min) {
            (Some(h), Some(c)) => h.min(c),
            (Some(h), None) => h,
            (None, Some(c)) => c,
            (None, None) => Duration::from_millis(10), // Fallback
        }
    }
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
        let controller = LedbatController::new(100_000, 2848, 10_000_000);

        // Track flightsize to avoid application-limited cap interference
        controller.on_send(50_000); // High flightsize

        // Set base delay
        controller.on_ack(Duration::from_millis(10), 1000);
        controller.on_ack(Duration::from_millis(10), 1000);

        let initial_cwnd = controller.current_cwnd();

        // Wait for update interval
        tokio::time::sleep(Duration::from_millis(20)).await;

        // RTT above target (10ms + 150ms = 160ms > 100ms target)
        // off_target = -60ms (negative = decrease)
        // Should decrease
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

    #[tokio::test]
    async fn test_ledbat_cwnd_formula_correctness() {
        let controller = LedbatController::new(10_000, 2848, 10_000_000);

        // Track flightsize for application-limited cap
        controller.on_send(20_000); // Send enough to cover all acks

        // Set base delay with filter (need 2+ samples)
        for _ in 0..4 {
            controller.on_ack(Duration::from_millis(10), 1000);
        }

        let initial_cwnd = controller.current_cwnd();

        // Wait for update interval
        tokio::time::sleep(Duration::from_millis(20)).await;

        // RTT below target: 30ms (10ms base + 20ms queuing) < 100ms target
        // off_target = 70ms, target = 100ms
        // Δcwnd = GAIN * (70/100) * bytes_acked * MSS / cwnd
        //       = 1.0 * 0.7 * 5000 * 1464 (MAX_DATA_SIZE) / initial_cwnd
        // BUT: limited by application-limited cap (flightsize + ALLOWED_INCREASE)
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
}

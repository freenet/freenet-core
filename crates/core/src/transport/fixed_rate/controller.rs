//! Fixed-rate congestion controller implementation.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crate::simulation::{RealTime, TimeSource};

/// Default rate: 100 Mbps in bytes/sec (100 * 1_000_000 / 8)
///
/// This rate is chosen to:
/// - Support fast contract retrieval (user-visible critical path)
/// - Account for sequential multi-hop transfer (3-5x slower than single hop)
/// - Remain conservative enough not to degrade other network usage
/// - Work reliably across real network paths (validated on nova↔vega)
///
/// Note: Localhost tests may use BBR via FREENET_CONGESTION_CONTROL=bbr
/// because FixedRate has timing issues on loopback interfaces.
pub const DEFAULT_RATE_BYTES_PER_SEC: usize = 12_500_000;

/// Configuration for the fixed-rate controller.
#[derive(Debug, Clone)]
pub struct FixedRateConfig {
    /// Target transmission rate in bytes per second.
    /// Default: 50 Mbps (6,250,000 bytes/sec)
    pub rate_bytes_per_sec: usize,
}

impl Default for FixedRateConfig {
    fn default() -> Self {
        Self {
            rate_bytes_per_sec: DEFAULT_RATE_BYTES_PER_SEC,
        }
    }
}

impl FixedRateConfig {
    /// Create a config with the specified rate in bytes per second.
    pub fn new(rate_bytes_per_sec: usize) -> Self {
        Self { rate_bytes_per_sec }
    }

    /// Create a config with the specified rate in megabits per second.
    pub fn from_mbps(mbps: usize) -> Self {
        Self {
            rate_bytes_per_sec: mbps * 1_000_000 / 8,
        }
    }
}

/// Fixed-rate congestion controller.
///
/// Maintains a constant transmission rate regardless of network feedback.
/// The cwnd is set to a very large value so cwnd-based flow control never
/// blocks; all rate limiting happens via the token bucket.
///
/// Thread-safe via atomic operations for flightsize tracking.
pub struct FixedRateController<T: TimeSource = RealTime> {
    /// Configured rate in bytes/sec.
    rate: usize,

    /// Current bytes in flight (sent but not yet ACKed).
    /// Uses saturating arithmetic to handle edge cases.
    flightsize: AtomicUsize,

    /// Time source (for interface compatibility, not actually used).
    #[allow(dead_code)]
    time_source: T,
}

impl FixedRateController<RealTime> {
    /// Create a new fixed-rate controller with default config and real time.
    pub fn new(config: FixedRateConfig) -> Self {
        Self::new_with_time_source(config, RealTime::new())
    }
}

impl<T: TimeSource> FixedRateController<T> {
    /// Create a new fixed-rate controller with custom time source.
    pub fn new_with_time_source(config: FixedRateConfig, time_source: T) -> Self {
        Self {
            rate: config.rate_bytes_per_sec,
            flightsize: AtomicUsize::new(0),
            time_source,
        }
    }

    /// Called when bytes are sent.
    pub fn on_send(&self, bytes: usize) {
        self.flightsize.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Called when an ACK is received with RTT sample.
    /// The RTT is ignored since we don't adapt to network conditions.
    pub fn on_ack(&self, _rtt_sample: Duration, bytes_acked: usize) {
        self.on_ack_without_rtt(bytes_acked);
    }

    /// Called when an ACK is received for a retransmitted packet.
    pub fn on_ack_without_rtt(&self, bytes_acked: usize) {
        // Saturating subtraction to handle edge cases
        self.flightsize
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(bytes_acked))
            })
            .ok();
    }

    /// Called when packet loss is detected.
    /// Ignored - we don't adapt to loss.
    pub fn on_loss(&self) {
        // No-op: fixed rate doesn't respond to loss
    }

    /// Called when a retransmission timeout occurs.
    /// Ignored - we don't adapt to timeouts.
    pub fn on_timeout(&self) {
        // No-op: fixed rate doesn't respond to timeouts
    }

    /// Returns a very large cwnd so cwnd-based flow control never blocks.
    /// All rate limiting is done by the token bucket.
    pub fn current_cwnd(&self) -> usize {
        // Use a large but not overflow-prone value
        // This ensures flightsize + packet_size <= cwnd is always true
        usize::MAX / 2
    }

    /// Returns the configured fixed rate.
    pub fn current_rate(&self, _rtt: Duration) -> usize {
        self.rate
    }

    /// Returns current bytes in flight.
    pub fn flightsize(&self) -> usize {
        self.flightsize.load(Ordering::Relaxed)
    }

    /// Returns zero - we don't track base delay.
    pub fn base_delay(&self) -> Duration {
        Duration::ZERO
    }

    /// Returns zero - we don't track queuing delay.
    pub fn queuing_delay(&self) -> Duration {
        Duration::ZERO
    }

    /// Returns the cwnd (same as current_cwnd for fixed rate).
    pub fn peak_cwnd(&self) -> usize {
        self.current_cwnd()
    }

    /// Returns the configured rate.
    pub fn rate(&self) -> usize {
        self.rate
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::simulation::VirtualTime;

    #[test]
    fn test_default_config() {
        let config = FixedRateConfig::default();
        assert_eq!(config.rate_bytes_per_sec, 12_500_000); // 100 Mbps
    }

    #[test]
    fn test_from_mbps() {
        let config = FixedRateConfig::from_mbps(100);
        assert_eq!(config.rate_bytes_per_sec, 12_500_000); // 100 Mbps
    }

    #[test]
    fn test_flightsize_tracking() {
        let controller = FixedRateController::new(FixedRateConfig::default());

        assert_eq!(controller.flightsize(), 0);

        controller.on_send(1000);
        assert_eq!(controller.flightsize(), 1000);

        controller.on_send(500);
        assert_eq!(controller.flightsize(), 1500);

        controller.on_ack(Duration::from_millis(100), 800);
        assert_eq!(controller.flightsize(), 700);

        controller.on_ack_without_rtt(700);
        assert_eq!(controller.flightsize(), 0);
    }

    #[test]
    fn test_flightsize_saturating() {
        let controller = FixedRateController::new(FixedRateConfig::default());

        controller.on_send(100);
        // ACK more than we sent - should saturate to 0, not underflow
        controller.on_ack_without_rtt(200);
        assert_eq!(controller.flightsize(), 0);
    }

    #[test]
    fn test_cwnd_always_large() {
        let controller = FixedRateController::new(FixedRateConfig::default());

        // cwnd should be large enough that any reasonable flightsize fits
        let cwnd = controller.current_cwnd();
        assert!(cwnd > 1_000_000_000); // At least 1GB
    }

    #[test]
    fn test_rate_constant() {
        let config = FixedRateConfig::from_mbps(50);
        let controller = FixedRateController::new(config);

        // Rate should be constant regardless of RTT
        assert_eq!(
            controller.current_rate(Duration::from_millis(10)),
            6_250_000
        );
        assert_eq!(
            controller.current_rate(Duration::from_millis(100)),
            6_250_000
        );
        assert_eq!(
            controller.current_rate(Duration::from_millis(1000)),
            6_250_000
        );
    }

    #[test]
    fn test_ignores_loss_and_timeout() {
        let controller = FixedRateController::new(FixedRateConfig::default());

        controller.on_send(1000);
        let flightsize_before = controller.flightsize();

        // Loss and timeout should not affect flightsize
        controller.on_loss();
        assert_eq!(controller.flightsize(), flightsize_before);

        controller.on_timeout();
        assert_eq!(controller.flightsize(), flightsize_before);

        // cwnd should remain large
        assert!(controller.current_cwnd() > 1_000_000_000);
    }

    #[test]
    fn test_with_virtual_time() {
        let time_source = VirtualTime::new();
        let config = FixedRateConfig::from_mbps(25);
        let controller = FixedRateController::new_with_time_source(config, time_source);

        assert_eq!(controller.rate(), 3_125_000);
        assert_eq!(controller.flightsize(), 0);
    }
}

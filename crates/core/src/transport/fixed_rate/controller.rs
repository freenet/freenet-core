//! Fixed-rate congestion controller implementation.
//!
//! Maintains a constant transmission rate with a loss-pause mechanism:
//! when packet loss or retransmission timeout is detected, the cwnd is
//! temporarily capped at the current flightsize. This prevents new data
//! from competing with retransmissions, breaking the congestion cascade
//! that causes stream stalls. The cap is released after a successful ACK.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use crate::simulation::{RealTime, TimeSource};

/// Default rate: 10 Mbps in bytes/sec (10 * 1_000_000 / 8)
///
/// This rate is chosen to:
/// - Support contract retrieval without saturating residential connections
/// - Account for sequential multi-hop transfer (3-5x slower than single hop)
/// - With 10 connections, total throughput is ~100 Mbps — within most residential
///   upload bandwidth limits
/// - Previous 100 Mbps per-connection rate caused residential ISPs/routers to
///   throttle or drop all connections (see diagnostic report MEB6RV)
pub const DEFAULT_RATE_BYTES_PER_SEC: usize = 1_250_000;

/// Configuration for the fixed-rate controller.
#[derive(Debug, Clone)]
pub struct FixedRateConfig {
    /// Target transmission rate in bytes per second.
    /// Default: 10 Mbps (1,250,000 bytes/sec)
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
/// Maintains a constant transmission rate with a loss-pause mechanism.
/// When loss or timeout is detected, the cwnd is temporarily capped at
/// the current flightsize, preventing new data from being sent until
/// ACKs reduce the flightsize. This gives retransmissions room to
/// succeed without competing with new data.
///
/// The pause is automatically released on the next successful ACK.
///
/// Thread-safe via atomic operations for flightsize and pause tracking.
pub struct FixedRateController<T: TimeSource = RealTime> {
    /// Configured rate in bytes/sec.
    rate: usize,

    /// Current bytes in flight (sent but not yet ACKed).
    /// Uses saturating arithmetic to handle edge cases.
    flightsize: AtomicUsize,

    /// When true, cwnd is capped at flightsize instead of usize::MAX/2.
    /// Set on loss/timeout, cleared on successful ACK.
    loss_pause: AtomicBool,

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
            loss_pause: AtomicBool::new(false),
            time_source,
        }
    }

    /// Called when bytes are sent.
    pub fn on_send(&self, bytes: usize) {
        self.flightsize.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Called when an ACK is received with RTT sample.
    /// The RTT is ignored since we don't adapt to network conditions.
    /// Clears the loss pause so new data can resume flowing.
    pub fn on_ack(&self, _rtt_sample: Duration, bytes_acked: usize) {
        self.on_ack_without_rtt(bytes_acked);
    }

    /// Called when an ACK is received for a retransmitted packet.
    /// Clears the loss pause so new data can resume flowing.
    pub fn on_ack_without_rtt(&self, bytes_acked: usize) {
        // Clear the loss pause — an ACK means the path is working again.
        self.loss_pause.store(false, Ordering::Release);
        // Saturating subtraction to handle edge cases
        self.flightsize
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(bytes_acked))
            })
            .ok();
    }

    /// Called when packet loss is detected.
    /// Activates the loss pause: cwnd is capped at current flightsize
    /// until the next successful ACK, preventing new data from competing
    /// with retransmissions.
    pub fn on_loss(&self) {
        self.loss_pause.store(true, Ordering::Release);
    }

    /// Called when a retransmission timeout occurs.
    /// Activates the loss pause (same as on_loss).
    pub fn on_timeout(&self) {
        self.loss_pause.store(true, Ordering::Release);
    }

    /// Returns the effective congestion window.
    ///
    /// Normally returns a very large value so cwnd never blocks (all rate
    /// limiting is done by the token bucket). When loss_pause is active,
    /// returns the current flightsize — this prevents any new data from
    /// being sent until ACKs reduce the flightsize, giving retransmissions
    /// exclusive access to the link.
    pub fn current_cwnd(&self) -> usize {
        if self.loss_pause.load(Ordering::Acquire) {
            // Cap at current flightsize: no new data until ACKs arrive.
            // Both flightsize() and current_cwnd() read the same AtomicUsize,
            // so this makes the send check `flightsize + packet_size <= cwnd`
            // permanently false while loss_pause is active. This is intentional:
            // loss_pause is cleared by on_ack(), at which point current_cwnd()
            // returns usize::MAX/2 and sends resume immediately.
            // The CWND_WAIT_TIMEOUT in send_stream/pipe_stream catches dead
            // connections where ACKs never arrive.
            self.flightsize.load(Ordering::Relaxed)
        } else {
            usize::MAX / 2
        }
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
        assert_eq!(config.rate_bytes_per_sec, 1_250_000); // 10 Mbps
    }

    #[test]
    fn test_from_mbps() {
        let config = FixedRateConfig::from_mbps(10);
        assert_eq!(config.rate_bytes_per_sec, 1_250_000); // 10 Mbps
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
    fn test_loss_pause_caps_cwnd() {
        let controller = FixedRateController::new(FixedRateConfig::default());

        controller.on_send(1000);
        let flightsize_before = controller.flightsize();

        // Before loss: cwnd is large
        assert!(controller.current_cwnd() > 1_000_000_000);

        // Loss activates pause: cwnd capped at flightsize (blocks all new sends)
        controller.on_loss();
        assert_eq!(controller.flightsize(), flightsize_before);
        assert_eq!(controller.current_cwnd(), flightsize_before);

        // Timeout also activates pause
        controller.on_timeout();
        assert_eq!(controller.current_cwnd(), flightsize_before);

        // ACK clears the pause and restores large cwnd
        controller.on_ack(Duration::from_millis(50), 500);
        assert!(controller.current_cwnd() > 1_000_000_000);
        assert_eq!(controller.flightsize(), 500); // 1000 - 500
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

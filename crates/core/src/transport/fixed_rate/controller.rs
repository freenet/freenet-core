//! Fixed-rate congestion controller implementation.
//!
//! Maintains a constant transmission rate with a loss-pause mechanism:
//! when packet loss or retransmission timeout is detected, the cwnd is
//! temporarily capped at the current flightsize. This prevents new data
//! from competing with retransmissions, breaking the congestion cascade
//! that causes stream stalls. The cap is released after a successful ACK.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crate::simulation::{RealTime, TimeSource};
use crate::transport::packet_data::MAX_PACKET_SIZE;

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

/// Margin added to cwnd during loss_pause recovery.
///
/// When loss_pause is active, cwnd = flightsize + this margin. The margin
/// allows continued forward progress during recovery so a single loss event
/// doesn't stall the entire stream. Any single ACK clears loss_pause
/// entirely, so the margin just needs to sustain enough data flow for at
/// least one ACK to arrive.
///
/// Set to 50 max-size packets (~60KB). At 5% packet loss, the probability
/// of all 50 packets being lost is 0.05^50 ≈ 10^-65 — effectively zero.
/// This guarantees at least one packet gets through to trigger an ACK,
/// clearing the pause. The margin is still conservative: 60KB is well below
/// the 125KB token bucket capacity, so loss_pause still restricts the
/// sending rate during recovery.
///
/// Previously set to 2 packets (2400 bytes), which caused production stream
/// stalls: if both trickle packets were lost, the sender blocked for the
/// full CWND_WAIT_TIMEOUT before aborting. This was observed as ~10
/// cwnd timeouts/hour on the gateway, causing GET failures for users.
const LOSS_PAUSE_MARGIN: usize = 50 * MAX_PACKET_SIZE;

// Guard against future regressions to a dangerously small margin.
const _: () = assert!(
    LOSS_PAUSE_MARGIN >= 10 * MAX_PACKET_SIZE,
    "LOSS_PAUSE_MARGIN must allow enough packets for reliable recovery under loss"
);

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

    /// When non-zero, cwnd is capped at this value + LOSS_PAUSE_MARGIN.
    /// Set to the flightsize at loss/timeout time, reset to 0 on successful ACK.
    /// Using AtomicUsize instead of AtomicBool so we capture the flightsize
    /// at the moment of loss — this prevents the margin from sliding upward
    /// as new packets are sent during recovery.
    loss_pause_cwnd: AtomicUsize,

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
            loss_pause_cwnd: AtomicUsize::new(0),
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
        self.loss_pause_cwnd.store(0, Ordering::Release);
        // Saturating subtraction to handle edge cases
        self.flightsize
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(bytes_acked))
            })
            .ok();
    }

    /// Called when packet loss is detected.
    /// Captures the current flightsize and caps cwnd at that level
    /// (plus margin) until the next successful ACK.
    pub fn on_loss(&self) {
        let fs = self.flightsize.load(Ordering::Relaxed);
        self.loss_pause_cwnd.store(fs.max(1), Ordering::Release);
    }

    /// Called when a retransmission timeout occurs.
    /// Activates the loss pause (same as on_loss).
    pub fn on_timeout(&self) {
        let fs = self.flightsize.load(Ordering::Relaxed);
        self.loss_pause_cwnd.store(fs.max(1), Ordering::Release);
    }

    /// Returns the effective congestion window.
    ///
    /// Normally returns a very large value so cwnd never blocks (all rate
    /// limiting is done by the token bucket). When loss_pause is active,
    /// returns the flightsize captured at loss time + LOSS_PAUSE_MARGIN.
    /// The captured value is frozen per loss event, but note that successive
    /// retransmission timeouts will re-capture at the current (higher)
    /// flightsize, effectively sliding the cap upward. This is acceptable
    /// because the token bucket is the real rate limiter for FixedRate;
    /// loss_pause primarily prevents complete stalls, not rate reduction.
    pub fn current_cwnd(&self) -> usize {
        let paused_at = self.loss_pause_cwnd.load(Ordering::Acquire);
        if paused_at > 0 {
            paused_at + LOSS_PAUSE_MARGIN
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
    fn test_loss_pause_caps_cwnd_with_margin() {
        let controller = FixedRateController::new(FixedRateConfig::default());

        controller.on_send(1000);
        let flightsize_at_loss = controller.flightsize();

        // Before loss: cwnd is large
        assert!(controller.current_cwnd() > 1_000_000_000);

        // Loss captures flightsize and caps cwnd
        controller.on_loss();
        assert_eq!(
            controller.current_cwnd(),
            flightsize_at_loss + LOSS_PAUSE_MARGIN
        );

        // Sending more data increases flightsize but NOT the cwnd cap
        // (cwnd is frozen at the flightsize captured at loss time)
        controller.on_send(2000);
        assert_eq!(controller.flightsize(), 3000);
        assert_eq!(
            controller.current_cwnd(),
            flightsize_at_loss + LOSS_PAUSE_MARGIN,
            "cwnd must be frozen at loss-time flightsize, not current flightsize"
        );

        // ACK clears the pause and restores large cwnd
        controller.on_ack(Duration::from_millis(50), 500);
        assert!(controller.current_cwnd() > 1_000_000_000);
        assert_eq!(controller.flightsize(), 2500); // 3000 - 500
    }

    /// Regression test for #3702: loss_pause must allow at least one packet
    /// through, otherwise the cwnd check `flightsize + packet_size <= cwnd`
    /// can never pass and the connection stalls completely.
    #[test]
    fn test_loss_pause_allows_packet_then_blocks() {
        let controller = FixedRateController::new(FixedRateConfig::default());
        let packet_size = MAX_PACKET_SIZE;

        controller.on_send(5000);
        controller.on_loss();

        let flightsize = controller.flightsize();
        let cwnd = controller.current_cwnd();

        // The margin must allow at least one packet through
        assert!(
            flightsize + packet_size <= cwnd,
            "loss_pause cwnd ({cwnd}) must allow at least one packet \
             (flightsize={flightsize}, packet_size={packet_size}). \
             Without margin, sending stalls completely. See #3702."
        );

        // After consuming the full margin, it blocks
        controller.on_send(LOSS_PAUSE_MARGIN);
        let new_flightsize = controller.flightsize();
        assert!(
            new_flightsize + packet_size > cwnd,
            "After consuming the margin, loss_pause should block further sending \
             (flightsize={new_flightsize}, cwnd={cwnd})"
        );
    }

    /// Regression test: with the old 2-packet margin, a single loss event during
    /// a 1MB stream transfer could stall the sender for the full CWND_WAIT_TIMEOUT
    /// if both trickle packets were also lost. The 50-packet margin ensures that
    /// even at 20% loss, the sender can sustain enough data flow for ACKs to
    /// arrive and clear the pause.
    #[test]
    fn test_loss_pause_margin_sustains_progress_under_loss() {
        let controller = FixedRateController::new(FixedRateConfig::default());

        // Simulate a 1MB stream: 100KB already sent and in flight
        let initial_flightsize = 100_000;
        controller.on_send(initial_flightsize);
        controller.on_timeout();

        let cwnd = controller.current_cwnd();
        let margin = cwnd - initial_flightsize;

        // The margin should allow many packets, not just 2
        let packets_allowed = margin / MAX_PACKET_SIZE;
        assert!(
            packets_allowed >= 20,
            "loss_pause margin should allow at least 20 packets for recovery, \
             got {packets_allowed} (margin={margin}B). A 2-packet margin caused \
             production stream stalls when both trickle packets were lost."
        );
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

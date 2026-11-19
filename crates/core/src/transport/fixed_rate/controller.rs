//! Fixed-rate congestion controller implementation.
//!
//! Maintains a constant transmission rate with a loss-pause mechanism:
//! when packet loss or retransmission timeout is detected, the cwnd is
//! temporarily capped at the current flightsize. This prevents new data
//! from competing with retransmissions, breaking the congestion cascade
//! that causes stream stalls. The cap is released after a successful ACK.
//!
//! Independently of the loss pause, cwnd never exceeds a flight-size ceiling
//! (`MAX_FLIGHT_SECS` of data at the configured rate), so the unconfirmed data
//! a connection keeps for retransmission stays bounded whatever the remote
//! chooses to confirm.

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

/// Upper bound on flight size (bytes sent and neither confirmed nor
/// abandoned), in seconds of sending at the configured rate.
///
/// The token bucket bounds how fast data is sent, not how much of it stays
/// unconfirmed, and the `SentPacketTracker` keeps every unconfirmed packet for
/// retransmission. Which packets are confirmed is the remote's choice, so this
/// bound must not depend on receipts or timeouts: a remote that confirms one
/// packet now and then clears the loss pause each time, and would otherwise
/// be sent new data at the full rate while the tracker kept all of it.
///
/// A remote that sends receipts keeps flight size near
/// `rate * (RTT + receipt delay)`, well under a second of data. The rest is
/// room for a path that goes quiet for a few seconds and recovers (the 3.4 s
/// RTT spike of `link_model::r1_tests`). A stream that finds the ceiling full
/// waits for receipts and fails after `CWND_WAIT_TIMEOUT`.
pub(crate) const MAX_FLIGHT_SECS: usize = 8;

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
    /// Activates the loss pause (caps cwnd at the current flightsize). Flight
    /// size is unchanged: the timed-out packet is immediately re-sent and stays
    /// in flight.
    pub fn on_timeout(&self) {
        let fs = self.flightsize.load(Ordering::Relaxed);
        self.loss_pause_cwnd.store(fs.max(1), Ordering::Release);
    }

    /// Release an abandoned packet's bytes from flight size (issue #4345).
    ///
    /// Called when a packet is permanently abandoned after
    /// `MAX_PACKET_RETRANSMITS`. This is what lets a flight size pinned by a
    /// never-ACKed packet actually drain (otherwise the frozen
    /// `loss_pause_cwnd + LOSS_PAUSE_MARGIN` ceiling stalls every subsequent
    /// stream — 64% of live `cwnd wait timeout` failures). Saturating; does NOT
    /// touch the loss pause (only an ACK clears that).
    pub fn release_flightsize(&self, bytes: usize) {
        self.flightsize
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(bytes))
            })
            .ok();
    }

    /// Returns the effective congestion window.
    ///
    /// Normally returns the flight-size ceiling (see `MAX_FLIGHT_SECS`), which
    /// a peer that sends receipts never approaches, so the token bucket does
    /// the rate limiting. When loss_pause is active, returns the flightsize
    /// captured at loss time + LOSS_PAUSE_MARGIN, at most the ceiling.
    /// The captured value is frozen per loss event, but note that successive
    /// retransmission timeouts will re-capture at the current (higher)
    /// flightsize, effectively sliding the cap upward, and any receipt clears
    /// it. So loss_pause prevents complete stalls; it does not bound flight
    /// size. The ceiling does.
    pub fn current_cwnd(&self) -> usize {
        let ceiling = self.flight_ceiling();
        let paused_at = self.loss_pause_cwnd.load(Ordering::Acquire);
        if paused_at > 0 {
            paused_at.saturating_add(LOSS_PAUSE_MARGIN).min(ceiling)
        } else {
            ceiling
        }
    }

    /// `MAX_FLIGHT_SECS` of data at the configured rate, and at least
    /// LOSS_PAUSE_MARGIN so a very low rate still leaves the loss pause its
    /// margin.
    fn flight_ceiling(&self) -> usize {
        self.rate
            .saturating_mul(MAX_FLIGHT_SECS)
            .max(LOSS_PAUSE_MARGIN)
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

    /// Outside a loss pause cwnd is the flight-size ceiling: `MAX_FLIGHT_SECS`
    /// of data at the configured rate, far above what a peer that sends
    /// receipts leaves in flight, but bounded.
    #[test]
    fn test_cwnd_is_the_flight_ceiling() {
        let controller = FixedRateController::new(FixedRateConfig::default());
        assert_eq!(
            controller.current_cwnd(),
            MAX_FLIGHT_SECS * DEFAULT_RATE_BYTES_PER_SEC
        );

        // The ceiling follows the configured rate.
        let fast = FixedRateController::new(FixedRateConfig::from_mbps(50));
        assert_eq!(fast.current_cwnd(), MAX_FLIGHT_SECS * 6_250_000);

        // A very low or zero rate still leaves the loss pause its margin.
        let slow = FixedRateController::new(FixedRateConfig::new(1_000));
        assert_eq!(slow.current_cwnd(), LOSS_PAUSE_MARGIN);
        let zero = FixedRateController::new(FixedRateConfig::new(0));
        assert_eq!(zero.current_cwnd(), LOSS_PAUSE_MARGIN);

        // An absurd rate does not overflow.
        let huge = FixedRateController::new(FixedRateConfig::new(usize::MAX));
        assert_eq!(huge.current_cwnd(), usize::MAX);
    }

    /// A remote that sends a receipt for only an occasional packet must not
    /// grow flight size without bound.
    ///
    /// Each receipt clears the loss pause and each retransmission timeout
    /// captures it again at the current flight size, so the loss pause never
    /// holds such a sender back. Before the ceiling cwnd was then
    /// `usize::MAX / 2`, so the sender kept sending at the full rate and the
    /// `SentPacketTracker` kept every packet the remote did not confirm.
    #[test]
    fn test_flight_ceiling_holds_against_sparse_receipts() {
        let controller = FixedRateController::new(FixedRateConfig::default());
        let packet_size = MAX_PACKET_SIZE;
        let ceiling = MAX_FLIGHT_SECS * DEFAULT_RATE_BYTES_PER_SEC;

        // Offer ten times the ceiling, sending as `send_stream` does (only
        // while the packet fits in cwnd). One packet in 100 is confirmed;
        // the others time out.
        let offered = 10 * ceiling / packet_size;
        let mut sent = 0usize;
        for i in 0..offered {
            if controller.flightsize() + packet_size <= controller.current_cwnd() {
                controller.on_send(packet_size);
                sent += 1;
            }
            if i % 100 == 0 {
                controller.on_ack(Duration::from_millis(50), packet_size);
            } else if i % 100 == 50 {
                controller.on_timeout();
            }
            assert!(
                controller.flightsize() <= ceiling,
                "flight size {} passed the ceiling {ceiling} after {sent} packets",
                controller.flightsize()
            );
        }
        // The window filled, so the ceiling is what stopped the sender, and
        // from then on only what the receipts released was sent.
        assert!(controller.flightsize() + 2 * packet_size > ceiling);
        assert!(
            sent <= ceiling / packet_size + offered / 100 + 1,
            "sent {sent} of {offered} offered packets"
        );
    }

    /// The loss pause never lifts cwnd above the ceiling, including when
    /// short messages (which do not wait on cwnd) pushed flight size past it.
    #[test]
    fn test_loss_pause_does_not_exceed_the_flight_ceiling() {
        let controller = FixedRateController::new(FixedRateConfig::default());
        let packet_size = MAX_PACKET_SIZE;
        let ceiling = MAX_FLIGHT_SECS * DEFAULT_RATE_BYTES_PER_SEC;

        for _ in 0..ceiling / packet_size {
            controller.on_send(packet_size);
        }
        assert!(controller.flightsize() + packet_size > controller.current_cwnd());

        controller.on_timeout();
        assert_eq!(controller.current_cwnd(), ceiling);

        controller.on_send(2 * packet_size);
        controller.on_timeout();
        assert_eq!(controller.current_cwnd(), ceiling);
        assert!(controller.flightsize() + packet_size > controller.current_cwnd());

        // Once receipts bring flight size back under the ceiling, sending
        // resumes.
        controller.on_ack(Duration::from_millis(50), 3 * packet_size);
        assert!(controller.flightsize() + packet_size <= controller.current_cwnd());
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

        // Before loss: cwnd is the flight-size ceiling
        assert_eq!(
            controller.current_cwnd(),
            MAX_FLIGHT_SECS * DEFAULT_RATE_BYTES_PER_SEC
        );

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

        // ACK clears the pause and restores the ceiling
        controller.on_ack(Duration::from_millis(50), 500);
        assert_eq!(
            controller.current_cwnd(),
            MAX_FLIGHT_SECS * DEFAULT_RATE_BYTES_PER_SEC
        );
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

    /// Regression test for issue #4345: flightsize must be released when a
    /// stream's packets are abandoned after repeated retransmission timeouts
    /// with no ACK ever arriving.
    ///
    /// FixedRate is the production default. Live telemetry (issue #4345)
    /// showed 64% of all `cwnd wait timeout` failures clustered at
    /// `cwnd ≈ 60-70KB` (LOSS_PAUSE_MARGIN + a small frozen `loss_pause_cwnd`)
    /// with the cwnd-flightsize gap pinned at ~110 bytes — below one packet.
    ///
    /// Mechanism: on a lossy path the reverse ACK channel is starved, so
    /// `on_ack_without_rtt` (the ONLY place flightsize is decremented AND the
    /// only place loss_pause is cleared) never runs. The sending stream keeps
    /// calling `on_send`, flightsize climbs to the frozen cwnd ceiling, and
    /// the cwnd-wait loop `flightsize + packet_size <= cwnd` can never find
    /// headroom → 3s abort at ~39% of the stream. Worse, because flightsize
    /// is never released, the NEXT stream on the same connection starts
    /// already pinned (telemetry: `sent 0/2`).
    ///
    /// This test sends a burst that is never ACKed, then releases each packet
    /// as abandonment would, and asserts a fresh packet can be sent — i.e.
    /// flightsize was drained. Before the fix there was no release path, so
    /// flightsize stayed at the full burst and the new packet could not fit.
    #[test]
    fn test_issue_4345_release_flightsize_drains_abandoned_bytes() {
        let controller = FixedRateController::new(FixedRateConfig::default());

        let packet_size = MAX_PACKET_SIZE;
        let num_packets = 60usize; // > LOSS_PAUSE_MARGIN / MAX_PACKET_SIZE (50)

        // Send a burst; none will ever be ACKed (dead reverse ACK path).
        for _ in 0..num_packets {
            controller.on_send(packet_size);
        }
        assert_eq!(
            controller.flightsize(),
            num_packets * packet_size,
            "flightsize should account for every in-flight byte after send"
        );

        // on_timeout() (per-RTO) caps cwnd via loss_pause but must NOT change
        // flight size — the packet is still in flight, being retransmitted.
        controller.on_timeout();
        assert_eq!(
            controller.flightsize(),
            num_packets * packet_size,
            "on_timeout() must not change flight size; only abandonment/ACK does"
        );

        // After each packet is abandoned (MAX_PACKET_RETRANSMITS), the recv
        // loop calls release_flightsize(). Without it, flightsize leaks for the
        // life of the connection and stalls every subsequent stream.
        for _ in 0..num_packets {
            controller.release_flightsize(packet_size);
        }

        let leaked = controller.flightsize();
        assert!(
            leaked <= packet_size,
            "issue #4345: flightsize leaked after abandonment — expected ~0, \
             got {leaked} B ({} packets still counted in flight). \
             release_flightsize() must drain abandoned in-flight bytes.",
            leaked / packet_size,
        );

        // The connection must be usable again: a fresh packet must fit in cwnd.
        let cwnd = controller.current_cwnd();
        assert!(
            controller.flightsize() + packet_size <= cwnd,
            "issue #4345: after abandonment a fresh packet ({packet_size} B) \
             must fit in cwnd ({cwnd} B) but flightsize is still {} B — the \
             cwnd-wait loop would never open and every subsequent stream aborts \
             with 'cwnd wait timeout'",
            controller.flightsize(),
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

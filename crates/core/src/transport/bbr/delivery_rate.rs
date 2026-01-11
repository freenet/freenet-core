//! Per-packet delivery rate estimation.
//!
//! BBRv3 estimates the delivery rate by tracking how many bytes are delivered
//! over what time interval. This module provides per-packet metadata tracking
//! and delivery rate computation.
//!
//! ## Design
//!
//! Each sent packet carries a "token" with:
//! - `delivered_at_send`: Total bytes delivered when this packet was sent
//! - `sent_time_nanos`: Timestamp when the packet was sent
//!
//! When an ACK arrives:
//! - `delivery_rate = (current_delivered - token.delivered_at_send) / (now - token.sent_time_nanos)`
//!
//! This approach is more accurate than simple `bytes_acked / rtt` because it
//! measures delivery progress over the packet's actual flight time.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

/// Token attached to each sent packet for delivery rate computation.
///
/// When a packet is sent, we record the current state of the delivery rate
/// sampler. When the ACK arrives, we use this token to compute the delivery
/// rate over the packet's flight time.
#[derive(Debug, Clone, Copy)]
pub struct DeliveryRateToken {
    /// Total bytes delivered when this packet was sent.
    pub delivered: u64,
    /// Timestamp when this packet was sent (nanoseconds).
    pub sent_time_nanos: u64,
    /// Total bytes sent when this packet was sent.
    pub sent: u64,
    /// Whether the sender was application-limited when this packet was sent.
    pub is_app_limited: bool,
    /// Bytes delivered at the time of the first sent packet in the current burst.
    pub first_sent_delivered: u64,
    /// Timestamp of the first sent packet in the current burst.
    pub first_sent_time_nanos: u64,
}

/// Delivery rate sample computed from an ACK.
#[derive(Debug, Clone, Copy)]
pub(crate) struct DeliveryRateSample {
    /// Delivery rate in bytes/sec.
    pub delivery_rate: u64,
    /// Whether this sample is application-limited (and thus not reliable for max_bw).
    pub is_app_limited: bool,
    /// Bytes delivered in this ACK.
    pub delivered: u64,
    /// Time interval over which delivery was measured (nanoseconds).
    pub interval_nanos: u64,
    /// RTT for this packet.
    pub rtt: Duration,
    /// Number of bytes lost (for loss detection).
    pub lost: u64,
}

/// Delivery rate sampler for per-packet tracking.
///
/// This sampler maintains state needed to compute delivery rates from ACKs.
/// It tracks total bytes delivered, bytes sent, and the timing of the first
/// packet in the current "burst" of packets.
pub(crate) struct DeliveryRateSampler {
    /// Total bytes delivered (acknowledged).
    delivered: AtomicU64,
    /// Total bytes sent.
    sent: AtomicU64,
    /// Total bytes lost.
    lost: AtomicU64,
    /// Timestamp of the first sent packet in the current burst.
    first_sent_time_nanos: AtomicU64,
    /// Bytes delivered when the first packet in the current burst was sent.
    first_sent_delivered: AtomicU64,
    /// Whether the connection is currently application-limited.
    /// True if cwnd > flightsize (we could send more but have nothing to send).
    app_limited: AtomicBool,
    /// Round counter (increments each time we send after receiving an ACK).
    round_count: AtomicU64,
    /// Delivered count at the start of the current round.
    round_start_delivered: AtomicU64,
}

impl DeliveryRateSampler {
    /// Create a new delivery rate sampler.
    pub(crate) fn new() -> Self {
        Self {
            delivered: AtomicU64::new(0),
            sent: AtomicU64::new(0),
            lost: AtomicU64::new(0),
            first_sent_time_nanos: AtomicU64::new(0),
            first_sent_delivered: AtomicU64::new(0),
            app_limited: AtomicBool::new(false),
            round_count: AtomicU64::new(0),
            round_start_delivered: AtomicU64::new(0),
        }
    }

    /// Called when a packet is sent.
    ///
    /// # Arguments
    /// * `bytes` - Size of the packet in bytes
    /// * `now_nanos` - Current time in nanoseconds
    /// * `inflight` - Current bytes in flight (before this send)
    ///
    /// # Returns
    /// A token to attach to the packet for delivery rate computation on ACK.
    pub(crate) fn on_send(
        &self,
        bytes: usize,
        now_nanos: u64,
        inflight: usize,
    ) -> DeliveryRateToken {
        let delivered = self.delivered.load(Ordering::Acquire);
        let sent_before = self.sent.fetch_add(bytes as u64, Ordering::AcqRel);

        // If this is the first packet in a new burst (no packets in flight),
        // record the current state for the burst.
        let (first_sent_delivered, first_sent_time_nanos) = if inflight == 0 {
            self.first_sent_delivered
                .store(delivered, Ordering::Release);
            self.first_sent_time_nanos
                .store(now_nanos, Ordering::Release);
            (delivered, now_nanos)
        } else {
            (
                self.first_sent_delivered.load(Ordering::Acquire),
                self.first_sent_time_nanos.load(Ordering::Acquire),
            )
        };

        let is_app_limited = self.app_limited.load(Ordering::Acquire);

        DeliveryRateToken {
            delivered,
            sent_time_nanos: now_nanos,
            sent: sent_before + bytes as u64,
            is_app_limited,
            first_sent_delivered,
            first_sent_time_nanos,
        }
    }

    /// Called when an ACK is received.
    ///
    /// # Arguments
    /// * `token` - The token from the acknowledged packet
    /// * `bytes_acked` - Number of bytes acknowledged
    /// * `now_nanos` - Current time in nanoseconds
    ///
    /// # Returns
    /// A delivery rate sample, or `None` if the sample is invalid.
    pub(crate) fn on_ack(
        &self,
        token: DeliveryRateToken,
        bytes_acked: usize,
        now_nanos: u64,
    ) -> Option<DeliveryRateSample> {
        // Update delivered count
        let delivered_before = self
            .delivered
            .fetch_add(bytes_acked as u64, Ordering::AcqRel);
        let delivered_now = delivered_before + bytes_acked as u64;

        // Compute delivery interval: use the longer of send/ack intervals
        // to handle cases where packets are bunched up.
        let send_elapsed = now_nanos.saturating_sub(token.first_sent_time_nanos);
        let ack_elapsed = now_nanos.saturating_sub(token.sent_time_nanos);
        let interval_nanos = send_elapsed.max(ack_elapsed);

        if interval_nanos == 0 {
            return None;
        }

        // Compute bytes delivered since first packet in burst
        let delivered_bytes = delivered_now.saturating_sub(token.first_sent_delivered);

        // Compute delivery rate (bytes/sec)
        // delivery_rate = delivered_bytes / interval_nanos * 1_000_000_000
        let delivery_rate = if delivered_bytes > 0 {
            (delivered_bytes as u128 * 1_000_000_000 / interval_nanos as u128) as u64
        } else {
            0
        };

        // Cap delivery rate to send rate to prevent overestimation when ACKs arrive in bursts.
        // This follows the quiche implementation: delivery_rate = min(send_rate, ack_rate)
        let bytes_sent_since_token = self.sent.load(Ordering::Acquire).saturating_sub(token.sent);
        let send_rate = if bytes_sent_since_token > 0 && interval_nanos > 0 {
            (bytes_sent_since_token as u128 * 1_000_000_000 / interval_nanos as u128) as u64
        } else {
            u64::MAX // No cap if we can't compute send rate
        };
        let delivery_rate = delivery_rate.min(send_rate);

        // Compute RTT for this packet
        let rtt_nanos = now_nanos.saturating_sub(token.sent_time_nanos);
        let rtt = Duration::from_nanos(rtt_nanos);

        // Check if we've started a new round
        if delivered_now > self.round_start_delivered.load(Ordering::Acquire) {
            self.round_count.fetch_add(1, Ordering::AcqRel);
            self.round_start_delivered
                .store(delivered_now, Ordering::Release);
        }

        Some(DeliveryRateSample {
            delivery_rate,
            is_app_limited: token.is_app_limited,
            delivered: delivered_bytes,
            interval_nanos,
            rtt,
            lost: 0, // Will be set by caller if there are losses
        })
    }

    /// Called when packets are detected as lost.
    ///
    /// # Arguments
    /// * `bytes_lost` - Number of bytes lost
    pub(crate) fn on_loss(&self, bytes_lost: usize) {
        self.lost.fetch_add(bytes_lost as u64, Ordering::AcqRel);
    }

    /// Mark the connection as application-limited.
    ///
    /// Call this when cwnd > flightsize and there's no more data to send.
    pub(crate) fn set_app_limited(&self, app_limited: bool) {
        self.app_limited.store(app_limited, Ordering::Release);
    }

    /// Check if the connection is application-limited.
    pub(crate) fn is_app_limited(&self) -> bool {
        self.app_limited.load(Ordering::Acquire)
    }

    /// Get total bytes delivered.
    pub(crate) fn delivered(&self) -> u64 {
        self.delivered.load(Ordering::Acquire)
    }

    /// Get total bytes lost.
    pub(crate) fn lost(&self) -> u64 {
        self.lost.load(Ordering::Acquire)
    }

    /// Get the current round count.
    pub(crate) fn round_count(&self) -> u64 {
        self.round_count.load(Ordering::Acquire)
    }

    /// Reset the sampler state (e.g., after a timeout).
    pub(crate) fn reset(&self) {
        self.delivered.store(0, Ordering::Release);
        self.sent.store(0, Ordering::Release);
        self.lost.store(0, Ordering::Release);
        self.first_sent_time_nanos.store(0, Ordering::Release);
        self.first_sent_delivered.store(0, Ordering::Release);
        self.app_limited.store(false, Ordering::Release);
        self.round_count.store(0, Ordering::Release);
        self.round_start_delivered.store(0, Ordering::Release);
    }
}

impl Default for DeliveryRateSampler {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for DeliveryRateSampler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeliveryRateSampler")
            .field("delivered", &self.delivered.load(Ordering::Relaxed))
            .field("sent", &self.sent.load(Ordering::Relaxed))
            .field("lost", &self.lost.load(Ordering::Relaxed))
            .field("app_limited", &self.app_limited.load(Ordering::Relaxed))
            .field("round_count", &self.round_count.load(Ordering::Relaxed))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delivery_rate_basic() {
        let sampler = DeliveryRateSampler::new();

        // Send a packet
        let token = sampler.on_send(1000, 0, 0);
        assert_eq!(token.delivered, 0);
        assert_eq!(token.sent_time_nanos, 0);

        // ACK after 100ms = 100_000_000 ns
        let sample = sampler.on_ack(token, 1000, 100_000_000).unwrap();

        // Delivery rate = 1000 bytes / 100ms = 10000 bytes/sec
        assert_eq!(sample.delivery_rate, 10000);
        assert_eq!(sample.rtt, Duration::from_millis(100));
    }

    #[test]
    fn test_delivery_rate_multiple_packets() {
        let sampler = DeliveryRateSampler::new();

        // Send two packets
        let token1 = sampler.on_send(1000, 0, 0);
        let token2 = sampler.on_send(1000, 10_000_000, 1000); // 10ms later

        // ACK first packet after 100ms
        let sample1 = sampler.on_ack(token1, 1000, 100_000_000).unwrap();
        assert_eq!(sample1.rtt, Duration::from_millis(100));

        // ACK second packet after 110ms (also 100ms RTT)
        let sample2 = sampler.on_ack(token2, 1000, 110_000_000).unwrap();
        assert_eq!(sample2.rtt, Duration::from_millis(100));

        // Second sample should show higher delivery rate (more bytes delivered)
        assert!(sample2.delivered > sample1.delivered);
    }

    #[test]
    fn test_app_limited_flag() {
        let sampler = DeliveryRateSampler::new();

        // Not app limited by default
        assert!(!sampler.is_app_limited());

        // Mark as app limited
        sampler.set_app_limited(true);
        let token = sampler.on_send(1000, 0, 0);
        assert!(token.is_app_limited);

        // Clear app limited
        sampler.set_app_limited(false);
        let token2 = sampler.on_send(1000, 10_000_000, 1000);
        assert!(!token2.is_app_limited);
    }

    #[test]
    fn test_round_counting() {
        let sampler = DeliveryRateSampler::new();

        assert_eq!(sampler.round_count(), 0);

        // Send and ACK a packet
        let token = sampler.on_send(1000, 0, 0);
        sampler.on_ack(token, 1000, 100_000_000);

        // Should have started round 1
        assert_eq!(sampler.round_count(), 1);

        // Send and ACK another packet
        let token2 = sampler.on_send(1000, 100_000_000, 0);
        sampler.on_ack(token2, 1000, 200_000_000);

        // Should have started round 2
        assert_eq!(sampler.round_count(), 2);
    }

    #[test]
    fn test_loss_tracking() {
        let sampler = DeliveryRateSampler::new();

        assert_eq!(sampler.lost(), 0);

        sampler.on_loss(1000);
        assert_eq!(sampler.lost(), 1000);

        sampler.on_loss(500);
        assert_eq!(sampler.lost(), 1500);
    }

    #[test]
    fn test_reset() {
        let sampler = DeliveryRateSampler::new();

        let token = sampler.on_send(1000, 0, 0);
        sampler.on_ack(token, 1000, 100_000_000);
        sampler.on_loss(500);
        sampler.set_app_limited(true);

        assert!(sampler.delivered() > 0);
        assert!(sampler.lost() > 0);
        assert!(sampler.is_app_limited());

        sampler.reset();

        assert_eq!(sampler.delivered(), 0);
        assert_eq!(sampler.lost(), 0);
        assert!(!sampler.is_app_limited());
        assert_eq!(sampler.round_count(), 0);
    }
}

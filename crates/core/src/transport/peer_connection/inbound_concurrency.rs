//! Per-connection estimate of how many inbound QUIC streams the connection
//! would hold open if it ran over QUIC. Telemetry only: it measures the
//! inbound concurrency `quic::config::MAX_UNI_STREAMS` has to allow, and
//! nothing reads it but `TRANSPORT_METRICS`.
//!
//! On QUIC every message is one unidirectional stream, and a stream takes
//! one of the receiver's stream slots from when the sender opens it until
//! the sender gets the credit back: about one RTT plus the receiver's ACK
//! delay for a short message, and the whole transfer for a streamed one. So
//! at each inbound message the estimate is
//!
//! ```text
//! open inbound streams + packets still queued for the connection
//!     + short messages that arrived within (srtt + 25 ms)
//! ```
//!
//! It is built to err high, not a strict upper bound. The terms that run
//! high:
//! - open streams are `InboundStreamBudget::stream_count`, whose charge
//!   lasts until the stream's consumers drop its buffer, while a QUIC slot
//!   frees when the stream has been read to its end;
//! - the queued packets (`inbound_packet_recv`, not yet read) would hold
//!   their slots unread on QUIC, but the count also includes ACK-only
//!   packets and fragments of streams already counted;
//! - packets a gateway handshake buffered share one stamp, so a new gateway
//!   connection's first messages count as arriving together.
//!
//! Known undercounts:
//! - a sample skipped at a snapshot boundary (see
//!   `TransportMetrics::record_inbound_concurrency`);
//! - short messages older than [`HORIZON`] are forgotten, so a window
//!   longer than that (an RTT near 2 s) counts only the last [`HORIZON`];
//! - at most [`MAX_TRACKED_SHORTS`] short messages are remembered;
//! - streams the per-connection budget (1024) refuses are not counted;
//! - open streams and the queue length are read when the connection
//!   processes the message, not when it arrived, so a stream that ended
//!   while the message waited in the queue is missed;
//! - packets the listener dropped because the queue was full are not
//!   counted until they are retransmitted, and then at the later time;
//! - a stream that arrives whole in one fragment is counted as a short
//!   message on the legacy path (never charged) but as an open stream on
//!   the operations path (charged until its consumers let go).
//!
//! So it can support LOWERING `MAX_UNI_STREAMS`, never raising it. A limit
//! that is too low costs only back-pressure on the sender (it waits for a
//! stream credit); QUIC shadow mode (e5a) is expected to confirm the value
//! on real QUIC before any default changes. It says nothing about what a
//! peer opening streams on purpose could hold.
//!
//! Arrival times are when the listener queued each packet
//! (`InboundPacket::queued_nanos`), which never decrease on one connection,
//! so the remembered times stay sorted and the window is counted with a
//! binary search. The memory is at most [`MAX_TRACKED_SHORTS`] u64s (8 KiB)
//! per connection; it is not shrunk after a burst.

use std::collections::VecDeque;
use std::time::Duration;

/// Added to the smoothed RTT to give how long a short message holds a QUIC
/// stream slot: the sender gets the credit back once the receiver has read
/// the stream and acknowledged it, and the receiver may delay that
/// acknowledgement by up to its `max_ack_delay`. 25 ms is the QUIC default
/// (RFC 9000 section 18.2) and quinn's: `quic::config` does not set
/// `max_ack_delay`, so this mirrors quinn's default rather than a value of
/// ours. If that config ever sets it, use the same value here.
pub(super) const ACK_DELAY_ALLOWANCE: Duration = Duration::from_millis(25);

/// The RTT used before the connection has a smoothed RTT (no data ACK or
/// keep-alive sample yet).
pub(super) const FALLBACK_RTT: Duration = Duration::from_millis(100);

/// How long a short message's arrival is remembered, whatever the current
/// window. Fixed, so that when the smoothed RTT grows the window can take in
/// arrivals a shorter window had already passed.
pub(super) const HORIZON: Duration = Duration::from_secs(2);

/// Most short-message arrival times kept. At the cap the oldest is dropped,
/// so the short-message term stops growing at this many, which is above the
/// largest reported threshold (256).
pub(super) const MAX_TRACKED_SHORTS: usize = 1024;

/// The arrival times of recent short messages. Owned by the connection's
/// recv loop.
#[derive(Debug, Default)]
pub(super) struct InboundConcurrency {
    /// Arrival times (nanoseconds from the connection's time source),
    /// oldest first, never older than [`HORIZON`] at the latest push.
    shorts: VecDeque<u64>,
}

impl InboundConcurrency {
    /// Record a short message arriving at `now_nanos` and return the
    /// estimate, counting it. `outstanding` is the connection's open
    /// inbound streams plus the packets still queued for it.
    pub(super) fn on_short_message(
        &mut self,
        now_nanos: u64,
        srtt: Option<Duration>,
        outstanding: usize,
    ) -> u32 {
        self.prune(now_nanos);
        if self.shorts.len() >= MAX_TRACKED_SHORTS {
            self.shorts.pop_front();
        }
        self.shorts.push_back(now_nanos);
        self.estimate(now_nanos, srtt, outstanding)
    }

    /// Return the estimate at `now_nanos`, when a new inbound stream has
    /// been admitted. `outstanding` (open inbound streams plus queued
    /// packets) must already count the stream.
    pub(super) fn on_stream_admitted(
        &mut self,
        now_nanos: u64,
        srtt: Option<Duration>,
        outstanding: usize,
    ) -> u32 {
        self.prune(now_nanos);
        self.estimate(now_nanos, srtt, outstanding)
    }

    /// How long a short message counts after it arrives.
    pub(super) fn window(srtt: Option<Duration>) -> Duration {
        srtt.unwrap_or(FALLBACK_RTT)
            .saturating_add(ACK_DELAY_ALLOWANCE)
    }

    /// Forget the short messages that arrived more than [`HORIZON`] before
    /// `now_nanos`.
    fn prune(&mut self, now_nanos: u64) {
        let cutoff = now_nanos.saturating_sub(nanos(HORIZON));
        while self.shorts.front().is_some_and(|&at| at < cutoff) {
            self.shorts.pop_front();
        }
    }

    /// The remembered short messages that arrived within one window of
    /// `now_nanos`: a suffix, since the times are sorted.
    fn shorts_in_window(&self, now_nanos: u64, srtt: Option<Duration>) -> usize {
        let cutoff = now_nanos.saturating_sub(nanos(Self::window(srtt)));
        self.shorts.len() - self.shorts.partition_point(|&at| at < cutoff)
    }

    fn estimate(&self, now_nanos: u64, srtt: Option<Duration>, outstanding: usize) -> u32 {
        let total = outstanding.saturating_add(self.shorts_in_window(now_nanos, srtt));
        u32::try_from(total).unwrap_or(u32::MAX)
    }
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: u64 = 1_000_000;

    #[test]
    fn short_messages_outside_the_window_are_pruned() {
        let srtt = Some(Duration::from_millis(50));
        // Window: 50 ms + 25 ms.
        let mut c = InboundConcurrency::default();
        assert_eq!(c.on_short_message(1_000 * MS, srtt, 0), 1);
        assert_eq!(c.on_short_message(1_040 * MS, srtt, 0), 2);
        // 75 ms after the first: still inside the window.
        assert_eq!(c.on_short_message(1_075 * MS, srtt, 0), 3);
        // 76 ms after the first: it has left; the other two remain.
        assert_eq!(c.on_short_message(1_076 * MS, srtt, 0), 3);
        // Long after all of them: only the new one counts.
        assert_eq!(c.on_short_message(2_000 * MS, srtt, 0), 1);
    }

    /// Arrivals are forgotten only after HORIZON, whatever the window.
    #[test]
    fn arrivals_are_remembered_for_the_horizon() {
        let srtt = Some(Duration::from_millis(10));
        let mut c = InboundConcurrency::default();
        c.on_short_message(1_000 * MS, srtt, 0);
        c.on_short_message(1_500 * MS, srtt, 0);
        assert_eq!(c.shorts.len(), 2, "outside the window, inside the horizon");
        c.on_short_message(1_000 * MS + nanos(HORIZON), srtt, 0);
        assert_eq!(c.shorts.len(), 3, "exactly HORIZON old is still kept");
        c.on_short_message(1_000 * MS + nanos(HORIZON) + 1, srtt, 0);
        assert_eq!(c.shorts.len(), 3, "older than HORIZON is forgotten");
        assert_eq!(c.shorts.front(), Some(&(1_500 * MS)));
    }

    /// When the smoothed RTT grows, arrivals a shorter window had passed
    /// count again: at 10 ms (window 35 ms) the arrival at 0 has left by
    /// 50 ms, and at 100 ms (window 125 ms) all three count.
    #[test]
    fn a_growing_srtt_counts_earlier_arrivals_again() {
        let short = Some(Duration::from_millis(10));
        let long = Some(Duration::from_millis(100));
        let mut c = InboundConcurrency::default();
        let t0 = 1_000 * MS;
        assert_eq!(c.on_short_message(t0, short, 0), 1);
        assert_eq!(c.on_short_message(t0 + 40 * MS, short, 0), 1);
        assert_eq!(c.on_short_message(t0 + 50 * MS, short, 0), 2);
        assert_eq!(c.on_stream_admitted(t0 + 50 * MS, long, 0), 3);
    }

    #[test]
    fn open_streams_count_with_the_short_messages() {
        let srtt = Some(Duration::from_millis(20));
        let mut c = InboundConcurrency::default();
        assert_eq!(c.on_stream_admitted(10 * MS, srtt, 3), 3);
        assert_eq!(c.on_short_message(11 * MS, srtt, 3), 4);
        assert_eq!(c.on_stream_admitted(12 * MS, srtt, 5), 6);
        // The short message has left the window; the streams have not.
        assert_eq!(c.on_stream_admitted(100 * MS, srtt, 5), 5);
    }

    #[test]
    fn tracked_short_messages_are_capped() {
        let srtt = Some(Duration::from_secs(10));
        let mut c = InboundConcurrency::default();
        for i in 0..MAX_TRACKED_SHORTS as u64 + 10 {
            c.on_short_message(1_000 * MS + i, srtt, 0);
        }
        assert_eq!(c.shorts.len(), MAX_TRACKED_SHORTS);
        assert_eq!(
            c.on_short_message(2_000 * MS, srtt, 2),
            MAX_TRACKED_SHORTS as u32 + 2,
            "at the cap the estimate stays at the cap plus the open streams"
        );
        assert_eq!(
            c.shorts.front(),
            Some(&(1_000 * MS + 11)),
            "the oldest arrivals are the ones dropped"
        );
    }

    #[test]
    fn without_a_smoothed_rtt_the_fallback_is_used() {
        assert_eq!(
            InboundConcurrency::window(None),
            FALLBACK_RTT + ACK_DELAY_ALLOWANCE
        );
        let mut c = InboundConcurrency::default();
        c.on_short_message(1_000 * MS, None, 0);
        // 125 ms later: still inside the fallback window.
        assert_eq!(c.on_short_message(1_125 * MS, None, 0), 2);
        // 126 ms after the first: it has left.
        assert_eq!(c.on_short_message(1_126 * MS, None, 0), 2);
        assert_eq!(c.on_short_message(1_252 * MS, None, 0), 1);
    }
}

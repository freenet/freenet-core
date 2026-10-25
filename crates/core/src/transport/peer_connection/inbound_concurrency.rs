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
//! open inbound streams + short messages received within (srtt + 25 ms)
//! ```
//!
//! The open streams are `InboundStreamBudget::stream_count`. That
//! overestimates what QUIC would hold: a charge here lasts until the
//! stream's consumers drop its buffer, while a QUIC slot frees when the
//! stream has been read to its end. So the estimate is an upper bound on
//! what ordinary traffic needs, and can justify lowering the limit, never
//! raising it. It is also capped by the per-connection stream budget
//! (1024): streams the budget refuses are not counted. It says nothing
//! about what a peer opening streams on purpose could hold.
//!
//! A stream that arrives whole in one fragment differs by path: on the
//! legacy path it is never charged and is counted as a short message; on
//! the operations path it is charged until its consumers let go and is
//! counted as an open stream. Both overestimate.

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

/// Most short-message arrival times kept. At the cap the oldest is dropped,
/// so the estimate stops growing at this many short messages plus the open
/// streams, which is above the largest reported threshold (256).
pub(super) const MAX_TRACKED_SHORTS: usize = 1024;

/// The arrival times of the short messages that may still hold a stream
/// slot. Owned by the connection's recv loop.
#[derive(Debug, Default)]
pub(super) struct InboundConcurrency {
    /// Arrival times (nanoseconds from the connection's time source),
    /// oldest first.
    shorts: VecDeque<u64>,
}

impl InboundConcurrency {
    /// Record a short message arriving at `now_nanos` and return the
    /// estimate, counting it. `open_streams` is the connection's count of
    /// inbound streams holding state.
    pub(super) fn on_short_message(
        &mut self,
        now_nanos: u64,
        srtt: Option<Duration>,
        open_streams: usize,
    ) -> u32 {
        self.prune(now_nanos, srtt);
        if self.shorts.len() >= MAX_TRACKED_SHORTS {
            self.shorts.pop_front();
        }
        self.shorts.push_back(now_nanos);
        self.estimate(open_streams)
    }

    /// Return the estimate at `now_nanos`, when a new inbound stream has
    /// been admitted. `open_streams` must already count it.
    pub(super) fn on_stream_admitted(
        &mut self,
        now_nanos: u64,
        srtt: Option<Duration>,
        open_streams: usize,
    ) -> u32 {
        self.prune(now_nanos, srtt);
        self.estimate(open_streams)
    }

    /// How long a short message counts after it arrives.
    pub(super) fn window(srtt: Option<Duration>) -> Duration {
        srtt.unwrap_or(FALLBACK_RTT)
            .saturating_add(ACK_DELAY_ALLOWANCE)
    }

    /// Drop the short messages that arrived more than one window before
    /// `now_nanos`.
    fn prune(&mut self, now_nanos: u64, srtt: Option<Duration>) {
        let window = u64::try_from(Self::window(srtt).as_nanos()).unwrap_or(u64::MAX);
        let cutoff = now_nanos.saturating_sub(window);
        while self.shorts.front().is_some_and(|&at| at < cutoff) {
            self.shorts.pop_front();
        }
    }

    fn estimate(&self, open_streams: usize) -> u32 {
        u32::try_from(open_streams.saturating_add(self.shorts.len())).unwrap_or(u32::MAX)
    }
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
        assert_eq!(c.shorts.len(), 1, "pruned entries are not kept");
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

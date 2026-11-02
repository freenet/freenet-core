//! What the node does with the outcome of a data send on a path (#5940).
//!
//! A message has one route, chosen when it is sent. [`next_step`] is the table
//! that says whether it goes again on the UDP session. The rule behind every
//! row: a message goes on UDP only when the outcome proves the receiver cannot
//! deliver it. So a message is never delivered twice, and a Short needs no dedup
//! at the receiver.
//!
//! | Outcome | Short | Stream or pipe |
//! |---|---|---|
//! | `Sent` | done | done |
//! | `MaybeDelivered` | done | done |
//! | `Failed(NotOpened)` | UDP | UDP, same id |
//! | `Failed(TimedOut)` after open | UDP | give up |
//! | `ConnectionLost` before the end | UDP | give up |
//! | Stopped `NOT_ACCEPTING_DATA` | UDP | UDP, same id |
//! | Stopped `BUDGET_REFUSED` | UDP | give up |
//! | Any other stop | give up | give up |
//! | A frame that cannot be built | UDP | UDP, same id |
//! | `Source`, `LengthMismatch` | not reached | give up |
//!
//! A Short needs its end to be delivered, so one whose end was not sent cannot
//! arrive. A Stream that opened may be known to the receiver, which remembers its
//! id and may hold its handle (now cancelled) and its metadata, so it never goes
//! again under that id. The operation layer retries with a fresh id, as it does
//! after a UDP stream fails. A frame that cannot be built opened no stream, and
//! UDP then refuses or carries the message by its own rules.
//!
//! Every outcome goes through [`QuicPath::settle`], which also counts it and
//! keeps the path's stall count.

use std::sync::atomic::Ordering;

use super::bounded::{BoundedSend, MaybeDelivered, NotOpened, SendFailure};
use super::{QuicPath, QuicSendError, stop_code};
use crate::transport::metrics::QuicCloseCode;

/// Send timeouts in a row that close a path with `DOWNGRADE`. A send that opens
/// nothing costs no reset, so `recycle::SEND_ABORT_LIMIT` does not see it, and
/// that limit is too slow to catch a path that has stopped moving. This costs a
/// normal backoff and is never permanent.
pub(crate) const STALLED_SENDS_LIMIT: u32 = 8;

/// Which kind of data send an outcome belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SendKind {
    /// A Short of one packet, from the path's short queue.
    Short,
    /// A Short past one packet, which goes on UDP as a stream.
    LargeShort,
    /// A Stream from bytes.
    Stream,
    /// A Stream from an inbound stream being forwarded.
    Pipe,
}

impl SendKind {
    pub(crate) const ALL: [Self; 4] = [Self::Short, Self::LargeShort, Self::Stream, Self::Pipe];

    fn is_short(self) -> bool {
        match self {
            Self::Short | Self::LargeShort => true,
            Self::Stream | Self::Pipe => false,
        }
    }
}

/// What the sender does next with a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NextStep {
    /// The send happened: `Sent` or `MaybeDelivered`.
    Done,
    /// The receiver cannot deliver this: send it on the UDP session.
    SendOnUdp,
    /// The message is gone. The operation layer retries if it wants to.
    GiveUp,
}

quic_outcomes! {
    /// How a data send ended, for the path's counters.
    enum QuicSendResult {
        Sent => "sent",
        MaybeDelivered => "maybe_delivered",
        /// No stream was opened, and the message went on UDP.
        NotOpenedUdp => "not_opened_udp",
        /// A stream was opened, and the message went on UDP.
        FailedUdp => "failed_udp",
        Dropped => "dropped",
    }
}

quic_outcomes! {
    /// What else happened to the path's data sends.
    enum QuicSendEvent {
        /// The short queue was full, so the message went on UDP at once.
        ShortOverflowUdp => "short_overflow_udp",
        /// The peer refused data with `NOT_ACCEPTING_DATA`.
        DataRefused => "data_refused",
        /// The path was closed after `STALLED_SENDS_LIMIT` send timeouts in a row.
        StalledClosed => "stalled_closed",
        /// A message on its way to UDP could not be sent there, and is lost.
        UdpSendFailed => "udp_send_failed",
    }
}

/// What to do after `outcome` of a send of `kind`, and how to count it.
pub(crate) fn next_step(kind: SendKind, outcome: &BoundedSend) -> (NextStep, QuicSendResult) {
    use NextStep::{Done, GiveUp, SendOnUdp};
    use QuicSendResult as R;
    let short_goes_on_udp = if kind.is_short() { SendOnUdp } else { GiveUp };
    let (step, opened) = match outcome {
        BoundedSend::Sent => return (Done, R::Sent),
        BoundedSend::MaybeDelivered(_) => return (Done, R::MaybeDelivered),
        BoundedSend::Failed(SendFailure::NotOpened(_)) => (SendOnUdp, false),
        BoundedSend::Failed(SendFailure::TimedOut) => (short_goes_on_udp, true),
        BoundedSend::Failed(SendFailure::Error(error)) => match error {
            QuicSendError::InvalidFrame(_) => (SendOnUdp, false),
            QuicSendError::ConnectionLost(_) | QuicSendError::Closed => (short_goes_on_udp, true),
            QuicSendError::Source(_) | QuicSendError::LengthMismatch { .. } => (GiveUp, true),
            QuicSendError::Stopped(code) => match u32::try_from(*code) {
                Ok(stop_code::NOT_ACCEPTING_DATA) => (SendOnUdp, true),
                Ok(stop_code::BUDGET_REFUSED) => (short_goes_on_udp, true),
                Ok(_) | Err(_) => (GiveUp, true),
            },
        },
    };
    let result = match (step, opened) {
        (SendOnUdp, false) => R::NotOpenedUdp,
        (SendOnUdp, true) => R::FailedUdp,
        (Done | GiveUp, _) => R::Dropped,
    };
    (step, result)
}

/// Whether `outcome` is a deadline running out on this path.
fn timed_out(outcome: &BoundedSend) -> bool {
    match outcome {
        BoundedSend::MaybeDelivered(MaybeDelivered::TimedOut)
        | BoundedSend::Failed(SendFailure::NotOpened(NotOpened::TimedOut))
        | BoundedSend::Failed(SendFailure::TimedOut) => true,
        BoundedSend::Sent
        | BoundedSend::MaybeDelivered(MaybeDelivered::ConnectionLost)
        | BoundedSend::Failed(SendFailure::NotOpened(NotOpened::ConnectionLost(_)))
        | BoundedSend::Failed(SendFailure::Error(_)) => false,
    }
}

impl QuicPath {
    /// Count `outcome` of a send of `kind` and say what to do with the message.
    /// Every data send of the node ends here.
    ///
    /// `source_stalled`: the send was waiting for its source when it timed out.
    /// That says nothing about the path, so the stall count does not move.
    pub(crate) fn settle(
        &self,
        kind: SendKind,
        outcome: &BoundedSend,
        source_stalled: bool,
    ) -> NextStep {
        let (step, result) = next_step(kind, outcome);
        self.stats.record_send(kind, result);
        if let BoundedSend::Failed(SendFailure::Error(QuicSendError::Stopped(code))) = outcome {
            self.stats.record_stop(*code);
            if *code == u64::from(stop_code::NOT_ACCEPTING_DATA) {
                self.stop_sending_data();
            }
        }
        if matches!(outcome, BoundedSend::Sent) {
            self.stalled_sends.store(0, Ordering::Relaxed);
        } else if timed_out(outcome) && !source_stalled {
            let in_a_row = self.stalled_sends.fetch_add(1, Ordering::Relaxed) + 1;
            if in_a_row == STALLED_SENDS_LIMIT {
                self.stats.record_send_event(QuicSendEvent::StalledClosed);
                tracing::warn!(
                    in_a_row,
                    "QUIC path: sends keep timing out; closing the path, and the \
                     connection stays on UDP"
                );
                self.close(QuicCloseCode::Downgrade);
            }
        }
        step
    }
}

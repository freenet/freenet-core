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

use std::time::Duration;

use super::super::recycle::{self, ACK_WATCH_WINDOW};
use super::bounded::{BoundedSend, MaybeDelivered, NotOpened, SendFailure};
use super::{QuicPath, QuicSendError, stop_code};
use crate::transport::metrics::QuicCloseCode;

/// Send timeouts in a row, with no acknowledgement on the path, that close it
/// with `DOWNGRADE` ([`StallCount`]). A send that never opens or that waits for
/// its acknowledgement past its deadline costs no reset, so
/// `recycle::SEND_ABORT_LIMIT` does not see it, and that limit is too slow to catch
/// a path that has stopped moving. This costs a normal backoff and is never
/// permanent.
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

/// Whether `outcome` is a send deadline running out: before the stream opened,
/// with the stream open and not moving, or waiting for the acknowledgement of
/// its end. Whether it counts against the path is [`StallCount`]'s decision.
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

/// How long the path must have gone with no packet newly acknowledged before a
/// timeout counts against it: the acknowledgement watch's window.
pub(crate) const STALL_PROGRESS_WINDOW: Duration = ACK_WATCH_WINDOW;

/// The path's count of send timeouts in a row, kept only while the path makes no
/// forward progress.
///
/// Decision (e5b-5 review, kept in the e5b-6 review): every kind of timeout can
/// count, `NotOpened(TimedOut)` and `MaybeDelivered(TimedOut)` included. A path
/// whose permits are all held by sends stuck waiting for acknowledgements past
/// `MaybeDelivered` costs every streamed send its 2 s open deadline, and only this
/// close recycles it. But a timeout counts only when no packet on the connection
/// was newly acknowledged within [`STALL_PROGRESS_WINDOW`] (quinn's acknowledged
/// packets, read from `recycle::AckCounting`). So a busy relay whose 48 permits
/// are held by transfers that are being acknowledged, or a receiver whose consumer
/// is slow while its QUIC stack keeps acknowledging, never closes, and a path with
/// no acknowledgements does. Any acknowledgement, like any `Sent`, starts the
/// count again. Do not drop a kind of timeout from the count, and do not count
/// one while acknowledgements arrive.
///
/// The acknowledgement count is read when a send settles, so progress is seen at
/// the next settle after it, within one send's deadline. A receiver whose
/// consumer stays stalled past the window while nothing else on the path is
/// acknowledged reads as a stalled path.
#[derive(Debug)]
pub(crate) struct StallCount {
    in_a_row: u32,
    /// The acknowledged-packet count at the last settle.
    acked: Option<u64>,
    /// When the count last moved, on the path's clock.
    progressed_at: u64,
}

impl StallCount {
    /// A count for a path built at `now_nanos`, when the connection had
    /// acknowledged `acked` packets: the path counts as having moved then.
    pub(crate) fn new(acked: Option<u64>, now_nanos: u64) -> Self {
        Self {
            in_a_row: 0,
            acked,
            progressed_at: now_nanos,
        }
    }

    /// Take `outcome`, with the connection's acknowledged-packet count `acked`
    /// (`None` when the controller does not count them) at `now_nanos`. Returns
    /// whether the path should close now: once, at the limit.
    pub(crate) fn take(
        &mut self,
        outcome: &BoundedSend,
        source_stalled: bool,
        acked: Option<u64>,
        now_nanos: u64,
    ) -> bool {
        if let Some(acked) = acked {
            if self.acked != Some(acked) {
                self.acked = Some(acked);
                self.progressed_at = now_nanos;
                self.in_a_row = 0;
            }
        }
        if matches!(outcome, BoundedSend::Sent) {
            self.in_a_row = 0;
            return false;
        }
        let window = u64::try_from(STALL_PROGRESS_WINDOW.as_nanos()).unwrap_or(u64::MAX);
        let quiet = acked.is_none() || now_nanos.saturating_sub(self.progressed_at) >= window;
        if !timed_out(outcome) || source_stalled || !quiet {
            return false;
        }
        self.in_a_row += 1;
        self.in_a_row == STALLED_SENDS_LIMIT
    }
}

impl QuicPath {
    /// Count `outcome` of a send of `kind` and say what to do with the message.
    /// Every data send of the node ends here.
    ///
    /// `source_stalled`: the send was waiting for its source when it timed out.
    /// That says nothing about the path, so the stall count does not move.
    /// `now_nanos` is on the path's clock ([`StallCount`]).
    pub(crate) fn settle(
        &self,
        kind: SendKind,
        outcome: &BoundedSend,
        source_stalled: bool,
        now_nanos: u64,
    ) -> NextStep {
        let (step, result) = next_step(kind, outcome);
        self.stats.record_send(kind, result);
        if let BoundedSend::Failed(SendFailure::Error(QuicSendError::Stopped(code))) = outcome {
            self.stats.record_stop(*code);
            if *code == u64::from(stop_code::NOT_ACCEPTING_DATA) {
                self.stop_sending_data();
            }
        }
        let acked = self.ack_progress.as_ref().map(recycle::AckProgress::acked);
        let close = self
            .stall
            .lock()
            .take(outcome, source_stalled, acked, now_nanos);
        if close {
            self.stats.record_send_event(QuicSendEvent::StalledClosed);
            tracing::warn!(
                in_a_row = STALLED_SENDS_LIMIT,
                "QUIC path: sends keep timing out and nothing is acknowledged; closing \
                 the path, and the connection stays on UDP"
            );
            self.close(QuicCloseCode::Downgrade);
        }
        step
    }
}

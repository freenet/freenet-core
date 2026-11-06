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
//! | `Failed(NotOpened(Paced))` (the pacer refused it) | UDP, charged to the pacer | UDP, same id, charged to the pacer |
//! | `Failed(TimedOut)` after open | UDP | give up |
//! | `Failed(Paced)` after open | UDP, charged to the pacer | give up |
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

use super::super::recycle::{self, ACK_WATCH_WINDOW, cap_held};
use super::super::socket::CapHoldReading;
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

    /// Its name in telemetry (`quic_data_send_<kind>_<result>`).
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Short => "short",
            Self::LargeShort => "large_short",
            Self::Stream => "stream",
            Self::Pipe => "pipe",
        }
    }

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
        /// A send timed out with nothing acknowledged for the window, so new
        /// sends went on UDP ([`QuicPath::is_suspect`]).
        SuspectEntered => "suspect_entered",
        /// The connection acknowledged something again, so the path took sends
        /// again.
        SuspectLeft => "suspect_left",
        /// The connection's bandwidth-limit pacer refused a send, before or
        /// after its stream opened (`pacer` module). Also counted under its
        /// kind's result.
        PacerRefused => "pacer_refused",
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
        BoundedSend::Failed(SendFailure::TimedOut | SendFailure::Paced) => {
            (short_goes_on_udp, true)
        }
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
/// A send the pacer refused is not one: this side held it back, and the path
/// said nothing (`pacer` module), so it never counts toward the stall close.
fn timed_out(outcome: &BoundedSend) -> bool {
    match outcome {
        BoundedSend::MaybeDelivered(MaybeDelivered::TimedOut)
        | BoundedSend::Failed(SendFailure::NotOpened(NotOpened::TimedOut))
        | BoundedSend::Failed(SendFailure::TimedOut) => true,
        BoundedSend::Sent
        | BoundedSend::MaybeDelivered(MaybeDelivered::ConnectionLost)
        | BoundedSend::Failed(SendFailure::NotOpened(NotOpened::ConnectionLost(_)))
        | BoundedSend::Failed(SendFailure::NotOpened(NotOpened::Paced))
        | BoundedSend::Failed(SendFailure::Paced)
        | BoundedSend::Failed(SendFailure::Error(_)) => false,
    }
}

/// How long the path must have gone with no packet newly acknowledged before a
/// timeout counts against it: the acknowledgement watch's window.
pub(crate) const STALL_PROGRESS_WINDOW: Duration = ACK_WATCH_WINDOW;

/// What [`StallCount::take`] decided.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct StallVerdict {
    /// The eighth such timeout in a row: close the path.
    pub(crate) close: bool,
    /// A timeout that counted: the path made no progress for the window.
    pub(crate) quiet_timeout: bool,
}

/// The path's count of send timeouts in a row, kept only while the path makes no
/// forward progress.
///
/// Decision (e5b-5 review, kept in the e5b-6 review): every kind of timeout can
/// count, `NotOpened(TimedOut)` and `MaybeDelivered(TimedOut)` included. A path
/// whose permits are all held by sends stuck waiting for acknowledgements past
/// `MaybeDelivered` costs every streamed send its 2 s open deadline, and only this
/// close recycles it. But a timeout counts only when no packet on the connection
/// was newly acknowledged within [`STALL_PROGRESS_WINDOW`] (quinn's acknowledged
/// packets, read from `recycle::CappedCubic`). So a busy relay whose 48 permits
/// are held by transfers that are being acknowledged, or a receiver whose consumer
/// is slow while its QUIC stack keeps acknowledging, never closes, and a path with
/// no acknowledgements does. Any acknowledgement, like any `Sent`, starts the
/// count again. Do not drop a kind of timeout from the count, and do not count
/// one while acknowledgements arrive.
///
/// The first counted timeout also makes the path suspect
/// ([`QuicPath::become_suspect`]), which keeps new sends off it. So on a path
/// that went dark the close fires only if `STALLED_SENDS_LIMIT` sends were
/// already on it; otherwise the path ends on quinn's idle timeout, with its new
/// data on UDP meanwhile.
///
/// The acknowledgement count is read when a send settles, so progress is seen at
/// the next settle after it, within one send's deadline. A receiver whose
/// consumer stays stalled past the window while nothing else on the path is
/// acknowledged reads as a stalled path.
///
/// An egress cap hold counts as progress, as it does for the acknowledgement
/// watch (`recycle::cap_held`): while `total-bandwidth-limit` holds the node's
/// QUIC transmits nothing can be acknowledged, one held transmit can wait for
/// longer than the window at a low cap, and the path itself is not stalled.
///
/// A send the path's pacer refused ([`NotOpened::Paced`], [`SendFailure::Paced`])
/// is not a timeout and never counts: this side held it back. A pacer's waits
/// are not progress: they only hold writes into quinn's buffer, and on a live
/// path acknowledgements keep arriving meanwhile, so a paced path that goes
/// dark still goes suspect and closes.
#[derive(Debug)]
pub(crate) struct StallCount {
    in_a_row: u32,
    /// The acknowledged-packet count at the last settle.
    acked: Option<u64>,
    /// When the count last moved, or the cap last held a transmit, on the
    /// path's clock.
    progressed_at: u64,
    /// The cap's holds at the last settle, and since when a hold has been
    /// reported with no new hold event (`recycle::cap_held`).
    cap: Option<CapHoldReading>,
    hold_unchanged_since: Option<u64>,
}

impl StallCount {
    /// A count for a path built at `now_nanos`, when the connection had
    /// acknowledged `acked` packets: the path counts as having moved then.
    pub(crate) fn new(acked: Option<u64>, now_nanos: u64) -> Self {
        Self {
            in_a_row: 0,
            acked,
            progressed_at: now_nanos,
            cap: None,
            hold_unchanged_since: None,
        }
    }

    /// Take `outcome`, with the connection's acknowledged-packet count `acked`
    /// (`None` when the controller does not count them) and the egress cap's
    /// holds `cap`, at `now_nanos`: whether it counted, and whether the path
    /// should close now (once, at the limit).
    pub(crate) fn take(
        &mut self,
        outcome: &BoundedSend,
        source_stalled: bool,
        acked: Option<u64>,
        cap: CapHoldReading,
        now_nanos: u64,
    ) -> StallVerdict {
        let previous = self.cap.replace(cap).unwrap_or(cap);
        let held = cap_held(&mut self.hold_unchanged_since, now_nanos, &cap, &previous);
        let acknowledged = acked.is_some_and(|acked| self.acked != Some(acked));
        if acknowledged {
            self.acked = acked;
        }
        if acknowledged || held {
            self.progressed_at = now_nanos;
            self.in_a_row = 0;
        }
        if matches!(outcome, BoundedSend::Sent) {
            self.in_a_row = 0;
            return StallVerdict::default();
        }
        let window = u64::try_from(STALL_PROGRESS_WINDOW.as_nanos()).unwrap_or(u64::MAX);
        let quiet = acked.is_none() || now_nanos.saturating_sub(self.progressed_at) >= window;
        if !timed_out(outcome) || source_stalled || !quiet {
            return StallVerdict::default();
        }
        self.in_a_row += 1;
        StallVerdict {
            close: self.in_a_row == STALLED_SENDS_LIMIT,
            quiet_timeout: true,
        }
    }
    /// [`Self::take`] with no egress cap.
    #[cfg(test)]
    pub(crate) fn take_uncapped(
        &mut self,
        outcome: &BoundedSend,
        source_stalled: bool,
        acked: Option<u64>,
        now_nanos: u64,
    ) -> bool {
        let no_cap = CapHoldReading {
            holds: 0,
            held_now: false,
            validity: Duration::ZERO,
        };
        self.take(outcome, source_stalled, acked, no_cap, now_nanos)
            .close
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
        if matches!(
            outcome,
            BoundedSend::Failed(SendFailure::NotOpened(NotOpened::Paced) | SendFailure::Paced)
        ) {
            self.stats.record_send_event(QuicSendEvent::PacerRefused);
        }
        if let BoundedSend::Failed(SendFailure::Error(QuicSendError::Stopped(code))) = outcome {
            self.stats.record_stop(*code);
            if *code == u64::from(stop_code::NOT_ACCEPTING_DATA) {
                self.stop_sending_data();
            }
        }
        let acked = self.ack_progress.as_ref().map(recycle::AckProgress::acked);
        let cap = self.connection.cap_holds().read();
        let verdict = self
            .stall
            .lock()
            .take(outcome, source_stalled, acked, cap, now_nanos);
        if let (true, Some(acked)) = (verdict.quiet_timeout, acked) {
            self.become_suspect(acked);
        }
        if verdict.close {
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

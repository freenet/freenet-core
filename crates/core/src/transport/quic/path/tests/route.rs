//! The table of `path::route` and what `QuicPath::settle` does with an outcome.

use super::super::route::{NextStep, QuicSendEvent, QuicSendResult, STALLED_SENDS_LIMIT, next_step};
use super::super::{SendKind, framing_error_for_tests};
use super::*;

fn lost() -> quinn::ConnectionError {
    quinn::ConnectionError::TimedOut
}

fn stopped(code: u32) -> BoundedSend {
    BoundedSend::Failed(SendFailure::Error(QuicSendError::Stopped(code.into())))
}

fn error(error: QuicSendError) -> BoundedSend {
    BoundedSend::Failed(SendFailure::Error(error))
}

/// Every row of the table in the module doc, for a Short and a Stream. A large
/// short takes the Short column and a pipe the Stream column.
#[test]
fn each_outcome_takes_its_row() {
    use NextStep::{Done, GiveUp, SendOnUdp};
    use QuicSendResult as R;
    let other_stops = [
        stop_code::HEADER_REFUSED,
        stop_code::STREAM_ID_IN_USE,
        stop_code::CHANNEL_FULL,
        stop_code::OVERLONG,
        stop_code::EVICTED,
        stop_code::CANCELLED,
        stop_code::FRAGMENT_REFUSED,
        0x1f,
    ];
    let mut rows: Vec<(BoundedSend, (NextStep, R), (NextStep, R))> = vec![
        (BoundedSend::Sent, (Done, R::Sent), (Done, R::Sent)),
        (
            BoundedSend::MaybeDelivered(MaybeDelivered::TimedOut),
            (Done, R::MaybeDelivered),
            (Done, R::MaybeDelivered),
        ),
        (
            BoundedSend::MaybeDelivered(MaybeDelivered::ConnectionLost),
            (Done, R::MaybeDelivered),
            (Done, R::MaybeDelivered),
        ),
        (
            BoundedSend::Failed(SendFailure::NotOpened(NotOpened::TimedOut)),
            (SendOnUdp, R::NotOpenedUdp),
            (SendOnUdp, R::NotOpenedUdp),
        ),
        (
            BoundedSend::Failed(SendFailure::NotOpened(NotOpened::ConnectionLost(lost()))),
            (SendOnUdp, R::NotOpenedUdp),
            (SendOnUdp, R::NotOpenedUdp),
        ),
        (
            BoundedSend::Failed(SendFailure::TimedOut),
            (SendOnUdp, R::FailedUdp),
            (GiveUp, R::Dropped),
        ),
        (
            error(QuicSendError::ConnectionLost(lost())),
            (SendOnUdp, R::FailedUdp),
            (GiveUp, R::Dropped),
        ),
        (
            error(QuicSendError::Closed),
            (SendOnUdp, R::FailedUdp),
            (GiveUp, R::Dropped),
        ),
        (
            stopped(stop_code::NOT_ACCEPTING_DATA),
            (SendOnUdp, R::FailedUdp),
            (SendOnUdp, R::FailedUdp),
        ),
        (
            stopped(stop_code::BUDGET_REFUSED),
            (SendOnUdp, R::FailedUdp),
            (GiveUp, R::Dropped),
        ),
        (
            error(QuicSendError::InvalidFrame(framing_error_for_tests())),
            (SendOnUdp, R::NotOpenedUdp),
            (SendOnUdp, R::NotOpenedUdp),
        ),
        (
            error(QuicSendError::Source(StreamError::Cancelled)),
            (GiveUp, R::Dropped),
            (GiveUp, R::Dropped),
        ),
        (
            error(QuicSendError::LengthMismatch {
                declared: 2,
                delivered: 1,
            }),
            (GiveUp, R::Dropped),
            (GiveUp, R::Dropped),
        ),
    ];
    rows.extend(other_stops.map(|code| {
        (
            stopped(code),
            (GiveUp, R::Dropped),
            (GiveUp, R::Dropped),
        )
    }));
    for (outcome, short, stream) in &rows {
        assert_eq!(next_step(SendKind::Short, outcome), *short, "{outcome:?}");
        assert_eq!(next_step(SendKind::LargeShort, outcome), *short, "{outcome:?}");
        assert_eq!(next_step(SendKind::Stream, outcome), *stream, "{outcome:?}");
        assert_eq!(next_step(SendKind::Pipe, outcome), *stream, "{outcome:?}");
    }
}

/// `settle` closes the path at the eighth timeout in a row, whichever kind of
/// timeout it is; any `Sent` starts the count again; and an outcome that is no
/// timeout leaves the count alone. A send waiting for its source does not count.
#[test]
fn eight_timeouts_in_a_row_close_the_path_and_a_sent_starts_again() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-route-stall", 70, QuicPathLimits::DEFAULT).await;
        let timeouts = [
            BoundedSend::Failed(SendFailure::NotOpened(NotOpened::TimedOut)),
            BoundedSend::MaybeDelivered(MaybeDelivered::TimedOut),
            BoundedSend::Failed(SendFailure::TimedOut),
        ];
        let closed = |path: &QuicPath| path.quinn_connection().close_reason().is_some();
        let settle = |outcome: &BoundedSend| p.a.settle(SendKind::Stream, outcome, false);

        for outcome in timeouts.iter().cycle().take(STALLED_SENDS_LIMIT as usize - 1) {
            settle(outcome);
        }
        assert!(!closed(&p.a), "seven are not enough");

        settle(&BoundedSend::Sent);
        for outcome in timeouts.iter().cycle().take(STALLED_SENDS_LIMIT as usize - 1) {
            settle(outcome);
        }
        assert!(!closed(&p.a), "a Sent started the count again");

        // Neither of these is a timeout, and neither resets the count.
        settle(&stopped(stop_code::CHANNEL_FULL));
        settle(&BoundedSend::MaybeDelivered(MaybeDelivered::ConnectionLost));
        assert!(!closed(&p.a));
        p.a.settle(SendKind::Pipe, &timeouts[0], true);
        p.a.settle(SendKind::Pipe, &timeouts[2], true);
        assert!(!closed(&p.a), "a stalled source is not the path's");

        settle(&timeouts[1]);
        assert!(closed(&p.a), "the eighth of a kind that counts");
        assert_eq!(p.a.stats().send_event_count(QuicSendEvent::StalledClosed), 1);
        settle(&timeouts[1]);
        assert_eq!(
            p.a.stats().send_event_count(QuicSendEvent::StalledClosed),
            1,
            "counted once"
        );
    });
}

/// `NOT_ACCEPTING_DATA` turns sending off, and only that code does. The path may
/// still take a send already on its way: the debug assertion for a path that never
/// had the bits does not fire for one that lost them.
#[test]
fn a_refusal_stops_new_sends_on_the_path() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-route-refused", 71, QuicPathLimits::DEFAULT).await;
        assert!(p.a.sends_data());
        p.a.settle(SendKind::Stream, &stopped(stop_code::BUDGET_REFUSED), false);
        assert!(p.a.sends_data());
        for _ in 0..2 {
            p.a.settle(
                SendKind::Short,
                &stopped(stop_code::NOT_ACCEPTING_DATA),
                false,
            );
        }
        assert!(!p.a.sends_data());
        assert_eq!(p.a.stats().send_event_count(QuicSendEvent::DataRefused), 2);
        p.a.debug_assert_carries_data();
    });
}

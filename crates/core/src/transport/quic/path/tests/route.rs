//! The table of `path::route` and what `QuicPath::settle` does with an outcome.

use super::super::STALLED_SENDS_LIMIT;
use super::super::route::{NextStep, QuicSendEvent, QuicSendResult, StallCount, next_step};
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
    type Row = (BoundedSend, (NextStep, R), (NextStep, R));
    let mut rows: Vec<Row> = vec![
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
    rows.extend(
        other_stops.map(|code| (stopped(code), (GiveUp, R::Dropped), (GiveUp, R::Dropped))),
    );
    for (outcome, short, stream) in &rows {
        assert_eq!(next_step(SendKind::Short, outcome), *short, "{outcome:?}");
        assert_eq!(
            next_step(SendKind::LargeShort, outcome),
            *short,
            "{outcome:?}"
        );
        assert_eq!(next_step(SendKind::Stream, outcome), *stream, "{outcome:?}");
        assert_eq!(next_step(SendKind::Pipe, outcome), *stream, "{outcome:?}");
    }
}

const SECOND: u64 = 1_000_000_000;

/// Past the window with no acknowledgement, from a count made at 0.
const QUIET: u64 = 11 * SECOND;

fn timeouts() -> [BoundedSend; 3] {
    [
        BoundedSend::Failed(SendFailure::NotOpened(NotOpened::TimedOut)),
        BoundedSend::MaybeDelivered(MaybeDelivered::TimedOut),
        BoundedSend::Failed(SendFailure::TimedOut),
    ]
}

/// The eighth timeout in a row closes once nothing has been acknowledged for the
/// window, whichever kind of timeout each is; a `Sent` starts the count again;
/// and an outcome that is no timeout leaves the count alone. A send waiting for
/// its source does not count.
#[test]
fn timeouts_on_a_quiet_path_close_it_and_a_sent_starts_again() {
    let mut count = StallCount::new(Some(5), 0);
    let mut take = |outcome: &BoundedSend| count.take(outcome, false, Some(5), QUIET);
    for outcome in timeouts()
        .iter()
        .cycle()
        .take(STALLED_SENDS_LIMIT as usize - 1)
    {
        assert!(!take(outcome), "seven are not enough");
    }
    assert!(!take(&BoundedSend::Sent));
    for outcome in timeouts()
        .iter()
        .cycle()
        .take(STALLED_SENDS_LIMIT as usize - 1)
    {
        assert!(!take(outcome), "a Sent started the count again");
    }
    assert!(!take(&stopped(stop_code::CHANNEL_FULL)));
    assert!(!take(&BoundedSend::MaybeDelivered(
        MaybeDelivered::ConnectionLost
    )));
    assert!(
        !count.take(&timeouts()[2], true, Some(5), QUIET),
        "a stalled source"
    );
    let mut take = |outcome: &BoundedSend| count.take(outcome, false, Some(5), QUIET);
    assert!(take(&timeouts()[0]), "the eighth, a send that never opened");
    assert!(!take(&timeouts()[1]), "the close comes once");
}

/// While acknowledgements arrive, no timeout counts: a busy path is not a stalled
/// one. Each acknowledgement starts the count again, and the window runs from it.
#[test]
fn timeouts_count_only_once_nothing_is_acknowledged_for_the_window() {
    let mut count = StallCount::new(Some(0), 0);
    // Inside the window after the path was built.
    for _ in 0..2 * STALLED_SENDS_LIMIT {
        assert!(!count.take(&timeouts()[0], false, Some(0), 9 * SECOND));
    }
    // Packets acknowledged every second: never quiet.
    for second in 0..40u64 {
        assert!(!count.take(
            &timeouts()[0],
            false,
            Some(second + 1),
            (10 + second) * SECOND
        ));
    }
    // Seven quiet timeouts, then an acknowledgement, then seven more: no close.
    let last = 49 * SECOND;
    for _ in 1..STALLED_SENDS_LIMIT {
        assert!(!count.take(&timeouts()[1], false, Some(40), last + QUIET));
    }
    assert!(!count.take(&timeouts()[1], false, Some(41), last + QUIET));
    let quiet_again = last + 2 * QUIET;
    for _ in 1..STALLED_SENDS_LIMIT {
        assert!(!count.take(&timeouts()[1], false, Some(41), quiet_again));
    }
    assert!(count.take(&timeouts()[1], false, Some(41), quiet_again));
}

/// A connection whose controller counts no acknowledgements cannot tell a busy
/// path from a stalled one, so every timeout counts, as before the window.
#[test]
fn with_no_acknowledgement_count_every_timeout_counts() {
    let mut count = StallCount::new(None, 0);
    for _ in 1..STALLED_SENDS_LIMIT {
        assert!(!count.take(&timeouts()[0], false, None, 0));
    }
    assert!(count.take(&timeouts()[0], false, None, 0));
}

/// On a real path, with nothing acknowledged since it was built: eight timeouts
/// past the window close it, counted once, and the path reads the connection's
/// own acknowledgement count.
#[test]
fn a_quiet_path_closes_after_eight_timeouts() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-route-stall", 70, QuicPathLimits::DEFAULT).await;
        assert!(
            p.a.ack_progress.is_some(),
            "the connection counts acknowledgements"
        );
        let later = p.time().now_nanos() + QUIET;
        for _ in 1..STALLED_SENDS_LIMIT {
            p.a.settle(SendKind::Stream, &timeouts()[2], false, later);
        }
        assert!(p.a.quinn_connection().close_reason().is_none());
        p.a.settle(SendKind::Stream, &timeouts()[2], false, later);
        assert!(p.a.quinn_connection().close_reason().is_some());
        p.a.settle(SendKind::Stream, &timeouts()[2], false, later);
        assert_eq!(
            p.a.stats().send_event_count(QuicSendEvent::StalledClosed),
            1
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
        p.a.settle(
            SendKind::Stream,
            &stopped(stop_code::BUDGET_REFUSED),
            false,
            0,
        );
        assert!(p.a.sends_data());
        for _ in 0..2 {
            p.a.settle(
                SendKind::Short,
                &stopped(stop_code::NOT_ACCEPTING_DATA),
                false,
                0,
            );
        }
        assert!(!p.a.sends_data());
        assert_eq!(p.a.stats().send_event_count(QuicSendEvent::DataRefused), 2);
        p.a.debug_assert_carries_data();
    });
}

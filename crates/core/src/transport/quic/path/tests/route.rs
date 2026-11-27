//! The table of `path::route` and what `QuicPath::settle` does with an outcome.

use super::super::super::socket::CapHoldReading;
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
            BoundedSend::Failed(SendFailure::NotOpened(NotOpened::Paced)),
            (SendOnUdp, R::NotOpenedUdp),
            (SendOnUdp, R::NotOpenedUdp),
        ),
        (
            BoundedSend::Failed(SendFailure::TimedOut),
            (SendOnUdp, R::FailedUdp),
            (GiveUp, R::Dropped),
        ),
        (
            BoundedSend::Failed(SendFailure::Paced),
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

/// The timeouts that count: each ran out with its stream open.
fn timeouts() -> [BoundedSend; 2] {
    [
        BoundedSend::MaybeDelivered(MaybeDelivered::TimedOut),
        BoundedSend::Failed(SendFailure::TimedOut),
    ]
}

/// A send that ran out before its stream opened waited for its source, a
/// permit or the peer's stream credit, which is back-pressure: it never
/// counts, however many come in a row on a quiet path.
#[test]
fn a_send_that_never_opened_does_not_count() {
    let not_opened = BoundedSend::Failed(SendFailure::NotOpened(NotOpened::TimedOut));
    let mut count = StallCount::new(Some(5), 0);
    for _ in 0..2 * STALLED_SENDS_LIMIT {
        assert!(!count.take_uncapped(&not_opened, false, Some(5), QUIET));
    }
    let mut count = StallCount::new(None, 0);
    for _ in 0..2 * STALLED_SENDS_LIMIT {
        assert!(!count.take_uncapped(&not_opened, false, None, 0));
    }
}

/// The eighth timeout in a row closes once nothing has been acknowledged for the
/// window, whichever kind of counting timeout each is; a `Sent` starts the count again;
/// and an outcome that is no timeout leaves the count alone. A send waiting for
/// its source does not count.
#[test]
fn timeouts_on_a_quiet_path_close_it_and_a_sent_starts_again() {
    let mut count = StallCount::new(Some(5), 0);
    let mut take = |outcome: &BoundedSend| count.take_uncapped(outcome, false, Some(5), QUIET);
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
        !count.take_uncapped(&timeouts()[1], true, Some(5), QUIET),
        "a stalled source"
    );
    let mut take = |outcome: &BoundedSend| count.take_uncapped(outcome, false, Some(5), QUIET);
    assert!(take(&timeouts()[0]), "the eighth");
    // Past the limit the verdict holds, so a close left alone at the limit
    // (`QuicPath::settle`) is made at a later timeout; the path counts it once.
    assert!(take(&timeouts()[1]), "past the limit");
}

/// While acknowledgements arrive, no timeout counts: a busy path is not a stalled
/// one. Each acknowledgement starts the count again, and the window runs from it.
#[test]
fn timeouts_count_only_once_nothing_is_acknowledged_for_the_window() {
    let mut count = StallCount::new(Some(0), 0);
    // Inside the window after the path was built.
    for _ in 0..2 * STALLED_SENDS_LIMIT {
        assert!(!count.take_uncapped(&timeouts()[0], false, Some(0), 9 * SECOND));
    }
    // Packets acknowledged every second: never quiet.
    for second in 0..40u64 {
        assert!(!count.take_uncapped(
            &timeouts()[0],
            false,
            Some(second + 1),
            (10 + second) * SECOND
        ));
    }
    // Seven quiet timeouts, then an acknowledgement, then seven more: no close.
    let last = 49 * SECOND;
    for _ in 1..STALLED_SENDS_LIMIT {
        assert!(!count.take_uncapped(&timeouts()[1], false, Some(40), last + QUIET));
    }
    assert!(!count.take_uncapped(&timeouts()[1], false, Some(41), last + QUIET));
    let quiet_again = last + 2 * QUIET;
    for _ in 1..STALLED_SENDS_LIMIT {
        assert!(!count.take_uncapped(&timeouts()[1], false, Some(41), quiet_again));
    }
    assert!(count.take_uncapped(&timeouts()[1], false, Some(41), quiet_again));
}

/// A hold of the egress cap counts as progress, but only within the window of
/// the connection's last acknowledgement (a dark connection's own probes, held
/// by a saturated cap, read as progress). An acknowledgement at 5 s
/// and a hold live from then on: no timeout counts while the hold is within the
/// window of it (to 15 s); after that the hold counts for nothing, the last
/// progress stays at 15 s, and from 25 s eight timeouts close the path. A hold
/// reported with no new event past its validity is a poller left behind and
/// does not count.
#[test]
fn a_cap_hold_counts_as_progress() {
    let reading = |holds, held_now| CapHoldReading {
        holds,
        held_now,
        validity: Duration::from_secs(60),
        longest_wait: Duration::ZERO,
    };
    let mut count = StallCount::new(Some(0), 0);
    let acked = 5 * SECOND;
    assert!(
        !count
            .take(
                &timeouts()[1],
                false,
                Some(1),
                reading(1, true),
                None,
                acked
            )
            .quiet_timeout
    );
    for second in 1..=10u64 {
        let now = acked + second * SECOND;
        assert!(
            !count
                .take(&timeouts()[1], false, Some(1), reading(1, true), None, now)
                .quiet_timeout,
            "held at {second} s after the acknowledgement: progress"
        );
    }
    for second in 11..20u64 {
        let now = acked + second * SECOND;
        assert!(
            !count
                .take(&timeouts()[1], false, Some(1), reading(1, true), None, now)
                .quiet_timeout,
            "within the window of the last progress (15 s)"
        );
    }
    let late = acked + 20 * SECOND;
    for _ in 1..STALLED_SENDS_LIMIT {
        let verdict = count.take(&timeouts()[1], false, Some(1), reading(1, true), None, late);
        assert!(
            verdict.quiet_timeout && !verdict.close,
            "the hold no longer counts"
        );
    }
    assert!(
        count
            .take(&timeouts()[1], false, Some(1), reading(1, true), None, late)
            .close,
        "eight timeouts with only a hold close the path"
    );

    // A hold that stays reported with no new event outlives its validity.
    let mut count = StallCount::new(Some(0), 0);
    assert!(
        !count
            .take(&timeouts()[1], false, Some(0), reading(7, true), None, 0)
            .close
    );
    let stale = 61 * SECOND + QUIET;
    for _ in 1..STALLED_SENDS_LIMIT {
        assert!(
            !count
                .take(
                    &timeouts()[1],
                    false,
                    Some(0),
                    reading(7, true),
                    None,
                    stale
                )
                .close
        );
    }
    assert!(
        count
            .take(
                &timeouts()[1],
                false,
                Some(0),
                reading(7, true),
                None,
                stale
            )
            .close
    );
}

/// A real path whose transmits a low egress cap holds, with nothing newly
/// acknowledged for longer than the window: send timeouts do not close it.
#[test]
fn a_binding_cap_with_no_acknowledgements_keeps_the_path() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths_capped("quic-route-cap", 50).await;
        let (a, time) = (p.a.clone(), p.time());
        p.drive(async move {
            // A stream the cap holds for many minutes at 50 B/s.
            let sender = {
                let (a, time) = (a.clone(), time.clone());
                tokio::spawn(async move {
                    a.send_stream_bounded(
                        &time,
                        Duration::from_secs(3600),
                        StreamId::next(),
                        None,
                        payload(64 * 1024).into(),
                        None,
                    )
                    .await
                })
            };
            let progress = a.ack_progress.clone().expect("counts acknowledgements");
            let not_a_timeout = error(QuicSendError::Closed);
            // Find a span past the window in which nothing was acknowledged and the
            // cap held throughout. Without the exemption the timeouts below count.
            let mut found = false;
            for _ in 0..20 {
                a.settle(SendKind::Stream, &not_a_timeout, false, time.now_nanos());
                let acked = progress.acked();
                time.sleep(Duration::from_secs(11)).await;
                if progress.acked() == acked && a.connection.cap_holds().read().held_now {
                    found = true;
                    break;
                }
            }
            assert!(found, "no quiet span with the cap holding");
            for _ in 0..STALLED_SENDS_LIMIT {
                a.settle(SendKind::Stream, &timeouts()[1], false, time.now_nanos());
            }
            assert!(a.quinn_connection().close_reason().is_none());
            assert_eq!(a.stats().send_event_count(QuicSendEvent::StalledClosed), 0);
            sender.abort();
        })
        .await;
    });
}

/// A timeout that counts makes the path suspect: new sends go on UDP. It stops
/// being suspect the first time it is asked after the connection acknowledged
/// something new. A timeout inside the window does not make it suspect.
#[test]
fn a_path_with_a_quiet_timeout_is_suspect_until_the_peer_acknowledges() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-route-suspect", QuicPathLimits::DEFAULT).await;
        let (a, time) = (p.a.clone(), p.time());
        let built = time.now_nanos();
        a.settle(SendKind::Short, &timeouts()[0], false, built + SECOND);
        assert!(!a.is_suspect(), "inside the window");
        a.settle(SendKind::Short, &timeouts()[0], false, built + QUIET);
        assert!(a.is_suspect());
        assert!(a.is_suspect(), "until something is acknowledged");
        assert_eq!(a.stats().send_event_count(QuicSendEvent::SuspectEntered), 1);
        let control = p
            .drive(async move {
                a.send_control_bounded(
                    &time,
                    Duration::from_secs(5),
                    QuicControl::Ready {
                        attempt: 1,
                        caps: 0,
                    },
                )
                .await
            })
            .await;
        assert!(matches!(control, BoundedSend::Sent), "{control:?}");
        assert!(!p.a.is_suspect(), "acknowledged again");
        assert_eq!(p.a.stats().send_event_count(QuicSendEvent::SuspectLeft), 1);
        assert!(p.a.quinn_connection().close_reason().is_none());
    });
}

/// The stall close on a path with a UDP session leaves alone a path the dark
/// rule would have closed (the watch's flag, `recycle::AckWatch::publishing_dark`):
/// with the flag set before the limit, timeouts past it close nothing; once the
/// flag clears (the peer heard again) the next counted timeout closes it, the
/// verdict holding past the limit, counted once. With the flag set only after
/// the eighth timeout, or a stall the dark rule would not have closed (the peer
/// still heard: the flag never set), the close comes at the eighth, as before.
/// A native path closes whatever the flag says.
#[test]
fn the_stall_close_leaves_a_path_the_dark_rule_would_have_closed() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let closed = |p: &Paths| p.a.quinn_connection().close_reason().is_some();
        let stalled = |p: &Paths| p.a.stats().send_event_count(QuicSendEvent::StalledClosed);

        let p = paths("quic-route-stall-dark", QuicPathLimits::DEFAULT).await;
        let later = p.time().now_nanos() + QUIET;
        p.a.set_dark_for_test(true);
        for _ in 0..STALLED_SENDS_LIMIT + 2 {
            p.a.settle(SendKind::Stream, &timeouts()[1], false, later);
        }
        assert!(!closed(&p), "left to the idle timeout while dark");
        assert!(p.a.is_suspect(), "new data goes on UDP");
        assert_eq!(stalled(&p), 0);
        p.a.set_dark_for_test(false);
        p.a.settle(SendKind::Stream, &timeouts()[1], false, later);
        assert!(closed(&p), "closed past the limit once the flag cleared");
        p.a.settle(SendKind::Stream, &timeouts()[1], false, later);
        assert_eq!(stalled(&p), 1);

        let p = paths("quic-route-stall-dark-late", QuicPathLimits::DEFAULT).await;
        let later = p.time().now_nanos() + QUIET;
        for _ in 0..STALLED_SENDS_LIMIT {
            p.a.settle(SendKind::Stream, &timeouts()[1], false, later);
        }
        assert!(closed(&p), "the dark rule would not have closed yet");
        p.a.set_dark_for_test(true);
        p.a.settle(SendKind::Stream, &timeouts()[1], false, later);
        assert_eq!(stalled(&p), 1);

        let p = paths_native("quic-route-stall-dark-native").await;
        let later = p.time().now_nanos() + QUIET;
        p.a.set_dark_for_test(true);
        for _ in 0..STALLED_SENDS_LIMIT {
            p.a.settle(SendKind::Stream, &timeouts()[1], false, later);
        }
        assert!(closed(&p), "a native path has no UDP to leave it to");
        assert_eq!(stalled(&p), 1);
    });
}

/// A connection whose controller counts no acknowledgements cannot tell a busy
/// path from a stalled one, so every timeout counts, as before the window.
#[test]
fn with_no_acknowledgement_count_every_timeout_counts() {
    let mut count = StallCount::new(None, 0);
    for _ in 1..STALLED_SENDS_LIMIT {
        assert!(!count.take_uncapped(&timeouts()[0], false, None, 0));
    }
    assert!(count.take_uncapped(&timeouts()[0], false, None, 0));
}

/// On a real path, with nothing acknowledged since it was built: eight timeouts
/// past the window close it, counted once, and the path reads the connection's
/// own acknowledgement count.
#[test]
fn a_quiet_path_closes_after_eight_timeouts() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-route-stall", QuicPathLimits::DEFAULT).await;
        assert!(
            p.a.ack_progress.is_some(),
            "the connection counts acknowledgements"
        );
        let later = p.time().now_nanos() + QUIET;
        for _ in 1..STALLED_SENDS_LIMIT {
            p.a.settle(SendKind::Stream, &timeouts()[1], false, later);
        }
        assert!(p.a.quinn_connection().close_reason().is_none());
        p.a.settle(SendKind::Stream, &timeouts()[1], false, later);
        assert!(p.a.quinn_connection().close_reason().is_some());
        p.a.settle(SendKind::Stream, &timeouts()[1], false, later);
        assert_eq!(
            p.a.stats().send_event_count(QuicSendEvent::StalledClosed),
            1
        );
    });
}

/// A send the pacer refused is held back on this side and is not a timeout:
/// any number of them, past the window with nothing acknowledged, neither
/// close the path nor make it suspect. A paced path whose sends really time out
/// (it went dark) closes after eight, as an unpaced one does: pacing adds no
/// exemption beyond the refusals themselves.
#[test]
fn pacer_refusals_never_count_and_a_dark_paced_path_still_closes() {
    use super::super::pacer::LimitPacer;
    use crate::transport::quic::config::QuicRateLimit;
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-route-paced", QuicPathLimits::DEFAULT).await;
        p.a.pace_with(Arc::new(
            LimitPacer::new(QuicRateLimit::new(Some(100_000), None), p.time())
                .expect("a limit paces"),
        ));
        let later = p.time().now_nanos() + QUIET;
        let refusals = [
            BoundedSend::Failed(SendFailure::NotOpened(NotOpened::Paced)),
            BoundedSend::Failed(SendFailure::Paced),
        ];
        for refused in refusals
            .iter()
            .cycle()
            .take(STALLED_SENDS_LIMIT as usize + 2)
        {
            p.a.settle(SendKind::Stream, refused, false, later);
        }
        assert!(p.a.quinn_connection().close_reason().is_none());
        assert!(!p.a.is_suspect(), "a refusal is not a quiet timeout");
        assert_eq!(
            p.a.stats().send_event_count(QuicSendEvent::PacerRefused),
            u64::from(STALLED_SENDS_LIMIT) + 2,
            "each refusal is counted once"
        );
        for _ in 1..STALLED_SENDS_LIMIT {
            p.a.settle(SendKind::Stream, &timeouts()[1], false, later);
        }
        assert!(p.a.is_suspect(), "a dark paced path goes suspect");
        assert!(p.a.quinn_connection().close_reason().is_none());
        p.a.settle(SendKind::Stream, &timeouts()[1], false, later);
        assert!(
            p.a.quinn_connection().close_reason().is_some(),
            "and closes at the eighth timeout"
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
        let p = paths("quic-route-refused", QuicPathLimits::DEFAULT).await;
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

/// A [`QuietProof`](super::super::bounded::QuietProof) dates the progress the
/// stall count sees from its `since`, so a timeout right after a path went dark
/// mid-send counts; one whose counts no longer match the connection's (an
/// acknowledgement or a cap hold came after it) is ignored, and the progress is
/// dated when it is seen.
#[test]
fn a_quiet_proof_dates_progress_only_while_its_counts_match() {
    use super::super::bounded::QuietProof;
    let free = CapHoldReading {
        holds: 3,
        held_now: false,
        validity: Duration::from_secs(60),
        longest_wait: Duration::ZERO,
    };
    let proof = QuietProof {
        acked: 9,
        holds: 3,
        since: SECOND,
        live_hold_seen: false,
    };
    let now = SECOND + QUIET;
    let timeout = &timeouts()[1];
    // The acknowledgements it sees (5 to 9) came no later than `since`: quiet.
    let mut count = StallCount::new(Some(5), 0);
    let verdict = count.take(timeout, false, Some(9), free, Some(proof), now);
    assert!(verdict.quiet_timeout, "counted from the proof's since");
    // One more acknowledgement after the proof: progress now, not quiet.
    let mut count = StallCount::new(Some(5), 0);
    let verdict = count.take(timeout, false, Some(10), free, Some(proof), now);
    assert!(
        !verdict.quiet_timeout,
        "the proof no longer matches the acked count"
    );
    // A cap hold after the proof: the same.
    let mut count = StallCount::new(Some(5), 0);
    let held_since = CapHoldReading { holds: 4, ..free };
    let verdict = count.take(timeout, false, Some(9), held_since, Some(proof), now);
    assert!(
        !verdict.quiet_timeout,
        "the proof no longer matches the hold count"
    );
    // Without a proof the same acknowledgements are dated when seen.
    let mut count = StallCount::new(Some(5), 0);
    let verdict = count.take(timeout, false, Some(9), free, None, now);
    assert!(!verdict.quiet_timeout);
    // An earlier settle saw the acknowledgements and dated them late: the
    // proof, still matching, dates them back although this settle sees nothing
    // new.
    let mut count = StallCount::new(Some(5), 0);
    let early = SECOND * 5;
    let _ = count.take(&BoundedSend::Sent, false, Some(9), free, None, early);
    let verdict = count.take(timeout, false, Some(9), free, None, now);
    assert!(!verdict.quiet_timeout, "dated at the earlier settle");
    let verdict = count.take(timeout, false, Some(9), free, Some(proof), now);
    assert!(verdict.quiet_timeout, "dated back to the proof's since");

    // A send that saw a live hold proves nothing: a live hold is progress the
    // counts do not show, and it may have been live after `since`.
    let mut count = StallCount::new(Some(5), 0);
    let _ = count.take(&BoundedSend::Sent, false, Some(9), free, None, early);
    let saw_hold = QuietProof {
        live_hold_seen: true,
        ..proof
    };
    let verdict = count.take(timeout, false, Some(9), free, Some(saw_hold), now);
    assert!(!verdict.quiet_timeout, "the proof's send saw a live hold");

    // A hold still reported now, past its validity so not progress by itself,
    // keeps the proof from applying.
    let briefly = CapHoldReading {
        held_now: true,
        validity: SECOND_DURATION,
        ..free
    };
    let mut count = StallCount::new(Some(5), 0);
    let _ = count.take(&BoundedSend::Sent, false, Some(9), briefly, None, early);
    let verdict = count.take(timeout, false, Some(9), briefly, Some(proof), now);
    assert!(!verdict.quiet_timeout, "a hold is reported at the settle");

    // The proof only ever moves the last progress earlier: an earlier settle
    // that dated it before `since` keeps that date.
    let mut count = StallCount::new(Some(5), 0);
    let _ = count.take(&BoundedSend::Sent, false, Some(9), free, None, SECOND);
    let later = QuietProof {
        since: SECOND * 5,
        ..proof
    };
    let verdict = count.take(timeout, false, Some(9), free, Some(later), SECOND * 12);
    assert!(verdict.quiet_timeout, "kept at 1 s, not moved to 5 s");
}

const SECOND_DURATION: Duration = Duration::from_secs(1);

/// A hold's credit after the last acknowledgement is the window, or the
/// longest one transmit can wait at the cap if that is longer, from the one
/// derivation `cap_hold_validity` also uses: the reading carries
/// `cap_transmit_wait` of the cap's rate, and the validity is that wait times
/// its margin. About 19.8 s at 64 KiB/s, 13.3 s at 100 kB/s; the window at
/// 1 MB/s.
#[test]
fn hold_credit_is_the_window_or_the_caps_longest_wait() {
    use super::super::super::socket::{
        CAP_HOLD_VALIDITY_MARGIN, CapHolds, cap_hold_validity, cap_transmit_wait,
    };
    use super::super::route::{STALL_PROGRESS_WINDOW, hold_credit};
    for rate in [50, 64 * 1024, 100_000, 1_000_000, 1_000_000_000] {
        let reading = CapHolds::new(Some(rate)).read();
        assert_eq!(reading.longest_wait, cap_transmit_wait(rate), "{rate}");
        assert_eq!(
            reading
                .longest_wait
                .saturating_mul(CAP_HOLD_VALIDITY_MARGIN),
            cap_hold_validity(rate),
            "{rate}: the validity is the same wait with its margin"
        );
        assert_eq!(
            hold_credit(&reading),
            STALL_PROGRESS_WINDOW.max(cap_transmit_wait(rate)),
            "{rate}"
        );
    }
    let credit = |rate| hold_credit(&CapHolds::new(Some(rate)).read()).as_secs_f64();
    assert!(
        (19.5..20.0).contains(&credit(64 * 1024)),
        "{}",
        credit(64 * 1024)
    );
    assert!(
        (13.0..13.5).contains(&credit(100_000)),
        "{}",
        credit(100_000)
    );
    assert_eq!(credit(1_000_000), STALL_PROGRESS_WINDOW.as_secs_f64());
    assert_eq!(
        hold_credit(&CapHolds::new(None).read()),
        STALL_PROGRESS_WINDOW,
        "no cap: the window"
    );
}

/// At a cap where one transmit can wait longer than the window (100 kB/s:
/// about 13.3 s), a live path whose transmits the cap holds, acknowledged at
/// 5 s and nothing since, is not counted as stalled while the hold is within
/// that credit (18 s), past the 10 s window; after the credit and a window with
/// nothing acknowledged its timeouts count.
#[test]
fn a_hold_is_credited_for_the_caps_longest_wait_past_the_window() {
    use super::super::super::socket::CapHolds;
    use super::super::route::hold_credit;
    let held = CapHoldReading {
        holds: 1,
        held_now: true,
        ..CapHolds::new(Some(100_000)).read()
    };
    let credit = hold_credit(&held).as_nanos() as u64;
    assert!(credit > QUIET - SECOND, "premise: past the window");
    let mut count = StallCount::new(Some(0), 0);
    let acked = 5 * SECOND;
    assert!(
        !count
            .take(&timeouts()[1], false, Some(1), held, None, acked)
            .quiet_timeout
    );
    // Past the window (10 s after the acknowledgement), within the credit.
    let within = acked + credit - SECOND;
    assert!(
        !count
            .take(&timeouts()[1], false, Some(1), held, None, within)
            .quiet_timeout,
        "a live path held past the window is not stalled"
    );
    // The hold no longer counts once past the credit; a window after the last
    // progress (`within`), timeouts count.
    let late = within + QUIET;
    assert!(
        count
            .take(&timeouts()[1], false, Some(1), held, None, late)
            .quiet_timeout
    );
}

/// A matching quiet proof moves the last acknowledgement back too, not only the
/// last progress: a settle that first sees an acknowledgement dates it when it
/// sees it (8 s), but a proof says nothing moved after its `since` (1 s), so a
/// later hold (at 12 s) is past the credit after the real acknowledgement and
/// is no progress. Dated at 8 s, the hold would be credited and the timeout
/// would not count.
#[test]
fn a_quiet_proof_dates_the_last_acknowledgement_back() {
    use super::super::bounded::QuietProof;
    let free = CapHoldReading {
        holds: 3,
        held_now: false,
        validity: Duration::from_secs(60),
        longest_wait: Duration::ZERO,
    };
    let held = CapHoldReading {
        holds: 4,
        held_now: true,
        ..free
    };
    let proof = QuietProof {
        acked: 5,
        holds: 3,
        since: SECOND,
        live_hold_seen: false,
    };
    let mut count = StallCount::new(Some(0), 0);
    let _ = count.take(&BoundedSend::Sent, false, Some(5), free, None, 8 * SECOND);
    let _ = count.take(
        &timeouts()[1],
        false,
        Some(5),
        free,
        Some(proof),
        9 * SECOND,
    );
    let verdict = count.take(&timeouts()[1], false, Some(5), held, None, 12 * SECOND);
    assert!(
        verdict.quiet_timeout,
        "the hold at 12 s is past the credit after the acknowledgement at 1 s"
    );
}

/// Only a native path (no UDP session) that still takes data retries, only a send
/// that waited out its open deadline for the peer's stream credit, and only
/// `NOT_OPENED_RETRIES` times. Each retry is counted once.
#[test]
fn retry_not_opened_decides_by_path_outcome_and_attempt() {
    use super::super::route::NOT_OPENED_RETRIES;
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let no_credit = BoundedSend::Failed(SendFailure::NotOpened(NotOpened::NoCredit));
        let native = paths_native("quic-retry-decision-native").await;
        let a = &native.a;
        let retried = || a.stats().send_event_count(QuicSendEvent::NotOpenedRetried);
        for attempt in 0..NOT_OPENED_RETRIES {
            assert!(a.retry_not_opened(&no_credit, attempt), "attempt {attempt}");
        }
        assert_eq!(retried(), u64::from(NOT_OPENED_RETRIES));
        for attempt in [NOT_OPENED_RETRIES, NOT_OPENED_RETRIES + 1, u32::MAX] {
            assert!(!a.retry_not_opened(&no_credit, attempt), "past the cap");
        }
        // A wait on this side's own resources, a pacer refusal, a lost connection,
        // and every outcome that is not a send that never opened.
        for outcome in [
            BoundedSend::Failed(SendFailure::NotOpened(NotOpened::TimedOut)),
            BoundedSend::Failed(SendFailure::NotOpened(NotOpened::Paced)),
            BoundedSend::Failed(SendFailure::NotOpened(NotOpened::ConnectionLost(lost()))),
            BoundedSend::Failed(SendFailure::TimedOut),
            BoundedSend::Failed(SendFailure::Paced),
            BoundedSend::Failed(SendFailure::Error(QuicSendError::Closed)),
            stopped(stop_code::NOT_ACCEPTING_DATA),
            BoundedSend::MaybeDelivered(MaybeDelivered::TimedOut),
            BoundedSend::Sent,
        ] {
            assert!(!a.retry_not_opened(&outcome, 0), "{outcome:?}");
        }
        assert_eq!(
            retried(),
            u64::from(NOT_OPENED_RETRIES),
            "none of those counted"
        );
        // The peer refused data: it is not asked again.
        a.settle(
            SendKind::Stream,
            &stopped(stop_code::NOT_ACCEPTING_DATA),
            false,
            0,
        );
        assert!(!a.sends_data());
        assert!(
            !a.retry_not_opened(&no_credit, 0),
            "a path that refused data"
        );

        // An upgraded path has UDP to fall back to, and never retries.
        let upgraded = paths("quic-retry-decision-udp", QuicPathLimits::DEFAULT).await;
        assert!(!upgraded.a.retry_not_opened(&no_credit, 0));
        assert_eq!(
            upgraded
                .a
                .stats()
                .send_event_count(QuicSendEvent::NotOpenedRetried),
            0
        );
    })
}

/// Every outcome that can end on UDP for some kind of send, with the step a
/// path WITH a UDP session takes for a Short (also a LargeShort) and for a
/// Stream (also a Pipe). Written out, not computed from `next_step`, so a change
/// to the table or to what `settle` does with it shows here.
fn udp_rows() -> Vec<(BoundedSend, NextStep, NextStep)> {
    use NextStep::{GiveUp, SendOnUdp};
    let not_opened = |n| BoundedSend::Failed(SendFailure::NotOpened(n));
    vec![
        (not_opened(NotOpened::TimedOut), SendOnUdp, SendOnUdp),
        (not_opened(NotOpened::Paced), SendOnUdp, SendOnUdp),
        (not_opened(NotOpened::NoCredit), SendOnUdp, SendOnUdp),
        (
            not_opened(NotOpened::ConnectionLost(lost())),
            SendOnUdp,
            SendOnUdp,
        ),
        (
            error(QuicSendError::InvalidFrame(framing_error_for_tests())),
            SendOnUdp,
            SendOnUdp,
        ),
        (stopped(stop_code::NOT_ACCEPTING_DATA), SendOnUdp, SendOnUdp),
        (
            BoundedSend::Failed(SendFailure::TimedOut),
            SendOnUdp,
            GiveUp,
        ),
        (BoundedSend::Failed(SendFailure::Paced), SendOnUdp, GiveUp),
        (
            error(QuicSendError::ConnectionLost(lost())),
            SendOnUdp,
            GiveUp,
        ),
        (error(QuicSendError::Closed), SendOnUdp, GiveUp),
        (stopped(stop_code::BUDGET_REFUSED), SendOnUdp, GiveUp),
    ]
}

/// With the build constant at its shipped value an upgraded path is what it was
/// before the constant: its sends go on UDP (the steps are written out in
/// [`udp_rows`]), a quiet timeout makes it suspect, and nothing is retried on it
/// (D-3, dark).
#[test]
fn an_upgraded_path_keeps_its_udp_fallback_while_the_constant_is_true() {
    const { assert!(super::super::super::breaker::UDP_FALLBACK) };
    assert!(QuicPathLimits::DEFAULT.effective_udp_fallback());
    assert!(!QuicPathLimits::NATIVE.effective_udp_fallback());
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-route-constant-true", QuicPathLimits::DEFAULT).await;
        let a = &p.a;
        assert!(a.udp_fallback());
        for (outcome, short, stream) in udp_rows() {
            let path = paths("quic-route-constant-true-row", QuicPathLimits::DEFAULT).await;
            for (kind, expected) in [
                (SendKind::Short, short),
                (SendKind::LargeShort, short),
                (SendKind::Stream, stream),
                (SendKind::Pipe, stream),
            ] {
                assert_eq!(
                    path.a.settle(kind, &outcome, false, 0),
                    expected,
                    "{kind:?} {outcome:?}"
                );
            }
        }
        let no_credit = BoundedSend::Failed(SendFailure::NotOpened(NotOpened::NoCredit));
        assert!(!a.retry_not_opened(&no_credit, 0));
        let built = p.time().now_nanos();
        a.settle(SendKind::Short, &timeouts()[0], false, built + SECOND);
        a.settle(SendKind::Short, &timeouts()[0], false, built + QUIET);
        assert!(a.is_suspect());
    });
}

/// With the build constant at `false` a path built on the default limits has no
/// UDP fallback (the path level only; the connection keeps its UDP session):
/// what would go on UDP is given up (the operation layer retries), counted
/// dropped, a send that found no stream credit is tried again on the path, and a
/// quiet timeout never makes it suspect, since there is nowhere to divert to.
/// The test runs in a child process with the constant off
/// (`breaker::run_with_no_udp_fallback`).
#[test]
fn with_no_udp_fallback_an_upgraded_path_gives_up_instead_of_going_on_udp() {
    if !super::super::super::breaker::run_with_no_udp_fallback(
        module_path!(),
        "with_no_udp_fallback_an_upgraded_path_gives_up_instead_of_going_on_udp",
    ) {
        return;
    }
    assert!(!QuicPathLimits::DEFAULT.effective_udp_fallback());
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-route-constant-false", QuicPathLimits::DEFAULT).await;
        let a = &p.a;
        assert!(!a.udp_fallback());
        for (outcome, _, _) in udp_rows() {
            let path = paths("quic-route-constant-false-row", QuicPathLimits::DEFAULT).await;
            for kind in [
                SendKind::Short,
                SendKind::LargeShort,
                SendKind::Stream,
                SendKind::Pipe,
            ] {
                assert_eq!(
                    path.a.settle(kind, &outcome, false, 0),
                    NextStep::GiveUp,
                    "{kind:?} {outcome:?}"
                );
                assert_eq!(
                    path.a.stats().send_count(kind, QuicSendResult::Dropped),
                    1,
                    "{kind:?} {outcome:?} is counted dropped"
                );
            }
        }
        let no_credit = BoundedSend::Failed(SendFailure::NotOpened(NotOpened::NoCredit));
        assert!(a.retry_not_opened(&no_credit, 0));
        let built = p.time().now_nanos();
        a.settle(SendKind::Short, &timeouts()[0], false, built + SECOND);
        for i in 1..4 {
            a.settle(SendKind::Short, &timeouts()[0], false, built + QUIET * i);
        }
        assert!(!a.is_suspect());
        assert_eq!(a.stats().send_event_count(QuicSendEvent::SuspectEntered), 0);
    });
}

/// What `short_dropped_<cause>` a path's counters hold, as `(label, count)` for
/// the non-zero ones.
fn short_drop_counts(path: &QuicPath) -> Vec<(&'static str, u64)> {
    crate::transport::metrics::ShortDropCause::ALL
        .iter()
        .enumerate()
        .map(|(i, cause)| {
            (
                crate::transport::metrics::SHORT_DROP_LABELS[i],
                path.stats().short_drop_count(*cause),
            )
        })
        .filter(|(_, n)| *n != 0)
        .collect()
}

/// The labels of `SHORT_DROP_LABELS` are in the order of the cause enum, so a
/// cause lands under the label it names.
#[test]
fn short_drop_causes_follow_their_labels() {
    use crate::transport::metrics::SHORT_DROP_LABELS;
    assert_eq!(
        SHORT_DROP_LABELS.len(),
        crate::transport::metrics::ShortDropCause::ALL.len()
    );
    for (i, cause) in crate::transport::metrics::ShortDropCause::ALL
        .iter()
        .enumerate()
    {
        assert_eq!(*cause as usize, i, "{cause:?}");
    }
}

/// A Short a native path gives up is counted dropped under exactly one cause, the
/// one its outcome names, and the causes sum to the dropped count. A path with
/// no UDP session gives up what would have gone on UDP, so every outcome of the
/// table lands somewhere. A stream's drop has no cause.
#[test]
fn a_dropped_short_is_counted_under_the_cause_of_its_outcome() {
    use QuicSendError as E;
    let not_opened = |n| BoundedSend::Failed(SendFailure::NotOpened(n));
    let rows: Vec<(BoundedSend, &str)> = vec![
        (not_opened(NotOpened::TimedOut), "not_opened_exhausted"),
        (not_opened(NotOpened::NoCredit), "not_opened_exhausted"),
        (not_opened(NotOpened::Paced), "paced"),
        (
            not_opened(NotOpened::ConnectionLost(lost())),
            "connection_lost",
        ),
        (BoundedSend::Failed(SendFailure::TimedOut), "timed_out"),
        (BoundedSend::Failed(SendFailure::Paced), "paced"),
        (error(E::ConnectionLost(lost())), "connection_lost"),
        (error(E::Closed), "connection_lost"),
        (
            error(E::InvalidFrame(framing_error_for_tests())),
            "invalid_frame",
        ),
        (
            error(E::LengthMismatch {
                declared: 2,
                delivered: 1,
            }),
            "source_error",
        ),
        (stopped(stop_code::NOT_ACCEPTING_DATA), "peer_refused_data"),
        (stopped(stop_code::BUDGET_REFUSED), "budget_refused"),
        (stopped(stop_code::CHANNEL_FULL), "stopped_other"),
        (stopped(0x1f), "stopped_other"),
    ];
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        for (i, (outcome, label)) in rows.iter().enumerate() {
            let name: &'static str =
                Box::leak(format!("quic-short-drop-cause-{i}").into_boxed_str());
            let p = paths_native(name).await;
            assert_eq!(
                p.a.settle(SendKind::Short, outcome, false, 0),
                NextStep::GiveUp,
                "{outcome:?}"
            );
            assert_eq!(
                p.a.stats()
                    .send_count(SendKind::Short, QuicSendResult::Dropped),
                1
            );
            assert_eq!(short_drop_counts(&p.a), vec![(*label, 1)], "{outcome:?}");

            // The same outcome for a Stream or a large short is dropped without
            // a short cause.
            let q = paths_native(Box::leak(format!("{name}-other").into_boxed_str())).await;
            q.a.settle(SendKind::Stream, outcome, false, 0);
            q.a.settle(SendKind::LargeShort, outcome, false, 0);
            assert!(short_drop_counts(&q.a).is_empty(), "{outcome:?}");
        }
    });
}

/// A path with a UDP session gives up only what UDP cannot carry, and counts it
/// the same way; what goes on UDP is not a drop and has no cause.
#[test]
fn a_path_with_a_udp_session_counts_only_what_it_gives_up() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-short-drop-cause-udp", QuicPathLimits::DEFAULT).await;
        assert!(p.a.udp_fallback());
        assert_eq!(
            p.a.settle(SendKind::Short, &stopped(0x1f), false, 0),
            NextStep::GiveUp
        );
        assert_eq!(
            p.a.settle(
                SendKind::Short,
                &BoundedSend::Failed(SendFailure::TimedOut),
                false,
                0
            ),
            NextStep::SendOnUdp
        );
        assert_eq!(
            p.a.stats()
                .send_count(SendKind::Short, QuicSendResult::Dropped),
            1
        );
        assert_eq!(short_drop_counts(&p.a), vec![("stopped_other", 1)]);
    });
}

/// Every call that counts a data send, in the production source of the
/// transport (comments stripped, test modules cut at their `#[cfg(test)]`), as
/// `file: callee(arguments)` with whitespace removed.
fn send_counting_calls(files: &[(String, String)]) -> Vec<String> {
    let mut calls = Vec::new();
    for (name, src) in files {
        let src = src
            .split("#[cfg(test)]\nmod ")
            .next()
            .expect("split yields one piece");
        let src = crate::contract::source_pin_util::strip_comments(src);
        for callee in [
            "record_send(",
            "record_send_unchecked(",
            "record_short_drop_cause(",
        ] {
            for (at, _) in src.match_indices(callee) {
                // A definition is not a call.
                if src[..at].trim_end().ends_with("fn") {
                    continue;
                }
                let open = at + callee.len();
                let mut depth = 1usize;
                let mut end = open;
                for (i, byte) in src.bytes().enumerate().skip(open) {
                    match byte {
                        b'(' => depth += 1,
                        b')' => {
                            depth -= 1;
                            if depth == 0 {
                                end = i;
                                break;
                            }
                        }
                        _ => {}
                    }
                }
                assert!(end > open, "{name}: unbalanced `{callee}`");
                let args: String = src[open..end]
                    .chars()
                    .filter(|c| !c.is_whitespace())
                    .collect();
                // The receiver's text before the callee, to its statement start.
                let recv_start = src[..at]
                    .rfind(|c: char| c.is_whitespace() || c == '{' || c == ';')
                    .map_or(0, |i| i + 1);
                let recv = &src[recv_start..at];
                calls.push(format!("{name}: {recv}{callee}{args})"));
            }
        }
    }
    calls.sort();
    calls
}

fn transport_production_sources() -> Vec<(String, String)> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/transport");
    let mut dirs = vec![root.clone()];
    let mut files = Vec::new();
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(&dir).expect("transport source dir") {
            let path = entry.expect("entry").path();
            let rel = path
                .strip_prefix(&root)
                .expect("under root")
                .to_string_lossy();
            let is_test_file = rel.split('/').any(|part| {
                part == "tests" || part.ends_with("tests.rs") || part.ends_with("_tests")
            });
            if path.is_dir() {
                if !is_test_file {
                    dirs.push(path);
                }
            } else if rel.ends_with(".rs") && !is_test_file {
                let text = std::fs::read_to_string(&path).expect("source");
                files.push((rel.to_string(), text));
            }
        }
    }
    files
}

/// What the production transport source may call to count a data send. A Short
/// is counted dropped only by `record_short_dropped`, which counts its cause with
/// it, so the causes cannot drift from `short_dropped`. This list is every
/// counting call, so a new site, a renamed receiver, or a direct call on the
/// node's counters fails here and has to be read: it is either a Short drop
/// (route it through `record_short_dropped`) or another kind of send.
const EXPECTED_SEND_COUNTING_CALLS: [&str; 6] = [
    "quic/path.rs: self.node.record_send(kind,result)",
    "quic/path.rs: self.node.record_short_drop_cause(cause)",
    "quic/path.rs: self.record_send_unchecked(SendKind::Short,QuicSendResult::Dropped)",
    "quic/path.rs: self.record_send_unchecked(kind,result)",
    "quic/path/route.rs: self.stats.record_send(kind,result)",
    // Not a data send: the loss gate's own round-trip sample.
    "quic/recycle/loss_gate.rs: self.record_send(sent,sample)",
];

#[test]
fn only_record_short_dropped_counts_a_dropped_short() {
    let files = transport_production_sources();
    assert!(files.len() > 20, "the scan found {} files", files.len());
    for (name, _) in [("quic/path.rs", ()), ("quic/path/route.rs", ())] {
        assert!(files.iter().any(|(n, _)| n == name), "{name} not scanned");
    }
    assert_eq!(
        send_counting_calls(&files),
        EXPECTED_SEND_COUNTING_CALLS,
        "a call that counts a data send was added, moved or renamed"
    );
    // `settle` is the one caller that passes a variable kind and result: it must
    // send a dropped Short to `record_short_dropped` before it counts anything.
    let route = &files
        .iter()
        .find(|(n, _)| n == "quic/path/route.rs")
        .expect("scanned")
        .1;
    let route = crate::contract::source_pin_util::strip_comments(route);
    let branch = route
        .find("kind == SendKind::Short && result == QuicSendResult::Dropped")
        .expect("settle's Short drop branch moved");
    let short = route[branch..]
        .find("record_short_dropped(short_drop_cause(outcome))")
        .expect("the Short drop branch does not call record_short_dropped");
    let plain = route[branch..]
        .find("self.stats.record_send(kind, result)")
        .expect("settle's plain count moved");
    assert!(short < plain, "the Short drop branch must come first");
}

/// The scan sees a second site planted in production code, and one on a
/// differently named receiver.
#[test]
fn the_send_counting_scan_sees_a_planted_site() {
    let planted =
        |body: &str| send_counting_calls(&[("quic/path/route.rs".to_string(), body.to_string())]);
    assert!(planted("fn f() { /* x.record_send(a, b) */ }").is_empty());
    assert_eq!(
        planted("fn f(s: &S) { s.node().record_send(SendKind::Short, QuicSendResult::Dropped); }"),
        ["quic/path/route.rs: s.node().record_send(SendKind::Short,QuicSendResult::Dropped)"]
    );
    assert!(
        planted(
            "fn record_send(&self) {}\n#[cfg(test)]\nmod tests { fn t() { a.record_send(1) } }"
        )
        .is_empty()
    );
}

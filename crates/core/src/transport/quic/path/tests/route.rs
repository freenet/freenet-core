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
        !count.take_uncapped(&timeouts()[2], true, Some(5), QUIET),
        "a stalled source"
    );
    let mut take = |outcome: &BoundedSend| count.take_uncapped(outcome, false, Some(5), QUIET);
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
                &timeouts()[2],
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
                .take(&timeouts()[2], false, Some(1), reading(1, true), None, now)
                .quiet_timeout,
            "held at {second} s after the acknowledgement: progress"
        );
    }
    for second in 11..20u64 {
        let now = acked + second * SECOND;
        assert!(
            !count
                .take(&timeouts()[2], false, Some(1), reading(1, true), None, now)
                .quiet_timeout,
            "within the window of the last progress (15 s)"
        );
    }
    let late = acked + 20 * SECOND;
    for _ in 1..STALLED_SENDS_LIMIT {
        let verdict = count.take(&timeouts()[2], false, Some(1), reading(1, true), None, late);
        assert!(
            verdict.quiet_timeout && !verdict.close,
            "the hold no longer counts"
        );
    }
    assert!(
        count
            .take(&timeouts()[2], false, Some(1), reading(1, true), None, late)
            .close,
        "eight timeouts with only a hold close the path"
    );

    // A hold that stays reported with no new event outlives its validity.
    let mut count = StallCount::new(Some(0), 0);
    assert!(
        !count
            .take(&timeouts()[2], false, Some(0), reading(7, true), None, 0)
            .close
    );
    let stale = 61 * SECOND + QUIET;
    for _ in 1..STALLED_SENDS_LIMIT {
        assert!(
            !count
                .take(
                    &timeouts()[2],
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
                &timeouts()[2],
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
                a.settle(SendKind::Stream, &timeouts()[2], false, time.now_nanos());
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
            p.a.settle(SendKind::Stream, &timeouts()[2], false, later);
        }
        assert!(p.a.is_suspect(), "a dark paced path goes suspect");
        assert!(p.a.quinn_connection().close_reason().is_none());
        p.a.settle(SendKind::Stream, &timeouts()[2], false, later);
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
    let timeout = &timeouts()[2];
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
            .take(&timeouts()[2], false, Some(1), held, None, acked)
            .quiet_timeout
    );
    // Past the window (10 s after the acknowledgement), within the credit.
    let within = acked + credit - SECOND;
    assert!(
        !count
            .take(&timeouts()[2], false, Some(1), held, None, within)
            .quiet_timeout,
        "a live path held past the window is not stalled"
    );
    // The hold no longer counts once past the credit; a window after the last
    // progress (`within`), timeouts count.
    let late = within + QUIET;
    assert!(
        count
            .take(&timeouts()[2], false, Some(1), held, None, late)
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
        &timeouts()[2],
        false,
        Some(5),
        free,
        Some(proof),
        9 * SECOND,
    );
    let verdict = count.take(&timeouts()[2], false, Some(5), held, None, 12 * SECOND);
    assert!(
        verdict.quiet_timeout,
        "the hold at 12 s is past the credit after the acknowledgement at 1 s"
    );
}

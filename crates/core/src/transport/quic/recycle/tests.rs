//! Tests for [`super::AckWatch`], [`super::CappedCubic`] and the recycle
//! constants. The guards on a live path are tested in `path::tests`.

use super::*;

const SEC: u64 = 1_000_000_000;

/// A reading of [`CapHolds`] with no cap.
const NO_CAP: CapHoldReading = CapHoldReading {
    holds: 0,
    held_now: false,
    validity: Duration::ZERO,
    longest_wait: Duration::ZERO,
};

/// The ticks of the window (`window / tick`), and the tick a window
/// started at tick `start` closes at.
fn closes_at(start: u64) -> u64 {
    start + ACK_WATCH_WINDOW.as_secs() / ACK_WATCH_TICK.as_secs()
}

/// The tick at which the dark-path rule closes a spell that started at
/// tick `start`, with a send every tick and no round trip known: the window
/// ends at [`closes_at`], the first send seen after it is at the next tick,
/// and its grace (one tick, the least) ends a tick later.
fn dark_closes_at(start: u64) -> u64 {
    closes_at(start) + 2
}

/// What one sample reads, in the units `AckSample::synthetic` takes.
#[derive(Clone, Copy)]
struct Reading {
    acked: u64,
    /// Batches of acknowledgements; [`reading`] makes it `acked` (every
    /// `on_ack` comes in a batch), [`Reading::batches`] sets it.
    batches: u64,
    outstanding: u64,
    sent: u64,
    received: u64,
    /// Hold events of the egress cap and whether it holds a QUIC transmit
    /// now ([`Reading::cap_held`]); a hold stays valid for `cap_validity`
    /// with no new event (an hour unless a test sets it).
    cap_holds: u64,
    cap_held_now: bool,
    cap_validity: Duration,
    /// The credit watch's readings ([`Reading::credit`]).
    credit: Credit,
    /// quinn's smoothed round trip ([`Reading::rtt`]); zero if unknown.
    rtt: Duration,
}

/// What the credit watch reads ([`CreditWatch`]), all 0 unless a test sets
/// them.
#[derive(Clone, Copy, Default)]
struct Credit {
    acked_bytes: u64,
    congestion_limited_batches: u64,
    blocked: u64,
    stream_frames: u64,
    writers: usize,
}

const fn reading(acked: u64, outstanding: u64, sent: u64, received: u64) -> Reading {
    Reading {
        acked,
        batches: acked,
        cap_holds: 0,
        cap_held_now: false,
        cap_validity: Duration::from_secs(3600),
        credit: Credit {
            acked_bytes: 0,
            congestion_limited_batches: 0,
            blocked: 0,
            stream_frames: 0,
            writers: 0,
        },
        outstanding,
        sent,
        received,
        rtt: Duration::ZERO,
    }
}

impl Reading {
    /// The same reading with `batches` batches of acknowledgements.
    const fn batches(self, batches: u64) -> Self {
        Self { batches, ..self }
    }

    /// The same reading with the egress cap holding a QUIC transmit, held
    /// `holds` times so far.
    const fn cap_held(self, holds: u64) -> Self {
        Self {
            cap_holds: holds,
            cap_held_now: true,
            ..self
        }
    }

    /// The same reading with holds valid for `validity` with no new event.
    const fn cap_validity(self, validity: Duration) -> Self {
        Self {
            cap_validity: validity,
            ..self
        }
    }

    /// The same reading with `holds` hold events so far and none held now.
    const fn cap_events(self, holds: u64) -> Self {
        Self {
            cap_holds: holds,
            cap_held_now: false,
            ..self
        }
    }

    /// The same reading with these credit-watch readings.
    const fn credit(self, credit: Credit) -> Self {
        Self { credit, ..self }
    }

    /// The same reading with quinn's smoothed round trip at `rtt`.
    const fn rtt(self, rtt: Duration) -> Self {
        Self { rtt, ..self }
    }

    /// The same reading with no transmit held now.
    const fn released(self) -> Self {
        Self {
            cap_held_now: false,
            ..self
        }
    }
}

/// Feed a watch one sample per `ACK_WATCH_TICK`, from tick 1 to `ticks`
/// (tick 0 is the first sample); the first tick at which it said close.
fn run(ticks: u64, at: impl FnMut(u64) -> Reading) -> Option<u64> {
    run_why(ticks, at).map(|(tick, _)| tick)
}

/// [`run`], with why the watch said close.
fn run_why(ticks: u64, mut at: impl FnMut(u64) -> Reading) -> Option<(u64, Stall)> {
    let record = Arc::default();
    let sample = |r: Reading| {
        AckSample::synthetic(
            &record,
            r.acked,
            r.batches,
            r.outstanding,
            r.sent,
            r.received,
            CapHoldReading {
                holds: r.cap_holds,
                held_now: r.cap_held_now,
                validity: r.cap_validity,
                longest_wait: Duration::ZERO,
            },
        )
        .with_credit(
            r.credit.acked_bytes,
            r.credit.congestion_limited_batches,
            r.credit.blocked,
            r.credit.stream_frames,
            r.credit.writers,
        )
        .with_rtt(r.rtt)
    };
    let mut watch = AckWatch::new(ACK_WATCH_WINDOW, sample(at(0)));
    (1..=ticks).find_map(|tick| {
        watch
            .observe(tick * ACK_WATCH_TICK.as_secs() * SEC, sample(at(tick)))
            .then(|| (tick, watch.stall().expect("a reason")))
    })
}

/// [`run_why`] on a watch observing for `quic_dark_episode_<label>`, on a path
/// that carries data or not; also the dark-episode counts once the watch is
/// dropped (`DARK_EPISODE_LABELS` order).
fn run_observed(
    ticks: u64,
    carries_data: bool,
    mut at: impl FnMut(u64) -> Reading,
) -> (Option<(u64, Stall)>, Vec<u64>) {
    let counters: &'static crate::transport::metrics::QuicDataCounters =
        Box::leak(Box::new(crate::transport::metrics::QuicDataCounters::new()));
    let record = Arc::default();
    let sample = |r: Reading| {
        AckSample::synthetic(
            &record,
            r.acked,
            r.batches,
            r.outstanding,
            r.sent,
            r.received,
            CapHoldReading {
                holds: r.cap_holds,
                held_now: r.cap_held_now,
                validity: r.cap_validity,
                longest_wait: Duration::ZERO,
            },
        )
    };
    let flag = Arc::new(std::sync::atomic::AtomicBool::new(carries_data));
    let mut watch = AckWatch::new(ACK_WATCH_WINDOW, sample(at(0))).observing(flag, counters);
    let closed = (1..=ticks).find_map(|tick| {
        watch
            .observe(tick * ACK_WATCH_TICK.as_secs() * SEC, sample(at(tick)))
            .then(|| (tick, watch.stall().expect("a reason")))
    });
    drop(watch);
    (closed, counters.snapshot(true).dark_episodes)
}

/// A dark spell of 30 s (sends from tick 1, the peer heard from again at tick
/// 16) on a path that carries no data is not closed and is counted to its end:
/// 30 s, in `recovered_lt_120s`. On a path that carries data the dark-path rule
/// closes it at the window, counted `closed_at_window`.
#[test]
fn a_path_without_data_counts_a_dark_spell_to_its_end() {
    let spell = |t: u64| reading(5, 1200, 10 + t.min(15), 100 + t.saturating_sub(15));
    let (closed, counts) = run_observed(40, false, spell);
    assert_eq!(closed, None, "a path that carries no data is not closed");
    assert_eq!(counts, vec![0, 0, 0, 1, 0, 0, 0, 0]);
    let (closed, counts) = run_observed(40, true, spell);
    assert_eq!(closed, Some((dark_closes_at(1), Stall::Dark)));
    assert_eq!(counts, vec![0, 0, 0, 0, 0, 0, 0, 1]);
}

/// Short spells land in the short buckets (4 s and 8 s, read at the ticks),
/// and one still going when the watch ends is `unrecovered`.
#[test]
fn dark_spells_are_counted_by_how_long_they_lasted() {
    // Sends at ticks 1-2, heard from again at tick 3: 4 s.
    let (_, counts) = run_observed(10, false, |t| {
        reading(5, 1200, 10 + t.min(2), 100 + t.saturating_sub(2))
    });
    assert_eq!(counts, vec![1, 0, 0, 0, 0, 0, 0, 0]);
    // Sends at ticks 1-4, heard from again at tick 5: 8 s.
    let (_, counts) = run_observed(10, false, |t| {
        reading(5, 1200, 10 + t.min(4), 100 + t.saturating_sub(4))
    });
    assert_eq!(counts, vec![0, 1, 0, 0, 0, 0, 0, 0]);
    // Dark to the end of the watch.
    let (closed, counts) = run_observed(30, false, |t| reading(5, 1200, 10 + t, 100));
    assert_eq!(closed, None);
    assert_eq!(counts, vec![0, 0, 0, 0, 0, 1, 0, 0]);
}

/// A dark spell that the egress cap interrupts (it holds this side's sends
/// from tick 3) ends there, counted `cap_held`: whether the peer is dark can
/// no longer be told, and the episode is not dropped uncounted.
#[test]
fn a_dark_spell_ended_by_a_cap_hold_is_counted_cap_held() {
    let (closed, counts) = run_observed(10, false, |t| {
        let reading = reading(5, 1200, 10 + t, 100);
        if t >= 3 {
            reading.cap_held(t - 2)
        } else {
            reading
        }
    });
    assert_eq!(closed, None);
    assert_eq!(counts, vec![0, 0, 0, 0, 0, 0, 1, 0]);
}

/// Feed a watch the readings `at(i)` taken at `times[i]` seconds; the first
/// index at which it said close, and why.
fn run_at(times: &[u64], mut at: impl FnMut(usize) -> Reading) -> Option<(usize, Stall)> {
    let record = Arc::default();
    let sample = |r: Reading| {
        AckSample::synthetic(
            &record,
            r.acked,
            r.batches,
            r.outstanding,
            r.sent,
            r.received,
            NO_CAP,
        )
    };
    let mut watch = AckWatch::new(ACK_WATCH_WINDOW, sample(at(0)));
    (1..times.len()).find_map(|i| {
        watch
            .observe(times[i] * SEC, sample(at(i)))
            .then(|| (i, watch.stall().expect("a reason")))
    })
}

/// This side's runtime stalls for 30 s between two samples (a late tick, or
/// a suspended host) while a send is outstanding on a live path: the late
/// sample counts as one tick, so the path is not closed as dark, and the
/// peer's acknowledgement at the next sample ends the spell. A path that
/// stays dark after the stall still closes, a window of samples later.
#[test]
fn a_stall_of_this_sides_runtime_does_not_close_the_path() {
    // Samples every 2 s, then one 30 s late, then every 2 s again.
    let times: Vec<u64> = (0..=3)
        .map(|i| 2 * i)
        .chain((0..=20).map(|i| 36 + 2 * i))
        .collect();
    // A send at index 1, outstanding through the stall, acknowledged at the
    // sample after it (index 5).
    let recovered = run_at(&times, |i| {
        if i >= 5 {
            reading(6, 0, 11, 100 + i as u64)
        } else {
            reading(5, 1200, 10 + u64::from(i >= 1), 100)
        }
    });
    assert_eq!(recovered, None, "a stall of our own is not darkness");
    // Dark from index 1 to the end: the stall counts as one tick, so the
    // window ends five samples after index 1 and the close comes two samples
    // later ([`dark_closes_at`]), not at the first sample after the stall.
    let dark = run_at(&times, |i| reading(5, 1200, 10 + i as u64, 100));
    assert_eq!(dark, Some((8, Stall::Dark)));
}

/// A send at tick 1, never acknowledged, while the peer sends every tick:
/// closed exactly a window after tick 1, not a tick earlier.
#[test]
fn an_unacknowledged_send_while_the_peer_sends_closes_after_the_window() {
    let closed = run(100, |t| reading(5, 0, 10 + t.min(1), 100 + t));
    assert_eq!(closed, Some(closes_at(1)));
}

/// Fix for the review's finding 3: an acknowledgement at tick 3 that leaves
/// packets outstanding starts the window there, with nothing new sent after
/// it; the peer then acknowledges nothing more while it keeps sending.
/// Closed a window after tick 3.
#[test]
fn packets_left_outstanding_by_an_acknowledgement_close_after_the_window() {
    let closed = run(100, |t| {
        let acked = if t >= 3 { 6 } else { 5 };
        reading(acked, 1200, 10, 100 + t)
    });
    assert_eq!(closed, Some(closes_at(3)));
}

/// quinn's probe backoff on an 800 ms path whose peer only acknowledges:
/// after the last acknowledgement this side sends only at its probes (here
/// at 2, 6 and 14 s: ticks 1, 3 and 7), and nothing comes back until a probe
/// is answered. Whether the first two probes met an outage that ended at
/// 6.5 s or were lost to a burst of tail losses with no outage at all, the
/// path is dark past the window (tick 6), and the probe at 14 s, the first
/// sent after the window, is answered at 16 s, inside its grace (three round
/// trips, 2.4 s): not closed. Unanswered, it closes at the first sample past
/// its grace (18 s, tick 9).
#[test]
fn a_dark_spell_answered_at_the_first_probe_after_the_window_is_not_closed() {
    let rtt = Duration::from_millis(800);
    let sent = |t: u64| 10 + u64::from(t >= 1) + u64::from(t >= 3) + u64::from(t >= 7);
    let watch = |answered: bool| {
        let record = Arc::default();
        let sample = |t: u64| {
            let (acked, outstanding, received) = if answered && t >= 8 {
                (6, 0, 101)
            } else {
                (5, 1200, 100)
            };
            AckSample::synthetic(
                &record,
                acked,
                acked,
                outstanding,
                sent(t),
                received,
                NO_CAP,
            )
            .with_rtt(rtt)
        };
        let mut watch = AckWatch::new(ACK_WATCH_WINDOW, sample(0));
        (1..=40u64).find_map(|t| {
            watch
                .observe(t * ACK_WATCH_TICK.as_secs() * SEC, sample(t))
                .then(|| (t, watch.stall().expect("a reason")))
        })
    };
    assert_eq!(watch(true), None, "answered within its grace");
    assert_eq!(watch(false), Some((9, Stall::Dark)));
}

/// An acknowledgement that leaves nothing outstanding, with nothing sent
/// after it: no window, however long the peer sends.
#[test]
fn an_acknowledgement_of_everything_starts_no_window() {
    let closed = run(100, |t| {
        let acked = if t >= 3 { 6 } else { 5 };
        reading(acked, 0, 10 + t.min(2), 100 + t)
    });
    assert_eq!(closed, None);
}

/// The peer silent throughout while this side keeps sending and nothing is
/// acknowledged: a dark path, closed once a send made after the window has
/// gone unanswered for its grace ([`dark_closes_at`]), without waiting for
/// quinn's idle timeout.
#[test]
fn a_silent_peer_with_sends_outstanding_is_a_dark_path() {
    assert_eq!(
        run_why(100, |t| reading(5, 1200, 10 + t, 100)),
        Some((dark_closes_at(1), Stall::Dark))
    );
}

/// One sample with nothing from the peer inside the window does not end it
/// (its packets may arrive with gaps longer than a tick); two in a row do,
/// and the next send then starts a new window.
#[test]
fn two_samples_in_a_row_with_nothing_from_the_peer_end_the_window() {
    // Sends every tick; nothing from the peer at tick 4 only.
    let closed = run(100, |t| {
        let received = if t >= 4 { 99 + t } else { 100 + t };
        reading(5, 0, 10 + t, received)
    });
    assert_eq!(closed, Some(closes_at(1)));
    // Nothing from the peer at ticks 4 and 5: the window ends at 5, and
    // the send at tick 6 starts the next one. The dark-path window those two
    // silent samples start ends when the peer is heard again.
    let closed = run_why(100, |t| {
        let received = match t {
            0..=3 => 100 + t,
            4 | 5 => 103,
            _ => 98 + t,
        };
        reading(5, 0, 10 + t, received)
    });
    assert_eq!(closed, Some((closes_at(6), Stall::NotAcknowledging)));
}

/// Review finding (second round): a window started by an acknowledgement
/// that left bytes outstanding ends when quinn later declares those bytes
/// lost, with nothing sent since. Without that, a node that mostly receives
/// and whose last ack-eliciting packet is lost would be closed while the
/// peer keeps sending.
#[test]
fn a_window_ends_when_its_outstanding_bytes_are_declared_lost() {
    let closed = run(100, |t| {
        let acked = if t >= 3 { 6 } else { 5 };
        let outstanding = if t >= 5 { 0 } else { 1200 };
        reading(acked, outstanding, 10, 100 + t)
    });
    assert_eq!(closed, None);
}

/// A send made after the latest batch of acknowledgements stays
/// outstanding while quinn's loss timer declares the bytes left from that
/// batch lost, with no new batch (the timer declares loss only below the
/// largest acknowledged packet, so not the send): the window goes on and
/// closes.
#[test]
fn a_send_after_the_latest_acknowledgement_keeps_the_window_open() {
    let closed = run(100, |t| {
        let outstanding = if t >= 4 { 0 } else { 1200 };
        reading(5, outstanding, 10 + t.min(1), 100 + t)
    });
    assert_eq!(closed, Some(closes_at(1)));
}

/// Review finding (third round): a batch of acknowledgements with no
/// `on_ack` (the peer acknowledged only this side's acknowledgement-only
/// packets) runs quinn's loss detection, which may declare the latched send
/// lost. Such a batch clears the latch, and with nothing left in flight the
/// window ends: no close while the peer keeps sending.
#[test]
fn a_batch_without_new_acknowledgements_clears_a_latched_send() {
    let closed = run(100, |t| {
        // A send at tick 1, never acknowledged; at tick 3 a batch that
        // acknowledges only acknowledgement-only packets and leaves nothing
        // in flight.
        let batches = if t >= 3 { 6 } else { 5 };
        reading(5, 0, 10 + t.min(1), 100 + t).batches(batches)
    });
    assert_eq!(closed, None);
}

/// Review finding (fourth round): quinn counts a transmit's frames when it
/// builds it, and a saturated egress cap can hold it longer than the
/// window. A send at tick 1 whose transmit the cap holds from tick 1 to
/// tick 9, while the peer sends every tick and acknowledges nothing (its
/// acknowledgement cannot come before the send leaves): no close while
/// held, and the latch is gone afterwards.
#[test]
fn a_send_held_by_the_egress_cap_starts_no_window() {
    let closed = run(40, |t| {
        let r = reading(5, 0, 10 + t.min(1), 100 + t).cap_held(1);
        if (1..=9).contains(&t) {
            r
        } else {
            r.released()
        }
    });
    assert_eq!(closed, None);
}

/// A hold that starts and ends between two samples is seen through the
/// hold events (`holds` goes from 0 to 1 while `held_now` stays false): the
/// send latched at tick 1 may be the transmit that waited, so the window
/// and the latch end, and with nothing sent since there is no close.
#[test]
fn a_hold_between_two_samples_ends_the_window() {
    let closed = run(40, |t| {
        let r = reading(5, 0, 10 + t.min(1), 100 + t);
        if t >= 3 { r.cap_events(1) } else { r }
    });
    assert_eq!(closed, None);
}

/// A hold reported with no new event for longer than its validity was left
/// by a connection that ended (its poller is never polled again), and stops
/// pausing the watch: a send after that starts a window that closes.
#[test]
fn a_hold_past_its_validity_no_longer_pauses_the_watch() {
    let closed = run(40, |t| {
        // Held from the start with one event, valid for 4 s; first seen at
        // tick 1 (2 s), so live through tick 3 (6 s) and stale from tick 4.
        // Sends from tick 5 on, the peer sending every tick.
        reading(5, 0, 10 + t.saturating_sub(4), 100 + t)
            .cap_held(1)
            .cap_validity(Duration::from_secs(4))
    });
    assert_eq!(closed, Some(closes_at(5)));
}

/// An outage in both directions longer than the window, with this side's
/// probes unacknowledged, is a dark path: closed a window after the first
/// probe. One shorter than the window is not: after it, packets still
/// outstanding but nothing new sent and nothing acknowledged (the probe
/// timer is backed off), while the peer sends every tick, start no window,
/// and the probe at tick 30 is acknowledged at tick 31.
#[test]
fn only_an_outage_longer_than_the_window_closes() {
    let long = run_why(200, |t| reading(5, 1200, 10 + t.min(15), 100));
    assert_eq!(long, Some((dark_closes_at(1), Stall::Dark)));
    let closed = run(200, |t| match t {
        // The outage, shorter than the window: probes, nothing from the
        // peer.
        0..=3 => reading(5, 1200, 10 + t, 100),
        // Recovered: the peer sends every tick, nothing of ours moves.
        4..=29 => reading(5, 1200, 13, 100 + t),
        // The backed-off probe, then its acknowledgement.
        30 => reading(5, 1200, 14, 100 + t),
        _ => reading(6, 0, 14, 100 + t),
    });
    assert_eq!(closed, None);
}

/// An acknowledgement in the sample the close would have come at prevents
/// it. A peer that acknowledges something within every window is never
/// closed: the guard is a conformance check, not a liveness check.
#[test]
fn an_acknowledgement_at_the_closing_tick_prevents_the_close() {
    // The acknowledgement leaves packets outstanding, so it is the
    // acknowledgement itself, not an empty flight, that holds the close off.
    let at_close = closes_at(1);
    let closed = run(at_close, |t| {
        let (acked, outstanding) = if t >= at_close { (6, 1200) } else { (5, 0) };
        reading(acked, outstanding, 10 + t, 100 + t)
    });
    assert_eq!(closed, None);
    // And a peer acknowledging once every window, sends throughout.
    let closed = run(1000, |t| reading(5 + t / closes_at(0), 0, 10 + t, 100 + t));
    assert_eq!(closed, None);
}

/// Nothing sent: idle, never closed, however much the peer sends.
#[test]
fn an_idle_path_is_never_closed() {
    assert_eq!(run(1000, |t| reading(5, 0, 10, 100 + t)), None);
}

/// A keepalive every 15 s, acknowledged one round trip (here up to 9 s)
/// later, with the peer sending every tick: never closed.
#[test]
fn a_slow_conforming_peer_is_never_closed() {
    for rtt in [1, 3, 9] {
        let closed = run(1000, |tick| {
            let t = tick * ACK_WATCH_TICK.as_secs();
            let pings = t / 15 + 1;
            let acked = t.saturating_sub(rtt) / 15 + u64::from(t >= rtt);
            let outstanding = if acked < pings { 50 } else { 0 };
            reading(acked, outstanding, pings, t)
        });
        assert_eq!(closed, None, "rtt {rtt} s");
    }
}

/// A busy conforming peer: sends and acknowledgements every tick, with
/// packets always outstanding: never closed.
#[test]
fn a_busy_conforming_peer_is_never_closed() {
    assert_eq!(run(1000, |t| reading(t * 7, 30_000, t * 9, t * 11)), None);
}

/// A new controller (a new record) ends the window, like an
/// acknowledgement with nothing known to be outstanding.
#[test]
fn a_new_controller_restarts_the_window() {
    let first: Arc<Mutex<AckState>> = Arc::default();
    let second: Arc<Mutex<AckState>> = Arc::default();
    let s = |record, sent, received| AckSample::synthetic(record, 0, 0, 0, sent, received, NO_CAP);
    let mut watch = AckWatch::new(ACK_WATCH_WINDOW, s(&first, 0, 0));
    assert!(!watch.observe(2 * SEC, s(&first, 1, 1)));
    assert!(!watch.observe(4 * SEC, s(&second, 2, 2)));
    assert!(!watch.observe(6 * SEC, s(&second, 3, 3)));
    for at in [8, 10, 12, 14] {
        assert!(!watch.observe(at * SEC, s(&second, at, at)));
    }
    assert!(watch.observe(16 * SEC, s(&second, 16, 16)));
}

/// Every frame kind except ACK and CONNECTION_CLOSE counts as
/// ack-eliciting; every kind counts as received.
#[test]
fn ack_and_close_frames_are_not_ack_eliciting() {
    let mut frames = FrameStats::default();
    frames.acks = 1000;
    frames.connection_close = 7;
    assert_eq!(ack_eliciting_frames(&frames), 0);
    assert_eq!(frames_received(&frames), 1007);
    frames.ping = 1;
    frames.stream = 2;
    frames.max_streams_uni = 3;
    frames.handshake_done = 4;
    assert_eq!(ack_eliciting_frames(&frames), 10);
    assert_eq!(frames_received(&frames), 1017);
}

/// The values the node runs with, written out, and the relation the window
/// logic needs (several samples per window).
#[test]
fn the_production_values_are_pinned() {
    assert_eq!(SEND_ABORT_LIMIT, 4096);
    assert_eq!(ACK_WATCH_WINDOW, Duration::from_secs(10));
    assert_eq!(ACK_WATCH_TICK, Duration::from_secs(2));
    assert!(ACK_WATCH_WINDOW > 2 * ACK_WATCH_TICK);
    assert_eq!(
        super::super::path::QuicPathLimits::DEFAULT.max_send_aborts,
        SEND_ABORT_LIMIT
    );
}

/// One call into a controller, with comparable arguments.
#[derive(Debug, Clone, PartialEq)]
enum Call {
    OnSent(Instant, u64, u64),
    OnAck(Instant, Instant, u64, bool, Duration, Duration),
    OnEndAcks(Instant, u64, bool, Option<u64>),
    OnCongestionEvent(Instant, Instant, bool, u64),
    OnMtuUpdate(u16),
    Window,
    Metrics,
    CloneBox,
    InitialWindow,
}

/// A controller that logs every call and forwards it to `inner`.
struct Recorder {
    log: Arc<Mutex<Vec<Call>>>,
    inner: Box<dyn Controller>,
}

impl Recorder {
    fn wrap(log: &Arc<Mutex<Vec<Call>>>, inner: Box<dyn Controller>) -> Box<dyn Controller> {
        Box::new(Self {
            log: log.clone(),
            inner,
        })
    }

    fn push(&self, call: Call) {
        self.log.lock().push(call);
    }
}

impl Controller for Recorder {
    fn on_sent(&mut self, now: Instant, bytes: u64, last_packet_number: u64) {
        self.push(Call::OnSent(now, bytes, last_packet_number));
        self.inner.on_sent(now, bytes, last_packet_number);
    }

    fn on_ack(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        app_limited: bool,
        rtt: &quinn_proto::RttEstimator,
    ) {
        self.push(Call::OnAck(
            now,
            sent,
            bytes,
            app_limited,
            rtt.get(),
            rtt.min(),
        ));
        self.inner.on_ack(now, sent, bytes, app_limited, rtt);
    }

    fn on_end_acks(
        &mut self,
        now: Instant,
        in_flight: u64,
        app_limited: bool,
        largest_packet_num_acked: Option<u64>,
    ) {
        self.push(Call::OnEndAcks(
            now,
            in_flight,
            app_limited,
            largest_packet_num_acked,
        ));
        self.inner
            .on_end_acks(now, in_flight, app_limited, largest_packet_num_acked);
    }

    fn on_congestion_event(
        &mut self,
        now: Instant,
        sent: Instant,
        is_persistent_congestion: bool,
        lost_bytes: u64,
    ) {
        self.push(Call::OnCongestionEvent(
            now,
            sent,
            is_persistent_congestion,
            lost_bytes,
        ));
        self.inner
            .on_congestion_event(now, sent, is_persistent_congestion, lost_bytes);
    }

    fn on_mtu_update(&mut self, new_mtu: u16) {
        self.push(Call::OnMtuUpdate(new_mtu));
        self.inner.on_mtu_update(new_mtu);
    }

    fn window(&self) -> u64 {
        self.push(Call::Window);
        self.inner.window()
    }

    fn metrics(&self) -> ControllerMetrics {
        self.push(Call::Metrics);
        self.inner.metrics()
    }

    fn clone_box(&self) -> Box<dyn Controller> {
        self.push(Call::CloneBox);
        Box::new(Self {
            log: self.log.clone(),
            inner: self.inner.clone_box(),
        })
    }

    fn initial_window(&self) -> u64 {
        self.push(Call::InitialWindow);
        self.inner.initial_window()
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

/// `outer` (a [`Recorder`] around an [`CappedCubic`] around a [`Recorder`]
/// around Cubic) and its two logs, and the `CappedCubic`'s record.
struct Stack {
    outer: Box<dyn Controller>,
    outer_log: Arc<Mutex<Vec<Call>>>,
    inner_log: Arc<Mutex<Vec<Call>>>,
}

impl Stack {
    fn new(now: Instant) -> Self {
        let outer_log = Arc::default();
        let inner_log = Arc::default();
        let cubic = Arc::new(quinn::congestion::CubicConfig::default()).build(now, 1200);
        let counting = Box::new(CappedCubic::new(
            Recorder::wrap(&inner_log, cubic),
            QuicRateLimit::default(),
            1200,
        ));
        Self {
            outer: Recorder::wrap(&outer_log, counting),
            outer_log,
            inner_log,
        }
    }

    /// Every call reached `inner` exactly as it reached the `CappedCubic`.
    /// `clone_box` is not compared: the `CappedCubic` clones its inner too.
    fn assert_forwarded(&self) {
        let calls = |log: &Arc<Mutex<Vec<Call>>>| -> Vec<Call> {
            log.lock()
                .iter()
                .filter(|call| **call != Call::CloneBox)
                .cloned()
                .collect()
        };
        let outer = calls(&self.outer_log);
        assert!(!outer.is_empty());
        assert_eq!(outer, calls(&self.inner_log));
    }
}

/// The `CappedCubic` record inside a controller built by [`Stack`].
fn record_of(outer: &dyn Controller) -> Arc<Mutex<AckState>> {
    let recorder = outer
        .clone_box()
        .into_any()
        .downcast::<Recorder>()
        .expect("the outer recorder");
    recorder
        .inner
        .into_any()
        .downcast::<CappedCubic>()
        .expect("the CappedCubic")
        .state
}

/// Every method an `CappedCubic` gets is forwarded to its inner controller
/// once, with the same arguments, and returns what the inner returns; a
/// clone shares the record. (`on_ack` needs quinn's `RttEstimator`, which
/// only quinn builds: see the next test.)
#[test]
fn ack_counting_forwards_every_call_unchanged() {
    let now = Instant::now();
    let later = now + Duration::from_millis(30);
    let mut stack = Stack::new(now);
    let c = &mut stack.outer;
    c.on_sent(now, 1200, 7);
    c.on_end_acks(later, 2400, true, Some(7));
    c.on_congestion_event(later, now, false, 1200);
    c.on_mtu_update(1200);
    let window = c.window();
    let metrics = c.metrics();
    let initial = c.initial_window();
    // The stack here wraps quinn's default Cubic, not the node's factory, so
    // this checks forwarding, not the initial window the node uses.
    let reference = Arc::new(quinn::congestion::CubicConfig::default()).build(now, 1200);
    assert_eq!(initial, reference.initial_window());
    assert!(window > 0 && metrics.congestion_window == window);
    stack.assert_forwarded();

    let record = record_of(stack.outer.as_ref());
    assert_eq!(
        *record.lock(),
        AckState {
            acked: 0,
            batches: 1,
            in_flight_after_acks: 2400,
            lost_since_acks: 1200,
            ..AckState::default()
        }
    );
    assert_eq!(record.lock().outstanding_after_acks(), 1200);
    let clone = stack.outer.clone_box();
    assert!(Arc::ptr_eq(&record, &record_of(clone.as_ref())));
    // A new batch of acknowledgements resets the losses counted since.
    stack.outer.on_end_acks(later, 0, false, Some(8));
    assert_eq!(record.lock().outstanding_after_acks(), 0);
}

/// On a live quinn-proto connection, every call quinn makes reaches the
/// inner controller unchanged, `on_ack` included, and the record counts
/// each `on_ack` once.
#[test]
fn ack_counting_forwards_quinns_calls_and_counts_each_ack() {
    use super::super::memory_tests::{connected_pair_with, transfer_one_stream};

    struct StackFactory {
        outer_log: Arc<Mutex<Vec<Call>>>,
        inner_log: Arc<Mutex<Vec<Call>>>,
    }
    impl ControllerFactory for StackFactory {
        fn build(self: Arc<Self>, now: Instant, mtu: u16) -> Box<dyn Controller> {
            let mut cubic = quinn::congestion::CubicConfig::default();
            cubic.initial_window(1 << 30);
            let counting = Box::new(CappedCubic::new(
                Recorder::wrap(&self.inner_log, Arc::new(cubic).build(now, mtu)),
                QuicRateLimit::default(),
                mtu,
            ));
            Recorder::wrap(&self.outer_log, counting)
        }
    }
    let outer_log = Arc::default();
    let inner_log = Arc::default();
    let factory = Arc::new(StackFactory {
        outer_log: Arc::clone(&outer_log),
        inner_log: Arc::clone(&inner_log),
    });
    let (mut receiver, mut sender, now) = connected_pair_with(factory);
    let now = transfer_one_stream(&mut receiver, &mut sender, now, 64 * 1024);
    // A transfer back, so the stack also handles batches while receiving.
    transfer_one_stream(&mut sender, &mut receiver, now, 64 * 1024);
    let stack = Stack {
        outer: sender.conn().congestion_state().clone_box(),
        outer_log,
        inner_log,
    };
    stack.assert_forwarded();
    let acks = stack
        .outer_log
        .lock()
        .iter()
        .filter(|call| matches!(call, Call::OnAck(..)))
        .count();
    assert!(acks > 0, "the transfer was acknowledged");
    let record = *record_of(stack.outer.as_ref()).lock();
    assert_eq!(record.acked, acks as u64);
    // Every batch is counted. (A batch with no `on_ack`, which acknowledges
    // only acknowledgement-only packets, is not produced on demand here:
    // each packet this side sends while receiving also carries a
    // flow-control frame. The latch rule for it is pinned by
    // `a_batch_without_new_acknowledgements_clears_a_latched_send`.)
    let log = stack.outer_log.lock().clone();
    let batches = log
        .iter()
        .filter(|call| matches!(call, Call::OnEndAcks(..)))
        .count();
    assert_eq!(record.batches, batches as u64);
}

/// The names of the `fn`s declared directly in the `{ ... }` block that
/// starts at the first `{` after `start` in `source`.
fn fn_names_in_block(source: &str, start: &str) -> Vec<String> {
    let at = source
        .find(start)
        .unwrap_or_else(|| panic!("`{start}` not found"));
    let open = at + source[at..].find('{').expect("a block");
    let mut depth = 0usize;
    let mut names = Vec::new();
    let body = &source[open..];
    let mut end = None;
    for (i, c) in body.char_indices() {
        match c {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    end = Some(i);
                    break;
                }
            }
            _ => {}
        }
        if depth == 1 && body[i..].starts_with("fn ") {
            let rest = &body[i + 3..];
            let name: String = rest
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            names.push(name);
        }
    }
    assert!(end.is_some(), "`{start}`: the block does not end");
    names.sort();
    names
}

/// The `congestion.rs` of the quinn-proto this crate links, or a reason it
/// was not found. Two ways, both offline, so a runner without network does
/// not wait:
/// 1. `cargo metadata --offline --filter-platform <host>`: freenet's own
///    resolved `quinn_proto` dependency. Without the filter, cargo needs the
///    crates of every platform, and a CI cache holds only the host's.
/// 2. The registry sources under `$CARGO_HOME` for the quinn-proto version
///    locked in `Cargo.lock`.
fn quinn_proto_congestion_rs() -> Result<std::path::PathBuf, String> {
    let manifest = concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml");
    let metadata_error = match quinn_proto_from_metadata(manifest) {
        Ok(source) => return Ok(source),
        Err(error) => error,
    };
    quinn_proto_from_registry().map_err(|registry_error| {
        format!("cargo metadata: {metadata_error}; registry: {registry_error}")
    })
}

/// [`quinn_proto_congestion_rs`], way 1.
fn quinn_proto_from_metadata(manifest: &str) -> Result<std::path::PathBuf, String> {
    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_string());
    let version = std::process::Command::new(&rustc)
        .arg("-vV")
        .output()
        .map_err(|error| format!("run {rustc} -vV: {error}"))?;
    let host = String::from_utf8_lossy(&version.stdout)
        .lines()
        .find_map(|line| line.strip_prefix("host: ").map(str::to_string))
        .ok_or("no host triple in rustc -vV")?;
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let output = std::process::Command::new(&cargo)
        .args([
            "metadata",
            "--format-version",
            "1",
            "--locked",
            "--offline",
            "--filter-platform",
            &host,
            "--manifest-path",
            manifest,
        ])
        .output()
        .map_err(|error| format!("run cargo metadata: {error}"))?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).into_owned());
    }
    let metadata: serde_json::Value =
        serde_json::from_slice(&output.stdout).map_err(|error| error.to_string())?;
    let packages = metadata["packages"].as_array().ok_or("no packages")?;
    let freenet = packages
        .iter()
        .find(|package| {
            package["name"] == "freenet"
                && package["manifest_path"].as_str().is_some_and(|path| {
                    std::path::Path::new(path) == std::path::Path::new(manifest)
                })
        })
        .ok_or("this crate is not in cargo metadata")?;
    let node = metadata["resolve"]["nodes"]
        .as_array()
        .ok_or("no resolve graph")?
        .iter()
        .find(|node| node["id"] == freenet["id"])
        .ok_or("this crate is not in the resolve graph")?;
    let quinn_proto = node["deps"]
        .as_array()
        .ok_or("no deps")?
        .iter()
        .find(|dep| dep["name"] == "quinn_proto")
        .ok_or("quinn_proto is not among this crate's dependencies")?["pkg"]
        .clone();
    let manifest_path = packages
        .iter()
        .find(|package| package["id"] == quinn_proto)
        .and_then(|package| package["manifest_path"].as_str())
        .ok_or("no manifest path for quinn-proto")?;
    let source = std::path::Path::new(manifest_path)
        .parent()
        .ok_or("quinn-proto's manifest has no directory")?
        .join("src/congestion.rs");
    if source.is_file() {
        Ok(source)
    } else {
        Err(format!("{} is not on disk", source.display()))
    }
}

/// [`quinn_proto_congestion_rs`], way 2.
fn quinn_proto_from_registry() -> Result<std::path::PathBuf, String> {
    let lock = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../../Cargo.lock"))
        .map_err(|error| format!("read Cargo.lock: {error}"))?;
    let marker = "name = \"quinn-proto\"\nversion = \"";
    let mut versions = lock.match_indices(marker).map(|(at, _)| {
        let rest = &lock[at + marker.len()..];
        rest[..rest.find('"').unwrap_or(0)].to_string()
    });
    let version = versions.next().ok_or("quinn-proto is not in Cargo.lock")?;
    if versions.next().is_some() {
        return Err("Cargo.lock holds more than one quinn-proto".to_string());
    }
    let cargo_home = std::env::var("CARGO_HOME")
        .or_else(|_| std::env::var("HOME").map(|home| format!("{home}/.cargo")))
        .map_err(|_| "neither CARGO_HOME nor HOME is set".to_string())?;
    let registry = std::path::Path::new(&cargo_home).join("registry/src");
    std::fs::read_dir(&registry)
        .map_err(|error| format!("{}: {error}", registry.display()))?
        .filter_map(Result::ok)
        .map(|index| {
            index
                .path()
                .join(format!("quinn-proto-{version}/src/congestion.rs"))
        })
        .find(|path| path.is_file())
        .ok_or_else(|| format!("quinn-proto {version} is not under {}", registry.display()))
}

/// Pin: `impl Controller for CappedCubic` implements every method of the
/// linked quinn-proto's `Controller` trait. A method quinn adds with a
/// default body would otherwise run quinn's default instead of reaching
/// Cubic (and, for an acknowledgement method, the record), and nothing else
/// would fail. Fails if the quinn-proto source cannot be found.
#[test]
fn ack_counting_implements_every_controller_method() {
    let source = quinn_proto_congestion_rs().unwrap_or_else(|error| panic!("{error}"));
    let quinn = std::fs::read_to_string(&source).expect("read congestion.rs");
    let trait_methods = fn_names_in_block(&quinn, "pub trait Controller");
    let ours = fn_names_in_block(
        include_str!("../recycle.rs"),
        "impl Controller for CappedCubic",
    );
    assert!(trait_methods.len() >= 10, "{trait_methods:?}");
    assert_eq!(ours, trait_methods);
}

/// Each report is the change since the last one, sent and received kept apart,
/// and the next is taken from the state it hands back; a count that went down
/// reports 0.
#[test]
fn udp_bytes_report_the_change_since_the_last_report() {
    let at = |sent, received| UdpBytes { sent, received };
    let start = at(10, 20);
    let (next, sent, received) = start.step(at(15, 50));
    assert_eq!((sent, received), (5, 30));
    assert_eq!(next, at(15, 50));
    let (next, sent, received) = next.step(at(115, 51));
    assert_eq!(
        (sent, received),
        (100, 1),
        "from the last report, not the start"
    );
    let (_, sent, received) = next.step(at(100, 51));
    assert_eq!((sent, received), (0, 0));
}

/// Each loss report is the change since the last one, field by field, and the
/// next is taken from the state it hands back; a count that went down reports 0.
#[test]
fn path_loss_reports_the_change_since_the_last_report() {
    let at = |sent, lost, bytes, events| PathLoss {
        sent_packets: sent,
        lost_packets: lost,
        lost_bytes: bytes,
        congestion_events: events,
    };
    let (next, delta) = at(100, 2, 2400, 1).step(at(350, 5, 6000, 3));
    assert_eq!(delta, at(250, 3, 3600, 2));
    assert_eq!(next, at(350, 5, 6000, 3));
    let (_, delta) = next.step(at(360, 4, 6000, 3));
    assert_eq!(
        delta,
        at(10, 0, 0, 0),
        "from the last report, not the start"
    );
}

/// A busy conforming peer's reading at tick `t` (acknowledgements, sends and
/// frames from the peer every tick), with credit-watch readings `credit`: the
/// other rules never close it, so only the credit rule can.
fn busy(t: u64, credit: Credit) -> Reading {
    reading(t * 7, 30_000, t * 9, t * 11).credit(credit)
}

/// Bytes the floor rate gives in one tick.
const FLOOR_PER_TICK: u64 = FLOOR_RATE_BYTES_PER_SEC * 2;

/// A send blocked by the peer's credit, with nothing more streamed and
/// almost nothing acknowledged: closed as soon as it has waited
/// `CREDIT_LIMITED_MIN`, two intervals after the blocked frame was first
/// seen (that first interval is not counted: it began at the first sample,
/// whose time is unknown). A credit raise for another stream (MAX_STREAM_DATA the blocked
/// send cannot use) sends no STREAM frame, so it changes nothing here.
#[test]
fn a_send_starved_of_credit_closes() {
    let closed = run_why(100, |t| {
        busy(
            t,
            Credit {
                acked_bytes: 100 * t,
                blocked: u64::from(t >= 1),
                stream_frames: 50,
                writers: 1,
                ..Credit::default()
            },
        )
    });
    assert_eq!(closed, Some((3, Stall::CreditStarved)));
}

/// A peer that raises its credit in tiny steps: each step lets the blocked
/// send write a little (STREAM frames) and block again at the new limit (a
/// new blocked frame), so every interval still counts, and the bytes
/// acknowledged stay under the floor rate: closed.
#[test]
fn credit_raised_in_tiny_steps_still_closes() {
    let closed = run_why(100, |t| {
        busy(
            t,
            Credit {
                acked_bytes: 4 * 1024 * t,
                blocked: t,
                stream_frames: 50 + 3 * t,
                writers: 1,
                ..Credit::default()
            },
        )
    });
    assert_eq!(closed, Some((3, Stall::CreditStarved)));
}

/// Blocked frames every tick, but every interval also has a batch of
/// acknowledgements while quinn had data its congestion window held back: a
/// path limited by its congestion window, a shared uplink (40 busy
/// connections on 1 MB/s, about 25 KiB/s each, under the floor rate) or a
/// backlog is not waiting on the peer, and is never closed.
#[test]
fn a_congestion_limited_path_under_the_floor_rate_is_never_closed() {
    let closed = run(1000, |t| {
        busy(
            t,
            Credit {
                acked_bytes: 50 * 1024 * t,
                congestion_limited_batches: t,
                blocked: t,
                stream_frames: 50 + 40 * t,
                writers: 3,
            },
        )
    });
    assert_eq!(closed, None);
}

/// Waiting on the peer's credit every interval, with the acknowledged rate at
/// the floor rate or above: never closed. Just under it: closed.
#[test]
fn the_floor_rate_is_the_bound() {
    let at = |per_tick: u64| {
        run(1000, move |t| {
            busy(
                t,
                Credit {
                    acked_bytes: per_tick * t,
                    blocked: t,
                    stream_frames: 50 + 40 * t,
                    writers: 1,
                    ..Credit::default()
                },
            )
        })
    };
    assert_eq!(at(FLOOR_PER_TICK), None);
    assert_eq!(at(2 * FLOOR_PER_TICK), None);
    assert_eq!(at(FLOOR_PER_TICK - 1), Some(3));
}

/// A blocked frame from a send that has since ended (nothing writing) does
/// not keep the path counted as waiting: never closed, however idle.
#[test]
fn a_blocked_frame_from_an_ended_send_does_not_count() {
    let closed = run(1000, |t| {
        busy(
            t,
            Credit {
                blocked: 1,
                stream_frames: 50,
                writers: usize::from(t <= 1),
                ..Credit::default()
            },
        )
    });
    assert_eq!(closed, None);
}

/// A send blocked once, then given credit it uses (STREAM frames, no new
/// blocked frame), then waiting on a slow source with little acknowledged:
/// the wait is not the peer's, never closed.
#[test]
fn a_send_waiting_on_its_source_after_a_credit_raise_is_not_starved() {
    let closed = run(1000, |t| {
        busy(
            t,
            Credit {
                acked_bytes: 100 * t,
                blocked: u64::from(t >= 1),
                stream_frames: 50 + u64::from(t >= 2),
                writers: 1,
                ..Credit::default()
            },
        )
    });
    assert_eq!(closed, None);
}

/// While the egress cap holds this connection's transmits, waiting is this
/// side's back-pressure: never closed by the credit rule.
#[test]
fn a_send_held_by_the_egress_cap_is_not_starved() {
    let closed = run(100, |t| {
        busy(
            t,
            Credit {
                blocked: t,
                stream_frames: 50,
                writers: 1,
                ..Credit::default()
            },
        )
        .cap_held(t)
    });
    assert_eq!(closed, None);
}

/// A send blocked by the peer's credit on a path delivering at twice the
/// floor rate, through an outage in both directions of 3 s or 6 s (no
/// acknowledgement and nothing acknowledged meanwhile, a loss spell the
/// dark rule tolerates): the intervals with no acknowledgement say nothing
/// about credit and are not counted, so the path is not closed as
/// credit-starved. Counting them would have closed it.
#[test]
fn a_credit_blocked_send_through_a_short_outage_is_not_starved() {
    for outage_ticks in [1u64, 3] {
        // Delivery stops after tick 4 for `outage_ticks` ticks.
        let delivered = |t: u64| {
            let live = if t <= 4 {
                t
            } else if t <= 4 + outage_ticks {
                4
            } else {
                t - outage_ticks
            };
            2 * FLOOR_PER_TICK * live
        };
        let closed = run(40, |t| {
            let live = t <= 4 || t > 4 + outage_ticks;
            let acks = if live { t * 7 } else { 28 };
            reading(acks, 30_000, t * 9, if live { t * 11 } else { 44 }).credit(Credit {
                acked_bytes: delivered(t),
                blocked: t,
                stream_frames: 50 + t,
                writers: 1,
                ..Credit::default()
            })
        });
        assert_eq!(closed, None, "a {} s outage", 2 * outage_ticks);
    }
}

/// The floor follows the round trip: at a 3 s round trip an honest receiver
/// with the smallest window (64 KiB) returns 64 KiB per round trip, about
/// 21.8 KB/s, under the 32 KiB/s floor, and is not closed; the floor at that
/// round trip is 64 KiB / 3 s. A peer cannot lower it further by acknowledging
/// later: past `CREDIT_FLOOR_RTT_MAX` (4 s) the floor stays at 16 KiB/s, so a
/// path delivering at 12 KiB/s on a claimed 8 s round trip is closed.
#[test]
fn the_credit_floor_follows_the_round_trip_up_to_its_cap() {
    assert_eq!(credit_floor_rate(Duration::ZERO), FLOOR_RATE_BYTES_PER_SEC);
    assert_eq!(
        credit_floor_rate(Duration::from_secs(1)),
        FLOOR_RATE_BYTES_PER_SEC
    );
    assert_eq!(credit_floor_rate(Duration::from_secs(4)), 16 * 1024);
    assert_eq!(credit_floor_rate(Duration::from_secs(8)), 16 * 1024);
    let at = |rtt: Duration, rate: u64| {
        run_why(100, move |t| {
            busy(
                t,
                Credit {
                    acked_bytes: rate * 2 * t,
                    blocked: t,
                    stream_frames: 50 + t,
                    writers: 1,
                    ..Credit::default()
                },
            )
            .rtt(rtt)
        })
    };
    let far = 64 * 1024 / 3;
    assert_eq!(at(Duration::from_secs(3), far + 512), None);
    assert_eq!(
        at(Duration::from_secs(1), far + 512),
        Some((3, Stall::CreditStarved))
    );
    assert_eq!(
        at(Duration::from_secs(8), 12 * 1024),
        Some((3, Stall::CreditStarved))
    );
}

/// The egress cap holds for most of each window and releases for one
/// interval in five: only the released intervals can count, 2 s of every
/// 10 s, under `CREDIT_LIMITED_MIN`, so the send is never closed, though it
/// is blocked and almost nothing is acknowledged throughout. Counting the
/// held intervals too would close it at the first released sample.
#[test]
fn a_send_held_by_the_egress_cap_for_part_of_the_window_is_not_starved() {
    let held = |t: u64| t % 5 != 0;
    let closed = run(100, |t| {
        let reading = busy(
            t,
            Credit {
                acked_bytes: 100 * t,
                blocked: t,
                stream_frames: 50,
                writers: 1,
                ..Credit::default()
            },
        );
        // Hold events so far, unchanged at a released sample.
        let holds = (1..=t).filter(|&i| held(i)).count() as u64;
        if held(t) {
            reading.cap_held(holds)
        } else {
            Reading {
                cap_holds: holds,
                ..reading
            }
        }
    });
    assert_eq!(closed, None);
}

/// One interval with a block is under `CREDIT_LIMITED_MIN` (the block may
/// have come at its end): a send that blocks once, as an honest one may on
/// the connection's small first window, and then streams as credit comes,
/// with little acknowledged by then, is never closed.
#[test]
fn a_single_block_then_streaming_is_not_starved() {
    let closed = run(100, |t| {
        busy(
            t,
            Credit {
                acked_bytes: if t <= 2 { 0 } else { 2 * FLOOR_PER_TICK * t },
                blocked: u64::from(t >= 2),
                stream_frames: if t <= 2 { 50 } else { 50 + t },
                writers: 1,
                ..Credit::default()
            },
        )
    });
    assert_eq!(closed, None);
}

/// A peer that raises its credit in one interval and lets the send block in
/// the next, over and over, with little acknowledged: every other interval
/// counts, and it is closed once two have.
#[test]
fn credit_raised_every_other_interval_still_closes() {
    let closed = run_why(100, |t| {
        busy(
            t,
            Credit {
                acked_bytes: 1024 * t,
                blocked: t.div_ceil(2),
                stream_frames: 50 + t / 2,
                writers: 1,
                ..Credit::default()
            },
        )
    });
    assert!(
        matches!(closed, Some((tick, Stall::CreditStarved)) if tick <= 5),
        "{closed:?}"
    );
}

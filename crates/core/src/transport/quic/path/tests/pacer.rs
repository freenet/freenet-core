//! A path's data sends paced at the connection's bandwidth limit
//! (`path::pacer`).
//!
//! The simulated link has no bandwidth limit and a round trip far under the
//! 2.4 ms below which `CappedCubic`'s two-datagram floor stops a window cap
//! from binding at 1 MB/s, so a send's speed there is set by the pacer alone.

use std::sync::atomic::{AtomicUsize, Ordering};

use super::super::bounded::SendDeadlines;
use super::super::pacer::{LimitPacer, SendPacer};
use super::*;
use crate::transport::global_bandwidth::GlobalBandwidthManager;
use crate::transport::quic::config::QuicRateLimit;
use crate::transport::quic::control::QuicControl;

/// A connection's pacer at a `bandwidth-limit` of `rate`, starting full.
fn pacer_at(time: &VirtualTime, rate: usize) -> Arc<LimitPacer<VirtualTime>> {
    Arc::new(
        LimitPacer::new(QuicRateLimit::new(Some(rate), None), time.clone()).expect("a limit paces"),
    )
}

/// How long `len` bytes take as one Stream from `a` to `b`, and the outcome.
async fn time_a_stream(p: &Paths, len: usize) -> (Duration, BoundedSend) {
    let (a, time) = (p.a.clone(), p.time());
    p.drive(async move {
        let started = time.now();
        let outcome = a
            .send_stream_bounded(
                &time,
                SendDeadlines::stream(len as u64),
                StreamId::next(),
                None,
                Bytes::from(payload(len)),
                None,
            )
            .await;
        (time.now() - started, outcome)
    })
    .await
}

/// The regression: with a limit of 100 kB/s, a 500 kB stream on a path with a
/// sub-millisecond round trip takes about 4.9 s (500 kB less the 10 kB burst),
/// where it took milliseconds before. The same path unpaced is the control.
#[test]
fn a_paced_path_keeps_to_the_limit_on_a_short_round_trip() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        const RATE: usize = 100_000;
        const LEN: usize = 500_000;
        let p = paths("quic-pacer-rate", QuicPathLimits::DEFAULT).await;
        let (unpaced, outcome) = time_a_stream(&p, LEN).await;
        assert!(matches!(outcome, BoundedSend::Sent), "{outcome:?}");
        assert!(
            unpaced < Duration::from_millis(500),
            "unpaced, the link is fast: {unpaced:?}"
        );
        p.a.pace_with(pacer_at(&p.time(), RATE));
        let (paced, outcome) = time_a_stream(&p, LEN).await;
        assert!(matches!(outcome, BoundedSend::Sent), "{outcome:?}");
        let floor = Duration::from_secs_f64((LEN - RATE / 10) as f64 / RATE as f64);
        assert!(
            paced >= floor && paced < floor + Duration::from_millis(500),
            "{LEN} bytes at {RATE} B/s took {paced:?}, expected about {floor:?}"
        );
    });
}

/// With the bucket deep in debt: a control frame and a Short of one packet go
/// at once, and a Stream is refused before its stream opens
/// (`NotOpened(TimedOut)`, so it may go on UDP), at once, taking nothing.
#[test]
fn control_frames_and_one_packet_shorts_do_not_wait_behind_data() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-pacer-control", QuicPathLimits::DEFAULT).await;
        // At the bucket's lowest rate, 1 KiB/s, a 120 kB debt takes over 100 s.
        let pacer = pacer_at(&p.time(), 1024);
        assert!(
            pacer
                .reserve_within(120_000, Duration::MAX)
                .expect("no limit")
                > Duration::from_secs(100),
            "deep in debt"
        );
        p.a.pace_with(pacer.clone());
        let (a, time) = (p.a.clone(), p.time());
        let (elapsed, control, short, stream, large, pipe) = p
            .drive(async move {
                let started = time.now();
                let control = a
                    .send_control_bounded(
                        &time,
                        Duration::from_secs(1),
                        QuicControl::Ready {
                            attempt: 1,
                            caps: 0,
                        },
                    )
                    .await;
                let short = a
                    .send_short_bounded(&time, SendDeadlines::short(5), b"short")
                    .await;
                let stream = a
                    .send_stream_bounded(
                        &time,
                        SendDeadlines::stream(MIB as u64),
                        StreamId::next(),
                        None,
                        Bytes::from(payload(MIB)),
                        None,
                    )
                    .await;
                // A Short past one packet and a pipe are charged too.
                let large = a
                    .send_short_bounded(&time, SendDeadlines::short(3_000), &payload(3_000))
                    .await;
                let chunks = vec![Ok(Bytes::from(payload(3_000)))];
                let pipe = a
                    .send_stream_chunks_bounded(
                        &time,
                        SendDeadlines::stream(3_000),
                        StreamId::next(),
                        3_000,
                        None,
                        futures::stream::iter(chunks),
                        None,
                    )
                    .await;
                (time.now() - started, control, short, stream, large, pipe)
            })
            .await;
        assert!(matches!(control, BoundedSend::Sent), "{control:?}");
        assert!(matches!(short, BoundedSend::Sent), "{short:?}");
        for (kind, outcome) in [
            ("stream", &stream),
            ("large short", &large),
            ("pipe", &pipe),
        ] {
            assert!(
                matches!(
                    outcome,
                    BoundedSend::Failed(SendFailure::NotOpened(NotOpened::Paced))
                ),
                "{kind}: {outcome:?}"
            );
        }
        assert!(
            elapsed < Duration::from_secs(1),
            "nothing waited: {elapsed:?}"
        );
        assert_eq!(p.a.send_aborts(), 0, "no stream was opened to reset");
        // The refusal took nothing: the debt is what it was, about 109 s.
        let wait = pacer
            .reserve_within(0, Duration::MAX)
            .expect("no limit to refuse at");
        assert!(wait > Duration::from_secs(100), "{wait:?}");
    });
}

/// A pacer whose answers a test sets: from call `refuse_from` on it refuses,
/// and before that it grants after `wait`. It records what each reservation
/// asked for and counts what comes back.
struct Scripted {
    refuse_from: usize,
    wait: Duration,
    calls: AtomicUsize,
    limits: parking_lot::Mutex<Vec<Duration>>,
    asked: parking_lot::Mutex<Vec<usize>>,
    charged: AtomicUsize,
    given_back: AtomicUsize,
    time: VirtualTime,
}

impl Scripted {
    fn new(time: &VirtualTime, refuse_from: usize, wait: Duration) -> Arc<Self> {
        Arc::new(Self {
            refuse_from,
            wait,
            calls: AtomicUsize::new(0),
            limits: parking_lot::Mutex::new(Vec::new()),
            asked: parking_lot::Mutex::new(Vec::new()),
            charged: AtomicUsize::new(0),
            given_back: AtomicUsize::new(0),
            time: time.clone(),
        })
    }
}

impl SendPacer for Scripted {
    fn reserve_within(&self, bytes: usize, limit: Duration) -> Result<Duration, Duration> {
        self.limits.lock().push(limit);
        self.asked.lock().push(bytes);
        if self.calls.fetch_add(1, Ordering::Relaxed) >= self.refuse_from {
            Err(limit)
        } else {
            Ok(self.wait)
        }
    }

    fn give_back(&self, bytes: usize) {
        self.given_back.fetch_add(bytes, Ordering::Relaxed);
    }

    fn charge(&self, bytes: usize) -> usize {
        self.charged.fetch_add(bytes, Ordering::Relaxed);
        bytes
    }

    fn sleep(
        &self,
        wait: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        self.time.sleep_cancelable(wait)
    }
}

/// A refusal after the stream opened ends the send `Failed(Paced)` and resets
/// the stream, so the receiver drops what came.
/// Each reservation is limited to what the send's current deadline leaves: the
/// first, before the stream opens, to the open deadline.
#[test]
fn a_refusal_after_the_stream_opened_resets_it() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-pacer-refused", QuicPathLimits::DEFAULT).await;
        // Header and metadata, then two fragments, then a refusal.
        let pacer = Scripted::new(&p.time(), 3, Duration::ZERO);
        p.a.pace_with(pacer.clone());
        let (_, outcome) = time_a_stream(&p, 10 * FRAGMENT_PAYLOAD_SIZE).await;
        assert!(
            matches!(outcome, BoundedSend::Failed(SendFailure::Paced)),
            "{outcome:?}"
        );
        assert_eq!(p.a.send_aborts(), 1, "the stream was reset");
        let limits = pacer.limits.lock().clone();
        assert_eq!(limits.len(), 4);
        assert!(
            limits[0] <= Duration::from_secs(2),
            "the open deadline: {limits:?}"
        );
        assert!(
            limits[1..]
                .iter()
                .all(|limit| *limit <= Duration::from_secs(5)),
            "the gap: {limits:?}"
        );
        assert_eq!(pacer.given_back.load(Ordering::Relaxed), 0);
    });
}

/// A send dropped while it waits on the pacer (here its gap deadline passed
/// first) gives back the bytes it took for the write it did not make.
#[test]
fn bytes_taken_for_a_write_not_made_go_back() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-pacer-give-back", QuicPathLimits::DEFAULT).await;
        // Header and metadata go at once; the first fragment is granted after
        // 30 s, ignoring the limit it is offered (a real pacer would refuse
        // any wait past the 5 s gap, which is how this case arises: a pacer
        // that waits longer than it said). 30 s is past the gap and past the
        // connection's window too: the header's acknowledgement, seen at the
        // first wake (the 2 s open deadline), extends the deadline to 12 s,
        // and nothing moves after it, since the send waits on the pacer. A
        // 10 s grant would finish inside that window and the send complete.
        let pacer = Scripted::new(&p.time(), usize::MAX, Duration::ZERO);
        struct SlowAfterFirst(Arc<Scripted>);
        impl SendPacer for SlowAfterFirst {
            fn reserve_within(&self, bytes: usize, limit: Duration) -> Result<Duration, Duration> {
                let first = self.0.calls.load(Ordering::Relaxed) == 0;
                self.0.reserve_within(bytes, limit)?;
                Ok(if first {
                    Duration::ZERO
                } else {
                    Duration::from_secs(30)
                })
            }
            fn give_back(&self, bytes: usize) {
                self.0.give_back(bytes);
            }
            fn charge(&self, bytes: usize) -> usize {
                self.0.charge(bytes)
            }
            fn sleep(
                &self,
                wait: Duration,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
                self.0.sleep(wait)
            }
        }
        p.a.pace_with(Arc::new(SlowAfterFirst(pacer.clone())));
        let (_, outcome) = time_a_stream(&p, 10 * FRAGMENT_PAYLOAD_SIZE).await;
        assert!(
            matches!(outcome, BoundedSend::Failed(SendFailure::TimedOut)),
            "{outcome:?}"
        );
        assert_eq!(
            pacer.given_back.load(Ordering::Relaxed),
            FRAGMENT_PAYLOAD_SIZE,
            "the first fragment's bytes, taken and not written"
        );
    });
}

/// The pacer paces at the limit itself, read again at every reservation: under
/// `total-bandwidth-limit` its rate follows the per-connection share as
/// connections come and go. A `bandwidth-limit` above the UDP congestion
/// controller's 1.25 MB/s is the pacer's rate too: QUIC is never paced below
/// what the operator set.
#[test]
fn a_limit_pacer_paces_at_the_limit_and_follows_the_share() {
    let time = VirtualTime::new();
    let high = LimitPacer::new(QuicRateLimit::new(Some(5_000_000), None), time.clone())
        .expect("a limit paces");
    assert_eq!(high.rate(), 5_000_000);
    assert!(
        LimitPacer::new(QuicRateLimit::new(None, None), time.clone()).is_none(),
        "no limit, no pacer"
    );

    let manager = Arc::new(GlobalBandwidthManager::new(1_000_000, Some(1_000)));
    manager.register_connection();
    let shared = LimitPacer::new(
        QuicRateLimit::new(None, Some(manager.clone())),
        time.clone(),
    )
    .expect("a total limit paces");
    assert_eq!(shared.rate(), 1_000_000);
    manager.register_connection();
    shared
        .reserve_within(1_200, Duration::MAX)
        .expect("no limit to refuse at");
    assert_eq!(shared.rate(), 500_000, "the share of two connections");
}

/// What each write takes from the pacer: the header and the metadata, before
/// the stream opens, then each fragment-sized piece of the payload.
#[test]
fn each_write_takes_its_own_bytes() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-pacer-bytes", QuicPathLimits::DEFAULT).await;
        let pacer = Scripted::new(&p.time(), usize::MAX, Duration::ZERO);
        p.a.pace_with(pacer.clone());
        let (a, time) = (p.a.clone(), p.time());
        let outcome = p
            .drive(async move {
                a.send_stream_bounded(
                    &time,
                    SendDeadlines::stream(2 * FRAGMENT_PAYLOAD_SIZE as u64),
                    StreamId::next(),
                    Some(b"announce"),
                    Bytes::from(payload(2 * FRAGMENT_PAYLOAD_SIZE)),
                    None,
                )
                .await
            })
            .await;
        assert!(matches!(outcome, BoundedSend::Sent), "{outcome:?}");
        assert_eq!(
            *pacer.asked.lock(),
            vec![
                HEADER_LEN + b"announce".len(),
                FRAGMENT_PAYLOAD_SIZE,
                FRAGMENT_PAYLOAD_SIZE
            ]
        );
    });
}

/// The pacer's burst stays a tenth of its rate as the share changes, so a
/// connection that opened alone does not keep a tenth of the whole total once
/// it shares it, and one that opened among many gets its share's burst later.
#[test]
fn a_limit_pacer_burst_follows_the_share() {
    let time = VirtualTime::new();
    let manager = Arc::new(GlobalBandwidthManager::new(1_000_000, Some(1_000)));
    manager.register_connection();
    let shared = LimitPacer::new(
        QuicRateLimit::new(None, Some(manager.clone())),
        time.clone(),
    )
    .expect("a total limit paces");
    assert_eq!(shared.capacity(), 100_000);
    manager.register_connection();
    shared
        .reserve_within(0, Duration::MAX)
        .expect("no limit to refuse at");
    assert_eq!(shared.capacity(), 50_000, "a tenth of the share of two");
    manager.unregister_connection();
    shared
        .reserve_within(0, Duration::MAX)
        .expect("no limit to refuse at");
    assert_eq!(shared.capacity(), 100_000, "and back");
}

/// Bytes charged for a message that went on UDP are debt: the next reservation
/// waits them out at the limit (up to `FALLBACK_DEBT` of them).
#[test]
fn a_charged_fallback_holds_the_next_send_to_the_limit() {
    let time = VirtualTime::new();
    let pacer = LimitPacer::new(QuicRateLimit::new(Some(100_000), None), time.clone())
        .expect("a limit paces");
    // Spend the 10 kB burst, then charge 50 kB sent on UDP.
    assert_eq!(
        pacer.reserve_within(10_000, Duration::MAX),
        Ok(Duration::ZERO)
    );
    assert_eq!(
        pacer.charge(50_000),
        50_000,
        "half a second of the rate: all of it"
    );
    let wait = pacer
        .reserve_within(0, Duration::MAX)
        .expect("no limit to refuse at");
    assert!(
        wait >= Duration::from_millis(490) && wait <= Duration::from_millis(510),
        "50 kB at 100 kB/s: {wait:?}"
    );
}

/// Fallback debt is bounded at `FALLBACK_DEBT`: under an overload that sends
/// 50 MB on UDP, QUIC owes half a second, not 500 s, and takes a datagram
/// again half a second after the overload ends.
#[test]
fn fallback_debt_is_bounded_and_quic_resumes_after_an_overload() {
    use super::super::pacer::FALLBACK_DEBT;
    let time = VirtualTime::new();
    let pacer = pacer_at(&time, 100_000);
    let mut charged = 0;
    for _ in 0..100 {
        charged += pacer.charge(500_000);
    }
    // The 10 kB burst, then half a second at 100 kB/s.
    assert_eq!(charged, 60_000, "50 MB sent on UDP, 60 kB charged");
    let owed = pacer
        .reserve_within(0, Duration::MAX)
        .expect("no limit to refuse at");
    assert_eq!(owed, FALLBACK_DEBT);
    time.advance(FALLBACK_DEBT);
    assert_eq!(
        pacer.reserve_within(1_200, Duration::from_millis(100)),
        Ok(Duration::from_millis(12)),
        "QUIC sends again once the bounded debt is paid, at the rate: 1200 B in 12 ms"
    );
}

/// A pacer that charges a large fallback (10 MB, as a 1 MB stream refused at a
/// small share would have) at its `at`th reservation, as a concurrent message
/// going on UDP does, and otherwise is the limit pacer it wraps.
struct ChargesMidway {
    inner: Arc<LimitPacer<VirtualTime>>,
    at: usize,
    calls: AtomicUsize,
}

impl SendPacer for ChargesMidway {
    fn reserve_within(&self, bytes: usize, limit: Duration) -> Result<Duration, Duration> {
        if self.calls.fetch_add(1, Ordering::Relaxed) == self.at {
            self.inner.charge(10_000_000);
        }
        self.inner.reserve_within(bytes, limit)
    }

    fn give_back(&self, bytes: usize) {
        self.inner.give_back(bytes);
    }

    fn charge(&self, bytes: usize) -> usize {
        self.inner.charge(bytes)
    }

    fn sleep(
        &self,
        wait: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        self.inner.sleep(wait)
    }
}

/// A stream already open when another message's fallback is charged finishes:
/// the charge adds at most half a second to its next write, inside its gap
/// deadline. With the debt unbounded its next fragment would wait 100 s and
/// be refused, and a refused open stream is dropped.
#[test]
fn an_open_stream_survives_a_fallback_charge() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-pacer-survives", QuicPathLimits::DEFAULT).await;
        // The charge comes at the second fragment, after the stream opened.
        p.a.pace_with(Arc::new(ChargesMidway {
            inner: pacer_at(&p.time(), 100_000),
            at: 2,
            calls: AtomicUsize::new(0),
        }));
        let (elapsed, outcome) = time_a_stream(&p, 5 * FRAGMENT_PAYLOAD_SIZE).await;
        assert!(matches!(outcome, BoundedSend::Sent), "{outcome:?}");
        assert!(elapsed < Duration::from_secs(2), "{elapsed:?}");
    });
}

/// A limit pacer that counts the reservations it made wait.
struct CountsWaits {
    inner: Arc<LimitPacer<VirtualTime>>,
    waited: AtomicUsize,
}

impl SendPacer for CountsWaits {
    fn reserve_within(&self, bytes: usize, limit: Duration) -> Result<Duration, Duration> {
        let answer = self.inner.reserve_within(bytes, limit);
        if matches!(answer, Ok(wait) if !wait.is_zero()) {
            self.waited.fetch_add(1, Ordering::Relaxed);
        }
        answer
    }

    fn give_back(&self, bytes: usize) {
        self.inner.give_back(bytes);
    }

    fn charge(&self, bytes: usize) -> usize {
        self.inner.charge(bytes)
    }

    fn sleep(
        &self,
        wait: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        self.inner.sleep(wait)
    }
}

/// A paced path whose peer goes dark: a real limit pacer holds the stream's
/// writes, the writes then stall on the peer's flow control and the send ends
/// `Failed(TimedOut)` at its gap, not `Paced`, since the pacer waited and
/// refused nothing. The send ran past the stall window with nothing
/// acknowledged, so its timeout makes the path suspect, as on an unpaced path:
/// pacer waits are not progress.
#[test]
fn a_paced_send_to_a_dark_peer_times_out_and_the_path_goes_suspect() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let network = "quic-pacer-dark";
        let p = paths(network, QuicPathLimits::DEFAULT).await;
        let pacer = Arc::new(CountsWaits {
            inner: pacer_at(&p.time(), 400_000),
            waited: AtomicUsize::new(0),
        });
        p.a.pace_with(pacer.clone());
        let _dark = DropFrom::new(network, &[p.pair.b_addr]);
        let (a, time) = (p.a.clone(), p.time());
        let body = over_window();
        let deadlines = SendDeadlines::stream(body.len() as u64);
        let outcome = p
            .drive(async move {
                a.send_stream_bounded(&time, deadlines, StreamId::next(), None, body, None)
                    .await
            })
            .await;
        assert!(
            matches!(outcome, BoundedSend::Failed(SendFailure::TimedOut)),
            "{outcome:?}"
        );
        assert!(
            pacer.waited.load(Ordering::Relaxed) > 0,
            "the pacer held writes"
        );
        // The peer was dark from the start and the send ran past the stall
        // window (the window's writes at 400 kB/s, then the 5 s gap).
        let now = p.time().now_nanos();
        p.a.settle(SendKind::Stream, &outcome, false, now);
        assert!(
            p.a.is_suspect(),
            "nothing acknowledged for the window: the dark paced path is suspect"
        );
    });
}

/// A pacer wait is not progress (`StallCount`, the `pacer` module): neither
/// function that waits on the pacer marks the send as having moved. Behaviour
/// cannot show this, since every wait is followed by the write it paid for,
/// which is itself a step, so it is pinned on the source. Each region is the
/// function's own body (a free function ends at the first `}` in column 0, a
/// method at the first in column 4) and must hold the pacer's sleep, so a
/// moved signature fails instead of widening the region.
#[test]
fn a_pacer_wait_never_marks_a_step() {
    let source = include_str!("../../path.rs");
    for (signature, end) in [
        ("\nasync fn pace(", "\n}\n"),
        ("\n    async fn pace(&mut self", "\n    }\n"),
    ] {
        assert_eq!(source.matches(signature).count(), 1, "{signature:?}");
        let start = source.find(signature).expect("counted above");
        let body = &source[start..];
        let body = &body[..body.find(end).expect("the function's end")];
        assert!(
            body.contains("pacer.sleep(wait)"),
            "{signature:?} waits on the pacer"
        );
        assert!(
            !body.contains("step("),
            "{signature:?} must not count a pacer wait as progress"
        );
    }
}

/// A pacer that grants every reservation at once until `slow_from`
/// reservations have been made, then makes each wait `wait`, refusing one whose
/// wait would reach the limit it is offered, as `LimitPacer` does. Records the
/// limits it was offered.
struct Honest {
    slow_from: usize,
    wait: Duration,
    calls: AtomicUsize,
    limits: parking_lot::Mutex<Vec<Duration>>,
    given_back: AtomicUsize,
    time: VirtualTime,
}

impl Honest {
    fn new(time: &VirtualTime, slow_from: usize, wait: Duration) -> Arc<Self> {
        Arc::new(Self {
            slow_from,
            wait,
            calls: AtomicUsize::new(0),
            limits: parking_lot::Mutex::new(Vec::new()),
            given_back: AtomicUsize::new(0),
            time: time.clone(),
        })
    }
}

impl SendPacer for Honest {
    fn reserve_within(&self, _bytes: usize, limit: Duration) -> Result<Duration, Duration> {
        self.limits.lock().push(limit);
        let wait = if self.calls.fetch_add(1, Ordering::Relaxed) < self.slow_from {
            Duration::ZERO
        } else {
            self.wait
        };
        if wait < limit { Ok(wait) } else { Err(wait) }
    }

    fn give_back(&self, bytes: usize) {
        self.given_back.fetch_add(bytes, Ordering::Relaxed);
    }

    fn charge(&self, bytes: usize) -> usize {
        bytes
    }

    fn sleep(
        &self,
        wait: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        self.time.sleep_cancelable(wait)
    }
}

/// A pacer is offered at most the send's own gap, never the
/// connection's window, so its queue holds no more than #6126 allowed. Over
/// the lossy path of
/// `a_lossy_path_finishes_a_stream_whose_writes_wait_past_the_gap`, where
/// quinn makes writes wait past the gap and the connection's window carries
/// the send, every reservation is offered at most 5 s and the send completes:
/// the long waits are quinn's, after a reservation, and the write they end in
/// is a step, so the next reservation is offered a fresh gap. Plain Cubic,
/// as in that test.
#[test]
fn a_pacer_is_offered_at_most_the_gap_on_a_lossy_live_path() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let network = "quic-pacer-lossy-window";
        let loss = seeded_loss_from(network, 5, 0x5940_0007, Some(Duration::from_millis(100)));
        let p = paths_without_loss_gate(network).await;
        let pacer = Scripted::new(&p.time(), usize::MAX, Duration::ZERO);
        p.a.pace_with(pacer.clone());
        let (elapsed, outcome) = time_a_stream(&p, 8 * MIB).await;
        crate::transport::in_memory_socket::set_packet_delivery_callback(network, None);
        let dropped = loss.dropped.load(Ordering::Relaxed);
        assert!(dropped > 0, "the loss happened");
        assert!(matches!(outcome, BoundedSend::Sent), "{outcome:?}");
        let gap = crate::transport::peer_connection::streaming::STREAM_INACTIVITY_TIMEOUT;
        assert!(
            elapsed > gap,
            "premise: the send outlived its own gap: {elapsed:?}"
        );
        let longest = pacer
            .limits
            .lock()
            .iter()
            .copied()
            .max()
            .expect("the send was paced");
        assert!(longest <= gap, "offered {longest:?}, past the gap");
    });
}

/// Long pacer waits inside the gap on a live connection: every fragment after
/// the header waits 4 s, under the 5 s gap the pacer is offered each time
/// (each write is a step), and the stream is `Sent` after about 4 s per
/// fragment.
#[test]
fn long_pacer_waits_inside_the_gap_complete() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-pacer-long-waits", QuicPathLimits::DEFAULT).await;
        let pacer = Honest::new(&p.time(), 1, Duration::from_secs(4));
        p.a.pace_with(pacer.clone());
        let fragments = 3;
        let (elapsed, outcome) = time_a_stream(&p, fragments * FRAGMENT_PAYLOAD_SIZE).await;
        assert!(matches!(outcome, BoundedSend::Sent), "{outcome:?}");
        assert!(
            elapsed >= Duration::from_secs(4 * fragments as u64),
            "{elapsed:?}"
        );
        assert_eq!(pacer.calls.load(Ordering::Relaxed), 1 + fragments);
    });
}

/// A wait past the gap is refused, after the stream opened: the send ends
/// `Failed(Paced)` at once, leaves no quiet proof, and its settle counts
/// nothing toward the stall close, so the path does not go suspect. The first
/// fragment is offered at most 5 s, so the 8 s wait is refused.
#[test]
fn a_pacer_wait_past_the_gap_is_refused_and_counts_for_nothing() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-pacer-past-gap", QuicPathLimits::DEFAULT).await;
        let pacer = Honest::new(&p.time(), 1, Duration::from_secs(8));
        p.a.pace_with(pacer.clone());
        let (elapsed, outcome) = time_a_stream(&p, 3 * FRAGMENT_PAYLOAD_SIZE).await;
        assert!(
            matches!(outcome, BoundedSend::Failed(SendFailure::Paced)),
            "{outcome:?}"
        );
        assert!(
            elapsed < Duration::from_secs(1),
            "refused at once: {elapsed:?}"
        );
        let limits = pacer.limits.lock().clone();
        assert_eq!(limits.len(), 2, "the header, then the refused fragment");
        assert!(limits[1] <= Duration::from_secs(5), "{limits:?}");
        assert_eq!(p.a.stall_state(), (0, None), "no proof left");
        p.a.settle(SendKind::Stream, &outcome, false, p.time().now_nanos());
        assert_eq!(p.a.stall_state().0, 0, "not counted");
        assert!(!p.a.is_suspect());
    });
}

/// The pacer is offered no more than `total` leaves: with a 3 s total, under
/// the 5 s gap, every limit after the header is at most what remains of it.
#[test]
fn the_pacer_is_offered_no_more_than_total_leaves() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-pacer-total", QuicPathLimits::DEFAULT).await;
        let pacer = Honest::new(&p.time(), usize::MAX, Duration::ZERO);
        p.a.pace_with(pacer.clone());
        let (a, time) = (p.a.clone(), p.time());
        let len = 3 * FRAGMENT_PAYLOAD_SIZE;
        let deadlines = SendDeadlines {
            total: Duration::from_secs(3),
            ..SendDeadlines::stream(len as u64)
        };
        let outcome = p
            .drive(async move {
                a.send_stream_bounded(
                    &time,
                    deadlines,
                    StreamId::next(),
                    None,
                    Bytes::from(payload(len)),
                    None,
                )
                .await
            })
            .await;
        assert!(matches!(outcome, BoundedSend::Sent), "{outcome:?}");
        let limits = pacer.limits.lock().clone();
        assert!(limits.len() > 1, "{limits:?}");
        assert!(
            limits.iter().all(|limit| *limit <= Duration::from_secs(3)),
            "{limits:?}"
        );
    });
}

/// A paced path that goes dark mid-send fails the send within two windows of
/// the open deadline's wake, as without a pacer, and its timeout counts: the
/// stall count's first counted timeout, the quiet proof, and the path suspect.
/// A pacer's waits are not progress (a real limit pacer at 400 kB/s waits here).
#[test]
fn a_paced_path_dark_mid_send_fails_within_two_windows_and_goes_suspect() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let network = "quic-pacer-dark-mid-send";
        let p = paths(network, QuicPathLimits::DEFAULT).await;
        let pacer = Arc::new(CountsWaits {
            inner: pacer_at(&p.time(), 400_000),
            waited: AtomicUsize::new(0),
        });
        p.a.pace_with(pacer.clone());
        let (a, time) = (p.a.clone(), p.time());
        let body = over_window();
        let fragments = body.len().div_ceil(FRAGMENT_PAYLOAD_SIZE);
        let silencer = Arc::new(SilenceAfter {
            network,
            from: p.pair.b_addr,
            // A quarter in: at 400 kB/s the rest is more than a send window,
            // so writes are still waiting when the path goes dark.
            after: fragments / 4,
            sent: AtomicUsize::new(0),
            silenced: parking_lot::Mutex::new(None),
        });
        let watched = silencer.clone();
        let deadlines = SendDeadlines::stream(body.len() as u64);
        let (outcome, took) = p
            .drive(async move {
                let start = time.now();
                let outcome = a
                    .send_stream_bounded(
                        &time,
                        deadlines,
                        StreamId::next(),
                        None,
                        body,
                        Some(&*watched),
                    )
                    .await;
                (outcome, time.now() - start)
            })
            .await;
        assert!(silencer.silenced.lock().is_some(), "the path went dark");
        assert!(
            pacer.waited.load(Ordering::Relaxed) > 0,
            "the pacer held writes"
        );
        assert!(
            matches!(outcome, BoundedSend::Failed(SendFailure::TimedOut)),
            "{outcome:?}"
        );
        let window = super::super::route::STALL_PROGRESS_WINDOW;
        assert!(
            took > Duration::from_secs(5) && took <= Duration::from_secs(2) + 2 * window,
            "took {took:?}"
        );
        assert!(p.a.stall_state().1.is_some(), "the timeout left a proof");
        p.a.settle(SendKind::Stream, &outcome, false, p.time().now_nanos());
        assert_eq!(p.a.stall_state().0, 1, "the timeout counted");
        assert!(p.a.is_suspect(), "the first timeout makes the path suspect");
        drop(silencer.silenced.lock().take());
    });
}

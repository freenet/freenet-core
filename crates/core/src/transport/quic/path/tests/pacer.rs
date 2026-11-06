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
        let p = paths("quic-pacer-rate", 120, QuicPathLimits::DEFAULT).await;
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
        assert!(p.a.pacer_holds() > 0, "the sends waited on the pacer");
    });
}

/// With the bucket deep in debt: a control frame and a Short of one packet go
/// at once, and a Stream is refused before its stream opens
/// (`NotOpened(TimedOut)`, so it may go on UDP), at once, taking nothing.
#[test]
fn control_frames_and_one_packet_shorts_do_not_wait_behind_data() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-pacer-control", 121, QuicPathLimits::DEFAULT).await;
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
        let (elapsed, control, short, stream) = p
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
                (time.now() - started, control, short, stream)
            })
            .await;
        assert!(matches!(control, BoundedSend::Sent), "{control:?}");
        assert!(matches!(short, BoundedSend::Sent), "{short:?}");
        assert!(
            matches!(
                stream,
                BoundedSend::Failed(SendFailure::NotOpened(NotOpened::TimedOut))
            ),
            "{stream:?}"
        );
        assert!(
            elapsed < Duration::from_secs(1),
            "nothing waited: {elapsed:?}"
        );
        assert_eq!(p.a.send_aborts(), 0, "no stream was opened to reset");
        assert_eq!(p.a.pacer_holds(), 1, "the one refusal");
        // The refusal took nothing: the debt is what it was, about 109 s.
        let wait = pacer
            .reserve_within(0, Duration::MAX)
            .expect("no limit to refuse at");
        assert!(wait > Duration::from_secs(100), "{wait:?}");
    });
}

/// A pacer whose answers a test sets: from call `refuse_from` on it refuses,
/// and before that it grants after `wait`. It counts what comes back.
struct Scripted {
    refuse_from: usize,
    wait: Duration,
    calls: AtomicUsize,
    limits: parking_lot::Mutex<Vec<Duration>>,
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
            given_back: AtomicUsize::new(0),
            time: time.clone(),
        })
    }
}

impl SendPacer for Scripted {
    fn reserve_within(&self, _bytes: usize, limit: Duration) -> Result<Duration, Duration> {
        self.limits.lock().push(limit);
        if self.calls.fetch_add(1, Ordering::Relaxed) >= self.refuse_from {
            Err(limit)
        } else {
            Ok(self.wait)
        }
    }

    fn give_back(&self, bytes: usize) {
        self.given_back.fetch_add(bytes, Ordering::Relaxed);
    }

    fn sleep(
        &self,
        wait: Duration,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        self.time.sleep_cancelable(wait)
    }
}

/// A refusal after the stream opened ends the send as at its gap deadline,
/// `Failed(TimedOut)`, and resets the stream, so the receiver drops what came.
/// Each reservation is limited to what the send's current deadline leaves: the
/// first, before the stream opens, to the open deadline.
#[test]
fn a_refusal_after_the_stream_opened_resets_it() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let p = paths("quic-pacer-refused", 122, QuicPathLimits::DEFAULT).await;
        // Header and metadata, then two fragments, then a refusal.
        let pacer = Scripted::new(&p.time(), 3, Duration::ZERO);
        p.a.pace_with(pacer.clone());
        let (_, outcome) = time_a_stream(&p, 10 * FRAGMENT_PAYLOAD_SIZE).await;
        assert!(
            matches!(outcome, BoundedSend::Failed(SendFailure::TimedOut)),
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
        let p = paths("quic-pacer-give-back", 123, QuicPathLimits::DEFAULT).await;
        // Header and metadata go at once; the first fragment is granted after
        // 10 s, past the 5 s gap.
        let pacer = Scripted::new(&p.time(), usize::MAX, Duration::ZERO);
        struct SlowAfterFirst(Arc<Scripted>);
        impl SendPacer for SlowAfterFirst {
            fn reserve_within(&self, bytes: usize, limit: Duration) -> Result<Duration, Duration> {
                let first = self.0.calls.load(Ordering::Relaxed) == 0;
                self.0.reserve_within(bytes, limit)?;
                Ok(if first {
                    Duration::ZERO
                } else {
                    Duration::from_secs(10)
                })
            }
            fn give_back(&self, bytes: usize) {
                self.0.give_back(bytes);
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

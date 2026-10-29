//! Tests for [`super::AckWatch`], [`super::AckCounting`] and the recycle
//! constants. The guards on a live path are tested in `path::tests`.

use super::*;

const SEC: u64 = 1_000_000_000;

/// A reading of [`CapHolds`] with no cap.
const NO_CAP: CapHoldReading = CapHoldReading {
    holds: 0,
    held_now: false,
    validity: Duration::ZERO,
};

/// The ticks of the window (`window / tick`), and the tick a window
/// started at tick `start` closes at.
fn closes_at(start: u64) -> u64 {
    start + ACK_WATCH_WINDOW.as_secs() / ACK_WATCH_TICK.as_secs()
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
}

const fn reading(acked: u64, outstanding: u64, sent: u64, received: u64) -> Reading {
    Reading {
        acked,
        batches: acked,
        cap_holds: 0,
        cap_held_now: false,
        cap_validity: Duration::from_secs(3600),
        outstanding,
        sent,
        received,
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
fn run(ticks: u64, mut at: impl FnMut(u64) -> Reading) -> Option<u64> {
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
            },
        )
    };
    let mut watch = AckWatch::new(ACK_WATCH_WINDOW, sample(at(0)));
    (1..=ticks).find(|tick| watch.observe(tick * ACK_WATCH_TICK.as_secs() * SEC, sample(at(*tick))))
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

/// The peer silent throughout: never closed (quinn's idle timeout ends
/// that connection).
#[test]
fn a_silent_peer_is_left_to_the_idle_timeout() {
    assert_eq!(run(100, |t| reading(5, 1200, 10 + t, 100)), None);
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
    // the send at tick 6 starts the next one.
    let closed = run(100, |t| {
        let received = match t {
            0..=3 => 100 + t,
            4 | 5 => 103,
            _ => 98 + t,
        };
        reading(5, 0, 10 + t, received)
    });
    assert_eq!(closed, Some(closes_at(6)));
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

/// Recovery from an outage in both directions. Before it, sends with
/// nothing from the peer (probes); after it, packets still outstanding but
/// nothing new sent and nothing acknowledged (the probe timer is backed
/// off), while the peer sends every tick: no window. The probe at tick 30
/// is acknowledged at tick 31: still no close.
#[test]
fn recovery_from_an_outage_in_both_directions_does_not_close() {
    let closed = run(200, |t| match t {
        // The outage: probes, nothing from the peer.
        0..=15 => reading(5, 1200, 10 + t, 100),
        // Recovered: the peer sends every tick, nothing of ours moves.
        16..=29 => reading(5, 1200, 25, 100 + t),
        // The backed-off probe, then its acknowledgement.
        30 => reading(5, 1200, 26, 100 + t),
        _ => reading(6, 0, 26, 100 + t),
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

/// `outer` (a [`Recorder`] around an [`AckCounting`] around a [`Recorder`]
/// around Cubic) and its two logs, and the `AckCounting`'s record.
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
        let counting = Box::new(AckCounting::new(Recorder::wrap(&inner_log, cubic)));
        Self {
            outer: Recorder::wrap(&outer_log, counting),
            outer_log,
            inner_log,
        }
    }

    /// Every call reached `inner` exactly as it reached the `AckCounting`.
    /// `clone_box` is not compared: the `AckCounting` clones its inner too.
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

/// The `AckCounting` record inside a controller built by [`Stack`].
fn record_of(outer: &dyn Controller) -> Arc<Mutex<AckState>> {
    let recorder = outer
        .clone_box()
        .into_any()
        .downcast::<Recorder>()
        .expect("the outer recorder");
    recorder
        .inner
        .into_any()
        .downcast::<AckCounting>()
        .expect("the AckCounting")
        .state
}

/// Every method an `AckCounting` gets is forwarded to its inner controller
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
            let counting = Box::new(AckCounting::new(Recorder::wrap(
                &self.inner_log,
                Arc::new(cubic).build(now, mtu),
            )));
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

/// Pin: `impl Controller for AckCounting` implements every method of the
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
        "impl Controller for AckCounting",
    );
    assert!(trait_methods.len() >= 10, "{trait_methods:?}");
    assert_eq!(ours, trait_methods);
}

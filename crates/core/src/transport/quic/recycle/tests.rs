//! Tests for [`super::AckWatch`], [`super::AckCounting`] and the recycle
//! constants. The guards on a live path are tested in `path::tests`.

use super::*;

const SEC: u64 = 1_000_000_000;

/// The ticks of the window (`window / tick`), and the tick a window
/// started at tick `start` closes at.
fn closes_at(start: u64) -> u64 {
    start + ACK_WATCH_WINDOW.as_secs() / ACK_WATCH_TICK.as_secs()
}

/// What one sample reads, in the units `AckSample::synthetic` takes.
#[derive(Clone, Copy)]
struct Reading {
    acked: u64,
    outstanding: u64,
    sent: u64,
    received: u64,
}

const fn reading(acked: u64, outstanding: u64, sent: u64, received: u64) -> Reading {
    Reading {
        acked,
        outstanding,
        sent,
        received,
    }
}

/// Feed a watch one sample per `ACK_WATCH_TICK`, from tick 1 to `ticks`
/// (tick 0 is the first sample); the first tick at which it said close.
fn run(ticks: u64, mut at: impl FnMut(u64) -> Reading) -> Option<u64> {
    let record = Arc::default();
    let sample =
        |r: Reading| AckSample::synthetic(&record, r.acked, r.outstanding, r.sent, r.received);
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

/// One sample with nothing from the peer, inside the window, ends it; the
/// next send starts a new one.
#[test]
fn a_sample_with_nothing_from_the_peer_ends_the_window() {
    // Sends every tick; nothing from the peer at tick 4.
    let closed = run(100, |t| {
        let received = if t >= 4 { 99 + t } else { 100 + t };
        reading(5, 0, 10 + t, received)
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
    let at_close = closes_at(1);
    let closed = run(at_close, |t| {
        let acked = if t >= at_close { 6 } else { 5 };
        reading(acked, 0, 10 + t, 100 + t)
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
    let s = |record, sent, received| AckSample::synthetic(record, 0, 0, sent, received);
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
    transfer_one_stream(&mut receiver, &mut sender, now, 64 * 1024);
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
    assert_eq!(record_of(stack.outer.as_ref()).lock().acked, acks as u64);
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

/// The locked quinn-proto version, from the workspace `Cargo.lock`.
fn locked_quinn_proto_version() -> String {
    let lock = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../../Cargo.lock"))
        .expect("the workspace Cargo.lock");
    let at = lock
        .find("name = \"quinn-proto\"\nversion = \"")
        .expect("quinn-proto in Cargo.lock");
    let rest = &lock[at + "name = \"quinn-proto\"\nversion = \"".len()..];
    rest[..rest.find('"').expect("a version")].to_string()
}

/// Pin: `impl Controller for AckCounting` implements every method of the
/// locked quinn-proto's `Controller` trait. A method quinn adds with a
/// default body would otherwise run quinn's default instead of reaching
/// Cubic (and, for an acknowledgement method, the record), and nothing else
/// would fail. Fails if the quinn-proto source cannot be found.
#[test]
fn ack_counting_implements_every_controller_method() {
    let version = locked_quinn_proto_version();
    let cargo_home = std::env::var("CARGO_HOME").unwrap_or_else(|_| {
        format!(
            "{}/.cargo",
            std::env::var("HOME").expect("HOME or CARGO_HOME")
        )
    });
    let registry = std::path::Path::new(&cargo_home).join("registry/src");
    let source = std::fs::read_dir(&registry)
        .unwrap_or_else(|error| panic!("{}: {error}", registry.display()))
        .filter_map(Result::ok)
        .map(|index| {
            index
                .path()
                .join(format!("quinn-proto-{version}/src/congestion.rs"))
        })
        .find(|path| path.is_file())
        .unwrap_or_else(|| {
            panic!(
                "quinn-proto {version}'s congestion.rs is not under {}",
                registry.display()
            )
        });
    let quinn = std::fs::read_to_string(&source).expect("read congestion.rs");
    let trait_methods = fn_names_in_block(&quinn, "pub trait Controller");
    let ours = fn_names_in_block(
        include_str!("../recycle.rs"),
        "impl Controller for AckCounting",
    );
    assert!(trait_methods.len() >= 10, "{trait_methods:?}");
    assert_eq!(ours, trait_methods);
}

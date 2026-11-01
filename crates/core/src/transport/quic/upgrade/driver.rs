//! Carries out [`UpgradeMachine`]'s actions for every UDP session of a node
//! (#5940, step e5a-5b).
//!
//! # Shape
//!
//! [`QuicUpgrades`] is per node: the endpoint, the policy, the routes from an
//! address to its session, the peer keys of live sessions and the backoff
//! store. [`QuicPathState`] is the session's part, a field of `PeerConnection`.
//! It owns a driver task that runs the session's machine.
//!
//! The driver is a task and not an arm of `PeerConnection::recv`, for two
//! reasons. `recv` is cancelled on every outbound message, and the machine's
//! caller contract needs a whole step (an `Offer` sent, then `OfferSent`
//! reported) to run to its end. And with QUIC off there is then no task, no
//! select arm and no random draw, so `recv` runs exactly as it did before.
//!
//! The only thing `recv` does for an upgrade is hand a received message to the
//! session with `try_send`. Nothing here waits inside `recv`, and the driver
//! holds no reference to `recv`'s liveness state (`last_received_nanos`,
//! `last_inbound_nanos`), so nothing that arrives over QUIC can refresh the UDP
//! session. An active path writes one other stamp, read only by the node's check
//! of whether a key's holder has gone quiet ([`Session::quic_receipt_nanos`]).
//! With QUIC off a received Offer sets a flag, and the reply goes out at
//! the top of `recv`'s loop, where a cancelled send is tried again.
//!
//! # The caller contract of [`UpgradeMachine`]
//!
//! - A step and the sends it asks for run in the driver, one event at a time.
//!   `OfferSent` is stepped right after the send and before the inbox is read
//!   again, so no reply can be stepped first.
//! - `allow_peer` runs before the `Offer`. A `TearDown` removes an `Offer` not
//!   sent yet ([`Session::apply`]); one that comes from another task (a newer
//!   session ending this one) is seen by the check just before the send.
//! - [`Session::end`] steps `SessionEnded` and only then reads `backoff_seed`;
//!   `None` files nothing.
//! - [`Session::promote`] ends any older session of the peer key, which files
//!   its seed, before it steps `Promoted` on this one. [`SeedStore`] compares
//!   session numbers, so an older session's seed never replaces a newer one's.
//!   The lock order is a `by_key` shard, then a session's state; nothing holding
//!   a session's state takes `by_key`.
//! - Every session reads `now` from its connection's `time_source`, the
//!   listener's clock clone. A client and a server machine are one machine, with
//!   the session's lifetime.
//!
//! # Shadow and data
//!
//! A path is built on the peer's UDP budget, with [`QuicPathLimits::SHADOW`] unless
//! this node's data bit is set for the peer (`UpgradeMachine::accepts_data`). With
//! the bit set it accepts data at once, and what arrives waits in the path's
//! channels, since nothing reads them yet. [`Action::EnableData`] lets the path
//! send data once both bits are set. Only `send_control_bounded` is called on it:
//! no code sends data yet, and a peer that sends none costs nothing here.

#[cfg(test)]
pub(in crate::transport) mod tests;

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use bytes::Bytes;
use dashmap::DashMap;
use dashmap::mapref::entry::Entry;
use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::machine::{
    Action, BackoffSeed, CONTROL_SEND_TIMEOUT, Event, FallbackCause, Inputs, Outcome, Role,
    UpgradeMachine,
};
use super::policy::QuicPolicy;
use crate::config::{GlobalExecutor, GlobalRng};
use crate::node::background_task_monitor::LateRegistrar;
use crate::simulation::{RealTime, TimeSource, VirtualTime};
use crate::transport::crypto::TransportPublicKey;
use crate::transport::metrics::{QuicCloseCode, QuicFallback, QuicUpgradeOutcome};
use crate::transport::peer_connection::UpgradeSender;
use crate::transport::peer_connection::inbound_budget::SharedInboundBudget;
use crate::transport::quic::control::ControlError;
use crate::transport::quic::identity::PeerKey;
use crate::transport::quic::path::{BoundedSend, QuicInbound, QuicPath, QuicPathLimits};
use crate::transport::quic::{QuicConnection, QuicEndpoint, QuicIncoming, QuicPeerPermit};
use crate::transport::symmetric_message::DeclineReason;
use crate::transport::symmetric_message::UpgradeControl;
use crate::transport::{
    QuicErrorKind, QuicRefusal, Socket, TRANSPORT_METRICS, normalize_mapped_addr,
};

/// Pending events of one session. Peer-sent messages are dropped when it is full
/// (`try_send`), and the session's own tasks wait for room.
const SESSION_INBOX_CAPACITY: usize = 32;

/// Peer keys the backoff store remembers. An entry is a key and a [`FiledSeed`],
/// both of fixed size (under 100 B with the LRU's links), so the store is at most
/// a few hundred KiB, whatever peers do. The least recently used entry goes
/// first; losing one costs that peer one early offer, which the peer's own
/// backoff then answers.
const SEED_STORE_ENTRIES: NonZeroUsize = NonZeroUsize::new(4096).expect("not zero");

/// A connection sends at most one `Decline` per this long. A `Decline` needs no
/// operation state and is tracked and retransmitted, so without a bound a peer
/// grows this node's memory and upload with its own, one Offer per packet id.
/// Shorter than the shortest retry after an uncharged decline
/// (`BACKOFF_MIN` less its 20% jitter, 48 s), so an honest offerer is never held
/// back and charged for a decline that names no fault.
const DECLINE_INTERVAL: Duration = Duration::from_secs(45);

/// The bound on `Decline` replies, one per connection, on the connection's clock:
/// a field of `PeerConnection` with QUIC off and of the driver with QUIC on. An
/// offerer whose reply is held back, which only a faster-than-honest one can be,
/// times out as `NoReply`, the same penalty as `Disabled`.
#[derive(Default)]
pub(in crate::transport) struct DeclineGate {
    last_nanos: Option<u64>,
}

impl DeclineGate {
    /// Whether a `Decline` may be sent now. A `false` is counted here.
    pub(in crate::transport) fn admit(&mut self, now_nanos: u64) -> bool {
        let held_back = self.last_nanos.is_some_and(|last| {
            now_nanos.saturating_sub(last) < DECLINE_INTERVAL.as_nanos() as u64
        });
        if held_back {
            TRANSPORT_METRICS.record_upgrade_decline_suppressed();
            return false;
        }
        self.last_nanos = Some(now_nanos);
        true
    }
}

/// The clock of the node's QUIC paths: the one quinn runs on. In production
/// `RealTime`, in a simulation the network's `VirtualTime`.
#[derive(Clone)]
pub(in crate::transport) enum PathClock {
    Real(RealTime),
    Virtual(VirtualTime),
}

impl TimeSource for PathClock {
    fn now_nanos(&self) -> u64 {
        match self {
            Self::Real(time) => time.now_nanos(),
            Self::Virtual(time) => time.now_nanos(),
        }
    }

    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        match self {
            Self::Real(time) => time.sleep(duration),
            Self::Virtual(time) => time.sleep(duration),
        }
    }

    fn sleep_until(&self, deadline_nanos: u64) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        match self {
            Self::Real(time) => time.sleep_until(deadline_nanos),
            Self::Virtual(time) => time.sleep_until(deadline_nanos),
        }
    }

    fn sleep_cancelable(&self, duration: Duration) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        match self {
            Self::Real(time) => time.sleep_cancelable(duration),
            Self::Virtual(time) => time.sleep_cancelable(duration),
        }
    }

    fn timeout<F, T>(
        &self,
        duration: Duration,
        future: F,
    ) -> Pin<Box<dyn Future<Output = Option<T>> + Send>>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        match self {
            Self::Real(time) => time.timeout(duration, future),
            Self::Virtual(time) => time.timeout(duration, future),
        }
    }

    fn connection_idle_timeout(&self) -> Duration {
        match self {
            Self::Real(time) => time.connection_idle_timeout(),
            Self::Virtual(time) => time.connection_idle_timeout(),
        }
    }

    fn supports_keepalive(&self) -> bool {
        match self {
            Self::Real(time) => time.supports_keepalive(),
            Self::Virtual(time) => time.supports_keepalive(),
        }
    }
}

/// A seed and the session that filed it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FiledSeed {
    session: u64,
    seed: BackoffSeed,
}

/// The backoff each peer key's last session left, for the session that replaces
/// it. Bounded by [`SEED_STORE_ENTRIES`] with fixed-size values.
struct SeedStore {
    filed: lru::LruCache<TransportPublicKey, FiledSeed>,
}

impl SeedStore {
    fn new(entries: NonZeroUsize) -> Self {
        Self {
            filed: lru::LruCache::new(entries),
        }
    }

    /// File `seed` for `key`, unless a later session already filed one: a max
    /// merge would undo the clear a stable path made, so the session number
    /// decides.
    fn file(&mut self, key: &TransportPublicKey, session: u64, seed: BackoffSeed) {
        if self
            .filed
            .peek(key)
            .is_some_and(|filed| filed.session > session)
        {
            return;
        }
        self.filed.put(key.clone(), FiledSeed { session, seed });
    }

    /// What the last session of `key` left, or the default.
    fn seed_for(&mut self, key: &TransportPublicKey) -> BackoffSeed {
        self.filed
            .get(key)
            .map(|filed| filed.seed)
            .unwrap_or_default()
    }
}

/// What a session's driver is told.
enum SessionEvent {
    /// An upgrade message arrived on the UDP session.
    Upgrade(UpgradeControl),
    /// A connection attempt from this session's address.
    Incoming(Box<QuicIncoming>),
    /// The dial or the accepted attempt of `attempt` finished its handshake.
    Connected {
        attempt: u32,
        connection: QuicConnection,
    },
    /// The dial or the accepted attempt of `attempt` failed.
    ConnectFailed { attempt: u32, kind: QuicErrorKind },
    /// A control frame arrived on the path of `attempt`.
    ControlFrame { attempt: u32, payload: Bytes },
    /// A `Ready` or `ReadyAck` send of `attempt` ended.
    ControlSent { attempt: u32, outcome: BoundedSend },
    /// The connection of `attempt` ended.
    PathClosed { attempt: u32, kind: QuicErrorKind },
    /// The machine may have a new deadline.
    Rearm,
}

/// A send the driver makes on the UDP session, after the step that asked for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UdpSend {
    /// Report `OfferSent` once it is sent, if the attempt is still live.
    Offer {
        attempt: u32,
        caps: u32,
    },
    Reply(UpgradeControl),
}

/// A path and the receiving ends of its channels. In shadow nothing arrives on
/// `shorts` and `streams`: the path refuses data before it reaches them. With this
/// node's data bit set a peer's frames wait here, charged to the peer's budget,
/// until the path ends. Holding them keeps a frame from being counted as dropped.
struct LivePath {
    path: Arc<QuicPath>,
    _shorts: mpsc::Receiver<crate::transport::quic::path::InboundShort>,
    _streams: mpsc::Receiver<crate::transport::quic::path::InboundStreamArrival>,
}

/// An attempt's place in the `quic_paths_*` gauges: counted as offered or
/// dialing from creation, as active once the machine reports the path
/// established, and out again when the attempt is dropped. A guard, so no
/// way of ending an attempt leaves a gauge high.
struct PathGauge {
    remote: SocketAddr,
    /// When the path became active, on the session clock.
    active_since: Option<Duration>,
}

impl PathGauge {
    fn new(remote: SocketAddr) -> Self {
        TRANSPORT_METRICS.quic_path_attempting(remote);
        Self {
            remote,
            active_since: None,
        }
    }

    fn activate(&mut self, now: Duration) {
        if self.active_since.is_none() {
            self.active_since = Some(now);
            TRANSPORT_METRICS.quic_path_activated(self.remote);
        }
    }
}

impl Drop for PathGauge {
    fn drop(&mut self) {
        TRANSPORT_METRICS.quic_path_left(self.active_since.is_some(), self.remote);
    }
}

/// What one attempt holds. Dropping it drops all of it.
struct Attempt {
    id: u32,
    gauge: PathGauge,
    /// The server's `allow_peer` permit.
    allow: Option<QuicPeerPermit>,
    /// A routed connection attempt is finishing its handshake.
    establishing: bool,
    path: Option<LivePath>,
    /// The dial or establish task, the control sends and the path's pump.
    tasks: Vec<JoinHandle<()>>,
}

impl Attempt {
    fn new(id: u32, remote: SocketAddr) -> Self {
        Self {
            id,
            gauge: PathGauge::new(remote),
            allow: None,
            establishing: false,
            path: None,
            tasks: Vec::new(),
        }
    }
}

impl Drop for Attempt {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

struct SessionState {
    machine: UpgradeMachine,
    attempt: Option<Attempt>,
    /// The key the node promoted the session under.
    peer_key: Option<TransportPublicKey>,
    ended: bool,
    /// When this node sent its Offer or Accept, on the session clock, until
    /// the path is established.
    setup_started: Option<Duration>,
}

/// One UDP session's upgrade. The driver, the connection and a newer session of
/// the same peer key can each reach it; the state's lock is never held across an
/// await.
struct Session {
    /// Order of creation in this node. See [`SeedStore::file`].
    id: u64,
    remote: SocketAddr,
    remote_version: Option<(u8, u8, u16)>,
    hub: Arc<QuicUpgrades>,
    inbox: mpsc::Sender<SessionEvent>,
    budget: SharedInboundBudget,
    /// When an active path of this session last heard from the peer, on [`Self::now`].
    /// Half of the connection's `ReceiptStamp`. Written from the path's watch
    /// because the path runs on another clock in simulation.
    quic_receipt_nanos: Arc<AtomicU64>,
    /// The connection's `TimeSource::now`, the listener's clock.
    now: Arc<dyn Fn() -> Duration + Send + Sync>,
    state: Mutex<SessionState>,
}

/// The node's upgrades. See the module docs.
pub(in crate::transport) struct QuicUpgrades {
    endpoint: QuicEndpoint,
    policy: QuicPolicy,
    local_key: TransportPublicKey,
    path_clock: PathClock,
    /// The session of each address. An attempt is routed here, and a session
    /// removes only its own entry when it ends.
    routes: DashMap<SocketAddr, (u64, mpsc::Sender<SessionEvent>)>,
    /// The session each key was last promoted under. A session removes only its
    /// own entry when it ends.
    by_key: DashMap<TransportPublicKey, Weak<Session>>,
    seeds: Mutex<SeedStore>,
    next_session: AtomicU64,
    /// Peer-sent upgrade messages dropped because an inbox was full. Only
    /// throttles the warning; the exported count is
    /// `quic_upgrade_inbox_overflow`.
    inbox_overflow: AtomicU64,
    #[cfg(any(test, feature = "testing"))]
    trace: Option<super::sim_trace::NodeTrace>,
}

impl QuicUpgrades {
    pub(in crate::transport) fn new(
        endpoint: QuicEndpoint,
        policy: QuicPolicy,
        local_key: TransportPublicKey,
        path_clock: PathClock,
        #[cfg(any(test, feature = "testing"))] trace: Option<super::sim_trace::NodeTrace>,
    ) -> Arc<Self> {
        #[cfg(any(test, feature = "testing"))]
        if let Some(trace) = &trace {
            trace.register_permits(policy.permits().clone());
        }
        Arc::new(Self {
            endpoint,
            policy,
            local_key,
            path_clock,
            routes: DashMap::new(),
            by_key: DashMap::new(),
            seeds: Mutex::new(SeedStore::new(SEED_STORE_ENTRIES)),
            next_session: AtomicU64::new(1),
            inbox_overflow: AtomicU64::new(0),
            #[cfg(any(test, feature = "testing"))]
            trace,
        })
    }

    /// Start the task that routes each connection attempt from `attempts` to the
    /// session of its address. It holds this value weakly, so it does not keep
    /// the endpoint alive, and it parks when `attempts` closes, as the accept loop
    /// does, since the monitor reads any end of a registered task as a failure.
    pub(in crate::transport) fn spawn_attempt_router(
        self: &Arc<Self>,
        registrar: &LateRegistrar,
        mut attempts: mpsc::Receiver<QuicIncoming>,
    ) {
        let hub = Arc::downgrade(self);
        let handle = GlobalExecutor::spawn(async move {
            while let Some(incoming) = attempts.recv().await {
                match hub.upgrade() {
                    Some(hub) => hub.route(incoming),
                    None => incoming.ignore(),
                }
            }
            tracing::debug!("QUIC attempt router stopped; parking");
            drop(attempts);
            std::future::pending::<()>().await;
        });
        registrar.register("quic_attempt_router", handle);
    }

    /// Hand `incoming` to the session of its address, without waiting. With no
    /// session, or one whose inbox is full, the attempt is ignored and counted:
    /// the client retransmits its Initial, as after a lost datagram.
    fn route(&self, incoming: QuicIncoming) {
        let addr = incoming.source();
        let sender = self.routes.get(&addr).map(|route| route.value().1.clone());
        let unrouted = match sender {
            None => Some(incoming),
            Some(sender) => match sender.try_send(SessionEvent::Incoming(Box::new(incoming))) {
                Ok(()) => None,
                Err(
                    mpsc::error::TrySendError::Full(event)
                    | mpsc::error::TrySendError::Closed(event),
                ) => match event {
                    SessionEvent::Incoming(incoming) => Some(*incoming),
                    SessionEvent::Upgrade(_)
                    | SessionEvent::Connected { .. }
                    | SessionEvent::ConnectFailed { .. }
                    | SessionEvent::ControlFrame { .. }
                    | SessionEvent::ControlSent { .. }
                    | SessionEvent::PathClosed { .. }
                    | SessionEvent::Rearm => None,
                },
            },
        };
        if let Some(incoming) = unrouted {
            TRANSPORT_METRICS.record_quic_upgrade(QuicUpgradeOutcome::AttemptUnrouted, addr);
            tracing::debug!(%addr, "Ignoring a QUIC connection attempt no session wanted");
            incoming.ignore();
        }
    }

    /// Start the upgrade of a new UDP session.
    pub(in crate::transport) fn open_session<S: Socket, T: TimeSource>(
        self: &Arc<Self>,
        remote: SocketAddr,
        remote_version: Option<(u8, u8, u16)>,
        budget: SharedInboundBudget,
        quic_receipt_nanos: Arc<AtomicU64>,
        sender: UpgradeSender<S, T>,
        time: T,
    ) -> QuicPathState {
        let (inbox, inbox_rx) = mpsc::channel(SESSION_INBOX_CAPACITY);
        let id = self.next_session.fetch_add(1, Ordering::Relaxed);
        let remote = normalize_mapped_addr(remote);
        // A connection replaced at this address has not necessarily dropped yet.
        // Its session removes only its own route, so the new one stands.
        self.routes.insert(remote, (id, inbox.clone()));
        let clock = time.clone();
        let session = Arc::new(Session {
            id,
            remote,
            remote_version,
            hub: Arc::clone(self),
            inbox,
            budget,
            quic_receipt_nanos,
            now: Arc::new(move || clock.now()),
            state: Mutex::new(SessionState {
                machine: UpgradeMachine::new(self.local_key.clone()),
                attempt: None,
                peer_key: None,
                ended: false,
                setup_started: None,
            }),
        });
        let driver = GlobalExecutor::spawn(report_panics(
            remote,
            drive(Arc::clone(&session), inbox_rx, sender, time),
        ));
        QuicPathState { session, driver }
    }

    /// Whether a test made this node's record of its peers' keys wrong.
    fn record_is_wrong(&self) -> bool {
        #[cfg(any(test, feature = "testing"))]
        if let Some(trace) = &self.trace {
            return trace.wrong_record();
        }
        false
    }

    /// Deliver to this node an Initial with `source` as its source address,
    /// when a test asked for one on each allowed attempt.
    #[cfg(any(test, feature = "testing"))]
    fn send_test_initial(&self, source: SocketAddr) {
        let Some(trace) = self.trace.as_ref().filter(|t| t.initial_on_each_attempt()) else {
            return;
        };
        let datagram = self
            .endpoint
            .initial_datagram(trace.addr(), &self.local_key);
        trace.deliver_to_node(datagram, source);
    }

    /// The peer's version as the machine reads it.
    fn peer_version(&self, reported: Option<(u8, u8, u16)>) -> Option<(u8, u8, u16)> {
        #[cfg(any(test, feature = "testing"))]
        if let Some(trace) = &self.trace {
            return trace.peer_version(reported);
        }
        reported
    }

    fn file_seed(&self, key: &TransportPublicKey, session: u64, seed: BackoffSeed) {
        self.seeds.lock().file(key, session, seed);
    }
}

impl Session {
    /// Take `event` on the machine. An ended session takes nothing.
    fn handle(&self, event: SessionEvent) -> Vec<UdpSend> {
        let mut state = self.state.lock();
        if state.ended {
            match event {
                SessionEvent::Incoming(incoming) => incoming.ignore(),
                SessionEvent::Connected { connection, .. } => {
                    connection.close(QuicCloseCode::Downgrade)
                }
                SessionEvent::Upgrade(_)
                | SessionEvent::ConnectFailed { .. }
                | SessionEvent::ControlFrame { .. }
                | SessionEvent::ControlSent { .. }
                | SessionEvent::PathClosed { .. }
                | SessionEvent::Rearm => {}
            }
            return Vec::new();
        }
        let actions = match event {
            SessionEvent::Upgrade(control) => self.step(&mut state, Event::Upgrade(control)),
            SessionEvent::Incoming(incoming) => {
                self.on_incoming(&mut state, *incoming);
                Vec::new()
            }
            SessionEvent::Connected {
                attempt,
                connection,
            } => self.on_connected(&mut state, attempt, connection),
            SessionEvent::ConnectFailed { attempt, kind }
            | SessionEvent::PathClosed { attempt, kind } => {
                self.step(&mut state, Event::PathFailed { attempt, kind })
            }
            SessionEvent::ControlFrame { attempt, payload } => {
                self.on_control_frame(&mut state, attempt, &payload)
            }
            SessionEvent::ControlSent { attempt, outcome } => {
                self.step(&mut state, Event::ControlSent { attempt, outcome })
            }
            SessionEvent::Rearm => Vec::new(),
        };
        self.apply(&mut state, actions)
    }

    /// Step the machine with no I/O beyond what the actions need, for an event
    /// that comes from the driver itself.
    fn take(&self, event: Event) -> Vec<UdpSend> {
        let mut state = self.state.lock();
        if state.ended {
            return Vec::new();
        }
        let actions = self.step(&mut state, event);
        self.apply(&mut state, actions)
    }

    fn step(&self, state: &mut SessionState, event: Event) -> Vec<Action> {
        let inputs = Inputs {
            now: (self.now)(),
            sample: GlobalRng::random_range(0.0..1.0),
            policy: &self.hub.policy,
        };
        state.machine.step(inputs, event)
    }

    /// Carry out `actions`, in order, and return the sends for the driver to
    /// make. Nothing here waits: `allow_peer`, spawning and closing do not.
    fn apply(&self, state: &mut SessionState, actions: Vec<Action>) -> Vec<UdpSend> {
        let mut queue = VecDeque::from(actions);
        let mut sends = Vec::new();
        while let Some(action) = queue.pop_front() {
            match action {
                Action::AllowPeerThenOffer { attempt, caps } => {
                    match self.allow_peer(state, attempt) {
                        Ok(()) => sends.push(UdpSend::Offer { attempt, caps }),
                        Err(kind) => {
                            queue.extend(self.step(state, Event::PathFailed { attempt, kind }))
                        }
                    }
                }
                Action::SendUpgrade(control) => sends.push(UdpSend::Reply(control)),
                Action::Dial { attempt } => self.dial(state, attempt),
                Action::SendControl(control) => self.send_control(state, control),
                Action::EnableData { attempt } => self.enable_data(state, attempt),
                Action::TearDown { attempt, code } => {
                    // A later step of this same batch must not send it.
                    sends.retain(|send| {
                        !matches!(send, UdpSend::Offer { attempt: offered, .. } if *offered == attempt)
                    });
                    self.tear_down(state, attempt, code);
                }
                Action::Record(outcome) => {
                    tracing::debug!(peer = %self.remote, session = self.id, ?outcome, "QUIC upgrade");
                    self.note(|| format!("{outcome:?}"));
                    self.count(state, outcome);
                }
            }
        }
        sends
    }

    /// Count what the machine decided. Exhaustive, so a new outcome does not
    /// compile until it has a counter.
    fn count(&self, state: &mut SessionState, outcome: Outcome) {
        use QuicUpgradeOutcome as C;
        let now = (self.now)();
        let count = |counter| TRANSPORT_METRICS.record_quic_upgrade(counter, self.remote);
        let declined = |reason: Option<DeclineReason>| match reason {
            Some(DeclineReason::Disabled) => C::DeclinedDisabled,
            Some(DeclineReason::Backoff) => C::DeclinedBackoff,
            Some(DeclineReason::Busy) => C::DeclinedBusy,
            Some(DeclineReason::NotReady) => C::DeclinedNotReady,
            None => C::DeclinedUnknown,
        };
        match outcome {
            Outcome::Offered => {
                state.setup_started = Some(now);
                count(C::Offered);
            }
            Outcome::Accepted => count(C::Accepted),
            Outcome::AcceptSent => {
                state.setup_started = Some(now);
                count(C::AcceptSent);
            }
            Outcome::Established(role) => {
                count(C::Established);
                if role == Role::Server {
                    count(C::EstablishedAsOfferer);
                }
                if let Some(started) = state.setup_started.take() {
                    TRANSPORT_METRICS
                        .record_quic_establish_latency(now.saturating_sub(started), self.remote);
                }
                if let Some(held) = state.attempt.as_mut() {
                    held.gauge.activate(now);
                    if let Some(live) = &held.path {
                        live.path.report_receipts_to(self.receipt_writer());
                    }
                }
            }
            // No counter yet, and at a share of 0 it does not happen. Not in
            // `QuicUpgradeOutcome`: that array is at 32, the longest serde and
            // `Default` take, so a data count needs a labelled set of its own.
            Outcome::DataEnabled => {}
            Outcome::Declined { reason, uncharged } => {
                count(declined(reason));
                if uncharged {
                    count(C::DeclinedUncharged);
                }
            }
            // Counted where the decline passes the rate gate, since the gate
            // can hold it back after the machine decided (`count_decline_sent`).
            Outcome::DeclineSent(_) => {}
            Outcome::IneligibleVersion => count(C::IneligibleVersion),
            Outcome::NoReply => count(C::NoReply),
            Outcome::Timeout => count(C::Timeout),
            Outcome::HandshakeFailed => count(C::HandshakeFailed),
            Outcome::IdentityMismatch => count(C::IdentityMismatch),
            Outcome::RoleConflict => count(C::RoleConflict),
            Outcome::AllowSetFull => count(C::AllowSetFull),
            Outcome::KeyConflict => count(C::KeyConflict),
            Outcome::ControlRefused => count(C::ControlRefused),
            Outcome::SetupDowngraded => count(C::SetupDowngraded),
            Outcome::SetupEndpointDown => count(C::SetupEndpointDown),
            Outcome::SessionEnded { was_active } => count(if was_active {
                C::SessionEndedActive
            } else {
                C::SessionEndedSetup
            }),
            Outcome::Fallback(cause) => TRANSPORT_METRICS.record_quic_fallback(
                match cause {
                    FallbackCause::Idle => QuicFallback::Idle,
                    FallbackCause::Reset => QuicFallback::Reset,
                    FallbackCause::Protocol => QuicFallback::Protocol,
                    FallbackCause::DowngradeReceived => QuicFallback::DowngradeReceived,
                    FallbackCause::EndpointDown => QuicFallback::EndpointDown,
                },
                self.remote,
            ),
        }
    }

    /// Count a `Decline` this node sends, by reason. Called after the decline
    /// rate gate admits it, so a held-back decline is not counted.
    fn count_decline_sent(&self, reason: u8) {
        use QuicUpgradeOutcome as C;
        let counter = match DeclineReason::from_wire(reason) {
            Some(DeclineReason::Disabled) => C::DeclineSentDisabled,
            Some(DeclineReason::Backoff) => C::DeclineSentBackoff,
            Some(DeclineReason::Busy) => C::DeclineSentBusy,
            Some(DeclineReason::NotReady) => C::DeclineSentNotReady,
            // This node only sends reasons it knows.
            None => return,
        };
        TRANSPORT_METRICS.record_quic_upgrade(counter, self.remote);
    }

    /// Put the promoted key on the allow-set for `attempt`. The key comes only
    /// from the node's record: a machine with a role has one.
    fn allow_peer(&self, state: &mut SessionState, attempt: u32) -> Result<(), QuicErrorKind> {
        let Some(key) = state.peer_key.as_ref() else {
            return Err(QuicErrorKind::Refused {
                reason: QuicRefusal::Local,
            });
        };
        let permit = self
            .hub
            .endpoint
            .allow_peer(self.remote, key)
            .map_err(|error| self.hub.endpoint.error_kind(error))?;
        let mut held = Attempt::new(attempt, self.remote);
        held.allow = Some(permit);
        state.attempt = Some(held);
        #[cfg(any(test, feature = "testing"))]
        self.hub.send_test_initial(self.remote);
        Ok(())
    }

    fn dial(&self, state: &mut SessionState, attempt: u32) {
        let Some(key) = state.peer_key.clone() else {
            return;
        };
        let hub = Arc::clone(&self.hub);
        let inbox = self.inbox.clone();
        let remote = self.remote;
        let mut held = Attempt::new(attempt, self.remote);
        held.tasks.push(GlobalExecutor::spawn(async move {
            let event = match hub.endpoint.connect(remote, &key).await {
                Ok(connection) => SessionEvent::Connected {
                    attempt,
                    connection,
                },
                Err(error) => SessionEvent::ConnectFailed {
                    attempt,
                    kind: hub.endpoint.error_kind(error),
                },
            };
            report(&inbox, event).await;
        }));
        state.attempt = Some(held);
    }

    /// Finish the handshake of an attempt from this session's address, if the
    /// live attempt is waiting for one. The machine's deadline bounds the task:
    /// its `TearDown` drops the attempt, which aborts the task.
    ///
    /// A UDP source address proves nothing, and only one attempt can take the
    /// slot, so an attempt that has not returned a Retry token is answered with
    /// a Retry and leaves the slot open. The client answers the Retry inside
    /// quinn, within its dial, and its next Initial arrives here validated. That
    /// costs one round trip, on the same deadline.
    fn on_incoming(&self, state: &mut SessionState, incoming: QuicIncoming) {
        let waiting = state
            .attempt
            .as_mut()
            .filter(|held| held.allow.is_some() && held.path.is_none() && !held.establishing);
        let Some(held) = waiting else {
            TRANSPORT_METRICS.record_quic_upgrade(QuicUpgradeOutcome::AttemptUnrouted, self.remote);
            incoming.ignore();
            return;
        };
        if !incoming.address_validated() {
            TRANSPORT_METRICS.record_quic_upgrade(QuicUpgradeOutcome::AttemptRetried, self.remote);
            self.note(|| "retried an unvalidated attempt".to_string());
            incoming.retry();
            return;
        }
        held.establishing = true;
        let attempt = held.id;
        let hub = Arc::clone(&self.hub);
        let inbox = self.inbox.clone();
        held.tasks.push(GlobalExecutor::spawn(async move {
            let event = match incoming.establish().await {
                Ok(connection) => SessionEvent::Connected {
                    attempt,
                    connection,
                },
                Err(error) => SessionEvent::ConnectFailed {
                    attempt,
                    kind: hub.endpoint.error_kind(error),
                },
            };
            report(&inbox, event).await;
        }));
    }

    /// A connection arrived. One for anything but the live attempt is stale, and
    /// closed: attempt ids restart per session.
    fn on_connected(
        &self,
        state: &mut SessionState,
        attempt: u32,
        connection: QuicConnection,
    ) -> Vec<Action> {
        let live = state.machine.live_attempt() == Some(attempt)
            && state
                .attempt
                .as_ref()
                .is_some_and(|held| held.id == attempt && held.path.is_none());
        if !live {
            self.note(|| format!("stale connection of attempt {attempt}"));
            connection.close(QuicCloseCode::Downgrade);
            return Vec::new();
        }
        let matches_record = !self.hub.record_is_wrong()
            && state
                .peer_key
                .as_ref()
                .and_then(|key| PeerKey::of(key).ok())
                .is_some_and(|record| record == *connection.peer_key());
        self.build_path(state, attempt, connection);
        let event = if matches_record {
            Event::PathEstablished { attempt }
        } else {
            tracing::warn!(
                peer = %self.remote,
                session = self.id,
                "The key proved on a QUIC path is not the key on record; closing the path"
            );
            Event::IdentityMismatch { attempt }
        };
        self.step(state, event)
    }

    /// What an active path calls when it hears from the peer.
    fn receipt_writer(&self) -> Box<dyn Fn() + Send + Sync> {
        let stamp = Arc::clone(&self.quic_receipt_nanos);
        let now = Arc::clone(&self.now);
        Box::new(move || stamp.store(now().as_nanos() as u64, Ordering::Release))
    }

    fn enable_data(&self, state: &SessionState, attempt: u32) {
        debug_assert!(state.machine.carries_data(), "attempt {attempt}");
        let live = state
            .attempt
            .as_ref()
            .filter(|held| held.id == attempt)
            .and_then(|held| held.path.as_ref());
        if let Some(live) = live {
            live.path.enable_data();
        }
    }

    fn build_path(&self, state: &mut SessionState, attempt: u32, connection: QuicConnection) {
        // At once, not when the peer's bit arrives: the peer's first stream can
        // overtake the frame that carries it.
        let limits = if state.machine.accepts_data() {
            QuicPathLimits::DEFAULT
        } else {
            QuicPathLimits::SHADOW
        };
        let (path, inbound) = QuicPath::with_budget(
            connection,
            self.hub.path_clock.clone(),
            limits,
            self.budget.clone(),
        );
        let QuicInbound {
            shorts,
            streams,
            controls,
        } = inbound;
        let pump = spawn_pump(
            attempt,
            self.remote,
            path.quinn_connection(),
            controls,
            Arc::clone(&self.hub),
            self.inbox.clone(),
        );
        if let Some(held) = state.attempt.as_mut() {
            held.tasks.push(pump);
            held.path = Some(LivePath {
                path: Arc::new(path),
                _shorts: shorts,
                _streams: streams,
            });
        }
    }

    fn on_control_frame(
        &self,
        state: &mut SessionState,
        attempt: u32,
        payload: &[u8],
    ) -> Vec<Action> {
        let Some(path) = state
            .attempt
            .as_ref()
            .filter(|held| held.id == attempt)
            .and_then(|held| held.path.as_ref())
        else {
            return Vec::new();
        };
        let decoded: Result<_, ControlError> = path.path.decode_control(payload);
        match decoded {
            Ok(control) => self.step(state, Event::Control(control)),
            Err(error) => self.step(state, Event::ControlRefused { attempt, error }),
        }
    }

    /// Send a control frame on the live attempt's path, bounded, in a task. The
    /// only kind of send a shadow path makes.
    fn send_control(
        &self,
        state: &mut SessionState,
        control: crate::transport::quic::control::QuicControl,
    ) {
        let attempt = control.attempt();
        let Some(held) = state.attempt.as_mut().filter(|held| held.id == attempt) else {
            return;
        };
        let Some(live) = held.path.as_ref() else {
            return;
        };
        let path = Arc::clone(&live.path);
        let clock = self.hub.path_clock.clone();
        let inbox = self.inbox.clone();
        held.tasks.push(GlobalExecutor::spawn(async move {
            let outcome = path
                .send_control_bounded(&clock, CONTROL_SEND_TIMEOUT, control)
                .await;
            report(&inbox, SessionEvent::ControlSent { attempt, outcome }).await;
        }));
    }

    fn tear_down(&self, state: &mut SessionState, attempt: u32, code: QuicCloseCode) {
        let Some(held) = state.attempt.take_if(|held| held.id == attempt) else {
            return;
        };
        // Only the teardown of an active path ends one, so this is where its
        // age is known.
        if let Some(since) = held.gauge.active_since {
            TRANSPORT_METRICS
                .record_quic_path_closed((self.now)().saturating_sub(since), self.remote);
        }
        if let Some(live) = &held.path {
            live.path.close(code);
            self.note(|| format!("closed path with {code:?}"));
            self.note(|| {
                let refused = live.path.stats().count(
                    crate::transport::quic::path::QuicInboundOutcome::RefusedNotAcceptingData,
                );
                format!(
                    "path carried data: refused {refused}, delivered {}",
                    live._shorts.len() + live._streams.len()
                )
            });
        }
    }

    /// When the machine next needs a `Tick`, in nanoseconds of the session's clock.
    fn deadline_nanos(&self) -> Option<u64> {
        let state = self.state.lock();
        if state.ended {
            return None;
        }
        state.machine.deadline().map(|at| at.as_nanos() as u64)
    }

    /// Whether `attempt` still holds what its `allow_peer` gave it.
    fn attempt_is_live(&self, attempt: u32) -> bool {
        let state = self.state.lock();
        !state.ended
            && state.machine.live_attempt() == Some(attempt)
            && state
                .attempt
                .as_ref()
                .is_some_and(|held| held.id == attempt)
    }

    /// The node promoted this session under `key`: end any older session of the
    /// key, which files its seed, then start this one with the seed filed for it.
    /// Not waiting: a session is reached through its lock, and every step here is
    /// a local close or a map update.
    fn promote(self: &Arc<Self>, key: TransportPublicKey) {
        // The compare and the write are one step under the entry's guard, so of
        // any number of sessions of a key the newest stands. `peer_key` is set
        // under the guard too: `end` finds this entry through it, and waits for
        // the guard, so an entry written here is never left behind.
        let entry = self.hub.by_key.entry(key.clone());
        {
            let mut state = self.state.lock();
            if state.ended {
                return;
            }
            state.peer_key = Some(key.clone());
        }
        let older = match entry {
            Entry::Vacant(slot) => {
                slot.insert(Arc::downgrade(self));
                None
            }
            Entry::Occupied(mut slot) => {
                let live = slot.get().upgrade().filter(|held| !held.state.lock().ended);
                match live {
                    Some(held) if held.id > self.id => {
                        // A newer session of this key got here first, so this one
                        // is the older of the two, and it ends. It files nothing:
                        // it has taken no seed.
                        drop(slot);
                        self.end();
                        return;
                    }
                    Some(held) if held.id == self.id => None,
                    held => {
                        slot.insert(Arc::downgrade(self));
                        held
                    }
                }
            }
        };
        if let Some(older) = older {
            older.end();
        }
        let backoff = self.hub.seeds.lock().seed_for(&key);
        let own_caps = self.hub.policy.own_caps(&self.hub.local_key, &key);
        let sends = {
            let mut state = self.state.lock();
            if state.ended {
                return;
            }
            let actions = self.step(
                &mut state,
                Event::Promoted {
                    peer_key: key,
                    remote_version: self.hub.peer_version(self.remote_version),
                    backoff,
                    own_caps,
                },
            );
            self.apply(&mut state, actions)
        };
        debug_assert!(sends.is_empty(), "a promotion sends nothing: {sends:?}");
        // Lossy on purpose: a full inbox means the driver is about to go round its
        // loop, and it reads the deadline each time.
        let _unused = self.inbox.try_send(SessionEvent::Rearm);
    }

    /// The UDP session is gone or replaced: its path ends with it, and what the
    /// machine leaves is filed for the next session of the key. Runs once.
    fn end(&self) {
        {
            let mut state = self.state.lock();
            if state.ended {
                return;
            }
            let actions = self.step(&mut state, Event::SessionEnded);
            let sends = self.apply(&mut state, actions);
            debug_assert!(
                sends.is_empty(),
                "ending a session sends nothing: {sends:?}"
            );
            state.ended = true;
            if let (Some(seed), Some(key)) = (state.machine.backoff_seed(), state.peer_key.as_ref())
            {
                self.hub.file_seed(key, self.id, seed);
            }
            state.attempt = None;
        }
        self.hub
            .routes
            .remove_if(&self.remote, |_, route| route.0 == self.id);
        let key = self.state.lock().peer_key.clone();
        if let Some(key) = key {
            self.hub.by_key.remove_if(&key, |_, held| {
                held.upgrade().is_none_or(|held| held.id == self.id)
            });
        }
        self.note(|| "session ended".to_string());
    }

    /// Write a line to a test's trace, when it has one.
    fn note(&self, what: impl FnOnce() -> String) {
        #[cfg(any(test, feature = "testing"))]
        if let Some(trace) = &self.hub.trace {
            trace.record(self.remote, self.id, what());
        }
        #[cfg(not(any(test, feature = "testing")))]
        let _ = what;
    }
}

/// Tell a session's driver what a helper task found. A helper is not an event
/// loop: it is aborted with its attempt, nothing else waits for it, and the
/// driver does not wait for helpers, so waiting for room here cannot stall
/// anything else (channel-safety.md, same-runtime internal consumers).
async fn report(inbox: &mpsc::Sender<SessionEvent>, event: SessionEvent) {
    // channel-safety: ok, per the doc comment above.
    if inbox.send(event).await.is_err() {
        tracing::trace!("QUIC session already gone");
    }
}

/// Run `task`. If it panics, count that and raise the same panic again, so the
/// runtime sees what it saw before.
async fn report_panics(remote: SocketAddr, task: impl Future<Output = ()>) {
    use futures::FutureExt;
    if let Err(payload) = std::panic::AssertUnwindSafe(task).catch_unwind().await {
        TRANSPORT_METRICS.record_quic_upgrade(QuicUpgradeOutcome::TaskPanicked, remote);
        std::panic::resume_unwind(payload);
    }
}

/// Forward a path's control frames, and its end, to the session's driver.
fn spawn_pump(
    attempt: u32,
    remote: SocketAddr,
    connection: quinn::Connection,
    mut controls: mpsc::Receiver<Bytes>,
    hub: Arc<QuicUpgrades>,
    inbox: mpsc::Sender<SessionEvent>,
) -> JoinHandle<()> {
    GlobalExecutor::spawn(report_panics(remote, async move {
        let mut open = true;
        loop {
            crate::deterministic_select! {
                frame = controls.recv(), if open => {
                    match frame {
                        Some(payload) => {
                            report(&inbox, SessionEvent::ControlFrame { attempt, payload }).await;
                        }
                        None => open = false,
                    }
                },
                reason = connection.closed() => {
                    let kind = hub.endpoint.error_kind(reason);
                    report(&inbox, SessionEvent::PathClosed { attempt, kind }).await;
                    return;
                },
            }
        }
    }))
}

/// Ends the session when the driver stops for any reason: the connection dropped
/// it, or it panicked, in which case the path must not outlive it.
struct EndOnExit(Arc<Session>);

impl Drop for EndOnExit {
    fn drop(&mut self) {
        self.0.end();
    }
}

/// Run one session's machine until the connection drops it.
async fn drive<S: Socket, T: TimeSource>(
    session: Arc<Session>,
    mut inbox: mpsc::Receiver<SessionEvent>,
    sender: UpgradeSender<S, T>,
    time: T,
) {
    let _end = EndOnExit(Arc::clone(&session));
    let mut availability = session.hub.endpoint.availability();
    let mut watching_endpoint = true;
    let mut timer: Option<(u64, crate::simulation::BoxedSleep)> = None;
    let mut declines = DeclineGate::default();
    loop {
        let deadline = session.deadline_nanos();
        if timer.as_ref().map(|(at, _)| *at) != deadline {
            timer = deadline.map(|at| (at, time.sleep_until(at)));
        }
        let armed = timer.is_some();
        let sends = crate::deterministic_select! {
            event = inbox.recv() => {
                match event {
                    Some(event) => session.handle(event),
                    // The session holds a sender, so this does not happen while
                    // it is alive.
                    None => return,
                }
            },
            availability = availability.not_up(), if watching_endpoint => {
                watching_endpoint = false;
                session.take(Event::EndpointNotUp(availability))
            },
            _ = std::future::poll_fn(|cx| match timer.as_mut() {
                Some((_, sleep)) => sleep.as_mut().poll(cx),
                None => std::task::Poll::Pending,
            }), if armed => {
                timer = None;
                session.take(Event::Tick)
            },
        };
        let mut sends = VecDeque::from(sends);
        while let Some(send) = sends.pop_front() {
            match send {
                UdpSend::Offer { attempt, caps } => {
                    // The session may have been ended by a newer one, from another
                    // task, since the step that asked for this. The check is not
                    // atomic with the send, so one Offer per attempt can still go
                    // out for a session just ended. Its allow-set entry is gone,
                    // so the peer's dial fails as after a lost datagram.
                    if !session.attempt_is_live(attempt) {
                        continue;
                    }
                    let offer = UpgradeControl::Offer { attempt, caps };
                    session.note(|| format!("sent {offer:?}"));
                    match sender.send(offer).await {
                        // Before the driver reads the inbox again, so no reply
                        // to this offer can be stepped first.
                        Ok(()) => sends.extend(session.take(Event::OfferSent { attempt })),
                        // No `OfferSent`, so the machine's Offer deadline still
                        // runs: a send that failed here is charged as `NoReply`.
                        Err(error) => {
                            tracing::debug!(%error, attempt, "QUIC upgrade offer not sent");
                        }
                    }
                }
                UdpSend::Reply(control) => {
                    if matches!(control, UpgradeControl::Decline { .. })
                        && !declines.admit(time.now_nanos())
                    {
                        session.note(|| format!("held back {control:?}"));
                        continue;
                    }
                    if let UpgradeControl::Decline { reason, .. } = control {
                        session.count_decline_sent(reason);
                    }
                    session.note(|| format!("sent {control:?}"));
                    if let Err(error) = sender.send(control).await {
                        tracing::debug!(%error, ?control, "QUIC upgrade reply not sent");
                    }
                }
            }
        }
    }
}

/// How a new connection joins its node's upgrades. With QUIC off, or with an
/// endpoint that could not be built, it holds no upgrades. Holds them weakly: the
/// listener owns them, and a handler must not keep the endpoint alive after it.
#[derive(Clone, Default)]
pub(in crate::transport) struct UpgradeSite {
    upgrades: Weak<QuicUpgrades>,
    #[cfg(any(test, feature = "testing"))]
    trace: Option<super::sim_trace::NodeTrace>,
}

impl UpgradeSite {
    pub(in crate::transport) fn new(
        upgrades: Option<&Arc<QuicUpgrades>>,
        #[cfg_attr(not(any(test, feature = "testing")), allow(unused_variables))]
        local_addr: SocketAddr,
    ) -> Self {
        Self {
            upgrades: upgrades.map_or_else(Weak::new, Arc::downgrade),
            #[cfg(any(test, feature = "testing"))]
            trace: crate::transport::in_memory_socket::quic_upgrade_trace_at(local_addr),
        }
    }

    pub(in crate::transport) fn upgrades(&self) -> Option<Arc<QuicUpgrades>> {
        self.upgrades.upgrade()
    }

    /// Where this node's upgrade messages are recorded for a test.
    #[cfg(any(test, feature = "testing"))]
    pub(in crate::transport) fn trace(&self) -> Option<super::sim_trace::NodeTrace> {
        self.trace.clone()
    }
}

/// A connection's QUIC upgrade: the path, once there is one, and what drives it.
/// A field of `PeerConnection`, declared before its inbound budget, so the path
/// on that budget goes first. Dropping this ends the session.
pub(in crate::transport) struct QuicPathState {
    session: Arc<Session>,
    driver: JoinHandle<()>,
}

impl QuicPathState {
    /// An upgrade message arrived on the UDP session. Never waits: `recv` calls
    /// this. A message dropped on a full inbox costs what a lost datagram costs,
    /// and the machine's deadlines cover that.
    pub(in crate::transport) fn on_upgrade(&self, control: UpgradeControl) {
        if let Err(error) = self.session.inbox.try_send(SessionEvent::Upgrade(control)) {
            if matches!(error, mpsc::error::TrySendError::Full(_)) {
                TRANSPORT_METRICS
                    .record_quic_upgrade(QuicUpgradeOutcome::InboxOverflow, self.session.remote);
            }
            let count = self
                .session
                .hub
                .inbox_overflow
                .fetch_add(1, Ordering::Relaxed)
                + 1;
            if count == 1 || count % 1024 == 0 {
                tracing::warn!(
                    peer = %self.session.remote,
                    dropped = count,
                    full = matches!(error, mpsc::error::TrySendError::Full(_)),
                    "QUIC upgrade inbox refused a message; dropping it"
                );
            }
        }
    }

    /// The node promoted this connection under `key` and the transport took it.
    pub(in crate::transport) fn promote(&self, key: TransportPublicKey) {
        self.session.promote(key);
    }

    /// Hand the driver what a path's pump hands it for a control frame, and the
    /// path's end.
    #[cfg(test)]
    pub(in crate::transport) fn inject_quic_inbound(&self) {
        for event in [
            SessionEvent::ControlFrame {
                attempt: 1,
                payload: Bytes::from_static(&[1, 0, 0, 0, 0, 1, 0, 0, 0, 0]),
            },
            SessionEvent::PathClosed {
                attempt: 1,
                kind: QuicErrorKind::TimedOut,
            },
        ] {
            assert!(self.session.inbox.try_send(event).is_ok());
        }
    }

    /// The stamp the session's active path writes.
    #[cfg(test)]
    pub(in crate::transport) fn quic_receipt_nanos(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.session.quic_receipt_nanos)
    }

    /// The key this session was promoted under, if any.
    #[cfg(test)]
    pub(in crate::transport) fn promoted_key(&self) -> Option<TransportPublicKey> {
        self.session.state.lock().peer_key.clone()
    }
}

impl Drop for QuicPathState {
    fn drop(&mut self) {
        self.session.end();
        self.driver.abort();
    }
}

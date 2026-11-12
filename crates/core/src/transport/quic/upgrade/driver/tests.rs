use super::*;
use crate::transport::crypto::TransportKeypair;

fn seed(failures: u32, retry_secs: u64) -> BackoffSeed {
    BackoffSeed {
        failures,
        retry_at: Duration::from_secs(retry_secs),
    }
}

fn store(entries: usize) -> SeedStore {
    SeedStore::new(NonZeroUsize::new(entries).unwrap())
}

/// The newer session's seed stands, whichever files first. A max merge would
/// keep the old session's count and undo the clear a stable path made.
#[test]
fn an_older_session_never_replaces_a_newer_sessions_seed() {
    let key = TransportKeypair::new().public().clone();
    let mut seeds = store(4);
    seeds.file(&key, 2, seed(0, 0));
    seeds.file(&key, 1, seed(5, 3000));
    assert_eq!(seeds.seed_for(&key), seed(0, 0));
    seeds.file(&key, 3, seed(1, 60));
    assert_eq!(seeds.seed_for(&key), seed(1, 60));
    seeds.file(&key, 3, seed(2, 120));
    assert_eq!(seeds.seed_for(&key), seed(2, 120), "a session may refile");
}

/// The store never holds more than its limit, and the least recently used key
/// goes first. A key it lost reads as no backoff.
#[test]
fn the_store_is_bounded_and_evicts_the_least_recently_used() {
    let keys: Vec<_> = (0..4)
        .map(|_| TransportKeypair::new().public().clone())
        .collect();
    let mut seeds = store(2);
    seeds.file(&keys[0], 1, seed(1, 1));
    seeds.file(&keys[1], 2, seed(2, 2));
    assert_eq!(seeds.seed_for(&keys[0]), seed(1, 1), "refreshes keys[0]");
    seeds.file(&keys[2], 3, seed(3, 3));
    assert_eq!(seeds.filed.len(), 2);
    assert_eq!(seeds.seed_for(&keys[1]), BackoffSeed::default());
    assert_eq!(seeds.seed_for(&keys[0]), seed(1, 1));
    assert_eq!(seeds.seed_for(&keys[2]), seed(3, 3));
    for key in &keys {
        seeds.file(key, 9, seed(9, 9));
        assert!(seeds.filed.len() <= 2);
    }
}

// A session's step is exercised here with no driver and a clock the test moves;
// the simulation tests run the whole thing.

use crate::config::GlobalRng;
use crate::transport::quic::QuicAvailability;
use crate::transport::quic::breaker::BreakerOutcome;
use crate::transport::quic::path::QuicInbound;
use crate::transport::quic::tests::{Pair, SEED, bounded, drive, pair};
use crate::transport::quic::upgrade::machine::{BACKOFF_MIN, OFFER_TIMEOUT};
use crate::transport::quic::upgrade::policy::QuicPolicyConfig;
use crate::transport::symmetric_message::QUIC_UPGRADE_NEGOTIATES_IN;

pub(in crate::transport) struct Harness {
    pair: Pair,
    hub: Arc<QuicUpgrades>,
    /// The peer's address and key, as the hub's node knows them.
    remote: SocketAddr,
    pub(in crate::transport) peer: TransportPublicKey,
    /// The clock of every session of the hub, in nanoseconds.
    now: Arc<AtomicU64>,
    /// What each session's paths deliver, kept open as a connection keeps it.
    inboxes: std::sync::Mutex<Vec<QuicDataInbox>>,
    /// Where the hub's paths keep their node-wide counts.
    counters: &'static crate::transport::metrics::QuicDataCounters,
    /// What each session sent on UDP from a path's queue.
    udp: Arc<RecordingUdp>,
    /// What each new session paces its data with, as a connection under a
    /// bandwidth limit passes it.
    pacer: Option<Arc<dyn SendPacer>>,
}

/// A UDP session that records the shorts it is given.
#[derive(Default)]
struct RecordingUdp {
    shorts: std::sync::Mutex<Vec<Vec<u8>>>,
}

impl UdpShorts for RecordingUdp {
    fn send_short(
        &self,
        payload: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = Result<(), crate::transport::TransportError>> + Send + '_>>
    {
        self.shorts.lock().unwrap().push(payload);
        Box::pin(std::future::ready(Ok(())))
    }
}

impl Harness {
    /// A hub whose node has the higher key, so its sessions are servers.
    pub(in crate::transport) async fn server(network: &str) -> Self {
        Self::server_with(network, "shadow", None).await
    }

    /// [`Self::server`] in this mode, with this `quic-data-percent`.
    async fn server_with(network: &str, mode: &str, data_percent: Option<&str>) -> Self {
        let net = crate::transport::test_net::subnet();
        let (a_addr, b_addr) = (net.addr(1), net.addr(2));
        let pair = pair(network, a_addr, b_addr, None).await;
        let a_is_higher = pair.a_keys.public() > pair.b_keys.public();
        let (endpoint, local, remote, peer) = if a_is_higher {
            (&pair.a, &pair.a_keys, b_addr, pair.b_keys.public())
        } else {
            (&pair.b, &pair.b_keys, a_addr, pair.a_keys.public())
        };
        let config: crate::config::NetworkApiConfig = toml::from_str("").expect("defaults");
        let mut config = config;
        config.transport_quic = Some(mode.to_string());
        config.quic_data_percent = data_percent.map(str::to_string);
        let policy = QuicPolicy::from_config(
            QuicPolicyConfig::new(&config, BreakerOutcome::NotConsulted),
            endpoint.availability(),
        );
        assert_eq!(endpoint.availability().get(), QuicAvailability::Up);
        let mut hub = QuicUpgrades::new(
            endpoint.clone(),
            policy,
            local.public().clone(),
            PathClock::Virtual(pair.time.clone()),
            None,
        );
        let counters: &'static _ =
            Box::leak(Box::new(crate::transport::metrics::QuicDataCounters::new()));
        Arc::get_mut(&mut hub)
            .expect("the hub is not shared yet")
            .test_counters = Some(counters);
        let peer = peer.clone();
        Self {
            pair,
            hub,
            remote,
            peer,
            now: Arc::new(AtomicU64::new(0)),
            inboxes: std::sync::Mutex::new(Vec::new()),
            counters,
            udp: Arc::default(),
            pacer: None,
        }
    }

    /// How a connection joins this hub, with a driver of its own.
    /// The virtual clock the hub's paths run on (`PathClock::Virtual`).
    pub(in crate::transport) fn time(&self) -> VirtualTime {
        self.pair.time.clone()
    }

    pub(in crate::transport) fn site(&self) -> UpgradeSite {
        UpgradeSite::new(Some(&self.hub), self.remote)
    }

    fn session(&self) -> (Arc<Session>, mpsc::Receiver<SessionEvent>) {
        let (inbox, inbox_rx) = mpsc::channel(SESSION_INBOX_CAPACITY);
        let id = self.hub.next_session.fetch_add(1, Ordering::Relaxed);
        self.hub.routes.insert(self.remote, (id, inbox.clone()));
        let now = self.now.clone();
        let (data, data_inbox) = data_channels();
        self.inboxes.lock().unwrap().push(data_inbox);
        let session = Arc::new(Session {
            id,
            remote: self.remote,
            remote_version: Some(QUIC_UPGRADE_NEGOTIATES_IN),
            hub: Arc::clone(&self.hub),
            inbox,
            budget: SharedInboundBudget::new(16, 1 << 20),
            data,
            udp: self.udp.clone(),
            pacer: self.pacer.clone(),
            routed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            quic_receipt_nanos: Arc::new(AtomicU64::new(0)),
            now: Arc::new(move || Duration::from_nanos(now.load(Ordering::Relaxed))),
            state: Mutex::new(SessionState {
                machine: UpgradeMachine::new(self.hub.local_key.clone()),
                attempt: None,
                peer_key: None,
                ended: false,
                setup_started: None,
            }),
        });
        (session, inbox_rx)
    }

    fn set_now(&self, at: Duration) {
        self.now.store(at.as_nanos() as u64, Ordering::Relaxed);
    }

    /// `session` makes an offer and it times out, which charges it.
    fn fail_one_offer(&self, session: &Session) {
        let at = Duration::from_nanos(session.deadline_nanos().expect("a server's first offer"));
        self.set_now(at);
        let sends = session.take(Event::Tick);
        assert!(
            matches!(sends.as_slice(), [UdpSend::Offer { .. }]),
            "{sends:?}"
        );
        assert_eq!(self.hub.policy.permits().in_use(), 1);
        self.set_now(at + OFFER_TIMEOUT);
        assert!(session.take(Event::Tick).is_empty());
        assert_eq!(self.hub.policy.permits().in_use(), 0);
    }

    fn seed_of(&self, session: &Session) -> Option<BackoffSeed> {
        session.state.lock().machine.backoff_seed()
    }
}

/// A raw connection between two endpoints, with a path on each end. `ours` is
/// built as a node builds one, on the given budget and channels; `peer` is a test
/// path with no negotiation, so it sends data. For tests of what a path delivers.
pub(in crate::transport) struct DataPath {
    pub(in crate::transport) time: VirtualTime,
    _pair: Pair,
    peer: QuicPath,
    peer_inbound: QuicInbound,
    ours: Option<Arc<QuicPath>>,
    _controls: mpsc::Receiver<Bytes>,
}

impl DataPath {
    pub(in crate::transport) fn peer(&self) -> &QuicPath {
        &self.peer
    }

    pub(in crate::transport) fn peer_inbound(&mut self) -> &mut QuicInbound {
        &mut self.peer_inbound
    }

    /// The address the peer sends from.
    pub(in crate::transport) fn peer_addr(&self) -> SocketAddr {
        self._pair.a_addr
    }

    pub(in crate::transport) fn ours_handle(&self) -> Arc<QuicPath> {
        Arc::clone(self.ours.as_ref().expect("the path has not ended"))
    }

    pub(in crate::transport) fn ours(&self) -> &QuicPath {
        self.ours.as_ref().expect("the path has not ended")
    }

    /// Close the node's path and let go of it, as a session does.
    pub(in crate::transport) fn end_ours(&mut self) {
        let path = self.ours.take().expect("the path has not ended");
        path.close(QuicCloseCode::Downgrade);
    }
}

pub(in crate::transport) async fn data_path(
    network: &str,
    limits: QuicPathLimits,
    budget: SharedInboundBudget,
    data: QuicDataSenders,
) -> DataPath {
    data_path_to(network, limits, QuicPathLimits::DEFAULT, budget, data).await
}

/// [`data_path`] to a peer with `peer_limits`.
pub(in crate::transport) async fn data_path_to(
    network: &str,
    limits: QuicPathLimits,
    peer_limits: QuicPathLimits,
    budget: SharedInboundBudget,
    data: QuicDataSenders,
) -> DataPath {
    data_path_with(network, limits, peer_limits, budget, data, false).await
}

/// [`data_path`] on a multi-thread runtime.
pub(in crate::transport) async fn data_path_on_threads(
    network: &str,
    budget: SharedInboundBudget,
    data: QuicDataSenders,
) -> DataPath {
    let limits = QuicPathLimits::DEFAULT;
    data_path_with(network, limits, limits, budget, data, true).await
}

async fn data_path_with(
    network: &str,
    limits: QuicPathLimits,
    peer_limits: QuicPathLimits,
    budget: SharedInboundBudget,
    data: QuicDataSenders,
    on_threads: bool,
) -> DataPath {
    let net = crate::transport::test_net::subnet();
    let (a_addr, b_addr) = (net.addr(1), net.addr(2));
    let pair = pair(network, a_addr, b_addr, None).await;
    let permit = pair
        .b
        .allow_peer(a_addr, pair.a_keys.public())
        .expect("allowed");
    let (a, b) = (pair.a.clone(), pair.b.clone());
    let b_key = pair.b_keys.public().clone();
    let handshake = async move {
        let (client, server) = tokio::join!(a.connect(b_addr, &b_key), async {
            b.accept().await.expect("an attempt").establish().await
        });
        (client.expect("client"), server.expect("server"))
    };
    let limit = Duration::from_secs(30);
    let (client, server) = if on_threads {
        crate::transport::quic::tests::drive_on_threads(&pair.time, limit, handshake).await
    } else {
        drive(&pair.time, limit, handshake).await
    };
    drop(permit);
    let time = pair.time.clone();
    let (peer, peer_inbound) = QuicPath::with_limits(client, time.clone(), peer_limits);
    let (ours, controls) = QuicPath::with_budget(server, time.clone(), limits, budget, data, None);
    DataPath {
        time,
        _pair: pair,
        peer,
        peer_inbound,
        ours: Some(Arc::new(ours)),
        _controls: controls,
    }
}

/// Set up with `prepare`, then drive its test on the virtual clock it returns, under
/// the QUIC tests' watchdog.
pub(in crate::transport) fn run_bounded<P, PF, F>(prepare: P)
where
    P: FnOnce() -> PF + Send + 'static,
    PF: Future<Output = (VirtualTime, F)>,
    F: Future<Output = ()> + Send + 'static,
{
    bounded(move || async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let (time, test) = prepare().await;
        drive(&time, Duration::from_secs(600), test).await;
    });
}

/// The promotion of a new session of a key ends the older one first, which closes
/// out its attempt and files its seed, so the new machine starts with that backoff.
/// Dropping the older session's connection may come later than this, or never.
#[test]
fn a_new_promotion_ends_the_older_session_and_takes_its_backoff() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let h = Harness::server("quic-driver-order").await;
        let (older, _older_rx) = h.session();
        older.promote(h.peer.clone());
        h.fail_one_offer(&older);
        let left = h.seed_of(&older).expect("promoted, so it took a seed");
        assert_eq!(left.failures, 1);
        assert!(left.retry_at > Duration::from_nanos(h.now.load(Ordering::Relaxed)));

        let (newer, _newer_rx) = h.session();
        newer.promote(h.peer.clone());
        assert!(older.state.lock().ended, "the older session is ended");
        assert_eq!(
            h.seed_of(&newer),
            Some(left),
            "the new machine has the backoff of the old one"
        );
        assert_eq!(
            newer.deadline_nanos(),
            Some(left.retry_at.as_nanos() as u64),
            "the backoff is longer than the first offer's delay"
        );
        assert!(left.retry_at >= BACKOFF_MIN.mul_f64(0.8));
    });
}

/// A session promoted after a newer one of the same key is the older of the two:
/// it ends, files nothing and leaves the newer one in place.
#[test]
fn an_older_session_promoted_late_ends_itself() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let h = Harness::server("quic-driver-late").await;
        let (older, _older_rx) = h.session();
        let (newer, _newer_rx) = h.session();
        newer.promote(h.peer.clone());
        h.fail_one_offer(&newer);
        let left = h.seed_of(&newer);
        older.promote(h.peer.clone());
        assert!(older.state.lock().ended);
        assert!(!newer.state.lock().ended, "the newer session is untouched");
        assert_eq!(h.seed_of(&older), None, "it took no seed");
        assert_eq!(h.seed_of(&newer), left);
        let held = h.hub.by_key.get(&h.peer).unwrap().upgrade().unwrap();
        assert_eq!(held.id, newer.id);
    });
}

/// Ending a session removes its route and key entry, and returns what its attempt
/// held. Ending one replaced at its address leaves the replacement's entries.
#[test]
fn ending_a_session_cleans_every_map_and_only_its_own_entries() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let h = Harness::server("quic-driver-clean").await;
        let (older, _older_rx) = h.session();
        let (newer, _newer_rx) = h.session();
        newer.promote(h.peer.clone());
        older.end();
        assert_eq!(h.hub.routes.get(&h.remote).unwrap().0, newer.id);
        assert!(h.hub.by_key.contains_key(&h.peer));

        let at = Duration::from_nanos(newer.deadline_nanos().unwrap());
        h.set_now(at);
        assert_eq!(newer.take(Event::Tick).len(), 1, "offered");
        assert_eq!(h.hub.policy.permits().in_use(), 1);
        newer.end();
        assert!(h.hub.routes.is_empty());
        assert!(h.hub.by_key.is_empty());
        assert_eq!(h.hub.policy.permits().in_use(), 0);
        assert!(newer.state.lock().attempt.is_none());
        newer.end();
        assert_eq!(newer.deadline_nanos(), None, "ended, so never a tick again");
    });
}

/// An `Offer` not yet sent is dropped when its attempt is torn down in the same
/// batch of actions, and one a newer session's `end` overtakes is seen by the check
/// the driver makes just before the send.
#[test]
fn a_torn_down_attempt_sends_no_offer() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let h = Harness::server("quic-driver-deferred").await;
        let (session, _rx) = h.session();
        session.promote(h.peer.clone());
        let mut state = session.state.lock();
        let sends = session.apply(
            &mut state,
            vec![
                Action::AllowPeerThenOffer {
                    attempt: 7,
                    caps: 0,
                },
                Action::TearDown {
                    attempt: 7,
                    code: QuicCloseCode::Downgrade,
                },
            ],
        );
        assert!(sends.is_empty(), "{sends:?}");
        assert!(state.attempt.is_none());
        drop(state);

        let at = Duration::from_nanos(session.deadline_nanos().unwrap());
        h.set_now(at);
        let sends = session.take(Event::Tick);
        let [UdpSend::Offer { attempt, .. }] = sends.as_slice() else {
            panic!("{sends:?}");
        };
        assert!(session.attempt_is_live(*attempt));
        session.end();
        assert!(!session.attempt_is_live(*attempt));
    });
}

/// A connection for anything but the live attempt is closed, not adopted: attempt
/// ids restart per session, so it may belong to another one.
#[test]
fn a_connection_for_no_live_attempt_is_closed() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let h = Harness::server("quic-driver-stale").await;
        let (session, _rx) = h.session();
        session.promote(h.peer.clone());
        let permit = h
            .pair
            .b
            .allow_peer(h.pair.a_addr, h.pair.a_keys.public())
            .unwrap();
        let (a, b) = (h.pair.a.clone(), h.pair.b.clone());
        let (b_addr, b_key) = (h.pair.b_addr, h.pair.b_keys.public().clone());
        let (client, _server) = drive(&h.pair.time, Duration::from_secs(30), async move {
            let (client, server) = tokio::join!(a.connect(b_addr, &b_key), async {
                b.accept().await.expect("an attempt").establish().await
            });
            (client.expect("client"), server.expect("server"))
        })
        .await;
        drop(permit);
        let quinn = client.inner().clone();
        assert!(quinn.close_reason().is_none());
        let sends = session.handle(SessionEvent::Connected {
            attempt: 9,
            connection: client,
        });
        assert!(sends.is_empty());
        assert!(quinn.close_reason().is_some(), "closed at once");
        assert!(session.state.lock().attempt.is_none());
    });
}

/// A dial that connects after its session ended is closed with SESSION_ENDED,
/// as the session's own end closes a path.
#[test]
fn a_connection_after_the_session_ended_closes_with_session_ended() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let h = Harness::server("quic-driver-late-dial").await;
        let (session, _rx) = h.session();
        session.promote(h.peer.clone());
        let permit = h
            .pair
            .b
            .allow_peer(h.pair.a_addr, h.pair.a_keys.public())
            .unwrap();
        let (a, b) = (h.pair.a.clone(), h.pair.b.clone());
        let (b_addr, b_key) = (h.pair.b_addr, h.pair.b_keys.public().clone());
        let (client, _server) = drive(&h.pair.time, Duration::from_secs(30), async move {
            let (client, server) = tokio::join!(a.connect(b_addr, &b_key), async {
                b.accept().await.expect("an attempt").establish().await
            });
            (client.expect("client"), server.expect("server"))
        })
        .await;
        drop(permit);
        session.end();
        let quinn = client.inner().clone();
        let sent_before = TRANSPORT_METRICS.quic_close_sent_to(b_addr).len();
        let sends = session.handle(SessionEvent::Connected {
            attempt: 1,
            connection: client,
        });
        assert!(sends.is_empty());
        assert!(quinn.close_reason().is_some(), "closed at once");
        assert_eq!(
            TRANSPORT_METRICS.quic_close_sent_to(b_addr)[sent_before..],
            [QuicCloseCode::SessionEnded]
        );
    });
}

/// `PeerConnection` declares its QUIC path before the inbound budget the path
/// charges, so its fields drop in that order (budget case (d)).
#[test]
fn quic_path_state_is_declared_before_the_budget() {
    let source = include_str!("../../../peer_connection.rs");
    let (_, rest) = source
        .split_once("pub struct PeerConnection<")
        .expect("the struct");
    let (body, _) = rest.split_once("\n}\n").expect("the struct's end");
    let at = |field: &str| {
        body.find(&format!("\n    {field}:"))
            .unwrap_or_else(|| panic!("no field {field}"))
    };
    assert!(at("quic_path") < at("inbound_budget"));
}

/// A routed attempt becomes a path in shadow mode: the handshake finishes in a task,
/// `Ready` goes out on it, and a data frame from the peer is refused, so nothing
/// reaches the channels and the budget is not charged.
#[test]
fn a_routed_attempt_becomes_a_shadow_path() {
    use crate::transport::quic::control::QuicControl;
    use crate::transport::quic::path::QuicInboundOutcome;
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let h = Harness::server("quic-driver-path").await;
        let (session, inbox) = h.session();
        let attempt = offered(&h, &session);
        let up = path_up(&h, &session, inbox);
        let time = h.pair.time.clone();
        let session_for_task = Arc::clone(&session);
        let (peer_path, mut peer_inbound, refused, charged) =
            drive(&h.pair.time, Duration::from_secs(60), async move {
                let (peer_path, peer_inbound, _inbox) = up.await;
                // Acked or stopped, it does not matter: a sent message is not
                // a delivered one.
                let _outcome = peer_path
                    .send_short_bounded(&time, Duration::from_secs(5), b"data")
                    .await;
                time.sleep(Duration::from_secs(1)).await;
                let state = session_for_task.state.lock();
                let live = state.attempt.as_ref().unwrap().path.as_ref().unwrap();
                let refused = live
                    .stats()
                    .count(QuicInboundOutcome::RefusedNotAcceptingData);
                assert_eq!(live.stats().count(QuicInboundOutcome::DeliveredShort), 0);
                let charged = session_for_task.budget.lock().stream_count();
                drop(state);
                (peer_path, peer_inbound, refused, charged)
            })
            .await;
        assert_eq!(refused, 1, "the data frame is refused, and counted");
        assert_eq!(charged, 0, "and not charged");
        let payload = peer_inbound.controls.try_recv().expect("Ready arrived");
        assert_eq!(
            peer_path.decode_control(&payload),
            Ok(QuicControl::Ready { attempt, caps: 0 })
        );
    });
}

/// Offers, routes the peer's dial through the Retry and returns the peer's path,
/// with the session's in `Confirming`. Run it under `drive`.
fn path_up(
    h: &Harness,
    session: &Arc<Session>,
    mut inbox: mpsc::Receiver<SessionEvent>,
) -> impl Future<Output = (QuicPath, QuicInbound, mpsc::Receiver<SessionEvent>)> + use<> {
    let local_is_a = h.pair.a_keys.public() > h.pair.b_keys.public();
    let (ours, theirs, our_addr, our_key) = if local_is_a {
        (&h.pair.a, &h.pair.b, h.pair.a_addr, h.pair.a_keys.public())
    } else {
        (&h.pair.b, &h.pair.a, h.pair.b_addr, h.pair.b_keys.public())
    };
    let (ours, theirs, our_key) = (ours.clone(), theirs.clone(), our_key.clone());
    let time = h.pair.time.clone();
    let hub = Arc::clone(&h.hub);
    let session = Arc::clone(session);
    async move {
        let client = tokio::spawn(async move { theirs.connect(our_addr, &our_key).await });
        hub.route(ours.accept().await.expect("an attempt"));
        loop {
            let event = inbox.recv().await.expect("the session lives");
            let retried = matches!(event, SessionEvent::Incoming(_));
            assert!(session.handle(event).is_empty());
            if retried {
                break;
            }
        }
        hub.route(ours.accept().await.expect("the attempt with its token"));
        while session
            .state
            .lock()
            .attempt
            .as_ref()
            .is_none_or(|held| held.path.is_none())
        {
            let event = inbox.recv().await.expect("the session lives");
            assert!(session.handle(event).is_empty());
        }
        let client = client.await.unwrap().expect("client handshake");
        let (peer_path, peer_inbound) = QuicPath::new(client, time.clone());
        (peer_path, peer_inbound, inbox)
    }
}

/// A promoted server session up to its first offer.
fn offered(h: &Harness, session: &Arc<Session>) -> u32 {
    session.promote(h.peer.clone());
    h.set_now(Duration::from_nanos(session.deadline_nanos().unwrap()));
    let sends = session.take(Event::Tick);
    let [UdpSend::Offer { attempt, .. }] = sends[..] else {
        panic!("{sends:?}");
    };
    attempt
}

/// The peer's `ReadyAck` reaches the session's driver.
fn ready_ack(session: &Session, attempt: u32, caps: u32) {
    use crate::transport::quic::control::QuicControl;
    let payload = Bytes::copy_from_slice(&QuicControl::ReadyAck { attempt, caps }.encode());
    let event = SessionEvent::ControlFrame { attempt, payload };
    assert!(session.handle(event).is_empty());
}

/// The driver acts on every (own bit, peer bit): what the path accepts follows
/// this node's bit from the moment the path exists, its `Ready` says so, and it
/// may send only once the peer's frame has the bit too. Mode `shadow` sets no bit
/// at any percentage, and neither does `prefer` at this release's share.
#[test]
fn a_path_follows_the_data_bits() {
    use crate::transport::quic::control::{QuicControl, caps};
    use crate::transport::quic::path::QuicInboundOutcome;
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        for (mode, percent, peer_caps, accepts, sends) in [
            ("prefer", Some("100"), caps::DATA, true, true),
            ("prefer", Some("100"), 0, true, false),
            ("prefer", Some("0"), caps::DATA, false, false),
            ("shadow", Some("100"), caps::DATA, false, false),
            ("prefer", None, caps::DATA, false, false),
        ] {
            let case = format!("{mode} {percent:?}, peer {peer_caps}");
            let h = Harness::server_with("quic-driver-bits", mode, percent).await;
            let (session, inbox) = h.session();
            let attempt = offered(&h, &session);
            let up = path_up(&h, &session, inbox);
            let time = h.pair.time.clone();
            let session_for_task = Arc::clone(&session);
            let (peer_path, mut peer_inbound, delivered, refused) =
                drive(&h.pair.time, Duration::from_secs(60), async move {
                    let (peer_path, peer_inbound, inbox) = up.await;
                    let _outcome = peer_path
                        .send_short_bounded(&time, Duration::from_secs(5), b"early")
                        .await;
                    time.sleep(Duration::from_secs(1)).await;
                    let state = session_for_task.state.lock();
                    let live = state.attempt.as_ref().unwrap().path.as_ref().unwrap();
                    let refused = live
                        .stats()
                        .count(QuicInboundOutcome::RefusedNotAcceptingData);
                    let delivered = live.stats().count(QuicInboundOutcome::DeliveredShort);
                    drop(state);
                    drop(inbox);
                    (peer_path, peer_inbound, delivered, refused)
                })
                .await;
            assert_eq!(
                (delivered, refused),
                if accepts { (1, 0) } else { (0, 1) },
                "{case}: a frame before the peer's bit has arrived"
            );
            let want_caps = if accepts { caps::DATA } else { 0 };
            let payload = peer_inbound.controls.try_recv().expect("Ready arrived");
            assert_eq!(
                peer_path.decode_control(&payload),
                Ok(QuicControl::Ready {
                    attempt,
                    caps: want_caps,
                }),
                "{case}"
            );
            let sends_data = |session: &Session| {
                let state = session.state.lock();
                let live = state.attempt.as_ref().unwrap().path.as_ref().unwrap();
                live.sends_data()
            };
            assert!(!sends_data(&session), "{case}: not before the peer's frame");
            assert!(session.data_route().is_none(), "{case}");
            ready_ack(&session, attempt, peer_caps);
            assert_eq!(sends_data(&session), sends, "{case}");
            assert_eq!(session.data_route().is_some(), sends, "{case}");
        }
    });
}

/// A path that ends stops taking sends at once, and what the session's route
/// holds goes on UDP from a worker that outlives the attempt: the attempt's tasks
/// are aborted, and nothing queued is lost. A peer's refusal takes the path off the
/// route the same way, with the path up.
#[test]
fn a_route_ends_with_its_path_and_what_it_holds_goes_on_udp() {
    use crate::transport::quic::control::caps;
    use crate::transport::quic::path::{NextStep, QuicSendError, SendFailure, SendKind, stop_code};
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let h = Harness::server_with("quic-driver-route", "prefer", Some("100")).await;
        let (session, inbox) = h.session();
        let attempt = offered(&h, &session);
        let up = path_up(&h, &session, inbox);
        let time = h.pair.time.clone();
        let (_peer_path, _peer_inbound, inbox) =
            drive(&h.pair.time, Duration::from_secs(60), async move {
                let up = up.await;
                time.sleep(Duration::from_secs(1)).await;
                up
            })
            .await;
        ready_ack(&session, attempt, caps::DATA);
        let route = session.data_route().expect("both bits");
        let path = Arc::downgrade(&route.path_handle());

        let refused = BoundedSend::Failed(SendFailure::Error(QuicSendError::Stopped(
            stop_code::NOT_ACCEPTING_DATA.into(),
        )));
        assert_eq!(
            route.path().settle(SendKind::Stream, &refused, false, 0),
            NextStep::SendOnUdp
        );
        assert!(
            session.data_route().is_none(),
            "a refused path takes no sends"
        );
        assert!(
            data_route_takes_no_lock(&session),
            "a refused route is off the fast path"
        );
        assert!(route.path().quinn_connection().close_reason().is_none());
        // The route still takes what a connection read a moment ago.
        for i in 0..10u8 {
            route.queue_short(vec![i]).expect("the queue has room");
        }
        session.end();
        assert!(session.data_route().is_none());
        let time = h.pair.time.clone();
        let udp = Arc::clone(&h.udp);
        drive(&h.pair.time, Duration::from_secs(60), async move {
            time.sleep(Duration::from_secs(1)).await;
            drop(inbox);
            let mut shorts = udp.shorts.lock().unwrap().clone();
            shorts.sort();
            assert_eq!(shorts, (0..10u8).map(|i| vec![i]).collect::<Vec<_>>());
        })
        .await;
        drop(route);
        let time = h.pair.time.clone();
        drive(&h.pair.time, Duration::from_secs(60), async move {
            time.sleep(Duration::from_secs(1)).await;
        })
        .await;
        assert!(
            path.upgrade().is_none(),
            "the worker ended and let the path go"
        );
    });
}

/// A path with both data bits counts once in `quic_enabled_data_paths` and in the
/// `quic_live_data_paths` gauge while its route lives, and a session that ends under it
/// counts by the path's age. A shadow path counts none of them.
#[test]
fn data_paths_are_counted_while_they_carry_data() {
    use crate::transport::quic::control::caps;
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        for (peer_caps, carries) in [(caps::DATA, true), (0, false)] {
            let h = Harness::server_with("quic-driver-data-counts", "prefer", Some("100")).await;
            let (session, inbox) = h.session();
            let attempt = offered(&h, &session);
            let up = path_up(&h, &session, inbox);
            let time = h.pair.time.clone();
            let _up = drive(&h.pair.time, Duration::from_secs(60), async move {
                let up = up.await;
                time.sleep(Duration::from_secs(1)).await;
                up
            })
            .await;
            ready_ack(&session, attempt, peer_caps);
            let snap = h.counters.snapshot(false);
            assert_eq!(snap.data_enabled, u64::from(carries), "{peer_caps}");
            assert_eq!(snap.data_paths, u64::from(carries), "{peer_caps}");
            h.set_now(Duration::from_secs(120));
            session.end();
            let snap = h.counters.snapshot(true);
            assert_eq!(snap.data_paths, 0, "the route went with the session");
            let ended: u64 = snap.session_ended_with_data_path.iter().sum();
            assert_eq!(ended, u64::from(carries), "{peer_caps}");
            if carries {
                let lt_10m = crate::transport::metrics::QuicPathAge::Lt10M as usize;
                assert_eq!(snap.session_ended_with_data_path[lt_10m], 1);
            }
        }
    });
}

/// A path the peer refuses data on leaves the `quic_live_data_paths` gauge at the
/// refusal, and only once: a second refusal and the attempt's drop lower nothing
/// more, and a session that ends after the refusal did not end under a data path.
#[test]
fn a_refused_path_leaves_the_gauge_once_and_does_not_end_with_data() {
    use crate::transport::quic::control::caps;
    use crate::transport::quic::path::{QuicSendError, SendFailure, SendKind, stop_code};
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let h = Harness::server_with("quic-driver-refused-gauge", "prefer", Some("100")).await;
        let (session, inbox) = h.session();
        let attempt = offered(&h, &session);
        let up = path_up(&h, &session, inbox);
        let time = h.pair.time.clone();
        let _up = drive(&h.pair.time, Duration::from_secs(60), async move {
            let up = up.await;
            time.sleep(Duration::from_secs(1)).await;
            up
        })
        .await;
        ready_ack(&session, attempt, caps::DATA);
        // A second data path on the node, so a double decrement would show.
        h.counters.data_path_started();
        assert_eq!(h.counters.snapshot(false).data_paths, 2);
        let route = session.data_route().expect("both bits");
        let refused = BoundedSend::Failed(SendFailure::Error(QuicSendError::Stopped(
            stop_code::NOT_ACCEPTING_DATA.into(),
        )));
        route.path().settle(SendKind::Stream, &refused, false, 0);
        assert_eq!(
            h.counters.snapshot(false).data_paths,
            1,
            "lowered at the refusal"
        );
        route.path().settle(SendKind::Short, &refused, false, 0);
        assert_eq!(h.counters.snapshot(false).data_paths, 1, "and only once");
        h.set_now(Duration::from_secs(120));
        session.end();
        drop(route);
        let snap = h.counters.snapshot(true);
        assert_eq!(snap.data_paths, 1, "the drop lowered nothing more");
        assert_eq!(
            snap.session_ended_with_data_path.iter().sum::<u64>(),
            0,
            "the session ended under a refused path"
        );
    });
}

/// Whether `session.data_route()` returns while another holder has the session's
/// lock, so it took none.
fn data_route_takes_no_lock(session: &Arc<Session>) -> bool {
    let held = session.state.lock();
    let session = Arc::clone(session);
    let (done, finished) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let route = session.data_route();
        done.send(route.is_none()).ok();
    });
    let took_none = finished.recv_timeout(Duration::from_secs(2));
    drop(held);
    took_none == Ok(true)
}

/// With no route, as on a path without both data bits, a send reads one atomic
/// and never waits for the session's lock, which the driver also takes. Shadow is
/// the default on Linux, macOS and Windows, so this is every connection there.
#[test]
fn a_connection_without_a_route_takes_no_lock_to_send() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let h = Harness::server_with("quic-driver-fast", "prefer", Some("100")).await;
        let (session, inbox) = h.session();
        assert!(data_route_takes_no_lock(&session), "before any path");
        let attempt = offered(&h, &session);
        let up = path_up(&h, &session, inbox);
        let time = h.pair.time.clone();
        let _up = drive(&h.pair.time, Duration::from_secs(60), async move {
            let up = up.await;
            time.sleep(Duration::from_secs(1)).await;
            up
        })
        .await;
        ready_ack(&session, attempt, 0);
        assert!(session.data_route().is_none(), "the peer set no bit");
        assert!(
            data_route_takes_no_lock(&session),
            "an active path in shadow"
        );
    });
}

/// The driver reserves a control frame's stream when it acts on the frame,
/// before the task that sends it has run (`QuicPath::control_first`): a
/// client's `ReadyAck` goes out in the step that enables its data, and a
/// reservation taken only when the task is first polled would let data sends
/// spawned in that step take the peer's stream slots first. The test runtime
/// is single-threaded, so no task runs until the test yields.
#[test]
fn a_control_send_reserves_its_stream_before_its_task_runs() {
    use crate::transport::quic::control::QuicControl;
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let h = Harness::server_with("quic-driver-control-first", "prefer", Some("100")).await;
        let (session, inbox) = h.session();
        let attempt = offered(&h, &session);
        let up = path_up(&h, &session, inbox);
        let time = h.pair.time.clone();
        let (_peer_path, mut peer_inbound, _inbox) =
            drive(&h.pair.time, Duration::from_secs(60), async move {
                let up = up.await;
                time.sleep(Duration::from_secs(1)).await;
                up
            })
            .await;
        let path = session
            .state
            .lock()
            .attempt
            .as_ref()
            .and_then(|held| held.path.clone())
            .expect("the path is up");
        assert!(
            peer_inbound.controls.try_recv().is_ok(),
            "the Ready arrived"
        );
        assert_eq!(path.controls_reserved(), 0, "the Ready has its stream");
        session.send_control(
            &mut session.state.lock(),
            QuicControl::Ready { attempt, caps: 0 },
        );
        assert_eq!(
            path.controls_reserved(),
            1,
            "reserved before the send task ran"
        );
        let time = h.pair.time.clone();
        drive(&h.pair.time, Duration::from_secs(60), async move {
            time.sleep(Duration::from_secs(1)).await;
        })
        .await;
        assert_eq!(path.controls_reserved(), 0, "released once sent");
        assert!(
            peer_inbound.controls.try_recv().is_ok(),
            "the frame arrived"
        );
    });
}

/// `EnableData` builds one route per activation: a frame repeated by the peer
/// changes nothing, and a second call is a bug the debug build catches.
#[test]
fn data_is_enabled_once_per_activation() {
    use crate::transport::quic::control::caps;
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let h = Harness::server_with("quic-driver-once", "prefer", Some("100")).await;
        let (session, inbox) = h.session();
        let attempt = offered(&h, &session);
        let up = path_up(&h, &session, inbox);
        let time = h.pair.time.clone();
        let _up = drive(&h.pair.time, Duration::from_secs(60), async move {
            let up = up.await;
            time.sleep(Duration::from_secs(1)).await;
            up
        })
        .await;
        ready_ack(&session, attempt, caps::DATA);
        let first = session.data_route().expect("both bits");
        ready_ack(&session, attempt, caps::DATA);
        assert!(session.data_route().expect("still").is(&first));
        // The debug assertion is the check; a release build returns at the guard.
        #[cfg(debug_assertions)]
        {
            let again = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                session.enable_data(&mut session.state.lock(), attempt)
            }));
            assert!(again.is_err(), "a second route must not be built");
        }
        #[cfg(not(debug_assertions))]
        session.enable_data(&mut session.state.lock(), attempt);
        assert!(session.data_route().expect("still").is(&first));
        // A send that timed out with nothing acknowledged for the window makes the
        // path suspect, and the route gives no path for new data.
        // The first settle reads what the setup had acknowledged.
        let now = h.pair.time.now_nanos();
        first.path().settle(
            crate::transport::quic::path::SendKind::Short,
            &BoundedSend::Sent,
            false,
            now,
        );
        let quiet = now + 11_000_000_000;
        let timed_out = BoundedSend::Failed(crate::transport::quic::path::SendFailure::NotOpened(
            crate::transport::quic::path::NotOpened::TimedOut,
        ));
        first.path().settle(
            crate::transport::quic::path::SendKind::Short,
            &timed_out,
            false,
            quiet,
        );
        assert!(
            session.data_route().is_none(),
            "a suspect path takes no new data"
        );
        // A route that was never refused: its attempt's end clears the fast path.
        session.end();
        assert!(
            data_route_takes_no_lock(&session),
            "an ended attempt clears the fast path"
        );
    });
}

/// A connection's view of a session in shadow: its path is up and carries
/// control frames, and neither side set the data bit.
pub(in crate::transport) struct ShadowPath {
    pub(in crate::transport) state: QuicPathState,
    pub(in crate::transport) path: Arc<QuicPath>,
    pub(in crate::transport) time: VirtualTime,
    _keep: (Harness, QuicPath, QuicInbound, mpsc::Receiver<SessionEvent>),
}

/// [`ShadowPath`], on a network of its own.
pub(in crate::transport) async fn shadow_path(network: &str) -> ShadowPath {
    let h = Harness::server_with(network, "shadow", None).await;
    let (session, inbox) = h.session();
    let attempt = offered(&h, &session);
    let up = path_up(&h, &session, inbox);
    let time = h.pair.time.clone();
    let clock = time.clone();
    let (peer_path, peer_inbound, inbox) = drive(&time, Duration::from_secs(60), async move {
        let up = up.await;
        clock.sleep(Duration::from_secs(1)).await;
        up
    })
    .await;
    ready_ack(&session, attempt, 0);
    let path = session
        .state
        .lock()
        .attempt
        .as_ref()
        .and_then(|held| held.path.clone())
        .expect("the path is up");
    let state = QuicPathState {
        session,
        driver: tokio::spawn(std::future::pending()),
    };
    ShadowPath {
        state,
        path,
        time,
        _keep: (h, peer_path, peer_inbound, inbox),
    }
}

/// The two sides of the stamp on a real path: frames on an active path keep the
/// receipt stamp fresh with the UDP clock frozen for 31 s, past the node's 30 s
/// `HOLD_WINDOW`, so a newer claim stays held; a path still being set up writes
/// nothing; and when the peer goes silent the stamp ages, so a quiet holder is
/// still replaced. The stamp is on the session's clock, not the path's.
#[test]
fn an_active_path_keeps_the_receipt_stamp_fresh() {
    use crate::transport::quic::control::QuicControl;
    const HOLD_WINDOW: Duration = Duration::from_secs(30);
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let h = Harness::server("quic-driver-stamp").await;
        let (session, inbox) = h.session();
        let attempt = offered(&h, &session);
        let up = path_up(&h, &session, inbox);
        let time = h.pair.time.clone();
        let session_for_task = Arc::clone(&session);
        let now = h.now.clone();
        let frozen_udp = Arc::new(AtomicU64::new(now.load(Ordering::Relaxed)));
        let stamp = crate::transport::ReceiptStamp::new(
            frozen_udp,
            session.quic_receipt_nanos.clone(),
            Arc::new({
                let now = now.clone();
                move || now.load(Ordering::Relaxed)
            }),
        );
        // Offset from the path's clock, to catch a write made on the wrong one.
        let offset = Duration::from_secs(7_000);
        let elapse = {
            let time = time.clone();
            let now = now.clone();
            move |by: Duration| {
                let time = time.clone();
                let now = now.clone();
                async move {
                    time.sleep(by).await;
                    now.store(
                        time.now_nanos() + offset.as_nanos() as u64,
                        Ordering::Relaxed,
                    );
                }
            }
        };
        drive(&h.pair.time, Duration::from_secs(120), async move {
            let (peer_path, _peer_inbound, _inbox) = up.await;
            // A frame the machine ignores: it names another attempt.
            let hello = QuicControl::ReadyAck {
                attempt: attempt + 1,
                caps: 0,
            };
            let sent = peer_path
                .send_control_bounded(&time, Duration::from_secs(2), hello)
                .await;
            assert!(matches!(sent, BoundedSend::Sent), "{sent:?}");
            elapse(Duration::from_secs(5)).await;
            assert_eq!(
                session_for_task.quic_receipt_nanos.load(Ordering::Acquire),
                0,
                "not active yet"
            );

            ready_ack(&session_for_task, attempt, 0);
            let mut heard_for = Duration::ZERO;
            while heard_for < Duration::from_secs(31) {
                let sent = peer_path
                    .send_control_bounded(&time, Duration::from_secs(2), hello)
                    .await;
                assert!(matches!(sent, BoundedSend::Sent), "{sent:?}");
                elapse(Duration::from_secs(3)).await;
                heard_for += Duration::from_secs(3);
                assert!(
                    stamp.since_last_receipt() <= Duration::from_secs(4),
                    "after {heard_for:?}: {:?}",
                    stamp.since_last_receipt()
                );
            }
            assert!(stamp.since_last_receipt() <= HOLD_WINDOW);

            elapse(HOLD_WINDOW + Duration::from_secs(5)).await;
            assert!(
                stamp.since_last_receipt() > HOLD_WINDOW,
                "a silent peer goes quiet: {:?}",
                stamp.since_last_receipt()
            );
            drop(peer_path);
        })
        .await;
    });
}

/// A session's path charges its connection's budget, so the peer has one allowance
/// across UDP and QUIC.
#[test]
fn a_session_is_opened_on_the_connections_budget() {
    let source = include_str!("../../../peer_connection.rs");
    let (_, rest) = source
        .split_once("    pub(super) fn with_upgrade_site(")
        .expect("the function");
    let (body, _) = rest.split_once("\n    }\n").expect("its end");
    assert!(body.contains("self.inbound_budget.clone(),"), "{body}");
}

/// One `Decline` per interval, measured from the last one let through, and a clock
/// that steps back holds it back rather than opening the gate.
#[test]
fn the_decline_gate_admits_one_per_interval() {
    const INTERVAL: u64 = 45_000_000_000;
    let before = TRANSPORT_METRICS.cumulative_upgrade_decline_suppressed();
    let mut gate = DeclineGate::default();
    assert!(gate.admit(5), "the first one goes");
    assert!(!gate.admit(5), "at the same instant");
    assert!(!gate.admit(5 + INTERVAL - 1), "one nanosecond short");
    assert!(!gate.admit(0), "a clock stepping back");
    assert!(gate.admit(5 + INTERVAL), "exactly one interval later");
    assert!(!gate.admit(5 + 2 * INTERVAL - 1), "the interval restarted");
    // Other tests may only add to it.
    assert!(
        TRANSPORT_METRICS.cumulative_upgrade_decline_suppressed() - before >= 4,
        "every `false` is counted where it is decided"
    );
    assert_eq!(DECLINE_INTERVAL, Duration::from_secs(45));
    // An uncharged decline's retry comes at BACKOFF_MIN (60 s) less 20% jitter.
    assert!(DECLINE_INTERVAL < Duration::from_secs(48));
}

/// A repeat of a promotion on an ended session adds no entry: nothing would ever
/// remove it.
#[test]
fn a_promotion_of_an_ended_session_adds_no_entry() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let h = Harness::server("quic-driver-ended").await;
        let (session, _rx) = h.session();
        session.promote(h.peer.clone());
        session.end();
        assert!(h.hub.by_key.is_empty());
        session.promote(h.peer.clone());
        assert!(h.hub.by_key.is_empty(), "the session ended");
    });
}

/// An ended session's entry does not stand in the way of a live one, even with a
/// higher number.
#[test]
fn an_entry_of_an_ended_session_is_replaced() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let h = Harness::server("quic-driver-dead-entry").await;
        let (older, _older_rx) = h.session();
        let (newer, _newer_rx) = h.session();
        newer.end();
        h.hub.by_key.insert(h.peer.clone(), Arc::downgrade(&newer));
        older.promote(h.peer.clone());
        assert!(!older.state.lock().ended, "nothing live is newer");
        let held = h.hub.by_key.get(&h.peer).unwrap().upgrade().unwrap();
        assert_eq!(held.id, older.id);
    });
}

/// Three sessions of one key promoted at once: whatever the order, only the newest
/// is left live and it holds the entry. Without one step for the compare and the
/// write, two can stay live.
#[test]
fn of_three_sessions_promoted_at_once_only_the_newest_stays() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let h = Harness::server("quic-driver-three").await;
        for round in 0..2000 {
            let (a, _a_rx) = h.session();
            let (b, _b_rx) = h.session();
            let (c, _c_rx) = h.session();
            let barrier = std::sync::Barrier::new(3);
            std::thread::scope(|scope| {
                for session in [&a, &b, &c] {
                    let (barrier, key) = (&barrier, h.peer.clone());
                    scope.spawn(move || {
                        barrier.wait();
                        session.promote(key);
                    });
                }
            });
            assert!(a.state.lock().ended, "round {round}: the oldest");
            assert!(b.state.lock().ended, "round {round}: the middle one");
            assert!(!c.state.lock().ended, "round {round}: the newest");
            let held = h.hub.by_key.get(&h.peer).unwrap().upgrade().unwrap();
            assert_eq!(held.id, c.id, "round {round}");
            drop(held);
            c.end();
            assert!(h.hub.by_key.is_empty(), "round {round}");
        }
    });
}

/// A full inbox drops what the peer sent, and counts it, and a helper's report of a
/// path's end waits for room and arrives.
#[test]
fn a_full_inbox_drops_peer_messages_and_still_delivers_a_path_end() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let h = Harness::server("quic-driver-overflow").await;
        let (session, mut inbox) = h.session();
        let state = QuicPathState {
            session: Arc::clone(&session),
            driver: tokio::spawn(std::future::pending()),
        };
        let offer = UpgradeControl::Offer {
            attempt: 1,
            caps: 0,
        };
        for _ in 0..SESSION_INBOX_CAPACITY {
            state.on_upgrade(offer);
        }
        assert_eq!(h.hub.inbox_overflow.load(Ordering::Relaxed), 0);
        state.on_upgrade(offer);
        assert_eq!(h.hub.inbox_overflow.load(Ordering::Relaxed), 1);

        let helper_inbox = session.inbox.clone();
        let helper = tokio::spawn(async move {
            let event = SessionEvent::PathClosed {
                attempt: 1,
                kind: QuicErrorKind::TimedOut,
            };
            report(&helper_inbox, event).await;
        });
        tokio::task::yield_now().await;
        assert!(!helper.is_finished(), "the helper waits for room");
        assert!(matches!(inbox.recv().await, Some(SessionEvent::Upgrade(_))));
        helper
            .await
            .expect("the helper finishes once there is room");
        let mut last = None;
        for _ in 0..SESSION_INBOX_CAPACITY {
            last = inbox.recv().await;
        }
        assert!(matches!(last, Some(SessionEvent::PathClosed { .. })));
    });
}

/// The machine's decisions reach the telemetry counters through the driver, and the
/// path gauges follow the attempt: an offer is counted, an attempt is in the
/// offered-or-dialing gauge until it ends, and a timeout is counted once.
#[test]
fn offer_outcomes_and_the_path_gauge_reach_telemetry() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let h = Harness::server("quic-driver-telemetry").await;
        let (session, _rx) = h.session();
        session.promote(h.peer.clone());
        let events = || TRANSPORT_METRICS.quic_events_of(h.remote);
        let at = Duration::from_nanos(session.deadline_nanos().unwrap());
        h.set_now(at);
        let sends = session.take(Event::Tick);
        let [UdpSend::Offer { attempt, .. }] = sends[..] else {
            panic!("{sends:?}");
        };
        session.take(Event::OfferSent { attempt });
        assert_eq!(events(), ["\"path_attempting\"", "Offered"]);
        h.set_now(at + OFFER_TIMEOUT);
        assert!(session.take(Event::Tick).is_empty());
        assert_eq!(
            events(),
            [
                "\"path_attempting\"",
                "Offered",
                "(\"path_left\", false)",
                "NoReply"
            ]
        );
    });
}

/// A decline reaches the counters under its reason and, when the machine spared
/// the peer the penalty, as an uncharged decline; the one the node sends is counted
/// by reason.
#[test]
fn declines_are_counted_by_reason_and_penalty() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let h = Harness::server("quic-driver-declines").await;
        let (session, _rx) = h.session();
        session.promote(h.peer.clone());
        let events = || TRANSPORT_METRICS.quic_events_of(h.remote);
        for (reason, counted) in [
            (
                DeclineReason::Busy.wire(),
                vec!["DeclinedBusy", "DeclinedUncharged"],
            ),
            (DeclineReason::Disabled.wire(), vec!["DeclinedDisabled"]),
            (0xEE, vec!["DeclinedUnknown"]),
        ] {
            h.set_now(Duration::from_nanos(session.deadline_nanos().unwrap()));
            let sends = session.take(Event::Tick);
            let [UdpSend::Offer { attempt, .. }] = sends[..] else {
                panic!("{sends:?}");
            };
            session.take(Event::OfferSent { attempt });
            let before = events().len();
            session.handle(SessionEvent::Upgrade(UpgradeControl::Decline {
                attempt,
                reason,
            }));
            let after = events();
            let counted: Vec<_> = counted.into_iter().map(String::from).collect();
            assert_eq!(after[before], "(\"path_left\", false)", "reason {reason}");
            assert_eq!(after[before + 1..], counted[..], "reason {reason}");
        }
    });
}

/// A panic in a task the driver spawns is counted, and the same panic goes on.
#[test]
fn a_panicking_task_is_counted_and_still_panics() {
    let net = crate::transport::test_net::subnet();
    let remote: SocketAddr = net.addr(1);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    runtime.block_on(async {
        let task = tokio::spawn(report_panics(remote, async { panic!("task failed") }));
        let error = task.await.expect_err("the panic goes on");
        assert!(error.is_panic());
    });
    assert_eq!(TRANSPORT_METRICS.quic_events_of(remote), ["TaskPanicked"]);
}

/// An established path is counted under both its role counters, its latency runs
/// from the offer, the gauge moves from offered-or-dialing to active, and tearing
/// the path down records its age and leaves the active gauge.
#[test]
fn an_established_path_is_counted_timed_and_aged() {
    use crate::transport::quic::upgrade::machine::{Outcome, Role};
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let h = Harness::server("quic-driver-established").await;
        let (session, _rx) = h.session();
        session.promote(h.peer.clone());
        h.set_now(Duration::from_secs(100));
        let mut state = session.state.lock();
        session.apply(
            &mut state,
            vec![
                Action::AllowPeerThenOffer {
                    attempt: 1,
                    caps: 0,
                },
                Action::Record(Outcome::Offered),
            ],
        );
        h.set_now(Duration::from_secs(100) + Duration::from_millis(250));
        session.apply(
            &mut state,
            vec![Action::Record(Outcome::Established(Role::Server))],
        );
        h.set_now(Duration::from_secs(100) + Duration::from_secs(90));
        session.apply(
            &mut state,
            vec![Action::TearDown {
                attempt: 1,
                code: QuicCloseCode::Downgrade,
            }],
        );
        drop(state);
        assert_eq!(
            TRANSPORT_METRICS.quic_events_of(h.remote),
            [
                "\"path_attempting\"",
                "Offered",
                "Established",
                "EstablishedAsOfferer",
                "(\"establish_ms\", 250)",
                "\"path_activated\"",
                "Lt10M",
                "(\"path_left\", true)",
            ]
        );
    });
}

/// A message dropped on a full inbox is counted as an overflow; one refused
/// because the inbox is closed (the session is ending) is not.
#[test]
fn only_a_full_inbox_counts_as_an_overflow() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let h = Harness::server("quic-driver-overflow-count").await;
        let (session, inbox) = h.session();
        let state = QuicPathState {
            session: Arc::clone(&session),
            driver: tokio::spawn(std::future::pending()),
        };
        let offer = UpgradeControl::Offer {
            attempt: 1,
            caps: 0,
        };
        let events = || TRANSPORT_METRICS.quic_events_of(h.remote);
        for _ in 0..SESSION_INBOX_CAPACITY {
            state.on_upgrade(offer);
        }
        assert!(events().is_empty());
        state.on_upgrade(offer);
        assert_eq!(events(), ["InboxOverflow"]);
        drop(inbox);
        state.on_upgrade(offer);
        assert_eq!(events(), ["InboxOverflow"], "a closed inbox is not counted");
    });
}

/// A connection attempt with no session, and one the session has no attempt
/// waiting for, are each counted as unrouted.
#[test]
fn unrouted_connection_attempts_are_counted() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let h = Harness::server("quic-driver-unrouted").await;
        let local_is_a = h.pair.a_keys.public() > h.pair.b_keys.public();
        let (ours, theirs, our_addr, our_key) = if local_is_a {
            (&h.pair.a, &h.pair.b, h.pair.a_addr, h.pair.a_keys.public())
        } else {
            (&h.pair.b, &h.pair.a, h.pair.b_addr, h.pair.b_keys.public())
        };
        let (ours, theirs, our_key) = (ours.clone(), theirs.clone(), our_key.clone());
        let remote = h.remote;
        let events = move || TRANSPORT_METRICS.quic_events_of(remote);
        // The peer is allowed, as it is when its session has just ended or
        // its attempt is not the one waiting; otherwise the endpoint refuses
        // the attempt before the router sees it.
        let _permit = ours.allow_peer(remote, &h.peer).expect("allowed");

        let hub = Arc::clone(&h.hub);
        let (o, t, k) = (ours.clone(), theirs.clone(), our_key.clone());
        drive(&h.pair.time, Duration::from_secs(60), async move {
            let dial = tokio::spawn(async move { t.connect(our_addr, &k).await });
            hub.route(o.accept().await.expect("an attempt"));
            dial.abort();
        })
        .await;
        assert_eq!(events(), ["AttemptUnrouted"], "no session for the address");

        let (session, _rx) = h.session();
        drive(&h.pair.time, Duration::from_secs(60), async move {
            let dial = tokio::spawn(async move { theirs.connect(our_addr, &our_key).await });
            let incoming = ours.accept().await.expect("an attempt");
            session.handle(SessionEvent::Incoming(Box::new(incoming)));
            dial.abort();
        })
        .await;
        assert_eq!(
            events(),
            ["AttemptUnrouted", "AttemptUnrouted"],
            "a session with no attempt waiting"
        );
    });
}

/// Each outcome that names how a setup or a session ended reaches its own
/// counter, and a decline is counted where the send passes the rate gate, not
/// where the machine decides.
#[test]
fn session_and_setup_endings_reach_their_own_counters() {
    use crate::transport::quic::upgrade::machine::Outcome;
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let h = Harness::server("quic-driver-endings").await;
        let (session, _rx) = h.session();
        let events = || TRANSPORT_METRICS.quic_events_of(h.remote);
        let mut state = session.state.lock();
        session.apply(
            &mut state,
            vec![
                Action::Record(Outcome::SetupDowngraded),
                Action::Record(Outcome::SetupEndpointDown),
                Action::Record(Outcome::SessionEnded { was_active: false }),
                Action::Record(Outcome::SessionEnded { was_active: true }),
                Action::Record(Outcome::DeclineSent(DeclineReason::Busy)),
            ],
        );
        drop(state);
        assert_eq!(
            events(),
            [
                "SetupDowngraded",
                "SetupEndpointDown",
                "SessionEndedSetup",
                "SessionEndedActive"
            ],
            "a decline is not counted by the machine's record"
        );
        session.count_decline_sent(DeclineReason::Busy.wire());
        assert_eq!(events().last().map(String::as_str), Some("DeclineSentBusy"));
    });
}

/// A fallback and a fallback the session's end followed reach their two
/// counter families under the same label, for every cause.
#[test]
fn fallbacks_and_churned_fallbacks_reach_their_own_counters() {
    use crate::transport::metrics::QuicFallback;
    use crate::transport::quic::upgrade::machine::{FallbackCause, Outcome};
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let h = Harness::server("quic-driver-churn").await;
        let (session, _rx) = h.session();
        let causes = [
            (FallbackCause::Idle, QuicFallback::Idle),
            (FallbackCause::Reset, QuicFallback::Reset),
            (FallbackCause::Protocol, QuicFallback::Protocol),
            (
                FallbackCause::DowngradeReceived,
                QuicFallback::DowngradeReceived,
            ),
            (FallbackCause::EndpointDown, QuicFallback::EndpointDown),
            (
                FallbackCause::SessionEndedReceived,
                QuicFallback::SessionEndedReceived,
            ),
            (
                FallbackCause::OverBudgetReceived,
                QuicFallback::OverBudgetReceived,
            ),
        ];
        assert_eq!(causes.len(), QuicFallback::COUNT);
        let mut state = session.state.lock();
        session.apply(
            &mut state,
            causes
                .iter()
                .flat_map(|(cause, _)| {
                    [
                        Action::Record(Outcome::Fallback(*cause)),
                        Action::Record(Outcome::ChurnedFallback(*cause)),
                    ]
                })
                .collect(),
        );
        drop(state);
        let want: Vec<String> = causes
            .iter()
            .flat_map(|(_, label)| [format!("{label:?}"), format!("(\"churned\", {label:?})")])
            .collect();
        assert_eq!(TRANSPORT_METRICS.quic_events_of(h.remote), want);
    });
}

/// What a retry test needs of a server harness.
struct RetryRig {
    ours: QuicEndpoint,
    theirs: QuicEndpoint,
    our_addr: SocketAddr,
    our_key: TransportPublicKey,
    time: VirtualTime,
}

impl RetryRig {
    fn of(h: &Harness) -> Self {
        let local_is_a = h.pair.a_keys.public() > h.pair.b_keys.public();
        let (ours, theirs, our_addr, our_key) = if local_is_a {
            (&h.pair.a, &h.pair.b, h.pair.a_addr, h.pair.a_keys.public())
        } else {
            (&h.pair.b, &h.pair.a, h.pair.b_addr, h.pair.b_keys.public())
        };
        Self {
            ours: ours.clone(),
            theirs: theirs.clone(),
            our_addr,
            our_key: our_key.clone(),
            time: h.pair.time.clone(),
        }
    }

    /// The peer dials this node, in a task.
    fn dial(&self) -> JoinHandle<Result<QuicConnection, crate::transport::quic::QuicConnectError>> {
        let (theirs, our_addr, our_key) =
            (self.theirs.clone(), self.our_addr, self.our_key.clone());
        tokio::spawn(async move { theirs.connect(our_addr, &our_key).await })
    }

    /// The next attempt the endpoint hands on, by the virtual clock.
    async fn next_attempt(&self) -> QuicIncoming {
        let ours = self.ours.clone();
        drive(&self.time, Duration::from_secs(30), async move {
            ours.accept().await.expect("an attempt")
        })
        .await
    }
}

/// A session with a live attempt waiting for a connection, as after an Offer.
fn waiting_session(h: &Harness) -> (Arc<Session>, mpsc::Receiver<SessionEvent>) {
    let (session, inbox) = h.session();
    session.promote(h.peer.clone());
    let mut state = session.state.lock();
    let sends = session.apply(
        &mut state,
        vec![Action::AllowPeerThenOffer {
            attempt: 1,
            caps: 0,
        }],
    );
    assert_eq!(sends.len(), 1, "allowed, so the Offer goes out");
    drop(state);
    (session, inbox)
}

fn slot_is_taken(session: &Session) -> bool {
    session
        .state
        .lock()
        .attempt
        .as_ref()
        .expect("the attempt is live")
        .establishing
}

fn retries(remote: SocketAddr) -> usize {
    TRANSPORT_METRICS
        .quic_events_of(remote)
        .iter()
        .filter(|event| *event == "AttemptRetried")
        .count()
}

/// The next `Connected` or `ConnectFailed` in `inbox`, by the virtual clock.
async fn attempt_result(
    time: &VirtualTime,
    mut inbox: mpsc::Receiver<SessionEvent>,
) -> SessionEvent {
    drive(time, Duration::from_secs(30), async move {
        loop {
            let event = inbox.recv().await.expect("the session lives");
            if matches!(
                event,
                SessionEvent::Connected { .. } | SessionEvent::ConnectFailed { .. }
            ) {
                return event;
            }
        }
    })
    .await
}

/// An Initial carrying the peer's address as its source, which the peer did not
/// send, is answered with a Retry and leaves the attempt open. The real dial then
/// takes the slot and connects, with one Retry of its own. Taking the first
/// Initial for the address, as before, would leave the real dial ignored until
/// the attempt's deadline.
#[test]
fn an_initial_the_peer_did_not_send_cannot_take_the_attempts_slot() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let network = "quic-driver-retry-stray";
        let h = Harness::server(network).await;
        let rig = RetryRig::of(&h);
        let (session, inbox) = waiting_session(&h);

        for sent in 1..=3 {
            let datagram = rig.theirs.initial_datagram(rig.our_addr, &rig.our_key);
            assert!(
                crate::transport::in_memory_socket::deliver_packet_to_network(
                    network,
                    rig.our_addr,
                    datagram,
                    h.remote,
                ),
                "the datagram reached the socket"
            );
            let stray = rig.next_attempt().await;
            assert!(!stray.address_validated(), "it returned no token");
            session.handle(SessionEvent::Incoming(Box::new(stray)));
            assert!(!slot_is_taken(&session), "after {sent} of them");
            assert_eq!(retries(h.remote), sent);
        }
        assert!(
            session
                .state
                .lock()
                .attempt
                .as_ref()
                .is_some_and(|held| held.allow.is_some()),
            "the attempt keeps its allow-set permit"
        );

        let dial = rig.dial();
        let first = rig.next_attempt().await;
        session.handle(SessionEvent::Incoming(Box::new(first)));
        assert!(!slot_is_taken(&session), "the dial's own first Initial");
        let second = rig.next_attempt().await;
        assert!(second.address_validated(), "it returned the token");
        session.handle(SessionEvent::Incoming(Box::new(second)));
        assert!(
            slot_is_taken(&session),
            "a validated attempt takes the slot"
        );
        assert_eq!(retries(h.remote), 4, "one Retry for the dial");

        let event = attempt_result(&rig.time, inbox).await;
        assert!(
            matches!(event, SessionEvent::Connected { attempt: 1, .. }),
            "the real dial connected"
        );
        dial.abort();
    });
}

/// An attempt the Retry gave up holds nothing of the address: with its session's
/// attempt gone, the address can be allowed again under another key, which a key
/// hold left behind would refuse.
#[test]
fn a_retried_attempt_keeps_no_hold_on_the_address() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let h = Harness::server("quic-driver-retry-hold").await;
        let rig = RetryRig::of(&h);
        let (session, _inbox) = waiting_session(&h);
        let dial = rig.dial();
        let first = rig.next_attempt().await;
        session.handle(SessionEvent::Incoming(Box::new(first)));
        assert_eq!(retries(h.remote), 1);
        dial.abort();

        session.state.lock().attempt = None;
        let other = TransportKeypair::new().public().clone();
        let _permit = rig
            .ours
            .allow_peer(h.remote, &other)
            .expect("no hold on the address remains");
    });
}

/// A session with no attempt waiting ignores an unvalidated attempt, as it does
/// any other: no Retry goes out for an address with nothing to connect.
#[test]
fn a_session_with_no_attempt_waiting_sends_no_retry() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let h = Harness::server("quic-driver-retry-no-wait").await;
        let rig = RetryRig::of(&h);
        let _permit = rig.ours.allow_peer(h.remote, &h.peer).expect("allowed");
        let (session, _inbox) = h.session();
        let dial = rig.dial();
        let first = rig.next_attempt().await;
        session.handle(SessionEvent::Incoming(Box::new(first)));
        // The client's next Initial would carry a token if a Retry had reached it.
        let again = rig.next_attempt().await;
        assert!(!again.address_validated(), "no Retry went out");
        again.ignore();
        dial.abort();
        assert_eq!(retries(h.remote), 0);
        assert_eq!(
            TRANSPORT_METRICS.quic_events_of(h.remote),
            ["AttemptUnrouted"]
        );
    });
}

/// With the first answer lost, the dial still connects well inside the connect
/// timeout, on the client's retransmission, and with one Retry. Losing the Initial
/// and losing the Retry look the same to the client.
#[test]
fn a_lost_retry_still_connects_inside_the_connect_timeout() {
    use crate::transport::quic::upgrade::machine::CONNECT_TIMEOUT;
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let h = Harness::server("quic-driver-retry-lost").await;
        let rig = RetryRig::of(&h);
        let (session, inbox) = waiting_session(&h);
        let started = rig.time.now_nanos();
        let dial = rig.dial();

        let lost = rig.next_attempt().await;
        assert!(!lost.address_validated());
        lost.ignore();
        let resent = rig.next_attempt().await;
        assert!(
            rig.time.now_nanos() > started,
            "the second came from the client's retransmission timer"
        );
        session.handle(SessionEvent::Incoming(Box::new(resent)));
        assert_eq!(retries(h.remote), 1);
        let validated = rig.next_attempt().await;
        assert!(validated.address_validated());
        session.handle(SessionEvent::Incoming(Box::new(validated)));

        let event = attempt_result(&rig.time, inbox).await;
        assert!(matches!(event, SessionEvent::Connected { .. }));
        let took = Duration::from_nanos(rig.time.now_nanos() - started);
        assert!(
            took < CONNECT_TIMEOUT / 2,
            "took {took:?} against {CONNECT_TIMEOUT:?}"
        );
        dial.abort();
    });
}

/// A token stays valid for as long as a client can be using it.
#[test]
fn a_retry_token_outlasts_the_setup_deadlines() {
    use crate::transport::quic::config::RETRY_TOKEN_LIFETIME;
    use crate::transport::quic::upgrade::machine::CONNECT_TIMEOUT;
    assert!(RETRY_TOKEN_LIFETIME > CONNECT_TIMEOUT);
    assert!(RETRY_TOKEN_LIFETIME > OFFER_TIMEOUT);
}

/// A session opened with a pacer (a connection under a bandwidth limit) paces
/// the path of every route it builds; one opened with none paces nothing.
#[test]
fn a_data_route_is_paced_only_when_the_session_has_a_pacer() {
    use crate::transport::quic::config::QuicRateLimit;
    use crate::transport::quic::control::caps;
    use crate::transport::quic::path::pacer::LimitPacer;
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        for (case, paced) in [("unpaced", false), ("paced", true)] {
            let mut h =
                Harness::server_with(&format!("quic-driver-paced-{case}"), "prefer", Some("100"))
                    .await;
            if paced {
                h.pacer = Some(Arc::new(
                    LimitPacer::new(QuicRateLimit::new(Some(100_000), None), h.pair.time.clone())
                        .expect("a limit paces"),
                ));
            }
            let (session, inbox) = h.session();
            let attempt = offered(&h, &session);
            let up = path_up(&h, &session, inbox);
            let time = h.pair.time.clone();
            let (_peer_path, _peer_inbound, _inbox) =
                drive(&h.pair.time, Duration::from_secs(60), async move {
                    let up = up.await;
                    time.sleep(Duration::from_secs(1)).await;
                    up
                })
                .await;
            ready_ack(&session, attempt, caps::DATA);
            let route = session.data_route().expect("both bits");
            assert_eq!(route.path().is_paced(), paced, "{case}");
            session.end();
        }
    });
}

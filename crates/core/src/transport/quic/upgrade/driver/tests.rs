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
}

impl Harness {
    /// A hub whose node has the higher key, so its sessions are servers.
    pub(in crate::transport) async fn server(network: &str, subnet: u8) -> Self {
        Self::server_with(network, subnet, "shadow", None).await
    }

    /// [`Self::server`] in this mode, with this `quic-data-percent`.
    async fn server_with(
        network: &str,
        subnet: u8,
        mode: &str,
        data_percent: Option<&str>,
    ) -> Self {
        let a_addr: SocketAddr = format!("10.204.{subnet}.1:5000").parse().unwrap();
        let b_addr: SocketAddr = format!("10.204.{subnet}.2:5000").parse().unwrap();
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
        let hub = QuicUpgrades::new(
            endpoint.clone(),
            policy,
            local.public().clone(),
            PathClock::Virtual(pair.time.clone()),
            None,
        );
        let peer = peer.clone();
        Self {
            pair,
            hub,
            remote,
            peer,
            now: Arc::new(AtomicU64::new(0)),
            inboxes: std::sync::Mutex::new(Vec::new()),
        }
    }

    /// How a connection joins this hub, with a driver of its own.
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

/// The promotion of a new session of a key ends the older one first, which closes
/// out its attempt and files its seed, so the new machine starts with that backoff.
/// Dropping the older session's connection may come later than this, or never.
#[test]
fn a_new_promotion_ends_the_older_session_and_takes_its_backoff() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let h = Harness::server("quic-driver-order", 1).await;
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
        let h = Harness::server("quic-driver-late", 2).await;
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
        let h = Harness::server("quic-driver-clean", 3).await;
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
        let h = Harness::server("quic-driver-deferred", 4).await;
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
        let h = Harness::server("quic-driver-stale", 5).await;
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
        let h = Harness::server("quic-driver-path", 6).await;
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
        for (subnet, mode, percent, peer_caps, accepts, sends) in [
            (30, "prefer", Some("100"), caps::DATA, true, true),
            (31, "prefer", Some("100"), 0, true, false),
            (32, "prefer", Some("0"), caps::DATA, false, false),
            (33, "shadow", Some("100"), caps::DATA, false, false),
            (34, "prefer", None, caps::DATA, false, false),
        ] {
            let case = format!("{mode} {percent:?}, peer {peer_caps}");
            let h = Harness::server_with("quic-driver-bits", subnet, mode, percent).await;
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
            ready_ack(&session, attempt, peer_caps);
            assert_eq!(sends_data(&session), sends, "{case}");
        }
    });
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
        let h = Harness::server("quic-driver-stamp", 35).await;
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
        let h = Harness::server("quic-driver-ended", 7).await;
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
        let h = Harness::server("quic-driver-dead-entry", 8).await;
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
        let h = Harness::server("quic-driver-three", 9).await;
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
        let h = Harness::server("quic-driver-overflow", 10).await;
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
        let h = Harness::server("quic-driver-telemetry", 11).await;
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
        let h = Harness::server("quic-driver-declines", 12).await;
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
    let remote: SocketAddr = "10.204.13.1:5000".parse().unwrap();
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
        let h = Harness::server("quic-driver-established", 14).await;
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
        let h = Harness::server("quic-driver-overflow-count", 15).await;
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
        let h = Harness::server("quic-driver-unrouted", 16).await;
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
        let h = Harness::server("quic-driver-endings", 17).await;
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
        let h = Harness::server(network, 18).await;
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
        let h = Harness::server("quic-driver-retry-hold", 19).await;
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
        let h = Harness::server("quic-driver-retry-no-wait", 20).await;
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
        let h = Harness::server("quic-driver-retry-lost", 21).await;
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

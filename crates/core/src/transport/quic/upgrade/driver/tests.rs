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
}

impl Harness {
    /// A hub whose node has the higher key, so its sessions are servers.
    pub(in crate::transport) async fn server(network: &str, subnet: u8) -> Self {
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
        config.transport_quic = Some("shadow".to_string());
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
        let session = Arc::new(Session {
            id,
            remote: self.remote,
            remote_version: Some(QUIC_UPGRADE_NEGOTIATES_IN),
            hub: Arc::clone(&self.hub),
            inbox,
            budget: SharedInboundBudget::new(16, 1 << 20),
            now: Box::new(move || Duration::from_nanos(now.load(Ordering::Relaxed))),
            state: Mutex::new(SessionState {
                machine: UpgradeMachine::new(self.hub.local_key.clone()),
                attempt: None,
                peer_key: None,
                ended: false,
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
        let (session, mut inbox) = h.session();
        session.promote(h.peer.clone());
        h.set_now(Duration::from_nanos(session.deadline_nanos().unwrap()));
        let sends = session.take(Event::Tick);
        let [UdpSend::Offer { attempt, .. }] = sends[..] else {
            panic!("{sends:?}");
        };
        let local_is_a = h.pair.a_keys.public() > h.pair.b_keys.public();
        let (ours, theirs, our_addr, our_key) = if local_is_a {
            (&h.pair.a, &h.pair.b, h.pair.a_addr, h.pair.a_keys.public())
        } else {
            (&h.pair.b, &h.pair.a, h.pair.b_addr, h.pair.b_keys.public())
        };
        let (ours, theirs, our_key) = (ours.clone(), theirs.clone(), our_key.clone());
        let time = h.pair.time.clone();
        let hub = Arc::clone(&h.hub);
        let session_for_task = Arc::clone(&session);
        let (peer_path, mut peer_inbound, refused, charged) =
            drive(&h.pair.time, Duration::from_secs(60), async move {
                let client = tokio::spawn(async move { theirs.connect(our_addr, &our_key).await });
                hub.route(ours.accept().await.expect("an attempt"));
                // The routed attempt, then its finished handshake.
                while session_for_task
                    .state
                    .lock()
                    .attempt
                    .as_ref()
                    .is_none_or(|held| held.path.is_none())
                {
                    let event = inbox.recv().await.expect("the session lives");
                    assert!(session_for_task.handle(event).is_empty());
                }
                let client = client.await.unwrap().expect("client handshake");
                let (peer_path, peer_inbound) = QuicPath::new(client, time.clone());
                // Acked or stopped, it does not matter: a sent message is not
                // a delivered one.
                let _outcome = peer_path
                    .send_short_bounded(&time, Duration::from_secs(5), b"data")
                    .await;
                time.sleep(Duration::from_secs(1)).await;
                let state = session_for_task.state.lock();
                let live = state.attempt.as_ref().unwrap().path.as_ref().unwrap();
                let refused = live
                    .path
                    .stats()
                    .count(QuicInboundOutcome::RefusedNotAcceptingData);
                assert_eq!(live._shorts.len(), 0);
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
            Ok(QuicControl::Ready {
                attempt,
                caps: crate::transport::quic::control::caps::KNOWN,
            })
        );
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

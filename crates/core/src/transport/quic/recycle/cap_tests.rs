//! Tests for the window cap of [`super::CappedCubic`]: the formula, the rate
//! rule, the window it gives on a live connection, and the rate a path keeps
//! to through a real endpoint on `VirtualTime`.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use quinn::congestion::{Controller, ControllerFactory};

use super::super::config::{QuicRateLimit, QuicSettings};
use super::super::memory_tests::{connected_pair_with, transfer_one_stream};
use super::super::tests::{bounded, drive};
use super::{CappedCubic, CappedCubicFactory, rate_window};
use crate::simulation::{TimeSource, VirtualTime};
use crate::transport::Socket;
use crate::transport::crypto::TransportKeypair;
use crate::transport::demux::DemuxSocket;
use crate::transport::egress_cap::EgressCap;
use crate::transport::global_bandwidth::GlobalBandwidthManager;
use crate::transport::in_memory_socket::{
    SimulationSocket, register_address_network, register_network_time_source,
    unregister_address_network, unregister_network_time_source,
};
use crate::transport::packet_data::MAX_RECV_PACKET_SIZE;

const MTU: u16 = 1200;

#[test]
fn the_rate_window_is_rate_times_srtt_with_a_floor_that_admits_two_datagrams() {
    assert_eq!(
        rate_window(1_000_000, Duration::from_millis(50), MTU),
        50_000
    );
    assert_eq!(rate_window(400_000, Duration::from_millis(40), MTU), 16_000);
    // quinn refuses a send at `in_flight + bytes >= window`, so two full
    // datagrams need one byte more than their size.
    assert_eq!(rate_window(1_000, Duration::from_millis(10), MTU), 2401);
    assert_eq!(rate_window(0, Duration::from_secs(1), MTU), 2401);
    assert_eq!(
        rate_window(u64::MAX, Duration::from_secs(3600), MTU),
        u64::MAX
    );
}

/// The cap's rate is the UDP token bucket's: the fair share when a global
/// manager exists, else `bandwidth-limit`, with 0 as unset, else none; and
/// the share follows the connection count.
#[test]
fn the_rate_follows_the_udp_rule() {
    use crate::transport::peer_connection::paced_rate;
    let manager = Arc::new(GlobalBandwidthManager::new(1_000_000, Some(1000)));
    for (limit, global, want) in [
        (None, None, None),
        (Some(0), None, None),
        (Some(80_000), None, Some(80_000)),
        (None, Some(manager.clone()), Some(1_000_000)),
        (Some(80_000), Some(manager.clone()), Some(1_000_000)),
    ] {
        let rate = QuicRateLimit::new(limit, global.clone());
        assert_eq!(rate.rate(), want, "{limit:?} {}", global.is_some());
        let udp = paced_rate(
            usize::MAX,
            global.map(|g| g.current_per_connection_rate()),
            limit,
        );
        assert_eq!(rate.rate(), (udp != usize::MAX).then_some(udp as u64));
    }
    let rate = QuicRateLimit::new(None, Some(manager.clone()));
    for _ in 0..4 {
        manager.register_connection();
    }
    assert_eq!(rate.rate(), Some(250_000), "four connections share it");
    manager.unregister_connection();
    assert_eq!(rate.rate(), Some(333_333));
}

/// A live quinn-proto connection with `rate` as the sender's cap: the
/// sender's controller after a transfer, and Cubic's window inside it.
fn after_a_transfer(rate: QuicRateLimit) -> (Box<dyn Controller>, u64) {
    struct Capped(QuicRateLimit);
    impl ControllerFactory for Capped {
        fn build(self: Arc<Self>, now: Instant, mtu: u16) -> Box<dyn Controller> {
            Arc::new(CappedCubicFactory::new(self.0.clone())).build(now, mtu)
        }
    }
    let (mut receiver, mut sender, now) = connected_pair_with(Arc::new(Capped(rate)));
    transfer_one_stream(&mut receiver, &mut sender, now, 256 * 1024);
    let controller = sender.conn().congestion_state().clone_box();
    let capped = controller
        .clone_box()
        .into_any()
        .downcast::<CappedCubic>()
        .expect("the CappedCubic");
    assert!(capped.srtt.is_some(), "the transfer gave an RTT sample");
    let cubic = capped.inner.window();
    (controller, cubic)
}

/// Without a limit the window is Cubic's, sample or not.
#[test]
fn with_no_limit_the_window_is_cubics() {
    let (controller, cubic) = after_a_transfer(QuicRateLimit::default());
    assert_eq!(controller.window(), cubic);
    let fresh = CappedCubic::new(
        Arc::new(quinn::congestion::CubicConfig::default()).build(Instant::now(), MTU),
        QuicRateLimit::new(Some(1000), None),
        MTU,
    );
    assert_eq!(
        fresh.window(),
        fresh.inner.window(),
        "no RTT sample yet: Cubic's window"
    );
}

/// While the cap binds, Cubic does not grow behind it: after a transfer held
/// to the cap its window is still the initial one, where the same transfer
/// with no limit grows it. Then, with the cap lifted above the path, a loss
/// takes the window below where it started at once, so the path backs off.
/// Before the fix Cubic grew on every acknowledgement while capped, and one
/// loss left the window far above the start.
#[test]
fn cubic_does_not_grow_behind_the_cap_and_still_backs_off() {
    let initial = Arc::new(quinn::congestion::CubicConfig::default())
        .build(Instant::now(), MTU)
        .initial_window();
    let (_, uncapped) = after_a_transfer(QuicRateLimit::default());
    assert!(
        uncapped > initial,
        "a free transfer grows Cubic: {uncapped}"
    );

    let (controller, cubic) = after_a_transfer(QuicRateLimit::new(Some(100_000), None));
    assert_eq!(cubic, initial, "Cubic stayed where it was while capped");

    let mut capped = controller
        .into_any()
        .downcast::<CappedCubic>()
        .expect("the CappedCubic");
    // The capacity is now below the cap: a far higher rate, a real RTT.
    capped.rate = QuicRateLimit::new(Some(usize::MAX / 2), None);
    capped.srtt = Some(Duration::from_millis(40));
    assert_eq!(capped.window(), initial);
    let now = Instant::now();
    capped.on_congestion_event(now, now, false, u64::from(MTU));
    assert!(
        capped.window() < initial,
        "one loss backs off: {} vs {initial}",
        capped.window()
    );
}

/// With a limit the window is the lower of Cubic's and the rate's.
#[test]
fn with_a_limit_the_window_is_the_lower_of_cubics_and_the_rates() {
    let (controller, cubic) = after_a_transfer(QuicRateLimit::new(Some(100_000), None));
    let capped = controller
        .clone_box()
        .into_any()
        .downcast::<CappedCubic>()
        .expect("the CappedCubic");
    let srtt = capped.srtt.expect("a sample");
    let want = cubic.min(rate_window(100_000, srtt, capped.mtu));
    assert_eq!(controller.window(), want);
    assert!(
        controller.window() < cubic,
        "the cap binds at 100 kB/s: {} vs {cubic}",
        controller.window()
    );
    // A limit far above what the path does leaves Cubic's window. (This
    // in-memory pair has a round trip of 0, where any rate gives the floor,
    // so the RTT is set here.)
    let mut fast = CappedCubic::new(
        Arc::new(quinn::congestion::CubicConfig::default()).build(Instant::now(), MTU),
        QuicRateLimit::new(Some(usize::MAX / 2), None),
        MTU,
    );
    fast.srtt = Some(Duration::from_millis(40));
    assert_eq!(fast.window(), fast.inner.window());
    assert_eq!(fast.metrics().congestion_window, fast.inner.window());
    // And a low one at the same RTT lowers it, and the metrics report it.
    fast.rate = QuicRateLimit::new(Some(100_000), None);
    assert_eq!(fast.window(), 4000);
    assert_eq!(fast.metrics().congestion_window, 4000);
}

/// A socket that delivers each datagram `delay` after it is sent, on
/// `time`, and counts the bytes it sends with the virtual time.
struct Delayed {
    inner: Arc<SimulationSocket>,
    time: VirtualTime,
    delay: Duration,
    sent: Arc<Mutex<Vec<(u64, usize, bool)>>>,
}

impl Delayed {
    fn send_later(&self, buf: &[u8], target: SocketAddr) -> usize {
        let quic =
            crate::transport::demux::classify(buf) == crate::transport::metrics::DemuxClass::Quic;
        self.sent
            .lock()
            .unwrap()
            .push((self.time.now_nanos(), buf.len(), quic));
        // The wakeup is registered now, so datagrams keep their order.
        let wake = self.time.sleep(self.delay);
        let (inner, bytes) = (self.inner.clone(), buf.to_vec());
        tokio::spawn(async move {
            wake.await;
            let _lost_like_any_datagram = inner.try_send_to(&bytes, target);
        });
        buf.len()
    }
}

impl Socket for Delayed {
    async fn bind(_addr: SocketAddr) -> std::io::Result<Self> {
        unreachable!("built by wrapping a bound socket")
    }

    async fn recv_from(&self, buf: &mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
        self.inner.recv_from(buf).await
    }

    async fn send_to(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
        Ok(self.send_later(buf, target))
    }

    fn send_to_blocking(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
        Ok(self.send_later(buf, target))
    }

    fn try_send_to(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
        Ok(self.send_later(buf, target))
    }
}

/// Two endpoints on one `VirtualTime`, [`ONE_WAY`] apart each way unless
/// given, `a` sending with `rate` and an egress cap of `a_cap`. Unregisters
/// on drop.
struct Link {
    network: String,
    time: VirtualTime,
    a_socket: Arc<DemuxSocket<Delayed>>,
    a: super::super::QuicEndpoint,
    b: super::super::QuicEndpoint,
    a_addr: SocketAddr,
    b_addr: SocketAddr,
    a_keys: TransportKeypair,
    b_keys: TransportKeypair,
    sent: Arc<Mutex<Vec<(u64, usize, bool)>>>,
    pumps: Vec<tokio::task::JoinHandle<()>>,
}

const ONE_WAY: Duration = Duration::from_millis(20);

impl Link {
    async fn new(network: &str, subnet: u8, rate: QuicRateLimit, a_cap: Option<u64>) -> Self {
        Self::with(
            network,
            subnet,
            [rate, QuicRateLimit::default()],
            a_cap,
            ONE_WAY,
        )
        .await
    }

    /// [`Self::new`] with `rates` for `a` and `b`, and `one_way` of delay.
    async fn with(
        network: &str,
        subnet: u8,
        rates: [QuicRateLimit; 2],
        a_cap: Option<u64>,
        one_way: Duration,
    ) -> Self {
        let [a_rate, b_rate] = rates;
        let a_addr: SocketAddr = format!("10.207.{subnet}.1:5000").parse().unwrap();
        let b_addr: SocketAddr = format!("10.207.{subnet}.2:5000").parse().unwrap();
        let time = VirtualTime::new();
        register_network_time_source(network, time.clone());
        register_address_network(a_addr, network);
        register_address_network(b_addr, network);
        let sent = Arc::new(Mutex::new(Vec::new()));
        let socket = |addr, cap, sent: Arc<Mutex<_>>| {
            let time = time.clone();
            async move {
                let inner = Arc::new(SimulationSocket::bind(addr).await.expect("sim bind"));
                Arc::new(DemuxSocket::wrap(
                    Delayed {
                        inner,
                        time,
                        delay: one_way,
                        sent,
                    },
                    cap,
                ))
            }
        };
        let a_socket = socket(
            a_addr,
            a_cap.map(|cap| EgressCap::new(cap, time.clone())),
            sent.clone(),
        )
        .await;
        let b_socket = socket(b_addr, None, Arc::default()).await;
        let settings = |rate_limit| QuicSettings {
            max_connections: 4,
            memory_limit: 1024 * 1024 * 1024,
            idle_timeout: None,
            rate_limit,
        };
        let (a_keys, b_keys) = (TransportKeypair::new(), TransportKeypair::new());
        let a = super::super::QuicEndpoint::bind(
            a_socket.clone(),
            a_addr,
            time.clone(),
            &settings(a_rate),
            &a_keys,
        )
        .expect("endpoint a");
        let b = super::super::QuicEndpoint::bind(
            b_socket.clone(),
            b_addr,
            time.clone(),
            &settings(b_rate),
            &b_keys,
        )
        .expect("endpoint b");
        let pump = |socket: Arc<DemuxSocket<Delayed>>| {
            tokio::spawn(async move {
                let mut buf = vec![0u8; MAX_RECV_PACKET_SIZE];
                while socket.recv_classified(&mut buf).await.is_ok() {}
            })
        };
        let pumps = vec![pump(a_socket.clone()), pump(b_socket)];
        Self {
            network: network.to_string(),
            time,
            a_socket,
            a,
            b,
            a_addr,
            b_addr,
            a_keys,
            b_keys,
            sent,
            pumps,
        }
    }

    /// Connect and send `len` bytes on one stream from `a`. Returns the
    /// virtual time from the connection being up to `b` having read it all.
    async fn send(&self, len: usize, limit: Duration) -> Duration {
        self.send_from(true, len, limit).await
    }

    /// [`Self::send`], from `a` to `b` when `from_a`, else from `b` to `a`.
    async fn send_from(&self, from_a: bool, len: usize, limit: Duration) -> Duration {
        let _permit = self
            .b
            .allow_peer(self.a_addr, self.a_keys.public())
            .expect("allow-set has room");
        let (a, b) = (self.a.clone(), self.b.clone());
        let (b_addr, b_key) = (self.b_addr, self.b_keys.public().clone());
        let time = self.time.clone();
        drive(&self.time, limit, async move {
            let (client, server) = tokio::join!(a.connect(b_addr, &b_key), async {
                b.accept().await.expect("an attempt").establish().await
            });
            let (client, server) = (client.expect("client"), server.expect("server"));
            let start = time.now_nanos();
            let (sender, receiver) = if from_a {
                (client.inner().clone(), server.inner().clone())
            } else {
                (server.inner().clone(), client.inner().clone())
            };
            let mut send = sender.open_uni().await.expect("open");
            let (_, read) = tokio::join!(
                async {
                    send.write_all(&vec![7u8; len]).await.expect("write");
                    send.finish().expect("finish");
                },
                async {
                    let mut recv = receiver.accept_uni().await.expect("accept");
                    recv.read_to_end(len + 1).await.expect("read")
                }
            );
            assert_eq!(read.len(), len);
            let elapsed = Duration::from_nanos(time.now_nanos() - start);
            drop((client, server));
            elapsed
        })
        .await
    }
}

impl Drop for Link {
    fn drop(&mut self) {
        for pump in &self.pumps {
            pump.abort();
        }
        unregister_address_network(&self.a_addr, &self.network);
        unregister_address_network(&self.b_addr, &self.network);
        unregister_network_time_source(&self.network);
    }
}

/// A path under `bandwidth-limit` transfers at the limit, within 10%, on
/// `VirtualTime` over a 40 ms round trip, where the flow-control window alone
/// would allow about 50 times as much.
#[test]
fn a_path_under_a_bandwidth_limit_keeps_to_it() {
    bounded(|| async move {
        const LIMIT: usize = 400_000;
        const LEN: usize = 2_000_000;
        let link = Link::new(
            "quic-cap-bandwidth-limit",
            1,
            QuicRateLimit::new(Some(LIMIT), None),
            None,
        )
        .await;
        let elapsed = link.send(LEN, Duration::from_secs(60)).await;
        let rate = LEN as f64 / elapsed.as_secs_f64();
        assert!(
            (rate - LIMIT as f64).abs() <= 0.1 * LIMIT as f64,
            "{rate:.0} B/s over {elapsed:?}, limit {LIMIT}"
        );
    })
}

/// Where `rate x RTT` is under the floor, a path keeps two datagrams in
/// flight, in both directions: about two datagrams per round trip, well above
/// the limit (the documented limit of the cap). With a floor of exactly two
/// datagrams quinn would admit one, and each would wait for the peer's
/// delayed acknowledgement, about 1200 B per RTT + 25 ms.
#[test]
fn the_floor_keeps_two_datagrams_in_flight_both_ways() {
    bounded(|| async move {
        const LIMIT: usize = 100_000;
        const LEN: usize = 400_000;
        const ONE_WAY_SHORT: Duration = Duration::from_millis(2);
        let two_per_rtt = 2.0 * f64::from(MTU) / (2.0 * ONE_WAY_SHORT.as_secs_f64());
        for (subnet, from_a) in [(5, true), (6, false)] {
            let rate = || QuicRateLimit::new(Some(LIMIT), None);
            let link = Link::with(
                &format!("quic-cap-floor-{subnet}"),
                subnet,
                [rate(), rate()],
                None,
                ONE_WAY_SHORT,
            )
            .await;
            let elapsed = link.send_from(from_a, LEN, Duration::from_secs(60)).await;
            let achieved = LEN as f64 / elapsed.as_secs_f64();
            assert!(
                achieved >= 0.7 * two_per_rtt && achieved <= 1.3 * two_per_rtt,
                "from a {from_a}: {achieved:.0} B/s, two datagrams per round trip is \
                 {two_per_rtt:.0} B/s"
            );
        }
    })
}

/// A no-starvation check. Under a node-wide egress cap, a QUIC path and a
/// sender that takes from the cap greedily, with no bucket of its own, both
/// finish, no UDP fragment waits past a stream's 5 s gap deadline, and
/// everything the node sends stays within the cap. Fair shares are
/// `a_quic_path_and_udp_streams_each_get_their_fair_share_under_a_saturated_egress_cap`.
#[test]
fn a_quic_path_and_udp_streams_both_progress_under_an_egress_cap() {
    use crate::transport::egress_cap::{PACED_WAIT_DEADLINE, try_take};
    bounded(|| async move {
        const CAP: u64 = 500_000;
        const QUIC_LEN: usize = 1_000_000;
        const UDP_LEN: usize = 1_000_000;
        // A minimum below the share, or the default minimum (1 MB/s, clamped
        // to the total) makes each connection's share the whole cap.
        let manager = Arc::new(GlobalBandwidthManager::new(CAP as usize, Some(1000)));
        manager.register_connection();
        manager.register_connection();
        let link = Arc::new(
            Link::new(
                "quic-cap-shared",
                2,
                QuicRateLimit::new(None, Some(manager)),
                Some(CAP),
            )
            .await,
        );
        let udp_done = Arc::new(AtomicU64::new(0));
        let udp = {
            let (link, done) = (link.clone(), udp_done.clone());
            tokio::spawn(async move {
                // A stream fragment as the UDP transport sends one: take from
                // the cap, send at once, or sleep the deficit and try again.
                let fragment = [0x02u8; 1200];
                let (mut sent, mut last, mut longest) = (0, link.time.now_nanos(), 0u64);
                while sent < UDP_LEN {
                    match try_take(link.a_socket.as_ref(), Duration::ZERO) {
                        Ok(Some(take)) => {
                            let n = link
                                .a_socket
                                .try_send_to_paced(&fragment, link.b_addr, take)
                                .expect("sent");
                            sent += n;
                            let now = link.time.now_nanos();
                            longest = longest.max(now - last);
                            last = now;
                        }
                        Ok(None) => unreachable!("the socket has a cap"),
                        Err(deficit) => link.time.sleep(deficit).await,
                    }
                }
                done.store(1, Ordering::Relaxed);
                Duration::from_nanos(longest)
            })
        };
        let start = link.time.now_nanos();
        let quic_elapsed = link.send(QUIC_LEN, Duration::from_secs(120)).await;
        let longest_gap = drive(&link.time, Duration::from_secs(120), async move {
            udp.await.expect("the UDP sender")
        })
        .await;
        assert_eq!(
            udp_done.load(Ordering::Relaxed),
            1,
            "the UDP sender finished"
        );
        assert!(
            longest_gap < PACED_WAIT_DEADLINE,
            "a UDP fragment waited {longest_gap:?}"
        );
        assert!(quic_elapsed < Duration::from_secs(60), "{quic_elapsed:?}");
        let end = link.time.now_nanos();
        let sent: u64 = link
            .sent
            .lock()
            .unwrap()
            .iter()
            .filter(|(at, _, _)| *at >= start)
            .map(|(_, len, _)| *len as u64)
            .sum();
        let seconds = (end - start) as f64 / 1e9;
        let burst = link.a_socket.egress_cap().expect("a cap").burst_bytes();
        let allowed = CAP as f64 * seconds + burst as f64;
        assert!(
            sent as f64 <= allowed,
            "{sent} bytes in {seconds:.2} s, cap {CAP} B/s"
        );
        assert!(
            sent as usize >= QUIC_LEN + UDP_LEN,
            "both transfers went out: {sent}"
        );
    })
}

/// Under a saturated egress cap shared by two connections, a QUIC path and a
/// UDP stream sender on the other connection each get their fair share, half
/// the cap, within 15%. The UDP sender is paced as the UDP transport paces a
/// stream fragment: it reserves from its own connection's token bucket at the
/// share (`TokenBucket::reserve`), then takes from the egress cap. Both
/// transfers run for the whole measured window, the QUIC transfer's time, and
/// each side's rate is the bytes it put on the wire in that window.
///
/// Measured: QUIC about 239 kB/s and UDP about 253 kB/s of a 250 kB/s share.
/// With the window cap removed (and Cubic no longer told about it) QUIC took
/// about 491 kB/s and UDP about 12 kB/s, so the window cap is what holds QUIC
/// to its share here.
///
/// #6112 reported QUIC at about 390 kB/s against a 250 kB/s share. That share
/// was wrong: the test built its manager with the default minimum per
/// connection (1 MB/s), which `GlobalBandwidthManager::new` clamps to the
/// total, so each connection's share was the whole 500 kB/s cap, and 390 kB/s
/// was within it. The share here sets a minimum below it.
#[test]
fn a_quic_path_and_udp_streams_each_get_their_fair_share_under_a_saturated_egress_cap() {
    use crate::transport::egress_cap::{jittered, try_take};
    use crate::transport::token_bucket::TokenBucket;
    bounded(|| async move {
        const CAP: u64 = 500_000;
        const QUIC_LEN: usize = 2_000_000;
        let share = CAP as f64 / 2.0;
        // A minimum below the share, as above.
        let manager = Arc::new(GlobalBandwidthManager::new(CAP as usize, Some(1000)));
        manager.register_connection();
        manager.register_connection();
        let link = Arc::new(
            Link::new(
                "quic-cap-fair-share",
                7,
                QuicRateLimit::new(None, Some(manager.clone())),
                Some(CAP),
            )
            .await,
        );
        let udp = {
            let link = link.clone();
            let share = manager.current_per_connection_rate();
            tokio::spawn(async move {
                // The connection's bucket, as `initial_paced_rate` builds it:
                // the global share, a tenth of it as capacity.
                let bucket =
                    TokenBucket::new_with_time_source(share / 10, share, link.time.clone());
                let fragment = [0x02u8; 1200];
                loop {
                    let wait = bucket.reserve(fragment.len());
                    if !wait.is_zero() {
                        link.time.sleep(wait).await;
                    }
                    loop {
                        match try_take(link.a_socket.as_ref(), Duration::ZERO) {
                            Ok(Some(take)) => {
                                link.a_socket
                                    .try_send_to_paced(&fragment, link.b_addr, take)
                                    .expect("sent");
                                break;
                            }
                            Ok(None) => unreachable!("the socket has a cap"),
                            // As `StreamPacing` sleeps a refused take.
                            Err(deficit) => link.time.sleep(jittered(deficit)).await,
                        }
                    }
                }
            })
        };
        let start = link.time.now_nanos();
        let elapsed = link.send(QUIC_LEN, Duration::from_secs(120)).await;
        udp.abort();
        let end = start + elapsed.as_nanos() as u64;
        let (mut quic, mut udp_bytes) = (0u64, 0u64);
        for (at, len, is_quic) in link.sent.lock().unwrap().iter() {
            if (start..=end).contains(at) {
                if *is_quic {
                    quic += *len as u64;
                } else {
                    udp_bytes += *len as u64;
                }
            }
        }
        let seconds = elapsed.as_secs_f64();
        let (quic_rate, udp_rate) = (quic as f64 / seconds, udp_bytes as f64 / seconds);
        for (name, rate) in [("QUIC", quic_rate), ("UDP", udp_rate)] {
            assert!(
                (rate - share).abs() <= 0.15 * share,
                "{name} ran at {rate:.0} B/s, the share is {share:.0} (QUIC {quic_rate:.0}, \
                 UDP {udp_rate:.0}, over {seconds:.2} s)"
            );
        }
    })
}

/// The body of `bind_udp_listener`, whitespace removed.
fn bind_udp_listener_body() -> String {
    let source = include_str!("../../connection_handler.rs");
    let signature = "pub(super) async fn bind_udp_listener<S: Socket>(";
    assert_eq!(
        source.matches(signature).count(),
        1,
        "one bind_udp_listener"
    );
    let start = source.find(signature).expect("counted above");
    let open = start + source[start..].find('{').expect("a body");
    let mut depth = 0usize;
    for (i, byte) in source.bytes().enumerate().skip(open) {
        match byte {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return source[open..=i]
                        .lines()
                        .map(|line| line.split_once("//").map_or(line, |(code, _)| code))
                        .collect::<String>()
                        .split_whitespace()
                        .collect();
                }
            }
            _ => {}
        }
    }
    panic!("unbalanced braces in bind_udp_listener");
}

/// The transport builds its endpoint from settings that carry its own
/// bandwidth settings, and reports the endpoint's window. Both are in
/// `bind_udp_listener`, which no unit test runs with a limit, so this pins
/// the lines; comments are stripped first, so a commented-out line fails.
#[test]
fn the_transport_gives_the_endpoint_its_bandwidth_settings() {
    let body = bind_udp_listener_body();
    assert!(
        body.contains(
            "rate_limit:super::quic::config::QuicRateLimit::new(config.bandwidth_limit,config.global_bandwidth.clone(),),..settings"
        ),
        "the settings take the transport's bandwidth settings"
    );
    assert!(
        body.contains("letquic_endpoint=quic_settings.as_ref().and_then(|settings|"),
        "the endpoint is built from those settings"
    );
    assert_eq!(
        body.matches("bind_or_warn(").count(),
        2,
        "both builds of the endpoint are inside that closure"
    );
    assert!(
        body.contains(
            "if let(Some(_),Some(settings))=(&quic_endpoint,&quic_settings){super::TRANSPORT_METRICS.set_quic_flow_control_window_bytes(settings.window_bytes());}"
                .replace(' ', "")
                .as_str()
        ),
        "the window is reported once the endpoint is built"
    );
}

/// Shadow paths carry only control frames, a few dozen bytes each, below the
/// cap's two-datagram floor. So a limit, even at the 64 KiB/s floor, does not
/// change when a handshake and a control-sized message complete: the virtual
/// times match exactly with and without it.
#[test]
fn a_limit_does_not_delay_a_control_sized_exchange() {
    bounded(|| async move {
        use crate::transport::peer_connection::MIN_BANDWIDTH_LIMIT;
        let mut finished = Vec::new();
        for (subnet, rate) in [
            (3, QuicRateLimit::default()),
            (4, QuicRateLimit::new(Some(MIN_BANDWIDTH_LIMIT), None)),
        ] {
            let _seed = crate::config::GlobalRng::seed_guard(0x5940_E5B7);
            let link = Link::new(&format!("quic-cap-control-{subnet}"), subnet, rate, None).await;
            let elapsed = link.send(32, Duration::from_secs(10)).await;
            finished.push((elapsed, link.time.now_nanos()));
        }
        assert_eq!(finished[0], finished[1], "with and without a limit");
    })
}

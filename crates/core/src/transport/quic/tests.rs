//! Tests for the QUIC endpoint.
//!
//! Most run two endpoints over `SimulationSocket` on one `VirtualTime`, with
//! `GlobalRng` seeded, on the test's single-threaded runtime: quinn's clock,
//! timers, connection IDs and keys are then all under the test's control.
//! Each endpoint authenticates with a `TransportKeypair` drawn from the
//! seeded `GlobalRng`, as a node does with its own (`identity`); tests where
//! a peer presents a key of its own use `test_tls`.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::config::{
    self, ALPN, INCOMING_BUFFER_BYTES, INCOMING_BUFFER_TOTAL_BYTES, KEEPALIVE_INTERVAL,
    MAX_BIDI_STREAMS, MAX_INCOMING, MAX_UDP_PAYLOAD, MAX_UNI_STREAMS, MAX_WINDOW_BYTES,
    MIN_WINDOW_BYTES,
};
use super::identity::{PeerKey, PinnedClientKey, PinnedServerKey, QuicIdentity};
use super::test_tls::TestIdentity;
use super::*;
use crate::config::GlobalRng;
use crate::simulation::{RealTime, VirtualTime};
use crate::transport::Socket;
use crate::transport::crypto::{TransportKeypair, TransportPublicKey};
use crate::transport::demux::{DemuxSocket, classify};
use crate::transport::egress_cap::EgressCap;
use crate::transport::in_memory_socket::{
    SimulationSocket, register_address_network, register_network_time_source,
    unregister_network_time_source,
};
use crate::transport::metrics::DemuxClass;
use crate::transport::packet_data::MAX_RECV_PACKET_SIZE;

pub(super) const SEED: u64 = 0x0059_40C2;

/// One datagram as a test socket saw it leave.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Sent {
    at_ns: u64,
    from: SocketAddr,
    to: SocketAddr,
    bytes: Vec<u8>,
}

/// A socket that records every datagram sent through it, with the time on
/// `time`, and otherwise passes everything to `inner`.
struct Recording<S> {
    inner: S,
    addr: SocketAddr,
    time: VirtualTime,
    log: Arc<Mutex<Vec<Sent>>>,
}

impl<S: Socket> Recording<S> {
    fn note(&self, buf: &[u8], to: SocketAddr) {
        self.log.lock().unwrap().push(Sent {
            at_ns: self.time.now_nanos(),
            from: self.addr,
            to,
            bytes: buf.to_vec(),
        });
    }
}

impl<S: Socket> Socket for Recording<S> {
    async fn bind(_addr: SocketAddr) -> std::io::Result<Self> {
        unreachable!("built by wrapping a bound socket")
    }

    async fn recv_from(&self, buf: &mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
        self.inner.recv_from(buf).await
    }

    async fn send_to(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
        let sent = self.inner.send_to(buf, target).await?;
        self.note(buf, target);
        Ok(sent)
    }

    fn send_to_blocking(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
        let sent = self.inner.send_to_blocking(buf, target)?;
        self.note(buf, target);
        Ok(sent)
    }

    fn try_send_to(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
        let sent = self.inner.try_send_to(buf, target)?;
        self.note(buf, target);
        Ok(sent)
    }
}

type SimDemux = DemuxSocket<Recording<SimulationSocket>>;

/// The listener's part, for a socket with no listener: read it, so its
/// QUIC ingress queue is filled.
fn pump<S: Socket>(socket: Arc<DemuxSocket<S>>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut buf = vec![0u8; MAX_RECV_PACKET_SIZE];
        while socket.recv_classified(&mut buf).await.is_ok() {}
    })
}

/// Run `fut` on the test runtime, letting every task run until nothing is
/// ready and then moving `time` to its next wakeup. Fails if virtual time
/// passes `limit` first.
pub(super) async fn drive<F>(time: &VirtualTime, limit: Duration, fut: F) -> F::Output
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    let task = tokio::spawn(fut);
    let limit_ns = time.now_nanos() + limit.as_nanos() as u64;
    loop {
        // Enough turns for every runnable task to run until it waits.
        for _ in 0..64 {
            if task.is_finished() {
                return task.await.expect("the driven future panicked");
            }
            tokio::task::yield_now().await;
        }
        if task.is_finished() {
            return task.await.expect("the driven future panicked");
        }
        assert!(
            time.advance_to_next_wakeup().is_some(),
            "nothing is runnable and no timer is armed at {} ns: the transfer is stuck",
            time.now_nanos()
        );
        assert!(
            time.now_nanos() <= limit_ns,
            "virtual time passed {limit:?} before the future finished"
        );
    }
}

/// How long a QUIC test may run, in real time, before it is failed.
const WATCHDOG: Duration = Duration::from_secs(120);

/// Run an async test on its own thread and current-thread runtime, failing
/// it if it has not finished within [`WATCHDOG`]. A quinn driver that spins
/// (a poller that says writable while every send is refused) never yields,
/// so a test on the test's own runtime would hang `cargo test` forever
/// instead of failing; the spinning thread is left behind and dies with the
/// test process.
pub(super) fn bounded<F, Fut>(test: F)
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()>,
{
    let (done, finished) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        let outcome =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| runtime.block_on(test())));
        // The receiver is gone only if the watchdog already failed the test.
        done.send(outcome).ok();
    });
    match finished.recv_timeout(WATCHDOG) {
        Ok(Ok(())) => {}
        Ok(Err(panic)) => std::panic::resume_unwind(panic),
        Err(_) => panic!(
            "the test did not finish within {WATCHDOG:?}; a QUIC driver is probably \
             spinning without yielding"
        ),
    }
}

/// The value of field `name` in a `{:?}` rendering, matched exactly: the
/// name must follow `{ ` or `, `, so `receive_window` does not match
/// `stream_receive_window`, and the value runs to the next `,` or `}` at
/// its own nesting level, so `1` does not match `100`.
fn debug_field(debug: &str, name: &str) -> String {
    for prefix in ["{ ", ", "] {
        let pattern = format!("{prefix}{name}: ");
        if let Some(at) = debug.find(&pattern) {
            let mut depth = 0i32;
            let rest = &debug[at + pattern.len()..];
            for (i, c) in rest.char_indices() {
                match c {
                    '(' | '[' | '{' => depth += 1,
                    ')' | ']' => depth -= 1,
                    '}' if depth == 0 => return rest[..i].trim().to_string(),
                    '}' => depth -= 1,
                    ',' if depth == 0 => return rest[..i].trim().to_string(),
                    _ => {}
                }
            }
        }
    }
    panic!("no field {name:?} in {debug}");
}

/// Settings with windows of [`MAX_WINDOW_BYTES`], so a 1 MiB stream fits.
fn settings() -> QuicSettings {
    QuicSettings {
        max_connections: 4,
        memory_limit: 1024 * 1024 * 1024,
    }
}

/// Two endpoints, `a` (client) and `b` (server), on one simulated network.
pub(super) struct Pair {
    pub(super) time: VirtualTime,
    network: String,
    pub(super) a: QuicEndpoint,
    pub(super) b: QuicEndpoint,
    pub(super) a_addr: SocketAddr,
    pub(super) b_addr: SocketAddr,
    pub(super) a_keys: TransportKeypair,
    pub(super) b_keys: TransportKeypair,
    a_socket: Arc<SimDemux>,
    b_socket: Arc<SimDemux>,
    log: Arc<Mutex<Vec<Sent>>>,
    pumps: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for Pair {
    fn drop(&mut self) {
        for pump in &self.pumps {
            pump.abort();
        }
        unregister_network_time_source(&self.network);
    }
}

/// Build a pair on addresses `a_addr` and `b_addr` (unique per test, since
/// the simulated network registry is keyed by address), with an egress cap
/// of `a_cap` bytes per second on `a`'s socket, on the same `VirtualTime`
/// as the endpoints. `b` accepts connections from `a`.
pub(super) async fn pair(
    network: &str,
    a_addr: SocketAddr,
    b_addr: SocketAddr,
    a_cap: Option<u64>,
) -> Pair {
    let time = VirtualTime::new();
    register_network_time_source(network, time.clone());
    register_address_network(a_addr, network);
    register_address_network(b_addr, network);
    let log = Arc::new(Mutex::new(Vec::new()));
    let bind = |addr: SocketAddr, cap: Option<EgressCap>| {
        let time = time.clone();
        let log = log.clone();
        async move {
            let inner = SimulationSocket::bind(addr).await.expect("sim bind");
            Arc::new(DemuxSocket::wrap(
                Recording {
                    inner,
                    addr,
                    time,
                    log,
                },
                cap,
            ))
        }
    };
    let a_socket = bind(a_addr, a_cap.map(|rate| EgressCap::new(rate, time.clone()))).await;
    let b_socket = bind(b_addr, None).await;
    let a_keys = TransportKeypair::new();
    let b_keys = TransportKeypair::new();
    let a = QuicEndpoint::bind(a_socket.clone(), a_addr, time.clone(), &settings(), &a_keys)
        .expect("endpoint a");
    let b = QuicEndpoint::bind(b_socket.clone(), b_addr, time.clone(), &settings(), &b_keys)
        .expect("endpoint b");
    let pumps = vec![pump(a_socket.clone()), pump(b_socket.clone())];
    Pair {
        time,
        network: network.to_string(),
        a,
        b,
        a_addr,
        b_addr,
        a_keys,
        b_keys,
        a_socket,
        b_socket,
        log,
        pumps,
    }
}

impl Pair {
    fn a_key(&self) -> TransportPublicKey {
        self.a_keys.public().clone()
    }

    fn b_key(&self) -> TransportPublicKey {
        self.b_keys.public().clone()
    }

    /// Connect `a` to `b`, send `payload` on one unidirectional stream from
    /// `a`, and return what `b` read and both connections.
    async fn transfer(
        &self,
        payload: Vec<u8>,
        limit: Duration,
    ) -> (Vec<u8>, QuicConnection, QuicConnection) {
        let permit = self
            .b
            .allow_peer(self.a_addr, self.a_keys.public())
            .expect("allow-set has room");
        let a = self.a.clone();
        let b = self.b.clone();
        let (b_addr, b_key) = (self.b_addr, self.b_key());
        let result = drive(&self.time, limit, async move {
            let (client, server) = tokio::join!(a.connect(b_addr, &b_key), async {
                b.accept()
                    .await
                    .expect("an incoming attempt")
                    .establish()
                    .await
            });
            let client = client.expect("client handshake");
            let server = server.expect("server accepted");
            let mut send = client.inner().open_uni().await.expect("open stream");
            let (_, received) = tokio::join!(
                async {
                    send.write_all(&payload).await.expect("write");
                    send.finish().expect("finish");
                },
                async {
                    let mut recv = server.inner().accept_uni().await.expect("accept stream");
                    recv.read_to_end(4 * 1024 * 1024).await.expect("read")
                }
            );
            (received, client, server)
        })
        .await;
        drop(permit);
        result
    }

    fn sent(&self) -> Vec<Sent> {
        self.log.lock().unwrap().clone()
    }
}

/// The raw public key the holder of `key` presents (its SPKI DER).
fn spki_of(key: &TransportPublicKey) -> Vec<u8> {
    PeerKey::of(key).expect("a usable key").spki().to_vec()
}

pub(super) fn payload(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

pub(super) const MIB: usize = 1024 * 1024;

/// Two endpoints on the simulated network complete a TLS 1.3 handshake
/// with Ed25519 raw public keys and move 1 MiB on a unidirectional stream,
/// on virtual time. Each side sees the other's key, the fixed ALPN, and no
/// server name (SNI is off). Every datagram either side sent is QUIC-class
/// with the fixed bit set (greasing is off) and at most 1200 bytes.
#[test]
fn handshake_and_one_mib_stream_on_virtual_time() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let pair = pair(
            "quic-handshake",
            "10.201.1.1:5000".parse().unwrap(),
            "10.201.1.2:5000".parse().unwrap(),
            None,
        )
        .await;
        let data = payload(MIB);
        let (received, client, server) = pair.transfer(data.clone(), Duration::from_secs(60)).await;
        assert_eq!(received.len(), data.len());
        assert!(received == data, "the stream arrived intact");

        let peer_key = |conn: &QuicConnection| -> Vec<u8> {
            let certs = conn
                .inner()
                .peer_identity()
                .expect("a raw public key was presented")
                .downcast::<Vec<rustls::pki_types::CertificateDer<'static>>>()
                .expect("rustls identity");
            certs[0].to_vec()
        };
        assert_eq!(
            peer_key(&server),
            spki_of(pair.a_keys.public()),
            "server saw a's key"
        );
        assert_eq!(
            peer_key(&client),
            spki_of(pair.b_keys.public()),
            "client saw b's key"
        );
        let handshake = server
            .inner()
            .handshake_data()
            .expect("handshake data")
            .downcast::<quinn::crypto::rustls::HandshakeData>()
            .expect("rustls handshake data");
        assert_eq!(handshake.protocol.as_deref(), Some(ALPN));
        assert_eq!(handshake.server_name, None, "SNI is off");
        for conn in [&client, &server] {
            assert!(
                conn.inner()
                    .congestion_state()
                    .into_any()
                    .downcast::<quinn::congestion::Cubic>()
                    .is_ok(),
                "Cubic congestion control"
            );
        }

        let sent = pair.sent();
        assert!(
            sent.len() > MIB / usize::from(MAX_UDP_PAYLOAD),
            "the transfer was recorded"
        );
        for datagram in &sent {
            assert!(
                datagram.bytes.len() <= usize::from(MAX_UDP_PAYLOAD),
                "a {} byte datagram",
                datagram.bytes.len()
            );
            assert_eq!(classify(&datagram.bytes), DemuxClass::Quic);
            assert_ne!(
                datagram.bytes[0] & 0x40,
                0,
                "fixed bit cleared (greasing on?) in {:#04x}",
                datagram.bytes[0]
            );
        }
    })
}

/// A peer stays on the demux allow-set exactly as long as its QUIC
/// connection: on both sides it is allowed while the connection is open and
/// removed once the connection has ended, here by the client dropping its
/// handle (which closes the connection) and the server seeing the close.
#[test]
fn a_peer_leaves_the_allow_set_when_its_connection_ends() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let pair = pair(
            "quic-allow-lifecycle",
            "10.201.13.1:5000".parse().unwrap(),
            "10.201.13.2:5000".parse().unwrap(),
            None,
        )
        .await;
        let (received, client, server) =
            pair.transfer(payload(1000), Duration::from_secs(60)).await;
        assert_eq!(received.len(), 1000);
        // `transfer` dropped its own permit; only the connections hold the
        // addresses now.
        assert!(
            pair.a_socket.is_quic_source_allowed(pair.b_addr),
            "client side"
        );
        assert!(
            pair.b_socket.is_quic_source_allowed(pair.a_addr),
            "server side"
        );

        drop(client);
        let server_conn = server.inner().clone();
        drive(&pair.time, Duration::from_secs(60), async move {
            server_conn.closed().await;
        })
        .await;
        // Let the watchers run after the close they waited for.
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        assert!(
            !pair.a_socket.is_quic_source_allowed(pair.b_addr),
            "client side released"
        );
        assert!(
            !pair.b_socket.is_quic_source_allowed(pair.a_addr),
            "server side released"
        );
        drop(server);
    })
}

/// The handshake fails when the client offers another ALPN protocol, on
/// both sides, and neither side keeps the other on its allow-set.
#[test]
fn a_client_with_another_alpn_is_refused() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let pair = pair(
            "quic-alpn",
            "10.201.2.1:5000".parse().unwrap(),
            "10.201.2.2:5000".parse().unwrap(),
            None,
        )
        .await;
        let _permit = pair.b.allow_peer(pair.a_addr, &pair.a_key()).expect("room");
        let b_peer = PeerKey::of(pair.b_keys.public()).unwrap();
        let mut tls = config::rustls_client_config(
            pair.a.identity.certified_key(),
            Arc::new(PinnedServerKey::new(&b_peer)),
        )
        .unwrap();
        tls.alpn_protocols = vec![b"something-else".to_vec()];
        let client_config = pair.a.client_config(Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(Arc::new(tls)).unwrap(),
        ));
        let a = pair.a.clone();
        let b = pair.b.clone();
        let b_addr = pair.b_addr;
        let (client, server) = drive(&pair.time, Duration::from_secs(60), async move {
            tokio::join!(
                async {
                    a.connect_with(b_addr, client_config, b_peer)
                        .await
                        .map(|_| ())
                },
                async {
                    b.accept()
                        .await
                        .expect("an attempt")
                        .establish()
                        .await
                        .map(|_| ())
                }
            )
        })
        .await;
        assert!(
            matches!(client, Err(QuicConnectError::Connection(_))),
            "{client:?}"
        );
        assert!(server.is_err(), "the server's handshake failed too");
        // Neither failed handshake leaves its peer on the allow-set.
        drop(_permit);
        assert!(
            !pair.a_socket.is_quic_source_allowed(pair.b_addr),
            "client side"
        );
        assert!(
            !pair.b_socket.is_quic_source_allowed(pair.a_addr),
            "server side"
        );
    })
}

/// What one handshake attempt ended with, on each side.
type Outcome = (
    Result<QuicConnection, QuicConnectError>,
    Result<QuicConnection, quinn::ConnectionError>,
);

/// How the server side of [`attempt`] answers the attempt.
enum ServerTls {
    /// As the node does: TLS pinned to the key the source was allowed with.
    Pinned,
    /// With this rustls configuration instead (an impostor's).
    Custom(Box<rustls::ServerConfig>),
}

/// `client` connects to `server` at `server_addr` with `client_config`,
/// and `server` accepts the attempt, answering it as `server_tls` says. The
/// caller has allowed the client's address on `server`.
async fn attempt(
    time: &VirtualTime,
    client: &QuicEndpoint,
    server: &QuicEndpoint,
    server_addr: SocketAddr,
    (client_config, expected_server): (quinn::ClientConfig, PeerKey),
    server_tls: ServerTls,
) -> Outcome {
    let (client, server) = (client.clone(), server.clone());
    drive(time, Duration::from_secs(60), async move {
        tokio::join!(
            client.connect_with(server_addr, client_config, expected_server),
            async {
                let incoming = server.accept().await.expect("an incoming attempt");
                match server_tls {
                    ServerTls::Pinned => incoming.establish().await,
                    ServerTls::Custom(tls) => {
                        let mut config = (*server.server).clone();
                        config.crypto = Arc::new(
                            quinn::crypto::rustls::QuicServerConfig::try_from(Arc::new(*tls))
                                .unwrap(),
                        );
                        let connection = incoming.incoming.accept_with(Arc::new(config))?.await?;
                        Ok(QuicConnection::new(
                            connection,
                            incoming.permit,
                            Some(incoming.hold),
                            incoming.peer,
                        ))
                    }
                }
            }
        )
    })
    .await
}

/// The client configuration `endpoint` builds for a server expected to hold
/// `server_key`, as `connect` does.
fn pinned_client_config(
    endpoint: &QuicEndpoint,
    server_key: &TransportPublicKey,
) -> (quinn::ClientConfig, PeerKey) {
    let peer = PeerKey::of(server_key).unwrap();
    let tls = endpoint.identity.client_tls(&peer).unwrap();
    (endpoint.client_config(Arc::new(tls)), peer)
}

/// A client configuration that presents `key` (any raw public key and
/// signer) and expects the server to hold `server_key`.
fn client_config_presenting(
    endpoint: &QuicEndpoint,
    key: Arc<rustls::sign::CertifiedKey>,
    server_key: &TransportPublicKey,
) -> (quinn::ClientConfig, PeerKey) {
    let peer = PeerKey::of(server_key).unwrap();
    let tls = config::tls_client_config(key, Arc::new(PinnedServerKey::new(&peer))).unwrap();
    (endpoint.client_config(Arc::new(tls)), peer)
}

/// A server configuration that presents `key` and expects the client to
/// hold `client_key`.
fn server_tls_presenting(
    key: Arc<rustls::sign::CertifiedKey>,
    client_key: &TransportPublicKey,
) -> rustls::ServerConfig {
    config::rustls_server_config(
        key,
        Arc::new(PinnedClientKey::new(&PeerKey::of(client_key).unwrap())),
    )
    .unwrap()
}

/// A raw public key of `claimed` signed by `signer`'s secret: an impostor
/// claiming another node's key without holding it.
fn claimed_key(
    claimed: &TransportPublicKey,
    signer: &TransportKeypair,
) -> Arc<rustls::sign::CertifiedKey> {
    Arc::new(rustls::sign::CertifiedKey::new(
        vec![rustls::pki_types::CertificateDer::from(spki_of(claimed))],
        Arc::new(super::identity::XeddsaSigningKey::new(signer).unwrap()),
    ))
}

/// TLS alert `access_denied`: the presented key is not the expected one
/// (rustls's `ApplicationVerificationFailure`).
const ACCESS_DENIED: u8 = 49;
/// TLS alert `decrypt_error`: the CertificateVerify signature does not
/// verify under the expected key (rustls's `BadSignature`).
const DECRYPT_ERROR: u8 = 51;

/// The TLS alert a connection error carries (QUIC crypto error 0x100 +
/// alert), from either side: raised locally, or received in the peer's
/// CONNECTION_CLOSE.
fn tls_alert(error: &quinn::ConnectionError) -> Option<u8> {
    let code = match error {
        quinn::ConnectionError::TransportError(e) => e.code,
        quinn::ConnectionError::ConnectionClosed(close) => close.error_code,
        quinn::ConnectionError::VersionMismatch
        | quinn::ConnectionError::ApplicationClosed(_)
        | quinn::ConnectionError::Reset
        | quinn::ConnectionError::TimedOut
        | quinn::ConnectionError::LocallyClosed
        | quinn::ConnectionError::CidsExhausted => return None,
    };
    let code = u64::from(code);
    (0x100..0x200)
        .contains(&code)
        .then_some((code - 0x100) as u8)
}

/// The handshake was refused, with TLS alert `alert` on BOTH sides, so a
/// refusal test cannot pass for an unrelated failure (a timeout, a version
/// mismatch, another alert). The client's side fails outright when it
/// refused the server; when the SERVER refused the client, TLS 1.3 lets the
/// client finish its side first (it sends its Finished without waiting for
/// a verdict on its key), so `connect` may return a connection, which the
/// server's CONNECTION_CLOSE then ends before any stream: the test drives
/// it until it closes and reads the alert from that close.
/// `client_refused_server` says which side refused.
async fn assert_refused(
    time: &VirtualTime,
    outcome: Outcome,
    client_refused_server: bool,
    alert: u8,
    what: &str,
) {
    let (client, server) = outcome;
    match &server {
        Err(error) => assert_eq!(
            tls_alert(error),
            Some(alert),
            "{what}: the server side failed with {error:?}"
        ),
        Ok(_) => panic!("{what}: the server side completed: {server:?}"),
    }
    let client_error = match client {
        Err(QuicConnectError::Connection(error)) => error,
        Ok(_) if client_refused_server => {
            panic!("{what}: the client accepted the server's key")
        }
        Err(other) => panic!("{what}: the client failed before the handshake: {other:?}"),
        Ok(connection) => {
            let inner = connection.inner().clone();
            drive(time, Duration::from_secs(60), async move {
                inner.closed().await
            })
            .await
        }
    };
    assert_eq!(
        tls_alert(&client_error),
        Some(alert),
        "{what}: the client side ended by {client_error:?}"
    );
}

/// With the keys each side expects, the handshake completes in both
/// directions over one pair of endpoints, and each side sees the other's
/// converted transport key. Every refusal test below uses the same setup,
/// so they fail because of the key and not the setup.
#[test]
fn handshakes_with_the_expected_keys_succeed_both_ways() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let pair = pair(
            "quic-identity-ok",
            "10.201.21.1:5000".parse().unwrap(),
            "10.201.21.2:5000".parse().unwrap(),
            None,
        )
        .await;
        for (client, server, client_keys, server_keys, server_addr, client_addr) in [
            (
                &pair.a,
                &pair.b,
                &pair.a_keys,
                &pair.b_keys,
                pair.b_addr,
                pair.a_addr,
            ),
            (
                &pair.b,
                &pair.a,
                &pair.b_keys,
                &pair.a_keys,
                pair.a_addr,
                pair.b_addr,
            ),
        ] {
            let _permit = server
                .allow_peer(client_addr, client_keys.public())
                .unwrap();
            let (client_conn, server_conn) = attempt(
                &pair.time,
                client,
                server,
                server_addr,
                pinned_client_config(client, server_keys.public()),
                ServerTls::Pinned,
            )
            .await;
            let (client_conn, server_conn) = (client_conn.unwrap(), server_conn.unwrap());
            assert_eq!(
                client_conn.peer_key(),
                &PeerKey::of(server_keys.public()).unwrap()
            );
            assert_eq!(
                server_conn.peer_key(),
                &PeerKey::of(client_keys.public()).unwrap()
            );
            assert_eq!(server_conn.peer_key().transport_key(), client_keys.public());
            let presented = |conn: &QuicConnection| {
                conn.inner()
                    .peer_identity()
                    .unwrap()
                    .downcast::<Vec<rustls::pki_types::CertificateDer<'static>>>()
                    .unwrap()[0]
                    .to_vec()
            };
            assert_eq!(presented(&client_conn), spki_of(server_keys.public()));
            assert_eq!(presented(&server_conn), spki_of(client_keys.public()));
        }
    })
}

/// A client that expects another key than the server holds refuses the
/// server, and the server's side fails too.
#[test]
fn a_server_without_the_expected_key_is_refused() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let pair = pair(
            "quic-identity-wrong-server",
            "10.201.22.1:5000".parse().unwrap(),
            "10.201.22.2:5000".parse().unwrap(),
            None,
        )
        .await;
        let _permit = pair.b.allow_peer(pair.a_addr, &pair.a_key()).unwrap();
        let someone_else = TransportKeypair::new();
        let outcome = attempt(
            &pair.time,
            &pair.a,
            &pair.b,
            pair.b_addr,
            pinned_client_config(&pair.a, someone_else.public()),
            ServerTls::Pinned,
        )
        .await;
        assert_refused(
            &pair.time,
            outcome,
            true,
            ACCESS_DENIED,
            "wrong expected server key",
        )
        .await;
        // `connect`, the node's path, refuses it the same way.
        let (a, b, b_addr) = (pair.a.clone(), pair.b.clone(), pair.b_addr);
        let key = someone_else.public().clone();
        let outcome = drive(&pair.time, Duration::from_secs(60), async move {
            tokio::join!(a.connect(b_addr, &key), async {
                b.accept().await.expect("an attempt").establish().await
            })
        })
        .await;
        assert_refused(
            &pair.time,
            outcome,
            true,
            ACCESS_DENIED,
            "connect with a wrong expected server key",
        )
        .await;
    })
}

/// A server that was told to expect another key than the client holds
/// refuses the client.
#[test]
fn a_client_without_the_expected_key_is_refused() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let pair = pair(
            "quic-identity-wrong-client",
            "10.201.23.1:5000".parse().unwrap(),
            "10.201.23.2:5000".parse().unwrap(),
            None,
        )
        .await;
        let someone_else = TransportKeypair::new();
        let _permit = pair
            .b
            .allow_peer(pair.a_addr, someone_else.public())
            .unwrap();
        let outcome = attempt(
            &pair.time,
            &pair.a,
            &pair.b,
            pair.b_addr,
            pinned_client_config(&pair.a, pair.b_keys.public()),
            ServerTls::Pinned,
        )
        .await;
        assert_refused(
            &pair.time,
            outcome,
            false,
            ACCESS_DENIED,
            "wrong client key",
        )
        .await;
    })
}

/// A plain Ed25519 raw public key (one not converted from any transport
/// key, signed by ring's own Ed25519) is refused on either side.
#[test]
fn a_foreign_ed25519_key_is_refused_on_either_side() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let pair = pair(
            "quic-identity-foreign",
            "10.201.24.1:5000".parse().unwrap(),
            "10.201.24.2:5000".parse().unwrap(),
            None,
        )
        .await;
        let _permit = pair.b.allow_peer(pair.a_addr, &pair.a_key()).unwrap();
        let foreign = TestIdentity::generate();
        let outcome = attempt(
            &pair.time,
            &pair.a,
            &pair.b,
            pair.b_addr,
            pinned_client_config(&pair.a, pair.b_keys.public()),
            ServerTls::Custom(Box::new(server_tls_presenting(
                foreign.key.clone(),
                pair.a_keys.public(),
            ))),
        )
        .await;
        assert_refused(
            &pair.time,
            outcome,
            true,
            ACCESS_DENIED,
            "server with a foreign key",
        )
        .await;
        let outcome = attempt(
            &pair.time,
            &pair.a,
            &pair.b,
            pair.b_addr,
            client_config_presenting(&pair.a, foreign.key.clone(), pair.b_keys.public()),
            ServerTls::Pinned,
        )
        .await;
        assert_refused(
            &pair.time,
            outcome,
            false,
            ACCESS_DENIED,
            "client with a foreign key",
        )
        .await;
    })
}

/// A man in the middle M, with its own transport key, between a and b,
/// cannot complete either leg. Toward a (which expects b's key) it fails
/// presenting its own key, and presenting b's key it cannot sign as b.
/// Toward b (which expects a's key from the address M uses) it fails
/// presenting its own key, and presenting a's key it cannot sign as a.
#[test]
fn a_man_in_the_middle_with_its_own_key_fails_both_legs() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let pair = pair(
            "quic-identity-mitm",
            "10.201.25.1:5000".parse().unwrap(),
            "10.201.25.2:5000".parse().unwrap(),
            None,
        )
        .await;
        let m_addr: SocketAddr = "10.201.25.3:5000".parse().unwrap();
        register_address_network(m_addr, &pair.network);
        let m_socket = Arc::new(DemuxSocket::wrap(
            SimulationSocket::bind(m_addr).await.unwrap(),
            None,
        ));
        let _m_pump = pump(m_socket.clone());
        let m_keys = TransportKeypair::new();
        let m =
            QuicEndpoint::bind(m_socket, m_addr, pair.time.clone(), &settings(), &m_keys).unwrap();

        // Leg to a: a dials M believing it is b.
        let _permit = m.allow_peer(pair.a_addr, &pair.a_key()).unwrap();
        let outcome = attempt(
            &pair.time,
            &pair.a,
            &m,
            m_addr,
            pinned_client_config(&pair.a, pair.b_keys.public()),
            ServerTls::Pinned,
        )
        .await;
        assert_refused(
            &pair.time,
            outcome,
            true,
            ACCESS_DENIED,
            "M toward a with its own key",
        )
        .await;
        let outcome = attempt(
            &pair.time,
            &pair.a,
            &m,
            m_addr,
            pinned_client_config(&pair.a, pair.b_keys.public()),
            ServerTls::Custom(Box::new(server_tls_presenting(
                claimed_key(pair.b_keys.public(), &m_keys),
                pair.a_keys.public(),
            ))),
        )
        .await;
        assert_refused(
            &pair.time,
            outcome,
            true,
            DECRYPT_ERROR,
            "M toward a claiming b's key",
        )
        .await;

        // Leg to b: b expects a's key from M's address.
        let _permit = pair.b.allow_peer(m_addr, &pair.a_key()).unwrap();
        let outcome = attempt(
            &pair.time,
            &m,
            &pair.b,
            pair.b_addr,
            pinned_client_config(&m, pair.b_keys.public()),
            ServerTls::Pinned,
        )
        .await;
        assert_refused(
            &pair.time,
            outcome,
            false,
            ACCESS_DENIED,
            "M toward b with its own key",
        )
        .await;
        let outcome = attempt(
            &pair.time,
            &m,
            &pair.b,
            pair.b_addr,
            client_config_presenting(
                &m,
                claimed_key(pair.a_keys.public(), &m_keys),
                pair.b_keys.public(),
            ),
            ServerTls::Pinned,
        )
        .await;
        assert_refused(
            &pair.time,
            outcome,
            false,
            DECRYPT_ERROR,
            "M toward b claiming a's key",
        )
        .await;
    })
}

/// An address stays bound to the key an attempt or connection from it was
/// authenticated against, even after the permit that allowed it is gone:
/// while the attempt, then the connection, is alive, allowing the address
/// for another key is a `KeyConflict`, and a new attempt from it reaches
/// the endpoint and is ignored by `accept` (no permit). Once the connection has closed the address takes
/// a new key.
#[test]
fn a_live_attempt_or_connection_keeps_its_address_bound_to_its_key() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let pair = pair(
            "quic-identity-hold",
            "10.201.27.1:5000".parse().unwrap(),
            "10.201.27.2:5000".parse().unwrap(),
            None,
        )
        .await;
        let other = TransportKeypair::new();
        let permit = pair.b.allow_peer(pair.a_addr, &pair.a_key()).unwrap();
        let (a, b, b_addr, b_key) = (pair.a.clone(), pair.b.clone(), pair.b_addr, pair.b_key());
        let client = tokio::spawn(async move { a.connect(b_addr, &b_key).await });
        let incoming = drive(&pair.time, Duration::from_secs(60), async move {
            b.accept().await.expect("an attempt")
        })
        .await;
        assert_eq!(incoming.peer, PeerKey::of(pair.a_keys.public()).unwrap());

        // The attempt holds the binding after the permit is gone.
        drop(permit);
        assert!(matches!(
            pair.b.allow_peer(pair.a_addr, other.public()),
            Err(QuicAllowError::KeyConflict)
        ));
        assert!(
            pair.b.expected_clients.get(pair.a_addr).is_none(),
            "no permit"
        );

        // So does the connection it becomes.
        let server = drive(&pair.time, Duration::from_secs(60), async move {
            incoming.establish().await
        })
        .await
        .expect("handshake");
        let client = client.await.unwrap().expect("client handshake");
        assert_eq!(server.peer_key().transport_key(), pair.a_keys.public());
        assert!(matches!(
            pair.b.allow_peer(pair.a_addr, other.public()),
            Err(QuicAllowError::KeyConflict)
        ));
        // A second attempt from the address, while only the connection
        // holds its key (no permit), reaches b's queue and is ignored by
        // accept(), not accepted under the held key.
        let forwarded_before = pair
            .b_socket
            .quic_outcome_count(crate::transport::metrics::QuicIngressOutcome::Forwarded);
        let (a2, b_addr2, b_key2) = (pair.a.clone(), pair.b_addr, pair.b_key());
        let second = tokio::spawn(async move { a2.connect(b_addr2, &b_key2).await.map(|_| ()) });
        let (b2, time) = (pair.b.clone(), pair.time.clone());
        let accepted = drive(&pair.time, Duration::from_secs(60), async move {
            tokio::select! {
                incoming = b2.accept() => incoming.map(|_| ()),
                _ = time.sleep(Duration::from_secs(5)) => None,
            }
        })
        .await;
        assert!(
            pair.b_socket
                .quic_outcome_count(crate::transport::metrics::QuicIngressOutcome::Forwarded)
                > forwarded_before,
            "the second Initial reached b's ingress queue"
        );
        assert!(
            accepted.is_none(),
            "an attempt from an address with no permit is ignored"
        );
        second.abort();

        // The same key may be allowed again meanwhile.
        drop(pair.b.allow_peer(pair.a_addr, &pair.a_key()).unwrap());

        // Closed: the binding goes, and another key may take the address.
        drop(client);
        let server_conn = server.inner().clone();
        drive(&pair.time, Duration::from_secs(60), async move {
            server_conn.closed().await;
        })
        .await;
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        let _moved = pair
            .b
            .allow_peer(pair.a_addr, other.public())
            .expect("the address takes a new key once nothing uses the old one");
        drop(server);
    })
}

/// `allow_peer` keeps one key per address: a second permit for the same
/// key is fine, another key is refused while the first is held, and the
/// address takes a new key once every permit is gone.
#[test]
fn an_allowed_address_keeps_its_key_until_released() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let pair = pair(
            "quic-identity-permits",
            "10.201.26.1:5000".parse().unwrap(),
            "10.201.26.2:5000".parse().unwrap(),
            None,
        )
        .await;
        let other = TransportKeypair::new();
        let first = pair.b.allow_peer(pair.a_addr, &pair.a_key()).unwrap();
        let second = pair.b.allow_peer(pair.a_addr, &pair.a_key()).unwrap();
        assert!(matches!(
            pair.b.allow_peer(pair.a_addr, other.public()),
            Err(QuicAllowError::KeyConflict)
        ));
        // The refused call left the address allowed with a's key.
        assert_eq!(
            pair.b.expected_clients.get(pair.a_addr),
            Some(PeerKey::of(pair.a_keys.public()).unwrap())
        );
        drop(first);
        assert!(pair.b_socket.is_quic_source_allowed(pair.a_addr));
        assert!(pair.b.expected_clients.get(pair.a_addr).is_some());
        drop(second);
        assert!(!pair.b_socket.is_quic_source_allowed(pair.a_addr));
        assert!(pair.b.expected_clients.get(pair.a_addr).is_none());
        let _third = pair.b.allow_peer(pair.a_addr, other.public()).unwrap();
        assert_eq!(
            pair.b.expected_clients.get(pair.a_addr),
            Some(PeerKey::of(other.public()).unwrap())
        );
    })
}

/// The same seed gives the same event-level trace: the same datagrams, in
/// the same order, at the same virtual times, between the same endpoints,
/// of the same lengths and header forms. The BYTES differ between runs:
/// rustls draws its key exchange and handshake randoms from ring's OS RNG,
/// which `GlobalRng` does not reach, so ciphertexts (and the
/// header-protected bits of the first byte) are not repeatable. The key
/// material (reset key, token key, CID MAC key) is from the OS too, so the
/// MAC half of every connection ID differs between runs; the trace does not
/// include bytes, so it compares only what stays deterministic.
#[test]
fn the_same_seed_gives_the_same_event_trace() {
    bounded(|| async move {
        async fn run(subnet: u8) -> Vec<(u64, bool, usize, bool)> {
            // Let tasks left over from an earlier run (connection drivers
            // closing) finish before seeding, so they cannot draw from this
            // run's seeded RNG.
            for _ in 0..1_000 {
                tokio::task::yield_now().await;
            }
            let _seed = GlobalRng::seed_guard(SEED);
            let a_addr: SocketAddr = format!("10.201.{subnet}.1:5000").parse().unwrap();
            let b_addr: SocketAddr = format!("10.201.{subnet}.2:5000").parse().unwrap();
            let pair = pair(&format!("quic-determinism-{subnet}"), a_addr, b_addr, None).await;
            let (received, _client, _server) = pair
                .transfer(payload(256 * 1024), Duration::from_secs(60))
                .await;
            assert_eq!(received.len(), 256 * 1024);
            pair.sent()
                .into_iter()
                .map(|d| {
                    (
                        d.at_ns,
                        d.from == a_addr,
                        d.bytes.len(),
                        // The header form bit is not header-protected.
                        d.bytes[0] & 0x80 != 0,
                    )
                })
                .collect()
        }
        let first = run(3).await;
        let second = run(4).await;
        assert!(first.len() > 100, "a real transfer was traced");
        assert_eq!(first, second, "the event trace must repeat under one seed");
    })
}

/// A bulk QUIC transfer through a socket with an egress cap stays within the
/// cap's bound: over every interval ending at a send, the bytes sent are at
/// most `burst + rate * interval` (every datagram the endpoint sends is
/// admitted by the cap; nothing is charged unpaced, so nothing is forgiven).
/// Without the cap the same transfer breaks that bound, so the check is not
/// one any transfer passes.
#[test]
fn a_bulk_transfer_under_the_egress_cap_stays_within_its_bound() {
    bounded(|| async move {
        const RATE: u64 = 2_000_000;
        let check = |sent: &[Sent], from: SocketAddr, burst: u64| -> Result<(), String> {
            let sends: Vec<(u64, u64)> = sent
                .iter()
                .filter(|d| d.from == from)
                .map(|d| (d.at_ns, d.bytes.len() as u64))
                .collect();
            for end in 0..sends.len() {
                let mut bytes = 0u64;
                for start in (0..=end).rev() {
                    bytes += sends[start].1;
                    let interval_ns = sends[end].0 - sends[start].0;
                    let allowed =
                        burst + (RATE as u128 * interval_ns as u128 / 1_000_000_000) as u64;
                    if bytes > allowed {
                        return Err(format!(
                            "{bytes} bytes in {interval_ns} ns (sends {start}..={end}), allowed {allowed}"
                        ));
                    }
                }
            }
            Ok(())
        };
        let burst = EgressCap::new(RATE, VirtualTime::new()).burst_bytes();

        let _seed = GlobalRng::seed_guard(SEED);
        let capped = pair(
            "quic-cap",
            "10.201.5.1:5000".parse().unwrap(),
            "10.201.5.2:5000".parse().unwrap(),
            Some(RATE),
        )
        .await;
        let (received, _c, _s) = capped
            .transfer(payload(MIB), Duration::from_secs(120))
            .await;
        assert_eq!(received.len(), MIB);
        let sent = capped.sent();
        check(&sent, capped.a_addr, burst).expect("the capped transfer stays within the bound");
        let stats = capped.a_socket.egress_cap().expect("a cap").stats();
        let a_bytes: u64 = sent
            .iter()
            .filter(|d| d.from == capped.a_addr)
            .map(|d| d.bytes.len() as u64)
            .sum();
        assert_eq!(stats.bytes, a_bytes, "every byte sent was charged, once");
        assert_eq!(
            stats.packets[crate::transport::metrics::EgressDecision::Unpaced as usize],
            0,
            "nothing went around the cap's admission"
        );
        let elapsed_ns = sent.last().unwrap().at_ns - sent.first().unwrap().at_ns;
        assert!(
            elapsed_ns as u128 * RATE as u128 / 1_000_000_000 >= (MIB as u128 * 9 / 10),
            "1 MiB took {elapsed_ns} ns at {RATE} B/s"
        );
        drop(capped);

        let uncapped = pair(
            "quic-nocap",
            "10.201.6.1:5000".parse().unwrap(),
            "10.201.6.2:5000".parse().unwrap(),
            None,
        )
        .await;
        let (received, _c, _s) = uncapped
            .transfer(payload(MIB), Duration::from_secs(120))
            .await;
        assert_eq!(received.len(), MIB);
        assert!(
            check(&uncapped.sent(), uncapped.a_addr, burst).is_err(),
            "without the cap the same transfer must exceed the bound, or the check proves nothing"
        );
    })
}

/// A dual-stack endpoint (`[::]`) completes handshakes with an IPv4 peer in
/// both directions, over real UDP sockets on loopback: it receives the
/// peer's datagrams from a plain IPv4 address and sends to it on an IPv6
/// socket.
#[test]
fn a_dual_stack_endpoint_talks_to_an_ipv4_peer() {
    bounded(|| async move {
        use crate::transport::UdpTransportSocket;
        let _seed = GlobalRng::seed_guard(SEED);
        // Skip, saying so, where the host cannot do dual-stack: no IPv6, or
        // IPv6 sockets forced to v6-only.
        let dual_stack_available = std::net::UdpSocket::bind((Ipv6Addr::UNSPECIFIED, 0))
            .and_then(|v6| {
                let port = v6.local_addr()?.port();
                let v4 = std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))?;
                v4.send_to(b"probe", (Ipv4Addr::LOCALHOST, port))?;
                v6.set_read_timeout(Some(Duration::from_secs(2)))?;
                let mut buf = [0u8; 8];
                v6.recv_from(&mut buf).map(|_| ())
            })
            .is_ok();
        if !dual_stack_available {
            // REQUIRE_DUAL_STACK=1 turns this skip into a failure on a host
            // known to have dual-stack IPv6. Nothing sets it in CI today, so
            // there the skip is only the printed line below.
            assert!(
                std::env::var("REQUIRE_DUAL_STACK").as_deref() != Ok("1"),
                "REQUIRE_DUAL_STACK=1 but this host has no dual-stack IPv6: \
                 a_dual_stack_endpoint_talks_to_an_ipv4_peer cannot run"
            );
            // stdout as well as stderr, so the skip shows in captured output.
            println!(
                "SKIPPED a_dual_stack_endpoint_talks_to_an_ipv4_peer: no dual-stack IPv6 here \
                 (set REQUIRE_DUAL_STACK=1 to make this a failure)"
            );
            eprintln!(
                "SKIPPED a_dual_stack_endpoint_talks_to_an_ipv4_peer: no dual-stack IPv6 here"
            );
            return;
        }
        // Bind each socket on the address it serves, retrying on a port another
        // process took between the reservation and the bind.
        async fn bind_free(ip: IpAddr) -> (u16, Arc<DemuxSocket<UdpTransportSocket>>) {
            for _ in 0..20 {
                let port = crate::test_utils::reserve_local_port().unwrap();
                crate::test_utils::release_local_port(port);
                if let Ok(socket) =
                    DemuxSocket::<UdpTransportSocket>::bind(SocketAddr::new(ip, port)).await
                {
                    return (port, Arc::new(socket));
                }
            }
            panic!("no free port on {ip} after 20 tries");
        }
        let (v6_port, v6_socket) = bind_free(IpAddr::V6(Ipv6Addr::UNSPECIFIED)).await;
        let (v4_port, v4_socket) = bind_free(IpAddr::V4(Ipv4Addr::LOCALHOST)).await;
        let v6_bind = SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), v6_port);
        let v4_bind = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), v4_port);
        let _pumps = [pump(v6_socket.clone()), pump(v4_socket.clone())];
        let time = RealTime::new();
        let dual_keys = TransportKeypair::new();
        let v4_keys = TransportKeypair::new();
        let dual =
            QuicEndpoint::bind(v6_socket, v6_bind, time.clone(), &settings(), &dual_keys).unwrap();
        let v4 = QuicEndpoint::bind(v4_socket, v4_bind, time, &settings(), &v4_keys).unwrap();
        let dual_at = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), v6_port);
        async fn within<F: std::future::Future>(
            f: F,
        ) -> Result<F::Output, tokio::time::error::Elapsed> {
            tokio::time::timeout(Duration::from_secs(20), f).await
        }

        // The IPv4 peer connects to the dual-stack endpoint.
        let _permit = dual.allow_peer(v4_bind, v4_keys.public()).unwrap();
        let (client, server) = within(async {
            tokio::join!(v4.connect(dual_at, dual_keys.public()), async {
                dual.accept().await.expect("an attempt").establish().await
            })
        })
        .await
        .expect("handshake within 20 s");
        let client = client.expect("v4 -> dual handshake");
        let server = server.expect("dual accepted");
        assert_eq!(
            server.inner().remote_address(),
            SocketAddr::new(Ipv4Addr::LOCALHOST.to_ipv6_mapped().into(), v4_port),
            "quinn sees an IPv4 peer of an IPv6 socket in mapped form"
        );
        drop((client, server));

        // The dual-stack endpoint connects to the IPv4 peer.
        let _permit = v4.allow_peer(dual_at, dual_keys.public()).unwrap();
        let (client, server) = within(async {
            tokio::join!(dual.connect(v4_bind, v4_keys.public()), async {
                v4.accept().await.expect("an attempt").establish().await
            })
        })
        .await
        .expect("handshake within 20 s");
        client.expect("dual -> v4 handshake");
        server.expect("v4 accepted");
    })
}

/// A short-header datagram for a connection the endpoint does not know gets
/// no reply when its connection ID was not made with the endpoint's key,
/// so a prober learns nothing; quinn's own generators would answer it with
/// a stateless reset.
#[test]
fn connection_ids_validate_only_with_the_key_that_made_them() {
    use super::cid::{CID_LEN, MacConnectionIdGenerator};
    use quinn::ConnectionIdGenerator;
    let _seed = GlobalRng::seed_guard(SEED);
    let key = MacConnectionIdGenerator::new_key().unwrap();
    let mut generator = MacConnectionIdGenerator::new(key);
    let other = MacConnectionIdGenerator::new(MacConnectionIdGenerator::new_key().unwrap());
    assert_eq!(generator.cid_len(), CID_LEN);
    assert_eq!(generator.cid_lifetime(), None);
    for _ in 0..100 {
        let cid = generator.generate_cid();
        assert_eq!(cid.len(), CID_LEN);
        assert!(generator.validate(&cid).is_ok(), "its own ID");
        assert!(
            MacConnectionIdGenerator::new(key).validate(&cid).is_ok(),
            "another generator with the same key (as quinn uses per connection)"
        );
        assert!(other.validate(&cid).is_err(), "a different key");
        let mut forged = cid.to_vec();
        forged[CID_LEN - 1] ^= 1;
        assert!(
            generator
                .validate(&quinn::ConnectionId::new(&forged))
                .is_err(),
            "one flipped MAC bit"
        );
    }
    assert!(
        generator
            .validate(&quinn::ConnectionId::new(&[0u8; 8]))
            .is_err(),
        "a wrong length"
    );
}

/// Every endpoint setting pinned in `config::endpoint_config` that quinn's
/// `Debug` shows. (The connection-ID generator is pinned by behaviour in
/// `a_short_header_packet_with_a_forged_id_gets_no_reply`.)
#[test]
fn endpoint_config_pins_every_value() {
    let _seed = GlobalRng::seed_guard(SEED);
    let config = config::endpoint_config().unwrap();
    let debug = format!("{config:?}");
    assert_eq!(
        config.get_max_udp_payload_size(),
        u64::from(MAX_UDP_PAYLOAD)
    );
    for (field, expected) in [
        ("max_udp_payload_size", "1200"),
        ("grease_quic_bit", "false"),
        ("supported_versions", "[1]"),
    ] {
        assert_eq!(debug_field(&debug, field), expected, "{field} in {debug}");
    }
    assert!(
        debug_field(&debug, "rng_seed").starts_with("Some(["),
        "{debug}"
    );
    // quinn's RNG seed follows GlobalRng: the same seed, the same quinn seed.
    let again = format!("{:?}", {
        let _seed = GlobalRng::seed_guard(SEED);
        config::endpoint_config().unwrap()
    });
    assert_eq!(debug, again, "the rng seed follows GlobalRng");
}

/// The transport fields `config::transport_config` pins, with the value
/// each must render as, for a window of `window` bytes.
fn pinned_transport_fields(window: u64) -> Vec<(&'static str, String)> {
    vec![
        ("max_concurrent_bidi_streams", MAX_BIDI_STREAMS.to_string()),
        ("max_concurrent_uni_streams", MAX_UNI_STREAMS.to_string()),
        ("stream_receive_window", window.to_string()),
        ("receive_window", window.to_string()),
        ("send_window", window.to_string()),
        ("send_fairness", "true".into()),
        ("packet_threshold", "3".into()),
        ("time_threshold", "1.125".into()),
        ("initial_rtt", "333ms".into()),
        ("initial_mtu", MAX_UDP_PAYLOAD.to_string()),
        ("min_mtu", MAX_UDP_PAYLOAD.to_string()),
        ("mtu_discovery_config", "None".into()),
        ("pad_to_mtu", "false".into()),
        ("ack_frequency_config", "None".into()),
        ("persistent_congestion_threshold", "3".into()),
        ("crypto_buffer_size", "16384".into()),
        ("allow_spin", "false".into()),
        ("datagram_receive_buffer_size", "None".into()),
        ("datagram_send_buffer_size", "0".into()),
        ("enable_segmentation_offload", "false".into()),
    ]
}

/// Every transport setting pinned in `config::transport_config`, for the
/// real clock (keepalive on, 120 s idle) and the virtual one (no keepalive,
/// its long idle timeout). Cubic is checked on a live connection in the
/// handshake test; quinn's `Debug` omits the congestion controller.
#[test]
fn transport_config_pins_every_value() {
    let window = settings().window_bytes();
    assert_eq!(
        window, MAX_WINDOW_BYTES,
        "these settings reach the window cap"
    );
    let real = format!(
        "{:?}",
        config::transport_config(&settings(), &RealTime::new())
    );
    let virt = format!(
        "{:?}",
        config::transport_config(&settings(), &VirtualTime::new())
    );
    for (field, expected) in pinned_transport_fields(window) {
        assert_eq!(debug_field(&real, field), expected, "{field} in {real}");
        assert_eq!(debug_field(&virt, field), expected, "{field} in {virt}");
    }
    assert_eq!(
        debug_field(&real, "keep_alive_interval"),
        format!("Some({KEEPALIVE_INTERVAL:?})")
    );
    assert_eq!(debug_field(&real, "max_idle_timeout"), "Some(120000)");
    assert_eq!(debug_field(&virt, "keep_alive_interval"), "None");
    let virt_idle_ms = VirtualTime::new().connection_idle_timeout().as_millis();
    assert_eq!(
        debug_field(&virt, "max_idle_timeout"),
        format!("Some({virt_idle_ms})")
    );
}

/// Every client setting `config::client_config` pins that quinn's `Debug`
/// shows: the transport settings and QUIC version 1. The token store and
/// the initial destination CID provider are not shown (see the config
/// module doc).
#[test]
fn client_config_pins_every_value() {
    let _seed = GlobalRng::seed_guard(SEED);
    let keys = TransportKeypair::new();
    let identity = QuicIdentity::new(&keys).unwrap();
    let crypto = Arc::new(
        identity
            .client_tls(&PeerKey::of(TransportKeypair::new().public()).unwrap())
            .unwrap(),
    );
    let client = config::client_config(
        crypto,
        Arc::new(config::transport_config(&settings(), &VirtualTime::new())),
    );
    let debug = format!("{client:?}");
    assert_eq!(debug_field(&debug, "version"), "1", "{debug}");
    let transport = debug_field(&debug, "transport");
    for (field, expected) in pinned_transport_fields(settings().window_bytes()) {
        assert_eq!(
            debug_field(&transport, field),
            expected,
            "{field} in {transport}"
        );
    }
}

/// Every server setting pinned in `config::server_config`.
#[test]
fn server_config_pins_every_value() {
    let _seed = GlobalRng::seed_guard(SEED);
    let keys = TransportKeypair::new();
    let identity = QuicIdentity::new(&keys).unwrap();
    let crypto = Arc::new(
        identity
            .server_tls(&PeerKey::of(TransportKeypair::new().public()).unwrap())
            .unwrap(),
    );
    let server = config::server_config(
        crypto,
        Arc::new(config::transport_config(&settings(), &VirtualTime::new())),
        config::system_clock(VirtualTime::new()),
    )
    .unwrap();
    let debug = format!("{server:?}");
    for (field, expected) in [
        ("migration", "false".to_string()),
        ("retry_token_lifetime", "15s".into()),
        ("max_incoming", MAX_INCOMING.to_string()),
        ("incoming_buffer_size", INCOMING_BUFFER_BYTES.to_string()),
        (
            "incoming_buffer_size_total",
            INCOMING_BUFFER_TOTAL_BYTES.to_string(),
        ),
        ("preferred_address_v4", "None".into()),
        ("preferred_address_v6", "None".into()),
    ] {
        assert_eq!(debug_field(&debug, field), expected, "{field} in {debug}");
    }
    let tokens = debug_field(&debug, "validation_token");
    assert_eq!(
        debug_field(&tokens, "sent"),
        "0",
        "no validation tokens: {tokens}"
    );
    let transport = debug_field(&debug, "transport");
    for (field, expected) in pinned_transport_fields(settings().window_bytes()) {
        assert_eq!(
            debug_field(&transport, field),
            expected,
            "{field} in {transport}"
        );
    }
}

/// The TLS settings both sides pin: TLS 1.3 only with the ring provider,
/// the fixed ALPN, no SNI, no resumption, no early data, no tickets, no
/// certificate compression.
#[test]
fn tls_configs_pin_every_value() {
    let _seed = GlobalRng::seed_guard(SEED);
    let identity = QuicIdentity::new(&TransportKeypair::new()).unwrap();
    let peer = PeerKey::of(TransportKeypair::new().public()).unwrap();
    let server = config::rustls_server_config(
        identity.certified_key(),
        Arc::new(PinnedClientKey::new(&peer)),
    )
    .unwrap();
    let client = config::rustls_client_config(
        identity.certified_key(),
        Arc::new(PinnedServerKey::new(&peer)),
    )
    .unwrap();
    // TLS 1.3 only: QUIC carries nothing else (RFC 9001 section 4.2),
    // rustls's QUIC mode refuses other versions, and the builders are given
    // only TLS13. The provider is ring's.
    let ring = format!(
        "{:?}",
        rustls::crypto::ring::default_provider().cipher_suites
    );
    assert_eq!(
        format!("{:?}", server.crypto_provider().cipher_suites),
        ring
    );
    assert_eq!(
        format!("{:?}", client.crypto_provider().cipher_suites),
        ring
    );
    assert_eq!(server.alpn_protocols, vec![ALPN.to_vec()]);
    assert_eq!(server.max_early_data_size, 0);
    assert_eq!(server.send_tls13_tickets, 0);
    assert!(!server.session_storage.can_cache(), "no session storage");
    assert!(server.cert_compressors.is_empty());
    assert!(server.cert_decompressors.is_empty());
    assert_eq!(client.alpn_protocols, vec![ALPN.to_vec()]);
    assert!(!client.enable_sni);
    assert!(!client.enable_early_data);
    assert_eq!(
        format!("{:?}", client.resumption),
        format!("{:?}", rustls::client::Resumption::disabled())
    );
    assert!(client.cert_compressors.is_empty());
    assert!(client.cert_decompressors.is_empty());
    // quinn accepts both (TLS 1.3 with its initial suite).
    assert!(identity.server_tls(&peer).is_ok());
    assert!(identity.client_tls(&peer).is_ok());
    assert!(identity.refusing_server_tls().is_ok());
}

/// The window is 5% of memory split over every connection's receive and
/// send windows, clamped to [128 KiB, 2 MiB].
#[test]
fn windows_are_a_clamped_share_of_memory() {
    const GIB: usize = 1024 * 1024 * 1024;
    let window = config::connection_window_bytes;
    // 4 GiB / 20 / (2 * 200) = 536 870 bytes: inside the clamp.
    assert_eq!(window(4 * GIB, 200), (4 * GIB / 20 / 400) as u64);
    assert_eq!(window(256 * 1024 * 1024, 200), MIN_WINDOW_BYTES, "floor");
    assert_eq!(window(64 * GIB, 4), MAX_WINDOW_BYTES, "ceiling");
    assert_eq!(
        window(GIB, 0),
        MAX_WINDOW_BYTES,
        "zero connections does not divide by zero"
    );
}

/// The connection receive window, the stream receive window and the send
/// window are at most `MAX_INBOUND_STREAM_BYTES_PER_CONNECTION` (read from
/// the constant, not copied) at every memory limit and connection count, the
/// stream window is at most the connection window, and the transport
/// configuration quinn is given carries exactly those values. The cap does
/// not bind with today's 2 MiB clamp, so this guards only a future change of
/// the window constants. It says nothing about a connection's total memory,
/// which is not accounted (see `config::connection_window_bytes`).
#[test]
fn windows_never_exceed_the_reassembly_allowance() {
    use crate::transport::peer_connection::streaming::MAX_INBOUND_STREAM_BYTES_PER_CONNECTION as CAP;
    const MIB: usize = 1024 * 1024;
    let limits = [0, MIB, 256 * MIB, 1024 * MIB, 64 * 1024 * MIB, usize::MAX];
    let connections = [0, 1, 4, 200, 10_000];
    for &memory_limit in &limits {
        for &max_connections in &connections {
            let settings = QuicSettings {
                max_connections,
                memory_limit,
            };
            let window = settings.window_bytes();
            assert!(
                window <= CAP,
                "{memory_limit} B / {max_connections} connections: window {window} > {CAP}"
            );
            let debug = format!(
                "{:?}",
                config::transport_config(&settings, &VirtualTime::new())
            );
            let field = |name: &str| debug_field(&debug, name).parse::<u64>().unwrap();
            let (stream, connection, send) = (
                field("stream_receive_window"),
                field("receive_window"),
                field("send_window"),
            );
            assert_eq!((stream, connection, send), (window, window, window));
            assert!(stream <= connection && connection <= CAP && send <= CAP);
        }
    }
}

/// `transport-quic` values: unset and `off` are off, `prefer` (any case)
/// is prefer, and anything else is off, never an error, so a value a later
/// release adds cannot stop this binary from starting.
#[test]
fn transport_quic_values_and_unknown_ones() {
    assert_eq!(QuicMode::from_config_value(None), QuicMode::Off);
    assert_eq!(QuicMode::from_config_value(Some("off")), QuicMode::Off);
    assert_eq!(
        QuicMode::from_config_value(Some("prefer")),
        QuicMode::Prefer
    );
    assert_eq!(
        QuicMode::from_config_value(Some(" PREFER ")),
        QuicMode::Prefer
    );
    assert_eq!(QuicMode::from_config_value(Some("force")), QuicMode::Off);
    assert_eq!(QuicMode::from_config_value(Some("")), QuicMode::Off);
    assert_eq!(QuicMode::default(), QuicMode::Off);
}

/// The socket sends one datagram per transmit and receives one per call.
#[test]
fn the_socket_never_batches() {
    // Checked on the trait methods quinn reads; a value above 1 would let
    // quinn hand the cap a GSO batch larger than its burst.
    let check = |socket: &dyn quinn::AsyncUdpSocket| {
        assert_eq!(socket.max_transmit_segments(), 1);
        assert_eq!(socket.max_receive_segments(), 1);
        assert!(socket.may_fragment(), "so quinn never runs MTU discovery");
    };
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let network = "quic-segments";
        let addr: SocketAddr = "10.201.7.1:5000".parse().unwrap();
        register_network_time_source(network, VirtualTime::new());
        register_address_network(addr, network);
        let demux = Arc::new(DemuxSocket::wrap(
            SimulationSocket::bind(addr).await.unwrap(),
            None,
        ));
        let ingress = demux.enable_quic_ingress().unwrap();
        let socket = super::socket::QuicSocket::new(demux, ingress, addr);
        check(&socket);
        unregister_network_time_source(network);
    });
}

/// The timer quinn arms follows the time source: it fires at its deadline
/// on virtual time, a later `reset` moves it without arming another sleep,
/// and an earlier one re-arms it.
#[test]
fn the_runtime_timer_follows_virtual_time() {
    use quinn::Runtime;
    use std::task::{Context, Poll};
    let time = VirtualTime::new();
    let runtime = super::runtime::QuicRuntime::new(
        time.clone(),
        super::liveness::EndpointHealth::new("127.0.0.1:1".parse().unwrap()),
    );
    let start = runtime.now();
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut timer = runtime.timer_at(start + Duration::from_millis(10));
    assert_eq!(timer.as_mut().poll(&mut cx), Poll::Pending);
    let armed = time.pending_wakeup_count();
    timer.as_mut().reset(start + Duration::from_millis(30));
    assert_eq!(timer.as_mut().poll(&mut cx), Poll::Pending);
    assert_eq!(
        time.pending_wakeup_count(),
        armed,
        "a later deadline keeps the armed sleep"
    );
    time.advance(Duration::from_millis(10));
    assert_eq!(
        timer.as_mut().poll(&mut cx),
        Poll::Pending,
        "not yet at 30 ms"
    );
    time.advance(Duration::from_millis(20));
    assert_eq!(timer.as_mut().poll(&mut cx), Poll::Ready(()));
    assert_eq!(
        runtime.now() - start,
        Duration::from_millis(30),
        "quinn's clock is the time source"
    );
    timer
        .as_mut()
        .reset(runtime.now() + Duration::from_millis(50));
    assert_eq!(timer.as_mut().poll(&mut cx), Poll::Pending);
    timer
        .as_mut()
        .reset(runtime.now() + Duration::from_millis(5));
    assert_eq!(timer.as_mut().poll(&mut cx), Poll::Pending);
    time.advance(Duration::from_millis(5));
    assert_eq!(
        timer.as_mut().poll(&mut cx),
        Poll::Ready(()),
        "an earlier deadline re-armed the timer"
    );
}

/// With `transport-quic` off the transport builds no endpoint, so a
/// QUIC-class datagram to its port finds no ingress; with it on the ingress
/// exists and its allow-set is empty, so the same datagram from a stranger
/// is dropped as not allowed. Either way nothing answers it.
#[test]
fn the_transport_builds_an_endpoint_only_with_the_key_on() {
    bounded(|| async move {
        use crate::transport::UdpTransportSocket;
        use crate::transport::connection_handler::{UdpTransportConfig, open_udp_transport};
        use crate::transport::metrics::{QuicIngressOutcome, TRANSPORT_METRICS};
        for quic in [None, Some(settings())] {
            let port = crate::test_utils::reserve_local_port().unwrap();
            crate::test_utils::release_local_port(port);
            let _transport = open_udp_transport::<UdpTransportSocket>(
                crate::transport::TransportKeypair::new(),
                IpAddr::V4(Ipv4Addr::LOCALHOST),
                port,
                false,
                UdpTransportConfig {
                    quic,
                    ..UdpTransportConfig::default()
                },
            )
            .await
            .unwrap();
            let prober = std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
            prober.set_nonblocking(true).unwrap();
            let from = prober.local_addr().unwrap();
            TRANSPORT_METRICS.watch_quic_ingress(from);
            // A QUIC Initial-shaped datagram (long header, version 1), padded.
            let mut initial = vec![0xC0, 0, 0, 0, 1, 8];
            initial.resize(1200, 0);
            prober
                .send_to(&initial, (Ipv4Addr::LOCALHOST, port))
                .unwrap();
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            while TRANSPORT_METRICS.quic_ingress_from(from).is_empty() {
                assert!(tokio::time::Instant::now() < deadline, "datagram not seen");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let expected = if quic.is_some() {
                QuicIngressOutcome::NotAllowed
            } else {
                QuicIngressOutcome::NoIngress
            };
            assert_eq!(TRANSPORT_METRICS.quic_ingress_from(from), vec![expected]);
            // Give a reply time to arrive, with the runtime free to run the
            // listener (and the endpoint, when there is one).
            tokio::time::sleep(Duration::from_millis(300)).await;
            let mut buf = [0u8; 2048];
            assert_eq!(
                prober
                    .recv_from(&mut buf)
                    .map_err(|e| e.kind())
                    .unwrap_err(),
                std::io::ErrorKind::WouldBlock,
                "nothing answered the prober (quic = {})",
                quic.is_some()
            );
        }
    })
}

/// What [`ScriptedSocket::try_send_to`] does.
#[derive(Clone, Copy, PartialEq, Eq)]
enum SendMode {
    Send,
    WouldBlock,
    Fail,
}

/// An inner socket for the adapter's own paths: sends follow `mode`, and
/// receives return the scripted datagrams, then wait.
struct ScriptedSocket {
    mode: Mutex<SendMode>,
    sent: Mutex<Vec<(Vec<u8>, SocketAddr)>>,
    inbox: Mutex<std::collections::VecDeque<(Vec<u8>, SocketAddr)>>,
}

impl ScriptedSocket {
    fn new(mode: SendMode, inbox: Vec<(Vec<u8>, SocketAddr)>) -> Self {
        Self {
            mode: Mutex::new(mode),
            sent: Mutex::new(Vec::new()),
            inbox: Mutex::new(inbox.into()),
        }
    }
}

impl Socket for ScriptedSocket {
    async fn bind(_addr: SocketAddr) -> std::io::Result<Self> {
        unreachable!()
    }

    async fn recv_from(&self, buf: &mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
        let next = self.inbox.lock().unwrap().pop_front();
        match next {
            Some((bytes, from)) => {
                buf[..bytes.len()].copy_from_slice(&bytes);
                Ok((bytes.len(), from))
            }
            None => std::future::pending().await,
        }
    }

    async fn send_to(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
        self.try_send_to(buf, target)
    }

    fn send_to_blocking(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
        self.try_send_to(buf, target)
    }

    fn try_send_to(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
        match *self.mode.lock().unwrap() {
            SendMode::Send => {
                self.sent.lock().unwrap().push((buf.to_vec(), target));
                Ok(buf.len())
            }
            SendMode::WouldBlock => Err(std::io::ErrorKind::WouldBlock.into()),
            SendMode::Fail => Err(std::io::ErrorKind::PermissionDenied.into()),
        }
    }
}

/// An adapter over a [`ScriptedSocket`], with an optional cap on `time`.
fn scripted(
    local: SocketAddr,
    mode: SendMode,
    inbox: Vec<(Vec<u8>, SocketAddr)>,
    cap: Option<(u64, &VirtualTime)>,
) -> (
    Arc<super::socket::QuicSocket<ScriptedSocket>>,
    Arc<DemuxSocket<ScriptedSocket>>,
) {
    let demux = Arc::new(DemuxSocket::wrap(
        ScriptedSocket::new(mode, inbox),
        cap.map(|(rate, time)| EgressCap::new(rate, time.clone())),
    ));
    let ingress = demux.enable_quic_ingress().unwrap();
    (
        Arc::new(super::socket::QuicSocket::new(
            demux.clone(),
            ingress,
            local,
        )),
        demux,
    )
}

fn transmit(contents: &[u8], destination: SocketAddr) -> quinn::udp::Transmit<'_> {
    quinn::udp::Transmit {
        destination,
        ecn: None,
        contents,
        segment_size: None,
        src_ip: None,
    }
}

/// A sent datagram is charged to the cap exactly once, as an admitted
/// (not unpaced) send, and an IPv4-mapped destination reaches the socket in
/// plain form.
#[test]
fn a_sent_datagram_is_charged_once() {
    use crate::transport::metrics::EgressDecision;
    use quinn::AsyncUdpSocket;
    let time = VirtualTime::new();
    let local: SocketAddr = "10.201.8.1:5000".parse().unwrap();
    let (socket, demux) = scripted(local, SendMode::Send, Vec::new(), Some((1_000_000, &time)));
    let target_v4: SocketAddr = "10.201.8.2:5000".parse().unwrap();
    let mapped = SocketAddr::new(Ipv4Addr::new(10, 201, 8, 2).to_ipv6_mapped().into(), 5000);
    socket.try_send(&transmit(&[0xC0; 1000], mapped)).unwrap();
    let stats = demux.egress_cap().unwrap().stats();
    assert_eq!(stats.bytes, 1000);
    assert_eq!(stats.packets[EgressDecision::Immediate as usize], 1);
    assert_eq!(stats.packets[EgressDecision::Unpaced as usize], 0);
    assert_eq!(
        *demux.inner_for_test().sent.lock().unwrap(),
        vec![(vec![0xC0; 1000], target_v4)]
    );
}

/// With the cap short of the datagram the send is `WouldBlock`, nothing is
/// sent and nothing is charged; quinn then waits in `poll_writable`, which is
/// `Pending` until the cap has a full datagram and `Ready` once it does.
#[test]
fn a_cap_short_of_the_datagram_is_would_block() {
    use quinn::AsyncUdpSocket;
    use std::task::Context;
    let time = VirtualTime::new();
    let local: SocketAddr = "10.201.9.1:5000".parse().unwrap();
    let (socket, demux) = scripted(local, SendMode::Send, Vec::new(), Some((1_000_000, &time)));
    let cap = demux.egress_cap().unwrap();
    cap.charge_unpaced(cap.burst_bytes() as usize);
    let level = cap.level_bytes();
    let target: SocketAddr = "10.201.9.2:5000".parse().unwrap();
    let err = socket
        .try_send(&transmit(&[0xC0; 1200], target))
        .unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);
    assert!(demux.inner_for_test().sent.lock().unwrap().is_empty());
    assert_eq!(cap.level_bytes(), level, "nothing was charged");

    let mut poller = socket.clone().create_io_poller();
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(poller.as_mut().poll_writable(&mut cx).is_pending());
    time.advance(Duration::from_micros(1_200));
    assert!(poller.as_mut().poll_writable(&mut cx).is_ready());
    socket.try_send(&transmit(&[0xC0; 1200], target)).unwrap();
}

/// When the socket's own buffer is full the send is `WouldBlock` and the
/// cap gets back everything it was charged for the attempt.
#[test]
fn a_full_socket_buffer_is_would_block_and_returns_the_take() {
    use quinn::AsyncUdpSocket;
    let time = VirtualTime::new();
    let local: SocketAddr = "10.201.10.1:5000".parse().unwrap();
    let (socket, demux) = scripted(
        local,
        SendMode::WouldBlock,
        Vec::new(),
        Some((1_000_000, &time)),
    );
    let cap = demux.egress_cap().unwrap();
    let level = cap.level_bytes();
    let err = socket
        .try_send(&transmit(
            &[0xC0; 1200],
            "10.201.10.2:5000".parse().unwrap(),
        ))
        .unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);
    assert_eq!(cap.level_bytes(), level, "the take came back");
    assert_eq!(cap.stats().bytes, 0);
}

/// A send that fails with any other error is reported to quinn as sent
/// (quinn's connection driver would stop the connection on it), counted,
/// and not charged; with no cap the same holds.
#[test]
fn a_failed_send_is_counted_and_swallowed() {
    use quinn::AsyncUdpSocket;
    let time = VirtualTime::new();
    let target: SocketAddr = "10.201.11.2:5000".parse().unwrap();
    for cap in [Some((1_000_000, &time)), None] {
        let local: SocketAddr = "10.201.11.1:5000".parse().unwrap();
        let (socket, demux) = scripted(local, SendMode::Fail, Vec::new(), cap);
        socket.try_send(&transmit(&[0xC0; 900], target)).unwrap();
        socket.try_send(&transmit(&[0xC0; 900], target)).unwrap();
        assert_eq!(socket.send_errors(), 2);
        if let Some(cap) = demux.egress_cap() {
            assert_eq!(cap.stats().bytes, 0, "a failed send is not charged");
            assert_eq!(cap.level_bytes(), cap.burst_bytes() as i64);
        }
    }
}

/// Received datagrams reach quinn with their source; on an IPv6 socket an
/// IPv4 source is given in `::ffff:` form (the form quinn connects to there),
/// on an IPv4 socket it is unchanged.
#[tokio::test]
async fn received_ipv4_sources_are_mapped_on_an_ipv6_socket() {
    use quinn::AsyncUdpSocket;
    use std::io::IoSliceMut;
    use std::task::{Context, Poll};
    let from: SocketAddr = "10.201.12.2:5000".parse().unwrap();
    let datagram = vec![0xC1, 7, 7, 7];
    for local in ["[::]:5000", "10.201.12.1:5000"] {
        let local: SocketAddr = local.parse().unwrap();
        let (socket, demux) = scripted(local, SendMode::Send, vec![(datagram.clone(), from)], None);
        let _permit = demux.allow_quic_source(from).unwrap();
        let mut buf = vec![0u8; MAX_RECV_PACKET_SIZE];
        demux.recv_classified(&mut buf).await.unwrap();

        let mut storage = vec![0u8; 2048];
        let mut bufs = [IoSliceMut::new(&mut storage)];
        let mut meta = [quinn::udp::RecvMeta::default()];
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let Poll::Ready(Ok(1)) = socket.poll_recv(&mut cx, &mut bufs, &mut meta) else {
            panic!("one datagram is ready");
        };
        let expected = if local.is_ipv6() {
            SocketAddr::new(Ipv4Addr::new(10, 201, 12, 2).to_ipv6_mapped().into(), 5000)
        } else {
            from
        };
        assert_eq!(meta[0].addr, expected);
        assert_eq!(
            (meta[0].len, meta[0].stride),
            (datagram.len(), datagram.len())
        );
        assert_eq!(&storage[..datagram.len()], datagram.as_slice());
        let mut bufs = [IoSliceMut::new(&mut storage)];
        assert!(
            socket.poll_recv(&mut cx, &mut bufs, &mut meta).is_pending(),
            "nothing else queued, and no error"
        );
    }
}

/// What a raw socket on the simulated network receives within `wait` of
/// virtual time, driving `time` meanwhile.
async fn received_within(time: &VirtualTime, socket: &SimulationSocket, wait: Duration) -> usize {
    use futures::FutureExt;
    let deadline = time.now_nanos() + wait.as_nanos() as u64;
    let mut buf = [0u8; 2048];
    let mut count = 0;
    while time.now_nanos() < deadline {
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        while let Some(Ok(_)) = socket.recv_from(&mut buf).now_or_never() {
            count += 1;
        }
        time.advance(Duration::from_millis(5));
    }
    count
}

/// The endpoint really uses the keyed connection-ID generator. A
/// short-header packet, from an allowed source, for an unknown connection
/// whose ID carries a valid MAC under the endpoint's key gets a stateless
/// reset (quinn treats the ID as one of its own); the same packet with one
/// MAC bit flipped gets no reply. Any other generator (quinn's default
/// hashed one, or none installed) fails one of the two.
#[test]
fn a_short_header_packet_with_a_forged_id_gets_no_reply() {
    use quinn::ConnectionIdGenerator;
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let key = super::cid::MacConnectionIdGenerator::new_key().unwrap();
        let mut valid = super::cid::MacConnectionIdGenerator::new(key)
            .generate_cid()
            .to_vec();
        for (subnet, forged) in [(14u8, false), (15, true)] {
            let network = format!("quic-forged-cid-{subnet}");
            let time = VirtualTime::new();
            register_network_time_source(&network, time.clone());
            let endpoint_addr: SocketAddr = format!("10.201.{subnet}.1:5000").parse().unwrap();
            let prober_addr: SocketAddr = format!("10.201.{subnet}.2:5000").parse().unwrap();
            register_address_network(endpoint_addr, &network);
            register_address_network(prober_addr, &network);
            let socket = Arc::new(DemuxSocket::wrap(
                SimulationSocket::bind(endpoint_addr).await.unwrap(),
                None,
            ));
            let prober = SimulationSocket::bind(prober_addr).await.unwrap();
            let _pump = pump(socket.clone());
            let endpoint = QuicEndpoint::bind_with(
                socket,
                endpoint_addr,
                time.clone(),
                &settings(),
                &TransportKeypair::new(),
                config::endpoint_config_with_cid_key(key).unwrap(),
            )
            .unwrap();
            let _permit = endpoint
                .allow_peer(prober_addr, TransportKeypair::new().public())
                .unwrap();
            if forged {
                let last = valid.len() - 1;
                valid[last] ^= 1;
            }
            // Short header (fixed bit set), the destination ID, and enough
            // payload for a reset.
            let mut packet = vec![0x41];
            packet.extend_from_slice(&valid);
            packet.resize(100, 0x5A);
            Socket::send_to(&prober, &packet, endpoint_addr)
                .await
                .unwrap();
            let replies = received_within(&time, &prober, Duration::from_millis(200)).await;
            if forged {
                assert_eq!(replies, 0, "a forged ID gets no reply");
            } else {
                assert!(replies > 0, "a valid but unknown ID gets a stateless reset");
            }
            unregister_network_time_source(&network);
        }
    });
}

/// A peer leaves the allow-set when its connection ends by idle timeout:
/// once `b` hears nothing more (its socket is no longer read), both sides
/// time out and release their permits.
#[test]
fn an_idle_timeout_releases_the_allow_set() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let pair = pair(
            "quic-idle",
            "10.201.16.1:5000".parse().unwrap(),
            "10.201.16.2:5000".parse().unwrap(),
            None,
        )
        .await;
        let (_, client, server) = pair.transfer(payload(1000), Duration::from_secs(60)).await;
        pair.pumps[1].abort();
        let (c, s) = (client.inner().clone(), server.inner().clone());
        let idle = VirtualTime::new().connection_idle_timeout();
        drive(&pair.time, idle * 3, async move {
            let (c, s) = tokio::join!(c.closed(), s.closed());
            assert!(matches!(c, quinn::ConnectionError::TimedOut), "{c:?}");
            assert!(matches!(s, quinn::ConnectionError::TimedOut), "{s:?}");
        })
        .await;
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        assert!(
            !pair.a_socket.is_quic_source_allowed(pair.b_addr),
            "client side"
        );
        assert!(
            !pair.b_socket.is_quic_source_allowed(pair.a_addr),
            "server side"
        );
    });
}

/// Closing an endpoint while connected closes its connections and releases
/// their permits on both sides.
#[test]
fn closing_an_endpoint_releases_the_allow_set() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let pair = pair(
            "quic-endpoint-close",
            "10.201.17.1:5000".parse().unwrap(),
            "10.201.17.2:5000".parse().unwrap(),
            None,
        )
        .await;
        let (_, client, server) = pair.transfer(payload(1000), Duration::from_secs(60)).await;
        pair.a
            .endpoint
            .close(error::close_code::NODE_DROPPED_PEER.into(), b"test");
        let s = server.inner().clone();
        drive(&pair.time, Duration::from_secs(60), async move {
            s.closed().await;
        })
        .await;
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        assert!(
            !pair.a_socket.is_quic_source_allowed(pair.b_addr),
            "client side"
        );
        assert!(
            !pair.b_socket.is_quic_source_allowed(pair.a_addr),
            "server side"
        );
        drop((client, server));
    });
}

/// `connect` with the allow-set full fails at once and leaves the set as
/// it was.
#[test]
fn connect_with_a_full_allow_set_fails_and_leaks_nothing() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let pair = pair(
            "quic-allow-full",
            "10.201.18.1:5000".parse().unwrap(),
            "10.201.18.2:5000".parse().unwrap(),
            None,
        )
        .await;
        let max = crate::transport::demux::QUIC_ALLOWED_SOURCES_MAX;
        let fillers: Vec<_> = (0..max)
            .map(|i| {
                pair.a_socket
                    .allow_quic_source(SocketAddr::from(([10, 251, (i >> 8) as u8, i as u8], 1)))
                    .expect("room")
            })
            .collect();
        let result = pair.a.connect(pair.b_addr, &pair.b_key()).await;
        assert!(
            matches!(result, Err(QuicConnectError::AllowSetFull)),
            "{result:?}"
        );
        assert!(!pair.a_socket.is_quic_source_allowed(pair.b_addr));
        drop(fillers);
        assert!(
            pair.a_socket.allow_quic_source(pair.b_addr).is_some(),
            "the refused attempt held no place"
        );
    });
}

/// A connection attempt whose source left the allow-set after its first
/// datagram was queued is ignored by `accept`, not resurrected: `accept`
/// takes a permit only for an address that is still allowed. The test first
/// checks the Initial really was queued, so it cannot pass because the demux
/// dropped the datagram. (The concurrent race `allow_existing_quic_source`
/// closes, a permit dropped between a check and a grant, is covered by the
/// demux unit test `quic_allow_set_bound_holds_under_concurrent_callers`.)
#[test]
fn accept_ignores_an_attempt_whose_source_was_released() {
    bounded(|| async move {
        let _seed = GlobalRng::seed_guard(SEED);
        let pair = pair(
            "quic-accept-released",
            "10.201.19.1:5000".parse().unwrap(),
            "10.201.19.2:5000".parse().unwrap(),
            None,
        )
        .await;
        let permit = pair.b.allow_peer(pair.a_addr, &pair.a_key()).unwrap();
        let a = pair.a.clone();
        let (b_addr, b_key) = (pair.b_addr, pair.b_key());
        let connecting = tokio::spawn(async move { a.connect(b_addr, &b_key).await.map(|_| ()) });
        // The client's Initial reaches b's queue, then the expectation ends.
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        assert!(
            pair.b_socket
                .quic_outcome_count(crate::transport::metrics::QuicIngressOutcome::Forwarded)
                > 0,
            "the client's Initial reached b's ingress queue before the permit was dropped"
        );
        drop(permit);
        let b = pair.b.clone();
        let time = pair.time.clone();
        let accepted = drive(&pair.time, Duration::from_secs(60), async move {
            tokio::select! {
                incoming = b.accept() => incoming.map(|_| ()),
                _ = time.sleep(Duration::from_secs(5)) => None,
            }
        })
        .await;
        assert!(
            accepted.is_none(),
            "the released source's attempt was ignored"
        );
        assert!(
            !pair.b_socket.is_quic_source_allowed(pair.a_addr),
            "not brought back"
        );
        connecting.abort();
    });
}

/// Building a second endpoint on a socket whose QUIC ingress is already
/// enabled fails with `AlreadyExists`; `bind_or_warn`, which the transport
/// uses, turns that into no endpoint, so a node with `prefer` still starts.
#[test]
fn a_failed_bind_leaves_the_node_running_without_quic() {
    bounded(|| async move {
        let network = "quic-bind-twice";
        let addr: SocketAddr = "10.201.20.1:5000".parse().unwrap();
        let time = VirtualTime::new();
        register_network_time_source(network, time.clone());
        register_address_network(addr, network);
        let socket = Arc::new(DemuxSocket::wrap(
            SimulationSocket::bind(addr).await.unwrap(),
            None,
        ));
        let keys = TransportKeypair::new();
        let _first =
            QuicEndpoint::bind(socket.clone(), addr, time.clone(), &settings(), &keys).unwrap();
        let err =
            QuicEndpoint::bind(socket.clone(), addr, time.clone(), &settings(), &keys).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);
        assert!(QuicEndpoint::bind_or_warn(socket, addr, time, &settings(), &keys).is_none());
        unregister_network_time_source(network);
    });
}

/// The egress cap wakes `poll_admit` waiters per datagram of the QUIC
/// endpoint's size; the two constants must agree.
#[test]
fn the_cap_wakes_waiters_per_quic_datagram() {
    assert_eq!(
        crate::transport::egress_cap::WAKE_DATAGRAM_BYTES,
        usize::from(MAX_UDP_PAYLOAD)
    );
}

/// Each close code reaches the peer as that code, and the peer maps it to
/// the kind its handling depends on: `Downgrade` and `IdentityMismatch` are
/// refusals, `NodeDroppedPeer` (also what dropping a connection sends) is a
/// lost connection. Either side can close. Each close is counted once, by
/// code; a second close of an ended connection sends and counts nothing.
#[test]
fn close_codes_reach_the_peer() {
    bounded(|| async move {
        use crate::transport::metrics::{QuicCloseCode, TRANSPORT_METRICS};
        use crate::transport::{QuicErrorKind, QuicRefusal};
        let _seed = GlobalRng::seed_guard(SEED);
        let pair = pair(
            "quic-close-codes",
            "10.201.30.1:5000".parse().unwrap(),
            "10.201.30.2:5000".parse().unwrap(),
            None,
        )
        .await;
        let expected = |code: QuicCloseCode| match code {
            QuicCloseCode::NodeDroppedPeer => QuicErrorKind::ConnectionLost,
            QuicCloseCode::Downgrade => QuicErrorKind::Refused {
                reason: QuicRefusal::Downgrade,
            },
            QuicCloseCode::IdentityMismatch => QuicErrorKind::Refused {
                reason: QuicRefusal::IdentityMismatch,
            },
        };
        let mut sent_by_a = Vec::new();
        let mut sent_by_b = Vec::new();
        for code in QuicCloseCode::ALL {
            for a_closes in [true, false] {
                let (_, client, server) =
                    pair.transfer(payload(100), Duration::from_secs(60)).await;
                let (closer, peer) = if a_closes {
                    sent_by_a.push(code);
                    (client, server)
                } else {
                    sent_by_b.push(code);
                    (server, client)
                };
                closer.close(code);
                // Closing again sends and counts nothing.
                closer.close(QuicCloseCode::Downgrade);
                let seen = peer.inner().clone();
                let error = drive(&pair.time, Duration::from_secs(60), async move {
                    seen.closed().await
                })
                .await;
                let quinn::ConnectionError::ApplicationClosed(close) = &error else {
                    panic!("{code:?}: the peer saw {error:?}");
                };
                assert_eq!(
                    close.error_code.into_inner(),
                    u64::from(code.code()),
                    "{code:?}"
                );
                assert_eq!(QuicErrorKind::from(error), expected(code), "{code:?}");
                // Dropping both ends after the close counts nothing more.
                drop((closer, peer));
            }
        }
        assert_eq!(TRANSPORT_METRICS.quic_close_sent_to(pair.b_addr), sent_by_a);
        assert_eq!(TRANSPORT_METRICS.quic_close_sent_to(pair.a_addr), sent_by_b);

        // Dropping an open connection closes it with NodeDroppedPeer.
        let (_, client, server) = pair.transfer(payload(100), Duration::from_secs(60)).await;
        drop(client);
        let seen = server.inner().clone();
        let error = drive(&pair.time, Duration::from_secs(60), async move {
            seen.closed().await
        })
        .await;
        let quinn::ConnectionError::ApplicationClosed(close) = &error else {
            panic!("the peer saw {error:?}");
        };
        assert_eq!(
            close.error_code.into_inner(),
            u64::from(error::close_code::NODE_DROPPED_PEER)
        );
        sent_by_a.push(QuicCloseCode::NodeDroppedPeer);
        assert_eq!(TRANSPORT_METRICS.quic_close_sent_to(pair.b_addr), sent_by_a);
        drop(server);
        assert_eq!(
            TRANSPORT_METRICS.quic_close_sent_to(pair.a_addr),
            sent_by_b,
            "the peer's handle, dropped after the close, sent nothing"
        );
    });
}

/// When the endpoint driver dies while the node holds the endpoint (here a
/// receive error, as from a socket that fails for good), the endpoint's
/// availability turns `Down` and the death is counted once. Nothing stops:
/// the accept loop parks instead of ending (its monitor does not fire), and
/// the socket keeps passing UDP-transport datagrams. Its connections end
/// and new ones fail as `EndpointDown`.
#[test]
fn endpoint_death_turns_quic_off_without_stopping_the_node() {
    bounded(|| async move {
        use crate::node::background_task_monitor::BackgroundTaskMonitor;
        use crate::transport::QuicErrorKind;
        use crate::transport::metrics::TRANSPORT_METRICS;
        let _seed = GlobalRng::seed_guard(SEED);
        let pair = pair(
            "quic-endpoint-death",
            "10.201.31.1:5000".parse().unwrap(),
            "10.201.31.2:5000".parse().unwrap(),
            None,
        )
        .await;
        let (_, client, _server) = pair.transfer(payload(100), Duration::from_secs(60)).await;
        let mut availability = pair.a.availability();
        assert_eq!(availability.get(), QuicAvailability::Up);

        let monitor = BackgroundTaskMonitor::new();
        let (attempts, _attempts_rx) = tokio::sync::mpsc::channel(4);
        assert!(pair.a.spawn_accept_loop(&monitor, attempts));
        let monitor_fired = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let wait = {
            let fired = monitor_fired.clone();
            let wait = monitor.wait_for_any_exit();
            tokio::spawn(async move {
                let _ = wait.await;
                fired.store(true, std::sync::atomic::Ordering::SeqCst);
            })
        };

        pair.a.fail_driver();
        let seen = drive(&pair.time, Duration::from_secs(60), async move {
            availability.not_up().await
        })
        .await;
        assert_eq!(seen, QuicAvailability::Down);
        assert_eq!(pair.a.availability().get(), QuicAvailability::Down);
        assert_eq!(TRANSPORT_METRICS.quic_endpoint_down_at(pair.a_addr), 1);

        // The endpoint's connection ends, and with the availability Down it
        // is blamed on this endpoint, not on the peer, whichever error quinn
        // reports (a local INTERNAL_ERROR, or LocallyClosed once the
        // connection's driver closed it; which one depends on scheduling,
        // so only the attributed kind is asserted). A new one cannot start.
        let c = client.inner().clone();
        let ended = drive(&pair.time, Duration::from_secs(60), async move {
            c.closed().await
        })
        .await;
        assert_eq!(pair.a.availability().get(), QuicAvailability::Down);
        assert_eq!(
            pair.a.error_kind(ended.clone()),
            QuicErrorKind::EndpointDown,
            "{ended:?}"
        );
        let refused = pair
            .a
            .connect(pair.b_addr, &pair.b_key())
            .await
            .expect_err("the endpoint is down");
        assert_eq!(QuicErrorKind::from(refused), QuicErrorKind::EndpointDown);

        // Let everything run a while: the accept loop has seen `None` and
        // parked, so the monitor never fires.
        let time = pair.time.clone();
        drive(&pair.time, Duration::from_secs(60), async move {
            time.sleep(Duration::from_secs(30)).await;
        })
        .await;
        assert!(
            !monitor_fired.load(std::sync::atomic::Ordering::SeqCst),
            "the accept loop's end would stop the node"
        );
        assert!(!wait.is_finished());

        // The UDP side of the socket still works: a legacy-class datagram
        // sent to `a` is read as one (by this test, in the listener's
        // place).
        pair.pumps[0].abort();
        pair.b_socket
            .send_to(&[0x01, 0, 0, 0], pair.a_addr)
            .await
            .unwrap();
        let socket = pair.a_socket.clone();
        let (len, from) = drive(&pair.time, Duration::from_secs(10), async move {
            let mut buf = vec![0u8; MAX_RECV_PACKET_SIZE];
            loop {
                match socket.recv_classified(&mut buf).await.unwrap() {
                    crate::transport::SocketRead::Datagram(len, from) => break (len, from),
                    crate::transport::SocketRead::Diverted => {}
                }
            }
        })
        .await;
        assert_eq!((len, from), (4, pair.b_addr));

        // Dropping every handle afterwards counts nothing more.
        drop(client);
        drop(pair);
        assert_eq!(
            TRANSPORT_METRICS.quic_endpoint_down_at("10.201.31.1:5000".parse().unwrap()),
            1
        );
        wait.abort();
    });
}

/// Dropping the last handle is not a death, even with the accept loop
/// running: the loop does not keep the endpoint owned, so it stops and
/// drops its handle, quinn's driver ends, the socket is let go, the
/// availability becomes `Released` and nothing is counted. Waiting on a
/// final availability returns that value at once, every time, so a loop
/// around `not_up` cannot spin on a stale `Up`.
#[test]
fn dropping_the_endpoint_is_not_a_death() {
    bounded(|| async move {
        use crate::node::background_task_monitor::BackgroundTaskMonitor;
        use crate::transport::metrics::TRANSPORT_METRICS;
        let network = "quic-endpoint-drop";
        let addr: SocketAddr = "10.201.32.1:5000".parse().unwrap();
        let time = VirtualTime::new();
        register_network_time_source(network, time.clone());
        register_address_network(addr, network);
        let socket = Arc::new(DemuxSocket::wrap(
            SimulationSocket::bind(addr).await.unwrap(),
            None,
        ));
        let endpoint = QuicEndpoint::bind(
            socket.clone(),
            addr,
            time.clone(),
            &settings(),
            &TransportKeypair::new(),
        )
        .unwrap();
        let monitor = BackgroundTaskMonitor::new();
        let (attempts, mut attempts_rx) = tokio::sync::mpsc::channel(4);
        assert!(endpoint.spawn_accept_loop(&monitor, attempts));
        let (second, _second_rx) = tokio::sync::mpsc::channel(4);
        assert!(
            !endpoint.spawn_accept_loop(&monitor, second),
            "one accept loop per endpoint"
        );
        let mut availability = endpoint.availability();
        let clone = endpoint.clone();
        drop(endpoint);
        assert_eq!(availability.get(), QuicAvailability::Up, "a handle is left");
        assert!(
            Arc::strong_count(&socket) > 1,
            "the endpoint holds the socket"
        );
        drop(clone);
        let socket_in = socket.clone();
        let seen = drive(&time, Duration::from_secs(10), async move {
            let seen = availability.not_up().await;
            // The loop let go of its handle, so quinn's driver ends and
            // drops the endpoint's socket (this task holds one more).
            while Arc::strong_count(&socket_in) > 2 {
                tokio::task::yield_now().await;
            }
            for _ in 0..3 {
                assert_eq!(availability.not_up().await, seen, "final and immediate");
            }
            seen
        })
        .await;
        assert_eq!(seen, QuicAvailability::Released);
        assert!(
            attempts_rx.recv().await.is_none(),
            "the stopped loop dropped its sender"
        );
        assert_eq!(Arc::strong_count(&socket), 1, "the socket was let go");
        assert_eq!(TRANSPORT_METRICS.quic_endpoint_down_at(addr), 0);
        let mut disabled = QuicAvailabilityWatch::disabled();
        for _ in 0..3 {
            assert_eq!(disabled.not_up().await, QuicAvailability::Disabled);
        }
        drop(monitor);
        unregister_network_time_source(network);
    });
}

/// An attempt that finds the accept queue full is ignored and counted.
#[test]
fn a_full_accept_queue_ignores_and_counts_the_attempt() {
    bounded(|| async move {
        use crate::node::background_task_monitor::BackgroundTaskMonitor;
        let _seed = GlobalRng::seed_guard(SEED);
        let pair = pair(
            "quic-accept-full",
            "10.201.33.1:5000".parse().unwrap(),
            "10.201.33.2:5000".parse().unwrap(),
            None,
        )
        .await;
        let _permit = pair.b.allow_peer(pair.a_addr, &pair.a_key()).unwrap();
        let monitor = BackgroundTaskMonitor::new();
        // Room for one attempt, never taken.
        let (attempts, _attempts_rx) = tokio::sync::mpsc::channel(1);
        assert!(pair.b.spawn_accept_loop(&monitor, attempts));
        let clients: Vec<_> = (0..2)
            .map(|_| {
                let (a, b_addr, b_key) = (pair.a.clone(), pair.b_addr, pair.b_key());
                tokio::spawn(async move { a.connect(b_addr, &b_key).await.map(|_| ()) })
            })
            .collect();
        let b = pair.b.clone();
        drive(&pair.time, Duration::from_secs(10), async move {
            while b.accept_queue_full() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert_eq!(pair.b.accept_queue_full(), 1);
        for client in clients {
            client.abort();
        }
    });
}

/// With `transport-quic` off the transport builds no endpoint and reports
/// `Disabled`; with it on, `Up`. When that endpoint's driver dies, the
/// transport reports `Down` and its listener keeps running: endpoint death
/// never reaches `ListenerExit`, which would exit the node. On simulation
/// sockets (no real port, no port reuse). The endpoint inside the
/// transport runs on `RealTime`, but nothing here waits on time: the
/// injected failure wakes the driver at once.
#[test]
fn the_transport_reports_quic_availability() {
    bounded(|| async move {
        use crate::transport::connection_handler::{UdpTransportConfig, bind_udp_listener};
        use crate::transport::metrics::TRANSPORT_METRICS;
        let network = "quic-transport-availability";
        let time = VirtualTime::new();
        register_network_time_source(network, time.clone());
        for (port, quic) in [(5000, None), (5001, Some(settings()))] {
            let local = SocketAddr::from(([10, 201, 34, 1], port));
            register_address_network(local, network);
            let (outbound, _inbound, listener) = bind_udp_listener::<SimulationSocket>(
                crate::transport::TransportKeypair::new(),
                local.ip(),
                port,
                false,
                UdpTransportConfig {
                    quic,
                    ..UdpTransportConfig::default()
                },
            )
            .await
            .unwrap();
            let mut availability = outbound.quic_availability();
            let Some(endpoint) = outbound.quic_endpoint() else {
                assert!(quic.is_none(), "an endpoint is built with the key on");
                assert_eq!(availability.get(), QuicAvailability::Disabled);
                assert_eq!(availability.not_up().await, QuicAvailability::Disabled);
                listener.abort();
                continue;
            };
            assert!(quic.is_some(), "no endpoint is built with the key off");
            assert_eq!(availability.get(), QuicAvailability::Up);
            endpoint.fail_driver();
            // A bound only: the wake is immediate.
            let seen = tokio::time::timeout(Duration::from_secs(30), availability.not_up())
                .await
                .expect("the driver's death is reported");
            assert_eq!(seen, QuicAvailability::Down);
            assert_eq!(TRANSPORT_METRICS.quic_endpoint_down_at(local), 1);
            for _ in 0..64 {
                tokio::task::yield_now().await;
            }
            assert!(!listener.is_finished(), "the listener keeps running");
            listener.abort();
        }
        for port in [5000, 5001] {
            crate::transport::in_memory_socket::unregister_address_network(&SocketAddr::from((
                [10, 201, 34, 1],
                port,
            )));
        }
        unregister_network_time_source(network);
    })
}

/// Closers racing on one connection (the receive supervisor and the
/// path's owner on two threads) record exactly one close. The guard is
/// also checked deterministically: a closer that finds a close already in
/// progress sends and counts nothing, so the connection stays open.
#[test]
fn concurrent_closers_close_and_count_once() {
    bounded(|| async move {
        use crate::transport::metrics::{QuicCloseCode, TRANSPORT_METRICS};
        let _seed = GlobalRng::seed_guard(SEED);
        let pair = pair(
            "quic-close-once",
            "10.201.36.1:5000".parse().unwrap(),
            "10.201.36.2:5000".parse().unwrap(),
            None,
        )
        .await;

        // Deterministic: a close already in progress wins.
        let (_, client, _server) = pair.transfer(payload(100), Duration::from_secs(60)).await;
        let closer = client.closer();
        closer.mark_closing();
        assert!(!client.closer().close(QuicCloseCode::Downgrade));
        assert!(
            client.inner().close_reason().is_none(),
            "the second closer did not close"
        );
        assert!(TRANSPORT_METRICS.quic_close_sent_to(pair.b_addr).is_empty());
        drop((client, _server));
        assert!(
            TRANSPORT_METRICS.quic_close_sent_to(pair.b_addr).is_empty(),
            "dropping the handle is a closer too"
        );

        // Racing: many threads, released at once.
        let (_, client, _server) = pair.transfer(payload(100), Duration::from_secs(60)).await;
        let threads = 8;
        let barrier = Arc::new(std::sync::Barrier::new(threads));
        let closed: usize = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..threads)
                .map(|i| {
                    let closer = client.closer();
                    let barrier = barrier.clone();
                    scope.spawn(move || {
                        barrier.wait();
                        let code = if i % 2 == 0 {
                            QuicCloseCode::Downgrade
                        } else {
                            QuicCloseCode::NodeDroppedPeer
                        };
                        usize::from(closer.close(code))
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).sum()
        });
        assert_eq!(closed, 1, "exactly one closer closed");
        assert_eq!(TRANSPORT_METRICS.quic_close_sent_to(pair.b_addr).len(), 1);
        drop((client, _server));
        assert_eq!(TRANSPORT_METRICS.quic_close_sent_to(pair.b_addr).len(), 1);
    });
}

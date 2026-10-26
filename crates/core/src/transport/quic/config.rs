//! Every quinn and rustls setting of the QUIC endpoint, pinned explicitly.
//!
//! quinn's defaults are tuned for a general internet server, and several of
//! them are wrong here: QUIC bit greasing would break the first-byte demux,
//! path MTU discovery and GSO would send datagrams larger than the UDP
//! transport's, migration would change a connection's address (which the node
//! uses as the peer's identity), and the flow-control windows allow far more
//! memory per connection than a node can give. So nothing is left to a
//! default: each value is set here.
//!
//! What `quic::tests` pins, and how: values quinn's `Debug` shows are pinned
//! field by field (endpoint, transport, server and client configurations);
//! the rustls settings are read off the rustls configs; Cubic is checked on
//! a live connection (`congestion_state`); the connection-ID generator is
//! checked by sending an endpoint a short-header packet with a random ID and
//! seeing no reply. NOT pinned, because quinn exposes them neither in `Debug`
//! nor in behaviour a test can observe cheaply: the token store
//! (`NoneTokenStore`), the initial destination CID provider, the
//! stateless-reset key, `min_reset_interval` and the server's system clock.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use quinn::{
    ClientConfig, EndpointConfig, IdleTimeout, MtuDiscoveryConfig, NoneTokenLog, NoneTokenStore,
    ServerConfig, TransportConfig, ValidationTokenConfig, VarInt, congestion,
};
use ring_crypto::{hkdf, hmac};

use super::cid::MacConnectionIdGenerator;
use crate::config::GlobalRng;
use crate::simulation::TimeSource;

/// The largest UDP payload the endpoint sends or advertises it accepts: the
/// UDP transport's packet size and QUIC's minimum (RFC 9000 section 14), so
/// a QUIC datagram is never larger than a legacy one.
pub(crate) const MAX_UDP_PAYLOAD: u16 = 1200;

/// The only QUIC version spoken: version 1 (RFC 9000).
pub(crate) const QUIC_VERSION: u32 = 1;

/// The ALPN protocol both sides must offer. A peer offering anything else
/// fails the handshake.
pub(crate) const ALPN: &[u8] = b"freenet-quic/1";

/// The server name quinn requires for a connection attempt. With SNI off it
/// is never sent; peers are identified by their raw public keys.
pub(crate) const SERVER_NAME: &str = "freenet";

/// Keepalive interval: the UDP transport's NAT keepalive guarantee
/// (`peer_connection::MAX_KEEPALIVE_INTERVAL`, 15 s), so a QUIC connection
/// holds a NAT mapping open as long as a legacy one does.
pub(crate) const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);

/// Concurrent unidirectional streams a peer may open. The transport sends
/// one message per stream, so this bounds the messages in flight from one
/// peer.
pub(crate) const MAX_UNI_STREAMS: u32 = 256;

/// Concurrent bidirectional streams a peer may open: one, kept for a control
/// stream; messages use unidirectional streams.
pub(crate) const MAX_BIDI_STREAMS: u32 = 1;

/// Connection attempts quinn holds before they are accepted or refused.
pub(crate) const MAX_INCOMING: usize = 64;

/// Bytes quinn buffers for one connection attempt before it is accepted.
pub(crate) const INCOMING_BUFFER_BYTES: u64 = 64 * 1024;

/// Bytes quinn buffers for all connection attempts together.
pub(crate) const INCOMING_BUFFER_TOTAL_BYTES: u64 = 1024 * 1024;

// What one QUIC connection may hold, per side, measured by
// `quic::memory_tests` with quinn 0.11.12 / quinn-proto 0.11.19 at the
// settings of a 2 GiB node (window W = 268,435 B), in bytes rounded up to
// jemalloc's size classes (`util::alloc_count`), the allocator the node
// runs on. The terms, which the per-path ceiling (memory gate M2) sums:
//
//   QUIC_CONNECTION_FIXED_BYTES                         idle state
//   + streams x QUIC_ASSEMBLER_HOLD_PER_STREAM_BYTES    } received stream
//   + QUIC_ASSEMBLER_HOLD_WINDOW_MULTIPLE x W           } data, held by quinn
//   + W                                                 sent, unacknowledged
//   + QUIC_ACK_TAIL_BYTES                               ACK records
//
// plus the path's own inbound reassembly allowance. Each constant is the
// largest measurement plus a stated margin; the tests fail if a
// re-measurement exceeds it, and on a quinn, quinn-proto or rustls bump
// ([`QUIC_MEMORY_MEASURED_WITH`]) until someone re-measures. None is in
// `declared_cache_ceiling` yet (memory gate M4).

/// The heap one idle QUIC connection holds on one side, with the
/// `QuicPath` a node keeps on it: quinn's connection and driver task,
/// rustls's state and keys, the packet spaces, congestion and connection-ID
/// state, the connection watcher and the path's receive task and channels.
/// Not included: what grows with traffic (received stream data and the
/// assembler, sent data, sent-packet and ACK bookkeeping), which the other
/// terms above cover; per-peer allow-set and demux entries; keep-alive
/// PINGs and their sent-packet records (off under `VirtualTime`); and
/// socket buffers, which are per endpoint.
///
/// Measured: two endpoints on `SimulationSocket` and one `VirtualTime`, on
/// one current-thread runtime, counting live bytes on that thread. After a
/// warm-up connection and a settle that must leave the count unchanged, N
/// connections are opened and settled, the network is cut, and each side
/// is closed and drained in turn; what each side frees is its cost, and
/// what both leave behind (endpoint tables grown for N) is charged to the
/// larger side. Per connection, rounded (requested in brackets):
///
/// | N  | client            | server            | retained        | per connection |
/// |----|-------------------|-------------------|-----------------|----------------|
/// | 1  | 52,568 (48,449) B | 52,440 (48,320) B | 96 (88) B       | 52,664 B       |
/// | 10 | 52,453 (48,335) B | 52,101 (48,005) B | 2,176 (1,968) B | 54,629 B       |
/// | 16 | 52,404 (48,301) B | 52,370 (48,256) B | 1,980 (1,769) B | 54,384 B       |
/// | 50 | 52,359 (48,248) B | 52,260 (48,144) B | 1,957 (1,747) B | 54,316 B       |
///
/// The test runs N = 16. Set to the largest, 54,629 B, plus 20%, rounded
/// up to 64 KiB.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const QUIC_CONNECTION_FIXED_BYTES: usize = 64 * 1024;

/// The receive-side hold per open uni stream, in the per-connection
/// assembler bound `streams x QUIC_ASSEMBLER_HOLD_PER_STREAM_BYTES +
/// QUIC_ASSEMBLER_HOLD_WINDOW_MULTIPLE x W`.
///
/// quinn keeps every STREAM frame as a slice of the datagram it arrived in
/// (`quinn/src/endpoint.rs`, one buffer per datagram), so a frame of any
/// length keeps a 1200-byte datagram (1280 B in jemalloc) alive until the
/// stream is read or quinn-proto's assembler copies it out. The assembler
/// copies only when its estimate of the waste passes
/// `max(32768, 1.5 x buffered)` per stream (quinn-proto
/// `connection/assembler.rs`, `Assembler::insert`), and nothing past a gap
/// at the start of a stream can be read. So each stream can hold about
/// 32 KiB of datagrams for a few bytes of flow-control credit: the
/// one-byte-frame shape, which peaks at 39,408 B per stream (28 frames,
/// then compaction; 256 such streams held 10,020,960 B, 39,144 B each).
/// Set to that peak plus 14%, 44 KiB.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const QUIC_ASSEMBLER_HOLD_PER_STREAM_BYTES: usize = 44 * 1024;

/// The window-proportional part of the per-connection assembler bound (see
/// [`QUIC_ASSEMBLER_HOLD_PER_STREAM_BYTES`]).
///
/// Past about 22 KB buffered on a stream the proportional threshold
/// governs, and frames of more than about 460 B never trigger compaction:
/// the stream then holds a whole datagram for every frame, up to the
/// window. Frames a little shorter do trigger it, and then the copy is made
/// while the chunks it copies are still alive. Measured on one stream
/// filling the window, sweeping every frame length from 160 to 1100 B and
/// every third below: the highest hold between packets is 3.05 W (470 B
/// frames), the highest at any instant 4.115 W (444 B frames, 1,104,656 B,
/// during compaction). Many streams sharing the window hold less (64
/// streams of 460 B frames: 3.12 W). Set to 5, 22% over 4.115.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const QUIC_ASSEMBLER_HOLD_WINDOW_MULTIPLE: usize = 5;

/// The acknowledgement-only packets one connection tracks after its last
/// ack-eliciting send, up to quinn-proto's cap of 1000.
///
/// quinn-proto keeps a record of every packet it sends until the peer
/// acknowledges it or it is declared lost, and it sends an ACK for about
/// every second packet it receives. It keeps at most 1000
/// acknowledgement-only records after its last ack-eliciting packet
/// (`connection/spaces.rs`, `MAX_UNACKED_NON_ACK_ELICTING_TAIL`). Measured
/// with none of them acknowledged: 212,480 B after 2003 such packets,
/// 209,920 B after 4003. Set to 212,480 B plus 15%, 240 KiB. The per-path
/// ceiling (memory gate M2) must re-check this term against quinn-proto's
/// behaviour.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const QUIC_ACK_TAIL_BYTES: usize = 240 * 1024;

/// The quinn, quinn-proto and rustls versions the figures above were
/// measured with, checked against `Cargo.lock` by `quic::memory_tests`.
#[cfg(test)]
pub(crate) const QUIC_MEMORY_MEASURED_WITH: [(&str, &str); 3] = [
    ("quinn", "0.11.12"),
    ("quinn-proto", "0.11.19"),
    ("rustls", "0.23.45"),
];

/// The smallest flow-control window per connection and direction.
pub(crate) const MIN_WINDOW_BYTES: u64 = 128 * 1024;

/// The largest flow-control window per connection and direction.
pub(crate) const MAX_WINDOW_BYTES: u64 = 2 * 1024 * 1024;

/// The share of the node's memory all QUIC windows together may hold, as a
/// divisor: one twentieth, 5%.
const WINDOW_MEMORY_DIVISOR: usize = 20;

/// What the endpoint's settings are derived from.
#[derive(Debug, Clone, Copy)]
pub(crate) struct QuicSettings {
    /// `max-number-of-connections`: the windows are split between this many.
    pub max_connections: usize,
    /// The memory the node may use (host RAM, or a cgroup limit).
    pub memory_limit: usize,
}

impl QuicSettings {
    /// The flow-control window per connection and direction.
    pub(crate) fn window_bytes(&self) -> u64 {
        connection_window_bytes(self.memory_limit, self.max_connections)
    }
}

/// The flow-control window for one connection in one direction: 5% of the
/// memory split over every connection's receive and send windows, clamped
/// to [`MIN_WINDOW_BYTES`], [`MAX_WINDOW_BYTES`], and never more than
/// [`MAX_INBOUND_STREAM_BYTES_PER_CONNECTION`] (that cap wins over the
/// floor). The same value is the connection receive window, the per-stream
/// receive window (so a stream's window is never above its connection's)
/// and the send window.
///
/// The cap does not bind today (the 2 MiB clamp is far below it); it only
/// stops a future constant change from raising the windows past it. It does
/// NOT mean a QUIC connection holds no more memory than a UDP-transport one:
/// QUIC windows come on top of whatever the application reassembles, the
/// send window is outbound memory the reassembly exception never covered,
/// and quinn holds received data well beyond the window (its assembler keeps
/// whole datagrams for small frames) and per-connection state the windows do
/// not bound. The measured terms are listed above
/// [`QUIC_CONNECTION_FIXED_BYTES`]: about 5 x W received plus 44 KiB per
/// open stream, W sent, 64 KiB fixed, and the acknowledgement tail.
///
/// The windows are the same class of per-connection exception as inbound
/// stream reassembly and are in the `NOT_SUMMED` table of
/// `contract::executor::declared_cache_ceiling` (see the "Recorded
/// exception: inbound stream reassembly" note in
/// `.claude/rules/bug-prevention-patterns.md`). That memory is NOT
/// accounted in the node aggregate, and must be before QUIC is on by
/// default (the rollout plan records that gate).
///
/// [`MAX_INBOUND_STREAM_BYTES_PER_CONNECTION`]: crate::transport::peer_connection::streaming::MAX_INBOUND_STREAM_BYTES_PER_CONNECTION
pub(crate) fn connection_window_bytes(memory_limit: usize, max_connections: usize) -> u64 {
    let share = memory_limit / WINDOW_MEMORY_DIVISOR / (2 * max_connections.max(1));
    (share as u64)
        .clamp(MIN_WINDOW_BYTES, MAX_WINDOW_BYTES)
        .min(PER_CONNECTION_INBOUND_CAP)
}

/// [`connection_window_bytes`]'s upper bound: the UDP transport's
/// per-connection reassembly allowance.
const PER_CONNECTION_INBOUND_CAP: u64 =
    crate::transport::peer_connection::streaming::MAX_INBOUND_STREAM_BYTES_PER_CONNECTION;

/// Fill `out` from the OS entropy pool, for key material: the
/// stateless-reset key, the handshake-token key and the connection-ID MAC
/// key. These must not come from `GlobalRng`, whose seed is fixed in tests
/// and would make the keys predictable to anyone who reads the seed
/// (`.claude/rules/code-style.md`, OsRng exception). They do not affect the
/// order or timing of what the endpoint sends, so a seeded run still
/// repeats its event trace. ring's SystemRandom reads getrandom(2) (or the
/// platform equivalent); a failure is returned, so the endpoint is not built
/// and the node runs with QUIC off instead of panicking.
pub(crate) fn os_random(out: &mut [u8]) -> std::io::Result<()> {
    use ring_crypto::rand::SecureRandom;
    ring_crypto::rand::SystemRandom::new()
        .fill(out)
        .map_err(|_| std::io::Error::other("the OS entropy source failed"))
}

/// The endpoint configuration: version, payload size, greasing, the
/// stateless-reset key, the connection-ID generator and quinn's RNG seed.
/// Keys come from the OS ([`os_random`]); quinn's RNG seed comes from
/// [`GlobalRng`], so a seeded run is repeatable.
pub(crate) fn endpoint_config() -> std::io::Result<EndpointConfig> {
    endpoint_config_with_cid_key(MacConnectionIdGenerator::new_key()?)
}

/// [`endpoint_config`] with a given connection-ID MAC key, so a test can
/// make IDs the endpoint accepts.
pub(crate) fn endpoint_config_with_cid_key(cid_key: [u8; 32]) -> std::io::Result<EndpointConfig> {
    let mut reset_key = [0u8; 64];
    os_random(&mut reset_key)?;
    let mut config = EndpointConfig::new(Arc::new(hmac::Key::new(hmac::HMAC_SHA256, &reset_key)));
    config.cid_generator(move || Box::new(MacConnectionIdGenerator::new(cid_key)));
    let mut seed = [0u8; 32];
    GlobalRng::fill_bytes(&mut seed);
    config
        // MAX_UDP_PAYLOAD (1200) is within quinn's accepted 1200..=65527.
        .max_udp_payload_size(MAX_UDP_PAYLOAD)
        .unwrap_or_else(|_| unreachable!("1200 is quinn's minimum UDP payload size"))
        .supported_versions(vec![QUIC_VERSION])
        // Must stay false: a cleared fixed bit could make a short-header
        // packet start with 0x01 or 0x02, which the demux reads as a UDP
        // transport packet (RFC 9287; transport::demux).
        .grease_quic_bit(false)
        .min_reset_interval(Duration::from_millis(20))
        .rng_seed(Some(seed));
    Ok(config)
}

/// The transport configuration shared by both directions of a connection.
/// `time` decides the keepalive and the idle timeout, as for the UDP
/// transport: no keepalive and a long idle timeout where the time source
/// says so (simulation).
pub(crate) fn transport_config<T: TimeSource>(
    settings: &QuicSettings,
    time: &T,
) -> TransportConfig {
    let window = settings.window_bytes();
    let mut config = TransportConfig::default();
    // No `max_ack_delay` is set, so quinn's default (25 ms) applies; the
    // UDP-side inbound concurrency gauge assumes it
    // (`peer_connection::inbound_concurrency::ACK_DELAY_ALLOWANCE`). If it
    // is ever set here, change that constant to match.
    config
        .max_concurrent_uni_streams(VarInt::from_u32(MAX_UNI_STREAMS))
        .max_concurrent_bidi_streams(VarInt::from_u32(MAX_BIDI_STREAMS))
        .max_idle_timeout(Some(idle_timeout(time.connection_idle_timeout())))
        .keep_alive_interval(time.supports_keepalive().then_some(KEEPALIVE_INTERVAL))
        // A window is at most MAX_WINDOW_BYTES (2 MiB), far inside a VarInt.
        .stream_receive_window(VarInt::from_u32(window as u32))
        .receive_window(VarInt::from_u32(window as u32))
        .send_window(window)
        .send_fairness(true)
        .initial_mtu(MAX_UDP_PAYLOAD)
        .min_mtu(MAX_UDP_PAYLOAD)
        .mtu_discovery_config(None::<MtuDiscoveryConfig>)
        .pad_to_mtu(false)
        .enable_segmentation_offload(false)
        .congestion_controller_factory(Arc::new(congestion::CubicConfig::default()))
        .initial_rtt(Duration::from_millis(333))
        .packet_threshold(3)
        .time_threshold(9.0 / 8.0)
        .persistent_congestion_threshold(3)
        .ack_frequency_config(None)
        .crypto_buffer_size(16 * 1024)
        // The spin bit lets an on-path observer measure a connection's RTT.
        .allow_spin(false)
        // No unreliable datagrams: messages go on streams.
        .datagram_receive_buffer_size(None)
        .datagram_send_buffer_size(0);
    config
}

/// `timeout` as quinn's idle timeout, the largest quinn accepts if it is
/// longer.
fn idle_timeout(timeout: Duration) -> IdleTimeout {
    IdleTimeout::try_from(timeout).unwrap_or_else(|_| IdleTimeout::from(VarInt::MAX))
}

/// A server configuration over `crypto`: no migration, no address
/// validation tokens, few and small pending connection attempts, keys from
/// [`GlobalRng`] and the clock from `time`.
pub(crate) fn server_config(
    crypto: Arc<dyn quinn::crypto::ServerConfig>,
    transport: Arc<TransportConfig>,
    clock: Arc<dyn quinn::TimeSource>,
) -> std::io::Result<ServerConfig> {
    let mut master_key = [0u8; 64];
    os_random(&mut master_key)?;
    let token_key = hkdf::Salt::new(hkdf::HKDF_SHA256, &[]).extract(&master_key);
    let mut tokens = ValidationTokenConfig::default();
    tokens.sent(0).log(Arc::new(NoneTokenLog));
    let mut config = ServerConfig::new(crypto, Arc::new(token_key));
    config
        .transport_config(transport)
        .validation_token_config(tokens)
        .retry_token_lifetime(Duration::from_secs(15))
        // A migrated connection would change the remote address, which the
        // node uses as the peer's identity (#5940 decision 7).
        .migration(false)
        .preferred_address_v4(None)
        .preferred_address_v6(None)
        .max_incoming(MAX_INCOMING)
        .incoming_buffer_size(INCOMING_BUFFER_BYTES)
        .incoming_buffer_size_total(INCOMING_BUFFER_TOTAL_BYTES)
        .time_source(clock);
    Ok(config)
}

/// A client configuration over `crypto`: version 1, no stored tokens, and
/// initial destination connection IDs from [`GlobalRng`].
pub(crate) fn client_config(
    crypto: Arc<dyn quinn::crypto::ClientConfig>,
    transport: Arc<TransportConfig>,
) -> ClientConfig {
    let mut config = ClientConfig::new(crypto);
    config
        .transport_config(transport)
        .token_store(Arc::new(NoneTokenStore))
        .version(QUIC_VERSION)
        .initial_dst_cid_provider(Arc::new(|| {
            let mut bytes = [0u8; 16];
            GlobalRng::fill_bytes(&mut bytes);
            quinn::ConnectionId::new(&bytes)
        }));
    config
}

/// The rustls settings both sides share: the ring provider, TLS 1.3 only.
fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// The TLS server configuration: TLS 1.3 only with the ring provider, the
/// raw public key in `key`, peers checked by `verifier`, the fixed ALPN, and
/// no session resumption, tickets or early data. `identity` builds the key
/// and the verifiers.
pub(crate) fn tls_server_config(
    key: Arc<rustls::sign::CertifiedKey>,
    verifier: Arc<dyn rustls::server::danger::ClientCertVerifier>,
) -> Result<quinn::crypto::rustls::QuicServerConfig, TlsConfigError> {
    let config = rustls_server_config(key, verifier)?;
    Ok(quinn::crypto::rustls::QuicServerConfig::try_from(
        Arc::new(config),
    )?)
}

/// The rustls half of [`tls_server_config`].
pub(crate) fn rustls_server_config(
    key: Arc<rustls::sign::CertifiedKey>,
    verifier: Arc<dyn rustls::server::danger::ClientCertVerifier>,
) -> Result<rustls::ServerConfig, rustls::Error> {
    let mut config = rustls::ServerConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .with_client_cert_verifier(verifier)
        .with_cert_resolver(Arc::new(
            rustls::server::AlwaysResolvesServerRawPublicKeys::new(key),
        ));
    config.alpn_protocols = vec![ALPN.to_vec()];
    config.max_early_data_size = 0;
    config.send_tls13_tickets = 0;
    config.session_storage = Arc::new(rustls::server::NoServerSessionStorage {});
    config.cert_compressors = Vec::new();
    config.cert_decompressors = Vec::new();
    Ok(config)
}

/// The TLS client configuration: TLS 1.3 only with the ring provider, the
/// raw public key in `key`, the server checked by `verifier`, the fixed
/// ALPN, no SNI, and no session resumption or early data. `identity` builds
/// the key and the verifier.
pub(crate) fn tls_client_config(
    key: Arc<rustls::sign::CertifiedKey>,
    verifier: Arc<dyn rustls::client::danger::ServerCertVerifier>,
) -> Result<quinn::crypto::rustls::QuicClientConfig, TlsConfigError> {
    let config = rustls_client_config(key, verifier)?;
    Ok(quinn::crypto::rustls::QuicClientConfig::try_from(
        Arc::new(config),
    )?)
}

/// The rustls half of [`tls_client_config`].
pub(crate) fn rustls_client_config(
    key: Arc<rustls::sign::CertifiedKey>,
    verifier: Arc<dyn rustls::client::danger::ServerCertVerifier>,
) -> Result<rustls::ClientConfig, rustls::Error> {
    let mut config = rustls::ClientConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_client_cert_resolver(Arc::new(
            rustls::client::AlwaysResolvesClientRawPublicKeys::new(key),
        ));
    config.alpn_protocols = vec![ALPN.to_vec()];
    config.enable_sni = false;
    config.enable_early_data = false;
    config.resumption = rustls::client::Resumption::disabled();
    config.cert_compressors = Vec::new();
    config.cert_decompressors = Vec::new();
    Ok(config)
}

/// A TLS configuration quinn or rustls rejected.
#[derive(Debug, thiserror::Error)]
pub(crate) enum TlsConfigError {
    #[error(transparent)]
    Rustls(#[from] rustls::Error),
    #[error(transparent)]
    NoInitialCipherSuite(#[from] quinn::crypto::rustls::NoInitialCipherSuite),
}

/// quinn's system clock (used to date handshake tokens) over `time`, so it
/// follows `VirtualTime` too. The epoch is arbitrary: tokens are not used,
/// and nothing compares this with real dates.
pub(crate) fn system_clock<T: TimeSource>(time: T) -> Arc<dyn quinn::TimeSource> {
    Arc::new(SystemClock(time))
}

struct SystemClock<T>(T);

impl<T: TimeSource> quinn::TimeSource for SystemClock<T> {
    fn now(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH + self.0.now()
    }
}

//! A QUIC endpoint (quinn) on the UDP transport's port (#5940, step c2).
//!
//! The endpoint reads only the QUIC-class datagrams the [`DemuxSocket`] queues
//! for it, and sends through the same socket, so it shares the port, the
//! node-wide egress cap and the simulation socket with the UDP transport.
//! Nothing in this step connects to anyone: with `transport-quic = "prefer"`
//! the node builds the endpoint at bind with no server configuration and an
//! empty allow-set, so no datagram reaches it and it sends nothing. Peer
//! identity (step c3) and upgrading a UDP transport session to QUIC come
//! later; until then only tests open QUIC connections.
//!
//! The parts:
//!
//! - [`socket`]: `QuicSocket`, quinn's `AsyncUdpSocket` over the demux
//!   socket. Every datagram it sends is admitted by the egress cap first.
//! - [`runtime`]: quinn's `Runtime` over the crate's [`TimeSource`], so
//!   `VirtualTime` drives quinn's timers in tests.
//! - [`config`]: every quinn and rustls setting, pinned in one place.
//! - [`cid`]: connection IDs that carry a keyed MAC, so quinn drops a short
//!   header packet with a forged ID silently instead of answering it.
//!
//! [`DemuxSocket`]: super::DemuxSocket
//! [`TimeSource`]: crate::simulation::TimeSource

pub(crate) mod cid;
pub(crate) mod config;
pub(crate) mod runtime;
pub(crate) mod socket;

#[cfg(test)]
pub(crate) mod test_tls;
#[cfg(test)]
mod tests;

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use super::Socket;
use super::demux::{DemuxSocket, QuicSourcePermit};
use crate::config::GlobalExecutor;
use crate::simulation::TimeSource;

pub(crate) use config::QuicSettings;

/// Whether the node may use QUIC: the `transport-quic` config key.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum QuicMode {
    /// No QUIC endpoint is built and no QUIC datagram is read. The default.
    #[default]
    Off,
    /// The node builds a QUIC endpoint on its UDP port. In this release it
    /// accepts nothing and connects to nobody, so the node behaves as with
    /// `Off` apart from the endpoint's memory.
    Prefer,
}

impl QuicMode {
    /// The value written in `config.toml` and accepted on the command line.
    pub const OFF: &'static str = "off";
    /// See [`QuicMode::Prefer`].
    pub const PREFER: &'static str = "prefer";

    /// The mode for a configured `transport-quic` value. Unset is `Off`. An
    /// unknown value is `Off` with a warning, never an error: a value a
    /// later release added must not stop an older binary (a crash-loop
    /// rollback reinstalls one) from starting.
    pub fn from_config_value(value: Option<&str>) -> Self {
        match value.map(|v| v.trim().to_ascii_lowercase()) {
            None => Self::Off,
            Some(v) if v == Self::OFF => Self::Off,
            Some(v) if v == Self::PREFER => Self::Prefer,
            Some(other) => {
                tracing::warn!(
                    transport_quic = %other,
                    "Unknown transport-quic value; QUIC stays off. Known values: off, prefer"
                );
                Self::Off
            }
        }
    }

    /// Whether `value` is anything other than the default (`off` or unset),
    /// so `ConfigArgs::build` merges it from `config.toml`. An unknown value
    /// counts as non-default, so it survives a rewrite of the file and a
    /// later binary that knows it still sees it.
    pub(crate) fn is_non_default_value(value: &str) -> bool {
        !value.trim().eq_ignore_ascii_case(Self::OFF)
    }
}

/// The node's QUIC endpoint. Built only when `transport-quic` is not `off`.
///
/// Holds the quinn endpoint and a way to add peers to the demux socket's
/// allow-set. Dropping it drops the quinn endpoint handle; quinn's driver
/// task ends once no connection is left.
pub(crate) struct QuicEndpoint {
    endpoint: quinn::Endpoint,
    sources: Arc<dyn QuicSources>,
    /// The transport settings of every connection (`config::transport_config`).
    #[cfg_attr(not(test), allow(dead_code))]
    transport: Arc<quinn::TransportConfig>,
    /// quinn's system clock on the endpoint's time source.
    #[cfg_attr(not(test), allow(dead_code))]
    clock: Arc<dyn quinn::TimeSource>,
}

/// The demux socket's QUIC allow-set, without its socket type.
trait QuicSources: Send + Sync {
    fn allow(&self, addr: SocketAddr) -> Option<QuicSourcePermit>;
    fn is_allowed(&self, addr: SocketAddr) -> bool;
}

impl<S: Socket> QuicSources for DemuxSocket<S> {
    fn allow(&self, addr: SocketAddr) -> Option<QuicSourcePermit> {
        self.allow_quic_source(addr)
    }

    fn is_allowed(&self, addr: SocketAddr) -> bool {
        self.is_quic_source_allowed(addr)
    }
}

impl std::fmt::Debug for QuicEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuicEndpoint")
            .field("local_addr", &self.endpoint.local_addr().ok())
            .finish_non_exhaustive()
    }
}

/// Why [`QuicEndpoint::connect`] could not start a connection.
#[derive(Debug, thiserror::Error)]
pub(crate) enum QuicConnectError {
    /// The demux socket's allow-set is full, so the peer's replies could not
    /// reach the endpoint.
    #[error("the QUIC allow-set is full")]
    AllowSetFull,
    #[error(transparent)]
    Connect(#[from] quinn::ConnectError),
    #[error(transparent)]
    Connection(#[from] quinn::ConnectionError),
}

impl QuicEndpoint {
    /// Build an endpoint over `socket`, which must be the socket the
    /// listener reads (it fills the QUIC ingress queue), bound at
    /// `local_addr`.
    ///
    /// `time` drives quinn's clock and timers. It must be the clock the
    /// socket's egress cap was built on, so the cap computes a waiting
    /// sender's deficit on the clock that sender's timer runs on: the node
    /// passes `RealTime` for both (`bind_udp_listener`), tests one
    /// `VirtualTime` for both.
    ///
    /// Enables the socket's QUIC ingress with an empty allow-set and sets no
    /// server configuration, so the endpoint accepts nothing until
    /// [`Self::set_server_config`] and receives nothing until a peer is
    /// allowed ([`Self::allow_peer`], or a connection this endpoint opens).
    /// Fails if the socket's ingress was already enabled.
    pub(crate) fn bind<S: Socket, T: TimeSource>(
        socket: Arc<DemuxSocket<S>>,
        local_addr: SocketAddr,
        time: T,
        settings: &QuicSettings,
    ) -> io::Result<Self> {
        let ingress = socket.enable_quic_ingress().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::AlreadyExists,
                "the socket's QUIC ingress is already enabled",
            )
        })?;
        let sources: Arc<dyn QuicSources> = socket.clone();
        let quic_socket = Arc::new(socket::QuicSocket::new(socket, ingress, local_addr));
        let transport = Arc::new(config::transport_config(settings, &time));
        let clock = config::system_clock(time.clone());
        let runtime = Arc::new(runtime::QuicRuntime::new(time));
        let endpoint = quinn::Endpoint::new_with_abstract_socket(
            config::endpoint_config(),
            None,
            quic_socket,
            runtime,
        )?;
        tracing::info!(
            %local_addr,
            window_bytes = settings.window_bytes(),
            "QUIC endpoint built (transport-quic = prefer); it accepts no connections in this release"
        );
        Ok(Self {
            endpoint,
            sources,
            transport,
            clock,
        })
    }

    /// Let QUIC datagrams from `addr` reach the endpoint while the permit
    /// is alive: a peer expected to connect to this endpoint. `None` if the
    /// allow-set is full.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn allow_peer(&self, addr: SocketAddr) -> Option<QuicSourcePermit> {
        self.sources.allow(addr)
    }

    /// A server configuration with this endpoint's settings over `crypto`
    /// (`config::server_config`).
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn server_config(
        &self,
        crypto: Arc<dyn quinn::crypto::ServerConfig>,
    ) -> quinn::ServerConfig {
        config::server_config(crypto, self.transport.clone(), self.clock.clone())
    }

    /// A client configuration with this endpoint's settings over `crypto`
    /// (`config::client_config`).
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn client_config(
        &self,
        crypto: Arc<dyn quinn::crypto::ClientConfig>,
    ) -> quinn::ClientConfig {
        config::client_config(crypto, self.transport.clone())
    }

    /// Accept incoming connections with `config`. Nothing calls this outside
    /// tests until peers have QUIC identities (step c3).
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn set_server_config(&self, config: quinn::ServerConfig) {
        self.endpoint.set_server_config(Some(config));
    }

    /// Connect to `addr` and wait for the handshake. The peer's address is
    /// on the allow-set for the life of the connection.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn connect(
        &self,
        addr: SocketAddr,
        config: quinn::ClientConfig,
    ) -> Result<QuicConnection, QuicConnectError> {
        let permit = self
            .allow_peer(addr)
            .ok_or(QuicConnectError::AllowSetFull)?;
        let connecting = self
            .endpoint
            .connect_with(config, addr, config::SERVER_NAME)?;
        let connection = connecting.await?;
        Ok(QuicConnection::new(connection, permit))
    }

    /// The next incoming connection, once its handshake completes. An
    /// attempt from an address that is not on the allow-set (which the demux
    /// socket should never pass on) is ignored, and so is one whose handshake
    /// fails. `None` once the endpoint is closed.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn accept(&self) -> Option<QuicConnection> {
        loop {
            let incoming = self.endpoint.accept().await?;
            let remote = incoming.remote_address();
            // The demux socket passes on only allowed sources, so this is a
            // second check on the same set, not the filter itself.
            let permit = if self.sources.is_allowed(remote) {
                self.allow_peer(remote)
            } else {
                None
            };
            let Some(permit) = permit else {
                tracing::debug!(%remote, "Ignoring a QUIC connection attempt from a source not allowed");
                incoming.ignore();
                continue;
            };
            match incoming.await {
                Ok(connection) => return Some(QuicConnection::new(connection, permit)),
                Err(error) => {
                    tracing::debug!(%error, "Incoming QUIC handshake failed");
                }
            }
        }
    }
}

/// An established QUIC connection. The peer's address stays on the demux
/// socket's allow-set until the connection ends: closed by either side,
/// timed out, failed, or dropped here (dropping closes it).
pub(crate) struct QuicConnection {
    connection: quinn::Connection,
}

impl QuicConnection {
    fn new(connection: quinn::Connection, permit: QuicSourcePermit) -> Self {
        // The watcher holds the permit until quinn reports the connection
        // closed, however it ends. Dropping this handle closes it explicitly
        // (see Drop), because the watcher's clone keeps quinn from closing
        // it implicitly.
        let watched = connection.clone();
        GlobalExecutor::spawn(async move {
            let reason = watched.closed().await;
            tracing::debug!(%reason, remote = %watched.remote_address(), "QUIC connection ended");
            drop(permit);
        });
        Self { connection }
    }

    /// The quinn connection.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn inner(&self) -> &quinn::Connection {
        &self.connection
    }
}

impl Drop for QuicConnection {
    fn drop(&mut self) {
        self.connection.close(0u32.into(), b"closed");
    }
}

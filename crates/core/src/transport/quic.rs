//! A QUIC endpoint (quinn) on the UDP transport's port (#5940, step c2).
//!
//! The endpoint reads only the QUIC-class datagrams the [`DemuxSocket`] queues
//! for it, and sends through the same socket, so it shares the port, the
//! node-wide egress cap and the simulation socket with the UDP transport.
//! Nothing in the node connects to anyone yet: with `transport-quic =
//! "prefer"` the node builds the endpoint at bind with an empty allow-set, so
//! no datagram reaches it and it sends nothing. Upgrading a UDP transport
//! session to QUIC comes later; until then only tests open QUIC connections.
//!
//! Every connection is authenticated by the transport keys the two nodes
//! already have ([`identity`]): [`QuicEndpoint::connect`] takes the server's
//! expected [`TransportPublicKey`], and [`QuicEndpoint::allow_peer`] the
//! client's, and a handshake with any other key fails.
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
//! - [`identity`]: the TLS raw public key, XEdDSA signer and pinned
//!   verifiers made from the node's X25519 transport key.
//! - [`error`]: quinn's and the framing's errors mapped to
//!   [`QuicError`](super::QuicError) kinds, and the application error
//!   codes (connection close, stream stop and reset), one range each.
//! - [`liveness`]: whether the endpoint is alive ([`QuicAvailability`]);
//!   endpoint death turns QUIC off for the node and never stops it.
//! - [`upgrade`]: upgrading a UDP session to QUIC. So far only the policy
//!   ([`upgrade::policy`]): whether the node may upgrade at all, and how
//!   many paths it may hold.
//!
//! [`DemuxSocket`]: super::DemuxSocket
//! [`TransportPublicKey`]: super::TransportPublicKey
//! [`TimeSource`]: crate::simulation::TimeSource

pub(crate) mod breaker;
pub(crate) mod cid;
pub(crate) mod config;
pub(crate) mod error;
pub(crate) mod framing;
pub(crate) mod identity;
pub(crate) mod liveness;
pub(crate) mod path;
pub(crate) mod recycle;
pub(crate) mod runtime;
pub(crate) mod socket;
pub(crate) mod upgrade;

#[cfg(test)]
mod memory_tests;
#[cfg(test)]
pub(crate) mod test_tls;
#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::mpsc;

use super::Socket;
use super::crypto::{TransportKeypair, TransportPublicKey};
use super::demux::{DemuxSocket, QuicSourcePermit};
use super::metrics::{QuicCloseCode, TRANSPORT_METRICS};
use crate::config::GlobalExecutor;
use crate::node::background_task_monitor::BackgroundTaskMonitor;
use crate::simulation::TimeSource;
use identity::{PeerKey, QuicIdentity};
use liveness::EndpointHealth;

pub(crate) use config::QuicSettings;
pub(crate) use liveness::QuicAvailability;
pub(crate) use liveness::QuicAvailabilityWatch;

/// Whether the node may use QUIC: the `transport-quic` config key.
///
/// Off is the default on every platform, and must stay the default on
/// Windows until the crash-loop breaker works there (#6031): the breaker is
/// unix-only ([`breaker`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum QuicMode {
    /// No QUIC endpoint is built and no QUIC datagram is read. The default.
    #[default]
    Off,
    /// The node builds a QUIC endpoint on its UDP port and may upgrade a UDP
    /// session to a QUIC path that carries only control frames: all
    /// application traffic stays on UDP. In this release nothing in the node
    /// upgrades yet ([`upgrade::policy`] is not wired in), so the endpoint
    /// accepts and opens no connection. It still differs from `Off` in three
    /// ways: the demux socket has a QUIC ingress, so a QUIC-class datagram is
    /// counted `quic_ingress_not_allowed` instead of
    /// `quic_ingress_no_ingress`; quinn's endpoint driver runs as a task
    /// (idle, waiting on the empty ingress queue); and the endpoint's memory.
    /// If the endpoint cannot be built the node logs a warning and runs with
    /// QUIC off.
    Shadow,
    /// Meant to carry application data over QUIC. Until that ships it acts
    /// as [`Self::Shadow`] (see [`Self::effective`]), so a node that sets it
    /// early starts carrying data over QUIC once it runs a release where
    /// `prefer` does. That release's notes must say so.
    Prefer,
}

impl QuicMode {
    /// The value written in `config.toml` and accepted on the command line.
    pub const OFF: &'static str = "off";
    /// See [`QuicMode::Shadow`].
    pub const SHADOW: &'static str = "shadow";
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
            Some(v) if v == Self::SHADOW => Self::Shadow,
            Some(v) if v == Self::PREFER => Self::Prefer,
            Some(other) => {
                tracing::warn!(
                    transport_quic = %other,
                    "Unknown transport-quic value; QUIC stays off. Known values: off, shadow, prefer"
                );
                Self::Off
            }
        }
    }

    /// Whether the node builds a QUIC endpoint in this mode.
    pub(crate) fn builds_endpoint(self) -> bool {
        match self {
            Self::Off => false,
            Self::Shadow | Self::Prefer => true,
        }
    }

    /// The mode this release runs: `Off` and `Shadow` are themselves, and
    /// `Prefer` runs as `Shadow`. Startup says so at INFO
    /// ([`upgrade::policy::log_startup`]).
    pub(crate) fn effective(self) -> Self {
        match self {
            Self::Off => Self::Off,
            Self::Shadow | Self::Prefer => Self::Shadow,
        }
    }
}

/// The node's QUIC endpoint. Built only when `transport-quic` is not `off`.
///
/// Holds the quinn endpoint, the node's QUIC identity, and a way to add peers
/// to the demux socket's allow-set. Clones share all of it. Dropping the last
/// handle drops the quinn endpoint; quinn's driver task ends once no
/// connection is left.
#[derive(Clone)]
pub(crate) struct QuicEndpoint {
    // Declared first so it is dropped before `core`: the node's last handle
    // marks the endpoint released before quinn's driver can see no handle
    // left and end, so that end is not taken for a death.
    owner: Arc<EndpointOwner>,
    core: EndpointCore,
}

impl std::ops::Deref for QuicEndpoint {
    type Target = EndpointCore;

    fn deref(&self) -> &EndpointCore {
        &self.core
    }
}

/// Everything an endpoint handle holds except its owner share: what the
/// accept loop holds, so it does not keep the endpoint owned (see
/// [`QuicEndpoint::spawn_accept_loop`]).
///
/// Warning: a clone of this keeps quinn's endpoint, its driver and the
/// socket alive WITHOUT counting as an owner, so the endpoint can be
/// `Released` while a core clone still holds all of it. Only the accept
/// loop holds one, and drops it when the endpoint is released.
#[derive(Clone)]
pub(crate) struct EndpointCore {
    endpoint: quinn::Endpoint,
    sources: Arc<dyn QuicSources>,
    /// The transport settings of every connection (`config::transport_config`).
    transport: Arc<quinn::TransportConfig>,
    /// The server settings every accepted connection starts from
    /// (`config::server_config`), with TLS that refuses every client: each
    /// accepted attempt replaces the TLS with one pinned to the client it
    /// expects ([`QuicIncoming::establish`]).
    server: Arc<quinn::ServerConfig>,
    /// This node's raw public key and signer.
    identity: Arc<QuicIdentity>,
    /// The transport key each allowed client must present.
    expected_clients: Arc<ExpectedClients>,
    /// The endpoint's health, handed to each connection (its watcher, and
    /// its closer, which does not count a close after an adapter panic).
    health: Arc<EndpointHealth>,
    /// The QUIC transmits the egress cap holds (`socket::CapHolds`).
    cap_holds: Arc<socket::CapHolds>,
    /// Test hooks that inject faults into quinn's drivers.
    #[cfg(test)]
    faults: Arc<socket::SocketFaults>,
    /// The type name of the clock quinn runs on (tests).
    #[cfg(any(test, feature = "testing"))]
    clock: &'static str,
}

/// Shared by every handle to one endpoint; the last one dropped marks the
/// endpoint released ([`EndpointHealth::release`]).
struct EndpointOwner {
    health: Arc<EndpointHealth>,
    /// Attempts the accept loop ignored because its queue was full.
    accept_queue_full: Arc<std::sync::atomic::AtomicU64>,
    /// Whether the accept loop was started (it may be started once).
    accept_loop_started: std::sync::atomic::AtomicBool,
}

impl Drop for EndpointOwner {
    fn drop(&mut self) {
        self.health.release();
    }
}

/// The key each allowed client address must present. An entry counts the
/// [`QuicPeerPermit`]s that allow the address and, separately, the attempts
/// and connections authenticated (or being authenticated) against that key
/// ([`KeyHold`]). It leaves when both counts reach zero, so while an attempt
/// or connection for a key is alive the address cannot be allowed for
/// another key ([`QuicAllowError::KeyConflict`]). New attempts are accepted
/// only while a permit is alive.
#[derive(Default)]
struct ExpectedClients {
    by_addr: Mutex<HashMap<SocketAddr, ExpectedEntry>>,
}

struct ExpectedEntry {
    key: PeerKey,
    permits: usize,
    holds: usize,
}

impl ExpectedClients {
    /// The key `addr` was allowed with, if a permit for it is alive.
    #[cfg(test)]
    fn get(&self, addr: SocketAddr) -> Option<PeerKey> {
        self.by_addr
            .lock()
            .get(&addr)
            .filter(|entry| entry.permits > 0)
            .map(|entry| entry.key.clone())
    }

    /// The key `addr` was allowed with and a hold on it, in one step, if a
    /// permit for it is alive.
    fn hold(self: &Arc<Self>, addr: SocketAddr) -> Option<(PeerKey, KeyHold)> {
        let mut by_addr = self.by_addr.lock();
        let entry = by_addr.get_mut(&addr).filter(|entry| entry.permits > 0)?;
        entry.holds += 1;
        Some((
            entry.key.clone(),
            KeyHold {
                expected: self.clone(),
                addr,
            },
        ))
    }

    /// Count one release of `addr`'s entry, a permit or a hold.
    fn release(&self, addr: SocketAddr, permit: bool) {
        let mut by_addr = self.by_addr.lock();
        if let Some(entry) = by_addr.get_mut(&addr) {
            if permit {
                entry.permits = entry.permits.saturating_sub(1);
            } else {
                entry.holds = entry.holds.saturating_sub(1);
            }
            if entry.permits == 0 && entry.holds == 0 {
                by_addr.remove(&addr);
            }
        }
    }
}

/// Keeps an address bound to the key an attempt or connection from it is
/// authenticated against, while alive.
struct KeyHold {
    expected: Arc<ExpectedClients>,
    addr: SocketAddr,
}

impl Drop for KeyHold {
    fn drop(&mut self) {
        self.expected.release(self.addr, false);
    }
}

/// Keeps a peer's address on the allow-set, and the key it must present
/// when it connects, while alive. Returned by [`QuicEndpoint::allow_peer`].
#[must_use = "the peer is allowed only while the permit is alive"]
pub(crate) struct QuicPeerPermit {
    expected: Arc<ExpectedClients>,
    addr: SocketAddr,
    // Declared last so it is dropped after the key entry: between the two,
    // an attempt from the address finds no key and is ignored.
    _source: QuicSourcePermit,
}

impl std::fmt::Debug for QuicPeerPermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuicPeerPermit")
            .field("addr", &self.addr)
            .finish_non_exhaustive()
    }
}

impl Drop for QuicPeerPermit {
    fn drop(&mut self) {
        self.expected.release(self.addr, true);
    }
}

/// Why [`QuicEndpoint::allow_peer`] refused a peer.
#[derive(Debug, thiserror::Error)]
pub(crate) enum QuicAllowError {
    /// The demux socket's allow-set is full.
    #[error("the QUIC allow-set is full")]
    AllowSetFull,
    /// The address is already bound to another key: a permit for it, or an
    /// attempt or connection authenticated against it, is alive. The earlier
    /// key wins until all of those are gone, so an address cannot be moved to
    /// a new key while anything for the old one is using it.
    #[error("the address is already allowed for another key")]
    KeyConflict,
    #[error(transparent)]
    UnusableKey(#[from] identity::UnusableKey),
}

/// The demux socket's QUIC allow-set, without its socket type.
trait QuicSources: Send + Sync {
    fn allow(&self, addr: SocketAddr) -> Option<QuicSourcePermit>;
    fn allow_existing(&self, addr: SocketAddr) -> Option<QuicSourcePermit>;
}

impl<S: Socket> QuicSources for DemuxSocket<S> {
    fn allow(&self, addr: SocketAddr) -> Option<QuicSourcePermit> {
        self.allow_quic_source(addr)
    }

    fn allow_existing(&self, addr: SocketAddr) -> Option<QuicSourcePermit> {
        self.allow_existing_quic_source(addr)
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
    UnusableKey(#[from] identity::UnusableKey),
    #[error(transparent)]
    Tls(#[from] config::TlsConfigError),
    #[error(transparent)]
    Connect(#[from] quinn::ConnectError),
    #[error(transparent)]
    Connection(#[from] quinn::ConnectionError),
}

/// The QUIC transport contains a panic in its socket adapter by catching
/// the unwind (`socket`, `liveness`). A build with `panic = "abort"` cannot
/// do that: the first such panic would end the process. So the endpoint is
/// refused there, and the node runs with QUIC off (`bind_or_warn` logs it).
/// A runtime check rather than a compile-time one, so such a build still
/// compiles and runs with QUIC off. It covers a build whose profile sets
/// `panic = "abort"`; this crate built to unwind and then linked into an
/// aborting binary passes it.
fn require_unwinding(unwinds: bool) -> io::Result<()> {
    if unwinds {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "this build aborts on panic (panic = \"abort\"), and the QUIC transport \
             needs panics to unwind to contain them",
        ))
    }
}

/// Whether this build unwinds on panic, which the QUIC transport needs
/// ([`require_unwinding`]).
pub(crate) const UNWINDS: bool = cfg!(panic = "unwind");

impl QuicEndpoint {
    /// Build an endpoint over `socket`, which must be the socket the
    /// listener reads (it fills the QUIC ingress queue), bound at
    /// `local_addr`, authenticating as the holder of `keypair`.
    ///
    /// `time` drives quinn's clock and timers. It must be the clock the
    /// socket's egress cap was built on, so the cap computes a waiting
    /// sender's deficit on the clock that sender's timer runs on: the node
    /// passes `RealTime` for both (`bind_udp_listener`), tests one
    /// `VirtualTime` for both.
    ///
    /// Enables the socket's QUIC ingress with an empty allow-set, so the
    /// endpoint receives nothing until a peer is allowed
    /// ([`Self::allow_peer`], or a connection this endpoint opens). Fails if
    /// the socket's ingress was already enabled, or if the TLS
    /// configuration cannot be built, or if the build aborts on panic
    /// ([`require_unwinding`]).
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn bind<S: Socket, T: TimeSource>(
        socket: Arc<DemuxSocket<S>>,
        local_addr: SocketAddr,
        time: T,
        settings: &QuicSettings,
        keypair: &TransportKeypair,
    ) -> io::Result<Self> {
        Self::bind_checked(UNWINDS, socket, local_addr, time, settings, keypair)
    }

    /// [`Self::bind`] for a build that does (`unwinds`) or does not unwind
    /// on panic; tests drive the aborting case through it.
    fn bind_checked<S: Socket, T: TimeSource>(
        unwinds: bool,
        socket: Arc<DemuxSocket<S>>,
        local_addr: SocketAddr,
        time: T,
        settings: &QuicSettings,
        keypair: &TransportKeypair,
    ) -> io::Result<Self> {
        require_unwinding(unwinds)?;
        Self::bind_with(
            socket,
            local_addr,
            time,
            settings,
            keypair,
            config::endpoint_config()?,
        )
    }

    /// Build the endpoint as [`Self::bind`] describes, for this build
    /// ([`UNWINDS`], through [`Self::bind_checked`]), or `None` with a WARN
    /// when it fails, so a node with QUIC on still starts, with QUIC off.
    pub(crate) fn bind_or_warn<S: Socket, T: TimeSource>(
        socket: Arc<DemuxSocket<S>>,
        local_addr: SocketAddr,
        time: T,
        settings: &QuicSettings,
        keypair: &TransportKeypair,
    ) -> Option<Self> {
        Self::bind_or_warn_checked(UNWINDS, socket, local_addr, time, settings, keypair)
    }

    /// [`Self::bind_or_warn`] through [`Self::bind_checked`].
    fn bind_or_warn_checked<S: Socket, T: TimeSource>(
        unwinds: bool,
        socket: Arc<DemuxSocket<S>>,
        local_addr: SocketAddr,
        time: T,
        settings: &QuicSettings,
        keypair: &TransportKeypair,
    ) -> Option<Self> {
        match Self::bind_checked(unwinds, socket, local_addr, time, settings, keypair) {
            Ok(endpoint) => Some(endpoint),
            Err(error) => {
                tracing::warn!(
                    %error,
                    %local_addr,
                    "Could not build the QUIC endpoint; running with QUIC off"
                );
                None
            }
        }
    }

    /// [`Self::bind`] with a given endpoint configuration; tests build an
    /// endpoint with quinn's own connection-ID generator through it.
    fn bind_with<S: Socket, T: TimeSource>(
        socket: Arc<DemuxSocket<S>>,
        local_addr: SocketAddr,
        time: T,
        settings: &QuicSettings,
        keypair: &TransportKeypair,
        endpoint_config: quinn::EndpointConfig,
    ) -> io::Result<Self> {
        let invalid = |error: &dyn std::fmt::Display| {
            io::Error::new(io::ErrorKind::InvalidInput, error.to_string())
        };
        let identity = Arc::new(QuicIdentity::new(keypair).map_err(|e| invalid(&e))?);
        let refusing = identity.refusing_server_tls().map_err(|e| invalid(&e))?;
        let sources: Arc<dyn QuicSources> = socket.clone();
        let transport = Arc::new(config::transport_config(settings, &time));
        let clock = config::system_clock(time.clone());
        let server = Arc::new(config::server_config(
            Arc::new(refusing),
            transport.clone(),
            clock,
        )?);
        let health = EndpointHealth::new(local_addr);
        #[cfg(test)]
        let mut faults = None;
        let mut cap_holds = None;
        // If quinn refuses the socket after the ingress is enabled, the
        // ingress is disabled again, so the node is left as with QUIC off.
        let endpoint = socket
            .with_quic_ingress(|ingress| {
                let quic_socket = Arc::new(socket::QuicSocket::new(
                    socket.clone(),
                    ingress,
                    local_addr,
                    health.clone(),
                ));
                cap_holds = Some(quic_socket.cap_holds());
                #[cfg(test)]
                {
                    faults = Some(quic_socket.faults());
                }
                let runtime = Arc::new(runtime::QuicRuntime::new(time, health.clone()));
                // The server configuration is set so quinn hands every
                // Initial from an allowed source to `accept` (without one it
                // answers with a stateless reset); its TLS refuses every
                // client, and `accept` replaces it per attempt.
                quinn::Endpoint::new_with_abstract_socket(
                    endpoint_config,
                    Some((*server).clone()),
                    quic_socket,
                    runtime,
                )
            })
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "the socket's QUIC ingress is already enabled",
                )
            })??;
        tracing::info!(
            %local_addr,
            window_bytes = settings.window_bytes(),
            "QUIC endpoint built (transport-quic = prefer); nothing opens connections on it in this release"
        );
        Ok(Self {
            owner: Arc::new(EndpointOwner {
                health: health.clone(),
                accept_queue_full: Arc::default(),
                accept_loop_started: std::sync::atomic::AtomicBool::new(false),
            }),
            core: EndpointCore {
                endpoint,
                sources,
                transport,
                server,
                identity,
                expected_clients: Arc::new(ExpectedClients::default()),
                health: health.clone(),
                // Set by the closure, which ran: `endpoint` is built.
                cap_holds: cap_holds.expect("the QUIC socket's cap holds"),
                #[cfg(test)]
                faults: faults.unwrap_or_default(),
                #[cfg(any(test, feature = "testing"))]
                clock: std::any::type_name::<T>(),
            },
        })
    }

    /// Whether this endpoint is alive: `Up` until its driver dies, then
    /// `Down` for good (see [`liveness`]).
    pub(crate) fn availability(&self) -> QuicAvailabilityWatch {
        self.owner.health.watch()
    }

    /// Fail the endpoint's next receive, which ends quinn's endpoint driver
    /// as a socket that fails for good would (tests).
    #[cfg(test)]
    pub(crate) fn fail_driver(&self) {
        self.faults.fail_receive();
    }

    /// The hooks that inject faults into quinn's drivers (tests).
    #[cfg(test)]
    pub(crate) fn faults(&self) -> &socket::SocketFaults {
        &self.faults
    }

    /// The type name of the clock quinn runs on (tests).
    #[cfg(any(test, feature = "testing"))]
    pub(crate) fn clock_type(&self) -> &'static str {
        self.clock
    }

    /// Accept connection attempts in a task registered with `monitor` as
    /// `quic_accept_loop`, handing each to `attempts` without waiting. When
    /// `attempts` is full the attempt is ignored and counted
    /// ([`Self::accept_queue_full`], a WARN on the first and every 1024th):
    /// the client retransmits its Initial, as after a lost datagram.
    ///
    /// The task holds the endpoint's [`EndpointCore`], not an owner share,
    /// so it does not keep the endpoint alive: when the node drops its last
    /// handle the task sees the availability turn `Released`, drops the
    /// core (letting quinn's driver end and the socket go) and stops
    /// accepting. It also stops when `accept` returns `None` (closed, or the
    /// driver ended) or `attempts` is closed (a WARN: the consumer is gone).
    /// When it stops it PARKS instead of returning, because the monitor
    /// reads any end of a registered task as a node failure and an
    /// endpoint's end must not stop the node. Endpoint death is reported by
    /// the driver's watcher ([`Self::availability`]), not by this task. A
    /// panic here is still a node failure, as for every monitored task; a
    /// quinn panic is not contained (see `liveness`).
    ///
    /// `monitor` must not have started waiting yet
    /// ([`BackgroundTaskMonitor::wait_for_any_exit`] drains it, and a task
    /// registered after that is not monitored). In `run_node` that happens
    /// before the event listener opens the transport, so e5 must register
    /// this task through a monitor that is still live at that point.
    ///
    /// Attempts already queued in `attempts` each hold a quinn `Incoming`,
    /// which holds the quinn endpoint: until the consumer drains or drops
    /// them, they keep quinn's driver and the socket alive after the loop
    /// stopped. When it stops the loop drops `attempts`, so the consumer's
    /// `recv` returns `None` once the queue is drained.
    ///
    /// Call once per endpoint: two loops would split attempts between two
    /// consumers. A second call logs a WARN, starts nothing and returns
    /// `false`.
    #[cfg_attr(not(test), allow(dead_code))]
    #[must_use = "false means no accept loop was started"]
    pub(crate) fn spawn_accept_loop(
        &self,
        monitor: &BackgroundTaskMonitor,
        attempts: mpsc::Sender<QuicIncoming>,
    ) -> bool {
        if self
            .owner
            .accept_loop_started
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            tracing::warn!(
                "QUIC accept loop already started for this endpoint; not starting another"
            );
            return false;
        }
        let core = self.core.clone();
        let mut availability = self.availability();
        let full = self.owner.accept_queue_full.clone();
        let handle = GlobalExecutor::spawn(async move {
            loop {
                let incoming = tokio::select! {
                    incoming = core.accept() => incoming,
                    _ = availability.not_up() => None,
                };
                let Some(incoming) = incoming else { break };
                match attempts.try_send(incoming) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(incoming)) => {
                        let count = full.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                        if count == 1 || count % 1024 == 0 {
                            tracing::warn!(
                                remote = %incoming.incoming.remote_address(),
                                accept_queue_full = count,
                                "QUIC accept queue full; ignoring the attempt"
                            );
                        }
                        incoming.incoming.ignore();
                    }
                    Err(mpsc::error::TrySendError::Closed(incoming)) => {
                        tracing::warn!(
                            "QUIC accept loop: the attempt consumer is gone; no longer accepting"
                        );
                        incoming.incoming.ignore();
                        break;
                    }
                }
            }
            tracing::debug!("QUIC accept loop stopped; parking");
            drop(attempts);
            drop(core);
            std::future::pending::<()>().await;
        });
        monitor.register("quic_accept_loop", handle);
        true
    }

    /// The kind of any QUIC error from this endpoint (a quinn connection,
    /// read or write error, a path send error, a connect error), attributed
    /// to the endpoint when it is `Down` (see [`error::attribute`]). Use
    /// this, not the bare mapping, to decide what to do about an error.
    ///
    /// There is a short window where the attribution is not yet possible:
    /// quinn's `EndpointDriver::drop` ends the endpoint's connections (local
    /// INTERNAL_ERROR) before the driver's watcher records `Down`, so on a
    /// multi-thread runtime a connection can end as `Protocol { code: 0x1 }`
    /// while the availability still reads `Up`. e5 must not take an
    /// irreversible per-peer action (a ban, a long backoff) on
    /// `Protocol { code: 0x1 }` without re-checking: wait on
    /// `availability().not_up()` with a short timeout (it never returns
    /// while the endpoint is healthy), then call this again.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::transport) fn error_kind(
        &self,
        error: impl Into<super::QuicErrorKind>,
    ) -> super::QuicErrorKind {
        error::attribute(self.availability().get(), error.into())
    }

    /// Attempts the accept loop ignored because its queue was full.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn accept_queue_full(&self) -> u64 {
        self.owner
            .accept_queue_full
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Let the holder of `expected` connect from `addr` while the permit is
    /// alive: QUIC datagrams from `addr` reach the endpoint, and an attempt
    /// from it completes only if the client presents `expected`.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn allow_peer(
        &self,
        addr: SocketAddr,
        expected: &TransportPublicKey,
    ) -> Result<QuicPeerPermit, QuicAllowError> {
        let key = PeerKey::of(expected)?;
        let source = self
            .sources
            .allow(addr)
            .ok_or(QuicAllowError::AllowSetFull)?;
        // The demux normalizes the address (an IPv4-mapped IPv6 address to
        // IPv4); key the entry the same way so `accept` finds it.
        let addr = source.addr();
        {
            let mut by_addr = self.expected_clients.by_addr.lock();
            match by_addr.get_mut(&addr) {
                Some(entry) if entry.key == key => entry.permits += 1,
                Some(_) => return Err(QuicAllowError::KeyConflict),
                None => {
                    by_addr.insert(
                        addr,
                        ExpectedEntry {
                            key,
                            permits: 1,
                            holds: 0,
                        },
                    );
                }
            }
        }
        Ok(QuicPeerPermit {
            expected: self.expected_clients.clone(),
            addr,
            _source: source,
        })
    }

    /// A client configuration with this endpoint's settings over `crypto`
    /// (`config::client_config`).
    fn client_config(&self, crypto: Arc<dyn quinn::crypto::ClientConfig>) -> quinn::ClientConfig {
        config::client_config(crypto, self.transport.clone())
    }

    /// Connect to `addr`, which must present the raw public key of
    /// `expected` (the peer's transport key), and wait for the handshake.
    /// The peer's address is on the allow-set for the life of the
    /// connection.
    ///
    /// Fails as `EndpointDown` (read through [`Self::error_kind`]) if the
    /// endpoint is Down or its socket adapter caught a panic when called:
    /// checked here, before quinn sees the attempt. When the endpoint driver
    /// returned, quinn already refuses (it dropped the driver before the
    /// endpoint went Down). After an adapter panic it refuses only once the
    /// woken endpoint driver has run again and been dropped, and on a
    /// multi-thread runtime a connect in that window would otherwise send an
    /// Initial. If the
    /// endpoint goes Down during the handshake, the handshake ends with an
    /// error that also reads as `EndpointDown` (see
    /// [`QuicIncoming::establish`]).
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn connect(
        &self,
        addr: SocketAddr,
        expected: &TransportPublicKey,
    ) -> Result<QuicConnection, QuicConnectError> {
        let peer = PeerKey::of(expected)?;
        let tls = self.identity.client_tls(&peer)?;
        let config = self.client_config(Arc::new(tls));
        self.connect_with(addr, config, peer).await
    }

    /// [`Self::connect`] with a given client configuration, which must pin
    /// the server to `peer`.
    async fn connect_with(
        &self,
        addr: SocketAddr,
        config: quinn::ClientConfig,
        peer: PeerKey,
    ) -> Result<QuicConnection, QuicConnectError> {
        if self.health.refuses_connects() {
            return Err(quinn::ConnectError::EndpointStopping.into());
        }
        let permit = self
            .sources
            .allow(addr)
            .ok_or(QuicConnectError::AllowSetFull)?;
        let connecting = self
            .endpoint
            .connect_with(config, addr, config::SERVER_NAME)?;
        let connection = connecting.await?;
        Ok(QuicConnection::new(
            connection,
            permit,
            None,
            peer,
            self.health.clone(),
            self.cap_holds.clone(),
        ))
    }
}

impl EndpointCore {
    /// The next incoming connection attempt from an allowed source, with its
    /// handshake NOT awaited: the caller drives it with
    /// [`QuicIncoming::establish`], so one stalled handshake does not hold
    /// back the next attempt. The attempt is pinned to the key its address
    /// was allowed with. An attempt from an address that is not on the
    /// allow-set (which the demux socket should not pass on; it can happen
    /// when the permit was dropped after the datagram was queued), or that
    /// has no expected key (this endpoint's own outgoing connection to it is
    /// what allowed it), is ignored. `None` once the endpoint is closed.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn accept(&self) -> Option<QuicIncoming> {
        loop {
            let incoming = self.endpoint.accept().await?;
            let remote = incoming.remote_address();
            // One step under the allow-set entry's lock, so a permit dropped
            // concurrently is not brought back.
            let Some(permit) = self.sources.allow_existing(remote) else {
                tracing::debug!(%remote, "Ignoring a QUIC connection attempt from a source not allowed");
                incoming.ignore();
                continue;
            };
            let Some((expected, hold)) = self.expected_clients.hold(permit.addr()) else {
                tracing::debug!(%remote, "Ignoring a QUIC connection attempt from a source with no expected key");
                incoming.ignore();
                continue;
            };
            let tls = match self.identity.server_tls(&expected) {
                Ok(tls) => tls,
                Err(error) => {
                    tracing::warn!(%remote, %error, "Could not build the TLS configuration for a QUIC connection attempt");
                    incoming.ignore();
                    continue;
                }
            };
            let mut config = (*self.server).clone();
            config.crypto = Arc::new(tls);
            return Some(QuicIncoming {
                health: self.health.clone(),
                incoming,
                permit,
                config: Arc::new(config),
                peer: expected,
                hold,
                cap_holds: self.cap_holds.clone(),
            });
        }
    }
}

/// An incoming connection attempt from an allowed source, holding a permit
/// for its address until the handshake ends.
pub(crate) struct QuicIncoming {
    incoming: quinn::Incoming,
    /// The endpoint's health, for the connection's watcher and closer.
    health: Arc<EndpointHealth>,
    permit: QuicSourcePermit,
    /// The endpoint's server settings with TLS pinned to the key the
    /// source was allowed with.
    config: Arc<quinn::ServerConfig>,
    /// That key, which the handshake must prove.
    peer: PeerKey,
    /// Keeps the address bound to `peer` while the attempt lives.
    hold: KeyHold,
    /// The endpoint's record of cap-held transmits.
    cap_holds: Arc<socket::CapHolds>,
}

impl QuicIncoming {
    /// Complete the handshake. On failure the permit is dropped with it.
    ///
    /// A socket-adapter panic in this connection's driver during the
    /// handshake (write readiness or send) ends the driver with an error,
    /// which skips quinn's own terminate. The handshake still ends: dropping
    /// the driver drops the second-to-last reference to the connection, and
    /// quinn's `ConnectionRef::drop` (quinn 0.11.12, `connection.rs:946`)
    /// then closes it implicitly, so this returns `LocallyClosed` (read as
    /// `EndpointDown` through `QuicEndpoint::error_kind`) and the permit
    /// goes with it. A panic anywhere else in the adapter (another
    /// connection's driver, or the endpoint's receive) ends the endpoint
    /// driver, whose drop ends this connection's driver with INTERNAL_ERROR,
    /// which also reads as `EndpointDown`. `connect` ends the same ways.
    /// Pinned by the `a_panic_during_*_ends_the_handshake_*` and
    /// `a_*_panic_during_connect_ends_the_handshake` tests.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn establish(self) -> Result<QuicConnection, quinn::ConnectionError> {
        let connection = self.incoming.accept_with(self.config)?.await?;
        Ok(QuicConnection::new(
            connection,
            self.permit,
            Some(self.hold),
            self.peer,
            self.health,
            self.cap_holds,
        ))
    }
}

/// An established QUIC connection. The peer's address stays on the demux
/// socket's allow-set until the connection ends: closed by either side,
/// timed out, failed, or dropped here (dropping closes it).
pub(crate) struct QuicConnection {
    connection: quinn::Connection,
    /// Closes the connection at most once, shared with the path's receive
    /// supervisor.
    closer: QuicCloser,
    /// The key the peer proved in the handshake.
    peer: PeerKey,
    /// The endpoint's record of cap-held transmits, for the path's watch.
    cap_holds: Arc<socket::CapHolds>,
}

impl std::fmt::Debug for QuicConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuicConnection")
            .field("remote", &self.connection.remote_address())
            .finish_non_exhaustive()
    }
}

impl QuicConnection {
    fn new(
        connection: quinn::Connection,
        permit: QuicSourcePermit,
        hold: Option<KeyHold>,
        peer: PeerKey,
        health: Arc<EndpointHealth>,
        cap_holds: Arc<socket::CapHolds>,
    ) -> Self {
        let mut availability = health.watch();
        let mut adapter_panic = health.adapter_panic_watch();
        let closer = QuicCloser::new(connection.clone(), health);
        // The watcher holds the permit, and on the server side the hold
        // binding the address to the client's key, until quinn reports the
        // connection closed, however it ends. Dropping this handle closes it
        // explicitly (see Drop), because the watcher's clone keeps quinn from
        // closing it implicitly. On EVERY Down (a driver that returned, as
        // well as a panic the socket adapter caught) the watcher closes the
        // connection locally with DOWNGRADE: after an adapter panic this
        // connection's driver may have ended without closing it, and a local
        // close is what makes calls waiting on it return an error (quinn's
        // close fails them at once, driver or not). The close is not
        // counted (`QuicCloser::close`), and the peer is not told: nothing
        // can be sent after Down (see `socket`). After a driver return quinn
        // has usually ended the connection already, and the close does
        // nothing. Released (the node dropped the endpoint) closes nothing
        // by itself: the connections stay with their owners. A
        // socket-adapter panic after Released leaves the availability
        // Released but makes every send of the endpoint fail, which can end
        // any of its connection drivers without closing the connection, so
        // the watcher then closes it the same way.
        let watched = connection.clone();
        let down_closer = closer.clone();
        GlobalExecutor::spawn(async move {
            let close_now = async {
                if availability.not_up().await != QuicAvailability::Down
                    && adapter_panic.wait_for(|panicked| *panicked).await.is_err()
                {
                    // Unreachable while this task runs: `down_closer` holds
                    // the health, whose sender therefore outlives this
                    // receiver. Kept as a safe fallback (never close).
                    std::future::pending::<()>().await;
                }
            };
            tokio::select! {
                _ = watched.closed() => {}
                _ = close_now => {
                    down_closer.close(QuicCloseCode::Downgrade);
                }
            }
            let reason = watched.closed().await;
            tracing::debug!(%reason, remote = %watched.remote_address(), "QUIC connection ended");
            drop(hold);
            drop(permit);
        });
        Self {
            connection,
            closer,
            peer,
            cap_holds,
        }
    }

    /// The key the peer proved in the handshake: as client, the server's
    /// expected key; as server, the key the client's address was allowed
    /// with. Attribute the connection to this key, not to its address.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn peer_key(&self) -> &PeerKey {
        &self.peer
    }

    /// The quinn connection.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn inner(&self) -> &quinn::Connection {
        &self.connection
    }
}

impl QuicConnection {
    /// Close the connection with `code`, which the peer reads in its
    /// connection error. Counted once, by code, if the connection was still
    /// open.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn close(&self, code: QuicCloseCode) {
        self.closer.close(code);
    }

    /// The endpoint's record of the QUIC transmits its egress cap holds.
    pub(crate) fn cap_holds(&self) -> Arc<socket::CapHolds> {
        self.cap_holds.clone()
    }

    /// The connection's closer, for another task that may close it (the
    /// path's receive supervisor).
    pub(crate) fn closer(&self) -> QuicCloser {
        self.closer.clone()
    }
}

impl Drop for QuicConnection {
    fn drop(&mut self) {
        self.closer.close(QuicCloseCode::NodeDroppedPeer);
    }
}

/// Closes one QUIC connection at most once, from any number of clones.
/// Every close the QUIC code sends goes through here.
#[derive(Clone)]
pub(crate) struct QuicCloser {
    connection: quinn::Connection,
    /// Set by the first close attempt, in one step: a second caller, on
    /// any thread, sends and counts nothing.
    closing: Arc<std::sync::atomic::AtomicBool>,
    /// The endpoint's health: a close is counted only while it can still
    /// be sent ([`EndpointHealth::closes_can_be_sent`]).
    health: Arc<EndpointHealth>,
}

impl QuicCloser {
    fn new(connection: quinn::Connection, health: Arc<EndpointHealth>) -> Self {
        Self {
            connection,
            closing: Arc::default(),
            health,
        }
    }

    /// Close the connection with `code` and count it, if no close was
    /// attempted before and the connection is still open. Returns whether
    /// this call closed it. Only if still open: closing an ended connection
    /// would count a close that sent nothing and replace the reason quinn
    /// reports (the peer's close) with LocallyClosed. A peer close landing
    /// between the check and `close` is still reported as LocallyClosed and
    /// counted; two local closers can no longer both count.
    ///
    /// Not counted once the endpoint is Down or its socket adapter caught a
    /// panic: quinn's drivers may have ended by then, so the close may never
    /// leave. `quic_close_sent_*` counts closes queued while they could be
    /// sent (an upper bound: a panic just after the count can still stop
    /// one). That covers the
    /// close the connection's watcher makes on Down, and a close after a
    /// panic that left the endpoint Released.
    pub(crate) fn close(&self, code: QuicCloseCode) -> bool {
        if self.closing.swap(true, std::sync::atomic::Ordering::AcqRel) {
            return false;
        }
        if self.connection.close_reason().is_some() {
            return false;
        }
        if self.health.closes_can_be_sent() {
            TRANSPORT_METRICS.record_quic_close_sent(code, self.connection.remote_address());
        }
        self.connection.close(code.code().into(), code.reason());
        true
    }

    /// Whether the connection is still open (quinn has no close reason).
    pub(crate) fn is_open(&self) -> bool {
        self.connection.close_reason().is_none()
    }

    /// The connection's remote address (for logs).
    pub(crate) fn remote_address(&self) -> SocketAddr {
        self.connection.remote_address()
    }

    /// Mark a close as in progress without closing (tests: a second
    /// closer arriving while the first has not closed yet).
    #[cfg(test)]
    pub(crate) fn mark_closing(&self) {
        self.closing
            .store(true, std::sync::atomic::Ordering::Release);
    }
}

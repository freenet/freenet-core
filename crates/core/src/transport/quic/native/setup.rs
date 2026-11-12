//! Setting up a native connection (#5940, design 2.4): punching, the QUIC
//! handshake with the native ALPN, and the Hello exchange, all inside one
//! setup window.
//!
//! Roles follow key order and are the caller's decision: the server calls
//! [`accept_native`], the client [`connect_native`]. Both punch toward the
//! other every [`PUNCH_INTERVAL`](super::punch::PUNCH_INTERVAL) until the
//! connection is up or the window ends. The client makes one dial for the
//! whole window. The server accepts one attempt at a time
//! ([`NativeWaiters`]) and takes the next if a handshake fails.
//!
//! The window is [`NATIVE_WINDOW`], cut to the caller's deadline; a setup with
//! less than [`MIN_WINDOW_LEFT`] left is not started. What a setup may send
//! toward one destination IP is bounded across setups by a [`PunchBudget`],
//! charged before anything is sent.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::time::Duration;

use super::exchange::{HelloFailure, PeerHello, exchange};
use super::hello::Hello;
use super::punch::{CLIENT_BYTES_PER_DIAL, PUNCH_BYTES_PER_DIAL, PunchBudget, punch};
use super::waiters::NativeWaiters;
use crate::simulation::TimeSource;
use crate::transport::connection_handler::native_version;
use crate::transport::crypto::TransportPublicKey;
use crate::transport::normalize_mapped_addr;
use crate::transport::quic::{QuicAllowError, QuicConnectError, QuicConnection, QuicEndpoint};

/// The setup window: punching, the handshake and the Hello exchange.
pub(crate) const NATIVE_WINDOW: Duration = Duration::from_secs(8);

/// A setup with less than this left of its window is not started.
pub(crate) const MIN_WINDOW_LEFT: Duration = Duration::from_secs(4);

/// The other side of a native setup, from the node's own record.
#[derive(Debug, Clone, Copy)]
pub(crate) struct NativePeer<'a> {
    pub addr: SocketAddr,
    /// The key the peer must prove in the handshake.
    pub key: &'a TransportPublicKey,
    /// Whether the peer is a gateway: only a gateway's refusal of this
    /// node's version sets the version-mismatch signal, as on UDP.
    pub is_gateway: bool,
}

/// A native connection that has finished its Hello exchange.
pub(crate) struct NativeConnection {
    pub connection: QuicConnection,
    pub peer: PeerHello,
}

impl std::fmt::Debug for NativeConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeConnection")
            .field("peer", &self.peer)
            .finish_non_exhaustive()
    }
}

/// Why a native setup gave no connection.
#[derive(Debug, thiserror::Error)]
pub(crate) enum NativeSetupError {
    #[error("less than {MIN_WINDOW_LEFT:?} of the setup window left")]
    TooLate,
    #[error("the punch budget for this destination is spent")]
    OverBudget,
    #[error("a native setup to this address is already waiting")]
    AlreadyWaiting,
    #[error(transparent)]
    Allow(#[from] QuicAllowError),
    #[error(transparent)]
    Connect(#[from] QuicConnectError),
    #[error("no connection within the setup window")]
    TimedOut,
    #[error(transparent)]
    Hello(#[from] HelloFailure),
}

/// The end of the window on `clock`: [`NATIVE_WINDOW`] from now, or the
/// caller's `deadline` if sooner, refused with less than [`MIN_WINDOW_LEFT`].
fn window_end<T: TimeSource>(clock: &T, deadline: Duration) -> Result<Duration, NativeSetupError> {
    let now = clock.now();
    let end = deadline.min(now + NATIVE_WINDOW);
    if end.saturating_sub(now) < MIN_WINDOW_LEFT {
        return Err(NativeSetupError::TooLate);
    }
    Ok(end)
}

/// This node's Hello to `peer`.
fn own_hello(peer: SocketAddr, caps: u32) -> Hello {
    Hello {
        protoc_version: native_version::OWN,
        caps,
        observed: normalize_mapped_addr(peer),
    }
}

/// Punch `to` until `end`, then wait for ever; raced with the setup, which
/// drops it.
async fn punch_until<T: TimeSource>(
    endpoint: &QuicEndpoint,
    to: SocketAddr,
    clock: &T,
    end: Duration,
) -> Infallible {
    punch(
        |to, datagram| endpoint.send_punch(datagram, to),
        to,
        clock,
        end,
    )
    .await;
    std::future::pending().await
}

/// Set up a native connection to `peer` as the client: punch, dial once with
/// the native ALPN, and exchange Hellos, all before the window ends
/// (`deadline`, on `clock`, cut to [`NATIVE_WINDOW`] from now).
pub(crate) async fn connect_native<T: TimeSource>(
    endpoint: &QuicEndpoint,
    budget: &PunchBudget,
    peer: NativePeer<'_>,
    caps: u32,
    clock: &T,
    deadline: Duration,
) -> Result<NativeConnection, NativeSetupError> {
    let end = window_end(clock, deadline)?;
    if !budget.charge(peer.addr, CLIENT_BYTES_PER_DIAL, clock.now()) {
        return Err(NativeSetupError::OverBudget);
    }
    let left = end.saturating_sub(clock.now());
    let connection = tokio::select! {
        dialled = endpoint.dial_native(peer.addr, peer.key) => dialled?,
        () = clock.sleep_cancelable(left) => return Err(NativeSetupError::TimedOut),
        never = punch_until(endpoint, peer.addr, clock, end) => match never {},
    };
    finish(connection, peer, caps, clock, end).await
}

/// Set up a native connection from `peer` as the server: allow the peer as
/// native, punch, and take its attempts one at a time until one completes
/// its handshake, then exchange Hellos, all before the window ends.
pub(crate) async fn accept_native<T: TimeSource>(
    endpoint: &QuicEndpoint,
    waiters: &std::sync::Arc<NativeWaiters>,
    budget: &PunchBudget,
    peer: NativePeer<'_>,
    caps: u32,
    clock: &T,
    deadline: Duration,
) -> Result<NativeConnection, NativeSetupError> {
    let end = window_end(clock, deadline)?;
    let mut waiter = waiters
        .register(peer.addr)
        .ok_or(NativeSetupError::AlreadyWaiting)?;
    if !budget.charge(peer.addr, PUNCH_BYTES_PER_DIAL, clock.now()) {
        return Err(NativeSetupError::OverBudget);
    }
    // Held until the handshake ends; the connection then keeps its own.
    let _permit = endpoint.allow_native_peer(peer.addr, peer.key)?;
    let accept = async {
        loop {
            let Some(attempt) = waiter.next().await else {
                // The router holds the sender while the waiter lives.
                return Err(NativeSetupError::TimedOut);
            };
            let _in_flight = waiter.in_flight();
            match attempt.establish().await {
                Ok(connection) => return Ok(connection),
                Err(error) => {
                    tracing::debug!(addr = %peer.addr, %error, "A native QUIC handshake failed; waiting for the next attempt");
                }
            }
        }
    };
    let left = end.saturating_sub(clock.now());
    let connection = tokio::select! {
        accepted = accept => accepted?,
        () = clock.sleep_cancelable(left) => return Err(NativeSetupError::TimedOut),
        never = punch_until(endpoint, peer.addr, clock, end) => match never {},
    };
    drop(waiter);
    finish(connection, peer, caps, clock, end).await
}

/// The Hello exchange on a new connection, within what is left of the window.
async fn finish<T: TimeSource>(
    connection: QuicConnection,
    peer: NativePeer<'_>,
    caps: u32,
    clock: &T,
    end: Duration,
) -> Result<NativeConnection, NativeSetupError> {
    let own = own_hello(peer.addr, caps);
    let left = end.saturating_sub(clock.now());
    let hello = exchange(&connection, &own, peer.is_gateway, clock, left).await?;
    Ok(NativeConnection {
        connection,
        peer: hello,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::config::GlobalRng;
    use crate::transport::demux::{PUNCH_FIRST_BYTE, PUNCH_LEN};
    use crate::transport::quic::native::punch::{PUNCH_BUDGET_BYTES, PUNCHES_PER_DIAL};
    use crate::transport::quic::tests::{Pair, bounded, drive, pair};

    const SEED: u64 = 0x0059_40A4_B002;
    const LIMIT: Duration = Duration::from_secs(120);

    /// Hands every attempt `endpoint` accepts to `waiters`, as the attempt
    /// router does for native attempts.
    fn route(endpoint: &QuicEndpoint, waiters: &Arc<NativeWaiters>) -> tokio::task::JoinHandle<()> {
        let (endpoint, waiters) = (endpoint.clone(), waiters.clone());
        tokio::spawn(async move {
            while let Some(attempt) = endpoint.accept().await {
                waiters.route(attempt);
            }
        })
    }

    /// Punches `from` sent to `to`.
    fn punches(pair: &Pair, from: SocketAddr, to: SocketAddr) -> usize {
        pair.sent()
            .iter()
            .filter(|sent| sent.from == from && sent.to == to)
            .filter(|sent| sent.bytes.len() == PUNCH_LEN && sent.bytes[0] == PUNCH_FIRST_BYTE)
            .count()
    }

    /// Both sides come up: each punched the other, each got the other's Hello
    /// with this build's version and its own address as the other sees it,
    /// and the server's waiter is gone.
    #[test]
    fn a_native_connection_comes_up_on_both_sides() {
        bounded(|| async {
            let _seed = GlobalRng::seed_guard(SEED);
            let net = crate::transport::test_net::subnet();
            let pair = pair("native-setup-up", net.addr(1), net.addr(2), None).await;
            let waiters = Arc::new(NativeWaiters::default());
            let (a, b, clock) = (pair.a.clone(), pair.b.clone(), pair.time.clone());
            let (a_addr, b_addr) = (pair.a_addr, pair.b_addr);
            let (a_key, b_key) = (pair.a_keys.public().clone(), pair.b_keys.public().clone());
            let w = waiters.clone();
            let (client, server) = drive(&pair.time, LIMIT, async move {
                let router = route(&b, &w);
                let deadline = clock.now() + Duration::from_secs(60);
                let budgets = (PunchBudget::default(), PunchBudget::default());
                let to_b = NativePeer {
                    addr: b_addr,
                    key: &b_key,
                    is_gateway: false,
                };
                let to_a = NativePeer {
                    addr: a_addr,
                    key: &a_key,
                    is_gateway: false,
                };
                let both = tokio::join!(
                    connect_native(&a, &budgets.0, to_b, 0, &clock, deadline),
                    accept_native(&b, &w, &budgets.1, to_a, 0, &clock, deadline),
                );
                router.abort();
                both
            })
            .await;
            let (client, server) = (client.expect("client"), server.expect("server"));
            let own = native_version::check(&native_version::OWN).expect("own version");
            assert_eq!(client.peer.version, own);
            assert_eq!(server.peer.version, own);
            assert_eq!(
                client.peer.hello.observed, a_addr,
                "b sees a at a's address"
            );
            assert_eq!(
                server.peer.hello.observed, b_addr,
                "a sees b at b's address"
            );
            assert_eq!(waiters.len(), 0);
            assert!(client.connection.closer().is_open());
            assert!(server.connection.closer().is_open());
            assert!(punches(&pair, a_addr, b_addr) >= 1);
            assert!(punches(&pair, b_addr, a_addr) >= 1);
        });
    }

    /// With less than MIN_WINDOW_LEFT before the deadline, neither side
    /// starts, and nothing is sent.
    #[test]
    fn a_setup_with_too_little_window_left_is_not_started() {
        bounded(|| async {
            let net = crate::transport::test_net::subnet();
            let pair = pair("native-setup-late", net.addr(1), net.addr(2), None).await;
            let waiters = Arc::new(NativeWaiters::default());
            let budget = PunchBudget::default();
            let deadline = pair.time.now() + MIN_WINDOW_LEFT - Duration::from_millis(1);
            let to_b = NativePeer {
                addr: pair.b_addr,
                key: pair.b_keys.public(),
                is_gateway: false,
            };
            let to_a = NativePeer {
                addr: pair.a_addr,
                key: pair.a_keys.public(),
                is_gateway: false,
            };
            let client = connect_native(&pair.a, &budget, to_b, 0, &pair.time, deadline).await;
            let server =
                accept_native(&pair.b, &waiters, &budget, to_a, 0, &pair.time, deadline).await;
            assert!(matches!(client, Err(NativeSetupError::TooLate)));
            assert!(matches!(server, Err(NativeSetupError::TooLate)));
            assert!(pair.sent().is_empty());
            assert_eq!(waiters.len(), 0);
        });
    }

    /// A spent punch budget stops both sides before anything is sent, and
    /// leaves no waiter and no allowed address behind.
    #[test]
    fn a_spent_budget_stops_the_setup_before_anything_is_sent() {
        bounded(|| async {
            let net = crate::transport::test_net::subnet();
            let pair = pair("native-setup-budget", net.addr(1), net.addr(2), None).await;
            let waiters = Arc::new(NativeWaiters::default());
            let budget = PunchBudget::default();
            let now = pair.time.now();
            assert!(budget.charge(pair.b_addr, PUNCH_BUDGET_BYTES, now));
            assert!(budget.charge(pair.a_addr, PUNCH_BUDGET_BYTES, now));
            let deadline = now + NATIVE_WINDOW;
            let to_b = NativePeer {
                addr: pair.b_addr,
                key: pair.b_keys.public(),
                is_gateway: false,
            };
            let to_a = NativePeer {
                addr: pair.a_addr,
                key: pair.a_keys.public(),
                is_gateway: false,
            };
            let client = connect_native(&pair.a, &budget, to_b, 0, &pair.time, deadline).await;
            let server =
                accept_native(&pair.b, &waiters, &budget, to_a, 0, &pair.time, deadline).await;
            assert!(
                matches!(client, Err(NativeSetupError::OverBudget)),
                "{client:?}"
            );
            assert!(
                matches!(server, Err(NativeSetupError::OverBudget)),
                "{server:?}"
            );
            assert!(pair.sent().is_empty());
            assert_eq!(waiters.len(), 0);
            // b did not allow a: a key of another kind is not refused.
            drop(
                pair.b
                    .allow_peer(pair.a_addr, pair.a_keys.public())
                    .expect("not held"),
            );
        });
    }

    /// A side whose peer never answers gives up at the end of its window,
    /// having punched at most PUNCHES_PER_DIAL times, and the server leaves
    /// no waiter and no allowed address behind.
    #[test]
    fn a_silent_peer_times_out_at_the_window_end() {
        bounded(|| async {
            for client_side in [true, false] {
                let _seed = GlobalRng::seed_guard(SEED);
                let net = crate::transport::test_net::subnet();
                let pair = pair("native-setup-silent", net.addr(1), net.addr(2), None).await;
                let waiters = Arc::new(NativeWaiters::default());
                let (a, b, clock) = (pair.a.clone(), pair.b.clone(), pair.time.clone());
                let (a_addr, b_addr) = (pair.a_addr, pair.b_addr);
                let (a_key, b_key) = (pair.a_keys.public().clone(), pair.b_keys.public().clone());
                let w = waiters.clone();
                let started = clock.now();
                let (result, ended) = drive(&pair.time, LIMIT, async move {
                    let router = route(&b, &w);
                    let budget = PunchBudget::default();
                    let deadline = clock.now() + Duration::from_secs(60);
                    let result = if client_side {
                        let to_b = NativePeer {
                            addr: b_addr,
                            key: &b_key,
                            is_gateway: false,
                        };
                        connect_native(&a, &budget, to_b, 0, &clock, deadline).await
                    } else {
                        let to_a = NativePeer {
                            addr: a_addr,
                            key: &a_key,
                            is_gateway: false,
                        };
                        accept_native(&b, &w, &budget, to_a, 0, &clock, deadline).await
                    };
                    router.abort();
                    (result, clock.now())
                })
                .await;
                assert!(
                    matches!(result, Err(NativeSetupError::TimedOut)),
                    "client {client_side}: {result:?}"
                );
                assert_eq!(ended - started, NATIVE_WINDOW, "client {client_side}");
                let (from, to) = if client_side {
                    (a_addr, b_addr)
                } else {
                    (b_addr, a_addr)
                };
                let sent = punches(&pair, from, to);
                assert!(
                    (1..=PUNCHES_PER_DIAL as usize).contains(&sent),
                    "client {client_side}: {sent} punches"
                );
                assert_eq!(waiters.len(), 0);
                drop(
                    pair.b
                        .allow_peer(pair.a_addr, pair.a_keys.public())
                        .expect("not held"),
                );
            }
        });
    }

    /// A second setup for an address already waiting is refused at once.
    #[test]
    fn one_server_setup_per_address() {
        bounded(|| async {
            let net = crate::transport::test_net::subnet();
            let pair = pair("native-setup-twice", net.addr(1), net.addr(2), None).await;
            let waiters = Arc::new(NativeWaiters::default());
            let _first = waiters.register(pair.a_addr).expect("first");
            let budget = PunchBudget::default();
            let to_a = NativePeer {
                addr: pair.a_addr,
                key: pair.a_keys.public(),
                is_gateway: false,
            };
            let deadline = pair.time.now() + NATIVE_WINDOW;
            let second =
                accept_native(&pair.b, &waiters, &budget, to_a, 0, &pair.time, deadline).await;
            assert!(
                matches!(second, Err(NativeSetupError::AlreadyWaiting)),
                "{second:?}"
            );
            assert!(pair.sent().is_empty());
        });
    }
}

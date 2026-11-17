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
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::future::FusedFuture;
use tokio::sync::mpsc;

use super::exchange::{HelloFailure, PeerHello, exchange};
use super::hello::Hello;
use super::punch::{CLIENT_BYTES_PER_DIAL, PUNCH_BYTES_PER_DIAL, PunchBudget, PunchRefusal, punch};
use super::waiters::{MAX_IN_FLIGHT, NativeAttempt, NativeWaiter, NativeWaiters, Slot};
use crate::simulation::TimeSource;
use crate::transport::connection_handler::native_version;
use crate::transport::crypto::TransportPublicKey;
use crate::transport::normalize_mapped_addr;
use crate::transport::quic::identity::PeerKey;
use crate::transport::quic::{QuicAllowError, QuicConnectError, QuicConnection, QuicEndpoint};

/// The setup window: punching, the handshake and the Hello exchange.
pub(crate) const NATIVE_WINDOW: Duration = Duration::from_secs(8);

/// A setup with less than this left of its window is not started.
pub(crate) const MIN_WINDOW_LEFT: Duration = Duration::from_secs(4);

/// A client whose first attempt has not finished (handshake and Hello)
/// starts a second, overlapping one (design 6.2's variant) when this much of
/// its window is left, and no sooner than [`OVERLAP_MIN_DELAY`] after the
/// first. The first is never cancelled for it: both run until one finishes
/// or the window ends. A lost Handshake packet late in the first attempt
/// leaves its next probe (backed off from a round trip inflated by the
/// setup's own losses) past the window; the second attempt's fresh handshake
/// does not wait on it. Timed from the window's END rather than its start so
/// that a server starting late (up to `MIN_WINDOW_LEFT` into the client's
/// window) is more likely listening when the second attempt begins, and a
/// client starting late, with a short window, still gets its second attempt
/// early. Chosen by the gate-15 rows (800 ms, 10% loss on punches, Initials
/// and Handshakes; 400 seeds per cell): every cell 96% or more, against
/// 72-91% for one attempt and 83% at the edges for a second attempt at a
/// fixed 2.5 s.
pub(crate) const OVERLAP_BEFORE_END: Duration = Duration::from_secs(5);

/// The least time between a client's first attempt and its overlapping
/// second ([`OVERLAP_BEFORE_END`]).
pub(crate) const OVERLAP_MIN_DELAY: Duration = Duration::from_millis(500);

/// What the server's window adds to [`NATIVE_WINDOW`]. The acceptor, the
/// server when its key is higher, starts its window when it accepts, before
/// its response has travelled back to the joiner through the relays; the
/// response carries no deadline, so the server waits this much longer for the
/// client's attempt. Provisional: A5c measures the skew.
pub(crate) const SERVER_WINDOW_ALLOWANCE: Duration = Duration::from_secs(4);

/// When each side of a CONNECT whose native setup failed starts its UDP
/// fallback: this long after its own reference point (the acceptor's
/// decision, the joiner's receipt of the response), never earlier. Both
/// native setups have ended by then, whichever role each side had, and the
/// two references are the response's travel time apart, so the two UDP hole
/// punches (each `NAT_TRAVERSAL_DEADLINE`, 3 s) overlap. A side that fell
/// back as soon as its own setup failed would punch alone: the client's
/// window ends 4 s before the server's.
pub(crate) const NATIVE_FALLBACK_AFTER: Duration =
    Duration::from_secs(NATIVE_WINDOW.as_secs() + SERVER_WINDOW_ALLOWANCE.as_secs());

/// The window `local` runs in a native setup with `peer`: [`NATIVE_WINDOW`]
/// as the client (the lower key), plus [`SERVER_WINDOW_ALLOWANCE`] as the
/// server (the higher). `None` for equal keys, which have no role and no
/// setup.
pub(crate) fn window_of(local: &TransportPublicKey, peer: &TransportPublicKey) -> Option<Duration> {
    use crate::transport::quic::upgrade::machine::Role;
    Some(match Role::of(local, peer)? {
        Role::Client => NATIVE_WINDOW,
        Role::Server => NATIVE_WINDOW + SERVER_WINDOW_ALLOWANCE,
    })
}

/// The most native setups a node runs at once, before the node's connection
/// limit lowers it (`QuicPolicy::native_setups_max`). Each holds an allow-set
/// slot for up to its window. Provisional.
pub(crate) const NATIVE_SETUPS_MAX: usize = 64;

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
    #[error("the punch budget is spent ({0:?})")]
    OverBudget(PunchRefusal),
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

/// The end of the window on `clock`: `window` from now, or the caller's
/// `deadline` if sooner, refused with less than [`MIN_WINDOW_LEFT`].
fn window_end<T: TimeSource>(
    clock: &T,
    deadline: Duration,
    window: Duration,
) -> Result<Duration, NativeSetupError> {
    let now = clock.now();
    let end = deadline.min(now + window);
    if end.saturating_sub(now) < MIN_WINDOW_LEFT {
        return Err(NativeSetupError::TooLate);
    }
    Ok(end)
}

/// This node's Hello to `peer`. The observed address is normalized, so an
/// IPv4 peer reached through an IPv4-mapped IPv6 address is encoded as IPv4
/// (family 4), as the reader's counters compare it.
pub(in crate::transport) fn own_hello(peer: SocketAddr, caps: u32) -> Hello {
    Hello {
        protoc_version: native_version::OWN,
        caps,
        observed: normalize_mapped_addr(peer),
    }
}

impl<A: NativeAttempt> NativeWaiters<A> {
    /// Wait for native attempts from `addr`. `None` when a waiter for it
    /// already exists: one native connection is set up per address at a
    /// time. Private to this module, so `accept_native` is the only way an
    /// address gets a waiter, and so the only way a native attempt reaches
    /// anything.
    fn register(self: &Arc<Self>, addr: SocketAddr) -> Option<NativeWaiter<A>> {
        let addr = normalize_mapped_addr(addr);
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel(MAX_IN_FLIGHT);
        let busy = Arc::new(AtomicUsize::new(0));
        match self.by_addr.entry(addr) {
            dashmap::mapref::entry::Entry::Occupied(_) => None,
            dashmap::mapref::entry::Entry::Vacant(vacant) => {
                vacant.insert(Slot {
                    id,
                    attempts: tx,
                    busy: busy.clone(),
                });
                Some(NativeWaiter {
                    waiters: self.clone(),
                    addr,
                    id,
                    attempts: rx,
                    busy,
                })
            }
        }
    }

    /// [`Self::register`] for a test elsewhere.
    #[cfg(test)]
    pub(in crate::transport::quic) fn register_for_test(
        self: &Arc<Self>,
        addr: SocketAddr,
    ) -> Option<NativeWaiter<A>> {
        self.register(addr)
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
///
/// The budget is charged before anything is sent, after the checks that can
/// fail without sending (the window, the peer's key). A dial that then fails
/// still spends its charge, since its punches may already have gone.
pub(in crate::transport) async fn connect_native<T: TimeSource>(
    endpoint: &QuicEndpoint,
    budget: &PunchBudget,
    peer: NativePeer<'_>,
    caps: u32,
    clock: &T,
    deadline: Duration,
) -> Result<NativeConnection, NativeSetupError> {
    let end = window_end(clock, deadline, NATIVE_WINDOW)?;
    PeerKey::of(peer.key).map_err(QuicConnectError::from)?;
    budget
        .charge(peer.addr, CLIENT_BYTES_PER_DIAL, clock.now())
        .map_err(NativeSetupError::OverBudget)?;
    // One attempt: a dial with the native ALPN, then the Hello exchange on
    // its connection. Each dial is a new connection (a new DCID).
    let attempt = || {
        futures::FutureExt::fuse(async {
            let connection = endpoint.dial_native(peer.addr, peer.key).await?;
            finish(connection, peer, caps, clock, end).await
        })
    };
    let first = attempt();
    tokio::pin!(first);
    let mut second = std::pin::pin!(futures::future::OptionFuture::from(None));
    let mut second_due = Some(overlap_at(clock.now(), end));
    let mut first_failed: Option<NativeSetupError> = None;
    loop {
        let now = clock.now();
        let left = end.saturating_sub(now);
        let until_second = second_due.map(|at| at.saturating_sub(now));
        // `biased` (attempts, overlap timer, window, punches): a fixed order
        // so a simulation's seed decides the run, and an attempt that
        // completes as the window ends still counts. The punch arm never
        // completes, so nothing starves.
        tokio::select! {
            biased;
            done = &mut first, if first_failed.is_none() => match done {
                Ok(connection) => return Ok(connection),
                // Without a second attempt running, the setup has failed.
                Err(error) if second_due.is_some() || second.is_terminated() => return Err(error),
                Err(error) => first_failed = Some(error),
            },
            Some(done) = &mut second, if !second.is_terminated() => match done {
                Ok(connection) => return Ok(connection),
                Err(error) => match first_failed.take() {
                    Some(first) => return Err(first),
                    None => tracing::debug!(addr = %peer.addr, %error, "the overlapping native attempt failed; the first continues"),
                },
            },
            () = clock.sleep_cancelable(until_second.unwrap_or_default()), if until_second.is_some() => {
                second_due = None;
                match budget.charge(peer.addr, CLIENT_BYTES_PER_DIAL, clock.now()) {
                    Ok(()) => second.set(Some(attempt()).into()),
                    Err(refusal) => tracing::debug!(addr = %peer.addr, ?refusal, "no overlapping native attempt: the punch budget is spent"),
                }
            },
            () = clock.sleep_cancelable(left) => return Err(NativeSetupError::TimedOut),
            never = punch_until(endpoint, peer.addr, clock, end) => match never {},
        }
    }
}

/// Set up a native connection from `peer` as the server: allow the peer as
/// native, punch, and take its attempts one at a time until one completes
/// its handshake, then exchange Hellos, all before the window ends
/// (`deadline`, cut to [`NATIVE_WINDOW`] + [`SERVER_WINDOW_ALLOWANCE`] from
/// now). The
/// budget is charged after the waiter and the allow, which can fail without
/// sending, and before the first punch.
pub(in crate::transport) async fn accept_native<T: TimeSource>(
    endpoint: &QuicEndpoint,
    waiters: &Arc<NativeWaiters>,
    budget: &PunchBudget,
    peer: NativePeer<'_>,
    caps: u32,
    clock: &T,
    deadline: Duration,
) -> Result<NativeConnection, NativeSetupError> {
    let end = window_end(clock, deadline, NATIVE_WINDOW + SERVER_WINDOW_ALLOWANCE)?;
    let mut waiter = waiters
        .register(peer.addr)
        .ok_or(NativeSetupError::AlreadyWaiting)?;
    // Held until the handshake ends; the connection then keeps its own.
    let _permit = endpoint.allow_native_peer(peer.addr, peer.key)?;
    budget
        .charge(peer.addr, PUNCH_BYTES_PER_DIAL, clock.now())
        .map_err(NativeSetupError::OverBudget)?;
    // Up to MAX_IN_FLIGHT attempts' handshakes run at once (a client whose
    // first attempt stalls starts a second, design 6.2); the first to
    // complete is the connection, and the others are dropped with their
    // guards.
    let accept = async {
        let mut handshakes = futures::stream::FuturesUnordered::new();
        let mut open = true;
        loop {
            tokio::select! {
                biased;
                Some((done, _in_flight)) = futures::StreamExt::next(&mut handshakes), if !handshakes.is_empty() => {
                    match done {
                        Ok(connection) => return Ok(connection),
                        Err(error) => {
                            tracing::debug!(addr = %peer.addr, %error, "A native QUIC handshake failed; waiting for the next attempt");
                        }
                    }
                }
                next = waiter.next(), if open => match next {
                    Some((attempt, in_flight)) => {
                        handshakes.push(async move { (attempt.establish().await, in_flight) });
                    }
                    // The router holds the sender while the waiter lives.
                    None => open = false,
                },
                else => return Err(NativeSetupError::TimedOut),
            }
        }
    };
    let left = end.saturating_sub(clock.now());
    // `biased`, as in `connect_native`.
    let connection = tokio::select! {
        biased;
        accepted = accept => accepted?,
        () = clock.sleep_cancelable(left) => return Err(NativeSetupError::TimedOut),
        never = punch_until(endpoint, peer.addr, clock, end) => match never {},
    };
    drop(waiter);
    finish(connection, peer, caps, clock, end).await
}

/// When a client that started at `start` with its window ending at `end`
/// starts its overlapping second attempt.
fn overlap_at(start: Duration, end: Duration) -> Duration {
    end.saturating_sub(OVERLAP_BEFORE_END)
        .max(start + OVERLAP_MIN_DELAY)
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
mod matrix_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::GlobalRng;
    use crate::transport::demux::{PUNCH_FIRST_BYTE, PUNCH_LEN};
    use crate::transport::quic::native::punch::PUNCH_BUDGET_BYTES;
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
                // A setup that finished before OVERLAP_BEFORE_END's point
                // started no second attempt, so the client was charged once:
                // three more client charges fit the destination's budget, as
                // they would with none taken (64 KiB holds four).
                let mut more = 0;
                while budgets
                    .0
                    .charge(b_addr, CLIENT_BYTES_PER_DIAL, clock.now())
                    .is_ok()
                {
                    more += 1;
                }
                assert_eq!(more, 3, "charged for one attempt");
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

    /// An IPv4-mapped peer address is encoded as IPv4 in this node's Hello;
    /// an IPv6 one stays IPv6.
    #[test]
    fn the_hello_encodes_a_mapped_peer_address_as_ipv4() {
        let mapped: SocketAddr = "[::ffff:198.51.100.1]:31337".parse().unwrap();
        let bytes = own_hello(mapped, 0).encode();
        assert_eq!(bytes[18], 4, "family");
        assert_eq!(&bytes[19..23], &[198, 51, 100, 1]);
        let decoded = Hello::decode(&bytes).unwrap();
        assert_eq!(
            decoded.observed,
            "198.51.100.1:31337".parse::<SocketAddr>().unwrap()
        );
        let v6: SocketAddr = "[2001:db8::1]:31337".parse().unwrap();
        assert_eq!(own_hello(v6, 0).encode()[18], 6);
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
            let deadline = pair.time.now() + Duration::from_millis(3999);
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
            assert_eq!(budget.charge(pair.b_addr, PUNCH_BUDGET_BYTES, now), Ok(()));
            assert_eq!(budget.charge(pair.a_addr, PUNCH_BUDGET_BYTES, now), Ok(()));
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
                matches!(
                    client,
                    Err(NativeSetupError::OverBudget(PunchRefusal::Destination))
                ),
                "{client:?}"
            );
            assert!(
                matches!(
                    server,
                    Err(NativeSetupError::OverBudget(PunchRefusal::Destination))
                ),
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
    /// having punched exactly 32 times and sent no more than it was charged,
    /// and the server leaves
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
                    let ended = clock.now();
                    // Let the dropped dial's close go out before counting.
                    clock.sleep(Duration::from_secs(2)).await;
                    router.abort();
                    (result, ended)
                })
                .await;
                assert!(
                    matches!(result, Err(NativeSetupError::TimedOut)),
                    "client {client_side}: {result:?}"
                );
                // The server waits the client's window plus its allowance.
                let window = if client_side {
                    NATIVE_WINDOW
                } else {
                    NATIVE_WINDOW + SERVER_WINDOW_ALLOWANCE
                };
                assert_eq!(ended - started, window, "client {client_side}");
                let (from, to) = if client_side {
                    (a_addr, b_addr)
                } else {
                    (b_addr, a_addr)
                };
                assert_eq!(punches(&pair, from, to), 32, "client {client_side}");
                // Everything this side sent toward the peer, punches, Initials
                // and the close included, is within what it was charged.
                let bytes: u64 = pair
                    .sent()
                    .iter()
                    .filter(|sent| sent.from == from && sent.to == to)
                    .map(|sent| sent.bytes.len() as u64)
                    .sum();
                // A silent server makes the client start its overlapping
                // second attempt, charged again.
                let charged = if client_side {
                    2 * CLIENT_BYTES_PER_DIAL
                } else {
                    PUNCH_BYTES_PER_DIAL
                };
                assert!(
                    bytes <= charged,
                    "client {client_side}: sent {bytes} B, charged {charged} B"
                );
                if client_side {
                    // The Initials are really there, so the bound is not met
                    // by a dial that sent nothing.
                    assert!(bytes > PUNCH_BYTES_PER_DIAL + 8 * 1200, "{bytes} B");
                }
                assert_eq!(waiters.len(), 0);
                drop(
                    pair.b
                        .allow_peer(pair.a_addr, pair.a_keys.public())
                        .expect("not held"),
                );
            }
        });
    }

    /// A client answered with one Retry and then nothing sends no more than
    /// it was charged: its punches, the Initials after the Retry (quinn does
    /// not reset its probe count for one) and the close.
    #[test]
    fn a_client_retried_then_ignored_sends_no_more_than_its_charge() {
        bounded(|| async {
            let _seed = GlobalRng::seed_guard(SEED);
            let net = crate::transport::test_net::subnet();
            let pair = pair("native-setup-retried", net.addr(1), net.addr(2), None).await;
            let (a, b, clock) = (pair.a.clone(), pair.b.clone(), pair.time.clone());
            let (a_addr, b_addr) = (pair.a_addr, pair.b_addr);
            let b_key = pair.b_keys.public().clone();
            let _permit = b.allow_native_peer(a_addr, pair.a_keys.public()).unwrap();
            let (result, retries) = drive(&pair.time, LIMIT, async move {
                let retries = Arc::new(std::sync::atomic::AtomicUsize::new(0));
                let counted = retries.clone();
                let router = tokio::spawn(async move {
                    while let Some(attempt) = b.accept().await {
                        if !attempt.address_validated() && counted.load(Ordering::SeqCst) == 0 {
                            counted.fetch_add(1, Ordering::SeqCst);
                            attempt.retry();
                        } else {
                            attempt.ignore();
                        }
                    }
                });
                let budget = PunchBudget::default();
                let to_b = NativePeer {
                    addr: b_addr,
                    key: &b_key,
                    is_gateway: false,
                };
                let deadline = clock.now() + Duration::from_secs(60);
                let result = connect_native(&a, &budget, to_b, 0, &clock, deadline).await;
                clock.sleep(Duration::from_secs(2)).await;
                router.abort();
                (result, retries.load(Ordering::SeqCst))
            })
            .await;
            assert!(
                matches!(result, Err(NativeSetupError::TimedOut)),
                "{result:?}"
            );
            assert_eq!(retries, 1, "the client was answered with a Retry");
            let sent: Vec<usize> = pair
                .sent()
                .iter()
                .filter(|sent| sent.from == a_addr && sent.to == b_addr)
                .map(|sent| sent.bytes.len())
                .collect();
            let padded = sent.iter().filter(|len| **len >= 1200).count();
            let bytes: u64 = sent.iter().map(|len| *len as u64).sum();
            // Two attempts: the first, retried, and the overlapping second
            // from 3 s on (OVERLAP_BEFORE_END before the 8 s window's end),
            // each charged CLIENT_BYTES_PER_DIAL.
            assert!(
                bytes <= 2 * CLIENT_BYTES_PER_DIAL,
                "{bytes} B > 2 x {CLIENT_BYTES_PER_DIAL} B"
            );
            assert_eq!(
                padded, 21,
                "the first attempt's 10 Initials and close, and the second's 9 \
                 Initials (from 3 s, ignored) and close"
            );
        });
    }

    /// The overlapping second attempt starts 5 s before the window's end (3 s
    /// into a full 8 s window), and at least 0.5 s after the first (a 4 s
    /// window, the shortest started), as literals.
    #[test]
    fn the_second_attempt_starts_five_seconds_before_the_end() {
        let s = Duration::from_secs;
        assert_eq!(overlap_at(s(10), s(18)), s(13));
        assert_eq!(overlap_at(s(10), s(14)), s(10) + Duration::from_millis(500));
        assert_eq!(overlap_at(s(10), s(15)), s(10) + Duration::from_millis(500));
        assert_eq!(overlap_at(s(10), s(16)), s(11));
    }

    /// The window is 8 s for the client and 12 s for the server, cut to the
    /// deadline, and at least 4 s, as literals.
    #[test]
    fn the_window_is_eight_seconds_and_at_least_four() {
        let clock = crate::simulation::VirtualTime::new();
        let now = clock.now();
        let far = now + Duration::from_secs(60);
        assert_eq!(
            window_end(&clock, far, NATIVE_WINDOW).unwrap() - now,
            Duration::from_secs(8)
        );
        assert_eq!(
            window_end(&clock, far, NATIVE_WINDOW + SERVER_WINDOW_ALLOWANCE).unwrap() - now,
            Duration::from_secs(12),
            "the server waits 4 s longer"
        );
        let four = now + Duration::from_secs(4);
        assert_eq!(window_end(&clock, four, NATIVE_WINDOW).unwrap(), four);
        assert!(matches!(
            window_end(&clock, four - Duration::from_millis(1), NATIVE_WINDOW),
            Err(NativeSetupError::TooLate)
        ));
    }

    /// A server setup whose allow fails takes nothing from the budget.
    #[test]
    fn a_failed_allow_takes_nothing_from_the_budget() {
        bounded(|| async {
            let net = crate::transport::test_net::subnet();
            let pair = pair("native-setup-allow", net.addr(1), net.addr(2), None).await;
            let waiters = Arc::new(NativeWaiters::default());
            let budget = PunchBudget::default();
            // The address is held for an upgrade, so a native allow conflicts.
            let _upgrade = pair
                .b
                .allow_peer(pair.a_addr, pair.a_keys.public())
                .unwrap();
            let to_a = NativePeer {
                addr: pair.a_addr,
                key: pair.a_keys.public(),
                is_gateway: false,
            };
            let deadline = pair.time.now() + NATIVE_WINDOW;
            let result =
                accept_native(&pair.b, &waiters, &budget, to_a, 0, &pair.time, deadline).await;
            assert!(
                matches!(
                    result,
                    Err(NativeSetupError::Allow(QuicAllowError::KeyConflict))
                ),
                "{result:?}"
            );
            assert_eq!(waiters.len(), 0);
            assert!(pair.sent().is_empty());
            let now = pair.time.now();
            assert_eq!(budget.charge(pair.a_addr, PUNCH_BUDGET_BYTES, now), Ok(()));
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

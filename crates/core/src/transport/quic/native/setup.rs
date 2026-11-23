//! Setting up a native connection (#5940, design 2.4): punching, the QUIC
//! handshake with the native ALPN, and the Hello exchange, all inside one
//! setup window.
//!
//! Roles follow key order and are the caller's decision: the server calls
//! [`accept_native`], the client [`connect_native`]. Both punch toward the
//! other every [`PUNCH_INTERVAL`](super::punch::PUNCH_INTERVAL) until the
//! connection is up or the window ends. The client dials once and, if that
//! attempt (dial and Hello) has not finished [`OVERLAP_BEFORE_END`] before
//! the window's end, dials again beside it (overlapping attempts, design
//! 6.2). The server takes up to two attempts at a time ([`NativeWaiters`])
//! and the first handshake to complete wins.
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
/// or the window ends. A first attempt that ERRORS before that point (a hard
/// refusal, not a loss, which shows as a timeout) ends the setup with its
/// error and starts no second, so no second charge is taken. The offset is
/// fixed, not jittered: it is a single hedge, not a retry loop. A lost
/// Handshake packet late in the first attempt
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
    let attempt = || async {
        let connection = endpoint.dial_native(peer.addr, peer.key).await?;
        finish(connection, peer, caps, clock, end).await
    };
    // `biased`: the race first, as the punch arm never completes.
    tokio::select! {
        biased;
        done = race_attempts(attempt, budget, peer.addr, clock, end) => done,
        never = punch_until(endpoint, peer.addr, clock, end) => match never {},
    }
}

/// Join the gateway `peer` with a native connection (#5940, phase B): the
/// client's setup ([`connect_native`]) whatever the key order, since a
/// gateway admits a joiner it was never told about only as the server
/// (`quic::admission`), and without punching, since a gateway is reachable at
/// its address and drops a punch from an unknown source. The overlapping
/// second attempt is kept; a gateway admits one attempt per source address
/// per second, and the second starts later than that. The Hello exchange
/// treats `peer` as a gateway whatever `peer.is_gateway` says, so a refusal
/// of this node's version sets the version-mismatch signal as on UDP.
pub(in crate::transport) async fn join_native<T: TimeSource>(
    endpoint: &QuicEndpoint,
    budget: &PunchBudget,
    peer: NativePeer<'_>,
    caps: u32,
    clock: &T,
    deadline: Duration,
) -> Result<NativeConnection, NativeSetupError> {
    let peer = NativePeer {
        is_gateway: true,
        ..peer
    };
    let end = window_end(clock, deadline, NATIVE_WINDOW)?;
    PeerKey::of(peer.key).map_err(QuicConnectError::from)?;
    // The dial's Initials still go to the destination, so they are charged
    // as a client's are.
    budget
        .charge(peer.addr, CLIENT_BYTES_PER_DIAL, clock.now())
        .map_err(NativeSetupError::OverBudget)?;
    let attempt = || async {
        let connection = endpoint.dial_native(peer.addr, peer.key).await?;
        finish(connection, peer, caps, clock, end).await
    };
    race_attempts(attempt, budget, peer.addr, clock, end).await
}

/// The client's attempts: `attempt` runs at once, and a second `attempt`
/// starts at [`overlap_at`] if the first has not finished, charged to
/// `budget` for `addr`. The first to succeed is returned and the other is
/// dropped (closing its connection). A first attempt that ERRORS before the
/// overlap timer ends the setup with that error and starts no second: a hard
/// error is a refusal, a second dial would be refused the same way and
/// charged; loss shows as a timeout, which the overlap covers. After the
/// second has started, the first's error waits for the second's outcome.
async fn race_attempts<T, C, F, Fut>(
    attempt: F,
    budget: &PunchBudget,
    addr: SocketAddr,
    clock: &T,
    end: Duration,
) -> Result<C, NativeSetupError>
where
    T: TimeSource,
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<C, NativeSetupError>>,
{
    use futures::FutureExt;
    let first = attempt().fuse();
    tokio::pin!(first);
    let mut second = std::pin::pin!(futures::future::OptionFuture::from(None));
    let mut second_due = Some(overlap_at(clock.now(), end));
    let mut first_failed: Option<NativeSetupError> = None;
    loop {
        let now = clock.now();
        let left = end.saturating_sub(now);
        let until_second = second_due.map(|at| at.saturating_sub(now));
        // `biased` (attempts, overlap timer, window): a fixed order so a
        // simulation's seed decides the run, and an attempt that completes
        // as the window ends still counts.
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
                    None => tracing::debug!(%addr, %error, "the overlapping native attempt failed; the first continues"),
                },
            },
            () = clock.sleep_cancelable(until_second.unwrap_or_default()), if until_second.is_some() => {
                second_due = None;
                match budget.charge(addr, CLIENT_BYTES_PER_DIAL, clock.now()) {
                    Ok(()) => second.set(Some(attempt().fuse()).into()),
                    Err(refusal) => tracing::debug!(%addr, ?refusal, "no overlapping native attempt: the punch budget is spent"),
                }
            },
            () = clock.sleep_cancelable(left) => return Err(NativeSetupError::TimedOut),
        }
    }
}

/// Set up a native connection from `peer` as the server: allow the peer as
/// native, punch, and run up to [`MAX_IN_FLIGHT`] (2) attempts' handshakes at
/// once (a client's first and its overlapping second); the first to complete
/// its handshake is the connection and the other is dropped. Then exchange
/// Hellos, all before the window ends
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
            // `biased`: finished handshakes first, then new attempts, so a
            // completed connection is taken before more work is queued. The
            // new-attempt arm cannot starve: `handshakes` never holds more
            // than MAX_IN_FLIGHT (2) entries (the router hands over no more
            // until a guard drops), so the first arm runs at most twice
            // before it has nothing ready, and an attempt that completes
            // while another waits still wins.
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

    /// A budget with one client charge taken, as `connect_native` has taken
    /// before its first attempt, and how many more fit.
    fn charged_once(addr: SocketAddr, now: Duration) -> PunchBudget {
        let budget = PunchBudget::default();
        assert_eq!(budget.charge(addr, CLIENT_BYTES_PER_DIAL, now), Ok(()));
        budget
    }

    fn charges_left(budget: &PunchBudget, addr: SocketAddr, now: Duration) -> usize {
        let mut more = 0;
        while budget.charge(addr, CLIENT_BYTES_PER_DIAL, now).is_ok() {
            more += 1;
        }
        more
    }

    /// A first attempt that fails with an error before the overlap timer is
    /// the setup's error: no second attempt starts and none is charged.
    #[test]
    fn a_first_attempt_that_errors_early_ends_the_setup_uncharged() {
        bounded(|| async {
            let net = crate::transport::test_net::subnet();
            let pair = pair("native-race-error", net.addr(1), net.addr(2), None).await;
            let clock = pair.time.clone();
            let addr = pair.b_addr;
            let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let c = calls.clone();
            let budget = charged_once(addr, clock.now());
            let end = clock.now() + NATIVE_WINDOW;
            let (clock2, c2) = (clock.clone(), clock.clone());
            let result = drive(&pair.time, LIMIT, async move {
                let attempt = || {
                    c.fetch_add(1, Ordering::SeqCst);
                    let clock = clock2.clone();
                    async move {
                        // Fails well before the overlap point (3 s).
                        clock.sleep(Duration::from_millis(200)).await;
                        Err::<(), _>(NativeSetupError::AlreadyWaiting)
                    }
                };
                let result = race_attempts(attempt, &budget, addr, &c2, end).await;
                (result, charges_left(&budget, addr, c2.now()))
            })
            .await;
            assert!(matches!(result.0, Err(NativeSetupError::AlreadyWaiting)));
            assert_eq!(calls.load(Ordering::SeqCst), 1, "no second attempt");
            assert_eq!(result.1, 3, "no second charge");
        });
    }

    /// Runs `race_attempts` against a script: attempt `i` (0 first, 1
    /// second) finishes after `plan[i].0` ms from its own start, with its
    /// index if `plan[i].1` and an error otherwise (AlreadyWaiting for the
    /// first, TooLate for the second). `spent` further charges are taken
    /// first. Returns the result, attempts started, time taken and charges
    /// still available.
    async fn run_race(
        name: &'static str,
        plan: [(u64, bool); 2],
        spent: usize,
    ) -> (Result<usize, NativeSetupError>, usize, Duration, usize) {
        let net = crate::transport::test_net::subnet();
        let pair = pair(name, net.addr(1), net.addr(2), None).await;
        let clock = pair.time.clone();
        let addr = pair.b_addr;
        let start = clock.now();
        let budget = charged_once(addr, start);
        for _ in 0..spent {
            assert_eq!(budget.charge(addr, CLIENT_BYTES_PER_DIAL, start), Ok(()));
        }
        let end = start + NATIVE_WINDOW;
        let calls = Arc::new(AtomicUsize::new(0));
        let (c, clk) = (calls.clone(), clock.clone());
        let (result, took, left) = drive(&pair.time, LIMIT, async move {
            let attempt = || {
                let index = c.fetch_add(1, Ordering::SeqCst);
                let (ms, ok) = plan[index];
                let clock = clk.clone();
                async move {
                    clock.sleep(Duration::from_millis(ms)).await;
                    match (ok, index) {
                        (true, i) => Ok(i),
                        (false, 0) => Err(NativeSetupError::AlreadyWaiting),
                        (false, _) => Err(NativeSetupError::TooLate),
                    }
                }
            };
            let result = race_attempts(attempt, &budget, addr, &clk, end).await;
            let took = clk.now() - start;
            (result, took, charges_left(&budget, addr, clk.now()))
        })
        .await;
        (result, calls.load(Ordering::SeqCst), took, left)
    }

    /// The overlap point is 3 s into the 8 s window (5 s before its end).
    /// A second attempt the punch budget refuses is not started and not
    /// charged; the first continues alone and can still succeed.
    #[test]
    fn a_refused_second_attempt_leaves_the_first_running() {
        bounded(|| async {
            // Four charges (64 KiB) spent: the refill by 3 s does not cover
            // a fifth.
            let (result, calls, _, _) =
                run_race("native-race-refused", [(5_000, true), (0, true)], 3).await;
            assert_eq!(result.expect("the first succeeds"), 0);
            assert_eq!(calls, 1, "no second attempt");
        });
    }

    /// The first fails after the second started: its error waits for the
    /// second's outcome. If the second succeeds it wins.
    #[test]
    fn a_first_failure_after_the_second_started_waits_for_the_second() {
        bounded(|| async {
            let (result, calls, took, _) = run_race(
                "native-race-first-fails-second-wins",
                [(4_000, false), (3_000, true)],
                0,
            )
            .await;
            assert_eq!(result.expect("the second succeeds"), 1);
            assert_eq!(calls, 2);
            assert_eq!(took, Duration::from_secs(6), "waited for the second");
        });
    }

    /// The first fails after the second started and the second fails too:
    /// the first's error is the setup's, at the second's end.
    #[test]
    fn both_attempts_failing_returns_the_first_error() {
        bounded(|| async {
            let (result, calls, took, _) =
                run_race("native-race-both-fail", [(4_000, false), (2_000, false)], 0).await;
            assert!(
                matches!(result, Err(NativeSetupError::AlreadyWaiting)),
                "{result:?}"
            );
            assert_eq!(calls, 2);
            assert_eq!(took, Duration::from_secs(5));
        });
    }

    /// The second fails while the first runs: the first continues and wins.
    #[test]
    fn a_failed_second_attempt_leaves_the_first_running() {
        bounded(|| async {
            let (result, calls, took, _) = run_race(
                "native-race-second-fails",
                [(6_000, true), (1_000, false)],
                0,
            )
            .await;
            assert_eq!(result.expect("the first succeeds"), 0);
            assert_eq!(calls, 2);
            assert_eq!(took, Duration::from_secs(6));
        });
    }

    /// Both attempts still pending when the window ends: TimedOut, at the
    /// window's end, both charged.
    #[test]
    fn both_attempts_pending_at_the_window_end_time_out() {
        bounded(|| async {
            let (result, calls, took, left) =
                run_race("native-race-timeout", [(60_000, true), (60_000, true)], 0).await;
            assert!(
                matches!(result, Err(NativeSetupError::TimedOut)),
                "{result:?}"
            );
            assert_eq!(calls, 2);
            assert_eq!(took, NATIVE_WINDOW);
            // The destination's budget refills about 8.7 KB over the window, so
            // three more fit after two charges (as in the silent-peer test);
            // `calls` is what shows the second was started and charged.
            assert_eq!(left, 3);
        });
    }

    /// A first attempt that succeeds exactly at the overlap point wins
    /// before the second starts: no second attempt, no second charge.
    #[test]
    fn a_first_success_at_the_overlap_point_starts_no_second() {
        bounded(|| async {
            let (result, calls, took, left) =
                run_race("native-race-at-overlap", [(3_000, true), (0, true)], 0).await;
            assert_eq!(result.expect("the first succeeds"), 0);
            assert_eq!(calls, 1);
            assert_eq!(took, Duration::from_secs(3));
            assert_eq!(left, 3, "charged once");
        });
    }

    /// Counts the probes alive: one per attempt started and not dropped.
    struct Probe(Arc<std::sync::atomic::AtomicUsize>);
    impl Probe {
        fn new(live: &Arc<std::sync::atomic::AtomicUsize>) -> Self {
            live.fetch_add(1, Ordering::SeqCst);
            Probe(live.clone())
        }
    }
    impl Drop for Probe {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    /// Both attempts complete at the same instant: one connection comes
    /// back and the other is dropped (not left alive), and both were charged.
    #[test]
    fn when_both_attempts_complete_one_is_returned_and_the_other_dropped() {
        bounded(|| async {
            let net = crate::transport::test_net::subnet();
            let pair = pair("native-race-both", net.addr(1), net.addr(2), None).await;
            let clock = pair.time.clone();
            let addr = pair.b_addr;
            let live = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let budget = charged_once(addr, clock.now());
            let start = clock.now();
            let end = start + NATIVE_WINDOW;
            // The second starts at end - 5 s = start + 3 s; both finish at
            // start + 4 s.
            let finish_at = start + Duration::from_secs(4);
            let (l, c, clk) = (live.clone(), calls.clone(), clock.clone());
            let (returned, alive_after_return, left) = drive(&pair.time, LIMIT, async move {
                let attempt = || {
                    c.fetch_add(1, Ordering::SeqCst);
                    let (clock, live) = (clk.clone(), l.clone());
                    // Alive from the attempt's start, as its connection is.
                    let probe = Probe::new(&live);
                    async move {
                        let wait = finish_at.saturating_sub(clock.now());
                        clock.sleep(wait).await;
                        Ok::<_, NativeSetupError>(probe)
                    }
                };
                let won = race_attempts(attempt, &budget, addr, &clk, end).await;
                let left = charges_left(&budget, addr, clk.now());
                (won.is_ok(), l.load(Ordering::SeqCst), left)
            })
            .await;
            assert!(returned);
            assert_eq!(calls.load(Ordering::SeqCst), 2, "both attempts ran");
            assert_eq!(alive_after_return, 1, "exactly one result survives");
            assert_eq!(live.load(Ordering::SeqCst), 0, "the winner dropped too");
            assert_eq!(left, 2, "both attempts charged");
        });
    }

    /// Two real attempts from the same client reach one server waiter and
    /// both handshakes run; the first to complete is the connection. When
    /// `accept_native` returns, the in-flight count is back at 0 and the
    /// waiter is gone.
    #[test]
    fn two_attempts_at_the_server_leave_no_count() {
        bounded(|| async {
            let _seed = GlobalRng::seed_guard(SEED);
            let net = crate::transport::test_net::subnet();
            let pair = pair("native-two-attempts", net.addr(1), net.addr(2), None).await;
            let waiters = Arc::new(NativeWaiters::default());
            let (a, b, clock) = (pair.a.clone(), pair.b.clone(), pair.time.clone());
            let (a_addr, b_addr) = (pair.a_addr, pair.b_addr);
            let (a_key, b_key) = (pair.a_keys.public().clone(), pair.b_keys.public().clone());
            let w = waiters.clone();
            let (accepted, busy, queued) = drive(&pair.time, LIMIT, async move {
                let router = route(&b, &w);
                let deadline = clock.now() + Duration::from_secs(60);
                let server = tokio::spawn({
                    let (b, w, clock) = (b.clone(), w.clone(), clock.clone());
                    let a_key = a_key.clone();
                    async move {
                        let to_a = NativePeer {
                            addr: a_addr,
                            key: &a_key,
                            is_gateway: false,
                        };
                        accept_native(&b, &w, &PunchBudget::default(), to_a, 0, &clock, deadline)
                            .await
                            .map(|c| c.connection.closer().is_open())
                    }
                });
                // Wait for the waiter, then dial twice at once.
                while w.len() == 0 {
                    clock.sleep(Duration::from_millis(10)).await;
                }
                let busy = w.by_addr.get(&a_addr).map(|slot| slot.busy.clone());
                let dials: Vec<_> = (0..2)
                    .map(|_| {
                        let (a, b_key, clock) = (a.clone(), b_key.clone(), clock.clone());
                        tokio::spawn(async move {
                            let to_b = NativePeer {
                                addr: b_addr,
                                key: &b_key,
                                is_gateway: false,
                            };
                            let connection = a.dial_native(b_addr, &b_key).await?;
                            let end = clock.now() + NATIVE_WINDOW;
                            finish(connection, to_b, 0, &clock, end).await
                        })
                    })
                    .collect();
                let accepted = server.await.expect("server task");
                for dial in dials {
                    dial.abort();
                }
                router.abort();
                let busy = busy.expect("the waiter's count").load(Ordering::Acquire);
                (accepted, busy, w.len())
            })
            .await;
            assert!(accepted.expect("one connection"));
            assert_eq!(busy, 0, "no attempt left counted in flight");
            assert_eq!(queued, 0, "the waiter is gone");
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
                    // The client's overlapping second attempt was charged too.
                    // The destination's 64 KiB refills about 8.7 KB over the
                    // 8 s window, so three more client charges fit after two
                    // were taken, and four after one.
                    if client_side {
                        let mut more = 0;
                        while budget
                            .charge(b_addr, CLIENT_BYTES_PER_DIAL, clock.now())
                            .is_ok()
                        {
                            more += 1;
                        }
                        assert_eq!(more, 3, "charged for two attempts");
                    }
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

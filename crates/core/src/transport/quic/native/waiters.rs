//! The attempt router's lookup of native waiters (#5940, design 2.4).
//!
//! A native server allows its peer with
//! [`QuicEndpoint::allow_native_peer`](crate::transport::quic::QuicEndpoint::allow_native_peer)
//! and registers here for the peer's address. The attempt router hands each
//! native attempt from that address to the waiter, which is the only way a
//! native attempt reaches anything. An address gets an entry only from
//! [`NativeWaiters::register`], and the native accept is its only caller.
//!
//! At most one attempt per waiter is in flight: while the waiter is
//! completing one handshake the router ignores the next attempts, and the
//! channel holds at most one more. An attempt whose sender has not shown it
//! reads datagrams sent to its address is answered with a Retry first, as an
//! upgrade attempt is: one more round trip inside the 8 s window.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use dashmap::DashMap;
use tokio::sync::mpsc;

use crate::transport::quic::QuicIncoming;

/// What the router did with a native attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NativeRoute {
    /// Handed to the waiter for its address.
    Handed,
    /// Answered with a Retry: the sender had not validated its address.
    Retried,
    /// No waiter for the address: ignored.
    NoWaiter,
    /// The waiter is completing another attempt, or one is already queued
    /// for it: ignored.
    Busy,
}

/// The native waiters of one endpoint, by address (normalized as the
/// allow-set normalizes it).
#[derive(Default)]
pub(crate) struct NativeWaiters {
    by_addr: DashMap<SocketAddr, Slot>,
    next_id: AtomicU64,
}

struct Slot {
    id: u64,
    attempts: mpsc::Sender<QuicIncoming>,
    /// Set while the waiter completes a handshake.
    busy: Arc<AtomicBool>,
}

/// One native server's place in [`NativeWaiters`]. Dropping it removes the
/// entry, and only its own.
pub(crate) struct NativeWaiter {
    waiters: Arc<NativeWaiters>,
    addr: SocketAddr,
    id: u64,
    attempts: mpsc::Receiver<QuicIncoming>,
    busy: Arc<AtomicBool>,
}

/// Marks the waiter busy while alive.
pub(crate) struct InFlight(Arc<AtomicBool>);

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl NativeWaiters {
    /// Wait for native attempts from `addr`. `None` when a waiter for it
    /// already exists: one native connection is set up per address at a
    /// time.
    pub(crate) fn register(self: &Arc<Self>, addr: SocketAddr) -> Option<NativeWaiter> {
        let addr = crate::transport::normalize_mapped_addr(addr);
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel(1);
        let busy = Arc::new(AtomicBool::new(false));
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

    /// Hand a native attempt to the waiter for its address, without
    /// waiting. Anything not handed is ignored or answered with a Retry, so
    /// the endpoint keeps no state for it.
    pub(crate) fn route(&self, incoming: QuicIncoming) -> NativeRoute {
        let addr = incoming.source();
        let Some(slot) = self.by_addr.get(&addr) else {
            incoming.ignore();
            return NativeRoute::NoWaiter;
        };
        if slot.busy.load(Ordering::Acquire) {
            incoming.ignore();
            return NativeRoute::Busy;
        }
        if !incoming.address_validated() {
            incoming.retry();
            return NativeRoute::Retried;
        }
        match slot.attempts.try_send(incoming) {
            Ok(()) => NativeRoute::Handed,
            Err(
                mpsc::error::TrySendError::Full(incoming)
                | mpsc::error::TrySendError::Closed(incoming),
            ) => {
                incoming.ignore();
                NativeRoute::Busy
            }
        }
    }

    /// The number of addresses with a waiter.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.by_addr.len()
    }
}

impl NativeWaiter {
    /// The next attempt handed to this waiter. Cancel-safe.
    pub(crate) async fn next(&mut self) -> Option<QuicIncoming> {
        self.attempts.recv().await
    }

    /// Mark the waiter busy until the returned guard drops, so the router
    /// ignores further attempts while one handshake runs.
    pub(crate) fn in_flight(&self) -> InFlight {
        self.busy.store(true, Ordering::Release);
        InFlight(self.busy.clone())
    }
}

impl Drop for NativeWaiter {
    fn drop(&mut self) {
        self.waiters
            .by_addr
            .remove_if(&self.addr, |_, slot| slot.id == self.id);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::config::GlobalRng;
    use crate::transport::quic::tests::{bounded, drive, pair};

    const SEED: u64 = 0x0059_40A4_B001;
    const LIMIT: Duration = Duration::from_secs(30);

    /// A native dial reaches the waiter for its address only after a Retry,
    /// and the handshake completes on the attempt the waiter was handed.
    #[test]
    fn a_native_attempt_is_retried_then_handed_to_its_waiter() {
        bounded(|| async {
            let _seed = GlobalRng::seed_guard(SEED);
            let net = crate::transport::test_net::subnet();
            let pair = pair("native-waiter-handed", net.addr(1), net.addr(2), None).await;
            let waiters = Arc::new(NativeWaiters::default());
            let _permit = pair
                .b
                .allow_native_peer(pair.a_addr, pair.a_keys.public())
                .expect("room");
            let mut waiter = waiters.register(pair.a_addr).expect("first waiter");
            let (a, b) = (pair.a.clone(), pair.b.clone());
            let (b_addr, b_key) = (pair.b_addr, pair.b_keys.public().clone());
            let router = waiters.clone();
            let (client, (routes, server)) = drive(&pair.time, LIMIT, async move {
                tokio::join!(a.dial_native(b_addr, &b_key), async {
                    let mut routes = Vec::new();
                    while routes.last() != Some(&NativeRoute::Handed) {
                        routes.push(router.route(b.accept().await.expect("an attempt")));
                    }
                    let attempt = waiter.next().await.expect("handed");
                    let _in_flight = waiter.in_flight();
                    (routes, attempt.establish().await)
                })
            })
            .await;
            assert_eq!(routes, [NativeRoute::Retried, NativeRoute::Handed]);
            client.expect("client");
            server.expect("server");
        });
    }

    /// A native attempt from an address with no waiter is ignored, and an
    /// attempt while the waiter is busy is ignored too.
    #[test]
    fn a_native_attempt_with_no_free_waiter_is_ignored() {
        bounded(|| async {
            for registered in [false, true] {
                let _seed = GlobalRng::seed_guard(SEED);
                let net = crate::transport::test_net::subnet();
                let pair = pair("native-waiter-ignored", net.addr(1), net.addr(2), None).await;
                let waiters = Arc::new(NativeWaiters::default());
                let _permit = pair
                    .b
                    .allow_native_peer(pair.a_addr, pair.a_keys.public())
                    .expect("room");
                let waiter = registered.then(|| waiters.register(pair.a_addr).expect("waiter"));
                let _busy = waiter.as_ref().map(NativeWaiter::in_flight);
                let (a, b) = (pair.a.clone(), pair.b.clone());
                let (b_addr, b_key) = (pair.b_addr, pair.b_keys.public().clone());
                let router = waiters.clone();
                let route = drive(&pair.time, LIMIT, async move {
                    let dial = tokio::spawn(async move { a.dial_native(b_addr, &b_key).await });
                    let route = router.route(b.accept().await.expect("an attempt"));
                    dial.abort();
                    route
                })
                .await;
                let expected = if registered {
                    NativeRoute::Busy
                } else {
                    NativeRoute::NoWaiter
                };
                assert_eq!(route, expected);
            }
        });
    }

    /// One waiter per address; dropping it frees the address, and a stale
    /// waiter's drop does not remove a newer one.
    #[test]
    fn one_waiter_per_address_and_drop_removes_only_its_own() {
        let waiters = Arc::new(NativeWaiters::default());
        let addr: SocketAddr = "10.0.0.1:5000".parse().unwrap();
        let first = waiters.register(addr).expect("first");
        assert!(waiters.register(addr).is_none(), "one at a time");
        // The IPv4-mapped form is the same address.
        let mapped: SocketAddr = "[::ffff:10.0.0.1]:5000".parse().unwrap();
        assert!(waiters.register(mapped).is_none(), "normalized");
        drop(first);
        assert_eq!(waiters.len(), 0);
        let second = waiters.register(addr).expect("free again");
        // A slot for another id is left alone by a drop of a stale waiter.
        let stale = NativeWaiter {
            waiters: waiters.clone(),
            addr,
            id: second.id + 1,
            attempts: mpsc::channel(1).1,
            busy: Arc::default(),
        };
        drop(stale);
        assert_eq!(waiters.len(), 1);
        drop(second);
        assert_eq!(waiters.len(), 0);
    }
}

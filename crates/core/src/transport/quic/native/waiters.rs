//! The attempt router's lookup of native waiters (#5940, design 2.4).
//!
//! A native server allows its peer as native and registers here for the
//! peer's address; both happen in `setup::accept_native`, and registering is
//! private to `setup`. The attempt router hands each native attempt from that
//! address to the waiter, which is the only way a native attempt reaches
//! anything.
//!
//! At most one attempt per waiter is in flight: handing an attempt over marks
//! the waiter busy, and the router ignores further attempts until the waiter
//! has finished with it (its [`InFlight`] guard drops). An attempt whose
//! sender has not shown it reads datagrams sent to its address is answered
//! with a Retry first, as an upgrade attempt is: one more round trip inside
//! the 8 s window.

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use dashmap::DashMap;
use tokio::sync::mpsc;

use crate::transport::quic::QuicIncoming;

/// What the router needs of an attempt; [`QuicIncoming`] in production, a
/// stand-in in tests.
pub(crate) trait NativeAttempt: Send + 'static {
    /// The address it came from, normalized as the allow-set does.
    fn source(&self) -> SocketAddr;
    /// Whether it returned a Retry token for its address.
    fn address_validated(&self) -> bool;
    /// Drop it without an answer.
    fn ignore(self);
    /// Answer it with a Retry and drop it.
    fn retry(self);
}

impl NativeAttempt for QuicIncoming {
    fn source(&self) -> SocketAddr {
        QuicIncoming::source(self)
    }

    fn address_validated(&self) -> bool {
        QuicIncoming::address_validated(self)
    }

    fn ignore(self) {
        QuicIncoming::ignore(self)
    }

    fn retry(self) {
        QuicIncoming::retry(self)
    }
}

/// What the router did with a native attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NativeRoute {
    /// Handed to the waiter for its address.
    Handed,
    /// Answered with a Retry: the sender had not validated its address.
    Retried,
    /// No waiter for the address: ignored.
    NoWaiter,
    /// The waiter has an attempt it has not finished with: ignored.
    Busy,
}

/// The native waiters of one endpoint, by address (normalized as the
/// allow-set normalizes it).
pub(crate) struct NativeWaiters<A = QuicIncoming> {
    pub(super) by_addr: DashMap<SocketAddr, Slot<A>>,
    pub(super) next_id: AtomicU64,
}

impl<A> Default for NativeWaiters<A> {
    fn default() -> Self {
        Self {
            by_addr: DashMap::new(),
            next_id: AtomicU64::new(0),
        }
    }
}

pub(super) struct Slot<A> {
    pub(super) id: u64,
    pub(super) attempts: mpsc::Sender<A>,
    /// Set when an attempt is handed over, cleared when the waiter has
    /// finished with it.
    pub(super) busy: Arc<AtomicBool>,
}

/// One native server's place in [`NativeWaiters`]. Dropping it removes the
/// entry, and only its own.
pub(crate) struct NativeWaiter<A: NativeAttempt = QuicIncoming> {
    pub(super) waiters: Arc<NativeWaiters<A>>,
    pub(super) addr: SocketAddr,
    pub(super) id: u64,
    pub(super) attempts: mpsc::Receiver<A>,
    pub(super) busy: Arc<AtomicBool>,
}

/// Keeps the waiter busy while alive; dropping it lets the router hand over
/// the next attempt.
pub(crate) struct InFlight(Arc<AtomicBool>);

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl<A: NativeAttempt> NativeWaiters<A> {
    /// Hand a native attempt to the waiter for its address, without
    /// waiting. Anything not handed is ignored or answered with a Retry, so
    /// the endpoint keeps no state for it. Called from the router task only.
    pub(crate) fn route(&self, attempt: A) -> NativeRoute {
        let addr = attempt.source();
        let Some(slot) = self.by_addr.get(&addr) else {
            attempt.ignore();
            return NativeRoute::NoWaiter;
        };
        if slot.busy.load(Ordering::Acquire) {
            attempt.ignore();
            return NativeRoute::Busy;
        }
        if !attempt.address_validated() {
            attempt.retry();
            return NativeRoute::Retried;
        }
        // Busy from here, before the waiter can take it, so the router hands
        // over nothing more until the waiter's guard drops.
        slot.busy.store(true, Ordering::Release);
        match slot.attempts.try_send(attempt) {
            Ok(()) => NativeRoute::Handed,
            Err(mpsc::error::TrySendError::Full(attempt))
            | Err(mpsc::error::TrySendError::Closed(attempt)) => {
                attempt.ignore();
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

impl<A: NativeAttempt> NativeWaiter<A> {
    /// The next attempt handed to this waiter, with the guard that keeps the
    /// waiter busy until the caller has finished with it. Cancel-safe.
    pub(crate) async fn next(&mut self) -> Option<(A, InFlight)> {
        let attempt = self.attempts.recv().await?;
        Some((attempt, InFlight(self.busy.clone())))
    }
}

impl<A: NativeAttempt> Drop for NativeWaiter<A> {
    fn drop(&mut self) {
        self.waiters
            .by_addr
            .remove_if(&self.addr, |_, slot| slot.id == self.id);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::Duration;

    use super::*;
    use crate::config::GlobalRng;
    use crate::transport::quic::tests::{bounded, drive, pair};

    const SEED: u64 = 0x0059_40A4_B001;
    const LIMIT: Duration = Duration::from_secs(30);

    /// A stand-in attempt that records what the router did with it.
    struct Fake {
        source: SocketAddr,
        validated: bool,
        log: Arc<Mutex<Vec<&'static str>>>,
    }

    impl NativeAttempt for Fake {
        fn source(&self) -> SocketAddr {
            self.source
        }
        fn address_validated(&self) -> bool {
            self.validated
        }
        fn ignore(self) {
            self.log.lock().unwrap().push("ignored");
        }
        fn retry(self) {
            self.log.lock().unwrap().push("retried");
        }
    }

    fn addr() -> SocketAddr {
        "10.0.0.1:5000".parse().unwrap()
    }

    fn fake(validated: bool, log: &Arc<Mutex<Vec<&'static str>>>) -> Fake {
        Fake {
            source: addr(),
            validated,
            log: log.clone(),
        }
    }

    /// Every branch of the routing decision: no waiter, a Retry, a hand-off,
    /// busy until the waiter's guard drops, a full channel and a closed one
    /// (both ignored), each attempt ignored or retried exactly once.
    #[test]
    fn the_router_takes_each_branch() {
        bounded(|| async {
            let log = Arc::new(Mutex::new(Vec::new()));
            let waiters = Arc::new(NativeWaiters::<Fake>::default());
            assert_eq!(waiters.route(fake(true, &log)), NativeRoute::NoWaiter);
            let mut waiter = waiters.register_for_test(addr()).expect("waiter");
            assert_eq!(waiters.route(fake(false, &log)), NativeRoute::Retried);
            assert_eq!(waiters.route(fake(true, &log)), NativeRoute::Handed);
            // Busy from the hand-off, before the waiter takes the attempt.
            assert_eq!(waiters.route(fake(true, &log)), NativeRoute::Busy);
            let (_attempt, in_flight) = waiter.next().await.expect("handed");
            assert_eq!(waiters.route(fake(true, &log)), NativeRoute::Busy);
            drop(in_flight);
            assert_eq!(waiters.route(fake(true, &log)), NativeRoute::Handed);
            // The guard of the one queued is not taken yet; clear busy as a
            // finished attempt would, so the next meets a full channel.
            waiter.busy.store(false, Ordering::Release);
            assert_eq!(waiters.route(fake(true, &log)), NativeRoute::Busy, "full");
            waiter.busy.store(false, Ordering::Release);
            waiter.attempts.close();
            while waiter.attempts.try_recv().is_ok() {}
            assert_eq!(waiters.route(fake(true, &log)), NativeRoute::Busy, "closed");
            assert_eq!(
                *log.lock().unwrap(),
                [
                    "ignored", "retried", "ignored", "ignored", "ignored", "ignored"
                ]
            );
        });
    }

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
            let mut waiter = waiters
                .register_for_test(pair.a_addr)
                .expect("first waiter");
            let (a, b) = (pair.a.clone(), pair.b.clone());
            let (b_addr, b_key) = (pair.b_addr, pair.b_keys.public().clone());
            let router = waiters.clone();
            let (client, (routes, server)) = drive(&pair.time, LIMIT, async move {
                tokio::join!(a.dial_native(b_addr, &b_key), async {
                    let mut routes = Vec::new();
                    while routes.last() != Some(&NativeRoute::Handed) {
                        routes.push(router.route(b.accept().await.expect("an attempt")));
                    }
                    let (attempt, _in_flight) = waiter.next().await.expect("handed");
                    (routes, attempt.establish().await)
                })
            })
            .await;
            assert_eq!(routes, [NativeRoute::Retried, NativeRoute::Handed]);
            client.expect("client");
            server.expect("server");
        });
    }

    /// One waiter per address; dropping it frees the address, and a stale
    /// waiter's drop does not remove a newer one.
    #[test]
    fn one_waiter_per_address_and_drop_removes_only_its_own() {
        let waiters = Arc::new(NativeWaiters::<Fake>::default());
        let first = waiters.register_for_test(addr()).expect("first");
        assert!(waiters.register_for_test(addr()).is_none(), "one at a time");
        let mapped: SocketAddr = "[::ffff:10.0.0.1]:5000".parse().unwrap();
        assert!(waiters.register_for_test(mapped).is_none(), "normalized");
        drop(first);
        assert_eq!(waiters.len(), 0);
        let second = waiters.register_for_test(addr()).expect("free again");
        let stale = NativeWaiter {
            waiters: waiters.clone(),
            addr: addr(),
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

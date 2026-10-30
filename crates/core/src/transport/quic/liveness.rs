//! Whether the node's QUIC endpoint is alive (#5940, step e4).
//!
//! "Alive" here means quinn's endpoint driver has not ENDED. It says nothing
//! about whether the endpoint is making progress: a driver that is stuck but
//! still pending reads as `Up`.
//!
//! quinn spawns its endpoint driver itself, through the runtime, and keeps no
//! handle to it. [`QuicRuntime`] wraps that driver, so the moment its future
//! returns is observed here:
//!
//! - while the node still holds the endpoint, the return is a death: the
//!   endpoint's [`QuicAvailability`] becomes `Down` (once), with a WARN and
//!   the `quic_endpoint_down` counter. The UDP transport is untouched and the
//!   node keeps running: endpoint death NEVER feeds `ListenerExit`, which
//!   exits the node. e5 watches the availability and moves every QUIC path
//!   back to UDP.
//! - after the node dropped its last handle, the return is the normal
//!   shutdown (quinn's driver ends once no handle and no connection is
//!   left): the availability becomes `Released` and nothing is counted.
//! - a driver task dropped without finishing (the runtime shutting down) is
//!   not reported: the process is ending.
//!
//! Panics. The node relies on unwinding (`panic = "unwind"`; a build that
//! aborts on panic refuses to build the endpoint, `super::require_unwinding`,
//! and runs with QUIC off), and what a panic does depends on where it
//! starts:
//!
//! - In the node's socket adapter ([`QuicSocket`]: `poll_recv`, `try_send`,
//!   `poll_writable`), which quinn calls while holding its endpoint or
//!   connection state lock: CONTAINED. The adapter catches it before it
//!   unwinds into quinn, so no lock is poisoned, and reports it through
//!   [`EndpointHealth::adapter_panicked`]: the endpoint is `Down` (counted
//!   in `quic_endpoint_down` and `quic_adapter_panicked`, and logged at
//!   ERROR once by the node; quinn also logs its own ERROR "I/O error" for
//!   each of its drivers that ends with the error, so a busy endpoint logs
//!   one such line per connection whose driver ran into it), and the
//!   adapter returns an I/O error, which ends the quinn driver that called
//!   it cleanly. From then on every adapter call returns
//!   that error without running the adapter's code. QUIC then stays off
//!   until the node restarts: the endpoint driver ends at its next receive;
//!   `connect` refuses at once (`QuicEndpoint::connect` checks);
//!   a handshake in progress ends with an error (quinn's implicit close, see
//!   `QuicIncoming::establish`); and each connection is closed locally by
//!   its watcher (`QuicConnection::new`), so callers get an error instead of
//!   waiting on a driver that has stopped. Peers are not told: no
//!   CONNECTION_CLOSE leaves (see `socket`), so they find out from their own
//!   idle timeout. A panic after the node released the endpoint is counted
//!   and logged but leaves it `Released`; its connections (which the node
//!   may still hold) are closed locally by their watchers all the same,
//!   since every send of the endpoint now fails (see the watcher in
//!   `QuicConnection::new`).
//! - In the node's own per-connection tasks (the path's receive task, see
//!   `path::supervise_receive`): contained per connection, which is closed
//!   with DOWNGRADE, and not counted in `quic_adapter_panicked`.
//! - In the node's runtime adapter ([`QuicRuntime`]: `now`, and
//!   `QuicTimer::reset` / `poll`, which call the `TimeSource`, including
//!   `sleep_until`): NOT contained. quinn calls them from its drivers under
//!   the same state lock, so a panic there behaves as one in quinn's own
//!   code (below). They have no error to return a panic as (`now` must
//!   return an `Instant`), so they are not wrapped like the socket adapter.
//! - In quinn's own code inside its endpoint or connection driver: NOT
//!   contained, and it can abort the process. quinn 0.11.12 polls each
//!   driver holding a `std::sync::Mutex` it locks with `.lock().unwrap()`,
//!   so the panic poisons it, and quinn's destructors unwrap the poisoned
//!   lock. For the endpoint driver that happens on every such panic: quinn
//!   spawns it inside its own `async` block, and the unwind out of that
//!   block drops the driver there, before any catch around the spawned
//!   future (ours or tokio's) runs. `EndpointDriver::drop`
//!   (`endpoint.rs:403`) and then `EndpointRef::drop` (`endpoint.rs:724`)
//!   lock the poisoned mutex during the unwind, and the process aborts. For
//!   a connection driver, `SendStream`, `RecvStream` and the last
//!   `Connection` handle lock in their `Drop`, so dropping one afterwards
//!   panics in whatever task drops it, and aborts the process if that drop
//!   runs during an unwind. The fix belongs in quinn: those destructors
//!   must tolerate a poisoned lock (both `endpoint.rs:403` and `:724` for
//!   the endpoint driver);
//!   `a_panic_unwinding_through_the_endpoint_driver_still_aborts` pins the
//!   current behaviour.
//!
//! With `transport-quic = "off"` no endpoint exists, and the transport
//! reports [`QuicAvailability::Disabled`].
//!
//! [`QuicRuntime`]: super::runtime::QuicRuntime
//! [`QuicSocket`]: super::socket::QuicSocket

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::watch;

use crate::transport::metrics::TRANSPORT_METRICS;

/// Whether this node can use QUIC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QuicAvailability {
    /// No endpoint: `transport-quic` is off, or the endpoint could not be
    /// built.
    Disabled,
    /// The endpoint's driver has not ended.
    Up,
    /// The endpoint's driver returned while the node held it. Final:
    /// nothing restarts it, and the node runs with QUIC off until it
    /// restarts.
    Down,
    /// The node dropped its last handle to the endpoint. Final.
    Released,
}

/// A view of a [`QuicAvailability`] that can be read at any time and
/// waited on. Clones see the same value.
#[derive(Debug, Clone)]
pub(crate) struct QuicAvailabilityWatch(watch::Receiver<QuicAvailability>);

impl QuicAvailabilityWatch {
    /// A watch that always reports [`QuicAvailability::Disabled`].
    pub(crate) fn disabled() -> Self {
        // The sender is dropped at once: the value never changes.
        let (_, receiver) = watch::channel(QuicAvailability::Disabled);
        Self(receiver)
    }

    /// A watch starting at `initial`, and the sender that changes it (tests).
    #[cfg(test)]
    pub(crate) fn for_test(initial: QuicAvailability) -> (watch::Sender<QuicAvailability>, Self) {
        let (sender, receiver) = watch::channel(initial);
        (sender, Self(receiver))
    }

    /// The current value.
    pub(crate) fn get(&self) -> QuicAvailability {
        *self.0.borrow()
    }

    /// Wait until the availability is not `Up`, and return it. Every value
    /// other than `Up` is final, so a caller that loops on this gets the
    /// same value back at once and never waits again. Cancel-safe.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn not_up(&mut self) -> QuicAvailability {
        let seen = self
            .0
            .wait_for(|availability| *availability != QuicAvailability::Up)
            .await
            .map(|availability| *availability);
        match seen {
            Ok(availability) => availability,
            // The sender is dropped only with the endpoint's health, after
            // the owner released it (which stores `Released` first); if it
            // somehow left `Up`, report the endpoint as released, never `Up`.
            Err(_) => match self.get() {
                QuicAvailability::Up => QuicAvailability::Released,
                other @ (QuicAvailability::Disabled
                | QuicAvailability::Down
                | QuicAvailability::Released) => other,
            },
        }
    }
}

/// The liveness state one endpoint shares with its driver's watcher and
/// its accept loop.
pub(crate) struct EndpointHealth {
    availability: watch::Sender<QuicAvailability>,
    /// Set when the node dropped its last handle to the endpoint; the
    /// driver ending after that is the normal shutdown.
    released: AtomicBool,
    /// Set by the first panic the socket adapter caught; final.
    adapter_panicked: AtomicBool,
    /// `true` once the socket adapter caught a panic, for connection
    /// watchers to wait on after Released ([`Self::adapter_panic_watch`]).
    adapter_panic_signal: watch::Sender<bool>,
    local_addr: SocketAddr,
}

impl EndpointHealth {
    pub(crate) fn new(local_addr: SocketAddr) -> Arc<Self> {
        let (availability, _) = watch::channel(QuicAvailability::Up);
        Arc::new(Self {
            availability,
            released: AtomicBool::new(false),
            adapter_panicked: AtomicBool::new(false),
            adapter_panic_signal: watch::channel(false).0,
            local_addr,
        })
    }

    pub(crate) fn watch(&self) -> QuicAvailabilityWatch {
        QuicAvailabilityWatch(self.availability.subscribe())
    }

    /// The node dropped its last handle to the endpoint.
    ///
    /// Ordering: `released` is stored before the node's last quinn
    /// `Endpoint` handle is dropped (the owner field is declared first). The
    /// driver can return only after quinn sees no handle left; quinn's
    /// handle count is `Relaxed`, but the last handle's drop then takes the
    /// endpoint state mutex to wake the driver, and the driver takes the
    /// same mutex before it returns, so the store happens before the
    /// driver's return, and the watcher's Acquire load after it sees it.
    pub(crate) fn release(&self) {
        self.released.store(true, Ordering::Release);
        self.availability.send_if_modified(|availability| {
            if *availability == QuicAvailability::Up {
                *availability = QuicAvailability::Released;
                true
            } else {
                false
            }
        });
    }

    /// Whether the node dropped its last handle (see [`Self::release`]).
    pub(crate) fn is_released(&self) -> bool {
        self.released.load(Ordering::Acquire)
    }

    /// A view that turns `true` once the socket adapter caught a panic. A
    /// connection's watcher waits on it after Released, when no `Down`
    /// will come (see `QuicConnection::new`).
    pub(crate) fn adapter_panic_watch(&self) -> watch::Receiver<bool> {
        self.adapter_panic_signal.subscribe()
    }

    /// Whether `connect` must refuse: the endpoint is Down, or the socket
    /// adapter caught a panic (the flag is set before the availability
    /// flips, so this also covers that gap).
    pub(crate) fn refuses_connects(&self) -> bool {
        self.has_adapter_panicked() || *self.availability.borrow() == QuicAvailability::Down
    }

    /// Whether a CONNECTION_CLOSE made now can still be sent: not once the
    /// endpoint is Down (its endpoint driver has ended, or is about to) or
    /// the socket adapter caught a panic (which may have ended the
    /// connection's driver, and makes every send fail; this holds after
    /// Released too).
    pub(crate) fn closes_can_be_sent(&self) -> bool {
        !self.has_adapter_panicked() && *self.availability.borrow() != QuicAvailability::Down
    }

    /// Whether the socket adapter caught a panic ([`Self::adapter_panicked`]).
    /// One atomic load: read on every receive.
    pub(crate) fn has_adapter_panicked(&self) -> bool {
        self.adapter_panicked.load(Ordering::Acquire)
    }

    /// The socket adapter caught a panic in `call`, before it unwound into
    /// quinn. The first one counts `quic_adapter_panicked`, turns the
    /// endpoint `Down`, signals [`Self::adapter_panic_watch`] and logs at
    /// ERROR; if the endpoint was no longer `Up` (the node had released it,
    /// or its driver had returned) it stays as it was and the log says so,
    /// choosing the message from the transition, so a release racing this
    /// cannot make it wrong. A later one only logs at DEBUG, so a fault that
    /// repeats is counted once per endpoint.
    pub(crate) fn adapter_panicked(
        &self,
        call: &'static str,
        payload: &(dyn std::any::Any + Send),
    ) {
        let message = panic_message(payload);
        if self.adapter_panicked.swap(true, Ordering::AcqRel) {
            tracing::debug!(local_addr = %self.local_addr, call, panic = message, "QUIC socket adapter panicked again");
            return;
        }
        TRANSPORT_METRICS.record_quic_adapter_panicked(self.local_addr);
        let mut was = QuicAvailability::Up;
        let went_down = self.availability.send_if_modified(|availability| {
            was = *availability;
            if *availability == QuicAvailability::Up {
                *availability = QuicAvailability::Down;
                true
            } else {
                false
            }
        });
        self.adapter_panic_signal.send_replace(true);
        if went_down {
            TRANSPORT_METRICS.record_quic_endpoint_down(self.local_addr);
            tracing::error!(
                local_addr = %self.local_addr,
                call,
                panic = message,
                "The QUIC socket adapter panicked; the panic was caught. The QUIC endpoint is \
                 down: the node keeps running on the UDP transport, with QUIC off until it restarts"
            );
        } else {
            tracing::error!(
                local_addr = %self.local_addr,
                call,
                panic = message,
                availability = ?was,
                "The QUIC socket adapter panicked; the panic was caught. The QUIC endpoint was \
                 already not up; its connections are closed locally"
            );
        }
    }

    /// The endpoint driver's future returned.
    fn driver_ended(&self) {
        if self.is_released() {
            tracing::debug!(local_addr = %self.local_addr, "QUIC endpoint driver ended after the endpoint was dropped");
            return;
        }
        // Only from Up, in one step under the watch's lock: a release that
        // won the race (stored Released first) is not overwritten, and a
        // second report (none is expected: the driver ends once) changes
        // and counts nothing.
        let went_down = self.availability.send_if_modified(|availability| {
            if *availability == QuicAvailability::Up {
                *availability = QuicAvailability::Down;
                true
            } else {
                false
            }
        });
        if went_down {
            TRANSPORT_METRICS.record_quic_endpoint_down(self.local_addr);
            tracing::warn!(
                local_addr = %self.local_addr,
                "QUIC endpoint is down: its driver ended while the node held it. \
                 The node keeps running on the UDP transport, with QUIC off until it restarts"
            );
        }
    }
}

/// The text of a panic payload, when it has one.
fn panic_message(payload: &(dyn std::any::Any + Send)) -> &str {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        message
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message
    } else {
        "(not a string)"
    }
}

/// Run quinn's endpoint driver and report to `health` when it returns.
///
/// No panic handling here: a panic in quinn's driver code is not contained
/// (see the module docs), and the socket adapter catches its own.
pub(crate) async fn watch_endpoint_driver(
    driver: Pin<Box<dyn Future<Output = ()> + Send>>,
    health: Arc<EndpointHealth>,
) {
    // quinn's spawned future logs and swallows the driver's I/O error, so an
    // error return and a normal return look alike here; `released` tells
    // them apart.
    driver.await;
    health.driver_ended();
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The driver's return and the release race. Whichever stores first
    /// wins: a release already stored is not turned into `Down` (and
    /// nothing is counted), and a `Down` already stored is not turned into
    /// `Released`. The first case models a release that stored `Released`
    /// after the driver's watcher read `released` as false.
    #[test]
    fn the_first_final_state_wins() {
        let addr: SocketAddr = "10.201.35.1:1".parse().unwrap();
        let health = EndpointHealth::new(addr);
        health.availability.send_replace(QuicAvailability::Released);
        health.driver_ended();
        assert_eq!(health.watch().get(), QuicAvailability::Released);
        assert_eq!(TRANSPORT_METRICS.quic_endpoint_down_at(addr), 0);

        let addr: SocketAddr = "10.201.35.2:1".parse().unwrap();
        let health = EndpointHealth::new(addr);
        health.driver_ended();
        health.release();
        assert_eq!(health.watch().get(), QuicAvailability::Down);
        health.driver_ended();
        assert_eq!(TRANSPORT_METRICS.quic_endpoint_down_at(addr), 1);
    }

    /// A socket-adapter panic is counted and turns the endpoint Down once,
    /// however often it repeats (a second, concurrent one only logs). One
    /// after the node released the endpoint is counted but leaves it
    /// `Released`, and is not counted as a death. Either way a close can no
    /// longer be sent.
    #[test]
    fn an_adapter_panic_counts_once_and_keeps_released() {
        let addr: SocketAddr = "10.201.35.3:1".parse().unwrap();
        let health = EndpointHealth::new(addr);
        assert!(health.closes_can_be_sent());
        health.adapter_panicked("test", &"first");
        health.adapter_panicked("test", &"second");
        assert_eq!(health.watch().get(), QuicAvailability::Down);
        assert_eq!(TRANSPORT_METRICS.quic_adapter_panicked_at(addr), 1);
        assert_eq!(TRANSPORT_METRICS.quic_endpoint_down_at(addr), 1);
        assert!(!health.closes_can_be_sent());

        let addr: SocketAddr = "10.201.35.4:1".parse().unwrap();
        let health = EndpointHealth::new(addr);
        health.release();
        assert!(health.closes_can_be_sent());
        health.adapter_panicked("test", &"after release");
        assert_eq!(health.watch().get(), QuicAvailability::Released);
        assert_eq!(TRANSPORT_METRICS.quic_adapter_panicked_at(addr), 1);
        assert_eq!(TRANSPORT_METRICS.quic_endpoint_down_at(addr), 0);
        assert!(!health.closes_can_be_sent());
    }
}

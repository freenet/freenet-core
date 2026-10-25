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
//! What this does NOT cover: a PANIC in the driver. quinn's driver returns
//! only on an I/O error from `drive_recv`, and [`QuicSocket`] never returns
//! one, so in production today the driver cannot return at all; the only
//! way it ends is a panic, and a panic is not contained. quinn 0.11.12 polls
//! the driver while holding its endpoint state mutex
//! (`EndpointDriver::poll`, `endpoint.rs` line 370: `state.lock().unwrap()`),
//! so a panic there poisons it, and every later lock of it panics too:
//! `EndpointDriver::drop` (line 403), `Accept::poll` (line 651, inside
//! `quic_accept_loop`), `connect` (line 214) and the connection drivers. Where
//! one of those runs while the first panic is still unwinding, the process
//! aborts. So this circuit only reports a driver that RETURNED (tests inject
//! that with a receive error, `socket::ReceiveFault`). Containing a quinn
//! panic (catch the unwind, leak the driver instead of dropping it, guard
//! the accept loop against the poisoned lock) is a separate prerequisite
//! before QUIC is on by default.
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
    local_addr: SocketAddr,
}

impl EndpointHealth {
    pub(crate) fn new(local_addr: SocketAddr) -> Arc<Self> {
        let (availability, _) = watch::channel(QuicAvailability::Up);
        Arc::new(Self {
            availability,
            released: AtomicBool::new(false),
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

/// Run quinn's endpoint driver and report to `health` when it returns.
///
/// No panic handling: a panic in quinn's driver is not contained here (see
/// the module docs) and is never reported as `Down`.
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
}

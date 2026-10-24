//! Whether the node's QUIC endpoint is alive (#5940, step e4).
//!
//! quinn spawns its endpoint driver itself, through the runtime, and keeps
//! no handle to it: if the driver ends (a socket error it cannot get past)
//! or panics, every connection on the endpoint ends and `accept` returns
//! `None`, and nothing else says so. [`QuicRuntime`] wraps that driver, so
//! its end is observed here:
//!
//! - while the node still holds the endpoint, the end is a death: the
//!   endpoint's [`QuicAvailability`] becomes `Down` (once), with a WARN and
//!   the `quic_endpoint_down` counter. The UDP transport is untouched and
//!   the node keeps running: endpoint death NEVER feeds `ListenerExit`,
//!   which exits the node. e5 watches the availability and moves every QUIC
//!   path back to UDP.
//! - after the node dropped the endpoint, the driver ending is the normal
//!   shutdown (quinn's driver ends once no handle and no connection is
//!   left), and nothing is reported.
//! - if the driver task is dropped without finishing (the runtime shutting
//!   down), nothing is reported either: the process is ending.
//!
//! With `transport-quic = "off"` no endpoint exists, and the transport
//! reports [`QuicAvailability::Disabled`].
//!
//! [`QuicRuntime`]: super::runtime::QuicRuntime

use std::future::Future;
use std::net::SocketAddr;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures::FutureExt;
use tokio::sync::watch;

use crate::transport::metrics::TRANSPORT_METRICS;

/// Whether this node can use QUIC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QuicAvailability {
    /// No endpoint: `transport-quic` is off, or the endpoint could not be
    /// built.
    Disabled,
    /// The endpoint is running.
    Up,
    /// The endpoint died while the node held it. Final: nothing restarts
    /// it, and the node runs with QUIC off until it restarts.
    Down,
}

/// A view of a [`QuicAvailability`] that can be read at any time and
/// waited on. Clones see the same value.
#[derive(Debug, Clone)]
pub(crate) struct QuicAvailabilityWatch(watch::Receiver<QuicAvailability>);

impl QuicAvailabilityWatch {
    /// A watch that always reports [`QuicAvailability::Disabled`].
    pub(crate) fn disabled() -> Self {
        // The sender is dropped at once: the value never changes, and a
        // waiter in `down` sees the channel closed and returns.
        let (_, receiver) = watch::channel(QuicAvailability::Disabled);
        Self(receiver)
    }

    /// The current value.
    pub(crate) fn get(&self) -> QuicAvailability {
        *self.0.borrow()
    }

    /// Wait until the endpoint is no longer `Up`: `Down`, or `Disabled`
    /// from the start. Also returns once the endpoint is dropped (its
    /// value is then no longer updated). Cancel-safe.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn not_up(&mut self) -> QuicAvailability {
        let seen = self
            .0
            .wait_for(|availability| *availability != QuicAvailability::Up)
            .await
            .map(|availability| *availability);
        seen.unwrap_or_else(|_| self.get())
    }
}

/// The liveness state one endpoint shares with its driver's watcher.
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
    pub(crate) fn release(&self) {
        self.released.store(true, Ordering::Release);
    }

    /// The endpoint driver's future finished (`panicked`: by panicking).
    fn driver_ended(&self, panicked: bool) {
        if self.released.load(Ordering::Acquire) && !panicked {
            tracing::debug!(local_addr = %self.local_addr, "QUIC endpoint driver ended after the endpoint was dropped");
            return;
        }
        // send_replace returns the previous value, so a second report (none
        // is expected: the driver ends once) changes and counts nothing.
        if self.availability.send_replace(QuicAvailability::Down) != QuicAvailability::Down {
            TRANSPORT_METRICS.record_quic_endpoint_down(self.local_addr);
            tracing::warn!(
                local_addr = %self.local_addr,
                panicked,
                "QUIC endpoint is down: its driver ended while the node held it. \
                 The node keeps running on the UDP transport, with QUIC off until it restarts"
            );
        }
    }
}

/// Run quinn's endpoint driver and report how it ended to `health`.
pub(crate) async fn watch_endpoint_driver(
    driver: Pin<Box<dyn Future<Output = ()> + Send>>,
    health: Arc<EndpointHealth>,
) {
    let mut guard = DriverGuard {
        health: Some(health),
    };
    // quinn's spawned future logs and swallows the driver's I/O error, so
    // an error end and a normal end look alike here; `released` tells them
    // apart. A panic is caught so it is reported too.
    let ended = AssertUnwindSafe(driver).catch_unwind().await;
    if let Some(health) = guard.health.take() {
        health.driver_ended(ended.is_err());
    }
}

/// Notes a driver task dropped before it finished.
struct DriverGuard {
    health: Option<Arc<EndpointHealth>>,
}

impl Drop for DriverGuard {
    fn drop(&mut self) {
        if let Some(health) = self.health.take() {
            tracing::debug!(
                local_addr = %health.local_addr,
                "QUIC endpoint driver task dropped before it finished (runtime shutting down)"
            );
        }
    }
}

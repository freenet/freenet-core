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
//!   back to UDP. The one exception is a gateway run by the `freenet`
//!   binary under a service supervisor ([`enable_gateway_quic_restarts`];
//!   with no UDP to fall back to, #6151, a supervised peer too):
//!   its transport listener then reports the death as
//!   `ListenerExit::QuicEndpointDown`, the node's fatal-listener path exits
//!   the process, and the supervisor restarts it with a new endpoint
//!   ([`gateway_quic_restarts`]). The crash-loop breaker restarts QUIC after
//!   the first such exit in a day and trips after later ones
//!   (`breaker::ENDPOINT_DOWN_EXITS_TO_TRIP`); under its override the
//!   gateway does not exit for a death.
//!   The same switch restarts such a gateway when a breaker cooldown that
//!   kept its QUIC off ends (`breaker::quic_restore_at`).
//!   A node that does not exit for a death rebuilds its endpoint in process
//!   instead, on the same socket, when `super::rebuild::ENDPOINT_REBUILD` is
//!   on (#6272; dark until the UDP removal turns it on together with
//!   `breaker::UDP_FALLBACK = false`). See `super::rebuild`.
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
//! - In quinn's own code inside one of its drivers (the endpoint driver or a
//!   connection driver): an ENDPOINT DEATH. quinn polls each driver holding
//!   a `std::sync::Mutex` it locks with `.lock().unwrap()`, so the panic
//!   poisons it. Until quinn-rs/quinn#2987, which the pinned `freenet-quinn`
//!   fork carries (#6184), quinn's destructors then unwrapped the poisoned
//!   lock during the unwind (`EndpointDriver::drop`, `endpoint.rs:399`, and
//!   `EndpointRef::drop`, `endpoint.rs:770`, for the endpoint driver, which
//!   quinn drops inside its own `async` block) and the process aborted; they
//!   now tolerate it. The panic reaches the driver's watcher
//!   ([`watch_endpoint_driver`], [`watch_connection_driver`]), which catches
//!   it and reports it through [`EndpointHealth::quinn_panicked`]: an ERROR,
//!   the endpoint `Down`, and every connection closed locally by its
//!   watcher. From there it is handled as any endpoint death (above): a
//!   supervised node exits through `ListenerExit::QuicEndpointDown` and is
//!   restarted, counted by the crash breaker; an unsupervised one moves its
//!   QUIC paths back to UDP. The node does NOT wrap its calls into quinn:
//!   every other quinn call that takes the poisoned lock (`accept`,
//!   `connect`, `local_addr`, `Incoming::ignore` / `retry` / `accept` for
//!   the endpoint; a read, write, `reset` or `close` for the connection)
//!   unwraps it and panics in its caller. Instead it stops making those
//!   calls once the endpoint is not `Up`: `connect` refuses first, the
//!   accept loop checks the availability before each poll of `accept`, and
//!   the attempt router DROPS an attempt instead of answering it (quinn's
//!   `Incoming::drop` skips a poisoned lock). Nothing it runs during an
//!   unwind calls quinn, since a second panic then aborts the process:
//!   `Outgoing::drop` skips its reset (also after a reported quinn panic),
//!   and a close made during an unwind (`QuicConnection::drop`, a session's
//!   `EndOnExit::drop` or `QuicPathState::drop` tearing its path down) is
//!   sent from a task (`QuicCloser::close_inner`). A call that still reaches
//!   the lock (a call on another thread in the moment between quinn's panic
//!   and its watcher's report, a session acting on an attempt it was handed
//!   before the death, a session whose path is on the poisoned connection
//!   tearing it down on Down before the watcher's close) panics in QUIC
//!   code (only the first in a monitored task; the others are log noise),
//!   which the crash breaker attributes to QUIC (`quic_attributed`), so it
//!   turns QUIC off after the restart. Pinned by
//!   `a_panic_unwinding_through_the_endpoint_driver_is_contained`,
//!   `a_panic_in_a_quinn_connection_driver_drives_the_endpoint_down`,
//!   `a_send_in_flight_on_a_poisoned_connection_does_not_abort`,
//!   `a_close_during_an_unwind_on_a_poisoned_connection_does_not_abort` and
//!   `attempts_queued_before_a_driver_panic_are_dropped`. If such a panic
//!   kills a monitored task on a node that exits for an endpoint death, the
//!   node exits as for the death ([`driver_death_restart`]). A node that
//!   rebuilds its endpoint instead (`super::rebuild`, never one that exits
//!   for a death) has those tasks owned by its rebuild loop, which takes
//!   such an end for the death and rebuilds, waiting the same
//!   [`DRIVER_PANIC_REPORT_WAIT`] for the report.
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

/// Set by [`enable_gateway_quic_restarts`]; off in tests and embedders.
static GATEWAY_QUIC_RESTARTS: AtomicBool = AtomicBool::new(false);

/// Let a gateway exit for its supervisor to restart it when that restores
/// QUIC (#5940, a prerequisite of native gateway joins): when its QUIC
/// endpoint dies, and when a crash-breaker cooldown that kept QUIC off for
/// the run ends. The `freenet` binary calls this only when its own service
/// unit marks it supervised (`FREENET_SUPERVISED`, set by the units
/// `freenet service install` writes), so a gateway started by hand, or under
/// a supervisor that may not restart it, keeps running over UDP, as before.
/// Peers never exit for this while UDP remains; with no UDP to fall back to
/// (#6151) a supervised peer exits for an endpoint death too.
pub fn enable_gateway_quic_restarts() {
    GATEWAY_QUIC_RESTARTS.store(true, Ordering::Relaxed);
}

/// Set by [`note_gateway_supervisor_unverified`].
static SUPERVISOR_UNVERIFIED: AtomicBool = AtomicBool::new(false);

/// Record that the binary runs under systemd (`INVOCATION_ID`) without the
/// `FREENET_SUPERVISED` marker. Such a unit may not restart the node (a
/// `systemd-run` command, a unit with `Restart=no`), so gateway restarts
/// stay off and a gateway logs a WARN saying how to turn them on.
pub fn note_gateway_supervisor_unverified() {
    SUPERVISOR_UNVERIFIED.store(true, Ordering::Relaxed);
}

/// Set by [`exit_for_endpoint_death`]: this process's transport listener
/// exits when its QUIC endpoint dies (`ListenerExit::QuicEndpointDown`).
static ENDPOINT_DEATH_EXITS: AtomicBool = AtomicBool::new(false);

/// Set by [`EndpointHealth::quinn_panicked`]: a panic in a quinn driver
/// poisoned a quinn lock in this process.
static DRIVER_PANICKED: std::sync::LazyLock<watch::Sender<bool>> =
    std::sync::LazyLock::new(|| watch::channel(false).0);

/// Record that this process's transport listener exits for its supervisor to
/// restart it when the QUIC endpoint dies (it watches the endpoint, see
/// `connection_handler::gateway_quic_restart`). Read by
/// [`driver_death_restart`].
pub(crate) fn exit_for_endpoint_death() {
    ENDPOINT_DEATH_EXITS.store(true, Ordering::Relaxed);
}

/// How long [`driver_death_restart`] waits for a driver panic's report,
/// which follows the panic within its unwind; a task on another worker can
/// fail on the poisoned lock just before it.
pub(crate) const DRIVER_PANIC_REPORT_WAIT: std::time::Duration = std::time::Duration::from_secs(1);

/// Whether a monitored task's failure is a quinn driver panic's consequence
/// that should exit the node as the endpoint death (#6184): this process
/// exits for one ([`exit_for_endpoint_death`]), a driver panic is reported
/// within [`DRIVER_PANIC_REPORT_WAIT`], and `record` (the listener's
/// once-per-process `breaker::note_endpoint_down_exit_once`) recorded the
/// death. Otherwise the failure keeps its own exit.
pub(crate) async fn driver_death_restart(
    record: impl FnOnce(std::time::SystemTime) -> bool,
) -> bool {
    if !ENDPOINT_DEATH_EXITS.load(Ordering::Relaxed) {
        return false;
    }
    let mut panicked = DRIVER_PANICKED.subscribe();
    let reported = tokio::time::timeout(DRIVER_PANIC_REPORT_WAIT, panicked.wait_for(|p| *p))
        .await
        .is_ok_and(|seen| seen.is_ok());
    reported && record(std::time::SystemTime::now())
}

/// Whether [`note_gateway_supervisor_unverified`] was called.
pub(crate) fn gateway_supervisor_unverified() -> bool {
    SUPERVISOR_UNVERIFIED.load(Ordering::Relaxed)
}

/// Whether a node in this role exits to restore QUIC, and only when the
/// binary enabled it ([`enable_gateway_quic_restarts`]): a gateway, and, once
/// there is no UDP to fall back to (#6151, `breaker::udp_fallback`), a peer
/// too, since a peer whose endpoint died would otherwise have no transport.
pub(crate) fn gateway_quic_restarts(is_gateway: bool) -> bool {
    gateway_quic_restarts_with(
        is_gateway,
        GATEWAY_QUIC_RESTARTS.load(Ordering::Relaxed),
        super::breaker::udp_fallback(),
    )
}

/// Whether a node in this role restarts to restore QUIC at all, enabled or
/// not: a gateway, or any node with no UDP to fall back to.
pub(crate) fn restarts_for_quic(is_gateway: bool) -> bool {
    restarts_for_quic_with(is_gateway, super::breaker::udp_fallback())
}

fn restarts_for_quic_with(is_gateway: bool, udp_fallback: bool) -> bool {
    is_gateway || !udp_fallback
}

pub(in crate::transport::quic) fn gateway_quic_restarts_with(
    is_gateway: bool,
    enabled: bool,
    udp_fallback: bool,
) -> bool {
    enabled && restarts_for_quic_with(is_gateway, udp_fallback)
}

/// Whether this node can use QUIC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QuicAvailability {
    /// No endpoint: `transport-quic` is off, or the endpoint could not be
    /// built.
    Disabled,
    /// The endpoint's driver has not ended.
    Up,
    /// The endpoint's driver returned while the node held it. Final for
    /// that endpoint: nothing restarts it, and the node runs with QUIC off
    /// until it restarts, or, for a node that rebuilds its endpoint
    /// (`super::rebuild`, dark until the UDP removal), until it has built a
    /// new one, whose own watch starts at `Up` again.
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
        Self::settable(initial)
    }

    /// A watch starting at `initial`, and the sender that changes it: the
    /// node's view of a QUIC endpoint it rebuilds when it dies
    /// (`super::rebuild`), which goes from `Down` back to `Up`. Unlike one
    /// endpoint's own watch, `Down` is not final here.
    pub(crate) fn settable(initial: QuicAvailability) -> (watch::Sender<QuicAvailability>, Self) {
        let (sender, receiver) = watch::channel(initial);
        (sender, Self(receiver))
    }

    /// The current value.
    pub(crate) fn get(&self) -> QuicAvailability {
        *self.0.borrow()
    }

    /// Resolve when the endpoint dies (`Down`); never resolve when it is
    /// released, disabled or still up at the end. Cancel-safe.
    pub(crate) async fn died(mut self) {
        if self.not_up().await == QuicAvailability::Down {
            return;
        }
        std::future::pending::<()>().await;
    }

    /// Wait until the availability is not `Up`, and return it. On one
    /// endpoint's watch every value other than `Up` is final, so a caller
    /// that loops on this gets the same value back at once and never waits
    /// again. (On the node's view of an endpoint it rebuilds,
    /// [`Self::settable`], `Down` can turn back into `Up`, so a loop on it
    /// spins while the node is down; nothing loops on that view.) Cancel-safe.
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
                 down: the node keeps running on the UDP transport, with QUIC off until it restarts \
                 (a supervised gateway exits for its supervisor to restart it)"
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

    /// A panic in quinn's own code unwound out of one of its drivers (the
    /// endpoint driver, or a connection driver) and was caught by the
    /// driver's watcher ([`watch_endpoint_driver`], [`watch_connection_driver`]).
    /// The panic poisoned the lock quinn holds that state under, so this is
    /// an endpoint death: the endpoint turns `Down` (the first report logs at
    /// ERROR and counts `quic_endpoint_down`, a later one logs at DEBUG), and,
    /// as after an adapter panic, every adapter call then fails, connects are
    /// refused and the connection watchers close their connections locally.
    /// `quic_adapter_panicked` is not counted: the panic was not the
    /// adapter's. What follows Down is the usual path (module docs).
    pub(crate) fn quinn_panicked(&self, call: &'static str, payload: &(dyn std::any::Any + Send)) {
        let message = panic_message(payload);
        if self.adapter_panicked.swap(true, Ordering::AcqRel) {
            tracing::debug!(local_addr = %self.local_addr, call, panic = message, "A panic in quinn was caught again");
            return;
        }
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
        DRIVER_PANICKED.send_replace(true);
        self.adapter_panic_signal.send_replace(true);
        if went_down {
            TRANSPORT_METRICS.record_quic_endpoint_down(self.local_addr);
        }
        tracing::error!(
            local_addr = %self.local_addr,
            call,
            panic = message,
            availability = ?was,
            "A QUIC driver panicked inside quinn; the panic was caught. The QUIC endpoint is \
             down and its connections are closed locally"
        );
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
                 The node keeps running on the UDP transport, with QUIC off until it restarts \
                 (a supervised gateway exits for its supervisor to restart it)"
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

/// Run quinn's endpoint driver and report to `health` when it returns or
/// panics.
///
/// A panic in quinn's own driver code no longer aborts the process (the
/// destructors that ran during the unwind unwrapped the poisoned lock until
/// quinn#2987, which the pinned `freenet-quinn` carries). It unwinds out of
/// the driver's future to here, where it is logged and reported like any
/// other end of the driver: the endpoint goes Down. Without the catch the
/// tokio task would swallow the panic and the endpoint would stay `Up`
/// with nothing driving it. The socket adapter catches its own panics
/// before they reach quinn.
pub(crate) async fn watch_endpoint_driver(
    driver: Pin<Box<dyn Future<Output = ()> + Send>>,
    health: Arc<EndpointHealth>,
) {
    use futures::FutureExt;
    // quinn's spawned future logs and swallows the driver's I/O error, so an
    // error return and a normal return look alike here; `released` tells
    // them apart.
    if let Err(payload) = std::panic::AssertUnwindSafe(driver).catch_unwind().await {
        health.quinn_panicked("endpoint driver", &*payload);
    }
    health.driver_ended();
}

/// Run one of quinn's connection drivers and report a panic in it to
/// `health` as an endpoint death (module docs). Without this the tokio task
/// would swallow the panic and the endpoint would stay `Up` with that
/// connection wedged on its poisoned state lock. A driver that returns
/// normally needs nothing here.
pub(crate) async fn watch_connection_driver(
    driver: Pin<Box<dyn Future<Output = ()> + Send>>,
    health: Arc<EndpointHealth>,
) {
    use futures::FutureExt;
    if let Err(payload) = std::panic::AssertUnwindSafe(driver).catch_unwind().await {
        health.quinn_panicked("connection driver", &*payload);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// While UDP exists only an enabled gateway exits on endpoint death;
    /// with no UDP to fall back to (#6151) an enabled peer does too.
    #[test]
    fn only_an_enabled_gateway_exits_on_endpoint_death() {
        for udp in [true, false] {
            assert!(gateway_quic_restarts_with(true, true, udp));
            assert!(!gateway_quic_restarts_with(true, false, udp), "not enabled");
            assert!(!gateway_quic_restarts_with(false, false, udp));
        }
        assert!(
            !gateway_quic_restarts_with(false, true, true),
            "a peer, with UDP"
        );
        assert!(
            gateway_quic_restarts_with(false, true, false),
            "a peer, no UDP"
        );
        assert!(restarts_for_quic_with(true, true));
        assert!(!restarts_for_quic_with(false, true));
        assert!(restarts_for_quic_with(false, false));
        // While the switch says UDP exists, a peer never restarts.
        assert_eq!(
            restarts_for_quic(false),
            !crate::transport::quic::breaker::UDP_FALLBACK
        );
        // Nothing in the unit-test binary enables it.
        assert!(!gateway_quic_restarts(true));
    }

    /// `died` resolves on `Down` only: a release or a disabled endpoint is
    /// not a death and never resolves it.
    #[tokio::test]
    async fn died_resolves_only_on_down() {
        use std::time::Duration;
        let (sender, watch) = QuicAvailabilityWatch::for_test(QuicAvailability::Up);
        let died = tokio::spawn(watch.died());
        tokio::task::yield_now().await;
        assert!(!died.is_finished());
        sender.send(QuicAvailability::Down).unwrap();
        tokio::time::timeout(Duration::from_secs(5), died)
            .await
            .expect("Down resolves it")
            .unwrap();

        for other in [QuicAvailability::Released, QuicAvailability::Disabled] {
            let (sender, watch) = QuicAvailabilityWatch::for_test(QuicAvailability::Up);
            let died = watch.died();
            sender.send(other).unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(50), died)
                    .await
                    .is_err(),
                "{other:?} is not a death"
            );
        }
        // The sender gone while Up reads as released, not a death.
        let (sender, watch) = QuicAvailabilityWatch::for_test(QuicAvailability::Up);
        drop(sender);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), watch.died())
                .await
                .is_err()
        );
    }

    /// The driver's return and the release race. Whichever stores first
    /// wins: a release already stored is not turned into `Down` (and
    /// nothing is counted), and a `Down` already stored is not turned into
    /// `Released`. The first case models a release that stored `Released`
    /// after the driver's watcher read `released` as false.
    #[test]
    fn the_first_final_state_wins() {
        let net = crate::transport::test_net::subnet();
        let addr: SocketAddr = net.addr_port(1, 1);
        let health = EndpointHealth::new(addr);
        health.availability.send_replace(QuicAvailability::Released);
        health.driver_ended();
        assert_eq!(health.watch().get(), QuicAvailability::Released);
        assert_eq!(TRANSPORT_METRICS.quic_endpoint_down_at(addr), 0);

        let addr: SocketAddr = net.addr_port(2, 1);
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
        let net = crate::transport::test_net::subnet();
        let addr: SocketAddr = net.addr_port(3, 1);
        let health = EndpointHealth::new(addr);
        assert!(health.closes_can_be_sent());
        health.adapter_panicked("test", &"first");
        health.adapter_panicked("test", &"second");
        assert_eq!(health.watch().get(), QuicAvailability::Down);
        assert_eq!(TRANSPORT_METRICS.quic_adapter_panicked_at(addr), 1);
        assert_eq!(TRANSPORT_METRICS.quic_endpoint_down_at(addr), 1);
        assert!(!health.closes_can_be_sent());

        let addr: SocketAddr = net.addr_port(4, 1);
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

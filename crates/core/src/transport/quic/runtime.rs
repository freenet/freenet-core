//! quinn's `Runtime` over the crate's [`TimeSource`].
//!
//! quinn reads the time and arms its timers only through its `Runtime`
//! (quinn `runtime.rs`: `now`, `new_timer`), and `quinn-proto` takes the time
//! as an argument everywhere, so routing those two calls through a
//! [`TimeSource`] puts the whole endpoint on that clock: `VirtualTime` in
//! tests, `RealTime` on a node. Tasks are spawned with [`GlobalExecutor`].
//!
//! The first task quinn spawns is the endpoint driver
//! (`Endpoint::new_with_abstract_socket` spawns it before returning; every
//! later spawn is a connection driver). The runtime wraps that one so its end
//! is reported to the endpoint's [`EndpointHealth`] (see `liveness`).

use std::fmt;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use quinn::{AsyncTimer, AsyncUdpSocket, Runtime};

use super::liveness::{EndpointHealth, watch_endpoint_driver};
use crate::config::GlobalExecutor;
use crate::simulation::TimeSource;

type SleepFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Converts between quinn's `Instant`s and a [`TimeSource`]'s nanoseconds.
///
/// quinn's API takes `std::time::Instant`, which has no constructor but
/// `now()`. One is read when the runtime is built and serves only as the
/// zero point: every later `Instant` quinn sees is that zero point plus the
/// time source's elapsed nanoseconds, so quinn's clock is the time source,
/// and the wall clock is never consulted again. (The zero point comes from
/// tokio's clock, like `RealTime`'s epoch.)
#[derive(Clone)]
struct Clock<T> {
    time: T,
    zero_instant: Instant,
    zero_nanos: u64,
}

impl<T: TimeSource> Clock<T> {
    fn new(time: T) -> Self {
        Self {
            zero_nanos: time.now_nanos(),
            zero_instant: tokio::time::Instant::now().into_std(),
            time,
        }
    }

    fn now(&self) -> Instant {
        self.instant_at(self.time.now_nanos())
    }

    fn instant_at(&self, nanos: u64) -> Instant {
        self.zero_instant + Duration::from_nanos(nanos.saturating_sub(self.zero_nanos))
    }

    fn nanos_at(&self, instant: Instant) -> u64 {
        let since = instant.saturating_duration_since(self.zero_instant);
        self.zero_nanos
            .saturating_add(u64::try_from(since.as_nanos()).unwrap_or(u64::MAX))
    }
}

/// The QUIC endpoint's runtime: quinn's clock and timers on `T`.
pub(crate) struct QuicRuntime<T> {
    clock: Arc<Clock<T>>,
    /// Taken by the first spawn, quinn's endpoint driver.
    endpoint_driver: parking_lot::Mutex<Option<Arc<EndpointHealth>>>,
}

impl<T: TimeSource> QuicRuntime<T> {
    /// A runtime on `time` whose first spawned task, the endpoint driver,
    /// reports its end to `health`.
    pub(crate) fn new(time: T, health: Arc<EndpointHealth>) -> Self {
        Self {
            clock: Arc::new(Clock::new(time)),
            endpoint_driver: parking_lot::Mutex::new(Some(health)),
        }
    }
}

impl<T> fmt::Debug for QuicRuntime<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuicRuntime").finish_non_exhaustive()
    }
}

impl<T: TimeSource> Runtime for QuicRuntime<T> {
    fn new_timer(&self, deadline: Instant) -> Pin<Box<dyn AsyncTimer>> {
        Box::pin(QuicTimer {
            clock: self.clock.clone(),
            deadline_ns: self.clock.nanos_at(deadline),
            armed: None,
        })
    }

    fn spawn(&self, future: Pin<Box<dyn Future<Output = ()> + Send>>) {
        // quinn spawns its endpoint driver first, then one driver per
        // connection; each ends when its endpoint or connection does. None
        // is registered with BackgroundTaskMonitor: their end must not stop
        // the node. The endpoint driver's end is reported instead.
        let endpoint_driver = self.endpoint_driver.lock().take();
        match endpoint_driver {
            Some(health) => {
                GlobalExecutor::spawn(watch_endpoint_driver(future, health));
            }
            None => {
                GlobalExecutor::spawn(future);
            }
        }
    }

    /// Never used: the endpoint is built over its own socket
    /// (`Endpoint::new_with_abstract_socket`).
    fn wrap_udp_socket(&self, _socket: std::net::UdpSocket) -> io::Result<Arc<dyn AsyncUdpSocket>> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "the QUIC endpoint uses the transport's socket, not a std socket",
        ))
    }

    fn now(&self) -> Instant {
        self.clock.now()
    }
}

/// One quinn timer on the time source.
///
/// quinn moves a connection's deadline on almost every packet. Arming a new
/// time-source sleep on each move would leave the replaced ones behind, since
/// a `VirtualTime` sleep stays registered until its deadline whether or not
/// it is still polled. So `reset` only records the deadline: a sleep already
/// armed for an earlier time is kept, wakes early, and is re-armed for the
/// recorded deadline then; a new sleep is armed only when the deadline moves
/// earlier than the armed one.
struct QuicTimer<T> {
    clock: Arc<Clock<T>>,
    deadline_ns: u64,
    armed: Option<(u64, SleepFuture)>,
}

impl<T> fmt::Debug for QuicTimer<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuicTimer")
            .field("deadline_ns", &self.deadline_ns)
            .field("armed_at_ns", &self.armed.as_ref().map(|(at, _)| *at))
            .finish()
    }
}

impl<T: TimeSource> AsyncTimer for QuicTimer<T> {
    fn reset(self: Pin<&mut Self>, deadline: Instant) {
        let this = self.get_mut();
        this.deadline_ns = this.clock.nanos_at(deadline);
    }

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        loop {
            if this.clock.time.now_nanos() >= this.deadline_ns {
                this.armed = None;
                return Poll::Ready(());
            }
            if matches!(&this.armed, Some((at, _)) if *at > this.deadline_ns) {
                this.armed = None;
            }
            let deadline = this.deadline_ns;
            let clock = &this.clock;
            let (_, sleep) = this
                .armed
                .get_or_insert_with(|| (deadline, clock.time.sleep_until(deadline)));
            match sleep.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                // An early sleep fired; check the deadline again.
                Poll::Ready(()) => this.armed = None,
            }
        }
    }
}

#[cfg(test)]
impl<T: TimeSource> QuicRuntime<T> {
    /// A timer as quinn makes one, for tests.
    pub(crate) fn timer_at(&self, deadline: Instant) -> Pin<Box<dyn AsyncTimer>> {
        self.new_timer(deadline)
    }
}

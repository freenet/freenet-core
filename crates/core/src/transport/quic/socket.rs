//! quinn's `AsyncUdpSocket` over the [`DemuxSocket`].
//!
//! Receiving reads the demux socket's QUIC ingress queue, which the listener
//! fills; the endpoint never reads the UDP socket itself. Sending goes
//! through the demux socket without waiting, and when the node has an egress
//! cap each datagram is admitted by it first.
//!
//! quinn calls `poll_recv`, `try_send` and `poll_writable` while it holds its
//! endpoint or connection state lock. A panic in them is caught here, before
//! it unwinds into quinn (where it would poison that lock), and reported
//! ([`EndpointHealth::adapter_panicked`]): the endpoint goes `Down`. The call
//! then returns an I/O error, which ends the quinn driver that made it
//! without looping. Every later call returns one too (one atomic load,
//! before the adapter's own code runs), so a fault that lasts is hit once
//! per endpoint, and the endpoint driver is woken for its next receive, so
//! it ends promptly. See `super::liveness` for what is and is not
//! contained.
//!
//! Sends fail too, so the endpoint's other connections do not send their
//! CONNECTION_CLOSE: their peers learn of the end only from their own idle
//! timeout. Sending it would not work anyway: once the endpoint driver
//! ends, quinn's `EndpointDriver::drop` drops each connection's event
//! channel, and a connection driver that sees that ends with
//! INTERNAL_ERROR before it transmits (quinn 0.11.12,
//! `ConnectionDriver::poll`), which is what usually happens first. Pinned
//! by `an_adapter_panic_ends_every_connection_and_counts_once`.
//!
//! The catches use `AssertUnwindSafe`: the state a panic can interrupt here
//! (the demux socket, its egress cap and allow-set) is shared with the UDP
//! transport and is left as the panic left it, not repaired. Their locks
//! (`parking_lot`, `DashMap`) do not poison, so UDP keeps working, but a
//! panic part way through an update there could leave a count off.

use std::fmt;
use std::io::{self, IoSliceMut};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use quinn::udp::{RecvMeta, Transmit};
use quinn::{AsyncUdpSocket, UdpPoller};

use super::config::MAX_UDP_PAYLOAD;
use super::liveness::EndpointHealth;
use crate::transport::demux::{DemuxSocket, QuicIngressReceiver};
use crate::transport::egress_cap::AdmissionWaiter;
use crate::transport::{Socket, normalize_mapped_addr};

/// QUIC transmits the egress cap is holding, node-wide: quinn has built
/// them (their frames are counted in its stats) but they have not been
/// sent. The path's acknowledgement watch (`recycle`) runs no window while
/// one is held, since a send it counted may not have left yet.
///
/// A hold is a poller whose `poll_writable` the cap answered `Pending`.
/// Waiting for the kernel's send buffer (`poll_send_ready` pending, no cap
/// involved) is not counted: quinn's frames are counted the same way then,
/// but such a wait lasts as long as the socket stays full, which also holds
/// up every other send, the peer's acknowledgements of earlier packets
/// included, and it does not outlast a working socket.
///
/// A poller of a connection that has ended stays held until quinn drops it
/// with the connection's last handle, and is never polled again. So
/// [`Self::read`] reports how long a hold may stay valid
/// ([`cap_hold_validity`]), and every poll that still waits counts as a new
/// hold event (`holds`); the watch ignores a hold that has gone that long
/// with no new event.
#[derive(Debug, Default)]
pub(crate) struct CapHolds {
    /// Pollers the cap holds right now.
    held_now: AtomicU64,
    /// Polls the cap answered `Pending`, ever: a poller starting to wait,
    /// and every later poll while it still waits.
    holds: AtomicU64,
    /// [`cap_hold_validity`] of the cap's rate; zero without a cap.
    validity: Duration,
}

/// What [`CapHolds::read`] returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CapHoldReading {
    pub(crate) holds: u64,
    pub(crate) held_now: bool,
    pub(crate) validity: Duration,
}

/// The margin [`cap_hold_validity`] puts on the longest wait the cap can
/// give one QUIC transmit, for the time from a waiter's timer to its poll.
pub(crate) const CAP_HOLD_VALIDITY_MARGIN: u32 = 2;

/// How long a hold stays valid with no new hold event, for a cap of
/// `rate_bytes_per_sec`: [`CAP_HOLD_VALIDITY_MARGIN`] times the longest the
/// cap makes one waiting transmit wait before its poller is polled again.
/// That is the cap's debt limit (unpaced sends may run the bucket that far
/// into debt) plus one datagram's refill for every waiter that can be queued
/// ahead of it (`MAX_ADMISSION_WAKERS`) and for its own. A held poller of a
/// live connection is polled again within that, at its timer; one that is
/// not was left behind by a connection that ended.
///
/// The tail is long at low rates: about 4 min at 10 KB/s, 27 s at
/// 100 KB/s, under 5 s at 1 MB/s. For that long a left-behind hold still
/// pauses guard 2. The bound is in the cap's time, and the watch measures
/// the hold's age on the path's clock, so the two must be one clock;
/// `connection_handler` already requires a QUIC endpoint to share the cap's
/// clock before it uses the cap.
pub(crate) fn cap_hold_validity(rate_bytes_per_sec: u64) -> Duration {
    use crate::transport::egress_cap::{DEBT_LIMIT, MAX_ADMISSION_WAKERS, WAKE_DATAGRAM_BYTES};
    let queued = (MAX_ADMISSION_WAKERS as u64 + 1) * WAKE_DATAGRAM_BYTES as u64;
    let refill = Duration::from_secs_f64(queued as f64 / rate_bytes_per_sec.max(1) as f64);
    (DEBT_LIMIT + refill).saturating_mul(CAP_HOLD_VALIDITY_MARGIN)
}

impl CapHolds {
    /// Records for an egress cap of `cap_rate` bytes per second, or for no
    /// cap.
    pub(crate) fn new(cap_rate: Option<u64>) -> Self {
        Self {
            validity: cap_rate.map(cap_hold_validity).unwrap_or_default(),
            ..Self::default()
        }
    }

    /// The cap answered a poll `Pending`; `already_held` if the poller was
    /// already waiting.
    fn waiting(&self, already_held: bool) {
        if !already_held {
            self.held_now.fetch_add(1, Ordering::AcqRel);
        }
        // A count read only for change: no ordering needed.
        self.holds.fetch_add(1, Ordering::Relaxed);
    }

    fn release(&self) {
        self.held_now.fetch_sub(1, Ordering::AcqRel);
    }

    /// The hold events so far, whether a poller is held now, and how long a
    /// hold stays valid with no new event.
    pub(crate) fn read(&self) -> CapHoldReading {
        let holds = self.holds.load(Ordering::Relaxed);
        CapHoldReading {
            holds,
            held_now: self.held_now.load(Ordering::Acquire) > 0,
            validity: self.validity,
        }
    }
}

/// The QUIC endpoint's socket. See the module docs.
pub(crate) struct QuicSocket<S> {
    socket: Arc<DemuxSocket<S>>,
    /// Transmits the egress cap holds, shared with every connection's path.
    cap_holds: Arc<CapHolds>,
    /// Only quinn's endpoint driver receives, one call at a time; the lock
    /// is never contended.
    ingress: parking_lot::Mutex<QuicIngressReceiver>,
    local_addr: SocketAddr,
    /// Sends that failed with an error other than `WouldBlock`.
    send_errors: AtomicU64,
    /// Whether the ingress queue's sending side is gone has been logged.
    ingress_closed_logged: AtomicBool,
    /// Told of a panic caught here.
    health: Arc<EndpointHealth>,
    /// The endpoint driver's waker from its last receive, woken when the
    /// adapter catches a panic so the driver comes back to receive, gets the
    /// error and ends (it might otherwise wait for a datagram that never
    /// comes, since the endpoint's connections let their addresses go).
    receive_waker: futures::task::AtomicWaker,
    /// Test hooks: faults injected into quinn's drivers.
    #[cfg(test)]
    faults: Arc<SocketFaults>,
}

/// Test hooks: faults injected into quinn's drivers through this socket.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct SocketFaults {
    /// What the endpoint's next receive does (`RECEIVE_*`).
    receive: std::sync::atomic::AtomicU8,
    waker: parking_lot::Mutex<Option<std::task::Waker>>,
    /// What the next write-readiness poll does (`WRITABLE_*`).
    writable: std::sync::atomic::AtomicU8,
    /// Write-readiness polls that reached the adapter's panicking hook.
    writable_panics: AtomicU64,
    /// Every send panics in the adapter's own code.
    panic_sends: AtomicBool,
    /// Every send returns `WouldBlock`, as when the socket's buffer or the
    /// egress cap is short.
    block_sends: AtomicBool,
    /// Sends that reached the adapter's panicking hook.
    send_panics: AtomicU64,
}

#[cfg(test)]
const RECEIVE_OK: u8 = 0;
#[cfg(test)]
const RECEIVE_ERROR: u8 = 1;
/// A panic in the adapter's own code, which it catches.
#[cfg(test)]
const RECEIVE_PANIC: u8 = 2;
/// A panic that unwinds through quinn, as one in quinn's own code would.
#[cfg(test)]
const RECEIVE_PANIC_IN_QUINN: u8 = 3;
/// Every write-readiness poll panics in the adapter's own code.
#[cfg(test)]
const WRITABLE_PANIC: u8 = 1;
/// The next write-readiness poll panics in the adapter's own code, once.
#[cfg(test)]
const WRITABLE_PANIC_ONCE: u8 = 2;
/// Panics of a lasting write-readiness fault after which its hook reports
/// `Pending` instead (tests only; see `QuicPoller::poll_ready`).
#[cfg(test)]
const WRITABLE_PANIC_LIMIT: u64 = 1000;

#[cfg(test)]
impl SocketFaults {
    /// Fail the endpoint's next receive with a socket error quinn's
    /// endpoint driver cannot get past, the way a socket that fails for
    /// good would end it, and wake the driver so it makes one.
    pub(crate) fn fail_receive(&self) {
        self.trip_receive(RECEIVE_ERROR);
    }

    /// Panic in the endpoint's next receive, in the adapter's own code.
    pub(crate) fn panic_receive(&self) {
        self.trip_receive(RECEIVE_PANIC);
    }

    /// Panic in the endpoint's next receive so that it unwinds through
    /// quinn's endpoint driver while it holds the endpoint's state lock.
    pub(crate) fn panic_receive_in_quinn(&self) {
        self.trip_receive(RECEIVE_PANIC_IN_QUINN);
    }

    fn trip_receive(&self, fault: u8) {
        self.receive.store(fault, Ordering::Release);
        if let Some(waker) = self.waker.lock().take() {
            waker.wake();
        }
    }

    /// Panic in EVERY write-readiness poll from now on, in the adapter's
    /// own code (a fault that does not go away).
    pub(crate) fn panic_every_writable(&self) {
        self.writable.store(WRITABLE_PANIC, Ordering::Release);
    }

    /// Panic in the NEXT write-readiness poll only, in the adapter's own
    /// code.
    pub(crate) fn panic_next_writable(&self) {
        self.writable.store(WRITABLE_PANIC_ONCE, Ordering::Release);
    }

    /// Write-readiness polls that panicked.
    pub(crate) fn writable_panics(&self) -> u64 {
        self.writable_panics.load(Ordering::Acquire)
    }

    /// Panic in EVERY send from now on, in the adapter's own code.
    pub(crate) fn panic_every_send(&self) {
        self.panic_sends.store(true, Ordering::Release);
    }

    /// Make every send return `WouldBlock` from now on.
    pub(crate) fn block_every_send(&self) {
        self.block_sends.store(true, Ordering::Release);
    }

    /// Sends that panicked.
    pub(crate) fn send_panics(&self) -> u64 {
        self.send_panics.load(Ordering::Acquire)
    }
}

impl<S> fmt::Debug for QuicSocket<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuicSocket")
            .field("local_addr", &self.local_addr)
            .field("send_errors", &self.send_errors.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl<S: Socket> QuicSocket<S> {
    /// The transmits the egress cap holds, for the paths' watches.
    pub(crate) fn cap_holds(&self) -> Arc<CapHolds> {
        self.cap_holds.clone()
    }

    pub(crate) fn new(
        socket: Arc<DemuxSocket<S>>,
        ingress: QuicIngressReceiver,
        local_addr: SocketAddr,
        health: Arc<EndpointHealth>,
    ) -> Self {
        let cap_holds = Arc::new(CapHolds::new(
            socket.egress_cap().map(|cap| cap.rate_bytes_per_sec()),
        ));
        Self {
            socket,
            cap_holds,
            ingress: parking_lot::Mutex::new(ingress),
            local_addr,
            send_errors: AtomicU64::new(0),
            ingress_closed_logged: AtomicBool::new(false),
            health,
            receive_waker: futures::task::AtomicWaker::new(),
            #[cfg(test)]
            faults: Arc::default(),
        }
    }

    /// The hooks that inject faults through this socket (tests).
    #[cfg(test)]
    pub(crate) fn faults(&self) -> Arc<SocketFaults> {
        self.faults.clone()
    }

    /// Report a panic the adapter caught, and wake the endpoint driver so
    /// its next receive returns the error that ends it.
    fn panicked(&self, call: &'static str, payload: &(dyn std::any::Any + Send)) {
        self.health.adapter_panicked(call, payload);
        self.receive_waker.wake();
    }

    /// Run `call` with a panic in it caught: reported, and turned into an
    /// I/O error (see the module docs).
    fn caught<R>(&self, call: &'static str, f: impl FnOnce() -> io::Result<R>) -> io::Result<R> {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
            Ok(result) => result,
            Err(payload) => {
                self.panicked(call, &*payload);
                Err(adapter_panicked_error())
            }
        }
    }

    /// Sends that failed with an error other than `WouldBlock`.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn send_errors(&self) -> u64 {
        self.send_errors.load(Ordering::Relaxed)
    }

    /// The address quinn sees for a datagram from `from`: on an IPv6 socket
    /// an IPv4 peer is written `::ffff:a.b.c.d`, the form quinn uses for the
    /// addresses it connects to there, so its replies and its connection
    /// lookup agree.
    fn quinn_addr(&self, from: SocketAddr) -> SocketAddr {
        match from {
            SocketAddr::V4(v4) if self.local_addr.is_ipv6() => {
                SocketAddr::new(v4.ip().to_ipv6_mapped().into(), v4.port())
            }
            addr @ (SocketAddr::V4(_) | SocketAddr::V6(_)) => addr,
        }
    }

    fn count_send_error(&self, error: &io::Error, target: SocketAddr) {
        let errors = self.send_errors.fetch_add(1, Ordering::Relaxed) + 1;
        // The first failure, then one line per 1024, so a socket that fails
        // every send cannot flood the log.
        if errors == 1 || errors % 1024 == 0 {
            tracing::warn!(
                %error,
                %target,
                send_errors = errors,
                "QUIC datagram send failed; counted and dropped (quinn retransmits it)"
            );
        }
    }
}

impl<S: Socket> AsyncUdpSocket for QuicSocket<S> {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        Box::pin(QuicPoller {
            socket: self,
            waiter: std::sync::Mutex::new(AdmissionWaiter::default()),
            held: false,
        })
    }

    /// Sends one datagram, or returns `WouldBlock` when it cannot go now:
    /// the egress cap is short of its size (nothing is charged) or the
    /// socket's send buffer is full (the cap's bytes are given back). Every
    /// other error is counted and reported to quinn as sent, because quinn's
    /// connection driver stops the connection on a send error; a datagram
    /// lost here is retransmitted like one lost on the path. The exception
    /// is a panic, caught here, and every send after one: those return the
    /// error, to end the driver (see the module docs).
    fn try_send(&self, transmit: &Transmit) -> io::Result<()> {
        // A connection driver polls write readiness before every send, and
        // that check refuses first. The endpoint driver does not: quinn's
        // `respond` (stateless reset, version negotiation, Retry, a close
        // for a refused attempt) calls this directly, so this check is what
        // keeps those off the wire after a panic.
        if self.health.has_adapter_panicked() {
            return Err(adapter_panicked_error());
        }
        self.caught("socket send", || self.send_one(transmit))
    }

    /// Returns queued datagrams one per call. Never returns an error while
    /// the endpoint is healthy: quinn's endpoint driver stops on any receive
    /// error other than `ConnectionReset`, and nothing here is one it should
    /// stop for. Once the queue's sending side is gone (the demux socket was
    /// dropped with the transport) it stays `Pending`. Once the adapter
    /// caught a panic it returns an error, which ends the endpoint driver
    /// (see the module docs).
    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        #[cfg(test)]
        if self
            .faults
            .receive
            .compare_exchange(
                RECEIVE_PANIC_IN_QUINN,
                RECEIVE_OK,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            panic!("injected panic in the QUIC endpoint driver, unwinding through quinn");
        }
        // Register before reading the flag: a panic caught on another
        // thread between the two then wakes this waker, so the driver comes
        // back and sees the flag. (Not testable deterministically: the tests
        // run on one thread.)
        self.receive_waker.register(cx.waker());
        if self.health.has_adapter_panicked() {
            return Poll::Ready(Err(adapter_panicked_error()));
        }
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.receive_one(cx, bufs, meta)
        })) {
            Ok(polled) => polled,
            Err(payload) => {
                self.panicked("socket receive", &*payload);
                Poll::Ready(Err(adapter_panicked_error()))
            }
        }
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.local_addr)
    }

    /// One datagram per transmit: no GSO, and no request larger than one
    /// datagram ever reaches the egress cap.
    fn max_transmit_segments(&self) -> usize {
        1
    }

    fn max_receive_segments(&self) -> usize {
        1
    }

    /// Reported as `true` so quinn does not run path MTU discovery even if
    /// the transport configuration asked for it; it does not.
    fn may_fragment(&self) -> bool {
        true
    }
}

/// The error the adapter returns once it caught a panic.
fn adapter_panicked_error() -> io::Error {
    io::Error::other("the QUIC socket adapter panicked; QUIC is off until the node restarts")
}

impl<S: Socket> QuicSocket<S> {
    /// [`AsyncUdpSocket::try_send`], below the panic catch.
    fn send_one(&self, transmit: &Transmit) -> io::Result<()> {
        #[cfg(test)]
        if self.faults.panic_sends.load(Ordering::Acquire) {
            self.faults.send_panics.fetch_add(1, Ordering::AcqRel);
            panic!("injected panic in the QUIC socket adapter (send)");
        }
        #[cfg(test)]
        if self.faults.block_sends.load(Ordering::Acquire) {
            return Err(io::ErrorKind::WouldBlock.into());
        }
        debug_assert!(
            transmit.segment_size.is_none(),
            "max_transmit_segments is 1, so quinn sends one datagram per transmit"
        );
        // The socket below maps an IPv4 target back to `::ffff:` form for
        // an IPv6 socket, and keys its metering by the plain form.
        let target = normalize_mapped_addr(transmit.destination);
        let sent = match self.socket.egress_cap() {
            None => self.socket.try_send_to(transmit.contents, target),
            Some(cap) => {
                // One datagram per transmit keeps every take within the
                // burst (at least 4 datagrams), so the bucket's bound holds
                // exactly and the take can always be admitted eventually.
                debug_assert!(
                    transmit.contents.len() as u64 <= cap.burst_bytes(),
                    "a {} byte transmit is larger than the egress burst",
                    transmit.contents.len()
                );
                match cap.try_take(transmit.contents.len(), Duration::ZERO) {
                    Ok(take) => self
                        .socket
                        .try_send_to_paced(transmit.contents, target, take),
                    Err(_) => return Err(io::ErrorKind::WouldBlock.into()),
                }
            }
        };
        match sent {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => Err(e),
            Err(e) => {
                self.count_send_error(&e, target);
                Ok(())
            }
        }
    }

    /// [`AsyncUdpSocket::poll_recv`], below the panic catch.
    fn receive_one(
        &self,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        let (Some(buf), Some(slot)) = (bufs.first_mut(), meta.first_mut()) else {
            return Poll::Ready(Ok(0));
        };
        #[cfg(test)]
        {
            match self.faults.receive.load(Ordering::Acquire) {
                RECEIVE_ERROR => {
                    return Poll::Ready(Err(io::Error::other("injected receive failure")));
                }
                RECEIVE_PANIC => {
                    self.faults.receive.store(RECEIVE_OK, Ordering::Release);
                    panic!("injected panic in the QUIC socket adapter (receive)");
                }
                _ => {}
            }
            *self.faults.waker.lock() = Some(cx.waker().clone());
        }
        let datagram = match self.ingress.lock().poll_recv(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Some(datagram)) => datagram,
            Poll::Ready(None) => {
                if !self.ingress_closed_logged.swap(true, Ordering::Relaxed) {
                    tracing::debug!(
                        "QUIC ingress queue closed; the endpoint receives nothing more"
                    );
                }
                return Poll::Pending;
            }
        };
        // The listener reads into a buffer of MAX_RECV_PACKET_SIZE, and
        // quinn's buffers are at least MAX_UDP_PAYLOAD; a longer datagram
        // would be malformed for quinn anyway, so a cut one is dropped there.
        let len = datagram.bytes.len().min(buf.len());
        buf[..len].copy_from_slice(&datagram.bytes[..len]);
        *slot = RecvMeta {
            addr: self.quinn_addr(datagram.from),
            len,
            stride: len,
            ecn: None,
            dst_ip: None,
        };
        Poll::Ready(Ok(1))
    }
}

/// quinn's write-readiness check for one connection: ready when the socket
/// can take a send and the egress cap holds a full datagram.
struct QuicPoller<S> {
    socket: Arc<QuicSocket<S>>,
    /// Behind a mutex only so the poller is `Sync`, which quinn requires;
    /// `poll_writable` has `&mut self` and uses `get_mut`, so it is never
    /// locked.
    waiter: std::sync::Mutex<AdmissionWaiter>,
    /// The cap is holding this poller's transmit ([`CapHolds`]).
    held: bool,
}

impl<S> QuicPoller<S> {
    /// Record the cap's answer to this poll ([`CapHolds`]).
    fn record_admission(&mut self, waiting: bool) {
        if waiting {
            self.socket.cap_holds.waiting(self.held);
        } else if self.held {
            self.socket.cap_holds.release();
        }
        self.held = waiting;
    }
}

impl<S> Drop for QuicPoller<S> {
    fn drop(&mut self) {
        if self.held {
            self.socket.cap_holds.release();
        }
    }
}

impl<S> fmt::Debug for QuicPoller<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuicPoller").finish_non_exhaustive()
    }
}

impl<S: Socket> UdpPoller for QuicPoller<S> {
    /// A panic is caught and returned as an I/O error, which ends the
    /// connection's driver (see the module docs). Never `Ready(Ok)`: quinn
    /// would then try to send, and a fault that persists would be hit on
    /// every transmit. Once the adapter caught a panic, every poll returns
    /// the error without running the adapter's code.
    fn poll_writable(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let socket = this.socket.clone();
        if socket.health.has_adapter_panicked() {
            return Poll::Ready(Err(adapter_panicked_error()));
        }
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| this.poll_ready(cx))) {
            Ok(polled) => polled,
            Err(payload) => {
                socket.panicked("socket write readiness", &*payload);
                Poll::Ready(Err(adapter_panicked_error()))
            }
        }
    }
}

impl<S: Socket> QuicPoller<S> {
    /// [`UdpPoller::poll_writable`], below the panic catch.
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        #[cfg(test)]
        {
            let fault = self.socket.faults.writable.load(Ordering::Acquire);
            if fault == WRITABLE_PANIC_ONCE {
                self.socket.faults.writable.store(0, Ordering::Release);
            }
            // Past this many panics the hook reports Pending instead, so a
            // test of a driver that loops on the fault ends and can count
            // the loops.
            if fault == WRITABLE_PANIC
                && self.socket.faults.writable_panics.load(Ordering::Acquire)
                    >= WRITABLE_PANIC_LIMIT
            {
                return Poll::Pending;
            }
            if fault == WRITABLE_PANIC || fault == WRITABLE_PANIC_ONCE {
                self.socket
                    .faults
                    .writable_panics
                    .fetch_add(1, Ordering::AcqRel);
                panic!("injected panic in the QUIC socket adapter (write readiness)");
            }
        }
        let this = self;
        let demux = &this.socket.socket;
        match demux.poll_send_ready(cx) {
            Poll::Pending => return Poll::Pending,
            // A socket error is reported by the send that follows, which
            // counts it and carries on; returning it here would stop the
            // connection.
            Poll::Ready(_) => {}
        }
        let Some(cap) = demux.egress_cap() else {
            return Poll::Ready(Ok(()));
        };
        // A poisoned lock still holds a usable waiter: it is only a timer.
        let waiter = match this.waiter.get_mut() {
            Ok(waiter) => waiter,
            Err(poisoned) => poisoned.into_inner(),
        };
        let admitted = cap.poll_admit(usize::from(MAX_UDP_PAYLOAD), waiter, cx);
        this.record_admission(admitted.is_pending());
        admitted.map(Ok)
    }
}

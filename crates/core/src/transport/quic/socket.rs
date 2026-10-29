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
//! without looping, and every later receive returns one too, so the endpoint
//! driver ends at its next receive. See `super::liveness` for what is and is
//! not contained.

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

/// The QUIC endpoint's socket. See the module docs.
pub(crate) struct QuicSocket<S> {
    socket: Arc<DemuxSocket<S>>,
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

    /// Write-readiness polls that panicked.
    pub(crate) fn writable_panics(&self) -> u64 {
        self.writable_panics.load(Ordering::Acquire)
    }

    /// Panic in EVERY send from now on, in the adapter's own code.
    pub(crate) fn panic_every_send(&self) {
        self.panic_sends.store(true, Ordering::Release);
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
    pub(crate) fn new(
        socket: Arc<DemuxSocket<S>>,
        ingress: QuicIngressReceiver,
        local_addr: SocketAddr,
        health: Arc<EndpointHealth>,
    ) -> Self {
        Self {
            socket,
            ingress: parking_lot::Mutex::new(ingress),
            local_addr,
            send_errors: AtomicU64::new(0),
            ingress_closed_logged: AtomicBool::new(false),
            health,
            #[cfg(test)]
            faults: Arc::default(),
        }
    }

    /// The hooks that inject faults through this socket (tests).
    #[cfg(test)]
    pub(crate) fn faults(&self) -> Arc<SocketFaults> {
        self.faults.clone()
    }

    /// Run `call` with a panic in it caught: reported, and turned into an
    /// I/O error (see the module docs).
    fn caught<R>(&self, call: &'static str, f: impl FnOnce() -> io::Result<R>) -> io::Result<R> {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
            Ok(result) => result,
            Err(payload) => {
                self.health.adapter_panicked(call, &*payload);
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
        })
    }

    /// Sends one datagram, or returns `WouldBlock` when it cannot go now:
    /// the egress cap is short of its size (nothing is charged) or the
    /// socket's send buffer is full (the cap's bytes are given back). Every
    /// other error is counted and reported to quinn as sent, because quinn's
    /// connection driver stops the connection on a send error; a datagram
    /// lost here is retransmitted like one lost on the path.
    fn try_send(&self, transmit: &Transmit) -> io::Result<()> {
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
        if self.health.has_adapter_panicked() {
            return Poll::Ready(Err(adapter_panicked_error()));
        }
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.receive_one(cx, bufs, meta)
        })) {
            Ok(polled) => polled,
            Err(payload) => {
                self.health.adapter_panicked("socket receive", &*payload);
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
    /// every transmit.
    fn poll_writable(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let socket = this.socket.clone();
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| this.poll_ready(cx))) {
            Ok(polled) => polled,
            Err(payload) => {
                socket
                    .health
                    .adapter_panicked("socket write readiness", &*payload);
                Poll::Ready(Err(adapter_panicked_error()))
            }
        }
    }
}

impl<S: Socket> QuicPoller<S> {
    /// [`UdpPoller::poll_writable`], below the panic catch.
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        #[cfg(test)]
        if self.socket.faults.writable.load(Ordering::Acquire) == WRITABLE_PANIC {
            self.socket
                .faults
                .writable_panics
                .fetch_add(1, Ordering::AcqRel);
            panic!("injected panic in the QUIC socket adapter (write readiness)");
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
        cap.poll_admit(usize::from(MAX_UDP_PAYLOAD), waiter, cx)
            .map(Ok)
    }
}

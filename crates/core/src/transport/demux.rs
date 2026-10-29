//! Splits inbound datagrams by their first byte, so a QUIC endpoint can share
//! the UDP transport's port (#5940).
//!
//! Every packet the UDP transport accepts starts with a plaintext type byte,
//! [`PACKET_TYPE_INTRO`] or [`PACKET_TYPE_SYMMETRIC`]. Every QUIC packet has
//! the fixed bit or the long-header bit set in its first byte. The two sets
//! of first bytes do not overlap, so the first byte alone says which protocol
//! a datagram belongs to.
//!
//! QUIC-class datagrams go to an optional QUIC ingress queue, which a QUIC
//! endpoint reads (#5940), and only from source addresses on the ingress
//! allow-set. Everything else that is not legacy is counted and dropped.

use std::io;
use std::net::SocketAddr;
#[cfg(test)]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};

use dashmap::DashMap;
use tokio::sync::mpsc;

use super::crypto::{PACKET_TYPE_INTRO, PACKET_TYPE_SYMMETRIC};
use super::egress_cap::{EgressCap, EgressTake};
use super::metrics::{DemuxClass, QuicIngressOutcome, TRANSPORT_METRICS};
use super::packet_data::MAX_RECV_PACKET_SIZE;
use super::{Socket, SocketRead, normalize_mapped_addr};

/// Bits of a QUIC packet's first byte of which at least one is always set.
///
/// `0x80` is the header form bit (RFC 9000 section 17.2: set on every long
/// header packet). `0x40` is the fixed bit (RFC 9000 sections 17.2 and 17.3:
/// set on every long header packet and every short header packet, except
/// Version Negotiation, which has the form bit set instead). The rule is
/// exact only while the QUIC endpoint never clears the fixed bit, so QUIC bit
/// greasing (RFC 9287) must stay disabled on it.
const QUIC_FIRST_BYTE_MASK: u8 = 0xC0;

/// How many non-legacy datagrams in a row are dropped before the receive loop
/// yields to the runtime. `tokio::net::UdpSocket` already takes part in
/// cooperative budgeting, so this only matters for sockets that can return
/// ready forever without yielding, such as the in-memory simulation socket
/// with a deep inbox.
const DROPS_BETWEEN_YIELDS: u32 = 64;

/// How many datagrams the QUIC ingress queue holds before it drops.
pub(crate) const QUIC_INGRESS_QUEUE_DATAGRAMS: usize = 1024;

/// How many datagrams from one source address the QUIC ingress queue holds
/// at once: a quarter of it, so one allowed source (or a sender spoofing an
/// allowed address) that fills its share leaves the rest of the queue to the
/// other connections. Datagrams over the share are dropped and counted as
/// `quic_ingress_source_full`.
pub(crate) const QUIC_INGRESS_PER_SOURCE_DATAGRAMS: usize = QUIC_INGRESS_QUEUE_DATAGRAMS / 4;

/// How many distinct source addresses the QUIC ingress allow-set holds. One
/// entry per peer with a QUIC connection (or one being set up), so this is
/// far above any `max-number-of-connections` in use; past it,
/// [`DemuxSocket::allow_quic_source`] refuses.
pub(crate) const QUIC_ALLOWED_SOURCES_MAX: usize = 4096;

/// The most memory the QUIC ingress queue can hold: every slot holding a
/// datagram of the largest size the listener receives (its read buffer is
/// `MAX_RECV_PACKET_SIZE`, so no read is longer), plus the slot itself.
/// The bound assumes that buffer: a caller reading into a larger one could
/// queue longer datagrams. One queue per node, and only while a QUIC ingress
/// is enabled. The allow-set is not counted here; its size is up to the
/// caller. Declared in `contract::executor::declared_cache_ceiling`, which
/// only tests read.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const QUIC_INGRESS_QUEUE_BYTES: usize =
    QUIC_INGRESS_QUEUE_DATAGRAMS * (MAX_RECV_PACKET_SIZE + std::mem::size_of::<QuicDatagram>());

/// A QUIC-class datagram passed to the QUIC ingress: its bytes and its source
/// address (IPv4-mapped IPv6 addresses already normalized to IPv4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QuicDatagram {
    pub bytes: Vec<u8>,
    pub from: SocketAddr,
}

/// The state of a QUIC ingress shared by the socket (the sending side), the
/// receiving side and the allow-set permits.
#[derive(Default)]
struct IngressShared {
    /// Allowed source addresses, each with the number of live
    /// [`QuicSourcePermit`]s for it. At most [`QUIC_ALLOWED_SOURCES_MAX`].
    allowed: DashMap<SocketAddr, usize>,
    /// `allowed.len()`, kept separately so a new address reserves its place
    /// atomically (see [`DemuxSocket::allow_quic_source`]).
    allowed_count: AtomicUsize,
    /// Datagrams queued and not yet received, by source. An entry exists only
    /// while its count is above zero, so this holds at most
    /// [`QUIC_INGRESS_QUEUE_DATAGRAMS`] entries.
    queued: DashMap<SocketAddr, usize>,
    /// Set by [`DemuxSocket::disable_quic_ingress`]: nothing is allowed or
    /// queued any more, and every QUIC-class datagram is `no_ingress`.
    disabled: AtomicBool,
}

/// The QUIC ingress of one [`DemuxSocket`]: the queue's sending half and the
/// state it shares with the receiving half.
struct QuicIngress {
    queue: mpsc::Sender<QuicDatagram>,
    shared: Arc<IngressShared>,
}

/// The receiving half of a QUIC ingress queue (see
/// [`DemuxSocket::enable_quic_ingress`]). Receiving a datagram frees its slot
/// in its source's share of the queue.
pub(crate) struct QuicIngressReceiver {
    queue: mpsc::Receiver<QuicDatagram>,
    shared: Arc<IngressShared>,
}

impl QuicIngressReceiver {
    /// Poll for the next queued datagram. `Ready(None)` once the socket that
    /// fills the queue is gone.
    pub(crate) fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<QuicDatagram>> {
        let polled = self.queue.poll_recv(cx);
        if let Poll::Ready(Some(datagram)) = &polled {
            self.release(datagram.from);
        }
        polled
    }

    fn release(&self, from: SocketAddr) {
        self.shared.queued.remove_if_mut(&from, |_, queued| {
            *queued = queued.saturating_sub(1);
            *queued == 0
        });
    }

    #[cfg(test)]
    pub(crate) fn try_recv(&mut self) -> Result<QuicDatagram, mpsc::error::TryRecvError> {
        let datagram = self.queue.try_recv()?;
        self.release(datagram.from);
        Ok(datagram)
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.queue.len()
    }
}

/// Keeps one source address on a QUIC ingress allow-set: the address is
/// allowed while at least one permit for it is alive. Returned by
/// [`DemuxSocket::allow_quic_source`]; the QUIC endpoint holds one for each
/// connection and drops it when the connection ends.
#[must_use = "the source is allowed only while the permit is alive"]
pub(crate) struct QuicSourcePermit {
    shared: Arc<IngressShared>,
    addr: SocketAddr,
}

impl std::fmt::Debug for QuicSourcePermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuicSourcePermit")
            .field("addr", &self.addr)
            .finish()
    }
}

impl QuicSourcePermit {
    /// The (normalized) address this permit allows.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn addr(&self) -> SocketAddr {
        self.addr
    }
}

impl Drop for QuicSourcePermit {
    fn drop(&mut self) {
        let removed = self.shared.allowed.remove_if_mut(&self.addr, |_, permits| {
            *permits = permits.saturating_sub(1);
            *permits == 0
        });
        if removed.is_some() {
            self.shared.allowed_count.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

/// Classify a datagram by its first byte. An empty datagram is
/// [`DemuxClass::Other`].
pub(crate) fn classify(packet: &[u8]) -> DemuxClass {
    match packet.first() {
        Some(&PACKET_TYPE_INTRO | &PACKET_TYPE_SYMMETRIC) => DemuxClass::Legacy,
        Some(first) if first & QUIC_FIRST_BYTE_MASK != 0 => DemuxClass::Quic,
        _ => DemuxClass::Other,
    }
}

/// A [`Socket`] that hands the listener only the datagrams of the UDP
/// transport and counts the rest.
///
/// It is public because [`create_connection_handler`] returns handlers over
/// `DemuxSocket<S>`; the transport binds it itself, so callers do not need
/// to construct it.
///
/// It exists so a QUIC endpoint can use the same UDP port as the UDP
/// transport (#5940). QUIC-shaped datagrams are counted as `demux_quic` and
/// dropped, or queued for the QUIC ingress when one is enabled (see below);
/// datagrams that are neither are dropped and counted as `demux_other`. Legacy datagrams are returned
/// unchanged, and are counted as `demux_legacy` so the ratio is visible.
///
/// It wraps the socket below the [`Socket`] trait, so every implementation of
/// the trait (the real UDP sockets and `SimulationSocket`) gets the same
/// behaviour.
///
/// It is also where outbound datagrams are charged to the node-wide egress
/// cap, when `total-bandwidth-limit` is set (see `transport::egress_cap`):
/// every datagram the transport sends goes through this socket.
///
/// # QUIC ingress
///
/// [`Self::enable_quic_ingress`] gives the socket a bounded queue of
/// QUIC-class datagrams for a QUIC endpoint. The listener stays the only
/// reader of the UDP socket and fills the queue with `try_reserve`, so a slow
/// or stopped endpoint costs dropped QUIC datagrams and never stalls the
/// listener. A datagram is queued only if its source address is on the
/// allow-set ([`Self::allow_quic_source`]); the rest are dropped as before,
/// so a prober sending QUIC Initials gets no answer that says the port
/// speaks QUIC. Each QUIC-class datagram is counted once by what happened to
/// it (`quic_ingress_<label>`, see [`QuicIngressOutcome`]). Without an
/// ingress every one is `no_ingress`, which is every node today.
///
/// [`create_connection_handler`]: super::create_connection_handler
pub struct DemuxSocket<S> {
    inner: S,
    egress: Option<EgressCap>,
    quic: OnceLock<QuicIngress>,
    /// Datagrams consumed here since the last legacy one, for the yield in
    /// `recv_classified`. Only the listener reads, so relaxed is enough.
    diverted_in_a_row: AtomicU32,
    #[cfg(test)]
    quic_outcomes: [AtomicU64; QuicIngressOutcome::COUNT],
}

impl<S> DemuxSocket<S> {
    fn new(inner: S, egress: Option<EgressCap>) -> Self {
        Self {
            inner,
            egress,
            quic: OnceLock::new(),
            diverted_in_a_row: AtomicU32::new(0),
            #[cfg(test)]
            quic_outcomes: Default::default(),
        }
    }

    /// Charge every datagram sent through this socket to `cap`.
    pub(crate) fn with_egress_cap(mut self, cap: Option<EgressCap>) -> Self {
        self.egress = cap;
        self
    }

    /// Wrap an already-bound socket. Test-only: the transport binds through
    /// [`Socket::bind`].
    #[cfg(test)]
    pub(crate) fn wrap(inner: S, egress: Option<EgressCap>) -> Self {
        Self::new(inner, egress)
    }

    /// Start passing allowed QUIC-class datagrams to a queue of
    /// [`QUIC_INGRESS_QUEUE_DATAGRAMS`], and return its receiving half.
    /// The allow-set starts empty. `None` if an ingress was already enabled.
    pub(crate) fn enable_quic_ingress(&self) -> Option<QuicIngressReceiver> {
        let (queue, receiver) = mpsc::channel(QUIC_INGRESS_QUEUE_DATAGRAMS);
        let shared = Arc::new(IngressShared::default());
        self.quic
            .set(QuicIngress {
                queue,
                shared: shared.clone(),
            })
            .ok()?;
        Some(QuicIngressReceiver {
            queue: receiver,
            shared,
        })
    }

    /// [`Self::enable_quic_ingress`] and hand the receiver to `build`; if
    /// `build` fails (the QUIC endpoint could not be built), disable the
    /// ingress again, so the socket is left as with QUIC off instead of
    /// holding an allow-set and a queue whose receiver is gone. `None` if an
    /// ingress was already enabled.
    pub(crate) fn with_quic_ingress<R, E>(
        &self,
        build: impl FnOnce(QuicIngressReceiver) -> Result<R, E>,
    ) -> Option<Result<R, E>> {
        let receiver = self.enable_quic_ingress()?;
        let built = build(receiver);
        if built.is_err() {
            self.disable_quic_ingress();
        }
        Some(built)
    }

    /// Stop the QUIC ingress for good: refuse new permits, empty the
    /// allow-set, and count every QUIC-class datagram as `no_ingress`.
    /// Permits still held release nothing (their entries are gone).
    fn disable_quic_ingress(&self) {
        if let Some(ingress) = self.quic.get() {
            let shared = &ingress.shared;
            shared.disabled.store(true, Ordering::Release);
            shared.allowed.clear();
            shared.allowed_count.store(0, Ordering::Release);
        }
    }

    /// The enabled ingress, unless it was disabled.
    fn live_quic_ingress(&self) -> Option<&QuicIngress> {
        self.quic
            .get()
            .filter(|ingress| !ingress.shared.disabled.load(Ordering::Acquire))
    }

    /// Let QUIC-class datagrams from `addr` reach the ingress queue while
    /// the returned permit is alive. `None` if no ingress is enabled, or if
    /// the allow-set already holds [`QUIC_ALLOWED_SOURCES_MAX`] other
    /// addresses. Several permits for one address keep it allowed until the
    /// last is dropped.
    pub(crate) fn allow_quic_source(&self, addr: SocketAddr) -> Option<QuicSourcePermit> {
        let ingress = self.live_quic_ingress()?;
        let addr = normalize_mapped_addr(addr);
        let shared = &ingress.shared;
        match shared.allowed.entry(addr) {
            dashmap::Entry::Occupied(mut held) => *held.get_mut() += 1,
            dashmap::Entry::Vacant(free) => {
                // Reserve a place before inserting, while the entry's shard
                // is locked: concurrent callers cannot both take the last.
                shared
                    .allowed_count
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                        (count < QUIC_ALLOWED_SOURCES_MAX).then_some(count + 1)
                    })
                    .ok()?;
                free.insert(1);
            }
        }
        Some(QuicSourcePermit {
            shared: shared.clone(),
            addr,
        })
    }

    /// Another permit for `addr` if it is on the allow-set now, in one step
    /// under the entry's lock, so a permit dropped concurrently cannot be
    /// brought back. `None` if it is not allowed.
    pub(crate) fn allow_existing_quic_source(&self, addr: SocketAddr) -> Option<QuicSourcePermit> {
        let ingress = self.live_quic_ingress()?;
        let addr = normalize_mapped_addr(addr);
        let mut held = ingress.shared.allowed.get_mut(&addr)?;
        *held += 1;
        drop(held);
        Some(QuicSourcePermit {
            shared: ingress.shared.clone(),
            addr,
        })
    }

    /// Whether QUIC-class datagrams from `addr` currently reach the ingress.
    #[cfg(test)]
    pub(crate) fn is_quic_source_allowed(&self, addr: SocketAddr) -> bool {
        self.quic.get().is_some_and(|ingress| {
            ingress
                .shared
                .allowed
                .contains_key(&normalize_mapped_addr(addr))
        })
    }

    /// Queue a QUIC-class datagram if an ingress is enabled, its source is
    /// allowed and both the queue and the source's share of it have room;
    /// count what happened to it. Never waits, and copies the bytes only
    /// when they are queued.
    fn divert_quic(&self, datagram: &[u8], from: SocketAddr) {
        let outcome = match self.live_quic_ingress() {
            None => QuicIngressOutcome::NoIngress,
            Some(ingress) if !ingress.shared.allowed.contains_key(&from) => {
                QuicIngressOutcome::NotAllowed
            }
            Some(ingress) => ingress.queue_from(datagram, from),
        };
        TRANSPORT_METRICS.record_quic_ingress(outcome);
        #[cfg(test)]
        self.quic_outcomes[outcome as usize].fetch_add(1, Ordering::Relaxed);
        #[cfg(any(test, feature = "testing"))]
        TRANSPORT_METRICS.note_quic_ingress_from(from, outcome);
    }

    /// This socket's count of QUIC-class datagrams with `outcome`.
    #[cfg(test)]
    pub(crate) fn quic_outcome_count(&self, outcome: QuicIngressOutcome) -> u64 {
        self.quic_outcomes[outcome as usize].load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn inner_for_test(&self) -> &S {
        &self.inner
    }
}

impl QuicIngress {
    /// Queue a datagram from an allowed source, within the source's share.
    /// The share is counted before the slot is taken and given back if the
    /// queue refuses, so a datagram that is not queued never holds a share.
    fn queue_from(&self, datagram: &[u8], from: SocketAddr) -> QuicIngressOutcome {
        {
            let mut queued = self.shared.queued.entry(from).or_insert(0);
            if *queued >= QUIC_INGRESS_PER_SOURCE_DATAGRAMS {
                return QuicIngressOutcome::SourceFull;
            }
            *queued += 1;
        }
        let refused = match self.queue.try_reserve() {
            Ok(permit) => {
                permit.send(QuicDatagram {
                    bytes: datagram.to_vec(),
                    from,
                });
                return QuicIngressOutcome::Forwarded;
            }
            Err(mpsc::error::TrySendError::Full(())) => QuicIngressOutcome::QueueFull,
            Err(mpsc::error::TrySendError::Closed(())) => QuicIngressOutcome::NoIngress,
        };
        self.shared.queued.remove_if_mut(&from, |_, queued| {
            *queued = queued.saturating_sub(1);
            *queued == 0
        });
        refused
    }
}

impl<S: Socket> DemuxSocket<S> {
    /// Sends without waiting a datagram whose bytes a sender already took
    /// from the egress cap (the QUIC endpoint), and returns what the
    /// datagram did not use; on any error, `WouldBlock` included, all of the
    /// take is returned. Charged once, by the take, never again here.
    pub(crate) fn try_send_to_paced(
        &self,
        buf: &[u8],
        target: SocketAddr,
        take: EgressTake<'_>,
    ) -> io::Result<usize> {
        match self.inner.try_send_to(buf, target) {
            Ok(sent) => {
                take.sent(sent);
                Ok(sent)
            }
            Err(e) => {
                drop(take);
                Err(e)
            }
        }
    }
}

impl<S: Socket> Socket for DemuxSocket<S> {
    async fn bind(addr: SocketAddr) -> io::Result<Self> {
        Ok(Self::new(S::bind(addr).await?, None))
    }

    /// Returns only on a legacy datagram or an error; see
    /// [`Self::recv_classified`], which the listener uses instead.
    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        loop {
            if let SocketRead::Datagram(len, from) = self.recv_classified(buf).await? {
                return Ok((len, from));
            }
        }
    }

    /// One read of the inner socket. A legacy datagram is returned as
    /// [`SocketRead::Datagram`]; any other is counted, queued for the QUIC
    /// ingress or dropped, and returned as [`SocketRead::Diverted`], so the
    /// listener sees that the socket delivered something. Errors from the
    /// inner socket are returned unchanged, because the listener's receive
    /// error tracking depends on them.
    ///
    /// After [`DROPS_BETWEEN_YIELDS`] diverted datagrams in a row it yields
    /// to the runtime once before the next read. Cancellation-safe when the
    /// inner `recv_from` is: the only await points are the yield, before the
    /// read, and the read itself, and the rest is synchronous.
    async fn recv_classified(&self, buf: &mut [u8]) -> io::Result<SocketRead> {
        if self.diverted_in_a_row.load(Ordering::Relaxed) >= DROPS_BETWEEN_YIELDS {
            tokio::task::yield_now().await;
            // Reset after the yield completes: if the future is dropped while
            // the yield is pending, the next call yields again.
            self.diverted_in_a_row.store(0, Ordering::Relaxed);
        }
        let (len, from) = self.inner.recv_from(buf).await?;
        let class = classify(&buf[..len]);
        TRANSPORT_METRICS.record_demux(from, class);
        match class {
            DemuxClass::Legacy => {
                self.diverted_in_a_row.store(0, Ordering::Relaxed);
                return Ok(SocketRead::Datagram(len, from));
            }
            DemuxClass::Quic => self.divert_quic(&buf[..len], from),
            DemuxClass::Other => {}
        }
        self.diverted_in_a_row.fetch_add(1, Ordering::Relaxed);
        Ok(SocketRead::Diverted)
    }

    /// Sends the datagram at once and, when it was sent, charges the bytes
    /// sent to the egress cap, if there is one: a send that reaches this
    /// method is never paced. The charge may put the cap into debt, which
    /// paced senders then wait for. A failed send is not charged.
    async fn send_to(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
        let sent = self.inner.send_to(buf, target).await?;
        if let Some(cap) = &self.egress {
            cap.charge_unpaced(sent);
        }
        Ok(sent)
    }

    fn send_to_blocking(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
        let sent = self.inner.send_to_blocking(buf, target)?;
        if let Some(cap) = &self.egress {
            cap.charge_unpaced(sent);
        }
        Ok(sent)
    }

    /// Sends without waiting and, when it was sent, charges it like
    /// [`Self::send_to`]. `WouldBlock` and other errors are returned
    /// unchanged and not charged.
    fn try_send_to(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
        let sent = self.inner.try_send_to(buf, target)?;
        if let Some(cap) = &self.egress {
            cap.charge_unpaced(sent);
        }
        Ok(sent)
    }

    fn poll_send_ready(&self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.inner.poll_send_ready(cx)
    }

    fn egress_cap(&self) -> Option<&EgressCap> {
        self.egress.as_ref()
    }

    /// Sends, then returns what the datagram did not use of the take. On a
    /// failed send the take is dropped, which returns all of it.
    async fn send_to_paced(
        &self,
        buf: &[u8],
        target: SocketAddr,
        take: EgressTake<'_>,
    ) -> io::Result<usize> {
        match self.inner.send_to(buf, target).await {
            Ok(sent) => {
                take.sent(sent);
                Ok(sent)
            }
            Err(e) => {
                drop(take);
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::Mutex;

    /// The expected class of a first byte, written out as literal ranges
    /// rather than through the constants the implementation uses.
    fn expected_class(first: u8) -> DemuxClass {
        match first {
            0x01 | 0x02 => DemuxClass::Legacy,
            0x40..=0xFF => DemuxClass::Quic,
            _ => DemuxClass::Other,
        }
    }

    #[test]
    fn classify_matches_the_rule_for_every_first_byte() {
        for first in 0..=255u8 {
            for packet in [vec![first], vec![first, 0xAA], vec![first; 1200]] {
                assert_eq!(
                    classify(&packet),
                    expected_class(first),
                    "first byte {first:#04x}, length {}",
                    packet.len()
                );
            }
        }
        assert_eq!(classify(&[]), DemuxClass::Other, "empty datagram");
    }

    /// Only the first byte matters: later bytes never change the class.
    #[test]
    fn classify_ignores_everything_after_the_first_byte() {
        assert_eq!(classify(&[0x01, 0xFF, 0xFF]), DemuxClass::Legacy);
        assert_eq!(classify(&[0x00, 0x01, 0x02]), DemuxClass::Other);
        assert_eq!(classify(&[0x3F, 0xC0, 0x40]), DemuxClass::Other);
    }

    enum Step {
        Packet(SocketAddr, Vec<u8>),
        Error(io::ErrorKind),
    }

    /// An inner socket that replays a script, then waits forever.
    struct ScriptedSocket {
        script: Mutex<VecDeque<Step>>,
        sent: Mutex<Vec<(Vec<u8>, SocketAddr)>>,
        /// `try_send_to` returns `WouldBlock` while this is set.
        send_buffer_full: std::sync::atomic::AtomicBool,
    }

    impl ScriptedSocket {
        fn new(script: Vec<Step>) -> Self {
            Self {
                script: Mutex::new(script.into()),
                sent: Mutex::new(Vec::new()),
                send_buffer_full: std::sync::atomic::AtomicBool::new(false),
            }
        }
    }

    impl Socket for ScriptedSocket {
        async fn bind(_addr: SocketAddr) -> io::Result<Self> {
            Ok(Self::new(Vec::new()))
        }

        async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
            let step = self.script.lock().unwrap().pop_front();
            match step {
                Some(Step::Packet(from, data)) => {
                    buf[..data.len()].copy_from_slice(&data);
                    Ok((data.len(), from))
                }
                Some(Step::Error(kind)) => Err(io::Error::new(kind, "scripted")),
                None => std::future::pending().await,
            }
        }

        async fn send_to(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
            self.sent.lock().unwrap().push((buf.to_vec(), target));
            Ok(buf.len())
        }

        fn send_to_blocking(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
            self.sent.lock().unwrap().push((buf.to_vec(), target));
            Ok(buf.len())
        }

        fn try_send_to(&self, buf: &[u8], target: SocketAddr) -> io::Result<usize> {
            if self.send_buffer_full.load(Ordering::SeqCst) {
                return Err(io::ErrorKind::WouldBlock.into());
            }
            self.sent.lock().unwrap().push((buf.to_vec(), target));
            Ok(buf.len())
        }
    }

    /// Receive, failing the test instead of hanging when no datagram is
    /// returned (the script runs dry and the inner socket then waits forever).
    async fn recv_within_timeout(
        socket: &DemuxSocket<ScriptedSocket>,
        buf: &mut [u8],
    ) -> (usize, SocketAddr) {
        tokio::time::timeout(std::time::Duration::from_secs(5), socket.recv_from(buf))
            .await
            .expect("a legacy datagram is returned")
            .unwrap()
    }

    #[tokio::test]
    async fn recv_returns_only_legacy_and_counts_every_datagram() {
        let from: SocketAddr = "10.254.1.1:4001".parse().unwrap();
        TRANSPORT_METRICS.watch_demux(from);
        let legacy = vec![PACKET_TYPE_SYMMETRIC, 9, 8, 7];
        let socket = DemuxSocket::wrap(
            ScriptedSocket::new(vec![
                Step::Packet(from, vec![0xC3, 1, 2, 3]),
                Step::Packet(from, vec![0x00, 1, 2, 3]),
                Step::Packet(from, Vec::new()),
                Step::Packet(from, legacy.clone()),
            ]),
            None,
        );
        let mut buf = [0u8; 64];
        let (len, got_from) = recv_within_timeout(&socket, &mut buf).await;
        assert_eq!(got_from, from);
        assert_eq!(&buf[..len], legacy.as_slice());
        assert_eq!(
            TRANSPORT_METRICS.demux_from(from),
            vec![
                DemuxClass::Quic,
                DemuxClass::Other,
                DemuxClass::Other,
                DemuxClass::Legacy
            ]
        );
    }

    /// A long run of dropped datagrams ends at the next legacy one, and the
    /// loop lets other tasks run on the way, once per yield period. The test
    /// runtime has one thread and the inner socket is always ready, so the
    /// other task only runs when the receive loop yields.
    #[tokio::test]
    async fn recv_survives_a_flood_of_dropped_datagrams() {
        let from: SocketAddr = "10.254.1.2:4002".parse().unwrap();
        TRANSPORT_METRICS.watch_demux(from);
        let flood = 3 * DROPS_BETWEEN_YIELDS as usize + 1;
        let mut script: Vec<Step> = (0..flood)
            .map(|_| Step::Packet(from, vec![0x40, 0]))
            .collect();
        script.push(Step::Packet(from, vec![PACKET_TYPE_INTRO]));
        let socket = DemuxSocket::wrap(ScriptedSocket::new(script), None);
        let turns = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = turns.clone();
        let other_task = tokio::spawn(async move {
            loop {
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                tokio::task::yield_now().await;
            }
        });
        let mut buf = [0u8; 8];
        let (len, _) = recv_within_timeout(&socket, &mut buf).await;
        assert_eq!(&buf[..len], &[PACKET_TYPE_INTRO]);
        other_task.abort();
        // The flood is three full yield periods plus one datagram, and each
        // yield gives the other task one turn.
        assert!(
            turns.load(std::sync::atomic::Ordering::SeqCst) >= 3,
            "the receive loop must yield once per {DROPS_BETWEEN_YIELDS} dropped datagrams"
        );
        let log = TRANSPORT_METRICS.demux_from(from);
        assert_eq!(log.len(), flood + 1);
        assert_eq!(
            log.iter().filter(|c| **c == DemuxClass::Quic).count(),
            flood
        );
    }

    #[tokio::test]
    async fn recv_returns_inner_errors_unchanged() {
        let socket = DemuxSocket::wrap(
            ScriptedSocket::new(vec![Step::Error(io::ErrorKind::ConnectionReset)]),
            None,
        );
        let mut buf = [0u8; 8];
        let err = socket.recv_from(&mut buf).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::ConnectionReset);
        assert_eq!(err.to_string(), "scripted");
    }

    #[tokio::test]
    async fn send_passes_through() {
        let target: SocketAddr = "10.254.1.3:4003".parse().unwrap();
        let socket = DemuxSocket::wrap(ScriptedSocket::new(Vec::new()), None);
        assert_eq!(socket.send_to(&[0xC0, 1], target).await.unwrap(), 2);
        assert_eq!(socket.send_to_blocking(&[0x01], target).unwrap(), 1);
        assert_eq!(
            *socket.inner.sent.lock().unwrap(),
            vec![(vec![0xC0, 1], target), (vec![0x01], target)]
        );
    }

    fn quic(tag: u8) -> Vec<u8> {
        vec![0xC3, tag, 0, 0]
    }

    fn outcomes(socket: &DemuxSocket<ScriptedSocket>) -> [u64; QuicIngressOutcome::COUNT] {
        QuicIngressOutcome::ALL.map(|o| socket.quic_outcome_count(o))
    }

    /// Allowed QUIC-class datagrams reach the ingress queue in arrival order,
    /// with their bytes and source, interleaved with a legacy datagram that
    /// is still returned to the caller. Each QUIC one is reported to the
    /// listener as `Diverted`.
    #[tokio::test]
    async fn quic_ingress_queues_allowed_datagrams_in_order() {
        let from: SocketAddr = "10.254.2.1:4101".parse().unwrap();
        let legacy = vec![PACKET_TYPE_SYMMETRIC, 1];
        let socket = DemuxSocket::wrap(
            ScriptedSocket::new(vec![
                Step::Packet(from, quic(1)),
                Step::Packet(from, legacy.clone()),
                Step::Packet(from, quic(2)),
                Step::Packet(from, quic(3)),
            ]),
            None,
        );
        let mut queue = socket.enable_quic_ingress().expect("first enable");
        let _permit = socket.allow_quic_source(from).expect("allowed");

        let mut buf = [0u8; 64];
        let mut reads = Vec::new();
        for _ in 0..4 {
            reads.push(socket.recv_classified(&mut buf).await.unwrap());
        }
        assert_eq!(
            reads,
            vec![
                SocketRead::Diverted,
                SocketRead::Datagram(legacy.len(), from),
                SocketRead::Diverted,
                SocketRead::Diverted,
            ]
        );
        for tag in 1..=3 {
            assert_eq!(
                queue.try_recv().unwrap(),
                QuicDatagram {
                    bytes: quic(tag),
                    from
                }
            );
        }
        assert!(queue.try_recv().is_err(), "nothing else was queued");
        assert_eq!(outcomes(&socket), [3, 0, 0, 0, 0]);
    }

    /// With the queue full and nobody reading it, QUIC-class datagrams are
    /// dropped and counted, and a legacy datagram behind them is still
    /// returned at once. Four sources fill the queue with their shares, so a
    /// fifth one's datagrams find the queue itself full. The test runs on a
    /// paused clock, so if the listener waited for room in the queue the
    /// runtime would go idle, the clock would jump to the 5 s timeout and the
    /// test would fail at once rather than after 5 real seconds.
    #[tokio::test(start_paused = true)]
    async fn quic_ingress_queue_full_drops_and_counts_without_stalling_legacy() {
        let sources: Vec<SocketAddr> = (0..5u16)
            .map(|i| SocketAddr::from(([10, 254, 2, 2], 4102 + i)))
            .collect();
        let overflow = 5;
        let shares = QUIC_INGRESS_QUEUE_DATAGRAMS / QUIC_INGRESS_PER_SOURCE_DATAGRAMS;
        assert_eq!(shares, 4, "the test fills the queue with four shares");
        let mut script: Vec<Step> = Vec::new();
        for from in &sources[..shares] {
            for i in 0..QUIC_INGRESS_PER_SOURCE_DATAGRAMS {
                script.push(Step::Packet(*from, quic(i as u8)));
            }
        }
        for i in 0..overflow {
            script.push(Step::Packet(sources[shares], quic(i as u8)));
        }
        script.push(Step::Packet(sources[0], vec![PACKET_TYPE_INTRO, 7]));
        let socket = DemuxSocket::wrap(ScriptedSocket::new(script), None);
        let queue = socket.enable_quic_ingress().expect("first enable");
        let _permits: Vec<_> = sources
            .iter()
            .map(|a| socket.allow_quic_source(*a).expect("allowed"))
            .collect();

        let mut buf = [0u8; 64];
        let (len, got_from) = recv_within_timeout(&socket, &mut buf).await;
        assert_eq!(
            (&buf[..len], got_from),
            (&[PACKET_TYPE_INTRO, 7][..], sources[0])
        );
        assert_eq!(
            outcomes(&socket),
            [
                QUIC_INGRESS_QUEUE_DATAGRAMS as u64,
                0,
                overflow as u64,
                0,
                0
            ]
        );
        assert_eq!(queue.len(), QUIC_INGRESS_QUEUE_DATAGRAMS);
    }

    /// One source cannot take more than its share of the queue: past
    /// `QUIC_INGRESS_PER_SOURCE_DATAGRAMS` queued and unread datagrams its
    /// next ones are dropped as `source_full` while another source still
    /// gets in, and reading one of its datagrams frees one slot of its share.
    #[tokio::test]
    async fn quic_ingress_limits_each_source_to_its_share() {
        let flooder: SocketAddr = "10.254.2.8:4108".parse().unwrap();
        let other: SocketAddr = "10.254.2.9:4109".parse().unwrap();
        let mut script: Vec<Step> = (0..QUIC_INGRESS_PER_SOURCE_DATAGRAMS + 3)
            .map(|i| Step::Packet(flooder, quic(i as u8)))
            .collect();
        script.push(Step::Packet(other, quic(0xEE)));
        script.push(Step::Packet(flooder, quic(0xF0)));
        script.push(Step::Packet(flooder, quic(0xF1)));
        let socket = DemuxSocket::wrap(ScriptedSocket::new(script), None);
        let mut queue = socket.enable_quic_ingress().expect("first enable");
        let _a = socket.allow_quic_source(flooder).expect("allowed");
        let _b = socket.allow_quic_source(other).expect("allowed");
        let mut buf = [0u8; 64];
        for _ in 0..QUIC_INGRESS_PER_SOURCE_DATAGRAMS + 4 {
            socket.recv_classified(&mut buf).await.unwrap();
        }
        assert_eq!(
            outcomes(&socket),
            [QUIC_INGRESS_PER_SOURCE_DATAGRAMS as u64 + 1, 0, 0, 3, 0],
            "the flooder's share filled; the other source still got in"
        );
        // Reading one of the flooder's datagrams frees one slot of its share.
        assert_eq!(queue.try_recv().unwrap().from, flooder);
        socket.recv_classified(&mut buf).await.unwrap();
        socket.recv_classified(&mut buf).await.unwrap();
        assert_eq!(
            outcomes(&socket),
            [QUIC_INGRESS_PER_SOURCE_DATAGRAMS as u64 + 2, 0, 0, 4, 0],
            "one freed slot admits exactly one more"
        );
    }

    /// A QUIC-class datagram from a source not on the allow-set is dropped
    /// and counted, including after its last permit was dropped. The
    /// allow-set compares normalized addresses, as the UDP sockets report,
    /// and an address stays allowed until its last permit is gone.
    #[tokio::test]
    async fn quic_ingress_drops_and_counts_sources_not_allowed() {
        let allowed: SocketAddr = "10.254.2.3:4103".parse().unwrap();
        let stranger: SocketAddr = "10.254.2.4:4104".parse().unwrap();
        let socket = DemuxSocket::wrap(
            ScriptedSocket::new(vec![
                Step::Packet(stranger, quic(1)),
                Step::Packet(allowed, quic(2)),
                Step::Packet(allowed, quic(3)),
                Step::Packet(allowed, quic(4)),
            ]),
            None,
        );
        let mut queue = socket.enable_quic_ingress().expect("first enable");
        // Allowed in its IPv4-mapped IPv6 form, twice.
        let first = socket
            .allow_quic_source("[::ffff:10.254.2.3]:4103".parse().unwrap())
            .expect("allowed");
        assert_eq!(
            first.addr(),
            allowed,
            "the permit holds the normalized form"
        );
        let second = socket.allow_quic_source(allowed).expect("allowed");
        let mut buf = [0u8; 64];

        assert_eq!(
            socket.recv_classified(&mut buf).await.unwrap(),
            SocketRead::Diverted
        );
        assert!(
            queue.try_recv().is_err(),
            "a stranger's datagram is not queued"
        );
        assert_eq!(outcomes(&socket), [0, 1, 0, 0, 0]);

        socket.recv_classified(&mut buf).await.unwrap();
        assert_eq!(queue.try_recv().unwrap().bytes, quic(2));

        drop(first);
        assert!(socket.is_quic_source_allowed(allowed), "one permit is left");
        socket.recv_classified(&mut buf).await.unwrap();
        assert_eq!(queue.try_recv().unwrap().bytes, quic(3));

        drop(second);
        assert!(
            !socket.is_quic_source_allowed(allowed),
            "the last permit is gone"
        );
        socket.recv_classified(&mut buf).await.unwrap();
        assert!(queue.try_recv().is_err(), "a removed source is not queued");
        assert_eq!(outcomes(&socket), [2, 2, 0, 0, 0]);
    }

    /// The allow-set holds at most `QUIC_ALLOWED_SOURCES_MAX` addresses: past
    /// it a new address is refused while one already allowed can take another
    /// permit, and dropping permits makes room again.
    #[test]
    fn quic_allow_set_is_bounded() {
        let socket = DemuxSocket::wrap(ScriptedSocket::new(Vec::new()), None);
        let _queue = socket.enable_quic_ingress().expect("first enable");
        let addr = |i: usize| SocketAddr::from(([10, 253, (i >> 8) as u8, i as u8], 5000));
        let mut permits: Vec<_> = (0..QUIC_ALLOWED_SOURCES_MAX)
            .map(|i| socket.allow_quic_source(addr(i)).expect("room"))
            .collect();
        assert!(
            socket
                .allow_quic_source(addr(QUIC_ALLOWED_SOURCES_MAX))
                .is_none(),
            "a new address past the bound is refused"
        );
        let again = socket.allow_quic_source(addr(0));
        assert!(
            again.is_some(),
            "an allowed address can take another permit"
        );
        drop(again);
        permits.pop();
        assert!(
            socket
                .allow_quic_source(addr(QUIC_ALLOWED_SOURCES_MAX))
                .is_some(),
            "a dropped permit makes room"
        );
    }

    /// Callers racing for the last places of the allow-set never take more
    /// than `QUIC_ALLOWED_SOURCES_MAX` between them: a new address reserves
    /// its place atomically. And a permit for an address already allowed is
    /// granted only while it is allowed.
    #[test]
    fn quic_allow_set_bound_holds_under_concurrent_callers() {
        let socket = Arc::new(DemuxSocket::wrap(ScriptedSocket::new(Vec::new()), None));
        let _queue = socket.enable_quic_ingress().expect("first enable");
        const THREADS: usize = 8;
        let per_thread = QUIC_ALLOWED_SOURCES_MAX / THREADS + 64;
        let barrier = Arc::new(std::sync::Barrier::new(THREADS));
        let granted: Vec<Vec<QuicSourcePermit>> = (0..THREADS)
            .map(|t| {
                let socket = socket.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    (0..per_thread)
                        .filter_map(|i| {
                            let n = t * per_thread + i;
                            socket.allow_quic_source(SocketAddr::from((
                                [10, 252, (n >> 8) as u8, n as u8],
                                5000,
                            )))
                        })
                        .collect()
                })
            })
            .collect::<Vec<_>>()
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect();
        let total: usize = granted.iter().map(Vec::len).sum();
        assert_eq!(
            total, QUIC_ALLOWED_SOURCES_MAX,
            "exactly the bound, never more"
        );

        let addr: SocketAddr = "10.252.255.254:6000".parse().unwrap();
        assert!(
            socket.allow_existing_quic_source(addr).is_none(),
            "not allowed, so no permit"
        );
        drop(granted);
        let first = socket.allow_quic_source(addr).expect("room again");
        let second = socket
            .allow_existing_quic_source(addr)
            .expect("allowed now");
        drop(first);
        drop(second);
        assert!(
            socket.allow_existing_quic_source(addr).is_none(),
            "a released address is not brought back"
        );
    }

    /// Without an ingress (every node until a QUIC endpoint runs) every
    /// QUIC-class datagram counts as `no_ingress`, the allow-set cannot be
    /// written, and a dropped receiver counts the same. Other datagrams are
    /// not counted as QUIC at all.
    #[tokio::test]
    async fn quic_datagrams_without_an_ingress_count_as_no_ingress() {
        let from: SocketAddr = "10.254.2.5:4105".parse().unwrap();
        let socket = DemuxSocket::wrap(
            ScriptedSocket::new(vec![
                Step::Packet(from, quic(1)),
                Step::Packet(from, vec![0x00]),
                Step::Packet(from, quic(2)),
            ]),
            None,
        );
        assert!(
            socket.allow_quic_source(from).is_none(),
            "no ingress, no allow-set"
        );
        let mut buf = [0u8; 64];
        socket.recv_classified(&mut buf).await.unwrap();
        socket.recv_classified(&mut buf).await.unwrap();
        assert_eq!(outcomes(&socket), [0, 0, 0, 0, 1]);

        let queue = socket.enable_quic_ingress().expect("first enable");
        assert!(socket.enable_quic_ingress().is_none(), "enabled only once");
        let _permit = socket.allow_quic_source(from).expect("allowed");
        drop(queue);
        socket.recv_classified(&mut buf).await.unwrap();
        assert_eq!(
            outcomes(&socket),
            [0, 0, 0, 0, 2],
            "a closed queue is no ingress"
        );
    }

    /// When the QUIC endpoint cannot be built after the ingress was enabled
    /// (`with_quic_ingress` with a failing build), the ingress is disabled:
    /// the allow-set is emptied and refuses new entries, every QUIC-class
    /// datagram counts as `no_ingress`, and the ingress cannot be enabled a
    /// second time. A build that succeeds leaves it working.
    #[tokio::test]
    async fn a_failed_endpoint_build_disables_the_ingress() {
        let from: SocketAddr = "10.254.2.7:4107".parse().unwrap();
        let socket =
            DemuxSocket::wrap(ScriptedSocket::new(vec![Step::Packet(from, quic(1))]), None);
        let mut held = None;
        let built: Result<(), &str> = socket
            .with_quic_ingress(|receiver| {
                held = Some(
                    socket
                        .allow_quic_source(from)
                        .expect("allowed while building"),
                );
                drop(receiver);
                Err("the endpoint failed")
            })
            .expect("first enable");
        assert!(built.is_err());
        assert!(!socket.is_quic_source_allowed(from), "allow-set emptied");
        assert!(socket.allow_quic_source(from).is_none(), "and closed");
        assert!(socket.allow_existing_quic_source(from).is_none());
        drop(held);
        let mut buf = [0u8; 2048];
        socket.recv_classified(&mut buf).await.unwrap();
        assert_eq!(outcomes(&socket), [0, 0, 0, 0, 1], "no ingress");
        assert!(socket.with_quic_ingress(|_| Ok::<(), ()>(())).is_none());

        let working = DemuxSocket::wrap(ScriptedSocket::new(Vec::new()), None);
        let receiver = working
            .with_quic_ingress(Ok::<_, ()>)
            .expect("first enable")
            .expect("built");
        assert!(working.allow_quic_source(from).is_some());
        drop(receiver);
    }

    /// `try_send_to` returns the inner socket's `WouldBlock` unchanged and
    /// charges nothing for it; a send that goes through is charged once,
    /// unpaced, like `send_to`.
    #[test]
    fn try_send_to_passes_would_block_through_and_charges_only_sends() {
        let target: SocketAddr = "10.254.2.6:4106".parse().unwrap();
        let cap = EgressCap::new(1_000_000, crate::simulation::VirtualTime::new());
        let socket = DemuxSocket::wrap(ScriptedSocket::new(Vec::new()), Some(cap));
        socket.inner.send_buffer_full.store(true, Ordering::SeqCst);
        let err = socket.try_send_to(&[0xC0; 100], target).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
        let stats = socket.egress_cap().unwrap().stats();
        assert_eq!(stats.bytes, 0, "a send that did not happen is not charged");
        assert!(socket.inner.sent.lock().unwrap().is_empty());

        socket.inner.send_buffer_full.store(false, Ordering::SeqCst);
        assert_eq!(socket.try_send_to(&[0xC0; 100], target).unwrap(), 100);
        let stats = socket.egress_cap().unwrap().stats();
        assert_eq!(stats.bytes, 100);
        assert_eq!(
            stats.packets[crate::transport::metrics::EgressDecision::Unpaced as usize],
            1
        );
        assert_eq!(
            *socket.inner.sent.lock().unwrap(),
            vec![(vec![0xC0; 100], target)]
        );
    }

    /// The default `try_send_to` of a socket that does not implement it
    /// says so instead of sending or blocking.
    #[test]
    fn default_try_send_to_is_unsupported() {
        struct Minimal;
        impl Socket for Minimal {
            async fn bind(_addr: SocketAddr) -> io::Result<Self> {
                Ok(Self)
            }
            async fn recv_from(&self, _buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
                std::future::pending().await
            }
            async fn send_to(&self, buf: &[u8], _target: SocketAddr) -> io::Result<usize> {
                Ok(buf.len())
            }
            fn send_to_blocking(&self, buf: &[u8], _target: SocketAddr) -> io::Result<usize> {
                Ok(buf.len())
            }
        }
        let err = Minimal
            .try_send_to(&[1], "10.254.2.7:4107".parse().unwrap())
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Unsupported);
    }

    /// Through the real bind path of the transport: a QUIC-shaped datagram
    /// sent to a bound transport is counted and dropped, and a legacy
    /// connection between two transports still establishes and carries a
    /// message. Fails if the transport binds its socket without the wrapper,
    /// because nothing would then be counted for either sender.
    #[tokio::test]
    async fn real_transport_counts_quic_and_still_connects() -> anyhow::Result<()> {
        use super::super::{TransportKeypair, UdpTransportSocket, create_connection_handler};
        use std::net::{IpAddr, Ipv4Addr};
        use std::time::Duration;

        async fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
            while !done() {
                assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }

        let host = IpAddr::V4(Ipv4Addr::LOCALHOST);
        // Ports come from the crate's test port registry, which keeps each
        // one bound until it is released. Each is released immediately before
        // the handler that binds it, so the window in which another process
        // could take it is only that call.
        let gateway_port = crate::test_utils::reserve_local_port()?;
        let joiner_port = crate::test_utils::reserve_local_port()?;
        let gateway_addr = SocketAddr::new(host, gateway_port);
        let joiner_addr = SocketAddr::new(host, joiner_port);
        TRANSPORT_METRICS.watch_demux(joiner_addr);
        let gateway_keypair = TransportKeypair::new();
        let gateway_pub = gateway_keypair.public().clone();

        crate::test_utils::release_local_port(gateway_port);
        let (_gateway_out, mut gateway_in, _gateway_task) =
            create_connection_handler::<UdpTransportSocket>(
                gateway_keypair,
                host,
                gateway_port,
                true,
                None,
                None,
                None,
                None,
                None,
            )
            .await?;
        crate::test_utils::release_local_port(joiner_port);
        let (mut joiner_out, _joiner_in, _joiner_task) =
            create_connection_handler::<UdpTransportSocket>(
                TransportKeypair::new(),
                host,
                joiner_port,
                false,
                None,
                None,
                None,
                None,
                None,
            )
            .await?;

        // A QUIC-shaped datagram and a stray one, from a unique address.
        let injector = std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))?;
        let injector_addr = injector.local_addr()?;
        TRANSPORT_METRICS.watch_demux(injector_addr);
        injector.send_to(&[0xC1, 0, 0, 0], gateway_addr)?;
        injector.send_to(&[0x00, 0, 0, 0], gateway_addr)?;
        wait_until("the injected datagrams are counted", || {
            TRANSPORT_METRICS.demux_from(injector_addr).len() == 2
        })
        .await;
        assert_eq!(
            TRANSPORT_METRICS.demux_from(injector_addr),
            vec![DemuxClass::Quic, DemuxClass::Other]
        );

        // A legacy connection still works.
        let connecting = joiner_out.connect(gateway_pub, gateway_addr).await;
        let accepting = gateway_in.next_connection();
        let (joiner_conn, gateway_conn) = tokio::time::timeout(Duration::from_secs(15), async {
            tokio::join!(connecting, accepting)
        })
        .await?;
        let mut joiner_conn = joiner_conn?;
        let mut gateway_conn = gateway_conn.expect("the gateway accepts the connection");
        joiner_conn.send("through the demux").await?;
        let received = tokio::time::timeout(Duration::from_secs(10), gateway_conn.recv()).await??;
        assert_eq!(&received[8..], b"through the demux");

        let seen = TRANSPORT_METRICS.demux_from(joiner_addr);
        assert!(!seen.is_empty(), "the joiner's packets were counted");
        assert!(
            seen.iter().all(|c| *c == DemuxClass::Legacy),
            "every joiner packet is legacy: {seen:?}"
        );
        Ok(())
    }
}

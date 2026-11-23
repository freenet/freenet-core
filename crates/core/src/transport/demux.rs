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
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::AtomicU64;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
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

/// How many datagrams of the QUIC ingress queue admitted-but-unpromoted
/// native joiners (`quic::admission`) may hold, all of them together: a
/// sixteenth of the queue. A joiner's source is on the allow-set from its
/// admission, so without this it would take the per-source share
/// ([`QUIC_INGRESS_PER_SOURCE_DATAGRAMS`], 256) for the up to 8 s its
/// handshake and `Hello` may last, and the per-address admission limit is
/// keyed on IP and port, so one host on several ports could fill the queue
/// down to the quiet reserve. The share is for all unpromoted joiners
/// together, whatever their addresses, so established peers keep the other
/// 960 slots. An honest join exchanges a few dozen datagrams over its whole
/// window, and the global admission rate (50 a second) admits that many
/// joiners in one second, so this holds the handshake flights of a burst
/// without letting joiners crowd the queue; a joiner datagram over it is
/// dropped and the QUIC handshake retransmits it. A joiner never uses the
/// quiet reserve. A source stops counting as a joiner when its
/// [`QuicJoinerMark`] is dropped: at promotion or the end of the join.
pub(crate) const QUIC_INGRESS_JOINER_DATAGRAMS: usize = QUIC_INGRESS_QUEUE_DATAGRAMS / 16;

const _: () = assert!(
    QUIC_INGRESS_JOINER_DATAGRAMS <= QUIC_INGRESS_PER_SOURCE_DATAGRAMS
        && QUIC_INGRESS_JOINER_DATAGRAMS
            <= QUIC_INGRESS_QUEUE_DATAGRAMS - QUIC_INGRESS_QUIET_RESERVE
);

/// The last slots of the QUIC ingress queue, kept for sources that have fewer
/// than [`QUIC_INGRESS_QUIET_SOURCE_DATAGRAMS`] datagrams queued. Once only
/// these are free, a datagram from a source holding more is dropped and
/// counted as `reserve_full`. So sources that are bursting can fill at most
/// `QUIC_INGRESS_QUEUE_DATAGRAMS - QUIC_INGRESS_QUIET_RESERVE` (896) slots
/// between them at any moment (a source that drains below the mark counts as
/// quiet again), and a quiet connection's datagram (a loss probe, an
/// acknowledgement) still finds room. The cost is 12.5% less burst room for
/// busy sources. With about 256 or more connections each holding a few
/// datagrams, most traffic counts as quiet and the queue is close to a plain
/// FIFO. Without it, when the endpoint drained
/// the queue more slowly than busy connections filled it, a quiet
/// connection's probes kept arriving at a full queue. quinn doubled its probe
/// timeout after each loss, and the connection went silent for 5 to 11 s
/// (e5b-9's real-time hold run, 256 connections into one node, debug build).
pub(crate) const QUIC_INGRESS_QUIET_RESERVE: usize = QUIC_INGRESS_QUEUE_DATAGRAMS / 8;

/// A source with fewer than this many datagrams queued may use
/// [`QUIC_INGRESS_QUIET_RESERVE`].
pub(crate) const QUIC_INGRESS_QUIET_SOURCE_DATAGRAMS: usize = 4;

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
/// is enabled, with the queue for unknown sources beside it
/// (`quic::admission::UNKNOWN_SOURCE_QUEUE_DATAGRAMS`, empty unless a
/// gateway admits native joins). The allow-set is not counted here; its size
/// is up to the caller. Declared in `contract::executor::declared_cache_ceiling`,
/// which only tests read.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const QUIC_INGRESS_QUEUE_BYTES: usize = (QUIC_INGRESS_QUEUE_DATAGRAMS
    + super::quic::admission::UNKNOWN_SOURCE_QUEUE_DATAGRAMS)
    * (MAX_RECV_PACKET_SIZE + std::mem::size_of::<QuicDatagram>());

/// A QUIC-class datagram passed to the QUIC ingress: its bytes and its source
/// address (IPv4-mapped IPv6 addresses already normalized to IPv4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QuicDatagram {
    pub bytes: Vec<u8>,
    pub from: SocketAddr,
    /// Whether it holds a slot of the joiners' share of the queue
    /// ([`QUIC_INGRESS_JOINER_DATAGRAMS`]), fixed when it was queued, so the
    /// slot is given back even if its source was promoted since.
    pub(crate) joiner_slot: bool,
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
    /// Set by [`DemuxSocket::enable_unknown_source_lane`]: a datagram from a
    /// source not on the allow-set that can open a connection goes to the
    /// unknown-source queue instead of being dropped (`quic::admission`).
    unknown_lane: AtomicBool,
    /// Allowed sources that are admitted native joiners, not yet promoted,
    /// each with the number of live joiner permits for it.
    joiners: DashMap<SocketAddr, usize>,
    /// Datagrams queued and not yet received that hold a slot of the
    /// joiners' share ([`QUIC_INGRESS_JOINER_DATAGRAMS`]).
    joiner_queued: AtomicUsize,
}

/// The QUIC ingress of one [`DemuxSocket`]: the queue's sending half and the
/// state it shares with the receiving half.
struct QuicIngress {
    queue: mpsc::Sender<QuicDatagram>,
    /// Datagrams from sources not on the allow-set, while the unknown-source
    /// lane is on (`quic::admission`).
    unknown: mpsc::Sender<QuicDatagram>,
    shared: Arc<IngressShared>,
}

/// The receiving half of a QUIC ingress queue (see
/// [`DemuxSocket::enable_quic_ingress`]). Receiving a datagram frees its slot
/// in its source's share of the queue.
pub(crate) struct QuicIngressReceiver {
    queue: mpsc::Receiver<QuicDatagram>,
    /// The unknown-source queue, read only when `queue` has nothing ready.
    unknown: mpsc::Receiver<QuicDatagram>,
    shared: Arc<IngressShared>,
}

impl QuicIngressReceiver {
    /// Poll for the next queued datagram: one from the established queue if
    /// any is ready, else one from the unknown-source queue (empty unless a
    /// gateway admits native joins), so established traffic is served first.
    /// `Ready(None)` once the socket that fills the queues is gone.
    pub(crate) fn poll_recv(&mut self, cx: &mut Context<'_>) -> Poll<Option<QuicDatagram>> {
        match self.queue.poll_recv(cx) {
            Poll::Ready(Some(datagram)) => {
                self.release(&datagram);
                Poll::Ready(Some(datagram))
            }
            Poll::Ready(None) => Poll::Ready(None),
            // Both queues register the waker, so a datagram on either wakes
            // the reader. An unknown-source datagram holds no share.
            Poll::Pending => match self.unknown.poll_recv(cx) {
                Poll::Ready(Some(datagram)) => Poll::Ready(Some(datagram)),
                Poll::Ready(None) | Poll::Pending => Poll::Pending,
            },
        }
    }

    fn release(&self, datagram: &QuicDatagram) {
        if datagram.joiner_slot {
            self.shared.joiner_queued.fetch_sub(1, Ordering::AcqRel);
        }
        self.shared
            .queued
            .remove_if_mut(&datagram.from, |_, queued| {
                *queued = queued.saturating_sub(1);
                *queued == 0
            });
    }

    #[cfg(test)]
    pub(crate) fn try_recv(&mut self) -> Result<QuicDatagram, mpsc::error::TryRecvError> {
        let datagram = self.queue.try_recv()?;
        self.release(&datagram);
        Ok(datagram)
    }

    /// The next unknown-source datagram, if one is queued (tests).
    #[cfg(test)]
    pub(crate) fn try_recv_unknown(&mut self) -> Result<QuicDatagram, mpsc::error::TryRecvError> {
        self.unknown.try_recv()
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

/// Marks an allowed source as an unpromoted native joiner while it lives
/// ([`DemuxSocket::allow_quic_joiner`]); dropping it is promotion (or the
/// end of the join), after which the source's datagrams take the established
/// share. Datagrams already queued keep the slots they took.
#[must_use = "the source counts as a joiner only while the mark is alive"]
pub(crate) struct QuicJoinerMark {
    shared: Arc<IngressShared>,
    addr: SocketAddr,
}

impl Drop for QuicJoinerMark {
    fn drop(&mut self) {
        self.shared.unmark_joiner(self.addr);
    }
}

impl IngressShared {
    fn unmark_joiner(&self, addr: SocketAddr) {
        self.joiners.remove_if_mut(&addr, |_, marks| {
            *marks = marks.saturating_sub(1);
            *marks == 0
        });
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

/// First byte of a QUIC-native punch datagram (#5940, design 2.4): 9 bytes,
/// this byte then 8 random ones, sent by both sides of a native connection to
/// open their NATs before the QUIC handshake. It is classified
/// [`DemuxClass::Other`] like any byte that is neither the UDP transport's nor
/// QUIC-shaped, so the receiver drops and counts it (`demux_other`); it is
/// reserved here so no later datagram type takes it. Checked at compile time
/// against both other classes.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const PUNCH_FIRST_BYTE: u8 = 0x03;

/// Length of a punch datagram: [`PUNCH_FIRST_BYTE`] and 8 random bytes.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const PUNCH_LEN: usize = 9;

const _: () = {
    assert!(
        PUNCH_FIRST_BYTE & QUIC_FIRST_BYTE_MASK == 0,
        "a punch is not QUIC-shaped"
    );
    assert!(
        PUNCH_FIRST_BYTE != PACKET_TYPE_INTRO && PUNCH_FIRST_BYTE != PACKET_TYPE_SYMMETRIC,
        "a punch is not a UDP transport packet"
    );
};

/// Classify a datagram by its first byte. An empty datagram is
/// [`DemuxClass::Other`].
/// Whether `packet` is a legacy UDP intro, one of the packets that open a
/// UDP connection. Test-only, for the simulation's intro delay.
#[cfg(any(test, feature = "testing"))]
pub(crate) fn is_legacy_intro(packet: &[u8]) -> bool {
    packet.first() == Some(&PACKET_TYPE_INTRO)
}

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
    /// The QUIC ingress, once enabled. Replaced only when the node rebuilds
    /// its QUIC endpoint (`quic::rebuild`, [`Self::with_replaced_quic_ingress`]);
    /// read once per QUIC-class datagram.
    quic: parking_lot::RwLock<Option<Arc<QuicIngress>>>,
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
            quic: parking_lot::RwLock::new(None),
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
        let (unknown, unknown_receiver) =
            mpsc::channel(super::quic::admission::UNKNOWN_SOURCE_QUEUE_DATAGRAMS);
        let shared = Arc::new(IngressShared::default());
        {
            let mut slot = self.quic.write();
            if slot.is_some() {
                return None;
            }
            *slot = Some(Arc::new(QuicIngress {
                queue,
                unknown,
                shared: shared.clone(),
            }));
        }
        Some(QuicIngressReceiver {
            queue: receiver,
            unknown: unknown_receiver,
            shared,
        })
    }

    /// Pass a datagram from a source not on the allow-set to the ingress's
    /// unknown-source queue when it can open a connection, instead of
    /// dropping it (`quic::admission`, a gateway admitting native joins).
    /// Whether an ingress was there to take it.
    pub(crate) fn enable_unknown_source_lane(&self) -> bool {
        match self.live_quic_ingress().as_deref() {
            Some(ingress) => {
                ingress.shared.unknown_lane.store(true, Ordering::Release);
                true
            }
            None => false,
        }
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

    /// [`Self::with_quic_ingress`] for an endpoint built to replace one that
    /// died (`quic::rebuild`): the ingress the dead endpoint read is disabled
    /// (its allow-set emptied, its permits releasing nothing) and dropped, so
    /// the dead endpoint's receiver ends, and a new one, with an empty
    /// allow-set and empty queues, takes its place. If `build` fails the new
    /// ingress is disabled too, leaving the socket as with QUIC off. `None`,
    /// with nothing changed, if no ingress was ever enabled: a rebuild only
    /// replaces an endpoint the node had.
    pub(crate) fn with_replaced_quic_ingress<R, E>(
        &self,
        build: impl FnOnce(QuicIngressReceiver) -> Result<R, E>,
    ) -> Option<Result<R, E>> {
        let receiver = {
            let mut slot = self.quic.write();
            let old = slot.take()?;
            disable(&old.shared);
            let (queue, receiver) = mpsc::channel(QUIC_INGRESS_QUEUE_DATAGRAMS);
            let (unknown, unknown_receiver) =
                mpsc::channel(super::quic::admission::UNKNOWN_SOURCE_QUEUE_DATAGRAMS);
            let shared = Arc::new(IngressShared::default());
            *slot = Some(Arc::new(QuicIngress {
                queue,
                unknown,
                shared: shared.clone(),
            }));
            QuicIngressReceiver {
                queue: receiver,
                unknown: unknown_receiver,
                shared,
            }
        };
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
        if let Some(ingress) = self.quic.read().as_deref() {
            disable(&ingress.shared);
        }
    }

    /// The enabled ingress, unless it was disabled.
    fn live_quic_ingress(&self) -> Option<Arc<QuicIngress>> {
        self.quic
            .read()
            .as_ref()
            .filter(|ingress| !ingress.shared.disabled.load(Ordering::Acquire))
            .cloned()
    }

    /// The ingress, enabled or disabled (tests).
    #[cfg(test)]
    fn ingress_for_test(&self) -> Option<Arc<QuicIngress>> {
        self.quic.read().clone()
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

    /// [`Self::allow_quic_source`] for an admitted native joiner
    /// (`quic::admission`), with the mark that makes it one: until the mark
    /// is dropped (the joiner is promoted, or its join ends), the datagrams
    /// from `addr` share [`QUIC_INGRESS_JOINER_DATAGRAMS`] queue slots with
    /// every other joiner instead of taking the per-source share.
    pub(crate) fn allow_quic_joiner(
        &self,
        addr: SocketAddr,
    ) -> Option<(QuicSourcePermit, QuicJoinerMark)> {
        let shared = self.live_quic_ingress()?.shared.clone();
        let addr = normalize_mapped_addr(addr);
        // Marked BEFORE the address is allowed: a datagram is classified only
        // once its source is on the allow-set, so none can be taken as
        // established in between and use the per-source share. The mark's
        // drop is the rollback if the allow-set refuses (and a source not on
        // the allow-set takes the unknown-source lane, which never reads
        // the mark).
        *shared.joiners.entry(addr).or_insert(0) += 1;
        let mark = QuicJoinerMark { shared, addr };
        let permit = self.allow_quic_source(addr)?;
        Some((permit, mark))
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
        self.ingress_for_test().is_some_and(|ingress| {
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
        // The read guard, not a clone of the `Arc`: one lock per datagram.
        let slot = self.quic.read();
        let live = slot
            .as_deref()
            .filter(|ingress| !ingress.shared.disabled.load(Ordering::Acquire));
        let outcome = match live {
            None => QuicIngressOutcome::NoIngress,
            Some(ingress) if !ingress.shared.allowed.contains_key(&from) => {
                if ingress.shared.unknown_lane.load(Ordering::Acquire) {
                    ingress.queue_unknown(datagram, from)
                } else {
                    QuicIngressOutcome::NotAllowed
                }
            }
            Some(ingress) => ingress.queue_from(datagram, from),
        };
        drop(slot);
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

    /// Datagrams holding a slot of the joiners' share (tests).
    #[cfg(test)]
    fn joiner_queued_for_test(&self) -> usize {
        self.ingress_for_test().map_or(0, |ingress| {
            ingress.shared.joiner_queued.load(Ordering::Acquire)
        })
    }

    #[cfg(test)]
    pub(crate) fn inner_for_test(&self) -> &S {
        &self.inner
    }
}

/// Disable an ingress: see [`DemuxSocket::disable_quic_ingress`].
fn disable(shared: &IngressShared) {
    shared.disabled.store(true, Ordering::Release);
    shared.allowed.clear();
    shared.allowed_count.store(0, Ordering::Release);
}

impl QuicIngress {
    /// Queue a datagram from a source not on the allow-set if it can open a
    /// connection and the unknown-source queue has room. It counts as
    /// `forwarded` when queued and `not_allowed` otherwise, and its fate in
    /// detail as `quic_gateway_admission_<label>`.
    fn queue_unknown(&self, datagram: &[u8], from: SocketAddr) -> QuicIngressOutcome {
        use super::metrics::GatewayAdmission;
        let admitted = match super::quic::admission::unknown_source_datagram(datagram) {
            Err(refused) => refused.outcome(),
            Ok(()) => match self.unknown.try_reserve() {
                Ok(permit) => {
                    permit.send(QuicDatagram {
                        bytes: datagram.to_vec(),
                        from,
                        joiner_slot: false,
                    });
                    GatewayAdmission::Queued
                }
                Err(
                    mpsc::error::TrySendError::Full(()) | mpsc::error::TrySendError::Closed(()),
                ) => GatewayAdmission::QueueFull,
            },
        };
        TRANSPORT_METRICS.record_gateway_admission(admitted);
        if admitted == GatewayAdmission::Queued {
            QuicIngressOutcome::Forwarded
        } else {
            QuicIngressOutcome::NotAllowed
        }
    }

    /// Queue a datagram from an allowed source, within the source's share
    /// and outside [`QUIC_INGRESS_QUIET_RESERVE`] unless the source is quiet.
    /// The share is counted before the slot is taken and given back if the
    /// queue refuses, so a datagram that is not queued never holds a share.
    /// The listener is the only sender, so the free slots read here cannot
    /// shrink before the `try_reserve` below; the receiver can only add some.
    fn queue_from(&self, datagram: &[u8], from: SocketAddr) -> QuicIngressOutcome {
        let joiner = self.shared.joiners.contains_key(&from);
        {
            let mut queued = self.shared.queued.entry(from).or_insert(0);
            if *queued >= QUIC_INGRESS_PER_SOURCE_DATAGRAMS {
                return QuicIngressOutcome::SourceFull;
            }
            if (joiner || *queued >= QUIC_INGRESS_QUIET_SOURCE_DATAGRAMS)
                && self.queue.capacity() <= QUIC_INGRESS_QUIET_RESERVE
            {
                // A source that newly took an entry above holds none, so
                // a non-joiner reaches here only with a non-zero count and
                // no empty entry is left behind; a joiner never uses the
                // reserve, so its fresh entry is removed below.
                if *queued == 0 {
                    drop(queued);
                    self.shared
                        .queued
                        .remove_if(&from, |_, queued| *queued == 0);
                }
                return QuicIngressOutcome::ReserveFull;
            }
            if joiner && !self.take_joiner_slot() {
                if *queued == 0 {
                    drop(queued);
                    self.shared
                        .queued
                        .remove_if(&from, |_, queued| *queued == 0);
                }
                super::TRANSPORT_METRICS
                    .record_gateway_admission(super::metrics::GatewayAdmission::JoinerShareFull);
                return QuicIngressOutcome::SourceFull;
            }
            *queued += 1;
        }
        let refused = match self.queue.try_reserve() {
            Ok(permit) => {
                permit.send(QuicDatagram {
                    bytes: datagram.to_vec(),
                    from,
                    joiner_slot: joiner,
                });
                return QuicIngressOutcome::Forwarded;
            }
            Err(mpsc::error::TrySendError::Full(())) => QuicIngressOutcome::QueueFull,
            Err(mpsc::error::TrySendError::Closed(())) => QuicIngressOutcome::NoIngress,
        };
        if joiner {
            self.shared.joiner_queued.fetch_sub(1, Ordering::AcqRel);
        }
        self.shared.queued.remove_if_mut(&from, |_, queued| {
            *queued = queued.saturating_sub(1);
            *queued == 0
        });
        refused
    }

    /// Take one slot of the joiners' share, if one is free.
    fn take_joiner_slot(&self) -> bool {
        self.shared
            .joiner_queued
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |held| {
                (held < QUIC_INGRESS_JOINER_DATAGRAMS).then_some(held + 1)
            })
            .is_ok()
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

/// The body of the production function whose signature line starts with
/// `signature`, from `source` (a file read with `include_str!`), with comment
/// lines stripped. Only the part of the file before its test module is
/// searched, so a pin's own literals (which sit in the test module) cannot
/// satisfy it, and a renamed or missing function panics: source-scrape pins
/// fail closed (`bug-prevention-patterns.md`). The function's closing brace
/// is its first line `    }` at four-space indent (a method in an `impl`).
#[cfg(test)]
pub(crate) fn production_fn_body(source: &str, signature: &str) -> String {
    let production = source
        .split("\n#[cfg(test)]\nmod tests {")
        .next()
        .expect("the production part");
    let start = production
        .find(signature)
        .unwrap_or_else(|| panic!("`{signature}` is not in the production code"));
    let end = production[start..]
        .find("\n    }\n")
        .unwrap_or_else(|| panic!("the end of `{signature}`"));
    production[start..start + end]
        .lines()
        .filter(|line| !line.trim_start().starts_with("//"))
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::Mutex;

    /// Every first byte's expected class, by value: the UDP transport's two
    /// types, the reserved punch byte and the QUIC-shaped range, written out
    /// as literals rather than through the constants the implementation uses.
    fn expected_class(first: u8) -> DemuxClass {
        match first {
            0x01 | 0x02 => DemuxClass::Legacy,
            // PUNCH_FIRST_BYTE: dropped and counted, like the rest of Other.
            0x03 => DemuxClass::Other,
            0x40..=0xFF => DemuxClass::Quic,
            _ => DemuxClass::Other,
        }
    }

    /// The reserved punch byte is the one the table above names, and a punch
    /// of any content classifies as `Other`.
    #[test]
    fn a_punch_is_dropped_as_other_whatever_its_tail() {
        assert_eq!(PUNCH_FIRST_BYTE, 0x03);
        assert_eq!(expected_class(PUNCH_FIRST_BYTE), DemuxClass::Other);
        for tail in [[0u8; 8], [0xFF; 8], [0x01; 8], [0x40, 0, 0, 0, 0, 0, 0, 0]] {
            let mut punch = [0u8; PUNCH_LEN];
            punch[0] = PUNCH_FIRST_BYTE;
            punch[1..].copy_from_slice(&tail);
            assert_eq!(classify(&punch), DemuxClass::Other, "{punch:?}");
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
                    from,
                    joiner_slot: false,
                }
            );
        }
        assert!(queue.try_recv().is_err(), "nothing else was queued");
        assert_eq!(outcomes(&socket), [3, 0, 0, 0, 0, 0]);
    }

    /// A long-header datagram of `len` bytes with QUIC version `version`.
    fn initial(len: usize, version: u32) -> Vec<u8> {
        let mut datagram = vec![0u8; len];
        datagram[0] = 0xC3;
        datagram[1..5].copy_from_slice(&version.to_be_bytes());
        datagram
    }

    /// With the unknown-source lane on (a gateway admitting native joins),
    /// a possible Initial from a source not on the allow-set goes to its own
    /// queue and counts as `forwarded`; a short header, a runt or an unknown
    /// version is still dropped as `not_allowed`. A datagram from an allowed
    /// source takes the established queue as before. Without the lane every
    /// one of them is `not_allowed`, as before.
    #[tokio::test]
    async fn the_unknown_source_lane_takes_only_possible_initials() {
        let stranger: SocketAddr = "10.254.2.40:4140".parse().unwrap();
        let known: SocketAddr = "10.254.2.41:4141".parse().unwrap();
        let script = || {
            vec![
                Step::Packet(stranger, initial(1200, 1)),
                Step::Packet(stranger, initial(1199, 1)),
                Step::Packet(stranger, quic(9)),
                Step::Packet(stranger, initial(1200, 2)),
                Step::Packet(known, quic(1)),
            ]
        };
        let mut buf = [0u8; 2048];

        let off = DemuxSocket::wrap(ScriptedSocket::new(script()), None);
        let mut queue = off.enable_quic_ingress().expect("first enable");
        let _permit = off.allow_quic_source(known).expect("allowed");
        for _ in 0..5 {
            off.recv_classified(&mut buf).await.unwrap();
        }
        assert_eq!(outcomes(&off), [1, 4, 0, 0, 0, 0], "lane off");
        assert!(queue.try_recv_unknown().is_err());

        let on = DemuxSocket::wrap(ScriptedSocket::new(script()), None);
        let mut queue = on.enable_quic_ingress().expect("first enable");
        let _permit = on.allow_quic_source(known).expect("allowed");
        assert!(on.enable_unknown_source_lane());
        for _ in 0..5 {
            on.recv_classified(&mut buf).await.unwrap();
        }
        assert_eq!(outcomes(&on), [2, 3, 0, 0, 0, 0], "lane on");
        assert_eq!(
            queue.try_recv_unknown().unwrap(),
            QuicDatagram {
                bytes: initial(1200, 1),
                from: stranger,
                joiner_slot: false,
            }
        );
        assert!(
            queue.try_recv_unknown().is_err(),
            "only the possible Initial"
        );
        assert_eq!(
            queue.try_recv().unwrap().from,
            known,
            "established as before"
        );
    }

    /// The lane needs a live ingress, and its queue holds at most
    /// `UNKNOWN_SOURCE_QUEUE_DATAGRAMS`; the rest are dropped without
    /// stalling the listener.
    #[tokio::test(start_paused = true)]
    async fn the_unknown_source_lane_is_bounded() {
        let none = DemuxSocket::wrap(ScriptedSocket::new(vec![]), None);
        assert!(!none.enable_unknown_source_lane(), "no ingress, no lane");

        let limit = crate::transport::quic::admission::UNKNOWN_SOURCE_QUEUE_DATAGRAMS;
        let steps = (0..limit + 5)
            .map(|i| {
                Step::Packet(
                    SocketAddr::from(([10, 254, 3, (i % 250) as u8], 4200 + i as u16)),
                    initial(1200, 1),
                )
            })
            .collect();
        let socket = DemuxSocket::wrap(ScriptedSocket::new(steps), None);
        let _queue = socket.enable_quic_ingress().expect("first enable");
        assert!(socket.enable_unknown_source_lane());
        let mut buf = [0u8; 2048];
        for _ in 0..limit + 5 {
            socket.recv_classified(&mut buf).await.unwrap();
        }
        assert_eq!(outcomes(&socket), [limit as u64, 5, 0, 0, 0, 0]);
    }

    /// The endpoint is handed established traffic first: with datagrams in
    /// both queues, `poll_recv` returns every established one before any
    /// from an unknown source.
    #[tokio::test]
    async fn established_datagrams_are_served_before_unknown_ones() {
        let stranger: SocketAddr = "10.254.2.42:4142".parse().unwrap();
        let known: SocketAddr = "10.254.2.43:4143".parse().unwrap();
        let socket = DemuxSocket::wrap(
            ScriptedSocket::new(vec![
                Step::Packet(stranger, initial(1200, 1)),
                Step::Packet(known, quic(1)),
                Step::Packet(stranger, initial(1300, 1)),
                Step::Packet(known, quic(2)),
            ]),
            None,
        );
        let mut queue = socket.enable_quic_ingress().expect("first enable");
        let _permit = socket.allow_quic_source(known).expect("allowed");
        assert!(socket.enable_unknown_source_lane());
        let mut buf = [0u8; 2048];
        for _ in 0..4 {
            socket.recv_classified(&mut buf).await.unwrap();
        }
        let mut order = Vec::new();
        for _ in 0..4 {
            let datagram = std::future::poll_fn(|cx| queue.poll_recv(cx))
                .await
                .expect("queued");
            order.push((datagram.from, datagram.bytes.len()));
        }
        assert_eq!(
            order,
            vec![(known, 4), (known, 4), (stranger, 1200), (stranger, 1300)]
        );
    }

    /// With the queue full and nobody reading it, QUIC-class datagrams are
    /// dropped and counted, and a legacy datagram behind them is still
    /// returned at once. Four sources send their shares: they fill the queue
    /// up to its quiet reserve and the fourth's last datagrams are refused at
    /// the reserve; quiet sources then fill the reserve, and a last source's
    /// datagrams find the queue itself full. The test runs on a
    /// paused clock, so if the listener waited for room in the queue the
    /// runtime would go idle, the clock would jump to the 5 s timeout and the
    /// test would fail at once rather than after 5 real seconds.
    #[tokio::test(start_paused = true)]
    async fn quic_ingress_queue_full_drops_and_counts_without_stalling_legacy() {
        let sources: Vec<SocketAddr> = (0..4u16)
            .map(|i| SocketAddr::from(([10, 254, 2, 2], 4102 + i)))
            .collect();
        let fillers: Vec<SocketAddr> =
            (0..(QUIC_INGRESS_QUIET_RESERVE / QUIC_INGRESS_QUIET_SOURCE_DATAGRAMS) as u16)
                .map(|i| SocketAddr::from(([10, 254, 2, 5], 4500 + i)))
                .collect();
        let last = SocketAddr::from(([10, 254, 2, 6], 4600));
        let overflow = 5;
        let shares = QUIC_INGRESS_QUEUE_DATAGRAMS / QUIC_INGRESS_PER_SOURCE_DATAGRAMS;
        assert_eq!(shares, 4, "the test fills the queue with four shares");
        let mut script: Vec<Step> = Vec::new();
        for from in &sources {
            for i in 0..QUIC_INGRESS_PER_SOURCE_DATAGRAMS {
                script.push(Step::Packet(*from, quic(i as u8)));
            }
        }
        for from in &fillers {
            for i in 0..QUIC_INGRESS_QUIET_SOURCE_DATAGRAMS {
                script.push(Step::Packet(*from, quic(i as u8)));
            }
        }
        for i in 0..overflow {
            script.push(Step::Packet(last, quic(i as u8)));
        }
        script.push(Step::Packet(sources[0], vec![PACKET_TYPE_INTRO, 7]));
        let socket = DemuxSocket::wrap(ScriptedSocket::new(script), None);
        let queue = socket.enable_quic_ingress().expect("first enable");
        let _permits: Vec<_> = sources
            .iter()
            .chain(&fillers)
            .chain(std::iter::once(&last))
            .map(|a| socket.allow_quic_source(*a).expect("allowed"))
            .collect();

        let mut buf = [0u8; 64];
        let (len, got_from) = recv_within_timeout(&socket, &mut buf).await;
        assert_eq!(
            (&buf[..len], got_from),
            (&[PACKET_TYPE_INTRO, 7][..], sources[0])
        );
        let refused_at_mark = QUIC_INGRESS_QUIET_RESERVE as u64;
        assert_eq!(
            outcomes(&socket),
            [
                QUIC_INGRESS_QUEUE_DATAGRAMS as u64,
                0,
                overflow as u64,
                0,
                0,
                refused_at_mark
            ]
        );
        assert_eq!(queue.len(), QUIC_INGRESS_QUEUE_DATAGRAMS);
    }

    /// The quiet reserve, step by step: its sizes; a busy source refused at
    /// the 896 mark while under its share (and the share checked first); a
    /// quiet source admitted up to its fourth datagram and refused at its
    /// fifth; the reserve reopening as the queue drains; a source that drained
    /// below four admitted again at the mark; and every per-source count back
    /// to zero once the queue is empty.
    #[tokio::test]
    async fn quic_ingress_keeps_its_last_slots_for_quiet_sources() {
        assert_eq!(QUIC_INGRESS_QUIET_RESERVE, 128);
        assert_eq!(QUIC_INGRESS_QUIET_SOURCE_DATAGRAMS, 4);
        assert_eq!(
            QUIC_INGRESS_QUEUE_DATAGRAMS - QUIC_INGRESS_QUIET_RESERVE,
            896
        );
        let share = QUIC_INGRESS_PER_SOURCE_DATAGRAMS;
        let addr = |host: u8, port: u16| SocketAddr::from(([10, 254, 3, host], port));
        // Three sources at their full share and one at half: 896, the mark.
        let busy: Vec<SocketAddr> = (0..4u16).map(|i| addr(1, 4200 + i)).collect();
        let quiet = addr(2, 4300);
        let refill = addr(3, 4400);
        let mut script = Vec::new();
        for (i, from) in busy.iter().enumerate() {
            let count = if i < 3 { share } else { share / 2 };
            script.extend((0..count).map(|n| Step::Packet(*from, quic(n as u8))));
        }
        let at_mark = script.len();
        script.push(Step::Packet(busy[0], quic(0xB0))); // at its share
        script.push(Step::Packet(busy[3], quic(0xB3))); // under its share
        script.extend((0..5u8).map(|n| Step::Packet(quiet, quic(0xC0 + n))));
        let after_quiet = script.len();
        // Drained by 5 below the mark (in the test): one more busy datagram
        // gets in, the next finds the mark again.
        script.push(Step::Packet(busy[3], quic(0xB4)));
        script.push(Step::Packet(busy[3], quic(0xB5)));
        let after_reopen = script.len();
        // busy[0] drained to 3 queued (in the test), the queue refilled to
        // the mark by another source; busy[0] is quiet again: its next
        // datagram gets in, the one after is refused.
        script.extend((0..248u16).map(|n| Step::Packet(refill, quic(n as u8))));
        script.push(Step::Packet(busy[0], quic(0xB6)));
        script.push(Step::Packet(busy[0], quic(0xB7)));
        let total = script.len();
        let socket = DemuxSocket::wrap(ScriptedSocket::new(script), None);
        let mut queue = socket.enable_quic_ingress().expect("first enable");
        let _permits: Vec<_> = busy
            .iter()
            .chain([&quiet, &refill])
            .map(|a| socket.allow_quic_source(*a).expect("allowed"))
            .collect();
        async fn read(socket: &DemuxSocket<ScriptedSocket>, n: usize) {
            let mut buf = [0u8; 64];
            for _ in 0..n {
                socket.recv_classified(&mut buf).await.unwrap();
            }
        }
        read(&socket, at_mark).await;
        assert_eq!(outcomes(&socket), [896, 0, 0, 0, 0, 0]);
        read(&socket, after_quiet - at_mark).await;
        assert_eq!(
            outcomes(&socket),
            [900, 0, 0, 1, 0, 2],
            "at the mark: the share first, then the reserve; the quiet source's \
             fourth gets in and its fifth does not"
        );
        // Five read off the queue (all busy[0]'s): 895 queued, the reserve
        // is no longer all that is free.
        for _ in 0..5 {
            assert_eq!(queue.try_recv().unwrap().from, busy[0]);
        }
        read(&socket, after_reopen - after_quiet).await;
        assert_eq!(outcomes(&socket), [901, 0, 0, 1, 0, 3]);
        assert_eq!(queue.len(), 896);
        // Drain busy[0] (first in the queue) down to 3 queued.
        for _ in 0..(share - 5 - 3) {
            assert_eq!(queue.try_recv().unwrap().from, busy[0]);
        }
        assert_eq!(queue.len(), 896 - 248);
        read(&socket, total - after_reopen).await;
        assert_eq!(
            outcomes(&socket),
            [901 + 248 + 1, 0, 0, 1, 0, 4],
            "busy[0], drained below 4, is admitted at the mark once more"
        );
        assert_eq!(queue.len(), 897);
        while queue.try_recv().is_ok() {}
        let ingress = socket.ingress_for_test().expect("ingress");
        assert!(
            ingress.shared.queued.is_empty(),
            "every per-source count is back to zero"
        );
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
            [QUIC_INGRESS_PER_SOURCE_DATAGRAMS as u64 + 1, 0, 0, 3, 0, 0],
            "the flooder's share filled; the other source still got in"
        );
        // Reading one of the flooder's datagrams frees one slot of its share.
        assert_eq!(queue.try_recv().unwrap().from, flooder);
        socket.recv_classified(&mut buf).await.unwrap();
        socket.recv_classified(&mut buf).await.unwrap();
        assert_eq!(
            outcomes(&socket),
            [QUIC_INGRESS_PER_SOURCE_DATAGRAMS as u64 + 2, 0, 0, 4, 0, 0],
            "one freed slot admits exactly one more"
        );
    }

    /// `ports` joiner sources (one host, many ports) each send `each`
    /// datagrams, all admitted as joiners; then one established source sends
    /// `established`. Returns the queue, the socket and the outcome counts.
    async fn joiner_flood(
        ports: u16,
        each: usize,
        established: usize,
    ) -> (
        DemuxSocket<ScriptedSocket>,
        QuicIngressReceiver,
        Vec<(QuicSourcePermit, QuicJoinerMark)>,
        QuicSourcePermit,
    ) {
        let joiner = |port: u16| SocketAddr::from(([10, 254, 3, 1], 5000 + port));
        let peer: SocketAddr = "10.254.3.2:5999".parse().unwrap();
        let mut script = Vec::new();
        for i in 0..each {
            for port in 0..ports {
                script.push(Step::Packet(joiner(port), quic(i as u8)));
            }
        }
        for i in 0..established {
            script.push(Step::Packet(peer, quic(i as u8)));
        }
        let total = script.len();
        let socket = DemuxSocket::wrap(ScriptedSocket::new(script), None);
        let queue = socket.enable_quic_ingress().expect("first enable");
        let joiners: Vec<_> = (0..ports)
            .map(|port| socket.allow_quic_joiner(joiner(port)).expect("allowed"))
            .collect();
        let permit = socket.allow_quic_source(peer).expect("allowed");
        let mut buf = [0u8; 64];
        for _ in 0..total {
            socket.recv_classified(&mut buf).await.unwrap();
        }
        (socket, queue, joiners, permit)
    }

    /// Gate 23: however many ports one host joins from, all unpromoted
    /// joiners together hold at most `QUIC_INGRESS_JOINER_DATAGRAMS` queue
    /// slots (the rest are `source_full`), and an established peer keeps its
    /// whole per-source share whatever they do.
    #[tokio::test]
    async fn unpromoted_joiners_share_one_bounded_part_of_the_queue() {
        let (socket, mut queue, _joiners, _peer) =
            joiner_flood(40, 4, QUIC_INGRESS_PER_SOURCE_DATAGRAMS).await;
        let joiner_sent = 40 * 4;
        let forwarded = QUIC_INGRESS_JOINER_DATAGRAMS + QUIC_INGRESS_PER_SOURCE_DATAGRAMS;
        assert_eq!(
            outcomes(&socket),
            [
                forwarded as u64,
                0,
                0,
                (joiner_sent - QUIC_INGRESS_JOINER_DATAGRAMS) as u64,
                0,
                0
            ],
            "joiners hold their share and no more; the peer lost nothing"
        );
        let mut from_joiners = 0;
        let mut from_peer = 0;
        while let Ok(datagram) = queue.try_recv() {
            if datagram.from.ip() == std::net::Ipv4Addr::new(10, 254, 3, 1) {
                from_joiners += 1;
            } else {
                from_peer += 1;
            }
        }
        assert_eq!(from_joiners, QUIC_INGRESS_JOINER_DATAGRAMS);
        assert_eq!(from_peer, QUIC_INGRESS_PER_SOURCE_DATAGRAMS);
    }

    /// Reading a joiner's datagram frees one slot of the joiners' share, and
    /// promoting a joiner (dropping its mark) moves its source to the
    /// established share: its datagrams are no longer held to the joiners'
    /// 64, though the slots it already took stay until read.
    #[tokio::test]
    async fn a_read_frees_a_joiner_slot_and_promotion_leaves_the_joiner_share() {
        let (socket, mut queue, mut joiners, _peer) = joiner_flood(40, 2, 0).await;
        assert_eq!(queue.len(), QUIC_INGRESS_JOINER_DATAGRAMS);
        let joiner = |port: u16| SocketAddr::from(([10, 254, 3, 1], 5000 + port));
        let late = |port: u16, tag: u8| (joiner(port), quic(tag));
        // Everything fell into the share: a late joiner datagram is refused.
        let before = outcomes(&socket)[3];
        let feed = |socket: &DemuxSocket<ScriptedSocket>, items: Vec<(SocketAddr, Vec<u8>)>| {
            let mut script = socket.inner_for_test().script.lock().unwrap();
            script.extend(items.into_iter().map(|(a, d)| Step::Packet(a, d)));
        };
        let mut buf = [0u8; 64];
        feed(&socket, vec![late(0, 0x70)]);
        socket.recv_classified(&mut buf).await.unwrap();
        assert_eq!(outcomes(&socket)[3], before + 1, "share full");
        // One read frees one slot, which exactly one more datagram takes.
        queue.try_recv().expect("a queued joiner datagram");
        feed(&socket, vec![late(1, 0x71), late(2, 0x72)]);
        socket.recv_classified(&mut buf).await.unwrap();
        socket.recv_classified(&mut buf).await.unwrap();
        assert_eq!(outcomes(&socket)[3], before + 2, "one freed, one taken");
        // Promote port 5: its datagrams are admitted past the full share.
        let (_permit, mark) = joiners.remove(5);
        drop(mark);
        feed(&socket, vec![late(5, 0x73), late(6, 0x74)]);
        socket.recv_classified(&mut buf).await.unwrap();
        socket.recv_classified(&mut buf).await.unwrap();
        assert_eq!(
            outcomes(&socket)[3],
            before + 3,
            "the promoted source got in; an unpromoted one did not"
        );
        // Draining everything returns the share to zero, promoted or not.
        while queue.try_recv().is_ok() {}
        assert_eq!(socket.joiner_queued_for_test(), 0, "no slot leaked");
    }

    /// A source marked as a joiner is a joiner from the moment it is on the
    /// allow-set: the mark is placed first, so no datagram can see the source
    /// allowed and unmarked. A datagram right after `allow_quic_joiner`
    /// counts against the joiners' share, and when the allow-set refuses, no
    /// mark is left behind.
    #[tokio::test]
    async fn a_joiner_is_marked_before_it_is_allowed() {
        let (socket, _queue, _joiners, _peer) = joiner_flood(1, 1, 0).await;
        assert_eq!(socket.joiner_queued_for_test(), 1, "counted as a joiner");
        // Source scrape, bounded to the production function: the mark is
        // made before the allow-set entry.
        let body = production_fn_body(
            include_str!("demux.rs"),
            "    pub(crate) fn allow_quic_joiner(",
        );
        let mark = body.find("shared.joiners.entry(addr)").expect("the mark");
        let allow = body
            .find("self.allow_quic_source(addr)?")
            .expect("the allow");
        assert!(mark < allow, "marked first");

        // A full allow-set refuses the joiner and leaves no mark.
        let full = DemuxSocket::wrap(ScriptedSocket::new(vec![]), None);
        let _queue = full.enable_quic_ingress().expect("first enable");
        let _held: Vec<_> = (0..QUIC_ALLOWED_SOURCES_MAX)
            .map(|i| {
                let addr = SocketAddr::from(([10, 9, (i / 250) as u8, (i % 250) as u8], 7000));
                full.allow_quic_source(addr).expect("room")
            })
            .collect();
        let late: SocketAddr = "10.8.8.8:7000".parse().unwrap();
        assert!(full.allow_quic_joiner(late).is_none(), "the set is full");
        assert!(
            !full
                .ingress_for_test()
                .unwrap()
                .shared
                .joiners
                .contains_key(&late),
            "the refused joiner left no mark"
        );
    }

    /// With the queue filled down to the quiet reserve, a joiner datagram is
    /// refused as `reserve_full` (a joiner never uses the reserve, however
    /// few datagrams it holds) and leaves no `queued` entry behind, while a
    /// quiet established source still gets in.
    #[tokio::test]
    async fn a_joiner_never_uses_the_quiet_reserve_and_leaves_no_entry() {
        let fill = QUIC_INGRESS_QUEUE_DATAGRAMS - QUIC_INGRESS_QUIET_RESERVE;
        let source =
            |i: usize| SocketAddr::from(([10, 253, (i / 250) as u8, (i % 250) as u8], 8000));
        let joiner: SocketAddr = "10.252.0.1:8100".parse().unwrap();
        let quiet: SocketAddr = "10.252.0.2:8101".parse().unwrap();
        let mut script: Vec<Step> = (0..fill)
            .map(|i| Step::Packet(source(i), quic(1)))
            .collect();
        script.push(Step::Packet(joiner, quic(2)));
        script.push(Step::Packet(quiet, quic(3)));
        let socket = DemuxSocket::wrap(ScriptedSocket::new(script), None);
        let _queue = socket.enable_quic_ingress().expect("first enable");
        let _permits: Vec<_> = (0..fill)
            .map(|i| socket.allow_quic_source(source(i)).expect("allowed"))
            .collect();
        let _joiner = socket.allow_quic_joiner(joiner).expect("allowed");
        let _quiet = socket.allow_quic_source(quiet).expect("allowed");
        let mut buf = [0u8; 64];
        for _ in 0..fill {
            socket.recv_classified(&mut buf).await.unwrap();
        }
        let before = outcomes(&socket);
        socket.recv_classified(&mut buf).await.unwrap();
        socket.recv_classified(&mut buf).await.unwrap();
        let after = outcomes(&socket);
        assert_eq!(after[5], before[5] + 1, "the joiner got reserve_full");
        assert_eq!(after[0], before[0] + 1, "the quiet source used the reserve");
        let shared = &socket.ingress_for_test().unwrap().shared;
        assert!(
            !shared.queued.contains_key(&joiner),
            "the refused joiner left no queued entry"
        );
        assert_eq!(
            socket.joiner_queued_for_test(),
            0,
            "and took no joiner slot"
        );
    }

    /// When the queue refuses a joiner's datagram after its slot was taken
    /// (here: the receiver is gone, `Closed`), the joiner slot and the
    /// source's share are rolled back, so nothing leaks. The refusal as
    /// `Full` is the same lines but unreachable here: the one sender checks
    /// the free capacity first and is the only sender.
    #[tokio::test]
    async fn a_refused_joiner_datagram_rolls_back_its_slot() {
        let joiner: SocketAddr = "10.252.1.1:8200".parse().unwrap();
        let socket = DemuxSocket::wrap(
            ScriptedSocket::new(vec![
                Step::Packet(joiner, quic(1)),
                Step::Packet(joiner, quic(2)),
            ]),
            None,
        );
        let queue = socket.enable_quic_ingress().expect("first enable");
        let _joiner = socket.allow_quic_joiner(joiner).expect("allowed");
        drop(queue);
        let mut buf = [0u8; 64];
        socket.recv_classified(&mut buf).await.unwrap();
        socket.recv_classified(&mut buf).await.unwrap();
        assert_eq!(outcomes(&socket)[4], 2, "both refused as no_ingress");
        assert_eq!(socket.joiner_queued_for_test(), 0, "no joiner slot leaked");
        let shared = &socket.ingress_for_test().unwrap().shared;
        assert!(!shared.queued.contains_key(&joiner), "no share leaked");
    }

    /// While one joiner keeps the joiners' share full, an honest joiner's
    /// datagram is dropped (and the QUIC handshake retransmits it), and is
    /// admitted as soon as the endpoint reads a slot free. The honest join is
    /// slowed to the endpoint's read rate, not stopped.
    #[tokio::test]
    async fn an_honest_joiner_gets_in_as_the_endpoint_reads_while_the_share_is_full() {
        let attacker = |i: usize| SocketAddr::from(([10, 254, 4, 1], 6000 + i as u16));
        let honest: SocketAddr = "10.254.4.9:6500".parse().unwrap();
        let mut script: Vec<Step> = (0..QUIC_INGRESS_JOINER_DATAGRAMS)
            .map(|i| Step::Packet(attacker(i), quic(1)))
            .collect();
        script.push(Step::Packet(honest, quic(0xA0)));
        script.push(Step::Packet(honest, quic(0xA1)));
        let socket = DemuxSocket::wrap(ScriptedSocket::new(script), None);
        let mut queue = socket.enable_quic_ingress().expect("first enable");
        let _attackers: Vec<_> = (0..QUIC_INGRESS_JOINER_DATAGRAMS)
            .map(|i| socket.allow_quic_joiner(attacker(i)).expect("allowed"))
            .collect();
        let _honest = socket.allow_quic_joiner(honest).expect("allowed");
        let mut buf = [0u8; 64];
        for _ in 0..QUIC_INGRESS_JOINER_DATAGRAMS + 1 {
            socket.recv_classified(&mut buf).await.unwrap();
        }
        assert_eq!(outcomes(&socket)[3], 1, "the honest datagram was dropped");
        queue.try_recv().expect("the endpoint reads one");
        socket.recv_classified(&mut buf).await.unwrap();
        assert_eq!(outcomes(&socket)[3], 1, "its retransmission got in");
        assert!(
            std::iter::from_fn(|| queue.try_recv().ok()).any(|d| d.from == honest),
            "the honest joiner's datagram is queued"
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
        assert_eq!(outcomes(&socket), [0, 1, 0, 0, 0, 0]);

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
        assert_eq!(outcomes(&socket), [2, 2, 0, 0, 0, 0]);
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
        assert_eq!(outcomes(&socket), [0, 0, 0, 0, 1, 0]);

        let queue = socket.enable_quic_ingress().expect("first enable");
        assert!(socket.enable_quic_ingress().is_none(), "enabled only once");
        let _permit = socket.allow_quic_source(from).expect("allowed");
        drop(queue);
        socket.recv_classified(&mut buf).await.unwrap();
        assert_eq!(
            outcomes(&socket),
            [0, 0, 0, 0, 2, 0],
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
        assert_eq!(outcomes(&socket), [0, 0, 0, 0, 1, 0], "no ingress");
        assert!(socket.with_quic_ingress(|_| Ok::<(), ()>(())).is_none());

        let working = DemuxSocket::wrap(ScriptedSocket::new(Vec::new()), None);
        let receiver = working
            .with_quic_ingress(Ok::<_, ()>)
            .expect("first enable")
            .expect("built");
        assert!(working.allow_quic_source(from).is_some());
        drop(receiver);
    }

    /// `with_replaced_quic_ingress` (a rebuilt endpoint, `quic::rebuild`):
    /// the old ingress is disabled (its allow-set emptied, its receiver
    /// ended) and a new one with an empty allow-set takes over; a failed
    /// build leaves the ingress disabled; with no ingress ever enabled it
    /// returns `None` and changes nothing.
    #[tokio::test]
    async fn a_replaced_ingress_disables_the_old_one_and_starts_empty() {
        let from: SocketAddr = "10.254.2.8:4108".parse().unwrap();
        let socket =
            DemuxSocket::wrap(ScriptedSocket::new(vec![Step::Packet(from, quic(1))]), None);
        assert!(
            socket
                .with_replaced_quic_ingress(|_| Ok::<(), ()>(()))
                .is_none(),
            "nothing to replace"
        );
        assert!(socket.ingress_for_test().is_none(), "and nothing changed");

        let mut old = socket.enable_quic_ingress().expect("first enable");
        let old_permit = socket.allow_quic_source(from).expect("allowed");
        assert!(socket.is_quic_source_allowed(from));
        let mut fresh = socket
            .with_replaced_quic_ingress(Ok::<_, ()>)
            .expect("replaced")
            .expect("built");
        assert!(
            !socket.is_quic_source_allowed(from),
            "allow-set starts empty"
        );
        assert!(
            old.shared.disabled.load(Ordering::Acquire),
            "the old ingress is disabled"
        );
        assert!(
            old.queue.recv().await.is_none(),
            "the old endpoint's receiver ends"
        );
        // The old permit releases nothing in the new ingress.
        let new_permit = socket
            .allow_quic_source(from)
            .expect("the new ingress works");
        drop(old_permit);
        assert!(
            socket.is_quic_source_allowed(from),
            "kept by the new permit"
        );
        let mut buf = [0u8; 2048];
        socket.recv_classified(&mut buf).await.unwrap();
        assert!(
            fresh.queue.try_recv().is_ok(),
            "datagrams reach the new queue"
        );
        drop(new_permit);

        // A failed build leaves the replaced ingress disabled.
        let failed: Result<(), &str> = socket
            .with_replaced_quic_ingress(|_| Err("no endpoint"))
            .expect("replaced");
        assert!(failed.is_err());
        assert!(socket.allow_quic_source(from).is_none(), "disabled");
        assert!(
            fresh.queue.recv().await.is_none(),
            "the replaced receiver ends"
        );
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

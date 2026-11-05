//! What a simulation test can see of the QUIC upgrades of its nodes, and the two
//! faults it can inject (#5940, step e5a-5b). Compiled for tests only.
//!
//! The test takes a [`QuicUpgradeTrace`] from `SimNetwork::quic_upgrade_trace`
//! before the run. Each node's transport finds it when it opens, by its network.
//! The handle holds its data in an `Arc`, so it can be read after the run has
//! consumed the network.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use super::policy::QuicPathPermits;
use crate::transport::quic::path::QuicPathStats;

type Version = (u8, u8, u16);

/// One thing a node's upgrades did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuicUpgradeRecord {
    /// On the runtime's clock, which every node shares.
    pub at: Duration,
    pub node: SocketAddr,
    pub peer: SocketAddr,
    /// The session's number in its node; 0 for a node with QUIC off.
    pub session: u64,
    /// An `Outcome` in its `Debug` form, or `received ...`, `sent ...`,
    /// `closed path with ...`, `stale connection ...`, `session ended`.
    pub what: String,
    /// The path permits the node held just after this.
    pub permits: usize,
}

/// What one path's own counters read when asked, live or ended. Each count is
/// made where the path decides it (`QuicPathStats`), not derived from the
/// records.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QuicPathTotals {
    /// Data frames this side refused with NOT_ACCEPTING_DATA (its own bit 0).
    pub refused_not_accepting_data: u64,
    /// Data sends the peer refused with NOT_ACCEPTING_DATA (`quic_data_refused`).
    pub data_refused: u64,
    /// Shorts this side took off the path.
    pub delivered_shorts: u64,
    /// Streams this side announced to `recv`.
    pub announced_streams: u64,
    /// Data sends that ended `Sent`, by kind: short, large short, stream, pipe.
    pub sent: [u64; 4],
    /// Data sends that ended `MaybeDelivered`, all kinds.
    pub maybe_delivered: u64,
    /// Data sends that went to UDP after the path (not opened, or failed before
    /// the end), all kinds.
    pub on_udp: u64,
    /// Data sends that were dropped, all kinds.
    pub dropped: u64,
    /// Shorts sent on UDP because the path's queue was full.
    pub short_overflow_udp: u64,
    /// Whether the path was closed after its sends kept timing out.
    pub stalled_closed: u64,
}

impl QuicPathTotals {
    /// Data sends that ended `Sent`, all kinds.
    pub fn sent_total(&self) -> u64 {
        self.sent.iter().sum()
    }

    fn of(stats: &QuicPathStats) -> Self {
        use crate::transport::quic::path::{
            QuicInboundOutcome as In, QuicSendEvent as Event, QuicSendResult as Result, SendKind,
        };
        Self {
            refused_not_accepting_data: stats.count(In::RefusedNotAcceptingData),
            data_refused: stats.send_event_count(Event::DataRefused),
            delivered_shorts: stats.count(In::DeliveredShort),
            announced_streams: stats.count(In::AnnouncedStream),
            sent: SendKind::ALL.map(|kind| stats.send_count(kind, Result::Sent)),
            maybe_delivered: stats.send_total(Result::MaybeDelivered),
            on_udp: stats.send_total(Result::NotOpenedUdp) + stats.send_total(Result::FailedUdp),
            dropped: stats.send_total(Result::Dropped),
            short_overflow_udp: stats.send_event_count(Event::ShortOverflowUdp),
            stalled_closed: stats.send_event_count(Event::StalledClosed),
        }
    }
}

/// One path a node built, with its counters as they read now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuicPathRecord {
    pub node: SocketAddr,
    pub peer: SocketAddr,
    /// The session's number in its node, as in [`QuicUpgradeRecord::session`].
    pub session: u64,
    /// When the path was built, on the clock of [`QuicUpgradeRecord::at`].
    pub built_at: Duration,
    pub totals: QuicPathTotals,
}

/// A path a node built: node, peer, session, when, and its counters.
struct BuiltPath {
    node: SocketAddr,
    peer: SocketAddr,
    session: u64,
    built_at: Duration,
    stats: Arc<QuicPathStats>,
}

#[derive(Default)]
struct TraceData {
    records: Mutex<Vec<QuicUpgradeRecord>>,
    /// Every path built, with its counters, which outlive it.
    paths: Mutex<Vec<BuiltPath>>,
    wrong_record: Mutex<BTreeSet<SocketAddr>>,
    down_at: Mutex<BTreeMap<SocketAddr, Duration>>,
    permits: Mutex<BTreeMap<SocketAddr, QuicPathPermits>>,
    connections: Mutex<BTreeMap<SocketAddr, usize>>,
    /// Nodes that receive an extra Initial each time they allow a peer.
    initial_on_attempt: Mutex<BTreeSet<SocketAddr>>,
    /// The version a peer reports, and the release it then reads as.
    release: Mutex<Option<(Version, Version)>>,
    /// Per node, `(n, sends seen)`: every nth data send that could go on a path
    /// goes on UDP.
    divert: Mutex<BTreeMap<SocketAddr, (u64, u64)>>,
}

/// A network's upgrade records. Clones share them.
#[derive(Clone, Default)]
pub struct QuicUpgradeTrace {
    data: Arc<TraceData>,
    epoch: Arc<Mutex<Option<tokio::time::Instant>>>,
}

impl QuicUpgradeTrace {
    /// Every record so far, in the order they happened.
    pub fn records(&self) -> Vec<QuicUpgradeRecord> {
        self.data.records.lock().clone()
    }

    /// The time now, on the clock of [`QuicUpgradeRecord::at`].
    pub fn now(&self) -> Duration {
        let now = tokio::time::Instant::now();
        now.saturating_duration_since(*self.epoch.lock().get_or_insert(now))
    }

    /// Every path any node built, in the order they were built, with what its
    /// counters read now.
    pub fn paths(&self) -> Vec<QuicPathRecord> {
        self.data
            .paths
            .lock()
            .iter()
            .map(|built| QuicPathRecord {
                node: built.node,
                peer: built.peer,
                session: built.session,
                built_at: built.built_at,
                totals: QuicPathTotals::of(&built.stats),
            })
            .collect()
    }

    /// The counters of every path summed, field by field.
    pub fn path_totals(&self) -> QuicPathTotals {
        self.paths()
            .iter()
            .fold(QuicPathTotals::default(), |mut sum, path| {
                let t = &path.totals;
                sum.refused_not_accepting_data += t.refused_not_accepting_data;
                sum.data_refused += t.data_refused;
                sum.delivered_shorts += t.delivered_shorts;
                sum.announced_streams += t.announced_streams;
                for (total, sent) in sum.sent.iter_mut().zip(t.sent) {
                    *total += sent;
                }
                sum.maybe_delivered += t.maybe_delivered;
                sum.on_udp += t.on_udp;
                sum.dropped += t.dropped;
                sum.short_overflow_udp += t.short_overflow_udp;
                sum.stalled_closed += t.stalled_closed;
                sum
            })
    }

    /// What `node` recorded about `peer`, in order.
    pub fn of(&self, node: SocketAddr, peer: SocketAddr) -> Vec<QuicUpgradeRecord> {
        self.records()
            .into_iter()
            .filter(|record| record.node == node && record.peer == peer)
            .collect()
    }

    /// Records of `node` about anyone whose `what` starts with `prefix`.
    pub fn matching(&self, node: SocketAddr, prefix: &str) -> Vec<QuicUpgradeRecord> {
        self.records()
            .into_iter()
            .filter(|record| record.node == node && record.what.starts_with(prefix))
            .collect()
    }

    /// The UDP connections `node`'s transport opened, with QUIC on or off.
    pub fn connections_opened(&self, node: SocketAddr) -> usize {
        self.data
            .connections
            .lock()
            .get(&node)
            .copied()
            .unwrap_or(0)
    }

    /// The path permits `node` holds now; `None` when it has no upgrades.
    pub fn permits_in_use(&self, node: SocketAddr) -> Option<usize> {
        self.data
            .permits
            .lock()
            .get(&node)
            .map(QuicPathPermits::in_use)
    }

    /// `node` compares a new path's key with a record that does not match, so it
    /// closes the path with IDENTITY_MISMATCH. The handshake itself is pinned to
    /// the right key: a record that disagrees with the pinned key cannot be made
    /// any other way from outside the node.
    pub fn make_record_wrong(&self, node: SocketAddr) {
        self.data.wrong_record.lock().insert(node);
    }

    /// Each time `node` allows a peer's address, it receives at once an Initial
    /// with that address as its source and a connection ID the peer never used,
    /// so the Initial gets there before anything the peer sends.
    pub fn send_initial_on_each_attempt(&self, node: SocketAddr) {
        self.data.initial_on_attempt.lock().insert(node);
    }

    /// A peer that reports `real` is treated as running `release` when the machine
    /// reads its version. Only for what the crate's version cannot yet show: this
    /// tree carries the negotiation and reports the last release, which is below
    /// the floor until the release bump. A peer advertising an older version is
    /// not affected.
    pub fn run_as_release(&self, real: Version, release: Version) {
        *self.data.release.lock() = Some((real, release));
    }

    /// Of `node`'s data sends that could go on a path, every `n`th goes on UDP
    /// instead, whatever its kind, and is recorded (`sent on UDP for the test`).
    /// So a message's metadata and its stream can take different paths, in
    /// either order, and adjacent messages can arrive out of order.
    pub fn send_every_nth_on_udp(&self, node: SocketAddr, n: u64) {
        assert!(n > 0, "n is at least 1");
        self.data.divert.lock().insert(node, (n, 0));
    }

    /// `node`'s QUIC endpoint driver fails `at` after its transport opens.
    pub fn fail_endpoint_at(&self, node: SocketAddr, at: Duration) {
        self.data.down_at.lock().insert(node, at);
    }

    pub(crate) fn node(&self, addr: SocketAddr) -> NodeTrace {
        NodeTrace {
            trace: self.clone(),
            addr,
        }
    }
}

/// A [`QuicUpgradeTrace`], for one node.
#[derive(Clone)]
pub(crate) struct NodeTrace {
    trace: QuicUpgradeTrace,
    addr: SocketAddr,
}

impl NodeTrace {
    pub(crate) fn connection_opened(&self) {
        *self
            .trace
            .data
            .connections
            .lock()
            .entry(self.addr)
            .or_default() += 1;
    }

    pub(crate) fn record(&self, peer: SocketAddr, session: u64, what: String) {
        let at = self.trace.now();
        let permits = self
            .trace
            .data
            .permits
            .lock()
            .get(&self.addr)
            .map_or(0, QuicPathPermits::in_use);
        self.trace.data.records.lock().push(QuicUpgradeRecord {
            at,
            node: self.addr,
            peer,
            session,
            what,
            permits,
        });
    }

    /// Keep `stats` of a path this node built to `peer` in `session`, for
    /// [`QuicUpgradeTrace::paths`].
    pub(crate) fn path_built(&self, peer: SocketAddr, session: u64, stats: Arc<QuicPathStats>) {
        let at = self.trace.now();
        self.trace.data.paths.lock().push(BuiltPath {
            node: self.addr,
            peer,
            session,
            built_at: at,
            stats,
        });
    }

    /// The version the machine reads for a peer that reports `reported`.
    pub(crate) fn peer_version(&self, reported: Option<Version>) -> Option<Version> {
        match *self.trace.data.release.lock() {
            Some((real, release)) if reported == Some(real) => Some(release),
            Some(_) | None => reported,
        }
    }

    pub(crate) fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub(crate) fn initial_on_each_attempt(&self) -> bool {
        self.trace
            .data
            .initial_on_attempt
            .lock()
            .contains(&self.addr)
    }

    /// Deliver `datagram` to this node's socket, as sent from `source`, and
    /// record that it was (`injected an Initial`), so a test can check the
    /// hook ran rather than infer it. Nothing is recorded when the node's
    /// network cannot be found.
    pub(crate) fn deliver_to_node(&self, datagram: Vec<u8>, source: SocketAddr) {
        if crate::transport::in_memory_socket::deliver_packet_at(self.addr, datagram, source) {
            self.record(source, 0, "injected an Initial".to_string());
        }
    }

    /// Whether this data send goes on UDP for [`QuicUpgradeTrace::send_every_nth_on_udp`].
    pub(crate) fn divert_to_udp(&self) -> bool {
        let divert = {
            let mut divert = self.trace.data.divert.lock();
            let Some((n, seen)) = divert.get_mut(&self.addr) else {
                return false;
            };
            *seen += 1;
            *seen % *n == 0
        };
        if divert {
            self.record(self.addr, 0, "sent on UDP for the test".to_string());
        }
        divert
    }

    pub(crate) fn wrong_record(&self) -> bool {
        self.trace.data.wrong_record.lock().contains(&self.addr)
    }

    pub(crate) fn down_at(&self) -> Option<Duration> {
        self.trace.data.down_at.lock().get(&self.addr).copied()
    }

    pub(crate) fn register_permits(&self, permits: QuicPathPermits) {
        self.trace.data.permits.lock().insert(self.addr, permits);
    }
}

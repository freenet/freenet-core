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

#[derive(Default)]
struct TraceData {
    records: Mutex<Vec<QuicUpgradeRecord>>,
    wrong_record: Mutex<BTreeSet<SocketAddr>>,
    down_at: Mutex<BTreeMap<SocketAddr, Duration>>,
    permits: Mutex<BTreeMap<SocketAddr, QuicPathPermits>>,
    connections: Mutex<BTreeMap<SocketAddr, usize>>,
    /// Nodes that receive an extra Initial each time they allow a peer.
    initial_on_attempt: Mutex<BTreeSet<SocketAddr>>,
    /// The version a peer reports, and the release it then reads as.
    release: Mutex<Option<(Version, Version)>>,
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

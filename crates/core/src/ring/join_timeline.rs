//! Per-peer join timeline recorder for simulation testing (#5858).
//!
//! The event log a simulation collects carries no virtual timestamps
//! (`NetLogMessage::datetime` is wall-clock), so it cannot answer "how long
//! after its first CONNECT did this peer get its first ring connection?". This
//! recorder can: it timestamps, in the simulation's paused tokio clock, when
//! each peer started, when it initiated each CONNECT, and every change in its
//! ring-connection count.
//!
//! Recording is opt-in per network. A test calls
//! `SimNetwork::join_timeline_handle()` before running, which registers a
//! [`JoinTimelineRecorder`] under the network's name; the direct runner sets
//! the time origin when it starts. Networks that never ask for a handle (and
//! production nodes, which have no current network name) record nothing.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use dashmap::DashMap;
use parking_lot::Mutex;
use tokio::time::Instant;

use super::topology_registry::get_current_network_name;
use crate::transport::TransportPublicKey;

/// What the recorder knows about one peer. All times are offsets from the
/// recorder's origin (the direct runner's start).
#[derive(Debug, Clone, Default)]
pub struct JoinTimeline {
    /// When the peer's node task started running.
    pub started_at: Option<Duration>,
    /// When the peer initiated its first CONNECT of any kind.
    pub first_connect_at: Option<Duration>,
    /// Number of CONNECTs this peer initiated (gateway joins, ring
    /// acquisition, lattice probes, version probes).
    pub connects_initiated: u64,
    /// When each of those CONNECTs was initiated.
    pub connect_times: Vec<Duration>,
    /// `connects_initiated` at the moment the first ring connection was
    /// recorded: how many attempts the first connection took.
    pub connects_before_first_ring_connection: Option<u64>,
    /// Ring-connection count after every change, in time order.
    pub ring_count_history: Vec<(Duration, usize)>,
}

impl JoinTimeline {
    /// When the peer first held at least one ring connection.
    pub fn first_ring_connection_at(&self) -> Option<Duration> {
        self.reached_at(1)
    }

    /// When the peer first held at least `count` ring connections.
    pub fn reached_at(&self, count: usize) -> Option<Duration> {
        self.ring_count_history
            .iter()
            .find(|(_, c)| *c >= count)
            .map(|(t, _)| *t)
    }

    /// The peer's ring-connection count at time `t` (0 before any change).
    pub fn count_at(&self, t: Duration) -> usize {
        self.ring_count_history
            .iter()
            .take_while(|(at, _)| *at <= t)
            .last()
            .map(|(_, c)| *c)
            .unwrap_or(0)
    }

    /// The highest ring-connection count the peer ever held.
    pub fn max_count(&self) -> usize {
        self.ring_count_history
            .iter()
            .map(|(_, c)| *c)
            .max()
            .unwrap_or(0)
    }
}

/// Shared recorder for one simulated network. Tests keep an `Arc` to it and
/// read it after the run; `SimNetwork::Drop` only unregisters it.
#[derive(Debug, Default)]
pub struct JoinTimelineRecorder {
    origin: Mutex<Option<Instant>>,
    peers: Mutex<HashMap<TransportPublicKey, JoinTimeline>>,
}

impl JoinTimelineRecorder {
    /// Copy of every peer's timeline recorded so far.
    pub fn snapshot(&self) -> HashMap<TransportPublicKey, JoinTimeline> {
        self.peers.lock().clone()
    }

    fn elapsed(&self) -> Option<Duration> {
        self.origin.lock().map(|origin| origin.elapsed())
    }

    fn with_peer(&self, peer: &TransportPublicKey, f: impl FnOnce(&mut JoinTimeline, Duration)) {
        let Some(now) = self.elapsed() else {
            return;
        };
        let mut peers = self.peers.lock();
        f(peers.entry(peer.clone()).or_default(), now);
    }
}

static RECORDERS: LazyLock<DashMap<String, Arc<JoinTimelineRecorder>>> =
    LazyLock::new(DashMap::new);

fn current_recorder() -> Option<Arc<JoinTimelineRecorder>> {
    let name = get_current_network_name()?;
    RECORDERS.get(&name).map(|r| r.value().clone())
}

/// Register a recorder for `network_name`, replacing any previous one.
pub fn install_join_timeline_recorder(network_name: &str) -> Arc<JoinTimelineRecorder> {
    let recorder = Arc::new(JoinTimelineRecorder::default());
    RECORDERS.insert(network_name.to_string(), recorder.clone());
    recorder
}

/// Start the clock of `network_name`'s recorder, if it has one. Called by the
/// direct runner inside its paused runtime, so offsets are virtual time.
pub fn start_join_timeline_clock(network_name: &str) {
    if let Some(recorder) = RECORDERS.get(network_name) {
        *recorder.origin.lock() = Some(Instant::now());
    }
}

/// Unregister `network_name`'s recorder (for test cleanup). Holders of the
/// `Arc` can still read it.
pub fn clear_join_timeline_recorder(network_name: &str) {
    RECORDERS.remove(network_name);
}

/// Record that `peer`'s node task started.
pub fn record_node_started(peer: &TransportPublicKey) {
    if let Some(recorder) = current_recorder() {
        recorder.with_peer(peer, |t, now| {
            t.started_at.get_or_insert(now);
        });
    }
}

/// Record that `peer` initiated a CONNECT.
pub fn record_connect_initiated(peer: &TransportPublicKey) {
    if let Some(recorder) = current_recorder() {
        recorder.with_peer(peer, |t, now| {
            t.first_connect_at.get_or_insert(now);
            t.connects_initiated += 1;
            t.connect_times.push(now);
        });
    }
}

/// Record `peer`'s ring-connection count after it changed.
pub fn record_ring_connection_count(peer: &TransportPublicKey, count: usize) {
    if let Some(recorder) = current_recorder() {
        recorder.with_peer(peer, |t, now| {
            if count > 0 && t.connects_before_first_ring_connection.is_none() {
                t.connects_before_first_ring_connection = Some(t.connects_initiated);
            }
            t.ring_count_history.push((now, count));
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn timeline(history: &[(u64, usize)]) -> JoinTimeline {
        JoinTimeline {
            ring_count_history: history
                .iter()
                .map(|(s, c)| (Duration::from_secs(*s), *c))
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn reached_at_finds_first_time_at_or_above_count() {
        let t = timeline(&[(5, 1), (7, 0), (9, 2), (12, 3)]);
        assert_eq!(t.first_ring_connection_at(), Some(Duration::from_secs(5)));
        assert_eq!(t.reached_at(2), Some(Duration::from_secs(9)));
        assert_eq!(t.reached_at(4), None);
        assert_eq!(t.max_count(), 3);
    }

    #[test]
    fn count_at_is_zero_before_first_change_and_tracks_drops() {
        let t = timeline(&[(5, 1), (7, 0), (9, 2)]);
        assert_eq!(t.count_at(Duration::from_secs(4)), 0);
        assert_eq!(t.count_at(Duration::from_secs(5)), 1);
        assert_eq!(t.count_at(Duration::from_secs(8)), 0);
        assert_eq!(t.count_at(Duration::from_secs(100)), 2);
    }

    #[test]
    fn empty_timeline_has_no_connection() {
        let t = JoinTimeline::default();
        assert_eq!(t.first_ring_connection_at(), None);
        assert_eq!(t.count_at(Duration::from_secs(1)), 0);
        assert_eq!(t.max_count(), 0);
    }
}

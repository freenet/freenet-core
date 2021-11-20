use std::sync::atomic::Ordering::SeqCst;
use std::{sync::atomic::AtomicUsize, time::Instant};

use crate::operations::join_ring::JoinRingMsg;
use crate::ring::{Location, PeerKeyLocation};
use crate::{
    conn_manager::PeerKey,
    message::{Message, Transaction},
};

#[cfg(test)]
pub(super) use test_utils::TestEventListener;

static LOG_ID: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Clone, Copy)]
struct ListenerLogId(usize);

/// A type that reacts to incoming messages from the network.
/// It injects itself at the message event loop.
///
/// This type then can emit it's own information to adjacent systems
/// or is a no-op.
pub(crate) trait EventListener {
    fn event_received(&mut self, ev: EventLog);
    fn trait_clone(&self) -> Box<dyn EventListener + Send + Sync + 'static>;
}

pub(crate) struct EventLog<'a> {
    tx: &'a Transaction,
    peer_id: &'a PeerKey,
    kind: EventKind,
}

impl<'a> EventLog<'a> {
    pub fn new(msg: &'a Message, peer_id: &'a PeerKey) -> Self {
        let kind = match msg {
            Message::JoinRing(JoinRingMsg::Response { sender, target, .. }) => {
                EventKind::Connected {
                    loc: sender.location.unwrap(),
                    to: *target,
                }
            }
            _ => EventKind::Unknown,
        };
        EventLog {
            tx: msg.id(),
            peer_id,
            kind,
        }
    }
}

struct MessageLog {
    peer_id: PeerKey,
    ts: Instant,
    kind: EventKind,
}

#[derive(Clone)]
pub(super) struct EventRegister {}

impl EventRegister {
    pub fn new() -> Self {
        EventRegister {}
    }
}

impl EventListener for EventRegister {
    fn event_received(&mut self, log: EventLog) {
        let (_msg_log, _log_id) = create_log(log);
        // TODO: save log
    }

    fn trait_clone(&self) -> Box<dyn EventListener + Send + Sync + 'static> {
        Box::new(self.clone())
    }
}

#[derive(Debug, PartialEq, Eq)]
enum EventKind {
    Connected { loc: Location, to: PeerKeyLocation },
    Unknown,
}

#[inline]
fn create_log(log: EventLog) -> (MessageLog, ListenerLogId) {
    let log_id = ListenerLogId(LOG_ID.fetch_add(1, SeqCst));
    let EventLog { peer_id, kind, .. } = log;
    let msg_log = MessageLog {
        ts: Instant::now(),
        peer_id: *peer_id,
        kind,
    };
    (msg_log, log_id)
}

#[cfg(test)]
mod test_utils {
    use std::{collections::HashMap, sync::Arc};

    use dashmap::DashMap;
    use parking_lot::RwLock;

    use crate::{message::TxType, ring::Distance};

    use super::*;

    #[derive(Clone)]
    pub(in crate::node) struct TestEventListener {
        node_labels: Arc<DashMap<String, PeerKey>>,
        tx_log: Arc<DashMap<Transaction, Vec<ListenerLogId>>>,
        logs: Arc<RwLock<Vec<MessageLog>>>,
    }

    impl TestEventListener {
        pub fn new() -> Self {
            TestEventListener {
                node_labels: Arc::new(DashMap::new()),
                tx_log: Arc::new(DashMap::new()),
                logs: Arc::new(RwLock::new(Vec::new())),
            }
        }

        pub fn add_node(&mut self, label: String, peer: PeerKey) {
            self.node_labels.insert(label, peer);
        }

        pub fn is_connected(&self, peer: PeerKey) -> bool {
            let logs = self.logs.read();
            logs.iter()
                .any(|log| log.peer_id == peer && matches!(log.kind, EventKind::Connected { .. }))
        }

        /// Unique connections for a given peer and their relative distance to other peers.
        pub fn connections(&self, key: PeerKey) -> impl Iterator<Item = (PeerKey, Distance)> {
            let logs = self.logs.read();
            logs.iter()
                .filter_map(|l| {
                    if let EventKind::Connected {
                        loc,
                        to:
                            PeerKeyLocation {
                                location: Some(other_loc),
                                peer,
                            },
                    } = l.kind
                    {
                        if l.peer_id == key {
                            return Some((peer, loc.distance(&other_loc)));
                        }
                    }
                    None
                })
                .collect::<HashMap<_, _>>()
                .into_iter()
        }
    }

    impl super::EventListener for TestEventListener {
        fn event_received(&mut self, log: EventLog) {
            let tx = log.tx;
            let mut logs = self.logs.write();
            let (msg_log, log_id) = create_log(log);
            logs.push(msg_log);
            std::mem::drop(logs);
            self.tx_log.entry(*tx).or_default().push(log_id);
        }

        fn trait_clone(&self) -> Box<dyn EventListener + Send + Sync + 'static> {
            Box::new(self.clone())
        }
    }

    #[test]
    fn test_get_connections() -> Result<(), anyhow::Error> {
        let peer_id = PeerKey::random();
        let loc = Location::try_from(0.5)?;
        let tx = Transaction::new(<JoinRingMsg as TxType>::tx_type_id(), &peer_id);
        let locations = [
            (PeerKey::random(), Location::try_from(0.5)?),
            (PeerKey::random(), Location::try_from(0.75)?),
            (PeerKey::random(), Location::try_from(0.25)?),
        ];

        let mut listener = TestEventListener::new();
        locations.iter().for_each(|(other, location)| {
            listener.event_received(EventLog {
                tx: &tx,
                peer_id: &peer_id,
                kind: EventKind::Connected {
                    loc,
                    to: PeerKeyLocation {
                        peer: *other,
                        location: Some(*location),
                    },
                },
            });
        });

        let distances: Vec<_> = listener.connections(peer_id).collect();
        assert!(distances.len() == 3);
        assert!((distances.iter().map(|(_, l)| l.0).sum::<f64>() - 0.5f64).abs() < f64::EPSILON);
        Ok(())
    }
}

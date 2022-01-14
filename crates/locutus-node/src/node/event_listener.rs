use std::sync::atomic::Ordering::SeqCst;
use std::{sync::atomic::AtomicUsize, time::Instant};

use super::PeerKey;
use crate::{
    contract::{ContractKey, ContractValue, StoreResponse},
    message::{Message, Transaction},
    operations::{get::GetMsg, join_ring::JoinRingMsg, put::PutMsg},
    ring::{Location, PeerKeyLocation},
};

#[cfg(test)]
pub(super) use test_utils::TestEventListener;

use super::OpManager;

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
    pub fn new<CErr>(msg: &'a Message, op_storage: &'a OpManager<CErr>) -> Self
    where
        CErr: std::error::Error,
    {
        let kind = match msg {
            Message::JoinRing(JoinRingMsg::Response { sender, target, .. }) => {
                EventKind::Connected {
                    loc: sender.location.unwrap(),
                    to: *target,
                }
            }
            Message::Put(PutMsg::RequestPut {
                contract, target, ..
            }) => EventKind::Put(
                PutEvent::Request {
                    performer: target.peer,
                    key: contract.key(),
                },
                *msg.id(),
            ),
            Message::Put(PutMsg::SuccessfulUpdate { new_value, .. }) => EventKind::Put(
                PutEvent::PutSuccess {
                    requester: op_storage.ring.peer_key,
                    value: new_value.clone(),
                },
                *msg.id(),
            ),
            Message::Put(PutMsg::Broadcasting {
                new_value,
                broadcast_to,
                key,
                ..
            }) => EventKind::Put(
                PutEvent::BroadcastEmitted {
                    broadcast_to: broadcast_to.clone(),
                    key: key.clone(),
                    value: new_value.clone(),
                },
                *msg.id(),
            ),
            Message::Put(PutMsg::BroadcastTo {
                sender,
                new_value,
                key,
                ..
            }) => EventKind::Put(
                PutEvent::BroadcastReceived {
                    requester: sender.peer.clone(),
                    key: key.clone(),
                    value: new_value.clone(),
                },
                *msg.id(),
            ),
            Message::Get(GetMsg::ReturnGet {
                key,
                value: StoreResponse { value: Some(_), .. },
                ..
            }) => EventKind::Get { key: *key },
            _ => EventKind::Unknown,
        };
        EventLog {
            tx: msg.id(),
            peer_id: &op_storage.ring.peer_key,
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
    fn event_received(&mut self, _log: EventLog) {
        // let (_msg_log, _log_id) = create_log(log);
        // TODO: save log
    }

    fn trait_clone(&self) -> Box<dyn EventListener + Send + Sync + 'static> {
        Box::new(self.clone())
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
enum EventKind {
    Connected { loc: Location, to: PeerKeyLocation },
    Put(PutEvent, Transaction),
    Get { key: ContractKey },
    Unknown,
}

#[derive(Debug, PartialEq, Eq, Clone)]
enum PutEvent {
    Request {
        performer: PeerKey,
        key: ContractKey,
    },
    PutSuccess {
        requester: PeerKey,
        value: ContractValue,
    },
    PutComplete {
        /// peer who performed the event
        performer: PeerKey,
        /// peer who started the put op
        requester: PeerKey,
        /// key of the contract which value was being updated
        key: ContractKey,
        /// value that was put
        value: ContractValue,
    },
    BroadcastEmitted {
        /// subscribed peers
        broadcast_to: Vec<PeerKeyLocation>,
        /// key of the contract which value was being updated
        key: ContractKey,
        /// value that was put
        value: ContractValue,
    },
    BroadcastReceived {
        /// peer who started the broadcast op
        requester: PeerKey,
        /// key of the contract which value was being updated
        key: ContractKey,
        /// value that was put
        value: ContractValue,
    },
    BroadcastComplete {
        /// peer who performed the event
        performer: PeerKey,
        /// peer who started the broadcast op
        requester: PeerKey,
        /// key of the contract which value was being updated
        key: ContractKey,
        /// value that was put
        value: ContractValue,
    },
}

#[inline]
fn create_log(logs: &[MessageLog], log: EventLog) -> (MessageLog, ListenerLogId) {
    let log_id = ListenerLogId(LOG_ID.fetch_add(1, SeqCst));
    let EventLog {
        tx: incoming_tx,
        peer_id,
        kind,
        ..
    } = log;

    let find_put_ops = logs
        .iter()
        .filter_map(|l| {
            if matches!(l, MessageLog { kind: EventKind::Put(_, id), .. } if incoming_tx == id ) {
                match l.kind {
                    EventKind::Put(PutEvent::BroadcastEmitted { .. }, _) => None,
                    _ => Some(&l.kind),
                }
            } else {
                None
            }
        })
        .chain([&kind]);
    let kind = fuse_successful_put_op(find_put_ops).unwrap_or(kind);

    let msg_log = MessageLog {
        ts: Instant::now(),
        peer_id: *peer_id,
        kind,
    };
    (msg_log, log_id)
}

fn fuse_successful_put_op<'a>(
    mut put_ops: impl Iterator<Item = &'a EventKind>,
) -> Option<EventKind> {
    let prev_msgs = [put_ops.next().cloned(), put_ops.next().cloned()];
    match prev_msgs {
        [Some(EventKind::Put(PutEvent::Request { performer, key }, id)), Some(EventKind::Put(PutEvent::PutSuccess { requester, value }, _))] => {
            Some(EventKind::Put(
                PutEvent::PutComplete {
                    performer,
                    requester,
                    key,
                    value,
                },
                id,
            ))
        }
        _ => None,
    }
}

#[cfg(test)]
mod test_utils {
    use std::{collections::HashMap, sync::Arc};

    use dashmap::DashMap;
    use itertools::Itertools;
    use parking_lot::RwLock;

    use crate::{contract::ContractKey, message::TxType, ring::Distance};

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

        pub fn is_connected(&self, peer: &PeerKey) -> bool {
            let logs = self.logs.read();
            logs.iter()
                .any(|log| &log.peer_id == peer && matches!(log.kind, EventKind::Connected { .. }))
        }

        pub fn has_put_contract(
            &self,
            peer: &PeerKey,
            expected_key: &ContractKey,
            expected_value: &ContractValue,
        ) -> bool {
            let logs = self.logs.read();
            logs.iter().any(|log| {
                &log.peer_id == peer
                    && matches!(log.kind, EventKind::Put(PutEvent::PutComplete { ref key, ref value, .. }, ..) if key == expected_key && value == expected_value )
            })
        }

        pub fn get_broadcast_count(
            &self,
            expected_key: &ContractKey,
            expected_value: &ContractValue,
        ) -> usize {
            let mut logs = self.logs.read();
            logs.iter().filter(|log| {
                matches!(log.kind, EventKind::Put(PutEvent::BroadcastEmitted { ref key, ref value, .. }, ..) if key == expected_key && value == expected_value )
            }).count()
        }

        pub fn has_broadcast_contract(
            &self,
            mut broadcast_pairs: Vec<(PeerKey, PeerKey)>,
            expected_key: &ContractKey,
            expected_value: &ContractValue,
        ) -> bool {
            let logs = self.logs.read();
            let mut broadcast_ops = logs.iter().filter_map(|l| {
                if matches!(
                    l,
                    MessageLog {
                        kind: EventKind::Put(_, id),
                        ..
                    }
                ) {
                    match l.kind {
                        EventKind::Put(PutEvent::BroadcastEmitted { .. }, _)
                        | EventKind::Put(PutEvent::BroadcastReceived { .. }, _) => Some(&l.kind),
                        _ => None,
                    }
                } else {
                    None
                }
            });

            let prev_msgs = [broadcast_ops.next().cloned(), broadcast_ops.next().cloned()];
            let broadcast = match prev_msgs {
                [Some(EventKind::Put(PutEvent::BroadcastEmitted { broadcast_to, .. }, id1)), Some(EventKind::Put(
                    PutEvent::BroadcastReceived {
                        requester,
                        key,
                        value,
                    },
                    id2,
                ))] => {
                    if id1 == id2 {
                        Some(EventKind::Put(
                            PutEvent::BroadcastComplete {
                                performer: broadcast_to.get(0).unwrap().peer,
                                requester,
                                key,
                                value,
                            },
                            id1,
                        ))
                    } else {
                        None
                    }
                }
                _ => None,
            };

            match broadcast {
                Some(EventKind::Put(
                    PutEvent::BroadcastComplete {
                        ref performer,
                        ref requester,
                        ..
                    },
                    _,
                )) => {
                    let expected_pair = (performer, requester);
                    broadcast_pairs.retain(|pair| matches!(pair, expected_pair));
                    !broadcast_pairs.is_empty()
                }
                _ => false,
            }
        }

        pub fn has_got_contract(&self, peer: &PeerKey, expected_key: &ContractKey) -> bool {
            let logs = self.logs.read();
            logs.iter().any(|log| {
                &log.peer_id == peer
                    && matches!(log.kind, EventKind::Get { ref key } if key == expected_key  )
            })
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
            let (msg_log, log_id) = create_log(&*logs, log);
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

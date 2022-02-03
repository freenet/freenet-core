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

#[allow(dead_code)] // fixme: remove this
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
            Message::JoinRing(JoinRingMsg::Connected { sender, target, .. }) => {
                EventKind::Connected {
                    loc: target.location.unwrap(),
                    from: target.peer,
                    to: *sender,
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
                    key: *key,
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
                    requester: sender.peer,
                    key: *key,
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

#[cfg(test)]
struct MessageLog {
    peer_id: PeerKey,
    kind: EventKind,
}

#[derive(Clone)]
pub(super) struct EventRegister {}

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
    Connected {
        loc: Location,
        from: PeerKey,
        to: PeerKeyLocation,
    },
    Put(PutEvent, Transaction),
    Get {
        key: ContractKey,
    },
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
}

#[cfg(test)]
mod test_utils {
    use std::{
        collections::HashMap,
        sync::{
            atomic::{AtomicUsize, Ordering::SeqCst},
            Arc,
        },
    };

    use dashmap::DashMap;
    use parking_lot::RwLock;

    use super::*;
    use crate::{contract::ContractKey, message::TxType, ring::Distance};

    static LOG_ID: AtomicUsize = AtomicUsize::new(0);

    #[inline]
    fn create_log(log: EventLog) -> (MessageLog, ListenerLogId) {
        let log_id = ListenerLogId(LOG_ID.fetch_add(1, SeqCst));
        let EventLog { peer_id, kind, .. } = log;
        let msg_log = MessageLog {
            peer_id: *peer_id,
            kind,
        };
        (msg_log, log_id)
    }

    #[derive(Clone)]
    pub(crate) struct TestEventListener {
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
            for_key: &ContractKey,
            expected_value: &ContractValue,
        ) -> bool {
            let logs = self.logs.read();
            let put_ops = logs.iter().filter_map(|l| match &l.kind {
                EventKind::Put(ev, id) => Some((id, ev)),
                _ => None,
            });
            let put_ops: HashMap<_, Vec<_>> = put_ops.fold(HashMap::new(), |mut acc, (id, ev)| {
                acc.entry(id).or_default().push(ev);
                acc
            });

            for (_tx, events) in put_ops {
                let mut is_expected_value = false;
                let mut is_expected_key = false;
                let mut is_expected_peer = false;
                for ev in events {
                    match ev {
                        PutEvent::Request { key, .. } if key != for_key => break,
                        PutEvent::Request { key, .. } if key == for_key => {
                            is_expected_key = true;
                        }
                        PutEvent::PutSuccess { requester, value }
                            if requester == peer && value == expected_value =>
                        {
                            is_expected_peer = true;
                            is_expected_value = true;
                        }
                        _ => {}
                    }
                }
                if is_expected_value && is_expected_peer && is_expected_key {
                    return true;
                }
            }
            false
        }

        /// The contract was broadcasted from one peer to an other successfully.
        pub fn contract_broadcasted(&self, for_key: &ContractKey) -> bool {
            let logs = self.logs.read();
            let put_broadcast_ops = logs.iter().filter_map(|l| match &l.kind {
                EventKind::Put(ev @ PutEvent::BroadcastEmitted { .. }, id)
                | EventKind::Put(ev @ PutEvent::BroadcastReceived { .. }, id) => Some((id, ev)),
                _ => None,
            });
            let put_broadcast_by_tx: HashMap<_, Vec<_>> =
                put_broadcast_ops.fold(HashMap::new(), |mut acc, (id, ev)| {
                    acc.entry(id).or_default().push(ev);
                    acc
                });
            for (_tx, events) in put_broadcast_by_tx {
                let mut was_emitted = false;
                let mut was_received = false;
                for ev in events {
                    match ev {
                        PutEvent::BroadcastEmitted { key, .. } if key == for_key => {
                            was_emitted = true;
                        }
                        PutEvent::BroadcastReceived { key, .. } if key == for_key => {
                            was_received = true;
                        }
                        _ => {}
                    }
                }
                if was_emitted && was_received {
                    return true;
                }
            }
            false
        }

        pub fn has_got_contract(&self, peer: &PeerKey, expected_key: &ContractKey) -> bool {
            let logs = self.logs.read();
            logs.iter().any(|log| {
                &log.peer_id == peer
                    && matches!(log.kind, EventKind::Get { ref key } if key == expected_key  )
            })
        }

        /// Unique connections for a given peer and their relative distance to other peers.
        pub fn connections(&self, peer: PeerKey) -> impl Iterator<Item = (PeerKey, Distance)> {
            let logs = self.logs.read();
            logs.iter()
                .filter_map(|l| {
                    if let EventKind::Connected { loc, from, to } = l.kind {
                        if from == peer {
                            return Some((to.peer, loc.distance(&to.location.unwrap())));
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
                    from: peer_id,
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

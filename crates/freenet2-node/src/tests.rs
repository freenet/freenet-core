use std::{array::IntoIter, collections::HashMap, sync::Arc};

use parking_lot::RwLock;

use crate::{
    conn_manager::{
        self, Channel, ConnectionManager, ListenerHandle, PeerKey, PeerKeyLocation,
        RemoveConnHandler, Transport,
    },
    message::{Message, MsgTypeId, TransactionId},
};

#[derive(Clone)]
pub(crate) struct TestingConnectionManager {
    /// stores listeners by message type
    listeners: Arc<HashMap<MsgTypeId, ListenerRegistry>>,
    transport: InMemoryTransport,
}

type ListenerFn =
    Box<dyn FnOnce(PeerKeyLocation, Message) -> conn_manager::Result<()> + Send + Sync>;

struct ListenerRegistry {
    /// listeners for outbound messages
    outbound: RwLock<HashMap<TransactionId, ListenerFn>>,
    /// listeners for inbound messages
    inbound: RwLock<HashMap<ListenerHandle, ListenerFn>>,
}

impl Default for ListenerRegistry {
    fn default() -> Self {
        Self {
            outbound: RwLock::new(Default::default()),
            inbound: RwLock::new(Default::default()),
        }
    }
}

impl TestingConnectionManager {
    pub fn new(is_open: bool) -> Self {
        Self {
            listeners: Arc::new(
                IntoIter::new(MsgTypeId::enumeration())
                    .map(|id| (id, ListenerRegistry::default()))
                    .collect(),
            ),
            transport: InMemoryTransport { is_open },
        }
    }
}

impl ConnectionManager for TestingConnectionManager {
    type Transport = InMemoryTransport;

    fn on_remove_conn(&self, _func: RemoveConnHandler) {}

    fn listen<F>(&self, tx_type: MsgTypeId, listen_fn: F) -> ListenerHandle
    where
        F: FnOnce(PeerKeyLocation, Message) -> conn_manager::Result<()> + Send + Sync + 'static,
    {
        let tx_ty_listener = self.listeners.get(&tx_type).unwrap();
        let handle_id = ListenerHandle::new();
        tx_ty_listener
            .inbound
            .write()
            .insert(handle_id, Box::new(listen_fn));
        handle_id
    }

    fn listen_to_replies<F>(&self, tx_id: TransactionId, callback: F)
    where
        F: FnOnce(PeerKeyLocation, Message) -> conn_manager::Result<()> + Send + Sync + 'static,
    {
        let tx_ty_listener = self.listeners.get(&tx_id.msg_type()).unwrap();
        tx_ty_listener
            .outbound
            .write()
            .insert(tx_id, Box::new(callback));
    }

    fn transport_mut(&mut self) -> &mut Self::Transport {
        &mut self.transport
    }

    fn transport(&self) -> &Self::Transport {
        &self.transport
    }

    fn add_connection(&self, peer_key: PeerKeyLocation, unsolicited: bool) {
        todo!()
    }

    fn send_with_callback<F>(&self, to: PeerKey, msg_id: TransactionId, msg: Message, callback: F)
    where
        F: FnOnce(PeerKey, Message) -> conn_manager::Result<()> + Send + Sync + 'static,
    {
        todo!()
    }

    fn send(&self, to: PeerKey, tx_id: TransactionId, msg: Message) {
        todo!()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct InMemoryTransport {
    is_open: bool,
}

impl Transport for InMemoryTransport {
    fn send(&mut self, peer: PeerKey, message: &[u8]) {
        todo!()
    }

    fn is_open(&self) -> bool {
        self.is_open
    }

    fn recipient(&self) -> Channel {
        todo!()
    }
}

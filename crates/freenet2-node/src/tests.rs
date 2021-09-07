use std::{collections::HashMap, sync::Arc};

use parking_lot::RwLock;

use crate::{
    conn_manager::{
        self, Channel, ConnectionManager, ListeningHandler, PeerKey, PeerKeyLocation,
        RemoveConnHandler, Transport,
    },
    message::{Message, MsgTypeId, TransactionId},
};

#[derive(Clone)]
pub(crate) struct TestingConnectionManager {
    listeners: Arc<RwLock<HashMap<MsgTypeId, ListenerRegistry>>>,
    transport: InMemoryTransport,
}

type ListenerCallback = Box<dyn FnOnce(PeerKey, Message) -> conn_manager::Result<()> + Send + Sync>;

#[derive(Default)]
struct ListenerRegistry {
    global: HashMap<TransactionId, ListenerCallback>,
}

impl TestingConnectionManager {
    pub fn new(is_open: bool) -> Self {
        Self {
            listeners: Arc::new(RwLock::new(HashMap::new())),
            transport: InMemoryTransport { is_open },
        }
    }
}

impl ConnectionManager for TestingConnectionManager {
    type Transport = InMemoryTransport;

    fn on_remove_conn(&self, _func: RemoveConnHandler) {}

    fn listen<F>(&self, listen_fn: F)
    where
        F: FnOnce(PeerKey, Message) -> conn_manager::Result<()> + Send + Sync + 'static,
    {
        todo!()
    }

   
    fn listen_to_replies<F>(&self, msg_id: TransactionId, callback: F) -> ListeningHandler
    where
        F: FnOnce(PeerKey, Message) -> conn_manager::Result<()> + Send + Sync + 'static,
    {
        let handler_id = ListeningHandler::new(&msg_id);
        let mut reg = self.listeners.write();
        let reg = reg.entry(msg_id.msg_type()).or_default();
        reg.global.insert(msg_id, Box::new(callback));
        handler_id
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

    fn send(&self, to: PeerKey, msg_id: TransactionId, msg: Message) {
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

use std::{
    collections::HashMap,
    sync::{atomic::AtomicU64, Arc},
};

use parking_lot::RwLock;

use crate::{
    conn_manager::{
        self, Channel, ConnectionManager, ListeningHandler, PeerKey, PeerKeyLocation,
        RemoveConnHandler, Transport,
    },
    message::{Message, MessageType},
};

static HANDLE_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
pub(crate) struct TestingConnectionManager {
    listeners: Arc<RwLock<HashMap<MessageType, ListenerRegistry>>>,
}

type ListenerCallback = Box<dyn FnOnce(PeerKey, Message) -> conn_manager::Result<()> + Send + Sync>;

#[derive(Default)]
struct ListenerRegistry {
    global: HashMap<ListeningHandler, ListenerCallback>,
}

impl TestingConnectionManager {
    pub fn new() -> Self {
        Self {
            listeners: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl ConnectionManager for TestingConnectionManager {
    type Transport = InMemoryTransport;

    fn on_remove_conn(&self, _func: RemoveConnHandler) {}

    fn listen<F>(&self, msg_type: MessageType, callback: F) -> ListeningHandler
    where
        F: FnOnce(PeerKey, Message) -> conn_manager::Result<()> + Send + Sync + 'static,
    {
        let handler_id = ListeningHandler::new();
        let mut reg = self.listeners.write();
        let reg = reg.entry(msg_type).or_default();
        reg.global.insert(handler_id, Box::new(callback));
        handler_id
    }

    fn transport(&self) -> Self::Transport {
        todo!()
    }

    fn add_connection(&self, peer_key: PeerKeyLocation, unsolicited: bool) {
        todo!()
    }

    fn send<F>(&self, to: &PeerKey, callback: F)
    where
        F: FnOnce(PeerKey, Message) -> conn_manager::Result<()> + Send + Sync + 'static,
    {
        todo!()
    }
}

pub(crate) struct InMemoryTransport;

impl Transport for InMemoryTransport {
    fn send(&mut self, peer: PeerKey, message: &[u8]) {
        todo!()
    }

    fn is_open(&self) -> bool {
        todo!()
    }

    fn recipient(&self) -> Channel {
        todo!()
    }
}

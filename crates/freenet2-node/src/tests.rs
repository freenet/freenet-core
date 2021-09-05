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
    message::{Message, MessageId, MsgTypeId},
};

#[derive(Clone)]
pub(crate) struct TestingConnectionManager {
    listeners: Arc<RwLock<HashMap<MsgTypeId, ListenerRegistry>>>,
}

type ListenerCallback = Box<dyn FnOnce(PeerKey, Message) -> conn_manager::Result<()> + Send + Sync>;

#[derive(Default)]
struct ListenerRegistry {
    global: HashMap<MessageId, ListenerCallback>,
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

    // FIXME: the fn could take arguments by ref if necessary but due to
    // https://github.com/rust-lang/rust/issues/70263 it won't compile
    // can workaround by wrapping up the fn to express lifetime constraints,
    // consider this, meanwhile passing by value is fine
    fn listen_to_replies<F>(&self, msg_id: MessageId, callback: F) -> ListeningHandler
    where
        F: FnOnce(PeerKey, Message) -> conn_manager::Result<()> + Send + Sync + 'static,
    {
        let handler_id = ListeningHandler::new(&msg_id);
        let mut reg = self.listeners.write();
        let reg = reg.entry(msg_id.msg_type()).or_default();
        reg.global.insert(msg_id, Box::new(callback));
        handler_id
    }

    fn transport(&self) -> Self::Transport {
        todo!()
    }

    fn add_connection(&self, peer_key: PeerKeyLocation, unsolicited: bool) {
        todo!()
    }

    // FIXME: same problem as om tje `listen` fn
    fn send_with_callback<F>(&self, to: PeerKey, msg_id: MessageId, msg: Message, callback: F)
    where
        F: FnOnce(PeerKey, Message) -> conn_manager::Result<()> + Send + Sync + 'static,
    {
        todo!()
    }

    fn send(&self, to: PeerKey, msg_id: MessageId, msg: Message) {
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

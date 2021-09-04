use crate::conn_manager::{
    Channel, ConnectionManager, ListenerCallback, ListeningHandler, PeerKey, RemoveConnHandler,
    SendCallback, Transport,
};

#[derive(Clone)]
pub(crate) struct TestingConnectionManager;

impl ConnectionManager for TestingConnectionManager {
    type Transport = InMemoryTransport;

    fn on_remove_conn(&mut self, _func: RemoveConnHandler) {}
    fn listen(&mut self, callback: ListenerCallback) -> ListeningHandler {
        ListeningHandler
    }

    fn transport(&self) -> Self::Transport {
        todo!()
    }

    fn add_connection(&self, peer_key: PeerKey, unsolicited: bool) {
        todo!()
    }

    fn send(&self, to: &PeerKey, callback: SendCallback) {
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

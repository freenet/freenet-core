use std::{array::IntoIter, collections::HashMap, io::Cursor, sync::Arc};

use once_cell::sync::OnceCell;
use parking_lot::{Mutex, RwLock};
use tokio::sync::broadcast::{Receiver, Sender};

use crate::{
    conn_manager::{
        self, Channel, ConnectionManager, ListenerHandle, PeerKey, PeerKeyLocation,
        RemoveConnHandler, Transport,
    },
    message::{Message, MsgTypeId, TransactionId},
    ring_proto::Location,
};

#[derive(Clone)]
pub(crate) struct TestingConnectionManager {
    /// stores listeners by message type
    listeners: Arc<HashMap<MsgTypeId, ListenerRegistry>>,
    transport: InMemoryTransport,
}

type ResponseListenerFn =
    Box<dyn Fn(PeerKeyLocation, Message) -> conn_manager::Result<()> + Send + Sync>;
type InboundListenerFn = Box<dyn Fn(PeerKey, Message) -> conn_manager::Result<()> + Send + Sync>;

struct ListenerRegistry {
    /// listeners for outbound messages
    outbound: RwLock<HashMap<TransactionId, ResponseListenerFn>>,
    /// listeners for inbound messages
    inbound: RwLock<HashMap<ListenerHandle, InboundListenerFn>>,
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
    pub fn new(is_open: bool, peer: PeerKey, location: Option<Location>) -> Self {
        let transport = InMemoryTransport::new(is_open, peer, location);
        let listeners = Arc::new(
            IntoIter::new(MsgTypeId::enumeration())
                .map(|id| (id, ListenerRegistry::default()))
                .collect(),
        );

        let tr_cp = transport.clone();
        let listeners_cp: Arc<HashMap<_, _>> = Arc::clone(&listeners);

        // evaluate the messages as they arrive
        tokio::spawn(async move {
            loop {
                if let Some(msg) = tr_cp.msg_stack_queue.lock().pop() {
                    let msg_data: Message =
                        bincode::deserialize_from(Cursor::new(msg.data)).unwrap();
                    let listeners = &listeners_cp[&msg_data.msg_type()];
                    if let Some(_prev_tx) = listeners.outbound.read().get(msg_data.id()) {
                        log::debug!("Received response for msg {}", msg_data.id());
                    } else {
                        let reg = listeners.inbound.read();
                        for func in reg.values() {
                            if let Err(err) = func(msg.origin, msg_data.clone()) {
                                log::error!("Error while calling inbound msg handler: {}", err);
                            } else {
                                log::debug!("Successfully processed a message");
                            }
                        }
                    }
                }
            }
        });

        Self {
            listeners,
            transport,
        }
    }
}

impl ConnectionManager for TestingConnectionManager {
    type Transport = InMemoryTransport;

    fn on_remove_conn(&self, _func: RemoveConnHandler) {}

    fn listen<F>(&self, tx_type: MsgTypeId, listen_fn: F) -> ListenerHandle
    where
        F: Fn(PeerKey, Message) -> conn_manager::Result<()> + Send + Sync + 'static,
    {
        let tx_ty_listener = &self.listeners[&tx_type];
        let handle_id = ListenerHandle::new();
        tx_ty_listener
            .inbound
            .write()
            .insert(handle_id, Box::new(listen_fn));

        // TODO: set a background thread doing the listening
        // the handle should have a way to know that something has been received
        // so the protocol can handle the message

        handle_id
    }

    fn listen_to_replies<F>(&self, tx_id: TransactionId, callback: F)
    where
        F: Fn(PeerKeyLocation, Message) -> conn_manager::Result<()> + Send + Sync + 'static,
    {
        let tx_ty_listener = self.listeners.get(&tx_id.msg_type()).unwrap();
        tx_ty_listener
            .outbound
            .write()
            .insert(tx_id, Box::new(callback));
    }

    fn transport(&self) -> &Self::Transport {
        &self.transport
    }

    fn add_connection(&self, _peer_key: PeerKeyLocation, _unsolicited: bool) {}

    fn send_with_callback<F>(
        &self,
        to: PeerKey,
        tx_id: TransactionId,
        msg: Message,
        callback: F,
    ) -> conn_manager::Result<()>
    where
        F: Fn(PeerKeyLocation, Message) -> conn_manager::Result<()> + Send + Sync + 'static,
    {
        // store listening func
        let tx_ty_listener = self.listeners.get(&tx_id.msg_type()).unwrap();
        tx_ty_listener
            .outbound
            .write()
            .insert(tx_id, Box::new(callback));

        // send the msg
        let serialized = bincode::serialize(&msg)?;
        self.transport.send(to, None, serialized);
        Ok(())
    }

    fn send(&self, to: PeerKey, _tx_id: TransactionId, msg: Message) -> conn_manager::Result<()> {
        let serialized = bincode::serialize(&msg)?;
        self.transport.send(to, None, serialized);
        Ok(())
    }
}

static NETWORK_TRAFFIC_SIM: OnceCell<(Sender<MessageOnTransit>, Receiver<MessageOnTransit>)> =
    OnceCell::new();

#[derive(Clone, Debug)]
struct MessageOnTransit {
    origin: PeerKey,
    origin_loc: Option<Location>,
    target: PeerKey,
    data: Vec<u8>,
}

#[derive(Clone, Debug)]
pub(crate) struct InMemoryTransport {
    interface_peer: PeerKey,
    location: Option<Location>,
    is_open: bool,
    /// received messages per each peer awaiting processing
    msg_stack_queue: Arc<Mutex<Vec<MessageOnTransit>>>,
    /// all messages 'traversing' the network at a given time
    network: Sender<MessageOnTransit>,
}

impl InMemoryTransport {
    fn new(is_open: bool, interface_peer: PeerKey, location: Option<Location>) -> Self {
        let msg_stack_queue = Arc::new(Mutex::new(Vec::new()));
        let (tx, _) = NETWORK_TRAFFIC_SIM.get_or_init(|| tokio::sync::broadcast::channel(100));

        // store messages incoming from the network in the msg stack
        let rcv_msg_c = msg_stack_queue.clone();
        let mut rx = tx.subscribe();
        tokio::spawn(async move {
            loop {
                if let Ok(msg) = rx.recv().await {
                    if msg.target == interface_peer {
                        log::debug!("Inbound message received for peer {}", interface_peer);
                        rcv_msg_c.lock().push(msg);
                    }
                }
            }
        });

        Self {
            interface_peer,
            location,
            is_open,
            msg_stack_queue,
            network: tx.clone(),
        }
    }
}

impl Transport for InMemoryTransport {
    fn send(&self, peer: PeerKey, location: Option<Location>, message: Vec<u8>) {
        self.network
            .send(MessageOnTransit {
                origin: self.interface_peer,
                origin_loc: location,
                target: peer,
                data: message,
            })
            .expect("failed to send msg over the network");
    }

    fn is_open(&self) -> bool {
        self.is_open
    }

    fn recipient(&self) -> Channel {
        todo!()
    }
}

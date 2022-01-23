//! A in-memory connection manager and transport implementation. Used for testing pourpouses.
use std::{
    collections::HashMap,
    io::Cursor,
    sync::Arc,
    time::{Duration, Instant},
};

use crossbeam::channel::{self, Receiver, Sender};
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use rand::{prelude::StdRng, thread_rng, Rng, SeedableRng};

use super::{ConnectionBridge, ConnectionError, PeerKey};
use crate::{
    config::{tracing::Logger, GlobalExecutor},
    message::Message,
};

static NETWORK_WIRES: OnceCell<(Sender<MessageOnTransit>, Receiver<MessageOnTransit>)> =
    OnceCell::new();

pub(in crate::node) struct MemoryConnManager {
    pub transport: InMemoryTransport,
    msg_queue: Arc<Mutex<Vec<Message>>>,
    peer: PeerKey,
}

impl MemoryConnManager {
    pub fn new(peer: PeerKey) -> Self {
        Logger::init_logger();
        let transport = InMemoryTransport::new(peer);
        let msg_queue = Arc::new(Mutex::new(Vec::new()));

        let msg_queue_cp = msg_queue.clone();
        let tr_cp = transport.clone();
        GlobalExecutor::spawn(async move {
            // evaluate the messages as they arrive
            loop {
                let msg = { tr_cp.msg_stack_queue.lock().pop() };
                if let Some(msg) = msg {
                    let msg_data: Message =
                        bincode::deserialize_from(Cursor::new(msg.data)).unwrap();
                    if let Some(mut queue) = msg_queue_cp.try_lock() {
                        queue.push(msg_data);
                        std::mem::drop(queue);
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });

        Self {
            transport,
            msg_queue,
            peer,
        }
    }

    pub async fn recv(&self) -> Result<Message, ConnectionError> {
        loop {
            if let Some(mut queue) = self.msg_queue.try_lock() {
                let msg = queue.pop();
                std::mem::drop(queue);
                if let Some(msg) = msg {
                    return Ok(msg);
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

impl Clone for MemoryConnManager {
    fn clone(&self) -> Self {
        Self {
            transport: self.transport.clone(),
            msg_queue: self.msg_queue.clone(),
            peer: self.peer,
        }
    }
}

#[async_trait::async_trait]
impl ConnectionBridge for MemoryConnManager {
    async fn send(&self, target: &PeerKey, msg: Message) -> Result<(), ConnectionError> {
        let msg = bincode::serialize(&msg)?;
        self.transport.send(*target, msg);
        Ok(())
    }

    fn add_connection(&mut self, _peer: PeerKey) -> super::ConnResult<()> {
        Ok(())
    }

    fn drop_connection(&mut self, _peer: &PeerKey) {}
}

#[derive(Clone, Debug)]
struct MessageOnTransit {
    origin: PeerKey,
    target: PeerKey,
    data: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct InMemoryTransport {
    interface_peer: PeerKey,
    /// received messages per each peer awaiting processing
    msg_stack_queue: Arc<Mutex<Vec<MessageOnTransit>>>,
    /// all messages 'traversing' the network at a given time
    network: Sender<MessageOnTransit>,
}

impl InMemoryTransport {
    fn new(interface_peer: PeerKey) -> Self {
        let msg_stack_queue = Arc::new(Mutex::new(Vec::new()));
        let (tx, rx) = NETWORK_WIRES.get_or_init(crossbeam::channel::unbounded);

        // store messages incoming from the network in the msg stack
        let rcv_msg_c = msg_stack_queue.clone();
        let rx = rx.clone();
        let tx_cp = tx.clone();
        GlobalExecutor::spawn(async move {
            const MAX_DELAYED_MSG: usize = 10;
            let mut rng = StdRng::from_entropy();
            // delayed messages per target
            let mut delayed: HashMap<_, Vec<_>> = HashMap::with_capacity(MAX_DELAYED_MSG);
            let last_drain = Instant::now();
            loop {
                match rx.try_recv() {
                    Ok(msg) if msg.target == interface_peer => {
                        log::trace!(
                            "Inbound message received for peer {} from {}",
                            interface_peer,
                            msg.origin
                        );
                        if (rng.gen_bool(0.5) && delayed.len() < MAX_DELAYED_MSG)
                            || delayed.contains_key(&msg.target)
                        {
                            delayed.entry(msg.target).or_default().push(msg);
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        } else {
                            rcv_msg_c.lock().push(msg);
                        }
                    }
                    Ok(msg) => {
                        // send back to the network since this msg belongs to other peer
                        tx_cp.send(msg).expect("failed to send msg back to network");
                        tokio::time::sleep(Duration::from_nanos(1_000)).await
                    }
                    Err(channel::TryRecvError::Disconnected) => break,
                    Err(channel::TryRecvError::Empty) => {
                        tokio::time::sleep(Duration::from_millis(10)).await
                    }
                }
                if (last_drain.elapsed() > Duration::from_millis(rng.gen_range(1_000..5_000))
                    && !delayed.is_empty())
                    || delayed.len() == MAX_DELAYED_MSG
                {
                    let mut queue = rcv_msg_c.lock();
                    for (_, msgs) in delayed.drain() {
                        queue.extend(msgs);
                    }
                    Self::shuffle(&mut *queue);
                }
            }
            log::error!("Stopped receiving messages in {}", interface_peer);
        });

        let network = tx.clone();
        Self {
            interface_peer,
            msg_stack_queue,
            network,
        }
    }

    fn send(&self, peer: PeerKey, message: Vec<u8>) {
        let send_res = self.network.send(MessageOnTransit {
            origin: self.interface_peer,
            target: peer,
            data: message,
        });
        if let Err(channel::SendError(_)) = send_res {
            log::error!("Network shutdown")
        }
    }

    fn shuffle<T>(iter: &mut Vec<T>) {
        let mut rng = thread_rng();
        for i in (1..(iter.len() - 1)).rev() {
            let idx = rng.gen_range(0..=i);
            iter.swap(idx, i);
        }
    }
}

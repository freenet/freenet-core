//! A in-memory connection manager and transport implementation. Used for testing purposes.
use std::{
    collections::HashMap,
    io::Cursor,
    sync::Arc,
    time::{Duration, Instant},
};

use crossbeam::channel::{self, Receiver, Sender};
use once_cell::sync::OnceCell;
use rand::{prelude::StdRng, seq::SliceRandom, Rng, SeedableRng};
use tokio::sync::Mutex;

use super::{ConnectionError, NetworkBridge, PeerId};
use crate::{
    config::GlobalExecutor,
    message::NetMessage,
    node::{testing_impl::NetworkBridgeExt, NetEventRegister, OpManager},
    tracing::NetEventLog,
};

#[derive(Clone)]
pub(in crate::node) struct MemoryConnManager {
    transport: InMemoryTransport,
    log_register: Arc<dyn NetEventRegister>,
    op_manager: Arc<OpManager>,
    msg_queue: Arc<Mutex<Vec<NetMessage>>>,
}

impl MemoryConnManager {
    pub fn new(
        peer: PeerId,
        log_register: impl NetEventRegister,
        op_manager: Arc<OpManager>,
        add_noise: bool,
    ) -> Self {
        let transport = InMemoryTransport::new(peer, add_noise);
        let msg_queue = Arc::new(Mutex::new(Vec::new()));

        let msg_queue_cp = msg_queue.clone();
        let transport_cp = transport.clone();
        GlobalExecutor::spawn(async move {
            // evaluate the messages as they arrive
            loop {
                let Some(msg) = transport_cp.msg_stack_queue.lock().await.pop() else {
                    continue;
                };
                let msg_data: NetMessage =
                    bincode::deserialize_from(Cursor::new(msg.data)).unwrap();
                msg_queue_cp.lock().await.push(msg_data);
            }
        });

        Self {
            transport,
            log_register: Arc::new(log_register),
            op_manager,
            msg_queue,
        }
    }
}

impl NetworkBridge for MemoryConnManager {
    async fn send(&self, target: &PeerId, msg: NetMessage) -> super::ConnResult<()> {
        self.log_register
            .register_events(NetEventLog::from_outbound_msg(&msg, &self.op_manager.ring))
            .await;
        self.op_manager.sending_transaction(target, &msg);
        let msg = bincode::serialize(&msg)?;
        self.transport.send(target.clone(), msg);
        Ok(())
    }

    async fn drop_connection(&mut self, _peer: &PeerId) -> super::ConnResult<()> {
        Ok(())
    }
}

impl NetworkBridgeExt for MemoryConnManager {
    async fn recv(&mut self) -> Result<NetMessage, ConnectionError> {
        loop {
            let mut queue = self.msg_queue.lock().await;
            let Some(msg) = queue.pop() else {
                std::mem::drop(queue);
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            };
            return Ok(msg);
        }
    }
}

#[derive(Clone, Debug)]
struct MessageOnTransit {
    origin: PeerId,
    target: PeerId,
    data: Vec<u8>,
}

static NETWORK_WIRES: OnceCell<(Sender<MessageOnTransit>, Receiver<MessageOnTransit>)> =
    OnceCell::new();

#[derive(Clone, Debug)]
struct InMemoryTransport {
    interface_peer: PeerId,
    /// received messages per each peer awaiting processing
    msg_stack_queue: Arc<Mutex<Vec<MessageOnTransit>>>,
    /// all messages 'traversing' the network at a given time
    network: Sender<MessageOnTransit>,
}

impl InMemoryTransport {
    fn new(interface_peer: PeerId, add_noise: bool) -> Self {
        let msg_stack_queue = Arc::new(Mutex::new(Vec::new()));
        let (network_tx, network_rx) = NETWORK_WIRES.get_or_init(crossbeam::channel::unbounded);

        // store messages incoming from the network in the msg stack
        let msg_stack_queue_cp = msg_stack_queue.clone();
        let network_tx_cp = network_tx.clone();
        let ip = interface_peer.clone();
        GlobalExecutor::spawn(async move {
            const MAX_DELAYED_MSG: usize = 10;
            let mut rng = StdRng::from_entropy();
            // delayed messages per target
            let mut delayed: HashMap<_, Vec<_>> = HashMap::with_capacity(MAX_DELAYED_MSG);
            let last_drain = Instant::now();
            loop {
                match network_rx.try_recv() {
                    Ok(msg) if msg.target == ip => {
                        tracing::trace!(
                            "Inbound message received for peer {} from {}",
                            ip,
                            msg.origin
                        );
                        if rng.gen_bool(0.5) && delayed.len() < MAX_DELAYED_MSG && add_noise {
                            delayed
                                .entry(msg.target.clone())
                                .or_default()
                                .push(msg.clone());
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        } else {
                            let mut queue = msg_stack_queue_cp.lock().await;
                            queue.push(msg);
                            if add_noise && rng.gen_bool(0.2) {
                                queue.shuffle(&mut rng);
                            }
                        }
                    }
                    Ok(msg) => {
                        // send back to the network since this msg belongs to other peer
                        network_tx_cp
                            .send(msg)
                            .expect("failed to send msg back to network");
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
                    let mut queue = msg_stack_queue_cp.lock().await;
                    for (_, msgs) in delayed.drain() {
                        queue.extend(msgs);
                    }
                    let queue = &mut queue;
                    queue.shuffle(&mut rng);
                }
            }
            tracing::error!("Stopped receiving messages in {ip}");
        });

        Self {
            interface_peer,
            msg_stack_queue,
            network: network_tx.clone(),
        }
    }

    fn send(&self, peer: PeerId, message: Vec<u8>) {
        let send_res = self.network.send(MessageOnTransit {
            origin: self.interface_peer.clone(),
            target: peer,
            data: message,
        });
        if let Err(channel::SendError(_)) = send_res {
            tracing::error!("Network shutdown")
        }
    }
}

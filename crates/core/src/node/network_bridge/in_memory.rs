//! A in-memory connection manager and transport implementation. Used for testing purposes.
use std::{
    collections::HashMap,
    io::Cursor,
    net::SocketAddr,
    sync::{Arc, LazyLock, RwLock},
    time::Duration,
};

/// Divisor for determining shuffle probability in noise mode.
/// When `msg_hash % NOISE_SHUFFLE_DIVISOR == 0`, the message queue is shuffled.
/// A value of 5 gives approximately 20% shuffle probability.
const NOISE_SHUFFLE_DIVISOR: u64 = 5;

use crossbeam::channel::{self, Sender};
use rand::{prelude::StdRng, seq::SliceRandom, SeedableRng};
use tokio::sync::Mutex;

use super::{ConnectionError, NetworkBridge};
use crate::{
    config::GlobalExecutor,
    message::NetMessage,
    node::{testing_impl::NetworkBridgeExt, NetEventRegister, OpManager, PeerId},
    ring::PeerKeyLocation,
    tracing::NetEventLog,
    transport::TransportPublicKey,
};

/// Registry that maps socket addresses to their dedicated message channels.
/// This enables direct peer-to-peer message routing without busy-polling.
#[derive(Default)]
struct PeerRegistry {
    /// Maps peer socket address to (public_key, message_sender)
    peers: HashMap<SocketAddr, (TransportPublicKey, Sender<MessageOnTransit>)>,
}

impl PeerRegistry {
    fn register(
        &mut self,
        addr: SocketAddr,
        pub_key: TransportPublicKey,
        sender: Sender<MessageOnTransit>,
    ) {
        self.peers.insert(addr, (pub_key, sender));
    }

    fn unregister(&mut self, addr: &SocketAddr) {
        self.peers.remove(addr);
    }

    fn send(&self, target_addr: SocketAddr, msg: MessageOnTransit) -> Result<(), ConnectionError> {
        if let Some((_, sender)) = self.peers.get(&target_addr) {
            sender
                .send(msg)
                .map_err(|_| ConnectionError::SendNotCompleted(target_addr))
        } else {
            tracing::warn!("No peer registered at {}", target_addr);
            Err(ConnectionError::SendNotCompleted(target_addr))
        }
    }

    fn get_public_key(&self, addr: &SocketAddr) -> Option<TransportPublicKey> {
        self.peers.get(addr).map(|(pk, _)| pk.clone())
    }
}

/// Global peer registry for all in-memory peers
static PEER_REGISTRY: LazyLock<RwLock<PeerRegistry>> =
    LazyLock::new(|| RwLock::new(PeerRegistry::default()));

#[derive(Clone)]
pub(in crate::node) struct MemoryConnManager {
    transport: InMemoryTransport,
    log_register: Arc<dyn NetEventRegister>,
    op_manager: Arc<OpManager>,
    /// Queue of received messages with their source addresses
    msg_queue: Arc<Mutex<Vec<(NetMessage, SocketAddr)>>>,
}

impl MemoryConnManager {
    pub fn new(
        peer: PeerId,
        log_register: impl NetEventRegister,
        op_manager: Arc<OpManager>,
        add_noise: bool,
        rng_seed: u64,
    ) -> Self {
        let transport = InMemoryTransport::new(peer, add_noise, rng_seed);
        let msg_queue = Arc::new(Mutex::new(Vec::new()));

        let msg_queue_cp = msg_queue.clone();
        let transport_cp = transport.clone();
        GlobalExecutor::spawn(async move {
            // evaluate the messages as they arrive
            loop {
                let Some(msg) = transport_cp.msg_stack_queue.lock().await.pop() else {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    continue;
                };
                let source_addr = msg.origin.addr;
                let msg_data: NetMessage =
                    bincode::deserialize_from(Cursor::new(msg.data)).unwrap();
                msg_queue_cp.lock().await.push((msg_data, source_addr));
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
    async fn send(&self, target_addr: SocketAddr, msg: NetMessage) -> super::ConnResult<()> {
        self.log_register
            .register_events(NetEventLog::from_outbound_msg(&msg, &self.op_manager.ring))
            .await;

        // Look up the target peer's public key from the registry
        let target_pub_key = {
            let registry = PEER_REGISTRY.read().unwrap();
            registry.get_public_key(&target_addr)
        };

        let target_pub_key = target_pub_key.ok_or_else(|| {
            tracing::warn!("No peer registered at target address {}", target_addr);
            ConnectionError::SendNotCompleted(target_addr)
        })?;

        // Create correct PeerId with target's public key
        let target_peer_id = PeerId::new(target_addr, target_pub_key.clone());

        // Create PeerKeyLocation for op_manager tracking
        let target_peer = PeerKeyLocation::new(target_pub_key, target_addr);
        self.op_manager.sending_transaction(&target_peer, &msg);

        let msg = bincode::serialize(&msg)?;
        self.transport.send(target_peer_id, msg)?;
        Ok(())
    }

    async fn drop_connection(&mut self, peer_addr: SocketAddr) -> super::ConnResult<()> {
        tracing::debug!("Dropping in-memory connection to {}", peer_addr);
        PEER_REGISTRY.write().unwrap().unregister(&peer_addr);
        Ok(())
    }
}

impl NetworkBridgeExt for MemoryConnManager {
    async fn recv(&mut self) -> Result<(NetMessage, Option<SocketAddr>), ConnectionError> {
        loop {
            let mut queue = self.msg_queue.lock().await;
            let Some((msg, source_addr)) = queue.pop() else {
                std::mem::drop(queue);
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            };
            return Ok((msg, Some(source_addr)));
        }
    }
}

#[derive(Clone, Debug)]
struct MessageOnTransit {
    origin: PeerId,
    #[allow(dead_code)] // Kept for debugging and potential future use
    target: PeerId,
    data: Vec<u8>,
}

#[derive(Clone, Debug)]
struct InMemoryTransport {
    interface_peer: PeerId,
    /// received messages per each peer awaiting processing
    msg_stack_queue: Arc<Mutex<Vec<MessageOnTransit>>>,
}

impl InMemoryTransport {
    fn new(interface_peer: PeerId, add_noise: bool, rng_seed: u64) -> Self {
        let msg_stack_queue = Arc::new(Mutex::new(Vec::new()));

        // Create a dedicated channel for this peer
        let (tx, rx) = channel::unbounded();

        // Register this peer in the global registry
        {
            let mut registry = PEER_REGISTRY.write().unwrap();
            registry.register(interface_peer.addr, interface_peer.pub_key.clone(), tx);
        }

        // Spawn a task to receive messages from our dedicated channel
        let msg_stack_queue_cp = msg_stack_queue.clone();
        let ip = interface_peer.clone();
        GlobalExecutor::spawn(async move {
            loop {
                match rx.try_recv() {
                    Ok(msg) => {
                        tracing::trace!(
                            "Inbound message received for peer {} from {}",
                            ip,
                            msg.origin
                        );
                        let mut queue = msg_stack_queue_cp.lock().await;

                        if add_noise {
                            // Derive shuffle decision from message content for determinism
                            // This avoids depending on async arrival order
                            let msg_hash = Self::hash_message(&msg.data);
                            queue.push(msg);

                            // Shuffle based on message content (probability ~1/NOISE_SHUFFLE_DIVISOR)
                            if msg_hash % NOISE_SHUFFLE_DIVISOR == 0 {
                                // Derive shuffle seed from base seed + message hash
                                let shuffle_seed = rng_seed.wrapping_add(msg_hash);
                                let mut shuffle_rng = StdRng::seed_from_u64(shuffle_seed);
                                queue.shuffle(&mut shuffle_rng);
                            }
                        } else {
                            queue.push(msg);
                        }
                    }
                    Err(channel::TryRecvError::Disconnected) => {
                        tracing::debug!("Channel closed for peer {}", ip);
                        break;
                    }
                    Err(channel::TryRecvError::Empty) => {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                }
            }
            tracing::debug!("Stopped receiving messages in {ip}");
        });

        Self {
            interface_peer,
            msg_stack_queue,
        }
    }

    /// Compute a simple hash of message data for deterministic shuffle decisions.
    /// Uses FNV-1a for speed (we don't need cryptographic strength here).
    fn hash_message(data: &[u8]) -> u64 {
        const FNV_OFFSET: u64 = 0xcbf29ce484222325;
        const FNV_PRIME: u64 = 0x100000001b3;

        let mut hash = FNV_OFFSET;
        for byte in data {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        hash
    }

    fn send(&self, target: PeerId, message: Vec<u8>) -> Result<(), ConnectionError> {
        let msg = MessageOnTransit {
            origin: self.interface_peer.clone(),
            target: target.clone(),
            data: message,
        };

        // Send directly to the target peer's channel
        let registry = PEER_REGISTRY.read().unwrap();
        registry.send(target.addr, msg)
    }
}

impl Drop for InMemoryTransport {
    fn drop(&mut self) {
        // Unregister from the global registry when dropped
        let mut registry = PEER_REGISTRY.write().unwrap();
        registry.unregister(&self.interface_peer.addr);
    }
}

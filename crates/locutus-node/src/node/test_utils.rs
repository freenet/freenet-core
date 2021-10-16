use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener};

use libp2p::identity;
use rand::Rng;
use tokio::sync::mpsc;

use crate::{
    conn_manager::{ConnectionBridge, PeerKey, Transport},
    contract::ContractHandlerChannel,
    message::Message,
    node::{InitPeerNode, NodeInMemory},
    operations::{
        join_ring::{handle_join_ring, JoinRingMsg},
        OpError,
    },
    ring::{Distance, Location, Ring},
    NodeConfig,
};

use super::OpManager;

pub fn get_free_port() -> Result<u16, ()> {
    let mut port;
    for _ in 0..100 {
        port = get_dynamic_port();
        let bind_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, port));
        if let Ok(conn) = TcpListener::bind(bind_addr) {
            std::mem::drop(conn);
            return Ok(port);
        }
    }
    Err(())
}

pub fn get_dynamic_port() -> u16 {
    const FIRST_DYNAMIC_PORT: u16 = 49152;
    const LAST_DYNAMIC_PORT: u16 = 65535;
    rand::thread_rng().gen_range(FIRST_DYNAMIC_PORT..LAST_DYNAMIC_PORT)
}

pub(crate) fn get_test_op_storage() -> OpManager<String> {
    let ring = Ring::new();
    let (notification_tx, _notification_channel) = mpsc::channel(100);
    let ch_handler = ContractHandlerChannel::new();
    OpManager::new(ring, notification_tx, ch_handler)
}

pub(crate) struct SimNetwork {
    // gateways: HashMap<String, InMemory>,
    // peers: HashMap<String, InMemory>,
    meta_info_tx: mpsc::Sender<Result<NetEvent, OpError<String>>>,
    meta_info_rx: mpsc::Receiver<Result<NetEvent, OpError<String>>>,
}

pub(crate) struct NetEvent {
    pub(crate) event: EventType,
}

pub(crate) enum EventType {
    /// A peer joined the network through some gateway.
    JoinSuccess { peer: PeerKey },
}

impl SimNetwork {
    pub fn build(network_size: usize, ring_max_htl: usize, rnd_if_htl_above: usize) -> SimNetwork {
        let sim = SimNetwork::new();

        // build gateway node
        // let probe_protocol = Some(ProbeProtocol::new(ring_protocol.clone(), loc));
        let gateway_pair = identity::Keypair::generate_ed25519();
        let gateway_peer_id = gateway_pair.public().into_peer_id();
        let gateway_port = get_free_port().unwrap();
        let gateway_loc = Location::random();
        let config = NodeConfig::new()
            .with_ip(Ipv6Addr::LOCALHOST)
            .with_port(gateway_port)
            .with_key(gateway_pair)
            .with_location(gateway_loc)
            .max_hops_to_live(ring_max_htl)
            .rnd_if_htl_above(rnd_if_htl_above);
        let gateway = NodeInMemory::build(config).unwrap();
        sim.initialize_gateway(gateway, "gateway".to_owned());

        // add other nodes to the simulation
        for node_no in 0..network_size {
            let label = format!("node-{}", node_no);
            let config = NodeConfig::new()
                .add_provider(
                    InitPeerNode::new()
                        .listening_ip(Ipv6Addr::LOCALHOST)
                        .listening_port(gateway_port)
                        .with_identifier(gateway_peer_id)
                        .with_location(gateway_loc),
                )
                .max_hops_to_live(ring_max_htl)
                .rnd_if_htl_above(rnd_if_htl_above);
            sim.initialize_peer(NodeInMemory::build(config).unwrap(), label);
        }
        sim
    }

    pub async fn recv_net_events(&mut self) -> Option<Result<NetEvent, OpError<String>>> {
        self.meta_info_rx.recv().await
    }

    fn new() -> Self {
        let (meta_info_tx, meta_info_rx) = mpsc::channel(100);
        Self {
            meta_info_rx,
            meta_info_tx,
        }
    }

    fn initialize_gateway(&self, gateway: NodeInMemory, sender_label: String) {
        let info_ch = self.meta_info_tx.clone();
        tokio::spawn(Self::listen(gateway, info_ch, sender_label));
    }

    fn initialize_peer(&self, mut peer: NodeInMemory, sender_label: String) {
        let info_ch = self.meta_info_tx.clone();
        tokio::spawn(async move {
            if peer.join_ring().await.is_err() {
                let _ = info_ch.send(Err(OpError::IllegalStateTransition)).await;
                return Err(());
            }
            Self::listen(peer, info_ch, sender_label).await
        });
    }

    async fn listen(
        mut gateway: NodeInMemory,
        info_ch: mpsc::Sender<Result<NetEvent, OpError<String>>>,
        _sender: String,
    ) -> Result<(), ()> {
        while let Ok(msg) = gateway.conn_manager.recv().await {
            if let Message::JoinRing(msg) = msg {
                if let JoinRingMsg::Connected { target, .. } = msg {
                    let _ = info_ch
                        .send(Ok(NetEvent {
                            event: EventType::JoinSuccess { peer: target.peer },
                        }))
                        .await;
                    break;
                }
                match handle_join_ring(&gateway.op_storage, &mut gateway.conn_manager, msg).await {
                    Err(err) => {
                        let _ = info_ch.send(Err(err)).await;
                    }
                    Ok(()) => {}
                }
            } else {
                return Err(());
            }
        }
        Ok(())
    }
}

/// Builds an histogram of the distribution in the ring of each node relative to each other.
fn _ring_distribution<'a>(
    nodes: impl Iterator<Item = &'a NodeInMemory> + 'a,
) -> impl Iterator<Item = Distance> + 'a {
    // TODO: groupby  certain intervals
    // e.g. grouping func: (it * 200.0).roundToInt().toDouble() / 200.0
    nodes
        .map(|node| {
            let node_ring = &node.op_storage.ring;
            let self_loc = node.conn_manager.transport.location().unwrap();
            node_ring
                .connections_by_location
                .read()
                .keys()
                .map(|d| self_loc.distance(d))
                .collect::<Vec<_>>()
        })
        .flatten()
}

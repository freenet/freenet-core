#![allow(unused)] // FIXME: remove this attr

use std::{
    collections::{BTreeMap, HashSet},
    convert::TryFrom,
    fmt::Display,
    hash::Hasher,
    sync::Arc,
    time::{Duration, Instant},
};

use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};

use crate::{
    conn_manager::{self, ConnectionBridge, ListenerHandle, PeerKey, PeerKeyLocation, Transport},
    message::{Message, TransactionType, Transaction},
    ring_proto::messages::{JoinRequest, JoinResponse},
    StdResult,
};

type Result<T> = StdResult<T, RingProtoError>;

pub(crate) struct RingProtocol<CM> {
    pub conn_manager: Arc<CM>,
    peer_key: PeerKey,
    /// A location gets assigned once a node joins the network via a gateway,
    /// until then it has no location unless the node is a gateway.
    pub location: RwLock<Option<Location>>,
    gateways: RwLock<HashSet<PeerKeyLocation>>,
    max_hops_to_live: usize,
    rnd_if_htl_above: usize,
    pub ring: Ring,
}

impl<CM, T> RingProtocol<CM>
where
    T: Transport + 'static,
    CM: ConnectionBridge<Transport = T> + 'static,
{
    fn new(
        conn_manager: CM,
        peer_key: PeerKey,
        max_hops_to_live: usize,
        rnd_if_htl_above: usize,
    ) -> Arc<Self> {
        Arc::new(RingProtocol {
            conn_manager: Arc::new(conn_manager),
            peer_key,
            location: RwLock::new(None),
            gateways: RwLock::new(HashSet::new()),
            max_hops_to_live,
            rnd_if_htl_above,
            ring: Ring::new(),
        })
    }

    pub fn with_location(self: Arc<Self>, loc: Location) -> Arc<Self> {
        *self.location.write() = Some(loc);
        self
    }

    fn listen_for_close_conn(&self) {
        todo!()
    }

}

#[derive(thiserror::Error, Debug)]
pub(crate) enum RingProtoError {
    #[error("failed while attempting to join a ring")]
    Join,
    #[error(transparent)]
    ConnError(#[from] conn_manager::ConnError),
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use libp2p::identity;
    use rand::Rng;

    use super::{messages::OpenConnection, *};
    use crate::{
        config::tracing::Logger,
        conn_manager::in_memory::MemoryConnManager,
        message::ProbeRequest,
        probe_proto::{self, ProbeProtocol},
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn node0_to_gateway_conn() -> StdResult<(), Box<dyn std::error::Error>> {
        //! Given a network of one node and one gateway test that both are connected.
        Logger::init_logger();

        let ring_protocols = sim_network_builder(1, 1, 0);
        tokio::time::sleep(Duration::from_secs(3)).await;

        assert_eq!(
            ring_protocols["node-0"]
                .ring_protocol
                .ring
                .connections_by_location
                .read()
                .len(),
            1
        );

        assert_eq!(
            ring_protocols["gateway"]
                .ring_protocol
                .ring
                .connections_by_location
                .read()
                .len(),
            1
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn all_nodes_should_connect() -> StdResult<(), Box<dyn std::error::Error>> {
        //! Given a network of 1000 peers all nodes should have connections.
        Logger::init_logger();

        let mut sim_nodes = sim_network_builder(10, 10, 7);
        tokio::time::sleep(Duration::from_secs(300)).await;
        // let _hist: Vec<_> = _ring_distribution(sim_nodes.values()).collect();

        const NUM_PROBES: usize = 10;
        let mut probe_responses = Vec::with_capacity(NUM_PROBES);
        for probe_idx in 0..NUM_PROBES {
            let target = Location::random();
            let idx: usize = rand::thread_rng().gen_range(0..sim_nodes.len());
            let rnd_node = sim_nodes
                .get_mut(&format!("node-{}", idx))
                .ok_or("node not found")?;
            let probe_response = ProbeProtocol::probe(
                rnd_node.ring_protocol.clone(),
                Transaction::new(<ProbeRequest as TransactionType>::msg_type_id()),
                ProbeRequest {
                    hops_to_live: 7,
                    target,
                },
            )
            .await
            .expect("failed to get probe response");
            probe_responses.push(probe_response);
        }
        // probe_proto::utils::plot_probe_responses(probe_responses);

        let any_empties = sim_nodes
            .values()
            .map(|node| {
                node.ring_protocol
                    .ring
                    .connections_by_location
                    .read()
                    .is_empty()
            })
            .any(|is_empty| is_empty);
        assert!(!any_empties);

        Ok(())
    }

    struct SimulatedNode {
        ring_protocol: Arc<RingProtocol<MemoryConnManager>>,
        probe_protocol: Option<ProbeProtocol>,
    }

    fn sim_network_builder(
        network_size: usize,
        ring_max_htl: usize,
        rnd_if_htl_above: usize,
        // _per_node_delay: usize,
    ) -> HashMap<String, SimulatedNode> {
        let mut nodes = HashMap::new();

        // build gateway node
        let keypair = identity::Keypair::generate_ed25519();
        let gw_key = keypair.public().into();
        let loc = Location::random();
        let conn_manager = MemoryConnManager::new(true, gw_key, Some(loc));
        let ring_protocol = RingProtocol::new(conn_manager, gw_key, ring_max_htl, rnd_if_htl_above)
            .with_location(loc);
        let probe_protocol = Some(ProbeProtocol::new(ring_protocol.clone(), loc));
        ring_protocol.listen_for_join_req();
        nodes.insert(
            "gateway".to_owned(),
            SimulatedNode {
                ring_protocol,
                probe_protocol,
            },
        );

        // add other nodes to the simulation
        for node_no in 0..network_size {
            let label = format!("node-{}", node_no);
            let keypair = identity::Keypair::generate_ed25519();
            let peer_key = keypair.public().into();
            let conn_manager = MemoryConnManager::new(false, peer_key, None);
            let ring_protocol =
                RingProtocol::new(conn_manager, peer_key, ring_max_htl, rnd_if_htl_above);
            ring_protocol.gateways.write().insert(PeerKeyLocation {
                peer: gw_key,
                location: Some(loc),
            });
            ring_protocol.join_ring().unwrap();

            nodes.insert(
                label,
                SimulatedNode {
                    ring_protocol,
                    probe_protocol: None,
                },
            );
        }
        nodes
    }

    /// Builds an histogram of the distribution in the ring of each node relative to each other.
    fn _ring_distribution<'a>(
        nodes: impl Iterator<Item = &'a SimulatedNode> + 'a,
    ) -> impl Iterator<Item = Distance> + 'a {
        // TODO: groupby  certain intervals
        // e.g. grouping func: (it * 200.0).roundToInt().toDouble() / 200.0
        nodes
            .map(|node| {
                let node_ring = &node.ring_protocol.ring;
                let self_loc = node.ring_protocol.location.read().unwrap();
                node_ring
                    .connections_by_location
                    .read()
                    .keys()
                    .map(|d| self_loc.distance(d))
                    .collect::<Vec<_>>()
            })
            .flatten()
    }
}

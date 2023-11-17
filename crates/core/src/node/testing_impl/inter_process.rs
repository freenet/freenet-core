use std::sync::Arc;

use crate::{
    contract::{ExecutorToEventLoopChannel, NetworkEventListenerHalve},
    dev_tool::PeerKey,
    node::{
        network_bridge::{inter_process::InterProcessConnManager, EventLoopNotifications},
        NetEventRegister, OpManager,
    },
    ring::PeerKeyLocation,
};

use super::{Builder, DefaultRegistry};

pub struct SimPeer(pub(super) Builder<DefaultRegistry>);

impl SimPeer {
    pub fn peer_key(&self) -> PeerKey {
        self.0.peer_key
    }
}

struct InterProcessNode {
    peer_key: PeerKey,
    op_storage: Arc<OpManager>,
    gateways: Vec<PeerKeyLocation>,
    notification_channel: EventLoopNotifications,
    conn_manager: InterProcessConnManager,
    event_register: Box<dyn NetEventRegister>,
    is_gateway: bool,
    _executor_listener: ExecutorToEventLoopChannel<NetworkEventListenerHalve>,
}

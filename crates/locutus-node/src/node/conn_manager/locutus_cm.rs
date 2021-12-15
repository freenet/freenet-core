use std::net::IpAddr;

use futures::future;
use libp2p::{
    core::{muxing, transport, UpgradeInfo},
    identify,
    identity::Keypair,
    multiaddr::Protocol,
    ping,
    swarm::{
        protocols_handler::{InboundUpgradeSend, OutboundUpgradeSend},
        KeepAlive, NegotiatedSubstream, NetworkBehaviour, ProtocolsHandler, ProtocolsHandlerEvent,
        ProtocolsHandlerUpgrErr, SubstreamProtocol, SwarmBuilder,
    },
    InboundUpgrade, Multiaddr, OutboundUpgrade, PeerId, Swarm,
};

use crate::{config::GlobalExecutor, NodeConfig};

use super::ConnectionError;

const CURRENT_AGENT_VER: &str = "locutus/agent/0.1.0";
const CURRENT_PROTOC_VER: &[u8] = b"locutus/agent/0.1.0";
const CURRENT_IDENTIFY_PROTOC_VER: &str = "id/1.0.0";

fn config_behaviour(local_key: &Keypair) -> NetBehaviour {
    let ident_config =
        identify::IdentifyConfig::new(CURRENT_IDENTIFY_PROTOC_VER.to_string(), local_key.public())
            .with_agent_version(CURRENT_AGENT_VER.to_string());

    let ping = if cfg!(debug_assertions) {
        ping::Ping::new(ping::PingConfig::new().with_keep_alive(true))
    } else {
        ping::Ping::default()
    };
    NetBehaviour {
        identify: identify::Identify::new(ident_config),
        ping,
        locutus: LocutusBehaviour {},
    }
}

/// Small helper function to convert a tuple composed of an IP address and a port
/// to a libp2p Multiaddr type.
fn multiaddr_from_connection(conn: (IpAddr, u16)) -> Multiaddr {
    let mut addr = Multiaddr::with_capacity(2);
    addr.push(Protocol::from(conn.0));
    addr.push(Protocol::Tcp(conn.1));
    addr
}

pub(in crate::node) struct LocutusConnManager {
    pub swarm: Swarm<NetBehaviour>,
    listen_on: Option<(IpAddr, u16)>,
}

impl LocutusConnManager {
    pub fn build(
        transport: transport::Boxed<(PeerId, muxing::StreamMuxerBox)>,
        config: &NodeConfig,
    ) -> Self {
        // We set a global executor which is virtually the Tokio multi-threaded executor
        // to reuse it's thread pool and scheduler in order to drive futures.
        let global_executor = Box::new(GlobalExecutor);
        let builder = SwarmBuilder::new(
            transport,
            config_behaviour(&config.local_key),
            PeerId::from(config.local_key.public()),
        )
        .executor(global_executor);
        let swarm = builder.build();
        LocutusConnManager {
            swarm,
            listen_on: config.local_ip.zip(config.local_port),
        }
    }

    pub fn listen_on(&mut self) -> Result<(), anyhow::Error> {
        if let Some(conn) = self.listen_on {
            let listening_addr = multiaddr_from_connection(conn);
            self.swarm.listen_on(listening_addr).unwrap();
            Ok(())
        } else {
            anyhow::bail!("failed listening to connections")
        }
    }
}

pub(crate) struct LocutusBehaviour {}

impl NetworkBehaviour for LocutusBehaviour {
    type ProtocolsHandler = Handler;

    type OutEvent = LocutusEvent;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        todo!()
    }

    fn inject_event(
        &mut self,
        peer_id: PeerId,
        connection: libp2p::core::connection::ConnectionId,
        event: <<Self::ProtocolsHandler as libp2p::swarm::IntoProtocolsHandler>::Handler as libp2p::swarm::ProtocolsHandler>::OutEvent,
    ) {
        todo!()
    }

    fn poll(
        &mut self,
        cx: &mut std::task::Context<'_>,
        params: &mut impl libp2p::swarm::PollParameters,
    ) -> std::task::Poll<
        libp2p::swarm::NetworkBehaviourAction<Self::OutEvent, Self::ProtocolsHandler>,
    > {
        todo!()
    }
}

pub(crate) struct Handler {}

impl ProtocolsHandler for Handler {
    type InEvent = ();

    type OutEvent = ();

    type Error = ConnectionError;

    type InboundProtocol = LocutusProtocol;

    type OutboundProtocol = LocutusProtocol;

    type InboundOpenInfo = ();

    type OutboundOpenInfo = ();

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        todo!()
    }

    fn inject_fully_negotiated_inbound(
        &mut self,
        protocol: <Self::InboundProtocol as InboundUpgradeSend>::Output,
        info: Self::InboundOpenInfo,
    ) {
        todo!()
    }

    fn inject_fully_negotiated_outbound(
        &mut self,
        protocol: <Self::OutboundProtocol as OutboundUpgradeSend>::Output,
        info: Self::OutboundOpenInfo,
    ) {
        todo!()
    }

    fn inject_event(&mut self, event: Self::InEvent) {
        todo!()
    }

    fn inject_dial_upgrade_error(
        &mut self,
        info: Self::OutboundOpenInfo,
        error: ProtocolsHandlerUpgrErr<<Self::OutboundProtocol as OutboundUpgradeSend>::Error>,
    ) {
        todo!()
    }

    fn connection_keep_alive(&self) -> KeepAlive {
        todo!()
    }

    fn poll(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<
        ProtocolsHandlerEvent<
            Self::OutboundProtocol,
            Self::OutboundOpenInfo,
            Self::OutEvent,
            Self::Error,
        >,
    > {
        todo!()
    }
}

pub(crate) struct LocutusProtocol {}

impl UpgradeInfo for LocutusProtocol {
    type Info = &'static [u8];
    type InfoIter = std::iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        std::iter::once(CURRENT_PROTOC_VER)
    }
}

impl InboundUpgrade<NegotiatedSubstream> for LocutusProtocol {
    type Output = NegotiatedSubstream;
    type Error = ConnectionError;
    type Future = future::Ready<Result<Self::Output, Self::Error>>;

    fn upgrade_inbound(self, stream: NegotiatedSubstream, _: Self::Info) -> Self::Future {
        // TODO: check protocol ver
        future::ok(stream)
    }
}

impl OutboundUpgrade<NegotiatedSubstream> for LocutusProtocol {
    type Output = NegotiatedSubstream;
    type Error = ConnectionError;
    type Future = future::Ready<Result<Self::Output, Self::Error>>;

    fn upgrade_outbound(self, stream: NegotiatedSubstream, _: Self::Info) -> Self::Future {
        // TODO: check protocol ver
        future::ok(stream)
    }
}

/// The network behaviour implements the following capabilities:
///
/// - [Identify](https://github.com/libp2p/specs/tree/master/identify) libp2p protocol.
/// - Pinging between peers.
#[derive(libp2p::NetworkBehaviour)]
#[behaviour(event_process = false)]
#[behaviour(out_event = "NetEvent")]
pub(crate) struct NetBehaviour {
    identify: identify::Identify,
    ping: ping::Ping,
    locutus: LocutusBehaviour,
}

#[derive(Debug)]
pub(crate) enum NetEvent {
    Identify(Box<identify::IdentifyEvent>),
    Ping(ping::PingEvent),
    Locutus(LocutusEvent),
}

impl From<identify::IdentifyEvent> for NetEvent {
    fn from(event: identify::IdentifyEvent) -> NetEvent {
        Self::Identify(Box::new(event))
    }
}

impl From<ping::PingEvent> for NetEvent {
    fn from(event: ping::PingEvent) -> NetEvent {
        Self::Ping(event)
    }
}

#[derive(Debug)]
pub(crate) enum LocutusEvent {}

impl From<LocutusEvent> for NetEvent {
    fn from(event: LocutusEvent) -> NetEvent {
        Self::Locutus(event)
    }
}

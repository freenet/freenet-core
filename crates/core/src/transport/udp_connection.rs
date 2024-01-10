use crate::transport::errors::ConnectionError;
use crate::transport::udp_transport::UdpTransport;
use crate::transport::{ConnectionEvent, SenderStream};
use aes::Aes128;
use libp2p_identity::PublicKey;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

pub struct UdpConnection {
    transport: Arc<RwLock<UdpTransport>>,
    pub(in crate::transport) raw_packets: (
        mpsc::Sender<(SocketAddr, Vec<u8>)>,
        mpsc::Receiver<(SocketAddr, Vec<u8>)>,
    ),
    outbound_symmetric_key: Option<Aes128>,
    inbound_symmetric_key: Option<Aes128>,
    remote_is_gateway: bool,
}

impl UdpConnection {}

impl UdpConnection {
    fn remote_ip_address(&self) -> IpAddr {
        todo!()
    }

    fn remote_public_key(&self) -> PublicKey {
        todo!()
    }

    fn remote_port(&self) -> u16 {
        todo!()
    }

    fn outbound_symmetric_key(&self) -> Vec<u8> {
        todo!()
    }

    fn inbound_symmetric_key(&self) -> Vec<u8> {
        todo!()
    }

    async fn read_event(&self) -> Result<ConnectionEvent, ConnectionError> {
        todo!()
    }

    async fn send_short_message(&self, message: Vec<u8>) -> Result<(), ConnectionError> {
        todo!()
    }

    async fn send_streamed_message(
        &self,
        message_length: usize,
    ) -> Result<SenderStream, ConnectionError> {
        todo!()
    }
}

enum InternalMessage {}

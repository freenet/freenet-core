use crate::transport::errors::ConnectionError;
use crate::transport::{Connection, ConnectionEvent, SenderStream};
use libp2p_identity::PublicKey;
use std::net::IpAddr;
use tokio::sync::mpsc;

pub(super) struct UdpConnection {
    channel: (
        mpsc::Sender<InternalMessage>,
        mpsc::Receiver<InternalMessage>,
    ),
}

impl Connection for UdpConnection {
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

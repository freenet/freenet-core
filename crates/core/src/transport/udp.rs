use crate::transport::errors::*;
use crate::transport::{BytesPerSecond, Connection, ConnectionEvent, SenderStream, Transport};
use dashmap::DashMap;
use libp2p_identity::ed25519::Keypair;
use libp2p_identity::PublicKey;
use std::net::IpAddr;
use time::Duration;
use tokio::sync::mpsc;

struct UdpTransport {
    connections: DashMap<(IpAddr, u16), UdpConnection>,
}

impl Transport<UdpConnection> for UdpTransport {
    fn new(
        keypair: Keypair,
        listen_port: u16,
        is_gateway: bool,
        max_upstream_rate: BytesPerSecond,
    ) -> Result<Self, TransportError>
    where
        Self: Sized,
    {
        todo!()
    }

    async fn connect(
        &self,
        remote_public_key: PublicKey,
        remote_ip_address: IpAddr,
        remote_port: u16,
        remote_is_gateway: bool,
        timeout: Duration,
    ) -> Result<UdpConnection, TransportError> {
        todo!()
    }

    fn update_max_upstream_rate(&self, max_upstream_rate: BytesPerSecond) {
        todo!()
    }

    async fn listen_for_connection(&self) -> Result<UdpConnection, TransportError> {
        todo!()
    }
}

struct UdpConnection {}

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

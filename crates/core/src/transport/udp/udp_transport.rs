use super::*;
use crate::transport::udp::udp_connection::UdpConnection;

struct UdpTransport {
    connections: DashMap<(IpAddr, u16), UdpConnection>,
    channel: (
        mpsc::Sender<InternalMessage>,
        mpsc::Receiver<InternalMessage>,
    ),
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

enum InternalMessage {}

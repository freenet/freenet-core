use aes_gcm::aes::Aes128;
use std::net::SocketAddr;
use thiserror::Error;

use super::crypto::TransportPublicKey;

pub(super) struct ConnectionInfo {
    pub outbound_symmetric_key: Aes128,
    pub inbound_symmetric_key: Aes128,
    pub remote_public_key: TransportPublicKey,
    pub remote_is_gateway: bool,
    pub remote_addr: SocketAddr,
}

#[derive(Debug, Error)]
pub(crate) enum ConnectionError {}

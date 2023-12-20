use std::net::SocketAddr;

use aes_gcm::Aes128Gcm;
use thiserror::Error;

use super::{crypto::TransportPublicKey, SenderStreamError};

pub(super) struct ConnectionInfo {
    pub outbound_symmetric_key: Aes128Gcm,
    pub inbound_symmetric_key: Aes128Gcm,
    pub remote_public_key: TransportPublicKey,
    pub remote_is_gateway: bool,
    pub remote_addr: SocketAddr,
}

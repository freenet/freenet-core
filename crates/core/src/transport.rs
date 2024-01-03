use std::net::IpAddr;
use std::time::Duration;
use libp2p_identity::{Keypair, PublicKey};

pub(crate) struct Transport {

}

impl <C : Connection> Transport {
    pub fn new(keypair : Keypair, listen_port : u16) -> Result<Self, anyhow::Error> {
        todo!()
    }

    pub async fn connect(&self,
                         remote_public_key : PublicKey,
                         remote_ip_address : IpAddr,
                         remote_port : u16,
                         timeout : Duration
    ) -> Result<C, anyhow::Error> {
        todo!()
    }
}

pub trait Connection {

}
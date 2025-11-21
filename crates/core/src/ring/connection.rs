use super::PeerKeyLocation;

#[derive(Clone, Debug)]
pub struct Connection {
    pub(crate) location: PeerKeyLocation,
}

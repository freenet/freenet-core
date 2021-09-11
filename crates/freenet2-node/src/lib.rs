mod config;
pub mod conn_manager;
mod message;
mod node;
mod probe_proto;
mod ring_proto;

pub use conn_manager::{in_memory::MemoryConnManager, PeerKey};
pub use node::NodeConfig;
pub use ring_proto::Location;

type StdResult<T, E> = std::result::Result<T, E>;

mod config;
mod conn_manager;
mod message;
mod node;
mod probe_proto;
mod ring_proto;

#[cfg(test)]
mod tests;

pub use node::NodeConfig;

type StdResult<T, E> = std::result::Result<T, E>;

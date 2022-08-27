use locutus_node::ClientRequest;

pub mod build_tool;
pub mod config;
pub mod local_node;
pub mod new_pckg;
pub mod util;

type CommandReceiver = tokio::sync::mpsc::Receiver<ClientRequest>;
type CommandSender = tokio::sync::mpsc::Sender<ClientRequest>;
type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;

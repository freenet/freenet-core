use locutus_node::ClientRequest;

mod config;
mod executor;
mod local_node;
mod state;
mod user_events;

pub use config::Config;
pub use executor::wasm_runtime;
pub use local_node::LocalNode;
pub use locutus_runtime::ContractStore;
pub use state::AppState;
pub use user_events::user_fn_handler;

type CommandReceiver = tokio::sync::mpsc::Receiver<ClientRequest>;
type CommandSender = tokio::sync::mpsc::Sender<ClientRequest>;
type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;

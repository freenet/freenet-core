use locutus_node::ClientRequest;

mod config;
mod executor;
mod local_node;
mod state;
mod user_events;
pub(crate) mod util;

pub use config::SubCommand;
pub use config::ContractType;
pub use config::Config;
pub use config::LocalNodeConfig;
pub use config::StateConfig;
pub use executor::wasm_runtime;
pub use local_node::LocalNode;
pub use locutus_runtime::ContractStore;
pub use state::AppState;
pub use user_events::user_fn_handler;
pub use util::set_cleanup_on_exit;

type CommandReceiver = tokio::sync::mpsc::Receiver<ClientRequest>;
type CommandSender = tokio::sync::mpsc::Sender<ClientRequest>;
type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;

use std::borrow::Cow;

use locutus_stdlib::api::ClientRequest;

pub mod build;
pub mod commands;
pub mod config;
pub mod local_node;
pub mod new_pckg;
pub mod util;

type CommandReceiver = tokio::sync::mpsc::Receiver<ClientRequest<'static>>;
type CommandSender = tokio::sync::mpsc::Sender<ClientRequest<'static>>;
type DynError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("Configuration error: {0}")]
    MissConfiguration(Cow<'static, str>),
    #[error("Command failed: {0}")]
    CommandFailed(&'static str),
}

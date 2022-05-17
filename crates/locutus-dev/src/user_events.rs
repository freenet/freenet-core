use std::{fmt::Display, fs::File, future::Future, io::Read, pin::Pin, time::Duration};

use either::Either;
use locutus_node::{
    BoxedClient, ClientError, ClientEventsProxy, ClientId, ClientRequest, ErrorKind, HostResponse,
};
use locutus_runtime::prelude::*;
use serde::{Deserialize, Serialize};

use crate::{config::Config, state::AppState, CommandSender, DynError};

type HostIncomingMsg = Result<(ClientId, ClientRequest), ClientError>;

pub async fn user_fn_handler(
    config: Config,
    command_sender: CommandSender,
    app_state: AppState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let mut input = StdInput::new(config, app_state)?;
    println!("running... send a command or write \"help\" for help");
    loop {
        tokio::select! {
            command = input.recv() => {
                command_sender.send(command?.1).await?;
            }
            interrupt = tokio::signal::ctrl_c() => {
                interrupt?;
                break;
            }
        }
    }
    Ok(())
}

struct StdInput {
    config: Config,
    contract: WrappedContract<'static>,
    input: File,
    buf: Vec<u8>,
    app_state: AppState,
}

impl StdInput {
    fn new(config: Config, app_state: AppState) -> Result<Self, DynError> {
        let contract = WrappedContract::try_from((&*config.contract, vec![].into()))?;
        Ok(StdInput {
            input: File::open(&config.input_file)?,
            config,
            contract,
            buf: vec![],
            app_state,
        })
    }

    fn read_input(&mut self) -> Result<CommandInput, DynError> {
        let mut buf = vec![];
        self.input.read_to_end(&mut buf).unwrap();
        bincode::deserialize(&buf).map_err(Into::into)
    }
}

#[derive(Serialize, Deserialize)]
/// Data to be read from the input file after commands are issued.
pub enum CommandInput {
    Put { state: State<'static> },
    Update { delta: StateDelta<'static> },
}

impl CommandInput {
    fn unwrap_put(self) -> State<'static> {
        match self {
            Self::Put { state } => state,
            _ => panic!("expected put"),
        }
    }

    fn unwrap_delta(self) -> StateDelta<'static> {
        match self {
            Self::Update { delta } => delta,
            _ => panic!("expected put"),
        }
    }
}

impl Display for CommandInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Put { .. } => {
                write!(f, "Put")
            }
            Self::Update { .. } => {
                write!(f, "Update")
            }
        }
    }
}

#[derive(Debug)]
enum Command {
    Put,
    Get,
    Update,
    Help,
}

struct CommandInfo {
    cmd: Command,
    contract: WrappedContract<'static>,
    input: Option<CommandInput>,
}

impl From<CommandInfo> for (ClientId, ClientRequest) {
    fn from(cmd: CommandInfo) -> Self {
        let req = match cmd.cmd {
            Command::Get => ClientRequest::Get {
                key: *cmd.contract.key(),
                contract: false,
            },
            Command::Put => {
                let state = cmd.input.unwrap().unwrap_put();
                ClientRequest::Put {
                    contract: cmd.contract,
                    state: WrappedState::new(state.into_owned()),
                }
            }
            Command::Update => {
                let delta = cmd.input.unwrap().unwrap_delta();
                ClientRequest::Update {
                    key: *cmd.contract.key(),
                    delta,
                }
            }
            _ => unreachable!(),
        };
        (ClientId::new(0), req)
    }
}

const HELP: &str = "Locutus Contract Development Environment

SUBCOMMANDS:
    help        Print this message
    get         Gets the current value of the contract. It will be piped into the set output pipe (file, terminal, etc.)
    update      Attempts to update the contract and prints out the result of the operation";

impl TryFrom<&[u8]> for Command {
    type Error = String;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let cmd = std::str::from_utf8(value).map_err(|e| format!("{e}"))?;
        match cmd {
            "put" => Ok(Command::Put),
            "get" => Ok(Command::Get),
            "update" => Ok(Command::Update),
            "help" => Ok(Command::Help),
            v => Err(format!("command {v} unknown")),
        }
    }
}

#[allow(clippy::needless_lifetimes)]
impl ClientEventsProxy for StdInput {
    fn recv(&mut self) -> Pin<Box<dyn Future<Output = HostIncomingMsg> + Send + Sync + '_>> {
        Box::pin(async {
            loop {
                let stdin = std::io::stdin();
                for b in stdin.bytes() {
                    let b =
                        b.map_err(|_| ClientError::from(ErrorKind::TransportProtocolDisconnect))?;
                    if b == b'\n' {
                        break;
                    }
                    self.buf.push(b);
                }
                // try parse command
                match Command::try_from(&self.buf[..]) {
                    Ok(cmd) if matches!(cmd, Command::Help) => {
                        println!("{HELP}");
                    }
                    Ok(cmd) if matches!(cmd, Command::Put) => match self.read_input() {
                        Ok(CommandInput::Put { state }) => {
                            return Ok(CommandInfo {
                                cmd,
                                contract: self.contract.clone(),
                                input: Some(CommandInput::Put { state }),
                            }
                            .into());
                        }
                        Ok(cmd) => {
                            println!("Unexpected command: {cmd}");
                        }
                        Err(err) => {
                            println!("Initial put error: {err}");
                        }
                    },
                    Ok(cmd) if matches!(cmd, Command::Update) => match self.read_input() {
                        Ok(CommandInput::Update { delta }) => {
                            return Ok(CommandInfo {
                                cmd,
                                contract: self.contract.clone(),
                                input: Some(CommandInput::Update { delta }),
                            }
                            .into());
                        }
                        Ok(cmd) => {
                            println!("Unexpected command: {cmd}");
                        }
                        Err(err) => {
                            println!("Initial put error: {err}");
                        }
                    },
                    Ok(cmd) => {
                        self.buf.clear();
                        return Ok(CommandInfo {
                            cmd,
                            contract: self.contract.clone(),
                            input: None,
                        }
                        .into());
                    }
                    Err(err) => {
                        println!("error: {err}");
                        return Err(ClientError::from(ErrorKind::TransportProtocolDisconnect));
                    }
                }
                self.buf.clear();
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
    }

    fn send<'a>(
        &'a mut self,
        _id: ClientId,
        _response: Result<HostResponse, ClientError>,
    ) -> Pin<Box<dyn Future<Output = Result<(), ClientError>> + Send + Sync + '_>> {
        todo!()
    }

    fn cloned(&self) -> BoxedClient {
        Box::new(self.clone())
    }
}

impl Clone for StdInput {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            contract: self.contract.clone(),
            buf: Vec::new(),
            input: File::open(&self.config.input_file).unwrap(),
            app_state: self.app_state.clone(),
        }
    }
}

use std::{
    fmt::Display,
    fs::File,
    io::{Read, Seek},
    time::Duration,
};

use either::Either;
use futures::future::BoxFuture;
use locutus_core::{
    ClientError, ClientEventsProxy, ClientId, ClientRequest, ContractRequest, ErrorKind,
    HostResponse, OpenRequest,
};
use locutus_runtime::prelude::*;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{util, CommandSender, DynError};

use super::{state::AppState, DeserializationFmt, LocalNodeCliConfig};

const HELP: &str = "Locutus Contract Development Environment

SUBCOMMANDS:
    help        Print this message
    get         Gets the current value of the contract. It will be piped into the set output pipe (file, terminal, etc.)
    update      Attempts to update the contract and prints out the result of the operation
    put         Puts the state for the contract for the first time
    exit        Exit from the TUI";

type HostIncomingMsg = Result<OpenRequest<'static>, ClientError>;

pub(super) async fn user_fn_handler(
    config: LocalNodeCliConfig,
    command_sender: CommandSender,
    app_state: AppState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let mut input = StdInput::new(config, app_state)?;
    tracing::debug!("running... send a command or write \"help\" for help");
    loop {
        let command = input.recv().await;
        let command = command?;
        let dc = command.request.is_disconnect();
        command_sender.send(command.request).await?;
        if dc {
            break;
        }
    }
    Ok(())
}

struct StdInput {
    config: LocalNodeCliConfig,
    contract: ContractContainer,
    input: File,
    buf: Vec<u8>,
    app_state: AppState,
}

impl StdInput {
    fn new(config: LocalNodeCliConfig, app_state: AppState) -> Result<Self, DynError> {
        let params = config
            .params
            .as_ref()
            .map(|p| {
                let mut f = File::open(p)?;
                let mut buf = vec![];
                f.read_to_end(&mut buf)?;
                Ok::<_, DynError>(buf)
            })
            .transpose()?
            .unwrap_or_default();

        let contract = ContractContainer::Wasm(WasmAPIVersion::V0_0_1(WrappedContract::try_from(
            (&*config.contract, params.into()),
        )?));
        Ok(StdInput {
            input: File::open(&config.input_file)?,
            config,
            contract,
            buf: vec![],
            app_state,
        })
    }

    fn read_input<T>(&mut self) -> Result<T, DynError>
    where
        T: DeserializeOwned,
    {
        let mut buf = vec![];
        self.input.read_to_end(&mut buf).unwrap();
        util::deserialize(self.config.ser_format, &buf)
    }

    fn get_command_input<T>(&mut self, cmd: Command) -> Result<T, ClientError>
    where
        T: From<Vec<u8>>,
    {
        self.input.rewind().map_err(|e| ErrorKind::Unhandled {
            cause: format!("{e}"),
        })?;
        match self.config.ser_format {
            Some(DeserializationFmt::Json) => {
                let state: serde_json::Value =
                    self.read_input().map_err(|e| ErrorKind::Unhandled {
                        cause: format!("deserialization error: {e}"),
                    })?;
                let json_str =
                    serde_json::to_string_pretty(&state).map_err(|e| ErrorKind::Unhandled {
                        cause: format!("{e}"),
                    })?;
                tracing::debug!("{cmd:?} value:\n{json_str}");
                Ok(json_str.into_bytes().into())
            }
            #[cfg(feature = "messagepack")]
            Some(DeserializationFmt::MessagePack) => {
                let mut buf = vec![];
                self.input.read_to_end(&mut buf).unwrap();
                let state = rmpv::decode::read_value_ref(&mut buf.as_ref()).map_err(|e| {
                    ErrorKind::Unhandled {
                        cause: format!("deserialization error: {e}"),
                    }
                })?;
                tracing::debug!("{cmd:?} value:\n{state}");
                Ok(buf.into())
            }
            _ => {
                let state: Vec<u8> = self.read_input().map_err(|e| ErrorKind::Unhandled {
                    cause: format!("deserialization error: {e}"),
                })?;
                Ok(state.into())
            }
        }
    }
}

#[derive(Serialize, Deserialize)]
/// Data to be read from the input file after commands are issued.
pub(super) enum CommandInput<'a> {
    Put {
        #[serde(borrow)]
        state: State<'a>,
    },
    Update {
        #[serde(borrow)]
        delta: StateDelta<'a>,
    },
}

impl<'a> CommandInput<'a> {
    fn unwrap_put(self) -> State<'a> {
        match self {
            Self::Put { state } => state,
            _ => panic!("expected put"),
        }
    }

    fn unwrap_delta(self) -> StateDelta<'a> {
        match self {
            Self::Update { delta } => delta,
            _ => panic!("expected put"),
        }
    }
}

impl Display for CommandInput<'_> {
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
    GetParams,
    Update,
    Help,
    Exit,
}

impl TryFrom<&[u8]> for Command {
    type Error = String;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let cmd = std::str::from_utf8(value).map_err(|e| format!("{e}"))?;
        match cmd {
            "put" => Ok(Command::Put),
            "get" => Ok(Command::Get),
            "get params" => Ok(Command::GetParams),
            "update" => Ok(Command::Update),
            "help" => Ok(Command::Help),
            "exit" => Ok(Command::Exit),
            v => Err(format!("unknown command: {v}")),
        }
    }
}

struct CommandInfo {
    cmd: Command,
    contract: ContractContainer,
    input: Option<CommandInput<'static>>,
}

impl From<CommandInfo> for OpenRequest<'static> {
    fn from(cmd: CommandInfo) -> Self {
        let key = cmd.contract.get_key();
        let req = match cmd.cmd {
            Command::Get => ContractRequest::Get {
                key,
                fetch_contract: false,
            }
            .into(),
            Command::Put => {
                let state = cmd.input.unwrap().unwrap_put();
                ContractRequest::Put {
                    contract: cmd.contract,
                    state: WrappedState::new(state.into_bytes()),
                    related_contracts: Default::default(),
                }
                .into()
            }
            Command::Update => {
                let data = cmd.input.unwrap().unwrap_delta().into();
                ContractRequest::Update { key, data }.into()
            }
            Command::Exit => ClientRequest::Disconnect {
                cause: Some("shutdown".to_owned()),
            },
            _ => unreachable!(),
        };
        OpenRequest::new(ClientId::new(0), req)
    }
}

impl ClientEventsProxy for StdInput {
    fn recv(&mut self) -> BoxFuture<'_, HostIncomingMsg> {
        Box::pin(async {
            loop {
                self.buf.clear();
                let f = async {
                    let stdin = std::io::stdin();
                    'stdin: for b in stdin.bytes() {
                        let b = b.map_err(|_| {
                            ClientError::from(ErrorKind::TransportProtocolDisconnect)
                        })?;
                        if b == b'\n' {
                            break 'stdin;
                        }
                        self.buf.push(b);
                    }
                    if String::from_utf8_lossy(&self.buf[..]).trim().is_empty() {
                        return Ok(Either::Right(()));
                    }
                    // try parse command
                    match Command::try_from(&self.buf[..]) {
                        Ok(Command::Help) => {
                            tracing::debug!("{HELP}");
                        }
                        Ok(cmd @ Command::Put) => {
                            let state: State = match self.get_command_input(Command::Put) {
                                Ok(v) => v,
                                Err(e) => {
                                    tracing::debug!("Put event error: {e}");
                                    return Ok(Either::Right(()));
                                }
                            };
                            return Ok(Either::Left(
                                CommandInfo {
                                    cmd,
                                    contract: self.contract.clone(),
                                    input: Some(CommandInput::Put { state }),
                                }
                                .into(),
                            ));
                        }
                        Ok(cmd @ Command::Update) => {
                            let delta: StateDelta = match self.get_command_input(Command::Update) {
                                Ok(v) => v,
                                Err(e) => {
                                    tracing::debug!("Update event error: {e}");
                                    return Ok(Either::Right(()));
                                }
                            };
                            return Ok(Either::Left(
                                CommandInfo {
                                    cmd,
                                    contract: self.contract.clone(),
                                    input: Some(CommandInput::Update { delta }),
                                }
                                .into(),
                            ));
                        }
                        Ok(Command::GetParams) => {
                            // FIXME: related to issue 272
                            let _node = &*self.app_state.local_node.read().await;
                            // let p = node
                            //     .contract_state
                            //     .get_params(self.contract.key())
                            //     .await
                            //     .unwrap();
                            // self.app_state.printout_deser(&p);
                        }
                        Ok(cmd) => {
                            return Ok(Either::Left(
                                CommandInfo {
                                    cmd,
                                    contract: self.contract.clone(),
                                    input: None,
                                }
                                .into(),
                            ));
                        }
                        Err(err) => {
                            tracing::debug!("{err}");
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    Ok(Either::<OpenRequest, _>::Right(()))
                };
                match f.await {
                    Ok(Either::Right(_)) => continue,
                    Ok(Either::Left(r)) => return Ok(r),
                    Err(err) => return Err(err),
                }
            }
        })
    }

    fn send(
        &mut self,
        _id: ClientId,
        _response: Result<HostResponse, ClientError>,
    ) -> BoxFuture<'_, Result<(), ClientError>> {
        unimplemented!()
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

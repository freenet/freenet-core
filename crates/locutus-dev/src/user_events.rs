use std::{future::Future, io::Read, pin::Pin, time::Duration};

use locutus_node::{
    BoxedClient, ClientError, ClientEventsProxy, ClientId, ClientRequest, ErrorKind, HostResponse,
};
use locutus_runtime::prelude::*;

use crate::{Cli, CommandSender};

type HostIncomingMsg = Result<(ClientId, ClientRequest), ClientError>;

pub(crate) async fn user_fn_handler(
    command_sender: CommandSender,
    config: Cli,
) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>> {
    let mut input = StdInput::new(config)?;
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
    config: Cli,
    contract_key: ContractKey,
    buf: Vec<u8>,
}

impl StdInput {
    fn new(config: Cli) -> Result<Self, Box<dyn std::error::Error + Send + Sync + 'static>> {
        let contract = WrappedContract::try_from(config.contract.clone())?;
        let contract_key = contract.key();
        Ok(StdInput {
            config,
            contract_key,
            buf: vec![],
        })
    }
}

#[derive(Debug)]
enum Command {
    Get,
    Update { delta: StateDelta<'static> },
    Help,
}

struct CommandInfo {
    cmd: Command,
    key: ContractKey,
}

impl From<CommandInfo> for (ClientId, ClientRequest) {
    fn from(cmd: CommandInfo) -> Self {
        let req = match cmd.cmd {
            Command::Get => ClientRequest::Get {
                key: cmd.key,
                contract: false,
            },
            _ => todo!(),
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
            "get" => Ok(Command::Get),
            "update" => todo!(),
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
                    Ok(cmd) => {
                        self.buf.clear();
                        return Ok(CommandInfo {
                            cmd,
                            key: self.contract_key,
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
            contract_key: self.contract_key,
            buf: Vec::new(),
        }
    }
}

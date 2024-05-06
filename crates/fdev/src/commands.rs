use std::{
    fs::File,
    io::Read,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
};

use freenet::dev_tool::OperationMode;
use freenet_stdlib::{
    client_api::{ClientRequest, ContractRequest, DelegateRequest, WebApi},
    prelude::*,
};

use crate::config::{BaseConfig, PutConfig, UpdateConfig};

#[derive(Debug, Clone, clap::Subcommand)]
pub(crate) enum PutType {
    /// Puts a new contract
    Contract(PutContract),
    /// Puts a new delegate
    Delegate(PutDelegate),
}

#[derive(clap::Parser, Clone, Debug)]
pub(crate) struct PutContract {
    /// A path to a JSON file listing the related contracts.
    #[arg(long)]
    pub(crate) related_contracts: Option<PathBuf>,
    /// A path to the initial state for the contract being published.
    #[arg(long)]
    pub(crate) state: Option<PathBuf>,
}

#[derive(clap::Parser, Clone, Debug)]
pub(crate) struct PutDelegate {
    /// Base58 encoded nonce. If empty the default value will be used, this is only allowed in local mode.
    #[arg(long, env = "DELEGATE_NONCE", default_value_t = String::new())]
    pub(crate) nonce: String,
    /// Base58 encoded cipher. If empty the default value will be used, this is only allowed in local mode.
    #[arg(long, env = "DELEGATE_CIPHER", default_value_t = String::new())]
    pub(crate) cipher: String,
}

pub async fn put(config: PutConfig, other: BaseConfig) -> Result<(), anyhow::Error> {
    if config.release {
        anyhow::bail!("Cannot publish contracts in the network yet");
    }
    let params = if let Some(params) = &config.parameters {
        let mut buf = vec![];
        File::open(params)?.read_to_end(&mut buf)?;
        Parameters::from(buf)
    } else {
        Parameters::from(&[] as &[u8])
    };
    match &config.package_type {
        PutType::Contract(contract) => put_contract(&config, contract, other, params).await,
        PutType::Delegate(delegate) => put_delegate(&config, delegate, other, params).await,
    }
}

async fn put_contract(
    config: &PutConfig,
    contract_config: &PutContract,
    other: BaseConfig,
    params: Parameters<'static>,
) -> Result<(), anyhow::Error> {
    let contract = ContractContainer::try_from((config.code.as_path(), params))?;
    let state = if let Some(ref state_path) = contract_config.state {
        let mut buf = vec![];
        File::open(state_path)?.read_to_end(&mut buf)?;
        buf.into()
    } else {
        tracing::warn!("no state provided for contract, if your contract cannot handle empty state correctly, this will always cause an error.");
        vec![].into()
    };
    let related_contracts = if let Some(_related) = &contract_config.related_contracts {
        todo!("use `related` contracts")
    } else {
        Default::default()
    };

    println!("Putting contract {}", contract.key());
    let request = ContractRequest::Put {
        contract,
        state,
        related_contracts,
    }
    .into();
    execute_command(request, other, config.address, config.port).await
}

async fn put_delegate(
    config: &PutConfig,
    delegate_config: &PutDelegate,
    other: BaseConfig,
    params: Parameters<'static>,
) -> Result<(), anyhow::Error> {
    let delegate = DelegateContainer::try_from((config.code.as_path(), params))?;

    let (cipher, nonce) = if delegate_config.cipher.is_empty() && delegate_config.nonce.is_empty() {
        println!(
"Using default cipher and nonce. 
For additional hardening is recommended to use a different cipher and nonce to encrypt secrets in storage.");
        (
            ::freenet_stdlib::client_api::DelegateRequest::DEFAULT_CIPHER,
            ::freenet_stdlib::client_api::DelegateRequest::DEFAULT_NONCE,
        )
    } else {
        let mut cipher = [0; 32];
        bs58::decode(delegate_config.cipher.as_bytes())
            .with_alphabet(bs58::Alphabet::BITCOIN)
            .onto(&mut cipher)?;

        let mut nonce = [0; 24];
        bs58::decode(delegate_config.nonce.as_bytes())
            .with_alphabet(bs58::Alphabet::BITCOIN)
            .onto(&mut nonce)?;
        (cipher, nonce)
    };

    println!("Putting delegate {} ", delegate.key().encode());
    let request = DelegateRequest::RegisterDelegate {
        delegate,
        cipher,
        nonce,
    }
    .into();
    execute_command(request, other, config.address, config.port).await
}

pub async fn update(config: UpdateConfig, other: BaseConfig) -> Result<(), anyhow::Error> {
    if config.release {
        anyhow::bail!("Cannot publish contracts in the network yet");
    }
    let key = ContractInstanceId::try_from(config.key)?.into();
    println!("Updating contract {key}");
    let data = {
        let mut buf = vec![];
        File::open(&config.delta)?.read_to_end(&mut buf)?;
        StateDelta::from(buf).into()
    };
    let request = ContractRequest::Update { key, data }.into();
    execute_command(request, other, config.address, config.port).await
}

async fn execute_command(
    request: ClientRequest<'static>,
    other: BaseConfig,
    address: IpAddr,
    port: u16,
) -> Result<(), anyhow::Error> {
    let mode = other.mode;

    let target = match mode {
        OperationMode::Local => {
            if !address.is_loopback() {
                return Err(anyhow::anyhow!(
                    "invalid ip: {address}, expecting a loopback ip address in local mode"
                ));
            }
            SocketAddr::new(address, port)
        }
        OperationMode::Network => SocketAddr::new(address, port),
    };

    let (stream, _) = tokio_tungstenite::connect_async(&format!(
        "ws://{}/contract/command?encodingProtocol=native",
        target
    ))
    .await
    .map_err(|e| {
        tracing::error!(err=%e);
        anyhow::anyhow!(format!("fail to connect to the host({target}): {e}"))
    })?;

    WebApi::start(stream)
        .send(request)
        .await
        .map_err(Into::into)
}

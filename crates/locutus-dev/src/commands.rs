use std::{fs::File, io::Read, path::PathBuf};

use locutus_core::{
    locutus_runtime::StateDelta, ClientId, Config, Executor, OperationMode, Storage,
};
use locutus_runtime::{
    ContractContainer, ContractInstanceId, ContractStore, Delegate, DelegateCode, DelegateStore,
    Parameters, SecretsStore, StateStore,
};
use locutus_stdlib::client_api::{ClientRequest, ContractRequest, DelegateRequest};

use crate::{
    config::{BaseConfig, PutConfig, UpdateConfig},
    DynError,
};

const MAX_MEM_CACHE: u32 = 10_000_000;
const DEFAULT_MAX_CONTRACT_SIZE: i64 = 50 * 1024 * 1024;
const DEFAULT_MAX_DELEGATE_SIZE: i64 = 50 * 1024 * 1024;

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
    pub(crate) state: PathBuf,
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

// #[track_caller]
pub async fn put(config: PutConfig, other: BaseConfig) -> Result<(), DynError> {
    if config.release {
        return Err("Cannot publish contracts in the network yet".into());
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
) -> Result<(), DynError> {
    let contract = ContractContainer::try_from((config.code.as_path(), params))?;
    let state = {
        let mut buf = vec![];
        File::open(&contract_config.state)?.read_to_end(&mut buf)?;
        buf.into()
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
    execute_command(request, other).await
}

async fn put_delegate(
    config: &PutConfig,
    delegate_config: &PutDelegate,
    other: BaseConfig,
    params: Parameters<'static>,
) -> Result<(), DynError> {
    let code = DelegateCode::load(&config.code)?;

    let (cipher, nonce) = if other.mode == OperationMode::Local
        && delegate_config.cipher.is_empty()
        && delegate_config.nonce.is_empty()
    {
        println!("Using default cipher and nonce. This is only allowed in local mode, don't reuse those for network mode.");
        const CIPHER: [u8; 32] = [
            0, 24, 22, 150, 112, 207, 24, 65, 182, 161, 169, 227, 66, 182, 237, 215, 206, 164, 58,
            161, 64, 108, 157, 195, 0, 0, 0, 0, 0, 0, 0, 0,
        ];
        const NONCE: [u8; 24] = [
            57, 18, 79, 116, 63, 134, 93, 39, 208, 161, 156, 229, 222, 247, 111, 79, 210, 126, 127,
            55, 224, 150, 139, 80,
        ];
        (CIPHER, NONCE)
    } else {
        let mut cipher = [0; 32];
        bs58::decode(delegate_config.cipher.as_bytes())
            .with_alphabet(bs58::Alphabet::BITCOIN)
            .into(&mut cipher)?;

        let mut nonce = [0; 24];
        bs58::decode(delegate_config.nonce.as_bytes())
            .with_alphabet(bs58::Alphabet::BITCOIN)
            .into(&mut nonce)?;
        (cipher, nonce)
    };

    let delegate = Delegate::from((&code, &params));
    println!("Putting delegate {} ", delegate.key().encode());

    let request = DelegateRequest::RegisterDelegate {
        delegate,
        cipher,
        nonce,
    }
    .into();
    execute_command(request, other).await
}

pub async fn update(config: UpdateConfig, other: BaseConfig) -> Result<(), DynError> {
    if config.release {
        return Err("Cannot publish contracts in the network yet".into());
    }
    let key = ContractInstanceId::try_from(config.key)?.into();
    println!("Updating contract {key}");
    let data = {
        let mut buf = vec![];
        File::open(&config.delta)?.read_to_end(&mut buf)?;
        StateDelta::from(buf).into()
    };
    let request = ContractRequest::Update { key, data }.into();
    execute_command(request, other).await
}

async fn execute_command(
    request: ClientRequest<'static>,
    other: BaseConfig,
) -> Result<(), DynError> {
    let contracts_data_path = other
        .contract_data_dir
        .unwrap_or_else(|| Config::get_conf().config_paths.local_contracts_dir());
    let delegates_data_path = other
        .delegate_data_dir
        .unwrap_or_else(|| Config::get_conf().config_paths.local_delegates_dir());
    let secrets_data_path = other
        .secret_data_dir
        .unwrap_or_else(|| Config::get_conf().config_paths.local_secrets_dir());

    let contract_store = ContractStore::new(contracts_data_path, DEFAULT_MAX_CONTRACT_SIZE)?;
    let delegate_store = DelegateStore::new(delegates_data_path, DEFAULT_MAX_DELEGATE_SIZE)?;
    let secret_store = SecretsStore::new(secrets_data_path)?;
    let state_store = StateStore::new(Storage::new().await?, MAX_MEM_CACHE).unwrap();
    let mut executor = Executor::new(
        contract_store,
        delegate_store,
        secret_store,
        state_store,
        || {},
        OperationMode::Local,
    )
    .await?;

    executor
        .handle_request(ClientId::new(0), request, None)
        .await
        .map_err(|e| match e {
            either::Either::Right(e) => e,
            either::Either::Left(_) => unreachable!(),
        })?;

    Ok(())
}

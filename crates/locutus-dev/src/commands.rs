use std::{fs::File, io::Read};

use locutus_core::{
    locutus_runtime::StateDelta, ClientId, Config, Executor, OperationMode, Storage,
};
use locutus_runtime::{
    ContractContainer, ContractInstanceId, ContractStore, Parameters, StateStore,
};
use locutus_stdlib::client_api::{ClientRequest, ContractRequest};

use crate::{
    config::{BaseConfig, PutConfig, UpdateConfig},
    DynError,
};

const MAX_MEM_CACHE: u32 = 10_000_000;
const DEFAULT_MAX_CONTRACT_SIZE: i64 = 50 * 1024 * 1024;

// #[track_caller]
pub async fn put(config: PutConfig, other: BaseConfig) -> Result<(), DynError> {
    if config.release {
        return Err("Cannot publish contracts in the network yet".into());
    }
    let params = if let Some(params) = config.parameters {
        let mut buf = vec![];
        File::open(params)?.read_to_end(&mut buf)?;
        Parameters::from(buf)
    } else {
        Parameters::from(&[] as &[u8])
    };
    let contract = ContractContainer::try_from((config.code.as_path(), params))?;
    let state = {
        let mut buf = vec![];
        File::open(&config.state)?.read_to_end(&mut buf)?;
        buf.into()
    };
    let related_contracts = if let Some(_related) = config.related_contracts {
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
    let data_path = other
        .contract_data_dir
        .unwrap_or_else(|| Config::get_conf().config_paths.local_contracts_dir());
    let contract_store = ContractStore::new(data_path, DEFAULT_MAX_CONTRACT_SIZE)?;
    let state_store = StateStore::new(Storage::new().await?, MAX_MEM_CACHE).unwrap();
    let mut executor =
        Executor::new(contract_store, state_store, || {}, OperationMode::Local).await?;

    executor
        .handle_request(ClientId::new(0), request, None)
        .await
        .map_err(|e| match e {
            either::Either::Right(e) => e,
            either::Either::Left(_) => unreachable!(),
        })?;

    Ok(())
}

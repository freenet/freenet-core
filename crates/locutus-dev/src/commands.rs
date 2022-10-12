use std::{fs::File, io::Read, sync::Arc};

use locutus_core::{
    locutus_runtime::StateDelta, ClientId, ClientRequest, ContractExecutor, SqlitePool,
};
use locutus_runtime::{ContractInstanceId, ContractStore, Parameters, StateStore, WrappedContract};

use crate::{
    config::{BaseConfig, PutConfig, UpdateConfig},
    DynError,
};

const MAX_MEM_CACHE: u32 = 10_000_000;
const DEFAULT_MAX_CONTRACT_SIZE: i64 = 50 * 1024 * 1024;

pub async fn put(config: PutConfig, other: BaseConfig) -> Result<(), DynError> {
    if config.release {
        return Err("Cannot publish contracts in the network yet".into());
    }
    let code = {
        let mut buf = vec![];
        File::open(&config.code)?.read_to_end(&mut buf)?;
        buf.into()
    };
    let params = if let Some(params) = config.parameters {
        let mut buf = vec![];
        File::open(&params)?.read_to_end(&mut buf)?;
        Parameters::from(buf)
    } else {
        Parameters::from(&[] as &[u8])
    };
    let state = {
        let mut buf = vec![];
        File::open(&config.state)?.read_to_end(&mut buf)?;
        buf.into()
    };
    let contract = WrappedContract::new(Arc::new(code), params);
    let related_contracts = if let Some(_related) = config.related_contracts {
        todo!("use `related` contracts")
    } else {
        Default::default()
    };

    println!("Putting contract {}", contract.key());
    let request = ClientRequest::Put {
        contract,
        state,
        related_contracts,
    };
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
    let request = ClientRequest::Update { key, data };
    execute_command(request, other).await
}

async fn execute_command(
    request: ClientRequest<'static>,
    other: BaseConfig,
) -> Result<(), DynError> {
    let data_path = other
        .data_dir
        .unwrap_or_else(|| std::env::temp_dir().join("locutus"));
    let contract_store =
        ContractStore::new(data_path.join("contracts"), DEFAULT_MAX_CONTRACT_SIZE)?;
    let state_store = StateStore::new(SqlitePool::new().await?, MAX_MEM_CACHE).unwrap();
    let mut executor = ContractExecutor::new(contract_store, state_store, || {}).await?;

    executor
        .handle_request(ClientId::new(0), request, None)
        .await
        .map_err(|e| match e {
            either::Either::Right(e) => e,
            either::Either::Left(_) => unreachable!(),
        })?;

    Ok(())
}

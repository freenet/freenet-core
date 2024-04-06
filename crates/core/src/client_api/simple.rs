use crate::client_api::commands;

use freenet_stdlib::prelude::ContractInstanceId;
use freenet_stdlib::prelude::StateDelta;
use freenet_stdlib::client_api::ContractRequest;



use crate::dev_tool::{
    ClientId, Config, ContractStore, DelegateStore, Executor, OperationMode, SecretsStore,
    StateStore, Storage,
};


pub struct API {
    port: u32,
}

impl API {
    pub fn new(port: u32) -> Self {
         API {
             port: port
         }
    }
    
    pub async fn update(&mut self, key: String, delta: Vec<u8>) -> Result<(), anyhow::Error> {
        //if config.release {
        //    anyhow::bail!("Cannot publish contracts in the network yet");
        //}
        let key = ContractInstanceId::try_from(key)?.into();
        println!("Updating contract {key}");
        let data = StateDelta::from(delta).into();
        let request = ContractRequest::Update { key, data }.into();
        commands::execute_command(request, Some(Config::conf().contracts_dir()), Some(Config::conf().delegates_dir()), Some(Config::conf().secrets_dir()), Some(Config::conf().db_dir())).await
    }
}

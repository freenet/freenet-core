use crate::client_api::commands;

use freenet_stdlib::prelude::ContractInstanceId;
use freenet_stdlib::prelude::StateDelta;
use freenet_stdlib::client_api::ContractRequest;

use home::home_dir;


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
    
    /// run an UPDATE query to the sit at address `key` with delta `delta`.
    pub async fn update(&mut self, key: String, delta: Vec<u8>) -> Result<(), anyhow::Error> {
        //if config.release {
        //    anyhow::bail!("Cannot publish contracts in the network yet");
        //}
        let key = ContractInstanceId::try_from(key)?.into();
        println!("Updating contract {key:?}");
        let data = StateDelta::from(delta).into();
        println!("Updating contract {data:?}");
        let request = ContractRequest::Update { key, data }.into();
        println!("Updating contract {request:?}");
        
        
        
        
        //todo: instead use [Some(Config::conf().contracts_dir()), Some(Config::conf().delegates_dir()), Some(Config::conf().secrets_dir()), Some(Config::conf().db_dir())]
        
        let mut contract_path = home::home_dir().expect("Error: Wrong path to home.");
        contract_path.push(".local/share/freenet/contracts/local");
        
        let mut delegate_path = home::home_dir().expect("Error: Wrong path to home.");
        delegate_path.push(".local/share/freenet/delegates/local");
        
        let mut secret_path = home::home_dir().expect("Error: Wrong path to home.");
        secret_path.push(".local/share/freenet/freenet/secrets/local");
        
        let mut database_path = home::home_dir().expect("Error: Wrong path to home.");
        database_path.push(".local/share/freenet/db/local");
        
        commands::execute_command(request, Some(contract_path), Some(delegate_path), Some(secret_path), Some(database_path)).await
    }
}

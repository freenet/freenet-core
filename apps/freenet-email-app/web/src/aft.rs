use std::{cell::RefCell, collections::HashMap};

use locutus_aft_interface::{TokenAllocationRecord, TokenParameters};
use locutus_stdlib::prelude::{ContractKey, ContractRequest, State};

use crate::{api::WebApiRequestClient, app::Identity, DynError};

pub(crate) static TOKEN_RECORD_CODE_HASH: &str =
    include_str!("../build/token_allocation_record_code_hash");
pub(crate) static TOKEN_GENERATOR_DELEGATE_CODE_HASH: &str =
    include_str!("../build/token_generator_code_hash");

pub(crate) struct AftRecords {}

thread_local! {
    static RECORDS: RefCell<HashMap<Identity, TokenAllocationRecord>> = RefCell::new(HashMap::new());
}

impl AftRecords {
    pub(crate) async fn load_all(client: &mut WebApiRequestClient, contracts: &[Identity]) {
        todo!()
    }

    async fn load(
        client: &mut WebApiRequestClient,
        id: &Identity,
    ) -> Result<ContractKey, DynError> {
        let params = TokenParameters::new(id.key.to_public_key())
            .try_into()
            .map_err(|e| format!("{e}"))?;
        let contract_key =
            ContractKey::from_params(TOKEN_RECORD_CODE_HASH, params).map_err(|e| format!("{e}"))?;
        Self::get_state(client, contract_key.clone()).await?;
        Self::subscribe(client, contract_key.clone()).await?;
        Ok(contract_key)
    }

    pub fn set(identity: Identity, state: State<'_>) -> Result<(), DynError> {
        let record = TokenAllocationRecord::try_from(state)?;
        RECORDS.with(|recs| {
            let recs = &mut *recs.borrow_mut();
            recs.insert(identity, record);
        });
        Ok(())
    }

    async fn get_state(client: &mut WebApiRequestClient, key: ContractKey) -> Result<(), DynError> {
        let request = ContractRequest::Get {
            key,
            fetch_contract: false,
        };
        client.send(request.into()).await?;
        Ok(())
    }

    async fn subscribe(client: &mut WebApiRequestClient, key: ContractKey) -> Result<(), DynError> {
        let request = ContractRequest::Subscribe { key };
        client.send(request.into()).await?;
        Ok(())
    }
}

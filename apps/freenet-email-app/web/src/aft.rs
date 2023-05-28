use std::{cell::RefCell, collections::HashMap};

use locutus_aft_interface::{TokenAllocationRecord, TokenAssignment, TokenParameters};
use locutus_stdlib::prelude::{ContractKey, ContractRequest, DelegateKey, State};

use crate::{api::WebApiRequestClient, app::Identity, DynError};

pub(crate) static TOKEN_RECORD_CODE_HASH: &str =
    include_str!("../build/token_allocation_record_code_hash");
pub(crate) static TOKEN_GENERATOR_DELEGATE_CODE_HASH: &str =
    include_str!("../build/token_generator_code_hash");

pub(crate) struct AftRecords {}

thread_local! {
    static RECORDS: RefCell<HashMap<Identity, TokenAllocationRecord>> = RefCell::new(HashMap::new());
    static TOKEN: RefCell<HashMap<DelegateKey, Vec<TokenAssignment>>> = RefCell::new(HashMap::new());
}

impl AftRecords {
    pub async fn load_all(client: &mut WebApiRequestClient, contracts: &[Identity]) {
        todo!()
    }

    pub async fn recv_token(id: &DelegateKey) -> Option<TokenAssignment> {
        TOKEN.with(|t| {
            let ids = &mut *t.borrow_mut();
            ids.get_mut(id).and_then(|v| v.pop())
        })
    }

    pub fn allocated_assignment(key: &DelegateKey, assignment: &TokenAssignment) {
        Self::register_allocation(key, assignment.clone());
        TOKEN.with(|t| {
            let tr = &mut *t.borrow_mut();
            // tr.entry(key.encode()).or_default().push(assignment);
            match tr.get_mut(key) {
                Some(tokens) => tokens.push(assignment.clone()),
                None => {
                    tr.insert(key.clone(), vec![assignment.clone()]);
                }
            }
        });
    }

    // todo: should wait for adt record update confirmation before doing anything with the token
    async fn register_allocation(key: &DelegateKey, assignment: TokenAssignment) {
        // TODO: update the token record in the node so it can be verified that the token was allocated
        // to avoid double-spending
        todo!()
    }

    pub fn set(identity: Identity, state: State<'_>) -> Result<(), DynError> {
        let record = TokenAllocationRecord::try_from(state)?;
        RECORDS.with(|recs| {
            let recs = &mut *recs.borrow_mut();
            recs.insert(identity, record);
        });
        Ok(())
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

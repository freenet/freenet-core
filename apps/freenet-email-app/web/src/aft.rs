use std::{cell::RefCell, collections::HashMap};

use chrono::{DateTime, Utc};
use locutus_aft_interface::{
    TokenAllocationRecord, TokenAllocationSummary, TokenAssignment, TokenParameters,
};
use locutus_stdlib::client_api::ContractRequest;
use locutus_stdlib::prelude::{
    ContractInstanceId, ContractKey, DelegateKey, State, StateDelta, UpdateData,
};

use crate::inbox::InboxModel;
use crate::{
    api::WebApiRequestClient,
    app::{error_handling, Identity, TryNodeAction},
    DynError,
};

pub(crate) static TOKEN_RECORD_CODE_HASH: &str =
    include_str!("../build/token_allocation_record_code_hash");
pub(crate) static TOKEN_GENERATOR_DELEGATE_CODE_HASH: &str =
    include_str!("../build/token_generator_code_hash");

pub(crate) struct AftRecords {}

type InboxContract = ContractKey;
type AftRecord = ContractInstanceId;
type AftDelegate = DelegateKey;

thread_local! {
    static RECORDS: RefCell<HashMap<Identity, TokenAllocationRecord>> = RefCell::new(HashMap::new());
    /// Contracts that require a token assingment still for a pending message.
    static PENDING_TOKEN_ASSIGNMENT: RefCell<HashMap<AftDelegate, Vec<InboxContract>>> = RefCell::new(HashMap::new());
    /// Assignments obtained from the delegate that need to be inserted in the record still.
    static PENDING_CONFIRMED_ASSIGNMENTS: RefCell<HashMap<AftRecord, Vec<PendingAssignmentRegister>>> = RefCell::new(HashMap::new());
    /// A token which has been confirmed as valid by a contract update.
    static VALID_TOKEN: RefCell<HashMap<AftDelegate, Vec<TokenAssignment>>> = RefCell::new(HashMap::new());
}

// FIXME: we should check time to time in the API coroutine if any of the pending assignments
// have not been verified; if that's the case we may need to request again and cannot guarantee
// that a message has been delivered
struct PendingAssignmentRegister {
    /// time at which the request started
    start: DateTime<Utc>,
    // time_slot: DateTime<Utc>,
    // tier: locutus_aft_interface::Tier,
    record: TokenAssignment,
    requester: AftDelegate,
}

impl AftRecords {
    pub async fn load_all(
        client: &mut WebApiRequestClient,
        contracts: &[Identity],
        contract_to_id: &mut HashMap<ContractKey, Identity>,
    ) {
        for identity in contracts {
            let r = Self::load_contract(client, identity).await;
            if let Ok(key) = &r {
                contract_to_id.insert(key.clone(), identity.clone());
            }
            error_handling(
                client.clone().into(),
                r.map(|_| ()),
                TryNodeAction::LoadTokenRecord,
            )
            .await;
        }
    }

    async fn load_contract(
        client: &mut WebApiRequestClient,
        identity: &Identity,
    ) -> Result<ContractKey, DynError> {
        let params = TokenParameters::new(identity.key.to_public_key())
            .try_into()
            .map_err(|e| format!("{e}"))?;
        let contract_key =
            ContractKey::from_params(TOKEN_RECORD_CODE_HASH, params).map_err(|e| format!("{e}"))?;
        Self::get_state(client, contract_key.clone()).await?;
        Self::subscribe(client, contract_key.clone()).await?;
        Ok(contract_key)
    }

    pub fn pending_assignment(delegate: DelegateKey, contract: ContractKey) {
        PENDING_TOKEN_ASSIGNMENT.with(|map| {
            let map = &mut *map.borrow_mut();
            map.entry(delegate).or_default().push(contract);
        })
    }

    pub async fn confirm_allocation(
        client: &mut WebApiRequestClient,
        aft_record: AftRecord,
        summary: TokenAllocationSummary,
    ) -> Result<(), DynError> {
        let Some(confirmed) = PENDING_CONFIRMED_ASSIGNMENTS.with(|pending| {
            let pending = &mut *pending.borrow_mut();
            pending.get_mut(&aft_record).and_then(|registers| {
                registers
                    .iter()
                    .position(|r| {
                        // fixme: the summary should also have the assignment hash
                        if summary.contains_alloc(r.record.tier, r.record.time_slot) {
                            return true;
                        }
                        false
                    })
                    .map(|idx| registers.remove(idx))
            })
        }) else { return Ok(()) };
        // we have a valid token now, so we can update the inbox contract
        InboxModel::finish_sending(client, confirmed.record).await;
        Ok(())
    }

    pub async fn allocated_assignment(
        client: &mut WebApiRequestClient,
        delegate_key: DelegateKey,
        record: TokenAssignment,
    ) -> Result<(), DynError> {
        // update the token record contract for this delegate.
        let key = ContractKey::from(record.token_record);
        let request = ContractRequest::Update {
            key,
            data: UpdateData::Delta(serde_json::to_vec(&record)?.into()),
        };
        client.send(request.into()).await?;
        let token_record = record.token_record;
        let pending_register = PendingAssignmentRegister {
            start: Utc::now(),
            record,
            requester: delegate_key,
        };
        PENDING_CONFIRMED_ASSIGNMENTS.with(|pending| {
            let pending = &mut *pending.borrow_mut();
            pending
                .entry(token_record)
                .or_default()
                .push(pending_register);
        });
        Ok(())
    }

    pub fn set(identity: Identity, state: State<'_>) -> Result<(), DynError> {
        let record = TokenAllocationRecord::try_from(state)?;
        RECORDS.with(|recs| {
            let recs = &mut *recs.borrow_mut();
            recs.insert(identity, record);
        });
        Ok(())
    }

    pub fn update_record(identity: Identity, delta: StateDelta<'_>) -> Result<(), DynError> {
        let record = TokenAllocationRecord::try_from(delta)?;
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
        // todo: send the proper summary from the current state
        let request = ContractRequest::Subscribe { key, summary: None };
        client.send(request.into()).await?;
        Ok(())
    }
}

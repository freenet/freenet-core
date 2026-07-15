use chacha20poly1305::{
    XChaCha20Poly1305,
    aead::{AeadCore, KeyInit, OsRng},
};
use freenet_stdlib::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use std::os::unix::fs::PermissionsExt;

use crate::util::tests::get_temp_dir;
use crate::wasm_runtime::delegate_api::DelegateApiVersion;

use super::super::{
    ContractStore, Runtime, RuntimeResult, SecretsStore, delegate_store::DelegateStore,
    engine::InstanceHandle,
};
use super::*;

const TEST_DELEGATE_2: &str = "test_delegate_2";

/// Message types for test-delegate-2 (host function API)
mod delegate2_messages {
    use super::*;

    #[derive(Debug, Serialize, Deserialize)]
    pub enum InboundAppMessage {
        CreateInboxRequest,
        PleaseSignMessage(Vec<u8>),
        WriteContext(Vec<u8>),
        ReadContext,
        ClearContext,
        IncrementCounter,
        HasSecret(Vec<u8>),
        GetNonExistentSecret(Vec<u8>),
        StoreSecret { key: Vec<u8>, value: Vec<u8> },
        RemoveSecret(Vec<u8>),
        WriteLargeContext(usize),
        StoreLargeSecret { key: Vec<u8>, size: usize },
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub enum OutboundAppMessage {
        CreateInboxResponse(Vec<u8>),
        MessageSigned(Vec<u8>),
        ContextData(Vec<u8>),
        CounterValue(u32),
        SecretExists(bool),
        SecretResult(Option<Vec<u8>>),
        ContextWritten,
        ContextCleared,
        SecretStored,
        SecretRemoved,
        LargeContextWritten(usize),
        LargeSecretStored(usize),
        SecretStoreFailed,
    }
}

async fn setup_runtime(
    name: &str,
) -> Result<(DelegateContainer, Runtime, tempfile::TempDir), Box<dyn std::error::Error>> {
    use crate::contract::storages::Storage;
    let temp_dir = get_temp_dir();
    let contracts_dir = temp_dir.path().join("contracts");
    let delegates_dir = temp_dir.path().join("delegates");
    let secrets_dir = temp_dir.path().join("secrets");

    let db = Storage::new(temp_dir.path()).await?;
    let contract_store = ContractStore::new(contracts_dir, 10_000, db.clone())?;
    let delegate_store = DelegateStore::new(delegates_dir, 10_000, db.clone())?;
    let secret_store = SecretsStore::new(secrets_dir, Default::default(), db)?;

    let mut runtime = Runtime::build(contract_store, delegate_store, secret_store, false).unwrap();

    let delegate = {
        let bytes = super::super::tests::get_test_module(name)?;
        DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(Delegate::from((
            &bytes.into(),
            &vec![].into(),
        ))))
    };
    let _stored = runtime.delegate_store.store_delegate(delegate.clone());

    let key = XChaCha20Poly1305::generate_key(&mut OsRng);
    let cipher = XChaCha20Poly1305::new(&key);
    let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
    let _registered = runtime
        .secret_store
        .register_delegate(delegate.key().clone(), cipher, nonce);

    Ok((delegate, runtime, temp_dir))
}

const TEST_DELEGATE_CAPABILITIES: &str = "test_delegate_capabilities";

/// Message types for test-delegate-capabilities (must match the delegate's types)
mod capabilities_messages {
    use super::*;

    #[derive(Debug, Serialize, Deserialize)]
    #[allow(clippy::enum_variant_names)]
    pub enum DelegateCommand {
        GetContractState {
            contract_id: ContractInstanceId,
        },
        GetMultipleContractStates {
            contract_ids: Vec<ContractInstanceId>,
        },
        GetContractWithEcho {
            contract_id: ContractInstanceId,
            echo_message: String,
        },
        PutContractState {
            contract: ContractContainer,
            state: Vec<u8>,
        },
        UpdateContractState {
            contract_id: ContractInstanceId,
            state: Vec<u8>,
        },
        SubscribeContract {
            contract_id: ContractInstanceId,
        },
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub enum DelegateResponse {
        ContractState {
            contract_id: ContractInstanceId,
            state: Option<Vec<u8>>,
        },
        MultipleContractStates {
            results: Vec<(ContractInstanceId, Option<Vec<u8>>)>,
        },
        Echo {
            message: String,
        },
        ContractPutResult {
            contract_id: ContractInstanceId,
            success: bool,
            error: Option<String>,
        },
        ContractUpdateResult {
            contract_id: ContractInstanceId,
            success: bool,
            error: Option<String>,
        },
        ContractSubscribeResult {
            contract_id: ContractInstanceId,
            success: bool,
            error: Option<String>,
        },
        ContractNotificationReceived {
            contract_id: ContractInstanceId,
            new_state: Vec<u8>,
        },
        Error {
            message: String,
        },
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_contract_request_response() -> Result<(), Box<dyn std::error::Error>> {
    use capabilities_messages::*;

    let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_CAPABILITIES).await?;
    let target_contract_id = ContractInstanceId::new([42u8; 32]);
    let _app_id = ContractInstanceId::new([1u8; 32]);

    let command = DelegateCommand::GetContractState {
        contract_id: target_contract_id,
    };
    let payload = bincode::serialize(&command)?;
    let app_msg = ApplicationMessage::new(payload);

    let outbound = runtime.inbound_app_message(
        delegate.key(),
        &vec![].into(),
        None,
        None,
        vec![InboundDelegateMsg::ApplicationMessage(app_msg)],
    )?;

    assert_eq!(outbound.len(), 1, "Expected exactly one outbound message");
    let contract_request = match &outbound[0] {
        OutboundDelegateMsg::GetContractRequest(req) => req.clone(),
        other @ OutboundDelegateMsg::ApplicationMessage(_)
        | other @ OutboundDelegateMsg::RequestUserInput(_)
        | other @ OutboundDelegateMsg::ContextUpdated(_)
        | other @ OutboundDelegateMsg::PutContractRequest(_)
        | other @ OutboundDelegateMsg::UpdateContractRequest(_)
        | other @ OutboundDelegateMsg::SubscribeContractRequest(_)
        | other @ OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected GetContractRequest, got {:?}", other)
        }
    };
    assert_eq!(contract_request.contract_id, target_contract_id);
    assert!(!contract_request.processed);

    let contract_state = vec![1, 2, 3, 4, 5];
    let response = InboundDelegateMsg::GetContractResponse(GetContractResponse {
        contract_id: target_contract_id,
        state: Some(WrappedState::new(contract_state.clone())),
        context: contract_request.context.clone(),
    });

    let final_outbound =
        runtime.inbound_app_message(delegate.key(), &vec![].into(), None, None, vec![response])?;

    assert_eq!(final_outbound.len(), 1);
    let final_msg = match &final_outbound[0] {
        OutboundDelegateMsg::ApplicationMessage(msg) => msg,
        other @ OutboundDelegateMsg::RequestUserInput(_)
        | other @ OutboundDelegateMsg::ContextUpdated(_)
        | other @ OutboundDelegateMsg::GetContractRequest(_)
        | other @ OutboundDelegateMsg::PutContractRequest(_)
        | other @ OutboundDelegateMsg::UpdateContractRequest(_)
        | other @ OutboundDelegateMsg::SubscribeContractRequest(_)
        | other @ OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected ApplicationMessage, got {:?}", other)
        }
    };
    assert!(final_msg.processed);

    let response: DelegateResponse = bincode::deserialize(&final_msg.payload)?;
    match response {
        DelegateResponse::ContractState { contract_id, state } => {
            assert_eq!(contract_id, target_contract_id);
            assert_eq!(state, Some(contract_state));
        }
        other @ DelegateResponse::MultipleContractStates { .. }
        | other @ DelegateResponse::Echo { .. }
        | other @ DelegateResponse::ContractPutResult { .. }
        | other @ DelegateResponse::ContractUpdateResult { .. }
        | other @ DelegateResponse::ContractSubscribeResult { .. }
        | other @ DelegateResponse::ContractNotificationReceived { .. }
        | other @ DelegateResponse::Error { .. } => {
            panic!("Expected ContractState response, got {:?}", other)
        }
    }

    std::mem::drop(temp_dir);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_contract_not_found() -> Result<(), Box<dyn std::error::Error>> {
    use capabilities_messages::*;

    let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_CAPABILITIES).await?;
    let target_contract_id = ContractInstanceId::new([99u8; 32]);
    let _app_id = ContractInstanceId::new([1u8; 32]);

    let command = DelegateCommand::GetContractState {
        contract_id: target_contract_id,
    };
    let payload = bincode::serialize(&command)?;
    let app_msg = ApplicationMessage::new(payload);

    let outbound = runtime.inbound_app_message(
        delegate.key(),
        &vec![].into(),
        None,
        None,
        vec![InboundDelegateMsg::ApplicationMessage(app_msg)],
    )?;

    let contract_request = match &outbound[0] {
        OutboundDelegateMsg::GetContractRequest(req) => req.clone(),
        other @ OutboundDelegateMsg::ApplicationMessage(_)
        | other @ OutboundDelegateMsg::RequestUserInput(_)
        | other @ OutboundDelegateMsg::ContextUpdated(_)
        | other @ OutboundDelegateMsg::PutContractRequest(_)
        | other @ OutboundDelegateMsg::UpdateContractRequest(_)
        | other @ OutboundDelegateMsg::SubscribeContractRequest(_)
        | other @ OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected GetContractRequest, got {:?}", other)
        }
    };

    let response = InboundDelegateMsg::GetContractResponse(GetContractResponse {
        contract_id: target_contract_id,
        state: None,
        context: contract_request.context.clone(),
    });

    let final_outbound =
        runtime.inbound_app_message(delegate.key(), &vec![].into(), None, None, vec![response])?;

    let final_msg = match &final_outbound[0] {
        OutboundDelegateMsg::ApplicationMessage(msg) => msg,
        other @ OutboundDelegateMsg::RequestUserInput(_)
        | other @ OutboundDelegateMsg::ContextUpdated(_)
        | other @ OutboundDelegateMsg::GetContractRequest(_)
        | other @ OutboundDelegateMsg::PutContractRequest(_)
        | other @ OutboundDelegateMsg::UpdateContractRequest(_)
        | other @ OutboundDelegateMsg::SubscribeContractRequest(_)
        | other @ OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected ApplicationMessage, got {:?}", other)
        }
    };

    let response: DelegateResponse = bincode::deserialize(&final_msg.payload)?;
    match response {
        DelegateResponse::ContractState { state, .. } => {
            assert!(state.is_none());
        }
        other @ DelegateResponse::MultipleContractStates { .. }
        | other @ DelegateResponse::Echo { .. }
        | other @ DelegateResponse::ContractPutResult { .. }
        | other @ DelegateResponse::ContractUpdateResult { .. }
        | other @ DelegateResponse::ContractSubscribeResult { .. }
        | other @ DelegateResponse::ContractNotificationReceived { .. }
        | other @ DelegateResponse::Error { .. } => {
            panic!("Expected ContractState response, got {:?}", other)
        }
    }

    std::mem::drop(temp_dir);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_contract_requests() -> Result<(), Box<dyn std::error::Error>> {
    use capabilities_messages::*;

    let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_CAPABILITIES).await?;

    let contract1 = ContractInstanceId::new([1u8; 32]);
    let contract2 = ContractInstanceId::new([2u8; 32]);
    let contract3 = ContractInstanceId::new([3u8; 32]);
    let _app_id = ContractInstanceId::new([10u8; 32]);

    let command = DelegateCommand::GetMultipleContractStates {
        contract_ids: vec![contract1, contract2, contract3],
    };
    let payload = bincode::serialize(&command)?;
    let app_msg = ApplicationMessage::new(payload);

    let outbound = runtime.inbound_app_message(
        delegate.key(),
        &vec![].into(),
        None,
        None,
        vec![InboundDelegateMsg::ApplicationMessage(app_msg)],
    )?;

    assert_eq!(outbound.len(), 1);
    let req1 = match &outbound[0] {
        OutboundDelegateMsg::GetContractRequest(req) => req.clone(),
        other @ OutboundDelegateMsg::ApplicationMessage(_)
        | other @ OutboundDelegateMsg::RequestUserInput(_)
        | other @ OutboundDelegateMsg::ContextUpdated(_)
        | other @ OutboundDelegateMsg::PutContractRequest(_)
        | other @ OutboundDelegateMsg::UpdateContractRequest(_)
        | other @ OutboundDelegateMsg::SubscribeContractRequest(_)
        | other @ OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected GetContractRequest, got {:?}", other)
        }
    };
    assert_eq!(req1.contract_id, contract1);

    let response1 = InboundDelegateMsg::GetContractResponse(GetContractResponse {
        contract_id: contract1,
        state: Some(WrappedState::new(vec![1, 1, 1])),
        context: req1.context,
    });

    let outbound2 =
        runtime.inbound_app_message(delegate.key(), &vec![].into(), None, None, vec![response1])?;

    assert_eq!(outbound2.len(), 1);
    let req2 = match &outbound2[0] {
        OutboundDelegateMsg::GetContractRequest(req) => req.clone(),
        other @ OutboundDelegateMsg::ApplicationMessage(_)
        | other @ OutboundDelegateMsg::RequestUserInput(_)
        | other @ OutboundDelegateMsg::ContextUpdated(_)
        | other @ OutboundDelegateMsg::PutContractRequest(_)
        | other @ OutboundDelegateMsg::UpdateContractRequest(_)
        | other @ OutboundDelegateMsg::SubscribeContractRequest(_)
        | other @ OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected GetContractRequest for contract2, got {:?}", other)
        }
    };
    assert_eq!(req2.contract_id, contract2);

    let response2 = InboundDelegateMsg::GetContractResponse(GetContractResponse {
        contract_id: contract2,
        state: Some(WrappedState::new(vec![2, 2, 2])),
        context: req2.context,
    });

    let outbound3 =
        runtime.inbound_app_message(delegate.key(), &vec![].into(), None, None, vec![response2])?;

    assert_eq!(outbound3.len(), 1);
    let req3 = match &outbound3[0] {
        OutboundDelegateMsg::GetContractRequest(req) => req.clone(),
        other @ OutboundDelegateMsg::ApplicationMessage(_)
        | other @ OutboundDelegateMsg::RequestUserInput(_)
        | other @ OutboundDelegateMsg::ContextUpdated(_)
        | other @ OutboundDelegateMsg::PutContractRequest(_)
        | other @ OutboundDelegateMsg::UpdateContractRequest(_)
        | other @ OutboundDelegateMsg::SubscribeContractRequest(_)
        | other @ OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected GetContractRequest for contract3, got {:?}", other)
        }
    };
    assert_eq!(req3.contract_id, contract3);

    let response3 = InboundDelegateMsg::GetContractResponse(GetContractResponse {
        contract_id: contract3,
        state: Some(WrappedState::new(vec![3, 3, 3])),
        context: req3.context,
    });

    let final_outbound =
        runtime.inbound_app_message(delegate.key(), &vec![].into(), None, None, vec![response3])?;

    assert_eq!(final_outbound.len(), 1);
    let final_msg = match &final_outbound[0] {
        OutboundDelegateMsg::ApplicationMessage(msg) => msg,
        other @ OutboundDelegateMsg::RequestUserInput(_)
        | other @ OutboundDelegateMsg::ContextUpdated(_)
        | other @ OutboundDelegateMsg::GetContractRequest(_)
        | other @ OutboundDelegateMsg::PutContractRequest(_)
        | other @ OutboundDelegateMsg::UpdateContractRequest(_)
        | other @ OutboundDelegateMsg::SubscribeContractRequest(_)
        | other @ OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected ApplicationMessage, got {:?}", other)
        }
    };
    assert!(final_msg.processed);

    let response: DelegateResponse = bincode::deserialize(&final_msg.payload)?;
    match response {
        DelegateResponse::MultipleContractStates { results } => {
            assert_eq!(results.len(), 3);
            assert_eq!(results[0].0, contract1);
            assert_eq!(results[0].1, Some(vec![1, 1, 1]));
            assert_eq!(results[1].0, contract2);
            assert_eq!(results[1].1, Some(vec![2, 2, 2]));
            assert_eq!(results[2].0, contract3);
            assert_eq!(results[2].1, Some(vec![3, 3, 3]));
        }
        other @ DelegateResponse::ContractState { .. }
        | other @ DelegateResponse::Echo { .. }
        | other @ DelegateResponse::ContractPutResult { .. }
        | other @ DelegateResponse::ContractUpdateResult { .. }
        | other @ DelegateResponse::ContractSubscribeResult { .. }
        | other @ DelegateResponse::ContractNotificationReceived { .. }
        | other @ DelegateResponse::Error { .. } => {
            panic!("Expected MultipleContractStates, got {:?}", other)
        }
    }

    std::mem::drop(temp_dir);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_message_accumulation() -> Result<(), Box<dyn std::error::Error>> {
    use capabilities_messages::*;

    let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_CAPABILITIES).await?;

    let contract_id = ContractInstanceId::new([42u8; 32]);
    let _app_id = ContractInstanceId::new([1u8; 32]);
    let echo_message = "Hello from test!".to_string();

    let command = DelegateCommand::GetContractWithEcho {
        contract_id,
        echo_message: echo_message.clone(),
    };
    let payload = bincode::serialize(&command)?;
    let app_msg = ApplicationMessage::new(payload);

    let outbound = runtime.inbound_app_message(
        delegate.key(),
        &vec![].into(),
        None,
        None,
        vec![InboundDelegateMsg::ApplicationMessage(app_msg)],
    )?;

    assert_eq!(outbound.len(), 2);

    let contract_request = outbound
        .iter()
        .find_map(|msg| match msg {
            OutboundDelegateMsg::GetContractRequest(req) => Some(req.clone()),
            OutboundDelegateMsg::ApplicationMessage(_)
            | OutboundDelegateMsg::RequestUserInput(_)
            | OutboundDelegateMsg::ContextUpdated(_)
            | OutboundDelegateMsg::PutContractRequest(_)
            | OutboundDelegateMsg::UpdateContractRequest(_)
            | OutboundDelegateMsg::SubscribeContractRequest(_)
            | OutboundDelegateMsg::SendDelegateMessage(_) => None,
        })
        .expect("Expected a GetContractRequest");
    assert_eq!(contract_request.contract_id, contract_id);

    let echo_msg = outbound
        .iter()
        .find_map(|msg| match msg {
            OutboundDelegateMsg::ApplicationMessage(m) => Some(m.clone()),
            OutboundDelegateMsg::RequestUserInput(_)
            | OutboundDelegateMsg::ContextUpdated(_)
            | OutboundDelegateMsg::GetContractRequest(_)
            | OutboundDelegateMsg::PutContractRequest(_)
            | OutboundDelegateMsg::UpdateContractRequest(_)
            | OutboundDelegateMsg::SubscribeContractRequest(_)
            | OutboundDelegateMsg::SendDelegateMessage(_) => None,
        })
        .expect("Expected an ApplicationMessage (Echo)");
    assert!(echo_msg.processed);

    let echo_response: DelegateResponse = bincode::deserialize(&echo_msg.payload)?;
    match echo_response {
        DelegateResponse::Echo { message } => {
            assert_eq!(message, echo_message);
        }
        other @ DelegateResponse::ContractState { .. }
        | other @ DelegateResponse::MultipleContractStates { .. }
        | other @ DelegateResponse::ContractPutResult { .. }
        | other @ DelegateResponse::ContractUpdateResult { .. }
        | other @ DelegateResponse::ContractSubscribeResult { .. }
        | other @ DelegateResponse::ContractNotificationReceived { .. }
        | other @ DelegateResponse::Error { .. } => {
            panic!("Expected Echo response, got {:?}", other)
        }
    }

    let contract_response = InboundDelegateMsg::GetContractResponse(GetContractResponse {
        contract_id,
        state: Some(WrappedState::new(vec![1, 2, 3, 4])),
        context: contract_request.context,
    });

    let final_outbound = runtime.inbound_app_message(
        delegate.key(),
        &vec![].into(),
        None,
        None,
        vec![contract_response],
    )?;

    assert_eq!(final_outbound.len(), 1);
    let final_msg = match &final_outbound[0] {
        OutboundDelegateMsg::ApplicationMessage(msg) => msg,
        other @ OutboundDelegateMsg::RequestUserInput(_)
        | other @ OutboundDelegateMsg::ContextUpdated(_)
        | other @ OutboundDelegateMsg::GetContractRequest(_)
        | other @ OutboundDelegateMsg::PutContractRequest(_)
        | other @ OutboundDelegateMsg::UpdateContractRequest(_)
        | other @ OutboundDelegateMsg::SubscribeContractRequest(_)
        | other @ OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected ApplicationMessage, got {:?}", other)
        }
    };

    let response: DelegateResponse = bincode::deserialize(&final_msg.payload)?;
    match response {
        DelegateResponse::ContractState {
            contract_id: id,
            state,
        } => {
            assert_eq!(id, contract_id);
            assert_eq!(state, Some(vec![1, 2, 3, 4]));
        }
        other @ DelegateResponse::MultipleContractStates { .. }
        | other @ DelegateResponse::Echo { .. }
        | other @ DelegateResponse::ContractPutResult { .. }
        | other @ DelegateResponse::ContractUpdateResult { .. }
        | other @ DelegateResponse::ContractSubscribeResult { .. }
        | other @ DelegateResponse::ContractNotificationReceived { .. }
        | other @ DelegateResponse::Error { .. } => {
            panic!("Expected ContractState response, got {:?}", other)
        }
    }

    std::mem::drop(temp_dir);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn validate_host_function_delegate() -> Result<(), Box<dyn std::error::Error>> {
    use delegate2_messages::{InboundAppMessage, OutboundAppMessage};

    let contract = WrappedContract::new(
        Arc::new(ContractCode::from(vec![1])),
        Parameters::from(vec![]),
    );
    let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_2).await?;
    let _app = ContractInstanceId::try_from(contract.key.to_string()).unwrap();

    let payload: Vec<u8> = bincode::serialize(&InboundAppMessage::CreateInboxRequest).unwrap();
    let create_msg = ApplicationMessage::new(payload);
    let inbound = InboundDelegateMsg::ApplicationMessage(create_msg);
    let outbound =
        runtime.inbound_app_message(delegate.key(), &vec![].into(), None, None, vec![inbound])?;

    let expected_payload =
        bincode::serialize(&OutboundAppMessage::CreateInboxResponse(vec![1])).unwrap();
    assert_eq!(outbound.len(), 1);
    assert!(matches!(
        outbound.first(),
        Some(OutboundDelegateMsg::ApplicationMessage(msg)) if *msg.payload == expected_payload
    ));

    let payload: Vec<u8> =
        bincode::serialize(&InboundAppMessage::PleaseSignMessage(vec![1, 2, 3])).unwrap();
    let sign_msg = ApplicationMessage::new(payload);
    let inbound = InboundDelegateMsg::ApplicationMessage(sign_msg);
    let outbound =
        runtime.inbound_app_message(delegate.key(), &vec![].into(), None, None, vec![inbound])?;

    let expected_payload =
        bincode::serialize(&OutboundAppMessage::MessageSigned(vec![4, 5, 2])).unwrap();
    assert_eq!(outbound.len(), 1);
    assert!(matches!(
        outbound.first(),
        Some(OutboundDelegateMsg::ApplicationMessage(msg)) if *msg.payload == expected_payload
    ));

    std::mem::drop(temp_dir);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_context_persistence_within_call() -> Result<(), Box<dyn std::error::Error>> {
    use delegate2_messages::{InboundAppMessage, OutboundAppMessage};

    let contract = WrappedContract::new(
        Arc::new(ContractCode::from(vec![1])),
        Parameters::from(vec![]),
    );
    let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_2).await?;
    let _app = ContractInstanceId::try_from(contract.key.to_string()).unwrap();

    let test_data = vec![1, 2, 3, 4, 5];

    let write_payload = bincode::serialize(&InboundAppMessage::WriteContext(test_data.clone()))?;
    let read_payload = bincode::serialize(&InboundAppMessage::ReadContext)?;

    let messages = vec![
        InboundDelegateMsg::ApplicationMessage(ApplicationMessage::new(write_payload)),
        InboundDelegateMsg::ApplicationMessage(ApplicationMessage::new(read_payload)),
    ];

    let outbound =
        runtime.inbound_app_message(delegate.key(), &vec![].into(), None, None, messages)?;

    assert_eq!(outbound.len(), 2);

    let response1: OutboundAppMessage = match &outbound[0] {
        OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
        OutboundDelegateMsg::RequestUserInput(_)
        | OutboundDelegateMsg::ContextUpdated(_)
        | OutboundDelegateMsg::GetContractRequest(_)
        | OutboundDelegateMsg::PutContractRequest(_)
        | OutboundDelegateMsg::UpdateContractRequest(_)
        | OutboundDelegateMsg::SubscribeContractRequest(_)
        | OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected ApplicationMessage")
        }
    };
    assert!(matches!(response1, OutboundAppMessage::ContextWritten));

    let response2: OutboundAppMessage = match &outbound[1] {
        OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
        OutboundDelegateMsg::RequestUserInput(_)
        | OutboundDelegateMsg::ContextUpdated(_)
        | OutboundDelegateMsg::GetContractRequest(_)
        | OutboundDelegateMsg::PutContractRequest(_)
        | OutboundDelegateMsg::UpdateContractRequest(_)
        | OutboundDelegateMsg::SubscribeContractRequest(_)
        | OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected ApplicationMessage")
        }
    };
    match response2 {
        OutboundAppMessage::ContextData(data) => {
            assert_eq!(data, test_data);
        }
        other @ OutboundAppMessage::CreateInboxResponse(_)
        | other @ OutboundAppMessage::MessageSigned(_)
        | other @ OutboundAppMessage::CounterValue(_)
        | other @ OutboundAppMessage::SecretExists(_)
        | other @ OutboundAppMessage::SecretResult(_)
        | other @ OutboundAppMessage::ContextWritten
        | other @ OutboundAppMessage::ContextCleared
        | other @ OutboundAppMessage::SecretStored
        | other @ OutboundAppMessage::SecretRemoved
        | other @ OutboundAppMessage::LargeContextWritten(_)
        | other @ OutboundAppMessage::LargeSecretStored(_)
        | other @ OutboundAppMessage::SecretStoreFailed => {
            panic!("Expected ContextData, got {:?}", other)
        }
    }

    std::mem::drop(temp_dir);
    Ok(())
}

/// Regression test for harvest cross-app share / ghostkey RequestAnyAccess
/// flow: a delegate's `ctx.write()` MUST survive across separate
/// `inbound_app_message` calls. The canonical use is a permission prompt
/// where call #1 emits `RequestUserInput` (and stores `PendingPrompt` via
/// `ctx.write`), and call #2 receives the user's `UserResponse` and
/// expects `ctx.read()` to return that pending blob.
///
/// Before the fix in this PR, every `inbound_app_message` started with a
/// fresh empty context Vec, so the second call saw nothing and the
/// ghostkey delegate hit "received UserResponse with no pending context".
/// This test exercises the same failure shape via `IncrementCounter`,
/// which reads the counter from context, increments, and writes back: if
/// context is reset between calls, the counter stays stuck at 1.
#[tokio::test(flavor = "multi_thread")]
async fn test_context_persists_between_calls() -> Result<(), Box<dyn std::error::Error>> {
    use delegate2_messages::{InboundAppMessage, OutboundAppMessage};

    let contract = WrappedContract::new(
        Arc::new(ContractCode::from(vec![1])),
        Parameters::from(vec![]),
    );
    let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_2).await?;
    let _app = ContractInstanceId::try_from(contract.key.to_string()).unwrap();

    let payload = bincode::serialize(&InboundAppMessage::IncrementCounter)?;
    let msg = ApplicationMessage::new(payload);
    let outbound = runtime.inbound_app_message(
        delegate.key(),
        &vec![].into(),
        None,
        None,
        vec![InboundDelegateMsg::ApplicationMessage(msg)],
    )?;

    let response: OutboundAppMessage = match &outbound[0] {
        OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
        OutboundDelegateMsg::RequestUserInput(_)
        | OutboundDelegateMsg::ContextUpdated(_)
        | OutboundDelegateMsg::GetContractRequest(_)
        | OutboundDelegateMsg::PutContractRequest(_)
        | OutboundDelegateMsg::UpdateContractRequest(_)
        | OutboundDelegateMsg::SubscribeContractRequest(_)
        | OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected ApplicationMessage")
        }
    };
    assert!(
        matches!(response, OutboundAppMessage::CounterValue(1)),
        "first call: expected CounterValue(1), got {response:?}"
    );

    let payload = bincode::serialize(&InboundAppMessage::IncrementCounter)?;
    let msg = ApplicationMessage::new(payload);
    let outbound = runtime.inbound_app_message(
        delegate.key(),
        &vec![].into(),
        None,
        None,
        vec![InboundDelegateMsg::ApplicationMessage(msg)],
    )?;

    let response: OutboundAppMessage = match &outbound[0] {
        OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
        OutboundDelegateMsg::RequestUserInput(_)
        | OutboundDelegateMsg::ContextUpdated(_)
        | OutboundDelegateMsg::GetContractRequest(_)
        | OutboundDelegateMsg::PutContractRequest(_)
        | OutboundDelegateMsg::UpdateContractRequest(_)
        | OutboundDelegateMsg::SubscribeContractRequest(_)
        | OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected ApplicationMessage")
        }
    };
    // Without context persistence this returns CounterValue(1) again
    // because each call sees a fresh, empty context.
    assert!(
        matches!(response, OutboundAppMessage::CounterValue(2)),
        "second call: expected CounterValue(2) (context persisted), got {response:?}"
    );

    // Unregister must clear the persisted context so a re-registered
    // delegate with the same key starts from scratch.
    let cache = runtime.delegate_contexts.clone();
    runtime.unregister_delegate(delegate.key())?;
    assert!(
        !cache.contains_key(delegate.key()),
        "unregister_delegate should clear delegate_contexts entry"
    );

    std::mem::drop(temp_dir);
    Ok(())
}

/// FIX 4 (issue #4441 review): running a delegate populates the delegate
/// module cache with a real compiled-byte size, and `unregister_delegate`
/// MUST decrement `total_bytes` back to zero (it removes the cache entry via
/// `ModuleCache::remove`, not `LruCache::pop`, so the byte accounting stays
/// exact). A leak here would let the delegate cache's tracked total drift
/// above its real footprint and evict prematurely.
#[tokio::test(flavor = "multi_thread")]
async fn test_unregister_delegate_decrements_cache_bytes() -> Result<(), Box<dyn std::error::Error>>
{
    use delegate2_messages::InboundAppMessage;

    let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_2).await?;

    // Run the delegate once so its module is compiled and cached.
    let payload = bincode::serialize(&InboundAppMessage::IncrementCounter)?;
    let msg = ApplicationMessage::new(payload);
    let _outbound = runtime.inbound_app_message(
        delegate.key(),
        &vec![].into(),
        None,
        None,
        vec![InboundDelegateMsg::ApplicationMessage(msg)],
    )?;

    {
        let cache = runtime.delegate_modules.lock().unwrap();
        assert_eq!(
            cache.len(),
            1,
            "delegate module should be cached after a call"
        );
        assert!(
            cache.total_bytes() > 0,
            "delegate cache must track a non-zero compiled size"
        );
    }

    // Unregister must drop the module entry AND its bytes.
    runtime.unregister_delegate(delegate.key())?;
    {
        let cache = runtime.delegate_modules.lock().unwrap();
        assert_eq!(cache.len(), 0, "unregister must remove the cached module");
        assert_eq!(
            cache.total_bytes(),
            0,
            "unregister must decrement total_bytes to zero (no byte leak)"
        );
    }

    std::mem::drop(temp_dir);
    Ok(())
}

/// FIX 4 (issue #4441 review): the DELEGATE module cache evicts by BYTES
/// under a tight budget, exactly like the contract cache. Load several
/// distinct-param delegates over the same code under a budget that only
/// holds ~1-2 modules and assert the cache stays within budget and evicts
/// the rest (resident count far below the loaded count).
#[tokio::test(flavor = "multi_thread")]
async fn test_delegate_cache_evicts_by_bytes() -> Result<(), Box<dyn std::error::Error>> {
    use super::super::{ContractStore, SecretsStore, delegate_store::DelegateStore};
    use crate::contract::storages::Storage;

    let temp_dir = get_temp_dir();
    let db = Storage::new(temp_dir.path()).await?;
    let contract_store = ContractStore::new(temp_dir.path().join("c"), 10_000, db.clone())?;
    let delegate_store = DelegateStore::new(temp_dir.path().join("d"), 10_000, db.clone())?;
    let secret_store = SecretsStore::new(temp_dir.path().join("s"), Default::default(), db)?;

    // Probe one delegate's compiled size with a generous budget.
    let code = super::super::tests::get_test_module(TEST_DELEGATE_2)?;
    let mk_delegate = |params: Vec<u8>| {
        DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(Delegate::from((
            &code.clone().into(),
            &params.into(),
        ))))
    };

    let mut runtime = Runtime::build_with_config(
        contract_store,
        delegate_store,
        secret_store,
        false,
        super::super::runtime::RuntimeConfig {
            module_cache_budget_bytes: 512 * 1024 * 1024,
            ..Default::default()
        },
    )?;

    // Register + run 6 distinct-param delegates so each is a distinct key.
    let payload = bincode::serialize(&delegate2_messages::InboundAppMessage::IncrementCounter)?;
    let run = |runtime: &mut Runtime, d: &DelegateContainer| -> RuntimeResult<()> {
        let msg = ApplicationMessage::new(payload.clone());
        runtime.inbound_app_message(
            d.key(),
            &vec![].into(),
            None,
            None,
            vec![InboundDelegateMsg::ApplicationMessage(msg)],
        )?;
        Ok(())
    };

    let delegates: Vec<DelegateContainer> = (0..6)
        .map(|i| mk_delegate(format!("param-{i}").into_bytes()))
        .collect();
    for d in &delegates {
        runtime.delegate_store.store_delegate(d.clone()).unwrap();
        let key = XChaCha20Poly1305::generate_key(&mut OsRng);
        let cipher = XChaCha20Poly1305::new(&key);
        let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
        runtime
            .secret_store
            .register_delegate(d.key().clone(), cipher, nonce)
            .unwrap();
    }

    // Load the first to measure per-module size.
    run(&mut runtime, &delegates[0])?;
    let per_module = {
        let cache = runtime.delegate_modules.lock().unwrap();
        assert_eq!(cache.len(), 1);
        cache.total_bytes()
    };
    assert!(per_module > 0, "delegate module size must be measurable");

    // Re-budget the delegate cache to hold ~1-2 modules.
    let budget = per_module + per_module / 2; // between 1x and 2x
    {
        let mut cache = runtime.delegate_modules.lock().unwrap();
        *cache = super::super::ModuleCache::with_label(budget, "delegate", None);
    }

    // Load all 6 distinct delegates; the cache must stay within budget.
    for d in &delegates {
        run(&mut runtime, d)?;
        let cache = runtime.delegate_modules.lock().unwrap();
        assert!(
            cache.total_bytes() <= cache.budget_bytes(),
            "delegate cache total_bytes {} exceeded budget {}",
            cache.total_bytes(),
            cache.budget_bytes()
        );
    }

    let cache = runtime.delegate_modules.lock().unwrap();
    assert!(
        cache.len() < delegates.len(),
        "byte-budget eviction must drop some of the {} loaded delegates, got {} resident",
        delegates.len(),
        cache.len()
    );
    assert!(
        cache.total_bytes() <= cache.budget_bytes(),
        "final delegate cache total_bytes {} must be within budget {}",
        cache.total_bytes(),
        cache.budget_bytes()
    );

    std::mem::drop(temp_dir);
    Ok(())
}

/// Pin the oversize-context drop path: a delegate that writes a context
/// larger than `DelegateContext::MAX_SIZE` must NOT crash the runtime on
/// the subsequent call. The runtime drops the persisted bytes (with a
/// warn-level log) and the next call sees an empty context.
///
/// Without this guard the next `inbound_app_message` would assert inside
/// `DelegateContext::new(...)` when threading the bytes back to the WASM
/// boundary, taking down the executor pool worker.
#[tokio::test(flavor = "multi_thread")]
async fn test_context_oversize_is_dropped_not_crashed() -> Result<(), Box<dyn std::error::Error>> {
    use delegate2_messages::{InboundAppMessage, OutboundAppMessage};

    let contract = WrappedContract::new(
        Arc::new(ContractCode::from(vec![1])),
        Parameters::from(vec![]),
    );
    let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_2).await?;
    let _app = ContractInstanceId::try_from(contract.key.to_string()).unwrap();

    // Write past MAX_SIZE — `ctx.write` host fn has no size cap of its
    // own, so the WASM call itself succeeds even though the bytes won't
    // fit in `DelegateContext`.
    let oversize = freenet_stdlib::prelude::DelegateContext::MAX_SIZE + 1024;
    let payload = bincode::serialize(&InboundAppMessage::WriteLargeContext(oversize))?;
    let outbound = runtime.inbound_app_message(
        delegate.key(),
        &vec![].into(),
        None,
        None,
        vec![InboundDelegateMsg::ApplicationMessage(
            ApplicationMessage::new(payload),
        )],
    )?;
    assert!(
        matches!(&outbound[0], OutboundDelegateMsg::ApplicationMessage(_)),
        "first call should still produce a normal ApplicationMessage outbound"
    );

    // The persisted entry must be dropped — readers don't get to see
    // bytes that would crash on the next round-trip.
    assert!(
        !runtime.delegate_contexts.contains_key(delegate.key()),
        "oversize ctx.write should be dropped from the persisted cache"
    );

    // A follow-up call must succeed (no crash) and the delegate must
    // observe an empty context (NOT the oversize bytes).
    let payload = bincode::serialize(&InboundAppMessage::ReadContext)?;
    let outbound = runtime.inbound_app_message(
        delegate.key(),
        &vec![].into(),
        None,
        None,
        vec![InboundDelegateMsg::ApplicationMessage(
            ApplicationMessage::new(payload),
        )],
    )?;
    // Wildcard satisfies #[non_exhaustive] on OutboundDelegateMsg so
    // future stdlib variants don't break this test at compile time.
    #[allow(clippy::wildcard_enum_match_arm)]
    let response: OutboundAppMessage = match &outbound[0] {
        OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
        other => panic!("Expected ApplicationMessage, got {other:?}"),
    };
    // Wildcard satisfies #[non_exhaustive] on OutboundAppMessage so
    // future stdlib variants don't break this test at compile time.
    #[allow(clippy::wildcard_enum_match_arm)]
    match response {
        OutboundAppMessage::ContextData(data) => {
            assert!(
                data.is_empty(),
                "after oversize drop, next call must read an empty context, got {} bytes",
                data.len()
            );
        }
        other => panic!("Expected ContextData, got {other:?}"),
    }

    std::mem::drop(temp_dir);
    Ok(())
}

/// Pin the TTL-based eviction: an entry whose `last_write` is older than
/// `DELEGATE_CONTEXT_TTL` is dropped on the next sweep, even if
/// `unregister_delegate` is never called. Without TTL the cache leaks
/// pending-state bytes for any delegate whose `RequestUserInput` doesn't
/// receive a matching `UserResponse` (prompt dismissed, app crash,
/// network partition) until process restart.
///
/// Doesn't go through the WASM path; manipulates the cache and then
/// drives a no-op `inbound_app_message` so the loader's `prune_expired`
/// runs.
#[tokio::test(flavor = "multi_thread")]
async fn test_context_ttl_evicts_stale_entry() -> Result<(), Box<dyn std::error::Error>> {
    use delegate2_messages::InboundAppMessage;

    let contract = WrappedContract::new(
        Arc::new(ContractCode::from(vec![1])),
        Parameters::from(vec![]),
    );
    let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_2).await?;
    let _app = ContractInstanceId::try_from(contract.key.to_string()).unwrap();

    // Plant an entry whose last_write is older than the TTL. Using
    // `tokio::time::Instant` matches the entry's storage type and lets
    // `tokio::time::pause` / `tokio::time::advance` drive the sweep
    // deterministically in future tests if needed.
    let stale_at = tokio::time::Instant::now()
        .checked_sub(super::super::native_api::DELEGATE_CONTEXT_TTL)
        .expect("Instant arithmetic on test platform")
        .checked_sub(std::time::Duration::from_secs(1))
        .expect("Instant arithmetic on test platform");
    runtime.delegate_contexts.insert(
        delegate.key().clone(),
        super::super::native_api::DelegateContextEntry {
            bytes: vec![0xAA; 64],
            last_write: stale_at,
        },
    );
    assert!(runtime.delegate_contexts.contains_key(delegate.key()));

    // Drive any inbound — the loader's prune sweep runs first.
    let payload = bincode::serialize(&InboundAppMessage::ReadContext)?;
    runtime.inbound_app_message(
        delegate.key(),
        &vec![].into(),
        None,
        None,
        vec![InboundDelegateMsg::ApplicationMessage(
            ApplicationMessage::new(payload),
        )],
    )?;

    // The stale entry must be gone. The ReadContext call wrote nothing,
    // so no fresh entry was inserted in its place.
    assert!(
        !runtime.delegate_contexts.contains_key(delegate.key()),
        "TTL sweep should evict an entry older than DELEGATE_CONTEXT_TTL"
    );

    std::mem::drop(temp_dir);
    Ok(())
}

/// Checks that `inbound_app_message` calls `touch_inherited_origin` before
/// `prune_expired_inherited_origins`. The other tests verify the eviction
/// logic on its own, so without this one nobody would notice if those two
/// calls were removed or swapped — and the #3492 leak would come back. Reads
/// the source and asserts both calls are present and in that order.
#[test]
fn inbound_app_message_wires_inherited_origins_ttl_in_order() {
    let src = include_str!("interface.rs");
    // The impl is the LAST occurrence; the first is the trait method signature.
    let body = src
        .rsplit("fn inbound_app_message(")
        .next()
        .expect("inbound_app_message impl must exist");
    // Anchor on the call, not its arguments: the map these take is injected
    // state now (#4813), and pinning the argument list would have failed that
    // refactor for no reason. What must not regress is that both run, in order.
    let touch = body
        .find("touch_inherited_origin(")
        .expect("inbound_app_message must refresh inherited-origin liveness (touch)");
    let prune = body
        .find("prune_expired_inherited_origins(")
        .expect("inbound_app_message must run the inherited-origins TTL sweep (prune)");
    assert!(
        touch < prune,
        "touch_inherited_origin must run BEFORE prune so a just-delivered message is not evicted"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_has_secret_host_function() -> Result<(), Box<dyn std::error::Error>> {
    use delegate2_messages::{InboundAppMessage, OutboundAppMessage};

    let contract = WrappedContract::new(
        Arc::new(ContractCode::from(vec![1])),
        Parameters::from(vec![]),
    );
    let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_2).await?;
    let _app = ContractInstanceId::try_from(contract.key.to_string()).unwrap();

    let secret_key = vec![10, 20, 30];
    let secret_value = vec![100, 200];

    let payload = bincode::serialize(&InboundAppMessage::HasSecret(secret_key.clone()))?;
    let msg = ApplicationMessage::new(payload);
    let outbound = runtime.inbound_app_message(
        delegate.key(),
        &vec![].into(),
        None,
        None,
        vec![InboundDelegateMsg::ApplicationMessage(msg)],
    )?;

    let response: OutboundAppMessage = match &outbound[0] {
        OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
        OutboundDelegateMsg::RequestUserInput(_)
        | OutboundDelegateMsg::ContextUpdated(_)
        | OutboundDelegateMsg::GetContractRequest(_)
        | OutboundDelegateMsg::PutContractRequest(_)
        | OutboundDelegateMsg::UpdateContractRequest(_)
        | OutboundDelegateMsg::SubscribeContractRequest(_)
        | OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected ApplicationMessage")
        }
    };
    assert!(matches!(response, OutboundAppMessage::SecretExists(false)));

    let payload = bincode::serialize(&InboundAppMessage::StoreSecret {
        key: secret_key.clone(),
        value: secret_value.clone(),
    })?;
    let msg = ApplicationMessage::new(payload);
    let _ = runtime.inbound_app_message(
        delegate.key(),
        &vec![].into(),
        None,
        None,
        vec![InboundDelegateMsg::ApplicationMessage(msg)],
    )?;

    let payload = bincode::serialize(&InboundAppMessage::HasSecret(secret_key.clone()))?;
    let msg = ApplicationMessage::new(payload);
    let outbound = runtime.inbound_app_message(
        delegate.key(),
        &vec![].into(),
        None,
        None,
        vec![InboundDelegateMsg::ApplicationMessage(msg)],
    )?;

    let response: OutboundAppMessage = match &outbound[0] {
        OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
        OutboundDelegateMsg::RequestUserInput(_)
        | OutboundDelegateMsg::ContextUpdated(_)
        | OutboundDelegateMsg::GetContractRequest(_)
        | OutboundDelegateMsg::PutContractRequest(_)
        | OutboundDelegateMsg::UpdateContractRequest(_)
        | OutboundDelegateMsg::SubscribeContractRequest(_)
        | OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected ApplicationMessage")
        }
    };
    assert!(matches!(response, OutboundAppMessage::SecretExists(true)));

    std::mem::drop(temp_dir);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_get_nonexistent_secret() -> Result<(), Box<dyn std::error::Error>> {
    use delegate2_messages::{InboundAppMessage, OutboundAppMessage};

    let contract = WrappedContract::new(
        Arc::new(ContractCode::from(vec![1])),
        Parameters::from(vec![]),
    );
    let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_2).await?;
    let _app = ContractInstanceId::try_from(contract.key.to_string()).unwrap();

    let nonexistent_key = vec![99, 98, 97];
    let payload = bincode::serialize(&InboundAppMessage::GetNonExistentSecret(nonexistent_key))?;
    let msg = ApplicationMessage::new(payload);
    let outbound = runtime.inbound_app_message(
        delegate.key(),
        &vec![].into(),
        None,
        None,
        vec![InboundDelegateMsg::ApplicationMessage(msg)],
    )?;

    let response: OutboundAppMessage = match &outbound[0] {
        OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
        OutboundDelegateMsg::RequestUserInput(_)
        | OutboundDelegateMsg::ContextUpdated(_)
        | OutboundDelegateMsg::GetContractRequest(_)
        | OutboundDelegateMsg::PutContractRequest(_)
        | OutboundDelegateMsg::UpdateContractRequest(_)
        | OutboundDelegateMsg::SubscribeContractRequest(_)
        | OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected ApplicationMessage")
        }
    };
    assert!(matches!(response, OutboundAppMessage::SecretResult(None)));

    std::mem::drop(temp_dir);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_store_and_retrieve_secret() -> Result<(), Box<dyn std::error::Error>> {
    use delegate2_messages::{InboundAppMessage, OutboundAppMessage};

    let contract = WrappedContract::new(
        Arc::new(ContractCode::from(vec![1])),
        Parameters::from(vec![]),
    );
    let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_2).await?;
    let _app = ContractInstanceId::try_from(contract.key.to_string()).unwrap();

    let secret_key = vec![42, 43, 44];
    let secret_value = vec![1, 2, 3, 4, 5, 6, 7, 8];

    let payload = bincode::serialize(&InboundAppMessage::StoreSecret {
        key: secret_key.clone(),
        value: secret_value.clone(),
    })?;
    let msg = ApplicationMessage::new(payload);
    let outbound = runtime.inbound_app_message(
        delegate.key(),
        &vec![].into(),
        None,
        None,
        vec![InboundDelegateMsg::ApplicationMessage(msg)],
    )?;

    let response: OutboundAppMessage = match &outbound[0] {
        OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
        OutboundDelegateMsg::RequestUserInput(_)
        | OutboundDelegateMsg::ContextUpdated(_)
        | OutboundDelegateMsg::GetContractRequest(_)
        | OutboundDelegateMsg::PutContractRequest(_)
        | OutboundDelegateMsg::UpdateContractRequest(_)
        | OutboundDelegateMsg::SubscribeContractRequest(_)
        | OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected ApplicationMessage")
        }
    };
    assert!(matches!(response, OutboundAppMessage::SecretStored));

    let payload = bincode::serialize(&InboundAppMessage::GetNonExistentSecret(secret_key.clone()))?;
    let msg = ApplicationMessage::new(payload);
    let outbound = runtime.inbound_app_message(
        delegate.key(),
        &vec![].into(),
        None,
        None,
        vec![InboundDelegateMsg::ApplicationMessage(msg)],
    )?;

    let response: OutboundAppMessage = match &outbound[0] {
        OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
        OutboundDelegateMsg::RequestUserInput(_)
        | OutboundDelegateMsg::ContextUpdated(_)
        | OutboundDelegateMsg::GetContractRequest(_)
        | OutboundDelegateMsg::PutContractRequest(_)
        | OutboundDelegateMsg::UpdateContractRequest(_)
        | OutboundDelegateMsg::SubscribeContractRequest(_)
        | OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected ApplicationMessage")
        }
    };
    match response {
        OutboundAppMessage::SecretResult(Some(value)) => {
            assert_eq!(value, secret_value);
        }
        other @ OutboundAppMessage::CreateInboxResponse(_)
        | other @ OutboundAppMessage::MessageSigned(_)
        | other @ OutboundAppMessage::ContextData(_)
        | other @ OutboundAppMessage::CounterValue(_)
        | other @ OutboundAppMessage::SecretExists(_)
        | other @ OutboundAppMessage::SecretResult(_)
        | other @ OutboundAppMessage::ContextWritten
        | other @ OutboundAppMessage::ContextCleared
        | other @ OutboundAppMessage::SecretStored
        | other @ OutboundAppMessage::SecretRemoved
        | other @ OutboundAppMessage::LargeContextWritten(_)
        | other @ OutboundAppMessage::LargeSecretStored(_)
        | other @ OutboundAppMessage::SecretStoreFailed => {
            panic!("Expected SecretResult(Some(...)), got {:?}", other)
        }
    }

    std::mem::drop(temp_dir);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_set_secret_failure_returns_secret_store_failed()
-> Result<(), Box<dyn std::error::Error>> {
    use delegate2_messages::{InboundAppMessage, OutboundAppMessage};

    let contract = WrappedContract::new(
        Arc::new(ContractCode::from(vec![1])),
        Parameters::from(vec![]),
    );
    let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_2).await?;
    let _app = ContractInstanceId::try_from(contract.key.to_string()).unwrap();

    // Make secrets directory read-only so store_secret fails with an I/O error
    let secrets_dir = temp_dir.path().join("secrets");
    std::fs::set_permissions(&secrets_dir, std::fs::Permissions::from_mode(0o444))?;

    let payload = bincode::serialize(&InboundAppMessage::StoreSecret {
        key: vec![1, 2, 3],
        value: vec![4, 5, 6],
    })?;
    let msg = ApplicationMessage::new(payload);
    let outbound = runtime.inbound_app_message(
        delegate.key(),
        &vec![].into(),
        None,
        None,
        vec![InboundDelegateMsg::ApplicationMessage(msg)],
    )?;

    let response: OutboundAppMessage = match &outbound[0] {
        OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
        OutboundDelegateMsg::RequestUserInput(_)
        | OutboundDelegateMsg::ContextUpdated(_)
        | OutboundDelegateMsg::GetContractRequest(_)
        | OutboundDelegateMsg::PutContractRequest(_)
        | OutboundDelegateMsg::UpdateContractRequest(_)
        | OutboundDelegateMsg::SubscribeContractRequest(_)
        | OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected ApplicationMessage")
        }
    };
    assert!(
        matches!(response, OutboundAppMessage::SecretStoreFailed),
        "Expected SecretStoreFailed when secrets dir is read-only, got {:?}",
        response
    );

    // Restore permissions so temp_dir cleanup works
    std::fs::set_permissions(&secrets_dir, std::fs::Permissions::from_mode(0o755))?;
    std::mem::drop(temp_dir);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_read_empty_context() -> Result<(), Box<dyn std::error::Error>> {
    use delegate2_messages::{InboundAppMessage, OutboundAppMessage};

    let contract = WrappedContract::new(
        Arc::new(ContractCode::from(vec![1])),
        Parameters::from(vec![]),
    );
    let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_2).await?;
    let _app = ContractInstanceId::try_from(contract.key.to_string()).unwrap();

    let payload = bincode::serialize(&InboundAppMessage::ReadContext)?;
    let msg = ApplicationMessage::new(payload);
    let outbound = runtime.inbound_app_message(
        delegate.key(),
        &vec![].into(),
        None,
        None,
        vec![InboundDelegateMsg::ApplicationMessage(msg)],
    )?;

    let response: OutboundAppMessage = match &outbound[0] {
        OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
        OutboundDelegateMsg::RequestUserInput(_)
        | OutboundDelegateMsg::ContextUpdated(_)
        | OutboundDelegateMsg::GetContractRequest(_)
        | OutboundDelegateMsg::PutContractRequest(_)
        | OutboundDelegateMsg::UpdateContractRequest(_)
        | OutboundDelegateMsg::SubscribeContractRequest(_)
        | OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected ApplicationMessage")
        }
    };
    match response {
        OutboundAppMessage::ContextData(data) => {
            assert!(data.is_empty());
        }
        other @ OutboundAppMessage::CreateInboxResponse(_)
        | other @ OutboundAppMessage::MessageSigned(_)
        | other @ OutboundAppMessage::CounterValue(_)
        | other @ OutboundAppMessage::SecretExists(_)
        | other @ OutboundAppMessage::SecretResult(_)
        | other @ OutboundAppMessage::ContextWritten
        | other @ OutboundAppMessage::ContextCleared
        | other @ OutboundAppMessage::SecretStored
        | other @ OutboundAppMessage::SecretRemoved
        | other @ OutboundAppMessage::LargeContextWritten(_)
        | other @ OutboundAppMessage::LargeSecretStored(_)
        | other @ OutboundAppMessage::SecretStoreFailed => {
            panic!("Expected ContextData, got {:?}", other)
        }
    }

    std::mem::drop(temp_dir);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_context_clear() -> Result<(), Box<dyn std::error::Error>> {
    use delegate2_messages::{InboundAppMessage, OutboundAppMessage};

    let contract = WrappedContract::new(
        Arc::new(ContractCode::from(vec![1])),
        Parameters::from(vec![]),
    );
    let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_2).await?;
    let _app = ContractInstanceId::try_from(contract.key.to_string()).unwrap();

    let test_data = vec![1, 2, 3, 4, 5];
    let payload = bincode::serialize(&InboundAppMessage::WriteContext(test_data.clone()))?;
    let msg = ApplicationMessage::new(payload);
    let _ = runtime.inbound_app_message(
        delegate.key(),
        &vec![].into(),
        None,
        None,
        vec![InboundDelegateMsg::ApplicationMessage(msg)],
    )?;

    let payload = bincode::serialize(&InboundAppMessage::ClearContext)?;
    let msg = ApplicationMessage::new(payload);
    let outbound = runtime.inbound_app_message(
        delegate.key(),
        &vec![].into(),
        None,
        None,
        vec![InboundDelegateMsg::ApplicationMessage(msg)],
    )?;

    let response: OutboundAppMessage = match &outbound[0] {
        OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
        OutboundDelegateMsg::RequestUserInput(_)
        | OutboundDelegateMsg::ContextUpdated(_)
        | OutboundDelegateMsg::GetContractRequest(_)
        | OutboundDelegateMsg::PutContractRequest(_)
        | OutboundDelegateMsg::UpdateContractRequest(_)
        | OutboundDelegateMsg::SubscribeContractRequest(_)
        | OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected ApplicationMessage")
        }
    };
    assert!(matches!(response, OutboundAppMessage::ContextCleared));

    let payload = bincode::serialize(&InboundAppMessage::ReadContext)?;
    let msg = ApplicationMessage::new(payload);
    let outbound = runtime.inbound_app_message(
        delegate.key(),
        &vec![].into(),
        None,
        None,
        vec![InboundDelegateMsg::ApplicationMessage(msg)],
    )?;

    let response: OutboundAppMessage = match &outbound[0] {
        OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
        OutboundDelegateMsg::RequestUserInput(_)
        | OutboundDelegateMsg::ContextUpdated(_)
        | OutboundDelegateMsg::GetContractRequest(_)
        | OutboundDelegateMsg::PutContractRequest(_)
        | OutboundDelegateMsg::UpdateContractRequest(_)
        | OutboundDelegateMsg::SubscribeContractRequest(_)
        | OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected ApplicationMessage")
        }
    };
    match response {
        OutboundAppMessage::ContextData(data) => {
            assert!(data.is_empty());
        }
        other @ OutboundAppMessage::CreateInboxResponse(_)
        | other @ OutboundAppMessage::MessageSigned(_)
        | other @ OutboundAppMessage::CounterValue(_)
        | other @ OutboundAppMessage::SecretExists(_)
        | other @ OutboundAppMessage::SecretResult(_)
        | other @ OutboundAppMessage::ContextWritten
        | other @ OutboundAppMessage::ContextCleared
        | other @ OutboundAppMessage::SecretStored
        | other @ OutboundAppMessage::SecretRemoved
        | other @ OutboundAppMessage::LargeContextWritten(_)
        | other @ OutboundAppMessage::LargeSecretStored(_)
        | other @ OutboundAppMessage::SecretStoreFailed => {
            panic!("Expected ContextData, got {:?}", other)
        }
    }

    std::mem::drop(temp_dir);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_context_shared_across_batch() -> Result<(), Box<dyn std::error::Error>> {
    use delegate2_messages::{InboundAppMessage, OutboundAppMessage};

    let contract = WrappedContract::new(
        Arc::new(ContractCode::from(vec![1])),
        Parameters::from(vec![]),
    );
    let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_2).await?;
    let _app = ContractInstanceId::try_from(contract.key.to_string()).unwrap();

    let messages: Vec<InboundDelegateMsg> = (0..3)
        .map(|_| {
            let payload = bincode::serialize(&InboundAppMessage::IncrementCounter).unwrap();
            InboundDelegateMsg::ApplicationMessage(ApplicationMessage::new(payload))
        })
        .collect();

    let outbound =
        runtime.inbound_app_message(delegate.key(), &vec![].into(), None, None, messages)?;

    assert_eq!(outbound.len(), 3);

    for (i, msg) in outbound.iter().enumerate() {
        let response: OutboundAppMessage = match msg {
            OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
            OutboundDelegateMsg::RequestUserInput(_)
            | OutboundDelegateMsg::ContextUpdated(_)
            | OutboundDelegateMsg::GetContractRequest(_)
            | OutboundDelegateMsg::PutContractRequest(_)
            | OutboundDelegateMsg::UpdateContractRequest(_)
            | OutboundDelegateMsg::SubscribeContractRequest(_)
            | OutboundDelegateMsg::SendDelegateMessage(_) => {
                panic!("Expected ApplicationMessage")
            }
        };
        match response {
            OutboundAppMessage::CounterValue(value) => {
                assert_eq!(value, (i + 1) as u32);
            }
            other @ OutboundAppMessage::CreateInboxResponse(_)
            | other @ OutboundAppMessage::MessageSigned(_)
            | other @ OutboundAppMessage::ContextData(_)
            | other @ OutboundAppMessage::SecretExists(_)
            | other @ OutboundAppMessage::SecretResult(_)
            | other @ OutboundAppMessage::ContextWritten
            | other @ OutboundAppMessage::ContextCleared
            | other @ OutboundAppMessage::SecretStored
            | other @ OutboundAppMessage::SecretRemoved
            | other @ OutboundAppMessage::LargeContextWritten(_)
            | other @ OutboundAppMessage::LargeSecretStored(_)
            | other @ OutboundAppMessage::SecretStoreFailed => {
                panic!("Expected CounterValue, got {:?}", other)
            }
        }
    }

    std::mem::drop(temp_dir);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_remove_secret_host_function() -> Result<(), Box<dyn std::error::Error>> {
    use delegate2_messages::{InboundAppMessage, OutboundAppMessage};

    let contract = WrappedContract::new(
        Arc::new(ContractCode::from(vec![1])),
        Parameters::from(vec![]),
    );
    let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_2).await?;
    let _app = ContractInstanceId::try_from(contract.key.to_string()).unwrap();

    let secret_key = vec![50, 51, 52];
    let secret_value = vec![200, 201, 202];

    let payload = bincode::serialize(&InboundAppMessage::StoreSecret {
        key: secret_key.clone(),
        value: secret_value.clone(),
    })?;
    let msg = ApplicationMessage::new(payload);
    let _ = runtime.inbound_app_message(
        delegate.key(),
        &vec![].into(),
        None,
        None,
        vec![InboundDelegateMsg::ApplicationMessage(msg)],
    )?;

    let payload = bincode::serialize(&InboundAppMessage::HasSecret(secret_key.clone()))?;
    let msg = ApplicationMessage::new(payload);
    let outbound = runtime.inbound_app_message(
        delegate.key(),
        &vec![].into(),
        None,
        None,
        vec![InboundDelegateMsg::ApplicationMessage(msg)],
    )?;
    let response: OutboundAppMessage = match &outbound[0] {
        OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
        OutboundDelegateMsg::RequestUserInput(_)
        | OutboundDelegateMsg::ContextUpdated(_)
        | OutboundDelegateMsg::GetContractRequest(_)
        | OutboundDelegateMsg::PutContractRequest(_)
        | OutboundDelegateMsg::UpdateContractRequest(_)
        | OutboundDelegateMsg::SubscribeContractRequest(_)
        | OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected ApplicationMessage")
        }
    };
    assert!(matches!(response, OutboundAppMessage::SecretExists(true)));

    let payload = bincode::serialize(&InboundAppMessage::RemoveSecret(secret_key.clone()))?;
    let msg = ApplicationMessage::new(payload);
    let outbound = runtime.inbound_app_message(
        delegate.key(),
        &vec![].into(),
        None,
        None,
        vec![InboundDelegateMsg::ApplicationMessage(msg)],
    )?;
    let response: OutboundAppMessage = match &outbound[0] {
        OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
        OutboundDelegateMsg::RequestUserInput(_)
        | OutboundDelegateMsg::ContextUpdated(_)
        | OutboundDelegateMsg::GetContractRequest(_)
        | OutboundDelegateMsg::PutContractRequest(_)
        | OutboundDelegateMsg::UpdateContractRequest(_)
        | OutboundDelegateMsg::SubscribeContractRequest(_)
        | OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected ApplicationMessage")
        }
    };
    assert!(matches!(response, OutboundAppMessage::SecretRemoved));

    let payload = bincode::serialize(&InboundAppMessage::HasSecret(secret_key.clone()))?;
    let msg = ApplicationMessage::new(payload);
    let outbound = runtime.inbound_app_message(
        delegate.key(),
        &vec![].into(),
        None,
        None,
        vec![InboundDelegateMsg::ApplicationMessage(msg)],
    )?;
    let response: OutboundAppMessage = match &outbound[0] {
        OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
        OutboundDelegateMsg::RequestUserInput(_)
        | OutboundDelegateMsg::ContextUpdated(_)
        | OutboundDelegateMsg::GetContractRequest(_)
        | OutboundDelegateMsg::PutContractRequest(_)
        | OutboundDelegateMsg::UpdateContractRequest(_)
        | OutboundDelegateMsg::SubscribeContractRequest(_)
        | OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected ApplicationMessage")
        }
    };
    assert!(matches!(response, OutboundAppMessage::SecretExists(false)));

    std::mem::drop(temp_dir);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_large_context_data() -> Result<(), Box<dyn std::error::Error>> {
    use delegate2_messages::{InboundAppMessage, OutboundAppMessage};

    let contract = WrappedContract::new(
        Arc::new(ContractCode::from(vec![1])),
        Parameters::from(vec![]),
    );
    let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_2).await?;
    let _app = ContractInstanceId::try_from(contract.key.to_string()).unwrap();

    // `DelegateContext::MAX_SIZE` caps the on-wire context size; pick the
    // largest value that fits with a margin so the persisted bytes still
    // round-trip through `DelegateContext::new(...)` on the next call.
    // The pre-fix version of this test wrote 1 MiB, well past the cap;
    // it only avoided the assertion because context didn't persist
    // between calls (the bug this PR fixes).
    let large_size = (freenet_stdlib::prelude::DelegateContext::MAX_SIZE / 2).min(256 * 1024);

    let payload = bincode::serialize(&InboundAppMessage::WriteLargeContext(large_size))?;
    let msg = ApplicationMessage::new(payload);
    let outbound = runtime.inbound_app_message(
        delegate.key(),
        &vec![].into(),
        None,
        None,
        vec![InboundDelegateMsg::ApplicationMessage(msg)],
    )?;
    let response: OutboundAppMessage = match &outbound[0] {
        OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
        OutboundDelegateMsg::RequestUserInput(_)
        | OutboundDelegateMsg::ContextUpdated(_)
        | OutboundDelegateMsg::GetContractRequest(_)
        | OutboundDelegateMsg::PutContractRequest(_)
        | OutboundDelegateMsg::UpdateContractRequest(_)
        | OutboundDelegateMsg::SubscribeContractRequest(_)
        | OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected ApplicationMessage")
        }
    };
    match response {
        OutboundAppMessage::LargeContextWritten(size) => {
            assert_eq!(size, large_size);
        }
        other @ OutboundAppMessage::CreateInboxResponse(_)
        | other @ OutboundAppMessage::MessageSigned(_)
        | other @ OutboundAppMessage::ContextData(_)
        | other @ OutboundAppMessage::CounterValue(_)
        | other @ OutboundAppMessage::SecretExists(_)
        | other @ OutboundAppMessage::SecretResult(_)
        | other @ OutboundAppMessage::ContextWritten
        | other @ OutboundAppMessage::ContextCleared
        | other @ OutboundAppMessage::SecretStored
        | other @ OutboundAppMessage::SecretRemoved
        | other @ OutboundAppMessage::LargeSecretStored(_)
        | other @ OutboundAppMessage::SecretStoreFailed => {
            panic!("Expected LargeContextWritten, got {:?}", other)
        }
    }

    let payload = bincode::serialize(&InboundAppMessage::ReadContext)?;
    let msg = ApplicationMessage::new(payload);
    let outbound = runtime.inbound_app_message(
        delegate.key(),
        &vec![].into(),
        None,
        None,
        vec![InboundDelegateMsg::ApplicationMessage(msg)],
    )?;
    let response: OutboundAppMessage = match &outbound[0] {
        OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
        OutboundDelegateMsg::RequestUserInput(_)
        | OutboundDelegateMsg::ContextUpdated(_)
        | OutboundDelegateMsg::GetContractRequest(_)
        | OutboundDelegateMsg::PutContractRequest(_)
        | OutboundDelegateMsg::UpdateContractRequest(_)
        | OutboundDelegateMsg::SubscribeContractRequest(_)
        | OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected ApplicationMessage")
        }
    };
    match response {
        OutboundAppMessage::ContextData(data) => {
            // Pre-fix this assertion expected `is_empty()` because the
            // context was reset between calls. With persistence the
            // bytes the delegate wrote on the previous call survive
            // and the read on this call must return them.
            assert_eq!(
                data.len(),
                large_size,
                "persisted context should round-trip the size the delegate wrote"
            );
        }
        other @ OutboundAppMessage::CreateInboxResponse(_)
        | other @ OutboundAppMessage::MessageSigned(_)
        | other @ OutboundAppMessage::CounterValue(_)
        | other @ OutboundAppMessage::SecretExists(_)
        | other @ OutboundAppMessage::SecretResult(_)
        | other @ OutboundAppMessage::ContextWritten
        | other @ OutboundAppMessage::ContextCleared
        | other @ OutboundAppMessage::SecretStored
        | other @ OutboundAppMessage::SecretRemoved
        | other @ OutboundAppMessage::LargeContextWritten(_)
        | other @ OutboundAppMessage::LargeSecretStored(_)
        | other @ OutboundAppMessage::SecretStoreFailed => {
            panic!("Expected ContextData, got {:?}", other)
        }
    }

    std::mem::drop(temp_dir);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_large_context_within_batch() -> Result<(), Box<dyn std::error::Error>> {
    use delegate2_messages::{InboundAppMessage, OutboundAppMessage};

    let contract = WrappedContract::new(
        Arc::new(ContractCode::from(vec![1])),
        Parameters::from(vec![]),
    );
    let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_2).await?;
    let _app = ContractInstanceId::try_from(contract.key.to_string()).unwrap();

    let large_size = 256 * 1024;

    let write_payload = bincode::serialize(&InboundAppMessage::WriteLargeContext(large_size))?;
    let read_payload = bincode::serialize(&InboundAppMessage::ReadContext)?;

    let messages = vec![
        InboundDelegateMsg::ApplicationMessage(ApplicationMessage::new(write_payload)),
        InboundDelegateMsg::ApplicationMessage(ApplicationMessage::new(read_payload)),
    ];

    let outbound =
        runtime.inbound_app_message(delegate.key(), &vec![].into(), None, None, messages)?;

    assert_eq!(outbound.len(), 2);

    let response: OutboundAppMessage = match &outbound[1] {
        OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
        OutboundDelegateMsg::RequestUserInput(_)
        | OutboundDelegateMsg::ContextUpdated(_)
        | OutboundDelegateMsg::GetContractRequest(_)
        | OutboundDelegateMsg::PutContractRequest(_)
        | OutboundDelegateMsg::UpdateContractRequest(_)
        | OutboundDelegateMsg::SubscribeContractRequest(_)
        | OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected ApplicationMessage")
        }
    };
    match response {
        OutboundAppMessage::ContextData(data) => {
            assert_eq!(data.len(), large_size);
            for (i, byte) in data.iter().enumerate() {
                assert_eq!(*byte, (i % 256) as u8, "Data pattern mismatch at index {i}");
            }
        }
        other @ OutboundAppMessage::CreateInboxResponse(_)
        | other @ OutboundAppMessage::MessageSigned(_)
        | other @ OutboundAppMessage::CounterValue(_)
        | other @ OutboundAppMessage::SecretExists(_)
        | other @ OutboundAppMessage::SecretResult(_)
        | other @ OutboundAppMessage::ContextWritten
        | other @ OutboundAppMessage::ContextCleared
        | other @ OutboundAppMessage::SecretStored
        | other @ OutboundAppMessage::SecretRemoved
        | other @ OutboundAppMessage::LargeContextWritten(_)
        | other @ OutboundAppMessage::LargeSecretStored(_)
        | other @ OutboundAppMessage::SecretStoreFailed => {
            panic!("Expected ContextData, got {:?}", other)
        }
    }

    std::mem::drop(temp_dir);
    Ok(())
}

/// Indirect regression test for #3248 (stale WASM memory base pointer).
///
/// Storing a 1 MB secret forces `memory.grow` which can relocate WASM linear
/// memory. If the cached `MEM_ADDR.start_ptr` is not refreshed via
/// `refresh_mem_addr_from_caller`, the subsequent read uses a stale pointer
/// and returns garbage data. Under full parallel test suite runs (~1600 tests)
/// the relocation is more likely due to memory pressure.
#[tokio::test(flavor = "multi_thread")]
async fn test_large_secret_data() -> Result<(), Box<dyn std::error::Error>> {
    use delegate2_messages::{InboundAppMessage, OutboundAppMessage};

    let contract = WrappedContract::new(
        Arc::new(ContractCode::from(vec![1])),
        Parameters::from(vec![]),
    );
    let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_2).await?;
    let _app = ContractInstanceId::try_from(contract.key.to_string()).unwrap();

    let secret_key = vec![77, 88, 99];
    let large_size = 1024 * 1024;

    let payload = bincode::serialize(&InboundAppMessage::StoreLargeSecret {
        key: secret_key.clone(),
        size: large_size,
    })?;
    let msg = ApplicationMessage::new(payload);
    let outbound = runtime.inbound_app_message(
        delegate.key(),
        &vec![].into(),
        None,
        None,
        vec![InboundDelegateMsg::ApplicationMessage(msg)],
    )?;
    let response: OutboundAppMessage = match &outbound[0] {
        OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
        OutboundDelegateMsg::RequestUserInput(_)
        | OutboundDelegateMsg::ContextUpdated(_)
        | OutboundDelegateMsg::GetContractRequest(_)
        | OutboundDelegateMsg::PutContractRequest(_)
        | OutboundDelegateMsg::UpdateContractRequest(_)
        | OutboundDelegateMsg::SubscribeContractRequest(_)
        | OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected ApplicationMessage")
        }
    };
    match response {
        OutboundAppMessage::LargeSecretStored(size) => {
            assert_eq!(size, large_size);
        }
        other @ OutboundAppMessage::CreateInboxResponse(_)
        | other @ OutboundAppMessage::MessageSigned(_)
        | other @ OutboundAppMessage::ContextData(_)
        | other @ OutboundAppMessage::CounterValue(_)
        | other @ OutboundAppMessage::SecretExists(_)
        | other @ OutboundAppMessage::SecretResult(_)
        | other @ OutboundAppMessage::ContextWritten
        | other @ OutboundAppMessage::ContextCleared
        | other @ OutboundAppMessage::SecretStored
        | other @ OutboundAppMessage::SecretRemoved
        | other @ OutboundAppMessage::LargeContextWritten(_)
        | other @ OutboundAppMessage::SecretStoreFailed => {
            panic!("Expected LargeSecretStored, got {:?}", other)
        }
    }

    let payload = bincode::serialize(&InboundAppMessage::GetNonExistentSecret(secret_key.clone()))?;
    let msg = ApplicationMessage::new(payload);
    let outbound = runtime.inbound_app_message(
        delegate.key(),
        &vec![].into(),
        None,
        None,
        vec![InboundDelegateMsg::ApplicationMessage(msg)],
    )?;
    let response: OutboundAppMessage = match &outbound[0] {
        OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
        OutboundDelegateMsg::RequestUserInput(_)
        | OutboundDelegateMsg::ContextUpdated(_)
        | OutboundDelegateMsg::GetContractRequest(_)
        | OutboundDelegateMsg::PutContractRequest(_)
        | OutboundDelegateMsg::UpdateContractRequest(_)
        | OutboundDelegateMsg::SubscribeContractRequest(_)
        | OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected ApplicationMessage")
        }
    };
    match response {
        OutboundAppMessage::SecretResult(Some(data)) => {
            assert_eq!(data.len(), large_size);
            for (i, byte) in data.iter().enumerate() {
                assert_eq!(*byte, (i % 256) as u8, "Data pattern mismatch at index {i}");
            }
        }
        other @ OutboundAppMessage::CreateInboxResponse(_)
        | other @ OutboundAppMessage::MessageSigned(_)
        | other @ OutboundAppMessage::ContextData(_)
        | other @ OutboundAppMessage::CounterValue(_)
        | other @ OutboundAppMessage::SecretExists(_)
        | other @ OutboundAppMessage::SecretResult(_)
        | other @ OutboundAppMessage::ContextWritten
        | other @ OutboundAppMessage::ContextCleared
        | other @ OutboundAppMessage::SecretStored
        | other @ OutboundAppMessage::SecretRemoved
        | other @ OutboundAppMessage::LargeContextWritten(_)
        | other @ OutboundAppMessage::LargeSecretStored(_)
        | other @ OutboundAppMessage::SecretStoreFailed => {
            panic!("Expected SecretResult(Some(...)), got {:?}", other)
        }
    }

    std::mem::drop(temp_dir);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_delegate_execution() -> Result<(), Box<dyn std::error::Error>> {
    use delegate2_messages::{InboundAppMessage, OutboundAppMessage};
    use std::sync::Arc;
    use tokio::sync::Barrier;

    let (delegate1, runtime1, temp_dir1) = setup_runtime(TEST_DELEGATE_2).await?;
    let (delegate2, runtime2, temp_dir2) = setup_runtime(TEST_DELEGATE_2).await?;

    let contract = WrappedContract::new(
        Arc::new(ContractCode::from(vec![1])),
        Parameters::from(vec![]),
    );
    let _app = ContractInstanceId::try_from(contract.key.to_string()).unwrap();

    let runtime1 = Arc::new(std::sync::Mutex::new(runtime1));
    let runtime2 = Arc::new(std::sync::Mutex::new(runtime2));
    let delegate1 = Arc::new(delegate1);
    let delegate2 = Arc::new(delegate2);

    let barrier = Arc::new(Barrier::new(2));

    let barrier1 = barrier.clone();
    let runtime1_clone = runtime1.clone();
    let delegate1_clone = delegate1.clone();
    let handle1 = tokio::spawn(async move {
        barrier1.wait().await;

        let secret_key = b"thread1_key".to_vec();
        let secret_value = b"thread1_value".to_vec();

        let payload = bincode::serialize(&InboundAppMessage::StoreSecret {
            key: secret_key.clone(),
            value: secret_value.clone(),
        })
        .unwrap();
        let msg = ApplicationMessage::new(payload);
        {
            let mut runtime = runtime1_clone.lock().unwrap();
            let _ = runtime
                .inbound_app_message(
                    delegate1_clone.key(),
                    &vec![].into(),
                    None,
                    None,
                    vec![InboundDelegateMsg::ApplicationMessage(msg)],
                )
                .unwrap();
        }

        let payload =
            bincode::serialize(&InboundAppMessage::GetNonExistentSecret(secret_key.clone()))
                .unwrap();
        let msg = ApplicationMessage::new(payload);
        let outbound = {
            let mut runtime = runtime1_clone.lock().unwrap();
            runtime
                .inbound_app_message(
                    delegate1_clone.key(),
                    &vec![].into(),
                    None,
                    None,
                    vec![InboundDelegateMsg::ApplicationMessage(msg)],
                )
                .unwrap()
        };

        let response: OutboundAppMessage = match &outbound[0] {
            OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload).unwrap(),
            OutboundDelegateMsg::RequestUserInput(_)
            | OutboundDelegateMsg::ContextUpdated(_)
            | OutboundDelegateMsg::GetContractRequest(_)
            | OutboundDelegateMsg::PutContractRequest(_)
            | OutboundDelegateMsg::UpdateContractRequest(_)
            | OutboundDelegateMsg::SubscribeContractRequest(_)
            | OutboundDelegateMsg::SendDelegateMessage(_) => {
                panic!("Expected ApplicationMessage")
            }
        };
        match response {
            OutboundAppMessage::SecretResult(Some(value)) => {
                assert_eq!(value, secret_value);
            }
            other @ OutboundAppMessage::CreateInboxResponse(_)
            | other @ OutboundAppMessage::MessageSigned(_)
            | other @ OutboundAppMessage::ContextData(_)
            | other @ OutboundAppMessage::CounterValue(_)
            | other @ OutboundAppMessage::SecretExists(_)
            | other @ OutboundAppMessage::SecretResult(_)
            | other @ OutboundAppMessage::ContextWritten
            | other @ OutboundAppMessage::ContextCleared
            | other @ OutboundAppMessage::SecretStored
            | other @ OutboundAppMessage::SecretRemoved
            | other @ OutboundAppMessage::LargeContextWritten(_)
            | other @ OutboundAppMessage::LargeSecretStored(_)
            | other @ OutboundAppMessage::SecretStoreFailed => panic!(
                "Thread 1: Expected SecretResult(Some(...)), got {:?}",
                other
            ),
        }
    });

    let barrier2 = barrier.clone();
    let runtime2_clone = runtime2.clone();
    let delegate2_clone = delegate2.clone();
    let handle2 = tokio::spawn(async move {
        barrier2.wait().await;

        let secret_key = b"thread2_key".to_vec();
        let secret_value = b"thread2_value".to_vec();

        let payload = bincode::serialize(&InboundAppMessage::StoreSecret {
            key: secret_key.clone(),
            value: secret_value.clone(),
        })
        .unwrap();
        let msg = ApplicationMessage::new(payload);
        {
            let mut runtime = runtime2_clone.lock().unwrap();
            let _ = runtime
                .inbound_app_message(
                    delegate2_clone.key(),
                    &vec![].into(),
                    None,
                    None,
                    vec![InboundDelegateMsg::ApplicationMessage(msg)],
                )
                .unwrap();
        }

        let payload =
            bincode::serialize(&InboundAppMessage::GetNonExistentSecret(secret_key.clone()))
                .unwrap();
        let msg = ApplicationMessage::new(payload);
        let outbound = {
            let mut runtime = runtime2_clone.lock().unwrap();
            runtime
                .inbound_app_message(
                    delegate2_clone.key(),
                    &vec![].into(),
                    None,
                    None,
                    vec![InboundDelegateMsg::ApplicationMessage(msg)],
                )
                .unwrap()
        };

        let response: OutboundAppMessage = match &outbound[0] {
            OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload).unwrap(),
            OutboundDelegateMsg::RequestUserInput(_)
            | OutboundDelegateMsg::ContextUpdated(_)
            | OutboundDelegateMsg::GetContractRequest(_)
            | OutboundDelegateMsg::PutContractRequest(_)
            | OutboundDelegateMsg::UpdateContractRequest(_)
            | OutboundDelegateMsg::SubscribeContractRequest(_)
            | OutboundDelegateMsg::SendDelegateMessage(_) => {
                panic!("Expected ApplicationMessage")
            }
        };
        match response {
            OutboundAppMessage::SecretResult(Some(value)) => {
                assert_eq!(value, secret_value);
            }
            other @ OutboundAppMessage::CreateInboxResponse(_)
            | other @ OutboundAppMessage::MessageSigned(_)
            | other @ OutboundAppMessage::ContextData(_)
            | other @ OutboundAppMessage::CounterValue(_)
            | other @ OutboundAppMessage::SecretExists(_)
            | other @ OutboundAppMessage::SecretResult(_)
            | other @ OutboundAppMessage::ContextWritten
            | other @ OutboundAppMessage::ContextCleared
            | other @ OutboundAppMessage::SecretStored
            | other @ OutboundAppMessage::SecretRemoved
            | other @ OutboundAppMessage::LargeContextWritten(_)
            | other @ OutboundAppMessage::LargeSecretStored(_)
            | other @ OutboundAppMessage::SecretStoreFailed => panic!(
                "Thread 2: Expected SecretResult(Some(...)), got {:?}",
                other
            ),
        }
    });

    handle1.await?;
    handle2.await?;

    std::mem::drop(temp_dir1);
    std::mem::drop(temp_dir2);
    Ok(())
}

/// Verify that V1 delegates are correctly detected as V1 even when
/// state_store_db is configured. This ensures backward compatibility —
/// V2 detection is based on module imports, not runtime configuration.
#[tokio::test(flavor = "multi_thread")]
async fn test_v1_delegate_detected_as_v1_with_state_store() -> Result<(), Box<dyn std::error::Error>>
{
    use crate::contract::storages::Storage;
    use delegate2_messages::{InboundAppMessage, OutboundAppMessage};

    let temp_dir = get_temp_dir();
    let contracts_dir = temp_dir.path().join("contracts");
    let delegates_dir = temp_dir.path().join("delegates");
    let secrets_dir = temp_dir.path().join("secrets");

    let db = Storage::new(temp_dir.path()).await?;
    let contract_store = ContractStore::new(contracts_dir, 10_000, db.clone())?;
    let delegate_store = DelegateStore::new(delegates_dir, 10_000, db.clone())?;
    let secret_store = SecretsStore::new(secrets_dir, Default::default(), db.clone())?;

    let mut runtime = Runtime::build(contract_store, delegate_store, secret_store, false).unwrap();

    // Configure state_store_db — V1 delegates should STILL be detected as V1
    runtime.set_state_store_db(db);

    let delegate = {
        let bytes = super::super::tests::get_test_module(TEST_DELEGATE_2)?;
        DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(Delegate::from((
            &bytes.into(),
            &vec![].into(),
        ))))
    };
    let _stored = runtime.delegate_store.store_delegate(delegate.clone());

    let key = XChaCha20Poly1305::generate_key(&mut OsRng);
    let cipher = XChaCha20Poly1305::new(&key);
    let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
    let _registered = runtime
        .secret_store
        .register_delegate(delegate.key().clone(), cipher, nonce);

    // Verify API version detection: V1 delegate should be V1
    let (mut running, api_version) =
        runtime.prepare_delegate_call(&vec![].into(), delegate.key(), 4096)?;
    assert_eq!(
        api_version,
        DelegateApiVersion::V1,
        "V1 delegate should be detected as V1 even with state_store_db configured"
    );
    runtime.drop_running_instance(&mut running);

    // Verify the delegate still works normally via the V1 path
    let contract = WrappedContract::new(
        Arc::new(ContractCode::from(vec![1])),
        Parameters::from(vec![]),
    );
    let _app = ContractInstanceId::try_from(contract.key.to_string()).unwrap();

    let payload: Vec<u8> = bincode::serialize(&InboundAppMessage::CreateInboxRequest).unwrap();
    let create_msg = ApplicationMessage::new(payload);
    let inbound = InboundDelegateMsg::ApplicationMessage(create_msg);
    let outbound =
        runtime.inbound_app_message(delegate.key(), &vec![].into(), None, None, vec![inbound])?;

    let expected_payload =
        bincode::serialize(&OutboundAppMessage::CreateInboxResponse(vec![1])).unwrap();
    assert_eq!(outbound.len(), 1);
    assert!(matches!(
        outbound.first(),
        Some(OutboundDelegateMsg::ApplicationMessage(msg)) if *msg.payload == expected_payload
    ));

    std::mem::drop(temp_dir);
    Ok(())
}

const TEST_DELEGATE_V2_CONTRACTS: &str = "test_delegate_v2_contracts";

/// Message types for test-delegate-v2-contracts (must match the delegate's types)
mod v2_contracts_messages {
    use super::*;

    #[derive(Debug, Serialize, Deserialize)]
    pub enum InboundAppMessage {
        GetContractState {
            contract_id: [u8; 32],
        },
        PutContractState {
            contract_id: [u8; 32],
            state: Vec<u8>,
        },
        UpdateContractState {
            contract_id: [u8; 32],
            state: Vec<u8>,
        },
        SubscribeContract {
            contract_id: [u8; 32],
        },
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub enum OutboundAppMessage {
        ContractState {
            contract_id: [u8; 32],
            state: Vec<u8>,
        },
        ContractNotFound {
            contract_id: [u8; 32],
            error_code: i64,
        },
        Success {
            contract_id: [u8; 32],
        },
        Failed {
            contract_id: [u8; 32],
            error_code: i64,
        },
    }
}

/// V2 delegate end-to-end test: a real compiled WASM delegate that reads
/// contract state via host functions from the `freenet_delegate_contracts`
/// namespace. This exercises the full V2 async call path:
///
/// 1. Module is detected as V2 (imports `freenet_delegate_contracts`)
/// 2. `call_3i64_async_imports` is used instead of `call_3i64`
/// 3. Host functions `get_contract_state_len` and `get_contract_state`
///    read from the ReDb state store
#[tokio::test(flavor = "multi_thread")]
async fn test_v2_delegate_reads_contract_state() -> Result<(), Box<dyn std::error::Error>> {
    use crate::contract::storages::Storage;
    use crate::wasm_runtime::StateStorage;
    use v2_contracts_messages::*;

    let temp_dir = get_temp_dir();
    let contracts_dir = temp_dir.path().join("contracts");
    let delegates_dir = temp_dir.path().join("delegates");
    let secrets_dir = temp_dir.path().join("secrets");

    let db = Storage::new(temp_dir.path()).await?;
    let contract_store = ContractStore::new(contracts_dir, 10_000, db.clone())?;
    let delegate_store = DelegateStore::new(delegates_dir, 10_000, db.clone())?;
    let secret_store = SecretsStore::new(secrets_dir, Default::default(), db.clone())?;

    let mut runtime = Runtime::build(contract_store, delegate_store, secret_store, false).unwrap();
    runtime.set_state_store_db(db.clone());

    // Store contract state in the DB so the V2 delegate can read it
    let contract_instance_id = ContractInstanceId::new([42u8; 32]);
    let contract_code = ContractCode::from(vec![1, 2, 3]);
    let contract_key = ContractKey::from_id_and_code(contract_instance_id, *contract_code.hash());
    let expected_state = vec![10, 20, 30, 40, 50, 60, 70, 80];
    db.store(contract_key, WrappedState::new(expected_state.clone()))
        .await?;
    // Index the contract so code_hash_from_id() works
    runtime.contract_store.ensure_key_indexed(&contract_key)?;

    // Load the V2 delegate
    let delegate = {
        let bytes = super::super::tests::get_test_module(TEST_DELEGATE_V2_CONTRACTS)?;
        DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(Delegate::from((
            &bytes.into(),
            &vec![].into(),
        ))))
    };
    let _stored = runtime.delegate_store.store_delegate(delegate.clone());

    let key = XChaCha20Poly1305::generate_key(&mut OsRng);
    let cipher = XChaCha20Poly1305::new(&key);
    let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
    let _registered = runtime
        .secret_store
        .register_delegate(delegate.key().clone(), cipher, nonce);

    // Verify the module is detected as V2
    let (mut running, api_version) =
        runtime.prepare_delegate_call(&vec![].into(), delegate.key(), 4096)?;
    assert_eq!(
        api_version,
        DelegateApiVersion::V2,
        "V2 delegate should be detected as V2 (imports freenet_delegate_contracts)"
    );
    runtime.drop_running_instance(&mut running);

    // Send a message asking the delegate to read the contract state
    let _app_id = ContractInstanceId::new([1u8; 32]);
    let command = InboundAppMessage::GetContractState {
        contract_id: [42u8; 32],
    };
    let payload = bincode::serialize(&command)?;
    let app_msg = ApplicationMessage::new(payload);

    let outbound = runtime.inbound_app_message(
        delegate.key(),
        &vec![].into(),
        None,
        None,
        vec![InboundDelegateMsg::ApplicationMessage(app_msg)],
    )?;

    assert_eq!(outbound.len(), 1, "Expected exactly one outbound message");
    let response_msg = match &outbound[0] {
        OutboundDelegateMsg::ApplicationMessage(msg) => msg,
        other @ OutboundDelegateMsg::RequestUserInput(_)
        | other @ OutboundDelegateMsg::ContextUpdated(_)
        | other @ OutboundDelegateMsg::GetContractRequest(_)
        | other @ OutboundDelegateMsg::PutContractRequest(_)
        | other @ OutboundDelegateMsg::UpdateContractRequest(_)
        | other @ OutboundDelegateMsg::SubscribeContractRequest(_)
        | other @ OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected ApplicationMessage, got {:?}", other)
        }
    };
    assert!(response_msg.processed);

    let response: OutboundAppMessage = bincode::deserialize(&response_msg.payload)?;
    match response {
        OutboundAppMessage::ContractState { contract_id, state } => {
            assert_eq!(contract_id, [42u8; 32]);
            assert_eq!(
                state, expected_state,
                "V2 delegate should read contract state via host functions"
            );
        }
        other @ OutboundAppMessage::ContractNotFound { .. }
        | other @ OutboundAppMessage::Success { .. }
        | other @ OutboundAppMessage::Failed { .. } => {
            panic!("V2 delegate returned {other:?} — expected ContractState");
        }
    }

    std::mem::drop(temp_dir);
    Ok(())
}

/// V2 delegate: contract not found returns error code.
#[tokio::test(flavor = "multi_thread")]
async fn test_v2_delegate_contract_not_found() -> Result<(), Box<dyn std::error::Error>> {
    use crate::contract::storages::Storage;
    use v2_contracts_messages::*;

    let temp_dir = get_temp_dir();
    let contracts_dir = temp_dir.path().join("contracts");
    let delegates_dir = temp_dir.path().join("delegates");
    let secrets_dir = temp_dir.path().join("secrets");

    let db = Storage::new(temp_dir.path()).await?;
    let contract_store = ContractStore::new(contracts_dir, 10_000, db.clone())?;
    let delegate_store = DelegateStore::new(delegates_dir, 10_000, db.clone())?;
    let secret_store = SecretsStore::new(secrets_dir, Default::default(), db.clone())?;

    let mut runtime = Runtime::build(contract_store, delegate_store, secret_store, false).unwrap();
    runtime.set_state_store_db(db);

    // Load the V2 delegate (no contract state stored — should get not-found)
    let delegate = {
        let bytes = super::super::tests::get_test_module(TEST_DELEGATE_V2_CONTRACTS)?;
        DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(Delegate::from((
            &bytes.into(),
            &vec![].into(),
        ))))
    };
    let _stored = runtime.delegate_store.store_delegate(delegate.clone());

    let key = XChaCha20Poly1305::generate_key(&mut OsRng);
    let cipher = XChaCha20Poly1305::new(&key);
    let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
    let _registered = runtime
        .secret_store
        .register_delegate(delegate.key().clone(), cipher, nonce);

    // Ask for a contract that doesn't exist
    let _app_id = ContractInstanceId::new([1u8; 32]);
    let command = InboundAppMessage::GetContractState {
        contract_id: [99u8; 32],
    };
    let payload = bincode::serialize(&command)?;
    let app_msg = ApplicationMessage::new(payload);

    let outbound = runtime.inbound_app_message(
        delegate.key(),
        &vec![].into(),
        None,
        None,
        vec![InboundDelegateMsg::ApplicationMessage(app_msg)],
    )?;

    assert_eq!(outbound.len(), 1);
    let response_msg = match &outbound[0] {
        OutboundDelegateMsg::ApplicationMessage(msg) => msg,
        other @ OutboundDelegateMsg::RequestUserInput(_)
        | other @ OutboundDelegateMsg::ContextUpdated(_)
        | other @ OutboundDelegateMsg::GetContractRequest(_)
        | other @ OutboundDelegateMsg::PutContractRequest(_)
        | other @ OutboundDelegateMsg::UpdateContractRequest(_)
        | other @ OutboundDelegateMsg::SubscribeContractRequest(_)
        | other @ OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected ApplicationMessage, got {:?}", other)
        }
    };

    let response: OutboundAppMessage = bincode::deserialize(&response_msg.payload)?;
    match response {
        OutboundAppMessage::ContractNotFound { error_code, .. } => {
            assert!(
                error_code < 0,
                "Expected negative error code for not-found, got {error_code}"
            );
        }
        other @ OutboundAppMessage::ContractState { .. }
        | other @ OutboundAppMessage::Success { .. }
        | other @ OutboundAppMessage::Failed { .. } => {
            panic!("Expected ContractNotFound for non-existent contract, got {other:?}");
        }
    }

    std::mem::drop(temp_dir);
    Ok(())
}

/// Helper: set up a V2 delegate runtime with a registered contract.
async fn setup_v2_runtime_with_contract(
    contract_id_byte: u8,
    initial_state: Option<&[u8]>,
) -> Result<
    (
        DelegateContainer,
        Runtime,
        ContractInstanceId,
        tempfile::TempDir,
    ),
    Box<dyn std::error::Error>,
> {
    use crate::contract::storages::Storage;
    use crate::wasm_runtime::StateStorage;

    let temp_dir = get_temp_dir();
    let contracts_dir = temp_dir.path().join("contracts");
    let delegates_dir = temp_dir.path().join("delegates");
    let secrets_dir = temp_dir.path().join("secrets");

    let db = Storage::new(temp_dir.path()).await?;
    let contract_store = ContractStore::new(contracts_dir, 10_000, db.clone())?;
    let delegate_store = DelegateStore::new(delegates_dir, 10_000, db.clone())?;
    let secret_store = SecretsStore::new(secrets_dir, Default::default(), db.clone())?;

    let mut runtime = Runtime::build(contract_store, delegate_store, secret_store, false).unwrap();
    runtime.set_state_store_db(db.clone());

    // Register the contract
    let contract_instance_id = ContractInstanceId::new([contract_id_byte; 32]);
    let contract_code = ContractCode::from(vec![contract_id_byte, 2, 3]);
    let contract_key = ContractKey::from_id_and_code(contract_instance_id, *contract_code.hash());
    runtime.contract_store.ensure_key_indexed(&contract_key)?;

    if let Some(state) = initial_state {
        db.store(contract_key, WrappedState::new(state.to_vec()))
            .await?;
    }

    // Load the V2 delegate
    let delegate = {
        let bytes = super::super::tests::get_test_module(TEST_DELEGATE_V2_CONTRACTS)?;
        DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(Delegate::from((
            &bytes.into(),
            &vec![].into(),
        ))))
    };
    let _stored = runtime.delegate_store.store_delegate(delegate.clone());

    let key = XChaCha20Poly1305::generate_key(&mut OsRng);
    let cipher = XChaCha20Poly1305::new(&key);
    let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
    let _registered = runtime
        .secret_store
        .register_delegate(delegate.key().clone(), cipher, nonce);

    Ok((delegate, runtime, contract_instance_id, temp_dir))
}

/// Helper: send a message to the V2 delegate and deserialize the response.
fn send_v2_message(
    runtime: &mut Runtime,
    delegate: &DelegateContainer,
    message: &v2_contracts_messages::InboundAppMessage,
) -> Result<v2_contracts_messages::OutboundAppMessage, Box<dyn std::error::Error>> {
    let _app_id = ContractInstanceId::new([1u8; 32]);
    let payload = bincode::serialize(message)?;
    let app_msg = ApplicationMessage::new(payload);

    let outbound = runtime.inbound_app_message(
        delegate.key(),
        &vec![].into(),
        None,
        None,
        vec![InboundDelegateMsg::ApplicationMessage(app_msg)],
    )?;

    assert_eq!(outbound.len(), 1, "Expected exactly one outbound message");
    let response_msg = match &outbound[0] {
        OutboundDelegateMsg::ApplicationMessage(msg) => msg,
        other @ OutboundDelegateMsg::RequestUserInput(_)
        | other @ OutboundDelegateMsg::ContextUpdated(_)
        | other @ OutboundDelegateMsg::GetContractRequest(_)
        | other @ OutboundDelegateMsg::PutContractRequest(_)
        | other @ OutboundDelegateMsg::UpdateContractRequest(_)
        | other @ OutboundDelegateMsg::SubscribeContractRequest(_)
        | other @ OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected ApplicationMessage, got {:?}", other)
        }
    };
    assert!(response_msg.processed);

    Ok(bincode::deserialize(&response_msg.payload)?)
}

/// V2 E2E: PUT state via delegate, then GET it back.
#[tokio::test(flavor = "multi_thread")]
async fn test_v2_delegate_put_then_get() -> Result<(), Box<dyn std::error::Error>> {
    use v2_contracts_messages::*;

    let (delegate, mut runtime, contract_instance_id, _temp_dir) =
        setup_v2_runtime_with_contract(50, None).await?;
    let cid: [u8; 32] = contract_instance_id.as_bytes().try_into().unwrap();

    // PUT state
    let put_response = send_v2_message(
        &mut runtime,
        &delegate,
        &InboundAppMessage::PutContractState {
            contract_id: cid,
            state: vec![100, 200, 150],
        },
    )?;
    match put_response {
        OutboundAppMessage::Success { contract_id } => {
            assert_eq!(contract_id, cid);
        }
        other @ OutboundAppMessage::ContractState { .. }
        | other @ OutboundAppMessage::ContractNotFound { .. }
        | other @ OutboundAppMessage::Failed { .. } => {
            panic!("Expected Success from PUT, got {:?}", other)
        }
    }

    // GET it back
    let get_response = send_v2_message(
        &mut runtime,
        &delegate,
        &InboundAppMessage::GetContractState { contract_id: cid },
    )?;
    match get_response {
        OutboundAppMessage::ContractState { contract_id, state } => {
            assert_eq!(contract_id, cid);
            assert_eq!(state, vec![100, 200, 150]);
        }
        other @ OutboundAppMessage::ContractNotFound { .. }
        | other @ OutboundAppMessage::Success { .. }
        | other @ OutboundAppMessage::Failed { .. } => {
            panic!("Expected ContractState from GET, got {:?}", other)
        }
    }

    Ok(())
}

/// V2 E2E: UPDATE existing state via delegate.
#[tokio::test(flavor = "multi_thread")]
async fn test_v2_delegate_update_existing_state() -> Result<(), Box<dyn std::error::Error>> {
    use v2_contracts_messages::*;

    let (delegate, mut runtime, contract_instance_id, _temp_dir) =
        setup_v2_runtime_with_contract(51, Some(&[1, 2, 3])).await?;
    let cid: [u8; 32] = contract_instance_id.as_bytes().try_into().unwrap();

    // UPDATE the existing state
    let update_response = send_v2_message(
        &mut runtime,
        &delegate,
        &InboundAppMessage::UpdateContractState {
            contract_id: cid,
            state: vec![7, 8, 9],
        },
    )?;
    match update_response {
        OutboundAppMessage::Success { contract_id } => {
            assert_eq!(contract_id, cid);
        }
        other @ OutboundAppMessage::ContractState { .. }
        | other @ OutboundAppMessage::ContractNotFound { .. }
        | other @ OutboundAppMessage::Failed { .. } => {
            panic!("Expected Success from UPDATE, got {:?}", other)
        }
    }

    // Verify via GET
    let get_response = send_v2_message(
        &mut runtime,
        &delegate,
        &InboundAppMessage::GetContractState { contract_id: cid },
    )?;
    match get_response {
        OutboundAppMessage::ContractState { state, .. } => {
            assert_eq!(state, vec![7, 8, 9]);
        }
        other @ OutboundAppMessage::ContractNotFound { .. }
        | other @ OutboundAppMessage::Success { .. }
        | other @ OutboundAppMessage::Failed { .. } => {
            panic!("Expected ContractState, got {:?}", other)
        }
    }

    Ok(())
}

/// V2 E2E: UPDATE non-existent state returns error.
#[tokio::test(flavor = "multi_thread")]
async fn test_v2_delegate_update_nonexistent_fails() -> Result<(), Box<dyn std::error::Error>> {
    use v2_contracts_messages::*;

    let (delegate, mut runtime, contract_instance_id, _temp_dir) =
        setup_v2_runtime_with_contract(52, None).await?;
    let cid: [u8; 32] = contract_instance_id.as_bytes().try_into().unwrap();

    let response = send_v2_message(
        &mut runtime,
        &delegate,
        &InboundAppMessage::UpdateContractState {
            contract_id: cid,
            state: vec![1, 2, 3],
        },
    )?;
    match response {
        OutboundAppMessage::Failed { error_code, .. } => {
            assert!(
                error_code < 0,
                "Expected negative error code, got {error_code}"
            );
        }
        other @ OutboundAppMessage::ContractState { .. }
        | other @ OutboundAppMessage::ContractNotFound { .. }
        | other @ OutboundAppMessage::Success { .. } => panic!(
            "Expected Failed from UPDATE on non-existent, got {:?}",
            other
        ),
    }

    Ok(())
}

/// V2 E2E: SUBSCRIBE to a known contract succeeds.
#[tokio::test(flavor = "multi_thread")]
async fn test_v2_delegate_subscribe_known() -> Result<(), Box<dyn std::error::Error>> {
    use v2_contracts_messages::*;

    let (delegate, mut runtime, contract_instance_id, _temp_dir) =
        setup_v2_runtime_with_contract(53, Some(&[1])).await?;
    let cid: [u8; 32] = contract_instance_id.as_bytes().try_into().unwrap();

    let response = send_v2_message(
        &mut runtime,
        &delegate,
        &InboundAppMessage::SubscribeContract { contract_id: cid },
    )?;
    match response {
        OutboundAppMessage::Success { contract_id } => {
            assert_eq!(contract_id, cid);
        }
        other @ OutboundAppMessage::ContractState { .. }
        | other @ OutboundAppMessage::ContractNotFound { .. }
        | other @ OutboundAppMessage::Failed { .. } => {
            panic!("Expected Success from SUBSCRIBE, got {:?}", other)
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_put_contract_request_response() -> Result<(), Box<dyn std::error::Error>> {
    use capabilities_messages::*;

    let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_CAPABILITIES).await?;

    let _app_id = ContractInstanceId::new([1u8; 32]);

    let code = ContractCode::from(vec![0u8; 10]);
    let params = Parameters::from(vec![]);
    let wrapped = WrappedContract::new(Arc::new(code), params);
    let contract = ContractContainer::Wasm(ContractWasmAPIVersion::V1(wrapped));
    let contract_key = contract.key();

    let command = DelegateCommand::PutContractState {
        contract,
        state: vec![10, 20, 30],
    };
    let payload = bincode::serialize(&command)?;
    let app_msg = ApplicationMessage::new(payload);

    let outbound = runtime.inbound_app_message(
        delegate.key(),
        &vec![].into(),
        None,
        None,
        vec![InboundDelegateMsg::ApplicationMessage(app_msg)],
    )?;

    assert_eq!(outbound.len(), 1, "Expected exactly one outbound message");
    let put_request = match &outbound[0] {
        OutboundDelegateMsg::PutContractRequest(req) => req.clone(),
        other @ OutboundDelegateMsg::ApplicationMessage(_)
        | other @ OutboundDelegateMsg::RequestUserInput(_)
        | other @ OutboundDelegateMsg::ContextUpdated(_)
        | other @ OutboundDelegateMsg::GetContractRequest(_)
        | other @ OutboundDelegateMsg::UpdateContractRequest(_)
        | other @ OutboundDelegateMsg::SubscribeContractRequest(_)
        | other @ OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected PutContractRequest, got {:?}", other)
        }
    };
    assert_eq!(put_request.contract.key(), contract_key);
    assert!(!put_request.processed);

    let response = InboundDelegateMsg::PutContractResponse(PutContractResponse {
        contract_id: *contract_key.id(),
        result: Ok(()),
        context: put_request.context.clone(),
    });

    let final_outbound =
        runtime.inbound_app_message(delegate.key(), &vec![].into(), None, None, vec![response])?;

    assert_eq!(
        final_outbound.len(),
        1,
        "Expected exactly one final message"
    );
    let final_msg = match &final_outbound[0] {
        OutboundDelegateMsg::ApplicationMessage(msg) => msg,
        other @ OutboundDelegateMsg::RequestUserInput(_)
        | other @ OutboundDelegateMsg::ContextUpdated(_)
        | other @ OutboundDelegateMsg::GetContractRequest(_)
        | other @ OutboundDelegateMsg::PutContractRequest(_)
        | other @ OutboundDelegateMsg::UpdateContractRequest(_)
        | other @ OutboundDelegateMsg::SubscribeContractRequest(_)
        | other @ OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected ApplicationMessage, got {:?}", other)
        }
    };
    assert!(final_msg.processed);

    let response: DelegateResponse = bincode::deserialize(&final_msg.payload)?;
    match response {
        DelegateResponse::ContractPutResult { success, error, .. } => {
            assert!(success);
            assert!(error.is_none());
        }
        other @ DelegateResponse::ContractState { .. }
        | other @ DelegateResponse::MultipleContractStates { .. }
        | other @ DelegateResponse::Echo { .. }
        | other @ DelegateResponse::ContractUpdateResult { .. }
        | other @ DelegateResponse::ContractSubscribeResult { .. }
        | other @ DelegateResponse::ContractNotificationReceived { .. }
        | other @ DelegateResponse::Error { .. } => {
            panic!("Expected ContractPutResult, got {:?}", other)
        }
    }

    std::mem::drop(temp_dir);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_update_contract_request_response() -> Result<(), Box<dyn std::error::Error>> {
    use capabilities_messages::*;

    let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_CAPABILITIES).await?;

    let contract_id = ContractInstanceId::new([42u8; 32]);
    let _app_id = ContractInstanceId::new([1u8; 32]);

    let command = DelegateCommand::UpdateContractState {
        contract_id,
        state: vec![10, 20, 30],
    };
    let payload = bincode::serialize(&command)?;
    let app_msg = ApplicationMessage::new(payload);

    let outbound = runtime.inbound_app_message(
        delegate.key(),
        &vec![].into(),
        None,
        None,
        vec![InboundDelegateMsg::ApplicationMessage(app_msg)],
    )?;

    assert_eq!(outbound.len(), 1, "Expected exactly one outbound message");
    let update_request = match &outbound[0] {
        OutboundDelegateMsg::UpdateContractRequest(req) => req.clone(),
        other @ OutboundDelegateMsg::ApplicationMessage(_)
        | other @ OutboundDelegateMsg::RequestUserInput(_)
        | other @ OutboundDelegateMsg::ContextUpdated(_)
        | other @ OutboundDelegateMsg::GetContractRequest(_)
        | other @ OutboundDelegateMsg::PutContractRequest(_)
        | other @ OutboundDelegateMsg::SubscribeContractRequest(_)
        | other @ OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected UpdateContractRequest, got {:?}", other)
        }
    };
    assert_eq!(update_request.contract_id, contract_id);
    assert!(!update_request.processed);

    let response = InboundDelegateMsg::UpdateContractResponse(UpdateContractResponse {
        contract_id,
        result: Ok(()),
        context: update_request.context.clone(),
    });

    let final_outbound =
        runtime.inbound_app_message(delegate.key(), &vec![].into(), None, None, vec![response])?;

    assert_eq!(
        final_outbound.len(),
        1,
        "Expected exactly one final message"
    );
    let final_msg = match &final_outbound[0] {
        OutboundDelegateMsg::ApplicationMessage(msg) => msg,
        other @ OutboundDelegateMsg::RequestUserInput(_)
        | other @ OutboundDelegateMsg::ContextUpdated(_)
        | other @ OutboundDelegateMsg::GetContractRequest(_)
        | other @ OutboundDelegateMsg::PutContractRequest(_)
        | other @ OutboundDelegateMsg::UpdateContractRequest(_)
        | other @ OutboundDelegateMsg::SubscribeContractRequest(_)
        | other @ OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected ApplicationMessage, got {:?}", other)
        }
    };
    assert!(final_msg.processed);

    let response: DelegateResponse = bincode::deserialize(&final_msg.payload)?;
    match response {
        DelegateResponse::ContractUpdateResult {
            contract_id: id,
            success,
            error,
        } => {
            assert_eq!(id, contract_id);
            assert!(success);
            assert!(error.is_none());
        }
        other @ DelegateResponse::ContractState { .. }
        | other @ DelegateResponse::MultipleContractStates { .. }
        | other @ DelegateResponse::Echo { .. }
        | other @ DelegateResponse::ContractPutResult { .. }
        | other @ DelegateResponse::ContractSubscribeResult { .. }
        | other @ DelegateResponse::ContractNotificationReceived { .. }
        | other @ DelegateResponse::Error { .. } => {
            panic!("Expected ContractUpdateResult, got {:?}", other)
        }
    }

    std::mem::drop(temp_dir);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_subscribe_contract_request_response() -> Result<(), Box<dyn std::error::Error>> {
    use capabilities_messages::*;

    let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_CAPABILITIES).await?;

    let contract_id = ContractInstanceId::new([42u8; 32]);
    let _app_id = ContractInstanceId::new([1u8; 32]);

    let command = DelegateCommand::SubscribeContract { contract_id };
    let payload = bincode::serialize(&command)?;
    let app_msg = ApplicationMessage::new(payload);

    let outbound = runtime.inbound_app_message(
        delegate.key(),
        &vec![].into(),
        None,
        None,
        vec![InboundDelegateMsg::ApplicationMessage(app_msg)],
    )?;

    assert_eq!(outbound.len(), 1, "Expected exactly one outbound message");
    let subscribe_request = match &outbound[0] {
        OutboundDelegateMsg::SubscribeContractRequest(req) => req.clone(),
        other @ OutboundDelegateMsg::ApplicationMessage(_)
        | other @ OutboundDelegateMsg::RequestUserInput(_)
        | other @ OutboundDelegateMsg::ContextUpdated(_)
        | other @ OutboundDelegateMsg::GetContractRequest(_)
        | other @ OutboundDelegateMsg::PutContractRequest(_)
        | other @ OutboundDelegateMsg::UpdateContractRequest(_)
        | other @ OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected SubscribeContractRequest, got {:?}", other)
        }
    };
    assert_eq!(subscribe_request.contract_id, contract_id);
    assert!(!subscribe_request.processed);

    let response = InboundDelegateMsg::SubscribeContractResponse(SubscribeContractResponse {
        contract_id,
        result: Err("not yet implemented".to_string()),
        context: subscribe_request.context.clone(),
    });

    let final_outbound =
        runtime.inbound_app_message(delegate.key(), &vec![].into(), None, None, vec![response])?;

    assert_eq!(
        final_outbound.len(),
        1,
        "Expected exactly one final message"
    );
    let final_msg = match &final_outbound[0] {
        OutboundDelegateMsg::ApplicationMessage(msg) => msg,
        other @ OutboundDelegateMsg::RequestUserInput(_)
        | other @ OutboundDelegateMsg::ContextUpdated(_)
        | other @ OutboundDelegateMsg::GetContractRequest(_)
        | other @ OutboundDelegateMsg::PutContractRequest(_)
        | other @ OutboundDelegateMsg::UpdateContractRequest(_)
        | other @ OutboundDelegateMsg::SubscribeContractRequest(_)
        | other @ OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected ApplicationMessage, got {:?}", other)
        }
    };
    assert!(final_msg.processed);

    let response: DelegateResponse = bincode::deserialize(&final_msg.payload)?;
    match response {
        DelegateResponse::ContractSubscribeResult {
            contract_id: id,
            success,
            error,
        } => {
            assert_eq!(id, contract_id);
            assert!(!success);
            assert!(error.is_some());
        }
        other @ DelegateResponse::ContractState { .. }
        | other @ DelegateResponse::MultipleContractStates { .. }
        | other @ DelegateResponse::Echo { .. }
        | other @ DelegateResponse::ContractPutResult { .. }
        | other @ DelegateResponse::ContractUpdateResult { .. }
        | other @ DelegateResponse::ContractNotificationReceived { .. }
        | other @ DelegateResponse::Error { .. } => {
            panic!("Expected ContractSubscribeResult, got {:?}", other)
        }
    }

    std::mem::drop(temp_dir);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_contract_notification_delivered() -> Result<(), Box<dyn std::error::Error>> {
    use capabilities_messages::*;

    let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_CAPABILITIES).await?;

    let contract_id = ContractInstanceId::new([42u8; 32]);
    let new_state = vec![10, 20, 30, 40];

    let notification = InboundDelegateMsg::ContractNotification(ContractNotification {
        contract_id,
        new_state: WrappedState::new(new_state.clone()),
        context: DelegateContext::default(),
    });

    let outbound = runtime.inbound_app_message(
        delegate.key(),
        &vec![].into(),
        None,
        None,
        vec![notification],
    )?;

    assert_eq!(outbound.len(), 1, "Expected exactly one outbound message");
    let msg = match &outbound[0] {
        OutboundDelegateMsg::ApplicationMessage(msg) => msg,
        other @ OutboundDelegateMsg::RequestUserInput(_)
        | other @ OutboundDelegateMsg::ContextUpdated(_)
        | other @ OutboundDelegateMsg::GetContractRequest(_)
        | other @ OutboundDelegateMsg::PutContractRequest(_)
        | other @ OutboundDelegateMsg::UpdateContractRequest(_)
        | other @ OutboundDelegateMsg::SubscribeContractRequest(_)
        | other @ OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected ApplicationMessage, got {:?}", other)
        }
    };
    assert!(msg.processed);

    let response: DelegateResponse = bincode::deserialize(&msg.payload)?;
    match response {
        DelegateResponse::ContractNotificationReceived {
            contract_id: id,
            new_state: state,
        } => {
            assert_eq!(id, contract_id);
            assert_eq!(state, new_state);
        }
        other @ DelegateResponse::ContractState { .. }
        | other @ DelegateResponse::MultipleContractStates { .. }
        | other @ DelegateResponse::Echo { .. }
        | other @ DelegateResponse::ContractPutResult { .. }
        | other @ DelegateResponse::ContractUpdateResult { .. }
        | other @ DelegateResponse::ContractSubscribeResult { .. }
        | other @ DelegateResponse::Error { .. } => {
            panic!("Expected ContractNotificationReceived, got {:?}", other)
        }
    }

    std::mem::drop(temp_dir);
    Ok(())
}

/// End-to-end integration test: subscribe → registry populated → notification delivered.
///
/// Verifies the full pipeline:
/// 1. Delegate subscribes to a contract via SubscribeContractRequest
/// 2. Subscription is registered in DELEGATE_SUBSCRIPTIONS
/// 3. ContractNotification is delivered to the delegate
/// 4. Delegate responds with ContractNotificationReceived
/// 5. Cleanup: unregister delegate removes subscription entries
#[tokio::test(flavor = "multi_thread")]
async fn test_subscribe_then_notify_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    use crate::contract::storages::Storage;
    use crate::wasm_runtime::StateStorage;
    use capabilities_messages::*;

    let temp_dir = get_temp_dir();
    let contracts_dir = temp_dir.path().join("contracts");
    let delegates_dir = temp_dir.path().join("delegates");
    let secrets_dir = temp_dir.path().join("secrets");

    let db = Storage::new(temp_dir.path()).await?;
    let contract_store = ContractStore::new(contracts_dir, 10_000, db.clone())?;
    let delegate_store = DelegateStore::new(delegates_dir, 10_000, db.clone())?;
    let secret_store = SecretsStore::new(secrets_dir, Default::default(), db.clone())?;

    let mut runtime = Runtime::build(contract_store, delegate_store, secret_store, false).unwrap();
    runtime.set_state_store_db(db.clone());

    // Set up a contract so subscribe validation passes
    let contract_instance_id = ContractInstanceId::new([42u8; 32]);
    let contract_code = ContractCode::from(vec![42, 2, 3]);
    let contract_key = ContractKey::from_id_and_code(contract_instance_id, *contract_code.hash());
    runtime.contract_store.ensure_key_indexed(&contract_key)?;
    db.store(contract_key, WrappedState::new(vec![1, 2, 3]))
        .await?;

    // Load the delegate
    let delegate = {
        let bytes = super::super::tests::get_test_module(TEST_DELEGATE_CAPABILITIES)?;
        DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(Delegate::from((
            &bytes.into(),
            &vec![].into(),
        ))))
    };
    let _stored = runtime.delegate_store.store_delegate(delegate.clone());

    let key = XChaCha20Poly1305::generate_key(&mut OsRng);
    let cipher = XChaCha20Poly1305::new(&key);
    let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
    let _registered = runtime
        .secret_store
        .register_delegate(delegate.key().clone(), cipher, nonce);

    let delegate_key = delegate.key().clone();
    let _app_id = ContractInstanceId::new([1u8; 32]);

    // --- Step 1: Delegate subscribes to the contract ---
    let subscribe_cmd = DelegateCommand::SubscribeContract {
        contract_id: contract_instance_id,
    };
    let payload = bincode::serialize(&subscribe_cmd)?;
    let app_msg = ApplicationMessage::new(payload);

    let outbound = runtime.inbound_app_message(
        &delegate_key,
        &vec![].into(),
        None,
        None,
        vec![InboundDelegateMsg::ApplicationMessage(app_msg)],
    )?;

    // Should emit SubscribeContractRequest
    assert_eq!(outbound.len(), 1);
    let subscribe_req = match &outbound[0] {
        OutboundDelegateMsg::SubscribeContractRequest(req) => req.clone(),
        other @ OutboundDelegateMsg::ApplicationMessage(_)
        | other @ OutboundDelegateMsg::RequestUserInput(_)
        | other @ OutboundDelegateMsg::ContextUpdated(_)
        | other @ OutboundDelegateMsg::GetContractRequest(_)
        | other @ OutboundDelegateMsg::PutContractRequest(_)
        | other @ OutboundDelegateMsg::UpdateContractRequest(_)
        | other @ OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected SubscribeContractRequest, got {:?}", other)
        }
    };
    assert_eq!(subscribe_req.contract_id, contract_instance_id);

    // Simulate the V1 subscribe handler path (contract.rs:387-405):
    // validate contract existence via lookup, then register if found.
    let subscribe_result = if runtime
        .contract_store
        .code_hash_from_id(&subscribe_req.contract_id)
        .is_some()
    {
        crate::wasm_runtime::DELEGATE_SUBSCRIPTIONS
            .entry(subscribe_req.contract_id)
            .or_default()
            .insert(delegate_key.clone());
        Ok(())
    } else {
        Err("Contract not found".to_string())
    };
    assert!(
        subscribe_result.is_ok(),
        "Subscribe should succeed for known contract"
    );

    // Feed the SubscribeContractResponse back to the delegate
    let subscribe_response =
        InboundDelegateMsg::SubscribeContractResponse(SubscribeContractResponse {
            contract_id: subscribe_req.contract_id,
            result: subscribe_result,
            context: subscribe_req.context.clone(),
        });
    let outbound = runtime.inbound_app_message(
        &delegate_key,
        &vec![].into(),
        None,
        None,
        vec![subscribe_response],
    )?;
    // Delegate should emit a ContractSubscribeResult ApplicationMessage
    assert_eq!(outbound.len(), 1);
    match &outbound[0] {
        OutboundDelegateMsg::ApplicationMessage(msg) => {
            let resp: DelegateResponse = bincode::deserialize(&msg.payload)?;
            match resp {
                DelegateResponse::ContractSubscribeResult { success, .. } => {
                    assert!(success, "Subscribe response should indicate success");
                }
                other @ DelegateResponse::ContractState { .. }
                | other @ DelegateResponse::MultipleContractStates { .. }
                | other @ DelegateResponse::Echo { .. }
                | other @ DelegateResponse::ContractPutResult { .. }
                | other @ DelegateResponse::ContractUpdateResult { .. }
                | other @ DelegateResponse::ContractNotificationReceived { .. }
                | other @ DelegateResponse::Error { .. } => {
                    panic!("Expected ContractSubscribeResult, got {:?}", other)
                }
            }
        }
        other @ OutboundDelegateMsg::RequestUserInput(_)
        | other @ OutboundDelegateMsg::ContextUpdated(_)
        | other @ OutboundDelegateMsg::GetContractRequest(_)
        | other @ OutboundDelegateMsg::PutContractRequest(_)
        | other @ OutboundDelegateMsg::UpdateContractRequest(_)
        | other @ OutboundDelegateMsg::SubscribeContractRequest(_)
        | other @ OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected ApplicationMessage, got {:?}", other)
        }
    }

    // --- Step 2: Verify registry is populated ---
    {
        let entry = crate::wasm_runtime::DELEGATE_SUBSCRIPTIONS.get(&contract_instance_id);
        let subscribers = entry.as_ref().unwrap();
        assert!(
            subscribers.contains(&delegate_key),
            "Delegate should be registered as subscriber"
        );
    }

    // Also verify that subscribing to an UNKNOWN contract fails validation
    let unknown_id = ContractInstanceId::new([99u8; 32]);
    let has_code = runtime
        .contract_store
        .code_hash_from_id(&unknown_id)
        .is_some();
    assert!(!has_code, "Unknown contract should not be in store");

    // --- Step 3: Deliver ContractNotification ---
    let updated_state = vec![10, 20, 30, 40, 50];
    let notification = InboundDelegateMsg::ContractNotification(ContractNotification {
        contract_id: contract_instance_id,
        new_state: WrappedState::new(updated_state.clone()),
        context: DelegateContext::default(),
    });

    let outbound = runtime.inbound_app_message(
        &delegate_key,
        &vec![].into(),
        None,
        None,
        vec![notification],
    )?;

    // --- Step 4: Verify delegate responds correctly ---
    assert_eq!(outbound.len(), 1, "Expected one outbound from notification");
    let msg = match &outbound[0] {
        OutboundDelegateMsg::ApplicationMessage(msg) => msg,
        other @ OutboundDelegateMsg::RequestUserInput(_)
        | other @ OutboundDelegateMsg::ContextUpdated(_)
        | other @ OutboundDelegateMsg::GetContractRequest(_)
        | other @ OutboundDelegateMsg::PutContractRequest(_)
        | other @ OutboundDelegateMsg::UpdateContractRequest(_)
        | other @ OutboundDelegateMsg::SubscribeContractRequest(_)
        | other @ OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected ApplicationMessage, got {:?}", other)
        }
    };
    assert!(msg.processed);

    let response: DelegateResponse = bincode::deserialize(&msg.payload)?;
    match response {
        DelegateResponse::ContractNotificationReceived {
            contract_id: id,
            new_state: state,
        } => {
            assert_eq!(id, contract_instance_id);
            assert_eq!(state, updated_state);
        }
        other @ DelegateResponse::ContractState { .. }
        | other @ DelegateResponse::MultipleContractStates { .. }
        | other @ DelegateResponse::Echo { .. }
        | other @ DelegateResponse::ContractPutResult { .. }
        | other @ DelegateResponse::ContractUpdateResult { .. }
        | other @ DelegateResponse::ContractSubscribeResult { .. }
        | other @ DelegateResponse::Error { .. } => {
            panic!("Expected ContractNotificationReceived, got {:?}", other)
        }
    }

    // --- Step 5: Cleanup on delegate unregister ---
    crate::wasm_runtime::DELEGATE_SUBSCRIPTIONS.retain(|_, subscribers| {
        subscribers.remove(&delegate_key);
        !subscribers.is_empty()
    });

    // Verify cleanup
    let entry = crate::wasm_runtime::DELEGATE_SUBSCRIPTIONS.get(&contract_instance_id);
    assert!(
        entry.is_none() || entry.as_ref().unwrap().is_empty(),
        "Subscription should be cleaned up after delegate unregister"
    );

    std::mem::drop(temp_dir);
    Ok(())
}

/// Regression test for #3275: a notification-driven delegate `ApplicationMessage`
/// is routed to the app registered with that delegate, instead of being dropped.
///
/// Drives the real production `Runtime`: an app subscribes a delegate to a
/// contract, the contract's state changes, the delegate emits an
/// `ApplicationMessage` from the `ContractNotification` callback, and this test
/// registers the app's notification channel in `delegate_app_registry` and
/// routes via `route_to_apps` — the exact helper `handle_delegate_notification`
/// invokes in production. Before the fix there was no registry and no routing
/// call; the message was logged-and-dropped, so the app channel stayed empty.
#[tokio::test(flavor = "multi_thread")]
#[serial_test::serial]
// Test asserts on specific outbound/response variants; the `other => panic!`
// arms deliberately catch every non-matching (and future non_exhaustive) case.
#[allow(clippy::wildcard_enum_match_arm)]
async fn test_notification_application_message_routed_to_registered_app()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::client_events::{ClientId, HostResult};
    use crate::contract::delegate_app_registry;
    use crate::wasm_runtime::StateStorage;
    use capabilities_messages::*;
    use freenet_stdlib::client_api::HostResponse;

    let temp_dir = get_temp_dir();
    let contracts_dir = temp_dir.path().join("contracts");
    let delegates_dir = temp_dir.path().join("delegates");
    let secrets_dir = temp_dir.path().join("secrets");

    let db = crate::contract::storages::Storage::new(temp_dir.path()).await?;
    let contract_store = ContractStore::new(contracts_dir, 10_000, db.clone())?;
    let delegate_store = DelegateStore::new(delegates_dir, 10_000, db.clone())?;
    let secret_store = SecretsStore::new(secrets_dir, Default::default(), db.clone())?;

    let mut runtime = Runtime::build(contract_store, delegate_store, secret_store, false).unwrap();
    runtime.set_state_store_db(db.clone());

    // Contract the delegate will subscribe to.
    let contract_instance_id = ContractInstanceId::new([7u8; 32]);
    let contract_code = ContractCode::from(vec![7, 7, 7]);
    let contract_key = ContractKey::from_id_and_code(contract_instance_id, *contract_code.hash());
    runtime.contract_store.ensure_key_indexed(&contract_key)?;
    db.store(contract_key, WrappedState::new(vec![1, 2, 3]))
        .await?;

    // Load the capabilities delegate.
    let delegate = {
        let bytes = super::super::tests::get_test_module(TEST_DELEGATE_CAPABILITIES)?;
        DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(Delegate::from((
            &bytes.into(),
            &vec![].into(),
        ))))
    };
    let _stored = runtime.delegate_store.store_delegate(delegate.clone());
    let key = XChaCha20Poly1305::generate_key(&mut OsRng);
    let cipher = XChaCha20Poly1305::new(&key);
    let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
    let _registered = runtime
        .secret_store
        .register_delegate(delegate.key().clone(), cipher, nonce);
    let delegate_key = delegate.key().clone();

    // The delegate subscribes to the contract.
    let subscribe_cmd = DelegateCommand::SubscribeContract {
        contract_id: contract_instance_id,
    };
    let outbound = runtime.inbound_app_message(
        &delegate_key,
        &vec![].into(),
        None,
        None,
        vec![InboundDelegateMsg::ApplicationMessage(
            ApplicationMessage::new(bincode::serialize(&subscribe_cmd)?),
        )],
    )?;
    let subscribe_req = match &outbound[0] {
        OutboundDelegateMsg::SubscribeContractRequest(req) => req.clone(),
        other => panic!("Expected SubscribeContractRequest, got {other:?}"),
    };
    crate::wasm_runtime::DELEGATE_SUBSCRIPTIONS
        .entry(subscribe_req.contract_id)
        .or_default()
        .insert(delegate_key.clone());
    // Feed the subscribe response back so the delegate finishes subscribing.
    let _ = runtime.inbound_app_message(
        &delegate_key,
        &vec![].into(),
        None,
        None,
        vec![InboundDelegateMsg::SubscribeContractResponse(
            SubscribeContractResponse {
                contract_id: subscribe_req.contract_id,
                result: Ok(()),
                context: subscribe_req.context.clone(),
            },
        )],
    )?;

    // The app registers its notification channel with the delegate (this is what
    // client_events.rs does for an ApplicationMessages request carrying a
    // notification channel).
    let app_client = ClientId::FIRST;
    let (app_tx, mut app_rx) = tokio::sync::mpsc::channel::<HostResult>(8);
    assert!(
        delegate_app_registry::register_app(&delegate_key, app_client, app_tx),
        "app registration must succeed"
    );

    // Contract state changes → the delegate is notified and emits an
    // ApplicationMessage (ContractNotificationReceived) meant for the app.
    let updated_state = vec![9, 8, 7, 6];
    let outbound = runtime.inbound_app_message(
        &delegate_key,
        &vec![].into(),
        None,
        None,
        vec![InboundDelegateMsg::ContractNotification(
            ContractNotification {
                contract_id: contract_instance_id,
                new_state: WrappedState::new(updated_state.clone()),
                context: DelegateContext::default(),
            },
        )],
    )?;
    let app_msg = match &outbound[0] {
        OutboundDelegateMsg::ApplicationMessage(msg) => msg.clone(),
        other => panic!("Expected ApplicationMessage, got {other:?}"),
    };

    // Production routing: fan the ApplicationMessage out to registered apps,
    // exactly as handle_delegate_notification does.
    let response: HostResult = Ok(HostResponse::DelegateResponse {
        key: delegate_key.clone(),
        values: vec![OutboundDelegateMsg::ApplicationMessage(app_msg)],
    });
    let delivered = delegate_app_registry::route_to_apps(&delegate_key, response);
    assert_eq!(delivered, 1, "message must reach the one registered app");

    // The registered app receives the delegate's notification-driven reply.
    let received = app_rx
        .try_recv()
        .expect("registered app must receive the ApplicationMessage");
    match received {
        Ok(HostResponse::DelegateResponse { key, values }) => {
            assert_eq!(key, delegate_key);
            assert_eq!(values.len(), 1);
            match &values[0] {
                OutboundDelegateMsg::ApplicationMessage(msg) => {
                    let resp: DelegateResponse = bincode::deserialize(&msg.payload)?;
                    match resp {
                        DelegateResponse::ContractNotificationReceived {
                            contract_id,
                            new_state,
                        } => {
                            assert_eq!(contract_id, contract_instance_id);
                            assert_eq!(new_state, updated_state);
                        }
                        other => panic!("Expected ContractNotificationReceived, got {other:?}"),
                    }
                }
                other => panic!("Expected ApplicationMessage, got {other:?}"),
            }
        }
        other => panic!("Expected DelegateResponse, got {other:?}"),
    }

    // Clean up: disconnecting the app removes its registration.
    delegate_app_registry::remove_client(app_client);
    assert_eq!(
        delegate_app_registry::route_to_apps(
            &delegate_key,
            Ok(HostResponse::DelegateResponse {
                key: delegate_key.clone(),
                values: vec![],
            }),
        ),
        0,
        "after disconnect no app should remain registered"
    );

    crate::wasm_runtime::DELEGATE_SUBSCRIPTIONS.retain(|_, subs| {
        subs.remove(&delegate_key);
        !subs.is_empty()
    });
    std::mem::drop(temp_dir);
    Ok(())
}

/// Test: removing a contract cleans up DELEGATE_SUBSCRIPTIONS.
#[tokio::test(flavor = "multi_thread")]
async fn test_contract_removal_cleans_subscriptions() -> Result<(), Box<dyn std::error::Error>> {
    use crate::contract::storages::Storage;

    let temp_dir = get_temp_dir();
    let contracts_dir = temp_dir.path().join("contracts");
    let delegates_dir = temp_dir.path().join("delegates");
    let secrets_dir = temp_dir.path().join("secrets");

    let db = Storage::new(temp_dir.path()).await?;
    let contract_store = ContractStore::new(contracts_dir, 10_000, db.clone())?;
    let delegate_store = DelegateStore::new(delegates_dir, 10_000, db.clone())?;
    let secret_store = SecretsStore::new(secrets_dir, Default::default(), db.clone())?;

    let mut runtime = Runtime::build(contract_store, delegate_store, secret_store, false).unwrap();
    runtime.set_state_store_db(db.clone());

    // Create and store a contract
    let contract_instance_id = ContractInstanceId::new([99u8; 32]);
    let contract_code = ContractCode::from(vec![99, 2, 3]);
    let contract_key = ContractKey::from_id_and_code(contract_instance_id, *contract_code.hash());
    // Store the WASM file so remove_contract can delete it
    let wasm_path = runtime.contract_store.get_contract_path(&contract_key)?;
    std::fs::create_dir_all(wasm_path.parent().unwrap())?;
    std::fs::write(&wasm_path, [0u8; 10])?;
    runtime.contract_store.ensure_key_indexed(&contract_key)?;

    // Simulate delegate subscriptions
    let delegate_key_a = DelegateKey::new([1u8; 32], CodeHash::new([10u8; 32]));
    let delegate_key_b = DelegateKey::new([2u8; 32], CodeHash::new([20u8; 32]));
    {
        let mut entry = crate::wasm_runtime::DELEGATE_SUBSCRIPTIONS
            .entry(contract_instance_id)
            .or_default();
        entry.insert(delegate_key_a);
        entry.insert(delegate_key_b);
    }

    // Verify subscriptions exist
    assert!(
        crate::wasm_runtime::DELEGATE_SUBSCRIPTIONS
            .get(&contract_instance_id)
            .is_some()
    );

    // Remove the contract — should clean up subscriptions
    runtime.contract_store.remove_contract(&contract_key)?;

    // Verify subscriptions are cleaned up
    assert!(
        crate::wasm_runtime::DELEGATE_SUBSCRIPTIONS
            .get(&contract_instance_id)
            .is_none(),
        "DELEGATE_SUBSCRIPTIONS should be cleaned up when contract is removed"
    );

    std::mem::drop(temp_dir);
    Ok(())
}

// --- Delegate-to-delegate messaging tests ---

const TEST_DELEGATE_MESSAGING: &str = "test_delegate_messaging";

mod messaging_messages {
    use super::*;

    #[derive(Debug, Serialize, Deserialize)]
    pub enum InboundAppMessage {
        SendToDelegate {
            target_key_bytes: Vec<u8>,
            target_code_hash: Vec<u8>,
            payload: Vec<u8>,
        },
        Ping {
            data: Vec<u8>,
        },
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub enum OutboundAppMessage {
        MessageSent,
        DelegateMessageReceived {
            sender_key_bytes: Vec<u8>,
            payload: Vec<u8>,
            /// Mirror of the same field in the WASM-side
            /// `OutboundAppMessage`; populated when the runtime delivered
            /// this message with `MessageOrigin::Delegate(k)` (#3860).
            origin_delegate_key_bytes: Option<Vec<u8>>,
        },
        PingResponse {
            data: Vec<u8>,
        },
    }
}

async fn setup_runtime_with_params(
    name: &str,
    params: Vec<u8>,
) -> Result<(DelegateContainer, Runtime, tempfile::TempDir), Box<dyn std::error::Error>> {
    use crate::contract::storages::Storage;
    let temp_dir = get_temp_dir();
    let contracts_dir = temp_dir.path().join("contracts");
    let delegates_dir = temp_dir.path().join("delegates");
    let secrets_dir = temp_dir.path().join("secrets");

    let db = Storage::new(temp_dir.path()).await?;
    let contract_store = ContractStore::new(contracts_dir, 10_000, db.clone())?;
    let delegate_store = DelegateStore::new(delegates_dir, 10_000, db.clone())?;
    let secret_store = SecretsStore::new(secrets_dir, Default::default(), db)?;

    let mut runtime = Runtime::build(contract_store, delegate_store, secret_store, false).unwrap();

    let delegate = {
        let bytes = super::super::tests::get_test_module(name)?;
        DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(Delegate::from((
            &bytes.into(),
            &params.into(),
        ))))
    };
    let _stored = runtime.delegate_store.store_delegate(delegate.clone());

    let key = XChaCha20Poly1305::generate_key(&mut OsRng);
    let cipher = XChaCha20Poly1305::new(&key);
    let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
    let _registered = runtime
        .secret_store
        .register_delegate(delegate.key().clone(), cipher, nonce);

    Ok((delegate, runtime, temp_dir))
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delegate_emits_send_delegate_message() -> Result<(), Box<dyn std::error::Error>> {
    use messaging_messages::*;

    let (delegate_a, mut runtime, _temp_dir) =
        setup_runtime_with_params(TEST_DELEGATE_MESSAGING, vec![1]).await?;
    let key_a = delegate_a.key().clone();

    // Create a fake target delegate key B
    let target_key_bytes = vec![42u8; 32];
    let target_code_hash = vec![99u8; 32];

    let _app_id = ContractInstanceId::new([1u8; 32]);
    let payload = bincode::serialize(&InboundAppMessage::SendToDelegate {
        target_key_bytes: target_key_bytes.clone(),
        target_code_hash: target_code_hash.clone(),
        payload: b"hello".to_vec(),
    })?;
    let app_msg = ApplicationMessage::new(payload);

    let outbound = runtime.inbound_app_message(
        &key_a,
        &vec![1u8].into(),
        None,
        None,
        vec![InboundDelegateMsg::ApplicationMessage(app_msg)],
    )?;

    // Should have SendDelegateMessage and ApplicationMessage(MessageSent)
    assert!(
        !outbound.is_empty(),
        "Expected at least 1 outbound message, got {}",
        outbound.len()
    );

    let send_msg = outbound
        .iter()
        .find_map(|m| match m {
            OutboundDelegateMsg::SendDelegateMessage(msg) => Some(msg),
            OutboundDelegateMsg::ApplicationMessage(_)
            | OutboundDelegateMsg::RequestUserInput(_)
            | OutboundDelegateMsg::ContextUpdated(_)
            | OutboundDelegateMsg::GetContractRequest(_)
            | OutboundDelegateMsg::PutContractRequest(_)
            | OutboundDelegateMsg::UpdateContractRequest(_)
            | OutboundDelegateMsg::SubscribeContractRequest(_) => None,
        })
        .expect("Expected SendDelegateMessage in outbound");

    // Verify target matches what we sent
    let mut expected_key = [0u8; 32];
    expected_key.copy_from_slice(&target_key_bytes);
    let mut expected_hash = [0u8; 32];
    expected_hash.copy_from_slice(&target_code_hash);
    assert_eq!(*send_msg.target, expected_key);
    assert_eq!(send_msg.target.code_hash(), &CodeHash::new(expected_hash));

    // Verify sender attestation: runtime overwrites sender with actual delegate key
    assert_eq!(
        send_msg.sender, key_a,
        "Sender should be attested as delegate A"
    );

    // Verify payload
    assert_eq!(send_msg.payload, b"hello");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delegate_receives_delegate_message() -> Result<(), Box<dyn std::error::Error>> {
    use messaging_messages::*;

    let (delegate_b, mut runtime, _temp_dir) =
        setup_runtime_with_params(TEST_DELEGATE_MESSAGING, vec![2]).await?;
    let key_b = delegate_b.key().clone();

    // Create a fake sender key A
    let sender_key = DelegateKey::new([11u8; 32], CodeHash::new([22u8; 32]));

    // Deliver a DelegateMessage to B
    let delegate_msg = DelegateMessage::new(key_b.clone(), sender_key.clone(), b"hello".to_vec());

    let outbound = runtime.inbound_app_message(
        &key_b,
        &vec![2u8].into(),
        None,
        None,
        vec![InboundDelegateMsg::DelegateMessage(delegate_msg)],
    )?;

    assert_eq!(outbound.len(), 1, "Expected 1 outbound message");

    let app_msg = match &outbound[0] {
        OutboundDelegateMsg::ApplicationMessage(msg) => msg,
        OutboundDelegateMsg::RequestUserInput(_)
        | OutboundDelegateMsg::ContextUpdated(_)
        | OutboundDelegateMsg::GetContractRequest(_)
        | OutboundDelegateMsg::PutContractRequest(_)
        | OutboundDelegateMsg::UpdateContractRequest(_)
        | OutboundDelegateMsg::SubscribeContractRequest(_)
        | OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!("Expected ApplicationMessage, got {:?}", &outbound[0])
        }
    };

    let response: OutboundAppMessage = bincode::deserialize(&app_msg.payload)?;
    match response {
        OutboundAppMessage::DelegateMessageReceived {
            sender_key_bytes,
            payload,
            origin_delegate_key_bytes,
        } => {
            assert_eq!(sender_key_bytes, sender_key.bytes());
            assert_eq!(payload, b"hello");
            assert!(
                origin_delegate_key_bytes.is_none(),
                "origin was None, so receiver should see no Delegate origin"
            );
        }
        OutboundAppMessage::MessageSent | OutboundAppMessage::PingResponse { .. } => {
            panic!("Expected DelegateMessageReceived, got {:?}", response)
        }
    }

    Ok(())
}

/// Regression test for issue #3860: when the runtime delivers an inbound
/// `DelegateMessage` with `Some(MessageOrigin::Delegate(caller_key))`, the
/// receiving delegate's `process()` MUST see exactly that origin in its
/// `origin` parameter. Previously the inter-delegate dispatch path passed
/// `None`, leaving the receiver unable to authorize on caller identity.
#[tokio::test(flavor = "multi_thread")]
async fn test_inbound_app_message_propagates_delegate_origin()
-> Result<(), Box<dyn std::error::Error>> {
    use messaging_messages::*;

    let (delegate_b, mut runtime, _temp_dir) =
        setup_runtime_with_params(TEST_DELEGATE_MESSAGING, vec![2]).await?;
    let key_b = delegate_b.key().clone();

    // Synthetic caller delegate A — its key is what we expect the
    // receiver to observe via MessageOrigin::Delegate.
    let caller_a = DelegateKey::new([0xA1u8; 32], CodeHash::new([0xA2u8; 32]));

    // Build an inbound DelegateMessage for B, with `sender` distinct from
    // `caller_a` so the test cannot pass by accident if the receiver
    // confuses `msg.sender` with the runtime-attested origin.
    let inband_sender = DelegateKey::new([0xBBu8; 32], CodeHash::new([0xCCu8; 32]));
    let delegate_msg = DelegateMessage::new(key_b.clone(), inband_sender, b"attest-me".to_vec());

    let origin = MessageOrigin::Delegate(caller_a.clone());
    let outbound = runtime.inbound_app_message(
        &key_b,
        &vec![2u8].into(),
        Some(&origin),
        None,
        vec![InboundDelegateMsg::DelegateMessage(delegate_msg)],
    )?;

    assert_eq!(outbound.len(), 1, "Expected exactly one outbound message");

    // Wildcard satisfies #[non_exhaustive] on OutboundDelegateMsg so
    // future stdlib variants don't break this test at compile time.
    #[allow(clippy::wildcard_enum_match_arm)]
    let app_msg = match &outbound[0] {
        OutboundDelegateMsg::ApplicationMessage(m) => m,
        other => panic!("Expected ApplicationMessage, got {other:?}"),
    };
    let response: OutboundAppMessage = bincode::deserialize(&app_msg.payload)?;
    // Wildcard satisfies #[non_exhaustive] on OutboundAppMessage so
    // future stdlib variants don't break this test at compile time.
    #[allow(clippy::wildcard_enum_match_arm)]
    match response {
        OutboundAppMessage::DelegateMessageReceived {
            origin_delegate_key_bytes,
            ..
        } => {
            let observed = origin_delegate_key_bytes
                .expect("Receiver should see Some(MessageOrigin::Delegate(..))");
            assert_eq!(
                observed,
                caller_a.bytes(),
                "Receiver must see the runtime-attested caller key, not msg.sender"
            );
        }
        other => panic!("Expected DelegateMessageReceived, got {other:?}"),
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delegate_to_delegate_roundtrip() -> Result<(), Box<dyn std::error::Error>> {
    use messaging_messages::*;

    // Load same code with different params → different keys
    let (delegate_a, mut runtime_a, _temp_dir_a) =
        setup_runtime_with_params(TEST_DELEGATE_MESSAGING, vec![1]).await?;
    let key_a = delegate_a.key().clone();

    let (delegate_b, mut runtime_b, _temp_dir_b) =
        setup_runtime_with_params(TEST_DELEGATE_MESSAGING, vec![2]).await?;
    let key_b = delegate_b.key().clone();

    // Step 1: Send command to A: "send a message to B"
    let _app_id = ContractInstanceId::new([1u8; 32]);
    let payload = bincode::serialize(&InboundAppMessage::SendToDelegate {
        target_key_bytes: key_b.bytes().to_vec(),
        target_code_hash: key_b.code_hash().as_ref().to_vec(),
        payload: b"inter-delegate".to_vec(),
    })?;
    let app_msg = ApplicationMessage::new(payload);

    let outbound_a = runtime_a.inbound_app_message(
        &key_a,
        &vec![1u8].into(),
        None,
        None,
        vec![InboundDelegateMsg::ApplicationMessage(app_msg)],
    )?;

    // Step 2: Extract the SendDelegateMessage from A's output
    let send_msg = outbound_a
        .iter()
        .find_map(|m| match m {
            OutboundDelegateMsg::SendDelegateMessage(msg) => Some(msg.clone()),
            OutboundDelegateMsg::ApplicationMessage(_)
            | OutboundDelegateMsg::RequestUserInput(_)
            | OutboundDelegateMsg::ContextUpdated(_)
            | OutboundDelegateMsg::GetContractRequest(_)
            | OutboundDelegateMsg::PutContractRequest(_)
            | OutboundDelegateMsg::UpdateContractRequest(_)
            | OutboundDelegateMsg::SubscribeContractRequest(_) => None,
        })
        .expect("Expected SendDelegateMessage from delegate A");

    assert_eq!(send_msg.sender, key_a, "Sender should be attested as A");
    assert_eq!(send_msg.payload, b"inter-delegate");

    // Step 3: Deliver to B as InboundDelegateMsg::DelegateMessage
    let outbound_b = runtime_b.inbound_app_message(
        &key_b,
        &vec![2u8].into(),
        None,
        None,
        vec![InboundDelegateMsg::DelegateMessage(send_msg)],
    )?;

    assert_eq!(outbound_b.len(), 1);

    let app_msg_b = match &outbound_b[0] {
        OutboundDelegateMsg::ApplicationMessage(msg) => msg,
        OutboundDelegateMsg::RequestUserInput(_)
        | OutboundDelegateMsg::ContextUpdated(_)
        | OutboundDelegateMsg::GetContractRequest(_)
        | OutboundDelegateMsg::PutContractRequest(_)
        | OutboundDelegateMsg::UpdateContractRequest(_)
        | OutboundDelegateMsg::SubscribeContractRequest(_)
        | OutboundDelegateMsg::SendDelegateMessage(_) => {
            panic!(
                "Expected ApplicationMessage from B, got {:?}",
                &outbound_b[0]
            )
        }
    };

    let response: OutboundAppMessage = bincode::deserialize(&app_msg_b.payload)?;
    match response {
        OutboundAppMessage::DelegateMessageReceived {
            sender_key_bytes,
            payload,
            origin_delegate_key_bytes,
        } => {
            assert_eq!(
                sender_key_bytes,
                key_a.bytes(),
                "B should see A as the sender"
            );
            assert_eq!(payload, b"inter-delegate");
            // Origin not asserted here: this roundtrip test calls
            // inbound_app_message with origin=None (it bypasses the
            // executor that injects MessageOrigin::Delegate). End-to-end
            // propagation of MessageOrigin::Delegate through the WASM
            // boundary is covered by
            // `test_inbound_app_message_propagates_delegate_origin`.
            let _ = origin_delegate_key_bytes;
        }
        OutboundAppMessage::MessageSent | OutboundAppMessage::PingResponse { .. } => {
            panic!("Expected DelegateMessageReceived, got {:?}", response)
        }
    }

    Ok(())
}

/// Verify that when a delegate emits multiple SendDelegateMessage outbound,
/// all of them get sender attestation (not just the first one).
/// Regression test for PR #3282 review: drain(..) bypass.
#[tokio::test(flavor = "multi_thread")]
async fn test_multiple_send_delegate_messages_all_attested()
-> Result<(), Box<dyn std::error::Error>> {
    let (delegate_a, mut runtime, _temp_dir) =
        setup_runtime_with_params(TEST_DELEGATE_MESSAGING, vec![1]).await?;
    let key_a = delegate_a.key().clone();

    // Create two different target keys
    let target_b = DelegateKey::new([42u8; 32], CodeHash::new([99u8; 32]));
    let target_c = DelegateKey::new([43u8; 32], CodeHash::new([98u8; 32]));

    // Send two messages via two separate inbound ApplicationMessages.
    // The first triggers SendDelegateMessage → break + drain, so
    // the second SendDelegateMessage goes through drain(..).
    let _app_id = ContractInstanceId::new([1u8; 32]);
    let payload1 = bincode::serialize(&messaging_messages::InboundAppMessage::SendToDelegate {
        target_key_bytes: target_b.bytes().to_vec(),
        target_code_hash: target_b.code_hash().as_ref().to_vec(),
        payload: b"msg1".to_vec(),
    })?;
    let payload2 = bincode::serialize(&messaging_messages::InboundAppMessage::SendToDelegate {
        target_key_bytes: target_c.bytes().to_vec(),
        target_code_hash: target_c.code_hash().as_ref().to_vec(),
        payload: b"msg2".to_vec(),
    })?;

    let outbound = runtime.inbound_app_message(
        &key_a,
        &vec![1u8].into(),
        None,
        None,
        vec![
            InboundDelegateMsg::ApplicationMessage(ApplicationMessage::new(payload1)),
            InboundDelegateMsg::ApplicationMessage(ApplicationMessage::new(payload2)),
        ],
    )?;

    // Collect all SendDelegateMessage from outbound
    let send_msgs: Vec<&DelegateMessage> = outbound
        .iter()
        .filter_map(|m| match m {
            OutboundDelegateMsg::SendDelegateMessage(msg) => Some(msg),
            OutboundDelegateMsg::ApplicationMessage(_)
            | OutboundDelegateMsg::RequestUserInput(_)
            | OutboundDelegateMsg::ContextUpdated(_)
            | OutboundDelegateMsg::GetContractRequest(_)
            | OutboundDelegateMsg::PutContractRequest(_)
            | OutboundDelegateMsg::UpdateContractRequest(_)
            | OutboundDelegateMsg::SubscribeContractRequest(_) => None,
        })
        .collect();

    // Should have at least 1 (the first triggers break+drain,
    // second may come through drain)
    assert!(
        !send_msgs.is_empty(),
        "Expected at least one SendDelegateMessage"
    );

    // ALL SendDelegateMessage must have sender attested as key_a
    for (i, msg) in send_msgs.iter().enumerate() {
        assert_eq!(
            msg.sender, key_a,
            "SendDelegateMessage[{i}] sender should be attested as delegate A, \
             but got {:?}",
            msg.sender
        );
    }

    Ok(())
}

/// Build a bare `Runtime` with empty stores and no loaded delegate.
///
/// `process_outbound` never touches the WASM engine for the terminal-arm
/// drain paths (its `_handle`/`_instance_id`/`_params` arguments are unused
/// there), so these regression tests can drive it directly without compiling
/// or instantiating a delegate module.
async fn bare_runtime() -> (Runtime, tempfile::TempDir) {
    use crate::contract::storages::Storage;
    let temp_dir = get_temp_dir();
    let db = Storage::new(temp_dir.path()).await.unwrap();
    let contract_store =
        ContractStore::new(temp_dir.path().join("contracts"), 10_000, db.clone()).unwrap();
    let delegate_store =
        DelegateStore::new(temp_dir.path().join("delegates"), 10_000, db.clone()).unwrap();
    let secret_store =
        SecretsStore::new(temp_dir.path().join("secrets"), Default::default(), db).unwrap();
    let runtime = Runtime::build(contract_store, delegate_store, secret_store, false).unwrap();
    (runtime, temp_dir)
}

/// Regression test for #4668: a `SendDelegateMessage` queued behind a
/// non-`SendDelegateMessage` terminal message (here `ApplicationMessage`)
/// must still have its `sender` re-attested to the real delegate key.
///
/// Before the fix, the `ApplicationMessage` terminal arm blind-drained the
/// remaining messages, so a forged `DelegateMessage.sender` reached the
/// target delegate unchanged.
#[tokio::test(flavor = "multi_thread")]
async fn test_drain_behind_application_message_reattests_sender()
-> Result<(), Box<dyn std::error::Error>> {
    use std::collections::VecDeque;

    let (mut runtime, _temp_dir) = bare_runtime().await;

    let delegate_key = DelegateKey::new([7u8; 32], CodeHash::new([8u8; 32]));
    // Distinct, non-zero forged sender so the assertion is unambiguous.
    let forged = DelegateKey::new([0xEEu8; 32], CodeHash::new([0xEFu8; 32]));
    let target = DelegateKey::new([42u8; 32], CodeHash::new([99u8; 32]));

    let mut outbound: VecDeque<OutboundDelegateMsg> = VecDeque::new();
    outbound.push_back(OutboundDelegateMsg::ApplicationMessage(
        ApplicationMessage::new(vec![1, 2, 3]),
    ));
    outbound.push_back(OutboundDelegateMsg::SendDelegateMessage(
        DelegateMessage::new(target.clone(), forged.clone(), b"forged".to_vec()),
    ));

    let handle = InstanceHandle { id: 0 };
    let params: Parameters = vec![].into();
    let mut context = Vec::new();
    let mut results = Vec::new();
    runtime.process_outbound(
        &delegate_key,
        &handle,
        0,
        &params,
        None,
        &mut outbound,
        &mut context,
        &mut results,
    )?;

    let send = results
        .iter()
        .find_map(|m| match m {
            OutboundDelegateMsg::SendDelegateMessage(m) => Some(m),
            OutboundDelegateMsg::ApplicationMessage(_)
            | OutboundDelegateMsg::RequestUserInput(_)
            | OutboundDelegateMsg::ContextUpdated(_)
            | OutboundDelegateMsg::GetContractRequest(_)
            | OutboundDelegateMsg::PutContractRequest(_)
            | OutboundDelegateMsg::UpdateContractRequest(_)
            | OutboundDelegateMsg::SubscribeContractRequest(_) => None,
        })
        .expect("SendDelegateMessage should be drained into results");

    assert_eq!(
        send.sender, delegate_key,
        "SendDelegateMessage drained behind an ApplicationMessage must be \
         re-attested to the real delegate key, not the forged {forged:?}"
    );

    Ok(())
}

/// Regression test for #4668: same bypass via the `GetContractRequest`
/// (unprocessed) terminal arm — a second non-`SendDelegateMessage` terminal
/// arm to guard against the blind-drain pattern being reintroduced piecemeal.
#[tokio::test(flavor = "multi_thread")]
async fn test_drain_behind_get_contract_request_reattests_sender()
-> Result<(), Box<dyn std::error::Error>> {
    use std::collections::VecDeque;

    let (mut runtime, _temp_dir) = bare_runtime().await;

    let delegate_key = DelegateKey::new([1u8; 32], CodeHash::new([2u8; 32]));
    let forged = DelegateKey::new([0xAAu8; 32], CodeHash::new([0xBBu8; 32]));
    let target = DelegateKey::new([3u8; 32], CodeHash::new([4u8; 32]));

    let mut outbound: VecDeque<OutboundDelegateMsg> = VecDeque::new();
    // Unprocessed GetContractRequest is a terminal arm (passes to executor).
    outbound.push_back(OutboundDelegateMsg::GetContractRequest(
        GetContractRequest::new(ContractInstanceId::new([5u8; 32])),
    ));
    outbound.push_back(OutboundDelegateMsg::SendDelegateMessage(
        DelegateMessage::new(target, forged.clone(), b"forged".to_vec()),
    ));

    let handle = InstanceHandle { id: 0 };
    let params: Parameters = vec![].into();
    let mut context = Vec::new();
    let mut results = Vec::new();
    runtime.process_outbound(
        &delegate_key,
        &handle,
        0,
        &params,
        None,
        &mut outbound,
        &mut context,
        &mut results,
    )?;

    let send = results
        .iter()
        .find_map(|m| match m {
            OutboundDelegateMsg::SendDelegateMessage(m) => Some(m),
            OutboundDelegateMsg::ApplicationMessage(_)
            | OutboundDelegateMsg::RequestUserInput(_)
            | OutboundDelegateMsg::ContextUpdated(_)
            | OutboundDelegateMsg::GetContractRequest(_)
            | OutboundDelegateMsg::PutContractRequest(_)
            | OutboundDelegateMsg::UpdateContractRequest(_)
            | OutboundDelegateMsg::SubscribeContractRequest(_) => None,
        })
        .expect("SendDelegateMessage should be drained into results");

    assert_eq!(
        send.sender, delegate_key,
        "SendDelegateMessage drained behind a GetContractRequest must be \
         re-attested to the real delegate key, not the forged {forged:?}"
    );

    Ok(())
}

/// Hosted-mode per-user secret namespace tests (P2 of #4381).
///
/// These drive the real secret host functions (`set_secret`/`get_secret`/
/// `has_secret`/`remove_secret`) through `inbound_app_message` with varying
/// `user_context` values, and assert that the namespace a delegate's secret
/// operations land in is determined SOLELY by the `user_context` argument —
/// the connection-boundary credential — and never by the message body, the
/// delegate key, or the origin.
mod hosted_user_secrets {
    use super::delegate2_messages::{InboundAppMessage, OutboundAppMessage};
    use super::*;
    use crate::wasm_runtime::UserSecretContext;

    /// Drive a single `delegate2` app message through `inbound_app_message`
    /// under the given `user_context` and decode the single outbound
    /// `OutboundAppMessage` reply. This is the one place the test exercises
    /// the secret scope: `user_context` is the ONLY thing that varies the
    /// namespace; `key`, `params`, `origin`, and the message body are held
    /// identical across users by the callers below.
    fn run(
        runtime: &mut Runtime,
        delegate_key: &DelegateKey,
        user_context: Option<&UserSecretContext>,
        msg: InboundAppMessage,
    ) -> OutboundAppMessage {
        let payload = bincode::serialize(&msg).expect("serialize inbound");
        let app_msg = ApplicationMessage::new(payload);
        let outbound = runtime
            .inbound_app_message(
                delegate_key,
                &vec![].into(),
                None, // origin: identical for every user — not the scope source
                user_context,
                vec![InboundDelegateMsg::ApplicationMessage(app_msg)],
            )
            .expect("inbound_app_message");
        let OutboundDelegateMsg::ApplicationMessage(m) = &outbound[0] else {
            panic!("Expected ApplicationMessage reply, got {:?}", &outbound[0]);
        };
        bincode::deserialize(&m.payload).expect("decode outbound")
    }

    fn store(
        runtime: &mut Runtime,
        key: &DelegateKey,
        ctx: Option<&UserSecretContext>,
        sk: Vec<u8>,
        sv: Vec<u8>,
    ) {
        let resp = run(
            runtime,
            key,
            ctx,
            InboundAppMessage::StoreSecret { key: sk, value: sv },
        );
        assert!(
            matches!(resp, OutboundAppMessage::SecretStored),
            "store should succeed, got {resp:?}"
        );
    }

    fn get(
        runtime: &mut Runtime,
        key: &DelegateKey,
        ctx: Option<&UserSecretContext>,
        sk: Vec<u8>,
    ) -> Option<Vec<u8>> {
        let resp = run(
            runtime,
            key,
            ctx,
            InboundAppMessage::GetNonExistentSecret(sk),
        );
        let OutboundAppMessage::SecretResult(v) = resp else {
            panic!("Expected SecretResult, got {resp:?}");
        };
        v
    }

    fn has(
        runtime: &mut Runtime,
        key: &DelegateKey,
        ctx: Option<&UserSecretContext>,
        sk: Vec<u8>,
    ) -> bool {
        let resp = run(runtime, key, ctx, InboundAppMessage::HasSecret(sk));
        let OutboundAppMessage::SecretExists(b) = resp else {
            panic!("Expected SecretExists, got {resp:?}");
        };
        b
    }

    /// Two different user tokens get disjoint secret namespaces under the
    /// SAME delegate: A's secret is invisible to B, B's to A, and each can
    /// read only its own — even though the secret KEY is identical.
    #[tokio::test(flavor = "multi_thread")]
    async fn two_users_have_disjoint_secret_namespaces() -> Result<(), Box<dyn std::error::Error>> {
        let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_2).await?;
        let key = delegate.key().clone();

        let ctx_a = UserSecretContext::from_token(b"token-A");
        let ctx_b = UserSecretContext::from_token(b"token-B");

        // Same secret KEY for both users; different values.
        let sk = vec![7u8, 7, 7];
        let val_a = vec![0xAA; 16];
        let val_b = vec![0xBB; 16];

        store(&mut runtime, &key, Some(&ctx_a), sk.clone(), val_a.clone());
        store(&mut runtime, &key, Some(&ctx_b), sk.clone(), val_b.clone());

        // Each user reads back ONLY its own value.
        assert_eq!(
            get(&mut runtime, &key, Some(&ctx_a), sk.clone()),
            Some(val_a)
        );
        assert_eq!(
            get(&mut runtime, &key, Some(&ctx_b), sk.clone()),
            Some(val_b)
        );

        // A cannot read B's namespace and vice-versa is implied by the
        // distinct values above; assert existence is per-namespace too.
        assert!(has(&mut runtime, &key, Some(&ctx_a), sk.clone()));
        assert!(has(&mut runtime, &key, Some(&ctx_b), sk.clone()));

        // Removing A's secret leaves B's intact (independent namespaces).
        let resp = run(
            &mut runtime,
            &key,
            Some(&ctx_a),
            InboundAppMessage::RemoveSecret(sk.clone()),
        );
        assert!(matches!(resp, OutboundAppMessage::SecretRemoved));
        assert!(
            !has(&mut runtime, &key, Some(&ctx_a), sk.clone()),
            "A's secret should be gone"
        );
        assert!(
            has(&mut runtime, &key, Some(&ctx_b), sk.clone()),
            "B's secret must survive A's removal"
        );

        std::mem::drop(temp_dir);
        Ok(())
    }

    /// A secret written with NO user context (single-user `Local`) is NOT
    /// visible under any user token, and vice-versa: the Local namespace
    /// and every User namespace are mutually disjoint. This proves that
    /// turning hosted mode on for a connection (token present) does not
    /// expose — or collide with — the node's pre-existing single-user
    /// secrets, so the flag-off behavior is preserved byte-for-byte.
    #[tokio::test(flavor = "multi_thread")]
    async fn local_and_user_namespaces_are_disjoint() -> Result<(), Box<dyn std::error::Error>> {
        let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_2).await?;
        let key = delegate.key().clone();
        let ctx_a = UserSecretContext::from_token(b"token-A");

        let sk = vec![9u8, 9, 9];
        let local_val = vec![0x11; 8];
        let user_val = vec![0x22; 8];

        // Write under Local (no token).
        store(&mut runtime, &key, None, sk.clone(), local_val.clone());
        // Same key under user A.
        store(
            &mut runtime,
            &key,
            Some(&ctx_a),
            sk.clone(),
            user_val.clone(),
        );

        // Each side sees only its own.
        assert_eq!(get(&mut runtime, &key, None, sk.clone()), Some(local_val));
        assert_eq!(
            get(&mut runtime, &key, Some(&ctx_a), sk.clone()),
            Some(user_val)
        );

        // A key written only under Local is invisible to a user, and a
        // key written only under a user is invisible to Local.
        let local_only = vec![1u8];
        store(&mut runtime, &key, None, local_only.clone(), vec![1]);
        assert!(
            !has(&mut runtime, &key, Some(&ctx_a), local_only.clone()),
            "Local-only secret must be invisible to a user"
        );

        let user_only = vec![2u8];
        store(&mut runtime, &key, Some(&ctx_a), user_only.clone(), vec![2]);
        assert!(
            !has(&mut runtime, &key, None, user_only),
            "User-only secret must be invisible to Local"
        );

        std::mem::drop(temp_dir);
        Ok(())
    }

    /// INVARIANT: the secret namespace is a pure function of `user_context`.
    /// Holding the delegate key, params, origin, and message body byte-for-byte
    /// identical, swapping ONLY the `user_context` swaps the namespace —
    /// nothing in the message body can reach across to another user's
    /// secrets. This is the core unforgeability property: a delegate (or a
    /// client crafting the message body) cannot select WHICH user's
    /// namespace it operates on, because that choice is carried entirely by
    /// the out-of-band `user_context` argument.
    #[tokio::test(flavor = "multi_thread")]
    async fn namespace_is_determined_solely_by_user_context()
    -> Result<(), Box<dyn std::error::Error>> {
        let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_2).await?;
        let key = delegate.key().clone();

        let ctx_a = UserSecretContext::from_token(b"alice");
        let ctx_b = UserSecretContext::from_token(b"bob");

        // IDENTICAL message body for the write — only the context differs.
        let sk = vec![5u8, 5];
        store(&mut runtime, &key, Some(&ctx_a), sk.clone(), vec![0xA1]);

        // Reading the SAME key, with the SAME body, under a DIFFERENT
        // context yields nothing: the body did not carry the namespace.
        assert_eq!(
            get(&mut runtime, &key, Some(&ctx_b), sk.clone()),
            None,
            "A different user_context must NOT see user A's secret, even with an identical request body"
        );
        // And re-reading under A's context still finds it.
        assert_eq!(
            get(&mut runtime, &key, Some(&ctx_a), sk.clone()),
            Some(vec![0xA1])
        );

        // Re-deriving the context from the same token reproduces the same
        // namespace (the derivation is deterministic in the token alone).
        let ctx_a_again = UserSecretContext::from_token(b"alice");
        assert_eq!(
            get(&mut runtime, &key, Some(&ctx_a_again), sk),
            Some(vec![0xA1])
        );

        std::mem::drop(temp_dir);
        Ok(())
    }

    /// The same token always maps to the same `UserId`, and two different
    /// tokens map to different `UserId`s — pinned here so the connection
    /// boundary's identity derivation can't silently change and re-key every
    /// hosted user's secrets. (The dek_secret is never asserted on directly;
    /// it is exercised end-to-end by the namespace tests above.)
    #[test]
    fn user_id_is_a_stable_function_of_the_token() {
        let a1 = UserSecretContext::from_token(b"token-A");
        let a2 = UserSecretContext::from_token(b"token-A");
        let b = UserSecretContext::from_token(b"token-B");
        assert_eq!(a1.user_id(), a2.user_id(), "same token => same UserId");
        assert_ne!(
            a1.user_id(),
            b.user_id(),
            "different tokens => different UserId"
        );
    }

    /// The `Debug` impl must never leak the `dek_secret`. A struct that
    /// transitively holds a `UserSecretContext` (e.g. the delegate-request
    /// contract-handler event) is logged with `{:?}`, so a non-redacting
    /// Debug would write key material to the logs.
    #[test]
    fn debug_redacts_the_dek_secret() {
        let ctx = UserSecretContext::from_token(b"super-secret-token");
        let rendered = format!("{ctx:?}");
        assert!(
            rendered.contains("redacted"),
            "dek_secret must be redacted in Debug, got: {rendered}"
        );
        // The non-secret user_id is fine to show.
        assert!(
            rendered.contains(&ctx.user_id().encode()),
            "user_id should appear in Debug"
        );
    }

    /// The per-user namespace does NOT propagate across a delegate-to-delegate
    /// hop. When delegate A (running under user X's connection) sends a
    /// `SendDelegateMessage` to delegate B, the executor delivers it to B with
    /// `user_context = None` and `origin = Some(MessageOrigin::Delegate(A))`
    /// (see `contract.rs::handle_delegate_with_contract_requests`, the
    /// `execute_delegate_request(target_req, None, Some(delegate_key), None)`
    /// call). So B's secret op MUST land in B's `Local` namespace, never under
    /// user X — the user namespace is bound to the originating connection, not
    /// transitively inherited through the inter-delegate hop.
    ///
    /// This test drives B exactly the way the executor drives it for that hop:
    /// `origin = Delegate(A)`, `user_context = None`. It is parameterised over
    /// the user X whose namespace must stay untouched.
    #[tokio::test(flavor = "multi_thread")]
    async fn user_namespace_does_not_propagate_across_inter_delegate_hop()
    -> Result<(), Box<dyn std::error::Error>> {
        // Register two distinct delegates (A the sender, B the target). Only B
        // performs the secret op; A's key is used solely to attest the
        // inter-delegate origin the executor would inject.
        let (delegate_b, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_2).await?;
        let key_b = delegate_b.key().clone();
        // Synthetic sender delegate A (key only — its WASM is irrelevant here;
        // we are testing what B's secrets bind to, given the hop's arguments).
        let key_a = DelegateKey::new([0xA1u8; 32], CodeHash::new([0xA2u8; 32]));

        // User X's connection: a secret B stores DIRECTLY under X's context.
        let ctx_x = UserSecretContext::from_token(b"user-X");
        let sk = vec![0xEE, 0xEE];
        let x_value = vec![0x58; 8]; // 'X'
        store(
            &mut runtime,
            &key_b,
            Some(&ctx_x),
            sk.clone(),
            x_value.clone(),
        );

        // Now deliver to B the way the executor delivers an inter-delegate hop
        // that originated from A (which was itself invoked under user X):
        //   origin = Some(MessageOrigin::Delegate(A)),  user_context = None.
        // B stores under the SAME secret key but a different value.
        let hop_value = vec![0x42; 8]; // 'B' — the inter-delegate write
        let origin = MessageOrigin::Delegate(key_a.clone());
        let payload = bincode::serialize(&InboundAppMessage::StoreSecret {
            key: sk.clone(),
            value: hop_value.clone(),
        })
        .expect("serialize");
        let app_msg = ApplicationMessage::new(payload);
        let outbound = runtime
            .inbound_app_message(
                &key_b,
                &vec![].into(),
                Some(&origin), // attested inter-delegate origin (A)
                None,          // user_context = None: the hop does NOT carry X
                vec![InboundDelegateMsg::ApplicationMessage(app_msg)],
            )
            .expect("inbound_app_message");
        let OutboundDelegateMsg::ApplicationMessage(m) = &outbound[0] else {
            panic!("Expected ApplicationMessage reply, got {:?}", &outbound[0]);
        };
        let resp: OutboundAppMessage = bincode::deserialize(&m.payload).expect("decode");
        assert!(
            matches!(resp, OutboundAppMessage::SecretStored),
            "inter-delegate-hop store should succeed, got {resp:?}"
        );

        // The hop's write landed in B's Local namespace...
        assert_eq!(
            get(&mut runtime, &key_b, None, sk.clone()),
            Some(hop_value),
            "inter-delegate-hop secret must be in B's Local namespace"
        );
        // ...and user X's namespace is UNTOUCHED — it still holds X's value,
        // NOT the value written during the A->B hop. This is the property:
        // the hop did not write into (or read from) X's namespace.
        assert_eq!(
            get(&mut runtime, &key_b, Some(&ctx_x), sk),
            Some(x_value),
            "user X's secret must be unchanged by the inter-delegate hop"
        );

        std::mem::drop(temp_dir);
        Ok(())
    }
}

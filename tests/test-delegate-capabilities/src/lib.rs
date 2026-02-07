//! Test delegate for exercising all delegate capabilities from issue #2827.
//!
//! This delegate responds to different command messages to test:
//! - #2828: Contract GET (GetContractRequest/GetContractResponse)
//! - #2829: Contract PUT (PutContractRequest/PutContractResponse)
//! - #2830: Contract SUBSCRIBE (future)
//! - #2831: Contract UPDATE (future)
//! - #2832: Delegate registration (future)
//! - #2833: Delegate unregistration (future)

use freenet_stdlib::prelude::*;
use serde::{Deserialize, Serialize};

/// Context stored between delegate calls
#[derive(Debug, Serialize, Deserialize, Default, Clone)]
struct DelegateState {
    /// The contract we're waiting for state from
    pending_contract_get: Option<ContractInstanceId>,
    /// Store any additional state needed for multi-step operations
    operation_context: Option<Vec<u8>>,
    /// For GetMultipleContractStates: remaining contracts to fetch
    remaining_contracts: Vec<ContractInstanceId>,
    /// For GetMultipleContractStates: accumulated results
    accumulated_results: Vec<(ContractInstanceId, Option<Vec<u8>>)>,
    /// For GetContractWithEcho: the echo message to include
    echo_message: Option<String>,
}

impl DelegateState {
    fn to_context(&self) -> DelegateContext {
        DelegateContext::new(bincode::serialize(self).unwrap_or_default())
    }

    fn from_context(ctx: &DelegateContext) -> Self {
        if ctx.as_ref().is_empty() {
            Self::default()
        } else {
            bincode::deserialize(ctx.as_ref()).unwrap_or_default()
        }
    }
}

/// Commands that can be sent to this delegate via ApplicationMessage
#[derive(Debug, Serialize, Deserialize)]
pub enum DelegateCommand {
    /// Request to get a contract's state
    /// Returns GetContractRequest, then on response returns the state (or None)
    GetContractState { contract_id: ContractInstanceId },

    /// Request to get multiple contracts' states in sequence
    /// Returns GetContractRequest for the first contract, then when response comes,
    /// returns GetContractRequest for the next, etc.
    /// This tests the handler loop's ability to iterate multiple times.
    GetMultipleContractStates {
        contract_ids: Vec<ContractInstanceId>,
    },

    /// Test message accumulation: emit both GetContractRequest AND ApplicationMessage
    /// This tests that non-contract messages are accumulated while processing contract requests
    GetContractWithEcho {
        contract_id: ContractInstanceId,
        echo_message: String,
    },

    /// Request to put a contract's state (fire-and-forget)
    /// Emits PutContractRequest which the runtime handles asynchronously
    PutContractState {
        contract: ContractContainer,
        state: Vec<u8>,
    },
    // Future capabilities for #2827:
    // SubscribeContract { contract_id: ContractInstanceId },
    // UpdateContract { contract_id: ContractInstanceId, delta: Vec<u8> },
    // RegisterDelegate { delegate_code: Vec<u8>, params: Vec<u8> },
    // UnregisterDelegate { delegate_key: Vec<u8> },
}

/// Responses from this delegate
#[derive(Debug, Serialize, Deserialize)]
pub enum DelegateResponse {
    /// Contract state retrieved (or None if not found)
    ContractState {
        contract_id: ContractInstanceId,
        state: Option<Vec<u8>>,
    },
    /// Multiple contract states retrieved
    MultipleContractStates {
        results: Vec<(ContractInstanceId, Option<Vec<u8>>)>,
    },
    /// Echo message (for testing message accumulation)
    Echo { message: String },
    /// Contract PUT result (fire-and-forget, result comes via PutContractResponse)
    ContractPutResult {
        contract_id: ContractInstanceId,
        success: bool,
        error: Option<String>,
    },
    /// Error occurred
    Error { message: String },
    // Future responses:
    // SubscriptionConfirmed { contract_id: ContractInstanceId },
    // UpdateApplied { contract_id: ContractInstanceId },
    // DelegateRegistered { delegate_key: Vec<u8> },
    // DelegateUnregistered { delegate_key: Vec<u8> },
}

struct TestDelegate;

#[delegate]
impl DelegateInterface for TestDelegate {
    fn process(
        _ctx: &mut DelegateCtx,
        _params: Parameters<'static>,
        _attested: Option<&'static [u8]>,
        message: InboundDelegateMsg,
    ) -> Result<Vec<OutboundDelegateMsg>, DelegateError> {
        match message {
            InboundDelegateMsg::ApplicationMessage(app_msg) => handle_application_message(app_msg),
            InboundDelegateMsg::GetContractResponse(response) => {
                handle_get_contract_response(response)
            }
            InboundDelegateMsg::PutContractResponse(response) => {
                handle_put_contract_response(response)
            }
            InboundDelegateMsg::UserResponse(_) => {
                // Not used by this delegate
                Ok(vec![])
            }
        }
    }
}

fn handle_application_message(
    app_msg: ApplicationMessage,
) -> Result<Vec<OutboundDelegateMsg>, DelegateError> {
    let command: DelegateCommand = bincode::deserialize(&app_msg.payload)
        .map_err(|e| DelegateError::Other(format!("Failed to deserialize command: {}", e)))?;

    match command {
        DelegateCommand::GetContractState { contract_id } => {
            // Store state so we know what we're waiting for
            let state = DelegateState {
                pending_contract_get: Some(contract_id),
                operation_context: Some(app_msg.app.as_bytes().to_vec()),
                ..Default::default()
            };

            // Emit GetContractRequest
            let request = GetContractRequest {
                contract_id,
                context: state.to_context(),
                processed: false,
            };

            Ok(vec![OutboundDelegateMsg::GetContractRequest(request)])
        }

        DelegateCommand::GetMultipleContractStates { contract_ids } => {
            if contract_ids.is_empty() {
                // No contracts to fetch, return empty result
                let response = DelegateResponse::MultipleContractStates { results: vec![] };
                let payload = bincode::serialize(&response)
                    .map_err(|e| DelegateError::Other(format!("Serialize error: {}", e)))?;
                let msg = ApplicationMessage::new(app_msg.app, payload)
                    .processed(true)
                    .with_context(DelegateContext::default());
                return Ok(vec![OutboundDelegateMsg::ApplicationMessage(msg)]);
            }

            // Take first contract, store rest for later
            let mut remaining = contract_ids;
            let first = remaining.remove(0);

            let state = DelegateState {
                pending_contract_get: Some(first),
                operation_context: Some(app_msg.app.as_bytes().to_vec()),
                remaining_contracts: remaining,
                accumulated_results: vec![],
                echo_message: None,
            };

            let request = GetContractRequest {
                contract_id: first,
                context: state.to_context(),
                processed: false,
            };

            Ok(vec![OutboundDelegateMsg::GetContractRequest(request)])
        }

        DelegateCommand::PutContractState { contract, state } => {
            let mut request = PutContractRequest::new(
                contract,
                WrappedState::new(state),
                RelatedContracts::default(),
            );
            // Store context so the response handler can identify the originating app
            let delegate_state = DelegateState {
                pending_contract_get: None,
                operation_context: Some(app_msg.app.as_bytes().to_vec()),
                ..Default::default()
            };
            request.context = delegate_state.to_context();

            Ok(vec![OutboundDelegateMsg::PutContractRequest(request)])
        }

        DelegateCommand::GetContractWithEcho {
            contract_id,
            echo_message,
        } => {
            let state = DelegateState {
                pending_contract_get: Some(contract_id),
                operation_context: Some(app_msg.app.as_bytes().to_vec()),
                echo_message: Some(echo_message.clone()),
                ..Default::default()
            };

            // Emit BOTH GetContractRequest AND an immediate echo ApplicationMessage
            // This tests message accumulation
            let request = GetContractRequest {
                contract_id,
                context: state.to_context(),
                processed: false,
            };

            let echo_response = DelegateResponse::Echo {
                message: echo_message,
            };
            let echo_payload = bincode::serialize(&echo_response)
                .map_err(|e| DelegateError::Other(format!("Serialize error: {}", e)))?;
            let echo_msg = ApplicationMessage::new(app_msg.app, echo_payload)
                .processed(true)
                .with_context(DelegateContext::default());

            // Return both messages - the handler should accumulate the echo
            // while processing the contract request
            Ok(vec![
                OutboundDelegateMsg::GetContractRequest(request),
                OutboundDelegateMsg::ApplicationMessage(echo_msg),
            ])
        }
    }
}

fn handle_get_contract_response(
    response: GetContractResponse,
) -> Result<Vec<OutboundDelegateMsg>, DelegateError> {
    // Restore state from context
    let mut state = DelegateState::from_context(&response.context);

    // Verify this is the contract we were waiting for
    let expected_contract = state.pending_contract_get.ok_or_else(|| {
        DelegateError::Other("Received GetContractResponse but wasn't expecting one".into())
    })?;

    if expected_contract != response.contract_id {
        return Err(DelegateError::Other(format!(
            "Contract ID mismatch: expected {}, got {}",
            expected_contract, response.contract_id
        )));
    }

    // Get the original app ID from context
    let app_id = state
        .operation_context
        .clone()
        .and_then(|bytes| {
            ContractInstanceId::try_from(String::from_utf8(bytes).unwrap_or_default()).ok()
        })
        .unwrap_or_else(|| ContractInstanceId::new([0u8; 32]));

    // Check if this is a multi-contract fetch
    if !state.remaining_contracts.is_empty() || !state.accumulated_results.is_empty() {
        // Multi-contract case: accumulate this result
        state
            .accumulated_results
            .push((response.contract_id, response.state.map(|s| s.to_vec())));

        if state.remaining_contracts.is_empty() {
            // All contracts fetched, return accumulated results
            let response_payload = DelegateResponse::MultipleContractStates {
                results: state.accumulated_results,
            };
            let payload = bincode::serialize(&response_payload)
                .map_err(|e| DelegateError::Other(format!("Serialize error: {}", e)))?;
            let app_msg = ApplicationMessage::new(app_id, payload)
                .processed(true)
                .with_context(DelegateContext::default());
            return Ok(vec![OutboundDelegateMsg::ApplicationMessage(app_msg)]);
        } else {
            // More contracts to fetch
            let next = state.remaining_contracts.remove(0);
            state.pending_contract_get = Some(next);

            let request = GetContractRequest {
                contract_id: next,
                context: state.to_context(),
                processed: false,
            };
            return Ok(vec![OutboundDelegateMsg::GetContractRequest(request)]);
        }
    }

    // Single contract case: return immediately
    let response_payload = DelegateResponse::ContractState {
        contract_id: response.contract_id,
        state: response.state.map(|s| s.to_vec()),
    };

    let payload = bincode::serialize(&response_payload)
        .map_err(|e| DelegateError::Other(format!("Failed to serialize response: {}", e)))?;

    let app_msg = ApplicationMessage::new(app_id, payload)
        .processed(true)
        .with_context(DelegateContext::default());

    Ok(vec![OutboundDelegateMsg::ApplicationMessage(app_msg)])
}

fn handle_put_contract_response(
    response: PutContractResponse,
) -> Result<Vec<OutboundDelegateMsg>, DelegateError> {
    let state = DelegateState::from_context(&response.context);

    let app_id = state
        .operation_context
        .and_then(|bytes| {
            ContractInstanceId::try_from(String::from_utf8(bytes).unwrap_or_default()).ok()
        })
        .unwrap_or_else(|| ContractInstanceId::new([0u8; 32]));

    let (success, error) = match response.result {
        Ok(()) => (true, None),
        Err(e) => (false, Some(e)),
    };

    let response_payload = DelegateResponse::ContractPutResult {
        contract_id: response.contract_id,
        success,
        error,
    };

    let payload = bincode::serialize(&response_payload)
        .map_err(|e| DelegateError::Other(format!("Failed to serialize response: {}", e)))?;

    let app_msg = ApplicationMessage::new(app_id, payload)
        .processed(true)
        .with_context(DelegateContext::default());

    Ok(vec![OutboundDelegateMsg::ApplicationMessage(app_msg)])
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper to call process with dummy ctx
    fn call_process(
        message: InboundDelegateMsg,
    ) -> Result<Vec<OutboundDelegateMsg>, DelegateError> {
        let mut ctx = DelegateCtx::default();
        TestDelegate::process(&mut ctx, Parameters::from(vec![]), None, message)
    }

    #[test]
    fn test_get_contract_state_request() {
        let contract_id = ContractInstanceId::new([1u8; 32]);
        let app_id = ContractInstanceId::new([2u8; 32]);

        let command = DelegateCommand::GetContractState { contract_id };
        let payload = bincode::serialize(&command).unwrap();
        let app_msg = ApplicationMessage::new(app_id, payload);

        let result = call_process(InboundDelegateMsg::ApplicationMessage(app_msg)).unwrap();

        assert_eq!(result.len(), 1);
        match &result[0] {
            OutboundDelegateMsg::GetContractRequest(req) => {
                assert_eq!(req.contract_id, contract_id);
                assert!(!req.processed);
            }
            other => panic!("Expected GetContractRequest, got {:?}", other),
        }
    }

    #[test]
    fn test_get_contract_response_with_state() {
        let contract_id = ContractInstanceId::new([1u8; 32]);
        let app_id = ContractInstanceId::new([2u8; 32]);

        // Build context as if we had made a request
        let state = DelegateState {
            pending_contract_get: Some(contract_id),
            operation_context: Some(app_id.to_string().into_bytes()),
            ..Default::default()
        };

        let response = GetContractResponse {
            contract_id,
            state: Some(WrappedState::new(vec![1, 2, 3, 4])),
            context: state.to_context(),
        };

        let result = call_process(InboundDelegateMsg::GetContractResponse(response)).unwrap();

        assert_eq!(result.len(), 1);
        match &result[0] {
            OutboundDelegateMsg::ApplicationMessage(msg) => {
                assert!(msg.processed);
                let response: DelegateResponse = bincode::deserialize(&msg.payload).unwrap();
                match response {
                    DelegateResponse::ContractState {
                        contract_id: id,
                        state,
                    } => {
                        assert_eq!(id, contract_id);
                        assert_eq!(state, Some(vec![1, 2, 3, 4]));
                    }
                    other => panic!("Expected ContractState, got {:?}", other),
                }
            }
            other => panic!("Expected ApplicationMessage, got {:?}", other),
        }
    }

    #[test]
    fn test_get_contract_response_not_found() {
        let contract_id = ContractInstanceId::new([1u8; 32]);
        let app_id = ContractInstanceId::new([2u8; 32]);

        let state = DelegateState {
            pending_contract_get: Some(contract_id),
            operation_context: Some(app_id.to_string().into_bytes()),
            ..Default::default()
        };

        let response = GetContractResponse {
            contract_id,
            state: None, // Contract not found
            context: state.to_context(),
        };

        let result = call_process(InboundDelegateMsg::GetContractResponse(response)).unwrap();

        assert_eq!(result.len(), 1);
        match &result[0] {
            OutboundDelegateMsg::ApplicationMessage(msg) => {
                let response: DelegateResponse = bincode::deserialize(&msg.payload).unwrap();
                match response {
                    DelegateResponse::ContractState { state, .. } => {
                        assert!(state.is_none());
                    }
                    other => panic!("Expected ContractState, got {:?}", other),
                }
            }
            other => panic!("Expected ApplicationMessage, got {:?}", other),
        }
    }

    #[test]
    fn test_get_multiple_contracts_empty() {
        let app_id = ContractInstanceId::new([2u8; 32]);

        let command = DelegateCommand::GetMultipleContractStates {
            contract_ids: vec![],
        };
        let payload = bincode::serialize(&command).unwrap();
        let app_msg = ApplicationMessage::new(app_id, payload);

        let result = call_process(InboundDelegateMsg::ApplicationMessage(app_msg)).unwrap();

        // Should return empty results immediately
        assert_eq!(result.len(), 1);
        match &result[0] {
            OutboundDelegateMsg::ApplicationMessage(msg) => {
                assert!(msg.processed);
                let response: DelegateResponse = bincode::deserialize(&msg.payload).unwrap();
                match response {
                    DelegateResponse::MultipleContractStates { results } => {
                        assert!(results.is_empty());
                    }
                    other => panic!("Expected MultipleContractStates, got {:?}", other),
                }
            }
            other => panic!("Expected ApplicationMessage, got {:?}", other),
        }
    }

    #[test]
    fn test_get_multiple_contracts_first_request() {
        let contract1 = ContractInstanceId::new([1u8; 32]);
        let contract2 = ContractInstanceId::new([2u8; 32]);
        let contract3 = ContractInstanceId::new([3u8; 32]);
        let app_id = ContractInstanceId::new([10u8; 32]);

        let command = DelegateCommand::GetMultipleContractStates {
            contract_ids: vec![contract1, contract2, contract3],
        };
        let payload = bincode::serialize(&command).unwrap();
        let app_msg = ApplicationMessage::new(app_id, payload);

        let result = call_process(InboundDelegateMsg::ApplicationMessage(app_msg)).unwrap();

        // Should emit GetContractRequest for the first contract
        assert_eq!(result.len(), 1);
        match &result[0] {
            OutboundDelegateMsg::GetContractRequest(req) => {
                assert_eq!(req.contract_id, contract1);
                assert!(!req.processed);
                // Verify remaining contracts are in context
                let state = DelegateState::from_context(&req.context);
                assert_eq!(state.remaining_contracts.len(), 2);
                assert_eq!(state.remaining_contracts[0], contract2);
                assert_eq!(state.remaining_contracts[1], contract3);
            }
            other => panic!("Expected GetContractRequest, got {:?}", other),
        }
    }

    #[test]
    fn test_get_multiple_contracts_response_continues() {
        let contract1 = ContractInstanceId::new([1u8; 32]);
        let contract2 = ContractInstanceId::new([2u8; 32]);
        let contract3 = ContractInstanceId::new([3u8; 32]);
        let app_id = ContractInstanceId::new([10u8; 32]);

        // Simulate state after first request
        let state = DelegateState {
            pending_contract_get: Some(contract1),
            operation_context: Some(app_id.to_string().into_bytes()),
            remaining_contracts: vec![contract2, contract3],
            accumulated_results: vec![],
            echo_message: None,
        };

        let response = GetContractResponse {
            contract_id: contract1,
            state: Some(WrappedState::new(vec![1, 1, 1])),
            context: state.to_context(),
        };

        let result = call_process(InboundDelegateMsg::GetContractResponse(response)).unwrap();

        // Should emit GetContractRequest for the second contract
        assert_eq!(result.len(), 1);
        match &result[0] {
            OutboundDelegateMsg::GetContractRequest(req) => {
                assert_eq!(req.contract_id, contract2);
                // Verify accumulated results and remaining
                let state = DelegateState::from_context(&req.context);
                assert_eq!(state.accumulated_results.len(), 1);
                assert_eq!(state.accumulated_results[0].0, contract1);
                assert_eq!(state.remaining_contracts.len(), 1);
                assert_eq!(state.remaining_contracts[0], contract3);
            }
            other => panic!("Expected GetContractRequest, got {:?}", other),
        }
    }

    #[test]
    fn test_get_multiple_contracts_final_response() {
        let contract1 = ContractInstanceId::new([1u8; 32]);
        let contract2 = ContractInstanceId::new([2u8; 32]);
        let contract3 = ContractInstanceId::new([3u8; 32]);
        let app_id = ContractInstanceId::new([10u8; 32]);

        // Simulate state after second response (last contract pending)
        let state = DelegateState {
            pending_contract_get: Some(contract3),
            operation_context: Some(app_id.to_string().into_bytes()),
            remaining_contracts: vec![], // No more contracts
            accumulated_results: vec![
                (contract1, Some(vec![1, 1, 1])),
                (contract2, Some(vec![2, 2, 2])),
            ],
            echo_message: None,
        };

        let response = GetContractResponse {
            contract_id: contract3,
            state: Some(WrappedState::new(vec![3, 3, 3])),
            context: state.to_context(),
        };

        let result = call_process(InboundDelegateMsg::GetContractResponse(response)).unwrap();

        // Should return accumulated results
        assert_eq!(result.len(), 1);
        match &result[0] {
            OutboundDelegateMsg::ApplicationMessage(msg) => {
                assert!(msg.processed);
                let response: DelegateResponse = bincode::deserialize(&msg.payload).unwrap();
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
                    other => panic!("Expected MultipleContractStates, got {:?}", other),
                }
            }
            other => panic!("Expected ApplicationMessage, got {:?}", other),
        }
    }

    #[test]
    fn test_get_contract_with_echo() {
        let contract_id = ContractInstanceId::new([1u8; 32]);
        let app_id = ContractInstanceId::new([2u8; 32]);
        let echo_message = "Hello from test!".to_string();

        let command = DelegateCommand::GetContractWithEcho {
            contract_id,
            echo_message: echo_message.clone(),
        };
        let payload = bincode::serialize(&command).unwrap();
        let app_msg = ApplicationMessage::new(app_id, payload);

        let result = call_process(InboundDelegateMsg::ApplicationMessage(app_msg)).unwrap();

        // Should emit BOTH GetContractRequest AND Echo message
        assert_eq!(result.len(), 2);

        // First message should be GetContractRequest
        match &result[0] {
            OutboundDelegateMsg::GetContractRequest(req) => {
                assert_eq!(req.contract_id, contract_id);
                assert!(!req.processed);
            }
            other => panic!("Expected GetContractRequest, got {:?}", other),
        }

        // Second message should be Echo ApplicationMessage
        match &result[1] {
            OutboundDelegateMsg::ApplicationMessage(msg) => {
                assert!(msg.processed);
                let response: DelegateResponse = bincode::deserialize(&msg.payload).unwrap();
                match response {
                    DelegateResponse::Echo { message } => {
                        assert_eq!(message, echo_message);
                    }
                    other => panic!("Expected Echo, got {:?}", other),
                }
            }
            other => panic!("Expected ApplicationMessage, got {:?}", other),
        }
    }

    fn make_test_contract() -> ContractContainer {
        use std::sync::Arc;
        let code = ContractCode::from(vec![0u8; 10]);
        let params = Parameters::from(vec![]);
        let wrapped = WrappedContract::new(Arc::new(code), params);
        ContractContainer::Wasm(ContractWasmAPIVersion::V1(wrapped))
    }

    #[test]
    fn test_put_contract_request() {
        let app_id = ContractInstanceId::new([2u8; 32]);
        let contract = make_test_contract();
        let contract_key = contract.key();

        let command = DelegateCommand::PutContractState {
            contract,
            state: vec![1, 2, 3, 4],
        };
        let payload = bincode::serialize(&command).unwrap();
        let app_msg = ApplicationMessage::new(app_id, payload);

        let result = call_process(InboundDelegateMsg::ApplicationMessage(app_msg)).unwrap();

        assert_eq!(result.len(), 1);
        match &result[0] {
            OutboundDelegateMsg::PutContractRequest(req) => {
                assert_eq!(req.contract.key(), contract_key);
                assert_eq!(req.state.as_ref(), &[1, 2, 3, 4]);
                assert!(!req.processed);
            }
            other => panic!("Expected PutContractRequest, got {:?}", other),
        }
    }

    #[test]
    fn test_put_contract_response_success() {
        let contract_id = ContractInstanceId::new([1u8; 32]);
        let app_id = ContractInstanceId::new([2u8; 32]);

        let state = DelegateState {
            pending_contract_get: None,
            operation_context: Some(app_id.to_string().into_bytes()),
            ..Default::default()
        };

        let response = PutContractResponse {
            contract_id,
            result: Ok(()),
            context: state.to_context(),
        };

        let result = call_process(InboundDelegateMsg::PutContractResponse(response)).unwrap();

        assert_eq!(result.len(), 1);
        match &result[0] {
            OutboundDelegateMsg::ApplicationMessage(msg) => {
                assert!(msg.processed);
                let response: DelegateResponse = bincode::deserialize(&msg.payload).unwrap();
                match response {
                    DelegateResponse::ContractPutResult {
                        contract_id: id,
                        success,
                        error,
                    } => {
                        assert_eq!(id, contract_id);
                        assert!(success);
                        assert!(error.is_none());
                    }
                    other => panic!("Expected ContractPutResult, got {:?}", other),
                }
            }
            other => panic!("Expected ApplicationMessage, got {:?}", other),
        }
    }

    #[test]
    fn test_put_contract_response_error() {
        let contract_id = ContractInstanceId::new([1u8; 32]);
        let app_id = ContractInstanceId::new([2u8; 32]);

        let state = DelegateState {
            pending_contract_get: None,
            operation_context: Some(app_id.to_string().into_bytes()),
            ..Default::default()
        };

        let response = PutContractResponse {
            contract_id,
            result: Err("contract validation failed".to_string()),
            context: state.to_context(),
        };

        let result = call_process(InboundDelegateMsg::PutContractResponse(response)).unwrap();

        assert_eq!(result.len(), 1);
        match &result[0] {
            OutboundDelegateMsg::ApplicationMessage(msg) => {
                let response: DelegateResponse = bincode::deserialize(&msg.payload).unwrap();
                match response {
                    DelegateResponse::ContractPutResult {
                        success, error, ..
                    } => {
                        assert!(!success);
                        assert_eq!(error, Some("contract validation failed".to_string()));
                    }
                    other => panic!("Expected ContractPutResult, got {:?}", other),
                }
            }
            other => panic!("Expected ApplicationMessage, got {:?}", other),
        }
    }
}

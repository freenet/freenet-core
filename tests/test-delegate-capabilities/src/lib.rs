//! Test delegate for exercising all delegate capabilities from issue #2827.
//!
//! This delegate responds to different command messages to test:
//! - #2828: Contract GET (GetContractRequest/GetContractResponse)
//! - #2829: Contract PUT (future)
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
    GetContractState {
        contract_id: ContractInstanceId,
    },

    // Future capabilities for #2827:
    // PutContractState { contract_id: ContractInstanceId, state: Vec<u8> },
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
    /// Error occurred
    Error {
        message: String,
    },

    // Future responses:
    // ContractPutResult { contract_id: ContractInstanceId, success: bool },
    // SubscriptionConfirmed { contract_id: ContractInstanceId },
    // UpdateApplied { contract_id: ContractInstanceId },
    // DelegateRegistered { delegate_key: Vec<u8> },
    // DelegateUnregistered { delegate_key: Vec<u8> },
}

struct TestDelegate;

#[delegate]
impl DelegateInterface for TestDelegate {
    fn process(
        _params: Parameters<'static>,
        _attested: Option<&'static [u8]>,
        message: InboundDelegateMsg,
    ) -> Result<Vec<OutboundDelegateMsg>, DelegateError> {
        match message {
            InboundDelegateMsg::ApplicationMessage(app_msg) => {
                handle_application_message(app_msg)
            }
            InboundDelegateMsg::GetContractResponse(response) => {
                handle_contract_response(response)
            }
            InboundDelegateMsg::GetSecretResponse(_) => {
                // Not used by this delegate
                Ok(vec![])
            }
            InboundDelegateMsg::UserResponse(_) => {
                // Not used by this delegate
                Ok(vec![])
            }
            InboundDelegateMsg::GetSecretRequest(_) => {
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
                pending_contract_get: Some(contract_id.clone()),
                operation_context: Some(app_msg.app.as_bytes().to_vec()),
            };

            // Emit GetContractRequest
            let request = GetContractRequest {
                contract_id,
                context: state.to_context(),
                processed: false,
            };

            Ok(vec![OutboundDelegateMsg::GetContractRequest(request)])
        }
    }
}

fn handle_contract_response(
    response: GetContractResponse,
) -> Result<Vec<OutboundDelegateMsg>, DelegateError> {
    // Restore state from context
    let state = DelegateState::from_context(&response.context);

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
        .map(|bytes| {
            ContractInstanceId::try_from(
                String::from_utf8(bytes).unwrap_or_default()
            ).ok()
        })
        .flatten()
        .unwrap_or_else(|| ContractInstanceId::new([0u8; 32]));

    // Build response
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_contract_state_request() {
        let contract_id = ContractInstanceId::new([1u8; 32]);
        let app_id = ContractInstanceId::new([2u8; 32]);

        let command = DelegateCommand::GetContractState {
            contract_id: contract_id.clone(),
        };
        let payload = bincode::serialize(&command).unwrap();
        let app_msg = ApplicationMessage::new(app_id.clone(), payload);

        let result = TestDelegate::process(
            Parameters::from(vec![]),
            None,
            InboundDelegateMsg::ApplicationMessage(app_msg),
        )
        .unwrap();

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
            pending_contract_get: Some(contract_id.clone()),
            operation_context: Some(app_id.to_string().into_bytes()),
        };

        let response = GetContractResponse {
            contract_id: contract_id.clone(),
            state: Some(WrappedState::new(vec![1, 2, 3, 4])),
            context: state.to_context(),
        };

        let result = TestDelegate::process(
            Parameters::from(vec![]),
            None,
            InboundDelegateMsg::GetContractResponse(response),
        )
        .unwrap();

        assert_eq!(result.len(), 1);
        match &result[0] {
            OutboundDelegateMsg::ApplicationMessage(msg) => {
                assert!(msg.processed);
                let response: DelegateResponse = bincode::deserialize(&msg.payload).unwrap();
                match response {
                    DelegateResponse::ContractState { contract_id: id, state } => {
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
            pending_contract_get: Some(contract_id.clone()),
            operation_context: Some(app_id.to_string().into_bytes()),
        };

        let response = GetContractResponse {
            contract_id: contract_id.clone(),
            state: None, // Contract not found
            context: state.to_context(),
        };

        let result = TestDelegate::process(
            Parameters::from(vec![]),
            None,
            InboundDelegateMsg::GetContractResponse(response),
        )
        .unwrap();

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
}

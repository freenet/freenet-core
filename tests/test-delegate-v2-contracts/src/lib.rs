/// V2 test delegate that exercises contract state host functions.
///
/// This delegate imports functions from the `freenet_delegate_contracts` namespace,
/// which makes it a V2 delegate (detected by inspecting WASM module imports).
/// The host registers these as async functions, so the runtime uses
/// `call_3i64_async_imports` instead of `call_3i64` for this delegate.
use freenet_stdlib::prelude::*;
use serde::{Deserialize, Serialize};

// -- V2 contract access host functions --
//
// These are provided by the Freenet runtime when state_store_db is configured.
// Importing from this namespace is what makes this a V2 delegate.
#[cfg(target_family = "wasm")]
#[link(wasm_import_module = "freenet_delegate_contracts")]
extern "C" {
    /// Returns the byte length of the contract's state, or a negative error code.
    fn __frnt__delegate__get_contract_state_len(id_ptr: i64, id_len: i32) -> i64;
    /// Copies the contract state into the buffer at out_ptr. Returns bytes written,
    /// or a negative error code.
    fn __frnt__delegate__get_contract_state(
        id_ptr: i64,
        id_len: i32,
        out_ptr: i64,
        out_len: i64,
    ) -> i64;
    /// Stores contract state. Returns 0 on success, negative error code on failure.
    fn __frnt__delegate__put_contract_state(
        id_ptr: i64,
        id_len: i32,
        state_ptr: i64,
        state_len: i64,
    ) -> i64;
    /// Updates contract state (must already exist). Returns 0 on success.
    fn __frnt__delegate__update_contract_state(
        id_ptr: i64,
        id_len: i32,
        state_ptr: i64,
        state_len: i64,
    ) -> i64;
    /// Subscribes to contract state changes. Returns 0 on success.
    fn __frnt__delegate__subscribe_contract(id_ptr: i64, id_len: i32) -> i64;
}

// -- Application message types --

#[derive(Debug, Serialize, Deserialize)]
pub enum InboundAppMessage {
    /// Request the state of a contract by its 32-byte instance ID.
    GetContractState { contract_id: [u8; 32] },
    /// PUT state for a contract.
    PutContractState {
        contract_id: [u8; 32],
        state: Vec<u8>,
    },
    /// UPDATE state for a contract (must already exist).
    UpdateContractState {
        contract_id: [u8; 32],
        state: Vec<u8>,
    },
    /// Subscribe to a contract.
    SubscribeContract { contract_id: [u8; 32] },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum OutboundAppMessage {
    /// Contract state was found.
    ContractState {
        contract_id: [u8; 32],
        state: Vec<u8>,
    },
    /// Contract was not found (negative error code from host).
    ContractNotFound {
        contract_id: [u8; 32],
        error_code: i64,
    },
    /// Operation succeeded (PUT, UPDATE, SUBSCRIBE).
    Success { contract_id: [u8; 32] },
    /// Operation failed with error code.
    Failed {
        contract_id: [u8; 32],
        error_code: i64,
    },
}

// -- Delegate implementation --

struct Delegate;

#[delegate]
impl DelegateInterface for Delegate {
    fn process(
        _ctx: &mut DelegateCtx,
        _params: Parameters<'static>,
        _attested: Option<&'static [u8]>,
        messages: InboundDelegateMsg,
    ) -> Result<Vec<OutboundDelegateMsg>, DelegateError> {
        match messages {
            InboundDelegateMsg::ApplicationMessage(incoming_app) => {
                let message: InboundAppMessage =
                    bincode::deserialize(incoming_app.payload.as_slice())
                        .map_err(|err| DelegateError::Other(format!("{err}")))?;

                let response = match message {
                    InboundAppMessage::GetContractState { contract_id } => {
                        read_contract_state(contract_id)
                    }
                    InboundAppMessage::PutContractState { contract_id, state } => {
                        put_contract_state(contract_id, &state)
                    }
                    InboundAppMessage::UpdateContractState { contract_id, state } => {
                        update_contract_state(contract_id, &state)
                    }
                    InboundAppMessage::SubscribeContract { contract_id } => {
                        subscribe_contract(contract_id)
                    }
                };

                let payload = bincode::serialize(&response)
                    .map_err(|err| DelegateError::Other(format!("{err}")))?;
                let response_msg =
                    ApplicationMessage::new(incoming_app.app, payload).processed(true);
                Ok(vec![OutboundDelegateMsg::ApplicationMessage(response_msg)])
            }
            _ => Err(DelegateError::Other(
                "Unexpected inbound message".to_string(),
            )),
        }
    }
}

/// Read contract state using V2 host functions.
fn read_contract_state(contract_id: [u8; 32]) -> OutboundAppMessage {
    #[cfg(target_family = "wasm")]
    {
        let id_ptr = contract_id.as_ptr() as i64;
        let id_len = 32i32;

        let state_len = unsafe { __frnt__delegate__get_contract_state_len(id_ptr, id_len) };

        if state_len < 0 {
            return OutboundAppMessage::ContractNotFound {
                contract_id,
                error_code: state_len,
            };
        }

        let mut buf = vec![0u8; state_len as usize];
        let bytes_read = unsafe {
            __frnt__delegate__get_contract_state(
                id_ptr,
                id_len,
                buf.as_mut_ptr() as i64,
                buf.len() as i64,
            )
        };

        if bytes_read < 0 {
            return OutboundAppMessage::ContractNotFound {
                contract_id,
                error_code: bytes_read,
            };
        }

        buf.truncate(bytes_read as usize);
        OutboundAppMessage::ContractState {
            contract_id,
            state: buf,
        }
    }

    #[cfg(not(target_family = "wasm"))]
    {
        OutboundAppMessage::ContractNotFound {
            contract_id,
            error_code: -99,
        }
    }
}

/// PUT contract state using V2 host function.
fn put_contract_state(contract_id: [u8; 32], state: &[u8]) -> OutboundAppMessage {
    #[cfg(target_family = "wasm")]
    {
        let result = unsafe {
            __frnt__delegate__put_contract_state(
                contract_id.as_ptr() as i64,
                32,
                state.as_ptr() as i64,
                state.len() as i64,
            )
        };
        if result == 0 {
            OutboundAppMessage::Success { contract_id }
        } else {
            OutboundAppMessage::Failed {
                contract_id,
                error_code: result,
            }
        }
    }

    #[cfg(not(target_family = "wasm"))]
    {
        let _ = state;
        OutboundAppMessage::Failed {
            contract_id,
            error_code: -99,
        }
    }
}

/// UPDATE contract state using V2 host function.
fn update_contract_state(contract_id: [u8; 32], state: &[u8]) -> OutboundAppMessage {
    #[cfg(target_family = "wasm")]
    {
        let result = unsafe {
            __frnt__delegate__update_contract_state(
                contract_id.as_ptr() as i64,
                32,
                state.as_ptr() as i64,
                state.len() as i64,
            )
        };
        if result == 0 {
            OutboundAppMessage::Success { contract_id }
        } else {
            OutboundAppMessage::Failed {
                contract_id,
                error_code: result,
            }
        }
    }

    #[cfg(not(target_family = "wasm"))]
    {
        let _ = state;
        OutboundAppMessage::Failed {
            contract_id,
            error_code: -99,
        }
    }
}

/// SUBSCRIBE to contract using V2 host function.
fn subscribe_contract(contract_id: [u8; 32]) -> OutboundAppMessage {
    #[cfg(target_family = "wasm")]
    {
        let result =
            unsafe { __frnt__delegate__subscribe_contract(contract_id.as_ptr() as i64, 32) };
        if result == 0 {
            OutboundAppMessage::Success { contract_id }
        } else {
            OutboundAppMessage::Failed {
                contract_id,
                error_code: result,
            }
        }
    }

    #[cfg(not(target_family = "wasm"))]
    {
        OutboundAppMessage::Failed {
            contract_id,
            error_code: -99,
        }
    }
}

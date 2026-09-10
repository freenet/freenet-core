/// Test delegate that reads contract state through the
/// `__frnt__delegate__get_contract_state` host functions.
///
/// That pair is the only contract-state host function a delegate has, and it
/// only reports what THIS NODE already holds. The write and subscribe host
/// functions this fixture used to exercise were removed in freenet-core#5637;
/// a delegate writes contract state by emitting
/// `OutboundDelegateMsg::{PutContractRequest, UpdateContractRequest}`.
use freenet_stdlib::prelude::*;
use serde::{Deserialize, Serialize};

#[cfg(target_family = "wasm")]
#[link(wasm_import_module = "freenet_delegate_contracts")]
unsafe extern "C" {
    /// Returns the byte length of the state this node holds for the contract,
    /// or a negative error code.
    fn __frnt__delegate__get_contract_state_len(id_ptr: i64, id_len: i32) -> i64;
    /// Copies that state into the buffer at out_ptr. Returns bytes written, or
    /// a negative error code.
    fn __frnt__delegate__get_contract_state(
        id_ptr: i64,
        id_len: i32,
        out_ptr: i64,
        out_len: i64,
    ) -> i64;
}

// -- Application message types --

#[derive(Debug, Serialize, Deserialize)]
pub enum InboundAppMessage {
    /// Request the state this node holds for a contract, by its 32-byte
    /// instance ID.
    GetContractState { contract_id: [u8; 32] },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum OutboundAppMessage {
    /// This node holds the contract; here is its state.
    ContractState {
        contract_id: [u8; 32],
        state: Vec<u8>,
    },
    /// The host returned a negative error code (for example, this node does
    /// not hold the contract).
    ContractNotFound {
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
        _origin: Option<MessageOrigin>,
        messages: InboundDelegateMsg,
    ) -> Result<Vec<OutboundDelegateMsg>, DelegateError> {
        match messages {
            InboundDelegateMsg::ApplicationMessage(incoming_app) => {
                let message: InboundAppMessage =
                    bincode::deserialize(incoming_app.payload.as_slice())
                        .map_err(|err| DelegateError::Other(format!("{err}")))?;

                let response = match message {
                    InboundAppMessage::GetContractState { contract_id } => {
                        read_local_contract_state(contract_id)
                    }
                };

                let payload = bincode::serialize(&response)
                    .map_err(|err| DelegateError::Other(format!("{err}")))?;
                let response_msg = ApplicationMessage::new(payload).processed(true);
                Ok(vec![OutboundDelegateMsg::ApplicationMessage(response_msg)])
            }
            _ => Err(DelegateError::Other(
                "Unexpected inbound message".to_string(),
            )),
        }
    }
}

/// Read the state this node holds for a contract.
fn read_local_contract_state(contract_id: [u8; 32]) -> OutboundAppMessage {
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

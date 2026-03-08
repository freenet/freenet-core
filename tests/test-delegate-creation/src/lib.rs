use freenet_stdlib::prelude::*;
use serde::{Deserialize, Serialize};

/// Default cipher key (same as DelegateRequest::DEFAULT_CIPHER)
const DEFAULT_CIPHER: [u8; 32] = [
    0, 24, 22, 150, 112, 207, 24, 65, 182, 161, 169, 227, 66, 182, 237, 215, 206, 164, 58, 161,
    64, 108, 157, 195, 0, 0, 0, 0, 0, 0, 0, 0,
];

/// Default nonce (same as DelegateRequest::DEFAULT_NONCE)
const DEFAULT_NONCE: [u8; 24] = [
    57, 18, 79, 116, 63, 134, 93, 39, 208, 161, 156, 229, 222, 247, 111, 79, 210, 126, 127, 55,
    224, 150, 139, 80,
];

// --- Guest-side host function for delegate creation ---
// These extern declarations match the host functions registered in
// crates/core/src/wasm_runtime/engine/wasmtime_engine.rs under the
// "freenet_delegate_management" namespace. Defined inline here to avoid
// requiring a freenet-stdlib fork for this experimental feature.

#[cfg(target_family = "wasm")]
#[link(wasm_import_module = "freenet_delegate_management")]
extern "C" {
    fn __frnt__delegate__create_delegate(
        wasm_ptr: i64,
        wasm_len: i64,
        params_ptr: i64,
        params_len: i32,
        cipher_ptr: i64,
        nonce_ptr: i64,
        out_key_ptr: i64,
        out_hash_ptr: i64,
    ) -> i32;
}

/// Call the host to create a new child delegate from WASM bytes + parameters.
/// Returns `Ok((key_hash, code_hash))` on success, `Err(error_code)` on failure.
#[cfg(target_family = "wasm")]
fn create_delegate(
    ctx: &mut DelegateCtx,
    wasm_code: &[u8],
    params: &[u8],
    cipher: &[u8; 32],
    nonce: &[u8; 24],
) -> Result<([u8; 32], [u8; 32]), i32> {
    let _ = ctx; // ctx ensures we're inside process()
    let mut key_buf = [0u8; 32];
    let mut hash_buf = [0u8; 32];
    let result = unsafe {
        __frnt__delegate__create_delegate(
            wasm_code.as_ptr() as i64,
            wasm_code.len() as i64,
            params.as_ptr() as i64,
            params.len() as i32,
            cipher.as_ptr() as i64,
            nonce.as_ptr() as i64,
            key_buf.as_mut_ptr() as i64,
            hash_buf.as_mut_ptr() as i64,
        )
    };
    if result == 0 {
        Ok((key_buf, hash_buf))
    } else {
        Err(result)
    }
}

/// Non-WASM stub for native compilation (unit tests).
#[cfg(not(target_family = "wasm"))]
fn create_delegate(
    _ctx: &mut DelegateCtx,
    _wasm_code: &[u8],
    _params: &[u8],
    _cipher: &[u8; 32],
    _nonce: &[u8; 24],
) -> Result<([u8; 32], [u8; 32]), i32> {
    Err(-1) // Not available outside WASM
}

#[derive(Debug, Serialize, Deserialize)]
pub enum InboundAppMessage {
    /// Create a child delegate from the provided WASM bytes and params.
    CreateChildDelegate {
        child_wasm: Vec<u8>,
        child_params: Vec<u8>,
    },
    /// Simple ping for sanity checks.
    Ping { data: Vec<u8> },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum OutboundAppMessage {
    /// Successfully created a child delegate.
    ChildCreated {
        key_bytes: Vec<u8>,
        code_hash_bytes: Vec<u8>,
    },
    /// Failed to create a child delegate.
    CreateFailed { error_code: i32 },
    /// Ping response.
    PingResponse { data: Vec<u8> },
}

struct Delegate;

#[delegate]
impl DelegateInterface for Delegate {
    fn process(
        ctx: &mut DelegateCtx,
        _params: Parameters<'static>,
        _attested: Option<&'static [u8]>,
        messages: InboundDelegateMsg,
    ) -> Result<Vec<OutboundDelegateMsg>, DelegateError> {
        match messages {
            InboundDelegateMsg::ApplicationMessage(incoming_app) => {
                let message: InboundAppMessage =
                    bincode::deserialize(incoming_app.payload.as_slice())
                        .map_err(|err| DelegateError::Other(format!("{err}")))?;

                match message {
                    InboundAppMessage::CreateChildDelegate {
                        child_wasm,
                        child_params,
                    } => {
                        let result = create_delegate(
                            ctx,
                            &child_wasm,
                            &child_params,
                            &DEFAULT_CIPHER,
                            &DEFAULT_NONCE,
                        );

                        let response_payload = match result {
                            Ok((key_bytes, code_hash_bytes)) => {
                                bincode::serialize(&OutboundAppMessage::ChildCreated {
                                    key_bytes: key_bytes.to_vec(),
                                    code_hash_bytes: code_hash_bytes.to_vec(),
                                })
                            }
                            Err(error_code) => {
                                bincode::serialize(&OutboundAppMessage::CreateFailed { error_code })
                            }
                        }
                        .map_err(|err| DelegateError::Other(format!("{err}")))?;

                        let response =
                            ApplicationMessage::new(incoming_app.app, response_payload)
                                .processed(true);
                        Ok(vec![OutboundDelegateMsg::ApplicationMessage(response)])
                    }
                    InboundAppMessage::Ping { data } => {
                        let response_payload =
                            bincode::serialize(&OutboundAppMessage::PingResponse { data })
                                .map_err(|err| DelegateError::Other(format!("{err}")))?;
                        let response =
                            ApplicationMessage::new(incoming_app.app, response_payload)
                                .processed(true);
                        Ok(vec![OutboundDelegateMsg::ApplicationMessage(response)])
                    }
                }
            }
            _ => Ok(vec![]),
        }
    }
}

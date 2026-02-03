/// Test delegate that uses the new host function API for context and secret access.
///
/// This delegate demonstrates the simplified pattern where:
/// - Context is read/written via `__frnt__delegate__ctx_*` host functions
/// - Secrets are fetched synchronously via `__frnt__delegate__get_secret`
/// - No GetSecretRequest/GetSecretResponse round-trip is needed
/// - The delegate handles everything in a single `process()` call
use freenet_stdlib::prelude::*;
use serde::{Deserialize, Serialize};

// -- Host function declarations (will be provided by the runtime) --

#[link(wasm_import_module = "freenet_delegate_ctx")]
extern "C" {
    /// Returns the current context length in bytes.
    fn __frnt__delegate__ctx_len() -> i32;
    /// Reads context into the buffer at `ptr` (max `len` bytes). Returns bytes written.
    fn __frnt__delegate__ctx_read(ptr: i64, len: i32) -> i32;
    /// Writes `len` bytes from `ptr` into the context, replacing existing content.
    fn __frnt__delegate__ctx_write(ptr: i64, len: i32);
}

#[link(wasm_import_module = "freenet_delegate_secrets")]
extern "C" {
    /// Get a secret. Returns bytes written to `out_ptr`, or -1 if not found.
    fn __frnt__delegate__get_secret(key_ptr: i64, key_len: i32, out_ptr: i64, out_len: i32) -> i32;
    /// Store a secret. Returns 0 on success, -1 on error.
    fn __frnt__delegate__set_secret(key_ptr: i64, key_len: i32, val_ptr: i64, val_len: i32) -> i32;
    /// Check if a secret exists. Returns 1 if yes, 0 if no.
    fn __frnt__delegate__has_secret(key_ptr: i64, key_len: i32) -> i32;
}

// -- Safe wrappers around the host functions --

fn ctx_read_bytes() -> Vec<u8> {
    let len = unsafe { __frnt__delegate__ctx_len() };
    if len <= 0 {
        return Vec::new();
    }
    let mut buf = vec![0u8; len as usize];
    let read = unsafe { __frnt__delegate__ctx_read(buf.as_mut_ptr() as i64, len) };
    buf.truncate(read as usize);
    buf
}

fn ctx_write_bytes(data: &[u8]) {
    unsafe {
        __frnt__delegate__ctx_write(data.as_ptr() as i64, data.len() as i32);
    }
}

fn host_get_secret(key: &[u8]) -> Option<Vec<u8>> {
    let mut out = vec![0u8; 4096]; // generous buffer
    let result = unsafe {
        __frnt__delegate__get_secret(
            key.as_ptr() as i64,
            key.len() as i32,
            out.as_mut_ptr() as i64,
            out.len() as i32,
        )
    };
    if result < 0 {
        None
    } else {
        out.truncate(result as usize);
        Some(out)
    }
}

fn host_set_secret(key: &[u8], value: &[u8]) -> bool {
    let result = unsafe {
        __frnt__delegate__set_secret(
            key.as_ptr() as i64,
            key.len() as i32,
            value.as_ptr() as i64,
            value.len() as i32,
        )
    };
    result == 0
}

fn host_has_secret(key: &[u8]) -> bool {
    let result = unsafe { __frnt__delegate__has_secret(key.as_ptr() as i64, key.len() as i32) };
    result == 1
}

// -- Application message types --

const PRIVATE_KEY: [u8; 3] = [1, 2, 3];
const PUB_KEY: [u8; 1] = [1];

#[derive(Debug, Serialize, Deserialize)]
pub enum InboundAppMessage {
    /// Original test: create inbox and store secret
    CreateInboxRequest,
    /// Original test: sign message using stored secret
    PleaseSignMessage(Vec<u8>),
    /// New: Write data to context
    WriteContext(Vec<u8>),
    /// New: Read context and return it
    ReadContext,
    /// New: Increment a counter stored in context (tests read-modify-write)
    IncrementCounter,
    /// New: Check if a secret exists
    HasSecret(Vec<u8>),
    /// New: Try to get a non-existent secret (should return error info)
    GetNonExistentSecret(Vec<u8>),
    /// New: Store a secret with given key and value
    StoreSecret { key: Vec<u8>, value: Vec<u8> },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum OutboundAppMessage {
    CreateInboxResponse(Vec<u8>),
    MessageSigned(Vec<u8>),
    /// New: Context data read back
    ContextData(Vec<u8>),
    /// New: Counter value after increment
    CounterValue(u32),
    /// New: Whether secret exists
    SecretExists(bool),
    /// New: Result of getting a secret (None if not found)
    SecretResult(Option<Vec<u8>>),
    /// New: Acknowledgement of context write
    ContextWritten,
    /// New: Acknowledgement of secret store
    SecretStored,
}

// -- Delegate implementation --

struct Delegate;

#[delegate]
impl DelegateInterface for Delegate {
    fn process(
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
                    InboundAppMessage::CreateInboxRequest => {
                        // Store the secret directly via host function
                        host_set_secret(&PRIVATE_KEY, &PRIVATE_KEY);

                        let response_msg_content =
                            OutboundAppMessage::CreateInboxResponse(PUB_KEY.to_vec());
                        let payload = bincode::serialize(&response_msg_content)
                            .map_err(|err| DelegateError::Other(format!("{err}")))?;
                        let response =
                            ApplicationMessage::new(incoming_app.app, payload).processed(true);
                        Ok(vec![OutboundDelegateMsg::ApplicationMessage(response)])
                    }

                    InboundAppMessage::PleaseSignMessage(inbox_priv_key) => {
                        // Fetch the secret directly — no round-trip needed!
                        let _secret = host_get_secret(&inbox_priv_key)
                            .ok_or_else(|| DelegateError::Other("Secret not found".into()))?;

                        let signature = vec![4, 5, 2];
                        let response_msg_content = OutboundAppMessage::MessageSigned(signature);
                        let payload = bincode::serialize(&response_msg_content)
                            .map_err(|err| DelegateError::Other(format!("{err}")))?;
                        let response =
                            ApplicationMessage::new(incoming_app.app, payload).processed(true);
                        Ok(vec![OutboundDelegateMsg::ApplicationMessage(response)])
                    }

                    InboundAppMessage::WriteContext(data) => {
                        // Write data to context via host function
                        ctx_write_bytes(&data);

                        let response_msg_content = OutboundAppMessage::ContextWritten;
                        let payload = bincode::serialize(&response_msg_content)
                            .map_err(|err| DelegateError::Other(format!("{err}")))?;
                        let response =
                            ApplicationMessage::new(incoming_app.app, payload).processed(true);
                        Ok(vec![OutboundDelegateMsg::ApplicationMessage(response)])
                    }

                    InboundAppMessage::ReadContext => {
                        // Read context via host function
                        let data = ctx_read_bytes();

                        let response_msg_content = OutboundAppMessage::ContextData(data);
                        let payload = bincode::serialize(&response_msg_content)
                            .map_err(|err| DelegateError::Other(format!("{err}")))?;
                        let response =
                            ApplicationMessage::new(incoming_app.app, payload).processed(true);
                        Ok(vec![OutboundDelegateMsg::ApplicationMessage(response)])
                    }

                    InboundAppMessage::IncrementCounter => {
                        // Read current counter from context, increment, write back
                        let ctx = ctx_read_bytes();
                        let current: u32 = if ctx.is_empty() {
                            0
                        } else {
                            bincode::deserialize(&ctx).unwrap_or(0)
                        };
                        let new_value = current + 1;
                        let new_ctx = bincode::serialize(&new_value)
                            .map_err(|err| DelegateError::Other(format!("{err}")))?;
                        ctx_write_bytes(&new_ctx);

                        let response_msg_content = OutboundAppMessage::CounterValue(new_value);
                        let payload = bincode::serialize(&response_msg_content)
                            .map_err(|err| DelegateError::Other(format!("{err}")))?;
                        let response =
                            ApplicationMessage::new(incoming_app.app, payload).processed(true);
                        Ok(vec![OutboundDelegateMsg::ApplicationMessage(response)])
                    }

                    InboundAppMessage::HasSecret(key) => {
                        let exists = host_has_secret(&key);

                        let response_msg_content = OutboundAppMessage::SecretExists(exists);
                        let payload = bincode::serialize(&response_msg_content)
                            .map_err(|err| DelegateError::Other(format!("{err}")))?;
                        let response =
                            ApplicationMessage::new(incoming_app.app, payload).processed(true);
                        Ok(vec![OutboundDelegateMsg::ApplicationMessage(response)])
                    }

                    InboundAppMessage::GetNonExistentSecret(key) => {
                        let result = host_get_secret(&key);

                        let response_msg_content = OutboundAppMessage::SecretResult(result);
                        let payload = bincode::serialize(&response_msg_content)
                            .map_err(|err| DelegateError::Other(format!("{err}")))?;
                        let response =
                            ApplicationMessage::new(incoming_app.app, payload).processed(true);
                        Ok(vec![OutboundDelegateMsg::ApplicationMessage(response)])
                    }

                    InboundAppMessage::StoreSecret { key, value } => {
                        host_set_secret(&key, &value);

                        let response_msg_content = OutboundAppMessage::SecretStored;
                        let payload = bincode::serialize(&response_msg_content)
                            .map_err(|err| DelegateError::Other(format!("{err}")))?;
                        let response =
                            ApplicationMessage::new(incoming_app.app, payload).processed(true);
                        Ok(vec![OutboundDelegateMsg::ApplicationMessage(response)])
                    }
                }
            }
            _ => Err(DelegateError::Other(
                "Unexpected inbound message".to_string(),
            )),
        }
    }
}

/// Test delegate that uses the new host function API for context and secret access.
///
/// This delegate demonstrates the simplified pattern where:
/// - Context is read/written via `ctx.read()` / `ctx.write()`
/// - Secrets are accessed via `ctx.get_secret()` / `ctx.set_secret()` / `ctx.has_secret()`
/// - No GetSecretRequest/GetSecretResponse round-trip is needed
/// - The delegate handles everything in a single `process()` call
use freenet_stdlib::prelude::*;
use serde::{Deserialize, Serialize};

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
    /// New: Clear the context (write empty data)
    ClearContext,
    /// New: Increment a counter stored in context (tests read-modify-write)
    IncrementCounter,
    /// New: Check if a secret exists
    HasSecret(Vec<u8>),
    /// New: Try to get a non-existent secret (should return error info)
    GetNonExistentSecret(Vec<u8>),
    /// New: Store a secret with given key and value
    StoreSecret { key: Vec<u8>, value: Vec<u8> },
    /// New: Remove a secret by key
    RemoveSecret(Vec<u8>),
    /// New: Write large data to context (for stress testing)
    WriteLargeContext(usize),
    /// New: Store large secret (for stress testing)
    StoreLargeSecret { key: Vec<u8>, size: usize },
    /// Test deprecated message path - emit a GetSecretRequest (deprecated)
    EmitDeprecatedSecretRequest { key: Vec<u8> },
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
    /// New: Acknowledgement of context clear
    ContextCleared,
    /// New: Acknowledgement of secret store
    SecretStored,
    /// New: Acknowledgement of secret removal
    SecretRemoved,
    /// New: Result of large context write (returns size written)
    LargeContextWritten(usize),
    /// New: Result of large secret store (returns size stored)
    LargeSecretStored(usize),
    /// Acknowledgement that deprecated request was emitted
    DeprecatedRequestEmitted,
}

// -- Delegate implementation --

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
                    InboundAppMessage::CreateInboxRequest => {
                        // Store the secret directly via ctx handle
                        ctx.set_secret(&PRIVATE_KEY, &PRIVATE_KEY);

                        let response_msg_content =
                            OutboundAppMessage::CreateInboxResponse(PUB_KEY.to_vec());
                        let payload = bincode::serialize(&response_msg_content)
                            .map_err(|err| DelegateError::Other(format!("{err}")))?;
                        let response =
                            ApplicationMessage::new(incoming_app.app, payload).processed(true);
                        Ok(vec![OutboundDelegateMsg::ApplicationMessage(response)])
                    }

                    InboundAppMessage::PleaseSignMessage(inbox_priv_key) => {
                        // Fetch the secret directly via ctx handle — no round-trip needed!
                        let _secret = ctx
                            .get_secret(&inbox_priv_key)
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
                        // Write data to context via ctx handle
                        ctx.write(&data);

                        let response_msg_content = OutboundAppMessage::ContextWritten;
                        let payload = bincode::serialize(&response_msg_content)
                            .map_err(|err| DelegateError::Other(format!("{err}")))?;
                        let response =
                            ApplicationMessage::new(incoming_app.app, payload).processed(true);
                        Ok(vec![OutboundDelegateMsg::ApplicationMessage(response)])
                    }

                    InboundAppMessage::ReadContext => {
                        // Read context via ctx handle
                        let data = ctx.read();

                        let response_msg_content = OutboundAppMessage::ContextData(data);
                        let payload = bincode::serialize(&response_msg_content)
                            .map_err(|err| DelegateError::Other(format!("{err}")))?;
                        let response =
                            ApplicationMessage::new(incoming_app.app, payload).processed(true);
                        Ok(vec![OutboundDelegateMsg::ApplicationMessage(response)])
                    }

                    InboundAppMessage::ClearContext => {
                        // Clear context by calling clear()
                        ctx.clear();

                        let response_msg_content = OutboundAppMessage::ContextCleared;
                        let payload = bincode::serialize(&response_msg_content)
                            .map_err(|err| DelegateError::Other(format!("{err}")))?;
                        let response =
                            ApplicationMessage::new(incoming_app.app, payload).processed(true);
                        Ok(vec![OutboundDelegateMsg::ApplicationMessage(response)])
                    }

                    InboundAppMessage::IncrementCounter => {
                        // Read current counter from context, increment, write back
                        let ctx_bytes = ctx.read();
                        let current: u32 = if ctx_bytes.is_empty() {
                            0
                        } else {
                            bincode::deserialize(&ctx_bytes).unwrap_or(0)
                        };
                        let new_value = current + 1;
                        let new_ctx = bincode::serialize(&new_value)
                            .map_err(|err| DelegateError::Other(format!("{err}")))?;
                        ctx.write(&new_ctx);

                        let response_msg_content = OutboundAppMessage::CounterValue(new_value);
                        let payload = bincode::serialize(&response_msg_content)
                            .map_err(|err| DelegateError::Other(format!("{err}")))?;
                        let response =
                            ApplicationMessage::new(incoming_app.app, payload).processed(true);
                        Ok(vec![OutboundDelegateMsg::ApplicationMessage(response)])
                    }

                    InboundAppMessage::HasSecret(key) => {
                        let exists = ctx.has_secret(&key);

                        let response_msg_content = OutboundAppMessage::SecretExists(exists);
                        let payload = bincode::serialize(&response_msg_content)
                            .map_err(|err| DelegateError::Other(format!("{err}")))?;
                        let response =
                            ApplicationMessage::new(incoming_app.app, payload).processed(true);
                        Ok(vec![OutboundDelegateMsg::ApplicationMessage(response)])
                    }

                    InboundAppMessage::GetNonExistentSecret(key) => {
                        let result = ctx.get_secret(&key);

                        let response_msg_content = OutboundAppMessage::SecretResult(result);
                        let payload = bincode::serialize(&response_msg_content)
                            .map_err(|err| DelegateError::Other(format!("{err}")))?;
                        let response =
                            ApplicationMessage::new(incoming_app.app, payload).processed(true);
                        Ok(vec![OutboundDelegateMsg::ApplicationMessage(response)])
                    }

                    InboundAppMessage::StoreSecret { key, value } => {
                        ctx.set_secret(&key, &value);

                        let response_msg_content = OutboundAppMessage::SecretStored;
                        let payload = bincode::serialize(&response_msg_content)
                            .map_err(|err| DelegateError::Other(format!("{err}")))?;
                        let response =
                            ApplicationMessage::new(incoming_app.app, payload).processed(true);
                        Ok(vec![OutboundDelegateMsg::ApplicationMessage(response)])
                    }

                    InboundAppMessage::RemoveSecret(key) => {
                        ctx.remove_secret(&key);

                        let response_msg_content = OutboundAppMessage::SecretRemoved;
                        let payload = bincode::serialize(&response_msg_content)
                            .map_err(|err| DelegateError::Other(format!("{err}")))?;
                        let response =
                            ApplicationMessage::new(incoming_app.app, payload).processed(true);
                        Ok(vec![OutboundDelegateMsg::ApplicationMessage(response)])
                    }

                    InboundAppMessage::WriteLargeContext(size) => {
                        // Generate deterministic data pattern
                        let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
                        ctx.write(&data);

                        let response_msg_content = OutboundAppMessage::LargeContextWritten(size);
                        let payload = bincode::serialize(&response_msg_content)
                            .map_err(|err| DelegateError::Other(format!("{err}")))?;
                        let response =
                            ApplicationMessage::new(incoming_app.app, payload).processed(true);
                        Ok(vec![OutboundDelegateMsg::ApplicationMessage(response)])
                    }

                    InboundAppMessage::StoreLargeSecret { key, size } => {
                        // Generate deterministic data pattern
                        let value: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
                        ctx.set_secret(&key, &value);

                        let response_msg_content = OutboundAppMessage::LargeSecretStored(size);
                        let payload = bincode::serialize(&response_msg_content)
                            .map_err(|err| DelegateError::Other(format!("{err}")))?;
                        let response =
                            ApplicationMessage::new(incoming_app.app, payload).processed(true);
                        Ok(vec![OutboundDelegateMsg::ApplicationMessage(response)])
                    }

                    InboundAppMessage::EmitDeprecatedSecretRequest { key } => {
                        // Emit a deprecated GetSecretRequest message to test backward compatibility
                        // The runtime should log a warning but still handle this gracefully
                        let deprecated_request =
                            OutboundDelegateMsg::GetSecretRequest(GetSecretRequest {
                                key: SecretsId::new(key),
                                context: DelegateContext::default(),
                                processed: false,
                            });

                        // Also send an app message so we know the delegate completed
                        let response_msg_content = OutboundAppMessage::DeprecatedRequestEmitted;
                        let payload = bincode::serialize(&response_msg_content)
                            .map_err(|err| DelegateError::Other(format!("{err}")))?;
                        let response =
                            ApplicationMessage::new(incoming_app.app, payload).processed(true);

                        Ok(vec![
                            deprecated_request,
                            OutboundDelegateMsg::ApplicationMessage(response),
                        ])
                    }
                }
            }
            _ => Err(DelegateError::Other(
                "Unexpected inbound message".to_string(),
            )),
        }
    }
}

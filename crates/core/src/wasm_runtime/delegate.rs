use std::collections::{HashMap, HashSet, VecDeque};

use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use freenet_stdlib::prelude::{
    ApplicationMessage, ClientResponse, DelegateContainer, DelegateContext, DelegateError,
    DelegateInterfaceResult, DelegateKey, GetContractRequest, InboundDelegateMsg,
    OutboundDelegateMsg, Parameters, SecretsId,
};
use serde::{Deserialize, Serialize};
use wasmer::{Instance, TypedFunction};

use super::native_api::{DelegateCallEnv, InstanceId, CURRENT_DELEGATE_INSTANCE, DELEGATE_ENV};
use super::{Runtime, RuntimeResult};
use crate::wasm_runtime::delegate_api::DelegateApiVersion;

/// RAII guard that ensures cleanup of delegate environment state.
/// When dropped, it clears the thread-local instance ID and removes the
/// entry from the global DELEGATE_ENV map.
struct DelegateEnvGuard {
    instance_id: InstanceId,
}

impl DelegateEnvGuard {
    fn new(instance_id: InstanceId) -> Self {
        Self { instance_id }
    }
}

impl Drop for DelegateEnvGuard {
    fn drop(&mut self) {
        // Clear thread-local first, then remove from global map
        CURRENT_DELEGATE_INSTANCE.with(|c| c.set(-1));
        DELEGATE_ENV.remove(&self.instance_id);
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Allowed,
    NotAllowed,
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct Context {
    waiting_for_user_input: HashSet<u32>,
    user_response: HashMap<u32, Response>,
}

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum DelegateExecError {
    #[error(transparent)]
    DelegateError(#[from] DelegateError),

    #[error("Permission denied: secret {secret} cannot be accesed by {delegate} at this time")]
    UnauthorizedSecretAccess {
        secret: SecretsId,
        delegate: DelegateKey,
    },

    #[error("Received an unexpected message from the client apps: {0}")]
    UnexpectedMessage(&'static str),
}

pub(crate) trait DelegateRuntimeInterface {
    fn inbound_app_message(
        &mut self,
        key: &DelegateKey,
        params: &Parameters,
        attested: Option<&[u8]>,
        inbound: Vec<InboundDelegateMsg>,
    ) -> RuntimeResult<Vec<OutboundDelegateMsg>>;

    fn register_delegate(
        &mut self,
        delegate: DelegateContainer,
        cipher: XChaCha20Poly1305,
        nonce: XNonce,
    ) -> RuntimeResult<()>;

    fn unregister_delegate(&mut self, key: &DelegateKey) -> RuntimeResult<()>;
}

impl Runtime {
    /// Execute the delegate's `process` function with the DelegateCallEnv set up
    /// so that host functions for context and secret access are available.
    ///
    /// Uses RAII guard pattern to ensure cleanup happens even if WASM execution panics.
    #[allow(clippy::too_many_arguments)]
    fn exec_inbound_with_env(
        &mut self,
        delegate_key: &DelegateKey,
        params: &Parameters<'_>,
        attested: Option<&[u8]>,
        msg: &InboundDelegateMsg,
        context: Vec<u8>,
        process_func: &TypedFunction<(i64, i64, i64), i64>,
        instance: &Instance,
        instance_id: i64,
        api_version: DelegateApiVersion,
    ) -> RuntimeResult<(Vec<OutboundDelegateMsg>, Vec<u8>)> {
        // Set up the delegate call environment with context, secret store, and
        // contract store access. SAFETY: self.secret_store and self.contract_store
        // are valid for the duration of the process_func.call() / call_async() below.
        // The guard ensures cleanup even on panic.
        let env = unsafe {
            DelegateCallEnv::new(
                context,
                &mut self.secret_store,
                &self.contract_store,
                self.state_store_db.clone(),
                delegate_key.clone(),
            )
        };

        // Debug assertion: instance IDs should never collide (they come from a monotonic counter)
        debug_assert!(
            !DELEGATE_ENV.contains_key(&instance_id),
            "Instance ID {instance_id} already exists in DELEGATE_ENV - this indicates a bug"
        );

        DELEGATE_ENV.insert(instance_id, env);
        CURRENT_DELEGATE_INSTANCE.with(|c| c.set(instance_id));

        // Create RAII guard to ensure cleanup on all exit paths (including panic)
        let _guard = DelegateEnvGuard::new(instance_id);

        // Execute the WASM process function.
        // V2 delegates use call_async (async host functions for contract access).
        // V1 delegates use synchronous call.
        let result = self.exec_inbound(params, attested, msg, process_func, instance, api_version);

        // Read back the (possibly mutated) context before guard drops
        // Note: We need to get the context before the guard drops and removes it
        let updated_context = DELEGATE_ENV
            .get(&instance_id)
            .map(|env| env.context.clone())
            .unwrap_or_default();

        // Guard will clean up CURRENT_DELEGATE_INSTANCE and DELEGATE_ENV on drop

        let outbound = result?;
        Ok((outbound, updated_context))
    }

    fn exec_inbound(
        &mut self,
        params: &Parameters<'_>,
        attested: Option<&[u8]>,
        msg: &InboundDelegateMsg,
        process_func: &TypedFunction<(i64, i64, i64), i64>,
        instance: &Instance,
        api_version: DelegateApiVersion,
    ) -> RuntimeResult<Vec<OutboundDelegateMsg>> {
        let param_buf_ptr = {
            let mut param_buf = self.init_buf(instance, params)?;
            param_buf.write(params)?;
            param_buf.ptr()
        };
        let attested_buf_ptr = {
            let bytes = attested.unwrap_or(&[]);
            let mut attested_buf = self.init_buf(instance, bytes)?;
            attested_buf.write(bytes)?;
            attested_buf.ptr()
        };
        let msg_ptr = {
            let msg = bincode::serialize(msg)?;
            let mut msg_buf = self.init_buf(instance, &msg)?;
            msg_buf.write(msg)?;
            msg_buf.ptr()
        };
        let inbound_msg_name = match msg {
            InboundDelegateMsg::ApplicationMessage(_) => "ApplicationMessage",
            InboundDelegateMsg::UserResponse(_) => "UserResponse",
            InboundDelegateMsg::GetContractResponse(_) => "GetContractResponse",
        };
        tracing::debug!(
            inbound_msg_name,
            api_version = %api_version,
            "Calling delegate with inbound message"
        );

        let res = match api_version {
            DelegateApiVersion::V1 => {
                // V1: synchronous call — no async host functions involved
                process_func.call(
                    self.wasm_store.as_mut().unwrap(),
                    param_buf_ptr as i64,
                    attested_buf_ptr as i64,
                    msg_ptr as i64,
                )?
            }
            DelegateApiVersion::V2 => {
                // V2: async call — contract host functions are async.
                // Take the Store out, convert to StoreAsync, call_async, convert back.
                //
                // We use futures::executor::block_on as the bridge from sync to async.
                // This works because the current async host functions complete
                // synchronously (ReDb reads). When truly async operations are added
                // (network fetches, subscriptions), the DelegateRuntimeInterface
                // trait should be made async and the bridge removed.
                let store = self
                    .wasm_store
                    .take()
                    .expect("wasm_store should be present");
                let store_async = store.into_async();

                let call_result = futures::executor::block_on(process_func.call_async(
                    &store_async,
                    param_buf_ptr as i64,
                    attested_buf_ptr as i64,
                    msg_ptr as i64,
                ));

                // Convert StoreAsync back to Store and restore it
                let store = store_async
                    .into_store()
                    .expect("StoreAsync should convert back to Store after call_async completes");
                self.wasm_store = Some(store);

                call_result?
            }
        };

        let linear_mem = self.linear_mem(instance)?;
        let outbound = unsafe {
            DelegateInterfaceResult::from_raw(res, &linear_mem)
                .unwrap(linear_mem)
                .map_err(Into::<DelegateExecError>::into)?
        };
        self.log_delegate_exec_result(inbound_msg_name, &outbound);
        Ok(outbound)
    }

    fn log_delegate_exec_result(&self, inbound_msg_name: &str, outbound: &[OutboundDelegateMsg]) {
        if tracing::enabled!(tracing::Level::DEBUG) {
            let outbound_message_names = outbound
                .iter()
                .map(|m| match m {
                    OutboundDelegateMsg::ApplicationMessage(am) => format!(
                        "ApplicationMessage(app={}, payload_len={}, processed={}, context_len={})",
                        am.app,
                        am.payload.len(),
                        am.processed,
                        am.context.as_ref().len()
                    ),
                    OutboundDelegateMsg::RequestUserInput(_) => "RequestUserInput".to_string(),
                    OutboundDelegateMsg::ContextUpdated(_) => "ContextUpdated".to_string(),
                    OutboundDelegateMsg::GetContractRequest(req) => {
                        format!("GetContractRequest(contract={})", req.contract_id)
                    }
                })
                .collect::<Vec<String>>()
                .join(", ");
            tracing::debug!(
                inbound_msg_name,
                outbound_message_names,
                "Delegate returned outbound messages"
            );
        } else {
            tracing::debug!(
                inbound_msg_name,
                outbound_len = outbound.len(),
                "Delegate returned outbound messages"
            );
        }
    }

    fn log_process_outbound_entry(
        &self,
        delegate_key: &DelegateKey,
        attested: Option<&[u8]>,
        outbound_msgs: &VecDeque<OutboundDelegateMsg>,
    ) {
        tracing::debug!(
            delegate_key = ?delegate_key,
            ?attested,
            outbound_msgs_len = outbound_msgs.len(),
            // Generate message details only if DEBUG level is enabled
            outbound_msg_details = debug(if tracing::enabled!(tracing::Level::DEBUG) {
                outbound_msgs.iter().map(|msg| {
                    match msg {
                        OutboundDelegateMsg::ApplicationMessage(m) => format!("AppMsg(payload_len={})", m.payload.len()),
                        OutboundDelegateMsg::RequestUserInput(_) => "UserInputReq".to_string(),
                        OutboundDelegateMsg::ContextUpdated(_) => "ContextUpdate".to_string(),
                        OutboundDelegateMsg::GetContractRequest(r) => format!("GetContractReq({})", r.contract_id),
                    }
                }).collect::<Vec<_>>()
            } else {
                // Avoid computation if tracing level is disabled
                Vec::new()
            }),
            "process_outbound called"
        );
    }

    /// Process outbound messages from a delegate.
    ///
    /// Delegates use host functions for secret access, so this function primarily
    /// handles contract requests and application messages.
    #[allow(clippy::too_many_arguments)]
    fn process_outbound(
        &mut self,
        delegate_key: &DelegateKey,
        _instance: &Instance,
        _instance_id: i64,
        _process_func: &TypedFunction<(i64, i64, i64), i64>,
        _params: &Parameters<'_>,
        attested: Option<&[u8]>,
        outbound_msgs: &mut VecDeque<OutboundDelegateMsg>,
        context: &mut Vec<u8>,
        results: &mut Vec<OutboundDelegateMsg>,
    ) -> RuntimeResult<()> {
        self.log_process_outbound_entry(delegate_key, attested, outbound_msgs);

        while let Some(outbound) = outbound_msgs.pop_front() {
            match outbound {
                // Application message — add to results
                OutboundDelegateMsg::ApplicationMessage(mut msg) => {
                    tracing::debug!(
                        app = %msg.app,
                        payload_len = msg.payload.len(),
                        processed = msg.processed,
                        "Adding ApplicationMessage to results"
                    );
                    msg.context = DelegateContext::default();
                    results.push(OutboundDelegateMsg::ApplicationMessage(msg));
                    // Drain remaining messages to results before breaking to avoid message loss
                    for remaining in outbound_msgs.drain(..) {
                        results.push(remaining);
                    }
                    break;
                }

                OutboundDelegateMsg::RequestUserInput(req) => {
                    let user_response =
                        ClientResponse::new(serde_json::to_vec(&Response::Allowed).unwrap());
                    let response: Response = serde_json::from_slice(&user_response)
                        .map_err(|err| DelegateError::Deser(format!("{err}")))
                        .unwrap();
                    let req_id = req.request_id;
                    let mut ctx: Context =
                        bincode::deserialize(context.as_slice()).unwrap_or_default();
                    ctx.waiting_for_user_input.remove(&req_id);
                    ctx.user_response.insert(req_id, response);
                    *context = bincode::serialize(&ctx).unwrap();
                }

                OutboundDelegateMsg::ContextUpdated(new_context) => {
                    *context = new_context.as_ref().to_vec();
                }
                OutboundDelegateMsg::GetContractRequest(req) if !req.processed => {
                    // Pass unprocessed contract requests to results for the executor to handle
                    tracing::debug!(
                        contract_id = %req.contract_id,
                        "Passing GetContractRequest to executor for async handling"
                    );
                    results.push(OutboundDelegateMsg::GetContractRequest(req));
                    // Drain remaining messages to results before breaking to avoid message loss
                    for remaining in outbound_msgs.drain(..) {
                        results.push(remaining);
                    }
                    // Break to let the executor handle this before continuing
                    break;
                }
                OutboundDelegateMsg::GetContractRequest(GetContractRequest {
                    context: ctx,
                    ..
                }) => {
                    // Processed contract request - just update context
                    tracing::debug!("GetContractRequest processed");
                    *context = ctx.as_ref().to_vec();
                }
            }
        }
        Ok(())
    }
}

impl DelegateRuntimeInterface for Runtime {
    fn inbound_app_message(
        &mut self,
        delegate_key: &DelegateKey,
        params: &Parameters,
        attested: Option<&[u8]>,
        inbound: Vec<InboundDelegateMsg>,
    ) -> RuntimeResult<Vec<OutboundDelegateMsg>> {
        let mut results = Vec::with_capacity(inbound.len());
        if inbound.is_empty() {
            return Ok(results);
        }
        let running = self.prepare_delegate_call(params, delegate_key, 4096)?;
        let process_func: TypedFunction<(i64, i64, i64), i64> = running
            .instance
            .exports
            .get_typed_function(self.wasm_store.as_ref().unwrap(), "process")?;
        let instance_id = running.id;

        // Detect API version: V2 delegates import from the freenet_delegate_contracts
        // namespace (async contract access host functions). For the prototype, we
        // always use V2 (call_async) since it's backward-compatible with V1 delegates
        // that don't call the contract functions — call_async just adds a thin
        // coroutine wrapper that completes immediately for non-yielding functions.
        let api_version = if self.state_store_db.is_some() {
            DelegateApiVersion::V2
        } else {
            DelegateApiVersion::V1
        };

        tracing::debug!(
            delegate_key = %delegate_key,
            api_version = %api_version,
            "Starting delegate execution"
        );

        // State maintained across process() calls within this conversation
        let mut context: Vec<u8> = Vec::new();

        for msg in inbound {
            match msg {
                InboundDelegateMsg::ApplicationMessage(ApplicationMessage {
                    app,
                    payload,
                    processed,
                    ..
                }) => {
                    let app_msg = InboundDelegateMsg::ApplicationMessage(
                        ApplicationMessage::new(app, payload)
                            .processed(processed)
                            .with_context(DelegateContext::new(context.clone())),
                    );

                    let (outbound, updated_context) = self.exec_inbound_with_env(
                        delegate_key,
                        params,
                        attested,
                        &app_msg,
                        context.clone(),
                        &process_func,
                        &running.instance,
                        instance_id,
                        api_version,
                    )?;
                    context = updated_context;

                    let mut outbound_queue = VecDeque::from(outbound);
                    self.process_outbound(
                        delegate_key,
                        &running.instance,
                        instance_id,
                        &process_func,
                        params,
                        attested,
                        &mut outbound_queue,
                        &mut context,
                        &mut results,
                    )?;
                }
                InboundDelegateMsg::UserResponse(response) => {
                    let (outbound, updated_context) = self.exec_inbound_with_env(
                        delegate_key,
                        params,
                        attested,
                        &InboundDelegateMsg::UserResponse(response),
                        context.clone(),
                        &process_func,
                        &running.instance,
                        instance_id,
                        api_version,
                    )?;
                    context = updated_context;

                    let mut outbound_queue = VecDeque::from(outbound);
                    self.process_outbound(
                        delegate_key,
                        &running.instance,
                        instance_id,
                        &process_func,
                        params,
                        attested,
                        &mut outbound_queue,
                        &mut context,
                        &mut results,
                    )?;
                }
                InboundDelegateMsg::GetContractResponse(response) => {
                    let (outbound, updated_context) = self.exec_inbound_with_env(
                        delegate_key,
                        params,
                        attested,
                        &InboundDelegateMsg::GetContractResponse(response),
                        context.clone(),
                        &process_func,
                        &running.instance,
                        instance_id,
                        api_version,
                    )?;
                    context = updated_context;

                    let mut outbound_queue = VecDeque::from(outbound);
                    self.process_outbound(
                        delegate_key,
                        &running.instance,
                        instance_id,
                        &process_func,
                        params,
                        attested,
                        &mut outbound_queue,
                        &mut context,
                        &mut results,
                    )?;
                }
            }
        }

        tracing::debug!(
            count = results.len(),
            "Final results returned by inbound_app_message"
        );
        Ok(results)
    }

    #[inline]
    fn register_delegate(
        &mut self,
        delegate: DelegateContainer,
        cipher: XChaCha20Poly1305,
        nonce: XNonce,
    ) -> RuntimeResult<()> {
        self.secret_store
            .register_delegate(delegate.key().clone(), cipher, nonce)?;
        self.delegate_store.store_delegate(delegate)
    }

    #[inline]
    fn unregister_delegate(&mut self, key: &DelegateKey) -> RuntimeResult<()> {
        // Remove from module cache to prevent stale code execution if re-registered
        self.delegate_modules.pop(key);
        self.delegate_store.remove_delegate(key)
    }
}

#[cfg(test)]
mod test {
    use chacha20poly1305::aead::{AeadCore, KeyInit, OsRng};
    use freenet_stdlib::prelude::*;
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;

    use crate::util::tests::get_temp_dir;

    use super::super::{delegate_store::DelegateStore, ContractStore, SecretsStore};
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
        }
    }

    async fn setup_runtime(
        name: &str,
    ) -> Result<(DelegateContainer, Runtime, tempfile::TempDir), Box<dyn std::error::Error>> {
        use crate::contract::storages::Storage;
        // let _ = tracing_subscriber::fmt().with_env_filter("info").try_init();
        let temp_dir = get_temp_dir();
        let contracts_dir = temp_dir.path().join("contracts");
        let delegates_dir = temp_dir.path().join("delegates");
        let secrets_dir = temp_dir.path().join("secrets");

        let db = Storage::new(temp_dir.path()).await?;
        let contract_store = ContractStore::new(contracts_dir, 10_000, db.clone())?;
        let delegate_store = DelegateStore::new(delegates_dir, 10_000, db.clone())?;
        let secret_store = SecretsStore::new(secrets_dir, Default::default(), db)?;

        let mut runtime =
            Runtime::build(contract_store, delegate_store, secret_store, false).unwrap();

        let delegate = {
            let bytes = super::super::tests::get_test_module(name)?;
            DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(Delegate::from((
                &bytes.into(),
                &vec![].into(),
            ))))
        };
        let _ = runtime.delegate_store.store_delegate(delegate.clone());

        let key = XChaCha20Poly1305::generate_key(&mut OsRng);
        let cipher = XChaCha20Poly1305::new(&key);
        let nonce = XChaCha20Poly1305::generate_nonce(&mut OsRng);
        let _ = runtime
            .secret_store
            .register_delegate(delegate.key().clone(), cipher, nonce);

        Ok((delegate, runtime, temp_dir))
    }

    // Test delegate module name for capabilities test
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
            Error {
                message: String,
            },
        }
    }

    /// Test that GetContractRequest is properly returned from WASM runtime
    /// and GetContractResponse is properly delivered back to the delegate.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_contract_request_response() -> Result<(), Box<dyn std::error::Error>> {
        use capabilities_messages::*;

        let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_CAPABILITIES).await?;

        // Create a contract ID that the delegate will request
        let target_contract_id = ContractInstanceId::new([42u8; 32]);

        // Create an app ID for the response
        let app_id = ContractInstanceId::new([1u8; 32]);

        // Step 1: Send GetContractState command to delegate
        let command = DelegateCommand::GetContractState {
            contract_id: target_contract_id,
        };
        let payload = bincode::serialize(&command)?;
        let app_msg = ApplicationMessage::new(app_id, payload);

        let outbound = runtime.inbound_app_message(
            delegate.key(),
            &vec![].into(),
            None,
            vec![InboundDelegateMsg::ApplicationMessage(app_msg)],
        )?;

        // Step 2: Verify we get a GetContractRequest back
        assert_eq!(outbound.len(), 1, "Expected exactly one outbound message");
        let contract_request = match &outbound[0] {
            OutboundDelegateMsg::GetContractRequest(req) => req.clone(),
            other => panic!("Expected GetContractRequest, got {:?}", other),
        };
        assert_eq!(
            contract_request.contract_id, target_contract_id,
            "Contract ID should match"
        );
        assert!(
            !contract_request.processed,
            "Request should not be processed yet"
        );

        // Step 3: Simulate the executor sending GetContractResponse back
        let contract_state = vec![1, 2, 3, 4, 5]; // Some test state
        let response = InboundDelegateMsg::GetContractResponse(GetContractResponse {
            contract_id: target_contract_id,
            state: Some(WrappedState::new(contract_state.clone())),
            context: contract_request.context.clone(),
        });

        let final_outbound =
            runtime.inbound_app_message(delegate.key(), &vec![].into(), None, vec![response])?;

        // Step 4: Verify we get the final ApplicationMessage with the contract state
        assert_eq!(
            final_outbound.len(),
            1,
            "Expected exactly one final message"
        );
        let final_msg = match &final_outbound[0] {
            OutboundDelegateMsg::ApplicationMessage(msg) => msg,
            other => panic!("Expected ApplicationMessage, got {:?}", other),
        };
        assert!(final_msg.processed, "Final message should be processed");

        // Verify the response content
        let response: DelegateResponse = bincode::deserialize(&final_msg.payload)?;
        match response {
            DelegateResponse::ContractState { contract_id, state } => {
                assert_eq!(contract_id, target_contract_id);
                assert_eq!(state, Some(contract_state));
            }
            other => panic!("Expected ContractState response, got {:?}", other),
        }

        std::mem::drop(temp_dir);
        Ok(())
    }

    /// Test that GetContractResponse with None state (contract not found) is handled correctly
    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_contract_not_found() -> Result<(), Box<dyn std::error::Error>> {
        use capabilities_messages::*;

        let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_CAPABILITIES).await?;

        let target_contract_id = ContractInstanceId::new([99u8; 32]);
        let app_id = ContractInstanceId::new([1u8; 32]);

        // Send GetContractState command
        let command = DelegateCommand::GetContractState {
            contract_id: target_contract_id,
        };
        let payload = bincode::serialize(&command)?;
        let app_msg = ApplicationMessage::new(app_id, payload);

        let outbound = runtime.inbound_app_message(
            delegate.key(),
            &vec![].into(),
            None,
            vec![InboundDelegateMsg::ApplicationMessage(app_msg)],
        )?;

        // Get the request
        let contract_request = match &outbound[0] {
            OutboundDelegateMsg::GetContractRequest(req) => req.clone(),
            other => panic!("Expected GetContractRequest, got {:?}", other),
        };

        // Send response with None state (contract not found)
        let response = InboundDelegateMsg::GetContractResponse(GetContractResponse {
            contract_id: target_contract_id,
            state: None, // Not found
            context: contract_request.context.clone(),
        });

        let final_outbound =
            runtime.inbound_app_message(delegate.key(), &vec![].into(), None, vec![response])?;

        // Verify the response indicates no state
        let final_msg = match &final_outbound[0] {
            OutboundDelegateMsg::ApplicationMessage(msg) => msg,
            other => panic!("Expected ApplicationMessage, got {:?}", other),
        };

        let response: DelegateResponse = bincode::deserialize(&final_msg.payload)?;
        match response {
            DelegateResponse::ContractState { state, .. } => {
                assert!(
                    state.is_none(),
                    "State should be None for not-found contract"
                );
            }
            other => panic!("Expected ContractState response, got {:?}", other),
        }

        std::mem::drop(temp_dir);
        Ok(())
    }

    /// Test that multiple contract requests work in sequence.
    /// The delegate requests contract1, then contract2, then contract3, and returns
    /// the accumulated results at the end.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_multiple_contract_requests() -> Result<(), Box<dyn std::error::Error>> {
        use capabilities_messages::*;

        let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_CAPABILITIES).await?;

        let contract1 = ContractInstanceId::new([1u8; 32]);
        let contract2 = ContractInstanceId::new([2u8; 32]);
        let contract3 = ContractInstanceId::new([3u8; 32]);
        let app_id = ContractInstanceId::new([10u8; 32]);

        // Step 1: Send GetMultipleContractStates command
        let command = DelegateCommand::GetMultipleContractStates {
            contract_ids: vec![contract1, contract2, contract3],
        };
        let payload = bincode::serialize(&command)?;
        let app_msg = ApplicationMessage::new(app_id, payload);

        let outbound = runtime.inbound_app_message(
            delegate.key(),
            &vec![].into(),
            None,
            vec![InboundDelegateMsg::ApplicationMessage(app_msg)],
        )?;

        // Step 2: Verify we get first GetContractRequest
        assert_eq!(outbound.len(), 1, "Expected one outbound message");
        let req1 = match &outbound[0] {
            OutboundDelegateMsg::GetContractRequest(req) => req.clone(),
            other => panic!("Expected GetContractRequest, got {:?}", other),
        };
        assert_eq!(req1.contract_id, contract1);

        // Step 3: Send response for first contract
        let response1 = InboundDelegateMsg::GetContractResponse(GetContractResponse {
            contract_id: contract1,
            state: Some(WrappedState::new(vec![1, 1, 1])),
            context: req1.context,
        });

        let outbound2 =
            runtime.inbound_app_message(delegate.key(), &vec![].into(), None, vec![response1])?;

        // Step 4: Verify we get second GetContractRequest
        assert_eq!(outbound2.len(), 1, "Expected one outbound message");
        let req2 = match &outbound2[0] {
            OutboundDelegateMsg::GetContractRequest(req) => req.clone(),
            other => panic!("Expected GetContractRequest for contract2, got {:?}", other),
        };
        assert_eq!(req2.contract_id, contract2);

        // Step 5: Send response for second contract
        let response2 = InboundDelegateMsg::GetContractResponse(GetContractResponse {
            contract_id: contract2,
            state: Some(WrappedState::new(vec![2, 2, 2])),
            context: req2.context,
        });

        let outbound3 =
            runtime.inbound_app_message(delegate.key(), &vec![].into(), None, vec![response2])?;

        // Step 6: Verify we get third GetContractRequest
        assert_eq!(outbound3.len(), 1, "Expected one outbound message");
        let req3 = match &outbound3[0] {
            OutboundDelegateMsg::GetContractRequest(req) => req.clone(),
            other => panic!("Expected GetContractRequest for contract3, got {:?}", other),
        };
        assert_eq!(req3.contract_id, contract3);

        // Step 7: Send response for third contract
        let response3 = InboundDelegateMsg::GetContractResponse(GetContractResponse {
            contract_id: contract3,
            state: Some(WrappedState::new(vec![3, 3, 3])),
            context: req3.context,
        });

        let final_outbound =
            runtime.inbound_app_message(delegate.key(), &vec![].into(), None, vec![response3])?;

        // Step 8: Verify we get the final MultipleContractStates response
        assert_eq!(final_outbound.len(), 1, "Expected one final message");
        let final_msg = match &final_outbound[0] {
            OutboundDelegateMsg::ApplicationMessage(msg) => msg,
            other => panic!("Expected ApplicationMessage, got {:?}", other),
        };
        assert!(final_msg.processed);

        let response: DelegateResponse = bincode::deserialize(&final_msg.payload)?;
        match response {
            DelegateResponse::MultipleContractStates { results } => {
                assert_eq!(results.len(), 3, "Should have 3 results");
                assert_eq!(results[0].0, contract1);
                assert_eq!(results[0].1, Some(vec![1, 1, 1]));
                assert_eq!(results[1].0, contract2);
                assert_eq!(results[1].1, Some(vec![2, 2, 2]));
                assert_eq!(results[2].0, contract3);
                assert_eq!(results[2].1, Some(vec![3, 3, 3]));
            }
            other => panic!("Expected MultipleContractStates, got {:?}", other),
        }

        std::mem::drop(temp_dir);
        Ok(())
    }

    /// Test that message accumulation works correctly.
    /// The delegate emits both a GetContractRequest AND an ApplicationMessage (Echo).
    /// The runtime should return both after processing the contract request.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_message_accumulation() -> Result<(), Box<dyn std::error::Error>> {
        use capabilities_messages::*;

        let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_CAPABILITIES).await?;

        let contract_id = ContractInstanceId::new([42u8; 32]);
        let app_id = ContractInstanceId::new([1u8; 32]);
        let echo_message = "Hello from test!".to_string();

        // Step 1: Send GetContractWithEcho command
        let command = DelegateCommand::GetContractWithEcho {
            contract_id,
            echo_message: echo_message.clone(),
        };
        let payload = bincode::serialize(&command)?;
        let app_msg = ApplicationMessage::new(app_id, payload);

        let outbound = runtime.inbound_app_message(
            delegate.key(),
            &vec![].into(),
            None,
            vec![InboundDelegateMsg::ApplicationMessage(app_msg)],
        )?;

        // Step 2: The delegate returns BOTH GetContractRequest AND Echo message
        assert_eq!(outbound.len(), 2, "Expected two outbound messages");

        // Find the GetContractRequest (order may vary)
        let contract_request = outbound
            .iter()
            .find_map(|msg| match msg {
                OutboundDelegateMsg::GetContractRequest(req) => Some(req.clone()),
                _ => None,
            })
            .expect("Expected a GetContractRequest");
        assert_eq!(contract_request.contract_id, contract_id);

        // Find the Echo ApplicationMessage
        let echo_msg = outbound
            .iter()
            .find_map(|msg| match msg {
                OutboundDelegateMsg::ApplicationMessage(m) => Some(m.clone()),
                _ => None,
            })
            .expect("Expected an ApplicationMessage (Echo)");
        assert!(echo_msg.processed);

        let echo_response: DelegateResponse = bincode::deserialize(&echo_msg.payload)?;
        match echo_response {
            DelegateResponse::Echo { message } => {
                assert_eq!(message, echo_message);
            }
            other => panic!("Expected Echo response, got {:?}", other),
        }

        // Step 3: Send the contract response
        let contract_response = InboundDelegateMsg::GetContractResponse(GetContractResponse {
            contract_id,
            state: Some(WrappedState::new(vec![1, 2, 3, 4])),
            context: contract_request.context,
        });

        let final_outbound = runtime.inbound_app_message(
            delegate.key(),
            &vec![].into(),
            None,
            vec![contract_response],
        )?;

        // Step 4: Verify we get the final ContractState response
        assert_eq!(final_outbound.len(), 1, "Expected one final message");
        let final_msg = match &final_outbound[0] {
            OutboundDelegateMsg::ApplicationMessage(msg) => msg,
            other => panic!("Expected ApplicationMessage, got {:?}", other),
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
            other => panic!("Expected ContractState response, got {:?}", other),
        }

        std::mem::drop(temp_dir);
        Ok(())
    }

    /// Test the new-style delegate (test-delegate-2) that uses host functions
    /// for context and secret access instead of the GetSecretRequest/Response round-trip.
    #[tokio::test(flavor = "multi_thread")]
    async fn validate_host_function_delegate() -> Result<(), Box<dyn std::error::Error>> {
        use delegate2_messages::{InboundAppMessage, OutboundAppMessage};

        let contract = WrappedContract::new(
            Arc::new(ContractCode::from(vec![1])),
            Parameters::from(vec![]),
        );
        let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_2).await?;
        let app = ContractInstanceId::try_from(contract.key.to_string()).unwrap();

        // Step 1: CreateInboxRequest — stores secret via host function, returns pub key
        let payload: Vec<u8> = bincode::serialize(&InboundAppMessage::CreateInboxRequest).unwrap();
        let create_msg = ApplicationMessage::new(app, payload);
        let inbound = InboundDelegateMsg::ApplicationMessage(create_msg);
        let outbound =
            runtime.inbound_app_message(delegate.key(), &vec![].into(), None, vec![inbound])?;

        let expected_payload =
            bincode::serialize(&OutboundAppMessage::CreateInboxResponse(vec![1])).unwrap();
        assert_eq!(outbound.len(), 1, "Expected exactly 1 outbound message");
        assert!(matches!(
            outbound.first(),
            Some(OutboundDelegateMsg::ApplicationMessage(msg)) if *msg.payload == expected_payload
        ));

        // Step 2: PleaseSignMessage — fetches secret via host function (no round-trip!)
        // and returns the signed message in a single process() call.
        let payload: Vec<u8> =
            bincode::serialize(&InboundAppMessage::PleaseSignMessage(vec![1, 2, 3])).unwrap();
        let sign_msg = ApplicationMessage::new(app, payload);
        let inbound = InboundDelegateMsg::ApplicationMessage(sign_msg);
        let outbound =
            runtime.inbound_app_message(delegate.key(), &vec![].into(), None, vec![inbound])?;

        let expected_payload =
            bincode::serialize(&OutboundAppMessage::MessageSigned(vec![4, 5, 2])).unwrap();
        assert_eq!(outbound.len(), 1, "Expected exactly 1 outbound message");
        assert!(matches!(
            outbound.first(),
            Some(OutboundDelegateMsg::ApplicationMessage(msg)) if *msg.payload == expected_payload
        ));

        std::mem::drop(temp_dir);
        Ok(())
    }

    /// Test that context written via host function persists within the same inbound_app_message call.
    /// Note: Context is NOT preserved across separate inbound_app_message calls - each call
    /// starts with a fresh context. This test validates context sharing within a single call.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_context_persistence_within_call() -> Result<(), Box<dyn std::error::Error>> {
        use delegate2_messages::{InboundAppMessage, OutboundAppMessage};

        let contract = WrappedContract::new(
            Arc::new(ContractCode::from(vec![1])),
            Parameters::from(vec![]),
        );
        let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_2).await?;
        let app = ContractInstanceId::try_from(contract.key.to_string()).unwrap();

        // Send WriteContext and ReadContext in the same batch - they should share context
        let test_data = vec![1, 2, 3, 4, 5];

        let write_payload =
            bincode::serialize(&InboundAppMessage::WriteContext(test_data.clone()))?;
        let read_payload = bincode::serialize(&InboundAppMessage::ReadContext)?;

        let messages = vec![
            InboundDelegateMsg::ApplicationMessage(ApplicationMessage::new(app, write_payload)),
            InboundDelegateMsg::ApplicationMessage(ApplicationMessage::new(app, read_payload)),
        ];

        let outbound =
            runtime.inbound_app_message(delegate.key(), &vec![].into(), None, messages)?;

        assert_eq!(outbound.len(), 2, "Should have 2 responses");

        // First response: ContextWritten
        let response1: OutboundAppMessage = match &outbound[0] {
            OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
            _ => panic!("Expected ApplicationMessage"),
        };
        assert!(matches!(response1, OutboundAppMessage::ContextWritten));

        // Second response: Should contain the data we wrote
        let response2: OutboundAppMessage = match &outbound[1] {
            OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
            _ => panic!("Expected ApplicationMessage"),
        };
        match response2 {
            OutboundAppMessage::ContextData(data) => {
                assert_eq!(
                    data, test_data,
                    "Context data should match what was written"
                );
            }
            other => panic!("Expected ContextData, got {:?}", other),
        }

        std::mem::drop(temp_dir);
        Ok(())
    }

    /// Test that context is reset between separate inbound_app_message calls.
    /// This validates that each call starts fresh - context is NOT preserved across calls.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_context_reset_between_calls() -> Result<(), Box<dyn std::error::Error>> {
        use delegate2_messages::{InboundAppMessage, OutboundAppMessage};

        let contract = WrappedContract::new(
            Arc::new(ContractCode::from(vec![1])),
            Parameters::from(vec![]),
        );
        let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_2).await?;
        let app = ContractInstanceId::try_from(contract.key.to_string()).unwrap();

        // First call: increment counter (should become 1)
        let payload = bincode::serialize(&InboundAppMessage::IncrementCounter)?;
        let msg = ApplicationMessage::new(app, payload);
        let outbound = runtime.inbound_app_message(
            delegate.key(),
            &vec![].into(),
            None,
            vec![InboundDelegateMsg::ApplicationMessage(msg)],
        )?;

        let response: OutboundAppMessage = match &outbound[0] {
            OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
            _ => panic!("Expected ApplicationMessage"),
        };
        assert!(
            matches!(response, OutboundAppMessage::CounterValue(1)),
            "First increment should return 1"
        );

        // Second call (separate inbound_app_message): increment counter again
        // Since context is reset, it should also return 1, not 2
        let payload = bincode::serialize(&InboundAppMessage::IncrementCounter)?;
        let msg = ApplicationMessage::new(app, payload);
        let outbound = runtime.inbound_app_message(
            delegate.key(),
            &vec![].into(),
            None,
            vec![InboundDelegateMsg::ApplicationMessage(msg)],
        )?;

        let response: OutboundAppMessage = match &outbound[0] {
            OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
            _ => panic!("Expected ApplicationMessage"),
        };
        assert!(
            matches!(response, OutboundAppMessage::CounterValue(1)),
            "Second call should also return 1 (context reset between calls)"
        );

        std::mem::drop(temp_dir);
        Ok(())
    }

    /// Test has_secret host function.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_has_secret_host_function() -> Result<(), Box<dyn std::error::Error>> {
        use delegate2_messages::{InboundAppMessage, OutboundAppMessage};

        let contract = WrappedContract::new(
            Arc::new(ContractCode::from(vec![1])),
            Parameters::from(vec![]),
        );
        let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_2).await?;
        let app = ContractInstanceId::try_from(contract.key.to_string()).unwrap();

        let secret_key = vec![10, 20, 30];
        let secret_value = vec![100, 200];

        // Step 1: Check that secret doesn't exist yet
        let payload = bincode::serialize(&InboundAppMessage::HasSecret(secret_key.clone()))?;
        let msg = ApplicationMessage::new(app, payload);
        let outbound = runtime.inbound_app_message(
            delegate.key(),
            &vec![].into(),
            None,
            vec![InboundDelegateMsg::ApplicationMessage(msg)],
        )?;

        let response: OutboundAppMessage = match &outbound[0] {
            OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
            _ => panic!("Expected ApplicationMessage"),
        };
        assert!(
            matches!(response, OutboundAppMessage::SecretExists(false)),
            "Secret should not exist yet"
        );

        // Step 2: Store the secret
        let payload = bincode::serialize(&InboundAppMessage::StoreSecret {
            key: secret_key.clone(),
            value: secret_value.clone(),
        })?;
        let msg = ApplicationMessage::new(app, payload);
        let _ = runtime.inbound_app_message(
            delegate.key(),
            &vec![].into(),
            None,
            vec![InboundDelegateMsg::ApplicationMessage(msg)],
        )?;

        // Step 3: Check that secret now exists
        let payload = bincode::serialize(&InboundAppMessage::HasSecret(secret_key.clone()))?;
        let msg = ApplicationMessage::new(app, payload);
        let outbound = runtime.inbound_app_message(
            delegate.key(),
            &vec![].into(),
            None,
            vec![InboundDelegateMsg::ApplicationMessage(msg)],
        )?;

        let response: OutboundAppMessage = match &outbound[0] {
            OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
            _ => panic!("Expected ApplicationMessage"),
        };
        assert!(
            matches!(response, OutboundAppMessage::SecretExists(true)),
            "Secret should exist after storing"
        );

        std::mem::drop(temp_dir);
        Ok(())
    }

    /// Test get_secret returns None for non-existent secrets.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_get_nonexistent_secret() -> Result<(), Box<dyn std::error::Error>> {
        use delegate2_messages::{InboundAppMessage, OutboundAppMessage};

        let contract = WrappedContract::new(
            Arc::new(ContractCode::from(vec![1])),
            Parameters::from(vec![]),
        );
        let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_2).await?;
        let app = ContractInstanceId::try_from(contract.key.to_string()).unwrap();

        // Try to get a secret that doesn't exist
        let nonexistent_key = vec![99, 98, 97];
        let payload =
            bincode::serialize(&InboundAppMessage::GetNonExistentSecret(nonexistent_key))?;
        let msg = ApplicationMessage::new(app, payload);
        let outbound = runtime.inbound_app_message(
            delegate.key(),
            &vec![].into(),
            None,
            vec![InboundDelegateMsg::ApplicationMessage(msg)],
        )?;

        let response: OutboundAppMessage = match &outbound[0] {
            OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
            _ => panic!("Expected ApplicationMessage"),
        };
        assert!(
            matches!(response, OutboundAppMessage::SecretResult(None)),
            "Should return None for non-existent secret"
        );

        std::mem::drop(temp_dir);
        Ok(())
    }

    /// Test that secrets stored via host function can be retrieved via host function.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_store_and_retrieve_secret() -> Result<(), Box<dyn std::error::Error>> {
        use delegate2_messages::{InboundAppMessage, OutboundAppMessage};

        let contract = WrappedContract::new(
            Arc::new(ContractCode::from(vec![1])),
            Parameters::from(vec![]),
        );
        let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_2).await?;
        let app = ContractInstanceId::try_from(contract.key.to_string()).unwrap();

        let secret_key = vec![42, 43, 44];
        let secret_value = vec![1, 2, 3, 4, 5, 6, 7, 8];

        // Step 1: Store the secret
        let payload = bincode::serialize(&InboundAppMessage::StoreSecret {
            key: secret_key.clone(),
            value: secret_value.clone(),
        })?;
        let msg = ApplicationMessage::new(app, payload);
        let outbound = runtime.inbound_app_message(
            delegate.key(),
            &vec![].into(),
            None,
            vec![InboundDelegateMsg::ApplicationMessage(msg)],
        )?;

        let response: OutboundAppMessage = match &outbound[0] {
            OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
            _ => panic!("Expected ApplicationMessage"),
        };
        assert!(matches!(response, OutboundAppMessage::SecretStored));

        // Step 2: Retrieve the secret using GetNonExistentSecret (which just calls get_secret)
        let payload =
            bincode::serialize(&InboundAppMessage::GetNonExistentSecret(secret_key.clone()))?;
        let msg = ApplicationMessage::new(app, payload);
        let outbound = runtime.inbound_app_message(
            delegate.key(),
            &vec![].into(),
            None,
            vec![InboundDelegateMsg::ApplicationMessage(msg)],
        )?;

        let response: OutboundAppMessage = match &outbound[0] {
            OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
            _ => panic!("Expected ApplicationMessage"),
        };
        match response {
            OutboundAppMessage::SecretResult(Some(value)) => {
                assert_eq!(
                    value, secret_value,
                    "Retrieved secret should match stored value"
                );
            }
            other => panic!("Expected SecretResult(Some(...)), got {:?}", other),
        }

        std::mem::drop(temp_dir);
        Ok(())
    }

    /// Test that empty context is handled correctly.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_read_empty_context() -> Result<(), Box<dyn std::error::Error>> {
        use delegate2_messages::{InboundAppMessage, OutboundAppMessage};

        let contract = WrappedContract::new(
            Arc::new(ContractCode::from(vec![1])),
            Parameters::from(vec![]),
        );
        let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_2).await?;
        let app = ContractInstanceId::try_from(contract.key.to_string()).unwrap();

        // Read context without writing anything first — should return empty
        let payload = bincode::serialize(&InboundAppMessage::ReadContext)?;
        let msg = ApplicationMessage::new(app, payload);
        let outbound = runtime.inbound_app_message(
            delegate.key(),
            &vec![].into(),
            None,
            vec![InboundDelegateMsg::ApplicationMessage(msg)],
        )?;

        let response: OutboundAppMessage = match &outbound[0] {
            OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
            _ => panic!("Expected ApplicationMessage"),
        };
        match response {
            OutboundAppMessage::ContextData(data) => {
                assert!(data.is_empty(), "Context should be empty initially");
            }
            other => panic!("Expected ContextData, got {:?}", other),
        }

        std::mem::drop(temp_dir);
        Ok(())
    }

    /// Test that context can be cleared using ctx.clear().
    #[tokio::test(flavor = "multi_thread")]
    async fn test_context_clear() -> Result<(), Box<dyn std::error::Error>> {
        use delegate2_messages::{InboundAppMessage, OutboundAppMessage};

        let contract = WrappedContract::new(
            Arc::new(ContractCode::from(vec![1])),
            Parameters::from(vec![]),
        );
        let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_2).await?;
        let app = ContractInstanceId::try_from(contract.key.to_string()).unwrap();

        // Step 1: Write some data to context
        let test_data = vec![1, 2, 3, 4, 5];
        let payload = bincode::serialize(&InboundAppMessage::WriteContext(test_data.clone()))?;
        let msg = ApplicationMessage::new(app, payload);
        let _ = runtime.inbound_app_message(
            delegate.key(),
            &vec![].into(),
            None,
            vec![InboundDelegateMsg::ApplicationMessage(msg)],
        )?;

        // Step 2: Clear the context
        let payload = bincode::serialize(&InboundAppMessage::ClearContext)?;
        let msg = ApplicationMessage::new(app, payload);
        let outbound = runtime.inbound_app_message(
            delegate.key(),
            &vec![].into(),
            None,
            vec![InboundDelegateMsg::ApplicationMessage(msg)],
        )?;

        let response: OutboundAppMessage = match &outbound[0] {
            OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
            _ => panic!("Expected ApplicationMessage"),
        };
        assert!(
            matches!(response, OutboundAppMessage::ContextCleared),
            "Expected ContextCleared response"
        );

        // Step 3: Read context and verify it's empty
        let payload = bincode::serialize(&InboundAppMessage::ReadContext)?;
        let msg = ApplicationMessage::new(app, payload);
        let outbound = runtime.inbound_app_message(
            delegate.key(),
            &vec![].into(),
            None,
            vec![InboundDelegateMsg::ApplicationMessage(msg)],
        )?;

        let response: OutboundAppMessage = match &outbound[0] {
            OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
            _ => panic!("Expected ApplicationMessage"),
        };
        match response {
            OutboundAppMessage::ContextData(data) => {
                assert!(data.is_empty(), "Context should be empty after clear");
            }
            other => panic!("Expected ContextData, got {:?}", other),
        }

        std::mem::drop(temp_dir);
        Ok(())
    }

    /// Test multiple messages in a single inbound_app_message call share context.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_context_shared_across_batch() -> Result<(), Box<dyn std::error::Error>> {
        use delegate2_messages::{InboundAppMessage, OutboundAppMessage};

        let contract = WrappedContract::new(
            Arc::new(ContractCode::from(vec![1])),
            Parameters::from(vec![]),
        );
        let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_2).await?;
        let app = ContractInstanceId::try_from(contract.key.to_string()).unwrap();

        // Send multiple IncrementCounter messages in one batch
        let messages: Vec<InboundDelegateMsg> = (0..3)
            .map(|_| {
                let payload = bincode::serialize(&InboundAppMessage::IncrementCounter).unwrap();
                InboundDelegateMsg::ApplicationMessage(ApplicationMessage::new(app, payload))
            })
            .collect();

        let outbound =
            runtime.inbound_app_message(delegate.key(), &vec![].into(), None, messages)?;

        // Should get 3 responses with counter values 1, 2, 3
        assert_eq!(outbound.len(), 3, "Should have 3 responses");

        for (i, msg) in outbound.iter().enumerate() {
            let response: OutboundAppMessage = match msg {
                OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
                _ => panic!("Expected ApplicationMessage"),
            };
            match response {
                OutboundAppMessage::CounterValue(value) => {
                    assert_eq!(
                        value,
                        (i + 1) as u32,
                        "Counter should increment across batch"
                    );
                }
                other => panic!("Expected CounterValue, got {:?}", other),
            }
        }

        std::mem::drop(temp_dir);
        Ok(())
    }

    /// Test remove_secret host function.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_remove_secret_host_function() -> Result<(), Box<dyn std::error::Error>> {
        use delegate2_messages::{InboundAppMessage, OutboundAppMessage};

        let contract = WrappedContract::new(
            Arc::new(ContractCode::from(vec![1])),
            Parameters::from(vec![]),
        );
        let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_2).await?;
        let app = ContractInstanceId::try_from(contract.key.to_string()).unwrap();

        let secret_key = vec![50, 51, 52];
        let secret_value = vec![200, 201, 202];

        // Step 1: Store a secret
        let payload = bincode::serialize(&InboundAppMessage::StoreSecret {
            key: secret_key.clone(),
            value: secret_value.clone(),
        })?;
        let msg = ApplicationMessage::new(app, payload);
        let _ = runtime.inbound_app_message(
            delegate.key(),
            &vec![].into(),
            None,
            vec![InboundDelegateMsg::ApplicationMessage(msg)],
        )?;

        // Step 2: Verify it exists
        let payload = bincode::serialize(&InboundAppMessage::HasSecret(secret_key.clone()))?;
        let msg = ApplicationMessage::new(app, payload);
        let outbound = runtime.inbound_app_message(
            delegate.key(),
            &vec![].into(),
            None,
            vec![InboundDelegateMsg::ApplicationMessage(msg)],
        )?;
        let response: OutboundAppMessage = match &outbound[0] {
            OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
            _ => panic!("Expected ApplicationMessage"),
        };
        assert!(
            matches!(response, OutboundAppMessage::SecretExists(true)),
            "Secret should exist before removal"
        );

        // Step 3: Remove the secret
        let payload = bincode::serialize(&InboundAppMessage::RemoveSecret(secret_key.clone()))?;
        let msg = ApplicationMessage::new(app, payload);
        let outbound = runtime.inbound_app_message(
            delegate.key(),
            &vec![].into(),
            None,
            vec![InboundDelegateMsg::ApplicationMessage(msg)],
        )?;
        let response: OutboundAppMessage = match &outbound[0] {
            OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
            _ => panic!("Expected ApplicationMessage"),
        };
        assert!(
            matches!(response, OutboundAppMessage::SecretRemoved),
            "Should acknowledge removal"
        );

        // Step 4: Verify it no longer exists
        let payload = bincode::serialize(&InboundAppMessage::HasSecret(secret_key.clone()))?;
        let msg = ApplicationMessage::new(app, payload);
        let outbound = runtime.inbound_app_message(
            delegate.key(),
            &vec![].into(),
            None,
            vec![InboundDelegateMsg::ApplicationMessage(msg)],
        )?;
        let response: OutboundAppMessage = match &outbound[0] {
            OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
            _ => panic!("Expected ApplicationMessage"),
        };
        assert!(
            matches!(response, OutboundAppMessage::SecretExists(false)),
            "Secret should not exist after removal"
        );

        std::mem::drop(temp_dir);
        Ok(())
    }

    /// Test large context data (1MB).
    #[tokio::test(flavor = "multi_thread")]
    async fn test_large_context_data() -> Result<(), Box<dyn std::error::Error>> {
        use delegate2_messages::{InboundAppMessage, OutboundAppMessage};

        let contract = WrappedContract::new(
            Arc::new(ContractCode::from(vec![1])),
            Parameters::from(vec![]),
        );
        let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_2).await?;
        let app = ContractInstanceId::try_from(contract.key.to_string()).unwrap();

        let large_size = 1024 * 1024; // 1MB

        // Write large context
        let payload = bincode::serialize(&InboundAppMessage::WriteLargeContext(large_size))?;
        let msg = ApplicationMessage::new(app, payload);
        let outbound = runtime.inbound_app_message(
            delegate.key(),
            &vec![].into(),
            None,
            vec![InboundDelegateMsg::ApplicationMessage(msg)],
        )?;
        let response: OutboundAppMessage = match &outbound[0] {
            OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
            _ => panic!("Expected ApplicationMessage"),
        };
        match response {
            OutboundAppMessage::LargeContextWritten(size) => {
                assert_eq!(size, large_size, "Should report correct size written");
            }
            other => panic!("Expected LargeContextWritten, got {:?}", other),
        }

        // Read it back and verify
        let payload = bincode::serialize(&InboundAppMessage::ReadContext)?;
        let msg = ApplicationMessage::new(app, payload);
        let outbound = runtime.inbound_app_message(
            delegate.key(),
            &vec![].into(),
            None,
            vec![InboundDelegateMsg::ApplicationMessage(msg)],
        )?;
        let response: OutboundAppMessage = match &outbound[0] {
            OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
            _ => panic!("Expected ApplicationMessage"),
        };
        match response {
            OutboundAppMessage::ContextData(data) => {
                // Note: context is reset between calls, so this should be empty
                // But within the same call batch, it would persist
                assert!(
                    data.is_empty(),
                    "Context should be empty (reset between calls)"
                );
            }
            other => panic!("Expected ContextData, got {:?}", other),
        }

        std::mem::drop(temp_dir);
        Ok(())
    }

    /// Test large context persistence within the same batch.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_large_context_within_batch() -> Result<(), Box<dyn std::error::Error>> {
        use delegate2_messages::{InboundAppMessage, OutboundAppMessage};

        let contract = WrappedContract::new(
            Arc::new(ContractCode::from(vec![1])),
            Parameters::from(vec![]),
        );
        let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_2).await?;
        let app = ContractInstanceId::try_from(contract.key.to_string()).unwrap();

        // MAX_SIZE in DelegateContext is ~400KB, so use less
        let large_size = 256 * 1024; // 256KB

        // Write large context and read it back in the same batch
        let write_payload = bincode::serialize(&InboundAppMessage::WriteLargeContext(large_size))?;
        let read_payload = bincode::serialize(&InboundAppMessage::ReadContext)?;

        let messages = vec![
            InboundDelegateMsg::ApplicationMessage(ApplicationMessage::new(app, write_payload)),
            InboundDelegateMsg::ApplicationMessage(ApplicationMessage::new(app, read_payload)),
        ];

        let outbound =
            runtime.inbound_app_message(delegate.key(), &vec![].into(), None, messages)?;

        assert_eq!(outbound.len(), 2, "Should have 2 responses");

        // Second response should have the large data
        let response: OutboundAppMessage = match &outbound[1] {
            OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
            _ => panic!("Expected ApplicationMessage"),
        };
        match response {
            OutboundAppMessage::ContextData(data) => {
                assert_eq!(
                    data.len(),
                    large_size,
                    "Context should contain the large data within batch"
                );
                // Verify data pattern
                for (i, byte) in data.iter().enumerate() {
                    assert_eq!(*byte, (i % 256) as u8, "Data pattern mismatch at index {i}");
                }
            }
            other => panic!("Expected ContextData, got {:?}", other),
        }

        std::mem::drop(temp_dir);
        Ok(())
    }

    /// Test large secret data (1MB).
    #[tokio::test(flavor = "multi_thread")]
    async fn test_large_secret_data() -> Result<(), Box<dyn std::error::Error>> {
        use delegate2_messages::{InboundAppMessage, OutboundAppMessage};

        let contract = WrappedContract::new(
            Arc::new(ContractCode::from(vec![1])),
            Parameters::from(vec![]),
        );
        let (delegate, mut runtime, temp_dir) = setup_runtime(TEST_DELEGATE_2).await?;
        let app = ContractInstanceId::try_from(contract.key.to_string()).unwrap();

        let secret_key = vec![77, 88, 99];
        let large_size = 1024 * 1024; // 1MB

        // Store large secret
        let payload = bincode::serialize(&InboundAppMessage::StoreLargeSecret {
            key: secret_key.clone(),
            size: large_size,
        })?;
        let msg = ApplicationMessage::new(app, payload);
        let outbound = runtime.inbound_app_message(
            delegate.key(),
            &vec![].into(),
            None,
            vec![InboundDelegateMsg::ApplicationMessage(msg)],
        )?;
        let response: OutboundAppMessage = match &outbound[0] {
            OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
            _ => panic!("Expected ApplicationMessage"),
        };
        match response {
            OutboundAppMessage::LargeSecretStored(size) => {
                assert_eq!(size, large_size, "Should report correct size stored");
            }
            other => panic!("Expected LargeSecretStored, got {:?}", other),
        }

        // Retrieve it using GetNonExistentSecret (which just calls get_secret)
        let payload =
            bincode::serialize(&InboundAppMessage::GetNonExistentSecret(secret_key.clone()))?;
        let msg = ApplicationMessage::new(app, payload);
        let outbound = runtime.inbound_app_message(
            delegate.key(),
            &vec![].into(),
            None,
            vec![InboundDelegateMsg::ApplicationMessage(msg)],
        )?;
        let response: OutboundAppMessage = match &outbound[0] {
            OutboundDelegateMsg::ApplicationMessage(m) => bincode::deserialize(&m.payload)?,
            _ => panic!("Expected ApplicationMessage"),
        };
        match response {
            OutboundAppMessage::SecretResult(Some(data)) => {
                assert_eq!(data.len(), large_size, "Secret should be correct size");
                // Verify data pattern
                for (i, byte) in data.iter().enumerate() {
                    assert_eq!(*byte, (i % 256) as u8, "Data pattern mismatch at index {i}");
                }
            }
            other => panic!("Expected SecretResult(Some(...)), got {:?}", other),
        }

        std::mem::drop(temp_dir);
        Ok(())
    }

    /// Test concurrent delegate execution on separate threads.
    /// This verifies that two delegates running in parallel don't interfere with each other.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrent_delegate_execution() -> Result<(), Box<dyn std::error::Error>> {
        use delegate2_messages::{InboundAppMessage, OutboundAppMessage};
        use std::sync::Arc;
        use tokio::sync::Barrier;

        // Set up two independent runtimes with their own delegates
        let (delegate1, runtime1, temp_dir1) = setup_runtime(TEST_DELEGATE_2).await?;
        let (delegate2, runtime2, temp_dir2) = setup_runtime(TEST_DELEGATE_2).await?;

        let contract = WrappedContract::new(
            Arc::new(ContractCode::from(vec![1])),
            Parameters::from(vec![]),
        );
        let app = ContractInstanceId::try_from(contract.key.to_string()).unwrap();

        // Wrap in Arc<Mutex> for thread-safe access
        let runtime1 = Arc::new(std::sync::Mutex::new(runtime1));
        let runtime2 = Arc::new(std::sync::Mutex::new(runtime2));
        let delegate1 = Arc::new(delegate1);
        let delegate2 = Arc::new(delegate2);

        // Use a barrier to synchronize the two threads
        let barrier = Arc::new(Barrier::new(2));

        // Thread 1: Store and retrieve secret with key "thread1"
        let barrier1 = barrier.clone();
        let runtime1_clone = runtime1.clone();
        let delegate1_clone = delegate1.clone();
        let handle1 = tokio::spawn(async move {
            barrier1.wait().await;

            let secret_key = b"thread1_key".to_vec();
            let secret_value = b"thread1_value".to_vec();

            // Store secret
            let payload = bincode::serialize(&InboundAppMessage::StoreSecret {
                key: secret_key.clone(),
                value: secret_value.clone(),
            })
            .unwrap();
            let msg = ApplicationMessage::new(app, payload);
            {
                let mut runtime = runtime1_clone.lock().unwrap();
                let _ = runtime
                    .inbound_app_message(
                        delegate1_clone.key(),
                        &vec![].into(),
                        None,
                        vec![InboundDelegateMsg::ApplicationMessage(msg)],
                    )
                    .unwrap();
            }

            // Retrieve and verify
            let payload =
                bincode::serialize(&InboundAppMessage::GetNonExistentSecret(secret_key.clone()))
                    .unwrap();
            let msg = ApplicationMessage::new(app, payload);
            let outbound = {
                let mut runtime = runtime1_clone.lock().unwrap();
                runtime
                    .inbound_app_message(
                        delegate1_clone.key(),
                        &vec![].into(),
                        None,
                        vec![InboundDelegateMsg::ApplicationMessage(msg)],
                    )
                    .unwrap()
            };

            let response: OutboundAppMessage = match &outbound[0] {
                OutboundDelegateMsg::ApplicationMessage(m) => {
                    bincode::deserialize(&m.payload).unwrap()
                }
                _ => panic!("Expected ApplicationMessage"),
            };
            match response {
                OutboundAppMessage::SecretResult(Some(value)) => {
                    assert_eq!(value, secret_value, "Thread 1 secret mismatch");
                }
                other => panic!(
                    "Thread 1: Expected SecretResult(Some(...)), got {:?}",
                    other
                ),
            }
        });

        // Thread 2: Store and retrieve secret with key "thread2"
        let barrier2 = barrier.clone();
        let runtime2_clone = runtime2.clone();
        let delegate2_clone = delegate2.clone();
        let handle2 = tokio::spawn(async move {
            barrier2.wait().await;

            let secret_key = b"thread2_key".to_vec();
            let secret_value = b"thread2_value".to_vec();

            // Store secret
            let payload = bincode::serialize(&InboundAppMessage::StoreSecret {
                key: secret_key.clone(),
                value: secret_value.clone(),
            })
            .unwrap();
            let msg = ApplicationMessage::new(app, payload);
            {
                let mut runtime = runtime2_clone.lock().unwrap();
                let _ = runtime
                    .inbound_app_message(
                        delegate2_clone.key(),
                        &vec![].into(),
                        None,
                        vec![InboundDelegateMsg::ApplicationMessage(msg)],
                    )
                    .unwrap();
            }

            // Retrieve and verify
            let payload =
                bincode::serialize(&InboundAppMessage::GetNonExistentSecret(secret_key.clone()))
                    .unwrap();
            let msg = ApplicationMessage::new(app, payload);
            let outbound = {
                let mut runtime = runtime2_clone.lock().unwrap();
                runtime
                    .inbound_app_message(
                        delegate2_clone.key(),
                        &vec![].into(),
                        None,
                        vec![InboundDelegateMsg::ApplicationMessage(msg)],
                    )
                    .unwrap()
            };

            let response: OutboundAppMessage = match &outbound[0] {
                OutboundDelegateMsg::ApplicationMessage(m) => {
                    bincode::deserialize(&m.payload).unwrap()
                }
                _ => panic!("Expected ApplicationMessage"),
            };
            match response {
                OutboundAppMessage::SecretResult(Some(value)) => {
                    assert_eq!(value, secret_value, "Thread 2 secret mismatch");
                }
                other => panic!(
                    "Thread 2: Expected SecretResult(Some(...)), got {:?}",
                    other
                ),
            }
        });

        // Wait for both threads to complete
        handle1.await?;
        handle2.await?;

        std::mem::drop(temp_dir1);
        std::mem::drop(temp_dir2);
        Ok(())
    }
}

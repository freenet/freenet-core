use std::collections::VecDeque;

use freenet_stdlib::prelude::{
    DelegateInterfaceResult, DelegateKey, DelegateMessage, GetContractRequest, InboundDelegateMsg,
    MessageOrigin, OutboundDelegateMsg, Parameters, PutContractRequest, SubscribeContractRequest,
    UpdateContractRequest,
};

use crate::wasm_runtime::delegate_api::DelegateApiVersion;

use super::super::engine::{InstanceHandle, WasmEngine};
use super::super::native_api::{
    CURRENT_DELEGATE_INSTANCE, DELEGATE_ENV, DelegateCallEnv, InstanceId,
};
use super::super::secrets_store::UserSecretContext;
use super::super::{Runtime, RuntimeResult};
use super::error::DelegateExecError;

/// RAII guard that ensures cleanup of delegate environment state.
/// When dropped, it clears the thread-local instance ID and removes the
/// entry from the global DELEGATE_ENV map.
pub(super) struct DelegateEnvGuard {
    instance_id: InstanceId,
}

impl DelegateEnvGuard {
    pub(super) fn new(instance_id: InstanceId) -> Self {
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

impl Runtime {
    /// Execute the delegate's `process` function with the DelegateCallEnv set up
    /// so that host functions for context and secret access are available.
    ///
    /// Uses RAII guard pattern to ensure cleanup happens even if WASM execution panics.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn exec_inbound_with_env(
        &mut self,
        delegate_key: &DelegateKey,
        params: &Parameters<'_>,
        origin: Option<&MessageOrigin>,
        user_context: Option<&UserSecretContext>,
        msg: &InboundDelegateMsg,
        context: Vec<u8>,
        handle: &InstanceHandle,
        instance_id: i64,
        api_version: DelegateApiVersion,
    ) -> RuntimeResult<(Vec<OutboundDelegateMsg>, Vec<u8>)> {
        // Set up the delegate call environment with context, secret store, and
        // contract store access.
        // SAFETY: `self.secret_store` and `self.contract_store` are valid for the
        // duration of the WASM `process()` call below, and the `DelegateEnvGuard`
        // ensures the env is removed from `DELEGATE_ENV` before this function returns.
        // Build the origin_contracts list from the MessageOrigin. Only WebApp
        // attestations grant the receiving delegate access to a contract on
        // behalf of the caller; an inter-delegate caller (Delegate variant)
        // does not propagate contract access — its identity is conveyed only
        // via the `origin` argument forwarded into the WASM `process()` call.
        let origin_contracts = match origin {
            Some(MessageOrigin::WebApp(contract_id)) => vec![*contract_id],
            Some(MessageOrigin::Delegate(_)) | None => Vec::new(),
            // MessageOrigin is `#[non_exhaustive]`; future variants reach
            // this arm because the compiler requires it. Default to "no
            // contract access" (fail closed) AND log a warning so the gap
            // is visible during the PR that adds the new variant — the
            // catch-all should not silently default in production.
            Some(other) => {
                tracing::warn!(
                    delegate_key = %delegate_key,
                    origin = ?other,
                    "Unknown MessageOrigin variant reached fail-closed default; \
                     wasm_runtime::delegate::Runtime::inbound_app_message must \
                     decide explicitly whether this variant grants contract access"
                );
                Vec::new()
            }
        };

        // SAFETY: The `DelegateCallEnv` does not outlive `self`. The raw pointers to
        // `secret_store`, `contract_store`, and `delegate_store` remain valid for the
        // duration of the WASM `process()` call, and are cleaned up via DELEGATE_ENV
        // removal below.
        let env = unsafe {
            DelegateCallEnv::new(
                context,
                &mut self.secret_store,
                &self.contract_store,
                self.state_store_db.clone(),
                self.state_write_callback.clone(),
                delegate_key.clone(),
                &mut self.delegate_store,
                0, // creation_depth: always 0 for top-level calls
                origin_contracts,
                // Clone the connection's user context into the env so the
                // owned `dek_secret` outlives every secret call during this
                // `process()` invocation. The context is read-only here; the
                // delegate cannot mutate or forge it. `None` outside hosted
                // mode keeps secret ops on `SecretScope::Local`.
                user_context.cloned(),
            )
        };

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
        let result = self.exec_inbound(params, origin, msg, handle, api_version);

        // Read back the (possibly mutated) context before guard drops
        let updated_context = DELEGATE_ENV
            .get(&instance_id)
            .map(|env| env.context.clone())
            .unwrap_or_default();

        let outbound = result?;
        Ok((outbound, updated_context))
    }

    pub(super) fn exec_inbound(
        &mut self,
        params: &Parameters<'_>,
        origin: Option<&MessageOrigin>,
        msg: &InboundDelegateMsg,
        handle: &InstanceHandle,
        api_version: DelegateApiVersion,
    ) -> RuntimeResult<Vec<OutboundDelegateMsg>> {
        let param_buf_ptr = {
            let mut param_buf = self.init_buf(handle, params)?;
            param_buf.write(params)?;
            param_buf.ptr()
        };
        let origin_buf_ptr = {
            let bytes = match origin {
                Some(o) => bincode::serialize(o)?,
                None => Vec::new(),
            };
            let mut origin_buf = self.init_buf(handle, &bytes)?;
            origin_buf.write(bytes)?;
            origin_buf.ptr()
        };
        let msg_ptr = {
            let msg = bincode::serialize(msg)?;
            let mut msg_buf = self.init_buf(handle, &msg)?;
            msg_buf.write(msg)?;
            msg_buf.ptr()
        };
        let inbound_msg_name = match msg {
            InboundDelegateMsg::ApplicationMessage(_) => "ApplicationMessage",
            InboundDelegateMsg::UserResponse(_) => "UserResponse",
            InboundDelegateMsg::GetContractResponse(_) => "GetContractResponse",
            InboundDelegateMsg::PutContractResponse(_) => "PutContractResponse",
            InboundDelegateMsg::UpdateContractResponse(_) => "UpdateContractResponse",
            InboundDelegateMsg::SubscribeContractResponse(_) => "SubscribeContractResponse",
            InboundDelegateMsg::ContractNotification(_) => "ContractNotification",
            InboundDelegateMsg::DelegateMessage(_) => "DelegateMessage",
            // `InboundDelegateMsg` is `#[non_exhaustive]` (stdlib 0.6.0+).
            // Future variants land here for tracing only — they still flow
            // through the wasm boundary as raw bincode below; classifying
            // them as "Unknown" affects logs only, not delivery.
            _ => "Unknown",
        };
        tracing::debug!(
            inbound_msg_name,
            api_version = %api_version,
            "Calling delegate with inbound message"
        );

        let res = match api_version {
            DelegateApiVersion::V1 => {
                // V1: synchronous call — no async host functions involved.
                // Must stay on calling thread for thread-local env.
                self.engine.call_3i64(
                    handle,
                    "process",
                    param_buf_ptr as i64,
                    origin_buf_ptr as i64,
                    msg_ptr as i64,
                )?
            }
            DelegateApiVersion::V2 => {
                // V2: async call — contract host functions are async.
                // Uses Store::into_async() + call_async() under the hood.
                self.engine.call_3i64_async_imports(
                    handle,
                    "process",
                    param_buf_ptr as i64,
                    origin_buf_ptr as i64,
                    msg_ptr as i64,
                )?
            }
        };

        let linear_mem = self.linear_mem(handle)?;
        // SAFETY: `res` is the return value from the WASM `process` call and
        // `linear_mem` points to the instance's live linear memory, so `from_raw`
        // reads a valid, in-bounds result descriptor.
        let outbound = unsafe {
            DelegateInterfaceResult::from_raw(res, &linear_mem)
                .unwrap(linear_mem)
                .map_err(Into::<DelegateExecError>::into)?
        };
        self.log_delegate_exec_result(inbound_msg_name, &outbound);
        Ok(outbound)
    }

    pub(super) fn log_delegate_exec_result(
        &self,
        inbound_msg_name: &str,
        outbound: &[OutboundDelegateMsg],
    ) {
        if tracing::enabled!(tracing::Level::DEBUG) {
            let outbound_message_names = outbound
                .iter()
                .map(|m| match m {
                    OutboundDelegateMsg::ApplicationMessage(am) => format!(
                        "ApplicationMessage(payload_len={}, processed={}, context_len={})",
                        am.payload.len(),
                        am.processed,
                        am.context.as_ref().len()
                    ),
                    OutboundDelegateMsg::RequestUserInput(_) => "RequestUserInput".to_string(),
                    OutboundDelegateMsg::ContextUpdated(_) => "ContextUpdated".to_string(),
                    OutboundDelegateMsg::GetContractRequest(req) => {
                        format!("GetContractRequest(contract={})", req.contract_id)
                    }
                    OutboundDelegateMsg::PutContractRequest(req) => {
                        format!("PutContractRequest(contract={})", req.contract.key())
                    }
                    OutboundDelegateMsg::UpdateContractRequest(req) => {
                        format!("UpdateContractRequest(contract={})", req.contract_id)
                    }
                    OutboundDelegateMsg::SubscribeContractRequest(req) => {
                        format!("SubscribeContractRequest(contract={})", req.contract_id)
                    }
                    OutboundDelegateMsg::SendDelegateMessage(msg) => {
                        format!(
                            "SendDelegateMessage(target={}, payload_len={})",
                            msg.target,
                            msg.payload.len()
                        )
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

    pub(super) fn log_process_outbound_entry(
        &self,
        delegate_key: &DelegateKey,
        origin: Option<&MessageOrigin>,
        outbound_msgs: &VecDeque<OutboundDelegateMsg>,
    ) {
        tracing::debug!(
            delegate_key = ?delegate_key,
            ?origin,
            outbound_msgs_len = outbound_msgs.len(),
            outbound_msg_details = debug(if tracing::enabled!(tracing::Level::DEBUG) {
                outbound_msgs.iter().map(|msg| {
                    match msg {
                        OutboundDelegateMsg::ApplicationMessage(m) => format!("AppMsg(payload_len={})", m.payload.len()),
                        OutboundDelegateMsg::RequestUserInput(_) => "UserInputReq".to_string(),
                        OutboundDelegateMsg::ContextUpdated(_) => "ContextUpdate".to_string(),
                        OutboundDelegateMsg::GetContractRequest(r) => format!("GetContractReq({})", r.contract_id),
                        OutboundDelegateMsg::PutContractRequest(r) => format!("PutContractReq({})", r.contract.key()),
                        OutboundDelegateMsg::UpdateContractRequest(r) => format!("UpdateContractReq({})", r.contract_id),
                        OutboundDelegateMsg::SubscribeContractRequest(r) => format!("SubscribeContractReq({})", r.contract_id),
                        OutboundDelegateMsg::SendDelegateMessage(m) => format!("SendDelegateMsg(target={})", m.target),
                    }
                }).collect::<Vec<_>>()
            } else {
                Vec::new()
            }),
            "process_outbound called"
        );
    }

    /// Process outbound messages from a delegate.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn process_outbound(
        &mut self,
        delegate_key: &DelegateKey,
        _handle: &InstanceHandle,
        _instance_id: i64,
        _params: &Parameters<'_>,
        origin: Option<&MessageOrigin>,
        outbound_msgs: &mut VecDeque<OutboundDelegateMsg>,
        context: &mut Vec<u8>,
        results: &mut Vec<OutboundDelegateMsg>,
    ) -> RuntimeResult<()> {
        self.log_process_outbound_entry(delegate_key, origin, outbound_msgs);

        while let Some(outbound) = outbound_msgs.pop_front() {
            match outbound {
                OutboundDelegateMsg::ApplicationMessage(mut msg) => {
                    tracing::debug!(
                        payload_len = msg.payload.len(),
                        processed = msg.processed,
                        "Adding ApplicationMessage to results"
                    );
                    msg.context = freenet_stdlib::prelude::DelegateContext::default();
                    results.push(OutboundDelegateMsg::ApplicationMessage(msg));
                    for remaining in outbound_msgs.drain(..) {
                        results.push(remaining);
                    }
                    break;
                }

                OutboundDelegateMsg::RequestUserInput(req) => {
                    tracing::debug!(
                        request_id = req.request_id,
                        "Passing RequestUserInput to executor for user prompting"
                    );
                    results.push(OutboundDelegateMsg::RequestUserInput(req));
                    for remaining in outbound_msgs.drain(..) {
                        results.push(remaining);
                    }
                    break;
                }

                OutboundDelegateMsg::ContextUpdated(new_context) => {
                    // avoid alloc churn — buffer reuse instead of to_vec
                    context.clear();
                    context.extend_from_slice(new_context.as_ref());
                }
                OutboundDelegateMsg::GetContractRequest(req) if !req.processed => {
                    tracing::debug!(
                        contract_id = %req.contract_id,
                        "Passing GetContractRequest to executor for async handling"
                    );
                    results.push(OutboundDelegateMsg::GetContractRequest(req));
                    for remaining in outbound_msgs.drain(..) {
                        results.push(remaining);
                    }
                    break;
                }
                OutboundDelegateMsg::GetContractRequest(GetContractRequest {
                    context: ctx,
                    ..
                }) => {
                    tracing::debug!("GetContractRequest processed");
                    context.clear();
                    context.extend_from_slice(ctx.as_ref());
                }
                OutboundDelegateMsg::PutContractRequest(req) if !req.processed => {
                    tracing::debug!(
                        contract = %req.contract.key(),
                        "Passing PutContractRequest to executor for async handling"
                    );
                    results.push(OutboundDelegateMsg::PutContractRequest(req));
                    for remaining in outbound_msgs.drain(..) {
                        results.push(remaining);
                    }
                    break;
                }
                OutboundDelegateMsg::PutContractRequest(PutContractRequest {
                    context: ctx,
                    ..
                }) => {
                    tracing::debug!("PutContractRequest processed");
                    context.clear();
                    context.extend_from_slice(ctx.as_ref());
                }
                OutboundDelegateMsg::UpdateContractRequest(req) if !req.processed => {
                    tracing::debug!(
                        contract_id = %req.contract_id,
                        "Passing UpdateContractRequest to executor for async handling"
                    );
                    results.push(OutboundDelegateMsg::UpdateContractRequest(req));
                    for remaining in outbound_msgs.drain(..) {
                        results.push(remaining);
                    }
                    break;
                }
                OutboundDelegateMsg::UpdateContractRequest(UpdateContractRequest {
                    context: ctx,
                    ..
                }) => {
                    tracing::debug!("UpdateContractRequest processed");
                    context.clear();
                    context.extend_from_slice(ctx.as_ref());
                }
                OutboundDelegateMsg::SubscribeContractRequest(req) if !req.processed => {
                    tracing::debug!(
                        contract_id = %req.contract_id,
                        "Passing SubscribeContractRequest to executor for async handling"
                    );
                    results.push(OutboundDelegateMsg::SubscribeContractRequest(req));
                    for remaining in outbound_msgs.drain(..) {
                        results.push(remaining);
                    }
                    break;
                }
                OutboundDelegateMsg::SubscribeContractRequest(SubscribeContractRequest {
                    context: ctx,
                    ..
                }) => {
                    tracing::debug!("SubscribeContractRequest processed");
                    context.clear();
                    context.extend_from_slice(ctx.as_ref());
                }
                OutboundDelegateMsg::SendDelegateMessage(mut msg) if !msg.processed => {
                    tracing::debug!(
                        target_delegate = %msg.target,
                        "Passing SendDelegateMessage to executor for delivery"
                    );
                    // Sender attestation: overwrite sender with the actual delegate key
                    msg.sender = delegate_key.clone();
                    results.push(OutboundDelegateMsg::SendDelegateMessage(msg));
                    // Attest any remaining SendDelegateMessage variants to prevent
                    // spoofing via drain bypass (see PR #3282 review).
                    for remaining in outbound_msgs.drain(..) {
                        match remaining {
                            OutboundDelegateMsg::SendDelegateMessage(mut m) if !m.processed => {
                                m.sender = delegate_key.clone();
                                results.push(OutboundDelegateMsg::SendDelegateMessage(m));
                            }
                            msg @ (OutboundDelegateMsg::ApplicationMessage(_)
                            | OutboundDelegateMsg::RequestUserInput(_)
                            | OutboundDelegateMsg::ContextUpdated(_)
                            | OutboundDelegateMsg::GetContractRequest(_)
                            | OutboundDelegateMsg::PutContractRequest(_)
                            | OutboundDelegateMsg::UpdateContractRequest(_)
                            | OutboundDelegateMsg::SubscribeContractRequest(_)
                            | OutboundDelegateMsg::SendDelegateMessage(_)) => results.push(msg),
                        }
                    }
                    break;
                }
                OutboundDelegateMsg::SendDelegateMessage(DelegateMessage {
                    context: ctx, ..
                }) => {
                    tracing::debug!("SendDelegateMessage processed");
                    context.clear();
                    context.extend_from_slice(ctx.as_ref());
                }
            }
        }
        Ok(())
    }
}

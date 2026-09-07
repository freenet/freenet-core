use std::collections::VecDeque;

use freenet_stdlib::prelude::{
    DelegateInterfaceResult, DelegateKey, DelegateMessage, GetContractRequest, InboundDelegateMsg,
    MessageOrigin, OutboundDelegateMsg, Parameters, PutContractRequest, SubscribeContractRequest,
    UpdateContractRequest,
};

use crate::wasm_runtime::delegate_api::DelegateApiVersion;

use super::super::engine::{InstanceHandle, WasmEngine};
use super::super::native_api::{
    CURRENT_DELEGATE_INSTANCE, DELEGATE_ENV, DelegateCallEnv, InstanceId, LIVE_DELEGATE_GUESTS,
};
use super::super::secrets_store::UserSecretContext;
use super::super::{Runtime, RuntimeResult};
use super::error::DelegateExecError;

/// RAII guard that removes the instance's entry from the global `DELEGATE_ENV`
/// map on every exit path, including a panic.
///
/// That removal is the load-bearing half, and it is what bounds the lifetime of
/// the raw store pointers the env holds: `DashMap::remove` takes the shard WRITE
/// lock, so it waits for any host call still holding a `Ref` before returning.
///
/// It also clears `CURRENT_DELEGATE_INSTANCE`, but note what that does and does
/// not do since #5480. The thread-local a delegate host function actually reads
/// is the one on the BLOCKING-POOL thread running the guest, installed and
/// cleared by `GuestDelegateInstance` in `wasmtime_engine.rs`. This clear (and
/// the matching `set` in `exec_inbound_with_env`) touches the CALLING thread's
/// copy, which no host function consults on the delegate path.
///
/// They are kept rather than deleted because they cost nothing and keep the
/// calling thread's thread-local honest for any path that ever runs a guest
/// inline. Do not read them as the mechanism that makes host-function dispatch
/// work — that is `GuestDelegateInstance`, and a change there is what would
/// break dispatch.
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

/// Drain every message remaining after a terminal arm of `process_outbound`,
/// re-attesting the sender of each unprocessed `SendDelegateMessage` to the
/// real `delegate_key` before pushing it into `results`.
///
/// This exists because a delegate can emit an unprocessed `SendDelegateMessage`
/// queued *behind* a terminal message of any variant (e.g. an
/// `ApplicationMessage`). Every terminal arm ends by draining the remainder of
/// the queue, and each such drain MUST overwrite the attacker-controlled
/// `sender` field — otherwise the target delegate observes a forged
/// `DelegateMessage.sender` (issue #4668; the fix for the `SendDelegateMessage`
/// arm alone was #3282). Centralizing the drain here keeps all terminal arms
/// consistent so a future arm cannot silently reintroduce the blind-drain.
///
/// The match lists every `OutboundDelegateMsg` variant exhaustively (rather
/// than a generic passthrough) so that adding a variant to the enum forces a
/// compile-time decision here instead of silently dropping or forwarding it.
fn drain_remaining_with_attestation(
    delegate_key: &DelegateKey,
    outbound_msgs: &mut VecDeque<OutboundDelegateMsg>,
    results: &mut Vec<OutboundDelegateMsg>,
) {
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
                self.state_admit_callback.clone(),
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
                // This node's created-delegate count, enforcing
                // MAX_CREATED_DELEGATES_PER_NODE across the pool's executors.
                self.created_delegates_count.clone(),
                // This node's attestation map, which a creation by an attested
                // parent extends with the child's inherited origins.
                self.inherited_origins.clone(),
            )
        };

        // HARD check, deliberately not `debug_assert!`: since #5480 this is a
        // MEMORY-SAFETY invariant, not a tidiness one, and it must hold in
        // release builds.
        //
        // A delegate guest now runs on a `spawn_blocking` worker, and on the
        // wall-clock timeout path it KEEPS RUNNING after this function returns
        // (`JoinHandle::abort()` cannot stop a `spawn_blocking` closure). One
        // `RunningInstance` id is shared by every message in a batch (see
        // `interface.rs`), so this insert runs once per message under the SAME
        // id. If an env were inserted while a previous message's guest were
        // still abandoned and running, that guest's `DELEGATE_ENV.get(&id)`
        // would resolve to the NEW env and dereference its raw store pointers
        // while this thread holds `&mut` to the very same stores -- aliasing UB
        // across two threads.
        //
        // Today the batch loop aborts on the first error, so this is
        // unreachable. That is an accident of control flow, not a guarantee: an
        // edit making the loop error-tolerant ("collect errors and continue",
        // "retry the message") would silently reintroduce it. Fail closed so
        // such an edit gets an error instead of undefined behaviour.
        //
        // BOTH halves are needed, and `DELEGATE_ENV` alone is the WRONG test.
        // `DelegateEnvGuard::drop` removes the env on every exit path of this
        // function INCLUDING the wall-clock-timeout `Err`, so by the time a
        // caller sees that error the entry is already gone while the guest is
        // still running on an abandoned blocking thread. `contains_key` asks
        // "is an env registered"; the question that matters is "is a guest
        // running", and those diverged the moment the guest could outlive the
        // call. `LIVE_DELEGATE_GUESTS` answers the second one — without it this
        // check would read false in precisely the scenario its own comment
        // above describes.
        if DELEGATE_ENV.contains_key(&instance_id) || LIVE_DELEGATE_GUESTS.contains(&instance_id) {
            return Err(anyhow::anyhow!(
                "delegate instance {instance_id} is already active (env registered, or a \
                 guest still running on an abandoned blocking thread); refusing to \
                 re-enter it (#5480)"
            )
            .into());
        }

        DELEGATE_ENV.insert(instance_id, env);
        CURRENT_DELEGATE_INSTANCE.with(|c| c.set(instance_id));

        // Create RAII guard to ensure cleanup on all exit paths (including panic)
        let _guard = DelegateEnvGuard::new(instance_id);

        // Execute the WASM process function.
        // V2 delegates use call_async (async host functions for contract access).
        // V1 delegates use synchronous call.
        let result = self.exec_inbound(params, origin, msg, handle, api_version);

        // Propagate the error BEFORE reading the context back. The `?` is
        // deliberately ahead of the read, not behind it (#5480).
        //
        // On the wall-clock-timeout path this thread returns while the guest is
        // STILL RUNNING on an abandoned blocking-pool thread, because
        // `JoinHandle::abort()` cannot stop a `spawn_blocking` closure. Reading
        // `context` here would therefore be a genuinely concurrent access to a
        // field the abandoned guest can still write through `context_write`.
        //
        // That IS a data race if the read happens above the `?`, and it became
        // one in #5593: `context` is now a `RefCell<Vec<u8>>` and
        // `context_write` mutates it through `DELEGATE_ENV.get` -- a shard READ
        // lock -- plus `borrow_mut()`. Shard read locks are SHARED, so nothing
        // separates the two threads any more. `RefCell`'s borrow flag is a
        // non-atomic `Cell<isize>`, so its own runtime check cannot detect the
        // overlap; and `to_vec()` reallocating the `Vec` while `clone()` reads
        // it is a use-after-free of the old buffer. `RefCell` is `!Sync`, but
        // the `unsafe impl Sync for DelegateCallEnv` overrides that, so the
        // compiler says nothing and there is no `unsafe` at either edit site.
        //
        // Before #5593 the mutator took `get_mut`, a shard WRITE lock, and the
        // ordering here did not matter. That was one call site's habit rather
        // than an invariant, which is exactly why it stopped holding. Do not
        // restore the habit as the defence; keep the read below the `?`.
        //
        // The read is pure waste on every error path regardless: `result?` used
        // to discard it a line later. Skipping it costs nothing, removes the
        // only concurrent touch of the env from this thread, and lets the
        // SAFETY argument rest on "the guest thread alone reaches the env"
        // rather than on a per-field exception.
        let outbound = result?;

        // Reached only on success, which means `execute_wasm_blocking` joined
        // the guest closure: the guest has finished and nothing else can be
        // touching the env.
        let updated_context = DELEGATE_ENV
            .get(&instance_id)
            .map(|env| env.context.borrow().clone())
            .unwrap_or_default();

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
                    drain_remaining_with_attestation(delegate_key, outbound_msgs, results);
                    break;
                }

                OutboundDelegateMsg::RequestUserInput(req) => {
                    tracing::debug!(
                        request_id = req.request_id,
                        "Passing RequestUserInput to executor for user prompting"
                    );
                    results.push(OutboundDelegateMsg::RequestUserInput(req));
                    drain_remaining_with_attestation(delegate_key, outbound_msgs, results);
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
                    drain_remaining_with_attestation(delegate_key, outbound_msgs, results);
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
                    drain_remaining_with_attestation(delegate_key, outbound_msgs, results);
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
                    drain_remaining_with_attestation(delegate_key, outbound_msgs, results);
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
                    drain_remaining_with_attestation(delegate_key, outbound_msgs, results);
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
                    // spoofing via drain bypass (see PR #3282 review, issue #4668).
                    drain_remaining_with_attestation(delegate_key, outbound_msgs, results);
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

#[cfg(test)]
mod pins {
    /// Source-scrape pin (#5480): in `exec_inbound_with_env`, the `?` that
    /// propagates the call's result MUST come BEFORE the `context` read-back.
    ///
    /// This is an ordering the compiler cannot enforce and that reads as a
    /// harmless rearrangement. It is not. On the wall-clock-timeout path the
    /// creating thread returns while the guest is still running on an abandoned
    /// `spawn_blocking` thread, so a read placed above the `?` runs concurrently
    /// with `native_api`'s `context_write`. Since #5593 both sides take only a
    /// SHARED `DELEGATE_ENV.get` and reach the `Vec` through a `RefCell`, whose
    /// borrow flag is a non-atomic `Cell<isize>` — a data race that `RefCell`'s
    /// own check cannot detect, that `!Sync` would normally catch, and that the
    /// `unsafe impl Sync for DelegateCallEnv` suppresses. Neither edit site
    /// needs `unsafe`, so nothing else would flag the change.
    ///
    /// Below the `?` the read is reached only on success, which means
    /// `execute_wasm_blocking` joined the guest closure and the guest is
    /// provably finished.
    #[test]
    fn context_readback_happens_after_the_result_is_propagated() {
        let src = include_str!("execution.rs");
        let start = src
            .find("fn exec_inbound_with_env(")
            .expect("`exec_inbound_with_env` not found — this pin has drifted");
        // Bound at the next method so a later `?` cannot satisfy the assertion.
        let rest = &src[start..];
        let end = ["\n    pub(super) fn ", "\n    fn ", "\n}"]
            .iter()
            .filter_map(|needle| rest.find(needle))
            .min()
            .map(|off| start + off)
            .unwrap_or(src.len());
        let body = &src[start..end];

        // Fail closed if the window was truncated: a shortened window would let
        // the "read-back is present" lookup miss and turn this pin vacuous.
        let opens = body.matches('{').count();
        let closes = body.matches('}').count();
        assert_eq!(
            opens, closes,
            "`exec_inbound_with_env`: scraped window is truncated ({opens} `{{` vs \
             {closes} `}}`), so this pin would pass vacuously. Widen the end \
             delimiters; do NOT delete the check."
        );

        let propagate = body
            .find("let outbound = result?;")
            .expect("`exec_inbound_with_env` must propagate the call result with `?`");
        let readback = body
            .find("DELEGATE_ENV\n            .get(&instance_id)")
            .expect("`exec_inbound_with_env` must read the context back from DELEGATE_ENV");

        assert!(
            propagate < readback,
            "the `context` read-back must sit AFTER `let outbound = result?;`. Above \
             it, the read runs on the wall-clock-timeout path while the guest is \
             still live on an abandoned blocking thread, racing `context_write` on \
             a `RefCell` that `unsafe impl Sync` has stripped the protection from \
             (#5480, #5593). Nothing but this pin would catch the move."
        );
    }
}

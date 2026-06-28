use std::collections::VecDeque;

use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use freenet_stdlib::prelude::{
    ApplicationMessage, DelegateContainer, DelegateContext, DelegateKey, InboundDelegateMsg,
    MessageOrigin, OutboundDelegateMsg, Parameters,
};

use super::super::native_api::{self, DelegateContextEntry};
use super::super::secrets_store::UserSecretContext;
use super::super::{Runtime, RuntimeResult};

pub(crate) trait DelegateRuntimeInterface {
    fn inbound_app_message(
        &mut self,
        key: &DelegateKey,
        params: &Parameters,
        origin: Option<&MessageOrigin>,
        user_context: Option<&UserSecretContext>,
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

impl DelegateRuntimeInterface for Runtime {
    fn inbound_app_message(
        &mut self,
        delegate_key: &DelegateKey,
        params: &Parameters,
        origin: Option<&MessageOrigin>,
        user_context: Option<&UserSecretContext>,
        inbound: Vec<InboundDelegateMsg>,
    ) -> RuntimeResult<Vec<OutboundDelegateMsg>> {
        let mut results = Vec::with_capacity(inbound.len());
        if inbound.is_empty() {
            return Ok(results);
        }
        let (mut running, api_version) = self.prepare_delegate_call(params, delegate_key, 4096)?;
        let instance_id = running.id;

        tracing::debug!(
            delegate_key = %delegate_key,
            api_version = %api_version,
            "Starting delegate execution"
        );

        // Context state maintained across process() calls.
        //
        // `self.delegate_contexts` persists the delegate's `ctx.write()` bytes
        // across separate `inbound_app_message` invocations so that, e.g., the
        // bytes a delegate writes when emitting `RequestUserInput` are still
        // readable via `ctx.read()` when the executor re-enters with the
        // matching `UserResponse`. Without this, the delegate hits "received
        // UserResponse with no pending context" because the Vec only used to
        // live for one `inbound_app_message` call. See
        // `native_api::DelegateContextCache`.
        //
        // Amortised TTL sweep on every entry prevents the cache from holding
        // bytes for prompts whose `UserResponse` never arrives (user
        // dismisses, app crashes, network partition).
        native_api::prune_expired_contexts(&self.delegate_contexts);
        // Keep the inherited-origins map tidy. Mark this delegate as just-used
        // so the cleanup keeps its entry, then drop entries for delegates that
        // have gone unused long enough. See `InheritedOriginsEntry`.
        native_api::touch_inherited_origin(delegate_key);
        native_api::prune_expired_inherited_origins();
        let mut context: Vec<u8> = self
            .delegate_contexts
            .get(delegate_key)
            .map(|entry| entry.bytes.clone())
            .unwrap_or_default();

        // Process all messages, collecting the result.
        // Cleanup happens after the loop regardless of success/failure.
        let process_result: RuntimeResult<()> = (|| {
            for msg in inbound {
                // The wildcard arm at the bottom of this match exists
                // solely because `InboundDelegateMsg` is `#[non_exhaustive]`
                // (stdlib 0.6.0+); every currently-known variant is
                // enumerated above. Re-listing them in a `pat | _` shape
                // (as `wildcard_enum_match_arm` would prefer) is needless
                // duplication that defeats the safety net the wildcard
                // provides for future variants.
                #[allow(clippy::wildcard_enum_match_arm)]
                match msg {
                    InboundDelegateMsg::ApplicationMessage(ApplicationMessage {
                        payload,
                        processed,
                        ..
                    }) => {
                        // clone kept — delegates read message-level context
                        let app_msg = InboundDelegateMsg::ApplicationMessage(
                            ApplicationMessage::new(payload)
                                .processed(processed)
                                .with_context(DelegateContext::new(context.clone())),
                        );

                        let (outbound, updated_context) = self.exec_inbound_with_env(
                            delegate_key,
                            params,
                            origin,
                            user_context,
                            &app_msg,
                            std::mem::take(&mut context),
                            &running.handle,
                            instance_id,
                            api_version,
                        )?;
                        context = updated_context;

                        let mut outbound_queue = VecDeque::from(outbound);
                        self.process_outbound(
                            delegate_key,
                            &running.handle,
                            instance_id,
                            params,
                            origin,
                            &mut outbound_queue,
                            &mut context,
                            &mut results,
                        )?;
                    }
                    InboundDelegateMsg::UserResponse(response) => {
                        let (outbound, updated_context) = self.exec_inbound_with_env(
                            delegate_key,
                            params,
                            origin,
                            user_context,
                            &InboundDelegateMsg::UserResponse(response),
                            std::mem::take(&mut context),
                            &running.handle,
                            instance_id,
                            api_version,
                        )?;
                        context = updated_context;

                        let mut outbound_queue = VecDeque::from(outbound);
                        self.process_outbound(
                            delegate_key,
                            &running.handle,
                            instance_id,
                            params,
                            origin,
                            &mut outbound_queue,
                            &mut context,
                            &mut results,
                        )?;
                    }
                    InboundDelegateMsg::GetContractResponse(response) => {
                        let (outbound, updated_context) = self.exec_inbound_with_env(
                            delegate_key,
                            params,
                            origin,
                            user_context,
                            &InboundDelegateMsg::GetContractResponse(response),
                            std::mem::take(&mut context),
                            &running.handle,
                            instance_id,
                            api_version,
                        )?;
                        context = updated_context;

                        let mut outbound_queue = VecDeque::from(outbound);
                        self.process_outbound(
                            delegate_key,
                            &running.handle,
                            instance_id,
                            params,
                            origin,
                            &mut outbound_queue,
                            &mut context,
                            &mut results,
                        )?;
                    }
                    msg @ (InboundDelegateMsg::PutContractResponse(_)
                    | InboundDelegateMsg::UpdateContractResponse(_)
                    | InboundDelegateMsg::SubscribeContractResponse(_)
                    | InboundDelegateMsg::ContractNotification(_)
                    | InboundDelegateMsg::DelegateMessage(_)) => {
                        let (outbound, updated_context) = self.exec_inbound_with_env(
                            delegate_key,
                            params,
                            origin,
                            user_context,
                            &msg,
                            std::mem::take(&mut context),
                            &running.handle,
                            instance_id,
                            api_version,
                        )?;
                        context = updated_context;

                        let mut outbound_queue = VecDeque::from(outbound);
                        self.process_outbound(
                            delegate_key,
                            &running.handle,
                            instance_id,
                            params,
                            origin,
                            &mut outbound_queue,
                            &mut context,
                            &mut results,
                        )?;
                    }
                    // `InboundDelegateMsg` is `#[non_exhaustive]` (stdlib
                    // 0.6.0+). Future variants are forwarded to the WASM
                    // through the same generic exec path so a delegate
                    // built against a newer stdlib can handle them; the
                    // host neither inspects nor classifies their payload.
                    other => {
                        let (outbound, updated_context) = self.exec_inbound_with_env(
                            delegate_key,
                            params,
                            origin,
                            user_context,
                            &other,
                            std::mem::take(&mut context),
                            &running.handle,
                            instance_id,
                            api_version,
                        )?;
                        context = updated_context;

                        let mut outbound_queue = VecDeque::from(outbound);
                        self.process_outbound(
                            delegate_key,
                            &running.handle,
                            instance_id,
                            params,
                            origin,
                            &mut outbound_queue,
                            &mut context,
                            &mut results,
                        )?;
                    }
                }
            }
            Ok(())
        })();

        // Always clean up the WASM Instance, even on error.
        self.drop_running_instance(&mut running);

        process_result?;

        // Persist the (possibly mutated) context so the next call into this
        // delegate sees what `ctx.write()` left behind. Skip the insert when
        // empty to keep the map sparse — `unwrap_or_default()` covers the
        // unset case symmetrically on read.
        //
        // Drop contexts that exceed `DelegateContext::MAX_SIZE`: the wire
        // format would assert on the next call when the runtime threads the
        // bytes back through `DelegateContext::new(...)`. The `ctx.write()`
        // host function has no size cap of its own, so a delegate with a
        // bug — or one trying to wedge the runtime — can otherwise stash a
        // value that would crash on the very next call. Treat oversize as
        // "delegate misbehavior, forget it" rather than "crash the node."
        if context.is_empty() {
            self.delegate_contexts.remove(delegate_key);
        } else if context.len() < freenet_stdlib::prelude::DelegateContext::MAX_SIZE {
            self.delegate_contexts.insert(
                delegate_key.clone(),
                DelegateContextEntry {
                    bytes: context,
                    last_write: tokio::time::Instant::now(),
                },
            );
        } else {
            tracing::warn!(
                delegate_key = %delegate_key,
                bytes = context.len(),
                max = freenet_stdlib::prelude::DelegateContext::MAX_SIZE,
                "Delegate ctx.write() exceeded DelegateContext::MAX_SIZE; \
                 dropping the persisted context to avoid a crash on the next call"
            );
            self.delegate_contexts.remove(delegate_key);
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
        self.delegate_modules.lock().unwrap().remove(key);
        // Drop persisted ctx.write() bytes so an unregistered delegate can't
        // hold onto stale state if it's later re-registered.
        self.delegate_contexts.remove(key);
        self.delegate_store.remove_delegate(key)
    }
}

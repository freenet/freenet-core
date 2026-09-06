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

    /// Register a delegate and prepare its secrets-store entry.
    ///
    /// This is a pass-through to `SecretsStore::register_delegate`, which
    /// accepts `cipher` and `nonce` for wire-format compatibility with clients
    /// built against older `freenet-stdlib` releases and then DISCARDS them:
    /// since #4140 / #4146 the delegate's local-scope DEK is derived from the
    /// node KEK via HKDF-SHA256 (`SecretsStore::derive_delegate_dek`). They are
    /// not key material, and a caller may pass any value — all-zero bytes
    /// included. (`freenet-stdlib` 0.8.0 removed the `DEFAULT_CIPHER` /
    /// `DEFAULT_NONCE` constants that used to be sent here; the fields are
    /// fixed-size `[u8; 32]` / `[u8; 24]`, so "any value" means any bytes, not
    /// an empty slice.)
    ///
    /// This says nothing about [`SecretScope::User`] secrets, whose DEK is
    /// derived from a caller-supplied `dek_secret` and is deliberately
    /// node-KEK-independent.
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
        native_api::touch_inherited_origin(&self.inherited_origins, delegate_key);
        native_api::prune_expired_inherited_origins(&self.inherited_origins);
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
        let key = delegate.key().clone();
        self.secret_store
            .register_delegate(key.clone(), cipher, nonce)?;
        // Roll the cipher registration back if the store refuses, mirroring
        // `native_api.rs`'s delegate-creation path. `store_delegate` became
        // fallible for a NEW reason when it started verifying that a delegate's
        // key is derived from its own code (see
        // `DelegateStore::verify_delegate_identity`), so without this a refused
        // registration leaves an entry in `SecretsStore::ciphers` and nothing
        // durable to show it: the caller sees an error, the delegate is not
        // stored, and the map has grown. The content is benign — since #4140 the
        // DEK is HKDF-derived from the node KEK, so the entry is re-derivable and
        // holds no secret the node did not already have — but `ciphers` is keyed
        // by a 64-byte value the requester chooses and is not bounded, so a
        // rejection path that grows it silently is the wrong shape regardless.
        if let Err(err) = self.delegate_store.store_delegate(delegate) {
            self.secret_store.remove_delegate_cipher(&key);
            return Err(err);
        }
        Ok(())
    }

    #[inline]
    fn unregister_delegate(&mut self, key: &DelegateKey) -> RuntimeResult<()> {
        let code_hash = self.delegate_store.code_hash_from_key(key);
        // Drop persisted ctx.write() bytes so an unregistered delegate can't
        // hold onto stale state if it's later re-registered.
        self.delegate_contexts.remove(key);
        self.delegate_store.remove_delegate(key)?;
        // Drop the compiled module only once NO remaining delegate runs that
        // code. The cache is keyed by code hash now (#5268), so one delegate's
        // removal must not evict the module its still-registered siblings —
        // other parameterizations of the same WASM — share. Mirrors the
        // reference check `remove_delegate` already applies to the `.wasm` blob,
        // and runs after it so the removed key is out of the index.
        if let Some(code_hash) = code_hash {
            if !self.delegate_store.code_still_referenced(&code_hash) {
                self.delegate_modules.lock().unwrap().remove(&code_hash);
            }
        }
        Ok(())
    }
}

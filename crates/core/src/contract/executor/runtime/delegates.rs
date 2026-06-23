//! Delegate request handling for `Executor<Runtime>`.
//!
//! This module owns the delegate-facing surface of the runtime executor:
//! registering/unregistering delegates, dispatching `ApplicationMessages`,
//! exporting per-user secrets, and the `MessageOrigin` precedence rules that
//! decide which identity a delegate message is attributed to.

use super::*;

impl Executor<Runtime> {
    /// Export this hosted user's per-user delegate secrets into an encrypted
    /// bundle, sealed under the user's `token` (hosted-mode export, P3-live of
    /// #4381).
    ///
    /// Runs entirely on the executor (which owns the `SecretsStore` via its
    /// `Runtime`), so the on-disk redb is touched only by its single writer.
    /// The bundle is scoped to `user_context.scope()` — strictly the per-user
    /// namespace, never `Local`. The `token` is the bundle-key material so the
    /// user re-imports on their own peer with the token they already hold.
    ///
    /// The token and the derived key material live only in borrowed/`Zeroizing`
    /// buffers here and inside `export_secret_bundle`; nothing is logged.
    pub fn export_user_secrets(
        &self,
        user_context: &UserSecretContext,
        token: &[u8],
    ) -> Result<Vec<u8>, ExecutorError> {
        use crate::wasm_runtime::secret_export::{BundleKeyMaterial, ExportError};
        self.runtime
            .export_secret_bundle(user_context.scope(), &BundleKeyMaterial::Token(token))
            .map_err(|e| {
                // Preserve the over-limit case as a typed marker so the HTTP
                // layer can map it to a 413 rather than a generic 500. The
                // Display text is non-secret (sizes only). Everything else stays
                // an opaque executor error. See #4381 P5. (An `if let` rather
                // than a `match` with a wildcard arm: `ExportError` is large and
                // a catch-all trips `clippy::wildcard_enum_match_arm`.)
                if let ExportError::TooLarge { .. } = &e {
                    ExecutorError::other(ExportTooLarge {
                        message: e.to_string(),
                    })
                } else {
                    ExecutorError::other(anyhow::anyhow!("secret export failed: {e}"))
                }
            })
    }

    pub fn delegate_request(
        &mut self,
        req: DelegateRequest<'_>,
        origin_contract: Option<&ContractInstanceId>,
        caller_delegate: Option<&DelegateKey>,
        user_context: Option<&UserSecretContext>,
    ) -> Response {
        // Mutual exclusion invariant: a single inbound delegate request is
        // either dispatched on behalf of a contract-backed web app
        // (`origin_contract = Some`) or on behalf of another delegate
        // (`caller_delegate = Some`), never both. The doc comment on
        // `ContractExecutor::execute_delegate_request` states this. The
        // `debug_assert!` turns the convention into a tripwire so a future
        // call site that violates it fails loudly in debug/test builds; in
        // release builds the precedence below silently picks `caller_delegate`
        // (fail-safe in the direction of "least surprising attestation").
        debug_assert!(
            !(origin_contract.is_some() && caller_delegate.is_some()),
            "execute_delegate_request: at most one of origin_contract and \
             caller_delegate may be Some (got both)"
        );
        tracing::debug!(
            origin_contract = ?origin_contract,
            caller_delegate = ?caller_delegate.map(|k| k.to_string()),
            "received delegate request"
        );
        match req {
            DelegateRequest::RegisterDelegate {
                delegate,
                cipher,
                nonce,
            } => {
                use chacha20poly1305::{KeyInit, XChaCha20Poly1305};
                let key = delegate.key().clone();
                let arr = (&cipher).into();
                let cipher = XChaCha20Poly1305::new(arr);
                let nonce = nonce.into();
                if let Some(contract) = origin_contract {
                    self.delegate_origin_ids
                        .entry(key.clone())
                        .or_default()
                        .push(*contract);
                }
                match self.runtime.register_delegate(delegate, cipher, nonce) {
                    Ok(_) => Ok(DelegateResponse {
                        key,
                        values: Vec::new(),
                    }),
                    Err(err) => {
                        tracing::warn!(
                            delegate_key = %key,
                            error = %err,
                            phase = "register_failed",
                            "Failed to register delegate"
                        );
                        Err(ExecutorError::other(StdDelegateError::RegisterError(key)))
                    }
                }
            }
            DelegateRequest::UnregisterDelegate(key) => {
                self.delegate_origin_ids.remove(&key);

                // Remove delegate from all contract subscription entries
                crate::wasm_runtime::DELEGATE_SUBSCRIPTIONS.retain(|_, subscribers| {
                    subscribers.remove(&key);
                    !subscribers.is_empty()
                });

                // Clean up delegate creation tracking to prevent unbounded growth
                crate::wasm_runtime::DELEGATE_INHERITED_ORIGINS.remove(&key);

                // Decrement the global created-delegates counter so the slot can be reused.
                // Only decrement if count > 0 to avoid underflow for delegates not created
                // via the host function (e.g., registered directly by apps).
                {
                    use std::sync::atomic::Ordering;
                    let count = &crate::wasm_runtime::CREATED_DELEGATES_COUNT;
                    let prev = count.load(Ordering::Relaxed);
                    if prev > 0 {
                        count.fetch_sub(1, Ordering::Relaxed);
                    }
                }

                match self.runtime.unregister_delegate(&key) {
                    Ok(_) => Ok(HostResponse::Ok),
                    Err(err) => {
                        tracing::warn!(
                            delegate_key = %key,
                            error = %err,
                            phase = "unregister_failed",
                            "Failed to unregister delegate"
                        );
                        Ok(HostResponse::Ok)
                    }
                }
            }
            DelegateRequest::ApplicationMessages {
                key,
                inbound,
                params,
            } => {
                let origin = resolve_message_origin(caller_delegate, origin_contract, &key);
                match self.runtime.inbound_app_message(
                    &key,
                    &params,
                    origin.as_ref(),
                    // The per-user secret scope, present only in hosted mode and
                    // derived solely from the connection token. It is delivered
                    // here on a SEPARATE channel from `origin`/the request body,
                    // so neither WASM nor any delegate-message content can set or
                    // change which user's namespace a secret op touches.
                    user_context,
                    inbound
                        .into_iter()
                        .map(InboundDelegateMsg::into_owned)
                        .collect(),
                ) {
                    Ok(values) => Ok(DelegateResponse { key, values }),
                    Err(err) => {
                        let key_display = key.to_string();
                        let exec_err =
                            ExecutorError::execution(err, Some(InnerOpError::Delegate(key)));
                        // Downgrade "not found" to warn — expected during legacy
                        // migration probes when old delegate WASM isn't on this node
                        if exec_err.is_missing_delegate() {
                            tracing::warn!(
                                delegate_key = %key_display,
                                "Delegate not found in store (expected for migration probes)"
                            );
                        } else {
                            tracing::error!(
                                delegate_key = %key_display,
                                error = %exec_err,
                                phase = "execution_failed",
                                "Failed executing delegate"
                            );
                        }
                        Err(exec_err)
                    }
                }
            }
            _ => Err(ExecutorError::other(anyhow::anyhow!("not supported"))),
        }
    }
}

/// Resolve a [`MessageOrigin`] for a delegate `ApplicationMessages` request,
/// in priority order:
///
/// 1. `caller_delegate` — set when another delegate dispatched this request
///    via `OutboundDelegateMsg::SendDelegateMessage` (issue #3860). The
///    runtime attests the caller's identity, so the receiver can authorize
///    on it. This wins unconditionally — an inter-delegate message
///    deliberately replaces (not composes with) any inherited WebApp origin.
/// 2. `origin_contract` — set when a contract-backed web app dispatched
///    this request via the WebSocket API.
/// 3. `DELEGATE_INHERITED_ORIGINS[delegate_key]` — set when a parent
///    delegate created this delegate via `create_delegate`, inheriting its
///    WebApp attestation.
///
/// Extracted as a free function so the precedence rules can be unit-tested
/// directly without standing up a full `Executor`.
fn resolve_message_origin(
    caller_delegate: Option<&DelegateKey>,
    origin_contract: Option<&ContractInstanceId>,
    delegate_key: &DelegateKey,
) -> Option<MessageOrigin> {
    if let Some(caller) = caller_delegate {
        Some(MessageOrigin::Delegate(caller.clone()))
    } else if let Some(contract_id) = origin_contract {
        Some(MessageOrigin::WebApp(*contract_id))
    } else {
        // Plain read, no timestamp update. The "last used" time is refreshed in
        // inbound_app_message instead, so a child that only ever gets messages
        // from other delegates (those don't reach this branch) still counts as
        // active and isn't dropped.
        crate::wasm_runtime::DELEGATE_INHERITED_ORIGINS
            .get(delegate_key)
            .and_then(|entry| entry.origins.first().copied().map(MessageOrigin::WebApp))
    }
}

#[cfg(test)]
mod resolve_message_origin_tests {
    use super::*;
    use freenet_stdlib::prelude::CodeHash;

    fn dkey(seed: u8) -> DelegateKey {
        DelegateKey::new([seed; 32], CodeHash::new([seed; 32]))
    }

    /// Caller delegate identity wins over a concurrently-supplied WebApp
    /// contract (regression for issue #3860 precedence rule).
    #[test]
    fn caller_delegate_takes_precedence_over_origin_contract() {
        let caller = dkey(0xA1);
        let recipient = dkey(0xB2);
        let app_contract = ContractInstanceId::new([0xC3; 32]);

        let origin = resolve_message_origin(Some(&caller), Some(&app_contract), &recipient);

        match origin {
            Some(MessageOrigin::Delegate(k)) => assert_eq!(k, caller),
            other => panic!("Expected Delegate(caller), got {other:?}"),
        }
    }

    /// With only `origin_contract` set, the receiver sees `WebApp(..)` —
    /// the historical behavior for web-app-driven dispatch must be
    /// preserved.
    #[test]
    fn origin_contract_alone_yields_webapp() {
        let recipient = dkey(0xB2);
        let app_contract = ContractInstanceId::new([0xC3; 32]);

        let origin = resolve_message_origin(None, Some(&app_contract), &recipient);

        match origin {
            Some(MessageOrigin::WebApp(id)) => assert_eq!(id, app_contract),
            other => panic!("Expected WebApp(app_contract), got {other:?}"),
        }
    }

    /// With neither argument set and no inherited origin in the static
    /// map, the receiver sees `None` (matches pre-#3860 behavior for
    /// orphaned dispatches and the fall-through case for unrelated
    /// recipients in tests).
    #[test]
    fn no_arguments_and_no_inherited_yields_none() {
        // Pick a recipient key with no entry in DELEGATE_INHERITED_ORIGINS.
        // Using a randomized seed avoids collision with anything another
        // test populated in the same process.
        let recipient = dkey(0xEE);
        crate::wasm_runtime::DELEGATE_INHERITED_ORIGINS.remove(&recipient);

        let origin = resolve_message_origin(None, None, &recipient);
        assert!(origin.is_none(), "Expected None, got {origin:?}");
    }

    /// Caller delegate identity also wins over an inherited WebApp origin
    /// in `DELEGATE_INHERITED_ORIGINS`. This documents the deliberate
    /// "inter-delegate calls revoke inherited contract access" semantics
    /// from the `MessageOrigin::Delegate` rustdoc.
    #[test]
    fn caller_delegate_overrides_inherited_origin() {
        let caller = dkey(0xA1);
        let recipient = dkey(0xB3);
        let inherited_contract = ContractInstanceId::new([0xDD; 32]);

        // Plant an inherited WebApp origin for the recipient so the
        // fallback branch would have something to return.
        crate::wasm_runtime::DELEGATE_INHERITED_ORIGINS.insert(
            recipient.clone(),
            crate::wasm_runtime::InheritedOriginsEntry::new(vec![inherited_contract]),
        );

        let origin = resolve_message_origin(Some(&caller), None, &recipient);

        // Cleanup before assertions so a panic doesn't leak state into
        // sibling tests sharing the same process.
        crate::wasm_runtime::DELEGATE_INHERITED_ORIGINS.remove(&recipient);

        match origin {
            Some(MessageOrigin::Delegate(k)) => assert_eq!(k, caller),
            other => panic!("Expected Delegate(caller), got {other:?}"),
        }
    }

    /// Fallback branch (no live caller/origin) yields the child's inherited
    /// WebApp origin via a pure read — it does not refresh `last_access`
    /// (liveness lives in `inbound_app_message`). Pairs with
    /// `no_arguments_and_no_inherited_yields_none`.
    #[test]
    fn inherited_origin_fallback_yields_webapp() {
        use crate::wasm_runtime::{DELEGATE_INHERITED_ORIGINS, InheritedOriginsEntry};

        let recipient = dkey(0xC5);
        let contract = ContractInstanceId::new([0xC6; 32]);
        DELEGATE_INHERITED_ORIGINS.insert(
            recipient.clone(),
            InheritedOriginsEntry::new(vec![contract]),
        );

        let origin = resolve_message_origin(None, None, &recipient);
        DELEGATE_INHERITED_ORIGINS.remove(&recipient);

        assert!(
            matches!(origin, Some(MessageOrigin::WebApp(c)) if c == contract),
            "fallback must yield the inherited WebApp origin, got {origin:?}"
        );
    }
}

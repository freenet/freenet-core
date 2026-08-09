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
    /// `Runtime`). This is a READ-ONLY walk: it enumerates the in-memory secret
    /// index and reads + AEAD-decrypts each on-disk secret BLOB (one file per
    /// `(delegate, secret_hash)` under the shared `secrets_dir`); it opens no
    /// redb write transaction and mutates nothing. So it is safe to run on a
    /// blocking thread (or concurrently with other read-only walks): a secret
    /// file racing a concurrent write is protected by per-file FS semantics and
    /// AEAD authentication — a torn read fails authentication and surfaces a
    /// clean export error, never silent corruption.
    /// The bundle is scoped to `user_context.scope()` — strictly the per-user
    /// namespace, never `Local`. `bundle_key_material` is the secret the bundle
    /// is encrypted under; it is DELIBERATELY decoupled from the scope. In the
    /// self-reimport case (`GET /v1/hosted/export`) it is the user's own token,
    /// so they decrypt with the token they already hold; in the magic-link
    /// migration case (`hosted_migrate` mint) it is a FRESH EPHEMERAL key, so
    /// the durable token never leaves the hosting node. Do NOT re-couple this to
    /// `user_context`'s token — the two are independent by design.
    ///
    /// The key material and the plaintext it derives live only in
    /// borrowed/`Zeroizing` buffers here and inside `export_secret_bundle`;
    /// nothing is logged.
    pub fn export_user_secrets(
        &self,
        user_context: &UserSecretContext,
        bundle_key_material: &[u8],
    ) -> Result<Vec<u8>, ExecutorError> {
        use crate::wasm_runtime::secret_export::{BundleKeyMaterial, ExportError};
        self.runtime
            .export_secret_bundle(
                user_context.scope(),
                &BundleKeyMaterial::Token(bundle_key_material),
            )
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

    /// Import delegate secrets from an encrypted `bundle` into this node's
    /// secrets store at `target_scope`, LIVE — the durable counterpart of
    /// [`Self::export_user_secrets`] and the mutating mirror of it (P3-live of
    /// #4592). Runs on the executor (which owns the `SecretsStore` via its
    /// `Runtime`). Unlike the read-only export, this is invoked ON the contract
    /// loop by the pool caller (`RuntimePool::import_secrets`) — the import WRITES
    /// and the store write path assumes node-wide write serialization, so it must
    /// not run off-loop where it could race another writer on the same secret.
    ///
    /// The bundle is decrypted under `material`; `import_bundle` authenticates
    /// the WHOLE bundle BEFORE writing anything, so a wrong key / corrupt bundle
    /// fails with NOTHING written (all-or-nothing on the key). `overwrite`
    /// controls collision handling (skip+report vs overwrite-with-snapshot).
    ///
    /// A client-input failure (wrong key, bad magic, truncated/unsupported
    /// bundle, malformed entry) is preserved as the typed [`ImportBadBundle`]
    /// marker so the HTTP layer can map it to a 4xx instead of a generic 500;
    /// its `Display` text is non-secret (never echoes the key or plaintext).
    /// Node-side failures (store/IO) stay an opaque executor error (→ 500). The
    /// `material` and the plaintext it decrypts live only in borrowed/`Zeroizing`
    /// buffers; nothing is logged.
    pub fn import_secrets(
        &mut self,
        target_scope: &crate::wasm_runtime::secret_export::TargetScope,
        bundle: &[u8],
        material: &crate::wasm_runtime::secret_export::BundleKeyMaterial<'_>,
        overwrite: bool,
    ) -> Result<crate::wasm_runtime::secret_export::ImportReport, ExecutorError> {
        use crate::contract::executor::{ImportBadBundle, is_bad_bundle_input};
        self.runtime
            .import_secret_bundle(bundle, material, target_scope, overwrite)
            .map_err(|e| {
                if is_bad_bundle_input(&e) {
                    ExecutorError::other(ImportBadBundle {
                        message: e.to_string(),
                    })
                } else {
                    ExecutorError::other(anyhow::anyhow!("secret import failed: {e}"))
                }
            })
    }

    /// Register a delegate and record its WebApp origin, for the
    /// `RegisterDelegate` request arm. Installs the client-supplied
    /// cipher/nonce, records `origin_contract` as this delegate's attestation,
    /// and registers the WASM module. Returns the delegate key on success, or a
    /// mapped [`ExecutorError`] (already carrying `RegisterError(key)`) on
    /// failure.
    ///
    /// This was previously shared with a second `RegisterDelegateWithPredecessors`
    /// arm, which drove a predecessor secret copy-forward (#4117). That handler
    /// was disabled unconditionally in #5199 (GHSA-824h-7x5x-wfmf) and the wire
    /// variant was then removed in freenet-stdlib 0.9.0, so only one caller
    /// remains. Do NOT re-introduce a copy-forward arm here without first
    /// hardening how `origin_contract` is attested — see #5201.
    fn register_delegate_and_record_origin(
        &mut self,
        delegate: DelegateContainer,
        cipher: [u8; 32],
        nonce: [u8; 24],
        origin_contract: Option<&ContractInstanceId>,
    ) -> Result<DelegateKey, ExecutorError> {
        use chacha20poly1305::{KeyInit, XChaCha20Poly1305};
        let key = delegate.key().clone();

        // RECORD BEFORE REGISTER (#4117 P1, H1 first-writer gate). The immutable
        // first-writer origin record is written (insert-if-absent) BEFORE the
        // delegate is registered, and a record-write failure ABORTS THE WHOLE
        // REGISTRATION (persistence-succeeds-before-usable). If we registered
        // first, or proceeded despite a failed record, the delegate would be
        // usable but RECORDLESS — an empty first-writer slot the next, possibly
        // attacker, registration could claim (stealing the victim's provenance
        // and then migrating its accumulated secrets). Registration already
        // depends on delegate-store disk health, so a failing origin-record DB is
        // a visibly sick node and the app simply retries; a usable-but-unowned
        // delegate is the one unacceptable state. Recording first also makes a
        // crash BETWEEN record and register harmless (a record for an
        // unregistered delegate gates nothing, and the app's retry re-registers
        // under its already-recorded origin — Ok(false), still success).
        let origin_bytes: Option<[u8; 32]> =
            origin_contract.and_then(|c| c.as_bytes().try_into().ok());
        if let Err(err) = self
            .runtime
            .record_delegate_registration_origin(&key, origin_bytes)
        {
            tracing::warn!(
                delegate_key = %key,
                error = %err,
                phase = "record_origin_failed",
                "aborting delegate registration: could not durably record the first-registration origin (persistence-succeeds-before-usable)"
            );
            return Err(ExecutorError::other(anyhow::anyhow!(
                "delegate registration aborted: could not durably record first-registration origin (H1 gate): {err}"
            )));
        }

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
            Ok(_) => Ok(key),
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

    pub fn delegate_request(
        &mut self,
        req: DelegateRequest<'_>,
        origin_contract: Option<&ContractInstanceId>,
        caller_delegate: Option<&DelegateKey>,
        connection_scope: crate::client_events::ConnectionScope,
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
        // GATE THE ORIGIN FOR *EVERY* CONSUMER, NOT JUST MESSAGE RESOLUTION
        // (GHSA-824h-7x5x-wfmf).
        //
        // `resolve_message_origin` gates what a delegate is TOLD about its
        // caller, but `origin_contract` has a second, more durable consumer:
        // `register_delegate_and_record_origin` writes it into the immutable
        // first-writer origin record. Leaving THAT ungated let a remote caller
        // mint a token for any contract id and register a delegate whose record
        // then claims that identity — which:
        //
        //   1. defeats the inter-delegate dispatch gate below, since the
        //      attacker's own delegate now has "an attested registration
        //      origin" and can lend it to a victim delegate; and
        //   2. enables durable denial by first-writer-poisoning a delegate key
        //      that is not yet registered locally (WASM and params are public,
        //      so the key is derivable), permanently fixing its attested origin
        //      to an attacker value so `route_to_apps` drops every notification
        //      to the legitimate app.
        //
        // The rustdoc on `register_delegate_and_record_origin` names "stealing
        // the victim's provenance" as the threat the record exists to resist, so
        // an ungated write is exactly the hole it was built to close. Shadow the
        // parameter here so no arm below can reach the raw value.

        let origin_contract = if connection_scope.is_local() {
            origin_contract
        } else {
            None
        };

        tracing::debug!(
            origin_contract = ?origin_contract,
            caller_delegate = ?caller_delegate.map(|k| k.to_string()),
            connection_scope = ?connection_scope,
            "received delegate request"
        );

        // WHY THERE IS NO REGISTRATION-RECORD GATE ON `caller_delegate`
        // (GHSA-824h-7x5x-wfmf). An earlier revision of this fix refused to
        // dispatch from any delegate whose first-registration record held no
        // contract id. It was removed, for two independent reasons:
        //
        //  1. It BROKE legitimate use. That record is `None` both for "never
        //     registered here" and for the tokenless local CLI shape, so every
        //     delegate installed by riverctl / atlasctl / fdev lost the ability
        //     to emit `SendDelegateMessage` — silently, because the caller
        //     swallows the error into a warning and the client still sees `Ok`
        //     with the reply simply missing.
        //  2. It was not protecting anything. `caller_delegate` is set by the
        //     runtime from the key of the delegate that actually ran, and a
        //     `DelegateKey` is a hash of that delegate's own code and params. So
        //     `MessageOrigin::Delegate(K)` is SELF-AUTHENTICATING: a caller can
        //     only ever speak as code it actually got registered, never as some
        //     other delegate. There is no identity to forge here.
        //
        // The escalation this gate was aimed at — an off-host caller laundering
        // its lack of attestation through a delegate hop — is closed where it
        // should be, by `resolve_message_origin` returning `None` for a non-local
        // connection: `contract.rs` propagates the ORIGINATING connection scope
        // into the hop, so the target sees no attestation either. What remains is
        // that a delegate may present its own key to another delegate. A delegate
        // that authorizes arbitrary peer delegate keys is making its own trust
        // decision; the runtime's job is only to make that key unforgeable, which
        // it does.

        match req {
            DelegateRequest::RegisterDelegate {
                delegate,
                cipher,
                nonce,
            } => match self.register_delegate_and_record_origin(
                delegate,
                cipher,
                nonce,
                origin_contract,
            ) {
                Ok(key) => Ok(DelegateResponse {
                    key,
                    values: Vec::new(),
                }),
                Err(err) => Err(err),
            },
            DelegateRequest::UnregisterDelegate(key) => {
                self.delegate_origin_ids.remove(&key);

                // Remove delegate from all contract subscription entries
                crate::wasm_runtime::DELEGATE_SUBSCRIPTIONS.retain(|_, subscribers| {
                    subscribers.remove(&key);
                    !subscribers.is_empty()
                });

                // Clean up delegate creation tracking to prevent unbounded growth
                self.runtime.inherited_origins.remove(&key);

                // Release this node's created-delegate slot so it can be reused.
                // Saturates at zero: delegates registered directly by apps were
                // never counted. See `release_created_delegate_slot`.
                crate::wasm_runtime::release_created_delegate_slot(
                    &self.runtime.created_delegates_count,
                );

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
                let origin = resolve_message_origin(
                    &self.runtime.inherited_origins,
                    caller_delegate,
                    origin_contract,
                    &key,
                    connection_scope,
                );
                // AUDIT (info!, not debug! — the crate builds with
                // `release_max_level_info`, so a `debug!` here would compile out
                // of every shipped binary and the audit trail would exist only
                // in development). Ids only: no token values, no key material.
                //
                // REFUSALS ONLY. An earlier revision also logged every
                // successfully-attested operation, on the reasoning that the
                // unattested local case was the high-volume one. That was exactly
                // backwards: the high-volume case is a web app WITH a token,
                // which resolves `Some` on every single `ApplicationMessages` —
                // and, because branch 3 resolves an inherited origin, on every
                // contract-notification invocation too. A River node in a busy
                // room would have emitted one shipped INFO per room update.
                //
                // Dropping the success case loses nothing: the GRANT of an
                // attested identity is already recorded once, with the peer
                // address, at the issuance point in
                // `server::client_api::render_shell_response`. Re-logging it per
                // message adds volume, not information. What is NOT recorded
                // anywhere else, and is genuinely rare, is a connection being
                // REFUSED attestation — so that is what this line reports.
                if !connection_scope.is_local() {
                    tracing::info!(
                        delegate_key = %key,
                        from_delegate = ?caller_delegate.map(|k| k.to_string()),
                        loopback = false,
                        "delegate ApplicationMessages: withheld attested origin \
                         from a non-local connection (GHSA-824h-7x5x-wfmf)"
                    );
                }
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
/// 3. `inherited_origins[delegate_key]` — set when a parent delegate created
///    this delegate via `create_delegate`, inheriting its WebApp attestation.
///
/// Extracted as a free function so the precedence rules can be unit-tested
/// directly without standing up a full `Executor`. `inherited_origins` is the
/// node's attestation map, passed in rather than reached for globally so a
/// test's map is its own (#4813).
fn resolve_message_origin(
    inherited_origins: &crate::wasm_runtime::SharedInheritedOrigins,
    caller_delegate: Option<&DelegateKey>,
    origin_contract: Option<&ContractInstanceId>,
    delegate_key: &DelegateKey,
    connection_scope: crate::client_events::ConnectionScope,
) -> Option<MessageOrigin> {
    // GATE THE RESOLVED ORIGIN, NOT ONE OF ITS INPUTS (GHSA-824h-7x5x-wfmf).
    //
    // The obvious-looking fix — refuse to look up `origin_contract` for an
    // off-host caller — closes only branch 2. Branch 3 is keyed on the TARGET
    // delegate, not on anything the caller supplied, so it would keep handing a
    // tokenless off-host caller a fully-attested WebApp origin for any delegate
    // that happens to carry an inherited attestation. Branch 1 is likewise
    // caller-independent once a delegate has been induced to emit
    // `SendDelegateMessage`. Returning early HERE is the only placement that
    // covers all three by construction, and it stays correct if a fourth branch
    // is ever added below.
    //
    // Withholding attestation is not an error: the caller sees exactly what a
    // tokenless local caller has always seen (`None`), so nothing on the wire
    // changes and no new response variant is needed.
    if !connection_scope.is_local() {
        return None;
    }
    if let Some(caller) = caller_delegate {
        Some(MessageOrigin::Delegate(caller.clone()))
    } else if let Some(contract_id) = origin_contract {
        Some(MessageOrigin::WebApp(*contract_id))
    } else {
        // Plain read, no timestamp update. The "last used" time is refreshed in
        // inbound_app_message instead, so a child that only ever gets messages
        // from other delegates (those don't reach this branch) still counts as
        // active and isn't dropped.
        inherited_origins
            .get(delegate_key)
            .and_then(|entry| entry.origins.first().copied().map(MessageOrigin::WebApp))
    }
}

#[cfg(test)]
mod resolve_message_origin_tests {
    use super::*;
    use crate::client_events::ConnectionScope;
    use freenet_stdlib::prelude::CodeHash;

    fn dkey(seed: u8) -> DelegateKey {
        DelegateKey::new([seed; 32], CodeHash::new([seed; 32]))
    }

    /// A fresh attestation map, standing in for one node's.
    ///
    /// Each test gets its own, so no test can see another's entries. These
    /// tests used to share one process-global map, which forced them to pick
    /// collision-avoiding keys and to hand back their entries before asserting
    /// (so a panic wouldn't leak into a sibling). Neither dance is needed now,
    /// and both are gone. See #4813.
    fn origins() -> crate::wasm_runtime::SharedInheritedOrigins {
        crate::wasm_runtime::new_inherited_origins()
    }

    /// Caller delegate identity wins over a concurrently-supplied WebApp
    /// contract (regression for issue #3860 precedence rule).
    #[test]
    fn caller_delegate_takes_precedence_over_origin_contract() {
        let caller = dkey(0xA1);
        let recipient = dkey(0xB2);
        let app_contract = ContractInstanceId::new([0xC3; 32]);

        let origin = resolve_message_origin(
            &origins(),
            Some(&caller),
            Some(&app_contract),
            &recipient,
            ConnectionScope::Local,
        );

        match origin {
            Some(MessageOrigin::Delegate(k)) => assert_eq!(k, caller),
            other => panic!("Expected Delegate(caller), got {other:?}"),
        }
    }

    /// With only `origin_contract` set, the receiver sees `WebApp(..)` — the
    /// historical behavior for web-app-driven dispatch must be preserved.
    #[test]
    fn origin_contract_alone_yields_webapp() {
        let recipient = dkey(0xB2);
        let app_contract = ContractInstanceId::new([0xC3; 32]);

        let origin = resolve_message_origin(
            &origins(),
            None,
            Some(&app_contract),
            &recipient,
            ConnectionScope::Local,
        );

        match origin {
            Some(MessageOrigin::WebApp(id)) => assert_eq!(id, app_contract),
            other => panic!("Expected WebApp(app_contract), got {other:?}"),
        }
    }

    /// With neither argument set and no inherited origin in the node's map, the
    /// receiver sees `None` (matches pre-#3860 behavior for orphaned dispatches
    /// and the fall-through case for unrelated recipients).
    #[test]
    fn no_arguments_and_no_inherited_yields_none() {
        let recipient = dkey(0xEE);

        let origin =
            resolve_message_origin(&origins(), None, None, &recipient, ConnectionScope::Local);
        assert!(origin.is_none(), "Expected None, got {origin:?}");
    }

    /// Caller delegate identity also wins over an inherited WebApp origin. This
    /// documents the deliberate "inter-delegate calls revoke inherited contract
    /// access" semantics from the `MessageOrigin::Delegate` rustdoc.
    #[test]
    fn caller_delegate_overrides_inherited_origin() {
        let caller = dkey(0xA1);
        let recipient = dkey(0xB3);
        let inherited_contract = ContractInstanceId::new([0xDD; 32]);
        let origins = origins();

        // Plant an inherited WebApp origin for the recipient so the fallback
        // branch would have something to return.
        origins.insert(
            recipient.clone(),
            crate::wasm_runtime::InheritedOriginsEntry::new(vec![inherited_contract]),
        );

        let origin = resolve_message_origin(
            &origins,
            Some(&caller),
            None,
            &recipient,
            ConnectionScope::Local,
        );

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
        use crate::wasm_runtime::InheritedOriginsEntry;

        let recipient = dkey(0xC5);
        let contract = ContractInstanceId::new([0xC6; 32]);
        let origins = origins();
        origins.insert(
            recipient.clone(),
            InheritedOriginsEntry::new(vec![contract]),
        );

        let origin =
            resolve_message_origin(&origins, None, None, &recipient, ConnectionScope::Local);

        assert!(
            matches!(origin, Some(MessageOrigin::WebApp(c)) if c == contract),
            "fallback must yield the inherited WebApp origin, got {origin:?}"
        );
    }

    /// Two nodes' attestation maps are independent (#4813).
    ///
    /// A `DelegateKey` is derived from the delegate's code and params, so two
    /// nodes running the same delegate collide on the key by construction.
    /// While this map was a `static`, that collision meant one node's inherited
    /// origin resolved as another's — and this map decides which contract a
    /// delegate message is attributed to, hence what it may access.
    #[test]
    fn inherited_origin_does_not_leak_across_nodes() {
        use crate::wasm_runtime::InheritedOriginsEntry;

        // The same delegate key on both nodes, as identical code+params gives.
        let recipient = dkey(0xC5);
        let contract = ContractInstanceId::new([0xC6; 32]);

        let node_a = origins();
        let node_b = origins();
        node_a.insert(
            recipient.clone(),
            InheritedOriginsEntry::new(vec![contract]),
        );

        assert!(
            matches!(
                resolve_message_origin(&node_a, None, None, &recipient, ConnectionScope::Local),
                Some(MessageOrigin::WebApp(c)) if c == contract
            ),
            "node A resolves its own inherited origin"
        );
        assert!(
            resolve_message_origin(&node_b, None, None, &recipient, ConnectionScope::Local)
                .is_none(),
            "node B must NOT resolve node A's inherited origin for the same key"
        );
    }

    // ---------------------------------------------------------------------
    // GHSA-824h-7x5x-wfmf: a connection the node cannot prove is local gets NO
    // attested origin — through EVERY branch of `resolve_message_origin`.
    //
    // These three are the load-bearing ones. The advisory's near-miss is a fix
    // that gates only branch 2 (`origin_contract`, the input the caller's token
    // produced) and leaves branches 1 and 3 open: branch 3 is keyed on the
    // TARGET delegate, so it would keep attesting a WebApp origin for a caller
    // that supplied nothing at all. Revert the `!connection_scope.is_local()`
    // early return and `remote_connection_gets_no_inherited_origin` fails while
    // a branch-2-only fix would leave it passing.
    // ---------------------------------------------------------------------

    /// Branch 2: a remote caller holding a valid auth token still gets nothing.
    #[test]
    fn remote_connection_gets_no_webapp_origin_from_token() {
        let recipient = dkey(0xB2);
        let app_contract = ContractInstanceId::new([0xC3; 32]);

        let origin = resolve_message_origin(
            &origins(),
            None,
            Some(&app_contract),
            &recipient,
            ConnectionScope::Remote,
        );
        assert!(
            origin.is_none(),
            "a non-loopback connection must receive NO attested origin, got {origin:?}"
        );
    }

    /// Branch 3: the branch a token-input-only gate misses. The caller supplies
    /// NOTHING — the origin comes from the node's own attestation map, keyed on
    /// the delegate being messaged — so gating the caller's input cannot help.
    #[test]
    fn remote_connection_gets_no_inherited_origin() {
        use crate::wasm_runtime::InheritedOriginsEntry;

        let recipient = dkey(0xC5);
        let contract = ContractInstanceId::new([0xC6; 32]);
        let origins = origins();
        origins.insert(
            recipient.clone(),
            InheritedOriginsEntry::new(vec![contract]),
        );

        // Sanity: the same call from a local connection DOES attest, so this
        // test is asserting on the scope gate and not on an empty map.
        assert!(
            resolve_message_origin(&origins, None, None, &recipient, ConnectionScope::Local)
                .is_some(),
            "fixture must attest for a local connection, or the assertion below is vacuous"
        );

        let origin =
            resolve_message_origin(&origins, None, None, &recipient, ConnectionScope::Remote);
        assert!(
            origin.is_none(),
            "a non-loopback connection must not inherit the target delegate's \
             WebApp attestation, got {origin:?}"
        );
    }

    /// Branch 1: the highest-precedence branch, which wins unconditionally over
    /// the other two, is gated as well.
    #[test]
    fn remote_connection_gets_no_caller_delegate_origin() {
        let caller = dkey(0xA1);
        let recipient = dkey(0xB3);

        assert!(
            resolve_message_origin(
                &origins(),
                Some(&caller),
                None,
                &recipient,
                ConnectionScope::Local
            )
            .is_some(),
            "fixture must attest for a local connection, or the assertion below is vacuous"
        );

        let origin = resolve_message_origin(
            &origins(),
            Some(&caller),
            None,
            &recipient,
            ConnectionScope::Remote,
        );
        assert!(
            origin.is_none(),
            "a non-loopback connection must not receive an attested caller \
             delegate identity, got {origin:?}"
        );
    }

    /// The gate must be on the RESOLVED value, not bolted onto one branch. A
    /// future fourth branch added below the early return is covered for free;
    /// one added above it is not. Pin the early return's position so a
    /// refactor that moves the branch dispatch above it fails here.
    #[test]
    fn scope_gate_precedes_every_branch_in_source() {
        let src = include_str!("delegates.rs");
        let body = src
            .split("fn resolve_message_origin(")
            .nth(1)
            .expect("resolve_message_origin must exist")
            .split("\n#[cfg(test)]")
            .next()
            .expect("function body is bounded by the test module");
        let gate = body
            .find("if !connection_scope.is_local() {")
            .expect("resolve_message_origin must early-return for a non-local connection");
        let first_branch = body
            .find("if let Some(caller) = caller_delegate {")
            .expect("branch 1 must exist");
        assert!(
            gate < first_branch,
            "the connection-scope gate must run BEFORE any origin branch, so a \
             branch added later is covered by construction"
        );
    }
}

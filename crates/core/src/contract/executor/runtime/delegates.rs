//! Delegate request handling for `Executor<Runtime>`.
//!
//! This module owns the delegate-facing surface of the runtime executor:
//! registering/unregistering delegates, dispatching `ApplicationMessages`,
//! exporting per-user secrets, and the `MessageOrigin` precedence rules that
//! decide which identity a delegate message is attributed to.

use super::*;

/// Upper bound on the number of predecessor delegate keys a single
/// `RegisterDelegateWithPredecessors` request may drive copy-forward for
/// (#4117 P2/M1). The predecessor list is client-controlled, and each
/// predecessor drives synchronous marker/index/redb writes on the contract
/// loop, so an unbounded (or duplicate-padded) list is a disk-growth / loop-
/// stall amplification vector. 64 matches the delegate-lineage / probe-hop
/// bound used by the app-side migration driver (`DEFAULT_MAX_PROBE_HOPS`): a
/// realistic delegate never accumulates anywhere near 64 retired generations.
/// Over the cap the list is deduped-then-truncated with a warning;
/// registration itself still succeeds.
const MAX_MIGRATION_PREDECESSORS: usize = 64;

/// Dedupe a client-supplied predecessor list, preserving newest-first order
/// (#4117 P2/M1). A duplicate is pure waste (the migration is idempotent per
/// pair), so it is dropped SILENTLY. The cap is enforced by the CALLER on the
/// deduped length: over the cap, the whole request is REJECTED before
/// registration (never silently truncated, which would strand older
/// generations — the client is expected to split its request). Extracted as a
/// free function so the dedupe is unit-testable without standing up an executor.
fn dedupe_predecessors(predecessors: Vec<DelegateKey>) -> Vec<DelegateKey> {
    let mut seen = std::collections::HashSet::new();
    let mut out: Vec<DelegateKey> = Vec::with_capacity(predecessors.len());
    for p in predecessors {
        if seen.insert(p.clone()) {
            out.push(p);
        }
    }
    out
}

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

    /// Register a delegate and record its WebApp origin, shared by the
    /// `RegisterDelegate` and `RegisterDelegateWithPredecessors` request arms so
    /// the two cannot drift. Installs the client-supplied cipher/nonce, records
    /// `origin_contract` as this delegate's attestation, and registers the WASM
    /// module. Returns the delegate key on success, or a mapped
    /// [`ExecutorError`] (already carrying `RegisterError(key)`) on failure.
    fn register_delegate_and_record_origin(
        &mut self,
        delegate: DelegateContainer,
        cipher: [u8; 32],
        nonce: [u8; 24],
        origin_contract: Option<&ContractInstanceId>,
    ) -> Result<DelegateKey, ExecutorError> {
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
            Ok(_) => {
                // Durably record the registration origin for the H1 same-origin
                // copy-forward gate (#4117): a future migration naming this
                // delegate as a predecessor copies its Local secrets only to a
                // successor registered from the SAME web-app origin (or the
                // Admin/None class for a loopback/CLI registration).
                let origin_bytes: Option<[u8; 32]> =
                    origin_contract.and_then(|c| c.as_bytes().try_into().ok());
                self.runtime
                    .record_delegate_registration_origin(&key, origin_bytes);
                Ok(key)
            }
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
            DelegateRequest::RegisterDelegateWithPredecessors {
                delegate,
                cipher,
                nonce,
                predecessors,
            } => {
                // Bound the client-controlled predecessor list BEFORE registering
                // (#4117 P2b/M1). Dedupe silently (a repeat is pure waste — the
                // migration is idempotent per pair), then REJECT the whole request
                // if the deduped list still exceeds the cap: each predecessor
                // drives synchronous marker/index/redb writes on the contract
                // loop, so an unbounded list is an amplification vector, and
                // silently truncating would strand older generations. The client
                // splits an over-cap request.
                let deduped = dedupe_predecessors(predecessors);
                if deduped.len() > MAX_MIGRATION_PREDECESSORS {
                    let key = delegate.key().clone();
                    tracing::warn!(
                        delegate_key = %key,
                        predecessors = deduped.len(),
                        cap = MAX_MIGRATION_PREDECESSORS,
                        "RegisterDelegateWithPredecessors rejected: too many predecessors (split the request)"
                    );
                    return Err(ExecutorError::other(anyhow::anyhow!(
                        "RegisterDelegateWithPredecessors: {} predecessors exceeds the cap of {}",
                        deduped.len(),
                        MAX_MIGRATION_PREDECESSORS
                    )));
                }

                // Register exactly as `RegisterDelegate` does, THEN run the
                // one-shot, Local-scope copy-forward of the predecessors' secrets.
                // The copy is best-effort and never fails registration: a
                // refused/absent/partly-unreadable predecessor is recorded in the
                // report and logged, but the successor still registers. See
                // `SecretsStore::migrate_secrets` for the full contract.
                match self.register_delegate_and_record_origin(
                    delegate,
                    cipher,
                    nonce,
                    origin_contract,
                ) {
                    Ok(key) => {
                        // Record the originating web-app contract (when present).
                        // `ContractInstanceId::as_bytes` is the 32-byte id;
                        // `try_into` never fails but is used to stay panic-free.
                        let origin_bytes: Option<[u8; 32]> =
                            origin_contract.and_then(|c| c.as_bytes().try_into().ok());
                        let report =
                            self.runtime
                                .migrate_delegate_secrets(&deduped, &key, origin_bytes);
                        if report.total_errors() > 0 {
                            tracing::warn!(
                                delegate_key = %key,
                                predecessors = deduped.len(),
                                copied = report.total_copied(),
                                skipped_existing = report.total_skipped_existing(),
                                user_scope_skipped = report.total_user_scope_skipped(),
                                errors = report.total_errors(),
                                phase = "secret_copy_forward",
                                "delegate secret copy-forward completed with errors"
                            );
                        } else {
                            tracing::info!(
                                delegate_key = %key,
                                predecessors = deduped.len(),
                                copied = report.total_copied(),
                                skipped_existing = report.total_skipped_existing(),
                                user_scope_skipped = report.total_user_scope_skipped(),
                                phase = "secret_copy_forward",
                                "delegate secret copy-forward completed"
                            );
                        }
                        Ok(DelegateResponse {
                            key,
                            values: Vec::new(),
                        })
                    }
                    Err(err) => Err(err),
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
                );
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
        inherited_origins
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

    /// #4117 P2/M1: the predecessor list is deduped silently, preserving
    /// newest-first order (first occurrence wins). The cap itself is enforced in
    /// the handler on the deduped length (over-cap → the whole request is
    /// rejected, never silently truncated).
    #[test]
    fn dedupe_predecessors_preserves_order_and_drops_duplicates() {
        // Unique → unchanged, order preserved.
        let keys: Vec<DelegateKey> = (0u8..5).map(dkey).collect();
        assert_eq!(dedupe_predecessors(keys.clone()), keys);

        // Duplicates dropped, first occurrence wins, order preserved.
        let dupes = vec![dkey(1), dkey(2), dkey(1), dkey(3), dkey(2)];
        assert_eq!(dedupe_predecessors(dupes), vec![dkey(1), dkey(2), dkey(3)]);

        // Dedupe does not itself cap: a large unique list passes through (the
        // handler rejects it against MAX_MIGRATION_PREDECESSORS).
        let many: Vec<DelegateKey> = (0u8..200).map(dkey).collect();
        assert_eq!(dedupe_predecessors(many).len(), 200);
        assert!(200 > MAX_MIGRATION_PREDECESSORS);
    }

    /// Caller delegate identity wins over a concurrently-supplied WebApp
    /// contract (regression for issue #3860 precedence rule).
    #[test]
    fn caller_delegate_takes_precedence_over_origin_contract() {
        let caller = dkey(0xA1);
        let recipient = dkey(0xB2);
        let app_contract = ContractInstanceId::new([0xC3; 32]);

        let origin =
            resolve_message_origin(&origins(), Some(&caller), Some(&app_contract), &recipient);

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

        let origin = resolve_message_origin(&origins(), None, Some(&app_contract), &recipient);

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

        let origin = resolve_message_origin(&origins(), None, None, &recipient);
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

        let origin = resolve_message_origin(&origins, Some(&caller), None, &recipient);

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

        let origin = resolve_message_origin(&origins, None, None, &recipient);

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
                resolve_message_origin(&node_a, None, None, &recipient),
                Some(MessageOrigin::WebApp(c)) if c == contract
            ),
            "node A resolves its own inherited origin"
        );
        assert!(
            resolve_message_origin(&node_b, None, None, &recipient).is_none(),
            "node B must NOT resolve node A's inherited origin for the same key"
        );
    }
}

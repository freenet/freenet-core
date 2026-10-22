//! `UnregisterDelegate` at the executor: only a local client may unregister a
//! delegate, nothing is removed when the request is refused or the store
//! removal fails, and a created-delegate slot is released only for a delegate
//! that holds one. On a hosted node a user's unregister removes nothing here
//! (only that user's push registrations go, one layer up).
//!
//! The client-API half (push registrations cleared only after the executor
//! answers `Ok`) is driven through `process_open_request` in
//! `client_events::unregister_dispatch_tests`.

use freenet_stdlib::client_api::{DelegateRequest, HostResponse};
use freenet_stdlib::prelude::{
    ContractInstanceId, Delegate, DelegateContainer, DelegateKey, DelegateWasmAPIVersion,
};

use crate::client_events::ConnectionScope;
use crate::contract::executor::Executor;
use crate::contract::storages::Storage;
use crate::wasm_runtime::{
    ContractStore, DelegateContextEntry, DelegateStore, InheritedOriginsEntry, Runtime,
    SecretsStore, StateStore, delegate_subscriptions,
};

struct Fixture {
    executor: Executor<Runtime>,
    db: Storage,
    delegates_dir: std::path::PathBuf,
    _temp: tempfile::TempDir,
}

async fn fixture() -> Fixture {
    let temp = crate::util::tests::get_temp_dir();
    let db = Storage::new(temp.path()).await.expect("create storage db");
    let contract_store = ContractStore::new(temp.path().join("contracts"), 10_000, db.clone())
        .expect("create contract store");
    let delegates_dir = temp.path().join("delegate");
    let delegate_store =
        DelegateStore::new(delegates_dir.clone(), 10_000, db.clone()).expect("create store");
    let secrets_store =
        SecretsStore::new(temp.path().join("secrets"), Default::default(), db.clone())
            .expect("create secrets store");
    let state_store = StateStore::new(db.clone(), 10_000_000).expect("create state store");
    // The production 5 s execution budget is wall clock, so it also counts the
    // host work a delegate's calls cause, and a real delegate run can exceed it
    // on a loaded CI runner. These tests check unregistration, not that policy,
    // so they set the budget explicitly, as `wasm_runtime::delegate::test`'s
    // `delegate_fixture_config` does (its rustdoc has the full reasoning).
    let runtime = Runtime::build_with_config(
        contract_store,
        delegate_store,
        secrets_store,
        false,
        crate::wasm_runtime::RuntimeConfig {
            max_execution_seconds: 60.0,
            ..Default::default()
        },
    )
    .expect("build runtime");
    let executor = Executor::new(
        state_store,
        || Ok(()),
        crate::contract::executor::OperationMode::Local,
        runtime,
        None,
    )
    .await
    .expect("create executor");
    Fixture {
        executor,
        db,
        delegates_dir,
        _temp: temp,
    }
}

/// Distinct params per test: the subscription registry is process-global, so
/// two tests sharing a delegate key would see each other's rows.
fn delegate(params: &[u8]) -> Delegate<'static> {
    Delegate::from((&vec![0u8].into(), &params.to_vec().into()))
}

fn register(f: &mut Fixture, d: &Delegate<'static>) {
    f.executor
        .delegate_request(
            DelegateRequest::RegisterDelegate {
                delegate: DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(d.clone())),
                cipher: [7u8; 32],
                nonce: [9u8; 24],
            },
            None,
            None,
            ConnectionScope::Local,
            None,
        )
        .expect("registration must succeed");
}

fn unregister(
    f: &mut Fixture,
    key: &DelegateKey,
    scope: ConnectionScope,
) -> Result<HostResponse, crate::contract::ExecutorError> {
    f.executor.delegate_request(
        DelegateRequest::UnregisterDelegate(key.clone()),
        None,
        None,
        scope,
        None,
    )
}

/// Everything an unregister removes, seeded so a test can tell what survived.
struct Seeded {
    key: DelegateKey,
    contract: ContractInstanceId,
}

/// Registers `d`, gives it context bytes and a persisted contract
/// subscription, and records it as a delegate-created delegate.
fn seed(f: &mut Fixture, d: &Delegate<'static>, contract_byte: u8) -> Seeded {
    register(f, d);
    let key = d.key().clone();
    f.executor
        .runtime
        .delegate_contexts_for_test()
        .insert_for_test(
            &key,
            None,
            DelegateContextEntry {
                bytes: b"ctx".to_vec(),
                last_write: tokio::time::Instant::now(),
            },
        );
    let contract = ContractInstanceId::new([contract_byte; 32]);
    delegate_subscriptions::subscribe(
        contract,
        &key,
        delegate_subscriptions::Durability::Persist(&f.db),
    );
    f.executor
        .runtime
        .created_delegates_count
        .record(key.clone());
    f.executor
        .delegate_origin_ids
        .insert(key.clone(), vec![contract]);
    f.executor
        .runtime
        .inherited_origins
        .insert(key.clone(), InheritedOriginsEntry::new(vec![contract]));
    Seeded { key, contract }
}

fn durably_subscribed(f: &Fixture, s: &Seeded) -> bool {
    f.db.load_delegate_subscriptions(usize::MAX)
        .expect("read durable subscriptions")
        .valid
        .iter()
        .any(|(c, d)| *c == s.contract && *d == s.key)
}

fn assert_intact(f: &Fixture, s: &Seeded, after: &str) {
    assert!(
        f.executor.runtime.delegate_code(&s.key).is_some(),
        "{after}: the delegate must still be stored"
    );
    assert!(
        f.executor
            .runtime
            .delegate_contexts_for_test()
            .contains_delegate(&s.key),
        "{after}: the delegate's context bytes must survive"
    );
    assert!(
        delegate_subscriptions::is_subscribed(&s.contract, &s.key),
        "{after}: the delegate's subscription must survive"
    );
    assert!(
        durably_subscribed(f, s),
        "{after}: the persisted subscription row must survive"
    );
    assert!(
        f.executor.runtime.created_delegates_count.contains(&s.key),
        "{after}: the delegate's created-delegate slot must still be held"
    );
    assert!(
        f.executor.delegate_origin_ids.contains_key(&s.key),
        "{after}: the delegate's origin ids must survive"
    );
    assert!(
        f.executor.runtime.inherited_origins.contains_key(&s.key),
        "{after}: the delegate's inherited origins must survive"
    );
}

/// The opposite of [`assert_intact`]: everything an unregister removes is gone.
fn assert_cleaned(f: &Fixture, s: &Seeded, after: &str) {
    assert!(
        f.executor.runtime.delegate_code(&s.key).is_none(),
        "{after}: the delegate must be gone"
    );
    assert!(
        !f.executor
            .runtime
            .delegate_contexts_for_test()
            .contains_delegate(&s.key),
        "{after}: the context bytes must be gone"
    );
    assert!(
        !delegate_subscriptions::is_subscribed(&s.contract, &s.key),
        "{after}: the subscription must be gone"
    );
    assert!(
        !durably_subscribed(f, s),
        "{after}: the persisted subscription row must be gone"
    );
    assert!(
        !f.executor.runtime.created_delegates_count.contains(&s.key),
        "{after}: the created-delegate slot must be released"
    );
    assert!(
        !f.executor.delegate_origin_ids.contains_key(&s.key),
        "{after}: the origin ids must be gone"
    );
    assert!(
        !f.executor.runtime.inherited_origins.contains_key(&s.key),
        "{after}: the inherited origins must be gone"
    );
}

/// A non-local `UnregisterDelegate` is refused with the typed error and
/// removes nothing; the same request from a local client unregisters.
///
/// FALSIFY by removing the `connection_scope.is_local()` check from the
/// executor's `UnregisterDelegate` arm: the request is not refused.
#[tokio::test(flavor = "multi_thread")]
async fn a_non_local_unregister_is_refused_and_removes_nothing() {
    let mut f = fixture().await;
    let s = seed(&mut f, &delegate(b"unregister-remote-1"), 0xD1);

    let err = unregister(&mut f, &s.key, ConnectionScope::Remote)
        .expect_err("a non-local unregister must be refused");
    assert!(
        err.is_unregister_not_local(),
        "the refusal must be the typed one, got {err}"
    );
    assert!(
        err.is_request(),
        "the refusal is a request error, got {err}"
    );
    assert_intact(&f, &s, "after a refused remote unregister");

    // Control: the same request from a local client does unregister, so the
    // assertions above are about the scope and not about a no-op request.
    let ok = unregister(&mut f, &s.key, ConnectionScope::Local).expect("local unregister");
    assert!(matches!(ok, HostResponse::Ok), "got {ok:?}");
    assert_cleaned(&f, &s, "after a local unregister");
}

/// `Executor::handle_request` does not know which connection a request came
/// from, so it treats every request as remote: an `UnregisterDelegate` routed
/// through it is refused and removes nothing.
///
/// FALSIFY by passing `ConnectionScope::Local` in `handle_request`'s
/// `DelegateOp` arm: the unregister succeeds.
#[tokio::test(flavor = "multi_thread")]
async fn handle_request_refuses_an_unregister() {
    let mut f = fixture().await;
    let s = seed(&mut f, &delegate(b"unregister-handle-request"), 0xD9);

    let err = f
        .executor
        .handle_request(
            crate::client_events::ClientId::FIRST,
            freenet_stdlib::client_api::ClientRequest::DelegateOp(
                DelegateRequest::UnregisterDelegate(s.key.clone()),
            ),
            None,
        )
        .await
        .expect_err("an unregister through handle_request must be refused");
    assert!(
        err.is_unregister_not_local(),
        "the refusal must be the typed one, got {err}"
    );
    assert_intact(&f, &s, "after an unregister through handle_request");
}

/// Only a delegate that holds a created-delegate slot releases one: a key
/// that was never registered, and a delegate an app registered directly, do
/// not move the count.
///
/// FALSIFY by releasing a slot for any key: the count assertions fail.
#[tokio::test(flavor = "multi_thread")]
async fn only_a_counted_delegate_releases_a_created_delegate_slot() {
    let mut f = fixture().await;
    // A delegate-created delegate holds the one slot in use.
    let created = seed(&mut f, &delegate(b"unregister-count-created"), 0xD2);
    let count = || f.executor.runtime.created_delegates_count.count();
    assert_eq!(count(), 1);

    let unknown = delegate(b"unregister-count-unknown").key().clone();
    unregister(&mut f, &unknown, ConnectionScope::Local)
        .expect("unregistering an unknown key is not an error");
    assert_eq!(
        f.executor.runtime.created_delegates_count.count(),
        1,
        "an unknown key must not release a slot"
    );

    let app_registered = delegate(b"unregister-count-app");
    register(&mut f, &app_registered);
    unregister(&mut f, app_registered.key(), ConnectionScope::Local).expect("local unregister");
    assert!(
        f.executor
            .runtime
            .delegate_code(app_registered.key())
            .is_none()
    );
    assert_eq!(
        f.executor.runtime.created_delegates_count.count(),
        1,
        "an app-registered delegate never held a slot, so it must not release one"
    );

    unregister(&mut f, &created.key, ConnectionScope::Local).expect("local unregister");
    assert_eq!(
        f.executor.runtime.created_delegates_count.count(),
        0,
        "the delegate-created delegate releases its slot"
    );
}

/// When the store cannot remove the delegate, the unregister reports the
/// failure and removes nothing else: the delegate may still be registered and
/// runnable, so its context, subscriptions and slot stay with it.
///
/// The failure is injected by putting a directory where the delegate's `.reg`
/// file was, so the store's `remove_file` fails with something other than
/// `NotFound`.
///
/// FALSIFY by answering `Ok` on a failed store removal, or by running the
/// cleanup before it: the error or the intact-state assertions fail.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_store_removal_reports_the_failure_and_removes_nothing() {
    let mut f = fixture().await;
    let s = seed(&mut f, &delegate(b"unregister-store-fails"), 0xD3);

    let reg = f.delegates_dir.join(s.key.encode()).with_extension("reg");
    std::fs::remove_file(&reg).expect("the registration wrote a .reg file");
    std::fs::create_dir(&reg).expect("plant a directory in its place");

    let err = unregister(&mut f, &s.key, ConnectionScope::Local)
        .expect_err("a failed store removal must be reported");
    assert!(
        !err.is_delegate_refusal(),
        "a store failure is not a refusal, got {err}"
    );
    assert_intact(&f, &s, "after a failed store removal");

    // Once the fault is gone, the same request completes the unregister: the
    // failure left nothing that blocks a retry.
    std::fs::remove_dir(&reg).expect("remove the planted directory");
    unregister(&mut f, &s.key, ConnectionScope::Local).expect("the retry succeeds");
    assert_cleaned(&f, &s, "after the retry");
}

/// A failure removing the delegate's code file, after its record and both
/// indexes are already gone, does not stop the unregister: the delegate is no
/// longer registered, so the rest of its state is removed and the request
/// succeeds. Failing here instead would leave that state behind with nothing
/// able to remove it, since every retry fails the same way.
///
/// The failure is injected by putting a non-empty directory where the code
/// file was.
///
/// FALSIFY by making the code-file removal in `DelegateStore::remove_delegate`
/// return its error: the request fails.
#[tokio::test(flavor = "multi_thread")]
async fn a_code_file_left_behind_does_not_block_the_unregister() {
    let mut f = fixture().await;
    let s = seed(&mut f, &delegate(b"unregister-code-file-stays"), 0xD4);

    let wasm = f
        .delegates_dir
        .join(s.key.code_hash().encode())
        .with_extension("wasm");
    std::fs::remove_file(&wasm).expect("the registration wrote the code file");
    std::fs::create_dir_all(wasm.join("blocker")).expect("plant a non-empty directory");

    unregister(&mut f, &s.key, ConnectionScope::Local)
        .expect("an unregistered delegate is reported unregistered");
    assert_cleaned(&f, &s, "after a code file could not be removed");
}

/// A key the store has not indexed names no delegate here. Unregistering it
/// succeeds and removes nothing; a registered delegate is untouched.
///
/// FALSIFY by removing the `code_hash_from_key` check at the top of
/// `Runtime::unregister_delegate`: the registered-delegate assertions fail.
#[tokio::test(flavor = "multi_thread")]
async fn an_unindexed_key_removes_nothing() {
    let mut f = fixture().await;
    let s = seed(&mut f, &delegate(b"unregister-other-code-hash"), 0xD5);
    let other = DelegateKey::new(
        s.key.bytes().try_into().expect("a 32-byte key"),
        freenet_stdlib::prelude::CodeHash::new([0xEE; 32]),
    );
    assert_ne!(other, s.key);

    unregister(&mut f, &other, ConnectionScope::Local)
        .expect("a key that names no delegate is not an error");
    assert!(
        f.delegates_dir
            .join(s.key.encode())
            .with_extension("reg")
            .exists(),
        "the registered delegate's record must survive"
    );
    assert!(
        f.executor
            .runtime
            .registered_delegate_params(&s.key)
            .is_ok(),
        "the registered delegate's parameters must still be readable"
    );
    assert_intact(&f, &s, "after unregistering a key with another code hash");
}

fn unregister_as(
    f: &mut Fixture,
    key: &DelegateKey,
    user: Option<&crate::wasm_runtime::UserSecretContext>,
) -> Result<HostResponse, crate::contract::ExecutorError> {
    f.executor.delegate_request(
        DelegateRequest::UnregisterDelegate(key.clone()),
        None,
        None,
        ConnectionScope::Local,
        user,
    )
}

/// On a hosted node, a user's `UnregisterDelegate` removes nothing at the
/// executor: the delegate and its state are shared with the node's other
/// users, and secrets are never removed by an unregister, so every user's
/// secrets and the node's own (`Local`) ones survive. The user's own push
/// registrations are removed one layer up, in `process_open_request`
/// (`client_events::unregister_dispatch_tests`).
///
/// FALSIFY by removing the `user_context` branch from the executor's
/// `UnregisterDelegate` arm: the hosted node refuses the request.
#[tokio::test(flavor = "multi_thread")]
async fn a_hosted_users_unregister_leaves_the_delegate_and_every_secret() {
    use crate::wasm_runtime::{SecretScope, UserSecretContext};
    use freenet_stdlib::prelude::SecretsId;

    let mut f = fixture().await;
    f.executor.hosted_mode = true;
    let s = seed(&mut f, &delegate(b"unregister-hosted-user"), 0xD6);
    let a = UserSecretContext::from_token(b"unregister-hosted-user-a");
    let b = UserSecretContext::from_token(b"unregister-hosted-user-b");
    let id = SecretsId::new(b"secret".to_vec());
    for (scope, value) in [
        (a.scope(), b"a".to_vec()),
        (b.scope(), b"b".to_vec()),
        (SecretScope::Local, b"local".to_vec()),
    ] {
        f.executor
            .runtime
            .store_secret_for_test(&s.key, &id, scope, value)
            .expect("store a secret");
    }

    let ok = unregister_as(&mut f, &s.key, Some(&a)).expect("a hosted user may unregister");
    assert!(matches!(ok, HostResponse::Ok), "got {ok:?}");

    let secret = |f: &Fixture, scope| f.executor.runtime.get_secret_for_test(&s.key, &id, scope);
    assert_eq!(
        secret(&f, a.scope()),
        Some(b"a".to_vec()),
        "the user's own secret stays"
    );
    assert_eq!(
        secret(&f, b.scope()),
        Some(b"b".to_vec()),
        "another user's secret stays"
    );
    assert_eq!(
        secret(&f, SecretScope::Local),
        Some(b"local".to_vec()),
        "the node's own secret stays"
    );
    assert_intact(&f, &s, "after a hosted user's unregister");
}

/// On a hosted node, a client without a user token is refused: the delegate
/// is shared by every user, and an unregister on a hosted node acts only for
/// the user who sends it.
///
/// FALSIFY by removing the `hosted_mode` check from the executor's
/// `UnregisterDelegate` arm: the request is not refused.
#[tokio::test(flavor = "multi_thread")]
async fn a_hosted_client_without_a_user_token_is_refused() {
    let mut f = fixture().await;
    f.executor.hosted_mode = true;
    let s = seed(&mut f, &delegate(b"unregister-hosted-tokenless"), 0xD7);

    let err = unregister_as(&mut f, &s.key, None)
        .expect_err("a hosted client without a user token must be refused");
    assert!(err.is_unregister_hosted(), "got {err}");
    assert!(
        err.is_request(),
        "the refusal is a request error, got {err}"
    );
    assert_intact(&f, &s, "after a refused tokenless hosted unregister");

    // Control: the same request on a node that is not hosted unregisters.
    f.executor.hosted_mode = false;
    unregister_as(&mut f, &s.key, None).expect("a local unregister on a normal node");
    assert_cleaned(&f, &s, "after a local unregister on a normal node");
}

/// End to end through a real WASM delegate: a child created through the
/// `create_delegate` host function holds a created-delegate slot, and
/// unregistering that child through the executor releases it. The other tests
/// seed the slot by hand, so they cannot catch the executor and the host
/// function disagreeing about which key holds the slot.
///
/// FALSIFY by recording a different key in `create_delegate_sync` (for
/// example the parent's): the child holds no slot.
#[tokio::test(flavor = "multi_thread")]
async fn unregistering_a_child_created_by_a_delegate_releases_its_slot() {
    use freenet_stdlib::prelude::{
        ApplicationMessage, CodeHash, InboundDelegateMsg, OutboundDelegateMsg, Parameters,
    };

    // Mirrors `tests/test-delegate-creation`'s message types (bincode, so the
    // variant order is the wire format).
    #[derive(serde::Serialize)]
    enum InboundAppMessage {
        CreateChildDelegate {
            child_wasm: Vec<u8>,
            child_params: Vec<u8>,
        },
    }
    #[derive(serde::Deserialize, Debug)]
    enum OutboundAppMessage {
        ChildCreated {
            key_bytes: Vec<u8>,
            code_hash_bytes: Vec<u8>,
        },
        CreateFailed {
            #[allow(dead_code)] // read only through the panic's `Debug`
            error_code: i32,
        },
    }

    let parent_code = tokio::task::spawn_blocking(|| {
        crate::wasm_runtime::tests::get_test_module("test_delegate_creation")
            .expect("build the creating fixture")
    })
    .await
    .expect("join fixture build");
    // Creation stores the child's bytes without compiling them, so the child
    // need not be a working module. A built fixture is not used here: its
    // size depends on the build environment, and a debug build can exceed
    // the 10 MiB creation limit (it did on CI).
    let child_code = b"\0asm\x01\0\0\0unregister-real-child".to_vec();

    let mut f = fixture().await;
    const PARENT_PARAMS: &[u8] = b"unregister-real-child-parent";
    let parent = Delegate::from((&parent_code.into(), &PARENT_PARAMS.to_vec().into()));
    register(&mut f, &parent);

    let payload = bincode::serialize(&InboundAppMessage::CreateChildDelegate {
        child_wasm: child_code,
        child_params: b"unregister-real-child".to_vec(),
    })
    .expect("encode");
    let response = f
        .executor
        .delegate_request(
            DelegateRequest::ApplicationMessages {
                key: parent.key().clone(),
                params: Parameters::from(PARENT_PARAMS.to_vec()),
                inbound: vec![InboundDelegateMsg::ApplicationMessage(
                    ApplicationMessage::new(payload),
                )],
            },
            None,
            None,
            ConnectionScope::Local,
            None,
        )
        .expect("the parent runs");
    let HostResponse::DelegateResponse { values, .. } = response else {
        panic!("expected a DelegateResponse, got {response:?}");
    };
    #[allow(clippy::wildcard_enum_match_arm)]
    let reply = values
        .into_iter()
        .find_map(|m| match m {
            OutboundDelegateMsg::ApplicationMessage(m) => Some(m.payload),
            _ => None,
        })
        .expect("the parent answers");
    let created: OutboundAppMessage = bincode::deserialize(&reply).expect("decode");
    let OutboundAppMessage::ChildCreated {
        key_bytes,
        code_hash_bytes,
    } = created
    else {
        panic!("the child was not created: {created:?}");
    };
    let child = DelegateKey::new(
        key_bytes.try_into().expect("a 32-byte key"),
        CodeHash::new(code_hash_bytes.try_into().expect("a 32-byte code hash")),
    );
    assert!(
        f.executor.runtime.created_delegates_count.contains(&child),
        "the created child holds a slot"
    );
    assert_eq!(f.executor.runtime.created_delegates_count.count(), 1);

    unregister(&mut f, &child, ConnectionScope::Local).expect("local unregister of the child");
    assert_eq!(
        f.executor.runtime.created_delegates_count.count(),
        0,
        "unregistering the child releases its slot"
    );
    assert!(f.executor.runtime.delegate_code(&child).is_none());
}

/// A non-local request carrying a hosted user's token is refused like any
/// other non-local one: the scope check comes before the per-user branch.
///
/// FALSIFY by moving the executor's `user_context` branch above the scope
/// check: the request succeeds.
#[tokio::test(flavor = "multi_thread")]
async fn a_non_local_request_with_a_user_token_is_refused() {
    use crate::wasm_runtime::UserSecretContext;
    use freenet_stdlib::prelude::SecretsId;

    let mut f = fixture().await;
    f.executor.hosted_mode = true;
    let s = seed(&mut f, &delegate(b"unregister-remote-user-token"), 0xD8);
    let a = UserSecretContext::from_token(b"unregister-remote-user-token-a");
    let id = SecretsId::new(b"secret".to_vec());
    f.executor
        .runtime
        .store_secret_for_test(&s.key, &id, a.scope(), b"a".to_vec())
        .expect("store a secret");

    let err = f
        .executor
        .delegate_request(
            DelegateRequest::UnregisterDelegate(s.key.clone()),
            None,
            None,
            ConnectionScope::Remote,
            Some(&a),
        )
        .expect_err("a non-local request must be refused");
    assert!(err.is_unregister_not_local(), "got {err}");
    assert_eq!(
        f.executor
            .runtime
            .get_secret_for_test(&s.key, &id, a.scope()),
        Some(b"a".to_vec()),
        "a refused request removes nothing"
    );
    assert_intact(
        &f,
        &s,
        "after a refused non-local request with a user token",
    );
}

/// The constructor a local-mode node uses reads `hosted_mode` from the node
/// config. The pool's constructor is covered through dispatch by
/// `client_events::unregister_dispatch_tests`; this pins the other one, since
/// a constructor that left the flag at its default would turn off the
/// tokenless-client refusal with nothing else noticing.
///
/// FALSIFY by removing the `executor.hosted_mode = hosted_mode` line in
/// `Executor::from_config`: the hosted assertion fails.
#[tokio::test(flavor = "multi_thread")]
async fn the_local_mode_constructor_reads_hosted_mode_from_the_config() {
    for hosted in [true, false] {
        let config_args = crate::config::ConfigArgs {
            id: Some(format!("unregister-hosted-flag-{hosted}")),
            mode: Some(crate::contract::OperationMode::Local),
            ws_api: crate::config::WebsocketApiArgs {
                hosted_mode: Some(hosted),
                ..Default::default()
            },
            ..Default::default()
        };
        let config = config_args.build().await.expect("build Config");
        let executor = Executor::from_config_local(std::sync::Arc::new(config))
            .await
            .expect("build the local-mode executor");
        assert_eq!(executor.hosted_mode, hosted);
    }
}

//! The user an inter-delegate hop runs under, at the executor, and the rule
//! that on a hosted node every delegate run belongs to a user.
//!
//! A hop runs its target under the user whose request led to the hop: that
//! user's secrets and that user's delegate context. There is one context per
//! delegate and hosted user, so a hop and a direct run for the same user share
//! that user's context, and a hop for another user does not. On a hosted node
//! a run with no user is refused before the delegate runs.

use freenet_stdlib::client_api::{DelegateRequest, HostResponse};
use freenet_stdlib::prelude::{
    ApplicationMessage, CodeHash, Delegate, DelegateContainer, DelegateKey, DelegateWasmAPIVersion,
    InboundDelegateMsg, OutboundDelegateMsg, Parameters,
};

use crate::client_events::ConnectionScope;
use crate::contract::executor::{ContractExecutor, Executor};
use crate::contract::storages::Storage;
use crate::wasm_runtime::{
    ContractStore, DelegateStore, Runtime, SecretsStore, StateStore, UserSecretContext,
};

/// Mirrors `tests/test-delegate-2`'s inbound message type (bincode, so the
/// variant order is the wire format). Only the variants these tests send are
/// named; the ones before them keep their indices.
#[allow(dead_code)]
#[derive(serde::Serialize)]
enum InboundAppMessage {
    CreateInboxRequest,
    PleaseSignMessage(Vec<u8>),
    WriteContext(Vec<u8>),
    ReadContext,
    ClearContext,
    IncrementCounter,
    HasSecret(Vec<u8>),
    GetNonExistentSecret(Vec<u8>),
    StoreSecret { key: Vec<u8>, value: Vec<u8> },
}

/// Mirrors the fixture's outbound type, up to the variants these tests read.
#[allow(dead_code)]
#[derive(serde::Deserialize, Debug)]
enum OutboundAppMessage {
    CreateInboxResponse(Vec<u8>),
    MessageSigned(Vec<u8>),
    ContextData(Vec<u8>),
    CounterValue(u32),
    SecretExists(bool),
    SecretResult(Option<Vec<u8>>),
    ContextWritten,
    ContextCleared,
    SecretStored,
}

const PARAMS: &[u8] = b"delegate-hop-context-user";

struct Fixture {
    executor: Executor<Runtime>,
    delegate: Delegate<'static>,
    _temp: tempfile::TempDir,
}

async fn fixture() -> Fixture {
    let code = tokio::task::spawn_blocking(|| {
        crate::wasm_runtime::tests::get_test_module("test_delegate_2")
            .expect("build the test_delegate_2 fixture")
    })
    .await
    .expect("join fixture build");

    let temp = crate::util::tests::get_temp_dir();
    let db = Storage::new(temp.path()).await.expect("create storage db");
    let contract_store = ContractStore::new(temp.path().join("contracts"), 10_000, db.clone())
        .expect("create contract store");
    let delegate_store = DelegateStore::new(temp.path().join("delegates"), 10_000, db.clone())
        .expect("create delegate store");
    let secrets_store =
        SecretsStore::new(temp.path().join("secrets"), Default::default(), db.clone())
            .expect("create secrets store");
    let state_store = StateStore::new(db, 10_000_000).expect("create state store");
    let runtime = Runtime::build(contract_store, delegate_store, secrets_store, false)
        .expect("build runtime");
    let mut executor = Executor::new(
        state_store,
        || Ok(()),
        crate::contract::executor::OperationMode::Local,
        runtime,
        None,
    )
    .await
    .expect("create executor");

    let delegate = Delegate::from((&code.into(), &PARAMS.to_vec().into()));
    executor
        .delegate_request(
            DelegateRequest::RegisterDelegate {
                delegate: DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(delegate.clone())),
                cipher: [7u8; 32],
                nonce: [9u8; 24],
            },
            None,
            None,
            ConnectionScope::Local,
            None,
        )
        .expect("registration must succeed");
    Fixture {
        executor,
        delegate,
        _temp: temp,
    }
}

fn increment_request(key: &DelegateKey) -> DelegateRequest<'static> {
    request(key, &InboundAppMessage::IncrementCounter)
}

fn request(key: &DelegateKey, msg: &InboundAppMessage) -> DelegateRequest<'static> {
    let payload = bincode::serialize(msg).expect("encode");
    DelegateRequest::ApplicationMessages {
        key: key.clone(),
        params: Parameters::from(PARAMS.to_vec()),
        inbound: vec![InboundDelegateMsg::ApplicationMessage(
            ApplicationMessage::new(payload),
        )],
    }
}

fn reply(response: crate::contract::executor::Response) -> OutboundAppMessage {
    let response = response.expect("the delegate runs");
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
        .expect("the delegate answers");
    bincode::deserialize(&reply).expect("decode")
}

fn counter(response: crate::contract::executor::Response) -> u32 {
    #[allow(clippy::wildcard_enum_match_arm)]
    match reply(response) {
        OutboundAppMessage::CounterValue(n) => n,
        other => panic!("expected CounterValue, got {other:?}"),
    }
}

/// Whether the delegate holds secret `sk` in the namespace of `user` (`None`
/// is the node-local namespace), asked by a direct run on a node that is not
/// hosted.
fn has_secret(
    f: &mut Fixture,
    key: &DelegateKey,
    user: Option<&UserSecretContext>,
    sk: &[u8],
) -> bool {
    let response = f.executor.delegate_request(
        request(key, &InboundAppMessage::HasSecret(sk.to_vec())),
        None,
        None,
        ConnectionScope::Local,
        user,
    );
    #[allow(clippy::wildcard_enum_match_arm)]
    match reply(response) {
        OutboundAppMessage::SecretExists(b) => b,
        other => panic!("expected SecretExists, got {other:?}"),
    }
}

/// A hop runs with the context of the user whose request led to it.
/// `IncrementCounter` keeps a counter in the context, so each user counts
/// independently, and a direct run for user A continues A's hop count.
///
/// FALSIFY: in `Executor<Runtime>::execute_delegate_hop` pass `None` instead
/// of `user_context`: every hop then uses the no-user context, so B's hop
/// reads 3 and A's direct run reads 1.
#[tokio::test(flavor = "multi_thread")]
async fn a_delegate_hop_uses_the_originating_users_context() {
    let mut f = fixture().await;
    let key = f.delegate.key().clone();
    let caller = DelegateKey::new([0xC1u8; 32], CodeHash::new([0xC2u8; 32]));
    let a = UserSecretContext::from_token(b"delegate-hop-context-user-a");
    let b = UserSecretContext::from_token(b"delegate-hop-context-user-b");

    async fn hop(
        f: &mut Fixture,
        key: &DelegateKey,
        caller: &DelegateKey,
        user: &UserSecretContext,
    ) -> u32 {
        counter(
            f.executor
                .execute_delegate_hop(
                    increment_request(key),
                    caller,
                    ConnectionScope::Local,
                    Some(user),
                )
                .await,
        )
    }

    assert_eq!(hop(&mut f, &key, &caller, &a).await, 1, "A's first hop");
    assert_eq!(hop(&mut f, &key, &caller, &a).await, 2, "A's second hop");
    assert_eq!(
        hop(&mut f, &key, &caller, &b).await,
        1,
        "B's hop starts from B's own context"
    );
    let direct = f.executor.delegate_request(
        increment_request(&key),
        None,
        None,
        ConnectionScope::Local,
        Some(&a),
    );
    assert_eq!(
        counter(direct),
        3,
        "a direct run for A uses the same context as A's hops"
    );
}

/// A hop runs its target under the originating user's secrets: a secret the
/// target stores during a hop for user X lands in X's namespace, not in the
/// node-local one and not in another user's.
///
/// FALSIFY: in `Executor<Runtime>::execute_delegate_hop` pass `None` for the
/// user: the secret lands in the node-local namespace and the first assertion
/// fails.
#[tokio::test(flavor = "multi_thread")]
async fn a_delegate_hop_runs_under_the_originating_users_secrets() {
    let mut f = fixture().await;
    let key = f.delegate.key().clone();
    let caller = DelegateKey::new([0xC3u8; 32], CodeHash::new([0xC4u8; 32]));
    let x = UserSecretContext::from_token(b"delegate-hop-secrets-user-x");
    let y = UserSecretContext::from_token(b"delegate-hop-secrets-user-y");
    let sk = b"hop-secret".to_vec();

    let stored = reply(
        f.executor
            .execute_delegate_hop(
                request(
                    &key,
                    &InboundAppMessage::StoreSecret {
                        key: sk.clone(),
                        value: b"written-by-a-hop".to_vec(),
                    },
                ),
                &caller,
                ConnectionScope::Local,
                Some(&x),
            )
            .await,
    );
    assert!(
        matches!(stored, OutboundAppMessage::SecretStored),
        "the hop's store succeeds, got {stored:?}"
    );

    assert!(
        has_secret(&mut f, &key, Some(&x), &sk),
        "the hop's secret is in the originating user's namespace"
    );
    assert!(
        !has_secret(&mut f, &key, None, &sk),
        "the hop's secret is not in the node-local namespace"
    );
    assert!(
        !has_secret(&mut f, &key, Some(&y), &sk),
        "the hop's secret is not in another user's namespace"
    );
}

/// On a hosted node every delegate run belongs to a user, so an
/// `ApplicationMessages` run with no user is refused before the delegate
/// runs, through both executor entry points, with the typed refusal. A run
/// with a user is unaffected, and on a node that is not hosted a run with no
/// user runs as it always has.
///
/// The counter shows the refused runs did not run: the no-user context still
/// reads 1 on the first run after hosted mode is turned off.
///
/// FALSIFY by removing the hosted check from the `ApplicationMessages` arm of
/// `Executor::delegate_request_as`: the refused runs succeed.
#[tokio::test(flavor = "multi_thread")]
async fn a_hosted_node_refuses_a_delegate_run_with_no_user() {
    let mut f = fixture().await;
    let key = f.delegate.key().clone();
    let caller = DelegateKey::new([0xC5u8; 32], CodeHash::new([0xC6u8; 32]));
    let a = UserSecretContext::from_token(b"hosted-run-needs-a-user-a");
    f.executor.hosted_mode = true;

    let direct = f
        .executor
        .delegate_request(
            increment_request(&key),
            None,
            None,
            ConnectionScope::Local,
            None,
        )
        .expect_err("a hosted node must refuse a run with no user");
    assert!(direct.is_hosted_run_without_user(), "got {direct}");
    assert!(
        direct.is_request(),
        "the refusal is a request error, got {direct}"
    );

    let hop = f
        .executor
        .execute_delegate_hop(
            increment_request(&key),
            &caller,
            ConnectionScope::Local,
            None,
        )
        .await
        .expect_err("a hosted node must refuse a hop with no user");
    assert!(hop.is_hosted_run_without_user(), "got {hop}");

    assert_eq!(
        counter(f.executor.delegate_request(
            increment_request(&key),
            None,
            None,
            ConnectionScope::Local,
            Some(&a),
        )),
        1,
        "a run with a user runs on a hosted node"
    );

    // Control: the same no-user run on a node that is not hosted.
    f.executor.hosted_mode = false;
    assert_eq!(
        counter(f.executor.delegate_request(
            increment_request(&key),
            None,
            None,
            ConnectionScope::Local,
            None,
        )),
        1,
        "a node that is not hosted runs it, and the refused runs left no trace"
    );
}

/// The pool's hop runs the target under the hop's user: that user's secrets
/// and context. The production node uses `RuntimePool`, which pops an
/// executor and forwards; the executor half of that is driven above, and this
/// pins the forwarding, since building a pool here would need a running node.
///
/// Bounded to the method body, and the call must be at statement position, so
/// a commented-out or moved call does not satisfy it.
///
/// FALSIFY by passing `None` for `user_context` in
/// `RuntimePool::execute_delegate_hop`: the argument assertion fails.
#[test]
fn the_pools_hop_forwards_the_user() {
    let src = include_str!("pool.rs");
    let start = src
        .find("    async fn execute_delegate_hop(")
        .expect("RuntimePool implements execute_delegate_hop");
    let body = &src[start..];
    let body = &body[..body
        .find("\n    }\n")
        .expect("the method is bounded by its closing brace")];
    let call = body
        .match_indices("executor.delegate_request(")
        .find(|(i, _)| {
            let line_start = body[..*i].rfind('\n').map_or(0, |n| n + 1);
            body[line_start..*i].trim() == "let result ="
        })
        .map(|(i, _)| &body[i..])
        .expect("the pool's hop calls delegate_request at statement position");
    let args: String = call
        .split(");")
        .next()
        .expect("the call is bounded")
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect();
    assert_eq!(
        args,
        "executor.delegate_request(req,None,Some(caller_delegate),connection_scope,user_context,",
        "the pool's hop must run the target under the hop's user"
    );
}

/// `Executor<Runtime>` reports the hosted flag it was built with, which is
/// what the contract loop reads to decide that a hosted node runs no delegate
/// on its own.
///
/// FALSIFY by making `Executor<Runtime>::hosted_mode` return `false`: the
/// first assertion fails.
#[tokio::test(flavor = "multi_thread")]
async fn the_executor_reports_its_hosted_flag() {
    let mut f = fixture().await;
    f.executor.hosted_mode = true;
    assert!(ContractExecutor::hosted_mode(&f.executor));
    f.executor.hosted_mode = false;
    assert!(!ContractExecutor::hosted_mode(&f.executor));
}

/// The pool, which the production node uses, reports the node's configured
/// hosted flag. Pinned by source because building a pool needs a running
/// node; bounded to the method body, and the read must be its only
/// statement.
///
/// FALSIFY by making `RuntimePool::hosted_mode` return `false`.
#[test]
fn the_pool_reports_the_configured_hosted_flag() {
    let src = include_str!("pool.rs");
    let sig = "    fn hosted_mode(&self) -> bool {";
    assert_eq!(
        src.matches(sig).count(),
        1,
        "RuntimePool implements hosted_mode exactly once"
    );
    let start = src.find(sig).expect("RuntimePool implements hosted_mode");
    let body = &src[start..];
    let body = &body[..body
        .find("\n    }\n")
        .expect("the method is bounded by its closing brace")];
    let statements: Vec<&str> = body
        .lines()
        .skip(1)
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with("//"))
        .collect();
    assert_eq!(
        statements,
        vec!["self.config.ws_api.hosted_mode"],
        "the pool must report the node's configured hosted flag"
    );
}

/// A hosted node has no delegate capabilities, so it starts no lifecycle or
/// wake-up run (`run_lifecycle` skips without them): on a hosted node the
/// node runs no delegate on its own. Pinned by source because building a pool
/// needs a running node; bounded to the `if` that chooses them.
///
/// FALSIFY by building capabilities on a hosted node in `RuntimePool::new`.
#[test]
fn a_hosted_pool_has_no_delegate_capabilities() {
    let src = include_str!("pool.rs");
    let gate = "let delegate_capabilities = if config.ws_api.hosted_mode {";
    assert_eq!(src.matches(gate).count(), 1, "one capability gate");
    let start = src.find(gate).expect("the gate exists") + gate.len();
    let hosted_arm = &src[start..start + src[start..].find("} else {").expect("an else arm")];
    let statements: Vec<&str> = hosted_arm
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with("//") && !l.starts_with("tracing::"))
        .collect();
    assert_eq!(
        statements,
        vec!["None"],
        "a hosted node must have no delegate capabilities"
    );
}

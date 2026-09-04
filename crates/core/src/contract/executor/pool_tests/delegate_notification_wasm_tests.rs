//! Behavioural coverage for the fan-out sites a MOCK runtime cannot reach.
//!
//! `perform_contract_put` and `get_updated_state` live on `impl
//! Executor<Runtime>` — the real WASM runtime — not on the generic impl the
//! `pool_tests` harness drives via `Executor<MockWasmRuntime>`. So the three
//! `finalize_state_commit` call sites they own, all newly routed through the
//! chokepoint for #5481, were covered only by the source-scrape pin in
//! `delegate_notification_tests`.
//!
//! A structural pin proves a call EXISTS. It does not prove the call is wired
//! correctly — right key, right params, right state — and the repo's own rule
//! review made the concrete case: swap the related-contract install's
//! `related_key`/`related_params` back to the enclosing `key`/`params` and every
//! call-count and write-count assertion still passes while delegates silently
//! stop being notified for that path. That is the same silent-failure shape as
//! #5481 itself, which is exactly the argument this PR makes elsewhere for
//! asserting the broadcast leg behaviourally rather than trusting the pin.
//!
//! So this module pays the cost of a real `Executor<Runtime>` — compiled WASM
//! contracts plus real stores — to assert the behaviour end to end at the
//! executor layer. It is feature-gated on `wasmtime-backend` alongside
//! `wasm_conformance_tests`, which is where the runtime-construction shape
//! comes from.
//!
//! The three sites and the test that covers each:
//!
//! | site | test |
//! |---|---|
//! | `perform_contract_put`, fresh-install branch | [`local_put_notifies_subscribed_delegates`] |
//! | `perform_contract_put`, existing-contract merge branch | [`local_reput_merge_notifies_subscribed_delegates`] |
//! | `get_updated_state`, related-contract install | [`related_contract_install_notifies_the_related_contracts_delegates`] |

use freenet_stdlib::client_api::ContractRequest;
use freenet_stdlib::prelude::*;
use std::sync::Arc;
use std::time::Duration;

use crate::client_events::ClientId;
use crate::config::ConfigArgs;
use crate::contract::executor::{DelegateNotification, Executor, OperationMode};
use crate::node::OpManager;
use crate::operations::get::GetResult;
use crate::wasm_runtime::{
    ContractStore, DELEGATE_SUBSCRIPTIONS, DelegateStore, Runtime, SecretsStore, StateStore,
};

/// The same mock-aligned fixture `wasm_conformance_tests` uses: its
/// `validate_state` always returns `Valid`, so a PUT of arbitrary bytes lands
/// and the test is about the fan-out rather than about contract semantics.
const MOCK_ALIGNED_CONTRACT: &str = "test-contract-mock-aligned";

/// Asks for a related contract from `update_state` on its first pass and
/// settles on the second. The only fixture that can enter the
/// fetch-and-install branch of `get_updated_state`.
const REQUIRES_RELATED_CONTRACT: &str = "test-contract-requires-related";

/// Every test loads its contracts under DISTINCT parameters.
///
/// `DELEGATE_SUBSCRIPTIONS` is process-global (#4824) and keyed by
/// `ContractInstanceId`. CI runs `cargo nextest` (process per test) but
/// contributors run plain `cargo test` (one process for all of them), and a
/// contract's instance id is `hash(code, params)` — so two tests loading the
/// same WASM with the same params would share one subscription entry and each
/// would see the other's delegate in its own channel. Distinct params give
/// distinct instance ids and the tests stop interacting. The fixtures ignore
/// their parameters, so this costs nothing but a second WASM instantiation.
fn params(tag: u8) -> Parameters<'static> {
    Parameters::from(vec![tag])
}

/// Registers one delegate against one contract and removes exactly that
/// registration on drop — `DELEGATE_SUBSCRIPTIONS` is process-global (#4824),
/// so a blanket `remove` would delete a concurrent test's entry under plain
/// `cargo test`. Mirrors the guard in the sibling module.
struct SubscriptionGuard {
    instance_id: ContractInstanceId,
    delegate: DelegateKey,
}

impl SubscriptionGuard {
    fn register(instance_id: ContractInstanceId, delegate: DelegateKey) -> Self {
        DELEGATE_SUBSCRIPTIONS
            .entry(instance_id)
            .or_default()
            .insert(delegate.clone());
        Self {
            instance_id,
            delegate,
        }
    }
}

impl Drop for SubscriptionGuard {
    fn drop(&mut self) {
        DELEGATE_SUBSCRIPTIONS.retain(|id, subs| {
            if id == &self.instance_id {
                subs.remove(&self.delegate);
            }
            !subs.is_empty()
        });
    }
}

/// Clears the sub-op GET stub however the test ends.
///
/// The stub is a thread-local, so leaving it set would leak into whatever test
/// the runtime schedules next on this thread under plain `cargo test`.
struct SubOpGetStubGuard;

impl Drop for SubOpGetStubGuard {
    fn drop(&mut self) {
        crate::contract::executor::runtime::set_test_sub_op_get_override(None);
    }
}

async fn build_op_manager(id: &str) -> (Arc<OpManager>, Box<dyn std::any::Any>) {
    let config_args = ConfigArgs {
        id: Some(id.to_string()),
        mode: Some(OperationMode::Local),
        ..Default::default()
    };
    let node_config =
        crate::node::NodeConfig::new(config_args.build().await.expect("build Config"))
            .await
            .expect("build NodeConfig");

    let (notification_rx, notification_tx) = crate::node::event_loop_notification_channel();
    let (ops_ch_channel, ch_channel, wait_for_event) = crate::contract::contract_handler_channel();
    let connection_manager = crate::ring::ConnectionManager::new(&node_config);
    let (result_router_tx, result_router_rx) = tokio::sync::mpsc::channel(100);
    let task_monitor = crate::node::background_task_monitor::BackgroundTaskMonitor::new();

    let op_manager = Arc::new(
        OpManager::new(
            notification_tx,
            ops_ch_channel,
            &node_config,
            crate::tracing::DynamicRegister::new(vec![]),
            connection_manager,
            result_router_tx,
            &task_monitor,
        )
        .expect("build OpManager"),
    );
    op_manager.ring.attach_op_manager(&op_manager);

    let guards: Box<dyn std::any::Any> = Box::new((
        notification_rx,
        ch_channel,
        wait_for_event,
        result_router_rx,
        task_monitor,
    ));
    (op_manager, guards)
}

/// A real `Executor<Runtime>` in local mode over real (temp-dir) stores, plus
/// the guards that must outlive it.
struct Harness {
    executor: Executor<Runtime>,
    _op_manager: Arc<OpManager>,
    _op_manager_guards: Box<dyn std::any::Any>,
    _temp_dir: tempfile::TempDir,
}

async fn build_harness(id: &str) -> Result<Harness, Box<dyn std::error::Error>> {
    let (op_manager, op_manager_guards) = build_op_manager(id).await;

    let temp_dir = crate::util::tests::get_temp_dir();
    let db = crate::contract::storages::Storage::new(temp_dir.path()).await?;
    let contract_store = ContractStore::new(temp_dir.path().join("contract"), 10_000, db.clone())?;
    let delegate_store = DelegateStore::new(temp_dir.path().join("delegate"), 10_000, db.clone())?;
    let secrets_store = SecretsStore::new(
        temp_dir.path().join("secrets"),
        Default::default(),
        db.clone(),
    )?;
    let state_store = StateStore::new(db, 10_000_000)?;
    let runtime = Runtime::build(contract_store, delegate_store, secrets_store, false)?;

    let executor = Executor::new(
        state_store,
        || Ok(()),
        OperationMode::Local,
        runtime,
        Some(op_manager.clone()),
    )
    .await?;

    Ok(Harness {
        executor,
        _op_manager: op_manager,
        _op_manager_guards: op_manager_guards,
        _temp_dir: temp_dir,
    })
}

async fn load(name: &'static str, params: Parameters<'static>) -> ContractContainer {
    tokio::task::spawn_blocking(move || crate::test_utils::load_contract(name, params))
        .await
        .expect("join contract compile")
        .expect("compile contract")
}

/// Await one delegate notification, failing with `context` rather than a bare
/// timeout so a regression names the path that stopped fanning out.
async fn expect_notification(
    rx: &mut tokio::sync::mpsc::Receiver<DelegateNotification>,
    context: &str,
) -> DelegateNotification {
    match tokio::time::timeout(Duration::from_secs(10), rx.recv()).await {
        Ok(Some(notification)) => notification,
        Ok(None) => panic!("delegate notification channel closed: {context}"),
        Err(_) => panic!("no delegate notification arrived: {context}"),
    }
}

/// #5481, on the path a mock runtime cannot reach: the FRESH-INSTALL branch of
/// `perform_contract_put` must notify subscribed delegates.
///
/// That branch stores a brand-new contract's state and, before this PR,
/// hand-inlined two of the four fan-out legs — dropping the telemetry leg and
/// the delegate leg. It is #5481 in a sibling file, and it was invisible to a
/// pin that scraped only `executor_impl.rs`.
///
/// Driven through the public `Executor::contract_requests`, which is the entry
/// point local-node mode uses, so the test exercises the real dispatch rather
/// than reaching for a private method.
#[tokio::test(flavor = "multi_thread")]
async fn local_put_notifies_subscribed_delegates() -> Result<(), Box<dyn std::error::Error>> {
    let contract = load(MOCK_ALIGNED_CONTRACT, params(1)).await;
    let contract_key = contract.key();

    let mut harness = build_harness("delegate-notify-local-put").await?;

    let delegate = DelegateKey::new([21u8; 32], CodeHash::new([21u8; 32]));
    let _subscription = SubscriptionGuard::register(*contract_key.id(), delegate.clone());

    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    harness.executor.set_delegate_notification_tx(tx);

    let state = WrappedState::new(b"local put fan-out".to_vec());
    harness
        .executor
        .contract_requests(
            ContractRequest::Put {
                contract: contract.clone(),
                state: state.clone(),
                related_contracts: RelatedContracts::default(),
                subscribe: false,
                blocking_subscribe: false,
            },
            ClientId::FIRST,
            None,
        )
        .await
        .map_err(|e| format!("local PUT failed: {e}"))?;

    let notification = expect_notification(
        &mut rx,
        "the fresh-install branch of perform_contract_put must notify subscribed \
         delegates. This is #5481 on the local PUT path — the branch installs a \
         brand-new contract's state and used to hand-inline only the WS-client \
         and broadcast legs",
    )
    .await;

    assert_eq!(
        notification.delegate_key, delegate,
        "notification must carry the subscribed delegate's key"
    );
    assert_eq!(
        notification.contract_id,
        *contract_key.id(),
        "notification must be keyed on the contract that was PUT"
    );
    let delivered: &[u8] = notification.new_state.as_ref().as_ref();
    assert_eq!(
        delivered,
        state.as_ref(),
        "notification must carry the state that was stored, not a re-read or a \
         reconstruction"
    );

    Ok(())
}

/// Issue a local PUT through the public request dispatcher.
async fn put(
    executor: &mut Executor<Runtime>,
    contract: ContractContainer,
    state: WrappedState,
) -> Result<(), String> {
    executor
        .contract_requests(
            ContractRequest::Put {
                contract,
                state,
                related_contracts: RelatedContracts::default(),
                subscribe: false,
                blocking_subscribe: false,
            },
            ClientId::FIRST,
            None,
        )
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// The OTHER `perform_contract_put` branch: a re-PUT into an
/// ALREADY-hosted contract merges and must fan out too.
///
/// This branch does not go through `verify_and_store_contract` at all — it
/// merges via `update_state`, writes with `state_store.update`, and before this
/// PR hand-inlined its own two legs. Structurally it is a different site from
/// the fresh install above and it fails independently, so it gets its own
/// assertion rather than being folded into that test.
///
/// The first PUT establishes the contract; only the SECOND one exercises the
/// merge branch, so the test asserts on the second notification and checks that
/// what arrives is the MERGED state, not the state that was already stored.
#[tokio::test(flavor = "multi_thread")]
async fn local_reput_merge_notifies_subscribed_delegates() -> Result<(), Box<dyn std::error::Error>>
{
    let contract = load(MOCK_ALIGNED_CONTRACT, params(2)).await;
    let contract_key = contract.key();

    let mut harness = build_harness("delegate-notify-local-reput").await?;

    let delegate = DelegateKey::new([22u8; 32], CodeHash::new([22u8; 32]));
    let _subscription = SubscriptionGuard::register(*contract_key.id(), delegate.clone());

    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    harness.executor.set_delegate_notification_tx(tx);

    let first = WrappedState::new(b"first install".to_vec());
    put(&mut harness.executor, contract.clone(), first.clone())
        .await
        .map_err(|e| format!("first PUT failed: {e}"))?;
    let install_notification =
        expect_notification(&mut rx, "the fresh-install branch must fan out first").await;
    assert_eq!(
        install_notification.contract_id,
        *contract_key.id(),
        "sanity: the install notification is for this test's contract"
    );

    // `test-contract-mock-aligned::update_state` takes the last incoming
    // `State`, so a different payload here produces a genuinely different
    // merged state and the branch does NOT take its no-change early return.
    let merged = WrappedState::new(b"merged by re-put".to_vec());
    put(&mut harness.executor, contract.clone(), merged.clone())
        .await
        .map_err(|e| format!("re-PUT failed: {e}"))?;

    let notification = expect_notification(
        &mut rx,
        "the existing-contract MERGE branch of perform_contract_put must notify \
         subscribed delegates. It is a distinct storing path from the fresh \
         install — it commits through state_store.update — and before #5481 it \
         hand-inlined only the WS-client and broadcast legs",
    )
    .await;

    assert_eq!(
        notification.delegate_key, delegate,
        "notification must carry the subscribed delegate's key"
    );
    assert_eq!(
        notification.contract_id,
        *contract_key.id(),
        "notification must be keyed on the merged contract"
    );
    let delivered: &[u8] = notification.new_state.as_ref().as_ref();
    assert_eq!(
        delivered,
        merged.as_ref(),
        "the merge branch must fan out the MERGED state, not the state that was \
         already stored"
    );

    Ok(())
}

/// The fifth `finalize_state_commit` site, and the one the rule review named:
/// `get_updated_state` installs a DIFFERENT contract mid-UPDATE — the related
/// one it fetched in order to validate the update — and owes THAT contract's
/// subscribers the fan-out.
///
/// The regression this exists to catch is specific: swapping the site's
/// `related_key`/`related_params` back to the enclosing `key`/`params`. Every
/// call-count and state-write pin in `delegate_notification_tests`
/// still passes under that swap, because the call is still there and still
/// counted — it is simply pointed at the wrong contract, and a delegate
/// subscribed to the related contract silently never hears about it. Here the
/// delegate is subscribed to the RELATED contract only, so under that swap the
/// notification is keyed on the target contract, this channel stays empty, and
/// the test fails.
///
/// Reaching the branch needs two things production has and a unit test does
/// not: a contract that asks for a related contract from `update_state`
/// (`test-contract-requires-related`) and a network that answers
/// (`set_test_sub_op_get_override`, the sub-op GET sibling of the existing
/// `set_test_network_fetch_override`). Both are test-only; the code path
/// between them is production's.
///
/// The stub is a thread-local, so this test — like the ones using
/// `set_test_network_fetch_override` — must stay on `current_thread`, or the
/// executor may run on a worker thread that cannot see it.
///
/// # What this test does NOT prove
///
/// That the branch runs in production today. It does not: `get_updated_state`
/// asks `local_state_or_from_network(&id, false)`, and the sub-op GET driver
/// hard-nulls the contract whenever `return_contract_code` is false
/// (`operations/get/op_ctx_task.rs`, `let client_contract = if
/// return_contract_code { contract } else { None }`), so the `let Some(contract)
/// = contract else` guard immediately above the install always takes its error
/// arm with "Missing contract". That mismatch predates #5481 — it arrived with
/// the executor split — and correcting it changes what this node asks the
/// network for, so it belongs in its own change rather than riding along here.
/// This test therefore pins the site's WIRING, which is what the rule review
/// asked for and what a future edit can silently break; it is deliberately not
/// evidence that the site is live.
#[tokio::test(flavor = "current_thread")]
async fn related_contract_install_notifies_the_related_contracts_delegates()
-> Result<(), Box<dyn std::error::Error>> {
    let target = load(REQUIRES_RELATED_CONTRACT, params(3)).await;
    let target_key = target.key();
    let related = load(MOCK_ALIGNED_CONTRACT, params(4)).await;
    let related_key = related.key();
    assert_ne!(
        target_key.id(),
        related_key.id(),
        "the two contracts must be distinct or the assertion below proves nothing"
    );

    let mut harness = build_harness("delegate-notify-related-install").await?;

    // Establish the contract being UPDATEd. This is the fresh-install branch
    // again; nothing is subscribed to it, so it fans out to nobody.
    harness
        .executor
        .contract_requests(
            ContractRequest::Put {
                contract: target.clone(),
                state: WrappedState::new(b"target initial".to_vec()),
                related_contracts: RelatedContracts::default(),
                subscribe: false,
                blocking_subscribe: false,
            },
            ClientId::FIRST,
            None,
        )
        .await
        .map_err(|e| format!("target PUT failed: {e}"))?;

    // Subscribed to the RELATED contract, which this node does not host yet.
    let delegate = DelegateKey::new([23u8; 32], CodeHash::new([23u8; 32]));
    let _subscription = SubscriptionGuard::register(*related_key.id(), delegate.clone());

    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    harness.executor.set_delegate_notification_tx(tx);

    // Stand in for the network: whatever instance id the target contract asks
    // for, answer with the related contract and its state. The executor
    // installs it under the RELATED contract's own key, which is the point.
    let related_state = WrappedState::new(b"state fetched for the related contract".to_vec());
    let _stub = SubOpGetStubGuard;
    {
        let related = related.clone();
        let related_state = related_state.clone();
        crate::contract::executor::runtime::set_test_sub_op_get_override(Some(std::rc::Rc::new(
            move |_id| Some(GetResult::new(related_state.clone(), Some(related.clone()))),
        )));
    }

    harness
        .executor
        .contract_requests(
            ContractRequest::Update {
                key: target_key,
                data: UpdateData::Delta(StateDelta::from(b"trigger".to_vec())),
            },
            ClientId::FIRST,
            None,
        )
        .await
        .map_err(|e| format!("local UPDATE failed: {e}"))?;

    let notification = expect_notification(
        &mut rx,
        "installing a related contract mid-UPDATE must fan out to the RELATED \
         contract's subscribers. An empty channel here is the named regression: \
         the site keyed its fan-out on the enclosing contract instead of on the \
         contract it just installed",
    )
    .await;

    assert_eq!(
        notification.contract_id,
        *related_key.id(),
        "the fan-out must be keyed on the contract that was just installed \
         (`related_key`), not on the contract being updated"
    );
    assert_ne!(
        notification.contract_id,
        *target_key.id(),
        "keying the fan-out on the enclosing contract is exactly the regression \
         this test exists to catch"
    );
    assert_eq!(
        notification.delegate_key, delegate,
        "notification must carry the delegate subscribed to the related contract"
    );
    let delivered: &[u8] = notification.new_state.as_ref().as_ref();
    assert_eq!(
        delivered,
        related_state.as_ref(),
        "the fan-out must carry the related contract's freshly-installed state"
    );

    // The related contract really was installed, not merely notified about —
    // otherwise the assertions above could hold for a notification emitted
    // before (or instead of) the store.
    let stored = harness
        .executor
        .state_store
        .get(&related_key)
        .await
        .map_err(|e| format!("related contract state must be stored locally: {e}"))?;
    assert_eq!(
        stored.as_ref(),
        related_state.as_ref(),
        "the related contract's state must be in the local store"
    );

    Ok(())
}

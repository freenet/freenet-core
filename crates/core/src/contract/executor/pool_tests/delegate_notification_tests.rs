//! Regression tests for the delegate half of the post-store fan-out
//! (issue #5481).
//!
//! A contract state commit has to reach four consumers: the dashboard
//! "last updated" telemetry, locally-subscribed WebSocket clients,
//! locally-subscribed *delegates*, and the network. Two production paths
//! store a state — the initial-install branch of
//! `bridged_upsert_contract_state_inner` (a contract whose `state_store`
//! entry is missing, which also covers ResyncResponse-driven recovery) and
//! `commit_state_update` (the merge path) — and until #5481 only the merge
//! path fanned out to delegates. A delegate whose subscription outlives the
//! contract's state entry therefore stopped receiving notifications for
//! exactly the deliveries that recover it, silently: the subscription is
//! still registered and no error is produced anywhere.
//!
//! This is the shape `.claude/rules/bug-prevention-patterns.md` calls
//! "manually-inlined originator side effects" — the install branch
//! re-inlined three of the four legs and dropped the fourth. The fix
//! extracts `Executor::finalize_state_commit`, which owns the whole
//! sequence, and calls it from both sites;
//! `finalize_state_commit_is_the_only_post_store_fan_out_site` below pins
//! that no branch may hand-inline a subset again.
//!
//! Harness mirrors `identical_input_probe_tests.rs`: a real
//! `OpManager`/`Ring` behind an `Executor<MockWasmRuntime>`, driving the
//! production `bridged_upsert_contract_state` path.

use either::Either;
use freenet_stdlib::prelude::*;
use std::sync::Arc;
use std::time::Duration;

use crate::config::ConfigArgs;
use crate::contract::executor::{ContractExecutor, Executor, OperationMode};
use crate::node::OpManager;
use crate::wasm_runtime::{DELEGATE_SUBSCRIPTIONS, MockStateStorage};

use super::super::mock_runtime::test::create_test_contract as test_contract;

/// Build a real `OpManager` backed by a temp-dir `Config`, mirroring
/// `identical_input_probe_tests.rs::build_op_manager`.
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

/// Registers one delegate against one contract in the process-global
/// `DELEGATE_SUBSCRIPTIONS` map and removes the entry on drop.
///
/// `DELEGATE_SUBSCRIPTIONS` is process-global (#4824), and CI runs
/// `cargo nextest` (process per test) while contributors run plain
/// `cargo test` (one process for all of them) — see
/// `.claude/rules/testing.md`. So each test uses a contract key derived
/// from its own name, and this guard clears the entry regardless of how
/// the test ends.
struct SubscriptionGuard {
    instance_id: ContractInstanceId,
}

impl SubscriptionGuard {
    fn register(instance_id: ContractInstanceId, delegate: DelegateKey) -> Self {
        DELEGATE_SUBSCRIPTIONS
            .entry(instance_id)
            .or_default()
            .insert(delegate);
        Self { instance_id }
    }
}

impl Drop for SubscriptionGuard {
    fn drop(&mut self) {
        DELEGATE_SUBSCRIPTIONS.remove(&self.instance_id);
    }
}

fn test_delegate_key(seed: u8) -> DelegateKey {
    DelegateKey::new([seed; 32], CodeHash::new([seed; 32]))
}

async fn build_executor(
    op_manager: &Arc<OpManager>,
) -> Executor<crate::contract::executor::mock_wasm_runtime::MockWasmRuntime, MockStateStorage> {
    Executor::new_mock_wasm("t", MockStateStorage::new(), None, Some(op_manager.clone()))
        .await
        .expect("build mock-wasm executor")
}

/// #5481: the INITIAL-STATE-INSTALL branch must notify subscribed
/// delegates.
///
/// Before the fix this branch called `record_contract_update`,
/// `send_update_notification` (WebSocket clients) and
/// `broadcast_state_change`, then returned — never reaching
/// `send_delegate_contract_notifications`. The delegate got nothing, with
/// no error anywhere. This test drives a genuinely new contract through
/// `upsert_contract_state` (which takes the install branch, exactly as a
/// network PUT or a ResyncResponse-driven apply does) and asserts the
/// notification lands.
#[tokio::test(flavor = "current_thread")]
async fn initial_state_install_notifies_subscribed_delegates() {
    let (op_manager, _guards) = build_op_manager("delegate-notify-install").await;
    let contract = test_contract(b"delegate_notify_install_contract");
    let key = contract.key();
    let state = WrappedState::new((1u8..=32).collect::<Vec<u8>>());

    let delegate = test_delegate_key(7);
    let _subscription = SubscriptionGuard::register(*key.id(), delegate.clone());

    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    let mut executor = build_executor(&op_manager).await;
    executor.set_delegate_notification_tx(tx);

    // A brand-new contract: no `state_store` entry, so this takes the
    // initial-install branch rather than the merge path.
    executor
        .upsert_contract_state(
            key,
            Either::Left(state.clone()),
            RelatedContracts::default(),
            Some(contract.clone()),
        )
        .await
        .expect("initial install");

    let notification = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect(
            "the initial-state-install branch must notify subscribed delegates; \
             no notification arrived (this is #5481 — the branch fanned out to \
             WebSocket clients and the network but never to delegates)",
        )
        .expect("delegate notification channel closed");

    assert_eq!(
        notification.delegate_key, delegate,
        "notification must carry the subscribed delegate's key"
    );
    assert_eq!(
        notification.contract_id,
        *key.id(),
        "notification must carry the installed contract's instance id"
    );
    let delivered: &[u8] = notification.new_state.as_ref().as_ref();
    assert_eq!(
        delivered,
        state.as_ref(),
        "notification must carry the state that was just installed"
    );
}

/// Control for the test above: the MERGE path has always notified
/// delegates (`commit_state_update` → `send_delegate_contract_notifications`).
///
/// Without this control, a harness mistake (no `delegate_notification_tx`,
/// a mis-keyed subscription, a mock runtime that never merges) would make
/// the install test fail for a reason that has nothing to do with #5481.
/// This one passes both before and after the fix.
#[tokio::test(flavor = "current_thread")]
async fn merge_path_notifies_subscribed_delegates() {
    let (op_manager, _guards) = build_op_manager("delegate-notify-merge").await;
    let contract = test_contract(b"delegate_notify_merge_contract");
    let key = contract.key();
    let initial = WrappedState::new((1u8..=32).collect::<Vec<u8>>());

    let delegate = test_delegate_key(9);
    let _subscription = SubscriptionGuard::register(*key.id(), delegate.clone());

    let (tx, mut rx) = tokio::sync::mpsc::channel(8);
    let mut executor = build_executor(&op_manager).await;
    executor.set_delegate_notification_tx(tx);

    executor
        .upsert_contract_state(
            key,
            Either::Left(initial.clone()),
            RelatedContracts::default(),
            Some(contract.clone()),
        )
        .await
        .expect("initial install");
    // Drain whatever the install produced so the assertion below is
    // unambiguously about the merge.
    while rx.try_recv().is_ok() {}

    // A DIFFERENT state now merges into the stored one — the merge path,
    // which reaches `commit_state_update`.
    let merged = WrappedState::new((33u8..=64).collect::<Vec<u8>>());
    executor
        .upsert_contract_state(
            key,
            Either::Left(merged.clone()),
            RelatedContracts::default(),
            None,
        )
        .await
        .expect("merge update");

    let notification = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("the merge path has always notified subscribed delegates")
        .expect("delegate notification channel closed");
    assert_eq!(notification.delegate_key, delegate);
    assert_eq!(notification.contract_id, *key.id());
}

/// Source-scrape pin: `finalize_state_commit` must own the WHOLE post-store
/// fan-out, and it must be the only place any leg of it is invoked.
///
/// The precedent this guards against is the bug itself: #5481 happened
/// because the install branch hand-inlined three of the four legs and
/// dropped the fourth, and a behavioral test for the missing leg did not
/// exist. A future path that stores a state and re-inlines its own subset
/// would reproduce it exactly. So this asserts (a) the helper contains
/// every required side effect, and (b) no other production site in
/// `executor_impl.rs` calls one of them directly.
///
/// Anchored on API surface (`record_contract_update(`,
/// `send_update_notification(`, …) rather than on local variable names, so
/// a rename inside the helper does not silently defeat the pin.
#[test]
fn finalize_state_commit_is_the_only_post_store_fan_out_site() {
    const EXECUTOR_IMPL_SRC: &str = include_str!("../runtime/executor_impl.rs");

    // Production slice only: everything before the first `#[cfg(test)]`,
    // so this file's own siblings and any test module in executor_impl.rs
    // cannot satisfy or break the pin.
    let production = EXECUTOR_IMPL_SRC
        .split("#[cfg(test)]")
        .next()
        .expect("split always yields at least one slice");

    let helper_start = production
        .find("async fn finalize_state_commit(")
        .expect("finalize_state_commit must exist — it is the post-store fan-out chokepoint");
    // The helper ends where the next `fn ` at method indentation begins.
    let helper_end = production[helper_start..]
        .find("\n    /// ")
        .map(|off| helper_start + off)
        .unwrap_or(production.len());
    let helper = &production[helper_start..helper_end];

    for required in [
        "record_contract_update(",
        "send_update_notification(",
        "send_delegate_contract_notifications(",
        "broadcast_state_change(",
    ] {
        assert!(
            helper.contains(required),
            "finalize_state_commit must invoke `{required}` — it owns the FULL \
             post-store fan-out. Dropping a leg here silently stops one class of \
             consumer (that is #5481: delegates were the dropped leg)."
        );
    }

    // Every leg is called exactly once in the production slice, and that
    // one call is inside the helper. A second call site means a branch
    // has started hand-inlining the sequence again.
    //
    // The needles carry a leading `.` so they match CALL sites and not the
    // `fn` definitions, and are written without `self` because rustfmt
    // splits a long call across lines (`self\n    .send_update_notification(`)
    // — a `self.`-prefixed needle would silently match nothing there and the
    // pin would pass vacuously.
    for leg in [
        ".record_contract_update(",
        ".send_update_notification(",
        ".send_delegate_contract_notifications(",
        ".broadcast_state_change(",
    ] {
        let count = count_call_sites(production, leg);
        assert_eq!(
            count, 1,
            "expected exactly 1 `{leg}` call site in the production slice of \
             executor_impl.rs (inside finalize_state_commit); found {count}. A \
             state-commit path must call `finalize_state_commit` rather than \
             re-inlining a subset of its legs — see the \
             'manually-inlined originator side effects' row in \
             `.claude/rules/bug-prevention-patterns.md`."
        );
        assert!(
            helper.contains(leg),
            "the single `{leg}` call site must be inside finalize_state_commit"
        );
    }

    // And both storing paths delegate to it.
    let calls = count_call_sites(production, ".finalize_state_commit(");
    assert_eq!(
        calls, 2,
        "expected exactly 2 `self.finalize_state_commit(` call sites (the \
         initial-state-install branch and `commit_state_update`); found \
         {calls}. If you added a third state-storing path, that is fine — it \
         MUST call the helper, and this expectation should be bumped with a \
         comment naming the new path."
    );
}

/// Count non-comment occurrences of `needle`, ignoring anything inside a
/// string literal (this file's own assertion messages contain the needles).
fn count_call_sites(src: &str, needle: &str) -> usize {
    src.lines()
        .filter(|line| {
            let trimmed = line.trim_start();
            if trimmed.starts_with("//") {
                return false;
            }
            strip_string_literals(line).contains(needle)
        })
        .count()
}

fn strip_string_literals(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut in_string = false;
    let mut prev_was_backslash = false;
    for c in line.chars() {
        if in_string {
            if c == '"' && !prev_was_backslash {
                in_string = false;
                out.push('"');
            }
        } else if c == '"' {
            in_string = true;
            out.push('"');
        } else {
            out.push(c);
        }
        prev_was_backslash = c == '\\' && !prev_was_backslash;
    }
    out
}

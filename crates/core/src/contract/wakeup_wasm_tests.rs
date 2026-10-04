//! Delegate wake-ups end to end: the PRODUCTION contract handler
//! (`NetworkContractHandler` over a real `RuntimePool`) running a REAL WASM
//! delegate (`tests/test-delegate-lifecycle`, which declares
//! `wakeups = [heartbeat = 60]`), with no client connected while the wake-ups
//! fire.
//!
//! The loop-level tests in `capability_loop_tests` cover the bounds with a
//! scripted mock delegate and a paused clock. They cannot show that a real
//! delegate built with `#[delegate(manifest(wakeups = ..))]` is recorded from
//! its own module, armed, and re-entered with a `WakeupFired` it decodes, all
//! through the code a node actually runs. This does.
//!
//! Time: the capability clock is real time plus an offset the test advances,
//! not a paused tokio clock. A paused clock auto-advances while a WASM call
//! waits on `spawn_blocking`, which fires the call's wall-clock backstop and
//! fails the run. With an offset, deadlines stay in the real future (the loop
//! does not spin) and a nudge through the handler channel makes the loop see
//! them as due.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use freenet_stdlib::client_api::DelegateRequest;
use freenet_stdlib::prelude::*;

use super::contract_handling;
use super::delegate_capabilities::{AppIdentity, BudgetLimits, DelegateCapabilities, Grant};
use super::executor::OperationMode;
use super::handler::{self, ContractHandler, ContractHandlerEvent, NetworkContractHandler};
use super::user_input::{CallerIdentity, UserInputPrompter};
use crate::config::{ConfigArgs, GlobalExecutor};
use crate::node::OpManager;
use crate::util::time_source::TimeSource;

/// Real time plus a test-controlled offset.
#[derive(Debug, Clone, Default)]
struct OffsetClock(Arc<Mutex<Duration>>);

impl OffsetClock {
    fn advance(&self, by: Duration) {
        *self.0.lock().unwrap() += by;
    }
}

impl TimeSource for OffsetClock {
    fn now(&self) -> tokio::time::Instant {
        tokio::time::Instant::now() + *self.0.lock().unwrap()
    }

    fn system_time_now(&self) -> SystemTime {
        SystemTime::now() + *self.0.lock().unwrap()
    }
}

/// Allows every capability prompt. The fixture never asks the user anything.
struct AllowAll;

impl UserInputPrompter for AllowAll {
    async fn prompt(
        &self,
        _request: &UserInputRequest<'static>,
        _delegate_key: &str,
        _caller: CallerIdentity,
    ) -> Option<(usize, ClientResponse<'static>)> {
        None
    }

    async fn prompt_capability(
        &self,
        _message: String,
        _labels: Vec<String>,
        _delegate_key: &str,
        _caller: CallerIdentity,
    ) -> Option<usize> {
        Some(super::delegate_capabilities::CapabilityPrompt::ALLOW_INDEX)
    }
}

struct Node {
    send: handler::ContractHandlerChannel<handler::SenderHalve>,
    caps: Arc<DelegateCapabilities>,
    clock: OffsetClock,
    handle: tokio::task::JoinHandle<Result<(), super::ContractError>>,
    _op_manager: Arc<OpManager>,
    _guards: Box<dyn std::any::Any>,
}

impl Drop for Node {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl Node {
    async fn start(id: &str) -> Self {
        let config_args = ConfigArgs {
            id: Some(id.to_string()),
            mode: Some(OperationMode::Local),
            ..Default::default()
        };
        let node_config =
            crate::node::NodeConfig::new(config_args.build().await.expect("build Config"))
                .await
                .expect("build NodeConfig");
        let config = node_config.config.clone();

        let (_notification_rx, notification_tx) = crate::node::event_loop_notification_channel();
        // The OpManager gets its own (unused) handler channel; the test talks
        // to the loop through `send` below.
        let (ops_ch_channel, ops_handler_half, ops_wait) = handler::contract_handler_channel();
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

        let (send, handler_half, _wait) = handler::contract_handler_channel();
        let mut handler =
            NetworkContractHandler::build(handler_half, op_manager.clone(), config.clone())
                .await
                .expect("build the production contract handler");

        // Same durable store the pool built its own capabilities over; only
        // the clock differs.
        let clock = OffsetClock::default();
        let storage = handler.executor().state_store().inner().clone();
        // No refill, so a balance can only move by a charge: the test can
        // then tell exactly whether a wake-up run was charged, and to which
        // buckets. The bursts (10 s per delegate, 30 s node-wide) are far
        // more than a few fixture runs spend.
        let caps = DelegateCapabilities::with_time_source(
            Arc::new(storage),
            Arc::new(clock.clone()),
            BudgetLimits {
                duty_refill_per_sec: Duration::ZERO,
                node_duty_refill_per_sec: Duration::ZERO,
                ..BudgetLimits::default()
            },
        );
        handler
            .executor()
            .set_delegate_capabilities_for_test(caps.clone());

        let handle = GlobalExecutor::spawn(contract_handling(handler, AllowAll));
        Node {
            send,
            caps,
            clock,
            handle,
            _op_manager: op_manager,
            _guards: Box::new((
                _notification_rx,
                ops_handler_half,
                ops_wait,
                result_router_rx,
                task_monitor,
                _wait,
            )),
        }
    }

    /// One round trip through the loop, so it runs an iteration and sees
    /// anything that fell due.
    async fn nudge(&self) {
        drop(
            self.send
                .send_to_handler(ContractHandlerEvent::GetQuery {
                    instance_id: ContractInstanceId::new([0xEE; 32]),
                    return_contract_code: false,
                })
                .await,
        );
    }

    async fn nudge_until(&self, label: &str, mut cond: impl FnMut() -> bool) {
        for _ in 0..400 {
            if cond() {
                return;
            }
            self.nudge().await;
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("timed out waiting for: {label}");
    }

    #[allow(clippy::wildcard_enum_match_arm)]
    async fn request(&self, event: ContractHandlerEvent) -> Vec<OutboundDelegateMsg> {
        match self
            .send
            .send_to_handler(event)
            .await
            .expect("handler reply")
        {
            ContractHandlerEvent::DelegateResponse(r) => r.expect("delegate request succeeds"),
            other => panic!("expected a DelegateResponse, got {other}"),
        }
    }

    /// Ask the delegate (as a client, AFTER the fact) how many wake-ups it
    /// has seen and with which parameters.
    async fn wakeups_seen(&self, key: &DelegateKey, params: &[u8]) -> Vec<u8> {
        let outbound = self
            .request(ContractHandlerEvent::DelegateRequest {
                req: DelegateRequest::ApplicationMessages {
                    key: key.clone(),
                    params: Parameters::from(params.to_vec()),
                    inbound: vec![InboundDelegateMsg::ApplicationMessage(
                        ApplicationMessage::new(b"wakeups?".to_vec()),
                    )],
                },
                origin_contract: None,
                connection_scope: crate::client_events::ConnectionScope::Local,
                user_context: None,
            })
            .await;
        #[allow(clippy::wildcard_enum_match_arm)]
        outbound
            .into_iter()
            .find_map(|m| match m {
                OutboundDelegateMsg::ApplicationMessage(m) => Some(m.payload),
                _ => None,
            })
            .expect("the fixture answers `wakeups?`")
    }
}

/// A real delegate declaring `wakeups = [heartbeat = 60]`, registered by an
/// app that the user allows to run in the background, is woken on schedule
/// by the production handler with no client connected, each time with its
/// registered parameters; revoking the grant stops it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_real_delegate_is_woken_with_no_client_connected() {
    const PARAMS: &[u8] = b"wake-params";
    let node = Node::start("wakeup-wasm-e2e").await;

    let code = crate::wasm_runtime::tests::get_test_module("test_delegate_lifecycle")
        .expect("build the fixture delegate");
    let manifest = DelegateManifest::from_wasm(&code)
        .expect("readable manifest")
        .expect("the fixture declares a manifest");
    assert_eq!(
        manifest.effective_wakeups(),
        vec![(b"heartbeat".to_vec(), Duration::from_secs(60))],
        "the fixture's wake-up must come from its own module"
    );
    let delegate = DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(Delegate::from((
        &code.into(),
        &PARAMS.to_vec().into(),
    ))));
    let key = delegate.key().clone();
    let app = ContractInstanceId::new([0x5A; 32]);

    // The app registers the delegate (a local connection with the app's
    // origin); the node asks, and the user allows.
    node.request(ContractHandlerEvent::DelegateRequest {
        req: DelegateRequest::RegisterDelegate {
            delegate,
            cipher: [0u8; 32],
            nonce: [0u8; 24],
        },
        origin_contract: Some(app),
        connection_scope: crate::client_events::ConnectionScope::Local,
        user_context: None,
    })
    .await;
    node.nudge_until("the Background grant", || {
        matches!(
            node.caps
                .grant(&AppIdentity::WebApp(app), Capability::Background),
            Some(Grant::Granted { .. })
        )
    })
    .await;
    let delivered = || node.caps.stats.wakeups_delivered.load(Ordering::Relaxed);

    // From here on, no client talks to the node until the wake-ups are in.
    // Nothing fires before the first one is due.
    node.nudge().await;
    assert_eq!(delivered(), 0, "no wake-up before it is due");

    // `Installed` has run by now (it is due at once after the grant); let it
    // finish, then take the balances it left.
    node.nudge_until("Installed", || {
        node.caps.stats.lifecycle_delivered.load(Ordering::Relaxed) >= 1
    })
    .await;
    let (delegate_before, node_before) = node.caps.duty_balances_us(&key);
    let delegate_before = delegate_before.expect("Installed charged the delegate");

    // First fire: within the start-up window (5 s + up to 60 s).
    node.clock.advance(Duration::from_secs(66));
    node.nudge_until("the first wake-up", || delivered() >= 1)
        .await;
    // ONE budget: the wake-up run was charged to the same per-delegate and
    // node-wide duty buckets lifecycle runs use (no refill, so any decrease
    // is a charge).
    let (delegate_after, node_after) = node.caps.duty_balances_us(&key);
    assert!(
        delegate_after.expect("still tracked") < delegate_before,
        "a wake-up run must be charged to its delegate's duty budget"
    );
    assert!(
        node_after < node_before,
        "a wake-up run must be charged to the node-wide duty budget"
    );
    // Second: one interval (+ <= 10% jitter) later.
    node.clock.advance(Duration::from_secs(67));
    node.nudge_until("the second wake-up", || delivered() >= 2)
        .await;
    assert_eq!(delivered(), 2);
    assert_eq!(node.caps.stats.wakeups_failed.load(Ordering::Relaxed), 0);

    // The delegate itself counted two `WakeupFired { tag: "heartbeat" }`,
    // each run with its REGISTERED parameters.
    let mut expected = b"wakeups:2:".to_vec();
    expected.extend_from_slice(PARAMS);
    assert_eq!(
        String::from_utf8_lossy(&node.wakeups_seen(&key, PARAMS).await),
        String::from_utf8_lossy(&expected)
    );

    // Revoked: the next fire finds no grant and the schedule ends.
    assert!(
        node.caps
            .revoke(&AppIdentity::WebApp(app), Capability::Background)
    );
    node.clock.advance(Duration::from_secs(67));
    node.nudge_until("the schedule to end", || {
        node.caps.stats.wakeups_stopped.load(Ordering::Relaxed) == 1
    })
    .await;
    node.clock.advance(Duration::from_secs(300));
    for _ in 0..5 {
        node.nudge().await;
    }
    assert_eq!(delivered(), 2, "no wake-up after revocation");
}

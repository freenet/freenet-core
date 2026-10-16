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
//! The same harness also drives the other run the node starts on its own, a
//! contract notification, against `tests/test-delegate-echo-params` (#5616):
//! see [`a_notification_run_sees_the_same_params_as_a_client_run`].
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

/// #5616, end to end on the same production handler and pool: a REAL
/// parameterized delegate, driven once by a client request and once by a
/// contract notification, must see the SAME parameters both times.
///
/// The client supplies the parameters itself; the notification run is
/// started by the node, which used to hand the delegate EMPTY ones — nothing
/// errored and nothing logged, the delegate simply ran as a different
/// instance of itself. The notification leg is the real one: a contract PUT
/// through the handler, fanned out by the pool to the subscribed delegate,
/// run by the loop. The fixture records the parameters of its notification
/// run in its secrets, since that run has no client to answer.
///
/// FALSIFY by restoring `params: Parameters::from(vec![])` in
/// `handle_delegate_notification`: the notified parameters come back empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_notification_run_sees_the_same_params_as_a_client_run() {
    const PARAMS: &[u8] = b"room-5616";
    let node = Node::start("notification-params-wasm-e2e").await;

    let code = tokio::task::spawn_blocking(|| {
        crate::wasm_runtime::tests::get_test_module("test_delegate_echo_params")
            .expect("build the fixture delegate")
    })
    .await
    .expect("join fixture build");
    let delegate = DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(Delegate::from((
        &code.into(),
        &PARAMS.to_vec().into(),
    ))));
    let key = delegate.key().clone();
    node.request(ContractHandlerEvent::DelegateRequest {
        req: DelegateRequest::RegisterDelegate {
            delegate,
            cipher: [0u8; 32],
            nonce: [0u8; 24],
        },
        origin_contract: None,
        connection_scope: crate::client_events::ConnectionScope::Local,
        user_context: None,
    })
    .await;

    #[allow(clippy::wildcard_enum_match_arm)]
    let report = || async {
        let outbound = node
            .request(ContractHandlerEvent::DelegateRequest {
                req: DelegateRequest::ApplicationMessages {
                    key: key.clone(),
                    params: Parameters::from(PARAMS.to_vec()),
                    inbound: vec![InboundDelegateMsg::ApplicationMessage(
                        ApplicationMessage::new(b"params?".to_vec()),
                    )],
                },
                origin_contract: None,
                connection_scope: crate::client_events::ConnectionScope::Local,
                user_context: None,
            })
            .await;
        let payload = outbound
            .into_iter()
            .find_map(|m| match m {
                OutboundDelegateMsg::ApplicationMessage(m) => Some(m.payload),
                _ => None,
            })
            .expect("the fixture answers `params?`");
        String::from_utf8(payload).expect("the fixture answers in UTF-8")
    };

    // Client leg, before any notification.
    assert_eq!(report().await, "client:room-5616|notified:none");

    // Notification leg: subscribe the delegate to a contract, then change
    // the contract's state through the handler. Distinct contract params so
    // the process-global subscription registry cannot collide with another
    // test's (see `delegate_notification_wasm_tests::params`).
    let contract = tokio::task::spawn_blocking(|| {
        crate::test_utils::load_contract(
            "test-contract-mock-aligned",
            Parameters::from(b"5616".to_vec()),
        )
    })
    .await
    .expect("join contract compile")
    .expect("compile contract");
    let contract_id = *contract.key().id();
    crate::wasm_runtime::delegate_subscriptions::subscribe(
        contract_id,
        &key,
        crate::wasm_runtime::delegate_subscriptions::Durability::InMemoryOnly,
    );
    struct Unsubscribe(ContractInstanceId, DelegateKey);
    impl Drop for Unsubscribe {
        fn drop(&mut self) {
            crate::wasm_runtime::delegate_subscriptions::unsubscribe(
                &self.0,
                &self.1,
                crate::wasm_runtime::delegate_subscriptions::Durability::InMemoryOnly,
            );
        }
    }
    let _unsubscribe = Unsubscribe(contract_id, key.clone());

    #[allow(clippy::wildcard_enum_match_arm)]
    match node
        .send
        .send_to_handler(ContractHandlerEvent::PutQuery {
            key: contract.key(),
            state: WrappedState::new(b"changed".to_vec()),
            related_contracts: RelatedContracts::default(),
            contract: Some(contract.clone()),
        })
        .await
        .expect("handler reply")
    {
        ContractHandlerEvent::PutResponse { new_value, .. } => {
            new_value.expect("the PUT lands");
        }
        other => panic!("expected a PutResponse, got {other}"),
    }

    // The notification is delivered on a later loop iteration.
    let mut last = String::new();
    for _ in 0..400 {
        last = report().await;
        if !last.ends_with("notified:none") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(
        last, "client:room-5616|notified:room-5616",
        "a contract-notification run must get the parameters the delegate was \
         registered with, exactly as a client run does (#5616)"
    );
}

/// A client `ApplicationMessages` request must carry parameters that derive
/// the delegate key it names, under the code hash this node indexed for that
/// key. Otherwise it is refused with a typed `DelegateError`, and the delegate
/// does not run. Driven through the production handler and pool against a
/// real WASM delegate.
///
/// "Does not run" is observed directly: the refused request carries a
/// `ContractNotification`, which makes the fixture record the parameters of
/// that run in its secrets. A later matching query must still report
/// `notified:none`. The positive control at the end sends the same message
/// with matching parameters and sees it recorded, so the probe can detect a
/// run.
///
/// FALSIFY by removing the `key_derives_from` check in
/// `Runtime::prepare_delegate_call`: the mismatched requests then succeed and
/// the first assertion fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_request_whose_params_do_not_derive_the_key_is_refused() {
    use freenet_stdlib::client_api::{DelegateError, RequestError};

    const PARAMS: &[u8] = b"room-a";
    let node = Node::start("mismatched-params-wasm-e2e").await;

    let code = tokio::task::spawn_blocking(|| {
        crate::wasm_runtime::tests::get_test_module("test_delegate_echo_params")
            .expect("build the fixture delegate")
    })
    .await
    .expect("join fixture build");

    // The same code registered twice: with parameters, and with none.
    let mut keys = Vec::new();
    for params in [PARAMS.to_vec(), Vec::new()] {
        let delegate = DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(Delegate::from((
            &code.clone().into(),
            &params.into(),
        ))));
        keys.push(delegate.key().clone());
        node.request(ContractHandlerEvent::DelegateRequest {
            req: DelegateRequest::RegisterDelegate {
                delegate,
                cipher: [0u8; 32],
                nonce: [0u8; 24],
            },
            origin_contract: None,
            connection_scope: crate::client_events::ConnectionScope::Local,
            user_context: None,
        })
        .await;
    }
    let (key, empty_key) = (keys[0].clone(), keys[1].clone());

    let send = |key: DelegateKey, params: Vec<u8>, inbound: Vec<InboundDelegateMsg<'static>>| {
        let send = &node.send;
        async move {
            #[allow(clippy::wildcard_enum_match_arm)]
            match send
                .send_to_handler(ContractHandlerEvent::DelegateRequest {
                    req: DelegateRequest::ApplicationMessages {
                        key,
                        params: Parameters::from(params),
                        inbound,
                    },
                    origin_contract: None,
                    connection_scope: crate::client_events::ConnectionScope::Local,
                    user_context: None,
                })
                .await
                .expect("handler reply")
            {
                ContractHandlerEvent::DelegateResponse(r) => r,
                other => panic!("expected a DelegateResponse, got {other}"),
            }
        }
    };
    let record_run = || {
        InboundDelegateMsg::ContractNotification(ContractNotification {
            contract_id: ContractInstanceId::new([0x33; 32]),
            new_state: WrappedState::new(b"s".to_vec()),
            context: DelegateContext::default(),
        })
    };
    let ask =
        || InboundDelegateMsg::ApplicationMessage(ApplicationMessage::new(b"params?".to_vec()));
    #[allow(clippy::wildcard_enum_match_arm)]
    let report = |key: DelegateKey, params: &[u8]| {
        let fut = send(key, params.to_vec(), vec![ask()]);
        async move {
            let payload = fut
                .await
                .expect("a matching request runs")
                .into_iter()
                .find_map(|m| match m {
                    OutboundDelegateMsg::ApplicationMessage(m) => Some(m.payload),
                    _ => None,
                })
                .expect("the fixture answers `params?`");
            String::from_utf8(payload).expect("the fixture answers in UTF-8")
        }
    };
    #[allow(clippy::wildcard_enum_match_arm)]
    let assert_refused = |result: Result<Vec<OutboundDelegateMsg>, super::ExecutorError>,
                          refused: &DelegateKey,
                          what: &str| {
        let err = result.expect_err(what);
        assert!(
            err.is_params_mismatch(),
            "{what}: the typed refusal flag must survive the production path"
        );
        match err.unwrap_request() {
            // A literal, so the client-facing text is pinned.
            RequestError::DelegateError(DelegateError::ExecutionError(msg)) => assert_eq!(
                msg,
                format!("delegate request parameters do not match the delegate key {refused}"),
                "{what}: wrong refusal message"
            ),
            other => panic!("{what}: expected the params-mismatch refusal, got {other:?}"),
        }
    };

    // Parameters that do not derive the key: other bytes, none at all, and
    // a key registered with none given some.
    assert_refused(
        send(key.clone(), b"room-b".to_vec(), vec![record_run()]).await,
        &key,
        "other params must be refused",
    );
    assert_refused(
        send(key.clone(), Vec::new(), vec![record_run()]).await,
        &key,
        "empty params for a parameterized key must be refused",
    );
    assert_refused(
        send(empty_key.clone(), PARAMS.to_vec(), vec![record_run()]).await,
        &empty_key,
        "params for an empty-params key must be refused",
    );
    // With no inbound messages too: a refusal, not an empty success.
    assert_refused(
        send(key.clone(), b"room-b".to_vec(), vec![]).await,
        &key,
        "mismatched params with no inbound must be refused",
    );
    send(key.clone(), PARAMS.to_vec(), vec![])
        .await
        .expect("matching params with no inbound succeed");

    // None of those ran: nothing was recorded for either key.
    assert_eq!(
        report(key.clone(), PARAMS).await,
        "client:room-a|notified:none"
    );
    assert_eq!(
        report(empty_key.clone(), &[]).await,
        "client:|notified:none"
    );

    // Code is resolved through this node's index, never through the code
    // hash a request's key carries. The same key bytes naming other code are
    // not a registered delegate, and get the existing `Missing` answer.
    let unregistered_code = DelegateKey::new(
        key.bytes().try_into().expect("32-byte key"),
        CodeHash::new([0x77; 32]),
    );
    #[allow(clippy::wildcard_enum_match_arm)]
    match send(
        unregistered_code.clone(),
        PARAMS.to_vec(),
        vec![record_run()],
    )
    .await
    .expect_err("a key naming other code is not registered")
    .unwrap_request()
    {
        RequestError::DelegateError(DelegateError::Missing(k)) => assert_eq!(k, unregistered_code),
        other => panic!("expected DelegateError::Missing, got {other:?}"),
    }

    // Positive control: matching params run, and the probe sees the run.
    send(key.clone(), PARAMS.to_vec(), vec![record_run()])
        .await
        .expect("matching params run");
    assert_eq!(report(key, PARAMS).await, "client:room-a|notified:room-a");
    send(empty_key.clone(), Vec::new(), vec![record_run()])
        .await
        .expect("matching empty params run");
    assert_eq!(report(empty_key, &[]).await, "client:|notified:");
}

/// Wire mirror of `tests/test-delegate-messaging`'s message types (bincode,
/// so the variant order must match the fixture's).
mod messaging {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize)]
    pub(super) enum Inbound {
        SendToDelegate {
            target_key_bytes: Vec<u8>,
            target_code_hash: Vec<u8>,
            payload: Vec<u8>,
        },
        Ping {
            data: Vec<u8>,
        },
        VerifySecret {
            expected: Vec<u8>,
        },
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    pub(super) enum Outbound {
        MessageSent,
        DelegateMessageReceived {
            sender_key_bytes: Vec<u8>,
            payload: Vec<u8>,
            origin_delegate_key_bytes: Option<Vec<u8>>,
        },
        PingResponse {
            data: Vec<u8>,
        },
        SecretStored {
            byte_count: u64,
            sender_key_bytes: Vec<u8>,
            origin_delegate_key_bytes: Option<Vec<u8>>,
        },
        SecretVerified {
            present: bool,
            matches: bool,
        },
    }
}

/// An inter-delegate hop into a delegate registered with NON-empty params
/// runs it under its own key: the hop reads the target's registered params
/// (#5616), which derive its key, so the params check lets it through. The
/// target stores a secret on the hop, and a later client request to the
/// target finds it.
///
/// FALSIFY by making the hop send `Parameters::from(vec![])` instead of the
/// registered params: the hop is refused and nothing is stored.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hop_into_a_parameterized_delegate_runs_it() {
    use messaging::{Inbound, Outbound};

    let node = Node::start("param-hop-wasm-e2e").await;
    let code = tokio::task::spawn_blocking(|| {
        crate::wasm_runtime::tests::get_test_module("test_delegate_messaging")
            .expect("build the fixture delegate")
    })
    .await
    .expect("join fixture build");

    let mut keys = Vec::new();
    for params in [b"sender".to_vec(), b"target-room".to_vec()] {
        let delegate = DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(Delegate::from((
            &code.clone().into(),
            &params.into(),
        ))));
        keys.push(delegate.key().clone());
        node.request(ContractHandlerEvent::DelegateRequest {
            req: DelegateRequest::RegisterDelegate {
                delegate,
                cipher: [0u8; 32],
                nonce: [0u8; 24],
            },
            origin_contract: None,
            connection_scope: crate::client_events::ConnectionScope::Local,
            user_context: None,
        })
        .await;
    }
    let (sender, target) = (keys[0].clone(), keys[1].clone());

    #[allow(clippy::wildcard_enum_match_arm)]
    let call = |key: DelegateKey, params: &'static [u8], msg: Inbound| {
        let node = &node;
        async move {
            node.request(ContractHandlerEvent::DelegateRequest {
                req: DelegateRequest::ApplicationMessages {
                    key,
                    params: Parameters::from(params.to_vec()),
                    inbound: vec![InboundDelegateMsg::ApplicationMessage(
                        ApplicationMessage::new(bincode::serialize(&msg).unwrap()),
                    )],
                },
                origin_contract: None,
                connection_scope: crate::client_events::ConnectionScope::Local,
                user_context: None,
            })
            .await
            .into_iter()
            .filter_map(|m| match m {
                OutboundDelegateMsg::ApplicationMessage(m) => {
                    Some(bincode::deserialize::<Outbound>(&m.payload).expect("fixture reply"))
                }
                _ => None,
            })
            .collect::<Vec<_>>()
        }
    };

    let replies = call(
        sender.clone(),
        b"sender",
        Inbound::SendToDelegate {
            target_key_bytes: target.bytes().to_vec(),
            target_code_hash: target.code_hash().as_ref().to_vec(),
            payload: b"SECRET:hop-secret".to_vec(),
        },
    )
    .await;
    assert!(
        replies.contains(&Outbound::MessageSent),
        "the sender ran: {replies:?}"
    );
    assert!(
        replies
            .iter()
            .any(|r| matches!(r, Outbound::SecretStored { byte_count: 10, .. })),
        "the hop must run the parameterized target: {replies:?}"
    );

    assert_eq!(
        call(
            target,
            b"target-room",
            Inbound::VerifySecret {
                expected: b"hop-secret".to_vec(),
            },
        )
        .await,
        vec![Outbound::SecretVerified {
            present: true,
            matches: true
        }],
        "the hop stored the secret under the target's own key"
    );
}

//! Delegate capabilities driven through the real `contract_handling` loop over
//! a scripted mock delegate: the forged-lifecycle refusal, consent at
//! registration, `Installed`/`NodeStarted` delivery, and the unprompted-run
//! budget.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use freenet_stdlib::client_api::DelegateRequest;
use freenet_stdlib::prelude::*;

use super::MockWasmContractHandler;
use super::delegate_capabilities::{
    AppIdentity, BudgetLimits, DelegateCapabilities, Grant, LifecycleRun, MemoryCapabilityStorage,
};
use super::executor::mock_wasm_runtime::ScriptedRun;
use super::handler::{self, ContractHandlerEvent};
use super::user_input::{self, CallerIdentity, UserInputPrompter};
use super::{ContractError, contract_handling};
use crate::config::GlobalExecutor;
use crate::util::time_source::InstantTimeSrc;

/// Answers every capability prompt with a fixed label index, or hangs until
/// `gate` gets a permit when one is given. Delegate prompts are approved.
struct CapabilityPrompter {
    answer: Option<usize>,
    gate: Option<Arc<tokio::sync::Semaphore>>,
    asked: Arc<std::sync::atomic::AtomicUsize>,
}

impl CapabilityPrompter {
    fn answering(answer: Option<usize>) -> Self {
        Self {
            answer,
            gate: None,
            asked: Arc::default(),
        }
    }
}

impl UserInputPrompter for CapabilityPrompter {
    async fn prompt(
        &self,
        request: &UserInputRequest<'static>,
        _delegate_key: &str,
        _caller: CallerIdentity,
        _owner: Option<crate::wasm_runtime::UserId>,
    ) -> Option<(usize, ClientResponse<'static>)> {
        request
            .responses
            .first()
            .map(|r| (0, r.clone().into_owned()))
    }

    async fn prompt_capability(
        &self,
        message: String,
        _labels: Vec<String>,
        _delegate_key: &str,
        caller: CallerIdentity,
    ) -> Option<usize> {
        assert!(
            matches!(caller, CallerIdentity::WebApp(_)),
            "a capability prompt always names the app"
        );
        assert!(message.contains("Freenet starts"), "{message}");
        self.asked.fetch_add(1, Ordering::SeqCst);
        if let Some(gate) = &self.gate {
            gate.acquire().await.expect("gate").forget();
        }
        self.answer
    }
}

fn app(n: u8) -> ContractInstanceId {
    ContractInstanceId::new([n; 32])
}

/// A WASM module holding only a manifest custom section. The mock runtime
/// never executes it; the node only walks its sections.
fn manifest_module(manifest: &DelegateManifest) -> Vec<u8> {
    let payload = manifest.to_bytes();
    let name = MANIFEST_SECTION_NAME.as_bytes();
    let mut body = vec![name.len() as u8];
    body.extend_from_slice(name);
    body.extend_from_slice(&payload);
    let mut m = vec![0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x00];
    let mut len = body.len() as u32;
    loop {
        let mut b = (len & 0x7f) as u8;
        len >>= 7;
        if len != 0 {
            b |= 0x80;
        }
        m.push(b);
        if len == 0 {
            break;
        }
    }
    m.extend(body);
    m
}

fn background(lifecycle: Vec<LifecycleKind>) -> DelegateManifest {
    DelegateManifest::new(lifecycle, vec![Capability::Background])
}

fn container(manifest: &DelegateManifest, params: &[u8]) -> DelegateContainer {
    let code = manifest_module(manifest);
    DelegateContainer::Wasm(DelegateWasmAPIVersion::V1(Delegate::from((
        &code.into(),
        &params.to_vec().into(),
    ))))
}

fn register(
    delegate: DelegateContainer,
    origin: Option<ContractInstanceId>,
) -> ContractHandlerEvent {
    ContractHandlerEvent::DelegateRequest {
        req: DelegateRequest::RegisterDelegate {
            delegate,
            cipher: [0u8; 32],
            nonce: [0u8; 24],
        },
        origin_contract: origin,
        connection_scope: crate::client_events::ConnectionScope::Local,
        user_context: None,
        lane_ticket: Default::default(),
    }
}

fn app_messages(
    key: &DelegateKey,
    inbound: Vec<InboundDelegateMsg<'static>>,
) -> ContractHandlerEvent {
    ContractHandlerEvent::DelegateRequest {
        req: DelegateRequest::ApplicationMessages {
            key: key.clone(),
            params: Parameters::from(Vec::new()),
            inbound,
        },
        origin_contract: None,
        connection_scope: crate::client_events::ConnectionScope::Local,
        user_context: None,
        lane_ticket: Default::default(),
    }
}

struct Loop {
    unregistered: super::executor::mock_wasm_runtime::UnregisteredDelegates,
    send: Arc<handler::ContractHandlerChannel<handler::SenderHalve>>,
    caps: Arc<DelegateCapabilities>,
    script: super::executor::mock_wasm_runtime::DelegateScript,
    observations: super::executor::mock_wasm_runtime::DelegateObservations,
    handle: tokio::task::JoinHandle<Result<(), ContractError>>,
}

impl Drop for Loop {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl Loop {
    /// Kinds seen by each delegate entry, in order.
    fn lifecycle_runs(&self) -> Vec<(DelegateKey, Vec<u8>)> {
        self.observations
            .lock()
            .unwrap()
            .iter()
            .filter(|o| o.inbound_kinds == vec!["Lifecycle"])
            .map(|o| (o.delegate_key.clone(), o.params.clone()))
            .collect()
    }

    /// Wake-up runs, in order, as (delegate, tag, params).
    fn wakeup_runs(&self) -> Vec<(DelegateKey, Vec<u8>)> {
        self.observations
            .lock()
            .unwrap()
            .iter()
            .filter(|o| o.inbound_kinds == vec!["WakeupFired"])
            .map(|o| (o.delegate_key.clone(), o.params.clone()))
            .collect()
    }

    /// Round-trip an event through the loop, so everything queued before it
    /// has had at least one loop iteration.
    async fn sync(&self) {
        drop(
            self.send
                .send_to_handler(ContractHandlerEvent::GetQuery {
                    instance_id: ContractInstanceId::new([0xEE; 32]),
                    return_contract_code: false,
                })
                .await,
        );
    }
}

async fn start<P: UserInputPrompter + 'static>(
    name: &str,
    caps: Arc<DelegateCapabilities>,
    script: Vec<ScriptedRun>,
    prompter: P,
) -> Loop {
    start_with_codes(name, caps, script, prompter, Vec::new()).await
}

/// [`start`], with delegate code the "node" already stores.
async fn start_with_codes<P: UserInputPrompter + 'static>(
    name: &str,
    caps: Arc<DelegateCapabilities>,
    script: Vec<ScriptedRun>,
    prompter: P,
    codes: Vec<(DelegateKey, Vec<u8>)>,
) -> Loop {
    let (send, rcv, _) = handler::contract_handler_channel();
    let mut handler = MockWasmContractHandler::new_test(rcv, None, name).await;
    let rt = handler.runtime_mut();
    rt.capabilities = Some(caps.clone());
    rt.delegate_codes.extend(codes);
    let script_handle = rt.delegate_script.clone();
    script_handle.lock().unwrap().extend(script);
    let observations = rt.delegate_observations.clone();
    let unregistered = rt.unregistered_delegates.clone();
    let handle = GlobalExecutor::spawn(contract_handling(handler, prompter));
    let lp = Loop {
        unregistered,
        send: Arc::new(send),
        caps,
        script: script_handle,
        observations,
        handle,
    };
    // Let the loop start (and seed NodeStarted from what is granted NOW)
    // before the test grants anything, so the seed never races the test.
    lp.sync().await;
    lp
}

async fn wait_until(label: &str, mut cond: impl FnMut() -> bool) {
    for _ in 0..2000 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("timed out waiting for: {label}");
}

#[allow(clippy::wildcard_enum_match_arm)]
fn answer(event: ContractHandlerEvent) -> Result<Vec<OutboundDelegateMsg>, String> {
    match event {
        ContractHandlerEvent::DelegateResponse(r) => r.map_err(|e| e.to_string()),
        other => panic!("expected a DelegateResponse, got {other}"),
    }
}

/// Every inbound variant except `ApplicationMessage`, as a client might forge
/// it. Each reports something only the node can know (a contract's state, the
/// answer to a contract request, a prompt answer, another delegate's message,
/// a lifecycle event, a wake-up), and none carries an origin the delegate
/// could check.
pub(crate) fn forged_node_only_messages(key: &DelegateKey) -> Vec<InboundDelegateMsg<'static>> {
    let contract_id = ContractInstanceId::new([9; 32]);
    let state = WrappedState::new(b"a state the contract never had".to_vec());
    vec![
        InboundDelegateMsg::ContractNotification(ContractNotification {
            contract_id,
            new_state: state.clone(),
            context: DelegateContext::default(),
        }),
        InboundDelegateMsg::GetContractResponse(GetContractResponse {
            contract_id,
            state: Some(state),
            context: DelegateContext::default(),
        }),
        InboundDelegateMsg::PutContractResponse(PutContractResponse {
            contract_id,
            result: Ok(()),
            context: DelegateContext::default(),
        }),
        InboundDelegateMsg::UpdateContractResponse(UpdateContractResponse {
            contract_id,
            result: Ok(()),
            context: DelegateContext::default(),
        }),
        InboundDelegateMsg::SubscribeContractResponse(SubscribeContractResponse {
            contract_id,
            result: Ok(()),
            context: DelegateContext::default(),
        }),
        InboundDelegateMsg::UnsubscribeContractResponse(UnsubscribeContractResponse {
            contract_id,
            result: Ok(()),
            context: DelegateContext::default(),
        }),
        InboundDelegateMsg::UserResponse(UserInputResponse {
            request_id: 1,
            response: ClientResponse::new(b"allow".to_vec()),
            context: DelegateContext::default(),
        }),
        InboundDelegateMsg::DelegateMessage(DelegateMessage::new(
            key.clone(),
            DelegateKey::new([4; 32], CodeHash::new([4; 32])),
            b"from another delegate".to_vec(),
        )),
        InboundDelegateMsg::Lifecycle(LifecycleEvent::Installed),
        InboundDelegateMsg::WakeupFired { tag: vec![1] },
    ]
}

/// The allowlist itself: every node-only variant is refused, alone and behind
/// an ordinary message, and an ordinary message alone is not.
///
/// The reported kind must be the variant's own name, taken from its derived
/// `Debug`, so an explicit arm that was deleted (and now falls into the
/// `_ => "an unrecognised inbound message"` wildcard) or that returns the
/// wrong name fails here, not only in the log.
#[test]
fn node_only_inbound_refuses_every_variant_but_application_message() {
    let key = DelegateKey::new([3; 32], CodeHash::new([3; 32]));
    let req = |inbound| DelegateRequest::ApplicationMessages {
        key: key.clone(),
        params: Parameters::from(Vec::new()),
        inbound,
    };
    let ordinary = || InboundDelegateMsg::ApplicationMessage(ApplicationMessage::new(vec![1]));
    for forged in forged_node_only_messages(&key) {
        let label = format!("{forged:?}");
        let variant = label
            .split(['(', ' ', '{'])
            .next()
            .expect("Debug output starts with the variant name")
            .to_string();
        assert_eq!(
            super::node_only_inbound(&req(vec![forged.clone()])),
            Some(variant.as_str()),
            "a client must not be able to send {label}"
        );
        assert_eq!(
            super::node_only_inbound(&req(vec![ordinary(), forged])),
            Some(variant.as_str()),
            "a forged message behind an ordinary one must be refused too: {label}"
        );
    }
    assert_eq!(super::node_only_inbound(&req(vec![ordinary()])), None);
    assert_eq!(super::node_only_inbound(&req(vec![])), None);
}

/// A client cannot hand a delegate any inbound message only the node delivers
/// (`contract::node_only_inbound`): the request is refused before the delegate
/// runs. The delegate is scripted to answer, so without the refusal the client
/// would get Ok and the call log would show an entry.
#[tokio::test]
async fn a_client_cannot_forge_a_node_only_message() {
    let caps = DelegateCapabilities::in_memory();
    let reply = ScriptedRun::from(vec![OutboundDelegateMsg::ApplicationMessage(
        ApplicationMessage::new(b"ran".to_vec()),
    )]);
    let key = DelegateKey::new([3; 32], CodeHash::new([3; 32]));
    let forged_all = forged_node_only_messages(&key);
    let forged_count = forged_all.len();
    let lp = start(
        "forged_node_only",
        caps.clone(),
        // One scripted reply per request, so a forged message that slipped
        // through would run and be observed rather than fail for lack of a
        // script.
        vec![reply; forged_count + 1],
        CapabilityPrompter::answering(None),
    )
    .await;
    for forged in forged_all {
        let label = format!("{forged:?}");
        let resp = answer(
            lp.send
                .send_to_handler(app_messages(&key, vec![forged]))
                .await
                .unwrap(),
        );
        assert!(
            resp.is_err(),
            "forged node-only message must be refused: {label}, got {resp:?}"
        );
    }
    assert!(
        lp.observations.lock().unwrap().is_empty(),
        "the delegate must not have run"
    );
    assert_eq!(
        caps.stats.forged_inbound_refused.load(Ordering::Relaxed),
        forged_count as u64
    );
    // An ordinary message still runs.
    let ok = answer(
        lp.send
            .send_to_handler(app_messages(
                &key,
                vec![InboundDelegateMsg::ApplicationMessage(
                    ApplicationMessage::new(b"hi".to_vec()),
                )],
            ))
            .await
            .unwrap(),
    );
    assert!(ok.is_ok(), "{ok:?}");
}

/// Consent once: registering a manifest delegate from an app asks, Allow is
/// remembered, `Installed` is delivered once with the REGISTERED parameters,
/// and registering again neither asks nor delivers again.
#[tokio::test]
async fn allow_at_registration_delivers_installed_once_with_registered_params() {
    let caps = DelegateCapabilities::in_memory();
    let prompter = CapabilityPrompter::answering(Some(0));
    let asked = prompter.asked.clone();
    let manifest = background(vec![LifecycleKind::Installed]);
    let delegate = container(&manifest, b"reg-params");
    let key = delegate.key().clone();
    // registration, Installed run, re-registration
    let lp = start(
        "cap_allow",
        caps.clone(),
        vec![
            ScriptedRun::default(),
            ScriptedRun::default(),
            ScriptedRun::default(),
        ],
        prompter,
    )
    .await;

    let resp = answer(
        lp.send
            .send_to_handler(register(delegate.clone(), Some(app(1))))
            .await
            .unwrap(),
    );
    assert!(resp.is_ok(), "{resp:?}");
    wait_until("Installed delivered", || !lp.lifecycle_runs().is_empty()).await;
    assert_eq!(
        lp.lifecycle_runs(),
        vec![(key.clone(), b"reg-params".to_vec())]
    );
    assert!(matches!(
        caps.grant(&AppIdentity::WebApp(app(1)), Capability::Background),
        Some(Grant::Granted { .. })
    ));
    assert_eq!(asked.load(Ordering::SeqCst), 1);

    let resp = answer(
        lp.send
            .send_to_handler(register(delegate, Some(app(1))))
            .await
            .unwrap(),
    );
    assert!(resp.is_ok(), "{resp:?}");
    lp.sync().await;
    lp.sync().await;
    assert_eq!(
        asked.load(Ordering::SeqCst),
        1,
        "a granted app is never asked again"
    );
    assert_eq!(lp.lifecycle_runs().len(), 1, "Installed is delivered once");
    assert_eq!(caps.stats.lifecycle_delivered.load(Ordering::Relaxed), 1);

    // Even if Installed is queued again (two apps granting at once, a
    // re-registration racing the first delivery), delivery re-checks the
    // flag and runs it at most once.
    for _ in 0..2 {
        assert!(caps.queue(LifecycleRun {
            key: key.clone(),
            event: LifecycleEvent::Installed.into(),
        }));
    }
    lp.sync().await;
    lp.sync().await;
    assert_eq!(
        lp.lifecycle_runs().len(),
        1,
        "a re-queued Installed is not re-run"
    );
}

/// "Not now" is remembered as a denial and nothing is delivered. A remote
/// registration, even claiming the same app, binds nothing and asks nobody.
#[tokio::test]
async fn not_now_delivers_nothing_and_a_remote_registration_binds_no_app() {
    let caps = DelegateCapabilities::in_memory();
    let prompter = CapabilityPrompter::answering(Some(1));
    let asked = prompter.asked.clone();
    let manifest = background(vec![LifecycleKind::Installed, LifecycleKind::NodeStarted]);
    let delegate = container(&manifest, b"p");
    let lp = start(
        "cap_deny",
        caps.clone(),
        vec![ScriptedRun::default(), ScriptedRun::default()],
        prompter,
    )
    .await;
    let resp = answer(
        lp.send
            .send_to_handler(register(delegate.clone(), Some(app(2))))
            .await
            .unwrap(),
    );
    assert!(resp.is_ok());
    wait_until("the prompt to be answered", || {
        caps.grant(&AppIdentity::WebApp(app(2)), Capability::Background)
            .is_some()
    })
    .await;
    assert!(matches!(
        caps.grant(&AppIdentity::WebApp(app(2)), Capability::Background),
        Some(Grant::Denied { .. })
    ));
    lp.sync().await;
    assert!(lp.lifecycle_runs().is_empty());

    // Remote scope: the app claim is ignored.
    let remote_delegate = container(&manifest, b"other");
    let resp = answer(
        lp.send
            .send_to_handler(ContractHandlerEvent::DelegateRequest {
                req: DelegateRequest::RegisterDelegate {
                    delegate: remote_delegate,
                    cipher: [0; 32],
                    nonce: [0; 24],
                },
                origin_contract: Some(app(3)),
                connection_scope: crate::client_events::ConnectionScope::Remote,
                user_context: None,
                lane_ticket: Default::default(),
            })
            .await
            .unwrap(),
    );
    assert!(resp.is_ok(), "{resp:?}");
    lp.sync().await;
    assert_eq!(
        asked.load(Ordering::SeqCst),
        1,
        "a remote registration prompts nobody"
    );
    assert!(
        caps.grant(&AppIdentity::WebApp(app(3)), Capability::Background)
            .is_none()
    );
}

/// Registration answers the client at once while the capability prompt waits
/// on a human: the prompt runs off the loop. With the prompt awaited inline
/// the registration response would hang with the gate closed.
#[tokio::test]
async fn a_pending_capability_prompt_does_not_block_registration() {
    let caps = DelegateCapabilities::in_memory();
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let prompter = CapabilityPrompter {
        answer: Some(0),
        gate: Some(gate.clone()),
        asked: Arc::default(),
    };
    let asked = prompter.asked.clone();
    let manifest = background(vec![LifecycleKind::Installed]);
    let delegate = container(&manifest, b"p");
    let lp = start(
        "cap_prompt_off_loop",
        caps.clone(),
        vec![ScriptedRun::default(), ScriptedRun::default()],
        prompter,
    )
    .await;
    let resp = tokio::time::timeout(
        Duration::from_secs(2),
        lp.send.send_to_handler(register(delegate, Some(app(4)))),
    )
    .await
    .expect("registration must not wait for the user's answer")
    .unwrap();
    assert!(answer(resp).is_ok());
    wait_until("the prompt to be raised", || {
        asked.load(Ordering::SeqCst) == 1
    })
    .await;
    assert!(lp.lifecycle_runs().is_empty(), "nothing before the answer");
    gate.add_permits(1);
    wait_until("Installed after Allow", || lp.lifecycle_runs().len() == 1).await;
}

/// `NodeStarted` goes, at loop start, to exactly the granted manifest
/// delegates that asked for it, with their registered parameters.
#[tokio::test(start_paused = true)]
async fn node_started_reaches_only_granted_delegates() {
    let caps = DelegateCapabilities::with_time_source(
        Arc::new(MemoryCapabilityStorage::default()),
        Arc::new(InstantTimeSrc::new()),
        BudgetLimits::default(),
    );
    let started_only = background(vec![LifecycleKind::NodeStarted]);
    let granted = container(&started_only, b"granted");
    let denied = container(&started_only, b"denied");
    let unattested = container(&started_only, b"unattested");
    let p = caps
        .on_registered(
            granted.key(),
            &manifest_module(&started_only),
            b"granted",
            Some(AppIdentity::WebApp(app(5))),
        )
        .unwrap();
    caps.record_answer(&p, true);
    let p = caps
        .on_registered(
            denied.key(),
            &manifest_module(&started_only),
            b"denied",
            Some(AppIdentity::WebApp(app(6))),
        )
        .unwrap();
    caps.record_answer(&p, false);
    assert!(
        caps.on_registered(
            unattested.key(),
            &manifest_module(&started_only),
            b"unattested",
            None
        )
        .is_none()
    );

    let lp = start(
        "cap_node_started",
        caps.clone(),
        vec![
            ScriptedRun::default(),
            ScriptedRun::default(),
            ScriptedRun::default(),
        ],
        CapabilityPrompter::answering(None),
    )
    .await;
    // Past the smear window (virtual time).
    tokio::time::sleep(
        super::delegate_capabilities::NODE_STARTED_MIN_DELAY
            + super::delegate_capabilities::NODE_STARTED_SMEAR
            + Duration::from_secs(1),
    )
    .await;
    lp.sync().await;
    assert_eq!(
        lp.lifecycle_runs(),
        vec![(granted.key().clone(), b"granted".to_vec())]
    );

    // A run that reaches the queue for an ungranted delegate (a grant revoked
    // after it was queued) is re-checked at delivery and not run.
    for key in [denied.key(), unattested.key()] {
        assert!(
            caps.queue(LifecycleRun {
                key: key.clone(),
                event: LifecycleEvent::NodeStarted {
                    down_since_ms: None
                }
                .into(),
            })
        );
    }
    lp.sync().await;
    lp.sync().await;
    assert_eq!(lp.lifecycle_runs().len(), 1);
    assert_eq!(
        caps.stats
            .lifecycle_dropped_not_granted
            .load(Ordering::Relaxed),
        2
    );
}

/// Unprompted runs are budgeted per delegate; client-driven runs are not.
#[tokio::test]
async fn unprompted_network_ops_are_refused_past_the_budget() {
    let caps = DelegateCapabilities::with_time_source(
        Arc::new(MemoryCapabilityStorage::default()),
        Arc::new(InstantTimeSrc::new()),
        BudgetLimits {
            ops_per_delegate_per_min: 1,
            ..BudgetLimits::default()
        },
    );
    let manifest = background(vec![LifecycleKind::NodeStarted]);
    let delegate = container(&manifest, b"p");
    let key = delegate.key().clone();

    let two_gets = || {
        ScriptedRun::from(vec![
            OutboundDelegateMsg::GetContractRequest(GetContractRequest::new(
                ContractInstanceId::new([0x51; 32]),
            )),
            OutboundDelegateMsg::GetContractRequest(GetContractRequest::new(
                ContractInstanceId::new([0x52; 32]),
            )),
        ])
    };
    // client run (2 GETs) + its follow-up, lifecycle run (2 GETs) + follow-up
    let lp = start(
        "cap_budget",
        caps.clone(),
        vec![
            two_gets(),
            ScriptedRun::default(),
            two_gets(),
            ScriptedRun::default(),
        ],
        CapabilityPrompter::answering(None),
    )
    .await;
    let p = caps
        .on_registered(
            &key,
            &manifest_module(&manifest),
            b"p",
            Some(AppIdentity::WebApp(app(7))),
        )
        .unwrap();
    caps.record_answer(&p, true);

    // Client-driven: not budgeted.
    let resp = answer(
        lp.send
            .send_to_handler(app_messages(
                &key,
                vec![InboundDelegateMsg::ApplicationMessage(
                    ApplicationMessage::new(b"go".to_vec()),
                )],
            ))
            .await
            .unwrap(),
    );
    assert!(resp.is_ok(), "{resp:?}");
    assert_eq!(caps.stats.refused_delegate_ops.load(Ordering::Relaxed), 0);

    // Unprompted: the second GET is refused.
    assert!(
        lp.caps.queue(LifecycleRun {
            key: key.clone(),
            event: LifecycleEvent::NodeStarted {
                down_since_ms: None
            }
            .into(),
        })
    );
    wait_until("the lifecycle run and its follow-up", || {
        lp.observations.lock().unwrap().len() == 4
    })
    .await;
    assert_eq!(caps.stats.refused_delegate_ops.load(Ordering::Relaxed), 1);
    let _ = &lp.script;
}

/// A delegate whose duty budget is spent does not get a lifecycle run; the
/// run is deferred, not dropped.
#[tokio::test]
async fn a_spent_duty_budget_defers_lifecycle_runs() {
    let caps = DelegateCapabilities::with_time_source(
        Arc::new(MemoryCapabilityStorage::default()),
        Arc::new(InstantTimeSrc::new()),
        BudgetLimits {
            duty_burst: Duration::from_millis(1),
            duty_refill_per_sec: Duration::from_micros(1),
            ..BudgetLimits::default()
        },
    );
    let manifest = background(vec![LifecycleKind::NodeStarted]);
    let delegate = container(&manifest, b"p");
    let key = delegate.key().clone();

    let lp = start(
        "cap_duty",
        caps.clone(),
        vec![ScriptedRun::default()],
        CapabilityPrompter::answering(None),
    )
    .await;
    let p = caps
        .on_registered(
            &key,
            &manifest_module(&manifest),
            b"p",
            Some(AppIdentity::WebApp(app(8))),
        )
        .unwrap();
    caps.record_answer(&p, true);
    caps.charge_duty(&key, Duration::from_secs(10), true);
    assert!(
        caps.queue(LifecycleRun {
            key,
            event: LifecycleEvent::NodeStarted {
                down_since_ms: None
            }
            .into(),
        })
    );
    wait_until("the deferral", || {
        caps.stats.lifecycle_deferred_duty.load(Ordering::Relaxed) == 1
    })
    .await;
    lp.sync().await;
    assert!(lp.lifecycle_runs().is_empty());
    let _ = user_input::AutoApprovePrompter;
}

/// `is_unprompted_run` classifies by `RunTrigger::Unprompted`, so the budget
/// is only as right as the set of callers passing it. Every production use of
/// `RunTrigger::Unprompted` must sit in a function that runs a delegate no
/// client asked for, and those functions must be exactly the known ones: a new
/// caller has to be classified here deliberately.
#[test]
fn unprompted_runs_are_exactly_the_unprompted_triggers() {
    let code = super::tests::production_code();
    let mut callers: Vec<String> = code
        .match_indices("RunTrigger::Unprompted")
        .map(|(idx, _)| {
            let before = &code[..idx];
            let start = [before.rfind("\nasync fn "), before.rfind("\nfn ")]
                .into_iter()
                .flatten()
                .max()
                .expect("every use sits inside a function");
            let sig = &code[start + 1..];
            let name_start = sig.find("fn ").expect("fn") + 3;
            let name_end = sig[name_start..]
                .find(|c: char| !(c.is_alphanumeric() || c == '_'))
                .expect("name end");
            sig[name_start..name_start + name_end].to_string()
        })
        .collect();
    callers.sort();
    callers.dedup();
    assert_eq!(
        callers,
        vec![
            "handle_delegate_notification".to_string(),
            "is_unprompted_run".to_string(),
            "run_lifecycle".to_string(),
            "run_queued_notification".to_string(),
        ],
        "a new RunTrigger::Unprompted caller must be classified as an unprompted run (or not) on purpose"
    );
}

/// A granted delegate with a Background manifest, recorded in `caps`.
fn granted(
    caps: &DelegateCapabilities,
    lifecycle: Vec<LifecycleKind>,
    params: &[u8],
    app_n: u8,
) -> DelegateKey {
    let manifest = background(lifecycle);
    let delegate = container(&manifest, params);
    let key = delegate.key().clone();
    let p = caps
        .on_registered(
            &key,
            &manifest_module(&manifest),
            params,
            Some(AppIdentity::WebApp(app(app_n))),
        )
        .expect("first registration asks");
    caps.record_answer(&p, true);
    key
}

/// The budget applies only to delegates that opted in. A delegate with no
/// capability record (River, and every delegate built before manifests) runs
/// its notification exactly as before, however many operations it emits.
#[tokio::test]
async fn the_budget_only_applies_to_opted_in_delegates() {
    let caps = DelegateCapabilities::with_time_source(
        Arc::new(MemoryCapabilityStorage::default()),
        Arc::new(InstantTimeSrc::new()),
        BudgetLimits {
            ops_per_delegate_per_min: 1,
            ..BudgetLimits::default()
        },
    );
    let opted_in = granted(&caps, vec![LifecycleKind::NodeStarted], b"p", 20);
    let legacy = DelegateKey::new([0x77; 32], CodeHash::new([0x77; 32]));
    let two_gets = || {
        ScriptedRun::from(vec![
            OutboundDelegateMsg::GetContractRequest(GetContractRequest::new(
                ContractInstanceId::new([0x61; 32]),
            )),
            OutboundDelegateMsg::GetContractRequest(GetContractRequest::new(
                ContractInstanceId::new([0x62; 32]),
            )),
        ])
    };
    let (_send, rcv, _) = handler::contract_handler_channel();
    let mut handler = MockWasmContractHandler::new_test(rcv, None, "cap_opt_in").await;
    let rt = handler.runtime_mut();
    rt.capabilities = Some(caps.clone());
    rt.delegate_script.lock().unwrap().extend([
        two_gets(),
        ScriptedRun::default(),
        two_gets(),
        ScriptedRun::default(),
    ]);
    let prompter = Arc::new(CapabilityPrompter::answering(None));
    for key in [&legacy, &opted_in] {
        super::handle_delegate_notification(
            &mut handler,
            super::executor::DelegateNotification {
                delegate_key: key.clone(),
                contract_id: ContractInstanceId::new([0x63; 32]),
                new_state: Arc::new(WrappedState::new(vec![1])),
            },
            &prompter,
            None,
        )
        .await;
        let refused = caps.stats.refused_delegate_ops.load(Ordering::Relaxed);
        if key == &legacy {
            assert_eq!(refused, 0, "a delegate without a manifest is not budgeted");
        } else {
            assert_eq!(refused, 1, "an opted-in delegate's notification run is");
        }
    }
}

/// Every refusal arm answers the delegate with its own response shape and is
/// counted: PUT, UPDATE and SUBSCRIBE, not only GET.
#[tokio::test]
async fn every_operation_kind_is_answered_when_refused() {
    let caps = DelegateCapabilities::with_time_source(
        Arc::new(MemoryCapabilityStorage::default()),
        Arc::new(InstantTimeSrc::new()),
        BudgetLimits {
            ops_per_delegate_per_min: 0,
            ..BudgetLimits::default()
        },
    );
    let contract = ContractContainer::Wasm(ContractWasmAPIVersion::V1(WrappedContract::new(
        Arc::new(ContractCode::from(b"budget-put".to_vec())),
        Parameters::from(vec![]),
    )));
    let target = ContractInstanceId::new([0x71; 32]);
    let ops = ScriptedRun::from(vec![
        OutboundDelegateMsg::PutContractRequest(PutContractRequest::new(
            contract,
            WrappedState::new(vec![1]),
            RelatedContracts::default(),
        )),
        OutboundDelegateMsg::UpdateContractRequest(UpdateContractRequest::new(
            target,
            UpdateData::State(State::from(vec![2])),
        )),
        OutboundDelegateMsg::SubscribeContractRequest(SubscribeContractRequest::new(target)),
    ]);
    let lp = start(
        "cap_refusal_arms",
        caps.clone(),
        vec![ops, ScriptedRun::default()],
        CapabilityPrompter::answering(None),
    )
    .await;
    let key = granted(&caps, vec![LifecycleKind::NodeStarted], b"p", 21);
    assert!(
        caps.queue(LifecycleRun {
            key,
            event: LifecycleEvent::NodeStarted {
                down_since_ms: None
            }
            .into(),
        })
    );
    wait_until("the follow-up run", || {
        lp.observations.lock().unwrap().len() == 2
    })
    .await;
    let follow_up = lp.observations.lock().unwrap()[1].inbound_kinds.clone();
    let mut kinds = follow_up.clone();
    kinds.sort();
    assert_eq!(
        kinds,
        vec![
            "PutContractResponse",
            "SubscribeContractResponse",
            "UpdateContractResponse"
        ],
        "{follow_up:?}"
    );
    assert_eq!(caps.stats.refused_delegate_ops.load(Ordering::Relaxed), 3);
}

/// A lifecycle run for a PARKED delegate is deferred, never run into the park
/// (the per-delegate exclusion of #5544).
#[tokio::test]
async fn a_parked_delegate_defers_its_lifecycle_run() {
    struct HangingDelegatePrompt;
    impl UserInputPrompter for HangingDelegatePrompt {
        async fn prompt(
            &self,
            _request: &UserInputRequest<'static>,
            _delegate_key: &str,
            _caller: CallerIdentity,
            _owner: Option<crate::wasm_runtime::UserId>,
        ) -> Option<(usize, ClientResponse<'static>)> {
            std::future::pending().await
        }
    }
    let caps = DelegateCapabilities::in_memory();
    let message = NotificationMessage::try_from(&serde_json::json!({"message": "allow?"}))
        .expect("notification message");
    let prompt = ScriptedRun::from(vec![OutboundDelegateMsg::RequestUserInput(
        UserInputRequest {
            request_id: 1,
            message,
            responses: vec![ClientResponse::new(b"yes".to_vec())],
        },
    )]);
    let lp = start(
        "cap_parked",
        caps.clone(),
        vec![prompt],
        HangingDelegatePrompt,
    )
    .await;
    let key = granted(&caps, vec![LifecycleKind::NodeStarted], b"p", 22);
    // Park the delegate: a client run that prompts and never gets an answer.
    let send = lp.send.clone();
    let parked_key = key.clone();
    let _client = tokio::spawn(async move {
        send.send_to_handler(app_messages(
            &parked_key,
            vec![InboundDelegateMsg::ApplicationMessage(
                ApplicationMessage::new(b"go".to_vec()),
            )],
        ))
        .await
    });
    wait_until("the delegate to park", || {
        lp.observations.lock().unwrap().len() == 1
    })
    .await;
    assert!(
        caps.queue(LifecycleRun {
            key,
            event: LifecycleEvent::NodeStarted {
                down_since_ms: None
            }
            .into(),
        })
    );
    wait_until("the deferral", || {
        caps.stats.lifecycle_deferred_parked.load(Ordering::Relaxed) == 1
    })
    .await;
    lp.sync().await;
    assert!(lp.lifecycle_runs().is_empty(), "never run into a park");
}

/// A run that cannot start is retried, then dropped after
/// `LIFECYCLE_MAX_ATTEMPTS`, and the drop is counted.
#[tokio::test(start_paused = true)]
async fn a_run_that_never_starts_is_dropped_and_counted() {
    let caps = DelegateCapabilities::with_time_source(
        Arc::new(MemoryCapabilityStorage::default()),
        Arc::new(InstantTimeSrc::new()),
        BudgetLimits {
            duty_burst: Duration::from_micros(1),
            duty_refill_per_sec: Duration::ZERO,
            ..BudgetLimits::default()
        },
    );
    let key = granted(&caps, vec![LifecycleKind::NodeStarted], b"p", 23);
    caps.charge_duty(&key, Duration::from_secs(1), true);
    let lp = start(
        "cap_dropped",
        caps.clone(),
        vec![],
        CapabilityPrompter::answering(None),
    )
    .await;
    assert!(
        caps.queue(LifecycleRun {
            key,
            event: LifecycleEvent::NodeStarted {
                down_since_ms: None
            }
            .into(),
        })
    );
    // The start-up seed already holds this run (the queued copy is a
    // duplicate), so allow for its smear as well as every retry.
    tokio::time::sleep(
        super::delegate_capabilities::NODE_STARTED_MIN_DELAY
            + super::delegate_capabilities::NODE_STARTED_SMEAR
            + super::delegate_capabilities::LIFECYCLE_RETRY_DELAY
                * (super::delegate_capabilities::LIFECYCLE_MAX_ATTEMPTS + 2),
    )
    .await;
    lp.sync().await;
    assert_eq!(
        caps.stats
            .lifecycle_dropped_attempts
            .load(Ordering::Relaxed),
        1
    );
    assert_eq!(
        caps.stats.lifecycle_deferred_duty.load(Ordering::Relaxed),
        u64::from(super::delegate_capabilities::LIFECYCLE_MAX_ATTEMPTS)
    );
    assert!(lp.lifecycle_runs().is_empty());
}

/// Only the bound app's own local unregister drops a delegate's capability
/// record; a remote one, a hosted user's, another app's, or one with no app
/// does not.
///
/// FALSIFY by dropping `!hosted_user` from `capability_hook`: the hosted
/// user's unregister drops the record.
#[tokio::test]
async fn a_remote_unregister_keeps_the_record() {
    let caps = DelegateCapabilities::in_memory();
    let key = granted(&caps, vec![LifecycleKind::NodeStarted], b"p", 24);
    let lp = start(
        "cap_unregister",
        caps.clone(),
        vec![ScriptedRun::default(); 5],
        CapabilityPrompter::answering(None),
    )
    .await;
    let unregister = |scope, origin| ContractHandlerEvent::DelegateRequest {
        req: DelegateRequest::UnregisterDelegate(key.clone()),
        origin_contract: origin,
        connection_scope: scope,
        user_context: None,
        lane_ticket: Default::default(),
    };
    let remote = answer(
        lp.send
            .send_to_handler(unregister(
                crate::client_events::ConnectionScope::Remote,
                Some(app(24)),
            ))
            .await
            .unwrap(),
    );
    assert!(remote.is_ok(), "{remote:?}");
    for (scope, origin) in [
        (crate::client_events::ConnectionScope::Local, Some(app(99))),
        (crate::client_events::ConnectionScope::Local, None),
    ] {
        let other = answer(
            lp.send
                .send_to_handler(unregister(scope, origin))
                .await
                .unwrap(),
        );
        assert!(other.is_ok(), "{other:?}");
    }
    assert!(
        caps.is_budgeted(&key),
        "a remote unregister must not drop the record"
    );
    // A hosted user's unregister removes only that user's own state, so even
    // from a local connection and the bound app it must not drop the record.
    let hosted_user = answer(
        lp.send
            .send_to_handler(ContractHandlerEvent::DelegateRequest {
                req: DelegateRequest::UnregisterDelegate(key.clone()),
                origin_contract: Some(app(24)),
                connection_scope: crate::client_events::ConnectionScope::Local,
                user_context: Some(crate::wasm_runtime::UserSecretContext::from_token(
                    b"capability-record-hosted-user",
                )),
                lane_ticket: Default::default(),
            })
            .await
            .unwrap(),
    );
    assert!(hosted_user.is_ok(), "{hosted_user:?}");
    assert!(
        caps.is_budgeted(&key),
        "a hosted user's unregister must not drop the shared record"
    );
    let local = answer(
        lp.send
            .send_to_handler(unregister(
                crate::client_events::ConnectionScope::Local,
                Some(app(24)),
            ))
            .await
            .unwrap(),
    );
    assert!(local.is_ok(), "{local:?}");
    assert!(!caps.is_budgeted(&key), "a local unregister drops it");
}

/// A lifecycle run for a delegate this node cannot load (removed by the CLI
/// or another connection, or a module that failed to read) is counted apart
/// from real failures, and the record is kept: "missing" is not proof the
/// delegate is gone.
#[tokio::test]
async fn a_lifecycle_run_for_a_missing_delegate_is_counted_and_kept() {
    let caps = DelegateCapabilities::in_memory();
    let lp = start(
        "cap_vanished",
        caps.clone(),
        vec![],
        CapabilityPrompter::answering(None),
    )
    .await;
    let key = granted(&caps, vec![LifecycleKind::NodeStarted], b"p", 25);
    lp.unregistered.lock().unwrap().insert(key.clone());
    assert!(
        caps.queue(LifecycleRun {
            key: key.clone(),
            event: LifecycleEvent::NodeStarted {
                down_since_ms: None
            }
            .into(),
        })
    );
    wait_until("the skip", || {
        caps.stats.lifecycle_skipped_missing.load(Ordering::Relaxed) == 1
    })
    .await;
    assert_eq!(caps.stats.lifecycle_failed.load(Ordering::Relaxed), 0);
    assert_eq!(caps.node_started_targets(), vec![key], "the record is kept");
}

/// A granted delegate whose manifest declares wake-ups (and no lifecycle
/// kinds, so every run the test sees is a wake-up).
fn granted_with_wakeups(
    caps: &DelegateCapabilities,
    wakeups: &[(&str, u64)],
    params: &[u8],
    app_n: u8,
) -> DelegateKey {
    let mut manifest = DelegateManifest::new(vec![], vec![Capability::Background]);
    for (tag, secs) in wakeups {
        manifest = manifest.with_wakeup(*tag, *secs);
    }
    let delegate = container(&manifest, params);
    let key = delegate.key().clone();
    let p = caps
        .on_registered(
            &key,
            &manifest_module(&manifest),
            params,
            Some(AppIdentity::WebApp(app(app_n))),
        )
        .expect("first registration asks");
    caps.record_answer(&p, true);
    key
}

fn paused_caps(limits: BudgetLimits) -> Arc<DelegateCapabilities> {
    DelegateCapabilities::with_time_source(
        Arc::new(MemoryCapabilityStorage::default()),
        Arc::new(InstantTimeSrc::new()),
        limits,
    )
}

/// The feature: with NO client connected, a granted delegate's declared
/// wake-up fires, first within the start-up window and then once per
/// interval (plus at most 10% jitter), each time with its REGISTERED
/// parameters, and the delegate is re-entered with `WakeupFired`.
#[tokio::test(start_paused = true)]
async fn a_wakeup_fires_on_schedule_with_no_client() {
    use super::delegate_capabilities::{NODE_STARTED_MIN_DELAY, NODE_STARTED_SMEAR};
    let caps = paused_caps(BudgetLimits::default());
    // Armed at "node start": the grant exists before the loop starts, so the
    // start-up seed arms it (the queued arm from the grant is a duplicate).
    let key = granted_with_wakeups(&caps, &[("hb", 60)], b"reg", 40);
    let lp = start(
        "wake_schedule",
        caps.clone(),
        vec![ScriptedRun::default(); 64],
        CapabilityPrompter::answering(None),
    )
    .await;

    // Not before the start-up floor.
    tokio::time::sleep(NODE_STARTED_MIN_DELAY - Duration::from_millis(100)).await;
    lp.sync().await;
    assert!(
        lp.wakeup_runs().is_empty(),
        "no fire before the start-up floor"
    );

    // First fire inside [floor, floor + min(interval, smear)).
    tokio::time::sleep(
        Duration::from_secs(60).min(NODE_STARTED_SMEAR) + Duration::from_millis(200),
    )
    .await;
    lp.sync().await;
    assert_eq!(lp.wakeup_runs(), vec![(key.clone(), b"reg".to_vec())]);

    // Then one per 60..66 s: over 660 s, 10 or 11 more.
    tokio::time::sleep(Duration::from_secs(660)).await;
    lp.sync().await;
    let runs = lp.wakeup_runs();
    assert!(
        (11..=12).contains(&runs.len()),
        "expected 11-12 fires by now, got {}",
        runs.len()
    );
    assert!(runs.iter().all(|r| *r == (key.clone(), b"reg".to_vec())));
    assert_eq!(
        caps.stats.wakeups_delivered.load(Ordering::Relaxed),
        runs.len() as u64
    );
    // Never more often than the interval: the storm bound.
    assert!(runs.len() as u64 <= 1 + 660 / 60);
}

/// A delegate asking for less than the floor gets the floor: a storm is not
/// possible whatever the manifest says.
#[tokio::test(start_paused = true)]
async fn a_sub_floor_interval_fires_no_faster_than_the_floor() {
    let caps = paused_caps(BudgetLimits::default());
    // A hand-written manifest can say 1 s; the macro would refuse it.
    let _key = granted_with_wakeups(&caps, &[("spin", 1)], b"", 41);
    let lp = start(
        "wake_floor",
        caps.clone(),
        vec![ScriptedRun::default(); 64],
        CapabilityPrompter::answering(None),
    )
    .await;
    tokio::time::sleep(Duration::from_secs(65 + 600)).await;
    lp.sync().await;
    let n = lp.wakeup_runs().len() as u64;
    let floor = freenet_stdlib::prelude::MIN_WAKEUP_INTERVAL_SECS;
    assert!(n >= 1, "it still fires");
    assert!(
        n <= 1 + 600 / floor,
        "{n} fires in 665 s: faster than the {floor} s floor"
    );
}

/// Revocation takes effect at the next fire and ends the schedule; a new
/// grant re-arms it.
#[tokio::test(start_paused = true)]
async fn revoking_stops_wakeups_and_a_new_grant_rearms_them() {
    // Fixed jitter draws: the counts below assume the first fire lands inside
    // 66 s and the next ones at least 60 s apart, true for any draw but the
    // exact-count assertions are easier to trust with a fixed one.
    let _seed = crate::config::GlobalRng::seed_guard(0x5eed_0001);
    let caps = paused_caps(BudgetLimits::default());
    let key = granted_with_wakeups(&caps, &[("hb", 60)], b"p", 42);
    let lp = start(
        "wake_revoke",
        caps.clone(),
        vec![ScriptedRun::default(); 64],
        CapabilityPrompter::answering(None),
    )
    .await;
    tokio::time::sleep(Duration::from_secs(66)).await;
    lp.sync().await;
    assert_eq!(lp.wakeup_runs().len(), 1);

    assert!(caps.revoke(&AppIdentity::WebApp(app(42)), Capability::Background));
    tokio::time::sleep(Duration::from_secs(600)).await;
    lp.sync().await;
    assert_eq!(lp.wakeup_runs().len(), 1, "no fire after revocation");
    assert_eq!(caps.stats.wakeups_stopped.load(Ordering::Relaxed), 1);

    // Granted again (the user re-allows at the app's next registration).
    caps.record_answer(
        &super::delegate_capabilities::CapabilityPrompt {
            app: AppIdentity::WebApp(app(42)),
            delegate: key.clone(),
            capabilities: vec![Capability::Background],
        },
        true,
    );
    tokio::time::sleep(Duration::from_secs(66)).await;
    lp.sync().await;
    assert_eq!(lp.wakeup_runs().len(), 2, "re-armed by the grant");
}

/// A delegate whose duty budget is spent does not fire: each fire is
/// deferred, then skipped after `WAKEUP_MAX_DEFERRALS`, and the SCHEDULE
/// GOES ON (it is not dropped the way a lifecycle run is), so it fires again
/// once the budget allows.
#[tokio::test(start_paused = true)]
async fn a_budget_starved_wakeup_is_skipped_but_the_schedule_continues() {
    use super::delegate_capabilities::WAKEUP_MAX_DEFERRALS;
    let caps = paused_caps(BudgetLimits {
        duty_burst: Duration::from_millis(10),
        // The debt below is floored at half a burst (5 ms); at 5 us/s it is
        // repaid after 1000 s.
        duty_refill_per_sec: Duration::from_micros(5),
        ..BudgetLimits::default()
    });
    let key = granted_with_wakeups(&caps, &[("hb", 60)], b"p", 43);
    // Deep in debt (floored at half a burst = 5 ms, so charge far more).
    caps.charge_duty(&key, Duration::from_secs(5), true);
    let lp = start(
        "wake_starved",
        caps.clone(),
        vec![ScriptedRun::default(); 64],
        CapabilityPrompter::answering(None),
    )
    .await;
    // Several intervals: every fire deferred, then skipped; none run. At
    // least 4 skips (one per <= 111 s: <= 66 s to the fire, 45 s of retries),
    // so the exact per-fire deferral count below cannot be matched by a
    // retry count one lower or one higher on any random draw.
    tokio::time::sleep(Duration::from_secs(66 + 5 * 112)).await;
    lp.sync().await;
    assert!(lp.wakeup_runs().is_empty(), "never runs over budget");
    let skipped = caps.stats.wakeups_skipped.load(Ordering::Relaxed);
    assert!(skipped >= 4, "each starved fire is skipped, got {skipped}");
    // Each skipped fire was deferred on its first attempt and on each of its
    // WAKEUP_MAX_DEFERRALS retries (one more fire may be part-way through).
    let deferred = caps.stats.wakeups_deferred.load(Ordering::Relaxed);
    let per_fire = u64::from(WAKEUP_MAX_DEFERRALS) + 1;
    assert!(
        deferred >= skipped * per_fire && deferred < (skipped + 1) * per_fire,
        "deferred {deferred}, skipped {skipped}"
    );
    // The debt is repaid (1000 s), and the SAME schedule fires again: it was
    // never dropped.
    tokio::time::sleep(Duration::from_secs(600)).await;
    lp.sync().await;
    assert!(
        !lp.wakeup_runs().is_empty(),
        "the schedule outlived the starved fires"
    );
}

/// Restart: wake-ups are not persisted, they are re-armed at node start from
/// the stored records. The "restarted node" is a second capability instance
/// over the same storage with an EMPTY queue, so only the start-up seed can
/// arm anything.
#[tokio::test(start_paused = true)]
async fn wakeups_are_rearmed_at_node_start() {
    let storage = Arc::new(MemoryCapabilityStorage::default());
    let before = DelegateCapabilities::with_time_source(
        storage.clone(),
        Arc::new(InstantTimeSrc::new()),
        BudgetLimits::default(),
    );
    let key = granted_with_wakeups(&before, &[("hb", 300)], b"p", 44);
    drop(before);

    let after = DelegateCapabilities::with_time_source(
        storage,
        Arc::new(InstantTimeSrc::new()),
        BudgetLimits::default(),
    );
    let lp = start(
        "wake_restart",
        after.clone(),
        vec![ScriptedRun::default(); 8],
        CapabilityPrompter::answering(None),
    )
    .await;
    tokio::time::sleep(Duration::from_secs(66)).await;
    lp.sync().await;
    assert_eq!(lp.wakeup_runs(), vec![(key, b"p".to_vec())]);
}

/// A delegate registered and granted on v0.2.138, whose record holds its
/// manifest re-serialized WITHOUT `wakeups`, gets its wake-ups on the first
/// start of this release: the start-up refresh re-reads the manifest from the
/// delegate's stored code. No re-registration involved.
#[tokio::test(start_paused = true)]
async fn a_record_written_without_wakeups_is_refreshed_from_the_code_at_start() {
    let storage = Arc::new(MemoryCapabilityStorage::default());
    let declared = DelegateManifest::new(
        vec![LifecycleKind::NodeStarted],
        vec![Capability::Background],
    )
    .with_wakeup("hb", 300);
    let full_code = manifest_module(&declared);
    let key = container(&declared, b"p").key().clone();
    // What v0.2.138 stored: the manifest minus the field it did not know.
    let as_stored = DelegateManifest::from_bytes(
        br#"{"manifest_version":1,"lifecycle":["node_started"],"capabilities":["background"]}"#,
    )
    .unwrap();
    {
        let old = DelegateCapabilities::with_time_source(
            storage.clone(),
            Arc::new(InstantTimeSrc::new()),
            BudgetLimits::default(),
        );
        let p = old
            .on_registered(
                &key,
                &manifest_module(&as_stored),
                b"p",
                Some(AppIdentity::WebApp(app(45))),
            )
            .unwrap();
        old.record_answer(&p, true);
        assert!(old.wakeup_start_targets().is_empty());
    }

    let caps = DelegateCapabilities::with_time_source(
        storage,
        Arc::new(InstantTimeSrc::new()),
        BudgetLimits::default(),
    );
    let lp = start_with_codes(
        "wake_refresh",
        caps.clone(),
        vec![ScriptedRun::default(); 8],
        CapabilityPrompter::answering(None),
        vec![(key.clone(), full_code)],
    )
    .await;
    tokio::time::sleep(Duration::from_secs(66)).await;
    lp.sync().await;
    assert_eq!(lp.wakeup_runs(), vec![(key, b"p".to_vec())]);
}

/// A wake-up run CAN write a contract (its UPDATE goes through the
/// unprompted-op admission, and a refusal is answered), and CAN reach another
/// delegate, which sees the waking delegate as the attested
/// `MessageOrigin::Delegate` (Ian, 2026-11-20: background runs may message
/// other delegates; River's chat delegate asks a key-vault delegate to sign
/// from a background run).
#[tokio::test(start_paused = true)]
async fn a_wakeup_run_can_update_a_contract_and_message_a_delegate() {
    let caps = paused_caps(BudgetLimits {
        ops_per_delegate_per_min: 1,
        ..BudgetLimits::default()
    });
    let key = granted_with_wakeups(&caps, &[("hb", 60)], b"p", 46);
    let other = DelegateKey::new([0x44; 32], CodeHash::new([0x44; 32]));
    let target = ContractInstanceId::new([0x72; 32]);
    let update = || {
        OutboundDelegateMsg::UpdateContractRequest(UpdateContractRequest::new(
            target,
            UpdateData::State(State::from(vec![7])),
        ))
    };
    let wake_run = ScriptedRun::from(vec![
        update(),
        update(),
        OutboundDelegateMsg::SendDelegateMessage(DelegateMessage::new(
            other.clone(),
            key.clone(),
            b"sign this".to_vec(),
        )),
    ]);
    // The wake-up run, the hop's target, the wake-up run's follow-up.
    let lp = start(
        "wake_ops",
        caps.clone(),
        vec![wake_run, ScriptedRun::default(), ScriptedRun::default()],
        CapabilityPrompter::answering(None),
    )
    .await;
    tokio::time::sleep(Duration::from_secs(66)).await;
    lp.sync().await;
    wait_until("the hop and the wake-up's follow-up run", || {
        lp.observations.lock().unwrap().len() >= 3
    })
    .await;
    lp.sync().await;
    let obs = lp.observations.lock().unwrap().clone();
    assert_eq!(obs[0].inbound_kinds, vec!["WakeupFired"]);
    assert!(
        obs[1..]
            .iter()
            .any(|o| o.delegate_key == key && o.inbound_kinds.contains(&"UpdateContractResponse")),
        "both UPDATEs are answered to the delegate: {obs:?}"
    );
    assert_eq!(
        caps.stats.refused_delegate_ops.load(Ordering::Relaxed),
        1,
        "the second UPDATE is past the per-minute allowance: refused, not dropped"
    );
    let reached: Vec<_> = obs.iter().filter(|o| o.delegate_key == other).collect();
    assert_eq!(
        reached.len(),
        1,
        "a wake-up run's message reaches the other delegate once: {obs:?}"
    );
    assert_eq!(reached[0].inbound_kinds, vec!["DelegateMessage"]);
    assert_eq!(
        reached[0].origin,
        Some(MessageOrigin::Delegate(key.clone())),
        "the target sees the waking delegate as its attested caller"
    );
}

/// A contract-NOTIFICATION run can message another delegate, and the target
/// sees the notified delegate as the attested `MessageOrigin::Delegate`. This
/// was the GHSA-824h-7x5x-wfmf suppression's path (removed on Ian's decision,
/// 2026-11-20): see "BACKGROUND RUNS MAY MESSAGE OTHER DELEGATES" in
/// `handle_delegate_with_contract_requests`.
#[tokio::test]
async fn a_notification_run_can_message_a_delegate() {
    let caps = DelegateCapabilities::in_memory();
    let key = granted(&caps, vec![LifecycleKind::NodeStarted], b"p", 53);
    let other = DelegateKey::new([0x45; 32], CodeHash::new([0x45; 32]));
    let (_send, rcv, _) = handler::contract_handler_channel();
    let mut handler = MockWasmContractHandler::new_test(rcv, None, "notify_hop").await;
    let rt = handler.runtime_mut();
    rt.capabilities = Some(caps.clone());
    rt.delegate_script.lock().unwrap().extend([
        // The notification run.
        ScriptedRun::from(vec![OutboundDelegateMsg::SendDelegateMessage(
            DelegateMessage::new(other.clone(), key.clone(), b"rotate".to_vec()),
        )]),
        // The hop's target.
        ScriptedRun::default(),
        // The notification run's follow-up.
        ScriptedRun::default(),
    ]);
    let observations = rt.delegate_observations.clone();
    let (resume_tx, _resume_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut park = super::delegate_park::DelegateParkCtx::new(resume_tx);
    let prompter = Arc::new(CapabilityPrompter::answering(None));

    super::handle_delegate_notification(
        &mut handler,
        super::executor::DelegateNotification {
            delegate_key: key.clone(),
            contract_id: ContractInstanceId::new([0x65; 32]),
            new_state: Arc::new(WrappedState::new(vec![1])),
        },
        &prompter,
        Some(&mut park),
    )
    .await;

    let obs = observations.lock().unwrap().clone();
    assert_eq!(
        obs.iter()
            .map(|o| (o.delegate_key.clone(), o.inbound_kinds.clone()))
            .collect::<Vec<_>>(),
        vec![
            (key.clone(), vec!["ContractNotification"]),
            (other.clone(), vec!["DelegateMessage"]),
            (key.clone(), vec![]),
        ],
        "notification run, then the hop's target, then the follow-up"
    );
    assert_eq!(
        obs[1].origin,
        Some(MessageOrigin::Delegate(key.clone())),
        "the target sees the notified delegate as its attested caller"
    );
}

/// A hop from a background run cannot launder network work out of the
/// unprompted budget, in either direction. The hop runs only the target's
/// `process()`; the target's own contract requests are not executed (not
/// admitted, not refused, not answered).
///
/// The allowance is 2 and the caller makes exactly 2 GETs, so `refused == 0`
/// depends on the hop: had the target's two GETs been admitted against the
/// caller's allowance, the target's second GET and the caller's second GET
/// would both be refused (2). Had they run outside the budget, the target
/// would be re-entered with `GetContractResponse`s, which the
/// one-observation assertion catches.
#[tokio::test(start_paused = true)]
async fn a_hop_from_a_background_run_does_no_unbudgeted_network_work() {
    let caps = paused_caps(BudgetLimits {
        ops_per_delegate_per_min: 2,
        ..BudgetLimits::default()
    });
    let key = granted_with_wakeups(&caps, &[("hb", 60)], b"p", 54);
    let other = DelegateKey::new([0x46; 32], CodeHash::new([0x46; 32]));
    let lp = start(
        "wake_hop_budget",
        caps.clone(),
        vec![
            // Wake-up run: one GET and a hop.
            ScriptedRun::from(vec![
                get(0x81),
                OutboundDelegateMsg::SendDelegateMessage(DelegateMessage::new(
                    other.clone(),
                    key.clone(),
                    b"go".to_vec(),
                )),
            ]),
            // The hop's target asks for network work of its own.
            ScriptedRun::from(vec![get(0x82), get(0x83)]),
            // The caller's follow-up: its second GET, the last within the
            // allowance.
            ScriptedRun::from(vec![get(0x84)]),
            // The caller's last follow-up.
            ScriptedRun::default(),
        ],
        CapabilityPrompter::answering(None),
    )
    .await;
    tokio::time::sleep(Duration::from_secs(66)).await;
    lp.sync().await;
    wait_until("the wake-up run's last leg", || {
        lp.observations.lock().unwrap().len() >= 4
    })
    .await;
    lp.sync().await;
    let obs = lp.observations.lock().unwrap().clone();
    assert_eq!(obs.len(), 4, "{obs:?}");
    let reached: Vec<_> = obs.iter().filter(|o| o.delegate_key == other).collect();
    assert_eq!(
        reached.len(),
        1,
        "the target runs once, for the message, and is never re-entered with \
         answers to its own requests: {obs:?}"
    );
    assert_eq!(reached[0].inbound_kinds, vec!["DelegateMessage"]);
    assert_eq!(
        reached[0].origin,
        Some(MessageOrigin::Delegate(key.clone()))
    );
    assert_eq!(
        caps.stats.refused_delegate_ops.load(Ordering::Relaxed),
        0,
        "the caller's two GETs fit its allowance of two: the target's GETs \
         were not charged to it"
    );
    assert_eq!(caps.stats.refused_node_ops.load(Ordering::Relaxed), 0);
}

/// The control that stays for CLIENT-driven runs: a hop carries the
/// originating connection's scope, so a request over a non-local connection
/// reaches the target with no attested origin, while the same request over a
/// local connection reaches it as `MessageOrigin::Delegate(caller)`.
#[tokio::test]
async fn a_remote_client_hop_reaches_the_target_unattested() {
    let caps = DelegateCapabilities::in_memory();
    let key = DelegateKey::new([0x47; 32], CodeHash::new([0x47; 32]));
    let other = DelegateKey::new([0x48; 32], CodeHash::new([0x48; 32]));
    let hop = || {
        ScriptedRun::from(vec![OutboundDelegateMsg::SendDelegateMessage(
            DelegateMessage::new(other.clone(), key.clone(), b"hi".to_vec()),
        )])
    };
    // Per request: the caller, the target, the caller's follow-up.
    let lp = start(
        "client_hop_scope",
        caps,
        vec![
            hop(),
            ScriptedRun::default(),
            ScriptedRun::default(),
            hop(),
            ScriptedRun::default(),
            ScriptedRun::default(),
        ],
        CapabilityPrompter::answering(None),
    )
    .await;
    for scope in [
        crate::client_events::ConnectionScope::Remote,
        crate::client_events::ConnectionScope::Local,
    ] {
        let resp = answer(
            lp.send
                .send_to_handler(ContractHandlerEvent::DelegateRequest {
                    req: DelegateRequest::ApplicationMessages {
                        key: key.clone(),
                        params: Parameters::from(Vec::new()),
                        inbound: vec![InboundDelegateMsg::ApplicationMessage(
                            ApplicationMessage::new(b"go".to_vec()),
                        )],
                    },
                    origin_contract: None,
                    connection_scope: scope,
                    user_context: None,
                    lane_ticket: Default::default(),
                })
                .await
                .unwrap(),
        );
        assert!(resp.is_ok(), "{scope:?}: {resp:?}");
    }
    let origins: Vec<_> = lp
        .observations
        .lock()
        .unwrap()
        .iter()
        .filter(|o| o.delegate_key == other)
        .map(|o| o.origin.clone())
        .collect();
    assert_eq!(
        origins,
        vec![None, Some(MessageOrigin::Delegate(key.clone()))],
        "remote client: no attested origin at the target; local client: the caller"
    );
}

/// The wake-up schedule is bounded: at `MAX_SCHEDULED_WAKEUPS` the entries
/// of delegates that are no longer eligible are swept before an arm is
/// refused, so churned registrations cannot grow it without bound, and a real
/// delegate still gets armed.
#[test]
fn a_full_wakeup_schedule_sweeps_stale_entries_before_refusing() {
    use super::delegate_capabilities::{LifecycleSchedule, MAX_SCHEDULED_WAKEUPS, RunEvent};
    let caps = DelegateCapabilities::in_memory();
    let live = granted_with_wakeups(&caps, &[("hb", 60)], b"p", 47);
    let mut schedule = LifecycleSchedule::default();
    let now = tokio::time::Instant::now();
    let wake = |key: DelegateKey| LifecycleRun {
        key,
        event: RunEvent::Wakeup {
            tag: b"hb".to_vec(),
            every: Duration::from_secs(60),
        },
    };
    for n in 0..MAX_SCHEDULED_WAKEUPS as u32 {
        let stale = DelegateKey::new(
            *blake3::hash(&n.to_le_bytes()).as_bytes(),
            CodeHash::new([9; 32]),
        );
        assert!(schedule.push(now, wake(stale), 0));
    }
    assert_eq!(schedule.wakeup_count(), MAX_SCHEDULED_WAKEUPS);
    super::schedule_queued_run(Some(&caps), &mut schedule, now, wake(live.clone()));
    assert_eq!(
        schedule.wakeup_count(),
        1,
        "stale entries swept, the live one armed"
    );
    assert_eq!(caps.stats.wakeups_schedule_full.load(Ordering::Relaxed), 0);

    // Nothing to sweep (no capability state to consult, so nothing is known
    // stale): the arm is refused and the schedule does not grow past the cap.
    let mut unsweepable = LifecycleSchedule::default();
    for n in 0..MAX_SCHEDULED_WAKEUPS as u32 {
        let k = DelegateKey::new(
            *blake3::hash(&(n + 1_000_000).to_le_bytes()).as_bytes(),
            CodeHash::new([8; 32]),
        );
        unsweepable.push(now, wake(k), 0);
    }
    super::schedule_queued_run(None, &mut unsweepable, now, wake(live.clone()));
    assert_eq!(
        unsweepable.wakeup_count(),
        MAX_SCHEDULED_WAKEUPS,
        "not grown past the cap"
    );
}

/// #5748: a background run that PARKS (any off-loop contract fetch or network
/// op does, not only a prompt) is charged for its resumed leg too, to the same
/// buckets as its first leg. Real clock with no refill, so the balances move
/// only by charges; the delegate is held parked (a gated prompt) so the test
/// can read the balance between the two legs.
#[tokio::test]
async fn the_resumed_leg_of_a_parked_background_run_is_charged() {
    struct GatedPrompt(Arc<tokio::sync::Semaphore>);
    impl UserInputPrompter for GatedPrompt {
        async fn prompt(
            &self,
            request: &UserInputRequest<'static>,
            _delegate_key: &str,
            _caller: CallerIdentity,
            _owner: Option<crate::wasm_runtime::UserId>,
        ) -> Option<(usize, ClientResponse<'static>)> {
            self.0.acquire().await.expect("gate").forget();
            request
                .responses
                .first()
                .map(|r| (0, r.clone().into_owned()))
        }
    }
    let caps = DelegateCapabilities::with_time_source(
        Arc::new(MemoryCapabilityStorage::default()),
        Arc::new(InstantTimeSrc::new()),
        BudgetLimits {
            duty_refill_per_sec: Duration::ZERO,
            node_duty_refill_per_sec: Duration::ZERO,
            ..BudgetLimits::default()
        },
    );
    let message = NotificationMessage::try_from(&serde_json::json!({"message": "ok?"}))
        .expect("notification message");
    let prompt = ScriptedRun::from(vec![OutboundDelegateMsg::RequestUserInput(
        UserInputRequest {
            request_id: 1,
            message,
            responses: vec![ClientResponse::new(b"yes".to_vec())],
        },
    )]);
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let lp = start(
        "resume_charged",
        caps.clone(),
        vec![prompt, ScriptedRun::default()],
        GatedPrompt(gate.clone()),
    )
    .await;
    // Granted AFTER the loop started, so the start-up seed holds nothing for it
    // and the run below is due at once (not a duplicate of a smeared seed).
    let key = granted(&caps, vec![LifecycleKind::NodeStarted], b"p", 48);
    assert!(
        caps.queue(LifecycleRun {
            key: key.clone(),
            event: LifecycleEvent::NodeStarted {
                down_since_ms: None
            }
            .into(),
        })
    );
    wait_until("the run to park", || {
        caps.stats.lifecycle_delivered.load(Ordering::Relaxed) == 1
    })
    .await;
    let (delegate_parked, node_parked) = caps.duty_balances_us(&key);
    let delegate_parked = delegate_parked.expect("first leg charged");

    gate.add_permits(1);
    wait_until("the resumed leg", || {
        lp.observations.lock().unwrap().len() == 2
    })
    .await;
    // The resumed leg's charge lands just after it returns.
    wait_until("the resumed leg's charge", || {
        caps.duty_balances_us(&key).0.expect("tracked") < delegate_parked
    })
    .await;
    assert!(
        caps.duty_balances_us(&key).1 < node_parked,
        "a lifecycle/wake-up run's resumed leg is charged node-wide too"
    );
}

/// A routine re-arm of an ALREADY scheduled wake-up does not trip the cap:
/// no sweep, no "schedule full", just counted as already armed.
#[test]
fn rearming_a_scheduled_wakeup_at_the_cap_is_not_a_refusal() {
    use super::delegate_capabilities::{LifecycleSchedule, MAX_SCHEDULED_WAKEUPS, RunEvent};
    let caps = DelegateCapabilities::in_memory();
    let live = granted_with_wakeups(&caps, &[("hb", 60)], b"p", 49);
    let now = tokio::time::Instant::now();
    let wake = |key: DelegateKey| LifecycleRun {
        key,
        event: RunEvent::Wakeup {
            tag: b"hb".to_vec(),
            every: Duration::from_secs(60),
        },
    };
    let mut schedule = LifecycleSchedule::default();
    assert!(schedule.push(now, wake(live.clone()), 0));
    for n in 1..MAX_SCHEDULED_WAKEUPS as u32 {
        let stale = DelegateKey::new(
            *blake3::hash(&n.to_le_bytes()).as_bytes(),
            CodeHash::new([7; 32]),
        );
        schedule.push(now, wake(stale), 0);
    }
    super::schedule_queued_run(Some(&caps), &mut schedule, now, wake(live));
    assert_eq!(
        schedule.wakeup_count(),
        MAX_SCHEDULED_WAKEUPS,
        "no sweep for a duplicate"
    );
    assert_eq!(caps.stats.wakeups_already_armed.load(Ordering::Relaxed), 1);
    assert_eq!(caps.stats.wakeups_schedule_full.load(Ordering::Relaxed), 0);
}

/// A storage error while checking a fire skips THAT fire and keeps the
/// schedule: once the store reads again, the delegate is woken again.
#[tokio::test(start_paused = true)]
async fn a_storage_error_at_fire_time_skips_the_fire_but_keeps_the_schedule() {
    // The first fire lands at a random point in [5 s, 65 s) after start-up, so
    // with an unseeded RNG a second fire falls inside the 66 s window for
    // about one draw in sixty and the exact count below fails (#6365). Pin the
    // draws, as `revoking_stops_wakeups_and_a_new_grant_rearms_them` does.
    let _seed = crate::config::GlobalRng::seed_guard(0x5eed_0001);
    let storage = Arc::new(super::delegate_capabilities::FlakyCapabilityStorage::default());
    let caps = DelegateCapabilities::with_time_source(
        storage.clone(),
        Arc::new(InstantTimeSrc::new()),
        BudgetLimits::default(),
    );
    let key = granted_with_wakeups(&caps, &[("hb", 60)], b"p", 50);
    let lp = start(
        "wake_storage_error",
        caps.clone(),
        vec![ScriptedRun::default(); 8],
        CapabilityPrompter::answering(None),
    )
    .await;
    storage.fail_records.store(true, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_secs(66)).await;
    lp.sync().await;
    assert!(lp.wakeup_runs().is_empty());
    assert_eq!(caps.stats.wakeups_storage_error.load(Ordering::Relaxed), 1);
    assert_eq!(caps.stats.wakeups_stopped.load(Ordering::Relaxed), 0);

    storage.fail_records.store(false, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_secs(70)).await;
    lp.sync().await;
    // How many fires fell in the window depends on the jitter draws; what
    // matters is that there is at least one and each is this delegate's.
    let runs = lp.wakeup_runs();
    assert!(
        !runs.is_empty(),
        "the schedule survived the unreadable fire"
    );
    assert!(runs.iter().all(|r| *r == (key.clone(), b"p".to_vec())));
}

fn user_input_run() -> ScriptedRun {
    let message = NotificationMessage::try_from(&serde_json::json!({"message": "ok?"}))
        .expect("notification message");
    ScriptedRun::from(vec![OutboundDelegateMsg::RequestUserInput(
        UserInputRequest {
            request_id: 1,
            message,
            responses: vec![ClientResponse::new(b"yes".to_vec())],
        },
    )])
}

fn zero_refill_caps() -> Arc<DelegateCapabilities> {
    DelegateCapabilities::with_time_source(
        Arc::new(MemoryCapabilityStorage::default()),
        Arc::new(InstantTimeSrc::new()),
        BudgetLimits {
            duty_refill_per_sec: Duration::ZERO,
            node_duty_refill_per_sec: Duration::ZERO,
            ..BudgetLimits::default()
        },
    )
}

/// The other side of #5748's fix: a CLIENT-driven run is never budgeted, so
/// neither of its legs is charged, even for a delegate that is budgeted for
/// its background runs.
#[tokio::test]
async fn a_parked_client_run_is_not_charged_on_resume() {
    let caps = zero_refill_caps();
    let lp = start(
        "resume_client_uncharged",
        caps.clone(),
        vec![user_input_run(), ScriptedRun::default()],
        CapabilityPrompter::answering(None),
    )
    .await;
    let key = granted(&caps, vec![LifecycleKind::NodeStarted], b"p", 51);
    let before = caps.duty_balances_us(&key);
    let resp = answer(
        lp.send
            .send_to_handler(app_messages(
                &key,
                vec![InboundDelegateMsg::ApplicationMessage(
                    ApplicationMessage::new(b"go".to_vec()),
                )],
            ))
            .await
            .unwrap(),
    );
    assert!(resp.is_ok(), "{resp:?}");
    assert_eq!(
        lp.observations.lock().unwrap().len(),
        2,
        "the client run parked and resumed"
    );
    assert_eq!(
        caps.duty_balances_us(&key),
        before,
        "a client run is never charged"
    );
}

/// A contract-NOTIFICATION run that parks is charged for its resumed leg to
/// its delegate only, as its first leg is (#5730): never to the node bucket,
/// so busy notification traffic cannot starve lifecycle and wake-up runs.
#[tokio::test]
async fn a_parked_notification_run_is_charged_to_its_delegate_only() {
    let caps = zero_refill_caps();
    let key = granted(&caps, vec![LifecycleKind::NodeStarted], b"p", 52);
    let (_send, rcv, _) = handler::contract_handler_channel();
    let mut handler = MockWasmContractHandler::new_test(rcv, None, "resume_notification").await;
    let rt = handler.runtime_mut();
    rt.capabilities = Some(caps.clone());
    rt.delegate_script
        .lock()
        .unwrap()
        .extend([user_input_run(), ScriptedRun::default()]);
    let observations = rt.delegate_observations.clone();
    let (resume_tx, mut resume_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut park = super::delegate_park::DelegateParkCtx::new(resume_tx);
    let prompter = Arc::new(CapabilityPrompter::answering(None));

    super::handle_delegate_notification(
        &mut handler,
        super::executor::DelegateNotification {
            delegate_key: key.clone(),
            contract_id: ContractInstanceId::new([0x64; 32]),
            new_state: Arc::new(WrappedState::new(vec![1])),
        },
        &prompter,
        Some(&mut park),
    )
    .await;
    assert!(park.is_parked(&key), "the notification run parked");
    let (delegate_parked, node_parked) = caps.duty_balances_us(&key);
    let delegate_parked = delegate_parked.expect("first leg charged to the delegate");

    let resume = tokio::time::timeout(Duration::from_secs(10), resume_rx.recv())
        .await
        .expect("the prompt is answered")
        .expect("resume");
    super::handle_delegate_resume(&mut handler, &mut park, &prompter, resume).await;
    assert_eq!(observations.lock().unwrap().len(), 2, "resumed");
    let (delegate_after, node_after) = caps.duty_balances_us(&key);
    assert!(
        delegate_after.expect("tracked") < delegate_parked,
        "the resumed leg is charged to the delegate"
    );
    assert_eq!(node_after, node_parked, "and never to the node bucket");
}

fn get(n: u8) -> OutboundDelegateMsg {
    OutboundDelegateMsg::GetContractRequest(GetContractRequest::new(ContractInstanceId::new(
        [n; 32],
    )))
}

fn send(to: &DelegateKey, from: &DelegateKey) -> OutboundDelegateMsg {
    OutboundDelegateMsg::SendDelegateMessage(DelegateMessage::new(
        to.clone(),
        from.clone(),
        b"msg".to_vec(),
    ))
}

fn app_out(bytes: &[u8]) -> OutboundDelegateMsg {
    OutboundDelegateMsg::ApplicationMessage(ApplicationMessage::new(bytes.to_vec()))
}

/// Each message as a comparable label: an application message by its
/// payload, anything else by its variant.
fn labels(msgs: &[OutboundDelegateMsg]) -> Vec<String> {
    msgs.iter()
        .map(|m| {
            if let OutboundDelegateMsg::ApplicationMessage(a) = m {
                return format!("app:{}", String::from_utf8_lossy(&a.payload));
            }
            format!("{m:?}")
                .split(['(', ' ', '{'])
                .next()
                .unwrap_or_default()
                .to_string()
        })
        .collect()
}

/// A handler with no loop behind it, for driving one run directly.
async fn direct_handler(
    name: &str,
    script: Vec<ScriptedRun>,
) -> (
    MockWasmContractHandler,
    super::executor::mock_wasm_runtime::DelegateObservations,
    handler::ContractHandlerChannel<handler::SenderHalve>,
) {
    let (send, rcv, _) = handler::contract_handler_channel();
    let mut handler = MockWasmContractHandler::new_test(rcv, None, name).await;
    let rt = handler.runtime_mut();
    rt.delegate_script.lock().unwrap().extend(script);
    let observations = rt.delegate_observations.clone();
    (handler, observations, send)
}

/// Run `key` once, from an application message, with the given trigger and
/// scope, and return what the run would hand on: to the client for a
/// `Client` run, to `route_notification_outbound` (the caller's registered
/// apps) for an unprompted one.
async fn run_direct(
    handler: &mut MockWasmContractHandler,
    park: &mut super::delegate_park::DelegateParkCtx,
    key: &DelegateKey,
    trigger: super::RunTrigger,
    scope: crate::client_events::ConnectionScope,
) -> Vec<OutboundDelegateMsg> {
    run_direct_from(handler, park, key, trigger, scope, None).await
}

/// [`run_direct`], for a run a web app (`origin_contract`) started.
async fn run_direct_from(
    handler: &mut MockWasmContractHandler,
    park: &mut super::delegate_park::DelegateParkCtx,
    key: &DelegateKey,
    trigger: super::RunTrigger,
    scope: crate::client_events::ConnectionScope,
    origin_contract: Option<&ContractInstanceId>,
) -> Vec<OutboundDelegateMsg> {
    let prompter = Arc::new(CapabilityPrompter::answering(None));
    let mut no_responder = None;
    let delivery = if trigger == super::RunTrigger::Client {
        super::delegate_park::Delivery::Client
    } else {
        super::delegate_park::Delivery::Apps
    };
    let outcome = super::handle_delegate_with_contract_requests(
        handler,
        DelegateRequest::ApplicationMessages {
            key: key.clone(),
            params: Parameters::from(Vec::new()),
            inbound: vec![InboundDelegateMsg::ApplicationMessage(
                ApplicationMessage::new(b"go".to_vec()),
            )],
        },
        origin_contract,
        scope,
        trigger,
        None,
        key,
        &prompter,
        Some(super::ParkingCtx {
            park,
            delivery,
            node_wide_duty: false,
            carried_responder: &mut no_responder,
        }),
        super::RunSeed::default(),
    )
    .await;
    match outcome {
        super::DelegateRunOutcome::Completed(msgs) => msgs,
        super::DelegateRunOutcome::Parked => panic!("the run parked"),
        super::DelegateRunOutcome::Failed(err) => panic!("the run failed: {err}"),
    }
}

fn new_park() -> super::delegate_park::DelegateParkCtx {
    let (resume_tx, _resume_rx) = tokio::sync::mpsc::unbounded_channel();
    super::delegate_park::DelegateParkCtx::new(resume_tx)
}

/// What a hop target replies with on a BACKGROUND run reaches nobody: its
/// application message is not handed on with the caller's output (which
/// would fan it out to the caller's apps labelled as the caller), its prompt
/// is not raised, and its GET is not executed. It is dropped and counted.
/// On a CLIENT run the target's reply still goes back to the client with the
/// caller's, as before.
#[tokio::test]
async fn a_background_hops_reply_reaches_no_app() {
    let key = DelegateKey::new([0x91; 32], CodeHash::new([0x91; 32]));
    let other = DelegateKey::new([0x92; 32], CodeHash::new([0x92; 32]));
    let message = NotificationMessage::try_from(&serde_json::json!({"message": "ok?"}))
        .expect("notification message");
    let target_reply = || {
        ScriptedRun::from(vec![
            app_out(b"from target"),
            OutboundDelegateMsg::RequestUserInput(UserInputRequest {
                request_id: 7,
                message: message.clone(),
                responses: vec![ClientResponse::new(b"yes".to_vec())],
            }),
            get(0x93),
        ])
    };
    let script = |_: ()| {
        vec![
            ScriptedRun::from(vec![send(&other, &key)]),
            target_reply(),
            ScriptedRun::from(vec![app_out(b"from caller")]),
        ]
    };

    // Background run.
    let (mut handler, observations, _send) = direct_handler("hop_reply_bg", script(())).await;
    let mut park = new_park();
    let out = run_direct(
        &mut handler,
        &mut park,
        &key,
        super::RunTrigger::Unprompted,
        crate::client_events::ConnectionScope::Local,
    )
    .await;
    assert_eq!(
        labels(&out),
        vec!["app:from caller".to_string()],
        "only the caller's own output is handed on to its apps"
    );
    let obs = observations.lock().unwrap().clone();
    assert_eq!(
        obs.iter()
            .map(|o| (o.delegate_key.clone(), o.inbound_kinds.clone()))
            .collect::<Vec<_>>(),
        vec![
            (key.clone(), vec!["ApplicationMessage"]),
            (other.clone(), vec!["DelegateMessage"]),
            (key.clone(), vec![]),
        ],
        "the target ran once and was never answered (no GET, no prompt)"
    );
    assert_eq!(
        park.dropped_hop_outputs(),
        3,
        "all three reply messages counted"
    );

    // Client run: unchanged, the target's reply goes back to the client.
    let (mut handler, _observations, _send) = direct_handler("hop_reply_client", script(())).await;
    let mut park = new_park();
    let out = run_direct(
        &mut handler,
        &mut park,
        &key,
        super::RunTrigger::Client,
        crate::client_events::ConnectionScope::Local,
    )
    .await;
    let out = labels(&out);
    assert!(
        out.contains(&"app:from target".to_string())
            && out.contains(&"app:from caller".to_string()),
        "a client run returns the target's reply with the caller's: {out:?}"
    );
    assert_eq!(park.dropped_hop_outputs(), 0);
}

/// A background run that hops on every iteration stops at
/// `MAX_UNPROMPTED_HOPS_PER_RUN`; the rest are refused and counted. A client
/// run is not capped.
#[tokio::test]
async fn a_background_run_stops_hopping_at_the_cap() {
    let cap = super::MAX_UNPROMPTED_HOPS_PER_RUN;
    let key = DelegateKey::new([0x94; 32], CodeHash::new([0x94; 32]));
    let other = DelegateKey::new([0x95; 32], CodeHash::new([0x95; 32]));
    let extra = 2;
    // Each scripted caller run is one iteration; after the cap the target no
    // longer runs, so its scripted replies stop being consumed.
    let script = |hops_delivered: usize| {
        let mut runs = Vec::new();
        for _ in 0..hops_delivered {
            runs.push(ScriptedRun::from(vec![send(&other, &key)]));
            runs.push(ScriptedRun::default());
        }
        for _ in hops_delivered..cap + extra {
            runs.push(ScriptedRun::from(vec![send(&other, &key)]));
        }
        runs.push(ScriptedRun::default());
        runs
    };
    let target_runs = |obs: &super::executor::mock_wasm_runtime::DelegateObservations| {
        obs.lock()
            .unwrap()
            .iter()
            .filter(|o| o.delegate_key == other)
            .count()
    };

    let (mut handler, observations, _send) = direct_handler("hop_cap_bg", script(cap)).await;
    let mut park = new_park();
    run_direct(
        &mut handler,
        &mut park,
        &key,
        super::RunTrigger::Unprompted,
        crate::client_events::ConnectionScope::Local,
    )
    .await;
    assert_eq!(target_runs(&observations), cap, "the target runs cap times");
    assert_eq!(
        park.refused_unprompted_hops(),
        extra as u64,
        "the hops past the cap are refused and counted"
    );

    let (mut handler, observations, _send) =
        direct_handler("hop_cap_client", script(cap + extra)).await;
    let mut park = new_park();
    run_direct(
        &mut handler,
        &mut park,
        &key,
        super::RunTrigger::Client,
        crate::client_events::ConnectionScope::Local,
    )
    .await;
    assert_eq!(
        target_runs(&observations),
        cap + extra,
        "a client run is not capped"
    );
    assert_eq!(park.refused_unprompted_hops(), 0);
}

/// A delegate cannot message itself: the self-hop is refused, so the
/// delegate is never re-entered with a `DelegateMessage` naming itself as the
/// attested caller.
#[tokio::test]
async fn a_delegate_cannot_message_itself() {
    let key = DelegateKey::new([0x96; 32], CodeHash::new([0x96; 32]));
    for trigger in [super::RunTrigger::Unprompted, super::RunTrigger::Client] {
        let (mut handler, observations, _send) = direct_handler(
            "self_hop",
            vec![
                ScriptedRun::from(vec![send(&key, &key), send(&key, &key)]),
                ScriptedRun::default(),
            ],
        )
        .await;
        let mut park = new_park();
        run_direct(
            &mut handler,
            &mut park,
            &key,
            trigger,
            crate::client_events::ConnectionScope::Local,
        )
        .await;
        let kinds: Vec<_> = observations
            .lock()
            .unwrap()
            .iter()
            .map(|o| o.inbound_kinds.clone())
            .collect();
        assert_eq!(
            kinds,
            vec![vec!["ApplicationMessage"], vec![]],
            "{trigger:?}: the run and its follow-up, no self-delivery"
        );
        assert_eq!(park.refused_self_hops(), 2, "{trigger:?}: both counted");
        assert_eq!(
            park.refused_unprompted_hops(),
            0,
            "{trigger:?}: a self-hop does not use up the hop cap"
        );
    }
}

/// The accepted trade-off, pinned so that re-tightening it is a visible
/// decision: a remote client drives delegate A, and A's hop then reaches B
/// with no attested origin; when A later runs in the background (a contract
/// notification), its hop reaches B as `MessageOrigin::Delegate(A)`.
#[tokio::test]
async fn a_remote_clients_delegate_later_attests_itself_from_a_background_run() {
    let key = DelegateKey::new([0x97; 32], CodeHash::new([0x97; 32]));
    let other = DelegateKey::new([0x98; 32], CodeHash::new([0x98; 32]));
    let leg = || {
        vec![
            ScriptedRun::from(vec![send(&other, &key)]),
            ScriptedRun::default(),
            ScriptedRun::default(),
        ]
    };
    let mut script = leg();
    script.extend(leg());
    let (mut handler, observations, _send) = direct_handler("remote_then_bg", script).await;
    let mut park = new_park();
    run_direct(
        &mut handler,
        &mut park,
        &key,
        super::RunTrigger::Client,
        crate::client_events::ConnectionScope::Remote,
    )
    .await;
    let prompter = Arc::new(CapabilityPrompter::answering(None));
    super::handle_delegate_notification(
        &mut handler,
        super::executor::DelegateNotification {
            delegate_key: key.clone(),
            contract_id: ContractInstanceId::new([0x99; 32]),
            new_state: Arc::new(WrappedState::new(vec![1])),
        },
        &prompter,
        Some(&mut park),
    )
    .await;
    let origins: Vec<_> = observations
        .lock()
        .unwrap()
        .iter()
        .filter(|o| o.delegate_key == other)
        .map(|o| o.origin.clone())
        .collect();
    assert_eq!(
        origins,
        vec![None, Some(MessageOrigin::Delegate(key.clone()))],
        "remote client run: unattested at B; A's later background run: attested as A"
    );
}

/// A LIFECYCLE run (here `NodeStarted`) can message another delegate too, and
/// the target sees the delegate as its attested caller.
#[tokio::test]
async fn a_lifecycle_run_can_message_a_delegate() {
    let caps = DelegateCapabilities::in_memory();
    let other = DelegateKey::new([0x9A; 32], CodeHash::new([0x9A; 32]));
    let lp = start(
        "lifecycle_hop",
        caps.clone(),
        vec![
            ScriptedRun::default(),
            ScriptedRun::default(),
            ScriptedRun::default(),
        ],
        CapabilityPrompter::answering(None),
    )
    .await;
    // Granted AFTER the loop started, so the start-up seed holds nothing for it
    // and the run below is due at once.
    let key = granted(&caps, vec![LifecycleKind::NodeStarted], b"p", 55);
    {
        // The lifecycle run's own script, now that its key is known.
        let mut script = lp.script.lock().unwrap();
        script.clear();
        script.push_back(ScriptedRun::from(vec![send(&other, &key)]));
        script.push_back(ScriptedRun::default());
        script.push_back(ScriptedRun::default());
    }
    assert!(
        caps.queue(LifecycleRun {
            key: key.clone(),
            event: LifecycleEvent::NodeStarted {
                down_since_ms: None
            }
            .into(),
        })
    );
    wait_until("the lifecycle run's hop", || {
        lp.observations
            .lock()
            .unwrap()
            .iter()
            .any(|o| o.delegate_key == other)
    })
    .await;
    let obs = lp.observations.lock().unwrap().clone();
    let reached: Vec<_> = obs.iter().filter(|o| o.delegate_key == other).collect();
    assert_eq!(reached.len(), 1, "{obs:?}");
    assert_eq!(reached[0].inbound_kinds, vec!["DelegateMessage"]);
    assert_eq!(reached[0].origin, Some(MessageOrigin::Delegate(key)));
}

/// A caller script that hops on every iteration: `delivered` hops that reach
/// the target (caller run, then the target's run), then `refused` caller runs
/// whose hop is refused at the cap (no target run), then a final empty run.
fn hop_every_iteration(
    key: &DelegateKey,
    other: &DelegateKey,
    delivered: usize,
    refused: usize,
) -> Vec<ScriptedRun> {
    let mut runs = Vec::new();
    for _ in 0..delivered {
        runs.push(ScriptedRun::from(vec![send(other, key)]));
        runs.push(ScriptedRun::default());
    }
    for _ in 0..refused {
        runs.push(ScriptedRun::from(vec![send(other, key)]));
    }
    runs.push(ScriptedRun::default());
    runs
}

fn runs_of(
    obs: &super::executor::mock_wasm_runtime::DelegateObservations,
    key: &DelegateKey,
) -> usize {
    obs.lock()
        .unwrap()
        .iter()
        .filter(|o| o.delegate_key == *key)
        .count()
}

/// The hop cap bounds the whole background RUN, across a park and resume: a
/// notification run that hops 10 times, parks on a prompt, and keeps hopping
/// after the resume gets 6 more hops, not 16 fresh ones.
#[tokio::test]
async fn the_hop_cap_holds_across_a_park() {
    let cap = super::MAX_UNPROMPTED_HOPS_PER_RUN;
    let before_park = 10;
    let extra = 2;
    let key = DelegateKey::new([0x9B; 32], CodeHash::new([0x9B; 32]));
    let other = DelegateKey::new([0x9C; 32], CodeHash::new([0x9C; 32]));
    let mut script = Vec::new();
    for _ in 0..before_park {
        script.push(ScriptedRun::from(vec![send(&other, &key)]));
        script.push(ScriptedRun::default());
    }
    // Parks on the prompt.
    script.push(user_input_run());
    // After the resume: the rest of the cap, then refusals.
    script.extend(hop_every_iteration(&key, &other, cap - before_park, extra));
    let (mut handler, observations, _send) = direct_handler("hop_cap_park", script).await;
    let (resume_tx, mut resume_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut park = super::delegate_park::DelegateParkCtx::new(resume_tx);
    let prompter = Arc::new(CapabilityPrompter::answering(None));

    super::handle_delegate_notification(
        &mut handler,
        super::executor::DelegateNotification {
            delegate_key: key.clone(),
            contract_id: ContractInstanceId::new([0x9D; 32]),
            new_state: Arc::new(WrappedState::new(vec![1])),
        },
        &prompter,
        Some(&mut park),
    )
    .await;
    assert!(park.is_parked(&key), "the run parked on its prompt");
    assert_eq!(runs_of(&observations, &other), before_park);

    let resume = tokio::time::timeout(Duration::from_secs(10), resume_rx.recv())
        .await
        .expect("the prompt is answered")
        .expect("resume");
    super::handle_delegate_resume(&mut handler, &mut park, &prompter, resume).await;
    assert!(!park.is_parked(&key), "the resumed run finished");
    assert_eq!(
        runs_of(&observations, &other),
        cap,
        "the target runs cap times over the whole run, both legs together"
    );
    assert_eq!(
        park.refused_unprompted_hops(),
        extra as u64,
        "the hops past the cap, after the resume, are refused"
    );
}

/// The cap also bounds a QUEUED notification run (`run_queued_notification`,
/// the path a notification takes when it arrived while its delegate was
/// parked).
#[tokio::test]
async fn the_hop_cap_applies_to_a_queued_notification_run() {
    let cap = super::MAX_UNPROMPTED_HOPS_PER_RUN;
    let extra = 2;
    let key = DelegateKey::new([0x9E; 32], CodeHash::new([0x9E; 32]));
    let other = DelegateKey::new([0x9F; 32], CodeHash::new([0x9F; 32]));
    let (mut handler, observations, _send) = direct_handler(
        "hop_cap_queued",
        hop_every_iteration(&key, &other, cap, extra),
    )
    .await;
    let mut park = new_park();
    let prompter = Arc::new(CapabilityPrompter::answering(None));
    let contract_id = ContractInstanceId::new([0xA0; 32]);
    super::run_queued_notification(
        &mut handler,
        Some(&mut park),
        &prompter,
        &key,
        contract_id,
        DelegateRequest::ApplicationMessages {
            key: key.clone(),
            params: Parameters::from(Vec::new()),
            inbound: vec![InboundDelegateMsg::ContractNotification(
                ContractNotification {
                    contract_id,
                    new_state: WrappedState::new(vec![1]),
                    context: DelegateContext::default(),
                },
            )],
        },
    )
    .await;
    assert_eq!(runs_of(&observations, &other), cap);
    assert_eq!(park.refused_unprompted_hops(), extra as u64);
}

/// A reply from a hop target to `to`, claiming to come from `claimed_sender`.
/// The node must deliver it as coming from the delegate that actually ran,
/// whatever this field says.
fn reply(to: &DelegateKey, claimed_sender: &DelegateKey) -> OutboundDelegateMsg {
    OutboundDelegateMsg::SendDelegateMessage(DelegateMessage::new(
        to.clone(),
        claimed_sender.clone(),
        b"signature".to_vec(),
    ))
}

/// Each run as (delegate, inbound kinds, senders of its inbound
/// `DelegateMessage`s).
type RunTrace = Vec<(DelegateKey, Vec<&'static str>, Vec<DelegateKey>)>;

fn trace(obs: &super::executor::mock_wasm_runtime::DelegateObservations) -> RunTrace {
    obs.lock()
        .unwrap()
        .iter()
        .map(|o| {
            (
                o.delegate_key.clone(),
                o.inbound_kinds.clone(),
                o.delegate_message_senders.clone(),
            )
        })
        .collect()
}

/// The core sets the sender on delivery: a message from A to B arrives with
/// `sender == A`, whatever A wrote into the field. Both run kinds.
#[tokio::test]
async fn the_core_sets_the_sender_on_delivery() {
    let key = DelegateKey::new([0xAD; 32], CodeHash::new([0xAD; 32]));
    let other = DelegateKey::new([0xAE; 32], CodeHash::new([0xAE; 32]));
    let claimed = DelegateKey::new([0xAF; 32], CodeHash::new([0xAF; 32]));
    for trigger in [super::RunTrigger::Unprompted, super::RunTrigger::Client] {
        let (mut handler, observations, _send) = direct_handler(
            "sender_on_delivery",
            vec![
                ScriptedRun::from(vec![send(&other, &claimed)]),
                ScriptedRun::default(),
                ScriptedRun::default(),
            ],
        )
        .await;
        let mut park = new_park();
        run_direct(
            &mut handler,
            &mut park,
            &key,
            trigger,
            crate::client_events::ConnectionScope::Local,
        )
        .await;
        assert_eq!(
            trace(&observations),
            vec![
                (key.clone(), vec!["ApplicationMessage"], vec![]),
                (other.clone(), vec!["DelegateMessage"], vec![key.clone()]),
                (key.clone(), vec![], vec![]),
            ],
            "{trigger:?}: B receives the message with A as its sender"
        );
    }
}

/// A hop target can answer its caller once. B replies to A during a hop, and
/// A receives the reply as an inbound `DelegateMessage` in its next
/// iteration, with `sender == B` even though B wrote another key into the
/// field. B runs once: delivering the reply does not run it again. Checked
/// for a CLIENT run (where B's application message still goes back to the
/// client) and a contract-NOTIFICATION run.
#[tokio::test]
async fn a_hop_target_can_reply_to_its_caller() {
    let key = DelegateKey::new([0xA1; 32], CodeHash::new([0xA1; 32]));
    let other = DelegateKey::new([0xA2; 32], CodeHash::new([0xA2; 32]));
    let forged = DelegateKey::new([0xA3; 32], CodeHash::new([0xA3; 32]));
    let script = || {
        vec![
            ScriptedRun::from(vec![send(&other, &key)]),
            ScriptedRun::from(vec![reply(&key, &forged), app_out(b"from target")]),
            ScriptedRun::from(vec![app_out(b"from caller")]),
        ]
    };

    // Client run.
    let (mut handler, observations, _send) = direct_handler("reply_client", script()).await;
    let mut park = new_park();
    let out = run_direct(
        &mut handler,
        &mut park,
        &key,
        super::RunTrigger::Client,
        crate::client_events::ConnectionScope::Local,
    )
    .await;
    assert_eq!(
        trace(&observations),
        vec![
            (key.clone(), vec!["ApplicationMessage"], vec![]),
            (other.clone(), vec!["DelegateMessage"], vec![key.clone()]),
            (key.clone(), vec!["DelegateMessage"], vec![other.clone()]),
        ],
        "client run: the hop, then the caller's next iteration with the reply from B"
    );
    let obs = observations.lock().unwrap().clone();
    assert_eq!(
        obs[2].origin,
        Some(MessageOrigin::Delegate(other.clone())),
        "while handling the reply the caller sees the target as its attested origin"
    );
    let out = labels(&out);
    assert!(
        out.contains(&"app:from target".to_string())
            && out.contains(&"app:from caller".to_string()),
        "a client run still returns the target's application message: {out:?}"
    );
    assert_eq!(park.dropped_hop_sends(), 0);

    // Notification run.
    let (mut handler, observations, _send) = direct_handler("reply_notify", script()).await;
    let mut park = new_park();
    let prompter = Arc::new(CapabilityPrompter::answering(None));
    super::handle_delegate_notification(
        &mut handler,
        super::executor::DelegateNotification {
            delegate_key: key.clone(),
            contract_id: ContractInstanceId::new([0xA4; 32]),
            new_state: Arc::new(WrappedState::new(vec![1])),
        },
        &prompter,
        Some(&mut park),
    )
    .await;
    assert_eq!(
        trace(&observations),
        vec![
            (key.clone(), vec!["ContractNotification"], vec![]),
            (other.clone(), vec!["DelegateMessage"], vec![key.clone()]),
            (key.clone(), vec!["DelegateMessage"], vec![other.clone()]),
        ],
        "notification run: the hop, then the caller's next iteration with the reply from B"
    );
    assert_eq!(
        observations.lock().unwrap()[2].origin,
        Some(MessageOrigin::Delegate(other.clone())),
        "notification run: the reply is handled under the target's origin"
    );
    assert_eq!(
        park.dropped_hop_outputs(),
        1,
        "on a background run the target's application message is still dropped"
    );
    assert_eq!(park.dropped_hop_sends(), 0);
}

/// Only ONE message from the target reaches anyone: a message to a third
/// delegate and a second reply to the caller are dropped and counted, and
/// the third delegate never runs. Both run kinds.
#[tokio::test]
async fn a_hop_targets_other_messages_are_dropped_and_counted() {
    let key = DelegateKey::new([0xA5; 32], CodeHash::new([0xA5; 32]));
    let other = DelegateKey::new([0xA6; 32], CodeHash::new([0xA6; 32]));
    let third = DelegateKey::new([0xA7; 32], CodeHash::new([0xA7; 32]));
    for trigger in [super::RunTrigger::Unprompted, super::RunTrigger::Client] {
        let (mut handler, observations, _send) = direct_handler(
            "reply_extra",
            vec![
                ScriptedRun::from(vec![send(&other, &key)]),
                ScriptedRun::from(vec![
                    send(&third, &other),
                    reply(&key, &other),
                    reply(&key, &other),
                ]),
                ScriptedRun::default(),
            ],
        )
        .await;
        let mut park = new_park();
        run_direct(
            &mut handler,
            &mut park,
            &key,
            trigger,
            crate::client_events::ConnectionScope::Local,
        )
        .await;
        assert_eq!(
            trace(&observations),
            vec![
                (key.clone(), vec!["ApplicationMessage"], vec![]),
                (other.clone(), vec!["DelegateMessage"], vec![key.clone()]),
                (key.clone(), vec!["DelegateMessage"], vec![other.clone()]),
            ],
            "{trigger:?}: one reply reaches the caller; the third delegate never runs"
        );
        assert_eq!(
            park.dropped_hop_sends(),
            2,
            "{trigger:?}: the message to the third delegate and the second reply"
        );
        assert_eq!(
            park.dropped_hop_outputs(),
            0,
            "{trigger:?}: dropped sends are not counted as other output"
        );
    }
}

/// A reply does not chain. The caller handles each reply by hopping to the
/// target again; those hops are the CALLER's and count against its cap on a
/// background run, so the exchange stops at `MAX_UNPROMPTED_HOPS_PER_RUN`
/// target runs, one per hop the caller made.
#[tokio::test]
async fn a_reply_triggered_hop_counts_toward_the_cap() {
    let cap = super::MAX_UNPROMPTED_HOPS_PER_RUN;
    let extra = 2;
    let key = DelegateKey::new([0xA8; 32], CodeHash::new([0xA8; 32]));
    let other = DelegateKey::new([0xA9; 32], CodeHash::new([0xA9; 32]));
    let mut script = Vec::new();
    for _ in 0..cap {
        // The caller hops (the first time from its own input, then from each
        // reply), and the target replies.
        script.push(ScriptedRun::from(vec![send(&other, &key)]));
        script.push(ScriptedRun::from(vec![reply(&key, &other)]));
    }
    for _ in 0..extra {
        script.push(ScriptedRun::from(vec![send(&other, &key)]));
    }
    script.push(ScriptedRun::default());
    let (mut handler, observations, _send) = direct_handler("reply_cap", script).await;
    let mut park = new_park();
    run_direct(
        &mut handler,
        &mut park,
        &key,
        super::RunTrigger::Unprompted,
        crate::client_events::ConnectionScope::Local,
    )
    .await;
    let runs = trace(&observations);
    assert_eq!(runs_of(&observations, &other), cap, "{runs:?}");
    assert_eq!(park.refused_unprompted_hops(), extra as u64);
    let replies_received = runs
        .iter()
        .filter(|(k, kinds, _)| *k == key && *kinds == vec!["DelegateMessage"])
        .count();
    assert_eq!(replies_received, cap, "every delivered hop's reply arrived");
}

/// A reply survives the caller parking in the same iteration. The caller
/// hops and prompts in one iteration; the target replies; the caller parks on
/// the prompt, and the resumed iteration receives the prompt's answer and
/// then, in a call of its own under the target's origin, the reply.
#[tokio::test]
async fn a_reply_survives_the_caller_parking() {
    let key = DelegateKey::new([0xAA; 32], CodeHash::new([0xAA; 32]));
    let other = DelegateKey::new([0xAB; 32], CodeHash::new([0xAB; 32]));
    let mut hop_and_prompt = user_input_run();
    hop_and_prompt.outbound.push(send(&other, &key));
    let (mut handler, observations, _send) = direct_handler(
        "reply_park",
        vec![
            hop_and_prompt,
            ScriptedRun::from(vec![reply(&key, &other)]),
            ScriptedRun::default(),
        ],
    )
    .await;
    let (resume_tx, mut resume_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut park = super::delegate_park::DelegateParkCtx::new(resume_tx);
    let prompter = Arc::new(CapabilityPrompter::answering(None));
    super::handle_delegate_notification(
        &mut handler,
        super::executor::DelegateNotification {
            delegate_key: key.clone(),
            contract_id: ContractInstanceId::new([0xAC; 32]),
            new_state: Arc::new(WrappedState::new(vec![1])),
        },
        &prompter,
        Some(&mut park),
    )
    .await;
    assert!(park.is_parked(&key), "the run parked on its prompt");
    assert_eq!(
        runs_of(&observations, &other),
        1,
        "the hop ran before the park"
    );

    let resume = tokio::time::timeout(Duration::from_secs(10), resume_rx.recv())
        .await
        .expect("the prompt is answered")
        .expect("resume");
    super::handle_delegate_resume(&mut handler, &mut park, &prompter, resume).await;
    assert!(!park.is_parked(&key), "the resumed run finished");
    let runs = trace(&observations);
    assert_eq!(
        runs[runs.len() - 2..].to_vec(),
        vec![
            (key.clone(), vec!["UserResponse"], vec![]),
            (key.clone(), vec!["DelegateMessage"], vec![other.clone()]),
        ],
        "the resumed iteration gets the prompt's answer, then the reply from B: {runs:?}"
    );
    assert_eq!(
        observations.lock().unwrap().last().unwrap().origin,
        Some(MessageOrigin::Delegate(other.clone()))
    );
}

/// A reply from `from` to `to` with a payload of `len` bytes.
fn reply_sized(to: &DelegateKey, from: &DelegateKey, len: usize) -> OutboundDelegateMsg {
    OutboundDelegateMsg::SendDelegateMessage(DelegateMessage::new(
        to.clone(),
        from.clone(),
        vec![0x5A; len],
    ))
}

/// While the caller handles a reply it sees the TARGET as its attested
/// origin, never the run's: on a run a web app started, the caller's own
/// batch sees `WebApp(app)` and the reply sees `Delegate(target)`. Over a
/// remote connection the reply, like every attested origin, is withheld.
#[tokio::test]
async fn a_reply_is_handled_under_the_targets_origin_not_the_runs() {
    let key = DelegateKey::new([0xB1; 32], CodeHash::new([0xB1; 32]));
    let other = DelegateKey::new([0xB2; 32], CodeHash::new([0xB2; 32]));
    let web_app = app(0xB3);
    let script = || {
        vec![
            ScriptedRun::from(vec![send(&other, &key)]),
            ScriptedRun::from(vec![reply(&key, &other)]),
            ScriptedRun::default(),
        ]
    };
    for (scope, expect_reply_origin, expect_run_origin) in [
        (
            crate::client_events::ConnectionScope::Local,
            Some(MessageOrigin::Delegate(other.clone())),
            Some(MessageOrigin::WebApp(web_app)),
        ),
        (crate::client_events::ConnectionScope::Remote, None, None),
    ] {
        let (mut handler, observations, _send) = direct_handler("reply_origin", script()).await;
        let mut park = new_park();
        run_direct_from(
            &mut handler,
            &mut park,
            &key,
            super::RunTrigger::Client,
            scope,
            Some(&web_app),
        )
        .await;
        let obs = observations.lock().unwrap().clone();
        assert_eq!(obs.len(), 3, "{scope:?}: {obs:?}");
        assert_eq!(
            obs[0].origin, expect_run_origin,
            "{scope:?}: the run's batch"
        );
        assert_eq!(obs[2].delegate_key, key);
        assert_eq!(obs[2].inbound_kinds, vec!["DelegateMessage"]);
        assert_eq!(
            obs[2].origin, expect_reply_origin,
            "{scope:?}: the caller handling the reply"
        );
    }
}

/// A reply over `MAX_HOP_REPLY_BYTES` is dropped and counted; one at exactly
/// the limit is delivered.
#[tokio::test]
async fn an_oversized_reply_is_dropped_and_counted() {
    let key = DelegateKey::new([0xB4; 32], CodeHash::new([0xB4; 32]));
    let other = DelegateKey::new([0xB5; 32], CodeHash::new([0xB5; 32]));
    for (len, delivered) in [
        (super::MAX_HOP_REPLY_BYTES, true),
        (super::MAX_HOP_REPLY_BYTES + 1, false),
    ] {
        let (mut handler, observations, _send) = direct_handler(
            "reply_size",
            vec![
                ScriptedRun::from(vec![send(&other, &key)]),
                ScriptedRun::from(vec![reply_sized(&key, &other, len)]),
                ScriptedRun::default(),
            ],
        )
        .await;
        let mut park = new_park();
        run_direct(
            &mut handler,
            &mut park,
            &key,
            super::RunTrigger::Unprompted,
            crate::client_events::ConnectionScope::Local,
        )
        .await;
        let runs = trace(&observations);
        let expect_kinds: Vec<&'static str> = if delivered {
            vec!["DelegateMessage"]
        } else {
            vec![]
        };
        assert_eq!(runs[2].1, expect_kinds, "{len} bytes: {runs:?}");
        assert_eq!(
            park.dropped_hop_replies(),
            u64::from(!delivered),
            "{len} bytes: counted only when dropped"
        );
    }
}

/// A run accepts at most `MAX_HOP_REPLY_BYTES_PER_RUN` of replies: on a
/// client run (which the hop cap does not bound) with every reply at the size
/// limit, the 17th is dropped and counted.
#[tokio::test]
async fn replies_past_the_run_allowance_are_dropped_and_counted() {
    let key = DelegateKey::new([0xB6; 32], CodeHash::new([0xB6; 32]));
    let other = DelegateKey::new([0xB7; 32], CodeHash::new([0xB7; 32]));
    let max = super::MAX_HOP_REPLY_BYTES;
    let fit = super::MAX_HOP_REPLY_BYTES_PER_RUN / max;
    assert_eq!(fit, super::MAX_UNPROMPTED_HOPS_PER_RUN);
    let mut script = vec![ScriptedRun::from(vec![send(&other, &key)])];
    for _ in 0..fit {
        script.push(ScriptedRun::from(vec![reply_sized(&key, &other, max)]));
        script.push(ScriptedRun::from(vec![send(&other, &key)]));
    }
    // The one past the allowance.
    script.push(ScriptedRun::from(vec![reply_sized(&key, &other, max)]));
    script.push(ScriptedRun::default());
    let (mut handler, observations, _send) = direct_handler("reply_allowance", script).await;
    let mut park = new_park();
    run_direct(
        &mut handler,
        &mut park,
        &key,
        super::RunTrigger::Client,
        crate::client_events::ConnectionScope::Local,
    )
    .await;
    let runs = trace(&observations);
    let received = runs
        .iter()
        .filter(|(k, kinds, _)| *k == key && *kinds == vec!["DelegateMessage"])
        .count();
    assert_eq!(runs_of(&observations, &other), fit + 1, "{runs:?}");
    assert_eq!(received, fit, "the replies within the allowance arrive");
    assert_eq!(park.dropped_hop_replies(), 1, "the one past it is counted");
}

/// A client run whose caller hops on every reply, with a target that always
/// replies, stops at `MAX_CONTRACT_REQUEST_ITERATIONS`: the target runs once
/// per iteration, and the reply still waiting at the cap is counted as
/// dropped.
#[tokio::test]
async fn a_client_reply_ping_pong_stops_at_the_iteration_cap() {
    let cap = super::MAX_CONTRACT_REQUEST_ITERATIONS;
    let key = DelegateKey::new([0xB8; 32], CodeHash::new([0xB8; 32]));
    let other = DelegateKey::new([0xB9; 32], CodeHash::new([0xB9; 32]));
    let mut script = vec![ScriptedRun::from(vec![send(&other, &key)])];
    // More turns than the cap allows, so only the cap can end the run.
    for _ in 0..cap + 5 {
        script.push(ScriptedRun::from(vec![reply(&key, &other)]));
        script.push(ScriptedRun::from(vec![send(&other, &key)]));
    }
    let (mut handler, observations, _send) = direct_handler("reply_ping_pong", script).await;
    let mut park = new_park();
    run_direct(
        &mut handler,
        &mut park,
        &key,
        super::RunTrigger::Client,
        crate::client_events::ConnectionScope::Local,
    )
    .await;
    assert_eq!(runs_of(&observations, &other), cap);
    assert_eq!(runs_of(&observations, &key), cap);
    assert_eq!(
        park.dropped_hop_replies(),
        1,
        "the reply produced in the last iteration is lost at the cap, and counted"
    );
}

/// A reply reaches the caller on a QUEUED notification run
/// (`run_queued_notification`) too, under the target's origin.
#[tokio::test]
async fn a_reply_reaches_the_caller_on_a_queued_notification_run() {
    let key = DelegateKey::new([0xBA; 32], CodeHash::new([0xBA; 32]));
    let other = DelegateKey::new([0xBB; 32], CodeHash::new([0xBB; 32]));
    let (mut handler, observations, _send) = direct_handler(
        "reply_queued",
        vec![
            ScriptedRun::from(vec![send(&other, &key)]),
            ScriptedRun::from(vec![reply(&key, &other)]),
            ScriptedRun::default(),
        ],
    )
    .await;
    let mut park = new_park();
    let prompter = Arc::new(CapabilityPrompter::answering(None));
    let contract_id = ContractInstanceId::new([0xBC; 32]);
    super::run_queued_notification(
        &mut handler,
        Some(&mut park),
        &prompter,
        &key,
        contract_id,
        DelegateRequest::ApplicationMessages {
            key: key.clone(),
            params: Parameters::from(Vec::new()),
            inbound: vec![InboundDelegateMsg::ContractNotification(
                ContractNotification {
                    contract_id,
                    new_state: WrappedState::new(vec![1]),
                    context: DelegateContext::default(),
                },
            )],
        },
    )
    .await;
    assert_eq!(
        trace(&observations),
        vec![
            (key.clone(), vec!["ContractNotification"], vec![]),
            (other.clone(), vec!["DelegateMessage"], vec![key.clone()]),
            (key.clone(), vec!["DelegateMessage"], vec![other.clone()]),
        ]
    );
    assert_eq!(
        observations.lock().unwrap()[2].origin,
        Some(MessageOrigin::Delegate(other))
    );
}

/// A reply produced on the RESUMED leg of a parked run reaches the caller:
/// the run parks on a prompt first, and only after the resume hops and gets
/// its reply.
#[tokio::test]
async fn a_reply_produced_on_a_resumed_leg_reaches_the_caller() {
    let key = DelegateKey::new([0xBD; 32], CodeHash::new([0xBD; 32]));
    let other = DelegateKey::new([0xBE; 32], CodeHash::new([0xBE; 32]));
    let (mut handler, observations, _send) = direct_handler(
        "reply_resumed_leg",
        vec![
            user_input_run(),
            ScriptedRun::from(vec![send(&other, &key)]),
            ScriptedRun::from(vec![reply(&key, &other)]),
            ScriptedRun::default(),
        ],
    )
    .await;
    let (resume_tx, mut resume_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut park = super::delegate_park::DelegateParkCtx::new(resume_tx);
    let prompter = Arc::new(CapabilityPrompter::answering(None));
    super::handle_delegate_notification(
        &mut handler,
        super::executor::DelegateNotification {
            delegate_key: key.clone(),
            contract_id: ContractInstanceId::new([0xBF; 32]),
            new_state: Arc::new(WrappedState::new(vec![1])),
        },
        &prompter,
        Some(&mut park),
    )
    .await;
    assert!(park.is_parked(&key), "the run parked on its prompt");
    let resume = tokio::time::timeout(Duration::from_secs(10), resume_rx.recv())
        .await
        .expect("the prompt is answered")
        .expect("resume");
    super::handle_delegate_resume(&mut handler, &mut park, &prompter, resume).await;
    assert!(!park.is_parked(&key), "the resumed run finished");
    assert_eq!(
        trace(&observations),
        vec![
            (key.clone(), vec!["ContractNotification"], vec![]),
            (key.clone(), vec!["UserResponse"], vec![]),
            (other.clone(), vec!["DelegateMessage"], vec![key.clone()]),
            (key.clone(), vec!["DelegateMessage"], vec![other.clone()]),
        ]
    );
    assert_eq!(
        observations.lock().unwrap()[3].origin,
        Some(MessageOrigin::Delegate(other))
    );
}

/// `split_hop_output` directly: the reply is the FIRST UNPROCESSED message
/// addressed to the caller, even after a send to a third delegate and a
/// processed message to the caller; it comes back with `sender` set to the
/// target and `context` reset; every other send is counted; the rest is
/// returned in order.
#[test]
fn split_hop_output_takes_the_first_reply_and_resets_it() {
    let caller = DelegateKey::new([0xC1; 32], CodeHash::new([0xC1; 32]));
    let target = DelegateKey::new([0xC2; 32], CodeHash::new([0xC2; 32]));
    let third = DelegateKey::new([0xC3; 32], CodeHash::new([0xC3; 32]));
    let mut processed = DelegateMessage::new(caller.clone(), target.clone(), b"echo".to_vec());
    processed.processed = true;
    let mut first = DelegateMessage::new(caller.clone(), third.clone(), b"first".to_vec());
    first.context = DelegateContext::new(b"target's context".to_vec());
    let out = super::split_hop_output(
        vec![
            send(&third, &target),
            OutboundDelegateMsg::SendDelegateMessage(processed),
            OutboundDelegateMsg::SendDelegateMessage(first),
            app_out(b"between"),
            OutboundDelegateMsg::SendDelegateMessage(DelegateMessage::new(
                caller.clone(),
                target.clone(),
                b"second".to_vec(),
            )),
        ],
        &caller,
        &target,
    );
    let reply = out.reply.expect("a reply");
    assert_eq!(reply.target, caller);
    assert_eq!(
        reply.payload,
        b"first".to_vec(),
        "the first unprocessed reply wins"
    );
    assert_eq!(reply.sender, target, "sender set to the target");
    assert!(!reply.processed);
    assert!(reply.context.as_ref().is_empty(), "context reset");
    assert_eq!(
        out.dropped_sends, 3,
        "the third-party send, the processed message and the second reply"
    );
    assert_eq!(labels(&out.other), vec!["app:between".to_string()]);
}

/// The per-run reply allowance bounds the whole RUN, across a park: a client
/// run takes 10 maximal replies, parks on a prompt, and after the resume gets
/// only the 6 more that fit, not a fresh allowance.
#[tokio::test]
async fn the_reply_allowance_holds_across_a_park() {
    let key = DelegateKey::new([0xC4; 32], CodeHash::new([0xC4; 32]));
    let other = DelegateKey::new([0xC5; 32], CodeHash::new([0xC5; 32]));
    let max = super::MAX_HOP_REPLY_BYTES;
    let fit = super::MAX_HOP_REPLY_BYTES_PER_RUN / max;
    let before_park = 10;
    let mut script = vec![ScriptedRun::from(vec![send(&other, &key)])];
    for _ in 0..before_park - 1 {
        script.push(ScriptedRun::from(vec![reply_sized(&key, &other, max)]));
        script.push(ScriptedRun::from(vec![send(&other, &key)]));
    }
    // The 10th reply; handling it, the caller prompts and parks.
    script.push(ScriptedRun::from(vec![reply_sized(&key, &other, max)]));
    script.push(user_input_run());
    // After the resume: hop again until the allowance is spent, plus one.
    script.push(ScriptedRun::from(vec![send(&other, &key)]));
    for _ in 0..fit - before_park - 1 {
        script.push(ScriptedRun::from(vec![reply_sized(&key, &other, max)]));
        script.push(ScriptedRun::from(vec![send(&other, &key)]));
    }
    script.push(ScriptedRun::from(vec![reply_sized(&key, &other, max)]));
    script.push(ScriptedRun::from(vec![send(&other, &key)]));
    script.push(ScriptedRun::from(vec![reply_sized(&key, &other, max)]));
    script.push(ScriptedRun::default());
    let (mut handler, observations, _send) = direct_handler("reply_allowance_park", script).await;
    let (resume_tx, mut resume_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut park = super::delegate_park::DelegateParkCtx::new(resume_tx);
    let prompter = Arc::new(CapabilityPrompter::answering(None));
    let mut no_responder = None;
    let outcome = super::handle_delegate_with_contract_requests(
        &mut handler,
        DelegateRequest::ApplicationMessages {
            key: key.clone(),
            params: Parameters::from(Vec::new()),
            inbound: vec![InboundDelegateMsg::ApplicationMessage(
                ApplicationMessage::new(b"go".to_vec()),
            )],
        },
        None,
        crate::client_events::ConnectionScope::Local,
        super::RunTrigger::Client,
        None,
        &key,
        &prompter,
        Some(super::ParkingCtx {
            park: &mut park,
            delivery: super::delegate_park::Delivery::Client,
            node_wide_duty: false,
            carried_responder: &mut no_responder,
        }),
        super::RunSeed::default(),
    )
    .await;
    assert!(matches!(outcome, super::DelegateRunOutcome::Parked));
    let received = |obs: &super::executor::mock_wasm_runtime::DelegateObservations| {
        obs.lock()
            .unwrap()
            .iter()
            .filter(|o| o.delegate_key == key && o.inbound_kinds == vec!["DelegateMessage"])
            .count()
    };
    assert_eq!(received(&observations), before_park);

    let resume = tokio::time::timeout(Duration::from_secs(10), resume_rx.recv())
        .await
        .expect("the prompt is answered")
        .expect("resume");
    super::handle_delegate_resume(&mut handler, &mut park, &prompter, resume).await;
    assert!(!park.is_parked(&key), "the resumed run finished");
    assert_eq!(
        received(&observations),
        fit,
        "both legs together accept exactly the allowance"
    );
    assert_eq!(park.dropped_hop_replies(), 1, "the one past it is counted");
}

/// Each reply is delivered in an iteration of its own, so replies cannot
/// raise the number of calls a run makes into its delegate above
/// `MAX_CONTRACT_REQUEST_ITERATIONS`. A client run hops 150 times in one
/// iteration and every target replies: the following iterations deliver the
/// replies that fit under the cap, one each, and the rest are counted.
#[tokio::test]
async fn reply_calls_count_against_the_iteration_cap() {
    let cap = super::MAX_CONTRACT_REQUEST_ITERATIONS;
    let hops = 150;
    let key = DelegateKey::new([0xC6; 32], CodeHash::new([0xC6; 32]));
    let other = DelegateKey::new([0xC7; 32], CodeHash::new([0xC7; 32]));
    let mut script = vec![ScriptedRun::from(
        (0..hops).map(|_| send(&other, &key)).collect::<Vec<_>>(),
    )];
    for _ in 0..hops {
        script.push(ScriptedRun::from(vec![reply(&key, &other)]));
    }
    let (mut handler, observations, _send) = direct_handler("reply_iteration_cap", script).await;
    let mut park = new_park();
    run_direct(
        &mut handler,
        &mut park,
        &key,
        super::RunTrigger::Client,
        crate::client_events::ConnectionScope::Local,
    )
    .await;
    // The first iteration is one call; the replies use the other cap - 1.
    let delivered = cap - 1;
    assert_eq!(runs_of(&observations, &other), hops, "every hop ran");
    assert_eq!(
        runs_of(&observations, &key),
        cap,
        "the caller is called at most MAX_CONTRACT_REQUEST_ITERATIONS times"
    );
    assert_eq!(
        park.dropped_hop_replies(),
        (hops - delivered) as u64,
        "the replies past the cap are counted"
    );
}

/// Several replies in one iteration are delivered in the order the hops ran,
/// each in a call of its own under its own target's origin.
#[tokio::test]
async fn several_replies_are_delivered_in_order_each_under_its_own_origin() {
    let key = DelegateKey::new([0xC8; 32], CodeHash::new([0xC8; 32]));
    let first = DelegateKey::new([0xC9; 32], CodeHash::new([0xC9; 32]));
    let second = DelegateKey::new([0xCA; 32], CodeHash::new([0xCA; 32]));
    let (mut handler, observations, _send) = direct_handler(
        "reply_order",
        vec![
            ScriptedRun::from(vec![send(&first, &key), send(&second, &key)]),
            ScriptedRun::from(vec![reply(&key, &first)]),
            ScriptedRun::from(vec![reply(&key, &second)]),
        ],
    )
    .await;
    let mut park = new_park();
    run_direct(
        &mut handler,
        &mut park,
        &key,
        super::RunTrigger::Unprompted,
        crate::client_events::ConnectionScope::Local,
    )
    .await;
    let obs = observations.lock().unwrap().clone();
    let replies: Vec<_> = obs
        .iter()
        .filter(|o| o.delegate_key == key && o.inbound_kinds == vec!["DelegateMessage"])
        .map(|o| (o.delegate_message_senders.clone(), o.origin.clone()))
        .collect();
    assert_eq!(
        replies,
        vec![
            (
                vec![first.clone()],
                Some(MessageOrigin::Delegate(first.clone()))
            ),
            (
                vec![second.clone()],
                Some(MessageOrigin::Delegate(second.clone()))
            ),
        ]
    );
}

/// A reply call that FAILS does not fail the run. The caller hops twice and
/// prompts, parks, and on resume its batch (the prompt's answer) sends a
/// further message, which is delivered; then the first reply call fails, the
/// second still runs, and the failed reply is counted.
#[tokio::test]
async fn a_failing_reply_call_is_dropped_and_the_round_goes_on() {
    let key = DelegateKey::new([0xCB; 32], CodeHash::new([0xCB; 32]));
    let first = DelegateKey::new([0xCC; 32], CodeHash::new([0xCC; 32]));
    let second = DelegateKey::new([0xCD; 32], CodeHash::new([0xCD; 32]));
    let later = DelegateKey::new([0xCE; 32], CodeHash::new([0xCE; 32]));
    let mut hop_twice_and_prompt = user_input_run();
    hop_twice_and_prompt
        .outbound
        .extend([send(&first, &key), send(&second, &key)]);
    let (mut handler, observations, _send) = direct_handler(
        "reply_call_fails",
        vec![
            // 1: the notification run; 2, 3: the two hops.
            hop_twice_and_prompt,
            ScriptedRun::from(vec![reply(&key, &first)]),
            ScriptedRun::from(vec![reply(&key, &second)]),
            // 4: the resumed batch. 5: the batch's hop target. 6 (the first
            // reply) fails before taking a script entry. 7: the second reply.
            ScriptedRun::from(vec![send(&later, &key)]),
            ScriptedRun::default(),
            ScriptedRun::default(),
        ],
    )
    .await;
    {
        let rt = handler.runtime_mut();
        let fail = rt.delegate_wrong_variant.clone();
        let mut calls = 0;
        *rt.delegate_run_hook.lock().unwrap() = Some(Box::new(move |_: &DelegateKey| {
            calls += 1;
            fail.store(calls == 6, Ordering::SeqCst);
        }));
    }
    let (resume_tx, mut resume_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut park = super::delegate_park::DelegateParkCtx::new(resume_tx);
    let prompter = Arc::new(CapabilityPrompter::answering(None));
    super::handle_delegate_notification(
        &mut handler,
        super::executor::DelegateNotification {
            delegate_key: key.clone(),
            contract_id: ContractInstanceId::new([0xCF; 32]),
            new_state: Arc::new(WrappedState::new(vec![1])),
        },
        &prompter,
        Some(&mut park),
    )
    .await;
    assert!(park.is_parked(&key), "the run parked on its prompt");
    let resume = tokio::time::timeout(Duration::from_secs(10), resume_rx.recv())
        .await
        .expect("the prompt is answered")
        .expect("resume");
    super::handle_delegate_resume(&mut handler, &mut park, &prompter, resume).await;
    assert!(!park.is_parked(&key), "the resumed run finished");
    let runs = trace(&observations);
    assert_eq!(
        runs[3..].to_vec(),
        vec![
            (key.clone(), vec!["UserResponse"], vec![]),
            (later.clone(), vec!["DelegateMessage"], vec![key.clone()]),
            (key.clone(), vec!["DelegateMessage"], vec![second.clone()]),
        ],
        "the batch ran and its message was delivered, and the second reply \
         ran after the failed first: {runs:?}"
    );
    assert_eq!(park.dropped_hop_replies(), 1, "the failed reply is counted");
}

/// When the caller fails on the BATCH (here, the resumed prompt answer) the
/// run fails as before, and the replies that were waiting behind it are
/// counted as dropped rather than lost silently.
#[tokio::test]
async fn replies_behind_a_failed_batch_are_counted() {
    let key = DelegateKey::new([0xD1; 32], CodeHash::new([0xD1; 32]));
    let first = DelegateKey::new([0xD2; 32], CodeHash::new([0xD2; 32]));
    let second = DelegateKey::new([0xD3; 32], CodeHash::new([0xD3; 32]));
    let mut hop_twice_and_prompt = user_input_run();
    hop_twice_and_prompt
        .outbound
        .extend([send(&first, &key), send(&second, &key)]);
    let (mut handler, observations, _send) = direct_handler(
        "reply_batch_fails",
        vec![
            hop_twice_and_prompt,
            ScriptedRun::from(vec![reply(&key, &first)]),
            ScriptedRun::from(vec![reply(&key, &second)]),
            ScriptedRun::default(),
        ],
    )
    .await;
    {
        let rt = handler.runtime_mut();
        let fail = rt.delegate_wrong_variant.clone();
        let mut calls = 0;
        // Call 4 is the resumed batch.
        *rt.delegate_run_hook.lock().unwrap() = Some(Box::new(move |_: &DelegateKey| {
            calls += 1;
            fail.store(calls == 4, Ordering::SeqCst);
        }));
    }
    let (resume_tx, mut resume_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut park = super::delegate_park::DelegateParkCtx::new(resume_tx);
    let prompter = Arc::new(CapabilityPrompter::answering(None));
    super::handle_delegate_notification(
        &mut handler,
        super::executor::DelegateNotification {
            delegate_key: key.clone(),
            contract_id: ContractInstanceId::new([0xD4; 32]),
            new_state: Arc::new(WrappedState::new(vec![1])),
        },
        &prompter,
        Some(&mut park),
    )
    .await;
    assert!(park.is_parked(&key), "the run parked on its prompt");
    let resume = tokio::time::timeout(Duration::from_secs(10), resume_rx.recv())
        .await
        .expect("the prompt is answered")
        .expect("resume");
    super::handle_delegate_resume(&mut handler, &mut park, &prompter, resume).await;
    assert!(!park.is_parked(&key), "the resumed run ended");
    assert_eq!(
        trace(&observations).len(),
        3,
        "no reply call ran after the failed batch"
    );
    assert_eq!(
        park.dropped_hop_replies(),
        2,
        "both waiting replies are counted"
    );
}

/// ONE CALL PER ITERATION: what the caller emits while handling one reply is
/// processed (here, its hop runs) before the next reply is delivered. So an
/// iteration never holds more than one call's output, and replies cannot
/// stack several calls' outputs up before any of it is handled.
#[tokio::test]
async fn a_replys_output_is_handled_before_the_next_reply_is_delivered() {
    let key = DelegateKey::new([0xD5; 32], CodeHash::new([0xD5; 32]));
    let first = DelegateKey::new([0xD6; 32], CodeHash::new([0xD6; 32]));
    let second = DelegateKey::new([0xD7; 32], CodeHash::new([0xD7; 32]));
    let third = DelegateKey::new([0xD8; 32], CodeHash::new([0xD8; 32]));
    let (mut handler, observations, _send) = direct_handler(
        "reply_one_call_per_iteration",
        vec![
            ScriptedRun::from(vec![send(&first, &key), send(&second, &key)]),
            ScriptedRun::from(vec![reply(&key, &first)]),
            ScriptedRun::from(vec![reply(&key, &second)]),
            // Handling the first reply, the caller messages a third delegate.
            ScriptedRun::from(vec![send(&third, &key)]),
            ScriptedRun::default(),
            ScriptedRun::default(),
        ],
    )
    .await;
    let mut park = new_park();
    run_direct(
        &mut handler,
        &mut park,
        &key,
        super::RunTrigger::Client,
        crate::client_events::ConnectionScope::Local,
    )
    .await;
    assert_eq!(
        trace(&observations),
        vec![
            (key.clone(), vec!["ApplicationMessage"], vec![]),
            (first.clone(), vec!["DelegateMessage"], vec![key.clone()]),
            (second.clone(), vec!["DelegateMessage"], vec![key.clone()]),
            (key.clone(), vec!["DelegateMessage"], vec![first.clone()]),
            (third.clone(), vec!["DelegateMessage"], vec![key.clone()]),
            (key.clone(), vec!["DelegateMessage"], vec![second.clone()]),
        ],
        "the first reply's hop runs before the second reply is delivered"
    );
}

/// A park that ends with NOTHING but a pending reply (force-resumed by the
/// TTL backstop with no inbound, after an iteration that only hopped and
/// prompted) still re-enters the caller to deliver the reply.
#[tokio::test]
async fn a_park_resumed_with_only_a_reply_delivers_it() {
    let key = DelegateKey::new([0xD9; 32], CodeHash::new([0xD9; 32]));
    let other = DelegateKey::new([0xDA; 32], CodeHash::new([0xDA; 32]));
    let mut hop_and_prompt = user_input_run();
    hop_and_prompt.outbound.push(send(&other, &key));
    let (mut handler, observations, _send) = direct_handler(
        "reply_only_resume",
        vec![
            hop_and_prompt,
            ScriptedRun::from(vec![reply(&key, &other)]),
            ScriptedRun::default(),
        ],
    )
    .await;
    let mut park = new_park();
    let prompter = Arc::new(CapabilityPrompter::answering(None));
    super::handle_delegate_notification(
        &mut handler,
        super::executor::DelegateNotification {
            delegate_key: key.clone(),
            contract_id: ContractInstanceId::new([0xDB; 32]),
            new_state: Arc::new(WrappedState::new(vec![1])),
        },
        &prompter,
        Some(&mut park),
    )
    .await;
    let epoch = park.epoch_of(&key).expect("the run parked on its prompt");
    // What the TTL backstop sends: no inbound at all.
    super::handle_delegate_resume(
        &mut handler,
        &mut park,
        &prompter,
        super::delegate_park::DelegateResume {
            delegate_key: key.clone(),
            epoch,
            cause: super::delegate_park::ResumeCause::TimedOut,
            inbound: Vec::new(),
            upserts: Vec::new(),
            unresolved_upserts: Vec::new(),
            contract_ops: Vec::new(),
            unresolved_contract_ops: Vec::new(),
        },
    )
    .await;
    assert!(!park.is_parked(&key));
    assert_eq!(
        trace(&observations),
        vec![
            (key.clone(), vec!["ContractNotification"], vec![]),
            (other.clone(), vec!["DelegateMessage"], vec![key.clone()]),
            (key.clone(), vec!["DelegateMessage"], vec![other.clone()]),
        ],
        "the force-resumed run delivers the pending reply"
    );
    assert_eq!(park.dropped_hop_replies(), 0);
}

/// `next_call` directly: no replies gives the batch; replies with an empty
/// batch give the oldest reply, naming its sender; replies with a non-empty
/// batch give the batch and leave the replies queued.
#[test]
fn next_call_picks_the_batch_then_one_reply() {
    let key = DelegateKey::new([0xDC; 32], CodeHash::new([0xDC; 32]));
    let a = DelegateKey::new([0xDD; 32], CodeHash::new([0xDD; 32]));
    let b = DelegateKey::new([0xDE; 32], CodeHash::new([0xDE; 32]));
    let params = Parameters::from(b"p".to_vec());
    let batch = |inbound: Vec<InboundDelegateMsg<'static>>| DelegateRequest::ApplicationMessages {
        key: key.clone(),
        params: params.clone(),
        inbound,
    };
    let pending = |from: &DelegateKey| super::delegate_park::PendingReply {
        from: from.clone(),
        msg: DelegateMessage::new(key.clone(), from.clone(), b"r".to_vec()),
    };
    let inbound_of = |req: &DelegateRequest<'static>| match req {
        DelegateRequest::ApplicationMessages { inbound, .. } => inbound.len(),
        DelegateRequest::RegisterDelegate { .. } | DelegateRequest::UnregisterDelegate(_) | _ => {
            usize::MAX
        }
    };

    let mut none = std::collections::VecDeque::new();
    let call = super::next_call(batch(vec![]), &mut none, &key, &params);
    assert!(
        call.reply_from.is_none(),
        "no replies: the batch, even empty"
    );

    let mut queued: std::collections::VecDeque<_> = [pending(&a), pending(&b)].into();
    let call = super::next_call(
        batch(vec![InboundDelegateMsg::ApplicationMessage(
            ApplicationMessage::new(b"x".to_vec()),
        )]),
        &mut queued,
        &key,
        &params,
    );
    assert!(call.reply_from.is_none(), "a non-empty batch goes first");
    assert_eq!(inbound_of(&call.req), 1);
    assert_eq!(queued.len(), 2, "the replies stay queued");

    let call = super::next_call(batch(vec![]), &mut queued, &key, &params);
    assert_eq!(call.reply_from, Some(a.clone()), "the oldest reply");
    assert_eq!(inbound_of(&call.req), 1);
    assert_eq!(queued.len(), 1, "one reply per call");
}

/// Start a CLIENT run of `key` directly, with a parking context and the
/// given hosted user, and return what the run did.
async fn start_client_run(
    handler: &mut MockWasmContractHandler,
    park: &mut super::delegate_park::DelegateParkCtx,
    prompter: &Arc<CapabilityPrompter>,
    key: &DelegateKey,
    user: Option<&crate::wasm_runtime::UserSecretContext>,
) -> super::DelegateRunOutcome {
    let mut no_responder = None;
    super::handle_delegate_with_contract_requests(
        handler,
        DelegateRequest::ApplicationMessages {
            key: key.clone(),
            params: Parameters::from(Vec::new()),
            inbound: vec![InboundDelegateMsg::ApplicationMessage(
                ApplicationMessage::new(b"go".to_vec()),
            )],
        },
        None,
        crate::client_events::ConnectionScope::Local,
        super::RunTrigger::Client,
        user,
        key,
        prompter,
        Some(super::ParkingCtx {
            park,
            delivery: super::delegate_park::Delivery::Client,
            node_wide_duty: false,
            carried_responder: &mut no_responder,
        }),
        super::RunSeed::default(),
    )
    .await
}

/// The reply queue never holds more replies than the run has iterations left
/// to deliver them: the excess is dropped and counted WHEN IT ARRIVES, not
/// retained (here across a park) until the cap. A client run hops 150 times
/// and prompts in its first iteration: with 99 iterations left, 51 replies
/// are counted at once, before the park.
#[tokio::test]
async fn the_reply_queue_never_outgrows_the_remaining_iterations() {
    let cap = super::MAX_CONTRACT_REQUEST_ITERATIONS;
    let hops = 150;
    let key = DelegateKey::new([0xE1; 32], CodeHash::new([0xE1; 32]));
    let other = DelegateKey::new([0xE2; 32], CodeHash::new([0xE2; 32]));
    let mut first = user_input_run();
    first.outbound.extend((0..hops).map(|_| send(&other, &key)));
    let mut script = vec![first];
    for _ in 0..hops {
        script.push(ScriptedRun::from(vec![reply(&key, &other)]));
    }
    let (mut handler, observations, _send) = direct_handler("reply_queue_bound", script).await;
    let (resume_tx, mut resume_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut park = super::delegate_park::DelegateParkCtx::new(resume_tx);
    let prompter = Arc::new(CapabilityPrompter::answering(None));
    let outcome = start_client_run(&mut handler, &mut park, &prompter, &key, None).await;
    assert!(matches!(outcome, super::DelegateRunOutcome::Parked));
    let queued = cap - 1;
    assert_eq!(
        park.dropped_hop_replies(),
        (hops - queued) as u64,
        "the replies past the iterations left are counted at enqueue, before the park"
    );

    let resume = tokio::time::timeout(Duration::from_secs(10), resume_rx.recv())
        .await
        .expect("the prompt is answered")
        .expect("resume");
    super::handle_delegate_resume(&mut handler, &mut park, &prompter, resume).await;
    // The resumed batch (the prompt's answer) takes one of the 99 iterations,
    // so one queued reply still meets the cap.
    let delivered = observations
        .lock()
        .unwrap()
        .iter()
        .filter(|o| o.delegate_key == key && o.inbound_kinds == vec!["DelegateMessage"])
        .count();
    assert_eq!(delivered, queued - 1);
    assert_eq!(park.dropped_hop_replies(), (hops - queued + 1) as u64);
}

/// A run that fails on an `UnsubscribeContractRequest` (not implemented yet,
/// #5600) while a reply is still queued counts that reply as dropped. The
/// caller hops twice; handling the first reply it asks to unsubscribe.
#[tokio::test]
async fn a_reply_queued_behind_an_unsubscribe_failure_is_counted() {
    let key = DelegateKey::new([0xE5; 32], CodeHash::new([0xE5; 32]));
    let first = DelegateKey::new([0xE6; 32], CodeHash::new([0xE6; 32]));
    let second = DelegateKey::new([0xE7; 32], CodeHash::new([0xE7; 32]));
    let (mut handler, observations, _send) = direct_handler(
        "reply_unsubscribe_failure",
        vec![
            ScriptedRun::from(vec![send(&first, &key), send(&second, &key)]),
            ScriptedRun::from(vec![reply(&key, &first)]),
            ScriptedRun::from(vec![reply(&key, &second)]),
            ScriptedRun::from(vec![OutboundDelegateMsg::UnsubscribeContractRequest(
                UnsubscribeContractRequest::new(ContractInstanceId::new([0xE8; 32])),
            )]),
        ],
    )
    .await;
    let mut park = new_park();
    let prompter = Arc::new(CapabilityPrompter::answering(None));
    let outcome = start_client_run(&mut handler, &mut park, &prompter, &key, None).await;
    assert!(matches!(outcome, super::DelegateRunOutcome::Failed(_)));
    assert_eq!(
        trace(&observations).last().map(|r| r.2.clone()),
        Some(vec![first.clone()]),
        "the run failed handling the first reply"
    );
    assert_eq!(
        park.dropped_hop_replies(),
        1,
        "the second reply, still queued, is counted"
    );
}

/// On a hosted node a reply call runs under the RUN's user (its secrets and
/// its context), like every other call of the run: on the direct path and on
/// a resumed leg.
#[tokio::test]
async fn a_reply_call_runs_under_the_runs_hosted_user() {
    let user = crate::wasm_runtime::UserSecretContext::from_token(b"hosted-user");
    let uid = *user.user_id();
    let key = DelegateKey::new([0xE3; 32], CodeHash::new([0xE3; 32]));
    let other = DelegateKey::new([0xE4; 32], CodeHash::new([0xE4; 32]));
    let reply_call_user = |obs: &super::executor::mock_wasm_runtime::DelegateObservations| {
        obs.lock()
            .unwrap()
            .iter()
            .filter(|o| o.delegate_key == key && o.inbound_kinds == vec!["DelegateMessage"])
            .map(|o| o.secret_user)
            .collect::<Vec<_>>()
    };
    let prompter = Arc::new(CapabilityPrompter::answering(None));

    // Direct path.
    let (mut handler, observations, _send) = direct_handler(
        "reply_hosted_direct",
        vec![
            ScriptedRun::from(vec![send(&other, &key)]),
            ScriptedRun::from(vec![reply(&key, &other)]),
            ScriptedRun::default(),
        ],
    )
    .await;
    let mut park = new_park();
    let outcome = start_client_run(&mut handler, &mut park, &prompter, &key, Some(&user)).await;
    assert!(matches!(outcome, super::DelegateRunOutcome::Completed(_)));
    assert_eq!(
        reply_call_user(&observations),
        vec![Some(uid)],
        "direct path"
    );

    // Resumed leg: the reply is produced before the park and delivered after.
    let mut hop_and_prompt = user_input_run();
    hop_and_prompt.outbound.push(send(&other, &key));
    let (mut handler, observations, _send) = direct_handler(
        "reply_hosted_resumed",
        vec![
            hop_and_prompt,
            ScriptedRun::from(vec![reply(&key, &other)]),
            ScriptedRun::default(),
        ],
    )
    .await;
    let (resume_tx, mut resume_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut park = super::delegate_park::DelegateParkCtx::new(resume_tx);
    let outcome = start_client_run(&mut handler, &mut park, &prompter, &key, Some(&user)).await;
    assert!(matches!(outcome, super::DelegateRunOutcome::Parked));
    let resume = tokio::time::timeout(Duration::from_secs(10), resume_rx.recv())
        .await
        .expect("the prompt is answered")
        .expect("resume");
    super::handle_delegate_resume(&mut handler, &mut park, &prompter, resume).await;
    assert_eq!(
        reply_call_user(&observations),
        vec![Some(uid)],
        "resumed leg"
    );
}

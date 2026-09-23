//! Delegate capabilities driven through the real `contract_handling` loop over
//! a scripted mock delegate: the forged-lifecycle refusal, consent at
//! registration, `Installed`/`NodeStarted` delivery, and the unprompted-run
//! budget.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use freenet_stdlib::client_api::DelegateRequest;
use freenet_stdlib::prelude::*;

use super::delegate_capabilities::{
    AppIdentity, BudgetLimits, DelegateCapabilities, Grant, LifecycleRun,
    MemoryCapabilityStorage,
};
use super::executor::mock_wasm_runtime::ScriptedRun;
use super::MockWasmContractHandler;
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
        assert!(message.contains("background"), "{message}");
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

fn register(delegate: DelegateContainer, origin: Option<ContractInstanceId>) -> ContractHandlerEvent {
    ContractHandlerEvent::DelegateRequest {
        req: DelegateRequest::RegisterDelegate {
            delegate,
            cipher: [0u8; 32],
            nonce: [0u8; 24],
        },
        origin_contract: origin,
        connection_scope: crate::client_events::ConnectionScope::Local,
        user_context: None,
    }
}

fn app_messages(key: &DelegateKey, inbound: Vec<InboundDelegateMsg<'static>>) -> ContractHandlerEvent {
    ContractHandlerEvent::DelegateRequest {
        req: DelegateRequest::ApplicationMessages {
            key: key.clone(),
            params: Parameters::from(Vec::new()),
            inbound,
        },
        origin_contract: None,
        connection_scope: crate::client_events::ConnectionScope::Local,
        user_context: None,
    }
}

struct Loop {
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

    /// Round-trip an event through the loop, so everything queued before it
    /// has had at least one loop iteration.
    async fn sync(&self) {
        let _ = self
            .send
            .send_to_handler(ContractHandlerEvent::GetQuery {
                instance_id: ContractInstanceId::new([0xEE; 32]),
                return_contract_code: false,
            })
            .await;
    }
}

async fn start<P: UserInputPrompter + 'static>(
    name: &str,
    caps: Arc<DelegateCapabilities>,
    script: Vec<ScriptedRun>,
    prompter: P,
) -> Loop {
    let (send, rcv, _) = handler::contract_handler_channel();
    let mut handler = MockWasmContractHandler::new_test(rcv, None, name).await;
    let rt = handler.runtime_mut();
    rt.capabilities = Some(caps.clone());
    let script_handle = rt.delegate_script.clone();
    script_handle.lock().unwrap().extend(script);
    let observations = rt.delegate_observations.clone();
    let handle = GlobalExecutor::spawn(contract_handling(handler, prompter));
    Loop {
        send: Arc::new(send),
        caps,
        script: script_handle,
        observations,
        handle,
    }
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

fn answer(event: ContractHandlerEvent) -> Result<Vec<OutboundDelegateMsg>, String> {
    match event {
        ContractHandlerEvent::DelegateResponse(r) => r.map_err(|e| e.to_string()),
        other => panic!("expected a DelegateResponse, got {other}"),
    }
}

/// A client cannot hand a delegate a Lifecycle or WakeupFired message: the
/// request is refused before the delegate runs. The delegate is scripted to
/// answer, so without the refusal the client would get Ok and the call log
/// would show an entry.
#[tokio::test]
async fn a_client_cannot_forge_a_host_only_message() {
    let caps = DelegateCapabilities::in_memory();
    let reply = ScriptedRun::from(vec![OutboundDelegateMsg::ApplicationMessage(
        ApplicationMessage::new(b"ran".to_vec()),
    )]);
    let lp = start("forged_lifecycle", caps.clone(), vec![reply.clone(), reply], CapabilityPrompter::answering(None)).await;
    let key = DelegateKey::new([3; 32], CodeHash::new([3; 32]));
    for forged in [
        InboundDelegateMsg::Lifecycle(LifecycleEvent::Installed),
        InboundDelegateMsg::WakeupFired { tag: vec![1] },
    ] {
        let resp = answer(lp.send.send_to_handler(app_messages(&key, vec![forged])).await.unwrap());
        assert!(resp.is_err(), "forged host-only message must be refused, got {resp:?}");
    }
    assert!(
        lp.observations.lock().unwrap().is_empty(),
        "the delegate must not have run"
    );
    assert_eq!(caps.stats.forged_lifecycle_refused.load(Ordering::Relaxed), 2);
    // An ordinary message still runs.
    let ok = answer(
        lp.send
            .send_to_handler(app_messages(
                &key,
                vec![InboundDelegateMsg::ApplicationMessage(ApplicationMessage::new(b"hi".to_vec()))],
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
        vec![ScriptedRun::default(), ScriptedRun::default(), ScriptedRun::default()],
        prompter,
    )
    .await;

    let resp = answer(lp.send.send_to_handler(register(delegate.clone(), Some(app(1)))).await.unwrap());
    assert!(resp.is_ok(), "{resp:?}");
    wait_until("Installed delivered", || !lp.lifecycle_runs().is_empty()).await;
    assert_eq!(lp.lifecycle_runs(), vec![(key.clone(), b"reg-params".to_vec())]);
    assert!(matches!(
        caps.grant(&AppIdentity::WebApp(app(1)), Capability::Background),
        Some(Grant::Granted { .. })
    ));
    assert_eq!(asked.load(Ordering::SeqCst), 1);

    let resp = answer(lp.send.send_to_handler(register(delegate, Some(app(1)))).await.unwrap());
    assert!(resp.is_ok(), "{resp:?}");
    lp.sync().await;
    lp.sync().await;
    assert_eq!(asked.load(Ordering::SeqCst), 1, "a granted app is never asked again");
    assert_eq!(lp.lifecycle_runs().len(), 1, "Installed is delivered once");
    assert_eq!(caps.stats.lifecycle_delivered.load(Ordering::Relaxed), 1);
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
    let resp = answer(lp.send.send_to_handler(register(delegate.clone(), Some(app(2)))).await.unwrap());
    assert!(resp.is_ok());
    wait_until("the prompt to be answered", || {
        caps.grant(&AppIdentity::WebApp(app(2)), Capability::Background).is_some()
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
            })
            .await
            .unwrap(),
    );
    assert!(resp.is_ok(), "{resp:?}");
    lp.sync().await;
    assert_eq!(asked.load(Ordering::SeqCst), 1, "a remote registration prompts nobody");
    assert!(caps.grant(&AppIdentity::WebApp(app(3)), Capability::Background).is_none());
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
    wait_until("the prompt to be raised", || asked.load(Ordering::SeqCst) == 1).await;
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
        .on_registered(granted.key(), &manifest_module(&started_only), b"granted", Some(AppIdentity::WebApp(app(5))))
        .unwrap();
    caps.record_answer(&p, true);
    let p = caps
        .on_registered(denied.key(), &manifest_module(&started_only), b"denied", Some(AppIdentity::WebApp(app(6))))
        .unwrap();
    caps.record_answer(&p, false);
    assert!(caps
        .on_registered(unattested.key(), &manifest_module(&started_only), b"unattested", None)
        .is_none());

    let lp = start(
        "cap_node_started",
        caps.clone(),
        vec![ScriptedRun::default(), ScriptedRun::default(), ScriptedRun::default()],
        CapabilityPrompter::answering(None),
    )
    .await;
    // Past the smear window (virtual time).
    tokio::time::sleep(super::delegate_capabilities::NODE_STARTED_MIN_DELAY
        + super::delegate_capabilities::NODE_STARTED_SMEAR
        + Duration::from_secs(1))
    .await;
    lp.sync().await;
    assert_eq!(
        lp.lifecycle_runs(),
        vec![(granted.key().clone(), b"granted".to_vec())]
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
    let p = caps
        .on_registered(&key, &manifest_module(&manifest), b"p", Some(AppIdentity::WebApp(app(7))))
        .unwrap();
    caps.record_answer(&p, true);

    let two_gets = || {
        ScriptedRun::from(vec![
            OutboundDelegateMsg::GetContractRequest(GetContractRequest::new(ContractInstanceId::new([0x51; 32]))),
            OutboundDelegateMsg::GetContractRequest(GetContractRequest::new(ContractInstanceId::new([0x52; 32]))),
        ])
    };
    // client run (2 GETs) + its follow-up, lifecycle run (2 GETs) + follow-up
    let lp = start(
        "cap_budget",
        caps.clone(),
        vec![two_gets(), ScriptedRun::default(), two_gets(), ScriptedRun::default()],
        CapabilityPrompter::answering(None),
    )
    .await;

    // Client-driven: not budgeted.
    let resp = answer(
        lp.send
            .send_to_handler(app_messages(
                &key,
                vec![InboundDelegateMsg::ApplicationMessage(ApplicationMessage::new(b"go".to_vec()))],
            ))
            .await
            .unwrap(),
    );
    assert!(resp.is_ok(), "{resp:?}");
    assert_eq!(caps.stats.refused_delegate_ops.load(Ordering::Relaxed), 0);

    // Unprompted: the second GET is refused.
    assert!(lp.caps.queue(LifecycleRun {
        key: key.clone(),
        event: LifecycleEvent::NodeStarted { down_since_ms: None },
    }));
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
    let p = caps
        .on_registered(&key, &manifest_module(&manifest), b"p", Some(AppIdentity::WebApp(app(8))))
        .unwrap();
    caps.record_answer(&p, true);
    caps.charge_duty(&key, Duration::from_secs(10));

    let lp = start("cap_duty", caps.clone(), vec![ScriptedRun::default()], CapabilityPrompter::answering(None)).await;
    assert!(caps.queue(LifecycleRun {
        key,
        event: LifecycleEvent::NodeStarted { down_since_ms: None },
    }));
    wait_until("the deferral", || {
        caps.stats.lifecycle_deferred_duty.load(Ordering::Relaxed) == 1
    })
    .await;
    lp.sync().await;
    assert!(lp.lifecycle_runs().is_empty());
    let _ = user_input::AutoApprovePrompter;
}

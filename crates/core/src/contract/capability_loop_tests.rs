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
    let lp = start(
        "forged_lifecycle",
        caps.clone(),
        vec![reply.clone(), reply],
        CapabilityPrompter::answering(None),
    )
    .await;
    let key = DelegateKey::new([3; 32], CodeHash::new([3; 32]));
    for forged in [
        InboundDelegateMsg::Lifecycle(LifecycleEvent::Installed),
        InboundDelegateMsg::WakeupFired { tag: vec![1] },
    ] {
        let resp = answer(
            lp.send
                .send_to_handler(app_messages(&key, vec![forged]))
                .await
                .unwrap(),
        );
        assert!(
            resp.is_err(),
            "forged host-only message must be refused, got {resp:?}"
        );
    }
    assert!(
        lp.observations.lock().unwrap().is_empty(),
        "the delegate must not have run"
    );
    assert_eq!(
        caps.stats.forged_lifecycle_refused.load(Ordering::Relaxed),
        2
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

/// `is_unprompted_run` classifies by `InterDelegateDispatch::Suppressed`, so
/// the budget is only as right as the set of callers passing it. Every
/// production use of `Suppressed` must sit in a function that runs a delegate
/// no client asked for, and those functions must be exactly the known ones:
/// a new caller has to be classified here deliberately.
#[test]
fn unprompted_runs_are_exactly_the_suppressed_ones() {
    let code = super::tests::production_code();
    let mut callers: Vec<String> = code
        .match_indices("InterDelegateDispatch::Suppressed")
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
        "a new InterDelegateDispatch::Suppressed caller must be classified as an unprompted run (or not) on purpose"
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
/// record; a remote one, another app's, or one with no app does not.
#[tokio::test]
async fn a_remote_unregister_keeps_the_record() {
    let caps = DelegateCapabilities::in_memory();
    let key = granted(&caps, vec![LifecycleKind::NodeStarted], b"p", 24);
    let lp = start(
        "cap_unregister",
        caps.clone(),
        vec![ScriptedRun::default(); 4],
        CapabilityPrompter::answering(None),
    )
    .await;
    let unregister = |scope, origin| ContractHandlerEvent::DelegateRequest {
        req: DelegateRequest::UnregisterDelegate(key.clone()),
        origin_contract: origin,
        connection_scope: scope,
        user_context: None,
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
        // The debt below is floored at half a burst (5 ms); at 10 us/s it is
        // repaid after 500 s.
        duty_refill_per_sec: Duration::from_micros(10),
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
    // least 3 skips, so the exact per-fire deferral count below cannot be
    // matched by a different retry count on any random draw.
    tokio::time::sleep(Duration::from_secs(66 + 5 * 72)).await;
    lp.sync().await;
    assert!(lp.wakeup_runs().is_empty(), "never runs over budget");
    let skipped = caps.stats.wakeups_skipped.load(Ordering::Relaxed);
    assert!(skipped >= 3, "each starved fire is skipped, got {skipped}");
    // Each skipped fire was deferred on its first attempt and on each of its
    // WAKEUP_MAX_DEFERRALS retries (one more fire may be part-way through).
    let deferred = caps.stats.wakeups_deferred.load(Ordering::Relaxed);
    let per_fire = u64::from(WAKEUP_MAX_DEFERRALS) + 1;
    assert!(
        deferred >= skipped * per_fire && deferred < (skipped + 1) * per_fire,
        "deferred {deferred}, skipped {skipped}"
    );
    // The debt is repaid (500 s), and the SAME schedule fires again: it was
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

/// What Harvest's design rests on, both halves: a wake-up run CAN write a
/// contract (its UPDATE goes through the unprompted-op admission, and a
/// refusal is answered), and CANNOT reach another delegate (the inter-delegate
/// hop is suppressed, as for every unprompted run).
#[tokio::test(start_paused = true)]
async fn a_wakeup_run_can_update_a_contract_but_not_message_a_delegate() {
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
    let lp = start(
        "wake_ops",
        caps.clone(),
        vec![wake_run, ScriptedRun::default(), ScriptedRun::default()],
        CapabilityPrompter::answering(None),
    )
    .await;
    tokio::time::sleep(Duration::from_secs(66)).await;
    lp.sync().await;
    wait_until("the wake-up's follow-up run", || {
        lp.observations.lock().unwrap().len() >= 2
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
    assert!(
        obs.iter().all(|o| o.delegate_key != other),
        "a wake-up run must not reach another delegate: {obs:?}"
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
    assert_eq!(
        lp.wakeup_runs(),
        vec![(key, b"p".to_vec())],
        "the schedule survived the unreadable fire"
    );
}

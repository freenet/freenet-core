//! The DELEGATE LANE: client delegate requests served off the serial
//! `contract_handling` loop.
//!
//! # The problem
//!
//! Every client delegate request used to run on `contract_handling`, the one
//! task that also runs every contract validate/update/summarize. A delegate
//! request that takes ~1.4 ms of WASM (a River chat-delegate signing request)
//! therefore waited behind whatever contract WASM was queued ahead of it. On
//! a production hosted node that was River room-contract work at ~250 ms per
//! call, several back to back, and delegate requests measured p99 855 ms,
//! p99.9 ~2 s, max 6.2 s over 22,260 real requests (2026-11-05).
//!
//! # The shape
//!
//! * Client `DelegateRequest`s go to the LANE task (a bounded intake channel
//!   installed into the contract handler channel's sender half), not to the
//!   loop. The lane runs a plain `ApplicationMessages` request itself.
//! * Every delegate `process()` on the node, lane or loop, runs on ONE
//!   executor dedicated to delegates and taken under its lock
//!   ([`DelegateRunner`]). So "at most one delegate `process()` executes
//!   node-wide at any instant" still holds, now by a lock instead of by the
//!   shape of the call graph, and every secret write still goes through one
//!   `SecretsStore` instance (its in-memory index and the hosted per-user
//!   quota both assume that; see `secrets_store::quota`).
//! * A run whose output needs the loop (a contract GET/PUT/UPDATE/SUBSCRIBE, an
//!   inter-delegate message, a user prompt) is HANDED OFF: the lane's first
//!   `process()` result goes to the loop as a [`LaneUnit`] and the loop
//!   continues the round-trip exactly as it would have, starting from that
//!   result. A request the lane does not run at all (registration,
//!   unregistration, a forged node-only inbound message) is FORWARDED the same
//!   way, so it is handled by the loop's existing code and keeps its place in
//!   the per-delegate order.
//!
//! # Per-delegate exclusion: [`DelegateGate`]
//!
//! `DelegateContextCache` is last-write-wins per delegate (and hosted user), so
//! a delegate's round-trip must never be interleaved with another run of the
//! SAME delegate (see `delegate_park`'s module docs). Before this lane that was
//! supplied by the serial loop plus the park registry. Now two tasks run
//! delegates, so the gate supplies it between them:
//!
//! * The lane runs delegate `D` only while `D` is entirely clear: no loop
//!   claim, no pending unit, no loop waiting for it.
//! * The loop CLAIMS `D` around every delegate run it starts (client request,
//!   contract notification, lifecycle run, park resume, handed-off unit) and
//!   for the inter-delegate hop target. A park entry holds a claim for as long
//!   as it lives, so a parked delegate stays excluded from the lane too.
//! * Claiming waits only for a lane run of `D` that is in progress (one
//!   `process()` call). The lane never waits on the loop while it is running,
//!   so that wait is bounded and cannot deadlock. If the lane's run left a
//!   pending unit for `D`, the claim TAKES it and the caller runs it first, so
//!   the handed-off continuation is never overtaken by a different run of `D`.
//!
//! Per-client order: the lane serves its queue in arrival order and never
//! reorders two requests for the same delegate (a request whose delegate is
//! busy blocks every later request for that delegate). Requests for different
//! delegates may overtake each other, as they could on the loop.
//!
//! # Bounds
//!
//! The intake channel is bounded ([`LANE_INTAKE_CAPACITY`]) and the sender
//! uses `try_send`; a full lane refuses the request (the client sees an error)
//! instead of queueing it anywhere unbounded. The lane holds at most
//! [`LANE_QUEUE_CAPACITY`] requests it has taken from the intake; past that it
//! stops reading, so the intake fills and refuses. Units to the loop are at
//! most one per delegate and are themselves drawn from that bounded queue.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use freenet_stdlib::client_api::{DelegateRequest, HostResponse};
use freenet_stdlib::prelude::{ContractInstanceId, DelegateKey, OutboundDelegateMsg};
use futures::future::BoxFuture;
use tokio::sync::Notify;

use super::ExecutorError;
use super::handler::{ContractHandlerEvent, EventId};
use crate::wasm_runtime::UserSecretContext;

/// Requests the intake channel buffers before `try_send` refuses.
pub(crate) const LANE_INTAKE_CAPACITY: usize = 128;

/// Requests the lane holds after taking them from the intake, waiting for
/// their delegate to be free. Past this the lane stops reading the intake.
pub(crate) const LANE_QUEUE_CAPACITY: usize = 256;

/// Runs one delegate `process()` call on the node's delegate executor.
///
/// Implementations MUST run every call on one executor under one lock: that
/// is what keeps "at most one delegate `process()` node-wide at any instant"
/// true now that two tasks start delegate runs. `RuntimePool` routes its own
/// `execute_delegate_request` / `execute_delegate_hop` through the same
/// runner, so the loop and the lane share the lock.
pub(crate) trait DelegateRunner: Send + Sync + 'static {
    fn run<'a>(
        &'a self,
        req: DelegateRequest<'a>,
        origin_contract: Option<&'a ContractInstanceId>,
        caller_delegate: Option<&'a DelegateKey>,
        connection_scope: crate::client_events::ConnectionScope,
        user_context: Option<&'a UserSecretContext>,
    ) -> BoxFuture<'a, Result<HostResponse, ExecutorError>>;
}

/// The client's response channel, the same one the loop's waiting-response
/// map holds for an ordinary event.
pub(crate) type Responder = tokio::sync::oneshot::Sender<(EventId, ContractHandlerEvent)>;

/// A client delegate request as it arrives at the lane.
pub(crate) struct LaneRequest {
    pub(crate) id: u64,
    pub(crate) responder: Responder,
    pub(crate) req: DelegateRequest<'static>,
    pub(crate) origin_contract: Option<ContractInstanceId>,
    pub(crate) connection_scope: crate::client_events::ConnectionScope,
    pub(crate) user_context: Option<UserSecretContext>,
}

/// Work the lane hands to the loop.
pub(crate) struct LaneUnit {
    pub(crate) key: DelegateKey,
    pub(crate) id: u64,
    pub(crate) responder: Responder,
    /// For a forwarded request, the request itself. For a continuation, the
    /// request with its inbound already consumed: only `key` and `params` are
    /// read, to build the round-trip's follow-up requests.
    pub(crate) req: DelegateRequest<'static>,
    pub(crate) origin_contract: Option<ContractInstanceId>,
    pub(crate) connection_scope: crate::client_events::ConnectionScope,
    pub(crate) user_context: Option<UserSecretContext>,
    /// `Some`: a continuation, carrying what the lane's `process()` call
    /// returned; the loop continues the round-trip from here without calling
    /// `process()` again. `None`: a forwarded request the loop runs from the
    /// start.
    pub(crate) first_values: Option<Vec<OutboundDelegateMsg>>,
}

#[derive(Default, Debug)]
struct Entry {
    /// The lane is inside a `process()` call for this delegate.
    lane_running: bool,
    /// A [`LaneUnit`] for this delegate is waiting in `Inner::units`.
    unit_pending: bool,
    /// Live [`LoopClaim`]s.
    loop_claims: usize,
    /// Loop claims waiting for a lane run to finish. While non-zero the lane
    /// starts no new run of this delegate, so a burst of client requests
    /// cannot starve the loop's claim.
    loop_waiting: usize,
}

impl Entry {
    fn is_clear(&self) -> bool {
        !self.lane_running && !self.unit_pending && self.loop_claims == 0 && self.loop_waiting == 0
    }
}

#[derive(Default)]
struct Inner {
    entries: HashMap<DelegateKey, Entry>,
    /// Units for the loop, oldest first. At most one per delegate.
    units: VecDeque<LaneUnit>,
}

impl Inner {
    fn remove_if_clear(&mut self, key: &DelegateKey) {
        if self.entries.get(key).is_some_and(Entry::is_clear) {
            self.entries.remove(key);
        }
    }

    fn take_unit(&mut self, key: &DelegateKey) -> Option<LaneUnit> {
        let pos = self.units.iter().position(|u| &u.key == key)?;
        self.units.remove(pos)
    }
}

/// Per-delegate exclusion between the lane and the loop. See the module docs.
#[derive(Default)]
pub(crate) struct DelegateGate {
    inner: std::sync::Mutex<Inner>,
    /// Loop -> lane: a delegate may have become free.
    lane_wake: Notify,
    /// Lane -> loop: a unit is waiting.
    units_ready: Notify,
    /// Lane -> loop claims waiting on a lane run: a lane run ended.
    lane_run_ended: Notify,
}

impl DelegateGate {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Wait until the lane is not running `key`, then claim it for the loop
    /// and take the delegate's pending unit, if any.
    ///
    /// The caller MUST run a returned unit before doing anything else with the
    /// delegate: it is the continuation of a round-trip already in progress.
    pub(crate) async fn claim(
        self: &Arc<Self>,
        key: &DelegateKey,
    ) -> (LoopClaim, Option<LaneUnit>) {
        self.wait_for_lane(key).await;
        let mut inner = self.lock();
        let entry = inner.entries.entry(key.clone()).or_default();
        entry.loop_waiting = entry.loop_waiting.saturating_sub(1);
        entry.loop_claims += 1;
        let unit = if entry.unit_pending {
            entry.unit_pending = false;
            inner.take_unit(key)
        } else {
            None
        };
        (
            LoopClaim {
                gate: self.clone(),
                key: key.clone(),
            },
            unit,
        )
    }

    /// [`Self::claim`] for a caller that cannot run a pending unit (the
    /// inter-delegate hop, which is itself inside a round-trip): `None` if the
    /// delegate has one, and nothing is claimed.
    pub(crate) async fn claim_unless_pending(
        self: &Arc<Self>,
        key: &DelegateKey,
    ) -> Option<LoopClaim> {
        self.wait_for_lane(key).await;
        let mut inner = self.lock();
        let entry = inner.entries.entry(key.clone()).or_default();
        entry.loop_waiting = entry.loop_waiting.saturating_sub(1);
        if entry.unit_pending {
            inner.remove_if_clear(key);
            drop(inner);
            self.lane_wake.notify_one();
            return None;
        }
        entry.loop_claims += 1;
        Some(LoopClaim {
            gate: self.clone(),
            key: key.clone(),
        })
    }

    /// Wait until the lane is not running `key`, leaving `loop_waiting`
    /// incremented for the caller to consume under the same lock it claims in.
    async fn wait_for_lane(&self, key: &DelegateKey) {
        let mut registered = false;
        loop {
            let ended = self.lane_run_ended.notified();
            tokio::pin!(ended);
            ended.as_mut().enable();
            {
                let mut inner = self.lock();
                let entry = inner.entries.entry(key.clone()).or_default();
                if !registered {
                    entry.loop_waiting += 1;
                    registered = true;
                }
                if !entry.lane_running {
                    return;
                }
            }
            ended.await;
        }
    }

    /// Claim `key` for the loop WITHOUT waiting. Only for a caller that already
    /// holds a claim on it (a park entry, taken while its run holds one).
    pub(crate) fn hold(self: &Arc<Self>, key: &DelegateKey) -> LoopClaim {
        let mut inner = self.lock();
        let entry = inner.entries.entry(key.clone()).or_default();
        debug_assert!(entry.loop_claims > 0, "hold() without a live loop claim");
        entry.loop_claims += 1;
        LoopClaim {
            gate: self.clone(),
            key: key.clone(),
        }
    }

    /// The oldest waiting unit, with its delegate claimed for the loop.
    pub(crate) fn take_next_unit(self: &Arc<Self>) -> Option<(LoopClaim, LaneUnit)> {
        let mut inner = self.lock();
        let unit = inner.units.pop_front()?;
        let entry = inner.entries.entry(unit.key.clone()).or_default();
        entry.unit_pending = false;
        entry.loop_claims += 1;
        let claim = LoopClaim {
            gate: self.clone(),
            key: unit.key.clone(),
        };
        Some((claim, unit))
    }

    /// Resolves when a unit may be waiting. Wake-only: the loop takes units
    /// with [`Self::take_next_unit`].
    pub(crate) async fn units_ready(&self) {
        self.units_ready.notified().await;
    }

    fn try_begin_lane_run(self: &Arc<Self>, key: &DelegateKey) -> Option<LaneRun> {
        let mut inner = self.lock();
        let entry = inner.entries.entry(key.clone()).or_default();
        if !entry.is_clear() {
            return None;
        }
        entry.lane_running = true;
        Some(LaneRun {
            gate: self.clone(),
            key: key.clone(),
            ended: false,
        })
    }

    fn try_forward(&self, unit: LaneUnit) -> Result<(), LaneUnit> {
        let mut inner = self.lock();
        let entry = inner.entries.entry(unit.key.clone()).or_default();
        if !entry.is_clear() {
            return Err(unit);
        }
        entry.unit_pending = true;
        inner.units.push_back(unit);
        drop(inner);
        self.units_ready.notify_one();
        Ok(())
    }

    fn is_free(&self, key: &DelegateKey) -> bool {
        self.lock().entries.get(key).is_none_or(Entry::is_clear)
    }

    fn end_lane_run(&self, key: &DelegateKey, unit: Option<LaneUnit>) {
        let mut inner = self.lock();
        let handed_off = unit.is_some();
        if let Some(entry) = inner.entries.get_mut(key) {
            entry.lane_running = false;
            if let Some(unit) = unit {
                entry.unit_pending = true;
                inner.units.push_back(unit);
            }
        }
        inner.remove_if_clear(key);
        drop(inner);
        if handed_off {
            self.units_ready.notify_one();
        }
        self.lane_run_ended.notify_waiters();
    }

    #[cfg(test)]
    pub(crate) fn is_tracking(&self, key: &DelegateKey) -> bool {
        self.lock().entries.contains_key(key)
    }
}

/// The loop's hold on a delegate. While any is live the lane does not run it.
pub(crate) struct LoopClaim {
    gate: Arc<DelegateGate>,
    key: DelegateKey,
}

impl Drop for LoopClaim {
    fn drop(&mut self) {
        let mut inner = self.gate.lock();
        if let Some(entry) = inner.entries.get_mut(&self.key) {
            entry.loop_claims = entry.loop_claims.saturating_sub(1);
        }
        inner.remove_if_clear(&self.key);
        drop(inner);
        self.gate.lane_wake.notify_one();
    }
}

/// The lane's hold on a delegate for one `process()` call. Ends the run on
/// drop, so a lane task that dies mid-call cannot leave the loop waiting.
struct LaneRun {
    gate: Arc<DelegateGate>,
    key: DelegateKey,
    ended: bool,
}

impl LaneRun {
    fn finish(mut self) {
        self.ended = true;
        self.gate.end_lane_run(&self.key, None);
    }

    fn hand_off(mut self, unit: LaneUnit) {
        self.ended = true;
        self.gate.end_lane_run(&self.key, Some(unit));
    }
}

impl Drop for LaneRun {
    fn drop(&mut self) {
        if !self.ended {
            self.gate.end_lane_run(&self.key, None);
        }
    }
}

/// Logs when the lane task ends for any reason other than its intake closing,
/// so a lane that died is visible. Requests then fall back to the loop (the
/// sender sees a closed intake).
struct ExitLog {
    clean: bool,
}

impl Drop for ExitLog {
    fn drop(&mut self) {
        if !self.clean {
            tracing::error!(
                "Delegate lane task ended unexpectedly; client delegate requests fall \
                 back to the contract loop"
            );
        }
    }
}

/// Whether the lane runs this request itself. Anything else is forwarded to
/// the loop, which owns the registration hooks and the forged-inbound refusal.
fn lane_runs_itself(req: &DelegateRequest<'_>) -> bool {
    matches!(req, DelegateRequest::ApplicationMessages { .. })
        && super::node_only_inbound(req).is_none()
}

/// Whether a `process()` result can be answered without the loop: only
/// messages that the round-trip returns to the client verbatim.
fn completes_without_loop(values: &[OutboundDelegateMsg]) -> bool {
    values.iter().all(|m| {
        matches!(
            m,
            OutboundDelegateMsg::ApplicationMessage(_) | OutboundDelegateMsg::ContextUpdated(_)
        )
    })
}

/// The continuation request: same key and params, no inbound (already
/// consumed by the lane's `process()` call).
fn continuation_request(req: &DelegateRequest<'static>) -> DelegateRequest<'static> {
    #[allow(clippy::wildcard_enum_match_arm)]
    match req {
        DelegateRequest::ApplicationMessages { key, params, .. } => {
            DelegateRequest::ApplicationMessages {
                key: key.clone(),
                params: params.clone(),
                inbound: Vec::new(),
            }
        }
        // Unreachable: only `ApplicationMessages` is run by the lane.
        other => other.clone(),
    }
}

fn respond(
    responder: Responder,
    id: u64,
    response: Result<Vec<OutboundDelegateMsg>, ExecutorError>,
) {
    // A dropped receiver means the client went away; the delegate already ran.
    let _ = responder.send((
        EventId { id },
        ContractHandlerEvent::DelegateResponse(response),
    ));
}

/// The lane task. Runs until its intake closes (the node's sender half is
/// gone) or it is aborted with the contract loop.
pub(crate) async fn run_lane(
    runner: Arc<dyn DelegateRunner>,
    gate: Arc<DelegateGate>,
    mut intake: tokio::sync::mpsc::Receiver<LaneRequest>,
) {
    let mut exit = ExitLog { clean: false };
    let mut queue: VecDeque<LaneRequest> = VecDeque::new();
    loop {
        while queue.len() < LANE_QUEUE_CAPACITY {
            match intake.try_recv() {
                Ok(req) => queue.push_back(req),
                Err(_) => break,
            }
        }

        if let Some(idx) = next_runnable(&queue, &gate) {
            let request = queue.remove(idx).expect("index from next_runnable");
            if let Err(request) = serve(&runner, &gate, request).await {
                // The loop claimed the delegate between the scan and here.
                // Back in its place: nothing for that delegate ran meanwhile.
                queue.insert(idx, request);
            }
            continue;
        }

        let wake = gate.lane_wake.notified();
        tokio::select! {
            req = intake.recv(), if queue.len() < LANE_QUEUE_CAPACITY => match req {
                Some(req) => queue.push_back(req),
                None => {
                    exit.clean = true;
                    return;
                }
            },
            () = wake => {}
        }
    }
}

/// The first queued request whose delegate is free and has no earlier request
/// still queued.
fn next_runnable(queue: &VecDeque<LaneRequest>, gate: &DelegateGate) -> Option<usize> {
    let mut blocked: HashSet<&DelegateKey> = HashSet::new();
    for (idx, request) in queue.iter().enumerate() {
        let key = request.req.key();
        if blocked.contains(key) {
            continue;
        }
        if gate.is_free(key) {
            return Some(idx);
        }
        blocked.insert(key);
    }
    None
}

/// Serve one request: run it, forward it, or hand it back if its delegate
/// stopped being free since the scan.
async fn serve(
    runner: &Arc<dyn DelegateRunner>,
    gate: &Arc<DelegateGate>,
    request: LaneRequest,
) -> Result<(), LaneRequest> {
    let key = request.req.key().clone();
    if !lane_runs_itself(&request.req) {
        let LaneRequest {
            id,
            responder,
            req,
            origin_contract,
            connection_scope,
            user_context,
        } = request;
        let unit = LaneUnit {
            key,
            id,
            responder,
            req,
            origin_contract,
            connection_scope,
            user_context,
            first_values: None,
        };
        return gate.try_forward(unit).map_err(|unit| LaneRequest {
            id: unit.id,
            responder: unit.responder,
            req: unit.req,
            origin_contract: unit.origin_contract,
            connection_scope: unit.connection_scope,
            user_context: unit.user_context,
        });
    }

    let Some(run) = gate.try_begin_lane_run(&key) else {
        return Err(request);
    };
    let LaneRequest {
        id,
        responder,
        req,
        origin_contract,
        connection_scope,
        user_context,
    } = request;
    let continuation = continuation_request(&req);
    tracing::debug!(delegate_key = %key, "Processing delegate request on the delegate lane");
    let result = runner
        .run(
            req,
            origin_contract.as_ref(),
            None,
            connection_scope,
            user_context.as_ref(),
        )
        .await;
    match super::delegate_response_values(result, &key) {
        Err(err) => {
            run.finish();
            respond(responder, id, Err(err));
        }
        Ok(values) if completes_without_loop(&values) => {
            run.finish();
            respond(responder, id, Ok(values));
        }
        Ok(values) => {
            tracing::debug!(
                delegate_key = %key,
                "Delegate output needs the contract loop; handing the round-trip off"
            );
            run.hand_off(LaneUnit {
                key,
                id,
                responder,
                req: continuation,
                origin_contract,
                connection_scope,
                user_context,
                first_values: Some(values),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn key(byte: u8) -> DelegateKey {
        DelegateKey::new(
            [byte; 32],
            freenet_stdlib::prelude::CodeHash::new([byte; 32]),
        )
    }

    fn unit(k: &DelegateKey) -> LaneUnit {
        let (responder, _) = tokio::sync::oneshot::channel();
        LaneUnit {
            key: k.clone(),
            id: 0,
            responder,
            req: DelegateRequest::UnregisterDelegate(k.clone()),
            origin_contract: None,
            connection_scope: crate::client_events::ConnectionScope::Local,
            user_context: None,
            first_values: None,
        }
    }

    #[tokio::test]
    async fn a_loop_claim_excludes_the_lane_until_dropped() {
        let gate = DelegateGate::new();
        let k = key(1);
        let (claim, pending) = gate.claim(&k).await;
        assert!(pending.is_none());
        assert!(
            gate.try_begin_lane_run(&k).is_none(),
            "claimed: lane must not run"
        );
        assert!(
            gate.try_forward(unit(&k)).is_err(),
            "claimed: lane must not forward"
        );
        drop(claim);
        assert!(!gate.is_tracking(&k), "a clear entry is removed");
        let run = gate
            .try_begin_lane_run(&k)
            .expect("free after the claim drops");
        run.finish();
        assert!(!gate.is_tracking(&k));
    }

    #[tokio::test]
    async fn a_claim_waits_for_the_lane_run_and_takes_its_handoff() {
        let gate = DelegateGate::new();
        let k = key(2);
        let run = gate.try_begin_lane_run(&k).expect("free");
        let g2 = gate.clone();
        let k2 = k.clone();
        let claim = tokio::spawn(async move { g2.claim(&k2).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!claim.is_finished(), "the claim must wait for the lane run");
        run.hand_off(unit(&k));
        let (_claim, pending) = tokio::time::timeout(Duration::from_secs(5), claim)
            .await
            .expect("claim resolves once the run ends")
            .expect("join");
        assert!(
            pending.is_some(),
            "the handed-off continuation must go to the claimer, not wait behind it"
        );
        assert!(gate.take_next_unit().is_none(), "taken exactly once");
    }

    #[tokio::test]
    async fn a_waiting_loop_claim_blocks_new_lane_runs() {
        let gate = DelegateGate::new();
        let k = key(3);
        let run = gate.try_begin_lane_run(&k).expect("free");
        let g2 = gate.clone();
        let k2 = k.clone();
        let claim = tokio::spawn(async move { g2.claim(&k2).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        run.finish();
        // The lane may get here before the claimer wakes; it must not start
        // another run of the delegate ahead of the waiting claim.
        assert!(gate.try_begin_lane_run(&k).is_none());
        let (c, _) = tokio::time::timeout(Duration::from_secs(5), claim)
            .await
            .unwrap()
            .unwrap();
        drop(c);
        assert!(gate.try_begin_lane_run(&k).is_some());
    }

    #[tokio::test]
    async fn the_hop_claim_refuses_a_delegate_with_a_pending_unit() {
        let gate = DelegateGate::new();
        let k = key(4);
        gate.try_forward(unit(&k)).ok().expect("free");
        assert!(gate.claim_unless_pending(&k).await.is_none());
        let (claim, u) = gate.take_next_unit().expect("still pending");
        assert_eq!(u.key, k);
        drop(claim);
        assert!(gate.claim_unless_pending(&k).await.is_some());
    }

    #[tokio::test]
    async fn a_lane_run_dropped_mid_call_releases_the_delegate() {
        let gate = DelegateGate::new();
        let k = key(5);
        drop(gate.try_begin_lane_run(&k).expect("free"));
        let (_c, pending) = tokio::time::timeout(Duration::from_secs(5), gate.claim(&k))
            .await
            .expect("a dead lane run must not strand the loop");
        assert!(pending.is_none());
    }
}

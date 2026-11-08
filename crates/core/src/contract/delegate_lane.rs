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
//! The lane runs only what it can run RIGHT NOW, and hands everything else
//! to the loop through the loop's ordinary path, so the loop's own reviewed
//! bounds apply to it unchanged.
//!
//! * Client `DelegateRequest`s go to the LANE task (a bounded intake channel
//!   installed into the contract handler channel's sender half). The lane
//!   takes them strictly in arrival order, one at a time.
//! * An `ApplicationMessages` request whose delegate is FREE (see
//!   [`DelegateGate`]) runs on the lane, when fewer than
//!   [`MAX_PENDING_UNITS`] continuations are waiting for the loop.
//! * Every other request is FORWARDED, unchanged (same event id, priority,
//!   connection scope, user and responder), into the loop's event channel,
//!   exactly where the handler sent it before this lane existed
//!   ([`LoopForward`]). There it gets the fair queue's capacity, per-slot cap
//!   and `CLIENT_LOCAL_RESERVE`, and, for a parked delegate, the park's
//!   `queue_pending` cap and byte budget. A request the fair queue refuses
//!   gets exactly the refusal it got before. This covers registration,
//!   unregistration and forged node-only inbound messages (which the lane
//!   never runs), and requests whose delegate is busy.
//! * Every delegate `process()` on the node, lane or loop, runs on ONE
//!   executor dedicated to delegates and taken under its lock
//!   ([`DelegateRunner`]). So "at most one delegate `process()` executes
//!   node-wide at any instant" still holds, now by a lock instead of by the
//!   shape of the call graph, and every secret write still goes through one
//!   `SecretsStore` instance (its in-memory index and the hosted per-user
//!   quota both assume that; see `secrets_store::quota`).
//! * A lane run whose output needs the loop (a contract GET/PUT/UPDATE/
//!   SUBSCRIBE, an inter-delegate message, a user prompt) is HANDED OFF: the
//!   lane's first `process()` result goes to the loop as a [`LaneUnit`] and the
//!   loop continues the round-trip exactly as it would have, starting from
//!   that result. Units are only such continuations: at most one per
//!   delegate, and at most [`MAX_PENDING_UNITS`] in all, because the lane
//!   checks that bound BEFORE starting a run; at the bound it forwards
//!   instead, which is the pre-lane behaviour, not a refusal or a stall.
//!
//! # Per-delegate exclusion and order: [`DelegateGate`]
//!
//! `DelegateContextCache` is last-write-wins per delegate (and hosted user), so
//! a delegate's round-trip must never be interleaved with another run of the
//! SAME delegate (see `delegate_park`'s module docs). Before this lane that was
//! supplied by the serial loop plus the park registry. Now two tasks run
//! delegates, so the gate supplies it between them. The lane runs delegate
//! `D` only while `D` is entirely FREE:
//!
//! * no loop CLAIM: the loop claims `D` around every delegate run it starts
//!   (client request, contract notification, lifecycle run, park resume,
//!   handed-off unit, inter-delegate hop target), and a park entry holds a
//!   claim for as long as it lives;
//! * no pending unit for `D`, and no loop claim waiting for a lane run of `D`;
//! * no LOOP-OWNED request for `D`: a request the lane forwarded for `D` and
//!   the loop has not finished. The lane marks it before forwarding and the
//!   loop's `DelegateRequest` arm (or the fair queue's refusal) releases it
//!   ([`DelegateGate::take_forwarded`]). So once one request for `D` is
//!   forwarded, every later one is forwarded too until the loop has done the
//!   earlier ones, and none overtakes it: the loop's channel and the fair
//!   queue's slot for client delegate requests are both FIFO.
//!
//! Claiming waits only for a lane run of `D` that is in progress (one
//! `process()` call). The lane never waits on the loop, so that wait is
//! bounded and cannot deadlock. If the lane's run left a pending unit for
//! `D`, the claim TAKES it and the caller runs it first, so the handed-off
//! continuation is never overtaken by a different run of `D`.
//!
//! # Bounds
//!
//! The intake channel is bounded ([`LANE_INTAKE_CAPACITY`]) and the sender
//! uses `try_send`; a full intake refuses the request (the client sees an
//! error). It fills only if the lane itself, at about a millisecond per run,
//! is flooded: the lane holds nothing else. Units are bounded as above.
//!
//! # Ordering caveats
//!
//! * Startup: a request sent before `contract_handling` installs the lane
//!   reaches the loop, and a later request to the same delegate, sent after
//!   installation, reaches the lane and may run first. Only requests already
//!   in flight while the node starts are affected.
//! * The gate is per delegate KEY, not per (key, hosted user), the same
//!   granularity as the park registry: one user's round-trip on a shared
//!   delegate key sends every other user's requests for that key to the loop
//!   until it finishes.

use std::collections::{HashMap, VecDeque};
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

/// Continuations ([`LaneUnit`]s) that may wait for the loop at once. Checked
/// before the lane starts a run: at the bound the request is forwarded to the
/// loop instead, as before this lane.
pub(crate) const MAX_PENDING_UNITS: usize = 16;

/// Count one refused client delegate request (telemetry); returns the
/// running total.
pub(crate) fn record_refusal() -> u64 {
    crate::node::network_status::record_delegate_lane_refusal()
}

/// Whether a refusal with this running total is logged: the first, then at
/// each power of two, so a flood of refusals is a handful of lines with the
/// total in each rather than one line per request.
pub(crate) fn log_this_refusal(total: u64) -> bool {
    total.is_power_of_two()
}

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

/// Hands a request the lane does not run to the loop's ordinary
/// `DelegateRequest` path. Implemented by `handler::LoopForwarder`.
pub(crate) trait LoopForward: Send + Sync + 'static {
    fn forward(&self, request: LaneRequest);
}

/// The client's response channel, the same one the loop's waiting-response
/// map holds for an ordinary event.
pub(crate) type Responder = tokio::sync::oneshot::Sender<(EventId, ContractHandlerEvent)>;

/// A client delegate request as it arrives at the lane: everything the loop
/// needs to receive it exactly as the handler would have sent it.
pub(crate) struct LaneRequest {
    pub(crate) id: u64,
    pub(crate) responder: Responder,
    pub(crate) req: DelegateRequest<'static>,
    pub(crate) origin_contract: Option<ContractInstanceId>,
    pub(crate) connection_scope: crate::client_events::ConnectionScope,
    pub(crate) user_context: Option<UserSecretContext>,
    pub(crate) priority: super::fair_queue::Priority,
}

/// The rest of a round-trip the lane started, handed to the loop.
pub(crate) struct LaneUnit {
    pub(crate) key: DelegateKey,
    pub(crate) id: u64,
    pub(crate) responder: Responder,
    /// The request with its inbound already consumed: only `key` and
    /// `params` are read, to build the round-trip's follow-up requests.
    pub(crate) req: DelegateRequest<'static>,
    pub(crate) origin_contract: Option<ContractInstanceId>,
    pub(crate) connection_scope: crate::client_events::ConnectionScope,
    pub(crate) user_context: Option<UserSecretContext>,
    /// What the lane's `process()` call returned; the loop continues the
    /// round-trip from here without calling `process()` again.
    pub(crate) first_values: Vec<OutboundDelegateMsg>,
}

#[derive(Default, Debug)]
struct Entry {
    /// The lane is inside a `process()` call for this delegate.
    lane_running: bool,
    /// A [`LaneUnit`] for this delegate is waiting in `Inner::units`.
    unit_pending: bool,
    /// Live [`LoopClaim`]s (a park entry's included).
    loop_claims: usize,
    /// Loop claims waiting for a lane run to finish. While non-zero the lane
    /// starts no new run of this delegate, so a burst of client requests
    /// cannot starve the loop's claim.
    loop_waiting: usize,
    /// Requests for this delegate the lane forwarded and the loop has not
    /// finished ([`LoopOwned`]).
    loop_owned: usize,
}

impl Entry {
    fn is_clear(&self) -> bool {
        !self.lane_running
            && !self.unit_pending
            && self.loop_claims == 0
            && self.loop_waiting == 0
            && self.loop_owned == 0
    }
}

#[derive(Default)]
struct Inner {
    entries: HashMap<DelegateKey, Entry>,
    /// Units for the loop, oldest first. At most one per delegate and
    /// [`MAX_PENDING_UNITS`] in all.
    units: VecDeque<LaneUnit>,
    /// Forwarded requests the loop has not finished, by event id.
    forwarded: HashMap<u64, DelegateKey>,
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

/// Per-delegate exclusion and order between the lane and the loop. See the
/// module docs.
///
/// One `std::sync::Mutex` over a `HashMap` rather than a `DashMap`: handing a
/// unit off updates a delegate's entry and the cross-delegate unit queue
/// together, and a claim reads both. The lock is never held across an
/// `.await`, and each critical section is a few map operations.
#[derive(Default)]
pub(crate) struct DelegateGate {
    inner: std::sync::Mutex<Inner>,
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
        let waiting = self.wait_for_lane(key).await;
        let mut inner = self.lock();
        waiting.finish(&mut inner);
        let entry = inner.entries.entry(key.clone()).or_default();
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

    /// [`Self::claim`] for a caller that may not run a pending unit (an
    /// inter-delegate hop from a round-trip that is itself nested inside
    /// another hop's unit run; see the hop in
    /// `handle_delegate_with_contract_requests`): `None` if the delegate has
    /// one, and nothing is claimed.
    pub(crate) async fn claim_unless_pending(
        self: &Arc<Self>,
        key: &DelegateKey,
    ) -> Option<LoopClaim> {
        let waiting = self.wait_for_lane(key).await;
        let mut inner = self.lock();
        waiting.finish(&mut inner);
        let entry = inner.entries.entry(key.clone()).or_default();
        if entry.unit_pending {
            inner.remove_if_clear(key);
            return None;
        }
        entry.loop_claims += 1;
        Some(LoopClaim {
            gate: self.clone(),
            key: key.clone(),
        })
    }

    /// Wait until the lane is not running `key`. Returns with the caller
    /// still registered as waiting (so the lane cannot start the delegate
    /// again first); the caller ends that under the same lock it claims in.
    /// Cancellation-safe: dropping the future, or the returned token, ends the
    /// registration.
    async fn wait_for_lane(self: &Arc<Self>, key: &DelegateKey) -> Waiting {
        let waiting = {
            let mut inner = self.lock();
            inner.entries.entry(key.clone()).or_default().loop_waiting += 1;
            Waiting {
                gate: self.clone(),
                key: key.clone(),
                done: false,
            }
        };
        loop {
            let ended = self.lane_run_ended.notified();
            tokio::pin!(ended);
            ended.as_mut().enable();
            if !self
                .lock()
                .entries
                .get(key)
                .is_some_and(|entry| entry.lane_running)
            {
                return waiting;
            }
            ended.await;
        }
    }

    /// Claim `key` for a PARK ENTRY, without waiting. Only for a caller that
    /// already holds a claim on it (the park is taken while its run holds
    /// one).
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

    /// Whether the loop has a unit waiting. Lets the loop skip its blocking
    /// wait while one is: `units_ready` is a `Notify`, which stores at most
    /// one permit, so after a capped drain the wake for the rest may already
    /// have been spent.
    pub(crate) fn has_pending_unit(&self) -> bool {
        !self.lock().units.is_empty()
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

    /// Start a lane run of `key` if it is free and the unit bound has room
    /// (a run may hand off). `None` means: forward the request instead.
    fn try_begin_lane_run(self: &Arc<Self>, key: &DelegateKey) -> Option<LaneRun> {
        let mut inner = self.lock();
        if inner.units.len() >= MAX_PENDING_UNITS {
            return None;
        }
        let entry = inner.entries.entry(key.clone()).or_default();
        if !entry.is_clear() {
            inner.remove_if_clear(key);
            return None;
        }
        entry.lane_running = true;
        Some(LaneRun {
            gate: self.clone(),
            key: key.clone(),
            ended: false,
        })
    }

    /// Record that the request `id` for `key` is being forwarded to the
    /// loop. Called BEFORE forwarding, so the loop cannot finish it first.
    fn mark_loop_owned(&self, key: &DelegateKey, id: u64) {
        let mut inner = self.lock();
        inner.entries.entry(key.clone()).or_default().loop_owned += 1;
        inner.forwarded.insert(id, key.clone());
    }

    /// The loop's side of a forwarded request: `Some` if the lane forwarded
    /// event `id`. Hold the guard until the loop is done with the request (a
    /// refusal counts); dropping it lets the lane run the delegate again.
    pub(crate) fn take_forwarded(self: &Arc<Self>, id: u64) -> Option<LoopOwned> {
        let key = self.lock().forwarded.remove(&id)?;
        Some(LoopOwned {
            gate: self.clone(),
            key,
        })
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

    /// Leave `unit` pending for the loop, as the lane does after handing a
    /// round-trip off. The delegate must be free.
    #[cfg(test)]
    pub(crate) fn hand_off_for_test(self: &Arc<Self>, unit: LaneUnit) {
        let key = unit.key.clone();
        let mut inner = self.lock();
        let entry = inner.entries.entry(key).or_default();
        assert!(entry.is_clear(), "the delegate must be free");
        entry.unit_pending = true;
        inner.units.push_back(unit);
        drop(inner);
        self.units_ready.notify_one();
    }

    #[cfg(test)]
    pub(crate) fn pending_units(&self) -> usize {
        self.lock().units.len()
    }
}

/// A loop claim waiting for a lane run to end. See [`DelegateGate::wait_for_lane`].
struct Waiting {
    gate: Arc<DelegateGate>,
    key: DelegateKey,
    done: bool,
}

impl Waiting {
    /// End the registration under a lock the caller already holds.
    fn finish(mut self, inner: &mut Inner) {
        self.done = true;
        if let Some(entry) = inner.entries.get_mut(&self.key) {
            entry.loop_waiting = entry.loop_waiting.saturating_sub(1);
        }
    }
}

impl Drop for Waiting {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        let mut inner = self.gate.lock();
        if let Some(entry) = inner.entries.get_mut(&self.key) {
            entry.loop_waiting = entry.loop_waiting.saturating_sub(1);
        }
        inner.remove_if_clear(&self.key);
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
    }
}

/// A forwarded request the loop holds. See [`DelegateGate::take_forwarded`].
pub(crate) struct LoopOwned {
    gate: Arc<DelegateGate>,
    key: DelegateKey,
}

impl Drop for LoopOwned {
    fn drop(&mut self) {
        let mut inner = self.gate.lock();
        if let Some(entry) = inner.entries.get_mut(&self.key) {
            entry.loop_owned = entry.loop_owned.saturating_sub(1);
        }
        inner.remove_if_clear(&self.key);
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

/// Logs the lane task being DROPPED before it finished, which happens only
/// when `contract_handling` aborts it on the way out (node shutdown, or the
/// loop returning an error). Not an error: the loop that owns the lane is
/// gone too. A lane that PANICS is reported by [`run_lane`] itself, at error.
struct AbortLog {
    finished: bool,
}

impl Drop for AbortLog {
    fn drop(&mut self) {
        if !self.finished {
            tracing::debug!("Delegate lane task stopped with the contract loop");
        }
    }
}

/// Whether the lane may run this request itself. Anything else is forwarded
/// to the loop, which owns the registration hooks and the forged-inbound
/// refusal.
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
    if responder
        .send((
            EventId { id },
            ContractHandlerEvent::DelegateResponse(response),
        ))
        .is_err()
    {
        tracing::debug!(id, "Delegate lane: the client went away before its answer");
    }
}

/// The lane task. Runs until its intake closes (the node's sender half is
/// gone) or it is aborted with the contract loop.
///
/// A panic inside ONE delegate run is caught in [`serve`]: that request's
/// client gets an error and the lane carries on. A panic anywhere else in the
/// lane is caught here, logged at error, and ends the lane: its intake closes
/// with it, so every later client delegate request falls back to the contract
/// loop (the sender sees a closed intake). The panic is not re-raised: nothing
/// awaits the lane's handle, so re-raising would only lose the message. An
/// abort is not a panic and is logged at debug by [`AbortLog`].
pub(crate) async fn run_lane(
    runner: Arc<dyn DelegateRunner>,
    gate: Arc<DelegateGate>,
    forward: Arc<dyn LoopForward>,
    mut intake: tokio::sync::mpsc::Receiver<LaneRequest>,
) {
    use futures::FutureExt;
    let mut exit = AbortLog { finished: false };
    let outcome = std::panic::AssertUnwindSafe(async {
        while let Some(request) = intake.recv().await {
            serve(&runner, &gate, &*forward, request).await;
        }
    })
    .catch_unwind()
    .await;
    exit.finished = true;
    match outcome {
        Ok(()) => tracing::debug!("Delegate lane task ended: its intake closed"),
        Err(payload) => {
            let reason = panic_reason(&*payload);
            crate::node::network_status::record_delegate_lane_panic();
            tracing::error!(
                %reason,
                "Delegate lane task PANICKED; client delegate requests fall back to \
                 the contract loop for the rest of this process"
            );
        }
    }
}

fn panic_reason(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-string panic payload".to_string())
}

/// Serve one request: run it on the lane if its delegate is free and the unit
/// bound has room, else forward it to the loop (see the module docs).
async fn serve(
    runner: &Arc<dyn DelegateRunner>,
    gate: &Arc<DelegateGate>,
    forward: &dyn LoopForward,
    request: LaneRequest,
) {
    let key = request.req.key().clone();
    let run = if lane_runs_itself(&request.req) {
        gate.try_begin_lane_run(&key)
    } else {
        None
    };
    let Some(run) = run else {
        tracing::debug!(
            delegate_key = %key,
            "Delegate lane: forwarding a request to the contract loop"
        );
        gate.mark_loop_owned(&key, request.id);
        forward.forward(request);
        return;
    };
    let LaneRequest {
        id,
        responder,
        req,
        origin_contract,
        connection_scope,
        user_context,
        priority: _,
    } = request;
    let continuation = continuation_request(&req);
    tracing::debug!(delegate_key = %key, "Processing delegate request on the delegate lane");
    // A panic inside this one run fails this one request, not the lane: the
    // client is answered with an error and the run's gate entry is released
    // (`LaneRun` drops). The runner itself marks its executor for
    // replacement before the next use (`PooledDelegateRunner`).
    //
    // CAVEAT: only the executor is replaced. Node state a host-side panic may
    // have left half-updated stays as it was: the delegate's
    // `DelegateContextCache` entry (read before the run, written after), the
    // inherited-origins map, the created-delegate counter, and an on-disk
    // secret or its quota counter if the panic hit a secret write. Before the
    // lane such a panic ended the contract loop and so the node. The locks
    // involved are poison-tolerant, so nothing cascades; a delegate whose
    // run panicked may see a stale or missing context on its next run. WASM
    // traps are not panics (they are errors), so this needs a host bug.
    let result = {
        use futures::FutureExt;
        std::panic::AssertUnwindSafe(runner.run(
            req,
            origin_contract.as_ref(),
            None,
            connection_scope,
            user_context.as_ref(),
        ))
        .catch_unwind()
        .await
    };
    let result = match result {
        Ok(result) => result,
        Err(payload) => {
            let reason = panic_reason(&*payload);
            crate::node::network_status::record_delegate_lane_panic();
            tracing::error!(
                delegate_key = %key,
                %reason,
                "A delegate run PANICKED on the delegate lane; answering its client \
                 with an error and continuing"
            );
            Err(ExecutorError::other(anyhow::anyhow!(
                "the delegate run panicked: {reason}"
            )))
        }
    };
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
                first_values: values,
            });
        }
    }
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
        let (responder, rx) = tokio::sync::oneshot::channel();
        std::mem::forget(rx);
        LaneUnit {
            key: k.clone(),
            id: 0,
            responder,
            req: DelegateRequest::ApplicationMessages {
                key: k.clone(),
                params: freenet_stdlib::prelude::Parameters::from(Vec::new()),
                inbound: Vec::new(),
            },
            origin_contract: None,
            connection_scope: crate::client_events::ConnectionScope::Local,
            user_context: None,
            first_values: Vec::new(),
        }
    }

    fn request_with(
        k: &DelegateKey,
        id: u64,
        req: DelegateRequest<'static>,
    ) -> (
        LaneRequest,
        tokio::sync::oneshot::Receiver<(EventId, ContractHandlerEvent)>,
    ) {
        let (responder, rx) = tokio::sync::oneshot::channel();
        let _ = k;
        (
            LaneRequest {
                id,
                responder,
                req,
                origin_contract: None,
                connection_scope: crate::client_events::ConnectionScope::Local,
                user_context: None,
                priority: super::super::fair_queue::Priority::ClientLocal,
            },
            rx,
        )
    }

    fn request(
        k: &DelegateKey,
        id: u64,
    ) -> (
        LaneRequest,
        tokio::sync::oneshot::Receiver<(EventId, ContractHandlerEvent)>,
    ) {
        request_with(
            k,
            id,
            DelegateRequest::ApplicationMessages {
                key: k.clone(),
                params: freenet_stdlib::prelude::Parameters::from(Vec::new()),
                inbound: Vec::new(),
            },
        )
    }

    /// Records forwarded requests, standing in for the loop's channel.
    struct Collect(tokio::sync::mpsc::UnboundedSender<LaneRequest>);
    impl LoopForward for Collect {
        fn forward(&self, request: LaneRequest) {
            self.0.send(request).expect("test receiver alive");
        }
    }

    /// Answers every request with an empty response and counts the runs.
    struct Counts(std::sync::atomic::AtomicUsize);
    impl DelegateRunner for Counts {
        fn run<'a>(
            &'a self,
            req: DelegateRequest<'a>,
            _origin_contract: Option<&'a ContractInstanceId>,
            _caller_delegate: Option<&'a DelegateKey>,
            _connection_scope: crate::client_events::ConnectionScope,
            _user_context: Option<&'a UserSecretContext>,
        ) -> BoxFuture<'a, Result<HostResponse, ExecutorError>> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let key = req.key().clone();
            Box::pin(async move {
                Ok(HostResponse::DelegateResponse {
                    key,
                    values: Vec::new(),
                })
            })
        }
    }

    struct Lane {
        gate: Arc<DelegateGate>,
        runner: Arc<Counts>,
        tx: tokio::sync::mpsc::Sender<LaneRequest>,
        forwarded: tokio::sync::mpsc::UnboundedReceiver<LaneRequest>,
        task: tokio::task::JoinHandle<()>,
    }

    impl Lane {
        fn start() -> Self {
            let gate = DelegateGate::new();
            let runner = Arc::new(Counts(Default::default()));
            let (tx, rx) = tokio::sync::mpsc::channel(LANE_INTAKE_CAPACITY);
            let (ftx, forwarded) = tokio::sync::mpsc::unbounded_channel();
            let task = tokio::spawn(run_lane(
                runner.clone(),
                gate.clone(),
                Arc::new(Collect(ftx)),
                rx,
            ));
            Lane {
                gate,
                runner,
                tx,
                forwarded,
                task,
            }
        }

        fn runs(&self) -> usize {
            self.runner.0.load(std::sync::atomic::Ordering::SeqCst)
        }

        async fn next_forwarded(&mut self) -> LaneRequest {
            tokio::time::timeout(Duration::from_secs(5), self.forwarded.recv())
                .await
                .expect("a request is forwarded")
                .expect("lane up")
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
        gate.hand_off_for_test(unit(&k));
        assert!(gate.claim_unless_pending(&k).await.is_none());
        let (claim, u) = gate.take_next_unit().expect("still pending");
        assert_eq!(u.key, k);
        drop(claim);
        assert!(gate.claim_unless_pending(&k).await.is_some());
    }

    #[tokio::test]
    async fn a_cancelled_claim_does_not_keep_the_lane_off_the_delegate() {
        let gate = DelegateGate::new();
        let k = key(6);
        let run = gate.try_begin_lane_run(&k).expect("free");
        let waiting = tokio::time::timeout(Duration::from_millis(20), gate.claim(&k)).await;
        assert!(waiting.is_err(), "the claim waits for the lane run");
        run.finish();
        assert!(
            gate.try_begin_lane_run(&k).is_some(),
            "a claim abandoned while waiting must not block the lane forever"
        );
    }

    /// A request for a delegate the loop holds is FORWARDED, not run; and
    /// once one request for a delegate is forwarded, later requests for it
    /// are forwarded too, in order, until the loop has finished the earlier
    /// ones. Only then does the lane run the delegate again.
    ///
    /// FALSIFY: drop `mark_loop_owned` from `serve` (or the `loop_owned`
    /// term from `Entry::is_clear`); the second request runs on the lane,
    /// ahead of the first one the loop still holds.
    #[tokio::test]
    async fn requests_follow_a_forwarded_request_to_the_loop_in_order() {
        let mut lane = Lane::start();
        let k = key(10);
        // The loop holds K (a notification run, say).
        let (claim, _) = lane.gate.claim(&k).await;
        let (r1, _a1) = request(&k, 1);
        lane.tx.send(r1).await.unwrap();
        let first = lane.next_forwarded().await;
        assert_eq!(first.id, 1, "a request for a claimed delegate is forwarded");
        drop(claim);

        // The loop has not yet processed the forwarded request: K is
        // loop-owned, so the next request must follow it, not overtake it.
        let (r2, _a2) = request(&k, 2);
        lane.tx.send(r2).await.unwrap();
        let second = lane.next_forwarded().await;
        assert_eq!(second.id, 2, "a later request follows the forwarded one");
        assert_eq!(lane.runs(), 0, "nothing for K ran on the lane");

        // The loop finishes both; the lane runs K again.
        drop(lane.gate.take_forwarded(1).expect("forwarded 1"));
        drop(lane.gate.take_forwarded(2).expect("forwarded 2"));
        let (r3, a3) = request(&k, 3);
        lane.tx.send(r3).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), a3)
            .await
            .expect("answered")
            .expect("answered on the lane");
        assert_eq!(lane.runs(), 1);
        assert!(!lane.gate.is_tracking(&k));
        lane.task.abort();
    }

    /// At the unit bound the lane FORWARDS a request for a free delegate
    /// (the pre-lane behaviour) rather than refusing it or holding it.
    ///
    /// FALSIFY: drop the `units.len() >= MAX_PENDING_UNITS` check from
    /// `try_begin_lane_run`; the request runs on the lane.
    #[tokio::test]
    async fn at_the_unit_bound_the_lane_forwards() {
        let mut lane = Lane::start();
        for i in 0..MAX_PENDING_UNITS {
            lane.gate.hand_off_for_test(unit(&key(100 + i as u8)));
        }
        let free = key(20);
        let (r, _a) = request(&free, 7);
        lane.tx.send(r).await.unwrap();
        assert_eq!(lane.next_forwarded().await.id, 7);
        assert_eq!(lane.runs(), 0);
        // One unit taken by the loop: room again, the lane runs.
        let (claim, _) = lane.gate.take_next_unit().expect("a unit");
        drop(claim);
        drop(lane.gate.take_forwarded(7));
        let (r, a) = request(&free, 8);
        lane.tx.send(r).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), a)
            .await
            .expect("answered")
            .expect("answered on the lane");
        assert_eq!(lane.runs(), 1);
        lane.task.abort();
    }

    /// Registrations, unregistrations and forged node-only inbound go to the
    /// loop's ordinary path; a flood of them for distinct keys leaves no
    /// lane state behind beyond the loop-owned marks the loop releases.
    #[tokio::test]
    async fn requests_the_lane_does_not_run_go_to_the_loop() {
        let mut lane = Lane::start();
        let n = 50;
        for i in 0..n {
            let k = key(i as u8);
            let (r, _a) = request_with(&k, i, DelegateRequest::UnregisterDelegate(k.clone()));
            std::mem::forget(_a);
            lane.tx.send(r).await.unwrap();
        }
        for i in 0..n {
            let r = lane.next_forwarded().await;
            assert_eq!(r.id, i, "forwarded in order");
            drop(lane.gate.take_forwarded(r.id));
        }
        assert_eq!(lane.runs(), 0);
        assert_eq!(
            lane.gate.pending_units(),
            0,
            "no units for forwarded requests"
        );
        for i in 0..n {
            assert!(!lane.gate.is_tracking(&key(i as u8)));
        }
        lane.task.abort();
    }

    /// A panic inside ONE delegate run fails that request only: its client
    /// gets an error, the gate releases the delegate, and the lane serves the
    /// next request.
    ///
    /// FALSIFY: in `serve`, await `runner.run(..)` without `catch_unwind`; the
    /// first client sees its responder dropped (not an error response), the
    /// lane dies, and the second request is never answered.
    #[tokio::test]
    async fn a_panicking_run_fails_only_its_request_and_the_lane_carries_on() {
        struct PanicsOnce(std::sync::atomic::AtomicBool);
        impl DelegateRunner for PanicsOnce {
            fn run<'a>(
                &'a self,
                req: DelegateRequest<'a>,
                _origin_contract: Option<&'a ContractInstanceId>,
                _caller_delegate: Option<&'a DelegateKey>,
                _connection_scope: crate::client_events::ConnectionScope,
                _user_context: Option<&'a UserSecretContext>,
            ) -> BoxFuture<'a, Result<HostResponse, ExecutorError>> {
                let first = !self.0.swap(true, std::sync::atomic::Ordering::SeqCst);
                let key = req.key().clone();
                Box::pin(async move {
                    if first {
                        panic!("runner bug");
                    }
                    Ok(HostResponse::DelegateResponse {
                        key,
                        values: Vec::new(),
                    })
                })
            }
        }
        let gate = DelegateGate::new();
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let (ftx, _frx) = tokio::sync::mpsc::unbounded_channel();
        let lane = tokio::spawn(run_lane(
            Arc::new(PanicsOnce(Default::default())),
            gate.clone(),
            Arc::new(Collect(ftx)),
            rx,
        ));
        let k = key(30);
        let (req, a) = request(&k, 1);
        tx.send(req).await.expect("lane is up");
        let (_, first) = tokio::time::timeout(Duration::from_secs(5), a)
            .await
            .expect("answered")
            .expect(
                "the panicking request's client gets an ERROR response, not a dropped responder",
            );
        assert!(
            matches!(first, ContractHandlerEvent::DelegateResponse(Err(_))),
            "got {first}"
        );
        let (req, b) = request(&k, 2);
        tx.send(req).await.expect("the lane is still up");
        let (_, second) = tokio::time::timeout(Duration::from_secs(5), b)
            .await
            .expect("the lane serves the next request")
            .expect("answered");
        assert!(
            matches!(second, ContractHandlerEvent::DelegateResponse(Ok(_))),
            "got {second}"
        );
        assert!(
            !gate.is_tracking(&k),
            "the panicked run released its delegate"
        );
        assert!(!lane.is_finished());
        lane.abort();
    }

    /// The force-resume path (`sweep_expired_parks` ->
    /// `handle_delegate_resume`) relies on claiming a PARKED delegate not
    /// suspending, so nothing can run between `should_force_resume` and
    /// `take_matching` (#5554).
    ///
    /// FALSIFY: add `tokio::task::yield_now().await;` to `claim`; this
    /// returns `None` from `now_or_never`.
    #[tokio::test]
    async fn a_claim_on_a_parked_delegate_does_not_suspend() {
        use futures::FutureExt;
        let gate = DelegateGate::new();
        let k = key(7);
        let (run_claim, _) = gate.claim(&k).await;
        let _park = gate.hold(&k);
        drop(run_claim);
        assert!(
            gate.claim(&k).now_or_never().is_some(),
            "claiming a parked delegate must complete without suspending"
        );
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

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
//!
//! A PARKED delegate (one waiting on a user prompt or an off-loop fetch, up to
//! `delegate_park::PARK_TTL`) keeps at most
//! [`MAX_PENDING_PER_DELEGATE`](super::delegate_park::MAX_PENDING_PER_DELEGATE)
//! requests queued here; the lane refuses the rest at once, with the shape the
//! park's own queue refused them with before this lane existed (the responder
//! is dropped, so the client sees an error). Without that cap one parked
//! delegate fills the queue, the lane stops reading, and every delegate on the
//! node is refused until the park ends. The cap is per delegate KEY, as the
//! park is: on a hosted node every user of a shared delegate key shares it.
//! The other reasons a delegate is not free (a loop claim, a pending unit, a
//! lane run) last one loop round-trip, not a human's reaction time, so they do
//! not refuse: a burst of requests to a busy delegate waits, as it did in the
//! loop's fair queue.
//!
//! A queued request whose client has gone away (its responder is closed) is
//! dropped instead of run, so a retry after a client-side timeout does not
//! execute twice.
//!
//! # Ordering caveats
//!
//! * Startup: a request sent before `contract_handling` installs the lane
//!   reaches the loop, and a later request to the same delegate, sent after
//!   installation, reaches the lane and may run first. Only requests already
//!   in flight while the node starts are affected.
//! * The gate is per delegate KEY, not per (key, hosted user), the same
//!   granularity as the park registry: one user's round-trip on a shared
//!   delegate key holds every other user's requests for that key.

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
    /// How many of `loop_claims` are held by a park entry
    /// ([`DelegateGate::hold`]). Non-zero means the delegate is PARKED, which
    /// can last up to the park TTL, so the lane caps its queued requests.
    park_holds: usize,
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
///
/// One `std::sync::Mutex` over a `HashMap` rather than a `DashMap`: handing a
/// unit off updates a delegate's entry and the cross-delegate unit queue
/// together, and a claim reads both. The lock is never held across an
/// `.await`, and each critical section is a few map operations.
#[derive(Default)]
pub(crate) struct DelegateGate {
    inner: std::sync::Mutex<Inner>,
    /// Loop -> lane: a delegate may have become free.
    lane_wake: Notify,
    /// Lane -> loop: a unit is waiting.
    units_ready: Notify,
    /// Lane -> loop claims waiting on a lane run: a lane run ended.
    lane_run_ended: Notify,
    /// Requests the lane refused because their delegate was parked with a
    /// full queue behind it. Counted, not only logged.
    refused_parked: std::sync::atomic::AtomicU64,
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
                park: false,
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
            drop(inner);
            self.lane_wake.notify_one();
            return None;
        }
        entry.loop_claims += 1;
        Some(LoopClaim {
            gate: self.clone(),
            key: key.clone(),
            park: false,
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
    /// one). Marks the delegate parked for the lane's per-delegate queue cap.
    pub(crate) fn hold(self: &Arc<Self>, key: &DelegateKey) -> LoopClaim {
        let mut inner = self.lock();
        let entry = inner.entries.entry(key.clone()).or_default();
        debug_assert!(entry.loop_claims > 0, "hold() without a live loop claim");
        entry.loop_claims += 1;
        entry.park_holds += 1;
        LoopClaim {
            gate: self.clone(),
            key: key.clone(),
            park: true,
        }
    }

    /// Whether a park entry holds `key`.
    fn is_parked(&self, key: &DelegateKey) -> bool {
        self.lock()
            .entries
            .get(key)
            .is_some_and(|entry| entry.park_holds > 0)
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
            park: false,
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

    fn try_forward(&self, unit: LaneUnit) -> Result<(), Box<LaneUnit>> {
        let mut inner = self.lock();
        let entry = inner.entries.entry(unit.key.clone()).or_default();
        if !entry.is_clear() {
            return Err(Box::new(unit));
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

    #[cfg(test)]
    pub(crate) fn refused_parked_count(&self) -> u64 {
        self.refused_parked
            .load(std::sync::atomic::Ordering::Relaxed)
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
        drop(inner);
        self.gate.lane_wake.notify_one();
    }
}

/// The loop's hold on a delegate. While any is live the lane does not run it.
pub(crate) struct LoopClaim {
    gate: Arc<DelegateGate>,
    key: DelegateKey,
    /// Taken by [`DelegateGate::hold`] for a park entry.
    park: bool,
}

impl Drop for LoopClaim {
    fn drop(&mut self) {
        let mut inner = self.gate.lock();
        if let Some(entry) = inner.entries.get_mut(&self.key) {
            entry.loop_claims = entry.loop_claims.saturating_sub(1);
            if self.park {
                entry.park_holds = entry.park_holds.saturating_sub(1);
            }
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
/// A panic inside the lane (a delegate runner bug) is caught here, logged at
/// error, and ends the lane: its intake closes with it, so every later client
/// delegate request falls back to the contract loop (the sender sees a closed
/// intake). The panic is not re-raised: nothing awaits the lane's handle, so
/// re-raising would only lose the message. An abort is not a panic and is
/// logged at debug by [`AbortLog`].
pub(crate) async fn run_lane(
    runner: Arc<dyn DelegateRunner>,
    gate: Arc<DelegateGate>,
    intake: tokio::sync::mpsc::Receiver<LaneRequest>,
) {
    use futures::FutureExt;
    let mut exit = AbortLog { finished: false };
    let outcome = std::panic::AssertUnwindSafe(lane_loop(runner, gate, intake))
        .catch_unwind()
        .await;
    exit.finished = true;
    match outcome {
        Ok(()) => tracing::debug!("Delegate lane task ended: its intake closed"),
        Err(payload) => {
            let reason = payload
                .downcast_ref::<&str>()
                .map(|s| (*s).to_string())
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "non-string panic payload".to_string());
            tracing::error!(
                %reason,
                "Delegate lane task PANICKED; client delegate requests fall back to \
                 the contract loop for the rest of this process"
            );
        }
    }
}

async fn lane_loop(
    runner: Arc<dyn DelegateRunner>,
    gate: Arc<DelegateGate>,
    mut intake: tokio::sync::mpsc::Receiver<LaneRequest>,
) {
    let mut queue: VecDeque<LaneRequest> = VecDeque::new();
    loop {
        while queue.len() < LANE_QUEUE_CAPACITY {
            match intake.try_recv() {
                Ok(req) => queue.push_back(req),
                Err(_) => break,
            }
        }
        prune(&mut queue, &gate);

        if let Some(idx) = next_runnable(&queue, |key| gate.is_free(key)) {
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
                None => return,
            },
            () = wake => {}
        }
    }
}

/// Drop queued requests whose client has gone away, and refuse the requests
/// for a PARKED delegate past [`MAX_PENDING_PER_DELEGATE`] (the oldest are
/// kept, so per-delegate order holds). See the module docs, "Bounds".
///
/// [`MAX_PENDING_PER_DELEGATE`]: super::delegate_park::MAX_PENDING_PER_DELEGATE
fn prune(queue: &mut VecDeque<LaneRequest>, gate: &DelegateGate) {
    use super::delegate_park::MAX_PENDING_PER_DELEGATE;
    // Per key: (parked, requests kept so far). One gate lookup per key.
    let mut seen: HashMap<DelegateKey, (bool, usize)> = HashMap::new();
    queue.retain(|request| {
        let key = request.req.key();
        if request.responder.is_closed() {
            tracing::debug!(
                delegate_key = %key,
                id = request.id,
                "Delegate lane: dropping a queued request whose client went away"
            );
            return false;
        }
        let (parked, kept) = seen
            .entry(key.clone())
            .or_insert_with(|| (gate.is_parked(key), 0));
        if *parked && *kept >= MAX_PENDING_PER_DELEGATE {
            let total = gate
                .refused_parked
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                + 1;
            tracing::warn!(
                delegate_key = %key,
                limit = MAX_PENDING_PER_DELEGATE,
                total_refused = total,
                "Refused a delegate request: the delegate is parked and already has \
                 the maximum number of requests waiting behind it. Dropping the \
                 responder so the client sees an error"
            );
            // Dropping the request drops its responder: the client's oneshot
            // closes and it surfaces an error, as the park's own queue did.
            return false;
        }
        *kept += 1;
        true
    });
}

/// The first queued request whose delegate is free and has no earlier request
/// still queued.
///
/// `is_free` reads the gate, which the loop changes concurrently, so two
/// requests for the same delegate can get different answers within one scan.
/// The `blocked` set is what stops the LATER one running when the earlier one
/// was seen busy (`next_runnable_never_runs_a_later_request_for_a_blocked_delegate`).
fn next_runnable(
    queue: &VecDeque<LaneRequest>,
    mut is_free: impl FnMut(&DelegateKey) -> bool,
) -> Option<usize> {
    let mut blocked: HashSet<&DelegateKey> = HashSet::new();
    for (idx, request) in queue.iter().enumerate() {
        let key = request.req.key();
        if blocked.contains(key) {
            continue;
        }
        if is_free(key) {
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

    fn request(
        k: &DelegateKey,
    ) -> (
        LaneRequest,
        tokio::sync::oneshot::Receiver<(EventId, ContractHandlerEvent)>,
    ) {
        let (responder, rx) = tokio::sync::oneshot::channel();
        let req = LaneRequest {
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
        };
        (req, rx)
    }

    /// A busy delegate does not block a free one behind it, and a request
    /// for a busy delegate is never overtaken by a later request for the
    /// same delegate, even when the gate's answer changes mid-scan.
    ///
    /// FALSIFY: remove `blocked.insert(key)` from `next_runnable`; the second
    /// assertion picks the later X (index 1).
    #[test]
    fn next_runnable_never_runs_a_later_request_for_a_blocked_delegate() {
        let (x, y) = (key(10), key(11));
        // [X busy, Y free, X]: Y, not the second X.
        let queue: VecDeque<_> = [&x, &y, &x].into_iter().map(|k| request(k).0).collect();
        assert_eq!(next_runnable(&queue, |k| k != &x), Some(1));

        // [X, X, Y busy]: the gate frees X between the two X checks (the
        // loop dropped its claim mid-scan). The first X was seen busy, so
        // the second X must NOT run ahead of it.
        let queue: VecDeque<_> = [&x, &x, &y].into_iter().map(|k| request(k).0).collect();
        let mut x_checks = 0;
        let picked = next_runnable(&queue, |k| {
            if k == &x {
                x_checks += 1;
                x_checks > 1
            } else {
                false
            }
        });
        assert_eq!(
            picked, None,
            "a later request for a delegate seen busy must wait for the earlier one"
        );
    }

    /// A PARKED delegate keeps at most `MAX_PENDING_PER_DELEGATE` queued
    /// requests (the oldest), the rest are refused (their responder is
    /// dropped); other delegates' requests and a non-parked delegate's are
    /// untouched; a request whose client went away is dropped.
    ///
    /// FALSIFY: remove the `*parked && *kept >= MAX_PENDING_PER_DELEGATE`
    /// refusal in `prune`; D keeps all of its requests. Remove the
    /// `is_closed()` check; the abandoned request stays queued.
    #[tokio::test]
    async fn prune_caps_a_parked_delegate_and_drops_abandoned_requests() {
        use super::super::delegate_park::MAX_PENDING_PER_DELEGATE;
        let gate = DelegateGate::new();
        let (d, e, f) = (key(20), key(21), key(22));
        let (claim, _) = gate.claim(&d).await;
        let park_hold = gate.hold(&d);
        drop(claim);
        // F is claimed by the loop but NOT parked: no cap.
        let (f_claim, _) = gate.claim(&f).await;

        let extra = 3;
        let mut queue = VecDeque::new();
        let mut d_rx = Vec::new();
        let mut f_rx = Vec::new();
        for _ in 0..MAX_PENDING_PER_DELEGATE + extra {
            let (r, rx) = request(&d);
            queue.push_back(r);
            d_rx.push(rx);
            let (r, rx) = request(&f);
            queue.push_back(r);
            f_rx.push(rx);
        }
        let (r, mut e_rx) = request(&e);
        queue.push_back(r);
        let (r, gone) = request(&e);
        drop(gone);
        queue.push_back(r);

        prune(&mut queue, &gate);

        fn count(queue: &VecDeque<LaneRequest>, k: &DelegateKey) -> usize {
            queue.iter().filter(|r| r.req.key() == k).count()
        }
        assert_eq!(
            count(&queue, &d),
            MAX_PENDING_PER_DELEGATE,
            "parked D is capped"
        );
        assert_eq!(
            count(&queue, &f),
            MAX_PENDING_PER_DELEGATE + extra,
            "a delegate that is busy but not parked is not capped"
        );
        assert_eq!(
            count(&queue, &e),
            1,
            "the abandoned request is dropped, the live one kept"
        );
        assert!(
            matches!(
                e_rx.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)
            ),
            "the live request is still waiting for its answer"
        );
        for (i, rx) in d_rx.iter_mut().enumerate() {
            let closed = matches!(
                rx.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Closed)
            );
            assert_eq!(
                closed,
                i >= MAX_PENDING_PER_DELEGATE,
                "the OLDEST requests are kept and the newest refused (request {i})"
            );
        }
        assert_eq!(gate.refused_parked_count(), extra as u64);

        // Unparked: no cap any more.
        drop(park_hold);
        drop(f_claim);
        let mut more_rx = Vec::new();
        for _ in 0..extra {
            let (r, rx) = request(&d);
            queue.push_back(r);
            more_rx.push(rx);
        }
        prune(&mut queue, &gate);
        assert_eq!(count(&queue, &d), MAX_PENDING_PER_DELEGATE + extra);
    }

    /// A panic inside the lane ends the lane quietly for its awaiter (it is
    /// logged, not re-raised) and closes its intake, so senders fall back to
    /// the loop.
    ///
    /// FALSIFY: in `run_lane`, await `lane_loop(..)` directly instead of
    /// through `catch_unwind`; the join reports a panic.
    #[tokio::test]
    async fn a_panicking_lane_is_caught_and_closes_its_intake() {
        struct Panics;
        impl DelegateRunner for Panics {
            fn run<'a>(
                &'a self,
                _req: DelegateRequest<'a>,
                _origin_contract: Option<&'a ContractInstanceId>,
                _caller_delegate: Option<&'a DelegateKey>,
                _connection_scope: crate::client_events::ConnectionScope,
                _user_context: Option<&'a UserSecretContext>,
            ) -> BoxFuture<'a, Result<HostResponse, ExecutorError>> {
                Box::pin(async { panic!("runner bug") })
            }
        }
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let lane = tokio::spawn(run_lane(Arc::new(Panics), DelegateGate::new(), rx));
        let (req, _answer) = request(&key(30));
        tx.send(req).await.expect("lane is up");
        tokio::time::timeout(Duration::from_secs(5), lane)
            .await
            .expect("the lane ends")
            .expect("the panic is caught inside the lane, not propagated to its join");
        assert!(tx.is_closed(), "the dead lane's intake is closed");
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

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
//! * no LOOP-OWNED request for `D` from the SAME USER: a request the lane
//!   forwarded for `D` and the loop has not finished. The lane marks it before
//!   forwarding; the mark is a [`LaneTicket`] carried INSIDE the forwarded
//!   event, released wherever that event ends (the loop's `DelegateRequest`
//!   arm, a fair-queue refusal or eviction, a closed channel). So once one of
//!   a user's requests for `D` is forwarded, that user's later requests for `D`
//!   are forwarded too until the loop has done the earlier ones, and none
//!   overtakes it: the loop's channel and each of the fair queue's
//!   per-priority slots are FIFO. The promise is therefore per client and
//!   priority slot: two forwarded requests at DIFFERENT priorities sit in
//!   different slots and may run in either order. Client delegate requests
//!   are all sent `ClientLocal`, so in practice that is one slot.
//!
//! The mark is per (delegate key, hosted user), the finest scope the event
//! carries: a `DelegateRequest` names no client or connection, only its user
//! (`user_context`, `None` on a node that is not hosted). Order is promised per
//! client, every client belongs to one user, and `DelegateContextCache` is
//! itself keyed by (delegate, user), so nothing needs one user's forwarded
//! request to hold back another user's. On a hosted node that keeps one
//! user's forward from sending every other user of a shared delegate key (the
//! River chat delegate) to the loop. Exclusion between lane and loop RUNS
//! stays per key. A forged node-only message is forwarded WITHOUT a mark: the
//! loop only refuses it, changing nothing a later request could observe.
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
//! is flooded: the lane holds nothing else. Requests that are not from the
//! node's own LOCAL client (a remote connection, or a hosted user, whose
//! traffic arrives through the loopback TLS proxy) may use only
//! `LANE_INTAKE_CAPACITY - LOCAL_INTAKE_RESERVE` slots, so a remote flood
//! cannot refuse the operator's own requests: the lane's version of the fair
//! queue's `CLIENT_LOCAL_RESERVE`, which does not see lane-run requests.
//! Units are bounded as above. The fair queue's bounds apply to FORWARDED
//! requests only; requests the lane runs are bounded by the intake, the
//! lane's speed and the websocket-level per-user operation limit.
//!
//! # Ordering caveats
//!
//! * Startup: a request sent before `contract_handling` installs the lane
//!   reaches the loop, and a later request to the same delegate, sent after
//!   installation, reaches the lane and may run first. Only requests already
//!   in flight while the node starts are affected. Closing this would mean
//!   creating the intake with the contract handler channel, which needs a
//!   drain path for channels driven without `contract_handling` (tests, and
//!   the window before the runner exists); not done, since the effect is
//!   limited to start-up.
//! * Exclusion (claims, parks, lane runs) is per delegate KEY, the same
//!   granularity as the park registry: one user's round-trip on a shared
//!   delegate key sends every other user's requests for that key to the loop
//!   until it finishes. The loop-owned MARK is finer, per (key, user), since
//!   that is all the ordering it protects needs: a user's later request
//!   follows that user's forwarded one, and does not hold back other users.
//! * On a node that is not hosted, every client has user `None`, so the
//!   mark is effectively per key there: any client's forwarded request (a
//!   registration, say) sends every client's requests for that delegate to
//!   the loop until the loop has done it. That costs pre-lane latency, never
//!   a refusal, and `delegate_lane_forwarded_loop_owned` shows it.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use freenet_stdlib::client_api::{DelegateRequest, HostResponse};
use freenet_stdlib::prelude::{ContractInstanceId, DelegateKey, OutboundDelegateMsg};
use futures::future::BoxFuture;
use tokio::sync::Notify;

use super::ExecutorError;
use super::handler::{ContractHandlerEvent, EventId};
use crate::wasm_runtime::{UserId, UserSecretContext};

/// Requests the intake channel buffers before `try_send` refuses.
pub(crate) const LANE_INTAKE_CAPACITY: usize = 128;

/// Continuations ([`LaneUnit`]s) that may wait for the loop at once. Checked
/// before the lane starts a run: at the bound the request is forwarded to the
/// loop instead, as before this lane.
pub(crate) const MAX_PENDING_UNITS: usize = 16;

/// Intake slots only a LOCAL request (local connection, no hosted user) may
/// take. See the module docs, "Bounds".
pub(crate) const LOCAL_INTAKE_RESERVE: usize = LANE_INTAKE_CAPACITY / 8;

/// Whether a request counts as the node's own local client for the intake
/// reserve: a local connection AND no hosted user (hosted traffic arrives
/// through the loopback proxy, so its scope reads `Local`).
pub(crate) fn is_local_client(
    connection_scope: crate::client_events::ConnectionScope,
    user_context: Option<&UserSecretContext>,
) -> bool {
    connection_scope.is_local() && user_context.is_none()
}

/// Declares [`ForwardReason`] and its [`ALL`](ForwardReason::ALL) list from
/// ONE list of variants, so no variant can be left out of `ALL`, and a
/// variant's index is its position (`as usize` on implicit discriminants).
/// [`FORWARDED`] is sized from `ALL`, so `record_forward` cannot index past
/// it.
macro_rules! forward_reasons {
    ($($(#[$doc:meta])* $variant:ident,)+) => {
        /// Why the lane forwarded a request instead of running it (telemetry).
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub(crate) enum ForwardReason {
            $($(#[$doc])* $variant,)+
        }

        impl ForwardReason {
            /// Every reason, in declaration order, which is
            /// [`index`](Self::index) order.
            pub(crate) const ALL: [ForwardReason; [$(ForwardReason::$variant,)+].len()] =
                [$(Self::$variant,)+];
        }
    };
}

forward_reasons! {
    /// The loop holds the delegate (a claim, a claim waiting, a pending unit).
    Busy,
    /// A park entry holds the delegate.
    Parked,
    /// An earlier request of the same user for it is still with the loop.
    LoopOwned,
    /// [`MAX_PENDING_UNITS`] continuations are already waiting.
    UnitBound,
    /// Registration or unregistration: the loop owns those hooks.
    NotRunnable,
    /// A forged node-only inbound message: the loop refuses it.
    Forged,
    /// The lane task has exited: the request went to the loop because
    /// there is no lane. Counted both for requests the dying lane handed
    /// over and for later ones that found its intake closed, which also
    /// happens while the node shuts down.
    LaneDown,
}

impl ForwardReason {
    pub(crate) const COUNT: usize = Self::ALL.len();

    /// Index into [`FORWARDED`]: the variant's position in the declaration
    /// (and so in [`ALL`](Self::ALL)).
    pub(crate) const fn index(self) -> usize {
        self as usize
    }
}

/// Lane-run and forwarded counts, by reason, for the router snapshot
/// ([`routing_counts`]). Plain atomics: bumped once per request.
pub(crate) static LANE_RUNS: AtomicU64 = AtomicU64::new(0);
pub(crate) static FORWARDED: [AtomicU64; ForwardReason::COUNT] =
    [const { AtomicU64::new(0) }; ForwardReason::COUNT];

/// Count one request that went to the loop for `reason`.
pub(crate) fn record_forward(reason: ForwardReason) {
    FORWARDED[reason.index()].fetch_add(1, AtomicOrdering::Relaxed);
}

/// A snapshot of the lane's routing counters.
pub(crate) struct RoutingCounts {
    pub(crate) lane_runs: u64,
    forwarded: [u64; ForwardReason::COUNT],
}

impl RoutingCounts {
    pub(crate) fn forwarded(&self, reason: ForwardReason) -> u64 {
        self.forwarded[reason.index()]
    }

    /// Copy these counts into the router snapshot's fields. The match is
    /// exhaustive, so a new reason does not compile until it has a field.
    pub(crate) fn write_into(&self, snapshot: &mut crate::router::RouterSnapshotInfo) {
        snapshot.delegate_lane_runs = Some(self.lane_runs);
        for reason in ForwardReason::ALL {
            let field = match reason {
                ForwardReason::Busy => &mut snapshot.delegate_lane_forwarded_busy,
                ForwardReason::Parked => &mut snapshot.delegate_lane_forwarded_parked,
                ForwardReason::LoopOwned => &mut snapshot.delegate_lane_forwarded_loop_owned,
                ForwardReason::UnitBound => &mut snapshot.delegate_lane_forwarded_unit_bound,
                ForwardReason::NotRunnable => &mut snapshot.delegate_lane_forwarded_not_runnable,
                ForwardReason::Forged => &mut snapshot.delegate_lane_forwarded_forged,
                ForwardReason::LaneDown => &mut snapshot.delegate_lane_forwarded_lane_down,
            };
            *field = Some(self.forwarded(reason));
        }
    }
}

pub(crate) fn routing_counts() -> RoutingCounts {
    RoutingCounts {
        lane_runs: LANE_RUNS.load(AtomicOrdering::Relaxed),
        forwarded: std::array::from_fn(|i| FORWARDED[i].load(AtomicOrdering::Relaxed)),
    }
}

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
/// `DelegateRequest` path, carrying its [`LaneTicket`] inside the event.
/// Implemented by `handler::LoopForwarder`.
pub(crate) trait LoopForward: Send + Sync + 'static {
    fn forward(&self, request: LaneRequest, ticket: LaneTicket);
}

/// The loop-owned mark of a forwarded request, carried in the forwarded
/// `ContractHandlerEvent::DelegateRequest` and released when it is dropped:
/// wherever the event ends. Empty for a request the lane did not forward.
#[derive(Default)]
pub(crate) struct LaneTicket(Option<LoopOwned>);

impl std::fmt::Debug for LaneTicket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(if self.0.is_some() {
            "LaneTicket(held)"
        } else {
            "LaneTicket(none)"
        })
    }
}

impl LaneTicket {
    #[cfg(test)]
    pub(crate) fn is_held(&self) -> bool {
        self.0.is_some()
    }
}

/// What the loop holds for one client delegate run, released together just
/// before it answers the client (`contract::dispatch_delegate_request`), as
/// the lane does: so a client's next request can find the delegate free.
#[derive(Default)]
pub(crate) struct LoopRelease {
    pub(crate) claim: Option<LoopClaim>,
    pub(crate) ticket: LaneTicket,
}

impl LoopRelease {
    /// Release the claim and the loop-owned mark now.
    pub(crate) fn release(self) {
        let Self { claim, ticket } = self;
        drop(claim);
        drop(ticket);
    }
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
    /// How many of `loop_claims` are park entries ([`DelegateGate::hold`]),
    /// for telemetry only.
    park_holds: usize,
    /// Requests for this delegate the lane forwarded and the loop has not
    /// finished ([`LoopOwned`]), per hosted user. Blocks a lane run only for
    /// the same user; see the module docs.
    loop_owned: HashMap<Option<UserId>, usize>,
}

impl Entry {
    /// Nothing holds the delegate at all: the entry can be removed.
    fn is_clear(&self) -> bool {
        self.runnable_by_lane() && self.loop_owned.is_empty()
    }

    /// Neither the lane nor the loop is running or holding the delegate
    /// (per KEY: exclusion between runs).
    fn runnable_by_lane(&self) -> bool {
        !self.lane_running && !self.unit_pending && self.loop_claims == 0 && self.loop_waiting == 0
    }
}

#[derive(Default)]
struct Inner {
    entries: HashMap<DelegateKey, Entry>,
    /// Units for the loop, oldest first. At most one per delegate and
    /// [`MAX_PENDING_UNITS`] in all.
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
    /// one).
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

    /// Start a lane run of `key` for `user` if the delegate is free and the
    /// unit bound has room (a run may hand off). `Err` says why to forward
    /// instead.
    fn try_begin_lane_run(
        self: &Arc<Self>,
        key: &DelegateKey,
        user: Option<UserId>,
    ) -> Result<LaneRun, ForwardReason> {
        let mut inner = self.lock();
        if inner.units.len() >= MAX_PENDING_UNITS {
            return Err(ForwardReason::UnitBound);
        }
        let entry = inner.entries.entry(key.clone()).or_default();
        let reason = if entry.park_holds > 0 {
            Some(ForwardReason::Parked)
        } else if !entry.runnable_by_lane() {
            Some(ForwardReason::Busy)
        } else if entry.loop_owned.contains_key(&user) {
            Some(ForwardReason::LoopOwned)
        } else {
            None
        };
        if let Some(reason) = reason {
            inner.remove_if_clear(key);
            return Err(reason);
        }
        entry.lane_running = true;
        Ok(LaneRun {
            gate: self.clone(),
            key: key.clone(),
            ended: false,
        })
    }

    /// Mark `user`'s requests for `key` loop-owned until the returned guard is
    /// dropped. Called BEFORE forwarding, so the loop cannot finish first.
    fn mark_loop_owned(self: &Arc<Self>, key: &DelegateKey, user: Option<UserId>) -> LoopOwned {
        let mut inner = self.lock();
        *inner
            .entries
            .entry(key.clone())
            .or_default()
            .loop_owned
            .entry(user)
            .or_default() += 1;
        LoopOwned {
            gate: self.clone(),
            key: key.clone(),
            user,
        }
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

    /// A loop-owned mark for `key` and `user`, as the lane makes when it
    /// forwards a request.
    #[cfg(test)]
    pub(crate) fn ticket_for_test(
        self: &Arc<Self>,
        key: &DelegateKey,
        user: Option<UserId>,
    ) -> LaneTicket {
        LaneTicket(Some(self.mark_loop_owned(key, user)))
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
    }
}

/// A forwarded request's loop-owned mark. Carried in a [`LaneTicket`].
pub(crate) struct LoopOwned {
    gate: Arc<DelegateGate>,
    key: DelegateKey,
    user: Option<UserId>,
}

impl Drop for LoopOwned {
    fn drop(&mut self) {
        let mut inner = self.gate.lock();
        if let Some(entry) = inner.entries.get_mut(&self.key)
            && let Some(count) = entry.loop_owned.get_mut(&self.user)
        {
            *count = count.saturating_sub(1);
            if *count == 0 {
                entry.loop_owned.remove(&self.user);
            }
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
            crate::node::network_status::record_delegate_lane_death();
            tracing::error!(
                %reason,
                "Delegate lane task PANICKED; client delegate requests fall back to \
                 the contract loop for the rest of this process"
            );
            // Requests already buffered in the intake go to the loop rather
            // than lose their responders. Close first, so new requests see a
            // closed intake and go to the loop directly; a sender racing the
            // close may still put its request on the loop's channel before a
            // drained one, so order across this hand-over is best effort.
            //
            // `recv` until `None`, not `try_recv` until empty: after `close`,
            // a sender that took its permit before the close may still be
            // completing its send, and `try_recv` reports that as `Empty`.
            // `recv` returns `None` only once every permit is used or dropped.
            //
            // Each hand-over runs under its own `catch_unwind`: if the panic
            // that killed the lane came from the forwarder, it may panic
            // again, and one such panic must not drop every request still
            // buffered behind it.
            intake.close();
            while let Some(request) = intake.recv().await {
                let reason = not_lane_runnable(&request).unwrap_or(ForwardReason::LaneDown);
                let delegate_key = request.req.key().clone();
                let handed_over = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    forward_to_loop(&gate, &*forward, request, reason)
                }));
                if let Err(payload) = handed_over {
                    // Its forward was already counted; count the panic too so
                    // the lost request is visible outside the log.
                    crate::node::network_status::record_delegate_lane_panic();
                    tracing::error!(
                        %delegate_key,
                        reason = %panic_reason(&*payload),
                        "Handing a buffered delegate request to the contract loop PANICKED; \
                         that client gets an error, the rest are still handed over"
                    );
                }
            }
        }
    }
}

/// Why the lane never runs `request` itself, whatever the gate's state:
/// only `ApplicationMessages` are lane work, and a forged node-only message
/// is the loop's to refuse.
fn not_lane_runnable(request: &LaneRequest) -> Option<ForwardReason> {
    if !matches!(request.req, DelegateRequest::ApplicationMessages { .. }) {
        Some(ForwardReason::NotRunnable)
    } else if super::node_only_inbound(&request.req).is_some() {
        Some(ForwardReason::Forged)
    } else {
        None
    }
}

/// Forward `request` to the loop (counted by `reason`), marked loop-owned for
/// its user unless it is a forged node-only message the loop only refuses.
fn forward_to_loop(
    gate: &Arc<DelegateGate>,
    forward: &dyn LoopForward,
    request: LaneRequest,
    reason: ForwardReason,
) {
    record_forward(reason);
    let key = request.req.key().clone();
    tracing::debug!(
        delegate_key = %key,
        ?reason,
        "Delegate lane: forwarding a request to the contract loop"
    );
    let ticket = if reason == ForwardReason::Forged {
        LaneTicket::default()
    } else {
        let user = request.user_context.as_ref().map(|u| *u.user_id());
        LaneTicket(Some(gate.mark_loop_owned(&key, user)))
    };
    forward.forward(request, ticket);
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
    let user = request.user_context.as_ref().map(|u| *u.user_id());
    let run = match not_lane_runnable(&request) {
        Some(reason) => Err(reason),
        None => gate.try_begin_lane_run(&key, user),
    };
    let run = match run {
        Ok(run) => run,
        Err(reason) => {
            forward_to_loop(gate, forward, request, reason);
            return;
        }
    };
    LANE_RUNS.fetch_add(1, AtomicOrdering::Relaxed);
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

    /// A request as it reached the loop, with its ticket.
    type Forwarded = (LaneRequest, LaneTicket);

    /// Records forwarded requests, standing in for the loop's channel.
    struct Collect(tokio::sync::mpsc::UnboundedSender<Forwarded>);
    impl LoopForward for Collect {
        fn forward(&self, request: LaneRequest, ticket: LaneTicket) {
            self.0.send((request, ticket)).expect("test receiver alive");
        }
    }

    fn user(token: &[u8]) -> UserSecretContext {
        UserSecretContext::from_token(token)
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
        forwarded: tokio::sync::mpsc::UnboundedReceiver<Forwarded>,
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

        async fn next_forwarded(&mut self) -> Forwarded {
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
            gate.try_begin_lane_run(&k, None).is_err(),
            "claimed: lane must not run"
        );
        drop(claim);
        assert!(!gate.is_tracking(&k), "a clear entry is removed");
        let run = gate
            .try_begin_lane_run(&k, None)
            .expect("free after the claim drops");
        run.finish();
        assert!(!gate.is_tracking(&k));
    }

    #[tokio::test]
    async fn a_claim_waits_for_the_lane_run_and_takes_its_handoff() {
        let gate = DelegateGate::new();
        let k = key(2);
        let run = gate.try_begin_lane_run(&k, None).expect("free");
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
        let run = gate.try_begin_lane_run(&k, None).expect("free");
        let g2 = gate.clone();
        let k2 = k.clone();
        let claim = tokio::spawn(async move { g2.claim(&k2).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        run.finish();
        // The lane may get here before the claimer wakes; it must not start
        // another run of the delegate ahead of the waiting claim.
        assert!(gate.try_begin_lane_run(&k, None).is_err());
        let (c, _) = tokio::time::timeout(Duration::from_secs(5), claim)
            .await
            .unwrap()
            .unwrap();
        drop(c);
        assert!(gate.try_begin_lane_run(&k, None).is_ok());
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
        let run = gate.try_begin_lane_run(&k, None).expect("free");
        let waiting = tokio::time::timeout(Duration::from_millis(20), gate.claim(&k)).await;
        assert!(waiting.is_err(), "the claim waits for the lane run");
        run.finish();
        assert!(
            gate.try_begin_lane_run(&k, None).is_ok(),
            "a claim abandoned while waiting must not block the lane forever"
        );
    }

    /// A request for a delegate the loop holds is FORWARDED, not run; and
    /// once one request for a delegate is forwarded, the same user's later
    /// requests for it are forwarded too, in order, until the loop has
    /// finished the earlier ones (the tickets are dropped). Only then does
    /// the lane run the delegate again.
    ///
    /// FALSIFY: forward with an empty ticket (no `mark_loop_owned`) in
    /// `forward_to_loop`; the second request runs on the lane, ahead of the
    /// first one the loop still holds.
    #[tokio::test]
    async fn requests_follow_a_forwarded_request_to_the_loop_in_order() {
        let mut lane = Lane::start();
        let k = key(10);
        // The loop holds K (a notification run, say).
        let (claim, _) = lane.gate.claim(&k).await;
        let (r1, _a1) = request(&k, 1);
        lane.tx.send(r1).await.unwrap();
        let (first, t1) = lane.next_forwarded().await;
        assert_eq!(first.id, 1, "a request for a claimed delegate is forwarded");
        assert!(t1.is_held(), "and carries its loop-owned mark");
        drop(claim);

        // The loop has not yet processed the forwarded request: K is
        // loop-owned, so the next request must follow it, not overtake it.
        let (r2, _a2) = request(&k, 2);
        lane.tx.send(r2).await.unwrap();
        let (second, t2) = lane.next_forwarded().await;
        assert_eq!(second.id, 2, "a later request follows the forwarded one");
        assert_eq!(lane.runs(), 0, "nothing for K ran on the lane");

        // The loop finishes both; the lane runs K again.
        drop((first, t1));
        drop((second, t2));
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

    /// The loop-owned mark is per (delegate, USER): user A's forwarded
    /// request for K does not send user B's request for K to the loop, while
    /// A's own later request for K still follows A's first.
    ///
    /// FALSIFY: key the mark by delegate only (look up `loop_owned` without
    /// the user); B's request is forwarded too.
    #[tokio::test]
    async fn one_users_forwarded_request_does_not_hold_back_another_user() {
        let mut lane = Lane::start();
        let k = key(11);
        let (claim, _) = lane.gate.claim(&k).await;
        let (mut a1, _x1) = request(&k, 1);
        a1.user_context = Some(user(b"lane-user-a"));
        lane.tx.send(a1).await.unwrap();
        let held_a1 = lane.next_forwarded().await;
        drop(claim);

        // B: runs on the lane although A's request is still with the loop.
        let (mut b1, yb) = request(&k, 2);
        b1.user_context = Some(user(b"lane-user-b"));
        lane.tx.send(b1).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), yb)
            .await
            .expect("B answered")
            .expect("B answered on the lane");
        assert_eq!(lane.runs(), 1);

        // A again: follows A's first, still with the loop.
        let (mut a2, _x2) = request(&k, 3);
        a2.user_context = Some(user(b"lane-user-a"));
        lane.tx.send(a2).await.unwrap();
        let (fwd, _t) = lane.next_forwarded().await;
        assert_eq!(fwd.id, 3, "A's later request follows A's forwarded one");
        assert_eq!(lane.runs(), 1);
        drop(held_a1);
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
        let forwarded = lane.next_forwarded().await;
        assert_eq!(forwarded.0.id, 7);
        assert_eq!(lane.runs(), 0);
        // One unit taken by the loop: room again, the lane runs.
        let (claim, _) = lane.gate.take_next_unit().expect("a unit");
        drop(claim);
        drop(forwarded);
        let (r, a) = request(&free, 8);
        lane.tx.send(r).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), a)
            .await
            .expect("answered")
            .expect("answered on the lane");
        assert_eq!(lane.runs(), 1);
        lane.task.abort();
    }

    /// Registrations and unregistrations go to the loop's ordinary path; a
    /// flood of them for distinct keys leaves no lane state behind once the
    /// loop has dropped their tickets.
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
            let (r, ticket) = lane.next_forwarded().await;
            assert_eq!(r.id, i, "forwarded in order");
            assert!(ticket.is_held());
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

    /// An `ApplicationMessages` request carrying an inbound message only the
    /// node may deliver is FORWARDED to the loop's refusal, never run on the
    /// lane (whose runner has no such check), and carries no loop-owned mark.
    ///
    /// FALSIFY: drop the `node_only_inbound` branch in `serve`; the forged
    /// request runs on the lane.
    #[tokio::test]
    async fn a_forged_node_only_message_is_forwarded_not_run() {
        let mut lane = Lane::start();
        let k = key(12);
        let (r, _a) = request_with(
            &k,
            1,
            DelegateRequest::ApplicationMessages {
                key: k.clone(),
                params: freenet_stdlib::prelude::Parameters::from(Vec::new()),
                inbound: vec![freenet_stdlib::prelude::InboundDelegateMsg::UserResponse(
                    freenet_stdlib::prelude::UserInputResponse {
                        request_id: 1,
                        response: freenet_stdlib::prelude::ClientResponse::new(b"y".to_vec()),
                        context: freenet_stdlib::prelude::DelegateContext::default(),
                    },
                )],
            },
        );
        lane.tx.send(r).await.unwrap();
        let (fwd, ticket) = lane.next_forwarded().await;
        assert_eq!(fwd.id, 1);
        assert!(!ticket.is_held(), "a forged request marks nothing");
        assert_eq!(lane.runs(), 0, "the forged request did not run on the lane");
        lane.task.abort();
    }

    /// If the lane task dies outside a run, requests already buffered in its
    /// intake are forwarded to the loop instead of losing their responders.
    ///
    /// FALSIFY: drop the drain after the panic in `run_lane`; the buffered
    /// requests are never forwarded.
    #[tokio::test]
    async fn a_dying_lane_hands_its_buffered_requests_to_the_loop() {
        /// Panics on its first forward (outside any run), then collects.
        struct PanicsOnce(
            std::sync::atomic::AtomicBool,
            tokio::sync::mpsc::UnboundedSender<Forwarded>,
        );
        impl LoopForward for PanicsOnce {
            fn forward(&self, request: LaneRequest, ticket: LaneTicket) {
                if !self.0.swap(true, std::sync::atomic::Ordering::SeqCst) {
                    panic!("forwarder bug");
                }
                self.1.send((request, ticket)).expect("receiver alive");
            }
        }
        let gate = DelegateGate::new();
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        let (ftx, mut frx) = tokio::sync::mpsc::unbounded_channel();
        // Queued before the lane task first runs (current-thread runtime).
        for i in 1..=3u8 {
            let k = key(40 + i);
            let (r, a) = request_with(&k, i as u64, DelegateRequest::UnregisterDelegate(k.clone()));
            std::mem::forget(a);
            tx.try_send(r).expect("room");
        }
        let lane = tokio::spawn(run_lane(
            Arc::new(Counts(Default::default())),
            gate,
            Arc::new(PanicsOnce(Default::default(), ftx)),
            rx,
        ));
        tokio::time::timeout(Duration::from_secs(5), lane)
            .await
            .expect("the lane ends")
            .expect("its panic is caught");
        let mut ids = Vec::new();
        while let Ok((r, _)) = frx.try_recv() {
            ids.push(r.id);
        }
        assert_eq!(
            ids,
            vec![2, 3],
            "the buffered requests reach the loop, in order"
        );
        assert!(tx.is_closed());
    }

    /// A forwarder that panics again while the dying lane hands over its
    /// buffer loses only the request it panicked on; the rest still reach
    /// the loop.
    ///
    /// FALSIFY: call `forward_to_loop` in the drain without its
    /// `catch_unwind`; the second panic ends the task and requests 3 and 4
    /// are dropped.
    #[tokio::test]
    async fn a_panic_while_handing_over_loses_only_that_request() {
        /// Panics on its first two forwards, then collects.
        struct PanicsTwice(
            std::sync::atomic::AtomicUsize,
            tokio::sync::mpsc::UnboundedSender<Forwarded>,
        );
        impl LoopForward for PanicsTwice {
            fn forward(&self, request: LaneRequest, ticket: LaneTicket) {
                if self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst) < 2 {
                    panic!("forwarder bug");
                }
                self.1.send((request, ticket)).expect("receiver alive");
            }
        }
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        let (ftx, mut frx) = tokio::sync::mpsc::unbounded_channel();
        for i in 1..=4u8 {
            let k = key(60 + i);
            let (r, a) = request_with(&k, i as u64, DelegateRequest::UnregisterDelegate(k.clone()));
            std::mem::forget(a);
            tx.try_send(r).expect("room");
        }
        let lane = tokio::spawn(run_lane(
            Arc::new(Counts(Default::default())),
            DelegateGate::new(),
            Arc::new(PanicsTwice(Default::default(), ftx)),
            rx,
        ));
        tokio::time::timeout(Duration::from_secs(5), lane)
            .await
            .expect("the lane ends")
            .expect("both panics are caught");
        let mut ids = Vec::new();
        while let Ok((r, _)) = frx.try_recv() {
            ids.push(r.id);
        }
        assert_eq!(
            ids,
            vec![3, 4],
            "the requests after the second panic still reach the loop"
        );
    }

    /// A dying lane drains its intake with `recv`, so a sender that reserved
    /// a slot BEFORE the close still has its request handed to the loop.
    /// Each drained request is counted by its real reason (`LaneDown` for
    /// lane work), and the lane's death is counted apart from a caught run
    /// panic.
    ///
    /// The counters are process-global and only ever increase, so the test
    /// asserts on deltas. Another test that bumps the same counter
    /// concurrently can only make a delta larger, so a correct lane never
    /// fails here; it could hide a counting mutation in a rare interleaving.
    ///
    /// FALSIFY: drain with `while let Ok(r) = intake.try_recv()` (the
    /// reserved request is lost); count drained requests as `Busy` (the
    /// `LaneDown` delta is 0); or record the death with
    /// `record_delegate_lane_panic` (the death delta is 0).
    #[tokio::test]
    async fn a_dying_lane_waits_for_reserved_slots_and_counts_by_reason() {
        /// Panics on its first forward (outside any run), then collects.
        struct PanicsOnce(
            std::sync::atomic::AtomicBool,
            tokio::sync::mpsc::UnboundedSender<Forwarded>,
        );
        impl LoopForward for PanicsOnce {
            fn forward(&self, request: LaneRequest, ticket: LaneTicket) {
                if !self.0.swap(true, std::sync::atomic::Ordering::SeqCst) {
                    panic!("forwarder bug");
                }
                self.1.send((request, ticket)).expect("receiver alive");
            }
        }
        let load = |r: ForwardReason| FORWARDED[r.index()].load(AtomicOrdering::Relaxed);
        let deaths_before = crate::node::network_status::delegate_lane_deaths_total();
        let down_before = load(ForwardReason::LaneDown);
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        let (ftx, mut frx) = tokio::sync::mpsc::unbounded_channel();
        // Request 1 makes the forwarder panic, killing the lane.
        let k1 = key(50);
        let (r1, a1) = request_with(&k1, 1, DelegateRequest::UnregisterDelegate(k1.clone()));
        std::mem::forget(a1);
        tx.try_send(r1).expect("room");
        // A slot reserved before the lane dies, used only after the close.
        let permit = tx.clone().reserve_owned().await.expect("room");
        let lane = tokio::spawn(run_lane(
            Arc::new(Counts(Default::default())),
            DelegateGate::new(),
            Arc::new(PanicsOnce(Default::default(), ftx)),
            rx,
        ));
        // Current-thread runtime: the lane runs until it has closed its
        // intake and is draining.
        while !tx.is_closed() {
            tokio::task::yield_now().await;
        }
        let (r2, a2) = request(&key(51), 2);
        std::mem::forget(a2);
        permit.send(r2);
        tokio::time::timeout(Duration::from_secs(5), lane)
            .await
            .expect("the lane ends")
            .expect("its panic is caught");
        let mut ids = Vec::new();
        while let Ok((r, _)) = frx.try_recv() {
            ids.push(r.id);
        }
        assert_eq!(ids, vec![2], "the reserved request reaches the loop");
        assert!(
            load(ForwardReason::LaneDown) > down_before,
            "a drained ApplicationMessages request counts as LaneDown"
        );
        assert!(
            crate::node::network_status::delegate_lane_deaths_total() > deaths_before,
            "the lane's death is counted"
        );
    }

    /// The forwarded-count array is sized from the reasons, and each reason
    /// reads back its own counter.
    #[test]
    fn every_forward_reason_has_its_own_counter() {
        assert_eq!(FORWARDED.len(), ForwardReason::ALL.len());
        for (i, r) in ForwardReason::ALL.iter().enumerate() {
            assert_eq!(r.index(), i);
        }
    }

    /// Each reason's count lands in its own router snapshot field.
    ///
    /// FALSIFY: swap two fields in `RoutingCounts::write_into`'s match.
    #[test]
    fn each_forward_reason_lands_in_its_own_snapshot_field() {
        use arbitrary::{Arbitrary, Unstructured};
        let counts = RoutingCounts {
            lane_runs: 99,
            forwarded: std::array::from_fn(|i| 100 + i as u64),
        };
        let mut u = Unstructured::new(&[0u8; 4096]);
        let mut s = crate::router::RouterSnapshotInfo::arbitrary(&mut u)
            .expect("construct RouterSnapshotInfo for test");
        counts.write_into(&mut s);
        let want = |r: ForwardReason| Some(100 + r.index() as u64);
        assert_eq!(s.delegate_lane_runs, Some(99));
        assert_eq!(s.delegate_lane_forwarded_busy, want(ForwardReason::Busy));
        assert_eq!(
            s.delegate_lane_forwarded_parked,
            want(ForwardReason::Parked)
        );
        assert_eq!(
            s.delegate_lane_forwarded_loop_owned,
            want(ForwardReason::LoopOwned)
        );
        assert_eq!(
            s.delegate_lane_forwarded_unit_bound,
            want(ForwardReason::UnitBound)
        );
        assert_eq!(
            s.delegate_lane_forwarded_not_runnable,
            want(ForwardReason::NotRunnable)
        );
        assert_eq!(
            s.delegate_lane_forwarded_forged,
            want(ForwardReason::Forged)
        );
        assert_eq!(
            s.delegate_lane_forwarded_lane_down,
            want(ForwardReason::LaneDown)
        );
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
        let (ftx, _frx) = tokio::sync::mpsc::unbounded_channel::<Forwarded>();
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
        drop(gate.try_begin_lane_run(&k, None).expect("free"));
        let (_c, pending) = tokio::time::timeout(Duration::from_secs(5), gate.claim(&k))
            .await
            .expect("a dead lane run must not strand the loop");
        assert!(pending.is_none());
    }
}

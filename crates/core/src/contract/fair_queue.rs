//! Priority-tiered, per-contract fair queuing for the WASM executor event loop.
//!
//! Prevents a single contract from monopolizing the sequential event loop, AND
//! prevents best-effort background work (placement-migration caching,
//! interest-sync summaries, renewal) from starving a node's own client requests
//! (issue #4534).
//!
//! # Design
//!
//! Two layers of fairness:
//!
//! 1. **Across priority classes** ([`Priority`]) — events are bucketed into one
//!    [`PriorityTier`] per class. [`FairEventQueue::pop`] drains `ClientLocal`
//!    fully before `NetworkRelay`, which drains before `Background`. Admission
//!    ([`FairEventQueue::try_push`]) reserves `CLIENT_LOCAL_RESERVE` slots that
//!    only `ClientLocal` may use, and [`FairEventQueue::evict_background`] sheds
//!    queued background work to make room for higher-priority events.
//! 2. **Within a class** — each tier keeps the original per-contract round-robin:
//!    a `HashMap` of per-contract queues, a default queue for events with no
//!    contract identity (delegates, disconnects), and a `VecDeque<QueueKey>`
//!    round-robin order.
//!
//! Events are rejected with a [`RejectedEvent`] when:
//! - the per-contract-per-tier queue has reached `MAX_QUEUED_PER_CONTRACT`,
//! - a sub-`ClientLocal` push would cross the soft cap
//!   (`MAX_TOTAL_FAIR_QUEUE - CLIENT_LOCAL_RESERVE`), or
//! - any push would cross the hard `MAX_TOTAL_FAIR_QUEUE` cap.
//!
//! The caller is responsible for sending an appropriate error response for
//! rejected events (see `send_queue_full_response` in `contract.rs`); a shed
//! `Background` event is a best-effort drop that re-emits on its next cycle.

use std::collections::{HashMap, VecDeque};

use freenet_stdlib::prelude::ContractInstanceId;

use super::handler::{ContractHandlerEvent, EventId};

/// Maximum events queued per contract before rejection.
/// A legitimate contract under load might have 10-20 pending operations.
/// 100 allows burst headroom (5-10x) while catching abuse.
pub(super) const MAX_QUEUED_PER_CONTRACT: usize = 100;

/// Maximum total events across all queues.
/// Global backpressure to bound total memory usage.
/// Set below MAX_PENDING_REQUESTS (10,000) so backpressure propagates
/// before the mediator hits its limit.
pub(super) const MAX_TOTAL_FAIR_QUEUE: usize = 5_000;

/// Admission capacity reserved for [`Priority::ClientLocal`] events.
///
/// Once `total_queued` reaches `MAX_TOTAL_FAIR_QUEUE - CLIENT_LOCAL_RESERVE`,
/// `NetworkRelay` and `Background` pushes are refused (after a Background-shed
/// attempt), but `ClientLocal` pushes keep being admitted into the reserve up
/// to the hard `MAX_TOTAL_FAIR_QUEUE` cap. This guarantees a locally-originated
/// client request on THIS node is never rejected with "contract queue full"
/// merely because background placement-migration / summarize churn (issue
/// #4534) has filled the queue. Sized to comfortably hold an interactive
/// client's in-flight working set without materially shrinking the shared cap.
pub(super) const CLIENT_LOCAL_RESERVE: usize = 256;

/// Maximum events to drain from channel per iteration.
/// Prevents the drain loop from blocking delegate notifications too long.
pub(super) const MAX_DRAIN_BATCH: usize = 256;

/// Scheduling/admission priority class for a contract-handler event (#4534).
///
/// The contract event loop is single-threaded, so a flood of best-effort
/// background work (placement-migration caching, interest-sync summaries,
/// renewal) can starve a node's own client requests. Tagging events with a
/// priority class lets [`FairEventQueue`] (a) drain higher classes first,
/// (b) reserve admission capacity for client requests, and (c) shed/evict
/// background work to make room.
///
/// Ordered low → high so `>=`/`cmp` reflect precedence
/// (`ClientLocal > NetworkRelay > Background`).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub(crate) enum Priority {
    /// Best-effort, node-internal background work (summarize for interest-sync,
    /// placement-migration caching, renewal, contract eviction). Shed FIRST
    /// under pressure; never surfaces a client-facing error when dropped.
    Background,
    /// Serving a remote peer's relayed operation. Normal precedence.
    NetworkRelay,
    /// Originated by a WS/HTTP client connected to THIS node. Highest
    /// precedence; admitted into the reserved lane so it is never rejected
    /// because background/relay work filled the queue.
    ClientLocal,
}

impl Priority {
    /// The class that untagged / legacy callers default to. `NetworkRelay` is
    /// the safe middle: it never starves clients more than today's flat queue
    /// did, and it is not silently sheddable like `Background`.
    pub(crate) const DEFAULT: Priority = Priority::NetworkRelay;
}

/// Key identifying which queue an event belongs to.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum QueueKey {
    /// Event associated with a specific contract.
    Contract(ContractInstanceId),
    /// Event with no contract identity (delegate requests, client disconnects, etc.).
    Default,
}

/// Why an event was refused admission. Lets the caller decide whether shedding
/// `Background` could help: it can only free space for a *global-capacity*
/// rejection, never a *per-contract* one (#4534 review).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum RejectReason {
    /// The hard `MAX_TOTAL_FAIR_QUEUE` cap, or the soft
    /// `MAX_TOTAL_FAIR_QUEUE - CLIENT_LOCAL_RESERVE` cap for a sub-`ClientLocal`
    /// class. Evicting `Background` can free a slot and let a retry succeed.
    GlobalCapacity,
    /// This event's own per-contract-per-tier queue is at
    /// `MAX_QUEUED_PER_CONTRACT`. Evicting `Background` (a different tier and/or
    /// contract) cannot help; a retry would fail identically.
    PerContract,
    /// An event evicted from the queue (not an admission failure). Carried so a
    /// shed `Background` event flows through the same response path.
    Evicted,
}

/// An event that was rejected (admission refused) or evicted (dropped after
/// being queued) to protect higher-priority capacity.
pub(super) struct RejectedEvent {
    pub id: EventId,
    pub event: ContractHandlerEvent,
    /// Priority class of the rejected/evicted event — lets the caller log the
    /// shed tier and decide whether a client-facing error is warranted
    /// (`Background` sheds are silent best-effort drops).
    pub priority: Priority,
    /// Why this event was rejected (or that it was evicted).
    pub reason: RejectReason,
}

impl std::fmt::Debug for RejectedEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RejectedEvent")
            .field("id", &self.id.id)
            .field("event", &self.event)
            .field("priority", &self.priority)
            .field("reason", &self.reason)
            .finish()
    }
}

/// One queued event together with its priority class.
type QueuedItem = (EventId, ContractHandlerEvent, Priority);

/// A single priority tier: per-contract round-robin scheduling within the tier.
///
/// Each tier is self-contained — its own per-contract queues, default queue,
/// round-robin order, and count — so the parent [`FairEventQueue`] can drain
/// higher tiers to exhaustion before lower ones and evict a whole tier without
/// disturbing the others.
#[derive(Default)]
struct PriorityTier {
    contract_queues: HashMap<ContractInstanceId, VecDeque<QueuedItem>>,
    default_queue: VecDeque<QueuedItem>,
    round_robin: VecDeque<QueueKey>,
    queued: usize,
}

impl PriorityTier {
    /// Per-contract (or default) length for the routing key of `event`.
    fn len_for(&self, event: &ContractHandlerEvent) -> usize {
        match extract_contract_id(event) {
            Some(cid) => self.contract_queues.get(&cid).map_or(0, VecDeque::len),
            None => self.default_queue.len(),
        }
    }

    /// Enqueue, registering the routing key in the round-robin when its queue
    /// transitions from empty. Caller has already enforced capacity.
    fn push(&mut self, item: QueuedItem) {
        match extract_contract_id(&item.1) {
            Some(cid) => {
                let queue = self.contract_queues.entry(cid).or_default();
                if queue.is_empty() {
                    self.round_robin.push_back(QueueKey::Contract(cid));
                }
                queue.push_back(item);
            }
            None => {
                if self.default_queue.is_empty() {
                    self.round_robin.push_back(QueueKey::Default);
                }
                self.default_queue.push_back(item);
            }
        }
        self.queued += 1;
    }

    /// Pop one event in round-robin order across this tier's contracts.
    fn pop(&mut self) -> Option<QueuedItem> {
        loop {
            let key = self.round_robin.pop_front()?;
            match key {
                QueueKey::Contract(cid) => {
                    if let Some(queue) = self.contract_queues.get_mut(&cid) {
                        if let Some(item) = queue.pop_front() {
                            self.queued = self.queued.saturating_sub(1);
                            if queue.is_empty() {
                                self.contract_queues.remove(&cid);
                            } else {
                                self.round_robin.push_back(key);
                            }
                            return Some(item);
                        }
                        self.contract_queues.remove(&cid);
                    }
                }
                QueueKey::Default => {
                    if let Some(item) = self.default_queue.pop_front() {
                        self.queued = self.queued.saturating_sub(1);
                        if !self.default_queue.is_empty() {
                            self.round_robin.push_back(QueueKey::Default);
                        }
                        return Some(item);
                    }
                }
            }
        }
    }
}

/// Per-contract fair event queue with priority tiering (#4534).
///
/// Two layers of fairness:
/// 1. **Across priority classes** — [`Priority::ClientLocal`] drains before
///    `NetworkRelay`, which drains before `Background`. A locally-originated
///    client request is never head-of-line-blocked by background migration /
///    summarize churn, and gets a reserved admission lane plus the ability to
///    evict queued `Background` work to make room.
/// 2. **Within a class** — the original per-contract round-robin, so no single
///    contract monopolizes the sequential executor loop.
pub(super) struct FairEventQueue {
    /// One tier per `Priority`, indexed by `priority as usize`
    /// (`Background`=0, `NetworkRelay`=1, `ClientLocal`=2).
    tiers: [PriorityTier; 3],
    /// Total events across all tiers (global backpressure bound).
    total_queued: usize,
    /// Consecutive `pop()`s that served `ClientLocal` while a lower tier had
    /// work waiting. Drives the anti-starvation floor (see `pop`): once this
    /// reaches `RELAY_STARVATION_FLOOR`, the next `pop` force-serves the highest
    /// non-empty lower tier so sustained client load cannot indefinitely starve
    /// remote relay / background execution (#4534 review).
    client_streak: usize,
}

/// After this many consecutive `ClientLocal` pops (while lower-tier work is
/// waiting), `pop()` serves one lower-tier event regardless of pending
/// `ClientLocal`. Bounds relay/background starvation latency to roughly one in
/// `RELAY_STARVATION_FLOOR + 1` pops while still strongly favouring client work.
pub(super) const RELAY_STARVATION_FLOOR: usize = 16;

impl FairEventQueue {
    /// Create a new, empty fair event queue.
    pub(super) fn new() -> Self {
        Self {
            tiers: Default::default(),
            total_queued: 0,
            client_streak: 0,
        }
    }

    fn tier(&mut self, priority: Priority) -> &mut PriorityTier {
        &mut self.tiers[priority as usize]
    }

    /// Attempt to enqueue an event at the given priority.
    ///
    /// Admission rules (issue #4534):
    /// - The per-contract-per-tier cap `MAX_QUEUED_PER_CONTRACT` always applies.
    /// - `ClientLocal` is admitted up to the hard `MAX_TOTAL_FAIR_QUEUE` cap —
    ///   it may use the `CLIENT_LOCAL_RESERVE` headroom that lower classes
    ///   cannot, so a client request is never refused because background/relay
    ///   work filled the queue.
    /// - `NetworkRelay` / `Background` are refused once
    ///   `total_queued >= MAX_TOTAL_FAIR_QUEUE - CLIENT_LOCAL_RESERVE`. Before
    ///   refusing, the caller-facing [`try_push`] first tries to evict queued
    ///   `Background` to make room (see [`evict_background`]).
    ///
    /// Returns `Err(Box<RejectedEvent>)` (tagged with `priority`) when the event
    /// cannot be admitted.
    pub(super) fn try_push(
        &mut self,
        id: EventId,
        event: ContractHandlerEvent,
        priority: Priority,
    ) -> Result<(), Box<RejectedEvent>> {
        // Hard global cap — never exceeded by any class.
        if self.total_queued >= MAX_TOTAL_FAIR_QUEUE {
            return Err(Box::new(RejectedEvent {
                id,
                event,
                priority,
                reason: RejectReason::GlobalCapacity,
            }));
        }

        // Soft cap: classes below ClientLocal must leave the reserve free. If a
        // foreground push (ClientLocal/NetworkRelay) would cross the soft cap,
        // the caller can first try to reclaim space by shedding already-queued
        // Background (a GlobalCapacity rejection is eviction-recoverable).
        let soft_cap = MAX_TOTAL_FAIR_QUEUE - CLIENT_LOCAL_RESERVE;
        if priority < Priority::ClientLocal && self.total_queued >= soft_cap {
            return Err(Box::new(RejectedEvent {
                id,
                event,
                priority,
                reason: RejectReason::GlobalCapacity,
            }));
        }

        // Per-contract-per-tier cap (DoS protection, preserved from the
        // original flat queue but now scoped to the tier). Evicting Background
        // cannot help here, so this is flagged PerContract.
        if self.tier(priority).len_for(&event) >= MAX_QUEUED_PER_CONTRACT {
            return Err(Box::new(RejectedEvent {
                id,
                event,
                priority,
                reason: RejectReason::PerContract,
            }));
        }

        self.tier(priority).push((id, event, priority));
        self.total_queued += 1;
        Ok(())
    }

    /// Evict up to `max` oldest `Background` events to free admission capacity
    /// for higher-priority work (#4534). Returns the evicted events so the
    /// caller can fire best-effort "queue full" responses for them — a shed
    /// `Background` task (summarize / migration caching / renewal) is
    /// re-emitted on its next cycle, so dropping it is non-fatal.
    pub(super) fn evict_background(&mut self, max: usize) -> Vec<RejectedEvent> {
        let mut evicted = Vec::new();
        for _ in 0..max {
            match self.tiers[Priority::Background as usize].pop() {
                Some((id, event, priority)) => {
                    self.total_queued = self.total_queued.saturating_sub(1);
                    evicted.push(RejectedEvent {
                        id,
                        event,
                        priority,
                        reason: RejectReason::Evicted,
                    });
                }
                None => break,
            }
        }
        evicted
    }

    /// Number of queued `Background`-tier events (for shed/admission decisions).
    pub(super) fn background_queued(&self) -> usize {
        self.tiers[Priority::Background as usize].queued
    }

    /// Pop one event in round-robin order.
    ///
    /// Takes the front key from the round-robin, pops one event from that queue.
    /// If the queue still has items, the key is moved to the back (round-robin).
    /// If the queue is now empty, the key is removed.
    ///
    /// Pop the next event, strongly favouring higher priority classes but with a
    /// bounded anti-starvation floor (#4534 + review).
    ///
    /// Normally serves the highest non-empty tier (`ClientLocal` → `NetworkRelay`
    /// → `Background`), round-robin within the tier — so newly-arrived background
    /// work never head-of-line-blocks a pending client request ("don't START new
    /// background ahead of a client"; an already-running WASM compile still
    /// finishes).
    ///
    /// To avoid the strict-priority failure mode where sustained `ClientLocal`
    /// load indefinitely starves remote relay/background execution, after
    /// `RELAY_STARVATION_FLOOR` consecutive `ClientLocal` pops *with lower-tier
    /// work waiting*, one lower-tier event is served instead. This bounds
    /// relay/background latency while keeping the overwhelming majority of slots
    /// for client work.
    ///
    /// Returns `None` if all tiers are empty.
    pub(super) fn pop(&mut self) -> Option<(EventId, ContractHandlerEvent)> {
        let client_nonempty = self.tiers[Priority::ClientLocal as usize].queued > 0;
        let lower_nonempty = self.tiers[Priority::NetworkRelay as usize].queued > 0
            || self.tiers[Priority::Background as usize].queued > 0;

        // Anti-starvation: if ClientLocal has monopolized the last
        // RELAY_STARVATION_FLOOR pops and a lower tier is waiting, serve the
        // highest non-empty lower tier this round and reset the streak.
        let force_lower =
            client_nonempty && lower_nonempty && self.client_streak >= RELAY_STARVATION_FLOOR;

        let order: [Priority; 3] = if force_lower {
            // Skip ClientLocal this round; drain NetworkRelay then Background.
            [
                Priority::NetworkRelay,
                Priority::Background,
                // ClientLocal last as a safety net (only reached if both lower
                // tiers raced to empty between the check and the pop).
                Priority::ClientLocal,
            ]
        } else {
            [
                Priority::ClientLocal,
                Priority::NetworkRelay,
                Priority::Background,
            ]
        };

        for priority in order {
            if let Some((id, event, _)) = self.tiers[priority as usize].pop() {
                debug_assert!(self.total_queued > 0);
                self.total_queued = self.total_queued.saturating_sub(1);
                // Track the ClientLocal streak only while lower work waits — an
                // uncontested client burst should not trip the floor.
                if priority == Priority::ClientLocal && lower_nonempty {
                    self.client_streak += 1;
                } else {
                    self.client_streak = 0;
                }
                return Some((id, event));
            }
        }
        None
    }

    /// Returns `true` if all queues are empty.
    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.total_queued == 0
    }

    /// Returns the total number of queued events across all contracts.
    #[cfg(test)]
    pub(super) fn total_queued(&self) -> usize {
        self.total_queued
    }
}

/// Extract the contract identity from an event for queue routing.
///
/// Returns `Some(ContractInstanceId)` for events associated with a specific contract,
/// or `None` for events with no contract identity (delegate requests, disconnects, etc.).
fn extract_contract_id(event: &ContractHandlerEvent) -> Option<ContractInstanceId> {
    match event {
        ContractHandlerEvent::PutQuery { key, .. }
        | ContractHandlerEvent::UpdateQuery { key, .. }
        | ContractHandlerEvent::GetSummaryQuery { key, .. }
        | ContractHandlerEvent::GetDeltaQuery { key, .. }
        // EvictContract is routed to its contract's per-key queue so disk
        // reclamation is serialized with any other in-flight ops on that
        // key (e.g. a concurrent GET/PUT touching the same contract).
        | ContractHandlerEvent::EvictContract { key, .. } => Some(*key.id()),
        ContractHandlerEvent::GetQuery { instance_id, .. }
        | ContractHandlerEvent::RegisterSubscriberListener {
            key: instance_id, ..
        } => Some(*instance_id),
        // These events have no contract identity and are routed to the default queue.
        ContractHandlerEvent::DelegateRequest { .. }
        | ContractHandlerEvent::DelegateResponse(_)
        | ContractHandlerEvent::ExportUserSecrets { .. }
        | ContractHandlerEvent::ExportUserSecretsResponse(_)
        | ContractHandlerEvent::ImportSecrets { .. }
        | ContractHandlerEvent::ImportSecretsResponse(_)
        | ContractHandlerEvent::PutResponse { .. }
        | ContractHandlerEvent::GetResponse { .. }
        | ContractHandlerEvent::UpdateResponse { .. }
        | ContractHandlerEvent::UpdateNoChange { .. }
        | ContractHandlerEvent::RegisterSubscriberListenerResponse { .. }
        | ContractHandlerEvent::QuerySubscriptions { .. }
        | ContractHandlerEvent::QuerySubscriptionsResponse
        | ContractHandlerEvent::GetSummaryResponse { .. }
        | ContractHandlerEvent::GetDeltaResponse { .. }
        | ContractHandlerEvent::ClientDisconnect { .. }
        | ContractHandlerEvent::DropSubscriberListener { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use freenet_stdlib::prelude::{
        ContractCode, ContractKey, Parameters, RelatedContracts, WrappedState,
    };

    use crate::contract::handler::EventId;

    fn make_contract_id(seed: u8) -> ContractInstanceId {
        make_contract_id_u32(seed as u32)
    }

    fn make_contract_id_u32(seed: u32) -> ContractInstanceId {
        let bytes = seed.to_le_bytes();
        let code = ContractCode::from(bytes.repeat(8)); // 32 bytes
        let params = Parameters::from(bytes.repeat(2)); // 8 bytes
        let key = ContractKey::from_params_and_code(&params, &code);
        *key.id()
    }

    fn make_event_id(id: u64) -> EventId {
        EventId { id }
    }

    fn make_get_event(contract_id: ContractInstanceId) -> ContractHandlerEvent {
        ContractHandlerEvent::GetQuery {
            instance_id: contract_id,
            return_contract_code: false,
        }
    }

    /// Extract the routed contract id from a `GetQuery` test event.
    fn get_cid(event: ContractHandlerEvent) -> ContractInstanceId {
        let ContractHandlerEvent::GetQuery { instance_id, .. } = event else {
            panic!("expected GetQuery event");
        };
        instance_id
    }

    fn make_delegate_event() -> ContractHandlerEvent {
        ContractHandlerEvent::ClientDisconnect {
            client_id: crate::client_events::ClientId::next(),
        }
    }

    impl FairEventQueue {
        /// Test shim: push at the default (`NetworkRelay`) priority.
        fn try_push_default(
            &mut self,
            id: EventId,
            event: ContractHandlerEvent,
        ) -> Result<(), Box<RejectedEvent>> {
            self.try_push(id, event, Priority::DEFAULT)
        }

        /// Test helper: total events queued in the given tier.
        fn tier_queued(&self, priority: Priority) -> usize {
            self.tiers[priority as usize].queued
        }
    }

    #[test]
    fn test_round_robin_fairness() {
        let mut queue = FairEventQueue::new();
        let a = make_contract_id(1);
        let b = make_contract_id(2);

        // Push 10 events for A, then 10 for B
        for i in 0..10u64 {
            queue
                .try_push_default(make_event_id(i), make_get_event(a))
                .expect("push should succeed");
        }
        for i in 10..20u64 {
            queue
                .try_push_default(make_event_id(i), make_get_event(b))
                .expect("push should succeed");
        }

        // Pop all — should interleave: A, B, A, B, ...
        let mut results = Vec::new();
        while let Some((_, event)) = queue.pop() {
            let ContractHandlerEvent::GetQuery { instance_id, .. } = event else {
                panic!("unexpected event type");
            };
            results.push(instance_id);
        }

        assert_eq!(results.len(), 20);
        for i in 0..10 {
            assert_eq!(results[i * 2], a, "expected A at position {}", i * 2);
            assert_eq!(
                results[i * 2 + 1],
                b,
                "expected B at position {}",
                i * 2 + 1
            );
        }
    }

    #[test]
    fn test_per_contract_limit() {
        let mut queue = FairEventQueue::new();
        let contract_id = make_contract_id(1);

        // Fill to the limit
        for i in 0..MAX_QUEUED_PER_CONTRACT as u64 {
            queue
                .try_push_default(make_event_id(i), make_get_event(contract_id))
                .expect("should succeed within limit");
        }

        // One more should be rejected
        let result = queue.try_push_default(
            make_event_id(MAX_QUEUED_PER_CONTRACT as u64),
            make_get_event(contract_id),
        );
        assert!(
            result.is_err(),
            "should reject when per-contract limit exceeded"
        );
    }

    /// Fill the queue with `priority` events across many contracts up to `target`
    /// total. Each contract holds at most `MAX_QUEUED_PER_CONTRACT`. Returns the
    /// next unused event id.
    fn fill_to(queue: &mut FairEventQueue, priority: Priority, target: usize) -> u64 {
        let mut pushed = queue.total_queued();
        // Start high so fill_to's synthetic contracts never collide with the
        // small hand-picked seeds individual tests use.
        let mut contract_seed = 1_000_000u32;
        let mut event_id = 1_000_000u64;
        while pushed < target {
            let batch = (target - pushed).min(MAX_QUEUED_PER_CONTRACT);
            // Vary the contract id by seed so we never hit the per-contract cap.
            let contract_id = make_contract_id_u32(contract_seed);
            for _ in 0..batch {
                queue
                    .try_push(
                        make_event_id(event_id),
                        make_get_event(contract_id),
                        priority,
                    )
                    .expect("should succeed below the relevant cap");
                event_id += 1;
                pushed += 1;
            }
            contract_seed = contract_seed.wrapping_add(1);
        }
        event_id
    }

    #[test]
    fn test_foreground_soft_cap_then_client_reserve_then_hard_cap() {
        let mut queue = FairEventQueue::new();
        let soft_cap = MAX_TOTAL_FAIR_QUEUE - CLIENT_LOCAL_RESERVE;

        // Foreground (NetworkRelay) fills to the soft cap, then is refused —
        // the CLIENT_LOCAL_RESERVE headroom stays free for client work.
        let mut event_id = fill_to(&mut queue, Priority::NetworkRelay, soft_cap);
        assert_eq!(queue.total_queued(), soft_cap);
        let relay_over = queue.try_push(
            make_event_id(event_id),
            make_get_event(make_contract_id_u32(9_999)),
            Priority::NetworkRelay,
        );
        assert!(
            relay_over.is_err(),
            "NetworkRelay must be refused once the soft cap (reserve) is reached"
        );
        event_id += 1;

        // ClientLocal keeps being admitted into the reserve, up to the hard cap.
        event_id = fill_to(&mut queue, Priority::ClientLocal, MAX_TOTAL_FAIR_QUEUE).max(event_id);
        assert_eq!(queue.total_queued(), MAX_TOTAL_FAIR_QUEUE);

        // Even ClientLocal is refused at the hard cap.
        let client_over = queue.try_push(
            make_event_id(event_id),
            make_get_event(make_contract_id_u32(8_888)),
            Priority::ClientLocal,
        );
        assert!(
            client_over.is_err(),
            "ClientLocal must still be refused at the hard MAX_TOTAL_FAIR_QUEUE cap"
        );
    }

    #[test]
    fn test_default_queue_participates() {
        let mut queue = FairEventQueue::new();
        let contract_id = make_contract_id(1);

        // Push one delegate (default) event and one contract event
        queue
            .try_push_default(make_event_id(0), make_delegate_event())
            .expect("push should succeed");
        queue
            .try_push_default(make_event_id(1), make_get_event(contract_id))
            .expect("push should succeed");

        assert_eq!(queue.total_queued(), 2);

        // Both should be popped
        let first = queue.pop();
        let second = queue.pop();
        let third = queue.pop();

        assert!(first.is_some(), "first pop should return an event");
        assert!(second.is_some(), "second pop should return an event");
        assert!(third.is_none(), "third pop should be empty");
    }

    #[test]
    fn test_empty_queue_removal() {
        let mut queue = FairEventQueue::new();
        let contract_id = make_contract_id(1);

        queue
            .try_push_default(make_event_id(0), make_get_event(contract_id))
            .expect("push should succeed");
        queue
            .try_push_default(make_event_id(1), make_get_event(contract_id))
            .expect("push should succeed");

        // Pop both events
        queue.pop().expect("first pop should succeed");
        queue.pop().expect("second pop should succeed");

        // Queue should be empty, and every tier's internal maps fully drained.
        assert!(queue.is_empty());
        for tier in &queue.tiers {
            assert!(
                tier.contract_queues.is_empty(),
                "contract queue map should be empty"
            );
            assert!(tier.round_robin.is_empty(), "round_robin should be empty");
            assert_eq!(tier.queued, 0, "tier count should be zero");
        }
    }

    #[test]
    fn test_single_contract_preserves_order() {
        let mut queue = FairEventQueue::new();
        let contract_id = make_contract_id(1);

        // Push events with distinct IDs to track ordering
        for i in 0..5u64 {
            queue
                .try_push_default(make_event_id(i), make_get_event(contract_id))
                .expect("push should succeed");
        }

        // Pop all and verify order matches insertion order
        let mut popped_ids = Vec::new();
        while let Some((id, _)) = queue.pop() {
            popped_ids.push(id.id);
        }

        assert_eq!(popped_ids, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn test_stress_many_contracts() {
        let mut queue = FairEventQueue::new();
        // Stay within the NetworkRelay soft cap (MAX_TOTAL - reserve = 4744):
        // 47 * 100 = 4700 fits; the reserve stays free for ClientLocal.
        let num_contracts = 47usize;
        let events_per_contract = MAX_QUEUED_PER_CONTRACT;

        // Push exactly events_per_contract events for each of num_contracts contracts
        let mut event_id = 0u64;
        for seed in 0..num_contracts as u8 {
            let contract_id = make_contract_id(seed);
            for _ in 0..events_per_contract {
                queue
                    .try_push_default(make_event_id(event_id), make_get_event(contract_id))
                    .expect("push should succeed");
                event_id += 1;
            }
        }

        assert_eq!(queue.total_queued(), num_contracts * events_per_contract);

        // Pop all events and count per-contract
        let mut per_contract_counts: HashMap<ContractInstanceId, usize> = HashMap::new();
        while let Some((_, event)) = queue.pop() {
            let ContractHandlerEvent::GetQuery { instance_id, .. } = event else {
                panic!("unexpected event type");
            };
            *per_contract_counts.entry(instance_id).or_insert(0) += 1;
        }

        // Each contract should have exactly events_per_contract events processed
        assert_eq!(per_contract_counts.len(), num_contracts);
        for count in per_contract_counts.values() {
            assert_eq!(*count, events_per_contract);
        }

        assert!(queue.is_empty());
    }

    #[test]
    fn test_stress_mixed_load() {
        let mut queue = FairEventQueue::new();
        let hot_contract = make_contract_id(0);
        let num_cold = 99usize;

        // Push MAX_QUEUED_PER_CONTRACT events for hot contract
        for i in 0..MAX_QUEUED_PER_CONTRACT as u64 {
            queue
                .try_push_default(make_event_id(i), make_get_event(hot_contract))
                .expect("push should succeed");
        }

        // Push 1 event each for cold contracts
        let mut event_id = MAX_QUEUED_PER_CONTRACT as u64;
        for seed in 1..=num_cold as u8 {
            let contract_id = make_contract_id(seed);
            queue
                .try_push_default(make_event_id(event_id), make_get_event(contract_id))
                .expect("push should succeed");
            event_id += 1;
        }

        let total = MAX_QUEUED_PER_CONTRACT + num_cold;
        assert_eq!(queue.total_queued(), total);

        // Pop all events — cold contracts should not be starved
        // In round-robin: hot gets 1 slot per rotation, cold contracts get 1 slot each
        // After all cold contracts are served (99 pops), the hot contract gets 99 of its 100 events
        // processed as part of the rotation. The final 1 hot event comes last.
        let mut hot_positions = Vec::new();
        let mut cold_positions = Vec::new();

        let mut position = 0;
        while let Some((_, event)) = queue.pop() {
            let ContractHandlerEvent::GetQuery { instance_id, .. } = event else {
                panic!("unexpected event type");
            };
            if instance_id == hot_contract {
                hot_positions.push(position);
            } else {
                cold_positions.push(position);
            }
            position += 1;
        }

        assert_eq!(hot_positions.len(), MAX_QUEUED_PER_CONTRACT);
        assert_eq!(cold_positions.len(), num_cold);

        // The hot contract should not have all its events processed before any cold events.
        // With round-robin, hot events are interleaved with cold events.
        // At least some cold contracts should appear before the hot contract's last event.
        let last_cold = cold_positions.iter().max().copied().unwrap_or(0);
        let first_hot = hot_positions.iter().min().copied().unwrap_or(usize::MAX);

        // Hot contract's first event should appear before some cold events (they interleave)
        assert!(
            first_hot < last_cold,
            "hot contract events should interleave with cold contract events"
        );

        // The hot contract should not dominate: its events should be spread out
        // (not all consecutive). Verify by checking that cold contracts appear
        // before hot contract exhausts its queue.
        assert_eq!(position, total, "all events should be popped");
    }

    #[test]
    fn test_is_empty() {
        let mut queue = FairEventQueue::new();
        assert!(queue.is_empty());

        let contract_id = make_contract_id(1);
        queue
            .try_push_default(make_event_id(0), make_get_event(contract_id))
            .expect("push should succeed");
        assert!(!queue.is_empty());

        queue.pop();
        assert!(queue.is_empty());
    }

    #[test]
    fn test_put_event_routing() {
        let mut queue = FairEventQueue::new();
        let code = ContractCode::from(vec![42u8; 32]);
        let params = Parameters::from(vec![7u8; 8]);
        let key = ContractKey::from_params_and_code(&params, &code);
        let contract_id = *key.id();

        // PutQuery should be routed to the contract's queue
        let event = ContractHandlerEvent::PutQuery {
            key,
            state: WrappedState::new(vec![1, 2, 3]),
            related_contracts: RelatedContracts::default(),
            contract: None,
        };

        queue
            .try_push_default(make_event_id(0), event)
            .expect("push should succeed");

        assert_eq!(queue.total_queued(), 1);
        // Default tier (NetworkRelay) holds it under the routed contract key.
        assert!(
            queue.tiers[Priority::DEFAULT as usize]
                .contract_queues
                .contains_key(&contract_id)
        );
    }

    #[test]
    fn test_default_queue_per_slot_limit() {
        let mut queue = FairEventQueue::new();

        // Fill default queue to per-slot limit with ClientDisconnect events
        for i in 0..MAX_QUEUED_PER_CONTRACT as u64 {
            queue
                .try_push_default(make_event_id(i), make_delegate_event())
                .expect("should succeed within limit");
        }

        // One more should be rejected
        let result = queue.try_push_default(
            make_event_id(MAX_QUEUED_PER_CONTRACT as u64),
            make_delegate_event(),
        );
        assert!(
            result.is_err(),
            "should reject when default queue per-slot limit exceeded"
        );
    }

    #[test]
    fn test_interleaved_push_pop() {
        let mut queue = FairEventQueue::new();
        let a = make_contract_id(1);
        let b = make_contract_id(2);

        // Push event for A, pop (gets A)
        queue
            .try_push_default(make_event_id(0), make_get_event(a))
            .unwrap();
        let (id0, _) = queue.pop().expect("should pop A");
        assert_eq!(id0.id, 0);

        // Push events for B and A
        queue
            .try_push_default(make_event_id(1), make_get_event(b))
            .unwrap();
        queue
            .try_push_default(make_event_id(2), make_get_event(a))
            .unwrap();

        // Round-robin should give B first (B was added after A was drained,
        // so B is at front of round_robin)
        let (id1, ev1) = queue.pop().expect("should pop B");
        let ContractHandlerEvent::GetQuery { instance_id, .. } = ev1 else {
            panic!("unexpected event type");
        };
        assert_eq!(instance_id, b, "expected B (id={})", id1.id);

        // Then A
        let (id2, ev2) = queue.pop().expect("should pop A");
        let ContractHandlerEvent::GetQuery { instance_id, .. } = ev2 else {
            panic!("unexpected event type");
        };
        assert_eq!(instance_id, a, "expected A (id={})", id2.id);

        assert!(queue.pop().is_none());
    }

    #[test]
    fn test_interleaved_three_contracts() {
        let mut queue = FairEventQueue::new();
        let a = make_contract_id(1);
        let b = make_contract_id(2);
        let c = make_contract_id(3);

        // Fill: A(3), B(2), C(1)
        for i in 0..3u64 {
            queue
                .try_push_default(make_event_id(i), make_get_event(a))
                .unwrap();
        }
        for i in 3..5u64 {
            queue
                .try_push_default(make_event_id(i), make_get_event(b))
                .unwrap();
        }
        queue
            .try_push_default(make_event_id(5), make_get_event(c))
            .unwrap();

        // Pop 3 — should be one from each (A, B, C in round-robin)
        let mut first_round = Vec::new();
        for _ in 0..3 {
            let (_, ev) = queue.pop().unwrap();
            let ContractHandlerEvent::GetQuery { instance_id, .. } = ev else {
                panic!("unexpected");
            };
            first_round.push(instance_id);
        }
        assert_eq!(first_round, vec![a, b, c]);

        // C is now drained, so second round is A, B
        let mut second_round = Vec::new();
        for _ in 0..2 {
            let (_, ev) = queue.pop().unwrap();
            let ContractHandlerEvent::GetQuery { instance_id, .. } = ev else {
                panic!("unexpected");
            };
            second_round.push(instance_id);
        }
        assert_eq!(second_round, vec![a, b]);

        // Third round: only A remains
        let (_, ev) = queue.pop().unwrap();
        let ContractHandlerEvent::GetQuery { instance_id, .. } = ev else {
            panic!("unexpected");
        };
        assert_eq!(instance_id, a);

        assert!(queue.pop().is_none());
    }

    /// Test that extract_contract_id correctly routes all event variants.
    #[test]
    fn test_extract_contract_id_all_variants() {
        use freenet_stdlib::prelude::UpdateData;
        use tokio::sync::mpsc;

        let code = ContractCode::from(vec![99u8; 32]);
        let params = Parameters::from(vec![88u8; 8]);
        let key = ContractKey::from_params_and_code(&params, &code);
        let contract_id = *key.id();

        // Events that SHOULD map to a contract queue (Some)
        let contract_events: Vec<(&str, ContractHandlerEvent)> = vec![
            (
                "PutQuery",
                ContractHandlerEvent::PutQuery {
                    key,
                    state: WrappedState::new(vec![1]),
                    related_contracts: RelatedContracts::default(),
                    contract: None,
                },
            ),
            (
                "UpdateQuery",
                ContractHandlerEvent::UpdateQuery {
                    key,
                    data: UpdateData::Delta(freenet_stdlib::prelude::StateDelta::from(vec![2])),
                    related_contracts: RelatedContracts::default(),
                },
            ),
            (
                "GetQuery",
                ContractHandlerEvent::GetQuery {
                    instance_id: contract_id,
                    return_contract_code: false,
                },
            ),
            (
                "GetSummaryQuery",
                ContractHandlerEvent::GetSummaryQuery { key },
            ),
            (
                "GetDeltaQuery",
                ContractHandlerEvent::GetDeltaQuery {
                    key,
                    their_summary: freenet_stdlib::prelude::StateSummary::from(vec![3]),
                },
            ),
            (
                "RegisterSubscriberListener",
                ContractHandlerEvent::RegisterSubscriberListener {
                    key: contract_id,
                    client_id: crate::client_events::ClientId::next(),
                    summary: None,
                    subscriber_listener: mpsc::channel(64).0,
                },
            ),
            // EvictContract must route to the contract's per-key queue so
            // disk reclamation is serialized with other ops on that key.
            (
                "EvictContract",
                ContractHandlerEvent::EvictContract {
                    key,
                    expected_generation: 0,
                },
            ),
        ];

        for (name, event) in &contract_events {
            let result = extract_contract_id(event);
            assert_eq!(
                result,
                Some(contract_id),
                "{name} should route to contract queue"
            );
        }

        // Events that SHOULD map to the default queue (None)
        let default_events: Vec<(&str, ContractHandlerEvent)> = vec![
            (
                "DelegateRequest",
                ContractHandlerEvent::DelegateRequest {
                    req: freenet_stdlib::client_api::DelegateRequest::ApplicationMessages {
                        key: freenet_stdlib::prelude::DelegateKey::new(
                            [1u8; 32],
                            freenet_stdlib::prelude::CodeHash::new([0u8; 32]),
                        ),
                        params: Parameters::from(vec![]),
                        inbound: vec![],
                    },
                    origin_contract: None,
                    user_context: None,
                },
            ),
            (
                "DelegateResponse",
                ContractHandlerEvent::DelegateResponse(vec![]),
            ),
            (
                "ClientDisconnect",
                ContractHandlerEvent::ClientDisconnect {
                    client_id: crate::client_events::ClientId::next(),
                },
            ),
            (
                "PutResponse",
                ContractHandlerEvent::PutResponse {
                    new_value: Ok(WrappedState::new(vec![])),
                    state_changed: false,
                },
            ),
            (
                "GetResponse",
                ContractHandlerEvent::GetResponse {
                    key: None,
                    response: Ok(crate::contract::handler::StoreResponse {
                        state: None,
                        contract: None,
                    }),
                },
            ),
            (
                "UpdateResponse",
                ContractHandlerEvent::UpdateResponse {
                    new_value: Ok(WrappedState::new(vec![])),
                    state_changed: false,
                },
            ),
            (
                "UpdateNoChange",
                ContractHandlerEvent::UpdateNoChange { key },
            ),
            (
                "RegisterSubscriberListenerResponse",
                ContractHandlerEvent::RegisterSubscriberListenerResponse { result: Ok(()) },
            ),
            (
                "QuerySubscriptionsResponse",
                ContractHandlerEvent::QuerySubscriptionsResponse,
            ),
            (
                "GetSummaryResponse",
                ContractHandlerEvent::GetSummaryResponse {
                    key,
                    summary: Ok(freenet_stdlib::prelude::StateSummary::from(vec![])),
                },
            ),
            (
                "GetDeltaResponse",
                ContractHandlerEvent::GetDeltaResponse {
                    key,
                    delta: Ok(freenet_stdlib::prelude::StateDelta::from(vec![])),
                },
            ),
            (
                "QuerySubscriptions",
                ContractHandlerEvent::QuerySubscriptions {
                    callback: mpsc::channel::<crate::message::QueryResult>(1).0,
                },
            ),
        ];

        for (name, event) in &default_events {
            let result = extract_contract_id(event);
            assert_eq!(result, None, "{name} should route to default queue");
        }
    }

    // ── Priority tiering (#4534) ──────────────────────────────────────────

    #[test]
    fn pop_drains_higher_priority_first() {
        let mut queue = FairEventQueue::new();
        let bg = make_contract_id_u32(1);
        let relay = make_contract_id_u32(2);
        let client = make_contract_id_u32(3);

        // Interleave the push order so ordering can't come from insertion order.
        queue
            .try_push(make_event_id(0), make_get_event(bg), Priority::Background)
            .unwrap();
        queue
            .try_push(
                make_event_id(1),
                make_get_event(relay),
                Priority::NetworkRelay,
            )
            .unwrap();
        queue
            .try_push(
                make_event_id(2),
                make_get_event(client),
                Priority::ClientLocal,
            )
            .unwrap();

        let order: Vec<ContractInstanceId> = std::iter::from_fn(|| queue.pop())
            .map(|(_, ev)| get_cid(ev))
            .collect();
        assert_eq!(
            order,
            vec![client, relay, bg],
            "must drain ClientLocal, then NetworkRelay, then Background"
        );
    }

    #[test]
    fn client_local_admitted_when_queue_full_of_background() {
        let mut queue = FairEventQueue::new();
        // Background, like any sub-ClientLocal class, is admission-capped at the
        // soft cap (it must leave the reserve free). So the most Background the
        // queue can hold is `soft_cap`.
        let soft_cap = MAX_TOTAL_FAIR_QUEUE - CLIENT_LOCAL_RESERVE;
        fill_to(&mut queue, Priority::Background, soft_cap);
        assert_eq!(queue.total_queued(), soft_cap);

        // ClientLocal is admitted straight into the reserve even with the queue
        // full of background — no eviction needed below the hard cap. Spread
        // across several contracts to stay under the per-contract-per-tier cap.
        for i in 0..CLIENT_LOCAL_RESERVE as u64 {
            let client = make_contract_id_u32(7000 + (i / MAX_QUEUED_PER_CONTRACT as u64) as u32);
            queue
                .try_push(
                    make_event_id(100_000 + i),
                    make_get_event(client),
                    Priority::ClientLocal,
                )
                .expect("ClientLocal must use the reserved lane past the soft cap");
        }
        assert_eq!(queue.total_queued(), MAX_TOTAL_FAIR_QUEUE);

        // Now the reserve is exhausted: a further ClientLocal needs room, which
        // eviction of Background provides (the loop's helper does evict→retry).
        let evicted = queue.evict_background(CLIENT_LOCAL_RESERVE);
        assert_eq!(evicted.len(), CLIENT_LOCAL_RESERVE);
        assert!(evicted.iter().all(|e| e.priority == Priority::Background));
        queue
            .try_push(
                make_event_id(999_999),
                make_get_event(make_contract_id_u32(7777)),
                Priority::ClientLocal,
            )
            .expect("ClientLocal admitted after Background eviction frees space");
    }

    #[test]
    fn evict_background_only_touches_background_tier() {
        let mut queue = FairEventQueue::new();
        let relay = make_contract_id_u32(2);
        let client = make_contract_id_u32(3);
        queue
            .try_push(
                make_event_id(0),
                make_get_event(relay),
                Priority::NetworkRelay,
            )
            .unwrap();
        queue
            .try_push(
                make_event_id(1),
                make_get_event(client),
                Priority::ClientLocal,
            )
            .unwrap();
        // 5 background events.
        for i in 0..5u64 {
            queue
                .try_push(
                    make_event_id(10 + i),
                    make_get_event(make_contract_id_u32(100 + i as u32)),
                    Priority::Background,
                )
                .unwrap();
        }
        assert_eq!(queue.background_queued(), 5);

        // Evicting more than present drains only background; foreground untouched.
        let evicted = queue.evict_background(100);
        assert_eq!(evicted.len(), 5);
        assert_eq!(queue.background_queued(), 0);
        assert_eq!(
            queue.tier_queued(Priority::NetworkRelay),
            1,
            "relay event must survive background eviction"
        );
        assert_eq!(
            queue.tier_queued(Priority::ClientLocal),
            1,
            "client event must survive background eviction"
        );
        assert_eq!(queue.total_queued(), 2);
    }

    #[test]
    fn per_contract_cap_is_per_tier() {
        // A contract saturated with Background must not block a ClientLocal op
        // on the SAME contract — the per-contract cap is scoped to the tier.
        let mut queue = FairEventQueue::new();
        let key = make_contract_id_u32(42);
        for i in 0..MAX_QUEUED_PER_CONTRACT as u64 {
            queue
                .try_push(make_event_id(i), make_get_event(key), Priority::Background)
                .expect("background fills its own per-contract slot");
        }
        // Same contract, ClientLocal tier — still has its own fresh slot.
        queue
            .try_push(
                make_event_id(9999),
                make_get_event(key),
                Priority::ClientLocal,
            )
            .expect("ClientLocal on a Background-saturated contract must still be admitted");

        // And ClientLocal drains first despite being pushed last.
        let (_, ev) = queue.pop().unwrap();
        assert_eq!(get_cid(ev), key);
        assert_eq!(queue.tier_queued(Priority::ClientLocal), 0);
        assert_eq!(
            queue.tier_queued(Priority::Background),
            MAX_QUEUED_PER_CONTRACT
        );
    }

    #[test]
    fn within_tier_round_robin_preserved() {
        // Two contracts in the SAME tier interleave fairly (the original
        // guarantee, now scoped per tier).
        let mut queue = FairEventQueue::new();
        let a = make_contract_id_u32(1);
        let b = make_contract_id_u32(2);
        for i in 0..4u64 {
            queue
                .try_push(make_event_id(i), make_get_event(a), Priority::NetworkRelay)
                .unwrap();
        }
        for i in 4..8u64 {
            queue
                .try_push(make_event_id(i), make_get_event(b), Priority::NetworkRelay)
                .unwrap();
        }
        let order: Vec<ContractInstanceId> = std::iter::from_fn(|| queue.pop())
            .map(|(_, ev)| get_cid(ev))
            .collect();
        assert_eq!(order, vec![a, b, a, b, a, b, a, b]);
    }

    #[test]
    fn relay_not_starved_by_sustained_client_load() {
        // With a permanent backlog of both ClientLocal and NetworkRelay work,
        // the anti-starvation floor must serve a NetworkRelay event at least
        // once every (RELAY_STARVATION_FLOOR + 1) pops (#4534 review).
        let mut queue = FairEventQueue::new();
        let client = make_contract_id_u32(1);
        let relay = make_contract_id_u32(2);

        // Saturate both tiers (spread across contracts to dodge per-contract cap).
        for i in 0..1000u64 {
            queue
                .try_push(
                    make_event_id(i),
                    make_get_event(make_contract_id_u32(1000 + (i / 50) as u32)),
                    Priority::ClientLocal,
                )
                .unwrap();
        }
        for i in 0..1000u64 {
            queue
                .try_push(
                    make_event_id(10_000 + i),
                    make_get_event(make_contract_id_u32(2000 + (i / 50) as u32)),
                    Priority::NetworkRelay,
                )
                .unwrap();
        }
        let _ = (client, relay);

        // Pop a long run and assert NetworkRelay is served on a bounded cadence:
        // no window of (FLOOR + 1) consecutive pops is all-ClientLocal.
        let mut gap = 0usize; // pops since last NetworkRelay
        let mut max_gap = 0usize;
        let mut relay_served = 0usize;
        for _ in 0..400 {
            let (_, ev) = queue.pop().unwrap();
            let cid = get_cid(ev);
            // relay contracts are the 2000.. series
            let is_relay = (2000..3000).any(|s| make_contract_id_u32(s) == cid);
            if is_relay {
                relay_served += 1;
                max_gap = max_gap.max(gap);
                gap = 0;
            } else {
                gap += 1;
            }
        }
        max_gap = max_gap.max(gap);
        assert!(relay_served > 0, "NetworkRelay must not be fully starved");
        assert!(
            max_gap <= RELAY_STARVATION_FLOOR,
            "gap between relay pops ({max_gap}) must not exceed the floor ({RELAY_STARVATION_FLOOR})"
        );
        // Client work still dominates: vastly more client than relay served.
        assert!(
            relay_served < 400 / 2,
            "client work should still dominate scheduling"
        );
    }

    #[test]
    fn uncontested_client_burst_does_not_trip_floor() {
        // With NO lower-tier work waiting, a long ClientLocal burst is served
        // back-to-back — the floor only applies when lower work is starved.
        let mut queue = FairEventQueue::new();
        for i in 0..200u64 {
            queue
                .try_push(
                    make_event_id(i),
                    make_get_event(make_contract_id_u32(1000 + (i / 50) as u32)),
                    Priority::ClientLocal,
                )
                .unwrap();
        }
        // All 200 pops are ClientLocal (no relay/background to divert to).
        for _ in 0..200 {
            assert!(queue.pop().is_some());
        }
        assert!(queue.is_empty());
    }

    #[test]
    fn per_contract_rejection_keeps_reason() {
        // A per-contract-cap rejection is flagged PerContract (so the loop skips
        // the futile Background eviction); a global-cap rejection is
        // GlobalCapacity.
        let mut queue = FairEventQueue::new();
        let key = make_contract_id_u32(1);
        for i in 0..MAX_QUEUED_PER_CONTRACT as u64 {
            queue
                .try_push(
                    make_event_id(i),
                    make_get_event(key),
                    Priority::NetworkRelay,
                )
                .unwrap();
        }
        let rejected = queue
            .try_push(
                make_event_id(999),
                make_get_event(key),
                Priority::NetworkRelay,
            )
            .expect_err("should reject at per-contract cap");
        assert_eq!(rejected.reason, RejectReason::PerContract);

        // Fill the rest of the queue to the soft cap with other contracts, then
        // a NetworkRelay push is refused for GlobalCapacity.
        let soft_cap = MAX_TOTAL_FAIR_QUEUE - CLIENT_LOCAL_RESERVE;
        fill_to(&mut queue, Priority::NetworkRelay, soft_cap);
        let rejected = queue
            .try_push(
                make_event_id(1000),
                make_get_event(make_contract_id_u32(55555)),
                Priority::NetworkRelay,
            )
            .expect_err("should reject at soft cap");
        assert_eq!(rejected.reason, RejectReason::GlobalCapacity);
    }

    #[test]
    fn evict_then_retry_can_still_fail_per_contract() {
        // Models the `still_rejected` arm of `push_with_background_eviction`:
        // even after Background is evicted (freeing GLOBAL capacity), a
        // ClientLocal push to a key whose ClientLocal tier is already at the
        // per-contract cap still fails — and is flagged PerContract, NOT
        // GlobalCapacity, so the loop does not loop on a futile re-evict.
        let mut queue = FairEventQueue::new();
        let hot = make_contract_id_u32(1);

        // Saturate the hot key's ClientLocal per-contract queue.
        for i in 0..MAX_QUEUED_PER_CONTRACT as u64 {
            queue
                .try_push(make_event_id(i), make_get_event(hot), Priority::ClientLocal)
                .unwrap();
        }
        // Add some evictable Background on other contracts.
        for i in 0..10u64 {
            queue
                .try_push(
                    make_event_id(1000 + i),
                    make_get_event(make_contract_id_u32(2000 + i as u32)),
                    Priority::Background,
                )
                .unwrap();
        }

        // Evicting Background frees global slots...
        let before = queue.total_queued();
        let evicted = queue.evict_background(CLIENT_LOCAL_RESERVE);
        assert_eq!(evicted.len(), 10);
        assert_eq!(queue.total_queued(), before - 10);

        // ...but a retry on the hot key still fails on the per-contract cap.
        let rejected = queue
            .try_push(
                make_event_id(9999),
                make_get_event(hot),
                Priority::ClientLocal,
            )
            .expect_err("hot key is at its ClientLocal per-contract cap");
        assert_eq!(
            rejected.reason,
            RejectReason::PerContract,
            "post-eviction retry failure must be PerContract, not GlobalCapacity"
        );
    }
}

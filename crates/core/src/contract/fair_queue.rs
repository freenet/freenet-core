//! Per-contract fair queuing for the WASM executor event loop.
//!
//! Provides round-robin scheduling across contracts to prevent a single contract
//! from monopolizing the sequential event loop. Events for each contract are
//! bucketed into per-contract queues, and the scheduler rotates through contracts
//! in round-robin order, processing one event per contract per turn.
//!
//! # Design
//!
//! The fair queue maintains:
//! - A `HashMap` of per-contract event queues (keyed by `ContractInstanceId`)
//! - A default queue for events with no contract identity (delegates, disconnects)
//! - A `VecDeque<QueueKey>` tracking the round-robin order
//! - A total event count for global backpressure
//!
//! Events are rejected with a `RejectedEvent` if either:
//! - The per-contract queue has reached `MAX_QUEUED_PER_CONTRACT`
//! - The total queue across all contracts has reached `MAX_TOTAL_FAIR_QUEUE`
//!
//! The caller is responsible for sending an appropriate error response for
//! rejected events (see `send_queue_full_response` in `contract.rs`).

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

/// Maximum events to drain from channel per iteration.
/// Prevents the drain loop from blocking delegate notifications too long.
pub(super) const MAX_DRAIN_BATCH: usize = 256;

/// Key identifying which queue an event belongs to.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum QueueKey {
    /// Event associated with a specific contract.
    Contract(ContractInstanceId),
    /// Event with no contract identity (delegate requests, client disconnects, etc.).
    Default,
}

/// An event that was rejected because its queue was full.
pub(super) struct RejectedEvent {
    pub id: EventId,
    pub event: ContractHandlerEvent,
}

impl std::fmt::Debug for RejectedEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RejectedEvent")
            .field("id", &self.id.id)
            .field("event", &self.event)
            .finish()
    }
}

/// Per-contract fair event queue providing round-robin scheduling.
///
/// Prevents a single contract from monopolizing the sequential executor event
/// loop by bucketing events by contract and rotating through them in order.
pub(super) struct FairEventQueue {
    /// Per-contract event queues.
    contract_queues: HashMap<ContractInstanceId, VecDeque<(EventId, ContractHandlerEvent)>>,
    /// Queue for events with no contract identity.
    default_queue: VecDeque<(EventId, ContractHandlerEvent)>,
    /// Round-robin ordering of queue keys.
    round_robin: VecDeque<QueueKey>,
    /// Total number of queued events across all queues.
    total_queued: usize,
}

impl FairEventQueue {
    /// Create a new, empty fair event queue.
    pub(super) fn new() -> Self {
        Self {
            contract_queues: HashMap::new(),
            default_queue: VecDeque::new(),
            round_robin: VecDeque::new(),
            total_queued: 0,
        }
    }

    /// Attempt to enqueue an event.
    ///
    /// Extracts the contract identity from the event and places it in the
    /// appropriate per-contract queue. If the event has no contract identity
    /// (delegates, disconnects, etc.), it is placed in the default queue.
    ///
    /// Returns `Err(Box<RejectedEvent>)` if:
    /// - The per-contract queue has reached `MAX_QUEUED_PER_CONTRACT`, or
    /// - The total queue has reached `MAX_TOTAL_FAIR_QUEUE`.
    pub(super) fn try_push(
        &mut self,
        id: EventId,
        event: ContractHandlerEvent,
    ) -> Result<(), Box<RejectedEvent>> {
        // Check global limit first (avoids per-contract map lookup)
        if self.total_queued >= MAX_TOTAL_FAIR_QUEUE {
            return Err(Box::new(RejectedEvent { id, event }));
        }

        match extract_contract_id(&event) {
            Some(contract_id) => {
                let queue = self.contract_queues.entry(contract_id).or_default();

                // Check per-contract limit
                if queue.len() >= MAX_QUEUED_PER_CONTRACT {
                    return Err(Box::new(RejectedEvent { id, event }));
                }

                // If this is a new contract, add it to the round-robin
                if queue.is_empty() {
                    self.round_robin.push_back(QueueKey::Contract(contract_id));
                }

                queue.push_back((id, event));
                self.total_queued += 1;
            }
            None => {
                // Default queue also respects the per-slot limit
                if self.default_queue.len() >= MAX_QUEUED_PER_CONTRACT {
                    return Err(Box::new(RejectedEvent { id, event }));
                }

                // Add the default key to round-robin only if queue is currently empty
                if self.default_queue.is_empty() {
                    self.round_robin.push_back(QueueKey::Default);
                }

                self.default_queue.push_back((id, event));
                self.total_queued += 1;
            }
        }

        Ok(())
    }

    /// Pop one event in round-robin order.
    ///
    /// Takes the front key from the round-robin, pops one event from that queue.
    /// If the queue still has items, the key is moved to the back (round-robin).
    /// If the queue is now empty, the key is removed.
    ///
    /// Returns `None` if all queues are empty.
    pub(super) fn pop(&mut self) -> Option<(EventId, ContractHandlerEvent)> {
        loop {
            let key = self.round_robin.pop_front()?;

            match key {
                QueueKey::Contract(contract_id) => {
                    if let Some(queue) = self.contract_queues.get_mut(&contract_id) {
                        if let Some(item) = queue.pop_front() {
                            debug_assert!(self.total_queued > 0);
                            self.total_queued = self.total_queued.saturating_sub(1);

                            if queue.is_empty() {
                                self.contract_queues.remove(&contract_id);
                            } else {
                                self.round_robin.push_back(key);
                            }

                            return Some(item);
                        }
                        // Queue existed but was empty — invariant violated, clean up
                        self.contract_queues.remove(&contract_id);
                    }
                    // Contract not in map or empty — skip, try next key in round_robin
                }
                QueueKey::Default => {
                    if let Some(item) = self.default_queue.pop_front() {
                        debug_assert!(self.total_queued > 0);
                        self.total_queued = self.total_queued.saturating_sub(1);

                        if !self.default_queue.is_empty() {
                            self.round_robin.push_back(QueueKey::Default);
                        }

                        return Some(item);
                    }
                    // Default queue was somehow empty — skip and try next
                }
            }
        }
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
        | ContractHandlerEvent::GetDeltaQuery { key, .. } => Some(*key.id()),
        ContractHandlerEvent::GetQuery { instance_id, .. }
        | ContractHandlerEvent::RegisterSubscriberListener {
            key: instance_id, ..
        }
        | ContractHandlerEvent::NotifySubscriptionError {
            key: instance_id, ..
        } => Some(*instance_id),
        // These events have no contract identity and are routed to the default queue.
        ContractHandlerEvent::DelegateRequest { .. }
        | ContractHandlerEvent::DelegateResponse(_)
        | ContractHandlerEvent::PutResponse { .. }
        | ContractHandlerEvent::GetResponse { .. }
        | ContractHandlerEvent::UpdateResponse { .. }
        | ContractHandlerEvent::UpdateNoChange { .. }
        | ContractHandlerEvent::RegisterSubscriberListenerResponse
        | ContractHandlerEvent::QuerySubscriptions { .. }
        | ContractHandlerEvent::QuerySubscriptionsResponse
        | ContractHandlerEvent::GetSummaryResponse { .. }
        | ContractHandlerEvent::GetDeltaResponse { .. }
        | ContractHandlerEvent::NotifySubscriptionErrorResponse
        | ContractHandlerEvent::ClientDisconnect { .. } => None,
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
        let code = ContractCode::from(vec![seed; 32]);
        let params = Parameters::from(vec![seed; 8]);
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

    fn make_delegate_event() -> ContractHandlerEvent {
        ContractHandlerEvent::ClientDisconnect {
            client_id: crate::client_events::ClientId::next(),
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
                .try_push(make_event_id(i), make_get_event(a))
                .expect("push should succeed");
        }
        for i in 10..20u64 {
            queue
                .try_push(make_event_id(i), make_get_event(b))
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
                .try_push(make_event_id(i), make_get_event(contract_id))
                .expect("should succeed within limit");
        }

        // One more should be rejected
        let result = queue.try_push(
            make_event_id(MAX_QUEUED_PER_CONTRACT as u64),
            make_get_event(contract_id),
        );
        assert!(
            result.is_err(),
            "should reject when per-contract limit exceeded"
        );
    }

    #[test]
    fn test_global_limit() {
        let mut queue = FairEventQueue::new();

        // Push events across many contracts until global limit
        let mut pushed = 0usize;
        let mut contract_seed = 0u8;
        let mut event_id = 0u64;

        while pushed < MAX_TOTAL_FAIR_QUEUE {
            let remaining = MAX_TOTAL_FAIR_QUEUE - pushed;
            let batch = remaining.min(MAX_QUEUED_PER_CONTRACT);
            let contract_id = make_contract_id(contract_seed);

            for _ in 0..batch {
                queue
                    .try_push(make_event_id(event_id), make_get_event(contract_id))
                    .expect("should succeed within limits");
                event_id += 1;
                pushed += 1;
            }

            contract_seed = contract_seed.wrapping_add(1);
        }

        assert_eq!(queue.total_queued(), MAX_TOTAL_FAIR_QUEUE);

        // Next push should be rejected
        let contract_id = make_contract_id(contract_seed.wrapping_add(1));
        let result = queue.try_push(make_event_id(event_id), make_get_event(contract_id));
        assert!(result.is_err(), "should reject when global limit exceeded");
    }

    #[test]
    fn test_default_queue_participates() {
        let mut queue = FairEventQueue::new();
        let contract_id = make_contract_id(1);

        // Push one delegate (default) event and one contract event
        queue
            .try_push(make_event_id(0), make_delegate_event())
            .expect("push should succeed");
        queue
            .try_push(make_event_id(1), make_get_event(contract_id))
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
            .try_push(make_event_id(0), make_get_event(contract_id))
            .expect("push should succeed");
        queue
            .try_push(make_event_id(1), make_get_event(contract_id))
            .expect("push should succeed");

        // Pop both events
        queue.pop().expect("first pop should succeed");
        queue.pop().expect("second pop should succeed");

        // Queue should be empty
        assert!(queue.is_empty());
        assert!(
            queue.contract_queues.is_empty(),
            "contract queue map should be empty"
        );
        assert!(queue.round_robin.is_empty(), "round_robin should be empty");
    }

    #[test]
    fn test_single_contract_preserves_order() {
        let mut queue = FairEventQueue::new();
        let contract_id = make_contract_id(1);

        // Push events with distinct IDs to track ordering
        for i in 0..5u64 {
            queue
                .try_push(make_event_id(i), make_get_event(contract_id))
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
        let num_contracts = 50usize; // Stay within limits: 50 * 100 = 5000
        let events_per_contract = MAX_QUEUED_PER_CONTRACT;

        // Push exactly events_per_contract events for each of num_contracts contracts
        let mut event_id = 0u64;
        for seed in 0..num_contracts as u8 {
            let contract_id = make_contract_id(seed);
            for _ in 0..events_per_contract {
                queue
                    .try_push(make_event_id(event_id), make_get_event(contract_id))
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
                .try_push(make_event_id(i), make_get_event(hot_contract))
                .expect("push should succeed");
        }

        // Push 1 event each for cold contracts
        let mut event_id = MAX_QUEUED_PER_CONTRACT as u64;
        for seed in 1..=num_cold as u8 {
            let contract_id = make_contract_id(seed);
            queue
                .try_push(make_event_id(event_id), make_get_event(contract_id))
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
            .try_push(make_event_id(0), make_get_event(contract_id))
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
            .try_push(make_event_id(0), event)
            .expect("push should succeed");

        assert_eq!(queue.total_queued(), 1);
        assert!(queue.contract_queues.contains_key(&contract_id));
    }

    #[test]
    fn test_default_queue_per_slot_limit() {
        let mut queue = FairEventQueue::new();

        // Fill default queue to per-slot limit with ClientDisconnect events
        for i in 0..MAX_QUEUED_PER_CONTRACT as u64 {
            queue
                .try_push(make_event_id(i), make_delegate_event())
                .expect("should succeed within limit");
        }

        // One more should be rejected
        let result = queue.try_push(
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
        queue.try_push(make_event_id(0), make_get_event(a)).unwrap();
        let (id0, _) = queue.pop().expect("should pop A");
        assert_eq!(id0.id, 0);

        // Push events for B and A
        queue.try_push(make_event_id(1), make_get_event(b)).unwrap();
        queue.try_push(make_event_id(2), make_get_event(a)).unwrap();

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
            queue.try_push(make_event_id(i), make_get_event(a)).unwrap();
        }
        for i in 3..5u64 {
            queue.try_push(make_event_id(i), make_get_event(b)).unwrap();
        }
        queue.try_push(make_event_id(5), make_get_event(c)).unwrap();

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
            (
                "NotifySubscriptionError",
                ContractHandlerEvent::NotifySubscriptionError {
                    key: contract_id,
                    reason: "test".to_string(),
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
                ContractHandlerEvent::RegisterSubscriberListenerResponse,
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
                "NotifySubscriptionErrorResponse",
                ContractHandlerEvent::NotifySubscriptionErrorResponse,
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
}

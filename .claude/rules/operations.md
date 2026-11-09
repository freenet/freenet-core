---
paths:
  - "crates/core/src/operations/**"
  - "crates/core/src/node/op_state_manager.rs"
---

# Operations Module Rules

> **Originator side effects after a task-per-tx migration.** A driver that
> replaces a legacy terminal-reply handler must run the FULL side-effect
> sequence, not a hand-inlined subset — the omitted call is silent (the op
> reports success and every test stays green). #3851 and #4223 are the
> precedents, the second costing ~37% of GETs through subscriber peers for
> months. The pattern, the fix shape (`finalize_*` helper plus a
> mutation-verified source pin) and the audit grep are in
> `.claude/rules/bug-prevention-patterns.md` — read that row before migrating
> an op.

## Execution Model

Every operation runs as a task-per-transaction driver: each `Transaction`
is owned and driven by a single spawned task; state lives in task
locals. Drivers publish their own `HostResult` via `result_router_tx`
and own routing/retry state in task locals. `OpManager` carries no
per-op state DashMaps; the only `Transaction`-keyed registries are the
RAII-guarded handoffs between a client retry loop and its own
originator-loopback relay (`stream_progress_registry`, `attempt_hop_registry`),
whose entries are removed by the loop's guard on every exit.

Driver entry points:

| Op | Entry points |
|----|--------------|
| CONNECT | `connect/op_ctx_task.rs::start_client_connect`, `start_relay_connect` |
| GET | `get/op_ctx_task.rs::start_client_get`, `start_relay_get`, `start_sub_op_get`, `start_targeted_sub_op_get` |
| PUT | `put/op_ctx_task.rs::start_client_put`, `start_relay_put`, `start_relay_put_streaming` |
| UPDATE | `update/op_ctx_task.rs::start_client_update`, `start_relay_request_update`, `start_relay_broadcast_to`, `start_relay_request_update_streaming`, `start_relay_broadcast_to_streaming` |
| SUBSCRIBE | `subscribe/op_ctx_task.rs::start_client_subscribe`, `start_directed_subscribe` (SubscribeHint placement nudge, #4404 — the inbound-hint receive arm in `node.rs` now refuses it above the module-cache INTERESTED-occupancy ceiling via `migration_admission_decision`, #4534), `run_executor_subscribe`, `run_renewal_subscribe`, `start_relay_subscribe`; `subscribe.rs::handle_unsubscribe_inbound` for `Unsubscribe` |

The shared retry-loop driver lives in `op_ctx.rs` (`RetryDriver` trait
+ `drive_retry_loop`). UPDATE is fire-and-forget (no retry loop, no
upstream reply); GET/PUT/SUBSCRIBE share the retry driver. GET's
client and sub-op drivers enter the loop via
`get/op_ctx_task.rs::drive_get_with_assembly_retry`, which treats a
post-terminal stream-assembly failure as a retryable attempt failure
(#4345) — post-terminal side effects that can fail retryably belong in
such a wrapper, never inside the loop's Terminal arm (pinned by
`drive_retry_loop_terminal_arm_does_not_call_advance`).

## Route-outcome labelling (#5657)

The router's failure model is only as good as its labels. Before #5657 a
relay recorded a downstream `NotFound` as a success and originators recorded
only their final success, so production routers saw ~0.5 % failures.

```
WHEN adding or changing a site where a GET/PUT/SUBSCRIBE attempt resolves
(reply classified, timeout, send failure) on an originator or relay driver:
  → Label non-success outcomes through the op's
    `operations::route_attempt::RouteAttemptRecorder` (one per operation),
    never a hand-inlined RouteEvent. Timeout / send failure / disconnect of
    the attempted peer → Failure now; NotFound → Failure ONLY with existence
    proof in the same op (`contract_exists()`), which requires state for the
    requested contract, from a REMOTE contacted peer, that this node stored.
    A SUBSCRIBE reply carries no state, so SUBSCRIBE NotFounds are never
    trained. Without proof, `ambiguous_not_found_policy()` (default
    Untrained: dropped).
    A peer is labelled at most once per operation.
  → A timeout is blamed on the recorded hop only if the hop had at least
    half of the attempt (`route_attempt::hop_had_budget_share`), counted
    from when the loopback relay's dispatch returned (`touch_hop`): a hop
    reached near the deadline did not stall it, and an attempt whose
    dispatch had not returned by the deadline blames nobody.
  → A failed GET stream is labelled per `stream_failure_is_peer_caused`,
    used by the client (claim and assembly), the relay's claim arm and the
    test hook: every assembly failure (a cancellation does not record which
    side ended it) and a claim timeout blame the hop; a claim waiter dropped
    on this node does not. The relay's failures after a successful claim
    label nothing, as before #5657.
  → Never treat local evidence or later evidence as proof. This node's own
    (possibly stale) copy, a local completion, or a terminal with no recorded
    hop proves nothing about a remote peer. "The contract exists now" is not
    "it existed when the NotFound was returned": a GET before a PUT gets
    correct NotFounds from exactly the peers the PUT then stores at, so
    labelling them from later evidence would train on correct answers. Naive
    training on ambiguous NotFounds measurably degraded the model
    (exp/estimator-bakeoff e26a92c1d).
  → Blame or credit only the peer the request ACTUALLY went to. GET/PUT
    originators send to their own loopback relay, which picks the real hop;
    read it from the per-attempt `AttemptHopRegistry` slot (the loopback relay
    fills it). No recorded hop → no event, success or failure. The driver's
    `current_target` is a guess.
  → A local callback drop (`NotificationError`), a disconnect of some other
    peer, and an unexpected reply blame nobody — on originators AND relays.
  → SUBSCRIBE renewals: the originator labels neither its timeouts (its
    clamped renewal budget, not the peer) nor NotFounds (structural: the
    renewer hosts the contract and sits in the visited bloom). A RELAY
    forwarding a renewal still labels a timeout: it waited the full
    OPERATION_TTL for its hop.
  → Router labels are router-only (`Ring::record_route_failure`): a NotFound
    or an end-to-end timeout is not a reason to evict a connection.
  → Originator peer_health, topology and `NetEventLog::route_event` inputs are
    EXACTLY main's: the same event against `current_target`, at the same sites
    and conditions, hop-less successes included, in both label modes
    (`route_attempt::report_originator_route_outcome`). Only the router label
    uses the recorded hop. Never move a health input to the hop.
  → The GET stream claim uses the recorded hop, falling back to
    `current_target` only when none was recorded. This is delivery, not
    labelling, so it does not follow the kill switch: with retry diversity
    (#5660) the loopback relay's pick often differs from `current_target`, and
    the stream is registered under the hop it actually came from.
  → GET retry diversity (#5660): the peers that answered NotFound in this
    operation are excluded from the originator-loopback relay's own
    first-hop pick only, handed over through `AttemptHopRegistry`, and are
    never carried in the forwarded visited bloom. A relay also answers
    NotFound when its own downstream send fails or its connection drops, so a
    whole-path exclusion could cut a single-host contract off behind one
    flapping peer. If the exclusions leave no candidate, the relay ignores
    them. Once the driver's own guesses run out (rings of three peers or
    fewer), `advance` chooses each remaining attempt's first hop itself and
    pins it for the loopback relay (`fallback_target`), so the router's
    ranking cannot reorder the choice. Every connected peer that failed once
    without answering NotFound gets its second attempt before any
    never-asked peer is tried, earliest-asked first: so with two such peers
    and one attempt left the earlier-asked one wins, and with two such peers
    a never-asked peer, even one that connected late, can go unasked. The
    relay honours the pin only while the peer passes `first_hop_candidate`
    (connected and not transient; readiness is not enforced). A peer given
    `FALLBACK_MAX_ASKS_PER_PEER` attempts is not chosen again and is
    excluded from the relay's own pick, so after the guesses run out the
    fallback re-asks each peer at most once. A local infra retry (a
    callback dropped on this node, within `MAX_INFRA_RETRIES`) does not
    count as an attempt on the peer; one beyond that budget does.
  → Every router label carries its routing-dataset source (#5648): the
    recorder passes its `AttemptOrigin` to the sink, and relay-observed
    events (recorder relay labels, legacy relay `SuccessUntimed`,
    `record_relay_route_event`) must reach `Router::add_relay_event`, never
    `add_event`. `Ring::record_route_event_router_only` takes the source.
  → Chain blame is accepted for router-only labels: an originator timeout is
    labelled against the first hop although the stall may be further down
    the chain. Watch it with the `timeout_label_*` router-snapshot histogram
    (telemetry-only: the window is drained per snapshot, so no local
    dashboard reads it).
  → NOT YET MIGRATED: PUT relay's downstream forwarding still labels its own
    send/timeout failures with `record_relay_route_event` (router-only too),
    outside the recorder. Those failures are not in the per-cause counters.
  → Kill switch: `FREENET_ROUTING_LEGACY_LABELS=1` restores the pre-#5657
    labels exactly (`route_attempt::label_mode()`). Any label site outside
    the recorder must branch on `recorder.mode()`.
```

## Wire-variant dispatch

`node.rs::handle_pure_network_message_v1` is the single dispatch site
for every inbound wire message. Pattern per op:

1. **Reply bypass.** If a terminal reply variant arrives and a
   `pending_op_results` callback is registered, forward it via
   `try_forward_driver_reply` and return. For GET/PUT/SUBSCRIBE the
   gate is `Response | ResponseStreaming` only. PUT additionally
   accepts `PutMsg::Error` so the originator-loopback failure path
   (issue #4111) delivers the contract-side cause via the same bypass
   instead of timing out the retry loop on a closed reply channel.
   CONNECT forwards
   all four non-`Request` variants (multi-reply fan-in).
2. **Relay dispatch.** Spawn the matching `start_relay_*` driver.
   Originator-loopback (`source_addr=None`) is mapped to
   `upstream_addr=own_addr` for GET/PUT/SUBSCRIBE so the same driver
   handles both relay hops and originator loopback. UPDATE has no
   loopback (fire-and-forget end-to-end).
3. **No fallthrough.** Every wire variant either bypasses to a waiter,
   dispatches a driver, or hits a dedicated free-function handler (e.g.
   `handle_unsubscribe_inbound`, `ForwardingAck` no-op). Anything else
   is dropped with a debug log.

## Test fixtures

`ConnectOp` survives only as a `#[cfg(test)]` fixture wrapping
`RelayState::handle_request`. Production CONNECT runs entirely on the
drivers in `connect/op_ctx_task.rs`.

## Critical Invariant: Tag Contract-Handler Events with the Right Priority (#4534)

```
Every event sent to the single-threaded contract_handling loop carries a
`crate::contract::Priority` (ClientLocal > NetworkRelay > Background).
The fair queue drains higher classes first, reserves admission capacity
(CLIENT_LOCAL_RESERVE) for ClientLocal, and sheds Background to make room.
A mis-tag silently degrades service: tag a client path Background and it
gets starved/evicted; tag relay/background ClientLocal and it can exhaust
the reserve meant to protect real client requests.

Rules for choosing the class at a send site:

  ClientLocal  → the event serves a WS/HTTP client connected to THIS node.
                 Set it at the client-facing entry points:
                 - client_events.rs request handlers (GET, RegisterSubscriberListener,
                   DelegateRequest)
                 - drive_client_update → update_contract(.., ClientLocal)
                 - hosted_export.rs ExportUserSecrets
                 - a client's own PUT reaches the relay driver via
                   originator-loopback (upstream_addr == own_addr); compute it
                   with put_store_priority() — DO NOT hardcode NetworkRelay on
                   the relay PUT store path.

  NetworkRelay → serving a remote peer's relayed op (the default). All
                 drive_relay_* drivers, ResyncResponse apply, broadcast-path
                 summaries/deltas that propagate a real UPDATE. This is also
                 `Priority::DEFAULT`, so untagged callers fall here safely.

  Background   → best-effort node-internal work that must yield to client/relay
                 traffic: the periodic interest-sync summarize
                 (summary_if_hosted_or_in_use), placement-migration caching,
                 renewal, and EvictContract disk reclamation. NEVER tag a
                 client- or peer-visible op Background.

When a shared helper is reached from BOTH a client and a relay path (e.g.
put_contract, update_contract), thread a `priority` param through rather
than hardcoding — decide the class at the driver that knows the origin.

The loop's pop() applies a bounded anti-starvation floor
(RELAY_STARVATION_FLOOR) so sustained ClientLocal load cannot indefinitely
starve NetworkRelay/Background — but that is a backstop, not a license to
mis-tag.
```

**Client `DelegateRequest`s and the delegate lane (#6144).** Once
`contract_handling` has started a delegate lane (`contract/delegate_lane.rs`;
production always does), `ContractHandlerChannel::send_to_handler` hands a
`DelegateRequest` to the lane's bounded intake (128, `try_send`; full means
refused at once; a request that is not the node's own local client, i.e. a
non-local scope or a hosted user, may fill only 7/8 of it,
`LOCAL_INTAKE_RESERVE`; on a hosted node the reserve therefore protects only
the operator's own local requests, and hosted users share the other 7/8 among
themselves). The lane runs a request itself only when its
delegate is free and fewer than `MAX_PENDING_UNITS` (16) continuations wait
for the loop; every other request it FORWARDS, unchanged (same id, priority,
scope, user, responder), into this loop's event channel. The fair-queue
bounds above (capacity, per-slot cap, `CLIENT_LOCAL_RESERVE`) and, for a
parked delegate, the park's `queue_pending` cap and byte budget apply ONLY
to forwarded requests; a request the lane runs never enters the fair queue.
Keep tagging `DelegateRequest` ClientLocal at the send site: the tag rides
through the lane. A forwarded request carries a `LaneTicket` marking its
(delegate key, user) "loop-owned" until the event is dropped (after the
`DelegateRequest` arm finishes, before the client is answered, or when the
fair queue refuses it), so that user's later requests for the delegate
follow rather than overtake. That order is per client and fair-queue
priority slot (each slot is FIFO; client delegate requests are all
`ClientLocal`). If the lane task dies it hands its buffered requests to the
loop, later requests go to the loop directly, and the router snapshot counts
the death (`delegate_lane_deaths`) and those requests
(`delegate_lane_forwarded_lane_down`). A forged node-only message is forwarded
without a ticket. Units (continuations of lane runs) run at the
TOP of each loop iteration, ahead of the fair queue, at most
`MAX_LANE_UNITS_PER_ITERATION` (4) per iteration; every `claim_delegate` and
the inter-delegate hop may also run one pending unit for the delegate it
claims.

## Critical Invariant: Initialize-Before-Send

```
All task-local state the reply handler will read (retry counters,
visited-peers filter, routing state) MUST be initialized BEFORE
calling `OpCtx::send_and_await`. The reply may arrive on the very
next poll.
```

## Critical Invariant: Forward Upstream Before Contract-Handling Work on Relay Paths

On a relay driver's response path, any operation that enqueues on the
single-threaded `contract_handling` event loop (GetQuery, PutQuery,
validate_state, `RequestRelated` recursion, etc.) MUST be sequenced
AFTER the upstream forward. Reversing this puts WASM `validate_state`
directly on the upstream-visible critical path, where it stacks across
multi-hop traversals.

Note (#4391): the related-contract NETWORK fetch's 10s
`RELATED_FETCH_TIMEOUT` is no longer awaited inline on the
`contract_handling` loop — a PUT/UPDATE that needs a related contract
not held locally now off-loads that fetch to a background task and
re-enqueues a continuation (see `contract.rs::maybe_defer_upsert`), so
the loop no longer stalls for that 10s window. WASM
`validate_state`/`update_state`/store still run serially on the loop,
so this invariant still holds for the WASM work; only the related-fetch
WAIT no longer contributes to the stall.

```
RIGHT (issue #4155 fix):
  let send_result = relay_send_found(...).await;
  cache_contract_locally(...).await;   // cache opportunistically
  send_result?;                         // propagate forward error AFTER cache

WRONG (re-introduces #4155):
  cache_contract_locally(...).await;   // blocks the relay for seconds
  relay_send_found(...).await?;        // upstream waits for the cache
```

This applies symmetrically to GET, PUT, UPDATE, SUBSCRIBE relay
drivers. The source-scrape pin tests
`all_relay_callsites_forward_before_caching` /
`relay_driver_forwards_upstream_before_caching_on_found_response` in
`crates/core/src/operations/get/op_ctx_task.rs` cover GET. Equivalent
pin tests should be added if a relay driver in another op grows a
post-forward contract-handling step.

The originator (`drive_client_get_inner`, `drive_client_put_inner`,
etc.) and sub-op drivers must STILL cache before completing to the
client — they validate the state for the local client, and skipping
that would expose unvalidated bytes to the application layer.

## State Machine Rules

### WHEN implementing a new operation state

```
1. Define states as enum variants with clear names
2. Each state must have a clear transition trigger
3. Document what message causes each transition
4. Handle unexpected messages gracefully (log + ignore)
```

### WHEN transitioning state

```
CORRECT:
  let next_state = match (current_state, message) {
      (State::A, Msg::X) => State::B,
      (State::B, Msg::Y) => State::C,
      _ => {
          tracing::warn!("unexpected message in state");
          return Ok(());
      }
  };

WRONG:
  // Don't panic on unexpected messages
  panic!("invalid state transition");
```

### WHEN an operation completes

```
1. Call op_manager.completed(tx)
2. Do NOT call push() after completed()
3. If sub-operations exist, wait for all to complete
4. Send result to client via result router
```

## Sub-Operation Rules

### WHEN spawning a sub-operation

```
Sub-operations are identified structurally — create them via
Transaction::new_child_of::<MsgType>(&parent_tx). The parent field is
set at construction; Transaction::is_sub_operation() returns true
without any DashMap registration.

Either await the child inline (blocking) or fire-and-forget (async).
There is no central tracker; failure propagation is the child driver's
responsibility (publish HostResult::Err on its own task).

The is_sub_operation guards at p2p_protoc.rs, node.rs, and subscribe.rs
all use the structural check.
```

## Streaming Rules

### WHEN payload exceeds streaming_threshold (default 64KB)

```
1. Use RequestStreaming/ResponseStreaming message variants
2. Implement "piped streaming" - forward while receiving
3. Handle OrphanStreamClaimFailed error (stream metadata arrived first)
4. Clean up stream handles on error
```

### WHEN implementing streaming

```
MUST:
  - Check op_manager.should_use_streaming(payload_size)
  - Fork stream handle for parallel forwarding
  - Deserialize and store locally while piping
  - Handle stream cancellation gracefully

DON'T:
  - Buffer entire payload in memory (defeats streaming purpose)
  - Forget to claim orphan streams when fragments arrive first
```

## Transaction Rules

### WHEN creating transactions

```
For new operations: Transaction::new::<OpType::Message>()
For sub-operations: Transaction::new_child_of::<OpType::Message>(&parent)
```

### WHEN checking timeout

```
Use: tx.timed_out()
→ Uses simulation time (TimeSource), NOT wall clock
→ Ensures deterministic testing
```

## State Consistency

### WHEN a connection is removed

```
All failure paths that remove a connection MUST also clean up
pending operation state for that peer.

MUST:
  → Remove any in-flight operations targeting the disconnected peer
  → Fail pending sub-operations that depend on the peer
  → Log stale operation cleanup at debug level

WHY: Orphaned operations to dead peers cause timeouts and resource leaks.
```

### WHEN syncing peer lists across nodes

```
Sync protocols that exchange peer lists MUST filter out
peers not currently in the live connection set.

WRONG:
  send_peer_list(all_known_peers)  // Includes long-dead peers

CORRECT:
  send_peer_list(connected_peers.filter(|p| is_live_connection(p)))
```

### WHEN publishing a terminal operation reply

```
Sync-failure paths (where the driver knows the operation is done and
must publish a Result to the originator) MUST deliver via the SAME
mechanism the success path uses — NOT via direct send_client_result.

WRONG:
  if put_contract(...).is_err() {
      op_manager.send_client_result(tx, Err(host_err));  // M1
      op_manager.completed(tx);                          // M2
  }

  M1 and M2 are independent try_send calls on different channels
  (result_router_tx vs event-loop notification channel). When the
  event loop processes M2 first, it removes pending_op_results[tx]
  BEFORE send_and_await consumes M1; the retry-loop sees the closed
  channel as NotificationError, advances, burns the retry budget on
  the same deterministic failure, and the user gets the synthesised
  "failed notifying, channel closed" instead of the real reason.

CORRECT:
  let err_msg = NetMessage::from(<Op>Msg::Error { id: tx, cause });
  ctx.send_local_loopback(err_msg).await?;

  Routing: handle_pure_network_message_v1 forwards <Op>Msg::Error
  into pending_op_results[tx] like Response; classify_reply returns
  ReplyClass::TerminalError { cause }; the driver returns
  Terminal(Err(cause)) → Done(Err); run_client_<op> publishes ONE
  HostResult::Err and calls op_manager.completed(client_tx)
  synchronously — no race window.

Multi-hop: when a downstream relay returns <Op>Msg::Error, the
relay MUST propagate the cause one hop further upstream via a
matching `relay_<op>_send_error` helper, not fall through to a
generic OpError::UnexpectedOpState wildcard.

DoS amplification: cap the cause length at the wire boundary.
PUT uses PUT_TERMINAL_CAUSE_MAX_BYTES = 2048 + a UTF-8-safe
truncator (see `bound_cause` in operations/put.rs).

Testing: PUT's bypass + multi-hop bubble is covered by unit tests
in `operations/put/op_ctx_task.rs::tests` (search for
`relay_put_send_error_with_ctx_*`, `drive_relay_put_error_arm_*`,
`run_relay_put_bubbles_local_failure_*`, and
`dispatch_loopback_shutdown_fallback_*`) plus the
`tests/error_notification.rs::test_put_error_notification*` E2E
pair. The wrapper-contract E2E that asserts on a stable,
contract-side cause string is tracked as follow-up #4147 — the
existing E2E tests assert only the absence of the synthesised
`"failed notifying, channel closed"` marker.
```

## Error Handling

### WHEN a reply arrives with no waiter

```
Benign (duplicate message, already completed). Drop with a debug log
at the dispatch site. Do NOT treat as an error.
```

### WHEN encountering InvalidStateTransition

```
→ Log with backtrace (debug builds)
→ Return error to caller
→ Do NOT panic
→ Consider: Is the message from malicious peer?
```

## Testing Checklist

```
□ Test state machine transitions with all message types
□ Test timeout handling (advance simulation time)
□ Test sub-operation completion order (child before/after parent)
□ Test streaming with payloads at threshold boundary
□ Test race conditions (fast responses)
```

### WHEN awaiting a reply on `pending_op_results[tx]` (#4313)

When a connection is pruned, `handle_orphaned_transactions` wakes each
parked driver. The cause travels **through the waiter channel itself**:
the `TransactionOrphaned { tx, peer }` event-loop handler does
`sender.try_send(WaiterReply::PeerDisconnected { peer })` and *then*
drops the sender. tokio mpsc delivers a buffered item before the
channel reads `None`, so the driver deterministically observes
`PeerDisconnected` (mapped to `OpError::PeerDisconnected`) — never the
`"failed notifying, channel closed"` FORBIDDEN_MARKER that
`tests/error_notification.rs` asserts against. There is no side
registry and no record-before-release ordering to get wrong.

```
The waiter channel item is `WaiterReply` (Reply | PeerDisconnected),
  NOT a bare NetMessage. Every recv site MUST handle PeerDisconnected
  — the type system enforces this. Funnel through
  OpCtx::recv_waiter_reply (used by send_and_await, send_to_and_await,
  and the PUT/CONNECT relays) rather than matching the enum by hand.

The TransactionOrphaned handler MUST send PeerDisconnected on the
  sender it removes from pending_op_results BEFORE that sender drops
  (send-before-drop). Dropping without sending re-opens the #4313
  race (driver wakes on close with no cause → FORBIDDEN_MARKER).

PeerDisconnected routes to the generic advance arm in
  drive_retry_loop (the peer is gone — advance to the next route),
  NOT the NotificationError same-peer infra-retry arm. MUST preserve
  `{err}` interpolation in the Exhausted format so the cause Display
  reaches the user.
```

Pins (source-scrape, fail the build on regression):
- `transaction_orphaned_handler_sends_cause_before_dropping_sender` (p2p_protoc.rs)
- `handle_orphaned_transactions_wakes_parked_drivers` (p2p_protoc.rs)
- behavioural: `parked_driver_always_observes_peer_disconnected_under_churn`,
  `recv_waiter_reply_*` (op_ctx.rs) and
  `orphaned_transaction_wakes_parked_waiter_with_peer_disconnected` (op_state_manager.rs)

## UPDATE propagation counters and the stall WARN (#6143)

Every place an UPDATE broadcast leg can end without a send logs at debug or
trace, which release builds compile out. Each outcome is counted instead as a
`PropagationEvent` (`operations/update/propagation_stats.rs`), per contract
in the per-minute `update_propagation_summary` INFO line and node-wide as
`prop_<label>` fields of the `broadcast_payload_mix` telemetry event.

```
WHEN adding a way for a broadcast leg, fan-out or heal to end:
  → Count it through `record_propagation_event` (or `PropagationSink::record`
    where there is no OpManager). Never call `UpdatePropagationStats` or
    `PayloadMix` directly: the one function feeds both, so the log and the
    telemetry cannot disagree.
  → A new event needs a variant, a label (a stable log/telemetry field name)
    and a production recording site. The pin test
    `every_propagation_event_has_a_production_recording_site`
    fails on a variant nothing records.
  → A stream's start and fate go through `record_propagation_stream_start` /
    `record_propagation_stream_fate` (or the `PropagationSink` methods), which
    match each fate to the window the stream started in. Recording
    `WireStream*` as a plain event loses that, and the stall check then waits
    on the wrong window.
  → Per-event logging stays at debug. The count is the release-build signal;
    a per-stream WARN brings back the #4252/#4272 log volume.
```

The fields: `client_originated` (a client apply that changed
state); `sent_delta` / `sent_full` (handed to the bridge); the skips
`skip_summary_equal`, `skip_cached_converged`, `skip_probe_converged`,
`skip_empty_delta`, `skip_not_hosted`, `skip_no_address`; `send_failed`;
transport `wire_short` (one packet), `wire_stream` and its fate
`wire_stream_delivered` / `_failed` / `_egress_abandoned`, `wire_unobserved`
(QUIC path); `fanout_event_dropped` / `heal_event_dropped` (`try_notify`
refused); and the receiver's `rx_dedup`, `rx_already_applied`, `rx_backoff`.

`update_propagation_stalled` WARNs (counted as `origination_stalls` in
telemetry) for a window in which a contract had a client update with
resolved targets, or a dropped fan-out while no fan-out of it was processed,
and nothing for it that started in that window or later reached the wire
(`ContractSummary::reached_wire_since`: a short packet, a QUIC send, or a
stream of that window or later delivered) by the end of the next window.
Each window is its own candidate, keyed `(contract, window)`, so a new stall
is reported on time behind an older candidate that is still waiting. A
candidate with one of its own window's streams in flight waits for that
stream's fate (up to `MAX_STALL_CARRY_WINDOWS`). At most one WARN per contract
per window, and at most `MAX_STALL_WARNS_PER_WINDOW` per window plus a rollup.

A client update whose legs were all skipped as converged (`skip_summary_equal`
and the like) counts as a stall: the update changed this node's state, so no
peer can already hold it, and the skip means this node's belief about the
peer is stale. That is the leading #6143 hypothesis.

**It cannot see:**
- relayed updates: only client updates are candidates;
- loss of a single packet or of a QUIC message after the transport took it:
  those count as reaching the wire. Read the receivers' `rx_*` counts across
  the fleet;
- partial failure: one leg on the wire clears the window, so 13 of 14 legs
  failing raises nothing;
- a fan-out that resolves no targets (`no_targets`, which has its own
  `BROADCAST_NO_TARGETS` WARN);
- a client fan-out dropped beside a processed relayed fan-out that resolved
  no targets (the drop counter cannot tell the two apart).

A stream fate that is not attributed (the 4096-contract tracking map was full,
or more than `MAX_EARLIER_START_WINDOWS` start windows reported in one window)
leaves its candidate waiting until `MAX_STALL_CARRY_WINDOWS`, then WARNs, which
is a false WARN about 80 minutes late if the stream was delivered.

## Event emission review (targeted vs fan-out)

**When reviewing (or writing) ANY code that emits a `NodeEvent`, you MUST
explicitly verify two things and note them in the review:**

```
1. WHICH peers receive this event?
   → Targeted   → SyncStateToPeer { target }  (exactly one peer)
   → Fan-out    → BroadcastStateChange        (ALL subscribers of the key)
   Name the actual recipient set out loud. "It syncs the peer" is not a
   review — "it fans out to every subscriber of the contract" is.

2. Does the code COMMENT match the actual dispatch path?
   → A comment that describes the INTENDED effect ("sends state only to
     peers with stale summaries") while the code takes the fan-out path is
     a review failure, not a nit. Comments on event-emission sites MUST
     name the concrete dispatch behavior (targeted address / all
     subscribers), NOT the wished-for outcome.
```

**Why this is a mandatory checklist item.** #3791 was a six-week-old
regression where the summary-mismatch handler emitted
`BroadcastStateChange` (all-subscriber fan-out) instead of the intended
targeted `SyncStateToPeer`. A misleading comment made the wrong event look
correct on a quick read; it turned one mismatch into O(N × fan-out)
transmissions and produced 19:1–163:1 upload/download ratios in
production. The full targeted-vs-broadcast cost model is in
`docs/architecture/event-dispatch.md`.

**The trap that hid it, and how we guard against it now.** The original
regression test only covered `InterestManager` stale-peer *data* logic —
reverting the `node.rs` dispatch would have left that test green. A test
for an event-emission fix MUST exercise the actual dispatch DECISION
(which variant, which target), not just the data layer that feeds it.

```
WHEN adding or reviewing a test for an event-emission fix:
  → Does the test assert on the emitted NodeEvent variant and its
    recipient (target address vs all-subscriber fan-out)?
    → NO: it does not guard the dispatch. A data-layer-only test would
          stay green if someone reverted the emit site to a broadcast.
  → Prefer a pure builder function for the emit decision (see
    `node.rs::stale_peer_sync_event`) so the variant+target choice is
    unit-testable without a full OpManager, PLUS a source-scrape pin that
    the production loop still routes through it.
```

Pins (source-scrape / builder, fail the build on regression):
- `stale_peer_sync_event_is_targeted_not_broadcast` (node.rs) — the heal
  event is a targeted `SyncStateToPeer`, never a `BroadcastStateChange`.
- `emit_site_uses_targeted_builder` (node.rs) — the `Summaries`-arm emit
  site routes through `stale_peer_sync_event` and never names
  `BroadcastStateChange`.

## Documentation

- Architecture: `docs/architecture/operations/README.md`
- Event dispatch (targeted vs fan-out): `docs/architecture/event-dispatch.md`
- OpManager: `crates/core/src/node/op_state_manager.rs`
- Round-trip primitive: `crates/core/src/operations/op_ctx.rs`
- Orphan-wake signal: `crate::node::WaiterReply::PeerDisconnected` + `NodeEvent::TransactionOrphaned` (#4313)

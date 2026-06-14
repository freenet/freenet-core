---
paths:
  - "crates/core/src/operations/**"
  - "crates/core/src/node/op_state_manager.rs"
---

# Operations Module Rules

## Execution Model

Every operation runs as a task-per-transaction driver: each `Transaction`
is owned and driven by a single spawned task; state lives in task
locals. Drivers publish their own `HostResult` via `result_router_tx`
and own routing/retry state in task locals. `OpManager` carries no
per-op DashMaps.

Driver entry points:

| Op | Entry points |
|----|--------------|
| CONNECT | `connect/op_ctx_task.rs::start_client_connect`, `start_relay_connect` |
| GET | `get/op_ctx_task.rs::start_client_get`, `start_relay_get`, `start_sub_op_get`, `start_targeted_sub_op_get` |
| PUT | `put/op_ctx_task.rs::start_client_put`, `start_relay_put`, `start_relay_put_streaming` |
| UPDATE | `update/op_ctx_task.rs::start_client_update`, `start_relay_request_update`, `start_relay_broadcast_to`, `start_relay_request_update_streaming`, `start_relay_broadcast_to_streaming` |
| SUBSCRIBE | `subscribe/op_ctx_task.rs::start_client_subscribe`, `start_directed_subscribe` (SubscribeHint placement nudge, #4404), `run_executor_subscribe`, `run_renewal_subscribe`, `start_relay_subscribe`; `subscribe.rs::handle_unsubscribe_inbound` for `Unsubscribe` |

The shared retry-loop driver lives in `op_ctx.rs` (`RetryDriver` trait
+ `drive_retry_loop`). UPDATE is fire-and-forget (no retry loop, no
upstream reply); GET/PUT/SUBSCRIBE share the retry driver. GET's
client and sub-op drivers enter the loop via
`get/op_ctx_task.rs::drive_get_with_assembly_retry`, which treats a
post-terminal stream-assembly failure as a retryable attempt failure
(#4345) — post-terminal side effects that can fail retryably belong in
such a wrapper, never inside the loop's Terminal arm (pinned by
`drive_retry_loop_terminal_arm_does_not_call_advance`).

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
and the 10s `RELATED_FETCH_TIMEOUT` directly on the upstream-visible
critical path, where they stack across multi-hop traversals.

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

## Documentation

- Architecture: `docs/architecture/operations/README.md`
- OpManager: `crates/core/src/node/op_state_manager.rs`
- Round-trip primitive: `crates/core/src/operations/op_ctx.rs`
- Orphan-wake signal: `crate::node::WaiterReply::PeerDisconnected` + `NodeEvent::TransactionOrphaned` (#4313)

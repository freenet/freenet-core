---
description: Prevent bounded channel deadlocks — cascading backpressure from .send().await in event loops has caused 5+ production incidents
paths:
  - "crates/core/src/node/**"
  - "crates/core/src/transport/**"
  - "crates/core/src/client_events/**"
  - "crates/core/src/contract/**"
---

# Bounded Channel Safety

## The Pattern That Keeps Killing Us

`.send().await` on a bounded channel inside an event loop or recv loop.
When the receiver is slow or blocked on another channel, the sender blocks,
stalling the entire event loop. This has caused 6+ production deadlocks:

- **StreamRegistry** (Mar 2026): `mpsc::channel(64)` whose receiver was never
  consumed. After 64 streams, `.send().await` blocked forever, deadlocking
  `PeerConnection::recv()`, causing 100% GET failure rate for hours.
- **Broadcast fan-out** (#3309, #3285): Inline `.send().await` to peers from
  the event loop exceeded bridge channel capacity, deadlocking the node.
- **Result router chain**: `result_router_tx.send().await` in the event loop
  could cascade if `SessionActor` was slow → router blocked → event loop blocked.
- **Listener forwarding family** (#3959 → #3960 → #3961, Apr 2026):
  the UDP listener task in `transport::connection_handler` calls
  `try_send_*` synchronously to route every inbound packet to the right
  per-peer channel. The original `fast_channel` wrapper used
  `crossbeam::channel`, whose internal `Backoff::snooze` could spin in
  `sched_yield` indefinitely on a wedged slot — taking the entire node
  offline (#3959). #3960 replaced it with `tokio::sync::mpsc` so
  `try_send` is guaranteed non-blocking. #3961 closed the remaining
  `.send().await` site on the listener path (`send_nat_traversal`).
  The recv-loop forwarding from `PeerConnection::recv` into the
  per-stream `inbound_streams` channel was investigated and *kept* as
  `.send().await`: the receiver there is the freenet-spawned
  `recv_stream` reassembly task, internal and same-runtime, so the
  cascading-backpressure pattern this rule prevents cannot form.
- **Event-loop notification channel back-pressure deadlock** (#4145, #4231,
  May 2026): both production gateways (nova, vega) wedged simultaneously
  with `channel_pending=2048` on senders and `notification_channel_pending=0`
  on the receiver — a self-feeding cycle. The executor's WASM commit
  path called `notify_node_event(BroadcastStateChange{…}).await` (30 s
  timeout) on every contract UPDATE. Under sustained UPDATE fan-out
  (50–80 broadcast targets per popular River contract), the
  `event_loop_notification` channel filled up faster than the event
  loop could drain it, and the drainers themselves were blocked
  downstream — every drained notification spawned per-peer dispatch
  work that re-entered the same backpressure surface. Compounding:
  `ConnEvent::StreamSend` / `PipeStream` in `p2p_protoc.rs` did
  `peer_connection.sender.send(...).await` with no timeout, so a single
  congested peer could stall the event loop indefinitely. Fix
  (#4231): (a) added `OpManager::try_notify_node_event` and switched
  the four best-effort Broadcast emission sites to it, breaking the
  cycle at the executor side; (b) wrapped per-peer dispatch in
  `tokio::time::timeout(500ms, peer_connection.sender.reserve())` so
  ownership of `completion_tx` is retained on the timeout path,
  releasing the broadcast queue's permit immediately instead of
  waiting `STREAM_COMPLETION_TIMEOUT` (120 s). One emission site
  (`announce_contract_hosted`) intentionally kept the blocking variant
  because it carries a one-shot transition — see its inline
  `DELIBERATELY blocking` comment for the rationale.

  The same #4145/#4231 investigation surfaced a SECOND, separate
  degradation under the same fan-out — not a deadlock but a full-state
  broadcast STORM (#4442). A peer's summary was cached only after a
  *delta* send (PR #2763's `if sent_delta`), but a delta needs the
  peer's cached summary to compute against — a chicken-and-egg that
  trapped every summary-less subscriber (any new subscriber, or one
  whose summary was cleared) on full state forever. Under sustained
  fan-out that meant every update re-sent full state to every
  subscriber, swamping the event loop. Fixed (#4442) by caching the
  summary on any *delivered* broadcast (delta OR full state): #4235's
  real-delivery signal (`BroadcastDeliveryOutcome::Delivered`) makes
  caching on a full-state delivery safe, so the next broadcast to that
  peer collapses to a small delta. A wrongly-cached summary (e.g. a
  lost stream tail — `Delivered` is sender-side completion, not a
  receiver ack) is corrected by the periodic InterestSync summary
  exchange and by the delta-apply-failure → ResyncRequest path that
  clears the sender's cached summary.

- **Event-log channel send** (#4466, Jun 2026): `EventRegister::register_events`
  and `notify_of_time_out` (`tracing.rs`) did an untimed `.send().await` on the
  bounded event-log `mpsc::channel(1000)`, awaited from the network event loop's
  hot outbound path (`p2p_protoc.rs` `OutboundMessageWithTarget` and disconnect
  handlers). The sole consumer `record_logs` blocks on its metrics-server
  WebSocket send (`ws://127.0.0.1:55010`, present on peers running the fdev
  metrics server) and its AOF write; when that sink stalled, the channel filled
  and the event loop blocked forever, parking every runtime thread on a futex at
  0% CPU — a silent freeze (process alive, API port open, logging frozen
  mid-activity, no crash so systemd never restarts). This is the sibling send
  the #4145/#4231 fix missed, three lines above the peer-send it *did* wrap.
  Fixed (#4467) by switching both sends to `try_send` with drop-on-full
  (best-effort telemetry) plus a `DROPPED_EVENT_LOGS` counter; the `trace-ot`
  `OTEventRegister` siblings were converted too. Observed on a 0.2.75 peer and
  corroborated by a real-user diagnostic report on 0.2.74.

## Exception: same-runtime internal consumers

`.send().await` on a bounded channel is acceptable when **all** of the
following hold:

1. The receiver task is freenet-internal (we spawned it; it is not an
   external client or peer).
2. The consumer has no upstream dependency on the producer (no cycle
   through which a stalled producer could starve its own consumer).
3. Blocking is scoped to a single per-peer or per-connection event loop
   (so it cannot starve other peers).
4. Per-message consumer work is bounded and small (microseconds, not
   "I might call into a slow external API").

Document the exception inline at the call site, citing this section.
Existing exception: `peer_connection.rs::process_inbound` legacy stream
fragment forwarding into `inbound_streams[stream_id]`.

## Rules

### WHEN sending to a bounded channel

```
Is this inside an event loop, recv loop, or select! loop?
  → NEVER use .send().await
  → USE: try_send() and drop/log on Full
  → OR: wrap in tokio::time::timeout()

Is this in a spawned task where blocking is acceptable?
  → .send().await is OK, but prefer timeout protection

Is the receiver actually consumed in production?
  → If #[allow(dead_code)] on receiver: DO NOT create the channel
  → .send().await on a channel with no consumer blocks forever
    once the buffer fills
```

### Quick Audit

```bash
# Find .send().await in production code (potential deadlocks)
grep -rn "\.send(.*).await" crates/core/src/ | grep -v "try_send\|#\[test\]\|#\[cfg(test)\]\|/tests/"
```

Each hit must be justified: is the caller a critical loop? Can the receiver block?

---
description: Prevent bounded channel deadlocks — cascading backpressure from .send().await in event loops has caused 4+ production incidents
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
stalling the entire event loop. This has caused 4+ production deadlocks:

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

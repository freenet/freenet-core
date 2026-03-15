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

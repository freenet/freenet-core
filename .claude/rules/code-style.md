---
paths:
  - "**/*.rs"
---

# Code Style Rules

## Trigger-Action Rules

### WHEN writing a new module

```
1. Order contents as:
   mod declarations → imports → types → trait impls → functions

2. Group imports:
   std:: → external crates → crate::

3. Check: Is this in crates/core/?
   → YES: See .claude/rules/testing.md for DST requirements
```

### WHEN handling errors

```
Is this production code?
  → YES: Use explicit match/if-let, never .unwrap()
  → Use thiserror for custom error types

Is this test code?
  → .unwrap() and .expect("reason") are acceptable
```

**Production pattern:**
```rust
match operation() {
    Ok(result) => process(result),
    Err(e) => return Err(e.into()),
}
```

### WHEN writing async code

```
Need to wait on multiple futures?
  → Use tokio::select!, not sequential .await

Need shared state across tasks?
  → Prefer channels (mpsc, oneshot) over Arc<Mutex<>>

Need a channel for notifications, events, or responses?
  → ALWAYS use bounded channels: mpsc::channel(N), NOT unbounded_channel()
  → In non-async contexts (inside executor checkout), use try_send() not send().await
  → Drop messages when full (lossy) rather than blocking or growing unboundedly
  → unbounded_channel() is only acceptable for internal control flow where the
    sender count is statically known and bounded (e.g., handler event loops)

Is cancellation possible?
  → Document cancellation safety in function docs

Need a shared HashMap across threads/tasks?
  → Use DashMap, NOT Arc<RwLock<HashMap<_, _>>> or Arc<Mutex<HashMap<_, _>>>
  → DashMap provides fine-grained per-shard locking: concurrent reads/writes
    to different keys proceed in parallel instead of serializing on a global lock
  → Only exception: when you need to atomically read-modify-write across
    MULTIPLE keys in a single transaction. In that case document why DashMap
    is insufficient and a global lock is required.
```

### WHEN adding per-key collections, per-client tracking, or fan-out patterns

```
NEVER use unbounded per-key collections for data that external
actors (clients, network peers) can influence.

1. Per-key collections (subscribers per contract, peers per resource)
   MUST have a maximum size enforced at insertion time.
   → Reject new entries when the limit is reached
   → Return an error or false so callers know registration was rejected

2. Per-client/per-peer resource counts MUST be bounded.
   → A single client must not hold unbounded subscriptions across all keys
   → A single peer must not register unbounded interest across all contracts

3. Fan-out patterns (one event → N recipients) MUST cap the cost.
   → Cap expensive per-recipient work (e.g., WASM calls) to a fixed limit
   → Fall back to cheaper alternatives (e.g., full state vs computed delta)
   → Log warnings when fan-out exceeds a threshold

WHY: Unbounded collections are amplification vectors.
An attacker who can register N subscribers or open N channels can
multiply the cost of every state update by N, exhausting memory,
CPU (WASM execution), and executor time. Per-key caps prevent
amplification; per-client caps prevent resource spreading attacks.

See: executor.rs constants (MAX_SUBSCRIBERS_PER_CONTRACT,
SUBSCRIBER_NOTIFICATION_CHANNEL_SIZE, MAX_DELTA_COMPUTATIONS_PER_FANOUT,
FANOUT_WARNING_THRESHOLD, MAX_SUBSCRIPTIONS_PER_CLIENT)
and hosting.rs (MAX_DOWNSTREAM_SUBSCRIBERS_PER_CONTRACT)
```

### WHEN using `biased` in `tokio::select!`

**Unguarded `biased;` select is banned.**

```
BEFORE adding `biased;` to a select loop:

1. Document WHY biased ordering is needed
2. Document WHICH arm could starve under load
3. MUST enforce a per-iteration cap on high-throughput arms
   (e.g., process at most N items then yield to other arms)
4. Review ALL arms for cancellation safety:
   → Work that must not be cancelled belongs outside select or in tokio::spawn

WRONG:
  loop {
      tokio::select! { biased;
          packet = inbound.recv() => process(packet),  // Starves everything below
          _ = outbound_flush() => {},
          _ = maintenance_tick() => {},
      }
  }

CORRECT:
  loop {
      tokio::select! { biased;
          packet = inbound.recv() => {
              process(packet);
              // Cap: process at most 64 packets before yielding
              for _ in 0..63 {
                  match inbound.try_recv() {
                      Ok(p) => process(p),
                      Err(_) => break,
                  }
              }
          },
          _ = outbound_flush() => {},
          _ = maintenance_tick() => {},
      }
  }
```

**Audit targets:** `grep -r "biased;" crates/core/src/` — verify each site has per-iteration caps and cancellation-safety documentation.

### WHEN introducing numeric thresholds or limits

```
Does this threshold relate to a configurable value (min_connections, max_connections, etc.)?
  → YES: Derive from that config value, NEVER hardcode a number
  → NO: Define as a named constant with a comment explaining the choice

WRONG:
  const BOOTSTRAP_THRESHOLD: usize = 4;  // Magic number, breaks when config changes

CORRECT:
  let threshold = connection_manager.min_connections;  // Tied to config

WHY: Hardcoded thresholds silently break when the related configuration
changes. A hardcoded 4 caused nodes to plateau far below min_connections=10+
for 9 months because the threshold didn't scale with the config.

See: issue #3414 — BOOTSTRAP_THRESHOLD=4 vs min_connections
```

### WHEN spawning tasks with `GlobalExecutor::spawn`

**No fire-and-forget spawns for critical tasks.**

```
Is this a task that must run for the node's lifetime?
  → YES: Register its JoinHandle with BackgroundTaskMonitor
  → NO: Fire-and-forget is acceptable for short-lived work

Is this sending a registration or critical message?
  → Use send() with timeout, NOT try_send()
  → try_send() silently drops when channel is full → user-visible failures

Does this task serve multiple clients?
  → MUST isolate per-client errors
  → One client disconnecting must NOT crash the task for all clients

Does this match expression classify outcomes for metrics/telemetry?
  → MUST NOT use catch-all `_ =>` arms
  → Prefer exhaustive matching so new variants are explicitly handled

WRONG:
  let _handle = GlobalExecutor::spawn(critical_task());  // Handle dropped!

CORRECT:
  let handle = GlobalExecutor::spawn(critical_task());
  monitor.register("task_name", handle);
```

### WHEN writing retry/backoff loops

```
DO NOT hand-roll exponential backoff logic.
  → USE: TrackedBackoff<K> from crate::util::backoff for per-key tracking
  → USE: ExponentialBackoff from crate::util::backoff for stateless delay calc
  → See also: ConnectionBackoff (ring/), PeerConnectionBackoff (ring/)

TrackedBackoff<K> provides:
  - Per-key failure counting with record_failure(key) / record_success(key)
  - Automatic exponential delay: base * 2^failures, capped at max
  - LRU eviction when max_entries exceeded
  - remaining_backoff(key) → Option<Duration>
  - is_in_backoff(key) → bool
  - cleanup_expired() for periodic GC

Example:
  use crate::util::backoff::{ExponentialBackoff, TrackedBackoff};

  let config = ExponentialBackoff::new(
      Duration::from_millis(100),  // base delay
      Duration::from_secs(5),      // max delay
  );
  let mut tracker: TrackedBackoff<PeerId> = TrackedBackoff::new(config, 64);

  // On failure:
  tracker.record_failure(peer_id);

  // Before retry:
  if let Some(delay) = tracker.remaining_backoff(&peer_id) {
      tokio::time::sleep(delay).await;
  }

  // On success:
  tracker.record_success(&peer_id);

All retry/backoff loops MUST apply random jitter:
  → At least ±20% of the interval to prevent thundering herd

Backoff sleeps MUST be interruptible:
  → Use tokio::select! to race sleep against cancellation signal
  → Plain tokio::time::sleep() in retry loop is PROHIBITED unless <1s

WRONG:
  loop {
      if try_connect().is_ok() { break; }
      tokio::time::sleep(backoff).await;  // Not interruptible!
      backoff *= 2;
  }

CORRECT:
  loop {
      if try_connect().is_ok() { break; }
      let jittered = backoff.mul_f64(GlobalRng::random_f64_range(0.8, 1.2));
      tokio::select! {
          _ = tokio::time::sleep(jittered) => {},
          _ = cancel.notified() => break,
      }
      backoff = (backoff * 2).min(max_backoff);
  }
```

### WHEN you need time/rng/sockets in `crates/core/`

```
Need current time?
  → DO NOT use: std::time::Instant::now(), tokio::time::sleep()
  → USE: TimeSource trait (crates/core/src/simulation/)

Need randomness?
  → DO NOT use: rand::random(), rand::thread_rng()
  → USE: GlobalRng (crates/core/src/config/mod.rs)

Need network socket in tests?
  → DO NOT use: tokio::net::UdpSocket
  → USE: Socket trait (crates/core/src/transport/)
```

### WHEN writing documentation

```
Is this a public API?
  → Add /// doc comment with:
    - One-line summary
    - # Example section for complex functions
    - # Panics section if it can panic
    - # Errors section if it returns Result

Is this implementation logic?
  → Comment explains WHY, not WHAT
  → If code needs a WHAT comment, refactor for clarity instead
```

### BEFORE submitting code

```
1. Run: cargo fmt
2. Run: cargo clippy --all-targets --all-features
3. Fix all warnings (CI enforces this)
```

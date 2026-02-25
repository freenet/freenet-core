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

Is cancellation possible?
  → Document cancellation safety in function docs
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

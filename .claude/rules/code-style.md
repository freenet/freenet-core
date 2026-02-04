---
applyTo: "**/*.rs"
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

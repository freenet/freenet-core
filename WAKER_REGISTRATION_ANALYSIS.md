# Waker Registration Failure - Complete Analysis

## Executive Summary

PUT operations timeout after 10 seconds because the event loop's notification channel receiver **stops waking up** after initial handshakes complete. The issue is a **waker registration failure** caused by the combination of nested `select!` macros and timeout wrappers, not a deadlock.

## Timeline of Critical Discovery

### Test Run: 2025-10-14 12:24:17

**Last event loop entry BEFORE PUT:**
```
12:24:17.704169Z - ENTERING select! (peer: v6MWKgqHv8XYU5ZA, channel_id: 1)
```

**PUT notification sent 56ms later:**
```
12:24:17.760807Z - Pushing operation and sending notification (tx: 01K7HAVC30D9Q4M20N1BH3B8R1)
12:24:17.760852Z - Notification sent successfully (channel_id: 1)
```

**Result:**
- ZERO "SELECTED" logs after notification sent
- Event loop continues entering select! every ~100ms (handshake timeout)
- Notification channel NEVER becomes ready
- PUT times out after 10 seconds

## Root Cause: Waker Registration Failure

### The Problem

When `tokio::select!` with a timeout wrapper polls futures:

1. Event loop enters `select!` and all futures return `Poll::Pending`
2. Each future registers a waker with the `Context` provided to its `poll()` method
3. Task parks, waiting for ANY waker to fire
4. **56ms later**: Message sent to notification channel
5. Channel tries to wake task by calling `waker.wake()`
6. **WAKER DOES NOT FIRE** - task stays parked
7. Only the handshake timeout fires (every 100ms), returning `Continue`
8. Loop repeats - notification channel never checked again

### Why Wakers Aren't Firing

The combination of:
1. Outer `biased select!` with multiple branches
2. `timeout()` wrapper around handshake branch
3. Nested `select!` inside `handshake.wait_for_events()`

...creates a complex waker chain where:
- The timeout future wraps the handshake future
- The handshake future contains its own `select!`
- Each layer replaces/invalidates wakers from inner layers
- The notification channel's waker gets lost in this complexity

### Evidence from Multiple Test Runs

| Test Date | Last ENTERING | PUT Sent | Delta | SELECTED After PUT |
|-----------|---------------|----------|-------|-------------------|
| 2025-10-13 22:49 | 22:49:32.313Z | 22:49:41.490Z | 9.2s | 0 |
| 2025-10-14 09:57 | 09:57:19.007Z | 09:57:19.058Z | 51ms | 0 |
| 2025-10-14 12:24 | 12:24:17.704Z | 12:24:17.760Z | 56ms | 0 |

**Consistent pattern**: Notification sent while task parked in `select!`, waker never fires.

## Why Unit Tests Pass

The unit tests in `network_bridge.rs` work because they:
- Don't have nested `select!` macros
- Don't use `timeout()` wrappers
- Test the channel in isolation
- Don't have competing handshake futures

The bug only manifests in the full event loop with all the async complexity.

## Attempted Fixes

### 1. Timeout Wrapper (PR #1950) ❌
**Approach**: Wrap handshake branch with 100ms timeout
**Result**: Prevents complete deadlock but doesn't fix waker issue
**Why**: Creates busy-wait loop; wakers still not firing

### 2. Remove `biased` Modifier ❌
**Approach**: Test if `biased` causes waker issues
**Result**: Identical behavior (no difference)
**Why**: Problem is nested select!, not biased ordering

### 3. Manual Polling with Noop Waker ❌
**Approach**: Explicitly poll each future manually
**Result**: All futures perpetually pending (332k iterations in 25s)
**Why**: Noop waker doesn't wake; proves waker registration is the issue

### 4. Custom Priority Select Combinator ⏳
**Approach**: Reference-based combinator that flattens handshake futures
**Status**: Implemented but has type visibility issues
**Next**: Needs refactoring to work with Rust's privacy rules

## The Solution: Custom Select Combinator

### Concept

Replace `tokio::select!` with a custom `Future` that:
1. Takes **references** to all futures (stable memory locations)
2. **Flattens** handshake futures (no nested select!)
3. Registers all wakers in **single `poll()` call**
4. Enforces **strict priority** (notifications always first)

### Implementation Sketch

```rust
impl Future for PrioritySelectFuture {
    type Output = SelectResult;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<SelectResult> {
        // Priority 1: Notification channel
        if let Poll::Ready(msg) = Pin::new(&mut self.notification_rx).poll_recv(cx) {
            return Poll::Ready(SelectResult::Notification(msg));
        }

        // Priority 2: Op execution
        if let Poll::Ready(msg) = Pin::new(&mut self.op_execution_rx).poll_recv(cx) {
            return Poll::Ready(SelectResult::OpExecution(msg));
        }

        // Priority 3-5: Handshake futures (FLATTENED, not nested!)
        if let Poll::Ready(conn) = Pin::new(&mut self.handshake.inbound).poll_next(cx) {
            return Poll::Ready(SelectResult::HandshakeInbound(conn));
        }

        if let Poll::Ready(event) = Pin::new(&mut self.handshake.outbound).poll_next(cx) {
            return Poll::Ready(SelectResult::HandshakeOutbound(event));
        }

        // ... remaining futures ...

        // ALL wakers registered in ONE poll call!
        Poll::Pending
    }
}
```

### Why This Should Work

1. **Single waker registration point**: All futures register wakers in one `poll()` call
2. **No nested select!**: Handshake futures polled directly, not through `wait_for_events()`
3. **No timeout wrapper**: Priority handled directly without extra future layers
4. **Stable references**: Futures don't move between polls

## Files Modified

### Investigation
- [`INVESTIGATION_1944.md`](INVESTIGATION_1944.md) - Complete investigation history
- [`COMPREHENSIVE_INVESTIGATION_1944.md`](COMPREHENSIVE_INVESTIGATION_1944.md) - Detailed timeline

### Code Changes
- [`network_bridge.rs`](crates/core/src/node/network_bridge.rs) - Channel ID tracking, unit tests
- [`op_state_manager.rs`](crates/core/src/node/op_state_manager.rs) - Detailed send logging
- [`p2p_protoc.rs`](crates/core/src/node/network_bridge/p2p_protoc.rs) - Event loop logging, priority select (WIP)
- [`handshake.rs`](crates/core/src/node/network_bridge/handshake.rs) - Helper methods

## Diagnostic Commands

### Run test with detailed logging:
```bash
env RUSTFLAGS="--cfg tokio_unstable" RUST_LOG=freenet=debug \
  cargo test --package freenet --features console-subscriber \
  --test ubertest test_basic_room_creation -- --nocapture
```

### Check for waker issues:
```bash
grep -E "(ENTERING|SELECTED|Notification sent)" test.log
```

### Look for timing patterns:
```bash
grep "ENTERING.*channel_id: 1" test.log | tail -20
grep "Notification sent successfully.*channel_id: 1" test.log
```

## Next Steps

1. **Fix type visibility issues** in priority_select module
   - Refactor to use trait objects or opaque types for private handshake types
   - Complete HandshakeUnconfirmed branch handling

2. **Test custom combinator** once compiled
   - Run with `FREENET_PRIORITY_SELECT=1`
   - Compare waker behavior to legacy `tokio::select!`
   - Verify notifications wake up correctly

3. **If successful**: Replace all event loops with custom combinator

4. **Alternative if needed**: Investigate Tokio's waker implementation for bugs
   - Check Tokio issue tracker for nested select! + timeout issues
   - Consider filing bug report if reproducible

## Conclusion

This is NOT a channel bug, NOT a deadlock, and NOT a Tokio bug in isolation. It's an **emergent behavior** from combining multiple async patterns (nested select!, timeouts, biased polling) in a way that confuses the waker registration mechanism.

The custom select combinator approach directly addresses the root cause by simplifying the async structure and giving us explicit control over waker registration.

---
**Investigation by**: Claude Code
**Branch**: `fix/put-operation-network-send`
**Issue**: [#1944](https://github.com/freenet/freenet-core/issues/1944)
**Last Updated**: 2025-10-14

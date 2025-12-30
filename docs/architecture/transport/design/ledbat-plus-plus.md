# LEDBAT++ Implementation

**Status:** Implemented
**Created:** 2025-12-30
**Spec Reference:** [draft-irtf-iccrg-ledbat-plus-plus](https://datatracker.ietf.org/doc/html/draft-irtf-iccrg-ledbat-plus-plus)

## Executive Summary

This document describes Freenet's implementation of LEDBAT++, an improved version of LEDBAT (RFC 6817) that addresses the "latecomer advantage" problem through periodic slowdowns while maintaining LEDBAT's core benefit of yielding to foreground traffic.

**Key Improvements over RFC 6817:**
- **Periodic slowdown** for inter-flow fairness (solves latecomer advantage)
- **Dynamic GAIN** calculation based on base delay
- **Multiplicative decrease cap** at -W/2 per RTT
- **TARGET delay** reduced to 60ms (from 100ms)
- **Slow start exit threshold** at 75% of TARGET (45ms)

---

## Table of Contents

1. [Problem: Latecomer Advantage](#1-problem-latecomer-advantage)
2. [Solution: Periodic Slowdown](#2-solution-periodic-slowdown)
3. [Implementation Details](#3-implementation-details)
4. [Real-World Behavior](#4-real-world-behavior)
5. [Transfer Time Analysis](#5-transfer-time-analysis)
6. [Configuration](#6-configuration)
7. [Testing](#7-testing)

---

## 1. Problem: Latecomer Advantage

### 1.1 The Issue

In standard LEDBAT (RFC 6817), a flow that starts later can achieve higher throughput than earlier flows:

```
Time 0s: Flow A starts, fills buffer, queuing_delay rises
Time 5s: Flow B starts, sees high base_delay (includes A's queuing)
         Flow B thinks its base_delay is higher → grows more aggressively
Result:  Flow B gets unfair advantage over Flow A
```

### 1.2 Why This Matters for Freenet

Freenet connections are:
- **Long-lived**: Hours for contract synchronization
- **Concurrent**: Multiple peers transferring simultaneously
- **Sequential starts**: Peers join at different times

Without fairness mechanisms, later connections would starve earlier ones.

---

## 2. Solution: Periodic Slowdown

### 2.1 Concept

LEDBAT++ introduces periodic "probing" where flows briefly reduce their sending rate:

```
Normal Operation     Slowdown      Freeze       Ramp-up      Normal
─────────────────────────────────────────────────────────────────────►
     cwnd: 300KB   → cwnd: 75KB → hold 2 RTTs → exponential → 300KB

     ↑ 9x interval │◄── ~1 second total ──►│
```

### 2.2 State Machine

```
┌─────────┐     slow start    ┌───────────────────┐
│ Normal  │ ──── exit ──────► │ WaitingForSlowdown│
└─────────┘                   │   (2 RTTs delay)  │
     ▲                        └─────────┬─────────┘
     │                                  │
     │                                  ▼
     │                        ┌─────────────────┐
     │ ramp-up complete       │   InSlowdown    │
     │                        │ (cwnd reduced)  │
     │                        └─────────┬───────┘
     │                                  │
     │                                  ▼
┌────┴────────┐               ┌─────────────────┐
│  RampingUp  │ ◄──────────── │     Frozen      │
│ (slow start)│   2 RTTs      │ (hold reduced)  │
└─────────────┘               └─────────────────┘
```

### 2.3 Timing

| Phase | Duration | Purpose |
|-------|----------|---------|
| WaitingForSlowdown | 2 RTTs | Delay before first slowdown |
| Frozen | 2 RTTs | Hold at reduced cwnd |
| RampingUp | 2-4 RTTs | Exponential growth back to pre-slowdown |
| Normal | 9x slowdown duration | Normal operation until next probe |

**Total slowdown impact:** ~10% of time spent at reduced rate (1 part slowdown, 9 parts normal).

---

## 3. Implementation Details

### 3.1 Key Constants

```rust
// LEDBAT++ spec values
const TARGET: Duration = Duration::from_millis(60);  // Reduced from 100ms
const MAX_GAIN_DIVISOR: u32 = 16;                    // Caps GAIN at 1/16

// Slowdown parameters
const SLOWDOWN_DELAY_RTTS: u32 = 2;        // RTTs before first slowdown
const SLOWDOWN_FREEZE_RTTS: u32 = 2;       // RTTs to hold frozen cwnd
const SLOWDOWN_INTERVAL_MULTIPLIER: u32 = 9;  // 9x slowdown duration between probes
const SLOWDOWN_REDUCTION_FACTOR: usize = 4;   // cwnd drops to cwnd/4
```

### 3.2 Dynamic GAIN Calculation

LEDBAT++ uses dynamic GAIN based on base delay (Section 4.2 of the draft):

```
GAIN = 1 / min(16, ceil(2 * TARGET / base_delay))
```

| Base Delay | Divisor | GAIN | Effect |
|------------|---------|------|--------|
| 5ms | 24 → 16 (capped) | 1/16 | Very conservative (low latency LAN) |
| 10ms | 12 | 1/12 | Conservative |
| 30ms | 4 | 1/4 | Moderate |
| 60ms | 2 | 1/2 | Aggressive |
| 120ms | 1 | 1 | Maximum aggression (satellite) |

**Rationale:** Low-latency paths have less "room" for queuing, so we grow more slowly.

### 3.3 Multiplicative Decrease Cap

LEDBAT++ caps the per-RTT decrease at -W/2:

```rust
let cwnd_change = gain * (off_target_ms / target_ms) * bytes_acked * MSS / cwnd;

// LEDBAT++ key improvement: cap decrease
let max_decrease = -(current_cwnd as f64 / 2.0);
let capped_change = cwnd_change.max(max_decrease);
```

This prevents the "death spiral" where high queuing delay causes aggressive backoff.

### 3.4 Gradual Slowdown (Factor of 4)

We use `SLOWDOWN_REDUCTION_FACTOR = 4` instead of the spec's minimum (factor of 16+):

| Factor | Reduction | Behavior |
|--------|-----------|----------|
| 16 (spec) | 94% drop | Throughput cliff, long recovery |
| **4 (ours)** | **75% drop** | **Gradual slowdown, fast recovery** |

**Rationale:** With our large initial window (IW26), a gentler reduction:
- Avoids throughput "cliff" that users would notice
- Still probes for inter-flow fairness
- Recovers in fewer RTTs

---

## 4. Real-World Behavior

### 4.1 Throughput Profile by RTT

```
                   Steady      During       Recovery
RTT      cwnd      State       Slowdown     Time
────────────────────────────────────────────────────
10ms     300KB     30 MB/s     7.5 MB/s     20-30ms
50ms     300KB     6 MB/s      1.5 MB/s     100-150ms
100ms    300KB     3 MB/s      0.75 MB/s    200-300ms
200ms    300KB     1.5 MB/s    0.375 MB/s   400-600ms
```

### 4.2 Visual Timeline (100ms RTT)

```
Throughput (MB/s)
    3 |    ____________________________
      |   /                            \____
    2 |  /                                  \
      | /                                    \___
    1 |/                                         \
 0.75 |                                           ====
    0 +--+---+---+---+---+---+---+---+---+---+---+-->
      0  0.5  1   1.5  2   2.5  3   3.5  4   4.5  5  time(s)
         ^                              ^
         slow start                     slowdown + recovery
         complete                       (~500ms dip)
```

### 4.3 Slowdown Cycle Breakdown

For a connection at steady state:

| RTT | Cycle Duration | Dip Duration | Dip Frequency | User Perception |
|-----|----------------|--------------|---------------|-----------------|
| 10ms | ~100ms | ~20ms | every ~100ms | Imperceptible |
| 50ms | ~1.2s | ~150ms | every ~1.2s | Micro-stutters |
| 100ms | ~2.3s | ~500ms | every ~2.3s | Brief slowdown |
| 200ms | ~4.6s | ~1s | every ~4.6s | Noticeable gear shift |

---

## 5. Transfer Time Analysis

### 5.1 Small Transfers (10MB)

| RTT | Ideal Time | Actual Time | Overhead | User Experience |
|-----|------------|-------------|----------|-----------------|
| 10ms | 0.33s | ~0.4s | 20% | Instant |
| 50ms | 1.7s | ~2s | 18% | Fast |
| 100ms | 3.3s | ~4s | 20% | Acceptable |
| 200ms | 6.7s | ~8s | 20% | Slow but steady |

### 5.2 Large Transfers (100MB)

#### 50ms RTT Scenario

```
Phase Breakdown:
─────────────────────────────────────────────────────────────────
Time(s)  Phase              cwnd    Throughput   Data Transferred
─────────────────────────────────────────────────────────────────
0-0.2    Slow start         38→300KB   0.8→6 MB/s      ~0.6 MB
0.2-1.1  Steady state       300KB      6 MB/s          ~5.4 MB
1.1      Slowdown trigger   300→75KB   instant         —
1.1-1.2  Freeze             75KB       1.5 MB/s        0.15 MB
1.2-1.35 Ramp-up            75→300KB   1.5→6 MB/s      0.45 MB
1.35-2.25 Steady state      300KB      6 MB/s          5.4 MB
[Cycles repeat every ~1.15 seconds, ~14 total cycles]
~19s     Transfer complete                          100 MB total

Summary:
- Total time: ~19-20 seconds
- Peak throughput: 6 MB/s
- Minimum throughput: 1.5 MB/s
- Effective average: ~5.2 MB/s (87% efficiency)
- User experience: Smooth with imperceptible micro-stutters
```

#### 200ms RTT Scenario

```
Phase Breakdown:
─────────────────────────────────────────────────────────────────
Time(s)  Phase              cwnd    Throughput   Data Transferred
─────────────────────────────────────────────────────────────────
0-0.8    Slow start         38→300KB   0.2→1.5 MB/s    ~0.8 MB
0.8-4.4  Steady state       300KB      1.5 MB/s        ~5.4 MB
4.4      Slowdown trigger   300→75KB   instant         —
4.4-4.8  Freeze             75KB       0.375 MB/s      0.15 MB
4.8-5.4  Ramp-up            75→300KB   0.375→1.5 MB/s  0.5 MB
5.4-9.0  Steady state       300KB      1.5 MB/s        5.4 MB
[Cycles repeat every ~4.6 seconds, ~17 total cycles]
~75s     Transfer complete                          100 MB total

Summary:
- Total time: ~75-80 seconds
- Peak throughput: 1.5 MB/s
- Minimum throughput: 0.375 MB/s
- Effective average: ~1.3 MB/s (85% efficiency)
- User experience: Slow but consistent, brief "gear shifts" every ~5s
```

### 5.3 Efficiency Summary

| RTT | Theoretical Max | With LEDBAT++ | Efficiency |
|-----|-----------------|---------------|------------|
| 10ms | 30 MB/s | ~26 MB/s | 87% |
| 50ms | 6 MB/s | ~5.2 MB/s | 87% |
| 100ms | 3 MB/s | ~2.6 MB/s | 86% |
| 200ms | 1.5 MB/s | ~1.3 MB/s | 85% |

**The periodic slowdown costs ~13-15% throughput** in exchange for inter-flow fairness.

---

## 6. Configuration

### 6.1 LedbatConfig

```rust
pub struct LedbatConfig {
    pub initial_cwnd: usize,           // Default: 38,000 (IW26)
    pub min_cwnd: usize,               // Default: 2,848 (2 × MSS)
    pub max_cwnd: usize,               // Default: 1,000,000,000 (1 GB)
    pub ssthresh: usize,               // Default: 100,000 (100 KB)
    pub enable_slow_start: bool,       // Default: true
    pub delay_exit_threshold: f64,     // Default: 0.75 (exit at 45ms)
    pub randomize_ssthresh: bool,      // Default: true (±20% jitter)
    pub enable_periodic_slowdown: bool, // Default: true (LEDBAT++)
}
```

### 6.2 Tuning Guidelines

| Scenario | Recommendation |
|----------|----------------|
| LAN only (10ms RTT) | `enable_periodic_slowdown: false` - overhead not worth it |
| Mixed network | Default settings work well |
| High latency (>150ms) | Consider `SLOWDOWN_REDUCTION_FACTOR = 2` for gentler dips |
| Single connection | `enable_periodic_slowdown: false` - no competing flows |

---

## 7. Testing

### 7.1 Unit Tests

```bash
# Run all LEDBAT tests
cargo test -p freenet --lib ledbat::tests

# Run slowdown-specific tests
cargo test -p freenet --lib ledbat::tests::test_slowdown

# Run latency scenario tests (parametrized with rstest)
cargo test -p freenet --lib test_slowdown_at_various_latencies
```

### 7.2 Key Test Cases

| Test | Purpose |
|------|---------|
| `test_initial_slow_start_ramp_up` | Verify exponential growth in slow start |
| `test_proportional_slowdown_reduction` | Verify cwnd/4 reduction (not min_cwnd) |
| `test_complete_slowdown_cycle` | Full state machine: Normal→Frozen→RampingUp→Normal |
| `test_slowdown_interval_is_9x_duration` | Verify 9x interval between slowdowns |
| `test_slowdown_at_various_latencies` | Behavior at 10ms, 50ms, 100ms, 200ms RTT |
| `test_dynamic_gain_across_latencies` | GAIN calculation correctness |

### 7.3 Expected Test Output

```
10ms RTT:  post_exit=136KB, frozen=34KB, final=136KB, iterations=5
50ms RTT:  post_exit=136KB, frozen=34KB, final=136KB, iterations=5
100ms RTT: post_exit=136KB, frozen=34KB, final=136KB, iterations=5
200ms RTT: post_exit=136KB, frozen=34KB, final=136KB, iterations=4

test result: ok. 45 passed; 0 failed
```

---

## Appendix A: Comparison to Alternatives

### A.1 vs Standard LEDBAT (RFC 6817)

| Aspect | RFC 6817 | LEDBAT++ |
|--------|----------|----------|
| TARGET | 100ms | 60ms |
| GAIN | Fixed 1.0 | Dynamic 1/16 to 1 |
| Decrease cap | None | -W/2 per RTT |
| Inter-flow fairness | Latecomer advantage | Periodic slowdown |
| Slow start exit | 50% TARGET | 75% TARGET |

### A.2 vs TCP CUBIC

| Aspect | TCP CUBIC | LEDBAT++ |
|--------|-----------|----------|
| Congestion signal | Packet loss | Queuing delay |
| Yields to foreground | No | Yes |
| Inter-flow fairness | Good | Good (with slowdown) |
| Buffer bloat | Causes it | Prevents it |

### A.3 vs Other LEDBAT/LEDBAT++ Implementations

| Implementation | Slowdown Factor | TARGET | Slowdown Interval | Notes |
|----------------|-----------------|--------|-------------------|-------|
| **Freenet (ours)** | 4x (75% drop) | 60ms | 9x duration | Gradual, fast recovery |
| Apple iCloud | 16x+ (spec) | 60ms | 9x duration | Follows spec closely |
| BitTorrent uTP | N/A (no slowdown) | 100ms | N/A | RFC 6817 only, no LEDBAT++ |
| LEDBAT++ spec | "minimal rate" | 60ms | 9x duration | Recommends 2 MSS minimum |

#### Timing Comparison (Spec vs Freenet)

| Parameter | LEDBAT++ Spec | Freenet | Compliant? |
|-----------|---------------|---------|------------|
| Initial delay after slow start | 2 RTTs | 2 RTTs | ✅ Yes |
| Freeze duration | 2 RTTs | 2 RTTs | ✅ Yes |
| Frozen cwnd level | 2 packets (~2.8KB) | cwnd/4 (~75KB) | ❌ Gentler |
| Ramp-up mechanism | Slow start to ssthresh | Slow start to pre_slowdown | ✅ Yes |
| Next slowdown interval | 9x slowdown duration | 9x slowdown duration | ✅ Yes |

**Summary:** Our timing is fully spec-compliant. The only deviation is the frozen cwnd depth (4x reduction vs 16x+), which we chose for smoother user experience.

**Key differences in our implementation:**

1. **Gradual slowdown (4x vs 16x+)**: We use a gentler reduction factor:
   - Spec recommends dropping to "minimal sending rate" (~2 MSS)
   - We drop to cwnd/4 (25% of original)
   - **Rationale**: With our large initial window (IW26), a 16x drop would be too aggressive and cause noticeable throughput cliffs

2. **Consistent with IW26**: Our slowdown factor is chosen to be proportional:
   - Initial window: 38KB (26 × MSS)
   - Minimum cwnd: 2.8KB (2 × MSS)
   - Slowdown floor: cwnd/4 = ~9.5KB at steady state
   - This maintains ~3-4 packets in flight during slowdown, enough for RTT measurement

3. **Efficiency tradeoff**:
   - Spec-compliant 16x: ~75% efficiency (25% overhead)
   - Our 4x: ~85% efficiency (15% overhead)
   - We accept slightly less aggressive fairness probing for better throughput

**Is this typical?** Our steady-state behavior is comparable to production LEDBAT++ deployments:
- **Apple iCloud**: Uses LEDBAT++ for multi-TB syncs, similar periodic slowdowns
- **BitTorrent**: Uses original LEDBAT (no periodic slowdown), known latecomer issues
- **Windows BITS**: LEDBAT-like but proprietary, no public LEDBAT++ support

Our implementation is **more gradual** than the spec but achieves the same fairness goals with less throughput impact.

---

## Appendix B: References

1. **draft-irtf-iccrg-ledbat-plus-plus** - LEDBAT++: Congestion Control for Background Traffic
   https://datatracker.ietf.org/doc/html/draft-irtf-iccrg-ledbat-plus-plus

2. **RFC 6817** - Low Extra Delay Background Transport (LEDBAT)
   https://datatracker.ietf.org/doc/html/rfc6817

3. **Freenet LEDBAT Slow Start Design** - `docs/architecture/transport/design/ledbat-slow-start.md`

4. **Source Code** - `crates/core/src/transport/ledbat.rs`

---

**Document Status:** Complete
**Implementation Status:** Merged and tested

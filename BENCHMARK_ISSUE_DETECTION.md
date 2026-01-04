# How Benchmarks Could Detect Issues from PR #2580/#2582

## The Problem: "ssthresh Death Spiral" (Issue #2578)

**From PR #2580 commit message:**
> On high-latency paths (>100ms RTT), repeated timeouts can cause ssthresh
> to collapse to ~5KB, severely limiting throughput recovery.
> Setting a higher floor prevents this "ssthresh death spiral".

### What Happened

1. **Initial state**: Connection has reasonable ssthresh (e.g., 100KB)
2. **Timeout occurs**: LEDBAT cuts ssthresh in half → 50KB
3. **Another timeout**: ssthresh → 25KB
4. **More timeouts**: ssthresh → 12KB → 6KB → **~5KB (floor)**
5. **Death spiral**: ssthresh stuck at ~5KB, throughput can never recover

**Root cause**: On high-latency or lossy paths, timeouts are more common, causing ssthresh to collapse repeatedly.

**Impact**: Throughput drops from 10 MB/s to <1 MB/s and never recovers

---

## Why Current Benchmarks Didn't Catch This

### 1. **No High-Latency Tests**

Current benchmarks use:
- Mock sockets (zero latency)
- Or low simulated latency (2-10ms)

**Missing**: Tests with 100ms+, 200ms+, 500ms+ RTT (intercontinental, satellite)

### 2. **No Timeout/Packet Loss Scenarios**

Current benchmarks assume:
- Zero packet loss (clean mock sockets)
- No timeout events

**Missing**: Tests with:
- Packet loss (1%, 5%, 10%)
- Timeout scenarios
- Timeout cascades

### 3. **No Long-Running Transfer Tests**

Current benchmarks run for:
- 3-30 seconds per variant
- Single transfer or a few iterations

**Missing**: Tests that:
- Run long enough for multiple timeouts to occur
- Measure throughput recovery after timeouts
- Detect cumulative degradation

### 4. **No Throughput Recovery Metrics**

Current benchmarks measure:
- Initial throughput
- Steady-state throughput

**Missing**:
- Throughput after 1 timeout
- Throughput after N timeouts
- Recovery rate

---

## Benchmarks That WOULD Have Caught This

### 1. **High-Latency Path Benchmark**

```rust
/// Detect ssthresh collapse on high-latency paths
#[test]
fn bench_high_latency_throughput() {
    for &rtt_ms in &[50, 100, 200, 500] {  // Continental to satellite
        // Measure sustained throughput over 1 minute
        // Inject occasional packet loss (1%)
        // Verify throughput doesn't collapse over time

        let initial_throughput = measure_throughput(0..10);
        let late_throughput = measure_throughput(50..60);

        // Should not degrade more than 20%
        assert!(late_throughput > initial_throughput * 0.8,
            "Throughput collapsed at {}ms RTT: {} → {}",
            rtt_ms, initial_throughput, late_throughput);
    }
}
```

**What it tests**: Sustained throughput on high-latency paths
**What it catches**: ssthresh death spiral (throughput would collapse)

### 2. **Timeout Recovery Benchmark**

```rust
/// Measure throughput recovery after timeouts
#[test]
fn bench_timeout_recovery() {
    let mut peer = create_connection(rtt = 100ms);

    // Baseline: measure throughput
    let baseline = measure_throughput(&mut peer, 10s);

    // Inject timeout (simulate packet loss burst)
    inject_packet_loss(100%  duration = 2s);  // Force timeout

    // Measure recovery
    let recovery_throughput = measure_throughput(&mut peer, 10s);

    // After ONE timeout, should recover to >70% baseline
    assert!(recovery_throughput > baseline * 0.7,
        "Poor recovery after timeout: {} → {}",
        baseline, recovery_throughput);

    // Inject MULTIPLE timeouts
    for _ in 0..5 {
        inject_packet_loss(100%, duration = 2s);
    }

    let final_throughput = measure_throughput(&mut peer, 10s);

    // Even after 5 timeouts, should maintain >50% baseline
    assert!(final_throughput > baseline * 0.5,
        "ssthresh death spiral detected: {} → {}",
        baseline, final_throughput);
}
```

**What it tests**: Throughput after repeated timeouts
**What it catches**: ssthresh death spiral (throughput would collapse to <10% after 5 timeouts)

### 3. **ssthresh Floor Validation**

```rust
/// Verify ssthresh doesn't collapse below minimum
#[test]
fn bench_ssthresh_floor() {
    let controller = LedbatController::new(
        initial_cwnd = 100KB,
        ssthresh = 100KB,
        min_ssthresh = 50KB,  // NEW: minimum floor
    );

    // Simulate 10 consecutive timeouts
    for i in 0..10 {
        controller.on_timeout();

        let current_ssthresh = controller.ssthresh();
        println!("After timeout {}: ssthresh = {}", i, current_ssthresh);

        // Should NEVER go below minimum
        assert!(current_ssthresh >= 50KB,
            "ssthresh below floor: {}", current_ssthresh);
    }
}
```

**What it tests**: ssthresh floor enforcement
**What it catches**: ssthresh collapsing below configured minimum

### 4. **Long-Running Stability Test**

```rust
/// Soak test: measure throughput over 1 hour with intermittent loss
#[test]
#[ignore]  // Long-running, not for CI
fn bench_long_running_stability() {
    let peer = create_connection(rtt = 100ms);

    // Run for 1 hour with periodic packet loss
    let mut throughputs = vec![];
    for minute in 0..60 {
        // 90% of time: normal
        // 10% of time: 5% packet loss (trigger timeouts)
        let packet_loss = if minute % 10 == 0 { 0.05 } else { 0.0 };

        let throughput = measure_throughput(
            &mut peer,
            duration = 60s,
            packet_loss
        );

        throughputs.push(throughput);
    }

    // Calculate throughput trend
    let initial_avg = throughputs[0..10].average();
    let final_avg = throughputs[50..60].average();

    // Should not degrade more than 20% over time
    assert!(final_avg > initial_avg * 0.8,
        "Long-term throughput degradation: {} → {}",
        initial_avg, final_avg);

    // Verify no continuous decline
    let slope = calculate_trend(&throughputs);
    assert!(slope > -0.01,  // Less than 1% decline per hour
        "Continuous throughput decline detected: slope = {}",
        slope);
}
```

**What it tests**: Long-term stability under realistic conditions
**What it catches**: Cumulative effects like ssthresh death spiral

---

## Property-Based Tests (What PR #2580 Added)

PR #2580 added comprehensive property tests to `ledbat.rs`:

```rust
#[cfg(test)]
mod property_tests {
    // Test that cwnd never exceeds max_cwnd
    // Test that ssthresh never goes below min_ssthresh (NEW!)
    // Test that flightsize never exceeds cwnd
    // Test timeout handling
    // Test slow start exit conditions
}
```

**These are great** but they test **logic**, not **real-world behavior**.

**Missing**: Integration tests that exercise the full protocol under realistic network conditions.

---

## Recommended Benchmark Additions

### Phase 1: Add to CI (Detect Similar Issues)

```rust
// crates/core/benches/transport_ci.rs

criterion_group!(
    name = resilience_ci;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(20))
        .noise_threshold(0.25)  // Higher variance expected
        .significance_level(0.05);
    targets =
        bench_high_latency_sustained,  // 100ms, 200ms RTT
        bench_packet_loss_1pct,        // 1% loss
        bench_timeout_recovery,        // Single timeout recovery
);

criterion_main!(
    warm_throughput_ci,
    resilience_ci,  // NEW!
    connection_setup_ci,
    streaming_buffer_ci,
);
```

**Estimated runtime**: +3-5 minutes to CI
**Impact**: Would have caught ssthresh death spiral

### Phase 2: Extended Validation (Not CI, Weekly)

```rust
// crates/core/benches/transport_soak.rs

criterion_main!(
    long_running_stability,        // 1+ hour
    timeout_cascade,               // Multiple consecutive timeouts
    varying_latency_throughput,    // Latency spikes
    intercontinental_paths,        // 200ms+ RTT
);
```

**Run**: Weekly on dedicated hardware
**Purpose**: Catch edge cases and long-term issues

---

## Implementation Priority

### P0 (Would Have Caught #2578)

1. **`bench_high_latency_sustained`** - 100ms+, sustained transfer
   - Effort: 1 day
   - Catches: ssthresh death spiral on high-latency paths

2. **`bench_timeout_recovery`** - Measure recovery after timeouts
   - Effort: 2 days
   - Catches: Poor recovery behavior

3. **`bench_packet_loss_1pct`** - Sustained throughput with packet loss
   - Effort: 1 day
   - Catches: Cumulative degradation under loss

### P1 (Defense in Depth)

4. **`bench_timeout_cascade`** - Multiple consecutive timeouts
   - Effort: 1 day
   - Catches: Cascading failures

5. **Property tests for min_ssthresh** (DONE in #2580!)
   - Ensures floor is enforced

---

## Key Insight

**The infrastructure to test this ALREADY EXISTS:**

```rust
// From crates/core/src/transport/mock_transport.rs

pub enum PacketDropPolicy {
    ReceiveAll,
    Factor(f64),              // ← Can simulate packet loss!
    Ranges(Vec<Range<usize>>),
}

pub async fn create_mock_peer_with_delay(
    policy: PacketDropPolicy,
    channels: Channels,
    delay: Duration,          // ← Can simulate high latency!
) -> ...
```

**We just need to USE it in benchmarks!**

---

## Concrete Next Steps

1. **This week**: Implement `bench_high_latency_sustained`
   ```rust
   bench_with_input(BenchmarkId::new("rtt", 100), &100, |b, &rtt_ms| {
       let delay = Duration::from_millis(rtt_ms / 2);  // One-way delay
       let peers = create_peer_pair_with_delay(new_channels(), delay)
           .await.connect().await;

       // Transfer 10 MB, measure throughput
       // Should not degrade over time
   });
   ```

2. **Next week**: Implement `bench_packet_loss_1pct`
   ```rust
   let peers = create_peer_pair_with_policy(
       PacketDropPolicy::Factor(0.01),  // 1% loss
       new_channels()
   ).await.connect().await;

   // Transfer 10 MB, measure:
   // - Final throughput
   // - Timeout count
   // - ssthresh evolution
   ```

3. **Week 3**: Add to CI, validate on historical commits
   - Run new benchmarks on commit before #2580
   - Verify they detect the issue (throughput should collapse)
   - Run on commit after #2580
   - Verify issue is fixed (throughput stable)

**Total effort**: ~1 week to add resilience benchmarks that would have caught #2578

---

## Bottom Line

**Question**: How could we detect issues fixed in #2580/#2582?

**Answer**: Add benchmarks that test:
1. High-latency paths (100ms+ RTT)
2. Packet loss scenarios (1-5%)
3. Timeout recovery
4. Long-running stability

**The infrastructure exists** - we just need to write the benchmark functions.

**Estimated effort**: ~1 week

**ROI**: Would have caught the ssthresh death spiral before it shipped.

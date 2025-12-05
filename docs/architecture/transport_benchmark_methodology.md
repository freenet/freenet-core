# Transport Benchmarking Methodology

## The Problem: Layers of Noise

When benchmarking network transport, you're measuring through multiple layers:

```
┌─────────────────────────────────────────────────────────────────┐
│ Application (Freenet transport)                                  │ ← What we want to measure
├─────────────────────────────────────────────────────────────────┤
│ Tokio runtime (async executor, I/O driver)                      │ ← Runtime overhead
├─────────────────────────────────────────────────────────────────┤
│ Rust std / libc (syscall wrappers)                              │
├─────────────────────────────────────────────────────────────────┤
│ Kernel (scheduler, network stack, socket buffers)               │ ← Kernel overhead
├─────────────────────────────────────────────────────────────────┤
│ Hypervisor / Container runtime (if present)                     │ ← Virtualization overhead
├─────────────────────────────────────────────────────────────────┤
│ Hardware (NIC, CPU, memory, cache)                              │ ← Hardware characteristics
└─────────────────────────────────────────────────────────────────┘
```

---

## 1. Sources of Noise

### 1.1 CPU Noise

| Source | Impact | Mitigation |
|--------|--------|------------|
| Frequency scaling | 10-40% variance | Disable DVFS, pin to max frequency |
| Turbo boost | 5-15% variance | Disable turbo or measure with it enabled consistently |
| Thermal throttling | 20%+ after warm-up | Adequate cooling, monitor temps |
| SMT/Hyperthreading | Unpredictable sharing | Disable HT or pin to physical cores |
| NUMA | 2-3x latency for remote memory | Pin to single NUMA node |

### 1.2 Kernel Noise

| Source | Impact | Mitigation |
|--------|--------|------------|
| Scheduler | Context switch = 1-10µs | CPU isolation, SCHED_FIFO |
| Interrupts | IRQ handling steals cycles | IRQ affinity, isolcpus |
| Timer ticks | 1-4ms jitter | tickless kernel (nohz_full) |
| Memory pressure | GC, page faults | Lock memory, disable swap |
| Network stack | Per-packet overhead | Kernel bypass (DPDK, io_uring) |

### 1.3 Virtualization Noise

| Environment | Overhead | Notes |
|-------------|----------|-------|
| Bare metal | Baseline | Best for benchmarking |
| KVM/QEMU | 5-15% CPU, 10-30% network | Virtio helps significantly |
| VMware/Hyper-V | Similar to KVM | Depends on configuration |
| Docker (native) | <1% CPU, ~5% network | Uses host kernel |
| Docker (Mac/Win) | 20-50% | Runs in hidden VM |
| WSL2 | 10-30% | Hyper-V backend |
| Cloud instances | Highly variable | Noisy neighbors, shared resources |

---

## 2. Isolating Pure Transport Logic

### 2.1 Layer Separation Strategy

To measure just the transport code without OS interaction:

```
Level 0: Pure Logic (no I/O)
├── Encryption/decryption throughput
├── Serialization throughput
├── Packet tracker operations
├── Rate limiter calculations
└── State machine transitions

Level 1: Mock I/O (in-process channels)
├── Replace UdpSocket with MockSocket
├── Use mpsc channels instead of kernel sockets
├── Measure end-to-end without syscalls
└── Isolates protocol logic from OS

Level 2: Loopback (minimal kernel)
├── 127.0.0.1 avoids NIC entirely
├── Kernel still involved (syscalls, buffers)
├── Good for syscall overhead measurement
└── Affected by kernel scheduling

Level 3: Real Network
├── Full stack measurement
├── Includes all real-world factors
├── Most representative but noisiest
└── Requires controlled environment
```

### 2.2 Mock Socket Implementation

The codebase already has a `MockSocket` for testing (`connection_handler.rs:1204-1286`):

```rust
struct MockSocket {
    inbound: Mutex<mpsc::UnboundedReceiver<(SocketAddr, Vec<u8>)>>,
    this: SocketAddr,
    packet_drop_policy: PacketDropPolicy,
    channels: Channels,  // Arc<DashMap<SocketAddr, Sender>>
}
```

This can benchmark pure protocol overhead without any syscalls.

---

## 3. Recommended Benchmark Environments

### 3.1 Bare Metal (Gold Standard)

```bash
# CPU isolation for benchmark cores
# Add to kernel cmdline: isolcpus=2,3 nohz_full=2,3 rcu_nocbs=2,3

# Disable frequency scaling
echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor

# Disable turbo boost
echo 1 | sudo tee /sys/devices/system/cpu/intel_pstate/no_turbo

# Pin benchmark to isolated cores
taskset -c 2,3 cargo bench

# Or use cset for full isolation
sudo cset shield -c 2,3 -k on
sudo cset shield --exec -- cargo bench
```

### 3.2 Container Environment

**DO NOT** use Docker for benchmarking on Mac/Windows - they run in hidden VMs.

On Linux, containers add minimal overhead but watch for:

```yaml
# docker-compose.yml for benchmarking
services:
  benchmark:
    image: rust:latest
    privileged: true  # Needed for perf, CPU pinning
    cpuset: "2,3"     # Pin to specific cores
    network_mode: host  # Avoid Docker network overhead
    ulimits:
      memlock: -1     # Allow memory locking
    volumes:
      - /sys:/sys:ro  # Access CPU controls
```

### 3.3 VM Environment

If you must use VMs:

```
KVM/QEMU recommended settings:
├── CPU: host-passthrough (exposes real CPU features)
├── vCPU pinning: pin vCPUs to physical cores
├── NUMA: match guest topology to host
├── Network: virtio-net with vhost-net
├── Disable: balloon driver, ksm, transparent hugepages
└── Memory: pre-allocated, locked
```

Example libvirt XML:
```xml
<cpu mode='host-passthrough'>
  <topology sockets='1' cores='4' threads='1'/>
</cpu>
<vcpu placement='static' cpuset='2-5'>4</vcpu>
<memoryBacking>
  <locked/>
  <hugepages/>
</memoryBacking>
```

---

## 4. Benchmark Hierarchy

### 4.1 Level 0: Pure Logic Benchmarks (No I/O)

These are **completely deterministic** and unaffected by OS:

```rust
// bench_pure_logic.rs - Zero I/O, pure computation

/// Encryption throughput - measures only AES-GCM
fn bench_encrypt_pure(c: &mut Criterion) {
    let cipher = Aes128Gcm::new(&[0u8; 16].into());
    let mut data = vec![0u8; 1400];
    let nonce = [0u8; 12];

    c.bench_function("aes_gcm_encrypt_1400b", |b| {
        b.iter(|| {
            cipher.encrypt_in_place_detached(
                &nonce.into(), &[], &mut data
            ).unwrap()
        })
    });
}

/// Packet tracker - pure state machine
fn bench_tracker_pure(c: &mut Criterion) {
    c.bench_function("tracker_report_sent", |b| {
        let mut tracker = SentPacketTracker::new();
        let payload: Arc<[u8]> = vec![0u8; 1400].into();
        let mut id = 0u32;

        b.iter(|| {
            tracker.report_sent_packet(id, payload.clone());
            id += 1;
        })
    });
}

/// Serialization - pure computation
fn bench_serialize_pure(c: &mut Criterion) {
    let msg = SymmetricMessage {
        packet_id: 12345,
        confirm_receipt: vec![1, 2, 3],
        payload: SymmetricMessagePayload::ShortMessage {
            payload: vec![0u8; 1300]
        },
    };

    c.bench_function("bincode_serialize_packet", |b| {
        b.iter(|| bincode::serialize(&msg).unwrap())
    });
}
```

### 4.2 Level 1: Mock I/O Benchmarks (In-Process)

Uses channels instead of sockets - measures protocol logic without syscalls:

```rust
// bench_mock_io.rs - Protocol logic with mock transport

async fn setup_mock_peers() -> (PeerConnection, PeerConnection) {
    let channels = Arc::new(DashMap::new());

    let (pk_a, mut handler_a, addr_a) =
        set_peer_connection_mock(channels.clone()).await;
    let (pk_b, mut handler_b, addr_b) =
        set_peer_connection_mock(channels.clone()).await;

    // Connect through mock transport
    let (conn_a, conn_b) = tokio::join!(
        handler_a.connect(pk_b, addr_b),
        handler_b.connect(pk_a, addr_a),
    );

    (conn_a.unwrap(), conn_b.unwrap())
}

fn bench_protocol_throughput(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("mock_io_roundtrip_1kb", |b| {
        b.to_async(&rt).iter(|| async {
            let (mut sender, mut receiver) = setup_mock_peers().await;
            let data = vec![0u8; 1024];

            sender.send(&data).await.unwrap();
            let received = receiver.recv().await.unwrap();

            black_box(received)
        })
    });
}
```

### 4.3 Level 2: Loopback Benchmarks (Kernel Involved)

Measures syscall overhead but avoids NIC:

```rust
// bench_loopback.rs - Real sockets, loopback interface

fn bench_loopback_syscall_overhead(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("udp_loopback_roundtrip", |b| {
        b.to_async(&rt).iter(|| async {
            let sock = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let addr = sock.local_addr().unwrap();
            sock.connect(addr).await.unwrap();

            let send_buf = [0u8; 1400];
            let mut recv_buf = [0u8; 1500];

            sock.send(&send_buf).await.unwrap();
            sock.recv(&mut recv_buf).await.unwrap();
        })
    });
}
```

### 4.4 Level 3: Real Network (Full Stack)

Requires controlled hardware environment - not suitable for CI.

---

## 5. Statistical Methodology

### 5.1 Dealing with Variance

```rust
use criterion::{Criterion, SamplingMode};

fn configure_criterion() -> Criterion {
    Criterion::default()
        // More samples for noisy benchmarks
        .sample_size(500)

        // Longer measurement for stability
        .measurement_time(Duration::from_secs(30))

        // Longer warmup to reach steady state
        .warm_up_time(Duration::from_secs(5))

        // For very noisy benchmarks
        .sampling_mode(SamplingMode::Flat)

        // Confidence interval
        .confidence_level(0.99)

        // Noise threshold for regression detection
        .noise_threshold(0.05)  // 5% noise tolerance
}
```

### 5.2 Percentile Tracking

```rust
use hdrhistogram::Histogram;

struct LatencyTracker {
    histogram: Histogram<u64>,
}

impl LatencyTracker {
    fn record(&mut self, latency_ns: u64) {
        self.histogram.record(latency_ns).unwrap();
    }

    fn report(&self) {
        println!("p50: {}ns", self.histogram.value_at_percentile(50.0));
        println!("p95: {}ns", self.histogram.value_at_percentile(95.0));
        println!("p99: {}ns", self.histogram.value_at_percentile(99.0));
        println!("p99.9: {}ns", self.histogram.value_at_percentile(99.9));
        println!("max: {}ns", self.histogram.max());
    }
}
```

### 5.3 Outlier Analysis

```rust
fn analyze_results(samples: &[Duration]) {
    let mut sorted: Vec<_> = samples.iter().map(|d| d.as_nanos()).collect();
    sorted.sort();

    let len = sorted.len();
    let median = sorted[len / 2];
    let p1 = sorted[len / 100];
    let p99 = sorted[len * 99 / 100];

    // IQR-based outlier detection
    let q1 = sorted[len / 4];
    let q3 = sorted[len * 3 / 4];
    let iqr = q3 - q1;
    let outlier_threshold = q3 + (iqr as f64 * 1.5) as u128;

    let outliers: Vec<_> = sorted.iter()
        .filter(|&&v| v > outlier_threshold)
        .collect();

    println!("Outliers: {} ({:.2}%)",
        outliers.len(),
        outliers.len() as f64 / len as f64 * 100.0
    );
}
```

---

## 6. Environment Validation

Before running benchmarks, validate the environment:

```rust
// bench_env_validation.rs

fn validate_benchmark_environment() {
    // Check CPU frequency is stable
    let freq_path = "/sys/devices/system/cpu/cpu0/cpufreq/scaling_cur_freq";
    if let Ok(freq) = std::fs::read_to_string(freq_path) {
        let freq_khz: u64 = freq.trim().parse().unwrap();
        let freq_ghz = freq_khz as f64 / 1_000_000.0;
        println!("CPU frequency: {:.2} GHz", freq_ghz);

        // Check if it's pinned (compare with scaling_max_freq)
        let max_path = "/sys/devices/system/cpu/cpu0/cpufreq/scaling_max_freq";
        if let Ok(max) = std::fs::read_to_string(max_path) {
            let max_khz: u64 = max.trim().parse().unwrap();
            if freq_khz < max_khz * 95 / 100 {
                eprintln!("WARNING: CPU not at max frequency!");
            }
        }
    }

    // Check for isolated CPUs
    if let Ok(isolated) = std::fs::read_to_string("/sys/devices/system/cpu/isolated") {
        if isolated.trim().is_empty() {
            eprintln!("WARNING: No isolated CPUs (isolcpus not set)");
        } else {
            println!("Isolated CPUs: {}", isolated.trim());
        }
    }

    // Check if running in VM
    if std::path::Path::new("/sys/hypervisor/type").exists() {
        let hypervisor = std::fs::read_to_string("/sys/hypervisor/type")
            .unwrap_or_default();
        eprintln!("WARNING: Running in VM ({})", hypervisor.trim());
    }

    // Check container status
    if std::path::Path::new("/.dockerenv").exists() {
        eprintln!("WARNING: Running in Docker container");
    }
}
```

---

## 7. Recommended Benchmark Script

```bash
#!/bin/bash
# run_benchmarks.sh - Proper benchmark execution

set -e

# Validate environment
echo "=== Environment Validation ==="
if [ -f /sys/devices/system/cpu/intel_pstate/no_turbo ]; then
    turbo=$(cat /sys/devices/system/cpu/intel_pstate/no_turbo)
    if [ "$turbo" = "0" ]; then
        echo "WARNING: Turbo boost is enabled"
    fi
fi

# Check governor
governor=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)
if [ "$governor" != "performance" ]; then
    echo "WARNING: CPU governor is '$governor', should be 'performance'"
fi

# Check isolated CPUs
isolated=$(cat /sys/devices/system/cpu/isolated 2>/dev/null || echo "none")
echo "Isolated CPUs: $isolated"

# Run benchmarks with proper affinity
echo ""
echo "=== Running Benchmarks ==="

# Level 0: Pure logic (any CPU is fine)
echo "Level 0: Pure logic benchmarks..."
cargo bench --bench transport_perf -- "crypto/" "serialization/"

# Level 1: Mock I/O (pin to isolated CPU if available)
echo "Level 1: Mock I/O benchmarks..."
if [ "$isolated" != "none" ]; then
    first_isolated=$(echo $isolated | cut -d',' -f1 | cut -d'-' -f1)
    taskset -c $first_isolated cargo bench --bench transport_perf -- "mock_io/"
else
    cargo bench --bench transport_perf -- "mock_io/"
fi

# Level 2: Loopback (definitely pin)
echo "Level 2: Loopback benchmarks..."
if [ "$isolated" != "none" ]; then
    taskset -c $first_isolated cargo bench --bench transport_perf -- "loopback/"
else
    echo "WARNING: Running loopback without CPU isolation"
    cargo bench --bench transport_perf -- "loopback/"
fi

echo ""
echo "=== Results in target/criterion/ ==="
```

---

## 8. Impact of Virtualization

### 8.1 What Virtualization Affects

| Component | VM Overhead | Container Overhead |
|-----------|-------------|-------------------|
| CPU compute | 1-5% (host-passthrough) | ~0% |
| Memory access | 5-10% (EPT/NPT) | ~0% |
| Syscalls | 10-30% (vmexit) | ~0% |
| Network (virtio) | 10-30% | 5-10% (bridge) |
| Network (SR-IOV) | ~5% | N/A |
| Timer resolution | Degraded | Shared with host |

### 8.2 Recommendations by Environment

**For CI/CD (automated testing):**
- Use Level 0 and Level 1 benchmarks only
- These are deterministic and reproducible
- Set wide noise thresholds (10-20%)

**For development (local testing):**
- Bare metal Linux preferred
- If VM: KVM with host-passthrough, pinned vCPUs
- If Docker on Linux: native mode with `--privileged`
- Avoid Docker Desktop (Mac/Windows)

**For official performance numbers:**
- Bare metal only
- Isolated CPUs, disabled turbo
- Multiple runs over days
- Publish full environment details

---

## 9. Summary: What to Measure Where

| Benchmark Type | Environment | OS Interaction | Reproducibility |
|----------------|-------------|----------------|-----------------|
| Encryption throughput | Any | None | Perfect |
| Serialization | Any | None | Perfect |
| Protocol state machines | Any | None | Perfect |
| Mock I/O throughput | Linux bare metal | Channels only | High |
| Syscall overhead | Linux bare metal | recv/send | Medium |
| Full stack | Controlled hardware | Everything | Low |

**Key insight**: To truly isolate transport logic from OS noise, use the existing `MockSocket` infrastructure. This gives you **protocol overhead** without **kernel overhead**.

For syscall optimization work specifically, you need Level 2+ benchmarks on controlled hardware, but the improvement targets can be validated with mock I/O first.

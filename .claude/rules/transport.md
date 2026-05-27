---
paths:
  - "crates/core/src/transport/**"
---

# Transport Module Rules

## Cryptographic Rules

### WHEN handling encryption

```
Handshake phase:
  → USE: X25519 key exchange + ChaCha20Poly1305
  → Keys: Ephemeral X25519 keypair per connection

Data phase:
  → USE: AES-128-GCM for all data packets
  → Nonces: Counter-based (4-byte random prefix + 8-byte atomic counter)
  → NEVER reuse (key, nonce) pair
```

### WHEN generating keys

```
Static node keys:
  → Generated once per node lifetime
  → Stored in configuration directory
  → 32-byte X25519 keypair

Session keys:
  → Generated per connection during handshake
  → 16-byte AES-128 keys
  → Independent keys for each direction
  → Cleared on connection close
```

### WHEN handling decryption failures

```
→ Drop packet silently (may be malicious)
→ Log at debug level
→ Do NOT propagate error to caller
→ Do NOT disconnect (could be transient)
```

## Congestion Control Rules

### WHEN using LEDBAT++

```
Key constants (do not change without understanding impact):
  TARGET: 60ms (queuing delay threshold)
  SLOWDOWN_REDUCTION_FACTOR: 4 (cwnd drops to 25%)
  SLOWDOWN_INTERVAL: 9x slowdown duration

MUST:
  - Exit slow start at 75% of TARGET (45ms)
  - Cap multiplicative decrease at -W/2 per RTT
  - Implement periodic slowdown for fairness
```

### WHEN configuring bandwidth

```
Per-connection mode (default):
  → Each connection gets independent limit
  → Default: 10 Mbps (1,250,000 bytes/sec); see DEFAULT_RATE_BYTES_PER_SEC in fixed_rate/controller.rs

Global pool mode:
  → total-bandwidth-limit enables this
  → Formula: rate = max(total/connections, min_per_connection)
  → min_per_connection prevents starvation
```

### WHEN modifying congestion control

```
BEFORE changing LEDBAT++ parameters:
  1. Understand RFC 6817 and draft-irtf-iccrg-ledbat-plus-plus
  2. Test at multiple RTT values (10ms, 50ms, 100ms, 200ms)
  3. Test inter-flow fairness (2+ concurrent connections)
  4. Document reasoning in commit message
```

### Shadow per-peer RTT registry (issue #4074, Phases 1 + 1.5)

`transport/rolling_rtt_stats.rs` maintains a process-wide
`SHADOW_RTT_REGISTRY` (`DashMap<SocketAddr, Arc<dyn RttSnapshotProvider>>`)
populated by `RollingRttStatsHandle` in every `RemoteConnection`. A
1Hz aggregator spawned from `p2p_impl.rs` (registered with
`BackgroundTaskMonitor` as `shadow_rtt_aggregator`) emits a
`shadow_rtt_aggregate` event both as `tracing::debug!` (file-log
mirror; visible via `RUST_LOG=…=debug` in debug builds, compiled
out entirely in release builds via the `release_max_level_info`
feature in `crates/core/Cargo.toml`) and via
`send_standalone_event_with_peer_id` so it reaches the OTLP
collector regardless of log level, tagged with the local node id
so the collector can disaggregate samples per reporting node.

`transport/reference_ping.rs` runs an analogous 1Hz loop
(`reference_ping` background task) that probes a fixed external
target (default `1.1.1.1:53`) over UDP with a synthetic DNS query
and feeds the RTT into a parallel `RollingRttStats`. It emits
`shadow_reference_ping` events with the same shape and the same
local-peer-id tag. The point is to separate "overlay multi-hop
queueing baseline" (visible only in the per-peer signal) from
"local uplink contention" (visible in both signals simultaneously),
which the Phase 1 analysis posted on #4074 showed Phase 1 alone
cannot answer.

```
NEVER read SHADOW_RTT_REGISTRY, cross_connection_median_inflation,
or the reference_ping stats from the production data path (rate
limiter, retry, congestion control). They exist only for the
staged rollout in #4074:
  Phase 1   → observation only — per-peer overlay RTT (current)
  Phase 1.5 → observation only — adds reference-path RTT + peer_id
              tagging so signals can be disaggregated per node and
              the overlay-vs-uplink confound can be tested
  Phase 2   → shadow controller, still no behaviour change
  Phase 3   → opt-in flag
  Phase 4   → default switch only after Phase 3 shows improvement
```

## Connection Lifecycle Rules

### WHEN establishing connections

```
NAT traversal:
  1. Send multiple intro packets — `NAT_TRAVERSAL_MAX_ATTEMPTS` (40 in release builds, 10 under `cfg(test)`) at a 200 ms cadence, capped by a 3 s `overall_deadline` (so ~15 attempts at production settings)
  2. Use exponential backoff: 50ms → 300ms → 1s → 5s
  3. First relay observes external address (ObservedAddress msg)
  4. Rate limit: 1 intro packet/second per source IP
  5. Gateway ramp-up: 5/s for 30s, 20/s for 2min, unlimited after
     (prevents thundering herd after gateway restart)

Restart detection (all peers, not just gateways):
  → If an intro packet arrives on an established connection, attempt
    asymmetric decryption (rate-limited to 1/sec per IP).
  → If valid: the remote peer restarted. Tear down stale session and
    accept a fresh handshake (server-side, same as gateway_connection).
  → This lets restarted peers reconnect via existing NAT holes without
    full gateway re-bootstrap (#3671).
```

### WHEN maintaining connections

```
Keep-alive:
  → Ping every 5 seconds initially
  → After 5 unanswered pings, interval backs off: 10s → 20s → 40s → 60s cap
  → Idle timeout: 120s (RealTime), 24h (VirtualTime/simulation)
  → On idle-timeout closure, per-peer backoff is recorded to prevent
    rapid reconnection cycles to dead peers (#3252)
```

### WHEN closing connections

```
Current behavior (no wire-level close message):
  → Local side: drop_connection_by_addr tears down local state and
    exits the per-connection task. No packet sent to remote.
  → Remote side: detects closure via keepalive timeout (120s idle).
  → SymmetricMessagePayload has no Close/Goodbye variant.

NOTE: The transport has no graceful close protocol. When one side
drops a connection, the remote peer must rely on the 120s idle
timeout to detect it. This is a known limitation (#3545).
```

## Rate Limiting Rules

### WHEN implementing DoS protection

```
Intro packet rate limiting:
  → 1 second minimum between decryption attempts per IP
  → Why: X25519 operations are expensive
  → Cleanup: Expire entries every 60 seconds
```

### WHEN using token bucket

```
Rate calculation:
  final_rate = min(ledbat_rate, global_pool_rate)

Token reservation:
  → Reserve BEFORE sending
  → Block if insufficient tokens
  → RTT-adaptive update interval
```

## Backoff, Jitter, and Recovery Rules

### WHEN implementing retry/backoff

```
All retry/backoff loops MUST apply random jitter:
  → At least ±20% of the interval
  → WHY: Prevents thundering herd after gateway restart (all peers reconnect simultaneously)

Backoff sleeps MUST be interruptible:
  → Use tokio::select! to race sleep against cancellation signal (Notify, CancellationToken)
  → Plain tokio::time::sleep() in retry loop is PROHIBITED unless <1s
  → WHY: Uninterruptible sleeps prevent recovery when conditions change (e.g., isolation cleared)
```

### WHEN node has zero connections

```
MUST have an explicit gateway re-bootstrap path:
  → Cannot depend on routing through existing connections (there are none)
  → Direct gateway contact must be a fallback

WHY: Normal recovery mechanisms (acquire_new) route through existing connections.
With zero connections, the node gets stuck permanently.
```

### WHEN sending critical control messages (ReadyState, etc.)

```
Single fire-and-forget UDP sends for state that affects routing decisions are PROHIBITED.

MUST:
  → Implement retry with backoff for critical control messages
  → Consider optimistic timeout (e.g., treat peer as ready after N seconds even without ACK)

WHY: In lossy environments, a single lost packet can permanently prevent a peer
from being seen as ready. ReadyState bug required re-broadcast every 30s + 60s fallback.
```

## Socket Abstraction Rules

### WHEN writing socket code

```
NEVER use: tokio::net::UdpSocket directly in crates/core
ALWAYS use: Socket trait (crates/core/src/transport.rs)

Why: Enables SimulationSocket for deterministic testing
```

#### Exception: `DefaultSocket` for non-peer-to-peer external probes

There is one documented exception, used by `transport/reference_ping.rs`:
the `DefaultSocket` type alias in `transport.rs` resolves to
`tokio::net::UdpSocket`, but referring to it by the alias keeps the
rule-lint check on the literal `tokio::net::UdpSocket` path satisfied
without re-introducing the raw path. The reference-ping probe uses
`DefaultSocket` deliberately to call the **inherent** `UdpSocket`
methods (NOT the `Socket` trait impl), because the trait's `send_to`
calls `TRANSPORT_METRICS.record_packet_sent` — which would pollute the
per-peer dashboard LRU with the reference target IP (`1.1.1.1:53` by
default), occupying one of the `MAX_TRACKED_PEERS = 256` slots
permanently.

`DefaultSocket` is acceptable ONLY for:
- Out-of-band probes whose target is not a Freenet peer (so the
  `SimulationSocket` substitution is meaningless).
- Code that explicitly needs to skip the trait's metering /
  buffer-tuning / dual-stack setup.

For ALL peer-to-peer transport code paths, continue using the
`Socket` trait so the simulation harness can substitute
`SimulationSocket`. If you are tempted to add another `DefaultSocket`
use site, justify it in a load-bearing comment at the call site and
update this section.

### WHEN testing transport code

```
USE: SimulationSocket::bind(addr).await
This provides:
  - Deterministic packet delivery
  - Fault injection (loss, latency, reordering)
  - Time control via VirtualTime
```

## Streaming Rules

### WHEN handling large payloads

```
Threshold: Check should_use_streaming(payload_size)
Default: 64KB

Streaming path:
  1. Send metadata first (stream_id, total_size)
  2. Fragment into chunks
  3. Send chunks with sequence numbers
  4. Reassemble at receiver
```

### WHEN implementing fragment reassembly

```
USE: LockFreeStreamBuffer (OnceLock<Bytes> array)
  - contiguous_fragments: AtomicU32 for frontier tracking
  - Lock-free insertion
  - Ordered delivery to consumer
```

## Testing Checklist

```
□ Test encryption round-trip
□ Test tampering detection (modify encrypted packet)
□ Test wrong key rejection
□ Test NAT traversal (SimNetwork with NAT config)
□ Test keep-alive and timeout
□ Test congestion control at various RTTs
□ Test streaming with packet loss
```

## Pitfalls to Avoid

```
DON'T: Use std::time for timeouts in production code
WHY: Breaks deterministic simulation testing
USE: TimeSource trait

DON'T: Assume packets arrive in order
WHY: UDP provides no ordering guarantees
HANDLE: Reordering, duplicates, loss

DON'T: Trust packet contents before decryption
WHY: Attacker can forge UDP packets
VERIFY: Decrypt and validate AEAD tag first

DON'T: Block on socket operations
WHY: Deadlocks event loop
USE: Async socket operations with timeout
```

## Documentation

- Architecture: `docs/architecture/transport/README.md`
- Security: `docs/architecture/transport/security.md`
- Connection lifecycle: `docs/architecture/transport/connection-lifecycle.md`
- LEDBAT++: `docs/architecture/transport/design/ledbat-plus-plus.md`
- Bandwidth config: `docs/architecture/transport/configuration/bandwidth-configuration.md`

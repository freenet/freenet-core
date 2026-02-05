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
  → Default: 10 MB/s

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

## Connection Lifecycle Rules

### WHEN establishing connections

```
NAT traversal:
  1. Send multiple intro packets (up to 10 over ~3s)
  2. Use exponential backoff: 50ms → 300ms → 1s → 5s
  3. First relay observes external address (ObservedAddress msg)
  4. Rate limit: 1 intro packet/second per source IP
```

### WHEN maintaining connections

```
Keep-alive:
  → Ping every 5 seconds if no traffic
  → 5 consecutive missed pings → connection dead
  → Idle timeout: 120s (RealTime), 24h (VirtualTime/simulation)
```

### WHEN closing connections

```
Graceful:
  1. Send final packets with delivery confirmation
  2. Wait for ACK (timeout: 5 seconds)
  3. Send explicit close message
  4. Clean up state

Timeout-based:
  → No traffic for idle timeout
  → Mark CLOSING, stop sending, cleanup after grace period
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

## Socket Abstraction Rules

### WHEN writing socket code

```
NEVER use: tokio::net::UdpSocket directly in crates/core
ALWAYS use: Socket trait (crates/core/src/transport/mod.rs)

Why: Enables SimulationSocket for deterministic testing
```

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
□ Use --test-threads=1 for determinism
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

# Transport Layer Documentation Gaps - Analysis Report

**Generated:** 2026-01-19
**Purpose:** Comprehensive analysis of missing documentation vs. implemented features

---

## Executive Summary

The Freenet transport layer has **sophisticated, production-ready implementation** but **severely incomplete documentation**. This gap analysis identifies critical missing documentation that blocks users, operators, and security auditors.

### Severity Classification

- **🔴 CRITICAL:** Blocks users, incorrect/outdated information, security concerns
- **🟡 HIGH:** Limits adoption, makes debugging difficult
- **🟢 MEDIUM:** Nice-to-have, improves understanding

---

## 🔴 CRITICAL Gaps

### 1. `ledbat_min_ssthresh` Parameter UNDOCUMENTED

**Status:** USER-FACING PARAMETER WITH ZERO DOCUMENTATION

**Location:** Available via:
- CLI: `--ledbat-min-ssthresh <bytes>`
- TOML: `[network-api] ledbat-min-ssthresh = 524288`

**Current State:**
- ❌ Not mentioned in `bandwidth-configuration.md`
- ❌ Not in README.md
- ✅ Exists in code: `crates/core/src/config/mod.rs:805-824`

**Why Critical:**
- Solves major production issue (#2578: timeout storms on high-latency paths)
- Without documentation, users can't fix slow intercontinental transfers
- Default value (~5.7KB) causes poor performance on >100ms RTT links

**Recommended Documentation:**

```markdown
### LEDBAT Tuning Parameters

#### `--ledbat-min-ssthresh` (CRITICAL for High-Latency Paths)

**Purpose:** Set minimum slow-start threshold floor for LEDBAT++

**Default:** None (uses spec-compliant ~5.7KB floor)

**When to Configure:**
- Intercontinental connections (>100ms RTT): 100KB-500KB
- Satellite links (>500ms RTT): 500KB-2MB
- Issue: Default causes aggressive cwnd reduction on timeout

**Calculation:**
```
min_ssthresh = BDP / 2
where BDP = bandwidth × RTT
```

**Example:**
- 10 MB/s link, 150ms RTT:
  - BDP = 10,000,000 × 0.15 = 1,500,000 bytes
  - min_ssthresh = 750,000 bytes (750KB)

**CLI:**
```bash
freenet --ledbat-min-ssthresh 524288  # 500KB
```

**TOML:**
```toml
[network-api]
ledbat-min-ssthresh = 524288  # 500KB
```

**Reference:** PR #2578, Issue #2578
```

---

### 2. Encryption Architecture OUTDATED/MISSING

**Status:** README SAYS "RSA" BUT CODE USES X25519

**Current State:**
- ❌ README.md:6 says "RSA key exchange" (OUTDATED)
- ❌ No documentation of X25519+ChaCha20Poly1305 intro packets
- ❌ No documentation of AES-128-GCM symmetric encryption
- ❌ No security threat model
- ✅ Implementation: `crates/core/src/transport/crypto.rs`

**What's Implemented:**
1. **Intro Phase:** X25519 static-ephemeral key exchange + ChaCha20Poly1305 AEAD
2. **Symmetric Phase:** AES-128-GCM for ongoing traffic
3. **Packet Discrimination:** Type byte (0x01 = intro, 0x02 = symmetric)

**Why Critical:**
- Misleading security information
- Security auditors cannot evaluate the system
- Migration from RSA to X25519 not documented (when/why?)

**Recommended:** Create `docs/architecture/transport/security.md`

---

### 3. Connection Lifecycle MISSING

**Status:** NO DIAGRAMS, NAT TRAVERSAL UNEXPLAINED

**Current State:**
- ❌ No connection state diagram
- ❌ No handshake sequence diagram
- ❌ NAT traversal strategy not documented
- ❌ Expected inbound mechanism not explained
- ✅ Implementation: `crates/core/src/transport/connection_handler.rs:1473-1900`

**What's Implemented:**
- Aggressive packet sending (10 attempts over 3 seconds for NAT hole-punching)
- Expected inbound tracking for NAT traversal
- Exponential backoff (50ms → 5s)
- DoS rate limiting (1s minimum per IP)

**Why Critical:**
- Users can't debug connection failures
- No guidance on firewall/NAT configuration
- Gateway operators don't understand connection lifecycle

**Recommended:** Create `docs/architecture/transport/connection-lifecycle.md`

---

### 4. Wire Protocol UNDOCUMENTED

**Status:** NO FORMAL PACKET FORMAT SPECIFICATION

**Current State:**
- ❌ No packet structure diagrams
- ❌ Message types not cataloged
- ❌ Fragmentation strategy not explained
- ❌ ACK timing semantics not specified
- ✅ Implementation scattered across multiple files

**What's Implemented:**

**Intro Packet (0x01):**
```
[type:1][ephemeral_pub:32][ciphertext:variable][tag:16]
Total: 73 bytes typical
```

**Symmetric Packet (0x02):**
```
[type:1][nonce:12][ciphertext:variable][tag:16]
Max plaintext: 1463 bytes
```

**Message Types:**
- `AckConnection` - Handshake completion
- `ShortMessage` - Single-packet messages (<1.4KB)
- `StreamFragment` - Multi-packet transfers
- `NoOp` - ACK-only packets
- `Ping`/`Pong` - Liveness detection

**Why Critical:**
- Impossible to build interoperable implementations
- Protocol evolution strategy unclear
- Version negotiation not documented

**Recommended:** Create `docs/architecture/transport/wire-protocol.md`

---

## 🟡 HIGH Priority Gaps

### 5. Metrics & Monitoring Guide MISSING

**Status:** RICH TELEMETRY EXISTS BUT UNDISCOVERABLE

**Current State:**
- ❌ No documentation on accessing metrics
- ❌ OTLP telemetry integration not mentioned
- ❌ No health monitoring guidance
- ✅ Implementation: `crates/core/src/transport/metrics.rs`

**What's Available:**
- Global metrics: `TRANSPORT_METRICS.take_snapshot()`
- Per-transfer events: Started/Completed/Failed
- Algorithm stats: `controller.stats()`
- OTLP telemetry integration

**Why High Priority:**
- Operators can't monitor transport health
- No guidance on alert thresholds
- Debugging requires reading source code

**Recommended:** Create `docs/architecture/transport/monitoring.md`

---

### 6. BBR Startup Rate Undocumented

**Status:** USER-FACING PARAMETER NOT IN DOCS

**Current State:**
- ❌ Not documented in bandwidth-configuration.md
- ✅ Available via `--bbr-startup-rate <bytes/sec>`
- ✅ Default: 25 MB/s

**Why High Priority:**
- Critical for CI/virtualized environments
- Can cause "death spiral" if too high

**Recommended:** Add to bandwidth-configuration.md

---

### 7. Reliability Mechanisms UNDOCUMENTED

**Status:** RFC-COMPLIANT IMPLEMENTATION NOT DOCUMENTED

**Current State:**
- ❌ TLP (Tail Loss Probe) not mentioned
- ❌ RTO calculation not documented
- ❌ Karn's algorithm not explained
- ❌ ACK batching strategy not specified
- ✅ Implementation: RFC 6298 + RFC 8985 TLP

**What's Implemented:**
- Selective ACK with piggybacking
- TLP at 2×SRTT (speculative, no backoff)
- RTO with exponential backoff (min 500ms, max 60s)
- 100ms ACK batching delay
- 20-packet buffer before forced ACK

**Why High Priority:**
- Developers can't understand reliability guarantees
- Performance tuning impossible without understanding
- Debugging timeout issues requires this knowledge

**Recommended:** Create `docs/architecture/transport/reliability.md`

---

## 🟢 MEDIUM Priority Gaps

### 8. Keep-Alive and Liveness Detection

**Implementation:**
- 5-second ping interval
- 5 unanswered pings = connection dead
- Bidirectional detection (Ping/Pong)
- 120s idle timeout (RealTime), 24h (VirtualTime)

**Location:** `peer_connection.rs:243-384`

### 9. Fast Channel Implementation

**Implementation:**
- 2x faster than tokio::mpsc (2.88 vs 1.33 Melem/s)
- Hybrid crossbeam + tokio::Notify
- Bounded channels for backpressure

**Location:** `fast_channel.rs:1-314`

### 10. Packet Tracking Details

**Implementation:**
- SentPacketTracker with RFC 6298 RTT estimation
- ReceivedPacketTracker with 20-packet buffering
- Karn's algorithm for retransmission RTT exclusion

**Location:** `sent_packet_tracker.rs`, `received_packet_tracker.rs`

---

## Priority Actions

### Immediate (This Week)

1. ✅ **Document `ledbat_min_ssthresh`** in bandwidth-configuration.md
2. ✅ **Fix RSA→X25519 reference** in README.md
3. ✅ **Create security.md** with encryption architecture
4. ✅ **Create connection-lifecycle.md** with handshake diagrams

### Short-term (This Month)

5. Create wire-protocol.md with packet formats
6. Create monitoring.md with metrics guide
7. Create reliability.md with ACK/RTO documentation
8. Add BBR startup rate to bandwidth-configuration.md

### Medium-term (This Quarter)

9. Expand README architecture diagram to show all components
10. Add troubleshooting section with common issues
11. Create tuning guide for different network profiles
12. Document internal components (fast channel, packet tracking)

---

## Code Reference Summary

| Feature | Documentation Status | Code Location |
|---------|---------------------|---------------|
| ledbat_min_ssthresh | 🔴 Missing | `config/mod.rs:805-824` |
| X25519 encryption | 🔴 Outdated (says RSA) | `crypto.rs:131-165` |
| Connection lifecycle | 🔴 Missing | `connection_handler.rs:1473-1900` |
| Wire protocol | 🔴 Missing | `packet_data.rs`, `symmetric_message.rs` |
| Metrics API | 🟡 Undiscoverable | `metrics.rs:228` |
| BBR startup rate | 🟡 Not in docs | `config/mod.rs:838-846` |
| Reliability (TLP/RTO) | 🟡 Not explained | `sent_packet_tracker.rs:368-468` |
| Keep-alive | 🟢 Minor | `peer_connection.rs:243-384` |
| Fast channel | 🟢 Minor | `fast_channel.rs:1-314` |

---

## Impact Assessment

### User Impact

**High-Latency Users (🔴 Critical):**
- Cannot configure `ledbat_min_ssthresh` → poor performance
- No tuning guidance for intercontinental links

**Gateway Operators (🟡 High):**
- Cannot monitor transport health
- No guidance on connection management
- Debugging connection issues is guesswork

**Security Auditors (🔴 Critical):**
- Outdated security information (RSA vs X25519)
- No threat model available
- Cannot evaluate security properties

**Protocol Developers (🔴 Critical):**
- Cannot build interoperable implementations
- Wire protocol not specified

### Developer Impact

**Contributing Developers (🟡 High):**
- Steep learning curve (must read source code)
- No architecture documentation
- Hard to understand reliability guarantees

**Integration Developers (🔴 Critical):**
- Cannot integrate with transport layer
- Metrics API undiscoverable
- Configuration options not documented

---

## Recommended Documentation Structure

```
docs/architecture/transport/
├── README.md (update with overview)
├── security.md (NEW - encryption, threat model)
├── connection-lifecycle.md (NEW - handshake, NAT)
├── wire-protocol.md (NEW - packet formats)
├── reliability.md (NEW - ACK, RTO, TLP)
├── monitoring.md (NEW - metrics, telemetry)
├── configuration/
│   └── bandwidth-configuration.md (UPDATE - add ledbat_min_ssthresh, bbr_startup_rate)
├── design/
│   ├── ledbat-plus-plus.md (existing)
│   ├── ledbat-slow-start.md (existing)
│   ├── streaming-infrastructure.md (existing)
│   └── global-bandwidth-pool.md (existing)
└── benchmarking/
    └── ... (existing)
```

---

## Conclusion

The Freenet transport layer is **production-ready with excellent implementation** but suffers from **critical documentation gaps** that:

1. **Block users** from configuring high-latency paths
2. **Prevent security audits** due to outdated/missing security docs
3. **Make debugging impossible** without reading source code
4. **Prevent interoperability** due to missing wire protocol spec

**Priority 1 (Critical):** Document user-facing parameters (`ledbat_min_ssthresh`) and fix outdated security information

**Priority 2 (High):** Create monitoring guide and reliability documentation

**Priority 3 (Medium):** Document internal components and create comprehensive tuning guide

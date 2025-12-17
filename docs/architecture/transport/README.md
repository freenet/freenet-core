# Freenet Transport Layer Documentation

## Overview

The Freenet transport layer provides low-level network communication over UDP with:
- **Encryption**: AES-128-GCM symmetric encryption after RSA key exchange
- **Reliability**: Packet acknowledgment and retransmission
- **Congestion Control**: LEDBAT (Low Extra Delay Background Transport) with slow start
- **Rate Limiting**: Token bucket for smooth packet pacing
- **RTT Tracking**: RFC 6298-compliant RTT estimation

## Implementation Status

| Component | Status | Week Completed |
|-----------|--------|----------------|
| RTT Estimation (RFC 6298) | ✅ Complete | Week 1 |
| Token Bucket Rate Limiter | ✅ Complete | Week 2 |
| LEDBAT Controller (RFC 6817) | ✅ Complete | Week 3 |
| Slow Start (IW26) | ✅ Complete | Week 4 |
| cwnd Enforcement | ✅ Complete | Week 4 |
| Bandwidth Configuration | ✅ Complete | Week 4 |
| Global Bandwidth Pool | 📋 Planned | Phase 2 |

## Document Map

### Design Documents
How the transport layer is designed and why.

- **[LEDBAT Slow Start Design](design/ledbat-slow-start.md)** - Congestion control algorithm design, slow start mechanics, and integration architecture

### Analysis & Results
Performance analysis and empirical results.

- **[Performance Analysis](analysis/performance-analysis.md)** - Deep dive into syscall overhead, channel bottlenecks, serialization costs, and experimental benchmarks
- **[Performance Comparison](analysis/performance-comparison.md)** - Week 0 (baseline) vs Week 4 (current) performance comparison, bottleneck discovery, real-world expectations

### Configuration
How to configure the transport layer.

- **[Bandwidth Configuration](configuration/bandwidth-configuration.md)** - Configuring per-connection bandwidth limits, token bucket discovery, implications for throughput goals

### Benchmarking
How to measure and validate transport performance.

- **[Benchmark Methodology](benchmarking/methodology.md)** - Benchmark levels (0-3), noise reduction strategies, running and interpreting results

### Historical Reference
Historical baselines and validation results from development.

- **[Baseline (Week 0)](historical/baseline-week0.md)** - Performance metrics before congestion control implementation (pre-LEDBAT baseline)
- **[RTT Validation (Week 1)](historical/rtt-validation-week1.md)** - RFC 6298 RTT implementation validation

### Future Work
Planned enhancements not yet implemented.

- **[Global Bandwidth Pool](future/global-bandwidth-pool.md)** - ⚠️ **NOT IMPLEMENTED** - Design for total bandwidth management across all connections (Phase 2)

## Quick Reference

### Default Configuration
- **Per-connection bandwidth limit**: 10 MB/s (configurable)
- **Initial window (IW26)**: 38 KB (26 × 1,464-byte MSS)
- **Slow start threshold**: 100 KB
- **Maximum cwnd**: 1 GB
- **Token bucket capacity**: 10 KB burst

### Key Performance Metrics
- **>3 MB/s goal**: ✅ Achieved (10 MB/s default, 3.3x target)
- **Cold start ramp-up**: ~300ms to reach 300KB cwnd @ 100ms RTT
- **Steady-state throughput**: Up to 10 MB/s (token bucket limit, not LEDBAT)
- **Real-world bottleneck**: Token bucket (10 MB/s), not LEDBAT cwnd (1 GB max)

### Running Benchmarks
```bash
# CI-friendly subset (fast, deterministic)
cargo bench --bench transport_perf -- level0
cargo bench --bench transport_perf -- transport

# LEDBAT validation (manual, slower)
cargo bench --bench transport_perf -- ledbat_validation
cargo bench --bench transport_perf -- slow_start

# See benchmarking/methodology.md for details
```

### Source Code Locations
- **Transport module**: `crates/core/src/transport/`
- **LEDBAT controller**: `crates/core/src/transport/ledbat.rs`
- **Token bucket**: `crates/core/src/transport/token_bucket.rs`
- **RTT tracking**: `crates/core/src/transport/sent_packet_tracker.rs`
- **Connection handling**: `crates/core/src/transport/connection_handler.rs`
- **Stream sending**: `crates/core/src/transport/peer_connection/outbound_stream.rs`

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                     Application Layer                       │
│              (PeerConnection::send/recv)                    │
└────────────────────┬────────────────────────────────────────┘
                     │
        ┌────────────▼────────────┐
        │   Outbound Stream       │
        │  (Fragment & Queue)     │
        └────┬───────────┬────────┘
             │           │
    ┌────────▼───┐  ┌───▼──────────┐
    │   LEDBAT   │  │ Token Bucket │
    │   (cwnd)   │  │ (rate limit) │
    └────┬───────┘  └───┬──────────┘
         │              │
         └──────┬───────┘
                │
      ┌─────────▼──────────┐
      │  Packet Assembly   │
      │  (Serialize +      │
      │   Encrypt)         │
      └─────────┬──────────┘
                │
      ┌─────────▼──────────┐
      │    UDP Socket      │
      │  (sendto/recvfrom) │
      └────────────────────┘
```

## Flow Control

**Send Path:**
1. Application calls `PeerConnection::send(message)`
2. **cwnd check**: Wait if `flightsize + packet_size > cwnd` (LEDBAT enforcement)
3. **Token reservation**: Reserve tokens, wait if needed (rate limiting)
4. **Packet assembly**: Serialize + encrypt
5. **Send tracking**: Record packet for RTT measurement and retransmission
6. **LEDBAT update**: Increment flightsize

**Receive Path:**
1. UDP socket receives packet
2. Decrypt + deserialize
3. **RTT update**: If ACK, update smoothed RTT and cwnd (LEDBAT feedback)
4. **Flightsize update**: Decrement flightsize on ACK
5. Deliver to application

## Development History

The transport layer was developed over 4 weeks:
- **Week 0**: Baseline with hardcoded 3 MB/s rate limit (no congestion control)
- **Week 1**: RTT estimation (RFC 6298) with Karn's algorithm
- **Week 2**: Token bucket rate limiter with dynamic rate updates
- **Week 3**: LEDBAT controller (RFC 6817) with delay-based congestion control
- **Week 4**: Slow start (IW26), cwnd enforcement, bandwidth configuration

See [Performance Comparison](analysis/performance-comparison.md) for detailed before/after analysis.

## Related Documentation

- **Main architecture doc**: `docs/architecture/README.md` (if exists)
- **RFCs**:
  - [RFC 6298: Computing TCP's Retransmission Timer](https://tools.ietf.org/html/rfc6298)
  - [RFC 6817: Low Extra Delay Background Transport (LEDBAT)](https://tools.ietf.org/html/rfc6817)
- **API docs**: Run `cargo doc --open` and navigate to `freenet::transport`

## Questions or Issues?

- **Code questions**: See inline documentation in `crates/core/src/transport/`
- **Performance issues**: Start with [Performance Analysis](analysis/performance-analysis.md)
- **Configuration**: See [Bandwidth Configuration](configuration/bandwidth-configuration.md)
- **Benchmarking**: See [Benchmark Methodology](benchmarking/methodology.md)

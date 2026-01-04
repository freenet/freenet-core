# Transport Bandwidth Configuration

## Overview

The Freenet transport layer supports two bandwidth configuration modes:

1. **Per-connection mode** (default): Each connection gets an independent bandwidth limit
2. **Global pool mode**: Total bandwidth is shared fairly across all connections

## Quick Start

### Per-Connection Mode (Default)

```bash
# Each connection can use up to 10 MB/s (default)
freenet

# Custom per-connection limit
freenet --bandwidth-limit 5000000  # 5 MB/s per connection
```

### Global Pool Mode (Recommended for bandwidth-constrained environments)

```bash
# Share 50 MB/s total across all connections
freenet --total-bandwidth-limit 50000000 --min-bandwidth-per-connection 1000000
```

## Configuration Options

### CLI Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `--bandwidth-limit <bytes/sec>` | Per-connection bandwidth limit | 10,000,000 (10 MB/s) |
| `--total-bandwidth-limit <bytes/sec>` | Total bandwidth across ALL connections | None (disabled) |
| `--min-bandwidth-per-connection <bytes/sec>` | Minimum per-connection rate (prevents starvation) | 1,000,000 (1 MB/s) |

### Config File (TOML)

```toml
[network-api]
# Per-connection mode (traditional)
bandwidth-limit = 10000000  # 10 MB/s per connection

# OR Global pool mode (recommended)
total-bandwidth-limit = 50000000           # 50 MB/s total
min-bandwidth-per-connection = 1000000     # 1 MB/s minimum
```

**Note**: If `total-bandwidth-limit` is set, it overrides `bandwidth-limit`.

## Global Pool Mode Details

When `total-bandwidth-limit` is configured, bandwidth is distributed using this formula:

```
per_connection_rate = max(total_limit / active_connections, min_per_connection)
```

### Example: 1 Gbps Connection

For a user with 1 Gbps (125 MB/s) wanting Freenet to use 80% of available bandwidth:

```toml
[network-api]
total-bandwidth-limit = 100000000          # 100 MB/s total
min-bandwidth-per-connection = 2000000     # 2 MB/s minimum
```

| Active Connections | Per-Connection Rate | Total Usage |
|-------------------|---------------------|-------------|
| 1 | 100 MB/s | 100 MB/s |
| 5 | 20 MB/s | 100 MB/s |
| 10 | 10 MB/s | 100 MB/s |
| 25 | 4 MB/s | 100 MB/s |
| 50 | 2 MB/s (min enforced) | 100 MB/s |
| 100 | 2 MB/s (min enforced) | 200 MB/s* |

*When `min × connections > total`, the minimum is honored to prevent connection starvation.

### Example: Home DSL Connection

For a user with 50 Mbps (6.25 MB/s) upload wanting conservative bandwidth usage:

```toml
[network-api]
total-bandwidth-limit = 3000000            # 3 MB/s total (~50% of upload)
min-bandwidth-per-connection = 500000      # 500 KB/s minimum
```

## How It Integrates with LEDBAT

The global bandwidth pool works alongside LEDBAT congestion control:

```
final_rate = min(ledbat_rate, global_pool_rate)
```

- **LEDBAT** measures network conditions and adjusts rate to avoid congestion
- **Global pool** enforces your bandwidth budget across all connections
- The **minimum** of both rates is used

This means:
- If the network is congested, LEDBAT will reduce the rate below the global limit
- If the network is clear, the global limit caps bandwidth to your configured total
- LEDBAT's delay-based feedback is never confused by artificial throttling

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    PeerConnection                           │
│                                                             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │    LEDBAT    │    │   Global     │    │  TokenBucket │  │
│  │  Controller  │    │  Bandwidth   │    │   (Pacer)    │  │
│  │              │    │   Manager    │    │              │  │
│  │ Measures RTT │    │ Counts conns │    │ Paces packets│  │
│  │ & queuing    │    │ Divides fair │    │ at final     │  │
│  │ delay        │    │ share        │    │ rate         │  │
│  └──────┬───────┘    └──────┬───────┘    └──────▲───────┘  │
│         │                   │                   │          │
│         │   ledbat_rate     │   global_rate     │          │
│         └─────────┬─────────┘                   │          │
│                   │                             │          │
│                   ▼                             │          │
│            ┌──────────────┐                     │          │
│            │  min(a, b)   │─────────────────────┘          │
│            └──────────────┘     final_rate                 │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Implementation Details

### Token Bucket

The transport uses a token bucket for smooth packet pacing:

| Parameter | Value |
|-----------|-------|
| Bucket capacity | 10 KB burst |
| Rate | Dynamically adjusted (LEDBAT + global pool) |
| Update frequency | RTT-adaptive (50-500ms) |

### Rate Update Timing

Rates are recalculated on an RTT-adaptive schedule:

| RTT Range | Update Interval |
|-----------|-----------------|
| < 10ms | ~50ms |
| 10-100ms | ~100-200ms |
| > 100ms | ~500ms |

### Connection Lifecycle

1. **Connection established**: `GlobalBandwidthManager::register_connection()` called
2. **During transfer**: Rate updated periodically via `current_per_connection_rate()`
3. **Connection closed**: `RemoteConnection::drop()` calls `unregister_connection()`

## Backward Compatibility

The default behavior (no `total-bandwidth-limit`) is unchanged:
- Each connection gets an independent 10 MB/s limit
- N connections can use up to N × 10 MB/s total

## Recommendations

### For Gateway Operators

```toml
[network-api]
# Dedicate significant bandwidth to serving the network
total-bandwidth-limit = 100000000          # 100 MB/s
min-bandwidth-per-connection = 1000000     # 1 MB/s minimum
```

### For Regular Peers

```toml
[network-api]
# Conservative settings for residential connections
total-bandwidth-limit = 10000000           # 10 MB/s total
min-bandwidth-per-connection = 500000      # 500 KB/s minimum
```

### For Testing/Development

```toml
[network-api]
# Higher limits for local testing
bandwidth-limit = 100000000                # 100 MB/s per connection
# OR
total-bandwidth-limit = 500000000          # 500 MB/s total
```

## LEDBAT Configuration for High-Latency Paths

### The ssthresh Death Spiral Problem (Issue #2578)

On high-latency paths (>100ms RTT, e.g., US-EU intercontinental), repeated packet
timeouts can cause the slow start threshold (ssthresh) to collapse:

```
After 8 timeouts: ssthresh drops from 1MB → 5KB
Result: Slow start exits immediately, throughput limited to ~300 Kbit/s
```

This manifests as:
- Transfers that should take 2-3 seconds taking 18+ seconds
- Throughput dropping to <500 Kbit/s despite available bandwidth

### Solution: Configure min_ssthresh

Set a minimum floor for ssthresh in `LedbatConfig`:

```rust
LedbatConfig {
    min_ssthresh: Some(100 * 1024),  // 100KB floor
    ..Default::default()
}
```

### Recommended min_ssthresh by Network Type

| Network Type | RTT | min_ssthresh | Example Use Case |
|--------------|-----|--------------|------------------|
| LAN | <10ms | `None` | Local testing |
| Regional | 10-50ms | `None` | Same-country peers |
| Continental | 50-100ms | `Some(50KB)` | Cross-country |
| **Intercontinental** | 100-200ms | **`Some(100KB-500KB)`** | **US-EU, US-Asia** |
| Satellite | 500ms+ | `Some(500KB-2MB)` | Remote/satellite links |

### Gateway Operators

For gateways serving global traffic, configure a higher floor:

```rust
LedbatConfig {
    min_ssthresh: Some(500 * 1024),  // 500KB - good for intercontinental
    ..Default::default()
}
```

---

## Troubleshooting

### Slow transfers despite high bandwidth limit

1. Check LEDBAT is not detecting congestion (high queuing delay)
2. Verify the receiving peer has sufficient bandwidth
3. Check if too many connections are sharing the global pool
4. **For high-latency paths (>100ms RTT):** Configure `min_ssthresh` (see above)

### Connections getting starved

Increase `min-bandwidth-per-connection`:
```bash
freenet --total-bandwidth-limit 50000000 --min-bandwidth-per-connection 2000000
```

### Total bandwidth exceeding limit

This happens when `min × connections > total`. Either:
- Reduce `min-bandwidth-per-connection`
- Increase `total-bandwidth-limit`
- Accept the overage (connections won't starve)

## Source Code

- **GlobalBandwidthManager**: `crates/core/src/transport/global_bandwidth.rs`
- **TokenBucket**: `crates/core/src/transport/token_bucket.rs`
- **LEDBAT**: `crates/core/src/transport/ledbat.rs`
- **Configuration**: `crates/core/src/config/mod.rs`

## References

- [Global Bandwidth Pool Design](../future/global-bandwidth-pool.md)
- [LEDBAT Slow Start Design](../design/ledbat-slow-start.md)
- [RFC 6817: LEDBAT](https://tools.ietf.org/html/rfc6817)

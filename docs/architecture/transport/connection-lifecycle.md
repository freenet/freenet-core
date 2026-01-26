# Connection Lifecycle

## Overview

This document describes the high-level connection lifecycle in the Freenet transport layer, from initial connection establishment through NAT traversal to graceful shutdown.

## Connection States

```
┌─────────────┐
│   INITIAL   │  No connection exists
└──────┬──────┘
       │ connect()
       ▼
┌─────────────┐
│ CONNECTING  │  Intro packets being sent, waiting for ACK
└──────┬──────┘
       │ ACK received + key exchange complete
       ▼
┌─────────────┐
│   ACTIVE    │  Bidirectional encrypted communication
└──────┬──────┘
       │ timeout, error, or explicit close()
       ▼
┌─────────────┐
│   CLOSED    │  Connection terminated, cleanup complete
└─────────────┘
```

## Outbound Connection Flow

### 1. Connection Initiation

**Trigger:** Node wants to connect to a peer (gateway join, peer discovery, message forwarding)

**Process:**
1. Lookup peer's public key from network information
2. Generate symmetric encryption key for this connection
3. Create intro packet containing protocol version + our symmetric key
4. Enter CONNECTING state

### 2. NAT Traversal

**Challenge:** Both nodes may be behind NAT/firewall

**Strategy:** Aggressive packet sending to establish NAT hole-punch

```
Node A (behind NAT)                         Node B (gateway, expected inbound)
      |                                              |
      | ─────────── intro packet 1 ──────────────> | (May be dropped by NAT)
      | ─────────── intro packet 2 ──────────────> | (May be dropped by NAT)
      | ─────────── intro packet 3 ──────────────> | ✓ NAT hole established!
      |                                              | (Decrypts, validates version)
      | <──────────── ACK (encrypted) ───────────── | (Contains B's symmetric key)
      | ✓ Connection established                    |
```

**Parameters:**
- 10 attempts over ~3 seconds
- Exponential backoff: 50ms → 300ms → 1s → 5s
- Rate limiting: Max 1 intro packet per second per source IP (DoS protection)

**Why Aggressive:** UDP is stateless, and NAT devices need to see outbound traffic before allowing inbound. Multiple rapid attempts increase probability of successful hole-punch.

**Code Reference:** `crates/core/src/transport/connection_handler.rs:1473-1900` (traverse_nat)

### 3. Handshake Completion

**Steps:**
1. Receive and decrypt ACK packet (contains remote peer's symmetric key)
2. Send confirmation packet (proves we received the ACK)
3. Transition to ACTIVE state
4. Begin keep-alive ping/pong cycle

**Security Check:** Protocol version must match exactly, otherwise connection rejected with version mismatch error.

## Inbound Connection Flow

### 1. Intro Packet Reception

**Trigger:** Unexpected packet arrives from unknown source

**Gateway Behavior:**
1. Check if packet type is intro (0x01)
2. Rate limit: Enforce 1 second minimum between intro packets from same IP
3. Decrypt intro packet with local static secret key
4. Extract protocol version and remote's symmetric key
5. Validate protocol version

**Why Rate Limiting:** Intro packet decryption (X25519 operations) is expensive. Rate limiting prevents crypto exhaustion DoS attacks.

### 2. Response and Key Exchange

**Process:**
1. Generate our own symmetric key for this connection
2. Encrypt ACK packet with remote's key (proves we decrypted their intro)
3. Send ACK packet
4. Wait for confirmation packet
5. Transition to ACTIVE state

**Expected Inbound Mechanism:**

For NAT traversal to work, the gateway must send traffic back immediately:

```
Peer (behind NAT)                           Gateway (public)
      |                                              |
      | ──────────── intro packet ───────────────> | ✓ Arrives at gateway
      |                                              | (Gateway's NAT table now has entry)
      | <────────────── ACK ──────────────────────  | ✓ Can reply through NAT hole
```

The gateway's response packet creates the "expected inbound" entry in the peer's NAT table, allowing bidirectional communication.

**Code Reference:** `crates/core/src/transport/connection_handler.rs:1282-1471` (gateway_connection)

## Active Connection

### Keep-Alive and Liveness Detection

**Purpose:** Detect dead connections and prevent NAT timeout

**Mechanism:**
- Ping packet sent every 5 seconds if no other traffic
- Pong response expected within reasonable time
- 5 consecutive unanswered pings → connection considered dead
- Bidirectional detection (both sides ping/pong)

**Idle Timeout:**
- RealTime: 120 seconds of complete silence → close
- VirtualTime (simulation): 24 hours (to avoid premature closure during time advancement)

**Why Different Timeouts:** Simulation tests can advance time rapidly, causing premature connection closure if using production timeout. VirtualTime uses longer timeout to accommodate testing scenarios.

### Message Exchange

**Normal Traffic:**
1. Application data serialized
2. Encrypted with AES-128-GCM + connection-specific key
3. Sent as UDP packet
4. ACK/retransmission handled by reliability layer
5. Received packets decrypted and delivered to application

**Flow Control:**
- LEDBAT++ congestion control adjusts sending rate
- Token bucket enforces configured bandwidth limits
- Backpressure from slow receiver handled by retry logic

## Connection Termination

### Graceful Shutdown

**Explicit Close:**
1. Send final packets with delivery confirmation
2. Wait for ACK (timeout: 5 seconds)
3. Send explicit close message
4. Clean up connection state
5. Remove from connection registry

### Timeout-Based Close

**Triggers:**
- No traffic for idle timeout period (120s for RealTime)
- 5 consecutive failed keep-alive pings
- Repeated send failures (connection likely dead)

**Process:**
1. Mark connection as CLOSING
2. Stop sending new packets
3. Clean up state after grace period
4. Notify application layer of disconnection

### Identity Change Detection

**Scenario:** Peer restarts and generates new identity keys, but reconnects from same IP

**Detection:**
1. Receive intro packet from known IP but with different public key
2. Recognize IP mismatch with expected identity
3. Close old connection
4. Accept new connection with new identity

**Purpose:** Allows nodes to restart and rejoin network without manual intervention, while maintaining security (new keys = new identity).

**Code Reference:** `crates/core/src/transport/connection_handler.rs:752-760`

## Error Handling

### Connection Failures

| Error | Cause | Recovery |
|-------|-------|----------|
| Version mismatch | Incompatible protocol versions | Reject connection, log for upgrade |
| Decryption failure | Wrong key or corrupted packet | Drop packet, continue (may indicate MITM) |
| Timeout during handshake | Network issues or peer offline | Retry with backoff, eventually give up |
| NAT traversal failure | Symmetric NAT or firewall | Fall back to relay (if available) |
| Rate limit hit | Too many connection attempts | Slow down, wait for rate limit reset |

### Recovery Strategies

**Transient Failures:**
- Packet loss: Retransmit after timeout
- Keep-alive timeout: Send ping
, wait for pong
- Brief disconnection: Reconnect with exponential backoff

**Permanent Failures:**
- Version mismatch: Requires software upgrade
- Identity change: Accept as new connection
- Persistent send failures: Close and remove peer from active set

## NAT Types and Compatibility

### NAT Classification

**Full Cone (Easy):**
- Any external host can send to internal IP once outbound packet sent
- Excellent compatibility

**Port-Restricted Cone (Common):**
- External host can only send if internal host sent to that specific IP:port first
- Requires coordinated hole-punching (what Freenet does)

**Symmetric NAT (Hard):**
- Maps internal IP:port to different external port for each destination
- Difficult to traverse without relay

### Freenet's Approach

**Primary Strategy:** Aggressive outbound packets to establish NAT mappings

**Why It Works:**
- Gateway IP is known in advance
- Peer sends multiple packets rapidly to gateway
- Gateway responds immediately
- NAT mapping created for this specific peer-gateway pair

**Limitations:**
- Symmetric NAT may require relay (not yet implemented)
- Multiple layers of NAT can be challenging
- Very restrictive firewalls may block all UDP

### Future Enhancements

**Potential Improvements (NOT Roadmap):**
1. STUN-like probing to classify NAT type
2. Relay servers for symmetric NAT scenarios
3. UPnP/PCP for automatic port forwarding
4. ICE-like simultaneous open for peer-to-peer NAT traversal

## Firewall and Router Configuration

### Recommended Settings for Gateways

**Port Forwarding:**
- Forward UDP port (default: dynamically assigned) to gateway internal IP
- Static port allocation recommended for reliable operation
- Configure both IPv4 and IPv6 if dual-stack

**Firewall Rules:**
- Allow inbound UDP on configured port
- Allow outbound UDP to any destination
- Do NOT enable SPI (Stateful Packet Inspection) strict mode for Freenet port

### Troubleshooting Connection Issues

**Symptom: Cannot connect to gateway**
- Check: Is gateway UDP port open?
- Check: Does firewall allow outbound UDP?
- Test: Can you ping gateway IP?

**Symptom: Frequent disconnections**
- Check: NAT timeout (may need to shorten keep-alive interval)
- Check: Network quality (packet loss rate)
- Check: Version compatibility

**Symptom: Slow connection establishment**
- Check: High RTT (may need to increase handshake timeout)
- Check: Packet loss during handshake
- Check: Rate limiting hitting threshold

## Performance Considerations

### Connection Overhead

**Memory per Connection:**
- ~1-2 KB per active connection (state, buffers, keys)
- Negligible for typical node (100-1000 connections)

**CPU Overhead:**
- Handshake: ~1ms per connection (X25519 operations)
- Data transfer: ~10-50 μs per packet (AES-GCM with hardware acceleration)
- Keep-alive: Minimal (1 packet every 5 seconds)

**Network Overhead:**
- Handshake: 3-10 packets (~250-800 bytes total)
- Keep-alive: 2 packets/10 seconds (~60 bytes)
- Encryption: 29 bytes per packet (type, nonce, tag)

### Scaling Limits

**Theoretical:**
- ~65,000 connections per IP (UDP port limit)
- Memory: ~130 MB for 65k connections
- CPU: Bottleneck is packet processing, not connection management

**Practical:**
- 1,000-10,000 connections typical for large gateway
- Bandwidth usually limits before connection count
- OS ulimit may need adjustment for >1024 connections

## Monitoring and Observability

### Key Metrics

**Connection Health:**
- Active connections count
- Connection establishment rate
- Connection failure rate by reason
- Average handshake duration

**NAT Traversal:**
- Intro packet send attempts before success
- Rate limit hit count per IP
- Expected inbound success rate

**Liveness:**
- Keep-alive round-trip time
- Missed ping count
- Unexpected disconnection rate

### Debug Logging

**Verbose Mode:**
- Logs every connection state transition
- Handshake packet contents (encrypted payload not shown)
- Keep-alive ping/pong activity
- Connection error details

**Production Mode:**
- Connection establishment/termination only
- Errors and warnings
- Aggregate statistics periodically

**Code Reference:** `crates/core/src/transport/metrics.rs` (telemetry)

## References

### Related Documentation

- [Security Architecture](security.md) - Encryption and key exchange details
- [LEDBAT++ Design](design/ledbat-plus-plus.md) - Congestion control during active connection
- [Bandwidth Configuration](configuration/bandwidth-configuration.md) - Rate limiting and flow control

### Source Code

| Component | Location |
|-----------|----------|
| Connection establishment | `crates/core/src/transport/connection_handler.rs:1473-1900` |
| Inbound connections | `crates/core/src/transport/connection_handler.rs:1282-1471` |
| Keep-alive mechanism | `crates/core/src/transport/peer_connection.rs:243-384` |
| Identity change detection | `crates/core/src/transport/connection_handler.rs:752-760` |
| DoS rate limiting | `crates/core/src/transport/connection_handler.rs:47-50, 654-668` |

---

**Last Updated:** 2026-01-19
**Status:** Production

# Fix: Reduce cold-start latency for GET operations

## Problem
GET operations were taking 13-17 seconds on cold start in small networks due to on-demand connection establishment. Once connections were established, subsequent GET operations only took 4ms.

## Root Cause
1. NAT traversal was using 600ms timeouts with exponential backoff
2. Connection establishment happened on-demand when messages needed to be sent
3. Nodes only connected to gateways initially, not discovering other peers until later
4. Connection maintenance task ran every 60 seconds (too slow for responsive startup)

## Solution
This PR implements several coordinated changes to ensure nodes establish connections quickly on startup:

### 1. NAT Traversal Optimization (`transport/connection_handler.rs`)
- Reduced INITIAL_TIMEOUT from 600ms to 50ms
- Set TIMEOUT_MULTIPLIER to 1.0 (no exponential backoff)
- Changed packet sending to continuous 50ms intervals for hole punching

### 2. Connection Configuration (`test nodes`)
- Set min_connections to 2 for test networks (was 25)
- Set max_connections to 10 for test networks

### 3. Gateway Connection Detection (`operations/connect.rs`)
- Fixed initial_join_procedure to check for unconnected gateways specifically
- Previously only connected if open_connections == 0

### 4. Connection Maintenance (`ring/mod.rs`)
- Reduced CHECK_TICK_DURATION to 2 seconds for tests (was 60 seconds)

### 5. Immediate Peer Discovery (`operations/connect.rs`, `node/p2p_impl.rs`)
- Send FindOptimalPeer request immediately after gateway connection
- Added aggressive connection acquisition phase during startup
- Track immediate connect operations in LiveTransactionTracker

## Results
- Nodes now establish 2 connections within 1 second of startup
- First GET: Still ~13 seconds (due to remaining on-demand connections)
- Second GET: 4ms (all connections established)

## Test Evidence
Added test that demonstrates:
- First GET: 13-17 seconds
- Second GET: 4ms
- Gateway second GET: 133ms

## Future Work
While this PR significantly improves connection establishment, the first GET still takes ~13 seconds due to on-demand connection establishment between non-gateway peers. A follow-up PR could:
1. Pre-warm all connections in small networks
2. Reduce NAT traversal MAX_TIMEOUT further
3. Implement connection prediction based on routing tables
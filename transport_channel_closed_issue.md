# Transport Layer Channel Closed Errors During Connection Establishment

## Summary

During connection establishment, particularly in tests with network topology constraints, we're seeing "channel closed" errors from the transport layer that cause nodes to fail. These are distinct from the connection callback dropping issues that were fixed in the related PR.

## Problem Description

When establishing connections between nodes, the transport layer's `UdpPacketsListener` encounters channel closed errors during packet reception. This leads to:

1. Gateway connection failures with "decryption error"
2. Transport connection drops with "failed to receive packet from remote, connection closed"
3. Node failures that cascade through the test infrastructure

## Affected Components

- `crates/core/src/transport/connection_handler.rs`:
  - `UdpPacketsListener::listen()` - lines 319 and 333
  - Gateway connection handling
  - Inbound packet sender channels

## Error Pattern

```
WARN freenet::transport::connection_handler: failed to receive packet from remote, connection closed, remote_addr: 127.0.0.1:53226, err: channel closed
ERROR freenet::transport::connection_handler: Failed to establish gateway connection, error: decryption error, remote_addr: 127.0.0.1:53226
```

## Tests Affected

- `test_ping_partially_connected_network`
- `test_ping_multi_node` 
- Other network topology tests with constrained connectivity

## Root Cause Analysis

The errors appear when:
1. A remote connection's inbound packet sender channel is dropped
2. The UDP packets listener tries to send packets to a closed channel
3. This often happens during rapid connection attempts or when nodes have connectivity constraints

## Potential Issues

1. **Race condition**: Channels may be dropped while packets are still being processed
2. **Cleanup timing**: Connection cleanup may happen too aggressively
3. **Error propagation**: Transport errors cascade up causing node failures
4. **Channel lifetime**: Inbound packet sender channels may have incorrect lifetimes

## Proposed Investigation

1. Trace the lifecycle of `inbound_packet_sender` channels
2. Identify why channels are being dropped during active connections
3. Add better error recovery for transport layer channel failures
4. Consider if connection attempts need better synchronization

## Related Issues

- Original issue about connection callback dropping (fixed)
- Network topology test failures (#1615)

## Notes

This is a separate issue from the connection callback dropping that was addressed. The transport layer channel management appears to have its own set of race conditions or lifecycle issues that need investigation.
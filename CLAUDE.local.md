- contract states are commutative monoids, they can be "merged" in any order to arrive at the same result. This may reduce some potential race conditions.

## Transport Layer Key Management Issues (2025-01-06)

### Problem
Integration test `test_put_contract` was failing with "Failed to decrypt packet" errors after v0.1.5 deployment to production. The same decryption failures were affecting the River app in production.

### Root Cause  
The transport layer was incorrectly handling symmetric key establishment for gateway connections:

1. **Gateway connection key misuse**: Gateway was using different keys for inbound/outbound when it should use the same client key for both directions
2. **Client ACK encryption error**: Client was encrypting its final ACK response with the gateway's key instead of its own key
3. **Packet routing overflow**: When existing connection channels became full, packets were misrouted to new gateway connection handlers instead of waiting

### Key Protocol Rules
- **Gateway connections**: Use the same symmetric key (client's key) for both inbound and outbound communication
- **Peer-to-peer connections**: Use different symmetric keys for each direction (each peer's own inbound key)
- **Connection establishment**: Only initial gateway connections and explicit connect operations should create new connections
- **PUT/GET/SUBSCRIBE/UPDATE operations**: Should only use existing active connections, never create new ones

### Fixes Applied

#### 1. Gateway Connection Key Fix (`crates/core/src/transport/connection_handler.rs:578-584`)
```rust
// For gateway connections, use the same key for both directions
let inbound_key = outbound_key.clone();
let outbound_ack_packet = SymmetricMessage::ack_ok(
    &outbound_key,
    outbound_key_bytes.try_into().unwrap(),
    remote_addr,
)?;
```

#### 2. Client ACK Response Fix (`crates/core/src/transport/connection_handler.rs:798-811`)
```rust
// Use our own key to encrypt the ACK response (same key for both directions with gateway)
outbound_packets
    .send((
        remote_addr,
        SymmetricMessage::ack_ok(
            &inbound_sym_key,  // Use our own key, not the gateway's
            inbound_sym_key_bytes,
            remote_addr,
        )?
        .prepared_send(),
    ))
    .await
    .map_err(|_| TransportError::ChannelClosed)?;
```

#### 3. Packet Sending Consistency (`crates/core/src/transport/connection_handler.rs:740-747`)
```rust
let packet_to_send = our_inbound.prepared_send();
outbound_packets
    .send((remote_addr, packet_to_send.clone()))
    .await
    .map_err(|_| TransportError::ChannelClosed)?;
sent_tracker
    .report_sent_packet(SymmetricMessage::FIRST_PACKET_ID, packet_to_send);
```

### Testing
- Created specialized transport tests in `crates/core/src/transport/test_gateway_handshake.rs`
- `test_gateway_handshake_symmetric_key_usage()`: Verifies gateway connections use same key for both directions  
- `test_peer_to_peer_different_keys()`: Verifies peer-to-peer connections use different keys
- Both specialized tests pass, confirming the transport layer fixes work correctly

### Root Cause Analysis Complete

#### PUT Operation Connection Creation Issue
**Location**: `crates/core/src/node/network_bridge/p2p_protoc.rs:242-291`

**Problem**: PUT/GET/SUBSCRIBE/UPDATE operations create new connections when no existing connection is found, violating the protocol rule that these operations should only use existing active connections.

**Behavior**: When `NetworkBridge.send()` is called and no existing connection exists:
1. System logs warning: "No existing outbound connection, establishing connection first"  
2. Creates new connection via `NodeEvent::ConnectPeer`
3. Waits up to 5 seconds for connection establishment
4. Attempts to send message on newly created connection

**Channel Overflow Root Cause**: Channels fill up due to throughput mismatch:
- **Fast UDP ingress**: Socket receives packets quickly
- **Slow application processing**: `peer_connection_listener` processes one message at a time sequentially
- **Limited buffering**: 100-packet channel buffer insufficient for high-throughput scenarios
- **No flow control**: System creates new connections instead of implementing proper backpressure

**Cascade Effect**: Channel overflow → packet misrouting → wrong connection handlers → decryption failures → new connection creation

#### Required Fix
The network bridge should fail gracefully or retry with existing connections instead of creating new ones for PUT/GET/SUBSCRIBE/UPDATE operations. Only initial gateway connections and explicit CONNECT operations should establish new connections.

### Next Steps
1. Modify PUT/GET operation handling to use only existing connections
2. Implement proper backpressure handling for full channels instead of creating new connections
3. Test that integration test `test_put_contract` passes after the fix
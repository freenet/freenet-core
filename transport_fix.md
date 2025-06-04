# Transport Layer Channel Management Fix

## Problem Analysis

The transport layer has several issues causing "channel closed" errors:

1. **Premature connection dropping**: When a new connection request arrives for an already connected peer, the existing connection is immediately dropped without proper cleanup or verification.

2. **No connection reuse**: The system doesn't check if an existing connection is still valid before dropping it.

3. **Race conditions**: During rapid connection attempts (common in constrained network tests), packets can arrive for connections still being established, leading to channel drops.

## Proposed Solution

### 1. Check Connection Health Before Dropping

Instead of immediately dropping existing connections, check if they're still active:

```rust
// Instead of:
if let Some(_conn) = self.remote_connections.remove(&remote_addr) {
    tracing::warn!(%remote_addr, "connection already established, dropping old connection");
}

// Do:
if let Some(existing_conn) = self.remote_connections.get(&remote_addr) {
    // Try to send a test packet to verify the connection is still alive
    if existing_conn.inbound_packet_sender.is_closed() {
        // Only remove if the channel is actually closed
        self.remote_connections.remove(&remote_addr);
        tracing::warn!(%remote_addr, "removing closed connection");
    } else {
        // Connection is still alive, ignore new connection attempt
        tracing::debug!(%remote_addr, "connection already established and healthy, ignoring new attempt");
        // Notify the caller that connection already exists
        let _ = open_connection.send(Err(TransportError::ConnectionEstablishmentFailure {
            cause: "connection already exists".into(),
        }));
        continue;
    }
}
```

### 2. Handle Channel Errors Gracefully

When sending packets fails due to closed channels, don't silently drop the connection:

```rust
// For established connections (line 316):
if let Some(remote_conn) = self.remote_connections.remove(&remote_addr) {
    match remote_conn.inbound_packet_sender.try_send(packet_data) {
        Ok(_) => {
            self.remote_connections.insert(remote_addr, remote_conn);
        }
        Err(mpsc::error::TrySendError::Full(_)) => {
            // Channel full, reinsert and log
            self.remote_connections.insert(remote_addr, remote_conn);
            tracing::warn!(%remote_addr, "inbound packet channel full");
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            // Channel closed, connection is dead
            tracing::warn!(%remote_addr, "connection closed, removing from active connections");
            // Don't reinsert - connection is truly dead
        }
    }
    continue;
}
```

### 3. Add Connection State Tracking

Track connection establishment state to avoid race conditions:

```rust
enum ConnectionState {
    Establishing,
    Established,
    Closing,
}

struct ConnectionTracker {
    state: ConnectionState,
    conn: Option<InboundRemoteConnection>,
}
```

### 4. Implement Connection Deduplication

For gateway connections, check if a connection attempt is already in progress:

```rust
// Before starting a new gateway connection:
if ongoing_gw_connections.contains_key(&remote_addr) {
    tracing::debug!(%remote_addr, "gateway connection already in progress, ignoring duplicate packet");
    continue;
}
```

## Implementation Steps

1. ✅ Add connection health checks before dropping
2. ✅ Use try_send instead of send for non-critical paths
3. ✅ Add proper error handling for channel failures
4. ✅ Increase channel buffer sizes from 1 to 100 for packet channels
5. Implement connection state tracking (if needed)
6. Add tests for rapid connection scenarios

## Expected Benefits

- Eliminates spurious "channel closed" errors
- Prevents dropping healthy connections
- Handles race conditions in connection establishment
- Improves stability in constrained network topologies
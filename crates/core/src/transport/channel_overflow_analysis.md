# Channel Overflow Analysis

## Problem
- Dropping 2251 packets in 10 seconds (225 packets/second)
- Channels in `peer_connection.rs` have buffer size of only 1
- Channels in `connection_handler.rs` use buffer size of 100

## Root Cause
In `peer_connection.rs` lines 261-262:
```rust
let (outbound_packets, outbound_packets_recv) = mpsc::channel(1);
let (inbound_packet_sender, inbound_packet_recv) = mpsc::channel(1);
```

Buffer size of 1 means:
- Only 1 packet can be queued
- Any additional packets are dropped if receiver hasn't processed the previous one
- With UDP's bursty nature, this causes massive packet loss

## Evidence
From connection_handler.rs:317-322:
```rust
match remote_conn.inbound_packet_sender.try_send(packet_data) {
    Ok(_) => { /* success */ }
    Err(mpsc::error::TrySendError::Full(_)) => {
        // Channel full - this is happening 225 times/second!
    }
}
```

## Solution
Increase channel buffer size from 1 to 100 to match other channels in the codebase.

## Impact
This should significantly reduce packet drops and improve connection stability.
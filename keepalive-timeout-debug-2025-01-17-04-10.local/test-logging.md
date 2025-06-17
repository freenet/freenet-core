# Test Plan for Keep-alive Logging

## Logging Targets Added

1. **freenet_core::transport::keepalive_lifecycle**
   - Keep-alive task start/stop
   - Each tick with timing info
   - Packet send attempts and results
   - Task exit with total lifetime

2. **freenet_core::transport::keepalive_received**
   - Received NoOp keep-alive packets
   - Received NoOp receipt packets
   - Time since last packet

3. **freenet_core::transport::keepalive_timeout**
   - Connection timeout events
   - Keep-alive task status at timeout

4. **freenet_core::transport::keepalive_health**
   - Periodic health checks (every 5s)
   - Time remaining before timeout

## Test Commands

```bash
# Kill any existing freenet
pkill -f "freenet network"

# Start freenet with targeted logging
RUST_LOG="freenet=info,freenet_core::transport::keepalive_lifecycle=info,freenet_core::transport::keepalive_received=info,freenet_core::transport::keepalive_timeout=info,freenet_core::transport::keepalive_health=trace" ./binaries-x86_64-freenet/freenet network --ws-api-port 55509 2>&1 | tee keepalive-timeout-debug-2025-01-17-04-10.local/test-$(date +%Y%m%d-%H%M%S).log
```

## What to Look For

1. **Hypothesis 1: Keep-alive task cancelled**
   - Look for "Keep-alive task EXITING" before timeout
   - Check elapsed_since_start_secs value

2. **Hypothesis 2: Packets sent but not received**
   - Count "Sending keep-alive NoOp packet" vs "Received NoOp keep-alive packet"
   - Check packet_id sequences

3. **Hypothesis 3: Wrong encryption keys**
   - Would show as decryption errors (already visible)

4. **Hypothesis 4: Channel overflow**
   - Look for send errors in keep-alive task
   - Check timing between send attempts

5. **Hypothesis 5: Timer interval bug**
   - Check tick_interval_ms values
   - Should be ~10000ms (10 seconds)
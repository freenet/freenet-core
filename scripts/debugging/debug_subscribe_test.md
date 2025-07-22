# Debugging test_put_with_subscribe_flag

## Test Overview
The test verifies that when a client PUTs a contract with `subscribe=true`, they receive update notifications when another client updates the contract.

## Test Flow
1. Node A (client) and Node B (gateway) start up
2. Client 1 connects to Node A and PUTs contract with subscribe=true
3. Client 2 connects to Node A and GETs the contract
4. Client 2 UPDATEs the contract
5. Client 1 should receive an update notification

## Failure Mode
The test times out after 60 seconds, likely because Client 1 never receives the update notification.

## Hypotheses

### H1: Subscription Registration Race
**Theory**: The subscription might not be fully registered before the update happens
**Test**: Add delays after PUT to ensure subscription is registered
**Evidence**: Need to check logs for subscription registration timing

### H2: Update Notification Dropped
**Theory**: The update notification might be dropped due to channel overflow or network issues
**Test**: Reduce channel sizes to make dropping more likely
**Evidence**: Look for CHANNEL_OVERFLOW warnings in logs

### H3: WebSocket Channel Issues
**Theory**: The WebSocket response channel might be too small (only size 1)
**Test**: Increase WebSocket channel size
**Evidence**: Check if messages are being dropped at WebSocket layer

### H4: Node Connection Timing
**Theory**: Nodes might not be fully connected when operations start
**Test**: Increase initial sleep time or add connection verification
**Evidence**: Check connection establishment logs

## Modifications to Increase Failure Rate

1. Reduce transport channel sizes from 100 to 10
2. Reduce WebSocket channel size from 1 to 1 (already minimal)
3. Add artificial delays in update notification path
4. Reduce initial node startup wait time
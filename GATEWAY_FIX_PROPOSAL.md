# Gateway Connection Fix Proposal

## Problem Statement

Peers connecting through gateways fail because:
1. Initial transport connection is registered in ConnectionManager
2. When peer sends StartJoinReq, gateway rejects it as "already connected"
3. This prevents proper ring membership

## Current (Flawed) Approach

The ConnectionState enum spreads throughout the codebase, creating a leaky abstraction that violates separation of concerns.

## Proposed Solution

### Core Principle
**Transport connections ≠ Ring membership**

### Implementation

1. **HandshakeHandler Changes**
   - Maintain internal set of pending connections
   - Don't register peers in ConnectionManager until accepted
   - Clean separation between transport and logical layers

2. **ConnectionManager Changes**
   - Remove all ConnectionState logic
   - Only track actual ring members
   - Simplify should_accept() logic

3. **Flow**
   ```
   Peer → Gateway (transport connection)
   Peer → StartJoinReq → Gateway
   Gateway checks should_accept() [peer not in ConnectionManager yet]
   If accepted → Add to ConnectionManager as ring member
   If rejected → Close transport connection
   ```

### Benefits
- No leaky abstractions
- Clear separation of concerns
- Simpler, more maintainable code
- Addresses Nacho's architectural concerns

### Key Insight
The bug is that we're conflating transport connectivity with ring membership. The fix is to keep them separate until the peer is actually accepted.
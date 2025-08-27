# Gateway Contract Storage Issue Test

## Test Setup
To reproduce the issue where a gateway with peer connections cannot store contracts:

### Scenario 1: Gateway WITH peer connections (reproduces bug)
1. Start a gateway with `--is-gateway` flag
2. Ensure it has at least one peer connection
3. Send a PUT request for a contract
4. Observe: Gateway forwards to peers but doesn't store locally
5. Send a GET request for the same contract
6. Result: "Contract state not found in store"

### Scenario 2: Gateway WITHOUT peer connections (works correctly)
1. Start an isolated gateway with `--is-gateway` flag
2. Ensure it has NO peer connections
3. Send a PUT request for a contract
4. Observe: Gateway stores locally (falls into "no peers" path)
5. Send a GET request for the same contract
6. Result: Contract retrieved successfully

## Root Cause Analysis

The bug is in `closest_potentially_caching` function (crates/core/src/ring/mod.rs:251):

```rust
pub fn closest_potentially_caching(
    &self,
    contract_key: &ContractKey,
    skip_list: impl Contains<PeerId>,
) -> Option<PeerKeyLocation> {
    let router = self.router.read();
    
    // BUG: Only considers connected peers, not self!
    self.connection_manager
        .routing(Location::from(contract_key), None, skip_list, &router)
}
```

When the gateway has connections:
1. PUT arrives → forwards to best peer based on location
2. Tries to subscribe → calls `closest_potentially_caching` 
3. Returns connected peers (not self)
4. If no peer is "close enough" → returns None
5. Subscribe fails with "NoCachingPeers" error
6. Contract is never stored locally

When the gateway has NO connections:
1. PUT arrives → no peers to forward to
2. Falls into special case: stores locally
3. Subscribe succeeds (subscribes to self)
4. Contract is available for GET

## Evidence from Production Logs

```
Aug 27 19:07:48 nova: Error subscribing to contract, error: Ran out of, or haven't found any, caching peers for contract 3nYeDLMKB2rq5R4PNMMHxrR7qMPanKPMQnRkTpjviUf4
Aug 27 16:35:29 nova: Contract state not found in store, contract: HzVUtHcG2cwV2RMxZaVLd5xAkEFrSC592QDjRiDmvYNJ
```

## Proposed Fix

The `closest_potentially_caching` function should consider the node itself as a potential caching location:

```rust
pub fn closest_potentially_caching(
    &self,
    contract_key: &ContractKey,
    skip_list: impl Contains<PeerId>,
) -> Option<PeerKeyLocation> {
    let router = self.router.read();
    let target_location = Location::from(contract_key);
    
    // Check connected peers
    let best_peer = self.connection_manager
        .routing(target_location, None, &skip_list, &router);
    
    // Also consider self if we have a location
    if let Some(own_location) = self.connection_manager.own_location() {
        // Compare self vs best peer to find closest to target
        match best_peer {
            Some(peer) => {
                // Return closer of self or peer
                if own_location.location.distance(&target_location) 
                    < peer.location.distance(&target_location) {
                    Some(own_location)
                } else {
                    Some(peer)
                }
            }
            None => Some(own_location), // No peers, use self
        }
    } else {
        best_peer
    }
}
```
# Test Contracts

This directory contains pre-compiled WASM contracts for use in integration tests.

## Ping Contract

The ping contract is a simple contract that maintains a record of "pings" from different peers. It's useful for testing because:

1. Simple state model - just a HashMap of peer -> timestamps
2. Easy to verify - can check if updates from different peers are received
3. Supports both PUT and UPDATE operations

### Contract Details

- **State**: HashMap<String, Vec<DateTime>> - Maps peer IDs to ping timestamps
- **Parameters**: PingContractOptions with TTL and frequency settings
- **Operations**: 
  - PUT: Initial contract deployment with empty or pre-populated state
  - UPDATE: Add new ping entries from peers
  - GET: Retrieve current ping state

### Compiling the Contract

To rebuild the WASM file:

```bash
cd apps/freenet-ping/contracts/ping
# For release (recommended - 210KB):
cargo build --target wasm32-unknown-unknown --release --features contract
# For debug (13MB):
cargo build --target wasm32-unknown-unknown --features contract
```

The compiled WASM will be at:
- Release: `target/wasm32-unknown-unknown/release/freenet_ping_contract.wasm` (210KB)
- Debug: `target/wasm32-unknown-unknown/debug/freenet_ping_contract.wasm` (13MB)

**Note**: We use the release version in tests due to the significant size difference.

### Using in Tests

```rust
// Load the pre-compiled WASM
let contract_bytes = include_bytes!("test_contracts/ping_contract.wasm");

// Create parameters
let ping_options = PingContractOptions {
    frequency: Duration::from_secs(2),
    ttl: Duration::from_secs(60),
    tag: "test".to_string(),
    code_key: "".to_string(),
};
```
# Freenet Application Ubertest

## Overview

The ubertest is a comprehensive integration test that verifies freenet-core can support real-world complex applications. It currently uses River (decentralized group chat) as the reference application.

## What It Tests

1. **Multi-peer network formation** (1 gateway + configurable number of peers, default 10)
2. **Network topology health** (verifies peer-to-peer connections, not just gateway-centric)
3. **Real application workflows** via `riverctl`:
   - Room creation
   - User invitation/joining
   - Bidirectional messaging
   - State consistency across peers

## Prerequisites

### Installing riverctl

The test requires `riverctl` to be installed and up-to-date:

```bash
cargo install riverctl
```

The test will verify that your installed riverctl matches the latest version on crates.io and fail with helpful instructions if not.

## Running the Test

### Basic Run

```bash
cd crates/core
cargo test --test ubertest
```

### Configure Peer Count

```bash
UBERTEST_PEER_COUNT=5 cargo test --test ubertest
```

## Test Phases

The test executes sequentially through these phases:

### Phase 1: Setup (Step 1-3)
- Verify riverctl installation and version
- Create gateway node
- Create N peer nodes with staggered startup (10s between each)

### Phase 2: Network Formation (Step 4-5)
- Wait for all peers to start
- Poll for network topology to stabilize
- Verify peer-to-peer connections exist

### Phase 3: Application Testing (Step 6-10)
- User 1 creates a chat room via peer 0
- User 2 joins the room via peer 1 (different peer!)
- User 2 sends message → User 1 receives
- User 1 sends message → User 2 receives
- Verify message delivery

## Expected Behavior

- **Startup time**: ~2-3 minutes for 10 peers (10s stagger + 30s buffer)
- **Network formation**: Additional 1-2 minutes for topology to stabilize
- **Total test time**: ~5-10 minutes depending on peer count
- **Success criteria**: All phases complete without errors

## Failure Modes

### riverctl Not Found
```
Error: riverctl not found in PATH

Please install riverctl:
$ cargo install riverctl
```

### Version Mismatch
```
Error: riverctl version mismatch!
Installed: 0.1.0
Latest:    0.1.1

Please update riverctl:
$ cargo install riverctl --force
```

### Network Formation Timeout
```
Error: Network topology verification - timeout after 120s
```

This indicates peers couldn't establish connections. Check:
- Firewall/network restrictions
- Port conflicts
- freenet-core logs for connection errors

### Contract Operation Failures
```
Error: Failed to create room via riverctl
```

This indicates freenet-core contract operations aren't working. Check:
- freenet-core logs for PUT/GET errors
- River contract compatibility
- WebSocket connection status

## CI Integration

### GitHub Actions

```yaml
- name: Install riverctl
  run: cargo install riverctl

- name: Run ubertest
  run: cargo test --test ubertest
  env:
    UBERTEST_PEER_COUNT: 6  # Fewer peers for faster CI
```

## Development Status

### ✅ Implemented
- riverctl version verification
- Multi-peer network setup with configurable count
- Staggered peer startup
- Basic polling infrastructure
- Test structure and phases

### 🚧 TODO
- Actual network topology verification (currently placeholder)
- riverctl command execution for room operations
- Message sending/receiving via riverctl
- State verification across peers
- Proper error handling and diagnostics

## Design Philosophy

- **Test belongs in freenet-core**: Tests the platform, not the application
- **River as dependency**: Uses latest released River via riverctl, not source
- **Sequential execution**: Each phase depends on previous success
- **Poll don't sleep**: Wait for actual conditions, not arbitrary timeouts
- **Configurable scale**: Adjustable peer count for local vs CI environments

## Future Enhancements

1. **Additional applications**: Test with other Freenet apps beyond River
2. **Chaos testing**: Random peer failures/restarts during execution
3. **Performance metrics**: Measure operation latency, throughput
4. **Network partitions**: Test split-brain scenarios
5. **Load testing**: Many concurrent operations across peers

## Related Issues

- Original discussion: [Insert GitHub issue link]
- River integration: [Insert River issue link]

## See Also

- [River Documentation](https://github.com/freenet/river)
- [riverctl CLI](https://crates.io/crates/riverctl)
- Other integration tests: `crates/core/tests/operations.rs`, `connectivity.rs`

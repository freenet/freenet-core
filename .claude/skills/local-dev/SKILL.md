---
name: freenet-local-dev
description: Set up and manage local Freenet development environments for building, publishing, and iterating on dApps (contracts, delegates, UIs). Use when the user wants to test contract changes locally, debug UI issues, run a local node with a test contract, or iterate on a Freenet application without deploying to the live network.
---

# River Local Development

River-specific build, publish, and debug workflows. For general Freenet local
node management, contract publishing, and debugging patterns, see the
`local-dev` skill in the [freenet-agent-skills](https://github.com/freenet/freenet-agent-skills) plugin.

## Quick Start (River on macOS)

### Prerequisites

```bash
which freenet fdev dx
rustup target add wasm32-unknown-unknown
```

### One-time setup

```bash
cd /Volumes/PRO-G40/projects/river

# 1. Build the web-container-tool (native, not cross-compiled)
cargo build --release -p web-container-tool

# 2. Build the web container contract WASM
cargo build --release --target wasm32-unknown-unknown -p web-container-contract

# 3. Generate test keys
mkdir -p test-contract
target/release/web-container-tool generate --output test-contract/test-keys.toml

# 4. Start an isolated test node (in a separate terminal or background)
mkdir -p ~/Library/Logs/freenet-test-node
freenet network \
  --network-port 31338 \
  --ws-api-port 7510 \
  --ws-api-address 0.0.0.0 \
  --is-gateway \
  --skip-load-from-network \
  --data-dir ~/freenet-test-node/data \
  --public-network-address 127.0.0.1 \
  --log-dir ~/Library/Logs/freenet-test-node \
  --log-level debug
```

### Fast iteration script

```bash
cd /Volumes/PRO-G40/projects/river

# Full rebuild + republish (~15s):
./scripts/local-republish.sh

# Skip UI build if only repackaging (~2s):
./scripts/local-republish.sh --skip-build

# Target a different port:
./scripts/local-republish.sh --port 7509
```

The script outputs desktop and phone URLs after publishing.

## Build Commands Reference

### Individual components

```bash
cargo build --release --target wasm32-unknown-unknown -p room-contract  # Room contract WASM
cargo build --release --target wasm32-unknown-unknown -p chat-delegate   # Chat delegate WASM
cargo build --release --target wasm32-unknown-unknown -p web-container-contract  # Web container
(cd ui && dx build --release)                                            # UI (Dioxus)
```

### Development mode

```bash
cargo make dev                     # dx serve with hot reload (localhost:8080)
cargo make dev-example             # dx serve with example data (no network needed)
```

## Fast Iteration Loop

### For UI changes only (fastest)

```bash
cd /Volumes/PRO-G40/projects/river
# 1. Make your UI change in ui/src/
# 2. Rebuild + republish:
./scripts/local-republish.sh
# 3. Hard-refresh browser (Cmd+Shift+R)
```

### For contract changes

```bash
# 1. Rebuild contract
cargo build --release --target wasm32-unknown-unknown -p room-contract

# 2. Copy to UI public dir (UI embeds contract WASM)
cp target/wasm32-unknown-unknown/release/room_contract.wasm ui/public/contracts/

# 3. Full rebuild + publish
./scripts/local-republish.sh
```

### For delegate changes

```bash
# 1. Rebuild delegate (UI includes delegate via include_bytes!)
cargo build --release --target wasm32-unknown-unknown -p chat-delegate

# 2. Rebuild UI (picks up new delegate) + publish
./scripts/local-republish.sh
```

## Manual River publish (macOS)

The `cargo make` publish tasks use `x86_64-unknown-linux-gnu` for the
web-container-tool. On macOS, use the `local-republish.sh` script or
run the steps manually:

```bash
cd /Volumes/PRO-G40/projects/river

# 1. Build UI
(cd ui && dx build --release)

# 2. Compress
(cd target/dx/river-ui/release/web/public && tar -cJf ../../../../../webapp/webapp.tar.xz *)

# 3. Sign with test keys
target/release/web-container-tool sign \
  --input target/webapp/webapp.tar.xz \
  --output target/webapp/webapp-test.metadata \
  --parameters target/webapp/webapp-test.parameters \
  --key-file test-contract/test-keys.toml \
  --version $(( $(date +%s) / 60 ))

# 4. Publish
fdev --port 7510 execute put \
  --code target/wasm32-unknown-unknown/release/web_container_contract.wasm \
  --parameters target/webapp/webapp-test.parameters \
  contract \
  --webapp-archive target/webapp/webapp.tar.xz \
  --webapp-metadata target/webapp/webapp-test.metadata
```

## River-Specific Debugging

### Debug overlay

River has a built-in debug overlay activated via `?debug=1` query parameter.
It shows timestamped log messages on-screen with a minimize/expand toggle —
essential for mobile where console is inaccessible.

```
http://{IP}:7510/v1/contract/web/{CONTRACT_ID}/?debug=1
```

Use `crate::util::debug_log("msg")` in Rust to log to the overlay. The overlay
is feature-flagged — it does nothing without `?debug=1`.

### Panic overlay

River installs a WASM panic hook that creates a visible red error overlay
showing the panic message. This appears automatically on any crash, no query
param needed.

### Delegate signing flow

When sending messages, River uses a delegate-based signing architecture:

1. **Room creation** → `create_room_modal.rs` generates `SigningKey`, stores in ROOMS signal, and calls `store_signing_key()` to save it in the chat delegate
2. **Message send** → UI calls `sign_message_with_fallback(room_key, msg, fallback_sk)`
3. **Delegate signing** → `send_delegate_request(SignMessage{...})` → delegate looks up `signing_key:{origin}:{room_key}` → returns signature
4. **Fallback** → If delegate fails, signs locally with `fallback_sk.sign()`
5. **Delta applied** → Message added to local state → `NEEDS_SYNC` set → `ProcessRooms` → UPDATE sent

Key debugging points:
- Node logs show `"Sign request for room, signature created: true/false"` — if false, delegate doesn't have the key
- Browser console shows fallback path: `"Delegate signing failed, using fallback"`
- If no UPDATE appears in node logs after signing, check if WebSocket is still connected

### Check contract state via riverctl

```bash
riverctl --node-url ws://127.0.0.1:7510/v1/contract/command?encodingProtocol=native room list
```

### Timeline analysis for message send

1. `SignMessage received` → delegate got the sign request
2. `signature created: true/false` → delegate had (or didn't have) the key
3. `Update { key: ... }` → UPDATE arrived at node
4. `ResultRouter received result` → UPDATE processed, result sent back to client

If step 1 happens but step 3 doesn't, the browser died between signing and sending the UPDATE.

### River-specific common issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| Messages fail to send (new room) | Signing key not stored in delegate | Fixed: `create_room_modal.rs` now calls `store_signing_key` after room creation |
| "signature created: false" in node logs | Delegate can't find signing key for room | Ensure `StoreSigningKey` is sent after room creation; fallback signs locally |
| Mobile send appears stuck | Browser suspends WASM when screen locks | Keep phone screen active; delegate signing avoids long async chains |

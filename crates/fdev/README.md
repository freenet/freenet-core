# fdev

The Freenet Development Tool (`fdev`) is a command-line utility to create, build, publish, inspect, and test Freenet contracts or delegates. It also supports local node simulations, a TUI-based WASM runtime, and optional network metrics gathering.

## Table of Contents

1. [Overview](#overview)
2. [Installation](#installation)
3. [Local Node Example](#local-node-example)
4. [Creating a New Package](#creating-a-new-package)
5. [Building a Contract or Delegate](#building-a-contract-or-delegate)
6. [Publishing a Contract or Delegate](#publishing-a-contract-or-delegate)
7. [Updating a Contract](#updating-a-contract)
8. [Contract State Builder Example](#contract-state-builder-example)
9. [Inspecting a Compiled WASM](#inspecting-a-compiled-wasm)
10. [Using the Local WASM Runtime (TUI)](#using-the-local-wasm-runtime-tui)
11. [Querying Connected Peers](#querying-connected-peers)
12. [Testing (Single-Process or Multi-Process)](#testing-single-process-or-multi-process)
13. [Network Metrics Server](#network-metrics-server)
14. [Directory & Module Structure](#directory--module-structure)
15. [Common Workflows](#common-workflows)

---

## Overview

`fdev` helps Freenet developers:

- **Create** new contract or web-app packages.
- **Build** Rust-based WASM contracts/delegates.
- **Publish** new contracts or delegates.
- **Update** existing contracts’ state.
- **Inspect** compiled artifacts (hash, version, etc.).
- **Run** local TUI-based or multi-process network simulations.
- **Query** your node for connected peers.
- **Optionally** run a metrics server for simulation insights.

---

## Installation

Usually you'll build `fdev` as part of the Freenet project:

1. Clone or be inside the Freenet repository.
2. Navigate to `freenet-core/main/crates/fdev`.
3. Build in release mode:
   ```bash
   cargo build --release
   ```
4. You can then run it directly:
   ```bash
   ./target/release/fdev --help
   ```
   Or place `target/release/fdev` on your PATH.

---

## Local Node Example

If you want to try out a contract in local mode, you typically run a separate “local node” process that listens for contract commands. There is a `local-node` executable (or an equivalent) in the Freenet repository. You can do something like:

```bash
# Example: run the local node (CLI options may vary)
local-node local-node-cli --help
```

Once the local node is up, you can run `fdev` or a script to send commands to it.  
Below is a *legacy* style example (paths/names may differ in current code):

```bash
# Example: older usage for local node exploration
./fdev run-local \
  --input-file /tmp/input \
  --terminal-output \
  --deser-format json \
  "/home/.../freenet/crates/http-gw/examples/test_web_contract.wasm"
```

In newer code, you will likely use `fdev wasm-runtime`, described in a [later section](#using-the-local-wasm-runtime-tui), to do an equivalent local testing workflow.

---

## Creating a New Package

`fdev new` scaffolds a new Freenet package. Two options:

- **`contract`**: A standard WASM contract.
- **`webapp`**: A contract container that also bundles a front-end (TypeScript/webpack by default).

Example:
```bash
fdev new contract
```
Generates Rust cargo boilerplate (and for web apps, TypeScript/webpack scaffolding too).

---

## Building a Contract or Delegate

Use `fdev build` to compile a Rust-based WASM contract or delegate. For example:

```bash
fdev build \
    --package-type [contract|delegate] \
    [--features some_features] \
    [--debug] \
    [--version 0.0.2]
```

- **`--package-type`** defaults to `contract`.
- **`--features`** can specify crate features, e.g. `freenet-main-contract` or `freenet-main-delegate`.
- **`--debug`** produces an unoptimized WASM (useful for local tests).
- **`--version`** sets the embedded Freenet ABI version (defaults to `0.0.1`).

`fdev` looks for a `freenet.toml` in your current directory that defines if your contract is standard or a webapp, and (for webapps) how to compile front-end code.

**After building**, the output artifact is typically placed in `build/freenet/` unless overridden by your config file.

---

## Publishing a Contract or Delegate

`fdev publish` uploads a **new** contract or delegate to a node (either local or network mode).

**Example: Publishing a contract**:
```bash
fdev publish \
    --code path/to/my_contract.wasm \
    --parameters path/to/params.json \
    contract \
    --state path/to/initial_state.json
```
- **`--code`**: path to your built WASM.
- **`--parameters`**: optional contract parameters.
- **`--state`**: optional initial state JSON or binary.

**Example: Publishing a delegate**:
```bash
fdev publish \
    --code path/to/my_delegate.wasm \
    --parameters delegate_params.json \
    delegate \
    [--nonce ... --cipher ...]
```
- If you skip `--nonce` and `--cipher`, a default local-only encryption is used (fine for tests, not recommended for production).

By default, `fdev publish` pushes in **local** mode unless you specify `--mode network` or set `MODE=network`.

---

## Updating a Contract

To push a state delta to an existing contract (identified by its Base58 key):

```bash
fdev execute update <BASE58_CONTRACT_KEY> \
    --delta path/to/delta.json \
    [--address 127.0.0.1 \
     --port 50509 \
     --release]
```

- **`--delta`**: The state delta file (JSON or binary).
- **`--release`** indicates you want to push to the network, instead of local-only.

---

## Contract State Builder Example

Sometimes you only want to build the “state artifact” for a contract or a web front-end. `fdev build` can do this if your `freenet.toml` includes a `state-sources` or webapp config.  
Older Freenet docs mention `build_state` as a separate script, but in practice you can do:

```bash
# Build a web-based front end
fdev build \
    --input-metadata-path /optional/metadata \
    --input-state-path contracts/freenet-microblogging/view/web \
    --output-file contracts/freenet-microblogging-web/freenet_microblogging_view \
    --package-type contract  # or 'webapp' if your config demands
```

Similarly, you can specify `--state` for a model-only contract:
```bash
fdev build \
    --input-state-path contracts/freenet-microblogging/model/ \
    --output-file contracts/freenet-microblogging-data/freenet_microblogging_model
```

*(Adjust paths, actual flags, and config to match your usage.)*

---

## Inspecting a Compiled WASM

`fdev inspect` prints metadata about a built WASM artifact, such as its hash, version, or a derived contract key.

Examples:

```bash
# Show code hash + contract API version
fdev inspect code path/to/contract.wasm

# Display the contract key (hash+empty params => key)
fdev inspect key path/to/contract.wasm

# For a delegate
fdev inspect delegate path/to/delegate.wasm
```

---

## Using the Local WASM Runtime (TUI)

`fdev wasm-runtime` provides a local, interactive environment to test your contract. For instance:

```bash
fdev wasm-runtime \
    --input-file /tmp/input.json \
    --deserialization-format json \
    --terminal-output
```
- The tool will connect to a local Freenet node by default (see `--mode`, `--address`, `--port` if needed).
- Commands you type—like `put`, `get`, `update`, or `exit`—are forwarded to the local node.
- Additional command data is read from the file specified by `--input-file`.

Common subcommands once inside the TUI:
```
help      # print instructions
put       # publish a fresh contract state
get       # retrieve the current contract state
update    # apply a delta to the contract state
exit      # quit TUI
```

---

## Querying Connected Peers

You can list a node’s open connections:

```bash
fdev query
```

It prints a table of peer identifiers and socket addresses for debugging or analysis.

---

## Testing (Single-Process or Multi-Process)

`fdev test` orchestrates an entire simulated network of Freenet nodes for end-to-end testing.

### Single Process

Spins up multiple in-memory nodes under one process:

```bash
fdev test \
    --gateways 2 \
    --nodes 10 \
    --events 100 \
    single-process
```
- **--gateways**: number of gateway nodes
- **--nodes**: number of normal nodes
- **--events**: random events (contract puts, updates, etc.)

### Multi-Process

```bash
fdev test \
    --gateways 2 \
    --nodes 10 \
    network
```
- One node acts as a **supervisor** (you run `fdev test ... network --mode supervisor`).
- Others run as **peers** (`--mode peer`) connecting to the supervisor. This can scale across multiple machines or containers.

*(See internal documentation or run `fdev test --help` for more detail.)*

---

## Network Metrics Server

```bash
fdev network-metrics-server [--log-directory path]
```
- Launches an HTTP server (default port `55010`) for collecting or pushing network stats and event logs.
- You can then visualize or store the data for debugging or simulation metrics analysis.

---

## Directory & Module Structure

Here’s a quick look at the major modules in the `fdev` crate:

```
fdev
├── src
│   ├── main.rs                # CLI entrypoint, top-level subcommands
│   ├── config.rs              # Config structs, subcommand definitions
│   ├── commands/              # 'put', 'update', connect to node, etc.
│   ├── build.rs               # Builds Rust WASM, web assets, etc.
│   ├── wasm_runtime/          # Local TUI runtime for testing contracts
│   ├── testing/               # Tools for single vs multi-process simulations
│   ├── network_metrics_server # Optional server for collecting network stats
│   └── util.rs                # Shared utility helpers
└── Cargo.toml
```

---

## Common Workflows

Below are a few typical steps you might perform:

1. **Create a new contract** (or webapp):
   ```bash
   fdev new contract
   ```
2. **Build**:
   ```bash
   fdev build
   ```
3. **Publish** (locally by default):
   ```bash
   fdev publish \
       --code build/freenet/my_contract.wasm \
       contract \
       --state initial_state.json
   ```
4. **Update** the contract:
   ```bash
   fdev execute update <contract_key> \
       --delta path/to/delta.json
   ```
5. **Inspect** a compiled WASM:
   ```bash
   fdev inspect code build/freenet/my_contract.wasm
   ```
6. **Run local TUI** (optionally):
   ```bash
   fdev wasm-runtime \
       --input-file my_input.json \
       --terminal-output \
       --deserialization-format json
   ```
7. **Check open peers**:
   ```bash
   fdev query
   ```
8. **Simulate** a small test network:
   ```bash
   fdev test --nodes 5 --gateways 1 single-process
   ```
9. **(Optional) Start metrics server**:
   ```bash
   fdev network-metrics-server --log-directory /path/to/logs
   ```

Feel free to run `fdev <subcommand> --help` for more details on any step. Enjoy building with Freenet!Below is a comprehensive README for `fdev` that combines the existing reference documentation and the original README content. You can drop it in place of your current `README.md`.

---

# fdev

The Freenet Development Tool (`fdev`) is a command-line utility to create, build, publish, inspect, and test Freenet contracts or delegates. It also supports local node simulations, a TUI-based WASM runtime, and optional network metrics gathering.

## Table of Contents

1. [Overview](#overview)
2. [Installation](#installation)
3. [Local Node Example](#local-node-example)
4. [Creating a New Package](#creating-a-new-package)
5. [Building a Contract or Delegate](#building-a-contract-or-delegate)
6. [Publishing a Contract or Delegate](#publishing-a-contract-or-delegate)
7. [Updating a Contract](#updating-a-contract)
8. [Contract State Builder Example](#contract-state-builder-example)
9. [Inspecting a Compiled WASM](#inspecting-a-compiled-wasm)
10. [Using the Local WASM Runtime (TUI)](#using-the-local-wasm-runtime-tui)
11. [Querying Connected Peers](#querying-connected-peers)
12. [Testing (Single-Process or Multi-Process)](#testing-single-process-or-multi-process)
13. [Network Metrics Server](#network-metrics-server)
14. [Directory & Module Structure](#directory--module-structure)
15. [Common Workflows](#common-workflows)

---

## Overview

`fdev` helps Freenet developers:

- **Create** new contract or web-app packages.
- **Build** Rust-based WASM contracts/delegates.
- **Publish** new contracts or delegates.
- **Update** existing contracts’ state.
- **Inspect** compiled artifacts (hash, version, etc.).
- **Run** local TUI-based or multi-process network simulations.
- **Query** your node for connected peers.
- **Optionally** run a metrics server for simulation insights.

---

## Installation

Usually you'll build `fdev` as part of the Freenet project:

1. Clone or be inside the Freenet repository.
2. Navigate to `freenet-core/main/crates/fdev`.
3. Build in release mode:
   ```bash
   cargo build --release
   ```
4. You can then run it directly:
   ```bash
   ./target/release/fdev --help
   ```
   Or place `target/release/fdev` on your PATH.

---

## Local Node Example

If you want to try out a contract in local mode, you typically run a separate “local node” process that listens for contract commands. There is a `local-node` executable (or an equivalent) in the Freenet repository. You can do something like:

```bash
# Example: run the local node (CLI options may vary)
local-node local-node-cli --help
```

Once the local node is up, you can run `fdev` or a script to send commands to it.  
Below is a *legacy* style example (paths/names may differ in current code):

```bash
# Example: older usage for local node exploration
./fdev run-local \
  --input-file /tmp/input \
  --terminal-output \
  --deser-format json \
  "/home/.../freenet/crates/http-gw/examples/test_web_contract.wasm"
```

In newer code, you will likely use `fdev wasm-runtime`, described in a [later section](#using-the-local-wasm-runtime-tui), to do an equivalent local testing workflow.

---

## Creating a New Package

`fdev new` scaffolds a new Freenet package. Two options:

- **`contract`**: A standard WASM contract.
- **`webapp`**: A contract container that also bundles a front-end (TypeScript/webpack by default).

Example:
```bash
fdev new contract
```
Generates Rust cargo boilerplate (and for web apps, TypeScript/webpack scaffolding too).

---

## Building a Contract or Delegate

Use `fdev build` to compile a Rust-based WASM contract or delegate. For example:

```bash
fdev build \
    --package-type [contract|delegate] \
    [--features some_features] \
    [--debug] \
    [--version 0.0.2]
```

- **`--package-type`** defaults to `contract`.
- **`--features`** can specify crate features, e.g. `freenet-main-contract` or `freenet-main-delegate`.
- **`--debug`** produces an unoptimized WASM (useful for local tests).
- **`--version`** sets the embedded Freenet ABI version (defaults to `0.0.1`).

`fdev` looks for a `freenet.toml` in your current directory that defines if your contract is standard or a webapp, and (for webapps) how to compile front-end code.

**After building**, the output artifact is typically placed in `build/freenet/` unless overridden by your config file.

---

## Publishing a Contract or Delegate

`fdev publish` uploads a **new** contract or delegate to a node (either local or network mode).

**Example: Publishing a contract**:
```bash
fdev publish \
    --code path/to/my_contract.wasm \
    --parameters path/to/params.json \
    contract \
    --state path/to/initial_state.json
```
- **`--code`**: path to your built WASM.
- **`--parameters`**: optional contract parameters.
- **`--state`**: optional initial state JSON or binary.

**Example: Publishing a delegate**:
```bash
fdev publish \
    --code path/to/my_delegate.wasm \
    --parameters delegate_params.json \
    delegate \
    [--nonce ... --cipher ...]
```
- If you skip `--nonce` and `--cipher`, a default local-only encryption is used (fine for tests, not recommended for production).

By default, `fdev publish` pushes in **local** mode unless you specify `--mode network` or set `MODE=network`.

---

## Updating a Contract

To push a state delta to an existing contract (identified by its Base58 key):

```bash
fdev execute update <BASE58_CONTRACT_KEY> \
    --delta path/to/delta.json \
    [--address 127.0.0.1 \
     --port 50509 \
     --release]
```

- **`--delta`**: The state delta file (JSON or binary).
- **`--release`** indicates you want to push to the network, instead of local-only.

---

## Contract State Builder Example

Sometimes you only want to build the “state artifact” for a contract or a web front-end. `fdev build` can do this if your `freenet.toml` includes a `state-sources` or webapp config.  
Older Freenet docs mention `build_state` as a separate script, but in practice you can do:

```bash
# Build a web-based front end
fdev build \
    --input-metadata-path /optional/metadata \
    --input-state-path contracts/freenet-microblogging/view/web \
    --output-file contracts/freenet-microblogging-web/freenet_microblogging_view \
    --package-type contract  # or 'webapp' if your config demands
```

Similarly, you can specify `--state` for a model-only contract:
```bash
fdev build \
    --input-state-path contracts/freenet-microblogging/model/ \
    --output-file contracts/freenet-microblogging-data/freenet_microblogging_model
```

*(Adjust paths, actual flags, and config to match your usage.)*

---

## Inspecting a Compiled WASM

`fdev inspect` prints metadata about a built WASM artifact, such as its hash, version, or a derived contract key.

Examples:

```bash
# Show code hash + contract API version
fdev inspect code path/to/contract.wasm

# Display the contract key (hash+empty params => key)
fdev inspect key path/to/contract.wasm

# For a delegate
fdev inspect delegate path/to/delegate.wasm
```

---

## Using the Local WASM Runtime (TUI)

`fdev wasm-runtime` provides a local, interactive environment to test your contract. For instance:

```bash
fdev wasm-runtime \
    --input-file /tmp/input.json \
    --deserialization-format json \
    --terminal-output
```
- The tool will connect to a local Freenet node by default (see `--mode`, `--address`, `--port` if needed).
- Commands you type—like `put`, `get`, `update`, or `exit`—are forwarded to the local node.
- Additional command data is read from the file specified by `--input-file`.

Common subcommands once inside the TUI:
```
help      # print instructions
put       # publish a fresh contract state
get       # retrieve the current contract state
update    # apply a delta to the contract state
exit      # quit TUI
```

---

## Querying Connected Peers

You can list a node’s open connections:

```bash
fdev query
```

It prints a table of peer identifiers and socket addresses for debugging or analysis.

---

## Testing (Single-Process or Multi-Process)

`fdev test` orchestrates an entire simulated network of Freenet nodes for end-to-end testing.

### Single Process

Spins up multiple in-memory nodes under one process:

```bash
fdev test \
    --gateways 2 \
    --nodes 10 \
    --events 100 \
    single-process
```
- **--gateways**: number of gateway nodes
- **--nodes**: number of normal nodes
- **--events**: random events (contract puts, updates, etc.)

### Multi-Process

```bash
fdev test \
    --gateways 2 \
    --nodes 10 \
    network
```
- One node acts as a **supervisor** (you run `fdev test ... network --mode supervisor`).
- Others run as **peers** (`--mode peer`) connecting to the supervisor. This can scale across multiple machines or containers.

*(See internal documentation or run `fdev test --help` for more detail.)*

---

## Network Metrics Server

```bash
fdev network-metrics-server [--log-directory path]
```
- Launches an HTTP server (default port `55010`) for collecting or pushing network stats and event logs.
- You can then visualize or store the data for debugging or simulation metrics analysis.

---

## Directory & Module Structure

Here’s a quick look at the major modules in the `fdev` crate:

```
fdev
├── src
│   ├── main.rs                # CLI entrypoint, top-level subcommands
│   ├── config.rs              # Config structs, subcommand definitions
│   ├── commands/              # 'put', 'update', connect to node, etc.
│   ├── build.rs               # Builds Rust WASM, web assets, etc.
│   ├── wasm_runtime/          # Local TUI runtime for testing contracts
│   ├── testing/               # Tools for single vs multi-process simulations
│   ├── network_metrics_server # Optional server for collecting network stats
│   └── util.rs                # Shared utility helpers
└── Cargo.toml
```

---

## Common Workflows

Below are a few typical steps you might perform:

1. **Create a new contract** (or webapp):
   ```bash
   fdev new contract
   ```
2. **Build**:
   ```bash
   fdev build
   ```
3. **Publish** (locally by default):
   ```bash
   fdev publish \
       --code build/freenet/my_contract.wasm \
       contract \
       --state initial_state.json
   ```
4. **Update** the contract:
   ```bash
   fdev execute update <contract_key> \
       --delta path/to/delta.json
   ```
5. **Inspect** a compiled WASM:
   ```bash
   fdev inspect code build/freenet/my_contract.wasm
   ```
6. **Run local TUI** (optionally):
   ```bash
   fdev wasm-runtime \
       --input-file my_input.json \
       --terminal-output \
       --deserialization-format json
   ```
7. **Check open peers**:
   ```bash
   fdev query
   ```
8. **Simulate** a small test network:
   ```bash
   fdev test --nodes 5 --gateways 1 single-process
   ```
9. **(Optional) Start metrics server**:
   ```bash
   fdev network-metrics-server --log-directory /path/to/logs
   ```

Feel free to run `fdev <subcommand> --help` for more details on any step. Enjoy building with Freenet!
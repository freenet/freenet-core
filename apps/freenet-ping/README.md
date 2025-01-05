# Freenet Ping Application

## Introduction

This is a simple ping application for Freenet designed to illustrate how easy it is to build a contract and a
command line app that deploys, modifies, and reads it. In this application:

- Every 1 second, the application generates a random name and sends an update request to the host.
- The application subscribes to these updates and logs "Hello {name}!" to the console.

Note: This application is for testing and demonstration purposes only. It does not perform any authorization for contract updates.

## Code Overview

- **[app/src/main.rs](https://github.com/freenet/freenet-core/blob/main/apps/freenet-ping/app/src/main.rs)**: Command line app
  that publishes the Ping contract, updates it periodically, and subscribes to changes.
- **[contracts/ping/src/lib.rs](https://github.com/freenet/freenet-core/blob/main/apps/freenet-ping/contracts/ping/src/lib.rs)**: Ping contract implementation.
- **[types/src/lib.rs](https://github.com/freenet/freenet-core/blob/main/apps/freenet-ping/types/src/lib.rs)**: Structs and other types that are shared between the command line app and the contract.

## Prerequisites

Ensure you have the following prerequisites installed:

### Rust and Cargo

- Install the latest version of Rust and Cargo:

  ```bash
  curl https://sh.rustup.rs -sSf | sh
  ```

### Additional Dependencies

#### Ubuntu

- Install the required libraries:

  ```bash
  sudo apt-get update
  sudo apt-get install libssl-dev libclang-dev pkg-config
  ```

#### macOS

- Install the required libraries using Homebrew:

  ```bash
  brew install openssl pkg-config
  ```

### Freenet Core

- Clone the Freenet Core repository and the stdlib submodule, and navigate to the application directory:

  ```bash
  git clone --recurse-submodules https://github.com/freenet/freenet-core.git
  cd freenet-core/apps/freenet-ping
  ```

### Freenet Development Tool and Kernel

- Install the Freenet development tool (`fdev`) and the Freenet kernel for local development:

  ```bash
  # You should be in freenet-core/apps/freenet-ping
  cargo install --path ../../crates/core
  cargo install --path ../../crates/fdev
  ```

### WebAssembly Target

- Add the WebAssembly target for Rust:

  ```bash
  rustup target add wasm32-unknown-unknown
  ```

## Setup Local Network

### Configuration Files

This project includes two Makefiles to help you set up and run a local Freenet network:

- `local-network.mk`: Located in `freenet-core/scripts`, this Makefile manages the local Freenet network (nodes and gateways).
- `run-ping.mk`: Builds and runs the ping application.

### Start Local Network
1. Navigate to the `freenet-core/scripts` directory:
   ```bash
   cd freenet-core/scripts
   ```

2. First, clean any previous setup and create necessary directories:
   ```bash
   make -f local-network.mk clean
   make -f local-network.mk setup N_GATEWAYS=1 N_NODES=2
   ```

3. Start the gateway:
   ```bash
   make -f local-network.mk start-gateways N_GATEWAYS=1
   ```

4. Start the nodes:
   ```bash
   make -f local-network.mk start-nodes N_NODES=2
   ```

5. Verify network status:
   ```bash
   make -f local-network.mk status N_GATEWAYS=1 N_NODES=2
   ```

The network will be configured with:
- 1 Gateway on port 3101
- Node 1: WebSocket port 3001, Network port 3102
- Node 2: WebSocket port 3002, Network port 3103

### Monitoring and Control

You can monitor your network using these commands:

```bash
# View logs from a specific node
make -f local-network.mk logs node=n1    # For node 1
make -f local-network.mk logs node=gw1   # For gateway 1

# Stop specific node
make -f local-network.mk stop-node node=n1

# Stop all nodes and gateways, clean up all files
make -f local-network.mk clean
```

## Build and Run Ping

1. Navigate to the `freenet-core/apps/freenet-ping` directory:
   ```bash
   cd freenet-core/apps/freenet-ping
   ```

1. View available options:
   ```bash
   make -f run-ping.mk help
   ```

2. Build the contract and application:
   ```bash
   make -f run-ping.mk build
   ```

3. Run ping towards node 1:
   ```bash
   make -f run-ping.mk run WS_PORT=3001
   ```

You will see something like this:
```bash
2024-05-14T15:33:20.685412Z  INFO freenet_ping: 154: put ping contract successfully! key=Cuj4LbFao6vzZ5VtvZAKZ64Y99qNh7MpTUdaCcEkU4oR
2024-05-14T15:33:20.729883Z  INFO freenet_ping: 146: Hello, ubiquitous-letters!
2024-05-14T15:33:22.154174Z  INFO freenet_ping: 146: Hello, unwieldy-level!
2024-05-14T15:33:23.668494Z  INFO freenet_ping: 146: Hello, woozy-pin!
```

You can customize the ping behavior with these parameters:
```bash
make -f run-ping.mk run WS_PORT=3001 FREQUENCY=2000 TTL=7200
```

### Cleanup

When you're done, clean up the network and build artifacts:

```bash
# Stop the network
make -f local-network.mk stop

# Clean up all files
make -f local-network.mk clean
make -f run-ping.mk clean
```

## Feedback

If you encounter any issues please let us know in our [Matrix](https://matrix.to/#/#freenet-locutus:matrix.org) channel or by submitting a [Github Issue](https://github.com/freenet/freenet-core/issues).
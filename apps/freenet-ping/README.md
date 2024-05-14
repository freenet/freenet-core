## Introduction

A simple ping application based on freenet. In this application, every 1 second, the application will generate a random name and send a update request to host.
And every 1.5 seconds, the application will fetch the latest names from host, and log "Hello {name}!" to console.

## Prerequisites

- If necessary install the latest version of Rust and Cargo (for Windows
  see [here](https://rustup.rs/)):

  ```bash
  curl https://sh.rustup.rs -sSf | sh
  ```

- (Ubuntu)

  ```bash
  sudo apt-get update
  sudo apt-get install libssl-dev libclang-dev pkg-config
  ```

- Clone the [freenet-core](https://github.com/freenet/freenet-core) repository and ensure you're in the same directory as this file:

  ```bash
  git clone git@github.com:freenet/freenet-core.git
  cd freenet-core/apps/freenet-ping
  ```

- Install the Freeenet development tool (fdev) and a working Freenet kernel that can be used for local development. Use cargo to install it:

  ```bash
  cargo install --path ../../crates/core
  cargo install --path ../../crates/fdev
  ```

- Add WebAssembly target

  ```bash
  rustup target add wasm32-unknown-unknown
  ```

## Build contract

```bash
cd contracts/ping && CARGO_TARGET_DIR=./target fdev build --features contract && cd -
```

## Run freenet locally

```bash
freenet local
```

## Run ping application

```bash
cd app && cargo install --path . && freenet-ping
```

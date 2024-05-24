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

## Build Contract

- Build the contract using the Freenet development tool:

  ```bash
  # You should be in freenet-core/apps/freenet-ping
  cd contracts/ping
  CARGO_TARGET_DIR=./target fdev build --features contract
  cd -
  ```

## Run Freenet Locally

- Start the Freenet network locally:

  ```bash
  freenet local
  ```

## Run Ping Application

- Run the ping application:

  ```bash
  cd app
  cargo install --path .
  freenet-ping
  ```
  
  You will see something like this:

  ```bash
    $ freenet-ping
    2024-05-14T15:33:20.685412Z  INFO freenet_ping: 154: put ping contract successfully! key=Cuj4LbFao6vzZ5VtvZAKZ64Y99qNh7MpTUdaCcEkU4oR
    2024-05-14T15:33:20.729883Z  INFO freenet_ping: 146: Hello, ubiquitous-letters!
    2024-05-14T15:33:22.154174Z  INFO freenet_ping: 146: Hello, unwieldy-level!
    2024-05-14T15:33:23.668494Z  INFO freenet_ping: 146: Hello, woozy-pin!
    2024-05-14T15:33:23.668504Z  INFO freenet_ping: 146: Hello, conscious-crayon!
    2024-05-14T15:33:25.153814Z  INFO freenet_ping: 146: Hello, unequal-kite!
    2024-05-14T15:33:26.668615Z  INFO freenet_ping: 146: Hello, absorbed-wren!
    2024-05-14T15:33:26.668628Z  INFO freenet_ping: 146: Hello, glib-disease!
    2024-05-14T15:33:28.154082Z  INFO freenet_ping: 146: Hello, secret-floor!
    2024-05-14T15:33:29.669125Z  INFO freenet_ping: 146: Hello, opposite-border!
    2024-05-14T15:33:29.669135Z  INFO freenet_ping: 146: Hello, plucky-stretch!
    ^C2024-05-14T15:33:29.713473Z  INFO freenet_ping: 169: shutting down...
    $ 
  ```

## Feedback

If you encounter any issues please let us know in our [Matrix](https://matrix.to/#/#freenet-locutus:matrix.org) channel or by submitting a [Github Issue](https://github.com/freenet/freenet-core/issues). 

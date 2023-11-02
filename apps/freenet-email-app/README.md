# Freenet Messaging App Setup Guide

Freenet Messaging App is a descentralized messaging application that runs on top of Freenet.

## Introduction

This guide will walk you through the setup and launch of the email application on Freenet.
Currently, Freenet is still under development, so the application will communicate with a
local node, which simulates a node on the network.

## Prerequisites

- Install the latest version of Rust and Cargo (for Windows
  see [here](https://rustup.rs/)):
  ```bash
  curl https://sh.rustup.rs -sSf | sh
  ```
- (Ubuntu)
  ```bash
  sudo apt-get update
  sudo apt-get install libssl-dev libclang-dev pkg-config
  ```
- Install the Freeenet development tool (fdev) and a working Freenet kernel that can be used for local development. Use cargo to install it:
  ```bash
  cargo install freenet
  cargo install fdev
  ```
- Install Dioxus-CLI, a GUI library for rust
  ```bash
  cargo install dioxus-cli
  ```
- Add WebAssembly target
  ```bash
  rustup target add wasm32-unknown-unknown
  ```
- Initializing & Fetching Submodules
  ```bash
  git submodule update --init --recursive
  ```
### Note about MacOS
Email account creation currently does not work on MacOS

## Prepare the Freenet email contracts and delegates

### Setup the identity management delegate

This delegate is responsible for managing a user's contacts. It can store and
retrieve contact information, and can be used by other components to send messages
to contacts.

This delegate is located inside the modules folder of freenet-core:

- `freenet-core/`
  - `modules/`
    - `identity-management/` <-- here is located the identity management delegate
      - `build/` <-- the generated folder that contains the compiled delegate binary with version + wasm code
      - `src/` <-- this folder contains the source code of the delegate
      - `Makefile` <-- this file contains the build instructions for the delegate
      - ...

Add the target directory for the project. This should be an absolute file path to freenet-core/target.
```bash
export CARGO_TARGET_DIR="... freenet-core/target"
```

To build the delegate, go to the `identity-management` folder and run the following command:

```bash
make build
```

This command will compile the delegate and generate a binary file inside the `build/freenet/` folder. It
generates the build folder if it doesn't exist. In addition, the build command will generate:

- `build/identity_management_code_hash` <-- this file contains the hash of the delegate's wasm code
- `build/identity-manager-key.private.pem` <-- this file contains a generated private key for the delegate
- `build/identity-manager-key.public.pem` <-- this file contains a generated public key for the delegate
- `build/identity-manager-params` <-- this file contains the parameters of the delegate, in that case, the delegate's
  SecretKey.

### Setup the anti-flood token system

The Antiflood Token System (AFT) is a decentralized system aimed to provide a simple, but general purpose solution
to flooding, denial-of-service attacks, and spam.

Is composed of one delegate, responsible for generating tokens, and a one contract that keeps track of the token
assignments.

This system is located inside the modules folder of freenet-core:

- `freenet-core/`
  - `modules/`
    - `antiflood-tokens/` <-- here is located the antiflood tokens system
      - `contracts/` <-- this folder contains the contract source code
        - `token-allocation-record/` <-- this folder contains the token allocation record contract
          - `build/` <-- the generated folder that contains the compiled contract binary with version + wasm
            code
          - `src/` <-- this folder contains the source code of the contract
          - ...
      - `delegates/` <-- this folder contains the delegate source code
        - `token-generator/` <-- this folder contains the token generator delegate
          - `build/` <-- the generated folder that contains the compiled delegate binary with version + wasm
            code
          - `src/` <-- this folder contains the source code of the delegate
          - ...
      - `Makefile` <-- this file contains the build instructions for building the delegate and the contract
      - ...

To build the Antiflood Token System, go to the `antiflood-tokens` folder and run the following command:

```bash
make build
```

This command will compile the contract and the delegate and generate a binary file inside the `build/freenet/` folder.
It generates the build folder if it doesn't exist.

- `contracts/token-allocation-record/build/identity_management_code_hash` <-- this file contains the hash of the
  contract's wasm code
- `delegates/token-generator/build/token_generator_code_hash` <-- this file contains the hash of the delegate's wasm
  code

### Setup the app

After building the previous delegates and contracts, what remains is to prepare the app, both the web
and the contract that will be responsible for maintaining the inboxes.

The app is located inside the `apps/freenet-email-app` folder:

- `freenet-core/`
  - `apps/`
    - `freenet-email-app/` <-- here is located the email app
      - `contracts/` <-- this folder contains the contract source code
        - `inbox/` <-- this folder contains the email inbox contract
          - `build/` <-- the generated folder that contains the compiled contract binary with version + wasm
            code
          - `src/` <-- this folder contains the source code of the contract
          - ...
      - `web/` <-- this folder contains the web app source code, web app built with Rust Dioxus framework
        - `build/` <-- the generated folder that contains the compiled web app binary with version + wasm
          code
        - `container/` <-- this folder contains the web contract container, a simple contract associated
          with the web app, as the state.
        - `src/` <-- this folder contains the source code of the web app
        - ...
      - `Makefile` <-- this file contains the build instructions for building and running the web app, and
        the local node.

To build the email application, go to the `apps/freenet-email-app` folder and run the following command:

```bash
make build
```

This command will compile the inbox and web contracts and the delegate, generate a binary files
inside the respective `build/freenet/` folders and publish the web contract. It generates the build folder if it doesn't
exist.

After building the app, what remains is to run the local node and the web app. To do that, run the following command:

1. Run the local node:
   ```bash
   make run-node
   ```
2. Run the web app:
   ```bash
   make run-web
   ```

During the development process, changes inside the web app will be automatically reloaded if it is running.

If you, instead, want to access the published app via the `build` command, go to your browser and write an URL like (with the node running):
`http://localhost:50509/contract/web/5zrr81Nbvk6PjkrXjXXFpDfrNZZvhx2JCc7BZTBHUKDo/`

The hash may be different and you can get it when you run the build command.

## Troubleshooting

### Freenet node throws an error when trying to find some contract or delegate

If any of the delegates or contracts definition change, is necessary to rebuild each of them and restart the node.
Probably the contract or delegate hash has changed and the node is trying to find the old version.

If the error persists, we recommend to make a clean build of all the delegates and contracts, and restart the node.

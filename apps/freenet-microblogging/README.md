# Freenet Microblogging

Freenet Microblogging App is a prototype decentralized twitter-like application that runs on top of Freenet.

## Introduction

This guide will walk you through the setup and launch of the microblogging application on Freenet.
Currently, Freenet is still under development, so the application will communicate with a
local node, which simulates a node on the network.

## Prerequisites

- Install the latest version of Rust and Cargo (for Windows
  see [here](https://rustup.rs/)):
  ```bash
  curl https://sh.rustup.rs -sSf | sh
  ```
- Install the Freeenet development tool (fdev) and a working Freenet kernel that can be used for local development. Use
  cargo to install it:
  ```bash
  cargo install freenet
  cargo install fdev
  ```

## Prepare the Freenet microblogging contracts

### Setup the app

Prepare the microblogging app web and posts contracts.

The app is located inside the `apps/freenet-microblogging` folder:

- `freenet-core/`
    - `apps/`
        - `freenet-microblogging/` <-- this folder contains the microblogging app
            - `contracts/` <-- this folder contains the contract source code
                - `posts/` <-- this folder contains the posts contract
                    - `build/` <-- the generated folder that contains the compiled contract binary with version + wasm
                      code
                    - `src/` <-- this folder contains the source code of the contract
                    - ...
            - `web/` <-- this folder contains the web app source code, web app built with node and webpack
                - `build/` <-- the generated folder that contains the compiled web app binary with version + wasm
                  code
                - `container/` <-- this folder contains the web contract container, a simple contract associated
                  with the web app, as the state.
                - `src/` <-- this folder contains the source code of the web app
                - ...
            - `Makefile` <-- this file contains the build instructions for building and running the web app, and
              the local node.

To build the microblogging application, go to the `apps/freenet-microblogging` folder and run the following command:

```bash
make build
```

This command will compile the posts and web contracts, generate a binary files
inside the respective `build/freenet/` folders and publish both contracts. It generates the build folder if it doesn't
exist.

After building the app, what remains is to run the local node and use browser to load the web app contract. To do
that, run the following command:

1. Run the local node:
   ```bash
   make node
   ```
2. Browse to the following URL to load the microblogging web, note that the hash may be different:
   ```
   http://127.0.0.1:50509/contract/web/9zaPEBmmMAF3eKy7NnuPUit2j1annxtVY226DWGLzEFN/
   ```

The hash may be different and you can get it when you run the build command.

During the development process, changes inside the web need to be recompiled and republished. To do that,
repeat the previous steps.

## Troubleshooting

### Freenet node throws an error when trying to find some contract

If any of the contracts definition change, is necessary to rebuild each of them and restart the node.
Probably the contract hash has changed and the node is trying to find the old version.

If the error persists, we recommend to make a clean build of all the contracts, and restart the node.

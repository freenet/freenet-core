# Building with Docker Images

## Prerequisites

Make sure docker is installed and working, and has the `docker compose` command.

## Contract DB Storage

The docker image stores its data at `/root/.local/share/freenet` inside the
container. This is mapped to `/tmp/freenet-docker` outside the container.

## Build the base docker image of Locutus

All the docker related files are in the `docker` subdirectory.

Requires that Docker be installed and working. Then, in the root directory of the repo:

To build the docker freenet container:

```sh
cd docker
docker compose build
```

## Running Freenet from the docker image

Note: Currently the node will not pick up new contracts when they are published.
Make sure the node is stopped and re-started after new contracts are added.

```sh
docker compose up
```

## Running the `fdev` tool from the docker image

There is a shell script in the `docker` sub directory which makes running `fdev`
from inside the container against source held outside the container easier. It
behaves just like the `fdev` tool, except as stated below.

### Getting help from `fdev`

```sh
/location/of/freenet/docker/fdev.sh --help
```

### Building Contracts

To BUILD a contract, we need to define 1 or 2 env vars:

- `PROJECT_SRC_DIR` = Root of the Project being build and defaults to `pwd` so
  if you are in your project root, no need to set it.
- `CONTRACT_SRC_DIR` = Relative DIR under PROJECT_SRC_DIR to the Contract to
  build. eg, `./web` would build a contract in the `web` subdirectory of the
  `PROJECT_SRC_DIR`. Note: This MUST be a subdirectory.

eg (in the root of the project):

```sh
CONTRACT_SRC_DIR=./web /location/of/freenet/docker/fdev.sh build
```

### Publishing Contracts

From the base directory of the contract project.

```sh
/location/of/freenet/docker/fdev.sh publish --code target/wasm32-unknown-unknown/release/freenet_microblogging_web.wasm --state web/build/freenet/contract-state
```

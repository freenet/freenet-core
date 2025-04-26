#!/bin/bash

export RUST_BACKTRACE=1
export RUST_LOG="info,freenet=debug,freenet-stdlib=debug,fdev=debug"

BASE_DIR="/root/.cache/freenet"
NODE_DIR="${BASE_DIR}/node"

freenet network \
  --config-dir "$BASE_DIR" \
  --data-dir "$NODE_DIR" \
  --ws-api-port "${WS_API_PORT:-50509}"
#!/bin/bash
export RUST_BACKTRACE=1
export RUST_LOG="info,freenet=debug,freenet-stdlib=debug,fdev=debug"

# Check if required parameters are provided
if [ -z "$1" ] || [ -z "$2" ]; then
    echo "Usage: $0 <transport-keypair-dir> <public-network-address> [public-network-port]"
    exit 1
fi

TRANSPORT_KEYPAIR="$1"
PUBLIC_NETWORK_ADDRESS="$2"
PUBLIC_NETWORK_PORT="${3:-31337}"
NETWORK_PORT="${4:-31337}"

freenet network \
    --skip-load-from-network \
    --is-gateway \
    --transport-keypair "$TRANSPORT_KEYPAIR" \
    --network-port "$NETWORK_PORT" \
    --public-network-address "$PUBLIC_NETWORK_ADDRESS" \
    --public-network-port "$PUBLIC_NETWORK_PORT"

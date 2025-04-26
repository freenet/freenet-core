#!/bin/bash
export RUST_BACKTRACE=1
export RUST_LOG="info,freenet=debug,freenet-stdlib=debug,fdev=debug"

# Check if required environment variables are set
if [ -z "$TRANSPORT_KEYPAIR" ] || [ -z "$PUBLIC_NETWORK_ADDRESS" ]; then
    echo "Error: Required environment variables are not set!"
    exit 1
fi

echo "Starting Freenet Gateway with the following parameters:"
echo "  Transport Keypair: $TRANSPORT_KEYPAIR"
echo "  Public Network Address: $PUBLIC_NETWORK_ADDRESS"
echo "  Public Network Port: $PUBLIC_NETWORK_PORT"
echo "  Network Port: $NETWORK_PORT"

freenet network \
    --skip-load-from-network \
    --is-gateway \
    --transport-keypair "$TRANSPORT_KEYPAIR" \
    --network-port "$NETWORK_PORT" \
    --public-network-address "$PUBLIC_NETWORK_ADDRESS" \
    --public-network-port "$PUBLIC_NETWORK_PORT"

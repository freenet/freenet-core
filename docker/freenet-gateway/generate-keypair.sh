#!/bin/bash

KEYS_DIR="/root/.cache/freenet/keys"
KEYPAIR_FILE="$KEYS_DIR/transport-keypair.pem"

# Ensure the keys directory exists
mkdir -p "$KEYS_DIR"

# Check if the keypair already exists
if [ -f "$KEYPAIR_FILE" ]; then
    echo "Keypair already exists at $KEYPAIR_FILE. Skipping generation."
else
    echo "Generating new transport keypair..."
    openssl genpkey -algorithm RSA -out "$KEYPAIR_FILE" -pkeyopt rsa_keygen_bits:4096
    chmod 600 "$KEYPAIR_FILE"
    echo "Keypair generated at $KEYPAIR_FILE."
fi
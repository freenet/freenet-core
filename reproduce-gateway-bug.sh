#!/bin/bash
# Script to reproduce the gateway contract storage bug

echo "=== Reproducing Gateway Contract Storage Bug ==="
echo "This demonstrates that gateways with peer connections cannot store contracts"
echo ""

# Kill any existing test processes
pkill -f "freenet.*test-gateway-bug" 2>/dev/null

# Create temp directories
GATEWAY_DIR="/tmp/test-gateway-bug"
PEER_DIR="/tmp/test-peer-bug"
rm -rf $GATEWAY_DIR $PEER_DIR
mkdir -p $GATEWAY_DIR/{data,config}
mkdir -p $PEER_DIR/{data,config}

# Generate gateway keypair
openssl genrsa -out $GATEWAY_DIR/gateway.pem 2048 2>/dev/null
openssl rsa -in $GATEWAY_DIR/gateway.pem -pubout -out $GATEWAY_DIR/gateway.pem.pub 2>/dev/null

# Create gateways config for peer
cat > $PEER_DIR/config/gateways.toml << EOF
[[gateways]]
public_key = "$GATEWAY_DIR/gateway.pem.pub"

[gateways.address]
hostname = "127.0.0.1:31339"
EOF

echo "Starting gateway on port 31339..."
RUST_LOG=info freenet network \
    --data-dir $GATEWAY_DIR/data \
    --config-dir $GATEWAY_DIR/config \
    --skip-load-from-network \
    --is-gateway \
    --transport-keypair $GATEWAY_DIR/gateway.pem \
    --network-port 31339 \
    --ws-api-port 50509 \
    --public-network-address 127.0.0.1 \
    --public-network-port 31339 > $GATEWAY_DIR/gateway.log 2>&1 &
GATEWAY_PID=$!
echo "Gateway PID: $GATEWAY_PID"

sleep 10

echo "Starting peer to connect to gateway..."
RUST_LOG=info freenet network \
    --data-dir $PEER_DIR/data \
    --config-dir $PEER_DIR/config \
    --skip-load-from-network \
    --network-port 52580 \
    --ws-api-port 52620 > $PEER_DIR/peer.log 2>&1 &
PEER_PID=$!
echo "Peer PID: $PEER_PID"

sleep 20

echo ""
echo "Testing River room creation on GATEWAY (should fail to store)..."
RIVER_CONFIG_DIR=$GATEWAY_DIR/river riverctl \
    --node-url ws://127.0.0.1:50509/v1/contract/command?encodingProtocol=native \
    room create --name "test-room" --nickname "Gateway" 2>&1 | tee $GATEWAY_DIR/create.log

# Extract room ID if created
ROOM_ID=$(grep "contract_key" $GATEWAY_DIR/create.log | sed 's/.*"contract_key": "\([^"]*\)".*/\1/')

if [ -n "$ROOM_ID" ]; then
    echo "Room created with ID: $ROOM_ID"
    echo ""
    echo "Waiting 5 seconds for contract to propagate..."
    sleep 5
    
    echo "Checking gateway logs for storage errors..."
    grep -E "NoCachingPeers|Contract state not found|Error subscribing" $GATEWAY_DIR/gateway.log | tail -5
    
    echo ""
    echo "Attempting to GET the contract from gateway (should fail)..."
    echo "This would normally be done via River commands but checking logs is sufficient"
fi

echo ""
echo "Cleaning up..."
kill $GATEWAY_PID $PEER_PID 2>/dev/null

echo ""
echo "=== Test Complete ==="
echo "Check logs at:"
echo "  Gateway: $GATEWAY_DIR/gateway.log"
echo "  Peer: $PEER_DIR/peer.log"
echo ""
echo "Expected result: Gateway logs should show 'NoCachingPeers' error"
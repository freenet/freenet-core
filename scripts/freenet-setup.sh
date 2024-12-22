#!/bin/bash
# freenet-setup.sh - Unified Gateway and Node setup script

# Check if required parameters are provided
if [ -z "$1" ] || [ -z "$2" ]; then
    echo "Usage: $0 <name> <type>"
    echo "Example: $0 my-gw gateway"
    exit 1
fi

NAME="$1"
TYPE="$2"

if [[ "$TYPE" != "gateway" && "$TYPE" != "node" ]]; then
    echo "Error: Type must be either 'gateway' or 'node'"
    exit 1
fi

# Base Setup
REPO_DIR=~/freenet/freenet-core
BASE_DIR="$HOME/.cache/freenet"
KEYS_DIR="$BASE_DIR/keys"
LOGS_DIR="$BASE_DIR/logs"
PID_DIR="$BASE_DIR/pids"
NODE_DIR="$BASE_DIR/$NAME"

# GW public IP
PUBLIC_IP=100.27.151.80
PUBLIC_PORT=31337

mkdir -p "$KEYS_DIR" "$LOGS_DIR" "$PID_DIR" "$NODE_DIR"

# Clone or Update Repository
if [ ! -d "$REPO_DIR" ]; then
    echo "Cloning freenet-core repository..."
    git clone https://github.com/freenet/freenet-core.git "$REPO_DIR"
    cd "$REPO_DIR" && git submodule update --init --recursive
else
    echo "Updating freenet-core repository..."
    cd "$REPO_DIR" && git pull && git submodule update --init --recursive
fi

# Generate Keys if not exists
KEY_FILE="$KEYS_DIR/${NAME}_private_key.pem"
if [ ! -f "$KEY_FILE" ]; then
    echo "Generating keys for $TYPE..."
    openssl genpkey -algorithm RSA -out "$KEY_FILE"
    openssl rsa -pubout -in "$KEY_FILE" -out "$KEYS_DIR/${NAME}_public_key.pem"
else
    echo "Keys already exist. Skipping key generation."
fi

# Configure gateways.toml if node
if [[ "$TYPE" == "node" ]]; then
    GATEWAY_CONFIG="$BASE_DIR/gateways.toml"
    echo "Configuring gateways.toml for node..."
    cat > "$GATEWAY_CONFIG" << EOL
# Freenet Gateway Configuration

[[gateways]]
address = { host_address = "$PUBLIC_IP:$PUBLIC_PORT" }
public_key = "$KEYS_DIR/freenet-gw_public_key.pem"
EOL
fi

# Create startup script
START_SCRIPT=$REPO_DIR/scripts/start-${NAME}.sh
cat > "$START_SCRIPT" << EOL
#!/bin/bash
export RUST_BACKTRACE=1
export RUST_LOG="info,freenet=debug,freenet-stdlib=debug,fdev=debug"

freenet network \\
  $([[ "$TYPE" == "node" ]] && echo "--config-dir $BASE_DIR") \\
  --ws-api-port 3001 \\
  --db-dir "$NODE_DIR" \\
  --transport-keypair "$KEY_FILE" \\
  --network-port $PUBLIC_PORT \\
  $([[ "$TYPE" == "gateway" ]] && echo "--is-gateway --public-network-address $PUBLIC_IP --public-network-port $PUBLIC_PORT") \\
  1> >(stdbuf -o0 sed 's/\x1b\[[0-9;]*m//g' >> "$LOGS_DIR/${NAME}.log") 2>&1 &

echo \$! > "$PID_DIR/${NAME}.pid"
echo "$TYPE $NAME started on $PUBLIC_IP:$PUBLIC_PORT (PID: \$!)"
EOL

chmod +x "$START_SCRIPT"

echo "$TYPE $NAME setup complete!"
echo "Start it with: $START_SCRIPT"

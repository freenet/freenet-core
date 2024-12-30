#!/bin/bash
# freenet-setup.sh - Unified Gateway and Node setup script

# Check if required parameters are provided
# Ahora sólo esperamos 3 parámetros: <name> <type> <gw-host>
if [ -z "$1" ] || [ -z "$2" ]; then
    echo "Usage: $0 <name> <type> <gw-host>"
    echo "Example: $0 my-node node vega.locut.us"
    exit 1
fi

NAME="$1"
TYPE="$2"
GW_HOST="$3"

# Validar el TYPE
if [[ "$TYPE" != "gateway" && "$TYPE" != "node" ]]; then
    echo "Error: Type must be either 'gateway' or 'node'."
    exit 1
fi

# Si es node, necesitamos GW_HOST (aunque ya lo recibimos en $3).
if [[ "$TYPE" == "node" && -z "$GW_HOST" ]]; then
    echo "Error: <gw-host> parameter is required for node setup."
    exit 1
fi

# Get public IP
PUBLIC_IP=$(curl -s ifconfig.me)
if [ -z "$PUBLIC_IP" ]; then
    echo "Error: Could not determine public IP."
    exit 1
fi

# Base Setup
FREENET_DIR=~/freenet
REPO_DIR="$FREENET_DIR/freenet-core"
BASE_DIR="$HOME/.cache/freenet"
KEYS_DIR="$BASE_DIR/keys"
LOGS_DIR="$BASE_DIR/logs"
PID_DIR="$BASE_DIR/pids"
NODE_DIR="$BASE_DIR/$NAME"

mkdir -p "$KEYS_DIR" "$LOGS_DIR" "$PID_DIR" "$NODE_DIR"

# Clone or Update Repository
if [ ! -d "$REPO_DIR" ]; then
    echo "Cloning freenet-core repository..."
    git clone https://github.com/freenet/freenet-core.git "$REPO_DIR"
    cd "$REPO_DIR" && git submodule update --init --recursive
else
    echo "Updating freenet-core repository..."
    cd "$REPO_DIR" && git pull
    # git submodule update --init --recursive
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

# Si es nodo, creamos el archivo gateways.toml (sin descargar nada por SSH/SCP).
if [[ "$TYPE" == "node" ]]; then
    GATEWAY_CONFIG="$BASE_DIR/gateways.toml"
    echo "Configuring gateways.toml for node..."

    # Check if GW_HOST is in the format 'IP:PORT' or just a hostname
    if [[ "$GW_HOST" =~ ^[0-9]{1,3}(\.[0-9]{1,3}){3}:[0-9]{1,5}$ ]]; then
        # Matches a simplistic IPv4:PORT pattern, e.g. 192.168.1.100:31337
        cat > "$GATEWAY_CONFIG" << EOL
# Freenet Gateway Configuration

[[gateways]]
address = { host_address = "$GW_HOST" }
public_key = "$FREENET_DIR/freenet-gw_public_key.pem"
EOL
    else
        # Otherwise, assume it's a hostname
        cat > "$GATEWAY_CONFIG" << EOL
# Freenet Gateway Configuration

[[gateways]]
address = { hostname = "$GW_HOST" }
public_key = "$FREENET_DIR/freenet-gw_public_key.pem"
EOL
    fi

    echo "Gateways.toml successfully configured."
fi

# Create startup script
START_SCRIPT=~/freenet/start-${NAME}.sh
cat > "$START_SCRIPT" << EOL
#!/bin/bash
export RUST_BACKTRACE=1
export RUST_LOG="info,freenet=debug,freenet-stdlib=debug,fdev=debug"

freenet network \
  $([[ "$TYPE" == "node" ]] && echo "--config-dir $BASE_DIR") \
  --ws-api-port $([[ "$TYPE" == "gateway" ]] && echo "3001" || echo "3002") \
  --db-dir "$NODE_DIR" \
  --transport-keypair "$KEY_FILE" \
  --network-port 31337 \
  $([[ "$TYPE" == "gateway" ]] && echo "--is-gateway --public-network-address $PUBLIC_IP --public-network-port 31337") \
  1> >(stdbuf -o0 sed 's/\x1b\[[0-9;]*m//g' >> "$LOGS_DIR/${NAME}.log") 2>&1 &

echo \$! > "$PID_DIR/${NAME}.pid"
echo "$TYPE $NAME started on $PUBLIC_IP:31337 (PID: \$!)"
EOL

chmod +x "$START_SCRIPT"

echo "$TYPE $NAME setup complete!"
echo "Start it with: $START_SCRIPT"

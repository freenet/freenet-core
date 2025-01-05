#!/bin/bash
# freenet-node-setup.sh - Setup script for Freenet nodes

# Check if required parameters are provided
if [ -z "$1" ]; then
    echo "Usage: $0 <name> [ws-api-port]"
    echo "Example: $0 my-node 50509"
    exit 1
fi

NAME="$1"
WS_API_PORT="${3:-50509}"

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
fi

# Create startup script
START_SCRIPT=~/freenet/init-${NAME}.sh
cat > "$START_SCRIPT" << EOL
#!/bin/bash
export RUST_BACKTRACE=1
export RUST_LOG="info,freenet=debug,freenet-stdlib=debug,fdev=debug"

freenet network \
  --ws-api-port "$WS_API_PORT" \
  1> >(stdbuf -o0 sed 's/\x1b\[[0-9;]*m//g' >> "$LOGS_DIR/${NAME}.log") 2>&1 &

echo \$! > "$PID_DIR/${NAME}.pid"
EOL

chmod +x "$START_SCRIPT"

echo "$TYPE $NAME setup complete!"
echo "Start it with: $START_SCRIPT"

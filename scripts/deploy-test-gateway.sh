#!/bin/bash
set -e

# Test Gateway Deployment Script
# Deploys instrumented Freenet gateway to vega for debugging keep-alive issues

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

# Configuration
VEGA_HOST="100.27.151.80"
VEGA_USER="ian"
TEST_PORT="31338"
TEST_DIR="/home/ian/freenet-test-gateway"
CONFIG_DIR="/home/ian/.config/freenet-test"
DATA_DIR="/home/ian/.local/share/freenet-test"
BINARY_NAME="freenet"

echo "=== Freenet Test Gateway Deployment Script ==="
echo "Target: $VEGA_USER@$VEGA_HOST"
echo "Port: $TEST_PORT"
echo ""

# Step 1: Build the binary with instrumentation
echo "📦 Building instrumented binary..."
cd "$PROJECT_ROOT"

# Add extra logging for keep-alive debugging
export RUST_LOG="freenet=debug,freenet::transport=trace,freenet::transport::peer_connection=trace"
# Build for generic x86-64 to ensure compatibility with vega
export RUSTFLAGS="-C target-cpu=x86-64"

cargo build --release --bin freenet
if [ $? -ne 0 ]; then
    echo "❌ Build failed"
    exit 1
fi
echo "✅ Build successful"

# Step 2: Prepare deployment package
echo ""
echo "📋 Preparing deployment package..."
TEMP_DIR=$(mktemp -d)
cp target/release/freenet "$TEMP_DIR/"
cp scripts/monitor-test-gateway.sh "$TEMP_DIR/" 2>/dev/null || true

# Create startup script
cat > "$TEMP_DIR/start-test-gateway.sh" << 'EOF'
#!/bin/bash
set -e

TEST_PORT="${1:-31338}"
LOG_FILE="freenet-test-gateway.log"
CONFIG_DIR="/home/ian/.config/freenet-test"
DATA_DIR="/home/ian/.local/share/freenet-test"

echo "Starting test gateway on port $TEST_PORT..."
echo "Config dir: $CONFIG_DIR"
echo "Data dir: $DATA_DIR"

# Kill any existing test gateway
pkill -f "freenet.*--port $TEST_PORT" || true
sleep 2

# Create isolated directories for test gateway
mkdir -p "$CONFIG_DIR"
mkdir -p "$DATA_DIR"

# Start with maximum debugging
export RUST_LOG="freenet=trace,freenet::transport=trace,freenet::transport::peer_connection=trace,freenet::transport::connection_handler=trace"
export RUST_BACKTRACE=1

# Set isolated config and data directories to avoid conflicts with production gateway
export FREENET_CONFIG_DIR="$CONFIG_DIR"
export FREENET_DATA_DIR="$DATA_DIR"

# Run gateway with instrumentation and isolated paths
nohup ./freenet network \
    --registry-port 50509 \
    --gateways dev \
    --id test-gateway \
    --port $TEST_PORT \
    --public-address "100.27.151.80:$TEST_PORT" \
    --config-dir "$CONFIG_DIR" \
    --data-dir "$DATA_DIR" \
    > "$LOG_FILE" 2>&1 &

PID=$!
echo "Started test gateway with PID: $PID"
echo $PID > test-gateway.pid

# Wait and check if it's running
sleep 3
if ps -p $PID > /dev/null; then
    echo "✅ Test gateway is running"
    echo "📄 Logs: tail -f $LOG_FILE"
else
    echo "❌ Test gateway failed to start"
    tail -20 "$LOG_FILE"
    exit 1
fi
EOF

chmod +x "$TEMP_DIR/start-test-gateway.sh"

# Step 3: Check for production gateway
echo ""
echo "🔍 Checking for production gateway..."
PROD_GATEWAY_STATUS=$(ssh "$VEGA_USER@$VEGA_HOST" "ps aux | grep -E 'freenet.*--port 31337' | grep -v grep || true")
if [ -n "$PROD_GATEWAY_STATUS" ]; then
    echo "✅ Production gateway is running (good - no conflict)"
    echo "$PROD_GATEWAY_STATUS" | head -1
fi

# Step 4: Deploy to vega
echo ""
echo "🚀 Deploying to vega..."

# Create test directory on vega
ssh "$VEGA_USER@$VEGA_HOST" "mkdir -p $TEST_DIR"

# Copy files
scp "$TEMP_DIR/freenet" "$VEGA_USER@$VEGA_HOST:$TEST_DIR/"
scp "$TEMP_DIR/start-test-gateway.sh" "$VEGA_USER@$VEGA_HOST:$TEST_DIR/"

echo "✅ Files deployed"

# Step 5: Stop existing test gateway
echo ""
echo "🛑 Stopping existing test gateway..."
ssh "$VEGA_USER@$VEGA_HOST" "cd $TEST_DIR && pkill -f 'freenet.*--port $TEST_PORT' || true"
sleep 2

# Step 6: Start new test gateway
echo ""
echo "▶️  Starting test gateway..."
ssh "$VEGA_USER@$VEGA_HOST" "cd $TEST_DIR && ./start-test-gateway.sh $TEST_PORT"

# Step 7: Verify it's running
echo ""
echo "🔍 Verifying gateway status..."
sleep 3

if ssh "$VEGA_USER@$VEGA_HOST" "cd $TEST_DIR && ps -p \$(cat test-gateway.pid 2>/dev/null) > /dev/null 2>&1"; then
    echo "✅ Test gateway is running successfully!"
    
    # Show process info
    echo ""
    echo "Process info:"
    ssh "$VEGA_USER@$VEGA_HOST" "cd $TEST_DIR && ps -fp \$(cat test-gateway.pid)"
    
    # Verify isolation
    echo ""
    echo "Verifying isolation from production gateway:"
    echo "Test gateway config: $CONFIG_DIR"
    echo "Test gateway data: $DATA_DIR"
    ssh "$VEGA_USER@$VEGA_HOST" "ls -la $CONFIG_DIR 2>/dev/null || echo 'Config dir not yet created'"
    
else
    echo "❌ Test gateway is not running!"
    echo "Recent logs:"
    ssh "$VEGA_USER@$VEGA_HOST" "cd $TEST_DIR && tail -20 freenet-test-gateway.log"
    exit 1
fi

# Cleanup
rm -rf "$TEMP_DIR"

echo ""
echo "✅ Deployment complete!"
echo ""
echo "Test gateway endpoints:"
echo "  - UDP: $VEGA_HOST:$TEST_PORT"
echo "  - Logs: ssh $VEGA_USER@$VEGA_HOST 'tail -f $TEST_DIR/freenet-test-gateway.log'"
echo ""
echo "To test connection:"
echo "  cargo run --bin freenet -- network --gateways 100.27.151.80:$TEST_PORT"
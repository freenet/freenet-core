#!/bin/bash

# Keep-Alive Test Client
# Connects to test gateway and monitors keep-alive behavior

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

# Default configuration
GATEWAY_HOST="100.27.151.80"
GATEWAY_PORT="31338"
TEST_DURATION="300"  # 5 minutes default
LOG_FILE="/tmp/keepalive-test-$(date +%Y%m%d-%H%M%S).log"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --gateway|-g)
            GATEWAY_HOST="$2"
            shift 2
            ;;
        --port|-p)
            GATEWAY_PORT="$2"
            shift 2
            ;;
        --duration|-d)
            TEST_DURATION="$2"
            shift 2
            ;;
        --help|-h)
            echo "Usage: $0 [options]"
            echo "Options:"
            echo "  -g, --gateway HOST    Gateway host (default: $GATEWAY_HOST)"
            echo "  -p, --port PORT       Gateway port (default: $GATEWAY_PORT)"
            echo "  -d, --duration SECS   Test duration in seconds (default: $TEST_DURATION)"
            echo "  -h, --help            Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo "=== Keep-Alive Test Client ==="
echo "Gateway: $GATEWAY_HOST:$GATEWAY_PORT"
echo "Duration: $TEST_DURATION seconds"
echo "Log file: $LOG_FILE"
echo ""

# Build if needed
if [ ! -f "$PROJECT_ROOT/target/release/freenet" ]; then
    echo "Building freenet binary..."
    cd "$PROJECT_ROOT"
    cargo build --release --bin freenet
fi

# Create monitoring script
cat > /tmp/monitor-keepalive.sh << 'EOF'
#!/bin/bash
LOG_FILE="$1"
START_TIME=$(date +%s)

echo "Monitoring keep-alives..."
echo "Time | Event | Details"
echo "----------------------------------------"

tail -f "$LOG_FILE" | while IFS= read -r line; do
    CURRENT_TIME=$(date +%s)
    ELAPSED=$((CURRENT_TIME - START_TIME))
    
    # Extract timestamp from log if present
    if [[ "$line" =~ ([0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}) ]]; then
        TIMESTAMP="${BASH_REMATCH[1]}"
    else
        TIMESTAMP=$(date '+%H:%M:%S.%3N')
    fi
    
    # Detect keep-alive events
    if [[ "$line" =~ "Sending KeepAlive" ]] || [[ "$line" =~ "KEEP_ALIVE_SENT" ]]; then
        echo -e "${ELAPSED}s | ${GREEN}SENT${NC} | Keep-alive sent at $TIMESTAMP"
    elif [[ "$line" =~ "Received KeepAlive" ]] || [[ "$line" =~ "KEEP_ALIVE_RECEIVED" ]]; then
        echo -e "${ELAPSED}s | ${GREEN}RECV${NC} | Keep-alive received at $TIMESTAMP"
    elif [[ "$line" =~ "keep.alive.*timeout" ]] || [[ "$line" =~ "No response to keep-alive" ]]; then
        echo -e "${ELAPSED}s | ${RED}TIMEOUT${NC} | Keep-alive timeout at $TIMESTAMP"
    elif [[ "$line" =~ "Connection closed" ]] || [[ "$line" =~ "connection.*lost" ]]; then
        echo -e "${ELAPSED}s | ${RED}CLOSED${NC} | Connection closed at $TIMESTAMP"
        echo ""
        echo "❌ Connection failed after $ELAPSED seconds"
        exit 1
    elif [[ "$line" =~ "Connected to gateway" ]] || [[ "$line" =~ "connection.*established" ]]; then
        echo -e "${ELAPSED}s | ${GREEN}CONNECTED${NC} | Connected at $TIMESTAMP"
    fi
done
EOF
chmod +x /tmp/monitor-keepalive.sh

# Start monitoring in background
/tmp/monitor-keepalive.sh "$LOG_FILE" &
MONITOR_PID=$!

# Function to cleanup on exit
cleanup() {
    echo ""
    echo "Cleaning up..."
    kill $MONITOR_PID 2>/dev/null
    kill $CLIENT_PID 2>/dev/null
    rm -f /tmp/monitor-keepalive.sh
    
    # Show summary
    echo ""
    echo "=== Test Summary ==="
    echo "Keep-alives sent: $(grep -c "KEEP_ALIVE_SENT\|Sending KeepAlive" "$LOG_FILE" 2>/dev/null || echo 0)"
    echo "Keep-alives received: $(grep -c "KEEP_ALIVE_RECEIVED\|Received KeepAlive" "$LOG_FILE" 2>/dev/null || echo 0)"
    echo "Timeouts: $(grep -c "keep.alive.*timeout\|No response to keep-alive" "$LOG_FILE" 2>/dev/null || echo 0)"
    echo "Log saved to: $LOG_FILE"
}
trap cleanup EXIT

# Run test client
echo "Starting test client..."
export RUST_LOG="freenet=debug,freenet::transport=trace,freenet::transport::peer_connection=trace"

cd "$PROJECT_ROOT"
timeout "$TEST_DURATION" ./target/release/freenet network \
    --id "keepalive-test-$(date +%s)" \
    --port 0 \
    --gateways "$GATEWAY_HOST:$GATEWAY_PORT" \
    > "$LOG_FILE" 2>&1 &

CLIENT_PID=$!

# Wait for client to finish or timeout
wait $CLIENT_PID
EXIT_CODE=$?

if [ $EXIT_CODE -eq 124 ]; then
    echo ""
    echo -e "${GREEN}✅ Test completed successfully!${NC}"
    echo "Connection remained stable for $TEST_DURATION seconds"
else
    echo ""
    echo -e "${RED}❌ Test failed with exit code: $EXIT_CODE${NC}"
fi

# Show last few keep-alive events
echo ""
echo "Last keep-alive events:"
grep -E "keep.alive|KEEP_ALIVE|KeepAlive" "$LOG_FILE" | tail -10
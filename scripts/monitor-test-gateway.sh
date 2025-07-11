#!/bin/bash

# Test Gateway Monitoring Script
# Real-time monitoring of keep-alive behavior and connection health

VEGA_HOST="100.27.151.80"
VEGA_USER="ubuntu"
TEST_DIR="/home/ubuntu/freenet-test-gateway"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo "=== Freenet Test Gateway Monitor ==="
echo "Monitoring keep-alive behavior on $VEGA_HOST"
echo ""

# Function to format log lines with colors
format_logs() {
    while IFS= read -r line; do
        # Add timestamp if not present
        if [[ ! "$line" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2} ]]; then
            timestamp=$(date '+%Y-%m-%d %H:%M:%S.%3N')
            line="$timestamp | $line"
        fi
        
        # Color code based on content
        if [[ "$line" =~ "KEEP_ALIVE" ]] || [[ "$line" =~ "keep.alive" ]] || [[ "$line" =~ "KeepAlive" ]]; then
            echo -e "${GREEN}[KEEP-ALIVE]${NC} $line"
        elif [[ "$line" =~ "ERROR" ]] || [[ "$line" =~ "error" ]]; then
            echo -e "${RED}[ERROR]${NC} $line"
        elif [[ "$line" =~ "WARN" ]] || [[ "$line" =~ "warn" ]]; then
            echo -e "${YELLOW}[WARN]${NC} $line"
        elif [[ "$line" =~ "connection.*established" ]] || [[ "$line" =~ "Connected" ]]; then
            echo -e "${BLUE}[CONNECT]${NC} $line"
        elif [[ "$line" =~ "connection.*closed" ]] || [[ "$line" =~ "Disconnected" ]]; then
            echo -e "${RED}[DISCONNECT]${NC} $line"
        elif [[ "$line" =~ "CRITICAL" ]]; then
            echo -e "${RED}[CRITICAL]${NC} $line"
        else
            echo "$line"
        fi
    done
}

# Function to show keep-alive summary
show_summary() {
    echo ""
    echo "=== Keep-Alive Summary ==="
    ssh "$VEGA_USER@$VEGA_HOST" "cd $TEST_DIR && grep -i 'keep.alive' freenet-test-gateway.log | tail -20" | \
    while IFS= read -r line; do
        if [[ "$line" =~ ([0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}) ]]; then
            echo "  ${BASH_REMATCH[1]} - $line"
        fi
    done
}

# Parse command line options
FILTER=""
SUMMARY_ONLY=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --keep-alive-only|-k)
            FILTER="keep.alive|KEEP_ALIVE|KeepAlive"
            shift
            ;;
        --errors-only|-e)
            FILTER="ERROR|error|WARN|warn|CRITICAL"
            shift
            ;;
        --connections-only|-c)
            FILTER="connection|Connected|Disconnected"
            shift
            ;;
        --summary|-s)
            SUMMARY_ONLY=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [options]"
            echo "Options:"
            echo "  -k, --keep-alive-only    Show only keep-alive related logs"
            echo "  -e, --errors-only        Show only errors and warnings"
            echo "  -c, --connections-only   Show only connection events"
            echo "  -s, --summary            Show keep-alive summary and exit"
            echo "  -h, --help               Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Show summary and exit if requested
if [ "$SUMMARY_ONLY" = true ]; then
    show_summary
    exit 0
fi

# Check if gateway is running
echo "Checking gateway status..."
if ssh "$VEGA_USER@$VEGA_HOST" "cd $TEST_DIR && ps -p \$(cat test-gateway.pid 2>/dev/null) > /dev/null 2>&1"; then
    echo -e "${GREEN}✓ Gateway is running${NC}"
else
    echo -e "${RED}✗ Gateway is not running${NC}"
    echo "Last 20 lines of log:"
    ssh "$VEGA_USER@$VEGA_HOST" "cd $TEST_DIR && tail -20 freenet-test-gateway.log" 2>/dev/null
    exit 1
fi

echo ""
echo "Monitoring logs... (Ctrl+C to stop)"
echo "----------------------------------------"

# Monitor logs with optional filtering
if [ -n "$FILTER" ]; then
    ssh "$VEGA_USER@$VEGA_HOST" "cd $TEST_DIR && tail -f freenet-test-gateway.log | grep -E '$FILTER'" | format_logs
else
    ssh "$VEGA_USER@$VEGA_HOST" "cd $TEST_DIR && tail -f freenet-test-gateway.log" | format_logs
fi
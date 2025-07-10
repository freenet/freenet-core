#!/bin/bash
# Monitor ziggy for contract execution timing issues

echo "Starting ziggy monitoring for contract timing..."
echo "This will capture SLOW operations and contract PUT/GET timing"
echo "Press Ctrl+C to stop"

# Monitor logs for timing issues
ssh ian@ziggy "sudo journalctl -u freenet -f | grep -E '(SLOW|contract PUT|contract GET|elapsed_ms|Starting contract|Timeout)' --line-buffered" | while read -r line; do
    timestamp=$(date +"%H:%M:%S")
    echo "[$timestamp] $line"
    
    # Alert on slow operations
    if echo "$line" | grep -q "SLOW"; then
        echo "*** ALERT: SLOW OPERATION DETECTED ***"
    fi
    
    # Alert on contract operations
    if echo "$line" | grep -q "Starting contract"; then
        echo ">>> Contract operation started..."
    fi
done
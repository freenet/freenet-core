#!/bin/bash

# Monitor vega for UDP backlog and contract timing issues
# This script will alert when the issue manifests

echo "Starting vega monitoring for UDP backlog and contract timing..."
echo "Press Ctrl+C to stop"

ALERT_FILE="/tmp/vega-issue-detected.log"
rm -f "$ALERT_FILE"

check_udp_backlog() {
    local recv_q=$(ssh freenet@vega.locut.us "ss -lun sport = :31337" 2>/dev/null | awk 'NR==2 {print $2}')
    if [[ -n "$recv_q" && "$recv_q" -gt 1000 ]]; then
        echo "$(date): ALERT! UDP backlog detected: ${recv_q} bytes" | tee -a "$ALERT_FILE"
        return 0
    fi
    return 1
}

check_contract_timing() {
    local slow_ops=$(ssh freenet@vega.locut.us "sudo journalctl -u freenet-gateway --since='1 minute ago' | grep -E 'SLOW.*execution|blocked message pipeline'" 2>/dev/null)
    if [[ -n "$slow_ops" ]]; then
        echo "$(date): ALERT! Slow contract operations detected:" | tee -a "$ALERT_FILE"
        echo "$slow_ops" | tee -a "$ALERT_FILE"
        return 0
    fi
    return 1
}

check_channel_overflow() {
    local overflow=$(ssh freenet@vega.locut.us "sudo journalctl -u freenet-gateway --since='1 minute ago' | grep -E 'Channel overflow|channel full|dropping packets'" 2>/dev/null)
    if [[ -n "$overflow" ]]; then
        echo "$(date): ALERT! Channel overflow detected:" | tee -a "$ALERT_FILE"
        echo "$overflow" | tee -a "$ALERT_FILE"
        return 0
    fi
    return 1
}

while true; do
    echo -n "$(date '+%H:%M:%S'): Checking... "
    
    issue_found=false
    
    if check_udp_backlog; then
        issue_found=true
    fi
    
    if check_contract_timing; then
        issue_found=true
    fi
    
    if check_channel_overflow; then
        issue_found=true
    fi
    
    if [[ "$issue_found" == "false" ]]; then
        echo "OK"
    else
        echo -e "\n$(date): Issue detected! Check $ALERT_FILE for details"
        # Also capture full logs when issue is detected
        ssh freenet@vega.locut.us "sudo journalctl -u freenet-gateway --since='5 minutes ago'" > "/tmp/vega-issue-logs-$(date +%Y%m%d-%H%M%S).log"
    fi
    
    sleep 30
done
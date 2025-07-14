#!/bin/bash
# Deploy instrumented freenet binary to ziggy

set -e

echo "Deploying instrumented freenet binary to ziggy..."

# Stop the service first
echo "Stopping freenet service..."
sudo systemctl stop freenet

# Wait for process to fully stop
sleep 2

# Backup existing binary
sudo cp /usr/local/bin/freenet /usr/local/bin/freenet.backup.$(date +%Y%m%d-%H%M%S)

# Copy new binary
sudo cp /tmp/freenet-instrumented /usr/local/bin/freenet
sudo chmod +x /usr/local/bin/freenet

# Start freenet service
echo "Starting freenet service..."
sudo systemctl start freenet

# Wait for service to start
sleep 5

# Check status
sudo systemctl status freenet | head -20

echo "Deployment complete! Monitor logs with:"
echo "sudo journalctl -u freenet -f | grep -E '(SLOW|contract|PUT|GET|elapsed_ms)'"
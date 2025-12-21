#!/bin/bash
# Install Freenet Gateway Auto-Update
#
# This script installs the auto-update mechanism for Freenet gateways.
# It sets up a systemd timer that checks for updates every 10 minutes.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}Freenet Gateway Auto-Update Installer${NC}"
echo "======================================="
echo

# Check for root
if [[ $EUID -ne 0 ]]; then
    echo -e "${RED}Error: This script must be run as root${NC}"
    echo "Run with: sudo $0"
    exit 1
fi

# Check dependencies
echo "Checking dependencies..."
MISSING=()
for cmd in curl jq unzip; do
    if ! command -v "$cmd" &> /dev/null; then
        MISSING+=("$cmd")
    fi
done

if [[ ${#MISSING[@]} -gt 0 ]]; then
    echo -e "${YELLOW}Installing missing dependencies: ${MISSING[*]}${NC}"
    apt-get update && apt-get install -y "${MISSING[@]}"
fi
echo -e "${GREEN}Dependencies OK${NC}"
echo

# Install the auto-update script
echo "Installing auto-update script..."
cp "$SCRIPT_DIR/gateway-auto-update.sh" /usr/local/bin/gateway-auto-update.sh
chmod 755 /usr/local/bin/gateway-auto-update.sh
echo -e "${GREEN}Installed: /usr/local/bin/gateway-auto-update.sh${NC}"

# Install the deploy script (if not already present)
if [[ ! -f /usr/local/bin/deploy-local-gateway.sh ]]; then
    echo "Installing deployment script..."
    cp "$SCRIPT_DIR/deploy-local-gateway.sh" /usr/local/bin/deploy-local-gateway.sh
    chmod 755 /usr/local/bin/deploy-local-gateway.sh
    echo -e "${GREEN}Installed: /usr/local/bin/deploy-local-gateway.sh${NC}"
fi

# Install systemd units
echo "Installing systemd units..."
cp "$SCRIPT_DIR/systemd/freenet-auto-update.service" /etc/systemd/system/
cp "$SCRIPT_DIR/systemd/freenet-auto-update.timer" /etc/systemd/system/
echo -e "${GREEN}Installed systemd units${NC}"

# Create log file with appropriate permissions
LOG_FILE="/var/log/freenet-auto-update.log"
touch "$LOG_FILE"
chmod 644 "$LOG_FILE"
echo -e "${GREEN}Created log file: $LOG_FILE${NC}"

# Reload systemd
echo "Reloading systemd..."
systemctl daemon-reload

# Enable and start the timer
echo "Enabling auto-update timer..."
systemctl enable freenet-auto-update.timer
systemctl start freenet-auto-update.timer

echo
echo -e "${GREEN}Installation complete!${NC}"
echo
echo "The gateway will now check for updates every 10 minutes."
echo
echo "Useful commands:"
echo "  systemctl status freenet-auto-update.timer    # Check timer status"
echo "  systemctl list-timers freenet-auto-update*    # See next run time"
echo "  journalctl -u freenet-auto-update -f          # Follow update logs"
echo "  gateway-auto-update.sh --force                # Force immediate update"
echo "  gateway-auto-update.sh --check                # Check without updating"
echo
echo "To disable auto-updates:"
echo "  sudo systemctl disable --now freenet-auto-update.timer"

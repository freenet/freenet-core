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
for cmd in curl jq tar unzip openssl; do
    if ! command -v "$cmd" &> /dev/null; then
        MISSING+=("$cmd")
    fi
done

if [[ ${#MISSING[@]} -gt 0 ]]; then
    echo -e "${YELLOW}Installing missing dependencies: ${MISSING[*]}${NC}"
    apt-get update && apt-get install -y "${MISSING[@]}"
fi
# gateway-auto-update.sh verifies each release's signed SHA256SUMS.txt with
# `openssl pkeyutl -rawin` (OpenSSL >= 3.0) and refuses every update without it.
if ! openssl version 2>/dev/null | grep -qE '^OpenSSL ([3-9]|[1-9][0-9])\.'; then
    echo -e "${RED}Error: OpenSSL >= 3.0 is required (found: $(openssl version 2>/dev/null || echo none))${NC}"
    exit 1
fi
if ! command -v sha256sum &> /dev/null; then
    echo -e "${RED}Error: sha256sum (coreutils) is required${NC}"
    exit 1
fi
echo -e "${GREEN}Dependencies OK${NC}"
echo

# Install the auto-update script
echo "Installing auto-update script..."
cp "$SCRIPT_DIR/gateway-auto-update.sh" /usr/local/bin/gateway-auto-update.sh
chmod 755 /usr/local/bin/gateway-auto-update.sh
echo -e "${GREEN}Installed: /usr/local/bin/gateway-auto-update.sh${NC}"

# Install or refresh the deploy script. Always refreshed: gateway-auto-update.sh
# runs it as root, and a stale copy that swallowed its own failure reported a
# dead gateway as updated (#4492). A changed previous copy is kept as .prev.
DEPLOY_SCRIPT=/usr/local/bin/deploy-local-gateway.sh
if [[ -f "$DEPLOY_SCRIPT" ]] && ! cmp -s "$SCRIPT_DIR/deploy-local-gateway.sh" "$DEPLOY_SCRIPT"; then
    cp -p "$DEPLOY_SCRIPT" "$DEPLOY_SCRIPT.prev"
    echo -e "${YELLOW}Replacing a different deploy-local-gateway.sh (previous copy: $DEPLOY_SCRIPT.prev)${NC}"
fi
install -m 0755 -o root -g root "$SCRIPT_DIR/deploy-local-gateway.sh" "$DEPLOY_SCRIPT"
echo -e "${GREEN}Installed: $DEPLOY_SCRIPT${NC}"

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

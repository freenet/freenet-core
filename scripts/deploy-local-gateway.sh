#!/bin/bash
# Deploy Freenet Gateway Locally
# Handles stopping service, installing binary, and restarting
#
# This script is designed to be as portable as possible while handling
# platform-specific service management (systemd, launchd, etc.)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Default values
BINARY_PATH=""
SERVICE_NAME="freenet-gateway"
INSTALL_PATH="/usr/local/bin/freenet"
DRY_RUN=false

show_help() {
    echo "Deploy Freenet Gateway Locally"
    echo
    echo "Usage: $0 [options]"
    echo
    echo "Options:"
    echo "  --binary PATH       Path to freenet binary (default: auto-detect from cargo build)"
    echo "  --service NAME      Service name (default: freenet-gateway)"
    echo "  --install-path PATH Installation path (default: /usr/local/bin/freenet)"
    echo "  --dry-run           Show what would be done without executing"
    echo "  --help              Show this help"
    echo
    echo "Examples:"
    echo "  $0                                    # Use default cargo release binary"
    echo "  $0 --binary ./target/release/freenet # Specify custom binary"
    echo "  $0 --dry-run                         # Preview actions"
    echo
    echo "Supported platforms:"
    echo "  - Linux (systemd)"
    echo "  - macOS (launchd) - planned"
    echo "  - Manual (stops service, copies binary, starts service)"
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --binary)
            BINARY_PATH="$2"
            shift 2
            ;;
        --service)
            SERVICE_NAME="$2"
            shift 2
            ;;
        --install-path)
            INSTALL_PATH="$2"
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --help|-h)
            show_help
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Detect platform
detect_platform() {
    case "$(uname -s)" in
        Linux*)
            echo "linux"
            ;;
        Darwin*)
            echo "macos"
            ;;
        *)
            echo "unknown"
            ;;
    esac
}

# Detect service manager
detect_service_manager() {
    if command -v systemctl &> /dev/null; then
        echo "systemd"
    elif command -v launchctl &> /dev/null; then
        echo "launchd"
    else
        echo "none"
    fi
}

# Check if running with sufficient privileges
check_privileges() {
    if [[ "$DRY_RUN" == "true" ]]; then
        return 0
    fi

    if [[ $EUID -ne 0 ]] && [[ "$INSTALL_PATH" == /usr/* ]]; then
        echo "Error: Installing to $INSTALL_PATH requires root privileges"
        echo "Please run with sudo or specify a different --install-path"
        exit 1
    fi
}

# Auto-detect binary path if not specified
if [[ -z "$BINARY_PATH" ]]; then
    # Try to find the git root
    if GIT_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel 2>/dev/null)"; then
        BINARY_PATH="$GIT_ROOT/target/release/freenet"
    else
        echo "Error: Could not auto-detect binary path. Please specify with --binary"
        exit 1
    fi
fi

# Validate binary exists
if [[ ! -f "$BINARY_PATH" ]]; then
    echo "Error: Binary not found at $BINARY_PATH"
    echo
    echo "You may need to build it first:"
    echo "  cargo build --release -p freenet"
    exit 1
fi

# Get binary version
BINARY_VERSION=$("$BINARY_PATH" --version 2>&1 | grep -o '[0-9]\+\.[0-9]\+\.[0-9]\+' | head -1 || echo "unknown")

PLATFORM=$(detect_platform)
SERVICE_MANAGER=$(detect_service_manager)

echo "Freenet Local Gateway Deployment"
echo "================================="
echo "Platform:        $PLATFORM"
echo "Service Manager: $SERVICE_MANAGER"
echo "Binary:          $BINARY_PATH"
echo "Version:         $BINARY_VERSION"
echo "Install Path:    $INSTALL_PATH"
echo "Service Name:    $SERVICE_NAME"
if [[ "$DRY_RUN" == "true" ]]; then
    echo "Mode:            DRY RUN"
fi
echo

check_privileges

# Service management functions
stop_service() {
    case "$SERVICE_MANAGER" in
        systemd)
            if systemctl is-active --quiet "$SERVICE_NAME.service" 2>/dev/null; then
                echo -n "  Stopping systemd service... "
                if [[ "$DRY_RUN" == "true" ]]; then
                    echo "[DRY RUN]"
                else
                    sudo systemctl stop "$SERVICE_NAME.service"
                    echo "✓"
                fi
            else
                echo "  ℹ️  Service not running, skipping stop"
            fi
            ;;
        launchd)
            # macOS launchd support
            if launchctl list | grep -q "$SERVICE_NAME" 2>/dev/null; then
                echo -n "  Stopping launchd service... "
                if [[ "$DRY_RUN" == "true" ]]; then
                    echo "[DRY RUN]"
                else
                    sudo launchctl stop "$SERVICE_NAME"
                    echo "✓"
                fi
            else
                echo "  ℹ️  Service not running, skipping stop"
            fi
            ;;
        none)
            echo "  ⚠️  No service manager detected, cannot stop service automatically"
            echo "     Please manually stop any running freenet processes"
            if [[ "$DRY_RUN" == "false" ]]; then
                echo -n "     Press Enter when ready to continue... "
                read -r
            fi
            ;;
    esac
}

start_service() {
    case "$SERVICE_MANAGER" in
        systemd)
            if systemctl list-unit-files | grep -q "^$SERVICE_NAME.service" 2>/dev/null; then
                echo -n "  Starting systemd service... "
                if [[ "$DRY_RUN" == "true" ]]; then
                    echo "[DRY RUN]"
                else
                    sudo systemctl start "$SERVICE_NAME.service"
                    echo "✓"
                fi
            else
                echo "  ⚠️  Service unit file not found, cannot start automatically"
                echo "     Start manually with: $INSTALL_PATH [args]"
            fi
            ;;
        launchd)
            if launchctl list | grep -q "$SERVICE_NAME" 2>/dev/null; then
                echo -n "  Starting launchd service... "
                if [[ "$DRY_RUN" == "true" ]]; then
                    echo "[DRY RUN]"
                else
                    sudo launchctl start "$SERVICE_NAME"
                    echo "✓"
                fi
            else
                echo "  ⚠️  Service not configured, cannot start automatically"
                echo "     Start manually with: $INSTALL_PATH [args]"
            fi
            ;;
        none)
            echo "  ℹ️  No service manager detected"
            echo "     Start manually with: $INSTALL_PATH [args]"
            ;;
    esac
}

verify_service() {
    case "$SERVICE_MANAGER" in
        systemd)
            if systemctl list-unit-files | grep -q "^$SERVICE_NAME.service" 2>/dev/null; then
                echo -n "  Verifying service status... "
                sleep 2  # Give service time to start
                if systemctl is-active --quiet "$SERVICE_NAME.service"; then
                    local running_version=$("$INSTALL_PATH" --version 2>&1 | grep -o '[0-9]\+\.[0-9]\+\.[0-9]\+' | head -1 || echo "unknown")
                    echo "✓"
                    echo "  ✓ Version: $running_version"
                    echo "  ✓ Service: Running"

                    # Show recent logs
                    echo
                    echo "Recent logs:"
                    sudo journalctl -u "$SERVICE_NAME.service" -n 10 --no-pager
                else
                    echo "✗"
                    echo "  ⚠️  Service failed to start"
                    echo "     Check logs with: sudo journalctl -u $SERVICE_NAME.service -n 50"
                    return 1
                fi
            fi
            ;;
        launchd)
            echo "  ℹ️  Manual verification required for launchd"
            echo "     Check with: launchctl list | grep $SERVICE_NAME"
            ;;
        none)
            echo "  ℹ️  Manual verification required"
            ;;
    esac
}

# Main deployment steps
echo "Deployment Steps:"
echo

stop_service

echo -n "  Installing binary to $INSTALL_PATH... "
if [[ "$DRY_RUN" == "true" ]]; then
    echo "[DRY RUN]"
else
    # Backup existing binary if it exists
    if [[ -f "$INSTALL_PATH" ]]; then
        sudo cp "$INSTALL_PATH" "$INSTALL_PATH.backup"
    fi

    sudo cp "$BINARY_PATH" "$INSTALL_PATH"
    sudo chmod 755 "$INSTALL_PATH"

    # Set ownership based on platform
    if [[ "$PLATFORM" == "linux" ]]; then
        sudo chown root:root "$INSTALL_PATH"
    elif [[ "$PLATFORM" == "macos" ]]; then
        sudo chown root:wheel "$INSTALL_PATH"
    fi

    echo "✓"
fi

start_service

if [[ "$DRY_RUN" == "false" ]] && [[ "$SERVICE_MANAGER" == "systemd" ]]; then
    verify_service
fi

echo
echo "✅ Deployment complete!"
echo
echo "Installed: $INSTALL_PATH (v$BINARY_VERSION)"

if [[ -f "$INSTALL_PATH.backup" ]]; then
    echo "Backup:    $INSTALL_PATH.backup"
fi

case "$SERVICE_MANAGER" in
    systemd)
        echo
        echo "Useful commands:"
        echo "  sudo systemctl status $SERVICE_NAME.service     # Check status"
        echo "  sudo journalctl -u $SERVICE_NAME.service -f    # Follow logs"
        echo "  sudo systemctl restart $SERVICE_NAME.service   # Restart"
        ;;
    launchd)
        echo
        echo "Useful commands:"
        echo "  launchctl list | grep $SERVICE_NAME            # Check status"
        echo "  sudo launchctl start $SERVICE_NAME             # Start"
        echo "  sudo launchctl stop $SERVICE_NAME              # Stop"
        ;;
    none)
        echo
        echo "Start manually with:"
        echo "  $INSTALL_PATH [args]"
        ;;
esac

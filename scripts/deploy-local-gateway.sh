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
ALL_INSTANCES=false
VERIFY_VERSION=true

show_help() {
    echo "Deploy Freenet Gateway Locally"
    echo
    echo "Usage: $0 [options]"
    echo
    echo "Options:"
    echo "  --binary PATH       Path to freenet binary (default: auto-detect from cargo build)"
    echo "  --service NAME      Service name (default: freenet-gateway)"
    echo "  --install-path PATH Installation path (default: /usr/local/bin/freenet)"
    echo "  --all-instances     Deploy to gateway + all peer instances (peer-01 to peer-10)"
    echo "  --no-verify         Skip version verification after deployment"
    echo "  --dry-run           Show what would be done without executing"
    echo "  --help              Show this help"
    echo
    echo "Examples:"
    echo "  $0                                    # Deploy to gateway only"
    echo "  $0 --all-instances                   # Deploy to gateway + all 10 peers"
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
        --all-instances)
            ALL_INSTANCES=true
            shift
            ;;
        --no-verify)
            VERIFY_VERSION=false
            shift
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

# Wait for binary to be released by all processes
wait_for_binary_release() {
    local max_wait=30
    local waited=0

    while sudo lsof "$INSTALL_PATH" &>/dev/null; do
        if [[ $waited -ge $max_wait ]]; then
            echo "⚠️  Timeout waiting for binary to be released"
            echo "     Processes still using $INSTALL_PATH:"
            sudo lsof "$INSTALL_PATH" || true
            return 1
        fi

        if [[ $waited -eq 0 ]]; then
            echo -n "  Waiting for binary to be released"
        fi
        echo -n "."
        sleep 1
        ((waited++))
    done

    if [[ $waited -gt 0 ]]; then
        echo " ✓"
    fi
    return 0
}

# Service management functions
stop_service() {
    local service_arg="$1"

    case "$SERVICE_MANAGER" in
        systemd)
            if systemctl is-active --quiet "$service_arg.service" 2>/dev/null; then
                echo -n "  Stopping systemd service ($service_arg)... "
                if [[ "$DRY_RUN" == "true" ]]; then
                    echo "[DRY RUN]"
                else
                    # Temporarily disable to prevent auto-restart
                    local was_enabled=false
                    if systemctl is-enabled --quiet "$service_arg.service" 2>/dev/null; then
                        was_enabled=true
                        sudo systemctl disable "$service_arg.service" --quiet
                    fi

                    sudo systemctl stop "$service_arg.service"
                    echo "✓"

                    # Store enabled state for later restoration
                    if [[ "$was_enabled" == "true" ]]; then
                        echo "$service_arg" >> /tmp/freenet-deploy-reenable.list
                    fi
                fi
            else
                echo "  ℹ️  Service $service_arg not running, skipping stop"
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
    local service_arg="$1"

    case "$SERVICE_MANAGER" in
        systemd)
            # Check if unit file exists by querying systemctl directly
            if systemctl list-unit-files "$service_arg.service" 2>/dev/null | grep -q "$service_arg.service"; then
                echo -n "  Starting systemd service ($service_arg)... "
                if [[ "$DRY_RUN" == "true" ]]; then
                    echo "[DRY RUN]"
                else
                    # Re-enable if it was enabled before
                    if [[ -f /tmp/freenet-deploy-reenable.list ]] && grep -q "^$service_arg$" /tmp/freenet-deploy-reenable.list; then
                        sudo systemctl enable "$service_arg.service" --quiet
                    fi

                    sudo systemctl start "$service_arg.service"
                    echo "✓"
                fi
            else
                echo "  ⚠️  Service unit file not found for $service_arg, cannot start automatically"
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
    local service_arg="$1"
    local expected_version="$2"
    local retry_count="${3:-0}"
    local max_retries=2

    case "$SERVICE_MANAGER" in
        systemd)
            # Check if unit file exists by querying systemctl directly
            if systemctl list-unit-files "$service_arg.service" 2>/dev/null | grep -q "$service_arg.service"; then
                echo -n "  Verifying service status ($service_arg)... "
                sleep 3  # Give service time to start

                if systemctl is-active --quiet "$service_arg.service"; then
                    # Find the actual freenet process, not the wrapper
                    # MainPID may point to bash if service uses ExecStart with shell
                    local freenet_pid=$(pgrep -f "freenet.*--is-gateway\|freenet.*network" | head -1)

                    if [[ -z "$freenet_pid" ]]; then
                        # Fallback to MainPID
                        freenet_pid=$(systemctl show -p MainPID --value "$service_arg.service" 2>/dev/null)
                    fi

                    if [[ -n "$freenet_pid" ]] && [[ "$freenet_pid" != "0" ]]; then
                        # CRITICAL: Verify the RUNNING process is using the correct binary
                        local running_binary=$(sudo readlink -f /proc/$freenet_pid/exe 2>/dev/null || echo "unknown")
                        local expected_binary=$(readlink -f "$INSTALL_PATH" 2>/dev/null || echo "$INSTALL_PATH")

                        # Handle case where binary is bash (wrapper script) - look for freenet specifically
                        if [[ "$running_binary" == *"/bash"* ]] || [[ "$running_binary" == *"/sh"* ]]; then
                            # Find the actual freenet process spawned by the wrapper
                            local child_pid=$(pgrep -P "$freenet_pid" -f freenet 2>/dev/null | head -1)
                            if [[ -n "$child_pid" ]]; then
                                running_binary=$(sudo readlink -f /proc/$child_pid/exe 2>/dev/null || echo "unknown")
                                freenet_pid="$child_pid"
                            else
                                # Try to find any freenet process
                                local any_freenet=$(pgrep -f "$INSTALL_PATH" | head -1)
                                if [[ -n "$any_freenet" ]]; then
                                    running_binary=$(sudo readlink -f /proc/$any_freenet/exe 2>/dev/null || echo "unknown")
                                    freenet_pid="$any_freenet"
                                fi
                            fi
                        fi

                        if [[ "$running_binary" != "unknown" ]] && [[ "$running_binary" != "$expected_binary" ]]; then
                            echo "✗"
                            echo "  ⚠️  CRITICAL: Running process using DIFFERENT binary!"
                            echo "     Running process (PID $freenet_pid) uses: $running_binary"
                            echo "     Expected (installed at):                 $expected_binary"

                            if [[ $retry_count -lt $max_retries ]]; then
                                echo "  🔄 Auto-restarting service to fix..."
                                sudo systemctl restart "$service_arg.service"
                                sleep 2
                                # Recursive retry
                                verify_service "$service_arg" "$expected_version" $((retry_count + 1))
                                return $?
                            else
                                echo "  ❌ Failed after $max_retries restart attempts"
                                echo "     Manual intervention required."
                                return 1
                            fi
                        fi
                    fi

                    # Verify the binary version on disk matches expected
                    local disk_version=$("$INSTALL_PATH" --version 2>&1 | grep -o '[0-9]\+\.[0-9]\+\.[0-9]\+' | head -1 || echo "unknown")

                    # Verify version matches
                    if [[ "$VERIFY_VERSION" == "true" ]] && [[ "$expected_version" != "unknown" ]] && [[ "$disk_version" != "$expected_version" ]]; then
                        echo "✗"
                        echo "  ⚠️  Version mismatch!"
                        echo "     Expected: $expected_version"
                        echo "     Got:      $disk_version"
                        return 1
                    fi

                    echo "✓"
                    echo "  ✓ Version: $disk_version"
                    echo "  ✓ Service: Running (PID ${freenet_pid:-unknown})"
                    echo "  ✓ Binary:  $running_binary"
                else
                    echo "✗"
                    echo "  ⚠️  Service failed to start"
                    echo "     Check logs with: sudo journalctl -u $service_arg.service -n 50"

                    if [[ $retry_count -lt $max_retries ]]; then
                        echo "  🔄 Retrying start..."
                        sudo systemctl start "$service_arg.service"
                        sleep 2
                        verify_service "$service_arg" "$expected_version" $((retry_count + 1))
                        return $?
                    fi
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

# Determine which services to deploy
SERVICES_TO_DEPLOY=("$SERVICE_NAME")
if [[ "$ALL_INSTANCES" == "true" ]]; then
    SERVICES_TO_DEPLOY=(freenet-gateway freenet-peer-{01..10})
fi

# Clear reenable list from previous runs
rm -f /tmp/freenet-deploy-reenable.list

# Main deployment steps
echo "Deployment Steps:"
echo

# Stop all services
for service in "${SERVICES_TO_DEPLOY[@]}"; do
    stop_service "$service"
done

# Wait for binary to be released
if [[ "$DRY_RUN" == "false" ]]; then
    wait_for_binary_release || {
        echo "⚠️  Failed to wait for binary release. Proceeding anyway..."
    }
fi

echo -n "  Installing binary to $INSTALL_PATH... "
if [[ "$DRY_RUN" == "true" ]]; then
    echo "[DRY RUN]"
else
    # Backup existing binary if it exists
    if [[ -f "$INSTALL_PATH" ]]; then
        sudo cp "$INSTALL_PATH" "$INSTALL_PATH.backup"
    fi

    # Remove old binary first
    sudo rm -f "$INSTALL_PATH"

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

# Start all services
for service in "${SERVICES_TO_DEPLOY[@]}"; do
    start_service "$service"
done

# Verify services
VERIFICATION_FAILED=false
if [[ "$DRY_RUN" == "false" ]] && [[ "$SERVICE_MANAGER" == "systemd" ]]; then
    echo
    for service in "${SERVICES_TO_DEPLOY[@]}"; do
        if ! verify_service "$service" "$BINARY_VERSION"; then
            VERIFICATION_FAILED=true
        fi
    done
fi

# Exit with error if verification failed
if [[ "$VERIFICATION_FAILED" == "true" ]]; then
    echo
    echo "❌ DEPLOYMENT VERIFICATION FAILED"
    echo "   Some services are not running the correct binary version."
    echo "   This can cause version incompatibility errors with peers."
    echo
    echo "   Try manually restarting: sudo systemctl restart $SERVICE_NAME"
    exit 1
fi

# Clean up reenable list
rm -f /tmp/freenet-deploy-reenable.list

echo
echo "✅ Deployment complete!"
echo
echo "Installed: $INSTALL_PATH (v$BINARY_VERSION)"

if [[ -f "$INSTALL_PATH.backup" ]]; then
    echo "Backup:    $INSTALL_PATH.backup"
fi

# Check for PATH shadowing - warn if 'freenet' in PATH resolves to a different binary
if command -v freenet &>/dev/null; then
    PATH_BINARY=$(command -v freenet)
    if [[ "$PATH_BINARY" != "$INSTALL_PATH" ]]; then
        echo
        echo "⚠️  WARNING: PATH shadowing detected!"
        echo "   'freenet' command resolves to: $PATH_BINARY"
        echo "   But we installed to:           $INSTALL_PATH"
        echo
        echo "   This can cause confusion when checking versions."
        echo "   The service uses the correct binary, but 'freenet --version'"
        echo "   from your shell may show a different (possibly stale) version."
        echo
        echo "   To fix: Remove the shadowing binary or update your PATH."
        PATH_VERSION=$("$PATH_BINARY" --version 2>&1 | grep -o '[0-9]\+\.[0-9]\+\.[0-9]\+' | head -1 || echo "unknown")
        INSTALLED_VERSION=$("$INSTALL_PATH" --version 2>&1 | grep -o '[0-9]\+\.[0-9]\+\.[0-9]\+' | head -1 || echo "unknown")
        echo "   PATH binary version:      $PATH_VERSION"
        echo "   Installed binary version: $INSTALLED_VERSION"
    fi
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

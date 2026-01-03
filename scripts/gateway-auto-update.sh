#!/bin/bash
# Freenet Gateway Auto-Update Script
#
# This script checks for new Freenet releases and updates the gateway if a newer
# version is available. It can be run manually or via systemd timer.
#
# Features:
# - Pull-based updates (no public attack surface)
# - Version comparison to avoid unnecessary updates
# - Safe update process using deploy-local-gateway.sh
# - Manual trigger support with --force flag
# - Dry-run mode for testing
#
# Usage:
#   ./gateway-auto-update.sh              # Check and update if newer version
#   ./gateway-auto-update.sh --force      # Force update even if same version
#   ./gateway-auto-update.sh --check      # Just check, don't update
#   ./gateway-auto-update.sh --dry-run    # Show what would be done

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Configuration
INSTALL_PATH="${INSTALL_PATH:-/usr/local/bin/freenet}"
GITHUB_REPO="${GITHUB_REPO:-freenet/freenet-core}"
DOWNLOAD_DIR="${DOWNLOAD_DIR:-/tmp/freenet-update}"
LOG_FILE="${LOG_FILE:-/var/log/freenet-auto-update.log}"

# Flags
FORCE_UPDATE=false
CHECK_ONLY=false
DRY_RUN=false
VERBOSE=false
ALL_INSTANCES=false

# Colors (disabled if not a terminal)
if [[ -t 1 ]]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    BLUE='\033[0;34m'
    NC='\033[0m'
else
    RED=''
    GREEN=''
    YELLOW=''
    BLUE=''
    NC=''
fi

log() {
    local level="$1"
    shift
    local message="$*"
    local timestamp
    timestamp=$(date '+%Y-%m-%d %H:%M:%S')

    # Console output (to stderr so it doesn't interfere with function return values)
    case "$level" in
        INFO)  echo -e "${GREEN}[INFO]${NC} $message" >&2 ;;
        WARN)  echo -e "${YELLOW}[WARN]${NC} $message" >&2 ;;
        ERROR) echo -e "${RED}[ERROR]${NC} $message" >&2 ;;
        DEBUG) [[ "$VERBOSE" == "true" ]] && echo -e "${BLUE}[DEBUG]${NC} $message" >&2 ;;
    esac

    # File logging (if writable)
    if [[ -w "$(dirname "$LOG_FILE")" ]] || [[ -w "$LOG_FILE" ]]; then
        echo "[$timestamp] [$level] $message" >> "$LOG_FILE" 2>/dev/null || true
    fi
}

show_help() {
    cat << EOF
Freenet Gateway Auto-Update

Checks for new Freenet releases and updates the gateway automatically.

Usage: $(basename "$0") [options]

Options:
  --force           Force update even if current version matches latest
  --check           Only check for updates, don't install
  --dry-run         Show what would be done without making changes
  --all-instances   Update all instances (gateway + peers), not just gateway
  --verbose, -v     Show verbose output
  --help, -h        Show this help message

Environment Variables:
  INSTALL_PATH      Path to installed freenet binary (default: /usr/local/bin/freenet)
  GITHUB_REPO       GitHub repository (default: freenet/freenet-core)
  DOWNLOAD_DIR      Temporary download directory (default: /tmp/freenet-update)
  LOG_FILE          Log file path (default: /var/log/freenet-auto-update.log)
  GITHUB_TOKEN      GitHub token for API requests (optional, increases rate limit)

Examples:
  # Check and update if newer version available
  $(basename "$0")

  # Force immediate update (useful when peers can't join with old gateway)
  $(basename "$0") --force

  # Check for updates without installing
  $(basename "$0") --check

  # Update all instances including peers
  $(basename "$0") --force --all-instances

Systemd Integration:
  This script is designed to be run by a systemd timer every 10 minutes.
  Install the timer with:
    sudo cp freenet-auto-update.service /etc/systemd/system/
    sudo cp freenet-auto-update.timer /etc/systemd/system/
    sudo systemctl daemon-reload
    sudo systemctl enable --now freenet-auto-update.timer

Security:
  - Pull-based: Gateway initiates connections, no public attack surface
  - Uses authenticated GitHub API (5,000 requests/hour with token)
  - Downloads from official GitHub releases only
  - Verifies architecture compatibility

EOF
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --force)
            FORCE_UPDATE=true
            shift
            ;;
        --check)
            CHECK_ONLY=true
            shift
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --all-instances)
            ALL_INSTANCES=true
            shift
            ;;
        --verbose|-v)
            VERBOSE=true
            shift
            ;;
        --help|-h)
            show_help
            exit 0
            ;;
        *)
            log ERROR "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Check dependencies
check_dependencies() {
    local missing=()

    for cmd in curl jq tar; do
        if ! command -v "$cmd" &> /dev/null; then
            missing+=("$cmd")
        fi
    done

    # Check for gh CLI (preferred) or curl
    if ! command -v gh &> /dev/null; then
        log DEBUG "gh CLI not found, will use curl for GitHub API"
    fi

    if [[ ${#missing[@]} -gt 0 ]]; then
        log ERROR "Missing required commands: ${missing[*]}"
        log ERROR "Install with: sudo apt install ${missing[*]}"
        exit 1
    fi
}

# Get current installed version
get_current_version() {
    if [[ ! -x "$INSTALL_PATH" ]]; then
        echo "0.0.0"
        return
    fi

    local version
    version=$("$INSTALL_PATH" --version 2>&1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1 || echo "0.0.0")
    echo "$version"
}

# Get latest release version from GitHub
get_latest_version() {
    local version=""

    # Try gh CLI first (handles auth automatically)
    if command -v gh &> /dev/null; then
        version=$(gh release view --repo "$GITHUB_REPO" --json tagName --jq '.tagName' 2>/dev/null || echo "")
    fi

    # Fall back to curl
    if [[ -z "$version" ]]; then
        local api_url="https://api.github.com/repos/$GITHUB_REPO/releases/latest"
        local auth_header=""

        if [[ -n "${GITHUB_TOKEN:-}" ]]; then
            auth_header="-H 'Authorization: token $GITHUB_TOKEN'"
        fi

        version=$(curl -s $auth_header "$api_url" | jq -r '.tag_name // empty' 2>/dev/null || echo "")
    fi

    # Strip 'v' prefix if present
    version="${version#v}"

    if [[ -z "$version" ]]; then
        log ERROR "Failed to fetch latest version from GitHub"
        return 1
    fi

    echo "$version"
}

# Compare versions (returns 0 if $1 > $2)
version_gt() {
    local v1="$1"
    local v2="$2"

    # Use sort -V for version comparison
    if [[ "$(printf '%s\n%s' "$v1" "$v2" | sort -V | tail -1)" == "$v1" ]] && [[ "$v1" != "$v2" ]]; then
        return 0
    fi
    return 1
}

# Detect system architecture
detect_arch() {
    local arch
    arch=$(uname -m)

    case "$arch" in
        x86_64|amd64)
            echo "x86_64"
            ;;
        aarch64|arm64)
            echo "aarch64"
            ;;
        *)
            log ERROR "Unsupported architecture: $arch"
            exit 1
            ;;
    esac
}

# Download latest release binary
download_release() {
    local version="$1"
    local arch
    arch=$(detect_arch)

    local asset_name="freenet-${arch}-unknown-linux-musl.tar.gz"
    local download_url="https://github.com/$GITHUB_REPO/releases/download/v${version}/${asset_name}"

    log INFO "Downloading $asset_name from release v$version..."
    log DEBUG "URL: $download_url"

    mkdir -p "$DOWNLOAD_DIR"
    local archive_path="$DOWNLOAD_DIR/$asset_name"

    if [[ "$DRY_RUN" == "true" ]]; then
        log INFO "[DRY RUN] Would download: $download_url"
        return 0
    fi

    # Download with progress
    if ! curl -L --progress-bar -o "$archive_path" "$download_url"; then
        log ERROR "Failed to download release"
        return 1
    fi

    # Verify download
    if [[ ! -f "$archive_path" ]] || [[ ! -s "$archive_path" ]]; then
        log ERROR "Downloaded file is missing or empty"
        return 1
    fi

    # Extract
    log INFO "Extracting binary..."
    if ! tar -xzf "$archive_path" -C "$DOWNLOAD_DIR"; then
        log ERROR "Failed to extract release"
        return 1
    fi

    local binary_path="$DOWNLOAD_DIR/freenet"
    if [[ ! -f "$binary_path" ]]; then
        log ERROR "Binary not found after extraction"
        return 1
    fi

    chmod +x "$binary_path"

    # Verify extracted binary
    local extracted_version
    extracted_version=$("$binary_path" --version 2>&1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1 || echo "")

    if [[ "$extracted_version" != "$version" ]]; then
        log WARN "Version mismatch: expected $version, got $extracted_version"
    fi

    log INFO "Downloaded and verified v$version"
    echo "$binary_path"
}

# Deploy the update using existing deployment script
deploy_update() {
    local binary_path="$1"

    log INFO "Deploying update..."

    local deploy_script="$SCRIPT_DIR/deploy-local-gateway.sh"

    if [[ ! -x "$deploy_script" ]]; then
        log ERROR "Deployment script not found: $deploy_script"
        return 1
    fi

    local deploy_args=("--binary" "$binary_path")

    if [[ "$ALL_INSTANCES" == "true" ]]; then
        deploy_args+=("--all-instances")
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        deploy_args+=("--dry-run")
    fi

    log DEBUG "Running: $deploy_script ${deploy_args[*]}"

    if ! sudo "$deploy_script" "${deploy_args[@]}"; then
        log ERROR "Deployment failed"
        return 1
    fi

    log INFO "Deployment successful"
}

# Cleanup temporary files
cleanup() {
    if [[ -d "$DOWNLOAD_DIR" ]]; then
        log DEBUG "Cleaning up $DOWNLOAD_DIR"
        rm -rf "$DOWNLOAD_DIR"
    fi
}

# Main function
main() {
    log INFO "Freenet Gateway Auto-Update starting..."

    check_dependencies

    # Get versions
    local current_version
    current_version=$(get_current_version)
    log INFO "Current version: $current_version"

    local latest_version
    if ! latest_version=$(get_latest_version); then
        log ERROR "Failed to determine latest version"
        exit 1
    fi
    log INFO "Latest version:  $latest_version"

    # Compare versions
    local needs_update=false

    if [[ "$FORCE_UPDATE" == "true" ]]; then
        log INFO "Force update requested"
        needs_update=true
    elif version_gt "$latest_version" "$current_version"; then
        log INFO "Update available: $current_version -> $latest_version"
        needs_update=true
    elif [[ "$latest_version" == "$current_version" ]]; then
        log INFO "Already running latest version"
    else
        log WARN "Current version ($current_version) is newer than latest release ($latest_version)"
    fi

    # Check-only mode
    if [[ "$CHECK_ONLY" == "true" ]]; then
        if [[ "$needs_update" == "true" ]]; then
            log INFO "Update available but --check specified, not updating"
            exit 0
        else
            log INFO "No update needed"
            exit 0
        fi
    fi

    # Perform update if needed
    if [[ "$needs_update" == "true" ]]; then
        trap cleanup EXIT

        local binary_path
        if ! binary_path=$(download_release "$latest_version"); then
            log ERROR "Failed to download release"
            exit 1
        fi

        if [[ "$DRY_RUN" == "false" ]]; then
            if ! deploy_update "$binary_path"; then
                log ERROR "Failed to deploy update"
                exit 1
            fi

            # Verify update
            local new_version
            new_version=$(get_current_version)
            if [[ "$new_version" == "$latest_version" ]]; then
                log INFO "Successfully updated to v$latest_version"
            else
                log WARN "Update completed but version mismatch: expected $latest_version, got $new_version"
            fi
        else
            log INFO "[DRY RUN] Would deploy v$latest_version"
        fi
    else
        log INFO "No update needed"
    fi

    log INFO "Auto-update check complete"
}

main "$@"

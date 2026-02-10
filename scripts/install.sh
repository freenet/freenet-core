#!/bin/sh
# Freenet installer script
# Usage: curl -fsSL https://freenet.org/install.sh | sh
#
# This script installs Freenet to ~/.local/bin/ and optionally sets up
# the system service.
#
# Options (via environment variables):
#   FREENET_INSTALL_DIR  - Installation directory (default: ~/.local/bin)
#   FREENET_NO_SERVICE   - Set to 1 to skip service installation prompt
#   FREENET_VERSION      - Specific version to install (default: latest)

set -eu

# Colors for output (if terminal supports it)
if [ -t 1 ]; then
    RED='\033[0;31m'
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    BLUE='\033[0;34m'
    NC='\033[0m' # No Color
else
    RED=''
    GREEN=''
    YELLOW=''
    BLUE=''
    NC=''
fi

info() {
    printf "${BLUE}info:${NC} %s\n" "$1"
}

success() {
    printf "${GREEN}success:${NC} %s\n" "$1"
}

warn() {
    printf "${YELLOW}warning:${NC} %s\n" "$1"
}

error() {
    printf "${RED}error:${NC} %s\n" "$1" >&2
    exit 1
}

# Detect the operating system
detect_os() {
    os=$(uname -s)
    case "$os" in
        Linux)
            echo "linux"
            ;;
        Darwin)
            echo "macos"
            ;;
        MINGW*|MSYS*|CYGWIN*)
            error "Windows is not yet supported. Please check https://freenet.org for updates."
            ;;
        *)
            error "Unsupported operating system: $os"
            ;;
    esac
}

# Detect the CPU architecture
detect_arch() {
    arch=$(uname -m)
    case "$arch" in
        x86_64|amd64)
            echo "x86_64"
            ;;
        aarch64|arm64)
            echo "aarch64"
            ;;
        armv7l)
            error "32-bit ARM is not supported. Please use a 64-bit system."
            ;;
        i386|i686)
            error "32-bit x86 is not supported. Please use a 64-bit system."
            ;;
        *)
            error "Unsupported architecture: $arch"
            ;;
    esac
}

# Build the target triple for downloading (primary choice)
get_target_triple() {
    os=$1
    arch=$2

    case "$os" in
        linux)
            # Use musl for static linking - works on all Linux distros regardless of glibc version
            echo "${arch}-unknown-linux-musl"
            ;;
        macos)
            echo "${arch}-apple-darwin"
            ;;
    esac
}

# Get fallback target triple for older releases that don't have musl binaries
get_fallback_target_triple() {
    os=$1
    arch=$2

    case "$os" in
        linux)
            echo "${arch}-unknown-linux-gnu"
            ;;
        *)
            # No fallback needed for non-Linux
            echo ""
            ;;
    esac
}

# Check if a command exists
has_cmd() {
    command -v "$1" >/dev/null 2>&1
}

# Download a file using curl or wget
download() {
    url=$1
    dest=$2

    if has_cmd curl; then
        curl -fsSL "$url" -o "$dest"
    elif has_cmd wget; then
        wget -q "$url" -O "$dest"
    else
        error "Neither curl nor wget found. Please install one of them."
    fi
}

# Try to download, return 0 on success, 1 on failure (without exiting)
try_download() {
    url=$1
    dest=$2

    if has_cmd curl; then
        curl -fsSL "$url" -o "$dest" 2>/dev/null
    elif has_cmd wget; then
        wget -q "$url" -O "$dest" 2>/dev/null
    else
        return 1
    fi
}

# Verify SHA256 checksum of a file
verify_checksum() {
    file=$1
    expected_hash=$2
    filename=$(basename "$file")

    if has_cmd sha256sum; then
        actual_hash=$(sha256sum "$file" | cut -d' ' -f1)
    elif has_cmd shasum; then
        actual_hash=$(shasum -a 256 "$file" | cut -d' ' -f1)
    else
        warn "Neither sha256sum nor shasum found. Skipping checksum verification."
        return 0
    fi

    if [ "$actual_hash" != "$expected_hash" ]; then
        error "Checksum verification failed for $filename
Expected: $expected_hash
Got:      $actual_hash
The download may be corrupted or tampered with."
    fi
}

# Get expected checksum from SHA256SUMS.txt
get_expected_checksum() {
    checksums_file=$1
    filename=$2

    grep "$filename" "$checksums_file" | cut -d' ' -f1
}

# Get the latest version from GitHub API
get_latest_version() {
    url="https://api.github.com/repos/freenet/freenet-core/releases/latest"

    if has_cmd curl; then
        version=$(curl -fsSL "$url" | grep '"tag_name"' | sed -E 's/.*"tag_name": *"v?([^"]+)".*/\1/')
    elif has_cmd wget; then
        version=$(wget -qO- "$url" | grep '"tag_name"' | sed -E 's/.*"tag_name": *"v?([^"]+)".*/\1/')
    else
        error "Neither curl nor wget found. Please install one of them."
    fi

    if [ -z "$version" ]; then
        error "Failed to fetch latest version from GitHub"
    fi

    echo "$version"
}

# Check if directory is in PATH
check_path() {
    dir=$1
    case ":$PATH:" in
        *":$dir:"*)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

# Provide shell-specific PATH instructions
print_path_instructions() {
    dir=$1
    shell=$(basename "$SHELL")

    warn "$dir is not in your PATH"
    echo ""
    echo "Add it to your PATH by adding this line to your shell configuration:"
    echo ""

    case "$shell" in
        bash)
            echo "  echo 'export PATH=\"\$HOME/.local/bin:\$PATH\"' >> ~/.bashrc"
            echo ""
            echo "Then reload your shell:"
            echo "  source ~/.bashrc"
            ;;
        zsh)
            echo "  echo 'export PATH=\"\$HOME/.local/bin:\$PATH\"' >> ~/.zshrc"
            echo ""
            echo "Then reload your shell:"
            echo "  source ~/.zshrc"
            ;;
        fish)
            echo "  fish_add_path ~/.local/bin"
            ;;
        *)
            echo "  export PATH=\"\$HOME/.local/bin:\$PATH\""
            echo ""
            echo "Add this line to your shell's configuration file."
            ;;
    esac
    echo ""
}

# Check if Freenet is installed
is_freenet_installed() {
    install_dir="${FREENET_INSTALL_DIR:-$HOME/.local/bin}"
    [ -f "$install_dir/freenet" ] || [ -f "$install_dir/fdev" ]
}

# Check if Freenet service is installed
is_service_installed() {
    os=$(detect_os)
    
    if [ "$os" = "linux" ]; then
        # Check systemd user service
        if has_cmd systemctl; then
            systemctl --user list-unit-files freenet.service >/dev/null 2>&1
            return $?
        fi
    elif [ "$os" = "macos" ]; then
        # Check launchd service
        [ -f "$HOME/Library/LaunchAgents/org.freenet.node.plist" ]
        return $?
    fi
    
    return 1
}

# Check if Freenet service is running
is_service_running() {
    os=$(detect_os)
    
    if [ "$os" = "linux" ]; then
        if has_cmd systemctl; then
            systemctl --user is-active freenet >/dev/null 2>&1
            return $?
        fi
    elif [ "$os" = "macos" ]; then
        if has_cmd launchctl; then
            launchctl list | grep -q org.freenet.node
            return $?
        fi
    fi
    
    # Fallback: check if process is running
    pgrep -x freenet >/dev/null 2>&1
}

# Uninstall Freenet
uninstall_freenet() {
    info "Freenet Uninstaller"
    echo ""
    
    install_dir="${FREENET_INSTALL_DIR:-$HOME/.local/bin}"
    os=$(detect_os)
    
    # Check if service is running and stop it
    if is_service_running; then
        warn "Freenet service is currently running. Stopping it..."
        if [ "$os" = "linux" ] && has_cmd systemctl; then
            systemctl --user stop freenet || warn "Failed to stop service"
        elif [ "$os" = "macos" ] && has_cmd launchctl; then
            launchctl stop org.freenet.node 2>/dev/null || warn "Failed to stop service"
        fi
    fi
    
    # Check if service is installed and remove it
    if is_service_installed; then
        info "Removing Freenet service..."
        if [ -f "$install_dir/freenet" ]; then
            "$install_dir/freenet" service uninstall 2>/dev/null || warn "Failed to uninstall service properly"
        fi
        
        # Manual cleanup for systemd
        if [ "$os" = "linux" ]; then
            service_file="$HOME/.config/systemd/user/freenet.service"
            if [ -f "$service_file" ]; then
                rm -f "$service_file"
                has_cmd systemctl && systemctl --user daemon-reload 2>/dev/null || true
            fi
        # Manual cleanup for launchd
        elif [ "$os" = "macos" ]; then
            plist_file="$HOME/Library/LaunchAgents/org.freenet.node.plist"
            if [ -f "$plist_file" ]; then
                launchctl unload "$plist_file" 2>/dev/null || true
                rm -f "$plist_file"
            fi
        fi
        success "Service removed"
    fi
    
    # Remove binaries
    if [ -f "$install_dir/freenet" ] || [ -f "$install_dir/fdev" ]; then
        info "Removing binaries from $install_dir..."
        rm -f "$install_dir/freenet" "$install_dir/fdev"
        success "Binaries removed"
    fi
    
    # Ask about data directory
    echo ""
    freenet_data_dir="${XDG_DATA_HOME:-$HOME/.local/share}/freenet"
    if [ -d "$freenet_data_dir" ]; then
        printf "${YELLOW}Warning:${NC} Freenet data directory exists at: $freenet_data_dir\n"
        printf "Would you like to remove it? This will delete all your Freenet data. [y/N] "
        read -r response </dev/tty
        case "$response" in
            [yY]|[yY][eE][sS])
                info "Removing data directory..."
                rm -rf "$freenet_data_dir"
                success "Data directory removed"
                ;;
            *)
                info "Keeping data directory at: $freenet_data_dir"
                ;;
        esac
    fi
    
    echo ""
    success "Freenet has been uninstalled successfully!"
    echo ""
    echo "Thank you for trying Freenet. We hope to see you again!"
    echo "For more information, visit: https://freenet.org"
}

# Main installation logic
main() {
    info "Freenet Installer"
    echo ""

    # Check if Freenet is already installed
    if is_freenet_installed; then
        install_dir="${FREENET_INSTALL_DIR:-$HOME/.local/bin}"
        
        # Get current version if possible
        if [ -f "$install_dir/freenet" ]; then
            current_version=$("$install_dir/freenet" --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' || echo "unknown")
            info "Freenet $current_version is currently installed"
        else
            info "Freenet is currently installed"
        fi
        
        # Check if service is installed
        if is_service_installed; then
            if is_service_running; then
                info "Freenet service is installed and running"
            else
                info "Freenet service is installed but not running"
            fi
        fi
        
        echo ""
        printf "Would you like to:\n"
        printf "  [r] Reinstall/upgrade Freenet\n"
        printf "  [u] Uninstall Freenet\n"
        printf "  [c] Cancel\n"
        printf "Choice [r/u/c]: "
        read -r choice </dev/tty
        
        case "$choice" in
            [uU])
                uninstall_freenet
                exit 0
                ;;
            [rR])
                info "Proceeding with reinstall/upgrade..."
                echo ""
                ;;
            *)
                info "Installation cancelled"
                exit 0
                ;;
        esac
    fi

    # Telemetry disclosure
    echo "${YELLOW}Note:${NC} Freenet collects anonymous telemetry data by default during alpha"
    echo "      to help diagnose network issues. This includes:"
    echo "      - Operation timing (connect, put, get, subscribe, update)"
    echo "      - Network topology information"
    echo "      - NO contract content is ever transmitted"
    echo ""
    echo "      To disable telemetry, run: freenet --telemetry-enabled=false"
    echo "      Or set FREENET_TELEMETRY_ENABLED=false in your environment"
    echo ""

    # Detect platform
    os=$(detect_os)
    arch=$(detect_arch)
    target=$(get_target_triple "$os" "$arch")
    fallback_target=$(get_fallback_target_triple "$os" "$arch")
    using_fallback=false

    info "Detected platform: $os ($arch)"

    # Get version
    if [ -n "${FREENET_VERSION:-}" ]; then
        version="$FREENET_VERSION"
        info "Installing specified version: $version"
    else
        info "Fetching latest version..."
        version=$(get_latest_version)
        info "Latest version: $version"
    fi

    # Set install directory
    install_dir="${FREENET_INSTALL_DIR:-$HOME/.local/bin}"

    # Create install directory if it doesn't exist
    if [ ! -d "$install_dir" ]; then
        info "Creating directory: $install_dir"
        mkdir -p "$install_dir"
    fi

    # Create temp directory for download
    tmp_dir=$(mktemp -d)
    trap 'rm -rf "$tmp_dir"' EXIT

    # Download checksums first
    info "Downloading checksums..."
    checksums_url="https://github.com/freenet/freenet-core/releases/download/v${version}/SHA256SUMS.txt"
    if ! download "$checksums_url" "$tmp_dir/SHA256SUMS.txt" 2>/dev/null; then
        warn "SHA256SUMS.txt not available for this release. Skipping checksum verification."
        checksums_available=false
    else
        checksums_available=true
    fi

    # Download freenet (try musl first, fall back to gnu for older releases)
    info "Downloading freenet..."
    freenet_archive="freenet-${target}.tar.gz"
    freenet_url="https://github.com/freenet/freenet-core/releases/download/v${version}/${freenet_archive}"

    if ! try_download "$freenet_url" "$tmp_dir/freenet.tar.gz"; then
        # Try fallback target (gnu) for older releases
        if [ -n "$fallback_target" ]; then
            warn "Musl binary not available, trying glibc version..."
            freenet_archive="freenet-${fallback_target}.tar.gz"
            freenet_url="https://github.com/freenet/freenet-core/releases/download/v${version}/${freenet_archive}"
            if ! try_download "$freenet_url" "$tmp_dir/freenet.tar.gz"; then
                error "Failed to download freenet binary for version $version"
            fi
            using_fallback=true
        else
            error "Failed to download freenet binary for version $version"
        fi
    fi

    # Verify freenet checksum
    if [ "$checksums_available" = true ]; then
        expected_hash=$(get_expected_checksum "$tmp_dir/SHA256SUMS.txt" "$freenet_archive")
        if [ -n "$expected_hash" ]; then
            info "Verifying freenet checksum..."
            verify_checksum "$tmp_dir/freenet.tar.gz" "$expected_hash"
        else
            warn "Checksum not found for $freenet_archive"
        fi
    fi

    # Download fdev (use same target as freenet)
    info "Downloading fdev..."
    if [ "$using_fallback" = true ]; then
        fdev_archive="fdev-${fallback_target}.tar.gz"
    else
        fdev_archive="fdev-${target}.tar.gz"
    fi
    fdev_url="https://github.com/freenet/freenet-core/releases/download/v${version}/${fdev_archive}"

    if ! try_download "$fdev_url" "$tmp_dir/fdev.tar.gz"; then
        error "Failed to download fdev binary for version $version"
    fi

    # Verify fdev checksum
    if [ "$checksums_available" = true ]; then
        expected_hash=$(get_expected_checksum "$tmp_dir/SHA256SUMS.txt" "$fdev_archive")
        if [ -n "$expected_hash" ]; then
            info "Verifying fdev checksum..."
            verify_checksum "$tmp_dir/fdev.tar.gz" "$expected_hash"
        else
            warn "Checksum not found for $fdev_archive"
        fi
    fi

    # Warn about glibc compatibility if using fallback
    if [ "$using_fallback" = true ]; then
        warn "Using glibc-linked binary. If you see 'GLIBC_X.XX not found' errors,"
        warn "please upgrade to a newer Freenet version or build from source."
    fi

    # Extract binaries
    info "Extracting binaries..."
    tar -xzf "$tmp_dir/freenet.tar.gz" -C "$tmp_dir"
    tar -xzf "$tmp_dir/fdev.tar.gz" -C "$tmp_dir"

    # Check if freenet is already installed and running
    if [ -f "$install_dir/freenet" ]; then
        # Check if freenet is running
        if pgrep -x freenet >/dev/null 2>&1; then
            warn "Freenet is currently running. Stopping service..."
            if has_cmd systemctl && systemctl --user is-active freenet >/dev/null 2>&1; then
                systemctl --user stop freenet
            elif has_cmd launchctl; then
                launchctl stop org.freenet.node 2>/dev/null || true
            fi
        fi

        # Get current version for comparison
        current_version=$("$install_dir/freenet" --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' || echo "unknown")
        if [ "$current_version" = "$version" ]; then
            info "Freenet $version is already installed"
        else
            info "Upgrading from $current_version to $version"
        fi
    fi

    # Verify extracted binaries exist
    if [ ! -f "$tmp_dir/freenet" ]; then
        error "Failed to extract freenet binary from archive"
    fi
    if [ ! -f "$tmp_dir/fdev" ]; then
        error "Failed to extract fdev binary from archive"
    fi

    # Install binaries (all paths quoted for safety with spaces/special chars)
    info "Installing to $install_dir..."
    mv -- "$tmp_dir/freenet" "$install_dir/freenet"
    mv -- "$tmp_dir/fdev" "$install_dir/fdev"
    chmod +x "$install_dir/freenet" "$install_dir/fdev"

    # On macOS, remove quarantine attribute to allow unsigned binaries to run
    if [ "$os" = "macos" ]; then
        xattr -d com.apple.quarantine "$install_dir/freenet" 2>/dev/null || true
        xattr -d com.apple.quarantine "$install_dir/fdev" 2>/dev/null || true
    fi

    # Verify the installed binary works
    if ! "$install_dir/freenet" --version >/dev/null 2>&1; then
        if [ "$os" = "macos" ]; then
            error "Binary verification failed. macOS may be blocking the unsigned binary.
Try running: xattr -d com.apple.quarantine $install_dir/freenet $install_dir/fdev
Then run: $install_dir/freenet --version"
        else
            error "Installed binary verification failed. The binary may be corrupted or incompatible with your system."
        fi
    fi

    success "Freenet $version installed successfully!"
    echo ""

    # Check PATH
    if ! check_path "$install_dir"; then
        print_path_instructions "$install_dir"
    fi

    # Ask about service installation (unless FREENET_NO_SERVICE is set)
    if [ "${FREENET_NO_SERVICE:-0}" != "1" ]; then
        echo ""
        printf "Would you like to install Freenet as a user service (auto-starts on login)? [y/N] "
        read -r response </dev/tty
        case "$response" in
            [yY]|[yY][eE][sS])
                info "Installing service..."
                "$install_dir/freenet" service install
                echo ""
                printf "Would you like to start the service now? [Y/n] "
                read -r start_response </dev/tty
                case "$start_response" in
                    [nN]|[nN][oO])
                        success "Service installed! Start it with: freenet service start"
                        ;;
                    *)
                        info "Starting service..."
                        "$install_dir/freenet" service start
                        success "Freenet is now running!"
                        ;;
                esac
                ;;
            *)
                info "Skipping service installation"
                echo ""
                echo "You can install the service later with:"
                echo "  freenet service install"
                ;;
        esac
    fi

    echo ""
    echo "To run Freenet manually:"
    echo "  freenet network"
    echo ""
    echo "For more information, visit: https://freenet.org"
}

# Run main function
main "$@"

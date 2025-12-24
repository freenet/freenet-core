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

# Build the target triple for downloading
get_target_triple() {
    os=$1
    arch=$2

    case "$os" in
        linux)
            echo "${arch}-unknown-linux-gnu"
            ;;
        macos)
            echo "${arch}-apple-darwin"
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

# Main installation logic
main() {
    info "Freenet Installer"
    echo ""

    # Detect platform
    os=$(detect_os)
    arch=$(detect_arch)
    target=$(get_target_triple "$os" "$arch")

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

    # Download freenet
    info "Downloading freenet..."
    freenet_url="https://github.com/freenet/freenet-core/releases/download/v${version}/freenet-${target}.tar.gz"
    download "$freenet_url" "$tmp_dir/freenet.tar.gz"

    # Download fdev
    info "Downloading fdev..."
    fdev_url="https://github.com/freenet/freenet-core/releases/download/v${version}/fdev-${target}.tar.gz"
    download "$fdev_url" "$tmp_dir/fdev.tar.gz"

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

    # Verify the installed binary works
    if ! "$install_dir/freenet" --version >/dev/null 2>&1; then
        error "Installed binary verification failed. The binary may be corrupted or incompatible with your system."
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
        printf "Would you like to install Freenet as a system service? [y/N] "
        read -r response
        case "$response" in
            [yY]|[yY][eE][sS])
                info "Installing service..."
                "$install_dir/freenet" service install
                echo ""
                success "Service installed! Start it with: freenet service start"
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

#!/bin/sh
# Freenet installer script
# Usage: curl -fsSL https://freenet.org/install.sh | sh
#
# This script installs Freenet to ~/.local/bin/ and sets up a supervised
# service so the node auto-updates.
#
# A *supervised* install registers a service manager (systemd on Linux,
# launchd on macOS) that catches the node's "update needed" exit (code 42)
# and runs `freenet update`. An UNSUPERVISED node detects a new release,
# exits to update, and never restarts on the new version - so it silently
# stops updating. This installer therefore sets up supervision by DEFAULT.
#
# On Linux it prefers a system service when it can elevate (root or sudo) -
# most reliable on the servers/VPS that dominate the node population - and
# otherwise installs a user service WITH lingering enabled (so it still runs
# when nobody is logged in). It only leaves a node unsupervised when you
# explicitly opt out.
#
# Options (via environment variables):
#   FREENET_INSTALL_DIR  - Installation directory (default: ~/.local/bin)
#   FREENET_NO_SERVICE   - Set to 1 to install WITHOUT a service. The node
#                          will NOT auto-update until you set one up.
#   FREENET_VERSION      - Specific version to install (default: latest)
#
# NOTE: This file is mirrored at hugo-site/static/install.sh in
# freenet/web (served from https://freenet.org/install.sh). Keep the
# two in sync. In particular, the service-install prompt below MUST
# keep its $0 + /dev/tty handling so that `curl | sh` users can
# answer the prompt - see https://github.com/freenet/web/pull/42 and
# its companion PR in this repo for context. A previous fix was
# already lost once via a bulk resync.

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
            error "For Windows, use PowerShell: irm https://freenet.org/install.ps1 | iex"
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

# ── Service supervision helpers ───────────────────────────────────────────
#
# A supervised install lets the node auto-update (the service manager catches
# exit code 42 and runs `freenet update`). These helpers decide WHAT kind of
# supervised service to set up. The pure decision functions
# (`decide_linux_service_mode`, `resolve_service_action`) delegate every
# environment probe to small overridable helpers so the test suite
# (scripts/test-install-sh.sh) can exercise the logic without real root/sudo.

# True if running as uid 0.
is_root() {
    [ "$(id -u 2>/dev/null)" = "0" ]
}

# True if sudo can run WITHOUT an interactive password prompt.
sudo_noninteractive_ok() {
    has_cmd sudo && sudo -n true 2>/dev/null
}

# True if a system-wide systemd unit already exists.
has_system_unit() {
    [ -f /etc/systemd/system/freenet.service ]
}

# True if a user systemd unit already exists for the invoking user.
has_user_unit() {
    [ -f "${HOME:-}/.config/systemd/user/freenet.service" ]
}

# Echo the `User=` value of the existing system unit (empty if none/unset).
existing_system_unit_user() {
    [ -f /etc/systemd/system/freenet.service ] || return 0
    sed -n 's/^User=//p' /etc/systemd/system/freenet.service 2>/dev/null | head -n1
}

# Decide whether it is safe to refresh an existing system unit. The binary
# derives the service `User=` (and home/log/ExecStart paths) from the user the
# refresh runs as, so refreshing as a DIFFERENT user would silently re-point
# the service and orphan the original node's data/identity. Only refresh when
# the unit's current user matches (or can't be determined). Pure + testable.
#   $1 = existing `User=` value (may be empty)
#   $2 = user the refresh would run as
# Echoes "refresh" or "skip".
should_refresh_system_unit() {
    if [ -z "$1" ] || [ "$1" = "$2" ]; then
        echo "refresh"
    else
        echo "skip"
    fi
}

# Decide whether a fresh supervised Linux install should be a system service
# or a user service. Pure function of two booleans so it is unit-testable.
#   $1 = am_root     ("1"/"0")
#   $2 = can_elevate ("1"/"0" - root, or sudo available)
# Echoes "system" or "user". A system service is preferred whenever we can
# elevate (it runs at boot, survives logout, and is the most reliable choice
# on headless servers); otherwise we fall back to a user service (which the
# binary sets up with lingering enabled).
decide_linux_service_mode() {
    if [ "$1" = "1" ] || [ "$2" = "1" ]; then
        echo "system"
    else
        echo "user"
    fi
}

# Resolve the effective Linux service action, honoring any existing install so
# a re-run refreshes it instead of creating a duplicate of the other type.
#   $1 = interactive ("1"/"0")
# Echoes "system" or "user".
resolve_service_action() {
    interactive=$1

    # Existing install wins: refresh the same kind we already have.
    if has_system_unit; then
        echo "system"
        return
    fi
    if has_user_unit; then
        echo "user"
        return
    fi

    am_root=0
    if is_root; then
        am_root=1
    fi

    can_elevate=0
    if [ "$am_root" = "1" ]; then
        can_elevate=1
    elif sudo_noninteractive_ok; then
        can_elevate=1
    elif [ "$interactive" = "1" ] && has_cmd sudo; then
        # An interactive run can prompt for the sudo password.
        can_elevate=1
    fi

    decide_linux_service_mode "$am_root" "$can_elevate"
}

# Loud warning printed whenever a node is left unsupervised.
warn_unsupervised() {
    bin=$1
    echo ""
    warn "Freenet is installed but NOT running under a service manager."
    warn "An unsupervised node detects new releases, exits to update, and does"
    warn "NOT restart on the new version - so it silently STOPS auto-updating."
    echo ""
    echo "To set up auto-updating supervision later, run ONE of:"
    echo "  sudo $bin service install --system   # system service (best on servers)"
    echo "  $bin service install                 # user service (enables lingering)"
    echo ""
}

# Install a supervised service for the freenet binary "$1", choosing system vs
# user (Linux) and falling back to an unsupervised install with a loud warning
# only as a last resort. "$2" = interactive ("1"/"0").
setup_service() {
    bin=$1
    interactive=$2

    if [ "$os" = "macos" ]; then
        # launchd user agent; no system/lingering distinction on macOS.
        info "Setting up the Freenet service (launchd user agent)..."
        if "$bin" service install; then
            success "Service installed. Start it with: freenet service start"
            echo "  Once running, open http://127.0.0.1:7509/ to view your dashboard."
        else
            warn "Service installation failed."
            warn_unsupervised "$bin"
        fi
        return
    fi

    # Linux.
    action=$(resolve_service_action "$interactive")
    case "$action" in
        system)
            # Refresh guard: an existing system unit is re-templated using the
            # user the refresh runs as. Refreshing it as a DIFFERENT user would
            # silently re-point the service (User=/home/log/ExecStart) and
            # orphan the original node. Skip the refresh in that case - the new
            # binary is already on disk and is picked up on the next restart.
            if has_system_unit; then
                refresh_user="${SUDO_USER:-$(id -un 2>/dev/null)}"
                existing_user=$(existing_system_unit_user)
                if [ "$(should_refresh_system_unit "$existing_user" "$refresh_user")" = "skip" ]; then
                    warn "An existing system service runs as user '$existing_user'."
                    warn "Not refreshing it as '$refresh_user' - that would re-point the"
                    warn "service and orphan the original node's data/identity."
                    warn "The updated binary is in place; restart the service to use it,"
                    warn "or re-run the installer as '$existing_user' to refresh the unit."
                    return
                fi
            fi

            if is_root; then
                # `curl ... | sudo sh` sets SUDO_USER, which the binary uses as
                # the non-root service user; a pure-root login has no such user
                # and the binary refuses to run the node as root.
                if [ -z "${SUDO_USER:-}" ]; then
                    warn "Running as root with no SUDO_USER set: the service must"
                    warn "run as a non-root user. Re-run the installer as that user"
                    warn "(e.g. 'sudo -u <user> sh install.sh') for a supervised setup."
                    warn_unsupervised "$bin"
                    return
                fi
                # The system unit runs as $SUDO_USER and points ExecStart at
                # $bin. With `curl | sudo sh`, HOME is usually /root, so $bin
                # lands under /root/.local/bin which $SUDO_USER cannot traverse:
                # the unit would install but fail to start. Verify the service
                # user can actually execute the binary before creating a unit
                # that points at it. (As root, `sudo -u` needs no password.)
                if ! sudo -u "$SUDO_USER" sh -c 'test -x "$1"' sh "$bin" 2>/dev/null; then
                    warn "The installed binary at '$bin' is not accessible to user"
                    warn "'$SUDO_USER' (it looks like it was installed under root's"
                    warn "home). A system service pointing there would fail to start."
                    warn "Re-run the installer as '$SUDO_USER' WITHOUT sudo:"
                    warn "  curl -fsSL https://freenet.org/install.sh | sh"
                    warn_unsupervised "$bin"
                    return
                fi
                info "Setting up a system service (running as \$SUDO_USER=$SUDO_USER)..."
                if "$bin" service install --system; then
                    print_service_success "system"
                else
                    warn "System service installation failed."
                    warn_unsupervised "$bin"
                fi
                return
            fi
            info "Setting up a system service (requires sudo)..."
            if sudo "$bin" service install --system; then
                print_service_success "system"
            elif has_system_unit; then
                # We picked "system" to REFRESH an existing system unit, but the
                # sudo refresh failed (e.g. no passwordless sudo in a scripted
                # rerun). Do NOT fall back to a user service - that would leave
                # BOTH a system and a user service installed (the duplicate the
                # existing-install routing exists to prevent). The freshly
                # downloaded binary is already in place at $bin and takes effect
                # on the next restart of the existing service.
                warn "Could not refresh the existing system service (needs sudo)."
                warn "The updated binary is in place and takes effect on the next restart."
                warn "To refresh the unit now, run: sudo $bin service install --system"
            else
                warn "System service install via sudo failed; falling back to a user service."
                if "$bin" service install; then
                    print_service_success "user"
                else
                    warn "User service installation failed."
                    warn_unsupervised "$bin"
                fi
            fi
            ;;
        user)
            info "Setting up a user service (with lingering, so it runs when logged out)..."
            if "$bin" service install; then
                print_service_success "user"
            else
                warn "User service installation failed."
                warn_unsupervised "$bin"
            fi
            ;;
    esac
}

# Print the post-install success blurb. "$1" = "system" or "user".
print_service_success() {
    echo ""
    if [ "$1" = "system" ]; then
        success "System service installed! Start it with: sudo freenet service start --system"
    else
        success "Service installed! Start it with: freenet service start"
    fi
    echo "  Once running, open http://127.0.0.1:7509/ to view your Freenet dashboard."
}

# Main installation logic
main() {
    info "Freenet Installer"
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

    # Set up a supervised service so the node auto-updates, UNLESS the user
    # opted out with FREENET_NO_SERVICE=1. Supervision is the default because
    # an unsupervised node silently stops updating (it exits to update and
    # never restarts on the new version).
    #
    # When the script is piped via `curl ... | sh`, sh reads its own script
    # source from stdin. A plain `read` would consume bytes of script source
    # instead of the user's answer, so we redirect from /dev/tty in that case.
    #
    # We detect "sh is reading the script from stdin" via $0: when sh runs a
    # script file, $0 is the script path; when sh reads its source from stdin,
    # $0 is the shell name itself (e.g. "sh", "bash"). In the script-file case
    # we read from stdin as before, so automation like
    # `printf 'n\n' | sh install.sh` still works. The shell-name allowlist
    # covers shells likely to appear as `curl ... | <shell>`.
    #
    # `{ true </dev/tty; } 2>/dev/null` is used instead of `[ -r /dev/tty ]`:
    # the access check can succeed even when the process has no controlling
    # terminal and the subsequent open(2) fails with ENXIO. Probe by actually
    # opening /dev/tty.
    #
    # `read -r response || response=""` keeps EOF (e.g. Ctrl-D, closed stdin)
    # from aborting the script under `set -eu`.
    if [ "${FREENET_NO_SERVICE:-0}" = "1" ]; then
        info "FREENET_NO_SERVICE is set: installing without a service."
        warn_unsupervised "$install_dir/freenet"
    else
        echo ""
        # answer_src: where to read the y/n answer from ("tty", "stdin", or ""
        # for none). has_tty: whether a real terminal exists, used to decide
        # whether an interactive sudo password prompt is possible.
        answer_src=""
        has_tty=0
        case "${0##*/}" in
            sh|bash|dash|ash|zsh|ksh|mksh|pdksh|yash|busybox|-sh|-bash|-dash|-ash|-zsh|-ksh|-mksh|-yash)
                # `curl | sh` form: the script source is on stdin, so the only
                # place an answer can come from is the controlling terminal.
                if { true </dev/tty; } 2>/dev/null; then
                    answer_src="tty"
                    has_tty=1
                fi
                ;;
            *)
                # Script ran as a file: stdin carries the answer (a terminal OR
                # a pipe), preserving automation like
                # `printf 'n\n' | sh install.sh`. An empty/EOF read keeps the
                # supervised default.
                answer_src="stdin"
                if [ -t 0 ]; then
                    has_tty=1
                fi
                ;;
        esac

        decline=0
        if [ -n "$answer_src" ]; then
            # Default is YES (supervision) - note the [Y/n].
            printf "Set up Freenet to run as an auto-updating background service? [Y/n] "
            response=""
            if [ "$answer_src" = "tty" ]; then
                read -r response </dev/tty || response=""
            else
                read -r response || response=""
            fi
            case "$response" in
                [nN]|[nN][oO]) decline=1 ;;
                *) decline=0 ;;
            esac
        else
            info "No interactive terminal detected; setting up supervision by default."
            info "(set FREENET_NO_SERVICE=1 to install without a service)"
        fi

        if [ "$decline" = "1" ]; then
            info "Skipping service installation at your request."
            warn_unsupervised "$install_dir/freenet"
        else
            setup_service "$install_dir/freenet" "$has_tty"
        fi
    fi

    echo ""
    echo "To run Freenet manually instead:"
    echo "  freenet network"
    echo ""
    echo "Once running, open http://127.0.0.1:7509/ to view your Freenet dashboard."
    echo ""
    echo "To uninstall Freenet completely:"
    echo "  freenet uninstall                                       # preferred"
    echo "  curl -fsSL https://freenet.org/uninstall.sh | sh        # fallback"
    echo ""
    echo "Do NOT prefix either command with 'sudo'. This installer placed Freenet"
    echo "under $install_dir, which is typically not on sudo's PATH - sudo would"
    echo "fail with 'command not found' and leave your install untouched. Only"
    echo "use sudo if you originally installed with 'freenet service install --system'."
    echo ""
    echo "For more information, visit: https://freenet.org"
}

# Run main function, unless this file is being sourced by the test suite
# (scripts/test-install-sh.sh sets FREENET_INSTALL_SH_LIB=1) purely to load
# the helper functions for unit testing without performing an install.
if [ "${FREENET_INSTALL_SH_LIB:-0}" != "1" ]; then
    main "$@"
fi

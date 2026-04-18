#!/bin/sh
# Freenet uninstaller script
# Usage: curl -fsSL https://freenet.org/uninstall.sh | sh
#
# Removes Freenet from the current user's install:
#  - Stops and removes the user systemd service / launchd agent.
#  - Deletes the freenet and fdev binaries from every known install
#    location (install.sh default ~/.local/bin, cargo install default
#    ~/.cargo/bin, and FREENET_INSTALL_DIR if set).
#  - Optionally purges data, config, cache, and logs.
#
# This script works even when the installed `freenet` binary is missing,
# broken, or invoked from the wrong user account. For a one-stop shop,
# `freenet uninstall` is still the preferred path when the binary runs;
# this script exists for the cases where it doesn't (a stale install, a
# user who tried `sudo freenet uninstall` and had it silently no-op
# because ~/.local/bin wasn't on sudo's PATH, etc.).
#
# Options (via environment variables or flags):
#   --purge                Also delete data, config, cache, and logs
#   --keep-data            Keep data, config, cache, and logs (skip prompt)
#   -y, --yes              Non-interactive: assume "keep data" when neither
#                          --purge nor --keep-data is set
#   FREENET_PURGE=1        Same as --purge
#   FREENET_KEEP_DATA=1    Same as --keep-data

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

has_cmd() {
    command -v "$1" >/dev/null 2>&1
}

# --- Safety guards --------------------------------------------------------
#
# Everything we remove is expressed as "${HOME}/..." paths. If $HOME is
# unset, empty, or points at an OS-owned location, those joins resolve to
# absolute system paths (e.g. `rm -rf "/Library/Application Support/..."`
# on macOS). Refuse before the first `rm`.
case "${HOME:-}" in
    "")
        error "HOME is unset or empty. Refusing to run - the uninstall would
target absolute system paths instead of your install."
        ;;
    "/"|"/root"|"/root/"|"/var/root"|"/var/root/")
        # These are sysadmin / service-account homes. Removing Freenet
        # artifacts there is legitimate only for a deliberate system-wide
        # cleanup, which this script is NOT designed for.
        error "HOME=${HOME} looks like a system/admin home directory. This
uninstaller targets user-level installs only. For a system-wide install,
use: sudo freenet service uninstall --system"
        ;;
esac

# Guard against well-meaning `sudo curl | sudo sh`: this script operates on
# the invoking user's home directory, so running under root silently wipes
# root's home rather than the user's. Use an absolute path for `id` so a
# shadowed ~/.local/bin/id can't lie to us about the effective user.
id_real_uid() {
    if [ -x /usr/bin/id ]; then
        /usr/bin/id -u 2>/dev/null
    elif [ -x /bin/id ]; then
        /bin/id -u 2>/dev/null
    else
        command -p id -u 2>/dev/null || printf ''
    fi
}

if [ "$(id_real_uid)" = "0" ] && [ -z "${FREENET_ALLOW_ROOT:-}" ]; then
    error "Do not run this uninstaller with sudo. The Freenet install lives in your own
home directory (~/.local/bin), not root's. Re-run without sudo, or set
FREENET_ALLOW_ROOT=1 if you really mean to uninstall root's install."
fi

# Parse flags
PURGE="${FREENET_PURGE:-0}"
KEEP_DATA="${FREENET_KEEP_DATA:-0}"
ASSUME_YES="0"

print_help() {
    cat <<'EOF'
Usage: uninstall.sh [--purge | --keep-data] [-y|--yes]

Removes Freenet from the current user's install: stops and removes the
user systemd service / launchd agent, deletes the freenet and fdev
binaries from every known install location (install.sh default
~/.local/bin, cargo install default ~/.cargo/bin, and
FREENET_INSTALL_DIR if set), and optionally purges data, config,
cache, and logs.

Options:
  --purge         Also delete data, config, cache, and logs
  --keep-data     Keep data, config, cache, and logs (skip prompt)
  -y, --yes       Non-interactive: assume "keep data" when neither
                  --purge nor --keep-data is set

Equivalents via environment variables:
  FREENET_PURGE=1            same as --purge
  FREENET_KEEP_DATA=1        same as --keep-data
  FREENET_ALLOW_ROOT=1       permit running under sudo / as root

For installs that were done with `cargo install freenet`, this script
still finds and removes the binary from ~/.cargo/bin. For Windows,
use `freenet.exe uninstall` from a user-level terminal.
EOF
}

while [ $# -gt 0 ]; do
    case "$1" in
        --purge)
            PURGE="1"
            ;;
        --keep-data)
            KEEP_DATA="1"
            ;;
        -y|--yes)
            ASSUME_YES="1"
            ;;
        -h|--help)
            print_help
            exit 0
            ;;
        *)
            error "Unknown option: $1"
            ;;
    esac
    shift
done

if [ "$PURGE" = "1" ] && [ "$KEEP_DATA" = "1" ]; then
    error "--purge and --keep-data are mutually exclusive"
fi

# Detect OS so we know which service manager and data directories to touch.
OS="$(uname -s)"
case "$OS" in
    Linux)  OS=linux ;;
    Darwin) OS=macos ;;
    *)      error "Unsupported OS: $OS. For Windows, open PowerShell as the current user
and run: freenet.exe uninstall (or delete %LOCALAPPDATA%\\Freenet\\bin by hand)." ;;
esac

info "Freenet uninstaller (${OS})"

# --- Step 1: stop and remove the service ----------------------------------

removed_service=""

if [ "$OS" = "linux" ]; then
    # User-level systemd unit
    if has_cmd systemctl && systemctl --user status freenet.service >/dev/null 2>&1; then
        info "Stopping user systemd service..."
        systemctl --user stop freenet.service >/dev/null 2>&1 || true
        systemctl --user disable freenet.service >/dev/null 2>&1 || true
        removed_service="user systemd"
    fi
    # Unit file (may exist even if service never ran)
    UNIT="${HOME}/.config/systemd/user/freenet.service"
    if [ -f "$UNIT" ]; then
        rm -f "$UNIT"
        info "Removed ${UNIT}"
        removed_service="${removed_service:-user systemd}"
    fi
    # System-wide unit - leave alone by default; needs sudo.
    if [ -f /etc/systemd/system/freenet.service ]; then
        warn "A system-wide service unit exists at /etc/systemd/system/freenet.service.
       This script only manages the user-level install. Remove the system unit with:
         sudo freenet service uninstall --system
       (or: sudo systemctl disable --now freenet && sudo rm /etc/systemd/system/freenet.service)"
    fi
elif [ "$OS" = "macos" ]; then
    PLIST="${HOME}/Library/LaunchAgents/org.freenet.node.plist"
    if [ -f "$PLIST" ]; then
        info "Unloading launchd agent..."
        launchctl unload "$PLIST" >/dev/null 2>&1 || true
        rm -f "$PLIST"
        info "Removed ${PLIST}"
        removed_service="launchd"
    fi
fi

# --- Step 2: remove binaries from every known install location ------------

removed_binaries="0"
remove_binary() {
    # $1: path
    if [ -e "$1" ]; then
        rm -f "$1" && info "Removed $1"
        removed_binaries="1"
    fi
}

# Build the ordered list of directories to scan for binaries. If
# FREENET_INSTALL_DIR was used at install time, it's the only directory
# install.sh wrote to, so the user expects it to be cleaned up here too.
# Always include the two default user-level locations as well, since a
# user may have installed multiple times in different ways.
install_dirs="${HOME}/.local/bin ${HOME}/.cargo/bin"
if [ -n "${FREENET_INSTALL_DIR:-}" ]; then
    install_dirs="${FREENET_INSTALL_DIR} ${install_dirs}"
fi

# Iterate install locations with a for-loop (not a pipe to `while read`) so
# that updates to `removed_binaries` survive into the summary step - the
# pipe variant runs the while-body in a subshell under POSIX sh.
for dir in $install_dirs; do
    [ -d "$dir" ] || continue
    for bin in freenet fdev freenet-service-wrapper.sh; do
        remove_binary "${dir}/${bin}"
    done
done

# --- Step 3: decide whether to purge data ---------------------------------

should_purge() {
    if [ "$PURGE" = "1" ]; then
        return 0
    fi
    if [ "$KEEP_DATA" = "1" ]; then
        return 1
    fi
    if [ "$ASSUME_YES" = "1" ]; then
        return 1 # default to keep when non-interactive
    fi
    if [ ! -t 0 ]; then
        info "Non-interactive session; keeping data/config/logs. Pass --purge to remove them."
        return 1
    fi
    printf "Also remove all Freenet data, config, and logs? [y/N] "
    read -r answer || answer=""
    case "$answer" in
        y|Y|yes|YES) return 0 ;;
        *)           return 1 ;;
    esac
}

if should_purge; then
    info "Removing data, config, cache, and logs..."
    if [ "$OS" = "linux" ]; then
        # The `directories` crate lowercases the application name on Linux
        # (`project_dirs_from` → `trim_and_lowercase_then_replace_spaces`),
        # so `ProjectDirs::from("", "The Freenet Project Inc", "Freenet")`
        # resolves to lowercase `freenet` paths - not the uppercase
        # `Freenet` that earlier versions of these docs mistakenly used.
        rm -rf \
            "${HOME}/.local/share/freenet" \
            "${HOME}/.config/freenet" \
            "${HOME}/.cache/freenet" \
            "${HOME}/.local/state/freenet"
    else
        # macOS: `directories` crate uses a dotted bundle ID under Application
        # Support and Caches, and lowercase `freenet` under Logs.
        rm -rf \
            "${HOME}/Library/Application Support/The-Freenet-Project-Inc.Freenet" \
            "${HOME}/Library/Caches/The-Freenet-Project-Inc.Freenet" \
            "${HOME}/Library/Caches/The-Freenet-Project-Inc.freenet" \
            "${HOME}/Library/Logs/freenet"
    fi
    success "Data removed."
else
    info "Keeping data directories. You can remove them later manually."
fi

# --- Summary --------------------------------------------------------------

if [ -z "$removed_service" ] && [ "$removed_binaries" = "0" ]; then
    info "Nothing to uninstall - Freenet does not appear to be installed for this user."
else
    success "Freenet uninstalled."
fi

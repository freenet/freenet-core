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
# - Refuses to install a download whose signed checksum does not verify
#
# Usage:
#   ./gateway-auto-update.sh              # Check and update if newer version
#   ./gateway-auto-update.sh --force      # Force update even if same version
#   ./gateway-auto-update.sh --check      # Just check, don't update
#   ./gateway-auto-update.sh --dry-run    # Show what would be done
#   sudo ./gateway-auto-update.sh --force --target-version vX.Y.Z --allow-downgrade
#                                         # Roll back to an older release (operator only)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Configuration
INSTALL_PATH="${INSTALL_PATH:-/usr/local/bin/freenet}"
GITHUB_REPO="${GITHUB_REPO:-freenet/freenet-core}"
# Empty means "a fresh private directory from mktemp", created in main(). A
# fixed path under /tmp would let any local user pre-create it and swap the
# archive between verification and extraction, which would make the
# signature check below worthless. sudo strips the environment, so the
# release-agent's invocation always gets the private directory. An override
# is honoured only if it is an existing directory, not a symlink, owned by the
# user running this script and mode 0700 (see prepare_download_dir).
DOWNLOAD_DIR="${DOWNLOAD_DIR:-}"
LOG_FILE="${LOG_FILE:-/var/log/freenet-auto-update.log}"
# Ed25519 public key that signs every release's SHA256SUMS.txt, as 64 hex
# characters (the raw 32-byte key). The ONE copy in this script.
#
# It MUST equal FREENET_RELEASE_PUBKEY in crates/core/src/bin/commands/update.rs
# (the key the node's own auto-updater trusts) and the key that
# cross-compile.yml's "Sign SHA256SUMS.txt (ed25519)" step signs with (also
# copied in docker/freenet-node/release-signing-key.der). Rotating the release
# key means changing all of them; gateway-auto-update_test.sh fails if this
# copy drifts from update.rs or the docker copy.
#
# Assigned unconditionally, never from the environment: a root script whose
# trusted key could be supplied by an env var would accept any release an
# attacker cared to sign. The test harness replaces it only after `source`-ing
# this file into its own shell, which is not a path production can take.
RELEASE_PUBKEY_HEX="eb2986604e34a3f985279a7e70852f3764d3347fe82074d92b1e4bc6336f8664"

# systemd unit (without .service) this run deploys to AND independently
# confirms is active afterwards. Used both as deploy-local-gateway.sh's
# --service and by verify_service_active so the two always target the same
# unit (#4492).
#
# Defaults to freenet-gateway (nova's unit, and the deploy-script default).
#
# A non-default instance (e.g. a secondary freenet-gateway-foo) needs SERVICE_NAME
# set, but the release-agent does NOT yet forward its `managed_service` config:
# it spawns this script via `sudo --non-interactive <cmd> --force
# --target-version vX.Y.Z` (updater.rs), and sudo strips the environment, so an
# `Environment=SERVICE_NAME=...` on the agent's unit would NOT reach here unless
# the sudoers policy also `env_keep`s it (it does not). The robust fix is to
# forward `managed_service` as an argv flag the agent already passes through the
# sudoers wildcard (like --target-version), then have the agent invoke this
# script with `--service`/SERVICE_NAME. Until that lands, both the deploy and
# the gate fall back to freenet-gateway on a non-default instance — tracked as a
# follow-up to #4492. (nova, the host in the #4492 incident, IS freenet-gateway,
# so the default path is correctly covered.)
SERVICE_NAME="${SERVICE_NAME:-freenet-gateway}"

# Flags
FORCE_UPDATE=false
CHECK_ONLY=false
DRY_RUN=false
VERBOSE=false
ALL_INSTANCES=false
# Optional explicit target version (semver, with or without `v` prefix). When
# set, this overrides the `get_latest_version` GitHub fetch. Used by
# freenet-release-agent so the version the agent signed for is the version
# actually installed, even if a newer release is published between the
# agent's GitHub check and this script's own fetch.
TARGET_VERSION=""
# Installing a release OLDER than the one installed is refused unless the
# operator passes --allow-downgrade. Argv only, never the environment. The
# release-agent's sudoers entry ends in a `*` wildcard that can match extra
# arguments, so the flag is also refused when sudo reports the caller as the
# agent's account (SUDO_USER is set by sudo itself from the invoking user and
# cannot be supplied by the caller). The agent never downgrades: it answers
# 403 to a version below the installed one.
ALLOW_DOWNGRADE=false
RELEASE_AGENT_USER="freenet-update"

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
  --force                 Force update even if current version matches latest
  --check                 Only check for updates, don't install
  --dry-run               Show what would be done without making changes
  --all-instances         Update all instances (gateway + peers), not just gateway
  --target-version vX.Y.Z Install this exact release rather than GitHub latest.
                          Used by freenet-release-agent so the version the agent
                          signed for is the version actually installed, even if
                          a newer release lands between sign and install.
                          Must be strict semver; whitespace and shell metachars
                          rejected (paired with sudoers wildcard for the agent).
  --allow-downgrade       Permit installing a release older than the installed
                          one (a rollback). Operator use only: refused when
                          invoked through sudo by the release-agent account.
  --verbose, -v           Show verbose output
  --help, -h              Show this help message

Environment Variables:
  INSTALL_PATH      Path to installed freenet binary (default: /usr/local/bin/freenet)
  GITHUB_REPO       GitHub repository (default: freenet/freenet-core)
  DOWNLOAD_DIR      Temporary download directory (default: a fresh mktemp -d)
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
  - Verifies the release's SHA256SUMS.txt signature against the release
    signing key built into this script, then the archive's hash against it,
    BEFORE extracting or installing anything (requires OpenSSL >= 3.0).
    There is no flag to skip this.
  - Verifies architecture compatibility

EOF
}

# Parse arguments. In a function (called only on direct execution, from the
# guard at the bottom) so that `source`-ing this script for unit tests does not
# consume the test harness's positional args or exit it on an unknown option.
parse_args() {
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
        --target-version)
            if [[ $# -lt 2 ]]; then
                log ERROR "--target-version requires an argument"
                exit 1
            fi
            TARGET_VERSION="${2#v}"
            # Defense in depth: the sudoers `*` wildcard matches arbitrary
            # characters, so even though the release-agent only forwards
            # values it parsed with `semver::Version::parse`, validate the
            # shape here. Rejects empty strings, shell metacharacters,
            # whitespace, and anything that isn't strict semver
            # (optionally with a prerelease/build identifier).
            if [[ ! "$TARGET_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[A-Za-z0-9.]+)?(\+[A-Za-z0-9.-]+)?$ ]]; then
                log ERROR "--target-version must be strict semver vX.Y.Z (got: $TARGET_VERSION)"
                exit 1
            fi
            shift 2
            ;;
        --allow-downgrade)
            ALLOW_DOWNGRADE=true
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
}

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

    # Release verification needs these; fail before any download rather than
    # after. verify_release_download checks again, since it is also callable
    # on its own.
    if ! check_verification_tools; then
        exit 1
    fi
}

# OpenSSL >= 3.0 (raw ed25519 `pkeyutl -rawin`) and sha256sum.
check_verification_tools() {
    if ! command -v openssl &> /dev/null; then
        log ERROR "Release verification: openssl not found (need OpenSSL >= 3.0); install it with: sudo apt install openssl"
        return 1
    fi
    local ossl_name ossl_ver
    read -r ossl_name ossl_ver _ <<< "$(openssl version 2>/dev/null)"
    if [[ "$ossl_name" != "OpenSSL" ]] || [[ ! "$ossl_ver" =~ ^([0-9]+)\. ]] || (( BASH_REMATCH[1] < 3 )); then
        log ERROR "Release verification: need OpenSSL >= 3.0 for raw ed25519 verification, found '${ossl_name:-?} ${ossl_ver:-?}'"
        return 1
    fi
    if ! command -v sha256sum &> /dev/null; then
        log ERROR "Release verification: sha256sum not found"
        return 1
    fi
}

# Get current installed version
get_current_version() {
    if [[ ! -x "$INSTALL_PATH" ]]; then
        echo "0.0.0"
        return
    fi

    local version
    # Keeps a prerelease suffix (0.2.150-rc1), so an rc install compares equal
    # to its own target instead of reading as a failed update.
    version=$("$INSTALL_PATH" --version 2>&1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.]+)?' | head -1 || true)
    echo "${version:-0.0.0}"
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
        # An array, so the header reaches curl as ONE argument. The old
        # string form split it at spaces and sent the literal quotes.
        local -a auth_header=()

        if [[ -n "${GITHUB_TOKEN:-}" ]]; then
            auth_header=(-H "Authorization: token $GITHUB_TOKEN")
        fi

        version=$(curl -s ${auth_header[@]+"${auth_header[@]}"} "$api_url" | jq -r '.tag_name // empty' 2>/dev/null || echo "")
    fi

    # Strip 'v' prefix if present
    version="${version#v}"

    if [[ -z "$version" ]]; then
        log ERROR "Failed to fetch latest version from GitHub"
        return 1
    fi

    echo "$version"
}

# Compare two semver versions (build metadata ignored). Prints -1, 0 or 1.
# Unlike `sort -V`, a prerelease ranks BELOW its release (0.2.150-rc1 <
# 0.2.150), and prerelease identifiers compare as SemVer 2.0 says: numeric ones
# numerically, below alphanumeric ones, and a shorter list below a longer one
# with the same prefix.
semver_cmp() {
    local LC_ALL=C  # alphanumeric identifiers compare in ASCII order
    local a="${1%%+*}" b="${2%%+*}"
    local a_core="${a%%-*}" b_core="${b%%-*}" a_pre="" b_pre=""
    [[ "$a" == *-* ]] && a_pre="${a#*-}"
    [[ "$b" == *-* ]] && b_pre="${b#*-}"
    local -a ac bc
    IFS=. read -r -a ac <<< "$a_core"
    IFS=. read -r -a bc <<< "$b_core"
    local i x y
    for i in 0 1 2; do
        x="${ac[i]:-0}" y="${bc[i]:-0}"
        [[ "$x" =~ ^[0-9]+$ ]] || x=0
        [[ "$y" =~ ^[0-9]+$ ]] || y=0
        if (( 10#$x > 10#$y )); then echo 1; return; fi
        if (( 10#$x < 10#$y )); then echo -1; return; fi
    done
    if [[ "$a_pre" == "$b_pre" ]]; then echo 0; return; fi
    if [[ -z "$a_pre" ]]; then echo 1; return; fi
    if [[ -z "$b_pre" ]]; then echo -1; return; fi
    local -a ap bp
    IFS=. read -r -a ap <<< "$a_pre"
    IFS=. read -r -a bp <<< "$b_pre"
    for (( i = 0; i < ${#ap[@]} || i < ${#bp[@]}; i++ )); do
        if (( i >= ${#ap[@]} )); then echo -1; return; fi
        if (( i >= ${#bp[@]} )); then echo 1; return; fi
        x="${ap[i]}" y="${bp[i]}"
        [[ "$x" == "$y" ]] && continue
        if [[ "$x" =~ ^[0-9]+$ && "$y" =~ ^[0-9]+$ ]]; then
            if (( 10#$x > 10#$y )); then echo 1; else echo -1; fi
        elif [[ "$x" =~ ^[0-9]+$ ]]; then
            echo -1
        elif [[ "$y" =~ ^[0-9]+$ ]]; then
            echo 1
        elif [[ "$x" > "$y" ]]; then
            echo 1
        else
            echo -1
        fi
        return
    done
    echo 0
}

# Returns 0 if $1 > $2 (semver order, see semver_cmp).
version_gt() {
    [[ "$(semver_cmp "$1" "$2")" == 1 ]]
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

    local archive_path="$DOWNLOAD_DIR/$asset_name"

    if [[ "$DRY_RUN" == "true" ]]; then
        log INFO "[DRY RUN] Would download: $download_url"
        return 0
    fi

    # Download with progress. --fail: a 404 must be an error, not an HTML body
    # saved under the archive's name.
    # HTTPS only, including across GitHub's redirect to its asset host, and
    # bounded in size and time so a hostile or broken server cannot fill the
    # disk or hang the update.
    if ! curl --fail -L --proto =https --proto-redir =https \
            --max-filesize 209715200 --max-time 900 \
            --progress-bar -o "$archive_path" "$download_url"; then
        log ERROR "Failed to download release"
        return 1
    fi

    # Verify download
    if [[ ! -f "$archive_path" ]] || [[ ! -s "$archive_path" ]]; then
        log ERROR "Downloaded file is missing or empty"
        return 1
    fi

    # The signed checksum manifest from the SAME release.
    local release_base="https://github.com/$GITHUB_REPO/releases/download/v${version}"
    local f
    for f in SHA256SUMS.txt SHA256SUMS.txt.sig; do
        if ! curl --fail -sSL --proto =https --proto-redir =https \
                --max-filesize 1048576 --max-time 120 \
                -o "$DOWNLOAD_DIR/$f" "$release_base/$f"; then
            log ERROR "Release verification FAILED: could not download $f from release v$version; refusing to install"
            return 1
        fi
    done

    # Nothing from the archive is extracted, let alone installed, until this
    # passes. Fails closed on every error.
    if ! verify_release_download "$DOWNLOAD_DIR" "$asset_name"; then
        log ERROR "Release verification FAILED for v$version; refusing to install. The running gateway is unchanged."
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

    # The archive must contain the release it was requested as. Asset names in
    # SHA256SUMS.txt carry no version, so an older release's archive, manifest
    # and signature all verify if re-uploaded under a newer tag. This is what
    # stops that replay, so a mismatch is fatal, before anything is installed.
    # (Running --version executes the binary, but only one whose hash the
    # signed manifest vouched for.)
    local extracted_version
    extracted_version=$("$binary_path" --version 2>&1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+(-[A-Za-z0-9.]+)?' | head -1 || echo "")

    if [[ "$extracted_version" != "${version%%+*}" ]]; then
        log ERROR "Release verification FAILED: release v$version contains a binary reporting version '${extracted_version:-unknown}'; refusing to install. The running gateway is unchanged."
        return 1
    fi

    log INFO "Downloaded and verified v$version"
    echo "$binary_path"
}

# Write the trusted release key to <out> as a DER SubjectPublicKeyInfo: the
# fixed 12-byte ed25519 header followed by the raw 32-byte key.
write_release_pubkey_der() {
    local out="$1"
    if [[ ! "$RELEASE_PUBKEY_HEX" =~ ^[0-9a-f]{64}$ ]]; then
        log ERROR "Release verification: built-in release public key is malformed"
        return 1
    fi
    local hex="302a300506032b6570032100${RELEASE_PUBKEY_HEX}" esc="" i
    for (( i = 0; i < ${#hex}; i += 2 )); do
        esc+="\\x${hex:i:2}"
    done
    # shellcheck disable=SC2059  # esc is built only from validated hex digits
    printf "$esc" > "$out"
}

# verify_release_download <dir> <asset_name>
#
# Returns 0 only when ALL of these hold:
#   - OpenSSL >= 3.0 is installed (needed for raw ed25519 `pkeyutl -rawin`);
#   - <dir>/SHA256SUMS.txt.sig is a valid ed25519 signature over the exact bytes
#     of <dir>/SHA256SUMS.txt under RELEASE_PUBKEY_HEX (cross-compile.yml signs
#     with `openssl pkeyutl -sign -rawin`);
#   - the verified manifest has exactly one line naming <asset_name>;
#   - the sha256 of <dir>/<asset_name> equals the hash on that line.
# Every failure logs its reason and returns non-zero.
verify_release_download() {
    local dir="$1" asset="$2"
    local sums="$dir/SHA256SUMS.txt" sig="$dir/SHA256SUMS.txt.sig" archive="$dir/$asset"

    check_verification_tools || return 1

    if [[ ! -s "$sums" ]]; then
        log ERROR "Release verification: SHA256SUMS.txt is missing or empty"
        return 1
    fi
    if [[ ! -f "$sig" ]]; then
        log ERROR "Release verification: SHA256SUMS.txt.sig is missing"
        return 1
    fi
    local sig_size
    sig_size=$(wc -c < "$sig")
    if (( sig_size != 64 )); then
        log ERROR "Release verification: SHA256SUMS.txt.sig is ${sig_size} bytes, expected a 64-byte ed25519 signature"
        return 1
    fi

    # The trusted key goes in its own private directory, never next to the
    # downloaded files.
    local keydir sig_ok=false
    if ! keydir=$(mktemp -d); then
        log ERROR "Release verification: could not create a directory for the release key"
        return 1
    fi
    if write_release_pubkey_der "$keydir/release-pubkey.der" \
        && openssl pkeyutl -verify -pubin -keyform DER -inkey "$keydir/release-pubkey.der" -rawin \
            -in "$sums" -sigfile "$sig" > /dev/null 2>&1; then
        sig_ok=true
    fi
    rm -rf "$keydir"
    if [[ "$sig_ok" != true ]]; then
        log ERROR "Release verification: SHA256SUMS.txt signature is INVALID for release key $RELEASE_PUBKEY_HEX"
        return 1
    fi
    log INFO "Release verification: SHA256SUMS.txt signature verified (release key ${RELEASE_PUBKEY_HEX:0:16}...)"

    # Exact filename match on the second field (sha256sum's binary-mode `*`
    # prefix tolerated). A substring or prefix match could pick up another
    # asset's line.
    local -a hashes=()
    local line_hash
    while IFS= read -r line_hash; do
        hashes+=("$line_hash")
    done < <(awk -v f="$asset" 'NF == 2 { n = $2; sub(/^\*/, "", n); if (n == f) print $1 }' "$sums")
    if (( ${#hashes[@]} != 1 )); then
        log ERROR "Release verification: expected exactly one SHA256SUMS.txt line for $asset, found ${#hashes[@]}"
        return 1
    fi
    local expected="${hashes[0],,}"
    if [[ ! "$expected" =~ ^[0-9a-f]{64}$ ]]; then
        log ERROR "Release verification: malformed hash for $asset in SHA256SUMS.txt"
        return 1
    fi

    local actual
    actual=$(sha256sum "$archive" 2>/dev/null | awk '{print $1}')
    if [[ "$actual" != "$expected" ]]; then
        log ERROR "Release verification: sha256 MISMATCH for $asset (manifest $expected, downloaded ${actual:-unreadable})"
        return 1
    fi
    log INFO "Release verification: $asset sha256 matches the signed manifest ($expected)"
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
    else
        # Deploy (stop/swap/start/verify) the SAME unit the post-deploy gate
        # below checks. Without this, a non-default instance (e.g. a
        # secondary freenet-gateway-foo) would deploy the default freenet-gateway
        # while verify_service_active checks freenet-gateway-foo — two
        # different units, defeating the gate (#4492). --all-instances has its
        # own fixed unit list, so --service does not apply there.
        deploy_args+=("--service" "$SERVICE_NAME")
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        deploy_args+=("--dry-run")
    fi

    log DEBUG "Running: $deploy_script ${deploy_args[*]}"

    if ! sudo "$deploy_script" "${deploy_args[@]}"; then
        log ERROR "Deployment failed"
        return 1
    fi

    # Independent post-deploy service-health gate.
    #
    # WHY this is here even though deploy-local-gateway.sh already verifies:
    # the release-agent spawns whatever deploy-local-gateway.sh is INSTALLED on
    # the gateway, which can lag the repo. On 2026-06-18 nova ran a months-old
    # deploy script that swallowed its own verify failure (`verify_service ...
    # || true`) and printed "Deployment complete!" + exit 0 while the gateway
    # service was dead — so this wrapper logged "Deployment successful" and the
    # release was reported green for a down gateway (#4492). This gate does NOT
    # trust the deploy script's exit code for liveness: it independently asks
    # systemd whether the managed unit is active, and fails the update if it is
    # not. That makes the wrapper robust regardless of which deploy script is
    # installed. (Skipped on --all-instances, where the unit set is dynamic and
    # the per-instance verification is the deploy script's job.)
    if [[ "$ALL_INSTANCES" != "true" ]]; then
        if ! verify_service_active "$SERVICE_NAME"; then
            log ERROR "Deploy script exited 0 but service '$SERVICE_NAME' is NOT active after update; treating as a failed update"
            return 1
        fi
    fi

    log INFO "Deployment successful"
}

# Independently confirm the managed systemd unit is `active`. Returns 0 only
# when systemd reports `active`; any other state (inactive/failed/activating),
# a missing unit, or a non-systemd host returns non-zero. `systemctl is-active`
# is a read-only query and needs no root. Mirrors the release-agent's
# version.rs::query_service_active so the script-side and HTTP-side gates agree.
# Verify the deployed unit AND every companion unit configured to start with it.
#
# `WantedBy=` pulls a companion up when the primary starts, but systemd does NOT
# propagate the companion's START FAILURE back to the primary — so a companion
# that fails to come up leaves the primary active and this check green while a
# gateway is silently down. Same shape as the v0.2.71 incident this function
# exists for.
#
# On hosts that still use that wiring (a companion `WantedBy=` the primary) this
# check applies. nova's freenet-gateway-2 no longer does (#6336): it is restarted
# after the primary by freenet-gateway-stagger.service, which fails visibly on
# its own, so this loop finds no companion there.
#
# Companions are read from the `.wants` SYMLINKS on disk, deliberately NOT from
# `systemctl show -p ConsistsOf`. ConsistsOf reflects the LOADED unit graph and
# comes back EMPTY once the companion is stopped — so a ConsistsOf-based check
# finds nothing to verify at exactly the moment it is needed and passes. That
# was measured, not assumed. The symlinks are static configuration and are
# present whether or not the unit is running.
#
# Reading from disk also survives the release-agent's `sudo` call, which strips
# the environment — the same reason SERVICE_NAME forwarding is still unsolved
# (see the note at the top of this file).
verify_service_active() {
    local service_arg="$1"

    # Search roots are overridable ONLY so the test harness can point them at a
    # fixture; production never sets this. Without it the companion branch is
    # untestable and would ship unexercised, which is how the gap this function
    # closes got in.
    local -a unit_roots
    IFS=: read -r -a unit_roots <<< "${SYSTEMD_UNIT_ROOTS:-/etc/systemd/system:/usr/lib/systemd/system}"

    local root wants_dir companion cstate cname
    for root in "${unit_roots[@]}"; do
        wants_dir="$root/$service_arg.service.wants"
        [[ -d "$wants_dir" ]] || continue
        for companion in "$wants_dir"/*.service; do
            [[ -e "$companion" ]] || continue
            cname=$(basename "$companion")
            cstate=$(systemctl is-active "$cname" 2>/dev/null || true)
            if [[ "$cstate" != "active" ]]; then
                log ERROR "Companion unit '$cname' of '$service_arg' is '$cstate', not active; treating the update as failed"
                return 1
            fi
            log INFO "Companion unit '$cname' is active"
        done
    done

    # Only systemd is supported by the automated update path (nova). On a
    # host without systemctl we cannot confirm liveness; fail closed so a
    # non-systemd gateway is never silently reported as a healthy update.
    if ! command -v systemctl &> /dev/null; then
        log ERROR "systemctl not found; cannot confirm '$service_arg' is running"
        return 1
    fi

    # Give the unit a moment to settle after the deploy script's own start.
    sleep 2

    local state
    state=$(systemctl is-active "$service_arg.service" 2>/dev/null || true)
    if [[ "$state" == "active" ]]; then
        log INFO "Service '$service_arg' is active"
        return 0
    fi

    log ERROR "Service '$service_arg' is '${state:-unknown}' (expected 'active')"
    log ERROR "Check logs with: sudo journalctl -u $service_arg.service -n 50"
    return 1
}

# Points DOWNLOAD_DIR at a fresh mktemp directory this run owns and will
# remove: under /tmp by default, or under a caller-supplied DOWNLOAD_DIR, which
# must be an existing directory (not a symlink) owned by the running uid with
# mode 0700. The caller's directory itself is never removed. Returns non-zero
# (with a log line) on an unsafe override.
prepare_download_dir() {
    local parent="/tmp"
    if [[ -n "$DOWNLOAD_DIR" ]]; then
        if [[ -L "$DOWNLOAD_DIR" || ! -d "$DOWNLOAD_DIR" ]] \
            || [[ "$(stat -c '%u %a' "$DOWNLOAD_DIR" 2>/dev/null)" != "$EUID 700" ]]; then
            log ERROR "DOWNLOAD_DIR=$DOWNLOAD_DIR must be an existing directory (not a symlink) owned by uid $EUID with mode 0700; refusing to download into it"
            return 1
        fi
        parent="$DOWNLOAD_DIR"
    fi
    if ! DOWNLOAD_DIR=$(mktemp -d "$parent/freenet-update.XXXXXX"); then
        log ERROR "Could not create a private download directory under $parent"
        return 1
    fi
    DOWNLOAD_DIR_CREATED=true
}

# Cleanup temporary files: only the directory this run created.
DOWNLOAD_DIR_CREATED=false
cleanup() {
    if [[ "$DOWNLOAD_DIR_CREATED" == true && -d "$DOWNLOAD_DIR" ]]; then
        log DEBUG "Cleaning up $DOWNLOAD_DIR"
        rm -rf "$DOWNLOAD_DIR"
    fi
}

# Main function
# Hosts with a companion gateway (nova: freenet-gateway-2) install
# freenet-gateway-stagger.service. The companion is deliberately NOT tied to the
# primary's stop/start, so after a successful primary update this queues a
# restart of the companion that waits until the primary has been up and healthy
# for ~10 minutes (#6336: restarting both gateways together doubled the share of
# SUBSCRIBEs needing extra hops for 10+ hours). `--no-block` returns at once; the
# stagger unit logs to the journal and fails visibly if the restart does not
# land. Hosts without the unit are unaffected.
schedule_companion_restart() {
    local stagger_unit="${STAGGER_UNIT:-freenet-gateway-stagger.service}"

    # --all-instances always deploys freenet-gateway, so it queues too.
    if [[ "$ALL_INSTANCES" != "true" && "$SERVICE_NAME" != "freenet-gateway" ]]; then
        return 0
    fi
    if ! systemctl list-unit-files "$stagger_unit" 2>/dev/null | grep -q "$stagger_unit"; then
        return 0
    fi
    if ! systemctl start --no-block "$stagger_unit"; then
        log ERROR "Could not queue $stagger_unit; the companion gateway is still on the OLD binary"
        return 1
    fi
    log INFO "Queued $stagger_unit: the companion gateway restarts once '$SERVICE_NAME' has been healthy for ~10 min (journalctl -u $stagger_unit)"
}

main() {
    log INFO "Freenet Gateway Auto-Update starting..."

    if [[ "$ALLOW_DOWNGRADE" == "true" && "${SUDO_USER:-}" == "$RELEASE_AGENT_USER" ]]; then
        log ERROR "--allow-downgrade is for operators; refusing it from the release-agent account ($RELEASE_AGENT_USER)"
        exit 1
    fi

    check_dependencies

    # Get versions
    local current_version
    current_version=$(get_current_version)
    log INFO "Current version: $current_version"

    local latest_version
    if [[ -n "$TARGET_VERSION" ]]; then
        latest_version="$TARGET_VERSION"
        log INFO "Target version:  $latest_version (pinned via --target-version)"
    else
        if ! latest_version=$(get_latest_version); then
            log ERROR "Failed to determine latest version"
            exit 1
        fi
        log INFO "Latest version:  $latest_version"
    fi

    # Compare versions
    local needs_update=false

    if [[ "$FORCE_UPDATE" == "true" ]]; then
        log INFO "Force update requested"
        needs_update=true
    elif version_gt "$latest_version" "$current_version"; then
        log INFO "Update available: $current_version -> $latest_version"
        needs_update=true
    elif [[ "$(semver_cmp "$latest_version" "$current_version")" == 0 ]]; then
        log INFO "Already running latest version"
    else
        log WARN "Current version ($current_version) is newer than latest release ($latest_version)"
    fi

    # Never move the gateway BACKWARDS without an operator saying so: --force
    # means "reinstall even if the version matches", not "accept an older
    # release". A signed older release is still a valid signature, so this is
    # the check that stops a replayed old release from being installed.
    if [[ "$needs_update" == "true" ]] && version_gt "$current_version" "$latest_version"; then
        if [[ "$ALLOW_DOWNGRADE" != "true" ]]; then
            log ERROR "Refusing to downgrade from v$current_version to v$latest_version; an operator can roll back with --allow-downgrade. The running gateway is unchanged."
            exit 1
        fi
        log WARN "Downgrading from v$current_version to v$latest_version (--allow-downgrade)"
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
        prepare_download_dir || exit 1
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

            # Verify update. A version mismatch here means the new binary did
            # NOT actually land on disk — that is a FAILED update, not a
            # warning. Previously this only WARNed, so an install that left the
            # old binary in place still exited 0 and was reported as success.
            local new_version
            new_version=$(get_current_version)
            if [[ "$(semver_cmp "$new_version" "$latest_version")" == 0 ]]; then
                log INFO "Successfully updated to v$latest_version"
                schedule_companion_restart || exit 1
            else
                log ERROR "Update failed: expected v$latest_version on disk but found v$new_version"
                exit 1
            fi
        else
            log INFO "[DRY RUN] Would deploy v$latest_version"
        fi
    else
        log INFO "No update needed"
        # The companion may still be on an old binary (primary not healthy in
        # time, a failed stagger run, a second release during the settle wait).
        # Queueing the stagger again is idempotent: it exits at once when the
        # companion already runs the installed binary.
        if [[ "$CHECK_ONLY" != "true" && "$DRY_RUN" != "true" ]]; then
            schedule_companion_restart || exit 1
        fi
    fi

    log INFO "Auto-update check complete"
}

# Only parse args and run main when executed directly, not when sourced.
# Sourcing is how the regression test (gateway-auto-update_test.sh) exercises
# verify_service_active and deploy_update in isolation without going through the
# network download path; it must not have its own args parsed or be exited by
# this script. `${BASH_SOURCE[0]} == ${0}` is true only on direct execution.
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    parse_args "$@"
    main "$@"
fi

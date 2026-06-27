#!/bin/sh
# Smoke tests for the service-supervision decision logic in scripts/install.sh.
#
# install.sh now sets up a SUPERVISED service by default (issue #4073) so new
# nodes auto-update with no user action. On Linux it prefers a system service
# when it can elevate (root or sudo) and otherwise installs a user service
# (with lingering). This test exercises that decision in isolation.
#
# It sources install.sh with FREENET_INSTALL_SH_LIB=1 (which suppresses the
# `main` call) and then redefines the small environment-probe helpers
# (is_root, sudo_noninteractive_ok, has_cmd, has_system_unit, has_user_unit)
# to simulate each scenario without needing real root/sudo. This is why those
# probes are factored out as overridable functions in install.sh.
#
# Usage: scripts/test-install-sh.sh
# Exit codes: 0 = all green, 1 = at least one failure.

set -u

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
INSTALL="${SCRIPT_DIR}/install.sh"

if [ ! -f "$INSTALL" ]; then
    echo "missing: $INSTALL" >&2
    exit 2
fi

fails=0
pass() { printf 'PASS  %s\n' "$1"; }
fail() {
    printf 'FAIL  %s - %s\n' "$1" "$2" >&2
    fails=$((fails + 1))
}

check_eq() {
    # $1: test name, $2: expected, $3: actual
    if [ "$2" = "$3" ]; then
        pass "$1"
    else
        fail "$1" "expected '$2', got '$3'"
    fi
}

# Call the pure decide function: decide_linux_service_mode AM_ROOT CAN_ELEVATE
decide() {
    FREENET_INSTALL_SH_LIB=1 INSTALL="$INSTALL" sh -c '
        . "$INSTALL"
        decide_linux_service_mode "$1" "$2"
    ' _ "$1" "$2"
}

# Call resolve_service_action with a snippet of helper overrides applied first.
#   $1: override snippet (shell code), $2: interactive flag ("1"/"0")
resolve_with() {
    overrides=$1
    inter=$2
    FREENET_INSTALL_SH_LIB=1 INSTALL="$INSTALL" sh -c '
        . "$INSTALL"
        '"$overrides"'
        resolve_service_action "$1"
    ' _ "$inter"
}

# ── decide_linux_service_mode: pure system-vs-user policy ──────────────────

check_eq "decide: root + elevate -> system"      "system" "$(decide 1 1)"
check_eq "decide: root, no-elevate -> system"    "system" "$(decide 1 0)"
check_eq "decide: non-root + elevate -> system"  "system" "$(decide 0 1)"
check_eq "decide: non-root, no-elevate -> user"  "user"   "$(decide 0 0)"

# ── resolve_service_action: existing-install routing wins ──────────────────

# Even a non-root, no-sudo run must refresh an existing SYSTEM unit (not
# create a duplicate user service).
check_eq "resolve: existing system unit -> system" "system" \
    "$(resolve_with '
        is_root() { return 1; }
        sudo_noninteractive_ok() { return 1; }
        has_cmd() { return 1; }
        has_system_unit() { return 0; }
        has_user_unit() { return 1; }
    ' 0)"

check_eq "resolve: existing user unit -> user" "user" \
    "$(resolve_with '
        is_root() { return 1; }
        sudo_noninteractive_ok() { return 1; }
        has_cmd() { return 1; }
        has_system_unit() { return 1; }
        has_user_unit() { return 0; }
    ' 0)"

# ── resolve_service_action: fresh install, elevation detection ─────────────

check_eq "resolve: fresh + root -> system" "system" \
    "$(resolve_with '
        is_root() { return 0; }
        sudo_noninteractive_ok() { return 1; }
        has_cmd() { return 1; }
        has_system_unit() { return 1; }
        has_user_unit() { return 1; }
    ' 0)"

check_eq "resolve: fresh + passwordless sudo -> system" "system" \
    "$(resolve_with '
        is_root() { return 1; }
        sudo_noninteractive_ok() { return 0; }
        has_cmd() { return 1; }
        has_system_unit() { return 1; }
        has_user_unit() { return 1; }
    ' 0)"

# Non-interactive, no root, no passwordless sudo -> user (safe supervised
# fallback rather than leaving the node unsupervised).
check_eq "resolve: fresh + non-root + no sudo + non-interactive -> user" "user" \
    "$(resolve_with '
        is_root() { return 1; }
        sudo_noninteractive_ok() { return 1; }
        has_cmd() { return 1; }
        has_system_unit() { return 1; }
        has_user_unit() { return 1; }
    ' 0)"

# Interactive but sudo not installed -> user.
check_eq "resolve: fresh + interactive + no sudo cmd -> user" "user" \
    "$(resolve_with '
        is_root() { return 1; }
        sudo_noninteractive_ok() { return 1; }
        has_cmd() { return 1; }
        has_system_unit() { return 1; }
        has_user_unit() { return 1; }
    ' 1)"

# Interactive, sudo present but needs a password -> system (we can prompt).
# SC2016: the `$1` below is intentionally literal — it is the argument of the
# overriding has_cmd inside the sourced subshell, not a variable to expand here.
# shellcheck disable=SC2016
check_eq "resolve: fresh + interactive + sudo present -> system" "system" \
    "$(resolve_with '
        is_root() { return 1; }
        sudo_noninteractive_ok() { return 1; }
        has_cmd() { case "$1" in sudo) return 0 ;; *) return 1 ;; esac; }
        has_system_unit() { return 1; }
        has_user_unit() { return 1; }
    ' 1)"

# ── should_refresh_system_unit: same-user refresh guard ────────────────────

# Pure helper: only refresh an existing system unit when the unit's current
# user matches the user the refresh would run as (else the refresh silently
# re-points the service to a different account).
refresh_decision() {
    FREENET_INSTALL_SH_LIB=1 INSTALL="$INSTALL" sh -c '
        . "$INSTALL"
        should_refresh_system_unit "$1" "$2"
    ' _ "$1" "$2"
}

check_eq "refresh: same user -> refresh"      "refresh" "$(refresh_decision alice alice)"
check_eq "refresh: different user -> skip"    "skip"    "$(refresh_decision alice bob)"
check_eq "refresh: empty existing -> refresh" "refresh" "$(refresh_decision '' bob)"

# ── sourcing the lib must not perform an install ───────────────────────────
#
# With FREENET_INSTALL_SH_LIB=1, sourcing install.sh must define functions but
# never run main() (which would try to download). We assert that sourcing is
# quick and silent rather than attempting any network work.
src_out=$(FREENET_INSTALL_SH_LIB=1 INSTALL="$INSTALL" sh -c '. "$INSTALL"; echo SOURCED_OK' 2>&1)
check_eq "sourcing lib does not run main" "SOURCED_OK" "$src_out"

echo
if [ "$fails" -eq 0 ]; then
    echo "All install.sh smoke tests passed."
    exit 0
else
    echo "${fails} test(s) failed."
    exit 1
fi

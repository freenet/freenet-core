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

# Call the pure decide function:
#   decide AM_ROOT CAN_ELEVATE [INSTALL_DIR] [SELINUX]
# SELINUX defaults to 0 — the overwhelmingly common case, and the one whose
# behaviour must be unchanged by #4924.
decide() {
    FREENET_INSTALL_SH_LIB=1 INSTALL="$INSTALL" sh -c '
        . "$INSTALL"
        decide_linux_service_mode "$1" "$2" "$3" "$4"
    ' _ "$1" "$2" "${3:-/usr/local/bin}" "${4:-0}"
}

# Call decide with a custom HOME so ${HOME:-} matching can be exercised.
#   $1: am_root, $2: can_elevate, $3: install_dir, $4: HOME override,
#   $5: selinux ("1"/"0", default 1 — these cases exist to exercise the
#       user-local routing, which only applies under SELinux)
decide_with_home() {
    HOME="$4" FREENET_INSTALL_SH_LIB=1 INSTALL="$INSTALL" sh -c '
        export HOME
        . "$INSTALL"
        decide_linux_service_mode "$1" "$2" "$3" "$4"
    ' _ "$1" "$2" "$3" "${5:-1}"
}

# Call resolve_service_action with a snippet of helper overrides applied first.
#   $1: override snippet (shell code), $2: interactive flag ("1"/"0"),
#   $3: install_dir (optional, defaults to /usr/local/bin)
# `selinux_active` is stubbed to false first, so the default is the non-SELinux
# machine; a snippet that wants the other case overrides it after.
resolve_with() {
    overrides=$1
    inter=$2
    install_dir=${3:-/usr/local/bin}
    FREENET_INSTALL_SH_LIB=1 INSTALL="$INSTALL" sh -c '
        . "$INSTALL"
        selinux_active() { return 1; }
        '"$overrides"'
        resolve_service_action "$1" "'"$install_dir"'"
    ' _ "$inter"
}

# ── decide_linux_service_mode: pure system-vs-user policy ──────────────────

check_eq "decide: root + elevate + system dir -> system"      "system" "$(decide 1 1 /usr/local/bin)"
check_eq "decide: root, no-elevate + system dir -> system"    "system" "$(decide 1 0 /usr/local/bin)"
check_eq "decide: non-root + elevate + system dir -> system"  "system" "$(decide 0 1 /usr/local/bin)"
check_eq "decide: non-root, no-elevate + system dir -> user"  "user"   "$(decide 0 0 /usr/local/bin)"

# ON SELINUX, user-local directories (e.g. ~/.local/bin) use a user service to
# avoid init_t denials (#4924).
check_eq "decide: selinux + root + elevate + ~/.local/bin -> user"      "user"   "$(decide 1 1 /home/alice/.local/bin 1)"
check_eq "decide: selinux + non-root + elevate + ~/.local/bin -> user"  "user"   "$(decide 0 1 /home/alice/.local/bin 1)"
check_eq "decide: selinux + root + elevate + ~/projects -> user"        "user"   "$(decide_with_home 1 1 /home/alice/projects /home/alice 1)"

# OFF SELinux the routing must be exactly what it was before #4924. These are
# the regression cases for the scope concern: $install_dir defaults to
# $HOME/.local/bin for everyone, so an ungated rule would have flipped every
# ordinary Linux install to a user service and quietly cost it start-at-boot.
check_eq "decide: no selinux + root + elevate + ~/.local/bin -> system"     "system" "$(decide 1 1 /home/alice/.local/bin 0)"
check_eq "decide: no selinux + non-root + elevate + ~/.local/bin -> system" "system" "$(decide 0 1 /home/alice/.local/bin 0)"
check_eq "decide: no selinux + root + elevate + ~/projects -> system"       "system" \
    "$(decide_with_home 1 1 /home/alice/projects /home/alice 0)"
# ...and a user with no elevation still gets a user service off SELinux.
check_eq "decide: no selinux + non-root + no-elevate + ~/.local/bin -> user" "user"  "$(decide 0 0 /home/alice/.local/bin 0)"

# Path must require a separator after $HOME: /home/aliceproject should NOT
# match $HOME=/home/alice (prefix-only match bug, fixed with "${HOME}/"*).
check_eq "decide: selinux + /home/aliceproject/bin with HOME=/home/alice -> system" "system" \
    "$(decide_with_home 1 1 /home/aliceproject/bin /home/alice 1)"
check_eq "decide: selinux + /home/alice/project with HOME=/home/alice -> user"     "user" \
    "$(decide_with_home 1 1 /home/alice/project /home/alice 1)"

# HOME unset: should NOT match-everything — fall through to elevate logic.
check_eq "decide: selinux + root + elevate + /usr/local/bin, HOME unset -> system" "system" \
    "$(FREENET_INSTALL_SH_LIB=1 INSTALL="$INSTALL" sh -c '
        unset HOME
        . "$INSTALL"
        decide_linux_service_mode "$1" "$2" "$3" "$4"
    ' _ 1 1 /usr/local/bin 1)"

# ── restore_install_context: SELinux restorecon integration ────────────────

# When restorecon is available, it must be called on the installed binaries.
check_eq "restore: has_cmd restorecon -> calls restorecon" "called" \
    "$(FREENET_INSTALL_SH_LIB=1 INSTALL="$INSTALL" sh -c '
        . "$INSTALL"
        has_cmd() { case "$1" in restorecon) return 0 ;; *) return 1 ;; esac; }
        restorecon() { printf "called"; }
        restore_install_context /tmp/freenet-install
    ')"

# When restorecon is not available, it must NOT be called.
_check_restore_skip() {
    rm -f /tmp/.freenet-restore-test
    FREENET_INSTALL_SH_LIB=1 INSTALL="$INSTALL" sh -c '
        . "$INSTALL"
        has_cmd() { return 1; }
        restorecon() { touch /tmp/.freenet-restore-test; }
        restore_install_context /tmp/freenet-install
    '
    if [ -f /tmp/.freenet-restore-test ]; then
        printf "called"
    else
        printf "not_called"
    fi
    rm -f /tmp/.freenet-restore-test
}
check_eq "restore: no restorecon cmd -> skip" "not_called" "$(_check_restore_skip)"

# Verify restorecon receives the correct binary paths.
check_eq "restore: restorecon args match install_dir" "-v /tmp/freenet-install/freenet /tmp/freenet-install/fdev" \
    "$(FREENET_INSTALL_SH_LIB=1 INSTALL="$INSTALL" sh -c '
        . "$INSTALL"
        has_cmd() { case "$1" in restorecon) return 0 ;; *) return 1 ;; esac; }
        restorecon() { printf "%s" "$*"; }
        restore_install_context /tmp/freenet-install
    ')"

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

check_eq "resolve: fresh + root + system dir -> system" "system" \
    "$(resolve_with '
        is_root() { return 0; }
        sudo_noninteractive_ok() { return 1; }
        has_cmd() { return 1; }
        has_system_unit() { return 1; }
        has_user_unit() { return 1; }
    ' 0 /usr/local/bin)"

check_eq "resolve: fresh + passwordless sudo + system dir -> system" "system" \
    "$(resolve_with '
        is_root() { return 1; }
        sudo_noninteractive_ok() { return 0; }
        has_cmd() { return 1; }
        has_system_unit() { return 1; }
        has_user_unit() { return 1; }
    ' 0 /usr/local/bin)"

# Non-interactive, no root, no passwordless sudo -> user (safe supervised
# fallback rather than leaving the node unsupervised).
check_eq "resolve: fresh + non-root + no sudo + non-interactive -> user" "user" \
    "$(resolve_with '
        is_root() { return 1; }
        sudo_noninteractive_ok() { return 1; }
        has_cmd() { return 1; }
        has_system_unit() { return 1; }
        has_user_unit() { return 1; }
    ' 0 /usr/local/bin)"

# Interactive but sudo not installed -> user.
check_eq "resolve: fresh + interactive + no sudo cmd -> user" "user" \
    "$(resolve_with '
        is_root() { return 1; }
        sudo_noninteractive_ok() { return 1; }
        has_cmd() { return 1; }
        has_system_unit() { return 1; }
        has_user_unit() { return 1; }
    ' 1 /usr/local/bin)"

# Interactive, sudo present but needs a password -> system (we can prompt).
# SC2016: the `$1` below is intentionally literal — it is the argument of the
# overriding has_cmd inside the sourced subshell, not a variable to expand here.
# shellcheck disable=SC2016
check_eq "resolve: fresh + interactive + sudo present + system dir -> system" "system" \
    "$(resolve_with '
        is_root() { return 1; }
        sudo_noninteractive_ok() { return 1; }
        has_cmd() { case "$1" in sudo) return 0 ;; *) return 1 ;; esac; }
        has_system_unit() { return 1; }
        has_user_unit() { return 1; }
    ' 1 /usr/local/bin)"

# ── resolve_service_action: user-local directory prefers user service ──────

# On SELinux, even with elevation available, a user-local install dir uses a
# user service to avoid init_t denials (#4924).
check_eq "resolve: selinux + fresh + root + ~/.local/bin -> user" "user" \
    "$(resolve_with '
        is_root() { return 0; }
        sudo_noninteractive_ok() { return 1; }
        has_cmd() { return 1; }
        has_system_unit() { return 1; }
        has_user_unit() { return 1; }
        selinux_active() { return 0; }
    ' 0 /home/alice/.local/bin)"

# Off SELinux the same install goes to a system service, as it always did.
# This is the end-to-end companion to the decide-level regression cases above:
# it pins that resolve_service_action really does consult selinux_active,
# rather than the gating being bypassed somewhere on the way through.
check_eq "resolve: no selinux + fresh + root + ~/.local/bin -> system" "system" \
    "$(resolve_with '
        is_root() { return 0; }
        sudo_noninteractive_ok() { return 1; }
        has_cmd() { return 1; }
        has_system_unit() { return 1; }
        has_user_unit() { return 1; }
    ' 0 /home/alice/.local/bin)"

# An existing system unit still wins over the SELinux user-local rule: a re-run
# must refresh what is already installed rather than silently create the other
# kind and leave two.
check_eq "resolve: selinux + existing system unit + ~/.local/bin -> system" "system" \
    "$(resolve_with '
        is_root() { return 0; }
        sudo_noninteractive_ok() { return 1; }
        has_cmd() { return 1; }
        has_system_unit() { return 0; }
        has_user_unit() { return 1; }
        selinux_active() { return 0; }
    ' 0 /home/alice/.local/bin)"

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

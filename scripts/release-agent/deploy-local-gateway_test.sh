#!/usr/bin/env bash
# Regression test for deploy-local-gateway.sh::verify_service (#4492).
#
# This is the function that actually regressed in production on 2026-06-18: a
# stale copy on nova swallowed its verify failure and exited 0 while the
# gateway service was dead. The repo copy is fixed, but verify_service had
# several return-0 fall-throughs worth pinning so they don't come back:
#   - service NOT active                       → exit 1
#   - systemd unit file not found              → exit 1
#   - active but NO running freenet process    → exit 1 (active-but-exited)
#   - active + correct binary + version match  → exit 0  (the only success)
#
# Strategy: run the REAL deploy-local-gateway.sh end-to-end with stub
# systemctl/sudo/pgrep/lsof on PATH and a throwaway --install-path, then assert
# the script's overall exit code (verify_service's result propagates to it via
# VERIFICATION_FAILED → exit 1). No real systemd, no root, no network.
#
# Run manually: bash scripts/release-agent/deploy-local-gateway_test.sh
# Also wired into CI (the Fmt job in .github/workflows/ci.yml).

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_SH="$SCRIPT_DIR/../deploy-local-gateway.sh"

if [[ ! -x "$DEPLOY_SH" ]]; then
    echo "FAIL: $DEPLOY_SH not found or not executable" >&2
    exit 1
fi

FAILURES=0
pass() { echo "ok   - $1"; }
fail() { echo "FAIL - $1" >&2; FAILURES=$((FAILURES + 1)); }

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$TMP/bin" "$TMP/install"

# Fake freenet binary that reports the target version. Used both as the source
# --binary and (after the script copies it) as the installed binary.
cat > "$TMP/source-binary" <<'EOF'
#!/bin/bash
echo "freenet 0.2.78"
EOF
chmod +x "$TMP/source-binary"

# Stub `sudo`: chown is a no-op (we are not root and INSTALL_PATH is in tmp);
# `readlink` is intercepted to report the installed path so the running-binary
# check matches; everything else execs through.
cat > "$TMP/bin/sudo" <<EOF
#!/bin/bash
[[ "\$1" == "-n" || "\$1" == "--non-interactive" ]] && shift
case "\$1" in
  chown) exit 0 ;;
  readlink) echo "$TMP/install/freenet"; exit 0 ;;
  *) exec "\$@" ;;
esac
EOF
chmod +x "$TMP/bin/sudo"

# `lsof` stub: binary is never busy.
printf '#!/bin/bash\nexit 1\n' > "$TMP/bin/lsof"
chmod +x "$TMP/bin/lsof"

export PATH="$TMP/bin:$PATH"

# Write a systemctl stub for a given is-active state, unit-file presence, and
# MainPID. $1=is-active-exit (0=active), $2=unit-found (true/false),
# $3=mainpid.
write_systemctl() {
    local active_exit="$1" unit_found="$2" mainpid="$3"
    local unit_line=""
    [[ "$unit_found" == "true" ]] && unit_line='echo "freenet-gateway.service enabled"'
    cat > "$TMP/bin/systemctl" <<EOF
#!/bin/bash
case "\$1" in
  is-active)
    [[ "\$*" == *"--quiet"* ]] || echo "active"
    exit $active_exit
    ;;
  list-unit-files) $unit_line ; exit 0 ;;
  is-enabled) exit 1 ;;
  show) echo "$mainpid" ;;
  start|stop|restart|disable|enable|daemon-reload) exit 0 ;;
  *) exit 0 ;;
esac
EOF
    chmod +x "$TMP/bin/systemctl"
}

# `pgrep` stub: $1 controls whether a freenet process is "found".
write_pgrep() {
    if [[ "$1" == "found" ]]; then
        printf '#!/bin/bash\necho 4242\n' > "$TMP/bin/pgrep"
    else
        printf '#!/bin/bash\nexit 1\n' > "$TMP/bin/pgrep"
    fi
    chmod +x "$TMP/bin/pgrep"
}

run_deploy() {
    bash "$DEPLOY_SH" \
        --binary "$TMP/source-binary" \
        --service freenet-gateway \
        --install-path "$TMP/install/freenet" \
        >/dev/null 2>&1
    echo $?
}

# ── active + correct binary + matching version → success (exit 0) ────
write_systemctl 0 true 4242   # active, unit found, MainPID 4242
write_pgrep found
rc=$(run_deploy)
if [[ "$rc" == "0" ]]; then
    pass "verify_service: active + running process + version match → exit 0"
else
    fail "verify_service: healthy deploy should exit 0 (got $rc)"
fi

# ── service NOT active → failure (exit 1) — THE incident class ────────
write_systemctl 3 true 4242   # is-active exits non-zero
write_pgrep found
rc=$(run_deploy)
if [[ "$rc" != "0" ]]; then
    pass "verify_service: dead service → non-zero exit (#4492)"
else
    fail "verify_service: dead service must NOT exit 0 (got $rc)"
fi

# ── systemd unit file not found → failure (exit 1) ───────────────────
write_systemctl 0 false 4242  # active reported, but no unit file listed
write_pgrep found
rc=$(run_deploy)
if [[ "$rc" != "0" ]]; then
    pass "verify_service: unit file not found → non-zero exit"
else
    fail "verify_service: missing unit must NOT exit 0 (got $rc)"
fi

# ── active but no running freenet process → failure (exit 1) ─────────
# active (exited) / oneshot misconfig: the unit reads active but there is no
# live process to confirm the new binary. Must fail, not bless.
write_systemctl 0 true 0      # active, unit found, MainPID 0
write_pgrep notfound          # pgrep finds nothing either
rc=$(run_deploy)
if [[ "$rc" != "0" ]]; then
    pass "verify_service: active-but-no-process → non-zero exit"
else
    fail "verify_service: active-but-no-process must NOT exit 0 (got $rc)"
fi

# ── result ────────────────────────────────────────────────────────────
if (( FAILURES > 0 )); then
    echo "$FAILURES deploy-local-gateway.sh test(s) failed" >&2
    exit 1
fi
echo "All deploy-local-gateway.sh tests passed."

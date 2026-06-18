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

# Hard-fail on any fixture-setup error (e.g. a read-only /tmp where mktemp/mkdir
# fail). The body intentionally avoids `set -e` (assertions run in `if`
# conditions and must keep going after a failure), so a broken harness could
# otherwise let a negative-case assertion pass vacuously — guard setup
# explicitly.
TMP="$(mktemp -d)" || { echo "FAIL: mktemp -d failed (is /tmp writable?)" >&2; exit 1; }
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$TMP/bin" "$TMP/install" || { echo "FAIL: could not create $TMP dirs" >&2; exit 1; }

# Fake freenet binary that reports the target version. Used both as the source
# --binary and (after the script copies it) as the installed binary.
cat > "$TMP/source-binary" <<'EOF'
#!/bin/bash
echo "freenet 0.2.78"
EOF
chmod +x "$TMP/source-binary"

# Stub `sudo`: chown is a no-op (we are not root and INSTALL_PATH is in tmp);
# `readlink` is intercepted to report the installed path so the running-binary
# check matches; `cat /proc/<pid>/cgroup` is answered from $TMP/cgroup-content
# so a test can control whether the pgrep-fallback candidate is attributed to
# the target unit; everything else execs through.
cat > "$TMP/bin/sudo" <<EOF
#!/bin/bash
[[ "\$1" == "-n" || "\$1" == "--non-interactive" ]] && shift
case "\$1" in
  chown) exit 0 ;;
  readlink) echo "$TMP/install/freenet"; exit 0 ;;
  cat)
    # Only the cgroup read is modelled; echo whatever the test configured.
    cat "$TMP/cgroup-content" 2>/dev/null || true
    exit 0
    ;;
  *) exec "\$@" ;;
esac
EOF
chmod +x "$TMP/bin/sudo"
# Default cgroup content: belongs to the target unit (so the fallback-path
# candidate, if used, is attributed to this unit). Tests that exercise a
# foreign process overwrite this.
echo "0::/system.slice/freenet-gateway.service" > "$TMP/cgroup-content"

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
    # Capture combined output to $TMP/last-output so assertions can inspect
    # WHICH path was taken (not just the exit code), then echo the exit code.
    bash "$DEPLOY_SH" \
        --binary "$TMP/source-binary" \
        --service freenet-gateway \
        --install-path "$TMP/install/freenet" \
        >"$TMP/last-output" 2>&1
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
# It must reach the GRACEFUL active-but-no-process branch, not abort via set -e
# on the pgrep-not-found command substitution (which would also exit non-zero,
# but at the wrong place, skipping the retry). Assert the branch's message.
if grep -q "no running freenet process could be found" "$TMP/last-output"; then
    pass "verify_service: active-but-no-process reaches graceful branch (no set -e abort on pgrep)"
else
    fail "verify_service: active-but-no-process did not reach its graceful branch (likely set -e abort)"
fi

# ── active-but-no-process for THIS unit, but ANOTHER freenet process is
#    alive → still failure. Pins the MainPID-first + cgroup-attribution: a live
#    process from a DIFFERENT instance (its cgroup names another unit) must NOT
#    bless this unit's empty MainPID.
write_systemctl 0 true 0      # THIS unit: active, MainPID 0 (no process)
write_pgrep found             # a global pgrep WOULD find some freenet process
# ...but that process belongs to a different unit's cgroup.
echo "0::/system.slice/freenet-gateway-hector.service" > "$TMP/cgroup-content"
rc=$(run_deploy)
if [[ "$rc" != "0" ]]; then
    pass "verify_service: MainPID 0 + foreign-cgroup freenet process → non-zero exit (no cross-unit bless)"
else
    fail "verify_service: a foreign-cgroup freenet process must NOT bless this unit's empty MainPID (got $rc)"
fi
# Restore the default (this-unit) cgroup for any later cases.
echo "0::/system.slice/freenet-gateway.service" > "$TMP/cgroup-content"

# ── MainPID 0 (wrapper ExecStart) but pgrep finds the unit's OWN freenet
#    child (cgroup matches) → success. Pins that the cgroup-attributed
#    fallback still blesses a legitimately-running wrapper-launched gateway.
write_systemctl 0 true 0      # active, MainPID 0 (wrapper points MainPID at shell)
write_pgrep found             # freenet child found by command line
echo "0::/system.slice/freenet-gateway.service" > "$TMP/cgroup-content"  # belongs to this unit
rc=$(run_deploy)
if [[ "$rc" == "0" ]]; then
    pass "verify_service: MainPID 0 + this-unit-cgroup freenet child → exit 0 (wrapper ExecStart still verifies)"
else
    fail "verify_service: a this-unit freenet child should verify even when MainPID is 0 (got $rc)"
fi

# ── result ────────────────────────────────────────────────────────────
if (( FAILURES > 0 )); then
    echo "$FAILURES deploy-local-gateway.sh test(s) failed" >&2
    exit 1
fi
echo "All deploy-local-gateway.sh tests passed."

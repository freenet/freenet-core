#!/usr/bin/env bash
# Regression test for the independent service-health gate in
# gateway-auto-update.sh (#4492).
#
# THE incident this pins: on 2026-06-18 (v0.2.78) nova ran a stale
# deploy-local-gateway.sh that swallowed its own verify failure and exited 0
# while the gateway service was DEAD. gateway-auto-update.sh trusted that exit
# code, logged "Deployment successful", and the release was reported green for
# a down gateway.
#
# The fix added verify_service_active(), an independent `systemctl is-active`
# gate that deploy_update() applies AFTER the deploy script — so a deploy
# script that lies (exits 0 on a dead service) is still caught.
#
# Strategy: source the script (its `main` is guarded by a
# BASH_SOURCE==$0 check, so sourcing does NOT run the update) and drive the two
# functions directly with a stub `systemctl`/`sudo` on PATH. No network, no
# real systemd, no root.
#
# Run manually: bash scripts/release-agent/gateway-auto-update_test.sh
# Also wired into CI (the Fmt job in .github/workflows/ci.yml).

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
AUTO_UPDATE_SH="$SCRIPT_DIR/../gateway-auto-update.sh"

if [[ ! -f "$AUTO_UPDATE_SH" ]]; then
    echo "FAIL: $AUTO_UPDATE_SH not found" >&2
    exit 1
fi

FAILURES=0
pass() { echo "ok   - $1"; }
fail() { echo "FAIL - $1" >&2; FAILURES=$((FAILURES + 1)); }

# A throwaway tempdir holding stub binaries and the fake deploy script.
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$TMP/bin"

# Stub `systemctl`: its `is-active` answer is whatever is written to the
# SERVICE_STATE file. Everything else (start/stop/etc.) is a no-op success.
cat > "$TMP/bin/systemctl" <<EOF
#!/bin/bash
case "\$1" in
  is-active)
    state="\$(cat "$TMP/service-state" 2>/dev/null || echo unknown)"
    echo "\$state"
    [[ "\$state" == "active" ]] && exit 0 || exit 3
    ;;
  *) exit 0 ;;
esac
EOF
chmod +x "$TMP/bin/systemctl"

# Stub `sudo`: drop a leading -n/--non-interactive and exec the rest, so
# `sudo "$deploy_script"` just runs the (fake) deploy script.
cat > "$TMP/bin/sudo" <<'EOF'
#!/bin/bash
[[ "$1" == "-n" || "$1" == "--non-interactive" ]] && shift
exec "$@"
EOF
chmod +x "$TMP/bin/sudo"

export PATH="$TMP/bin:$PATH"

# Quiet the script's logger and avoid writing to /var/log during the test.
export LOG_FILE="$TMP/auto-update.log"

# Source the script. `main` is guarded, so this only defines functions.
# shellcheck source=scripts/gateway-auto-update.sh
source "$AUTO_UPDATE_SH"

# Functions log to stderr; keep the test output readable by routing there.
exec 3>&2

# ── verify_service_active ────────────────────────────────────────────

echo "active" > "$TMP/service-state"
if verify_service_active "freenet-gateway" 2>/dev/null; then
    pass "verify_service_active: active service → 0"
else
    fail "verify_service_active: active service should return 0"
fi

echo "failed" > "$TMP/service-state"
if verify_service_active "freenet-gateway" 2>/dev/null; then
    fail "verify_service_active: failed service must NOT return 0 (vega/nova case)"
else
    pass "verify_service_active: failed service → non-zero"
fi

echo "inactive" > "$TMP/service-state"
if verify_service_active "freenet-gateway" 2>/dev/null; then
    fail "verify_service_active: inactive service must NOT return 0"
else
    pass "verify_service_active: inactive service → non-zero"
fi

# ── deploy_update: the lying deploy script (the actual incident) ──────

# A deploy script that prints success and exits 0 EVEN THOUGH the service is
# dead — exactly nova's stale script on 2026-06-18.
LYING_DEPLOY="$TMP/scriptdir/deploy-local-gateway.sh"
mkdir -p "$TMP/scriptdir"
cat > "$LYING_DEPLOY" <<'EOF'
#!/bin/bash
echo "  Verifying service status (freenet-gateway)... ✗"
echo "  ⚠️  Service failed to start"
echo "✅ Deployment complete!"
exit 0
EOF
chmod +x "$LYING_DEPLOY"

# deploy_update resolves the deploy script via SCRIPT_DIR; point it at the dir
# holding our lying script. ALL_INSTANCES/DRY_RUN are read by deploy_update.
SCRIPT_DIR="$TMP/scriptdir"
ALL_INSTANCES=false
DRY_RUN=false
SERVICE_NAME="freenet-gateway"

# Service is DEAD. The deploy script exits 0, but the independent gate must
# still fail deploy_update — this is the regression: pre-fix it returned 0.
echo "failed" > "$TMP/service-state"
if deploy_update "$TMP/fake-binary" 2>/dev/null; then
    fail "deploy_update: lying deploy script + dead service must NOT succeed (#4492)"
else
    pass "deploy_update: dead service caught despite deploy script exiting 0"
fi

# Same deploy script, but now the service is genuinely active → success.
echo "active" > "$TMP/service-state"
if deploy_update "$TMP/fake-binary" 2>/dev/null; then
    pass "deploy_update: active service → success"
else
    fail "deploy_update: active service should succeed"
fi

# --all-instances deliberately skips the single-unit gate (the unit set is
# dynamic there; deploy-local-gateway.sh verifies each instance and its exit
# code is still checked). Pin the skip so a future edit can't accidentally
# invert the condition and drop the gate on the common single-instance path.
echo "failed" > "$TMP/service-state"
ALL_INSTANCES=true
if deploy_update "$TMP/fake-binary" 2>/dev/null; then
    pass "deploy_update: --all-instances skips the single-unit gate (deploy exit code still gates)"
else
    fail "deploy_update: --all-instances should not be blocked by the single-unit gate"
fi
ALL_INSTANCES=false

# ── result ────────────────────────────────────────────────────────────

if (( FAILURES > 0 )); then
    echo "$FAILURES gateway-auto-update.sh test(s) failed" >&2
    exit 1
fi
echo "All gateway-auto-update.sh tests passed."

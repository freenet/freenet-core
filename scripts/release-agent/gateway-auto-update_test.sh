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

# A throwaway tempdir holding stub binaries and the fake deploy script. Hard-
# fail on any fixture-setup error (e.g. a read-only /tmp where mktemp/mkdir
# fail) so a broken harness can NEVER let a negative-case assertion pass
# vacuously. We deliberately do NOT `set -e` for the body (assertions run in
# `if` conditions and must keep going after a failure), so guard setup
# explicitly.
TMP="$(mktemp -d)" || { echo "FAIL: mktemp -d failed (is /tmp writable?)" >&2; exit 1; }
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$TMP/bin" || { echo "FAIL: could not create $TMP/bin" >&2; exit 1; }

# Stub `systemctl`: its `is-active` answer is whatever is written to the
# SERVICE_STATE file. Everything else (start/stop/etc.) is a no-op success.
cat > "$TMP/bin/systemctl" <<EOF
#!/bin/bash
case "\$1" in
  is-active)
    # Per-unit state file if present, else the shared default. Lets a companion
    # be down while the primary is up, which is the case the companion check
    # exists for.
    unit="\${2%.service}"
    if [[ -f "$TMP/state-\$unit" ]]; then
      state="\$(cat "$TMP/state-\$unit")"
    else
      state="\$(cat "$TMP/service-state" 2>/dev/null || echo unknown)"
    fi
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

# ── companion units (WantedBy=) ──────────────────────────────────────
#
# nova runs a SECOND gateway process, freenet-gateway-2, with
# `WantedBy=freenet-gateway.service` and no release-agent of its own. systemd
# does NOT propagate a wanted unit's START FAILURE back to the primary, so
# without this check the primary comes up, the update reports success, and the
# second gateway is silently down — the v0.2.71 failure class again.
#
# SYSTEMD_UNIT_ROOTS points the lookup at a fixture instead of /etc/systemd.

export SYSTEMD_UNIT_ROOTS="$TMP/units"
WANTS="$TMP/units/freenet-gateway.service.wants"
mkdir -p "$WANTS" || { echo "FAIL: could not create $WANTS" >&2; exit 1; }

echo "active" > "$TMP/service-state"

# (1) No companions: the loop must fall through, not fail.
if verify_service_active "freenet-gateway" 2>/dev/null; then
    pass "companions: none configured -> 0"
else
    fail "companions: an empty wants dir must not fail the update"
fi

# (2) Companion present and active.
touch "$WANTS/freenet-gateway-2.service"
echo "active" > "$TMP/state-freenet-gateway-2"
if verify_service_active "freenet-gateway" 2>/dev/null; then
    pass "companions: active companion -> 0"
else
    fail "companions: an active companion must not fail the update"
fi

# (3) THE CASE THIS EXISTS FOR: primary active, companion down. Must fail.
echo "inactive" > "$TMP/state-freenet-gateway-2"
if verify_service_active "freenet-gateway" 2>/dev/null; then
    fail "companions: primary active + companion INACTIVE must NOT return 0 (silent gateway outage)"
else
    pass "companions: inactive companion -> non-zero"
fi

# (4) A failed companion is equally unacceptable.
echo "failed" > "$TMP/state-freenet-gateway-2"
if verify_service_active "freenet-gateway" 2>/dev/null; then
    fail "companions: primary active + companion FAILED must NOT return 0"
else
    pass "companions: failed companion -> non-zero"
fi

# (5) A wants dir that does not exist must not fail (hosts without companions).
rm -f "$TMP/state-freenet-gateway-2"
export SYSTEMD_UNIT_ROOTS="$TMP/no-such-root"
if verify_service_active "freenet-gateway" 2>/dev/null; then
    pass "companions: missing wants root -> 0"
else
    fail "companions: a missing wants root must not fail the update"
fi
unset SYSTEMD_UNIT_ROOTS

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

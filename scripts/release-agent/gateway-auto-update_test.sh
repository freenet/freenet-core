#!/usr/bin/env bash
# Regression tests for gateway-auto-update.sh: the independent service-health
# gate (#4492), the companion restart (#6336), and the signed-checksum
# verification that must pass before a downloaded release is installed.
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
# Captured before sourcing: the sourced script assigns its own SCRIPT_DIR.
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
UPDATE_RS="$REPO_ROOT/crates/core/src/bin/commands/update.rs"
DOCKER_KEY_DER="$REPO_ROOT/docker/freenet-node/release-signing-key.der"

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

# The trusted key as the script ships it, before any test replaces it.
PROD_PUBKEY_HEX="$RELEASE_PUBKEY_HEX"

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

# ── schedule_companion_restart (#6336) ───────────────────────────────
# The companion gateway is restarted by freenet-gateway-stagger.service, queued
# after a primary update (and again on the "already latest" path).
SC="$TMP/sc"
mkdir -p "$SC"
cat > "$SC/systemctl" <<EOF
#!/bin/bash
case "\$1" in
  list-unit-files) [[ -f "$SC/has-unit" ]] && echo "\$2 static"; exit 0 ;;
  start) echo "\$*" >> "$SC/started"; [[ -f "$SC/start-fails" ]] && exit 1; exit 0 ;;
esac
exit 0
EOF
chmod +x "$SC/systemctl"
sc_reset() { rm -f "$SC/has-unit" "$SC/started" "$SC/start-fails"; }
# shellcheck disable=SC2030  # subshell-local on purpose
sc_run() { ( PATH="$SC:$PATH"; SERVICE_NAME="${1:-freenet-gateway}"; ALL_INSTANCES="${2:-false}"; schedule_companion_restart >/dev/null 2>&1 ); }

sc_reset; touch "$SC/has-unit"
if sc_run && grep -q 'start --no-block freenet-gateway-stagger.service' "$SC/started" 2>/dev/null; then
    pass "schedule_companion_restart: queues the stagger unit with --no-block"
else
    fail "schedule_companion_restart: should queue freenet-gateway-stagger.service"
fi

sc_reset
if sc_run && [[ ! -e "$SC/started" ]]; then
    pass "schedule_companion_restart: no stagger unit installed -> skipped, success"
else
    fail "schedule_companion_restart: must skip hosts without the stagger unit"
fi

sc_reset; touch "$SC/has-unit"
if sc_run freenet-gateway-hector && [[ ! -e "$SC/started" ]]; then
    pass "schedule_companion_restart: a non-default service is skipped"
else
    fail "schedule_companion_restart: must not act for a non-default unit"
fi

sc_reset; touch "$SC/has-unit"
if sc_run freenet-gateway true && grep -q 'start --no-block freenet-gateway-stagger.service' "$SC/started" 2>/dev/null; then
    pass "schedule_companion_restart: --all-instances also queues the stagger"
else
    fail "schedule_companion_restart: --all-instances deploys the primary too, so it must queue the stagger"
fi

sc_reset; touch "$SC/has-unit" "$SC/start-fails"
if ! sc_run; then
    pass "schedule_companion_restart: failure to queue -> non-zero"
else
    fail "schedule_companion_restart: a failed queue must not be reported as success"
fi

# ── release key: one copy, matching the node's ───────────────────────

# The key must live in exactly one assignment, and that assignment must not
# read the environment (an env-supplied trusted key on a root script is a
# bypass).
key_lines=$(grep -cE '^RELEASE_PUBKEY_HEX=' "$AUTO_UPDATE_SH")
if [[ "$key_lines" == 1 ]] && grep -qE '^RELEASE_PUBKEY_HEX="[0-9a-f]{64}"$' "$AUTO_UPDATE_SH"; then
    pass "release key: one literal assignment, not taken from the environment"
else
    fail "release key: expected exactly one RELEASE_PUBKEY_HEX=\"<64 hex>\" line, found $key_lines (or a non-literal value)"
fi

# FREENET_RELEASE_PUBKEY's byte array in update.rs, as lowercase hex.
rs_hex=$(awk '/^const FREENET_RELEASE_PUBKEY: \[u8; 32\] = \[/ { f = 1; next } f && /^\];/ { exit } f' "$UPDATE_RS" \
    | grep -oE '0x[0-9a-fA-F]{2}' | sed 's/^0x//' | tr -d '\n' | tr 'A-F' 'a-f')
if [[ ${#rs_hex} == 64 && "$rs_hex" == "$PROD_PUBKEY_HEX" ]]; then
    pass "release key: matches FREENET_RELEASE_PUBKEY in update.rs"
else
    fail "release key: gateway-auto-update.sh has '$PROD_PUBKEY_HEX' but update.rs FREENET_RELEASE_PUBKEY is '$rs_hex'; a key rotation missed a copy"
fi

der_hex=$(tail -c 32 "$DOCKER_KEY_DER" 2>/dev/null | od -An -v -tx1 | tr -d ' \n')
if [[ "$der_hex" == "$PROD_PUBKEY_HEX" ]]; then
    pass "release key: matches docker/freenet-node/release-signing-key.der"
else
    fail "release key: docker release-signing-key.der carries '$der_hex', script has '$PROD_PUBKEY_HEX'"
fi

# ── signed-checksum verification, end to end through main ────────────
#
# Drives main() with a pinned target version against a stub curl serving
# fixture release files, a fake deploy script that "installs" by copying the
# binary to INSTALL_PATH, and the stub systemctl/sudo above. A refused update
# must exit non-zero, never reach the deploy script, and leave INSTALL_PATH
# untouched.
#
# Fixtures are signed with a throwaway ed25519 key generated here. The script's
# trusted key is replaced by assigning RELEASE_PUBKEY_HEX in THIS shell after
# sourcing; nothing a caller of the installed script controls can do that.

E2E="$TMP/e2e"
FIX="$E2E/release"
mkdir -p "$E2E/bin" "$E2E/scriptdir" "$E2E/installed" "$E2E/build" "$FIX" \
    || { echo "FAIL: could not create $E2E" >&2; exit 1; }

if ! openssl genpkey -algorithm ed25519 -out "$E2E/test.key" 2>/dev/null \
    || ! openssl genpkey -algorithm ed25519 -out "$E2E/other.key" 2>/dev/null; then
    echo "FAIL: openssl cannot generate ed25519 keys; the verification tests cannot run" >&2
    exit 1
fi
TEST_PUBKEY_HEX=$(openssl pkey -in "$E2E/test.key" -pubout -outform DER | tail -c 32 | od -An -v -tx1 | tr -d ' \n')
[[ ${#TEST_PUBKEY_HEX} == 64 ]] || { echo "FAIL: could not derive the test public key" >&2; exit 1; }

ASSET="freenet-$(detect_arch)-unknown-linux-musl.tar.gz"

# The release binary the archive carries, and the one "installed" beforehand.
printf '#!/bin/bash\necho "Freenet version: 0.2.999"\n' > "$E2E/build/freenet"
chmod +x "$E2E/build/freenet"
tar -czf "$E2E/good.tar.gz" -C "$E2E/build" freenet || { echo "FAIL: could not build the fixture archive" >&2; exit 1; }
GOOD_SHA=$(sha256sum "$E2E/good.tar.gz" | awk '{print $1}')
OTHER_LINE="$(printf '%064d' 0)  fdev-$(detect_arch)-unknown-linux-musl.tar.gz"

# curl stub: copies $FIX/<basename of the URL> to the -o path, or fails like
# `curl --fail` on a 404 when the fixture is absent.
cat > "$E2E/bin/curl" <<EOF
#!/bin/bash
out=""; url=""
while [[ \$# -gt 0 ]]; do
    case "\$1" in
        -o) out="\$2"; shift 2 ;;
        -*) shift ;;
        *) url="\$1"; shift ;;
    esac
done
src="$FIX/\${url##*/}"
[[ -f "\$src" ]] || exit 22
cp "\$src" "\$out"
EOF
chmod +x "$E2E/bin/curl"

cat > "$E2E/scriptdir/deploy-local-gateway.sh" <<EOF
#!/bin/bash
[[ "\$1" == "--binary" ]] || exit 2
cp "\$2" "$E2E/installed/freenet" && touch "$E2E/deployed"
EOF
chmod +x "$E2E/scriptdir/deploy-local-gateway.sh"

# write_release <sums content>: the archive plus a manifest signed by test.key.
write_release() {
    rm -f "$FIX"/*
    cp "$E2E/good.tar.gz" "$FIX/$ASSET"
    printf '%s\n' "$1" > "$FIX/SHA256SUMS.txt"
    openssl pkeyutl -sign -inkey "$E2E/test.key" -rawin \
        -in "$FIX/SHA256SUMS.txt" -out "$FIX/SHA256SUMS.txt.sig"
}

# run_update [key hex]: one full main() run. Prints its exit code.
run_update() {
    local key="${1:-$TEST_PUBKEY_HEX}"
    printf '#!/bin/bash\necho "Freenet version: 0.2.1"\n' > "$E2E/installed/freenet"
    chmod +x "$E2E/installed/freenet"
    rm -f "$E2E/deployed"
    : > "$E2E/log"
    echo "active" > "$TMP/service-state"
    (
        # shellcheck disable=SC2031  # subshell-local on purpose
        PATH="$E2E/bin:$PATH"
        RELEASE_PUBKEY_HEX="$key"
        SCRIPT_DIR="$E2E/scriptdir"
        INSTALL_PATH="$E2E/installed/freenet"
        DOWNLOAD_DIR=""
        TARGET_VERSION="0.2.999"
        FORCE_UPDATE=false CHECK_ONLY=false DRY_RUN=false ALL_INSTANCES=false
        SERVICE_NAME="freenet-gateway"
        export SYSTEMD_UNIT_ROOTS="$E2E/no-units"
        main
    ) > /dev/null 2> "$E2E/log"
    echo $?
}

installed_version() { "$E2E/installed/freenet" --version; }

# expect_refused <description>: the last run_update must have refused cleanly.
expect_refused() {
    local desc="$1" rc="$2"
    if [[ "$rc" != 0 && ! -e "$E2E/deployed" ]] \
        && [[ "$(installed_version)" == "Freenet version: 0.2.1" ]] \
        && grep -qF "Release verification FAILED" "$E2E/log"; then
        pass "verify: $desc -> refused, nothing installed"
    else
        fail "verify: $desc must refuse before installing (rc=$rc, deployed=$([[ -e "$E2E/deployed" ]] && echo yes || echo no), installed='$(installed_version)')"
        sed 's/^/    | /' "$E2E/log" >&2
    fi
}

# (1) Good signature + good hash: installs.
write_release "$(printf '%s  %s\n%s' "$GOOD_SHA" "$ASSET" "$OTHER_LINE")"
rc=$(run_update)
if [[ "$rc" == 0 && -e "$E2E/deployed" ]] \
    && [[ "$(installed_version)" == "Freenet version: 0.2.999" ]] \
    && grep -qF "SHA256SUMS.txt signature verified" "$E2E/log" \
    && grep -qF "sha256 matches the signed manifest" "$E2E/log"; then
    pass "verify: good signature + good hash -> installed"
else
    fail "verify: good signature + good hash should install (rc=$rc)"
    sed 's/^/    | /' "$E2E/log" >&2
fi

# (2) The same, correctly signed release under the SHIPPED key: refused. Proves
# the trusted key is the production one unless this harness replaces it.
rc=$(run_update "$PROD_PUBKEY_HEX")
expect_refused "manifest signed by a key other than the release key" "$rc"

# (3) Bad signature: signed by a different key.
write_release "$(printf '%s  %s' "$GOOD_SHA" "$ASSET")"
openssl pkeyutl -sign -inkey "$E2E/other.key" -rawin \
    -in "$FIX/SHA256SUMS.txt" -out "$FIX/SHA256SUMS.txt.sig"
rc=$(run_update)
expect_refused "signature by the wrong key" "$rc"

# (4) SHA256SUMS.txt altered after signing (hash rewritten to the tampered
# archive's, which is what an attacker serving their own binary would do).
write_release "$(printf '%s  %s' "$(printf '%064d' 1)" "$ASSET")"
printf '%s  %s\n' "$GOOD_SHA" "$ASSET" > "$FIX/SHA256SUMS.txt"
rc=$(run_update)
expect_refused "SHA256SUMS.txt tampered after signing" "$rc"

# (5) Validly signed manifest whose hash does not match the archive.
write_release "$(printf '%s  %s' "$(printf '%064d' 7)" "$ASSET")"
rc=$(run_update)
expect_refused "archive hash mismatch" "$rc"

# (6) Missing signature file.
write_release "$(printf '%s  %s' "$GOOD_SHA" "$ASSET")"
rm -f "$FIX/SHA256SUMS.txt.sig"
rc=$(run_update)
expect_refused "missing SHA256SUMS.txt.sig" "$rc"

# (7) Signed manifest with no line for the archive. The near-miss names (a
# suffix and a prefix of the real one) must not be taken as a match.
write_release "$(printf '%s  %s.old\n%s  x%s\n%s' "$GOOD_SHA" "$ASSET" "$GOOD_SHA" "$ASSET" "$OTHER_LINE")"
rc=$(run_update)
expect_refused "no SHA256SUMS.txt line for the archive" "$rc"

# (8) Two lines for the archive, even with the same correct hash.
write_release "$(printf '%s  %s\n%s  %s' "$GOOD_SHA" "$ASSET" "$GOOD_SHA" "$ASSET")"
rc=$(run_update)
expect_refused "duplicate SHA256SUMS.txt lines for the archive" "$rc"

# (9) Missing SHA256SUMS.txt.
write_release "$(printf '%s  %s' "$GOOD_SHA" "$ASSET")"
rm -f "$FIX/SHA256SUMS.txt"
rc=$(run_update)
expect_refused "missing SHA256SUMS.txt" "$rc"

# (10) OpenSSL too old for `pkeyutl -rawin`: refused before verifying.
write_release "$(printf '%s  %s' "$GOOD_SHA" "$ASSET")"
REAL_OPENSSL=$(command -v openssl)
cat > "$E2E/bin/openssl" <<EOF
#!/bin/bash
[[ "\$1" == version ]] && { echo "OpenSSL 1.1.1w  11 Sep 2023"; exit 0; }
exec "$REAL_OPENSSL" "\$@"
EOF
chmod +x "$E2E/bin/openssl"
rc=$(run_update)
expect_refused "OpenSSL older than 3.0" "$rc"
rm -f "$E2E/bin/openssl"

# ── result ────────────────────────────────────────────────────────────

if (( FAILURES > 0 )); then
    echo "$FAILURES gateway-auto-update.sh test(s) failed" >&2
    exit 1
fi
echo "All gateway-auto-update.sh tests passed."

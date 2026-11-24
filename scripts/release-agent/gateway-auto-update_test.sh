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

# make_archive <version>: an archive whose `freenet` reports <version>.
make_archive() {
    local v="$1" d="$E2E/build-$1"
    mkdir -p "$d" || return 1
    printf '#!/bin/bash\necho "Freenet version: %s"\n' "$v" > "$d/freenet"
    chmod +x "$d/freenet"
    tar -czf "$E2E/archive-$v.tar.gz" -C "$d" freenet
}
for v in 0.2.999 0.2.998 0.2.5 0.2.999-rc1; do
    make_archive "$v" || { echo "FAIL: could not build the fixture archive for $v" >&2; exit 1; }
done
cp "$E2E/archive-0.2.999.tar.gz" "$E2E/good.tar.gz"
GOOD_SHA=$(sha256sum "$E2E/good.tar.gz" | awk '{print $1}')
OTHER_LINE="$(printf '%064d' 0)  fdev-$(detect_arch)-unknown-linux-musl.tar.gz"

# curl stub: logs its argv (one line per call), then copies $FIX/<basename of
# the URL> to the -o path, or fails like `curl --fail` on a 404.
cat > "$E2E/bin/curl" <<EOF
#!/bin/bash
printf '%s\n' "\$*" >> "$E2E/curl-argv"
out=""; url=""
while [[ \$# -gt 0 ]]; do
    case "\$1" in
        -o) out="\$2"; shift 2 ;;
        --max-filesize|--max-time|--proto|--proto-redir|-H) shift 2 ;;
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

# write_release <sums content> [archive]: the archive plus a manifest signed by test.key.
write_release() {
    rm -f "$FIX"/*
    cp "${2:-$E2E/good.tar.gz}" "$FIX/$ASSET"
    printf '%s\n' "$1" > "$FIX/SHA256SUMS.txt"
    openssl pkeyutl -sign -inkey "$E2E/test.key" -rawin \
        -in "$FIX/SHA256SUMS.txt" -out "$FIX/SHA256SUMS.txt.sig"
}

# Per-run knobs; ru_reset restores the defaults (an upgrade 0.2.1 -> 0.2.999).
ru_reset() {
    RU_KEY="$TEST_PUBKEY_HEX" RU_INSTALLED=0.2.1 RU_TARGET=0.2.999
    RU_FORCE=false RU_ALLOW=false RU_SUDO_USER="" RU_DLDIR=""
}
ru_reset

# run_update: one full main() run with the RU_* knobs. Prints its exit code.
run_update() {
    printf '#!/bin/bash\necho "Freenet version: %s"\n' "$RU_INSTALLED" > "$E2E/installed/freenet"
    chmod +x "$E2E/installed/freenet"
    rm -f "$E2E/deployed" "$E2E/curl-argv"
    : > "$E2E/log"
    echo "active" > "$TMP/service-state"
    (
        # shellcheck disable=SC2030,SC2031  # subshell-local on purpose
        PATH="$E2E/bin:$PATH"
        RELEASE_PUBKEY_HEX="$RU_KEY"
        SCRIPT_DIR="$E2E/scriptdir"
        INSTALL_PATH="$E2E/installed/freenet"
        DOWNLOAD_DIR="$RU_DLDIR"
        TARGET_VERSION="$RU_TARGET"
        FORCE_UPDATE="$RU_FORCE" ALLOW_DOWNGRADE="$RU_ALLOW"
        CHECK_ONLY=false DRY_RUN=false ALL_INSTANCES=false
        SERVICE_NAME="freenet-gateway"
        if [[ -n "$RU_SUDO_USER" ]]; then export SUDO_USER="$RU_SUDO_USER"; else unset SUDO_USER; fi
        export SYSTEMD_UNIT_ROOTS="$E2E/no-units"
        main
    ) > /dev/null 2> "$E2E/log"
    echo $?
}

installed_version() { "$E2E/installed/freenet" --version; }
dump_log() { sed 's/^/    | /' "$E2E/log" >&2; }

# expect_installed <description> <rc> <version>
expect_installed() {
    local desc="$1" rc="$2" v="$3"
    if [[ "$rc" == 0 && -e "$E2E/deployed" ]] \
        && [[ "$(installed_version)" == "Freenet version: $v" ]]; then
        pass "verify: $desc -> installed"
    else
        fail "verify: $desc should install v$v (rc=$rc, installed='$(installed_version)')"
        dump_log
    fi
}

# expect_refused <description> <rc> [log text, default "Release verification FAILED"]:
# the last run_update must have refused before installing anything.
expect_refused() {
    local desc="$1" rc="$2" why="${3:-Release verification FAILED}"
    if [[ "$rc" != 0 && ! -e "$E2E/deployed" ]] \
        && [[ "$(installed_version)" == "Freenet version: $RU_INSTALLED" ]] \
        && grep -qF -- "$why" "$E2E/log"; then
        pass "verify: $desc -> refused, nothing installed"
    else
        fail "verify: $desc must refuse before installing, logging '$why' (rc=$rc, deployed=$([[ -e "$E2E/deployed" ]] && echo yes || echo no), installed='$(installed_version)')"
        dump_log
    fi
}

# (1) Good signature + good hash: installs.
write_release "$(printf '%s  %s\n%s' "$GOOD_SHA" "$ASSET" "$OTHER_LINE")"
rc=$(run_update)
expect_installed "good signature + good hash" "$rc" 0.2.999
if grep -qF "SHA256SUMS.txt signature verified" "$E2E/log" \
    && grep -qF "sha256 matches the signed manifest" "$E2E/log"; then
    pass "verify: a successful install logs both verification steps"
else
    fail "verify: the install log should show the signature and the hash verified"
fi

# The downloads: every curl call is --fail, HTTPS-only across redirects and
# size-bounded, and lands in a fresh mktemp directory, not a fixed path.
calls=$(wc -l < "$E2E/curl-argv" 2>/dev/null || echo 0)
hardened=$(grep -cE -- '--fail .*--proto =https --proto-redir =https --max-filesize [0-9]+ --max-time [0-9]+' "$E2E/curl-argv" 2>/dev/null || true)
fresh=$(grep -cE -- '-o /tmp/freenet-update\.[A-Za-z0-9]{6}/' "$E2E/curl-argv" 2>/dev/null || true)
if [[ "$calls" == 3 && "$hardened" == 3 && "$fresh" == 3 ]]; then
    pass "downloads: 3 curl calls, all --fail + https-only + bounded, into a fresh mktemp dir"
else
    fail "downloads: expected 3 hardened curl calls into /tmp/freenet-update.XXXXXX (calls=$calls hardened=$hardened fresh=$fresh)"
    sed 's/^/    | /' "$E2E/curl-argv" >&2
fi

# (2) The same, correctly signed release under the SHIPPED key: refused. Proves
# the trusted key is the production one unless this harness replaces it.
RU_KEY="$PROD_PUBKEY_HEX"
rc=$(run_update)
expect_refused "manifest signed by a key other than the release key" "$rc"
ru_reset

# (2b) Executed (not sourced), with RELEASE_PUBKEY_HEX EXPORTED as the test
# key: the environment must not replace the shipped key, so the test-signed
# release is refused. Runs a copy beside a fake deploy script, so a failure
# here can never reach the real deploy-local-gateway.sh.
mkdir -p "$E2E/copy"
cp "$AUTO_UPDATE_SH" "$E2E/copy/gateway-auto-update.sh"
cp "$E2E/scriptdir/deploy-local-gateway.sh" "$E2E/copy/"
printf '#!/bin/bash\necho "Freenet version: 0.2.1"\n' > "$E2E/installed/freenet"
rm -f "$E2E/deployed"
# `|| rc=$?`: sourcing the script turned on errexit in this shell.
rc=0
# shellcheck disable=SC2031  # PATH here is the parent shell's, as intended
env -u SUDO_USER PATH="$E2E/bin:$PATH" RELEASE_PUBKEY_HEX="$TEST_PUBKEY_HEX" \
    INSTALL_PATH="$E2E/installed/freenet" LOG_FILE="$E2E/exec.log" \
    SYSTEMD_UNIT_ROOTS="$E2E/no-units" \
    bash "$E2E/copy/gateway-auto-update.sh" --force --target-version v0.2.999 > /dev/null 2> "$E2E/log" || rc=$?
expect_refused "exported RELEASE_PUBKEY_HEX on direct execution" "$rc" "signature is INVALID for release key $PROD_PUBKEY_HEX"

# (3) Bad signature: signed by a different key.
write_release "$(printf '%s  %s' "$GOOD_SHA" "$ASSET")"
openssl pkeyutl -sign -inkey "$E2E/other.key" -rawin \
    -in "$FIX/SHA256SUMS.txt" -out "$FIX/SHA256SUMS.txt.sig"
rc=$(run_update)
expect_refused "signature by the wrong key" "$rc"

# (3b) A 65-byte signature (valid one plus a trailing byte).
write_release "$(printf '%s  %s' "$GOOD_SHA" "$ASSET")"
printf 'x' >> "$FIX/SHA256SUMS.txt.sig"
rc=$(run_update)
expect_refused "65-byte signature" "$rc" "is 65 bytes, expected a 64-byte"

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

# (7b) A single near-miss line carrying the CORRECT hash, and no exact line.
write_release "$(printf '%s  ./%s' "$GOOD_SHA" "$ASSET")"
rc=$(run_update)
expect_refused "only a near-miss line (correct hash)" "$rc"

# (7c) A near-miss line plus the exact line: the exact one decides.
write_release "$(printf '%s  %s.old\n%s  %s' "$(printf '%064d' 3)" "$ASSET" "$GOOD_SHA" "$ASSET")"
rc=$(run_update)
expect_installed "near-miss line plus the exact line" "$rc" 0.2.999

# (8) Two lines for the archive, even with the same correct hash.
write_release "$(printf '%s  %s\n%s  %s' "$GOOD_SHA" "$ASSET" "$GOOD_SHA" "$ASSET")"
rc=$(run_update)
expect_refused "duplicate SHA256SUMS.txt lines for the archive" "$rc"

# (9) Missing SHA256SUMS.txt.
write_release "$(printf '%s  %s' "$GOOD_SHA" "$ASSET")"
rm -f "$FIX/SHA256SUMS.txt"
rc=$(run_update)
expect_refused "missing SHA256SUMS.txt" "$rc"

# (10) OpenSSL too old for `pkeyutl -rawin`, or not OpenSSL at all: refused
# up front by check_dependencies, before any download.
write_release "$(printf '%s  %s' "$GOOD_SHA" "$ASSET")"
REAL_OPENSSL=$(command -v openssl)
for fake in "OpenSSL 1.1.1w  11 Sep 2023" "LibreSSL 3.8.2"; do
    cat > "$E2E/bin/openssl" <<EOF
#!/bin/bash
[[ "\$1" == version ]] && { echo "$fake"; exit 0; }
exec "$REAL_OPENSSL" "\$@"
EOF
    chmod +x "$E2E/bin/openssl"
    rc=$(run_update)
    expect_refused "openssl reporting '$fake'" "$rc" "need OpenSSL >= 3.0"
    if [[ ! -s "$E2E/curl-argv" ]]; then
        pass "verify: openssl '$fake' is refused before any download"
    else
        fail "verify: openssl '$fake' must be refused before downloading"
    fi
done
rm -f "$E2E/bin/openssl"

# (11) Replay: an OLDER release's archive, correctly signed, served under the
# requested tag. The asset name carries no version, so only the binary's own
# version can tell; a mismatch must be fatal before install.
write_release "$(printf '%s  %s' "$(sha256sum "$E2E/archive-0.2.998.tar.gz" | awk '{print $1}')" "$ASSET")" "$E2E/archive-0.2.998.tar.gz"
rc=$(run_update)
expect_refused "archive whose binary reports a different version" "$rc" "contains a binary reporting version '0.2.998'"

# (12) Downgrade: target older than installed is refused without
# --allow-downgrade, even with --force, and before any download.
write_release "$(printf '%s  %s' "$(sha256sum "$E2E/archive-0.2.5.tar.gz" | awk '{print $1}')" "$ASSET")" "$E2E/archive-0.2.5.tar.gz"
RU_INSTALLED=0.2.9 RU_TARGET=0.2.5 RU_FORCE=true
rc=$(run_update)
expect_refused "--force downgrade 0.2.9 -> 0.2.5 without --allow-downgrade" "$rc" "Refusing to downgrade"
if [[ ! -s "$E2E/curl-argv" ]]; then
    pass "verify: a refused downgrade downloads nothing"
else
    fail "verify: a refused downgrade must not download"
fi

# (12b) The operator's rollback: --allow-downgrade installs the older release.
RU_ALLOW=true
rc=$(run_update)
expect_installed "--force --allow-downgrade 0.2.9 -> 0.2.5" "$rc" 0.2.5

# (12c) ...but not when sudo says the caller is the release-agent account,
# whose sudoers wildcard could otherwise smuggle the flag in.
RU_SUDO_USER=freenet-update
rc=$(run_update)
expect_refused "--allow-downgrade from the release-agent account" "$rc" "refusing it from the release-agent account"
ru_reset

# (13) --force reinstalls the SAME version (not a downgrade).
write_release "$(printf '%s  %s' "$GOOD_SHA" "$ASSET")"
RU_INSTALLED=0.2.999 RU_FORCE=true
rc=$(run_update)
expect_installed "--force reinstall of the installed version" "$rc" 0.2.999
ru_reset

# (14) A DOWNLOAD_DIR override that is not a private 0700 directory is refused;
# a private one is used.
mkdir -p "$E2E/shared-dl" && chmod 755 "$E2E/shared-dl"
RU_DLDIR="$E2E/shared-dl"
rc=$(run_update)
expect_refused "DOWNLOAD_DIR override with mode 0755" "$rc" "must be an existing directory"
ln -sfn "$E2E/shared-dl" "$E2E/link-dl"
RU_DLDIR="$E2E/link-dl"
rc=$(run_update)
expect_refused "DOWNLOAD_DIR override that is a symlink" "$rc" "must be an existing directory"
mkdir -p "$E2E/private-dl" && chmod 700 "$E2E/private-dl"
echo keep > "$E2E/private-dl/operator-file"
RU_DLDIR="$E2E/private-dl"
rc=$(run_update)
expect_installed "DOWNLOAD_DIR override that is private (0700, ours)" "$rc" 0.2.999
# Only the run's own subdirectory is removed; the operator's directory and
# what was already in it stay.
if [[ -f "$E2E/private-dl/operator-file" ]] \
    && [[ "$(find "$E2E/private-dl" -mindepth 1 | wc -l)" == 1 ]] \
    && grep -qE -- '-o '"$E2E"'/private-dl/freenet-update\.[A-Za-z0-9]{6}/' "$E2E/curl-argv"; then
    pass "downloads: an override gets a fresh subdirectory, removed afterwards; its own contents are kept"
else
    fail "downloads: cleanup must remove only the run's subdirectory of DOWNLOAD_DIR"
    find "$E2E/private-dl" >&2
fi
ru_reset

# (15) Prereleases. A prerelease ranks BELOW its release, and an rc install
# reads back as itself (not as "Update failed").
rc1_sha=$(sha256sum "$E2E/archive-0.2.999-rc1.tar.gz" | awk '{print $1}')
write_release "$(printf '%s  %s' "$rc1_sha" "$ASSET")" "$E2E/archive-0.2.999-rc1.tar.gz"
RU_TARGET=0.2.999-rc1
rc=$(run_update)
expect_installed "an rc release (0.2.1 -> 0.2.999-rc1)" "$rc" 0.2.999-rc1
RU_INSTALLED=0.2.999 RU_FORCE=true
rc=$(run_update)
expect_refused "--force from 0.2.999 to its own rc (a downgrade)" "$rc" "Refusing to downgrade"
ru_reset
write_release "$(printf '%s  %s' "$GOOD_SHA" "$ASSET")"
RU_INSTALLED=0.2.999-rc1
rc=$(run_update)
expect_installed "rc to its release (0.2.999-rc1 -> 0.2.999), no --force needed" "$rc" 0.2.999
ru_reset

# semver_cmp, directly: SemVer 2.0 precedence, not `sort -V`.
sv_fail=0
while read -r a b want; do
    got=$(semver_cmp "$a" "$b")
    if [[ "$got" != "$want" ]]; then
        fail "semver_cmp $a $b: got $got, want $want"
        sv_fail=1
    fi
done <<'EOF'
0.2.150-rc1 0.2.150 -1
0.2.150 0.2.150-rc1 1
0.2.150 0.2.150 0
0.2.150+build.7 0.2.150 0
0.2.100 0.2.99 1
0.2.9 0.2.10 -1
1.0.0-alpha 1.0.0-alpha.1 -1
1.0.0-alpha.1 1.0.0-alpha.beta -1
1.0.0-alpha.beta 1.0.0-beta -1
1.0.0-beta.2 1.0.0-beta.11 -1
1.0.0-beta.11 1.0.0-rc.1 -1
1.0.0-rc.1 1.0.0 -1
EOF
(( sv_fail )) || pass "semver_cmp: SemVer precedence, prereleases below their release"

# ── verify_release_download, called directly ─────────────────────────
VD="$E2E/direct"
vd_case() {
    # vd_case <description> <log text>: $VD is already set up; must refuse.
    if verify_release_download "$VD" "$ASSET" 2> "$E2E/vd.log"; then
        fail "verify_release_download: $1 must return non-zero"
    elif grep -qF -- "$2" "$E2E/vd.log"; then
        pass "verify_release_download: $1 -> non-zero"
    else
        fail "verify_release_download: $1 refused, but not for the expected reason ('$2')"
        sed 's/^/    | /' "$E2E/vd.log" >&2
    fi
}
vd_setup() {
    rm -rf "$VD"; mkdir -p "$VD"
    cp "$E2E/good.tar.gz" "$VD/$ASSET"
    printf '%s\n' "$1" > "$VD/SHA256SUMS.txt"
    openssl pkeyutl -sign -inkey "$E2E/test.key" -rawin \
        -in "$VD/SHA256SUMS.txt" -out "$VD/SHA256SUMS.txt.sig"
}
RELEASE_PUBKEY_HEX="$TEST_PUBKEY_HEX"
vd_setup "$(printf '%s  %s' "$GOOD_SHA" "$ASSET")"
if verify_release_download "$VD" "$ASSET" 2> /dev/null; then
    pass "verify_release_download: good manifest -> 0"
else
    fail "verify_release_download: good manifest should return 0"
fi
vd_setup "x"; : > "$VD/SHA256SUMS.txt"
vd_case "empty manifest" "SHA256SUMS.txt is missing or empty"
vd_setup "$(printf '%s  %s' "$GOOD_SHA" "$ASSET")"; rm -f "$VD/SHA256SUMS.txt.sig"
vd_case "missing signature" "SHA256SUMS.txt.sig is missing"
vd_setup "$(printf '%s  %s' "${GOOD_SHA:0:63}" "$ASSET")"
vd_case "hash of the wrong length" "malformed hash"
vd_setup "$(printf '%sz  %s' "${GOOD_SHA:0:63}" "$ASSET")"
vd_case "hash with a non-hex digit" "malformed hash"
if [[ -z "$(find "$VD" -name '*.der')" ]]; then
    pass "verify_release_download: the trusted key is never written into the download dir"
else
    fail "verify_release_download: a key file was left in the download dir"
fi
RELEASE_PUBKEY_HEX="$PROD_PUBKEY_HEX"

# ── result ────────────────────────────────────────────────────────────

if (( FAILURES > 0 )); then
    echo "$FAILURES gateway-auto-update.sh test(s) failed" >&2
    exit 1
fi
echo "All gateway-auto-update.sh tests passed."

#!/bin/sh
# Smoke tests for scripts/uninstall.sh.
#
# Runs each test case against a tempdir standing in for $HOME so nothing
# real gets touched. Every case asserts both what MUST be removed and what
# MUST be preserved — the latter is the safety invariant (we shouldn't wipe
# shared XDG roots or sibling apps' data).
#
# Usage: scripts/test-uninstall-sh.sh
# Exit codes: 0 = all green, 1 = at least one failure.

set -u

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
UNINSTALL="${SCRIPT_DIR}/uninstall.sh"

if [ ! -x "$UNINSTALL" ]; then
    echo "missing or non-executable: $UNINSTALL" >&2
    exit 2
fi

fails=0
pass() { printf 'PASS  %s\n' "$1"; }
fail() { printf 'FAIL  %s — %s\n' "$1" "$2" >&2; fails=$((fails + 1)); }

assert_absent() {
    # $1: test name, $2: path
    if [ -e "$2" ]; then
        fail "$1" "expected absent: $2"
        return 1
    fi
}
assert_present() {
    if [ ! -e "$2" ]; then
        fail "$1" "expected present: $2"
        return 1
    fi
}

run_case() {
    # $1: test name, rest: args passed to uninstall.sh
    name="$1"
    shift
    home=$(mktemp -d)
    mkdir -p "$home/.local/bin" "$home/.cargo/bin" \
             "$home/.local/share/Freenet" \
             "$home/.config/Freenet" \
             "$home/.cache/Freenet" \
             "$home/.cache/freenet" \
             "$home/.local/state/freenet" \
             "$home/.config/systemd/user"
    touch "$home/.local/bin/freenet" "$home/.local/bin/fdev" \
          "$home/.cargo/bin/freenet"
    echo "unit" > "$home/.config/systemd/user/freenet.service"
    # Plant a sibling file so we can verify shared XDG parents survive.
    echo "other app" > "$home/.local/share/other-app.txt"

    HOME="$home" FREENET_ALLOW_ROOT=1 sh "$UNINSTALL" "$@" </dev/null \
        >"$home/out.log" 2>&1
    status=$?

    CASE_HOME="$home"
    CASE_STATUS="$status"
    CASE_NAME="$name"
}

cleanup_case() { rm -rf "$CASE_HOME"; }

# ── Case 1: --purge removes everything but leaves shared XDG parents ───────

run_case "--purge removes all, preserves shared parents" --purge --yes
if [ "$CASE_STATUS" -ne 0 ]; then
    fail "$CASE_NAME" "uninstall.sh exited $CASE_STATUS"
else
    assert_absent "$CASE_NAME" "$CASE_HOME/.local/bin/freenet"
    assert_absent "$CASE_NAME" "$CASE_HOME/.local/bin/fdev"
    assert_absent "$CASE_NAME" "$CASE_HOME/.cargo/bin/freenet"
    assert_absent "$CASE_NAME" "$CASE_HOME/.local/share/Freenet"
    assert_absent "$CASE_NAME" "$CASE_HOME/.config/Freenet"
    assert_absent "$CASE_NAME" "$CASE_HOME/.cache/Freenet"
    assert_absent "$CASE_NAME" "$CASE_HOME/.local/state/freenet"
    # Shared XDG parents MUST remain (the sibling file must not be wiped).
    assert_present "$CASE_NAME" "$CASE_HOME/.local/share"
    assert_present "$CASE_NAME" "$CASE_HOME/.config"
    assert_present "$CASE_NAME" "$CASE_HOME/.cache"
    assert_present "$CASE_NAME" "$CASE_HOME/.local/state"
    assert_present "$CASE_NAME" "$CASE_HOME/.local/share/other-app.txt"
fi
[ "$CASE_STATUS" -eq 0 ] && pass "$CASE_NAME"
cleanup_case

# ── Case 2: --keep-data removes binaries but leaves data in place ─────────

run_case "--keep-data removes binaries only" --keep-data
if [ "$CASE_STATUS" -ne 0 ]; then
    fail "$CASE_NAME" "uninstall.sh exited $CASE_STATUS"
else
    assert_absent  "$CASE_NAME" "$CASE_HOME/.local/bin/freenet"
    assert_absent  "$CASE_NAME" "$CASE_HOME/.cargo/bin/freenet"
    assert_present "$CASE_NAME" "$CASE_HOME/.local/share/Freenet"
    assert_present "$CASE_NAME" "$CASE_HOME/.config/Freenet"
fi
[ "$CASE_STATUS" -eq 0 ] && pass "$CASE_NAME"
cleanup_case

# ── Case 3: nothing installed → exits cleanly with the right message ───────

home=$(mktemp -d)
HOME="$home" FREENET_ALLOW_ROOT=1 sh "$UNINSTALL" --keep-data </dev/null \
    >"$home/out.log" 2>&1
st=$?
if [ "$st" -ne 0 ]; then
    fail "noop run" "exit $st"
else
    if grep -q "does not appear to be installed" "$home/out.log"; then
        pass "noop run prints 'not installed'"
    else
        fail "noop run" "expected 'not installed' in output"
    fi
fi
rm -rf "$home"

# ── Case 4: --purge and --keep-data are mutually exclusive ─────────────────

home=$(mktemp -d)
HOME="$home" FREENET_ALLOW_ROOT=1 sh "$UNINSTALL" --purge --keep-data \
    </dev/null >"$home/out.log" 2>&1
st=$?
if [ "$st" -eq 0 ]; then
    fail "conflicting flags" "expected nonzero exit, got 0"
else
    if grep -q "mutually exclusive" "$home/out.log"; then
        pass "conflicting flags rejected"
    else
        fail "conflicting flags" "expected 'mutually exclusive' in output"
    fi
fi
rm -rf "$home"

# ── Case 5: unknown flag errors out ────────────────────────────────────────

home=$(mktemp -d)
HOME="$home" FREENET_ALLOW_ROOT=1 sh "$UNINSTALL" --definitely-not-a-flag \
    </dev/null >"$home/out.log" 2>&1
st=$?
if [ "$st" -eq 0 ]; then
    fail "unknown flag" "expected nonzero exit"
else
    pass "unknown flag rejected"
fi
rm -rf "$home"

# ── Case 6: --help prints usage and exits 0 ────────────────────────────────

out=$(HOME=/nonexistent FREENET_ALLOW_ROOT=1 sh "$UNINSTALL" --help </dev/null 2>&1)
st=$?
if [ "$st" -ne 0 ]; then
    fail "--help" "exit $st"
elif ! printf '%s' "$out" | grep -q "Usage:"; then
    fail "--help" "missing Usage: line"
else
    pass "--help prints usage"
fi

# ── Case 7: running as root (simulated) is rejected without override ───────
#
# We can't become root in the test, but we can invoke the script with a
# fake `id` that returns 0, via PATH shim.

shim=$(mktemp -d)
cat >"$shim/id" <<'EOF'
#!/bin/sh
if [ "$1" = "-u" ]; then echo 0; else /usr/bin/id "$@"; fi
EOF
chmod +x "$shim/id"

home=$(mktemp -d)
HOME="$home" PATH="$shim:$PATH" sh "$UNINSTALL" </dev/null \
    >"$home/out.log" 2>&1
st=$?
if [ "$st" -eq 0 ]; then
    fail "root guard" "expected nonzero exit when id -u == 0"
else
    if grep -q "Do not run this uninstaller with sudo" "$home/out.log"; then
        pass "root guard rejects sudo invocation"
    else
        fail "root guard" "expected sudo warning in output"
    fi
fi
rm -rf "$home" "$shim"

# ── Case 8: FREENET_ALLOW_ROOT overrides the root guard ────────────────────

shim=$(mktemp -d)
cat >"$shim/id" <<'EOF'
#!/bin/sh
if [ "$1" = "-u" ]; then echo 0; else /usr/bin/id "$@"; fi
EOF
chmod +x "$shim/id"

home=$(mktemp -d)
HOME="$home" FREENET_ALLOW_ROOT=1 PATH="$shim:$PATH" \
    sh "$UNINSTALL" --keep-data </dev/null >"$home/out.log" 2>&1
st=$?
if [ "$st" -ne 0 ]; then
    fail "root override" "exit $st"
else
    pass "FREENET_ALLOW_ROOT overrides root guard"
fi
rm -rf "$home" "$shim"

echo
if [ "$fails" -eq 0 ]; then
    echo "All uninstall.sh smoke tests passed."
    exit 0
else
    echo "${fails} test(s) failed."
    exit 1
fi

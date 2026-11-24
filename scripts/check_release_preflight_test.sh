#!/usr/bin/env bash
# Tests for scripts/check_release_preflight.sh, run against stubbed gh, git and
# curl. Each case flips one input and asserts the verdict, so deleting any one
# check (or making a lookup fail open) turns a case red.
#
# Run manually with: bash scripts/check_release_preflight_test.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PREFLIGHT="$SCRIPT_DIR/check_release_preflight.sh"
command -v jq > /dev/null || { echo "FAIL - this test needs jq" >&2; exit 2; }
TEST_TMP=$(mktemp -d)
trap 'rm -rf "$TEST_TMP"' EXIT

FAILURES=0
check() {
    local desc="$1" actual="$2" expected="$3"
    if [[ "$actual" == "$expected" ]]; then
        echo "ok   - $desc"
    else
        echo "FAIL - $desc (got '$actual', expected '$expected')" >&2
        FAILURES=$((FAILURES + 1))
    fi
}

STUBS="$TEST_TMP/bin"
mkdir -p "$STUBS"
# Each stub answers from a file the case writes, or fails when the file says FAIL.
cat > "$STUBS/gh" <<'EOF'
#!/usr/bin/env bash
case "$1 $2" in
    "run list") f="$STUB_DIR/runs" ;;
    "pr list") f="$STUB_DIR/prs" ;;
    *) echo "unexpected gh $*" >&2; exit 99 ;;
esac
[ "$(cat "$f")" = FAIL ] && exit 1
cat "$f"
EOF
cat > "$STUBS/git" <<'EOF'
#!/usr/bin/env bash
[ "$1" = ls-remote ] || { echo "unexpected git $*" >&2; exit 99; }
[ "$(cat "$STUB_DIR/tags")" = FAIL ] && exit 128
cat "$STUB_DIR/tags"
EOF
cat > "$STUBS/curl" <<'EOF'
#!/usr/bin/env bash
[ "$(cat "$STUB_DIR/crates")" = FAIL ] && exit 6
printf '%s' "$(cat "$STUB_DIR/crates")"
EOF
chmod +x "$STUBS"/*

# run_case <desc> <expected rc> [key=value ...]: start from a passing baseline
# and override the named inputs.
run_case() {
    local desc="$1" want="$2"
    shift 2
    local dir="$TEST_TMP/case"
    rm -rf "$dir"
    mkdir -p "$dir"
    echo '[{"databaseId":100,"status":"in_progress"},{"databaseId":7,"status":"completed"}]' > "$dir/runs"
    echo '[{"number":1,"headRefName":"fix/x","title":"fix: x"}]' > "$dir/prs"
    : > "$dir/tags"
    echo 404 > "$dir/crates"
    printf '[package]\nname = "freenet"\nversion = "0.2.163"\n\n[dependencies]\nversion = "9.9.9"\n' > "$dir/Cargo.toml"
    local version=0.2.164
    for kv in "$@"; do
        case "$kv" in
            version=*) version="${kv#version=}" ;;
            cargo=*) printf '[package]\nversion = "%s"\n' "${kv#cargo=}" > "$dir/Cargo.toml" ;;
            *) printf '%s\n' "${kv#*=}" > "$dir/${kv%%=*}" ;;
        esac
    done
    local rc=0
    STUB_DIR="$dir" PATH="$STUBS:$PATH" RELEASE_VERSION="$version" \
        GITHUB_REPOSITORY=freenet/freenet-core GITHUB_RUN_ID=100 \
        CARGO_TOML="$dir/Cargo.toml" bash "$PREFLIGHT" > "$dir/out" 2>&1 || rc=$?
    check "$desc" "$rc" "$want"
}

run_case "baseline passes (own run in progress is ignored; [dependencies] version is not read)" 0
run_case "another release run in progress refuses" 1 \
    runs='[{"databaseId":100,"status":"in_progress"},{"databaseId":101,"status":"queued"}]'
run_case "a failed run listing refuses" 1 runs=FAIL
run_case "an open release/v* PR refuses" 1 \
    prs='[{"number":5,"headRefName":"release/v0.2.164","title":"x"}]'
run_case "an open build: release PR refuses" 1 \
    prs='[{"number":5,"headRefName":"other","title":"build: release 0.2.164"}]'
run_case "a failed PR listing refuses" 1 prs=FAIL
run_case "an existing tag refuses" 1 tags='abc123	refs/tags/v0.2.164'
run_case "a failed tag lookup refuses" 1 tags=FAIL
run_case "a version already on crates.io refuses" 1 crates=200
run_case "an unknown crates.io answer (403) refuses" 1 crates=403
run_case "a curl failure refuses" 1 crates=FAIL
run_case "a version equal to Cargo.toml refuses" 1 cargo=0.2.164
run_case "a downgrade refuses" 1 cargo=0.2.170
run_case "a numeric-not-lexical comparison passes (0.2.99 -> 0.2.100)" 0 cargo=0.2.99 version=0.2.100
run_case "an unreadable Cargo.toml version refuses" 1 cargo=garbage
run_case "a malformed version refuses" 1 version=0.2

if [ "$FAILURES" -ne 0 ]; then
    echo "$FAILURES failure(s)" >&2
    exit 1
fi
echo "All check_release_preflight tests passed."

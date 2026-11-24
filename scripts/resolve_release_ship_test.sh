#!/usr/bin/env bash
# Tests for scripts/resolve_release_ship.sh (release.yml's `validate` step that
# picks and checks the ship commit) against a stubbed `gh`. Each case flips one
# input from a passing baseline, so deleting any check, or making a lookup fail
# open, turns a case red. Also pins scripts/apply_version_bump.sh against the
# real manifests.
#
# Run manually with: bash scripts/resolve_release_ship_test.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
RESOLVE="$SCRIPT_DIR/resolve_release_ship.sh"
BUMP="$SCRIPT_DIR/apply_version_bump.sh"
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

SHIP=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
LAUNCH=cccccccccccccccccccccccccccccccccccccccc
TAGC=dddddddddddddddddddddddddddddddddddddddd
PARENT=eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee

STUBS="$TEST_TMP/bin"
mkdir -p "$STUBS"
# Answers from $STUB_DIR: compare/<base>...<head> from file cmp_<base>_<head>
# (holding just a status word, or FAIL), the rest from named files. Applies the
# script's own --jq with real jq.
cat > "$STUBS/gh" <<'EOF'
#!/usr/bin/env bash
echo "gh $*" >> "$STUB_DIR/calls"
jqarg=""; path=""
args=("$@")
for ((i = 1; i < ${#args[@]}; i++)); do
    case "${args[i]}" in
        --jq) jqarg="${args[i+1]}"; i=$((i + 1)) ;;
        -H) i=$((i + 1)) ;;
        repos/*) path="${args[i]}" ;;
    esac
done
answer() { [ -f "$1" ] || exit 1; [ "$(cat "$1")" = FAIL ] && exit 1; return 0; }
case "$path" in
    repos/freenet/freenet-core/compare/*)
        spec="${path#repos/freenet/freenet-core/compare/}"
        f="$STUB_DIR/cmp_${spec%%...*}_${spec#*...}"
        answer "$f"
        json="{\"status\":\"$(cat "$f")\"}" ;;
    "repos/freenet/freenet-core/releases?per_page=100")
        [[ " $* " == *" --paginate "* ]] || { echo "releases must be paginated" >&2; exit 98; }
        answer "$STUB_DIR/releases"; json=$(cat "$STUB_DIR/releases") ;;
    repos/freenet/freenet-core/commits/*)
        f="$STUB_DIR/commit_${path##*/}"; answer "$f"; json=$(cat "$f") ;;
    repos/freenet/freenet-core/contents/*)
        rel="${path#repos/freenet/freenet-core/contents/}"
        ref="${rel##*\?ref=}"; file="${rel%%\?*}"
        f="$STUB_DIR/tree_${ref}/${file}"; answer "$f"; cat "$f"; exit 0 ;;
    *) echo "unexpected gh $*" >&2; exit 99 ;;
esac
jq -rc "$jqarg" <<< "$json"
EOF
cat > "$STUBS/git" <<'EOF'
#!/usr/bin/env bash
[ "$1 $2" = "ls-remote --tags" ] || { echo "unexpected git $*" >&2; exit 99; }
[ "$(cat "$STUB_DIR/tags")" = FAIL ] && exit 128
cat "$STUB_DIR/tags"
EOF
cat > "$STUBS/curl" <<'EOF'
#!/usr/bin/env bash
[ "$(cat "$STUB_DIR/crates_fdev")" = FAIL ] && exit 6
[[ "$*" == *"https://crates.io/api/v1/crates/fdev"* && "$*" == *"-A "* ]] || { echo "unexpected curl $*" >&2; exit 99; }
cat "$STUB_DIR/crates_fdev"
EOF
chmod +x "$STUBS/gh" "$STUBS/git" "$STUBS/curl"
rel() { printf '{"tag_name":"%s","draft":%s,"prerelease":%s}' "$1" "$2" "$3"; }
tagline() { printf '%s\trefs/tags/%s\n' "$TAGC" "$1"; }
CI_PUSH=$'on:\n  push:\n    branches: [main, \'release/v*\']\n  pull_request:\n'
CI_OLD=$'on:\n  push:\n    branches: [main]\n  pull_request:\n'

manifest() { printf '[package]\nname = "%s"\nversion = "%s"\n\n[dependencies]\nversion = "9.9.9"\n' "$1" "$2"; }

run_case() {
    # run_case <desc> <expected rc> [live=..] [input=..] [file=content ...]
    local desc="$1" want="$2"
    shift 2
    local dir="$TEST_TMP/case" live=true input="$SHIP" version=0.2.164
    rm -rf "$dir" && mkdir -p "$dir/stub" "$dir/tmp"
    local S="$dir/stub"
    echo identical > "$S/cmp_${SHIP}_main"
    # v0.2.163 is the highest PUBLISHED release; GitHub's movable
    # /releases/latest marker is never read.
    echo "[$(rel v0.2.163 false false),$(rel v0.2.162 false false),$(rel v0.2.170 false true)]" > "$S/releases"
    { tagline v0.2.162; tagline v0.2.163; } > "$S/tags"
    echo '{"crate":{"max_version":"0.3.305"}}' > "$S/crates_fdev"
    echo "{\"sha\":\"$TAGC\",\"parents\":[{\"sha\":\"$PARENT\"}]}" > "$S/commit_v0.2.163"
    echo "{\"sha\":\"$TAGC\",\"parents\":[{\"sha\":\"$PARENT\"}]}" > "$S/commit_$TAGC"
    echo diverged > "$S/cmp_${TAGC}_main"     # the tag sits on a release/v* bump commit
    echo ahead > "$S/cmp_${PARENT}_${SHIP}"   # ship contains the previous ship commit
    mkdir -p "$S/tree_$SHIP/crates/core" "$S/tree_$SHIP/crates/fdev" "$S/tree_v0.2.163/crates/fdev"
    manifest freenet 0.2.163 > "$S/tree_$SHIP/crates/core/Cargo.toml"
    manifest fdev 0.3.300 > "$S/tree_$SHIP/crates/fdev/Cargo.toml"
    manifest fdev 0.3.305 > "$S/tree_v0.2.163/crates/fdev/Cargo.toml"
    mkdir -p "$S/tree_$SHIP/.github/workflows"
    echo "name: Release sync" > "$S/tree_$SHIP/.github/workflows/release-sync.yml"
    printf '%s' "$CI_PUSH" > "$S/tree_$SHIP/.github/workflows/ci.yml"
    local kv
    for kv in "$@"; do
        case "$kv" in
            live=*) live="${kv#live=}" ;;
            input=*) input="${kv#input=}" ;;
            version=*) version="${kv#version=}" ;;
            *) mkdir -p "$(dirname "$S/${kv%%=*}")"; printf '%s\n' "${kv#*=}" > "$S/${kv%%=*}" ;;
        esac
    done
    : > "$dir/out"
    local rc=0
    STUB_DIR="$S" PATH="$STUBS:$PATH" SHIP_SHA_INPUT="$input" RELEASE_LIVE="$live" RELEASE_VERSION="$version" \
        LAUNCH_SHA="$LAUNCH" GITHUB_REPOSITORY=freenet/freenet-core GITHUB_OUTPUT="$dir/out" \
        RUNNER_TEMP="$dir/tmp" bash "$RESOLVE" > "$dir/log" 2>&1 || rc=$?
    check "$desc" "$rc" "$want"
}
out() { grep -m1 "^$1=" "$TEST_TMP/case/out" | cut -d= -f2-; }

run_case "baseline: live, ship on main, contains the previous ship commit" 0
check "baseline: exports ship_sha" "$(out ship_sha)" "$SHIP"
check "baseline: previous ship commit is the tag commit's FIRST PARENT when the tag is off main" "$(out prev_ship_sha)" "$PARENT"
check "baseline: fdev = max(ship 0.3.300, tag 0.3.305) + 1" "$(out fdev_version)" "0.3.306"
check "baseline: hands the ship commit's core manifest to the pre-flight" \
    "$(grep -m1 '^version' "$(out ship_cargo_toml)")" 'version = "0.2.163"'
run_case "fdev from the ship tree when it is the higher one" 0 \
    "tree_$SHIP/crates/fdev/Cargo.toml=$(manifest fdev 0.3.310)"
check "fdev = 0.3.310 + 1" "$(out fdev_version)" "0.3.311"
run_case "fdev comparison is numeric (0.3.99 vs 0.3.100)" 0 \
    "tree_$SHIP/crates/fdev/Cargo.toml=$(manifest fdev 0.3.99)" \
    "tree_v0.2.163/crates/fdev/Cargo.toml=$(manifest fdev 0.3.100)" \
    crates_fdev='{"crate":{"max_version":"0.3.98"}}'
check "fdev = 0.3.100 + 1" "$(out fdev_version)" "0.3.101"
run_case "a tag ON main (older PR flow) is itself the previous ship commit" 0 \
    "cmp_${TAGC}_main=ahead" "cmp_${TAGC}_${SHIP}=ahead"
check "previous ship commit = the tag commit" "$(out prev_ship_sha)" "$TAGC"

run_case "live without ship_sha refuses" 1 input=
check "no ship_sha: says it is required" "$(grep -c 'ship_sha is required' "$TEST_TMP/case/log")" "1"
run_case "dry run without ship_sha defaults to the dispatch commit" 0 live=false input= \
    "cmp_${LAUNCH}_main=identical" "cmp_${PARENT}_${LAUNCH}=ahead" \
    "tree_$LAUNCH/crates/core/Cargo.toml=$(manifest freenet 0.2.163)" \
    "tree_$LAUNCH/crates/fdev/Cargo.toml=$(manifest fdev 0.3.300)" \
    "tree_$LAUNCH/.github/workflows/release-sync.yml=x" "tree_$LAUNCH/.github/workflows/ci.yml=$CI_PUSH"
check "dry run default: ship_sha is the dispatch commit" "$(out ship_sha)" "$LAUNCH"
run_case "a short SHA refuses" 1 input=aaaaaaa
run_case "a branch name refuses" 1 input=main
run_case "an upper-case SHA is accepted and normalised" 0 input="${SHIP^^}"
check "normalised to lower case" "$(out ship_sha)" "$SHIP"

run_case "ship_sha not on main refuses (live)" 1 "cmp_${SHIP}_main=diverged"
run_case "ship_sha behind... i.e. main is an ancestor of it, refuses (live)" 1 "cmp_${SHIP}_main=behind"
run_case "ship_sha ahead of main (main contains it) passes" 0 "cmp_${SHIP}_main=ahead"
run_case "a failed on-main compare refuses (fail closed)" 1 "cmp_${SHIP}_main=FAIL"
run_case "a dry run only warns when ship_sha is not on main" 0 live=false "cmp_${SHIP}_main=diverged"
check "the dry-run warning says it is not enforced" "$(grep -c 'not enforced' "$TEST_TMP/case/log")" "1"
run_case "ship_sha that drops the previous ship commit refuses" 1 "cmp_${PARENT}_${SHIP}=diverged"
run_case "ship_sha BEHIND the previous ship commit refuses" 1 "cmp_${PARENT}_${SHIP}=behind"
run_case "ship_sha identical to the previous ship commit passes" 0 "cmp_${PARENT}_${SHIP}=identical"
run_case "a failed containment compare refuses" 1 "cmp_${PARENT}_${SHIP}=FAIL"
run_case "a failed release listing refuses" 1 releases=FAIL
run_case "no published vX.Y.Z release refuses" 1 "releases=[$(rel nightly false false),$(rel v0.2.164 true false)]"
# The previous release is the highest PUBLISHED non-prerelease by version, not
# GitHub's latest marker (which can be moved by hand) and not a draft.
run_case "the previous release is the highest-semver published one" 0 \
    "releases=[$(rel v0.2.99 false false),$(rel v0.2.163 false false),$(rel v0.2.164 true false)]"
check "previous release = v0.2.163 (not the draft v0.2.164, not v0.2.99)" "$(out prev_tag)" "v0.2.163"

# --- fdev never collides with an unfinished or published release (#6362) ------
run_case "fdev accounts for a DRAFT or unfinished tag above the latest release" 0 \
    "tags=$(tagline v0.2.163; tagline v0.2.164)" \
    "tree_v0.2.164/crates/fdev/Cargo.toml=$(manifest fdev 0.3.306)"
check "fdev = unfinished v0.2.164's 0.3.306 + 1" "$(out fdev_version)" "0.3.307"
run_case "fdev accounts for crates.io being ahead of every tree" 0 \
    crates_fdev='{"crate":{"max_version":"0.3.400"}}'
check "fdev = crates.io's 0.3.400 + 1" "$(out fdev_version)" "0.3.401"
run_case "an unreadable crates.io fdev answer refuses" 1 crates_fdev=FAIL
run_case "a crates.io answer with no version refuses" 1 crates_fdev='{"errors":[{"detail":"x"}]}'
run_case "a failed tag listing refuses" 1 tags=FAIL
run_case "an EMPTY tag listing with a published release refuses with an error" 1 tags=
check "empty tags: names the cause instead of dying silently" "$(grep -c '^::error title=No release tags found' "$TEST_TMP/case/log")" "1"

# --- ship_sha must carry the exact-SHA release flow (#6362) ---------------------
run_case "a ship_sha without release-sync.yml refuses (live)" 1 "tree_$SHIP/.github/workflows/release-sync.yml=FAIL"
check "missing release-sync.yml: names the flow" "$(grep -c 'predates the exact-SHA release flow' "$TEST_TMP/case/log")" "1"
run_case "a ship_sha whose ci.yml does not run on release/v* refuses (live)" 1 "tree_$SHIP/.github/workflows/ci.yml=$CI_OLD"
run_case "an unreadable ship ci.yml refuses (live)" 1 "tree_$SHIP/.github/workflows/ci.yml=FAIL"
run_case "release/v* only in pull_request, not push, refuses" 1 \
    "tree_$SHIP/.github/workflows/ci.yml=$(printf 'on:\n  push:\n    branches: [main]\n  pull_request:\n    branches: [main, \x27release/v*\x27]\n')"
run_case "a dry run only warns about an old-flow ship_sha" 0 live=false "tree_$SHIP/.github/workflows/ci.yml=$CI_OLD"
# Parsed as YAML with GitHub's filter rules, not matched as one text form.
run_case "a block-list, double-quoted release/v* push filter is accepted" 0 \
    "tree_$SHIP/.github/workflows/ci.yml=$(printf 'on:\n  push:\n    branches:\n      - main\n      - "release/v*"\n')"
run_case "a broader release/** push filter is accepted" 0 \
    "tree_$SHIP/.github/workflows/ci.yml=$(printf 'on:\n  push:\n    branches: [main, "release/**"]\n')"
run_case "an unfiltered push trigger is accepted" 0 "tree_$SHIP/.github/workflows/ci.yml=$(printf 'on: [push, pull_request]\n')"
run_case "a negated !release/** after release/v* refuses (last match wins)" 1 \
    "tree_$SHIP/.github/workflows/ci.yml=$(printf 'on:\n  push:\n    branches: ["release/v*", "!release/**"]\n')"
run_case "branches-ignore covering release/v* refuses" 1 \
    "tree_$SHIP/.github/workflows/ci.yml=$(printf 'on:\n  push:\n    branches-ignore: ["release/*"]\n')"
run_case "a commented-out release/v* refuses" 1 \
    "tree_$SHIP/.github/workflows/ci.yml=$(printf "on:\n  push:\n    branches: [main] # 'release/v*'\n")"
run_case "this repo's own ci.yml is accepted" 0 "tree_$SHIP/.github/workflows/ci.yml=$(cat "$REPO_ROOT/.github/workflows/ci.yml")"
# The filters are evaluated against THIS release's branch, release/v0.2.164.
run_case "a filter excluding another version's branches is accepted" 0 \
    "tree_$SHIP/.github/workflows/ci.yml=$(printf 'on:\n  push:\n    branches: ["release/v*", "!release/v0.0.*"]\n')"
run_case "a filter excluding this version's branch refuses" 1 \
    "tree_$SHIP/.github/workflows/ci.yml=$(printf 'on:\n  push:\n    branches: ["release/v*", "!release/v0.2.*"]\n')"
check "excluded branch: names the release branch" "$(grep -c 'pushes to release/v0.2.164' "$TEST_TMP/case/log")" "1"
run_case "without a release version refuses" 1 version=
check "no release version: says so" "$(grep -c 'RELEASE_VERSION must be X.Y.Z' "$TEST_TMP/case/log")" "1"
run_case "a ci.yml with no push trigger refuses" 1 \
    "tree_$SHIP/.github/workflows/ci.yml=$(printf 'on:\n  pull_request:\n    branches: [main]\n')"
run_case "a tags-only push filter refuses" 1 \
    "tree_$SHIP/.github/workflows/ci.yml=$(printf "on:\n  push:\n    tags: ['v*']\n")"
run_case "a bare 'on: push' is accepted" 0 "tree_$SHIP/.github/workflows/ci.yml=$(printf 'on: push\n')"
run_case "a lone '*' does not match release/v0.2.164 (it stops at /)" 1 \
    "tree_$SHIP/.github/workflows/ci.yml=$(printf "on:\n  push:\n    branches: [main, '*']\n")"
run_case "a malformed branches value refuses as unparseable" 1 \
    "tree_$SHIP/.github/workflows/ci.yml=$(printf 'on:\n  push:\n    branches: 5\n')"
check "malformed branches: says it could not parse" "$(grep -c 'Could not parse ship_sha' "$TEST_TMP/case/log")" "1"
run_case "an unparseable ci.yml refuses (live)" 1 "tree_$SHIP/.github/workflows/ci.yml=$(printf 'on: [push\n  : :\n')"
check "unparseable ci.yml: says it could not parse" "$(grep -c 'Could not parse ship_sha' "$TEST_TMP/case/log")" "1"
run_case "an unreadable tag commit refuses" 1 commit_v0.2.163=FAIL
run_case "an unreadable first parent refuses" 1 "commit_$TAGC=FAIL"
run_case "a failed tag-on-main compare refuses" 1 "cmp_${TAGC}_main=FAIL"
run_case "an unreadable ship manifest refuses (even in a dry run)" 1 live=false \
    "tree_$SHIP/crates/fdev/Cargo.toml=FAIL"
run_case "an unreadable tag fdev manifest refuses" 1 "tree_v0.2.163/crates/fdev/Cargo.toml=FAIL"
run_case "a refusal writes no outputs" 1 "cmp_${PARENT}_${SHIP}=diverged"
check "a refusal writes no outputs: empty" "$(wc -c < "$TEST_TMP/case/out" | tr -d ' ')" "0"

# --- apply_version_bump.sh on the REAL manifests -------------------------------
BT="$TEST_TMP/bump"
mkdir -p "$BT/crates/core" "$BT/crates/fdev"
cp "$REPO_ROOT/crates/core/Cargo.toml" "$BT/crates/core/"
cp "$REPO_ROOT/crates/fdev/Cargo.toml" "$BT/crates/fdev/"
rc=0; ( cd "$BT" && bash "$BUMP" 9.8.7 6.5.4 ) > "$TEST_TMP/bump.log" 2>&1 || rc=$?
check "apply_version_bump: succeeds on the real manifests" "$rc" "0"
pv() { awk '/^\[package\]/ {p = 1; next} /^\[/ {p = 0} p && /^version *=/ {gsub(/.*= *"|".*/, ""); print; exit}' "$1"; }
check "apply_version_bump: core version" "$(pv "$BT/crates/core/Cargo.toml")" "9.8.7"
check "apply_version_bump: fdev version" "$(pv "$BT/crates/fdev/Cargo.toml")" "6.5.4"
check "apply_version_bump: fdev's freenet dependency follows the release (#4119)" \
    "$(grep -cE '^freenet = \{ path = "\.\./core", version = "9\.8\.7"' "$BT/crates/fdev/Cargo.toml")" "1"
check "apply_version_bump: both fdev binstall URLs point at the release (#3995)" \
    "$(grep -c 'releases/download/v9.8.7/fdev-' "$BT/crates/fdev/Cargo.toml")" "2"
check "apply_version_bump: only the expected lines change in core" \
    "$(diff "$REPO_ROOT/crates/core/Cargo.toml" "$BT/crates/core/Cargo.toml" | grep -c '^>')" "1"
check "apply_version_bump: only the expected lines change in fdev" \
    "$(diff "$REPO_ROOT/crates/fdev/Cargo.toml" "$BT/crates/fdev/Cargo.toml" | grep -c '^>')" "4"
# The self-check: a manifest whose dependency line no longer matches the
# rewrite must fail, not silently keep the old version (#4119).
sed -i 's/^freenet = { path = "..\/core", version = /freenet = { version = /' "$BT/crates/fdev/Cargo.toml"
rc=0; ( cd "$BT" && bash "$BUMP" 9.8.8 6.5.5 ) > "$TEST_TMP/bump.log" 2>&1 || rc=$?
check "apply_version_bump: a rewrite that matched nothing fails" "$rc" "1"
rc=0; ( cd "$BT" && bash "$BUMP" 9.8 6.5.5 ) > /dev/null 2>&1 || rc=$?
check "apply_version_bump: a malformed version refuses" "$rc" "1"

# --- wiring --------------------------------------------------------------------
RELEASE_YML="$REPO_ROOT/.github/workflows/release.yml"
check "release.yml's validate runs the resolver" \
    "$(grep -cE '^[[:space:]]*run: bash scripts/resolve_release_ship\.sh[[:space:]]*$' "$RELEASE_YML")" "1"
# shellcheck disable=SC2016  # matched literally
check "the pre-flight reads the ship commit's manifest" \
    "$(grep -cE '^[[:space:]]*CARGO_TOML: \$\{\{ steps\.ship\.outputs\.ship_cargo_toml \}\}[[:space:]]*$' "$RELEASE_YML")" "1"
check "both release workflows apply the bump through the one script" \
    "$(grep -lE 'apply_version_bump\.sh' "$RELEASE_YML" "$SCRIPT_DIR/release_sync.sh" | wc -l | tr -d ' ')" "2"
check "release.yml keeps no inline copy of the version seds" \
    "$(grep -cE "sed .*(\^version = |freenet = \\\\\\\\\{|releases/download/v\[0-9\])" "$RELEASE_YML" || true)" "0"
check "CI runs this test" \
    "$(grep -cE '^[[:space:]]*run: bash scripts/resolve_release_ship_test\.sh[[:space:]]*$' "$REPO_ROOT/.github/workflows/ci.yml")" "1"

if [ "$FAILURES" -ne 0 ]; then
    echo "$FAILURES failure(s)" >&2
    exit 1
fi
echo "All resolve_release_ship / apply_version_bump tests passed."

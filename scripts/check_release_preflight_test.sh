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
    "run list") f="$STUB_DIR/runs"; min=20 ;;
    "pr list") f="$STUB_DIR/prs"; min=1000 ;;
    "api --paginate")
        [ "$3" = "repos/freenet/freenet-core/releases?per_page=100" ] || { echo "unexpected gh $*" >&2; exit 99; }
        f="$STUB_DIR/releases"; min=none ;;
    *) echo "unexpected gh $*" >&2; exit 99 ;;
esac
# A listing shorter than these could miss the run or PR that should refuse the
# cut, so a smaller --limit is recorded (run_case fails on it) and refused.
limit="" prev=""
for a in "$@"; do [ "$prev" = --limit ] && limit="$a"; prev="$a"; done
if [ "$min" != none ] && ! [ "$limit" -ge "$min" ] 2>/dev/null; then
    echo "gh $1 $2 --limit '$limit' is below $min" >> "$STUB_DIR/limit_violations"
    exit 97
fi
[ "$(cat "$f")" = FAIL ] && exit 1
if [ "$1" = api ]; then
    # Apply the script's own --jq filter, so a wrong field name fails here.
    jq -r "$5" "$f"
else
    cat "$f"
fi
EOF
# ls-remote answers like git: --tags from $STUB_DIR/tags, --heads from
# $STUB_DIR/branches, filtered by the pattern (an exact ref, or a `prefix*`).
cat > "$STUBS/git" <<'EOF'
#!/usr/bin/env bash
[ "$1" = ls-remote ] || { echo "unexpected git $*" >&2; exit 99; }
case "$2" in
    --tags) f="$STUB_DIR/tags"; ns=refs/tags/ ;;
    --heads) f="$STUB_DIR/branches"; ns=refs/heads/ ;;
    *) echo "unexpected git $*" >&2; exit 99 ;;
esac
[ "$(cat "$f")" = FAIL ] && exit 128
pat="${4:-}"
[[ "$pat" == refs/* ]] || pat="$ns$pat"
while IFS=$'\t' read -r sha ref; do
    [ -n "$ref" ] || continue
    if [[ "$pat" == *'*' && "$ref" == "${pat%\*}"* ]] || [ "$ref" = "$pat" ]; then
        printf '%s\t%s\n' "$sha" "$ref"
    fi
done < "$f"
EOF
cat > "$STUBS/curl" <<'EOF'
#!/usr/bin/env bash
[ "$(cat "$STUB_DIR/crates")" = FAIL ] && exit 6
printf '%s' "$(cat "$STUB_DIR/crates")"
EOF
chmod +x "$STUBS"/*

# run_case <desc> <expected rc> <expected error> [key=value ...]: start from a
# passing baseline and override the named inputs. <expected error> must appear
# in a `::error` line (for a titled refusal pass `title=<title>`), so a case
# cannot pass on a refusal other than the one it is about. Empty means no
# `::error` line at all.
run_case() {
    local desc="$1" want="$2" want_err="$3"
    shift 3
    local dir="$TEST_TMP/case"
    rm -rf "$dir"
    mkdir -p "$dir"
    echo '[{"databaseId":100,"status":"in_progress"},{"databaseId":7,"status":"completed"}]' > "$dir/runs"
    echo '[{"number":1,"headRefName":"fix/x","title":"fix: x"}]' > "$dir/prs"
    printf 'aaa\trefs/tags/v0.2.162\naaa\trefs/tags/v0.2.163\n' > "$dir/tags"
    printf 'aaa\trefs/heads/release/v0.2.163\n' > "$dir/branches"
    echo 404 > "$dir/crates"
    # v0.2.163 is the highest PUBLISHED non-prerelease. v0.2.170 is a published
    # prerelease, which neither counts as the latest nor as in flight.
    echo '[{"tag_name":"v0.2.163","draft":false,"prerelease":false},{"tag_name":"v0.2.162","draft":false,"prerelease":false},{"tag_name":"v0.2.170","draft":false,"prerelease":true}]' > "$dir/releases"
    printf '[package]\nname = "freenet"\nversion = "0.2.163"\n\n[dependencies]\nversion = "9.9.9"\n' > "$dir/Cargo.toml"
    local version=0.2.164
    for kv in "$@"; do
        case "$kv" in
            version=*) version="${kv#version=}" ;;
            cargo=*) printf '[package]\nversion = "%s"\n' "${kv#cargo=}" > "$dir/Cargo.toml" ;;
            cargoraw=*) printf '%b' "${kv#cargoraw=}" > "$dir/Cargo.toml" ;;
            # A history whose only release (and tag) is the one given.
            latest=*) printf '[{"tag_name":"%s","draft":false,"prerelease":false}]\n' "${kv#latest=}" > "$dir/releases"
                      printf 'aaa\trefs/tags/%s\n' "${kv#latest=}" > "$dir/tags"
                      : > "$dir/branches" ;;
            addtag=*) printf 'bbb\trefs/tags/%s\n' "${kv#addtag=}" >> "$dir/tags" ;;
            addbranch=*) printf 'bbb\trefs/heads/release/%s\n' "${kv#addbranch=}" >> "$dir/branches" ;;
            *) printf '%s\n' "${kv#*=}" > "$dir/${kv%%=*}" ;;
        esac
    done
    local rc=0
    STUB_DIR="$dir" PATH="$STUBS:$PATH" RELEASE_VERSION="$version" \
        GITHUB_REPOSITORY=freenet/freenet-core GITHUB_RUN_ID=100 \
        CARGO_TOML="$dir/Cargo.toml" bash "$PREFLIGHT" > "$dir/out" 2>&1 || rc=$?
    local errs got_err=match
    errs=$(grep '^::error' "$dir/out" || true)
    if [ -z "$want_err" ]; then
        [ -z "$errs" ] || got_err="unexpected: $errs"
    elif ! grep -qF -- "$want_err" <<<"$errs"; then
        got_err="missing '$want_err' in: ${errs:-<no ::error line>}"
    fi
    if [ -s "$dir/limit_violations" ]; then
        got_err="$(cat "$dir/limit_violations")"
    fi
    check "$desc" "rc=$rc $got_err" "rc=$want match"
}

run_case "baseline passes (own run in progress is ignored; [dependencies] version is not read)" 0 ""
run_case "another release run in progress refuses" 1 "title=Another release run is active" \
    runs='[{"databaseId":100,"status":"in_progress"},{"databaseId":101,"status":"queued"}]'
run_case "a failed run listing refuses" 1 "title=Could not list release runs" runs=FAIL
run_case "an open release/v* PR refuses" 1 "title=A version-bump PR is already open" \
    prs='[{"number":5,"headRefName":"release/v0.2.164","title":"x"}]'
run_case "an open build: release PR refuses" 1 "title=A version-bump PR is already open" \
    prs='[{"number":5,"headRefName":"other","title":"build: release 0.2.164"}]'
run_case "a failed PR listing refuses" 1 "title=Could not list open PRs" prs=FAIL
run_case "an unrelated 'build: release-…' title does not block" 0 "" \
    prs='[{"number":6,"headRefName":"ci/rp","title":"build: release-please config"}]'
run_case "a listing that hits the PR limit refuses (cannot rule out a bump PR)" 1 "title=Too many open PRs to check" \
    prs="$(jq -nc '[range(1000) | {number: ., headRefName: "x", title: "fix: x"}]')"
run_case "an existing tag refuses" 1 "title=Tag v0.2.164 already exists" tags='abc123	refs/tags/v0.2.164'
run_case "a failed tag lookup refuses" 1 "title=Could not list tags" tags=FAIL
run_case "a version already on crates.io refuses" 1 "title=freenet 0.2.164 is already on crates.io" crates=200
run_case "an unknown crates.io answer (403) refuses" 1 "title=Could not check crates.io" crates=403
run_case "a curl failure refuses" 1 "title=Could not check crates.io" crates=FAIL
run_case "a version equal to Cargo.toml refuses" 1 "title=The ship commit is already at 0.2.164" cargo=0.2.164
run_case "a downgrade refuses" 1 "title=0.2.164 is a downgrade" cargo=0.2.170
run_case "a numeric-not-lexical comparison passes (0.2.99 -> 0.2.100)" 0 "" cargo=0.2.99 version=0.2.100 latest=v0.2.98
run_case "an unreadable Cargo.toml version refuses" 1 "title=Could not read the current version" cargo=garbage
run_case "a malformed version refuses" 1 "RELEASE_VERSION must be X.Y.Z" version=0.2
# Check 5: main's Cargo version lags a release until its merge-back lands, so
# the ship commit alone cannot say whether a version is already out.
run_case "a version equal to the latest published release refuses (ship commit lagging)" 1 \
    "title=0.2.164 is not above the latest release" cargo=0.2.163 latest=v0.2.164
run_case "a version below the latest published release refuses" 1 \
    "title=0.2.161 is not above the latest release" cargo=0.2.160 version=0.2.161 latest=v0.2.163
run_case "a version above both the ship commit and the latest release passes" 0 "" \
    cargo=0.2.162 latest=v0.2.163
run_case "numeric comparison against the latest release (0.2.99 -> 0.2.100)" 0 "" \
    cargo=0.2.99 version=0.2.100 latest=v0.2.99
run_case "a failed release listing refuses" 1 "title=Could not read the latest release" releases=FAIL
run_case "no published vX.Y.Z release refuses" 1 "title=Could not read the latest release" \
    releases='[{"tag_name":"nightly","draft":false,"prerelease":false}]'
# The latest release is the highest-semver PUBLISHED non-prerelease, never
# GitHub's movable /releases/latest marker: listing order does not matter, and
# a draft above it does not count as published.
run_case "the latest release is the highest version, in any listing order" 1 \
    "title=0.2.164 is not above the latest release" \
    releases='[{"tag_name":"v0.2.99","draft":false,"prerelease":false},{"tag_name":"v0.2.164","draft":false,"prerelease":false}]'

# Check 6: another release in flight above the latest published release.
run_case "an unpublished tag above the latest release refuses" 1 "title=Another release is unfinished" \
    version=0.2.165 addtag=v0.2.164
run_case "a release/v* branch above the latest release with no tag refuses" 1 "title=Another release is unfinished" \
    version=0.2.165 addbranch=v0.2.164
# Drafts alone are deliberately ignored: validate's read-only token cannot see
# them, so a draft check here would be dead code that reads as a guard.
run_case "a draft alone above the latest release does not refuse" 0 "" version=0.2.165 \
    releases='[{"tag_name":"v0.2.163","draft":false,"prerelease":false},{"tag_name":"v0.2.164","draft":true,"prerelease":false}]'
run_case "the SAME version's branch is a re-cut, not a conflict" 0 "" addbranch=v0.2.164
run_case "branches and tags at or below the latest release are history" 0 "" \
    addbranch=v0.2.160 addtag=v0.2.100
run_case "a published prerelease's tag above the latest is not in flight" 0 "" version=0.2.171 addtag=v0.2.170
run_case "a published release branch (its tag exists) is not in flight" 0 "" version=0.2.165 \
    addbranch=v0.2.164 addtag=v0.2.164 \
    releases='[{"tag_name":"v0.2.164","draft":false,"prerelease":false},{"tag_name":"v0.2.163","draft":false,"prerelease":false}]'
run_case "a failed branch listing refuses" 1 "title=Could not list release branches" branches=FAIL
run_case "a [package] with no version refuses, even when [dependencies] has one" 1 \
    "title=Could not read the current version" \
    cargoraw='[package]\nname = "freenet"\n\n[dependencies]\nversion = "0.2.1"\n'

if [ "$FAILURES" -ne 0 ]; then
    echo "$FAILURES failure(s)" >&2
    exit 1
fi
echo "All check_release_preflight tests passed."

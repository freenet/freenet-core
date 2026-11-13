#!/usr/bin/env bash
# Tests for scripts/check_quic_data_ramp_t0.sh, the release-time check that the
# QUIC data ramp release is cut within 3 h of its ramp start (#5940, #6138).
#
# Each case runs the script against a policy file written here, with the clock
# passed as --now, and checks its exit code and one line of its output. Then it
# checks the tree as it is: with QUIC_DATA_RAMP_RELEASE = None the check is a
# no-op for any version, live or dry. Last, the wiring, each read from the
# step's (or function's) own lines: release.yml's validate job runs it on the
# resolved version and run mode; cross-compile.yml's attach-to-release job runs
# it again on the tag's version before the crates.io publish and the un-draft;
# the local release script runs it at the start and in each publish function.
#
# Run manually: bash scripts/check_quic_data_ramp_t0_test.sh
# Also wired into CI (the Fmt job in .github/workflows/ci.yml).

# The needles below are literal workflow and script text, `${{ }}` and `$VAR`
# included, so single quotes are meant.
# shellcheck disable=SC2016
# Two cases run check_tree in a subshell on purpose, so the failures they
# expect stay out of this run's count.
# shellcheck disable=SC2030,SC2031
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHECK="$SCRIPT_DIR/check_quic_data_ramp_t0.sh"
POLICY="$SCRIPT_DIR/../crates/core/src/transport/quic/upgrade/policy.rs"
RELEASE_YML="$SCRIPT_DIR/../.github/workflows/release.yml"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
FAILURES=0
T0=1800000000
HOUR=3600

# A policy file declaring the two constants as $1 and $2.
policy() {
    local file="$WORK/policy-$RANDOM.rs"
    {
        echo "/// When this release's data ramp starts."
        echo "pub(in crate::transport) const QUIC_DATA_RAMP_START: Option<u64> = $1;"
        echo ""
        echo "#[cfg_attr(not(test), allow(dead_code))]"
        echo "pub(in crate::transport) const QUIC_DATA_RAMP_RELEASE: Option<&str> = $2;"
        echo "// QUIC_DATA_RAMP_START: Option<u64> = Some(1); a comment is not read"
    } > "$file"
    printf '%s' "$file"
}

# A crates/core/Cargo.toml at version $1.
cargo_at() {
    local file="$WORK/cargo-$1.toml"
    printf '[package]\nname = "freenet"\nversion = "%s"\n' "$1" > "$file"
    printf '%s' "$file"
}
SHIPPED="$(cargo_at 0.2.157)"
BEFORE="$(cargo_at 0.2.156)"

# crates.io and GitHub, as the check sees them: a `curl` first on PATH that
# records its arguments (and the header file it is given, which is where a
# token goes) and answers by host: GitHub with HTTP $FAKE_GH_CODE and body
# $FAKE_GH_BODY written to the -o file, crates.io with HTTP $FAKE_CRATES_CODE.
# $FAKE_CURL_EXIT, when set, makes it fail with no answer at all. A `gh` that
# fails keeps the check from reading a real token on this machine.
mkdir -p "$WORK/bin"
cat > "$WORK/bin/curl" <<'CURL'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$FAKE_CURL_LOG"
out="" url=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        -o) out="$2"; shift 2 ;;
        -H) if [[ "$2" == @* ]]; then cat "${2#@}" >> "$FAKE_CURL_LOG.headers"; fi; shift 2 ;;
        http*) url="$1"; shift ;;
        *) shift ;;
    esac
done
if [[ -n "${FAKE_CURL_EXIT:-}" ]]; then
    exit "$FAKE_CURL_EXIT"
fi
case "$url" in
    https://api.github.com/*)
        if [[ -n "$out" ]]; then printf '%s' "$FAKE_GH_BODY" > "$out"; fi
        printf '%s' "$FAKE_GH_CODE" ;;
    *) printf '%s' "$FAKE_CRATES_CODE" ;;
esac
CURL
printf '#!/usr/bin/env bash\nexit 1\n' > "$WORK/bin/gh"
chmod +x "$WORK/bin/curl" "$WORK/bin/gh"
export PATH="$WORK/bin:$PATH"
unset GH_TOKEN GITHUB_TOKEN
export FAKE_CURL_LOG="$WORK/curl.log"
export FAKE_CRATES_CODE=200 FAKE_GH_CODE=200
export FAKE_GH_BODY='{"tag_name": "v0.2.157", "draft": false, "prerelease": false}'
: > "$FAKE_CURL_LOG"

# check_case NAME EXPECTED_RC EXPECTED_TEXT -- ARGS...
check_case() {
    local name="$1" want_rc="$2" want_text="$3" out rc
    shift 4
    out="$(bash "$CHECK" "$@" 2>&1)"
    rc=$?
    if [[ "$rc" != "$want_rc" || "$out" != *"$want_text"* ]]; then
        echo "FAIL - $name: rc=$rc (want $want_rc), output: $out"
        FAILURES=$((FAILURES + 1))
    else
        echo "ok - $name"
    fi
}

unset_both="$(policy None None)"
check_case "no ramp release, live: no-op" 0 "no ramp release" -- \
    --version 0.2.156 --live true --now "$T0" --policy "$unset_both"
check_case "no ramp release, dry run: no-op" 0 "no ramp release" -- \
    --version 0.2.156 --live false --now "$T0" --policy "$unset_both"

set_both="$(policy "Some(1_800_000_000)" 'Some("0.2.157")')"
check_case "a later release, after T0, is not checked (a warning)" 0 "::warning title=QUIC data ramp start::Release 0.2.158 comes after" -- \
    --version 0.2.158 --live true --now $((T0 + 30 * HOUR)) --policy "$set_both" --cargo-toml "$SHIPPED"
check_case "a later release at T0 (a hotfix) is not checked" 0 "comes after the QUIC data ramp release" -- \
    --version 0.2.158 --live true --now "$T0" --policy "$set_both" --cargo-toml "$SHIPPED"
check_case "a later release cut more than 3 h before T0, live: refused" 1 "ahead of its start" -- \
    --version 0.2.158 --live true --now $((T0 - 3 * HOUR - 1)) --policy "$set_both" --cargo-toml "$SHIPPED"
check_case "a later release that skips the unshipped ramp release, live: refused" 1 "skips the QUIC data ramp release" -- \
    --version 0.2.158 --live true --now "$T0" --policy "$set_both" --cargo-toml "$BEFORE"
check_case "a missing Cargo.toml for a later release: refused" 2 "Cargo.toml not found" -- \
    --version 0.2.158 --live true --now "$T0" --policy "$set_both" --cargo-toml "$WORK/none.toml"
if grep -qF -- "-A freenet-release-check" "$FAKE_CURL_LOG" \
    && grep -qF "https://crates.io/api/v1/crates/freenet/0.2.157" "$FAKE_CURL_LOG" \
    && grep -qF "https://api.github.com/repos/freenet/freenet-core/releases/tags/v0.2.157" "$FAKE_CURL_LOG"; then
    echo "ok - a later release asks GitHub for v0.2.157 and crates.io for freenet 0.2.157, with a User-Agent"
else
    echo "FAIL - a later release did not ask GitHub and crates.io about 0.2.157 with a User-Agent: $(cat "$FAKE_CURL_LOG")"
    FAILURES=$((FAILURES + 1))
fi
: > "$FAKE_CURL_LOG"
GH_TOKEN=test-secret-token bash "$CHECK" --version 0.2.158 --live true --now "$T0" \
    --policy "$set_both" --cargo-toml "$SHIPPED" > /dev/null 2>&1
if grep -qF "test-secret-token" "$FAKE_CURL_LOG"; then
    echo "FAIL - the GitHub token is on curl's command line"
    FAILURES=$((FAILURES + 1))
elif grep -qxF "Authorization: Bearer test-secret-token" "$FAKE_CURL_LOG.headers" 2>/dev/null; then
    echo "ok - the GitHub token reaches curl in a header file, not on its command line"
else
    echo "FAIL - the GitHub token did not reach curl"
    FAILURES=$((FAILURES + 1))
fi
export FAKE_GH_CODE=404
check_case "a later release whose ramp release is not a published GitHub release (none, or a draft), live: refused" 1 "is not a published GitHub release" -- \
    --version 0.2.158 --live true --now "$T0" --policy "$set_both" --cargo-toml "$SHIPPED"
check_case "the same, dry run: warning only" 0 "::warning" -- \
    --version 0.2.158 --live false --now "$T0" --policy "$set_both" --cargo-toml "$SHIPPED"
export FAKE_GH_CODE=403
check_case "a later release when GitHub refuses to answer, live: refused" 1 "is a published GitHub release cannot be determined" -- \
    --version 0.2.158 --live true --now "$T0" --policy "$set_both" --cargo-toml "$SHIPPED"
export FAKE_GH_CODE=200 FAKE_GH_BODY='{"tag_name": "v0.2.157", "draft": true}'
check_case "a later release when GitHub's answer does not say published, live: refused" 1 "is a published GitHub release cannot be determined" -- \
    --version 0.2.158 --live true --now "$T0" --policy "$set_both" --cargo-toml "$SHIPPED"
export FAKE_GH_BODY='{"tag_name": "v0.2.157", "draft": false, "prerelease": false}'
export FAKE_CRATES_CODE=404
check_case "a later release whose ramp release is published on GitHub but not on crates.io, live: refused" 1 "is not on crates.io" -- \
    --version 0.2.158 --live true --now "$T0" --policy "$set_both" --cargo-toml "$SHIPPED"
check_case "the same, dry run: warning only" 0 "::warning" -- \
    --version 0.2.158 --live false --now "$T0" --policy "$set_both" --cargo-toml "$SHIPPED"
export FAKE_CRATES_CODE=403
check_case "a later release when crates.io refuses to answer, live: refused" 1 "cannot be determined" -- \
    --version 0.2.158 --live true --now "$T0" --policy "$set_both" --cargo-toml "$SHIPPED"
export FAKE_CRATES_CODE=200 FAKE_CURL_EXIT=7
check_case "a later release when nothing can be reached, live: refused" 1 "cannot be determined" -- \
    --version 0.2.158 --live true --now "$T0" --policy "$set_both" --cargo-toml "$SHIPPED"
unset FAKE_CURL_EXIT
: > "$FAKE_CURL_LOG"
check_case "an earlier release carrying the ramp, live: refused" 1 "comes before the QUIC data ramp release" -- \
    --version 0.2.156 --live true --now "$T0" --policy "$set_both" --cargo-toml "$BEFORE"
check_case "an earlier release carrying the ramp, dry run: warning only" 0 "::warning" -- \
    --version 0.2.156 --live false --now "$T0" --policy "$set_both" --cargo-toml "$BEFORE"
check_case "versions compare as numbers, not text" 1 "comes before the QUIC data ramp release" -- \
    --version 0.2.99 --live true --now "$T0" --policy "$set_both" --cargo-toml "$(cargo_at 0.2.98)"
start_no_release="$(policy "Some(1_800_000_000)" None)"
check_case "a ramp start with no ramp release, live: refused" 1 "QUIC_DATA_RAMP_RELEASE is None" -- \
    --version 0.2.157 --live true --now "$T0" --policy "$start_no_release"
check_case "ramp release at T0" 0 "within 180 min" -- \
    --version 0.2.157 --live true --now "$T0" --policy "$set_both"
check_case "ramp release 3 h after T0 (boundary)" 0 "within 180 min" -- \
    --version 0.2.157 --live true --now $((T0 + 3 * HOUR)) --policy "$set_both"
check_case "ramp release 3 h before T0 (boundary)" 0 "within 180 min" -- \
    --version 0.2.157 --live true --now $((T0 - 3 * HOUR)) --policy "$set_both"
check_case "ramp release 3 h 1 s after T0, live: refused" 1 "min after its ramp start" -- \
    --version 0.2.157 --live true --now $((T0 + 3 * HOUR + 1)) --policy "$set_both"
check_case "ramp release 3 h 1 s before T0, live: refused" 1 "min before its ramp start" -- \
    --version 0.2.157 --live true --now $((T0 - 3 * HOUR - 1)) --policy "$set_both"
check_case "ramp release a day late, dry run: warning only" 0 "::warning" -- \
    --version 0.2.157 --live false --now $((T0 + 24 * HOUR)) --policy "$set_both"

# The two declarations as rustfmt would wrap them if they grew past the line
# width: read the same as on one line.
wrapped="$WORK/wrapped.rs"
cat > "$wrapped" <<'RUST'
pub(in crate::transport) const QUIC_DATA_RAMP_START: Option<u64> = Some(
    1_800_000_000,
);
pub(in crate::transport) const QUIC_DATA_RAMP_RELEASE: Option<&str> =
    Some("0.2.157");
RUST
check_case "wrapped declarations, ramp release at T0" 0 "within 180 min" -- \
    --version 0.2.157 --live true --now "$T0" --policy "$wrapped"
check_case "wrapped declarations, 3 h 1 s late, live: refused" 1 "min after its ramp start" -- \
    --version 0.2.157 --live true --now $((T0 + 3 * HOUR + 1)) --policy "$wrapped"

release_no_start="$(policy None 'Some("0.2.157")')"
check_case "ramp release with no start, live: refused, naming the stop remedies" 1 "cut another version instead, or set QUIC_DATA_RAMP_RELEASE to None" -- \
    --version 0.2.157 --live true --now "$T0" --policy "$release_no_start"
check_case "emergency stop release (start reset) is not checked" 0 "no ramp start" -- \
    --version 0.2.158 --live true --now "$T0" --policy "$release_no_start"

check_case "missing policy file: refused" 2 "policy file not found" -- \
    --version 0.2.157 --live false --now "$T0" --policy "$WORK/does-not-exist.rs"
only_start="$WORK/only-start.rs"
grep -v QUIC_DATA_RAMP_RELEASE "$set_both" > "$only_start"
check_case "missing constant: refused in both modes" 2 "expected one declaration of QUIC_DATA_RAMP_RELEASE" -- \
    --version 0.2.157 --live false --now "$T0" --policy "$only_start"
two="$WORK/two.rs"
cat "$set_both" "$set_both" > "$two"
check_case "two declarations: refused" 2 "found 2" -- \
    --version 0.2.157 --live true --now "$T0" --policy "$two"
bad_release="$(policy "Some(1_800_000_000)" 'Some("v0.2.157")')"
check_case "release not X.Y.Z: refused" 2 "must be None or Some" -- \
    --version 0.2.157 --live true --now "$T0" --policy "$bad_release"
check_case "bad --version: refused" 2 "--version must be X.Y.Z" -- \
    --version v0.2.157 --live true --now "$T0" --policy "$set_both"
check_case "bad --live: refused" 2 "--live must be true or false" -- \
    --version 0.2.157 --live yes --now "$T0" --policy "$set_both"

# check_tree LABEL POLICY CARGO_TOML NOW: the checks CI runs on the tree, here
# so the stop and pre-cut states can be run on fixtures as well as on the tree.
# Each state the constants are meant to pass through must pass CI:
#   - neither set (no ramp): a no-op for any version;
#   - both set (the ramp release's PR): the ramp release passes at its own T0,
#     so a T0 the script cannot read fails CI; and while T0 is more than WINDOW
#     away, the named release must still be ahead of this tree, or the ramp
#     release would never be the one cut (it was already taken), which only the
#     dispatch-time check sees otherwise;
#   - T0 reset to None with the release left named (the emergency stop): a
#     later version passes as "no ramp start", and the named release itself is
#     refused, as the stop intends.
# Any other shape fails.
check_tree() {
    local label="$1" policy="$2" cargo="$3" now="$4"
    local release_none start_none tree_release tree_t0 tree_version later
    release_none=no
    start_none=no
    grep -qxE 'pub\(in crate::transport\) const QUIC_DATA_RAMP_RELEASE: Option<&str> = None;' "$policy" && release_none=yes
    grep -qxE 'pub\(in crate::transport\) const QUIC_DATA_RAMP_START: Option<u64> = None;' "$policy" && start_none=yes
    tree_release="$(sed -n 's/^pub(in crate::transport) const QUIC_DATA_RAMP_RELEASE: Option<&str> = Some("\([0-9]*\.[0-9]*\.[0-9]*\)");$/\1/p' "$policy")"
    tree_t0="$(tr -d '\n ' < "$policy" | sed -n 's/.*pub(incrate::transport)constQUIC_DATA_RAMP_START:Option<u64>=Some(\([0-9_]*\),\{0,1\});.*/\1/p' | tr -d _)"
    if [[ "$release_none" == yes && "$start_none" == yes ]]; then
        for live in true false; do
            check_case "$label (no ramp), live=$live: no-op" 0 "no ramp release" -- \
                --version 0.2.156 --live "$live" --policy "$policy" --cargo-toml "$cargo"
        done
    elif [[ -n "$tree_release" && "$start_none" == yes ]]; then
        later="${tree_release%.*}.$(( ${tree_release##*.} + 1 ))"
        check_case "$label (emergency stop), a later release: passes" 0 "no ramp start" -- \
            --version "$later" --live true --policy "$policy" --cargo-toml "$cargo"
        check_case "$label (emergency stop), the named release: refused" 1 "QUIC_DATA_RAMP_START is None" -- \
            --version "$tree_release" --live true --policy "$policy" --cargo-toml "$cargo"
    elif [[ -n "$tree_release" && -n "$tree_t0" ]]; then
        check_case "$label, as its ramp release at its T0" 0 "is the ramp release" -- \
            --version "$tree_release" --live true --now "$tree_t0" --policy "$policy" --cargo-toml "$cargo"
        tree_version="$(sed -n 's/^version[[:space:]]*=[[:space:]]*"\([0-9][0-9.]*\)".*/\1/p' "$cargo" | head -1)"
        if (( now + 3 * HOUR < 10#$tree_t0 )); then
            if [[ "$tree_version" != "$tree_release" && "$(printf '%s\n%s\n' "$tree_version" "$tree_release" | sort -V | tail -1)" == "$tree_release" ]]; then
                expect "$label: before the cut window, the ramp release ($tree_release) is ahead of the tree ($tree_version)" yes
            else
                expect "$label: before the cut window, the ramp release ($tree_release) is ahead of the tree ($tree_version)" no
            fi
        fi
    else
        echo "FAIL - $label: the ramp constants are in no state CI accepts (release '$tree_release', T0 '$tree_t0', release None: $release_none, T0 None: $start_none)"
        FAILURES=$((FAILURES + 1))
    fi
}

expect() {
    if [[ "$2" == "yes" ]]; then
        echo "ok - $1"
    else
        echo "FAIL - $1"
        FAILURES=$((FAILURES + 1))
    fi
}

# The tree as it is.
TREE_CARGO="$SCRIPT_DIR/../crates/core/Cargo.toml"
if [[ ! -f "$POLICY" ]]; then
    echo "FAIL - policy.rs not found at $POLICY"
    FAILURES=$((FAILURES + 1))
else
    check_tree "this tree" "$POLICY" "$TREE_CARGO" "$(date +%s)"
fi

# The same checks on each state the tree is meant to pass through.
check_tree "fixture: no ramp" "$unset_both" "$BEFORE" "$T0"
check_tree "fixture: the stop" "$release_no_start" "$SHIPPED" "$T0"
check_tree "fixture: ramp PR, at T0" "$set_both" "$SHIPPED" "$T0"
check_tree "fixture: ramp PR, a day before T0" "$set_both" "$BEFORE" $((T0 - 24 * HOUR))
# The release already taken before the ramp PR merged: refused before the
# cut window, where only the dispatch check would otherwise see it.
before_out="$(FAILURES=0; check_tree "x" "$set_both" "$SHIPPED" $((T0 - 24 * HOUR)); echo "rc=$FAILURES")"
if [[ "$before_out" == *"FAIL - x: before the cut window"* && "$before_out" == *"rc=1" ]]; then
    echo "ok - a ramp release the tree has already reached, a day before T0: refused"
else
    echo "FAIL - a ramp release the tree has already reached, a day before T0, was not refused: $before_out"
    FAILURES=$((FAILURES + 1))
fi
garbled="$(policy "Some(soon)" 'Some("0.2.157")')"
garbled_out="$(FAILURES=0; check_tree "x" "$garbled" "$BEFORE" "$T0"; echo "rc=$FAILURES")"
if [[ "$garbled_out" == *"in no state CI accepts"* && "$garbled_out" == *"rc=1" ]]; then
    echo "ok - an unreadable T0 in the tree fails CI"
else
    echo "FAIL - an unreadable T0 in the tree did not fail CI: $garbled_out"
    FAILURES=$((FAILURES + 1))
fi

# The lines of job $2 in workflow file $1, from its `  <job>:` line to the next
# job.
job_block() {
    awk -v head="  $2:" '
        $0 == head { inside = 1; print; next }
        inside && /^  [A-Za-z0-9_-]+:/ { exit }
        inside { print }
    ' "$1"
}

# The lines of the step named $1 in the job block on stdin, from its
# `      - name:` line to the next step. Its own lines only, so a call or an
# input in another step of the job does not count for it.
step_block() {
    awk -v head="      - name: $1" '
        $0 == head { inside = 1; print; next }
        inside && /^      - / { exit }
        inside { print }
    '
}

# The 1-based position of the step named $2 among the steps of job block $1, or
# empty.
step_index() {
    printf '%s\n' "$1" | grep -E '^      - name: ' | sed -E 's/^      - name: //' | grep -nxF -- "$2" | cut -d: -f1 | head -1
}

nonempty() { if [[ -n "$1" ]]; then echo yes; else echo no; fi; }

# The text on stdin without blank lines and `#` comment lines: what YAML or
# bash acts on. A step block also holds the next step's leading comments,
# which this drops.
code_lines() { grep -vE '^[[:space:]]*(#|$)' || true; }

# exact NAME ACTUAL EXPECTED: the step's (or function's) code is exactly
# EXPECTED, so an added `continue-on-error`, `if:`, `|| true`, extra argument
# or extra line fails, where a substring match would pass it.
exact() {
    if [[ "$2" == "$3" ]]; then
        echo "ok - $1"
    else
        echo "FAIL - $1"
        diff <(printf '%s\n' "$3") <(printf '%s\n' "$2") | sed 's/^/    /'
        FAILURES=$((FAILURES + 1))
    fi
}

# No `continue-on-error` anywhere in a job block (job or step level): either
# would let the publish run after a refusal.
no_continue_on_error() {
    if printf '%s\n' "$2" | code_lines | grep -qE '^[[:space:]]*continue-on-error:'; then
        expect "$1 has no continue-on-error" no
    else
        expect "$1 has no continue-on-error" yes
    fi
}

# release.yml's validate job runs the check, in a step of its own that is given
# the resolved version and run mode, after the steps that resolve them.
VALIDATE_STEP="Check the QUIC data ramp start is near now"
validate_job="$(job_block "$RELEASE_YML" validate)"
validate_step="$(printf '%s\n' "$validate_job" | step_block "$VALIDATE_STEP")"
expect "release.yml's validate job has the check step" "$(nonempty "$validate_step")"
exact "the validate step runs the check on the resolved version and run mode, and nothing else" \
    "$(printf '%s\n' "$validate_step" | code_lines)" \
    '      - name: Check the QUIC data ramp start is near now
        env:
          RELEASE_VERSION: ${{ steps.validate.outputs.version }}
          RELEASE_LIVE: ${{ steps.mode.outputs.live }}
          GH_TOKEN: ${{ github.token }}
        run: bash scripts/check_quic_data_ramp_t0.sh --version "$RELEASE_VERSION" --live "$RELEASE_LIVE"'
no_continue_on_error "release.yml's validate job" "$validate_job"
order="$(printf '%s\n' "$validate_job" | grep -nxE '        id: (mode|validate)|      - name: '"$VALIDATE_STEP" | cut -d: -f1 | tr '\n' ' ')"
read -r first second third <<< "$order"
if [[ -n "${third:-}" && "$third" -gt "$first" && "$third" -gt "$second" ]]; then
    expect "the check runs after the mode and version are resolved" yes
else
    expect "the check runs after the mode and version are resolved (lines: $order)" no
fi

# cross-compile.yml's attach-to-release job checks again, live on the tag's
# version, after the source checkout and before the crates.io publish and the
# un-draft, so a re-run of the job checks the clock it publishes at.
CROSS_YML="$SCRIPT_DIR/../.github/workflows/cross-compile.yml"
ATTACH_STEP="Check the QUIC data ramp start is near now (before publishing)"
attach_job="$(job_block "$CROSS_YML" attach-to-release)"
attach_step="$(printf '%s\n' "$attach_job" | step_block "$ATTACH_STEP")"
ATTACH_IF='        if: ${{ success() && startsWith(github.ref, '"'refs/tags/v'"') }}'
expect "attach-to-release has the check step" "$(nonempty "$attach_step")"
exact "the attach step runs the check live on the tag's version, in the source, on success only, and nothing else" \
    "$(printf '%s\n' "$attach_step" | code_lines)" \
    "      - name: $ATTACH_STEP
$ATTACH_IF
        working-directory: _src
        env:
          GH_TOKEN: \${{ github.token }}
        run: |
          TAG_NAME=\"\${GITHUB_REF#refs/tags/}\"
          bash scripts/check_quic_data_ramp_t0.sh --version \"\${TAG_NAME#v}\" --live true"
no_continue_on_error "cross-compile.yml's attach-to-release job" "$attach_job"
# A refusal stops the publish only because the steps after it require success:
# the crates.io publish states the same `if:` as the check, and the un-draft
# has none (so success() applies). A skipped check would not stop them, which
# is why the check's own `if:` is pinned above.
publish_ifs="$(printf '%s\n' "$attach_job" | step_block "Publish crates to crates.io" | code_lines | grep -E '^        if:')"
exact "the crates.io publish runs only on success, under the check's condition" "$publish_ifs" "$ATTACH_IF"
# The un-draft is the step named exactly "Publish release", and it is the only
# step in the job that un-drafts: renaming it, or un-drafting from another
# step, fails here rather than leaving the condition check below reading an
# empty step.
undraft_step="$(printf '%s\n' "$attach_job" | step_block "Publish release")"
expect "attach-to-release has the un-draft step, named \"Publish release\"" "$(nonempty "$undraft_step")"
undraft_in_job="$(printf '%s\n' "$attach_job" | code_lines | grep -cF -- '--draft=false' || true)"
undraft_in_step="$(printf '%s\n' "$undraft_step" | code_lines | grep -cF -- '--draft=false' || true)"
if [[ "$undraft_in_step" -ge 1 && "$undraft_in_job" == "$undraft_in_step" ]]; then
    expect "only the \"Publish release\" step un-drafts" yes
else
    expect "only the \"Publish release\" step un-drafts (job $undraft_in_job, step $undraft_in_step)" no
fi
undraft_ifs="$(printf '%s\n' "$undraft_step" | code_lines | grep -E '^        if:' || true)"
exact "the un-draft has no condition of its own (success() applies)" "$undraft_ifs" ""
checkout_at="$(step_index "$attach_job" "Check out the release source")"
check_at="$(step_index "$attach_job" "$ATTACH_STEP")"
publish_at="$(step_index "$attach_job" "Publish crates to crates.io")"
undraft_at="$(step_index "$attach_job" "Publish release")"
if [[ -n "$checkout_at" && -n "$check_at" && -n "$publish_at" && "$check_at" -gt "$checkout_at" && "$check_at" -lt "$publish_at" ]]; then
    expect "the attach check comes after the checkout and before the crates.io publish" yes
else
    expect "the attach check comes after the checkout and before the crates.io publish ($checkout_at/$check_at/$publish_at)" no
fi
if [[ -n "$undraft_at" && -n "$check_at" && "$check_at" -lt "$undraft_at" ]]; then
    expect "the attach check comes before the un-draft" yes
else
    expect "the attach check comes before the un-draft ($check_at/$undraft_at)" no
fi

# The local release script checks when it starts and again inside each publish
# function, at statement position, so a resumed run checks before publishing.
RELEASE_SH="$SCRIPT_DIR/rel""ease.sh"
fn_body() {
    awk -v head="$2() {" '
        $0 == head { inside = 1; next }
        inside && /^}$/ { exit }
        inside { print }
    ' "$1"
}
for fn in publish_crates publish_draft_release; do
    exact "the local release script checks first thing in $fn" \
        "$(fn_body "$RELEASE_SH" "$fn" | code_lines | head -1)" '    check_quic_data_ramp_t0'
done
# One definition of the helper: bash runs the last one, while fn_body reads
# the first, so a second definition would replace the one pinned below.
helper_defs="$(grep -cE '^[[:space:]]*(function[[:space:]]+)?check_quic_data_ramp_t0[[:space:]]*(\(\))?[[:space:]]*\{' "$RELEASE_SH" || true)"
exact "the local release script defines the helper once" "$helper_defs" "1"
# The helper refuses with exit 1 on any failure of the check, whatever the run
# mode (the check itself makes a dry run's finding a warning).
exact "the local release script's helper exits 1 when the check refuses" \
    "$(fn_body "$RELEASE_SH" check_quic_data_ramp_t0 | code_lines)" \
    '    local live=true
    if [[ "$DRY_RUN" == "true" ]]; then
        live=false
    fi
    if ! bash "$PROJECT_ROOT/scripts/check_quic_data_ramp_t0.sh" --version "$VERSION" --live "$live"; then
        echo "  ✗ The QUIC data ramp start check refused release $VERSION; see above."
        exit 1
    fi'
if grep -A1 -xF 'check_prerequisites' "$RELEASE_SH" | grep -qxF 'check_quic_data_ramp_t0'; then
    expect "the local release script checks when it starts" yes
else
    expect "the local release script checks when it starts" no
fi

if [[ "$FAILURES" -gt 0 ]]; then
    echo "$FAILURES failure(s)"
    exit 1
fi
echo "all passed"

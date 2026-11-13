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
check_case "ramp release with no start, live: refused" 1 "QUIC_DATA_RAMP_START is None" -- \
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

# The tree as it is.
if [[ ! -f "$POLICY" ]]; then
    echo "FAIL - policy.rs not found at $POLICY"
    FAILURES=$((FAILURES + 1))
fi
if grep -qE '^pub\(in crate::transport\) const QUIC_DATA_RAMP_RELEASE: Option<&str> = None;$' "$POLICY"; then
    for live in true false; do
        check_case "this tree (release None), live=$live: no-op" 0 "no ramp release" -- \
            --version 0.2.156 --live "$live" --policy "$POLICY"
    done
else
    # Once the ramp release is named, check the tree as that release at its own
    # T0, so a T0 the script cannot read fails CI instead of passing as "not
    # the ramp release".
    tree_release="$(sed -n 's/^pub(in crate::transport) const QUIC_DATA_RAMP_RELEASE: Option<&str> = Some("\([0-9.]*\)");$/\1/p' "$POLICY")"
    tree_t0="$(tr -d '\n ' < "$POLICY" | sed -n 's/.*pub(incrate::transport)constQUIC_DATA_RAMP_START:Option<u64>=Some(\([0-9_]*\),\{0,1\});.*/\1/p' | tr -d _)"
    if [[ -z "$tree_release" || -z "$tree_t0" ]]; then
        echo "FAIL - this tree names a ramp release but its release or T0 cannot be read (release '$tree_release', T0 '$tree_t0')"
        FAILURES=$((FAILURES + 1))
    else
        check_case "this tree, as its ramp release at its T0" 0 "is the ramp release" -- \
            --version "$tree_release" --live true --now "$tree_t0" --policy "$POLICY"
    fi
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

expect() {
    if [[ "$2" == "yes" ]]; then
        echo "ok - $1"
    else
        echo "FAIL - $1"
        FAILURES=$((FAILURES + 1))
    fi
}
has() { if [[ "$1" == *"$2"* ]]; then echo yes; else echo no; fi; }
nonempty() { if [[ -n "$1" ]]; then echo yes; else echo no; fi; }

# release.yml's validate job runs the check, in a step of its own that is given
# the resolved version and run mode, after the steps that resolve them.
VALIDATE_STEP="Check the QUIC data ramp start is near now"
validate_job="$(job_block "$RELEASE_YML" validate)"
validate_step="$(printf '%s\n' "$validate_job" | step_block "$VALIDATE_STEP")"
expect "release.yml's validate job has the check step" "$(nonempty "$validate_step")"
expect "the validate step runs the check" "$(has "$validate_step" 'run: bash scripts/check_quic_data_ramp_t0.sh --version "$RELEASE_VERSION" --live "$RELEASE_LIVE"')"
expect "the validate step is given the resolved version" "$(has "$validate_step" 'RELEASE_VERSION: ${{ steps.validate.outputs.version }}')"
expect "the validate step is given the resolved run mode" "$(has "$validate_step" 'RELEASE_LIVE: ${{ steps.mode.outputs.live }}')"
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
expect "attach-to-release has the check step" "$(nonempty "$attach_step")"
expect "the attach step runs the check live on the tag's version" "$(has "$attach_step" 'bash scripts/check_quic_data_ramp_t0.sh --version "${TAG_NAME#v}" --live true')"
expect "the attach step takes the tag from the ref" "$(has "$attach_step" 'TAG_NAME="${GITHUB_REF#refs/tags/}"')"
expect "the attach step runs in the checked-out source" "$(has "$attach_step" 'working-directory: _src')"
expect "the attach step runs only on success" "$(has "$attach_step" 'if: ${{ success() && startsWith(github.ref, ')"
checkout_at="$(step_index "$attach_job" "Check out the release source")"
check_at="$(step_index "$attach_job" "$ATTACH_STEP")"
publish_at="$(step_index "$attach_job" "Publish crates to crates.io")"
undraft_at="$(printf '%s\n' "$attach_job" | grep -E '^      - name: ' | grep -nE 'Publish (the )?release' | tail -1 | cut -d: -f1)"
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
    if fn_body "$RELEASE_SH" "$fn" | grep -qxE '[[:space:]]+check_quic_data_ramp_t0'; then
        expect "the local release script checks in $fn" yes
    else
        expect "the local release script checks in $fn" no
    fi
done
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

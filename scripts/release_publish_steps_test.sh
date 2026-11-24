#!/usr/bin/env bash
# Behavioural tests for the release workflow steps that publish, sign and
# announce a release: cross-compile.yml's attach-to-release job,
# release-announce.yml's resolve job, and two release.yml steps (the
# RELEASE_PAT check and the release notes).
#
# Each step is extracted from the real workflow with
# `check_release_dry_run.py --dump-step` (its `${{ }}` expressions become
# literal `@@expr@@` tokens) and EXECUTED with its own `env:` against stub gh
# and zip, so a test sees what the step does rather than what its text says.
# The inputs a case cares about are passed as env overrides.
#
# Run manually with: bash scripts/release_publish_steps_test.sh

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LINTER="$REPO_ROOT/.github/scripts/check_release_dry_run.py"
CROSS_YML="$REPO_ROOT/.github/workflows/cross-compile.yml"
ANNOUNCE_YML="$REPO_ROOT/.github/workflows/release-announce.yml"
RELEASE_YML="$REPO_ROOT/.github/workflows/release.yml"
DOCKER_YML="$REPO_ROOT/.github/workflows/docker-publish.yml"
# An ambient token or ref must not leak into a step under test.
unset GH_TOKEN GITHUB_TOKEN GITHUB_REF
TEST_TMP=$(mktemp -d)
trap 'rm -rf "$TEST_TMP"' EXIT

FAILURES=0
check() {
    # check <description> <actual> <expected>
    local desc="$1" actual="$2" expected="$3"
    if [[ "$actual" == "$expected" ]]; then
        echo "ok   - $desc"
    else
        echo "FAIL - $desc (got '$actual', expected '$expected')" >&2
        FAILURES=$((FAILURES + 1))
    fi
}

# One gh stub for every step: it logs its argv to $STUB_CALLS and answers from
# env, so each case states its inputs inline.
#   LIST_OUT / LIST_RC: `gh release list` output (after --jq) and exit code
#   LIST_JSON: when set, the raw `gh release list --json` answer instead; the
#              step's own --jq filter is applied to it with real jq, so the
#              filter itself (e.g. dropping prereleases) is under test
#   COMBINED_RC: exit code of a `release edit` that both un-drafts and sets --latest=
#   LATEST_RC: exit code of a bare `release edit ... --latest`
#   API_OUT: `gh api` output
STUBS="$TEST_TMP/bin"
mkdir -p "$STUBS"
cat > "$STUBS/gh" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >> "$STUB_CALLS"
case "$1 $2" in
    "release list")
        [ "${LIST_RC:-0}" = 0 ] || exit "$LIST_RC"
        if [ -n "${LIST_JSON:-}" ]; then
            filter=""
            while [ $# -gt 0 ]; do
                [ "$1" = --jq ] && { filter="$2"; break; }
                shift
            done
            jq -r "${filter:-.}" <<<"$LIST_JSON"
        else
            printf '%b' "${LIST_OUT:-}"
        fi ;;
    "release edit")
        case " $* " in
            *" --draft=false --latest="*) exit "${COMBINED_RC:-0}" ;;
            *" --latest ") exit "${LATEST_RC:-0}" ;;
        esac ;;
    "api *"|api*) printf '%s\n' "${API_OUT:-notes}" ;;
esac
exit 0
EOF
# zip <archive> <file>: the real one is not needed to test the step's control flow.
cat > "$STUBS/zip" <<'EOF'
#!/usr/bin/env bash
[ -f "$2" ] && : > "$1"
EOF
chmod +x "$STUBS/gh" "$STUBS/zip"

# load_step <workflow> <job> <step> <dir>: writes <dir>/step.sh and <dir>/step.env.
load_step() {
    local dir="$4"
    rm -rf "$dir"
    mkdir -p "$dir/work"
    if ! python3 "$LINTER" --dump-step "$1" "$2" "$3" > "$dir/dump" 2> "$dir/dump.err"; then
        echo "FAIL - could not extract '$2 / $3' from $(basename "$1")" >&2
        cat "$dir/dump.err" >&2
        FAILURES=$((FAILURES + 1))
        return 1
    fi
    awk '/^@@ENV@@$/ {exit} {print}' "$dir/dump" > "$dir/step.sh"
    awk 'f && NF {print} /^@@ENV@@$/ {f = 1}' "$dir/dump" > "$dir/step.env"
}

# run_step <dir> [KEY=value ...]: runs <dir>/step.sh in <dir>/work as Actions
# does (`bash -e`), with the step's env, then the overrides. Prints the exit
# code; output lands in <dir>/out, gh calls in <dir>/calls.
run_step() {
    local dir="$1" rc=0 line
    shift
    local -a envs=()
    while IFS= read -r line; do envs+=("$line"); done < "$dir/step.env"
    : > "$dir/calls"
    : > "$dir/github_output"
    (
        builtin cd -- "$dir/work" || exit 99
        env ${envs[@]+"${envs[@]}"} "$@" PATH="$STUBS:$PATH" STUB_CALLS="$dir/calls" \
            GITHUB_OUTPUT="$dir/github_output" bash -e "$dir/step.sh"
    ) > "$dir/out" 2>&1 || rc=$?
    echo "$rc"
}

# --- 1. cross-compile.yml: Publish release ------------------------------------
#
# The un-draft and the latest marker are ONE call, decided before it: only the
# highest published version takes the marker, a failed listing fails while the
# release is still a draft, and a refused combined call falls back to the old
# two-call form only for the newest release (a bare un-draft is made latest by
# GitHub, which for an older tag moves the marker backwards).
PUB="$TEST_TMP/publish"
if load_step "$CROSS_YML" attach-to-release "Publish release" "$PUB"; then
    # publish_case <desc> <tag> <published tags, \n-separated> <list rc> <combined edit rc> <expected>
    #   expected: "rc=N edits=<each release edit, --repo dropped, joined by |>"
    publish_case() {
        local desc="$1" tag="$2" published="$3" list_rc="$4" combined_rc="$5" want="$6" rc edits
        rc=$(run_step "$PUB" GITHUB_REF="refs/tags/$tag" LIST_OUT="$published" LIST_RC="$list_rc" COMBINED_RC="$combined_rc")
        edits=$(grep '^release edit' "$PUB/calls" | sed 's/ --repo [^ ]*//' | paste -sd'|' -)
        check "Publish release: $desc" "rc=$rc edits=$edits" "$want"
    }
    publish_case "the highest tag un-drafts and takes latest in one call" v0.2.99 'v0.2.98\nv0.2.97\n' 0 0 \
        "rc=0 edits=release edit v0.2.99 --draft=false --latest=true"
    publish_case "a tag older than a published release does not take latest" v0.2.99 'v0.2.100\nv0.2.98\n' 0 0 \
        "rc=0 edits=release edit v0.2.99 --draft=false --latest=false"
    publish_case "versions compare numerically (v0.2.100 over v0.2.99)" v0.2.100 'v0.2.99\nv0.2.98\n' 0 0 \
        "rc=0 edits=release edit v0.2.100 --draft=false --latest=true"
    publish_case "no published release yet: takes latest" v0.2.99 '' 0 0 \
        "rc=0 edits=release edit v0.2.99 --draft=false --latest=true"
    publish_case "a failed listing fails the step before any edit" v0.2.99 'v0.2.98\n' 1 0 \
        "rc=1 edits="
    publish_case "combined edit refused for the newest: un-draft, then latest" v0.2.99 'v0.2.98\n' 0 1 \
        "rc=0 edits=release edit v0.2.99 --draft=false --latest=true|release edit v0.2.99 --draft=false|release edit v0.2.99 --latest"
    publish_case "combined edit refused for an older tag: fails, no bare un-draft" v0.2.99 'v0.2.100\n' 0 1 \
        "rc=1 edits=release edit v0.2.99 --draft=false --latest=false"
    # The release is already public by the fallback's last call, so a refused
    # --latest there warns rather than failing a published release.
    rc=$(run_step "$PUB" GITHUB_REF=refs/tags/v0.2.99 LIST_OUT='v0.2.98\n' COMBINED_RC=1 LATEST_RC=1)
    check "Publish release: a refused --latest in the fallback warns, exit 0" \
        "rc=$rc $(grep -c '^::warning::could not mark v0.2.99 as latest' "$PUB/out")" "rc=0 1"
    # Drafts must not count: a spent version left as a draft is not published.
    check "Publish release: the listing excludes drafts" \
        "$(grep -c '^release list .*--exclude-drafts' "$PUB/calls")" "1"
    # The step's own --jq runs here, so deleting its prerelease filter fails this.
    rc=$(run_step "$PUB" GITHUB_REF="refs/tags/v0.2.99" \
        LIST_JSON='[{"tagName":"v0.3.0-rc1","isPrerelease":true},{"tagName":"v0.2.98","isPrerelease":false}]')
    check "Publish release: a newer PRERELEASE does not stop the release taking latest" \
        "rc=$rc edits=$(grep '^release edit' "$PUB/calls" | sed 's/ --repo [^ ]*//' | paste -sd'|' -)" \
        "rc=0 edits=release edit v0.2.99 --draft=false --latest=true"
fi

# shellcheck disable=SC2016  # a literal $MAKE_LATEST is the text being matched
check "Publish release: reports made_latest for Gate B" \
    "$(grep -c 'made_latest=\$MAKE_LATEST' "$PUB/step.sh" 2>/dev/null)" "1"
check "Gate B skips a non-latest recovery publish" \
    "$(python3 -c '
import sys, yaml
jobs = yaml.safe_load(open(sys.argv[1]))["jobs"]
print(jobs["attach-to-release"]["outputs"]["made_latest"], jobs["auto-update-selfupdate-canary"]["if"], sep="|")
' "$CROSS_YML")" \
    "\${{ steps.publish.outputs.made_latest }}|startsWith(github.ref, 'refs/tags/v') && needs.attach-to-release.outputs.made_latest != 'false'"

# --- 2. cross-compile.yml: signing, checksums, assets -------------------------
SIGN="$TEST_TMP/sign"
if load_step "$CROSS_YML" attach-to-release "Sign SHA256SUMS.txt (ed25519)" "$SIGN"; then
    printf 'abc  freenet.exe\n' > "$SIGN/work/SHA256SUMS.txt"
    check "Sign: a missing signing key refuses an unsigned release" \
        "rc=$(run_step "$SIGN" FREENET_RELEASE_SIGNING_KEY=) sig=$([ -e "$SIGN/work/SHA256SUMS.txt.sig" ] && echo yes || echo no)" \
        "rc=1 sig=no"
    if KEY=$(openssl genpkey -algorithm ed25519 2>/dev/null) && [[ -n "$KEY" ]]; then
        check "Sign: a key signs the manifest (64-byte signature)" \
            "rc=$(run_step "$SIGN" FREENET_RELEASE_SIGNING_KEY="$KEY") sig=$(wc -c < "$SIGN/work/SHA256SUMS.txt.sig" 2>/dev/null | tr -d ' ')" \
            "rc=0 sig=64"
    else
        echo "FAIL - openssl cannot make an ed25519 key; the positive signing case cannot run" >&2
        FAILURES=$((FAILURES + 1))
    fi
    rm -f "$SIGN/work/SHA256SUMS.txt.sig"
    : > "$SIGN/work/SHA256SUMS.txt"
    check "Sign: an empty manifest refuses" "rc=$(run_step "$SIGN" FREENET_RELEASE_SIGNING_KEY=x)" "rc=1"
fi

SUMS="$TEST_TMP/sums"
if load_step "$CROSS_YML" attach-to-release "Generate SHA256 checksums" "$SUMS"; then
    touch "$SUMS/work/a.tar.gz" "$SUMS/work/a.zip" "$SUMS/work/a.exe" "$SUMS/work/a.dmg"
    check "SHA256 checksums: all asset classes present" "rc=$(run_step "$SUMS")" "rc=0"
    rm "$SUMS/work/a.dmg"
    check "SHA256 checksums: a missing asset class fails the step" "rc=$(run_step "$SUMS")" "rc=1"
fi

ASSETS="$TEST_TMP/assets"
if load_step "$CROSS_YML" attach-to-release "Prepare release assets" "$ASSETS"; then
    make_artifacts() {
        local a="$ASSETS/work/artifacts" t
        rm -rf "$ASSETS/work"
        mkdir -p "$ASSETS/work"
        for t in x86_64-linux arm64-linux arm64-macos x86_64-macos; do
            mkdir -p "$a/$t-freenet" "$a/$t-fdev"
            echo bin > "$a/$t-freenet/freenet"
            echo bin > "$a/$t-fdev/fdev"
        done
        mkdir -p "$a/x86_64-windows-freenet" "$a/x86_64-windows-fdev" "$a/macos-dmg"
        echo bin > "$a/x86_64-windows-freenet/freenet.exe"
        echo bin > "$a/x86_64-windows-fdev/fdev.exe"
        echo dmg > "$a/macos-dmg/Freenet-9.9.9.dmg"
    }
    make_artifacts
    check "Prepare release assets: a full artifact set packages" \
        "rc=$(run_step "$ASSETS") $(find "$ASSETS/work" -maxdepth 1 -type f \( -name '*.tar.gz' -o -name '*.zip' -o -name '*.exe' -o -name '*.dmg' \) | wc -l | tr -d ' ')" "rc=0 13"
    # A missing artifact directory: the old `cd X && tar ... && cd ../..` form
    # skipped the whole line (a failed `cd` in an && list is not an errexit) and
    # the release shipped without that archive.
    make_artifacts
    rm -rf "$ASSETS/work/artifacts/x86_64-linux-fdev"
    rc=$(run_step "$ASSETS")
    check "Prepare release assets: a missing artifact fails the step" "$([ "$rc" -ne 0 ] && echo failed || echo "passed rc=$rc")" "failed"
    # The Windows archives are zipped in a subshell; its failure must count too.
    make_artifacts
    rm -rf "$ASSETS/work/artifacts/x86_64-windows-fdev"
    rc=$(run_step "$ASSETS")
    check "Prepare release assets: a missing Windows artifact fails the step" "$([ "$rc" -ne 0 ] && echo failed || echo "passed rc=$rc")" "failed"
    check "Prepare release assets: runs under set -euo pipefail" \
        "$(grep -cxF 'set -euo pipefail' "$ASSETS/step.sh")" "1"
    check "Prepare release assets: no 'cd artifacts/... && tar' form" \
        "$(grep -cE 'cd artifacts/.*&&.*tar' "$ASSETS/step.sh")" "0"
fi

# --- 3. cross-compile.yml: structure the steps above cannot see ----------------
#
# pwsh fails a step only on its LAST native command, so each earlier command
# in these two steps must check its own exit code.
for spec in "Compile for x86_64-pc-windows-msvc|cargo build --release -p freenet" \
            "Smoke test|target/release/freenet.exe --version"; do
    step="${spec%%|*}" cmd="${spec#*|}"
    WIN="$TEST_TMP/win"
    if load_step "$CROSS_YML" build-x86_64-windows "$step" "$WIN"; then
        # shellcheck disable=SC2016 # the expected text is PowerShell, not shell
        check "Windows '$step': '$cmd' is followed by an exit-code check" \
            "$(awk -v c="$cmd" 'prev == c {print; exit} {prev = $0}' "$WIN/step.sh")" \
            'if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }'
    fi
done
# A second run for a tag (the documented recovery re-run) must queue, not
# cancel the first one between the crates.io publishes and the un-draft.
check "cross-compile concurrency never cancels a tag run" \
    "$(python3 -c '
import sys, yaml
print(yaml.safe_load(open(sys.argv[1]))["concurrency"]["cancel-in-progress"])
' "$CROSS_YML")" \
    "\${{ github.ref != 'refs/heads/main' && !startsWith(github.ref, 'refs/tags/') }}"

# --- 4. release-announce.yml: Resolve version ----------------------------------
#
# A release event announces only the newest non-prerelease; a dispatch is
# taken as meant; a listing failure fails rather than announcing blind.
ANN="$TEST_TMP/announce"
if load_step "$ANNOUNCE_YML" resolve "Resolve version" "$ANN"; then
    # announce_case <desc> <event> <tag or dispatch version> <prerelease> <published> <list rc> <expected "rc=N skip=X">
    announce_case() {
        local desc="$1" event="$2" ver="$3" pre="$4" published="$5" list_rc="$6" want="$7" rc
        rc=$(run_step "$ANN" EVENT_NAME="$event" TAG_NAME="$ver" DISPATCH_VERSION="$ver" PRERELEASE="$pre" \
            REPO=example/repo LIST_OUT="$published" LIST_RC="$list_rc")
        check "Resolve version: $desc" "rc=$rc skip=$(sed -n 's/^skip=//p' "$ANN/github_output" | paste -sd, -)" "$want"
    }
    announce_case "the newest release is announced" release v0.2.99 false 'v0.2.98\n' 0 "rc=0 skip=false"
    announce_case "versions compare numerically (v0.2.100 over v0.2.99)" release v0.2.100 false 'v0.2.99\n' 0 "rc=0 skip=false"
    announce_case "a prerelease is not announced" release v0.2.99 true 'v0.2.98\n' 0 "rc=0 skip=true"
    announce_case "a release older than a published one is not announced" release v0.2.99 false 'v0.2.100\n' 0 "rc=0 skip=true"
    announce_case "a workflow_dispatch is announced as asked" workflow_dispatch 0.2.99 "" 'v0.2.100\n' 0 "rc=0 skip=false"
    announce_case "a failed listing on a release event fails the step" release v0.2.99 false '' 1 "rc=1 skip="
    check "Resolve version: the listing excludes drafts" \
        "$(grep -c '^release list .*--exclude-drafts' "$ANN/calls")" "1"
    # The step's own --jq runs here, so deleting its prerelease filter fails this.
    rc=$(run_step "$ANN" EVENT_NAME=release TAG_NAME=v0.2.99 DISPATCH_VERSION= PRERELEASE=false REPO=example/repo \
        LIST_JSON='[{"tagName":"v0.3.0-rc1","isPrerelease":true},{"tagName":"v0.2.98","isPrerelease":false}]')
    check "Resolve version: a newer PRERELEASE does not suppress the announcement" \
        "rc=$rc skip=$(sed -n 's/^skip=//p' "$ANN/github_output" | paste -sd, -)" "rc=0 skip=false"
fi
check "release-announce: both posting jobs are gated on resolve's skip output" \
    "$(python3 -c '
import sys, yaml
jobs = yaml.safe_load(open(sys.argv[1]))["jobs"]
print(jobs["resolve"]["outputs"].get("skip"), *(jobs[j].get("if") for j in ("matrix", "river")), sep="|")
' "$ANNOUNCE_YML")" \
    "\${{ steps.v.outputs.skip }}|needs.resolve.outputs.skip != 'true'|needs.resolve.outputs.skip != 'true'"

# --- 5. release.yml --------------------------------------------------------------
PAT="$TEST_TMP/pat"
if load_step "$RELEASE_YML" validate "Check RELEASE_PAT is configured" "$PAT"; then
    # pat_case <desc> <present> <live> <expected "rc=N <annotation>">
    pat_case() {
        local rc ann
        rc=$(run_step "$PAT" RELEASE_PAT_PRESENT="$2" RELEASE_LIVE="$3")
        ann=$(grep -oE '^::(error|warning)' "$PAT/out" | head -1)
        check "RELEASE_PAT: $1" "rc=$rc ${ann:-none}" "$4"
    }
    pat_case "configured: passes" true true "rc=0 none"
    pat_case "missing on a live run: refuses" false true "rc=1 ::error"
    pat_case "missing on a dry run: warns only" false false "rc=0 ::warning"
fi

# step_if <workflow> <job> <step>: the step's `if:` expression, or None.
step_if() {
    python3 -c '
import sys, yaml
steps = yaml.safe_load(open(sys.argv[1]))["jobs"][sys.argv[2]]["steps"]
print(next(s for s in steps if s.get("name") == sys.argv[3]).get("if"))
' "$@"
}
LIVE_GATE="steps.mode.outputs.live == 'true'"

# The live pre-flight runs scripts/check_release_preflight.sh (tested on its
# own). Here a stand-in in the work dir records what the step hands it, so
# `run: true` or a dropped env entry fails, and its exit code must propagate.
PRE="$TEST_TMP/preflight"
if load_step "$RELEASE_YML" validate "Refuse a version that is spent or already being cut" "$PRE"; then
    mkdir -p "$PRE/work/scripts"
    # shellcheck disable=SC2016  # expanded by the stand-in, not here
    printf '#!/usr/bin/env bash
printf "RELEASE_VERSION=%%s GH_TOKEN=%%s\n" "${RELEASE_VERSION-unset}" "${GH_TOKEN-unset}" > "$PWD/ran"
exit "${FAKE_RC:-0}"
' \
        > "$PRE/work/scripts/check_release_preflight.sh"
    rm -f "$PRE/work/ran"
    rc=$(run_step "$PRE")
    check "pre-flight step: runs check_release_preflight.sh with the version and a token" \
        "rc=$rc $(cat "$PRE/work/ran" 2>/dev/null || echo not-run)" \
        "rc=0 RELEASE_VERSION=@@steps.validate.outputs.version@@ GH_TOKEN=@@github.token@@"
    check "pre-flight step: a refusal fails the step" "rc=$(run_step "$PRE" FAKE_RC=1)" "rc=1"
fi
check "pre-flight step: gated on a live run" \
    "$(step_if "$RELEASE_YML" validate "Refuse a version that is spent or already being cut")" "$LIVE_GATE"

SIGNKEY="$TEST_TMP/signkey"
if load_step "$RELEASE_YML" validate "Verify the release signing key before anything is created" "$SIGNKEY"; then
    check "signing-key check: present passes" \
        "rc=$(run_step "$SIGNKEY" FREENET_RELEASE_SIGNING_KEY_PRESENT=true)" "rc=0"
    rc=$(run_step "$SIGNKEY" FREENET_RELEASE_SIGNING_KEY_PRESENT=false)
    check "signing-key check: missing refuses with an error" \
        "rc=$rc $(grep -c '^::error' "$SIGNKEY/out")" "rc=1 1"
fi
check "signing-key check: gated on a live run" \
    "$(step_if "$RELEASE_YML" validate "Verify the release signing key before anything is created")" "$LIVE_GATE"

NOTES="$TEST_TMP/notes"
if load_step "$RELEASE_YML" create_release "Generate release notes" "$NOTES"; then
    # The step writes /tmp/release_notes.md; point it into the case directory.
    sed -i "s|/tmp/release_notes.md|$NOTES/release_notes.md|g; s|@@needs.validate.outputs.version@@|9.9.9|g" "$NOTES/step.sh"
    rc=$(run_step "$NOTES" RELEASE_SHA=5555555555555555555555555555555555555555 LIST_OUT='v9.9.8\n' API_OUT="the notes")
    check "release notes: measured from the previous PUBLISHED release (--exclude-drafts)" \
        "rc=$rc $(grep -c '^release list .*--exclude-drafts' "$NOTES/calls") $(grep -c 'previous_tag_name=v9.9.8' "$NOTES/calls")" "rc=0 1 1"
    check "release notes: a failed listing fails the step" \
        "rc=$(run_step "$NOTES" RELEASE_SHA=5555555555555555555555555555555555555555 LIST_RC=1)" "rc=1"
fi
VALIDATE="$TEST_TMP/validate"
if load_step "$RELEASE_YML" validate "Resolve + validate version" "$VALIDATE"; then
    # The raw input reaches the script through env, never templated into it.
    check "Resolve + validate version: no \${{ inputs.* }} in the script" \
        "$(grep -c '@@inputs\.' "$VALIDATE/step.sh")" "0"
    # Auto-bump (blank input) bumps from crates.io alone. The dispatch tree is
    # not the ship commit, main lags a release until its merge-back, and a Gate
    # A block leaves no bump on main, so the tree's version must not move it.
    mkdir -p "$VALIDATE/work/crates/core" "$VALIDATE/work/crates/fdev"
    # shellcheck disable=SC2016  # $CURL_OUT is expanded by the stub, not here
    printf '#!/usr/bin/env bash\nprintf %%s "$CURL_OUT"\n' > "$STUBS/curl"
    chmod +x "$STUBS/curl"
    autobump_case() {
        local desc="$1" crates="$2" tree="$3" want="$4" rc
        printf '[package]\nname = "freenet"\nversion = "%s"\n' "$tree" > "$VALIDATE/work/crates/core/Cargo.toml"
        printf '[package]\nname = "fdev"\nversion = "0.3.10"\n' > "$VALIDATE/work/crates/fdev/Cargo.toml"
        rc=$(run_step "$VALIDATE" VERSION_INPUT= CURL_OUT="{\"crate\":{\"max_version\":\"$crates\"}}")
        check "auto-bump: $desc" "rc=$rc $(sed -n 's/^version=//p' "$VALIDATE/github_output")" "$want"
    }
    autobump_case "normal: tree equals crates.io" 0.2.163 0.2.163 "rc=0 0.2.164"
    autobump_case "main lagging a release (merge-back not landed): tree behind crates.io" 0.2.163 0.2.162 "rc=0 0.2.164"
    autobump_case "the tree's version is ignored even when ahead" 0.2.163 0.2.170 "rc=0 0.2.164"
    autobump_case "numeric patch bump (0.2.99 -> 0.2.100)" 0.2.99 0.2.99 "rc=0 0.2.100"
    autobump_case "unreadable crates.io answer refuses" garbage 0.2.163 "rc=1 "
    rm -f "$STUBS/curl"
fi

# --- 6. docker-publish.yml: version tags and the floating tags -----------------
#
# The build pushes only the per-release tags. "Move the floating tags" then
# re-lists the published releases AFTER the push and retags the image as
# `latest` only for the highest published non-prerelease release, and as `X.Y`
# only for the highest within that minor (the same definition as
# cross-compile.yml's Publish release above), so re-publishing an older tag
# cannot move either backwards. A failed or empty listing moves nothing
# without failing the step; on a `release` event the tag counts as published
# even if the listing lags.
DOCK="$TEST_TMP/docker-version"
if load_step "$DOCKER_YML" publish "Derive version tags" "$DOCK"; then
    derive_case() {
        local desc="$1" tag="$2" pre="$3" want="$4" rc
        rc=$(run_step "$DOCK" RELEASE_TAG="$tag" RELEASE_PRERELEASE="$pre")
        check "Docker version tags: $desc" \
            "rc=$rc stable=$(sed -n 's/^is_stable=//p' "$DOCK/github_output") minor=$(sed -n 's/^minor=//p' "$DOCK/github_output")" \
            "$want"
    }
    derive_case "a stable tag" v0.2.150 false "rc=0 stable=true minor=0.2"
    derive_case "the prerelease flag on a tag without '-' is not stable" v0.2.150 true "rc=0 stable=false minor=0.2"
    derive_case "a dispatched prerelease (no event flag) is not stable" v0.3.0-rc1 "" "rc=0 stable=false minor=0.3"
    derive_case "a tag without the leading v is refused" 0.2.150 "" "rc=1 stable= minor="
fi

# docker stub: logs its argv, fails when DOCKER_RC is set.
cat > "$STUBS/docker" <<'EOF'
#!/usr/bin/env bash
printf 'docker %s\n' "$*" >> "$STUB_CALLS"
exit "${DOCKER_RC:-0}"
EOF
chmod +x "$STUBS/docker"

FLOAT="$TEST_TMP/docker-floating"
if load_step "$DOCKER_YML" publish "Move the floating tags" "$FLOAT"; then
    # float_case <desc> <tag> <event> <stable> <want "rc= latest= minor= retags=">  [env...]
    float_case() {
        local desc="$1" tag="$2" event="$3" stable="$4" want="$5" rc minor retags
        shift 5
        minor="${tag#v}"; minor="${minor%.*}"
        rc=$(run_step "$FLOAT" RELEASE_TAG="$tag" MINOR="$minor" IS_STABLE="$stable" EVENT_NAME="$event" \
            IMAGE=ghcr.io/freenet/freenet-core GITHUB_REPOSITORY=freenet/freenet-core "$@")
        retags=$(grep '^docker ' "$FLOAT/calls" | sed 's/^docker buildx imagetools create -t ghcr.io\/freenet\/freenet-core://; s/ ghcr.io\/freenet\/freenet-core:/<-/' | paste -sd',' -)
        check "Floating tags: $desc" \
            "rc=$rc latest=$(sed -n 's/^moved_latest=//p' "$FLOAT/github_output") minor=$(sed -n 's/^moved_minor=//p' "$FLOAT/github_output") retags=$retags" \
            "$want"
    }
    float_case "the highest published release moves latest and its minor" v0.2.150 release true \
        "rc=0 latest=true minor=true retags=0.2<-v0.2.150,latest<-v0.2.150" LIST_OUT='v0.2.150\nv0.2.149\n'
    float_case "re-publishing an older release moves neither" v0.2.140 workflow_dispatch true \
        "rc=0 latest=false minor=false retags=" LIST_OUT='v0.2.150\nv0.2.140\n'
    float_case "an older MINOR's highest patch moves its minor tag only" v0.1.99 workflow_dispatch true \
        "rc=0 latest=false minor=true retags=0.1<-v0.1.99" LIST_OUT='v0.2.150\nv0.1.99\nv0.1.98\n'
    float_case "an older patch in an older minor moves nothing" v0.1.98 workflow_dispatch true \
        "rc=0 latest=false minor=false retags=" LIST_OUT='v0.2.150\nv0.1.99\nv0.1.98\n'
    float_case "minor match is by prefix, not substring (v0.2. vs v0.20.)" v0.2.5 workflow_dispatch true \
        "rc=0 latest=false minor=true retags=0.2<-v0.2.5" LIST_OUT='v0.20.1\nv0.2.5\n'
    float_case "versions compare numerically (v0.2.100 over v0.2.99)" v0.2.100 release true \
        "rc=0 latest=true minor=true retags=0.2<-v0.2.100,latest<-v0.2.100" LIST_OUT='v0.2.99\nv0.2.100\n'
    float_case "a release event counts its own tag when the listing lags" v0.2.151 release true \
        "rc=0 latest=true minor=true retags=0.2<-v0.2.151,latest<-v0.2.151" LIST_OUT='v0.2.150\n'
    float_case "a dispatched tag absent from the list (e.g. a draft) moves nothing" v0.2.151 workflow_dispatch true \
        "rc=0 latest=false minor=false retags=" LIST_OUT='v0.2.150\n'
    float_case "a failed listing moves nothing and FAILS the step (stale latest is noticed)" v0.2.150 release true \
        "rc=1 latest=false minor=false retags=" LIST_RC=1
    check "Floating tags: a failed listing is an ::error::" "$(grep -c '^::error::Could not list published releases' "$FLOAT/out")" "1"
    float_case "an empty listing moves nothing and fails the step" v0.2.150 release true \
        "rc=1 latest=false minor=false retags=" LIST_OUT=''
    # A release not flagged as a prerelease but named like one must not count.
    float_case "an unflagged v0.3.0-beta does not pin latest" v0.2.150 workflow_dispatch true \
        "rc=0 latest=true minor=true retags=0.2<-v0.2.150,latest<-v0.2.150" \
        LIST_JSON='[{"tagName":"v0.3.0-beta","isPrerelease":false},{"tagName":"v0.2.150","isPrerelease":false}]'
    float_case "a listing with only non-vX.Y.Z tags fails the step" v0.2.150 workflow_dispatch true \
        "rc=1 latest=false minor=false retags=" LIST_OUT='v0.3.0-beta\nnightly\n'
    float_case "a prerelease moves nothing and lists nothing" v0.3.0-rc1 release false \
        "rc=0 latest=false minor=false retags=" LIST_OUT='v0.3.0-rc1\n'
    check "Floating tags: a prerelease makes no gh call" "$(grep -c '^release list' "$FLOAT/calls")" "0"
    float_case "a not-highest skip is a warning" v0.2.140 workflow_dispatch true \
        "rc=0 latest=false minor=false retags=" LIST_OUT='v0.2.150\nv0.2.140\n'
    check "Floating tags: the latest skip is a ::warning::" \
        "$(grep -c '^::warning::v0.2.140 is not the highest published release' "$FLOAT/out")" "1"
    # The step's own --jq runs here, so deleting its prerelease filter fails this.
    float_case "a newer PRERELEASE does not stop the highest stable moving latest" v0.2.150 workflow_dispatch true \
        "rc=0 latest=true minor=true retags=0.2<-v0.2.150,latest<-v0.2.150" \
        LIST_JSON='[{"tagName":"v0.3.0-rc1","isPrerelease":true},{"tagName":"v0.2.150","isPrerelease":false}]'
    check "Floating tags: the listing excludes drafts" \
        "$(grep -c '^release list .*--exclude-drafts' "$FLOAT/calls")" "1"
    float_case "a failed retag fails the step" v0.2.150 release true \
        "rc=1 latest= minor= retags=0.2<-v0.2.150" LIST_OUT='v0.2.150\n' DOCKER_RC=1
fi
rm -f "$STUBS/docker"

# The build must push only the per-release tags; the floating ones are the
# step above's job, decided after the push.
check "Docker metadata: the build pushes no floating tag" \
    "$(python3 -c '
import sys, yaml
steps = yaml.safe_load(open(sys.argv[1]))["jobs"]["publish"]["steps"]
meta = [s for s in steps if s.get("id") == "metadata"][0]["with"]["tags"]
names = [s.get("name", s.get("uses", "")) for s in steps]
print("latest" in meta, "minor" in meta, names.index("Move the floating tags") > max(i for i, n in enumerate(names) if n.startswith("docker/build-push-action")))
' "$DOCKER_YML")" "False False True"

echo
if [[ "$FAILURES" -ne 0 ]]; then
    echo "FAILED: $FAILURES check(s)" >&2
    exit 1
fi
echo "All release publish-step assertions passed."

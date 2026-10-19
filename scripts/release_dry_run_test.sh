#!/usr/bin/env bash
# Regression tests for #5936: release.yml's `dry_run=true` was not a dry run.
#
# The version-bump job had no dry-run guard, so a "dry run" pushed
# `release/vX.Y.Z`, opened the real `build: release X.Y.Z` PR and enabled
# auto-merge on it, landing the bump on main. Only the tag and release steps
# carried their own `if: ${{ !inputs.dry_run }}`.
#
# The fix resolves the input ONCE (scripts/resolve_release_mode.sh, exported as
# `needs.validate.outputs.live`), gates every mutating step on it, and makes
# each mutating script refuse on its own when not live.
# `.github/scripts/check_release_dry_run.py` enforces that shape and has its own
# `--self-test` over synthetic fixtures. This file tests what that cannot:
#
#   1. the resolver fails closed (only the literal `false` is live);
#   2. the linter fires on the REAL pre-fix job, and passes the repo today;
#   3. the obvious future regressions of the real file are caught: each is
#      applied to a copy of release.yml and the linter must go red;
#   4. the job/step topology of BOTH modes, by evaluating every `if:` for
#      live and for dry (`--simulate`), so a guard flipped the wrong way --
#      which would break a LIVE release -- fails here;
#   5. every mutating step of the real release.yml, EXECUTED with stubbed
#      git/gh and its real `env:`, refuses before calling anything when not
#      live, and when live reaches its command with no empty argument and the
#      right expression feeding each argument;
#   6. the real non-mutating steps of the dry path (bump, compose, report,
#      bundle, rehearsal checkout, verifier) run end to end in throwaway repos;
#   7. the linter and this test are wired into CI, and not disabled in place.
#
# Run manually with: bash scripts/release_dry_run_test.sh

set -euo pipefail

# The end-to-end section runs real git in throwaway repos. Keep the caller's
# git environment out of it: an exported GIT_DIR would aim it at a real repo,
# and global signing or hook settings would fail or prompt.
unset GIT_DIR GIT_WORK_TREE GIT_INDEX_FILE GIT_OBJECT_DIRECTORY
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LINTER="$REPO_ROOT/.github/scripts/check_release_dry_run.py"
RESOLVER="$REPO_ROOT/scripts/resolve_release_mode.sh"
RELEASE_YML="$REPO_ROOT/.github/workflows/release.yml"
CI_YML="$REPO_ROOT/.github/workflows/ci.yml"
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

# --- 1. the resolver ---------------------------------------------------------

resolve() {
    # resolve <value|__unset__> -> the `live=` value written, plus stdout
    local out="$TEST_TMP/mode.out" rc=0
    : > "$out"
    if [[ "$1" == "__unset__" ]]; then
        env -u DRY_RUN_INPUT GITHUB_OUTPUT="$out" bash "$RESOLVER" > "$TEST_TMP/mode.log" 2>&1 || rc=$?
    else
        DRY_RUN_INPUT="$1" GITHUB_OUTPUT="$out" bash "$RESOLVER" > "$TEST_TMP/mode.log" 2>&1 || rc=$?
    fi
    [[ "$rc" -eq 0 ]] || { echo "resolver-exit-$rc"; return; }
    grep -E '^live=' "$out" | paste -sd, -
}

check "resolver: dry_run=false is a live run" "$(resolve false)" "live=true"
check "resolver: dry_run=true is a dry run" "$(resolve true)" "live=false"
check "resolver: an empty input is a dry run (fail closed)" "$(resolve '')" "live=false"
check "resolver: an unset input is a dry run (fail closed)" "$(resolve __unset__)" "live=false"
check "resolver: 'False' is not 'false', so a dry run" "$(resolve False)" "live=false"
resolve '' > /dev/null
check "resolver: an unexpected value is reported, not silently accepted" \
    "$(grep -c 'Treating this run as a dry run' "$TEST_TMP/mode.log")" "1"

# --- 2. the linter against real files ----------------------------------------

SELF_RC=0
python3 "$LINTER" --self-test > /dev/null 2>&1 || SELF_RC=$?
check "linter self-test passes" "$SELF_RC" "0"

REPO_RC=0
python3 "$LINTER" "$REPO_ROOT/.github/workflows" > "$TEST_TMP/repo.out" 2>&1 || REPO_RC=$?
check "linter passes the repo's workflows" "$REPO_RC" "0"
[[ "$REPO_RC" -eq 0 ]] || cat "$TEST_TMP/repo.out" >&2

# The pre-fix shape, abbreviated from origin/main 9c09d2dbf (the three
# mutating steps of the version-bump job are as they were; the rest is cut to
# the minimum): guarded tag steps and the UNGUARDED version-bump job. If the linter does not flag this,
# it would not have caught the bug it exists for.
mkdir -p "$TEST_TMP/prefix"
cat > "$TEST_TMP/prefix/release.yml" <<'YAML'
name: Release
on:
  workflow_dispatch:
    inputs:
      dry_run:
        description: 'Dry run - show what would be done without publishing'
        required: false
        type: boolean
        default: false
jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout code
        uses: actions/checkout@v7
  update_versions:
    name: Update versions and create PR
    runs-on: ubuntu-latest
    needs: validate
    steps:
      - name: Checkout code
        uses: actions/checkout@v7
        with:
          token: ${{ secrets.RELEASE_PAT || secrets.GITHUB_TOKEN }}

      - name: Create release branch
        run: |
          BRANCH="release/v${{ needs.validate.outputs.version }}"
          git checkout -b "$BRANCH"
          git add -A
          git commit -m "chore: bump versions"
          git push origin "$BRANCH"
          echo "RELEASE_BRANCH=$BRANCH" >> $GITHUB_ENV

      - name: Create Pull Request
        id: create_pr
        env:
          GH_TOKEN: ${{ secrets.RELEASE_PAT || secrets.GITHUB_TOKEN }}
        run: |
          PR_URL=$(gh pr create \
            --title "build: release ${{ needs.validate.outputs.version }}" \
            --base main \
            --head "release/v${{ needs.validate.outputs.version }}")

      - name: Enable auto-merge
        env:
          GH_TOKEN: ${{ secrets.RELEASE_PAT || secrets.GITHUB_TOKEN }}
        run: |
          gh pr merge ${{ steps.create_pr.outputs.pr_number }} --auto
  create_release:
    runs-on: ubuntu-latest
    needs: [validate]
    if: always() && (needs.verify_publishable.result == 'success' || inputs.dry_run)
    steps:
      - name: Create and push tag
        if: ${{ !inputs.dry_run }}
        run: |
          git push origin "v$VERSION"
YAML
PREFIX_RC=0
python3 "$LINTER" "$TEST_TMP/prefix" > "$TEST_TMP/prefix.out" 2>&1 || PREFIX_RC=$?
check "linter fails the pre-fix release.yml" "$PREFIX_RC" "1"
for step in "Create release branch" "Create Pull Request" "Enable auto-merge"; do
    check "pre-fix: names the unguarded step '$step'" \
        "$(grep -cF "step \`$step\` mutates" "$TEST_TMP/prefix.out" | tr -d ' ' | sed 's/^[1-9][0-9]*$/flagged/')" "flagged"
done
check "pre-fix: names the per-step inputs.dry_run reads" \
    "$(grep -c 'read outside the mode step' "$TEST_TMP/prefix.out" | sed 's/^[1-9][0-9]*$/flagged/')" "flagged"

# --- 3. likely future regressions of the REAL file ---------------------------
#
# Each mutation is a small python edit of a copy of release.yml. A mutation
# that fails to apply is a FAILURE, not a pass: an unapplied mutation proves
# nothing about the linter (it is how earlier pins in this repo went vacuous).

mutate_case() {
    # mutate_case <description> <expected diagnostic substring> <python-expression-over-s>
    local desc="$1" needle="$2" expr="$3" dir="$TEST_TMP/mut" rc=0
    rm -rf "$dir" && mkdir -p "$dir"
    if ! python3 - "$RELEASE_YML" "$dir/release.yml" "$expr" <<'PY'
import sys
src, dst, expr = sys.argv[1], sys.argv[2], sys.argv[3]
s = open(src).read()
new = eval(expr, {"s": s})
if new == s:
    sys.exit(3)
open(dst, "w").write(new)
PY
    then
        echo "FAIL - mutation did not apply: $desc" >&2
        FAILURES=$((FAILURES + 1))
        return
    fi
    python3 "$LINTER" "$dir" > "$dir/out" 2>&1 || rc=$?
    # The specific diagnostic, not just a non-zero exit: a mutation that also
    # trips an unrelated rule must not keep this green after the intended
    # rule breaks.
    check "linter catches: $desc" \
        "$rc:$(grep -cF -- "$needle" "$dir/out" | sed 's/^[1-9][0-9]*$/named/')" "1:named"
}

mutate_case "the push step loses its if: guard" "step \`Push the release branch\` mutates" \
    "s.replace(\"      - name: Push the release branch\n        if: needs.validate.outputs.live == 'true'\n\", \"      - name: Push the release branch\n\", 1)"
mutate_case "the push step's guard is ORed away" "step \`Push the release branch\` mutates" \
    "s.replace(\"      - name: Push the release branch\n        if: needs.validate.outputs.live == 'true'\n\", \"      - name: Push the release branch\n        if: always() || needs.validate.outputs.live == 'true'\n\", 1)"
mutate_case "the auto-merge step loses its in-script refusal" "does not refuse in its own script" \
    "s.replace('          [ \"\${RELEASE_LIVE:-}\" = \"true\" ] || { echo \"::error title=Refusing in a dry run::Not enabling auto-merge: this run is not live (#5936).\"; exit 1; }\n', '', 1)"
mutate_case "create_release loses its job-level live guard and the tag step its own" "step \`Create and push tag\` mutates" \
    "s.replace(\" && needs.validate.outputs.live == 'true'\n    env:\n      RELEASE_LIVE\", \"\n    env:\n      RELEASE_LIVE\", 1).replace(\"      - name: Create and push tag\n        if: needs.validate.outputs.live == 'true'\n\", \"      - name: Create and push tag\n\", 1)"
mutate_case "a step goes back to reading inputs.dry_run" "read outside the mode step" \
    "s.replace(\"      - name: Create GitHub release\n        if: needs.validate.outputs.live == 'true'\", \"      - name: Create GitHub release\n        if: \${{ !inputs.dry_run }}\", 1)"
mutate_case "a new unguarded step edits a release in a dry-run job" "step \`Sneaky\` mutates" \
    "s.replace('      - name: Compose the release PR\n', '      - name: Sneaky\n        run: gh release edit v1 --draft=false\n\n      - name: Compose the release PR\n', 1)"
mutate_case "a new unguarded step hides a merge in bash -c" "step \`Sneaky\` mutates" \
    "s.replace('      - name: Compose the release PR\n', '      - name: Sneaky\n        run: bash -c \"gh pr merge 1 --auto\"\n\n      - name: Compose the release PR\n', 1)"
mutate_case "update_versions persists its push credential in a dry run" "persist-credentials" \
    "s.replace(\"          persist-credentials: \${{ needs.validate.outputs.live == 'true' }}\n\", '', 1)"
mutate_case "the mode step stops using the tested resolver" "resolve_release_mode.sh" \
    "s.replace('run: bash scripts/resolve_release_mode.sh', 'run: echo live=true >> \"\$GITHUB_OUTPUT\"', 1)"
mutate_case "the dry_run input is renamed, taking release.yml out of scope" "nothing was checked" \
    "s.replace('      dry_run:\n', '      rehearse:\n', 1)"
mutate_case "the rehearsal job gets a write token" "read-only \`permissions:\`" \
    "s.replace('    permissions:\n      contents: read\n    env:\n      RELEASE_SHA:', '    permissions:\n      contents: write\n    env:\n      RELEASE_SHA:', 1)"
# Live-path plumbing: each of these left a live release broken while every
# dry-run check stayed green.
mutate_case "Create Pull Request stops writing pr_number" "never writes \`pr_number=\`" \
    "s.replace('          echo \"pr_number=\$PR_NUMBER\" >> \"\$GITHUB_OUTPUT\"\n', '', 1)"
mutate_case "update_versions stops exporting pr_number" "does not declare" \
    "s.replace('      pr_number: \${{ steps.create_pr.outputs.pr_number }}\n', '', 1)"

# --- 4. topology of both modes -------------------------------------------------

simulate() { python3 "$LINTER" --simulate "$RELEASE_YML" "$@"; }
LIVE_RUN=$(simulate live)
DRY_RUN=$(simulate dry)
check "live: exactly these jobs run" \
    "$(grep -v ' / ' <<< "$LIVE_RUN" | paste -sd' ' -)" \
    "validate update_versions wait_for_pr verify_publishable create_release summary"
check "dry: exactly these jobs run" \
    "$(grep -v ' / ' <<< "$DRY_RUN" | paste -sd' ' -)" \
    "validate update_versions dry_run_rehearsal summary"
for step in "update_versions / Push the release branch" "update_versions / Create Pull Request" \
            "update_versions / Enable auto-merge" "create_release / Create and push tag" \
            "create_release / Create GitHub release" \
            "validate / Verify the crates.io token before anything is created"; do
    check "live runs '$step'" "$(grep -cxF "$step" <<< "$LIVE_RUN")" "1"
    check "dry does not run '$step'" "$(grep -cxF "$step" <<< "$DRY_RUN")" "0"
done
for step in "update_versions / Dry run: report what a live run would push" \
            "update_versions / Dry run: hand the bump commit to the rehearsal job"; do
    check "dry runs '$step'" "$(grep -cxF "$step" <<< "$DRY_RUN")" "1"
    check "live does not run '$step'" "$(grep -cxF "$step" <<< "$LIVE_RUN")" "0"
done
# ORDER, not just membership, for every job in both modes: the PR is composed
# before it is opened and the branch pushed before the PR names it; the
# checkout verifier runs before the tag push and before each packaging check;
# the bundle is fetched before it is checked out. A reordering that changes
# behaviour must change this list deliberately.
seq() { grep "^$2 / " <<< "$1" | sed "s|^$2 / ||" | paste -sd'|' -; }
check "live: validate step order" "$(seq "$LIVE_RUN" validate)" \
    "Checkout code|Resolve run mode (live or dry run)|Warn if RELEASE_PAT is not configured|Verify the crates.io token before anything is created|Resolve + validate version"
check "live: update_versions step order" "$(seq "$LIVE_RUN" update_versions)" \
    "Checkout code|Configure git|Update freenet version|Update fdev version|Install Rust|Update Cargo.lock|Commit the version bump locally|Compose the release PR|Push the release branch|Create Pull Request|Enable auto-merge"
check "live: wait_for_pr step order" "$(seq "$LIVE_RUN" wait_for_pr)" \
    "Checkout code|Wait for PR to be merged|Resolve the release commit"
check "live: verify_publishable step order" "$(seq "$LIVE_RUN" verify_publishable)" \
    "Checkout the validated release commit|Verify the checkout is the validated release commit|Install Rust|Verify freenet packages cleanly (dry run)"
check "live: create_release step order" "$(seq "$LIVE_RUN" create_release)" \
    "Checkout the validated release commit|Verify the checkout is the validated release commit|Generate release notes|Create and push tag|Create GitHub release"
check "dry: update_versions step order" "$(seq "$DRY_RUN" update_versions)" \
    "Checkout code|Configure git|Update freenet version|Update fdev version|Install Rust|Update Cargo.lock|Commit the version bump locally|Compose the release PR|Dry run: report what a live run would push|Dry run: hand the bump commit to the rehearsal job"
check "dry: dry_run_rehearsal step order" "$(seq "$DRY_RUN" dry_run_rehearsal)" \
    "Checkout the launch commit|Fetch the bump bundle|Check out the unpushed version-bump commit|Verify the checkout is the bump commit|Install Rust|Verify freenet packages cleanly (dry run)|Changelog since the previous release"
# Failure paths: nothing irreversible may follow a failed gate.
check "live: a failed wait_for_pr stops publishing and tagging" \
    "$(simulate live --fail wait_for_pr | grep -cE '^(verify_publishable|create_release)$')" "0"
check "live: a failed packaging check stops tagging" \
    "$(simulate live --fail verify_publishable | grep -cxF create_release)" "0"
check "live: a failed version bump stops everything after it" \
    "$(simulate live --fail update_versions | grep -v ' / ' | paste -sd' ' -)" "validate update_versions summary"
check "live: a failed validate runs nothing but the summary" \
    "$(simulate live --fail validate | grep -v ' / ' | paste -sd' ' -)" "validate summary"
check "dry: a failed validate runs nothing but the summary" \
    "$(simulate dry --fail validate | grep -v ' / ' | paste -sd' ' -)" "validate summary"

# --- 5. execute every mutating step ------------------------------------------
#
# Stubs record every call. In a dry run each step must exit non-zero with the
# refusal and record NOTHING. Live, each must reach a stubbed command -- the
# control that shows the extraction is the real script, so the dry-run half
# is not passing because the script was empty or broke early -- and with the
# step's real `env:` exported, no argument may be empty: a renamed env key
# would otherwise push `git push origin ""` on a live release.

STUBS="$TEST_TMP/stubs"
mkdir -p "$STUBS"
for tool in git gh cargo curl docker npm wget ssh scp; do
    cat > "$STUBS/$tool" <<'SH'
#!/usr/bin/env bash
echo "$(basename "$0") $*" >> "$STUB_CALLS"
for a in "$@"; do [ -n "$a" ] || echo "EMPTYARG" >> "$STUB_CALLS"; done
if [[ "$(basename "$0")" == gh && "$1 $2" == "pr create" ]]; then
    echo "https://github.com/example/repo/pull/123"
fi
exit 0
SH
    chmod +x "$STUBS/$tool"
done

RUNS="$TEST_TMP/runs"
LIVE_CALLS=""
python3 "$LINTER" --extract-mutating-runs "$RELEASE_YML" "$RUNS"
EXTRACTED=$(cat "$RUNS"/*.name 2>/dev/null || true)
for expected in \
    "update_versions / Push the release branch" \
    "update_versions / Create Pull Request" \
    "update_versions / Enable auto-merge" \
    "create_release / Create and push tag" \
    "create_release / Create GitHub release"; do
    check "extractor finds mutating step '$expected'" \
        "$(grep -cxF "$expected" <<< "$EXTRACTED")" "1"
done
# Exactly those five. The extractor shares the linter's detector, so a step it
# misses is neither guarded nor executed here; a new mutating step must be
# added to the list above deliberately.
check "extractor finds exactly the five known mutating steps" \
    "$(grep -c . <<< "$EXTRACTED")" "5"

run_step() {
    # run_step <script> <RELEASE_LIVE value|__unset__> -> "rc=<n> calls=<n>"
    local script="$1" live="$2" calls="$TEST_TMP/calls" out="$TEST_TMP/step.out" rc=0
    : > "$calls"
    local -a envs=()
    local line
    while IFS= read -r line; do envs+=("$line"); done < "${script%.sh}.env"
    envs+=(PATH="$STUBS:$PATH" STUB_CALLS="$calls" GITHUB_OUTPUT="$TEST_TMP/gh_output"
           GITHUB_ENV="$TEST_TMP/gh_env" RUNNER_TEMP="$TEST_TMP")
    if [[ "$live" == "__unset__" ]]; then
        env -u RELEASE_LIVE "${envs[@]}" bash -e "$script" > "$out" 2>&1 || rc=$?
    else
        env "${envs[@]}" RELEASE_LIVE="$live" bash -e "$script" > "$out" 2>&1 || rc=$?
    fi
    echo "rc=$rc calls=$(wc -l < "$calls" | tr -d ' ')"
}

for script in "$RUNS"/*.sh; do
    name=$(cat "${script%.sh}.name")
    for mode in false "" __unset__ TRUE; do
        result=$(run_step "$script" "$mode")
        check "'$name' refuses with RELEASE_LIVE=${mode:-<empty>} and calls nothing" \
            "$(sed -E 's/^rc=[1-9][0-9]* /rc=nonzero /' <<< "$result")" "rc=nonzero calls=0"
        check "'$name' names the dry run when it refuses (RELEASE_LIVE=${mode:-<empty>})" \
            "$(grep -c 'Refusing in a dry run' "$TEST_TMP/step.out")" "1"
    done
    result=$(run_step "$script" true)
    check "'$name' reaches its command when live (control)" \
        "$(sed -E 's/^rc=0 calls=[1-9][0-9]*$/reached/' <<< "$result")" "reached"
    check "'$name' passes no empty argument when live" \
        "$(grep -c EMPTYARG "$TEST_TMP/calls")" "0"
    LIVE_CALLS="$LIVE_CALLS$(cat "$TEST_TMP/calls")"$'\n'
done
# The commands a live release actually issues, and WHICH value feeds each:
# every `${{ expr }}` is extracted as `@@expr@@`, so tagging the fdev version
# or pushing a branch built from the wrong output fails here.
for call in "git push origin @@steps.bump.outputs.branch@@" \
            "gh pr create --title @@steps.pr_text.outputs.title@@ --body-file @@steps.pr_text.outputs.body_file@@ --base main --head @@steps.bump.outputs.branch@@" \
            "gh pr merge @@steps.create_pr.outputs.pr_number@@ --auto" \
            "git push origin v@@needs.validate.outputs.version@@" \
            "gh release create v@@needs.validate.outputs.version@@ --title v@@needs.validate.outputs.version@@ --notes-file @@steps.release_notes.outputs.notes_file@@ --draft"; do
    check "live issues '$call'" "$(grep -cxF "$call" <<< "$LIVE_CALLS")" "1"
done
for step in "Wait for PR to be merged" "Resolve the release commit"; do
    check "wait_for_pr / $step reads the bump PR's number" \
        "$(python3 "$LINTER" --dump-step "$RELEASE_YML" wait_for_pr "$step" | grep -cxF 'PR_NUMBER=@@needs.update_versions.outputs.pr_number@@')" "1"
done

# --- 6. run the real non-mutating steps end to end ---------------------------
#
# The live and dry paths both depend on steps nothing above executes: the bump
# commit and its branch name, the composed PR title (ci.yml's merge-queue
# release gate keys off its `build: release ` prefix), the dry-run report and
# its git bundle, and the rehearsal's checkout of that bundle. Run them, as
# extracted from release.yml, in throwaway local repos with no remote they
# could push to.

WF_VERSION="9.9.9"
WF_FDEV="9.9.8"
VALUES="$TEST_TMP/values"
printf '%s\n' "needs.validate.outputs.version=$WF_VERSION" "needs.validate.outputs.fdev_version=$WF_FDEV" \
    "needs.validate.outputs.live=false" "secrets.GITHUB_TOKEN=fake-read-only-token" > "$VALUES"

run_wf_step() {
    # run_wf_step <job> <step name> <step id or -> <dir>: runs that step from
    # release.yml with every `@@expr@@` replaced from $VALUES, and records its
    # outputs in $VALUES as steps.<id>.outputs.<key>.
    local job="$1" name="$2" id="$3" workdir="$4" out="$TEST_TMP/step_output" rc=0
    : > "$out"
    if ! python3 - "$LINTER" "$RELEASE_YML" "$job" "$name" "$VALUES" "$TEST_TMP/step.sh" "$TEST_TMP/step.env" \
        > "$TEST_TMP/step.log" 2>&1 <<'PY'
import subprocess, sys
linter, wf, job, name, values, script_out, env_out = sys.argv[1:]
dump = subprocess.run([sys.executable, linter, "--dump-step", wf, job, name], check=True,
                      capture_output=True, text=True).stdout
script, env = dump.split("\n@@ENV@@\n")
known = dict(l.split("=", 1) for l in open(values).read().splitlines() if l)
def sub(t):
    for k, v in known.items():
        t = t.replace("@@" + k + "@@", v)
    if "@@" in t:
        sys.exit(f"unresolved expression in {name}: {t[t.index('@@'):][:80]}")
    return t
open(script_out, "w").write(sub(script))
open(env_out, "w").write(sub(env))
PY
    then
        echo "extract-failed"
        return
    fi
    local -a envs=()
    local line
    while IFS= read -r line; do [[ -n "$line" ]] && envs+=("$line"); done < "$TEST_TMP/step.env"
    (
        builtin cd -- "$workdir" || exit 99
        env ${envs[@]+"${envs[@]}"} PATH="${STEP_PATH:-$PATH}" GITHUB_OUTPUT="$out" \
            GITHUB_STEP_SUMMARY="$TEST_TMP/summary" RUNNER_TEMP="$TEST_TMP/runner" \
            GITHUB_REPOSITORY=example/repo bash -e "$TEST_TMP/step.sh"
    ) > "$TEST_TMP/step.log" 2>&1 || rc=$?
    if [[ "$id" != "-" ]]; then
        while IFS= read -r line; do
            [[ "$line" == *=* ]] && echo "steps.$id.outputs.$line" >> "$VALUES"
        done < "$out"
    fi
    echo "$rc"
}

mkdir -p "$TEST_TMP/runner" "$TEST_TMP/origin/crates/core" "$TEST_TMP/origin/crates/fdev" "$TEST_TMP/origin/scripts"
cp "$REPO_ROOT/crates/core/Cargo.toml" "$TEST_TMP/origin/crates/core/"
cp "$REPO_ROOT/crates/fdev/Cargo.toml" "$TEST_TMP/origin/crates/fdev/"
cp "$REPO_ROOT/scripts/verify_release_checkout.sh" "$TEST_TMP/origin/scripts/"
git -C "$TEST_TMP/origin" init -q -b main
git -C "$TEST_TMP/origin" add -A
git -C "$TEST_TMP/origin" -c user.email=t@example.invalid -c user.name=t commit -q -m base
LAUNCH_SHA=$(git -C "$TEST_TMP/origin" rev-parse HEAD)
echo "github.sha=$LAUNCH_SHA" >> "$VALUES"
check "a depth-1 clone of the launch commit, as actions/checkout makes" \
    "$(git clone -q --depth 1 "file://$TEST_TMP/origin" "$TEST_TMP/bump" 2>&1 && echo cloned)" "cloned"

for spec in "Configure git|-" "Update freenet version|-" "Update fdev version|-" \
            "Commit the version bump locally|bump" "Compose the release PR|pr_text" \
            "Dry run: report what a live run would push|dry_run_report"; do
    rc=$(run_wf_step update_versions "${spec%|*}" "${spec#*|}" "$TEST_TMP/bump")
    check "dry path: update_versions / ${spec%|*} runs" "$rc" "0"
    [[ "$rc" == "0" ]] || cat "$TEST_TMP/step.log" >&2
done
value() { grep -m1 "^$1=" "$VALUES" | cut -d= -f2-; }
check "the bump branch is release/v<version>" "$(value steps.bump.outputs.branch)" "release/v$WF_VERSION"
check "the PR title keeps the 'build: release ' prefix ci.yml's release gate keys off" \
    "$(value steps.pr_text.outputs.title)" "build: release $WF_VERSION"
check "the PR body names both versions" \
    "$(grep -cE "freenet: → \*\*$WF_VERSION\*\*|fdev: → \*\*$WF_FDEV\*\*" "$(value steps.pr_text.outputs.body_file)")" "2"
check "the bump commit carries the new freenet version" \
    "$(git -C "$TEST_TMP/bump" show HEAD:crates/core/Cargo.toml | grep -m1 '^version = ')" "version = \"$WF_VERSION\""
check "the bump commit carries the new fdev version" \
    "$(git -C "$TEST_TMP/bump" show HEAD:crates/fdev/Cargo.toml | grep -m1 '^version = ')" "version = \"$WF_FDEV\""
check "fdev's freenet dependency follows the release version" \
    "$(git -C "$TEST_TMP/bump" show HEAD:crates/fdev/Cargo.toml | grep -cE "^freenet = \{ path = \"\.\./core\", version = \"$WF_VERSION\"")" "1"
check "the bundle carries only the bump commit (the launch commit is its prerequisite)" \
    "$(git -C "$TEST_TMP/bump" bundle verify "$TEST_TMP/runner/release-bump.bundle" 2>&1 | grep -c "$LAUNCH_SHA")" "1"
check "the dry-run report hands over the bump commit" \
    "$(value steps.dry_run_report.outputs.bump_sha)" "$(git -C "$TEST_TMP/bump" rev-parse HEAD)"
check "the dry-run report says nothing was pushed" "$(grep -c 'nothing was pushed' "$TEST_TMP/summary")" "1"

# The rehearsal, in a separate depth-1 clone of the launch commit, as the job sees it.
echo "needs.update_versions.outputs.dry_run_bump_sha=$(value steps.dry_run_report.outputs.bump_sha)" >> "$VALUES"
check "a second depth-1 clone for the rehearsal job" \
    "$(git clone -q --depth 1 "file://$TEST_TMP/origin" "$TEST_TMP/rehearse" 2>&1 && echo cloned)" "cloned"
# The artifact hand-off, as declared in release.yml: the upload and download
# must name the same artifact, and the file the upload takes must land where
# the checkout step fetches it. Emulated from the declared paths, not placed
# by hand, so a renamed name or path fails here instead of on every dry run.
SEAM=$(python3 - "$LINTER" "$RELEASE_YML" "$TEST_TMP/runner" <<'PY'
import importlib.util, os, sys
spec = importlib.util.spec_from_file_location("lint", sys.argv[1]); m = importlib.util.module_from_spec(spec)
sys.modules["lint"] = m
spec.loader.exec_module(m)
text, runner = open(sys.argv[2]).read(), sys.argv[3]
_, up = m.find_step(text, "update_versions", "Dry run: hand the bump commit to the rehearsal job")
_, down = m.find_step(text, "dry_run_rehearsal", "Fetch the bump bundle")
t = lambda v: v.replace("${{ runner.temp }}", runner)
src, dest = t(up.with_["path"]), t(down.with_["path"])
print(up.with_.get("name") == down.with_.get("name"), src, os.path.join(dest, os.path.basename(src)))
PY
)
read -r SEAM_NAMES SEAM_SRC SEAM_DEST <<< "$SEAM"
check "upload and download name the same artifact" "$SEAM_NAMES" "True"
check "the dry-run report wrote the file the upload takes" "$([ -f "$SEAM_SRC" ] && echo present || echo missing)" "present"
mkdir -p "$(dirname "$SEAM_DEST")"
cp "$SEAM_SRC" "$SEAM_DEST" 2>/dev/null || true
for step in "Check out the unpushed version-bump commit" "Verify the checkout is the bump commit"; do
    rc=$(run_wf_step dry_run_rehearsal "$step" - "$TEST_TMP/rehearse")
    check "rehearsal: $step" "$rc" "0"
    [[ "$rc" == "0" ]] || cat "$TEST_TMP/step.log" >&2
done

# The changelog preview, against a `gh` that answers like GitHub. A failed
# `gh release list` must fail the step, not read as "no previous release".
GH_FAKE="$TEST_TMP/ghfake"
mkdir -p "$GH_FAKE"
cat > "$GH_FAKE/gh" <<'SH'
#!/usr/bin/env bash
echo "gh $*" >> "$GH_FAKE_LOG"
case "$1 $2" in
  "release list") [[ "${GH_FAKE_FAIL:-}" == 1 ]] && exit 1; echo "v9.9.8" ;;
  "api repos/example/repo/compare/"*)
    if [[ "$*" == *total_commits* ]]; then echo 2; else printf -- '- aaaaaaaaa one\n- bbbbbbbbb two\n'; fi ;;
esac
SH
chmod +x "$GH_FAKE/gh"
: > "$TEST_TMP/summary"
rc=$(GH_FAKE_LOG="$TEST_TMP/ghfake.log" STEP_PATH="$GH_FAKE:$PATH" \
    run_wf_step dry_run_rehearsal "Changelog since the previous release" - "$TEST_TMP/rehearse")
check "rehearsal: the changelog preview runs" "$rc" "0"
check "the changelog compares the previous release with the launch commit" \
    "$(grep -c "compare/v9.9.8...$LAUNCH_SHA" "$TEST_TMP/ghfake.log")" "2"
check "the changelog skips draft releases" "$(grep -c -- '--exclude-drafts' "$TEST_TMP/ghfake.log")" "1"
check "the changelog preview lists the commits" "$(grep -c '^- [ab]\{9\} ' "$TEST_TMP/summary")" "2"
rc=$(GH_FAKE_FAIL=1 GH_FAKE_LOG="$TEST_TMP/ghfake.log" STEP_PATH="$GH_FAKE:$PATH" \
    run_wf_step dry_run_rehearsal "Changelog since the previous release" - "$TEST_TMP/rehearse")
check "a failed release listing fails the changelog step" "$([ "$rc" != 0 ] && echo failed || echo passed)" "failed"

# `set -o pipefail` in the report: a failed `git diff` must fail the step, not
# hide behind `tee`. Only `git diff` fails here (a wrapper delegates every
# other git command), in a copy of the real bump repo, so nothing else in the
# step can be what fails it.
GIT_FAKE="$TEST_TMP/gitfake"
mkdir -p "$GIT_FAKE"
REAL_GIT=$(command -v git)
# The wrapper's $1/$@ are meant literally: they are expanded when it runs.
# shellcheck disable=SC2016
printf '#!/usr/bin/env bash\n[ "$1" = diff ] && exit 1\nexec "%s" "$@"\n' "$REAL_GIT" > "$GIT_FAKE/git"
chmod +x "$GIT_FAKE/git"
cp -R "$TEST_TMP/bump" "$TEST_TMP/bump-diff-fails"
rc=$(STEP_PATH="$GIT_FAKE:$PATH" run_wf_step update_versions "Dry run: report what a live run would push" - "$TEST_TMP/bump-diff-fails")
check "the dry-run report fails when its diff fails" "$([ "$rc" != 0 ] && echo failed || echo passed)" "failed"

# The summary, which used to say "cut and tagged" for every outcome.
summary_case() {
    # summary_case <validate result> <live> <create_release result> <expected substring>
    local base="$TEST_TMP/values.base"
    [[ -f "$base" ]] || cp "$VALUES" "$base"
    { grep -v '^needs\.validate\.outputs\.live=' "$base"
      echo "needs.validate.result=$1"; echo "needs.validate.outputs.live=$2"
      echo "needs.create_release.result=$3"; echo "needs.dry_run_rehearsal.result=success"
      echo "github.repository=example/repo"; } > "$VALUES"
    run_wf_step summary "Release summary" - "$TEST_TMP/rehearse" > /dev/null
    check "summary for validate=$1 live=$2 create_release=$3" \
        "$(grep -cF -- "$4" "$TEST_TMP/step.log")" "1"
    cp "$base" "$VALUES"
}
summary_case failure "" skipped "validate did not succeed"
summary_case success false skipped "DRY RUN for $WF_VERSION"
summary_case success true failure "did NOT complete"
summary_case success true success "is cut and tagged"

# --- 7. wiring ----------------------------------------------------------------
#
# A guard nobody runs is not a guard. Executable lines only, so a commented-out
# step does not count.

check "Rule Lint runs the linter's self-test" \
    "$(grep -cE '^[[:space:]]*run: python3 \.github/scripts/check_release_dry_run\.py --self-test[[:space:]]*$' "$CI_YML")" "1"
check "Rule Lint runs the linter over .github/workflows" \
    "$(grep -cE '^[[:space:]]*run: python3 \.github/scripts/check_release_dry_run\.py \.github/workflows[[:space:]]*$' "$CI_YML")" "1"
check "CI runs this test" \
    "$(grep -cE '^[[:space:]]*run: bash scripts/release_dry_run_test\.sh[[:space:]]*$' "$CI_YML")" "1"
# ...and not disabled in place: no `if:` or `continue-on-error` on those steps
# or their jobs, and Rule Lint still runs on pull requests.
check "the CI steps running this guard are not disabled in place" \
    "$(python3 - "$LINTER" "$CI_YML" <<'PY'
import importlib.util, sys
spec = importlib.util.spec_from_file_location("lint", sys.argv[1])
m = importlib.util.module_from_spec(spec)
sys.modules["lint"] = m  # dataclasses resolve annotations through sys.modules
spec.loader.exec_module(m)
jobs, _ = m.parse_workflow(open(sys.argv[2]).read())
bad, seen = [], 0
for j in jobs:
    for st in j.steps:
        if st.run and ("check_release_dry_run.py" in st.run or "release_dry_run_test.sh" in st.run):
            seen += 1
            if "if" in st.keys or "continue-on-error" in st.keys:
                bad.append(st.label)
            if "continue-on-error" in j.keys:
                bad.append(j.name)
            # Any job-level `if:` can switch the guard off for PRs; Rule
            # Lint's own (pull requests only) is the one expected value.
            expected_if = "github.event_name == 'pull_request'" if j.name == "rule_lint" else ""
            if j.keys.get("if", "").strip() != expected_if:
                bad.append(f"{j.name} if:")
print(("disabled: " + ",".join(bad)) if bad else f"enabled x{seen}")
PY
)" "enabled x4"  # Rule Lint self-test + lint, the Fmt-job test, shellcheck
check "release.yml runs the tested resolver" \
    "$(grep -cE '^[[:space:]]*run: bash scripts/resolve_release_mode\.sh[[:space:]]*$' "$RELEASE_YML")" "1"

if [[ "$FAILURES" -ne 0 ]]; then
    echo "FAILED: $FAILURES check(s)" >&2
    exit 1
fi
echo "PASS: release.yml's dry_run is side-effect free and guarded against regressions."

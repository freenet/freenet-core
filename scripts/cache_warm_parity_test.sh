#!/usr/bin/env bash
# Regression tests for the Rust cache-warm parity guard and its release-script
# companion (PR #6105).
#
# ci-cache-warm.yml writes the main-scoped Rust caches that ci.yml's heavy PR
# jobs restore. It helps only while it produces the same keys and compiles the
# same things, and drift fails silently: PR jobs stay green and quietly build
# cold. `.github/scripts/check_cache_warm_parity.py --self-test` covers the
# linter on synthetic fixtures. This file covers what that cannot:
#   - the repo satisfies the invariant today;
#   - the linter fires on the REAL defect it found when it was written:
#     Simulation's run step lacked the build step's linker env, so cargo
#     recompiled before testing (3-4 min of a standard-16 on every PR run);
#   - the linter is still wired into Rule Lint against the real files;
#   - scripts/release.sh's pre-flight ignores "Warm cache:" check runs and
#     still counts real ones, using the jq programs extracted from the script
#     itself rather than copies, and reads more than one default page.
#
# Run manually with: bash scripts/cache_warm_parity_test.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
LINTER="$REPO_ROOT/.github/scripts/check_cache_warm_parity.py"
CI_YML="$REPO_ROOT/.github/workflows/ci.yml"
WARM_YML="$REPO_ROOT/.github/workflows/ci-cache-warm.yml"
RELEASE_SH="$REPO_ROOT/scripts/release.sh"
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

# --- the linter --------------------------------------------------------------

RC=0
python3 "$LINTER" --self-test >/dev/null 2>&1 || RC=$?
check "linter self-test passes" "$RC" "0"

RC=0
python3 "$LINTER" "$CI_YML" "$WARM_YML" >/dev/null 2>&1 || RC=$?
check "the repo's ci.yml and ci-cache-warm.yml are in parity" "$RC" "0"

# Re-create the pre-fix Simulation run step: drop the two linker lines that
# follow its RUST_LOG override. The edit must actually apply, or this check
# would pass vacuously.
python3 - "$CI_YML" "$TEST_TMP/ci.yml" <<'PY'
import re, sys
src, dst = sys.argv[1], sys.argv[2]
s = open(src).read()
pat = re.compile(
    r"(          RUST_LOG: info,turmoil=warn\n(?:          #.*\n)*)"
    r"          CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER: clang\n"
    r"          CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS: -C link-arg=-fuse-ld=mold\n")
out, n = pat.subn(r"\1", s, count=1)
open(dst, "w").write(out)

PY
APPLIED=$(python3 - "$CI_YML" "$TEST_TMP/ci.yml" <<'PY'
import sys
print(int(open(sys.argv[1]).read() != open(sys.argv[2]).read()))
PY
)
check "pre-fix Simulation run step re-created" "$APPLIED" "1"
RC=0
OUT=$(python3 "$LINTER" "$TEST_TMP/ci.yml" "$WARM_YML" 2>&1) || RC=$?
check "flags the run step that recompiled Simulation" "$RC" "1"
check "names test_simulation's compile commands" \
    "$(grep -cF "job \`test_simulation\`: compile commands" <<<"$OUT")" "1"

# Wiring: Rule Lint runs both the self-test and the real-file check, as live
# steps that can fail the job. Parsed with the linter's own YAML helpers rather
# than grepped, so a commented-out step, an `if:` or `continue-on-error` on it,
# or the command moving to another job all fail here.
WIRING=$(python3 - "$LINTER" "$CI_YML" <<'PY'
import importlib.util, sys
spec = importlib.util.spec_from_file_location("parity", sys.argv[1])
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)
job = m.extract_job(open(sys.argv[2]).read(), "rule_lint") or []
want = {
    "python3 .github/scripts/check_cache_warm_parity.py --self-test": "self-test",
    "python3 .github/scripts/check_cache_warm_parity.py .github/workflows/ci.yml "
    ".github/workflows/ci-cache-warm.yml": "real-files",
}
live = set()
for step in m._steps(job):
    run, _ = m._step_key(step, "run")
    gated = any(m._step_key(step, k)[0] is not None for k in ("if", "continue-on-error"))
    if run in want and not gated:
        live.add(want[run])
print(" ".join(sorted(live)))
PY
)
check "Rule Lint runs the self-test and the real-file check as live steps" \
    "$WIRING" "real-files self-test"

# --- the release pre-flight --------------------------------------------------

# Extract the live jq programs, so a later edit to the filters is what gets
# tested.
extract_jq() {
    # extract_jq <variable name>
    grep -E "^[[:space:]]*$1=\\$\\(echo \"\\\$check_runs_json\" \\| jq '" "$RELEASE_SH" \
        | sed -E "s/^.*\\| jq '(.*)'\\)\$/\\1/" || true
}
IN_PROGRESS_JQ=$(extract_jq in_progress_count)
FAILED_JQ=$(extract_jq failed_count)
check "found the in_progress_count filter" "$(grep -c 'Warm cache:' <<<"$IN_PROGRESS_JQ")" "1"
check "found the failed_count filter" "$(grep -c 'Warm cache:' <<<"$FAILED_JQ")" "1"

cat > "$TEST_TMP/runs.json" <<'JSON'
{"total_count": 5, "check_runs": [
  {"name": "Warm cache: Unit & Integration", "status": "in_progress", "conclusion": null},
  {"name": "Warm cache: Simulation", "status": "completed", "conclusion": "failure"},
  {"name": "Warm cache: notify on failure", "status": "queued", "conclusion": null},
  {"name": "Clippy", "status": "in_progress", "conclusion": null},
  {"name": "Unit & Integration", "status": "completed", "conclusion": "failure"}
]}
JSON
check "pre-flight ignores in-progress warm runs but not Clippy" \
    "$(jq "$IN_PROGRESS_JQ" "$TEST_TMP/runs.json")" "1"
check "pre-flight ignores a failed warm run but not a failed test job" \
    "$(jq "$FAILED_JQ" "$TEST_TMP/runs.json")" "1"
# The refusal message must apply the same exclusions as the count, or it can
# name only ignored checks. Compare the two select() clauses directly.
SELECTS=$(python3 - "$RELEASE_SH" <<'PY'
import re, sys
src = open(sys.argv[1]).read()
def sel(var):
    m = re.search(r"^\s*" + var + r"=\$\(echo \"\$check_runs_json\" \| jq (?:-r )?'\[\.check_runs\[\] \| (select\(.*?\))\]", src, re.M)
    return m.group(1) if m else "MISSING:" + var
print(int(sel("failed_count") == sel("failed_names")))
PY
)
check "refusal message uses the same filter as the failure count" "$SELECTS" "1"
check "pre-flight asks for 100 check runs per page" \
    "$(grep -cE '^[[:space:]]*check_runs_json=\$\(gh api "repos/freenet/freenet-core/commits/main/check-runs\?per_page=100"' "$RELEASE_SH" || true)" "1"

if [[ $FAILURES -gt 0 ]]; then
    echo "$FAILURES check(s) failed" >&2
    exit 1
fi
echo "all checks passed"

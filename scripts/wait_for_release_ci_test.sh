#!/usr/bin/env bash
# Tests for the release CI gate: scripts/wait_for_release_ci.sh (release.yml's
# `wait_for_ci` job) against a stubbed `gh`, and ci.yml's trigger and job
# conditions for a push to `release/v*`.
#
# The failure mode guarded here is a gate that silently passes: a ci.yml `if:`
# that skips the heavy jobs on the release push still gives a run that
# concludes `success`, and a waiter that reads only that conclusion would tag
# an untested commit. So this pins, from both ends:
#
#   1. ci.yml triggers on pushes to `release/v*`, and evaluating every job's
#      `if:` (and the Simulation/NAT `skip_check` steps) for such a push runs
#      exactly the jobs the waiter requires, while a push to main still runs
#      none of the heavy ones (cost);
#   2. the waiter fails on every non-success conclusion, on a success run whose
#      required jobs did not all pass, on a release commit that is not
#      ship_sha + version files, and when no run appears; retries transient gh
#      errors; and writes release_sha only on a full pass.
#
# Run manually with: bash scripts/wait_for_release_ci_test.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
WAITER="$SCRIPT_DIR/wait_for_release_ci.sh"
CI_YML="${CI_YML:-$REPO_ROOT/.github/workflows/ci.yml}"
RELEASE_YML="$REPO_ROOT/.github/workflows/release.yml"
LINTER="$REPO_ROOT/.github/scripts/check_release_dry_run.py"
command -v jq > /dev/null || { echo "FAIL - this test needs jq" >&2; exit 2; }
TEST_TMP=$(mktemp -d)
trap '[ -n "${KEEP_TMP:-}" ] || rm -rf "$TEST_TMP"' EXIT

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

# --- 1. ci.yml on a release push ---------------------------------------------
#
# Evaluates each job's `if:` with the Actions semantics the conditions use
# (`||`, `&&`, `!`, `==`, `!=`, `startsWith`, status functions) for a given
# event, ref and head_ref, and runs each `Detect skip` step's real script.
# Prints "<job name>\t<runs|skipped|steps-skipped>" per job.
evaluate_ci() {
    python3 - "$LINTER" "$CI_YML" "$@" <<'PY'
import importlib.util, os, re, subprocess, sys
spec = importlib.util.spec_from_file_location("lint", sys.argv[1]); m = importlib.util.module_from_spec(spec)
sys.modules["lint"] = m
spec.loader.exec_module(m)
text = open(sys.argv[2]).read()
event, ref, head_ref, merge_msg = sys.argv[3:7]
ctx = {
    "github.event_name": event, "github.ref": ref, "github.head_ref": head_ref,
    "github.event.merge_group.head_commit.message": merge_msg,
}

TOK = re.compile(r"\s*(\|\||&&|==|!=|!|\(|\)|,|'[^']*'|[A-Za-z_][\w.\-*]*)")
def evaluate(expr):
    toks, pos = [], 0
    while pos < len(expr):
        mt = TOK.match(expr, pos)
        if not mt:
            if expr[pos:].strip():
                raise SystemExit(f"cannot parse: {expr!r}")
            break
        toks.append(mt.group(1)); pos = mt.end()
    i = 0
    def peek():
        return toks[i] if i < len(toks) else None
    def take():
        nonlocal i
        i += 1
        return toks[i - 1]
    def truthy(v):
        return v != "" and v is not False and v is not None
    def or_():
        v = and_()
        while peek() == "||":
            take(); r = and_(); v = v if truthy(v) else r
        return v
    def and_():
        v = unary()
        while peek() == "&&":
            take(); r = unary(); v = r if truthy(v) else v
        return v
    def unary():
        if peek() == "!":
            take(); return not truthy(unary())
        return cmp()
    def cmp():
        v = atom()
        if peek() in ("==", "!="):
            op = take(); r = atom()
            eq = str(v).lower() == str(r).lower()
            return eq if op == "==" else not eq
        return v
    def atom():
        t = take()
        if t == "(":
            v = or_(); take(); return v
        if t.startswith("'"):
            return t[1:-1]
        if t in ("true", "false"):
            return t == "true"
        if peek() == "(":
            take(); args = []
            while peek() != ")":
                args.append(or_())
                if peek() == ",":
                    take()
            take()
            if t == "startsWith":
                return str(args[0]).lower().startswith(str(args[1]).lower())
            if t == "contains":
                return str(args[1]).lower() in str(args[0]).lower() if args[0] != "" else False
            if t in ("success", "always"):
                return True
            if t in ("failure", "cancelled"):
                return False
            raise SystemExit(f"unsupported function {t}")
        if t not in ctx and t.startswith("github."):
            return ""
        return ctx.get(t, "")
    v = or_()
    if peek() is not None:
        raise SystemExit(f"trailing tokens in {expr!r}")
    return truthy(v)

jobs, _ = m.parse_workflow(text)
for j in jobs:
    name = (j.keys.get("name") or j.name).strip("\"'")
    cond = m._unwrap(j.keys.get("if", ""))
    if cond and not evaluate(cond):
        print(f"{name}\tskipped"); continue
    verdict = "runs"
    for st in j.steps:
        if st.keys.get("id") == "skip_check":
            env = dict(os.environ)
            for k, v in st.env.items():
                v = re.sub(r"\$\{\{\s*([\w.]+)\s*\}\}", lambda mm: ctx.get(mm.group(1), ""), v)
                env[k] = v
            out = os.path.join(os.environ["TEST_TMP"], "skip_out")
            open(out, "w").close()
            env["GITHUB_OUTPUT"] = out
            subprocess.run(["bash", "-c", st.run], env=env, check=True, capture_output=True)
            if "skip=true" in open(out).read():
                verdict = "steps-skipped"
    print(f"{name}\t{verdict}")
PY
}
export TEST_TMP

RELEASE_PUSH=$(evaluate_ci push refs/heads/release/v0.2.200 "" "")
MAIN_PUSH=$(evaluate_ci push refs/heads/main "" "")
PR_RUN=$(evaluate_ci pull_request refs/pull/1/merge fix/x "")
MQ_RUN=$(evaluate_ci merge_group refs/heads/gh-readonly-queue/main/pr-1 "" "fix: x (#1)")

jobs_that_run() { awk -F'\t' '$2 == "runs" {print $1}' <<< "$1" | sort; }
REQUIRED=$("$WAITER" --print-required-jobs | sort)
check "the waiter requires at least the eight heavy and fast jobs" "$(grep -c . <<< "$REQUIRED")" "8"
check "a release push runs EXACTLY the jobs the waiter requires" \
    "$(jobs_that_run "$RELEASE_PUSH" | paste -sd'|' -)" "$(paste -sd'|' - <<< "$REQUIRED")"
for heavy in "Unit & Integration" "Simulation" "NAT Validation" "Windows Check" \
             "macOS Service Unit" "Windows Service Unit"; do
    check "release push runs '$heavy' with its steps" \
        "$(awk -F'\t' -v n="$heavy" '$1 == n {print $2}' <<< "$RELEASE_PUSH")" "runs"
    check "a push to main still skips '$heavy' (it was validated on its PR)" \
        "$(awk -F'\t' -v n="$heavy" '$1 == n {print $2}' <<< "$MAIN_PUSH")" "skipped"
    check "a pull request still runs '$heavy'" \
        "$(awk -F'\t' -v n="$heavy" '$1 == n {print $2}' <<< "$PR_RUN")" "runs"
done
# The non-release merge_group tiering (#3973) is unchanged.
check "non-release merge_group still skips Simulation's steps" \
    "$(awk -F'\t' '$1 == "Simulation" {print $2}' <<< "$MQ_RUN")" "steps-skipped"
check "non-release merge_group still runs Unit & Integration" \
    "$(awk -F'\t' '$1 == "Unit & Integration" {print $2}' <<< "$MQ_RUN")" "runs"
check "a push to main runs only Fmt and Clippy, as before" \
    "$(jobs_that_run "$MAIN_PUSH" | paste -sd'|' -)" "Clippy|Fmt"

# The trigger itself: without it there is no run at all and the waiter times
# out (fail closed, but after NO_RUN_TIMEOUT).
PUSH_BRANCHES=$(python3 - "$CI_YML" <<'PY'
import re, sys
s = open(sys.argv[1]).read()
on = s.split("\non:\n", 1)[1].split("\n\n", 1)[0]
push = on.split("  push:\n", 1)[1]
m = re.search(r"^    branches:\s*\[(.*)\]\s*$", push, re.M)
print(",".join(x.strip().strip("'\"") for x in m.group(1).split(",")) if m else "none")
PY
)
check "ci.yml runs on pushes to main and to release/v*" "$PUSH_BRANCHES" "main,release/v*"
# The paths-ignore on push is safe for the release branch only because the bump
# always touches a Cargo manifest, which no ignore pattern may match (GitHub
# glob: `*` stops at `/`, `**` does not). A run skipped by the filter would
# leave the waiter with nothing to wait for.
check "no push paths-ignore pattern matches a file the version bump changes" \
    "$(python3 - "$CI_YML" <<'PY2'
import re, sys
s = open(sys.argv[1]).read()
push = s.split("\non:\n", 1)[1].split("  pull_request:", 1)[0]
pats = re.findall(r"^      - '([^']+)'", push.split("paths-ignore:", 1)[1], re.M) if "paths-ignore:" in push else []
def rx(p):
    out, i = "", 0
    while i < len(p):
        if p.startswith("**", i): out += ".*"; i += 2
        elif p[i] == "*": out += "[^/]*"; i += 1
        else: out += re.escape(p[i]); i += 1
    return re.compile(out + "$")
files = ["Cargo.lock", "crates/core/Cargo.toml", "crates/fdev/Cargo.toml", "apps/freenet-ping/Cargo.lock"]
hits = [f"{p}~{f}" for p in pats for f in files if rx(p).match(f)]
print(f"{len(pats)} patterns, matches: {hits}")
PY2
)" "5 patterns, matches: []"

# release.yml wires the waiter.
check "wait_for_ci runs the tested waiter" \
    "$(python3 "$LINTER" --dump-step "$RELEASE_YML" wait_for_ci "Wait for the full CI suite on the release commit" | grep -cxF 'bash scripts/wait_for_release_ci.sh')" "1"

# The job must outlive the waiter's own budget with margin, or a slow suite is
# reported as a bare cancellation instead of the waiter's diagnosis.
JOB_TIMEOUT=$(awk '/^  wait_for_ci:/ {j = 1; next} j && /^  [a-z_]+:/ {exit} j && /^    timeout-minutes:/ {print $2}' "$RELEASE_YML")
# shellcheck disable=SC2016  # a sed pattern, not an expansion
WAITER_BUDGET=$(sed -n 's/^MAX_WAIT="\${MAX_WAIT:-\([0-9]*\)}"$/\1/p' "$WAITER")
check "wait_for_ci's job timeout exceeds the waiter's budget by at least 10 min" \
    "$([ -n "$JOB_TIMEOUT" ] && [ -n "$WAITER_BUDGET" ] && [ $((JOB_TIMEOUT * 60)) -ge $((WAITER_BUDGET + 600)) ] && echo ok || echo "job=${JOB_TIMEOUT}min waiter=${WAITER_BUDGET}s")" "ok"
# shellcheck disable=SC2016  # matched literally
check "the waiter measures wall time, not summed sleeps" "$(grep -cE '^[[:space:]]*ELAPSED=\$\(\(SECONDS - START\)\)' "$WAITER")" "1"

# --- 2. the waiter against a stubbed gh --------------------------------------

B=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
SHIP=5555555555555555555555555555555555555555
STUBS="$TEST_TMP/bin"
mkdir -p "$STUBS"
# Answers from files in $STUB_DIR and applies the script's own --jq filter with
# real jq, so a wrong field name fails here. A file containing FAIL makes that
# call fail; FAILn makes the first n calls fail.
cat > "$STUBS/gh" <<'EOF'
#!/usr/bin/env bash
echo "gh $*" >> "$STUB_DIR/calls"
jqarg=""
args=("$@")
for ((i = 0; i < ${#args[@]}; i++)); do
    [[ "${args[i]}" == --jq ]] && jqarg="${args[i+1]}"
done
case "$*" in
    "api repos/freenet/freenet-core/commits/"*) f=commit ;;
    "api repos/freenet/freenet-core/compare/"*) f=compare ;;
    "run list --repo freenet/freenet-core --workflow ci.yml --commit "*" --event push "*) f=runs ;;
    "run view "*" --repo freenet/freenet-core --json jobs "*) f=jobs ;;
    *) echo "unexpected gh $*" >&2; exit 99 ;;
esac
n=$(cat "$STUB_DIR/$f.n" 2>/dev/null || echo 0); n=$((n + 1)); echo "$n" > "$STUB_DIR/$f.n"
body=$(cat "$STUB_DIR/$f")
first="${body%%$'\n'*}"
if [[ "$first" == FAIL ]]; then exit 1; fi
if [[ "$first" =~ ^FAIL([0-9]+)$ ]]; then
    [ "$n" -le "${BASH_REMATCH[1]}" ] && exit 1
    body="${body#*$'\n'}"
fi
# A sequence of answers, one per call, separated by lines of ---; the last
# answer repeats.
if grep -qx -- '---' <<< "$body"; then
    body=$(awk -v n="$n" 'BEGIN{k=1} /^---$/{k++; next} {part[k]=part[k] $0 "\n"} END{if (n > k) n = k; printf "%s", part[n]}' <<< "$body")
fi
if [ -n "$jqarg" ]; then jq -rc "$jqarg" <<< "$body"; else printf '%s\n' "$body"; fi
EOF
chmod +x "$STUBS/gh"

ok_jobs() {
    "$WAITER" --print-required-jobs | jq -R '{name: ., conclusion: "success"}' | jq -sc '{jobs: .}'
}

run_waiter() {
    # run_waiter <desc> <expected rc> [file=content ...]: baseline is a clean pass.
    local desc="$1" want="$2"
    shift 2
    local dir="$TEST_TMP/case"
    rm -rf "$dir" && mkdir -p "$dir"
    echo "{\"sha\":\"$B\",\"parents\":[{\"sha\":\"$SHIP\"}]}" > "$dir/commit"
    echo '{"status":"ahead","ahead_by":1,"files":[{"filename":"Cargo.lock"},{"filename":"crates/core/Cargo.toml"},{"filename":"crates/fdev/Cargo.toml"},{"filename":"apps/freenet-ping/Cargo.lock"}]}' > "$dir/compare"
    echo '[{"databaseId":42,"status":"completed","conclusion":"success","url":"https://x/42","headBranch":"release/v0.2.200","createdAt":"2026-11-23T10:00:00Z"}]' > "$dir/runs"
    ok_jobs > "$dir/jobs"
    local kv
    for kv in "$@"; do printf '%s\n' "${kv#*=}" > "$dir/${kv%%=*}"; done
    : > "$dir/out"
    local rc=0
    STUB_DIR="$dir" PATH="$STUBS:$PATH" RELEASE_SHA="${CASE_B:-$B}" SHIP_SHA="$SHIP" \
        GITHUB_REPOSITORY=freenet/freenet-core GITHUB_OUTPUT="$dir/out" \
        RELEASE_BRANCH=release/v0.2.200 MAX_WAIT=5 NO_RUN_TIMEOUT=3 WAIT_INTERVAL=1 \
        bash "$WAITER" > "$dir/log" 2>&1 || rc=$?
    check "$desc" "$rc" "$want"
    if [ "$rc" -eq 0 ]; then
        check "$desc: writes release_sha=B" "$(cat "$dir/out")" "release_sha=$B"
    else
        check "$desc: writes no release_sha" "$(grep -c release_sha "$dir/out" || true)" "0"
    fi
}

run_waiter "a passing run with every required job passes" 0
for c in failure cancelled timed_out action_required startup_failure stale skipped neutral; do
    run_waiter "a run concluding '$c' fails" 1 \
        runs="[{\"databaseId\":42,\"status\":\"completed\",\"conclusion\":\"$c\",\"url\":\"u\",\"headBranch\":\"release/v0.2.200\",\"createdAt\":\"t\"}]"
done
run_waiter "a success run whose Simulation job was skipped fails" 1 \
    jobs="$(ok_jobs | jq -c '.jobs |= map(if .name == "Simulation" then .conclusion = "skipped" else . end)')"
run_waiter "a success run missing a required job fails" 1 \
    jobs="$(ok_jobs | jq -c '.jobs |= map(select(.name != "NAT Validation"))')"
run_waiter "the newest run decides (an older failure is superseded)" 0 \
    runs='[{"databaseId":41,"status":"completed","conclusion":"failure","url":"u","headBranch":"release/v0.2.200","createdAt":"2026-11-23T09:00:00Z"},{"databaseId":42,"status":"completed","conclusion":"success","url":"u","headBranch":"release/v0.2.200","createdAt":"2026-11-23T10:00:00Z"}]'
run_waiter "a run on another branch does not count" 1 \
    runs='[{"databaseId":42,"status":"completed","conclusion":"success","url":"u","headBranch":"main","createdAt":"t"}]'
run_waiter "no run at all fails after the no-run timeout" 1 runs='[]'
check "no run: the error names the likely cause" "$(grep -c 'RELEASE_PAT' "$TEST_TMP/case/log")" "1"
run_waiter "a run still in progress at the deadline fails" 1 \
    runs='[{"databaseId":42,"status":"in_progress","conclusion":null,"url":"u","headBranch":"release/v0.2.200","createdAt":"t"}]'
run_waiter "a run that completes after polling in progress passes" 0 \
    runs=$'[{"databaseId":42,"status":"in_progress","conclusion":null,"url":"u","headBranch":"release/v0.2.200","createdAt":"t"}]\n---\n[{"databaseId":42,"status":"completed","conclusion":"success","url":"u","headBranch":"release/v0.2.200","createdAt":"t"}]'
run_waiter "transient run-list errors are retried" 0 \
    runs=$'FAIL2\n[{"databaseId":42,"status":"completed","conclusion":"success","url":"u","headBranch":"release/v0.2.200","createdAt":"t"}]'
run_waiter "transient commit-lookup errors are retried" 0 \
    commit=$'FAIL2\n{"sha":"'"$B"'","parents":[{"sha":"'"$SHIP"'"}]}'
run_waiter "a gh that never answers fails at the deadline" 1 runs=FAIL
run_waiter "a release commit on another parent fails" 1 \
    commit='{"parents":[{"sha":"6666666666666666666666666666666666666666"}]}'
run_waiter "a merge commit (two parents) fails" 1 \
    commit="{\"parents\":[{\"sha\":\"$SHIP\"},{\"sha\":\"6666666666666666666666666666666666666666\"}]}"
run_waiter "a release commit touching a source file fails" 1 \
    compare='{"status":"ahead","ahead_by":1,"files":[{"filename":"crates/core/Cargo.toml"},{"filename":"crates/core/src/lib.rs"}]}'
run_waiter "a release commit two commits past ship fails" 1 \
    compare='{"status":"ahead","ahead_by":2,"files":[{"filename":"crates/core/Cargo.toml"}]}'
run_waiter "an empty release commit fails" 1 compare='{"status":"ahead","ahead_by":1,"files":[]}'
CASE_B=main run_waiter "a non-SHA release commit fails" 1

run_waiter "baseline again, to inspect the calls" 0
check "the waiter queries exactly the push runs of ci.yml for B" \
    "$(grep -c "^gh run list --repo freenet/freenet-core --workflow ci.yml --commit $B --event push " "$TEST_TMP/case/calls")" "1"

# --- 3. wiring ---------------------------------------------------------------
check "CI runs this test" \
    "$(grep -cE '^[[:space:]]*run: bash scripts/wait_for_release_ci_test\.sh[[:space:]]*$' "$REPO_ROOT/.github/workflows/ci.yml")" "1"
check "shellcheck covers the waiter and this test" \
    "$(grep -E '^[[:space:]]*run: shellcheck ' "$REPO_ROOT/.github/workflows/ci.yml" | grep -c 'scripts/wait_for_release_ci.sh scripts/wait_for_release_ci_test.sh')" "1"

if [ "$FAILURES" -ne 0 ]; then
    echo "$FAILURES failure(s)" >&2
    exit 1
fi
echo "All wait_for_release_ci tests passed."

#!/usr/bin/env bash
# Pins the cron/job pairing in simulation-nightly.yml (PR #6105).
#
# The workflow has two crons: the large simulation runs weekly and WAN Bench
# R1 daily. Each job picks its cron with `github.event.schedule == '<cron>'`,
# a literal copy of the string under `on.schedule`. If the two drift (say the
# weekly cron moves to Saturday and the job's `if:` is not updated), the job
# never runs on schedule again, and that shows only as SKIPPED -- which the
# failure notifiers (`failure()`) and the release pre-flight both treat as
# healthy. So: every cron a job selects must be scheduled, and every
# scheduled cron must be selected by some job (or it starts a run that does
# nothing). Set equality alone would pass if the two jobs SWAPPED crons, so the
# cadence each job gets is pinned too: the large simulation weekly (daily cost
# ~$42/month of Ubicloud, the reason it moved), WAN Bench R1 daily.
#
# Run manually with: bash scripts/simulation_schedule_test.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
WORKFLOW="${1:-$REPO_ROOT/.github/workflows/simulation-nightly.yml}"

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

RESULT=$(python3 - "$WORKFLOW" <<'PY'
import re, sys
text = open(sys.argv[1]).read()
code = "\n".join(l for l in text.splitlines() if not l.lstrip().startswith("#"))
crons = set(re.findall(r"^\s*-\s*cron:\s*[\"']([^\"']+)[\"']", code, re.M))
selected = set(re.findall(r"github\.event\.schedule\s*==\s*'([^']+)'", code))
# '|'-separated: cron strings contain spaces.
# Which cron each job selects: the literal in its own `if:`.
def job_cron(job_id):
    m = re.search(r"^  " + re.escape(job_id) + r":\n(.*?)(?=^  [A-Za-z0-9_-]+:\n|\Z)",
                  code, re.M | re.S)
    sel = re.findall(r"github\.event\.schedule\s*==\s*'([^']+)'", m.group(1)) if m else []
    return sel[0] if len(sel) == 1 else "?"
def cadence(cron):
    f = cron.split()
    if len(f) != 5:
        return "?"
    return "daily" if f[2:] == ["*", "*", "*"] else (
        "weekly" if f[2:4] == ["*", "*"] and f[4] != "*" else "other")
print("|".join([str(len(crons)), str(len(selected)),
                "; ".join(sorted(selected - crons)) or "none",
                "; ".join(sorted(crons - selected)) or "none",
                cadence(job_cron("large-scale-simulation")),
                cadence(job_cron("wan-bench-r1"))]))
PY
)
IFS="|" read -r N_CRONS N_SELECTED UNSCHEDULED UNUSED LARGE_SIM WAN_BENCH <<<"$RESULT"

check "the workflow has a schedule to check" "$([[ $N_CRONS -ge 1 ]] && echo yes || echo no)" "yes"
check "jobs select their cron explicitly" "$([[ $N_SELECTED -ge 1 ]] && echo yes || echo no)" "yes"
check "every cron a job selects is scheduled" "$UNSCHEDULED" "none"
check "every scheduled cron is selected by a job" "$UNUSED" "none"
check "the large simulation is scheduled weekly" "$LARGE_SIM" "weekly"
check "WAN Bench R1 is scheduled daily" "$WAN_BENCH" "daily"

if [[ $FAILURES -gt 0 ]]; then
    echo "$FAILURES check(s) failed" >&2
    exit 1
fi
echo "all checks passed"

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
# nothing).
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
print("|".join([str(len(crons)), str(len(selected)),
                "; ".join(sorted(selected - crons)) or "none",
                "; ".join(sorted(crons - selected)) or "none"]))
PY
)
IFS="|" read -r N_CRONS N_SELECTED UNSCHEDULED UNUSED <<<"$RESULT"

check "the workflow has a schedule to check" "$([[ $N_CRONS -ge 1 ]] && echo yes || echo no)" "yes"
check "jobs select their cron explicitly" "$([[ $N_SELECTED -ge 1 ]] && echo yes || echo no)" "yes"
check "every cron a job selects is scheduled" "$UNSCHEDULED" "none"
check "every scheduled cron is selected by a job" "$UNUSED" "none"

if [[ $FAILURES -gt 0 ]]; then
    echo "$FAILURES check(s) failed" >&2
    exit 1
fi
echo "all checks passed"

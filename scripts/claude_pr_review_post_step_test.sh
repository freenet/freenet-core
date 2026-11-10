#!/usr/bin/env bash
# Regression tests for the "Post review comment and set commit status" step of
# .github/workflows/claude-pr-review.yml (#5628).
#
# Why this exists. `Claude Rule Review` is a REQUIRED check, and its review step
# is continue-on-error so a broken reviewer never blocks merge. That made a dead
# reviewer invisible: for weeks every PR carried a green `rule-review/warnings`
# status for a review that never ran. The post step now reports a non-run as a
# FAILURE status and opens a tracking issue. Two properties matter and neither
# was pinned by anything:
#
#   1. A review that did not run must NEVER read as success.
#   2. The new issue bookkeeping must NEVER stop the comment/status from being
#      set, or fail the job: an API error there (rate limit, locked issue, 5xx)
#      would turn a required check red and block every merge.
#
# The script is pulled out of the live workflow (not copied), the `${{ }}`
# expressions are substituted, and it runs under mocked `github`/`core`/`context`.
# Then a few mutations of the workflow are run through the same harness, and each
# must make it FAIL: a guard nobody has seen fail is not known to guard.
#
# Needs: bash, awk, node. Run manually with:
#   bash scripts/claude_pr_review_post_step_test.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
WORKFLOW="$REPO_ROOT/.github/workflows/claude-pr-review.yml"
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

# extract_script <workflow-file> <out-file>: the `script: |` body of the post step,
# de-indented, with `${{ expr }}` replaced by fixed values (or placeholders the
# harness fills in). No YAML parser: this repo's linters avoid that dependency.
extract_script() {
    awk '
        /^      - name: Post review comment and set commit status/ { in_step = 1; next }
        in_step && /^          script: \|/ { in_body = 1; next }
        in_body {
            if ($0 ~ /^            / || $0 ~ /^[[:space:]]*$/) { sub(/^            /, ""); print; next }
            exit
        }
    ' "$1" \
    | sed -e 's/\${{ steps.review.outcome }}/__OUTCOME__/g' \
          -e 's/\${{ steps.review.outputs.execution_file }}/__EXEC__/g' \
          -e 's/\${{ github.server_url }}/https:\/\/github.com/g' \
          -e 's/\${{ github.repository }}/o\/r/g' \
          -e 's/\${{ github.run_id }}/99/g' \
          -e 's/\${{ github.event.pull_request.head.sha }}/abc/g' \
          -e 's/\${{ github.event.pull_request.number }}/1/g' \
    > "$2"
}

cat > "$TEST_TMP/harness.js" <<'EOF'
const fs = require('fs');
const [, , scriptPath, workDir] = process.argv;
const AsyncFn = Object.getPrototypeOf(async function () {}).constructor;
const src0 = fs.readFileSync(scriptPath, 'utf8');
if (!src0.includes('createCommitStatus')) { console.log('FAIL - extracted script is empty or wrong'); process.exit(2); }

const MARKER = '<!-- claude-rule-review-outage -->';
const bot = n => ({ number: n, body: MARKER + '\nx' });
let failures = 0;
const t = (desc, ok, extra) => { if (ok) console.log('ok   - ' + desc); else { failures++; console.error('FAIL - ' + desc + (extra ? ' ' + extra : '')); } };

async function run({ outcome, exec, open = [], throwOn = [] }) {
  const calls = [];
  const maybe = name => { if (throwOn.includes(name)) throw new Error('boom:' + name); };
  const execPath = workDir + '/exec.json';
  if (exec === undefined) fs.rmSync(execPath, { force: true });
  else fs.writeFileSync(execPath, typeof exec === 'string' ? exec : JSON.stringify(exec));
  const src = src0.replace(/__OUTCOME__/g, outcome).replace(/__EXEC__/g, execPath);
  const github = {
    paginate: async (fn, args) => { calls.push(['paginate', args]); maybe('paginate'); return open; },
    rest: {
      issues: {
        listForRepo: {},
        listComments: async () => { maybe('listComments'); return { data: [] }; },
        // The PR is #1 (context.issue.number); any other number is the outage issue.
        createComment: async a => { calls.push(['comment', a.issue_number]); maybe(a.issue_number === 1 ? 'prComment' : 'createComment'); },
        updateComment: async () => { calls.push(['updateComment']); },
        create: async a => { calls.push(['issue.create', a.title, a.body]); maybe('issue.create'); },
        update: async a => { calls.push(['issue.update', a.issue_number, a.state]); maybe('issue.update'); },
      },
      repos: { createCommitStatus: async a => { calls.push(['status', a.state, a.description]); maybe('status'); } },
    },
  };
  const core = { error: m => calls.push(['core.error', m]), warning: m => calls.push(['core.warning', m]) };
  let threw = null;
  const ctx = { repo: { owner: 'o', repo: 'r' }, issue: { number: 1 } };
  const origLog = console.log; console.log = () => {};
  try { await new AsyncFn('github', 'context', 'core', 'require', src)(github, ctx, core, require); }
  catch (e) { threw = e; }
  console.log = origLog;
  const idx = k => calls.findIndex(c => c[0] === k);
  return { calls, threw, statuses: calls.filter(c => c[0] === 'status'), creates: calls.filter(c => c[0] === 'issue.create'),
           closes: calls.filter(c => c[0] === 'issue.update'), errors: calls.filter(c => c[0] === 'core.error'), idx };
}

const resErr = (turns, text = 'Invalid API key') =>
  [{ type: 'result', subtype: turns > 1 ? 'error_max_turns' : 'success', is_error: true, num_turns: turns, total_cost_usd: 0, result: text }];
const okReport = [{ type: 'assistant', message: { content: [{ type: 'text', text: '### Rule Review: No issues found\n<!-- warning-count: 0 -->' }] } },
                  { type: 'result', subtype: 'success', is_error: false, num_turns: 3, result: 'x' }];

(async () => {
  // --- property 1: a non-run never reads as success --------------------------
  let r = await run({ outcome: 'failure', exec: resErr(1) });
  t('step failed: status is FAILURE, never success', r.statuses.length === 1 && r.statuses[0][1] === 'failure', JSON.stringify(r.statuses));
  t('step failed: status text says the review did not run', /did not run/i.test(r.statuses[0][2]));
  t('step failed: an annotation carries the API error', r.errors.length === 1 && r.errors[0][1].includes('Invalid API key'));
  t('step failed: no success status anywhere', !r.statuses.some(s => s[1] === 'success'));

  r = await run({ outcome: 'success', exec: [{ type: 'result', is_error: false }] });
  t('step "succeeded" but no report text: status is FAILURE', r.statuses.length === 1 && r.statuses[0][1] === 'failure', JSON.stringify(r.statuses));
  t('step "succeeded" but no report text: reported as an outage', r.creates.length === 1);

  r = await run({ outcome: 'success', exec: 'not json {' });
  t('unparseable execution file: status is FAILURE', r.statuses.length === 1 && r.statuses[0][1] === 'failure', JSON.stringify(r.statuses));
  t('unparseable execution file: reported as an outage', r.creates.length === 1);

  r = await run({ outcome: 'success', exec: okReport });
  t('good review with no warnings: status is success', r.statuses.length === 1 && r.statuses[0][1] === 'success');

  // --- tracking issue: opened once, only for outages PR content cannot cause --
  r = await run({ outcome: 'failure', exec: resErr(1) });
  t('API refused the run (1 turn): one outage issue opened', r.creates.length === 1);
  t('outage issue body carries the dedup marker', r.creates[0][2].includes(MARKER));

  r = await run({ outcome: 'failure', exec: resErr(1), open: [bot(7)] });
  t('outage issue already open: no duplicate created', r.creates.length === 0);

  r = await run({ outcome: 'failure', exec: resErr(1), open: [{ number: 8, body: MARKER, pull_request: {} }, { number: 9, body: 'unrelated' }] });
  t('a PR or an issue without the marker does not count as the outage issue', r.creates.length === 1);

  r = await run({ outcome: 'failure', exec: resErr(25, 'Reached maximum number of turns (25)') });
  t('max-turns failure (PR-induced): red status + annotation but NO issue', r.statuses[0][1] === 'failure' && r.errors.length === 1 && r.creates.length === 0);

  r = await run({ outcome: 'failure', exec: undefined });
  t('no execution file at all: treated as an outage', r.statuses[0][1] === 'failure' && r.creates.length === 1);

  const long = 'x`y'.repeat(200);
  r = await run({ outcome: 'failure', exec: resErr(1, long) });
  t('annotation excerpt is capped and has no backticks', !r.errors[0][1].includes('`') && r.errors[0][1].length < 500, String(r.errors[0][1].length));
  t('the excerpt is NOT copied into the public issue body', !r.creates[0][2].includes('x\'y'));

  r = await run({ outcome: 'failure', exec: resErr(1) });
  const lf = r.calls.find(c => c[0] === 'paginate');
  t('issue lookup filters on creator github-actions[bot]', lf && lf[1].creator === 'github-actions[bot]' && lf[1].state === 'open');

  // --- recovery: closed only on clear evidence, all duplicates --------------
  r = await run({ outcome: 'success', exec: okReport, open: [bot(7), bot(12)] });
  t('review completed: every open outage issue is closed', r.closes.length === 2 && r.closes.every(c => c[2] === 'closed'));
  t('review completed: the status is set BEFORE any issue is touched', r.idx('status') < r.idx('issue.update'));

  r = await run({ outcome: 'success', exec: [okReport[0], { type: 'result', is_error: true }], open: [bot(7)] });
  t('final result is an error: outage issue is NOT closed', r.closes.length === 0);

  r = await run({ outcome: 'success', exec: [okReport[0]], open: [bot(7)] });
  t('no result entry to vouch for the run: outage issue is NOT closed', r.closes.length === 0);

  // --- property 2: bookkeeping failures never cost the status or the job -----
  for (const name of ['paginate', 'issue.create']) {
    r = await run({ outcome: 'failure', exec: resErr(1), throwOn: [name] });
    t(`failure path, ${name} throws: job does not throw`, r.threw === null, String(r.threw));
    t(`failure path, ${name} throws: FAILURE status was still set`, r.statuses.length === 1 && r.statuses[0][1] === 'failure');
  }
  for (const name of ['listComments', 'prComment']) {
    r = await run({ outcome: 'failure', exec: resErr(1), throwOn: [name] });
    t(`failure path, ${name} throws: job does not throw and the status was set`, r.threw === null && r.statuses.length === 1 && r.statuses[0][1] === 'failure', String(r.threw));
  }

  for (const name of ['paginate', 'createComment', 'issue.update', 'listComments', 'prComment']) {
    r = await run({ outcome: 'success', exec: okReport, open: [bot(7)], throwOn: [name] });
    t(`success path, ${name} throws: job does not throw`, r.threw === null, String(r.threw));
    t(`success path, ${name} throws: the review status was still set`, r.statuses.length === 1 && r.statuses[0][1] === 'success');
  }
  r = await run({ outcome: 'success', exec: okReport });
  t('success path: the status is set BEFORE the sticky comment', r.idx('status') < r.idx('comment'));

  process.exit(failures ? 1 : 0);
})();
EOF

run_harness() {
    # run_harness <workflow-file>: runs the harness against that workflow's script.
    local wf="$1" script="$TEST_TMP/post_script.js"
    extract_script "$wf" "$script"
    node "$TEST_TMP/harness.js" "$script" "$TEST_TMP"
}

echo "--- live workflow"
if run_harness "$WORKFLOW"; then
    check "post-step harness passes against the live workflow" "pass" "pass"
else
    check "post-step harness passes against the live workflow" "fail" "pass"
fi

# --- mutations: each must make the harness FAIL -------------------------------
mutate_and_expect_fail() {
    # mutate_and_expect_fail <description> <sed-expression>
    local desc="$1" expr="$2" mutated="$TEST_TMP/mutated.yml"
    sed -e "$expr" "$WORKFLOW" > "$mutated"
    if cmp -s "$WORKFLOW" "$mutated"; then
        echo "FAIL - mutation did not change the workflow (sed no longer matches): $desc" >&2
        FAILURES=$((FAILURES + 1))
        return
    fi
    if run_harness "$mutated" >/dev/null 2>&1; then
        echo "FAIL - mutation SURVIVED (harness still passes): $desc" >&2
        FAILURES=$((FAILURES + 1))
    else
        echo "ok   - mutation caught: $desc"
    fi
}

echo "--- mutations"
mutate_and_expect_fail "a non-run reports success again (#5628)" "s/const NOT_RUN_STATE = 'failure';/const NOT_RUN_STATE = 'success';/"
mutate_and_expect_fail "bookkeeping errors are no longer swallowed" "s/core.warning(\`\${what} failed (ignored): \${e.message}\`);/throw e;/"
mutate_and_expect_fail "dedup creator filter dropped" "/creator: 'github-actions\[bot\]',/d"
mutate_and_expect_fail "issue opens even when the run got going (max-turns)" "s/systemic: !(last.is_error === true \&\& Number(last.num_turns) > 1),/systemic: true,/"
mutate_and_expect_fail "outage issue closes without a clean result entry" "s/if (resultEntry \&\& resultEntry.is_error !== true) {/if (true) {/"

# --- wiring: a guard nobody runs is not a guard -------------------------------
if grep -q 'claude_pr_review_post_step_test.sh' "$CI_YML"; then
    check "ci.yml runs this test" "wired" "wired"
else
    check "ci.yml runs this test" "not wired" "wired"
fi

echo
if [[ "$FAILURES" -gt 0 ]]; then
    echo "$FAILURES check(s) failed" >&2
    exit 1
fi
echo "All tests passed"

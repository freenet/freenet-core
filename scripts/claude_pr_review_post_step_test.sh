#!/usr/bin/env bash
# Regression tests for the "Post review comment and set commit status" step of
# .github/workflows/claude-pr-review.yml (#5628).
#
# Why this exists. `Claude Rule Review` is a REQUIRED check, and its review step
# is continue-on-error so a broken reviewer never blocks merge. That made a dead
# reviewer invisible: every PR carried a green `rule-review/warnings` status for
# a review that never ran. The post step now reports a non-run as a FAILURE
# status and opens a tracking issue. Two properties matter and nothing pinned
# either:
#
#   1. A review that did not run must NEVER read as success.
#   2. The new comment/issue bookkeeping must NEVER stop the status from being
#      set, or fail the job: an API error there (rate limit, locked issue, 5xx)
#      would turn a required check red and block every merge.
#
# The script is pulled out of the live workflow (not copied), the `${{ }}`
# expressions are substituted, and it runs under mocked `github`/`core`/`context`.
# The mocks enforce the API limits the real API would (status description length,
# state vocabulary, context, sha), because a mock that accepts anything cannot see
# a 422. Then mutations of the workflow are run through the same harness, and each
# must make it FAIL, at the check named for it: a guard nobody has seen fail is not
# known to guard, and one that fails for the wrong reason is not that guard.
#
# Out of scope, because they are step-level `if:` conditions outside the
# extracted script: the review-override label, merge_group, and the "no relevant
# files" skip.
#
# Updating this when you edit the post step: a mutation below pins an exact source
# string, so rewording that line makes the mutation report "no longer matches".
# Change the sed expression to follow the rewording; do not delete the mutation
# unless the behaviour it pins is gone.
#
# Needs: bash, awk, sed, node, git. Run manually with:
#   bash scripts/claude_pr_review_post_step_test.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
WORKFLOW="$REPO_ROOT/.github/workflows/claude-pr-review.yml"
CI_YML="$REPO_ROOT/.github/workflows/ci.yml"
TEST_TMP=$(mktemp -d)
trap 'rm -rf "$TEST_TMP"' EXIT

# A real post step is a few hundred lines. A truncated or empty extraction (a
# renamed step, a changed indent) must fail loudly instead of "passing" or making
# every mutation read as caught.
MIN_SCRIPT_LINES=150

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

const MARKER = '<!-- claude-rule-review-outage -->';
const STICKY = '<!-- claude-rule-review -->';
const bot = n => ({ number: n, body: MARKER + '\nx' });
let failures = 0;
const t = (desc, ok, extra) => { if (ok) console.log('ok   - ' + desc); else { failures++; console.error('FAIL - ' + desc + (extra ? ' ' + extra : '')); } };

const STATES = ['error', 'failure', 'pending', 'success'];

// Every mock method yields before recording, like a real network call. A call the
// script forgets to `await` is then missing from `calls` when the script returns,
// instead of being recorded synchronously and passing unnoticed.
const tick = () => new Promise(r => setImmediate(r));

async function run({ outcome, exec, open = [], comments = [], throwOn = [], throwForIssue = [], expectThrow = false }) {
  const calls = [];
  const maybe = name => { if (throwOn.includes(name)) throw new Error('boom:' + name); };
  const execPath = workDir + '/exec.json';
  if (exec === undefined) fs.rmSync(execPath, { force: true });
  else fs.writeFileSync(execPath, typeof exec === 'string' ? exec : JSON.stringify(exec));
  // Function replacers: a `$` in a path must not be read as a replacement pattern.
  const src = src0.replace(/__OUTCOME__/g, () => outcome).replace(/__EXEC__/g, () => execPath);
  const listForRepo = function listForRepo() {};
  // Calls must target this repo, with the paging the code relies on.
  const target = a => { if (a.owner !== 'o' || a.repo !== 'r') throw new Error('wrong repo target ' + a.owner + '/' + a.repo); };
  const paged = a => { if (a.per_page !== 100) throw new Error('missing per_page 100 (got ' + a.per_page + ')'); };
  const known = new Set([1, ...open.map(i => i.number)]);
  const github = {
    // Like the real paginate it is handed the endpoint method itself.
    paginate: async (fn, args) => {
      if (fn !== listForRepo) throw new Error('paginate called with something other than issues.listForRepo');
      target(args); paged(args);
      await tick(); calls.push(['paginate', args]); maybe('paginate'); return open;
    },
    rest: {
      issues: {
        listForRepo,
        listComments: async a => {
          target(a); paged(a);
          if (a.issue_number !== 1) throw new Error('wrong listComments issue_number ' + a.issue_number);
          await tick(); maybe('listComments'); return { data: comments };
        },
        // The PR is #1 (context.issue.number); any other number is the outage issue.
        createComment: async a => {
          target(a);
          if (!known.has(a.issue_number)) throw new Error('wrong createComment issue_number ' + a.issue_number);
          await tick(); calls.push(['comment', a.issue_number, a.body]);
          if (throwForIssue.includes(a.issue_number)) throw new Error('locked:' + a.issue_number);
          maybe(a.issue_number === 1 ? 'prComment' : 'createComment');
        },
        updateComment: async a => {
          target(a);
          if (!comments.some(c => c.id === a.comment_id)) throw new Error('wrong updateComment comment_id ' + a.comment_id);
          await tick(); calls.push(['updateComment', a.comment_id, a.body]);
        },
        create: async a => { target(a); await tick(); calls.push(['issue.create', a.title, a.body]); maybe('issue.create'); },
        update: async a => {
          target(a);
          if (!known.has(a.issue_number)) throw new Error('wrong update issue_number ' + a.issue_number);
          await tick(); calls.push(['issue.update', a.issue_number, a.state, a.state_reason]); maybe('issue.update');
        },
      },
      repos: {
        createCommitStatus: async a => {
          target(a);
          // What the real API would reject with a 422.
          if (!STATES.includes(a.state)) throw new Error('422 bad state ' + a.state);
          if (typeof a.description !== 'string' || a.description.length > 140) throw new Error('422 description over 140 chars: ' + (a.description || '').length);
          if (a.context !== 'rule-review/warnings') throw new Error('wrong status context ' + a.context);
          if (a.sha !== 'abc') throw new Error('wrong sha ' + a.sha);
          if (typeof a.target_url !== 'string' || !/^https:\/\/github\.com\/o\/r\/(pull\/1|actions\/runs\/99)$/.test(a.target_url)) throw new Error('wrong target_url ' + a.target_url);
          // Deliberately the slowest call: a status the script forgot to await lands after the comment.
          for (let i = 0; i < 6; i++) await tick();
          calls.push(['status', a.state, a.description, a.target_url]); maybe('status');
        },
      },
    },
  };
  const core = { error: m => calls.push(['core.error', m]), warning: m => calls.push(['core.warning', m]) };
  let threw = null;
  const ctx = { repo: { owner: 'o', repo: 'r' }, issue: { number: 1 } };
  const origLog = console.log; console.log = () => {};
  try { await new AsyncFn('github', 'context', 'core', 'require', src)(github, ctx, core, require); }
  catch (e) { threw = e; }
  console.log = origLog;
  // A violation of the API contract the mocks enforce is a test failure in its own right, named, not just a thrown error.
  if (threw && /^(422|wrong |missing |paginate called)/.test(threw.message)) t('mock API contract holds', false, threw.message);
  // Only the status call may fail the job, and only where a case says so: a throw anywhere else
  // would turn the required check red.
  if (threw && !expectThrow) t('the job does not throw', false, String(threw));
  const idx = k => calls.findIndex(c => c[0] === k);
  return { calls, threw, statuses: calls.filter(c => c[0] === 'status'), creates: calls.filter(c => c[0] === 'issue.create'),
           closes: calls.filter(c => c[0] === 'issue.update'), errors: calls.filter(c => c[0] === 'core.error'),
           updates: calls.filter(c => c[0] === 'updateComment'), prComments: calls.filter(c => c[0] === 'comment' && c[1] === 1), idx };
}

// Final entries of an execution file, in the shapes the action really writes.
const resErr = (turns, text = 'Invalid API key') =>
  [{ type: 'result', subtype: turns > 1 ? 'error_max_turns' : 'success', is_error: true, num_turns: turns, total_cost_usd: 0, result: text }];
// A real max-turns run (2026-09-09, #5628): 26 turns, $1.23, NO result text.
const realMaxTurns = [{ type: 'result', subtype: 'error_max_turns', is_error: true, num_turns: 26, total_cost_usd: 1.2327772 }];
const reportEntry = text => ({ type: 'assistant', message: { content: [{ type: 'text', text }] } });
const okResult = { type: 'result', subtype: 'success', is_error: false, num_turns: 3, result: 'x' };
const okReport = [reportEntry('### Rule Review: No issues found\n<!-- warning-count: 0 -->'), okResult];
const stale = { id: 5, body: STICKY + '\n### Rule Review: No issues found' };

(async () => {
  // --- property 1: a non-run never reads as success --------------------------
  let r = await run({ outcome: 'failure', exec: resErr(1) });
  t('step failed: status is FAILURE, never success', r.statuses.length === 1 && r.statuses[0][1] === 'failure', JSON.stringify(r.statuses));
  t('step failed: status text says the review did not run', /did not run/i.test(r.statuses[0][2]));
  t('step failed: an annotation carries the API error', r.errors.length === 1 && r.errors[0][1].includes('Invalid API key'));
  t('step failed: no success status anywhere', !r.statuses.some(s => s[1] === 'success'));
  t('step failed: status is set BEFORE the comment', r.idx('status') !== -1 && r.idx('status') < r.idx('comment'));

  // Every non-run replaces a stale "No issues found" sticky comment.
  // `outage` says whether the branch counts as an outage (opens the issue).
  const nonRuns = {
    'step failed': { outcome: 'failure', exec: resErr(1), outage: true },
    'unparseable execution file': { outcome: 'success', exec: 'not json {', outage: true },
    'no report text, no result entry': { outcome: 'success', exec: [{ type: 'assistant', message: { content: [] } }], outage: true },
    'no report text after a clean run': { outcome: 'success', exec: [okResult], outage: false },
    'whitespace-only report': { outcome: 'success', exec: [reportEntry('  \n '), okResult], outage: false },
    'final result is an error but a report exists': { outcome: 'success', exec: [reportEntry('Invalid API key'), ...resErr(1)], outage: true },
  };
  for (const [name, { outage, ...args }] of Object.entries(nonRuns)) {
    r = await run({ ...args, comments: [stale] });
    t(`${name}: the status text says the review did not run`, r.statuses.length === 1 && /did not run/i.test(r.statuses[0][2]), JSON.stringify(r.statuses));
    t(`${name}: ${outage ? 'opens' : 'does not open'} the outage issue`, r.creates.length === (outage ? 1 : 0));
    t(`${name}: status is FAILURE`, r.statuses.length === 1 && r.statuses[0][1] === 'failure', JSON.stringify(r.statuses));
    t(`${name}: the stale sticky comment is replaced with a not-reviewed one`, r.updates.length === 1 && /not reviewed/i.test(r.updates[0][2]) && r.prComments.length === 0);
  }

  r = await run({ outcome: 'success', exec: okReport });
  t('good review with no warnings: status is success', r.statuses.length === 1 && r.statuses[0][1] === 'success');
  t('success path: the status is set BEFORE the sticky comment', r.idx('status') !== -1 && r.idx('status') < r.idx('comment'));

  // --- where the status points, what the comments say, and what order things happen in
  r = await run({ outcome: 'failure', exec: resErr(1) });
  t('non-run status links to the run', r.statuses[0][3] === 'https://github.com/o/r/actions/runs/99');
  t('failure path: the status comes before the comment, the issue lookup and the issue', r.idx('status') !== -1 && r.idx('status') < r.idx('comment') && r.idx('status') < r.idx('paginate') && r.idx('status') < r.idx('issue.create'));
  t('not-reviewed comment carries the marker and says so', r.prComments.length === 1 && r.prComments[0][2].startsWith(STICKY) && /not reviewed/i.test(r.prComments[0][2]));
  t('outage issue links the run', r.creates[0][2].includes('/actions/runs/99'));
  r = await run({ outcome: 'success', exec: okReport, comments: [stale] });
  t('review status links to the PR', r.statuses[0][3] === 'https://github.com/o/r/pull/1');
  t('review comment carries the marker and the report', r.updates.length === 1 && r.updates[0][2].startsWith(STICKY) && r.updates[0][2].includes('No issues found'));

  // --- the LAST entries decide, not the first (real files have dozens of turns) ---
  r = await run({ outcome: 'success', exec: [reportEntry('Let me look at the diff'), reportEntry('#### Warnings\n- a\n<!-- warning-count: 1 -->'), okResult] });
  t('several assistant turns: the last one is the review', r.statuses[0][1] === 'failure' && r.statuses[0][2].includes('1 warning(s)'), JSON.stringify(r.statuses));
  r = await run({ outcome: 'success', exec: [{ type: 'result', subtype: 'success', is_error: false, num_turns: 1 }, reportEntry('x'), ...realMaxTurns] });
  t('a stale early result does not hide a final error result', r.statuses[0][1] === 'failure' && /did not run/i.test(r.statuses[0][2]));
  r = await run({ outcome: 'failure', exec: [{ type: 'result', subtype: 'success', is_error: false, num_turns: 5 }, ...resErr(1)] });
  t('step failed with an earlier result: the last entry decides, so an outage', r.creates.length === 1);

  // --- warnings: the merge-adjacent status for every normal PR ----------------
  r = await run({ outcome: 'success', exec: [reportEntry('#### Warnings\n<!-- warning-count: 3 -->'), okResult] });
  t('marker 3 with no bullets: the marker wins', r.statuses[0][1] === 'failure' && r.statuses[0][2].includes('3 warning(s)'), JSON.stringify(r.statuses));
  r = await run({ outcome: 'success', exec: [reportEntry('#### Warnings\n- a\n- b\n<!-- warning-count: 0 -->'), okResult] });
  t('marker 0 next to bullets: the marker wins', r.statuses[0][1] === 'success', JSON.stringify(r.statuses));
  r = await run({ outcome: 'success', exec: [reportEntry('#### Warnings\n- a\n- b\n<!-- warning-count: 2 -->'), okResult] });
  t('warning-count marker 2: status FAILURE naming 2 warnings', r.statuses[0][1] === 'failure' && r.statuses[0][2].includes('2 warning(s)'), JSON.stringify(r.statuses));
  r = await run({ outcome: 'success', exec: [reportEntry('#### Warnings\n- a\n- b\n- c\n'), okResult] });
  t('no marker: the bullet-count fallback finds 3 warnings', r.statuses[0][1] === 'failure' && r.statuses[0][2].includes('3 warning(s)'), JSON.stringify(r.statuses));
  r = await run({ outcome: 'success', exec: [reportEntry('### Rule Review: No issues found\n'), okResult] });
  t('no marker, "no issues found": status success', r.statuses[0][1] === 'success');

  // --- sticky comment: refreshed in place, not duplicated ---------------------
  r = await run({ outcome: 'success', exec: okReport, comments: [stale] });
  t('existing sticky comment is updated, not duplicated', r.updates.length === 1 && r.prComments.length === 0);
  r = await run({ outcome: 'success', exec: okReport, comments: [{ id: 6, body: 'unrelated' }] });
  t('no sticky comment yet: a new one is created', r.updates.length === 0 && r.prComments.length === 1);

  // --- tracking issue: opened once, only for outages PR content cannot cause --
  r = await run({ outcome: 'failure', exec: resErr(1) });
  t('API refused the run (1 turn): one outage issue opened', r.creates.length === 1);
  t('outage issue body carries the dedup marker and mentions @sanity', r.creates[0][2].includes(MARKER) && r.creates[0][2].includes('@sanity'));
  t('outage issue has its title', r.creates[0][1] === 'CI: Claude rule review is failing to run');
  t('outage issue body states no date and no certain cause', !/2025-09-27/.test(r.creates[0][2]) && /If the run ended within about a turn/.test(r.creates[0][2]));

  r = await run({ outcome: 'failure', exec: resErr(1), open: [bot(7)] });
  t('outage issue already open: no duplicate created', r.creates.length === 0);

  r = await run({ outcome: 'failure', exec: resErr(1), open: [{ number: 8, body: MARKER, pull_request: {} }, { number: 9, body: 'unrelated' }] });
  t('a PR or an issue without the marker does not count as the outage issue', r.creates.length === 1);

  r = await run({ outcome: 'failure', exec: realMaxTurns });
  t('real max-turns run (26 turns): red status + annotation but NO issue', r.statuses[0][1] === 'failure' && r.errors.length === 1 && r.creates.length === 0);

  r = await run({ outcome: 'failure', exec: resErr(25, 'the model said: secret diff text') });
  t('per-PR failure: the model text is NOT in the annotation', r.errors.length === 1 && !r.errors[0][1].includes('secret diff text'));

  r = await run({ outcome: 'failure', exec: undefined });
  t('no execution file at all: treated as an outage', r.statuses[0][1] === 'failure' && r.creates.length === 1);
  r = await run({ outcome: 'failure', exec: [{ type: 'assistant', message: { content: [] } }] });
  t('execution file with no result entry: treated as an outage', r.statuses[0][1] === 'failure' && r.creates.length === 1);

  r = await run({ outcome: 'success', exec: [okResult] });
  t('no report after a run that got going: not an outage, no issue', r.creates.length === 0 && r.statuses[0][1] === 'failure');
  r = await run({ outcome: 'success', exec: [{ type: 'assistant', message: { content: [] } }] });
  t('no report and no result entry: an outage', r.creates.length === 1);

  const long = 'x`y'.repeat(200);
  r = await run({ outcome: 'failure', exec: resErr(1, long) });
  t('annotation excerpt is capped at 300 characters and has no backticks', r.errors.length === 1 && !r.errors[0][1].includes('`') && r.errors[0][1].split('result=')[1].length <= 300, String(r.errors[0] && r.errors[0][1].length));
  t('the excerpt is NOT copied into the public issue body', r.creates.length === 1 && !r.creates[0][2].includes('x\'y'));
  r = await run({ outcome: 'failure', exec: resErr(1, 'line one\n::set-output name=x::y\r\nline three') });
  t('annotation excerpt is a single line', r.errors.length === 1 && !/[\r\n]/.test(r.errors[0][1]), JSON.stringify(r.errors[0] && r.errors[0][1]));

  r = await run({ outcome: 'failure', exec: resErr(1) });
  const lf = r.calls.find(c => c[0] === 'paginate');
  t('issue lookup filters on creator github-actions[bot]', !!lf && lf[1].creator === 'github-actions[bot]' && lf[1].state === 'open');

  // --- recovery: closed only on clear evidence, all duplicates --------------
  r = await run({ outcome: 'success', exec: okReport, open: [bot(7), bot(12)] });
  t('review completed: every open outage issue is closed as completed', r.closes.length === 2 && r.closes.every(c => c[2] === 'closed' && c[3] === 'completed'));
  t('review completed: the status is set BEFORE any issue is touched', r.idx('status') !== -1 && r.idx('status') < r.idx('issue.update'));

  r = await run({ outcome: 'success', exec: [reportEntry('x'), { type: 'result', is_error: true }], open: [bot(7)] });
  t('final result is an error: outage issue is NOT closed and the status is not success', r.closes.length === 0 && r.statuses[0][1] === 'failure');

  r = await run({ outcome: 'success', exec: [okReport[0]], open: [bot(7)] });
  t('no result entry to vouch for the run: outage issue is NOT closed', r.closes.length === 0);

  r = await run({ outcome: 'success', exec: okReport, open: [bot(7), bot(12)], throwForIssue: [7] });
  t('one outage issue cannot be closed: the others still are', r.threw === null && r.closes.length === 1 && r.closes[0][1] === 12, JSON.stringify(r.closes));

  // --- the one-turn boundary and malformed entries ----------------------------
  r = await run({ outcome: 'failure', exec: resErr(1) });
  t('1 turn (refused at the first call): an outage', r.creates.length === 1);
  r = await run({ outcome: 'failure', exec: resErr(2) });
  t('2 turns (the run reached the model): not an outage', r.creates.length === 0 && r.statuses[0][1] === 'failure');
  r = await run({ outcome: 'failure', exec: [{ type: 'result', is_error: true, result: 'x' }] });
  t('num_turns missing: treated as an outage', r.creates.length === 1);

  // A malformed entry must not throw before the status is set (the required job must stay green).
  r = await run({ outcome: 'failure', exec: '[{"type":"result","is_error":true,"num_turns":1,"result":{"toString":"x"},"subtype":{"toString":"y"}}]' });
  t('malformed result entry (objects where strings belong): job does not throw', r.threw === null, String(r.threw));
  t('malformed result entry: FAILURE status still set and the outage reported', r.statuses.length === 1 && r.statuses[0][1] === 'failure' && r.creates.length === 1);
  r = await run({ outcome: 'success', exec: [reportEntry('Invalid API key'), JSON.parse('{"type":"result","is_error":true,"num_turns":1,"result":{"toString":"x"}}')] });
  t('malformed error result after a "successful" step: job does not throw', r.threw === null && r.statuses.length === 1 && r.statuses[0][1] === 'failure', String(r.threw));

  // A null in the array must not lose a real review.
  r = await run({ outcome: 'success', exec: [null, ...okReport] });
  t('a null entry in the execution file does not lose the review', r.statuses.length === 1 && r.statuses[0][1] === 'success' && r.creates.length === 0);

  // One definition of "clean" for both gates: only a result that says it succeeded counts.
  // Anything else that is present is a non-run, and never closes the outage issue.
  const unclean = {
    'non-success subtype with is_error false': { type: 'result', subtype: 'error_max_turns', is_error: false, num_turns: 26 },
    'is_error missing': { type: 'result', subtype: 'success', num_turns: 3 },
    'is_error null': { type: 'result', subtype: 'success', is_error: null, num_turns: 3 },
    'subtype missing': { type: 'result', is_error: false, num_turns: 3 },
  };
  for (const [name, entry] of Object.entries(unclean)) {
    r = await run({ outcome: 'success', exec: [reportEntry('Let me look at the diff'), entry], open: [bot(7)] });
    t(`result with ${name}: a non-run, not a green review`, r.statuses.length === 1 && r.statuses[0][1] === 'failure' && /did not run/i.test(r.statuses[0][2]), JSON.stringify(r.statuses));
    t(`result with ${name}: the outage issue is NOT closed`, r.closes.length === 0);
  }

  // Malformed fields and nulls must not lose the status or a real review.
  r = await run({ outcome: 'failure', exec: '[{"type":"result","is_error":true,"num_turns":{"toString":"x"}}]' });
  t('malformed num_turns on a failed step: job does not throw, FAILURE status set', r.threw === null && r.statuses.length === 1 && r.statuses[0][1] === 'failure', String(r.threw));
  r = await run({ outcome: 'success', exec: [reportEntry('Invalid API key'), JSON.parse('{"type":"result","is_error":true,"num_turns":{"toString":"x"}}')] });
  t('malformed num_turns after a "successful" step: job does not throw, FAILURE status set', r.threw === null && r.statuses.length === 1 && r.statuses[0][1] === 'failure', String(r.threw));
  r = await run({ outcome: 'failure', exec: [...resErr(2), null] });
  t('trailing null entry on a failed step: the last real entry decides (2 turns, not an outage)', r.creates.length === 0 && r.statuses[0][1] === 'failure');
  r = await run({ outcome: 'success', exec: [{ type: 'assistant', message: { content: [null, { type: 'text', text: '### Rule Review: No issues found\n<!-- warning-count: 0 -->' }] } }, okResult] });
  t('null in message content does not lose the review', r.statuses.length === 1 && r.statuses[0][1] === 'success');

  // --- property 2: bookkeeping failures never cost the status or the job -----
  for (const name of ['paginate', 'issue.create']) {
    r = await run({ outcome: 'failure', exec: resErr(1), throwOn: [name] });
    t(`failure path, ${name} throws: job does not throw`, r.threw === null, String(r.threw));
    t(`failure path, ${name} throws: FAILURE status was still set`, r.statuses.length === 1 && r.statuses[0][1] === 'failure');
  }
  for (const name of ['listComments', 'prComment']) {
    r = await run({ outcome: 'failure', exec: resErr(1), throwOn: [name] });
    t(`failure path, ${name} throws: job does not throw and the status was set`, r.threw === null && r.statuses.length === 1 && r.statuses[0][1] === 'failure', String(r.threw));
    t(`failure path, ${name} throws: the lost comment is an error annotation`, r.calls.some(c => c[0] === 'core.error' && /posting the PR comment/.test(c[1])));
    t(`failure path, ${name} throws: the outage is still reported`, r.creates.length === 1);
  }
  for (const name of ['paginate', 'createComment', 'issue.update', 'listComments', 'prComment']) {
    r = await run({ outcome: 'success', exec: okReport, open: [bot(7)], throwOn: [name] });
    t(`success path, ${name} throws: job does not throw`, r.threw === null, String(r.threw));
    t(`success path, ${name} throws: the review status was still set`, r.statuses.length === 1 && r.statuses[0][1] === 'success');
  }
  // The one call that is NOT best-effort, on purpose: nothing else can report it.
  r = await run({ outcome: 'failure', exec: resErr(1), throwOn: ['status'], expectThrow: true });
  t('the status call itself failing fails the job loudly', r.threw !== null && /boom:status/.test(String(r.threw)));

  process.exit(failures ? 1 : 0);
})().catch(e => {
  // A crash in an assertion (an unexpected shape after a regression) must be a named failure.
  console.error('FAIL - harness crashed: ' + (e && e.stack ? e.stack.split('\n').slice(0, 3).join(' | ') : e));
  process.exit(1);
});
EOF

run_harness() {
    # run_harness <workflow-file>: runs the harness against that workflow's script.
    local wf="$1" script="$TEST_TMP/post_script.js"
    extract_script "$wf" "$script"
    local lines
    lines=$(wc -l < "$script")
    if [[ "$lines" -lt "$MIN_SCRIPT_LINES" ]]; then
        echo "FAIL - extracted post-step script is only $lines lines (expected at least $MIN_SCRIPT_LINES): the step was renamed or re-indented" >&2
        return 1
    fi
    if grep -n '\${{' "$script" >&2; then
        echo "FAIL - the script has a \${{ }} expression this test does not substitute (listed above): add it to extract_script, and think about whether it can carry PR-controlled text" >&2
        return 1
    fi
    node "$TEST_TMP/harness.js" "$script" "$TEST_TMP"
}

echo "--- live workflow"
if run_harness "$WORKFLOW"; then
    check "post-step harness passes against the live workflow" "pass" "pass"
else
    check "post-step harness passes against the live workflow" "fail" "pass"
fi

# --- mutations: each must make the harness FAIL, at the named check -----------
mutate_and_expect_fail() {
    # mutate_and_expect_fail <description> <sed-expression> <substring the failing output must contain>
    local desc="$1" expr="$2" want="$3" mutated="$TEST_TMP/mutated.yml" out
    sed -e "$expr" "$WORKFLOW" > "$mutated"
    if cmp -s "$WORKFLOW" "$mutated"; then
        echo "FAIL - mutation did not change the workflow (sed no longer matches): $desc" >&2
        FAILURES=$((FAILURES + 1))
        return
    fi
    if out=$(run_harness "$mutated" 2>&1); then
        echo "FAIL - mutation SURVIVED (harness still passes): $desc" >&2
        FAILURES=$((FAILURES + 1))
    elif printf '%s\n' "$out" | grep '^FAIL - ' | grep -qF -- "$want"; then
        # Only FAIL lines count: $out also holds every passing check's `ok` line,
        # which would match any substring and make this check vacuous.
        echo "ok   - mutation caught at '$want': $desc"
    else
        echo "FAIL - mutation was caught, but not at '$want' (wrong reason): $desc" >&2
        echo "$out" | grep -m3 '^FAIL' >&2 || true
        FAILURES=$((FAILURES + 1))
    fi
}

LONG_TEXT=$(printf 'x%.0s' $(seq 1 150))

echo "--- mutations"
mutate_and_expect_fail "a non-run reports success again (#5628)" \
    "s/const NOT_RUN_STATE = 'failure';/const NOT_RUN_STATE = 'success';/" "status is FAILURE"
mutate_and_expect_fail "bookkeeping errors are no longer swallowed" \
    "s/core\[level\](\`\${what} failed (ignored): \${e.message}\`);/throw e;/" "does not throw"
mutate_and_expect_fail "dedup creator filter dropped" \
    "/creator: 'github-actions\[bot\]',/d" "issue lookup filters on creator"
mutate_and_expect_fail "issue opens even when the run got going (max-turns)" \
    "s/const systemic = !(turns > 1);/const systemic = true;/" "real max-turns run"
mutate_and_expect_fail "outage issue closes without a clean result entry" \
    "s/if (cleanResult(resultEntry)) {/if (true) {/" "NOT closed"
mutate_and_expect_fail "a result is a non-run only when is_error is exactly true (the two gates disagree)" \
    "s/if (resultEntry \&\& !cleanResult(resultEntry)) {/if (resultEntry \&\& resultEntry.is_error === true) {/" "non-success subtype with is_error false"
mutate_and_expect_fail "a non-success subtype counts as a clean run" \
    "s/ \&\& e.subtype === 'success';/;/" "subtype missing"
mutate_and_expect_fail "a missing is_error counts as a clean run" \
    "s/e.is_error === false \&\&/e.is_error !== true \&\&/" "is_error missing"
mutate_and_expect_fail "a malformed num_turns can throw before the status is set" \
    "s/try { turns = Number(e.num_turns); } catch (_) { \/\* malformed: counts as not got going \*\/ }/turns = Number(e.num_turns);/" "malformed num_turns"
mutate_and_expect_fail "a trailing null entry hides the final result" \
    "s/entries.filter(x => x).pop()/entries[entries.length - 1]/" "trailing null entry"
mutate_and_expect_fail "a null in message content loses the review" \
    "s/.filter(c => c \&\& c.type === 'text')/.filter(c => c.type === 'text')/" "null in message content"
mutate_and_expect_fail "a null entry in the execution file loses the review" \
    "s/turns.filter(t => t \&\& t.type === 'assistant'/turns.filter(t => t.type === 'assistant'/" "null entry"
mutate_and_expect_fail "a malformed entry can throw before the status is set" \
    "s/try { return String(v ?? ''); } catch (_) { return '\[unprintable\]'; }/return String(v ?? '');/" "malformed error result after"
mutate_and_expect_fail "a non-run leaves the stale sticky comment in place" \
    "s/await postStickyComment(notReviewedBody);/\/\/ removed/" "stale sticky comment is replaced"
mutate_and_expect_fail "an error final result is believed to be a review" \
    "s/if (resultEntry \&\& !cleanResult(resultEntry)) {/if (false) {/" "final result is an error but a report exists"
mutate_and_expect_fail "per-PR model text leaks into the public annotation" \
    "s/const excerpt = systemic ? /const excerpt = true ? /" "model text is NOT in the annotation"
mutate_and_expect_fail "issues are closed without a reason" \
    "/state_reason: 'completed',/d" "closed as completed"
mutate_and_expect_fail "the @sanity notification is dropped" \
    "s/'@sanity: the /'the /" "mentions @sanity"
mutate_and_expect_fail "the sticky comment is never found, so every run adds one" \
    "s/const existing = comments.find(c => c.body.includes(marker));/const existing = undefined;/" "updated, not duplicated"
mutate_and_expect_fail "a status description over the API's 140-character limit" \
    "s/const NOT_RUN_TEXT = '/const NOT_RUN_TEXT = '$LONG_TEXT/" "description over 140"
mutate_and_expect_fail "warnings never turn the status red" \
    "s/const state = hasWarnings ? 'failure' : 'success';/const state = 'success';/" "warning-count marker 2"
mutate_and_expect_fail "one locked outage issue strands the others" \
    "/closing outage issue #/,/^              }\$/ { s/await bestEffort(\`closing outage issue #\${issue.number}\`, async () => {/await (async () => {/; s/^                });\$/                })();/; }" "one outage issue cannot be closed"

# --- the premises the script cannot check itself -------------------------------
# The extracted script is only half the design. Take away a permission or the
# tolerance on the review step and it keeps passing here while production breaks:
# without `issues: write` every issue call 403s and best-effort swallows it, so the
# outage issue silently never opens; without `continue-on-error` a broken reviewer
# blocks every merge.
check "job grants issues: write" "$(grep -Ec '^      issues: write$' "$WORKFLOW")" "1"
check "job grants statuses: write" "$(grep -Ec '^      statuses: write$' "$WORKFLOW")" "1"
REVIEW_TOLERANT=$(awk '/^        id: review$/ { f = 1 } f && /^        continue-on-error: true$/ { print "yes"; exit } f && /^        uses: / { print "no"; exit }' "$WORKFLOW")
check "the review step is continue-on-error" "${REVIEW_TOLERANT:-missing}" "yes"

# --- fork PR workspace isolation (pull_request_target security boundary) ------
# This workflow runs under pull_request_target with CLAUDE_CODE_OAUTH_TOKEN and
# write tokens. Untrusted PR head files (.claude/settings.json, hooks,
# .mcp.json, CLAUDE.md) must NEVER be checked out into $GITHUB_WORKSPACE, and
# Claude Code must only load user settings (--setting-sources user) where the
# action writes the read-only permissions lockdown.
extract_step() {
    local step_name="$1" wf="$2"
    awk -v target="      - name: ${step_name}" '
        $0 == target { in_step = 1; print; next }
        in_step && /^      - / { exit }
        in_step { print }
    ' "$wf"
}

strip_yaml_comments() {
    sed 's/^[[:space:]]*#.*$//'
}

extract_run_body() {
    local step_name="$1" wf="$2"
    extract_step "$step_name" "$wf" | awk '
        /^        run: \|/ { in_run = 1; next }
        in_run {
            if ($0 ~ /^          / || $0 ~ /^[[:space:]]*$/) { sub(/^          /, ""); print; next }
            exit
        }
    '
}

extract_prompt_body() {
    local wf="$1"
    extract_step "Claude rule review" "$wf" | awk '
        /^          prompt: \|/ { in_prompt = 1; next }
        in_prompt {
            if ($0 ~ /^            / || $0 ~ /^[[:space:]]*$/) { sub(/^            /, ""); print; next }
            exit
        }
    '
}

CHECKOUT_STEP=$(extract_step "Checkout repository" "$WORKFLOW" | strip_yaml_comments)
check "workflow has exactly one actions/checkout step" "$(strip_yaml_comments < "$WORKFLOW" | grep -Ec 'uses:[[:space:]]*actions/checkout' || true)" "1"
check "checkout step omits allow-unsafe-pr-checkout" "$(printf '%s\n' "$CHECKOUT_STEP" | grep -Ec 'allow-unsafe-pr-checkout' || true)" "0"
check "checkout step does not check out pull_request.head" "$(printf '%s\n' "$CHECKOUT_STEP" | grep -Ec 'pull_request\.head' || true)" "0"
check "checkout step omits ref (trusted default checkout)" "$(printf '%s\n' "$CHECKOUT_STEP" | grep -Ec '^[[:space:]]*ref:' || true)" "0"
check "checkout step disables credential persistence" "$(printf '%s\n' "$CHECKOUT_STEP" | grep -Ec 'persist-credentials:[[:space:]]*false' || true)" "1"
check "checkout step fetches full history for base/head diffs" "$(printf '%s\n' "$CHECKOUT_STEP" | grep -Ec 'fetch-depth:[[:space:]]*0' || true)" "1"
check "workflow never enables allow-unsafe-pr-checkout" "$(strip_yaml_comments < "$WORKFLOW" | grep -Ec 'allow-unsafe-pr-checkout' || true)" "0"

# Job-level branch skip rules must not allow arbitrary fork branches named
# release/v* or dependabot/* to skip the required review check.
JOB_IF=$(awk '/^    if: \|/ { f = 1; next } f && /^      / { print; next } f { exit }' "$WORKFLOW")
check "release/v skip is gated on same-repo head" "$(printf '%s\n' "$JOB_IF" | grep -Ec "head\.repo\.full_name == github\.repository.*release/v" || true)" "1"
check "dependabot/ skip is gated on dependabot[bot]" "$(printf '%s\n' "$JOB_IF" | grep -Ec "user\.login == 'dependabot\[bot\]'.*dependabot/" || true)" "1"

FETCH_RUN=$(extract_run_body "Fetch PR head commit without checkout" "$WORKFLOW")
check "fetch step verifies HEAD_SHA commit object" "$(printf '%s\n' "$FETCH_RUN" | grep -Ec 'git cat-file -e "\$\{HEAD_SHA\}\^\{commit\}"' || true)" "2"
check "fetch step verifies BASE_SHA commit object" "$(printf '%s\n' "$FETCH_RUN" | grep -Ec 'git cat-file -e "\$\{BASE_SHA\}\^\{commit\}"' || true)" "2"
check "fetch step never checks out or modifies working tree" "$(printf '%s\n' "$FETCH_RUN" | grep -Ec 'git[[:space:]]+(checkout|switch|reset|worktree)' || true)" "0"

REVIEW_STEP=$(extract_step "Claude rule review" "$WORKFLOW" | strip_yaml_comments)
CLAUDE_ARGS_LINE=$(printf '%s\n' "$REVIEW_STEP" | grep -E '^[[:space:]]*claude_args:' || true)
check "claude_args sets --setting-sources user only (no project/local)" "$(printf '%s\n' "$CLAUDE_ARGS_LINE" | grep -Ec -- "--setting-sources[[:space:]]+user([[:space:]'\"]|$)" || true)" "1"
check "claude_args has only one --setting-sources flag" "$(printf '%s\n' "$CLAUDE_ARGS_LINE" | grep -Eo -- '--setting-sources' | wc -l | tr -d ' ')" "1"
check "claude_args restricts built-in tools to Read,Glob,Grep" "$(printf '%s\n' "$CLAUDE_ARGS_LINE" | grep -Ec -- '--tools[[:space:]]+"Read,Glob,Grep"' || true)" "1"
check "claude_args sets --strict-mcp-config" "$(printf '%s\n' "$CLAUDE_ARGS_LINE" | grep -Ec -- '--strict-mcp-config([[:space:]'\''"]|$)' || true)" "1"

# Inspect raw prompt body (do NOT strip `#` lines, which are Markdown headings
# inside prompt: | and still evaluated for ${{ }} by GitHub Actions).
PROMPT_BODY=$(extract_prompt_body "$WORKFLOW")
check "review prompt does not inline untrusted pull_request.title" "$(printf '%s\n' "$PROMPT_BODY" | grep -Ec 'github\.event\.pull_request\.title' || true)" "0"
check "review prompt references precomputed pr-title.txt" "$(printf '%s\n' "$PROMPT_BODY" | grep -qE 'steps\.diff\.outputs\.dir.*/pr-title\.txt' && echo present || echo missing)" "present"

# Every shell run: block in trusted pre-review steps must exist and read PR
# metadata from env:, never via inline ${{ }} interpolation. Check RAW run
# bodies (not comment-stripped), because GitHub Actions expands ${{ }} even on
# bash comment lines before handing the script to bash.
for step_name in \
    "Check review-override label" \
    "Fetch PR head commit without checkout" \
    "Determine applicable rules" \
    "Snapshot trusted rules and compute PR diff"; do
    RUN_BODY=$(extract_run_body "$step_name" "$WORKFLOW")
    NON_EMPTY=$(printf '%s\n' "$RUN_BODY" | grep -Ec '[^[:space:]]' || true)
    check "run block exists: $step_name" "$([[ "$NON_EMPTY" -gt 0 ]] && echo present || echo missing)" "present"
    check "no inline \${{ }} in raw run block: $step_name" "$(printf '%s\n' "$RUN_BODY" | grep -Ec '\$\{\{' || true)" "0"
done

# End-to-end behavioural test of the extracted shell steps against a temporary
# origin + clone repo with a simulated fork PR ref (refs/pull/42/head). Proves
# the live workflow commands compute the PR diff and rule snapshots while
# keeping untrusted fork files (.mcp.json, modified .claude/settings.json,
# weakened .claude/rules/testing.md) off the workspace filesystem.
echo "--- workspace isolation behavioural test"
ISO_DIR="$TEST_TMP/iso"
ORIGIN_REPO="$ISO_DIR/origin.git"
SEED_REPO="$ISO_DIR/seed"
CLONE_REPO="$ISO_DIR/workspace"
mkdir -p "$ISO_DIR"
export GIT_CONFIG_GLOBAL=/dev/null
git init --bare -q -b main "$ORIGIN_REPO"
git init -q -b main "$SEED_REPO"
git -C "$SEED_REPO" config user.email "test@example.com"
git -C "$SEED_REPO" config user.name "Test"
git -C "$SEED_REPO" config commit.gpgsign false
mkdir -p "$SEED_REPO/.claude/rules" "$SEED_REPO/crates/core/src"
printf '%s\n' '{"trusted":true}' > "$SEED_REPO/.claude/settings.json"
printf '%s\n' '# base git-workflow rule' > "$SEED_REPO/.claude/rules/git-workflow.md"
printf '%s\n' '# base code-style rule' > "$SEED_REPO/.claude/rules/code-style.md"
printf '%s\n' '# base testing rule' > "$SEED_REPO/.claude/rules/testing.md"
printf '%s\n' 'pub fn hello() {}' > "$SEED_REPO/crates/core/src/lib.rs"
git -C "$SEED_REPO" add -A
git -C "$SEED_REPO" commit -q -m "base commit"
BASE_SHA=$(git -C "$SEED_REPO" rev-parse HEAD)
git -C "$SEED_REPO" remote add origin "$ORIGIN_REPO"
git -C "$SEED_REPO" push -q origin main

# Create a fork PR commit reachable ONLY via refs/pull/42/head (not on any branch)
# that adds untrusted .mcp.json and modifies .claude/settings.json,
# .claude/rules/testing.md, and a Rust file.
git -C "$SEED_REPO" checkout -q -b fork-pr
printf '%s\n' '{"mcpServers":{"evil":{"command":"sh"}}}' > "$SEED_REPO/.mcp.json"
printf '%s\n' '{"hooks":{"PreToolUse":[{"command":"id"}]}}' > "$SEED_REPO/.claude/settings.json"
printf '%s\n' '# weakened fork testing rule' > "$SEED_REPO/.claude/rules/testing.md"
printf '%s\n' 'pub fn hello() { let _x = 1; }' > "$SEED_REPO/crates/core/src/lib.rs"
git -C "$SEED_REPO" add -A
git -C "$SEED_REPO" commit -q -m "fork pr commit"
HEAD_SHA=$(git -C "$SEED_REPO" rev-parse HEAD)
git -C "$SEED_REPO" push -q origin "HEAD:refs/pull/42/head"

# Clone via --no-local so only advertised branches are fetched (mirrors
# actions/checkout default on pull_request_target without copying refs/pull/*).
git clone --no-local -q "$ORIGIN_REPO" "$CLONE_REPO"
check "fork commit absent before fetch step" "$(git -C "$CLONE_REPO" cat-file -e "${HEAD_SHA}^{commit}" 2>/dev/null && echo present || echo absent)" "absent"

extract_run_body "Check review-override label" "$WORKFLOW" > "$ISO_DIR/override.sh"
extract_run_body "Fetch PR head commit without checkout" "$WORKFLOW" > "$ISO_DIR/fetch.sh"
extract_run_body "Determine applicable rules" "$WORKFLOW" > "$ISO_DIR/rules.sh"
extract_run_body "Snapshot trusted rules and compute PR diff" "$WORKFLOW" > "$ISO_DIR/snapshot.sh"

# Check review-override label handles exact match, substring non-match, and shell chars.
OUT_EXACT="$ISO_DIR/out_exact"
: > "$OUT_EXACT"
LABELS='bug,review-override,ci' GITHUB_OUTPUT="$OUT_EXACT" bash --noprofile --norc -eo pipefail "$ISO_DIR/override.sh"
check "override step matches exact review-override label" "$(cat "$OUT_EXACT")" "overridden=true"

OUT_SUBSTR="$ISO_DIR/out_substr"
PWN_MARKER="$ISO_DIR/pwned"
: > "$OUT_SUBSTR"
LABELS="my-review-override-test,\$(touch $PWN_MARKER)" GITHUB_OUTPUT="$OUT_SUBSTR" bash --noprofile --norc -eo pipefail "$ISO_DIR/override.sh"
check "override step rejects substring label and shell metachars" "$(cat "$OUT_SUBSTR")" "overridden=false"
check "override step did not execute shell metacharacters" "$([[ -e "$PWN_MARKER" ]] && echo executed || echo clean)" "clean"

# Run fetch, rules, and snapshot steps inside the default-branch workspace.
(
    cd "$CLONE_REPO"
    BASE_SHA="$BASE_SHA" HEAD_SHA="$HEAD_SHA" PR_NUMBER="42" bash --noprofile --norc -eo pipefail "$ISO_DIR/fetch.sh"
)
check "fork commit present after fetch step" "$(git -C "$CLONE_REPO" cat-file -e "${HEAD_SHA}^{commit}" 2>/dev/null && echo present || echo absent)" "present"
check "fork .mcp.json is NOT checked out into workspace" "$([[ -e "$CLONE_REPO/.mcp.json" ]] && echo present || echo absent)" "absent"
check "workspace .claude/settings.json stays on trusted base copy" "$(cat "$CLONE_REPO/.claude/settings.json")" '{"trusted":true}'
check "workspace .claude/rules/testing.md stays on trusted base copy" "$(cat "$CLONE_REPO/.claude/rules/testing.md")" "# base testing rule"
check "workspace HEAD stays on base commit" "$(git -C "$CLONE_REPO" rev-parse HEAD)" "$BASE_SHA"

OUT_RULES="$ISO_DIR/out_rules"
: > "$OUT_RULES"
(
    cd "$CLONE_REPO"
    BASE_SHA="$BASE_SHA" HEAD_SHA="$HEAD_SHA" GITHUB_OUTPUT="$OUT_RULES" bash --noprofile --norc -eo pipefail "$ISO_DIR/rules.sh" > /dev/null
)
RULE_NAMES=$(sed -n 's/^rule_names=//p' "$OUT_RULES")
check "rules step detects git-workflow, code-style, and testing rules" "$RULE_NAMES" "git-workflow.md code-style.md testing.md"

# Verify Determine applicable rules fails closed (non-zero, no skip=true) on a
# broken git diff (e.g., invalid commit SHA / unrelated history).
OUT_BAD_RULES="$ISO_DIR/out_bad_rules"
: > "$OUT_BAD_RULES"
BAD_RC=0
(
    cd "$CLONE_REPO"
    BASE_SHA="0000000000000000000000000000000000000000" HEAD_SHA="$HEAD_SHA" \
        GITHUB_OUTPUT="$OUT_BAD_RULES" bash --noprofile --norc -eo pipefail "$ISO_DIR/rules.sh" >/dev/null 2>&1
) || BAD_RC=$?
check "rules step fails closed on git diff error (non-zero exit)" "$([[ "$BAD_RC" -ne 0 ]] && echo failed || echo passed)" "failed"
check "rules step does not emit skip=true on git diff error" "$(grep -Ec '^skip=true$' "$OUT_BAD_RULES" || true)" "0"

OUT_SNAP="$ISO_DIR/out_snap"
: > "$OUT_SNAP"
(
    cd "$CLONE_REPO"
    GITHUB_WORKSPACE="$CLONE_REPO" BASE_REF="main" BASE_SHA="$BASE_SHA" HEAD_SHA="$HEAD_SHA" \
        PR_TITLE='fix: test $(uname) `id`' RULE_NAMES="$RULE_NAMES" \
        GITHUB_OUTPUT="$OUT_SNAP" bash --noprofile --norc -eo pipefail "$ISO_DIR/snapshot.sh" > /dev/null
)
check "snapshot writes PR title verbatim to pr-title.txt" "$(cat "$CLONE_REPO/.pr-review-tmp/pr-title.txt")" 'fix: test $(uname) `id`'
check "snapshot diff contains fork Rust change" "$(grep -Ec 'let _x = 1;' "$CLONE_REPO/.pr-review-tmp/diff.patch" || true)" "1"
check "snapshot rules come from trusted base ref (not weakened fork rule)" "$(cat "$CLONE_REPO/.pr-review-tmp/rules/testing.md")" "# base testing rule"
check "fork .mcp.json still absent after snapshot step" "$([[ -e "$CLONE_REPO/.mcp.json" ]] && echo present || echo absent)" "absent"

# --- wiring: a guard nobody runs is not a guard -------------------------------
# Anchored to a real `run:` line, so a commented-out or no-op step that merely
# names the script cannot satisfy it.
if grep -Eq '^[[:space:]]+run:[[:space:]]+bash scripts/claude_pr_review_post_step_test\.sh[[:space:]]*$' "$CI_YML"; then
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

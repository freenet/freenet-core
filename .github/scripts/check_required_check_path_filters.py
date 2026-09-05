#!/usr/bin/env python3
"""Guard against a path filter making a PR structurally unmergeable.

A workflow-level ``paths:`` / ``paths-ignore:`` filter does not report a
skipped check -- it reports NOTHING. GitHub treats a required status context
that was never reported as **expected**, not as satisfied, so the merge queue
refuses the pull request entry. Forever. There is no way around it short of
adding an unrelated file to the PR.

Shipped in ``ci.yml`` and ``claude-pr-review.yml`` as a cost optimization
("skip CI for docs-only changes"). Seven of the eight required contexts came
from those two workflows, so a PR whose every file matched ``docs/**`` could
never merge. #5322 sat blocked from 2026-08-14 until #5451 removed the filters.

Two things made it hard to spot:

* ``*.md`` does NOT match at depth in GitHub path filters, so a PR touching
  ``.claude/rules/foo.md`` still ran the full suite and merged normally
  (#5375). "Docs PRs merge here all the time" was true and did not cover it.
* ``ci.yml`` also triggers on ``merge_group``, which reads like a rescue. It is
  not: a merge_group run only happens for a PR that has already been ADMITTED
  to the queue, and admission is exactly what was refused.

The knowledge was not even new -- ``macos-dmg-swap-e2e.yml`` carries a comment
saying precisely this above its own filter-free ``on:`` block. A comment in one
file protected nothing in the other two. Hence this linter.

Scope: a workflow is in scope only if it defines a job whose reported context
name is in ``REQUIRED_CONTEXTS`` below. Other workflows may filter freely --
that is a legitimate and common optimization when no required check is at stake.

The ``push:`` trigger is deliberately NOT checked. A push run to main happens
after the merge; nothing blocks on it, so filtering it is free.

Usage:
    check_required_check_path_filters.py [WORKFLOW_DIR]
    check_required_check_path_filters.py --self-test
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

# The contexts branch protection requires on the default branch.
#
# Re-derive with:
#   gh api repos/freenet/freenet-core/rulesets/4028534 \
#     --jq '.rules[] | select(.type=="required_status_checks")
#           | .parameters.required_status_checks[].context'
#
# Drift is one-sided in its danger. If a context is REMOVED from the ruleset and
# left here, this linter merely refuses a path filter that would now be safe --
# noisy, not harmful. If a context is ADDED to the ruleset and not added here,
# the new context is unguarded. Update this list whenever the ruleset changes.
REQUIRED_CONTEXTS = frozenset(
    {
        "Clippy",
        "Fmt",
        "Unit & Integration",
        "Simulation",
        "NAT Validation",
        "Rule Lint",
        "Claude Rule Review",
        "macOS bundle-swap E2E",
    }
)

# Triggers whose runs gate a pull request. `push` is excluded on purpose (see
# the module docstring).
GATING_TRIGGERS = ("pull_request", "pull_request_target")

# `jobs:` at column 0 ends the `on:` section for our purposes and begins the
# region where job names live.
JOBS_RE = re.compile(r"^jobs:\s*(?:#.*)?$")

# A job key is `  <id>:` at exactly two-space indent inside `jobs:`; its display
# name, when set, is `    name: ...` at exactly four. The context GitHub reports
# is the `name:` if present, otherwise the job id.
JOB_ID_RE = re.compile(r"^  (?P<id>[A-Za-z_][A-Za-z0-9_-]*):\s*(?:#.*)?$")
JOB_NAME_RE = re.compile(r"^    name:\s*(?P<name>.+?)\s*$")

# `  pull_request:` / `  pull_request_target:` at exactly two-space indent is a
# trigger under `on:`. Deeper indents are something else.
TRIGGER_RE = re.compile(r"^  (?P<trigger>[A-Za-z_][A-Za-z0-9_]*):\s*(?:#.*)?$")

# `    paths:` / `    paths-ignore:` at exactly four-space indent is a filter on
# the enclosing trigger.
FILTER_RE = re.compile(r"^    (?P<key>paths|paths-ignore):\s*(?:#.*)?$")


def _jobs_line(lines: list[str]) -> int | None:
    """Index of the top-level `jobs:` line, or None if absent."""
    for i, line in enumerate(lines):
        if JOBS_RE.match(line):
            return i
    return None


def reported_contexts(text: str) -> set[str]:
    """The status-check contexts this workflow's jobs report.

    A job reports under its `name:` when it has one, otherwise under its job id.
    Both are collected: either could be what branch protection names.
    """
    lines = text.splitlines()
    jobs_at = _jobs_line(lines)
    if jobs_at is None:
        return set()

    contexts: set[str] = set()
    current_id: str | None = None
    for line in lines[jobs_at + 1 :]:
        if line and not line[0].isspace():
            # A new top-level key ends the jobs block.
            break
        job = JOB_ID_RE.match(line)
        if job:
            current_id = job.group("id")
            contexts.add(current_id)
            continue
        named = JOB_NAME_RE.match(line)
        if named and current_id is not None:
            contexts.add(named.group("name").strip("'\""))
    return contexts


def check_source(text: str, name: str) -> list[str]:
    """Return human-readable errors for one workflow's source."""
    guarded = reported_contexts(text) & REQUIRED_CONTEXTS
    if not guarded:
        return []

    lines = text.splitlines()
    jobs_at = _jobs_line(lines)
    on_end = jobs_at if jobs_at is not None else len(lines)

    errors = []
    current_trigger: str | None = None
    for i in range(on_end):
        line = lines[i]
        trigger = TRIGGER_RE.match(line)
        if trigger:
            current_trigger = trigger.group("trigger")
            continue
        filt = FILTER_RE.match(line)
        if filt and current_trigger in GATING_TRIGGERS:
            errors.append(
                f"{name}:{i + 1}: `{filt.group('key')}:` on the "
                f"`{current_trigger}:` trigger of a workflow that provides "
                f"required status check(s) {sorted(guarded)}.\n"
                f"    A workflow skipped by a path filter reports NOTHING, and "
                f"GitHub treats a never-reported required context as EXPECTED "
                f"rather than satisfied, so any PR the filter excludes can "
                f"never enter the merge queue (#5451; #5322 was blocked for "
                f"three weeks by exactly this).\n"
                f"    A `merge_group:` trigger does not rescue it: that only "
                f"runs for a PR already admitted to the queue.\n"
                f"    Filter at the STEP level instead (see the `skip_check` "
                f"steps in ci.yml), so the job still runs and still reports."
            )
    return errors


def check_dir(workflow_dir: Path) -> list[str]:
    errors = []
    for path in sorted(
        p for p in workflow_dir.iterdir() if p.suffix in (".yml", ".yaml")
    ):
        errors.extend(check_source(path.read_text(encoding="utf-8"), str(path)))
    return errors


# --- self-test ---------------------------------------------------------------

# The real pre-fix ci.yml shape. Must be flagged once, for `pull_request` — and
# NOT for the `push` filter above it.
_BAD_CI = """\
name: CI
on:
  push:
    branches: [main]
    paths-ignore:
      - 'docs/**'
  pull_request:
    paths-ignore:
      - 'docs/**'
      - '*.md'
  merge_group:

jobs:
  fmt_check:
    name: Fmt
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v7
"""

# The real pre-fix claude-pr-review.yml shape: pull_request_target, with a
# `types:` key sitting between the trigger and the filter. Must be flagged.
_BAD_TARGET = """\
name: Claude PR Rule Review
on:
  pull_request_target:
    types: [opened, synchronize]
    paths-ignore:
      - 'docs/**'
  merge_group:

jobs:
  rule-review:
    name: Claude Rule Review
    runs-on: ubuntu-latest
"""

# An allowlist filter is exactly as dangerous as a denylist one: every PR
# OUTSIDE the list is then the one that can never merge. Must be flagged.
_BAD_PATHS_ALLOWLIST = """\
name: CI
on:
  pull_request:
    paths:
      - 'crates/**'

jobs:
  clippy_check:
    name: Clippy
    runs-on: ubuntu-latest
"""

# The fix. Must NOT be flagged: the `push` filter is free (nothing gates on a
# post-merge run), and the gating trigger carries no filter.
_GOOD_CI = """\
name: CI
on:
  push:
    branches: [main]
    paths-ignore:
      - 'docs/**'
      - '*.md'
  pull_request:
  merge_group:

jobs:
  fmt_check:
    name: Fmt
    runs-on: ubuntu-latest
  rule_lint:
    name: Rule Lint
    runs-on: ubuntu-latest
"""

# A workflow that provides no required context may filter freely — flagging it
# would make this linter noise rather than signal.
_GOOD_NOT_REQUIRED = """\
name: Benchmarks
on:
  pull_request:
    paths:
      - 'crates/core/benches/**'

jobs:
  bench:
    name: Run benchmarks
    runs-on: ubuntu-latest
"""

# A job with no `name:` reports under its job id, so the id must be matched
# against the required list too. Must be flagged.
_BAD_JOB_ID_CONTEXT = """\
name: Odd
on:
  pull_request:
    paths-ignore:
      - 'docs/**'

jobs:
  Simulation:
    runs-on: ubuntu-latest
"""

# `paths` appearing as a STEP input, not a trigger filter. Must NOT be flagged:
# it sits past `jobs:` and at a deeper indent.
_GOOD_PATHS_AS_STEP_INPUT = """\
name: CI
on:
  pull_request:

jobs:
  fmt_check:
    name: Fmt
    runs-on: ubuntu-latest
    steps:
      - uses: actions/upload-artifact@v4
        with:
          paths: |
            target/debug
"""

# The `on: [pull_request]` inline-list form carries no filter and cannot.
# Must NOT be flagged.
_GOOD_INLINE_TRIGGER_LIST = """\
name: CI
on: [pull_request, merge_group]

jobs:
  clippy_check:
    name: Clippy
    runs-on: ubuntu-latest
"""

_CASES = [
    ("pre-fix ci.yml pull_request filter", _BAD_CI, 1),
    ("pre-fix claude-pr-review.yml filter", _BAD_TARGET, 1),
    ("allowlist `paths:` on a required workflow", _BAD_PATHS_ALLOWLIST, 1),
    ("fixed ci.yml (push filter kept)", _GOOD_CI, 0),
    ("workflow with no required context", _GOOD_NOT_REQUIRED, 0),
    ("required context from a bare job id", _BAD_JOB_ID_CONTEXT, 1),
    ("`paths:` as a step input", _GOOD_PATHS_AS_STEP_INPUT, 0),
    ("inline `on: [pull_request]` list", _GOOD_INLINE_TRIGGER_LIST, 0),
]


def self_test() -> int:
    failures = 0
    for desc, source, expected in _CASES:
        found = len(check_source(source, "<fixture>"))
        if found == expected:
            print(f"ok   - {desc}")
        else:
            print(
                f"FAIL - {desc}: expected {expected} error(s), got {found}",
                file=sys.stderr,
            )
            failures += 1
    if failures:
        print(f"\n{failures} self-test failure(s)", file=sys.stderr)
        return 1
    print(f"\nAll {len(_CASES)} self-tests passed")
    return 0


def main(argv: list[str]) -> int:
    if "--self-test" in argv:
        return self_test()

    workflow_dir = Path(argv[1]) if len(argv) > 1 else Path(".github/workflows")
    if not workflow_dir.is_dir():
        print(f"ERROR: {workflow_dir} is not a directory", file=sys.stderr)
        return 2

    errors = check_dir(workflow_dir)
    if errors:
        print(
            "ERROR: required-check path-filter check failed "
            "(see #5451 for what this prevents)"
        )
        for err in errors:
            print(f"  - {err}")
        return 1

    print("required-check path-filter check passed")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))

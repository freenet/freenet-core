#!/usr/bin/env python3
"""Guard against merge-queue entries cancelling each other's required checks.

A workflow that triggers on ``merge_group`` runs once per merge-queue entry.
Its ``concurrency.group`` key must therefore contain something that VARIES per
entry. If it does not, every entry lands in the same group, and with
``cancel-in-progress: true`` each new entry cancels the previous entry's run.
When the workflow provides a REQUIRED check, a cancelled run never reports, so
the older queue entry goes ``UNMERGEABLE`` and stalls the queue behind it.

The trap is that this looks fine on a PR. ``github.event.pull_request`` simply
does not exist on a ``merge_group`` event, so a key like::

    group: claude-pr-review-${{ github.event.pull_request.number }}

silently collapses to the constant ``claude-pr-review-`` -- GitHub's own
cancellation message names that empty group verbatim. Nothing fails, nothing is
logged on the PR, and the only symptom is a queue entry that never advances.

Shipped in ``claude-pr-review.yml`` and found 2026-08-04 when three PRs were
queued together for the 0.2.120 release. Fixed by #5170.

A key is accepted when it references any token that differs between queue
entries: ``merge_group`` (``merge_group.head_sha`` is the queue entry's own
commit), ``github.ref`` (on a merge_group event this is the per-entry
``gh-readonly-queue/...`` branch), ``github.sha``, or ``github.run_id``.

Usage:
    check_merge_group_concurrency.py [WORKFLOW_DIR]
    check_merge_group_concurrency.py --self-test
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

# A `group:` key is per-entry-safe if it mentions any of these. `github.ref` and
# `github.sha` both resolve to per-entry values on a merge_group event, and
# `run_id` is unique per run, so any of them breaks the collapse.
PER_ENTRY_TOKENS = ("merge_group", "github.ref", "github.sha", "github.run_id")

# `  merge_group:` at exactly two-space indent is a trigger under `on:`. Deeper
# indents are payload references (`github.event.merge_group.head_sha`), not
# triggers, so anchoring the indent avoids matching a workflow that merely reads
# the payload without being triggered by the event.
MERGE_GROUP_TRIGGER_RE = re.compile(r"^  merge_group:\s*(?:#.*)?$", re.M)

# Capture each `concurrency:` block's `group:` line. GitHub allows `concurrency`
# at workflow level and per job, so scan for the key rather than assuming
# top-level, and keep the indent so the message can point at the right one.
CONCURRENCY_GROUP_RE = re.compile(
    r"^(?P<indent>[ ]*)concurrency:\s*(?:#.*)?\n"
    r"(?:^(?P=indent)[ ]+.*\n)*?"  # any keys before `group:` (e.g. `cancel-in-progress`)
    r"^(?P=indent)[ ]+group:[ ]*(?P<group>.*)$",
    re.M,
)


def _line_of(text: str, offset: int) -> int:
    return text.count("\n", 0, offset) + 1


def check_source(text: str, name: str) -> list[str]:
    """Return a list of human-readable errors for one workflow's source."""
    if not MERGE_GROUP_TRIGGER_RE.search(text):
        return []

    errors = []
    for match in CONCURRENCY_GROUP_RE.finditer(text):
        group = match.group("group").strip()
        if any(token in group for token in PER_ENTRY_TOKENS):
            continue
        line = _line_of(text, match.start("group"))
        errors.append(
            f"{name}:{line}: workflow triggers on `merge_group` but its "
            f"concurrency group does not vary per queue entry:\n"
            f"      group: {group}\n"
            f"    Every queue entry lands in the same group, so with "
            f"cancel-in-progress each new entry cancels the previous entry's "
            f"run. If this workflow provides a required check, the older entry "
            f"goes UNMERGEABLE and stalls the queue (see #5170).\n"
            f"    Add a fallback that differs per entry, e.g.\n"
            f"      ${{{{ github.event.pull_request.number || "
            f"github.event.merge_group.head_sha || github.run_id }}}}"
        )
    return errors


def check_dir(workflow_dir: Path) -> list[str]:
    errors = []
    paths = sorted(
        p for p in workflow_dir.iterdir() if p.suffix in (".yml", ".yaml")
    )
    for path in paths:
        errors.extend(
            check_source(path.read_text(encoding="utf-8"), str(path))
        )
    return errors


# --- self-test ---------------------------------------------------------------

# The real pre-fix key from claude-pr-review.yml. Must be flagged.
_BAD = """\
name: Bad
on:
  pull_request_target:
    types: [opened]
  merge_group:

concurrency:
  group: claude-pr-review-${{ github.event.pull_request.number }}
  cancel-in-progress: true
"""

# The fix. Must NOT be flagged.
_GOOD_FALLBACK = """\
name: Good
on:
  pull_request_target:
  merge_group:

concurrency:
  group: claude-pr-review-${{ github.event.pull_request.number || github.event.merge_group.head_sha || github.run_id }}
  cancel-in-progress: true
"""

# The repo's other merge_group workflows key on github.ref, which is the
# per-entry queue branch. Must NOT be flagged, or this linter would demand a
# pointless change to ci.yml and macos-dmg-swap-e2e.yml.
_GOOD_REF = """\
name: CI
on:
  pull_request:
  merge_group:

concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: true
"""

# No merge_group trigger, so a constant key is the author's business. Must NOT
# be flagged -- over-reach here would make the linter noise rather than signal.
_GOOD_NO_MERGE_GROUP = """\
name: Nightly
on:
  schedule:
    - cron: '0 3 * * *'

concurrency:
  group: netcheck-nightly
"""

# `merge_group` appearing ONLY as a payload reference is not a trigger. Must NOT
# be flagged: the workflow never runs on a merge_group event.
_GOOD_PAYLOAD_ONLY = """\
name: Reads payload
on:
  pull_request:

concurrency:
  group: reads-payload-${{ github.event.pull_request.number }}
jobs:
  x:
    steps:
      - run: echo "${{ github.event.merge_group.head_commit.message }}"
"""

# `cancel-in-progress` before `group:` -- the regex must not depend on key order.
_BAD_KEY_ORDER = """\
name: Bad, reordered
on:
  merge_group:

concurrency:
  cancel-in-progress: true
  group: fixed-${{ github.event.pull_request.number }}
"""

# Job-level concurrency, not workflow-level. Must still be flagged.
_BAD_JOB_LEVEL = """\
name: Bad, job level
on:
  merge_group:

jobs:
  review:
    concurrency:
      group: review-${{ github.event.pull_request.number }}
      cancel-in-progress: true
"""

_CASES = [
    ("pre-fix claude-pr-review key", _BAD, 1),
    ("the fallback chain", _GOOD_FALLBACK, 0),
    ("github.ref key (ci.yml pattern)", _GOOD_REF, 0),
    ("no merge_group trigger", _GOOD_NO_MERGE_GROUP, 0),
    ("merge_group only as payload ref", _GOOD_PAYLOAD_ONLY, 0),
    ("group after cancel-in-progress", _BAD_KEY_ORDER, 1),
    ("job-level concurrency", _BAD_JOB_LEVEL, 1),
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
            "ERROR: merge-queue concurrency check failed "
            "(see #5170 for what this prevents)"
        )
        for err in errors:
            print(f"  - {err}")
        return 1

    print("merge-queue concurrency check passed")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))

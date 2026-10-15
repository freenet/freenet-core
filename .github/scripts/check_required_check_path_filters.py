#!/usr/bin/env python3
"""Guard the invariant that every required status check reports on every PR.

A workflow-level ``paths:`` / ``paths-ignore:`` filter does not report a
skipped check -- it reports NOTHING. GitHub treats a required status context
that was never reported as **expected**, not as satisfied, so the merge queue
refuses the pull request entry. Ordinary contributors have no way around it
short of adding an unrelated file to the PR (a ruleset bypass actor or an admin
merge could still force it through).

Shipped in ``ci.yml`` and ``claude-pr-review.yml`` as a cost optimization
("skip CI for docs-only changes"). Seven of the eight required contexts came
from those two workflows, so a PR whose every file matched ``docs/**`` could
never merge. #5322 sat blocked from 2026-08-14 until #5571 removed the filters.

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

It makes three checks, because a path filter is only one of the ways a required
context stops reporting or stops being informative:

1. **No path filter on a gating trigger.** A workflow is in scope only if it
   defines a job whose reported context is in ``REQUIRED_CONTEXTS``. Other
   workflows may filter freely -- that is a legitimate optimization when no
   required check is at stake. The ``push:`` trigger is deliberately NOT
   checked: a push run to main happens after the merge and gates nothing.

2. **Coverage** (on by default; ``--no-coverage-check`` for fixture scans).
   Every context in ``REQUIRED_CONTEXTS`` must be produced by some workflow in
   the directory. Without this, check 1 is a signal that can go vacuously
   green: rename the ``Fmt`` job to ``Format`` and the required context ``Fmt``
   never reports, every PR is blocked with the #5322 symptom, and a
   filter-only linter sees nothing wrong. The same assertion also catches the
   linter losing track of a file it is supposed to be guarding -- a
   reindentation, a flow-style ``on:`` mapping, a job ``name:`` this parser
   cannot read -- all of which would otherwise silently narrow its scope to
   nothing.

3. **No job ``needs:`` a required-context job** (#5621), except the observers in
   ``NEEDS_ALLOWLIST``. A job whose ``needs:`` ancestor fails is reported
   SKIPPED, which renders grey and reads as "not applicable". ``Fmt`` used to
   gate six heavy jobs this way, so one misplaced line wrap left a release gate
   with no test signal that looked like a single red check. A required context
   already blocks merge on its own; a ``needs:`` edge on it adds nothing but
   that ambiguity.

Parser scope, stated because check 1's message reads like total coverage and is
not: block- and flow-style filters are both detected, at the 2-space trigger /
4-space key indentation every workflow in this repo uses. A workflow written
with 4-space YAML indentation or a flow-style ``on:`` mapping is not parsed.
Check 3 additionally fails closed on a job key it cannot read (a quoted id, a
flow-mapping job), because a job it cannot see is a job it cannot check. Check 2 is the backstop for exactly
that: a required-context workflow this parser cannot read drops out of the
coverage set and fails.

Usage:
    check_required_check_path_filters.py [WORKFLOW_DIR] [--no-coverage-check]
    check_required_check_path_filters.py --self-test
"""

from __future__ import annotations

import re
import sys
import tempfile
from pathlib import Path

# The contexts branch protection requires on the default branch.
#
# Re-derive with:
#   gh api repos/freenet/freenet-core/rulesets/4028534 \
#     --jq '.rules[] | select(.type=="required_status_checks")
#           | .parameters.required_status_checks[].context'
#
# Three ways this drifts, only the first of which is harmless:
#
#   * A context REMOVED from the ruleset and left here -- the linter refuses a
#     path filter that would now be safe. Noisy, not dangerous.
#   * A context ADDED to the ruleset and not added here -- that context is
#     unguarded by check 1. `playwright-shell.yml` is the live candidate: it is
#     path-filtered and its own comment contemplates being promoted to required.
#   * A job RENAMED in a workflow without the ruleset following -- the ruleset
#     then requires a context nothing reports (the #5322 symptom by another
#     route) AND this file's scope silently shrinks, two failures from one
#     cause. The coverage check exists for this one and fails closed on it.
#
# Update this list whenever the ruleset changes.
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

# Jobs allowed to `needs:` a required-context job. Both are failure OBSERVERS:
# they run only when a dependency fails (`if: failure()`), so the edge is what
# makes them fire at all, and neither produces test signal that could be hidden.
NEEDS_ALLOWLIST = frozenset({"claude-ci-analysis", "notify_dev_room_main_failure"})

# `    needs:` at four-space indent inside a job. Its VALUE is everything up to the
# next job key: the rest of that line, plus any following blank, comment or
# more-indented line, plus a `- item` sequence written at the key's own indent.
# That covers inline, flow (single- or multi-line), quoted, commented and block
# forms without trying to parse YAML structure -- the failure mode of a stricter
# regex is to go quiet on a valid form, and this check must not go quiet.
NEEDS_RE = re.compile(r"^    ['\"]?needs['\"]?\s*:(?P<value>.*)$")
IDENT_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_-]*")

# Triggers whose runs gate a pull request. `push` is excluded on purpose (see
# the module docstring).
GATING_TRIGGERS = ("pull_request", "pull_request_target")

# `jobs:` at column 0 -- everything indented under it is a job, everything
# outside it is trigger/config territory.
JOBS_RE = re.compile(r"^jobs:\s*(?:#.*)?$")

# A job key is `  <id>:` at exactly two-space indent inside `jobs:`; its display
# name, when set, is `    name: ...` at exactly four. The context GitHub reports
# is the `name:` if present, otherwise the job id.
JOB_ID_RE = re.compile(r"^  (?P<id>[A-Za-z_][A-Za-z0-9_-]*):\s*(?:#.*)?$")
JOB_NAME_RE = re.compile(r"^    name:\s*(?P<name>[^#]+?)\s*(?:#.*)?$")

# `  pull_request:` / `  pull_request_target:` at exactly two-space indent is a
# trigger under `on:`. Deeper indents are something else.
TRIGGER_RE = re.compile(r"^  (?P<trigger>[A-Za-z_][A-Za-z0-9_]*):\s*(?:#.*)?$")

# `    paths:` / `    paths-ignore:` at exactly four-space indent is a filter on
# the enclosing trigger. The value is matched loosely on purpose: the block form
# puts it on following lines, but `paths-ignore: ['docs/**', '*.md']` is equally
# valid YAML and equally capable of reintroducing #5451 -- and the flow style is
# already this repo's house style for the sibling key (`branches: [main]`), so
# it is the form someone re-adding the optimization is most likely to write.
FILTER_RE = re.compile(r"^    (?P<key>paths|paths-ignore):(?:\s.*)?$")


def _jobs_line(lines: list[str]) -> int | None:
    """Index of the top-level `jobs:` line, or None if absent."""
    for i, line in enumerate(lines):
        if JOBS_RE.match(line):
            return i
    return None


def _jobs_block(lines: list[str]) -> tuple[int, int]:
    """The [start, end) line range of the `jobs:` block's body."""
    jobs_at = _jobs_line(lines)
    if jobs_at is None:
        return len(lines), len(lines)
    end = len(lines)
    for i in range(jobs_at + 1, len(lines)):
        if (
            lines[i]
            and not lines[i][0].isspace()
            and not lines[i].startswith("#")
        ):
            end = i
            break
    return jobs_at + 1, end


def reported_contexts(text: str) -> set[str]:
    """The status-check contexts this workflow's jobs report.

    A job reports under its `name:` when it has one, otherwise under its job id.
    Both are collected: either could be what branch protection names.
    """
    lines = text.splitlines()
    start, end = _jobs_block(lines)

    contexts: set[str] = set()
    current_id: str | None = None
    for line in lines[start:end]:
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
    jobs_start, jobs_end = _jobs_block(lines)

    errors = []
    current_trigger: str | None = None
    for i, line in enumerate(lines):
        # YAML mappings are unordered, so `on:` may legally sit after `jobs:`.
        # Scan everything OUTSIDE the jobs block rather than only above it.
        if jobs_start <= i < jobs_end:
            continue
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
                f"never enter the merge queue (#5451; #5322 was blocked by "
                f"exactly this from 2026-08-14 until #5571).\n"
                f"    A `merge_group:` trigger does not rescue it: that only "
                f"runs for a PR already admitted to the queue.\n"
                f"    If the cost is worth it, gate the expensive STEPS on a "
                f"changed-files check instead, so the job still runs and still "
                f"reports. `ci.yml`'s `skip_check` steps are the shape to "
                f"follow, though they key on the event rather than on paths, "
                f"so a path version needs its own diff step."
            )
    return errors


def _bracket_depth(text: str) -> int:
    """Net count of opened-minus-closed flow brackets, ignoring comments."""
    code = text.split("#", 1)[0]
    return code.count("[") + code.count("{") - code.count("]") - code.count("}")


def check_needs(text: str, name: str) -> list[str]:
    """Errors for jobs that `needs:` a required-context job (see check 3)."""
    lines = text.splitlines()
    start, end = _jobs_block(lines)

    # Pass 1: which job ids report a required context.
    required_ids: set[str] = set()
    current_id: str | None = None
    for line in lines[start:end]:
        job = JOB_ID_RE.match(line)
        if job:
            current_id = job.group("id")
            if current_id in REQUIRED_CONTEXTS:
                required_ids.add(current_id)
            continue
        named = JOB_NAME_RE.match(line)
        if named and current_id is not None:
            if named.group("name").strip("'\"") in REQUIRED_CONTEXTS:
                required_ids.add(current_id)

    # Pass 2: every `needs:` entry, with the job it belongs to.
    errors = []
    current_id = None
    i = start
    while i < end:
        line = lines[i]
        job = JOB_ID_RE.match(line)
        if job:
            current_id = job.group("id")
            i += 1
            continue
        if re.match(r"^  \S", line) and not line.lstrip().startswith("#"):
            # A job key we cannot read (quoted id, flow-mapping job, ...).
            # Staying silent would also leave `current_id` pointing at the
            # PREVIOUS job, which could be an allowlisted observer.
            current_id = None
            errors.append(
                f"{name}:{i + 1}: cannot parse this job key, so its `needs:` "
                f"cannot be checked against required status contexts. Write "
                f"it as a plain `  <id>:` line."
            )
            i += 1
            continue
        needs = NEEDS_RE.match(line)
        if not needs:
            i += 1
            continue

        needs_at = i
        span = [needs.group("value")]
        # Unclosed `[` / `{` means a multi-line flow collection, whose
        # continuation lines may sit at ANY indent, including the key's own.
        depth = _bracket_depth(needs.group("value"))
        i += 1
        while i < end:
            nxt = lines[i]
            stripped = nxt.strip()
            indent = len(nxt) - len(nxt.lstrip())
            if (
                depth > 0
                or not stripped
                or stripped.startswith("#")
                or indent > 4
                or (indent == 4 and stripped.startswith("-"))
            ):
                depth += _bracket_depth(nxt)
                span.append(nxt)
                i += 1
            else:
                break
        if current_id is None or current_id in NEEDS_ALLOWLIST:
            continue
        needed = []
        for chunk in span:
            needed.extend(IDENT_RE.findall(chunk.split("#", 1)[0]))
        for dep in dict.fromkeys(needed):
            if dep in required_ids:
                errors.append(
                    f"{name}:{needs_at + 1}: job `{current_id}` has "
                    f"`needs: {dep}`, and `{dep}` reports a required status "
                    f"context.\n"
                    f"    If `{dep}` fails, `{current_id}` is reported SKIPPED: "
                    f"grey, which reads as 'not applicable' rather than 'never "
                    f"got to run', so a PR can look healthier than it is "
                    f"(#5621: Fmt gated the whole test suite this way).\n"
                    f"    A required context already blocks merge on its own, "
                    f"so the edge adds no safety. Remove it, or if "
                    f"`{current_id}` is a failure observer, add it to "
                    f"NEEDS_ALLOWLIST."
                )
    return errors


def check_dir(workflow_dir: Path, require_coverage: bool = True) -> list[str]:
    errors = []
    covered: set[str] = set()
    for path in sorted(
        p
        for p in workflow_dir.iterdir()
        if p.is_file() and p.suffix in (".yml", ".yaml")
    ):
        text = path.read_text(encoding="utf-8")
        covered |= reported_contexts(text)
        errors.extend(check_source(text, str(path)))
        errors.extend(check_needs(text, str(path)))

    if require_coverage:
        missing = sorted(REQUIRED_CONTEXTS - covered)
        if missing:
            errors.append(
                f"{workflow_dir}: no workflow in this directory reports the "
                f"required status context(s) {missing}.\n"
                f"    A required context that nothing reports blocks EVERY pull "
                f"request, with the same symptom as #5322: the merge queue "
                f"says the check is 'expected' and refuses entry.\n"
                f"    Either the job was renamed or removed without the "
                f"ruleset following, or REQUIRED_CONTEXTS in this script is "
                f"stale, or this script can no longer parse the workflow that "
                f"provides it. Re-derive the list with the `gh api` command in "
                f"the REQUIRED_CONTEXTS comment and reconcile."
            )
    return errors


# --- self-test ---------------------------------------------------------------

# Every `_BAD_*` / `_GOOD_*` fixture below is checked with `check_source`, which
# does not do the coverage check; the coverage check has its own cases at the
# bottom.

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

# The flow-sequence form. Identical effect, one line, and the style this repo
# already uses for `branches: [main]` — so it is the likeliest way the filter
# comes back. Must be flagged.
_BAD_FLOW_SEQUENCE = """\
name: CI
on:
  push:
    branches: [main]
  pull_request:
    paths-ignore: ['docs/**', '*.md']
  merge_group:

jobs:
  fmt_check:
    name: Fmt
    runs-on: ubuntu-latest
"""

# `on:` written after `jobs:`. YAML mappings are unordered, so this is legal and
# the filter is just as live. Must be flagged.
_BAD_ON_AFTER_JOBS = """\
name: CI
jobs:
  fmt_check:
    name: Fmt
    runs-on: ubuntu-latest

on:
  pull_request:
    paths-ignore:
      - 'docs/**'
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

# `paths` as a STEP input, inside the jobs block. Must NOT be flagged. The
# jobs-block exclusion is what has to carry this, since `_BAD_ON_AFTER_JOBS`
# forces the scan to cover lines below `jobs:`.
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

# A key that merely starts with `paths` is not a path filter. Must NOT be
# flagged, or the loosened value-matching above becomes a false-positive source.
_GOOD_PATHSLIKE_KEY = """\
name: CI
on:
  pull_request:
    paths_are_not_this: true

jobs:
  fmt_check:
    name: Fmt
    runs-on: ubuntu-latest
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

# (description, source, expected error count, expected 1-based line of the first
# error or None). The line is asserted so that a mutation reporting the right
# NUMBER of errors in the wrong place still fails.
_CASES = [
    ("pre-fix ci.yml pull_request filter", _BAD_CI, 1, 8),
    ("pre-fix claude-pr-review.yml filter", _BAD_TARGET, 1, 5),
    ("allowlist `paths:` on a required workflow", _BAD_PATHS_ALLOWLIST, 1, 4),
    ("flow-sequence `paths-ignore: [...]`", _BAD_FLOW_SEQUENCE, 1, 6),
    ("`on:` written after `jobs:`", _BAD_ON_AFTER_JOBS, 1, 9),
    ("required context from a bare job id", _BAD_JOB_ID_CONTEXT, 1, 4),
    ("fixed ci.yml (push filter kept)", _GOOD_CI, 0, None),
    ("workflow with no required context", _GOOD_NOT_REQUIRED, 0, None),
    ("`paths:` as a step input", _GOOD_PATHS_AS_STEP_INPUT, 0, None),
    ("a key merely starting with `paths`", _GOOD_PATHSLIKE_KEY, 0, None),
    ("inline `on: [pull_request]` list", _GOOD_INLINE_TRIGGER_LIST, 0, None),
]


def _needs_fixture(needs_lines: str, job: str = "heavy") -> str:
    return (
        "name: CI\non:\n  pull_request:\n\njobs:\n"
        "  fmt_check:\n    name: Fmt\n    runs-on: ubuntu-latest\n"
        f"  {job}:\n    name: Heavy\n{needs_lines}    runs-on: ubuntu-latest\n"
    )


# (description, source, expected error count, expected first error line)
_NEEDS_CASES = [
    ("no needs at all", _needs_fixture(""), 0, None),
    ("inline `needs: fmt_check` (the #5621 shape)", _needs_fixture("    needs: fmt_check\n"), 1, 11),
    ("quoted inline value", _needs_fixture("    needs: 'fmt_check'\n"), 1, 11),
    ("flow-list needs including a required job", _needs_fixture("    needs: [other, fmt_check]  # c\n"), 1, 11),
    ("multi-line flow list", _needs_fixture("    needs: [\n      other,\n      fmt_check\n    ]\n"), 1, 11),
    ("block list", _needs_fixture("    needs:\n      - other\n      - fmt_check\n"), 1, 11),
    ("block list with comment lines between items", _needs_fixture("    needs:\n      # gate\n      - other\n\n      # more\n      - fmt_check\n"), 1, 11),
    ("block list with quoted items", _needs_fixture("    needs:\n      - \"fmt_check\"\n"), 1, 11),
    ("block list at the key's own indent", _needs_fixture("    needs:\n    - fmt_check\n"), 1, 11),
    ("two required deps in one list", _needs_fixture("    needs: [fmt_check, clippy_check]\n").replace("  heavy:", "  clippy_check:\n    name: Clippy\n    runs-on: ubuntu-latest\n  heavy:"), 2, 14),
    ("needs on a non-required job", _needs_fixture("    needs: other\n"), 0, None),
    ("quoted `\"needs\"` key", _needs_fixture("    \"needs\": fmt_check\n"), 1, 11),
    ("`needs :` with a space before the colon", _needs_fixture("    needs : fmt_check\n"), 1, 11),
    ("a commented-out needs is not an edge", _needs_fixture("    # needs: fmt_check\n"), 0, None),
    ("needs value that merely follows a comment", _needs_fixture("    needs: other  # fmt_check\n"), 0, None),
    ("required job identified by its id, not a name", "name: CI\non:\n  pull_request:\n\njobs:\n  Fmt:\n    runs-on: ubuntu-latest\n  heavy:\n    needs: Fmt\n    runs-on: ubuntu-latest\n", 1, 9),
    ("required job defined AFTER its dependent", "name: CI\non:\n  pull_request:\n\njobs:\n  heavy:\n    needs: fmt_check\n    runs-on: ubuntu-latest\n  fmt_check:\n    name: Fmt\n    runs-on: ubuntu-latest\n", 1, 7),
    ("allowlisted claude-ci-analysis may need a required job", _needs_fixture("    needs: fmt_check\n", job="claude-ci-analysis"), 0, None),
    ("allowlisted notify_dev_room_main_failure may need a required job", _needs_fixture("    needs: fmt_check\n", job="notify_dev_room_main_failure"), 0, None),
    (
        "the allowlist is per-job: an offender after an observer is still flagged",
        _needs_fixture("    needs: fmt_check\n", job="claude-ci-analysis")
        + "  heavy:\n    needs: fmt_check\n    runs-on: ubuntu-latest\n",
        1,
        14,
    ),
    ("multi-line flow list with items at the key's own indent", _needs_fixture("    needs: [\n    other,\n    fmt_check\n    ]\n"), 1, 11),
    ("multi-line flow list opened with an item", _needs_fixture("    needs: [other,\n    fmt_check]\n"), 1, 11),
    ("`name:` with a trailing comment still counts as required", "name: CI\non:\n  pull_request:\n\njobs:\n  fmt_check:\n    name: Fmt  # required\n  heavy:\n    needs: fmt_check\n", 1, 9),
    ("unreadable (quoted) job key fails closed", "name: CI\non:\n  pull_request:\n\njobs:\n  fmt_check:\n    name: Fmt\n  \"heavy\":\n    needs: fmt_check\n", 1, 8),
    (
        "a column-0 comment inside jobs: does not hide later jobs",
        "name: CI\non:\n  pull_request:\n\njobs:\n  fmt_check:\n    name: Fmt\n    runs-on: ubuntu-latest\n# stray\n  heavy:\n    needs: fmt_check\n    runs-on: ubuntu-latest\n",
        1,
        11,
    ),
]


def _coverage_fixture(rename: str | None = None) -> dict[str, str]:
    """One filter-free workflow per required context, optionally renaming one."""
    files = {}
    for i, context in enumerate(sorted(REQUIRED_CONTEXTS)):
        reported = rename if (rename and context == "Fmt") else context
        files[f"w{i}.yml"] = (
            f"name: W{i}\non:\n  pull_request:\n\njobs:\n"
            f"  job_{i}:\n    name: {reported}\n    runs-on: ubuntu-latest\n"
        )
    return files


_COVERAGE_CASES = [
    ("every required context is reported", _coverage_fixture(), 0),
    (
        "a required job renamed out from under the ruleset",
        _coverage_fixture(rename="Format"),
        1,
    ),
]


def _first_error_line(errors: list[str]) -> int | None:
    if not errors:
        return None
    return int(errors[0].split(":")[1])


def self_test() -> int:
    failures = 0
    for desc, source, expected, expected_line in _CASES:
        errors = check_source(source, "<fixture>")
        found = len(errors)
        line = _first_error_line(errors)
        if found == expected and line == expected_line:
            print(f"ok   - {desc}")
        else:
            print(
                f"FAIL - {desc}: expected {expected} error(s) at line "
                f"{expected_line}, got {found} at line {line}",
                file=sys.stderr,
            )
            failures += 1

    for desc, source, expected, expected_line in _NEEDS_CASES:
        errors = check_needs(source, "<fixture>")
        line = _first_error_line(errors)
        if len(errors) == expected and line == expected_line:
            print(f"ok   - needs: {desc}")
        else:
            print(
                f"FAIL - needs: {desc}: expected {expected} error(s) at line "
                f"{expected_line}, got {len(errors)} at line {line}",
                file=sys.stderr,
            )
            failures += 1

    for desc, files, expected in _COVERAGE_CASES:
        with tempfile.TemporaryDirectory() as tmp:
            for filename, source in files.items():
                (Path(tmp) / filename).write_text(source, encoding="utf-8")
            found = len(check_dir(Path(tmp)))
        if found == expected:
            print(f"ok   - coverage: {desc}")
        else:
            print(
                f"FAIL - coverage: {desc}: expected {expected} error(s), "
                f"got {found}",
                file=sys.stderr,
            )
            failures += 1

    total = len(_CASES) + len(_NEEDS_CASES) + len(_COVERAGE_CASES)
    if failures:
        print(f"\n{failures} self-test failure(s)", file=sys.stderr)
        return 1
    print(f"\nAll {total} self-tests passed")
    return 0


def main(argv: list[str]) -> int:
    if "--self-test" in argv:
        return self_test()

    require_coverage = "--no-coverage-check" not in argv
    positional = [a for a in argv[1:] if not a.startswith("--")]
    workflow_dir = (
        Path(positional[0]) if positional else Path(".github/workflows")
    )
    if not workflow_dir.is_dir():
        print(f"ERROR: {workflow_dir} is not a directory", file=sys.stderr)
        return 2

    errors = check_dir(workflow_dir, require_coverage=require_coverage)
    if errors:
        print(
            "ERROR: required-check workflow check failed "
            "(see #5451 and #5621 for what this prevents)"
        )
        for err in errors:
            print(f"  - {err}")
        return 1

    print("required-check path-filter check passed")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))

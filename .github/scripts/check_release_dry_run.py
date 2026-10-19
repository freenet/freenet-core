#!/usr/bin/env python3
"""Guard: a workflow's `dry_run` must not be able to push, merge, tag or publish.

#5936. `release.yml` had a `dry_run` input described as "show what would be
done without publishing". Each mutating step carried its own
`if: ${{ !inputs.dry_run }}`, and the version-bump job carried none. So a dry
run pushed `release/vX.Y.Z`, opened the real `build: release X.Y.Z` PR and
enabled auto-merge on it, landing the bump on `main` through the merge queue:
a partial release cut, which also invalidated any soak of the previous `main`.

The fix replaces per-step guards with ONE resolved value, and this linter makes
that structure the only one that passes:

  1. The `validate` job resolves the input once, in its step `id: mode`
     (`scripts/resolve_release_mode.sh`), and exports `outputs.live`. Only the
     literal input `false` yields `live == 'true'`; anything else is a dry run.
  2. `inputs.dry_run` is read NOWHERE else: dotted or indexed, any case, or
     through a whole object that carries it (`inputs`, `github.event`,
     `github`). A second reader is how the guards drifted apart.
  3. Every MUTATING step is gated on `needs.validate.outputs.live == 'true'`,
     as a top-level `&&` term of its own `if:` or its job's `if:`, in an
     expression with no top-level `||` (`&&` binds tighter, so `a || b && G`
     runs without G). A missing or empty output SKIPS the step: fail closed.
  4. Every mutating `run:` step also refuses, as the first command of its
     own script (only `set -...` may precede it), unless `RELEASE_LIVE`
     (wired from the same output) is `true`. So deleting an `if:` turns a dry
     run red instead of letting it mutate.
  5. Credentials, the rule that makes any gap in the mutation detector below
     harmless. In a job a dry run can reach: `actions/checkout` persists no
     credential (a stray `git push` cannot authenticate) and gets RELEASE_PAT
     only through the live-gated expression `DRY_SAFE_CHECKOUT_TOKEN`; no
     step a dry run executes receives RELEASE_PAT at all; and if the job's
     token can write, no such step is passed ANY token or secret through an
     expression (workflow env, job env, step env/with, or `${{ }}` in `run:`,
     in the raw source; a presence test like `secrets.X != ''` is allowed).
     Actions' implicit default token for `actions/*` steps is not an
     expression and is not counted; with persistence off it stays inside the
     action. A dry-run-ONLY job declares read-only `permissions:`, and may
     then use its own read-only token. No dry-run job gets `id-token: write`.
     A job that pushes when live must persist its credential when live, or
     the push fails. YAML anchors and aliases are refused, since an alias can
     copy a live step's env into a dry-run step invisibly.
  6. Every `needs.J.*` reference names a job in `needs:`, every
     `needs.J.outputs.Y` names an output J declares, and every
     `steps.S.outputs.Y` names a step `id: S` in the same job whose script
     writes `Y=`. A broken reference is silently empty, which on the LIVE path
     skips a guarded step or passes an empty argument.
  7. Steps the reader cannot place (a `steps:` block it cannot itemise, a
     flow-style `steps: [...]` or `- {...}` item, an item with neither `run:`
     nor `uses:`) are an error, not a pass.
  8. `continue-on-error` is refused (it lets a release pass a failed gate),
     and so is comparing with an unquoted boolean (`'true' == true` is false
     in Actions).

To extend this guard, extend rule 5 (what credential a dry-run step can
hold), not the mutation detector: the detector can never be complete.

Rule 5 is the guarantee; the mutation detector below is best-effort. Its
known gaps (a command it does not recognise) cannot write, because a step
that runs in a dry run holds no usable write credential.

"Mutating" is decided from the step's source, not from its name: `git push`;
`gh pr|release|workflow|run|issue|label|secret|variable|repo|cache` write
verbs; `gh api` with a non-GET method, request fields, or a GraphQL `mutation`
(or a query read from a file, which cannot be inspected); `cargo publish`
without `--dry-run` on that same command; `cargo yank|owner`; `docker|podman
push`; `npm publish`; `curl` with a write method or a body; Python
`requests.post|put|patch|delete` and `subprocess` argv `["git", "push"]`.
Commands are matched in the raw line, quotes included, so `bash -c '...'`,
`eval "..."` and `CMD="git push"; $CMD` are all seen; only a line whose
command is `echo`/`printf` has its quoted text ignored, so an error message
that TELLS the operator to run `gh run rerun` is not a rerun. A `run:` that
invokes `scripts/` or `.github/scripts/` files is scanned through them. A
`uses:` step is mutating unless its action is on a short read-only allowlist,
and a job-level `uses:` (reusable workflow) always is.

Escape hatch, for the one call that is POST-shaped but writes nothing
(GitHub's `releases/generate-notes` renders text and creates nothing): a shell
comment line containing `dry-run-safe: <reason>` immediately above it. It
covers a single `gh api .../releases/generate-notes` command and nothing
chained or piped with it. It is visible in review, which is the point.

A false positive (a read-only step reported as mutating) is fixed by making
the step's source unambiguous, not by loosening a pattern; if that is
impossible, extend `_exemptable` deliberately, in review.

Scope: every workflow that declares a `workflow_dispatch` input named
`dry_run`, and every such workflow must have the `validate`/`mode` structure
above. Today that is `release.yml` only. A lint run over a directory where NO
workflow is in scope fails: renaming or removing the input must not silently
switch the check off.

Usage:
    check_release_dry_run.py [WORKFLOW_DIR]       # lint (default .github/workflows)
    check_release_dry_run.py --self-test
    check_release_dry_run.py --extract-mutating-runs WORKFLOW OUTDIR
        Writes each mutating `run:` script (every `${{ expr }}` replaced by
        `@@expr@@`) to OUTDIR/NN.sh, its job+step `env:` to OUTDIR/NN.env and
        its "job / step" label to OUTDIR/NN.name, so
        scripts/release_dry_run_test.sh can EXECUTE them.
    check_release_dry_run.py --simulate WORKFLOW live|dry [--fail JOB]
        Evaluates every job and step `if:` for that mode, with Actions'
        comparison and truthiness rules, and prints what would run, one `job`
        or `job / step` per line. `--fail JOB` makes that job fail if it
        runs. Lets the test pin the topology and failure paths of BOTH modes.
    check_release_dry_run.py --dump-step WORKFLOW JOB STEP
        Prints one step's script, a `@@ENV@@` line, then its job+step env,
        tokenized as above, so the test can run non-mutating steps too.

Tests: scripts/release_dry_run_test.sh (real files, mutations, execution).
"""

from __future__ import annotations

import re
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

MODE_JOB = "validate"
MODE_STEP_ID = "mode"
MODE_SCRIPT = "scripts/resolve_release_mode.sh"
GUARD = "needs.validate.outputs.live == 'true'"
DRY_ONLY = "needs.validate.outputs.live != 'true'"
# Inside the mode job itself `needs.validate` does not exist yet.
GUARD_IN_MODE_JOB = "steps.mode.outputs.live == 'true'"
LIVE_ENV_RE = re.compile(
    r"^RELEASE_LIVE:\s*\$\{\{\s*(needs\.validate|steps\.mode)\.outputs\.live\s*\}\}\s*$"
)
# The in-script refusal. Exact form, so it cannot be weakened by an edit that
# keeps the variable name but drops the exit.
ASSERT_RE = re.compile(
    r'^\[ "\$\{RELEASE_LIVE:-\}" = "true" \] \|\| \{ echo "[^"]*"; exit 1; \}$'
)
# Rule 5's credential test. Expression contexts are case-insensitive, and
# `secrets['X']`, `format(..., secrets.X)` and `toJSON(secrets)` all hand over
# a value, so match the context rather than one spelling. A presence test such
# as `${{ secrets.RELEASE_PAT != '' }}` hands over nothing and is allowed.
SECRET_RE = re.compile(
    r"\bsecrets\b|\bgithub\s*\.\s*token\b|\bgithub\s*\[\s*['\"]token"
    # The whole `github` context (`toJSON(github)`) or a computed index into
    # it carries `github.token` too. A named property other than `token`
    # (`github.sha`, `github.repository`) does not.
    r"|\bgithub\b(?!\s*\.\s*[A-Za-z_])(?!\s*\[\s*['\"](?!token['\"])[\w-]+['\"]\s*\])",
    re.I,
)
WORKFLOW_TOKEN_RE = re.compile(r"\bsecrets\s*\.\s*github_token\b|\bgithub\s*\.\s*token\b", re.I)
PAT_RE = re.compile(r"\brelease_pat\b|tojson\s*\(\s*secrets", re.I)
PRESENCE_RE = re.compile(r"^\$\{\{\s*secrets\.[\w]+\s*(!=|==)\s*''\s*\}\}$", re.I)
# A checkout in a job a dry run reaches: RELEASE_PAT when live (so the live
# push authenticates exactly as before), the workflow token otherwise, which
# with `persist-credentials` off is used for the fetch only.
DRY_SAFE_CHECKOUT_TOKEN = (
    "${{ (needs.validate.outputs.live == 'true' && secrets.RELEASE_PAT) || secrets.GITHUB_TOKEN }}"
)
# Expression property names are case-insensitive, and index syntax is valid.
DRY_RUN_INPUT_RE = re.compile(
    r"inputs\s*(\.\s*dry[_-]run\b|\[\s*['\"]dry[_-]run['\"]\s*\])", re.I
)
# The whole inputs object inside an expression (`toJSON(inputs)`) carries it too.
INPUTS_OBJECT_RE = re.compile(
    r"\binputs\b(?!\s*[.\[])"
    # ...as do `github.event` and the whole `github` context.
    r"|\bgithub\s*\.\s*event\b(?!\s*\.\s*[A-Za-z_])"
    r"|\bgithub\b(?!\s*[.\[])",
    re.I,
)
# YAML anchors and aliases (supported by Actions since 2025-09). An alias can
# copy a live step's env or script into a dry-run step with no `${{` in the
# step's own text, so in-scope workflows may not use them.
ANCHOR_RE = re.compile(
    r"""^\s*(-\s+)?["']?[\w.-]+["']?\s*:\s+[&*][A-Za-z0-9_-]+(\s|$)|^\s*-\s+[&*][A-Za-z0-9_-]+(\s|$)|^\s*<<\s*:"""
)
EXEMPT_MARKER = "dry-run-safe:"
BLOCK_SCALAR_RE = re.compile(r"^[|>][1-9]?[-+]?[1-9]?\s*(#.*)?$")

READ_ONLY_PERMS = ("read", "none", "read-all")


def _read_only(perms: dict[str, str]) -> bool:
    """`permissions: {}` grants nothing; it parses as {"*": "{}"}, and is
    accepted only in that whole-block form."""
    return perms == {"*": "{}"} or all(v in READ_ONLY_PERMS for v in perms.values())


def _action(uses: str) -> str:
    """`owner/repo` of a `uses:` value, compared EXACTLY: a prefix match would
    trust `actions/checkout-anything@v1`."""
    return uses.split("@", 1)[0].strip().strip("\"'").lower()


# Only actions that write nothing outside the run. Cache actions (including
# `Swatinem/rust-cache` and `actions/setup-*` with `cache:`) are deliberately
# absent: a cache SAVE is a shared write that a later live run restores.
READ_ONLY_ACTIONS = (
    "actions/checkout",
    "actions/upload-artifact",
    "actions/download-artifact",
    "dtolnay/rust-toolchain",
)

# `gh [global flags] <group> [flags] <verb>`: gh accepts flags such as
# `--repo o/r` / `-R o/r` before the verb.
_GH = r"\bgh\s+(?:-{1,2}[\w-]+(?:[ =]\S+)?\s+)*"
_FLAGS = r"(?:-{1,2}[\w-]+(?:[ =]\S+)?\s+)*"

MUTATING_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("git push", re.compile(r"\bgit\b(\s+-\S+(\s+\S+)?)*\s+push\b")),
    (
        "gh pr write",
        re.compile(
            _GH + r"pr\s+" + _FLAGS
            + r"(create|merge|edit|close|reopen|comment|review|ready|lock|unlock|update-branch|revert)\b"
        ),
    ),
    (
        "gh release write",
        re.compile(_GH + r"release\s+" + _FLAGS + r"(create|edit|upload|delete|delete-asset)\b"),
    ),
    ("gh workflow write", re.compile(_GH + r"workflow\s+" + _FLAGS + r"(run|enable|disable)\b")),
    ("gh run write", re.compile(_GH + r"run\s+" + _FLAGS + r"(rerun|cancel|delete)\b")),
    (
        "gh issue/label/secret/variable/repo/cache/project write",
        re.compile(
            _GH + r"(issue\s+" + _FLAGS + r"(create|edit|comment|close|reopen|delete|transfer|pin|unpin|lock|unlock|develop)"
            r"|label\s+" + _FLAGS + r"(create|edit|delete|clone)|secret\s+" + _FLAGS + r"(set|delete)"
            r"|variable\s+" + _FLAGS + r"(set|delete)"
            r"|repo\s+" + _FLAGS + r"(create|edit|delete|rename|archive|fork|sync)|cache\s+" + _FLAGS + r"delete"
            r"|project\s)\b"
        ),
    ),
    ("cargo yank/owner", re.compile(r"\bcargo(\s+[-+]\S+)*\s+(yank|owner)\b")),
    ("container push", re.compile(r"\b(docker|podman)\s+(image\s+)?push\b")),
    ("npm publish", re.compile(r"\bnpm\s+publish\b")),
    (
        "curl write",
        re.compile(
            r"\bcurl\b.*(\s-[A-Za-z]*X\s*['\"]?(POST|PUT|PATCH|DELETE)|\s--request[\s=]*['\"]?(POST|PUT|PATCH|DELETE)"
            r"|\s(-d|--data(-[a-z]+)?|--json|-F|--form|-T|--upload-file)(\s|=|@)|\s-d\S)"
        ),
    ),
    ("python HTTP write", re.compile(r"\brequests\.(post|put|patch|delete)\s*\(")),
    ("python git push", re.compile(r"""["']git["']\s*,\s*(["'][^"']*["']\s*,\s*)*["']push["']""")),
    ("python gh call", re.compile(r"""["']gh["']\s*,\s*["'](pr|release|workflow|run|api|issue|repo)["']""")),
    ("piped into a shell", re.compile(r"\|\s*(sudo\s+)?(ba|z|da)?sh\b|\|\s*xargs\b")),
]

SCRIPT_REF_RE = re.compile(
    r"""(?:^|[\s"'=(;&|])(?:\./|\$\{?GITHUB_WORKSPACE\}?/)?((?:\.github/)?scripts/[\w./-]+\.(?:sh|py))\b"""
)
CHAIN_RE = re.compile(r";|&&|\|\|")
SEGMENT_RE = re.compile(r";|&&|\|\||\|")
SHELLISH = ("sh", "bash", "eval", "xargs", "source", ".")


# --- a deliberately small YAML reader --------------------------------------
#
# No PyYAML: the other linters here are dependency-free and run on a bare
# runner. GitHub workflows in this repo use a fixed layout (jobs at 2 spaces,
# job keys at 4, steps at `      - `, step keys at 8). What this reader cannot
# itemise inside a `steps:` block is reported (rule 7), never skipped.


@dataclass
class Step:
    job: str
    line: int
    keys: dict[str, str] = field(default_factory=dict)
    env: dict[str, str] = field(default_factory=dict)
    with_: dict[str, str] = field(default_factory=dict)
    run: Optional[str] = None
    text: str = ""
    run_lines: range = range(0)

    @property
    def label(self) -> str:
        return (
            self.keys.get("name") or self.keys.get("id") or self.keys.get("uses") or "?"
        ).strip("\"'")


@dataclass
class Job:
    name: str
    line: int
    end: int
    keys: dict[str, str] = field(default_factory=dict)
    env: dict[str, str] = field(default_factory=dict)
    outputs: dict[str, str] = field(default_factory=dict)
    permissions: dict[str, str] = field(default_factory=dict)
    needs: list[str] = field(default_factory=list)
    text: str = ""
    header: str = ""
    steps: list[Step] = field(default_factory=list)
    unparsed: list[int] = field(default_factory=list)


def _indent(raw: str) -> int:
    return len(raw) - len(raw.lstrip(" "))


def _strip_comment(value: str) -> str:
    return re.sub(r"\s+#.*$", "", value).strip()


def _unquote_scalar(value: str) -> str:
    v = value.strip()
    if len(v) >= 2 and v[0] == v[-1] == '"':
        return v[1:-1].replace('\\"', '"').replace("\\\\", "\\")
    if len(v) >= 2 and v[0] == v[-1] == "'":
        return v[1:-1].replace("''", "'")
    return v


def _block_scalar(lines: list[str], start: int, min_indent: int) -> tuple[str, int]:
    """Collect lines after `start` indented deeper than `min_indent`."""
    body: list[str] = []
    i = start + 1
    while i < len(lines):
        raw = lines[i]
        if raw.strip() and _indent(raw) <= min_indent:
            break
        body.append(raw)
        i += 1
    while body and not body[-1].strip():
        body.pop()
    indents = [_indent(b) for b in body if b.strip()]
    cut = min(indents) if indents else 0
    return "\n".join(b[cut:] if len(b) >= cut else "" for b in body), i


def _parse_map(lines: list[str], start: int, indent: int) -> dict[str, str]:
    out: dict[str, str] = {}
    i = start + 1
    key_re = re.compile(r"^" + " " * indent + r"""["']?([A-Za-z0-9_.-]+)["']?:\s*(.*)$""")
    while i < len(lines):
        raw = lines[i]
        if raw.strip() and _indent(raw) < indent:
            break
        m = key_re.match(raw)
        if m and not raw.strip().startswith("#"):
            out[m.group(1)] = _unquote_scalar(_strip_comment(m.group(2)))
        i += 1
    return out


def _parse_step(job: str, lines: list[str], first: int, last: int) -> Step:
    step = Step(job=job, line=first + 1, text="\n".join(lines[first:last]))
    seg = list(lines[first:last])
    seg[0] = seg[0].replace("      - ", "        ", 1)
    key_re = re.compile(r"^        ([A-Za-z_-]+):\s*(.*)$")
    i = 0
    while i < len(seg):
        raw = seg[i]
        m = key_re.match(raw)
        if not m or raw.strip().startswith("#"):
            i += 1
            continue
        key, value = m.group(1), m.group(2)
        if key == "run" and BLOCK_SCALAR_RE.match(value):
            start = i
            step.run, i = _block_scalar(seg, i, 8)
            step.run_lines = range(first + start + 2, first + i + 1)
            continue
        if key == "run":
            # A quoted single-line `run:` must be scanned as the command it
            # is, not as one big string literal.
            step.run = _unquote_scalar(value)
        elif key == "env":
            step.env = _parse_map(seg, i, 10)
        elif key == "with":
            step.with_ = _parse_map(seg, i, 10)
        elif BLOCK_SCALAR_RE.match(value):
            body, i = _block_scalar(seg, i, 8)
            step.keys[key] = " ".join(body.split())
            continue
        else:
            step.keys[key] = _strip_comment(value)
        i += 1
    return step


def parse_workflow(text: str) -> tuple[list[Job], list[str]]:
    lines = text.split("\n")
    try:
        jobs_at = next(i for i, l in enumerate(lines) if re.match(r"^jobs:\s*(#.*)?$", l))
    except StopIteration:
        return [], lines
    starts: list[tuple[int, str]] = []
    end_of_jobs = len(lines)
    for i in range(jobs_at + 1, len(lines)):
        l = lines[i]
        if re.match(r"^[A-Za-z]", l):
            end_of_jobs = i
            break
        m = re.match(r"^  ([A-Za-z0-9_-]+):\s*(#.*)?$", l)
        if m:
            starts.append((i, m.group(1)))
    jobs: list[Job] = []
    for n, (begin, name) in enumerate(starts):
        end = starts[n + 1][0] if n + 1 < len(starts) else end_of_jobs
        job = Job(name=name, line=begin + 1, end=end, text="\n".join(lines[begin:end]))
        steps_from = steps_to = None
        k = begin + 1
        while k < end:
            raw = lines[k]
            m = re.match(r"^    ([A-Za-z_-]+):\s*(.*)$", raw)
            if not m or raw.strip().startswith("#"):
                k += 1
                continue
            key, value = m.group(1), m.group(2)
            if key == "steps":
                if _strip_comment(value):
                    job.unparsed.append(k + 1)  # flow-style `steps: [...]`
                steps_from = k + 1
                steps_to = next(
                    (j for j in range(k + 1, end) if re.match(r"^    [A-Za-z_-]+:", lines[j])),
                    end,
                )
                k = steps_to
                continue
            if key == "env":
                job.env = _parse_map(lines, k, 6)
            elif key == "outputs":
                job.outputs = _parse_map(lines, k, 6)
            elif key == "permissions":
                if _strip_comment(value):
                    job.permissions = {"*": _strip_comment(value)}
                else:
                    job.permissions = _parse_map(lines, k, 6)
            elif key == "needs":
                v = _strip_comment(value)
                if v.startswith("["):
                    job.needs = [x.strip() for x in v.strip("[]").split(",") if x.strip()]
                elif v:
                    job.needs = [v]
                else:
                    j = k + 1
                    while j < end and re.match(r"^      - ", lines[j]):
                        job.needs.append(_strip_comment(lines[j].split("- ", 1)[1]))
                        j += 1
            elif BLOCK_SCALAR_RE.match(value):
                blk, _ = _block_scalar(lines, k, 4)
                job.keys[key] = " ".join(blk.split())
            else:
                job.keys[key] = _strip_comment(value)
            k += 1
        # Every job line outside the steps block, including keys written
        # after `steps:` (YAML key order does not matter).
        if steps_from is None:
            job.header = job.text
        else:
            job.header = "\n".join(lines[begin:steps_from - 1] + lines[steps_to:end])
        if steps_from is not None:
            items = [j for j in range(steps_from, steps_to) if re.match(r"^      - ", lines[j])]
            first_item = items[0] if items else steps_to
            job.unparsed += [
                j + 1
                for j in range(steps_from, first_item)
                if lines[j].strip() and not lines[j].strip().startswith("#")
            ]
            for s, first in enumerate(items):
                last = items[s + 1] if s + 1 < len(items) else steps_to
                step = _parse_step(name, lines, first, last)
                # A flow-style item (`- { run: ... }`) or anything else that
                # yields no `run:`/`uses:` cannot be examined, so it is
                # reported (rule 7) rather than passed as an empty step.
                if step.run is None and "uses" not in step.keys:
                    job.unparsed.append(first + 1)
                job.steps.append(step)
        jobs.append(job)
    return jobs, lines


# --- expressions -----------------------------------------------------------


def _unwrap(expr: str) -> str:
    e = expr.strip()
    m = re.match(r"^\$\{\{(.*)\}\}$", e, re.S)
    return " ".join((m.group(1) if m else e).split())


def _split_top(e: str, op: str) -> list[str]:
    out, depth, cur, i, quote = [], 0, "", 0, None
    while i < len(e):
        c = e[i]
        if quote:
            if c == quote:
                quote = None
        elif c in "'\"":
            quote = c
        elif c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
        elif depth == 0 and e.startswith(op, i):
            out.append(cur.strip())
            cur = ""
            i += len(op)
            continue
        cur += c
        i += 1
    out.append(cur.strip())
    return [" ".join(x.split()) for x in out if x.strip()]


def conjuncts(expr: str) -> list[str]:
    """Top-level `&&` operands of an `if:` expression, whitespace-normalised.

    Empty when the expression has a top-level `||`: then no operand is
    required for the whole to be true, so none of them is a guard.
    """
    e = _unwrap(expr)
    if not e or len(_split_top(e, "||")) > 1:
        return []
    return _split_top(e, "&&")


def _guard_for(job: str) -> str:
    return GUARD_IN_MODE_JOB if job == MODE_JOB else GUARD


def is_guarded(job: Job, step: Optional[Step]) -> bool:
    if GUARD in conjuncts(job.keys.get("if", "")):
        return True
    return step is not None and _guard_for(job.name) in conjuncts(step.keys.get("if", ""))


# --- mutation detection ----------------------------------------------------


def _drop_literals(text: str) -> str:
    """Remove quoted text that cannot execute anything.

    Used ONLY for a line whose command is `echo`/`printf`, where quoted text is
    a message. Double-quoted text with a command substitution is kept.
    """
    out, i, n = [], 0, len(text)
    while i < n:
        c = text[i]
        if c == "\\":
            out.append(text[i : i + 2])
            i += 2
            continue
        if c == "'":
            j = text.find("'", i + 1)
            if j == -1:
                out.append(text[i:])
                break
            out.append("''")
            i = j + 1
            continue
        if c == '"':
            j, body = i + 1, []
            while j < n and text[j] != '"':
                if text[j] == "\\":
                    body.append(text[j : j + 2])
                    j += 2
                    continue
                body.append(text[j])
                j += 1
            inner = "".join(body)
            executes = "$(" in inner or re.search(r"(?<!\\)`", inner) is not None
            out.append(text[i : j + 1] if executes else '""')
            i = j + 1
            continue
        out.append(c)
        i += 1
    return "".join(out)


def _strip_trailing_comment(text: str) -> str:
    """Drop a ` # comment` outside quotes (`${x#v}` has no space before `#`)."""
    quote = None
    for i, c in enumerate(text):
        if quote:
            if c == quote:
                quote = None
        elif c in "'\"":
            quote = c
        elif c == "#" and i > 0 and text[i - 1] in " \t":
            return text[:i].rstrip()
    return text


def _scan_text(text: str) -> str:
    """The text mutation patterns run over.

    Raw, quotes included, so `bash -c '...'` and `eval "..."` are seen. The one
    exception is a line that is a SINGLE `echo`/`printf` command: its quoted
    text is a message. Anything chained or piped after it is scanned raw.
    """
    text = _strip_trailing_comment(text)
    first = text.split(None, 1)[0] if text.split() else ""
    if first in ("echo", "printf") and not SEGMENT_RE.search(_drop_literals(text)):
        return _drop_literals(text)
    return text


def logical_lines(script: str) -> list[tuple[int, str, bool]]:
    """(index, joined text, exempt) per shell logical line, comments dropped."""
    out: list[tuple[int, str, bool]] = []
    buf: list[str] = []
    exempt = False
    heredoc_end: Optional[str] = None
    for idx, raw in enumerate(script.split("\n")):
        s = raw.strip()
        if heredoc_end is not None:
            # The body of a `cat <<TAG` heredoc is data, not commands. (A
            # heredoc fed to a shell is not skipped: `bash <<TAG` executes it.)
            if s == heredoc_end:
                heredoc_end = None
            continue
        if not buf:
            if s.startswith("#"):
                if EXEMPT_MARKER in s:
                    exempt = True
                continue
            if not s:
                exempt = False
                continue
        buf.append(s)
        if s.endswith("\\"):
            continue
        joined = " ".join(p.rstrip("\\").strip() for p in buf)
        out.append((idx, joined, exempt))
        # Only a QUOTED delimiter makes the body data: `cat <<EOF` still runs
        # every `$(...)` and backtick in it.
        m = re.match(r"^cat\b[^<]*<<-?\s*(['\"])(\w+)\1\s*$", joined)
        if m:
            heredoc_end = m.group(2)
        buf, exempt = [], False
    if buf:
        out.append((len(script.split("\n")), " ".join(buf), exempt))
    return out


def _gh_api_mutates(segment: str) -> bool:
    m = re.search(r"\bgh\s+api\b(.*)", segment)
    if not m:
        return False
    args = m.group(1)
    method = re.search(r"(?:-X|--method)[\s=]*['\"]?([A-Za-z]+)", args)
    if method:
        return method.group(1).upper() != "GET"
    if re.search(r"(?:^|\s)graphql\b", args):
        # A query read from a file or a variable cannot be inspected here.
        return (
            "mutation" in args
            or re.search(r"query=['\"]?[@$]", args) is not None
            or re.search(r"(?:^|\s)--input\b", args) is not None
        )
    return re.search(r"(?:^|\s)(?:-f|-F|--field|--raw-field|--input)(?:\s|=)", args) is not None


def line_mutation(text: str) -> Optional[str]:
    code = _scan_text(text)
    if _match_patterns(code, ("piped into a shell",)):
        return "piped into a shell"
    # Per command, so `curl URL | grep -d` is not a curl write, while
    # `cargo publish --dry-run && cargo publish` and
    # `gh api -X GET a; gh api -X DELETE b` are both seen.
    for segment in SEGMENT_RE.split(code):
        label = _match_patterns(segment)
        if label:
            return label
        if re.search(r"\bcargo(\s+[-+]\S+)*\s+publish\b", segment) and "--dry-run" not in segment:
            return "cargo publish"
        if _gh_api_mutates(segment):
            return "gh api write"
    return None


def _match_patterns(text: str, only: tuple[str, ...] = ()) -> Optional[str]:
    for label, pat in MUTATING_PATTERNS:
        if only and label not in only:
            continue
        if pat.search(text):
            return label
    return None


def _exemptable(text: str) -> bool:
    """`dry-run-safe:` covers exactly one `gh api .../releases/generate-notes`
    call and nothing else: it is the one POST-shaped call that writes nothing."""
    code = _drop_literals(_strip_trailing_comment(text))
    return (
        len(re.findall(r"\bgh\s+api\b", text)) == 1
        and "/releases/generate-notes" in text
        and not SEGMENT_RE.search(code)
    )


def script_mutations(
    script: str, repo_root: Path, seen: Optional[set[str]] = None
) -> list[tuple[int, str]]:
    """(logical-line index, reason) for each mutating line, following scripts."""
    seen = set() if seen is None else seen
    found: list[tuple[int, str]] = []
    for idx, text, exempt in logical_lines(script):
        if exempt and _exemptable(text):
            continue
        reason = line_mutation(text)
        if reason:
            found.append((idx, f"{reason}: {text[:100]}"))
            continue
        # Follow only scripts the line RUNS, not ones a message mentions.
        for ref in SCRIPT_REF_RE.findall(_scan_text(text)):
            if ref in seen:
                continue
            seen.add(ref)
            path = repo_root / ref
            if path.is_file():
                inner = script_mutations(path.read_text(), repo_root, seen)
                if inner:
                    found.append((idx, f"via {ref} ({inner[0][1]})"))
                    break
    return found


def step_mutations(step: Step, repo_root: Path) -> list[tuple[int, str]]:
    uses = step.keys.get("uses", "")
    if uses and _action(uses) not in READ_ONLY_ACTIONS:
        return [(-1, f"uses: {uses} (not on the read-only action allowlist)")]
    if step.run is None:
        return []
    return script_mutations(step.run, repo_root)


# --- the rules -------------------------------------------------------------


def declares_dry_run(text: str) -> bool:
    return bool(re.search(r"^\s*workflow_dispatch:", text, re.M)) and bool(
        re.search(r"^\s+dry[_-]run:\s*(#.*)?$", text, re.M | re.I)
    )


def _with_scripts(run: str, repo_root: Path) -> str:
    """A step's script plus the scripts it invokes (where outputs are often written)."""
    parts = [run]
    for ref in SCRIPT_REF_RE.findall(run):
        path = repo_root / ref
        if path.is_file():
            parts.append(path.read_text())
    return "\n".join(parts)


def _credential_keys(values: dict[str, str], read_only: bool) -> list[str]:
    """Keys whose values hand a dry-run step a credential.

    RELEASE_PAT never; any token or secret when the job's token can write.
    """
    return [k for k, v in values.items() if _hands_credential(v, read_only)]


def _hands_credential(text: str, read_only: bool) -> bool:
    """Does any `${{ }}` expression in `text` inject a credential?

    Only expressions inject values; a message that merely names RELEASE_PAT
    does not. A presence test (`secrets.X != ''`) is allowed.
    """
    for expr in re.findall(r"\$\{\{(.*?)\}\}", text, re.S):
        if PRESENCE_RE.match("${{" + expr + "}}"):
            continue
        if PAT_RE.search(expr):
            return True
        # A read-only job may use its own (read-only) workflow token, and
        # nothing else: a PAT or the crates.io token ignores `permissions:`.
        # `github['token']` is matched before literals are dropped; after
        # that, a literal such as hashFiles('.github/x') is not a reference.
        if re.search(r"\bgithub\s*\[\s*['\"]token", expr, re.I):
            return True
        rest = _drop_expr_literals(expr)
        rest = WORKFLOW_TOKEN_RE.sub("", rest) if read_only else rest
        if SECRET_RE.search(rest):
            return True
    return False


def _drop_expr_literals(expr: str) -> str:
    return re.sub(r"'(?:[^']|'')*'", "''", expr)


def _source_without_comments(text: str) -> str:
    return "\n".join(l for l in text.split("\n") if not l.strip().startswith("#"))


def _references(job: Job) -> list[tuple[int, str]]:
    """(line, text) of the job's non-comment lines."""
    return [
        (job.line + n, l)
        for n, l in enumerate(job.text.split("\n"))
        if not l.strip().startswith("#")
    ]


def check_source(text: str, name: str, repo_root: Path) -> list[str]:
    if not declares_dry_run(text):
        return []
    jobs, lines = parse_workflow(text)
    errors: list[str] = []
    by_name = {j.name: j for j in jobs}
    jobs_at = next((i for i, l in enumerate(lines) if re.match(r"^jobs:\s*(#.*)?$", l)), len(lines))
    jobs_end = next((i for i in range(jobs_at + 1, len(lines)) if re.match(r"^[A-Za-z\"']", lines[i])), len(lines))
    # Every top-level line outside `jobs:`, including keys written after it.
    workflow_head = _source_without_comments("\n".join(lines[:jobs_at] + lines[jobs_end:]))
    perms_at = next((i for i, l in enumerate(lines) if re.match(r"^permissions:", l)), None)
    workflow_perms = {}
    if perms_at is not None:
        v = _strip_comment(lines[perms_at].split(":", 1)[1])
        workflow_perms = {"*": v} if v else _parse_map(lines, perms_at, 2)
    script_lines = {n for j in jobs for st in j.steps for n in st.run_lines}
    for n, raw in enumerate(lines, start=1):
        if n in script_lines:
            continue  # shell text: `*emphasis*` in a heredoc is not YAML
        if not raw.strip().startswith("#") and ANCHOR_RE.match(raw):
            errors.append(f"{name}:{n}: YAML anchors and aliases are not allowed in a workflow with a dry_run input: an alias can copy a live step's credential or script into a dry-run step without it appearing in that step's text.")

    def err(line: int, msg: str) -> None:
        errors.append(f"{name}:{line}: {msg}")

    # Rule 1: the mode is resolved once, by the tested script, and exported.
    mode_job = by_name.get(MODE_JOB)
    mode_step = None
    if mode_job is None:
        err(1, f"no `{MODE_JOB}` job: nothing resolves `dry_run` into `outputs.live`.")
    else:
        mode_step = next((s for s in mode_job.steps if s.keys.get("id") == MODE_STEP_ID), None)
        if mode_step is None:
            err(mode_job.line, f"`{MODE_JOB}` has no step with `id: {MODE_STEP_ID}`.")
        else:
            if (mode_step.run or "").strip() != f"bash {MODE_SCRIPT}":
                err(mode_step.line, f"the mode step must run `{MODE_SCRIPT}` (it is the tested resolver).")
            if not re.fullmatch(
                r"\$\{\{\s*inputs\.dry_run\s*\}\}", mode_step.env.get("DRY_RUN_INPUT", "")
            ):
                err(mode_step.line, "the mode step must set `DRY_RUN_INPUT: ${{ inputs.dry_run }}`.")
            if mode_job.steps.index(mode_step) > 1:
                err(mode_step.line, "the mode step must be one of the first two steps of the job.")
        if not re.fullmatch(r"\$\{\{\s*steps\.mode\.outputs\.live\s*\}\}", mode_job.outputs.get("live", "")):
            err(mode_job.line, f"`{MODE_JOB}` must export `live: ${{{{ steps.mode.outputs.live }}}}`.")

    # Rule 2: nothing else reads the input.
    mode_range = range(0)
    if mode_step is not None and mode_job is not None:
        nxt = [s.line for s in mode_job.steps if s.line > mode_step.line]
        mode_range = range(mode_step.line, (nxt[0] if nxt else mode_job.end + 1))
    for n, raw in enumerate(lines, start=1):
        if raw.strip().startswith("#") or n in mode_range:
            continue
        if DRY_RUN_INPUT_RE.search(raw) or any(
            INPUTS_OBJECT_RE.search(_drop_expr_literals(e)) for e in re.findall(r"\$\{\{(.*?)\}\}", raw)
        ):
            err(n, f"`inputs.dry_run` is read outside the mode step. Gate on `{GUARD}` instead; a second reader is how #5936's guards drifted apart.")

    # Rule 7, job level: every line at the job-id indent must start a job,
    # and every job must yield steps or a reusable `uses:`. Otherwise a job
    # in an unexpected layout (flow style, keys at 3 or 6 spaces) would get
    # no checks at all.
    for n in range(jobs_at + 2, jobs_end + 1):
        raw = lines[n - 1]
        if raw.strip() and not raw.strip().startswith("#") and _indent(raw) == 2 and not re.match(r"^  [A-Za-z0-9_-]+:\s*(#.*)?$", raw):
            err(n, "cannot place this line as a job (jobs must be `  <id>:` blocks with keys at 4 spaces); an unexamined job is not a passing one.")
    for job in jobs:
        if not job.steps and "uses" not in job.keys:
            err(job.line, f"job `{job.name}`: no steps or `uses:` could be parsed (keys must sit at 4 spaces, steps at `      - `); an unexamined job is not a passing one.")

    for job in jobs:
        # `continue-on-error` turns a failed gate (the checkout verifier, the
        # packaging dry run) into a pass, and everything downstream proceeds.
        for where_line, keys in [(job.line, job.keys)] + [(st.line, st.keys) for st in job.steps]:
            if "continue-on-error" in keys:
                err(where_line, "`continue-on-error` is not allowed in a workflow with a dry_run input: it lets a release proceed past a failed gate.")
        for where_line, expr in [(job.line, job.keys.get("if", ""))] + [(st.line, st.keys.get("if", "")) for st in job.steps]:
            if re.search(r"(==|!=)\s*(true|false)\b|\b(true|false)\s*(==|!=)", _unwrap(expr)):
                err(where_line, f"`if: {expr}` compares with an unquoted boolean. Outputs are strings, and in Actions `'true' == true` is false; compare with `'true'`.")

        # Rule 7: nothing in a steps block goes unexamined.
        for ln in job.unparsed:
            err(ln, f"job `{job.name}`: cannot place this line in a step (steps must be `      - ` items); an unexamined step is not a passing one.")

        # Rule 6: references resolve.
        for ln, l in _references(job):
            for dep, kind, out in re.findall(r"\bneeds\.([\w-]+)\.(outputs\.([\w-]+)|result)\b", l):
                if dep not in job.needs:
                    err(ln, f"job `{job.name}` reads `needs.{dep}.*` but does not list `{dep}` in `needs:`, so the value is always empty.")
                elif out and dep in by_name and out not in by_name[dep].outputs:
                    err(ln, f"job `{job.name}` reads `needs.{dep}.outputs.{out}`, which `{dep}` does not declare.")
            for sid, out in re.findall(r"\bsteps\.([\w-]+)\.outputs\.([\w-]+)\b", l):
                src = next((s for s in job.steps if s.keys.get("id") == sid), None)
                reader = next((st for st in reversed(job.steps) if st.line <= ln), None)
                if src is None:
                    err(ln, f"job `{job.name}` reads `steps.{sid}.outputs.{out}` but has no step `id: {sid}`.")
                elif reader is not None and job.steps.index(src) >= job.steps.index(reader):
                    err(ln, f"job `{job.name}`, step `{reader.label}` reads `steps.{sid}.outputs.{out}` before step `{sid}` has run.")
                elif src.run is not None and not re.search(rf"\b{re.escape(out)}=", _with_scripts(src.run, repo_root)):
                    err(ln, f"job `{job.name}` reads `steps.{sid}.outputs.{out}` but step `{sid}` never writes `{out}=`.")

        pushes = any(
            any(r.startswith("git push") for _, r in step_mutations(st, repo_root)) for st in job.steps
        )
        for st in job.steps:
            if pushes and _action(st.keys.get("uses", "")) == "actions/checkout" and st.with_.get("persist-credentials") == "false":
                err(st.line, f"job `{job.name}` pushes when live, but its checkout never persists a credential, so the live push cannot authenticate. Use `${{{{ {GUARD} }}}}`.")

        job_guarded = is_guarded(job, None)
        if "uses" in job.keys and not job_guarded:
            err(job.line, f"job `{job.name}` calls a reusable workflow (`uses: {job.keys['uses']}`) and is not gated on `{GUARD}`.")

        # Rule 5c: a dry-run-only job holds a read-only token.
        if DRY_ONLY in conjuncts(job.keys.get("if", "")):
            perms = job.permissions
            if not perms or not _read_only(perms):
                err(job.line, f"job `{job.name}` runs only in a dry run, so it must declare read-only `permissions:` (got {perms or 'the workflow default'}).")

        read_only = bool(job.permissions) and _read_only(job.permissions)
        # An OIDC token is a publish credential wherever trusted publishing is
        # configured, and it is not covered by the token rules above.
        effective = job.permissions or workflow_perms
        if not job_guarded and (effective.get("id-token") == "write" or "write-all" in effective.values()):
            err(job.line, f"job `{job.name}` runs in a dry run with `id-token: write` (or write-all); an OIDC token can publish.")
        if not job_guarded:
            for scope, src in (("workflow-level", workflow_head), ("job-level", _source_without_comments(job.header))):
                if _hands_credential(src, read_only):
                    err(job.line, f"job `{job.name}` runs in a dry run and receives a credential through its {scope} source (env or another key).")

        for step in job.steps:
            where = f"job `{job.name}`, step `{step.label}`"
            muts = step_mutations(step, repo_root)
            if muts:
                if not is_guarded(job, step):
                    err(step.line, f"{where} mutates ({muts[0][1]}) but is not gated on `{_guard_for(job.name)}` (in its own or its job's `if:`, as a top-level `&&` term, with no top-level `||`).")
                if step.run is not None:
                    live_env = {**job.env, **step.env}.get("RELEASE_LIVE")
                    if live_env is None or not LIVE_ENV_RE.match(f"RELEASE_LIVE: {live_env}"):
                        err(step.line, f"{where} mutates but `RELEASE_LIVE` is not wired from `outputs.live` (step or job `env:`).")
                    # The FIRST command (after any `set -...`), so it cannot sit
                    # inside an `if`/loop that skips it.
                    cmds = [t for _, t, _ in logical_lines(step.run) if not re.match(r"^set\s+[-+]", t)]
                    if not cmds or not ASSERT_RE.match(cmds[0]):
                        err(step.line, f"{where} mutates but does not refuse in its own script first. Make this the script's first command:\n      [ \"${{RELEASE_LIVE:-}}\" = \"true\" ] || {{ echo \"::error::...\"; exit 1; }}")

            if job_guarded or is_guarded(job, step):
                continue
            # Rule 5: a step a dry run executes holds no persisted credential and no PAT.
            uses = step.keys.get("uses", "")
            if _action(uses) == "actions/checkout":
                persist = step.with_.get("persist-credentials", "")
                if persist != "false" and _unwrap(persist) != _guard_for(job.name):
                    err(step.line, f"job `{job.name}` runs in a dry run, so its checkout must set `persist-credentials: false` (or `${{{{ {GUARD} }}}}`); otherwise a stray `git push` can authenticate.")
            # The whole step source, so a folded or flow-style value cannot
            # hide an expression. The one exemption is the exact, single-line
            # live-gated checkout token.
            src = [l for l in step.text.split("\n") if not l.strip().startswith("#")]
            allowed = f"token: {DRY_SAFE_CHECKOUT_TOKEN}"
            if _action(uses) == "actions/checkout":
                src = [l for l in src if l.strip() != allowed]
            if _hands_credential("\n".join(src), read_only):
                err(step.line, f"{where} runs in a dry run and receives a credential. In a job whose token can write, a dry-run step gets no token at all; a read-only job may use only its workflow token; RELEASE_PAT never (a checkout may take exactly `{allowed}`). Gate it on `{_guard_for(job.name)}`.")
    return errors


def check_dir(root: Path, repo_root: Path) -> list[str]:
    errors: list[str] = []
    in_scope = 0
    for path in sorted(list(root.glob("*.yml")) + list(root.glob("*.yaml"))):
        text = path.read_text()
        if declares_dry_run(text):
            in_scope += 1
        errors.extend(check_source(text, str(path), repo_root))
    if in_scope == 0:
        errors.append(f"{root}: no workflow declares a `dry_run` dispatch input, so nothing was checked. If the input was renamed or removed, this guard went with it; update the linter deliberately.")
    return errors


def tokenize(text: str) -> str:
    """Replace each `${{ expr }}` with `@@expr@@`, so a test can see WHICH
    value feeds a command (a swapped output is otherwise invisible)."""
    return re.sub(r"\$\{\{\s*(.*?)\s*\}\}", lambda m: "@@" + " ".join(m.group(1).split()) + "@@", text)


def find_step(text: str, job_name: str, step_name: str) -> tuple[Job, Step]:
    jobs, _ = parse_workflow(text)
    job = next(j for j in jobs if j.name == job_name)
    return job, next(st for st in job.steps if st.label == step_name)


def mutating_runs(text: str, repo_root: Path) -> list[tuple[str, str, dict[str, str]]]:
    jobs, _ = parse_workflow(text)
    out = []
    for job in jobs:
        for step in job.steps:
            if step.run is not None and step_mutations(step, repo_root):
                script = tokenize(step.run)
                env = {
                    k: tokenize(v) for k, v in {**job.env, **step.env}.items() if k != "RELEASE_LIVE"
                }
                out.append((f"{job.name} / {step.label}", script, env))
    return out


# --- simulation of `if:` for one mode --------------------------------------


class _Expr:
    """Evaluator for the `if:` subset workflows here use.

    `||`, `&&`, `!`, `==`, `!=`, parentheses, string literals, `true`/`false`,
    status functions, and dotted context paths resolved by `lookup`. String
    comparison is case-insensitive, as in GitHub Actions.
    """

    TOKEN = re.compile(r"\s*(\|\||&&|==|!=|!|\(|\)|'[^']*'|[A-Za-z_][\w.\-\[\]'*]*(?:\(\))?)")

    def __init__(self, text: str, lookup) -> None:
        self.toks, pos = [], 0
        while pos < len(text):
            m = self.TOKEN.match(text, pos)
            if not m:
                if text[pos:].strip():
                    raise ValueError(f"cannot parse expression: {text!r}")
                break
            self.toks.append(m.group(1))
            pos = m.end()
        self.i, self.lookup = 0, lookup

    def peek(self) -> Optional[str]:
        return self.toks[self.i] if self.i < len(self.toks) else None

    def take(self) -> str:
        self.i += 1
        return self.toks[self.i - 1]

    def parse(self):
        v = self.or_()
        if self.peek() is not None:
            raise ValueError(f"trailing tokens: {self.toks[self.i:]}")
        return v

    def or_(self):
        v = self.and_()
        while self.peek() == "||":
            self.take()
            r = self.and_()
            v = v if self.truthy(v) else r
        return v

    def and_(self):
        v = self.unary()
        while self.peek() == "&&":
            self.take()
            r = self.unary()
            v = r if self.truthy(v) else v
        return v

    def unary(self):
        if self.peek() == "!":
            self.take()
            return not self.truthy(self.unary())
        return self.cmp()

    def cmp(self):
        v = self.atom()
        if self.peek() in ("==", "!="):
            op = self.take()
            r = self.atom()
            eq = self.equal(v, r)
            return eq if op == "==" else not eq
        return v

    @staticmethod
    def _num(v) -> float:
        if isinstance(v, bool):
            return 1.0 if v else 0.0
        if v is None or v == "":
            return 0.0
        try:
            return float(v)
        except (TypeError, ValueError):
            return float("nan")

    @classmethod
    def equal(cls, a, b) -> bool:
        """GitHub Actions `==`: strings compare case-insensitively; mismatched
        types are coerced to numbers, so `'true' == true` is FALSE (NaN vs 1)."""
        if isinstance(a, str) and isinstance(b, str):
            return a.lower() == b.lower()
        if type(a) is type(b):
            return a == b
        return cls._num(a) == cls._num(b)

    def atom(self):
        t = self.take()
        if t == "(":
            v = self.or_()
            self.take()
            return v
        if t.startswith("'"):
            return t[1:-1]
        if t in ("true", "false"):
            return t == "true"
        return self.lookup(t)

    @staticmethod
    def truthy(v) -> bool:
        # As in Actions: false, 0, "" and null are falsy; the STRING 'false'
        # is truthy.
        if isinstance(v, str):
            return v != ""
        if isinstance(v, float) and v != v:
            return False
        return bool(v)


STATUS_FUNCS = ("always()", "success()", "failure()", "cancelled()")


def simulate(text: str, mode: str, fail: Optional[str] = None) -> list[str]:
    """What runs in `mode`; with `fail`, that job fails if it runs."""
    jobs, _ = parse_workflow(text)
    live = "true" if mode == "live" else "false"
    result: dict[str, str] = {}
    ran: list[str] = []
    pending = list(jobs)
    while pending:
        progressed = False
        for job in list(pending):
            if any(d not in result for d in job.needs):
                continue
            pending.remove(job)
            progressed = True
            deps_ok = all(result[d] == "success" for d in job.needs)

            def lookup(path: str, job=job, deps_ok=deps_ok):
                if path == "always()":
                    return True
                if path == "success()":
                    return deps_ok
                if path == "failure()":
                    return any(result.get(d) == "failure" for d in job.needs)
                if path == "cancelled()":
                    return False
                m = re.match(r"needs\.([\w-]+)\.(result|outputs\.([\w-]+))$", path)
                if m:
                    dep, _, out = m.groups()
                    if out is None:
                        return result.get(dep, "")
                    # Outputs written before a job failed are still available.
                    ran_dep = result.get(dep) in ("success", "failure")
                    if dep == MODE_JOB and out == "live":
                        return live if ran_dep else ""
                    return "x" if ran_dep else ""
                if path.startswith("inputs.dry_run"):
                    return "false" if mode == "live" else "true"
                if path.startswith("steps.") and path.endswith(".outputs.live"):
                    return live
                return "x" if path.startswith(("steps.", "github.", "secrets.")) else ""

            def runs(expr: str, lookup=lookup, deps_ok=deps_ok) -> bool:
                e = _unwrap(expr) if expr else ""
                if not e:
                    return deps_ok
                v = _Expr(e, lookup).parse()
                implicit = not any(f in e for f in STATUS_FUNCS)
                return _Expr.truthy(v) and (deps_ok or not implicit)

            if runs(job.keys.get("if", "")):
                result[job.name] = "failure" if job.name == fail else "success"
                ran.append(job.name)
                for step in job.steps:
                    if runs(step.keys.get("if", ""), deps_ok=True):
                        ran.append(f"{job.name} / {step.label}")
            else:
                result[job.name] = "skipped"
        if not progressed:
            raise ValueError(f"unresolvable needs: {[j.name for j in pending]}")
    return ran


# --- self-test -------------------------------------------------------------

_GOOD = """\
name: Release
on:
  workflow_dispatch:
    inputs:
      dry_run:
        type: boolean
        default: false
jobs:
  validate:
    runs-on: ubuntu-latest
    outputs:
      live: ${{ steps.mode.outputs.live }}
    steps:
      - name: Checkout
        uses: actions/checkout@v7
        with:
          persist-credentials: false
      - name: Resolve run mode
        id: mode
        env:
          DRY_RUN_INPUT: ${{ inputs.dry_run }}
        run: bash scripts/resolve_release_mode.sh
  bump:
    runs-on: ubuntu-latest
    needs: validate
    outputs:
      pr: ${{ steps.open.outputs.pr }}
    steps:
      - uses: actions/checkout@v7
        with:
          token: ${{ (needs.validate.outputs.live == 'true' && secrets.RELEASE_PAT) || secrets.GITHUB_TOKEN }}
          persist-credentials: ${{ needs.validate.outputs.live == 'true' }}
      - name: Commit locally
        run: git commit -am bump
      - name: Push
        if: needs.validate.outputs.live == 'true'
        env:
          RELEASE_LIVE: ${{ needs.validate.outputs.live }}
        run: |
          [ "${RELEASE_LIVE:-}" = "true" ] || { echo "::error::refusing to push in a dry run"; exit 1; }
          git push origin HEAD
      - name: Open PR
        id: open
        if: ${{ always() && needs.validate.outputs.live == 'true' }}
        env:
          GH_TOKEN: ${{ secrets.RELEASE_PAT || secrets.GITHUB_TOKEN }}
          RELEASE_LIVE: ${{ needs.validate.outputs.live }}
        run: |
          [ "${RELEASE_LIVE:-}" = "true" ] || { echo "::error::refusing in a dry run"; exit 1; }
          gh pr create \\
            --title x --body y
          echo "pr=1" >> "$GITHUB_OUTPUT"
  rehearse:
    runs-on: ubuntu-latest
    needs: [validate, bump]
    if: needs.validate.outputs.live != 'true'
    permissions:
      contents: read
    steps:
      - uses: actions/checkout@v7
        with:
          persist-credentials: false
      - name: Package
        run: cargo publish -p freenet --dry-run
      - name: Read releases
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
        run: gh release list
  tag:
    runs-on: ubuntu-latest
    needs: [validate, bump]
    if: always() && needs.validate.outputs.live == 'true'
    env:
      RELEASE_LIVE: ${{ needs.validate.outputs.live }}
    steps:
      - uses: actions/checkout@v7
      - name: Notes
        run: |
          # dry-run-safe: renders text only
          gh api repos/:owner/:repo/releases/generate-notes \\
            -f tag_name=v1
      - name: Tag
        run: |
          [ "${RELEASE_LIVE:-}" = "true" ] || { echo "::error::refusing in a dry run"; exit 1; }
          git tag v1 && git push origin v1 ${{ needs.bump.outputs.pr }}
"""


def _expect(errors: list[str], needle: Optional[str], case: str) -> Optional[str]:
    if needle is None:
        return None if not errors else f"{case}: expected clean, got:\n  " + "\n  ".join(errors)
    if not any(needle in e for e in errors):
        return f"{case}: expected an error containing {needle!r}, got: {errors}"
    return None


def self_test() -> int:
    failures: list[str] = []
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "scripts").mkdir()
        (root / ".github/scripts").mkdir(parents=True)
        (root / "scripts/resolve_release_mode.sh").write_text("echo live=true\n")
        (root / "scripts/sneaky.sh").write_text("#!/bin/sh\n# git push is fine in a comment\ngit push origin x\n")
        (root / ".github/scripts/sneaky.py").write_text("import subprocess\nsubprocess.run(['git', 'push', 'origin', 'x'])\n")
        (root / "scripts/reader.sh").write_text("gh api graphql -f query='query{viewer{login}}'\n")
        (root / "scripts/errmsg.sh").write_text('echo "::error::re-run it with \\`gh run rerun --failed\\`"\n')

        def run(case: str, text: str, needle: Optional[str]) -> None:
            if text == _GOOD and needle is not None:
                failures.append(f"{case}: mutation did not apply")
                return
            f = _expect(check_source(text, "wf.yml", root), needle, case)
            if f:
                failures.append(f)

        def body(cmd: str) -> str:
            return _GOOD.replace("run: git commit -am bump", cmd)

        unguard_push = lambda s: s.replace("        if: needs.validate.outputs.live == 'true'\n        env:\n          RELEASE_LIVE: ${{ needs.validate.outputs.live }}\n        run: |\n          [", "        env:\n          RELEASE_LIVE: ${{ needs.validate.outputs.live }}\n        run: |\n          [")

        run("good workflow passes", _GOOD, None)
        if check_source(_GOOD.replace("      dry_run:\n", "      other:\n"), "wf.yml", root):
            failures.append("a workflow without a dry_run input should be out of scope")

        # Rule 3: the #5936 shape and its disguises.
        run("unguarded push is flagged", unguard_push(_GOOD), "not gated")
        run("a guard under || is not a guard",
            _GOOD.replace("if: needs.validate.outputs.live == 'true'\n        env:", "if: needs.validate.outputs.live == 'true' || true\n        env:"), "not gated")
        run("a guard ANDed then ORed is not a guard (&& binds tighter)",
            _GOOD.replace("if: needs.validate.outputs.live == 'true'\n        env:", "if: always() || failure() && needs.validate.outputs.live == 'true'\n        env:"), "not gated")
        run("a negated guard is not a guard",
            _GOOD.replace("if: needs.validate.outputs.live == 'true'\n        env:", "if: needs.validate.outputs.live != 'true'\n        env:"), "not gated")
        run("job-level guard removed exposes the tag push",
            _GOOD.replace("    if: always() && needs.validate.outputs.live == 'true'\n", ""), "not gated")

        # Rule 2: every spelling of the input.
        run("per-step inputs.dry_run is refused",
            _GOOD.replace("if: needs.validate.outputs.live == 'true'\n        env:", "if: ${{ !inputs.dry_run }}\n        env:"), "read outside the mode step")
        run("github.event.inputs.dry_run is refused", body("run: echo ${{ github.event.inputs.dry_run }}"), "read outside the mode step")
        run("inputs.DRY_RUN is refused (case-insensitive)", body("run: echo ${{ inputs.DRY_RUN }}"), "read outside the mode step")
        run("inputs['dry_run'] is refused", body("run: echo ${{ inputs['dry_run'] }}"), "read outside the mode step")

        # Rule 4: the in-script refusal.
        run("missing in-script refusal is flagged",
            _GOOD.replace('          [ "${RELEASE_LIVE:-}" = "true" ] || { echo "::error::refusing to push in a dry run"; exit 1; }\n', ""),
            "does not refuse in its own script")
        run("a refusal AFTER the mutation does not count",
            _GOOD.replace(
                '          [ "${RELEASE_LIVE:-}" = "true" ] || { echo "::error::refusing to push in a dry run"; exit 1; }\n          git push origin HEAD\n',
                '          git push origin HEAD\n          [ "${RELEASE_LIVE:-}" = "true" ] || { echo "::error::late"; exit 1; }\n'),
            "does not refuse in its own script")
        run("a refusal without exit is not a refusal",
            _GOOD.replace('echo "::error::refusing to push in a dry run"; exit 1; }', 'echo "warn"; }'),
            "does not refuse in its own script")
        run("RELEASE_LIVE must be wired",
            _GOOD.replace("        env:\n          RELEASE_LIVE: ${{ needs.validate.outputs.live }}\n        run: |\n          [ \"${RELEASE_LIVE:-}\" = \"true\" ] || { echo \"::error::refusing to push",
                          "        run: |\n          [ \"${RELEASE_LIVE:-}\" = \"true\" ] || { echo \"::error::refusing to push"),
            "not wired")

        # What counts as a mutation.
        run("a mutation hidden in a called script is found", body("run: bash scripts/sneaky.sh"), "via scripts/sneaky.sh")
        run("a mutation in a .github/scripts python file is found", body("run: python3 .github/scripts/sneaky.py"), "via .github/scripts/sneaky.py")
        run("$GITHUB_WORKSPACE/scripts is followed", body('run: bash "$GITHUB_WORKSPACE/scripts/sneaky.sh"'), "via scripts/sneaky.sh")
        run("a read-only GraphQL query is not a mutation", body("run: bash scripts/reader.sh"), None)
        run("a GraphQL mutation is", body("run: gh api graphql -f query='mutation{x}'"), "gh api write")
        run("a GraphQL query from a file is treated as a mutation", body("run: gh api graphql -F query=@q.graphql"), "gh api write")
        run("a later gh api write on the same line is seen", body("run: gh api -X GET a; gh api -X DELETE b"), "gh api write")
        run("gh api POST via fields is a mutation without the marker",
            _GOOD.replace("          # dry-run-safe: renders text only\n", "").replace(
                "    if: always() && needs.validate.outputs.live == 'true'\n", "    if: always()\n"),
            "gh api write")
        run("the marker does not cover a chained command",
            _GOOD.replace("-f tag_name=v1\n", "-f tag_name=v1; git push origin x\n").replace(
                "    if: always() && needs.validate.outputs.live == 'true'\n", "    if: always()\n"),
            "git push")
        run("an error message that names a command is not a mutation", body("run: bash scripts/errmsg.sh"), None)
        run("an echo naming a command is not a mutation", body('run: echo "re-run it with gh run rerun, or git push by hand"'), None)
        run("a command substitution inside double quotes still is", body('run: X="$(gh release create v1)"'), "gh release write")
        run("bash -c '...' is seen", body("run: bash -c 'git push origin HEAD'"), "git push")
        run("eval \"...\" is seen", body('run: eval "git push origin HEAD"'), "git push")
        run("a command held in a variable is seen", body('run: CMD="gh pr merge 1 --auto"; $CMD'), "gh pr write")
        run("a YAML-quoted single-line run is seen", body('run: "gh pr merge 1 --auto"'), "gh pr write")
        # Round-2 review: shapes that slipped past the first rewrite.
        run("gh flags before the verb", body("run: gh pr --repo o/r merge 1 --auto"), "gh pr write")
        run("gh -R before the verb", body("run: gh pr -R o/r merge 1"), "gh pr write")
        run("gh pr update-branch is a write", body("run: gh pr update-branch 1"), "gh pr write")
        run("gh api with an attached method", body("run: gh api -XPUT repos/o/r/pulls/1/merge"), "gh api write")
        run("a GraphQL query from a variable is uninspectable", body('run: gh api graphql -f query="$Q"'), "gh api write")
        run("cargo with a toolchain before publish", body("run: cargo +1.93.0 publish -p freenet"), "cargo publish")
        run("curl -d@file is a write", body("run: curl -d@body.json https://x"), "curl write")
        run("python calling gh", body("run: python3 -c \"import subprocess; subprocess.run(['gh', 'pr', 'merge'])\""), "python gh call")
        run("echo piped into a shell", body('run: echo "gh pr merge 1 --auto" | bash'), "piped into a shell")
        run("printf piped into a shell", body("run: printf 'git push\\n' | sh"), "piped into a shell")
        run("a command chained after an echo is scanned raw", body("run: echo hi; bash -c 'git push origin x'"), "git push")
        run("a script a message mentions is not followed", body('run: echo "for a local rehearsal use scripts/sneaky.sh"'), None)
        run("a script a trailing comment mentions is not followed", body("run: true  # mirrors scripts/sneaky.sh"), None)
        run("curl piped into grep is a read", body('run: curl -sS https://x | grep -d skip foo'), None)
        run("a cat heredoc body is data",
            body("run: |\n          cat > f <<'EOF'\n          then gh pr merge it\n          EOF\n          true"), None)
        run("the marker covers only a gh api call",
            _GOOD.replace("          gh api repos/:owner/:repo/releases/generate-notes \\\n            -f tag_name=v1\n", "          git push origin HEAD\n").replace(
                "    if: always() && needs.validate.outputs.live == 'true'\n", "    if: always()\n"),
            "git push")
        run("the marker does not cover a pipe",
            _GOOD.replace("-f tag_name=v1\n", "-f tag_name=v1 | xargs gh pr merge\n").replace(
                "    if: always() && needs.validate.outputs.live == 'true'\n", "    if: always()\n"),
            "piped into a shell")
        run("an action whose name only starts with an allowlisted one is not trusted",
            body("uses: actions/checkout-helper@v1"), "read-only action allowlist")
        run("a look-alike checkout does not get the checkout exemption",
            _GOOD.replace("      - uses: actions/checkout@v7\n        with:\n          token: ${{ (needs",
                          "      - uses: actions/checkout-helper@v7\n        with:\n          token: ${{ (needs"),
            "read-only action allowlist")
        run("permissions: {} is read-only",
            _GOOD.replace("    permissions:\n      contents: read\n", "    permissions: {}\n").replace(
                "      - name: Read releases\n        env:\n          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}\n        run: gh release list\n", ""), None)
        run("Swatinem/rust-cache saves a shared cache", body("uses: Swatinem/rust-cache@v2"), "read-only action allowlist")
        # One positive case per mutating pattern, so deleting any one of them
        # fails this self-test.
        for label, cmd in [
            ("gh workflow write", "gh workflow run x.yml"),
            ("gh run write", "gh run rerun 1 --failed"),
            ("gh issue/label/secret/variable/repo/cache/project write", "gh issue create -t x"),
            ("gh issue/label/secret/variable/repo/cache/project write", "gh label create x"),
            ("gh issue/label/secret/variable/repo/cache/project write", "gh secret set X"),
            ("gh issue/label/secret/variable/repo/cache/project write", "gh variable set X"),
            ("gh issue/label/secret/variable/repo/cache/project write", "gh repo edit --visibility public"),
            ("gh issue/label/secret/variable/repo/cache/project write", "gh cache delete --all"),
            ("gh release write", "gh release upload v1 f"),
            ("cargo yank/owner", "cargo yank --version 1.0.0"),
            ("container push", "docker push img"),
            ("container push", "podman push img"),
            ("npm publish", "npm publish"),
        ]:
            run(f"positive case: {cmd}", body(f"run: {cmd}"), label)

        # Structural rules, one case each.
        run("a workflow without the mode job", _GOOD.replace("  validate:\n", "  check:\n", 1), "no `validate` job")
        run("the mode step must read the input",
            _GOOD.replace("DRY_RUN_INPUT: ${{ inputs.dry_run }}", "DRY_RUN_INPUT: 'false'"), "DRY_RUN_INPUT")
        run("the mode step must come first",
            _GOOD.replace("      - name: Resolve run mode\n", "      - name: a\n        run: true\n      - name: b\n        run: true\n      - name: Resolve run mode\n"),
            "first two steps")
        run("a reusable workflow in a dry-run job",
            _GOOD.replace("  rehearse:\n", "  reuse:\n    needs: validate\n    uses: ./.github/workflows/x.yml\n  rehearse:\n"), "reusable workflow")
        run("an unquoted boolean comparison",
            _GOOD.replace("if: needs.validate.outputs.live != 'true'", "if: needs.validate.outputs.live != true"), "unquoted boolean")
        run("a step output read before it is written",
            _GOOD.replace('          gh pr create \\\n            --title x --body y\n          echo "pr=1" >> "$GITHUB_OUTPUT"\n',
                          '          gh pr create --title ${{ steps.open.outputs.pr }}\n          echo "pr=1" >> "$GITHUB_OUTPUT"\n'),
            "before step `open` has run")
        run("a live push with no credential to push with",
            _GOOD.replace("          persist-credentials: ${{ needs.validate.outputs.live == 'true' }}\n", "          persist-credentials: false\n"),
            "cannot authenticate")
        run("the marker covers only generate-notes",
            _GOOD.replace("releases/generate-notes", "pulls/1/merge").replace(
                "    if: always() && needs.validate.outputs.live == 'true'\n", "    if: always()\n"),
            "gh api write")
        run("an unknown action is a mutation", body("uses: softprops/action-gh-release@v2"), "read-only action allowlist")
        run("actions/cache is not read-only", body("uses: actions/cache@v4"), "read-only action allowlist")
        run("cargo publish --dry-run is not a mutation", body("run: cargo publish -p freenet --dry-run"), None)
        run("cargo publish is", body("run: cargo publish -p freenet"), "cargo publish")
        run("a dry-run publish does not excuse a real one on the same line",
            body("run: cargo publish -p a --dry-run && cargo publish -p a"), "cargo publish")
        run("git -C dir push is still a push", body("run: git -C repo push origin x"), "git push")
        run("curl --request=POST is a write", body("run: curl --request=POST https://x"), "curl write")
        run("python requests.post is a write", body("run: python3 -c 'import requests; requests.post(u)'"), "python HTTP write")

        # Rule 5.
        run("a persisted credential in a dry-run job is flagged",
            _GOOD.replace("          persist-credentials: ${{ needs.validate.outputs.live == 'true' }}\n", ""), "persist-credentials")
        with_env = lambda v: _GOOD.replace("      - name: Commit locally\n        run:", "      - name: Commit locally\n        env:\n          GH_TOKEN: " + v + "\n        run:")
        run("RELEASE_PAT on an unguarded step is flagged", with_env("${{ secrets.RELEASE_PAT }}"), "receives a credential")
        run("the workflow token on an unguarded step of a write job is flagged", with_env("${{ github.token }}"), "receives a credential")
        run("secrets.GITHUB_TOKEN likewise", with_env("${{ secrets.GITHUB_TOKEN }}"), "receives a credential")
        run("a lower-case secret name is still a secret", with_env("${{ secrets.release_pat }}"), "receives a credential")
        run("index syntax is still a secret", with_env("${{ secrets['RELEASE_PAT'] }}"), "receives a credential")
        run("toJSON(secrets) is every secret", with_env("${{ toJSON(secrets) }}"), "receives a credential")
        run("a presence test hands over nothing", with_env("${{ secrets.RELEASE_PAT != '' }}"), None)
        run("a message naming RELEASE_PAT hands over nothing", body('run: echo "RELEASE_PAT not set"'), None)
        run("RELEASE_PAT inline in an unguarded run is flagged",
            body("run: echo ${{ secrets.RELEASE_PAT }} > /tmp/t"), "receives a credential")
        run("RELEASE_PAT in a dry-run job's env is flagged even when read-only",
            _GOOD.replace("    permissions:\n      contents: read\n", "    permissions:\n      contents: read\n    env:\n      GH_TOKEN: ${{ secrets.RELEASE_PAT || secrets.GITHUB_TOKEN }}\n"),
            "job-level source")
        run("a workflow-level secret reaches every dry-run job",
            _GOOD.replace("jobs:\n", "env:\n  GH_TOKEN: ${{ secrets.RELEASE_PAT }}\njobs:\n", 1), "workflow-level source")
        run("a dry-run checkout may not carry RELEASE_PAT",
            _GOOD.replace("token: ${{ (needs.validate.outputs.live == 'true' && secrets.RELEASE_PAT) || secrets.GITHUB_TOKEN }}",
                          "token: ${{ secrets.RELEASE_PAT || secrets.GITHUB_TOKEN }}"), "a checkout may take exactly")
        run("a dry-run-only job must stay read-only",
            _GOOD.replace("      contents: read\n", "      contents: write\n"), "read-only `permissions:`")
        run("a dry-run-only job must declare permissions",
            _GOOD.replace("    permissions:\n      contents: read\n", ""), "read-only `permissions:`")

        # Rule 6.
        run("guard needs validate in scope", _GOOD.replace("    needs: [validate, bump]\n    if: always()", "    needs: [bump]\n    if: always()"), "does not list `validate`")
        run("a job output that is not declared", _GOOD.replace("      pr: ${{ steps.open.outputs.pr }}\n", ""), "does not declare")
        run("a step output that is never written", _GOOD.replace('          echo "pr=1" >> "$GITHUB_OUTPUT"\n', ""), "never writes `pr=`")
        run("a step output from a step that does not exist", _GOOD.replace("        id: open\n", ""), "has no step `id: open`")

        # Round 3: credentials the parsed values hid, and read-only jobs.
        run("a read-only job may not take another secret",
            _GOOD.replace("          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}\n        run: gh release list",
                          "          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}\n          CARGO_REGISTRY_TOKEN: ${{ secrets.CARGO_REGISTRY_TOKEN }}\n        run: gh release list"),
            "receives a credential")
        run("a folded env value still hands over the secret",
            with_env(">-\n            ${{ secrets.RELEASE_PAT }}"), "receives a credential")
        run("a flow-style env still hands over the secret",
            _GOOD.replace("      - name: Commit locally\n        run:", "      - name: Commit locally\n        env: { GH_TOKEN: \"${{ secrets.RELEASE_PAT }}\" }\n        run:"),
            "receives a credential")
        run("a folded checkout token is not the allowed one-liner",
            _GOOD.replace("token: ${{ (needs.validate.outputs.live == 'true' && secrets.RELEASE_PAT) || secrets.GITHUB_TOKEN }}",
                          "token: >-\n            ${{ secrets.RELEASE_PAT }}"),
            "receives a credential")
        run("an unquoted cat heredoc body is executed, so it is scanned",
            body("run: |\n          cat > f <<EOF\n          $(gh pr merge 1 --auto)\n          EOF"), "gh pr write")

        # Round 4: credential routes that bypassed the scan.
        run("a YAML alias copying a live step's env is refused",
            _GOOD.replace("        env:\n          GH_TOKEN: ${{ secrets.RELEASE_PAT || secrets.GITHUB_TOKEN }}\n          RELEASE_LIVE:",
                          "        env: &ghenv\n          GH_TOKEN: ${{ secrets.RELEASE_PAT || secrets.GITHUB_TOKEN }}\n          RELEASE_LIVE:"),
            "anchors and aliases")
        run("toJSON(github) carries the workflow token", with_env("${{ toJSON(github) }}"), "receives a credential")
        run("github.sha is not a credential", with_env("${{ github.sha }}"), None)
        run("a job-level env written after steps: is scanned",
            _GOOD.replace("  rehearse:\n", "    env:\n      GH_TOKEN: ${{ secrets.RELEASE_PAT }}\n  rehearse:\n", 1),
            "job-level source")
        run("a workflow-level env written after jobs: is scanned",
            _GOOD + "env:\n  GH_TOKEN: ${{ secrets.RELEASE_PAT }}\n", "workflow-level source")
        run("a quoted permission key still counts",
            _GOOD.replace("      contents: read\n", "      actions: read\n      \"contents\": write\n"), "read-only `permissions:`")
        run("id-token: write in a dry-run job is refused",
            _GOOD.replace("jobs:\n", "permissions:\n  id-token: write\njobs:\n", 1), "id-token")
        run("the mode step runs exactly the resolver",
            _GOOD.replace("run: bash scripts/resolve_release_mode.sh", "run: bash scripts/resolve_release_mode.sh; echo live=true >> $GITHUB_OUTPUT"),
            "resolve_release_mode.sh")
        run("continue-on-error on a step is refused", body("continue-on-error: true\n        run: true"), "continue-on-error")
        run("continue-on-error on a job is refused",
            _GOOD.replace("  rehearse:\n    runs-on: ubuntu-latest\n", "  rehearse:\n    runs-on: ubuntu-latest\n    continue-on-error: true\n"), "continue-on-error")
        run("toJSON(github.event) reads the input", body("run: echo '${{ toJSON(github.event) }}'"), "read outside the mode step")

        # Round 5.
        run("a flow-style job is reported",
            _GOOD + "  sync: {runs-on: ubuntu-latest, needs: validate, steps: [{run: git push}]}\n", "cannot place this line as a job")
        run("a job with keys at 6 spaces is reported",
            _GOOD + "  sync:\n      runs-on: ubuntu-latest\n      steps:\n        - run: git push\n", "no steps or `uses:` could be parsed")
        run("a string literal naming .github is not the github context",
            with_env("${{ hashFiles('.github/workflows/release.yml') }}"), None)
        run("github.event.inputs.version is not dry_run",
            body("run: echo '${{ github.event.inputs.version }}'"), None)
        run("github['token'] is still the token", with_env("${{ github['token'] }}"), "receives a credential")
        run("markdown italics in a script are not an alias",
            body("run: |\n          cat > f <<'EOF'\n          *Do not edit by hand.*\n          EOF"), None)

        # Rule 1.
        run("the mode step must exist", _GOOD.replace("        id: mode\n", ""), "no step with `id: mode`")
        run("the mode step must use the tested resolver",
            _GOOD.replace("run: bash scripts/resolve_release_mode.sh", "run: echo live=true >> $GITHUB_OUTPUT"), "resolve_release_mode.sh")
        run("the mode output must be exported", _GOOD.replace("      live: ${{ steps.mode.outputs.live }}\n", ""), "must export")

        # Rule 7.
        run("steps the reader cannot place are an error",
            _GOOD.replace("      - name: Commit locally\n        run: git commit -am bump\n",
                          "    - name: Commit locally\n      run: git push origin x\n").replace(
                "    steps:\n      - uses: actions/checkout@v7\n        with:\n          token:", "    steps:\n    - name: shifted\n      run: git push\n      - uses: actions/checkout@v7\n        with:\n          token:"),
            "cannot place this line")
        run("a flow-style step is reported, not skipped",
            body('run: true\n      - { name: Sneaky, run: "gh pr merge 1 --auto" }'), "cannot place this line")
        run("flow-style steps: [...] is reported", _GOOD.replace(
            "  rehearse:\n", "  inline:\n    needs: validate\n    steps: [{run: 'git push origin x'}]\n  rehearse:\n"), "cannot place this line")
        run("toJSON(inputs) reads the input too", body("run: echo '${{ toJSON(inputs) }}'"), "read outside the mode step")
        run("a GraphQL query from --input is uninspectable", body("run: gh api graphql --input q.json"), "gh api write")
        run("a refusal inside an if-block is not a refusal",
            _GOOD.replace('          [ "${RELEASE_LIVE:-}" = "true" ] || { echo "::error::refusing to push in a dry run"; exit 1; }\n',
                          '          if false; then\n          [ "${RELEASE_LIVE:-}" = "true" ] || { echo "::error::refusing to push in a dry run"; exit 1; }\n          fi\n'),
            "does not refuse in its own script")
        run("run: |2 is parsed", _GOOD.replace("run: |\n          [ \"${RELEASE_LIVE:-}\" = \"true\" ] || { echo \"::error::refusing to push", "run: |2\n          [ \"${RELEASE_LIVE:-}\" = \"true\" ] || { echo \"::error::refusing to push").replace("        if: needs.validate.outputs.live == 'true'\n        env:\n          RELEASE_LIVE: ${{ needs.validate.outputs.live }}\n        run: |2", "        env:\n          RELEASE_LIVE: ${{ needs.validate.outputs.live }}\n        run: |2"), "not gated")

        # Scope: a directory with nothing in scope is a failure, not a pass.
        empty = root / "wf"
        empty.mkdir()
        (empty / "x.yml").write_text(_GOOD.replace("      dry_run:\n", "      rehearse:\n"))
        if not any("nothing was checked" in e for e in check_dir(empty, root)):
            failures.append("scope: a directory with no dry_run workflow must fail")

        # Parser and simulator sanity.
        labels = [l for l, _, _ in mutating_runs(_GOOD, root)]
        if labels != ["bump / Push", "bump / Open PR", "tag / Tag"]:
            failures.append(f"extractor: expected the three mutating steps, got {labels}")
        if conjuncts("${{ always() && (a || b) && needs.validate.outputs.live == 'true' }}") != ["always()", "(a || b)", GUARD]:
            failures.append("conjuncts: top-level && split is wrong")
        if conjuncts("a && b || c"):
            failures.append("conjuncts: a top-level || must yield no guard")
        live = simulate(_GOOD, "live")
        dry = simulate(_GOOD, "dry")
        if [j for j in live if "/" not in j] != ["validate", "bump", "tag"]:
            failures.append(f"simulate live: wrong jobs {live}")
        if [j for j in dry if "/" not in j] != ["validate", "bump", "rehearse"]:
            failures.append(f"simulate dry: wrong jobs {dry}")
        if "bump / Push" not in live or "bump / Push" in dry:
            failures.append("simulate: the push step must run live only")

        # Evaluator semantics that differ from naive Python.
        ev = lambda e: _Expr.truthy(_Expr(e, lambda p: "true").parse())
        if ev("'true' == true"):
            failures.append("evaluator: 'true' == true must be false, as in Actions")
        if not ev("'false'"):
            failures.append("evaluator: the string 'false' must be truthy, as in Actions")
        # As in Actions, outputs a job set before failing are still visible,
        # so an `always()` job gated only on `live` runs after a failed
        # validate. release.yml's mutating jobs also need a SUCCESSFUL
        # upstream job, which the release test pins; the simulator must not
        # hide this case by modelling the outputs as gone.
        after = [j for j in simulate(_GOOD, "live", fail="validate") if "/" not in j]
        if after != ["validate", "tag"]:
            failures.append(f"simulate: a failed validate keeps its outputs (got {after})")
        # ...while a failed bump does NOT stop `tag`, whose `if:` has always():
        # the simulator must model that, or it would vouch for gates it cannot see.
        # A dependent of a SKIPPED job is skipped too, unless it uses a status
        # function. In dry mode `bump` runs but nothing depends on `rehearse`;
        # add one that does and check it is skipped live.
        chained = _GOOD + "  after:\n    needs: [rehearse]\n    steps:\n      - run: true\n"
        if "after" in simulate(chained, "live"):
            failures.append("simulate: a dependent of a skipped job must be skipped")
        if "after" not in simulate(chained, "dry"):
            failures.append("simulate: a dependent of a successful job must run")
        if "tag" not in simulate(_GOOD, "live", fail="bump"):
            failures.append("simulate: always() must run after a failed dependency")

    if failures:
        for f in failures:
            print(f"SELF-TEST FAIL: {f}", file=sys.stderr)
        return 1
    print("check_release_dry_run self-test passed")
    return 0


def main(argv: list[str]) -> int:
    repo_root = Path(__file__).resolve().parents[2]
    if argv[1:2] == ["--self-test"]:
        return self_test()
    if argv[1:2] == ["--extract-mutating-runs"] and len(argv) == 4:
        out = Path(argv[3])
        out.mkdir(parents=True, exist_ok=True)
        for n, (label, script, env) in enumerate(mutating_runs(Path(argv[2]).read_text(), repo_root)):
            (out / f"{n:02d}.sh").write_text(script + "\n")
            (out / f"{n:02d}.name").write_text(label + "\n")
            (out / f"{n:02d}.env").write_text("".join(f"{k}={v}\n" for k, v in env.items()))
        return 0
    if argv[1:2] == ["--simulate"] and len(argv) in (4, 6) and argv[3] in ("live", "dry"):
        fail = argv[5] if len(argv) == 6 and argv[4] == "--fail" else None
        for line in simulate(Path(argv[2]).read_text(), argv[3], fail):
            print(line)
        return 0
    if argv[1:2] == ["--dump-step"] and len(argv) == 5:
        # The step's script, then its job+step env as KEY=value lines, both
        # tokenized; separated by a line `@@ENV@@`.
        job, step = find_step(Path(argv[2]).read_text(), argv[3], argv[4])
        print(tokenize(step.run or ""))
        print("@@ENV@@")
        for k, v in {**job.env, **step.env}.items():
            print(f"{k}={tokenize(v)}")
        return 0
    root = Path(argv[1]) if len(argv) > 1 else repo_root / ".github/workflows"
    errors = check_dir(root, repo_root)
    if errors:
        print("::error::release dry_run guard check failed (#5936).")
        for e in errors:
            print(f"  {e}")
        return 1
    print("Release dry_run guard check passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))

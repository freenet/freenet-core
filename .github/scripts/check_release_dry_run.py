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
  2. `inputs.dry_run` is read NOWHERE else. A second reader is how the guards
     drifted apart in the first place.
  3. Every MUTATING step is gated on `needs.validate.outputs.live == 'true'`,
     as a top-level `&&` conjunct of its own `if:` or its job's `if:`. A
     missing or empty output therefore SKIPS the step: the guard fails closed.
  4. Every mutating `run:` step also refuses in its own script, before its
     first mutating command, unless `RELEASE_LIVE` (wired from the same
     output) is `true`. So deleting an `if:` turns a dry run red instead of
     letting it mutate.
  5. In a job a dry run can reach, `actions/checkout` does not persist a
     credential (so a stray `git push` cannot authenticate), and RELEASE_PAT
     is handed only to live-gated steps.
  6. A job that reads `needs.validate.*` lists `validate` in `needs:`; without
     it the expression is silently empty and a live release would skip steps.

"Mutating" is decided from the step's source, not from its name:
`git push`; `gh pr|release|workflow|run|issue|label|secret|variable|repo|cache`
write verbs; `gh api` with a non-GET method, request fields, or a GraphQL
`mutation`; `cargo publish` (without `--dry-run`), `cargo yank|owner`;
`docker|podman push`; `npm publish`; `curl` with a write method or a body.
A `run:` that invokes a `scripts/*.sh|py` file is scanned through that file.
A `uses:` step is mutating unless its action is on a short read-only allowlist,
and a job-level `uses:` (reusable workflow) always is.

Escape hatch, for a call that is POST-shaped but writes nothing (GitHub's
`releases/generate-notes` renders text and creates nothing): a shell comment
line containing `dry-run-safe: <reason>` immediately above the command. It is
visible in review, which is the point.

Scope: every workflow that declares a `workflow_dispatch` input named
`dry_run`. Today that is `release.yml` only.

Usage:
    check_release_dry_run.py [WORKFLOW_DIR]       # lint (default .github/workflows)
    check_release_dry_run.py --self-test
    check_release_dry_run.py --extract-mutating-runs WORKFLOW OUTDIR
        Writes each mutating `run:` script (expressions replaced by
        `PLACEHOLDER`) to OUTDIR/NN.sh and its "job / step" label to
        OUTDIR/NN.name, so scripts/release_dry_run_test.sh can EXECUTE them
        and prove each one refuses in a dry run.

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
# RELEASE_PAT used AS a value (bare or in the `||` coalesce), as opposed to a
# presence test such as `secrets.RELEASE_PAT != ''`, which hands over nothing.
PAT_VALUE_RE = re.compile(r"secrets\.RELEASE_PAT\s*(\|\||\}\})")
DRY_RUN_INPUT_RE = re.compile(r"\binputs\.dry_run\b")
EXEMPT_MARKER = "dry-run-safe:"

READ_ONLY_ACTIONS = (
    "actions/checkout",
    "actions/upload-artifact",
    "actions/download-artifact",
    "actions/cache",
    "actions/setup-",
    "dtolnay/rust-toolchain",
    "Swatinem/rust-cache",
)

MUTATING_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("git push", re.compile(r"\bgit\b(\s+-\S+(\s+\S+)?)*\s+push\b")),
    (
        "gh pr write",
        re.compile(
            r"\bgh\s+pr\s+(create|merge|edit|close|reopen|comment|review|ready|lock|unlock)\b"
        ),
    ),
    (
        "gh release write",
        re.compile(r"\bgh\s+release\s+(create|edit|upload|delete|delete-asset)\b"),
    ),
    ("gh workflow write", re.compile(r"\bgh\s+workflow\s+(run|enable|disable)\b")),
    ("gh run write", re.compile(r"\bgh\s+run\s+(rerun|cancel|delete)\b")),
    (
        "gh issue/label/secret/variable/repo/cache write",
        re.compile(
            r"\bgh\s+(issue\s+(create|edit|comment|close|reopen|delete|transfer|pin|unpin|lock|unlock)"
            r"|label\s+(create|edit|delete|clone)|secret\s+(set|delete)|variable\s+(set|delete)"
            r"|repo\s+(create|edit|delete|rename|archive|fork|sync)|cache\s+delete)\b"
        ),
    ),
    ("cargo yank/owner", re.compile(r"\bcargo\s+(yank|owner)\b")),
    ("container push", re.compile(r"\b(docker|podman)\s+(image\s+)?push\b")),
    ("npm publish", re.compile(r"\bnpm\s+publish\b")),
    (
        "curl write",
        re.compile(
            r"\bcurl\b.*(\s(-X|--request)\s*['\"]?(POST|PUT|PATCH|DELETE)"
            r"|\s(-d|--data(-[a-z]+)?|--json|-F|--form|-T|--upload-file)(\s|=))"
        ),
    ),
]

SCRIPT_REF_RE = re.compile(r"(?<![\w./-])(?:\./)?(scripts/[\w./-]+\.(?:sh|py))\b")


# --- a deliberately small YAML reader --------------------------------------
#
# No PyYAML: the other linters here are dependency-free and run on a bare
# runner. GitHub workflows in this repo use a fixed layout (jobs at 2 spaces,
# job keys at 4, steps at `      - `, step keys at 8), and the self-test pins
# that this reader sees what it should. Anything it cannot place is ignored,
# which is why rule 3 is phrased as "must be guarded" rather than "must not
# exist": an unparsed step is reported as unguarded, not waved through.


@dataclass
class Step:
    job: str
    line: int
    keys: dict[str, str] = field(default_factory=dict)
    env: dict[str, str] = field(default_factory=dict)
    with_: dict[str, str] = field(default_factory=dict)
    run: Optional[str] = None
    run_line: int = 0

    @property
    def label(self) -> str:
        return self.keys.get("name") or self.keys.get("id") or self.keys.get("uses") or "?"


@dataclass
class Job:
    name: str
    line: int
    end: int
    keys: dict[str, str] = field(default_factory=dict)
    env: dict[str, str] = field(default_factory=dict)
    needs: list[str] = field(default_factory=list)
    header: str = ""
    steps: list[Step] = field(default_factory=list)


def _strip_comment(value: str) -> str:
    return re.sub(r"\s+#.*$", "", value).strip()


def _block_scalar(lines: list[str], start: int, min_indent: int) -> tuple[str, int]:
    """Collect lines after `start` indented deeper than `min_indent`."""
    body: list[str] = []
    i = start + 1
    while i < len(lines):
        raw = lines[i]
        if raw.strip() and (len(raw) - len(raw.lstrip(" "))) <= min_indent:
            break
        body.append(raw)
        i += 1
    while body and not body[-1].strip():
        body.pop()
    indents = [len(b) - len(b.lstrip(" ")) for b in body if b.strip()]
    cut = min(indents) if indents else 0
    return "\n".join(b[cut:] if len(b) >= cut else "" for b in body), i


def _parse_map(lines: list[str], start: int, indent: int) -> dict[str, str]:
    out: dict[str, str] = {}
    i = start + 1
    key_re = re.compile(r"^" + " " * indent + r"([A-Za-z0-9_.-]+):\s*(.*)$")
    while i < len(lines):
        raw = lines[i]
        if raw.strip() and (len(raw) - len(raw.lstrip(" "))) < indent:
            break
        m = key_re.match(raw)
        if m and not raw.strip().startswith("#"):
            out[m.group(1)] = _strip_comment(m.group(2)).strip("'\"") if m.group(2).strip() else ""
        i += 1
    return out


def _parse_step(job: str, lines: list[str], first: int, last: int) -> Step:
    step = Step(job=job, line=first + 1)
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
        if key == "run" and re.match(r"^[|>][-+]?\s*(#.*)?$", value):
            step.run, nxt = _block_scalar(seg, i, 8)
            step.run_line = first + i + 2
            i = nxt
            continue
        if key == "run":
            step.run = value.strip()
            step.run_line = first + i + 1
        elif key == "env":
            step.env = _parse_map(seg, i, 10)
        elif key == "with":
            step.with_ = _parse_map(seg, i, 10)
        elif re.match(r"^[|>][-+]?\s*$", value):
            body, nxt = _block_scalar(seg, i, 8)
            step.keys[key] = " ".join(body.split())
            i = nxt
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
        job = Job(name=name, line=begin + 1, end=end)
        body = lines[begin:end]
        steps_at = next(
            (k for k, l in enumerate(body) if re.match(r"^    steps:\s*(#.*)?$", l)), len(body)
        )
        header = body[:steps_at]
        job.header = "\n".join(header)
        k = 0
        while k < len(header):
            raw = header[k]
            m = re.match(r"^    ([A-Za-z_-]+):\s*(.*)$", raw)
            if m and not raw.strip().startswith("#"):
                key, value = m.group(1), m.group(2)
                if key == "env":
                    job.env = _parse_map(header, k, 6)
                elif key == "needs":
                    v = _strip_comment(value)
                    if v.startswith("["):
                        job.needs = [x.strip() for x in v.strip("[]").split(",") if x.strip()]
                    elif v:
                        job.needs = [v]
                    else:
                        j = k + 1
                        while j < len(header) and re.match(r"^      - ", header[j]):
                            job.needs.append(_strip_comment(header[j].split("- ", 1)[1]))
                            j += 1
                elif re.match(r"^[|>][-+]?\s*$", value):
                    blk, _ = _block_scalar(header, k, 4)
                    job.keys[key] = " ".join(blk.split())
                else:
                    job.keys[key] = _strip_comment(value)
            k += 1
        item_starts = [
            begin + k for k in range(steps_at + 1, len(body)) if re.match(r"^      - ", body[k])
        ]
        for s, first in enumerate(item_starts):
            last = item_starts[s + 1] if s + 1 < len(item_starts) else end
            job.steps.append(_parse_step(name, lines, first, last))
        jobs.append(job)
    return jobs, lines


# --- expressions -----------------------------------------------------------


def _unwrap(expr: str) -> str:
    e = expr.strip()
    m = re.match(r"^\$\{\{(.*)\}\}$", e, re.S)
    return " ".join((m.group(1) if m else e).split())


def conjuncts(expr: str) -> list[str]:
    """Top-level `&&` operands of an `if:` expression, whitespace-normalised."""
    e = _unwrap(expr)
    out, depth, cur, i = [], 0, "", 0
    while i < len(e):
        c = e[i]
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
        if depth == 0 and e.startswith("&&", i):
            out.append(cur.strip())
            cur = ""
            i += 2
            continue
        cur += c
        i += 1
    out.append(cur.strip())
    return [" ".join(x.split()) for x in out if x.strip()]


def _guard_for(job: str) -> str:
    return GUARD_IN_MODE_JOB if job == MODE_JOB else GUARD


def is_guarded(job: Job, step: Optional[Step]) -> bool:
    if GUARD in conjuncts(job.keys.get("if", "")):
        return True
    return step is not None and _guard_for(job.name) in conjuncts(step.keys.get("if", ""))


# --- mutation detection ----------------------------------------------------


def _drop_literals(text: str) -> str:
    """Remove quoted text that cannot execute anything.

    An error message that TELLS the operator to run `gh run rerun` is not a
    rerun. Single-quoted text never executes; double-quoted text is dropped
    only when it holds no command substitution (`$(` or an unescaped
    backtick), so `"$(gh release create ...)"` is still seen.
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


def logical_lines(script: str) -> list[tuple[int, str, bool]]:
    """(index, joined text, exempt) per shell logical line, comments dropped."""
    out: list[tuple[int, str, bool]] = []
    buf: list[str] = []
    exempt = False
    for idx, raw in enumerate(script.split("\n")):
        s = raw.strip()
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
        out.append((idx, " ".join(p.rstrip("\\").strip() for p in buf), exempt))
        buf, exempt = [], False
    if buf:
        out.append((len(script.split("\n")), " ".join(buf), exempt))
    return out


def _gh_api_mutates(text: str) -> bool:
    for m in re.finditer(r"\bgh\s+api\b(.*)", text):
        args = m.group(1)
        method = re.search(r"(?:-X|--method)[\s=]+['\"]?([A-Za-z]+)", args)
        if method:
            return method.group(1).upper() != "GET"
        if re.search(r"(?:^|\s)graphql\b", args):
            return "mutation" in args
        if re.search(r"(?:^|\s)(?:-f|-F|--field|--raw-field|--input)(?:\s|=)", args):
            return True
    return False


def line_mutation(text: str) -> Optional[str]:
    code = _drop_literals(text)
    for label, pat in MUTATING_PATTERNS:
        if pat.search(code):
            return label
    if re.search(r"\bcargo\s+publish\b", code) and "--dry-run" not in code:
        return "cargo publish"
    # The command must be code, but its arguments are read RAW: a GraphQL
    # `mutation` and a quoted `--method 'POST'` live inside quotes.
    if re.search(r"\bgh\s+api\b", code) and _gh_api_mutates(text):
        return "gh api write"
    return None


def script_mutations(
    script: str, repo_root: Path, seen: Optional[set[str]] = None
) -> list[tuple[int, str]]:
    """(logical-line index, reason) for each mutating line, following scripts/."""
    seen = set() if seen is None else seen
    found: list[tuple[int, str]] = []
    for idx, text, exempt in logical_lines(script):
        if exempt:
            continue
        reason = line_mutation(text)
        if reason:
            found.append((idx, f"{reason}: {text[:100]}"))
            continue
        for ref in SCRIPT_REF_RE.findall(text):
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
    if uses and not uses.startswith(READ_ONLY_ACTIONS):
        return [(-1, f"uses: {uses} (not on the read-only action allowlist)")]
    if step.run is None:
        return []
    return script_mutations(step.run, repo_root)


# --- the rules -------------------------------------------------------------


def declares_dry_run(text: str) -> bool:
    return bool(re.search(r"^  workflow_dispatch:", text, re.M)) and bool(
        re.search(r"^      dry_run:\s*(#.*)?$", text, re.M)
    )


def check_source(text: str, name: str, repo_root: Path) -> list[str]:
    if not declares_dry_run(text):
        return []
    jobs, lines = parse_workflow(text)
    errors: list[str] = []
    by_name = {j.name: j for j in jobs}

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
            if MODE_SCRIPT not in (mode_step.run or ""):
                err(mode_step.line, f"the mode step must run `{MODE_SCRIPT}` (it is the tested resolver).")
            if not re.fullmatch(
                r"\$\{\{\s*inputs\.dry_run\s*\}\}", mode_step.env.get("DRY_RUN_INPUT", "")
            ):
                err(mode_step.line, "the mode step must set `DRY_RUN_INPUT: ${{ inputs.dry_run }}`.")
            if mode_job.steps.index(mode_step) > 1:
                err(mode_step.line, "the mode step must be one of the first two steps of the job.")
        if not re.search(
            r"^      live:\s*\$\{\{\s*steps\.mode\.outputs\.live\s*\}\}\s*$", mode_job.header, re.M
        ):
            err(mode_job.line, f"`{MODE_JOB}` must export `live: ${{{{ steps.mode.outputs.live }}}}`.")

    # Rule 2: nothing else reads the input.
    mode_range = range(0)
    if mode_step is not None:
        nxt = [s.line for s in mode_job.steps if s.line > mode_step.line]  # type: ignore[union-attr]
        mode_range = range(mode_step.line, (nxt[0] if nxt else mode_job.end + 1))  # type: ignore[union-attr]
    for n, raw in enumerate(lines, start=1):
        if raw.strip().startswith("#") or n in mode_range:
            continue
        if DRY_RUN_INPUT_RE.search(raw):
            err(
                n,
                "`inputs.dry_run` is read outside the mode step. Gate on "
                f"`{GUARD}` instead; a second reader is how #5936's guards drifted apart.",
            )

    for job in jobs:
        # Rule 6: the guard expression needs `validate` in scope.
        if job.name != MODE_JOB and "needs.validate." in "\n".join(
            lines[job.line - 1 : job.end]
        ) and MODE_JOB not in job.needs:
            err(job.line, f"job `{job.name}` reads `needs.validate.*` but does not list `validate` in `needs:`, so the value is always empty.")

        job_guarded = is_guarded(job, None)
        if "uses" in job.keys and not job_guarded:
            err(job.line, f"job `{job.name}` calls a reusable workflow (`uses: {job.keys['uses']}`) and is not gated on `{GUARD}`.")

        for step in job.steps:
            muts = step_mutations(step, repo_root)
            if muts:
                where = f"job `{job.name}`, step `{step.label}`"
                if not is_guarded(job, step):
                    err(step.line, f"{where} mutates ({muts[0][1]}) but is not gated on `{_guard_for(job.name)}` (in its own or its job's `if:`, as a top-level `&&` term).")
                if step.run is not None:
                    live_env = {**job.env, **step.env}.get("RELEASE_LIVE")
                    if live_env is None or not LIVE_ENV_RE.match(f"RELEASE_LIVE: {live_env}"):
                        err(step.line, f"{where} mutates but `RELEASE_LIVE` is not wired from `outputs.live` (step or job `env:`).")
                    first_mut = min(i for i, _ in muts)
                    asserted = [
                        i for i, t, _ in logical_lines(step.run) if ASSERT_RE.match(t) and i < first_mut
                    ]
                    if not asserted:
                        err(step.line, f"{where} mutates but does not refuse in its own script first. Put this line before the first mutating command:\n      [ \"${{RELEASE_LIVE:-}}\" = \"true\" ] || {{ echo \"::error::...\"; exit 1; }}")

            if job_guarded:
                continue
            # Rule 5: a job a dry run reaches holds no persisted credential and no PAT.
            uses = step.keys.get("uses", "")
            if uses.startswith("actions/checkout"):
                persist = step.with_.get("persist-credentials", "")
                if persist != "false" and _unwrap(persist) != _guard_for(job.name):
                    err(step.line, f"job `{job.name}` runs in a dry run, so its checkout must set `persist-credentials: false` (or `${{{{ {GUARD} }}}}`); otherwise a stray `git push` can authenticate.")
            pat = [k for k, v in {**step.env, **step.with_}.items() if PAT_VALUE_RE.search(v) and k != "token"]
            if pat and not is_guarded(job, step):
                err(step.line, f"job `{job.name}`, step `{step.label}` hands RELEASE_PAT to a step a dry run executes ({', '.join(pat)}). Gate it on `{_guard_for(job.name)}`.")
    return errors


def check_dir(root: Path, repo_root: Path) -> list[str]:
    errors: list[str] = []
    for path in sorted(list(root.glob("*.yml")) + list(root.glob("*.yaml"))):
        errors.extend(check_source(path.read_text(), str(path), repo_root))
    return errors


def mutating_runs(text: str, repo_root: Path) -> list[tuple[str, str]]:
    jobs, _ = parse_workflow(text)
    out = []
    for job in jobs:
        for step in job.steps:
            if step.run is not None and step_mutations(step, repo_root):
                script = re.sub(r"\$\{\{.*?\}\}", "PLACEHOLDER", step.run)
                out.append((f"{job.name} / {step.label}", script))
    return out


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
    steps:
      - uses: actions/checkout@v7
        with:
          token: ${{ secrets.RELEASE_PAT || secrets.GITHUB_TOKEN }}
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
        if: ${{ always() && needs.validate.outputs.live == 'true' }}
        env:
          GH_TOKEN: ${{ secrets.RELEASE_PAT || secrets.GITHUB_TOKEN }}
          RELEASE_LIVE: ${{ needs.validate.outputs.live }}
        run: |
          [ "${RELEASE_LIVE:-}" = "true" ] || { echo "::error::refusing in a dry run"; exit 1; }
          gh pr create \\
            --title x --body y
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
          git tag v1 && git push origin v1
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
        (root / "scripts/resolve_release_mode.sh").write_text("echo live=true\n")
        (root / "scripts/sneaky.sh").write_text("#!/bin/sh\n# git push is fine in a comment\ngit push origin x\n")
        (root / "scripts/reader.sh").write_text("gh api graphql -f query='query{viewer{login}}'\n")

        def run(case: str, text: str, needle: Optional[str]) -> None:
            f = _expect(check_source(text, "wf.yml", root), needle, case)
            if f:
                failures.append(f)

        run("good workflow passes", _GOOD, None)
        run("a workflow without a dry_run input is out of scope",
            _GOOD.replace("      dry_run:\n", "      other:\n").replace(
                "        if: needs.validate.outputs.live == 'true'\n", ""), None)

        # The #5936 shape: an unguarded push.
        run("unguarded push is flagged",
            _GOOD.replace("        if: needs.validate.outputs.live == 'true'\n", ""), "not gated")
        run("a guard under || is not a guard",
            _GOOD.replace("if: needs.validate.outputs.live == 'true'",
                          "if: needs.validate.outputs.live == 'true' || true"), "not gated")
        run("a negated guard is not a guard",
            _GOOD.replace("if: needs.validate.outputs.live == 'true'",
                          "if: needs.validate.outputs.live != 'true'"), "not gated")
        run("per-step inputs.dry_run is refused",
            _GOOD.replace("if: needs.validate.outputs.live == 'true'",
                          "if: ${{ !inputs.dry_run }}"), "read outside the mode step")
        run("github.event.inputs.dry_run is refused too",
            _GOOD.replace("run: git commit -am bump",
                          "run: echo ${{ github.event.inputs.dry_run }}"), "read outside the mode step")
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
        run("job-level guard covers its steps", _GOOD, None)
        run("job-level guard removed exposes the tag push",
            _GOOD.replace("    if: always() && needs.validate.outputs.live == 'true'\n", ""), "not gated")
        run("a mutation hidden in a called script is found",
            _GOOD.replace("run: git commit -am bump", "run: bash scripts/sneaky.sh"), "via scripts/sneaky.sh")
        run("a read-only GraphQL query is not a mutation",
            _GOOD.replace("run: git commit -am bump", "run: bash scripts/reader.sh"), None)
        run("a GraphQL mutation is",
            _GOOD.replace("run: git commit -am bump",
                          "run: gh api graphql -f query='mutation{x}'"), "gh api write")
        run("gh api POST via fields is a mutation without the marker",
            _GOOD.replace("          # dry-run-safe: renders text only\n", "").replace(
                "    if: always() && needs.validate.outputs.live == 'true'\n", "    if: always()\n"),
            "gh api write")
        run("an error message that names a command is not a mutation",
            _GOOD.replace("run: git commit -am bump",
                          "run: echo \"re-run it with gh run rerun --failed, or git push by hand\""), None)
        run("a command substitution inside double quotes still is",
            _GOOD.replace("run: git commit -am bump", "run: X=\"$(gh release create v1)\""),
            "gh release write")
        run("an unknown action is a mutation",
            _GOOD.replace("run: git commit -am bump", "uses: softprops/action-gh-release@v2"),
            "read-only action allowlist")
        run("cargo publish --dry-run is not a mutation",
            _GOOD.replace("run: git commit -am bump", "run: cargo publish -p freenet --dry-run"), None)
        run("cargo publish is",
            _GOOD.replace("run: git commit -am bump", "run: cargo publish -p freenet"), "cargo publish")
        run("git -C dir push is still a push",
            _GOOD.replace("run: git commit -am bump", "run: git -C repo push origin x"), "git push")
        run("a persisted credential in a dry-run job is flagged",
            _GOOD.replace("          persist-credentials: ${{ needs.validate.outputs.live == 'true' }}\n", ""),
            "persist-credentials")
        run("RELEASE_PAT on an unguarded step is flagged",
            _GOOD.replace("      - name: Commit locally\n        run:",
                          "      - name: Commit locally\n        env:\n          GH_TOKEN: ${{ secrets.RELEASE_PAT }}\n        run:"),
            "hands RELEASE_PAT")
        run("guard needs validate in scope",
            _GOOD.replace("    needs: [validate, bump]\n", "    needs: [bump]\n"), "does not list `validate`")
        run("the mode step must exist",
            _GOOD.replace("        id: mode\n", ""), "no step with `id: mode`")
        run("the mode step must use the tested resolver",
            _GOOD.replace("run: bash scripts/resolve_release_mode.sh", "run: echo live=true >> $GITHUB_OUTPUT"),
            "resolve_release_mode.sh")
        run("the mode output must be exported",
            _GOOD.replace("      live: ${{ steps.mode.outputs.live }}\n", ""), "must export")

        # Parser sanity: the extractor finds exactly the mutating run steps.
        labels = [l for l, _ in mutating_runs(_GOOD, root)]
        if labels != ["bump / Push", "bump / Open PR", "tag / Tag"]:
            failures.append(f"extractor: expected the three mutating steps, got {labels}")
        if conjuncts("${{ always() && (a || b) && needs.validate.outputs.live == 'true' }}") != [
            "always()", "(a || b)", GUARD
        ]:
            failures.append("conjuncts: top-level && split is wrong")

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
        for n, (label, script) in enumerate(mutating_runs(Path(argv[2]).read_text(), repo_root)):
            (out / f"{n:02d}.sh").write_text(script + "\n")
            (out / f"{n:02d}.name").write_text(label + "\n")
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

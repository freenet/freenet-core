#!/usr/bin/env python3
"""Keep ci-cache-warm.yml's jobs producing the cache keys ci.yml restores.

ci.yml's heavy PR jobs (Unit & Integration, Simulation, NAT Validation) never
run on pushes to main, so nothing they do can write a main-scoped cache entry,
and a PR can only restore from its own branch or from main. Measured
2026-11-03: no ``v0-rust-test_unit-*`` entry existed, so every Unit &
Integration run compiled its dependency tree from scratch on a standard-16
Ubicloud runner. ci-cache-warm.yml writes those entries.

It only helps while its keys are the keys the PR jobs look up. Swatinem/
rust-cache restores by ``<prefix>-<key>-<GITHUB_JOB>-<OS>-<arch>-<hash>``,
where ``<hash>`` covers the rustc version and every env var whose name starts
with CARGO, CC, CFLAGS, CXX, CMAKE or RUST. And a restored entry only saves
work if cargo's fingerprints match, which needs the same flags, env and
feature sets on every compile command. Any drift fails SILENTLY: the PR jobs
stay green and quietly build cold again. Hence this linter, run from Rule Lint.

A "warm job" is a job in the warm workflow that uses Swatinem/rust-cache.
Against the ci.yml job with the SAME id it checks:

1. workflow-level ``env:``/``defaults:`` and job-level ``env:``/``defaults:``
   are identical, and so are ``$GITHUB_ENV`` writes and the ORDER of actions
   up to the cache step (the toolchain action sets CARGO_* env the key hashes);
2. the ``dtolnay/rust-toolchain`` inputs are identical (and present);
3. the ``Swatinem/rust-cache`` reference, inputs and step env are identical,
   except ``save-if``, which is checked separately: the ci.yml twin must save
   only from main (exactly ``SAVE_IF_MAIN``), and the warm job must save;
4. the compile signatures are identical: each cargo build/test/nextest/run/
   check/clippy/bench/doc command reduced to the flags that decide what is
   compiled, plus every ``make`` invocation, EACH paired with the compile-
   affecting env of its own step (step ``env:``, ``export``s, and inline
   ``VAR=value cargo ...`` prefixes). ``--no-run``, ``--locked``, ``-E``
   filters, ``-j`` and nextest's test ``--profile`` are ignored, so a warm job
   that compiles without running matches a CI job that compiles and runs;
5. ``apt-get install`` package sets are identical;
6. the warm job runs on a Ubicloud runner with the same label suffix
   (architecture, OS variant), and its display name starts with
   ``Warm cache:`` -- scripts/release.sh excludes check runs by that prefix,
   and a name that matched a required context would impersonate it.

It also DISCOVERS which ci.yml jobs need a twin: a Ubicloud job whose
rust-cache has a conditional ``save-if`` and whose ``if:`` gates on
``github.event_name`` without ever admitting ``== 'push'``. It fails if such a
job has no twin, or if a warm job's twin no longer needs one.

Standard library only, like the other Rule Lint scripts: the subset of YAML
these two files use is parsed by indentation.

Usage:
    check_cache_warm_parity.py <ci.yml> <ci-cache-warm.yml>
    check_cache_warm_parity.py --self-test
"""

from __future__ import annotations

import re
import shlex
import sys
from pathlib import Path

SAVE_IF_MAIN = "${{ github.ref == 'refs/heads/main' }}"
WARM_NAME_PREFIX = "Warm cache:"

# Step-level env that changes what cargo/rustc/cc/build scripts produce, so a
# cached artifact built without it is not reused. CARGO_TERM_* only colours
# output.
COMPILE_ENV_RE = re.compile(
    r"^(CARGO_(?!TERM_)|RUSTFLAGS$|RUSTDOCFLAGS$|RUSTC|CC$|CC_|CFLAGS|CXX|"
    r"CMAKE|PKG_CONFIG|LIBCLANG|BINDGEN|OPENSSL_|SDKROOT$|MACOSX_DEPLOYMENT_TARGET$)"
)
EXPORT_RE = re.compile(r"^\s*export\s+([A-Za-z_][A-Za-z0-9_]*)=(.*)$")
INLINE_ENV_RE = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)=(\"[^\"]*\"|'[^']*'|\S+)\s+")
CARGO_CMD_RE = re.compile(
    r"\bcargo\s+(build|nextest\s+run|test|run|check|clippy|bench|doc)\b(.*)"
)
MAKE_CMD_RE = re.compile(r"(?:^|[;&|]\s*|\s)(make\s.*)$")
APT_RE = re.compile(r"\bapt(?:-get)?\s+install\b(.*)")
UBICLOUD_LABEL_RE = re.compile(r"ubicloud-standard-\d+((?:-[a-z0-9]+)*)")

# Flags that take a value and decide what is compiled.
VALUE_FLAGS = {"-p", "--package", "--features", "-F", "--exclude", "--test",
               "--bin", "--example", "--bench", "--target", "--profile",
               "--cargo-profile", "--target-dir", "--config", "--manifest-path"}
# nextest's --profile picks a TEST profile (retries, timeouts), not a cargo
# profile, so it does not affect what is compiled.
NEXTEST_IGNORED_VALUE_FLAGS = {"--profile"}
BOOL_FLAGS = {"--no-default-features", "--all-features", "--release", "-r",
              "--workspace", "--all", "--lib", "--bins", "--tests", "--benches",
              "--examples", "--all-targets"}


# --- minimal indentation-based YAML access -----------------------------------

def _indent(line: str) -> int:
    return len(line) - len(line.lstrip(" "))


def _is_content(line: str) -> bool:
    s = line.strip()
    return bool(s) and not s.startswith("#")


def _norm(s: str) -> str:
    return " ".join(s.split())


def _block_after(lines: list[str], start: int) -> list[str]:
    """Lines nested under ``lines[start]`` (deeper indent), comments kept."""
    base = _indent(lines[start])
    out = []
    for line in lines[start + 1:]:
        if _is_content(line) and _indent(line) <= base:
            break
        out.append(line)
    return out


def _unquote(v: str) -> str:
    v = v.strip()
    if len(v) >= 2 and v[0] == v[-1] and v[0] in "\"'":
        return v[1:-1]
    return v


def _mapping(block: list[str]) -> dict[str, str]:
    """Flat ``key: value`` pairs at the shallowest indent of ``block``."""
    content = [l for l in block if _is_content(l)]
    if not content:
        return {}
    depth = min(_indent(l) for l in content)
    out = {}
    for line in content:
        if _indent(line) != depth or ":" not in line:
            continue
        key, _, value = line.strip().partition(":")
        out[key.strip()] = _unquote(value)
    return out


def _key_block(lines: list[str], key: str, indent: int) -> list[str] | None:
    for i, line in enumerate(lines):
        if re.match(rf"^{' ' * indent}{re.escape(key)}:\s*(#.*)?$", line):
            return _block_after(lines, i)
    return None


def _key_value(lines: list[str], key: str, indent: int) -> str | None:
    """A scalar ``key: value``, or a ``key: |`` block joined into one line."""
    for i, line in enumerate(lines):
        m = re.match(rf"^{' ' * indent}{re.escape(key)}:\s*(.*)$", line)
        if m:
            value = m.group(1).strip()
            if value in ("|", ">", "|-", ">-", ""):
                value = " ".join(l.strip() for l in _block_after(lines, i))
            return _unquote(value)
    return None


def _defaults(lines: list[str], indent: int) -> list[str]:
    block = _key_block(lines, "defaults", indent)
    return [l.strip() for l in block or [] if _is_content(l)]


def extract_job(text: str, job_id: str) -> list[str] | None:
    lines = text.splitlines()
    in_jobs = False
    for i, line in enumerate(lines):
        if re.match(r"^jobs:\s*(#.*)?$", line):
            in_jobs = True
            continue
        if in_jobs and re.match(rf"^  {re.escape(job_id)}:\s*(#.*)?$", line):
            return _block_after(lines, i)
    return None


def job_ids(text: str) -> list[str]:
    ids, in_jobs = [], False
    for line in text.splitlines():
        if re.match(r"^jobs:\s*(#.*)?$", line):
            in_jobs = True
            continue
        if in_jobs:
            m = re.match(r"^  ([A-Za-z0-9_-]+):\s*(#.*)?$", line)
            if m:
                ids.append(m.group(1))
            elif _is_content(line) and _indent(line) == 0:
                in_jobs = False
    return ids


def _steps(job: list[str]) -> list[list[str]]:
    steps, current, step_indent = [], None, None
    for line in job:
        m = re.match(r"^(\s*)- ", line)
        if m and (step_indent is None or len(m.group(1)) == step_indent):
            if step_indent is None:
                step_indent = len(m.group(1))
            if current is not None:
                steps.append(current)
            # Re-indent the first key so it lines up with the step's other keys.
            current = [" " * (step_indent + 2) + line[step_indent + 2:]]
        elif current is not None:
            if _is_content(line) and _indent(line) <= (step_indent or 0):
                steps.append(current)
                current = None
            else:
                current.append(line)
    if current is not None:
        steps.append(current)
    return steps


def _step_key(step: list[str], key: str) -> tuple[str | None, list[str]]:
    for i, line in enumerate(step):
        m = re.match(rf"^\s*{re.escape(key)}:\s*(.*)$", line)
        if m and _indent(line) == _indent(step[0]):
            return m.group(1).strip(), _block_after(step, i)
    return None, []


def _logical_lines(lines: list[str]) -> list[str]:
    """Shell lines with ``\\`` continuations joined and comments dropped."""
    out, buf = [], ""
    for raw in lines:
        s = raw.strip()
        if s.startswith("#"):
            continue
        if s.endswith("\\"):
            buf += s[:-1] + " "
            continue
        out.append(buf + s)
        buf = ""
    if buf:
        out.append(buf)
    return out


# --- facts about a job -------------------------------------------------------

def _job_env(job: list[str]) -> dict[str, str]:
    return _mapping(_key_block(job, "env", 4) or [])


def _runs_on(job: list[str]) -> str:
    return _key_value(job, "runs-on", 4) or ""


def _job_if(job: list[str]) -> str | None:
    return _key_value(job, "if", 4)


def _job_name(job: list[str]) -> str:
    return _key_value(job, "name", 4) or ""


def _uses_inputs(job: list[str], action: str) -> list[tuple[str, dict, dict]]:
    """(uses, with-inputs, step env) for each step using ``action``."""
    out = []
    for step in _steps(job):
        uses, _ = _step_key(step, "uses")
        if uses and uses.split("@")[0] == action:
            _, with_block = _step_key(step, "with")
            _, env_block = _step_key(step, "env")
            out.append((uses, _mapping(with_block), _mapping(env_block)))
    return out


def _cache_steps(job: list[str]) -> list[tuple[str, dict, dict]]:
    return _uses_inputs(job, "Swatinem/rust-cache")


def _restore_only_on_prs(job: list[str]) -> bool:
    steps = _cache_steps(job)
    return bool(steps) and all(_norm(w.get("save-if", "")) == SAVE_IF_MAIN
                               for _, w, _ in steps)


def needs_warm_twin(job: list[str]) -> bool:
    """A Ubicloud job that restores a cache it can never write itself.

    Its rust-cache saves conditionally, and its ``if:`` gates on the event
    without admitting a push, so it never runs where it could save a
    main-scoped entry. Clippy (no ``if:``) runs on main pushes and saves its
    own; a GitHub-hosted job uses a different cache backend, which a Ubicloud
    warm job could not fill.
    """
    if "ubicloud" not in _runs_on(job):
        return False
    steps = _cache_steps(job)
    if not steps or not any(w.get("save-if", "").strip() not in ("", "true")
                            for _, w, _ in steps):
        return False
    cond = _job_if(job) or ""
    # `== 'push'` admits a push; `!= 'push'` (a guard) does not.
    return ("event_name" in cond
            and not re.search(r"event_name\s*==\s*['\"]push['\"]", cond))


def _step_compile_env(step: list[str]) -> dict[str, str]:
    _, env_block = _step_key(step, "env")
    env = {k: v for k, v in _mapping(env_block).items() if COMPILE_ENV_RE.match(k)}
    for line in step:
        m = EXPORT_RE.match(line)
        if m and COMPILE_ENV_RE.match(m.group(1)):
            env[m.group(1)] = _unquote(m.group(2))
    return env


def _cargo_signature(sub: str, rest: str) -> tuple:
    sub = " ".join(sub.split())
    try:
        tokens = shlex.split(rest)
    except ValueError:
        tokens = rest.split()
    sig, i = [], 0
    while i < len(tokens):
        tok = tokens[i]
        if tok == "--":
            break
        name, eq, inline = tok.partition("=")
        if name in VALUE_FLAGS:
            value = inline if eq else (tokens[i + 1] if i + 1 < len(tokens) else "")
            i += 1 if eq else 2
            if sub == "nextest run" and name in NEXTEST_IGNORED_VALUE_FLAGS:
                continue
            if name in ("--features", "-F"):
                value = ",".join(sorted(f for f in re.split(r"[,\s]+", value) if f))
                name = "--features"
            if name == "--package":
                name = "-p"
            sig.append((name, value))
            continue
        if tok in BOOL_FLAGS:
            sig.append(("--release" if tok == "-r" else tok, ""))
        i += 1
    return (sub, tuple(sorted(sig)))


def compile_signatures(job: list[str]) -> set[tuple]:
    """Each compile command, paired with the compile env it actually sees.

    Pairing per command, not per job, matters: dropping the linker flags from
    the one step that compiles the tests must fail even though another step in
    the same job still sets them. (That is how this linter found Simulation's
    run step recompiling everything the build step had just built.)
    """
    sigs = set()
    for step in _steps(job):
        step_env = _step_compile_env(step)
        for line in _logical_lines(step):
            m = CARGO_CMD_RE.search(line)
            if m:
                env = dict(step_env)
                for k, v in INLINE_ENV_RE.findall(line[:m.start()]):
                    if COMPILE_ENV_RE.match(k):
                        env[k] = _unquote(v)
                sigs.add((_cargo_signature(m.group(1), m.group(2)),
                          frozenset(env.items())))
                continue
            m = MAKE_CMD_RE.search(line)
            if m:
                sigs.add((("make", _norm(m.group(1))), frozenset(step_env.items())))
    return sigs


def _apt_packages(job: list[str]) -> set[str]:
    pkgs = set()
    for line in _logical_lines(job):
        m = APT_RE.search(line)
        if m:
            pkgs |= {t for t in m.group(1).split() if not t.startswith("-")}
    return pkgs


def _github_env_writes(job: list[str]) -> set[str]:
    """Script lines that touch $GITHUB_ENV (``>>``, ``tee -a``, ...); env set
    there reaches every later step, including the cache step's key."""
    return {_norm(l) for l in _logical_lines(job) if "GITHUB_ENV" in l}


def _actions_before_cache(job: list[str]) -> list[str]:
    """Actions (``uses:``) in order, up to and including the first rust-cache.

    Order matters, not just presence: dtolnay/rust-toolchain writes CARGO_*
    variables to $GITHUB_ENV, and rust-cache hashes CARGO_* env into the key
    when it runs, so swapping the two steps changes the key.
    """
    out = []
    for step in _steps(job):
        uses, _ = _step_key(step, "uses")
        if uses:
            out.append(uses)
            if uses.split("@")[0] == "Swatinem/rust-cache":
                break
    return out


def _label_suffixes(runs_on: str) -> set[str]:
    return set(UBICLOUD_LABEL_RE.findall(runs_on))


# --- the check ---------------------------------------------------------------

def _fmt(value) -> str:
    if isinstance(value, (set, frozenset)):
        return "\n".join(f"        {v}" for v in sorted(value, key=str)) or "        (none)"
    return f"        {value}"


def check(ci_text: str, warm_text: str, ci_name: str = "ci.yml",
          warm_name: str = "ci-cache-warm.yml") -> list[str]:
    errors = []

    def differ(what: str, job: str, ci_val, warm_val, why: str):
        errors.append(
            f"{warm_name}: job `{job}`: {what} differs from {ci_name}.\n"
            f"    {ci_name}:\n{_fmt(ci_val)}\n"
            f"    {warm_name}:\n{_fmt(warm_val)}\n"
            f"    {why}"
        )

    key_why = ("This is a rust-cache key input, so the warm job saves under a "
               "key the PR job never looks up and PR runs silently go back to "
               "building cold. Change both files together.")
    fp_why = ("Cargo's fingerprints depend on this, so the PR job restores the "
              "entry and then recompiles anyway. Change both files together.")

    ci_lines, warm_lines = ci_text.splitlines(), warm_text.splitlines()
    ci_wf_env = _mapping(_key_block(ci_lines, "env", 0) or [])
    warm_wf_env = _mapping(_key_block(warm_lines, "env", 0) or [])
    if ci_wf_env != warm_wf_env:
        differ("workflow-level env", "*", ci_wf_env, warm_wf_env, key_why)
    if _defaults(ci_lines, 0) != _defaults(warm_lines, 0):
        differ("workflow-level defaults", "*", _defaults(ci_lines, 0),
               _defaults(warm_lines, 0), fp_why)

    all_warm = job_ids(warm_text)
    for job_id in all_warm:
        name = _job_name(extract_job(warm_text, job_id) or [])
        if not name.startswith(WARM_NAME_PREFIX):
            errors.append(
                f"{warm_name}: job `{job_id}` is named `{name}`. Every job here "
                f"must be named `{WARM_NAME_PREFIX} ...`: scripts/release.sh "
                f"ignores main-branch check runs by that prefix, and a name "
                f"equal to a required context would impersonate it."
            )
    ids = [j for j in all_warm if _cache_steps(extract_job(warm_text, j) or [])]
    if not ids:
        errors.append(f"{warm_name}: no job uses Swatinem/rust-cache; the parity "
                      f"check would be vacuous.")

    # Discovery, not a hardcoded list: a list catches a twin being removed but
    # never a NEW job that needs one.
    needed = [j for j in job_ids(ci_text)
              if needs_warm_twin(extract_job(ci_text, j) or [])]
    if not needed:
        errors.append(
            f"{ci_name}: no job looks like it needs a warm twin (Ubicloud, "
            f"conditional rust-cache save-if, `if:` on event_name without "
            f"'push'). Either the discovery rule no longer matches {ci_name}, "
            f"or {warm_name} is dead weight; find out which before silencing this."
        )
    for job_id in needed:
        if job_id not in ids:
            errors.append(
                f"{ci_name}: job `{job_id}` restores a Rust cache it can never "
                f"write (it never runs on a push to main), and {warm_name} has "
                f"no warm job `{job_id}` to write it, so every run builds from "
                f"scratch. Add a warm twin with the same job id."
            )

    for job_id in ids:
        warm_job = extract_job(warm_text, job_id) or []
        ci_job = extract_job(ci_text, job_id)
        if ci_job is None:
            errors.append(
                f"{warm_name}: job `{job_id}` has no job with the same id in "
                f"{ci_name}. rust-cache keys on the job id, so this warm job "
                f"serves nobody. Rename it to match, or delete it if the CI job "
                f"is gone."
            )
            continue
        if job_id not in needed:
            errors.append(
                f"{warm_name}: job `{job_id}` warms a cache its {ci_name} twin "
                f"does not need from it: the twin runs on pushes to main (so it "
                f"saves its own entry), saves unconditionally, or is not on "
                f"Ubicloud. Delete the warm job, or restore the twin's shape."
            )

        ci_env, warm_env = _job_env(ci_job), _job_env(warm_job)
        if ci_env != warm_env:
            differ("job-level env", job_id, ci_env, warm_env, key_why)
        if _defaults(ci_job, 4) != _defaults(warm_job, 4):
            differ("job-level defaults", job_id, _defaults(ci_job, 4),
                   _defaults(warm_job, 4), fp_why)
        ci_ge, warm_ge = _github_env_writes(ci_job), _github_env_writes(warm_job)
        if ci_ge != warm_ge:
            differ("writes to $GITHUB_ENV", job_id, ci_ge, warm_ge, key_why)

        ci_ord, warm_ord = _actions_before_cache(ci_job), _actions_before_cache(warm_job)
        if ci_ord != warm_ord:
            differ("actions run before (and including) the rust-cache step, in "
                   "order", job_id, ci_ord, warm_ord, key_why)

        ci_tc = _uses_inputs(ci_job, "dtolnay/rust-toolchain")
        warm_tc = _uses_inputs(warm_job, "dtolnay/rust-toolchain")
        if not ci_tc:
            errors.append(
                f"{ci_name}: job `{job_id}` has no dtolnay/rust-toolchain step, "
                f"so the toolchain comparison would be vacuous. Teach this "
                f"linter where the toolchain now comes from."
            )
        elif ci_tc != warm_tc:
            differ("dtolnay/rust-toolchain inputs", job_id, ci_tc, warm_tc, key_why)

        def cache_inputs(job):
            return [(u, {k: v for k, v in w.items() if k != "save-if"}, e)
                    for u, w, e in _cache_steps(job)]
        ci_rc, warm_rc = cache_inputs(ci_job), cache_inputs(warm_job)
        if not ci_rc:
            errors.append(
                f"{ci_name}: job `{job_id}` no longer uses Swatinem/rust-cache, "
                f"so its warm twin in {warm_name} is dead weight. Delete it."
            )
        elif ci_rc != warm_rc:
            differ("Swatinem/rust-cache reference, inputs or step env "
                   "(save-if excluded)", job_id, ci_rc, warm_rc, key_why)

        if ci_rc and not _restore_only_on_prs(ci_job):
            errors.append(
                f"{ci_name}: job `{job_id}`: every rust-cache step must have "
                f"exactly `save-if: {SAVE_IF_MAIN}`. Anything looser lets PR "
                f"runs upload a 1-2 GB entry only that branch can read, which is "
                f"what {warm_name} replaced."
            )
        for _, w, _ in _cache_steps(warm_job):
            save_if = _norm(w.get("save-if", ""))
            if save_if not in ("", "true", SAVE_IF_MAIN):
                errors.append(
                    f"{warm_name}: job `{job_id}`: `save-if: {save_if}` can stop "
                    f"the warm job saving, which makes it pointless. Use "
                    f"`{SAVE_IF_MAIN}`."
                )

        warm_ro = _runs_on(warm_job)
        if "ubicloud" not in warm_ro:
            errors.append(
                f"{warm_name}: job `{job_id}` runs on `{warm_ro}`. Ubicloud "
                f"serves its own cache backend, so an entry saved from any other "
                f"runner is invisible to the Ubicloud PR job. Use a "
                f"`ubicloud-standard-*` label."
            )
        else:
            ci_sfx, warm_sfx = _label_suffixes(_runs_on(ci_job)), _label_suffixes(warm_ro)
            if ci_sfx != warm_sfx:
                differ("runner label suffix (architecture / OS variant)", job_id,
                       ci_sfx, warm_sfx, key_why)

        if _apt_packages(ci_job) != _apt_packages(warm_job):
            differ("apt-get install packages", job_id, _apt_packages(ci_job),
                   _apt_packages(warm_job), fp_why)

        ci_sig, warm_sig = compile_signatures(ci_job), compile_signatures(warm_job)
        if not ci_sig:
            errors.append(
                f"{ci_name}: job `{job_id}`: no cargo/make compile commands "
                f"found, so the signature comparison would be vacuous. If the "
                f"job's commands moved into a script, teach this linter to "
                f"follow them."
            )
        elif ci_sig != warm_sig:
            differ("compile commands (each with the compile env it sees)",
                   job_id, ci_sig, warm_sig, fp_why)
    return errors


# --- self-test ---------------------------------------------------------------

_CI = """\
name: CI
on:
  pull_request:
jobs:
  fmt:
    runs-on: ubuntu-latest
    steps:
      - run: cargo fmt -- --check
  clippy_check:
    runs-on: ubicloud-standard-8
    steps:
      - uses: Swatinem/rust-cache@v2
      - run: cargo clippy --locked
  test_unit:
    name: Unit & Integration
    runs-on: ubicloud-standard-16
    if: |
      github.event_name == 'pull_request'
    env:
      RUST_LOG: error
      CARGO_TARGET_DIR: ${{ github.workspace }}/target
    steps:
      - uses: actions/checkout@v7
      - name: Install mold linker
        run: sudo apt-get install -y mold
      - uses: dtolnay/rust-toolchain@stable
        with:
          toolchain: 1.93.0
          targets: wasm32-unknown-unknown
      - uses: Swatinem/rust-cache@v2
        # a comment inside the step
        with:
          save-if: ${{ github.ref == 'refs/heads/main' }}
          key: ci-${{ hashFiles('.github/workflows/ci-cache-warm.yml') }}
      - name: Build
        env:
          CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS: -C link-arg=-fuse-ld=mold
        run: |
          # cargo build --release  (a comment, must be ignored)
          cargo build --locked
          make -C apps/ping -f run.mk build
      - name: Test
        env:
          CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS: -C link-arg=-fuse-ld=mold
          FREENET_BACKOFF_BASE_SECS: "5"
        run: |
          cargo nextest run --workspace --exclude ping-types \\
            --no-default-features \\
            --features trace,websocket \\
            --profile ci -E 'not test(blocked)'
"""

_WARM = """\
name: Warm
on:
  schedule:
    - cron: "0 4 * * *"
jobs:
  test_unit:
    name: "Warm cache: Unit & Integration"
    runs-on: ubicloud-standard-8
    env:
      RUST_LOG: error
      CARGO_TARGET_DIR: ${{ github.workspace }}/target
    steps:
      - uses: actions/checkout@v7
      - name: Install mold linker
        run: |
          sudo apt-get update
          sudo apt-get install -y mold
      - uses: dtolnay/rust-toolchain@stable
        with:
          toolchain: 1.93.0
          targets: wasm32-unknown-unknown
      - uses: Swatinem/rust-cache@v2
        with:
          save-if: ${{ github.ref == 'refs/heads/main' }}
          key: ci-${{ hashFiles('.github/workflows/ci-cache-warm.yml') }}
      - name: Build
        env:
          CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS: -C link-arg=-fuse-ld=mold
        run: |
          cargo build --locked
          make -C apps/ping -f run.mk build
      - name: Compile tests
        env:
          CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS: -C link-arg=-fuse-ld=mold
        run: |
          cargo nextest run --workspace --exclude ping-types \\
            --no-default-features \\
            --features websocket,trace \\
            --profile ci --no-run
  notify_failure:
    name: "Warm cache: notify on failure"
    needs: [test_unit]
    runs-on: ubuntu-latest
    steps:
      - run: echo notify
"""

_TEST_UNIT_CI = _CI[_CI.index("  test_unit:\n"):]


def _sub(text: str, old: str, new: str) -> str:
    assert old in text, f"self-test fixture is missing {old!r}"
    return text.replace(old, new, 1)


def _cases() -> list[tuple[str, str, str, str | None]]:
    """(description, ci text, warm text, substring the errors MUST contain).

    None means no errors expected. A substring, rather than "some error", pins
    each case to the check it names, so a case cannot pass because an
    unrelated check happened to fire.
    """
    linker = "          CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUSTFLAGS: -C link-arg=-fuse-ld=mold\n"
    cmds = "compile commands"
    return [
    ("identical (display name, save-if form, feature order, apt update differ)",
     _CI, _WARM, None),
    ("job-level env drift", _CI, _sub(_WARM, "RUST_LOG: error", "RUST_LOG: info"),
     "job-level env"),
    ("env var added to the CI job only",
     _sub(_CI, "      RUST_LOG: error\n", "      RUST_LOG: error\n      RUST_MIN_STACK: 1\n"),
     _WARM, "job-level env"),
    ("workflow-level env in CI only",
     _sub(_CI, "jobs:\n", "env:\n  CARGO_TERM_COLOR: always\njobs:\n"), _WARM,
     "workflow-level env"),
    ("workflow-level defaults in CI only",
     _sub(_CI, "jobs:\n", "defaults:\n  run:\n    working-directory: crates\njobs:\n"),
     _WARM, "workflow-level defaults"),
    ("job-level defaults in CI only",
     _sub(_CI, "    env:\n      RUST_LOG",
          "    defaults:\n      run:\n        shell: bash\n    env:\n      RUST_LOG"),
     _WARM, "job-level defaults"),
    ("$GITHUB_ENV write in CI only",
     _sub(_CI, "      - uses: actions/checkout@v7\n      - name: Install mold",
          "      - uses: actions/checkout@v7\n      - run: echo CARGO_INCREMENTAL=1 >> $GITHUB_ENV\n      - name: Install mold"),
     _WARM, "$GITHUB_ENV"),
    ("$GITHUB_ENV written via tee in CI only",
     _sub(_CI, "      - uses: actions/checkout@v7\n      - name: Install mold",
          "      - uses: actions/checkout@v7\n      - run: echo CARGO_INCREMENTAL=1 | tee -a \"$GITHUB_ENV\"\n      - name: Install mold"),
     _WARM, "$GITHUB_ENV"),
    ("rust-cache moved before the toolchain step in warm only",
     _CI, _sub(_WARM, """      - uses: dtolnay/rust-toolchain@stable
        with:
          toolchain: 1.93.0
          targets: wasm32-unknown-unknown
      - uses: Swatinem/rust-cache@v2
        with:
          save-if: ${{ github.ref == 'refs/heads/main' }}
          key: ci-${{ hashFiles('.github/workflows/ci-cache-warm.yml') }}
""", """      - uses: Swatinem/rust-cache@v2
        with:
          save-if: ${{ github.ref == 'refs/heads/main' }}
          key: ci-${{ hashFiles('.github/workflows/ci-cache-warm.yml') }}
      - uses: dtolnay/rust-toolchain@stable
        with:
          toolchain: 1.93.0
          targets: wasm32-unknown-unknown
"""), "in order"),
    ("checkout and toolchain swapped in warm only (same set, new order)",
     _CI, _sub(_sub(_WARM, "      - uses: actions/checkout@v7\n      - name: Install mold linker\n",
                    "      - name: Install mold linker\n"),
               "      - uses: Swatinem/rust-cache@v2\n        with:\n          save-if",
               "      - uses: actions/checkout@v7\n      - uses: Swatinem/rust-cache@v2\n        with:\n          save-if"),
     "in order"),
    ("a CI job that saves unconditionally does not need a twin",
     _sub(_CI, "          save-if: ${{ github.ref == 'refs/heads/main' }}\n          key: ci-",
          "          key: ci-"),
     _sub(_WARM, "          save-if: ${{ github.ref == 'refs/heads/main' }}\n          key: ci-",
          "          key: ci-"),
     "does not need from it"),
    ("toolchain bumped in CI only", _sub(_CI, "1.93.0", "1.94.0"), _WARM,
     "rust-toolchain inputs"),
    ("toolchain target dropped from warm",
     _CI, _sub(_WARM, "          targets: wasm32-unknown-unknown\n", ""),
     "rust-toolchain inputs"),
    ("CI job lost its toolchain step",
     _sub(_CI, "      - uses: dtolnay/rust-toolchain@stable\n        with:\n          toolchain: 1.93.0\n          targets: wasm32-unknown-unknown\n", ""),
     _WARM, "no dtolnay/rust-toolchain step"),
    ("rust-cache key changed in CI only",
     _sub(_CI, "key: ci-", "key: v2-"), _WARM, "rust-cache reference"),
    ("env on the rust-cache step in CI only",
     _sub(_CI, "      - uses: Swatinem/rust-cache@v2\n        # a comment",
          "      - uses: Swatinem/rust-cache@v2\n        env:\n          CARGO_HOME: /x\n        # a comment"),
     _WARM, "rust-cache reference"),
    ("CI job no longer uses rust-cache",
     _sub(_CI, "      - uses: Swatinem/rust-cache@v2\n        # a comment inside the step\n        with:\n          save-if: ${{ github.ref == 'refs/heads/main' }}\n          key: ci-${{ hashFiles('.github/workflows/ci-cache-warm.yml') }}\n", ""),
     _WARM, "no longer uses Swatinem/rust-cache"),
    ("CI save-if loosened to also save on PRs",
     _sub(_CI, "save-if: ${{ github.ref == 'refs/heads/main' }}",
          "save-if: ${{ github.ref == 'refs/heads/main' || github.event_name == 'pull_request' }}"),
     _WARM, "exactly `save-if:"),
    ("warm save-if false", _CI,
     _sub(_WARM, "save-if: ${{ github.ref == 'refs/heads/main' }}", "save-if: false"),
     "can stop the warm job saving"),
    ("feature added in CI only",
     _sub(_CI, "trace,websocket", "trace,websocket,redb"), _WARM, cmds),
    ("--no-default-features dropped from warm",
     _CI, _sub(_WARM, "--no-default-features \\\n            ", ""), cmds),
    ("make command missing from warm",
     _CI, _sub(_WARM, "          make -C apps/ping -f run.mk build\n", ""), cmds),
    ("linker env removed from every warm step",
     _CI, _WARM.replace(linker, ""), cmds),
    ("linker env dropped from ONE warm step while another keeps it",
     _CI, _sub(_WARM, "      - name: Compile tests\n        env:\n" + linker,
               "      - name: Compile tests\n"), cmds),
    ("-r added to one warm build",
     _CI, _sub(_WARM, "          cargo build --locked\n", "          cargo build --locked -r\n"), cmds),
    ("extra package in CI test command",
     _sub(_CI, "--exclude ping-types", "-p extra --exclude ping-types"), _WARM, cmds),
    ("RUSTFLAGS exported inside a CI script",
     _sub(_CI, "          cargo build --locked\n", "          export RUSTFLAGS=-Dwarnings\n          cargo build --locked\n"),
     _WARM, cmds),
    ("inline RUSTFLAGS prefix on a CI command",
     _sub(_CI, "          cargo build --locked\n", "          RUSTFLAGS=-Dwarnings cargo build --locked\n"),
     _WARM, cmds),
    ("CC set on a CI step",
     _sub(_CI, '          FREENET_BACKOFF_BASE_SECS: "5"', '          FREENET_BACKOFF_BASE_SECS: "5"\n          CC: clang'),
     _WARM, cmds),
    ("PKG_CONFIG_PATH set on a CI step",
     _sub(_CI, '          FREENET_BACKOFF_BASE_SECS: "5"', '          FREENET_BACKOFF_BASE_SECS: "5"\n          PKG_CONFIG_PATH: /x'),
     _WARM, cmds),
    ("--target-dir in CI only",
     _sub(_CI, "          cargo build --locked\n", "          cargo build --locked --target-dir t2\n"), _WARM, cmds),
    ("cargo clippy added to the CI job",
     _sub(_CI, "          cargo build --locked\n", "          cargo build --locked\n          cargo clippy --all-targets\n"),
     _WARM, cmds),
    ("CI job with no compile commands",
     _CI.replace("          cargo build --locked\n", "").replace("          make -C apps/ping -f run.mk build\n", "")
        .replace("          cargo nextest run --workspace --exclude ping-types \\\n            --no-default-features \\\n            --features trace,websocket \\\n            --profile ci -E 'not test(blocked)'\n", "          ./run-tests.sh\n"),
     _WARM, "no cargo/make compile commands"),
    ("apt package added in CI only",
     _sub(_CI, "apt-get install -y mold", "apt-get install -y mold liblzma-dev"), _WARM,
     "apt-get install packages"),
    ("warm job on a GitHub-hosted runner",
     _CI, _sub(_WARM, "runs-on: ubicloud-standard-8", "runs-on: ubuntu-latest"),
     "Ubicloud serves its own cache backend"),
    ("CI job moved to arm",
     _sub(_CI, "runs-on: ubicloud-standard-16", "runs-on: ubicloud-standard-16-arm"), _WARM,
     "runner label suffix"),
    ("CI job pinned to another OS image",
     _sub(_CI, "runs-on: ubicloud-standard-16", "runs-on: ubicloud-standard-16-ubuntu-2204"), _WARM,
     "runner label suffix"),
    ("warm job named like a required check",
     _CI, _sub(_WARM, 'name: "Warm cache: Unit & Integration"', 'name: "Unit & Integration"'),
     "must be named `Warm cache:"),
    ("warm job id with no CI twin",
     _CI, _sub(_WARM, "  test_unit:\n", "  test_units:\n"), "has no job with the same id"),
    ("a CI job that needs a twin has none",
     _CI + _TEST_UNIT_CI.replace("  test_unit:\n", "  test_sim:\n", 1), _WARM,
     "has no warm job `test_sim`"),
    ("CI twin now runs on push (warm job is dead weight)",
     _sub(_CI, "      github.event_name == 'pull_request'\n",
          "      github.event_name == 'pull_request' || github.event_name == 'push'\n"),
     _WARM, "does not need from it"),
    ("an `!= 'push'` guard still needs a twin",
     _sub(_CI, "      github.event_name == 'pull_request'\n",
          "      github.event_name != 'push' && github.event_name == 'pull_request'\n"),
     _WARM, None),
    ("no CI job needs a twin at all",
     _sub(_CI, "runs-on: ubicloud-standard-16", "runs-on: ubuntu-latest"), _WARM,
     "no job looks like it needs a warm twin"),
    ("non-compile step env (FREENET_*) is ignored",
     _sub(_CI, 'FREENET_BACKOFF_BASE_SECS: "5"', 'FREENET_BACKOFF_BASE_SECS: "9"'),
     _WARM, None),
    ("nextest --profile is a test profile, not a cargo profile",
     _sub(_CI, "--profile ci -E", "--profile default -E"), _WARM, None),
    ]


def self_test() -> int:
    failures = 0
    for desc, ci, warm, expect in _cases():
        errors = check(ci, warm)
        joined = "\n".join(errors)
        ok = (not errors) if expect is None else (expect in joined)
        if ok:
            print(f"ok:   {desc}")
            continue
        failures += 1
        want = "no errors" if expect is None else f"an error containing {expect!r}"
        print(f"FAIL: {desc}: expected {want}, got:")
        for e in errors or ["(no errors)"]:
            print("  " + e.replace("\n", "\n  "))
    # The parser must actually see the commands, or every comparison is vacuous.
    sigs = compile_signatures(extract_job(_CI, "test_unit") or [])
    if len(sigs) != 3:
        failures += 1
        print(f"FAIL: expected 3 compile signatures in the CI fixture, got {sigs}")
    if failures:
        print(f"{failures} self-test case(s) failed")
        return 1
    print("all self-test cases passed")
    return 0


def main(argv: list[str]) -> int:
    if argv[1:] == ["--self-test"]:
        return self_test()
    if len(argv) != 3:
        print(__doc__)
        return 2
    ci_path, warm_path = Path(argv[1]), Path(argv[2])
    errors = check(ci_path.read_text(encoding="utf-8"),
                   warm_path.read_text(encoding="utf-8"),
                   str(ci_path), str(warm_path))
    for e in errors:
        print(f"ERROR: {e}\n")
    if errors:
        print(f"{len(errors)} cache-warm parity error(s)")
        return 1
    print(f"{warm_path} matches {ci_path} for every warm job")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))

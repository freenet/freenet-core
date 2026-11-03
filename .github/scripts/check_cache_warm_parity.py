#!/usr/bin/env python3
"""Keep ci-cache-warm.yml's jobs producing the cache keys ci.yml restores.

ci.yml's heavy PR jobs (Unit & Integration, Simulation, NAT Validation) never
run on pushes to main, so nothing they do can write a main-scoped cache entry,
and a PR can only restore from its own branch or from main. Until
ci-cache-warm.yml existed, no ``v0-rust-test_unit-*`` entry existed at all and
every Unit & Integration run compiled its dependency tree from scratch on a
standard-16 Ubicloud runner.

The warm workflow only helps while its keys are the keys the PR jobs look up.
Swatinem/rust-cache restores by ``<prefix>-<GITHUB_JOB>-<OS>-<arch>-<hash>``,
where ``<hash>`` covers the rustc version and every env var whose name starts
with CARGO, CC, CFLAGS, CXX, CMAKE or RUST. And a restored entry only saves
work if cargo's fingerprints match, which needs the same linker flags and the
same feature sets. Any drift fails SILENTLY: the PR jobs stay green and just
go back to building cold, so nobody notices the bill climbing. Hence this
linter, run from Rule Lint.

For every job in the warm workflow it checks, against the ci.yml job with the
SAME id:

1. workflow-level and job-level ``env:`` are identical;
2. the ``dtolnay/rust-toolchain`` inputs (toolchain, targets, components) are
   identical;
3. the ``Swatinem/rust-cache`` reference and inputs are identical, except
   ``save-if``, which is expected to differ in spirit (the PR jobs never save);
4. compile-affecting step env (``CARGO_TARGET_*``, ``CARGO_PROFILE_*``,
   ``CARGO_BUILD_*``, ``RUSTFLAGS``, ``RUSTDOCFLAGS``) is identical as a set;
5. the set of compile signatures is identical: each ``cargo build`` /
   ``cargo nextest run`` / ``cargo test`` reduced to the flags that decide
   what gets compiled (packages, features, targets, profile), plus every
   ``make`` invocation. ``--no-run``, ``--locked``, ``-E`` filters, ``-j``
   and the like are ignored, so a warm job that compiles without running
   matches a CI job that compiles and runs.

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

COMPILE_ENV_RE = re.compile(
    r"^(CARGO_TARGET_|CARGO_PROFILE_|CARGO_BUILD_|RUSTFLAGS$|RUSTDOCFLAGS$)"
)
CARGO_CMD_RE = re.compile(r"\bcargo\s+(build|nextest\s+run|test)\b(.*)")
MAKE_CMD_RE = re.compile(r"(?:^|[;&|]\s*|\s)(make\s.*)$")

# Flags that take a value and decide what is compiled.
VALUE_FLAGS = {"-p", "--package", "--features", "-F", "--exclude", "--test",
               "--bin", "--example", "--bench", "--target", "--profile",
               "--cargo-profile"}
# nextest's --profile picks a TEST profile (retries, timeouts), not a cargo
# profile, so it does not affect what is compiled.
NEXTEST_IGNORED_VALUE_FLAGS = {"--profile"}
BOOL_FLAGS = {"--no-default-features", "--all-features", "--release",
              "--workspace", "--all", "--lib", "--bins", "--tests",
              "--all-targets"}


def _indent(line: str) -> int:
    return len(line) - len(line.lstrip(" "))


def _is_content(line: str) -> bool:
    s = line.strip()
    return bool(s) and not s.startswith("#")


def _block_after(lines: list[str], start: int) -> list[str]:
    """Lines nested under ``lines[start]`` (deeper indent), comments kept."""
    base = _indent(lines[start])
    out = []
    for line in lines[start + 1:]:
        if _is_content(line) and _indent(line) <= base:
            break
        out.append(line)
    return out


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
        out[key.strip()] = value.strip().strip('"').strip("'")
    return out


def _top_level_env(text: str) -> dict[str, str]:
    lines = text.splitlines()
    for i, line in enumerate(lines):
        if re.match(r"^env:\s*(#.*)?$", line):
            return _mapping(_block_after(lines, i))
    return {}


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


def _job_env(job: list[str]) -> dict[str, str]:
    for i, line in enumerate(job):
        if re.match(r"^    env:\s*(#.*)?$", line):
            return _mapping(_block_after(job, i))
    return {}


def _uses_inputs(job: list[str], action: str) -> list[tuple[str, dict]]:
    found = []
    for step in _steps(job):
        uses, _ = _step_key(step, "uses")
        if uses and uses.split("@")[0] == action:
            _, with_block = _step_key(step, "with")
            found.append((uses, _mapping(with_block)))
    return found


def _compile_env(job: list[str]) -> set[tuple[str, str]]:
    out = set()
    for step in _steps(job):
        _, env_block = _step_key(step, "env")
        for k, v in _mapping(env_block).items():
            if COMPILE_ENV_RE.match(k):
                out.add((k, v))
    return out


def _logical_lines(job: list[str]) -> list[str]:
    """Shell lines with ``\\`` continuations joined and comments dropped."""
    out, buf = [], ""
    for raw in job:
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
            sig.append((tok, ""))
        i += 1
    return (sub, tuple(sorted(sig)))


def compile_signatures(job: list[str]) -> set[tuple]:
    sigs = set()
    for line in _logical_lines(job):
        m = CARGO_CMD_RE.search(line)
        if m:
            sigs.add(_cargo_signature(m.group(1), m.group(2)))
            continue
        m = MAKE_CMD_RE.search(line)
        if m:
            sigs.add(("make", " ".join(m.group(1).split())))
    return sigs


def _fmt(value) -> str:
    if isinstance(value, set):
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
              "entry and then recompiles the dependencies anyway. Change both "
              "files together.")

    ci_wf_env, warm_wf_env = _top_level_env(ci_text), _top_level_env(warm_text)
    if ci_wf_env != warm_wf_env:
        differ("workflow-level env", "*", ci_wf_env, warm_wf_env, key_why)

    ids = job_ids(warm_text)
    if not ids:
        errors.append(f"{warm_name}: no jobs found; the parity check would be vacuous.")
    for job_id in ids:
        warm_job = extract_job(warm_text, job_id)
        ci_job = extract_job(ci_text, job_id)
        if ci_job is None:
            errors.append(
                f"{warm_name}: job `{job_id}` has no job with the same id in "
                f"{ci_name}. rust-cache keys on the job id, so this warm job "
                f"serves nobody. Rename it to match, or delete it if the CI job "
                f"is gone."
            )
            continue

        ci_env, warm_env = _job_env(ci_job), _job_env(warm_job)
        if ci_env != warm_env:
            differ("job-level env", job_id, ci_env, warm_env, key_why)

        ci_tc = _uses_inputs(ci_job, "dtolnay/rust-toolchain")
        warm_tc = _uses_inputs(warm_job, "dtolnay/rust-toolchain")
        if ci_tc != warm_tc:
            differ("dtolnay/rust-toolchain inputs", job_id, ci_tc, warm_tc, key_why)

        def cache_inputs(job):
            return [(u, {k: v for k, v in w.items() if k != "save-if"})
                    for u, w in _uses_inputs(job, "Swatinem/rust-cache")]
        ci_rc, warm_rc = cache_inputs(ci_job), cache_inputs(warm_job)
        if not ci_rc:
            errors.append(
                f"{ci_name}: job `{job_id}` no longer uses Swatinem/rust-cache, "
                f"so its warm twin in {warm_name} is dead weight. Delete it."
            )
        elif ci_rc != warm_rc:
            differ("Swatinem/rust-cache reference or inputs (save-if excluded)",
                   job_id, ci_rc, warm_rc, key_why)

        ci_ce, warm_ce = _compile_env(ci_job), _compile_env(warm_job)
        if ci_ce != warm_ce:
            differ("compile-affecting step env", job_id, ci_ce, warm_ce, fp_why)

        ci_sig, warm_sig = compile_signatures(ci_job), compile_signatures(warm_job)
        if not ci_sig:
            errors.append(
                f"{ci_name}: job `{job_id}`: no cargo/make compile commands "
                f"found, so the signature comparison would be vacuous. If the "
                f"job's commands moved into a script, teach this linter to "
                f"follow them."
            )
        elif ci_sig != warm_sig:
            differ("compile commands (packages/features/targets/profile)",
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
  test_unit:
    name: Unit & Integration
    runs-on: ubicloud-standard-16
    if: github.event_name == 'pull_request'
    env:
      RUST_LOG: error
      CARGO_TARGET_DIR: ${{ github.workspace }}/target
    steps:
      - uses: actions/checkout@v7
      - uses: dtolnay/rust-toolchain@stable
        with:
          toolchain: 1.93.0
          targets: wasm32-unknown-unknown
      - uses: Swatinem/rust-cache@v2
        # a comment inside the step
        with:
          save-if: ${{ github.ref == 'refs/heads/main' }}
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
    - cron: "0 4 * * 1"
jobs:
  test_unit:
    name: "Warm cache: Unit & Integration"
    runs-on: ubicloud-standard-8
    env:
      RUST_LOG: error
      CARGO_TARGET_DIR: ${{ github.workspace }}/target
    steps:
      - uses: actions/checkout@v7
      - uses: dtolnay/rust-toolchain@stable
        with:
          toolchain: 1.93.0
          targets: wasm32-unknown-unknown
      - uses: Swatinem/rust-cache@v2
        with:
          save-if: true
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
"""


def _sub(text: str, old: str, new: str) -> str:
    assert old in text, f"self-test fixture is missing {old!r}"
    return text.replace(old, new, 1)


def _cases() -> list[tuple[str, str, str, bool]]:
    return [
    # (description, ci text, warm text, expect errors?)
    ("identical keys (save-if and display name differ, feature order differs)",
     _CI, _WARM, False),
    ("job-level env drift", _CI, _sub(_WARM, "RUST_LOG: error", "RUST_LOG: info"), True),
    ("env var added to the CI job only",
     _sub(_CI, "      RUST_LOG: error\n", "      RUST_LOG: error\n      RUST_MIN_STACK: 1\n"),
     _WARM, True),
    ("toolchain bumped in CI only", _sub(_CI, "1.93.0", "1.94.0"), _WARM, True),
    ("toolchain target dropped from warm",
     _CI, _sub(_WARM, "          targets: wasm32-unknown-unknown\n", ""), True),
    ("rust-cache prefix-key added in CI only",
     _sub(_CI, "        with:\n          save-if",
          "        with:\n          prefix-key: v1-rust\n          save-if"),
     _WARM, True),
    ("feature added in CI only",
     _sub(_CI, "trace,websocket", "trace,websocket,redb"), _WARM, True),
    ("--no-default-features dropped from warm",
     _CI, _sub(_WARM, "--no-default-features \\\n            ", ""), True),
    ("make command missing from warm",
     _CI, _sub(_WARM, "          make -C apps/ping -f run.mk build\n", ""), True),
    ("linker flags missing from warm compile step",
     _CI, _WARM.replace("-fuse-ld=mold", "-fuse-ld=lld"), True),
    ("warm job id with no CI twin",
     _CI, _sub(_WARM, "  test_unit:\n", "  test_units:\n"), True),
    ("non-compile step env (FREENET_*) is ignored",
     _sub(_CI, 'FREENET_BACKOFF_BASE_SECS: "5"', 'FREENET_BACKOFF_BASE_SECS: "9"'),
     _WARM, False),
    ("nextest --profile is a test profile, not a cargo profile",
     _sub(_CI, "--profile ci -E", "--profile default -E"), _WARM, False),
]


def self_test() -> int:
    failures = 0
    for desc, ci, warm, expect in _cases():
        errors = check(ci, warm)
        if bool(errors) != expect:
            failures += 1
            print(f"FAIL: {desc}: expected {'errors' if expect else 'no errors'}, got:")
            for e in errors:
                print("  " + e.replace("\n", "\n  "))
        else:
            print(f"ok:   {desc}")
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

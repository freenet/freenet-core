#!/usr/bin/env python3
"""Rule-lint #1/#2/#3: ban NEW use of std::time::Instant::now(),
rand::thread_rng()/rand::random(), and tokio::net::UdpSocket in
production code under crates/core/src/ (top-level files and all subdirs).

These patterns break deterministic simulation testing (DST):
  #1. std::time::Instant::now()  — use TimeSource instead
  #2. rand::thread_rng() / rand::random()  — use GlobalRng instead
  #3. tokio::net::UdpSocket  — use Socket trait instead

This linter is diff-scoped: it only flags lines ADDED by the PR so that
pre-existing violations do not trigger CI failures. It improves on the
prior naive grep approach in one critical way:

  * It resolves each flagged line to its position in the HEAD file and
    checks whether the line is inside a #[cfg(test)]-gated scope (or
    attributed with #[test] / #[tokio::test] etc.). Such lines are
    LEGITIMATELY test-only and are EXEMPT from the ban. The prod-code
    enforcement is UNCHANGED — a non-test line that adds any of the
    banned patterns is still a hard failure.

WHY this exemption is needed
-----------------------------
Mechanical refactors that MOVE pre-existing test code (e.g. moving a
`#[cfg(test)] mod tests` from one file to another) make those lines
appear "added" in the diff even though they are verbatim pre-existing
test code. Without this exemption, rules #1/#2/#3 fire on the moved
lines with no escape hatch, blocking the refactor. Real example: PR
#4616 moved a test module containing `use tokio::net::UdpSocket;` from
connection_handler.rs to version_cmp.rs and rule #3 falsely fired.

Design
------
* Read file content from `git show HEAD:<path>` (the actual HEAD state,
  not the working tree) so the analysis reflects what will be in the
  repo after the PR merges.
* Walk the file line by line to build a per-line "is test scope" map:
  - Track brace depth and whether the CURRENT module/block was opened
    by a `#[cfg(test)]` (or `#[cfg(all(..., test, ...))]` etc.) item.
  - Additionally, any item attributed with #[test], #[tokio::test],
    #[test_log::test], or #[freenet_test(...)] is considered test scope
    for itself and its body.
* A line is test-exempt if its "is test scope" flag is True.
* When scope determination is ambiguous (e.g. the brace tracker falls
  out of sync due to braces in string/macro bodies), we FAIL CLOSED:
  the line is NOT exempted and the ban remains active. Document this
  in code with a comment. Do NOT invent exemptions we cannot verify.

Scope of exemption
------------------
Only genuinely test-only code is exempted:
  - `#[cfg(test)]` module bodies
  - `#[test]` / `#[tokio::test]` / `#[test_log::test]` / `#[freenet_test]`
    function bodies and their attributes
  - Items nested inside any of the above

NOT exempted (fail closed):
  - `#[cfg(any(test, feature = "some-feature"))]` — a cfg(any(...))
    that includes non-test variants is NOT guaranteed to be test-only.
    These are uncommon in this codebase; when in doubt, keep the ban.
  - Production helper functions called from both tests and prod code
    (they'll lack the cfg(test) gate and correctly remain banned).

Limitations
-----------
* Brace counting is heuristic: braces in raw strings (`r#"..."#`),
  attribute macros, or highly complex macros may confuse the tracker.
  In practice these are extremely rare on the specific lines that
  contain the banned patterns. If a false positive occurs, the
  maintainer can add the banned use inside a genuine #[cfg(test)] block
  (which is the correct structure anyway).
* Not a full Rust parser. Good enough for the narrow pattern-matching
  task at hand. If this breaks on a real case, report it and we'll
  tighten the tracker.

Run `--self-test` to exercise the matcher against good/bad fixtures.
"""

from __future__ import annotations

import re
import subprocess
import sys
from typing import Optional

# ---------------------------------------------------------------------------
# Banned patterns (rules #1, #2, #3)
# ---------------------------------------------------------------------------

BANNED_PATTERNS: list[tuple[int, str, re.Pattern[str]]] = [
    (
        1,
        "std::time::Instant::now() — use TimeSource instead",
        re.compile(r"std::time::Instant::now\(\)"),
    ),
    (
        2,
        "rand::thread_rng()/rand::random() — use GlobalRng instead",
        re.compile(r"rand::(thread_rng|random)\(\)"),
    ),
    (
        3,
        "tokio::net::UdpSocket — use Socket trait instead",
        re.compile(r"tokio::net::UdpSocket"),
    ),
]

# Attribute patterns that indicate a test-scoped item.
# Matches: #[test], #[tokio::test], #[tokio::test(...)], #[test_log::test],
#          #[test_log::test(...)], #[freenet_test], #[freenet_test(...)].
TEST_ATTR_RE = re.compile(
    r"#\s*\[\s*"
    r"(?:(?:tokio|test_log)\s*::\s*)?test(?:\s*\]|\s*\()"
    r"|#\s*\[\s*freenet_test(?:\s*\]|\s*\()"
)

# Matches #[cfg(test)] — plain cfg(test) only.
# We do NOT match cfg(any(..., test, ...)) because that includes non-test
# configurations (fail-closed design choice: see module docstring).
CFG_TEST_RE = re.compile(r"#\s*\[\s*cfg\s*\(\s*test\s*\)\s*\]")


def strip_line_comment(line: str) -> str:
    """Remove // line comments, preserving string literals.

    This is intentionally simple: it strips from the first // that is NOT
    inside a string literal. Not a full Rust lexer, but sufficient for
    the narrow purpose of preventing comment text from matching banned patterns.
    """
    out = []
    i = 0
    n = len(line)
    in_string = False
    while i < n:
        ch = line[i]
        if in_string:
            if ch == "\\" and i + 1 < n:
                out.append(ch)
                out.append(line[i + 1])
                i += 2
                continue
            if ch == '"':
                in_string = False
            out.append(ch)
            i += 1
            continue
        if ch == '"':
            in_string = True
            out.append(ch)
            i += 1
            continue
        if ch == "/" and i + 1 < n and line[i + 1] == "/":
            break  # rest is comment
        out.append(ch)
        i += 1
    return "".join(out)


# ---------------------------------------------------------------------------
# #[cfg(test)] scope tracker
# ---------------------------------------------------------------------------

def build_test_scope_map(file_lines: list[str]) -> list[bool]:
    """Return a per-line boolean list indicating whether each line (0-based) is
    inside a test-only scope.

    A line is test-scoped if it is inside:
      - A `#[cfg(test)]` module or item body
      - A `#[test]` / `#[tokio::test]` / etc. function body or attribute line

    Design
    ------
    We track a stack of "scope kinds". Each entry is either:
      - "test"  — opened by a cfg(test) or test-attr item
      - "other" — any other brace group

    When we push to the stack:
      - A `#[cfg(test)]` line sets a "pending cfg-test" flag; the next `{`
        opens a "test" scope on the stack.
      - A `#[test]` line (or similar test attr) sets "pending test-fn"; the
        next `{` opens a "test" scope.
      - Any other `{` pushes "other" (unless one of the above flags is set).

    A line is test-scoped if the stack is non-empty AND the current or any
    enclosing scope is "test".

    Fail-closed
    -----------
    Brace counting can be confused by:
      - Braces in string literals or char literals
      - Braces in macro invocations (vec![], format!())
      - Raw strings (r#"..."#)
    We strip // comments but do NOT attempt to strip string literal bodies
    for brace counting (that would require a real lexer). This means the
    tracker can go out of sync on files with complex macro/string usage.
    When we detect an inconsistency (e.g. brace depth goes negative), we
    emit a conservative result: treat the rest of the file as non-test-scope
    so the ban stays active.
    """
    n = len(file_lines)
    is_test = [False] * n

    # Stack entries: list of str ("test" or "other")
    scope_stack: list[str] = []
    # Whether any enclosing scope on the stack is "test":
    test_depth = 0  # count of "test" entries in scope_stack

    # Pending flags set by attribute lines, consumed by the next {
    pending_cfg_test = False
    pending_test_fn = False

    def _current_is_test() -> bool:
        return test_depth > 0

    for idx, raw in enumerate(file_lines):
        line = strip_line_comment(raw)

        # Mark the line itself as test-scoped before updating state.
        # Attribute lines (#[cfg(test)], #[test]) are considered part of
        # the test scope too (they're on the test item).
        is_in_test = _current_is_test() or pending_cfg_test or pending_test_fn
        is_test[idx] = is_in_test

        # Check for cfg(test) attribute on this line.
        if CFG_TEST_RE.search(line):
            pending_cfg_test = True

        # Check for test function attributes on this line.
        if TEST_ATTR_RE.search(line):
            pending_test_fn = True

        # Count braces (heuristic — ignores braces in strings/macros).
        for ch in line:
            if ch == "{":
                # What scope kind is this brace opening?
                if pending_cfg_test or pending_test_fn:
                    scope_stack.append("test")
                    test_depth += 1
                    pending_cfg_test = False
                    pending_test_fn = False
                else:
                    scope_stack.append("other")
            elif ch == "}":
                if scope_stack:
                    kind = scope_stack.pop()
                    if kind == "test":
                        test_depth -= 1
                else:
                    # Brace underflow — tracker is out of sync. Fail closed:
                    # treat remaining lines as non-test. This is conservative
                    # (the ban stays active), not liberal.
                    # Reset all pending flags too.
                    pending_cfg_test = False
                    pending_test_fn = False
                    # Fill remaining lines as False (already the default).
                    break

        # If neither a { nor a } was seen, pending flags survive to the
        # next line (they apply to the next item's brace). Reset only when
        # a { is consumed (done above).

    return is_test


# ---------------------------------------------------------------------------
# Diff parsing
# ---------------------------------------------------------------------------

def parse_added_lines(diff_text: str) -> dict[str, set[int]]:
    """Parse a unified diff and return {path: set of 1-based added line numbers}."""
    result: dict[str, set[int]] = {}
    path: Optional[str] = None
    new_lineno = 0
    for line in diff_text.splitlines():
        if line.startswith("+++ b/"):
            path = line[len("+++ b/"):]
            result.setdefault(path, set())
            continue
        if path is None:
            continue
        if line.startswith("@@"):
            m = re.search(r"\+(\d+)", line)
            new_lineno = int(m.group(1)) if m else 0
            continue
        if line.startswith("+") and not line.startswith("+++"):
            result[path].add(new_lineno)
            new_lineno += 1
        elif line.startswith("-") and not line.startswith("---"):
            pass  # removed lines: don't advance new-file counter
        elif line.startswith(" "):
            new_lineno += 1
    return result


# ---------------------------------------------------------------------------
# Violation finder
# ---------------------------------------------------------------------------

def find_violations_in_file(
    path: str,
    file_lines: list[str],
    added_linenos: set[int],
) -> list[str]:
    """For each ADDED line in this file, check all banned patterns.

    Returns a list of violation strings: "<path>:<lineno>: <content>"

    A line is NOT flagged if it is in a test-scoped context (see
    build_test_scope_map). A line IS flagged if:
      - It is in added_linenos (added by the PR), AND
      - It matches a banned pattern after comment-stripping, AND
      - It is NOT test-scoped, AND
      - Neither the line itself nor the line directly above it carries a
        ``// rule-lint: ok`` annotation (for intentional documented exceptions).

    The ``// rule-lint: ok`` annotation is for the rare case of a legitimate
    production use that must bypass the ban — e.g., the one
    ``tokio::net::UdpSocket`` import in transport.rs that the ``Socket`` trait
    itself wraps. The annotation MUST include a reason:
    ``// rule-lint: ok — <reason>``. New uses require maintainer sign-off;
    this is not a general escape hatch.
    """
    is_test = build_test_scope_map(file_lines)
    violations = []

    rule_lint_ok_re = re.compile(r"//\s*rule-lint:\s*ok")

    for lineno in sorted(added_linenos):
        idx = lineno - 1  # 0-based
        if idx < 0 or idx >= len(file_lines):
            continue
        raw = file_lines[idx]
        stripped = strip_line_comment(raw)

        # Test-scoped lines are legitimately allowed to use real clocks/RNG/sockets.
        if is_test[idx]:
            continue

        # A ``// rule-lint: ok`` annotation on the flagged line itself or on
        # the line directly above it suppresses the violation for documented
        # intentional exceptions (e.g., the one legitimate
        # tokio::net::UdpSocket import that the Socket trait wraps).
        prev_line = file_lines[idx - 1] if idx > 0 else ""
        if rule_lint_ok_re.search(raw) or rule_lint_ok_re.search(prev_line):
            continue

        for rule_num, rule_desc, pattern in BANNED_PATTERNS:
            if pattern.search(stripped):
                violations.append(
                    f"{path}:{lineno}: [rule #{rule_num}] {pattern.pattern} — {rule_desc}\n"
                    f"  line: {raw.rstrip()}"
                )
    return violations


# ---------------------------------------------------------------------------
# Self-test
# ---------------------------------------------------------------------------

# Each fixture is (description, src_text, added_linenos_set, expect_violation: bool)
# added_linenos is 1-based.

SELF_TEST_FIXTURES: list[tuple[str, str, set[int], bool]] = [
    # --- Rule #3: tokio::net::UdpSocket in prod code (MUST flag) ---
    (
        "prod UdpSocket import — must flag (rule #3)",
        """\
use tokio::net::UdpSocket;

fn bind_socket() {
    let _ = UdpSocket::bind("0.0.0.0:0");
}
""",
        {1},  # line 1 added
        True,  # expect violation
    ),
    # --- Rule #1: Instant::now in prod code (MUST flag) ---
    (
        "prod Instant::now call — must flag (rule #1)",
        """\
fn measure() {
    let t = std::time::Instant::now();
    println!("{:?}", t);
}
""",
        {2},
        True,
    ),
    # --- Rule #2: rand::thread_rng in prod code (MUST flag) ---
    (
        "prod rand::thread_rng — must flag (rule #2)",
        """\
fn gen_random() -> u64 {
    rand::thread_rng().gen()
}
""",
        {2},
        True,
    ),
    # --- Inside #[cfg(test)] mod: must NOT flag ---
    (
        "UdpSocket inside #[cfg(test)] mod tests — must NOT flag (the #4616 case)",
        """\
fn prod_fn() {}

#[cfg(test)]
mod tests {
    use tokio::net::UdpSocket;

    #[tokio::test]
    async fn test_bind() {
        let _ = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    }
}
""",
        {5, 9},  # both lines added (verbatim move of test module)
        False,  # expect NO violation
    ),
    # --- Inside #[test] fn: must NOT flag ---
    (
        "Instant::now inside #[test] fn — must NOT flag",
        """\
fn prod_fn() {}

#[test]
fn test_timing() {
    let t = std::time::Instant::now();
    assert!(t.elapsed().as_nanos() < 1_000_000_000);
}
""",
        {5},
        False,
    ),
    # --- Inside #[tokio::test] fn: must NOT flag ---
    (
        "rand::thread_rng inside #[tokio::test] — must NOT flag",
        """\
#[tokio::test]
async fn test_rng() {
    let _v: u8 = rand::random();
}
""",
        {3},
        False,
    ),
    # --- Commented-out banned pattern: must NOT flag ---
    (
        "banned pattern in // comment — must NOT flag",
        """\
fn prod_fn() {
    // don't use std::time::Instant::now() here
    let _ = 1;
}
""",
        {2},
        False,
    ),
    # --- cfg(test) attribute line itself is test-scoped: not flagged ---
    (
        "UdpSocket on the #[cfg(test)] attribute line — must NOT flag",
        """\
// hypothetical: banned pattern inside a cfg(test) annotation comment
#[cfg(test)] // tokio::net::UdpSocket
mod tests {}
""",
        {2},
        False,
    ),
    # --- Test attr line itself exempted, body also exempted ---
    (
        "all lines of a #[tokio::test] fn body — must NOT flag",
        """\
fn prod() {}

#[tokio::test]
async fn my_test() {
    use tokio::net::UdpSocket;
    let s = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    drop(s);
}
""",
        {3, 4, 5, 6, 7},  # all lines added including attr and body
        False,
    ),
    # --- cfg(any(test, feature="x")) is NOT exempted (fail-closed) ---
    (
        "UdpSocket under cfg(any(test, feature)) — MUST flag (fail-closed)",
        """\
#[cfg(any(test, feature = "test-helpers"))]
fn test_helper_fn() {
    let _ = tokio::net::UdpSocket::from_std(std::net::UdpSocket::bind("0.0.0.0:0").unwrap()).unwrap();
}
""",
        {3},
        True,  # fail-closed: not a pure cfg(test), keep the ban
    ),
    # --- Prod code AFTER closing brace of cfg(test) mod: MUST flag ---
    (
        "banned pattern in prod code after cfg(test) block — must flag",
        """\
fn prod_before() {}

#[cfg(test)]
mod tests {
    fn test_stuff() {}
}

fn prod_after() {
    let _t = std::time::Instant::now();
}
""",
        {9},
        True,
    ),
    # --- Multiple added lines: some test, some prod ---
    (
        "mixed: test line ok, prod line fails",
        """\
fn prod_fn() {
    let _t = std::time::Instant::now();
}

#[cfg(test)]
mod tests {
    fn test_inner() {
        let _t = std::time::Instant::now();
    }
}
""",
        {2, 8},  # prod line 2 AND test line 8 both added
        True,  # line 2 is prod and MUST flag; line 8 is test and must not
    ),
    # --- freenet_test attribute: must NOT flag ---
    (
        "banned pattern inside #[freenet_test] fn — must NOT flag",
        """\
#[freenet_test(timeout_secs = 30)]
async fn integration_test() {
    let _t = std::time::Instant::now();
}
""",
        {3},
        False,
    ),
    # -----------------------------------------------------------------------
    # Fixtures for issue #4632: pathspec now covers top-level src files too
    # -----------------------------------------------------------------------
    # A prod banned pattern in a top-level-style file (simulating transport.rs,
    # node.rs, etc.) MUST flag — this is the class of bug #4632 was missing.
    (
        "top-level-file prod Instant::now — MUST flag (issue #4632 regression guard)",
        """\
fn prod_fn() {
    let t = std::time::Instant::now();
    println!("{:?}", t);
}
""",
        {2},
        True,  # pathspec fix means this now reaches the linter and MUST flag
    ),
    # A banned pattern annotated with ``// rule-lint: ok`` on the SAME line
    # must NOT flag — covers the intentional transport.rs UdpSocket import.
    (
        "banned pattern with // rule-lint: ok on same line — must NOT flag",
        """\
use tokio::net::UdpSocket; // rule-lint: ok — intentional low-level construction site
""",
        {1},
        False,
    ),
    # A banned pattern with ``// rule-lint: ok`` on the line ABOVE must NOT flag.
    (
        "banned pattern with // rule-lint: ok on previous line — must NOT flag",
        """\
// rule-lint: ok — intentional documented exception
use tokio::net::UdpSocket;
""",
        {2},
        False,
    ),
    # cfg(test) exemption still works after pathspec broadening (regression guard
    # for the #4622 exemption not being broken by this PR's changes).
    (
        "cfg(test) exemption still works — regression guard for #4622",
        """\
fn prod_fn() {}

#[cfg(test)]
mod tests {
    fn test_fn() {
        let _t = std::time::Instant::now();
    }
}
""",
        {6},
        False,  # inside cfg(test) — must NOT flag
    ),
]


def self_test() -> int:
    failures: list[str] = []
    for desc, src, added, expect_violation in SELF_TEST_FIXTURES:
        file_lines = src.splitlines()
        violations = find_violations_in_file("test_fixture.rs", file_lines, added)
        got_violation = len(violations) > 0
        if got_violation != expect_violation:
            status = "WRONGLY FLAGGED" if got_violation else "NOT FLAGGED (should be)"
            failures.append(
                f"FIXTURE '{desc}': {status}\n"
                + (f"  violations: {violations}" if got_violation else "")
            )

    if failures:
        print("check_banned_patterns self-test FAILED:")
        for f in failures:
            print("  - " + f)
        return 1

    total = len(SELF_TEST_FIXTURES)
    print(f"check_banned_patterns self-test passed ({total} fixtures).")
    return 0


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv: list[str]) -> int:
    if "--self-test" in argv:
        return self_test()

    # Args: BASE HEAD (SHAs or refs). Diff restricted to crates/core/src rust files.
    if len(argv) >= 3:
        base, head = argv[1], argv[2]
        rng = f"{base}...{head}"
    else:
        head = "HEAD"
        rng = "origin/main...HEAD"

    diff_result = subprocess.run(
        [
            "git",
            "diff",
            rng,
            "--",
            # Both globs are needed: git's ** does NOT match files directly under
            # crates/core/src/ (node.rs, transport.rs, etc.) — only files in
            # subdirectories. Adding the bare *.rs glob covers the top-level files.
            # See issue #4632.
            "crates/core/src/*.rs",
            "crates/core/src/**/*.rs",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    diff_text = diff_result.stdout

    added_by_file = parse_added_lines(diff_text)

    violations: list[str] = []
    for path, added_linenos in sorted(added_by_file.items()):
        # Read file content from HEAD (not working tree) so the analysis reflects
        # the actual post-PR content rather than any local modifications.
        content_result = subprocess.run(
            ["git", "show", f"{head}:{path}"],
            capture_output=True,
            text=True,
            check=False,
        )
        if content_result.returncode != 0:
            # File deleted or unreadable — skip (no violations from a deletion).
            continue
        file_lines = content_result.stdout.splitlines()
        violations.extend(find_violations_in_file(path, file_lines, added_linenos))

    if violations:
        print(
            "ERROR (rules #1/#2/#3): banned DST-breaking patterns were ADDED in "
            "crates/core/src/ (production code).\n"
            "These break deterministic simulation testing. Use:\n"
            "  #1: TimeSource trait instead of std::time::Instant::now()\n"
            "  #2: GlobalRng instead of rand::thread_rng() / rand::random()\n"
            "  #3: Socket trait instead of tokio::net::UdpSocket\n"
            "See .claude/rules/testing.md and .claude/rules/code-style.md.\n"
            "Note: uses inside #[cfg(test)] / #[test] / #[tokio::test] scopes\n"
            "are automatically exempt (test code may use real clocks/RNG/sockets).\n"
        )
        for v in violations:
            print("  " + v)
        return 1

    print("rules #1/#2/#3 (banned DST-breaking patterns): no new prod-code offenders.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))

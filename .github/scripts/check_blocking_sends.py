#!/usr/bin/env python3
"""Rule-lint #6: ban NEW blocking `.send(...).await` on event-loop-reachable channels.

A blocking `.send(...).await` on a bounded channel that is drained by — or feeds —
the network event loop / handshake driver can self-stall the loop under fan-out
back-pressure. This is the #4145 / #4231 / #4466 incident class. New code MUST use
`try_send()` (drop+log on Full), `send_timeout(..)` (bounded, off-loop only), or
`tokio::time::timeout(..)`.

This linter is diff-scoped: it only inspects lines ADDED by the PR (so the
grandfathered sites it does not yet cover do not fail CI). It improves on a naive
line-based grep in four ways the reviewers asked for:

  * It joins continuation lines into a single logical statement, so a multi-line
    `sender\n    .send(x)\n    .await` is caught (the multi-line blind spot).
  * It strips `//` comments AND string/char-literal *contents* before matching, so
    prose or assertion strings that merely mention `foo.send(...).await` do not
    trip the lint (the CI-red-on-its-own-test-code failure).
  * It matches BOTH the direct form (`<watched>.send(...).await`) and the wrapper
    form (`<bridge-ish>.send(target, msg).await`) — the wrapper (`P2pBridge::send`)
    is the most common way the event loop hits the bounded `ev_listener_tx`.
  * The `// channel-safety: ok` escape hatch is honoured on the statement's own
    lines OR the immediately preceding source line (matching how the docs and the
    `spawn_outbound` example place it — on the line above).

It also does NOT blanket-exempt a statement merely because the token `try_send`
appears somewhere on it; the exemption requires that the blocking `.send(...).await`
call itself be a non-blocking variant (`try_send` / `send_timeout`), or an explicit
annotation.

The CI path (`find_violations_in_file`) reconstructs each logical statement from
the FULL file content (read at HEAD), then reports only statements that TOUCH a
diff-added line. This closes the split-statement gap below for the CI run. The
self-test path (`find_violations`) sees only synthetic added-line snippets.

Known limits (a maintainer extending this should NOT assume full coverage):

  * Split-statement, diff-only fallback: if a file cannot be read at HEAD (e.g. a
    pure deletion, or this matcher is invoked on raw added lines), the fallback
    joins only consecutive ADDED lines. A blocking send split across an added
    `.send(` line and an unchanged receiver/`.await` line is then missed. The CI
    path avoids this by using full-file context; the gap only remains for the
    fallback.
  * Escape-hatch is a SOFT gate: a bare `// channel-safety: ok` with no
    justification is honoured by the lint (it only matches the literal marker).
    The *reason* after the marker is enforced by human review, not by this
    script. A future tightening could require non-empty text after `ok —`, but
    that risks false-failing legitimate terse annotations, so it is left to
    review for now.
  * Not a Rust parser: string/comment stripping is line-oriented and does not
    understand raw strings (`r#"..."#`), nested block comments, or macro bodies.
    These are rare on a `.send(...).await` line; if one bites, annotate with the
    escape hatch.
  * Wrapper coverage is by *receiver identifier* (`bridge`) + a fixed method set
    (send / send_stream / pipe_stream / send_stream_with_completion). A wrapper
    reached under a different binding name, or a new blocking enqueue method on
    P2pBridge, must be added to WRAPPER_RECEIVERS / WRAPPER_METHODS.

Run `--self-test` to exercise the matcher against good/bad fixtures.
"""

from __future__ import annotations

import re
import subprocess
import sys

# Senders that are drained by — or feed — the network event loop / handshake
# driver. A blocking send on one of these from an on-loop context self-stalls.
WATCHED_SENDERS = [
    "ev_listener_tx",
    "bridge_sender",
    "handshake_cmd_sender",
    "handshake_commands",
    "events_tx",
    "result_router_tx",
]

# Wrapper receivers whose enqueue methods ultimately put a message onto a watched
# bounded channel (P2pBridge's methods all do `ev_listener_tx.send(..).await`).
# Matching these catches the inline-on-event-loop callers that go through the
# wrapper rather than touching the raw sender. We match the *receiver expression's
# trailing identifier*, so e.g. `self.bridge.send(`, `ctx.bridge.send(`, and
# `bridge.send(` all match, while an unrelated `something_else.send(` does not.
WRAPPER_RECEIVERS = [
    "bridge",
]

# The P2pBridge enqueue methods that each block on `ev_listener_tx.send(..).await`
# internally, so a blocking `.<method>(..).await` on a `bridge` receiver from an
# on-loop caller is a #4145-class self-stall — exactly like `bridge.send`. There
# is no on-loop caller of the stream methods today; this is forward-looking so a
# future one is caught by the lint rather than slipping through. Ordered longest-
# first so the regex alternation matches the full method name (e.g.
# `send_stream_with_completion` before `send_stream` before `send`).
WRAPPER_METHODS = [
    "send_stream_with_completion",
    "send_stream",
    "pipe_stream",
    "send",
]

ESCAPE_HATCH = "channel-safety: ok"

# `.await` somewhere in the (comment/string-stripped) statement.
AWAIT_RE = re.compile(r"\.await\b")


def _build_regexes():
    """Build the trigger and full-statement receiver regexes.

    NAME_RE matches a watched sender / wrapper identifier as a standalone token
    (not a substring of a longer identifier). It is the cheap per-line *trigger*:
    any added line mentioning a watched name starts a logical-statement scan.

    SEND_RECEIVER_RE matches the actual blocking enqueue receiver expression on
    the JOINED statement, allowing whitespace (so a multi-line
    `handshake_commands\n    .send(` matches once joined). The trailing identifier
    of the receiver chain must be a watched/wrapper name (`(?<![A-Za-z0-9_])`
    guards against `my_events_tx_wrapper.send`). Two cases are ORed:
      * a WATCHED raw sender doing `.send(`;
      * a WRAPPER receiver (`bridge`) doing any of its enqueue methods
        (`.send(`, `.send_stream(`, `.pipe_stream(`,
        `.send_stream_with_completion(`).
    None of these match `.try_send(` / `.send_timeout(` (the token after the `.`
    differs), so the matcher fires ONLY on the blocking form.
    """
    names = WATCHED_SENDERS + list(WRAPPER_RECEIVERS)
    name_alt = "|".join(re.escape(n) for n in names)
    name_re = re.compile(rf"(?<![A-Za-z0-9_])(?:{name_alt})(?![A-Za-z0-9_])")

    watched_alt = "|".join(re.escape(n) for n in WATCHED_SENDERS)
    wrapper_alt = "|".join(re.escape(n) for n in WRAPPER_RECEIVERS)
    method_alt = "|".join(re.escape(m) for m in WRAPPER_METHODS)
    send_re = re.compile(
        # watched raw sender: `<watched>.send(`
        rf"(?<![A-Za-z0-9_])(?:{watched_alt})\s*\.\s*send\s*\("
        # OR wrapper receiver: `<wrapper>.<enqueue method>(`
        rf"|(?<![A-Za-z0-9_])(?:{wrapper_alt})\s*\.\s*(?:{method_alt})\s*\("
    )
    return name_re, send_re


NAME_RE, SEND_RECEIVER_RE = _build_regexes()


def strip_comments_and_strings(line: str) -> str:
    """Remove `//` line-comment and the *contents* of string/char literals.

    Keeps the delimiters (so `.send(` inside a string becomes `.send(` -> `""`
    contents gone) but blanks the inside, so a `.send(...).await` that lives only
    inside a string literal no longer matches. Not a full Rust lexer — good enough
    for single-line lint scanning (we operate per logical statement). Handles
    escaped quotes within strings.
    """
    out = []
    i = 0
    n = len(line)
    while i < n:
        ch = line[i]
        # Line comment: drop the rest.
        if ch == "/" and i + 1 < n and line[i + 1] == "/":
            break
        # String literal.
        if ch == '"':
            out.append('"')
            i += 1
            while i < n:
                c = line[i]
                if c == "\\" and i + 1 < n:
                    i += 2
                    continue
                if c == '"':
                    break
                i += 1
            out.append('"')
            i += 1
            continue
        # Char literal (best effort): 'a' or '\n' — but skip lifetimes like 'a,
        # by only treating it as a char literal when a closing quote follows soon.
        if ch == "'":
            # Lookahead for a closing quote within 4 chars (covers '\n', '\\', 'a').
            close = line.find("'", i + 1, i + 5)
            if close != -1:
                out.append("''")
                i = close + 1
                continue
            # Otherwise it's a lifetime/label — keep as-is.
            out.append(ch)
            i += 1
            continue
        out.append(ch)
        i += 1
    return "".join(out)


def parse_added_lines(diff_text: str):
    """Yield (path, lineno, raw_line) for each ADDED line in a unified diff."""
    path = None
    new_lineno = 0
    for line in diff_text.splitlines():
        if line.startswith("+++ b/"):
            path = line[len("+++ b/"):]
            continue
        if line.startswith("@@"):
            # @@ -a,b +c,d @@
            m = re.search(r"\+(\d+)", line)
            new_lineno = int(m.group(1)) if m else 0
            continue
        if line.startswith("+") and not line.startswith("+++"):
            yield (path, new_lineno, line[1:])
            new_lineno += 1
        elif line.startswith("-") and not line.startswith("---"):
            # removed line: does not advance the new-file counter
            continue
        elif line.startswith(" "):
            new_lineno += 1


def _statement_is_hatched(file_stripped, file_raws, start_idx, end_idx):
    """Escape-hatch check over the statement's own lines + the contiguous comment
    block immediately preceding it (0-based indices into the file)."""
    for k in range(start_idx, end_idx + 1):
        if ESCAPE_HATCH in file_raws[k]:
            return True
    j = start_idx - 1
    while j >= 0:
        if ESCAPE_HATCH in file_raws[j]:
            return True
        s = file_raws[j].strip()
        if s == "" or s.startswith("//"):
            j -= 1
            continue
        break
    return False


def find_violations_in_file(path, file_lines, added_linenos):
    """Reconstruct logical statements from the FULL file content and flag any
    blocking watched/wrapper send whose statement TOUCHES an added line.

    Closes the split-statement gap (Codex P2): a blocking send split across an
    added `.send(` line and a pre-existing receiver or `.await` line is caught,
    because the statement is rebuilt from real file context — not only from the
    added lines. It stays diff-scoped by only reporting statements at least one
    of whose lines was added by this diff.

    `file_lines` is the list of raw source lines (no trailing newlines).
    `added_linenos` is a set of 1-based line numbers added by the diff.
    """
    violations = []
    stripped = [strip_comments_and_strings(l) for l in file_lines]
    n = len(file_lines)
    for idx in range(n):
        code = stripped[idx]
        if NAME_RE.search(code) is None:
            continue
        # Build the logical statement forward until `.await` or a terminator.
        joined = code
        end = idx
        steps = 0
        while AWAIT_RE.search(joined) is None and end + 1 < n and steps < 12:
            if ";" in stripped[end]:
                break
            end += 1
            steps += 1
            joined += " " + stripped[end]
        if SEND_RECEIVER_RE.search(joined) is None:
            continue
        if AWAIT_RE.search(joined) is None:
            continue
        # Diff scope: only flag if at least one line of this statement was added.
        touched = any((k + 1) in added_linenos for k in range(idx, end + 1))
        if not touched:
            continue
        if _statement_is_hatched(stripped, file_lines, idx, end):
            continue
        # Report at the first ADDED line of the statement (or the statement start).
        report_idx = next(
            (k for k in range(idx, end + 1) if (k + 1) in added_linenos), idx
        )
        violations.append(f"{path}:{report_idx + 1}: {file_lines[report_idx].strip()}")
    return violations


def find_violations(added):
    """added: list of (path, lineno, text). Returns list of violation strings.

    Diff-only reconstruction used by the self-test (which has no real files):
    joins consecutive added lines into a logical statement. The CI path uses
    `find_violations_in_file` instead, which has full-file context and closes the
    split-statement gap. See the "Known limits" section in the module docstring.
    """
    violations = []
    # Group consecutive added lines per file into runs (so continuation joins only
    # within a contiguous added block).
    runs = []
    cur = []
    last = None
    for item in added:
        path, lineno, text = item
        if last is not None and (path != last[0] or lineno != last[1] + 1):
            if cur:
                runs.append(cur)
            cur = []
        cur.append(item)
        last = item
    if cur:
        runs.append(cur)

    for run in runs:
        stripped = [strip_comments_and_strings(t) for (_, _, t) in run]
        raws = [t for (_, _, t) in run]
        n = len(run)
        for idx in range(n):
            code = stripped[idx]
            # Cheap trigger: this added line mentions a watched-sender token.
            if NAME_RE.search(code) is None:
                continue
            # Build the logical statement: extend forward until we see `.await`
            # or a statement terminator, joining stripped code.
            joined = code
            end = idx
            steps = 0
            while AWAIT_RE.search(joined) is None and end + 1 < n and steps < 12:
                if ";" in stripped[end]:
                    break
                end += 1
                steps += 1
                joined += " " + stripped[end]
            # The joined statement must actually be `<watched>.send( ... ).await`.
            # Note SEND_RECEIVER_RE matches `<watched>.send(` but NOT
            # `<watched>.try_send(` / `.send_timeout(` (those have a different
            # token after the `.`), so it already fires ONLY on the blocking form.
            # We therefore do NOT blanket-exempt a statement merely because the
            # token `try_send` appears elsewhere on it (review item 6): the match
            # itself proves the WATCHED sender did a blocking `.send(`.
            if SEND_RECEIVER_RE.search(joined) is None:
                continue
            # A blocking send is a violation only if it is actually awaited.
            if AWAIT_RE.search(joined) is None:
                continue
            # Escape hatch: on any of the statement's own lines, or on the
            # contiguous block of comment lines immediately preceding it (so a
            # multi-line justification ending in the annotation works, as does a
            # single `// channel-safety: ok` on the line directly above).
            window_raws = raws[idx : end + 1]
            hatched = any(ESCAPE_HATCH in r for r in window_raws)
            if not hatched:
                j = idx - 1
                while j >= 0:
                    stripped_prev = raws[j].strip()
                    if ESCAPE_HATCH in raws[j]:
                        hatched = True
                        break
                    # Only walk back over contiguous comment / blank lines.
                    if stripped_prev == "" or stripped_prev.startswith("//"):
                        j -= 1
                        continue
                    break
            if hatched:
                continue
            path, lineno, _ = run[idx]
            violations.append(f"{path}:{lineno}: {raws[idx].strip()}")
    return violations


# --------------------------------------------------------------------------- #
# Self-test
# --------------------------------------------------------------------------- #

GOOD = [
    # try_send is fine
    '+        if !handshake_cmd_sender.try_send(HandshakeCommand::DropConnection { peer }) {',
    # multi-line try_send
    "+        ctx.bridge.ev_listener_tx.try_send(\n+            P2pBridgeEvent::NodeAction(x),\n+        )",
    # send_timeout (bounded) is fine
    "+        match events_tx.send_timeout(event, dur).await {",
    # escape hatch on same line
    "+        events_tx.send(event).await; // channel-safety: ok — detached task",
    # escape hatch on previous line
    "+        // channel-safety: ok — runs in a detached per-connection task\n+        if let Err(e) = events_tx.send(event).await {",
    # escape hatch on the FIRST line of a multi-line comment block above the send
    "+        // channel-safety: ok — off-loop producer, blocking is intended\n+        // (op-driver / contract tasks producing INTO the loop).\n+        self.ev_listener_tx\n+            .send(P2pBridgeEvent::Message(t, m))\n+            .await",
    # prose in a doc comment mentioning the pattern must NOT trip
    "+    /// A blocking `events_tx.send(...).await` here would stall the driver.",
    # the same pattern inside a string literal (assertion message) must NOT trip
    '+            "#4145 SITE 8: a blocking events_tx.send(Event::Inbound).await remains",',
    # unrelated sender (not watched) is fine
    "+        some_other_tx.send(x).await;",
    # a watched *substring* of a longer identifier must NOT trip
    "+        my_events_tx_wrapper.send(x).await;",
    # a non-enqueue method on a wrapper receiver must NOT trip
    "+        let b = self.bridge.clone();",
    # the non-blocking wrapper variant must NOT trip
    "+        if let Err(e) = self.bridge.try_send(target, msg) {",
]

BAD = [
    # direct blocking send on a watched sender
    "+        ctx.bridge.ev_listener_tx.send(P2pBridgeEvent::Message(t, m)).await?;",
    # multi-line blocking send (the blind spot)
    "+        handshake_commands\n+            .send(HandshakeCommand::Connect { peer })\n+            .await;",
    # wrapper form: bridge.send(...).await inline
    "+        if let Err(e) = self.bridge.send(target, msg).await {",
    # item 6: a blocking watched send is NOT exempted just because the token
    # `try_send` also appears on the statement (here on an unrelated sender).
    "+        other.try_send(z); ev_listener_tx.send(m).await;",
    # wrapper stream methods (forward-looking, item 1): each blocks on
    # ev_listener_tx internally, so an on-loop `bridge.<stream method>(..).await`
    # is the same self-stall class.
    "+        self.bridge.send_stream(addr, sid, data, meta).await?;",
    "+        ctx.bridge\n+            .send_stream_with_completion(addr, sid, data, meta, tx)\n+            .await?;",
    "+        bridge.pipe_stream(addr, sid, handle, meta).await?;",
]


def self_test() -> int:
    failures = []

    for i, snippet in enumerate(GOOD):
        added = _snippet_to_added(snippet, base_line=100 + i * 50)
        v = find_violations(added)
        if v:
            failures.append(f"GOOD#{i} wrongly flagged: {v}\n  snippet: {snippet!r}")

    for i, snippet in enumerate(BAD):
        added = _snippet_to_added(snippet, base_line=1000 + i * 50)
        v = find_violations(added)
        if not v:
            failures.append(f"BAD#{i} NOT flagged (should be): {snippet!r}")

    # File-context tests (item 2): exercise find_violations_in_file with synthetic
    # whole-file content + an added-line set, including the split-statement case.
    file_failures = _file_context_self_test()
    failures.extend(file_failures)

    if failures:
        print("check_blocking_sends self-test FAILED:")
        for f in failures:
            print("  - " + f)
        return 1
    print(
        f"check_blocking_sends self-test passed "
        f"({len(GOOD)} good, {len(BAD)} bad, + file-context cases)."
    )
    return 0


def _file_context_self_test():
    """Exercise the CI-path matcher (`find_violations_in_file`) on synthetic files.

    The key case is the split statement: the `.send(` is on an ADDED line but the
    receiver token and/or the `.await` are on UNCHANGED lines. The diff-only
    matcher misses this; the file-context matcher must catch it.
    """
    fails = []

    # Case A (BAD): receiver on an unchanged line, `.send(` + `.await` added.
    # Lines (1-based):
    #   1: ev_listener_tx        (unchanged)
    #   2:     .send(msg)        (ADDED)
    #   3:     .await;           (ADDED)
    file_a = ["ev_listener_tx", "    .send(msg)", "    .await;"]
    v = find_violations_in_file("f.rs", file_a, added_linenos={2, 3})
    if not v:
        fails.append("FILE-CONTEXT A: split send (receiver unchanged) NOT flagged")

    # Case B (BAD): only `.send(` added; receiver AND `.await` unchanged.
    file_b = ["ev_listener_tx", "    .send(msg)", "    .await;"]
    v = find_violations_in_file("f.rs", file_b, added_linenos={2})
    if not v:
        fails.append("FILE-CONTEXT B: split send (only .send added) NOT flagged")

    # Case C (GOOD): the whole statement is unchanged (no added line touches it).
    file_c = ["ev_listener_tx", "    .send(msg)", "    .await;", "let x = 1;"]
    v = find_violations_in_file("f.rs", file_c, added_linenos={4})
    if v:
        fails.append(f"FILE-CONTEXT C: untouched statement wrongly flagged: {v}")

    # Case D (GOOD): split try_send is fine even when touched.
    file_d = ["ev_listener_tx", "    .try_send(msg)", "    ;"]
    v = find_violations_in_file("f.rs", file_d, added_linenos={2})
    if v:
        fails.append(f"FILE-CONTEXT D: split try_send wrongly flagged: {v}")

    # Case E (GOOD): escape hatch on the line above, statement split + touched.
    file_e = [
        "// channel-safety: ok — off-loop producer",
        "self.bridge",
        "    .send(target, msg)",
        "    .await;",
    ]
    v = find_violations_in_file("f.rs", file_e, added_linenos={3})
    if v:
        fails.append(f"FILE-CONTEXT E: escape-hatched split send wrongly flagged: {v}")

    return fails


def _snippet_to_added(snippet: str, base_line: int):
    """Turn a '+'-prefixed (possibly multi-line) snippet into added tuples."""
    out = []
    for j, ln in enumerate(snippet.split("\n")):
        assert ln.startswith("+"), ln
        out.append(("crates/core/src/test_snippet.rs", base_line + j, ln[1:]))
    return out


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def main(argv) -> int:
    if "--self-test" in argv:
        return self_test()

    # Args: BASE HEAD (SHAs or refs). Diff restricted to crates/core/src rust files.
    if len(argv) >= 3:
        base, head = argv[1], argv[2]
        rng = f"{base}...{head}"
    else:
        # Fallback: diff against origin/main.
        head = "HEAD"
        rng = "origin/main...HEAD"

    diff = subprocess.run(
        [
            "git",
            "diff",
            rng,
            "--",
            "crates/core/src/**/*.rs",
        ],
        capture_output=True,
        text=True,
        check=False,
    ).stdout

    # Group added line numbers per file.
    added_by_file = {}
    for path, lineno, _text in parse_added_lines(diff):
        added_by_file.setdefault(path, set()).add(lineno)

    # For each touched file, read its HEAD content and reconstruct statements
    # with full context (closes the split-statement gap, item 2). Fall back to
    # the diff-only matcher if the file can't be read (e.g. a pure deletion).
    violations = []
    for path, added_linenos in sorted(added_by_file.items()):
        content = subprocess.run(
            ["git", "show", f"{head}:{path}"],
            capture_output=True,
            text=True,
            check=False,
        )
        if content.returncode == 0:
            file_lines = content.stdout.split("\n")
            violations.extend(
                find_violations_in_file(path, file_lines, added_linenos)
            )
        else:
            # Fallback: diff-only (no file context available).
            file_added = [
                (path, ln, txt)
                for (p, ln, txt) in parse_added_lines(diff)
                if p == path
            ]
            violations.extend(find_violations(file_added))

    if violations:
        print(
            "ERROR (rule #6): blocking `.send(...).await` on an event-loop-reachable "
            "channel was ADDED in crates/core/.\n"
            "Use try_send() (drop+log on Full), send_timeout() (bounded, off-loop "
            "only), or tokio::time::timeout(); or annotate the call site with\n"
            "  // channel-safety: ok — <reason>\n"
            "if it is provably safe (e.g. a detached per-connection task). "
            "See .claude/rules/channel-safety.md (#4145/#4231/#4466).\n"
        )
        for v in violations:
            print("  " + v)
        return 1
    print("rule #6 (blocking event-loop sends): no new offenders.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))

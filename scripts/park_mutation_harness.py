#!/usr/bin/env python3
"""Mutation harness for the delegate-park byte accounting.

RESTORES ON EVERY EXIT PATH, and that is the whole point of the rewrite.

The previous version copied each file to a `.bak`, mutated the file in place,
and restored on the happy path only. When a run died -- killed, interrupted, or
the process simply going away -- the working tree kept the last mutation and
the backup could be cleaned up from under it. That failure is invisible in the
worst possible way: A STRANDED MUTATION COMPILES. Everything downstream then
runs against silently wrong code, and the next person has no reason to suspect
it because nothing looks broken.

This version never writes to the original at all. It snapshots the file
contents in memory, writes mutants, and restores from memory in a `finally`
plus signal handlers, so SIGINT and SIGTERM take the same path as a clean exit.
It also verifies the restore byte-for-byte and says so, because "I restored it"
is a claim and this workstream has learned to distrust those.

WHY THE LOCK IS NOT OPTIONAL, and why the usual check does not protect you.

`~/.claude/rules/never-trust-cwd-with-parallel-agents.md` exists because two
agents sharing a repository silently write into each other's work, and its
protection is check 3: read the diff before committing, and treat a file you do
not recognise as the signal. That check cannot fire here. Two harnesses in one
worktree each restore correctly FROM THEIR OWN SNAPSHOT, so the tree ends
clean, `git status` is empty, and there is no unfamiliar file to notice. Both
result sets are still garbage: each row is green or red for reasons unrelated
to the mutation the harness believes it applied.

So the failure leaves no trace in the files, only in the conclusions — which is
the one place nobody re-derives. Mutual exclusion is the only protection, which
is why the lock is acquired in `__init__` rather than offered as an option.

That is not hypothetical and it did not take long to demonstrate. The lock was
added after I ran a proof-of-restore against the worktree a live campaign was
using and had to discard the campaign. On its very first run afterwards it
refused a second acquisition — and the offender was `campaign.py` itself,
which constructed two `Tree` objects, the second over files the first had
already mutated. It would have snapshotted mutated content as "pristine" and
restored the tree to it, then produced a full-length, clean-looking table that
was wrong throughout. That was the script about to produce the final evidence
for the pull request.

A GUARD THAT CATCHES SOMETHING ON ITS FIRST RUN IS TELLING YOU THE CLASS IS
MORE COMMON THAN YOU THOUGHT, not that you got unlucky once.
"""
import atexit
import re
import os
import signal
import subprocess
import sys
from pathlib import Path

def _worktree() -> Path:
    """Where to mutate: `$PARK_HARNESS_WORKTREE`, else the repo this file is in.

    Hardcoding one contributor's absolute path made this unrunnable for anyone
    else and in CI — the same defect as leaving it in a job's scratch directory,
    one step along: committed so others can fix it, and still not runnable by
    them.
    """
    env = os.environ.get("PARK_HARNESS_WORKTREE")
    if env:
        return Path(env).resolve()
    # scripts/park_mutation_harness.py -> repo root
    return Path(__file__).resolve().parent.parent


WORKTREE = _worktree()

class Tree:
    """Holds pristine contents and guarantees they go back."""

    #: One harness per worktree. Two of them mutating the same files interleave
    #: their edits, so each restores correctly from its own snapshot and BOTH
    #: produce garbage verdicts -- rows green or red for reasons unrelated to
    #: the mutation each thinks it applied. I did exactly this: ran a
    #: proof-of-restore against the tree a live campaign was using, and had to
    #: discard the campaign. The tree was fine afterwards; the RESULTS were
    #: worthless, which is much harder to notice than a dirty file.
    LOCK = WORKTREE / ".park-mutation-harness.lock"

    def __init__(self, paths):
        try:
            # O_EXCL: fails if another harness already holds this worktree.
            fd = os.open(self.LOCK, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            raise SystemExit(
                f"another mutation harness holds {WORKTREE} (lock: {self.LOCK}). "
                f"Two harnesses on one worktree interleave their mutations and "
                f"both produce meaningless verdicts. Wait for it, or remove the "
                f"lock if you are certain no run is live."
            ) from None
        os.write(fd, f"pid={os.getpid()}\n".encode())
        os.close(fd)
        # EVERYTHING AFTER THE ACQUISITION IS INSIDE THE TRY, and the reason is
        # that the window between the two is where this lock strands itself.
        # `read_bytes()` on a missing file -- a wrong `$PARK_HARNESS_WORKTREE`,
        # a renamed source file -- raised with the lock already on disk and no
        # `atexit` hook or signal handler yet installed, so every later campaign
        # on that worktree was refused until somebody deleted the file by hand.
        #
        # The failure is worse than what the lock guards against, and for the
        # same reason as everything else in this file: it does not announce
        # itself. A stranded lock presents as "the harness refuses to run",
        # which reads exactly like the exclusivity working correctly. That is
        # the third time in this workstream that a guard's own failure mode has
        # been disguised as the guard doing its job.
        try:
            self.paths = [WORKTREE / p for p in paths]
            self.pristine = {p: p.read_bytes() for p in self.paths}
            self._armed = True
            atexit.register(self.restore)
            for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
                signal.signal(sig, self._on_signal)
        except BaseException:
            # Nothing has been mutated yet, so releasing is safe and is the
            # only correct action: re-raise so the caller still sees the real
            # error rather than a lock that outlives it.
            self.LOCK.unlink(missing_ok=True)
            raise

    def _on_signal(self, signum, _frame):
        self.restore()
        sys.exit(128 + signum)

    def restore(self):
        if not self._armed:
            return
        for p, content in self.pristine.items():
            if p.read_bytes() != content:
                p.write_bytes(content)
        # VERIFY, do not assume. A restore that silently failed would leave
        # exactly the stranded mutation this class exists to make impossible.
        bad = [str(p) for p, c in self.pristine.items() if p.read_bytes() != c]
        if bad:
            print(f"!! RESTORE FAILED for {bad} -- WORKING TREE IS DIRTY", flush=True)
            return
        self._armed = False
        self.LOCK.unlink(missing_ok=True)

    def mutate(self, path, old, new):
        """Apply a unique textual mutation; returns False if it does not match."""
        p = WORKTREE / path
        src = self.pristine[p].decode()
        if src.count(old) != 1:
            return False
        p.write_bytes(src.replace(old, new, 1).encode())
        return True

    def reset(self):
        for p, content in self.pristine.items():
            p.write_bytes(content)


#: `cargo test -- <filter>` prints a full, cheerful summary line when the filter
#: matches NOTHING:
#:
#:     test result: ok. 0 passed; 0 failed; 5588 filtered out
#:
#: which is textually indistinguishable from a real pass. A test renamed after a
#: case was written therefore reports GREEN forever with zero tests executed.
#: That is worst for the cases that EXPECT green -- the "documented limit,
#: demonstrated rather than asserted" evidence -- because a rename there
#: silently converts "watched to fail" back into "assumed", which is the exact
#: thing this whole harness exists to stop.
#: Names of the tests cargo actually EXECUTED, which is the only evidence
#: that a given filter matched something. The summary counts cannot answer
#: that question when more than one filter is passed.
_EXECUTED = re.compile(r"^test ([\w:]+) \.\.\. ", re.M)


def run_tests(tests):
    """Run the named tests and classify. NO TESTS RUN IS NOT A PASS.

    Verdicts key on the `test result:` line rather than on `"error:" in output`.
    `cargo test` prints `error: test failed` for a FAILING TEST, so an
    `error:`-keying classifier reports kills as compile breaks. Two people wrote
    that same broken classifier independently here, which makes it a trap rather
    than a mistake.
    """
    r = subprocess.run(
        ["cargo", "test", "-p", "freenet", "--lib", "--"] + tests,
        cwd=WORKTREE, capture_output=True, text=True,
    )
    out = r.stdout + r.stderr
    if "test result: FAILED" in out:
        return "RED"
    if "error[" in out or "error: could not compile" in out:
        return "COMPILE ERROR"
    if "test result: ok" in out:
        # PER FILTER, not "did anything run". Several filters are passed at
        # once, so a total count above zero is satisfied by the OTHER filters
        # while one of them matches nothing -- which is how a case reports the
        # verdict it expected without ever exercising the test it names.
        executed = _EXECUTED.findall(out)
        unmatched = [t for t in tests if not any(t in name for name in executed)]
        if unmatched:
            return f"NO TESTS RAN ({', '.join(unmatched)})"
        return "GREEN"
    # A non-unwinding panic (a null deref, say) ABORTS the process, so cargo
    # prints no `test result:` line at all. That is a KILL, so it must return
    # exactly "RED": `campaign` compares verdicts for equality, so a decorated
    # string like "ABORTED (counts as RED)" is scored as a mismatch and printed
    # with `!!`. It masked nothing, but it put a false alarm in front of the
    # next person to run this, which is its own kind of noise -- and a campaign
    # nobody trusts the output of is a campaign nobody runs.
    if "error: test failed" in out:
        print("      (aborted without a `test result:` line; a non-unwinding "
              "panic is a kill)", flush=True)
        return "RED"
    return "UNKNOWN"


def preflight(cases):
    """Refuse to start if any named test does not exist EXACTLY once.

    Checked before the first mutation rather than discovered as a suspicious
    row, because the failure this prevents does not look like a failure: the
    case reports its expected verdict having executed nothing. Catching it at
    the start also costs one grep instead of a compile.
    """
    src = []
    for path in (WORKTREE / "crates" / "core" / "src").rglob("*.rs"):
        src.append(path.read_text(errors="replace"))
    blob = "\n".join(src)
    missing = []
    for case in cases:
        for name in case[4]:
            n = blob.count(f"fn {name}(")
            if n != 1:
                missing.append((name, n))
    if missing:
        raise SystemExit(
            "PREFLIGHT FAILED, no mutation applied. These test names do not "
            "resolve to exactly one definition, so their cases would run zero "
            "tests and report their expected verdict anyway:\n"
            + "\n".join(f"  {name}: {n} definitions" for name, n in missing)
        )


def campaign(tree, cases, sha):
    """Run every mutation, and make a TRUNCATED run impossible to read as a result.

    The first version printed one row per case and nothing else. Four rows and
    fifteen rows therefore looked equally complete, and a run that died
    mid-campaign -- which happened, for a cause I could not afterwards
    establish -- produced output indistinguishable from a short campaign that
    finished. That is the same defect as the lost backup file one layer over: it
    fails quietly and the failure looks like data.

    So the count is declared UP FRONT, every row is numbered against it, and the
    run ends with an explicit marker naming how many of how many reported. A
    reader checks one line. Absent that line, the table is not evidence.
    """
    total = len(cases)
    print(f"mutation campaign at {sha}")
    print(f"CAMPAIGN START: {total} cases\n", flush=True)
    results = []
    for n, (label, path, old, new, tests, expect) in enumerate(cases, start=1):
        tree.reset()
        if not tree.mutate(path, old, new):
            verdict = "SKIP (pattern not unique)"
        else:
            verdict = run_tests(tests)
        ok = "ok " if verdict == expect else "!! "
        print(f"[{n:2}/{total}] {ok}{verdict:14} (want {expect:5}) {label}", flush=True)
        results.append((label, verdict, expect))

    tree.reset()
    baseline = run_tests([t for c in cases for t in c[4]])
    print(f"\n{'ok ' if baseline == 'GREEN' else '!! '}{baseline:14} (want GREEN) restored baseline")

    unexpected = [r for r in results if r[1] != r[2]]
    complete = len(results) == total and baseline == "GREEN"
    print(
        f"\nCAMPAIGN COMPLETE: {len(results)}/{total} reported, "
        f"{len(unexpected)} unexpected, baseline {baseline}",
        flush=True,
    )
    if not complete or unexpected:
        # Non-zero so a wrapper cannot mistake a bad campaign for a good one
        # either. A caller that only reads stdout still has the marker.
        sys.exit(1)
    return results

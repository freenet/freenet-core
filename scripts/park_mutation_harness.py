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
        self.paths = [WORKTREE / p for p in paths]
        self.pristine = {p: p.read_bytes() for p in self.paths}
        self._armed = True
        atexit.register(self.restore)
        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
            signal.signal(sig, self._on_signal)

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


def run_tests(tests):
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
        return "GREEN"
    return "UNKNOWN"


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

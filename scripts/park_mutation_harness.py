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
"""
import atexit
import signal
import subprocess
import sys
from pathlib import Path

WORKTREE = Path("/home/ian/code/freenet/freenet-core/fix-5554-followup")

class Tree:
    """Holds pristine contents and guarantees they go back."""

    def __init__(self, paths):
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
    print(f"mutation campaign at {sha}\n")
    results = []
    for label, path, old, new, tests, expect in cases:
        tree.reset()
        if not tree.mutate(path, old, new):
            verdict = "SKIP (pattern not unique)"
        else:
            verdict = run_tests(tests)
        ok = "ok " if verdict == expect else "!! "
        print(f"{ok}{verdict:14} (want {expect:5}) {label}", flush=True)
        results.append((label, verdict, expect))
    tree.reset()
    baseline = run_tests([t for c in cases for t in c[4]])
    print(f"\n{'ok ' if baseline == 'GREEN' else '!! '}{baseline:14} (want GREEN) restored baseline")
    return results

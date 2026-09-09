#!/usr/bin/env python3
"""Mutation harness for the delegate wakeup broker (freenet-core#3972).

Each mutation is the exact regression the named test claims to catch, applied
to `crates/core/src/wasm_runtime/delegate_wakeups.rs`. A test that stays GREEN
under its own mutation is not a guard, whatever its name says.

Run it from anywhere; it locates the tree from its own path:

    python3 scripts/delegate_wakeup_mutation_harness.py
    python3 scripts/delegate_wakeup_mutation_harness.py --worktree /path/to/tree

COMMIT YOUR WORK BEFORE RUNNING. This edits a tracked source file in place and
restores it from a backup at the end; a crash mid-run leaves one mutation on
disk, and `git checkout --` cannot tell a mutation from your own unfinished
edit.

DO NOT MOVE OR RENAME THIS SCRIPT'S WORKING DIRECTORY WHILE A CAMPAIGN IS LIVE.
The pristine backup is re-opened by absolute path on every iteration, so moving
it mid-run raises `FileNotFoundError` in the iteration AND in the `finally` that
restores the source. The campaign dies with a MUTATED source left in the tree,
which the next reader takes for ordinary uncommitted work rather than for
damage. This has been done once, during an unrelated tidy-up of a shared scratch
directory, and it survived only because the move happened to land inside a
multi-minute `cargo test` rather than in the sub-second window that reads the
backup. If scratch files need reorganising, move the idle ones and leave this
directory alone until the run ends.


WHY THE PATH IS NOT HARDCODED
-----------------------------

An earlier version of this script carried its author's worktree as a constant.
Run unchanged by anyone else, it would have mutated and then reverted a
DIFFERENT agent's checkout -- corrupting their tree deliberately and restoring
it from a backup of a file they had never seen. On a machine running many
agents over one repository that is not hypothetical. The tree is derived from
`__file__`, or given explicitly; there is no default that points anywhere else.


WHY THE VERDICT IS NOT A SUBSTRING SEARCH FOR "error"
-----------------------------------------------------

`cargo test` prints `error: test failed, to rerun pass ...` on an ordinary test
FAILURE. A matcher that checks `"error:" in output` therefore reports every
genuine KILL as a compile failure, and a compile failure is not evidence about
a guard -- so the whole campaign is discarded. Two agents wrote that identical
matcher independently on the same night, which makes it a trap rather than a
slip. Four discriminators replace it:

* `could not compile` / `error[E...]` is the only sound DID NOT COMPILE signal.
* No `test result:` line WITHOUT a compile error means the process ABORTED -- a
  non-unwinding panic (a null deref, a double panic) never reaches the summary.
  That is a kill, not an unknown.
* `test result: ok. 0 passed; 0 failed` means the FILTER MATCHED NOTHING. This
  is the subtle one: a renamed or deleted test renders as GREEN, which reads as
  "the guard did not fire" when the truth is "the guard was never run". A
  mutation table can otherwise certify a guard that no longer exists. The
  passed-count is parsed and a zero reported as NO TEST RAN.
* Only then is `ok` a survival and `FAILED` a kill.

A SURVIVED verdict still needs confirming by hand with a compile canary: put a
deliberate syntax error in the same region, check the build breaks, remove only
the canary. A stale artefact cannot produce a failing build followed by a
passing one.


A PATTERN THAT MATCHES IS NOT A PATTERN THAT IS TESTED
------------------------------------------------------

Each entry pairs a mutation with the test meant to catch it, and BOTH halves
can be wrong independently. Verifying that every pattern matches the source
exactly once, which this harness does before it will start, only checks the
mutation half. It says nothing about whether the named test is capable of
observing the property the mutation breaks.

Two ways that goes wrong, both of which produced false SURVIVORs on the first
real campaign here:

* THE PAIRING IS WRONG. The property is guarded, but by a DIFFERENT test than
  the one named. The mutation survives, and it reads as an ornamental guard
  when the guard is simply somewhere else. Before believing a survivor, grep
  for the property across the whole test module, not only in the named test.

* THE MUTATION DOES NOT EXPRESS ITS LABEL. The edited line looked like the one
  that delivers the property and was not. One entry here mutated a flag that
  gates the CAP CHECKS while its label claimed it controlled row replacement;
  replacement is done unconditionally a few lines later, so the mutation could
  not have caused the accumulation its label described, and the test passed
  because the property genuinely held. Before believing a survivor, confirm the
  mutated line is the line the property actually rests on.

A false survivor is the mirror of the fabricated kill the classifier notes
describe. Both render as a plausible result rather than as an error, which is
what makes them expensive.


A TABLE BELONGS TO THE SHA IT WAS MEASURED AT
---------------------------------------------

State that SHA whenever you report a table, and do not let a table read as
covering a head it was not measured on.

Carrying a table forward to a later head is allowed, but it has to be shown
rather than assumed, and it takes TWO checks, because a mutation result is a
claim about a PAIR: the target the mutation edits, and the guard that killed
it. Every mutation target must still match exactly once at the new head, AND
every named guard's function body must be byte-identical between the two
revisions. Checking only the target is the tempting half-version and it is
unsound: a table can have every target untouched while the tests that did the
killing were edited underneath it, and it would then describe kills the current
tests might no longer reproduce. Everything else in the file may change freely,
which is why this is a check on regions and not on file hashes.

If either check fails for a mutation, re-run that mutation. A targeted re-run
of two or three is minutes; it is never a reason to report a table you cannot
attribute.
"""

import argparse
import os
import pathlib
import re
import shutil
import subprocess
import sys

REL_SRC = "crates/core/src/wasm_runtime/delegate_wakeups.rs"

# (label, test filter, old, new)
MUTATIONS = [
    ("tag cap removed", "a_tag_over_the_cap_is_refused_and_one_at_the_cap_is_not",
     "    if tag.len() > MAX_WAKEUP_TAG_BYTES {\n        return Err(WakeupRefusal::TagTooLong);\n    }\n",
     "    // MUTATION_APPLIED: tag cap deleted\n"),

    ("delay floor removed", "a_delay_below_the_floor_is_refused_rather_than_clamped",
     "    if after < MIN_WAKEUP_DELAY {\n        return Err(WakeupRefusal::DelayTooShort);\n    }\n",
     "    // MUTATION_APPLIED: delay floor deleted\n"),

    ("delay horizon removed", "a_delay_past_the_horizon_is_refused",
     "    if after > MAX_WAKEUP_DELAY {\n        return Err(WakeupRefusal::DelayTooLong);\n    }\n",
     "    // MUTATION_APPLIED: delay horizon deleted\n"),

    ("per-delegate row cap raised to the node cap", "a_delegate_at_its_lease_cap_is_refused_with_its_own_code",
     "            if held >= MAX_WAKEUPS_PER_DELEGATE {",
     "            if held >= MAX_WAKEUPS_PER_NODE { // MUTATION_APPLIED"),

    ("node row cap removed", "a_full_node_refuses_with_a_code_distinct_from_the_per_delegate_one",
     "            if sched.index.len() >= MAX_WAKEUPS_PER_NODE {",
     "            if false { // MUTATION_APPLIED: node cap deleted"),

    ("per-delegate duty check removed", "a_delegate_that_has_spent_its_duty_budget_is_refused_and_told_why",
     "        if !has_delegate_credit {",
     "        if false { // MUTATION_APPLIED: per-delegate duty check deleted"),

    ("node duty check removed", "a_spent_node_budget_refuses_even_a_delegate_with_full_credit",
     "        if sched.node_budget.credit_micros <= node_floor {",
     "        if false { // MUTATION_APPLIED: node duty check deleted"),

    ("renewal reserve removed (a renewal gets no reserved capacity)", "a_renewal_is_admitted_from_reserved_node_capacity_that_refuses_a_new_lease",
     "        let node_floor: i64 = if renewing {\n            0\n        } else {\n            NODE_RENEWAL_RESERVE_MICROS as i64\n        };",
     "        let node_floor: i64 = 0; // MUTATION_APPLIED: reserve deleted"),

    ("renewal exempted from its OWN budget", "the_reserve_does_not_exempt_a_renewal_from_its_own_budget",
     "        if !has_delegate_credit {",
     "        if !has_delegate_credit && !renewing { // MUTATION_APPLIED"),

    ("duty refill disabled", "duty_credit_refills_with_wall_clock_at_the_stated_share",
     "        let earned = i64::try_from(elapsed.as_micros() / divisor as u128).unwrap_or(i64::MAX);",
     "        let earned = 0i64; // MUTATION_APPLIED: refill disabled"),

    ("charge is a no-op (a run costs nothing)", "a_run_is_charged_as_debt_so_the_next_one_is_not_affordable",
     "        let spent = i64::try_from(spent.as_micros()).unwrap_or(i64::MAX);",
     "        let spent = 0i64; // MUTATION_APPLIED: run charged nothing"),

    ("debt floor removed (one bad run can disable a delegate for hours)", "debt_is_bounded_so_one_pathological_run_cannot_disable_a_delegate_for_hours",
     "        self.credit_micros = self\n            .credit_micros\n            .saturating_sub(spent)\n            .max(-(burst as i64));",
     "        self.credit_micros = self.credit_micros.saturating_sub(spent); // MUTATION_APPLIED"),

    ("deferral bound removed", "deferral_bounds_itself_without_reference_to_park_ttl",
     "    if due.attempts >= MAX_WAKEUP_DEFERRALS {",
     "    if false { // MUTATION_APPLIED: deferral bound deleted"),

    ("orphan rows skipped rather than deleted", "boot_restore_reinstates_leases_and_drops_rows_whose_delegate_is_gone",
     "            outcome.orphaned += 1;\n            drop(sched);\n            db.forget_wakeup(&delegate, &tag);\n            sched = schedule_lock();\n            continue;",
     "            outcome.orphaned += 1; // MUTATION_APPLIED: row left on disk\n            continue;"),

    ("failing restore read reported as empty", "boot_restore_refuses_to_read_a_failing_store_as_an_empty_schedule",
     "    let rows = db.load_wakeups()?;",
     "    let rows = db.load_wakeups().unwrap_or_default(); // MUTATION_APPLIED"),

    ("lease granted despite a failed durable write", "a_durable_write_that_fails_refuses_the_lease_in_both_representations",
     "        return Err(WakeupRefusal::Storage);",
     "        // MUTATION_APPLIED: grant anyway"),

    ("boot smear removed", "boot_restore_smears_an_overdue_backlog_instead_of_firing_it_at_once",
     "            let slot = (outcome.overdue as u64).saturating_sub(1) % spread_ms.max(1);\n            now_ms.saturating_add(slot)",
     "            now_ms // MUTATION_APPLIED: smear removed"),

    ("a fired lease leaves its durable row behind", "only_past_deadlines_fire_and_they_fire_in_deadline_order",
     "        for due in &fired {\n            db.forget_wakeup(&due.delegate, &due.tag);\n        }",
     "        let _ = &fired; // MUTATION_APPLIED: durable row not released"),

    ("budget GC removed", "a_budget_entry_is_dropped_once_it_is_indistinguishable_from_a_fresh_one",
     "        sched.gc_budgets();",
     "        // MUTATION_APPLIED: budget GC deleted"),

    ("re-arm takes a new row instead of replacing", "rearming_a_tag_replaces_its_lease_instead_of_taking_another",
     "        let renewing = sched.index.contains_key(&id);",
     "        let renewing = false; // MUTATION_APPLIED"),

    ("fire batch bound removed", "the_fire_batch_is_bounded_and_the_remainder_stays_due",
     "        while fired.len() < max {",
     "        while fired.len() < usize::MAX { // MUTATION_APPLIED"),

    ("future deadlines fire too", "only_past_deadlines_fire_and_they_fire_in_deadline_order",
     "            if deadline.0 > cutoff {\n                break;\n            }",
     "            // MUTATION_APPLIED: deadline check deleted"),

    ("deferral count not carried out with the lease", "a_fresh_grant_resets_the_deferral_count",
     "            let attempts = sched.deferrals.remove(&id).unwrap_or(0);",
     "            let attempts = sched.deferrals.get(&id).copied().unwrap_or(0); // MUTATION_APPLIED"),
]

RESULT_RE = re.compile(r"test result: (ok|FAILED)\. (\d+) passed; (\d+) failed")
RUNNING_RE = re.compile(r"^running \d+ tests?$", re.MULTILINE)


def classify(out: str) -> str:
    if "could not compile" in out or "error[E" in out:
        return "DID NOT COMPILE"
    match = RESULT_RE.search(out)
    if not match:
        # No summary line. Two very different causes, and calling them both an
        # abort invents kills: a test binary that STARTED and then died without
        # reaching the summary really is a kill (a non-unwinding panic aborts
        # the process), but cargo failing before it ever ran anything -- a
        # missing manifest, a lock it could not take, a full disk -- is an
        # environment failure and says nothing about the guard. The `running N
        # tests` banner is what separates them, because only the binary prints
        # it.
        if RUNNING_RE.search(out):
            return "ABORTED (killed)"
        return "CARGO/ENV FAILURE -- no test binary ran"
    kind, passed, failed = match.group(1), int(match.group(2)), int(match.group(3))
    if passed == 0 and failed == 0:
        return "NO TEST RAN -- filter matched nothing"
    if kind == "FAILED":
        return f"RED (killed, {failed} failed)"
    return f"GREEN -- NOT A GUARD ({passed} passed)"


def check_every_pattern(source: str) -> list[str]:
    """Pre-flight. Both halves of every entry must be real, or nothing runs.

    THE PATTERN must appear EXACTLY once. A pattern that has drifted with the
    code is reported per-mutation by a lesser harness and the run continues,
    producing a table that reads complete while silently covering less than it
    claims. That is the same defect as the classifier bug above: the failure
    renders as a plausible result rather than as an error.

    THE NAMED TEST must exist, exactly once. This is the weak half of the
    pairing rule above, and weak is the honest word for it: it catches a test
    that was renamed or deleted, and it CANNOT catch a test that exists but
    cannot observe the property its mutation breaks. That second case is still
    on the reader, and it is the one that produced false survivors here. But a
    renamed test would otherwise run the campaign to completion and report
    `NO TEST RAN` after ninety minutes, or, on a harness without that
    discriminator, report a GREEN that reads as `the guard did not fire` when
    the guard was never built. Failing before the first `cargo` invocation is
    the cheapest place to catch it.
    """
    problems = []
    for label, test, old, _new in MUTATIONS:
        count = source.count(old)
        if count != 1:
            problems.append(f"  pattern: {count} matches (want exactly 1): {label}  [{test}]")
        defs = source.count(f"fn {test}(")
        if defs != 1:
            problems.append(
                f"  test:    `fn {test}(` defined {defs}x (want exactly 1) -- "
                f"renamed or deleted?  [{label}]"
            )
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--worktree",
        type=pathlib.Path,
        default=pathlib.Path(__file__).resolve().parent.parent,
        help="repository root (default: the tree this script lives in)",
    )
    parser.add_argument("--only", type=int, default=None, help="run one mutation by index")
    parser.add_argument("--list", action="store_true", help="list the mutations and exit")
    args = parser.parse_args()

    if args.list:
        for i, (label, test, _o, _n) in enumerate(MUTATIONS):
            print(f"{i:3}  {label}  [{test}]")
        return 0

    worktree = args.worktree.resolve()
    src = worktree / REL_SRC
    if not src.is_file():
        print(f"not a freenet-core tree: {src} does not exist", file=sys.stderr)
        return 2

    original = src.read_text()
    problems = check_every_pattern(original)
    if problems:
        print(
            "REFUSING TO RUN: the mutation list has drifted from the source.\n"
            "A `pattern:` line means the code a mutation edits has moved or changed, so\n"
            "that mutation would test nothing. A `test:` line means the guard it is paired\n"
            "with has been renamed or deleted, so the run would report a verdict about a\n"
            "test that does not exist. Either way the campaign would produce a table that\n"
            "reads complete while covering less than it claims, which is worse than no\n"
            "table. Fix the entries against the current code before trusting any result.\n",
            file=sys.stderr,
        )
        print("\n".join(problems), file=sys.stderr)
        return 3

    backup = src.with_suffix(".rs.mutation-backup")
    shutil.copy(src, backup)
    logs = worktree / "target" / "mutation-logs"
    logs.mkdir(parents=True, exist_ok=True)

    selected = MUTATIONS if args.only is None else [MUTATIONS[args.only]]
    results = []
    try:
        for index, (label, test, old, new) in enumerate(selected):
            src.write_text(original.replace(old, new, 1))
            completed = subprocess.run(
                ["cargo", "test", "-p", "freenet", "--lib", test],
                cwd=worktree,
                capture_output=True,
                text=True,
                check=False,
            )
            out = completed.stdout + completed.stderr
            (logs / f"{index:02d}-{test}.txt").write_text(out)
            line = f"{classify(out):34} {label}  [{test}]"
            print(line, flush=True)
            results.append(line)
    finally:
        shutil.copy(backup, src)
        os.unlink(backup)

    print("\n=== summary ===")
    for line in results:
        print(line)
    survivors = [line for line in results if line.startswith("GREEN")]
    if survivors:
        print(
            f"\n{len(survivors)} mutation(s) SURVIVED. Confirm each with a compile "
            "canary before reporting it: a stale artefact cannot produce a failing "
            "build followed by a passing one."
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())

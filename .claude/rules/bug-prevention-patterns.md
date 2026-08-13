---
paths:
  - "crates/core/src/bin/**"
  - "scripts/**"
---

# Bug Prevention Patterns (freenet-core)

Patterns that have caused repeat production bugs in this crate. When your
PR touches one of these patterns, apply the corresponding rule.

## `Command::spawn` after `FreeConsole()` or Windows autostart

Any child `Command::spawn()` reachable from a process that has called
`FreeConsole()` — or was launched by Windows autostart without a
console at all — MUST explicitly null all three standard handles:

```rust
cmd.stdin(std::process::Stdio::null())
   .stdout(std::process::Stdio::null())
   .stderr(std::process::Stdio::null());
```

Inheriting the parent's invalid standard handles makes `spawn()` fail
with `"The handle is invalid"` (`ERROR_INVALID_HANDLE`, os error 6).
The failure is silent — `spawn()` returns `Err` but the caller's only
signal is a "subprocess didn't run" at a higher level, which is easily
misdiagnosed as a network failure, AV lock, or permission issue.

### Repeat offender history

| Issue | Site | Fix |
|-------|------|-----|
| (original) | network-child spawn in `run_wrapper_loop` | `service.rs` — null stdio on the `freenet network` child spawn. |
| [#3933](https://github.com/freenet/freenet-core/issues/3933) | `open_log_file` notepad / open / xdg-open | `tray.rs` — null stdio on the viewer spawn. |
| [#3934](https://github.com/freenet/freenet-core/issues/3934) | `spawn_update_command` | `service.rs` — null stdio; caused the exit-42 restart loop. |

A cross-reference comment on the `FreeConsole()` call in
`service.rs::run_wrapper` enumerates the known downstream spawn sites.
Add any new one to that list the same commit you introduce it.

### Audit

Every `Command::new(...)` hit below must either set null stdio on all
three handles OR document why the call site is safe (e.g. user-facing
CLI entry point that has not detached from its console).

```bash
grep -n 'Command::new' crates/core/src/bin/commands/service.rs \
                      crates/core/src/bin/commands/tray.rs
```

Source-level regression pins live in:

- `commands::tray::tests::open_log_file_spawn_must_null_all_three_standard_handles`
- `commands::service::tests::spawn_update_command_must_null_all_three_standard_handles`
- `commands::service::tests::taskkill_pid_must_null_all_three_standard_handles`

A future revert of any of these null-stdio calls fails CI with a
specific, issue-numbered error message rather than shipping the
regression silently.

## Log markers that a CI gate greps for

A gate that decides whether a release ships by grepping the node's log is only
as real as the marker it greps for. Two independent mechanisms silently turn
such a gate into one that cannot fail, and #5236 hit both at once — in the
canary whose entire purpose was to stop a vacuous release signal.

**1. Level.** `crates/core/Cargo.toml:124` enables tracing's
`release_max_level_info`, which compiles out everything below INFO *in release
builds*. A `debug!` marker therefore does not exist in the binary the gate
inspects. It is present in every debug build, so it looks fine locally and in
any test that runs a debug binary; the gate observes nothing and can only pass
vacuously. The `Startup update check complete` marker was a `debug!`, which made
the most common healthy outcome ("finished, staying on this version") produce no
log ending at all — byte-for-byte indistinguishable from a node killed
mid-request.

**2. Anchor.** A whole-file `grep -F` for the marker is satisfied by ANY
occurrence in the file, including a `//` comment — very often one inside the
file's own `#[cfg(test)] mod tests` block, where log excerpts get pasted as
documentation. The source pin then tracks the comment, not the code.

| Marker | How it broke |
|--------|--------------|
| `Startup update check complete` | Emitted at `debug!`, so absent from every release binary. The canary's "did the check finish?" assertion could never observe it. |
| `failed to parse latest version` | Occurs twice in `auto_update.rs`: the production `tracing::warn!` (the format literal `Startup update check: failed to parse latest version '{}'`) and a prose comment inside that file's own `#[cfg(test)] mod tests` (`// WARN failed to parse latest version 'v0.2.121':`). Both are quoted rather than cited by line number on purpose — the first version of this row cited `:1546`/`:1757`, which this very commit's +23 lines had already shifted to `:1569`/`:1780`. A line number in a rule about stale pins rots faster than the thing it describes. Rewording the production line left all 22 assertions green — including `ok - source pin: parse-failure marker` — while a node carrying the #5221 bug then logged check-ran + reworded-warn + check-complete and the canary reported `OK: parsed GitHub's response`. An ordinary log reword deletes the gate, with CI green throughout. |
| (no marker at all) | The gate had nothing to say WHICH release the node compared against, so its healthy verdict was byte-identical to a silently-wrong comparator's — see the positive-fact rule below. Closed by `MARKER_LATEST_SEEN`. |
| `triggering auto-update` | A fixed string, so it never matched `freenet.rs:609`'s "triggering IMMEDIATE auto-update". A node that took the urgent path read as one that never decided to update, for as long as that site had existed. Fail-closed, hence unnoticed. Closed by `MARKER_TRIGGERED_RE` plus a count pin. |

### The rule

- Emit any gate-observed marker at **`info!` or above**. Never `debug!`/`trace!`.
- **Pin the emitting call, not the file.** Match the macro together with its
  literal (`tracing::warn!("<marker>`), whitespace-stripped on both sides so a
  rustfmt reflow cannot disarm it. A bare file grep is satisfied by prose.
- **Pin every arm that shares the marker.** `compare_versions_for_startup` has
  two parse-failure arms; a pin on one lets the other drift.
- Prefer a marker string that is **specific enough not to appear in prose** —
  keeping the `Startup update check: ` prefix is what stops the test-module
  comment quoted above from matching at all.
- **Assert a POSITIVE fact, not the absence of an error.** "No error appeared"
  is satisfied by a component that is silently WRONG as well as by one that
  works: a `version_from_tag` regressed to a constant, or a normaliser
  truncating `0.2.121` to `0.2.12`, parses, compares, declines to update and
  logs a clean completion — a log byte-identical to a healthy node's. Make the
  code log the value it acted on, and have the gate compare it against an
  independently-obtained expected value. Resolve that expected value from the
  **same source the code uses** (here, the `releases/latest` redirect, not
  `api.github.com`): two sources that are allowed to disagree produce failures
  that are not bugs.
- **Pin the COUNT when a marker is supposed to match a SET of call sites.** A
  fixed-string `MARKER_TRIGGERED` missed `freenet.rs:609` ("triggering
  IMMEDIATE auto-update") for as long as that site existed, so a node taking
  the urgent path read as one that never decided to update. It failed CLOSED,
  which is exactly why nobody noticed — **fail-closed is not the same as
  correct, and it is the condition under which a wrong enumeration survives
  longest.** But **the count must not be derived from the marker it audits.**
  The first version of that pin computed the actual site count as
  `grep -cE "$MARKER_TRIGGERED_RE"` — the very regex under audit — so a site
  the regex failed to match was missing from the count too, and the two errors
  cancelled. Demonstrated: adding a sixth trigger site worded "triggering a
  fresh auto-update" left the suite fully green, including the assertion
  claiming exactly five sites. It caught a REWORDED existing site (count drops)
  and nothing else, which is the weaker half of what it advertised. Derive the
  expected count from a **structural** anchor the marker cannot influence —
  here `update_tx.send(new_version)`, the call that actually requests the
  update — then assert the marker matches all of them. Same shape as the
  "metric re-derived at the call site" row below: an audit whose two operands
  come from one source cannot report a disagreement.
- **A skip branch in a gate is a vacuous-pass waiting to happen.** If the gate
  can only run its check when some input is present, pin the caller that
  supplies it. `assert_detection_healthy` skips the equality check when
  `CANARY_EXPECTED_LATEST` is unset — correct for keeping the function pure and
  fixture-testable, but it makes the check only as real as `cmd_preflight`, so
  that assignment is itself pinned.

### Audit

Every marker a script greps for must resolve to production code at every
occurrence, and its pin must be mutation-tested by rewording the real call and
confirming the pin goes RED.

```bash
grep -n '^MARKER_' scripts/auto-update-canary.sh
# then, for each marker, confirm no occurrence is a comment:
grep -n "<marker>" crates/core/src/bin/commands/auto_update.rs \
                   crates/core/src/bin/freenet.rs
```

Source-level regression pins live in
`scripts/auto-update-canary_test.sh`: `pin_warn_literal` for the parse-failure
arms, the INFO-level checks on `MARKER_CHECK_COMPLETE` and
`MARKER_LATEST_SEEN`, the trigger-site COUNT pin, and the pin that
`cmd_preflight` still arms the equality check. The gate's own WIRING — that the
canary still runs, and still runs before `--draft=false` — is pinned by
`scripts/release_canary_wiring_test.sh`.

## Self-satisfying `include_str!` source-scrape pins

A source-scrape pin — a test that `include_str!`s its own crate's source
and asserts some symbol appears inside a function — **silently stops
testing** when its anchor moves. `split_once` does not fail on a missing
anchor: it matches a *later* occurrence, which is very often the pin's
own assertion string literal further down the same file. The "scoped"
region then balloons to the rest of the file, where the searched-for
symbol certainly appears somewhere, and the assertion passes vacuously
under a name that tells the next maintainer the case is covered.

Seen twice, both shipped:

| Issue | Pin | How it broke |
|-------|-----|--------------|
| [#5102](https://github.com/freenet/freenet-core/issues/5102) | `test_get_latest_version_consults_rate_limit_bucket` | PR #5103 moved `reqwest::Client::builder()` out of the scoped function. Deleting the `try_consume_node_poll()` guard **entirely** still passed. |
| #5102 follow-up | `open_log_file_spawn_must_null_all_three_standard_handles` (tray.rs) | Anchor self-matched the test's own literal; end anchor `"\n#[cfg(test)]"` never matched (tray.rs's `#[cfg(test)]` is *above* the function), so `unwrap_or` widened the region to EOF. It bit only because that function is the file's sole null-stdio site — **complying with the `Command::spawn` rule above would have supplied a decoy and killed it.** |

### The rule

Scope every source-scrape pin to the function body, and make a moved
anchor fail LOUDLY rather than widen silently. `commands::auto_update`'s
test module has a `fn_body()` helper that does this; copy it rather than
hand-rolling a `split_once`. It:

- slices from the signature to the function's closing brace;
- rejects a signature that matched **inside the test module** (the #5103
  fallthrough — position check, because a region starting after
  `#[cfg(test)]` can never *contain* it);
- rejects an **indented** signature, because the `\n}\n` end anchor finds
  the enclosing `impl`'s brace for a method, not the method's own (all
  six `impl UpdateCommand` methods in `update.rs` slice to the same
  ~650-line region).

**Prefer a cross-file scrape.** A pin that lives in `auto_update.rs` and
scrapes `update.rs` cannot be satisfied by its own assertion literal at
all — a structural guarantee rather than a check someone must remember.

### The same class without `include_str!`: an expectation stored as a copy

A pin does not need a sliding anchor to follow its subject. **An expected
value stored as a plaintext copy of the value it guards is rewritten by
the same edit that changes the value**, so the pin renames itself and
stays green. Nothing self-references; the two literals are simply
identical, and the realistic edit is a sweep over both.

Seen in #5303. `MARKER_LATEST_SEEN_SINCE` names the first published
release emitting a log marker; rewording the marker requires moving it,
and nothing prompted anyone (the source pin interpolates the marker, so
it follows a rename by construction). A freeze was added storing the
expected marker text as a literal beside the constant. Its own mutation
test killed it: a reword is performed as a `sed` sweep across the files
mentioning the marker, that sweep rewrote the frozen expectation too, and
the suite passed with the constant still stale.

Then the fix's *fix* failed the same way one level up. With the text
stored base64 the reword went red — but following the failure message's
own instruction (regenerate the encoded blob) went green again with the
version constant untouched. Two adjacent assertions are not a pair.

**The rules:**

- **Encode the expectation** (base64, hash, checksum) whenever the value
  it guards is text a sweep could match. Leave it plaintext only when the
  realistic wrong edit is a human typing one new value — a version
  constant qualifies, since bumps touch `Cargo.toml` and the lockfile,
  not test scripts. Document the asymmetry where you rely on it.
- **A freeze forces a decision only if the remediation cannot be
  performed without making that decision.** Freeze values that must move
  together as ONE blob, so regenerating the expectation is impossible
  without re-stating both.
- **Mutation-test the REMEDIATION PATH, not just the regression.** Apply
  the mutation, then do exactly what your own failure message says, and
  check whether the suite goes green while the problem remains.

### Relation pins between quantities on different clocks

A pin asserting a fixed relation between two values that change on
*different schedules* is correct in one phase and wrong in another. It
looks anchored — the second quantity is one the author cannot influence,
which is normally the point — but it encodes a phase assumption nobody
writes down.

#5290 pinned `MARKER_LATEST_SEEN_SINCE >= crate version`, on the premise
"the marker is new in this tree, so no published release emits it". True
until that release published; the pin then blocks the *next* release for
holding the correct value. Reversing it to `<= crate version` is correct
after publication and wrong during a marker reword, where the right
constant is crate+1 and the pin goes red for it.

The constant tracks **release history**; the crate version tracks **this
tree**. No relation asserting they stay in step holds in both phases.

- Ask **"what future state legitimately requires this to move?"** before
  pinning a relation. If the answer is "a state where the relation is
  violated", the relation is the wrong pin.
- Prefer a **freeze on the value**, which is phase-independent and
  catches strictly more (the empty string, and the most likely wrong
  value, neither of which a one-sided relation sees).
- A *loose plausibility bound* is not the same thing and is not refuted
  by the above: `<= crate + 1` passes during a reword. Note that the
  claim "it fires on a reword too" was repeated through two reviews
  before anyone did the arithmetic — **check a refutation before
  propagating it**, especially one that is a single comparison.

### Audit

Every pin must be mutation-tested when written: apply the exact
regression it names and confirm it FAILS. A pin nobody has seen fail is
not known to work.

```bash
grep -rn 'include_str!("' crates/core/src/bin/ | grep -v assets
```

## SIGPIPE under `pipefail`: a present marker reads as absent

In a script that sets `set -o pipefail`, piping a producer into a consumer that
**short-circuits** — `grep -q`, `head -n`, `read` — makes the producer die with
SIGPIPE (exit 141) as soon as the consumer stops reading. `pipefail` then
promotes 141 to the pipeline's status. So:

```bash
set -uo pipefail
if printf '%s' "$logs" | grep -qF "$MARKER"; then   # WRONG
```

reports **false for a marker that is present**. The `if` is not testing whether
the marker is there; it is testing whether the producer finished writing.

Three properties make this worse than an ordinary bug:

**It is volume-dependent, so it is intermittent.** Below the 64 KB pipe buffer
the producer finishes before `grep -q` exits and nothing happens. Above it, the
producer is still writing and takes the signal. Every small fixture passes; the
failure waits for a real input. Measured on #5236's canary, same log content
with only trailing volume varied: 1 KB → `rc=0`; 200 KB → `rc=1` with a wrong
diagnosis; a real 3.65 MB node log → `grep -acF` finds the line (count 1) while
the piped `grep -q` exits **141** and the direct `grep -q` exits 0.

**It corrupts the diagnosis, not just the verdict.** The gate blamed
"the startup update check never ran" — pointing the next reader at auto-update
detection when the fault was a shell pipeline. It hit 2 of 3 real preflight
runs and `cmd_preflight` does not retry an rc=1, so a healthy release was
blocked by an error about the wrong subsystem.

**Framing decides whether it fires, invisibly.** Whether the consumer can
short-circuit early depends on where the match falls and how the stream is
split into lines, neither of which is visible at the call site. Same file, same
consumer, match on line 1 of a 165 KB source:

```bash
sed 's/\\$//' "$AU" | grep -qF 'Auto-update'                # rc=141
sed 's/\\$//' "$AU" | tr -d '[:space:]' | grep -qF '…'      # rc=0
```

The second is safe only because `tr` deletes every newline, leaving one line
grep must read to EOF before it can report. `pin_marker` depended on that
accident without knowing it — deleting the `tr` as a "simplification" would
have armed the hazard on every source pin in the file at once.

### Repeat offender history

| Site | How it broke |
|------|--------------|
| `node_decided_to_update`, `node_check_settled` (`scripts/auto-update-canary.sh`) | Diagnosed and fixed when the mechanism was first found. Latent — canary logs never got large enough. |
| Four checks in `assert_detection_healthy`, same file, same commit | NOT fixed by that pass, and not latent: Gate A's normal path leaves the node logging ~33 KB/s until it is killed seconds later, so the markers sit far behind the buffer. |
| `pin_marker` (`scripts/auto-update-canary_test.sh`) | Safe only by accident of an intervening `tr -d '[:space:]'`, as above. |
| `printf '%s' "$WRONG_OUT" \| grep -qF` (`scripts/auto-update-canary_lifecycle_test.sh`) | Latent; would have reported "the wrong diagnosis" for the right one. |

### The rule

**When you fix this, fix every instance in the repo, not the one you were
reading.** That is the actual lesson of #5236: the same commit correctly
diagnosed the mechanism, wrote the explanation down in a comment, fixed two
helpers — and left four call sites inside the very function those helpers
serve. Grep first, fix the set, then write the comment.

Safe forms, in order of preference:

- **Grep the file directly** rather than slurping it into a variable and piping
  it: `grep -aqF -- "$needle" "$dir"/*.log`. No pipe, no producer to kill.
- **Match the variable with a bash glob**: `[[ "$out" == *"$needle"* ]]`. Also
  cheaper than forking grep.
- **Take a count and test it**: `[ "$(grep -acF …)" -gt 0 ]` — the pipeline's
  value is used, not its status.
- **`|| true`** where the producer's status genuinely does not matter — but
  prefer one of the above, since `|| true` also swallows real errors.

`head` in a command substitution (`v="$(cmd | head -1)"`) is the same class but
is normally fine: the value is used and the status discarded. It is only a
hazard when something consumes the pipeline's status.

### Audit

```bash
# which scripts are exposed at all
grep -ln 'pipefail' scripts/*.sh

# candidate sites; cross-reference against that list
grep -rnE '\|[[:space:]]*(grep[[:space:]]+-[a-zA-Z]*q|head[[:space:]]|read[[:space:]])' scripts/
```

A hit matters when the pipeline's **status** is consumed — an `if`/`elif`,
`&&`/`||`, a `while` condition, or a function whose last command it is. A hit
whose stdout is captured and whose status is ignored is benign.

The release-gate scripts that set `pipefail` are pinned against regression by
`scripts/auto-update-canary_test.sh` ("no 'pipe into grep -q' …"), which fails
if the form reappears in any of them. Regression tests in the same file drive
>64 KB fixtures through `assert_detection_healthy` in both directions, since a
small-fixture test cannot see this class at all.

**The rest of `scripts/` is NOT pinned and has not been audited site by site.**
As of #5236 the greps above return 20 scripts setting `pipefail` and ~69
candidate sites across all of `scripts/`. Two that look worth a closer read
when someone next touches those files, neither investigated here:
`deploy-to-gateways.sh`'s health check pipes 100 journalctl lines into
`grep -q` and consumes the status, and `deploy-local-gateway.sh` pipes
`systemctl list-unit-files` (47 KB on one ordinary host, and it grows with the
machine) into `grep -q` at five sites. Both would fail in the safe direction —
reporting a healthy service as unhealthy — which is precisely the direction
that survives unnoticed.

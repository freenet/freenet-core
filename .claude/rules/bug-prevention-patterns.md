---
paths:
  - "crates/core/src/bin/**"
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

### Audit

Every pin must be mutation-tested when written: apply the exact
regression it names and confirm it FAILS. A pin nobody has seen fail is
not known to work.

```bash
grep -rn 'include_str!("' crates/core/src/bin/ | grep -v assets
```

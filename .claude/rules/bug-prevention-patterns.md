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
